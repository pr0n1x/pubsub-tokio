use core::fmt::Debug;
use futures_util::future::join_all;
use snafu::Snafu;
use std::{
    collections::HashMap,
    fmt::Display,
    future::Future,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc::{channel, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender};
#[cfg(feature = "tokio-stream")]
use tokio_stream::wrappers::ReceiverStream;

use crate::readonly::Ro;

pub type SubscriberId = u32;
pub type SubscriptionSeqNo = u32;
pub type SubscriptionCount = AtomicU32;
pub type SubscriptionKey = u64;
pub type SubscriptionAtomicKey = AtomicU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId {
    pub subscriber_id: Ro<SubscriberId>,
    pub seq_no: Ro<SubscriptionSeqNo>,
}

#[derive(Debug, Clone)]
pub enum Packet<T> {
    Data(T),
    Heartbeat(u64),
    Disconnect,
}

pub struct PacketReceiver<T> {
    pub subscription_id: Ro<SubscriptionId>,
    receiver: Receiver<Packet<T>>,
}

#[derive(Clone)]
pub struct PacketSender<T> {
    pub subscription_id: Ro<SubscriptionId>,
    sender: Sender<Packet<T>>,
    filter: Option<FilterInner<T>>,
    on_send: Option<OnTransferInner<T>>,
}

pub type FilterFuture = Pin<Box<dyn Future<Output = bool> + Send>>;

pub enum Filter<T> {
    Sync(Box<dyn Fn(&T) -> bool + Send + Sync>),
    Async(Box<dyn Fn(&T) -> FilterFuture + Send + Sync>),
}

#[derive(Clone)]
enum FilterInner<T> {
    Sync(Arc<dyn Fn(&T) -> bool + Send + Sync>),
    Async(Arc<dyn Fn(&T) -> FilterFuture + Send + Sync>),
}

pub struct OnTransfer<T>(Box<dyn Fn(&mut T) + Send + Sync>);
#[derive(Clone)]
struct OnTransferInner<T>(Arc<dyn Fn(&mut T) + Send + Sync>);

pub enum ConnectionMessage<T> {
    Connect(PacketSender<T>),
    Disconnect(SubscriptionId),
    Filter(SubscriberId, Filter<T>),
    RemoveFilter(SubscriberId),
    SubFilter(SubscriptionId, Filter<T>),
    RemoveSubFilter(SubscriptionId),
    OnSend(SubscriptionId, OnTransfer<T>),
    RemoveOnSend(SubscriptionId),
}

pub struct Subscriber<T> {
    pub id: Ro<SubscriberId>,
    connection_sender: Sender<ConnectionMessage<T>>,
    subscriptions_count: SubscriptionCount,
}

pub struct SubscriptionOptions<T> {
    pub filter: Option<Filter<T>>,
    pub on_send: Option<OnTransfer<T>>,
}

pub struct Dispatcher<T, const N: usize> {
    pub name: Ro<String>,
    log_prefix: String,
    data_receiver: Receiver<T>,
    connection_receiver: Receiver<ConnectionMessage<T>>,
    data_senders: HashMap<SubscriptionId, PacketSender<T>>,
    filters: HashMap<SubscriberId, FilterInner<T>>,
    on_dispatch: Option<OnTransferInner<T>>,
    disconnection_notifier: (UnboundedSender<SubscriptionId>, UnboundedReceiver<SubscriptionId>),
}

pub struct PreSubscriber<'a, 'b, T, const N: usize> {
    dispatcher: &'a mut Dispatcher<T, N>,
    subscriber: &'b Subscriber<T>,
}

pub struct PubSub<T, const N: usize = 1> {
    pub dispatcher: Dispatcher<T, N>,
    pub publisher: Sender<T>,
    pub subscribers: [Subscriber<T>; N],
}

pub const DEFAULT_BUFFER_SIZE: usize = 1024;

impl<T: Clone, const N: usize> PubSub<T, N> {
    pub fn new(name: impl Into<String>, buf: usize) -> PubSub<T, N> {
        if N > u16::MAX as usize {
            panic!("subscribers count can't be more than {}", u16::MAX);
        }

        let (connection_sender, connection_receiver) = channel(100);

        let (data_sender, data_receiver) = channel(buf);
        let name = name.into();
        let log_prefix = match name.is_empty() {
            true => "Dispatcher".into(),
            false => format!("Dispatcher[{name}]"),
        };
        PubSub {
            dispatcher: Dispatcher {
                name: Ro::new(name),
                log_prefix,
                data_receiver,
                connection_receiver,
                data_senders: HashMap::new(),
                filters: HashMap::new(),
                on_dispatch: None,
                // we can use UnboundSender without caring about it's size
                // because it can't be larger than the current subscription count
                disconnection_notifier: unbounded_channel(),
            },
            publisher: data_sender,
            subscribers: std::array::from_fn(|id| Subscriber {
                id: Ro::new(id as SubscriberId),
                connection_sender: connection_sender.clone(),
                subscriptions_count: SubscriptionCount::new(0),
            }),
        }
    }
}

impl<T: Clone, const N: usize> Default for PubSub<T, N> {
    fn default() -> Self {
        Self::new("", DEFAULT_BUFFER_SIZE)
    }
}

const SUBSCRIPTION_CONNECTION_TIMEOUT: Duration = Duration::from_millis(100);

trait SubscriptionConnector<T> {
    async fn subscribe(
        &self,
        subscription_id: SubscriptionId,
        filter: Option<Filter<T>>,
        on_send: Option<OnTransfer<T>>,
    ) -> SubscriptionResult<PacketReceiver<T>>;

    async fn filter(&self, subscriber_id: SubscriberId, filter: Option<Filter<T>>) -> SubscriptionResult;

    async fn subfilter(&self, subscription_id: SubscriptionId, filter: Option<Filter<T>>) -> SubscriptionResult;

    async fn on_send(
        &self,
        subscription_id: SubscriptionId,
        on_send: Option<impl Into<OnTransfer<T>>>,
    ) -> SubscriptionResult;

    async fn disconnect(&self, subscription_id: SubscriptionId) -> SubscriptionResult;
}

fn packet_channel<T>(
    subscription_id: SubscriptionId,
    sub_filter: Option<Filter<T>>,
    on_send: Option<OnTransfer<T>>,
) -> (PacketSender<T>, PacketReceiver<T>) {
    let (sender, receiver) = channel(1024);
    (
        PacketSender {
            subscription_id: Ro::new(subscription_id),
            sender,
            filter: sub_filter.map(FilterInner::from),
            on_send: on_send.map(OnTransferInner::from),
        },
        PacketReceiver {
            subscription_id: Ro::new(subscription_id),
            receiver,
        },
    )
}

impl<T> SubscriptionConnector<T> for Sender<ConnectionMessage<T>> {
    async fn subscribe(
        &self,
        subscription_id: SubscriptionId,
        sub_filter: Option<Filter<T>>,
        on_send: Option<OnTransfer<T>>,
    ) -> SubscriptionResult<PacketReceiver<T>> {
        let (sender, receiver) = packet_channel(subscription_id, sub_filter, on_send);
        self.send(ConnectionMessage::Connect(sender))
            .await
            .map_err(|_| SubscriptionError::ConnectionError)?;
        Ok(receiver)
    }

    async fn filter(&self, subscriber_id: SubscriberId, filter: Option<Filter<T>>) -> SubscriptionResult {
        self.send(match filter {
            Some(filter) => ConnectionMessage::Filter(subscriber_id, filter),
            None => ConnectionMessage::RemoveFilter(subscriber_id),
        })
        .await
        .map_err(|_| SubscriptionError::ConnectionError)
    }

    async fn subfilter(&self, subscription_id: SubscriptionId, filter: Option<Filter<T>>) -> SubscriptionResult {
        self.send(match filter {
            Some(filter) => ConnectionMessage::SubFilter(subscription_id, filter),
            None => ConnectionMessage::RemoveSubFilter(subscription_id),
        })
        .await
        .map_err(|_| SubscriptionError::ConnectionError)
    }

    async fn on_send(
        &self,
        subscription_id: SubscriptionId,
        on_send: Option<impl Into<OnTransfer<T>>>,
    ) -> SubscriptionResult {
        self.send(match on_send {
            Some(on_send) => ConnectionMessage::OnSend(subscription_id, on_send.into()),
            None => ConnectionMessage::RemoveOnSend(subscription_id),
        })
        .await
        .map_err(|_| SubscriptionError::ConnectionError)
    }

    async fn disconnect(&self, subscription_id: SubscriptionId) -> SubscriptionResult {
        self.send(ConnectionMessage::Disconnect(subscription_id))
            .await
            .map_err(|_| SubscriptionError::ConnectionError)
    }
}

impl<T> Subscriber<T> {
    pub const SUBSCRIPTION_CONNECTION_TIMEOUT: Duration = SUBSCRIPTION_CONNECTION_TIMEOUT;

    pub fn subscriptions_count(&self) -> SubscriptionSeqNo {
        self.subscriptions_count.load(Ordering::Relaxed)
    }

    pub async fn subscribe(&self) -> SubscriptionResult<PacketReceiver<T>> {
        self.subscribe_with(SubscriptionOptions::default()).await
    }

    pub async fn subscribe_with(&self, options: SubscriptionOptions<T>) -> SubscriptionResult<PacketReceiver<T>> {
        self.connection_sender
            .subscribe(self.next_subscription_id(), options.filter, options.on_send)
            .await
    }

    pub async fn filter(&self, filter: Option<Filter<T>>) -> SubscriptionResult {
        self.connection_sender.filter(*self.id, filter).await
    }

    pub async fn set_filter(&self, filter: Filter<T>) -> SubscriptionResult {
        self.filter(Some(filter)).await
    }

    pub async fn remove_filter(&self) -> SubscriptionResult {
        self.filter(None).await
    }

    pub async fn subfilter(&self, seq_no: SubscriptionSeqNo, filter: Option<Filter<T>>) -> SubscriptionResult {
        if seq_no >= self.subscriptions_count() {
            return Err(SubscriptionError::InvalidSubscriptionSeqNo);
        }
        let id = SubscriptionId {
            subscriber_id: self.id,
            seq_no: Ro::new(seq_no),
        };
        self.connection_sender.subfilter(id, filter).await
    }

    pub async fn set_subfilter(&self, seq_no: SubscriptionSeqNo, filter: Filter<T>) -> SubscriptionResult {
        self.subfilter(seq_no, Some(filter)).await
    }

    pub async fn remove_subfilter(&self, seq_no: SubscriptionSeqNo) -> SubscriptionResult {
        self.subfilter(seq_no, None).await
    }

    pub async fn on_send(&self, seq_no: SubscriptionSeqNo, on_send: impl Into<OnTransfer<T>>) -> SubscriptionResult {
        if seq_no >= self.subscriptions_count() {
            return Err(SubscriptionError::InvalidSubscriptionSeqNo);
        }
        let id = SubscriptionId {
            subscriber_id: self.id,
            seq_no: Ro::new(seq_no),
        };
        self.connection_sender.on_send(id, Some(on_send)).await
    }

    pub async fn disconnect(&self, seq_no: SubscriptionSeqNo) -> SubscriptionResult {
        if seq_no >= self.subscriptions_count() {
            return Err(SubscriptionError::InvalidSubscriptionSeqNo);
        }
        self.connection_sender
            .disconnect(SubscriptionId {
                subscriber_id: self.id,
                seq_no: Ro::new(seq_no),
            })
            .await
    }

    fn next_subscription_id(&self) -> SubscriptionId {
        SubscriptionId {
            subscriber_id: self.id,
            seq_no: Ro::new(self.subscriptions_count.fetch_add(1, Ordering::Relaxed)),
        }
    }
}

impl<T> SubscriptionOptions<T> {
    pub fn new() -> Self {
        Self {
            filter: None,
            on_send: None,
        }
    }

    pub fn filter(filter: Filter<T>) -> Self {
        Self {
            filter: Some(filter),
            on_send: None,
        }
    }

    pub fn on_send(on_send: impl Into<OnTransfer<T>>) -> Self {
        Self {
            filter: None,
            on_send: Some(on_send.into()),
        }
    }
}

impl<T> Default for SubscriptionOptions<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Filter<T> {
    pub fn cb(f: impl Fn(&T) -> bool + Send + Sync + 'static) -> Self {
        Filter::Sync(Box::new(f))
    }

    pub fn fut<Fut>(f: impl Fn(&T) -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = bool> + Send + 'static,
    {
        Filter::Async(Box::new(move |x| Box::pin(f(x))))
    }
}

impl<T> From<Filter<T>> for FilterInner<T> {
    fn from(value: Filter<T>) -> Self {
        match value {
            Filter::Sync(x) => FilterInner::Sync(Arc::from(x)),
            Filter::Async(x) => FilterInner::Async(Arc::from(x)),
        }
    }
}

impl<T> OnTransfer<T> {
    pub fn new(f: impl Fn(&mut T) + Send + Sync + 'static) -> Self {
        Self(Box::new(f))
    }
}

impl<T, F: Into<OnTransfer<T>>> From<F> for OnTransferInner<T> {
    fn from(value: F) -> Self {
        Self(Arc::from(value.into().0))
    }
}

impl<T, F: Fn(&mut T) + Send + Sync + 'static> From<F> for OnTransfer<T> {
    fn from(value: F) -> Self {
        OnTransfer(Box::new(value))
    }
}

impl<T> PacketReceiver<T> {
    pub fn destruct(self) -> (SubscriptionId, Receiver<Packet<T>>) {
        (*self.subscription_id, self.receiver)
    }

    pub fn into_receiver(self) -> Receiver<Packet<T>> {
        self.receiver
    }

    #[cfg(feature = "tokio-stream")]
    pub fn into_stream(self) -> ReceiverStream<Packet<T>> {
        ReceiverStream::new(self.receiver)
    }
}

impl<T> PacketSender<T> {
    pub fn destruct(self) -> (SubscriptionId, Sender<Packet<T>>) {
        (*self.subscription_id, self.sender)
    }

    pub fn into_sender(self) -> Sender<Packet<T>> {
        self.sender
    }
}

impl<T> Deref for PacketReceiver<T> {
    type Target = Receiver<Packet<T>>;
    fn deref(&self) -> &Self::Target {
        &self.receiver
    }
}

impl<T> DerefMut for PacketReceiver<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.receiver
    }
}

impl<T> Deref for PacketSender<T> {
    type Target = Sender<Packet<T>>;
    fn deref(&self) -> &Self::Target {
        &self.sender
    }
}

impl<T> DerefMut for PacketSender<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sender
    }
}

impl Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.subscriber_id, self.seq_no)
    }
}

impl<T> Packet<T> {
    pub fn filter(&self, f: impl Fn(&T) -> bool) -> bool {
        match self {
            Packet::Data(data) => f(data),
            _ => true,
        }
    }

    pub async fn filter_async<'a, F>(&'a self, f: impl Fn(&'a T) -> F) -> bool
    where
        F: Future<Output = bool> + 'a,
    {
        match self {
            Packet::Data(data) => f(data).await,
            _ => true,
        }
    }

    fn on_transfer(&mut self, f: &OnTransferInner<T>) {
        if let Packet::Data(data) = self {
            f.0(data);
        }
    }
}

impl<T: Clone + Debug, const N: usize> Dispatcher<T, N> {
    pub const fn subscribers_count(&self) -> usize {
        N
    }

    pub fn prepare<'a, 'b>(&'a mut self, subscriber: &'b Subscriber<T>) -> PreSubscriber<'a, 'b, T, N> {
        PreSubscriber {
            dispatcher: self,
            subscriber,
        }
    }

    pub fn on_dispatch(&mut self, on_dispatch: Option<impl Into<OnTransfer<T>>>) {
        self.on_dispatch = on_dispatch.map(|x| OnTransferInner::from(x.into()));
    }

    fn broadcast_packet(&self, mut packet: Packet<T>) -> impl Future<Output = Vec<()>> {
        let disconnect_sender = self.disconnection_notifier.0.clone();
        let senders = self.data_senders.values().map(Clone::clone).collect::<Box<[_]>>();
        let log_prefix = self.log_prefix.to_string();
        // there are a few subscribers, so it's ok to clone
        let filters = Rc::new(self.filters.clone());
        if let Some(on_dispatch) = &self.on_dispatch {
            packet.on_transfer(on_dispatch);
        }
        let packet = Rc::new(packet);
        join_all(
            senders
                .iter()
                .filter_map(move |sender| match filters.get(&sender.subscription_id.subscriber_id) {
                    Some(filter) => match filter {
                        FilterInner::Sync(f) => {
                            let f = f.as_ref();
                            match packet.filter(f) {
                                true => Some((sender, Rc::clone(&packet), None)),
                                false => None,
                            }
                        },
                        FilterInner::Async(f) => Some((sender, Rc::clone(&packet), Some(Arc::clone(f)))),
                    },
                    None => Some((sender, Rc::clone(&packet), None)),
                })
                .filter_map(|(sender, packet, filter)| match sender.filter {
                    Some(ref sub_filter) => match sub_filter {
                        FilterInner::Sync(f) => match packet.filter(f.as_ref()) {
                            true => Some((sender, packet, filter, None)),
                            false => None,
                        },
                        FilterInner::Async(f) => Some((sender, packet, filter, Some(Arc::clone(f)))),
                    },
                    None => Some((sender, packet, filter, None)),
                })
                .map(|(sender, packet, filter, sub_filter)| {
                    let packet = Rc::try_unwrap(packet).unwrap_or_else(|x| Packet::clone(&x));
                    let sender = sender.clone();
                    let disconnect_sender = disconnect_sender.clone();
                    let log_prefix = log_prefix.clone();
                    async move {
                        if let Some(f) = filter {
                            if !packet.filter_async(f.as_ref()).await {
                                return;
                            }
                        }
                        if let Some(f) = sub_filter {
                            if !packet.filter_async(f.as_ref()).await {
                                return;
                            }
                        }

                        let mut packet = packet;
                        if let Some(on_send) = &sender.on_send {
                            packet.on_transfer(on_send);
                        }

                        if sender.send(packet).await.is_err() {
                            tracing::error!(
                                "{}: subscription '{}' closed without sending 'Disconnect' message",
                                log_prefix,
                                *sender.subscription_id
                            );
                            disconnect_sender
                                .send(*sender.subscription_id)
                                .expect("unreachable: disconnect_sender can't be closed");
                        }
                    }
                }),
        )
    }

    pub async fn run_with_shutdown(mut self, shutdown_signal: impl Future<Output = ()> + Send) {
        let pfx = &self.log_prefix;
        tracing::info!("{pfx}: run");
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(1));
        let mut heartbeat_counter = 0;
        let mut data_counter = 0usize;
        let mut last_heartbeat_data_counter = 0usize;
        let mut shutdown_signal = std::pin::pin!(shutdown_signal);
        let mut connection_sender_stub: Option<Sender<ConnectionMessage<T>>> = None;
        loop {
            tokio::select! {
                biased;
                _ = &mut shutdown_signal => {
                    tracing::info!("{pfx}: shutting down (signal received)");
                    tracing::info!("{pfx}: sending 'Disconnect' signal to all subscribers");
                    self.broadcast_packet(Packet::Disconnect).await;
                    return;
                },
                id = self.disconnection_notifier.1.recv() => match id {
                    Some(id) => {
                        tracing::info!("{pfx}: disconnected subscription '{id}'");
                        self.data_senders.remove(&id);
                    },
                    None => tracing::error!("'Disconnect' signal receiver closed"),
                },
                conn = self.connection_receiver.recv() => match conn {
                    Some(ConnectionMessage::Connect(sender)) => {
                        tracing::debug!("{pfx}: new subscription '{}' established", sender.subscription_id);
                        self.data_senders.insert(*sender.subscription_id, sender);
                    },
                    Some(ConnectionMessage::Disconnect(id)) => {
                        tracing::debug!("{pfx}: disconnected subscription '{id}'");
                        self.data_senders.remove(&id);
                    },
                    Some(ConnectionMessage::Filter(id, filter)) => {
                        tracing::info!("{pfx}: set filter for subscriber id = {id}");
                        if self.filters.insert(id, FilterInner::from(filter)).is_some() {
                            tracing::warn!("{pfx}: filter for subscriber id = {id} replaced an existed filter");
                        }
                    },
                    Some(ConnectionMessage::RemoveFilter(id)) => {
                        tracing::info!("{pfx}: remove filter of subscriber id = {id}");
                        self.filters.remove(&id);
                    },
                    Some(ConnectionMessage::SubFilter(id, filter)) => {
                        tracing::info!("{pfx}: set filter for subscription '{id}'");
                        if let Some(sender) = self.data_senders.get_mut(&id) {
                            sender.filter = Some(FilterInner::from(filter));
                        }
                    },
                    Some(ConnectionMessage::RemoveSubFilter(id)) => {
                        tracing::info!("{pfx}: remove filter from subscription '{id}'");
                        if let Some(sender) = self.data_senders.get_mut(&id) {
                            sender.filter = None;
                        }
                    },
                    Some(ConnectionMessage::OnSend(id, on_send)) => {
                        tracing::info!("{pfx}: set an 'on_send' handler for the subscription '{id}'");
                        if let Some(sender) = self.data_senders.get_mut(&id) {
                            sender.on_send = Some(OnTransferInner::from(on_send));
                        }
                    },
                    Some(ConnectionMessage::RemoveOnSend(id)) => {
                        tracing::info!("{pfx}: remove 'on_send' from the subscription '{id}'");
                        if let Some(sender) = self.data_senders.get_mut(&id) {
                            sender.on_send = None;
                        }
                    }
                    None => {
                        if N > 0 {
                            tracing::error!("{pfx}: connection receiver closed");
                            return;
                        } else if connection_sender_stub.is_none() {
                            tracing::debug!("{pfx}: no subscribers, stub connection receiver");
                            let (sender, receiver) = channel(1);
                            self.connection_receiver = receiver;
                            connection_sender_stub = Some(sender);
                        }
                    },
                },
                x = self.data_receiver.recv() => if let Some(data) = x {
                    if N > 0 {
                        data_counter += 1;
                        if data_counter == 1 || data_counter.is_multiple_of(100) {
                            tracing::debug!("{pfx}: received data x {data_counter}");
                        }
                        self.broadcast_packet(Packet::Data(data)).await;
                    } else {
                        tracing::trace!("{pfx}: not subscribers, drain data");
                    }
                },
                _ = heartbeat_interval.tick() => {
                    heartbeat_counter += 1;
                    if N > 0 {
                        if last_heartbeat_data_counter != data_counter {
                            last_heartbeat_data_counter = data_counter;
                            tracing::trace!("{pfx}: sent heartbeat = {heartbeat_counter}, data received x {data_counter} times");
                        }
                        self.broadcast_packet(Packet::Heartbeat(heartbeat_counter)).await;
                    } else if heartbeat_counter == 1 || heartbeat_counter % 100 == 0 {
                        tracing::trace!("{pfx}: no subscribers, ignore heartbeat = {heartbeat_counter}");
                    }
                },
            }
        }
    }

    pub fn run(self) -> impl Future<Output = ()> {
        self.run_with_shutdown(std::future::pending())
    }
}

impl<'a, T, const N: usize> PreSubscriber<'a, '_, T, N> {
    pub fn subscribe(&'a mut self) -> PacketReceiver<T> {
        self.subscribe_with(SubscriptionOptions::default())
    }

    pub fn subscribe_with(&'a mut self, options: SubscriptionOptions<T>) -> PacketReceiver<T> {
        let id = self.subscriber.next_subscription_id();
        let (sender, receiver) = packet_channel(id, options.filter, options.on_send);
        self.dispatcher.data_senders.entry(id).or_insert(sender);
        receiver
    }

    pub fn filter(&'a mut self, filter: Option<Filter<T>>) {
        match filter {
            Some(filter) => {
                self.dispatcher
                    .filters
                    .insert(*self.subscriber.id, FilterInner::from(filter));
            },
            None => {
                if self.dispatcher.filters.contains_key(&self.subscriber.id) {
                    self.dispatcher.filters.remove(&self.subscriber.id);
                }
            },
        }
    }

    pub fn set_filter(&'a mut self, filter: Filter<T>) {
        self.filter(Some(filter));
    }

    pub fn remove_filter(&'a mut self) {
        self.filter(None);
    }

    pub fn subfilter(&'a mut self, seq_no: SubscriptionSeqNo, subfilter: Option<Filter<T>>) -> bool {
        self.dispatcher
            .data_senders
            .get_mut(&SubscriptionId::new(*self.subscriber.id, seq_no))
            .map(|sender| sender.filter = subfilter.map(FilterInner::from))
            .is_some()
    }

    pub fn set_subfilter(&'a mut self, seq_no: SubscriptionSeqNo, subfilter: Filter<T>) -> bool {
        self.subfilter(seq_no, Some(subfilter))
    }

    pub fn remove_subfilter(&'a mut self, seq_no: SubscriptionSeqNo) -> bool {
        self.subfilter(seq_no, None)
    }

    pub fn on_send(&'a mut self, seq_no: SubscriptionSeqNo, on_send: Option<impl Into<OnTransfer<T>>>) -> bool {
        self.dispatcher
            .data_senders
            .get_mut(&SubscriptionId::new(*self.subscriber.id, seq_no))
            .map(|sender| sender.on_send = on_send.map(|x| OnTransferInner::from(x.into())))
            .is_some()
    }

    pub fn set_on_send(&'a mut self, seq_no: SubscriptionSeqNo, on_send: impl Into<OnTransfer<T>>) -> bool {
        self.on_send(seq_no, Some(on_send))
    }

    pub fn remove_on_send(&'a mut self, seq_no: SubscriptionSeqNo) -> bool {
        self.on_send(seq_no, None::<OnTransfer<T>>)
    }
}

impl SubscriptionId {
    pub fn new(subscriber: SubscriberId, seq_no: SubscriptionSeqNo) -> Self {
        Self {
            subscriber_id: Ro::new(subscriber),
            seq_no: Ro::new(seq_no),
        }
    }

    pub fn key(self) -> SubscriptionKey {
        ((*self.subscriber_id as u64) << 32) | (*self.seq_no as u64)
    }

    pub fn atomic_key(self) -> SubscriptionAtomicKey {
        AtomicU64::new(self.key())
    }
}

pub type SubscriptionResult<T = ()> = Result<T, SubscriptionError>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SubscriptionError {
    #[snafu(display("subscription connection error"))]
    ConnectionError,
    #[snafu(display("invalid subscriber id = {id}"))]
    InvalidSubscriberId { id: SubscriberId },
    #[snafu(display("subscription invalid subscription seq_no"))]
    InvalidSubscriptionSeqNo,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::init_test;
    use futures_util::future::{join, join3};
    use std::sync::{atomic::AtomicU32, Arc};

    #[derive(Debug, Clone, Copy)]
    struct TestData(u32);

    #[test]
    fn test_u64_key() {
        assert_eq!(SubscriptionId::new(1, 1).key(), 0x0000_0001_0000_0001);
        assert_eq!(SubscriptionId::new(255, 255).key(), 0x0000_00FF_0000_00FF);
        assert_eq!(SubscriptionId::new(256, 256).key(), 0x0000_0100_0000_0100);
    }

    #[tokio::test]
    async fn test_sub() {
        let PubSub {
            dispatcher: _,
            publisher: _,
            subscribers: [subscriber],
        } = PubSub::<TestData>::new("test", 1024);

        assert_eq!(subscriber.subscriptions_count(), 0);
        _ = subscriber.subscribe().await;
        assert_eq!(subscriber.subscriptions_count(), 1);
    }

    #[tokio::test]
    async fn test_dispatcher() {
        init_test();
        let PubSub {
            dispatcher,
            publisher,
            subscribers: [sub1, sub2],
        } = PubSub::<TestData, 2>::new("test", 1024);

        let cnt_src = Arc::new(AtomicU32::new(0));

        // data generator
        let cnt = cnt_src.clone();
        tokio::spawn(async move {
            loop {
                let data = TestData(cnt.load(Ordering::Relaxed));
                tracing::info!("data_sender: sending data = {data:?}");
                _ = publisher.send(data).await;
                cnt.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        let mut tasks = [sub1, sub2].into_iter().enumerate().map(|(sub_index, sub)| {
            let sub = sub;
            let cnt = cnt_src.clone();
            tracing::info!("spawn task of subscriber {}", sub.id);
            tokio::spawn(async move {
                tracing::debug!("inside task: sub id = '{}'", sub.id);
                let mut conn1 = sub.subscribe().await.unwrap();
                let mut conn2 = sub.subscribe().await.unwrap();
                let mut conn3 = sub.subscribe().await.unwrap();
                assert_eq!(*conn1.subscription_id, SubscriptionId {
                    subscriber_id: Ro::new(sub_index as u32),
                    seq_no: Ro::new(0)
                });
                assert_eq!(*conn2.subscription_id, SubscriptionId {
                    subscriber_id: Ro::new(sub_index as u32),
                    seq_no: Ro::new(1)
                });
                assert_eq!(*conn3.subscription_id, SubscriptionId {
                    subscriber_id: Ro::new(sub_index as u32),
                    seq_no: Ro::new(2)
                });
                fn log_flt(sub_id: SubscriberId, seq_no: SubscriptionSeqNo, data: &TestData) -> bool {
                    let seq_mod = seq_no + 1;
                    let res = data.0.is_multiple_of(seq_mod);
                    match res {
                        true => tracing::info!("filter: sub '{sub_id}:{seq_no}'  passed {} % {} == 0", data.0, seq_mod),
                        false => {
                            tracing::info!("filter: sub '{sub_id}:{seq_no}' dropped {} % {} == 0", data.0, seq_mod)
                        },
                    }
                    res
                }
                let (seq_no1, seq_no2, seq_no3) = (
                    *conn1.subscription_id.seq_no,
                    *conn2.subscription_id.seq_no,
                    *conn3.subscription_id.seq_no,
                );
                sub.set_subfilter(seq_no1, Filter::cb(move |data| log_flt(*sub.id, seq_no1, data)))
                    .await
                    .unwrap();
                sub.set_subfilter(seq_no2, Filter::cb(move |data| log_flt(*sub.id, seq_no2, data)))
                    .await
                    .unwrap();
                sub.set_subfilter(seq_no3, Filter::cb(move |data| log_flt(*sub.id, seq_no3, data)))
                    .await
                    .unwrap();

                #[derive(Debug)]
                enum HandlerStatus {
                    Continue,
                    Break,
                }
                fn handle_packet(
                    subscription_id: SubscriptionId,
                    packet: &Packet<TestData>,
                    cnt: &AtomicU32,
                ) -> HandlerStatus {
                    match packet {
                        Packet::Heartbeat(n) => {
                            tracing::info!("subscription {subscription_id}: received heartbeat = {n}");
                        },
                        Packet::Data(data) => {
                            assert!(data.0 <= cnt.load(Ordering::Relaxed));
                            tracing::info!("subscription {subscription_id}: received data = {data:?}",);
                        },
                        Packet::Disconnect => {
                            tracing::info!("subscription {subscription_id}: received disconnection notification");
                            return HandlerStatus::Break;
                        },
                    }
                    HandlerStatus::Continue
                }

                let mut status = HandlerStatus::Continue;
                let mut break_cnt = 0;
                loop {
                    if let HandlerStatus::Break = status {
                        break_cnt += 1;
                        if break_cnt >= 3 {
                            break;
                        }
                        status = HandlerStatus::Continue;
                    }
                    tokio::select! {
                        x = conn1.recv() => if let Some(data) = x {
                            status = handle_packet(*conn1.subscription_id, &data, &cnt);
                        },
                        x = conn2.recv() => if let Some(data) = x {
                            status = handle_packet(*conn2.subscription_id, &data, &cnt);
                        },
                        x = conn3.recv() => if let Some(data) = x {
                            status = handle_packet(*conn3.subscription_id, &data, &cnt);
                        },
                    }
                }
            })
        });

        let [join1, join2] = std::array::from_fn(|_| tasks.next().unwrap());
        let bridge_future = dispatcher.run_with_shutdown(tokio::time::sleep(Duration::from_millis(15)));

        let (res1, res2, _) = join3(join1, join2, bridge_future).await;
        res1.unwrap();
        res2.unwrap();
    }

    #[tokio::test]
    async fn zero_subscribers_should_be_valid() {
        init_test();
        let PubSub {
            subscribers: _,
            publisher,
            dispatcher,
        } = PubSub::<TestData, 0>::new("zero", 2);

        let cnt = Arc::new(AtomicU32::new(0));

        // data generator
        let data_generator_task = tokio::spawn({
            let cnt = cnt.clone();
            async move {
                loop {
                    let current = cnt.load(Ordering::Relaxed);
                    if current >= 15 {
                        break;
                    }
                    let data = TestData(current);
                    tracing::info!("data_sender: sending data = {data:?}");
                    if let Err(e) = publisher.send(data).await {
                        tracing::info!("data_sender: {e}");
                        break;
                    }
                    cnt.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });

        let dispatcher_task = tokio::spawn(dispatcher.run_with_shutdown(tokio::time::sleep(Duration::from_millis(15))));

        let (res1, res2) = join(data_generator_task, dispatcher_task).await;
        res1.unwrap();
        res2.unwrap();

        assert!(cnt.load(Ordering::Relaxed) > 3)
    }
}
