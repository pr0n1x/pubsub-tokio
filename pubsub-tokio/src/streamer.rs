pub mod delay;

use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
};

use futures_core::Stream;
use pin_project_lite::pin_project;
use tokio_util::sync::ReusableBoxFuture;

pub trait NextStreamItem<Item>: Send {
    fn next_stream_item(&mut self) -> impl Future<Output = Option<Item>> + Send + '_;
}

pub trait Streamer<'a, T> {
    fn stream(&'a mut self) -> impl Stream<Item = T> + Send + 'a;
}

pub trait IntoStream<T> {
    fn into_stream(self) -> impl Stream<Item = T> + Send;
}

/// This extension trait is used only for inference in an IDE,
/// in cases with a lot of nested stream-combinators.
pub trait InferStream: Stream {
    fn infer_stream(self) -> impl Stream<Item = Self::Item>;
}

impl<S: Stream> InferStream for S {
    fn infer_stream(self) -> impl Stream<Item = S::Item> {
        self
    }
}

enum StreamStage<R, T> {
    Init(R),
    Wip(R, T),
    Fin,
}

pub struct ReaderRefStream<'a, Item, Reader: NextStreamItem<Item>> {
    inner: ReusableBoxFuture<'a, StreamStage<Reader, Item>>,
    completed: bool,
}

pub struct ReaderStream<Item, Reader: NextStreamItem<Item>>(ReaderRefStream<'static, Item, Reader>);

pin_project! {
    /// A stream that notifies a callback when the stream ends.
    pub struct CallbackOnEndStream<S, F> {
        #[pin]
        stream: S,
        callback: Option<F>,
    }
}

pin_project! {
    /// A stream that notifies an async callback when the stream ends.
    pub struct NotifyOnEndStream<S, F> {
        #[pin]
        stream: S,
        #[pin]
        future: F,
        stage: NotifyOnEndStreamStage,
    }
}

pin_project! {
    pub struct CallbackOnEachItemStream<S, F> {
        #[pin]
        stream: S,
        callback: Option<F>,
    }
}

pin_project! {
    pub struct NotifyOnEachItemStream<S, F, Fut> {
        #[pin]
        stream: S,
        callback: F,
        #[pin]
        stage: NotifyOnEachItemStreamStage<Fut>,
    }
}

pin_project! {
    pub struct MapAsyncStream<S, F, Fut> {
        #[pin]
        stream: S,
        callback: F,
        #[pin]
        stage: AsyncMapStreamStage<Fut>,
    }
}

pin_project! {
    #[project = AsyncMapStreamStageProjection]
    pub enum AsyncMapStreamStage<Fut> {
        Stream,
        Future { #[pin] future: Fut },
        Done,
    }
}

pin_project! {
    /// It different from tokio_stream::StreamExt::filter_map
    /// in that it returns Pending when the callback returns None.
    /// Theoretically, when in a stream (of type tokio_stream::stream_ext::FilterMap)
    /// we have a lot of filtered-out items (Fn(S::Item) -> None),
    /// it can lead to busy-looping.
    pub struct FilterMapRelaxedStream<S, F> {
        #[pin]
        stream: S,
        callback: F,
        exhausted: bool,
    }
}

pin_project! {
    pub struct ZippedPairStream<S, T> {
        #[pin]
        stream: S,
        second: Option<T>,
    }
}

pub enum Pair<T> {
    Single(T),
    Pair(T, T),
}

enum NotifyOnEndStreamStage {
    Stream,
    Notify,
    Fin,
}

pin_project! {
    #[project = NotifyOnEachItemStreamStageProjection]
    enum NotifyOnEachItemStreamStage<Fut> {
        Stream,
        Notify { #[pin] future: Fut },
        Fin,
    }
}

pub trait OnEndStream: Stream {
    fn on_end_cb<F>(self, cb: F) -> CallbackOnEndStream<Self, F>
    where
        F: FnOnce(),
        Self: Sized,
    {
        CallbackOnEndStream::new(self, cb)
    }

    fn on_end<F>(self, fut: F) -> NotifyOnEndStream<Self, F>
    where
        F: Future<Output = ()> + Send + 'static,
        Self: Sized,
    {
        NotifyOnEndStream::new(self, fut)
    }
}

pub trait OnEachStream: Stream {
    fn on_each_cb<F>(self, cb: F) -> CallbackOnEachItemStream<Self, F>
    where
        F: FnMut(&Self::Item) + Send + 'static,
        Self: Sized,
    {
        CallbackOnEachItemStream::new(self, cb)
    }

    fn on_each<F, Fut>(self, cb: F) -> NotifyOnEachItemStream<Self, F, Fut>
    where
        F: FnMut(&Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
        Self: Sized,
    {
        NotifyOnEachItemStream::new(self, cb)
    }
}

pub trait MapStream: Stream {
    fn map_async<F, Fut, T>(self, f: F) -> MapAsyncStream<Self, F, Fut>
    where
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        Self: Sized,
    {
        MapAsyncStream::new(self, f)
    }

    fn filter_map_relaxed<F, T>(self, f: F) -> FilterMapRelaxedStream<Self, F>
    where
        F: FnMut(Self::Item) -> Option<T> + Send + 'static,
        Self: Sized,
    {
        FilterMapRelaxedStream::new(self, f)
    }
}

pub trait ZipPairStream<T>: Stream<Item = Pair<T>> {
    fn zip_pair(self) -> ZippedPairStream<Self, T>
    where
        Self: Sized,
    {
        ZippedPairStream::new(self)
    }
}

impl<S: Stream> OnEndStream for S {}
impl<S: Stream> OnEachStream for S {}
impl<S: Stream> MapStream for S {}
impl<T, S: Stream<Item = Pair<T>>> ZipPairStream<T> for S {}

impl<'a, T, F, S> Streamer<'a, T> for F
where
    F: FnMut() -> S,
    S: Stream<Item = T> + Send + 'a,
{
    fn stream(&'a mut self) -> impl Stream<Item = T> + Send + 'a {
        self()
    }
}

impl<T, F, S> IntoStream<T> for F
where
    F: FnOnce() -> S,
    S: Stream<Item = T> + Send,
{
    fn into_stream(self) -> impl Stream<Item = T> + Send {
        self()
    }
}

impl<T, R: NextStreamItem<T>> StreamStage<R, T> {
    pub async fn next_future(mut reader: R) -> Self {
        match reader.next_stream_item().await {
            Some(item) => Self::Wip(reader, item),
            None => Self::Fin,
        }
    }
}

impl<'a, Item, Reader: NextStreamItem<Item> + 'a> ReaderRefStream<'a, Item, Reader> {
    pub fn new(reader: Reader) -> Self {
        Self {
            inner: ReusableBoxFuture::new(async move { StreamStage::Init(reader) }),
            completed: false,
        }
    }
}

impl<Item, Reader: NextStreamItem<Item> + 'static> ReaderStream<Item, Reader> {
    pub fn new(reader: Reader) -> Self {
        Self(ReaderRefStream::<'static, Item, Reader>::new(reader))
    }
}

impl<'a, Item, Reader> Stream for ReaderRefStream<'a, Item, Reader>
where
    Item: 'a,
    Reader: NextStreamItem<Item> + 'a,
{
    type Item = Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = match self.completed {
            true => Poll::Ready(None),
            false => match ready!(self.inner.poll(cx)) {
                StreamStage::Init(reader) => {
                    self.inner.set(StreamStage::next_future(reader));
                    Poll::Pending
                },
                StreamStage::Wip(reader, item) => {
                    self.inner.set(StreamStage::next_future(reader));
                    Poll::Ready(Some(item))
                },
                StreamStage::Fin => {
                    self.completed = true;
                    Poll::Ready(None)
                },
            },
        };
        cx.waker().wake_by_ref();
        poll
    }
}

impl<Item, Reader> Stream for ReaderStream<Item, Reader>
where
    Item: 'static,
    Reader: NextStreamItem<Item> + 'static,
{
    type Item = Item;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pinned = Pin::new(&mut self.0);
        pinned.poll_next(cx)
    }
}

impl<T, S, F> CallbackOnEndStream<S, F>
where
    S: Stream<Item = T>,
    F: FnOnce(),
{
    pub fn new(stream: S, callback: F) -> Self {
        Self {
            stream,
            callback: Some(callback),
        }
    }
}

impl<T, S, F> NotifyOnEndStream<S, F>
where
    S: Stream<Item = T>,
    F: Future<Output = ()> + Send + 'static,
{
    pub fn new(stream: S, future: F) -> Self {
        Self {
            stream,
            future,
            stage: NotifyOnEndStreamStage::Stream,
        }
    }

    pub fn pin(self) -> Pin<Box<NotifyOnEndStream<S, F>>> {
        Box::pin(self)
    }
}

impl<T, S, F> CallbackOnEachItemStream<S, F>
where
    S: Stream<Item = T>,
    F: FnMut(&S::Item) + Send + 'static,
{
    pub fn new(stream: S, callback: F) -> Self {
        Self {
            stream,
            callback: Some(callback),
        }
    }

    pub fn pin(self) -> Pin<Box<Self>> {
        Box::pin(self)
    }
}

impl<T, S, F, Fut> NotifyOnEachItemStream<S, F, Fut>
where
    S: Stream<Item = T>,
    F: FnMut(&S::Item) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    pub fn new(stream: S, callback: F) -> Self {
        Self {
            stream,
            callback,
            stage: NotifyOnEachItemStreamStage::Stream,
        }
    }

    pub fn pin(self) -> Pin<Box<Self>> {
        Box::pin(self)
    }
}

impl<T, S, F> Stream for CallbackOnEndStream<S, F>
where
    S: Stream<Item = T>,
    F: FnOnce(),
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.as_mut().project();
        // // fuse-like protection, do we need it?
        // if projection.callback.is_none() {
        //     return Poll::Ready(None);
        // }
        let poll = projection.stream.poll_next(cx);
        if let Poll::Ready(None) = poll {
            if let Some(callback) = projection.callback.take() {
                callback();
            }
        }
        poll
    }
}

impl<T, S, F> Stream for NotifyOnEndStream<S, F>
where
    S: Stream<Item = T>,
    F: Future<Output = ()>,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.stage {
            NotifyOnEndStreamStage::Stream => match self.as_mut().project().stream.poll_next(cx) {
                Poll::Ready(Some(x)) => Poll::Ready(Some(x)),
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    *self.as_mut().project().stage = NotifyOnEndStreamStage::Notify;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
            },
            NotifyOnEndStreamStage::Notify => {
                let poll = self.as_mut().project().future.poll(cx);
                match poll {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(()) => {
                        *self.as_mut().project().stage = NotifyOnEndStreamStage::Fin;
                        Poll::Ready(None)
                    },
                }
            },
            NotifyOnEndStreamStage::Fin => Poll::Ready(None),
        }
    }
}

impl<T, S, F> Stream for CallbackOnEachItemStream<S, F>
where
    S: Stream<Item = T>,
    F: FnMut(&S::Item),
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.as_mut().project().stream.poll_next(cx);
        if let Poll::Ready(Some(item)) = &poll {
            if let Some(callback) = self.as_mut().project().callback {
                callback(item);
            }
        }
        poll
    }
}

impl<T, S, F, Fut> Stream for NotifyOnEachItemStream<S, F, Fut>
where
    S: Stream<Item = T>,
    F: FnMut(&S::Item) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().project().stage.project() {
            NotifyOnEachItemStreamStageProjection::Stream => match self.as_mut().project().stream.poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    let mut projection = self.project();
                    projection.stage.set(NotifyOnEachItemStreamStage::Notify {
                        future: (projection.callback)(&item),
                    });
                    Poll::Ready(Some(item))
                },
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    self.project().stage.set(NotifyOnEachItemStreamStage::Fin);
                    Poll::Ready(None)
                },
            },
            NotifyOnEachItemStreamStageProjection::Notify { future } => {
                if future.poll(cx).is_ready() {
                    self.project().stage.set(NotifyOnEachItemStreamStage::Stream);
                    cx.waker().wake_by_ref();
                }
                Poll::Pending
            },
            NotifyOnEachItemStreamStageProjection::Fin => Poll::Ready(None),
        }
    }
}

impl<S, F, Fut, T> MapAsyncStream<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    pub fn new(stream: S, callback: F) -> Self {
        Self {
            stream,
            callback,
            stage: AsyncMapStreamStage::Stream,
        }
    }
}

impl<S, F, Fut, T> Stream for MapAsyncStream<S, F, Fut>
where
    S: Stream,
    F: FnMut(S::Item) -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
{
    type Item = Fut::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut projection = self.as_mut().project();
        match projection.stage.as_mut().project() {
            AsyncMapStreamStageProjection::Stream => match projection.stream.poll_next(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Some(item)) => {
                    projection.stage.set(AsyncMapStreamStage::Future {
                        future: (projection.callback)(item),
                    });
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
                Poll::Ready(None) => {
                    projection.stage.set(AsyncMapStreamStage::Done);
                    Poll::Ready(None)
                },
            },
            AsyncMapStreamStageProjection::Future { future } => match future.poll(cx) {
                Poll::Ready(output) => {
                    projection.stage.set(AsyncMapStreamStage::Stream);
                    Poll::Ready(Some(output))
                },
                Poll::Pending => Poll::Pending,
            },
            AsyncMapStreamStageProjection::Done => Poll::Ready(None),
        }
    }
}

impl<S, F, T> FilterMapRelaxedStream<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Option<T>,
{
    pub fn new(stream: S, callback: F) -> Self {
        Self {
            stream,
            callback,
            exhausted: false,
        }
    }
}

impl<S, F, T> Stream for FilterMapRelaxedStream<S, F>
where
    S: Stream,
    F: FnMut(S::Item) -> Option<T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.project();
        // fuse-like protection
        if *projection.exhausted {
            return Poll::Ready(None);
        }
        match projection.stream.poll_next(cx) {
            Poll::Ready(Some(item)) => match (projection.callback)(item) {
                Some(output) => Poll::Ready(Some(output)),
                None => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
            },
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                *projection.exhausted = true;
                Poll::Ready(None)
            },
        }
    }
}

impl<S, T> ZippedPairStream<S, T>
where
    S: Stream<Item = Pair<T>>,
{
    pub fn new(stream: S) -> Self {
        Self { stream, second: None }
    }
}

impl<S, T> Stream for ZippedPairStream<S, T>
where
    S: Stream<Item = Pair<T>>,
{
    type Item = T;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let projection = self.as_mut().project();
        if let Some(second) = projection.second.take() {
            return Poll::Ready(Some(second));
        }
        match projection.stream.poll_next(cx) {
            Poll::Ready(Some(Pair::Single(item))) => Poll::Ready(Some(item)),
            Poll::Ready(Some(Pair::Pair(first, second))) => {
                projection.second.replace(second);
                Poll::Ready(Some(first))
            },
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::testing::init_test;
    use futures::StreamExt;
    use std::sync::{atomic::AtomicU32, Arc};
    use tokio_stream::wrappers::ReceiverStream;

    #[tokio::test]
    async fn should_notify_on_stream_end_using_callback() {
        init_test();
        let cnt = AtomicU32::new(0);
        let mut stream = futures::stream::iter(0..10).on_end_cb(|| {
            cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!("callback-on-end-stream: stream ended")
        });
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        while let Some(item) = stream.next().await {
            tracing::info!("callback-on-end-stream: item #{item}");
        }
        tracing::info!("callback-on-end-stream: out of stream");
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn should_notify_on_stream_end_using_future() {
        init_test();
        let cnt = Arc::new(AtomicU32::new(0));
        let stream = futures::stream::iter(0..10).on_end({
            let cnt = Arc::clone(&cnt);
            async move {
                cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!("notify-on-end-stream: interrupt 1");
                tokio::task::yield_now().await;
                tracing::info!("notify-on-end-stream: interrupt 2");
                tokio::task::yield_now().await;
                tracing::info!("notify-on-end-stream: stream ended")
            }
        });
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        let mut stream = stream.pin();
        while let Some(item) = stream.next().await {
            tracing::info!("notify-on-end-stream item #{item}");
        }
        tracing::info!("notify-on-end-stream: out of stream");
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn should_notify_on_broken_channel_stream() {
        init_test();
        let cnt = Arc::new(AtomicU32::new(0));
        let (sender, receiver) = tokio::sync::mpsc::channel(1);

        let _sender_task = tokio::spawn(async move {
            for i in 0..10 {
                if sender.send(i).await.is_err() {
                    tracing::error!("callback-on-end-channel-stream: channel broken");
                    break;
                }
            }
        });

        let mut stream = ReceiverStream::new(receiver).on_end_cb(|| {
            cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::info!("callback-on-end-channel-stream: stream ended");
        });

        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        let mut i = 0;
        while let Some(item) = stream.next().await {
            tracing::info!("callback-on-end-channel-stream item #{item}");
            i += 1
        }
        assert_eq!(i, 10);
        tracing::info!("callback-on-end-channel-stream: out of stream");
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn should_notify_on_each_stream_item_using_future() {
        init_test();
        let cnt = Arc::new(AtomicU32::new(0));
        let stream = futures::stream::iter(0..10).on_each({
            let cnt = Arc::clone(&cnt);
            move |item| {
                let cnt = Arc::clone(&cnt);
                let item = *item;
                async move {
                    cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tracing::info!("notify-on-each-item-stream: async callback on item = {item}, interrupt 1");
                    tokio::task::yield_now().await;
                    tracing::info!("notify-on-each-item-stream: async callback on item = {item}, interrupt 2");
                }
            }
        });
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        let mut stream = stream.pin();
        while let Some(item) = stream.next().await {
            tracing::info!("notify-on-each-item-stream: item #{item}");
        }
        tracing::info!("notify-on-each-item-stream: out of stream");
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn should_notify_on_each_stream_item_using_callback() {
        init_test();
        let cnt = Arc::new(AtomicU32::new(0));
        let mut stream = futures::stream::iter(0..10).on_each_cb({
            let cnt = cnt.clone();
            move |item| {
                cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tracing::info!("callback-on-each-item-stream: stream item {item} callback")
            }
        });
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 0);
        while let Some(item) = stream.next().await {
            tracing::info!("callback-on-each-item-stream: item #{item}");
        }
        tracing::info!("callback-on-each-item-stream: out of stream");
        assert_eq!(cnt.load(std::sync::atomic::Ordering::Relaxed), 10);
    }

    #[tokio::test]
    async fn should_pull_items_from_pairs_stream() {
        init_test();
        let stream = futures::stream::iter(vec![
            Pair::Pair(1, 2),
            Pair::Single(3),
            Pair::Pair(4, 5),
            Pair::Single(6),
            Pair::Pair(7, 8),
        ]);
        let mut pair_pull = ZippedPairStream::new(stream);
        let mut items = vec![];
        while let Some(item) = pair_pull.next().await {
            items.push(item);
        }
        assert_eq!(items, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }
}
