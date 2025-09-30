use std::{
    future::Future,
    pin::Pin,
    task::{ready, Context, Poll},
    time::Duration,
};

use futures_core::Stream;
use pin_project_lite::pin_project;
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tokio_util::time::DelayQueue;

pub trait DelayStream: Stream {
    fn delay(self, delay: Duration) -> DelayedStream<Self>
    where
        Self: Sized,
        Self::Item: Clone + Send + 'static,
    {
        DelayedStream::new(self, delay)
    }
}

pub trait DelayPartiallyStream<T>: Stream {
    fn delay_partially<F, Fut>(self, should_delay: F) -> PartiallyDelayedStream<Self, F, Fut, T>
    where
        Self: Sized + Send + 'static,
        Self::Item: Send + 'static,
        F: FnMut(Self::Item) -> Fut + Send + 'static,
        Fut: Future<Output = DelayIt<T>> + Send + 'static,
    {
        PartiallyDelayedStream::new(self, should_delay)
    }
}

impl<S: Stream> DelayStream for S {}
impl<S: Stream, T> DelayPartiallyStream<T> for S {}

pin_project! {
    pub struct DelayedStream<S: Stream> {
        #[pin]
        stream: S,
        exhausted: bool,
        buffer: DelayQueue<S::Item>,
        delay: Duration
    }
}

pin_project! {
    pub struct PartiallyDelayedStream<S: Stream, Callback, Fut, T = <S as Stream>::Item> {
        #[pin]
        stream: S,
        should_delay: Callback,
        buffer: DelayQueue<(T, Duration)>,
        #[pin]
        stage: PartiallyDelayedStage<Fut>,
        small_rng: SmallRng,
        _output_item: std::marker::PhantomData<T>,
    }
}

pin_project! {
    #[project = PartiallyDelayedStageProjection]
    #[derive(Default)]
    pub enum PartiallyDelayedStage<Fut> {
        #[default]
        Next,
        Callback { #[pin] fut: Fut },
        Delayed,
        Exhausted,
        Completed,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayIt<T> {
    Pass(T),
    Delay(T, Duration),
}

impl<S: Stream> DelayedStream<S> {
    pub fn new(stream: S, delay: Duration) -> Self {
        Self {
            stream,
            exhausted: false,
            buffer: DelayQueue::new(),
            delay,
        }
    }
}

impl<S> Stream for DelayedStream<S>
where
    S: Stream + Send + 'static,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let delay = self.delay;
        // fuse-like protection
        match self.exhausted {
            true => match ready!(self.as_mut().project().buffer.poll_expired(cx)) {
                Some(delayed) => Poll::Ready(Some(delayed.into_inner())),
                None => Poll::Ready(None),
            },

            false => {
                let next = match self.as_mut().project().stream.poll_next(cx) {
                    Poll::Ready(Some(next)) => {
                        self.as_mut().project().buffer.insert(next, delay);
                        Poll::Ready(Some(()))
                    },
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(None) => {
                        *self.as_mut().project().exhausted = true;
                        Poll::Ready(None)
                    },
                };

                match self.as_mut().project().buffer.poll_expired(cx) {
                    Poll::Ready(Some(delayed)) => Poll::Ready(Some(delayed.into_inner())),
                    Poll::Pending => match next {
                        Poll::Pending | Poll::Ready(None) => Poll::Pending,
                        Poll::Ready(Some(())) => {
                            // If the expiration buffer is sleeping (till next expiration),
                            // we also need to poll the main stream, hence we have to wake up here.
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        },
                    },
                    Poll::Ready(None) => match next {
                        Poll::Ready(Some(())) => {
                            // The expiration buffer is exhausted, but we still have the next item,
                            // therefore, we need to wake up to poll the main stream again.
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        },
                        Poll::Pending => Poll::Pending,
                        Poll::Ready(None) => Poll::Ready(None),
                    },
                }
            },
        }
    }
}

impl<T> DelayIt<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> DelayIt<U> {
        match self {
            DelayIt::Pass(item) => DelayIt::Pass(f(item)),
            DelayIt::Delay(item, delay) => DelayIt::Delay(f(item), delay),
        }
    }

    pub fn into_inner(self) -> T {
        match self {
            DelayIt::Pass(item) => item,
            DelayIt::Delay(item, _) => item,
        }
    }
}

impl<S, Cb, Fut, T> PartiallyDelayedStream<S, Cb, Fut, T>
where
    S: Stream + Send + 'static,
    Cb: (FnMut(S::Item) -> Fut) + Send + 'static,
    Fut: Future<Output = DelayIt<T>> + Send + 'static,
{
    pub fn new(stream: S, should_delay: Cb) -> Self {
        Self {
            stream,
            should_delay,
            buffer: DelayQueue::new(),
            stage: Default::default(),
            small_rng: SmallRng::seed_from_u64(rand::rng().random()),
            _output_item: std::marker::PhantomData,
        }
    }

    pub fn pin(self) -> Pin<Box<PartiallyDelayedStream<S, Cb, Fut, T>>> {
        Box::pin(self)
    }
}

impl<S, Cb, Fut, T> Stream for PartiallyDelayedStream<S, Cb, Fut, T>
where
    S: Stream + Send + 'static,
    Cb: FnMut(S::Item) -> Fut + Send + 'static,
    Fut: Future<Output = DelayIt<T>> + Send + 'static,
{
    type Item = DelayIt<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const PENDING_RETRIES_LIMIT: usize = 10;
        let mut pending_retries = 0;
        loop {
            match (self.as_mut().poll_next_item(cx), &self.stage) {
                (Poll::Ready(Some(item)), _) => return Poll::Ready(Some(item)),
                (Poll::Ready(None), _) => return Poll::Ready(None),
                // there is no need to force retry if the stream is exhausted
                (Poll::Pending, PartiallyDelayedStage::Exhausted) => return Poll::Pending,
                (Poll::Pending, _) => {
                    pending_retries += 1;
                    if pending_retries >= PENDING_RETRIES_LIMIT {
                        return Poll::Pending;
                    }
                },
            }
        }
    }
}

impl<S, Cb, Fut, T> PartiallyDelayedStream<S, Cb, Fut, T>
where
    S: Stream + Send + 'static,
    Cb: FnMut(S::Item) -> Fut + Send + 'static,
    Fut: Future<Output = DelayIt<T>> + Send + 'static,
{
    fn poll_next_item(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<DelayIt<T>>> {
        let mut projection = self.as_mut().project();
        match projection.stage.as_mut().project() {
            PartiallyDelayedStageProjection::Next => {
                let mut projection = self.as_mut().project();
                match projection.stream.poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        projection.stage.set(PartiallyDelayedStage::Callback {
                            fut: (projection.should_delay)(item),
                        });
                    },
                    Poll::Ready(None) => {
                        projection.stage.set(PartiallyDelayedStage::Exhausted);
                    },
                    // 50% change to poll the next delayed item
                    Poll::Pending => {
                        if !projection.buffer.is_empty() && rand::rng().random_bool(0.5) {
                            projection.stage.set(PartiallyDelayedStage::Delayed);
                        }
                    },
                };
                match projection.stage.as_ref().get_ref() {
                    PartiallyDelayedStage::Next => {},
                    _ => cx.waker().wake_by_ref(),
                }
                Poll::Pending
            },
            PartiallyDelayedStageProjection::Callback { fut } => match ready!(fut.poll(cx)) {
                DelayIt::Pass(item) => {
                    match projection.buffer.is_empty() {
                        true => projection.stage.set(PartiallyDelayedStage::Next),
                        false => projection.stage.set(PartiallyDelayedStage::Delayed),
                    }
                    Poll::Ready(Some(DelayIt::Pass(item)))
                },
                DelayIt::Delay(item, delay) => {
                    projection.buffer.insert((item, delay), delay);
                    projection.stage.set(PartiallyDelayedStage::Delayed);
                    cx.waker().wake_by_ref();
                    Poll::Pending
                },
            },
            PartiallyDelayedStageProjection::Delayed => {
                let (delayed, force_switch_to_stream) = match projection.buffer.poll_expired(cx) {
                    Poll::Ready(Some(delayed)) => {
                        let (item, duration) = delayed.into_inner();
                        (Poll::Ready(Some(DelayIt::Delay(item, duration))), false)
                    },
                    Poll::Pending | Poll::Ready(None) => (Poll::Pending, true),
                };
                // 50% chance to switch to the next item in the stream if not forced
                if force_switch_to_stream || projection.small_rng.random_bool(0.5) {
                    projection.stage.set(PartiallyDelayedStage::Next);
                    cx.waker().wake_by_ref();
                }
                delayed
            },
            PartiallyDelayedStageProjection::Exhausted => {
                let result = ready!(projection.buffer.poll_expired(cx));
                if result.is_none() {
                    projection.stage.set(PartiallyDelayedStage::Completed);
                }
                Poll::Ready(result.map(|x| {
                    let (item, duration) = x.into_inner();
                    DelayIt::Delay(item, duration)
                }))
            },
            PartiallyDelayedStageProjection::Completed => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::init_test;
    use std::time::Duration;
    use tokio::time::Instant;
    use tokio_stream::{wrappers::ReceiverStream, StreamExt};

    const STREAM_DELAY: Duration = Duration::from_millis(100);
    const STREAM_DELAY_F64: f64 = STREAM_DELAY.as_secs_f64();
    const BATCHES_COUNT: u32 = 10;
    const PACKETS_COUNT: u32 = 10;
    const BATCH_DELAY: Duration = Duration::from_millis(10);
    const MAX_LAG: f64 = Duration::from_millis(PACKETS_COUNT as u64).as_secs_f64();

    struct StreamDelayTest {
        started: tokio::sync::oneshot::Receiver<()>,
        stream: ReceiverStream<TestStreamItem>,
        jh: tokio::task::JoinHandle<()>,
    }

    #[derive(Clone)]
    struct TestStreamItem {
        sent: Instant,
        seq_no: u64,
        #[allow(dead_code)]
        heavy: [u8; 1024 * 20], // 20 Kib of data to simulate a heavy item
    }

    async fn run_stream() -> StreamDelayTest {
        let (sender, receiver) = tokio::sync::mpsc::channel(1024);
        let (started_sender, started_receiver) = tokio::sync::oneshot::channel();
        let jh = tokio::spawn(async move {
            started_sender.send(()).unwrap();
            let mut seq_no = 0;
            for _ in 0..BATCHES_COUNT {
                for _ in 0..PACKETS_COUNT {
                    sender
                        .send(TestStreamItem {
                            sent: Instant::now(),
                            seq_no,
                            heavy: [1; 1024 * 20], // 20 Kib of data
                        })
                        .await
                        .unwrap();
                    // tracing::info!("sent:     {:>3}", seq_no);
                    seq_no += 1;
                }
                tokio::time::sleep(BATCH_DELAY).await;
            }
        });

        StreamDelayTest {
            started: started_receiver,
            stream: ReceiverStream::new(receiver),
            jh,
        }
    }

    // #[tokio::test(flavor = "multi_thread")]
    #[tokio::test]
    async fn test_delayed_stream() {
        init_test();
        let StreamDelayTest { started, stream, jh } = run_stream().await;
        let mut stream = stream.delay(STREAM_DELAY);
        started.await.unwrap();
        while let Some(delayed) = stream.next().await {
            let now = Instant::now();
            let delayed = now.duration_since(delayed.sent).as_secs_f64();
            assert!((STREAM_DELAY_F64..STREAM_DELAY_F64 + MAX_LAG).contains(&delayed));
        }
        jh.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    // #[tokio::test]
    async fn test_partially_delayed_stream() {
        init_test();
        let StreamDelayTest { started, stream, jh } = run_stream().await;
        struct TestStreamItemReceivedAt {
            received_at: Instant,
            item: TestStreamItem,
        }
        let mut stream = stream
            // .on_each_cb(|item| {
            //     tracing::info!("received: {:>3}: {:.3}", item.seq_no, item.time.elapsed().as_secs_f64());
            // })
            .map(|item| TestStreamItemReceivedAt {
                item,
                received_at: Instant::now(),
            })
            .delay_partially(|item| async move {
                match item.item.seq_no % 3 == 0 {
                    true => DelayIt::Pass(item),
                    false => DelayIt::Delay(item, Duration::from_millis(50)),
                }
            })
            .pin();
        started.await.unwrap();
        const PASS_LAG_MU: u128 = 2500;
        const SENT_DELAY_LAG_MU: u128 = 5000;
        const RECEIVED_DELAY_LAG_MU: u128 = 5000;
        const SENT_RECEIVED_LAG_MU: u128 = 2500;
        while let Some(item) = stream.next().await {
            match item {
                DelayIt::Pass(item) => {
                    let sent = item.item.sent.elapsed().as_micros();
                    tracing::info!("passed:   {:>3}: sent {}µs ago", item.item.seq_no, sent);
                    assert!(
                        sent < PASS_LAG_MU,
                        "sending lag is too big = {sent}µs, expected = {PASS_LAG_MU}µs"
                    );
                },
                DelayIt::Delay(item, delay) => {
                    let sent = item.item.sent.elapsed().as_micros();
                    let received = item.received_at.elapsed().as_micros();
                    let delay = delay.as_micros();
                    tracing::info!(
                        "delayed:  {:>3}: sent {}µs ago (delay: {:?}), received {}µs ago, size: {}",
                        item.item.seq_no,
                        sent,
                        delay,
                        received,
                        item.item.heavy.len(),
                    );
                    assert!(sent >= delay);
                    assert!(sent - delay < SENT_DELAY_LAG_MU);
                    assert!(received >= delay);
                    assert!(received - delay < RECEIVED_DELAY_LAG_MU);
                    assert!(sent >= received);
                    assert!(sent - received < SENT_RECEIVED_LAG_MU);
                },
            }
        }
        jh.await.unwrap();
    }
}
