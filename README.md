# pubsub-tokio

Fan-out (publish/subscribe) pattern implementation based on `tokio::mpsc` channels.

One publisher, one dispatcher task, and a compile-time-fixed set of subscribers.
Every published value is cloned and delivered to each active subscription that
passes its filters. On top of that, the crate ships a set of `Stream`
combinators for building delivery pipelines (e.g. gRPC response streams) out of
subscriptions.

A typical use case: fanning out a firehose of events to many concurrently
connected gRPC clients, each with its own set of filters.

## Features

- **Static subscriber topology** — `PubSub<T, N>` declares the number of
  subscribers (`N`) as a const generic, so the wiring of your application is
  visible in types. Each subscriber can open any number of dynamic
  *subscriptions* at runtime.
- **Two-level filtering** — a filter per *subscriber* (applies to all its
  subscriptions) plus a *subfilter* per individual subscription. Both can be
  synchronous (`Filter::cb`) or asynchronous (`Filter::fut`).
- **Mutation hooks** — `on_dispatch` (dispatcher-level, runs once per published
  value) and `on_send` (per subscription, runs on the clone sent to that
  subscription). Handy for timestamping stages of the pipeline.
- **Heartbeats and graceful disconnect** — subscribers receive
  `Packet::Heartbeat` every second and `Packet::Disconnect` on shutdown, so
  consumers can detect liveness and terminate cleanly.
- **Backpressure, not unbounded buffering** — all channels are bounded; a slow
  subscription slows the dispatch loop down instead of eating memory. Dropped
  (closed) subscriptions are detected and pruned automatically.
- **Stream combinators** (`streamer` module) — `on_end`, `on_each`,
  `map_async`, `filter_map_relaxed`, `delay`, `delay_partially`, `zip_pair`,
  and a `ReaderStream` adapter for turning a pull-style reader into a `Stream`.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
pubsub-tokio = { version = "1.0.0", features = ["tokio-stream"] }
```

Cargo features:

| Feature | Effect |
| --- | --- |
| `tokio-stream` | Adds `PacketReceiver::into_stream()` returning a `ReceiverStream<Packet<T>>` |
| `testing` | Test helpers (tracing/log initialization for tests) |

## Quick start

```rust
use pubsub_tokio::prelude::*;
use std::time::Duration;

#[derive(Debug, Clone)]
struct Tick(u64);

#[tokio::main]
async fn main() {
    // A hub with two subscribers; destructure it to get all the parts.
    let PubSub {
        dispatcher,
        publisher,
        subscribers: [sub_a, sub_b],
    } = PubSub::<Tick, 2>::new("ticks", DEFAULT_BUFFER_SIZE);

    // The dispatcher is a future — spawn it (optionally with a shutdown signal).
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let dispatcher_task = tokio::spawn(dispatcher.run_with_shutdown(async {
        _ = shutdown_rx.await;
    }));

    // The publisher is a plain `tokio::sync::mpsc::Sender<T>`.
    tokio::spawn(async move {
        for i in 0..u64::MAX {
            if publisher.send(Tick(i)).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    });

    // Each subscriber can open any number of subscriptions.
    let mut all_ticks = sub_a.subscribe().await.unwrap();
    let mut even_ticks = sub_b
        .subscribe_with(SubscriptionOptions::filter(Filter::cb(|t: &Tick| {
            t.0 % 2 == 0
        })))
        .await
        .unwrap();

    let consumer = tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(packet) = all_ticks.recv() => match packet {
                    Packet::Data(tick) => println!("tick {}", tick.0),
                    Packet::Heartbeat(n) => println!("heartbeat #{n}"),
                    Packet::Disconnect => break,
                },
                Some(packet) = even_ticks.recv() => {
                    if let Packet::Data(tick) = packet {
                        println!("even tick {}", tick.0);
                    }
                },
            }
        }
    });

    tokio::time::sleep(Duration::from_secs(1)).await;
    _ = shutdown_tx.send(()); // broadcasts Packet::Disconnect to everyone
    _ = tokio::join!(dispatcher_task, consumer);
}
```

Every subscription receives `Packet<T>` values:

```rust,ignore
pub enum Packet<T> {
    Data(T),          // a published value (cloned per subscription)
    Heartbeat(u64),   // sent every second by the dispatcher
    Disconnect,       // the dispatcher is shutting down
}
```

## Filters

Filters exist on two levels and can be changed at runtime through the
`Subscriber` handle (the dispatcher applies them on the fly):

```rust,ignore
// Subscriber-level: applies to ALL subscriptions of this subscriber.
subscriber.set_filter(Filter::cb(|t: &Tick| t.0 > 100)).await?;
subscriber.remove_filter().await?;

// Subscription-level ("subfilter"): applies to one subscription, by seq_no.
let seq_no = *subscription.subscription_id.seq_no;
subscriber.set_subfilter(seq_no, Filter::cb(|t: &Tick| t.0 % 2 == 0)).await?;
subscriber.remove_subfilter(seq_no).await?;
```

A filter can be asynchronous, which is useful when the decision requires
looking into shared state — for example, deciding whether a batch of events is
interesting to a particular gRPC client:

```rust,ignore
let subscription = event_subscriber
    .subscribe_with(SubscriptionOptions {
        on_send: Some(OnTransfer::new(|batch: &mut EventBatch| {
            batch.filtered_at = Some(SystemTime::now());
        })),
        filter: Some(Filter::fut({
            let topics_of_interest = Arc::clone(&topics_of_interest);
            move |batch: &EventBatch| {
                let events = batch.decode_events();
                let topics_of_interest = Arc::clone(&topics_of_interest);
                async move {
                    for event in events {
                        // async lookups into shared caches / dedupers here
                        if topics_of_interest.contains_one_of(event.topics()).await {
                            return true;
                        }
                    }
                    false
                }
            }
        })),
    })
    .await?;
```

Note: filters only see `Packet::Data`; heartbeats and disconnect notifications
always pass through.

## Mutation hooks: `on_dispatch` and `on_send`

`on_dispatch` runs once per published value before fan-out; `on_send` runs per
subscription on the clone being delivered. A typical use is timestamping
pipeline stages:

```rust,ignore
// Once, on the original value:
dispatcher.on_dispatch(Some(OnTransfer::new(|batch: &mut EventBatch| {
    batch.received_at = Some(SystemTime::now());
})));

// Per subscription, on its own clone (see subscribe_with above):
// on_send: Some(OnTransfer::new(|batch| { batch.filtered_at = ...; }))
```

## Subscribing before the dispatcher runs

`Subscriber::subscribe()` talks to the running dispatcher over a channel. If
you need to wire subscriptions up-front — during application startup, before
spawning the dispatcher — use `Dispatcher::prepare`, which registers the
subscription synchronously:

```rust,ignore
let mut dump_stream = dispatcher.prepare(&subscriber).subscribe();
tokio::spawn(dispatcher.run_with_shutdown(shutdown_signal));
```

## Subscriptions as streams

With the `tokio-stream` feature, a subscription converts into a `Stream`,
which composes with the combinators from `pubsub_tokio::streamer`. This is the
pattern used to serve gRPC subscriptions: forward data, drop heartbeats, turn
disconnects into a status, and clean up when the client goes away:

```rust,ignore
use pubsub_tokio::streamer::OnEndStream;
use tokio_stream::StreamExt;

let seq_no = *subscription.subscription_id.seq_no;
let response_stream = subscription
    .into_stream()
    .filter_map(|packet| match packet {
        Packet::Data(batch) => Some(Ok(batch.into_notification())),
        Packet::Heartbeat(_) => None,
        Packet::Disconnect => Some(Err(Status::aborted("disconnected"))),
    })
    .on_end({
        let subscriber = Arc::clone(&event_subscriber);
        Box::pin(async move {
            // The client dropped the stream: release resources and tell
            // the dispatcher to prune this subscription.
            _ = subscriber.disconnect(seq_no).await;
        })
    });
```

### Stream combinators

The `streamer` module works with any `futures_core::Stream`, not only with
subscriptions:

| Combinator | Purpose |
| --- | --- |
| `on_end_cb(f)` / `on_end(future)` | Run a callback / future when the stream ends (cleanup, deregistration) |
| `on_each_cb(f)` / `on_each(f)` | Observe every item (metrics, logging) without consuming it |
| `map_async(f)` | Map each item through an async function, preserving order |
| `filter_map_relaxed(f)` | Like `filter_map`, but yields to the runtime on filtered-out items to avoid busy-looping |
| `delay(duration)` | Delay every item by a fixed duration |
| `delay_partially(f)` | Per-item decision: pass through immediately or delay (`DelayIt::Pass` / `DelayIt::Delay`) |
| `zip_pair()` | Flatten a stream of `Pair<T>` (one or two items) into a stream of `T` |
| `ReaderStream` / `ReaderRefStream` | Turn any pull-style reader (`NextStreamItem`) into a `Stream` |

## Notes on behavior

- **Zero subscribers is valid.** `PubSub<T, 0>` gives you a dispatcher that
  simply drains published data — useful when subscribers are compiled out by
  configuration.
- **Slow consumers apply backpressure.** Each subscription has a bounded
  buffer (1024 packets); the dispatch loop awaits delivery, so one stuck
  subscription eventually slows the fan-out. Disconnect idle consumers or keep
  their handlers fast.
- **Dropped receivers are pruned.** If a subscription's receiver is dropped
  without an explicit `disconnect`, the failed send is logged and the
  subscription is removed from the dispatcher.

## License

Licensed under either of

- Apache License, Version 2.0 (`LICENSE-APACHE` or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT License (`LICENSE-MIT` or <http://opensource.org/licenses/MIT>)

at your option.
