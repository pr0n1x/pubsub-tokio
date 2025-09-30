#![cfg(any(test, feature = "testing"))]
use std::sync::atomic::AtomicBool;

static INITIALIZED: AtomicBool = AtomicBool::new(false);

pub fn init_test() {
    if INITIALIZED
        .compare_exchange(
            false,
            true,
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
        )
        .is_ok()
    {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_thread_ids(true)
            .with_thread_names(true)
            .with_line_number(true)
            .init();
    }
}
