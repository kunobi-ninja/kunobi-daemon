//! `local::shutdown_signal` against real signals sent to this test process.
//!
//! Windows session-end events cannot be raised from a test; the handler is
//! exercised directly in `src/local/windows.rs`.

#![cfg(all(unix, feature = "local", feature = "async"))]

use std::{future::Future, pin::pin, process::Command, task::Poll, time::Duration};

use kunobi_daemon::local::shutdown_signal;

#[tokio::test]
async fn sigterm_and_sigint_each_complete_the_shutdown_signal() {
    for signal in ["TERM", "INT"] {
        let mut shutdown = pin!(shutdown_signal().unwrap());
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(shutdown.as_mut().poll(cx).is_pending()));
        assert!(pending.await, "complete before SIG{signal}");

        // Handlers are installed when `shutdown_signal` returns, so the signal
        // reaches them instead of ending this process.
        let sent = Command::new("kill")
            .arg(format!("-{signal}"))
            .arg(std::process::id().to_string())
            .status()
            .unwrap();
        assert!(sent.success());
        tokio::time::timeout(Duration::from_secs(10), shutdown)
            .await
            .unwrap_or_else(|_| panic!("SIG{signal} did not complete the shutdown signal"));
    }
}
