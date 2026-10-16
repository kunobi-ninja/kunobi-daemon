//! `local::shutdown_signal` against real stop requests.
//!
//! Unix sends SIGTERM and SIGINT to this test process. Windows cannot raise a
//! console close, logoff or shutdown from a test, and Ctrl+C reaches every
//! process on a console, so a helper process with a console of its own sends
//! Ctrl+C to itself.

#![cfg(all(feature = "local", feature = "async"))]

use std::{future::Future, pin::Pin, task::Poll, time::Duration};

use kunobi_daemon::local::shutdown_signal;

/// True when `future` is not complete yet. Polls it once.
async fn is_pending(mut future: Pin<&mut impl Future<Output = ()>>) -> bool {
    std::future::poll_fn(move |cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
}

#[cfg(unix)]
#[tokio::test]
async fn sigterm_and_sigint_each_complete_the_shutdown_signal() {
    use std::process::Command;
    for signal in ["TERM", "INT"] {
        let mut shutdown = std::pin::pin!(shutdown_signal().unwrap());
        assert!(
            is_pending(shutdown.as_mut()).await,
            "complete before SIG{signal}"
        );

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

#[cfg(windows)]
const HELPER: &str = "KDAEMON_SHUTDOWN_SIGNAL_HELPER";

#[cfg(windows)]
#[tokio::test]
#[ignore = "helper process for the Ctrl+C test"]
#[allow(unsafe_code)]
async fn ctrl_c_helper() {
    if std::env::var_os(HELPER).is_none() {
        return;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn SetConsoleCtrlHandler(
            handler: Option<unsafe extern "system" fn(u32) -> i32>,
            add: i32,
        ) -> i32;
        fn GenerateConsoleCtrlEvent(event: u32, process_group: u32) -> i32;
    }
    const CTRL_C_EVENT: u32 = 0;
    // A parent that ignores Ctrl+C passes that on; turn it back on.
    // SAFETY: a null handler with `add` false only changes that flag.
    let enabled = unsafe { SetConsoleCtrlHandler(None, 0) };
    assert_ne!(enabled, 0, "{}", std::io::Error::last_os_error());
    let mut shutdown = std::pin::pin!(shutdown_signal().unwrap());
    assert!(
        is_pending(shutdown.as_mut()).await,
        "complete before Ctrl+C"
    );
    // SAFETY: takes no pointers. Group 0 is every process on this console,
    // which the parent created for this process alone.
    let sent = unsafe { GenerateConsoleCtrlEvent(CTRL_C_EVENT, 0) };
    assert_ne!(sent, 0, "{}", std::io::Error::last_os_error());
    tokio::time::timeout(Duration::from_secs(10), shutdown)
        .await
        .expect("Ctrl+C did not complete the shutdown signal");
    assert!(!kunobi_daemon::local::windows::session_end_requested());
}

#[cfg(windows)]
#[test]
fn ctrl_c_completes_the_shutdown_signal() {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};
    const CREATE_NEW_CONSOLE: u32 = 0x10;
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "ctrl_c_helper",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(HELPER, "1")
        .creation_flags(CREATE_NEW_CONSOLE)
        .stdin(Stdio::null())
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{}\n{stdout}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    // A filter that matched nothing would also exit successfully.
    assert!(stdout.contains("1 passed"), "{stdout}");
}
