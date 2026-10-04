//! The blocking control client against a real control service, and against a
//! server that misreports its process.

#![cfg(all(unix, feature = "wire-async", feature = "local-async"))]

use kunobi_daemon::ProcessId;
use kunobi_daemon::{
    Lifecycle, ServiceIdentity,
    admission::{Admission, Limits, Pool},
    client::{self, RequestError},
    control::ControlService,
    local::unix_socket::{self, Bound},
    peer::{Rejected, SameUser},
    serve::serve,
    wire::{self, Health, Hello, capability},
};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

fn offer() -> Hello {
    let identity = ServiceIdentity::new([0x71; 16], "client-test", "demo", "default").unwrap();
    Hello::new(
        &identity,
        capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
        capability::HEALTH | capability::HEALTH_DETAILS,
    )
}

fn own_pid() -> ProcessId {
    ProcessId::new(std::process::id()).unwrap()
}

fn bind(dir: &Path) -> (PathBuf, tokio::net::UnixListener) {
    let path = dir.join("control.sock");
    let Bound::Won(listener) = unix_socket::acquire(&path).unwrap() else {
        panic!("first owner")
    };
    listener.set_nonblocking(true).unwrap();
    (path, tokio::net::UnixListener::from_std(listener).unwrap())
}

/// A control service that answers every connection until the test ends.
fn spawn_service(listener: tokio::net::UnixListener) -> Arc<Lifecycle> {
    let lifecycle = Arc::new(Lifecycle::default());
    let service = Arc::new(ControlService::new(
        Arc::clone(&lifecycle),
        1,
        "client-test".into(),
        1,
    ));
    service.mark_ready();
    tokio::spawn(
        serve(
            listener,
            Arc::clone(&lifecycle),
            Arc::new(Admission::new(Limits::default())),
            SameUser,
        )
        .run(
            |()| Pool::Control,
            move |stream, permit| {
                let service = Arc::clone(&service);
                async move {
                    let _permit = permit;
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
                    let _ = service.serve(stream, &offer(), deadline).await;
                }
            },
        ),
    );
    lifecycle
}

/// Run a blocking client call off the async runtime.
async fn call<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> T {
    tokio::task::spawn_blocking(f).await.unwrap()
}

fn soon() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

#[tokio::test(flavor = "multi_thread")]
async fn health_from_the_expected_process_is_trusted() {
    let dir = tempfile::tempdir().unwrap();
    let (path, listener) = bind(dir.path());
    spawn_service(listener);
    let health = call(move || client::health(&path, &offer(), Some(own_pid()), soon()))
        .await
        .unwrap();
    assert_eq!(health.process_id, own_pid().get());
    assert!(health.ready);
    assert!(!health.draining);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_peer_that_is_not_the_expected_process_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (path, listener) = bind(dir.path());
    spawn_service(listener);
    let other = ProcessId::new(1).unwrap();
    let error = call(move || client::health(&path, &offer(), Some(other), soon()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RequestError::Peer(Rejected::OtherProcess { expected, observed })
                if expected == other && observed == own_pid()
        ),
        "{error:?}"
    );
    assert!(!error.is_transient());
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_closes_admission_and_reports_it() {
    let dir = tempfile::tempdir().unwrap();
    let (path, listener) = bind(dir.path());
    let lifecycle = spawn_service(listener);
    let health = call(move || client::drain(&path, &offer(), None, soon()))
        .await
        .unwrap();
    assert!(health.draining);
    assert!(!lifecycle.accepting_calls());
}

#[tokio::test(flavor = "multi_thread")]
async fn an_endpoint_nothing_serves_yet_is_unavailable_and_worth_retrying() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.sock");
    let error = call(move || {
        client::health(
            &path,
            &offer(),
            None,
            Instant::now() + Duration::from_millis(200),
        )
    })
    .await
    .unwrap_err();
    assert!(matches!(error, RequestError::Unavailable(_)), "{error:?}");
    assert!(error.is_transient());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reply_naming_another_process_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (path, listener) = bind(dir.path());
    // Answers like a control service, but claims to be PID 1.
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut session = wire::AsyncSession::accept(stream, &offer()).await.unwrap();
        let request = session.receive().await.unwrap();
        let lie = Health {
            process_id: 1,
            ready: true,
            ..Default::default()
        };
        session
            .send(&lie.response(&request).unwrap())
            .await
            .unwrap();
    });
    let error = call(move || client::health(&path, &offer(), None, soon()))
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            RequestError::ProcessMismatch { peer, reported: 1 } if peer == own_pid()
        ),
        "{error:?}"
    );
}
