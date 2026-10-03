//! Accept local connections, authenticate them, admit them, and serve them
//! until the service drains.
//!
//! [`serve`] is the accept loop a daemon would otherwise write itself:
//!
//! 1. Accept a connection. Running out of file descriptors pauses accepting
//!    briefly instead of ending the loop.
//! 2. Read its [`Evidence`](crate::peer::Evidence) and run the consumer's
//!    [`Policy`]. A rejected peer is dropped before it can hold any capacity.
//! 3. Take an [`Admission`] permit from the pool the consumer picks for that
//!    grant. No capacity drops the connection.
//! 4. Run the consumer's handler with the [`Authenticated`] connection and its
//!    permit.
//!
//! When [`Lifecycle`] starts draining, the loop stops accepting, gives the
//! running handlers until the drain budget expires, then aborts the rest and
//! reports what happened. Handlers still take a [`crate::RequestGuard`] per
//! operation; [`Lifecycle::unless_draining`] stops a persistent connection
//! from reading new requests once draining starts.
//!
//! Shut down by starting the drain and awaiting [`serve`]. Dropping the
//! [`serve`] future instead aborts the handlers without waiting for them.
//!
//! On Unix the listener is Tokio's `UnixListener`, which
//! `local::unix_socket::acquire` produces through `from_std`; on Windows it is
//! `interprocess`'s Tokio listener, which `local::windows_socket::acquire_tokio`
//! produces.

use crate::{
    Lifecycle,
    admission::{Admission, Permit, Pool},
    local::peer::PeerEvidence,
    peer::{Authenticated, Policy, authenticate},
};
use std::{future::Future, io, sync::Arc, time::Duration};
use tokio::task::JoinSet;

/// How long to stop accepting after the process runs out of file descriptors.
/// Accepting again immediately would spin on the same error.
pub const DESCRIPTOR_BACKOFF: Duration = Duration::from_millis(50);

/// A listener whose connections can report their peer.
pub trait Listener {
    /// The accepted connection.
    type Connection: PeerEvidence + Send + 'static;
    /// Wait for the next connection. Must be cancellation-safe: [`serve`]
    /// drops a pending accept whenever a handler finishes, and a dropped
    /// accept must not lose a connection.
    fn accept(&self) -> impl Future<Output = io::Result<Self::Connection>> + Send;
}

#[cfg(unix)]
impl Listener for tokio::net::UnixListener {
    type Connection = tokio::net::UnixStream;
    async fn accept(&self) -> io::Result<tokio::net::UnixStream> {
        tokio::net::UnixListener::accept(self)
            .await
            .map(|(stream, _)| stream)
    }
}

#[cfg(windows)]
mod windows;

/// What [`serve`] did with the connections it accepted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Served {
    /// Connections handed to the handler.
    pub admitted: u64,
    /// Connections whose evidence could not be read or whose peer the policy
    /// rejected.
    pub rejected: u64,
    /// Authenticated connections dropped because their pool was full.
    pub refused: u64,
    /// Handlers cancelled because they were still running when the drain
    /// budget expired.
    pub aborted: u64,
}

/// Whether an accept error means this process has no file descriptors left,
/// which is temporary: wait and accept again.
pub fn is_descriptor_exhaustion(error: &io::Error) -> bool {
    // EMFILE and ENFILE, the same numbers on Linux, macOS and the BSDs.
    cfg!(unix) && matches!(error.raw_os_error(), Some(23 | 24))
}

/// Accept, authenticate, admit and serve connections until `lifecycle` drains.
///
/// `pool` picks the admission pool for each grant. `handler` runs on its own
/// task with the authenticated connection and the permit, which it holds for
/// as long as the connection uses capacity. After draining starts, running
/// handlers get `drain_budget` to finish; the rest are aborted.
///
/// Returns the counts in [`Served`]. An accept error other than descriptor
/// exhaustion stops accepting, drains the running handlers the same way, and
/// is returned.
pub async fn serve<L, P, H, F>(
    listener: L,
    lifecycle: Arc<Lifecycle>,
    admission: Arc<Admission>,
    policy: P,
    pool: impl Fn(&P::Grant) -> Pool,
    handler: H,
    drain_budget: Duration,
) -> io::Result<Served>
where
    L: Listener,
    P: Policy,
    P::Grant: Send + 'static,
    H: Fn(Authenticated<L::Connection, P::Grant>, Permit) -> F,
    F: Future<Output = ()> + Send + 'static,
{
    let mut served = Served::default();
    let mut handlers = JoinSet::new();
    let stopped = 'accept: loop {
        tokio::select! {
            biased;
            () = lifecycle.draining() => break 'accept Ok(()),
            Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok(connection) => {
                    let Ok(evidence) = connection.evidence() else {
                        served.rejected += 1;
                        continue;
                    };
                    let Ok(authenticated) = authenticate(connection, evidence, &policy) else {
                        served.rejected += 1;
                        continue;
                    };
                    let Some(permit) = admission.try_acquire(pool(authenticated.grant())) else {
                        served.refused += 1;
                        continue;
                    };
                    // Draining can start while this peer is checked.
                    if !lifecycle.accepting_calls() {
                        break 'accept Ok(());
                    }
                    served.admitted += 1;
                    handlers.spawn(handler(authenticated, permit));
                }
                Err(error) if is_descriptor_exhaustion(&error) => {
                    tokio::select! {
                        () = lifecycle.draining() => break 'accept Ok(()),
                        () = descriptor_backoff() => {}
                    }
                }
                Err(error) => break 'accept Err(error),
            },
        }
    };
    served.aborted = finish(&mut handlers, drain_budget).await;
    stopped.map(|()| served)
}

#[expect(
    clippy::disallowed_methods,
    reason = "No event says a descriptor was freed; pause briefly, then accept again."
)]
async fn descriptor_backoff() {
    tokio::time::sleep(DESCRIPTOR_BACKOFF).await;
}

/// Give running handlers until `budget` expires, abort the rest, and count the
/// handlers that were cancelled.
async fn finish(handlers: &mut JoinSet<()>, budget: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + budget;
    while !handlers.is_empty() {
        if tokio::time::timeout_at(deadline, handlers.join_next())
            .await
            .is_err()
        {
            break;
        }
    }
    handlers.abort_all();
    let mut aborted = 0;
    while let Some(joined) = handlers.join_next().await {
        if joined.is_err_and(|error| error.is_cancelled()) {
            aborted += 1;
        }
    }
    aborted
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        admission::Limits,
        peer::{Evidence, ExpectedProcess, First, ProcessId, SameUser},
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn only_emfile_and_enfile_count_as_running_out_of_descriptors() {
        assert!(is_descriptor_exhaustion(&io::Error::from_raw_os_error(23)));
        assert!(is_descriptor_exhaustion(&io::Error::from_raw_os_error(24)));
        assert!(!is_descriptor_exhaustion(&io::Error::from_raw_os_error(22)));
        assert!(!is_descriptor_exhaustion(&io::ErrorKind::Other.into()));
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        path: std::path::PathBuf,
        listener: tokio::net::UnixListener,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("serve.sock");
        let listener = tokio::net::UnixListener::bind(&path).unwrap();
        Fixture {
            _dir: dir,
            path,
            listener,
        }
    }

    /// Connect and read until the server closes; returns what it sent.
    async fn exchange(path: &std::path::Path) -> Vec<u8> {
        let mut stream = tokio::net::UnixStream::connect(path).await.unwrap();
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await.unwrap();
        reply
    }

    #[tokio::test]
    async fn an_accepted_peer_is_served_with_its_grant_and_a_permit_from_its_pool() {
        let Fixture {
            _dir,
            path,
            listener,
        } = fixture();
        let lifecycle = Arc::new(Lifecycle::default());
        // A full grant routed to the application pool would be refused.
        let admission = Arc::new(Admission::new(Limits {
            application: 0,
            ..Limits::default()
        }));
        let own = ProcessId::new(std::process::id());
        let policy = First::new()
            .then(ExpectedProcess::new(move || own), "full")
            .then(SameUser, "redacted");
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            Arc::clone(&admission),
            policy,
            |grant: &&str| {
                if *grant == "full" {
                    Pool::Control
                } else {
                    Pool::Application
                }
            },
            |mut connection, permit| async move {
                let _permit = permit;
                let grant = *connection.grant();
                connection.write_all(grant.as_bytes()).await.unwrap();
            },
            Duration::from_secs(5),
        ));
        assert_eq!(exchange(&path).await, b"full");
        assert_eq!(admission.snapshot(Pool::Application).rejected, 0);
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(
            served,
            Served {
                admitted: 1,
                ..Served::default()
            }
        );
    }

    #[tokio::test]
    async fn a_rejected_peer_is_dropped_before_it_takes_capacity() {
        let Fixture {
            _dir,
            path,
            listener,
        } = fixture();
        let lifecycle = Arc::new(Lifecycle::default());
        let admission = Arc::new(Admission::new(Limits::default()));
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            Arc::clone(&admission),
            ExpectedProcess::new(|| ProcessId::new(1)),
            |_| Pool::Control,
            |_connection, _permit| async move {
                panic!("a rejected peer reached the handler");
            },
            Duration::from_secs(5),
        ));
        assert_eq!(exchange(&path).await, b"");
        let control = admission.snapshot(Pool::Control);
        assert_eq!((control.active, control.rejected), (0, 0));
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.rejected, 1);
        assert_eq!(served.admitted, 0);
    }

    #[tokio::test]
    async fn a_full_pool_refuses_an_authenticated_peer() {
        let Fixture {
            _dir,
            path,
            listener,
        } = fixture();
        let lifecycle = Arc::new(Lifecycle::default());
        let admission = Arc::new(Admission::new(Limits {
            control: 0,
            ..Limits::default()
        }));
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            Arc::clone(&admission),
            SameUser,
            |()| Pool::Control,
            |_connection, _permit| async move {
                panic!("a refused peer reached the handler");
            },
            Duration::from_secs(5),
        ));
        assert_eq!(exchange(&path).await, b"");
        assert_eq!(admission.snapshot(Pool::Control).rejected, 1);
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.refused, 1);
        assert_eq!(served.rejected, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn draining_stops_accepting_and_aborts_handlers_past_the_budget() {
        let Fixture {
            _dir,
            path,
            listener,
        } = fixture();
        let lifecycle = Arc::new(Lifecycle::default());
        let admission = Arc::new(Admission::new(Limits::default()));
        let (started, mut running) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            admission,
            SameUser,
            |()| Pool::Application,
            move |_connection, permit| {
                let started = started.clone();
                async move {
                    let _permit = permit;
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                }
            },
            Duration::from_secs(5),
        ));
        let _client = tokio::net::UnixStream::connect(&path).await.unwrap();
        running.recv().await.unwrap();
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.admitted, 1);
        assert_eq!(served.aborted, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_that_finishes_within_the_budget_is_not_aborted() {
        let Fixture {
            _dir,
            path,
            listener,
        } = fixture();
        let lifecycle = Arc::new(Lifecycle::default());
        let admission = Arc::new(Admission::new(Limits::default()));
        let (started, mut running) = tokio::sync::mpsc::unbounded_channel();
        let finish = Arc::new(tokio::sync::Notify::new());
        let release = Arc::clone(&finish);
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            admission,
            SameUser,
            |()| Pool::Application,
            move |_connection, permit| {
                let started = started.clone();
                let finish = Arc::clone(&finish);
                async move {
                    let _permit = permit;
                    started.send(()).unwrap();
                    finish.notified().await;
                }
            },
            Duration::from_secs(5),
        ));
        let _client = tokio::net::UnixStream::connect(&path).await.unwrap();
        running.recv().await.unwrap();
        lifecycle.start_drain();
        // Let the loop see the drain, then finish well inside the budget.
        tokio::task::yield_now().await;
        release.notify_one();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.admitted, 1);
        assert_eq!(served.aborted, 0);
    }

    /// A connection whose evidence the test decides.
    struct Fake(Option<Evidence>);

    impl PeerEvidence for Fake {
        fn evidence(&self) -> io::Result<Evidence> {
            self.0
                .ok_or_else(|| io::Error::other("no peer credentials"))
        }
    }

    /// Returns the scripted results in order, then waits forever.
    struct Scripted(std::sync::Mutex<std::collections::VecDeque<io::Result<Fake>>>);

    impl Scripted {
        fn new(results: impl IntoIterator<Item = io::Result<Fake>>) -> Self {
            Self(std::sync::Mutex::new(results.into_iter().collect()))
        }
    }

    impl Listener for Scripted {
        type Connection = Fake;
        async fn accept(&self) -> io::Result<Fake> {
            let next = self.0.lock().unwrap().pop_front();
            match next {
                Some(result) => result,
                None => std::future::pending().await,
            }
        }
    }

    fn same_user() -> Fake {
        Fake(Some(Evidence::new(
            ProcessId::new(std::process::id()),
            true,
        )))
    }

    /// Serve `listener` with a handler that reports each admission, then drain
    /// once `admissions` connections were admitted (or right away for zero).
    async fn serve_scripted(
        listener: Scripted,
        policy: impl Policy<Grant = ()> + Send + 'static,
        lifecycle: Arc<Lifecycle>,
        admissions: usize,
    ) -> io::Result<Served> {
        let (admitted, mut seen) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(serve(
            listener,
            Arc::clone(&lifecycle),
            Arc::new(Admission::new(Limits::default())),
            policy,
            |()| Pool::Application,
            move |_connection, _permit| {
                let admitted = admitted.clone();
                async move {
                    let _ = admitted.send(());
                }
            },
            Duration::from_secs(5),
        ));
        for _ in 0..admissions {
            seen.recv().await.unwrap();
        }
        tokio::task::yield_now().await;
        lifecycle.start_drain();
        server.await.unwrap()
    }

    #[tokio::test]
    async fn unreadable_evidence_rejects_the_connection() {
        let served = serve_scripted(
            Scripted::new([Ok(Fake(None)), Ok(same_user())]),
            SameUser,
            Arc::new(Lifecycle::default()),
            1,
        )
        .await
        .unwrap();
        assert_eq!(
            (served.rejected, served.admitted, served.refused),
            (1, 1, 0)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn running_out_of_descriptors_pauses_accepting_then_continues() {
        let started = tokio::time::Instant::now();
        let served = serve_scripted(
            Scripted::new([Err(io::Error::from_raw_os_error(24)), Ok(same_user())]),
            SameUser,
            Arc::new(Lifecycle::default()),
            1,
        )
        .await
        .unwrap();
        assert_eq!(served.admitted, 1);
        assert!(started.elapsed() >= DESCRIPTOR_BACKOFF);
    }

    #[tokio::test]
    async fn any_other_accept_error_stops_the_loop_and_is_returned() {
        let error = serve_scripted(
            Scripted::new([Err(io::Error::from(io::ErrorKind::PermissionDenied))]),
            SameUser,
            Arc::new(Lifecycle::default()),
            0,
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn a_connection_checked_while_draining_starts_is_not_handed_out() {
        struct DrainsWhileChecking(Arc<Lifecycle>);
        impl Policy for DrainsWhileChecking {
            type Grant = ();
            fn grant(&self, _: &Evidence) -> Result<(), crate::peer::Rejected> {
                self.0.start_drain();
                Ok(())
            }
        }
        let lifecycle = Arc::new(Lifecycle::default());
        let served = serve_scripted(
            Scripted::new([Ok(same_user())]),
            DrainsWhileChecking(Arc::clone(&lifecycle)),
            lifecycle,
            0,
        )
        .await
        .unwrap();
        assert_eq!(served.admitted, 0);
    }

    #[tokio::test]
    async fn unless_draining_stops_a_read_once_draining_starts() {
        let lifecycle = Lifecycle::default();
        assert_eq!(lifecycle.unless_draining(async { 7 }).await, Some(7));
        lifecycle.start_drain();
        assert_eq!(
            lifecycle
                .unless_draining(std::future::pending::<()>())
                .await,
            None
        );
    }
}
