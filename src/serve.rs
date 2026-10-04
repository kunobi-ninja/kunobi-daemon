//! Accept local connections, authenticate them, admit them, and serve them
//! until the service drains.
//!
//! [`serve`] is the accept loop a daemon would otherwise write itself:
//!
//! 1. Accept a connection. Running out of file descriptors pauses accepting
//!    briefly instead of ending the loop.
//! 2. Read its [`Credentials`](crate::peer::Credentials) and run the consumer's
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
    local::peer::PeerCredentials,
    peer::{Authenticated, Policy, authenticate},
};
use std::{future::Future, io, sync::Arc, time::Duration};
use tokio::task::JoinSet;

/// How long to stop accepting after the process runs out of file descriptors.
/// Accepting again immediately would spin on the same error.
const DESCRIPTOR_BACKOFF: Duration = Duration::from_millis(50);

/// A listener whose connections can report their peer.
pub trait Listener {
    /// The accepted connection.
    type Connection: PeerCredentials + Send + 'static;
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
    pub unauthenticated: u64,
    /// Authenticated connections dropped because their pool was full.
    pub over_capacity: u64,
    /// Handlers cancelled because they were still running when the drain
    /// budget expired.
    pub aborted: u64,
}

/// Whether an accept error means this process has no file descriptors left,
/// which is temporary: wait and accept again.
fn is_descriptor_exhaustion(error: &io::Error) -> bool {
    // EMFILE and ENFILE, the same numbers on Linux, macOS and the BSDs.
    cfg!(unix) && matches!(error.raw_os_error(), Some(23 | 24))
}

/// Configure an accept loop with its listener, lifecycle, admission and peer policy.
/// Call [`Serve::run`] with the grant-to-pool mapping and connection handler.
pub fn serve<L, P>(
    listener: L,
    lifecycle: Arc<Lifecycle>,
    admission: Arc<Admission>,
    policy: P,
) -> Serve<L, P> {
    Serve {
        listener,
        lifecycle,
        admission,
        policy,
        drain_budget: Duration::from_secs(5),
    }
}

/// An accept loop configured with one peer policy and a bounded handler drain.
pub struct Serve<L, P> {
    listener: L,
    lifecycle: Arc<Lifecycle>,
    admission: Arc<Admission>,
    policy: P,
    drain_budget: Duration,
}

/// An accept failure, retaining the counts after running handlers have drained.
#[derive(Debug)]
#[non_exhaustive]
pub struct ServeError {
    /// The listener's error.
    pub error: io::Error,
    /// Accepted, rejected and aborted connections before the loop ended.
    pub served: Served,
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "accept loop failed: {}", self.error)
    }
}

impl std::error::Error for ServeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

impl<L, P> Serve<L, P> {
    /// Give running handlers this long to finish after draining starts.
    /// The default is five seconds. Remaining handlers are then aborted.
    pub fn drain_budget(mut self, budget: Duration) -> Self {
        self.drain_budget = budget;
        self
    }

    /// Accept, authenticate, admit and serve until the lifecycle drains.
    ///
    /// `pool` chooses capacity for a grant; `handler` owns the authenticated
    /// connection and its permit. An accept failure drains handlers before
    /// returning [`ServeError`]. Dropping the future aborts handlers immediately.
    pub async fn run<H, F>(
        self,
        pool: impl Fn(&P::Grant) -> Pool,
        handler: H,
    ) -> Result<Served, ServeError>
    where
        L: Listener,
        P: Policy,
        P::Grant: Send + 'static,
        H: Fn(Authenticated<L::Connection, P::Grant>, Permit) -> F,
        F: Future<Output = ()> + Send + 'static,
    {
        let Self {
            listener,
            lifecycle,
            admission,
            policy,
            drain_budget,
        } = self;
        let mut served = Served::default();
        let mut handlers = JoinSet::new();
        let stopped = 'accept: loop {
            tokio::select! {
                biased;
                () = lifecycle.draining() => break 'accept Ok(()),
                Some(_) = handlers.join_next(), if !handlers.is_empty() => {}
                accepted = listener.accept() => match accepted {
                    Ok(connection) => {
                        let Ok(evidence) = connection.credentials() else {
                            served.unauthenticated += 1;
                            lifecycle.record(crate::observation::Event::Rejected);
                            continue;
                        };
                        let Ok(authenticated) = authenticate(connection, evidence, &policy) else {
                            served.unauthenticated += 1;
                            lifecycle.record(crate::observation::Event::Rejected);
                            continue;
                        };
                        let Some(permit) = admission.try_acquire(pool(authenticated.grant())) else {
                            served.over_capacity += 1;
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
        stopped
            .map(|()| served)
            .map_err(|error| ServeError { error, served })
    }
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
        ProcessId,
        admission::Limits,
        peer::{Credentials, ExpectedProcess, First, SameUser},
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
        let server = tokio::spawn(
            serve(
                listener,
                Arc::clone(&lifecycle),
                Arc::clone(&admission),
                policy,
            )
            .drain_budget(Duration::from_secs(5))
            .run(
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
            ),
        );
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
        let observations = Arc::new(crate::observation::Observations::new(4));
        let lifecycle = Arc::new(Lifecycle::observed(Arc::clone(&observations)));
        let admission = Arc::new(Admission::new(Limits::default()));
        let server = tokio::spawn(
            serve(
                listener,
                Arc::clone(&lifecycle),
                Arc::clone(&admission),
                ExpectedProcess::new(|| ProcessId::new(1)),
            )
            .drain_budget(Duration::from_secs(5))
            .run(
                |_| Pool::Control,
                |_connection, _permit| async move {
                    panic!("a rejected peer reached the handler");
                },
            ),
        );
        assert_eq!(exchange(&path).await, b"");
        let control = admission.snapshot(Pool::Control);
        assert_eq!((control.active, control.rejected), (0, 0));
        assert!(
            observations
                .take_events()
                .iter()
                .any(|event| event.event == crate::observation::Event::Rejected)
        );
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.unauthenticated, 1);
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
        let server = tokio::spawn(
            serve(
                listener,
                Arc::clone(&lifecycle),
                Arc::clone(&admission),
                SameUser,
            )
            .drain_budget(Duration::from_secs(5))
            .run(
                |()| Pool::Control,
                |_connection, _permit| async move {
                    panic!("a refused peer reached the handler");
                },
            ),
        );
        assert_eq!(exchange(&path).await, b"");
        assert_eq!(admission.snapshot(Pool::Control).rejected, 1);
        lifecycle.start_drain();
        let served = server.await.unwrap().unwrap();
        assert_eq!(served.over_capacity, 1);
        assert_eq!(served.unauthenticated, 0);
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
        let server = tokio::spawn(
            serve(listener, Arc::clone(&lifecycle), admission, SameUser)
                .drain_budget(Duration::from_secs(5))
                .run(
                    |()| Pool::Application,
                    move |_connection, permit| {
                        let started = started.clone();
                        async move {
                            let _permit = permit;
                            started.send(()).unwrap();
                            std::future::pending::<()>().await;
                        }
                    },
                ),
        );
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
        let server = tokio::spawn(
            serve(listener, Arc::clone(&lifecycle), admission, SameUser)
                .drain_budget(Duration::from_secs(5))
                .run(
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
                ),
        );
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

    #[tokio::test]
    async fn an_accept_failure_keeps_counts_and_its_error_source() {
        let error = serve(
            Scripted::new([
                Ok(Fake(None)),
                Ok(same_user()),
                Err(io::ErrorKind::PermissionDenied.into()),
            ]),
            Arc::new(Lifecycle::default()),
            Arc::new(Admission::new(Limits::default())),
            SameUser,
        )
        .run(|()| Pool::Application, |_connection, _permit| async {})
        .await
        .unwrap_err();
        assert_eq!(error.served.unauthenticated, 1);
        assert_eq!(error.served.admitted, 1);
        assert_eq!(error.served.over_capacity, 0);
        assert_eq!(error.served.aborted, 0);
        assert_eq!(error.error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(
            error.to_string(),
            format!("accept loop failed: {}", error.error)
        );
        assert!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<io::Error>()
                .is_some()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_handler_drain_budget_is_configurable_and_defaults_to_five_seconds() {
        for budget in [None, Some(Duration::from_secs(13))] {
            let lifecycle = Arc::new(Lifecycle::default());
            let (started, mut ready) = tokio::sync::mpsc::unbounded_channel();
            let configured = serve(
                Scripted::new([Ok(same_user())]),
                Arc::clone(&lifecycle),
                Arc::new(Admission::new(Limits::default())),
                SameUser,
            );
            let configured = match budget {
                Some(budget) => configured.drain_budget(budget),
                None => configured,
            };
            let server = tokio::spawn(configured.run(
                |()| Pool::Application,
                move |_connection, permit| {
                    let started = started.clone();
                    async move {
                        let _permit = permit;
                        started.send(()).unwrap();
                        std::future::pending::<()>().await;
                    }
                },
            ));
            ready.recv().await.unwrap();
            let began = tokio::time::Instant::now();
            lifecycle.start_drain();
            let served = server.await.unwrap().unwrap();
            assert_eq!(served.aborted, 1);
            assert_eq!(began.elapsed(), budget.unwrap_or(Duration::from_secs(5)));
        }
    }

    /// A connection whose evidence the test decides.
    struct Fake(Option<Credentials>);

    impl PeerCredentials for Fake {
        fn credentials(&self) -> io::Result<Credentials> {
            self.0
                .clone()
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
        Fake(Some(Credentials::new(
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
    ) -> Result<Served, ServeError> {
        let (admitted, mut seen) = tokio::sync::mpsc::unbounded_channel();
        let server = tokio::spawn(
            serve(
                listener,
                Arc::clone(&lifecycle),
                Arc::new(Admission::new(Limits::default())),
                policy,
            )
            .drain_budget(Duration::from_secs(5))
            .run(
                |()| Pool::Application,
                move |_connection, _permit| {
                    let admitted = admitted.clone();
                    async move {
                        let _ = admitted.send(());
                    }
                },
            ),
        );
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
            (
                served.unauthenticated,
                served.admitted,
                served.over_capacity
            ),
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
        assert_eq!(error.error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn a_connection_checked_while_draining_starts_is_not_handed_out() {
        struct DrainsWhileChecking(Arc<Lifecycle>);
        impl Policy for DrainsWhileChecking {
            type Grant = ();
            fn grant(&self, _: &Credentials) -> Result<(), crate::peer::Rejected> {
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
