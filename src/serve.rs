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
    /// Wait for the next connection.
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

impl Listener for interprocess::local_socket::tokio::Listener {
    type Connection = interprocess::local_socket::tokio::Stream;
    async fn accept(&self) -> io::Result<interprocess::local_socket::tokio::Stream> {
        interprocess::local_socket::traits::tokio::Listener::accept(self).await
    }
}

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
    /// Handlers still running when the drain budget expired, then aborted.
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
    let stopped = loop {
        tokio::select! {
            biased;
            () = lifecycle.draining() => break Ok(()),
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
                    served.admitted += 1;
                    handlers.spawn(handler(authenticated, permit));
                }
                Err(error) if is_descriptor_exhaustion(&error) => {
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "No event says a descriptor was freed; pause briefly, then accept again."
                    )]
                    tokio::time::sleep(DESCRIPTOR_BACKOFF).await;
                }
                Err(error) => break Err(error),
            },
        }
    };
    let deadline = tokio::time::Instant::now() + drain_budget;
    while !handlers.is_empty() {
        match tokio::time::timeout_at(deadline, handlers.join_next()).await {
            Ok(_) => {}
            Err(_) => {
                served.aborted = handlers.len() as u64;
                handlers.abort_all();
                while handlers.join_next().await.is_some() {}
            }
        }
    }
    stopped.map(|()| served)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{
        admission::Limits,
        peer::{ExpectedProcess, First, ProcessId, SameUser},
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
