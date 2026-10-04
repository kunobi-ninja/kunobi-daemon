//! Optional OS adapters. Unsafe calls are confined to these platform boundaries.
//!
//! [`ProcessHandle`] waits for a process to exit without polling its PID.
use std::io;
use std::time::{Duration, Instant};

/// What names a local endpoint on this platform.
///
/// A Unix socket is a filesystem path; a named pipe is a name in its own
/// namespace, with its own length limit and no directory. Deriving one from the
/// other is the caller's decision, not this crate's: the endpoint name is where
/// processes agree to meet, so a scheme chosen here would move the rendezvous
/// of every consumer that already has one.
#[cfg(unix)]
pub type Endpoint = std::path::Path;
/// What names a local endpoint on this platform.
#[cfg(windows)]
pub type Endpoint = str;

/// How long to keep retrying a refused connection before giving up.
///
/// The peer may be up while it is still binding, so a single attempt is too
/// strict. "Not running at all" is detected earlier and separately, so this
/// budget is only ever spent on the genuinely transient case.
pub const CONNECT_BUDGET: Duration = Duration::from_secs(5);

/// Gap between connection attempts.
pub const RETRY_INTERVAL: Duration = Duration::from_millis(50);

/// A local connection: how to open one, who is on the far end, and how to take
/// its halves apart.
///
/// Every method here has to exist on both platforms, which is the point. While
/// connection setup lived in inherent methods the two adapters drifted: Windows
/// never grew `connect_once_until`, and it carried its own copies of the budget
/// and retry interval above. The compiler now compares them.
pub trait Duplex: Sized + peer::PeerCredentials {
    /// Receive half owned by the downstream pump.
    type Reader: io::Read + Send + 'static;
    /// Send half with an explicit half-close operation.
    type Writer: crate::transport::WriteHalf;

    /// Make one connection attempt bounded by an absolute deadline.
    ///
    /// Planned handoff wants exactly this: one try at one endpoint, without
    /// spending the recovery retry budget on an endpoint that has moved.
    fn connect_once_until(endpoint: &Endpoint, deadline: Instant) -> Result<Self, ConnectError>;

    /// Retry availability until the deadline passes.
    ///
    /// Only [`ConnectError::is_transient`] failures are retried: a denial, an
    /// endpoint too long to address or any other OS failure does not resolve by
    /// waiting, and spending the budget first only makes the diagnosis slower.
    fn connect_until(endpoint: &Endpoint, deadline: Instant) -> Result<Self, ConnectError> {
        loop {
            match Self::connect_once_until(endpoint, deadline) {
                Ok(connected) => return Ok(connected),
                Err(error) if !error.is_transient() => return Err(error),
                Err(error) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(error);
                    }
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "A missing endpoint has no event to await; retry connects within the caller deadline."
                    )]
                    std::thread::sleep(RETRY_INTERVAL.min(remaining));
                }
            }
        }
    }

    /// One attempt under [`CONNECT_BUDGET`].
    fn connect_once(endpoint: &Endpoint) -> Result<Self, ConnectError> {
        Self::connect_once_until(endpoint, Instant::now() + CONNECT_BUDGET)
    }

    /// Retry under [`CONNECT_BUDGET`].
    fn connect(endpoint: &Endpoint) -> Result<Self, ConnectError> {
        Self::connect_until(endpoint, Instant::now() + CONNECT_BUDGET)
    }

    /// Arm or clear one absolute deadline for session establishment.
    fn set_read_deadline(&self, timeout: Option<Duration>) -> io::Result<()>;

    /// Split ownership without serializing reads and writes.
    fn split(self) -> io::Result<(Self::Reader, Self::Writer)>;
}

/// The outcome of trying to own an endpoint. Busy is ownership evidence, never
/// application readiness proof.
///
/// Shared across platforms because the outcome has the same shape everywhere.
/// `BindError` deliberately is not: a Unix socket can fail to prepare its parent
/// directory or to unlink a proven stale inode, and a named pipe has neither.
#[derive(Debug)]
pub enum Bound<L> {
    /// The caller owns this listener.
    Won(L),
    /// A listener or another serialized binder already owns the endpoint.
    AlreadyRunning,
}

/// This platform's [`Duplex`].
///
/// Safe to name without a `cfg` because the trait fixes what both adapters
/// offer; they cannot drift apart behind this alias.
#[cfg(unix)]
pub use unix::UnixDuplex as PlatformDuplex;
/// This platform's [`Duplex`].
#[cfg(windows)]
pub use windows::WindowsDuplex as PlatformDuplex;

pub(crate) use process::wait_child;
pub use process::{ProcessHandle, process_has_exited, process_state};
// Same name, same signature on both platforms. Everything else in `unix` and
// `windows` differs (uid and child-reaping helpers on one side, a session-end
// handler and an inherit guard on the other) and stays behind its module.
#[cfg(unix)]
pub use unix::terminate_legacy_peer;
#[cfg(windows)]
pub use windows::terminate_legacy_peer;

/// What the OS can establish about a PID.
///
/// `Unknown` is its own state because the two useful answers need proof in
/// opposite directions. Retiring a process's work needs proof it exited;
/// treating it as still serving needs proof it runs. A denied or failed query
/// proves neither, and a caller that folds it into either side eventually acts
/// on a guess: waiting forever for a process that is gone, or starting a
/// second owner next to one that is not.
///
/// `Alive` is about a PID, not a program: a PID is reused once its process is
/// gone, on Unix and on Windows alike, so a PID that reads as alive may belong
/// to an unrelated process by now. Before treating it as the daemon you
/// started, confirm through its endpoint (connect and check the peer). To
/// follow one process rather than a PID, keep a [`ProcessHandle`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProcessState {
    /// A process with this PID exists and has not exited. It may be a
    /// different process from the one that first had the PID.
    Alive,
    /// The process exited, or no process has this PID any more.
    Exited,
    /// The OS did not say, for example because access to the process was
    /// denied. Neither alive nor exited can be assumed.
    Unknown,
}

/// Listener ownership for this platform.
///
/// Both sides now offer `acquire` under `local`, returning a blocking listener.
/// Only Windows also offers `acquire_tokio`: a named pipe needs the Tokio
/// wrapper chosen at creation, while a Unix descriptor converts to one
/// afterwards, so asking this crate to do it would pull `interprocess` into
/// every Unix build to save the caller two lines.
#[cfg(unix)]
pub use unix_socket as socket;
/// Listener ownership for this platform.
#[cfg(windows)]
pub use windows_socket as socket;

/// Connection establishment failure, before application traffic is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectError {
    /// Nothing accepted the connection in time: the endpoint does not exist
    /// yet, refused the connection, is busy, or the deadline passed.
    ConnectTimeout,
    /// The OS denied access to the endpoint.
    PermissionDenied,
    /// The endpoint path does not fit in a Unix socket address, so no attempt
    /// can succeed. See [`crate::socket_path`].
    EndpointTooLong,
    /// The OS failed the attempt for another reason, which waiting does not fix.
    Failed(io::ErrorKind),
}
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectTimeout => f.write_str("daemon connection timed out"),
            Self::PermissionDenied => f.write_str("daemon endpoint permission denied"),
            Self::EndpointTooLong => {
                f.write_str("daemon endpoint path is too long for a Unix socket")
            }
            Self::Failed(kind) => write!(f, "daemon connection failed: {kind}"),
        }
    }
}
impl std::error::Error for ConnectError {}
impl ConnectError {
    /// Whether a later attempt can succeed: nothing accepted the connection in
    /// time, which includes an endpoint that does not exist yet.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::ConnectTimeout)
    }
}

/// Classify a failed connection attempt, the same way on every platform.
pub(crate) fn connect_error(error: &io::Error) -> ConnectError {
    use io::ErrorKind::{
        ConnectionAborted, ConnectionRefused, ConnectionReset, Interrupted, NotConnected, NotFound,
        PermissionDenied, TimedOut, WouldBlock,
    };
    match error.kind() {
        PermissionDenied => ConnectError::PermissionDenied,
        NotFound | ConnectionRefused | TimedOut | WouldBlock | Interrupted | ConnectionReset
        | ConnectionAborted | NotConnected => ConnectError::ConnectTimeout,
        kind => ConnectError::Failed(kind),
    }
}

pub mod peer;
mod process;
#[cfg(unix)]
pub mod unix;
#[cfg(unix)]
pub mod unix_socket;
#[cfg(windows)]
pub mod windows;

#[cfg(test)]
fn test_handshake(write: &mut impl io::Write, read: &mut impl io::Read, _: &str) -> io::Result<()> {
    write.write_all(b"hello\n")?;
    write.flush()?;
    let mut reply = [0; 6];
    read.read_exact(&mut reply)?;
    if reply != *b"ready\n" {
        return Err(io::Error::other("invalid test reply"));
    }
    Ok(())
}

#[cfg(windows)]
pub mod windows_socket;

#[cfg(any(all(windows, feature = "launch"), test))]
pub(crate) mod command_line;
#[cfg(all(windows, feature = "launch"))]
pub(crate) mod windows_spawn;

#[cfg(test)]
mod connect_error_tests {
    use super::{ConnectError, connect_error};
    use std::io::{self, ErrorKind};

    #[test]
    fn only_an_endpoint_nothing_serves_yet_is_worth_retrying() {
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::ConnectionRefused,
            ErrorKind::TimedOut,
            ErrorKind::WouldBlock,
            ErrorKind::Interrupted,
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::NotConnected,
        ] {
            let error = connect_error(&kind.into());
            assert_eq!(error, ConnectError::ConnectTimeout, "{kind:?}");
            assert!(error.is_transient());
        }
        assert_eq!(
            connect_error(&ErrorKind::PermissionDenied.into()),
            ConnectError::PermissionDenied
        );
        let other = connect_error(&ErrorKind::InvalidInput.into());
        assert_eq!(other, ConnectError::Failed(ErrorKind::InvalidInput));
        assert!(!other.is_transient());
        assert!(!ConnectError::PermissionDenied.is_transient());
        assert!(!ConnectError::EndpointTooLong.is_transient());
    }

    #[test]
    fn a_busy_pipe_is_retried_only_on_windows() {
        #[cfg(windows)]
        let busy = super::windows::connect_error(&io::Error::from_raw_os_error(231));
        #[cfg(not(windows))]
        let busy = connect_error(&io::Error::from_raw_os_error(231));
        if cfg!(windows) {
            assert_eq!(busy, ConnectError::ConnectTimeout);
        } else {
            assert_ne!(busy, ConnectError::ConnectTimeout);
        }
    }

    #[test]
    fn a_temporary_connection_failure_is_retried_until_the_endpoint_accepts() {
        use super::{Duplex, Endpoint, PlatformDuplex, peer::PeerCredentials};
        use std::sync::atomic::{AtomicUsize, Ordering};
        static ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
        struct StartsAfterOneAttempt;
        impl PeerCredentials for StartsAfterOneAttempt {
            fn credentials(&self) -> io::Result<crate::peer::Credentials> {
                unreachable!()
            }
        }
        impl Duplex for StartsAfterOneAttempt {
            type Reader = <PlatformDuplex as Duplex>::Reader;
            type Writer = <PlatformDuplex as Duplex>::Writer;
            fn connect_once_until(
                _: &Endpoint,
                _: std::time::Instant,
            ) -> Result<Self, ConnectError> {
                if ATTEMPTS.fetch_add(1, Ordering::Relaxed) == 0 {
                    Err(ConnectError::ConnectTimeout)
                } else {
                    Ok(Self)
                }
            }
            fn set_read_deadline(&self, _: Option<std::time::Duration>) -> io::Result<()> {
                unreachable!()
            }
            fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
                unreachable!()
            }
        }
        #[cfg(unix)]
        let endpoint = std::path::Path::new("unused");
        #[cfg(windows)]
        let endpoint = "unused";
        assert!(
            StartsAfterOneAttempt::connect_until(
                endpoint,
                std::time::Instant::now() + std::time::Duration::from_secs(1)
            )
            .is_ok()
        );
        assert_eq!(ATTEMPTS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn every_failure_says_what_happened() {
        let cases = [
            (ConnectError::ConnectTimeout, "daemon connection timed out"),
            (
                ConnectError::PermissionDenied,
                "daemon endpoint permission denied",
            ),
            (
                ConnectError::EndpointTooLong,
                "daemon endpoint path is too long for a Unix socket",
            ),
            (
                ConnectError::Failed(ErrorKind::InvalidInput),
                "daemon connection failed: invalid input parameter",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
    }
}

mod readiness;
