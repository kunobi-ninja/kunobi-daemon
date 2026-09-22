//! Optional OS adapters. Unsafe calls are confined to these platform boundaries.
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
pub trait Duplex: Sized {
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
    /// A denial short-circuits: permissions do not resolve by waiting, and
    /// spending the budget first only makes the diagnosis slower.
    fn connect_until(endpoint: &Endpoint, deadline: Instant) -> Result<Self, ConnectError> {
        loop {
            match Self::connect_once_until(endpoint, deadline) {
                Ok(connected) => return Ok(connected),
                Err(ConnectError::PermissionDenied) => return Err(ConnectError::PermissionDenied),
                Err(error) => {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(error);
                    }
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

    /// Reject another OS user before sending a preamble or handoff token.
    fn verify_peer_user(&self) -> io::Result<()>;

    /// PID of the process at the other end of this live connection.
    ///
    /// Stronger than trusting a discovery file: the open connection pins the
    /// peer while the kernel reports its credentials.
    fn peer_pid(&self) -> io::Result<u32>;

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

// Same name, same signature on both platforms. Everything else in `unix` and
// `windows` differs (uid and child-reaping helpers on one side, a session-end
// handler and an inherit guard on the other) and stays behind its module.
#[cfg(unix)]
pub use unix::{process_has_exited, terminate_legacy_peer};
#[cfg(windows)]
pub use windows::{process_has_exited, terminate_legacy_peer};

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
pub enum ConnectError {
    /// No connection was established within the configured setup budget.
    ConnectTimeout,
    /// The OS denied access to the endpoint.
    PermissionDenied,
}
impl std::fmt::Display for ConnectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ConnectTimeout => "daemon connection timed out",
            Self::PermissionDenied => "daemon endpoint permission denied",
        })
    }
}
impl std::error::Error for ConnectError {}

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
