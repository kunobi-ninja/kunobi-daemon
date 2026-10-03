//! Collect [`crate::peer::Evidence`] about the other end of a local connection.
//!
//! The same call serves a listener checking an accepted connection and a client
//! checking the service it reached. Pass the result to
//! [`crate::peer::authenticate`] with a [`crate::peer::Policy`], while the
//! connection is still open and before dispatching anything. The crate's own
//! adapters offer it as `UnixDuplex::evidence` and `WindowsDuplex::evidence`.
//! See [`crate::peer`] for what the evidence does and does not prove.

#[cfg(unix)]
use crate::peer::{Evidence, ProcessId};
#[cfg(unix)]
use std::io;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub use windows::evidence;

/// A connection that can report [`crate::peer::Evidence`] about its peer: the
/// crate's adapters, std and Tokio Unix streams, and `interprocess` local
/// sockets on Windows. The accept loop in `serve` requires it.
pub trait PeerEvidence {
    /// What the OS reports about the other end. See [`evidence`].
    fn evidence(&self) -> std::io::Result<crate::peer::Evidence>;
}

#[cfg(unix)]
impl PeerEvidence for std::os::unix::net::UnixStream {
    fn evidence(&self) -> io::Result<Evidence> {
        use std::os::fd::AsFd;
        evidence(self.as_fd())
    }
}

#[cfg(all(unix, feature = "async"))]
impl PeerEvidence for tokio::net::UnixStream {
    fn evidence(&self) -> io::Result<Evidence> {
        use std::os::fd::AsFd;
        evidence(self.as_fd())
    }
}

/// Evidence for a connected Unix socket, from either end.
///
/// `same_user` compares this process's effective user with the credentials
/// the kernel recorded for the connection. Failing to read them is an error,
/// never a same-user result.
///
/// `pid` is `None` when the kernel reports none, for example for a peer in
/// another PID namespace; [`crate::peer::ExpectedProcess`] rejects that. Linux
/// reports the PID and user captured when the peer connected, listened or
/// created the socket pair. macOS reports the user captured then, but the PID
/// of the socket's most recent owner, so after a descriptor is passed or
/// inherited the two can describe different processes.
#[cfg(unix)]
pub fn evidence(fd: std::os::fd::BorrowedFd<'_>) -> io::Result<Evidence> {
    use std::os::fd::AsRawFd;
    let raw = fd.as_raw_fd();
    let same_user = super::unix::peer_uid(raw)? == super::unix::own_uid();
    let pid = super::unix::peer_pid(raw).ok().and_then(ProcessId::new);
    Ok(Evidence::new(pid, same_user))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::fd::AsFd;

    #[test]
    fn a_socket_pair_reports_this_process_as_the_same_user_peer() {
        let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
        let own = ProcessId::new(std::process::id());
        for end in [&left, &right] {
            assert_eq!(evidence(end.as_fd()).unwrap(), Evidence::new(own, true));
            assert_eq!(end.evidence().unwrap(), Evidence::new(own, true));
        }
    }
}
