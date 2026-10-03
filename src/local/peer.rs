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

/// A connection that can report [`crate::peer::Evidence`] about its peer:
/// the crate's adapters, std and Tokio Unix streams, and `interprocess` local
/// sockets on both platforms. The accept loop in `serve` requires it.
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

/// `interprocess` reports Unix peer credentials itself; same rules as
/// [`evidence`]: an unknown user is an error, an unknown PID is `None`.
#[cfg(unix)]
fn from_creds(creds: interprocess::local_socket::PeerCreds) -> io::Result<Evidence> {
    let uid = creds
        .euid()
        .ok_or_else(|| io::Error::other("local socket peer has no user ID"))?;
    let pid = creds
        .pid()
        .and_then(|pid| u32::try_from(pid).ok())
        .and_then(ProcessId::new);
    Ok(Evidence::new(pid, uid == super::unix::own_uid()))
}

#[cfg(unix)]
impl PeerEvidence for interprocess::local_socket::Stream {
    fn evidence(&self) -> io::Result<Evidence> {
        use interprocess::local_socket::traits::StreamCommon;
        from_creds(self.peer_creds()?)
    }
}

#[cfg(all(unix, feature = "local-async"))]
impl PeerEvidence for interprocess::local_socket::tokio::Stream {
    fn evidence(&self) -> io::Result<Evidence> {
        use interprocess::local_socket::traits::StreamCommon;
        use interprocess::local_socket::traits::tokio::Stream as _;
        from_creds(self.peer_creds()?)
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

    #[test]
    fn an_interprocess_stream_reports_the_same_evidence_as_its_socket() {
        let own = ProcessId::new(std::process::id());
        let (left, _right) = std::os::unix::net::UnixStream::pair().unwrap();
        let stream = interprocess::local_socket::Stream::from(
            interprocess::os::unix::uds_local_socket::Stream::from(left),
        );
        assert_eq!(stream.evidence().unwrap(), Evidence::new(own, true));
    }
}
