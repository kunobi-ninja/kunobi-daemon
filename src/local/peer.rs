//! Read [`Credentials`] for the other end of a local connection.
//!
//! [`PeerCredentials`] serves a listener checking an accepted connection and a
//! client checking the service it reached. Pass the result to
//! [`crate::peer::authenticate`] with a [`crate::peer::Policy`], while the
//! connection is still open and before dispatching anything. See
//! [`crate::peer`] for what the credentials do and do not prove.
//!
//! On Unix, `same_user` compares this process's effective user with the user
//! the kernel recorded for the connection; failing to read it is an error,
//! never a same-user result. `pid` is `None` when the kernel reports none, for
//! example for a Linux peer in another PID namespace, which
//! [`crate::peer::ExpectedProcess`] rejects. Linux reports the PID and user
//! captured when the peer connected, listened or created the socket pair.
//! macOS reports the user captured then but the PID of the socket's most recent
//! owner, so after a descriptor is passed or inherited the two can describe
//! different processes.
//!
//! On Windows, a server sees its client's PID and a client sees the server's.
//! `same_user` compares that process's token user with this process's: a
//! lookup by PID, so if the original peer has exited while another process
//! holds its handle, a process that reused the PID is checked instead. A pipe
//! that reports no PID, or a process that cannot be queried, is an error.

use crate::peer::Credentials;
use std::io;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::pipe_credentials;

/// A connection that can report [`Credentials`] for its peer: the crate's
/// adapters, std and Tokio Unix streams, and `interprocess` local sockets on
/// Windows. [`super::Duplex`] and the accept loop in `serve` require it.
pub trait PeerCredentials {
    /// What the OS reports about the other end. See the module docs.
    fn credentials(&self) -> io::Result<Credentials>;
}

#[cfg(unix)]
impl PeerCredentials for std::os::unix::net::UnixStream {
    fn credentials(&self) -> io::Result<Credentials> {
        use std::os::fd::AsRawFd;
        super::unix::fd_credentials(self.as_raw_fd())
    }
}

#[cfg(all(unix, feature = "async"))]
impl PeerCredentials for tokio::net::UnixStream {
    fn credentials(&self) -> io::Result<Credentials> {
        use std::os::fd::AsRawFd;
        super::unix::fd_credentials(self.as_raw_fd())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::ProcessId;

    #[test]
    fn a_socket_pair_reports_this_process_as_the_same_user_peer() {
        let (left, right) = std::os::unix::net::UnixStream::pair().unwrap();
        for end in [&left, &right] {
            assert_eq!(
                end.credentials().unwrap(),
                Credentials::new(Some(ProcessId::current()), true)
            );
        }
    }
}
