//! Collect [`Evidence`] about the other end of a local connection.
//!
//! The same call serves a listener checking an accepted connection and a client
//! checking the service it reached. Pass the result to
//! [`crate::peer::authenticate`] with a [`crate::peer::Policy`], while the
//! connection is still open and before dispatching anything. See
//! [`crate::peer`] for what the evidence does and does not prove.

use crate::peer::{Evidence, ProcessId};
use std::io;

/// Evidence for a connected Unix socket, from either end.
///
/// Failing to read the peer's user is an error, never a same-user result. A
/// peer PID the platform cannot report is recorded as `None`, which
/// [`crate::peer::ExpectedProcess`] rejects. Linux reports the credentials
/// captured when the connection was made; macOS reports the socket's most
/// recent owner, which can differ after the descriptor is passed or inherited.
#[cfg(unix)]
pub fn evidence(fd: std::os::fd::BorrowedFd<'_>) -> io::Result<Evidence> {
    use std::os::fd::AsRawFd;
    let raw = fd.as_raw_fd();
    let same_user = super::unix::peer_uid(raw)? == super::unix::own_uid();
    let pid = super::unix::peer_pid(raw).ok().and_then(ProcessId::new);
    Ok(Evidence::new(pid, same_user))
}

/// Evidence for a connected named pipe, from either end: a server sees its
/// client and a client sees the server.
///
/// The user is checked against this process's token through the peer PID, so
/// a pipe that reports no PID is an error rather than unknown evidence. A
/// client can still pass or duplicate its handle to another process.
#[cfg(windows)]
pub fn evidence(
    stream: &impl interprocess::local_socket::traits::StreamCommon,
) -> io::Result<Evidence> {
    let pid = stream
        .peer_creds()?
        .pid()
        .and_then(ProcessId::new)
        .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))?;
    let same_user = match super::windows::verify_process_user(pid.get()) {
        Ok(()) => true,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => false,
        Err(error) => return Err(error),
    };
    Ok(Evidence::new(Some(pid), same_user))
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
        }
    }
}
