//! The process at the other end of an accepted local connection.
//!
//! Read the PID while the connection is open and before dispatching anything
//! privileged, then compare it with [`crate::peer::match_pid`]. The PID is
//! connection evidence, not executable identity: see [`crate::peer`].

use crate::peer::ProcessId;
use std::io;

/// The process that opened an accepted Unix-socket connection, after checking
/// that it runs as this OS user.
///
/// Linux reports the credentials captured when the connection was made.
/// macOS reports the socket's most recent owner, which can differ after the
/// descriptor is passed to or inherited by another process.
#[cfg(unix)]
pub fn accepted_peer(fd: std::os::fd::BorrowedFd<'_>) -> io::Result<ProcessId> {
    use std::os::fd::AsRawFd;
    let raw = fd.as_raw_fd();
    if super::unix::peer_uid(raw)? != super::unix::own_uid() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    ProcessId::new(super::unix::peer_pid(raw)?)
        .ok_or_else(|| io::Error::other("local socket peer has no PID"))
}

/// The client process of an accepted named-pipe connection, after checking
/// that it runs as this OS user.
///
/// The pipe reports the client's PID; a client can still pass or duplicate
/// its handle to another process afterwards.
#[cfg(windows)]
pub fn accepted_peer(
    stream: &impl interprocess::local_socket::traits::StreamCommon,
) -> io::Result<ProcessId> {
    let pid = stream
        .peer_creds()?
        .pid()
        .and_then(ProcessId::new)
        .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))?;
    super::windows::verify_process_user(pid.get())?;
    Ok(pid)
}
