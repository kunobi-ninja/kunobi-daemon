//! Named-pipe credentials. Built and tested by the Windows jobs.

use crate::{
    ProcessId,
    peer::{Credentials, UserEvidence},
};
use std::io;
use std::os::windows::io::{AsHandle, AsRawHandle, RawHandle};

/// Errors that mean the connection carries no client user to read:
/// `ERROR_CANNOT_IMPERSONATE`, a client under dynamic context tracking whose
/// first message the server has not read yet, and `ERROR_CANT_OPEN_ANONYMOUS`, a client that allows
/// only anonymous impersonation. Both fall back to the lookup by PID.
const NO_CONNECTION_USER: [i32; 2] = [1368, 1347];

/// Credentials for a connected named pipe. `server_end` is the pipe's handle
/// when this is the server's end, whose client's user is read from the
/// connection when it carries one; otherwise, and on a client's end, the
/// peer's user is looked up by PID. See the parent module for what they
/// describe.
fn pipe_credentials(
    stream: &impl interprocess::local_socket::traits::StreamCommon,
    server_end: Option<RawHandle>,
) -> io::Result<Credentials> {
    let pid = stream
        .peer_creds()?
        .pid()
        .and_then(ProcessId::new)
        .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))?;
    let (same_user, evidence) =
        match server_end.map(super::super::windows::pipe_client_runs_as_this_user) {
            Some(Ok(same_user)) => (same_user, UserEvidence::Connection),
            Some(Err(error))
                if !error
                    .raw_os_error()
                    .is_some_and(|code| NO_CONNECTION_USER.contains(&code)) =>
            {
                return Err(error);
            }
            _ => (
                super::super::windows::process_runs_as_this_user(pid)?,
                UserEvidence::PidLookup,
            ),
        };
    Ok(Credentials::new(Some(pid), same_user).with_user_evidence(evidence))
}

impl super::PeerCredentials for interprocess::local_socket::Stream {
    fn credentials(&self) -> io::Result<Credentials> {
        let Self::NamedPipe(pipe) = self;
        let server_end = pipe
            .inner()
            .is_server()
            .then(|| pipe.as_handle().as_raw_handle());
        pipe_credentials(self, server_end)
    }
}

#[cfg(feature = "local-async")]
impl super::PeerCredentials for interprocess::local_socket::tokio::Stream {
    fn credentials(&self) -> io::Result<Credentials> {
        let Self::NamedPipe(pipe) = self;
        let server_end = pipe
            .inner()
            .is_server()
            .then(|| pipe.as_handle().as_raw_handle());
        pipe_credentials(self, server_end)
    }
}
