//! Named-pipe credentials. Built and tested by the Windows jobs.

use crate::{ProcessId, peer::Credentials};
use std::io;

/// Credentials for a connected named pipe, from either end. See the parent
/// module for what they describe.
pub(crate) fn pipe_credentials(
    stream: &impl interprocess::local_socket::traits::StreamCommon,
) -> io::Result<Credentials> {
    let pid = stream
        .peer_creds()?
        .pid()
        .and_then(ProcessId::new)
        .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))?;
    let same_user = super::super::windows::process_runs_as_this_user(pid)?;
    Ok(Credentials::new(Some(pid), same_user))
}

impl super::PeerCredentials for interprocess::local_socket::Stream {
    fn credentials(&self) -> io::Result<Credentials> {
        pipe_credentials(self)
    }
}

#[cfg(feature = "local-async")]
impl super::PeerCredentials for interprocess::local_socket::tokio::Stream {
    fn credentials(&self) -> io::Result<Credentials> {
        pipe_credentials(self)
    }
}
