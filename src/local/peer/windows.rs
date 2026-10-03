//! Named-pipe evidence. Built and tested by the Windows jobs.

use crate::peer::{Evidence, ProcessId};
use std::io;

/// Evidence for a connected named pipe, from either end: a server sees its
/// client's PID and a client sees the server's.
///
/// `same_user` compares the token user of the process with that PID against
/// this process's token user. That is a lookup by PID: if the connection's
/// original peer has exited while another process holds a duplicated or
/// inherited handle, a process that reused the PID is checked instead. A pipe
/// that reports no PID, or a process that cannot be queried, is an error rather
/// than evidence.
pub fn evidence(
    stream: &impl interprocess::local_socket::traits::StreamCommon,
) -> io::Result<Evidence> {
    let pid = stream
        .peer_creds()?
        .pid()
        .and_then(ProcessId::new)
        .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))?;
    let same_user = super::super::windows::process_runs_as_this_user(pid.get())?;
    Ok(Evidence::new(Some(pid), same_user))
}

impl super::PeerEvidence for interprocess::local_socket::Stream {
    fn evidence(&self) -> io::Result<Evidence> {
        evidence(self)
    }
}

#[cfg(feature = "local-async")]
impl super::PeerEvidence for interprocess::local_socket::tokio::Stream {
    fn evidence(&self) -> io::Result<Evidence> {
        evidence(self)
    }
}
