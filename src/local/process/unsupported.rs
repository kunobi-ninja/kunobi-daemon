//! Unix systems with no exit event this crate uses: every handle follows the
//! PID.

use std::io;
use std::time::Instant;

use super::Opened;

#[derive(Debug)]
pub(super) enum Event {}

pub(super) fn open(_pid: u32) -> io::Result<Opened> {
    Ok(Opened::Unsupported)
}

impl Event {
    pub(super) fn wait(&mut self, _deadline: Option<Instant>) -> io::Result<bool> {
        match *self {}
    }

    #[cfg(feature = "async")]
    pub(super) async fn exited(&mut self) -> io::Result<()> {
        match *self {}
    }
}
