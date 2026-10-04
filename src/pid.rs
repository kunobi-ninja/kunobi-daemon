//! A nonzero operating-system process ID, used wherever the crate names a
//! process.

use std::{fmt, num::NonZeroU32, process::Child};

/// A nonzero operating-system process ID.
///
/// PID 0 names no single process: on Unix it addresses the caller's process
/// group, so every API that signals, watches or compares a process takes this
/// type instead of a raw `u32`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessId(NonZeroU32);

impl ProcessId {
    /// `None` for PID 0.
    pub const fn new(pid: u32) -> Option<Self> {
        match NonZeroU32::new(pid) {
            Some(pid) => Some(Self(pid)),
            None => None,
        }
    }

    /// This process.
    pub fn current() -> Self {
        Self::new(std::process::id()).expect("a running process has a nonzero PID")
    }

    /// A child this process spawned.
    pub fn of(child: &Child) -> Self {
        Self::new(child.id()).expect("a spawned child has a nonzero PID")
    }

    /// The raw PID.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_zero_is_not_a_process() {
        assert_eq!(ProcessId::new(0), None);
        assert_eq!(ProcessId::new(7).unwrap().get(), 7);
        assert_eq!(ProcessId::new(7).unwrap().to_string(), "7");
    }

    #[test]
    fn current_and_child_pids_come_from_the_os() {
        assert_eq!(ProcessId::current().get(), std::process::id());
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--list")
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        assert_eq!(ProcessId::of(&child).get(), child.id());
        child.wait().unwrap();
    }
}
