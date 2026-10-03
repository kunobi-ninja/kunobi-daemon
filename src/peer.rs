//! Compare the process behind a connection with the process a caller expects.
//!
//! A [`PidMatch`] proves only that two numbers agree: the PID the operating
//! system reports for an established connection, and a PID the caller expected,
//! usually read from a record its peer published. It does not prove who wrote
//! that record, which executable the peer runs, or which process writes later
//! bytes on the connection. A process running as the same OS user can publish
//! its own PID in such a record. Consumers decide what a match grants.
//!
//! With the `local` feature, `local::peer::accepted_peer` reads the PID of an
//! accepted connection.

use std::{fmt, num::NonZeroU32};

/// A nonzero operating-system process ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessId(NonZeroU32);

impl ProcessId {
    /// `None` for PID 0, which names no user process.
    pub const fn new(pid: u32) -> Option<Self> {
        match NonZeroU32::new(pid) {
            Some(pid) => Some(Self(pid)),
            None => None,
        }
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

/// The connection's PID equals the expected PID. See the module docs for what
/// this does not establish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PidMatch {
    pid: ProcessId,
}

impl PidMatch {
    /// The PID both sides agreed on.
    pub const fn pid(&self) -> ProcessId {
        self.pid
    }
}

/// Why a connection did not match the expected process.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum PidMismatch {
    /// The platform reported no PID for the connection. Missing evidence is
    /// never a match.
    Unreported,
    /// The connection belongs to another process.
    Different {
        /// The PID the caller expected.
        expected: ProcessId,
        /// The PID the operating system reported for the connection.
        observed: ProcessId,
    },
}

impl fmt::Display for PidMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreported => f.write_str("the connection's peer PID is not available"),
            Self::Different { expected, observed } => {
                write!(f, "peer PID {observed} is not the expected PID {expected}")
            }
        }
    }
}

impl std::error::Error for PidMismatch {}

/// Compare the PID reported for a connection with the expected process.
pub fn match_pid(
    observed: Option<ProcessId>,
    expected: ProcessId,
) -> Result<PidMatch, PidMismatch> {
    match observed {
        None => Err(PidMismatch::Unreported),
        Some(observed) if observed == expected => Ok(PidMatch { pid: observed }),
        Some(observed) => Err(PidMismatch::Different { expected, observed }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(value: u32) -> ProcessId {
        ProcessId::new(value).unwrap()
    }

    #[test]
    fn pid_zero_is_not_a_process() {
        assert_eq!(ProcessId::new(0), None);
        assert_eq!(pid(7).get(), 7);
    }

    #[test]
    fn only_an_equal_reported_pid_matches() {
        assert_eq!(match_pid(Some(pid(7)), pid(7)).map(|m| m.pid()), Ok(pid(7)));
        assert_eq!(
            match_pid(Some(pid(8)), pid(7)),
            Err(PidMismatch::Different {
                expected: pid(7),
                observed: pid(8)
            })
        );
        assert_eq!(match_pid(None, pid(7)), Err(PidMismatch::Unreported));
    }
}
