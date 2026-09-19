use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::Path;

/// An exclusive OS file lock, released when this value is dropped.
///
/// Every participant must use the same persistent path in an application-owned
/// directory. Neither this type nor a caller may unlink or replace the lock
/// file: another process could still hold a lock on the original inode.
/// Keep this guard alive through endpoint cleanup and the work it protects.
/// Use separate paths for daemon ownership and upgrade serialization.
#[derive(Debug)]
pub struct ProcessLock(File);

impl ProcessLock {
    /// Acquire without waiting, creating the file if necessary.
    ///
    /// Returns `Ok(None)` only for contention; filesystem and locking failures
    /// remain errors. The parent directory must already exist.
    pub fn try_acquire(path: impl AsRef<Path>) -> io::Result<Option<Self>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(Self(file))),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Explicit unlock also releases ownership if an OS-level duplicate of
        // the descriptor survives elsewhere in the process.
        let _ = self.0.unlock();
    }
}
