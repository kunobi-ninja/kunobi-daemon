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
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        Self::try_from_file(options.open(path)?)
    }

    /// Lock an already-open file. This preserves a consumer's secure open policy.
    pub fn try_from_file(file: File) -> io::Result<Option<Self>> {
        match file.try_lock() {
            Ok(()) => Ok(Some(Self(file))),
            Err(TryLockError::WouldBlock) => Ok(None),
            Err(TryLockError::Error(error)) => Err(error),
        }
    }

    /// Observe an existing lock without creating a file. Only contention is true.
    pub fn is_held(path: impl AsRef<Path>) -> io::Result<bool> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(Self::try_from_file(file)?.is_none())
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Explicit unlock also releases ownership if an OS-level duplicate of
        // the descriptor survives elsewhere in the process.
        let _ = self.0.unlock();
    }
}
