use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::Path;
use std::time::Instant;

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

    /// Acquire, waiting until `deadline` for the holder to release the lock.
    ///
    /// Opens the file as [`Self::try_acquire`] does. Returns `Ok(None)` if the
    /// lock is still held at the deadline; a deadline already passed still gets
    /// one attempt. The OS signals no release, so contention is rechecked after
    /// a pause that grows to 50 ms.
    pub fn acquire_until(path: impl AsRef<Path>, _deadline: Instant) -> io::Result<Option<Self>> {
        Self::try_acquire(path)
    }

    /// [`Self::acquire_until`] for a lock file private to this user.
    ///
    /// Refuses a lock file that is a symlink, or a junction on Windows. On Unix
    /// the file must also belong to the effective user, and its mode becomes
    /// 0600. Either refusal is `PermissionDenied`. Directories on the path are
    /// not checked: keep the lock in a private application-owned directory,
    /// whose ACL is also what makes the file private on Windows.
    #[cfg(feature = "local")]
    pub fn acquire_private_until(
        path: impl AsRef<Path>,
        deadline: Instant,
    ) -> io::Result<Option<Self>> {
        Self::acquire_until(path, deadline)
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
    ///
    /// The check takes the lock and releases it at once. A process that tries
    /// to acquire it during that moment sees contention.
    pub fn is_held(path: impl AsRef<Path>) -> io::Result<bool> {
        let file = match OpenOptions::new().read(true).write(true).open(path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(Self::try_from_file(file)?.is_none())
    }

    /// Wait until no process holds the lock, or `deadline` passes. True once
    /// it is free; a missing lock file is free, and none is created.
    ///
    /// Like [`Self::is_held`], each check takes the lock for a moment, and the
    /// lock is not kept: another process may take it before the caller acts.
    /// Failing to open the file is an error, not a release.
    pub fn wait_released_until(path: impl AsRef<Path>, _deadline: Instant) -> io::Result<bool> {
        Ok(!Self::is_held(path)?)
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Explicit unlock also releases ownership if an OS-level duplicate of
        // the descriptor survives elsewhere in the process.
        let _ = self.0.unlock();
    }
}
