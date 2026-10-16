use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::path::Path;
use std::time::Instant;

#[cfg(all(feature = "local", unix))]
use crate::local::unix::open_private;
#[cfg(all(feature = "local", windows))]
use crate::local::windows::open_private;

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
        Self::try_from_file(open_or_create(path.as_ref())?)
    }

    /// Acquire, waiting until `deadline` for the holder to release the lock.
    ///
    /// Opens the file as [`Self::try_acquire`] does. Returns `Ok(None)` if the
    /// lock is still held at the deadline; a deadline already passed still gets
    /// one attempt, and no later attempt is made after it. The OS signals no
    /// release, so contention is rechecked after a pause that grows to 50 ms.
    pub fn acquire_until(path: impl AsRef<Path>, deadline: Instant) -> io::Result<Option<Self>> {
        Self::lock_until(open_or_create(path.as_ref())?, deadline)
    }

    /// [`Self::acquire_until`] for a lock file private to this user.
    ///
    /// Refuses a lock file that is a symlink, or a junction on Windows. On Unix
    /// the file must also belong to the effective user, and its mode becomes
    /// 0600. Either refusal is `PermissionDenied`. Directories on the path are
    /// not checked: keep the lock in a private application-owned directory.
    /// On Windows nothing about access is checked: a new file inherits the
    /// directory's ACL, but an existing lock file keeps its own, so the caller
    /// must secure a lock file it did not create.
    #[cfg(feature = "local")]
    pub fn acquire_private_until(
        path: impl AsRef<Path>,
        deadline: Instant,
    ) -> io::Result<Option<Self>> {
        Self::lock_until(open_private(path.as_ref())?, deadline)
    }

    /// Lock an already-open file. This preserves a consumer's secure open policy.
    pub fn try_from_file(file: File) -> io::Result<Option<Self>> {
        Ok(if locked(&file)? {
            Some(Self(file))
        } else {
            None
        })
    }

    /// Observe an existing lock without creating a file. Only contention is true.
    ///
    /// The check takes the lock and releases it at once. A process that tries
    /// to acquire it during that moment sees contention.
    pub fn is_held(path: impl AsRef<Path>) -> io::Result<bool> {
        let Some(file) = open_existing(path.as_ref())? else {
            return Ok(false);
        };
        Ok(Self::try_from_file(file)?.is_none())
    }

    /// Wait until no process holds the lock, or `deadline` passes. True once
    /// it is free; a missing lock file is free, and none is created.
    ///
    /// Like [`Self::is_held`], each check takes the lock for a moment, and the
    /// lock is not kept: another process may take it before the caller acts.
    /// Failing to open the file is an error, not a release.
    pub fn wait_released_until(path: impl AsRef<Path>, deadline: Instant) -> io::Result<bool> {
        let Some(file) = open_existing(path.as_ref())? else {
            return Ok(true);
        };
        // Dropping the guard unlocks explicitly before the file closes.
        Ok(Self::lock_until(file, deadline)?.is_some())
    }

    /// Acquire, rechecking after each pause of the [`Self::acquire_until`]
    /// schedule until the lock is free. The caller's timeout bounds the wait.
    #[cfg(feature = "async")]
    pub(crate) async fn acquire_async(path: &Path) -> io::Result<Self> {
        let file = open_or_create(path)?;
        let mut pause = crate::backoff::FIRST;
        while !locked(&file)? {
            if let Some(nap) = crate::backoff::next_pause(&mut pause, None) {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "The OS signals no file-lock release; pauses grow to 50 ms and the caller's timeout bounds the wait."
                )]
                tokio::time::sleep(nap).await;
            }
        }
        Ok(Self(file))
    }

    fn lock_until(file: File, deadline: Instant) -> io::Result<Option<Self>> {
        let acquired = crate::backoff::poll(|| locked(&file), Some(deadline))?;
        Ok(if acquired { Some(Self(file)) } else { None })
    }
}

/// Take the lock if it is free. Contention is `false`, not an error.
fn locked(file: &File) -> io::Result<bool> {
    match file.try_lock() {
        Ok(()) => Ok(true),
        Err(TryLockError::WouldBlock) => Ok(false),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn open_or_create(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path)
}

/// Open the lock file if it exists. A missing file is `None`.
fn open_existing(path: &Path) -> io::Result<Option<File>> {
    match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

impl Drop for ProcessLock {
    fn drop(&mut self) {
        // Explicit unlock also releases ownership if an OS-level duplicate of
        // the descriptor survives elsewhere in the process.
        let _ = self.0.unlock();
    }
}
