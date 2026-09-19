//! Atomic publication of discovery records in an application-owned directory.

use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Replace a discovery record atomically, after syncing its complete contents.
///
/// The parent directory must already exist and belong to the application.
/// Temporary files stay on the same filesystem, are created exclusively, and
/// are removed on failure. On Unix their permissions are set to 0600 before any
/// contents are written. The caller remains responsible for directory security,
/// record format, ownership and eventual discovery repair after a crash. The
/// directory is not synced; this does not promise survival across power loss.
pub fn publish_record(path: &Path, contents: &[u8]) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "discovery path needs a filename",
        )
    })?;
    let (temporary, mut file): (Temporary, File) = loop {
        let mut name = OsString::from(".");
        name.push(filename);
        name.push(format!(
            ".{}.{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let temporary = parent.join(name);
        let mut options = OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary) {
            Ok(file) => break (Temporary(temporary), file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(&temporary.0, path)
}

/// A discovery slot whose publishers and conditional cleanup share a lock.
/// Use the same slot path in every generation. Lock files remain persistent.
#[derive(Clone, Debug)]
pub struct RecordSlot(PathBuf);
impl RecordSlot {
    /// Address an existing application-owned directory. This performs no I/O.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    fn lock(&self) -> io::Result<crate::ProcessLock> {
        let mut lock = self.0.as_os_str().to_owned();
        lock.push(".record.lock");
        crate::readiness::wait_until(
            std::time::Instant::now() + std::time::Duration::from_secs(2),
            |_| crate::ProcessLock::try_acquire(Path::new(&lock)),
        )?
        .ok_or_else(|| io::ErrorKind::TimedOut.into())
    }

    /// Atomically publish while excluding conditional cleanup by older owners.
    pub fn replace(&self, contents: &[u8]) -> io::Result<()> {
        let _lock = self.lock()?;
        publish_record(&self.0, contents)
    }

    /// Restore an owned record only if its contents differ. The caller must
    /// still be authorized to advertise this endpoint; a lock does not elect it.
    pub fn repair(&self, contents: &[u8]) -> io::Result<bool> {
        let _lock = self.lock()?;
        if self.matches(contents)? {
            return Ok(false);
        }
        publish_record(&self.0, contents)?;
        Ok(true)
    }

    /// Remove only the exact record still belonging to this owner. A successor's
    /// publication cannot race between the comparison and unlink.
    pub fn remove_if_matches(&self, contents: &[u8]) -> io::Result<bool> {
        let _lock = self.lock()?;
        if !self.matches(contents)? {
            return Ok(false);
        }
        std::fs::remove_file(&self.0)?;
        Ok(true)
    }

    fn matches(&self, contents: &[u8]) -> io::Result<bool> {
        let file = match File::open(&self.0) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(error),
        };
        let mut current = Vec::new();
        file.take(contents.len().saturating_add(1) as u64)
            .read_to_end(&mut current)?;
        Ok(current == contents)
    }
}
