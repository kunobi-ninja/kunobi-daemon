//! Atomic publication of discovery records in an application-owned directory.

use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(windows)]
#[allow(unsafe_code)]
mod windows;
#[cfg(windows)]
use windows::{open_record, replace};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Temporary(PathBuf);
impl Drop for Temporary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// How a record is written and read. The default is what [`publish_record`]
/// and [`RecordSlot`] have always done.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordOptions {
    durable: bool,
    #[cfg(unix)]
    mode: u32,
    read_limit: u64,
}

impl Default for RecordOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl RecordOptions {
    /// Not durable, mode 0600, and a 64 KiB read limit.
    pub const fn new() -> Self {
        Self {
            durable: false,
            #[cfg(unix)]
            mode: 0o600,
            read_limit: 64 * 1024,
        }
    }

    /// Make the rename itself survive a crash, not only the contents.
    ///
    /// On Unix the parent directory is synced after the rename. Windows has no
    /// directory sync, so the record is flushed again after the rename. An
    /// error from that step means the record is in place but may not survive
    /// a crash.
    pub const fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Permission bits of a published record, set before any contents are
    /// written. Choosing anything wider than 0600 is the caller's decision.
    #[cfg(unix)]
    pub const fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    /// Largest record [`RecordSlot::read`], [`RecordSlot::replace_if`] and
    /// [`RecordSlot::remove_if`] read, in bytes. A longer one is refused.
    /// `repair` and `remove_if_matches` keep their original comparison.
    pub const fn read_limit(mut self, bytes: u64) -> Self {
        self.read_limit = bytes;
        self
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
/// With `durable`, [`publish_record_with`] syncs it.
///
/// A reader reads either the old record or the new one, never part of one.
/// On Windows the temporary file is renamed through its own handle with POSIX
/// semantics, falling back to `std::fs::rename` on a file system without
/// them. Neither makes the replacement atomic for an open running at the same
/// time: the open can fail with `PermissionDenied`, or find no record while
/// the rename is in progress. [`RecordSlot::read`] and the slot's other reads
/// retry both; any other reader must too. The publisher keeps its handle open
/// for a moment after the rename: a reader that does not share write and
/// delete access can get a sharing violation then. `File::open` shares both.
pub fn publish_record(path: &Path, contents: &[u8]) -> io::Result<()> {
    publish_record_with(path, contents, RecordOptions::new())
}

/// [`publish_record`] with a chosen mode and durability.
pub fn publish_record_with(path: &Path, contents: &[u8], options: RecordOptions) -> io::Result<()> {
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
        let mut open = OpenOptions::new();
        open.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open.mode(0o600);
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            open.access_mode(windows::ACCESS);
        }
        match open.open(&temporary) {
            Ok(file) => break (Temporary(temporary), file),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e),
        }
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(options.mode))?;
    }
    file.write_all(contents)?;
    file.sync_all()?;
    replace(file, &temporary.0, path, options.durable)
}

/// Close the written temporary file and rename it over the record. With
/// `durable`, sync the directory, so the rename survives a crash.
#[cfg(not(windows))]
fn replace(file: File, from: &Path, to: &Path, durable: bool) -> io::Result<()> {
    drop(file);
    std::fs::rename(from, to)?;
    if durable {
        // The temporary is named inside the record's directory.
        let directory = from.parent().expect("a temporary has a directory");
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

/// Open a record. A Unix rename replaces the name in one step, so an open
/// finds the old record or the new one.
#[cfg(not(windows))]
fn open_record(open: &OpenOptions, path: &Path) -> io::Result<File> {
    open.open(path)
}

/// Read a record that must be a regular file no longer than `limit` bytes.
/// Missing is `None`.
///
/// The record is opened first and checked through that handle, so what is
/// read is the version that was opened. A symlink is not followed (Unix
/// `O_NOFOLLOW`, Windows `FILE_FLAG_OPEN_REPARSE_POINT`), and on Unix the open
/// does not wait for a writer if the record is a FIFO.
fn read_regular(path: &Path, limit: u64) -> io::Result<Option<Vec<u8>>> {
    let not_regular = || io::Error::new(io::ErrorKind::InvalidData, "record is not a regular file");
    let mut open = OpenOptions::new();
    open.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        open.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        // FILE_FLAG_OPEN_REPARSE_POINT (0x0020_0000) and
        // FILE_FLAG_BACKUP_SEMANTICS (0x0200_0000): open a link itself, and a
        // directory at all, so both are refused below.
        open.custom_flags(0x0220_0000);
    }
    let file = match open_record(&open, path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        // O_NOFOLLOW reports a symlink as ELOOP.
        #[cfg(unix)]
        Err(error) if error.raw_os_error() == Some(libc::ELOOP) => return Err(not_regular()),
        Err(error) => return Err(error),
    };
    if !file.metadata()?.is_file() {
        return Err(not_regular());
    }
    let mut contents = Vec::new();
    file.take(limit.saturating_add(1))
        .read_to_end(&mut contents)?;
    if contents.len() as u64 > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "record is longer than its read limit",
        ));
    }
    Ok(Some(contents))
}

/// Create `dir` if needed and check that it is private to this user.
///
/// The directory itself must not be a symlink, or a junction on Windows. On
/// Unix it must belong to the effective user, and its mode becomes 0700.
/// Either refusal is `PermissionDenied`. Ancestors are not checked. On
/// Windows, the ACL the directory inherits decides who else may use it.
#[cfg(feature = "local")]
pub fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let metadata = std::fs::symlink_metadata(dir)?;
    if !metadata.is_dir() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != crate::local::unix::own_uid() {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// A discovery slot whose publishers and conditional cleanup share a lock.
/// Use the same slot path in every generation. Lock files remain persistent.
#[derive(Clone, Debug)]
pub struct RecordSlot(PathBuf, RecordOptions);
impl RecordSlot {
    /// Address an existing application-owned directory. This performs no I/O.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self(path.into(), RecordOptions::new())
    }

    /// Publish and read with `options` instead of the defaults.
    pub fn options(mut self, options: RecordOptions) -> Self {
        self.1 = options;
        self
    }

    fn lock(&self) -> io::Result<crate::ProcessLock> {
        let mut lock = self.0.as_os_str().to_owned();
        lock.push(".record.lock");
        crate::ProcessLock::acquire_until(
            &lock,
            std::time::Instant::now() + std::time::Duration::from_secs(2),
        )?
        .ok_or_else(|| io::ErrorKind::TimedOut.into())
    }

    /// Atomically publish while excluding conditional cleanup by older owners.
    pub fn replace(&self, contents: &[u8]) -> io::Result<()> {
        let _lock = self.lock()?;
        publish_record_with(&self.0, contents, self.1)
    }

    /// Restore an owned record only if its contents differ. The caller must
    /// still be authorized to advertise this endpoint; a lock does not elect it.
    pub fn repair(&self, contents: &[u8]) -> io::Result<bool> {
        let _lock = self.lock()?;
        if self.matches(contents)? {
            return Ok(false);
        }
        publish_record_with(&self.0, contents, self.1)?;
        Ok(true)
    }

    /// Publish `contents` only if `replace` accepts the current record, or
    /// `None` when there is none. The record is read and the predicate runs
    /// under the slot lock, so no other slot user changes it in between.
    /// True if published.
    pub fn replace_if(
        &self,
        contents: &[u8],
        replace: impl FnOnce(Option<&[u8]>) -> bool,
    ) -> io::Result<bool> {
        let _lock = self.lock()?;
        let current = read_regular(&self.0, self.1.read_limit)?;
        if !replace(current.as_deref()) {
            return Ok(false);
        }
        publish_record_with(&self.0, contents, self.1)?;
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

    /// Remove the record only if `remove` accepts its contents, under the
    /// slot lock. A missing record is false, without calling the predicate.
    pub fn remove_if(&self, remove: impl FnOnce(&[u8]) -> bool) -> io::Result<bool> {
        let _lock = self.lock()?;
        let Some(current) = read_regular(&self.0, self.1.read_limit)? else {
            return Ok(false);
        };
        if !remove(&current) {
            return Ok(false);
        }
        std::fs::remove_file(&self.0)?;
        Ok(true)
    }

    /// Read the record without the slot lock: publication is atomic, so this
    /// sees one complete version. `None` when there is none. A record that
    /// is not a regular file, a symlink included, or that is longer than the
    /// read limit is `InvalidData`.
    ///
    /// On Windows an open that races a replacement can be refused, or find
    /// no record while the rename is in progress. A refused open is retried a
    /// few times. A missing record is waited for, up to 2 s, while a
    /// temporary file of this record created in the last 10 s shows a
    /// publication in progress; otherwise it is `None` at once.
    pub fn read(&self) -> io::Result<Option<Vec<u8>>> {
        read_regular(&self.0, self.1.read_limit)
    }

    fn matches(&self, contents: &[u8]) -> io::Result<bool> {
        let file = match open_record(OpenOptions::new().read(true), &self.0) {
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
