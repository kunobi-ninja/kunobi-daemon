//! Replacing a record through the temporary file's own handle, and opening a
//! record that may be being replaced. Built and tested by the Windows jobs.
//!
//! This is the crate's only FFI outside `local`: publication is always built,
//! and `std::fs::rename` first tries `MoveFileExW`, a legacy replacement.
//!
//! No rename makes the replacement atomic for an open running at the same
//! time. Measured on NTFS: an open that reached the old record as the rename
//! deleted it fails with `ERROR_ACCESS_DENIED` (`STATUS_FILE_DELETED`), and
//! while a slow rename is in progress the name can be missing for tens or
//! hundreds of milliseconds, with the temporary file still under its own name.
//! [`open_record`] retries both.

use std::ffi::{OsStr, OsString, c_void};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};
use std::path::{Component, Path, Prefix};
use std::time::{Duration, Instant, SystemTime};

const ERROR_ACCESS_DENIED: i32 = 5;
/// Opens refused with `ERROR_ACCESS_DENIED` that are retried at once. In CI
/// measurements one retry found the new record; more cover replacements in
/// a row.
const DENIED_RETRIES: u32 = 8;
/// How long a missing record is waited for while it is being published.
const PUBLISHING: Duration = Duration::from_secs(2);
/// A temporary file created longer ago than this is left over from a crash,
/// not a publication in progress.
const FRESH: Duration = Duration::from_secs(10);

/// Open the record at `path`, retrying what a concurrent replacement causes.
///
/// `ERROR_ACCESS_DENIED` is retried a few times at once: a lasting denial
/// still returns. `NotFound` is waited out, up to [`PUBLISHING`], only while a
/// recent temporary file of this record shows a publication in progress;
/// without one the record is opened once more and that answer stands.
pub(super) fn open_record(open: &OpenOptions, path: &Path) -> io::Result<File> {
    let mut denied = 0;
    let mut rechecked = false;
    let mut opened = None;
    let mut missing = None;
    crate::backoff::poll(
        || {
            loop {
                match open.open(path) {
                    Ok(file) => {
                        opened = Some(file);
                        return Ok(true);
                    }
                    Err(error)
                        if error.raw_os_error() == Some(ERROR_ACCESS_DENIED)
                            && denied < DENIED_RETRIES =>
                    {
                        denied += 1;
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        if publishing(path) {
                            missing = Some(error);
                            return Ok(false);
                        }
                        if rechecked {
                            return Err(error);
                        }
                        rechecked = true;
                    }
                    Err(error) => return Err(error),
                }
            }
        },
        Some(Instant::now() + PUBLISHING),
    )?;
    opened.ok_or_else(|| missing.unwrap_or_else(|| io::ErrorKind::NotFound.into()))
}

/// Whether a temporary file of `path`, named as `publish_record_with` names
/// it (`.<record>.<pid>.<n>.tmp`), was created within [`FRESH`].
fn publishing(path: &Path) -> bool {
    let (Some(parent), Some(record)) = (path.parent(), path.file_name()) else {
        return false;
    };
    let parent = if parent.as_os_str().is_empty() {
        Path::new(".")
    } else {
        parent
    };
    let Ok(entries) = std::fs::read_dir(parent) else {
        return false;
    };
    let now = SystemTime::now();
    entries.flatten().any(|entry| {
        is_temporary(&entry.file_name(), record)
            // A directory entry carries its times: no open, so the
            // publisher's rename is not disturbed.
            && entry
                .metadata()
                .and_then(|metadata| metadata.created())
                // A creation time ahead of the clock counts as recent.
                .is_ok_and(|created| now.duration_since(created).unwrap_or_default() < FRESH)
    })
}

fn is_temporary(name: &OsStr, record: &OsStr) -> bool {
    let name = name.as_encoded_bytes();
    let record = record.as_encoded_bytes();
    let Some(rest) = name
        .strip_prefix(b".")
        .and_then(|rest| rest.strip_prefix(record))
        .and_then(|rest| rest.strip_prefix(b"."))
        .and_then(|rest| rest.strip_suffix(b".tmp"))
    else {
        return false;
    };
    let mut parts = rest.split(|byte| *byte == b'.');
    let number = |part: Option<&[u8]>| {
        part.is_some_and(|part| !part.is_empty() && part.iter().all(u8::is_ascii_digit))
    };
    number(parts.next()) && number(parts.next()) && parts.next().is_none()
}

const GENERIC_WRITE: u32 = 0x4000_0000;
const DELETE: u32 = 0x0001_0000;
/// Access the temporary file is opened with: write its contents, then rename
/// it through the same handle.
pub(super) const ACCESS: u32 = GENERIC_WRITE | DELETE;

/// `FILE_INFO_BY_HANDLE_CLASS::FileRenameInfoEx`.
const FILE_RENAME_INFO_EX: i32 = 22;
const FILE_RENAME_FLAG_REPLACE_IF_EXISTS: u32 = 0x1;
const FILE_RENAME_FLAG_POSIX_SEMANTICS: u32 = 0x2;
const ERROR_INVALID_FUNCTION: i32 = 1;
const ERROR_NOT_SUPPORTED: i32 = 50;
const ERROR_INVALID_PARAMETER: i32 = 87;

/// `FILE_RENAME_INFO`, with the `Flags` member of its leading union.
#[repr(C)]
struct FileRenameInfo {
    flags: u32,
    root_directory: *mut c_void,
    file_name_length: u32,
    file_name: [u16; 1],
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn SetFileInformationByHandle(
        file: *mut c_void,
        class: i32,
        information: *const c_void,
        size: u32,
    ) -> i32;
}

/// Rename `file`, open at `from`, over `to`.
///
/// POSIX semantics replace the name in one step, so a reader opens the old
/// record or the new one and never one pending deletion. A file system that
/// does not support them gets `std::fs::rename`, and with it the window.
pub(super) fn replace(file: File, from: &Path, to: &Path) -> io::Result<()> {
    replace_with(file, from, to, rename_posix)
}

fn replace_with(
    file: File,
    from: &Path,
    to: &Path,
    rename: impl FnOnce(&File, &Path) -> io::Result<()>,
) -> io::Result<()> {
    match rename(&file, to) {
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(ERROR_INVALID_FUNCTION | ERROR_NOT_SUPPORTED | ERROR_INVALID_PARAMETER)
            ) =>
        {
            drop(file);
            std::fs::rename(from, to)
        }
        result => result,
    }
}

fn rename_posix(file: &File, to: &Path) -> io::Result<()> {
    let name: Vec<u16> = verbatim(&std::path::absolute(to)?).encode_wide().collect();
    let too_long = || io::Error::new(io::ErrorKind::InvalidInput, "record path is too long");
    let name_bytes = u32::try_from(name.len() * 2).map_err(|_| too_long())?;
    let offset = std::mem::offset_of!(FileRenameInfo, file_name);
    // The name and a terminating NUL follow the header.
    let size = offset + name.len() * 2 + 2;
    // Vec<usize> aligns the header's handle field, which Vec<u8> would not.
    let mut buffer = vec![0usize; size.div_ceil(std::mem::size_of::<usize>())];
    let info = buffer.as_mut_ptr().cast::<FileRenameInfo>();
    // SAFETY: `buffer` holds at least `size` zeroed bytes, aligned for
    // `FileRenameInfo`: its header, then room for the name and its NUL from
    // `file_name` on. Every write stays inside those bytes, and the pointers
    // come from `info`, which covers the whole buffer.
    unsafe {
        (&raw mut (*info).flags)
            .write(FILE_RENAME_FLAG_REPLACE_IF_EXISTS | FILE_RENAME_FLAG_POSIX_SEMANTICS);
        (&raw mut (*info).root_directory).write(std::ptr::null_mut());
        (&raw mut (*info).file_name_length).write(name_bytes);
        (&raw mut (*info).file_name)
            .cast::<u16>()
            .copy_from_nonoverlapping(name.as_ptr(), name.len());
    }
    let size = u32::try_from(size).map_err(|_| too_long())?;
    // SAFETY: the handle is open for the duration of the call, with DELETE
    // access. `buffer` holds `size` initialized bytes laid out as the
    // FileRenameInfoEx class expects, and the call only reads them.
    let renamed = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle(),
            FILE_RENAME_INFO_EX,
            buffer.as_ptr().cast(),
            size,
        )
    };
    if renamed == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// `C:\dir\record` as `\\?\C:\dir\record`, which is not limited to
/// `MAX_PATH`. Other absolute forms, UNC paths included, are kept as they are.
fn verbatim(path: &Path) -> OsString {
    match path.components().next() {
        Some(Component::Prefix(prefix)) if matches!(prefix.kind(), Prefix::Disk(_)) => {
            let mut verbatim = OsString::from(r"\\?\");
            verbatim.push(path.as_os_str());
            verbatim
        }
        _ => path.as_os_str().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::os::windows::fs::OpenOptionsExt;

    #[test]
    fn a_record_another_handle_holds_open_is_replaced_in_one_step() {
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("record");
        std::fs::write(&record, b"old").unwrap();
        // A reader's handle. Legacy replacement refuses a target that is open.
        let mut reader = std::fs::File::open(&record).unwrap();
        let temporary = root.path().join("temporary");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .access_mode(super::ACCESS)
            .open(&temporary)
            .unwrap();
        file.write_all(b"new").unwrap();
        super::rename_posix(&file, &record).unwrap();
        drop(file);
        assert_eq!(std::fs::read(&record).unwrap(), b"new");
        assert!(!temporary.exists());
        let mut old = Vec::new();
        reader.read_to_end(&mut old).unwrap();
        assert_eq!(old, b"old");
    }

    #[test]
    fn only_this_records_temporaries_mark_a_publication() {
        use std::ffi::OsStr;
        let record = OsStr::new("discovery.json");
        let temporary = |name: &str| super::is_temporary(OsStr::new(name), record);
        assert!(temporary(".discovery.json.4321.0.tmp"));
        assert!(temporary(".discovery.json.1.25.tmp"));
        for other in [
            "discovery.json",
            "discovery.json.1.0.tmp",
            ".discovery.json.tmp",
            ".discovery.json.1.tmp",
            ".discovery.json.1.2.3.tmp",
            ".discovery.json.1..tmp",
            ".discovery.json.x.0.tmp",
            ".discovery.json.1.0.tmp.old",
            ".discovery.jsonx.1.0.tmp",
            ".other.1.0.tmp",
        ] {
            assert!(!temporary(other), "{other}");
        }
    }

    #[test]
    fn a_lasting_denial_is_returned() {
        use std::os::windows::io::AsRawHandle;
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("record");
        std::fs::write(&record, b"old").unwrap();
        // A legacy delete leaves the file pending deletion until this handle
        // closes, and every open of it until then is ERROR_ACCESS_DENIED.
        let holder = std::fs::OpenOptions::new()
            .access_mode(super::DELETE)
            .open(&record)
            .unwrap();
        let delete: u8 = 1;
        // SAFETY: the handle is open with DELETE access, and `delete` is a
        // FILE_DISPOSITION_INFO, a single BOOLEAN, that the call only reads.
        let marked = unsafe {
            super::SetFileInformationByHandle(
                holder.as_raw_handle(),
                FILE_DISPOSITION_INFO,
                (&raw const delete).cast(),
                1,
            )
        };
        assert_ne!(marked, 0, "{}", std::io::Error::last_os_error());
        let mut read = std::fs::OpenOptions::new();
        read.read(true);
        let start = std::time::Instant::now();
        let refused = super::open_record(&read, &record).unwrap_err();
        assert_eq!(refused.raw_os_error(), Some(super::ERROR_ACCESS_DENIED));
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        drop(holder);
        let missing = super::open_record(&read, &record).unwrap_err();
        assert_eq!(missing.kind(), std::io::ErrorKind::NotFound);
    }

    /// `FILE_INFO_BY_HANDLE_CLASS::FileDispositionInfo`.
    const FILE_DISPOSITION_INFO: i32 = 4;

    #[test]
    fn a_file_system_without_posix_renames_gets_std_rename() {
        let root = tempfile::tempdir().unwrap();
        let record = root.path().join("record");
        std::fs::write(&record, b"old").unwrap();
        let temporary = root.path().join("temporary");
        std::fs::write(&temporary, b"new").unwrap();
        let file = std::fs::File::open(&temporary).unwrap();
        let unsupported = |_: &std::fs::File, _: &std::path::Path| {
            Err(std::io::Error::from_raw_os_error(
                super::ERROR_NOT_SUPPORTED,
            ))
        };
        super::replace_with(file, &temporary, &record, unsupported).unwrap();
        assert_eq!(std::fs::read(&record).unwrap(), b"new");
        assert!(!temporary.exists());

        let denied =
            |_: &std::fs::File, _: &std::path::Path| Err(std::io::Error::from_raw_os_error(5));
        std::fs::write(&temporary, b"newer").unwrap();
        let file = std::fs::File::open(&temporary).unwrap();
        assert!(super::replace_with(file, &temporary, &record, denied).is_err());
        assert_eq!(std::fs::read(&record).unwrap(), b"new");
    }
}
