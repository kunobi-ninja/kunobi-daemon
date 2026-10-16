//! Replacing a record through the temporary file's own handle. Built and
//! tested by the Windows jobs.
//!
//! This is the crate's only FFI outside `local`: publication is always built,
//! and `std::fs::rename` first tries `MoveFileExW`, whose replacement leaves
//! the old record pending deletion for a moment. A reader that opens it then
//! gets `ERROR_ACCESS_DENIED`.

use std::ffi::{OsString, c_void};
use std::fs::File;
use std::io;
use std::os::windows::{ffi::OsStrExt, io::AsRawHandle};
use std::path::{Component, Path, Prefix};

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
