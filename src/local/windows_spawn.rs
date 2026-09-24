//! `CreateProcessW` with an explicit inherited-handle list.
//!
//! `Command::spawn` passes `bInheritHandles = TRUE`, so the child inherits
//! every inheritable handle in the caller: pipes a build tool handed down,
//! files a library opened without clearing the flag. A daemon outlives its
//! caller, and each such handle it holds keeps the other end waiting for EOF.
//! Clearing the flag on the caller's three standard handles, as
//! [`super::windows::StdioInheritGuard`] does, misses all the others.
//! `PROC_THREAD_ATTRIBUTE_HANDLE_LIST` names the only handles the child may
//! inherit. The standard library can set it only through the unstable
//! `raw_attribute`, so this module calls `CreateProcessW` itself.

use std::ffi::{OsStr, c_void};
use std::fs::File;
use std::io;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle};
use std::os::windows::process::ExitStatusExt;
use std::path::Path;
use std::process::ExitStatus;
use std::ptr;

use super::command_line::{EnvChange, command_line, environment_block};

type Handle = *mut c_void;

#[repr(C)]
struct SecurityAttributes {
    length: u32,
    security_descriptor: *mut c_void,
    inherit_handle: i32,
}

#[repr(C)]
struct StartupInfoW {
    cb: u32,
    reserved: *mut u16,
    desktop: *mut u16,
    title: *mut u16,
    x: u32,
    y: u32,
    x_size: u32,
    y_size: u32,
    x_count_chars: u32,
    y_count_chars: u32,
    fill_attribute: u32,
    flags: u32,
    show_window: u16,
    reserved2_size: u16,
    reserved2: *mut u8,
    std_input: Handle,
    std_output: Handle,
    std_error: Handle,
}

#[repr(C)]
struct StartupInfoExW {
    startup_info: StartupInfoW,
    attribute_list: *mut c_void,
}

#[repr(C)]
struct ProcessInformation {
    process: Handle,
    thread: Handle,
    process_id: u32,
    thread_id: u32,
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateProcessW(
        application_name: *const u16,
        command_line: *mut u16,
        process_attributes: *const SecurityAttributes,
        thread_attributes: *const SecurityAttributes,
        inherit_handles: i32,
        creation_flags: u32,
        environment: *const c_void,
        current_directory: *const u16,
        startup_info: *const StartupInfoW,
        process_information: *mut ProcessInformation,
    ) -> i32;
    fn InitializeProcThreadAttributeList(
        attribute_list: *mut c_void,
        attribute_count: u32,
        flags: u32,
        size: *mut usize,
    ) -> i32;
    fn UpdateProcThreadAttribute(
        attribute_list: *mut c_void,
        flags: u32,
        attribute: usize,
        value: *const c_void,
        size: usize,
        previous_value: *mut c_void,
        return_size: *mut usize,
    ) -> i32;
    fn DeleteProcThreadAttributeList(attribute_list: *mut c_void);
    fn CreateFileW(
        file_name: *const u16,
        desired_access: u32,
        share_mode: u32,
        security_attributes: *const SecurityAttributes,
        creation_disposition: u32,
        flags_and_attributes: u32,
        template_file: Handle,
    ) -> Handle;
    fn DuplicateHandle(
        source_process: Handle,
        source_handle: Handle,
        target_process: Handle,
        target_handle: *mut Handle,
        desired_access: u32,
        inherit_handle: i32,
        options: u32,
    ) -> i32;
    fn GetCurrentProcess() -> Handle;
    fn WaitForSingleObject(handle: Handle, milliseconds: u32) -> u32;
    fn GetExitCodeProcess(process: Handle, exit_code: *mut u32) -> i32;
    fn TerminateProcess(process: Handle, exit_code: u32) -> i32;
}

const PROC_THREAD_ATTRIBUTE_HANDLE_LIST: usize = 0x0002_0002;
const STARTF_USESTDHANDLES: u32 = 0x0000_0100;
const EXTENDED_STARTUPINFO_PRESENT: u32 = 0x0008_0000;
const CREATE_UNICODE_ENVIRONMENT: u32 = 0x0000_0400;
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
const GENERIC_READ: u32 = 0x8000_0000;
const GENERIC_WRITE: u32 = 0x4000_0000;
const FILE_SHARE_READ: u32 = 0x0000_0001;
const FILE_SHARE_WRITE: u32 = 0x0000_0002;
const OPEN_EXISTING: u32 = 3;
const DUPLICATE_SAME_ACCESS: u32 = 0x0000_0002;
const INVALID_HANDLE_VALUE: Handle = -1isize as Handle;
const INFINITE: u32 = 0xFFFF_FFFF;
const WAIT_OBJECT_0: u32 = 0;
const WAIT_TIMEOUT: u32 = 258;

/// Where one of the child's output streams goes.
pub(crate) enum Target<'a> {
    Null,
    File(&'a File),
}

/// Everything `CreateProcessW` needs, already taken from the caller's builder.
pub(crate) struct Spawn<'a> {
    pub program: &'a Path,
    pub args: &'a [std::ffi::OsString],
    pub env: &'a [EnvChange],
    pub current_dir: Option<&'a Path>,
    pub stdout: Target<'a>,
    pub stderr: Target<'a>,
}

/// A process started by [`spawn`]. Dropping it closes the handle and leaves
/// the process running, as dropping a `std::process::Child` does.
#[derive(Debug)]
pub(crate) struct WindowsChild {
    process: OwnedHandle,
    pid: u32,
}

impl WindowsChild {
    pub(crate) fn id(&self) -> u32 {
        self.pid
    }

    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.wait_for(0)
    }

    pub(crate) fn wait(&mut self) -> io::Result<ExitStatus> {
        self.wait_for(INFINITE)?
            .ok_or_else(|| io::Error::other("infinite wait returned without an exit"))
    }

    pub(crate) fn kill(&mut self) -> io::Result<()> {
        // SAFETY: `process` is our open handle with PROCESS_ALL_ACCESS from
        // CreateProcessW. Terminating it affects only that process.
        if unsafe { TerminateProcess(self.process.as_raw_handle(), 1) } != 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        // A process that already exited refuses termination with access
        // denied; that is not a failure to stop it.
        if self.try_wait()?.is_some() {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn wait_for(&mut self, milliseconds: u32) -> io::Result<Option<ExitStatus>> {
        let handle = self.process.as_raw_handle();
        // SAFETY: `handle` is our open process handle; the wait only reads it.
        match unsafe { WaitForSingleObject(handle, milliseconds) } {
            WAIT_OBJECT_0 => {
                let mut code = 0;
                // SAFETY: `code` is a valid output slot and `handle` is open.
                if unsafe { GetExitCodeProcess(handle, &mut code) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(ExitStatus::from_raw(code)))
            }
            WAIT_TIMEOUT => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

/// Start `spec` detached from the caller.
///
/// `CREATE_NO_WINDOW` gives the child its own hidden console, so console
/// events from the caller's console (Ctrl-C, Ctrl-Break, closing the window)
/// never reach it, and console programs it starts in turn share that hidden
/// console instead of each opening a window. `DETACHED_PROCESS` would leave it
/// with no console at all, which does both of those badly and also stops
/// `SetConsoleCtrlHandler` from seeing logoff and shutdown.
/// `CREATE_NEW_PROCESS_GROUP` keeps it out of the caller's group for
/// `GenerateConsoleCtrlEvent`.
pub(crate) fn spawn(spec: &Spawn<'_>) -> io::Result<WindowsChild> {
    let extension = spec.program.extension().map(|ext| ext.to_ascii_lowercase());
    if matches!(extension.as_deref(), Some(ext) if ext == "bat" || ext == "cmd") {
        // Batch files run through cmd.exe with its own quoting rules; a
        // daemon is an executable.
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a detached daemon must be an executable, not a batch file",
        ));
    }

    let program: Vec<u16> = spec.program.as_os_str().encode_wide().collect();
    let args: Vec<Vec<u16>> = spec
        .args
        .iter()
        .map(|arg| arg.encode_wide().collect())
        .collect();
    let mut line = command_line(&program, &args)?;
    line.push(0);

    let inherited = std::env::vars_os()
        .map(|(name, value)| (name.encode_wide().collect(), value.encode_wide().collect()))
        .collect();
    let environment = environment_block(inherited, spec.env)?;

    let current_dir = spec.current_dir.map(|dir| wide_nul(dir.as_os_str()));

    let null = open_null()?;
    let stdout = inheritable(&spec.stdout)?;
    let stderr = inheritable(&spec.stderr)?;
    let std_handles = [
        null.as_raw_handle(),
        raw(&stdout, &null),
        raw(&stderr, &null),
    ];
    let mut inherit_list: Vec<RawHandle> = Vec::with_capacity(3);
    for handle in std_handles {
        // The list may not name one handle twice.
        if !inherit_list.contains(&handle) {
            inherit_list.push(handle);
        }
    }

    let mut attributes = AttributeList::with_handles(&inherit_list)?;

    let startup = StartupInfoExW {
        startup_info: StartupInfoW {
            cb: std::mem::size_of::<StartupInfoExW>() as u32,
            reserved: ptr::null_mut(),
            desktop: ptr::null_mut(),
            title: ptr::null_mut(),
            x: 0,
            y: 0,
            x_size: 0,
            y_size: 0,
            x_count_chars: 0,
            y_count_chars: 0,
            fill_attribute: 0,
            flags: STARTF_USESTDHANDLES,
            show_window: 0,
            reserved2_size: 0,
            reserved2: ptr::null_mut(),
            std_input: std_handles[0],
            std_output: std_handles[1],
            std_error: std_handles[2],
        },
        attribute_list: attributes.as_ptr(),
    };
    let mut info = ProcessInformation {
        process: ptr::null_mut(),
        thread: ptr::null_mut(),
        process_id: 0,
        thread_id: 0,
    };
    let flags = EXTENDED_STARTUPINFO_PRESENT
        | CREATE_UNICODE_ENVIRONMENT
        | CREATE_NEW_PROCESS_GROUP
        | CREATE_NO_WINDOW;
    // SAFETY: every pointer is valid for the duration of the call. `line` is a
    // NUL-terminated mutable buffer, as CreateProcessW requires; `environment`
    // is a double-NUL-terminated UTF-16 block matching
    // CREATE_UNICODE_ENVIRONMENT; `current_dir` is NUL-terminated or null.
    // `startup` is a STARTUPINFOEXW whose `cb` says so and whose attribute list
    // was initialised by AttributeList and outlives this call, as do the
    // handles it lists. `inherit_handles` must be TRUE for the list to apply,
    // and the list limits inheritance to exactly those handles.
    let created = unsafe {
        CreateProcessW(
            ptr::null(),
            line.as_mut_ptr(),
            ptr::null(),
            ptr::null(),
            1,
            flags,
            environment.as_ptr().cast(),
            current_dir.as_ref().map_or(ptr::null(), |dir| dir.as_ptr()),
            &startup.startup_info,
            &mut info,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateProcessW succeeded, so both handles are open and now ours.
    // The thread handle is not needed; wrapping it closes it on drop.
    let (process, _thread) = unsafe {
        (
            OwnedHandle::from_raw_handle(info.process),
            OwnedHandle::from_raw_handle(info.thread),
        )
    };
    Ok(WindowsChild {
        process,
        pid: info.process_id,
    })
}

fn wide_nul(text: &OsStr) -> Vec<u16> {
    text.encode_wide().chain(std::iter::once(0)).collect()
}

fn raw(owned: &Option<OwnedHandle>, null: &OwnedHandle) -> RawHandle {
    owned
        .as_ref()
        .map_or_else(|| null.as_raw_handle(), |handle| handle.as_raw_handle())
}

/// The null device, opened inheritable, for stdin and any stream sent nowhere.
fn open_null() -> io::Result<OwnedHandle> {
    let name = wide_nul(OsStr::new("NUL"));
    let security = SecurityAttributes {
        length: std::mem::size_of::<SecurityAttributes>() as u32,
        security_descriptor: ptr::null_mut(),
        inherit_handle: 1,
    };
    // SAFETY: `name` is NUL-terminated and `security` is a valid structure for
    // the duration of the call; no template file is passed.
    let handle = unsafe {
        CreateFileW(
            name.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &security,
            OPEN_EXISTING,
            0,
            ptr::null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreateFileW succeeded, so this is an open handle we now own.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle) })
}

/// An inheritable duplicate of a file target, or `None` for the null device.
///
/// Duplicating leaves the caller's own handle as it was: its inherit flag is
/// never touched, so no other spawn racing this one can pick it up.
fn inheritable(target: &Target<'_>) -> io::Result<Option<OwnedHandle>> {
    let Target::File(file) = target else {
        return Ok(None);
    };
    let mut duplicate: Handle = ptr::null_mut();
    // SAFETY: GetCurrentProcess returns a pseudo-handle that needs no closing.
    // `file` is an open handle for the duration of the call and `duplicate` is
    // a valid output slot. The duplicate is created inheritable with the same
    // access as the original.
    let duplicated = unsafe {
        let current = GetCurrentProcess();
        DuplicateHandle(
            current,
            file.as_raw_handle(),
            current,
            &mut duplicate,
            0,
            1,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if duplicated == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: DuplicateHandle succeeded, so this is an open handle we own.
    Ok(Some(unsafe { OwnedHandle::from_raw_handle(duplicate) }))
}

/// A process-thread attribute list holding one handle list.
///
/// The list points into `handles`, so both live together and the list is
/// deleted before its storage is freed.
struct AttributeList {
    storage: Vec<usize>,
    _handles: Box<[RawHandle]>,
}

impl AttributeList {
    fn with_handles(handles: &[RawHandle]) -> io::Result<Self> {
        let mut size = 0usize;
        // SAFETY: a null list with a size pointer is the documented size query;
        // it writes only `size` and returns FALSE with ERROR_INSUFFICIENT_BUFFER.
        unsafe {
            InitializeProcThreadAttributeList(ptr::null_mut(), 1, 0, &mut size);
        }
        if size == 0 {
            return Err(io::Error::last_os_error());
        }
        // Vec<usize> gives the pointer alignment the opaque list expects.
        let mut storage = vec![0usize; size.div_ceil(std::mem::size_of::<usize>())];
        // SAFETY: `storage` holds at least `size` bytes, the size just asked for.
        if unsafe {
            InitializeProcThreadAttributeList(storage.as_mut_ptr().cast(), 1, 0, &mut size)
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        let handles: Box<[RawHandle]> = handles.into();
        let mut list = Self {
            storage,
            _handles: handles,
        };
        // SAFETY: the list was initialised above with room for one attribute.
        // `value` points at the boxed handle array, which lives in `list` and
        // so outlives both this call and the CreateProcessW that reads it; the
        // size is that array's size in bytes.
        let updated = unsafe {
            UpdateProcThreadAttribute(
                list.as_ptr(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
                list._handles.as_ptr().cast(),
                std::mem::size_of_val(&*list._handles),
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
        if updated == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(list)
    }

    fn as_ptr(&mut self) -> *mut c_void {
        self.storage.as_mut_ptr().cast()
    }
}

impl Drop for AttributeList {
    fn drop(&mut self) {
        // SAFETY: the list was initialised in `with_handles` (a failed
        // initialisation returns before `Self` exists) and is deleted once,
        // before `storage` is freed.
        unsafe { DeleteProcThreadAttributeList(self.as_ptr()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::Duration;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreatePipe(
            read: *mut Handle,
            write: *mut Handle,
            attributes: *const SecurityAttributes,
            size: u32,
        ) -> i32;
    }

    fn system32(program: &str) -> PathBuf {
        let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
        PathBuf::from(root).join("System32").join(program)
    }

    /// An anonymous pipe whose write end is inheritable, as a build tool's
    /// output pipe is when it runs a compiler that starts the daemon.
    fn inheritable_pipe() -> (OwnedHandle, OwnedHandle) {
        let security = SecurityAttributes {
            length: std::mem::size_of::<SecurityAttributes>() as u32,
            security_descriptor: ptr::null_mut(),
            inherit_handle: 1,
        };
        let (mut read, mut write) = (ptr::null_mut(), ptr::null_mut());
        // SAFETY: both output slots and the attributes are valid locals.
        let created = unsafe { CreatePipe(&mut read, &mut write, &security, 0) };
        assert_ne!(created, 0, "{}", io::Error::last_os_error());
        // SAFETY: CreatePipe succeeded, so both handles are open and ours.
        unsafe {
            (
                OwnedHandle::from_raw_handle(read),
                OwnedHandle::from_raw_handle(write),
            )
        }
    }

    /// True if every write end is gone within `timeout`: a read then ends.
    fn reaches_eof_within(read: OwnedHandle, timeout: Duration) -> bool {
        use std::io::Read;
        let (done, finished) = mpsc::channel();
        std::thread::spawn(move || {
            // The standard library reads a broken pipe as end of file.
            let mut pipe = File::from(read);
            let mut buffer = [0u8; 64];
            while matches!(pipe.read(&mut buffer), Ok(n) if n > 0) {}
            let _ = done.send(());
        });
        finished.recv_timeout(timeout).is_ok()
    }

    fn ping(seconds: &str) -> Vec<std::ffi::OsString> {
        vec!["-n".into(), seconds.into(), "127.0.0.1".into()]
    }

    #[test]
    fn a_detached_child_inherits_only_its_standard_handles() {
        let (read, write) = inheritable_pipe();
        let program = system32("PING.EXE");
        let args = ping("30");
        let mut child = spawn(&Spawn {
            program: &program,
            args: &args,
            env: &[],
            current_dir: None,
            stdout: Target::Null,
            stderr: Target::Null,
        })
        .unwrap();
        drop(write);
        let eof = reaches_eof_within(read, Duration::from_secs(5));
        child.kill().unwrap();
        assert!(eof, "the daemon kept the caller's pipe open");
    }

    /// The control for the test above: `Command::spawn` does leak the pipe,
    /// so a pass there is the handle list working, not a blind harness.
    #[test]
    fn a_plain_spawn_would_keep_the_callers_pipe_open() {
        let (read, write) = inheritable_pipe();
        let mut child = std::process::Command::new(system32("PING.EXE"))
            .args(ping("30"))
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        drop(write);
        let eof = reaches_eof_within(read, Duration::from_secs(1));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!eof, "std spawn is expected to inherit the write end");
    }

    fn run_to_file(args: &[&str], env: &[EnvChange], dir: Option<&Path>) -> String {
        let out = tempfile::NamedTempFile::new().unwrap();
        let program = system32("cmd.exe");
        let args: Vec<std::ffi::OsString> = args.iter().map(Into::into).collect();
        let mut child = spawn(&Spawn {
            program: &program,
            args: &args,
            env,
            current_dir: dir,
            stdout: Target::File(out.as_file()),
            stderr: Target::Null,
        })
        .unwrap();
        assert!(child.wait().unwrap().success());
        std::fs::read_to_string(out.path()).unwrap()
    }

    #[test]
    fn output_env_and_directory_reach_the_child() {
        let set: Vec<u16> = "KDAEMON_SPAWN_TEST".encode_utf16().collect();
        let value: Vec<u16> = "from-the-builder".encode_utf16().collect();
        let printed = run_to_file(
            &["/c", "set", "KDAEMON_SPAWN_TEST"],
            &[EnvChange::Set(set, value)],
            None,
        );
        assert!(
            printed.contains("KDAEMON_SPAWN_TEST=from-the-builder"),
            "{printed}"
        );

        let dir = tempfile::tempdir().unwrap();
        let printed = run_to_file(&["/c", "cd"], &[], Some(dir.path()));
        let expected = dir.path().canonicalize().unwrap();
        let expected = expected.to_string_lossy();
        let expected = expected.trim_start_matches(r"\\?\");
        assert!(
            printed.trim().eq_ignore_ascii_case(expected),
            "{printed} vs {expected}"
        );
    }

    #[test]
    fn a_removed_variable_is_absent_in_the_child() {
        // COMPUTERNAME is always set on Windows, so this removes a variable
        // the child would otherwise inherit, without mutating the test
        // process's own environment while other tests spawn.
        assert!(std::env::var_os("COMPUTERNAME").is_some());
        let name: Vec<u16> = "computername".encode_utf16().collect();
        let printed = run_to_file(&["/c", "set"], &[EnvChange::Remove(name)], None);
        assert!(
            !printed
                .lines()
                .any(|line| line.to_ascii_uppercase().starts_with("COMPUTERNAME=")),
            "{printed}"
        );
    }

    #[test]
    fn exit_status_try_wait_and_kill() {
        let program = system32("PING.EXE");
        let args = ping("30");
        let mut child = spawn(&Spawn {
            program: &program,
            args: &args,
            env: &[],
            current_dir: None,
            stdout: Target::Null,
            stderr: Target::Null,
        })
        .unwrap();
        assert!(child.id() != 0);
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        let status = child.wait().unwrap();
        assert!(!status.success());
        // Killing an exited child is not an error.
        child.kill().unwrap();
    }

    #[test]
    fn a_batch_file_is_refused() {
        let program = PathBuf::from(r"C:\tools\run.cmd");
        let error = match spawn(&Spawn {
            program: &program,
            args: &[],
            env: &[],
            current_dir: None,
            stdout: Target::Null,
            stderr: Target::Null,
        }) {
            Ok(_) => panic!("a batch file must be refused"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}
