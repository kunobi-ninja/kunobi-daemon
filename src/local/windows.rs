//! Windows named-pipe adapters with peer checks and setup deadlines.
//!
//! Syscall import libs use lowercase names (`kernel32`, `advapi32`). cargo-xwin's
//! `lld-link` on Linux opens `Advapi32.lib` as a file and cannot see the xwin
//! splat's `advapi32.lib`. Native MSVC is case-insensitive, so lowercase is
//! valid on both.

use std::cell::Cell;
use std::ffi::c_void;
use std::io::{self, Read, Write};
use std::os::windows::io::{AsHandle, AsRawHandle};
use std::ptr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use interprocess::local_socket::traits::{Stream as _, StreamCommon as _};
use interprocess::local_socket::{ConnectOptions, GenericNamespaced, Stream, ToNsName};

use super::{ConnectError, Endpoint};
use crate::local::Duplex;
use crate::transport::WriteHalf;

/// A named-pipe connection with independent read and write ownership.
pub struct WindowsDuplex {
    stream: Stream,
    read_deadline: Cell<Option<Instant>>,
}

/// Verify a kernel-reported peer PID against the current process token user.
/// The caller must obtain the PID from its still-open socket, never a record.
pub fn verify_process_user(pid: u32) -> io::Result<()> {
    if process_runs_as_this_user(pid)? {
        Ok(())
    } else {
        Err(io::ErrorKind::PermissionDenied.into())
    }
}

/// Whether `pid` runs as this process's token user. `Ok(false)` only after a
/// successful SID comparison; failing to open or query the process is an error.
///
/// The process is looked up by PID, so if the connection's original peer has
/// exited and another process now has its PID, this describes that process.
pub(crate) fn process_runs_as_this_user(pid: u32) -> io::Result<bool> {
    // SAFETY: query-only handle to the kernel-reported pipe peer PID.
    let process = unsafe { OpenProcess(0x1000, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let process = Event(process);
    let peer = token_user(process.0)?;
    // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle.
    let own = token_user(unsafe { GetCurrentProcess() })?;
    // SAFETY: TOKEN_USER starts with a SID_AND_ATTRIBUTES whose first member is
    // the SID pointer, so element 0 of each buffer is that pointer. Both
    // buffers, and the SIDs the kernel wrote inside them, are owned locals that
    // outlive this call. EqualSid only reads them.
    let equal = unsafe { EqualSid(peer[0] as *const c_void, own[0] as *const c_void) };
    Ok(equal != 0)
}

/// One-time compatibility fallback for a protocol-v1 peer that cannot drain.
/// Terminate a peer whose identity and ownership the caller already verified.
pub fn terminate_legacy_peer(pid: u32) -> io::Result<()> {
    use std::ffi::c_void;
    type Handle = *mut c_void;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> Handle;
        fn TerminateProcess(process: Handle, exit_code: u32) -> i32;
        fn CloseHandle(object: Handle) -> i32;
    }
    const PROCESS_TERMINATE: u32 = 0x0001;
    // SAFETY: `pid` was obtained from the credentials of the still-live pipe.
    let process = unsafe { OpenProcess(PROCESS_TERMINATE, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `process` is a valid handle returned immediately above.
    let terminated = unsafe { TerminateProcess(process, 0) };
    // SAFETY: this closes exactly the handle opened above.
    let _ = unsafe { CloseHandle(process) };
    if terminated != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Overlapped named-pipe receive half with a setup deadline.
pub struct WindowsReader {
    inner: interprocess::local_socket::RecvHalf,
    read_deadline: Option<Instant>,
    setup: Arc<AtomicBool>,
}
/// Overlapped named-pipe send half with a shared setup deadline.
pub struct WindowsWriter {
    inner: interprocess::local_socket::SendHalf,
    deadline: Option<Instant>,
    setup: Arc<AtomicBool>,
}

impl WindowsReader {
    /// Return to normal blocking named-pipe reads for the steady-state pump.
    /// Restore ordinary unbounded I/O after setup succeeds.
    pub fn clear_read_deadline(&mut self) {
        self.read_deadline = None;
        self.setup.store(false, Ordering::Relaxed);
    }

    fn raw_handle(&self) -> *mut c_void {
        match &self.inner {
            interprocess::local_socket::RecvHalf::NamedPipe(pipe) => {
                pipe.as_handle().as_raw_handle()
            }
        }
    }
}

impl Read for WindowsReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(deadline) = self.read_deadline else {
            return self.inner.read(buf);
        };
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "named-pipe session establishment timed out",
            ));
        };
        read_overlapped(self.raw_handle(), buf, remaining)
    }
}

impl Write for WindowsWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.setup.load(Ordering::Relaxed) {
            let remaining = self
                .deadline
                .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
                .ok_or_else(|| io::Error::from(io::ErrorKind::TimedOut))?;
            let handle = match &self.inner {
                interprocess::local_socket::SendHalf::NamedPipe(pipe) => {
                    pipe.as_handle().as_raw_handle()
                }
            };
            write_overlapped(handle, buf, remaining)
        } else {
            self.inner.write(buf)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl WriteHalf for WindowsWriter {
    /// Flush, then report that the peer was not told: a named pipe has no
    /// half-close, and closing this handle would not end the stream while the
    /// reader still holds the pipe. End the session with
    /// [`crate::transport::Outstanding`] instead.
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.flush()?;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a named pipe has no half-close",
        ))
    }
}

impl WindowsDuplex {
    /// [`crate::peer::Evidence`] about the server at the other end, for
    /// [`crate::peer::authenticate`].
    pub fn evidence(&self) -> io::Result<crate::peer::Evidence> {
        super::peer::evidence(&self.stream)
    }
}

impl Duplex for WindowsDuplex {
    type Reader = WindowsReader;
    type Writer = WindowsWriter;

    fn connect_once_until(endpoint: &Endpoint, deadline: Instant) -> Result<Self, ConnectError> {
        let name = endpoint
            .to_ns_name::<GenericNamespaced>()
            .map_err(|_| ConnectError::ConnectTimeout)?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(ConnectError::ConnectTimeout);
        }
        ConnectOptions::new()
            .name(name)
            .wait_mode(interprocess::ConnectWaitMode::Timeout(Duration::ZERO))
            .connect_sync()
            .map(|stream| Self {
                stream,
                read_deadline: Cell::new(None),
            })
            .map_err(|error| {
                if error.kind() == io::ErrorKind::PermissionDenied {
                    ConnectError::PermissionDenied
                } else {
                    ConnectError::ConnectTimeout
                }
            })
    }

    /// Check the server process token before sending any client identity.
    fn verify_peer_user(&self) -> io::Result<()> {
        verify_process_user(self.peer_pid()?)
    }

    /// Read the server PID from the named-pipe kernel object.
    fn peer_pid(&self) -> io::Result<u32> {
        self.stream
            .peer_creds()?
            .pid()
            .ok_or_else(|| io::Error::other("named-pipe peer has no PID"))
    }

    /// Arm an absolute deadline for reads performed during session setup.
    ///
    /// The split reader enforces this with overlapped `ReadFile`, then the
    /// caller clears it before handing the reader to the ordinary byte pump.
    /// Set or clear the absolute setup deadline before splitting.
    fn set_read_deadline(&self, timeout: Option<Duration>) -> io::Result<()> {
        let deadline = timeout
            .map(|duration| {
                Instant::now().checked_add(duration).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "read deadline overflow")
                })
            })
            .transpose()?;
        self.read_deadline.set(deadline);
        Ok(())
    }

    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let deadline = self.read_deadline.get();
        let (read, write) = self.stream.split();
        let setup = Arc::new(AtomicBool::new(deadline.is_some()));
        Ok((
            WindowsReader {
                inner: read,
                read_deadline: deadline,
                setup: Arc::clone(&setup),
            },
            WindowsWriter {
                inner: write,
                deadline,
                setup,
            },
        ))
    }
}

type Handle = *mut c_void;

#[repr(C)]
#[derive(Clone, Copy)]
struct OverlappedOffset {
    offset: u32,
    offset_high: u32,
}

#[repr(C)]
union OverlappedPosition {
    offset: OverlappedOffset,
    pointer: *mut c_void,
}

#[repr(C)]
struct Overlapped {
    internal: usize,
    internal_high: usize,
    position: OverlappedPosition,
    event: Handle,
}

#[link(name = "kernel32")]
unsafe extern "system" {
    fn CreateEventW(
        event_attributes: *const c_void,
        manual_reset: i32,
        initial_state: i32,
        name: *const u16,
    ) -> Handle;
    fn ReadFile(
        file: Handle,
        buffer: *mut c_void,
        bytes_to_read: u32,
        bytes_read: *mut u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    fn WriteFile(
        file: Handle,
        buffer: *const c_void,
        length: u32,
        written: *mut u32,
        overlapped: *mut Overlapped,
    ) -> i32;
    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> Handle;
    fn GetCurrentProcess() -> Handle;
    fn WaitForSingleObject(handle: Handle, milliseconds: u32) -> u32;
    fn CancelIoEx(file: Handle, overlapped: *const Overlapped) -> i32;
    fn GetOverlappedResult(
        file: Handle,
        overlapped: *mut Overlapped,
        bytes_transferred: *mut u32,
        wait: i32,
    ) -> i32;
    fn CloseHandle(object: Handle) -> i32;
    fn SetConsoleCtrlHandler(
        handler: Option<unsafe extern "system" fn(u32) -> i32>,
        add: i32,
    ) -> i32;
    fn SetHandleInformation(object: Handle, mask: u32, flags: u32) -> i32;
}

#[link(name = "advapi32")]
unsafe extern "system" {
    fn OpenProcessToken(process: Handle, access: u32, token: *mut Handle) -> i32;
    fn GetTokenInformation(
        token: Handle,
        class: i32,
        info: *mut c_void,
        length: u32,
        needed: *mut u32,
    ) -> i32;
    fn EqualSid(first: *const c_void, second: *const c_void) -> i32;
}

fn token_user(process: Handle) -> io::Result<Vec<usize>> {
    let mut token = ptr::null_mut();
    // SAFETY: process is live and token is a valid output slot; TOKEN_QUERY=8.
    if unsafe { OpenProcessToken(process, 8, &mut token) } == 0 {
        return Err(io::Error::last_os_error());
    }
    let token = Event(token);
    let mut needed = 0;
    // SAFETY: `token` is a live token handle and TokenUser is class 1. A null
    // buffer with length 0 is the documented size query: the call writes only
    // through `needed` and returns FALSE with ERROR_INSUFFICIENT_BUFFER, which
    // is the 122 checked below.
    let result = unsafe { GetTokenInformation(token.0, 1, ptr::null_mut(), 0, &mut needed) };
    if result != 0
        || io::Error::last_os_error().raw_os_error() != Some(122)
        || needed < std::mem::size_of::<usize>() as u32
        || needed > 65536
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "cannot size peer token",
        ));
    }
    // A Vec<usize> gives TOKEN_USER's pointer alignment, which Vec<u8> does not.
    let mut buffer = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: the buffer holds at least `needed` bytes, the size the previous
    // call asked for, and is aligned for the pointer TOKEN_USER starts with.
    // The kernel writes at most `needed` bytes into it.
    if unsafe { GetTokenInformation(token.0, 1, buffer.as_mut_ptr().cast(), needed, &mut needed) }
        == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(buffer)
}

const ERROR_IO_PENDING: i32 = 997;
const ERROR_NOT_FOUND: i32 = 1168;
const WAIT_OBJECT_0: u32 = 0;
const WAIT_TIMEOUT: u32 = 258;

#[cfg(target_pointer_width = "64")]
const _: [(); 32] = [(); std::mem::size_of::<Overlapped>()];
#[cfg(target_pointer_width = "32")]
const _: [(); 20] = [(); std::mem::size_of::<Overlapped>()];

struct Event(Handle);

impl Drop for Event {
    fn drop(&mut self) {
        // SAFETY: this closes exactly one owned event, process or token handle.
        let _ = unsafe { CloseHandle(self.0) };
    }
}

fn deadline_millis(timeout: Duration) -> u32 {
    timeout.as_millis().clamp(1, u128::from(u32::MAX)) as u32
}

/// Perform one cancelable named-pipe read without changing steady-state mode.
fn read_overlapped(handle: Handle, buf: &mut [u8], timeout: Duration) -> io::Result<usize> {
    transfer_overlapped(handle, buf.len(), timeout, |operation, len| {
        // SAFETY: the mutable buffer lives until transfer_overlapped reaps this operation.
        unsafe {
            ReadFile(
                handle,
                buf.as_mut_ptr().cast(),
                len,
                ptr::null_mut(),
                operation,
            )
        }
    })
}

fn write_overlapped(handle: Handle, buf: &[u8], timeout: Duration) -> io::Result<usize> {
    transfer_overlapped(handle, buf.len(), timeout, |operation, len| {
        // SAFETY: the immutable buffer lives until completion or cancellation is reaped.
        unsafe { WriteFile(handle, buf.as_ptr().cast(), len, ptr::null_mut(), operation) }
    })
}

fn transfer_overlapped(
    handle: Handle,
    length: usize,
    timeout: Duration,
    start: impl FnOnce(*mut Overlapped, u32) -> i32,
) -> io::Result<usize> {
    if length == 0 {
        return Ok(0);
    }
    // SAFETY: a null security descriptor takes the default, and a null name
    // creates an unnamed event, both documented. The manual-reset and
    // initial-state arguments are plain booleans, so nothing is dereferenced.
    let event = Event(unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) });
    if event.0.is_null() {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: OVERLAPPED, its event and the caller's buffer remain alive until reaped.
    let mut operation: Overlapped = unsafe { std::mem::zeroed() };
    operation.event = event.0;
    let mut bytes_read = 0u32;
    let started = start(&mut operation, u32::try_from(length).unwrap_or(u32::MAX));
    if started != 0 {
        // An overlapped operation may complete inline. Ask the kernel for the
        // byte count instead of relying on `lpNumberOfBytesRead`, which must be
        // null for asynchronous handles.
        //
        // SAFETY: `operation` and `bytes_read` are stack locals that outlive
        // this call, and `handle` is the one the operation was started on.
        // `wait = 0` returns immediately rather than leaving the OVERLAPPED
        // pending past this frame.
        let completed = unsafe { GetOverlappedResult(handle, &mut operation, &mut bytes_read, 0) };
        return if completed != 0 {
            Ok(bytes_read as usize)
        } else {
            Err(io::Error::last_os_error())
        };
    }
    let start_error = io::Error::last_os_error();
    if start_error.raw_os_error() != Some(ERROR_IO_PENDING) {
        return Err(start_error);
    }

    // SAFETY: `event` is a live waitable event handle.
    match unsafe { WaitForSingleObject(event.0, deadline_millis(timeout)) } {
        WAIT_OBJECT_0 => {
            // SAFETY: the event signaled completion of this exact operation.
            let completed =
                unsafe { GetOverlappedResult(handle, &mut operation, &mut bytes_read, 0) };
            if completed != 0 {
                Ok(bytes_read as usize)
            } else {
                Err(io::Error::last_os_error())
            }
        }
        WAIT_TIMEOUT => {
            cancel_and_reap(handle, &mut operation, &mut bytes_read)?;
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "named-pipe session establishment timed out",
            ))
        }
        _ => {
            let wait_error = io::Error::last_os_error();
            // Even a failed wait does not prove the OVERLAPPED operation is
            // terminal. Cancel and collect it before returning the wait error.
            cancel_and_reap(handle, &mut operation, &mut bytes_read)?;
            Err(wait_error)
        }
    }
}

/// Cancel pending I/O and keep its stack storage alive until the kernel has
/// reached a terminal state.
fn cancel_and_reap(
    handle: Handle,
    operation: &mut Overlapped,
    bytes_read: &mut u32,
) -> io::Result<()> {
    // SAFETY: cancellation targets the exact live operation supplied here.
    let cancelled = unsafe { CancelIoEx(handle, operation) };
    let cancel_error = if cancelled == 0 {
        Some(io::Error::last_os_error())
    } else {
        None
    };

    // SAFETY: `wait = true` does not return while this OVERLAPPED remains
    // pending. This call is mandatory even when cancellation reports an
    // unexpected error: returning earlier could let the kernel write into the
    // caller's released buffer or this released stack structure.
    let _ = unsafe { GetOverlappedResult(handle, operation, bytes_read, 1) };

    match cancel_error {
        // The operation completed between the event wait and cancellation.
        // `GetOverlappedResult` above still establishes terminal completion.
        Some(error) if error.raw_os_error() == Some(ERROR_NOT_FOUND) => Ok(()),
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub use super::process::{process_has_exited, process_state};

static SESSION_END: AtomicBool = AtomicBool::new(false);

const CTRL_CLOSE_EVENT: u32 = 2;
const CTRL_LOGOFF_EVENT: u32 = 5;
const CTRL_SHUTDOWN_EVENT: u32 = 6;

unsafe extern "system" fn session_end_handler(event: u32) -> i32 {
    if matches!(
        event,
        CTRL_CLOSE_EVENT | CTRL_LOGOFF_EVENT | CTRL_SHUTDOWN_EVENT
    ) {
        SESSION_END.store(true, Ordering::Release);
        1
    } else {
        0
    }
}

/// Arm CLOSE/LOGOFF/SHUTDOWN so a published pipe is not left after logoff.
pub fn install_session_end_handler() -> io::Result<()> {
    // SAFETY: `session_end_handler` is process-lifetime and only stores an
    // atomic, which is safe on the system-created callback thread.
    if unsafe { SetConsoleCtrlHandler(Some(session_end_handler), 1) } != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// True after CLOSE, LOGOFF or SHUTDOWN once [`install_session_end_handler`] ran.
pub fn session_end_requested() -> bool {
    SESSION_END.load(Ordering::Acquire)
}

const HANDLE_FLAG_INHERIT: u32 = 0x00000001;
const INVALID_HANDLE_VALUE: Handle = -1isize as Handle;

/// Clears inherit on this process's standard handles and restores it on drop.
///
/// Explicit child stdio is unaffected: the standard library marks those
/// handles inheritable. This removes incidental inheritance of the caller's
/// pipes across `Command::spawn`.
pub struct StdioInheritGuard {
    restore: Vec<Handle>,
}

impl StdioInheritGuard {
    /// Suppress inherit for the duration of a spawn.
    pub fn suppress() -> Self {
        use std::os::windows::io::AsRawHandle;
        let handles: [Handle; 3] = [
            std::io::stdin().as_raw_handle(),
            std::io::stdout().as_raw_handle(),
            std::io::stderr().as_raw_handle(),
        ];
        let mut restore = Vec::new();
        for handle in handles {
            if handle.is_null() || handle == INVALID_HANDLE_VALUE {
                continue;
            }
            // SAFETY: handle is a live std handle; clearing inherit is
            // process-local and restored on drop.
            if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } != 0 {
                restore.push(handle);
            }
        }
        Self { restore }
    }
}

impl Drop for StdioInheritGuard {
    fn drop(&mut self) {
        for handle in &self.restore {
            // SAFETY: handles were successfully cleared by `suppress`.
            unsafe {
                SetHandleInformation(*handle, HANDLE_FLAG_INHERIT, HANDLE_FLAG_INHERIT);
            }
        }
    }
}

/// Every readiness pipe's name: this prefix and 32 lowercase hex digits, 128
/// random bits.
pub(crate) const READY_PIPE_PREFIX: &str = r"\\.\pipe\kunobi-daemon-ready-";

/// Whether `name` is a readiness pipe name as the launcher makes them.
fn is_ready_pipe_name(name: &str) -> bool {
    name.strip_prefix(READY_PIPE_PREFIX).is_some_and(|id| {
        id.len() == 32
            && id
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    })
}

/// Open the daemon's end of the readiness pipe named `name`.
///
/// Only a name of the launcher's own form is opened, so a stale or foreign
/// value cannot make the daemon write into some other file. The daemon
/// connects with identification-level impersonation only, and its handle is
/// not inheritable, so programs it starts do not hold the channel open.
pub(crate) fn open_ready_pipe(name: &str) -> io::Result<std::io::PipeWriter> {
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::OwnedHandle;
    const SECURITY_IDENTIFICATION: u32 = 1 << 16;
    if !is_ready_pipe_name(name) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the readiness channel is not a readiness pipe name",
        ));
    }
    let pipe = std::fs::OpenOptions::new()
        .write(true)
        .security_qos_flags(SECURITY_IDENTIFICATION)
        .open(name)?;
    Ok(std::io::PipeWriter::from(OwnedHandle::from(pipe)))
}

/// The launcher's end of a readiness channel: a named pipe the daemon opens
/// by name, so the daemon inherits no handle for it and no other child can.
#[cfg(feature = "launch")]
pub(crate) mod ready {
    use super::*;
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::sync::mpsc::Receiver;

    #[repr(C)]
    struct SecurityAttributes {
        length: u32,
        descriptor: *mut c_void,
        inherit: i32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateNamedPipeW(
            name: *const u16,
            open_mode: u32,
            pipe_mode: u32,
            max_instances: u32,
            out_buffer_size: u32,
            in_buffer_size: u32,
            default_timeout: u32,
            security: *const SecurityAttributes,
        ) -> Handle;
        fn ConnectNamedPipe(pipe: Handle, overlapped: *mut Overlapped) -> i32;
        fn PeekNamedPipe(
            pipe: Handle,
            buffer: *mut c_void,
            buffer_size: u32,
            read: *mut u32,
            available: *mut u32,
            left_in_message: *mut u32,
        ) -> i32;
        fn WaitForMultipleObjects(
            count: u32,
            handles: *const Handle,
            wait_all: i32,
            milliseconds: u32,
        ) -> u32;
        fn LocalFree(memory: *mut c_void) -> *mut c_void;
    }

    #[link(name = "advapi32")]
    unsafe extern "system" {
        fn ConvertSidToStringSidW(sid: *mut c_void, text: *mut *mut u16) -> i32;
        fn ConvertStringSecurityDescriptorToSecurityDescriptorW(
            text: *const u16,
            revision: u32,
            descriptor: *mut *mut c_void,
            size: *mut u32,
        ) -> i32;
        #[link_name = "SystemFunction036"]
        fn RtlGenRandom(buffer: *mut c_void, length: u32) -> u8;
    }

    const PIPE_ACCESS_INBOUND: u32 = 0x0000_0001;
    const FILE_FLAG_FIRST_PIPE_INSTANCE: u32 = 0x0008_0000;
    const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;
    /// The pipe mode's only flag: byte type, byte reads and blocking are all
    /// zero.
    const PIPE_REJECT_REMOTE_CLIENTS: u32 = 0x0000_0008;
    const SDDL_REVISION_1: u32 = 1;
    const INFINITE: u32 = 0xFFFF_FFFF;
    const ERROR_BROKEN_PIPE: i32 = 109;
    const ERROR_NO_DATA: i32 = 232;
    const ERROR_PIPE_CONNECTED: i32 = 535;
    const ERROR_OPERATION_ABORTED: i32 = 995;

    /// Memory the OS allocated with `LocalAlloc`.
    struct LocalMemory(*mut c_void);

    impl Drop for LocalMemory {
        fn drop(&mut self) {
            // SAFETY: the pointer came from an API that allocates with
            // LocalAlloc and is freed exactly once, here.
            unsafe {
                LocalFree(self.0);
            }
        }
    }

    /// A pipe name with 128 bits from the OS random source.
    fn random_name() -> io::Result<String> {
        let mut bytes = [0u8; 16];
        // SAFETY: the buffer is 16 writable bytes, the length passed.
        if unsafe { RtlGenRandom(bytes.as_mut_ptr().cast(), 16) } == 0 {
            return Err(io::Error::other("the OS random source failed"));
        }
        let id: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(format!("{READY_PIPE_PREFIX}{id}"))
    }

    /// A security descriptor whose protected DACL grants the current user,
    /// and nobody else, access.
    fn current_user_only() -> io::Result<LocalMemory> {
        // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle.
        let user = token_user(unsafe { GetCurrentProcess() })?;
        let mut text: *mut u16 = ptr::null_mut();
        // SAFETY: TOKEN_USER starts with the SID pointer, so element 0 of
        // `user` points at the SID inside that live buffer; `text` is a valid
        // output slot.
        if unsafe { ConvertSidToStringSidW(user[0] as *mut c_void, &mut text) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let text = LocalMemory(text.cast());
        let sid = text.0.cast::<u16>();
        let mut length = 0;
        // SAFETY: the string is NUL-terminated, so every index up to and
        // including the NUL is inside it.
        while unsafe { *sid.add(length) } != 0 {
            length += 1;
        }
        // SAFETY: the `length` units before the NUL are initialised and live
        // until `text` is freed at the end of this function.
        let sid = unsafe { std::slice::from_raw_parts(sid, length) };
        let sddl: Vec<u16> = "D:P(A;;GA;;;"
            .encode_utf16()
            .chain(sid.iter().copied())
            .chain(")".encode_utf16())
            .chain(std::iter::once(0))
            .collect();
        let mut descriptor = ptr::null_mut();
        // SAFETY: `sddl` is NUL-terminated and `descriptor` is a valid output
        // slot; the size output is optional.
        let converted = unsafe {
            ConvertStringSecurityDescriptorToSecurityDescriptorW(
                sddl.as_ptr(),
                SDDL_REVISION_1,
                &mut descriptor,
                ptr::null_mut(),
            )
        };
        if converted == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(LocalMemory(descriptor))
    }

    /// Create a readiness pipe: its name for the daemon, and the launcher's
    /// end.
    ///
    /// One inbound instance with a random name. `FILE_FLAG_FIRST_PIPE_INSTANCE`
    /// fails the creation instead of joining a pipe someone else made first,
    /// the DACL admits only the current user, remote clients are refused, and
    /// the handle is not inheritable.
    pub(crate) fn create() -> io::Result<(String, OwnedHandle)> {
        let name = random_name()?;
        let wide: Vec<u16> = name.encode_utf16().chain(std::iter::once(0)).collect();
        let descriptor = current_user_only()?;
        let security = SecurityAttributes {
            length: std::mem::size_of::<SecurityAttributes>() as u32,
            descriptor: descriptor.0,
            inherit: 0,
        };
        // SAFETY: `wide` is NUL-terminated and `security`, with the
        // descriptor it points to, lives for the call.
        let pipe = unsafe {
            CreateNamedPipeW(
                wide.as_ptr(),
                PIPE_ACCESS_INBOUND | FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
                PIPE_REJECT_REMOTE_CLIENTS,
                1,
                0,
                4096,
                0,
                &security,
            )
        };
        if pipe == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateNamedPipeW succeeded, so this is an open handle we own.
        Ok((name, unsafe { OwnedHandle::from_raw_handle(pipe) }))
    }

    /// What one step of reading found.
    enum Next {
        Got(usize),
        /// Nothing more can arrive.
        End,
        /// Try again: connected, or a read found nothing yet.
        Again,
    }

    /// Reads a readiness pipe while watching the daemon's process, so the
    /// daemon's exit ends the channel whether or not it ever connected.
    ///
    /// The process handle arrives once the daemon has started; the channel
    /// is read from before that. When the spawn fails the sender is dropped
    /// and the channel ends.
    pub(crate) struct Reader {
        pipe: OwnedHandle,
        started: Receiver<OwnedHandle>,
        process: Option<OwnedHandle>,
        connected: bool,
        /// The daemon exited: read what it left in the pipe, never wait.
        exited: bool,
        ended: bool,
    }

    impl Reader {
        pub(crate) fn new(pipe: OwnedHandle, started: Receiver<OwnedHandle>) -> Self {
            Self {
                pipe,
                started,
                process: None,
                connected: false,
                exited: false,
                ended: false,
            }
        }

        fn process(&mut self) -> Option<Handle> {
            if self.process.is_none() {
                self.process = self.started.recv().ok();
            }
            self.process.as_ref().map(|process| process.as_raw_handle())
        }

        fn step(&mut self, buf: &mut [u8]) -> io::Result<Next> {
            let Some(process) = self.process() else {
                return Ok(Next::End);
            };
            if !self.connected {
                return self.connect(process);
            }
            if self.exited {
                return self.drain(buf);
            }
            self.receive(buf, Some(process))
        }

        fn connect(&mut self, process: Handle) -> io::Result<Next> {
            let pipe = self.pipe.as_raw_handle();
            let event = new_event()?;
            let mut operation = operation(&event);
            // SAFETY: `operation` and its event outlive the connect: it fails
            // without starting, or `settle` returns only once it is terminal.
            let connected = unsafe { ConnectNamedPipe(pipe, &mut operation) };
            if connected == 0 {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    // The daemon connected, and perhaps already closed its end,
                    // before this call; what it wrote is still in the pipe.
                    Some(ERROR_PIPE_CONNECTED | ERROR_NO_DATA) => {
                        self.connected = true;
                        return Ok(Next::Again);
                    }
                    Some(ERROR_IO_PENDING) => {}
                    _ => return Err(error),
                }
            }
            let (result, exited) = self.settle(&mut operation, Some(process))?;
            self.exited = exited;
            match result {
                Ok(_) => {
                    self.connected = true;
                    Ok(Next::Again)
                }
                // Exited without connecting: nothing can arrive.
                Err(_) if exited => Ok(Next::End),
                Err(error) => Err(error),
            }
        }

        /// The daemon has exited: read only what it left behind.
        fn drain(&mut self, buf: &mut [u8]) -> io::Result<Next> {
            let mut available = 0u32;
            // SAFETY: a null buffer of size zero peeks no data; the call only
            // writes `available`.
            let peeked = unsafe {
                PeekNamedPipe(
                    self.pipe.as_raw_handle(),
                    ptr::null_mut(),
                    0,
                    ptr::null_mut(),
                    &mut available,
                    ptr::null_mut(),
                )
            };
            if peeked == 0 || available == 0 {
                return Ok(Next::End);
            }
            let length = buf.len().min(available as usize);
            self.receive(&mut buf[..length], None)
        }

        /// Read into `buf`, and when `process` is given, stop if the daemon
        /// exits first.
        fn receive(&mut self, buf: &mut [u8], process: Option<Handle>) -> io::Result<Next> {
            let pipe = self.pipe.as_raw_handle();
            let event = new_event()?;
            let mut operation = operation(&event);
            let length = u32::try_from(buf.len()).unwrap_or(u32::MAX);
            // SAFETY: `buf`, `operation` and its event outlive the read: it
            // fails without starting, or `settle` returns only once it is
            // terminal.
            let started = unsafe {
                ReadFile(
                    pipe,
                    buf.as_mut_ptr().cast(),
                    length,
                    ptr::null_mut(),
                    &mut operation,
                )
            };
            if started == 0 {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(ERROR_IO_PENDING) => {}
                    Some(ERROR_BROKEN_PIPE) => return Ok(Next::End),
                    _ => return Err(error),
                }
            }
            let (result, exited) = self.settle(&mut operation, process)?;
            self.exited |= exited;
            match result {
                Ok(0) => Ok(Next::Again),
                Ok(count) => Ok(Next::Got(count as usize)),
                Err(error) => match error.raw_os_error() {
                    Some(ERROR_BROKEN_PIPE) => Ok(Next::End),
                    // Cancelled because the daemon exited: drain next.
                    Some(ERROR_OPERATION_ABORTED) => Ok(Next::Again),
                    _ => Err(error),
                },
            }
        }

        /// Wait for `operation`, or for the daemon's exit when `process` is
        /// given, and return the operation's own result and whether the daemon
        /// exited first. Returns only once the operation is terminal, so the
        /// kernel never writes into it or its buffer afterwards.
        fn settle(
            &self,
            operation: &mut Overlapped,
            process: Option<Handle>,
        ) -> io::Result<(io::Result<u32>, bool)> {
            let pipe = self.pipe.as_raw_handle();
            // The operation's event comes first: when both are signalled the
            // wait reports it, so data sent just before an exit is read.
            let handles = [operation.event, process.unwrap_or(operation.event)];
            let count = if process.is_some() { 2 } else { 1 };
            // SAFETY: both handles are open for the whole wait.
            let woke = unsafe { WaitForMultipleObjects(count, handles.as_ptr(), 0, INFINITE) };
            let exited = process.is_some() && woke == WAIT_OBJECT_0 + 1;
            let wait_error = (woke != WAIT_OBJECT_0 && !exited).then(io::Error::last_os_error);
            if exited || wait_error.is_some() {
                // SAFETY: cancels only this operation, which is still live.
                unsafe {
                    CancelIoEx(pipe, operation);
                }
            }
            let mut transferred = 0u32;
            // SAFETY: `wait = 1` returns only once the operation is terminal.
            let completed = unsafe { GetOverlappedResult(pipe, operation, &mut transferred, 1) };
            let result = if completed != 0 {
                Ok(transferred)
            } else {
                Err(io::Error::last_os_error())
            };
            match wait_error {
                Some(error) => Err(error),
                None => Ok((result, exited)),
            }
        }
    }

    impl Read for Reader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            while !self.ended && !buf.is_empty() {
                match self.step(buf)? {
                    Next::Got(count) => return Ok(count),
                    Next::End => self.ended = true,
                    Next::Again => {}
                }
            }
            Ok(0)
        }
    }

    fn new_event() -> io::Result<OwnedHandle> {
        // SAFETY: a null security descriptor and a null name create an
        // unnamed manual-reset event, initially not signalled.
        let event = unsafe { CreateEventW(ptr::null(), 1, 0, ptr::null()) };
        if event.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: CreateEventW succeeded, so this is an open handle we own.
        Ok(unsafe { OwnedHandle::from_raw_handle(event) })
    }

    fn operation(event: &OwnedHandle) -> Overlapped {
        // SAFETY: an all-zero OVERLAPPED is a valid idle operation.
        let mut operation: Overlapped = unsafe { std::mem::zeroed() };
        operation.event = event.as_raw_handle();
        operation
    }
}

#[cfg(test)]
mod ready_pipe_tests {
    use super::*;

    #[test]
    fn only_a_readiness_pipe_name_is_opened() {
        let id = "0123456789abcdef0123456789abcdef";
        assert!(is_ready_pipe_name(&format!("{READY_PIPE_PREFIX}{id}")));
        for name in [
            String::new(),
            id.to_owned(),
            format!("{READY_PIPE_PREFIX}{}", &id[1..]),
            format!("{READY_PIPE_PREFIX}{id}0"),
            format!("{READY_PIPE_PREFIX}{}", id.to_uppercase()),
            format!(r"\\.\pipe\other-{id}"),
            r"C:\Windows\win.ini".to_owned(),
        ] {
            assert!(!is_ready_pipe_name(&name), "{name}");
            let error = open_ready_pipe(&name).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{name}");
        }
    }

    /// A process for the reader to watch, and its handle.
    #[cfg(feature = "launch")]
    fn watched(
        program: &str,
        args: &[&str],
    ) -> (std::process::Child, std::os::windows::io::OwnedHandle) {
        let child = std::process::Command::new(program)
            .args(args)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let handle = std::os::windows::io::AsHandle::as_handle(&child)
            .try_clone_to_owned()
            .unwrap();
        (child, handle)
    }

    #[cfg(feature = "launch")]
    #[test]
    fn a_readiness_pipe_carries_messages_until_its_writer_closes() {
        let (name, pipe) = ready::create().unwrap();
        assert!(is_ready_pipe_name(&name), "{name}");
        let (mut child, process) = watched("ping", &["-n", "30", "127.0.0.1"]);
        let (started, delivered) = std::sync::mpsc::channel();
        started.send(process).unwrap();
        let mut reader = ready::Reader::new(pipe, delivered);
        let mut daemon = open_ready_pipe(&name).unwrap();
        daemon.write_all(b"progress\nready\n").unwrap();
        drop(daemon);
        let mut received = Vec::new();
        reader.read_to_end(&mut received).unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(received, b"progress\nready\n");
    }

    #[cfg(feature = "launch")]
    #[test]
    fn a_daemon_that_exits_without_connecting_ends_its_channel() {
        // Nobody ever opens the pipe, so only the process's exit can end the
        // read; without watching it this would wait forever.
        let (_name, pipe) = ready::create().unwrap();
        let (mut child, process) = watched("cmd", &["/c", "exit", "0"]);
        let (started, delivered) = std::sync::mpsc::channel();
        started.send(process).unwrap();
        let mut received = Vec::new();
        ready::Reader::new(pipe, delivered)
            .read_to_end(&mut received)
            .unwrap();
        child.wait().unwrap();
        assert!(received.is_empty());
    }

    #[cfg(feature = "launch")]
    #[test]
    fn a_failed_spawn_ends_the_channel() {
        let (_name, pipe) = ready::create().unwrap();
        let (started, delivered) = std::sync::mpsc::channel::<std::os::windows::io::OwnedHandle>();
        drop(started);
        let mut received = Vec::new();
        ready::Reader::new(pipe, delivered)
            .read_to_end(&mut received)
            .unwrap();
        assert!(received.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use interprocess::local_socket::traits::Listener as _;
    use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName};

    use super::*;
    use crate::local::test_handshake as handshake;

    #[test]
    fn a_named_pipe_reports_that_it_cannot_half_close_and_keeps_its_replies() {
        // shutdown_write used to flush and return Ok, so a relay believed its
        // peer knew the requests had ended, and both ends waited for each other.
        let endpoint = format!("daemon-transport-no-half-close-{}", std::process::id());
        let listener = ListenerOptions::new()
            .name(endpoint.as_str().to_ns_name::<GenericNamespaced>().unwrap())
            .create_sync()
            .unwrap();
        let server = std::thread::spawn(move || {
            let mut peer = listener.accept().unwrap();
            let mut request = [0; 8];
            peer.read_exact(&mut request).unwrap();
            peer.write_all(b"reply\n").unwrap();
            request
        });
        let transport = WindowsDuplex::connect(&endpoint).unwrap();
        let (mut read, mut write) = transport.split().unwrap();
        write.write_all(b"request\n").unwrap();
        let error = write.shutdown_write().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(
            &server.join().unwrap(),
            b"request\n",
            "the flush still delivers the request"
        );
        let mut reply = [0; 6];
        read.read_exact(&mut reply).unwrap();
        assert_eq!(&reply, b"reply\n");
    }

    #[test]
    fn setup_write_deadline_bounds_a_nonreading_named_pipe() {
        let endpoint = format!("daemon-transport-write-deadline-{}", std::process::id());
        let listener = ListenerOptions::new()
            .name(endpoint.as_str().to_ns_name::<GenericNamespaced>().unwrap())
            .create_sync()
            .unwrap();
        let (release, wait) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let _peer = listener.accept().unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(2));
        });
        let transport = WindowsDuplex::connect(&endpoint).unwrap();
        transport.verify_peer_user().unwrap();
        transport
            .set_read_deadline(Some(Duration::from_millis(100)))
            .unwrap();
        let (_read, mut write) = transport.split().unwrap();
        let error = write.write_all(&vec![0; 16 << 20]).unwrap_err();
        release.send(()).ok();
        server.join().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[test]
    fn a_silent_named_pipe_times_out_the_handshake_read() {
        let endpoint = format!("daemon-transport-deadline-silent-{}", std::process::id());
        let name = endpoint.as_str().to_ns_name::<GenericNamespaced>().unwrap();
        let listener = ListenerOptions::new().name(name).create_sync().unwrap();
        let (release_tx, release_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let connection = listener.accept().unwrap();
            release_rx.recv().unwrap();
            drop(connection);
        });

        let transport = WindowsDuplex::connect(&endpoint).unwrap();
        transport
            .set_read_deadline(Some(Duration::from_millis(200)))
            .unwrap();
        let (mut read, mut write) = transport.split().unwrap();
        let started = Instant::now();
        let error = handshake(&mut write, &mut read, "windows-test").unwrap_err();
        assert!(
            error.kind() == io::ErrorKind::TimedOut,
            "silent pipe returned {error:?}"
        );
        assert!(
            started.elapsed() >= Duration::from_millis(150)
                && started.elapsed() < Duration::from_secs(2),
            "handshake deadline was not enforced: {:?}",
            started.elapsed()
        );

        release_tx.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn clearing_the_deadline_restores_an_unbounded_steady_state_read() {
        let endpoint = format!("daemon-transport-deadline-clear-{}", std::process::id());
        let name = endpoint.as_str().to_ns_name::<GenericNamespaced>().unwrap();
        let listener = ListenerOptions::new().name(name).create_sync().unwrap();
        let (send_tx, send_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let mut connection = listener.accept().unwrap();
            let mut preamble = [0u8; 6];
            connection.read_exact(&mut preamble).unwrap();
            connection.write_all(b"ready\n").unwrap();
            connection.flush().unwrap();
            send_rx.recv().unwrap();
            #[expect(
                clippy::disallowed_methods,
                reason = "The test must receive data after the cleared pipe deadline has passed."
            )]
            std::thread::sleep(Duration::from_millis(650));
            connection.write_all(b"x").unwrap();
        });

        let transport = WindowsDuplex::connect(&endpoint).unwrap();
        transport
            .set_read_deadline(Some(Duration::from_millis(500)))
            .unwrap();
        let (mut read, mut write) = transport.split().unwrap();
        handshake(&mut write, &mut read, "windows-test").unwrap();
        read.clear_read_deadline();
        send_tx.send(()).unwrap();

        // The byte arrives after the old deadline. Receiving it proves the
        // steady-state path reverted to the ordinary blocking reader.
        let mut byte = [0u8; 1];
        read.read_exact(&mut byte).unwrap();
        assert_eq!(byte, *b"x");
        server.join().unwrap();
    }
}
