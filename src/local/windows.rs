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

use super::ConnectError;
use crate::local::Duplex;
use crate::transport::WriteHalf;

/// A named-pipe connection with independent read and write ownership.
pub struct WindowsDuplex {
    stream: Stream,
    read_deadline: Cell<Option<Instant>>,
}

impl WindowsDuplex {
    /// Attempt one connection without retrying or sending application data.
    pub fn connect_once(endpoint: &str) -> Result<Self, ConnectError> {
        let name = endpoint
            .to_ns_name::<GenericNamespaced>()
            .map_err(|_| ConnectError::ConnectTimeout)?;
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
    /// Retry endpoint availability within the fixed connection setup budget.
    pub fn connect(endpoint: &str) -> Result<Self, ConnectError> {
        Self::connect_until(endpoint, Instant::now() + Duration::from_secs(5))
    }

    /// Retry local pipe availability under one absolute connection deadline.
    pub fn connect_until(endpoint: &str, deadline: Instant) -> Result<Self, ConnectError> {
        let name = endpoint
            .to_ns_name::<GenericNamespaced>()
            .map_err(|_| ConnectError::ConnectTimeout)?;
        loop {
            match ConnectOptions::new()
                .name(name.clone())
                .wait_mode(interprocess::ConnectWaitMode::Timeout(Duration::ZERO))
                .connect_sync()
            {
                Ok(stream) => {
                    return Ok(Self {
                        stream,
                        read_deadline: Cell::new(None),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                    return Err(ConnectError::PermissionDenied);
                }
                Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(
                        Duration::from_millis(50)
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                }
                Err(_) => return Err(ConnectError::ConnectTimeout),
            }
        }
    }

    /// Check the server process token before sending any client identity.
    pub fn verify_peer_user(&self) -> io::Result<()> {
        verify_process_user(self.peer_pid()?)
    }

    /// Read the server PID from the named-pipe kernel object.
    pub fn peer_pid(&self) -> io::Result<u32> {
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
    pub fn set_read_deadline(&self, timeout: Option<Duration>) -> io::Result<()> {
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
}

/// Verify a kernel-reported peer PID against the current process token user.
/// The caller must obtain the PID from its still-open socket, never a record.
pub fn verify_process_user(pid: u32) -> io::Result<()> {
    // SAFETY: query-only handle to the kernel-reported pipe server PID.
    let process = unsafe { OpenProcess(0x1000, 0, pid) };
    if process.is_null() {
        return Err(io::Error::last_os_error());
    }
    let process = Event(process);
    let peer = token_user(process.0)?;
    // SAFETY: GetCurrentProcess returns a borrowed pseudo-handle.
    let own = token_user(unsafe { GetCurrentProcess() })?;
    // TOKEN_USER begins with SID_AND_ATTRIBUTES; its first member is a SID pointer.
    // Both aligned buffers and their embedded SIDs remain alive for EqualSid.
    let equal = unsafe { EqualSid(peer[0] as *const c_void, own[0] as *const c_void) };
    if equal != 0 {
        Ok(())
    } else {
        Err(io::ErrorKind::PermissionDenied.into())
    }
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
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.flush()
    }
}

impl Duplex for WindowsDuplex {
    type Reader = WindowsReader;
    type Writer = WindowsWriter;

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
    // TokenUser=1. The first call obtains the OS-owned structure's required size.
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
    // usize storage provides TOKEN_USER's pointer alignment, unlike Vec<u8>.
    let mut buffer = vec![0usize; (needed as usize).div_ceil(std::mem::size_of::<usize>())];
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

#[cfg(test)]
mod tests {
    use std::sync::mpsc;

    use interprocess::local_socket::traits::Listener as _;
    use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName};

    use super::*;
    use crate::local::test_handshake as handshake;

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

/// True only when the OS establishes that a previously verified PID exited.
pub fn process_has_exited(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    const SYNCHRONIZE: u32 = 0x00100000;
    // SAFETY: requests only a waitable handle to the previously verified PID.
    let process = unsafe { OpenProcess(SYNCHRONIZE, 0, pid) };
    if process.is_null() {
        // A nonzero PID that no longer exists yields ERROR_INVALID_PARAMETER.
        // Access denial and other errors leave lifetime unknown.
        return std::io::Error::last_os_error().raw_os_error() == Some(87);
    }
    // SAFETY: process is our live handle; zero timeout never blocks. Closing
    // that handle releases our reference and does not stop the process.
    let result = unsafe { WaitForSingleObject(process, 0) };
    unsafe {
        CloseHandle(process);
    }
    result == 0 // WAIT_OBJECT_0: the process exited.
}

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

#[cfg(test)]
mod lifetime_tests {
    #[test]
    fn the_current_process_and_unknown_pid_are_not_retired() {
        assert!(!super::process_has_exited(std::process::id()));
        assert!(!super::process_has_exited(0));
    }
}
