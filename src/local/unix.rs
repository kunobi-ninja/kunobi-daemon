//! Unix stream adapters with peer checks and setup deadlines.

use std::cell::Cell;
use std::io::{self, Read, Write};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::time::{Duration, Instant};

use super::{ConnectError, Endpoint};
use crate::local::Duplex;
use crate::transport::WriteHalf;

/// A Unix connection whose setup deadline is shared by its independent halves.
pub struct UnixDuplex {
    stream: UnixStream,
    read_deadline: Cell<Option<Instant>>,
}

#[cfg(target_os = "linux")]
const CHILD_SIGNAL: std::os::raw::c_int = 17;
#[cfg(not(target_os = "linux"))]
const CHILD_SIGNAL: std::os::raw::c_int = 20;

fn child_disposition(handler: usize) -> io::Result<usize> {
    unsafe extern "C" {
        fn signal(number: std::os::raw::c_int, handler: usize) -> usize;
    }
    // SAFETY: every caller passes SIG_DFL (0) or SIG_IGN (1), the two
    // dispositions POSIX defines as constants rather than addresses, so no
    // Rust code is ever installed on a signal stack. `signal` reports failure
    // as SIG_ERR, checked below against `usize::MAX`.
    let previous = unsafe { signal(CHILD_SIGNAL, handler) };
    if previous == usize::MAX {
        Err(io::Error::last_os_error())
    } else {
        Ok(previous)
    }
}

/// The relay never waits for peer exit. Let the kernel reap it while this
/// relay is idle on stdin, without another thread or periodic polling.
pub fn auto_reap_children() -> io::Result<()> {
    child_disposition(1).map(|_| ())
}

/// Restore normal child waiting in the peer: it owns and verifies candidate
/// child processes. This only calls async-signal-safe signal(2).
pub fn restore_child_waiting() -> io::Result<()> {
    child_disposition(0).map(|_| ())
}

/// Spawn with normal child waiting, then restore the relay's reaping policy.
///
/// Rust may wait for a failed exec's child inside `Command::spawn`. Ignoring
/// SIGCHLD during that wait makes it panic instead of returning the exec error.
/// The peer also inherits normal waiting without a fork-only pre-exec hook.
///
/// The spawn waits while this crate creates a listening socket, so the child
/// cannot inherit one that is not yet close-on-exec; see
/// [`crate::local::unix_socket::acquire`].
pub fn spawn_with_child_waiting(
    command: &mut std::process::Command,
) -> io::Result<std::process::Child> {
    spawn_then(command, |_| Ok(())).map(|(child, ())| child)
}

/// Spawn as [`spawn_with_child_waiting`] does, and run `watch` on the child
/// while SIGCHLD is still at its default. Until the child is waited for, its
/// PID cannot name another process even if it has exited, so an exit handle
/// opened here follows the child. With SIGCHLD ignored the kernel reaps the
/// child as it exits, and the PID is free once that happens.
///
/// A failed `watch` stops the child and returns the error.
fn spawn_then<T>(
    command: &mut std::process::Command,
    watch: impl FnOnce(&std::process::Child) -> io::Result<T>,
) -> io::Result<(std::process::Child, T)> {
    // Signal dispositions are process-wide. Serialize relay launches until
    // both the previous policy and any exits during this window are handled.
    let _spawn = crate::spawn_lock::spawning();
    let previous = child_disposition(0)?;
    let child = command.spawn().and_then(|child| watched(child, watch));
    child_disposition(previous)?;
    if previous == 1 {
        reap_exited_children();
    }
    child
}

fn watched<T>(
    mut child: std::process::Child,
    watch: impl FnOnce(&std::process::Child) -> io::Result<T>,
) -> io::Result<(std::process::Child, T)> {
    match watch(&child) {
        Ok(watching) => Ok((child, watching)),
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            Err(error)
        }
    }
}

fn reap_exited_children() {
    unsafe extern "C" {
        fn waitpid(
            pid: std::os::raw::c_int,
            status: *mut std::os::raw::c_int,
            options: std::os::raw::c_int,
        ) -> std::os::raw::c_int;
    }
    // A peer can exit while SIGCHLD is temporarily default. Reap those
    // zombies after restoring SIG_IGN; subsequent exits are kernel-reaped.
    loop {
        // SAFETY: -1 selects any child of this process. POSIX allows a null
        // `stat_loc`, which discards the exit status rather than writing
        // through the pointer. WNOHANG is 1 on both supported targets, so the
        // call returns immediately instead of blocking here.
        let pid = unsafe { waitpid(-1, std::ptr::null_mut(), 1) };
        if pid > 0 {
            continue;
        }
        if pid == -1 && io::Error::last_os_error().kind() == io::ErrorKind::Interrupted {
            continue;
        }
        break;
    }
}

use std::os::fd::{AsRawFd, RawFd};

#[cfg(target_os = "macos")]
/// Read the OS process identity attached to an established local connection.
pub fn peer_pid(fd: RawFd) -> io::Result<u32> {
    use std::os::raw::{c_int, c_void};
    unsafe extern "C" {
        fn getsockopt(
            socket: c_int,
            level: c_int,
            name: c_int,
            value: *mut c_void,
            len: *mut u32,
        ) -> c_int;
    }
    const SOL_LOCAL: c_int = 0;
    const LOCAL_PEERPID: c_int = 0x002;
    let mut pid: c_int = 0;
    let mut len = std::mem::size_of::<c_int>() as u32;
    // SAFETY: `pid` and `len` point to correctly sized writable values and fd
    // remains owned by `self` for the duration of the call.
    let rc = unsafe {
        getsockopt(
            fd,
            SOL_LOCAL,
            LOCAL_PEERPID,
            (&raw mut pid).cast(),
            &raw mut len,
        )
    };
    if rc == 0 && pid > 0 {
        Ok(pid as u32)
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "macos"))]
/// Read the OS process identity attached to an established local connection.
pub fn peer_pid(fd: RawFd) -> io::Result<u32> {
    peer_credentials(fd).map(|(pid, _)| pid)
}

#[cfg(not(target_os = "macos"))]
fn peer_credentials(fd: RawFd) -> io::Result<(u32, u32)> {
    use std::os::raw::{c_int, c_void};
    #[repr(C)]
    struct UCred {
        pid: c_int,
        uid: u32,
        gid: u32,
    }
    unsafe extern "C" {
        fn getsockopt(
            socket: c_int,
            level: c_int,
            name: c_int,
            value: *mut c_void,
            len: *mut u32,
        ) -> c_int;
    }
    const SOL_SOCKET: c_int = 1;
    const SO_PEERCRED: c_int = 17;
    let mut cred = UCred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<UCred>() as u32;
    // SAFETY: `cred` and `len` are correctly sized writable values and fd is
    // a live Unix-domain socket owned by the caller.
    let rc = unsafe {
        getsockopt(
            fd,
            SOL_SOCKET,
            SO_PEERCRED,
            (&raw mut cred).cast(),
            &raw mut len,
        )
    };
    if rc == 0 && cred.pid > 0 {
        Ok((cred.pid as u32, cred.uid))
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Current effective OS user ID.
pub fn own_uid() -> u32 {
    unsafe extern "C" {
        fn geteuid() -> u32;
    }
    // SAFETY: geteuid takes no arguments and dereferences nothing. POSIX
    // specifies it as always successful, so there is no error case and no
    // errno to read afterwards.
    unsafe { geteuid() }
}

/// Effective user ID reported by the kernel for a still-open Unix socket.
pub fn peer_uid(fd: RawFd) -> io::Result<u32> {
    #[cfg(target_os = "macos")]
    let peer = {
        unsafe extern "C" {
            fn getpeereid(fd: RawFd, uid: *mut u32, gid: *mut u32) -> i32;
        }
        let (mut uid, mut gid) = (0, 0);
        // SAFETY: fd is a live socket; both output pointers are valid u32 values.
        if unsafe { getpeereid(fd, &mut uid, &mut gid) } != 0 {
            return Err(io::Error::last_os_error());
        }
        uid
    };
    #[cfg(not(target_os = "macos"))]
    let peer = peer_credentials(fd)?.1;
    Ok(peer)
}

fn verify_peer_user(fd: RawFd) -> io::Result<()> {
    if peer_uid(fd)? == own_uid() {
        Ok(())
    } else {
        Err(io::ErrorKind::PermissionDenied.into())
    }
}

pub use super::process::{process_has_exited, process_state};

/// What `kill(pid, 0)` and, on Linux, `/proc` establish about `pid`.
///
/// The fallback where no exit event is available: see
/// [`super::ProcessHandle`]. Outside Linux it reads a child that exited but
/// has not been waited for as alive.
pub(crate) fn signal_state(pid: u32) -> super::ProcessState {
    use super::ProcessState;
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    if pid == 0 || pid > i32::MAX as u32 {
        return ProcessState::Unknown;
    }
    // A container's PID 1 may leave an exited orphan unreaped. kill(pid, 0)
    // still succeeds for that zombie, although it cannot own any more work.
    #[cfg(target_os = "linux")]
    if std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(") ")
            .is_some_and(|(_, fields)| fields.starts_with("Z ") || fields.starts_with("X "))
    }) {
        return ProcessState::Exited;
    }
    // SAFETY: signal 0 checks process existence without delivering a signal.
    if unsafe { kill(pid as i32, 0) } == 0 {
        return ProcessState::Alive;
    }
    liveness_from_errno(io::Error::last_os_error().raw_os_error())
}

/// `kill(pid, 0)` failed: ESRCH means no such process, and EPERM means one
/// exists that this user may not signal. Anything else proves nothing.
fn liveness_from_errno(errno: Option<i32>) -> super::ProcessState {
    use super::ProcessState;
    const EPERM: i32 = 1;
    const ESRCH: i32 = 3;
    match errno {
        Some(ESRCH) => ProcessState::Exited,
        Some(EPERM) => ProcessState::Alive,
        _ => ProcessState::Unknown,
    }
}

/// Send SIGINT to every process in group `pgid`, as Ctrl-C in a terminal
/// does to its foreground group. Test support for the launch module.
#[cfg(all(test, feature = "launch"))]
pub(crate) fn interrupt_process_group(pgid: u32) -> io::Result<()> {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    const SIGINT: i32 = 2;
    let pgid = i32::try_from(pgid).map_err(|_| io::ErrorKind::InvalidInput)?;
    // SAFETY: a negative PID addresses the process group; the caller passes
    // the ID of a group it created for the test.
    if unsafe { kill(-pgid, SIGINT) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Spawn `command` as the leader of a new session.
///
/// `setsid` in the child, before exec, gives it its own session and process
/// group and no controlling terminal. So Ctrl-C in the terminal that started
/// the caller does not reach it (SIGINT goes to the terminal's foreground
/// process group), and neither does the hangup when that terminal closes. A
/// new process group alone would stop the first but keep the terminal as the
/// child's controlling terminal.
///
/// Every descriptor above stderr is also marked close-on-exec in the child, so
/// the new program starts with only its standard streams. Without this a
/// descriptor the caller left inheritable, such as a make jobserver pipe or a
/// build tool's output pipe, stays open for the daemon's whole life, and the
/// process waiting for its other end never sees EOF. The descriptors are
/// marked rather than closed because the standard library reports a failed
/// exec back to the parent through a close-on-exec pipe of its own; closing it
/// before exec would make a missing binary look like a successful spawn.
///
/// Child waiting is handled as in [`spawn_with_child_waiting`].
pub fn spawn_in_new_session(
    command: &mut std::process::Command,
) -> io::Result<std::process::Child> {
    in_new_session(command, None);
    spawn_with_child_waiting(command)
}

/// [`spawn_in_new_session`], with the child's exit handle opened before
/// anything can reap the child. `keep` preserves the readiness channel across
/// exec; the caller owns it and closes its copy after the spawn.
#[cfg(feature = "launch")]
pub(crate) fn spawn_in_new_session_watched(
    command: &mut std::process::Command,
    keep: Option<RawFd>,
) -> io::Result<(std::process::Child, super::ProcessHandle)> {
    in_new_session(command, keep);
    spawn_then(command, super::ProcessHandle::for_child)
}

/// Make `command` lead a new session, preserving only standard descriptors
/// and the optional readiness channel.
fn in_new_session(command: &mut std::process::Command, keep: Option<RawFd>) {
    use std::os::unix::process::CommandExt;
    // Computed here, in the parent: getrlimit is not on the async-signal-safe
    // list, and the child may only make such calls between fork and exec.
    let descriptor_limit = descriptor_limit();
    let prepare = move || {
        // SAFETY: setsid takes no arguments and only changes the calling
        // process's own session and process group. It cannot fail with EPERM
        // here: a freshly forked child is never a process group leader.
        if unsafe { libc::setsid() } == -1 {
            return Err(io::Error::last_os_error());
        }
        mark_inherited_descriptors_cloexec(descriptor_limit);
        if let Some(fd) = keep {
            // SAFETY: F_SETFD with no flags only clears close-on-exec on this
            // one descriptor, which the fork copied from the parent's open
            // channel end. A number that is not open fails with EBADF.
            checked(unsafe { libc::fcntl(fd, libc::F_SETFD, 0) })?;
        }
        Ok(())
    };
    // SAFETY: the hook runs in the forked child between fork and exec. It
    // calls only setsid, close_range (Linux) and fcntl, which are
    // async-signal-safe, and builds an `io::Error` from errno, which does not
    // allocate. It touches no memory shared with the parent.
    unsafe {
        command.pre_exec(prepare);
    }
}

/// A libc call's result, or the error it set. Only -1 reports failure.
///
/// Safe in a pre-exec hook: it reads `errno` and allocates nothing.
fn checked(result: libc::c_int) -> io::Result<libc::c_int> {
    if result == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(result)
    }
}

/// A readiness channel: the launcher's end, and the daemon's end as a
/// close-on-exec descriptor above stderr.
///
/// The daemon's end is moved to 3 or above, because a child's standard
/// streams are set up before the descriptor sweep and would replace a channel
/// at 0, 1 or 2, which a caller with a closed standard stream can be handed.
///
/// # Spawning while creating the pipe
///
/// Linux creates the pipe close-on-exec in one step (`pipe2`). macOS takes
/// two, and a child spawned by another thread between them inherits both
/// ends. A child holding the write end keeps the channel open for its whole
/// life, so the launcher sees neither the daemon's exit nor its end of the
/// channel.
///
/// The pipe is created while no spawn by this crate is under way, as a
/// listener is in [`crate::local::unix_socket::acquire`]. Other spawns can
/// still capture it. A process that spawns another way while it may be
/// launching a daemon should have each child close, or mark close-on-exec,
/// every descriptor above stderr before it execs.
#[cfg(feature = "launch")]
pub(crate) fn ready_pipe() -> io::Result<(std::io::PipeReader, std::os::fd::OwnedFd)> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let (reader, writer) = crate::spawn_lock::without_spawns(std::io::pipe)?;
    let writer = OwnedFd::from(writer);
    // SAFETY: F_DUPFD_CLOEXEC only duplicates this process's open `writer`
    // onto the lowest free number from 3 up, close-on-exec in the same step.
    let raised = checked(unsafe { libc::fcntl(writer.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) })?;
    // SAFETY: fcntl returned a new open descriptor that nothing else owns.
    Ok((reader, unsafe { OwnedFd::from_raw_fd(raised) }))
}

/// Take over the daemon's end of a readiness channel, named by the decimal
/// descriptor number `value`.
///
/// Refuses, without touching it, anything but the write end of a pipe above
/// stderr. The descriptor becomes close-on-exec, so programs the daemon
/// starts do not hold the channel open.
///
/// # Safety
///
/// A descriptor `value` names must be this process's channel end, which
/// nothing else in the process owns or closes: the returned writer closes it
/// when dropped. The checks catch a value that names no pipe, not one that
/// names some other pipe's write end.
pub(crate) unsafe fn take_ready_descriptor(value: &str) -> io::Result<std::io::PipeWriter> {
    use std::os::fd::{FromRawFd, OwnedFd};
    let refuse = |message: &'static str| io::Error::new(io::ErrorKind::InvalidInput, message);
    let fd: RawFd = value
        .parse()
        .map_err(|_| refuse("the readiness channel is not a descriptor number"))?;
    if fd <= 2 {
        return Err(refuse("a standard stream is not a readiness channel"));
    }
    let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat writes a `stat` into this valid slot, or fails with EBADF
    // for a number that is not an open descriptor and writes nothing.
    checked(unsafe { libc::fstat(fd, status.as_mut_ptr()) })?;
    // SAFETY: fstat succeeded, so it initialised `status`.
    let status = unsafe { status.assume_init() };
    if (status.st_mode & libc::S_IFMT) != libc::S_IFIFO {
        return Err(refuse("the readiness channel is not a pipe"));
    }
    // SAFETY: F_GETFL only reads the open file's status flags.
    let flags = checked(unsafe { libc::fcntl(fd, libc::F_GETFL) })?;
    if (flags & libc::O_ACCMODE) != libc::O_WRONLY {
        return Err(refuse(
            "the readiness channel is not the write end of a pipe",
        ));
    }
    // SAFETY: F_SETFD sets only this descriptor's close-on-exec flag, the
    // only descriptor flag there is.
    checked(unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) })?;
    // SAFETY: the descriptor is open (checked above), and the caller
    // guarantees that nothing else in this process owns it.
    Ok(std::io::PipeWriter::from(unsafe {
        OwnedFd::from_raw_fd(fd)
    }))
}

/// The highest descriptor number to consider, from the soft RLIMIT_NOFILE.
///
/// An unlimited or very large limit is capped: the fallback loop costs one
/// system call per number, and descriptors that high are not something a
/// caller hands to a daemon by accident.
fn descriptor_limit() -> libc::c_int {
    const CAP: libc::rlim_t = 1 << 16;
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `limit` is a valid output slot for getrlimit.
    let known = unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0;
    let current = if known { limit.rlim_cur } else { 1024 };
    libc::c_int::try_from(current.min(CAP)).unwrap_or(1 << 16)
}

/// A pipe with neither end close-on-exec, as a build tool's jobserver or
/// output pipe is when it runs a compiler that starts the daemon. Test support.
#[cfg(all(test, feature = "launch"))]
pub(crate) fn inheritable_pipe() -> (std::fs::File, std::fs::File) {
    use std::os::fd::FromRawFd;
    let mut fds = [0; 2];
    // SAFETY: `fds` is a valid two-element output array for pipe(2).
    assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
    // SAFETY: pipe succeeded, so both descriptors are open and now ours.
    unsafe {
        (
            std::fs::File::from_raw_fd(fds[0]),
            std::fs::File::from_raw_fd(fds[1]),
        )
    }
}

/// Mark every descriptor from 3 up close-on-exec. Async-signal-safe.
fn mark_inherited_descriptors_cloexec(limit: libc::c_int) {
    #[cfg(target_os = "linux")]
    {
        const CLOSE_RANGE_CLOEXEC: libc::c_uint = 1 << 2;
        // SAFETY: close_range with CLOSE_RANGE_CLOEXEC only sets a flag on
        // this process's descriptors; it frees nothing. Linux 5.11 and later
        // support it; older kernels return ENOSYS or EINVAL and the loop
        // below does the same job.
        let done = unsafe {
            libc::syscall(
                libc::SYS_close_range,
                3 as libc::c_uint,
                libc::c_uint::MAX,
                CLOSE_RANGE_CLOEXEC,
            )
        } == 0;
        if done {
            return;
        }
    }
    for fd in 3..limit {
        // SAFETY: fcntl on a number that is not an open descriptor fails with
        // EBADF and changes nothing. FD_CLOEXEC is the only descriptor flag,
        // so setting it outright loses nothing.
        unsafe {
            libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
        }
    }
}

/// One-time compatibility fallback for a protocol-v1 peer that cannot drain.
pub fn terminate_legacy_peer(pid: u32) -> io::Result<()> {
    use std::os::raw::c_int;
    unsafe extern "C" {
        fn kill(pid: c_int, signal: c_int) -> c_int;
    }
    const SIGTERM: c_int = 15;
    // SAFETY: the PID came from kernel credentials on the still-live socket;
    // this never trusts a stale discovery record.
    if unsafe { kill(pid as c_int, SIGTERM) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

impl Duplex for UnixDuplex {
    type Reader = UnixReader;
    type Writer = UnixWriter;

    /// Make one connection attempt bounded by the caller's absolute deadline.
    fn connect_once_until(path: &Endpoint, deadline: Instant) -> Result<Self, ConnectError> {
        // `SockAddr::unix` would refuse it with an invalid argument, which read
        // as a timeout and was retried until the deadline.
        if crate::socket_path::check_socket_path(path).is_err() {
            return Err(ConnectError::EndpointTooLong);
        }
        let connect = || -> io::Result<UnixStream> {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io::ErrorKind::TimedOut.into());
            }
            let socket = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)?;
            socket.connect_timeout(&socket2::SockAddr::unix(path)?, remaining)?;
            let fd: std::os::fd::OwnedFd = socket.into();
            let stream = UnixStream::from(fd);
            stream.peer_addr()?;
            Ok(stream)
        };
        connect()
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

    /// Reject another OS user before sending a preamble or handoff token.
    fn verify_peer_user(&self) -> io::Result<()> {
        verify_peer_user(self.stream.as_raw_fd())
    }

    /// PID of the process at the other end of this live socket.
    ///
    /// This is stronger than trusting the discovery file's PID: the open
    /// connection pins the peer while the kernel reports its credentials.
    fn peer_pid(&self) -> io::Result<u32> {
        peer_pid(self.stream.as_raw_fd())
    }

    /// Arm one absolute deadline for the whole session-establishment phase.
    fn set_read_deadline(&self, timeout: Option<Duration>) -> io::Result<()> {
        let deadline = timeout
            .map(|duration| {
                Instant::now().checked_add(duration).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "read deadline overflow")
                })
            })
            .transpose()?;
        self.stream.set_nonblocking(deadline.is_some())?;
        self.read_deadline.set(deadline);
        Ok(())
    }

    fn split(self) -> io::Result<(Self::Reader, Self::Writer)> {
        let deadline = self.read_deadline.get();
        let writer = self.stream.try_clone()?;
        let setup = Arc::new(Setup {
            active: AtomicBool::new(deadline.is_some()),
            transition: Mutex::new(()),
        });
        Ok((
            UnixReader {
                stream: self.stream,
                read_deadline: deadline,
                setup: Arc::clone(&setup),
            },
            UnixWriter {
                stream: writer,
                deadline,
                setup,
            },
        ))
    }
}

// Only setup-mode transitions serialize with setup writes. Ordinary transport
// I/O never acquires this mutex, and a read never holds a writer's lock.
struct Setup {
    active: AtomicBool,
    transition: Mutex<()>,
}

/// Unix receive half with an optional absolute setup deadline.
pub struct UnixReader {
    stream: UnixStream,
    read_deadline: Option<Instant>,
    setup: Arc<Setup>,
}

impl UnixReader {
    /// Return to ordinary blocking reads before entering the byte pump.
    pub fn clear_read_deadline(&mut self) -> io::Result<()> {
        let _transition = self
            .setup
            .transition
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.stream.set_nonblocking(false)?;
        self.read_deadline = None;
        self.setup.active.store(false, Ordering::Release);
        Ok(())
    }
}

impl Read for UnixReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let Some(deadline) = self.read_deadline else {
            return self.stream.read(buf);
        };
        read_until(&self.stream, buf, deadline)
    }
}

fn establishment_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "Unix-socket session establishment timed out",
    )
}

/// Unix send half; shutdown leaves the receive direction alive.
pub struct UnixWriter {
    stream: UnixStream,
    deadline: Option<Instant>,
    setup: Arc<Setup>,
}

impl Write for UnixWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.setup.active.load(Ordering::Acquire) {
            return self.stream.write(buf);
        }
        let transition = self
            .setup
            .transition
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.setup.active.load(Ordering::Acquire) {
            let deadline = self.deadline.expect("setup has a deadline");
            write_until(&self.stream, buf, deadline)
        } else {
            drop(transition);
            self.stream.write(buf)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

impl WriteHalf for UnixWriter {
    fn shutdown_write(&mut self) -> io::Result<()> {
        self.stream.shutdown(Shutdown::Write)
    }
}

impl Read for UnixDuplex {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.read_deadline.get() {
            Some(deadline) => read_until(&self.stream, buf, deadline),
            None => self.stream.read(buf),
        }
    }
}

impl Write for UnixDuplex {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.read_deadline.get() {
            Some(deadline) => write_until(&self.stream, buf, deadline),
            None => self.stream.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }
}

fn read_until(stream: &UnixStream, bytes: &mut [u8], deadline: Instant) -> io::Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let fd = stream.as_raw_fd();
    bounded_io(fd, libc::POLLIN, deadline, || {
        // SAFETY: the owned socket and writable slice stay alive through recv.
        unsafe {
            libc::recv(
                fd,
                bytes.as_mut_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT,
            )
        }
    })
}
fn write_until(stream: &UnixStream, bytes: &[u8], deadline: Instant) -> io::Result<usize> {
    if bytes.is_empty() {
        return Ok(0);
    }
    let fd = stream.as_raw_fd();
    bounded_io(fd, libc::POLLOUT, deadline, || {
        // SAFETY: the owned socket and immutable slice stay alive through send.
        // Setup holds the socket in nonblocking mode; the transition guard
        // prevents clearing that mode during this write. MSG_NOSIGNAL avoids SIGPIPE.
        unsafe {
            libc::send(
                fd,
                bytes.as_ptr().cast(),
                bytes.len(),
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        }
    })
}
fn bounded_io(
    fd: RawFd,
    events: libc::c_short,
    deadline: Instant,
    mut operation: impl FnMut() -> isize,
) -> io::Result<usize> {
    loop {
        if Instant::now() >= deadline {
            return Err(establishment_timeout());
        }
        let count = operation();
        if count >= 0 {
            if Instant::now() >= deadline {
                return Err(establishment_timeout());
            }
            return Ok(count as usize);
        }
        let error = io::Error::last_os_error();
        match error.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => {}
            _ => return Err(error),
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let millis = remaining.as_millis().max(1).min(i32::MAX as u128) as i32;
        let mut poll = libc::pollfd {
            fd,
            events,
            revents: 0,
        };
        // SAFETY: poll receives one live, initialized entry for this owned socket.
        let result = unsafe { libc::poll(&mut poll, 1, millis) };
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
        // recv/send remains nonblocking after readiness, including spurious wakeups.
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn only_minus_one_is_a_failed_call() {
        assert_eq!(super::checked(0).unwrap(), 0);
        assert_eq!(super::checked(1).unwrap(), 1);
        assert_eq!(super::checked(3).unwrap(), 3);
        assert!(super::checked(-1).is_err());
    }

    use super::*;
    use std::os::unix::net::UnixListener;

    #[test]
    fn a_closed_peer_does_not_hide_the_buffered_final_response() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let duplex = UnixDuplex {
            stream,
            read_deadline: Cell::new(None),
        };
        duplex
            .set_read_deadline(Some(Duration::from_secs(1)))
            .unwrap();
        let (mut read, _write) = duplex.split().unwrap();
        peer.write_all(b"final response").unwrap();
        drop(peer);
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"final response");
        read.clear_read_deadline().unwrap();
    }

    #[test]
    fn an_unsplit_socket_also_honors_its_setup_deadline() {
        let (stream, _peer) = UnixStream::pair().unwrap();
        let mut duplex = UnixDuplex {
            stream,
            read_deadline: Cell::new(None),
        };
        duplex
            .set_read_deadline(Some(Duration::from_millis(10)))
            .unwrap();
        let started = Instant::now();
        assert_eq!(
            duplex.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn process_state_reports_alive_exited_and_unknown() {
        use super::super::ProcessState;
        assert_eq!(process_state(std::process::id()), ProcessState::Alive);
        assert_eq!(process_state(0), ProcessState::Unknown);
        assert_eq!(process_state(u32::MAX), ProcessState::Unknown);
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        // Reaped, so the PID is gone (barring reuse within this test).
        assert_eq!(process_state(pid), ProcessState::Exited);
        assert!(process_has_exited(pid));
    }

    /// Hand `fd` back to an owner after a refused take.
    fn reclaim(fd: RawFd) {
        use std::os::fd::{FromRawFd, OwnedFd};
        // SAFETY: the test owned `fd` and the refused take did not close it.
        drop(unsafe { OwnedFd::from_raw_fd(fd) });
    }

    #[test]
    fn only_the_write_end_of_a_pipe_is_taken_as_a_readiness_channel() {
        use std::os::fd::{IntoRawFd, OwnedFd};
        for value in ["", "x", "-1", "0", "1", "2"] {
            // SAFETY: refused before any descriptor is touched.
            let error = unsafe { take_ready_descriptor(value) }.unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{value:?}");
        }

        let file = tempfile::tempfile().unwrap().into_raw_fd();
        // SAFETY: the test owns `file`, and a refused take leaves it alone.
        let error = unsafe { take_ready_descriptor(&file.to_string()) }.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        reclaim(file);

        let (reader, writer) = std::io::pipe().unwrap();
        let read_end = OwnedFd::from(reader).into_raw_fd();
        // SAFETY: the test owns `read_end`, and a refused take leaves it alone.
        let error = unsafe { take_ready_descriptor(&read_end.to_string()) }.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        reclaim(read_end);
        drop(writer);
    }

    #[test]
    fn a_taken_readiness_channel_is_not_inherited_and_carries_messages() {
        use std::os::fd::{IntoRawFd, OwnedFd};
        let (mut reader, writer) = std::io::pipe().unwrap();
        let write_end = OwnedFd::from(writer);
        // SAFETY: clears close-on-exec on an open descriptor this test owns,
        // as the launcher's child sees its channel end after the exec.
        let cleared = unsafe { libc::fcntl(write_end.as_raw_fd(), libc::F_SETFD, 0) };
        assert_ne!(cleared, -1);
        let write_end = write_end.into_raw_fd();
        // SAFETY: the test gave up ownership of `write_end` just above.
        let mut pipe = unsafe { take_ready_descriptor(&write_end.to_string()) }.unwrap();
        // SAFETY: F_GETFD only reads the descriptor's flags.
        let flags = unsafe { libc::fcntl(write_end, libc::F_GETFD) };
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "programs the daemon starts would hold it"
        );
        pipe.write_all(b"ready\n").unwrap();
        let mut received = [0u8; 6];
        reader.read_exact(&mut received).unwrap();
        assert_eq!(&received, b"ready\n");
    }

    #[cfg(feature = "launch")]
    #[test]
    fn a_readiness_pipe_leaves_the_standard_streams_alone() {
        let (_reader, child_end) = ready_pipe().unwrap();
        let fd = child_end.as_raw_fd();
        assert!(fd > 2, "{fd}");
        // SAFETY: F_GETFD only reads the descriptor's flags.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert_ne!(
            flags & libc::FD_CLOEXEC,
            0,
            "another child could inherit it"
        );
    }

    #[test]
    fn the_signal_fallback_reports_alive_exited_and_unknown() {
        use super::super::ProcessState;
        assert_eq!(signal_state(std::process::id()), ProcessState::Alive);
        assert_eq!(signal_state(0), ProcessState::Unknown);
        assert_eq!(signal_state(u32::MAX), ProcessState::Unknown);
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id();
        child.wait().unwrap();
        assert_eq!(signal_state(pid), ProcessState::Exited);
    }

    /// Linux reads a zombie's state from /proc, so the fallback reports it.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_signal_fallback_reports_an_unreaped_child_as_exited_on_linux() {
        use super::super::{ProcessHandle, ProcessState};
        let mut child = std::process::Command::new("true").spawn().unwrap();
        ProcessHandle::for_child(&child).unwrap().wait().unwrap();
        assert_eq!(signal_state(child.id()), ProcessState::Exited);
        child.wait().unwrap();
    }

    #[cfg(feature = "launch")]
    #[test]
    fn a_new_session_child_is_watched_from_its_spawn() {
        let mut command = std::process::Command::new("true");
        let (mut child, mut exit) = spawn_in_new_session_watched(&mut command, None).unwrap();
        assert_eq!(exit.pid(), child.id());
        exit.wait().unwrap();
        assert!(child.wait().unwrap().success());
    }

    #[test]
    fn a_failed_watch_stops_the_child_and_returns_the_error() {
        let mut command = std::process::Command::new("sleep");
        command.arg("30");
        let mut pid = None;
        let error = spawn_then(&mut command, |child| {
            pid = Some(child.id());
            Err::<(), _>(io::Error::other("watch failed"))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "watch failed");
        // Stopped and reaped: no process has the PID any more.
        assert_eq!(
            process_state(pid.unwrap()),
            super::super::ProcessState::Exited
        );
    }

    #[test]
    fn a_new_session_child_leads_its_own_session_and_process_group() {
        unsafe extern "C" {
            fn getsid(pid: i32) -> i32;
            fn getpgid(pid: i32) -> i32;
        }
        let mut command = std::process::Command::new("sleep");
        command.arg("30");
        let mut child = spawn_in_new_session(&mut command).unwrap();
        let pid = child.id() as i32;
        // SAFETY: both only read the session and group IDs of a live PID
        // (our child, not yet waited) and of this process (0).
        let (child_sid, child_pgid, own_sid) = unsafe { (getsid(pid), getpgid(pid), getsid(0)) };
        child.kill().unwrap();
        child.wait().unwrap();
        assert_eq!(child_sid, pid, "not a session leader");
        assert_eq!(child_pgid, pid, "not a process group leader");
        assert_ne!(child_sid, own_sid, "still in the caller's session");
    }

    #[test]
    fn a_process_this_user_may_not_signal_is_alive_not_exited() {
        use super::super::ProcessState;
        assert_eq!(liveness_from_errno(Some(1)), ProcessState::Alive);
        assert_eq!(liveness_from_errno(Some(3)), ProcessState::Exited);
        assert_eq!(liveness_from_errno(Some(22)), ProcessState::Unknown);
        assert_eq!(liveness_from_errno(None), ProcessState::Unknown);
    }

    fn temp_socket(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "daemon-transport-test-{}-{}.sock",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    #[test]
    fn setup_write_deadline_bounds_a_peer_that_never_reads() {
        let path = temp_socket("write-deadline");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let (release, wait) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (_peer, _) = listener.accept().unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(2));
        });
        let transport = UnixDuplex::connect(&path).unwrap();
        transport.verify_peer_user().unwrap();
        transport
            .set_read_deadline(Some(Duration::from_millis(100)))
            .unwrap();
        let (_read, mut write) = transport.split().unwrap();
        let error = write.write_all(&vec![0; 16 << 20]).unwrap_err();
        release.send(()).ok();
        server.join().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn clearing_setup_restores_writes_after_the_original_deadline() {
        let (stream, mut peer) = UnixStream::pair().unwrap();
        let transport = UnixDuplex {
            stream,
            read_deadline: Cell::new(None),
        };
        transport.verify_peer_user().unwrap();
        assert!(verify_peer_user(-1).is_err());
        transport
            .set_read_deadline(Some(Duration::from_millis(1)))
            .unwrap();
        let (mut read, mut write) = transport.split().unwrap();
        read.clear_read_deadline().unwrap();
        std::thread::sleep(Duration::from_millis(5));
        write.write_all(b"long job").unwrap();
        let mut bytes = [0; 8];
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"long job");
    }

    #[test]
    fn an_absent_socket_times_out_rather_than_hanging() {
        let path = temp_socket("absent");
        let started = Instant::now();
        // A short budget: the point is that it gives up, not how long it waits.
        let err = UnixDuplex::connect_until(&path, Instant::now() + Duration::from_millis(150));
        assert_eq!(err.err(), Some(ConnectError::ConnectTimeout));
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "connect did not respect its deadline"
        );
    }

    #[test]
    fn an_overlong_endpoint_fails_at_once_instead_of_timing_out() {
        // It used to read as a timeout and be retried for the whole budget.
        let path = std::path::PathBuf::from(format!(
            "/{}",
            "a".repeat(super::super::unix_socket::MAX_PATH_BYTES)
        ));
        let started = Instant::now();
        let err = UnixDuplex::connect_until(&path, Instant::now() + Duration::from_secs(5));
        assert_eq!(err.err(), Some(ConnectError::EndpointTooLong));
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "it retried a path that cannot work"
        );
    }

    #[test]
    fn a_listening_socket_connects_and_splits() {
        let path = temp_socket("listen");
        let listener = UnixListener::bind(&path).unwrap();

        let accept = std::thread::spawn(move || listener.accept().map(|(s, _)| s));

        let duplex = UnixDuplex::connect(&path).expect("connect");
        let (mut r, mut w) = duplex.split().expect("split");

        let mut server = accept.join().unwrap().unwrap();

        w.write_all(b"ping").unwrap();
        w.shutdown_write().unwrap();

        let mut got = [0u8; 4];
        server.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"ping");

        // The half-close must not have torn down the reply direction.
        server.write_all(b"pong").unwrap();
        drop(server);

        let mut back = Vec::new();
        r.read_to_end(&mut back).unwrap();
        assert_eq!(back, b"pong");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn the_two_halves_are_independent_file_descriptors() {
        // If `split` handed back the same fd behind a lock, the parked read
        // below would block the write and this test would hang.
        let path = temp_socket("split");
        let listener = UnixListener::bind(&path);
        let accept = std::thread::spawn(move || listener.unwrap().accept().map(|(s, _)| s));

        let (mut r, mut w) = UnixDuplex::connect(&path).unwrap().split().unwrap();
        let mut server = accept.join().unwrap().unwrap();

        let reader = std::thread::spawn(move || {
            let mut buf = [0u8; 4];
            r.read_exact(&mut buf).map(|_| buf)
        });

        // Written while the reader thread is parked in `read`.
        w.write_all(b"up__").unwrap();
        let mut got = [0u8; 4];
        server.read_exact(&mut got).unwrap();
        assert_eq!(&got, b"up__");

        server.write_all(b"down").unwrap();
        assert_eq!(&reader.join().unwrap().unwrap(), b"down");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_deadline_bounds_a_peer_that_never_answers_the_preamble() {
        // The wedge this exists for: the kernel completed the connection, so
        // `connect` succeeded, but nothing ever writes an acknowledgement.
        // Without the deadline the relay would park here forever during client setup.
        let path = temp_socket("silent");
        let listener = UnixListener::bind(&path).unwrap();
        let accept = std::thread::spawn(move || listener.accept().map(|(s, _)| s));

        let duplex = UnixDuplex::connect(&path).expect("the kernel-level connect succeeds");
        duplex
            .set_read_deadline(Some(Duration::from_millis(100)))
            .expect("arm");
        let mut server = accept.join().unwrap().unwrap();

        let (mut r, mut w) = duplex.split().expect("split");
        let started = Instant::now();
        let outcome = crate::local::test_handshake(&mut w, &mut r, "test");
        let elapsed = started.elapsed();

        assert!(outcome.is_err(), "a silent peer must not pass");
        assert!(
            elapsed < Duration::from_secs(2),
            "handshake ignored its deadline: {elapsed:?}"
        );
        // Disarming restores ordinary pump semantics: EOF arrives when the
        // peer closes, rather than every read failing with a timeout error.
        r.clear_read_deadline().unwrap();
        // The server never read our preamble. On Linux, dropping a socket
        // that still holds unread data surfaces to the peer as ECONNRESET
        // rather than EOF - so drain exactly what was written before closing,
        // and the client sees the clean EOF a healthy teardown produces.
        // `PREAMBLE_LEN` is the fixed wire size INCLUDING the identity.
        let mut drained = 0;
        while drained < 6 {
            match server.read(&mut [0u8; 64]) {
                Ok(0) => break,
                Ok(n) => drained += n,
                Err(e) => panic!("server drain failed: {e}"),
            }
        }
        drop(server);
        let mut sink = [0u8; 1];
        assert_eq!(r.read(&mut sink).unwrap(), 0);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_drip_fed_ack_cannot_extend_the_establishment_deadline() {
        let path = temp_socket("drip");
        let listener = UnixListener::bind(&path).unwrap();
        let server = std::thread::spawn(move || {
            let (mut connection, _) = listener.accept().unwrap();
            let mut preamble = [0u8; 6];
            connection.read_exact(&mut preamble).unwrap();
            for byte in *b"ready\n" {
                if connection.write_all(&[byte]).is_err() {
                    break;
                }
                connection.flush().unwrap();
                std::thread::sleep(Duration::from_millis(30));
            }
        });

        let duplex = UnixDuplex::connect(&path).unwrap();
        duplex
            .set_read_deadline(Some(Duration::from_millis(150)))
            .unwrap();
        let (mut read, mut write) = duplex.split().unwrap();
        let started = Instant::now();
        let outcome = crate::local::test_handshake(&mut write, &mut read, "test");
        let elapsed = started.elapsed();

        assert!(outcome.is_err(), "a slow drip must not reset the deadline");
        assert!(
            elapsed < Duration::from_millis(350),
            "drip feed extended the absolute deadline: {elapsed:?}"
        );
        server.join().unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_socket_with_no_permissions_is_reported_as_permission_denied() {
        // Mode 0000 is enforced on connect (verified on macOS 26.5), and this
        // must short-circuit rather than spend the whole retry budget.
        use std::os::unix::fs::PermissionsExt;

        let path = temp_socket("perms");
        let _listener = UnixListener::bind(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        let started = Instant::now();
        let result = UnixDuplex::connect(&path);
        let elapsed = started.elapsed();

        // Root ignores file modes, so on a root CI container this legitimately
        // connects; the assertion is conditional rather than flaky.
        if let Err(e) = result {
            assert_eq!(e, ConnectError::PermissionDenied);
            assert!(
                elapsed < crate::local::CONNECT_BUDGET,
                "permission failure burned the retry budget instead of short-circuiting"
            );
        }

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_live_endpoint_connect_once_does_not_retry_sleep() {
        use super::super::{Duplex, RETRY_INTERVAL};

        let path = temp_socket("live-once");
        let listener = UnixListener::bind(&path).unwrap();
        let accept = std::thread::spawn(move || listener.accept());
        let started = std::time::Instant::now();
        let duplex = UnixDuplex::connect_once(&path).expect("live endpoint");
        let elapsed = started.elapsed();
        assert!(
            elapsed < RETRY_INTERVAL,
            "live connect inserted a retry sleep: {elapsed:?}"
        );
        drop(duplex);
        let _ = accept.join();
        let _ = std::fs::remove_file(&path);
    }
}
