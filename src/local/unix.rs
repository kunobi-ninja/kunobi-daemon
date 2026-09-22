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
pub fn spawn_with_child_waiting(
    command: &mut std::process::Command,
) -> io::Result<std::process::Child> {
    // Signal dispositions are process-wide. Serialize relay launches until
    // both the previous policy and any exits during this window are handled.
    static SPAWN: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _spawn = SPAWN.lock().unwrap_or_else(|error| error.into_inner());
    let previous = child_disposition(0)?;
    let child = command.spawn();
    child_disposition(previous)?;
    if previous == 1 {
        reap_exited_children();
    }
    child
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

/// True only when the OS establishes that a previously verified PID exited.
pub fn process_has_exited(pid: u32) -> bool {
    unsafe extern "C" {
        fn kill(pid: i32, signal: i32) -> i32;
    }
    if pid == 0 || pid > i32::MAX as u32 {
        return false;
    }
    // A container's PID 1 may leave an exited orphan unreaped. kill(pid, 0)
    // still succeeds for that zombie, although it cannot own any more work.
    #[cfg(target_os = "linux")]
    if std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|stat| {
        stat.rsplit_once(") ")
            .is_some_and(|(_, fields)| fields.starts_with("Z ") || fields.starts_with("X "))
    }) {
        return true;
    }
    // SAFETY: signal 0 checks process existence without delivering a signal.
    (unsafe { kill(pid as i32, 0) }) != 0 && io::Error::last_os_error().raw_os_error() == Some(3)
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
}
