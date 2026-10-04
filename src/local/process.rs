//! Process exit as an event: [`ProcessHandle`], and the PID checks built on it.

use std::io;
use std::process::{Child, ExitStatus};
use std::time::Instant;

use super::ProcessState;
use crate::ProcessId;

#[cfg(any(target_os = "linux", target_os = "android"))]
mod pidfd;
#[cfg(any(target_os = "linux", target_os = "android"))]
use self::pidfd as sys;

#[cfg(any(
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
))]
mod kqueue;
#[cfg(any(
    target_vendor = "apple",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "openbsd",
    target_os = "netbsd"
))]
use self::kqueue as sys;

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))
))]
mod unsupported;
#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "openbsd",
        target_os = "netbsd"
    ))
))]
use self::unsupported as sys;

#[cfg(windows)]
mod windows;
#[cfg(windows)]
use self::windows as sys;

#[cfg(windows)]
use self::windows::unknown_state as fallback_state;
#[cfg(unix)]
use super::unix::signal_state as fallback_state;

/// What opening a process found.
enum Opened {
    /// The OS will report the process's exit through this event.
    Watching(sys::Event),
    /// No process has this PID: it exited, or never existed.
    Gone,
    /// This system cannot report the exit as an event.
    #[cfg(unix)]
    Unsupported,
}

#[derive(Debug)]
enum Source {
    Event(sys::Event),
    Exited,
    /// No exit event: check whether the PID still names a running process.
    #[cfg(unix)]
    Pid,
}

/// One process whose exit this process can wait for.
///
/// The OS tells the handle when the process exits, so nothing checks a PID on
/// a timer, and a PID the OS later gives to another process does not change
/// the answer:
///
/// | Platform | Exit event |
/// | --- | --- |
/// | Linux, Android | a pidfd (`pidfd_open`, Linux 5.3 and later), readable once the process exits |
/// | macOS, FreeBSD, NetBSD, OpenBSD, DragonFly | a kqueue watching `EVFILT_PROC` for `NOTE_EXIT` |
/// | Windows | a process handle, signaled once the process exits |
///
/// Open it with [`ProcessHandle::open`] while the PID is known to name the
/// process you mean, for example from the peer credentials of a connection
/// that is still open, or with [`ProcessHandle::for_child`] for a child that
/// has not been waited for. From then on the handle follows that process, not
/// its PID.
///
/// Waiting reports only that the process exited. Its exit status belongs to
/// its parent: for a child, call `wait` or `try_wait` on the child once the
/// handle reports the exit. An error means this process could not find out,
/// never that the watched process failed.
///
/// # Fallback without an exit event
///
/// On Linux before 5.3, where `pidfd_open` is missing or a seccomp policy
/// refuses it, and on other Unix systems, the handle checks the PID instead, with a
/// pause that grows to 50 ms between checks. That fallback has the PID's two
/// weaknesses: a PID reused by another process reads as still running, and,
/// except on Linux, a child that exited but has not been waited for reads as
/// running too. [`ProcessHandle::is_pid_only`] reports it. The crate's waits
/// for its own children (`launch::DaemonChild`, `Candidate` and the warmup
/// reaper) then ask the child through `try_wait` instead, and do the same
/// when the handle could not be opened at all.
#[derive(Debug)]
pub struct ProcessHandle {
    pid: ProcessId,
    source: Source,
}

impl ProcessHandle {
    /// Watch the process that has `pid` now.
    ///
    /// A process that already exited, or a PID no process has, gives a
    /// handle that reports the exit at once.
    ///
    /// The process need not be a child. Linux takes the PID in this
    /// process's PID namespace, and macOS lets any process be watched.
    /// FreeBSD watches only a process this user may debug and falls back to
    /// the PID for others. Windows needs the right to wait on the process,
    /// which an elevated or protected process may refuse with
    /// `PermissionDenied`.
    ///
    /// A PID read from a discovery record may already name another process.
    /// Take it from the peer credentials of a live connection instead
    /// ([`super::peer::PeerCredentials`]), and open the handle while that
    /// connection is still open.
    pub fn open(pid: ProcessId) -> io::Result<Self> {
        let source = match sys::open(pid.get())? {
            Opened::Watching(event) => Source::Event(event),
            Opened::Gone => Source::Exited,
            #[cfg(unix)]
            Opened::Unsupported => Source::Pid,
        };
        Ok(Self { pid, source })
    }

    /// Watch a child of this process.
    ///
    /// On Windows the child's own handle keeps its PID from being reused. On
    /// Unix the PID stays the child's until it is waited for, so open the
    /// handle before anything waits for it. A process that ignores SIGCHLD has
    /// its children reaped by the kernel as they exit, and there a child can
    /// be gone, and its PID reused, before this runs.
    pub fn for_child(child: &Child) -> io::Result<Self> {
        Self::open(ProcessId::of(child))
    }

    /// The PID this handle was opened with.
    pub fn pid(&self) -> ProcessId {
        self.pid
    }

    /// True when this handle checks the PID because no exit event was
    /// available. See the fallback section above.
    pub fn is_pid_only(&self) -> bool {
        #[cfg(unix)]
        {
            matches!(self.source, Source::Pid)
        }
        #[cfg(windows)]
        {
            false
        }
    }

    /// Whether the process has exited, without blocking.
    pub fn has_exited(&mut self) -> io::Result<bool> {
        self.wait_for(Some(Instant::now()))
    }

    /// Block until the process exits.
    pub fn wait(&mut self) -> io::Result<()> {
        self.wait_for(None).map(drop)
    }

    /// Block until the process exits or `deadline` passes. True if it exited.
    pub fn wait_until(&mut self, deadline: Instant) -> io::Result<bool> {
        self.wait_for(Some(deadline))
    }

    fn wait_for(&mut self, deadline: Option<Instant>) -> io::Result<bool> {
        let exited = match &mut self.source {
            Source::Event(event) => event.wait(deadline)?,
            Source::Exited => true,
            #[cfg(unix)]
            Source::Pid => pid_wait(self.pid.get(), deadline)?,
        };
        if exited {
            // A kqueue reports the exit once; remember it, and release the
            // descriptor or handle now that it has nothing more to say.
            self.source = Source::Exited;
        }
        Ok(exited)
    }

    /// Complete when the process exits.
    ///
    /// On Unix this registers the descriptor with the Tokio reactor, so it
    /// needs a runtime with I/O enabled (`enable_io` or `enable_all`). On
    /// Windows a thread-pool wait wakes it. Cancelling the future stops only
    /// this wait.
    #[cfg(feature = "async")]
    pub async fn exited(&mut self) -> io::Result<()> {
        match &mut self.source {
            Source::Event(event) => event.exited().await?,
            Source::Exited => {}
            #[cfg(unix)]
            Source::Pid => {
                let mut pause = crate::backoff::FIRST;
                while !pid_exited(self.pid.get())? {
                    let nap = crate::backoff::next_pause(&mut pause, None).expect("no deadline");
                    #[expect(
                        clippy::disallowed_methods,
                        reason = "Only the PID fallback polls; supported platforms await the OS exit event."
                    )]
                    tokio::time::sleep(nap).await;
                }
            }
        }
        self.source = Source::Exited;
        Ok(())
    }

    /// As [`Self::exited`], giving up at `deadline`. True if it exited.
    #[cfg(feature = "async")]
    pub async fn exited_until(&mut self, deadline: tokio::time::Instant) -> io::Result<bool> {
        match tokio::time::timeout_at(deadline, self.exited()).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        }
    }
}

/// What the OS establishes about `pid`: see [`ProcessState`].
///
/// This opens a [`ProcessHandle`] and checks it once. With only a PID the
/// answer is about whichever process holds that PID now; to follow one
/// process, keep a handle opened while the PID was known to be it. Where the
/// handle cannot be opened, for example because access was denied, Unix
/// falls back to `kill(pid, 0)`, which reports a process this user may not
/// signal as alive, and Windows reports `Unknown`.
pub fn process_state(pid: ProcessId) -> ProcessState {
    match ProcessHandle::open(pid).and_then(|mut handle| handle.has_exited()) {
        Ok(true) => ProcessState::Exited,
        Ok(false) => ProcessState::Alive,
        Err(_) => fallback_state(pid.get()),
    }
}

/// True only when the OS establishes that a previously verified PID exited.
pub fn process_has_exited(pid: ProcessId) -> bool {
    process_state(pid) == ProcessState::Exited
}

/// Whole milliseconds from `now` to `deadline`, rounded up so that a wait for
/// that long never ends before the deadline. Zero once it has passed.
// A kqueue takes its timeout in nanoseconds.
#[cfg_attr(
    not(any(target_os = "linux", target_os = "android", windows)),
    allow(dead_code)
)]
fn millis_until(deadline: Instant, now: Instant) -> u128 {
    deadline
        .saturating_duration_since(now)
        .as_nanos()
        .div_ceil(1_000_000)
}

#[cfg(unix)]
fn pid_exited(pid: u32) -> io::Result<bool> {
    match fallback_state(pid) {
        ProcessState::Exited => Ok(true),
        ProcessState::Alive => Ok(false),
        ProcessState::Unknown => Err(io::Error::other(
            "the OS did not say whether the process exited",
        )),
    }
}

/// The PID fallback's blocking wait.
#[cfg(unix)]
fn pid_wait(pid: u32, deadline: Option<Instant>) -> io::Result<bool> {
    crate::backoff::poll(|| pid_exited(pid), deadline)
}

/// Wait until a child this process owns exits or `deadline` passes. True if
/// it exited.
///
/// `exit` is the child's handle, or `None` if it could not be opened, and
/// `try_wait` asks the child itself. Without an exit event, whether for want
/// of a handle or because the handle only follows the PID, this polls
/// `try_wait`: a PID check outside Linux reads an exited child that has not
/// been waited for as running, but the child's parent can tell. That path
/// reaps the child, and its status stays with it.
pub(crate) fn wait_child(
    mut try_wait: impl FnMut() -> io::Result<Option<ExitStatus>>,
    exit: Option<&mut ProcessHandle>,
    deadline: Instant,
) -> io::Result<bool> {
    match exit {
        Some(exit) if !exit.is_pid_only() => exit.wait_until(deadline),
        _ => crate::backoff::poll(|| Ok(try_wait()?.is_some()), Some(deadline)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;
    use std::process::{Command, Stdio};
    use std::time::Duration;

    /// Run by [`blocked_child`]: block until stdin closes, then exit 3.
    #[test]
    #[ignore = "helper process for the exit-event tests"]
    fn blocked_child_helper() {
        if std::env::var_os("KDAEMON_BLOCKED_CHILD").is_none() {
            return;
        }
        let _ = std::io::stdin().read_to_end(&mut Vec::new());
        std::process::exit(3);
    }

    /// A child that runs until its stdin is closed, then exits with code 3.
    /// Dropping its `stdin` releases it; nothing else does.
    fn blocked_child() -> Child {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "local::process::tests::blocked_child_helper",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("KDAEMON_BLOCKED_CHILD", "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    }

    fn release(child: &mut Child) {
        drop(child.stdin.take());
    }

    #[test]
    fn the_exit_event_fires_only_once_the_process_exits() {
        let mut child = blocked_child();
        let mut exit = ProcessHandle::for_child(&child).unwrap();
        assert_eq!(exit.pid(), ProcessId::of(&child));
        assert!(!exit.is_pid_only(), "no exit event on this system");
        assert!(!exit.has_exited().unwrap());

        // The child cannot exit before it is released, so this must wait the
        // whole time and then report no exit.
        let started = Instant::now();
        assert!(
            !exit
                .wait_until(started + Duration::from_millis(100))
                .unwrap()
        );
        assert!(started.elapsed() >= Duration::from_millis(100));
        assert!(child.try_wait().unwrap().is_none());

        release(&mut child);
        assert!(
            exit.wait_until(Instant::now() + Duration::from_secs(30))
                .unwrap()
        );
        assert!(exit.has_exited().unwrap());
        // The event fired before the status was collected, and the status
        // is still the child's to report.
        assert_eq!(child.wait().unwrap().code(), Some(3));
        // Reaping does not make the handle forget.
        assert!(exit.has_exited().unwrap());
        exit.wait().unwrap();
    }

    #[test]
    fn a_blocking_wait_returns_when_the_process_exits_and_not_before() {
        let mut child = blocked_child();
        let mut exit = ProcessHandle::for_child(&child).unwrap();
        let (done, waited) = std::sync::mpsc::channel();
        let waiter = std::thread::spawn(move || {
            let result = exit.wait();
            let _ = done.send(());
            result
        });
        assert!(
            waited.recv_timeout(Duration::from_millis(200)).is_err(),
            "the wait returned while the process was still running"
        );
        release(&mut child);
        waiter.join().unwrap().unwrap();
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[test]
    fn an_exited_child_not_yet_waited_for_reads_as_exited() {
        // Before the exit event, a zombie answered kill(pid, 0) and read as
        // alive everywhere except Linux.
        let mut child = blocked_child();
        let mut first = ProcessHandle::for_child(&child).unwrap();
        release(&mut child);
        first.wait().unwrap();

        // Exited, not yet waited for: its PID still names it.
        let mut late = ProcessHandle::open(ProcessId::of(&child)).unwrap();
        assert!(late.has_exited().unwrap());
        assert_eq!(process_state(ProcessId::of(&child)), ProcessState::Exited);
        assert!(process_has_exited(ProcessId::of(&child)));
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[test]
    fn a_process_that_is_gone_reads_as_exited_at_once() {
        let mut child = blocked_child();
        release(&mut child);
        child.wait().unwrap();
        // Reaped, so no process has the PID (barring reuse within this test).
        let mut exit = ProcessHandle::open(ProcessId::of(&child)).unwrap();
        assert!(exit.has_exited().unwrap());
        exit.wait().unwrap();
        assert_eq!(process_state(ProcessId::of(&child)), ProcessState::Exited);
    }

    #[test]
    fn the_current_process_is_alive() {
        let mut own = ProcessHandle::open(ProcessId::current()).unwrap();
        assert!(!own.has_exited().unwrap());
        assert_eq!(process_state(ProcessId::current()), ProcessState::Alive);
        assert!(!process_has_exited(ProcessId::current()));
    }

    #[test]
    fn deadlines_round_up_to_whole_milliseconds() {
        let now = Instant::now();
        assert_eq!(millis_until(now, now), 0);
        assert_eq!(millis_until(now, now + Duration::from_secs(1)), 0);
        assert_eq!(millis_until(now + Duration::from_nanos(1), now), 1);
        assert_eq!(millis_until(now + Duration::from_millis(1), now), 1);
        assert_eq!(
            millis_until(
                now + Duration::from_millis(1) + Duration::from_nanos(1),
                now
            ),
            2
        );
        assert_eq!(millis_until(now + Duration::from_secs(5), now), 5000);
    }

    #[cfg(unix)]
    fn pid_only(child: &Child) -> ProcessHandle {
        ProcessHandle {
            pid: ProcessId::of(child),
            source: Source::Pid,
        }
    }

    #[cfg(unix)]
    #[test]
    fn the_pid_fallback_waits_for_the_process_to_go() {
        let mut child = blocked_child();
        let mut exit = pid_only(&child);
        assert!(exit.is_pid_only());
        assert!(!exit.has_exited().unwrap());
        let started = Instant::now();
        assert!(
            !exit
                .wait_until(started + Duration::from_millis(100))
                .unwrap()
        );
        assert!(started.elapsed() >= Duration::from_millis(100));

        // Without /proc the fallback cannot tell a zombie from a running
        // process, so reap the child where the test does not wait.
        release(&mut child);
        let reaper = std::thread::spawn(move || child.wait().unwrap());
        assert!(
            exit.wait_until(Instant::now() + Duration::from_secs(30))
                .unwrap()
        );
        assert!(exit.has_exited().unwrap());
        assert_eq!(reaper.join().unwrap().code(), Some(3));
    }

    /// Whether the kernel still lists `pid`, as a process or a zombie.
    #[cfg(target_os = "linux")]
    fn listed(pid: u32) -> bool {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    }

    #[test]
    fn an_owned_child_is_waited_for_on_its_exit_event_and_left_unreaped() {
        let mut child = blocked_child();
        let mut exit = ProcessHandle::for_child(&child).unwrap();
        let deadline = Instant::now() + Duration::from_millis(100);
        assert!(!wait_child(|| child.try_wait(), Some(&mut exit), deadline).unwrap());
        assert!(Instant::now() >= deadline);

        release(&mut child);
        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(wait_child(|| child.try_wait(), Some(&mut exit), deadline).unwrap());
        // The event leaves the child for its owner to wait for.
        #[cfg(target_os = "linux")]
        assert!(listed(child.id()));
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    /// Outside Linux a PID check reads an exited, unreaped child as running,
    /// so the wait asks the child and must end promptly everywhere.
    #[cfg(unix)]
    #[test]
    fn an_owned_child_whose_handle_follows_only_its_pid_is_asked_directly() {
        let mut child = blocked_child();
        let mut exit = pid_only(&child);
        release(&mut child);
        let started = Instant::now();
        let deadline = started + Duration::from_secs(30);
        assert!(wait_child(|| child.try_wait(), Some(&mut exit), deadline).unwrap());
        assert!(started.elapsed() < Duration::from_secs(20));
        // Asked through try_wait, the child is reaped and keeps its status.
        #[cfg(target_os = "linux")]
        assert!(!listed(child.id()));
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[test]
    fn an_owned_child_without_a_handle_is_asked_directly() {
        let mut child = blocked_child();
        let deadline = Instant::now() + Duration::from_millis(100);
        assert!(!wait_child(|| child.try_wait(), None, deadline).unwrap());
        assert!(Instant::now() >= deadline);

        release(&mut child);
        let deadline = Instant::now() + Duration::from_secs(30);
        assert!(wait_child(|| child.try_wait(), None, deadline).unwrap());
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn the_async_exit_event_fires_only_once_the_process_exits() {
        let mut child = blocked_child();
        let mut exit = ProcessHandle::for_child(&child).unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exit.exited())
                .await
                .is_err(),
            "the exit future completed while the process was still running"
        );
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        assert!(!exit.exited_until(deadline).await.unwrap());

        release(&mut child);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        assert!(exit.exited_until(deadline).await.unwrap());
        exit.exited().await.unwrap();
        assert!(exit.has_exited().unwrap());
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[cfg(all(unix, feature = "async"))]
    #[tokio::test]
    async fn the_async_pid_fallback_waits_for_the_process_to_go() {
        let mut child = blocked_child();
        let mut exit = pid_only(&child);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), exit.exited())
                .await
                .is_err()
        );
        release(&mut child);
        let reaper = std::thread::spawn(move || child.wait().unwrap());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        assert!(exit.exited_until(deadline).await.unwrap());
        assert_eq!(reaper.join().unwrap().code(), Some(3));
    }
}
