//! Client-side start of a local daemon from a stdio shim.
//!
//! Enable the `launch` feature. A process that only binds and serves keeps
//! `local` and does not compile this module.
//!
//! These are primitives, not a full replacement coordinator. A cache daemon
//! keeps [`crate::replacement`] and an application health probe; a byte-pump
//! shim can use [`spawn_and_wait`] with a connect probe.
//!
//! # Roles
//!
//! The kernel bind is the election: [`crate::local::unix_socket::acquire`] or
//! [`crate::local::windows_socket::acquire`] returns `Won` or `AlreadyRunning`.
//! Clients never unlink the endpoint.
//!
//! An advisory [`crate::ProcessLock`] on a sibling path is layer two. It
//! reduces a thundering herd of client forks. If the lock and the kernel
//! disagree, the kernel is right. Continue to bind or connect.
//!
//! Liveness is the caller's probe (connect or a protocol handshake), never the
//! existence of a discovery file. [`wait_until_live`] is a bool wrapper over
//! [`crate::readiness::wait_until`].
//!
//! [`spawn`] does not change stdio: the caller sets it. [`spawn_detached`]
//! nulls stdin, stdout and stderr for shims whose protocol owns those streams.
//! Both leave the child in the caller's session and, on Windows, let it
//! inherit every inheritable handle other than the caller's standard ones.
//!
//! # Starting a daemon
//!
//! A process meant to outlive its caller needs more: Ctrl-C in the caller's
//! terminal must not stop it, and it must not hold the caller's pipes open.
//! [`DaemonCommand`] starts it in a new session on Unix, and on Windows with a
//! hidden console of its own and an explicit list of the only handles it may
//! inherit. Use it for a long-lived peer; keep [`spawn`] for short-lived
//! children whose lifetime the caller controls.

use std::convert::Infallible;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::readiness;

/// Gap between unsuccessful liveness probes. Same cadence as [`readiness`].
pub const POLL: Duration = readiness::POLL_INTERVAL;

/// Outcome of [`spawn_and_wait`].
///
/// `live` is independent of `spawn_error`: another client may have won the
/// bind after this spawn failed.
#[derive(Debug)]
pub struct SpawnWait {
    /// The caller's probe succeeded before the budget elapsed.
    pub live: bool,
    /// Why this process could not exec the peer. `None` if exec started.
    pub spawn_error: Option<io::Error>,
}

/// Spawn `command` with OS spawn hygiene. Stdio, argv and env stay as set.
///
/// On Unix this restores SIGCHLD around `Command::spawn` so a failed exec
/// still returns an error instead of panicking, then reap-ignores again.
/// On Windows this suppresses inherit on the caller's standard handles for
/// the spawn, then restores them.
pub fn spawn(command: &mut Command) -> io::Result<Child> {
    #[cfg(unix)]
    {
        crate::local::unix::spawn_with_child_waiting(command)
    }
    #[cfg(windows)]
    {
        let _guard = crate::local::windows::StdioInheritGuard::suppress();
        command.spawn()
    }
}

/// Spawn a peer with stdin, stdout and stderr detached.
///
/// Use this when the caller's stdio is a client protocol. Callers that need a
/// daemon log on stderr set that stream themselves and call [`spawn`].
pub fn spawn_detached(command: &mut Command) -> io::Result<Child> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    spawn(command)
}

/// Where a daemon's stdout or stderr goes. Its stdin is always the null device.
#[derive(Debug, Default)]
pub enum DaemonOutput {
    /// Discard the stream.
    #[default]
    Null,
    /// Write the stream to this file, typically a daemon log opened for append.
    File(File),
}

#[derive(Clone, Debug)]
enum EnvChange {
    Set(OsString, OsString),
    Remove(OsString),
}

/// A daemon to start detached from the calling process.
///
/// The child inherits the caller's environment with the changes made here, in
/// order. Pass an absolute program path: on Windows the search for a bare name
/// differs from `Command`'s, and batch files are refused.
///
/// On Unix the child is a new session leader (`setsid`): Ctrl-C and the hangup
/// of the caller's terminal do not reach it. On Windows it gets its own hidden
/// console and a new process group, and inherits only its three standard
/// handles, so no pipe the caller holds stays open because of it.
#[derive(Debug)]
pub struct DaemonCommand {
    program: PathBuf,
    args: Vec<OsString>,
    env: Vec<EnvChange>,
    current_dir: Option<PathBuf>,
    stdout: DaemonOutput,
    stderr: DaemonOutput,
}

impl DaemonCommand {
    /// A daemon running `program`, with no arguments and both outputs discarded.
    pub fn new(program: impl AsRef<Path>) -> Self {
        Self {
            program: program.as_ref().to_path_buf(),
            args: Vec::new(),
            env: Vec::new(),
            current_dir: None,
            stdout: DaemonOutput::Null,
            stderr: DaemonOutput::Null,
        }
    }

    /// Append one argument.
    pub fn arg(&mut self, arg: impl Into<OsString>) -> &mut Self {
        self.args.push(arg.into());
        self
    }

    /// Append several arguments.
    pub fn args<I, S>(&mut self, args: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }

    /// Set a variable in the child's environment.
    pub fn env(&mut self, name: impl Into<OsString>, value: impl Into<OsString>) -> &mut Self {
        self.env.push(EnvChange::Set(name.into(), value.into()));
        self
    }

    /// Remove a variable the child would otherwise inherit.
    pub fn env_remove(&mut self, name: impl Into<OsString>) -> &mut Self {
        self.env.push(EnvChange::Remove(name.into()));
        self
    }

    /// Run the child in `dir` instead of the caller's working directory.
    pub fn current_dir(&mut self, dir: impl AsRef<Path>) -> &mut Self {
        self.current_dir = Some(dir.as_ref().to_path_buf());
        self
    }

    /// Where stdout goes. Discarded by default.
    pub fn stdout(&mut self, target: DaemonOutput) -> &mut Self {
        self.stdout = target;
        self
    }

    /// Where stderr goes. Discarded by default.
    pub fn stderr(&mut self, target: DaemonOutput) -> &mut Self {
        self.stderr = target;
        self
    }

    /// Start the daemon.
    ///
    /// Returns once the OS has created the process. Whether it goes on to
    /// serve is the caller's liveness probe, as with [`spawn_and_wait`].
    pub fn spawn(&self) -> io::Result<DaemonChild> {
        #[cfg(unix)]
        {
            let mut command = Command::new(&self.program);
            command.args(&self.args).stdin(Stdio::null());
            for change in &self.env {
                match change {
                    EnvChange::Set(name, value) => command.env(name, value),
                    EnvChange::Remove(name) => command.env_remove(name),
                };
            }
            if let Some(dir) = &self.current_dir {
                command.current_dir(dir);
            }
            command.stdout(unix_stdio(&self.stdout)?);
            command.stderr(unix_stdio(&self.stderr)?);
            crate::local::unix::spawn_in_new_session(&mut command)
                .map(|inner| DaemonChild { inner })
        }
        #[cfg(windows)]
        {
            use crate::local::command_line::EnvChange as WideChange;
            use crate::local::windows_spawn::{Spawn, Target, spawn};
            use std::os::windows::ffi::OsStrExt;
            let wide = |text: &OsString| text.encode_wide().collect::<Vec<u16>>();
            let env: Vec<WideChange> = self
                .env
                .iter()
                .map(|change| match change {
                    EnvChange::Set(name, value) => WideChange::Set(wide(name), wide(value)),
                    EnvChange::Remove(name) => WideChange::Remove(wide(name)),
                })
                .collect();
            fn target(output: &DaemonOutput) -> Target<'_> {
                match output {
                    DaemonOutput::Null => Target::Null,
                    DaemonOutput::File(file) => Target::File(file),
                }
            }
            spawn(&Spawn {
                program: &self.program,
                args: &self.args,
                env: &env,
                current_dir: self.current_dir.as_deref(),
                stdout: target(&self.stdout),
                stderr: target(&self.stderr),
            })
            .map(|inner| DaemonChild { inner })
        }
    }
}

#[cfg(unix)]
fn unix_stdio(target: &DaemonOutput) -> io::Result<Stdio> {
    Ok(match target {
        DaemonOutput::Null => Stdio::null(),
        DaemonOutput::File(file) => Stdio::from(file.try_clone()?),
    })
}

/// A daemon started by [`DaemonCommand::spawn`].
///
/// Dropping it leaves the daemon running, as dropping a `std::process::Child`
/// does. A caller that never waits should keep SIGCHLD reaping in mind on
/// Unix, as with any child.
#[derive(Debug)]
pub struct DaemonChild {
    #[cfg(unix)]
    inner: Child,
    #[cfg(windows)]
    inner: crate::local::windows_spawn::WindowsChild,
}

impl DaemonChild {
    /// The daemon's process ID.
    pub fn id(&self) -> u32 {
        self.inner.id()
    }

    /// The exit status if the daemon has exited, without blocking.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.inner.try_wait()
    }

    /// Block until the daemon exits.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.inner.wait()
    }

    /// Stop the daemon forcibly: SIGKILL on Unix, `TerminateProcess` on
    /// Windows. A daemon that already exited is not an error.
    pub fn kill(&mut self) -> io::Result<()> {
        self.inner.kill()
    }
}

/// Poll `is_live` until it succeeds or `deadline` is reached.
///
/// `is_live` must attempt a connect or a protocol probe. Do not use
/// `path.exists()`. Application proofs with errors use
/// [`readiness::wait_until`] directly.
pub fn wait_until_live(deadline: Instant, mut is_live: impl FnMut() -> bool) -> bool {
    readiness::wait_until(deadline, |_| Ok::<_, Infallible>(is_live().then_some(())))
        .ok()
        .flatten()
        .is_some()
}

/// Detach-spawn `command`, then wait on `is_live` for `budget`.
///
/// Exec failure is not fatal: another shim may already have started the peer.
/// The caller logs `spawn_error` if it wants a diagnostic.
pub fn spawn_and_wait(
    command: &mut Command,
    budget: Duration,
    is_live: impl FnMut() -> bool,
) -> SpawnWait {
    let spawn_error = spawn_detached(command).err();
    let live = wait_until_live(Instant::now() + budget, is_live);
    SpawnWait { live, spawn_error }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_until_live_fails_when_the_probe_never_succeeds() {
        let started = Instant::now();
        assert!(!wait_until_live(
            Instant::now() + Duration::from_millis(300),
            || false,
        ));
        assert!(started.elapsed() >= Duration::from_millis(250));
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[test]
    fn wait_until_live_returns_as_soon_as_the_probe_succeeds() {
        let mut n = 0;
        assert!(wait_until_live(
            Instant::now() + Duration::from_secs(2),
            || {
                n += 1;
                n >= 2
            }
        ));
        assert!(n >= 2);
    }

    #[test]
    fn a_missing_binary_is_reported_on_spawn_error() {
        let mut command = Command::new("/this/does/not/exist-kunobi-daemon-launch");
        let outcome = spawn_and_wait(&mut command, Duration::from_millis(80), || false);
        assert!(!outcome.live);
        assert!(outcome.spawn_error.is_some());
    }

    #[cfg(unix)]
    #[test]
    fn a_stale_socket_file_is_not_live() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("b.sock");
        {
            let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
            drop(listener);
        }
        assert!(sock.exists());
        let stale_deadline = Instant::now() + Duration::from_secs(1);
        while std::os::unix::net::UnixStream::connect(&sock).is_ok() {
            assert!(
                Instant::now() < stale_deadline,
                "closed listener stayed live"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!wait_until_live(
            Instant::now() + Duration::from_millis(300),
            || std::os::unix::net::UnixStream::connect(&sock).is_ok(),
        ));
    }

    #[cfg(unix)]
    #[test]
    fn spawn_and_wait_still_sees_a_peer_another_client_started() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("b.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).unwrap();
        let mut command = Command::new("/this/does/not/exist-kunobi-daemon-launch");
        let outcome = spawn_and_wait(&mut command, Duration::from_secs(2), || {
            std::os::unix::net::UnixStream::connect(&sock).is_ok()
        });
        assert!(outcome.live);
        assert!(outcome.spawn_error.is_some());
        drop(listener);
    }

    #[cfg(unix)]
    #[test]
    fn spawn_and_wait_waits_for_a_peer_that_binds_later() {
        let dir = tempfile::tempdir().unwrap();
        let sock = dir.path().join("b.sock");
        let delayed_socket = sock.clone();
        let (stop_tx, stop_rx) = std::sync::mpsc::channel();
        let delayed = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(250));
            let listener = std::os::unix::net::UnixListener::bind(delayed_socket).unwrap();
            let _ = stop_rx.recv_timeout(Duration::from_secs(2));
            listener
        });
        let mut command = Command::new("/this/does/not/exist-kunobi-daemon-launch");
        let outcome = spawn_and_wait(&mut command, Duration::from_secs(2), || {
            std::os::unix::net::UnixStream::connect(&sock).is_ok()
        });
        stop_tx.send(()).unwrap();
        drop(delayed.join().unwrap());
        assert!(outcome.live);
    }

    #[cfg(unix)]
    fn sleeper() -> DaemonChild {
        let mut command = DaemonCommand::new("/bin/sleep");
        command.arg("30");
        command.spawn().unwrap()
    }

    /// Run by [`a_daemon_survives_sigint_to_its_callers_process_group`] as the
    /// caller whose group receives SIGINT. Does nothing in a normal test run.
    #[cfg(unix)]
    #[test]
    #[ignore = "helper process for the SIGINT test"]
    fn detach_sigint_helper() {
        let Some(out) = std::env::var_os("KDAEMON_DETACH_HELPER_OUT") else {
            return;
        };
        let child = sleeper();
        std::fs::write(&out, child.id().to_string()).unwrap();
        // Stay alive until the signal ends this process.
        std::thread::sleep(Duration::from_secs(30));
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_survives_sigint_to_its_callers_process_group() {
        use std::os::unix::process::CommandExt;
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("pid");
        // The helper leads a process group of its own, standing in for the
        // terminal's foreground group that Ctrl-C signals.
        let mut helper = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "launch::tests::detach_sigint_helper",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("KDAEMON_DETACH_HELPER_OUT", &out)
            .stdout(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        let daemon: u32 = loop {
            if let Ok(pid) = std::fs::read_to_string(&out)
                && let Ok(pid) = pid.trim().parse()
            {
                break pid;
            }
            assert!(Instant::now() < deadline, "helper never started the daemon");
            std::thread::sleep(Duration::from_millis(20));
        };
        crate::local::unix::interrupt_process_group(helper.id()).unwrap();
        let status = helper.wait().unwrap();
        // A process SIGINT killed can still answer `kill(pid, 0)` as a zombie
        // until it is reaped, so one look right away proves nothing. Watch it
        // for a while: a daemon that shared the group turns up exited.
        let settle = Instant::now() + Duration::from_secs(2);
        let mut daemon_state = crate::local::process_state(daemon);
        while daemon_state == crate::local::ProcessState::Alive && Instant::now() < settle {
            std::thread::sleep(Duration::from_millis(20));
            daemon_state = crate::local::process_state(daemon);
        }
        let _ = Command::new("kill")
            .args(["-KILL", &daemon.to_string()])
            .status();
        assert!(!status.success(), "SIGINT did not reach the helper's group");
        assert_eq!(
            daemon_state,
            crate::local::ProcessState::Alive,
            "the daemon died with its caller's process group"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_gets_its_output_file_env_and_directory() {
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("daemon.log");
        let file = File::create(&log).unwrap();
        // Cargo sets CARGO_MANIFEST_DIR for the test process and no shell
        // invents it, so its absence in the child proves the removal.
        assert!(std::env::var_os("CARGO_MANIFEST_DIR").is_some());
        let mut command = DaemonCommand::new("/bin/sh");
        command
            .args([
                "-c",
                "echo \"$KDAEMON_SET|${CARGO_MANIFEST_DIR:-unset}|$(pwd)\" >&2",
            ])
            .env("KDAEMON_SET", "set-by-builder")
            .env_remove("CARGO_MANIFEST_DIR")
            .current_dir(dir.path())
            .stderr(DaemonOutput::File(file));
        let mut child = command.spawn().unwrap();
        assert!(child.wait().unwrap().success());
        let written = std::fs::read_to_string(&log).unwrap();
        let expected_dir = dir.path().canonicalize().unwrap();
        assert_eq!(
            written.trim(),
            format!("set-by-builder|unset|{}", expected_dir.display())
        );
    }

    #[test]
    fn a_missing_daemon_binary_is_a_spawn_error() {
        let error = DaemonCommand::new("/this/does/not/exist-kunobi-daemon")
            .spawn()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[cfg(unix)]
    #[test]
    fn spawn_detached_execs() {
        let bin = ["/usr/bin/true", "/bin/true"]
            .into_iter()
            .find(|path| std::path::Path::new(path).exists());
        let Some(bin) = bin else {
            return;
        };
        let mut command = Command::new(bin);
        let mut child = spawn_detached(&mut command).unwrap();
        assert!(child.wait().unwrap().success());
    }
}
