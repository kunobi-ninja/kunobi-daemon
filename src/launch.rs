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
//! The kernel bind is the election: `local::unix_socket::acquire` on Unix or
//! `local::windows_socket::acquire` on Windows returns `Won` or
//! `AlreadyRunning`. Each module exists only on its own platform, so these are
//! not links. Clients never unlink the endpoint.
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
//!
//! [`DaemonChild::wait_until_live`] probes it until it serves, and stops early
//! when it exits first: the OS reports the exit as an event, so a daemon that
//! fails at startup does not cost the caller its whole budget.
//!
//! # Knowing when it is ready
//!
//! [`DaemonCommand::readiness_channel`] gives the daemon a channel to say when
//! it is ready and to report progress on the way (see
//! [`crate::readiness::channel`]). A daemon that dies during startup is then
//! noticed at once, and the launcher's deadline becomes the longest silence it
//! accepts instead of a total. The daemon's word is not a proof: probe it
//! before using it.

use std::convert::Infallible;
use std::ffi::OsString;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, Instant};

use crate::local::ProcessHandle;
use crate::readiness;
use crate::readiness::channel::{Channel, ENV};

/// Gap between unsuccessful liveness probes. Same cadence as [`readiness`].
pub const POLL: Duration = readiness::POLL_INTERVAL;

/// Outcome of [`spawn_and_wait`].
///
/// `live` is independent of `spawn_error`: another client may have won the
/// bind after this spawn failed.
#[derive(Debug)]
#[non_exhaustive]
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
        let _spawn = crate::spawn_lock::spawning();
        let _guard = crate::local::windows::StdioInheritGuard::suppress()?;
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
/// of the caller's terminal do not reach it. Every descriptor above stderr is
/// closed across the exec, so it starts with only its standard streams.
///
/// On Windows it gets its own hidden console and a new process group, and
/// inherits only its three standard handles, so no pipe the caller holds stays
/// open because of it. It is also taken out of the caller's job object when
/// that job allows it.
///
/// A [readiness channel](DaemonCommand::readiness_channel) is the one
/// exception on Unix: its descriptor also survives the exec. On Windows the
/// daemon opens that channel by name and inherits nothing for it.
///
/// # Job objects on Windows
///
/// A job can forbid its processes' children from leaving it, and cargo's does:
/// cargo puts itself and every child in a job that kills them all when cargo
/// is interrupted, without allowing breakaway. A daemon started under such a
/// job stays in it and dies with the caller on Ctrl-C, whatever this builder
/// does. [`DaemonChild::in_callers_job`] reports that case so the caller can
/// say so. The only way out is to start the daemon from outside the job, for
/// example through a scheduled task or a service.
#[derive(Debug)]
pub struct DaemonCommand {
    program: PathBuf,
    args: Vec<OsString>,
    env: Vec<EnvChange>,
    current_dir: Option<PathBuf>,
    stdout: DaemonOutput,
    stderr: DaemonOutput,
    readiness: bool,
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
            readiness: false,
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
    ///
    /// On Windows, where names are case-insensitive, a later change to `path`
    /// replaces `Path`. That match folds ASCII letters only.
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

    /// Give the daemon a readiness channel; [`DaemonChild::take_readiness`]
    /// returns the launcher's end.
    ///
    /// The daemon takes its end with
    /// [`Notifier::from_env`](crate::readiness::channel::Notifier::from_env),
    /// which finds it through the [`ENV`] variable. Without this call the
    /// daemon gets no channel, and [`ENV`] is removed from its environment in
    /// case the caller's own has it.
    ///
    /// On Unix the channel is a pipe. On macOS the standard library creates a
    /// pipe and then marks it close-on-exec, so a child that another thread
    /// spawns in between, other than through this crate, can inherit it and
    /// hide the daemon's exit. The crate's own spawns wait while the pipe is
    /// created, as they do for a listener; see `local::unix_socket::acquire`
    /// for what other spawns need. On Windows it is a named pipe the
    /// daemon opens by name, and the launcher also watches the daemon's
    /// process, so a daemon that exits before it connects is reported at once.
    ///
    /// ```no_run
    /// use kunobi_daemon::launch::DaemonCommand;
    /// use std::time::Duration;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut daemon = DaemonCommand::new("/usr/local/bin/exampled")
    ///     .readiness_channel()
    ///     .spawn()?;
    /// let mut channel = daemon.take_readiness().expect("asked for above");
    /// // Up to ten seconds between messages, however long the start takes.
    /// if let Err(reason) = channel.wait(Duration::from_secs(10)) {
    ///     let doing = channel.last_progress().unwrap_or_default();
    ///     return Err(format!("{reason} (last progress: {doing:?})").into());
    /// }
    /// // Ready is the daemon's word: probe its endpoint before relying on it.
    /// # Ok(())
    /// # }
    /// ```
    pub fn readiness_channel(&mut self) -> &mut Self {
        self.readiness = true;
        self
    }

    /// Start the daemon.
    ///
    /// Returns once the OS has created the process. Whether it goes on to
    /// serve is the caller's liveness probe, as with [`spawn_and_wait`].
    pub fn spawn(&self) -> io::Result<DaemonChild> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
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
            let readiness = if self.readiness {
                let (reader, child_end) = crate::local::unix::ready_pipe()?;
                command.env(ENV, child_end.as_raw_fd().to_string());
                Some((Channel::start(reader)?, child_end))
            } else {
                command.env_remove(ENV);
                None
            };
            let keep = readiness
                .as_ref()
                .map(|(_, child_end)| child_end.as_raw_fd());
            let (inner, exit) =
                crate::local::unix::spawn_in_new_session_watched(&mut command, keep)?;
            // Dropping this copy of the daemon's end leaves the daemon the
            // only writer, so its exit ends the channel.
            Ok(DaemonChild {
                inner,
                exit,
                readiness: readiness.map(|(channel, _child_end)| channel),
            })
        }
        #[cfg(windows)]
        {
            use crate::local::command_line::EnvChange as WideChange;
            use crate::local::windows::ready;
            use crate::local::windows_spawn::{Spawn, Target, spawn};
            use std::os::windows::ffi::OsStrExt;
            let wide = |text: &OsString| text.encode_wide().collect::<Vec<u16>>();
            let mut env: Vec<WideChange> = self
                .env
                .iter()
                .map(|change| match change {
                    EnvChange::Set(name, value) => WideChange::Set(wide(name), wide(value)),
                    EnvChange::Remove(name) => WideChange::Remove(wide(name)),
                })
                .collect();
            let name: Vec<u16> = ENV.encode_utf16().collect();
            // The daemon opens the channel by name, so it inherits no handle
            // for it and neither can any other child of this process.
            let readiness = if self.readiness {
                let (pipe_name, pipe) = ready::create()?;
                env.push(WideChange::Set(name, pipe_name.encode_utf16().collect()));
                let (started, process) = std::sync::mpsc::channel();
                let channel = Channel::start(ready::Reader::new(pipe, process))?;
                Some((channel, started))
            } else {
                env.push(WideChange::Remove(name));
                None
            };
            fn target(output: &DaemonOutput) -> Target<'_> {
                match output {
                    DaemonOutput::Null => Target::Null,
                    DaemonOutput::File(file) => Target::File(file),
                }
            }
            let mut inner = spawn(&Spawn {
                program: &self.program,
                args: &self.args,
                env: &env,
                current_dir: self.current_dir.as_deref(),
                stdout: target(&self.stdout),
                stderr: target(&self.stderr),
            })?;
            let handles = (|| {
                let exit = ProcessHandle::open(
                    crate::ProcessId::new(inner.id()).expect("spawned daemon has a nonzero PID"),
                )?;
                // Before the daemon connects, its exit is the only event
                // that can end the channel. Clone its owned process handle.
                let readiness = match readiness {
                    Some((channel, started)) => {
                        let _ = started.send(inner.watch()?);
                        Some(channel)
                    }
                    None => None,
                };
                Ok((exit, readiness))
            })();
            match handles {
                Ok((exit, readiness)) => Ok(DaemonChild {
                    inner,
                    exit,
                    readiness,
                }),
                Err(error) => {
                    let _ = inner.kill();
                    Err(error)
                }
            }
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
///
/// Its [`ProcessHandle`] is opened at spawn, before anything can reap the
/// daemon, so it follows the daemon even in a process that ignores SIGCHLD.
#[derive(Debug)]
pub struct DaemonChild {
    #[cfg(unix)]
    inner: Child,
    #[cfg(windows)]
    inner: crate::local::windows_spawn::WindowsChild,
    exit: ProcessHandle,
    readiness: Option<Channel>,
}

/// How [`DaemonChild::wait_until_live`] ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Startup {
    /// A probe succeeded.
    Live,
    /// The daemon exited, and a probe made after its exit failed too. A
    /// daemon that lost the bind election to another one exits as well, and
    /// the probe after its exit then finds the winner, so this means no
    /// daemon answered.
    Exited,
    /// The deadline passed with the daemon still running and no probe
    /// successful.
    TimedOut,
}

impl DaemonChild {
    /// The daemon's process ID.
    pub fn id(&self) -> crate::ProcessId {
        crate::ProcessId::new(self.inner.id()).expect("spawned daemon has a nonzero PID")
    }

    /// The launcher's end of the readiness channel, the first time it is
    /// asked for. `None` without [`DaemonCommand::readiness_channel`].
    pub fn take_readiness(&mut self) -> Option<Channel> {
        self.readiness.take()
    }

    /// True when the daemon was left in the caller's job object because that
    /// job forbids breakaway, so it will die with the caller's job (see the
    /// job objects section of [`DaemonCommand`]). Always false on Unix, which
    /// has no job objects.
    ///
    /// False does not prove the daemon is in no job. With nested jobs, a job
    /// that allows breakaway lets the daemon leave it even when an enclosing
    /// job does not; the daemon then stays in that outer job without the
    /// breakaway being refused.
    pub fn in_callers_job(&self) -> bool {
        #[cfg(unix)]
        {
            false
        }
        #[cfg(windows)]
        {
            self.inner.in_callers_job()
        }
    }

    /// The exit status if the daemon has exited, without blocking.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.inner.try_wait()
    }

    /// Block until the daemon exits.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.inner.wait()
    }

    /// Block until the daemon exits or `deadline` passes: its exit status,
    /// or `None` while it still runs.
    ///
    /// The status comes from waiting for the child, so in a process that
    /// ignores SIGCHLD, where the kernel discards it, this fails after the
    /// exit. [`Self::exit_handle`] still reports the exit there.
    pub fn wait_until(&mut self, deadline: Instant) -> io::Result<Option<ExitStatus>> {
        if self.exits_by(deadline)? {
            self.inner.wait().map(Some)
        } else {
            Ok(None)
        }
    }

    /// Whether the daemon exits by `deadline`. Where the handle follows only
    /// the PID, this asks the child instead: see
    /// [`crate::local::ProcessHandle`]'s fallback.
    fn exits_by(&mut self, deadline: Instant) -> io::Result<bool> {
        crate::local::wait_child(|| self.inner.try_wait(), Some(&mut self.exit), deadline)
    }

    /// The daemon's exit as an event, for a caller that waits on it together
    /// with other work, for example with `exited` under the `async` feature.
    pub fn exit_handle(&mut self) -> &mut ProcessHandle {
        &mut self.exit
    }

    /// Probe with `is_live` until it succeeds, the daemon exits, or
    /// `deadline` passes.
    ///
    /// Between probes this waits on the daemon's exit rather than sleeping,
    /// for [`POLL`] at most, so a daemon that dies during startup ends the
    /// wait at once. `is_live` must attempt a connect or a protocol probe and
    /// bound its own blocking. An error means the exit could not be observed;
    /// the daemon may still be running.
    pub fn wait_until_live(
        &mut self,
        deadline: Instant,
        mut is_live: impl FnMut() -> bool,
    ) -> io::Result<Startup> {
        loop {
            if is_live() {
                return Ok(Startup::Live);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(Startup::TimedOut);
            }
            if self.exits_by(deadline.min(now + POLL))? {
                return Ok(if is_live() {
                    Startup::Live
                } else {
                    Startup::Exited
                });
            }
        }
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
/// The caller logs `spawn_error` if it wants a diagnostic. For the same
/// reason the wait does not end when the spawned child exits: the peer
/// another shim started may still be coming up. A caller that owns the only
/// start uses [`DaemonCommand`] and [`DaemonChild::wait_until_live`].
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
            #[expect(
                clippy::disallowed_methods,
                reason = "Bounded test probe waits for the closed Unix listener to stop accepting."
            )]
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
            #[expect(
                clippy::disallowed_methods,
                reason = "The fixture binds after startup to exercise retrying a failed liveness probe."
            )]
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
        #[expect(
            clippy::disallowed_methods,
            reason = "The signal-test helper must stay alive until the parent sends SIGINT."
        )]
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
            #[expect(
                clippy::disallowed_methods,
                reason = "The cross-process fixture reports its daemon PID through a file, with a deadline."
            )]
            std::thread::sleep(Duration::from_millis(20));
        };
        // Opened while the daemon certainly runs, so it follows that process.
        let mut daemon_exit = ProcessHandle::open(crate::ProcessId::new(daemon).unwrap()).unwrap();
        crate::local::unix::interrupt_process_group(helper.id()).unwrap();
        let status = helper.wait().unwrap();
        // A daemon that shared the group dies of the same SIGINT; give it
        // time to.
        let died = daemon_exit
            .wait_until(Instant::now() + Duration::from_secs(2))
            .unwrap();
        let _ = Command::new("kill")
            .args(["-KILL", &daemon.to_string()])
            .status();
        assert!(!status.success(), "SIGINT did not reach the helper's group");
        assert!(!died, "the daemon died with its caller's process group");
    }

    /// True if every write end is gone within `timeout`: a read then ends.
    #[cfg(unix)]
    fn reaches_eof_within(read: File, timeout: Duration) -> bool {
        use std::io::Read;
        let (done, finished) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let mut read = read;
            let mut buffer = [0u8; 64];
            while matches!(read.read(&mut buffer), Ok(n) if n > 0) {}
            let _ = done.send(());
        });
        finished.recv_timeout(timeout).is_ok()
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_does_not_keep_the_callers_pipe_open() {
        let (read, write) = crate::local::unix::inheritable_pipe();
        let mut child = sleeper();
        drop(write);
        let eof = reaches_eof_within(read, Duration::from_secs(5));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(eof, "the daemon kept the caller's pipe open");
    }

    /// The control for the test above: a plain spawn does keep the pipe open,
    /// so a pass there is the descriptor sweep working.
    #[cfg(unix)]
    #[test]
    fn a_plain_spawn_would_keep_the_callers_pipe_open() {
        let (read, write) = crate::local::unix::inheritable_pipe();
        let mut child = Command::new("/bin/sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        drop(write);
        let eof = reaches_eof_within(read, Duration::from_secs(1));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(!eof, "a plain spawn is expected to inherit the write end");
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

    /// A FIFO that holds a daemon from [`held_daemon`] until released.
    #[cfg(unix)]
    struct Held {
        fifo: PathBuf,
        _dir: tempfile::TempDir,
    }

    #[cfg(unix)]
    impl Held {
        /// Let the daemon exit. Blocks until it has opened the FIFO.
        fn release(&self) {
            std::fs::write(&self.fifo, b"go\n").unwrap();
        }
    }

    #[cfg(unix)]
    impl Drop for Held {
        /// Release a daemon a failed test left waiting, without blocking.
        fn drop(&mut self) {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            if let Ok(mut fifo) = std::fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(&self.fifo)
            {
                let _ = fifo.write_all(b"go\n");
            }
        }
    }

    /// A daemon that runs until [`Held::release`], then exits with code 3.
    /// Opening a FIFO to read blocks until a writer opens it, so nothing but
    /// the release lets it finish.
    #[cfg(unix)]
    fn held_daemon() -> (DaemonChild, Held) {
        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("release");
        assert!(
            Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .unwrap()
                .success()
        );
        let mut command = DaemonCommand::new("/bin/sh");
        command
            .args(["-c", "read line < \"$1\"; exit 3", "sh"])
            .arg(&fifo);
        (command.spawn().unwrap(), Held { fifo, _dir: dir })
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_that_exits_before_it_is_live_ends_the_wait() {
        let (mut child, held) = held_daemon();
        held.release();
        let started = Instant::now();
        let outcome = child
            .wait_until_live(started + Duration::from_secs(60), || false)
            .unwrap();
        assert_eq!(outcome, Startup::Exited);
        assert!(started.elapsed() < Duration::from_secs(30));
        assert_eq!(child.wait().unwrap().code(), Some(3));
    }

    #[cfg(unix)]
    #[test]
    fn a_live_daemon_is_reported_as_soon_as_a_probe_succeeds() {
        let (mut child, held) = held_daemon();
        let mut probes = 0;
        let started = Instant::now();
        let outcome = child
            .wait_until_live(started + Duration::from_secs(60), || {
                probes += 1;
                probes == 3
            })
            .unwrap();
        assert_eq!(outcome, Startup::Live);
        assert_eq!(probes, 3);
        assert!(started.elapsed() < Duration::from_secs(30));

        assert!(child.wait_until(Instant::now()).unwrap().is_none());
        held.release();
        let status = child
            .wait_until(Instant::now() + Duration::from_secs(30))
            .unwrap()
            .expect("the daemon exited once released");
        assert_eq!(status.code(), Some(3));
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_that_never_becomes_live_times_out_without_busy_probing() {
        let (mut child, held) = held_daemon();
        let mut probes = 0;
        let deadline = Instant::now() + Duration::from_millis(200);
        let outcome = child
            .wait_until_live(deadline, || {
                probes += 1;
                false
            })
            .unwrap();
        assert_eq!(outcome, Startup::TimedOut);
        assert!(Instant::now() >= deadline);
        // The exit wait spaces the probes by up to POLL.
        assert!(probes <= 20, "{probes} probes in 200 ms");
        held.release();
        child.wait().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_that_exits_while_another_serves_is_live() {
        // A daemon that lost the bind election exits, and the probe made
        // after its exit reaches the one that won.
        let (mut child, held) = held_daemon();
        held.release();
        child.exit_handle().wait().unwrap();
        assert_eq!(child.exit_handle().pid(), child.id());
        let mut probes = 0;
        let outcome = child
            .wait_until_live(Instant::now() + Duration::from_secs(60), || {
                probes += 1;
                probes == 2
            })
            .unwrap();
        assert_eq!(outcome, Startup::Live);
        assert_eq!(child.wait().unwrap().code(), Some(3));
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

    // ── Readiness channel ───────────────────────────────────────────

    use crate::readiness::channel::{NotReady, Notifier};

    /// Long enough never to pass in a test that expects an answer first.
    const SILENCE: Duration = Duration::from_secs(60);

    fn progress_then_ready(mut notifier: Notifier) -> io::Result<()> {
        notifier.progress("")?;
        notifier.progress("loading")?;
        notifier.ready()
    }

    /// Run by the readiness tests as the daemon, which reports what it found
    /// in the file the test names. Does nothing in a normal test run.
    #[test]
    #[ignore = "helper process for the readiness tests"]
    #[allow(unsafe_code)]
    fn readiness_helper() {
        let (Some(mode), Some(out)) = (
            std::env::var_os("KDAEMON_READY_HELPER"),
            std::env::var_os("KDAEMON_READY_HELPER_OUT"),
        ) else {
            return;
        };
        // SAFETY: this process runs this one test; libtest's main thread only
        // waits for it and does not touch the environment. The launcher that
        // set the variable is the test that started this process.
        let taken = unsafe { Notifier::from_env() };
        // Taking the channel removes the variable, so nothing else sees it.
        let left = std::env::var_os(ENV).is_some();
        // SAFETY: as above.
        let again = unsafe { Notifier::from_env() };
        let report = match (mode.to_str(), taken) {
            _ if left => "the variable was left in the environment".to_owned(),
            _ if !matches!(again, Ok(None)) => format!("taken twice: {again:?}"),
            (Some("progress-then-ready"), Ok(Some(notifier))) => {
                match progress_then_ready(notifier) {
                    Ok(()) => "sent".to_owned(),
                    Err(error) => error.to_string(),
                }
            }
            (Some("exit-before-ready"), Ok(Some(_notifier))) => "exiting".to_owned(),
            (Some("no-channel"), Ok(None)) => "no channel".to_owned(),
            (mode, taken) => format!("unexpected: {mode:?} {taken:?}"),
        };
        std::fs::write(out, report).unwrap();
    }

    /// This test binary as a daemon running [`readiness_helper`] in `mode`.
    fn readiness_helper_daemon(mode: &str, out: &Path) -> DaemonCommand {
        let mut command = DaemonCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "launch::tests::readiness_helper",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("KDAEMON_READY_HELPER", mode)
            .env("KDAEMON_READY_HELPER_OUT", out.as_os_str());
        command
    }

    /// A daemon that runs for 30 seconds unless it is killed, and says nothing.
    fn silent_daemon() -> DaemonCommand {
        #[cfg(unix)]
        {
            let mut command = DaemonCommand::new("/bin/sleep");
            command.arg("30");
            command
        }
        #[cfg(windows)]
        {
            let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
            let mut command =
                DaemonCommand::new(PathBuf::from(root).join("System32").join("PING.EXE"));
            command.args(["-n", "30", "127.0.0.1"]);
            command
        }
    }

    #[test]
    fn a_daemon_reports_progress_then_ready() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("report");
        let mut child = readiness_helper_daemon("progress-then-ready", &out)
            .readiness_channel()
            .spawn()
            .unwrap();
        let mut channel = child.take_readiness().expect("the channel was asked for");
        assert!(
            child.take_readiness().is_none(),
            "the channel is taken once"
        );
        assert_eq!(channel.wait(SILENCE), Ok(()));
        assert_eq!(channel.progress_reports(), 2);
        assert_eq!(channel.last_progress().as_deref(), Some("loading"));
        assert!(child.wait().unwrap().success());
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "sent");
    }

    #[test]
    fn a_daemon_that_exits_before_ready_is_noticed_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("report");
        let mut child = readiness_helper_daemon("exit-before-ready", &out)
            .readiness_channel()
            .spawn()
            .unwrap();
        let mut channel = child.take_readiness().unwrap();
        // Both observers follow the same daemon: opening its exit handle
        // must neither lose the readiness descriptor nor keep it alive.
        assert!(
            child
                .exit_handle()
                .wait_until(Instant::now() + SILENCE)
                .unwrap()
        );
        assert_eq!(channel.wait(SILENCE), Err(NotReady::Died));
        assert_eq!(
            child
                .wait_until_live(Instant::now() + SILENCE, || false)
                .unwrap(),
            Startup::Exited,
        );
        child.wait().unwrap();
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "exiting");
    }

    #[test]
    fn a_killed_daemon_ends_its_channel() {
        let mut child = silent_daemon().readiness_channel().spawn().unwrap();
        let mut channel = child.take_readiness().unwrap();
        // The daemon holds its end while it runs.
        assert_eq!(channel.wait(Duration::ZERO), Err(NotReady::Silent));
        child.kill().unwrap();
        child.wait().unwrap();
        // The launcher kept no copy of the daemon's end, so the kill ends it.
        assert_eq!(channel.wait(SILENCE), Err(NotReady::Died));
    }

    #[test]
    fn without_opting_in_the_daemon_gets_no_channel() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("report");
        let mut command = readiness_helper_daemon("no-channel", &out);
        // A value in the caller's own environment never reaches the daemon.
        command.env(ENV, "5");
        let mut child = command.spawn().unwrap();
        assert!(child.take_readiness().is_none());
        assert!(child.wait().unwrap().success());
        assert_eq!(std::fs::read_to_string(&out).unwrap(), "no channel");
    }

    #[cfg(unix)]
    #[test]
    fn a_daemon_with_a_readiness_channel_still_does_not_keep_the_callers_pipe_open() {
        let (read, write) = crate::local::unix::inheritable_pipe();
        let mut command = DaemonCommand::new("/bin/sleep");
        command.arg("30").readiness_channel();
        let mut child = command.spawn().unwrap();
        drop(write);
        let eof = reaches_eof_within(read, Duration::from_secs(5));
        child.kill().unwrap();
        child.wait().unwrap();
        assert!(eof, "the channel's exception let the caller's pipe through");
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn a_daemon_s_readiness_can_be_awaited() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("report");
        let mut child = readiness_helper_daemon("progress-then-ready", &out)
            .readiness_channel()
            .spawn()
            .unwrap();
        let mut channel = child.take_readiness().unwrap();
        assert_eq!(channel.wait_async(SILENCE).await, Ok(()));
        assert_eq!(channel.progress_reports(), 2);
        assert!(child.wait().unwrap().success());
    }
}
