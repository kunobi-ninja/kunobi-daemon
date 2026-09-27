//! Prefault a client shim so the next exec of the same path is not cold.
//!
//! The daemon cannot keep a pre-spawned shim: stdin and stdout belong to the
//! process that launched the client. What it can do is fault the file into the
//! OS page cache, and (on macOS) populate the per-path first-exec cache by
//! spawning a child that exits immediately.
//!
//! Call [`warm_executable`] at daemon start and after a replacement commit.
//! [`warm_spawn`] is for an argv the child understands as "exit without
//! talking to the daemon" (for example `--warmup`). Neither function runs on a
//! timer: a periodic exec fights idle-wakeup budgets, and the expensive miss is
//! a new path after replace, not eviction of a few hundred kilobytes.

use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read};
use std::path::Path;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

/// Bytes read per iteration. Matches a typical file-system block multiple and
/// the transport window; the buffer is reused so RSS stays at this size.
const WINDOW: usize = 65_536;

/// How long [`warm_spawn`] waits for the child to exit.
pub const SPAWN_BUDGET: Duration = Duration::from_secs(2);

/// How long a warmup child may run before it is killed.
///
/// Much longer than [`SPAWN_BUDGET`] because macOS checks a new executable on
/// its first run, and that check is what warming pays for. It took 0.2-2.3s
/// in tests, and can take longer. A child still running when its caller stops
/// waiting is left to finish, so the next exec of the path is warm.
pub const RUNAWAY_LIMIT: Duration = Duration::from_secs(60);

/// Read `path` into the OS file cache without executing it.
///
/// The file length is taken from metadata so special files with no end (such as
/// `/dev/zero`) cannot hang the caller. Missing, unreadable or directory paths
/// return an error. Replacement treats those errors as non-fatal.
pub fn warm_executable(path: &Path) -> io::Result<()> {
    let mut file = File::open(path)?;
    let metadata = file.metadata()?;
    if metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::IsADirectory,
            "warmup path is a directory",
        ));
    }
    let mut remaining = metadata.len();
    let mut buffer = vec![0u8; WINDOW];
    while remaining > 0 {
        let want = WINDOW.min(remaining as usize);
        let got = file.read(&mut buffer[..want])?;
        if got == 0 {
            break;
        }
        remaining -= got as u64;
    }
    Ok(())
}

/// Spawn `path` with `args` and wait up to [`SPAWN_BUDGET`] for a successful
/// exit.
///
/// Stdio is detached. The child must exit without connecting to a daemon or
/// taking its locks. A child still running at the budget is not killed:
/// this returns `TimedOut` and leaves it to finish in the background, up to
/// [`RUNAWAY_LIMIT`]. On Linux, a spawn refused because the file is still open
/// for writing somewhere (`ETXTBSY`) is retried within the same limit.
pub fn warm_spawn(path: &Path, args: &[impl AsRef<OsStr>]) -> io::Result<()> {
    warm_spawn_until(path, args, Instant::now() + SPAWN_BUDGET)
}

/// As [`warm_spawn`], waiting until `deadline` instead of [`SPAWN_BUDGET`].
///
/// The child is killed at [`RUNAWAY_LIMIT`] after the spawn, or at `deadline`
/// if that is later.
pub fn warm_spawn_until(
    path: &Path,
    args: &[impl AsRef<OsStr>],
    deadline: Instant,
) -> io::Result<()> {
    let exited = start(path, args, deadline, RUNAWAY_LIMIT)?;
    match exited.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(io::Error::other(format!(
            "warmup {} exited {status}",
            path.display()
        ))),
        Ok(Err(error)) => Err(error),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "warmup {} is still running after its budget; left to finish",
                path.display()
            ),
        )),
    }
}

/// Spawn and reap the child on its own thread, which reports how it exited.
/// The caller may stop listening at any time; the child is still reaped. It is
/// killed `runaway` after it was spawned, or at `deadline` if that is later.
fn start(
    path: &Path,
    args: &[impl AsRef<OsStr>],
    deadline: Instant,
    runaway: Duration,
) -> io::Result<mpsc::Receiver<io::Result<ExitStatus>>> {
    let mut command = Command::new(path);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let (report, exited) = mpsc::channel();
    // The spawn runs on the reaper thread too: on macOS the first-run check
    // can make the spawn itself slow, and the caller's budget covers it.
    std::thread::Builder::new()
        .name("kunobi-daemon-warmup".into())
        .spawn(move || {
            // The caller may stop waiting sooner; the child still gets to run.
            let busy_until = deadline.max(Instant::now() + runaway);
            let result = spawn_when_free(&mut command, busy_until, || {}).and_then(|child| {
                let limit = deadline.max(Instant::now() + runaway);
                reap(child, limit)
            });
            let _ = report.send(result);
        })?;
    Ok(exited)
}

/// Spawn `command`, retrying while its executable is busy until `until`.
/// `on_busy` runs after each refused attempt.
///
/// Linux refuses to exec a file that any process holds open for writing
/// (`ETXTBSY`). Warmup runs right after a new executable is written, and in a
/// multi-threaded process a child forked by another thread holds a copy of
/// the writer's descriptor until that child execs. The window is short.
fn spawn_when_free(
    command: &mut Command,
    until: Instant,
    mut on_busy: impl FnMut(),
) -> io::Result<Child> {
    let mut pause = Duration::from_millis(1);
    loop {
        match command.spawn() {
            Err(error)
                if error.kind() == io::ErrorKind::ExecutableFileBusy && Instant::now() < until =>
            {
                on_busy();
                std::thread::sleep(pause.min(until.saturating_duration_since(Instant::now())));
                pause = (pause * 2).min(Duration::from_millis(50));
            }
            result => return result,
        }
    }
}

/// Wait for `child` to exit, killing it at `limit`. The standard library has
/// no timed wait for a child, so this checks with a backoff capped at 50ms.
/// It runs off the caller's thread, which waits on a channel instead.
fn reap(mut child: Child, limit: Instant) -> io::Result<ExitStatus> {
    let mut pause = Duration::from_millis(1);
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        let left = limit.saturating_duration_since(Instant::now());
        if left.is_zero() {
            let _ = child.kill();
            child.wait()?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "warmup child killed at its runaway limit",
            ));
        }
        std::thread::sleep(pause.min(left));
        pause = (pause * 2).min(Duration::from_millis(50));
    }
}

pub(crate) fn prefault_all(paths: impl IntoIterator<Item = impl AsRef<Path>>) {
    for path in paths {
        let _ = warm_executable(path.as_ref());
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn a_runaway_child_is_killed_at_its_limit() {
        let runaway = Duration::from_millis(50);
        let exited = start(Path::new("/bin/sleep"), &["30"], Instant::now(), runaway).unwrap();
        let error = exited.recv().unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_busy_executable_is_retried_until_it_is_free() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let shim = dir.path().join("shim.sh");
        let mut writer = File::create(&shim).unwrap();
        writer.write_all(b"#!/bin/sh\nexit 0\n").unwrap();
        std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();

        // While this process holds the file open for writing, exec fails.
        let mut command = Command::new(&shim);
        let busy = spawn_when_free(&mut command, Instant::now(), || {}).unwrap_err();
        assert_eq!(busy.kind(), io::ErrorKind::ExecutableFileBusy);

        // The writer closes only after a retrying spawn was refused, so the
        // retry is what succeeds.
        let (refused, first_refusal) = mpsc::channel();
        let retrying = std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(30);
            spawn_when_free(&mut command, deadline, || {
                let _ = refused.send(());
            })
            .and_then(|mut child| child.wait())
        });
        first_refusal.recv().unwrap();
        drop(writer);
        assert!(retrying.join().unwrap().unwrap().success());
    }
}
