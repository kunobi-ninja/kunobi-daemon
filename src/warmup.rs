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
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Bytes read per iteration. Matches a typical file-system block multiple and
/// the transport window; the buffer is reused so RSS stays at this size.
const WINDOW: usize = 65_536;

/// How long [`warm_spawn`] waits before killing the child.
pub const SPAWN_BUDGET: Duration = Duration::from_secs(2);

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

/// Spawn `path` with `args`, wait for a successful exit, then return.
///
/// Stdio is detached. The child must exit without connecting to a daemon or
/// taking its locks. Uses [`SPAWN_BUDGET`].
pub fn warm_spawn(path: &Path, args: &[impl AsRef<OsStr>]) -> io::Result<()> {
    warm_spawn_until(path, args, Instant::now() + SPAWN_BUDGET)
}

/// As [`warm_spawn`], with an explicit deadline.
pub fn warm_spawn_until(
    path: &Path,
    args: &[impl AsRef<OsStr>],
    deadline: Instant,
) -> io::Result<()> {
    let mut child = Command::new(path)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;
    loop {
        match child.try_wait()? {
            Some(status) if status.success() => return Ok(()),
            Some(status) => {
                return Err(io::Error::other(format!(
                    "warmup {} exited {status}",
                    path.display()
                )));
            }
            None if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("warmup {} exceeded its budget", path.display()),
                ));
            }
            None => {
                let slice = Duration::from_millis(1)
                    .min(deadline.saturating_duration_since(Instant::now()));
                if slice.is_zero() {
                    continue;
                }
                std::thread::sleep(slice);
            }
        }
    }
}

pub(crate) fn prefault_all(paths: impl IntoIterator<Item = impl AsRef<Path>>) {
    for path in paths {
        let _ = warm_executable(path.as_ref());
    }
}
