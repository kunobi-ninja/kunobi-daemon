//! Client-side start of a local daemon from a stdio shim.
//!
//! Enable the `launch` feature. A process that only binds and serves keeps
//! `local` and does not compile this module.
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
//! Liveness is a connect (or an application handshake), never the existence of
//! a discovery file. [`wait_until_live`] polls the caller's probe.
//!
//! Stdio of this process is not inherited. A daemon that writes to the shim's
//! stdout corrupts the client's protocol.

use std::io;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Gap between liveness probes while waiting for a freshly spawned peer.
pub const POLL: Duration = Duration::from_millis(100);

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

/// Spawn a peer with stdin, stdout and stderr detached.
///
/// On Unix this restores SIGCHLD around `Command::spawn` so a failed exec
/// still returns an error instead of panicking, then reap-ignores again.
pub fn spawn_detached(command: &mut Command) -> io::Result<Child> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    #[cfg(unix)]
    {
        crate::local::unix::spawn_with_child_waiting(command)
    }
    #[cfg(not(unix))]
    {
        command.spawn()
    }
}

/// Poll `is_live` until it succeeds or `deadline` is reached.
///
/// `is_live` must attempt a connect (or a protocol probe). Do not use
/// `path.exists()`.
pub fn wait_until_live(deadline: Instant, mut is_live: impl FnMut() -> bool) -> bool {
    loop {
        if is_live() {
            return true;
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        std::thread::sleep(POLL.min(deadline.saturating_duration_since(now)));
    }
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
    use std::path::Path;

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
    #[test]
    fn spawn_detached_execs() {
        let bin = ["/usr/bin/true", "/bin/true"]
            .into_iter()
            .find(|path| Path::new(path).exists());
        let Some(bin) = bin else {
            return;
        };
        let mut command = Command::new(bin);
        let mut child = spawn_detached(&mut command).unwrap();
        assert!(child.wait().unwrap().success());
    }
}
