//! Unix listener acquisition and guarded endpoint cleanup.
use crate::ProcessLock;
use std::{
    io,
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::UnixListener,
    },
    path::{Path, PathBuf},
    time::Duration,
};

/// A bind result. Busy is ownership evidence, never application readiness proof.
#[derive(Debug)]
pub enum Bound {
    /// The caller owns this listener.
    Won(UnixListener),
    /// A listener or another serialized binder already owns the endpoint.
    AlreadyRunning,
}
/// Failed acquisition, preserving filesystem errors separately from contention.
#[derive(Debug)]
pub enum BindError {
    /// Failed to prepare the private parent directory.
    Directory(io::Error),
    /// Failed to bind, acquire the serialization lock, or probe the existing socket.
    Bind(io::Error),
    /// Failed to remove a proven stale socket.
    Unlink(io::Error),
}
fn lock_path(path: &Path) -> PathBuf {
    let mut path = path.as_os_str().to_owned();
    path.push(".bind.lock");
    path.into()
}
/// Prepare an application-owned directory before creating its socket.
pub fn ensure_private_dir(dir: &Path) -> io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let metadata = std::fs::symlink_metadata(dir)?;
    if !metadata.is_dir() || metadata.uid() != super::unix::own_uid() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}
fn bind_private(socket: &Path) -> io::Result<UnixListener> {
    // The 0700 parent excludes other users during bind; never alter global umask.
    let listener = UnixListener::bind(socket)?;
    std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}
/// Serialize stale-socket recovery. Only ConnectionRefused permits reclaiming
/// an existing socket; permission failures and unrelated files are preserved.
pub fn acquire(socket: &Path) -> Result<Bound, BindError> {
    ensure_private_dir(
        socket
            .parent()
            .ok_or_else(|| BindError::Directory(io::ErrorKind::InvalidInput.into()))?,
    )
    .map_err(BindError::Directory)?;
    let Some(_lock) = ProcessLock::try_acquire(lock_path(socket)).map_err(BindError::Bind)? else {
        return Ok(Bound::AlreadyRunning);
    };
    match bind_private(socket) {
        Ok(listener) => return Ok(Bound::Won(listener)),
        Err(error) if error.kind() == io::ErrorKind::AddrInUse => {}
        Err(error) => return Err(BindError::Bind(error)),
    }
    let metadata = std::fs::symlink_metadata(socket).map_err(BindError::Bind)?;
    if !metadata.file_type().is_socket() {
        return Err(BindError::Bind(io::ErrorKind::AlreadyExists.into()));
    }
    let probe = socket2::Socket::new(socket2::Domain::UNIX, socket2::Type::STREAM, None)
        .map_err(BindError::Bind)?;
    let address = socket2::SockAddr::unix(socket).map_err(BindError::Bind)?;
    match probe.connect_timeout(&address, Duration::from_millis(250)) {
        Ok(()) => return Ok(Bound::AlreadyRunning),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
            ) =>
        {
            return Ok(Bound::AlreadyRunning);
        }
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
        Err(error) => return Err(BindError::Bind(error)),
    }
    std::fs::remove_file(socket).map_err(BindError::Unlink)?;
    bind_private(socket)
        .map(Bound::Won)
        .map_err(BindError::Bind)
}

/// Remove only the socket inode this handle observed, under the bind lock.
/// An exiting generation cannot unlink an endpoint created by its successor.
pub struct SocketGuard {
    path: PathBuf,
    device: u64,
    inode: u64,
}
impl SocketGuard {
    /// Capture immediately after binding, before publishing discovery.
    pub fn capture(path: &Path) -> io::Result<Self> {
        let metadata = std::fs::symlink_metadata(path)?;
        if !metadata.file_type().is_socket() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(Self {
            path: path.to_owned(),
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let Ok(Some(_lock)) = ProcessLock::try_acquire(lock_path(&self.path)) else {
            return;
        };
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket()
                && metadata.dev() == self.device
                && metadata.ino() == self.inode
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!("daemon-single-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    #[test]
    fn the_first_binder_wins() {
        let dir = temp_dir("first");
        let sock = dir.join("b.sock");
        match acquire(&sock).unwrap() {
            Bound::Won(_l) => {}
            other => panic!("expected to win, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_second_binder_stands_down_while_the_first_is_listening() {
        let dir = temp_dir("second");
        let sock = dir.join("b.sock");
        let _first = match acquire(&sock).unwrap() {
            Bound::Won(l) => l,
            other => panic!("first should win, got {other:?}"),
        };

        match acquire(&sock).unwrap() {
            Bound::AlreadyRunning => {}
            other => panic!("second should stand down, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_stale_socket_left_by_a_dead_broker_is_reclaimed() {
        // Without this the system is permanently brokerless after any crash:
        // the path exists so `bind` fails, but nothing is listening so no client
        // can ever connect.
        let dir = temp_dir("stale");
        // Under the lock: a directory created while a sibling test holds a
        // narrowed umask comes out 0o600 and is untraversable.
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("b.sock");

        // Bind then drop the listener WITHOUT unlinking - exactly what a killed
        // process leaves behind.
        {
            let l = UnixListener::bind(&sock).unwrap();
            drop(l);
        }
        assert!(sock.exists(), "fixture should leave the inode behind");

        match acquire(&sock).unwrap() {
            Bound::Won(_l) => {}
            other => panic!("a stale socket should be reclaimed, got {other:?}"),
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_live_broker_is_never_unlinked() {
        // The dangerous mistake: unlinking on EADDRINUSE alone. That races a
        // healthy broker and silently detaches every client attached to it.
        let dir = temp_dir("nounlink");
        let sock = dir.join("b.sock");
        let _first = match acquire(&sock).unwrap() {
            Bound::Won(l) => l,
            other => panic!("{other:?}"),
        };

        let before = std::fs::metadata(&sock).unwrap();
        let _ = acquire(&sock).unwrap();
        let after = std::fs::metadata(&sock).unwrap();

        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            before.ino(),
            after.ino(),
            "the live broker's socket was replaced"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_regular_file_at_the_socket_path_is_preserved() {
        // An unrelated regular file must survive a failed acquisition.
        let dir = temp_dir("regular");
        // Under the lock: a directory created while a sibling test holds a
        // narrowed umask comes out 0o600 and is untraversable.
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("b.sock");
        let mut f = std::fs::File::create(&sock).unwrap();
        f.write_all(b"not a socket").unwrap();
        drop(f);

        assert!(acquire(&sock).is_err());
        assert_eq!(std::fs::read(&sock).unwrap(), b"not a socket");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_socket_is_owner_only() {
        let dir = temp_dir("perms");
        let sock = dir.join("b.sock");
        let _l = match acquire(&sock).unwrap() {
            Bound::Won(l) => l,
            other => panic!("{other:?}"),
        };

        let mode = std::fs::metadata(&sock).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "socket is 0o{mode:o}, expected 0o600");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn the_parent_directory_is_owner_only_even_if_it_already_existed() {
        // `create_dir_all` is a no-op on an existing directory, so a directory
        // created by an older build - or by a user - would keep its permissions
        // and never be checked.
        let dir = temp_dir("dirperms");
        // Under the lock: a directory created while a sibling test holds a
        // narrowed umask comes out 0o600 and is untraversable.
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();

        let sock = dir.join("b.sock");
        let _l = acquire(&sock).unwrap();

        let mode = std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700, "directory left at 0o{mode:o}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn many_concurrent_acquirers_produce_exactly_one_winner() {
        // The thundering herd: N relays start at once against no broker. Exactly
        // one must win; every other must stand down cleanly rather than error.
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = temp_dir("herd");
        // Under the lock: a directory created while a sibling test holds a
        // narrowed umask comes out 0o600 and is untraversable.
        std::fs::create_dir_all(&dir).unwrap();
        let sock = Arc::new(dir.join("b.sock"));

        let won = Arc::new(AtomicUsize::new(0));
        let stood_down = Arc::new(AtomicUsize::new(0));
        let failed = Arc::new(AtomicUsize::new(0));

        let handles: Vec<_> = (0..15)
            .map(|_| {
                let sock = Arc::clone(&sock);
                let won = Arc::clone(&won);
                let stood_down = Arc::clone(&stood_down);
                let failed = Arc::clone(&failed);
                std::thread::spawn(move || {
                    match acquire(&sock) {
                        Ok(Bound::Won(l)) => {
                            won.fetch_add(1, Ordering::SeqCst);
                            // Hold it, so the losers meet a LIVE socket.
                            std::thread::sleep(std::time::Duration::from_millis(200));
                            drop(l);
                        }
                        Ok(Bound::AlreadyRunning) => {
                            stood_down.fetch_add(1, Ordering::SeqCst);
                        }
                        Err(_) => {
                            failed.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(failed.load(Ordering::SeqCst), 0, "some acquirers errored");
        assert_eq!(
            won.load(Ordering::SeqCst),
            1,
            "expected exactly one winner, got {} (stood down: {})",
            won.load(Ordering::SeqCst),
            stood_down.load(Ordering::SeqCst)
        );
        assert_eq!(stood_down.load(Ordering::SeqCst), 14);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
