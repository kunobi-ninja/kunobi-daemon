//! Regression tests for daemon ownership.

use kunobi_daemon::ProcessLock;
use std::process::Command;
use std::time::{Duration, Instant};

#[test]
#[ignore = "subprocess entry point"]
fn lock_child() {
    let path = std::env::var_os("KUNOBI_DAEMON_TEST_LOCK").unwrap();
    let expected = std::env::var("KUNOBI_DAEMON_TEST_AVAILABLE").unwrap() == "yes";
    #[cfg(unix)]
    if std::env::var("KUNOBI_DAEMON_TEST_LEGACY").as_deref() == Ok("yes") {
        assert_eq!(legacy_lock(std::path::Path::new(&path)).is_some(), expected);
        return;
    }
    assert_eq!(ProcessLock::try_acquire(path).unwrap().is_some(), expected);
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn legacy_lock(path: &std::path::Path) -> Option<std::fs::File> {
    use std::os::fd::AsRawFd;
    unsafe extern "C" {
        fn flock(fd: i32, operation: i32) -> i32;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    // SAFETY: the owned descriptor is live. These are the legacy Unix LOCK_EX
    // and LOCK_NB values used by existing broker binaries on macOS and Linux.
    (unsafe { flock(file.as_raw_fd(), 2 | 4) } == 0).then_some(file)
}

#[cfg(unix)]
#[test]
fn old_flock_and_new_file_locks_exclude_each_other_across_processes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.lock");
    let new = ProcessLock::try_acquire(&path).unwrap().unwrap();
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lock_child", "--ignored"])
        .env("KUNOBI_DAEMON_TEST_LOCK", &path)
        .env("KUNOBI_DAEMON_TEST_AVAILABLE", "no")
        .env("KUNOBI_DAEMON_TEST_LEGACY", "yes")
        .status()
        .unwrap();
    assert!(status.success());
    drop(new);
    let old = legacy_lock(&path).unwrap();
    child(&path, false);
    drop(old);
    child(&path, true);
}

fn child(path: &std::path::Path, available: bool) {
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "lock_child", "--ignored"])
        .env("KUNOBI_DAEMON_TEST_LOCK", path)
        .env(
            "KUNOBI_DAEMON_TEST_AVAILABLE",
            if available { "yes" } else { "no" },
        )
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn another_process_observes_ownership_and_drop_releases_the_same_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.lock");
    std::fs::write(&path, b"persistent identity").unwrap();
    let owner = ProcessLock::try_acquire(&path).unwrap().unwrap();
    #[cfg(unix)]
    let inode = {
        use std::os::unix::fs::MetadataExt;
        std::fs::metadata(&path).unwrap().ino()
    };
    child(&path, false);
    drop(owner);
    assert_eq!(std::fs::read(&path).unwrap(), b"persistent identity");
    child(&path, true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_eq!(std::fs::metadata(path).unwrap().ino(), inode);
    }
}

#[test]
fn distinct_instances_do_not_block_each_other_and_io_errors_are_not_contention() {
    let dir = tempfile::tempdir().unwrap();
    let _a = ProcessLock::try_acquire(dir.path().join("a.lock"))
        .unwrap()
        .unwrap();
    child(&dir.path().join("b.lock"), true);
    assert!(ProcessLock::try_acquire(dir.path().join("missing/lock")).is_err());
}

/// Hold `lock` on another thread for `held`, then release it.
fn release_after(lock: ProcessLock, held: Duration) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        #[expect(
            clippy::disallowed_methods,
            reason = "The holder keeps the lock for a measured time so the waiter must wait for its release."
        )]
        std::thread::sleep(held);
        drop(lock);
    })
}

#[test]
fn acquiring_waits_for_the_holder_and_gives_up_at_the_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.lock");
    let holder = ProcessLock::try_acquire(&path).unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_millis(100);
    assert!(
        ProcessLock::acquire_until(&path, deadline)
            .unwrap()
            .is_none()
    );
    assert!(Instant::now() >= deadline);

    let start = Instant::now();
    let release = release_after(holder, Duration::from_millis(100));
    let lock = ProcessLock::acquire_until(&path, Instant::now() + Duration::from_secs(10))
        .unwrap()
        .expect("the holder released the lock before the deadline");
    assert!(start.elapsed() >= Duration::from_millis(100));
    release.join().unwrap();
    assert!(ProcessLock::try_acquire(&path).unwrap().is_none());
    drop(lock);

    // A deadline already passed still gets one attempt.
    assert!(
        ProcessLock::acquire_until(&path, Instant::now())
            .unwrap()
            .is_some()
    );
    assert!(ProcessLock::acquire_until(dir.path().join("missing/lock"), Instant::now()).is_err());
}

#[test]
fn waiting_for_a_release_creates_nothing_and_leaves_the_lock_free() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("run.lock");
    assert!(ProcessLock::wait_released_until(&path, Instant::now()).unwrap());
    assert!(!path.exists());

    let holder = ProcessLock::try_acquire(&path).unwrap().unwrap();
    let deadline = Instant::now() + Duration::from_millis(100);
    assert!(!ProcessLock::wait_released_until(&path, deadline).unwrap());
    assert!(Instant::now() >= deadline);

    let start = Instant::now();
    let release = release_after(holder, Duration::from_millis(100));
    assert!(
        ProcessLock::wait_released_until(&path, Instant::now() + Duration::from_secs(10)).unwrap()
    );
    assert!(start.elapsed() >= Duration::from_millis(100));
    release.join().unwrap();
    // The check took the lock only for a moment.
    assert!(ProcessLock::try_acquire(&path).unwrap().is_some());
    // A file that cannot be opened is an error, not a release.
    assert!(ProcessLock::wait_released_until(dir.path(), Instant::now()).is_err());
}

#[cfg(feature = "local")]
#[test]
fn a_private_lock_excludes_every_other_holder() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("private.lock");
    let lock = ProcessLock::acquire_private_until(&path, Instant::now())
        .unwrap()
        .unwrap();
    let deadline = Instant::now() + Duration::from_millis(50);
    assert!(
        ProcessLock::acquire_private_until(&path, deadline)
            .unwrap()
            .is_none()
    );
    assert!(ProcessLock::try_acquire(&path).unwrap().is_none());
    drop(lock);
    assert!(
        ProcessLock::acquire_private_until(&path, Instant::now())
            .unwrap()
            .is_some()
    );
}

#[cfg(all(unix, feature = "local"))]
#[test]
fn a_private_lock_refuses_a_symlink_and_makes_the_file_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("elsewhere");
    let link = dir.path().join("link.lock");
    std::os::unix::fs::symlink(&target, &link).unwrap();
    let refused = ProcessLock::acquire_private_until(&link, Instant::now()).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(
        !target.exists(),
        "a refused lock created the symlink's target"
    );
    std::fs::write(&target, b"").unwrap();
    let refused = ProcessLock::acquire_private_until(&link, Instant::now()).unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);

    let path = dir.path().join("shared.lock");
    std::fs::write(&path, b"").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    let _lock = ProcessLock::acquire_private_until(&path, Instant::now())
        .unwrap()
        .unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
}
