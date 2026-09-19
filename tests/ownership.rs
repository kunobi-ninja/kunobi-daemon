//! Regression tests for daemon ownership.

use kunobi_daemon::ProcessLock;
use std::process::Command;

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
