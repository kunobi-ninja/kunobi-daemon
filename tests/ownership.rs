//! Regression tests for daemon ownership.

use kunobi_daemon::ProcessLock;
use std::process::Command;

#[test]
#[ignore = "subprocess entry point"]
fn lock_child() {
    let path = std::env::var_os("KUNOBI_DAEMON_TEST_LOCK").unwrap();
    let expected = std::env::var("KUNOBI_DAEMON_TEST_AVAILABLE").unwrap() == "yes";
    assert_eq!(ProcessLock::try_acquire(path).unwrap().is_some(), expected);
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
