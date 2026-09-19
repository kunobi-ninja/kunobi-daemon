//! Candidate process ownership through cancellation and commit.
use kunobi_daemon::{Candidate, ProcessLock, readiness::wait_until};
use std::{
    io,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

fn wait<T>(probe: impl FnMut(Instant) -> io::Result<Option<T>>) -> T {
    wait_until(Instant::now() + Duration::from_secs(10), probe)
        .unwrap()
        .expect("child progress")
}

#[test]
#[ignore = "subprocess entry point"]
fn candidate_child() {
    let dir = std::env::var_os("DAEMON_CANDIDATE_TEST_DIR").unwrap();
    let dir = Path::new(&dir);
    let _lock = ProcessLock::try_acquire(dir.join("process.lock"))
        .unwrap()
        .unwrap();
    std::fs::write(dir.join("ready"), b"ready").unwrap();
    wait(|_| Ok(dir.join("release").exists().then_some(())));
    std::fs::write(dir.join("completed"), b"completed").unwrap();
}

#[test]
fn cancelling_a_candidate_releases_ownership_but_a_selected_process_keeps_working() {
    for selected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "candidate_child", "--ignored"])
            .env("DAEMON_CANDIDATE_TEST_DIR", dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()
            .unwrap();
        let mut candidate = Candidate::new(child);
        wait(|_| Ok(dir.path().join("ready").exists().then_some(())));
        assert!(candidate.try_wait().unwrap().is_none());
        let lock = dir.path().join("process.lock");
        if selected {
            candidate.selected();
        }
        drop(candidate);
        if selected {
            assert!(ProcessLock::is_held(&lock).unwrap());
            std::fs::write(dir.path().join("release"), b"release").unwrap();
            wait(|_| Ok(dir.path().join("completed").exists().then_some(())));
        } else {
            assert!(!dir.path().join("completed").exists());
        }
        wait(|_| ProcessLock::try_acquire(&lock));
    }
}
