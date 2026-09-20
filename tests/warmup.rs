//! Prefault and bounded spawn do not take daemon locks.
use kunobi_daemon::{warm_executable, warm_spawn, warm_spawn_until};
use std::{
    io,
    time::{Duration, Instant},
};

#[test]
fn prefault_reads_an_existing_file_and_rejects_missing_paths() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("shim");
    std::fs::write(&file, vec![0xA5; 80_000]).unwrap();
    warm_executable(&file).unwrap();
    let missing = warm_executable(&dir.path().join("absent")).unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
}

#[test]
fn an_empty_file_is_already_warm() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("empty");
    std::fs::write(&file, []).unwrap();
    warm_executable(&file).unwrap();
}

#[cfg(unix)]
#[test]
fn a_directory_is_not_an_executable() {
    let dir = tempfile::tempdir().unwrap();
    let error = warm_executable(dir.path()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::IsADirectory);
}

#[test]
fn spawn_requires_a_successful_exit() {
    #[cfg(unix)]
    {
        warm_spawn(std::path::Path::new("/usr/bin/true"), &[] as &[&str]).unwrap();
        let error = warm_spawn(std::path::Path::new("/usr/bin/false"), &[] as &[&str]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }
    #[cfg(windows)]
    {
        warm_spawn(std::path::Path::new("cmd.exe"), &["/c", "exit", "0"]).unwrap();
        let error = warm_spawn(std::path::Path::new("cmd.exe"), &["/c", "exit", "1"]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
    }
    let missing = warm_spawn(
        std::path::Path::new("kunobi-daemon-no-such-warmup"),
        &[] as &[&str],
    )
    .unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::NotFound);
}

#[cfg(unix)]
#[test]
fn a_stuck_spawn_is_killed_at_the_deadline() {
    let error = warm_spawn_until(
        std::path::Path::new("/bin/sleep"),
        &["30"],
        Instant::now() + Duration::from_millis(80),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
}
