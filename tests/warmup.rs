//! Prefault and bounded spawn do not take daemon locks.
use kunobi_daemon::{warm_executable, warm_spawn};
use std::io;
#[cfg(unix)]
use std::time::{Duration, Instant};

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
fn warm_spawn_uses_the_warmup_argv_and_does_not_run_discovery() {
    let dir = tempfile::tempdir().unwrap();
    let shim = dir.path().join("shim.sh");
    std::fs::write(
        &shim,
        "#!/bin/sh\nif [ \"$1\" = \"--warmup\" ]; then exit 0; fi\nsleep 30\nexit 1\n",
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&shim).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&shim, perms).unwrap();
    let started = Instant::now();
    warm_spawn(&shim, &["--warmup"]).unwrap();
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "warmup ran the shim's long path"
    );
}

#[cfg(unix)]
#[test]
fn a_stuck_spawn_is_killed_at_the_deadline() {
    use kunobi_daemon::warm_spawn_until;
    use std::time::{Duration, Instant};
    let error = warm_spawn_until(
        std::path::Path::new("/bin/sleep"),
        &["30"],
        Instant::now() + Duration::from_millis(80),
    )
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);
}
