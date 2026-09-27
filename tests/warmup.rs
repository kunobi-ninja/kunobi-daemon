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
    // The shim records which path it took. Timing cannot tell: macOS checks a
    // newly written executable on its first run, which alone took 0.2-2.3s here.
    let dir = tempfile::tempdir().unwrap();
    let shim = dir.path().join("shim.sh");
    let discovery = dir.path().join("ran-discovery");
    std::fs::write(
        &shim,
        format!(
            "#!/bin/sh\nif [ \"$1\" = \"--warmup\" ]; then exit 0; fi\ntouch '{}'\nsleep 30\nexit 1\n",
            discovery.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&shim).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&shim, perms).unwrap();
    kunobi_daemon::warm_spawn_until(
        &shim,
        &["--warmup"],
        Instant::now() + Duration::from_secs(20),
    )
    .unwrap();
    assert!(!discovery.exists(), "warmup ran the shim's long path");
}

#[cfg(unix)]
#[test]
fn a_child_still_running_at_the_deadline_is_left_to_finish() {
    // The child stands in for macOS's first-run check of a new executable,
    // which can outlast the budget. Killing it would throw that work away.
    use std::io::{Read, Write};
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let go = dir.path().join("go");
    let done = dir.path().join("done");
    for fifo in [&go, &done] {
        let made = std::process::Command::new("mkfifo").arg(fifo).status();
        assert!(made.unwrap().success());
    }
    let shim = dir.path().join("shim.sh");
    std::fs::write(
        &shim,
        "#!/bin/sh\nread line < \"$1\"\necho finished > \"$2\"\n",
    )
    .unwrap();
    let mut perms = std::fs::metadata(&shim).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&shim, perms).unwrap();

    // The child cannot exit before it is released, so the budget runs out.
    let error = kunobi_daemon::warm_spawn_until(&shim, &[&go, &done], Instant::now()).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::TimedOut);

    // Release it and read what it writes. A killed child never opens `go`,
    // so this thread would block and the receive below would fail.
    let (report, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let mut release = std::fs::OpenOptions::new().write(true).open(&go).unwrap();
        release.write_all(b"go\n").unwrap();
        drop(release);
        let mut out = String::new();
        std::fs::File::open(&done)
            .unwrap()
            .read_to_string(&mut out)
            .unwrap();
        let _ = report.send(out);
    });
    let out = finished.recv_timeout(Duration::from_secs(30));
    assert_eq!(out.as_deref(), Ok("finished\n"), "the child was killed");
}
