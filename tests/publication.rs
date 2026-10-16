//! Discovery publication contracts extracted from broker publication tests.

use kunobi_daemon::publish_record;
use std::sync::Arc;

#[test]
fn old_record_cleanup_preserves_a_new_publication_and_its_lock_inode() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let slot = kunobi_daemon::RecordSlot::new(&path);
    slot.replace(b"old").unwrap();
    assert!(!slot.repair(b"old").unwrap());
    slot.replace(b"new").unwrap();
    assert!(!slot.remove_if_matches(b"old").unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert!(slot.remove_if_matches(b"new").unwrap());
    assert!(!slot.remove_if_matches(b"new").unwrap());
    assert!(root.path().join("selected.record.lock").is_file());
}

#[test]
fn concurrent_publishers_and_readers_only_observe_complete_records() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    // Probabilistic coverage: readers read without pause while two
    // publishers replace the record, and the publishers keep going until the
    // readers have read at least READS times, so those reads overlapped
    // publication. On Windows an open that races a replacement can be
    // refused, or find the name missing while the rename is in progress;
    // `RecordSlot::read` must ride out both. One publisher is durable: its
    // publications must succeed while the other one renames.
    const PUBLISHERS: usize = 2;
    const READERS: usize = 4;
    const PUBLICATIONS: usize = 1_000;
    const READS: usize = 1_000;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    let a = Arc::new(vec![b'a'; 32_768]);
    let b = Arc::new(vec![b'b'; 65_536]);
    publish_record(&path, &a).unwrap();
    let publishing = AtomicUsize::new(PUBLISHERS);
    let reads = AtomicUsize::new(0);
    let reading = AtomicUsize::new(READERS);
    let barrier = std::sync::Barrier::new(PUBLISHERS + READERS);
    std::thread::scope(|scope| {
        for (bytes, durable) in [(&a, false), (&b, true)] {
            let (barrier, publishing, reads, reading, path) =
                (&barrier, &publishing, &reads, &reading, &path);
            let options = kunobi_daemon::RecordOptions::new().durable(durable);
            scope.spawn(move || {
                // Counts this publisher out even if it panics, so the readers stop.
                let _finished = OnDrop(|| {
                    publishing.fetch_sub(1, Ordering::Release);
                });
                barrier.wait();
                for published in 1.. {
                    kunobi_daemon::publish_record_with(path, bytes, options).unwrap();
                    // A reader that stopped early has panicked.
                    let enough_reads = reads.load(Ordering::Acquire) >= READS
                        || reading.load(Ordering::Acquire) < READERS;
                    if published >= PUBLICATIONS && enough_reads {
                        break;
                    }
                }
            });
        }
        for _ in 0..READERS {
            let (barrier, publishing, reads, reading, path, a, b) =
                (&barrier, &publishing, &reads, &reading, &path, &a, &b);
            scope.spawn(move || {
                // Lets the publishers stop even if a read panics.
                let _finished = OnDrop(|| {
                    reading.fetch_sub(1, Ordering::Release);
                });
                let slot = kunobi_daemon::RecordSlot::new(path);
                barrier.wait();
                while publishing.load(Ordering::Acquire) > 0 {
                    let contents = slot.read();
                    if !matches!(&contents, Ok(Some(contents)) if contents == &**a || contents == &**b)
                    {
                        let length = contents.map(|record| record.map(|record| record.len()));
                        panic!("a reader saw {length:?} instead of a complete record");
                    }
                    reads.fetch_add(1, Ordering::Release);
                }
            });
        }
    });
    assert!(reads.into_inner() >= READS);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

/// On Windows a replacement can leave the record's name missing while the
/// rename is in progress; the temporary file is still there. A read then
/// waits for the publication, up to 2 s, instead of reporting no record.
/// NTFS names ignore case, so a temporary of the same record under another
/// case counts. The record appearing during the wait is covered step by
/// step by the unit tests in `src/publication/windows.rs`.
#[cfg(windows)]
#[test]
fn a_slot_read_waits_while_a_temporary_shows_a_publication() {
    use std::time::{Duration, Instant};
    let root = tempfile::tempdir().unwrap();
    std::fs::write(
        root.path()
            .join(format!(".DISCOVERY.{}.0.tmp", std::process::id())),
        b"new",
    )
    .unwrap();
    let slot = kunobi_daemon::RecordSlot::new(root.path().join("discovery"));
    let start = Instant::now();
    assert_eq!(slot.read().unwrap(), None);
    assert!(start.elapsed() >= Duration::from_secs(2));
}

/// A missing record is reported at once when no publication is in progress,
/// including when a crash left an old temporary file behind.
#[cfg(windows)]
#[test]
fn a_slot_read_does_not_wait_for_a_stale_temporary() {
    use std::os::windows::fs::FileTimesExt;
    use std::time::{Duration, Instant, SystemTime};
    let root = tempfile::tempdir().unwrap();
    let slot = kunobi_daemon::RecordSlot::new(root.path().join("discovery"));
    let start = Instant::now();
    assert_eq!(slot.read().unwrap(), None);
    assert!(start.elapsed() < Duration::from_secs(1));

    let temporary = std::fs::File::create(root.path().join(".discovery.1.0.tmp")).unwrap();
    let created = SystemTime::now() - Duration::from_secs(60);
    temporary
        .set_times(std::fs::FileTimes::new().set_created(created))
        .unwrap();
    drop(temporary);
    let start = Instant::now();
    assert_eq!(slot.read().unwrap(), None);
    assert!(start.elapsed() < Duration::from_secs(1));
}

/// Runs a closure when dropped, including during a panic.
struct OnDrop<F: FnMut()>(F);
impl<F: FnMut()> Drop for OnDrop<F> {
    fn drop(&mut self) {
        (self.0)();
    }
}

#[test]
fn a_failed_rename_preserves_the_target_and_removes_the_temporary() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("occupied");
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("keep"), b"existing").unwrap();
    assert!(publish_record(&path, b"candidate").is_err());
    assert_eq!(std::fs::read(path.join("keep")).unwrap(), b"existing");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

#[cfg(unix)]
#[test]
fn a_published_record_is_owner_only() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    publish_record(&path, b"identity").unwrap();
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600
    );
}

#[test]
fn a_slot_waits_for_its_lock_holder_and_times_out_while_it_stays() {
    use kunobi_daemon::ProcessLock;
    use std::time::{Duration, Instant};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let slot = kunobi_daemon::RecordSlot::new(&path);
    let holder = ProcessLock::try_acquire(root.path().join("selected.record.lock"))
        .unwrap()
        .unwrap();
    let start = Instant::now();
    let refused = slot.replace(b"blocked").unwrap_err();
    assert_eq!(refused.kind(), std::io::ErrorKind::TimedOut);
    assert!(start.elapsed() >= Duration::from_secs(2));
    assert!(!path.exists());

    let release = std::thread::spawn(move || {
        #[expect(
            clippy::disallowed_methods,
            reason = "The holder keeps the slot lock for a measured time so the publisher must wait for it."
        )]
        std::thread::sleep(Duration::from_millis(100));
        drop(holder);
    });
    slot.replace(b"after").unwrap();
    release.join().unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"after");
}

#[test]
fn a_slot_reads_only_a_bounded_regular_file() {
    use kunobi_daemon::{RecordOptions, RecordSlot};
    use std::io::ErrorKind::InvalidData;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let slot = RecordSlot::new(&path).options(RecordOptions::new().read_limit(4));
    assert_eq!(slot.read().unwrap(), None);
    slot.replace(b"four").unwrap();
    assert_eq!(slot.read().unwrap().as_deref(), Some(&b"four"[..]));
    slot.replace(b"fives").unwrap();
    assert_eq!(slot.read().unwrap_err().kind(), InvalidData);

    // The default limit is 64 KiB.
    let default = RecordSlot::new(&path);
    assert_eq!(RecordOptions::default(), RecordOptions::new());
    default.replace(&vec![b'x'; 64 * 1024]).unwrap();
    assert_eq!(default.read().unwrap().unwrap().len(), 64 * 1024);
    default.replace(&vec![b'x'; 64 * 1024 + 1]).unwrap();
    assert_eq!(default.read().unwrap_err().kind(), InvalidData);

    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert_eq!(slot.read().unwrap_err().kind(), InvalidData);
    // A path that cannot exist is an error, not a missing record. Windows
    // reports it as not found.
    #[cfg(unix)]
    {
        let below_a_file = RecordSlot::new(root.path().join("selected.record.lock/inner"));
        assert_ne!(below_a_file.read().unwrap_err().kind(), InvalidData);
    }
}

#[cfg(unix)]
#[test]
fn a_slot_does_not_read_through_a_symlink() {
    let root = tempfile::tempdir().unwrap();
    let target = root.path().join("target");
    std::fs::write(&target, b"elsewhere").unwrap();
    let path = root.path().join("selected");
    std::os::unix::fs::symlink(&target, &path).unwrap();
    let slot = kunobi_daemon::RecordSlot::new(&path);
    assert_eq!(
        slot.read().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(slot.remove_if(|_| true).is_err());
    assert_eq!(std::fs::read(&target).unwrap(), b"elsewhere");
}

#[test]
fn conditional_changes_decide_under_the_slot_lock() {
    use kunobi_daemon::ProcessLock;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let lock = root.path().join("selected.record.lock");
    let slot = kunobi_daemon::RecordSlot::new(&path);

    assert!(!slot.remove_if(|_| panic!("no record to judge")).unwrap());
    assert!(
        slot.replace_if(b"first", |current| {
            assert!(ProcessLock::is_held(&lock).unwrap());
            current.is_none()
        })
        .unwrap()
    );
    assert!(
        !slot
            .replace_if(b"second", |current| current.is_none())
            .unwrap()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"first");
    assert!(
        slot.replace_if(b"second", |current| current == Some(&b"first"[..]))
            .unwrap()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"second");

    assert!(!slot.remove_if(|current| current == b"first").unwrap());
    assert!(path.exists());
    assert!(
        slot.remove_if(|current| {
            assert!(ProcessLock::is_held(&lock).unwrap());
            current == b"second"
        })
        .unwrap()
    );
    assert!(!path.exists());
    assert!(!ProcessLock::is_held(&lock).unwrap());
}

#[cfg(unix)]
#[test]
fn publication_options_set_the_mode_and_sync_the_directory() {
    use kunobi_daemon::{RecordOptions, publish_record_with};
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    publish_record_with(&path, b"shared", RecordOptions::new().mode(0o640)).unwrap();
    let mode = std::fs::metadata(&path).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o640);

    // A directory this user can write but not read: the rename succeeds, and
    // only the directory sync, which opens it, fails.
    let blind = root.path().join("blind");
    std::fs::create_dir(&blind).unwrap();
    std::fs::set_permissions(&blind, std::fs::Permissions::from_mode(0o300)).unwrap();
    if std::fs::read_dir(&blind).is_ok() {
        return; // A privileged user reads it anyway; nothing to observe.
    }
    let record = blind.join("discovery");
    publish_record(&record, b"plain").unwrap();
    let durable = RecordOptions::new().durable(true);
    let error = publish_record_with(&record, b"durable", durable).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    std::fs::set_permissions(&blind, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert_eq!(std::fs::read(&record).unwrap(), b"durable");
}

#[test]
fn a_durable_slot_replaces_its_record() {
    use kunobi_daemon::{RecordOptions, RecordSlot};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let slot = RecordSlot::new(&path).options(RecordOptions::new().durable(true));
    slot.replace(b"old").unwrap();
    slot.replace(b"new").unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 2);
}

#[cfg(feature = "local")]
#[test]
fn a_private_directory_is_created_and_must_be_a_real_directory() {
    use kunobi_daemon::ensure_private_dir;
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("a/b");
    ensure_private_dir(&dir).unwrap();
    assert!(dir.is_dir());
    let file = root.path().join("file");
    std::fs::write(&file, b"").unwrap();
    assert!(ensure_private_dir(&file).is_err());

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode =
            |path: &std::path::Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir), 0o700);
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        ensure_private_dir(&dir).unwrap();
        assert_eq!(mode(&dir), 0o700);

        let link = root.path().join("link");
        std::os::unix::fs::symlink(&dir, &link).unwrap();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        let refused = ensure_private_dir(&link).unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied);
        assert_eq!(mode(&dir), 0o755, "a refused symlink changed its target");
    }
}

#[test]
fn repair_restores_a_record_that_differs() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let slot = kunobi_daemon::RecordSlot::new(&path);
    assert!(slot.repair(b"mine").unwrap());
    std::fs::write(&path, b"stale").unwrap();
    assert!(slot.repair(b"mine").unwrap());
    assert_eq!(std::fs::read(&path).unwrap(), b"mine");
}

#[test]
fn a_slot_reads_one_complete_version_while_records_are_replaced() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    let slot = kunobi_daemon::RecordSlot::new(&path);
    let a = vec![b'a'; 16_384];
    let b = vec![b'b'; 32_768];
    publish_record(&path, &a).unwrap();
    let publishing = AtomicBool::new(true);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            // Stops the reader even if publishing panics.
            struct Finished<'a>(&'a AtomicBool);
            impl Drop for Finished<'_> {
                fn drop(&mut self) {
                    self.0.store(false, Ordering::Release);
                }
            }
            let _finished = Finished(&publishing);
            for bytes in [&a, &b].iter().cycle().take(400) {
                publish_record(&path, bytes).unwrap();
            }
        });
        // Probabilistic coverage: reads race the renames above for as long
        // as they run. Each must return one version whole.
        while publishing.load(Ordering::Acquire) {
            let contents = slot.read().unwrap().expect("the record always exists");
            assert!(contents == a || contents == b, "read a partial record");
        }
    });
}

#[cfg(unix)]
#[test]
fn a_slot_refuses_a_fifo_without_waiting_for_a_writer() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("selected");
    let made = std::process::Command::new("mkfifo")
        .arg(&path)
        .status()
        .unwrap();
    assert!(made.success());
    let slot = kunobi_daemon::RecordSlot::new(&path);
    assert_eq!(
        slot.read().unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
    assert!(slot.remove_if(|_| true).is_err());
    assert!(path.exists());
}
