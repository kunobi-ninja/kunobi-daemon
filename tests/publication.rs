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
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    // Probabilistic coverage: a reader reads without pause while two
    // publishers replace the record, and the publishers keep going until it
    // has read at least READS times, so those reads overlapped publication.
    // One reader leaves moments with no handle open, which is when a
    // replacement can leave the old record pending deletion on Windows.
    const PUBLISHERS: usize = 2;
    const PUBLICATIONS: usize = 500;
    const READS: usize = 200;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    let a = Arc::new(vec![b'a'; 32_768]);
    let b = Arc::new(vec![b'b'; 65_536]);
    publish_record(&path, &a).unwrap();
    let publishing = AtomicUsize::new(PUBLISHERS);
    let reads = AtomicUsize::new(0);
    let reading = AtomicBool::new(true);
    let barrier = std::sync::Barrier::new(PUBLISHERS + 1);
    std::thread::scope(|scope| {
        for bytes in [&a, &b] {
            let (barrier, publishing, reads, reading, path) =
                (&barrier, &publishing, &reads, &reading, &path);
            scope.spawn(move || {
                // Counts this publisher out even if it panics, so the reader stops.
                let _finished = OnDrop(|| {
                    publishing.fetch_sub(1, Ordering::Release);
                });
                barrier.wait();
                for published in 1.. {
                    publish_record(path, bytes).unwrap();
                    let enough_reads =
                        reads.load(Ordering::Acquire) >= READS || !reading.load(Ordering::Acquire);
                    if published >= PUBLICATIONS && enough_reads {
                        break;
                    }
                }
            });
        }
        let (barrier, publishing, reads, reading, path, a, b) =
            (&barrier, &publishing, &reads, &reading, &path, &a, &b);
        scope.spawn(move || {
            // Lets the publishers stop even if a read panics.
            let _finished = OnDrop(|| reading.store(false, Ordering::Release));
            barrier.wait();
            while publishing.load(Ordering::Acquire) > 0 {
                let contents = std::fs::read(path).unwrap();
                assert!(
                    contents == **a || contents == **b,
                    "reader saw a partial publication"
                );
                reads.fetch_add(1, Ordering::Release);
            }
        });
    });
    assert!(reads.into_inner() >= READS);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
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
