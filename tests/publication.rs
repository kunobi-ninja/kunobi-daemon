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
    // Readers keep reading for as long as both publishers replace the record,
    // so a replacement that briefly hides the record is caught on every run
    // rather than now and then.
    const PUBLISHERS: usize = 2;
    const READERS: usize = 2;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    let a = Arc::new(vec![b'a'; 32_768]);
    let b = Arc::new(vec![b'b'; 65_536]);
    publish_record(&path, &a).unwrap();
    let publishing = AtomicUsize::new(PUBLISHERS);
    let barrier = std::sync::Barrier::new(PUBLISHERS + READERS);
    std::thread::scope(|scope| {
        for bytes in [&a, &b] {
            let (barrier, publishing, path) = (&barrier, &publishing, &path);
            scope.spawn(move || {
                // Counts this publisher out even if it panics, so readers stop.
                struct Finished<'a>(&'a AtomicUsize);
                impl Drop for Finished<'_> {
                    fn drop(&mut self) {
                        self.0.fetch_sub(1, Ordering::Release);
                    }
                }
                let _finished = Finished(publishing);
                barrier.wait();
                for _ in 0..200 {
                    publish_record(path, bytes).unwrap();
                }
            });
        }
        for _ in 0..READERS {
            let (barrier, publishing, path, a, b) = (&barrier, &publishing, &path, &a, &b);
            scope.spawn(move || {
                barrier.wait();
                while publishing.load(Ordering::Acquire) > 0 {
                    let contents = std::fs::read(path).unwrap();
                    assert!(
                        contents == **a || contents == **b,
                        "reader saw a partial publication"
                    );
                }
            });
        }
    });
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
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
