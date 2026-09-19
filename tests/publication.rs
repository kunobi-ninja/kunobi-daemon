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
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("discovery");
    let a = Arc::new(vec![b'a'; 32_768]);
    let b = Arc::new(vec![b'b'; 65_536]);
    publish_record(&path, &a).unwrap();
    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(3));
        for bytes in [&a, &b] {
            let barrier = Arc::clone(&barrier);
            let path = &path;
            scope.spawn(move || {
                barrier.wait();
                for _ in 0..25 {
                    publish_record(path, bytes).unwrap();
                }
            });
        }
        barrier.wait();
        for _ in 0..100 {
            let contents = std::fs::read(&path).unwrap();
            assert!(
                contents == **a || contents == **b,
                "reader saw a partial publication"
            );
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
