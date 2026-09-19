//! Generation allocation and retirement do not depend on an application protocol.
#![cfg(feature = "async")]
use kunobi_daemon::{
    ProcessLock,
    generation::{Generation, allocate},
};
use std::{sync::Arc, time::Duration};

#[test]
fn counter_loss_uses_selected_and_reserved_generations_and_corruption_fails() {
    let root = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(root.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let counter = root.path().join("epoch");
    assert_eq!(allocate(&lock, root.path(), &counter, 40).unwrap(), 41);
    std::fs::write(root.path().join("45.lock"), "").unwrap();
    std::fs::remove_file(&counter).unwrap();
    assert_eq!(allocate(&lock, root.path(), &counter, 40).unwrap(), 46);
    std::fs::write(&counter, "corrupt").unwrap();
    assert!(allocate(&lock, root.path(), &counter, 40).is_err());
    std::fs::write(&counter, u64::MAX.to_string()).unwrap();
    assert!(allocate(&lock, root.path(), &counter, 40).is_err());
}

#[test]
fn selection_never_goes_backwards_and_a_lease_prevents_retirement() {
    let generation = Arc::new(Generation::new(2, 2, false));
    let mut session = generation.admit().unwrap();
    session.mark_legacy();
    session.mark_legacy();
    assert_eq!(generation.snapshot().legacy, 1);
    assert!(!generation.retire_if_idle(Duration::ZERO));
    generation.select(3).unwrap();
    assert!(generation.select(2).is_err());
    assert_eq!(generation.selected(), 3);
    assert!(!generation.retire_if_idle(Duration::ZERO));
    drop(session);
    assert_eq!(generation.snapshot().legacy, 0);
    assert!(generation.retire_if_idle(Duration::ZERO));
    assert!(generation.admit().is_none());
}

#[tokio::test(start_paused = true)]
async fn staged_candidate_expires_but_selected_generation_does_not() {
    let staged = Generation::new(3, 2, true);
    let selected = Generation::new(2, 2, false);
    tokio::time::advance(Duration::from_secs(31)).await;
    assert!(staged.retire_if_idle(Duration::from_secs(30)));
    assert!(!selected.retire_if_idle(Duration::from_secs(30)));
}

#[tokio::test]
async fn session_release_wakes_an_existing_observer() {
    let generation = Arc::new(Generation::new(2, 3, false));
    let mut changes = generation.subscribe();
    let session = generation.admit().unwrap();
    drop(session);
    tokio::time::timeout(Duration::from_secs(1), changes.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(generation.retire_if_idle(Duration::ZERO));
}

#[test]
fn unchanged_selection_does_not_wake_the_controller_into_a_busy_loop() {
    let generation = Generation::new(2, 2, false);
    let mut changes = generation.subscribe();
    generation.select(2).unwrap();
    assert!(!changes.has_changed().unwrap());
    generation.select(3).unwrap();
    assert!(changes.has_changed().unwrap());
    changes.borrow_and_update();
    generation.select(3).unwrap();
    assert!(!changes.has_changed().unwrap());
}
