//! Generation allocation and retirement do not depend on an application protocol.
#![cfg(feature = "async")]
use kunobi_daemon::{
    ProcessLock,
    generation::{Generation, Retirement, allocate},
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

#[tokio::test(start_paused = true)]
async fn drain_limit_retires_a_superseded_generation_that_still_holds_a_lease() {
    let generation =
        Arc::new(Generation::new(2, 2, false).with_drain_limit(Duration::from_secs(60)));
    let session = generation.admit().unwrap();
    generation.select(3).unwrap();
    tokio::time::advance(Duration::from_secs(59)).await;
    assert_eq!(generation.retire(Duration::ZERO), None);
    tokio::time::advance(Duration::from_secs(2)).await;
    // The idle-only check keeps its meaning after the limit.
    assert!(!generation.retire_if_idle(Duration::ZERO));
    assert!(!generation.snapshot().retired);
    assert_eq!(
        generation.retire(Duration::ZERO),
        Some(Retirement::DrainExpired {
            active: 1,
            legacy: 0
        })
    );
    assert!(generation.snapshot().retired);
    assert!(generation.admit().is_none());
    drop(session);
    assert_eq!(generation.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn without_a_drain_limit_a_held_lease_keeps_a_superseded_generation() {
    let generation = Arc::new(Generation::new(2, 2, false));
    let _session = generation.admit().unwrap();
    generation.select(3).unwrap();
    tokio::time::advance(Duration::from_secs(365 * 24 * 60 * 60)).await;
    assert_eq!(generation.retire(Duration::ZERO), None);
    assert!(!generation.snapshot().retired);
}

#[tokio::test(start_paused = true)]
async fn drain_limit_never_retires_the_selected_generation_or_a_live_candidate() {
    let selected = Arc::new(Generation::new(2, 2, false).with_drain_limit(Duration::from_secs(1)));
    let candidate = Arc::new(Generation::new(3, 2, true).with_drain_limit(Duration::from_secs(1)));
    let _held = selected.admit().unwrap();
    let _staging = candidate.admit().unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(selected.retire(Duration::from_secs(30)), None);
    assert_eq!(candidate.retire(Duration::from_secs(30)), None);
}

#[tokio::test(start_paused = true)]
async fn superseded_time_starts_at_the_first_newer_selection() {
    let generation = Generation::new(2, 2, false);
    assert_eq!(generation.snapshot().superseded_for, None);
    tokio::time::advance(Duration::from_secs(7)).await;
    generation.select(3).unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        generation.snapshot().superseded_for,
        Some(Duration::from_secs(5))
    );
    generation.select(4).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        generation.snapshot().superseded_for,
        Some(Duration::from_secs(6))
    );

    let promoted = Generation::new(3, 2, true);
    promoted.select(3).unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(promoted.snapshot().superseded_for, None);

    let started_behind = Generation::new(2, 3, false);
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(
        started_behind.snapshot().superseded_for,
        Some(Duration::from_secs(4))
    );
}

#[tokio::test(start_paused = true)]
async fn run_until_retired_reports_why_it_returned() {
    let draining = Arc::new(Generation::new(2, 3, false).with_drain_limit(Duration::from_secs(10)));
    let _session = draining.admit().unwrap();
    // Paused time advances on its own; the timeout fails the test instead of hanging it.
    let retirement = tokio::time::timeout(
        Duration::from_secs(3600),
        draining.run_until_retired(Duration::from_secs(1), Duration::ZERO, || async {}),
    )
    .await
    .expect("a superseded generation must retire once its drain limit passes");
    assert_eq!(
        retirement,
        Retirement::DrainExpired {
            active: 1,
            legacy: 0
        }
    );

    let idle = Generation::new(2, 3, false);
    let retirement = idle
        .run_until_retired(Duration::from_secs(1), Duration::ZERO, || async {})
        .await;
    assert_eq!(retirement, Retirement::Idle);
}

#[tokio::test(start_paused = true)]
async fn no_refresh_starts_after_the_drain_limit_passes() {
    let generation =
        Arc::new(Generation::new(2, 3, false).with_drain_limit(Duration::from_secs(10)));
    let _session = generation.admit().unwrap();
    let started = tokio::time::Instant::now();
    let calls = std::sync::Mutex::new(Vec::new());
    let retirement = tokio::time::timeout(
        Duration::from_secs(3600),
        generation.run_until_retired(Duration::from_secs(1), Duration::ZERO, || {
            calls.lock().unwrap().push(started.elapsed());
            async {}
        }),
    )
    .await
    .expect("a superseded generation must retire once its drain limit passes");
    assert_eq!(
        retirement,
        Retirement::DrainExpired {
            active: 1,
            legacy: 0
        }
    );
    let calls = calls.into_inner().unwrap();
    assert!(!calls.is_empty());
    assert!(
        calls.iter().all(|at| *at < Duration::from_secs(10)),
        "{calls:?}"
    );
}

#[tokio::test(start_paused = true)]
async fn an_expired_drain_with_no_lease_reports_idle() {
    let generation = Generation::new(2, 3, false).with_drain_limit(Duration::from_secs(1));
    tokio::time::advance(Duration::from_secs(2)).await;
    assert_eq!(generation.retire(Duration::ZERO), Some(Retirement::Idle));
}
