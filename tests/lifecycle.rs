//! Regression tests for daemon lifecycle.

use kunobi_daemon::{DrainOutcome, Lifecycle};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[tokio::test]
async fn closes_existing_sessions_and_waits_for_every_reply_guard() {
    let lifecycle = Arc::new(Lifecycle::default());
    let first = lifecycle.begin().unwrap();
    let second = lifecycle.begin().unwrap();
    assert!(lifecycle.start_drain());
    assert!(!lifecycle.start_drain());
    assert!(lifecycle.begin().is_none());
    let waiter = {
        let lifecycle = Arc::clone(&lifecycle);
        tokio::spawn(async move {
            lifecycle
                .drain_until(Instant::now() + Duration::from_secs(5))
                .await
        })
    };
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(first);
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(second);
    assert_eq!(waiter.await.unwrap(), DrainOutcome::Complete);
}

#[tokio::test(start_paused = true)]
async fn timeout_preserves_work_and_can_be_followed_by_a_successful_drain() {
    let lifecycle = Arc::new(Lifecycle::default());
    let work = lifecycle.begin().unwrap();
    assert_eq!(
        lifecycle
            .drain_until(Instant::now() + Duration::from_secs(1))
            .await,
        DrainOutcome::TimedOut { active: 1 }
    );
    assert!(lifecycle.begin().is_none());
    drop(work);
    assert_eq!(
        lifecycle.drain_until(Instant::now()).await,
        DrainOutcome::Complete
    );
}

#[tokio::test]
async fn early_and_late_shutdown_observers_both_wake() {
    let lifecycle = Arc::new(Lifecycle::default());
    let mut observers = Vec::new();
    for _ in 0..4 {
        let lifecycle = Arc::clone(&lifecycle);
        observers.push(tokio::spawn(async move { lifecycle.draining().await }));
    }
    tokio::task::yield_now().await;
    assert!(observers.iter().all(|task| !task.is_finished()));
    lifecycle.start_drain();
    for task in observers {
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(1), lifecycle.draining())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn simultaneous_completion_wakes_all_drainers() {
    let lifecycle = Arc::new(Lifecycle::default());
    let guards: Vec<_> = (0..32).map(|_| lifecycle.begin().unwrap()).collect();
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let lifecycle = Arc::clone(&lifecycle);
        waiters.push(tokio::spawn(async move {
            lifecycle
                .drain_until(Instant::now() + Duration::from_secs(5))
                .await
        }));
    }
    lifecycle.draining().await;
    for guard in guards {
        tokio::spawn(async move { drop(guard) });
    }
    for waiter in waiters {
        assert_eq!(waiter.await.unwrap(), DrainOutcome::Complete);
    }
}

#[tokio::test]
async fn cancelled_waiter_does_not_reopen_admission() {
    let lifecycle = Arc::new(Lifecycle::default());
    let work = lifecycle.begin().unwrap();
    let task = {
        let lifecycle = Arc::clone(&lifecycle);
        tokio::spawn(async move {
            lifecycle
                .drain_until(Instant::now() + Duration::from_secs(60))
                .await
        })
    };
    lifecycle.draining().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(lifecycle.begin().is_none());
    drop(work);
    assert_eq!(
        lifecycle.drain_until(Instant::now()).await,
        DrainOutcome::Complete
    );
}
