//! Regression tests for daemon upgrade.

use kunobi_daemon::{ProcessLock, UpgradeError, ensure_current};
use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

#[tokio::test(start_paused = true)]
async fn each_upgrade_phase_consumes_the_original_budget() {
    let dir = tempfile::tempdir().unwrap();
    let start = Instant::now();
    let result = ensure_current(
        &dir.path().join("upgrade.lock"),
        start + Duration::from_secs(1),
        || async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok::<_, Infallible>(None::<u8>)
        },
        |_| true,
        || async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok(())
        },
    )
    .await;
    assert!(matches!(result, Err(UpgradeError::Deadline)));
    assert!(start.elapsed() < Duration::from_millis(1100));
}

#[tokio::test]
async fn probe_and_filesystem_errors_are_preserved_without_replacing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("missing/upgrade.lock");
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || async { Err::<Option<u8>, _>("permission denied") },
        |_| true,
        || async { panic!("failed probe must stop replacement") },
    )
    .await;
    assert!(matches!(
        result,
        Err(UpgradeError::Probe("permission denied"))
    ));
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || async { Ok::<_, Infallible>(None::<u8>) },
        |_| true,
        || async { panic!("missing lock parent must stop replacement") },
    )
    .await;
    assert!(matches!(result, Err(UpgradeError::Lock(_))));
}

#[tokio::test]
async fn current_daemon_needs_no_lock_or_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let result = ensure_current(
        &dir.path().join("missing/upgrade.lock"),
        Instant::now() + Duration::from_secs(1),
        || async { Ok::<_, Infallible>(Some(42)) },
        |version| *version == 42,
        || async { panic!("already current") },
    )
    .await;
    assert_eq!(result.unwrap(), 42);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_upgraders_reprobe_under_lock_and_replace_once() {
    let dir = tempfile::tempdir().unwrap();
    let version = Arc::new(AtomicUsize::new(1));
    let replacements = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let path = dir.path().join("upgrade.lock");
        let version = Arc::clone(&version);
        let replacements = Arc::clone(&replacements);
        let barrier = Arc::clone(&barrier);
        tasks.push(tokio::spawn(async move {
            let mut first = true;
            ensure_current(
                &path,
                Instant::now() + Duration::from_secs(5),
                || {
                    let version = Arc::clone(&version);
                    let barrier = Arc::clone(&barrier);
                    let first = std::mem::replace(&mut first, false);
                    async move {
                        let observed = version.load(Ordering::SeqCst);
                        if first {
                            barrier.wait().await;
                        }
                        Ok::<_, Infallible>(Some(observed))
                    }
                },
                |v| *v == 2,
                || async {
                    replacements.fetch_add(1, Ordering::SeqCst);
                    version.store(2, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
            .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), 2);
    }
    assert_eq!(replacements.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn successful_spawn_with_only_stale_replies_is_a_failure() {
    let dir = tempfile::tempdir().unwrap();
    let result = ensure_current(
        &dir.path().join("upgrade.lock"),
        Instant::now() + Duration::from_secs(1),
        || async { Ok::<_, Infallible>(Some(1)) },
        |v| *v == 2,
        || async { Ok(()) },
    )
    .await;
    assert!(matches!(result, Err(UpgradeError::Deadline)));
}

#[tokio::test(start_paused = true)]
async fn lock_wait_and_replacement_use_the_same_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.lock");
    let owner = ProcessLock::try_acquire(&path).unwrap().unwrap();
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || async { Ok::<_, Infallible>(None::<u8>) },
        |_| true,
        || async { panic!("must not replace while another upgrader owns the lock") },
    )
    .await;
    assert!(matches!(result, Err(UpgradeError::Deadline)));
    drop(owner);
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || async { Ok::<_, Infallible>(None::<u8>) },
        |_| true,
        std::future::pending,
    )
    .await;
    assert!(matches!(result, Err(UpgradeError::Deadline)));
    assert!(ProcessLock::try_acquire(&path).unwrap().is_some());
}

#[tokio::test]
async fn errors_never_fall_back_to_a_retiring_daemon_response() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.lock");
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || async { Ok(Some(1)) },
        |v| *v == 2,
        || async { Err("service failed") },
    )
    .await;
    assert!(matches!(
        result,
        Err(UpgradeError::Replace("service failed"))
    ));
    let mut probes = 0;
    let result = ensure_current(
        &path,
        Instant::now() + Duration::from_secs(1),
        || {
            probes += 1;
            let response = if probes <= 2 {
                Ok(Some(1))
            } else {
                Err("connection lost")
            };
            async move { response }
        },
        |v| *v == 2,
        || async { Ok(()) },
    )
    .await;
    assert!(matches!(
        result,
        Err(UpgradeError::Probe("connection lost"))
    ));
    assert!(ProcessLock::try_acquire(&path).unwrap().is_some());
}

#[tokio::test]
async fn cancelled_replacement_releases_upgrade_lock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("upgrade.lock");
    let (started, receive) = tokio::sync::oneshot::channel();
    let task = {
        let path = path.clone();
        tokio::spawn(async move {
            ensure_current(
                &path,
                Instant::now() + Duration::from_secs(60),
                || async { Ok::<_, Infallible>(None::<u8>) },
                |_| true,
                || async {
                    started.send(()).unwrap();
                    std::future::pending().await
                },
            )
            .await
        })
    };
    receive.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(ProcessLock::try_acquire(&path).unwrap().is_some());
}
