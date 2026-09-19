//! Lifecycle scenarios adapted from Kunobi broker/relay conformance and interop.
//! The daemon, relay and test controller are separate operating-system processes.

#![cfg(feature = "async")]

mod support;

use kunobi_daemon::{ProcessLock, ensure_current};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use support::{BUDGET, Fixture, connect, read_line, send, wait_file};
use tokio::time::Instant;

#[test]
#[ignore = "child-process entry point, invoked by the E2E fixtures"]
fn fixture_process() {
    support::child_main();
}

#[tokio::test]
async fn client_eof_preserves_the_late_reply_and_closes_the_same_relay_cleanly() {
    use tokio::io::AsyncWriteExt;
    let fixture = Fixture::new();
    let old = fixture.start(1).await;
    let (relay_pid, mut client) = fixture.relay().await;
    send(&mut client, "CALL 30 hold").await.unwrap();
    wait_file(&fixture.root().join("accepted-ready-30")).await;
    client.get_mut().shutdown().await.unwrap();
    std::fs::write(fixture.root().join("release-30"), b"").unwrap();
    assert_eq!(
        read_line(&mut client).await.unwrap(),
        format!("OK 30 {} 1", old.pid)
    );
    fixture.exited_cleanly(relay_pid).await;
    assert!(fixture.alive(old.pid));
}

#[tokio::test]
async fn drain_finishes_the_old_reply_then_the_same_relay_reaches_the_new_daemon() {
    let fixture = Fixture::new();
    let old = fixture.start(1).await;
    let (relay_pid, mut client) = fixture.relay().await;
    send(&mut client, "CALL 1 hold").await.unwrap();
    wait_file(&fixture.root().join("accepted-ready-1")).await;

    let upgrade = {
        let fixture = Arc::clone(&fixture);
        tokio::spawn(async move {
            ensure_current(
                &fixture.root().join("upgrade.lock"),
                Instant::now() + BUDGET,
                || async { Ok::<_, io::Error>(connect(fixture.root()).await?.map(|(id, _)| id)) },
                |id| id.build == 2,
                || async {
                    fixture.request_drain().await?;
                    let pid = fixture.spawn("daemon", 2);
                    kunobi_daemon::publish_record(
                        &fixture.root().join("candidate.pid"),
                        pid.to_string().as_bytes(),
                    )?;
                    Ok(())
                },
            )
            .await
            .unwrap()
        })
    };
    wait_file(&fixture.root().join(format!("draining-{}", old.pid))).await;
    wait_file(&fixture.root().join("candidate.pid")).await;
    let candidate: u32 = std::fs::read_to_string(fixture.root().join("candidate.pid"))
        .unwrap()
        .parse()
        .unwrap();
    wait_file(&fixture.root().join(format!("attempted-lock-{candidate}"))).await;
    assert!(fixture.alive(old.pid), "old daemon exited before its reply");
    assert!(
        !upgrade.is_finished(),
        "replacement declared ready while the old owner held the lock"
    );
    assert!(
        ProcessLock::try_acquire(fixture.root().join("run.lock"))
            .unwrap()
            .is_none()
    );

    std::fs::write(fixture.root().join("release-1"), b"").unwrap();
    assert_eq!(
        read_line(&mut client).await.unwrap(),
        format!("OK 1 {} 1", old.pid)
    );
    let new = upgrade.await.unwrap();
    assert_eq!(new.pid, candidate);
    assert_ne!(new.pid, old.pid);
    fixture.exited_cleanly(old.pid).await;
    wait_file(
        &fixture
            .root()
            .join(format!("relay-{relay_pid}-connected-{}", new.pid)),
    )
    .await;
    send(&mut client, "CALL 2 ok").await.unwrap();
    assert_eq!(
        read_line(&mut client).await.unwrap(),
        format!("OK 2 {} 2", new.pid)
    );
    assert!(
        fixture.alive(relay_pid),
        "client session required a new relay process"
    );
}

#[tokio::test]
async fn a_crash_reports_the_pending_id_once_without_replaying_it_on_the_replacement() {
    let fixture = Fixture::new();
    let old = fixture.start(1).await;
    let (relay_pid, mut client) = fixture.relay().await;
    send(&mut client, "CALL 10 hold").await.unwrap();
    wait_file(&fixture.root().join("accepted-ready-10")).await;
    fixture.kill(old.pid);
    assert!(
        fixture.root().join("daemon.addr").exists(),
        "crash must leave stale discovery"
    );
    assert_eq!(read_line(&mut client).await.unwrap(), "UNCERTAIN 10");
    let new = ensure_current(
        &fixture.root().join("upgrade.lock"),
        Instant::now() + BUDGET,
        || async { Ok::<_, io::Error>(connect(fixture.root()).await?.map(|(id, _)| id)) },
        |id| id.build == 2,
        || async {
            fixture.spawn("daemon", 2);
            Ok(())
        },
    )
    .await
    .unwrap();
    wait_file(
        &fixture
            .root()
            .join(format!("relay-{relay_pid}-connected-{}", new.pid)),
    )
    .await;
    send(&mut client, "CALL 11 ok").await.unwrap();
    assert_eq!(
        read_line(&mut client).await.unwrap(),
        format!("OK 11 {} 2", new.pid)
    );
    assert_eq!(
        std::fs::read_to_string(fixture.root().join("accepted-10")).unwrap(),
        format!("{}\n", old.pid)
    );
    assert!(fixture.alive(relay_pid));
}

#[tokio::test]
async fn a_compatible_live_daemon_keeps_its_process_and_client_session() {
    let fixture = Fixture::new();
    let old = fixture.start(1).await;
    let (relay_pid, mut client) = fixture.relay().await;
    let accepted = ensure_current(
        &fixture.root().join("upgrade.lock"),
        Instant::now() + BUDGET,
        || async { Ok::<_, io::Error>(connect(fixture.root()).await?.map(|(id, _)| id)) },
        |id| id.build >= 1,
        || async { panic!("compatible build must not be replaced") },
    )
    .await
    .unwrap();
    assert_eq!(accepted, old);
    send(&mut client, "CALL 20 ok").await.unwrap();
    assert_eq!(
        read_line(&mut client).await.unwrap(),
        format!("OK 20 {} 1", old.pid)
    );
    assert!(fixture.alive(relay_pid));
    assert!(
        !fixture
            .root()
            .join(format!("draining-{}", old.pid))
            .exists()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_clients_create_one_replacement_process() {
    let fixture = Fixture::new();
    let old = fixture.start(1).await;
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let replacements = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let fixture = Arc::clone(&fixture);
        let barrier = Arc::clone(&barrier);
        let replacements = Arc::clone(&replacements);
        tasks.push(tokio::spawn(async move {
            let mut first = true;
            ensure_current(
                &fixture.root().join("upgrade.lock"),
                Instant::now() + BUDGET,
                || {
                    let first = std::mem::replace(&mut first, false);
                    let fixture = Arc::clone(&fixture);
                    let barrier = Arc::clone(&barrier);
                    async move {
                        let observed = connect(fixture.root()).await?.map(|(id, _)| id);
                        if first {
                            barrier.wait().await;
                        }
                        Ok::<_, io::Error>(observed)
                    }
                },
                |id| id.build == 2,
                || async {
                    replacements.fetch_add(1, Ordering::SeqCst);
                    fixture.request_drain().await?;
                    fixture.spawn("daemon", 2);
                    Ok(())
                },
            )
            .await
            .unwrap()
        }));
    }
    let mut ids = Vec::new();
    for task in tasks {
        ids.push(task.await.unwrap());
    }
    assert!(
        ids.iter()
            .all(|id| id == &ids[0] && id.pid != old.pid && id.build == 2)
    );
    assert_eq!(replacements.load(Ordering::SeqCst), 1);
    fixture.exited_cleanly(old.pid).await;
}
