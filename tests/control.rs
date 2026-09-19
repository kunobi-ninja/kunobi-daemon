//! Typed health and drain observations.
#![cfg(feature = "wire-async")]
use kunobi_daemon::{
    Lifecycle, ServiceIdentity,
    control::ControlService,
    wire::{self, capability, operation},
};
use std::sync::Arc;
use tokio::time::{Duration, Instant};

fn offer() -> wire::Hello {
    let id = ServiceIdentity::new([0x51; 16], "test-service", "test", "one").unwrap();
    wire::Hello::new(
        &id,
        capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
        capability::HEALTH | capability::HEALTH_DETAILS,
    )
}
fn request(operation: u32) -> wire::Control {
    wire::Control {
        operation,
        request_id: 81,
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn health_observes_initialization_and_hours_of_drain_without_waiting_for_work() {
    let lifecycle = Arc::new(Lifecycle::default());
    let service = ControlService::new(Arc::clone(&lifecycle), 22, "test-build".into(), 123);
    let health = request(operation::HEALTH);
    let initial = wire::Health::from_response(&service.handle(&health).unwrap(), &health).unwrap();
    assert!(!initial.ready);
    assert!(!initial.draining);
    assert_eq!(initial.process_id, std::process::id());
    assert_eq!(initial.generation, 22);
    service.mark_ready();
    let pending = lifecycle.begin().unwrap();
    let drain = request(operation::DRAIN);
    let reply = wire::Health::from_response(&service.handle(&drain).unwrap(), &drain).unwrap();
    assert!(reply.draining);
    assert!(!reply.ready);
    assert_eq!(reply.active, 1);
    assert!(lifecycle.begin().is_none());
    tokio::time::advance(Duration::from_secs(7200)).await;
    service.mark_ready();
    assert!(!service.snapshot().ready, "drain cannot be reopened");
    assert_eq!(service.snapshot().active, 1);
    drop(pending);
    assert_eq!(service.snapshot().active, 0);
}

#[tokio::test]
async fn binary_control_uses_negotiated_identity_and_correlated_typed_responses() {
    let lifecycle = Arc::new(Lifecycle::default());
    let service = Arc::new(ControlService::new(lifecycle, 4, "test".into(), 8));
    service.mark_ready();
    let (client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(async move {
        service
            .serve(server, &offer(), Instant::now() + Duration::from_secs(1))
            .await
    });
    let mut session = wire::AsyncSession::connect(client, &offer()).await.unwrap();
    let request = request(operation::HEALTH);
    session.send(&request).await.unwrap();
    let reply = session.receive().await.unwrap();
    let health = wire::Health::from_response(&reply, &request).unwrap();
    assert!(health.ready);
    assert_eq!(health.build, "test");
    assert_eq!(health.revision, 8);
    let mut wrong = request;
    wrong.request_id += 1;
    assert!(wire::Health::from_response(&reply, &wrong).is_err());
    task.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn stalled_handshake_is_bounded_without_starting_drain() {
    let lifecycle = Arc::new(Lifecycle::default());
    let service = ControlService::new(Arc::clone(&lifecycle), 0, "test".into(), 0);
    let (_client, server) = tokio::io::duplex(64);
    let error = service
        .serve(server, &offer(), Instant::now() + Duration::from_secs(2))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    assert!(lifecycle.accepting_calls());
    let mut wrong = request(operation::DRAIN);
    wrong.generation = 3;
    assert!(service.handle(&wrong).is_err());
    wrong.generation = 0;
    wrong.payload.push(1);
    assert!(service.handle(&wrong).is_err());
    wrong.payload.clear();
    wrong.kind = wire::MessageKind::Application.into();
    assert!(service.handle(&wrong).is_err());
    assert!(lifecycle.accepting_calls());
}
