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

fn watch_offer() -> wire::Hello {
    let mut offer = offer();
    offer.supported |= capability::WATCH | capability::APPLICATION;
    offer
}

#[tokio::test(start_paused = true)]
async fn watch_streams_ready_committed_selection_and_drain_without_idle_polling() {
    use kunobi_daemon::control::WatchClient;
    use wire::LifecycleChange;
    let lifecycle = Arc::new(Lifecycle::default());
    let service = Arc::new(ControlService::new(
        Arc::clone(&lifecycle),
        4,
        "old".into(),
        8,
    ));
    // A staged candidate must report the incumbent, not imply it was selected.
    assert!(
        service
            .selection_committed(4, "wrong-build".into())
            .is_err()
    );
    service.selection_committed(3, "incumbent".into()).unwrap();
    service.selection_committed(3, "incumbent".into()).unwrap();
    let (client, server) = tokio::io::duplex(1024);
    let serving = Arc::clone(&service);
    let task = tokio::spawn(async move {
        serving
            .serve(
                server,
                &watch_offer(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    let mut client = WatchClient::connect(
        client,
        &watch_offer(),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(client.snapshot().change, LifecycleChange::Snapshot);
    assert_eq!(client.snapshot().selected_generation, Some(3));
    assert!(!client.snapshot().ready);
    // The setup deadline does not become a subscription lifetime or heartbeat.
    tokio::time::advance(Duration::from_secs(3600)).await;
    assert!(!task.is_finished());
    service.mark_ready();
    assert_eq!(
        client.changed().await.unwrap().change,
        LifecycleChange::Ready
    );
    service.selection_committed(5, "new".into()).unwrap();
    let event = client.changed().await.unwrap();
    assert_eq!(event.change, LifecycleChange::SelectionChanged);
    assert_eq!(event.selected_generation, Some(5));
    assert_eq!(event.selected_build, "new");
    assert_eq!(event.generation, 4);
    assert!(service.selection_committed(3, "stale".into()).is_err());
    assert!(service.selection_committed(5, "different".into()).is_err());
    lifecycle.start_drain();
    let event = client.changed().await.unwrap();
    assert_eq!(event.change, LifecycleChange::Draining);
    assert!(!event.ready);
    service.mark_retiring();
    assert_eq!(
        client.changed().await.unwrap().change,
        LifecycleChange::Retiring
    );
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn late_watchers_get_current_state_and_slow_watchers_get_coalesced_updates() {
    use kunobi_daemon::control::WatchClient;
    let service = Arc::new(ControlService::new(
        Arc::new(Lifecycle::default()),
        1,
        "first".into(),
        0,
    ));
    service.mark_ready();
    service.selection_committed(2, "second".into()).unwrap();
    let (client, server) = tokio::io::duplex(128);
    let serving = Arc::clone(&service);
    let task = tokio::spawn(async move {
        serving
            .serve(
                server,
                &watch_offer(),
                Instant::now() + Duration::from_secs(2),
            )
            .await
    });
    let mut client = WatchClient::connect(
        client,
        &watch_offer(),
        Instant::now() + Duration::from_secs(2),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(client.snapshot().ready);
    assert_eq!(client.snapshot().selected_generation, Some(2));
    // No task can run between these publications. Only the last selection
    // needs to be retained; WATCH is current state, not an audit log.
    for generation in 3..1000 {
        service
            .selection_committed(generation, generation.to_string())
            .unwrap();
    }
    service.mark_retiring();
    let event = client.changed().await.unwrap();
    assert_eq!(event.selected_generation, Some(999));
    assert_eq!(event.selected_build, "999");
    assert!(event.retiring && event.draining && !event.ready);
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn watch_falls_back_only_when_the_peer_does_not_negotiate_it() {
    use kunobi_daemon::control::WatchClient;
    let (client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        let mut session = wire::AsyncSession::accept(server, &offer()).await.unwrap();
        assert!(
            session.receive().await.is_err(),
            "client sent WATCH to a legacy peer"
        );
    });
    assert!(
        WatchClient::connect(
            client,
            &watch_offer(),
            Instant::now() + Duration::from_secs(1)
        )
        .await
        .unwrap()
        .is_none()
    );
    task.await.unwrap();

    let (client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        let mut session = wire::AsyncSession::accept(server, &watch_offer())
            .await
            .unwrap();
        let request = session.receive().await.unwrap();
        assert_eq!(request.operation, operation::WATCH);
        let reply = wire::Control {
            operation: operation::WATCH,
            request_id: request.request_id,
            payload: vec![0xff],
            ..Default::default()
        };
        session.send(&reply).await.unwrap();
    });
    assert!(
        WatchClient::connect(
            client,
            &watch_offer(),
            Instant::now() + Duration::from_secs(1)
        )
        .await
        .is_err(),
        "an advertised malformed stream must not downgrade"
    );
    task.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn watch_releases_a_subscriber_that_stops_reading() {
    let service = Arc::new(ControlService::new(
        Arc::new(Lifecycle::default()),
        1,
        "large-build-id".repeat(512),
        0,
    ));
    let (client, server) = tokio::io::duplex(128);
    let task = tokio::spawn(async move {
        service
            .serve(
                server,
                &watch_offer(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    let mut client = wire::AsyncSession::connect(client, &watch_offer())
        .await
        .unwrap();
    client.send(&request(operation::WATCH)).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(6), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    drop(client);
}

#[tokio::test]
async fn invalid_watch_events_are_terminal_and_cannot_resume_the_stream() {
    use buffa::Message;
    use kunobi_daemon::control::WatchClient;
    for fault in 0..21 {
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let mut session = wire::AsyncSession::accept(server, &watch_offer())
                .await
                .unwrap();
            let request = session.receive().await.unwrap();
            let mut event = wire::LifecycleEvent {
                sequence: 1,
                process_id: 123,
                generation: 4,
                build: "own".into(),
                selected_generation: Some(5),
                selected_build: "selected".into(),
                ..Default::default()
            };
            if fault == 18 {
                event.selected_generation = None;
                event.selected_build.clear();
            }
            if fault == 19 || fault == 20 {
                event.draining = true;
                event.retiring = fault == 20;
            }
            let mut reply = wire::Control {
                operation: operation::WATCH,
                request_id: request.request_id,
                generation: 4,
                payload: event.encode_to_vec(),
                ..Default::default()
            };
            session.send(&reply).await.unwrap();
            event.sequence = 2;
            match fault {
                0 => event.sequence = 3,
                1 => event.process_id += 1,
                2 => event.build = "other".into(),
                3 => event.selected_generation = Some(4),
                4 => event.selected_build = "replaced-at-same-epoch".into(),
                5 => {
                    event.ready = true;
                    event.draining = true;
                }
                6 => event.retiring = true,
                7 => event.change = 999.into(),
                8 => reply.request_id += 1,
                9 => reply.token.push(1),
                10 => {
                    event.selected_generation = None;
                    event.selected_build.clear();
                }
                11 => reply.generation += 1,
                12 => reply.kind = wire::MessageKind::Application.into(),
                13 => reply.operation = operation::HEALTH,
                14 => reply.offset = Some(0),
                15 => event.change = wire::LifecycleChange::Ready.into(),
                16 => event.change = wire::LifecycleChange::Draining.into(),
                17 => event.change = wire::LifecycleChange::Retiring.into(),
                18 => {
                    event.change = wire::LifecycleChange::SelectionChanged.into();
                    event.selected_generation = None;
                    event.selected_build.clear();
                }
                19 => event.draining = false,
                20 => event.retiring = false,
                _ => unreachable!(),
            }
            reply.payload = event.encode_to_vec();
            session.send(&reply).await.unwrap();
        });
        let mut client = WatchClient::connect(
            client,
            &watch_offer(),
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            client.changed().await.is_err(),
            "accepted invalid event {fault}"
        );
        assert!(
            client
                .changed()
                .await
                .unwrap_err()
                .to_string()
                .contains("WATCH failed")
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn watch_rejects_requests_with_payloads_or_followup_bytes() {
    for payload in [true, false] {
        let service = ControlService::new(Arc::new(Lifecycle::default()), 1, "test".into(), 0);
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            service
                .serve(
                    server,
                    &watch_offer(),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        let mut client = wire::AsyncSession::connect(client, &watch_offer())
            .await
            .unwrap();
        let mut subscribe = request(operation::WATCH);
        if payload {
            subscribe.payload.push(1);
        }
        client.send(&subscribe).await.unwrap();
        if !payload {
            client.receive().await.unwrap();
            client.send(&request(operation::HEALTH)).await.unwrap();
        }
        assert_eq!(
            task.await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_watch_read_requires_reconnection() {
    use kunobi_daemon::control::WatchClient;
    let service = Arc::new(ControlService::new(
        Arc::new(Lifecycle::default()),
        4,
        "test".into(),
        8,
    ));
    let (client, server) = tokio::io::duplex(1024);
    let serving = Arc::clone(&service);
    let task = tokio::spawn(async move {
        serving
            .serve(
                server,
                &watch_offer(),
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    let mut client = WatchClient::connect(
        client,
        &watch_offer(),
        Instant::now() + Duration::from_secs(1),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(client.snapshot().selected_generation, None);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), client.changed())
            .await
            .is_err()
    );
    service.mark_ready();
    assert!(
        client
            .changed()
            .await
            .unwrap_err()
            .to_string()
            .contains("reconnect")
    );
    drop(client);
    // The peer may observe either EOF or a broken pipe if readiness was in flight.
    let _ = task.await.unwrap();
}

#[tokio::test]
async fn watch_initial_snapshot_has_coherent_identity_and_selection() {
    use buffa::Message;
    use kunobi_daemon::control::WatchClient;
    for fault in 0..6 {
        let (client, server) = tokio::io::duplex(4096);
        let task = tokio::spawn(async move {
            let mut session = wire::AsyncSession::accept(server, &watch_offer())
                .await
                .unwrap();
            let request = session.receive().await.unwrap();
            let mut event = wire::LifecycleEvent {
                sequence: 1,
                process_id: 123,
                generation: 4,
                build: "own".into(),
                selected_generation: Some(4),
                selected_build: "own".into(),
                ..Default::default()
            };
            match fault {
                0 => event.sequence = 2,
                1 => {
                    event.change = wire::LifecycleChange::Ready.into();
                    event.ready = true;
                }
                2 => event.process_id = 0,
                3 => event.selected_generation = None,
                4 => event.selected_build = "wrong-build".into(),
                _ => {} // The selected serving generation must also be accepted.
            }
            session
                .send(&wire::Control {
                    operation: operation::WATCH,
                    request_id: request.request_id,
                    generation: 4,
                    payload: event.encode_to_vec(),
                    ..Default::default()
                })
                .await
                .unwrap();
        });
        let result = WatchClient::connect(
            client,
            &watch_offer(),
            Instant::now() + Duration::from_secs(2),
        )
        .await;
        if fault == 5 {
            assert!(result.unwrap().is_some());
        } else {
            assert!(result.is_err(), "accepted invalid initial snapshot {fault}");
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn watch_checks_its_own_offer_and_needs_no_unrelated_capabilities() {
    use kunobi_daemon::control::WatchClient;
    let (client, _server) = tokio::io::duplex(128);
    let result =
        WatchClient::connect(client, &offer(), Instant::now() + Duration::from_millis(20)).await;
    assert_eq!(
        result.err().unwrap().kind(),
        std::io::ErrorKind::InvalidData
    );
    let mut only_watch = watch_offer();
    only_watch.supported = capability::WATCH;
    only_watch.required = capability::WATCH;
    let server_offer = only_watch.clone();
    let (client, server) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        ControlService::new(Arc::new(Lifecycle::default()), 4, "own".into(), 0)
            .serve(
                server,
                &server_offer,
                Instant::now() + Duration::from_secs(1),
            )
            .await
    });
    let client = WatchClient::connect(client, &only_watch, Instant::now() + Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    drop(client);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn watch_request_fields_are_validated_before_subscribing() {
    for case in 0..5 {
        let (client, server) = tokio::io::duplex(1024);
        let task = tokio::spawn(async move {
            ControlService::new(Arc::new(Lifecycle::default()), 4, "own".into(), 0)
                .serve(
                    server,
                    &watch_offer(),
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        let mut session = wire::AsyncSession::connect(client, &watch_offer())
            .await
            .unwrap();
        let mut request = request(operation::WATCH);
        match case {
            0 => request.token.push(1),
            1 => request.offset = Some(0),
            2 => request.generation = 3,
            3 => request.generation = 4,
            _ => {}
        }
        session.send(&request).await.unwrap();
        if case >= 3 {
            let reply = session.receive().await.unwrap();
            assert_eq!(reply.generation, 4);
            assert_eq!(reply.request_id, request.request_id);
            drop(session);
            task.await.unwrap().unwrap();
        } else {
            assert_eq!(
                task.await.unwrap().unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }
}
