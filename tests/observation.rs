//! Admission isolation and non-blocking local observations.
use kunobi_daemon::{
    admission::{Admission, Limits, Pool},
    observation::{Event, Observations, ObservedIo},
};
use std::{
    io::{self, Read, Write},
    sync::{Arc, mpsc},
    time::Duration,
};

#[test]
fn full_application_pool_cannot_starve_control_and_every_pool_is_bounded() {
    for capacity in [64, 128, 256] {
        let admission = Arc::new(Admission::new(Limits {
            application: capacity,
            control: 2,
            handshakes: 1,
        }));
        let sessions: Vec<_> = (0..capacity)
            .map(|_| admission.try_acquire(Pool::Application).unwrap())
            .collect();
        assert!(admission.try_acquire(Pool::Application).is_none());
        let controls: Vec<_> = (0..2)
            .map(|_| admission.try_acquire(Pool::Control).unwrap())
            .collect();
        assert!(admission.try_acquire(Pool::Control).is_none());
        let pending = admission.try_acquire(Pool::Handshake).unwrap();
        assert!(admission.try_acquire(Pool::Handshake).is_none());
        assert_eq!(admission.snapshot(Pool::Application).active, capacity);
        assert_eq!(admission.snapshot(Pool::Application).rejected, 1);
        drop((sessions, controls, pending));
        for pool in [Pool::Application, Pool::Control, Pool::Handshake] {
            assert_eq!(admission.snapshot(pool).active, 0);
            assert!(admission.try_acquire(pool).is_some());
        }
    }
}

#[test]
fn concurrent_admission_never_exceeds_capacity_and_zero_disables_a_pool() {
    let admission = Arc::new(Admission::new(Limits {
        application: 3,
        control: 0,
        handshakes: 0,
    }));
    let barrier = Arc::new(std::sync::Barrier::new(17));
    let threads: Vec<_> = (0..16)
        .map(|_| {
            let (admission, barrier) = (Arc::clone(&admission), Arc::clone(&barrier));
            std::thread::spawn(move || {
                let permit = admission.try_acquire(Pool::Application);
                barrier.wait();
                barrier.wait();
                drop(permit);
            })
        })
        .collect();
    barrier.wait();
    assert_eq!(admission.snapshot(Pool::Application).active, 3);
    assert_eq!(admission.snapshot(Pool::Application).rejected, 13);
    assert!(admission.try_acquire(Pool::Control).is_none());
    barrier.wait();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(admission.snapshot(Pool::Application).active, 0);
}

struct HeldWriter {
    entered: mpsc::Sender<()>,
    release: mpsc::Receiver<()>,
}
impl Write for HeldWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.entered.send(()).unwrap();
        self.release.recv().unwrap();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Err(io::ErrorKind::TimedOut.into())
    }
}
#[test]
fn blocked_writer_remains_observable_without_interrupting_long_work() {
    let observations = Arc::new(Observations::new(3));
    let (entered, waiting) = mpsc::channel();
    let (release, receiver) = mpsc::channel();
    let mut io = ObservedIo::new(
        HeldWriter {
            entered,
            release: receiver,
        },
        Arc::clone(&observations),
    );
    let thread = std::thread::spawn(move || {
        io.write_all(b"hello").unwrap();
        assert_eq!(io.flush().unwrap_err().kind(), io::ErrorKind::TimedOut);
    });
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    let snapshot = observations.snapshot();
    assert_eq!(snapshot.connections, 1);
    assert_eq!(snapshot.write.pending, 1);
    assert!(snapshot.write.busy_for.is_some());
    assert_eq!(snapshot.write.bytes, 0);
    assert_eq!(snapshot.write.since_progress, None);
    release.send(()).unwrap();
    thread.join().unwrap();
    let snapshot = observations.snapshot();
    assert_eq!(snapshot.connections, 0);
    assert_eq!(snapshot.write.pending, 0);
    assert!(snapshot.write.busy_for.is_none());
    assert_eq!(snapshot.write.bytes, 5);
    assert_eq!(snapshot.write.errors, 1);
    assert!(snapshot.write.since_progress.is_some());
    let events = observations.take_events();
    assert!(matches!(
        events[1].event,
        Event::IoFailed {
            kind: io::ErrorKind::TimedOut,
            ..
        }
    ));
    assert_eq!(events[2].event, Event::Disconnected);
}

#[test]
fn event_backpressure_does_not_block_io_and_eof_is_reported_once() {
    let observations = Arc::new(Observations::new(1));
    let mut io = ObservedIo::new(io::Cursor::new(b"data"), Arc::clone(&observations));
    let mut bytes = Vec::new();
    io.read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes, b"data");
    io.read_exact(&mut []).unwrap();
    assert_eq!(io.read(&mut [0]).unwrap(), 0);
    assert_eq!(observations.snapshot().read.bytes, 4);
    assert_eq!(observations.snapshot().lost_events, 1);
    assert_eq!(observations.take_events()[0].event, Event::Connected);
    drop(io);
    assert_eq!(observations.take_events()[0].event, Event::Disconnected);
}

#[cfg(feature = "async")]
#[tokio::test(start_paused = true)]
async fn drain_observations_survive_a_long_operation_and_timeout() {
    let observations = Arc::new(Observations::default());
    let lifecycle = Arc::new(kunobi_daemon::Lifecycle::observed(Arc::clone(
        &observations,
    )));
    let work = lifecycle.begin().unwrap();
    assert_eq!(lifecycle.snapshot().active, 1);
    assert_eq!(
        lifecycle
            .drain_until(tokio::time::Instant::now() + Duration::from_secs(7200))
            .await,
        kunobi_daemon::DrainOutcome::TimedOut { active: 1 }
    );
    assert_eq!(
        lifecycle.snapshot().draining_for,
        Some(Duration::from_secs(7200))
    );
    assert_eq!(lifecycle.snapshot().active, 1);
    assert!(lifecycle.begin().is_none());
    drop(work);
    lifecycle.drain().await;
    assert_eq!(lifecycle.snapshot().active, 0);
    assert_eq!(
        observations
            .take_events()
            .iter()
            .map(|event| event.event)
            .collect::<Vec<_>>(),
        vec![
            Event::DrainStarted,
            Event::DrainTimedOut,
            Event::DrainCompleted
        ]
    );
}

#[cfg(feature = "wire-async")]
#[tokio::test]
async fn async_pending_io_and_cancellation_remain_observable_until_transport_drop() {
    use kunobi_daemon::observation::AsyncObservedIo;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let observations = Arc::new(Observations::default());
    let (client, mut peer) = tokio::io::duplex(1);
    let mut client = AsyncObservedIo::new(client, Arc::clone(&observations));
    client.write_all(b"a").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), client.write_all(b"b"))
            .await
            .is_err()
    );
    assert_eq!(observations.snapshot().write.bytes, 1);
    assert_eq!(observations.snapshot().write.pending, 1);
    assert_eq!(peer.read_u8().await.unwrap(), b'a');
    client.write_all(b"b").await.unwrap();
    assert_eq!(observations.snapshot().write.pending, 0);
    assert_eq!(observations.snapshot().write.bytes, 2);
    peer.write_all(b"z").await.unwrap();
    assert_eq!(client.read_u8().await.unwrap(), b'z');
    assert_eq!(observations.snapshot().read.bytes, 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), client.read_u8())
            .await
            .is_err()
    );
    assert_eq!(observations.snapshot().read.pending, 1);
    drop(client);
    assert_eq!(observations.snapshot().read.pending, 0);
    assert_eq!(observations.snapshot().connections, 0);
}
