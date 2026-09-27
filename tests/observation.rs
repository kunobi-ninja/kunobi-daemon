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

/// Admission and observation counters against plain models, over generated
/// operation sequences.
mod properties {
    use super::*;
    use kunobi_daemon::{
        admission::{Permit, PoolSnapshot},
        observation::Direction,
    };
    use proptest::prelude::*;
    use std::collections::VecDeque;

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    const POOLS: [Pool; 3] = [Pool::Handshake, Pool::Application, Pool::Control];

    #[derive(Clone, Debug)]
    enum AdmissionOp {
        Acquire(usize),
        /// Drop one held permit of the pool, if any.
        Release(usize, usize),
    }

    fn admission_op() -> impl Strategy<Value = AdmissionOp> {
        prop_oneof![
            2 => (0usize..3).prop_map(AdmissionOp::Acquire),
            1 => (0usize..3, any::<usize>())
                .prop_map(|(pool, index)| AdmissionOp::Release(pool, index)),
        ]
    }

    #[derive(Clone, Debug)]
    enum QueueOp {
        Record(Event),
        Take,
    }

    fn queue_op() -> impl Strategy<Value = QueueOp> {
        let event = proptest::sample::select(vec![
            Event::Rejected,
            Event::DrainStarted,
            Event::DrainCompleted,
            Event::HandoffStarted,
        ]);
        prop_oneof![3 => event.prop_map(QueueOp::Record), 1 => Just(QueueOp::Take)]
    }

    /// A byte count the transport accepts, or the error it fails with.
    fn outcome() -> impl Strategy<Value = Result<usize, io::ErrorKind>> {
        prop_oneof![
            3 => (0usize..64).prop_map(Ok),
            1 => proptest::sample::select(vec![
                io::ErrorKind::Interrupted,
                io::ErrorKind::WouldBlock,
                io::ErrorKind::BrokenPipe,
                io::ErrorKind::ConnectionReset,
                io::ErrorKind::TimedOut,
            ])
            .prop_map(Err),
        ]
    }

    /// Answers each call from a script; more calls than scripted see EOF.
    struct Scripted {
        reads: VecDeque<Result<usize, io::ErrorKind>>,
        writes: VecDeque<Result<usize, io::ErrorKind>>,
    }

    impl Read for Scripted {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.reads.pop_front().unwrap_or(Ok(0))?;
            let n = n.min(buf.len());
            buf[..n].fill(7);
            Ok(n)
        }
    }

    impl Write for Scripted {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let n = self.writes.pop_front().unwrap_or(Ok(0))?;
            Ok(n.min(buf.len()))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// What the wrapper must count for a script: bytes moved, and failures
    /// other than the retryable Interrupted and WouldBlock.
    fn expected(
        script: &[Result<usize, io::ErrorKind>],
        buffer: usize,
    ) -> (u64, Vec<io::ErrorKind>) {
        let bytes = script.iter().flatten().map(|&n| n.min(buffer) as u64).sum();
        let failures = script
            .iter()
            .filter_map(|outcome| outcome.err())
            .filter(|kind| !matches!(kind, io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock))
            .collect();
        (bytes, failures)
    }

    proptest! {
        #![proptest_config(config())]

        #[test]
        fn each_pool_admits_up_to_its_own_limit_and_counts_every_refusal(
            limits in [0usize..4, 0usize..4, 0usize..4],
            ops in proptest::collection::vec(admission_op(), 0..64),
        ) {
            let admission = Arc::new(Admission::new(Limits {
                handshakes: limits[0],
                application: limits[1],
                control: limits[2],
            }));
            let mut held: [Vec<Permit>; 3] = Default::default();
            let mut rejected = [0u64; 3];
            for op in ops {
                match op {
                    AdmissionOp::Acquire(pool) => match admission.try_acquire(POOLS[pool]) {
                        Some(permit) => {
                            prop_assert!(held[pool].len() < limits[pool]);
                            held[pool].push(permit);
                        }
                        None => {
                            prop_assert_eq!(held[pool].len(), limits[pool]);
                            rejected[pool] += 1;
                        }
                    },
                    AdmissionOp::Release(pool, index) => {
                        if !held[pool].is_empty() {
                            let index = index % held[pool].len();
                            drop(held[pool].swap_remove(index));
                        }
                    }
                }
                for (index, pool) in POOLS.into_iter().enumerate() {
                    prop_assert_eq!(
                        admission.snapshot(pool),
                        PoolSnapshot {
                            limit: limits[index],
                            active: held[index].len(),
                            rejected: rejected[index],
                        }
                    );
                }
            }
        }

        #[test]
        fn the_event_queue_keeps_the_first_events_up_to_capacity_and_counts_the_rest(
            capacity in 0usize..5,
            ops in proptest::collection::vec(queue_op(), 0..48),
        ) {
            let observations = Observations::new(capacity);
            let mut queued = Vec::new();
            let mut lost = 0;
            for op in ops {
                match op {
                    QueueOp::Record(event) => {
                        observations.record(event);
                        // Zero capacity disables recording; nothing counts as lost.
                        if capacity > 0 {
                            if queued.len() < capacity {
                                queued.push(event);
                            } else {
                                lost += 1;
                            }
                        }
                    }
                    QueueOp::Take => {
                        let taken = observations.take_events();
                        prop_assert!(
                            taken.windows(2).all(|pair| pair[0].elapsed <= pair[1].elapsed)
                        );
                        let taken: Vec<Event> =
                            taken.into_iter().map(|timed| timed.event).collect();
                        prop_assert_eq!(taken, std::mem::take(&mut queued));
                    }
                }
                prop_assert_eq!(observations.snapshot().lost_events, lost);
            }
        }

        #[test]
        fn observed_io_passes_results_through_and_counts_exactly_what_moved(
            reads in proptest::collection::vec(outcome(), 0..16),
            writes in proptest::collection::vec(outcome(), 0..16),
        ) {
            const BUFFER: usize = 32;
            let observations = Arc::new(Observations::new(256));
            let mut observed = ObservedIo::new(
                Scripted {
                    reads: reads.iter().copied().collect(),
                    writes: writes.iter().copied().collect(),
                },
                Arc::clone(&observations),
            );
            let mut buffer = [0; BUFFER];
            for scripted in &reads {
                let result = observed.read(&mut buffer).map_err(|error| error.kind());
                prop_assert_eq!(result, scripted.map(|n| n.min(BUFFER)));
            }
            for scripted in &writes {
                let result = observed.write(&buffer).map_err(|error| error.kind());
                prop_assert_eq!(result, scripted.map(|n| n.min(BUFFER)));
            }

            let (read_bytes, read_failures) = expected(&reads, BUFFER);
            let (write_bytes, write_failures) = expected(&writes, BUFFER);
            let snapshot = observations.snapshot();
            prop_assert_eq!(snapshot.connections, 1);
            for (progress, bytes, failures) in [
                (snapshot.read, read_bytes, &read_failures),
                (snapshot.write, write_bytes, &write_failures),
            ] {
                prop_assert_eq!(progress.bytes, bytes);
                prop_assert_eq!(progress.errors, failures.len() as u64);
                prop_assert_eq!(progress.pending, 0);
                prop_assert_eq!(progress.busy_for, None);
                prop_assert_eq!(progress.since_progress.is_some(), bytes > 0);
            }

            drop(observed);
            prop_assert_eq!(observations.snapshot().connections, 0);
            let events: Vec<Event> = observations
                .take_events()
                .into_iter()
                .map(|timed| timed.event)
                .collect();
            let failed = |direction: Direction| -> Vec<io::ErrorKind> {
                events
                    .iter()
                    .filter_map(|event| match *event {
                        Event::IoFailed { direction: seen, kind } if seen == direction => {
                            Some(kind)
                        }
                        _ => None,
                    })
                    .collect()
            };
            prop_assert_eq!(failed(Direction::Read), read_failures);
            prop_assert_eq!(failed(Direction::Write), write_failures);
            // The first empty read reports the close; later ones do not repeat it.
            let closes = events.iter().filter(|event| **event == Event::ReadClosed).count();
            prop_assert_eq!(closes, usize::from(reads.contains(&Ok(0))));
            prop_assert_eq!(events.first(), Some(&Event::Connected));
            prop_assert_eq!(events.last(), Some(&Event::Disconnected));
        }
    }
}
