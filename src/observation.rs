//! Local observations. Consumers own logging, metrics, export and retention.
//!
//! Snapshots never take an I/O lock. Events have a fixed capacity and producers
//! never wait for the reader: contention or a full queue increments `lost_events`.
//! No callback, background worker, payload or credential is retained here.
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// Direction of a transport operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    /// Read from the peer.
    Read,
    /// Write or flush to the peer.
    Write,
}
/// Bounded event values; no free-form message or client identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// An observed transport was wrapped.
    Connected,
    /// An observed transport was dropped.
    Disconnected,
    /// The peer ended its output stream.
    ReadClosed,
    /// An I/O operation failed.
    IoFailed {
        /// Failed direction.
        direction: Direction,
        /// OS-independent error category.
        kind: io::ErrorKind,
    },
    /// Negotiation or authentication was refused by the caller.
    Rejected,
    /// Admission was closed.
    DrainStarted,
    /// All admitted work completed after drain began.
    DrainCompleted,
    /// A drain deadline expired, without cancelling active work.
    DrainTimedOut,
    /// The consumer began a cooperative handoff.
    HandoffStarted,
    /// The consumer committed a cooperative handoff.
    HandoffCompleted,
    /// The consumer abandoned a cooperative handoff.
    HandoffAborted,
}
/// An event with monotonic time relative to this observation instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedEvent {
    /// Time since construction.
    pub elapsed: Duration,
    /// Event category.
    pub event: Event,
}

#[derive(Default)]
struct Progress {
    bytes: AtomicU64,
    pending: AtomicUsize,
    busy_since: AtomicU64,
    last_progress: AtomicU64,
    errors: AtomicU64,
}
/// Progress in one direction. Samples are approximate during concurrent I/O.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProgressSnapshot {
    /// Bytes accepted by successful transport calls, not application acknowledgements.
    pub bytes: u64,
    /// I/O calls currently in progress. This does not count application jobs.
    pub pending: usize,
    /// Time since this direction became continuously busy; not a job deadline.
    pub busy_for: Option<Duration>,
    /// Time since the last nonempty transfer; `None` until the first transfer.
    pub since_progress: Option<Duration>,
    /// Failed I/O calls, excluding Interrupted and WouldBlock.
    pub errors: u64,
}
/// Transport observations aggregated over wrappers sharing this instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Currently owned observed transports.
    pub connections: usize,
    /// Input progress.
    pub read: ProgressSnapshot,
    /// Output progress.
    pub write: ProgressSnapshot,
    /// Events discarded due to capacity or reader contention.
    pub lost_events: u64,
}
/// Shared local data. Attach it only where the consumer needs observations.
pub struct Observations {
    started: Instant,
    read: Progress,
    write: Progress,
    connections: AtomicUsize,
    events: Mutex<VecDeque<TimedEvent>>,
    event_capacity: usize,
    lost_events: AtomicU64,
}
impl Observations {
    /// Create a bounded event queue. Zero disables event recording; counters remain active.
    pub fn new(event_capacity: usize) -> Self {
        Self {
            started: Instant::now(),
            read: Progress::default(),
            write: Progress::default(),
            connections: AtomicUsize::new(0),
            events: Mutex::new(VecDeque::with_capacity(event_capacity)),
            event_capacity,
            lost_events: AtomicU64::new(0),
        }
    }
    /// Record a lifecycle event without waiting for the event consumer.
    pub fn record(&self, event: Event) {
        if self.event_capacity == 0 {
            return;
        }
        if let Ok(mut events) = self.events.try_lock()
            && events.len() < self.event_capacity
        {
            events.push_back(TimedEvent {
                elapsed: self.started.elapsed(),
                event,
            });
            return;
        }
        self.lost_events.fetch_add(1, Ordering::Relaxed);
    }
    /// Consume queued events in order. Other consumers share this queue.
    pub fn take_events(&self) -> Vec<TimedEvent> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }
    /// Read counters without acquiring an I/O or event lock.
    pub fn snapshot(&self) -> Snapshot {
        let progress = |p: &Progress| {
            let last = p.last_progress.load(Ordering::Relaxed);
            let pending = p.pending.load(Ordering::Relaxed);
            ProgressSnapshot {
                bytes: p.bytes.load(Ordering::Relaxed),
                pending,
                busy_for: (pending != 0).then(|| {
                    self.started
                        .elapsed()
                        .saturating_sub(Duration::from_nanos(p.busy_since.load(Ordering::Relaxed)))
                }),
                since_progress: (last != 0).then(|| {
                    self.started
                        .elapsed()
                        .saturating_sub(Duration::from_nanos(last - 1))
                }),
                errors: p.errors.load(Ordering::Relaxed),
            }
        };
        Snapshot {
            connections: self.connections.load(Ordering::Relaxed),
            read: progress(&self.read),
            write: progress(&self.write),
            lost_events: self.lost_events.load(Ordering::Relaxed),
        }
    }
    fn progress(&self, direction: Direction) -> &Progress {
        match direction {
            Direction::Read => &self.read,
            Direction::Write => &self.write,
        }
    }
    fn start(&self, direction: Direction) {
        let progress = self.progress(direction);
        if progress.pending.fetch_add(1, Ordering::Relaxed) == 0 {
            progress.busy_since.store(
                self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
        }
    }
    fn begin(&self, direction: Direction) -> Pending<'_> {
        self.start(direction);
        Pending(&self.progress(direction).pending)
    }
    fn transferred(&self, direction: Direction, bytes: usize) {
        if bytes == 0 {
            return;
        }
        let p = self.progress(direction);
        p.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        let elapsed = self
            .started
            .elapsed()
            .as_nanos()
            .min(u128::from(u64::MAX - 1)) as u64;
        p.last_progress.fetch_max(elapsed + 1, Ordering::Relaxed);
    }
    fn failed(&self, direction: Direction, error: &io::Error) {
        if matches!(
            error.kind(),
            io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
        ) {
            return;
        }
        self.progress(direction)
            .errors
            .fetch_add(1, Ordering::Relaxed);
        self.record(Event::IoFailed {
            direction,
            kind: error.kind(),
        });
    }
}
impl Default for Observations {
    fn default() -> Self {
        Self::new(64)
    }
}

struct Pending<'a>(&'a AtomicUsize);
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Blocking I/O adapter. It preserves partial transfers, errors and backpressure.
/// Timeouts, authentication and cancellation remain the transport's responsibility.
pub struct ObservedIo<S> {
    inner: S,
    observations: Arc<Observations>,
    read_closed: bool,
}
impl<S> ObservedIo<S> {
    /// Wrap one connection and retain a separate observation handle for readers.
    pub fn new(inner: S, observations: Arc<Observations>) -> Self {
        observations.connections.fetch_add(1, Ordering::Relaxed);
        observations.record(Event::Connected);
        Self {
            inner,
            observations,
            read_closed: false,
        }
    }
    /// Access transport configuration. I/O through this reference bypasses counters.
    pub fn inner_mut(&mut self) -> &mut S {
        &mut self.inner
    }
}
impl<S> Drop for ObservedIo<S> {
    fn drop(&mut self) {
        self.observations
            .connections
            .fetch_sub(1, Ordering::Relaxed);
        self.observations.record(Event::Disconnected);
    }
}
impl<S: Read> Read for ObservedIo<S> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let _pending = self.observations.begin(Direction::Read);
        let result = self.inner.read(bytes);
        match &result {
            Ok(0) if !bytes.is_empty() && !self.read_closed => {
                self.read_closed = true;
                self.observations.record(Event::ReadClosed);
            }
            Ok(n) => self.observations.transferred(Direction::Read, *n),
            Err(error) => self.observations.failed(Direction::Read, error),
        }
        result
    }
}
impl<S: Write> Write for ObservedIo<S> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let _pending = self.observations.begin(Direction::Write);
        let result = self.inner.write(bytes);
        match &result {
            Ok(n) => self.observations.transferred(Direction::Write, *n),
            Err(error) => self.observations.failed(Direction::Write, error),
        }
        result
    }
    fn flush(&mut self) -> io::Result<()> {
        let _pending = self.observations.begin(Direction::Write);
        let result = self.inner.flush();
        if let Err(error) = &result {
            self.observations.failed(Direction::Write, error);
        }
        result
    }
}

#[cfg(feature = "wire-async")]
mod asynchronous {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

    /// Async adapter with the same counters as [`ObservedIo`].
    /// A Pending poll remains observable until a later completion or adapter drop.
    /// Cancelling a caller's future alone does not prove that transport I/O stopped.
    pub struct AsyncObservedIo<S> {
        io: ObservedIo<S>,
        read_pending: bool,
        write_pending: bool,
    }
    impl<S> AsyncObservedIo<S> {
        /// Wrap an async transport without spawning any task.
        pub fn new(inner: S, observations: Arc<Observations>) -> Self {
            Self {
                io: ObservedIo::new(inner, observations),
                read_pending: false,
                write_pending: false,
            }
        }
        /// Configure the inner transport; direct I/O bypasses observations.
        pub fn inner_mut(&mut self) -> &mut S {
            self.io.inner_mut()
        }
        fn begin(&mut self, direction: Direction) {
            let pending = match direction {
                Direction::Read => &mut self.read_pending,
                Direction::Write => &mut self.write_pending,
            };
            if !*pending {
                // The counter spans Pending polls; end() or Drop releases it.
                self.io.observations.start(direction);
                *pending = true;
            }
        }
        fn end(&mut self, direction: Direction) {
            let pending = match direction {
                Direction::Read => &mut self.read_pending,
                Direction::Write => &mut self.write_pending,
            };
            if std::mem::take(pending) {
                self.io
                    .observations
                    .progress(direction)
                    .pending
                    .fetch_sub(1, Ordering::Relaxed);
            }
        }
    }
    impl<S> Drop for AsyncObservedIo<S> {
        fn drop(&mut self) {
            self.end(Direction::Read);
            self.end(Direction::Write);
        }
    }
    impl<S: AsyncRead + Unpin> AsyncRead for AsyncObservedIo<S> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.begin(Direction::Read);
            let before = buf.filled().len();
            let capacity = buf.remaining();
            let result = Pin::new(&mut this.io.inner).poll_read(cx, buf);
            if let Poll::Ready(result) = &result {
                this.end(Direction::Read);
                match result {
                    Ok(()) => {
                        let bytes = buf.filled().len() - before;
                        this.io.observations.transferred(Direction::Read, bytes);
                        if bytes == 0 && capacity != 0 && !this.io.read_closed {
                            this.io.read_closed = true;
                            this.io.observations.record(Event::ReadClosed);
                        }
                    }
                    Err(error) => this.io.observations.failed(Direction::Read, error),
                }
            }
            result
        }
    }
    impl<S: AsyncWrite + Unpin> AsyncWrite for AsyncObservedIo<S> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            this.begin(Direction::Write);
            let result = Pin::new(&mut this.io.inner).poll_write(cx, bytes);
            if let Poll::Ready(result) = &result {
                this.end(Direction::Write);
                match result {
                    Ok(bytes) => this.io.observations.transferred(Direction::Write, *bytes),
                    Err(error) => this.io.observations.failed(Direction::Write, error),
                }
            }
            result
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.begin(Direction::Write);
            let result = Pin::new(&mut this.io.inner).poll_flush(cx);
            if let Poll::Ready(result) = &result {
                this.end(Direction::Write);
                if let Err(error) = result {
                    this.io.observations.failed(Direction::Write, error);
                }
            }
            result
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.begin(Direction::Write);
            let result = Pin::new(&mut this.io.inner).poll_shutdown(cx);
            if let Poll::Ready(result) = &result {
                this.end(Direction::Write);
                if let Err(error) = result {
                    this.io.observations.failed(Direction::Write, error);
                }
            }
            result
        }
    }
}
#[cfg(feature = "wire-async")]
pub use asynchronous::AsyncObservedIo;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn a_busy_event_reader_does_not_block_producers_or_snapshots() {
        let observations = Observations::new(1);
        let _reader = observations.events.lock().unwrap();
        observations.record(Event::Rejected);
        assert_eq!(observations.snapshot().lost_events, 1);
    }
}
