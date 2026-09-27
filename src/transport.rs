//! Blocking transport primitives extracted from the Kunobi relay.
//!
//! Transport adapters supply half-close and their own I/O deadlines. Discovery,
//! authentication, request IDs, and protocol bootstrap remain with the caller.

#[cfg(loom)]
use loom::sync::{Condvar, Mutex, MutexGuard};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::sync::TryLockError;
#[cfg(not(loom))]
use std::sync::{Condvar, Mutex, MutexGuard};

/// A transport writer that can close requests while preserving incoming replies.
pub trait WriteHalf: Write + Send + 'static {
    /// Close the sending direction without closing the receiving direction.
    fn shutdown_write(&mut self) -> io::Result<()>;
}

/// The current peer writer, replaceable without waiting for client input.
pub struct WriterSlot<W> {
    state: Mutex<WriterState<W>>,
    changed: Condvar,
}

struct WriterState<W> {
    generation: u64,
    writer: W,
    paused: bool,
    at_boundary: bool,
    written: u64,
    failed: bool,
    ended: bool,
    closed: bool,
}

impl<W: WriteHalf> WriterSlot<W> {
    /// Retain the initial connected writer.
    pub fn new(writer: W) -> Self {
        Self {
            state: Mutex::new(WriterState {
                generation: 0,
                writer,
                paused: false,
                at_boundary: true,
                written: 0,
                failed: false,
                ended: false,
                closed: false,
            }),
            changed: Condvar::new(),
        }
    }

    /// Install immediately; only one writer is retained even across repeated
    /// idle broker restarts.
    pub fn replace(&self, writer: W) {
        self.replace_with_offset(writer, 0);
    }

    /// Install a new writer with its already-written bootstrap byte count.
    pub fn replace_with_offset(&self, writer: W, written: u64) {
        self.replace_paused(writer, written);
        self.resume();
    }

    /// Keep future client bytes parked until old request observations have
    /// been settled by the downstream coordinator.
    pub fn replace_paused(&self, writer: W, written: u64) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.closed {
            let mut writer = writer;
            let _ = writer.shutdown_write();
            return;
        }
        let mut old = std::mem::replace(&mut state.writer, writer);
        state.generation = state.generation.wrapping_add(1);
        state.written = written;
        state.at_boundary = true;
        state.failed = false;
        state.paused = true;
        if state.ended {
            let _ = state.writer.shutdown_write();
        }
        self.changed.notify_all();
        drop(state);
        let _ = old.shutdown_write();
    }

    /// Freeze only between successful whole writes ending at a newline.
    /// A blocked writer or partial record postpones this connection's upgrade.
    pub fn try_pause(&self) -> Option<u64> {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return None,
        };
        if state.paused || state.failed || state.ended || state.closed || !state.at_boundary {
            return None;
        }
        state.paused = true;
        Some(state.written)
    }

    /// Resume client writes after the coordinator settles the old session.
    pub fn resume(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.paused = false;
        self.changed.notify_all();
    }

    /// Half-close the current writer, marking the end of the request stream.
    ///
    /// Public for the coordinator: when a reconnect completes after the
    /// upstream thread has already exited, nobody else is left to signal the
    /// replacement connection that no request will ever come.
    ///
    /// An error means the peer did not learn that the requests ended, for
    /// example because the transport has no half-close. End the session
    /// another way, such as [`Outstanding::wait_settled`].
    pub fn shutdown(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.ended = true;
        state.writer.shutdown_write()
    }

    /// Stop accepting bytes and wake writers waiting for a replacement.
    /// This cannot interrupt a transport write that is already blocked.
    ///
    /// Waiting writers are released even when the half-close fails; the error
    /// only reports that the peer did not learn that the requests ended.
    pub fn close(&self) -> io::Result<()> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let result = state.writer.shutdown_write();
        self.changed.notify_all();
        result
    }

    /// Write once. On ambiguity, discard the window and wait until the main
    /// thread has installed a different connection before reading more stdin.
    #[cfg(test)]
    fn write_or_wait_for_replacement(&self, bytes: &[u8]) {
        self.write_observed(bytes, || {});
    }

    /// Observe and write one window under the same pause boundary.
    /// A failed window is never replayed. The call waits for a replacement or
    /// `close`; transport writes must have their own deadlines.
    pub fn write_observed(&self, bytes: &[u8], observe: impl FnOnce()) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        while state.paused && !state.closed {
            state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if state.closed {
            return;
        }
        // Observation shares the pause boundary with the write. Unsent input
        // waiting for B must not be cleared with A's completed request ids.
        observe();
        if state
            .writer
            .write_all(bytes)
            .and_then(|()| state.writer.flush())
            .is_ok()
        {
            match state.written.checked_add(bytes.len() as u64) {
                Some(written) => state.written = written,
                None => state.failed = true,
            }
            state.at_boundary = bytes.last().is_none_or(|byte| *byte == b'\n');
            return;
        }
        state.failed = true;
        let _ = state.writer.shutdown_write();
        let failed_generation = state.generation;
        while state.generation == failed_generation && !state.closed {
            state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

/// Maximum bytes buffered in one transfer direction.
pub const BUFFER_SIZE: usize = 65_536;

/// Why forwarding from the peer to the client stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PumpExit {
    /// The peer closed or failed its stream; the coordinator may reconnect.
    PeerClosed,
    /// Writing to the client failed.
    ClientGone,
}

fn copy_flushing<R: Read, W: Write>(from: &mut R, to: &mut W, buf: &mut [u8]) -> io::Result<()> {
    loop {
        let n = match from.read(buf) {
            Ok(0) => return Ok(()),
            Ok(n) => n,
            // A signal interrupting a blocking read is not a failure; retrying
            // is the documented contract. Treating it as EOF would silently
            // truncate a response.
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        to.write_all(&buf[..n])?;
        // Without this a response with no trailing newline sits in the
        // LineWriter and the client waits forever.
        to.flush()?;
    }
}

/// Forward opaque request bytes, flushing each window, then half-close the peer.
/// The response direction remains open for a late reply after client EOF.
///
/// A copy error is returned first. Otherwise the half-close error is: for
/// example `Unsupported` on a named pipe, where the peer was not told that the
/// requests ended and the caller must end the session with [`Outstanding`].
pub fn pump_upstream<R: Read, W: WriteHalf>(client_in: &mut R, sock_w: &mut W) -> io::Result<()> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    let result = copy_flushing(client_in, sock_w, &mut buf);
    // Signal end-of-request-stream whichever way we got here. If the client
    // vanished mid-write the broker still needs to stop waiting for more, so
    // this is deliberately not on the success path only.
    let shutdown = sock_w.shutdown_write();
    result.and(shutdown)
}

/// Forward opaque response bytes, flushing even a newline-free tail.
/// On peer closure the caller may replace the connection; do not join a thread
/// that is still waiting for client input.
pub fn pump_downstream<R: Read, W: Write>(sock_r: &mut R, client_out: &mut W) -> PumpExit {
    let mut buf = vec![0u8; BUFFER_SIZE];
    loop {
        let n = match sock_r.read(&mut buf) {
            Ok(0) => {
                return if client_out.flush().is_ok() {
                    PumpExit::PeerClosed
                } else {
                    PumpExit::ClientGone
                };
            }
            Ok(n) => n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => return PumpExit::PeerClosed,
        };
        if client_out
            .write_all(&buf[..n])
            .and_then(|()| client_out.flush())
            .is_err()
        {
            return PumpExit::ClientGone;
        }
    }
}

/// Combine independently owned halves for a blocking codec.
pub struct SplitIo<R, W> {
    /// Input half.
    pub read: R,
    /// Output half.
    pub write: W,
}
impl<R: std::io::Read, W> std::io::Read for SplitIo<R, W> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        self.read.read(bytes)
    }
}
impl<R, W: std::io::Write> std::io::Write for SplitIo<R, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.write.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.write.flush()
    }
}

/// Requests sent to the peer whose replies have not yet reached the client.
///
/// When the client stops sending, [`WriterSlot::shutdown`] half-closes the
/// connection so the peer answers what it has and then closes. A transport
/// without a half-close, such as a Windows named pipe, cannot deliver that
/// signal, and both ends would wait for each other. There the session instead
/// ends once every outstanding request is settled: [`Outstanding::wait_settled`]
/// returns, and the caller closes the connection.
///
/// The caller decides what a key is (for example a JSON-RPC id) and calls
/// [`Outstanding::settle`] only after the reply has been written and flushed
/// to the client, including any record delimiter. Settling earlier lets the
/// session end with part of a reply unsent.
///
/// Settlements carry the [`Epoch`] in which the peer connection delivering
/// them started. [`Outstanding::fail`] and [`Outstanding::clear`] start a new
/// epoch, so a late reply from a failed peer cannot settle a newer request that
/// reuses its key.
pub struct Outstanding<K> {
    state: Mutex<OutstandingState<K>>,
    changed: Condvar,
    capacity: usize,
}

struct OutstandingState<K> {
    /// Each key with the number of its requests in flight. A client may reuse
    /// a key before the first reply arrives, and each request needs its own.
    keys: BTreeMap<K, usize>,
    /// Requests in flight, counting every duplicate.
    tracked: usize,
    /// Bumped whenever the tracked requests are failed or cleared.
    epoch: u64,
    /// A request arrived while the set was full, so an empty set no longer
    /// proves that every reply was delivered.
    overflowed: bool,
    /// [`Failing`] guards alive: keys taken, their errors still being written.
    failing: usize,
    /// [`Transition`] guards alive: the session is moving between peers.
    transitions: usize,
}

impl<K: Ord> Outstanding<K> {
    /// Track at most `capacity` requests. Beyond it the set can no longer
    /// prove that it is settled, until the requests are failed or cleared.
    pub fn new(capacity: usize) -> Self {
        Self {
            state: Mutex::new(OutstandingState {
                keys: BTreeMap::new(),
                tracked: 0,
                epoch: 0,
                overflowed: false,
                failing: 0,
                transitions: 0,
            }),
            changed: Condvar::new(),
            capacity,
        }
    }

    fn lock(&self) -> MutexGuard<'_, OutstandingState<K>> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record a request sent to the peer.
    pub fn begin(&self, key: K) {
        let mut state = self.lock();
        if state.tracked < self.capacity {
            *state.keys.entry(key).or_insert(0) += 1;
            state.tracked += 1;
        } else {
            state.overflowed = true;
        }
    }

    /// The current epoch. Capture it when a peer connection starts and pass it
    /// to [`Outstanding::settle`] for every reply that connection delivers.
    pub fn epoch(&self) -> Epoch {
        Epoch(self.lock().epoch)
    }

    /// Record that one reply for `key`, delivered by a connection that started
    /// in `epoch`, has fully reached the client. A reply from before the last
    /// [`Outstanding::fail`] or [`Outstanding::clear`] settles nothing.
    pub fn settle(&self, epoch: Epoch, key: &K) {
        let mut state = self.lock();
        if epoch.0 != state.epoch {
            return;
        }
        if let Some(count) = state.keys.get_mut(key) {
            *count -= 1;
            if *count == 0 {
                state.keys.remove(key);
            }
            state.tracked -= 1;
        }
        self.changed.notify_all();
    }

    /// Record that the peer proved every request settled, for example with a
    /// handoff receipt after all replies reached the client.
    pub fn clear(&self) {
        let mut state = self.lock();
        state.keys.clear();
        state.tracked = 0;
        state.epoch += 1;
        state.overflowed = false;
        self.changed.notify_all();
    }

    /// Take every outstanding request to report it as failed, because the
    /// peer that owned it is gone. The set does not count as settled until
    /// the returned guard drops, so the caller can finish writing the
    /// failures first. Untracked requests fail with the session, so this also
    /// clears an overflow.
    pub fn fail(&self) -> Failing<'_, K> {
        let mut state = self.lock();
        state.failing += 1;
        state.overflowed = false;
        state.tracked = 0;
        state.epoch += 1;
        let keys = std::mem::take(&mut state.keys);
        Failing { owner: self, keys }
    }

    /// Hold off [`Outstanding::wait_settled`] while the session moves to
    /// another peer, so it does not end with a replacement half-connected.
    pub fn transition(&self) -> Transition<'_, K> {
        self.lock().transitions += 1;
        Transition(self)
    }

    /// Block until every tracked request is settled, no failures are still
    /// being reported and no transition is in progress. After an overflow
    /// this waits until the requests are failed or cleared.
    pub fn wait_settled(&self) {
        let mut state = self.lock();
        while !state.settled() {
            state = self.changed.wait(state).unwrap_or_else(|e| e.into_inner());
        }
    }
}

impl<K> OutstandingState<K> {
    /// What [`Outstanding::wait_settled`] waits for.
    fn settled(&self) -> bool {
        !self.overflowed && self.failing == 0 && self.transitions == 0 && self.keys.is_empty()
    }
}

/// Which set of requests a settlement belongs to; see [`Outstanding::epoch`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Epoch(u64);

/// Requests taken by [`Outstanding::fail`], still being reported.
pub struct Failing<'a, K: Ord> {
    owner: &'a Outstanding<K>,
    keys: BTreeMap<K, usize>,
}

impl<K: Ord> Failing<'_, K> {
    /// The key of every request to report as failed: once per request, so a
    /// key reused by two requests appears twice.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.keys
            .iter()
            .flat_map(|(key, &count)| std::iter::repeat_n(key, count))
    }
}

impl<K: Ord> Drop for Failing<'_, K> {
    fn drop(&mut self) {
        let mut state = self.owner.lock();
        state.failing -= 1;
        self.owner.changed.notify_all();
    }
}

/// A session transition in progress; see [`Outstanding::transition`].
pub struct Transition<'a, K: Ord>(&'a Outstanding<K>);

impl<K: Ord> Drop for Transition<'_, K> {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.transitions -= 1;
        self.0.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    /// Counts flushes so the LineWriter rule can be asserted directly.
    #[derive(Default)]
    struct CountingSink {
        data: Vec<u8>,
        flushes: usize,
        writes: usize,
        shutdowns: AtomicUsize,
    }
    impl Write for CountingSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.data.extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }
    impl WriteHalf for CountingSink {
        fn shutdown_write(&mut self) -> io::Result<()> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct SharedSink {
        data: Arc<Mutex<Vec<u8>>>,
        fail: bool,
        attempted: Option<mpsc::Sender<()>>,
    }

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if let Some(attempted) = self.attempted.take() {
                let _ = attempted.send(());
            }
            if self.fail {
                return Err(io::Error::new(io::ErrorKind::BrokenPipe, "fixture"));
            }
            self.data.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl WriteHalf for SharedSink {
        fn shutdown_write(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct ShutdownSink(Arc<AtomicUsize>);

    impl Write for ShutdownSink {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl WriteHalf for ShutdownSink {
        fn shutdown_write(&mut self) -> io::Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn pause_never_splits_a_request_or_observes_input_waiting_for_replacement() {
        let slot = Arc::new(WriterSlot::new(CountingSink::default()));
        slot.write_or_wait_for_replacement(b"{\"id\":1");
        assert_eq!(slot.try_pause(), None);
        slot.write_or_wait_for_replacement(b"}\n");
        assert_eq!(slot.try_pause(), Some(9));
        let (observed, received) = mpsc::channel();
        let next = Arc::clone(&slot);
        let thread = std::thread::spawn(move || {
            next.write_observed(b"next\n", || observed.send(()).unwrap())
        });
        assert!(received.recv_timeout(Duration::from_millis(30)).is_err());
        slot.replace_paused(CountingSink::default(), 17);
        assert!(received.recv_timeout(Duration::from_millis(30)).is_err());
        slot.resume();
        received.recv_timeout(Duration::from_secs(1)).unwrap();
        thread.join().unwrap();
        assert_eq!(slot.try_pause(), Some(22));
        slot.shutdown().unwrap();
        slot.replace_paused(CountingSink::default(), 0);
        assert_eq!(
            slot.state
                .lock()
                .unwrap()
                .writer
                .shutdowns
                .load(Ordering::SeqCst),
            1,
            "replacement lost client EOF"
        );
    }

    #[test]
    fn writer_slot_shutdown_reaches_the_current_writer() {
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let slot = WriterSlot::new(ShutdownSink(Arc::clone(&shutdowns)));

        slot.shutdown().unwrap();

        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn writer_slot_keeps_only_the_latest_idle_replacement() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let latest = Arc::new(Mutex::new(Vec::new()));
        let slot = WriterSlot::new(SharedSink {
            data: Arc::clone(&first),
            fail: false,
            attempted: None,
        });
        slot.replace(SharedSink {
            data: Arc::clone(&second),
            fail: false,
            attempted: None,
        });
        slot.replace(SharedSink {
            data: Arc::clone(&latest),
            fail: false,
            attempted: None,
        });

        slot.write_or_wait_for_replacement(b"request");
        assert!(first.lock().unwrap().is_empty());
        assert!(second.lock().unwrap().is_empty());
        assert_eq!(&*latest.lock().unwrap(), b"request");
    }

    #[test]
    fn ambiguous_failed_window_is_not_replayed_after_replacement() {
        let failed = Arc::new(Mutex::new(Vec::new()));
        let replacement = Arc::new(Mutex::new(Vec::new()));
        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let slot = Arc::new(WriterSlot::new(SharedSink {
            data: Arc::clone(&failed),
            fail: true,
            attempted: Some(attempted_tx),
        }));
        let writer = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || {
                slot.write_or_wait_for_replacement(b"ambiguous");
                let _ = done_tx.send(());
            })
        };
        attempted_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer never attempted the ambiguous window");
        assert!(
            matches!(
                done_rx.recv_timeout(Duration::from_millis(100)),
                Err(mpsc::RecvTimeoutError::Timeout)
            ),
            "failed writer returned before a replacement existed"
        );
        slot.replace(SharedSink {
            data: Arc::clone(&replacement),
            fail: false,
            attempted: None,
        });
        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("writer did not resume after replacement");
        writer.join().unwrap();

        assert!(failed.lock().unwrap().is_empty());
        assert!(replacement.lock().unwrap().is_empty());
        slot.write_or_wait_for_replacement(b"next");
        assert_eq!(&*replacement.lock().unwrap(), b"next");
    }

    #[test]
    fn a_payload_larger_than_the_window_round_trips_byte_identical() {
        // Framing correctness across buffer boundaries: the pump must not care
        // where a message starts or ends.
        let payload: Vec<u8> = (0..(BUFFER_SIZE * 3 + 12345))
            .map(|i| (i % 251) as u8)
            .collect();
        let mut src = io::Cursor::new(payload.clone());
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert_eq!(
            sink.data, payload,
            "payload corrupted across buffer boundaries"
        );
    }

    #[test]
    fn the_recovery_window_remains_exactly_sixty_four_kibibytes() {
        assert_eq!(BUFFER_SIZE, 65_536);
    }

    #[test]
    fn a_newline_free_payload_is_still_flushed() {
        // The LineWriter trap. Without the flush this data would be invisible to
        // the client and the server would present as hung.
        let mut src = io::Cursor::new(b"{\"jsonrpc\":\"2.0\"}".to_vec());
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert!(
            !sink.data.contains(&b'\n'),
            "fixture should have no newline"
        );
        assert!(
            sink.flushes >= 1,
            "a newline-free payload was never flushed"
        );
        assert_eq!(sink.flushes, sink.writes, "every write must be flushed");
    }

    #[test]
    fn bytes_are_opaque_invalid_utf8_survives_unchanged() {
        // No String, no validation. A length-prefixed or binary-ish frame must
        // pass through untouched.
        let payload = vec![0xff, 0xfe, 0x00, 0x80, b'\r', b'\n', 0x01];
        let mut src = io::Cursor::new(payload.clone());
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert_eq!(sink.data, payload, "bytes were normalised or validated");
    }

    #[test]
    fn crlf_is_not_normalised() {
        let payload = b"a\r\nb\rc\nd".to_vec();
        let mut src = io::Cursor::new(payload.clone());
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];
        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();
        assert_eq!(sink.data, payload);
    }

    /// A reader that returns `Interrupted` before yielding its data.
    struct InterruptOnce {
        fired: bool,
        inner: io::Cursor<Vec<u8>>,
    }

    #[derive(Default)]
    struct ErrorThenEof {
        reads: usize,
    }

    impl Read for ErrorThenEof {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            if self.reads == 1 {
                Err(io::Error::other("fixture"))
            } else {
                Ok(0)
            }
        }
    }

    #[test]
    fn a_non_interrupted_read_error_is_returned() {
        let mut reader = ErrorThenEof::default();
        let mut sink = CountingSink::default();
        let mut buf = [0u8; 8];
        assert_eq!(
            copy_flushing(&mut reader, &mut sink, &mut buf)
                .unwrap_err()
                .kind(),
            io::ErrorKind::Other
        );
        assert_eq!(reader.reads, 1, "a real error must not be retried");
    }

    impl Read for InterruptOnce {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if !self.fired {
                self.fired = true;
                return Err(io::Error::new(io::ErrorKind::Interrupted, "signal"));
            }
            self.inner.read(buf)
        }
    }

    #[test]
    fn an_interrupted_read_is_retried_not_treated_as_eof() {
        // Treating EINTR as EOF would truncate a response under any signal the
        // host happens to deliver - a rare, unreproducible data-loss bug.
        let mut src = InterruptOnce {
            fired: false,
            inner: io::Cursor::new(b"payload".to_vec()),
        };
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert_eq!(sink.data, b"payload");
    }

    #[test]
    fn upstream_signals_shutdown_on_clean_eof() {
        let mut src = io::Cursor::new(b"request".to_vec());
        let mut sink = CountingSink::default();

        pump_upstream(&mut src, &mut sink).unwrap();

        assert_eq!(
            sink.shutdowns.load(Ordering::SeqCst),
            1,
            "without shutdown_write the broker waits forever for a request that will never come"
        );
        assert_eq!(sink.data, b"request");
    }

    /// Fails every write, standing in for a dead client's stdout or a broker
    /// that went away mid-request.
    #[derive(Default)]
    struct DeadPipe {
        shutdowns: AtomicUsize,
    }
    impl Write for DeadPipe {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::BrokenPipe, "EPIPE"))
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl WriteHalf for DeadPipe {
        fn shutdown_write(&mut self) -> io::Result<()> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::new(io::ErrorKind::NotConnected, "already gone"))
        }
    }

    #[test]
    fn upstream_still_signals_shutdown_when_the_write_fails() {
        // The client vanished mid-request. The broker must still be told the
        // request stream ended, or it holds the connection open indefinitely.
        let mut src = io::Cursor::new(b"half a request".to_vec());
        let mut sink = DeadPipe::default();

        let result = pump_upstream(&mut src, &mut sink);

        assert!(
            result.is_err(),
            "a failed write must be reported, not swallowed"
        );
        assert_eq!(
            sink.shutdowns.load(Ordering::SeqCst),
            1,
            "shutdown skipped on the error path"
        );
    }

    #[test]
    fn upstream_reports_a_half_close_the_transport_cannot_make() {
        // A named pipe has no half-close. Returning Ok here told the caller the
        // peer knew the requests had ended, and both ends waited for each other.
        struct NoHalfClose(Vec<u8>);
        impl Write for NoHalfClose {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        impl WriteHalf for NoHalfClose {
            fn shutdown_write(&mut self) -> io::Result<()> {
                Err(io::ErrorKind::Unsupported.into())
            }
        }
        let mut src = io::Cursor::new(b"request".to_vec());
        let mut sink = NoHalfClose(Vec::new());
        let error = pump_upstream(&mut src, &mut sink).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert_eq!(sink.0, b"request", "the request is still delivered");
    }

    #[test]
    fn upstream_reports_the_write_error_even_though_shutdown_also_failed() {
        // Both fail here. The caller must still learn the transfer failed rather
        // than seeing the shutdown error swallow it into an Ok.
        let mut src = io::Cursor::new(b"x".to_vec());
        let mut sink = DeadPipe::default();
        assert!(pump_upstream(&mut src, &mut sink).is_err());
    }

    #[test]
    fn downstream_reports_broker_closed_on_clean_eof() {
        let mut src = io::Cursor::new(b"response".to_vec());
        let mut sink = CountingSink::default();
        assert_eq!(pump_downstream(&mut src, &mut sink), PumpExit::PeerClosed);
        assert_eq!(sink.data, b"response");
    }

    #[test]
    fn downstream_allows_reconnect_after_a_peer_read_error() {
        let mut reader = ErrorThenEof::default();
        assert_eq!(
            pump_downstream(&mut reader, &mut CountingSink::default()),
            PumpExit::PeerClosed
        );
        assert_eq!(reader.reads, 1);
    }

    #[test]
    fn downstream_retries_interrupted_reads_and_preserves_the_reply() {
        let mut reader = InterruptOnce {
            fired: false,
            inner: io::Cursor::new(b"reply".to_vec()),
        };
        let mut sink = CountingSink::default();
        assert_eq!(
            pump_downstream(&mut reader, &mut sink),
            PumpExit::PeerClosed
        );
        assert_eq!(sink.data, b"reply");
    }

    #[test]
    fn downstream_reports_client_gone_on_a_broken_pipe() {
        // Must be an ordinary return, not a signal death: the relay owns its own
        // exit code so the host can tell "client left" from "relay crashed".
        let mut src = io::Cursor::new(b"response".to_vec());
        assert_eq!(
            pump_downstream(&mut src, &mut DeadPipe::default()),
            PumpExit::ClientGone
        );
    }

    /// A writer that accepts a bounded amount per call, so the pump's
    /// `write_all` has to loop - the shape a throttled socket presents.
    struct Throttled {
        chunk: usize,
        total: AtomicUsize,
    }
    impl Write for Throttled {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            let n = buf.len().min(self.chunk);
            self.total.fetch_add(n, Ordering::SeqCst);
            Ok(n)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_partial_write_does_not_lose_bytes() {
        // `write_all` handles this, but a future refactor to `write` would drop
        // the tail of every buffer silently, so it is pinned here.
        let payload: Vec<u8> = (0..200_000).map(|i| (i % 97) as u8).collect();
        let mut src = io::Cursor::new(payload.clone());
        let mut sink = Throttled {
            chunk: 13,
            total: AtomicUsize::new(0),
        };
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert_eq!(sink.total.load(Ordering::SeqCst), payload.len());
    }

    #[test]
    fn memory_stays_bounded_regardless_of_payload_size() {
        // The anti-buffering property, asserted the only way a unit test can:
        // the transfer window is a compile-time constant and the pump allocates
        // exactly one of them per direction, independent of how much flows.
        let huge: Vec<u8> = vec![b'x'; BUFFER_SIZE * 20];
        let mut src = io::Cursor::new(huge.clone());
        let mut sink = CountingSink::default();
        let mut buf = vec![0u8; BUFFER_SIZE];

        copy_flushing(&mut src, &mut sink, &mut buf).unwrap();

        assert_eq!(buf.len(), BUFFER_SIZE, "the window grew with the payload");
        assert_eq!(sink.data.len(), huge.len());
        assert!(
            sink.writes >= 20,
            "a single write means the payload was buffered whole, not streamed"
        );
    }
    #[test]
    fn closing_releases_a_writer_waiting_after_an_ambiguous_write() {
        let (attempted_tx, attempted_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let slot = Arc::new(WriterSlot::new(SharedSink {
            data: Arc::new(Mutex::new(Vec::new())),
            fail: true,
            attempted: Some(attempted_tx),
        }));
        let task = {
            let slot = Arc::clone(&slot);
            std::thread::spawn(move || {
                slot.write_or_wait_for_replacement(b"ambiguous");
                done_tx.send(()).unwrap();
            })
        };
        attempted_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        slot.close().unwrap();
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        task.join().unwrap();
        let unused = Arc::new(Mutex::new(Vec::new()));
        slot.replace(SharedSink {
            data: Arc::clone(&unused),
            fail: false,
            attempted: None,
        });
        slot.write_or_wait_for_replacement(b"after close");
        assert!(unused.lock().unwrap().is_empty());
    }

    /// Whether `wait_settled` returns within `within`, observed from another
    /// thread so a wrong answer fails the test instead of hanging it.
    fn settles(outstanding: &Arc<Outstanding<u32>>, within: Duration) -> bool {
        let (done, settled) = mpsc::channel();
        let waiter = Arc::clone(outstanding);
        std::thread::spawn(move || {
            waiter.wait_settled();
            let _ = done.send(());
        });
        settled.recv_timeout(within).is_ok()
    }

    const NOT_YET: Duration = Duration::from_millis(100);
    const SOON: Duration = Duration::from_secs(5);

    #[test]
    fn outstanding_settles_when_its_last_request_is_settled() {
        let outstanding = Arc::new(Outstanding::new(8));
        assert!(settles(&outstanding, SOON), "an empty set is settled");
        outstanding.begin(1);
        outstanding.begin(2);
        outstanding.settle(outstanding.epoch(), &1);
        assert!(!settles(&outstanding, NOT_YET));
        outstanding.settle(outstanding.epoch(), &2);
        assert!(settles(&outstanding, SOON));
    }

    #[test]
    fn an_overflowed_set_is_never_settled_until_its_requests_fail() {
        // Past capacity a request is not tracked, so an empty set could hide
        // a reply still on its way.
        let outstanding = Arc::new(Outstanding::new(2));
        for key in 0..3 {
            outstanding.begin(key);
        }
        outstanding.settle(outstanding.epoch(), &0);
        outstanding.settle(outstanding.epoch(), &1);
        assert!(!settles(&outstanding, NOT_YET));
        drop(outstanding.fail());
        assert!(
            settles(&outstanding, SOON),
            "failing the session clears the overflow"
        );
    }

    #[test]
    fn failed_requests_are_not_settled_until_their_report_is_written() {
        let outstanding = Arc::new(Outstanding::new(8));
        outstanding.begin(7);
        let failing = outstanding.fail();
        assert_eq!(failing.keys().copied().collect::<Vec<_>>(), [7]);
        assert!(
            !settles(&outstanding, NOT_YET),
            "settled while failures were still being written"
        );
        drop(failing);
        assert!(settles(&outstanding, SOON));
    }

    #[test]
    fn a_reused_key_needs_one_settle_per_request() {
        // A client may reuse an id before the first reply arrives. Counting it
        // once would end the session with the second reply still on its way.
        let outstanding = Arc::new(Outstanding::new(8));
        outstanding.begin(1);
        outstanding.begin(1);
        outstanding.settle(outstanding.epoch(), &1);
        assert!(!settles(&outstanding, NOT_YET));
        outstanding.settle(outstanding.epoch(), &1);
        assert!(settles(&outstanding, SOON));
    }

    #[test]
    fn a_late_reply_from_a_failed_peer_does_not_settle_a_newer_request() {
        let outstanding = Arc::new(Outstanding::new(8));
        let old_peer = outstanding.epoch();
        outstanding.begin(1);
        drop(outstanding.fail());
        // The client reuses the key with the replacement peer.
        outstanding.begin(1);
        outstanding.settle(old_peer, &1);
        assert!(
            !settles(&outstanding, NOT_YET),
            "a stale reply settled the new request"
        );
        outstanding.settle(outstanding.epoch(), &1);
        assert!(settles(&outstanding, SOON));
    }

    #[test]
    fn a_reused_key_is_reported_once_per_failed_request() {
        let outstanding = Outstanding::new(8);
        outstanding.begin(1);
        outstanding.begin(1);
        outstanding.begin(2);
        let failing = outstanding.fail();
        assert_eq!(failing.keys().copied().collect::<Vec<_>>(), [1, 1, 2]);
    }

    #[test]
    fn a_transition_holds_off_settling() {
        let outstanding = Arc::new(Outstanding::new(8));
        let transition = outstanding.transition();
        assert!(!settles(&outstanding, NOT_YET));
        drop(transition);
        assert!(settles(&outstanding, SOON));
    }

    #[test]
    fn a_receipt_clears_requests_and_an_overflow() {
        let outstanding = Arc::new(Outstanding::new(1));
        outstanding.begin(1);
        outstanding.begin(2);
        outstanding.clear();
        assert!(settles(&outstanding, SOON));
    }
}

/// `Outstanding` against a plain model: a multiset of keys for the current
/// epoch, an overflow flag, and counts of live report and transition guards.
/// Random operation sequences reach key reuse, stale settlements and overflow
/// in orders no example test lists.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    #[derive(Clone, Debug)]
    enum Op {
        Begin(u8),
        /// Settle `key` with the epoch captured `age` fails or clears ago.
        Settle {
            age: usize,
            key: u8,
        },
        Clear,
        /// Take the requests. `hold` keeps the report guard alive for later steps.
        Fail {
            hold: bool,
        },
        FinishReport(usize),
        StartTransition,
        EndTransition(usize),
    }

    fn op() -> impl Strategy<Value = Op> {
        // Few keys, so clients reuse them.
        let key = 0u8..4;
        prop_oneof![
            4 => key.clone().prop_map(Op::Begin),
            3 => key.clone().prop_map(|key| Op::Settle { age: 0, key }),
            1 => (1usize..4, key).prop_map(|(age, key)| Op::Settle { age, key }),
            1 => Just(Op::Clear),
            1 => any::<bool>().prop_map(|hold| Op::Fail { hold }),
            1 => any::<usize>().prop_map(Op::FinishReport),
            1 => Just(Op::StartTransition),
            1 => any::<usize>().prop_map(Op::EndTransition),
        ]
    }

    #[derive(Default)]
    struct Model {
        epoch: usize,
        keys: BTreeMap<u8, usize>,
        overflowed: bool,
        reports: usize,
        transitions: usize,
    }

    impl Model {
        fn settled(&self) -> bool {
            !self.overflowed && self.reports == 0 && self.transitions == 0 && self.keys.is_empty()
        }

        /// Every request in flight, once per request, in key order.
        fn outstanding(&self) -> Vec<u8> {
            self.keys
                .iter()
                .flat_map(|(&key, &count)| std::iter::repeat_n(key, count))
                .collect()
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn outstanding_matches_a_multiset_per_epoch(
            capacity in 0usize..5,
            ops in proptest::collection::vec(op(), 0..64),
        ) {
            let outstanding = Arc::new(Outstanding::new(capacity));
            let mut model = Model::default();
            // The epoch observed while the model was at each epoch number.
            let mut epochs = vec![outstanding.epoch()];
            let mut reports = Vec::new();
            let mut transitions = Vec::new();

            for op in &ops {
                match *op {
                    Op::Begin(key) => {
                        outstanding.begin(key);
                        if model.keys.values().sum::<usize>() < capacity {
                            *model.keys.entry(key).or_insert(0) += 1;
                        } else {
                            model.overflowed = true;
                        }
                    }
                    Op::Settle { age, key } => {
                        let captured = model.epoch.saturating_sub(age);
                        outstanding.settle(epochs[captured], &key);
                        // A reply from before the last fail or clear settles nothing.
                        if captured == model.epoch
                            && let Some(count) = model.keys.get_mut(&key)
                        {
                            *count -= 1;
                            if *count == 0 {
                                model.keys.remove(&key);
                            }
                        }
                    }
                    Op::Clear => {
                        outstanding.clear();
                        model.keys.clear();
                        model.overflowed = false;
                        model.epoch += 1;
                    }
                    Op::Fail { hold } => {
                        let failing = outstanding.fail();
                        prop_assert_eq!(
                            failing.keys().copied().collect::<Vec<_>>(),
                            model.outstanding(),
                            "fail() must report each request in flight exactly once"
                        );
                        model.keys.clear();
                        model.overflowed = false;
                        model.epoch += 1;
                        if hold {
                            reports.push(failing);
                            model.reports += 1;
                        }
                    }
                    Op::FinishReport(index) => {
                        if !reports.is_empty() {
                            drop(reports.swap_remove(index % reports.len()));
                            model.reports -= 1;
                        }
                    }
                    Op::StartTransition => {
                        transitions.push(outstanding.transition());
                        model.transitions += 1;
                    }
                    Op::EndTransition(index) => {
                        if !transitions.is_empty() {
                            drop(transitions.swap_remove(index % transitions.len()));
                            model.transitions -= 1;
                        }
                    }
                }
                if model.epoch == epochs.len() {
                    let epoch = outstanding.epoch();
                    prop_assert!(
                        !epochs.contains(&epoch),
                        "a fail or clear reused an earlier epoch"
                    );
                    epochs.push(epoch);
                }
                prop_assert_eq!(
                    outstanding.lock().settled(),
                    model.settled(),
                    "after {:?}",
                    op
                );
            }

            drop(reports);
            drop(transitions);
            model.reports = 0;
            model.transitions = 0;
            prop_assert_eq!(outstanding.lock().settled(), model.settled());

            // Failing whatever is left ends the session: an overflow included.
            let failing = outstanding.fail();
            prop_assert_eq!(
                failing.keys().copied().collect::<Vec<_>>(),
                model.outstanding()
            );
            drop(failing);
            let (done, settled) = mpsc::channel();
            let waiter = Arc::clone(&outstanding);
            std::thread::spawn(move || {
                waiter.wait_settled();
                let _ = done.send(());
            });
            prop_assert!(
                settled.recv_timeout(Duration::from_secs(5)).is_ok(),
                "wait_settled did not return after every request failed"
            );
        }
    }
}
