//! Blocking transport primitives extracted from the Kunobi relay.
//!
//! Transport adapters supply half-close and their own I/O deadlines. Discovery,
//! authentication, request IDs, and protocol bootstrap remain with the caller.

use std::io::{self, Read, Write};
use std::sync::{Condvar, Mutex, TryLockError};

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
    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.ended = true;
        let _ = state.writer.shutdown_write();
    }

    /// Stop accepting bytes and wake writers waiting for a replacement.
    /// This cannot interrupt a transport write that is already blocked.
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.closed = true;
        let _ = state.writer.shutdown_write();
        self.changed.notify_all();
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
pub fn pump_upstream<R: Read, W: WriteHalf>(client_in: &mut R, sock_w: &mut W) -> io::Result<()> {
    let mut buf = vec![0u8; BUFFER_SIZE];
    let result = copy_flushing(client_in, sock_w, &mut buf);
    // Signal end-of-request-stream whichever way we got here. If the client
    // vanished mid-write the broker still needs to stop waiting for more, so
    // this is deliberately not on the success path only.
    let _ = sock_w.shutdown_write();
    result
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
        slot.shutdown();
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

        slot.shutdown();

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
        slot.close();
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
}
