//! `WriterSlot` under every interleaving loom can produce.
//!
//! Run with
//! `RUSTFLAGS="--cfg loom" cargo test --release --no-default-features --test loom_writer_slot`.
//! Default features are off because Tokio reacts to `cfg(loom)` internally,
//! and `WriterSlot` does not use Tokio.
#![cfg(loom)]

use kunobi_daemon::transport::{WriteHalf, WriterSlot};
use loom::sync::{Arc, Mutex};
use loom::thread;
use std::io::{self, Write};

/// A peer writer that records what reached it and refuses writes after it
/// has been half-closed.
#[derive(Clone, Default)]
struct Peer {
    received: Arc<Mutex<Vec<u8>>>,
    shut: Arc<Mutex<bool>>,
    /// Records the attempt, then fails it, like a peer that just died.
    broken: bool,
}

impl Peer {
    fn broken() -> Self {
        Self {
            broken: true,
            ..Self::default()
        }
    }
    fn received(&self) -> Vec<u8> {
        self.received.lock().unwrap().clone()
    }
}

impl Write for Peer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert!(
            !*self.shut.lock().unwrap(),
            "bytes written to a peer after its half-close"
        );
        self.received.lock().unwrap().extend_from_slice(bytes);
        if self.broken {
            return Err(io::ErrorKind::BrokenPipe.into());
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl WriteHalf for Peer {
    fn shutdown_write(&mut self) -> io::Result<()> {
        *self.shut.lock().unwrap() = true;
        Ok(())
    }
}

/// A window that failed on the old peer is not sent again to the replacement,
/// and a window is never sent to both. The writer blocked after the failure
/// wakes when the replacement is installed; loom reports a deadlock otherwise.
#[test]
fn a_failed_window_is_never_replayed_and_its_writer_wakes_on_replacement() {
    loom::model(|| {
        let old = Peer::broken();
        let new = Peer::default();
        let slot = Arc::new(WriterSlot::new(old.clone()));

        let writer = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.write_observed(b"req\n", || {}))
        };
        slot.replace(new.clone());
        writer.join().unwrap();

        let on_old = !old.received().is_empty();
        let on_new = new.received() == b"req\n";
        assert!(
            on_old != on_new,
            "the window must be attempted on exactly one peer (old: {on_old}, new: {on_new})"
        );
    });
}

/// `close` releases a writer blocked after a failure, even though no
/// replacement will ever arrive.
#[test]
fn close_releases_a_writer_waiting_for_a_replacement() {
    loom::model(|| {
        let slot = Arc::new(WriterSlot::new(Peer::broken()));
        let writer = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.write_observed(b"req\n", || {}))
        };
        slot.close();
        writer.join().unwrap();
    });
}

/// The offset `try_pause` reports is exactly what the peer has received, so a
/// coordinator settling the old session never miscounts. Nothing reaches the
/// peer while paused, and the parked window goes out once resumed.
#[test]
fn a_pause_reports_exactly_what_the_peer_received_and_parks_the_rest() {
    loom::model(|| {
        let peer = Peer::default();
        let slot = Arc::new(WriterSlot::new(peer.clone()));

        let writer = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.write_observed(b"a\n", || {}))
        };
        if let Some(offset) = slot.try_pause() {
            let received = peer.received().len() as u64;
            assert_eq!(offset, received, "pause offset must match delivered bytes");
            slot.resume();
        }
        writer.join().unwrap();
        assert_eq!(peer.received(), b"a\n");
    });
}

/// A write racing `close` either completes before it or not at all. The peer
/// asserts that nothing arrives after its half-close.
#[test]
fn a_write_racing_close_never_reaches_a_closed_peer() {
    loom::model(|| {
        let peer = Peer::default();
        let slot = Arc::new(WriterSlot::new(peer.clone()));
        let writer = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.write_observed(b"a\n", || {}))
        };
        slot.close();
        writer.join().unwrap();
        let received = peer.received();
        assert!(received.is_empty() || received == b"a\n");
    });
}
