use std::sync::{Arc, Mutex};
use tokio::sync::watch;
use tokio::time::Instant;

#[derive(Clone, Copy, Default)]
struct State {
    draining: bool,
    active: usize,
}

/// A one-way request admission gate shared by all sessions of a daemon.
///
/// Call [`begin`](Self::begin) for each operation, including operations arriving
/// on existing connections. Keep the returned guard until its reply has been
/// flushed. Closing a listener alone does not close persistent sessions.
pub struct Lifecycle {
    state: Mutex<State>,
    changes: watch::Sender<()>,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self {
            state: Mutex::new(State::default()),
            changes: watch::channel(()).0,
        }
    }
}

impl Lifecycle {
    /// Admit an operation unless draining has started.
    pub fn begin(self: &Arc<Self>) -> Option<RequestGuard> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.draining {
            return None;
        }
        state.active += 1;
        Some(RequestGuard(Arc::clone(self)))
    }

    /// Close admission and wake shutdown observers. Only the first call is true.
    pub fn start_drain(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.draining {
            return false;
        }
        state.draining = true;
        self.changes.send_replace(());
        true
    }

    /// Wait until admission has closed, including when it closed before this call.
    pub async fn draining(&self) {
        let mut changes = self.changes.subscribe();
        loop {
            changes.borrow_and_update();
            if self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .draining
            {
                return;
            }
            changes.changed().await.expect("self keeps sender alive");
        }
    }

    /// Close admission and wait for every guard without a timeout.
    pub async fn drain(&self) {
        self.start_drain();
        let mut changes = self.changes.subscribe();
        loop {
            changes.borrow_and_update();
            let active = self.state.lock().unwrap_or_else(|e| e.into_inner()).active;
            if active == 0 {
                return;
            }
            changes.changed().await.expect("self keeps sender alive");
        }
    }

    /// Close admission and wait for all guards, under a single deadline.
    ///
    /// A timeout leaves admission closed and the operations running. The caller
    /// decides whether they can be cancelled or must be allowed to finish.
    pub async fn drain_until(&self, deadline: Instant) -> DrainOutcome {
        if tokio::time::timeout_at(deadline, self.drain())
            .await
            .is_ok()
        {
            return DrainOutcome::Complete;
        }
        let active = self.state.lock().unwrap_or_else(|e| e.into_inner()).active;
        if active == 0 {
            DrainOutcome::Complete
        } else {
            DrainOutcome::TimedOut { active }
        }
    }
}

/// Result of closing admission and waiting for admitted operations.
#[derive(Debug, PartialEq, Eq)]
pub enum DrainOutcome {
    /// Every admitted operation released its guard.
    Complete,
    /// Work remains; the crate has not cancelled it.
    TimedOut {
        /// Number of guards still held at the deadline observation.
        active: usize,
    },
}

/// Keeps an admitted operation in the drain count until dropped.
#[must_use = "keep the guard alive until the operation and its reply finish"]
pub struct RequestGuard(Arc<Lifecycle>);

impl Drop for RequestGuard {
    fn drop(&mut self) {
        let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        state.active -= 1;
        if state.draining && state.active == 0 {
            self.0.changes.send_replace(());
        }
    }
}
