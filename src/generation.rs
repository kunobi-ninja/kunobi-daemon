//! Generation selection and session retirement, independent of application messages.
use std::{
    io,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{sync::watch, time::Instant};

/// Allocate above every persisted generation while holding the upgrade lock.
/// The counter uses a decimal integer, compatible with an integer JSON record.
pub fn allocate(
    _lock: &crate::ProcessLock,
    directory: &Path,
    counter: &Path,
    selected: u64,
) -> io::Result<u64> {
    let prior = match std::fs::read_to_string(counter) {
        Ok(text) => text.trim().parse::<u64>().map_err(io::Error::other)?,
        Err(e) if e.kind() == io::ErrorKind::NotFound => 0,
        Err(e) => return Err(e),
    };
    let mut maximum = prior.max(selected);
    for entry in std::fs::read_dir(directory)? {
        if let Some(value) = entry?
            .path()
            .file_stem()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u64>().ok())
        {
            maximum = maximum.max(value);
        }
    }
    let next = maximum
        .checked_add(1)
        .ok_or_else(|| io::Error::other("generation exhausted"))?;
    crate::publish_record(counter, next.to_string().as_bytes())?;
    Ok(next)
}

#[derive(Default)]
struct Sessions {
    active: usize,
    legacy: usize,
    retired: bool,
}

/// A process generation keeps admitted sessions alive until their leases drop.
pub struct Generation {
    own: u64,
    selected: AtomicU64,
    sessions: Mutex<Sessions>,
    changes: watch::Sender<()>,
    started: Instant,
    staged: bool,
}

/// Local session state. Counts describe leases, not application operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// This process's epoch.
    pub own: u64,
    /// Monotonic selected epoch.
    pub selected: u64,
    /// Admitted sessions still held.
    pub active: usize,
    /// Admitted sessions using a legacy protocol.
    pub legacy: usize,
    /// Admission closed permanently after retirement.
    pub retired: bool,
}
impl Generation {
    /// Refresh application discovery until this process can retire. This loop
    /// owns session wakeups and retirement; the callback reads application
    /// selection and may invoke the shared replacement transaction.
    pub async fn run_until_retired<F, Fut>(
        &self,
        interval: Duration,
        staging_lifetime: Duration,
        mut refresh: F,
    ) where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let mut changes = self.subscribe();
        loop {
            changes.borrow_and_update();
            refresh().await;
            if self.retire_if_idle(staging_lifetime) {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {},
                _ = changes.changed() => {},
            }
        }
    }
    /// Initialize from the persisted selection and the process's startup record.
    pub fn new(own: u64, selected: u64, staged: bool) -> Self {
        Self {
            own,
            selected: AtomicU64::new(selected),
            sessions: Mutex::new(Sessions::default()),
            changes: watch::channel(()).0,
            started: Instant::now(),
            staged,
        }
    }
    /// Return the latest observed selected epoch.
    pub fn selected(&self) -> u64 {
        self.selected.load(Ordering::Acquire)
    }
    /// Observe a committed selection. Binary rollback still requires a larger epoch.
    pub fn select(&self, selected: u64) -> io::Result<()> {
        let previous = self
            .selected
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                (selected >= old).then_some(selected)
            })
            .map_err(|_| io::Error::other("generation selection moved backwards"))?;
        if previous != selected {
            self.changes.send_replace(());
        }
        Ok(())
    }
    /// Admit a session unless retirement closed admission.
    pub fn admit(self: &Arc<Self>) -> Option<SessionLease> {
        let mut state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if state.retired {
            return None;
        }
        state.active += 1;
        Some(SessionLease {
            generation: Arc::clone(self),
            legacy: false,
        })
    }
    /// Retire only an idle superseded generation or expired unselected candidate.
    pub fn retire_if_idle(&self, staging_lifetime: Duration) -> bool {
        let selected = self.selected();
        if selected <= self.own
            && !(self.staged && selected < self.own && self.started.elapsed() > staging_lifetime)
        {
            return false;
        }
        let mut state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if state.active != 0 {
            return false;
        }
        state.retired = true;
        true
    }
    /// Snapshot without waiting for any transport I/O.
    pub fn snapshot(&self) -> Snapshot {
        let state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        Snapshot {
            own: self.own,
            selected: self.selected(),
            active: state.active,
            legacy: state.legacy,
            retired: state.retired,
        }
    }
    /// Subscribe before reading state so a concurrent release is not missed.
    pub fn subscribe(&self) -> watch::Receiver<()> {
        self.changes.subscribe()
    }
}

/// Keeps a generation alive while its transport or application session is owned.
pub struct SessionLease {
    generation: Arc<Generation>,
    legacy: bool,
}
impl SessionLease {
    /// Mark the session once when a legacy protocol has been authenticated.
    pub fn mark_legacy(&mut self) {
        if !self.legacy {
            self.legacy = true;
            self.generation
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .legacy += 1;
        }
    }
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        let mut state = self
            .generation
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        state.active -= 1;
        if self.legacy {
            state.legacy -= 1;
        }
        self.generation.changes.send_replace(());
    }
}
