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
    superseded_since: Option<Instant>,
}

/// A process generation keeps admitted sessions alive until their leases drop.
pub struct Generation {
    own: u64,
    selected: AtomicU64,
    sessions: Mutex<Sessions>,
    changes: watch::Sender<()>,
    started: Instant,
    staged: bool,
    drain_limit: Option<Duration>,
}

/// Why [`Generation::run_until_retired`] returned.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Retirement {
    /// No session lease was held.
    Idle,
    /// The drain limit expired while leases were still held. Admission is
    /// closed; the application decides how to end the remaining sessions.
    DrainExpired {
        /// Session leases still held at retirement.
        active: usize,
        /// Of those, sessions using a legacy protocol.
        legacy: usize,
    },
}

/// Local session state. Counts describe leases, not application operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
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
    /// How long a newer generation has been selected, if one has.
    pub superseded_for: Option<Duration>,
}
impl Generation {
    /// Refresh application discovery until this process can retire. This loop
    /// owns session wakeups and retirement; the callback reads application
    /// selection and may invoke the shared replacement transaction.
    ///
    /// With a drain limit, the loop also wakes when the limit passes, and checks
    /// it before and after every refresh, so no refresh starts once it has
    /// passed. A `refresh` callback that never completes still delays retirement.
    #[expect(
        clippy::disallowed_methods,
        reason = "Discovery has no change notification; session changes wake the same select immediately."
    )]
    pub async fn run_until_retired<F, Fut>(
        &self,
        interval: Duration,
        staging_lifetime: Duration,
        mut refresh: F,
    ) -> Retirement
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let mut changes = self.subscribe();
        loop {
            changes.borrow_and_update();
            if let Some(retirement) = self.try_retire(staging_lifetime, false, true) {
                return retirement;
            }
            refresh().await;
            if let Some(retirement) = self.retire(staging_lifetime) {
                return retirement;
            }
            // Wake for the drain limit too, not only the refresh interval.
            let wait = self
                .drain_remaining()
                .map_or(interval, |remaining| remaining.min(interval));
            tokio::select! {
                _ = tokio::time::sleep(wait) => {},
                _ = changes.changed() => {},
            }
        }
    }
    /// Initialize from the persisted selection and the process's startup record.
    pub fn new(own: u64, selected: u64, staged: bool) -> Self {
        let started = Instant::now();
        Self {
            own,
            selected: AtomicU64::new(selected),
            sessions: Mutex::new(Sessions {
                superseded_since: (selected > own).then_some(started),
                ..Sessions::default()
            }),
            changes: watch::channel(()).0,
            started,
            staged,
            drain_limit: None,
        }
    }
    /// Bound how long a superseded generation waits for its session leases.
    ///
    /// Without a limit a held lease keeps a superseded generation alive
    /// indefinitely. With one, [`Self::retire`] and [`Self::run_until_retired`]
    /// retire it once the limit has passed since a newer generation was first
    /// selected, whatever leases remain.
    #[must_use]
    pub fn with_drain_limit(mut self, limit: Duration) -> Self {
        self.drain_limit = Some(limit);
        self
    }
    /// Return the latest observed selected epoch.
    pub fn selected(&self) -> u64 {
        self.selected.load(Ordering::Acquire)
    }
    /// Observe a committed selection. Binary rollback still requires a larger epoch.
    pub fn select(&self, selected: u64) -> io::Result<()> {
        // Under the session lock, so retirement decides against a selection
        // and drain clock that cannot change underneath it.
        let mut state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let previous = self
            .selected
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                (selected >= old).then_some(selected)
            })
            .map_err(|_| io::Error::other("generation selection moved backwards"))?;
        if previous != selected {
            if selected > self.own {
                state.superseded_since.get_or_insert_with(Instant::now);
            }
            drop(state);
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
    ///
    /// Ignores the drain limit; see [`Self::retire`].
    pub fn retire_if_idle(&self, staging_lifetime: Duration) -> bool {
        self.try_retire(staging_lifetime, true, false).is_some()
    }
    /// Retire an idle superseded generation or expired unselected candidate,
    /// or a superseded generation whose drain limit has passed.
    pub fn retire(&self, staging_lifetime: Duration) -> Option<Retirement> {
        self.try_retire(staging_lifetime, true, true)
    }
    /// `idle` retires an eligible generation without leases; `drain` retires a
    /// superseded one whose drain limit has passed.
    fn try_retire(
        &self,
        staging_lifetime: Duration,
        idle: bool,
        drain: bool,
    ) -> Option<Retirement> {
        let mut state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        let selected = self.selected();
        if selected <= self.own
            && !(self.staged && selected < self.own && self.started.elapsed() > staging_lifetime)
        {
            return None;
        }
        // Set only once a newer generation is selected; selection never moves back.
        let expired = drain
            && self
                .drain_limit
                .zip(state.superseded_since)
                .is_some_and(|(limit, since)| since.elapsed() >= limit);
        let idle = idle && state.active == 0;
        if !(expired || idle) {
            return None;
        }
        state.retired = true;
        Some(if state.active == 0 {
            Retirement::Idle
        } else {
            Retirement::DrainExpired {
                active: state.active,
                legacy: state.legacy,
            }
        })
    }
    /// Time left before the drain limit retires this generation, once superseded.
    fn drain_remaining(&self) -> Option<Duration> {
        let state = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        self.drain_limit
            .zip(state.superseded_since)
            .map(|(limit, since)| limit.saturating_sub(since.elapsed()))
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
            superseded_for: state.superseded_since.map(|since| since.elapsed()),
        }
    }
    /// Subscribe before reading state so a concurrent release is not missed.
    ///
    /// Notifies when the selected generation advances and when a session lease
    /// is released: the transitions that can make retirement possible.
    /// Admission and [`SessionLease::mark_legacy`] do not notify, so
    /// [`Self::run_until_retired`] does not refresh once per new session.
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
