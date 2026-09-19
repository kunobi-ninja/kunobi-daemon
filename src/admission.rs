//! Independent, bounded capacity for setup, application and control traffic.
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Connection class. Classify authenticated peers before admitting application work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pool {
    /// Connections whose handshake has not finished.
    Handshake,
    /// Ordinary application sessions.
    Application,
    /// Lifecycle control sessions; never dispatch application work on this budget.
    Control,
}

/// Independent limits. A zero limit disables its pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum pending handshakes.
    pub handshakes: usize,
    /// Maximum application sessions.
    pub application: usize,
    /// Maximum control sessions, even when application capacity is exhausted.
    pub control: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            handshakes: 32,
            application: 256,
            control: 32,
        }
    }
}

/// One pool's observation; counters are independent atomic samples.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// Configured capacity.
    pub limit: usize,
    /// Permits currently held.
    pub active: usize,
    /// Failed admissions since construction.
    pub rejected: u64,
}
struct Capacity {
    limit: usize,
    active: AtomicUsize,
    rejected: AtomicU64,
}

/// Shared capacity, with no waiting queue or background task.
pub struct Admission {
    pools: [Capacity; 3],
}
impl Admission {
    /// Construct separate budgets. Keep one instance per service generation.
    pub fn new(limits: Limits) -> Self {
        Self {
            pools: [limits.handshakes, limits.application, limits.control].map(|limit| Capacity {
                limit,
                active: AtomicUsize::new(0),
                rejected: AtomicU64::new(0),
            }),
        }
    }
    /// Acquire without waiting. Hold the permit for the whole session.
    pub fn try_acquire(self: &Arc<Self>, pool: Pool) -> Option<Permit> {
        let capacity = &self.pools[pool as usize];
        if capacity
            .active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |active| {
                (active < capacity.limit).then(|| active + 1)
            })
            .is_err()
        {
            capacity.rejected.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        Some(Permit {
            admission: Arc::clone(self),
            pool,
        })
    }
    /// Read occupancy without waiting for a connection or writer lock.
    pub fn snapshot(&self, pool: Pool) -> PoolSnapshot {
        let capacity = &self.pools[pool as usize];
        PoolSnapshot {
            limit: capacity.limit,
            active: capacity.active.load(Ordering::Relaxed),
            rejected: capacity.rejected.load(Ordering::Relaxed),
        }
    }
}
/// Releases one admission slot on drop, including cancellation and errors.
#[must_use = "hold the permit until the session ends"]
pub struct Permit {
    admission: Arc<Admission>,
    pool: Pool,
}
impl Drop for Permit {
    fn drop(&mut self) {
        self.admission.pools[self.pool as usize]
            .active
            .fetch_sub(1, Ordering::Relaxed);
    }
}
