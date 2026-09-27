//! Independent, bounded capacity for setup, application and control traffic.
use std::sync::{
    Arc,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

/// Connection class. Classify authenticated peers before admitting application work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
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

/// Properties of the three budgets for every short sequence of admissions and
/// releases. Atomicity under contention rests on `fetch_update`; this checks
/// the counting it performs.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Admissions and releases explored per harness: enough to fill a pool,
    /// be refused, and release a permit from another pool in between.
    const STEPS: usize = 5;
    const POOLS: [Pool; 3] = [Pool::Handshake, Pool::Application, Pool::Control];

    fn small() -> usize {
        usize::from(kani::any::<u8>() % 3)
    }

    /// A pool admits exactly while it has capacity, whatever the other pools
    /// hold, so exhausted application capacity never refuses a control
    /// session. Every permit returns its own slot when dropped, in any order.
    #[kani::proof]
    #[kani::unwind(6)]
    fn each_pool_admits_up_to_its_own_limit_and_no_further() {
        let limits = Limits {
            handshakes: small(),
            application: small(),
            control: small(),
        };
        let limit = [limits.handshakes, limits.application, limits.control];
        let admission = Arc::new(Admission::new(limits));
        let mut held = Vec::with_capacity(STEPS);
        let mut active = [0usize; 3];
        let mut rejected = [0u64; 3];

        for _ in 0..STEPS {
            if held.is_empty() || kani::any() {
                let pool: Pool = kani::any();
                let index = pool as usize;
                match admission.try_acquire(pool) {
                    Some(permit) => {
                        assert!(active[index] < limit[index]);
                        active[index] += 1;
                        held.push(permit);
                    }
                    None => {
                        assert!(active[index] == limit[index]);
                        rejected[index] += 1;
                    }
                }
            } else {
                let at = kani::any_where(|at: &usize| *at < held.len());
                let permit = held.swap_remove(at);
                active[permit.pool as usize] -= 1;
                drop(permit);
            }

            for pool in POOLS {
                let index = pool as usize;
                let snapshot = admission.snapshot(pool);
                assert!(snapshot.limit == limit[index]);
                assert!(snapshot.active == active[index]);
                assert!(snapshot.rejected == rejected[index]);
            }
        }
    }
}
