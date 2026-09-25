//! Wait for a replacement that another process drives.
//!
//! [`crate::replacement`] is for the process holding the upgrade lock. Its
//! observers need a different answer: whether the candidate it launched, or is
//! waiting for, is serving now. They have two kinds of evidence:
//!
//! - the published selection record, which says the candidate was chosen but
//!   is not a readiness proof;
//! - a fresh probe of the service, which is.
//!
//! Pairing them carelessly is a race. If the candidate commits while the
//! observer is still probing the incumbent, the observer holds a stale "not the
//! candidate" probe and a new "candidate committed" record. It then reports an
//! unproven commit although the candidate is already serving.
//!
//! This module fixes the order: every round reads the record first, then
//! probes. [`Selection::Current`] comes only from a fresh accepted probe. The
//! record only decides what to report when no proof arrives:
//! [`Selection::Committed`] (authoritative: never roll back underneath it) or
//! [`Selection::NotCommitted`] (the candidate may be discarded). This is the
//! boundary [`crate::replacement::Failure::committed`] draws for the driver.
//!
//! Polling is the default wait between rounds. A source that can be woken
//! overrides [`Evidence::wait`], or `AsyncEvidence::wait` with the `async`
//! feature, without changing the decision rules.
use std::time::{Duration, Instant};

use crate::readiness::POLL_INTERVAL;

/// What an observer may report about a candidate it does not drive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection<P> {
    /// A fresh probe proved the candidate is serving.
    Current(P),
    /// The selection names the candidate, but no fresh probe proved it within
    /// [`Budget::proof`]. The candidate is authoritative: do not roll back,
    /// restart or select anything around it.
    Committed,
    /// [`Budget::commit`] passed without the selection naming the candidate.
    NotCommitted,
}

/// Two independent bounds, both measured from the start of the wait.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    /// Longest wait for the selection to name the candidate at all. This covers
    /// the candidate's startup, which is the slow part.
    pub commit: Duration,
    /// Once the selection names the candidate, how long to keep probing for a
    /// fresh proof before reporting [`Selection::Committed`]. It starts when a
    /// round reads the commit, before that round's probe, and it also bounds
    /// that probe, so make it cover at least one probe round trip. A driver
    /// commits only after verifying the candidate, so a proof normally arrives
    /// on the first probe after the commit. Zero reports `Committed` as soon as
    /// the commit is read.
    pub proof: Duration,
}

/// Evidence about a candidate, gathered without an async runtime.
pub trait Evidence {
    /// A verified response from the candidate.
    type Proof;
    /// Application, transport or record failure. It ends the wait.
    type Error;

    /// Whether the published selection names the candidate. Read before each
    /// probe; a record is never treated as a proof.
    fn committed(&mut self) -> Result<bool, Self::Error>;

    /// A fresh proof from the candidate, or `None`. Bound blocking I/O by
    /// `deadline`; a proof returned after it is rejected.
    fn probe(&mut self, deadline: Instant) -> Result<Option<Self::Proof>, Self::Error>;

    /// Block until the next round is worth making, but not past `until`.
    ///
    /// The default sleeps for [`POLL_INTERVAL`]. A source that can be woken
    /// returns as soon as it has news.
    fn wait(&mut self, until: Instant) {
        std::thread::sleep(POLL_INTERVAL.min(until.saturating_duration_since(Instant::now())));
    }
}

/// Wait for a candidate that another process selects.
///
/// Returns the first fresh proof, or, when none arrives in time, whether the
/// candidate was committed. Errors from either source end the wait.
pub fn await_selection<E: Evidence>(
    budget: Budget,
    evidence: &mut E,
) -> Result<Selection<E::Proof>, E::Error> {
    let start = Instant::now();
    let mut watch = Watch::new(budget);
    loop {
        let committed = evidence.committed()?;
        watch.record(start.elapsed(), committed);
        let until = start + watch.until();
        let proof = evidence.probe(until)?.filter(|_| Instant::now() < until);
        let proven = proof.is_some();
        match watch.decide(start.elapsed(), proven) {
            Decision::Current => return Ok(Selection::Current(proof.expect("proven above"))),
            Decision::Committed => return Ok(Selection::Committed),
            Decision::NotCommitted => return Ok(Selection::NotCommitted),
            Decision::Wait => evidence.wait(start + watch.until()),
        }
    }
}

/// Evidence about a candidate, gathered on a Tokio runtime.
#[cfg(feature = "async")]
pub trait AsyncEvidence {
    /// A verified response from the candidate.
    type Proof;
    /// Application, transport or record failure. It ends the wait.
    type Error;

    /// Whether the published selection names the candidate. Read before each
    /// probe; a record is never treated as a proof.
    fn committed(&mut self) -> impl std::future::Future<Output = Result<bool, Self::Error>> + Send;

    /// A fresh proof from the candidate, or `None`. The caller also enforces the
    /// round's deadline with a timeout, so pending I/O must tolerate cancellation.
    fn probe(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<Self::Proof>, Self::Error>> + Send;

    /// Wait until the next round is worth making, but not past `until`.
    ///
    /// The default sleeps for [`POLL_INTERVAL`]. A source that can be woken
    /// completes as soon as it has news.
    fn wait(
        &mut self,
        until: tokio::time::Instant,
    ) -> impl std::future::Future<Output = ()> + Send {
        tokio::time::sleep_until((tokio::time::Instant::now() + POLL_INTERVAL).min(until))
    }
}

/// Async counterpart of [`await_selection`]. Each probe is also bounded by the
/// round's deadline; a probe that times out counts as no proof.
#[cfg(feature = "async")]
pub async fn await_selection_async<E: AsyncEvidence>(
    budget: Budget,
    evidence: &mut E,
) -> Result<Selection<E::Proof>, E::Error> {
    let start = tokio::time::Instant::now();
    let mut watch = Watch::new(budget);
    loop {
        let committed = evidence.committed().await?;
        watch.record(start.elapsed(), committed);
        let until = start + watch.until();
        let proof = match tokio::time::timeout_at(until, evidence.probe()).await {
            Ok(proof) => proof?.filter(|_| tokio::time::Instant::now() < until),
            Err(_) => None,
        };
        let proven = proof.is_some();
        match watch.decide(start.elapsed(), proven) {
            Decision::Current => return Ok(Selection::Current(proof.expect("proven above"))),
            Decision::Committed => return Ok(Selection::Committed),
            Decision::NotCommitted => return Ok(Selection::NotCommitted),
            Decision::Wait => evidence.wait(start + watch.until()).await,
        }
    }
}

/// The decision after one round, separated from I/O so it can be model-checked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Current,
    Committed,
    NotCommitted,
    Wait,
}

struct Watch {
    budget: Budget,
    /// When a round first read a committed selection. Selection is never
    /// withdrawn, so a later round that reads an older record keeps this.
    committed_at: Option<Duration>,
}

impl Watch {
    fn new(budget: Budget) -> Self {
        Self {
            budget,
            committed_at: None,
        }
    }

    /// Time from the start by which this wait must end.
    fn until(&self) -> Duration {
        match self.committed_at {
            Some(at) => at.saturating_add(self.budget.proof).min(self.budget.commit),
            None => self.budget.commit,
        }
    }

    /// Note the selection as read at `elapsed`, before this round's probe. The
    /// proof budget runs from here, so it also bounds that probe.
    fn record(&mut self, elapsed: Duration, committed: bool) {
        if committed && self.committed_at.is_none() {
            self.committed_at = Some(elapsed);
        }
    }

    /// Decide after the round's probe finished at `elapsed`.
    fn decide(&self, elapsed: Duration, proven: bool) -> Decision {
        if proven {
            return Decision::Current;
        }
        let expired = elapsed >= self.until();
        match (self.committed_at, expired) {
            (_, false) => Decision::Wait,
            (Some(_), true) => Decision::Committed,
            (None, true) => Decision::NotCommitted,
        }
    }
}

/// Properties of the decision rules for every sequence of evidence, not only
/// the ones a test author thought of. Each restates a promise from the module
/// documentation.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Rounds explored per harness. Durations are small so every ordering of
    /// commit, grace and deadline is reachable within them.
    const ROUNDS: usize = 6;

    fn small() -> Duration {
        Duration::from_millis(u64::from(kani::any::<u8>() % 8))
    }

    #[kani::proof]
    #[kani::unwind(7)]
    fn an_observer_never_claims_what_its_evidence_does_not_show() {
        let budget = Budget {
            commit: small(),
            proof: small(),
        };
        let mut watch = Watch::new(budget);
        let mut elapsed = Duration::ZERO;
        let mut seen_commit = false;

        for _ in 0..ROUNDS {
            // Time never runs backwards: the record is read, then the probe
            // runs for some time, then the next round starts.
            elapsed += small();
            let committed: bool = kani::any();
            watch.record(elapsed, committed);
            seen_commit |= committed;
            // A committed read bounds this round's probe by the proof budget.
            if committed {
                assert!(watch.until() <= elapsed + budget.proof);
            }
            elapsed += small();
            let proven: bool = kani::any();
            let decision = watch.decide(elapsed, proven);

            // Only a fresh proof in this very round reports the candidate current.
            assert_eq!(decision == Decision::Current, proven);
            // Committed needs a record read before an unproven probe.
            if decision == Decision::Committed {
                assert!(seen_commit && !proven);
            }
            // A commit once read is never reported as NotCommitted.
            if decision == Decision::NotCommitted {
                assert!(!seen_commit && elapsed >= budget.commit);
            }
            // The wait always ends by the commit budget, and by the proof
            // budget once a commit has been read.
            if elapsed >= budget.commit {
                assert!(decision != Decision::Wait);
            }
            if let Some(at) = watch.committed_at {
                if elapsed >= at + budget.proof {
                    assert!(decision != Decision::Wait);
                }
            }
            if decision != Decision::Wait {
                return;
            }
        }
    }
}
