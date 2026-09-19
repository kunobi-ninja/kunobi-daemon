//! Replacement ordering shared by blocking clients and async servers.
//!
//! The caller holds the persistent upgrade lock throughout the transaction.
//! Adapters perform one operation at a time; this module decides their order.
use crate::ProcessLock;
use std::time::{Duration, Instant};

/// Whether the application permits old and new processes to coexist.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// Verify and select the candidate before retiring the incumbent.
    Overlap,
    /// Drain and release the incumbent before starting the candidate.
    Exclusive,
}

/// One operation supplied by an application adapter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    /// Re-read selection under the transaction lock. Unchanged skips replacement.
    Recheck,
    /// Validate the artifact and capture application preparation state.
    Prepare,
    /// Close incumbent admission and release exclusive resources.
    Drain,
    /// Launch the candidate through its process owner.
    Start,
    /// Obtain a live identity/readiness proof. Pending retries this step only.
    Verify,
    /// Revalidate selection and application preparation after the live proof.
    Validate,
    /// Atomically select the verified candidate; never return an error after committing.
    Commit,
    /// Start retiring the incumbent. Pending keeps its obligations alive.
    Retire,
}

/// Result of one adapter operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    /// The operation completed.
    Done,
    /// The proof/drain is not ready, or retirement remains in progress.
    Pending,
    /// Recheck found no replacement necessary or another updater already won.
    Unchanged,
}

/// A replacement result never implies replay of application work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// No replacement was performed.
    Unchanged,
    /// Selection committed and the incumbent has no remaining obligations.
    Complete,
    /// Selection committed; the incumbent must remain alive for its obligations.
    RetirementPending,
}

/// Failure retains the phase and whether selection has already committed.
#[derive(Debug)]
pub struct Failure<E> {
    /// Failed operation.
    pub step: Step,
    /// Whether the new selection is authoritative despite this failure.
    pub committed: bool,
    /// Why the operation failed.
    pub reason: Reason<E>,
}

/// Cause of a replacement failure.
#[derive(Debug)]
pub enum Reason<E> {
    /// Setup or the consumer's explicit drain budget expired.
    Deadline,
    /// Application or operating system operation failed.
    Adapter(E),
    /// The adapter returned a result that is not valid for the current step.
    InvalidProgress,
}

impl<E: std::fmt::Display> std::fmt::Display for Failure<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "replacement {:?} failed: ", self.step)?;
        match &self.reason {
            Reason::Deadline => f.write_str("deadline expired"),
            Reason::Adapter(error) => error.fmt(f),
            Reason::InvalidProgress => f.write_str("invalid adapter progress"),
        }
    }
}
impl<E: std::error::Error + 'static> std::error::Error for Failure<E> {}

/// Short setup budget and independent application-owned drain policy.
#[derive(Clone, Copy, Debug)]
pub struct Budgets {
    /// Absolute budget shared by setup operations, restarted after exclusive drain.
    pub setup: Duration,
    /// None allows drain to take arbitrarily long. Expiry never kills work.
    pub drain: Option<Duration>,
}

/// Blocking application operations; no runtime is introduced into the client.
pub trait Driver {
    /// Application error.
    type Error;
    /// Perform exactly the requested operation, bounding I/O by the given deadline.
    fn perform(&mut self, step: Step, deadline: Option<Instant>) -> Result<Progress, Self::Error>;
}

/// Async application operations.
#[cfg(feature = "async")]
pub trait AsyncDriver {
    /// Application error.
    type Error;
    /// Perform exactly the requested operation. Cancellation must preserve ownership.
    fn perform(
        &mut self,
        step: Step,
        deadline: Option<tokio::time::Instant>,
    ) -> impl std::future::Future<Output = Result<Progress, Self::Error>> + Send;
}

struct Machine {
    mode: Mode,
    step: Step,
    committed: bool,
}
impl Machine {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            step: Step::Recheck,
            committed: false,
        }
    }
    fn failure<E>(&self, reason: Reason<E>) -> Failure<E> {
        Failure {
            step: self.step,
            committed: self.committed,
            reason,
        }
    }
    fn advance<E>(&mut self, progress: Progress) -> Result<Option<Outcome>, Failure<E>> {
        use Step::*;
        match (self.step, progress) {
            (Recheck, Progress::Unchanged) => return Ok(Some(Outcome::Unchanged)),
            (Recheck | Verify | Drain, Progress::Pending) => return Ok(None),
            (Retire, Progress::Pending) => return Ok(Some(Outcome::RetirementPending)),
            (_, Progress::Done) => {}
            _ => return Err(self.failure(Reason::InvalidProgress)),
        }
        self.step = match self.step {
            Recheck => Prepare,
            Prepare if self.mode == Mode::Exclusive => Drain,
            Prepare | Drain => Start,
            Start => Verify,
            Verify => Validate,
            Validate => Commit,
            Commit => {
                self.committed = true;
                if self.mode == Mode::Exclusive {
                    return Ok(Some(Outcome::Complete));
                }
                Retire
            }
            Retire => return Ok(Some(Outcome::Complete)),
        };
        Ok(None)
    }
}

/// Execute a serialized replacement. The lock is retained by the caller on return.
///
/// The adapter must preserve a selected candidate when reporting post-commit
/// retirement failures. No callback automatically replays requests or kills a daemon.
pub fn run<D: Driver>(
    _ownership: &ProcessLock,
    mode: Mode,
    budgets: Budgets,
    driver: &mut D,
) -> Result<Outcome, Failure<D::Error>> {
    let mut machine = Machine::new(mode);
    let mut deadline = Some(Instant::now() + budgets.setup);
    loop {
        if deadline.is_some_and(|limit| Instant::now() >= limit) {
            return Err(machine.failure(Reason::Deadline));
        }
        let step = machine.step;
        let progress = driver
            .perform(step, deadline)
            .map_err(|e| machine.failure(Reason::Adapter(e)))?;
        // A committed result must be recorded even if a blocking commit ran late.
        if step != Step::Commit && deadline.is_some_and(|limit| Instant::now() >= limit) {
            return Err(machine.failure(Reason::Deadline));
        }
        if let Some(outcome) = machine.advance(progress)? {
            return Ok(outcome);
        }
        if machine.step != step {
            if machine.step == Step::Drain || machine.step == Step::Retire {
                deadline = budgets.drain.map(|budget| Instant::now() + budget);
            } else if step == Step::Drain {
                deadline = Some(Instant::now() + budgets.setup);
            }
        } else {
            let wait = deadline.map_or(crate::readiness::POLL_INTERVAL, |limit| {
                crate::readiness::POLL_INTERVAL.min(limit.saturating_duration_since(Instant::now()))
            });
            std::thread::sleep(wait);
        }
    }
}

/// Execute the same transition machine on an async runtime.
#[cfg(feature = "async")]
pub async fn run_async<D: AsyncDriver>(
    _ownership: &ProcessLock,
    mode: Mode,
    budgets: Budgets,
    driver: &mut D,
) -> Result<Outcome, Failure<D::Error>> {
    let mut machine = Machine::new(mode);
    let mut deadline = Some(tokio::time::Instant::now() + budgets.setup);
    loop {
        if deadline.is_some_and(|limit| tokio::time::Instant::now() >= limit) {
            return Err(machine.failure(Reason::Deadline));
        }
        let step = machine.step;
        // Commit is an atomic publication operation. Interrupting it could leave its
        // result unknown, so its adapter must not await after publication.
        let result = if let Some(limit) = deadline.filter(|_| step != Step::Commit) {
            tokio::time::timeout_at(limit, driver.perform(step, deadline))
                .await
                .map_err(|_| machine.failure(Reason::Deadline))?
        } else {
            driver.perform(step, deadline).await
        };
        let progress = result.map_err(|e| machine.failure(Reason::Adapter(e)))?;
        if step != Step::Commit
            && deadline.is_some_and(|limit| tokio::time::Instant::now() >= limit)
        {
            return Err(machine.failure(Reason::Deadline));
        }
        if let Some(outcome) = machine.advance(progress)? {
            return Ok(outcome);
        }
        if machine.step != step {
            if machine.step == Step::Drain || machine.step == Step::Retire {
                deadline = budgets
                    .drain
                    .map(|budget| tokio::time::Instant::now() + budget);
            } else if step == Step::Drain {
                deadline = Some(tokio::time::Instant::now() + budgets.setup);
            }
        } else {
            let wake = tokio::time::Instant::now() + crate::readiness::POLL_INTERVAL;
            tokio::time::sleep_until(deadline.map_or(wake, |limit| limit.min(wake))).await;
        }
    }
}
