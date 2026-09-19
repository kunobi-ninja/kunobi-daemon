//! Deadline-bounded live probes. A connect or discovery record is not a proof.
use std::time::{Duration, Instant};

/// Default interval between unsuccessful probes. A successful fast path does not sleep.
pub const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Poll a live verifier with the remaining absolute budget.
///
/// `None` means not ready. Errors propagate, including peer authorization errors.
/// The callback must bound blocking operations by its supplied deadline. A proof
/// returned after that deadline is rejected. No process is killed by this helper.
pub fn wait_until<T, E>(
    deadline: Instant,
    mut probe: impl FnMut(Instant) -> Result<Option<T>, E>,
) -> Result<Option<T>, E> {
    loop {
        if Instant::now() >= deadline {
            return Ok(None);
        }
        let proof = probe(deadline)?;
        if Instant::now() >= deadline {
            return Ok(None);
        }
        if proof.is_some() {
            return Ok(proof);
        }
        std::thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

/// A stateful live verifier whose pending I/O can be cancelled at the deadline.
#[cfg(feature = "async")]
pub trait AsyncProbe {
    /// Verified application readiness.
    type Proof;
    /// Application or transport failure.
    type Error;
    /// Obtain fresh evidence. None asks the shared loop to retry.
    fn probe(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Option<Self::Proof>, Self::Error>> + Send;
}

/// Async counterpart of [`wait_until`], including a timeout around each probe.
#[cfg(feature = "async")]
pub async fn wait_until_async<P: AsyncProbe>(
    deadline: tokio::time::Instant,
    probe: &mut P,
) -> Result<Option<P::Proof>, P::Error> {
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        let Ok(proof) = tokio::time::timeout_at(deadline, probe.probe()).await else {
            return Ok(None);
        };
        let proof = proof?;
        if tokio::time::Instant::now() >= deadline {
            return Ok(None);
        }
        if proof.is_some() {
            return Ok(proof);
        }
        tokio::time::sleep_until((tokio::time::Instant::now() + POLL_INTERVAL).min(deadline)).await;
    }
}
