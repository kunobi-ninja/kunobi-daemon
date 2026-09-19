use crate::ProcessLock;
use std::{fmt, future::Future, io, path::Path, time::Duration};
use tokio::time::Instant;

/// Failure to obtain a live daemon satisfying the caller's version policy.
#[derive(Debug)]
pub enum UpgradeError<E> {
    /// A health probe failed, rather than reporting an absent daemon.
    Probe(E),
    /// The caller's replacement operation failed.
    Replace(E),
    /// The upgrade lock could not be opened or acquired.
    Lock(io::Error),
    /// The shared deadline expired during a probe, lock wait, or replacement.
    Deadline,
}

impl<E: fmt::Display> fmt::Display for UpgradeError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Probe(e) => write!(f, "daemon health probe failed: {e}"),
            Self::Replace(e) => write!(f, "daemon replacement failed: {e}"),
            Self::Lock(e) => write!(f, "daemon upgrade lock failed: {e}"),
            Self::Deadline => f.write_str("daemon did not become current before the deadline"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for UpgradeError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Probe(e) | Self::Replace(e) => Some(e),
            Self::Lock(e) => Some(e),
            Self::Deadline => None,
        }
    }
}

/// Return a live response accepted by the caller, replacing the daemon if needed.
///
/// `probe` must perform a live, cheap health request; `None` means no daemon is
/// available yet. Errors (including permission errors) are propagated. `accepts`
/// defines build/protocol compatibility; the crate does not order build strings.
///
/// A replacement holds the persistent `upgrade_lock` and probes again after
/// acquiring it, so concurrent upgraders can reuse the winner's replacement.
/// `replace` must use the existing service manager when it owns the daemon.
/// Success always comes from a fresh accepted probe, never an exit code or a
/// retiring daemon's response.
///
/// All awaits share `deadline`. Callbacks must yield, bound their own blocking
/// I/O, and tolerate cancellation. Dropping this future releases its upgrade
/// lock; it does not kill a process spawned by `replace`.
pub async fn ensure_current<T, E, P, PF, A, R, RF>(
    upgrade_lock: &Path,
    deadline: Instant,
    mut probe: P,
    accepts: A,
    replace: R,
) -> Result<T, UpgradeError<E>>
where
    P: FnMut() -> PF,
    PF: Future<Output = Result<Option<T>, E>>,
    A: Fn(&T) -> bool,
    R: FnOnce() -> RF,
    RF: Future<Output = Result<(), E>>,
{
    if Instant::now() >= deadline {
        return Err(UpgradeError::Deadline);
    }
    tokio::time::timeout_at(deadline, async {
        if let Some(current) = probe().await.map_err(UpgradeError::Probe)?
            && accepts(&current)
        {
            return Ok(current);
        }
        let _upgrade = loop {
            if let Some(guard) =
                ProcessLock::try_acquire(upgrade_lock).map_err(UpgradeError::Lock)?
            {
                break guard;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        if let Some(current) = probe().await.map_err(UpgradeError::Probe)?
            && accepts(&current)
        {
            return Ok(current);
        }
        replace().await.map_err(UpgradeError::Replace)?;
        loop {
            if let Some(current) = probe().await.map_err(UpgradeError::Probe)?
                && accepts(&current)
            {
                return Ok(current);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| UpgradeError::Deadline)?
}
