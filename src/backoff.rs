//! A check repeated with a growing pause, where the OS offers no event.
//!
//! Two places need it: a `local::ProcessHandle` that can only follow a PID,
//! and a child this process owns whose exit cannot be observed as an event,
//! which is asked through `try_wait` instead.

use std::io;
use std::time::{Duration, Instant};

/// The first pause, doubled after each check up to [`LONGEST`].
pub(crate) const FIRST: Duration = Duration::from_millis(1);
/// The longest pause between two checks.
const LONGEST: Duration = Duration::from_millis(50);

/// Check `done` until it holds or `deadline` passes, or without a deadline.
/// True once it held.
pub(crate) fn poll(
    mut done: impl FnMut() -> io::Result<bool>,
    deadline: Option<Instant>,
) -> io::Result<bool> {
    let mut pause = FIRST;
    loop {
        if done()? {
            return Ok(true);
        }
        let Some(nap) = next_pause(&mut pause, deadline) else {
            return Ok(false);
        };
        std::thread::sleep(nap);
    }
}

/// How long to pause before the next check, or `None` once `deadline` has
/// passed. Each pause doubles the next, up to [`LONGEST`].
pub(crate) fn next_pause(pause: &mut Duration, deadline: Option<Instant>) -> Option<Duration> {
    let nap = match deadline {
        Some(deadline) => {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                return None;
            }
            left.min(*pause)
        }
        None => *pause,
    };
    *pause = LONGEST.min(*pause * 2);
    Some(nap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pauses_grow_up_to_their_limit_and_stop_at_the_deadline() {
        let mut pause = FIRST;
        let naps: Vec<u64> = (0..8)
            .map(|_| next_pause(&mut pause, None).unwrap().as_millis() as u64)
            .collect();
        assert_eq!(naps, [1, 2, 4, 8, 16, 32, 50, 50]);

        let mut pause = LONGEST;
        assert_eq!(next_pause(&mut pause, Some(Instant::now())), None);
        let later = Instant::now() + Duration::from_secs(10);
        assert_eq!(next_pause(&mut pause, Some(later)), Some(LONGEST));
        // A pause never runs past the deadline. A runner that stalls for the
        // whole 40 ms gets `None`, which is also right.
        let soon = Instant::now() + Duration::from_millis(40);
        if let Some(nap) = next_pause(&mut pause, Some(soon)) {
            assert!(nap > Duration::ZERO && nap <= Duration::from_millis(40));
        }
    }

    #[test]
    fn polling_ends_when_the_check_holds_or_the_deadline_passes() {
        let mut checks = 0;
        let done = poll(
            || {
                checks += 1;
                Ok(checks == 3)
            },
            None,
        );
        assert!(done.unwrap());
        assert_eq!(checks, 3);

        let deadline = Instant::now() + Duration::from_millis(20);
        assert!(!poll(|| Ok(false), Some(deadline)).unwrap());
        assert!(Instant::now() >= deadline);

        let error = poll(|| Err(io::Error::other("check failed")), None).unwrap_err();
        assert_eq!(error.to_string(), "check failed");
    }
}
