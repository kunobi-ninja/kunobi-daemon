//! Persistable replacement attempt budgets. Never use these to replay requests.

/// Consumer policy for candidate retries across controller restarts.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
pub struct Policy {
    /// Maximum attempts for one unchanged candidate selection.
    pub limit: u64,
    /// Initial delay in seconds, doubled after each attempted start.
    pub delay: u64,
    /// Maximum delay in seconds.
    pub maximum_delay: u64,
}
/// State stored alongside the consumer's candidate fingerprint.
#[derive(Clone, Copy, Debug, Default)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
pub struct State {
    /// Reserved attempts, including candidates that die immediately after commit.
    pub attempts: u64,
    /// Earliest permitted retry in the consumer's persisted time domain (seconds).
    pub next_attempt: u64,
}
impl State {
    /// Whether this campaign may attempt a candidate now.
    pub fn allowed(&self, policy: Policy, now: u64) -> bool {
        self.attempts < policy.limit && now >= self.next_attempt
    }
    /// Reserve before launching. Persist the returned state before spawning;
    /// cancellation and controller crashes must not restore the spent attempt.
    pub fn reserve(&mut self, policy: Policy, now: u64) {
        self.attempts = self.attempts.saturating_add(1);
        let multiplier = 1u64 << self.attempts.min(63);
        self.next_attempt = now.saturating_add(
            policy
                .delay
                .saturating_mul(multiplier)
                .min(policy.maximum_delay),
        );
    }
    /// A permanent candidate failure exhausts this fingerprint's campaign.
    pub fn exhaust(&mut self, policy: Policy) {
        self.attempts = policy.limit;
    }
    /// Next retry time, or None once a new candidate selection is required.
    pub fn next(&self, policy: Policy) -> Option<u64> {
        (self.attempts < policy.limit).then_some(self.next_attempt)
    }
}

/// Properties of the budget for every policy, clock reading and persisted
/// state, including the extremes a stale or hand-edited record can hold.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Reserving never panics, never wraps the clock and never schedules the
    /// next attempt beyond the maximum delay.
    #[kani::proof]
    fn a_reservation_waits_no_longer_than_the_maximum_delay() {
        let policy: Policy = kani::any();
        let mut state: State = kani::any();
        let now: u64 = kani::any();
        let attempts = state.attempts;
        state.reserve(policy, now);
        assert!(state.attempts == attempts.saturating_add(1));
        assert!(now <= state.next_attempt);
        assert!(state.next_attempt <= now.saturating_add(policy.maximum_delay));
    }

    /// Backoff never shrinks as attempts grow, and never retries sooner than
    /// the initial delay unless the maximum is lower.
    ///
    /// The delay is bounded to 16 bits, about 18 hours, to keep the
    /// multiplication tractable for the solver. The doubled delay still
    /// saturates, from the 49th attempt for the longest one, so the capped
    /// arithmetic is covered.
    #[kani::proof]
    fn backoff_never_shrinks_as_attempts_grow() {
        let policy: Policy = kani::any();
        kani::assume(policy.delay <= u64::from(u16::MAX));
        let now: u64 = kani::any();
        let mut earlier = State {
            attempts: kani::any(),
            next_attempt: 0,
        };
        let mut later = State {
            attempts: kani::any(),
            next_attempt: 0,
        };
        kani::assume(earlier.attempts <= later.attempts);
        earlier.reserve(policy, now);
        later.reserve(policy, now);
        assert!(earlier.next_attempt <= later.next_attempt);
        let floor = policy.delay.min(policy.maximum_delay);
        assert!(earlier.next_attempt >= now.saturating_add(floor));
    }

    /// A consumer that reserves only when allowed never spends more than the
    /// limit, and an exhausted campaign allows nothing at any later time.
    #[kani::proof]
    fn a_campaign_never_exceeds_its_attempt_limit() {
        let policy: Policy = kani::any();
        let mut state: State = kani::any();
        let now: u64 = kani::any();
        // Both readings a consumer may act on give the same answer.
        let allowed = state.allowed(policy, now);
        assert!(allowed == state.next(policy).is_some_and(|at| now >= at));
        if allowed {
            state.reserve(policy, now);
            assert!(state.attempts <= policy.limit);
        }
        state.exhaust(policy);
        assert!(!state.allowed(policy, kani::any()));
        assert!(state.next(policy).is_none());
    }
}
