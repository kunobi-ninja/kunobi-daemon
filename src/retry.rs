//! Persistable replacement attempt budgets. Never use these to replay requests.

/// Consumer policy for candidate retries across controller restarts.
#[derive(Clone, Copy, Debug)]
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
