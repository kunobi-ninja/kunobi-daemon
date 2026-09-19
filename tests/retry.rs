//! Persisted candidate retry budgets.
use kunobi_daemon::retry::{Policy, State};

#[test]
fn persisted_attempts_bound_post_commit_crash_loops_and_delays() {
    let policy = Policy {
        limit: 6,
        delay: 30,
        maximum_delay: 300,
    };
    let mut state = State::default();
    assert!(state.allowed(policy, 100));
    for delay in [60, 120, 240, 300, 300, 300] {
        let now = state.next_attempt.max(100);
        state.reserve(policy, now);
        assert_eq!(state.next_attempt, now + delay);
        assert!(!state.allowed(policy, state.next_attempt - 1));
    }
    assert_eq!(state.next(policy), None);
    assert!(!state.allowed(policy, u64::MAX));
    let mut invalid = State::default();
    invalid.exhaust(policy);
    assert!(!invalid.allowed(policy, u64::MAX));
    state.attempts = u64::MAX;
    state.reserve(policy, u64::MAX);
    assert_eq!(state.attempts, u64::MAX);
    assert_eq!(state.next_attempt, u64::MAX);
}
