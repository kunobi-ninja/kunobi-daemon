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

/// Whole campaigns under generated policies and retry times.
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn policy() -> impl Strategy<Value = Policy> {
        let seconds = || prop_oneof![Just(0u64), 1u64..3_600, any::<u64>()];
        (0u64..12, seconds(), seconds()).prop_map(|(limit, delay, maximum_delay)| Policy {
            limit,
            delay,
            maximum_delay,
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        })]

        #[test]
        fn a_campaign_gets_exactly_its_limit_with_growing_capped_delays(
            policy in policy(),
            start in prop_oneof![0u64..1_000_000, any::<u64>()],
            lateness in proptest::collection::vec(prop_oneof![Just(0u64), 0u64..10_000], 12),
        ) {
            let mut state = State::default();
            let mut now = start;
            let mut reserved = 0;
            let mut previous_delay = 0;
            for late in lateness {
                let Some(next) = state.next(policy) else { break };
                if next > 0 {
                    prop_assert!(!state.allowed(policy, next - 1), "allowed before its delay");
                }
                // The caller retries at or after the permitted time.
                now = now.max(next).saturating_add(late);
                prop_assert!(state.allowed(policy, now));
                state.reserve(policy, now);
                reserved += 1;

                prop_assert!(state.next_attempt >= now);
                // A saturated clock cannot show the delay.
                if state.next_attempt != u64::MAX {
                    let delay = state.next_attempt - now;
                    prop_assert!(delay <= policy.maximum_delay, "delay {} over the cap", delay);
                    prop_assert!(delay >= policy.delay.min(policy.maximum_delay));
                    prop_assert!(delay >= previous_delay, "delay shrank to {}", delay);
                    previous_delay = delay;
                }
            }
            prop_assert_eq!(reserved, policy.limit);
            prop_assert_eq!(state.next(policy), None);
            prop_assert!(!state.allowed(policy, u64::MAX));
        }
    }
}
