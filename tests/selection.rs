//! Regression tests for waiting on a replacement another process drives.

use kunobi_daemon::selection::{Budget, Evidence, Selection, await_selection};
use std::time::{Duration, Instant};

/// A candidate whose selection and readiness follow a script. Each round reads
/// `committed` then calls `probe`; the log records that order.
struct Script {
    /// The selection names the candidate from this round on. `None`: never.
    commit_at_round: Option<usize>,
    /// Commit while this round's probe is still looking at the incumbent.
    commit_during_probe: Option<usize>,
    /// Probes from this round on return a proof. `None`: never.
    ready_at_round: Option<usize>,
    round: usize,
    committed: bool,
    log: Vec<&'static str>,
}

impl Script {
    fn new() -> Self {
        Self {
            commit_at_round: None,
            commit_during_probe: None,
            ready_at_round: None,
            round: 0,
            committed: false,
            log: Vec::new(),
        }
    }
}

impl Evidence for Script {
    type Proof = &'static str;
    type Error = &'static str;

    fn committed(&mut self) -> Result<bool, Self::Error> {
        self.log.push("committed");
        if self
            .commit_at_round
            .is_some_and(|round| self.round >= round)
        {
            self.committed = true;
        }
        Ok(self.committed)
    }

    fn probe(&mut self, _deadline: Instant) -> Result<Option<Self::Proof>, Self::Error> {
        self.log.push("probe");
        let round = self.round;
        self.round += 1;
        // The probe reached the incumbent; the candidate commits meanwhile.
        if self.commit_during_probe == Some(round) {
            self.committed = true;
            return Ok(None);
        }
        Ok(self
            .ready_at_round
            .is_some_and(|ready| round >= ready)
            .then_some("candidate"))
    }
}

const BUDGET: Budget = Budget {
    commit: Duration::from_secs(2),
    proof: Duration::from_millis(300),
};

#[test]
fn a_commit_during_a_probe_of_the_incumbent_waits_for_the_candidate_s_own_proof() {
    // The candidate commits while round 0 is still probing the incumbent, and
    // answers from round 1. Pairing round 0's stale probe with the new record
    // reported an unproven commit although the candidate was serving.
    let mut script = Script::new();
    script.commit_during_probe = Some(0);
    script.ready_at_round = Some(1);
    assert_eq!(
        await_selection(BUDGET, &mut script),
        Ok(Selection::Current("candidate"))
    );
    assert_eq!(script.log, ["committed", "probe", "committed", "probe"]);
}

#[test]
fn every_round_reads_the_selection_before_probing() {
    let mut script = Script::new();
    script.ready_at_round = Some(3);
    assert_eq!(
        await_selection(BUDGET, &mut script),
        Ok(Selection::Current("candidate"))
    );
    let rounds: Vec<_> = script.log.chunks(2).collect();
    assert_eq!(rounds.len(), 4);
    assert!(rounds.iter().all(|round| *round == ["committed", "probe"]));
}

#[test]
fn a_committed_candidate_that_never_answers_is_reported_committed_after_the_proof_budget() {
    let mut script = Script::new();
    script.commit_at_round = Some(0);
    let start = Instant::now();
    assert_eq!(
        await_selection(BUDGET, &mut script),
        Ok(Selection::Committed)
    );
    let elapsed = start.elapsed();
    assert!(elapsed >= BUDGET.proof, "gave up after {elapsed:?}");
    assert!(
        elapsed < BUDGET.commit,
        "waited out the commit budget: {elapsed:?}"
    );
}

#[test]
fn a_zero_proof_budget_reports_the_first_unproven_commit() {
    let mut script = Script::new();
    script.commit_at_round = Some(0);
    let budget = Budget {
        proof: Duration::ZERO,
        ..BUDGET
    };
    assert_eq!(
        await_selection(budget, &mut script),
        Ok(Selection::Committed)
    );
    assert_eq!(script.log, ["committed", "probe"]);
}

#[test]
fn a_candidate_that_never_commits_is_not_committed_at_the_commit_budget() {
    let mut script = Script::new();
    let budget = Budget {
        commit: Duration::from_millis(200),
        ..BUDGET
    };
    let start = Instant::now();
    assert_eq!(
        await_selection(budget, &mut script),
        Ok(Selection::NotCommitted)
    );
    assert!(start.elapsed() >= budget.commit);
}

#[test]
fn a_record_that_reads_older_after_a_commit_does_not_withdraw_it() {
    struct Flicker(usize);
    impl Evidence for Flicker {
        type Proof = ();
        type Error = ();
        fn committed(&mut self) -> Result<bool, ()> {
            self.0 += 1;
            Ok(self.0 == 1)
        }
        fn probe(&mut self, _: Instant) -> Result<Option<()>, ()> {
            Ok(None)
        }
    }
    assert_eq!(
        await_selection(BUDGET, &mut Flicker(0)),
        Ok(Selection::Committed)
    );
}

#[test]
fn a_proof_returned_after_the_round_deadline_is_rejected() {
    struct Late;
    impl Evidence for Late {
        type Proof = ();
        type Error = ();
        fn committed(&mut self) -> Result<bool, ()> {
            Ok(false)
        }
        fn probe(&mut self, deadline: Instant) -> Result<Option<()>, ()> {
            std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            Ok(Some(()))
        }
    }
    let budget = Budget {
        commit: Duration::from_millis(50),
        proof: Duration::ZERO,
    };
    assert_eq!(
        await_selection(budget, &mut Late),
        Ok(Selection::NotCommitted)
    );
}

#[test]
fn record_and_probe_errors_end_the_wait() {
    struct Failing(bool);
    impl Evidence for Failing {
        type Proof = ();
        type Error = &'static str;
        fn committed(&mut self) -> Result<bool, Self::Error> {
            if self.0 {
                Err("record unreadable")
            } else {
                Ok(false)
            }
        }
        fn probe(&mut self, _: Instant) -> Result<Option<()>, Self::Error> {
            Err("peer is another user")
        }
    }
    assert_eq!(
        await_selection(BUDGET, &mut Failing(true)),
        Err("record unreadable")
    );
    assert_eq!(
        await_selection(BUDGET, &mut Failing(false)),
        Err("peer is another user")
    );
}

#[cfg(feature = "async")]
mod asynchronous {
    use super::BUDGET;
    use kunobi_daemon::selection::{AsyncEvidence, Budget, Selection, await_selection_async};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use tokio::time::Instant;

    /// The candidate commits while the first probe is looking at the incumbent.
    struct Handoff {
        committed: Arc<AtomicBool>,
        probes: usize,
    }

    impl AsyncEvidence for Handoff {
        type Proof = usize;
        type Error = ();

        async fn committed(&mut self) -> Result<bool, ()> {
            Ok(self.committed.load(Ordering::SeqCst))
        }

        async fn probe(&mut self) -> Result<Option<usize>, ()> {
            self.probes += 1;
            if self.probes == 1 {
                self.committed.store(true, Ordering::SeqCst);
                return Ok(None);
            }
            Ok(Some(self.probes))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_commit_during_a_probe_of_the_incumbent_waits_for_the_candidate_s_own_proof() {
        let mut handoff = Handoff {
            committed: Arc::new(AtomicBool::new(false)),
            probes: 0,
        };
        assert_eq!(
            await_selection_async(BUDGET, &mut handoff).await,
            Ok(Selection::Current(2))
        );
    }

    /// The incumbent answers slowly, and the candidate commits meanwhile.
    struct SlowIncumbent {
        committed: bool,
        probes: usize,
    }

    impl AsyncEvidence for SlowIncumbent {
        type Proof = ();
        type Error = ();

        async fn committed(&mut self) -> Result<bool, ()> {
            Ok(self.committed)
        }

        async fn probe(&mut self) -> Result<Option<()>, ()> {
            self.probes += 1;
            if self.probes == 1 {
                tokio::time::sleep(Duration::from_millis(40)).await;
                self.committed = true;
                return Ok(None);
            }
            Ok(Some(()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn the_proof_budget_starts_when_the_commit_is_read_before_a_probe() {
        // Reading the record after the slow probe of the incumbent would start
        // the 20ms budget there and spend it waiting, so the candidate's own
        // probe never got a chance. Read first, and that probe gets all of it.
        let budget = Budget {
            commit: Duration::from_secs(2),
            proof: Duration::from_millis(20),
        };
        let mut evidence = SlowIncumbent {
            committed: false,
            probes: 0,
        };
        assert_eq!(
            await_selection_async(budget, &mut evidence).await,
            Ok(Selection::Current(()))
        );
        assert_eq!(evidence.probes, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_probe_that_outlives_its_round_counts_as_no_proof() {
        struct Hung;
        impl AsyncEvidence for Hung {
            type Proof = ();
            type Error = ();
            async fn committed(&mut self) -> Result<bool, ()> {
                Ok(true)
            }
            async fn probe(&mut self) -> Result<Option<()>, ()> {
                std::future::pending().await
            }
        }
        let start = Instant::now();
        assert_eq!(
            await_selection_async(BUDGET, &mut Hung).await,
            Ok(Selection::Committed)
        );
        // The hung probe was cut at the proof budget, not the commit budget.
        assert!(start.elapsed() >= BUDGET.proof);
        assert!(start.elapsed() < BUDGET.commit);
    }

    #[tokio::test(start_paused = true)]
    async fn a_wakeable_source_replaces_the_poll_interval() {
        // A source with its own wake-up keeps the decision rules unchanged.
        struct Woken(usize);
        impl AsyncEvidence for Woken {
            type Proof = ();
            type Error = ();
            async fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            async fn probe(&mut self) -> Result<Option<()>, ()> {
                self.0 += 1;
                Ok((self.0 == 3).then_some(()))
            }
            async fn wait(&mut self, _until: Instant) {}
        }
        let start = Instant::now();
        let budget = Budget {
            commit: Duration::from_secs(1),
            proof: Duration::ZERO,
        };
        assert_eq!(
            await_selection_async(budget, &mut Woken(0)).await,
            Ok(Selection::Current(()))
        );
        assert_eq!(start.elapsed(), Duration::ZERO, "slept despite being woken");
    }
}
