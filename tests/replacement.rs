//! Replacement ordering and failure boundaries.
use kunobi_daemon::{
    ProcessLock,
    replacement::{self, Budgets, Driver, Mode, Outcome, Progress, Reason, Step},
};
use std::{
    convert::Infallible,
    time::{Duration, Instant},
};

struct Recorder {
    steps: Vec<Step>,
    fail: Option<Step>,
    pending: Option<Step>,
    warmup: Vec<std::path::PathBuf>,
}
impl Driver for Recorder {
    type Error = &'static str;
    fn warmup_paths(&self) -> Vec<std::path::PathBuf> {
        self.warmup.clone()
    }
    fn perform(&mut self, step: Step, _: Option<Instant>) -> Result<Progress, Self::Error> {
        self.steps.push(step);
        if self.fail == Some(step) {
            return Err("injected failure");
        }
        if self.pending == Some(step) {
            self.pending = None;
            return Ok(Progress::Pending);
        }
        Ok(Progress::Done)
    }
}
fn budgets() -> Budgets {
    Budgets {
        setup: Duration::from_secs(1),
        drain: None,
    }
}

#[test]
fn exclusive_releases_the_incumbent_before_start_and_overlap_selects_before_retire() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    for (mode, expected) in [
        (
            Mode::Exclusive,
            vec![
                Step::Recheck,
                Step::Prepare,
                Step::Drain,
                Step::Start,
                Step::Verify,
                Step::Validate,
                Step::Commit,
            ],
        ),
        (
            Mode::Overlap,
            vec![
                Step::Recheck,
                Step::Prepare,
                Step::Start,
                Step::Verify,
                Step::Validate,
                Step::Commit,
                Step::Retire,
            ],
        ),
    ] {
        let mut driver = Recorder {
            steps: vec![],
            fail: None,
            pending: None,
            warmup: vec![],
        };
        assert_eq!(
            replacement::run(&lock, mode, budgets(), &mut driver).unwrap(),
            Outcome::Complete
        );
        assert_eq!(driver.steps, expected);
    }
}

#[test]
fn failures_do_not_advance_and_report_the_commit_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    for step in [
        Step::Recheck,
        Step::Prepare,
        Step::Start,
        Step::Verify,
        Step::Validate,
        Step::Commit,
        Step::Retire,
    ] {
        let mut driver = Recorder {
            steps: vec![],
            fail: Some(step),
            pending: None,
            warmup: vec![],
        };
        let error = replacement::run(&lock, Mode::Overlap, budgets(), &mut driver).unwrap_err();
        assert_eq!(error.step, step);
        assert_eq!(error.committed, step == Step::Retire);
        assert_eq!(driver.steps.last(), Some(&step));
        assert!(matches!(error.reason, Reason::Adapter("injected failure")));
    }
}

#[test]
fn pending_verification_retries_only_the_probe_and_pending_retirement_is_not_completion() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let mut driver = Recorder {
        steps: vec![],
        fail: None,
        pending: Some(Step::Verify),
        warmup: vec![],
    };
    replacement::run(&lock, Mode::Overlap, budgets(), &mut driver).unwrap();
    assert_eq!(
        driver.steps.iter().filter(|&&s| s == Step::Start).count(),
        1
    );
    assert_eq!(
        driver.steps.iter().filter(|&&s| s == Step::Verify).count(),
        2
    );
    let mut driver = Recorder {
        steps: vec![],
        fail: None,
        pending: Some(Step::Retire),
        warmup: vec![],
    };
    assert_eq!(
        replacement::run(&lock, Mode::Overlap, budgets(), &mut driver).unwrap(),
        Outcome::RetirementPending
    );
}

#[test]
fn commit_prefaults_warmup_paths_and_ignores_missing_files() {
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let shim = dir.path().join("shim");
    std::fs::write(&shim, vec![1; 8_192]).unwrap();
    let mut driver = Recorder {
        steps: vec![],
        fail: None,
        pending: None,
        warmup: vec![shim, dir.path().join("absent")],
    };
    assert_eq!(
        replacement::run(&lock, Mode::Exclusive, budgets(), &mut driver).unwrap(),
        Outcome::Complete
    );
    assert_eq!(*driver.steps.last().unwrap(), Step::Commit);
}

#[test]
fn unchanged_and_errors_from_readiness_do_not_spawn() {
    struct Existing;
    impl Driver for Existing {
        type Error = Infallible;
        fn perform(&mut self, step: Step, _: Option<Instant>) -> Result<Progress, Self::Error> {
            assert_eq!(step, Step::Recheck);
            Ok(Progress::Unchanged)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    assert_eq!(
        replacement::run(&lock, Mode::Overlap, budgets(), &mut Existing).unwrap(),
        Outcome::Unchanged
    );
    let result = kunobi_daemon::readiness::wait_until::<(), _>(
        Instant::now() + Duration::from_secs(1),
        |_| Err("permission denied"),
    );
    assert_eq!(result, Err("permission denied"));
}

#[cfg(feature = "async")]
#[tokio::test(start_paused = true)]
async fn async_exclusive_drain_can_last_hours_without_spending_candidate_startup_budget() {
    use replacement::AsyncDriver;
    struct SlowDrain(Vec<Step>);
    impl AsyncDriver for SlowDrain {
        type Error = Infallible;
        async fn perform(
            &mut self,
            step: Step,
            deadline: Option<tokio::time::Instant>,
        ) -> Result<Progress, Self::Error> {
            self.0.push(step);
            if step == Step::Drain {
                assert!(deadline.is_none());
                tokio::time::sleep(Duration::from_secs(7200)).await;
            } else {
                assert!(deadline.unwrap() > tokio::time::Instant::now());
            }
            Ok(Progress::Done)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let mut driver = SlowDrain(vec![]);
    assert_eq!(
        replacement::run_async(&lock, Mode::Exclusive, budgets(), &mut driver)
            .await
            .unwrap(),
        Outcome::Complete
    );
    assert_eq!(driver.0[2..4], [Step::Drain, Step::Start]);
}

#[cfg(feature = "async")]
#[tokio::test(start_paused = true)]
async fn stalled_candidate_cannot_commit_and_failed_preparation_cannot_drain() {
    use replacement::AsyncDriver;
    struct Stalled {
        steps: Vec<Step>,
        failed: bool,
    }
    impl AsyncDriver for Stalled {
        type Error = &'static str;
        async fn perform(
            &mut self,
            step: Step,
            _: Option<tokio::time::Instant>,
        ) -> Result<Progress, Self::Error> {
            self.steps.push(step);
            if step == Step::Prepare && self.failed {
                return Err("bad artifact");
            }
            if step == Step::Verify {
                std::future::pending::<()>().await;
            }
            Ok(Progress::Done)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let mut driver = Stalled {
        steps: vec![],
        failed: false,
    };
    let failure = replacement::run_async(&lock, Mode::Overlap, budgets(), &mut driver)
        .await
        .unwrap_err();
    assert!(matches!(failure.reason, Reason::Deadline));
    assert_eq!(failure.step, Step::Verify);
    assert!(!failure.committed);
    assert!(!driver.steps.contains(&Step::Commit));
    let mut driver = Stalled {
        steps: vec![],
        failed: true,
    };
    replacement::run_async(&lock, Mode::Exclusive, budgets(), &mut driver)
        .await
        .unwrap_err();
    assert!(!driver.steps.contains(&Step::Drain));
}

#[test]
fn an_initializing_owner_is_waited_for_without_preparation_or_drain() {
    struct Initializing(bool);
    impl Driver for Initializing {
        type Error = Infallible;
        fn perform(&mut self, step: Step, _: Option<Instant>) -> Result<Progress, Self::Error> {
            assert_eq!(step, Step::Recheck);
            Ok(if std::mem::replace(&mut self.0, true) {
                Progress::Unchanged
            } else {
                Progress::Pending
            })
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    for mode in [Mode::Exclusive, Mode::Overlap] {
        assert_eq!(
            replacement::run(&lock, mode, budgets(), &mut Initializing(false)).unwrap(),
            Outcome::Unchanged
        );
    }
}

#[cfg(feature = "async")]
#[tokio::test(start_paused = true)]
async fn an_initializing_owner_timeout_does_not_drain_or_spawn() {
    struct Initializing;
    impl replacement::AsyncDriver for Initializing {
        type Error = Infallible;
        async fn perform(
            &mut self,
            step: Step,
            _: Option<tokio::time::Instant>,
        ) -> Result<Progress, Self::Error> {
            assert_eq!(step, Step::Recheck);
            Ok(Progress::Pending)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let lock = ProcessLock::try_acquire(dir.path().join("upgrade.lock"))
        .unwrap()
        .unwrap();
    let error = replacement::run_async(&lock, Mode::Exclusive, budgets(), &mut Initializing)
        .await
        .unwrap_err();
    assert_eq!(error.step, Step::Recheck);
    assert!(!error.committed);
    assert!(matches!(error.reason, Reason::Deadline));
}

#[cfg(feature = "async")]
#[tokio::test(start_paused = true)]
async fn stateful_readiness_retains_the_probe_and_bounds_a_stalled_peer() {
    struct Probe(u8);
    impl kunobi_daemon::readiness::AsyncProbe for Probe {
        type Proof = u8;
        type Error = Infallible;
        async fn probe(&mut self) -> Result<Option<u8>, Infallible> {
            self.0 += 1;
            if self.0 == 3 {
                std::future::pending::<()>().await;
            }
            Ok((self.0 == 2).then_some(self.0))
        }
    }
    let mut probe = Probe(0);
    let result = kunobi_daemon::readiness::wait_until_async(
        tokio::time::Instant::now() + Duration::from_secs(1),
        &mut probe,
    )
    .await
    .unwrap();
    assert_eq!(result, Some(2));
    assert_eq!(probe.0, 2);
    let result = kunobi_daemon::readiness::wait_until_async(
        tokio::time::Instant::now() + Duration::from_secs(1),
        &mut probe,
    )
    .await
    .unwrap();
    assert_eq!(result, None);
    assert_eq!(probe.0, 3);
}
