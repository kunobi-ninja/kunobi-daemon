//! `Outstanding::wait_settled` under every interleaving loom can produce.
//!
//! A missed wakeup here is a session that never ends, so each test proves the
//! waiter returns however the other thread's steps are ordered.
//!
//! Run with
//! `RUSTFLAGS="--cfg loom" cargo test --release --no-default-features --test loom_outstanding`.
#![cfg(loom)]

use kunobi_daemon::transport::Outstanding;
use loom::sync::Arc;
use loom::thread;

#[test]
fn a_settle_on_another_thread_always_wakes_the_waiter() {
    loom::model(|| {
        let outstanding = Arc::new(Outstanding::new(4));
        outstanding.begin(1u32);
        let settler = Arc::clone(&outstanding);
        let epoch = outstanding.epoch();
        let settle = thread::spawn(move || settler.settle(epoch, &1));
        outstanding.wait_settled();
        settle.join().unwrap();
    });
}

#[test]
fn a_finished_failure_report_and_transition_release_the_waiter() {
    loom::model(|| {
        let outstanding = Arc::new(Outstanding::new(4));
        outstanding.begin(1u32);
        let waiter = Arc::clone(&outstanding);
        let wait = thread::spawn(move || waiter.wait_settled());
        drop(outstanding.fail());
        drop(outstanding.transition());
        wait.join().unwrap();
    });
}

#[test]
fn only_the_settle_that_empties_the_set_has_to_wake_the_waiter() {
    // Two settlers race. The first leaves a request outstanding and need not
    // wake anyone; the second empties the set and must wake the waiter, whether
    // it blocked before both settles or between them.
    loom::model(|| {
        let outstanding = Arc::new(Outstanding::new(4));
        outstanding.begin(1u32);
        outstanding.begin(2u32);
        let epoch = outstanding.epoch();
        let settlers: Vec<_> = [1u32, 2]
            .into_iter()
            .map(|key| {
                let settler = Arc::clone(&outstanding);
                thread::spawn(move || settler.settle(epoch, &key))
            })
            .collect();
        outstanding.wait_settled();
        for settler in settlers {
            settler.join().unwrap();
        }
    });
}

#[test]
fn a_settled_set_wakes_every_waiter() {
    // The first waiter to leave unregisters while the second may still be
    // blocked; the second must not be stranded.
    loom::model(|| {
        let outstanding = Arc::new(Outstanding::new(4));
        outstanding.begin(1u32);
        let waiters: Vec<_> = (0..2)
            .map(|_| {
                let waiter = Arc::clone(&outstanding);
                thread::spawn(move || waiter.wait_settled())
            })
            .collect();
        outstanding.settle(outstanding.epoch(), &1);
        for waiter in waiters {
            waiter.join().unwrap();
        }
    });
}

#[test]
fn clearing_an_overflow_wakes_the_waiter() {
    // Settling the one tracked request empties the set but leaves it
    // overflowed, so it is not settled; only the receipt may release the waiter.
    loom::model(|| {
        let outstanding = Arc::new(Outstanding::new(1));
        outstanding.begin(1u32);
        outstanding.begin(2u32);
        let waiter = Arc::clone(&outstanding);
        let wait = thread::spawn(move || waiter.wait_settled());
        outstanding.settle(outstanding.epoch(), &1);
        outstanding.clear();
        wait.join().unwrap();
    });
}
