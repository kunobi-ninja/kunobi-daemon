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
