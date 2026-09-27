//! A dropped listener stops accepting even while other threads spawn children.
//!
//! Binder threads acquire and drop listeners in a loop while another thread
//! spawns children that sleep through the probe. A child that inherited a
//! listener keeps it accepting after its owner dropped it, and stale-endpoint
//! recovery then reports `AlreadyRunning` for as long as that child lives.
#![cfg(all(unix, feature = "local"))]

use kunobi_daemon::local::unix_socket::{self, Bound};
use std::collections::VecDeque;
use std::io;
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::SeqCst};
use std::time::{Duration, Instant};

/// Children to start while the binders run.
const SPAWNS: usize = 1000;
/// Children kept running at once. Older ones are stopped as new ones start.
const LIVE_CHILDREN: usize = 64;
/// Threads binding and dropping a listener, each on its own socket.
const BINDERS: usize = 4;
/// Upper bound on one binder's loop, in case the spawns are slow.
const MAX_BINDS: usize = 100_000;
/// On Linux a spawn can return just before the child closes its close-on-exec
/// descriptors. A listener still accepting this long after it was dropped is
/// held by a running child, which sleeps far longer than this. Generous, so a
/// stalled runner cannot pass for a leak.
const SETTLE: Duration = Duration::from_secs(5);
const PROBE_INTERVAL: Duration = Duration::from_millis(20);

type Spawn = fn(&mut Command) -> io::Result<Child>;

#[test]
fn a_child_spawned_by_the_crate_never_keeps_a_dropped_listener() {
    assert_no_child_keeps_a_dropped_listener(kunobi_daemon::local::unix::spawn_with_child_waiting);
}

/// Stands for a spawn the crate does not own, such as the application's own.
#[test]
#[cfg_attr(
    target_vendor = "apple",
    ignore = "macOS creates a socket, then marks it close-on-exec; only the \
              crate's own spawns wait for that"
)]
fn a_child_spawned_elsewhere_never_keeps_a_dropped_listener() {
    assert_no_child_keeps_a_dropped_listener(Command::spawn);
}

#[derive(Default)]
struct Race {
    over: AtomicBool,
    /// Spawns begun and returned. Once `returned` reaches a count `begun`
    /// had, every child from those spawns has exec'd.
    begun: AtomicUsize,
    returned: AtomicUsize,
    /// Binders checking a listener that still accepted after its drop. The
    /// spawner pauses meanwhile, so the child holding it keeps running.
    checking: AtomicUsize,
}

fn assert_no_child_keeps_a_dropped_listener(spawn: Spawn) {
    let root = tempfile::tempdir().unwrap();
    let race = Race::default();
    let start = Barrier::new(BINDERS + 1);
    let (spawned, binders) = std::thread::scope(|scope| {
        let binders: Vec<_> = (0..BINDERS)
            .map(|n| {
                let socket = root.path().join(n.to_string()).join("service.sock");
                let (race, start) = (&race, &start);
                scope.spawn(move || {
                    start.wait();
                    let result = bind_and_drop(&socket, race);
                    if result.is_err() {
                        race.over.store(true, SeqCst);
                    }
                    result
                })
            })
            .collect();
        start.wait();
        let spawned = spawn_children(spawn, &race);
        race.over.store(true, SeqCst);
        let binders: Vec<_> = binders
            .into_iter()
            .map(|binder| binder.join().unwrap())
            .collect();
        (spawned, binders)
    });
    spawned.expect("spawning a child");
    for (n, binder) in binders.into_iter().enumerate() {
        let binds = binder.unwrap_or_else(|failure| panic!("binder {n}: {failure}"));
        assert!(binds > 0, "binder {n} never ran alongside the spawns");
    }
}

/// Acquire and drop a listener until the spawns end, probing after each drop.
fn bind_and_drop(socket: &Path, race: &Race) -> Result<usize, String> {
    let mut binds = 0;
    while !race.over.load(SeqCst) && binds < MAX_BINDS {
        match unix_socket::acquire(socket) {
            Ok(Bound::Won(listener)) => drop(listener),
            other => {
                return Err(format!(
                    "bind {binds}: a free socket was not won: {other:?}"
                ));
            }
        }
        binds += 1;
        // A spawn still in progress may hold a copy of every descriptor until
        // its child execs. Wait for those; later spawns cannot see this one.
        let begun = race.begun.load(SeqCst);
        while race.returned.load(SeqCst) < begun {
            std::thread::yield_now();
        }
        match UnixStream::connect(socket) {
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => {}
            Err(error) => return Err(format!("bind {binds}: probe failed: {error}")),
            Ok(_) => {
                race.checking.fetch_add(1, SeqCst);
                let held = keeps_accepting(socket);
                race.checking.fetch_sub(1, SeqCst);
                if held? {
                    return Err(format!(
                        "bind {binds}: the dropped listener still accepts connections \
                         {SETTLE:?} later; a child spawned while it was created holds a copy"
                    ));
                }
            }
        }
        std::fs::remove_file(socket).map_err(|error| format!("bind {binds}: unlink: {error}"))?;
    }
    if !race.over.load(SeqCst) {
        return Err(format!(
            "stopped after {binds} binds, before the spawns ended, so the rest ran without this binder"
        ));
    }
    Ok(binds)
}

/// Whether `socket` still accepts connections once [`SETTLE`] has passed.
fn keeps_accepting(socket: &Path) -> Result<bool, String> {
    let deadline = Instant::now() + SETTLE;
    loop {
        std::thread::sleep(PROBE_INTERVAL);
        match UnixStream::connect(socket) {
            Ok(_) if Instant::now() >= deadline => return Ok(true),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => return Ok(false),
            Err(error) => return Err(format!("probe failed: {error}")),
        }
    }
}

/// Spawn children that sleep well past any probe, keep the latest few
/// running, and stop every one of them before returning.
fn spawn_children(spawn: Spawn, race: &Race) -> io::Result<()> {
    let mut children = VecDeque::new();
    let mut result = Ok(());
    for _ in 0..SPAWNS {
        while race.checking.load(SeqCst) > 0 {
            std::thread::sleep(PROBE_INTERVAL);
        }
        if race.over.load(SeqCst) {
            break;
        }
        let mut command = Command::new("/bin/sleep");
        command
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        race.begun.fetch_add(1, SeqCst);
        let child = spawn(&mut command);
        race.returned.fetch_add(1, SeqCst);
        match child {
            Ok(child) => children.push_back(child),
            Err(error) => {
                result = Err(error);
                break;
            }
        }
        if children.len() > LIVE_CHILDREN {
            stop(children.pop_front().unwrap());
        }
    }
    children.into_iter().for_each(stop);
    result
}

fn stop(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}
