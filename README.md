# kunobi-daemon

Coordinate local daemon upgrades without reporting success from a retiring
process or deleting the replacement's lock.

This crate provides:

- `ProcessLock`: exclusive ownership through a persistent OS file lock.
- `Lifecycle`: close admission on every session and wait for active request guards.
- `ensure_current`: serialize replacements, recheck after acquiring the upgrade
  lock, and return only a live response that satisfies the caller's version policy.
- `publish_record`: publish complete discovery records with an atomic rename.
- `transport`: the Kunobi relay's bounded byte pumps, replaceable writer and
  pause boundaries for newline-framed messages.

The crate is not yet published on crates.io. Consumer migration is being
validated separately.

Blocking relays can use `default-features = false` to obtain process locks and
transport and publication primitives with no runtime dependencies. The default `async` feature
adds Tokio-based draining and upgrade coordination.

## Request draining

```rust
use kunobi_daemon::{DrainOutcome, Lifecycle};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;

#[tokio::main(flavor = "current_thread")]
async fn main() {
let lifecycle = Arc::new(Lifecycle::default());
let request = lifecycle.begin().expect("admission is open");
// Handle the request and flush its response before dropping this guard.
drop(request);

let outcome = lifecycle
    .drain_until(Instant::now() + Duration::from_secs(30))
    .await;
assert_eq!(outcome, DrainOutcome::Complete);
assert!(lifecycle.begin().is_none());
}
```

A drain timeout reports how many guards remain. It does not cancel work or
reopen admission. A broker can continue waiting for a mutating request; a cache
can abort an upload after persisting enough state to retry it.

## Upgrade contract

Pass `ensure_current` a persistent upgrade-lock path, one deadline, a live health
probe, a version predicate, and a replacement callback. The probe returns
`Ok(None)` while no daemon is available. Probe errors propagate.

The fast path is one probe. When replacement is needed, the function acquires
the upgrade lock and probes again: a concurrent client may already have finished
the upgrade. After replacement, it waits for a fresh accepted response. An old
response, a successful spawn, or an existing socket is insufficient.

Health should read process identity from memory, independently of storage scans
and maintenance locks. Callbacks must yield and tolerate cancellation. The shared
deadline bounds cooperative async work; it cannot interrupt blocking code or
undo a service-manager command.

## Ownership and integration

Use different lock paths for daemon ownership and upgrade serialization. Keep
the daemon's ownership guard through endpoint cleanup. Every process that starts
or recovers that instance must follow the same locking protocol. Never unlink
or replace the lock file. Paths belong in a private application-owned directory.

Service-manager integration stays with the caller: a launchd/systemd-managed
daemon must be replaced through that manager. Transport framing, protocol
compatibility, process identity verification, durable queues, and rollback to a
previous binary also remain caller responsibilities.

Kache and Kunobi share these lifecycle requirements but have different upgrade
policies. Kache compares build epochs and has persistent upload jobs; the broker
checks protocol compatibility and carries calls that may modify external state.
The API accepts the version policy and reports unfinished work so both can keep
their existing semantics.

## Development

Rust 1.89 or newer. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D
warnings`, and `cargo test`. CI checks Linux, macOS, Windows, and the minimum Rust
version. Tests cover concurrent upgraders, stale replies, cancellation, multiple
drain observers, and file ownership across real child processes.

The [process handoff suite](tests/README.md) also checks that a relay keeps its
client session through replacement, preserves late replies and never replays
an ambiguously accepted request. Run it with `cargo test --test process_handoff`.

Apache-2.0. See [LICENSE](LICENSE).
