# kunobi-daemon

Coordinate local daemon upgrades without reporting success from a retiring
process or deleting the replacement's lock.

Use the shared coordinator for ownership, readiness, drain and replacement.
Applications provide their messages, build policy, launch mechanism and durable
work. The crate has no MCP, cache database or telemetry exporter dependency.

- `replacement`: one transition machine for exclusive and overlapping upgrades,
  with blocking and async drivers.
- `selection`: wait for a replacement another process drives. Reports a fresh
  proof, an authoritative but unproven commit, or no commit.
- `ProcessLock`, `RecordSlot` and `local::unix_socket`: persistent ownership,
  atomic publication and cleanup that preserves a successor's endpoint.
- `Lifecycle` and `generation`: request guards, generation selection, session
  leases and retirement. `retry` bounds persisted candidate campaigns.
- `wire` and `control`: Buffa Protobuf negotiation, typed health and drain replies.
- `local` and `transport`: optional OS peer checks, setup deadlines, half-close,
  byte pumps and replaceable writers.
- `launch`: clients that may start the daemon, including `DaemonCommand` for a
  daemon fully detached from its caller. Off by default; needs `local`.
- `admission` and `observation`: independent capacity pools and local telemetry data.

See [Daemon lifecycle and replacement](https://github.com/kunobi-ninja/kunobi-daemon/blob/main/docs/architecture.md) for the transition
ordering, failure boundaries and consumer responsibilities.

The default `async` feature adds Tokio-based lifecycle and generation support.
Blocking clients use `default-features = false`; `wire` and `local` do not create
a runtime. `wire-async` adds the async protocol and control handler.
`local-async` adds the Tokio Windows listener with an explicit local-owner ACL.
`launch` is the client-side start recipe on top of `local`. A daemon that only
binds does not enable it.

A daemon a client starts should outlive that client and not tie it up.
`launch::DaemonCommand` starts one in a new session on Unix, so Ctrl-C in the
caller's terminal and the terminal's hangup do not reach it. On Windows it gets
its own hidden console and inherits only its three standard handles, so a pipe
the caller holds, such as a build tool's output pipe, is not kept open by the
daemon. On Unix every descriptor above stderr is closed across the exec for
the same reason.

On Windows the daemon also leaves the caller's job object when the job allows
it. A job that forbids breakaway keeps it, and cargo's job does: a daemon
started under cargo on Windows still dies with cargo on Ctrl-C.
`DaemonChild::in_callers_job` reports that case. With nested jobs the daemon
leaves the ones that allow breakaway and stays in any enclosing job that does
not, without that being reported. Starting the daemon from outside the job,
through a scheduled task or a service, is the only way around it.

`local::process_state` reports a PID as alive, exited or unknown, and never
folds an access-denied query into either answer. Alive describes the PID, which
the OS reuses, so confirm the daemon through its endpoint before trusting it.

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

Acquire an upgrade `ProcessLock` and call `replacement::run` or `run_async` with
an application driver. The coordinator orders recheck, preparation, drain when
required, start, live verification, validation, commit and retirement. A driver
performs one requested step; it does not implement another transition loop.

Choose exclusive replacement when generations cannot share mutable resources.
Overlap starts and verifies a candidate before selecting it, then preserves the
incumbent until its session obligations finish. Startup and drain have separate
budgets. The crate never turns a startup timeout into permission to kill work.

`ensure_current` remains available for existing integrations with an indivisible
replacement callback. New integrations should use the explicit coordinator.

## Ownership and integration

Use different lock paths for daemon ownership and upgrade serialization. Keep
the daemon's ownership guard through endpoint cleanup. Every process that starts
or recovers that instance must follow the same locking protocol. Never unlink
or replace the lock file. Paths belong in a private application-owned directory.

Service-manager integration stays with the caller: a launchd/systemd-managed
daemon must be replaced through that manager. Applications provide message
schemas, build compatibility policy, durable queues and any rollback policy. Shared wire identity validation and optional OS
peer checks must run before application dispatch.

Kache and Kunobi share these lifecycle requirements but have different upgrade
policies. Kache compares build epochs and has persistent upload jobs; the broker
checks protocol compatibility and carries calls that may modify external state.
The API accepts the version policy and reports unfinished work so both can keep
their existing semantics.

## Development

Rust 1.89 or newer. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D
warnings`, and `cargo test`. CI checks Linux, macOS, native Windows, a
`cargo xwin` MSVC link from Linux, and the minimum Rust version. Tests cover
concurrent upgraders, stale replies, cancellation, multiple drain observers,
and file ownership across real child processes.

The [process handoff suite](https://github.com/kunobi-ninja/kunobi-daemon/blob/main/tests/README.md) also checks that a relay keeps its
client session through replacement, preserves late replies and never replays
an ambiguously accepted request. Run it with `cargo test --test process_handoff`.

Apache-2.0. See [LICENSE](https://github.com/kunobi-ninja/kunobi-daemon/blob/main/LICENSE).

## Binary lifecycle protocol

The optional [`wire` and `wire-async` features](https://github.com/kunobi-ninja/kunobi-daemon/blob/main/docs/wire.md) provide bounded
Protobuf messages and version/capability negotiation on a separate endpoint.
Legacy clients retain their listener and message format. Both decoders feed the same lifecycle and handoff logic. A client that discovers
a binary endpoint must not downgrade after a failed negotiation.

## Capacity and local observations

`admission::Admission` gives handshake, application and control connections
independent budgets. Defaults are 32 pending handshakes, 256 application sessions
and 32 control sessions; consumers can configure each with `Limits`. Authenticate
and classify connections before assigning an application or control permit.
Never dispatch application work using a control permit. Permits release on drop.
These defaults are capacity bounds, not measured throughput guarantees.

`observation::Observations` holds local counters and a bounded queue of typed
events. `ObservedIo` counts blocking I/O; `AsyncObservedIo` does the same with
`wire-async`. Their snapshots remain readable while a write is pending. They
report bytes accepted by the transport, pending I/O, continuous busy time, time
since progress, errors and connection count. An async Pending poll remains
recorded until completion or transport drop; cancelling its caller does not prove
that the underlying I/O stopped. Counters are approximate concurrent samples.

`Lifecycle::snapshot` reports active request guards and drain state.
`Lifecycle::observed` also records drain transitions. Consumers record their own
rejection and handoff events with `Observations::record`, then decide how to log,
aggregate or export them. `take_events` consumes the shared queue. Producers drop
events instead of waiting on a full or busy queue; `lost_events` makes that loss
visible. No exporter, background task, user callback, payload or token is stored.
Use a separate observation instance when per-connection progress is needed.

The optional `local` adapters provide OS peer checks and absolute setup deadlines
for both reads and writes. Call authentication before sending protocol bytes. Clear setup deadlines before ordinary application traffic. Long jobs
and slow readers are application policy; observations never impose a job timeout
or cancel work.

`warm_executable` faults a client shim into the OS page cache without running
it. Call it when the daemon starts and after a replacement commit (drivers can
list paths on `replacement::Driver::warmup_paths`; missing files do not fail
the commit). `warm_spawn` is the extra step for macOS's per-path first-exec
cache: the child argv must exit without connecting or taking locks. Do not
poll on a timer; the miss that costs ~100 ms is a new path, not eviction.
