# Process handoff tests

Run `cargo test --test process_handoff`. These tests run on Linux, macOS and
Windows in CI. Each daemon and relay is a child process; the controller retains
the same client connection across replacement.

The scenarios adapt the lifecycle contracts from Kunobi's relay conformance and
broker/relay interop suites:

| Scenario | Required observation |
| --- | --- |
| Drain during a request | The old reply arrives before the old process exits; the successor waits for its ownership lock. |
| Relay reconnect | The same relay PID and client connection receive a reply identifying the new daemon PID and build. |
| Crash after acceptance | One error identifies the unanswered request; the successor never receives that request again. |
| Client input closes first | A late response still arrives, then the relay exits successfully. |
| Compatible live build | The existing daemon remains alive and no drain is requested. |
| Concurrent upgraders | Eight clients that observed the old build produce one replacement and agree on its live identity. |

`Lifecycle`, `ProcessLock`, `ensure_current`, `publish_record`, `WriterSlot` and the response pump
come from the library. The fixture supplies a small request protocol, health and
discovery adapters, and a supervisor callback that starts the child process.
It uses loopback TCP so the process contracts can run on every platform. It does
not test Unix-socket permissions, named-pipe ACLs, launchd/systemd, MCP bootstrap,
or runtime-manifest rollback; those remain consumer integration tests.

The shared transport regressions were extracted with the implementation from
Kunobi's `packages/kunobi-relay/src/pump.rs`. They cover pause boundaries, writer
replacement, ambiguous writes, half-close, byte preservation and the fixed memory
window. Protocol observation and the shipped relay's conformance tests remain
in Kunobi and exercise the shared dependency there.

Atomic publication tests cover concurrent publishers and readers, failed rename
cleanup, and owner-only files on Unix. The broker keeps its record format,
directory permissions, ownership cleanup and repair policy.
