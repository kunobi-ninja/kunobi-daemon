# Binary lifecycle control

Enable `wire` for blocking clients or `wire-async` for Tokio servers. The feature
is optional; existing users keep the std-only transport unless they enable it.
Buffa 0.9.2 encodes and decodes the binary messages, which follow [the Protobuf schema](../proto/lifecycle.proto).
Generated Rust types are checked in; consumers do not need a schema compiler.
The format is experimental until the first consumer release.

## Service identity

Generate a UUID once per service and keep it in the application's package.
`ServiceIdentity::new` takes its 16 bytes, an application name, a profile and an
instance. Use the same identity for `identity.paths(private_user_root)?` and
`Hello::new(&identity, supported, required)`. The UUID, name, profile and instance
must all match during negotiation. A mismatch closes the connection before any
application operation is dispatched. Nil or incorrectly sized UUIDs are invalid.

The resource directory is scoped by UUID, profile and instance. A different
application UUID gets separate ownership and upgrade locks, discovery and a
control socket, even if someone copies the application's display name. A binary
version or PID is never part of the identity: replacement must find its predecessor.
Existing consumers can retain their secure, application-specific paths when
migrating, especially the legacy endpoint required by old clients.

The caller owns the private per-user directory, permission checks and endpoint
binding. Keep Unix socket paths within the platform's length limit. Windows
named pipes must include the service identity and user SID and enforce a
same-user ACL. UUIDs prevent accidental cross-service connections; they do not
authenticate a process that copies another service's public UUID.

## Application-owned messages

`Control.kind` separates lifecycle and application operations. Both namespaces
start at operation 1; application IDs need not be renumbered to use this crate.
The shared crate dispatches no application work itself. The consumer checks the
operation ID and decodes its own Protobuf schema from `payload`.

Shared capability bits 0 through 15 are reserved. Applications own bits 16 through
63 within their service identity. Require the feature's bit, or check that it was
negotiated before sending the corresponding operation. `APPLICATION` alone only
means the peer understands the envelope, not every operation the application may
add. Reject unsupported IDs in the application handler.

See the [application-message example](../examples/application_messages.rs):

```sh
cargo run --no-default-features --features wire --example application_messages
```

## Typed lifecycle replies

`HEALTH_DETAILS` (bit 4) adds the shared `Health` payload to HEALTH and DRAIN
responses. Require this bit before decoding that payload. Existing peers that
only advertise HEALTH retain their original consumer-defined reply format.
`Health` carries process ID, generation, build, revision, readiness, drain state
and active request count. Validate it against the kernel peer PID as well as the
request identity. DRAIN acknowledges closed admission immediately; active work
can continue after that acknowledgement.

## Keep old clients working

Keep the legacy discovery schema, endpoint key, listener, handshake and message
format unchanged. Bind a separate binary endpoint, then publish it as an optional
field alongside the legacy endpoint. Never put a binary preamble on the legacy
socket. In Kunobi the added field is `controlSocketV2`; existing MCP data sessions
and their preamble protocol versions are independent of this control codec.

A new client uses the binary endpoint only when explicitly advertised. An older
discovery record selects legacy. An empty or aliased binary endpoint is an error.
A failed binary handshake requires rediscovery or an explicit error, not an
implicit downgrade. No operation is replayed after a connection failure.

Both adapters invoke the same admission and handoff coordinator. Adding a codec
does not change drain, receipt boundaries, OS peer authorization, or the policy
for sessions that cannot migrate. Keep historical client binaries in consumer
CI. Remove a legacy adapter only after its documented supported-client window
has ended and its corresponding compatibility baseline has been retired in the
same change. This change removes no legacy protocol support.

## Connection contract

Authenticate the OS peer before invoking the codec. Apply one establishment
deadline to all negotiation I/O; blocking transports must enforce that deadline
across individual reads and writes. The async adapter can be wrapped in
`tokio::time::timeout`. The caller owns subsequent operation deadlines.

1. Client writes the eight bytes `KNDPB002`, then its framed `Hello`.
2. Server validates the application identifier, version overlap, required
   capabilities and frame limits. It replies with its framed `Hello`.
3. Client independently validates the agreement and writes byte `1`.
4. Server reads that acceptance and writes byte `1`.
5. Both sides may now exchange framed `Control` messages.

The service identity is not a credential. Negotiation
currently selects binary version 2. Unknown optional capability bits are ignored;
unknown required bits fail the connection. A supported capability must have its
handler installed by the application. Application operations require their own stable registry and capability
negotiation within that service identity.

## Frames and evolution

Each frame is a four-byte little-endian body length followed by one Protobuf
message. Pre-negotiation frames are limited to 1024 bytes. Negotiated limits are
between 256 and 65536 bytes, including encoded field overhead. Receivers reject
oversized lengths before allocating the body, and never read past one frame.
Buffers are reused. The wire codec uses safe Rust; OS adapters keep their
unsafe calls under `local`.

Field tags in `Hello` and `Control` are permanent. Add optional fields with new
tags; do not reuse removed tags or change their wire types. Unknown fields are retained when re-encoding, within a limit of 128 per message.
Nested unknown groups are limited to depth 32. Absent fields receive their Protobuf defaults; `offset` retains explicit
presence so that zero is distinct from missing. Unknown control
operations fail rather than being treated as successful. Mutating semantics
must never depend on an optional field an older peer can silently ignore.

A frame read/write failure poisons the connection. Cancelling an async frame
also poisons it, since a prefix may already have crossed the stream. Reconnecting
does not establish whether an operation ran. The application must report that
uncertainty; the codec never retries the request.

## Validation

`cargo test --all-features` covers negotiation, required capabilities, optional
field evolution, fragmented/coalesced messages, every truncation of a valid
frame, oversized headers, partial writes, sync/async interoperability and
cancellation. The process suite runs simultaneous legacy and binary requests
against one lifecycle gate and checks that drain preserves both replies.

CI regenerates the Rust schema and compares golden message bodies against the
independent `protoc` encoder. Tests cover UUID/profile/instance mismatch, resource
isolation and application operation 1 coexisting with lifecycle operation 1.

The [benchmarks](benchmarks.md) count the instructions for the handshake and
for sending and receiving each kind of `Control` message over an in-memory
transport. CI fails a pull request that grows any of them by more than 5%. They
do not measure socket latency or complete upgrade time.

Regenerate the schemas with `cargo run --locked --manifest-path tools/proto-gen/Cargo.toml`.
The generator requires `protoc`; normal builds do not. Check freshness with the
same command followed by `-- --check`, then `python3 scripts/check-protobuf.py`.

## Lifecycle subscriptions

`WATCH` is operation 8 with capability bit 32. A client offers the capability
without requiring it. `control::WatchClient::connect` returns `None` only when a
successful handshake does not negotiate WATCH; the application may then poll
its existing discovery source. Handshake errors, malformed events, and stream
failures are errors, not reasons to downgrade. Authenticate the OS peer before
connecting and compare its PID with the initial snapshot.

A WATCH request has no payload, token, or offset. Its generation is zero or the
serving generation. The connection carries no further client requests. Every
response preserves the request ID and carries a `LifecycleEvent`. The first is
`Snapshot`, numbered one. Later sequence numbers increase by one per delivered
frame. `Ready`, `Draining`, `SelectionChanged`, and `Retiring` identify single
changes; simultaneous/coalesced changes use `Snapshot`. Every event contains the
complete state, including the serving identity and the selected generation/build.
The selected generation is absent until the application reports a committed
selection. It may be lower than the serving generation during candidate staging;
being ready does not imply being selected.

This is a current-state subscription, not a transition audit log. Slow observers
may skip intermediate states. A watch channel retains the latest state without
an unbounded queue. A blocked write expires after five seconds. Idle watchers
have no heartbeat timer and disconnects release the serving task. Callers must
account for long-lived watchers in their control-connection admission limits.
The setup deadline bounds negotiation and reading the request; each event has
its own write deadline.

The application calls `ControlService::selection_committed` only **after** its
selection publication succeeds, including whatever durability its own record
requires. The method rejects older generations and changing the build at the
same generation. It neither publishes the record nor makes a failed commit
successful. `mark_retiring` closes admission and announces retirement; the
application still drains existing requests and owns process shutdown.

The async client retains its latest snapshot. Cancelling `changed()` or receiving
an invalid event makes that client terminal: reconnect and obtain a new snapshot.
Discovery remains the bootstrap and crash-recovery source. Existing consumers
must opt into WATCH; upgrading the library alone does not stop their polling.
