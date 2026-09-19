# Binary lifecycle control

Enable `wire` for blocking clients or `wire-async` for Tokio servers. The feature
is optional; existing users keep the std-only transport unless they enable it.
Buffa 0.9.2 encodes and decodes the binary messages, which follow [the Protobuf schema](../proto/lifecycle.proto).
Generated Rust types are checked in; consumers do not need a schema compiler.
The format is experimental until the first consumer release.

## Service identity

Generate a UUID once per service and keep it in the application's package.
`ServiceIdentity::new` takes its 16 bytes, an application name, a profile and an
instance. Use the same identity for `identity.paths(private_user_root)` and
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
Buffers are reused. The implementation uses no unsafe Rust in this crate.

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

`cargo bench --features wire --bench control_codec` measures Protobuf control
messages with reusable output buffers. It reports p50/p95 of batch means after
warmup, not per-request latency percentiles. It does not measure socket latency,
allocations or complete upgrade time, or claim superiority over other codecs.
No timing threshold runs on shared CI.

Regenerate the schemas with `cargo run --locked --manifest-path tools/proto-gen/Cargo.toml`.
The generator requires `protoc`; normal builds do not. Check freshness with the
same command followed by `-- --check`, then `python3 scripts/check-protobuf.py`.
