# Binary lifecycle control

Enable `wire` for blocking clients or `wire-async` for Tokio servers. The feature
is optional; existing users keep the std-only transport unless they enable it.
Bilrost 0.1016.1 supplies field-tagged encoding from Rust structs. This choice is
based on schema evolution and Rust integration, not a claim that it beats every
other codec. The format is experimental until the first consumer release.

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

1. Client writes the eight bytes `KNDAEM02`, then its framed `Hello`.
2. Server validates the application identifier, version overlap, required
   capabilities and frame limits. It replies with its framed `Hello`.
3. Client independently validates the agreement and writes byte `1`.
4. Server reads that acceptance and writes byte `1`.
5. Both sides may now exchange framed `Control` messages.

The application string is a protocol namespace, not a credential. Negotiation
currently selects binary version 2. Unknown optional capability bits are ignored;
unknown required bits fail the connection. A supported capability must have its
handler installed by the application. Application operation IDs at or above
1024 require their own stable registry and capability negotiation within that
application protocol; the generic application bit alone cannot prove support
for a particular newly added operation.

## Frames and evolution

Each frame is a four-byte little-endian body length followed by one Bilrost
message. Pre-negotiation frames are limited to 1024 bytes. Negotiated limits are
between 256 and 65536 bytes, including encoded field overhead. Receivers reject
oversized lengths before allocating the body, and never read past one frame.
Buffers are reused. The implementation uses no unsafe Rust in this crate.

Field tags in `Hello` and `Control` are permanent. Add optional fields with new
tags; do not reuse removed tags or change their wire types. The relaxed decoder
ignores unknown fields and gives absent fields their defaults. Unknown control
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

`cargo bench --features wire --bench control_codec` prints 31 batches of 10000
operations after warmup, summarized as p50/p95 of batch means. It compares a
representative receipt with JSON encoded by Serde, using reusable output buffers.
It measures codec CPU time and body bytes, not the existing relay's complete
parser, socket latency, allocations, or end-to-end upgrade time. Cap'n Proto has
not been benchmarked by this harness. No timing threshold runs on shared CI.
