# Changelog

## 0.2.2

- `launch` feature (off by default, needs `local`): client spawn primitives.
  `spawn` keeps the caller's stdio/env; `spawn_detached` nulls stdio for a
  protocol shim; `spawn_and_wait` waits on the caller's probe via `readiness`.
  Kernel bind remains the election; `ProcessLock` is layer two.
- Windows `local`: `install_session_end_handler` / `session_end_requested` for
  CLOSE, LOGOFF and SHUTDOWN, and `StdioInheritGuard` for daemon spawns.

## 0.2.1

- Lowercase Windows `#[link]` names (`kernel32`, `advapi32`) so `cargo-xwin` /
  `lld-link` on Linux can find the xwin import libraries.

## 0.2.0

Shared replacement coordinator, process lock, drain, and optional Protobuf
sessions. See the 0.2.0 release notes.

## 0.1.0

Initial release of shared primitives for local daemons:

- Persistent process ownership, request draining and verified upgrade coordination.
- Blocking relay transport and atomic discovery publication.
- Optional Protobuf sessions using Buffa 0.9.2, bounded frames and service identity checks.
- Separate lifecycle and application operation namespaces, with an application-message example.
- Independent admission budgets and local snapshots/events for consumer-owned telemetry.

Requires Rust 1.89. Consumers own OS peer authentication, secure runtime paths,
transport deadlines and the policy for long-running work. Existing clients can
keep their legacy endpoint while newer clients negotiate a separate binary one.
