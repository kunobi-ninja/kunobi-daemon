# Changelog

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
