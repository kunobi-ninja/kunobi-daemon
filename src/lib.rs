//! Shared lifecycle primitives for local background services.
//!
//! [`ProcessLock`] keeps a persistent lock inode across replacements.
//! [`Lifecycle`] closes request admission and waits for admitted work.
//! [`ensure_current`] serializes upgrades and verifies the replacement through
//! the caller's live health protocol. The caller owns service-manager commands,
//! transport, version policy, and the treatment of unfinished work.

#![doc = include_str!("../README.md")]

mod lifecycle;
mod ownership;
mod upgrade;

pub use lifecycle::{DrainOutcome, Lifecycle, RequestGuard};
pub use ownership::ProcessLock;
pub use upgrade::{UpgradeError, ensure_current};
