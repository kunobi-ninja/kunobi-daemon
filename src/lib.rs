//! Shared lifecycle primitives for local background services.
//!
//! [`ProcessLock`] keeps a persistent lock inode across replacements.
//! With the default `async` feature, `Lifecycle` closes request admission and
//! waits for admitted work. `ensure_current` serializes upgrades through
//! the caller's live health protocol. The caller owns service-manager commands,
//! transport, version policy, and the treatment of unfinished work.

#![cfg_attr(feature = "async", doc = include_str!("../README.md"))]

#[cfg(feature = "local")]
#[allow(unsafe_code)]
pub mod local;

pub mod admission;
mod candidate;
#[cfg(feature = "wire-async")]
pub mod control;
mod identity;
pub use candidate::Candidate;
#[cfg(feature = "async")]
pub mod generation;
#[cfg(feature = "async")]
mod lifecycle;
pub mod observation;
mod ownership;
mod publication;
pub mod readiness;
pub mod replacement;
pub mod retry;
pub mod transport;
#[cfg(feature = "async")]
mod upgrade;

pub use identity::{ServiceIdentity, ServicePaths};
#[cfg(feature = "async")]
pub use lifecycle::{DrainOutcome, Lifecycle, LifecycleSnapshot, RequestGuard};
pub use ownership::ProcessLock;
pub use publication::{RecordSlot, publish_record};
#[cfg(feature = "async")]
pub use upgrade::{UpgradeError, ensure_current};

#[cfg(feature = "wire")]
pub mod wire;
