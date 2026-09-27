//! Keeps this crate's spawns out of socket creation where that takes two steps.
//!
//! Where the OS has no `SOCK_CLOEXEC`, as on macOS, the standard library
//! creates a socket and then marks it close-on-exec. A child spawned by
//! another thread between the two keeps the socket for its whole life, and a
//! listener it keeps goes on accepting after its owner closes it. Every spawn
//! in this crate holds [`spawning`], and listeners are created inside
//! [`without_spawns`].

use std::sync::{Mutex, MutexGuard, PoisonError};

static SPAWN: Mutex<()> = Mutex::new(());

/// Hold while spawning a child.
pub(crate) fn spawning() -> MutexGuard<'static, ()> {
    SPAWN.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Run `create` while no spawn by this crate is under way. Linux creates the
/// socket close-on-exec in one step, so nothing is held there.
#[cfg(all(unix, feature = "local"))]
pub(crate) fn without_spawns<T>(create: impl FnOnce() -> T) -> T {
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    let _spawn = spawning();
    create()
}
