//! Adopt the optional launch readiness channel at the OS boundary.

use crate::readiness::channel::Notifier;
use std::io;

impl Notifier {
    /// Take the channel [`crate::readiness::channel::ENV`] names, or `None` when the launcher did not ask
    /// for one. Enable the `local` feature.
    ///
    /// The variable is removed from the environment, so a later call, and any
    /// program the daemon starts, sees no channel. The channel's end is not
    /// inherited by those programs: close-on-exec on Unix, and opened not
    /// inheritable on Windows.
    ///
    /// A value that does not name a channel fails without touching what it
    /// names: on Unix anything but the write end of a pipe above stderr, on
    /// Windows anything but a readiness pipe name.
    ///
    /// # Safety
    ///
    /// On every platform, call it early, before other threads run that may
    /// read or change the environment: removing the variable while they do is
    /// undefined behaviour, as for [`std::env::remove_var`].
    ///
    /// On Unix the process must have been started by
    /// `launch::DaemonCommand::readiness_channel`, and nothing else in it may
    /// own or close the descriptor the variable names. The returned notifier
    /// takes that descriptor over and closes it when dropped; a stale or
    /// foreign value would make it a second owner of some other descriptor.
    /// This is the contract of systemd's `LISTEN_FDS`.
    ///
    /// On Windows the daemon opens the pipe by name and adopts no handle, so
    /// only the first rule applies.
    ///
    /// ```no_run
    /// use kunobi_daemon::readiness::channel::Notifier;
    ///
    /// // SAFETY: first thing in main, before any thread starts, in a daemon
    /// // that only a DaemonCommand with a readiness channel starts.
    /// let notifier = unsafe { Notifier::from_env() };
    /// let mut notifier = notifier.unwrap_or_else(|error| {
    ///     eprintln!("readiness channel: {error}");
    ///     None
    /// });
    /// if let Some(notifier) = &mut notifier {
    ///     // A failed send means the launcher stopped listening; carry on.
    ///     let _ = notifier.progress("opening the cache");
    /// }
    /// // Bind the endpoint and start serving, then:
    /// if let Some(notifier) = notifier {
    ///     let _ = notifier.ready();
    /// }
    /// ```
    #[cfg(feature = "local")]
    pub unsafe fn from_env() -> io::Result<Option<Self>> {
        let Some(value) = std::env::var_os(crate::readiness::channel::ENV) else {
            return Ok(None);
        };
        // SAFETY: the caller calls this before other threads may touch the
        // environment. Removing the variable first means a value that turns
        // out to be invalid still reaches no program this process starts.
        unsafe {
            std::env::remove_var(crate::readiness::channel::ENV);
        }
        let value = value.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "the readiness channel variable is not text",
            )
        })?;
        #[cfg(unix)]
        let pipe = {
            // SAFETY: the caller guarantees that a descriptor named by the
            // variable is this process's channel end, owned by nothing else.
            unsafe { crate::local::unix::take_ready_descriptor(value)? }
        };
        #[cfg(windows)]
        let pipe = crate::local::windows::open_ready_pipe(value)?;
        Ok(Some(Self::from_pipe(pipe)))
    }
}
