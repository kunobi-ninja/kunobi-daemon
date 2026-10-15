use std::{
    io,
    ops::{Deref, DerefMut},
    process::{Child, ExitStatus},
};

#[cfg(feature = "local")]
use crate::local::ProcessHandle;

/// A newly spawned candidate is disposable until its selection commits.
/// Dropping a selected candidate only reaps it; it never terminates the daemon.
pub struct Candidate {
    process: Process,
    selected: bool,
}
enum Process {
    Child {
        child: Reaped,
        #[cfg(feature = "local")]
        exit: Option<ProcessHandle>,
    },
    #[cfg(feature = "launch")]
    Daemon(crate::launch::DaemonChild),
}
impl Candidate {
    /// Take ownership immediately after spawning a staged candidate.
    ///
    /// With `local` this also opens the candidate's exit handle, while
    /// nothing has waited for the child, so the handle follows this process
    /// even if its PID is reused after a later reap.
    pub fn new(process: Child) -> Self {
        Self {
            process: Process::Child {
                #[cfg(feature = "local")]
                exit: ProcessHandle::for_child(&process).ok(),
                child: Reaped::new(process),
            },
            selected: false,
        }
    }
    /// OS process identity used by the live verifier.
    pub fn id(&self) -> crate::ProcessId {
        match &self.process {
            Process::Child { child, .. } => crate::ProcessId::of(child),
            #[cfg(feature = "launch")]
            Process::Daemon(daemon) => daemon.id(),
        }
    }
    /// Observe whether the candidate exited before readiness.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match &mut self.process {
            Process::Child { child, .. } => child.try_wait(),
            #[cfg(feature = "launch")]
            Process::Daemon(daemon) => daemon.try_wait(),
        }
    }
    /// The candidate's exit as an event, for a verifier that waits on it
    /// between probes instead of sleeping. The handle is opened by
    /// [`Self::new`]; this fails if that could not be done, and
    /// [`Self::wait_until`] then asks the child directly.
    #[cfg(feature = "local")]
    pub fn exit_handle(&mut self) -> io::Result<&mut ProcessHandle> {
        match &mut self.process {
            Process::Child { exit, .. } => exit
                .as_mut()
                .ok_or_else(|| io::Error::other("the candidate's exit handle could not be opened")),
            #[cfg(feature = "launch")]
            Process::Daemon(daemon) => Ok(daemon.exit_handle()),
        }
    }
    /// Block until the candidate exits or `deadline` passes: its exit status,
    /// or `None` while it still runs.
    #[cfg(feature = "local")]
    pub fn wait_until(&mut self, deadline: std::time::Instant) -> io::Result<Option<ExitStatus>> {
        match &mut self.process {
            Process::Child { child, exit } => {
                if !crate::local::wait_child(|| child.try_wait(), exit.as_mut(), deadline)? {
                    return Ok(None);
                }
                child.wait().map(Some)
            }
            #[cfg(feature = "launch")]
            Process::Daemon(daemon) => daemon.wait_until(deadline),
        }
    }
    /// Mark the selection boundary before doing repairable follow-up work.
    pub fn selected(&mut self) {
        self.selected = true;
    }
    /// Kill the candidate and wait for it to exit.
    fn stop(&mut self) -> io::Result<ExitStatus> {
        match &mut self.process {
            Process::Child { child, .. } => {
                child.kill()?;
                child.wait()
            }
            #[cfg(feature = "launch")]
            Process::Daemon(daemon) => {
                daemon.kill()?;
                daemon.wait()
            }
        }
    }
}
/// A daemon started by [`DaemonCommand`](crate::launch::DaemonCommand) as a
/// candidate: dropped before [`Candidate::selected`], it is stopped. Take its
/// readiness channel first; the candidate keeps it but does not give it out.
#[cfg(feature = "launch")]
impl From<crate::launch::DaemonChild> for Candidate {
    fn from(daemon: crate::launch::DaemonChild) -> Self {
        Self {
            process: Process::Daemon(daemon),
            selected: false,
        }
    }
}
impl Drop for Candidate {
    /// Stop an unselected candidate. Dropping its process then reaps it.
    fn drop(&mut self) {
        if !self.selected {
            let _ = self.stop();
        }
    }
}

/// A child that is reaped, never stopped, when dropped: at once if it has
/// exited, otherwise by a thread that waits for it. On Unix a child nothing
/// waits for stays a zombie after it exits.
#[derive(Debug)]
pub(crate) struct Reaped(Option<Child>);
impl Reaped {
    pub(crate) fn new(child: Child) -> Self {
        Self(Some(child))
    }
}
impl Deref for Reaped {
    type Target = Child;
    fn deref(&self) -> &Child {
        self.0
            .as_ref()
            .expect("the child is taken only when dropped")
    }
}
impl DerefMut for Reaped {
    fn deref_mut(&mut self) -> &mut Child {
        self.0
            .as_mut()
            .expect("the child is taken only when dropped")
    }
}
impl Drop for Reaped {
    fn drop(&mut self) {
        let Some(mut child) = self.0.take() else {
            return;
        };
        if child.try_wait().is_ok_and(|status| status.is_some()) {
            return;
        }
        let _ = std::thread::Builder::new()
            .name("daemon-reaper".into())
            .spawn(move || {
                let _ = child.wait();
            });
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "local")]
    #[test]
    fn a_candidate_watches_its_process_from_the_moment_it_is_taken() {
        // Opened later, after a `try_wait` had reaped the child, the handle
        // could follow whatever process reused the PID.
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--list")
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let pid = crate::ProcessId::of(&child);
        let mut candidate = super::Candidate::new(child);
        assert_eq!(candidate.exit_handle().unwrap().pid(), pid);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        assert!(candidate.wait_until(deadline).unwrap().unwrap().success());
    }
}
