use std::{
    io,
    process::{Child, ExitStatus},
};

#[cfg(feature = "local")]
use crate::local::ProcessHandle;

/// A newly spawned candidate is disposable until its selection commits.
/// Dropping a selected candidate only reaps it; it never terminates the daemon.
pub struct Candidate {
    process: Option<Child>,
    selected: bool,
    #[cfg(feature = "local")]
    exit: Option<ProcessHandle>,
}
impl Candidate {
    /// Take ownership immediately after spawning a staged candidate.
    pub fn new(process: Child) -> Self {
        Self {
            process: Some(process),
            selected: false,
            #[cfg(feature = "local")]
            exit: None,
        }
    }
    /// OS process identity used by the live verifier.
    pub fn id(&self) -> u32 {
        self.process.as_ref().expect("candidate owns child").id()
    }
    /// Observe whether the candidate exited before readiness.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.process
            .as_mut()
            .expect("candidate owns child")
            .try_wait()
    }
    /// The candidate's exit as an event, for a verifier that waits on it
    /// between probes instead of sleeping. Opened on first use; see
    /// [`ProcessHandle::for_child`] for why that must be before anything else
    /// waits for the child.
    #[cfg(feature = "local")]
    pub fn exit_handle(&mut self) -> io::Result<&mut ProcessHandle> {
        let process = self.process.as_ref().expect("candidate owns child");
        if self.exit.is_none() {
            self.exit = Some(ProcessHandle::for_child(process)?);
        }
        Ok(self.exit.as_mut().expect("opened above"))
    }
    /// Block until the candidate exits or `deadline` passes: its exit status,
    /// or `None` while it still runs.
    #[cfg(feature = "local")]
    pub fn wait_until(&mut self, deadline: std::time::Instant) -> io::Result<Option<ExitStatus>> {
        if !self.exit_handle()?.wait_until(deadline)? {
            return Ok(None);
        }
        self.process
            .as_mut()
            .expect("candidate owns child")
            .wait()
            .map(Some)
    }
    /// Mark the selection boundary before doing repairable follow-up work.
    pub fn selected(&mut self) {
        self.selected = true;
    }
}
impl Drop for Candidate {
    fn drop(&mut self) {
        let Some(mut process) = self.process.take() else {
            return;
        };
        if process.try_wait().is_ok_and(|status| status.is_some()) {
            return;
        }
        if !self.selected && process.kill().is_ok() {
            let _ = process.wait();
        } else {
            let _ = std::thread::Builder::new()
                .name("daemon-reaper".into())
                .spawn(move || {
                    let _ = process.wait();
                });
        }
    }
}
