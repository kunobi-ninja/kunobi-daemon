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
    ///
    /// With `local` this also opens the candidate's exit handle, while
    /// nothing has waited for the child, so the handle follows this process
    /// even if its PID is reused after a later reap.
    pub fn new(process: Child) -> Self {
        Self {
            #[cfg(feature = "local")]
            exit: ProcessHandle::for_child(&process).ok(),
            process: Some(process),
            selected: false,
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
    /// between probes instead of sleeping. The handle is opened by
    /// [`Self::new`]; this fails if that could not be done, and
    /// [`Self::wait_until`] then asks the child directly.
    #[cfg(feature = "local")]
    pub fn exit_handle(&mut self) -> io::Result<&mut ProcessHandle> {
        self.exit
            .as_mut()
            .ok_or_else(|| io::Error::other("the candidate's exit handle could not be opened"))
    }
    /// Block until the candidate exits or `deadline` passes: its exit status,
    /// or `None` while it still runs.
    #[cfg(feature = "local")]
    pub fn wait_until(&mut self, deadline: std::time::Instant) -> io::Result<Option<ExitStatus>> {
        let process = self.process.as_mut().expect("candidate owns child");
        if !crate::local::wait_child(|| process.try_wait(), self.exit.as_mut(), deadline)? {
            return Ok(None);
        }
        process.wait().map(Some)
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
        let pid = child.id();
        let mut candidate = super::Candidate::new(child);
        assert!(candidate.exit.is_some());
        assert_eq!(candidate.exit_handle().unwrap().pid(), pid);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        assert!(candidate.wait_until(deadline).unwrap().unwrap().success());
    }
}
