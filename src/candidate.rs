use std::{
    io,
    process::{Child, ExitStatus},
};

/// A newly spawned candidate is disposable until its selection commits.
/// Dropping a selected candidate only reaps it; it never terminates the daemon.
pub struct Candidate {
    process: Option<Child>,
    selected: bool,
}
impl Candidate {
    /// Take ownership immediately after spawning a staged candidate.
    pub fn new(process: Child) -> Self {
        Self {
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
