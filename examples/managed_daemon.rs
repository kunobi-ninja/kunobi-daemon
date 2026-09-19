//! An explicit manager command, using the example server's identity and run lock.
//! Invoke only with a manager configured to own the supplied directory.
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use kunobi_daemon::{
        Candidate, ProcessLock, ServiceIdentity,
        local::{Duplex, unix::UnixDuplex},
        readiness,
        replacement::{self, Budgets, Driver, Mode, Progress, Step},
        transport::SplitIo,
        wire::{self, capability, operation},
    };
    use std::{
        io,
        path::PathBuf,
        process::Command,
        time::{Duration, Instant},
    };

    struct Managed {
        root: PathBuf,
        command: Command,
        stopping: bool,
    }
    impl Managed {
        fn probe(&self, operation: u32, deadline: Instant) -> io::Result<wire::Health> {
            let stream = UnixDuplex::connect_once_until(&self.root.join("control.sock"), deadline)
                .map_err(io::Error::other)?;
            stream.verify_peer_user()?;
            let pid = stream.peer_pid()?;
            stream.set_read_deadline(Some(deadline.saturating_duration_since(Instant::now())))?;
            let (read, write) = stream.split()?;
            let identity = ServiceIdentity::new([0x71; 16], "example-daemon", "demo", "default")
                .map_err(io::Error::other)?;
            let offer = wire::Hello::new(
                &identity,
                capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
                capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
            );
            let mut session = wire::Session::connect(SplitIo { read, write }, &offer)?;
            let request = wire::Control {
                request_id: 1,
                operation,
                ..Default::default()
            };
            session.send(&request)?;
            let proof = wire::Health::from_response(&session.receive()?, &request)?;
            if proof.process_id != pid {
                return Err(io::Error::other("health differs from the kernel peer"));
            }
            Ok(proof)
        }
    }
    impl Driver for Managed {
        type Error = io::Error;
        fn perform(&mut self, step: Step, deadline: Option<Instant>) -> io::Result<Progress> {
            let deadline = deadline.expect("example uses bounded setup and drain");
            match step {
                Step::Recheck | Step::Prepare => {}
                Step::Drain => {
                    if ProcessLock::is_held(self.root.join("run.lock"))? {
                        if !self.stopping {
                            self.probe(operation::DRAIN, deadline)?;
                            self.stopping = true;
                        }
                        return Ok(Progress::Pending);
                    }
                }
                Step::Start => {
                    // The manager may restart on exit before its client is invoked.
                    if ProcessLock::is_held(self.root.join("run.lock"))? {
                        return Ok(Progress::Done);
                    }
                    let mut client = Candidate::new(self.command.spawn()?);
                    let status = readiness::wait_until(deadline, |_| client.try_wait())?
                        .ok_or(io::ErrorKind::TimedOut)?;
                    if !status.success() {
                        return Err(io::Error::other(format!(
                            "manager command failed: {status}"
                        )));
                    }
                }
                Step::Verify | Step::Validate => {
                    let proof = match self.probe(operation::HEALTH, deadline) {
                        Ok(proof) => proof,
                        Err(error)
                            if step == Step::Verify
                                && error.get_ref().is_some_and(|source| {
                                    source.downcast_ref::<kunobi_daemon::local::ConnectError>()
                                        == Some(&kunobi_daemon::local::ConnectError::ConnectTimeout)
                                }) =>
                        {
                            return Ok(Progress::Pending);
                        }
                        Err(error) => return Err(error),
                    };
                    if !proof.ready || proof.draining {
                        if step == Step::Verify {
                            return Ok(Progress::Pending);
                        }
                        return Err(io::Error::other("readiness changed before commit"));
                    }
                }
                // A fixed endpoint needs no selected-generation record.
                Step::Commit => {}
                Step::Retire => unreachable!("exclusive replacement"),
            }
            Ok(Progress::Done)
        }
    }
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(
        args.next()
            .ok_or("usage: managed_daemon DIRECTORY MANAGER [ARGS...]")?,
    );
    let mut command = Command::new(args.next().ok_or("missing manager executable")?);
    command.args(args);
    let lock = ProcessLock::try_acquire(root.join("upgrade.lock"))?
        .ok_or("replacement already running")?;
    let mut driver = Managed {
        root,
        command,
        stopping: false,
    };
    let outcome = replacement::run(
        &lock,
        Mode::Exclusive,
        Budgets {
            setup: Duration::from_secs(8),
            drain: Some(Duration::from_secs(30)),
        },
        &mut driver,
    )?;
    println!("{outcome:?}");
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("This example uses the Unix lifecycle_server endpoint.");
}
