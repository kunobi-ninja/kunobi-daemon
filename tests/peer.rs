//! Evidence from a local connection names the process at the other end, in
//! both directions, and policies grant or refuse it. The listener and its peer
//! are separate operating-system processes.

#![cfg(feature = "local")]

use kunobi_daemon::local::peer::evidence;
use kunobi_daemon::peer::{
    Evidence, ExpectedProcess, First, ProcessId, Rejected, SameUser, authenticate,
};
use std::{
    io::{Read, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::{Duration, Instant},
};

const BUDGET: Duration = Duration::from_secs(20);
const ENDPOINT_ENV: &str = "KUNOBI_DAEMON_PEER_TEST_ENDPOINT";
const SERVER_PID_ENV: &str = "KUNOBI_DAEMON_PEER_TEST_SERVER";

fn own_pid() -> ProcessId {
    ProcessId::new(std::process::id()).unwrap()
}

/// Kills and reaps the child if the test fails before it exits.
struct Reaped(Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

impl Reaped {
    fn spawn(endpoint: &str) -> Self {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "peer_fixture_process", "--ignored"])
            .env(ENDPOINT_ENV, endpoint)
            .env(SERVER_PID_ENV, std::process::id().to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        Self(child)
    }

    fn pid(&self) -> ProcessId {
        ProcessId::new(self.0.id()).unwrap()
    }

    /// Wait for the child to finish its own checks, within the budget.
    fn assert_succeeds(&mut self) {
        let deadline = Instant::now() + BUDGET;
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "the child's own checks failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "the child did not exit");
            #[expect(
                clippy::disallowed_methods,
                reason = "Bounded wait for a test child; std::process has no exit timeout."
            )]
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

/// The child: connect, check that the server is the parent, announce our PID,
/// then wait for the parent to close the connection.
#[test]
#[ignore = "child-process entry point, invoked by the evidence tests"]
fn peer_fixture_process() {
    let endpoint = std::env::var(ENDPOINT_ENV).unwrap();
    let server: u32 = std::env::var(SERVER_PID_ENV).unwrap().parse().unwrap();
    let mut stream = connect(&endpoint);
    let server_evidence = client_evidence(&stream);
    assert_eq!(server_evidence, Evidence::new(ProcessId::new(server), true));
    let accepted = authenticate(
        &stream,
        server_evidence,
        &ExpectedProcess::new(move || ProcessId::new(server)),
    );
    assert!(accepted.is_ok(), "the child refused its server");
    stream.write_all(&std::process::id().to_le_bytes()).unwrap();
    let _ = stream.read(&mut [0u8; 1]);
}

/// The parent's checks on the connection it accepted from `child`.
fn check_accepted(peer: Evidence, child: ProcessId, announced: [u8; 4]) {
    assert_eq!(peer, Evidence::new(Some(child), true));
    assert_eq!(peer.pid, ProcessId::new(u32::from_le_bytes(announced)));
    assert_ne!(peer.pid, Some(own_pid()));

    // The child is the expected process: full grant. Expecting this test
    // process instead leaves only the same-user tier.
    let tiers = |expected: Option<ProcessId>| {
        First::new()
            .then(ExpectedProcess::new(move || expected), "full")
            .then(SameUser, "redacted")
    };
    assert_eq!(
        authenticate((), peer, &tiers(Some(child))).map(|a| *a.grant()),
        Ok("full")
    );
    assert_eq!(
        authenticate((), peer, &tiers(Some(own_pid()))).map(|a| *a.grant()),
        Ok("redacted")
    );
    assert_eq!(
        authenticate((), peer, &ExpectedProcess::new(|| Some(own_pid()))).unwrap_err(),
        Rejected::Different {
            expected: own_pid(),
            observed: child
        }
    );
}

/// Accept on a thread so a child that never connects fails the test instead of
/// hanging it.
fn accept_within_budget<S: Send + 'static>(
    accept: impl FnOnce() -> std::io::Result<S> + Send + 'static,
) -> S {
    let (sent, accepted) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sent.send(accept());
    });
    accepted
        .recv_timeout(BUDGET)
        .expect("the child did not connect")
        .unwrap()
}

#[cfg(unix)]
mod platform {
    use super::*;
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixListener, UnixStream};

    pub(super) fn connect(endpoint: &str) -> UnixStream {
        let stream = UnixStream::connect(endpoint).unwrap();
        stream.set_read_timeout(Some(BUDGET)).unwrap();
        stream
    }

    pub(super) fn client_evidence(stream: &UnixStream) -> Evidence {
        evidence(stream.as_fd()).unwrap()
    }

    #[test]
    fn both_ends_of_a_unix_socket_see_the_other_process() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let mut child = Reaped::spawn(path.to_str().unwrap());

        let mut stream = accept_within_budget(move || listener.accept().map(|(s, _)| s));
        stream.set_read_timeout(Some(BUDGET)).unwrap();
        let mut announced = [0u8; 4];
        stream.read_exact(&mut announced).unwrap();
        check_accepted(evidence(stream.as_fd()).unwrap(), child.pid(), announced);

        drop(stream);
        child.assert_succeeds();
    }
}

#[cfg(windows)]
mod platform {
    use super::*;
    use interprocess::local_socket::traits::{Listener as _, Stream as _};
    use interprocess::local_socket::{GenericNamespaced, Stream, ToNsName};
    use kunobi_daemon::local::{Bound, socket::acquire};
    use std::path::Path;

    pub(super) fn connect(endpoint: &str) -> Stream {
        Stream::connect(endpoint.to_ns_name::<GenericNamespaced>().unwrap()).unwrap()
    }

    pub(super) fn client_evidence(stream: &Stream) -> Evidence {
        evidence(stream).unwrap()
    }

    #[test]
    fn both_ends_of_a_named_pipe_see_the_other_process() {
        let unique = tempfile::tempdir().unwrap();
        let name = format!(
            "daemon-peer-{}-{}",
            std::process::id(),
            unique.path().file_name().unwrap().to_string_lossy()
        );
        let Bound::Won(listener) = acquire(Path::new(&name)).unwrap() else {
            panic!("first owner")
        };
        let mut child = Reaped::spawn(&name);

        let mut stream = accept_within_budget(move || listener.accept());
        let mut announced = [0u8; 4];
        stream.read_exact(&mut announced).unwrap();
        check_accepted(evidence(&stream).unwrap(), child.pid(), announced);

        drop(stream);
        child.assert_succeeds();
    }
}

use platform::{client_evidence, connect};
