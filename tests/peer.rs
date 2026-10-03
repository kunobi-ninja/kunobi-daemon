//! Evidence from an accepted local connection names the process that
//! connected, and policies grant or refuse it.

#![cfg(feature = "local")]

use kunobi_daemon::local::peer::evidence;
use kunobi_daemon::peer::{Evidence, ProcessId};
use std::{sync::mpsc, time::Duration};

const BUDGET: Duration = Duration::from_secs(20);

fn own_pid() -> ProcessId {
    ProcessId::new(std::process::id()).unwrap()
}

#[cfg(unix)]
mod unix {
    use super::*;
    use kunobi_daemon::peer::{ExpectedProcess, First, Rejected, SameUser, authenticate};
    use std::io::{Read, Write};
    use std::os::fd::AsFd;
    use std::os::unix::net::{UnixListener, UnixStream};
    use std::process::{Command, Stdio};

    const SOCKET_ENV: &str = "KUNOBI_DAEMON_PEER_TEST_SOCKET";

    #[test]
    #[ignore = "child-process entry point, invoked by the accepted-peer test"]
    fn peer_fixture_process() {
        let path = std::env::var_os(SOCKET_ENV).unwrap();
        let mut stream = UnixStream::connect(path).unwrap();
        stream.write_all(&std::process::id().to_le_bytes()).unwrap();
        // Hold the connection open until the parent closes it.
        let _ = stream.read(&mut [0u8; 1]);
    }

    #[test]
    fn an_accepted_connection_reports_the_child_that_connected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("peer.sock");
        let listener = UnixListener::bind(&path).unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "unix::peer_fixture_process", "--ignored"])
            .env(SOCKET_ENV, &path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        // Accept on a thread so a child that never connects fails the test
        // instead of hanging it.
        let (sent, accepted) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = sent.send(listener.accept().map(|(stream, _)| stream));
        });
        let mut stream = accepted
            .recv_timeout(BUDGET)
            .expect("the child did not connect")
            .unwrap();
        let mut announced = [0u8; 4];
        stream.read_exact(&mut announced).unwrap();

        let peer = evidence(stream.as_fd()).unwrap();
        let child_pid = ProcessId::new(child.id());
        assert_eq!(peer, Evidence::new(child_pid, true));
        assert_eq!(peer.pid, ProcessId::new(u32::from_le_bytes(announced)));
        assert_ne!(peer.pid, Some(own_pid()));

        // The child is the expected process: full grant. Expecting this test
        // process instead leaves only the same-user tier.
        let tiers = |expected: Option<ProcessId>| {
            First::new()
                .then(ExpectedProcess::new(move || expected), "full")
                .then(SameUser, "redacted")
        };
        let accepted = authenticate(&stream, peer, &tiers(child_pid)).unwrap();
        assert_eq!(*accepted.grant(), "full");
        let accepted = authenticate(&stream, peer, &tiers(Some(own_pid()))).unwrap();
        assert_eq!(*accepted.grant(), "redacted");
        assert_eq!(
            authenticate(&stream, peer, &ExpectedProcess::new(|| Some(own_pid()))).unwrap_err(),
            Rejected::Different {
                expected: own_pid(),
                observed: child_pid.unwrap()
            }
        );

        drop(stream);
        child.wait().unwrap();
    }
}

#[cfg(windows)]
#[test]
fn an_accepted_pipe_reports_its_client() {
    use interprocess::local_socket::traits::{Listener as _, Stream as _};
    use interprocess::local_socket::{GenericNamespaced, Stream, ToNsName};
    use kunobi_daemon::local::{Bound, socket::acquire};
    use std::path::Path;

    let unique = tempfile::tempdir().unwrap();
    let name = format!(
        "daemon-peer-{}-{}",
        std::process::id(),
        unique.path().file_name().unwrap().to_string_lossy()
    );
    let Bound::Won(listener) = acquire(Path::new(&name)).unwrap() else {
        panic!("first owner")
    };
    let client_name = name.clone();
    let client = std::thread::spawn(move || {
        Stream::connect(
            client_name
                .as_str()
                .to_ns_name::<GenericNamespaced>()
                .unwrap(),
        )
        .unwrap()
    });
    let (sent, accepted) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sent.send(listener.accept());
    });
    let server = accepted
        .recv_timeout(BUDGET)
        .expect("the client did not connect")
        .unwrap();

    assert_eq!(
        evidence(&server).unwrap(),
        Evidence::new(Some(own_pid()), true)
    );
    drop(client.join().unwrap());
}
