//! Work a client does before its first byte reaches the daemon: deriving the
//! service's paths and checking socket paths.

use gungraun::prelude::*;
use kunobi_daemon::ServiceIdentity;
use kunobi_daemon::socket_path::{SocketDir, SocketName, check_socket_path};
use std::hint::black_box;
use std::path::PathBuf;

const DAEMON: SocketName = SocketName::new("daemon.sock");
const DAEMON_V2: SocketName = SocketName::new("daemon-v2.sock");
const RELAY: SocketName = SocketName::new("relay-1f2e3d4c.sock");

#[library_benchmark]
fn service_paths() -> (PathBuf, PathBuf, PathBuf) {
    let identity = ServiceIdentity::new(
        black_box([0x5a; 16]),
        black_box("org.example.cache"),
        black_box("stable"),
        black_box("default"),
    )
    .expect("valid identity");
    let paths = identity
        .paths(black_box("/run/user/1000"))
        .expect("short root");
    (
        paths.control_socket(),
        paths.ownership_lock(),
        paths.discovery(),
    )
}

#[library_benchmark]
fn socket_dir() -> [PathBuf; 3] {
    let dir = SocketDir::new(
        black_box("/run/user/1000/org.example/service"),
        &[DAEMON, DAEMON_V2, RELAY],
    )
    .expect("short directory");
    let paths = [
        dir.path(DAEMON).expect("fits"),
        dir.path(DAEMON_V2).expect("fits"),
        dir.path(RELAY).expect("fits"),
    ];
    for path in &paths {
        check_socket_path(black_box(path)).expect("fits");
    }
    paths
}

library_benchmark_group!(name = paths, benchmarks = [service_paths, socket_dir]);

main!(library_benchmark_groups = [paths]);
