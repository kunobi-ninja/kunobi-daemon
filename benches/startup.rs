//! Work a client does before its first byte reaches the daemon: deriving the
//! service's paths, checking socket paths, and on Windows, building the
//! command line and environment block of the daemon it launches.

use gungraun::prelude::*;
use kunobi_daemon::ServiceIdentity;
use kunobi_daemon::socket_path::{SocketDir, SocketName, check_socket_path};
use std::hint::black_box;
use std::path::PathBuf;

// The Windows spawn builds these itself for CreateProcessW. The functions are
// crate-private and pure, so the benchmark compiles the same source file on
// Linux instead of widening the crate's public API.
#[path = "../src/local/command_line.rs"]
mod command_line;
use command_line::{EnvChange, command_line, environment_block};

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

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().collect()
}

struct Launch {
    program: Vec<u16>,
    args: Vec<Vec<u16>>,
}

fn launch(args: &[&str]) -> Launch {
    Launch {
        program: wide(r"C:\Users\someone\AppData\Local\Programs\example\daemon.exe"),
        args: args.iter().map(|arg| wide(arg)).collect(),
    }
}

#[library_benchmark]
#[bench::plain(
    args = (&["serve", "--profile", "stable", "--instance", "default", "--log-level", "info"]),
    setup = launch
)]
#[bench::quoted(
    args = (&[
        "serve",
        "--root",
        r"C:\Users\some one\AppData\Local\example\",
        "--label",
        r#"say "hi" \"#,
        "",
        "--flag",
    ]),
    setup = launch
)]
fn windows_command_line(input: Launch) -> Vec<u16> {
    command_line(black_box(&input.program), black_box(&input.args)).expect("valid command")
}

struct Environment {
    inherited: Vec<(Vec<u16>, Vec<u16>)>,
    changes: Vec<EnvChange>,
}

fn environment(variables: usize) -> Environment {
    // A typical Windows session: a long PATH among a few dozen short variables.
    let mut inherited = vec![(
        wide("Path"),
        wide(&r"C:\Windows\system32;C:\Windows;C:\Program Files\Git\cmd;".repeat(8)),
    )];
    inherited.extend((1..variables).map(|i| {
        (
            wide(&format!("VARIABLE_{i:03}")),
            wide(&format!(r"C:\Users\someone\value-{i}")),
        )
    }));
    Environment {
        inherited,
        changes: vec![
            EnvChange::Set(wide("EXAMPLE_DAEMON_PROFILE"), wide("stable")),
            EnvChange::Set(wide("path"), wide(r"C:\Windows\system32")),
            EnvChange::Remove(wide("VARIABLE_007")),
        ],
    }
}

#[library_benchmark]
#[bench::vars_48(args = (48), setup = environment)]
fn windows_environment_block(input: Environment) -> Vec<u16> {
    environment_block(input.inherited, black_box(&input.changes)).expect("valid environment")
}

library_benchmark_group!(name = paths, benchmarks = [service_paths, socket_dir]);

library_benchmark_group!(
    name = windows_launch,
    benchmarks = [windows_command_line, windows_environment_block]
);

main!(library_benchmark_groups = [paths, windows_launch]);
