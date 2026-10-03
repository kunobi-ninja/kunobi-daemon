//! A blocking control client: health or drain, with the OS peer and reply checked.
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use kunobi_daemon::{
        ServiceIdentity, client,
        wire::{self, capability, operation},
    };
    use std::{
        path::PathBuf,
        time::{Duration, Instant},
    };
    let root = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("usage: blocking_health DIRECTORY [drain]")?,
    );
    let identity = ServiceIdentity::new([0x71; 16], "example-daemon", "demo", "default")?;
    let offer = wire::Hello::new(
        &identity,
        capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
        capability::HEALTH | capability::HEALTH_DETAILS,
    );
    let operation = if std::env::args().nth(2).as_deref() == Some("drain") {
        operation::DRAIN
    } else {
        operation::HEALTH
    };
    // Checks the peer is this OS user and that the reply names the process on
    // the connection. A daemon that advertises its PID would pass it instead
    // of `None` to require that exact process.
    let deadline = Instant::now() + Duration::from_secs(2);
    let health = client::request(
        &root.join("control.sock"),
        &offer,
        operation,
        None,
        deadline,
    )?;
    println!(
        "pid={} ready={} draining={} active={}",
        health.process_id, health.ready, health.draining, health.active
    );
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("This example uses Unix sockets; local::windows provides named-pipe clients.");
}
