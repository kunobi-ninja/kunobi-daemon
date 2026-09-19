//! A blocking client that validates the OS peer and typed lifecycle response.
#[cfg(unix)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use kunobi_daemon::{
        ServiceIdentity,
        local::{Duplex, unix::UnixDuplex},
        transport::SplitIo,
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
    let deadline = Instant::now() + Duration::from_secs(2);
    let stream = UnixDuplex::connect_once_until(&root.join("control.sock"), deadline)?;
    stream.verify_peer_user()?;
    let pid = stream.peer_pid()?;
    stream.set_read_deadline(Some(deadline.saturating_duration_since(Instant::now())))?;
    let (read, write) = stream.split()?;
    let identity = ServiceIdentity::new([0x71; 16], "example-daemon", "demo", "default")?;
    let offer = wire::Hello::new(
        &identity,
        capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
        capability::HEALTH | capability::HEALTH_DETAILS,
    );
    let mut session = wire::Session::connect(SplitIo { read, write }, &offer)?;
    let request = wire::Control {
        request_id: 1,
        operation: if std::env::args().nth(2).as_deref() == Some("drain") {
            operation::DRAIN
        } else {
            operation::HEALTH
        },
        ..Default::default()
    };
    session.send(&request)?;
    let health = wire::Health::from_response(&session.receive()?, &request)?;
    if health.process_id != pid {
        return Err("health response differs from OS peer".into());
    }
    println!(
        "pid={} ready={} draining={} active={}",
        pid, health.ready, health.draining, health.active
    );
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("This example uses Unix sockets; local::windows provides named-pipe clients.");
}
