//! A Unix control endpoint with ownership, authentication and independent admission.
#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use kunobi_daemon::{
        Lifecycle, ProcessLock, ServiceIdentity,
        admission::{Admission, Limits, Pool},
        control::ControlService,
        local::{
            unix,
            unix_socket::{self, Bound, SocketGuard},
        },
        wire::{Hello, capability},
    };
    use std::{os::fd::AsRawFd, path::PathBuf, sync::Arc, time::Duration};
    let root = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("usage: lifecycle_server DIRECTORY")?,
    );
    unix_socket::ensure_private_dir(&root)?;
    let _owner = ProcessLock::try_acquire(root.join("run.lock"))?.ok_or("already owned")?;
    let path = root.join("control.sock");
    let Bound::Won(listener) = unix_socket::acquire(&path).map_err(|error| format!("{error:?}"))?
    else {
        return Err("endpoint already owned".into());
    };
    let _cleanup = SocketGuard::capture(&path)?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::UnixListener::from_std(listener)?;
    let identity = ServiceIdentity::new([0x71; 16], "example-daemon", "demo", "default")?;
    let offer = Hello::new(
        &identity,
        capability::HEALTH | capability::HEALTH_DETAILS | capability::DRAIN,
        capability::HEALTH | capability::HEALTH_DETAILS,
    );
    let lifecycle = Arc::new(Lifecycle::default());
    let service = Arc::new(ControlService::new(
        Arc::clone(&lifecycle),
        1,
        "example".into(),
        1,
    ));
    let admission = Arc::new(Admission::new(Limits::default()));
    let mut connections = tokio::task::JoinSet::new();
    // An application marks this only after its handlers can serve requests.
    service.mark_ready();
    loop {
        tokio::select! {
            _ = lifecycle.draining() => break,
            Some(_) = connections.join_next(), if !connections.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                if unix::peer_uid(stream.as_raw_fd())? != unix::own_uid() { continue; }
                let Some(permit) = admission.try_acquire(Pool::Control) else { continue };
                let service = Arc::clone(&service);
                let offer = offer.clone();
                connections.spawn(async move {
                    let _permit = permit;
                    service.serve(stream, &offer, tokio::time::Instant::now() + Duration::from_secs(2)).await
                });
            }
        }
    }
    // Preserve the drain acknowledgement before dropping the runtime and owner.
    while connections.join_next().await.is_some() {}
    lifecycle.drain().await;
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("This example uses Unix sockets; local::windows provides named-pipe clients.");
}
