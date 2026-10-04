//! A Unix control endpoint with ownership, authentication and independent admission.
#[cfg(unix)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use kunobi_daemon::{
        Lifecycle, ProcessLock, ServiceIdentity,
        admission::{Admission, Limits, Pool},
        control::ControlService,
        local::unix_socket::{self, Bound, SocketGuard},
        peer::SameUser,
        serve::serve,
        wire::{Hello, capability},
    };
    use std::{path::PathBuf, sync::Arc, time::Duration};
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
    // An application marks this only after its handlers can serve requests.
    service.mark_ready();
    // Accept, authenticate the peer as this OS user, admit it to the control
    // pool and serve it; stop when a DRAIN request closes admission. Running
    // exchanges, including that DRAIN's acknowledgement, get five seconds.
    let served = serve(listener, Arc::clone(&lifecycle), admission, SameUser)
        .drain_budget(Duration::from_secs(5))
        .run(
            |()| Pool::Control,
            move |connection, permit| {
                let service = Arc::clone(&service);
                let offer = offer.clone();
                async move {
                    let _permit = permit;
                    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
                    let _ = service.serve(connection, &offer, deadline).await;
                }
            },
        )
        .await
        .map_err(io::Error::other)?;
    eprintln!("{served:?}");
    lifecycle.drain().await;
    Ok(())
}
#[cfg(not(unix))]
fn main() {
    eprintln!("This example uses Unix sockets; local::windows provides named-pipe clients.");
}
