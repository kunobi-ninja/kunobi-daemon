//! Named-pipe ownership with an explicit local owner ACL.
use std::io;
use std::path::Path;

use super::Duplex;

use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName};
use interprocess::os::windows::local_socket::ListenerOptionsExt;
use interprocess::os::windows::security_descriptor::SecurityDescriptor;
use widestring::u16cstr;

/// Kernel-owned listener acquisition. This is not application readiness proof.
pub enum Bound {
    /// This process owns the listener.
    Won(interprocess::local_socket::tokio::Listener),
    /// Another pipe listener already owns this endpoint.
    AlreadyRunning,
}

/// An OS error that is not normal listener contention.
#[derive(Debug)]
pub enum BindError {
    /// Bind, naming or security descriptor failure.
    Io(io::Error),
}

/// Bind a local pipe with owner, SYSTEM and Administrator access.
/// Interprocess rejects remote clients and uses FILE_FLAG_FIRST_PIPE_INSTANCE.
pub fn acquire(endpoint: &Path) -> Result<Bound, BindError> {
    let endpoint = endpoint.to_string_lossy();
    let name = endpoint
        .to_ns_name::<GenericNamespaced>()
        .map_err(BindError::Io)?;
    // Windows' default named-pipe ACL grants read access more broadly than the
    // service's same-user trust boundary permits. Owner, SYSTEM and
    // Administrators only; remote clients are disabled by interprocess too.
    let security =
        SecurityDescriptor::deserialize(u16cstr!("D:P(A;;GA;;;OW)(A;;GA;;;SY)(A;;GA;;;BA)"))
            .map_err(BindError::Io)?;
    match ListenerOptions::new()
        .name(name)
        .security_descriptor(security)
        .create_tokio()
    {
        Ok(listener) => Ok(Bound::Won(listener)),
        Err(e) if e.kind() == io::ErrorKind::AddrInUse || e.raw_os_error() == Some(231) => {
            Ok(Bound::AlreadyRunning)
        }
        // FIRST_PIPE_INSTANCE reports ACCESS_DENIED for an existing listener
        // too. Confirm a same-user live peer before classifying it as contention.
        Err(e) if e.raw_os_error() == Some(5) => {
            match super::windows::WindowsDuplex::connect_once(&endpoint) {
                Ok(peer) if peer.verify_peer_user().is_ok() => Ok(Bound::AlreadyRunning),
                _ => Err(BindError::Io(e)),
            }
        }
        Err(e) => Err(BindError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_live_pipe_owner_excludes_another_listener_and_drop_releases_it() {
        let unique = tempfile::tempdir().unwrap();
        let name = format!(
            "daemon-owner-{}-{}",
            std::process::id(),
            unique.path().file_name().unwrap().to_string_lossy()
        );
        let endpoint = Path::new(&name);
        let Bound::Won(owner) = acquire(endpoint).unwrap() else {
            panic!("first owner")
        };
        assert!(matches!(acquire(endpoint).unwrap(), Bound::AlreadyRunning));
        let other_name = format!("{name}-other");
        assert!(matches!(
            acquire(Path::new(&other_name)).unwrap(),
            Bound::Won(_)
        ));
        drop(owner);
        assert!(matches!(acquire(endpoint).unwrap(), Bound::Won(_)));
    }
}
