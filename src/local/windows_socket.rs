//! Named-pipe ownership with an explicit local owner ACL.
use std::io;
use std::path::Path;

use super::Duplex;

use interprocess::local_socket::{GenericNamespaced, ListenerOptions, ToNsName};
use interprocess::os::windows::local_socket::ListenerOptionsExt;
use interprocess::os::windows::security_descriptor::SecurityDescriptor;
use widestring::u16cstr;

pub use super::Bound;

/// An OS error that is not normal listener contention.
#[derive(Debug)]
pub enum BindError {
    /// Bind, naming or security descriptor failure.
    Io(io::Error),
}

/// Owner, SYSTEM and Administrators only.
///
/// Windows' default named-pipe ACL grants read access more broadly than the
/// same-user trust boundary permits. Remote clients are disabled by
/// interprocess as well.
fn options(name: &str) -> Result<ListenerOptions<'_>, BindError> {
    let name = name
        .to_ns_name::<GenericNamespaced>()
        .map_err(BindError::Io)?;
    let security =
        SecurityDescriptor::deserialize(u16cstr!("D:P(A;;GA;;;OW)(A;;GA;;;SY)(A;;GA;;;BA)"))
            .map_err(BindError::Io)?;
    Ok(ListenerOptions::new()
        .name(name)
        .security_descriptor(security))
}

/// Decide whether a failed bind is contention or a real error.
///
/// `FILE_FLAG_FIRST_PIPE_INSTANCE` reports `ACCESS_DENIED` for an existing
/// listener too, so that case is only contention once a same-user live peer
/// answers on the endpoint.
fn classify<L>(endpoint: &str, error: io::Error) -> Result<Bound<L>, BindError> {
    if error.kind() == io::ErrorKind::AddrInUse || error.raw_os_error() == Some(231) {
        return Ok(Bound::AlreadyRunning);
    }
    if error.raw_os_error() == Some(5) {
        return match super::windows::WindowsDuplex::connect_once(endpoint) {
            Ok(peer) if peer.verify_peer_user().is_ok() => Ok(Bound::AlreadyRunning),
            _ => Err(BindError::Io(error)),
        };
    }
    Err(BindError::Io(error))
}

/// Bind a local pipe, blocking, as the Unix side does.
///
/// Every instance interprocess creates is opened `FILE_FLAG_OVERLAPPED`
/// regardless, so this differs from [`acquire_tokio`] only in which listener
/// wrapper owns the handle, not in how the pipe is created.
pub fn acquire(endpoint: &Path) -> Result<Bound<interprocess::local_socket::Listener>, BindError> {
    let endpoint = endpoint.to_string_lossy();
    match options(&endpoint)?.create_sync() {
        Ok(listener) => Ok(Bound::Won(listener)),
        Err(error) => classify(&endpoint, error),
    }
}

/// Bind the same pipe for a Tokio accept loop.
#[cfg(feature = "local-async")]
pub fn acquire_tokio(
    endpoint: &Path,
) -> Result<Bound<interprocess::local_socket::tokio::Listener>, BindError> {
    let endpoint = endpoint.to_string_lossy();
    match options(&endpoint)?.create_tokio() {
        Ok(listener) => Ok(Bound::Won(listener)),
        Err(error) => classify(&endpoint, error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_name(tag: &str) -> String {
        let unique = tempfile::tempdir().unwrap();
        format!(
            "daemon-{tag}-{}-{}",
            std::process::id(),
            unique.path().file_name().unwrap().to_string_lossy()
        )
    }

    #[test]
    fn a_live_pipe_owner_excludes_another_listener_and_drop_releases_it() {
        let name = unique_name("owner");
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

    #[cfg(feature = "local-async")]
    #[tokio::test]
    async fn the_tokio_acquisition_contends_with_the_blocking_one_for_the_same_endpoint() {
        let name = unique_name("tokio-owner");
        let endpoint = Path::new(&name);
        let Bound::Won(owner) = acquire_tokio(endpoint).unwrap() else {
            panic!("first owner")
        };
        // Same kernel object: a blocking binder must see the Tokio owner.
        assert!(matches!(acquire(endpoint).unwrap(), Bound::AlreadyRunning));
        drop(owner);
        assert!(matches!(acquire_tokio(endpoint).unwrap(), Bound::Won(_)));
    }
}
