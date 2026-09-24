//! Named-pipe ownership with an explicit local owner ACL.
use std::io;
use std::path::Path;
use std::time::Duration;

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

const ERROR_ACCESS_DENIED: i32 = 5;
const ERROR_PIPE_BUSY: i32 = 231;

/// Pauses between bind attempts while `ACCESS_DENIED` meets no live peer.
///
/// A listener that just closed can leave pipe instances in the kernel for a
/// moment, and `FILE_FLAG_FIRST_PIPE_INSTANCE` refuses a new first instance
/// with `ACCESS_DENIED` until they go. Retrying for about a third of a second
/// rides that out; a denial that outlasts it is a real one.
const CLOSING_PIPE_BACKOFF: [Duration; 4] = [
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(200),
];

/// What to do after a failed bind.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    /// Another listener owns the endpoint.
    Contended,
    /// Wait this long, then try to bind again.
    Retry(Duration),
    /// A real error: report it.
    Fail,
}

/// Decide what a failed bind means.
///
/// `FILE_FLAG_FIRST_PIPE_INSTANCE` reports `ACCESS_DENIED` for an existing
/// listener too, so that case is only contention once a same-user live peer
/// answers on the endpoint. Without one it is usually a listener still
/// closing, so it is retried a few times before it counts as an error.
fn next_step(
    kind: io::ErrorKind,
    code: Option<i32>,
    peer_answered: bool,
    retries_used: usize,
) -> Next {
    if kind == io::ErrorKind::AddrInUse || code == Some(ERROR_PIPE_BUSY) {
        return Next::Contended;
    }
    if code != Some(ERROR_ACCESS_DENIED) {
        return Next::Fail;
    }
    if peer_answered {
        return Next::Contended;
    }
    CLOSING_PIPE_BACKOFF
        .get(retries_used)
        .map_or(Next::Fail, |pause| Next::Retry(*pause))
}

fn live_same_user_peer(endpoint: &str) -> bool {
    matches!(
        super::windows::WindowsDuplex::connect_once(endpoint),
        Ok(peer) if peer.verify_peer_user().is_ok()
    )
}

/// Run `create` until it binds, meets another owner, or fails for good.
fn bind<L>(
    endpoint: &str,
    mut create: impl FnMut() -> io::Result<L>,
) -> Result<Bound<L>, BindError> {
    let mut retries_used = 0;
    loop {
        let error = match create() {
            Ok(listener) => return Ok(Bound::Won(listener)),
            Err(error) => error,
        };
        let peer_answered =
            error.raw_os_error() == Some(ERROR_ACCESS_DENIED) && live_same_user_peer(endpoint);
        match next_step(
            error.kind(),
            error.raw_os_error(),
            peer_answered,
            retries_used,
        ) {
            Next::Contended => return Ok(Bound::AlreadyRunning),
            Next::Retry(pause) => {
                retries_used += 1;
                std::thread::sleep(pause);
            }
            Next::Fail => return Err(BindError::Io(error)),
        }
    }
}

/// Bind a local pipe, blocking, as the Unix side does.
///
/// Every instance interprocess creates is opened `FILE_FLAG_OVERLAPPED`
/// regardless, so this differs from [`acquire_tokio`] only in which listener
/// wrapper owns the handle, not in how the pipe is created.
pub fn acquire(endpoint: &Path) -> Result<Bound<interprocess::local_socket::Listener>, BindError> {
    let endpoint = endpoint.to_string_lossy();
    // Checked once so a bad name or descriptor is reported, not retried.
    options(&endpoint)?;
    bind(&endpoint, || match options(&endpoint) {
        Ok(options) => options.create_sync(),
        Err(BindError::Io(error)) => Err(error),
    })
}

/// Bind the same pipe for a Tokio accept loop.
///
/// A bind that meets a listener still closing waits for it on the calling
/// thread, for at most the few hundred milliseconds in
/// `CLOSING_PIPE_BACKOFF`.
#[cfg(feature = "local-async")]
pub fn acquire_tokio(
    endpoint: &Path,
) -> Result<Bound<interprocess::local_socket::tokio::Listener>, BindError> {
    let endpoint = endpoint.to_string_lossy();
    options(&endpoint)?;
    bind(&endpoint, || match options(&endpoint) {
        Ok(options) => options.create_tokio(),
        Err(BindError::Io(error)) => Err(error),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contention_is_reported_without_retrying() {
        assert_eq!(
            next_step(io::ErrorKind::AddrInUse, None, false, 0),
            Next::Contended
        );
        assert_eq!(
            next_step(io::ErrorKind::Other, Some(ERROR_PIPE_BUSY), false, 0),
            Next::Contended
        );
        assert_eq!(
            next_step(
                io::ErrorKind::PermissionDenied,
                Some(ERROR_ACCESS_DENIED),
                true,
                0
            ),
            Next::Contended
        );
    }

    #[test]
    fn a_denial_with_no_peer_is_retried_then_fails() {
        let denied = |used| {
            next_step(
                io::ErrorKind::PermissionDenied,
                Some(ERROR_ACCESS_DENIED),
                false,
                used,
            )
        };
        for (used, pause) in CLOSING_PIPE_BACKOFF.iter().enumerate() {
            assert_eq!(denied(used), Next::Retry(*pause));
        }
        assert_eq!(denied(CLOSING_PIPE_BACKOFF.len()), Next::Fail);
    }

    #[test]
    fn other_errors_fail_at_once() {
        assert_eq!(
            next_step(io::ErrorKind::NotFound, Some(2), false, 0),
            Next::Fail
        );
    }

    #[test]
    fn a_denial_that_clears_binds_on_a_later_attempt() {
        let name = unique_name("closing");
        let mut attempts = 0;
        let bound = bind(&name, || {
            attempts += 1;
            if attempts < 3 {
                Err(io::Error::from_raw_os_error(ERROR_ACCESS_DENIED))
            } else {
                Ok("listener")
            }
        })
        .unwrap();
        assert!(matches!(bound, Bound::Won("listener")));
        assert_eq!(attempts, 3);
    }

    #[test]
    fn a_denial_that_never_clears_is_the_original_error() {
        let name = unique_name("denied");
        let mut attempts = 0;
        let result = bind::<()>(&name, || {
            attempts += 1;
            Err(io::Error::from_raw_os_error(ERROR_ACCESS_DENIED))
        });
        let Err(BindError::Io(error)) = result else {
            panic!("a lasting denial must fail")
        };
        assert_eq!(error.raw_os_error(), Some(ERROR_ACCESS_DENIED));
        assert_eq!(attempts, CLOSING_PIPE_BACKOFF.len() + 1);
    }

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
