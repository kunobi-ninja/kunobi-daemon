//! Ask a running service for its health, or tell it to drain, over its binary
//! control endpoint.
//!
//! [`health`] and [`drain`] check the kernel-reported peer PID against the
//! reply and, when supplied, the expected PID. A PID match does not identify
//! an executable or the author of a discovery record; see [`crate::peer`].
//! The client also checks that the peer runs as this OS user and the reply
//! arrives before the deadline.
//!
//! A failure after the service advertised its binary endpoint is final for
//! that attempt: retry the same endpoint when [`RequestError::is_transient`]
//! says so, or rediscover it, but never fall back to an older protocol.

use crate::{
    ProcessId,
    local::{ConnectError, Duplex, Endpoint, PlatformDuplex, peer::PeerCredentials},
    peer::{ExpectedProcess, Policy, Rejected, SameUser},
    transport::SplitIo,
    wire::{self, Control, Health, Hello, operation},
};
use std::{fmt, io, time::Instant};

/// Why a control request did not produce a trusted reply.
#[derive(Debug)]
#[non_exhaustive]
pub enum RequestError {
    /// Nothing accepted the connection in time.
    Unavailable(ConnectError),
    /// The peer runs as another user or is not the expected process.
    Peer(Rejected),
    /// The OS could not read the peer credentials.
    Credentials(io::Error),
    /// The reply names a different process than the one on the connection.
    ProcessMismatch {
        /// The PID the kernel reports for the connection.
        peer: ProcessId,
        /// The PID the reply claims.
        reported: u32,
    },
    /// The deadline passed before a reply arrived.
    Late,
    /// Negotiation, framing or the reply itself failed.
    Protocol(io::Error),
}

impl RequestError {
    /// Whether retrying the same endpoint can succeed: nothing was serving it
    /// yet. Every other failure needs rediscovery or a different decision.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(error) if error.is_transient())
    }
}

impl fmt::Display for RequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(error) => write!(f, "control endpoint unavailable: {error}"),
            Self::Peer(rejected) => write!(f, "control peer refused: {rejected}"),
            Self::ProcessMismatch { peer, reported } => write!(
                f,
                "control reply names process {reported}, but the connection belongs to {peer}"
            ),
            Self::Late => f.write_str("control reply arrived after the deadline"),
            Self::Credentials(error) => write!(f, "control peer credentials failed: {error}"),
            Self::Protocol(error) => write!(f, "control exchange failed: {error}"),
        }
    }
}

impl std::error::Error for RequestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable(error) => Some(error),
            Self::Peer(rejected) => Some(rejected),
            Self::Protocol(error) | Self::Credentials(error) => Some(error),
            Self::ProcessMismatch { .. } | Self::Late => None,
        }
    }
}

/// Ask for health. See the module documentation.
pub fn health(
    endpoint: &Endpoint,
    offer: &Hello,
    expected: Option<ProcessId>,
    deadline: Instant,
) -> Result<Health, RequestError> {
    request(endpoint, offer, operation::HEALTH, expected, deadline)
}

/// Close the service's admission. The reply reports the requests still active;
/// it does not wait for them. See the module documentation.
pub fn drain(
    endpoint: &Endpoint,
    offer: &Hello,
    expected: Option<ProcessId>,
    deadline: Instant,
) -> Result<Health, RequestError> {
    request(endpoint, offer, operation::DRAIN, expected, deadline)
}

/// Send one lifecycle `operation` to the binary control endpoint and return the
/// reply, once the peer is this OS user, is `expected` when given, and the
/// reply names that same process. Everything happens before `deadline`.
fn request(
    endpoint: &Endpoint,
    offer: &Hello,
    operation: u32,
    expected: Option<ProcessId>,
    deadline: Instant,
) -> Result<Health, RequestError> {
    let stream = PlatformDuplex::connect_once_until(endpoint, deadline)
        .map_err(RequestError::Unavailable)?;
    let evidence = stream.credentials().map_err(RequestError::Credentials)?;
    match expected {
        Some(pid) => ExpectedProcess::new(move || Some(pid))
            .grant(&evidence)
            .map(drop),
        None => SameUser.grant(&evidence),
    }
    .map_err(RequestError::Peer)?;
    let peer = evidence
        .pid
        .ok_or(RequestError::Peer(Rejected::Unreported))?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(RequestError::Late);
    }
    stream
        .set_read_deadline(Some(remaining))
        .map_err(RequestError::Protocol)?;
    let (read, write) = stream.split().map_err(RequestError::Protocol)?;
    let mut session =
        wire::Session::connect(SplitIo { read, write }, offer).map_err(RequestError::Protocol)?;
    // One request per connection, so the reply needs no correlation ID.
    let message = Control {
        operation,
        ..Default::default()
    };
    session.send(&message).map_err(RequestError::Protocol)?;
    let reply = session.receive().map_err(RequestError::Protocol)?;
    let health = Health::from_response(&reply, &message).map_err(RequestError::Protocol)?;
    if health.process_id != peer.get() {
        return Err(RequestError::ProcessMismatch {
            peer,
            reported: health.process_id,
        });
    }
    if Instant::now() >= deadline {
        return Err(RequestError::Late);
    }
    Ok(health)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_an_endpoint_nothing_served_yet_is_worth_retrying() {
        assert!(RequestError::Unavailable(ConnectError::ConnectTimeout).is_transient());
        assert!(!RequestError::Unavailable(ConnectError::PermissionDenied).is_transient());
        assert!(!RequestError::Unavailable(ConnectError::EndpointTooLong).is_transient());
        assert!(!RequestError::Late.is_transient());
        assert!(!RequestError::Peer(Rejected::OtherUser).is_transient());
        assert!(!RequestError::Protocol(io::ErrorKind::Other.into()).is_transient());
    }

    #[test]
    fn nested_control_errors_keep_their_sources() {
        for error in [
            RequestError::Unavailable(ConnectError::Failed(io::ErrorKind::InvalidInput)),
            RequestError::Peer(Rejected::OtherUser),
            RequestError::Credentials(io::Error::other("credential query denied")),
            RequestError::Protocol(io::Error::other("bad frame")),
        ] {
            assert!(std::error::Error::source(&error).is_some());
            assert!(!error.is_transient());
        }
        assert!(
            std::error::Error::source(&RequestError::ProcessMismatch {
                peer: ProcessId::new(7).unwrap(),
                reported: 9,
            })
            .is_none()
        );
    }

    #[test]
    fn every_failure_says_what_happened() {
        let peer = ProcessId::new(7).unwrap();
        let cases: [(RequestError, &str); 6] = [
            (
                RequestError::Credentials(io::Error::other("denied")),
                "control peer credentials failed: denied",
            ),
            (
                RequestError::Unavailable(ConnectError::ConnectTimeout),
                "control endpoint unavailable: daemon connection timed out",
            ),
            (
                RequestError::Peer(Rejected::OtherUser),
                "control peer refused: the peer runs as another OS user",
            ),
            (
                RequestError::ProcessMismatch { peer, reported: 9 },
                "control reply names process 9, but the connection belongs to 7",
            ),
            (
                RequestError::Late,
                "control reply arrived after the deadline",
            ),
            (
                RequestError::Protocol(io::Error::other("bad frame")),
                "control exchange failed: bad frame",
            ),
        ];
        for (error, text) in cases {
            assert_eq!(error.to_string(), text);
        }
        assert!(std::error::Error::source(&RequestError::Late).is_none());
        assert!(std::error::Error::source(&RequestError::Peer(Rejected::OtherUser)).is_some());
    }
}
