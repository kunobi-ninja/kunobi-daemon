//! Negotiated binary sessions on a separate v2 endpoint.
//!
//! Keep the legacy listener and its discovery key unchanged. Only clients that
//! discover an explicitly advertised v2 endpoint may send this protocol. There
//! is no fallback after starting negotiation or sending an application request.
//! The caller authenticates the OS peer and applies I/O deadlines before using
//! this module. A codec cannot enforce deadlines on an arbitrary blocking I/O.

use buffa::Message;

#[rustfmt::skip]
#[allow(missing_docs, clippy::derivable_impls)]
mod generated;
use crate::ServiceIdentity;
pub use generated::{Control, Hello, MessageKind};
use std::io::{self, Read, Write};

/// Identifies the binary transport, independently of application versions.
pub const MAGIC: [u8; 8] = *b"KNDPB002";
/// First binary protocol version. Legacy application protocols are unchanged.
pub const VERSION: u32 = 2;
/// Absolute message bound, including encoded field overhead.
pub const MAX_FRAME: u32 = 65_536;
/// Bound used before a peer's receive limit is negotiated.
pub const HELLO_LIMIT: u32 = 1_024;
/// Smallest usable negotiated frame limit.
pub const MIN_FRAME: u32 = 256;

/// Capabilities agreed for one connection, not inferred from the build string.
pub mod capability {
    /// Read-only health observations.
    pub const HEALTH: u64 = 1;
    /// Close admission and drain active work.
    pub const DRAIN: u64 = 2;
    /// Cooperative prepare/ready/commit/abort exchange.
    pub const HANDOFF: u64 = 4;
    /// Application-defined operations, carried as opaque bounded payloads.
    pub const APPLICATION: u64 = 8;
}

/// Stable operation numbers. Never reuse retired numbers.
pub mod operation {
    /// Health request or response; application defines the observation payload.
    pub const HEALTH: u32 = 1;
    /// Drain request or acknowledgement.
    pub const DRAIN: u32 = 2;
    /// Offer a destination generation.
    pub const PREPARE: u32 = 3;
    /// Acknowledge the pause boundary.
    pub const PAUSED: u32 = 4;
    /// Acknowledge readiness to switch.
    pub const READY: u32 = 5;
    /// Commit the negotiated handoff.
    pub const COMMIT: u32 = 6;
    /// Abandon a handoff attempt.
    pub const ABORT: u32 = 7;
}

impl Hello {
    /// Offer capabilities for a stable service installation. Use the same
    /// identity to derive resource paths and negotiate on both endpoints.
    pub fn new(identity: &ServiceIdentity, supported: u64, required: u64) -> Self {
        Self {
            minimum: VERSION,
            maximum: VERSION,
            supported,
            required,
            max_frame: MAX_FRAME,
            application: identity.application().into(),
            profile: identity.profile().into(),
            instance: identity.instance().into(),
            service_id: identity.id().to_vec(),
            ..Default::default()
        }
    }

    fn validate(&self) -> io::Result<()> {
        ServiceIdentity::new(
            self.service_id
                .as_slice()
                .try_into()
                .map_err(|_| invalid("service UUID must have 16 bytes"))?,
            &self.application,
            &self.profile,
            &self.instance,
        )
        .map_err(|_| invalid("invalid service identity"))?;
        if self.minimum == 0
            || self.minimum > self.maximum
            || self.required & !self.supported != 0
            || !(MIN_FRAME..=MAX_FRAME).contains(&self.max_frame)
        {
            return Err(invalid("invalid version or capability offer"));
        }
        Ok(())
    }
}

/// Agreed limits for one connection. Both peers independently calculate this.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Agreement {
    /// Selected binary version.
    pub version: u32,
    /// Intersection of the peers' supported capabilities.
    pub capabilities: u64,
    /// Maximum encoded message size in either direction.
    pub max_frame: u32,
}

/// Negotiate compatible versions, required capabilities and receive bounds.
/// Unknown optional capabilities are ignored; unknown required ones fail.
pub fn negotiate(local: &Hello, remote: &Hello) -> io::Result<Agreement> {
    local.validate()?;
    remote.validate()?;
    if local.service_id != remote.service_id
        || local.application != remote.application
        || local.profile != remote.profile
        || local.instance != remote.instance
    {
        return Err(invalid("service identity mismatch"));
    }
    let minimum = local.minimum.max(remote.minimum);
    let maximum = local.maximum.min(remote.maximum);
    if minimum > VERSION || maximum < VERSION {
        return Err(invalid("no implemented binary version in common"));
    }
    let capabilities = local.supported & remote.supported;
    if (local.required | remote.required) & !capabilities != 0 {
        return Err(invalid("required capability unavailable"));
    }
    Ok(Agreement {
        version: VERSION,
        capabilities,
        max_frame: local.max_frame.min(remote.max_frame),
    })
}

fn required_capability(message: &Control) -> io::Result<u64> {
    match message.kind.as_known() {
        Some(MessageKind::Application) if message.operation != 0 => {
            return Ok(capability::APPLICATION);
        }
        Some(MessageKind::Lifecycle) => {}
        _ => return Err(invalid("invalid message kind or application operation")),
    }
    use self::{capability as c, operation as o};
    match message.operation {
        o::HEALTH => Ok(c::HEALTH),
        o::DRAIN => Ok(c::DRAIN),
        o::PREPARE..=o::ABORT => Ok(c::HANDOFF),
        _ => Err(invalid("unknown control operation")),
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn encode_frame<M: Message>(
    message: &M,
    limit: u32,
    buffer: &mut Vec<u8>,
    cache: &mut buffa::SizeCache,
) -> io::Result<()> {
    buffer.clear();
    buffer.extend_from_slice(&[0; 4]);
    let length = message
        .try_encode_bounded_with_cache(limit, cache, buffer)
        .map_err(io::Error::other)?;
    if length == 0 {
        return Err(invalid("empty message"));
    }
    buffer[..4].copy_from_slice(&length.to_le_bytes());
    Ok(())
}

fn decode_message<M: Message>(buffer: &[u8], limit: u32) -> io::Result<M> {
    buffa::DecodeOptions::new()
        .with_max_message_size(limit as usize)
        .with_recursion_limit(32)
        .with_unknown_field_limit(128)
        .with_element_memory_limit(MAX_FRAME as usize)
        .decode_from_slice(buffer)
        .map_err(io::Error::other)
}

/// Bounded framing with reusable scratch storage. The first four bytes are the
/// encoded body length in little endian, not a native Rust struct layout.
/// A framing/I/O error poisons this codec: reconnect instead of retrying bytes.
pub struct Framed<S> {
    io: S,
    buffer: Vec<u8>,
    cache: buffa::SizeCache,
    limit: u32,
    failed: bool,
}

impl<S: Read + Write> Framed<S> {
    /// Construct a frame codec; normally use [`Session::connect`] or [`Session::accept`].
    pub fn new(io: S, limit: u32) -> io::Result<Self> {
        if !(MIN_FRAME..=MAX_FRAME).contains(&limit) {
            return Err(invalid("invalid frame limit"));
        }
        Ok(Self {
            io,
            buffer: Vec::new(),
            cache: buffa::SizeCache::new(),
            limit,
            failed: false,
        })
    }

    fn usable(&self) -> io::Result<()> {
        if self.failed {
            Err(invalid("session failed; reconnect without replay"))
        } else {
            Ok(())
        }
    }

    /// Encode one complete bounded message and flush it. No automatic retries.
    pub fn send<M: Message>(&mut self, message: &M) -> io::Result<()> {
        self.usable()?;
        encode_frame(message, self.limit, &mut self.buffer, &mut self.cache)?;
        self.failed = true;
        self.io.write_all(&self.buffer)?;
        self.io.flush()?;
        self.failed = false;
        Ok(())
    }

    /// Read exactly one message without consuming bytes from the next frame.
    /// An oversized header is rejected before allocating its advertised size.
    pub fn receive<M: Message + Default>(&mut self) -> io::Result<M> {
        self.usable()?;
        self.failed = true;
        let mut length = [0; 4];
        self.io.read_exact(&mut length)?;
        let length = u32::from_le_bytes(length);
        if length == 0 || length > self.limit {
            return Err(invalid("received frame exceeds negotiated bounds"));
        }
        self.buffer.resize(length as usize, 0);
        self.io.read_exact(&mut self.buffer)?;
        let value = decode_message(self.buffer.as_slice(), self.limit)?;
        self.failed = false;
        Ok(value)
    }
}

/// An established binary session. Construction completes both offers and both
/// acceptance bytes before application operations may be sent or received.
pub struct Session<S> {
    frames: Framed<S>,
    agreement: Agreement,
}

impl<S: Read + Write> Session<S> {
    /// Connect on an explicitly advertised v2 endpoint. The caller sets a single
    /// establishment deadline and verifies the peer before sending the offer.
    pub fn connect(mut io: S, offer: &Hello) -> io::Result<Self> {
        offer.validate()?;
        io.write_all(&MAGIC)?;
        let mut frames = Framed::new(io, HELLO_LIMIT)?;
        frames.send(offer)?;
        let remote: Hello = frames.receive()?;
        let agreement = negotiate(offer, &remote)?;
        frames.io.write_all(&[1])?;
        frames.io.flush()?;
        read_acceptance(&mut frames.io)?;
        frames.limit = agreement.max_frame;
        Ok(Self { frames, agreement })
    }

    /// Admit a connection on the separate v2 listener, after authenticating its
    /// OS peer. No request is dispatched if either peer rejects the negotiation.
    pub fn accept(mut io: S, offer: &Hello) -> io::Result<Self> {
        offer.validate()?;
        let mut magic = [0; 8];
        io.read_exact(&mut magic)?;
        if magic != MAGIC {
            return Err(invalid("wrong binary protocol magic"));
        }
        let mut frames = Framed::new(io, HELLO_LIMIT)?;
        let remote: Hello = frames.receive()?;
        let agreement = negotiate(offer, &remote)?;
        frames.send(offer)?;
        read_acceptance(&mut frames.io)?;
        frames.io.write_all(&[1])?;
        frames.io.flush()?;
        frames.limit = agreement.max_frame;
        Ok(Self { frames, agreement })
    }

    /// Access the transport to change I/O deadlines after establishment.
    /// Do not read or write protocol bytes through this reference.
    pub fn transport_mut(&mut self) -> &mut S {
        &mut self.frames.io
    }

    /// Negotiated version, capabilities and frame bound.
    pub fn agreement(&self) -> Agreement {
        self.agreement
    }

    /// Send a negotiated operation. An I/O failure has an uncertain outcome;
    /// reconnecting must not replay it automatically.
    pub fn send(&mut self, message: &Control) -> io::Result<()> {
        self.check(message)?;
        self.frames.send(message)
    }

    /// Receive a negotiated operation. Unknown operations terminate the session;
    /// unknown optional fields within known operations remain compatible.
    pub fn receive(&mut self) -> io::Result<Control> {
        let message = self.frames.receive()?;
        if let Err(error) = self.check(&message) {
            self.frames.failed = true;
            return Err(error);
        }
        Ok(message)
    }

    fn check(&self, message: &Control) -> io::Result<()> {
        if required_capability(message)? & self.agreement.capabilities == 0 {
            return Err(invalid("operation capability was not negotiated"));
        }
        Ok(())
    }
}

fn read_acceptance(io: &mut impl Read) -> io::Result<()> {
    let mut accepted = [0];
    io.read_exact(&mut accepted)?;
    if accepted != [1] {
        return Err(invalid("peer did not accept negotiation"));
    }
    Ok(())
}

/// The client's endpoint choice is made before any socket bytes are sent.
#[derive(Debug, PartialEq, Eq)]
pub enum Endpoint<'a> {
    /// Use the unchanged legacy protocol on this endpoint.
    Legacy(&'a str),
    /// Use binary negotiation exclusively on this advertised endpoint.
    Binary(&'a str),
}

/// Prefer an explicitly advertised binary endpoint. An old discovery record
/// selects legacy. Malformed v2 advertisements fail instead of downgrading.
pub fn select_endpoint<'a>(legacy: &'a str, binary: Option<&'a str>) -> io::Result<Endpoint<'a>> {
    if legacy.is_empty() {
        return Err(invalid("missing legacy endpoint"));
    }
    match binary {
        None => Ok(Endpoint::Legacy(legacy)),
        Some(endpoint) if !endpoint.is_empty() && endpoint != legacy => {
            Ok(Endpoint::Binary(endpoint))
        }
        Some(_) => Err(invalid("binary endpoint must be distinct and nonempty")),
    }
}

#[cfg(feature = "wire-async")]
mod asynchronous;
#[cfg(feature = "wire-async")]
pub use asynchronous::AsyncSession;
