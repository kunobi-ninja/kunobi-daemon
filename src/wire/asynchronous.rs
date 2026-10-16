use super::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Async equivalent of [`Session`]. Wrap establishment in one timeout. Cancelling
/// a frame read or write poisons the session; it cannot resume on a byte prefix.
pub struct AsyncSession<S> {
    io: S,
    buffer: Vec<u8>,
    cache: buffa::SizeCache,
    agreement: Agreement,
    failed: bool,
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncSession<S> {
    fn unestablished(io: S) -> Self {
        Self {
            io,
            buffer: Vec::new(),
            cache: buffa::SizeCache::new(),
            agreement: Agreement {
                version: VERSION,
                capabilities: 0,
                max_frame: HELLO_LIMIT,
            },
            failed: false,
        }
    }

    /// Establish a client session on an advertised binary endpoint.
    pub async fn connect(io: S, offer: &Hello) -> io::Result<Self> {
        offer.validate()?;
        let mut session = Self::unestablished(io);
        session.io.write_all(&MAGIC).await?;
        session.send_message(offer).await?;
        let remote: Hello = session.receive_message().await?;
        check_refusal(&remote)?;
        let agreement = negotiate(offer, &remote)?;
        session.io.write_all(&[1]).await?;
        session.io.flush().await?;
        session.read_acceptance().await?;
        session.agreement = agreement;
        Ok(session)
    }

    /// Establish a server session after the caller has authenticated the peer.
    pub async fn accept(io: S, offer: &Hello) -> io::Result<Self> {
        offer.validate()?;
        let (mut session, remote) = Self::offered(io).await?;
        let agreement = match negotiate(offer, &remote) {
            Ok(agreement) => agreement,
            Err(error) => {
                if let Some(refusal) = rejection(offer, &remote) {
                    // The failed negotiation is the error to report.
                    let _ = session.send_message(&refusal).await;
                }
                return Err(error);
            }
        };
        session.send_message(offer).await?;
        session.read_acceptance().await?;
        session.io.write_all(&[1]).await?;
        session.io.flush().await?;
        session.agreement = agreement;
        Ok(session)
    }

    /// Async equivalent of [`Session::refuse`].
    pub async fn refuse(io: S, offer: &Hello, reason: Refused) -> io::Result<()> {
        let (mut session, remote) = Self::offered(io).await?;
        match reason.frame(offer, &remote) {
            Some(refusal) => session.send_message(&refusal).await,
            None => Ok(()),
        }
    }

    /// Read the magic and the client's offer.
    async fn offered(io: S) -> io::Result<(Self, Hello)> {
        let mut session = Self::unestablished(io);
        let mut magic = [0; 8];
        session.io.read_exact(&mut magic).await?;
        if magic != MAGIC {
            return Err(invalid("wrong binary protocol magic"));
        }
        let remote = session.receive_message().await?;
        Ok((session, remote))
    }

    async fn read_acceptance(&mut self) -> io::Result<()> {
        if self.io.read_u8().await? != 1 {
            return Err(invalid("peer did not accept negotiation"));
        }
        Ok(())
    }

    /// Negotiated capabilities and message bound.
    pub fn agreement(&self) -> Agreement {
        self.agreement
    }

    fn usable(&self) -> io::Result<()> {
        if self.failed {
            Err(invalid("session failed; reconnect without replay"))
        } else {
            Ok(())
        }
    }

    fn check(&self, message: &Control) -> io::Result<()> {
        if required_capability(message)? & self.agreement.capabilities == 0 {
            return Err(invalid("operation capability was not negotiated"));
        }
        Ok(())
    }

    /// Send one operation. Failure or cancellation must never trigger replay.
    pub async fn send(&mut self, message: &Control) -> io::Result<()> {
        self.check(message)?;
        self.send_message(message).await
    }

    /// Receive one operation; cancellation prevents further use of this session.
    pub async fn receive(&mut self) -> io::Result<Control> {
        let message = self.receive_message().await?;
        if let Err(error) = self.check(&message) {
            self.failed = true;
            return Err(error);
        }
        Ok(message)
    }

    /// Wait for a subscription peer to disconnect. No further requests are
    /// legal on that connection. Unlike a framed receive, cancelling this
    /// single-byte read before completion leaves the session usable.
    pub(crate) async fn subscription_closed(&mut self) -> io::Result<()> {
        self.usable()?;
        let mut byte = [0];
        let result = self.io.read(&mut byte).await;
        self.failed = true;
        match result? {
            0 => Ok(()),
            _ => Err(invalid("request after WATCH")),
        }
    }

    async fn send_message<M: Message>(&mut self, message: &M) -> io::Result<()> {
        self.usable()?;
        encode_frame(
            message,
            self.agreement.max_frame,
            &mut self.buffer,
            &mut self.cache,
        )?;
        self.failed = true;
        self.io.write_all(&self.buffer).await?;
        self.io.flush().await?;
        self.failed = false;
        Ok(())
    }

    async fn receive_message<M: Message + Default>(&mut self) -> io::Result<M> {
        self.usable()?;
        self.failed = true;
        let length = self.io.read_u32_le().await?;
        if length == 0 || length > self.agreement.max_frame {
            return Err(invalid("received frame exceeds negotiated bounds"));
        }
        self.buffer.resize(length as usize, 0);
        self.io.read_exact(&mut self.buffer).await?;
        let message = decode_message(self.buffer.as_slice(), self.agreement.max_frame)?;
        self.failed = false;
        Ok(message)
    }
}
