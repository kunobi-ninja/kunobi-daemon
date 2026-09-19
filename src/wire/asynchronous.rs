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
        let mut session = Self::unestablished(io);
        let mut magic = [0; 8];
        session.io.read_exact(&mut magic).await?;
        if magic != MAGIC {
            return Err(invalid("wrong binary protocol magic"));
        }
        let remote: Hello = session.receive_message().await?;
        let agreement = negotiate(offer, &remote)?;
        session.send_message(offer).await?;
        session.read_acceptance().await?;
        session.io.write_all(&[1]).await?;
        session.io.flush().await?;
        session.agreement = agreement;
        Ok(session)
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
