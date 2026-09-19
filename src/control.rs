//! In-memory health and drain handlers. The listener owns peer authentication.
use crate::{
    Lifecycle,
    wire::{self, Control, Health, Hello, MessageKind, operation},
};
use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::io::{AsyncRead, AsyncWrite};

/// One process's lifecycle control, independent of application capacity and I/O.
pub struct ControlService {
    lifecycle: Arc<Lifecycle>,
    ready: AtomicBool,
    process_id: u32,
    generation: u64,
    build: String,
    revision: u64,
}
impl ControlService {
    /// Start in the initializing state. The consumer marks readiness explicitly.
    pub fn new(lifecycle: Arc<Lifecycle>, generation: u64, build: String, revision: u64) -> Self {
        Self {
            lifecycle,
            ready: AtomicBool::new(false),
            process_id: std::process::id(),
            generation,
            build,
            revision,
        }
    }

    /// Mark application initialization complete. Drain always takes precedence.
    pub fn mark_ready(&self) {
        self.ready.store(true, Ordering::Release);
    }

    /// Read a bounded snapshot without waiting for storage, writers or draining.
    pub fn snapshot(&self) -> Health {
        let state = self.lifecycle.snapshot();
        Health {
            process_id: self.process_id,
            generation: self.generation,
            build: self.build.clone(),
            revision: self.revision,
            ready: self.ready.load(Ordering::Acquire) && !state.draining,
            draining: state.draining,
            active: state.active as u64,
            ..Default::default()
        }
    }

    /// Handle a control request without admitting application work or waiting for drain.
    pub fn handle(&self, request: &Control) -> io::Result<Control> {
        if request.kind != MessageKind::Lifecycle
            || !request.payload.is_empty()
            || !request.token.is_empty()
            || request.offset.is_some()
            || (request.generation != 0 && request.generation != self.generation)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid lifecycle request",
            ));
        }
        match request.operation {
            operation::HEALTH => {}
            operation::DRAIN => {
                self.lifecycle.start_drain();
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unsupported lifecycle request",
                ));
            }
        }
        self.snapshot().response(request)
    }

    /// Serve one authenticated connection under an absolute setup/control deadline.
    ///
    /// No application operation is accepted here. The caller reserves control
    /// admission before invoking this method and drops the stream after an error.
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: S,
        offer: &Hello,
        deadline: tokio::time::Instant,
    ) -> io::Result<()> {
        tokio::time::timeout_at(deadline, async {
            let mut session = wire::AsyncSession::accept(stream, offer).await?;
            if session.agreement().capabilities & wire::capability::HEALTH_DETAILS == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "typed health not negotiated",
                ));
            }
            let request = session.receive().await?;
            session.send(&self.handle(&request)?).await
        })
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
    }
}
