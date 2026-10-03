//! WATCH carries complete state, rather than an unbounded transition history.
use super::ControlService;
use crate::wire::{
    self, Control, Hello, LifecycleChange, LifecycleEvent, MessageKind, capability, operation,
};
use buffa::Message;
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::watch,
    time::Instant,
};

#[derive(Clone, PartialEq)]
struct Published {
    generation: Option<u64>,
    build: String,
    retiring: bool,
}

pub(super) struct Publisher(watch::Sender<Published>);
impl Publisher {
    pub(super) fn new() -> Self {
        Self(
            watch::channel(Published {
                generation: None,
                build: String::new(),
                retiring: false,
            })
            .0,
        )
    }
    pub(super) fn notify(&self) {
        self.0.send_modify(|_| {});
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

impl ControlService {
    /// Announce an already committed selection. Call only after publication
    /// succeeds; this method does not write the application's discovery record.
    /// A generation cannot move backwards or change its build identity. Until
    /// the first call, WATCH reports no observed selection. A staged process
    /// may observe an incumbent with a lower generation than its own.
    pub fn selection_committed(&self, generation: u64, build: String) -> io::Result<()> {
        if generation == self.generation && build != self.build {
            return Err(invalid("selection does not match serving build"));
        }
        let mut result = Ok(());
        self.events.0.send_if_modified(|state| {
            if state
                .generation
                .is_some_and(|previous| generation < previous)
                || (Some(generation) == state.generation && build != state.build)
            {
                result = Err(invalid("selection must advance monotonically"));
                return false;
            }
            if Some(generation) == state.generation {
                return false;
            }
            state.generation = Some(generation);
            state.build = build;
            true
        });
        result
    }

    /// Close admission and announce retirement. Existing requests keep their
    /// leases; the caller still owns draining and process shutdown.
    pub fn mark_retiring(&self) {
        self.lifecycle.start_drain();
        self.events.0.send_if_modified(|state| {
            if state.retiring {
                return false;
            }
            state.retiring = true;
            true
        });
    }

    pub(super) fn validate_watch(&self, request: &Control) -> io::Result<()> {
        if !request.payload.is_empty()
            || !request.token.is_empty()
            || request.offset.is_some()
            || (request.generation != 0 && request.generation != self.generation)
        {
            return Err(invalid("invalid WATCH request"));
        }
        Ok(())
    }

    pub(super) async fn serve_watch<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        mut session: wire::AsyncSession<S>,
        request: Control,
    ) -> io::Result<()> {
        // Subscribe before sampling so publication between those operations
        // cannot be lost. The channel retains state for late subscribers.
        let mut changes = self.events.0.subscribe();
        let mut previous: Option<LifecycleEvent> = None;
        let mut sequence = 0u64;
        loop {
            let published = changes.borrow_and_update().clone();
            let draining = self.lifecycle.snapshot().draining;
            let mut event = LifecycleEvent {
                process_id: self.process_id,
                generation: self.generation,
                build: self.build.clone(),
                ready: self.ready.load(std::sync::atomic::Ordering::Acquire) && !draining,
                draining,
                selected_generation: published.generation,
                selected_build: published.build,
                retiring: published.retiring,
                ..Default::default()
            };
            if let Some(old) = &previous {
                let changed = [
                    (
                        event.ready != old.ready && event.ready,
                        LifecycleChange::Ready,
                    ),
                    (event.draining != old.draining, LifecycleChange::Draining),
                    (
                        event.selected_generation != old.selected_generation,
                        LifecycleChange::SelectionChanged,
                    ),
                    (event.retiring != old.retiring, LifecycleChange::Retiring),
                ];
                let mut kinds = changed
                    .into_iter()
                    .filter_map(|(changed, kind)| changed.then_some(kind));
                let Some(first) = kinds.next() else {
                    tokio::select! {
                        result = session.subscription_closed() => return result,
                        _ = changes.changed() => {},
                        _ = self.lifecycle.draining(), if !draining => {},
                    }
                    continue;
                };
                event.change = if kinds.next().is_none() {
                    first
                } else {
                    LifecycleChange::Snapshot
                }
                .into();
            }
            sequence = sequence
                .checked_add(1)
                .ok_or_else(|| invalid("WATCH sequence exhausted"))?;
            event.sequence = sequence;
            let frame = Control {
                operation: operation::WATCH,
                request_id: request.request_id,
                generation: self.generation,
                payload: event.encode_to_vec(),
                ..Default::default()
            };
            // Idle subscribers have no timer. A client that stops reading gets
            // a bounded write, so it cannot retain control admission forever.
            tokio::time::timeout(Duration::from_secs(5), session.send(&frame))
                .await
                .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))??;
            previous = Some(event);
        }
    }
}

/// A negotiated lifecycle subscription. A peer without WATCH returns `None`
/// during setup so the application may retain discovery polling. An error
/// after advertising WATCH is terminal; it never triggers a protocol downgrade.
///
/// Authenticate the OS peer before connecting and compare `snapshot().process_id`
/// with the peer PID the kernel reports for this connection. Agreement checks the
/// protocol's claim against transport evidence; it does not authenticate the
/// executable or any discovery record. Cancelling `changed` poisons the
/// underlying framed session; drop it and reconnect rather than continuing from
/// a partial frame.
pub struct WatchClient<S> {
    session: wire::AsyncSession<S>,
    request: Control,
    latest: LifecycleEvent,
    failed: bool,
}
impl<S: AsyncRead + AsyncWrite + Unpin> WatchClient<S> {
    /// Negotiate, subscribe, and receive the initial snapshot under one deadline.
    pub async fn connect(stream: S, offer: &Hello, deadline: Instant) -> io::Result<Option<Self>> {
        if offer.supported & capability::WATCH == 0 {
            return Err(invalid("client offer does not support WATCH"));
        }
        tokio::time::timeout_at(deadline, async {
            let mut session = wire::AsyncSession::connect(stream, offer).await?;
            if session.agreement().capabilities & capability::WATCH == 0 {
                return Ok(None);
            }
            let request = Control {
                operation: operation::WATCH,
                ..Default::default()
            };
            session.send(&request).await?;
            let latest = decode_event(&session.receive().await?, &request)?;
            if latest.sequence != 1 || latest.change != LifecycleChange::Snapshot {
                return Err(invalid("WATCH must start with a snapshot"));
            }
            Ok(Some(Self {
                session,
                request,
                latest,
                failed: false,
            }))
        })
        .await
        .map_err(|_| io::Error::from(io::ErrorKind::TimedOut))?
    }

    /// Most recently received state, including the initial snapshot.
    pub fn snapshot(&self) -> &LifecycleEvent {
        &self.latest
    }

    /// Wait for new state. There is no background polling or idle timeout.
    pub async fn changed(&mut self) -> io::Result<&LifecycleEvent> {
        if self.failed {
            return Err(invalid("WATCH failed; reconnect"));
        }
        self.failed = true;
        let event = decode_event(&self.session.receive().await?, &self.request)?;
        if self.latest.sequence.checked_add(1) != Some(event.sequence)
            || event.process_id != self.latest.process_id
            || event.generation != self.latest.generation
            || event.build != self.latest.build
            || event.selected_generation < self.latest.selected_generation
            || (event.selected_generation == self.latest.selected_generation
                && event.selected_build != self.latest.selected_build)
            || (self.latest.draining && !event.draining)
            || (self.latest.retiring && !event.retiring)
        {
            return Err(invalid("inconsistent WATCH event"));
        }
        self.latest = event;
        self.failed = false;
        Ok(&self.latest)
    }
}

fn decode_event(reply: &Control, request: &Control) -> io::Result<LifecycleEvent> {
    if reply.kind != MessageKind::Lifecycle
        || reply.operation != operation::WATCH
        || reply.request_id != request.request_id
        || !reply.token.is_empty()
        || reply.offset.is_some()
    {
        return Err(invalid("unexpected WATCH response"));
    }
    let event: LifecycleEvent = wire::decode_message(&reply.payload, wire::MAX_FRAME)?;
    if event.change.as_known().is_none()
        || event.process_id == 0
        || event.generation != reply.generation
        || (event.selected_generation.is_none() && !event.selected_build.is_empty())
        || (event.selected_generation == Some(event.generation)
            && event.selected_build != event.build)
        || (event.change == LifecycleChange::SelectionChanged
            && event.selected_generation.is_none())
        || (event.ready && event.draining)
        || (event.retiring && !event.draining)
        || (event.change == LifecycleChange::Ready && !event.ready)
        || (event.change == LifecycleChange::Draining && !event.draining)
        || (event.change == LifecycleChange::Retiring && !event.retiring)
    {
        return Err(invalid("invalid WATCH state"));
    }
    Ok(event)
}
