//! A readiness channel from a daemon to the process that started it.
//!
//! A launcher that only polls a probe learns that its daemon died during
//! startup when the deadline passes, and a fixed deadline can fail a daemon
//! that is still making progress on a slow machine. Over this channel the
//! daemon says when it is ready and may report progress on the way. The
//! channel ending before `ready` tells the launcher at once that the daemon
//! died.
//!
//! The launcher asks for the channel with `launch::DaemonCommand::readiness_channel`
//! (`launch` feature) and takes a [`Channel`] from the started daemon. The
//! daemon takes its end with `Notifier::from_env` (`local` feature), reports
//! progress, and sends ready.
//!
//! The launcher's deadline is the longest silence it accepts. Every message
//! starts it again, so a daemon that keeps reporting progress is waited for as
//! long as it takes.
//!
//! `ready` is not a proof. It says when a probe is worth making; a fresh probe
//! still decides, as everywhere in [`crate::readiness`] and [`crate::selection`].
//! [`Signaled`] adds a channel to selection evidence without changing the
//! selection rules.
//!
//! # Wire format
//!
//! One message per line, in ASCII, each ending in a newline (`\n`) and at most
//! [`MAX_MESSAGE`] bytes long with it:
//!
//! - `ready`: the daemon is serving. The launcher reads nothing after it.
//! - `progress`, or `progress ` followed by a detail of at most [`MAX_DETAIL`]
//!   printable ASCII characters (space to `~`): the daemon is still starting.
//!
//! Anything else, including a line that grows past the limit, ends the channel
//! as malformed, and nothing after it is read. The channel ending without
//! `ready`, with or without part of a message, means the daemon died. A later
//! version of this protocol will use a new environment variable, so a daemon
//! never sends a launcher a message it does not know.
//!
//! # Passing the channel
//!
//! [`ENV`] tells the daemon where its end is.
//!
//! - On Unix it is the number of the write end of a pipe the launcher
//!   creates, the only descriptor above stderr that survives the exec. The
//!   daemon adopts that descriptor, which is why `Notifier::from_env` is
//!   `unsafe`, as adopting descriptors passed by `LISTEN_FDS` is.
//! - On Windows it is the name of a named pipe the launcher creates: 128
//!   random bits, one inbound instance, a DACL that admits only the current
//!   user, remote clients refused. The daemon opens it by name, so it
//!   inherits no handle for it, and neither can any other child the launcher
//!   starts. The launcher watches the daemon's process as well as the pipe,
//!   so a daemon that exits before it connects is reported at once.
//!
//! Taking the channel removes [`ENV`] from the daemon's environment, and its
//! end is not inherited by programs the daemon starts.

// Without `launch` nothing in the crate reads a channel; the daemon's side is
// still used.
#![cfg_attr(not(feature = "launch"), allow(dead_code))]

use std::fmt;
use std::io::{self, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

#[cfg(feature = "async")]
use crate::selection::AsyncEvidence;
use crate::selection::Evidence;

/// The environment variable that names the daemon's end of the channel.
pub const ENV: &str = "KUNOBI_DAEMON_READY";

/// Longest message in bytes, including its newline.
pub const MAX_MESSAGE: usize = 256;

/// Longest progress detail in bytes.
pub const MAX_DETAIL: usize = MAX_MESSAGE - "progress \n".len();

/// Why waiting on a channel ended without `ready`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NotReady {
    /// The channel ended before `ready`: the daemon exited, or closed its end
    /// without saying it was ready. The process that was started will not
    /// serve; another one may, which only a probe can tell.
    Died,
    /// The daemon sent something that is not a message of this protocol.
    /// Nothing after it is read.
    Malformed,
    /// No message arrived within the silence the caller allowed.
    Silent,
}

impl fmt::Display for NotReady {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Died => "the daemon's readiness channel ended before it was ready",
            Self::Malformed => "the daemon sent a malformed readiness message",
            Self::Silent => "the daemon's readiness channel stayed silent too long",
        })
    }
}

impl std::error::Error for NotReady {}

// ── Wire format ─────────────────────────────────────────────────────

/// One message from the daemon.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Message {
    Ready,
    Progress(Option<String>),
}

/// Space to `~`: no control characters, no escape sequences, no newline.
fn printable(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| (b' '..=b'~').contains(byte))
}

/// The message a complete line (without its newline) holds, if any.
fn parse(line: &[u8]) -> Option<Message> {
    match line {
        b"ready" => Some(Message::Ready),
        b"progress" => Some(Message::Progress(None)),
        _ => {
            let detail = line.strip_prefix(b"progress ")?;
            if detail.is_empty() || !printable(detail) {
                return None;
            }
            String::from_utf8(detail.to_vec())
                .ok()
                .map(|detail| Message::Progress(Some(detail)))
        }
    }
}

/// The line for a progress message; an empty detail sends bare `progress`.
fn progress_line(detail: &str) -> io::Result<Vec<u8>> {
    if detail.len() > MAX_DETAIL || !printable(detail.as_bytes()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "a progress detail is printable ASCII of at most MAX_DETAIL bytes",
        ));
    }
    let mut line = Vec::with_capacity(MAX_MESSAGE);
    line.extend_from_slice(b"progress");
    if !detail.is_empty() {
        line.push(b' ');
        line.extend_from_slice(detail.as_bytes());
    }
    line.push(b'\n');
    Ok(line)
}

/// What one more byte of the stream completed.
#[derive(Debug, PartialEq, Eq)]
enum Step {
    More,
    Message(Message),
    Malformed,
}

/// Splits the stream into messages, one byte at a time, so how the reads
/// happen to split it never matters.
#[derive(Debug, Default)]
struct Decoder {
    line: Vec<u8>,
}

impl Decoder {
    fn push(&mut self, byte: u8) -> Step {
        if byte == b'\n' {
            let message = parse(&self.line);
            self.line.clear();
            return message.map_or(Step::Malformed, Step::Message);
        }
        // No room left for this byte and the newline: fail now rather than
        // buffer a line that can never be valid.
        if self.line.len() >= MAX_MESSAGE - 1 {
            return Step::Malformed;
        }
        self.line.push(byte);
        Step::More
    }
}

// ── State machine ───────────────────────────────────────────────────

/// What the daemon has said so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum State {
    #[default]
    Waiting,
    Progressing,
    Ready,
    Died,
    Malformed,
}

/// One thing the launcher learned from the channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(kani, derive(kani::Arbitrary))]
enum Event {
    Progress,
    Ready,
    Malformed,
    /// The channel ended: every write end is closed, or reading failed.
    Closed,
}

/// What a waiter does now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Decision {
    Ready,
    NotReady(NotReady),
    /// Nothing to report for this long.
    Wait(Duration),
}

/// The channel's state and the time of its last message, kept apart from any
/// I/O so it can be model-checked. Times are measured from the channel's
/// opening.
#[derive(Clone, Copy, Debug, Default)]
struct Tracker {
    state: State,
    last_message: Duration,
}

impl Tracker {
    /// Record `event`, received at `at`. Ready, died and malformed are final.
    fn record(&mut self, at: Duration, event: Event) {
        if matches!(self.state, State::Ready | State::Died | State::Malformed) {
            return;
        }
        self.last_message = self.last_message.max(at);
        self.state = match event {
            Event::Progress => State::Progressing,
            Event::Ready => State::Ready,
            Event::Malformed => State::Malformed,
            Event::Closed => State::Died,
        };
    }

    /// Decide at `now` for a waiter that accepts `silence` between messages.
    fn decide(&self, now: Duration, silence: Duration) -> Decision {
        match self.state {
            State::Ready => Decision::Ready,
            State::Died => Decision::NotReady(NotReady::Died),
            State::Malformed => Decision::NotReady(NotReady::Malformed),
            State::Waiting | State::Progressing => {
                let until = self.last_message.saturating_add(silence);
                if now >= until {
                    Decision::NotReady(NotReady::Silent)
                } else {
                    Decision::Wait(until - now)
                }
            }
        }
    }
}

// ── The launcher's end ──────────────────────────────────────────────

#[derive(Debug, Default)]
struct Inner {
    tracker: Tracker,
    /// Counts every event, so a waiter can tell news from what it has seen.
    version: u64,
    progress: u64,
    last_progress: Option<String>,
}

#[derive(Debug)]
struct Shared {
    opened: Instant,
    inner: Mutex<Inner>,
    changed: Condvar,
    #[cfg(feature = "async")]
    notify: tokio::sync::Notify,
    /// Set when the launcher drops its [`Channel`]: the reader stops at its
    /// next read and closes the launcher's end.
    closed: AtomicBool,
}

impl Shared {
    fn new() -> Self {
        Self {
            opened: Instant::now(),
            inner: Mutex::default(),
            changed: Condvar::new(),
            #[cfg(feature = "async")]
            notify: tokio::sync::Notify::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn post(&self, event: Event, detail: Option<String>) {
        let at = self.opened.elapsed();
        {
            let mut inner = self.lock();
            inner.tracker.record(at, event);
            inner.version += 1;
            if event == Event::Progress {
                inner.progress += 1;
                inner.last_progress = detail;
            }
        }
        self.changed.notify_all();
        #[cfg(feature = "async")]
        self.notify.notify_one();
    }
}

/// Whether a failed read ends the channel. An interrupted read is retried.
fn ends_channel(error: &io::Error) -> bool {
    error.kind() != io::ErrorKind::Interrupted
}

/// Read messages until `ready`, the end of the channel, a malformed message,
/// or the launcher dropping its [`Channel`].
fn read_messages(mut reader: impl Read, shared: &Shared) {
    let mut decoder = Decoder::default();
    let mut buffer = [0u8; MAX_MESSAGE];
    loop {
        let read = reader.read(&mut buffer);
        if shared.closed.load(Ordering::Acquire) {
            return;
        }
        let count = match read {
            Ok(0) => return shared.post(Event::Closed, None),
            Ok(count) => count,
            Err(error) => {
                if ends_channel(&error) {
                    return shared.post(Event::Closed, None);
                }
                continue;
            }
        };
        for &byte in &buffer[..count] {
            match decoder.push(byte) {
                Step::More => {}
                Step::Message(Message::Progress(detail)) => shared.post(Event::Progress, detail),
                Step::Message(Message::Ready) => return shared.post(Event::Ready, None),
                Step::Malformed => return shared.post(Event::Malformed, None),
            }
        }
    }
}

/// The launcher's end of a daemon's readiness channel.
///
/// A thread reads the channel from the moment the daemon starts, so the
/// silence bound runs from then even before anyone waits. It stops at
/// `ready`, at the end of the channel, at a malformed message, or at the
/// first message after this handle is dropped; the daemon's next send then
/// fails instead of blocking.
#[derive(Debug)]
pub struct Channel {
    shared: Arc<Shared>,
}

impl Channel {
    /// Start reading the launcher's end of a channel.
    pub(crate) fn start(reader: impl Read + Send + 'static) -> io::Result<Self> {
        let shared = Arc::new(Shared::new());
        let reading = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("kunobi-daemon-readiness".into())
            .spawn(move || read_messages(reader, &reading))?;
        Ok(Self { shared })
    }

    /// Block until the daemon says it is ready, its channel ends, or
    /// `silence` passes without a message.
    ///
    /// `silence` is the longest wait between two messages, counted from the
    /// daemon's start for the first one. A daemon that keeps reporting
    /// progress is waited for however long it takes in total.
    ///
    /// `Ok` means the daemon said it was ready, not that it is: probe it
    /// before relying on it. Once the answer is ready, died or malformed,
    /// every later call returns it at once. After [`NotReady::Silent`] a later
    /// call can still see the daemon's next message.
    pub fn wait(&mut self, silence: Duration) -> Result<(), NotReady> {
        let mut inner = self.shared.lock();
        loop {
            let now = self.shared.opened.elapsed();
            match inner.tracker.decide(now, silence) {
                Decision::Ready => return Ok(()),
                Decision::NotReady(reason) => return Err(reason),
                Decision::Wait(remaining) => {
                    inner = self
                        .shared
                        .changed
                        .wait_timeout(inner, remaining)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0;
                }
            }
        }
    }

    /// Async counterpart of [`Channel::wait`], on a Tokio runtime with its
    /// timer enabled. Cancelling it loses nothing: the channel is still read.
    #[cfg(feature = "async")]
    pub async fn wait_async(&mut self, silence: Duration) -> Result<(), NotReady> {
        loop {
            let notified = self.shared.notify.notified();
            let now = self.shared.opened.elapsed();
            let decision = self.shared.lock().tracker.decide(now, silence);
            match decision {
                Decision::Ready => return Ok(()),
                Decision::NotReady(reason) => return Err(reason),
                Decision::Wait(remaining) => {
                    let _ = tokio::time::timeout(remaining, notified).await;
                }
            }
        }
    }

    /// How many progress messages the daemon has sent so far.
    pub fn progress_reports(&self) -> u64 {
        self.shared.lock().progress
    }

    /// The detail of the daemon's latest progress message, or `None` if it
    /// had none or nothing was reported yet. Useful in an error message when
    /// the daemon does not become ready.
    pub fn last_progress(&self) -> Option<String> {
        self.shared.lock().last_progress.clone()
    }

    fn version(&self) -> u64 {
        self.shared.lock().version
    }

    fn is_ready(&self) -> bool {
        self.shared.lock().tracker.state == State::Ready
    }

    /// Why waiting is over, if it is and not because the daemon is ready.
    fn failure(&self, silence: Duration) -> Option<NotReady> {
        let now = self.shared.opened.elapsed();
        match self.shared.lock().tracker.decide(now, silence) {
            Decision::NotReady(reason) => Some(reason),
            Decision::Ready | Decision::Wait(_) => None,
        }
    }

    /// Block until the channel has news since version `since`, the silence
    /// bound passes, or `until`.
    fn wait_for_news(&self, since: u64, until: Instant, silence: Duration) {
        let mut inner = self.shared.lock();
        loop {
            let now = self.shared.opened.elapsed();
            let Some(quiet) = quiet_for(&inner, since, now, silence) else {
                return;
            };
            let remaining = quiet.min(until.saturating_duration_since(Instant::now()));
            if remaining.is_zero() {
                return;
            }
            inner = self
                .shared
                .changed
                .wait_timeout(inner, remaining)
                .unwrap_or_else(PoisonError::into_inner)
                .0;
        }
    }

    /// Async counterpart of [`Channel::wait_for_news`].
    #[cfg(feature = "async")]
    async fn wait_for_news_async(
        &self,
        since: u64,
        until: tokio::time::Instant,
        silence: Duration,
    ) {
        loop {
            let notified = self.shared.notify.notified();
            let now = self.shared.opened.elapsed();
            let quiet = quiet_for(&self.shared.lock(), since, now, silence);
            let Some(quiet) = quiet else {
                return;
            };
            let remaining = quiet.min(until.saturating_duration_since(tokio::time::Instant::now()));
            if remaining.is_zero() {
                return;
            }
            if tokio::time::timeout(remaining, notified).await.is_err() {
                return;
            }
        }
    }
}

impl Drop for Channel {
    fn drop(&mut self) {
        self.shared.closed.store(true, Ordering::Release);
    }
}

/// How long nothing is worth a new round: `None` once there is news since
/// `since`, an answer, or silence past the bound.
fn quiet_for(inner: &Inner, since: u64, now: Duration, silence: Duration) -> Option<Duration> {
    if inner.version != since {
        return None;
    }
    match inner.tracker.decide(now, silence) {
        Decision::Wait(remaining) => Some(remaining),
        Decision::Ready | Decision::NotReady(_) => None,
    }
}

// ── The daemon's end ────────────────────────────────────────────────

/// The daemon's end of the readiness channel its launcher asked for.
///
/// Report progress while starting, then call [`Notifier::ready`] once the
/// daemon serves. Dropping it without `ready` tells the launcher the daemon
/// will not become ready, as exiting does.
///
/// A failed send means the launcher stopped listening, usually because it
/// gave up or exited. The daemon should carry on: whether it serves is not
/// the launcher's decision.
#[derive(Debug)]
pub struct Notifier {
    pipe: std::io::PipeWriter,
}

impl Notifier {
    #[cfg(feature = "local")]
    pub(crate) fn from_pipe(pipe: std::io::PipeWriter) -> Self {
        Self { pipe }
    }

    /// Report progress, which restarts the launcher's silence bound.
    ///
    /// `detail` is shown to whoever waits, for example in an error message;
    /// pass `""` for none. It must be printable ASCII (space to `~`) of at
    /// most [`MAX_DETAIL`] bytes, or this fails with
    /// [`io::ErrorKind::InvalidInput`] and sends nothing.
    pub fn progress(&mut self, detail: &str) -> io::Result<()> {
        let line = progress_line(detail)?;
        self.pipe.write_all(&line)
    }

    /// Say the daemon is serving, and close the channel.
    ///
    /// Send it once the daemon answers on its endpoint: the launcher probes
    /// it next.
    pub fn ready(mut self) -> io::Result<()> {
        self.pipe.write_all(b"ready\n")
    }
}

// ── Selection ───────────────────────────────────────────────────────

/// Selection evidence woken by a daemon's readiness channel.
///
/// Wraps the evidence a caller already has and changes only when a round is
/// worth making, never what it decides:
///
/// - Before the daemon says it is ready, the wait between rounds lasts until
///   the channel has news (a message, or its end) or the silence bound
///   passes, instead of a poll interval.
/// - After `ready`, the wrapped evidence's own wait paces the rounds.
/// - A probe still decides. When one does not prove the daemon and the
///   channel has died, turned malformed or stayed silent past the bound, the
///   probe fails with [`SignaledError::NotReady`], which ends the wait at once
///   instead of at the budget.
///
/// A fresh proof wins over a dead channel: when another instance won the
/// endpoint and the daemon this launcher started exited, a probe that reaches
/// the winner still reports it current.
///
/// The selection's commit budget still bounds the total wait. With a channel
/// it can be generous; the silence bound catches a daemon that stopped
/// reporting.
#[derive(Debug)]
pub struct Signaled<E> {
    evidence: E,
    channel: Channel,
    silence: Duration,
    /// The channel's version when the current round began.
    seen: u64,
}

impl<E> Signaled<E> {
    /// Wrap `evidence` with the daemon's `channel`, allowing at most
    /// `silence` between the daemon's messages.
    pub fn new(evidence: E, channel: Channel, silence: Duration) -> Self {
        Self {
            evidence,
            channel,
            silence,
            seen: 0,
        }
    }

    /// The wrapped evidence and the channel.
    pub fn into_parts(self) -> (E, Channel) {
        (self.evidence, self.channel)
    }
}

/// Why a [`Signaled`] wait failed.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SignaledError<E> {
    /// The wrapped evidence failed.
    Evidence(E),
    /// No fresh probe proved the daemon, and its channel says it will not
    /// become ready.
    NotReady(NotReady),
}

impl<E: fmt::Display> fmt::Display for SignaledError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => fmt::Display::fmt(error, f),
            Self::NotReady(reason) => fmt::Display::fmt(reason, f),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for SignaledError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Evidence(error) => Some(error),
            Self::NotReady(reason) => Some(reason),
        }
    }
}

impl<E: Evidence> Evidence for Signaled<E> {
    type Proof = E::Proof;
    type Error = SignaledError<E::Error>;

    fn committed(&mut self) -> Result<bool, Self::Error> {
        self.evidence.committed().map_err(SignaledError::Evidence)
    }

    fn probe(&mut self, deadline: Instant) -> Result<Option<Self::Proof>, Self::Error> {
        // News from here on wakes the wait after this round.
        self.seen = self.channel.version();
        let proof = self
            .evidence
            .probe(deadline)
            .map_err(SignaledError::Evidence)?;
        if proof.is_some() {
            return Ok(proof);
        }
        match self.channel.failure(self.silence) {
            Some(reason) => Err(SignaledError::NotReady(reason)),
            None => Ok(None),
        }
    }

    fn wait(&mut self, until: Instant) {
        // News during the round, such as a `ready` that arrived while an
        // unproven probe ran, is worth a new round at once.
        if self.channel.version() != self.seen {
            return;
        }
        if self.channel.is_ready() {
            self.evidence.wait(until);
        } else {
            self.channel.wait_for_news(self.seen, until, self.silence);
        }
    }
}

#[cfg(feature = "async")]
impl<E> AsyncEvidence for Signaled<E>
where
    E: AsyncEvidence + Send,
    E::Proof: Send,
    E::Error: Send,
{
    type Proof = E::Proof;
    type Error = SignaledError<E::Error>;

    async fn committed(&mut self) -> Result<bool, Self::Error> {
        self.evidence
            .committed()
            .await
            .map_err(SignaledError::Evidence)
    }

    async fn probe(&mut self) -> Result<Option<Self::Proof>, Self::Error> {
        self.seen = self.channel.version();
        let proof = self
            .evidence
            .probe()
            .await
            .map_err(SignaledError::Evidence)?;
        if proof.is_some() {
            return Ok(proof);
        }
        match self.channel.failure(self.silence) {
            Some(reason) => Err(SignaledError::NotReady(reason)),
            None => Ok(None),
        }
    }

    async fn wait(&mut self, until: tokio::time::Instant) {
        if self.channel.version() != self.seen {
            return;
        }
        if self.channel.is_ready() {
            self.evidence.wait(until).await;
        } else {
            self.channel
                .wait_for_news_async(self.seen, until, self.silence)
                .await;
        }
    }
}

/// The channel's states for every sequence of messages and silences, not
/// only the ones a test was written for.
#[cfg(kani)]
mod proofs {
    use super::*;

    /// Steps explored. Durations are small so every ordering of messages,
    /// silence bound and waiting time is reachable within them.
    const STEPS: usize = 5;

    fn small() -> Duration {
        Duration::from_millis(u64::from(kani::any::<u8>() % 8))
    }

    /// Waiting, progressing, ready, died and malformed: the launcher's answer
    /// follows the first final message, never anything after it, and until
    /// then only the silence since the last message can end the wait.
    #[kani::proof]
    #[kani::unwind(6)]
    fn a_launcher_learns_only_what_its_daemon_said() {
        let silence = small();
        let mut tracker = Tracker::default();
        let mut now = Duration::ZERO;
        let mut last_message = Duration::ZERO;
        let mut progressed = false;
        let mut end: Option<Event> = None;
        let mut settled: Option<Decision> = None;

        for _ in 0..STEPS {
            // Time never runs backwards; a message may or may not arrive.
            now += small();
            if kani::any() {
                let event: Event = kani::any();
                tracker.record(now, event);
                if end.is_none() {
                    last_message = now;
                    if event == Event::Progress {
                        progressed = true;
                    } else {
                        end = Some(event);
                    }
                }
            }

            let state = match end {
                Some(Event::Ready) => State::Ready,
                Some(Event::Closed) => State::Died,
                Some(Event::Malformed) => State::Malformed,
                Some(Event::Progress) | None if progressed => State::Progressing,
                Some(Event::Progress) | None => State::Waiting,
            };
            assert!(tracker.state == state);

            let decision = tracker.decide(now, silence);
            match end {
                Some(Event::Ready) => assert!(decision == Decision::Ready),
                // The end of the channel before ready fails at once,
                // whatever the silence bound.
                Some(Event::Closed) => assert!(decision == Decision::NotReady(NotReady::Died)),
                Some(Event::Malformed) => {
                    assert!(decision == Decision::NotReady(NotReady::Malformed));
                }
                // Every message restarts the silence bound.
                Some(Event::Progress) | None => {
                    let until = last_message + silence;
                    if now >= until {
                        assert!(decision == Decision::NotReady(NotReady::Silent));
                    } else {
                        assert!(decision == Decision::Wait(until - now));
                    }
                }
            }

            // A final answer never changes.
            if let Some(earlier) = settled {
                assert!(decision == earlier);
            }
            if matches!(
                decision,
                Decision::Ready | Decision::NotReady(NotReady::Died | NotReady::Malformed)
            ) {
                settled = Some(decision);
            }
        }
    }
}

/// The promises of the Kani harness over long runs, and the wire format over
/// generated streams.
#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    fn config() -> ProptestConfig {
        ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        }
    }

    fn event() -> impl Strategy<Value = Event> {
        prop_oneof![
            6 => Just(Event::Progress),
            1 => Just(Event::Ready),
            1 => Just(Event::Malformed),
            1 => Just(Event::Closed),
        ]
    }

    fn step() -> impl Strategy<Value = Duration> {
        (0u64..5_000_000).prop_map(Duration::from_micros)
    }

    /// Pieces of a stream: messages, near misses and noise.
    fn piece() -> impl Strategy<Value = Vec<u8>> {
        prop_oneof![
            3 => Just(b"ready\n".to_vec()),
            3 => Just(b"progress\n".to_vec()),
            3 => "progress [ -~]{1,40}\n".prop_map(String::into_bytes),
            1 => "progress [ -~]{240,250}\n".prop_map(String::into_bytes),
            1 => "[ -~]{0,300}".prop_map(String::into_bytes),
            1 => proptest::collection::vec(any::<u8>(), 0..8),
            1 => Just(b"\n".to_vec()),
        ]
    }

    /// Whether `line`, with its newline, is exactly what a daemon sends.
    fn sent_by_a_daemon(line: &[u8]) -> bool {
        if line == b"ready\n" {
            return true;
        }
        let Some(detail) = line
            .strip_prefix(b"progress")
            .and_then(|rest| rest.strip_suffix(b"\n"))
        else {
            return false;
        };
        let detail = match detail.strip_prefix(b" ") {
            Some(detail) if !detail.is_empty() => detail,
            Some(_) => return false,
            None if detail.is_empty() => detail,
            None => return false,
        };
        std::str::from_utf8(detail)
            .ok()
            .and_then(|detail| progress_line(detail).ok())
            .is_some_and(|encoded| encoded == line)
    }

    proptest! {
        #![proptest_config(config())]

        #[test]
        fn a_launcher_learns_only_what_its_daemon_said(
            silence in step(),
            steps in proptest::collection::vec((step(), proptest::option::of(event())), 1..64),
        ) {
            let mut tracker = Tracker::default();
            let mut now = Duration::ZERO;
            let mut last_message = Duration::ZERO;
            let mut end: Option<Event> = None;
            for (elapsed, event) in steps {
                now += elapsed;
                if let Some(event) = event {
                    tracker.record(now, event);
                    if end.is_none() {
                        last_message = now;
                        if event != Event::Progress {
                            end = Some(event);
                        }
                    }
                }
                let decision = tracker.decide(now, silence);
                let expected = match end {
                    Some(Event::Ready) => Decision::Ready,
                    Some(Event::Closed) => Decision::NotReady(NotReady::Died),
                    Some(Event::Malformed) => Decision::NotReady(NotReady::Malformed),
                    Some(Event::Progress) | None if now >= last_message + silence => {
                        Decision::NotReady(NotReady::Silent)
                    }
                    Some(Event::Progress) | None => Decision::Wait(last_message + silence - now),
                };
                prop_assert_eq!(decision, expected);
            }
        }

        #[test]
        fn every_message_decoded_is_exactly_what_a_daemon_sent(
            pieces in proptest::collection::vec(piece(), 0..24),
        ) {
            let mut decoder = Decoder::default();
            let mut line = Vec::new();
            for byte in pieces.concat() {
                line.push(byte);
                match decoder.push(byte) {
                    Step::More => prop_assert!(line.len() < MAX_MESSAGE),
                    Step::Message(_) => {
                        prop_assert!(sent_by_a_daemon(&line), "{}", line.escape_ascii());
                        line.clear();
                    }
                    Step::Malformed => {
                        prop_assert!(
                            line.len() == MAX_MESSAGE || !sent_by_a_daemon(&line),
                            "{}",
                            line.escape_ascii()
                        );
                        break;
                    }
                }
            }
        }

        #[test]
        fn whatever_a_daemon_sends_is_decoded_as_sent(
            details in proptest::collection::vec(
                prop_oneof![Just(String::new()), "[ -~]{1,246}"],
                0..8,
            ),
        ) {
            let mut stream = Vec::new();
            for detail in &details {
                stream.extend(progress_line(detail).unwrap());
            }
            stream.extend_from_slice(b"ready\n");
            let mut decoder = Decoder::default();
            let messages: Vec<Message> = stream
                .into_iter()
                .filter_map(|byte| match decoder.push(byte) {
                    Step::Message(message) => Some(message),
                    Step::More | Step::Malformed => None,
                })
                .collect();
            let mut expected: Vec<Message> = details
                .into_iter()
                .map(|detail| Message::Progress((!detail.is_empty()).then_some(detail)))
                .collect();
            expected.push(Message::Ready);
            prop_assert_eq!(messages, expected);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::{Budget, Selection, await_selection};
    use std::io::PipeWriter;

    /// Long enough never to pass in a test that expects an answer first.
    const LONG: Duration = Duration::from_secs(20);

    fn open() -> (Channel, PipeWriter) {
        let (reader, writer) = std::io::pipe().unwrap();
        (Channel::start(reader).unwrap(), writer)
    }

    fn ms(millis: u64) -> Duration {
        Duration::from_millis(millis)
    }

    // ── State machine ───────────────────────────────────────────────

    #[test]
    fn signaled_errors_preserve_the_failed_probe_or_channel() {
        let probe = SignaledError::Evidence(io::Error::other("unreadable record"));
        assert_eq!(
            std::error::Error::source(&probe).unwrap().to_string(),
            "unreadable record"
        );
        let channel: SignaledError<io::Error> = SignaledError::NotReady(NotReady::Died);
        assert_eq!(
            std::error::Error::source(&channel).unwrap().to_string(),
            NotReady::Died.to_string()
        );
    }

    #[test]
    fn progress_restarts_the_silence_bound_so_a_slow_start_is_not_cut_off() {
        // A total deadline of one second would fail this daemon at 1 s; it
        // reports progress every 0.9 s and is ready at 3.6 s.
        let silence = ms(1000);
        let mut tracker = Tracker::default();
        for step in 1..=3 {
            let at = ms(900 * step);
            tracker.record(at, Event::Progress);
            assert_eq!(tracker.decide(at, silence), Decision::Wait(silence));
            assert_eq!(
                tracker.decide(at + ms(899), silence),
                Decision::Wait(ms(101))
            );
        }
        tracker.record(ms(3600), Event::Ready);
        assert_eq!(tracker.decide(ms(3600), silence), Decision::Ready);
        assert_eq!(tracker.decide(ms(100_000), silence), Decision::Ready);
    }

    #[test]
    fn silence_ends_the_wait_exactly_at_the_bound() {
        let silence = ms(1000);
        let mut tracker = Tracker::default();
        assert_eq!(tracker.state, State::Waiting);
        assert_eq!(tracker.decide(ms(300), silence), Decision::Wait(ms(700)));
        assert_eq!(
            tracker.decide(silence, silence),
            Decision::NotReady(NotReady::Silent)
        );
        tracker.record(ms(2000), Event::Progress);
        assert_eq!(tracker.state, State::Progressing);
        assert_eq!(tracker.decide(ms(2600), silence), Decision::Wait(ms(400)));
        assert_eq!(
            tracker.decide(ms(3000), silence),
            Decision::NotReady(NotReady::Silent)
        );
        // An unbounded silence never overflows.
        assert_eq!(
            tracker.decide(ms(3000), Duration::MAX),
            Decision::Wait(Duration::MAX - ms(3000))
        );
    }

    #[test]
    fn the_first_final_message_decides_for_good() {
        let mut tracker = Tracker::default();
        tracker.record(ms(1000), Event::Closed);
        tracker.record(ms(2000), Event::Ready);
        assert_eq!(tracker.state, State::Died);
        assert_eq!(
            tracker.decide(Duration::ZERO, Duration::ZERO),
            Decision::NotReady(NotReady::Died)
        );

        let mut tracker = Tracker::default();
        tracker.record(ms(1000), Event::Malformed);
        tracker.record(ms(2000), Event::Ready);
        assert_eq!(
            tracker.decide(ms(3000), LONG),
            Decision::NotReady(NotReady::Malformed)
        );

        let mut tracker = Tracker::default();
        tracker.record(ms(1000), Event::Ready);
        tracker.record(ms(2000), Event::Closed);
        assert_eq!(tracker.decide(ms(3000), Duration::ZERO), Decision::Ready);
    }

    // ── Wire format ─────────────────────────────────────────────────

    #[test]
    fn messages_parse_strictly() {
        assert_eq!(parse(b"ready"), Some(Message::Ready));
        assert_eq!(parse(b"progress"), Some(Message::Progress(None)));
        assert_eq!(
            parse(b"progress opening the cache"),
            Some(Message::Progress(Some("opening the cache".into())))
        );
        assert_eq!(
            parse(b"progress  ~"),
            Some(Message::Progress(Some(" ~".into())))
        );
        let refused: [&[u8]; 11] = [
            b"",
            b"READY",
            b"ready ",
            b"ready\r",
            b"progress ",
            b"progressing",
            b"progress\tx",
            b"progress caf\xc3\xa9",
            b"progress x\x7f",
            b"progress x\x1b[2J",
            b"hello",
        ];
        for line in refused {
            assert_eq!(parse(line), None, "{}", line.escape_ascii());
        }
    }

    #[test]
    fn a_line_may_fill_the_limit_but_not_pass_it() {
        let longest = progress_line(&"x".repeat(MAX_DETAIL)).unwrap();
        assert_eq!(longest.len(), MAX_MESSAGE);
        let mut decoder = Decoder::default();
        let steps: Vec<Step> = longest.iter().map(|&byte| decoder.push(byte)).collect();
        assert_eq!(
            steps.last(),
            Some(&Step::Message(Message::Progress(Some(
                "x".repeat(MAX_DETAIL)
            ))))
        );

        let mut decoder = Decoder::default();
        for _ in 0..MAX_MESSAGE - 1 {
            assert_eq!(decoder.push(b'x'), Step::More);
        }
        assert_eq!(decoder.push(b'x'), Step::Malformed);
    }

    #[test]
    fn a_progress_detail_is_checked_before_anything_is_sent() {
        assert_eq!(progress_line("").unwrap(), b"progress\n");
        assert_eq!(progress_line("loading").unwrap(), b"progress loading\n");
        assert!(progress_line(&"x".repeat(MAX_DETAIL)).is_ok());
        for detail in [
            "x".repeat(MAX_DETAIL + 1),
            "two\nlines".into(),
            "café".into(),
            "\u{1b}[2J".into(),
        ] {
            let error = progress_line(&detail).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{detail:?}");
        }
    }

    #[test]
    fn only_an_interrupted_read_is_retried() {
        assert!(!ends_channel(&io::ErrorKind::Interrupted.into()));
        assert!(ends_channel(&io::ErrorKind::BrokenPipe.into()));
        assert!(ends_channel(&io::Error::other("read failed")));
    }

    #[test]
    fn errors_say_what_happened() {
        assert!(NotReady::Died.to_string().contains("ended before"));
        assert!(NotReady::Malformed.to_string().contains("malformed"));
        assert!(NotReady::Silent.to_string().contains("silent"));
        let error: SignaledError<NotReady> = SignaledError::NotReady(NotReady::Died);
        assert_eq!(error.to_string(), NotReady::Died.to_string());
        let error = SignaledError::Evidence(io::Error::other("record unreadable"));
        assert_eq!(error.to_string(), "record unreadable");
    }

    // ── A channel over a real pipe ──────────────────────────────────

    #[test]
    fn progress_then_ready() {
        let (mut channel, mut daemon) = open();
        daemon
            .write_all(b"progress\nprogress opening the cache\nready\n")
            .unwrap();
        assert_eq!(channel.wait(LONG), Ok(()));
        assert_eq!(channel.progress_reports(), 2);
        assert_eq!(
            channel.last_progress().as_deref(),
            Some("opening the cache")
        );
        // The answer stays, whatever the silence bound.
        assert_eq!(channel.wait(Duration::ZERO), Ok(()));
    }

    #[test]
    fn a_notifier_speaks_the_protocol() {
        let (reader, writer) = std::io::pipe().unwrap();
        let mut channel = Channel::start(reader).unwrap();
        let mut notifier = Notifier { pipe: writer };
        notifier.progress("").unwrap();
        notifier.progress("loading").unwrap();
        assert_eq!(
            notifier.progress("bad\ndetail").unwrap_err().kind(),
            io::ErrorKind::InvalidInput
        );
        notifier.ready().unwrap();
        assert_eq!(channel.wait(LONG), Ok(()));
        assert_eq!(channel.progress_reports(), 2);
        assert_eq!(channel.last_progress().as_deref(), Some("loading"));
    }

    #[test]
    fn the_end_of_the_channel_before_ready_fails_at_once() {
        let (mut channel, mut daemon) = open();
        daemon.write_all(b"progress\nrea").unwrap();
        drop(daemon);
        assert_eq!(channel.wait(LONG), Err(NotReady::Died));
        assert_eq!(channel.progress_reports(), 1);

        let (reader, writer) = std::io::pipe().unwrap();
        let mut channel = Channel::start(reader).unwrap();
        drop(Notifier { pipe: writer });
        assert_eq!(channel.wait(LONG), Err(NotReady::Died));
    }

    #[test]
    fn a_malformed_message_fails_closed() {
        let lines: [&[u8]; 6] = [
            b"READY\n",
            b"ready \n",
            b"ready\r\n",
            b"progress \n",
            b"\n",
            b"progress ok\nnonsense\nready\n",
        ];
        for line in lines {
            let (mut channel, mut daemon) = open();
            daemon.write_all(line).unwrap();
            assert_eq!(
                channel.wait(LONG),
                Err(NotReady::Malformed),
                "{}",
                line.escape_ascii()
            );
        }
    }

    #[test]
    fn an_overlong_line_fails_without_waiting_for_its_end() {
        let (mut channel, mut daemon) = open();
        daemon.write_all(&[b'x'; MAX_MESSAGE]).unwrap();
        assert_eq!(channel.wait(LONG), Err(NotReady::Malformed));
        drop(daemon);
    }

    #[test]
    fn nothing_after_ready_is_read() {
        let (mut channel, mut daemon) = open();
        daemon.write_all(b"ready\nnonsense\n").unwrap();
        drop(daemon);
        assert_eq!(channel.wait(LONG), Ok(()));
    }

    #[test]
    fn a_silent_wait_can_still_see_a_later_message() {
        let (mut channel, mut daemon) = open();
        assert_eq!(channel.wait(Duration::ZERO), Err(NotReady::Silent));
        daemon.write_all(b"ready\n").unwrap();
        assert_eq!(channel.wait(LONG), Ok(()));
    }

    #[test]
    fn dropping_the_channel_closes_the_launcher_s_end() {
        let (channel, mut daemon) = open();
        drop(channel);
        // The reader stops after the next message instead of reading on, so
        // a daemon's send fails rather than waiting on a full pipe.
        let error = loop {
            if let Err(error) = daemon.write_all(b"progress\n") {
                break error;
            }
        };
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    // ── Selection ───────────────────────────────────────────────────

    /// A probe that never proves the daemon, and must not be polled before
    /// the channel says the daemon is ready.
    struct Unproven {
        probes: usize,
    }

    impl Evidence for Unproven {
        type Proof = ();
        type Error = ();
        fn committed(&mut self) -> Result<bool, ()> {
            Ok(false)
        }
        fn probe(&mut self, _: Instant) -> Result<Option<()>, ()> {
            self.probes += 1;
            Ok(None)
        }
        fn wait(&mut self, _: Instant) {
            panic!("polled although the channel had nothing to say");
        }
    }

    const SELECTION: Budget = Budget {
        commit: Duration::from_secs(10),
        proof: Duration::ZERO,
    };

    #[test]
    fn a_dead_channel_ends_the_selection_at_once_unless_a_probe_proves_the_daemon() {
        let (channel, daemon) = open();
        drop(daemon);
        let mut signaled = Signaled::new(Unproven { probes: 0 }, channel, LONG);
        assert_eq!(
            await_selection(SELECTION, &mut signaled),
            Err(SignaledError::NotReady(NotReady::Died))
        );

        // Another instance serves: the proof wins over the dead channel.
        struct Proven;
        impl Evidence for Proven {
            type Proof = &'static str;
            type Error = ();
            fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            fn probe(&mut self, _: Instant) -> Result<Option<&'static str>, ()> {
                Ok(Some("winner"))
            }
        }
        let (channel, daemon) = open();
        drop(daemon);
        let mut signaled = Signaled::new(Proven, channel, LONG);
        assert_eq!(
            await_selection(SELECTION, &mut signaled),
            Ok(Selection::Current("winner"))
        );
    }

    #[test]
    fn silence_past_the_bound_ends_the_selection() {
        let (channel, _daemon) = open();
        let mut signaled = Signaled::new(Unproven { probes: 0 }, channel, Duration::ZERO);
        assert_eq!(
            await_selection(SELECTION, &mut signaled),
            Err(SignaledError::NotReady(NotReady::Silent))
        );
    }

    /// Probes made while waiting `budget` for a daemon that says nothing more.
    fn probes_while_quiet(channel: Channel) -> usize {
        let budget = Budget {
            commit: Duration::from_millis(300),
            proof: Duration::ZERO,
        };
        let mut signaled = Signaled::new(Unproven { probes: 0 }, channel, LONG);
        assert_eq!(
            await_selection(budget, &mut signaled),
            Ok(Selection::NotCommitted)
        );
        signaled.into_parts().0.probes
    }

    #[test]
    fn before_the_signal_a_round_waits_for_news_not_a_poll_interval() {
        // Nothing arrives, so after its first probe the wait lasts until the
        // commit budget: one more probe at most, however slow the machine is.
        // Polling would probe every 25 ms.
        let (channel, _daemon) = open();
        assert!(probes_while_quiet(channel) <= 2);

        // A message that arrived before the round began is not news to it.
        let (channel, mut daemon) = open();
        daemon.write_all(b"progress\n").unwrap();
        wait_for_version(&channel.shared, 1);
        assert!(probes_while_quiet(channel) <= 2);
    }

    #[test]
    fn a_quiet_channel_holds_the_wait_until_its_deadline() {
        let (channel, _daemon) = open();
        let until = Instant::now() + ms(50);
        channel.wait_for_news(channel.version(), until, LONG);
        assert!(Instant::now() >= until, "returned early without news");
    }

    #[test]
    fn news_is_whatever_arrived_since_the_round_began() {
        let (channel, mut daemon) = open();
        assert_eq!(channel.version(), 0);
        daemon.write_all(b"progress\n").unwrap();
        wait_for_version(&channel.shared, 1);
        assert_eq!(channel.version(), 1);

        let mut inner = Inner {
            version: 1,
            ..Inner::default()
        };
        inner.tracker.record(ms(100), Event::Progress);
        let silence = ms(1000);
        // Unseen news: a new round now.
        assert_eq!(quiet_for(&inner, 0, ms(300), silence), None);
        // Nothing new: quiet until the silence bound.
        assert_eq!(quiet_for(&inner, 1, ms(300), silence), Some(ms(800)));
        // The bound has passed: a new round now, which reports it.
        assert_eq!(quiet_for(&inner, 1, ms(1100), silence), None);
        inner.tracker.record(ms(200), Event::Ready);
        assert_eq!(quiet_for(&inner, 1, ms(300), silence), None);
    }

    #[test]
    fn errors_from_the_wrapped_evidence_end_the_wait() {
        struct Failing(bool);
        impl Evidence for Failing {
            type Proof = ();
            type Error = &'static str;
            fn committed(&mut self) -> Result<bool, Self::Error> {
                if self.0 {
                    Err("record unreadable")
                } else {
                    Ok(false)
                }
            }
            fn probe(&mut self, _: Instant) -> Result<Option<()>, Self::Error> {
                Err("peer is another user")
            }
        }
        for (record_fails, error) in [(true, "record unreadable"), (false, "peer is another user")]
        {
            let (channel, _daemon) = open();
            let mut signaled = Signaled::new(Failing(record_fails), channel, LONG);
            assert_eq!(
                await_selection(SELECTION, &mut signaled),
                Err(SignaledError::Evidence(error))
            );
        }
    }

    /// Block until `shared` has seen `version` events.
    fn wait_for_version(shared: &Shared, version: u64) {
        let mut inner = shared.lock();
        while inner.version < version {
            inner = shared
                .changed
                .wait(inner)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// The daemon sends `message` while the first probe runs, which fails
    /// anyway; the second probe proves it.
    struct NewsDuringProbe {
        daemon: PipeWriter,
        shared: Arc<Shared>,
        message: &'static [u8],
        probes: usize,
    }

    impl Evidence for NewsDuringProbe {
        type Proof = usize;
        type Error = ();
        fn committed(&mut self) -> Result<bool, ()> {
            Ok(false)
        }
        fn probe(&mut self, _: Instant) -> Result<Option<usize>, ()> {
            self.probes += 1;
            if self.probes == 1 {
                self.daemon.write_all(self.message).unwrap();
                wait_for_version(&self.shared, 1);
                return Ok(None);
            }
            Ok(Some(self.probes))
        }
        fn wait(&mut self, _: Instant) {
            panic!("waited although the channel had news");
        }
    }

    #[test]
    fn news_during_an_unproven_probe_starts_the_next_round_at_once() {
        // A `ready` that arrives while a probe fails is news too: waiting on
        // the evidence's own pace would delay the probe it makes worthwhile.
        let messages: [&'static [u8]; 2] = [b"progress\n", b"ready\n"];
        for message in messages {
            let (channel, daemon) = open();
            let evidence = NewsDuringProbe {
                daemon,
                shared: Arc::clone(&channel.shared),
                message,
                probes: 0,
            };
            let mut signaled = Signaled::new(evidence, channel, LONG);
            assert_eq!(
                await_selection(SELECTION, &mut signaled),
                Ok(Selection::Current(2)),
                "{}",
                message.escape_ascii()
            );
        }
    }

    #[test]
    fn after_ready_the_wrapped_evidence_paces_the_rounds() {
        struct Paced {
            probes: usize,
            waits: usize,
        }
        impl Evidence for Paced {
            type Proof = ();
            type Error = ();
            fn committed(&mut self) -> Result<bool, ()> {
                Ok(true)
            }
            fn probe(&mut self, _: Instant) -> Result<Option<()>, ()> {
                self.probes += 1;
                Ok((self.probes == 2).then_some(()))
            }
            fn wait(&mut self, _: Instant) {
                self.waits += 1;
            }
        }
        let (mut channel, mut daemon) = open();
        daemon.write_all(b"ready\n").unwrap();
        assert_eq!(channel.wait(LONG), Ok(()));
        let budget = Budget {
            commit: LONG,
            proof: LONG,
        };
        let mut signaled = Signaled::new(
            Paced {
                probes: 0,
                waits: 0,
            },
            channel,
            LONG,
        );
        // Ready is not a proof: the first probe fails and a second is made.
        assert_eq!(
            await_selection(budget, &mut signaled),
            Ok(Selection::Current(()))
        );
        let (evidence, _channel) = signaled.into_parts();
        assert_eq!(evidence.waits, 1);
    }

    #[test]
    fn a_ready_daemon_is_still_not_current_without_a_proof() {
        struct Never;
        impl Evidence for Never {
            type Proof = ();
            type Error = ();
            fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            fn probe(&mut self, _: Instant) -> Result<Option<()>, ()> {
                Ok(None)
            }
        }
        let (mut channel, mut daemon) = open();
        daemon.write_all(b"ready\n").unwrap();
        drop(daemon);
        assert_eq!(channel.wait(LONG), Ok(()));
        let budget = Budget {
            commit: Duration::from_millis(100),
            proof: Duration::ZERO,
        };
        let mut signaled = Signaled::new(Never, channel, LONG);
        assert_eq!(
            await_selection(budget, &mut signaled),
            Ok(Selection::NotCommitted)
        );
    }

    #[cfg(feature = "async")]
    mod asynchronous {
        use super::*;
        use crate::selection::await_selection_async;

        #[tokio::test]
        async fn progress_then_ready() {
            let (mut channel, mut daemon) = open();
            daemon.write_all(b"progress loading\nready\n").unwrap();
            assert_eq!(channel.wait_async(LONG).await, Ok(()));
            assert_eq!(channel.progress_reports(), 1);
            assert_eq!(channel.last_progress().as_deref(), Some("loading"));
        }

        #[tokio::test]
        async fn the_end_of_the_channel_before_ready_fails_at_once() {
            let (mut channel, daemon) = open();
            drop(daemon);
            assert_eq!(channel.wait_async(LONG).await, Err(NotReady::Died));
        }

        #[tokio::test]
        async fn silence_past_the_bound() {
            let (mut channel, _daemon) = open();
            assert_eq!(
                channel.wait_async(Duration::ZERO).await,
                Err(NotReady::Silent)
            );
        }

        struct AsyncNewsDuringProbe {
            daemon: PipeWriter,
            shared: Arc<Shared>,
            message: &'static [u8],
            probes: usize,
        }

        impl AsyncEvidence for AsyncNewsDuringProbe {
            type Proof = usize;
            type Error = ();
            async fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            async fn probe(&mut self) -> Result<Option<usize>, ()> {
                self.probes += 1;
                if self.probes == 1 {
                    self.daemon.write_all(self.message).unwrap();
                    wait_for_version(&self.shared, 1);
                    return Ok(None);
                }
                Ok(Some(self.probes))
            }
            async fn wait(&mut self, _: tokio::time::Instant) {
                panic!("waited although the channel had news");
            }
        }

        #[tokio::test]
        async fn news_during_an_unproven_probe_starts_the_next_round_at_once() {
            let messages: [&'static [u8]; 2] = [b"progress\n", b"ready\n"];
            for message in messages {
                let (channel, daemon) = open();
                let evidence = AsyncNewsDuringProbe {
                    daemon,
                    shared: Arc::clone(&channel.shared),
                    message,
                    probes: 0,
                };
                let mut signaled = Signaled::new(evidence, channel, LONG);
                assert_eq!(
                    await_selection_async(SELECTION, &mut signaled).await,
                    Ok(Selection::Current(2)),
                    "{}",
                    message.escape_ascii()
                );
            }
        }

        struct AsyncUnproven;

        impl AsyncEvidence for AsyncUnproven {
            type Proof = ();
            type Error = ();
            async fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            async fn probe(&mut self) -> Result<Option<()>, ()> {
                Ok(None)
            }
        }

        struct AsyncQuiet {
            probes: usize,
        }

        impl AsyncEvidence for AsyncQuiet {
            type Proof = ();
            type Error = ();
            async fn committed(&mut self) -> Result<bool, ()> {
                Ok(false)
            }
            async fn probe(&mut self) -> Result<Option<()>, ()> {
                self.probes += 1;
                Ok(None)
            }
            async fn wait(&mut self, _: tokio::time::Instant) {
                panic!("polled although the channel had nothing to say");
            }
        }

        /// Probes made while waiting out a 300 ms budget, in paused time,
        /// for a daemon that says nothing more.
        async fn probes_while_quiet(channel: Channel) -> usize {
            let budget = Budget {
                commit: Duration::from_millis(300),
                proof: Duration::ZERO,
            };
            let mut signaled = Signaled::new(AsyncQuiet { probes: 0 }, channel, LONG);
            assert_eq!(
                await_selection_async(budget, &mut signaled).await,
                Ok(Selection::NotCommitted)
            );
            signaled.into_parts().0.probes
        }

        #[tokio::test(start_paused = true)]
        async fn before_the_signal_a_round_waits_for_news_not_a_poll_interval() {
            // Paused time moves only when every task waits, so the first round
            // cannot outlast the budget: exactly one probe, then one at its end.
            let (channel, _daemon) = open();
            assert_eq!(probes_while_quiet(channel).await, 2);

            // A message that arrived before the round began is not news to it.
            let (channel, mut daemon) = open();
            daemon.write_all(b"progress\n").unwrap();
            wait_for_version(&channel.shared, 1);
            assert_eq!(probes_while_quiet(channel).await, 2);
        }

        #[tokio::test(start_paused = true)]
        async fn a_quiet_channel_holds_the_wait_until_its_deadline() {
            let (channel, _daemon) = open();
            let until = tokio::time::Instant::now() + ms(300);
            channel
                .wait_for_news_async(channel.version(), until, LONG)
                .await;
            assert!(tokio::time::Instant::now() >= until);
        }

        #[tokio::test]
        async fn errors_from_the_wrapped_evidence_end_the_wait() {
            struct Failing;
            impl AsyncEvidence for Failing {
                type Proof = ();
                type Error = &'static str;
                async fn committed(&mut self) -> Result<bool, Self::Error> {
                    Err("record unreadable")
                }
                async fn probe(&mut self) -> Result<Option<()>, Self::Error> {
                    Ok(None)
                }
            }
            let (channel, _daemon) = open();
            let mut signaled = Signaled::new(Failing, channel, LONG);
            assert_eq!(
                await_selection_async(SELECTION, &mut signaled).await,
                Err(SignaledError::Evidence("record unreadable"))
            );
        }

        #[tokio::test]
        async fn a_dead_channel_ends_the_selection_at_once() {
            let (channel, daemon) = open();
            drop(daemon);
            let mut signaled = Signaled::new(AsyncUnproven, channel, LONG);
            assert_eq!(
                await_selection_async(SELECTION, &mut signaled).await,
                Err(SignaledError::NotReady(NotReady::Died))
            );
        }

        #[tokio::test]
        async fn after_ready_the_wrapped_evidence_paces_the_rounds() {
            struct Paced {
                probes: usize,
                waits: usize,
            }
            impl AsyncEvidence for Paced {
                type Proof = ();
                type Error = ();
                async fn committed(&mut self) -> Result<bool, ()> {
                    Ok(true)
                }
                async fn probe(&mut self) -> Result<Option<()>, ()> {
                    self.probes += 1;
                    Ok((self.probes == 2).then_some(()))
                }
                async fn wait(&mut self, _: tokio::time::Instant) {
                    self.waits += 1;
                }
            }
            let (mut channel, mut daemon) = open();
            daemon.write_all(b"ready\n").unwrap();
            assert_eq!(channel.wait_async(LONG).await, Ok(()));
            let budget = Budget {
                commit: LONG,
                proof: LONG,
            };
            let mut signaled = Signaled::new(
                Paced {
                    probes: 0,
                    waits: 0,
                },
                channel,
                LONG,
            );
            assert_eq!(
                await_selection_async(budget, &mut signaled).await,
                Ok(Selection::Current(()))
            );
            let (evidence, _channel) = signaled.into_parts();
            assert_eq!(evidence.waits, 1);
        }
    }
}
