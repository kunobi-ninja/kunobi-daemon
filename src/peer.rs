//! Authenticate the process at the other end of a local connection.
//!
//! The flow has three steps. [`Credentials`] hold what the operating system
//! reports for one connection; with the `local` feature, any connection that
//! implements `local::peer::PeerCredentials` collects them. A [`Policy`] turns
//! credentials into a grant or a [`Rejected`] reason. [`authenticate`] runs the
//! policy and returns an [`Authenticated`] connection that carries its grant
//! into dispatch, so a privileged handler can require that grant in its
//! signature.
//!
//! The built-in policies compose: [`SameUser`] accepts any process of this OS
//! user, [`ExpectedProcess`] also requires the PID the caller expects, and
//! [`First`] maps the first policy that accepts to a grant of the consumer's
//! own type. Consumers implement [`Policy`] for anything else.
//!
//! A PID match is numeric agreement between the PID the kernel reports for the
//! connection and the PID the caller expected, usually read from a record its
//! peer published. It does not prove who wrote that record, which executable
//! the peer runs, or which process writes later bytes: a process of the same
//! OS user can publish its own PID, and a descriptor can be passed or inherited.
//! The consumer decides what a grant allows.

use crate::ProcessId;
use std::{borrow::Cow, fmt};

/// What the operating system reports about the other end of one connection.
///
/// Read them while the connection is open and before dispatching anything.
/// Later fields, such as a code-signature result, will be added without a
/// breaking change; construct them with [`Credentials::new`].
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Credentials {
    /// Kernel-reported PID of the peer, or `None` when the platform reports none.
    pub pid: Option<ProcessId>,
    /// Whether the peer runs as this process's OS user.
    pub same_user: bool,
}

impl Credentials {
    /// Credentials from an OS query, or for a consumer's own tests and sources.
    pub const fn new(pid: Option<ProcessId>, same_user: bool) -> Self {
        Self { pid, same_user }
    }
}

/// Why a policy refused a connection.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Rejected {
    /// The peer runs as another OS user.
    OtherUser,
    /// The platform reported no PID for the connection. Missing credentials
    /// are never a match.
    Unreported,
    /// The policy had no expected process to compare with, for example because
    /// the record naming it is missing.
    NoExpectedProcess,
    /// The connection belongs to another process.
    OtherProcess {
        /// The PID the policy expected.
        expected: ProcessId,
        /// The PID the operating system reported for the connection.
        observed: ProcessId,
    },
    /// The policy has no rules, so it accepts nobody.
    NoPolicy,
    /// A consumer's own policy refused the peer, for the reason given.
    Custom(Cow<'static, str>),
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OtherUser => f.write_str("the peer runs as another OS user"),
            Self::Unreported => f.write_str("the connection's peer PID is not available"),
            Self::NoExpectedProcess => f.write_str("no expected process to compare with"),
            Self::OtherProcess { expected, observed } => {
                write!(f, "peer PID {observed} is not the expected PID {expected}")
            }
            Self::NoPolicy => f.write_str("no policy accepts any peer"),
            Self::Custom(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for Rejected {}

/// The connection's PID equals the expected PID. See the module docs for what
/// this does not establish.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PidMatch {
    pid: ProcessId,
}

impl PidMatch {
    /// The PID both sides agreed on.
    pub const fn pid(&self) -> ProcessId {
        self.pid
    }
}

/// Compare the PID reported for a connection with the expected process. Only
/// [`ExpectedProcess`] calls this, after its same-user check.
fn match_pid(observed: Option<ProcessId>, expected: ProcessId) -> Result<PidMatch, Rejected> {
    match observed {
        None => Err(Rejected::Unreported),
        Some(observed) if observed == expected => Ok(PidMatch { pid: observed }),
        Some(observed) => Err(Rejected::OtherProcess { expected, observed }),
    }
}

/// Decide what a connection may do from its [`Credentials`].
pub trait Policy {
    /// What an accepted connection is allowed, in the consumer's own terms.
    type Grant;
    /// Grant access or say why not. Runs on the accepting task, so it must not
    /// block or perform I/O: read state the consumer keeps current elsewhere.
    fn grant(&self, credentials: &Credentials) -> Result<Self::Grant, Rejected>;
}

impl<P: Policy + ?Sized> Policy for &P {
    type Grant = P::Grant;
    fn grant(&self, credentials: &Credentials) -> Result<P::Grant, Rejected> {
        (**self).grant(credentials)
    }
}

impl<P: Policy + ?Sized> Policy for Box<P> {
    type Grant = P::Grant;
    fn grant(&self, credentials: &Credentials) -> Result<P::Grant, Rejected> {
        (**self).grant(credentials)
    }
}

/// Accept any process of this OS user.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SameUser;

impl Policy for SameUser {
    type Grant = ();
    fn grant(&self, credentials: &Credentials) -> Result<(), Rejected> {
        if credentials.same_user {
            Ok(())
        } else {
            Err(Rejected::OtherUser)
        }
    }
}

/// Accept one process of this OS user, whose PID `expected` returns when asked.
///
/// `expected` is called at most once per [`Policy::grant`], and only after the
/// same-user check passes. Like the policy, it must not block or perform I/O:
/// return a PID the consumer already read, for example from a record it
/// watches, or `None` when there is no process to accept.
#[derive(Clone, Copy)]
pub struct ExpectedProcess<F> {
    expected: F,
}

impl<F: Fn() -> Option<ProcessId>> ExpectedProcess<F> {
    /// Accept the process `expected` names at connection time.
    pub const fn new(expected: F) -> Self {
        Self { expected }
    }
}

impl<F: Fn() -> Option<ProcessId>> Policy for ExpectedProcess<F> {
    type Grant = PidMatch;
    fn grant(&self, credentials: &Credentials) -> Result<PidMatch, Rejected> {
        SameUser.grant(credentials)?;
        let expected = (self.expected)().ok_or(Rejected::NoExpectedProcess)?;
        match_pid(credentials.pid, expected)
    }
}

type Tier<G> = Box<dyn Fn(&Credentials) -> Result<G, Rejected> + Send + Sync>;

/// Try policies in order and grant the value paired with the first one that
/// accepts. When none accepts, the last policy's reason is returned, so put the
/// broadest policy last.
///
/// ```
/// use kunobi_daemon::{
///     ProcessId,
///     peer::{Credentials, ExpectedProcess, First, Policy, SameUser},
/// };
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum View { Full, Redacted }
///
/// let app = ProcessId::new(42);
/// let policy = First::new()
///     .then(ExpectedProcess::new(move || app), View::Full)
///     .then(SameUser, View::Redacted);
///
/// assert_eq!(policy.grant(&Credentials::new(app, true)), Ok(View::Full));
/// assert_eq!(policy.grant(&Credentials::new(ProcessId::new(7), true)), Ok(View::Redacted));
/// assert!(policy.grant(&Credentials::new(app, false)).is_err());
/// ```
pub struct First<G> {
    tiers: Vec<Tier<G>>,
}

impl<G> Default for First<G> {
    fn default() -> Self {
        Self { tiers: Vec::new() }
    }
}

impl<G> fmt::Debug for First<G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("First")
            .field("tiers", &self.tiers.len())
            .finish()
    }
}

impl<G: Clone + Send + Sync + 'static> First<G> {
    /// An empty list, which rejects every connection with [`Rejected::NoPolicy`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant `grant` when `policy` accepts and no earlier policy did.
    pub fn then<P>(mut self, policy: P, grant: G) -> Self
    where
        P: Policy + Send + Sync + 'static,
    {
        self.tiers.push(Box::new(move |credentials| {
            policy.grant(credentials).map(|_| grant.clone())
        }));
        self
    }
}

impl<G> Policy for First<G> {
    type Grant = G;
    fn grant(&self, credentials: &Credentials) -> Result<G, Rejected> {
        let mut rejected = Rejected::NoPolicy;
        for tier in &self.tiers {
            match tier(credentials) {
                Ok(grant) => return Ok(grant),
                Err(reason) => rejected = reason,
            }
        }
        Err(rejected)
    }
}

/// A connection whose peer a [`Policy`] accepted, with the grant it received.
///
/// Reads and writes pass through to the connection. There is no mutable access
/// to the connection itself, so it cannot be swapped for another one while the
/// grant is kept; take it out with [`Self::into_parts`] and authenticate again.
#[derive(Debug)]
pub struct Authenticated<C, G> {
    connection: C,
    grant: G,
    credentials: Credentials,
}

impl<C, G> Authenticated<C, G> {
    /// What the policy granted this connection.
    pub fn grant(&self) -> &G {
        &self.grant
    }

    /// The credentials the policy accepted.
    pub fn credentials(&self) -> &Credentials {
        &self.credentials
    }

    /// The connection, for serving it.
    pub fn connection(&self) -> &C {
        &self.connection
    }

    /// Give up the wrapper.
    pub fn into_parts(self) -> (C, G, Credentials) {
        (self.connection, self.grant, self.credentials)
    }
}

impl<C: std::io::Read, G> std::io::Read for Authenticated<C, G> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.connection.read(buf)
    }
}

impl<C: std::io::Write, G> std::io::Write for Authenticated<C, G> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.connection.write(buf)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.connection.flush()
    }
}

#[cfg(feature = "async")]
impl<C: tokio::io::AsyncRead + Unpin, G: Unpin> tokio::io::AsyncRead for Authenticated<C, G> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().connection).poll_read(cx, buf)
    }
}

#[cfg(feature = "async")]
impl<C: tokio::io::AsyncWrite + Unpin, G: Unpin> tokio::io::AsyncWrite for Authenticated<C, G> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        std::pin::Pin::new(&mut self.get_mut().connection).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().connection).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        std::pin::Pin::new(&mut self.get_mut().connection).poll_shutdown(cx)
    }
}

/// Run `policy` on `credentials` for `connection`. Collect the credentials from
/// the same connection, while it is open, before calling this: `authenticate`
/// cannot check where they came from.
pub fn authenticate<C, P: Policy + ?Sized>(
    connection: C,
    credentials: Credentials,
    policy: &P,
) -> Result<Authenticated<C, P::Grant>, Rejected> {
    let grant = policy.grant(&credentials)?;
    Ok(Authenticated {
        connection,
        grant,
        credentials,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(value: u32) -> ProcessId {
        ProcessId::new(value).unwrap()
    }

    fn creds(pid_value: u32, same_user: bool) -> Credentials {
        Credentials::new(Some(pid(pid_value)), same_user)
    }

    #[test]
    fn only_an_equal_reported_pid_matches() {
        assert_eq!(match_pid(Some(pid(7)), pid(7)).map(|m| m.pid()), Ok(pid(7)));
        assert_eq!(
            match_pid(Some(pid(8)), pid(7)),
            Err(Rejected::OtherProcess {
                expected: pid(7),
                observed: pid(8)
            })
        );
        assert_eq!(match_pid(None, pid(7)), Err(Rejected::Unreported));
    }

    #[test]
    fn same_user_rejects_another_user_whatever_its_pid() {
        assert_eq!(SameUser.grant(&creds(7, true)), Ok(()));
        assert_eq!(SameUser.grant(&Credentials::new(None, true)), Ok(()));
        assert_eq!(SameUser.grant(&creds(7, false)), Err(Rejected::OtherUser));
    }

    #[test]
    fn expected_process_needs_the_same_user_a_record_and_the_same_pid() {
        let app = ExpectedProcess::new(|| ProcessId::new(42));
        assert_eq!(app.grant(&creds(42, true)).map(|m| m.pid()), Ok(pid(42)));
        assert_eq!(app.grant(&creds(42, false)), Err(Rejected::OtherUser));
        assert_eq!(
            app.grant(&creds(7, true)),
            Err(Rejected::OtherProcess {
                expected: pid(42),
                observed: pid(7)
            })
        );
        assert_eq!(
            app.grant(&Credentials::new(None, true)),
            Err(Rejected::Unreported)
        );
        let missing = ExpectedProcess::new(|| None);
        assert_eq!(
            missing.grant(&creds(42, true)),
            Err(Rejected::NoExpectedProcess)
        );
    }

    #[derive(Clone, Debug, PartialEq)]
    enum View {
        Full,
        Redacted,
    }

    fn tiered() -> First<View> {
        First::new()
            .then(ExpectedProcess::new(|| ProcessId::new(42)), View::Full)
            .then(SameUser, View::Redacted)
    }

    #[test]
    fn first_grants_the_first_accepting_tier_in_order() {
        assert_eq!(tiered().grant(&creds(42, true)), Ok(View::Full));
        assert_eq!(tiered().grant(&creds(7, true)), Ok(View::Redacted));
        assert_eq!(tiered().grant(&creds(42, false)), Err(Rejected::OtherUser));
        // Order decides: the broad tier first shadows the narrow one.
        let shadowed = First::new()
            .then(SameUser, View::Redacted)
            .then(ExpectedProcess::new(|| ProcessId::new(42)), View::Full);
        assert_eq!(shadowed.grant(&creds(42, true)), Ok(View::Redacted));
    }

    #[test]
    fn an_empty_list_rejects_every_connection() {
        assert_eq!(
            First::<View>::new().grant(&creds(42, true)),
            Err(Rejected::NoPolicy)
        );
    }

    #[test]
    fn references_and_boxes_of_a_policy_are_policies() {
        fn by_value<P: Policy>(policy: P, credentials: &Credentials) -> Result<P::Grant, Rejected> {
            policy.grant(credentials)
        }
        let shared = tiered();
        assert_eq!(by_value(&shared, &creds(42, true)), Ok(View::Full));
        assert_eq!(by_value(&shared, &creds(7, true)), Ok(View::Redacted));
        let boxed: Box<dyn Policy<Grant = ()>> = Box::new(SameUser);
        assert_eq!(boxed.grant(&creds(7, false)), Err(Rejected::OtherUser));
        assert_eq!(boxed.grant(&creds(7, true)), Ok(()));
    }

    #[test]
    fn authenticate_carries_the_grant_and_credentials_with_the_connection() {
        let credentials = creds(42, true);
        let accepted = authenticate("conn", credentials.clone(), &tiered()).unwrap();
        assert_eq!(accepted.grant(), &View::Full);
        assert_eq!(accepted.credentials(), &credentials);
        assert_eq!(*accepted.connection(), "conn");
        assert_eq!(
            accepted.into_parts(),
            ("conn", View::Full, credentials.clone())
        );
        let dynamic: &dyn Policy<Grant = ()> = &SameUser;
        assert!(authenticate((), credentials, dynamic).is_ok());
        assert_eq!(
            authenticate("conn", creds(1, false), &tiered()).unwrap_err(),
            Rejected::OtherUser
        );
    }

    #[test]
    fn reads_and_writes_pass_through_to_the_connection() {
        use std::io::{Read, Write};
        let mut reader = authenticate(&b"ping"[..], creds(1, true), &SameUser).unwrap();
        let mut text = String::new();
        reader.read_to_string(&mut text).unwrap();
        assert_eq!(text, "ping");
        // A buffered writer only reaches its inner vector on flush, so this
        // fails if the passthrough does not flush the connection.
        let mut writer = authenticate(
            std::io::BufWriter::new(Vec::new()),
            creds(1, true),
            &SameUser,
        )
        .unwrap();
        writer.write_all(b"pong").unwrap();
        assert!(writer.connection().get_ref().is_empty());
        writer.flush().unwrap();
        assert_eq!(writer.connection().get_ref(), b"pong");
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn async_reads_and_writes_pass_through_to_the_connection() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut reader = authenticate(&b"ping"[..], creds(1, true), &SameUser).unwrap();
        let mut text = String::new();
        reader.read_to_string(&mut text).await.unwrap();
        assert_eq!(text, "ping");
        // As above: the buffered writer's inner vector changes only on flush.
        let mut writer = authenticate(
            tokio::io::BufWriter::new(Vec::new()),
            creds(1, true),
            &SameUser,
        )
        .unwrap();
        writer.write_all(b"pong").await.unwrap();
        assert!(writer.connection().get_ref().is_empty());
        writer.flush().await.unwrap();
        assert_eq!(writer.connection().get_ref(), b"pong");
        writer.shutdown().await.unwrap();
    }

    #[test]
    fn first_reports_the_last_tiers_reason_when_none_accepts() {
        struct Refuse;
        impl Policy for Refuse {
            type Grant = ();
            fn grant(&self, _: &Credentials) -> Result<(), Rejected> {
                Err(Rejected::Custom("closed for maintenance".into()))
            }
        }
        let refuse_last = First::new()
            .then(ExpectedProcess::new(|| None), View::Full)
            .then(Refuse, View::Redacted);
        assert_eq!(
            refuse_last.grant(&creds(42, true)),
            Err(Rejected::Custom("closed for maintenance".into()))
        );
        let missing_last = First::new()
            .then(Refuse, View::Redacted)
            .then(ExpectedProcess::new(|| None), View::Full);
        assert_eq!(
            missing_last.grant(&creds(42, true)),
            Err(Rejected::NoExpectedProcess)
        );
    }

    #[test]
    fn the_expected_pid_is_read_once_and_only_when_needed() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = {
            let calls = Arc::clone(&calls);
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                ProcessId::new(42)
            }
        };
        let app = ExpectedProcess::new(counted.clone());
        assert!(app.grant(&creds(42, true)).is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // Another user is refused before the record is consulted.
        assert!(app.grant(&creds(42, false)).is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        // A tier that already accepted stops the list.
        let stop_early = First::new()
            .then(SameUser, View::Redacted)
            .then(ExpectedProcess::new(counted), View::Full);
        assert_eq!(stop_early.grant(&creds(42, true)), Ok(View::Redacted));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn every_rejection_says_why() {
        let cases = [
            (Rejected::OtherUser, "the peer runs as another OS user"),
            (
                Rejected::Unreported,
                "the connection's peer PID is not available",
            ),
            (
                Rejected::NoExpectedProcess,
                "no expected process to compare with",
            ),
            (
                Rejected::OtherProcess {
                    expected: pid(42),
                    observed: pid(7),
                },
                "peer PID 7 is not the expected PID 42",
            ),
            (Rejected::NoPolicy, "no policy accepts any peer"),
            (Rejected::Custom("closed".into()), "closed"),
        ];
        for (rejected, text) in cases {
            assert_eq!(rejected.to_string(), text);
        }
    }
}
