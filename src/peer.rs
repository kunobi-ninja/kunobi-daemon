//! Authenticate the process at the other end of a local connection.
//!
//! The flow has three steps. [`Evidence`] holds what the operating system
//! reports for one connection; with the `local` feature, `local::peer::evidence`
//! collects it. A [`Policy`] turns evidence into a grant or a [`Rejected`]
//! reason. [`authenticate`] runs the policy and returns an [`Authenticated`]
//! connection that carries its grant into dispatch, so a privileged handler can
//! require that grant in its signature.
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

use std::{fmt, num::NonZeroU32};

/// A nonzero operating-system process ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProcessId(NonZeroU32);

impl ProcessId {
    /// `None` for PID 0, which names no user process.
    pub const fn new(pid: u32) -> Option<Self> {
        match NonZeroU32::new(pid) {
            Some(pid) => Some(Self(pid)),
            None => None,
        }
    }

    /// The raw PID.
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for ProcessId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// What the operating system reports about the other end of one connection.
///
/// Read it while the connection is open and before dispatching anything.
/// Later fields, such as a code-signature result, will be added without a
/// breaking change; construct it with [`Evidence::new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Evidence {
    /// Kernel-reported PID of the peer, or `None` when the platform reports none.
    pub pid: Option<ProcessId>,
    /// Whether the peer runs as this process's OS user.
    pub same_user: bool,
}

impl Evidence {
    /// Evidence from an OS query, or for a consumer's own tests and sources.
    pub const fn new(pid: Option<ProcessId>, same_user: bool) -> Self {
        Self { pid, same_user }
    }
}

/// Why a policy refused a connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Rejected {
    /// The peer runs as another OS user.
    OtherUser,
    /// The platform reported no PID for the connection. Missing evidence is
    /// never a match.
    Unreported,
    /// The policy had no expected process to compare with, for example because
    /// the record naming it is missing.
    NoExpectedProcess,
    /// The connection belongs to another process.
    Different {
        /// The PID the policy expected.
        expected: ProcessId,
        /// The PID the operating system reported for the connection.
        observed: ProcessId,
    },
}

impl fmt::Display for Rejected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OtherUser => f.write_str("the peer runs as another OS user"),
            Self::Unreported => f.write_str("the connection's peer PID is not available"),
            Self::NoExpectedProcess => f.write_str("no expected process to compare with"),
            Self::Different { expected, observed } => {
                write!(f, "peer PID {observed} is not the expected PID {expected}")
            }
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

/// Compare the PID reported for a connection with the expected process.
pub fn match_pid(observed: Option<ProcessId>, expected: ProcessId) -> Result<PidMatch, Rejected> {
    match observed {
        None => Err(Rejected::Unreported),
        Some(observed) if observed == expected => Ok(PidMatch { pid: observed }),
        Some(observed) => Err(Rejected::Different { expected, observed }),
    }
}

/// Decide what a connection may do from its [`Evidence`].
pub trait Policy {
    /// What an accepted connection is allowed, in the consumer's own terms.
    type Grant;
    /// Grant access or say why not. Must not block or perform I/O.
    fn grant(&self, evidence: &Evidence) -> Result<Self::Grant, Rejected>;
}

/// Accept any process of this OS user.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SameUser;

impl Policy for SameUser {
    type Grant = ();
    fn grant(&self, evidence: &Evidence) -> Result<(), Rejected> {
        if evidence.same_user {
            Ok(())
        } else {
            Err(Rejected::OtherUser)
        }
    }
}

/// Accept one process of this OS user, whose PID `expected` returns when asked.
///
/// `expected` runs once per connection, so it can read a record the peer
/// keeps up to date. It must not block for long; return `None` when there is
/// no process to accept.
#[derive(Clone, Copy, Debug)]
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
    fn grant(&self, evidence: &Evidence) -> Result<PidMatch, Rejected> {
        SameUser.grant(evidence)?;
        let expected = (self.expected)().ok_or(Rejected::NoExpectedProcess)?;
        match_pid(evidence.pid, expected)
    }
}

type Tier<G> = Box<dyn Fn(&Evidence) -> Result<G, Rejected> + Send + Sync>;

/// Try policies in order and grant the value paired with the first one that
/// accepts. When none accepts, the last policy's reason is returned, so put the
/// broadest policy last.
///
/// ```
/// use kunobi_daemon::peer::{Evidence, First, Policy, ProcessId, SameUser, ExpectedProcess};
///
/// #[derive(Clone, Debug, PartialEq)]
/// enum View { Full, Redacted }
///
/// let app = ProcessId::new(42);
/// let policy = First::new()
///     .then(ExpectedProcess::new(move || app), View::Full)
///     .then(SameUser, View::Redacted);
///
/// assert_eq!(policy.grant(&Evidence::new(app, true)), Ok(View::Full));
/// assert_eq!(policy.grant(&Evidence::new(ProcessId::new(7), true)), Ok(View::Redacted));
/// assert!(policy.grant(&Evidence::new(app, false)).is_err());
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
    /// An empty list, which rejects every connection.
    pub fn new() -> Self {
        Self::default()
    }

    /// Grant `grant` when `policy` accepts and no earlier policy did.
    pub fn then<P>(mut self, policy: P, grant: G) -> Self
    where
        P: Policy + Send + Sync + 'static,
    {
        self.tiers.push(Box::new(move |evidence| {
            policy.grant(evidence).map(|_| grant.clone())
        }));
        self
    }
}

impl<G> Policy for First<G> {
    type Grant = G;
    fn grant(&self, evidence: &Evidence) -> Result<G, Rejected> {
        let mut rejected = Rejected::NoExpectedProcess;
        for tier in &self.tiers {
            match tier(evidence) {
                Ok(grant) => return Ok(grant),
                Err(reason) => rejected = reason,
            }
        }
        Err(rejected)
    }
}

/// A connection whose peer a [`Policy`] accepted, with the grant it received.
#[derive(Debug)]
pub struct Authenticated<C, G> {
    connection: C,
    grant: G,
    evidence: Evidence,
}

impl<C, G> Authenticated<C, G> {
    /// What the policy granted this connection.
    pub fn grant(&self) -> &G {
        &self.grant
    }

    /// The evidence the policy accepted.
    pub fn evidence(&self) -> &Evidence {
        &self.evidence
    }

    /// The connection, for serving it.
    pub fn connection(&self) -> &C {
        &self.connection
    }

    /// The connection, mutably.
    pub fn connection_mut(&mut self) -> &mut C {
        &mut self.connection
    }

    /// Give up the wrapper.
    pub fn into_parts(self) -> (C, G, Evidence) {
        (self.connection, self.grant, self.evidence)
    }
}

/// Run `policy` on `evidence` for `connection`. Collect the evidence from the
/// same connection, while it is open, before calling this.
pub fn authenticate<C, P: Policy>(
    connection: C,
    evidence: Evidence,
    policy: &P,
) -> Result<Authenticated<C, P::Grant>, Rejected> {
    let grant = policy.grant(&evidence)?;
    Ok(Authenticated {
        connection,
        grant,
        evidence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pid(value: u32) -> ProcessId {
        ProcessId::new(value).unwrap()
    }

    #[test]
    fn pid_zero_is_not_a_process() {
        assert_eq!(ProcessId::new(0), None);
        assert_eq!(pid(7).get(), 7);
        assert_eq!(pid(7).to_string(), "7");
    }

    #[test]
    fn only_an_equal_reported_pid_matches() {
        assert_eq!(match_pid(Some(pid(7)), pid(7)).map(|m| m.pid()), Ok(pid(7)));
        assert_eq!(
            match_pid(Some(pid(8)), pid(7)),
            Err(Rejected::Different {
                expected: pid(7),
                observed: pid(8)
            })
        );
        assert_eq!(match_pid(None, pid(7)), Err(Rejected::Unreported));
    }

    #[test]
    fn same_user_rejects_another_user_whatever_its_pid() {
        assert_eq!(SameUser.grant(&Evidence::new(Some(pid(7)), true)), Ok(()));
        assert_eq!(SameUser.grant(&Evidence::new(None, true)), Ok(()));
        assert_eq!(
            SameUser.grant(&Evidence::new(Some(pid(7)), false)),
            Err(Rejected::OtherUser)
        );
    }

    #[test]
    fn expected_process_needs_the_same_user_a_record_and_the_same_pid() {
        let app = ExpectedProcess::new(|| ProcessId::new(42));
        assert_eq!(
            app.grant(&Evidence::new(Some(pid(42)), true))
                .map(|m| m.pid()),
            Ok(pid(42))
        );
        assert_eq!(
            app.grant(&Evidence::new(Some(pid(42)), false)),
            Err(Rejected::OtherUser)
        );
        assert_eq!(
            app.grant(&Evidence::new(Some(pid(7)), true)),
            Err(Rejected::Different {
                expected: pid(42),
                observed: pid(7)
            })
        );
        assert_eq!(
            app.grant(&Evidence::new(None, true)),
            Err(Rejected::Unreported)
        );
        let missing = ExpectedProcess::new(|| None);
        assert_eq!(
            missing.grant(&Evidence::new(Some(pid(42)), true)),
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
        assert_eq!(
            tiered().grant(&Evidence::new(Some(pid(42)), true)),
            Ok(View::Full)
        );
        assert_eq!(
            tiered().grant(&Evidence::new(Some(pid(7)), true)),
            Ok(View::Redacted)
        );
        assert_eq!(
            tiered().grant(&Evidence::new(Some(pid(42)), false)),
            Err(Rejected::OtherUser)
        );
        // Order decides: the broad tier first shadows the narrow one.
        let shadowed = First::new()
            .then(SameUser, View::Redacted)
            .then(ExpectedProcess::new(|| ProcessId::new(42)), View::Full);
        assert_eq!(
            shadowed.grant(&Evidence::new(Some(pid(42)), true)),
            Ok(View::Redacted)
        );
    }

    #[test]
    fn an_empty_list_rejects_every_connection() {
        assert_eq!(
            First::<View>::new().grant(&Evidence::new(Some(pid(42)), true)),
            Err(Rejected::NoExpectedProcess)
        );
        assert_eq!(format!("{:?}", tiered()), "First { tiers: 2 }");
    }

    #[test]
    fn authenticate_carries_the_grant_and_evidence_with_the_connection() {
        let evidence = Evidence::new(Some(pid(42)), true);
        let mut accepted = authenticate("conn", evidence, &tiered()).unwrap();
        assert_eq!(accepted.grant(), &View::Full);
        assert_eq!(accepted.evidence(), &evidence);
        assert_eq!(*accepted.connection(), "conn");
        *accepted.connection_mut() = "moved";
        assert_eq!(accepted.into_parts(), ("moved", View::Full, evidence));
        assert_eq!(
            authenticate("conn", Evidence::new(Some(pid(1)), false), &tiered()).unwrap_err(),
            Rejected::OtherUser
        );
    }

    #[test]
    fn every_rejection_says_why() {
        assert_eq!(
            Rejected::OtherUser.to_string(),
            "the peer runs as another OS user"
        );
        assert_eq!(
            Rejected::Unreported.to_string(),
            "the connection's peer PID is not available"
        );
        assert_eq!(
            Rejected::NoExpectedProcess.to_string(),
            "no expected process to compare with"
        );
        assert_eq!(
            Rejected::Different {
                expected: pid(42),
                observed: pid(7)
            }
            .to_string(),
            "peer PID 7 is not the expected PID 42"
        );
    }
}
