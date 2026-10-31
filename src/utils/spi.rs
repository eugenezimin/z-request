//! The interfaces every z-request module is built against.
//!
//! Each trait has one job. The library ships default implementations (the pool, the policies,
//! the REST and gRPC connectors), and each one can be replaced by the host's own.
//!
//! | Trait | Job |
//! |---|---|
//! | [`Connector`] / [`Connection`] | open and close connections for one protocol |
//! | [`Pool`] | hand out connections within the limits; take them back |
//! | [`EvictionPolicy`] | pick which held connection to close when the pool is full |
//! | [`RetryPolicy`] | decide whether and when to try again |
//! | [`CircuitBreaker`] | fail fast while a target keeps failing |
//! | [`CredentialsResolver`] | turn a connection's auth mode and the call's context into what is sent |
//! | [`ConfigLoader`] | produce the configuration |

use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::utils::error::{ConfigError, Error};

/// The wire protocol of a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    /// gRPC over HTTP/2.
    Grpc,
    /// REST over HTTP.
    Rest,
}

impl fmt::Display for Protocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Grpc => "grpc",
            Self::Rest => "rest",
        })
    }
}

/// Where requests go: a named connection and its endpoint.
///
/// Pool limits are counted per target. Two connections with the same endpoint are two targets.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Target {
    name: Arc<str>,
    endpoint: Arc<str>,
}

impl Target {
    /// A target for the connection `name` at `endpoint`.
    pub fn new(name: impl Into<Arc<str>>, endpoint: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            endpoint: endpoint.into(),
        }
    }

    /// The connection's configured name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The endpoint URL, as configured.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// The name, shared rather than copied (for metric labels).
    pub(crate) fn shared_name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

/// A secret value (a token or a service secret). `Debug` and `Display` never show it.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(Arc<str>);

impl Secret {
    /// Wraps a secret value.
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    /// The secret itself. Call this only where it is sent.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

impl fmt::Display for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// W3C trace context, request id and initiator, copied from the inbound request to every
/// outbound call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// The `traceparent` header.
    pub traceparent: Option<String>,
    /// The `tracestate` header.
    pub tracestate: Option<String>,
    /// The `x-request-id` header.
    pub request_id: Option<String>,
    /// Who started the work, sent as [`INITIATOR_HEADER`].
    pub initiator: Option<Initiator>,
}

/// The header (gRPC metadata key) that carries the [`Initiator`].
pub const INITIATOR_HEADER: &str = "x-initiator";

/// Longest initiator id, in bytes.
const MAX_INITIATOR_ID: usize = 200;

/// What kind of party started the work a call belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InitiatorKind {
    /// An end user, identified by an id (never an e-mail or a name).
    User,
    /// A service acting on its own, such as a startup load or a background job.
    Service,
}

impl InitiatorKind {
    /// The prefix on the wire: `user` or `service`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Service => "service",
        }
    }
}

/// Who started the work a call belongs to, for **tracing and audit only**.
///
/// Sent as `x-initiator: user:<id>` or `x-initiator: service:<name>` next to whatever
/// credentials the connection's auth mode sends. The receiving service cannot verify it, so it
/// must never be used for an access decision; it only says, in logs and traces, on whose behalf
/// an authenticated peer called.
///
/// The id is 1 to 200 visible ASCII characters (no spaces or control characters), so it is
/// always a valid header value. Use an opaque id, never personal data.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Initiator {
    kind: InitiatorKind,
    /// The whole wire value, `<kind>:<id>`.
    value: Arc<str>,
}

/// An initiator that cannot be sent: an unknown kind, or an empty, too long or non-printable id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid initiator `{value}`: {reason}")]
pub struct InvalidInitiator {
    /// The rejected value (truncated).
    pub value: String,
    /// Why it was rejected.
    pub reason: &'static str,
}

impl Initiator {
    /// An end user, e.g. `user:0192f0c4-…`.
    pub fn user(id: impl AsRef<str>) -> Result<Self, InvalidInitiator> {
        Self::new(InitiatorKind::User, id.as_ref())
    }

    /// A service acting on its own, e.g. `service:counterparties/address-backfill`.
    pub fn service(name: impl AsRef<str>) -> Result<Self, InvalidInitiator> {
        Self::new(InitiatorKind::Service, name.as_ref())
    }

    /// Reads a received `x-initiator` value, to forward it on the next call.
    pub fn parse(value: &str) -> Result<Self, InvalidInitiator> {
        match value.split_once(':') {
            Some(("user", id)) => Self::user(id),
            Some(("service", name)) => Self::service(name),
            _ => Err(invalid(value, "must start with `user:` or `service:`")),
        }
    }

    fn new(kind: InitiatorKind, id: &str) -> Result<Self, InvalidInitiator> {
        let value = format!("{}:{id}", kind.as_str());
        if id.is_empty() {
            return Err(invalid(&value, "the id is empty"));
        }
        if id.len() > MAX_INITIATOR_ID {
            return Err(invalid(&value, "the id is longer than 200 bytes"));
        }
        if !id.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(invalid(
                &value,
                "the id may only hold visible ASCII characters",
            ));
        }
        Ok(Self {
            kind,
            value: value.into(),
        })
    }

    /// The kind of party.
    pub fn kind(&self) -> InitiatorKind {
        self.kind
    }

    /// The id without the kind prefix.
    pub fn id(&self) -> &str {
        &self.value[self.kind.as_str().len() + 1..]
    }

    /// The whole header value, `<kind>:<id>`.
    pub fn as_str(&self) -> &str {
        &self.value
    }
}

fn invalid(value: &str, reason: &'static str) -> InvalidInitiator {
    InvalidInitiator {
        value: value.chars().take(64).collect(),
        reason,
    }
}

impl fmt::Display for Initiator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.value)
    }
}

impl fmt::Debug for Initiator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Initiator({})", self.value)
    }
}

/// What a single call carries: the caller's own token (if it has one) and the trace context,
/// including the [`Initiator`].
///
/// Which credentials are actually sent is decided by the connection's auth mode
/// (see [`CredentialsResolver`]). The initiator is sent whatever the mode, so a host that
/// authenticates every call with its own secret (`env`) still says on whose behalf it calls.
#[derive(Debug, Clone, Default)]
pub struct CallContext {
    user_token: Option<Secret>,
    trace: TraceContext,
}

impl CallContext {
    /// A call made on behalf of a user, with the user's own (already verified) access token.
    pub fn user(token: impl Into<Arc<str>>, trace: TraceContext) -> Self {
        Self {
            user_token: Some(Secret::new(token)),
            trace,
        }
    }

    /// A call with no user behind it (startup loads, background jobs).
    pub fn without_user(trace: TraceContext) -> Self {
        Self {
            user_token: None,
            trace,
        }
    }

    /// The caller's token, if the call has one.
    pub fn user_token(&self) -> Option<&Secret> {
        self.user_token.as_ref()
    }

    /// The trace context to propagate.
    pub fn trace(&self) -> &TraceContext {
        &self.trace
    }

    /// Who started the work, if known.
    pub fn initiator(&self) -> Option<&Initiator> {
        self.trace.initiator.as_ref()
    }

    /// The same call, on behalf of `initiator`.
    pub fn with_initiator(mut self, initiator: Initiator) -> Self {
        self.trace.initiator = Some(initiator);
        self
    }
}

/// The credentials a call actually sends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sent {
    /// No credentials.
    None,
    /// The caller's own token.
    User(Secret),
    /// The connection's service secret.
    Secret(Secret),
}

impl Sent {
    /// A label for metrics and spans: `none`, `user` or `secret`. Never the value.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::User(_) => "user",
            Self::Secret(_) => "secret",
        }
    }

    /// The bearer token to send, if any.
    pub fn bearer(&self) -> Option<&Secret> {
        match self {
            Self::None => None,
            Self::User(s) | Self::Secret(s) => Some(s),
        }
    }
}

/// Decides what a call sends, from the connection's auth mode and the call's context.
pub trait CredentialsResolver: Send + Sync + 'static {
    /// The credentials to send to `target` for this call.
    fn resolve(&self, target: &Target, ctx: &CallContext) -> Result<Sent, Error>;
}

/// Opens connections for one protocol.
pub trait Connector: Send + Sync + 'static {
    /// The connection type this connector produces.
    type Conn: Connection;

    /// Opens a new connection to `target`.
    fn connect(&self, target: &Target) -> impl Future<Output = Result<Self::Conn, Error>> + Send;
}

/// An open connection.
pub trait Connection: Send + Sync + 'static {
    /// Whether the connection can serve another request. `false` after a transport error.
    fn is_usable(&self) -> bool;

    /// Closes the connection.
    fn close(self) -> impl Future<Output = ()> + Send;
}

/// Identifies one pooled connection. Opaque: pools may encode their own data in it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SlotId(pub u64);

/// Takes a connection back when a [`Lease`] ends. Implemented by pools.
pub trait Release<C>: Send + Sync {
    /// Returns `conn` to slot `slot`. `usable == false` means it must be closed, not kept.
    fn release(&self, slot: SlotId, conn: C, usable: bool);
}

/// A connection checked out of a pool. It goes back to the pool when dropped.
pub struct Lease<C: Connection> {
    conn: Option<C>,
    slot: SlotId,
    pool: Arc<dyn Release<C>>,
}

impl<C: Connection> Lease<C> {
    /// Creates a lease. Called by [`Pool`] implementations.
    pub fn new(conn: C, slot: SlotId, pool: Arc<dyn Release<C>>) -> Self {
        Self {
            conn: Some(conn),
            slot,
            pool,
        }
    }

    /// The pooled connection's slot.
    pub fn slot(&self) -> SlotId {
        self.slot
    }

    /// Returns the connection as broken, so the pool closes it instead of keeping it.
    pub fn discard(mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.release(self.slot, conn, false);
        }
    }
}

impl<C: Connection> Deref for Lease<C> {
    type Target = C;

    fn deref(&self) -> &C {
        // `conn` is `Some` from creation until `discard` or `drop`, both of which consume `self`.
        match &self.conn {
            Some(c) => c,
            None => unreachable!("a lease is never used after it is returned"),
        }
    }
}

impl<C: Connection> DerefMut for Lease<C> {
    fn deref_mut(&mut self) -> &mut C {
        match &mut self.conn {
            Some(c) => c,
            None => unreachable!("a lease is never used after it is returned"),
        }
    }
}

impl<C: Connection> Drop for Lease<C> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let usable = conn.is_usable();
            self.pool.release(self.slot, conn, usable);
        }
    }
}

impl<C: Connection> fmt::Debug for Lease<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Lease")
            .field("slot", &self.slot)
            .finish_non_exhaustive()
    }
}

/// A snapshot of a pool's state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolStats {
    /// Connections serving a request right now.
    pub busy: usize,
    /// Connections open and idle, kept for the next request.
    pub held: usize,
    /// Requests waiting for a connection.
    pub waiting: usize,
}

/// Hands out connections within the configured limits and takes them back.
pub trait Pool<C: Connection>: Send + Sync + 'static {
    /// A connection to `target`, waiting at most the configured acquire timeout.
    fn acquire(&self, target: &Target) -> impl Future<Output = Result<Lease<C>, Error>> + Send;

    /// The pool's current state.
    fn stats(&self) -> PoolStats;
}

/// An idle, held connection the pool could close to make room.
#[derive(Debug, Clone, Copy)]
pub struct IdleView<'a> {
    /// The connection's slot.
    pub slot: SlotId,
    /// Its target.
    pub target: &'a Target,
    /// When it was last returned to the pool.
    pub idle_since: Instant,
    /// Whether closing it would take its target below its `min`.
    pub at_min: bool,
}

/// Picks which held connection to close when the pool is full and another target needs room.
///
/// The pool calls it while locked: it must be quick and must not call back into the pool.
pub trait EvictionPolicy: Send + Sync + 'static {
    /// The connection to close for `requested`, or `None` to wait instead.
    fn choose(&self, requested: &Target, idle: &[IdleView<'_>]) -> Option<SlotId>;
}

/// Decides whether and when to retry a failed call.
pub trait RetryPolicy: Send + Sync + 'static {
    /// The wait before attempt `attempt + 1`, or `None` to give up. `attempt` starts at 1.
    fn next_delay(&self, attempt: u32, error: &Error, idempotent: bool) -> Option<Duration>;
}

/// The result of a call, as seen by a [`CircuitBreaker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The call succeeded (including an application-level "not found").
    Success,
    /// The call failed in a way that counts against the target.
    Failure,
}

/// Fails fast while a target keeps failing.
pub trait CircuitBreaker: Send + Sync + 'static {
    /// `Ok` if a call to `target` may go ahead, `Err(Error::CircuitOpen)` otherwise.
    fn admit(&self, target: &Target) -> Result<(), Error>;

    /// Records the outcome of a call to `target`.
    fn record(&self, target: &Target, outcome: Outcome);
}

/// Produces a configuration.
pub trait ConfigLoader {
    /// The configuration type produced.
    type Config;

    /// Loads (and structurally checks) the configuration.
    fn load(&self) -> Result<Self::Config, ConfigError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct Fake {
        usable: bool,
    }
    impl Connection for Fake {
        fn is_usable(&self) -> bool {
            self.usable
        }
        async fn close(self) {}
    }

    #[derive(Default)]
    struct Recorder(Mutex<Vec<(SlotId, bool)>>);
    impl Release<Fake> for Recorder {
        fn release(&self, slot: SlotId, _conn: Fake, usable: bool) {
            self.0.lock().unwrap().push((slot, usable));
        }
    }

    #[test]
    fn a_lease_returns_its_connection_exactly_once() {
        let rec = Arc::new(Recorder::default());
        drop(Lease::new(Fake { usable: true }, SlotId(1), rec.clone()));
        drop(Lease::new(Fake { usable: false }, SlotId(2), rec.clone()));
        Lease::new(Fake { usable: true }, SlotId(3), rec.clone()).discard();
        assert_eq!(
            *rec.0.lock().unwrap(),
            vec![(SlotId(1), true), (SlotId(2), false), (SlotId(3), false)]
        );
    }

    #[test]
    fn initiators_are_valid_header_values() {
        let u = Initiator::user("0192f0c4-7b1e-7c3a-9f00-000000000001").unwrap();
        assert_eq!(u.kind(), InitiatorKind::User);
        assert_eq!(u.id(), "0192f0c4-7b1e-7c3a-9f00-000000000001");
        assert_eq!(u.as_str(), "user:0192f0c4-7b1e-7c3a-9f00-000000000001");

        let s = Initiator::service("counterparties/address-backfill").unwrap();
        assert_eq!(s.to_string(), "service:counterparties/address-backfill");
        assert_eq!(Initiator::parse(s.as_str()).unwrap(), s);
        assert_eq!(Initiator::parse("user:a:b").unwrap().id(), "a:b");

        for bad in [
            "",
            "user:",
            "admin:x",
            "user",
            "user:a b",
            "user:a\r\nx-evil: 1",
            "user:é",
        ] {
            assert!(Initiator::parse(bad).is_err(), "accepted {bad:?}");
        }
        assert!(Initiator::user("x".repeat(200)).is_ok());
        assert!(Initiator::user("x".repeat(201)).is_err());
    }

    #[test]
    fn the_initiator_travels_with_the_trace_context() {
        let who = Initiator::user("u1").unwrap();
        let ctx = CallContext::without_user(TraceContext::default()).with_initiator(who.clone());
        assert_eq!(ctx.initiator(), Some(&who));
        assert_eq!(ctx.trace().initiator.as_ref(), Some(&who));
        assert!(ctx.user_token().is_none());
    }

    #[test]
    fn secrets_never_print() {
        let s = Secret::new("top-secret");
        assert_eq!(format!("{s:?} {s}"), "Secret(<redacted>) <redacted>");
        let ctx = CallContext::user("top-secret", TraceContext::default());
        assert!(!format!("{ctx:?}").contains("top-secret"));
        assert_eq!(Sent::User(s.clone()).kind(), "user");
        assert_eq!(Sent::None.bearer(), None);
    }
}
