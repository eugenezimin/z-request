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

/// W3C trace context and request id, copied from the inbound request to every outbound call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TraceContext {
    /// The `traceparent` header.
    pub traceparent: Option<String>,
    /// The `tracestate` header.
    pub tracestate: Option<String>,
    /// The `x-request-id` header.
    pub request_id: Option<String>,
}

/// What a single call carries: the caller's own token (if it has one) and the trace context.
///
/// Which credentials are actually sent is decided by the connection's auth mode
/// (see [`CredentialsResolver`]).
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

/// Identifies one pooled connection.
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
    fn secrets_never_print() {
        let s = Secret::new("top-secret");
        assert_eq!(format!("{s:?} {s}"), "Secret(<redacted>) <redacted>");
        let ctx = CallContext::user("top-secret", TraceContext::default());
        assert!(!format!("{ctx:?}").contains("top-secret"));
        assert_eq!(Sent::User(s.clone()).kind(), "user");
        assert_eq!(Sent::None.bearer(), None);
    }
}
