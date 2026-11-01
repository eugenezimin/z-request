//! [`GrpcClient`]: calls to one gRPC connection, with credentials, trace metadata, the pool and
//! the policies.

use std::fmt;
use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tonic::Status;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tracing::Instrument;

use super::connector::{GrpcConnection, GrpcConnector};
use crate::config::ConnectionSettings;
use crate::credentials::outbound_headers;
use crate::policy::{ExponentialBackoff, Policies, outcome};
use crate::pool::TypedPool;
use crate::sharing::Share;
use crate::telemetry::{CallTelemetry, UNRESOLVED, error_status};
use crate::utils::error::{Error, error_text};
use crate::utils::spi::{
    CallContext, CircuitBreaker, CredentialsResolver, Lease, Pool, Protocol, RetryPolicy, Target,
};

/// The channel a call gets: a pooled connection that adds the call's metadata (credentials,
/// trace context, initiator, `grpc-timeout`) to every request. Pass it to a generated client,
/// e.g. `UsersClient::new(channel)`.
pub type GrpcChannel = InterceptedService<Channel, CallMetadata>;

/// The metadata added to every request on a [`GrpcChannel`]. Values are never printed.
#[derive(Clone)]
pub struct CallMetadata {
    entries: Arc<[(AsciiMetadataKey, AsciiMetadataValue)]>,
    timeout: Option<Duration>,
}

impl fmt::Debug for CallMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CallMetadata")
            .field(
                "keys",
                &self
                    .entries
                    .iter()
                    .map(|(k, _)| k.as_str())
                    .collect::<Vec<_>>(),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Interceptor for CallMetadata {
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let metadata = request.metadata_mut();
        for (key, value) in self.entries.iter() {
            metadata.insert(key.clone(), value.clone());
        }
        if let Some(timeout) = self.timeout {
            request.set_timeout(timeout);
        }
        Ok(request)
    }
}

/// Calls one gRPC connection.
///
/// [`call`](Self::call) runs one call through [`Policies::run`]: the breaker, a pooled
/// connection, the attempt within `request_timeout` (also sent as `grpc-timeout`), retries for
/// idempotent calls. [`dedicated`](Self::dedicated) hands out a connection for a long-lived
/// stream instead. Both send the credentials the connection's auth mode allows for the call's
/// [`CallContext`], the trace context and the initiator.
///
/// Clones share the same connection.
#[derive(Clone)]
pub struct GrpcClient(Arc<Inner>);

#[derive(Clone)]
struct Inner {
    target: Target,
    pool: TypedPool<GrpcConnector>,
    auth: Arc<dyn CredentialsResolver>,
    retry: Arc<dyn RetryPolicy>,
    breaker: Arc<dyn CircuitBreaker>,
    request_timeout: Duration,
}

impl fmt::Debug for GrpcClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcClient")
            .field("connection", &self.0.target.name())
            .finish_non_exhaustive()
    }
}

impl GrpcClient {
    /// A client for the gRPC connection `settings`, taking connections from `pool` (a handle
    /// over a [`GrpcConnector`] with this connection registered), credentials from `auth` and
    /// breaker state from `breaker`. Retries follow the connection's `retry` settings
    /// ([`ExponentialBackoff`]); see [`with_retry`](Self::with_retry).
    ///
    /// # Errors
    ///
    /// [`Error::WrongProtocol`] for a REST connection.
    pub fn new(
        settings: &ConnectionSettings,
        pool: TypedPool<GrpcConnector>,
        auth: Arc<dyn CredentialsResolver>,
        breaker: Arc<dyn CircuitBreaker>,
    ) -> Result<Self, Error> {
        if settings.protocol != Protocol::Grpc {
            return Err(Error::WrongProtocol {
                connection: settings.name.clone(),
                expected: Protocol::Grpc,
                actual: settings.protocol,
            });
        }
        Ok(Self(Arc::new(Inner {
            target: settings.target(),
            pool,
            auth,
            retry: Arc::new(ExponentialBackoff::for_connection(settings)),
            breaker,
            request_timeout: settings.request_timeout,
        })))
    }

    /// Replaces the retry policy.
    pub fn with_retry(mut self, retry: impl RetryPolicy) -> Self {
        Arc::make_mut(&mut self.0).retry = Arc::new(retry);
        self
    }

    /// The connection this client calls.
    pub fn target(&self) -> &Target {
        &self.0.target
    }

    /// Runs one call: `call` gets a [`GrpcChannel`] for each attempt and makes the request on
    /// it, typically through a generated client:
    ///
    /// ```ignore
    /// let user = grpc
    ///     .call(&ctx, true, |channel| async move {
    ///         UsersClient::new(channel).get_user(GetUser { id: 42 }).await
    ///     })
    ///     .await?
    ///     .into_inner();
    /// ```
    ///
    /// Only an `idempotent` call is retried after it may have reached the peer. A non-OK status
    /// becomes [`Error::Status`], a deadline [`Error::Timeout`], a connection that cannot be
    /// re-established [`Error::Connect`] and a broken one [`Error::Transport`] (the connection
    /// is then closed rather than reused).
    ///
    /// The connection returns to the pool when `call`'s future completes. A streamed response
    /// keeps using it afterwards; use [`dedicated`](Self::dedicated) for long-lived streams.
    pub async fn call<T, F, Fut>(
        &self,
        ctx: &CallContext,
        idempotent: bool,
        call: F,
    ) -> Result<T, Error>
    where
        F: FnMut(GrpcChannel) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let telemetry = CallTelemetry::start(&self.0.target, "grpc");
        let (auth, attempts) = (OnceLock::new(), AtomicU32::new(0));
        let result = self
            .call_all(ctx, idempotent, call, &auth, &attempts)
            .instrument(telemetry.span().clone())
            .await;
        let status = match &result {
            Ok(_) => Some(0),
            Err(e) => error_status(e),
        };
        let auth = auth.get().copied().unwrap_or(UNRESOLVED);
        telemetry.finish(auth, attempts.load(Ordering::Relaxed), status, &result);
        result
    }

    /// `call` without the telemetry: notes what was sent in `auth` and the attempts made.
    async fn call_all<T, F, Fut>(
        &self,
        ctx: &CallContext,
        idempotent: bool,
        mut call: F,
        auth: &OnceLock<&'static str>,
        attempts: &AtomicU32,
    ) -> Result<T, Error>
    where
        F: FnMut(GrpcChannel) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let inner = &*self.0;
        let (metadata, sent) = inner.metadata(ctx, Some(inner.request_timeout))?;
        let _ = auth.set(sent);
        let (connection, timeout) = (inner.target.name(), inner.request_timeout);
        let policies = Policies {
            retry: &*inner.retry,
            breaker: &*inner.breaker,
            request_timeout: inner.request_timeout,
        };
        policies
            .run(&inner.pool, &inner.target, idempotent, |lease, attempt| {
                attempts.store(attempt, Ordering::Relaxed);
                let attempt = call(InterceptedService::new(
                    lease.channel().clone(),
                    metadata.clone(),
                ));
                async move {
                    let result = attempt
                        .await
                        .map_err(|s| status_error(connection, &s, timeout));
                    // The socket failed: retire it, so every slot on it is closed.
                    if matches!(result, Err(Error::Connect { .. } | Error::Transport { .. })) {
                        lease.fail();
                    }
                    drop(lease);
                    result
                }
            })
            .await
    }

    /// A socket of its own, for a long-lived stream (a subscription): never shared with other
    /// calls and never retired by `max_connection_age`, so the stream is not cut off. It holds a
    /// pooled slot, counting against `max`, until the [`DedicatedChannel`] is dropped.
    ///
    /// Its metadata (credentials, trace context, initiator) is fixed now, and no
    /// `grpc-timeout` is sent. The breaker is checked once; nothing is retried.
    pub async fn dedicated(&self, ctx: &CallContext) -> Result<DedicatedChannel, Error> {
        let inner = &*self.0;
        let (metadata, _) = inner.metadata(ctx, None)?;
        inner.breaker.admit(&inner.target)?;
        let lease = inner.pool.acquire(&inner.target).await;
        if let Some(o) = outcome(&lease) {
            inner.breaker.record(&inner.target, o);
        }
        let lease = lease?;
        let socket = inner.pool.connector().dedicated(&inner.target).await;
        if let Some(o) = outcome(&socket) {
            inner.breaker.record(&inner.target, o);
        }
        let socket = socket?;
        tracing::debug!(
            connection = inner.target.name(),
            slot = lease.slot().0,
            "dedicated gRPC channel handed out"
        );
        Ok(DedicatedChannel {
            channel: InterceptedService::new(socket.client().clone(), metadata),
            _socket: socket,
            lease,
        })
    }
}

impl Inner {
    /// The call's metadata: the credentials its auth mode allows, the trace context and the
    /// initiator.
    /// Also returns what the credentials are (`none`, `user`, `secret`), for telemetry.
    fn metadata(
        &self,
        ctx: &CallContext,
        timeout: Option<Duration>,
    ) -> Result<(CallMetadata, &'static str), Error> {
        let sent = self.auth.resolve(&self.target, ctx)?;
        let entries = outbound_headers(&sent, ctx.trace())
            .filter_map(|(name, value)| {
                AsciiMetadataValue::try_from(value.as_ref())
                    .ok()
                    .map(|value| (AsciiMetadataKey::from_static(name), value))
            })
            .collect();
        Ok((CallMetadata { entries, timeout }, sent.kind()))
    }
}

/// A pooled connection held for a long-lived stream. Dropping it returns the connection to
/// the pool; [`discard`](Self::discard) closes it instead.
pub struct DedicatedChannel {
    channel: GrpcChannel,
    /// The stream's own socket, closed when the channel is dropped.
    _socket: Share<Channel>,
    /// Counts the stream against the connection's `max`.
    lease: Lease<GrpcConnection>,
}

impl fmt::Debug for DedicatedChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DedicatedChannel")
            .field("slot", &self.lease.slot())
            .finish_non_exhaustive()
    }
}

impl DedicatedChannel {
    /// The channel, for a generated client.
    pub fn channel(&self) -> GrpcChannel {
        self.channel.clone()
    }

    /// Closes the stream's socket and gives back its slot without reusing that slot's
    /// connection (after a transport error).
    pub fn discard(self) {
        self.lease.fail();
        self.lease.discard();
    }
}

/// Maps a status by its cause. tonic enforces `grpc-timeout` itself and reports a broken or
/// unreachable connection as a status too, so those are told apart from the peer's own status.
fn status_error(connection: &str, status: &Status, request_timeout: Duration) -> Error {
    let connection = connection.to_owned();
    let mut source = std::error::Error::source(status);
    let (mut timed_out, mut unreachable, mut broken) = (false, false, false);
    while let Some(s) = source {
        timed_out |= s.is::<tonic::TimeoutExpired>();
        unreachable |= s.is::<tonic::ConnectError>();
        broken |= s.is::<tonic::transport::Error>();
        source = s.source();
    }
    if timed_out {
        Error::Timeout {
            connection,
            after: request_timeout,
        }
    } else if unreachable {
        Error::Connect {
            connection,
            reason: error_text(status),
        }
    } else if broken {
        Error::Transport {
            connection,
            reason: error_text(status),
        }
    } else {
        Error::Status {
            connection,
            code: status.code() as i32,
            message: status.message().to_owned(),
        }
    }
}
