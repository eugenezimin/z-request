//! [`Connections`]: every configured connection, built from the resolved configuration in one
//! call.

use std::collections::BTreeMap;
#[cfg(any(feature = "rest", feature = "grpc"))]
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::config::{ConnectionSettings, ResolvedConfig};
use crate::credentials::AuthResolver;
#[cfg(feature = "grpc")]
use crate::grpc::{GrpcClient, GrpcConnector};
use crate::policy::ConsecutiveFailures;
use crate::pool::{ConnectionPool, ConnectionPoolBuilder};
#[cfg(feature = "rest")]
use crate::rest::{RestClient, RestConnector};
#[cfg(not(all(feature = "rest", feature = "grpc")))]
use crate::utils::error::ConfigError;
use crate::utils::error::Error;
use crate::utils::spi::{CircuitBreaker, CredentialsResolver, EvictionPolicy, Protocol};

/// Every configured connection, sharing one pool, one credentials resolver and one breaker.
///
/// ```no_run
/// use z_request::config::ConfigSource;
/// use z_request::{CallContext, Connections};
///
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// let resolved = ConfigSource::new()
///     .yaml_file("config/clients.yaml")?
///     .with_env("COUNTERPARTIES")
///     .load()?
///     .resolve()?;
/// let connections = Connections::build(&resolved)?;
///
/// let ctx = CallContext::user("<the caller's token>", Default::default());
/// let entry: serde_json::Value = connections
///     .rest("registry")?
///     .get("/entries/42")
///     .send_json(&ctx)
///     .await?;
///
/// connections.close().await; // on shutdown
/// # Ok(())
/// # }
/// ```
///
/// Clients are cheap to clone and share the pool; ask for them once and keep them, or ask per
/// call.
pub struct Connections {
    pool: ConnectionPool,
    protocols: BTreeMap<String, Protocol>,
    #[cfg(feature = "rest")]
    rest: HashMap<String, RestClient>,
    #[cfg(feature = "grpc")]
    grpc: HashMap<String, GrpcClient>,
}

impl fmt::Debug for Connections {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Connections")
            .field("connections", &self.protocols)
            .finish_non_exhaustive()
    }
}

impl Connections {
    /// Builds every connection of `config` with the default parts:
    /// [`LruEviction`](crate::LruEviction), [`AuthResolver`] and [`ConsecutiveFailures`].
    /// Nothing is opened yet; connections open on demand.
    ///
    /// Install the metrics recorder first, so the pool's gauges register with it.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if a connection's TLS files cannot be used, or its protocol's feature
    /// is not compiled in.
    pub fn build(config: &ResolvedConfig) -> Result<Self, Error> {
        Self::builder(config).build()
    }

    /// A builder, to replace the eviction policy, the credentials resolver or the breaker.
    pub fn builder(config: &ResolvedConfig) -> ConnectionsBuilder<'_> {
        ConnectionsBuilder {
            config,
            pool: ConnectionPool::builder(&config.pool),
            credentials: None,
            breaker: None,
        }
    }

    /// The REST connection `name`.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownConnection`], or [`Error::WrongProtocol`] for a gRPC connection.
    #[cfg(feature = "rest")]
    pub fn rest(&self, name: &str) -> Result<RestClient, Error> {
        self.check(name, Protocol::Rest)?;
        self.rest.get(name).cloned().ok_or_else(|| unknown(name))
    }

    /// The gRPC connection `name`.
    ///
    /// # Errors
    ///
    /// [`Error::UnknownConnection`], or [`Error::WrongProtocol`] for a REST connection.
    #[cfg(feature = "grpc")]
    pub fn grpc(&self, name: &str) -> Result<GrpcClient, Error> {
        self.check(name, Protocol::Grpc)?;
        self.grpc.get(name).cloned().ok_or_else(|| unknown(name))
    }

    /// Every connection's name and protocol, by name.
    pub fn names(&self) -> impl Iterator<Item = (&str, Protocol)> {
        self.protocols.iter().map(|(name, p)| (name.as_str(), *p))
    }

    /// The shared pool, for its statistics.
    pub fn pool(&self) -> &ConnectionPool {
        &self.pool
    }

    /// Shuts down: see [`ConnectionPool::close`]. Calls made afterwards fail with
    /// [`Error::PoolClosed`].
    pub async fn close(&self) {
        self.pool.close().await;
    }

    #[cfg(any(feature = "rest", feature = "grpc"))]
    fn check(&self, name: &str, expected: Protocol) -> Result<(), Error> {
        match self.protocols.get(name) {
            None => Err(unknown(name)),
            Some(&actual) if actual != expected => Err(Error::WrongProtocol {
                connection: name.to_owned(),
                expected,
                actual,
            }),
            Some(_) => Ok(()),
        }
    }
}

#[cfg(any(feature = "rest", feature = "grpc"))]
fn unknown(name: &str) -> Error {
    Error::UnknownConnection {
        connection: name.to_owned(),
    }
}

/// Builds [`Connections`] with replaced parts.
pub struct ConnectionsBuilder<'a> {
    config: &'a ResolvedConfig,
    pool: ConnectionPoolBuilder,
    credentials: Option<Arc<dyn CredentialsResolver>>,
    breaker: Option<Arc<dyn CircuitBreaker>>,
}

impl fmt::Debug for ConnectionsBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionsBuilder")
            .field("connections", &self.config.connections.len())
            .finish_non_exhaustive()
    }
}

impl ConnectionsBuilder<'_> {
    /// Replaces the pool's eviction policy.
    pub fn eviction(mut self, policy: impl EvictionPolicy) -> Self {
        self.pool = self.pool.eviction(policy);
        self
    }

    /// Replaces the credentials resolver (default: [`AuthResolver`], each connection's `auth`).
    pub fn credentials(mut self, resolver: impl CredentialsResolver) -> Self {
        self.credentials = Some(Arc::new(resolver));
        self
    }

    /// Replaces the circuit breaker (default: [`ConsecutiveFailures`], each connection's
    /// `breaker`).
    pub fn breaker(mut self, breaker: impl CircuitBreaker) -> Self {
        self.breaker = Some(Arc::new(breaker));
        self
    }

    /// Builds every connection. See [`Connections::build`].
    pub fn build(self) -> Result<Connections, Error> {
        crate::telemetry::describe();
        let connections = &self.config.connections;
        let pool = self.pool.build();
        let credentials: Arc<dyn CredentialsResolver> = match self.credentials {
            Some(resolver) => resolver,
            None => Arc::new(AuthResolver::new(connections)),
        };
        let breaker: Arc<dyn CircuitBreaker> = match self.breaker {
            Some(breaker) => breaker,
            None => Arc::new(ConsecutiveFailures::new(connections)),
        };
        let of = |protocol: Protocol| -> Vec<&ConnectionSettings> {
            connections
                .iter()
                .filter(|c| c.protocol == protocol)
                .collect()
        };
        let (rest, grpc) = (of(Protocol::Rest), of(Protocol::Grpc));
        // Without the feature, the parts are unused.
        let _ = (&credentials, &breaker);

        #[cfg(feature = "rest")]
        let rest = {
            let mut clients = HashMap::with_capacity(rest.len());
            if !rest.is_empty() {
                let handle = pool.typed(
                    RestConnector::new(rest.iter().copied())?,
                    rest.iter().copied(),
                )?;
                for c in &rest {
                    let client = RestClient::new(
                        c,
                        handle.clone(),
                        Arc::clone(&credentials),
                        Arc::clone(&breaker),
                    )?;
                    clients.insert(c.name.clone(), client);
                }
            }
            clients
        };
        #[cfg(not(feature = "rest"))]
        missing_feature(&rest, "rest")?;

        #[cfg(feature = "grpc")]
        let grpc = {
            let mut clients = HashMap::with_capacity(grpc.len());
            if !grpc.is_empty() {
                let handle = pool.typed(
                    GrpcConnector::new(grpc.iter().copied())?,
                    grpc.iter().copied(),
                )?;
                for c in &grpc {
                    let client = GrpcClient::new(
                        c,
                        handle.clone(),
                        Arc::clone(&credentials),
                        Arc::clone(&breaker),
                    )?;
                    clients.insert(c.name.clone(), client);
                }
            }
            clients
        };
        #[cfg(not(feature = "grpc"))]
        missing_feature(&grpc, "grpc")?;

        Ok(Connections {
            pool,
            protocols: connections
                .iter()
                .map(|c| (c.name.clone(), c.protocol))
                .collect(),
            #[cfg(feature = "rest")]
            rest,
            #[cfg(feature = "grpc")]
            grpc,
        })
    }
}

/// Rejects connections whose protocol's feature is not compiled in (configuration resolution
/// already does; this covers a hand-built configuration).
#[cfg(not(all(feature = "rest", feature = "grpc")))]
fn missing_feature(connections: &[&ConnectionSettings], feature: &str) -> Result<(), Error> {
    match connections.first() {
        None => Ok(()),
        Some(c) => Err(ConfigError::new(
            format!("connections.{}.protocol", c.name),
            format!("{feature} needs the `{feature}` feature of z-request"),
        )
        .into()),
    }
}

#[cfg(test)]
mod tests;
