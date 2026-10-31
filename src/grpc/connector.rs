//! [`GrpcConnector`] and [`GrpcConnection`]: one tonic channel (one HTTP/2 socket) per pooled
//! connection.

use std::collections::HashMap;
use std::fmt;

use tonic::transport::{Channel, Endpoint};

use crate::config::ConnectionSettings;
use crate::utils::error::{ConfigError, Error, Problem, error_text};
use crate::utils::spi::{Connection, Connector, Protocol, Target};

/// One pooled gRPC connection: a tonic channel over one HTTP/2 socket.
///
/// A lease is one call at a time, like REST, so the pool's `max` is both the sockets and the
/// concurrent calls to a peer. The socket opens when the connection is opened; if it drops, the
/// channel reconnects on the next call.
#[derive(Debug)]
pub struct GrpcConnection {
    channel: Channel,
}

impl GrpcConnection {
    /// The channel for this connection's socket.
    pub(crate) fn channel(&self) -> &Channel {
        &self.channel
    }
}

impl Connection for GrpcConnection {
    fn is_usable(&self) -> bool {
        // A call abandoned midway resets only its own stream; the connection stays usable.
        // After a transport error the lease is discarded instead.
        true
    }

    async fn close(self) {
        // Dropping the channel closes its socket.
    }
}

/// Opens [`GrpcConnection`]s for gRPC connections, each within its `connect_timeout`.
pub struct GrpcConnector {
    endpoints: HashMap<Target, Endpoint>,
}

impl fmt::Debug for GrpcConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcConnector")
            .field("connections", &self.endpoints.len())
            .finish_non_exhaustive()
    }
}

impl GrpcConnector {
    /// A connector for `connections`, all gRPC. Reads their TLS files now.
    ///
    /// # Errors
    ///
    /// [`Error::WrongProtocol`] for a REST connection; [`Error::Config`] for TLS files that
    /// cannot be read or used, or an `https://` endpoint without the `tls` feature.
    pub fn new<'a>(
        connections: impl IntoIterator<Item = &'a ConnectionSettings>,
    ) -> Result<Self, Error> {
        let mut endpoints = HashMap::new();
        let mut problems = Vec::new();
        for c in connections {
            if c.protocol != Protocol::Grpc {
                return Err(Error::WrongProtocol {
                    connection: c.name.clone(),
                    expected: Protocol::Grpc,
                    actual: c.protocol,
                });
            }
            match endpoint(c) {
                Ok(endpoint) => {
                    endpoints.insert(c.target(), endpoint);
                }
                Err(message) => problems.push(Problem {
                    path: format!("connections.{}.tls", c.name),
                    message,
                }),
            }
        }
        match ConfigError::from_problems(problems) {
            Some(e) => Err(e.into()),
            None => Ok(Self { endpoints }),
        }
    }
}

fn endpoint(c: &ConnectionSettings) -> Result<Endpoint, String> {
    let endpoint = Endpoint::from_shared(c.base_url())
        .map_err(|e| format!("invalid endpoint: {}", error_text(&e)))?
        .connect_timeout(c.connect_timeout)
        .tcp_nodelay(true);
    if !c.endpoint.secure {
        return Ok(endpoint);
    }
    #[cfg(feature = "tls")]
    {
        let mut tls = tonic::transport::ClientTlsConfig::new();
        if let Some(name) = c.server_name() {
            tls = tls.domain_name(name);
        }
        if let Some(identity) = crate::tls::identity_pem(c.tls.as_ref())? {
            tls = tls.identity(tonic::transport::Identity::from_pem(
                identity.cert,
                identity.key,
            ));
        }
        let verifier = crate::tls::verifier(c.tls.as_ref())?;
        endpoint
            .tls_config_with_verifier(tls, verifier)
            .map_err(|e| format!("cannot set up TLS: {}", error_text(&e)))
    }
    #[cfg(not(feature = "tls"))]
    Err("https:// needs the `tls` feature of z-request".into())
}

impl Connector for GrpcConnector {
    type Conn = GrpcConnection;

    async fn connect(&self, target: &Target) -> Result<GrpcConnection, Error> {
        let endpoint = self
            .endpoints
            .get(target)
            .ok_or_else(|| Error::UnknownConnection {
                connection: target.name().to_owned(),
            })?;
        let channel = endpoint.connect().await.map_err(|e| Error::Connect {
            connection: target.name().to_owned(),
            reason: error_text(&e),
        })?;
        Ok(GrpcConnection { channel })
    }
}
