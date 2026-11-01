//! [`GrpcConnector`] and [`GrpcConnection`]: pooled slots mapped onto shared HTTP/2 sockets.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};

use crate::config::ConnectionSettings;
use crate::sharing::{Negotiated, Peer, Share};
use crate::utils::error::{ConfigError, Error, Problem, error_text};
use crate::utils::spi::{Connection, Connector, Protocol, Target};

/// One pooled gRPC connection: a slot for one call at a time, on an HTTP/2 socket shared with
/// up to `streams_per_connection` other slots.
///
/// So the pool's `max` is the calls in flight to a peer, and the sockets are ⌈calls ÷
/// `streams_per_connection`⌉. A socket opens on its first call; if it drops, the channel
/// reconnects on the next call.
#[derive(Debug)]
pub struct GrpcConnection {
    share: Share<Channel>,
}

impl GrpcConnection {
    /// The channel for this connection's socket.
    pub(crate) fn channel(&self) -> &Channel {
        self.share.client()
    }

    /// The socket failed: no new calls go to it, and every slot on it is closed as it comes
    /// back.
    pub(crate) fn fail(&self) {
        self.share.retire();
    }
}

impl Connection for GrpcConnection {
    fn is_usable(&self) -> bool {
        // A call abandoned midway resets only its own stream. The slot is unusable once its
        // socket failed or outlived `max_connection_age`.
        self.share.usable()
    }

    async fn close(self) {
        // Dropping the share closes the socket with its last slot.
    }
}

/// Opens [`GrpcConnection`]s for gRPC connections, each within its `connect_timeout`.
pub struct GrpcConnector {
    endpoints: HashMap<Target, (Endpoint, Arc<Peer<Channel>>)>,
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
                    let target = c.target();
                    let peer = Peer::new(
                        &target,
                        "grpc",
                        Negotiated::Http2,
                        c.streams_per_connection,
                        c.max_connection_age,
                    );
                    endpoints.insert(target, (endpoint, peer));
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
        let (endpoint, peer) = self.profile(target)?;
        // A shared socket opens on its first call (a failure then is `Error::Connect`), so the
        // group exists at once and slots connecting together join it rather than each opening
        // a socket.
        let share = match peer.attach() {
            Some(share) => share,
            None => peer.add(endpoint.connect_lazy(), false),
        };
        Ok(GrpcConnection { share })
    }
}

impl GrpcConnector {
    fn profile(&self, target: &Target) -> Result<&(Endpoint, Arc<Peer<Channel>>), Error> {
        self.endpoints
            .get(target)
            .ok_or_else(|| Error::UnknownConnection {
                connection: target.name().to_owned(),
            })
    }

    /// A socket of its own for a long-lived stream: never shared, never retired by age.
    pub(crate) async fn dedicated(&self, target: &Target) -> Result<Share<Channel>, Error> {
        let (endpoint, peer) = self.profile(target)?;
        Ok(peer.add(open(endpoint, target).await?, true))
    }
}

/// Opens a channel (a socket) to `endpoint`.
async fn open(endpoint: &Endpoint, target: &Target) -> Result<Channel, Error> {
    endpoint.connect().await.map_err(|e| Error::Connect {
        connection: target.name().to_owned(),
        reason: error_text(&e),
    })
}
