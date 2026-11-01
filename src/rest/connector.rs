//! [`RestConnector`] and [`RestConnection`]: pooled slots mapped onto shared sockets.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{ConnectionSettings, Http2Mode};
use crate::sharing::{Negotiated, Peer, Share};
use crate::utils::error::error_text;
use crate::utils::error::{ConfigError, Error, Problem};
use crate::utils::spi::{Connection, Connector, Protocol, Target};

/// One pooled REST connection: a slot for one request at a time, on a socket that is its own
/// (HTTP/1.1) or shared with up to `streams_per_connection` other slots (HTTP/2).
///
/// The socket opens on the first request. Which protocol a peer speaks is learned from its
/// responses (`http2: auto`), so a peer is treated as HTTP/1.1 until it answers in HTTP/2.
#[derive(Debug)]
pub struct RestConnection {
    share: Share<reqwest::Client>,
    /// `http2: auto`: learn the peer's protocol from responses.
    learns: bool,
}

impl RestConnection {
    /// The client for this connection's socket.
    pub(crate) fn client(&self) -> &reqwest::Client {
        self.share.client()
    }

    /// Records the protocol a response came in.
    pub(crate) fn observe(&self, version: reqwest::Version) {
        if !self.learns {
            return;
        }
        let spoken = if version == reqwest::Version::HTTP_2 {
            Negotiated::Http2
        } else {
            Negotiated::Http1
        };
        self.share.peer().learn(&self.share, spoken);
    }

    /// The socket failed: no new requests go to it, and every slot on it is closed as it comes
    /// back. A connection that expected HTTP/2 and could not be made negotiates again.
    pub(crate) fn fail(&self, connecting: bool) {
        if connecting && self.learns {
            self.share.peer().forget();
        }
        self.share.retire();
    }
}

impl Connection for RestConnection {
    fn is_usable(&self) -> bool {
        // A request abandoned midway (timed out, cancelled) leaves nothing half-read behind:
        // HTTP/1.1 drops that socket and opens a new one, HTTP/2 resets just that stream. The
        // slot is unusable once its socket failed, was retired, or outlived
        // `max_connection_age`.
        self.share.usable()
    }

    async fn close(self) {
        // Dropping the share closes the socket with its last slot.
    }
}

/// Opens [`RestConnection`]s for REST connections.
///
/// Every client it builds connects within `connect_timeout`, follows no redirects (a redirect
/// could carry the caller's token elsewhere) and ignores proxy settings. Its only retries are
/// HTTP/2 requests the server refused without processing them; every other retry is
/// [`Policies::run`](crate::policy::Policies::run)'s.
pub struct RestConnector {
    profiles: HashMap<Target, Profile>,
}

struct Profile {
    mode: Http2Mode,
    secure: bool,
    connect_timeout: Duration,
    peer: Arc<Peer<reqwest::Client>>,
    #[cfg(feature = "tls")]
    tls: rustls::ClientConfig,
    #[cfg(feature = "tls")]
    resolver: Option<Arc<ServerNameResolver>>,
}

/// The HTTP versions a new client may speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Versions {
    Http1,
    Http2,
    Either,
}

impl Profile {
    /// What a new socket should speak: HTTP/2 only once the peer is known to speak it (so
    /// concurrent requests on a new client wait for one socket instead of opening several).
    fn versions(&self) -> Versions {
        match (self.mode, self.secure, self.peer.negotiated()) {
            (Http2Mode::Off, ..) | (Http2Mode::Auto, false, _) => Versions::Http1,
            (Http2Mode::PriorKnowledge, ..) | (Http2Mode::Auto, true, Negotiated::Http2) => {
                Versions::Http2
            }
            (Http2Mode::Auto, true, _) => Versions::Either,
        }
    }

    fn client(&self, target: &Target) -> Result<reqwest::Client, Error> {
        let versions = self.versions();
        let builder = reqwest::Client::builder()
            .connect_timeout(self.connect_timeout)
            .pool_max_idle_per_host(1)
            .pool_idle_timeout(None) // `hold` and `max_connection_age` decide
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        let builder = match versions {
            Versions::Http1 => builder.http1_only(),
            Versions::Http2 => builder.http2_prior_knowledge(),
            Versions::Either => builder,
        };
        #[cfg(feature = "tls")]
        let builder = {
            let mut tls = self.tls.clone();
            tls.alpn_protocols = match versions {
                Versions::Http1 => vec![b"http/1.1".to_vec()],
                Versions::Http2 => vec![b"h2".to_vec()],
                Versions::Either => vec![b"h2".to_vec(), b"http/1.1".to_vec()],
            };
            let builder = builder.tls_backend_preconfigured(tls);
            match &self.resolver {
                Some(resolver) => builder.dns_resolver(Arc::clone(resolver)),
                None => builder,
            }
        };
        builder.build().map_err(|e| Error::Connect {
            connection: target.name().to_owned(),
            reason: error_text(&e),
        })
    }
}

impl fmt::Debug for RestConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestConnector")
            .field("connections", &self.profiles.len())
            .finish_non_exhaustive()
    }
}

impl RestConnector {
    /// A connector for `connections`, all REST. Reads their TLS files now.
    ///
    /// Install the metrics recorder first: each connection's `z_request_sockets` gauge
    /// registers here.
    ///
    /// # Errors
    ///
    /// [`Error::WrongProtocol`] for a gRPC connection; [`Error::Config`] for TLS files that
    /// cannot be read or used, or an `https://` endpoint without the `tls` feature.
    pub fn new<'a>(
        connections: impl IntoIterator<Item = &'a ConnectionSettings>,
    ) -> Result<Self, Error> {
        let mut profiles = HashMap::new();
        let mut problems = Vec::new();
        #[cfg(feature = "tls")]
        let mut plain: Option<rustls::ClientConfig> = None;
        for c in connections {
            if c.protocol != Protocol::Rest {
                return Err(Error::WrongProtocol {
                    connection: c.name.clone(),
                    expected: Protocol::Rest,
                    actual: c.protocol,
                });
            }
            let problem = |message: String| Problem {
                path: format!("connections.{}.tls", c.name),
                message,
            };

            #[cfg(not(feature = "tls"))]
            if c.endpoint.secure {
                problems.push(problem(
                    "https:// needs the `tls` feature of z-request".into(),
                ));
                continue;
            }

            #[cfg(feature = "tls")]
            let tls = if c.endpoint.secure {
                crate::tls::client_config(c.tls.as_ref())
            } else {
                // reqwest needs a TLS configuration even for http://; share one.
                match &plain {
                    Some(config) => Ok(config.clone()),
                    None => crate::tls::client_config(None).inspect(|config| {
                        plain = Some(config.clone());
                    }),
                }
            };
            #[cfg(feature = "tls")]
            let tls = match tls {
                Ok(tls) => tls,
                Err(message) => {
                    problems.push(problem(message));
                    continue;
                }
            };
            #[cfg(not(feature = "tls"))]
            let _ = problem;

            let (negotiated, streams) = match (c.http2, c.endpoint.secure) {
                (Http2Mode::Off, _) | (Http2Mode::Auto, false) => (Negotiated::Http1, 1),
                (Http2Mode::PriorKnowledge, _) => (Negotiated::Http2, c.streams_per_connection),
                (Http2Mode::Auto, true) => (Negotiated::Unknown, c.streams_per_connection),
            };
            let target = c.target();
            let peer = Peer::new(&target, "rest", negotiated, streams, c.max_connection_age);
            profiles.insert(
                target,
                Profile {
                    mode: c.http2,
                    secure: c.endpoint.secure,
                    connect_timeout: c.connect_timeout,
                    peer,
                    #[cfg(feature = "tls")]
                    tls,
                    #[cfg(feature = "tls")]
                    resolver: c.server_name().map(|name| {
                        Arc::new(ServerNameResolver {
                            server_name: name.to_owned(),
                            host: c.endpoint.host.clone(),
                            port: c.endpoint.port,
                        })
                    }),
                },
            );
        }
        match ConfigError::from_problems(problems) {
            Some(e) => Err(e.into()),
            None => Ok(Self { profiles }),
        }
    }
}

impl Connector for RestConnector {
    type Conn = RestConnection;

    async fn connect(&self, target: &Target) -> Result<RestConnection, Error> {
        let profile = self
            .profiles
            .get(target)
            .ok_or_else(|| Error::UnknownConnection {
                connection: target.name().to_owned(),
            })?;
        let share = match profile.peer.attach() {
            Some(share) => share,
            None => profile.peer.add(profile.client(target)?, false),
        };
        Ok(RestConnection {
            share,
            learns: profile.mode == Http2Mode::Auto && profile.secure,
        })
    }
}

/// Resolves `tls.server_name` to the endpoint's host, so the connection goes to the configured
/// address while TLS checks the certificate for `server_name`.
#[cfg(feature = "tls")]
#[derive(Debug)]
pub(crate) struct ServerNameResolver {
    server_name: String,
    host: String,
    port: u16,
}

#[cfg(feature = "tls")]
impl reqwest::dns::Resolve for ServerNameResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let (host, port) = if name.as_str().eq_ignore_ascii_case(&self.server_name) {
            (self.host.clone(), self.port)
        } else {
            (name.as_str().to_owned(), 0)
        };
        Box::pin(async move {
            let addrs: Vec<_> = tokio::net::lookup_host((host.as_str(), port))
                .await?
                .collect();
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}
