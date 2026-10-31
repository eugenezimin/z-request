//! [`RestConnector`] and [`RestConnection`]: one reqwest client per pooled connection.

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use super::error_text;
use crate::config::ConnectionSettings;
use crate::utils::error::{ConfigError, Error, Problem};
use crate::utils::spi::{Connection, Connector, Protocol, Target};

/// One pooled REST connection: a reqwest client that keeps at most one socket open.
///
/// So the pool's counts are real sockets: `max` caps them, `hold` closes them, and a lease is
/// one request at a time, as HTTP/1.1 sends them. The socket opens on the first request.
#[derive(Debug)]
pub struct RestConnection {
    client: reqwest::Client,
}

impl RestConnection {
    /// The client for this connection's socket.
    pub(crate) fn client(&self) -> &reqwest::Client {
        &self.client
    }
}

impl Connection for RestConnection {
    fn is_usable(&self) -> bool {
        // Always reusable. A request abandoned midway (timed out, cancelled) leaves no
        // half-read response behind: the HTTP client drops that socket and opens a new one for
        // the next request. After a socket error the lease is discarded instead.
        true
    }

    async fn close(self) {
        // Dropping the client closes its socket.
    }
}

/// Opens [`RestConnection`]s for REST connections.
///
/// Every client it builds connects within `connect_timeout`, keeps one socket, follows no
/// redirects (a redirect could carry the caller's token elsewhere), ignores proxy settings and
/// never retries by itself (retries are [`Policies::run`](crate::policy::Policies::run)'s).
pub struct RestConnector {
    profiles: HashMap<Target, Profile>,
}

struct Profile {
    connect_timeout: Duration,
    #[cfg(feature = "tls")]
    tls: rustls::ClientConfig,
    #[cfg(feature = "tls")]
    resolver: Option<std::sync::Arc<ServerNameResolver>>,
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
                super::tls::client_config(c.tls.as_ref())
            } else {
                // reqwest needs a TLS configuration even for http://; share one.
                match &plain {
                    Some(config) => Ok(config.clone()),
                    None => super::tls::client_config(None).inspect(|config| {
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

            profiles.insert(
                c.target(),
                Profile {
                    connect_timeout: c.connect_timeout,
                    #[cfg(feature = "tls")]
                    tls,
                    #[cfg(feature = "tls")]
                    resolver: server_name(c).map(|name| {
                        std::sync::Arc::new(ServerNameResolver {
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
        let builder = reqwest::Client::builder()
            .connect_timeout(profile.connect_timeout)
            .pool_max_idle_per_host(1)
            .pool_idle_timeout(None) // `hold` decides when the socket closes
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy();
        #[cfg(feature = "tls")]
        let builder = {
            let builder = builder.tls_backend_preconfigured(profile.tls.clone());
            match &profile.resolver {
                Some(resolver) => builder.dns_resolver(std::sync::Arc::clone(resolver)),
                None => builder,
            }
        };
        let client = builder.build().map_err(|e| Error::Connect {
            connection: target.name().to_owned(),
            reason: error_text(&e),
        })?;
        Ok(RestConnection { client })
    }
}

/// The `tls.server_name` of an `https://` connection, if set.
pub(crate) fn server_name(c: &ConnectionSettings) -> Option<&str> {
    c.tls
        .as_ref()
        .and_then(|t| t.server_name.as_deref())
        .filter(|_| c.endpoint.secure)
}

/// The URL requests to `c` start with: scheme, host, port and base path, no trailing `/`.
///
/// With `tls.server_name`, the host is that name, so TLS expects it on the certificate (and
/// sends it as SNI and `Host`); [`ServerNameResolver`] still connects to the endpoint's host.
pub(crate) fn base_url(c: &ConnectionSettings) -> String {
    let e = &c.endpoint;
    let scheme = if e.secure { "https" } else { "http" };
    let host = match server_name(c) {
        Some(name) => name.to_owned(),
        None if e.host.contains(':') => format!("[{}]", e.host),
        None => e.host.clone(),
    };
    format!("{scheme}://{host}:{}{}", e.port, e.base_path)
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
