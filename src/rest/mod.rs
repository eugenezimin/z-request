//! The REST transport (feature `rest`): HTTP/1.1 and HTTP/2 over reqwest, `https://` with
//! feature `tls`.
//!
//! - [`RestConnector`] opens pooled [`RestConnection`]s: one request at a time each. Over
//!   HTTP/1.1 each has a socket of its own; over HTTP/2 up to `streams_per_connection` share
//!   one. With `http2: auto`, a peer's protocol is learned from its responses.
//! - [`RestClient`] calls one connection: [`RestRequest`]s are sent through
//!   [`Policies::run`](crate::policy::Policies::run) with the call's credentials and trace
//!   headers, and answered with a [`RestResponse`] read in full, up to `max_response_size`.
//!
//! Redirects are not followed (a non-2xx status is [`Error::Http`](crate::Error::Http)), proxy
//! settings are ignored, and a request path cannot leave the endpoint's base path.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use z_request::config::{ConnectionConfig, ZRequestConfig};
//! use z_request::rest::{RestClient, RestConnector};
//! use z_request::{AuthResolver, CallContext, ConnectionPool, ConsecutiveFailures, Protocol};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = ZRequestConfig::default();
//! config.connections.insert("registry".into(), ConnectionConfig {
//!     protocol: Some(Protocol::Rest),
//!     endpoint: Some("http://registry.internal:8080/api".into()),
//!     ..Default::default()
//! });
//! let resolved = config.resolve()?;
//! let registry = &resolved.connections[0];
//!
//! let pool = ConnectionPool::new(&resolved.pool);
//! let rest = pool.typed(RestConnector::new(&resolved.connections)?, &resolved.connections)?;
//! let auth = Arc::new(AuthResolver::new(&resolved.connections));
//! let breaker = Arc::new(ConsecutiveFailures::new(&resolved.connections));
//! let client = RestClient::new(registry, rest, auth, breaker)?;
//!
//! let ctx = CallContext::user("<the caller's token>", Default::default());
//! let entry: serde_json::Value = client
//!     .get("/entries/42")
//!     .query("fields", "name,owner")
//!     .send_json(&ctx)
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod client;
mod connector;

pub use reqwest::Method;
pub use reqwest::header::HeaderMap;

pub use self::client::{RestClient, RestRequest, RestResponse};
pub use self::connector::{RestConnection, RestConnector};

#[cfg(test)]
mod http2_tests;
#[cfg(test)]
mod tests;
