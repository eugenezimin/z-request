//! The REST transport (feature `rest`): HTTP/1.1 over reqwest, `https://` with feature `tls`.
//!
//! - [`RestConnector`] opens pooled [`RestConnection`]s: one reqwest client each, keeping at
//!   most one socket, so the pool's `min`, `max`, `hold` and eviction apply to real sockets.
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
#[cfg(feature = "tls")]
mod tls;

pub use reqwest::Method;
pub use reqwest::header::HeaderMap;

pub use self::client::{RestClient, RestRequest, RestResponse};
pub use self::connector::{RestConnection, RestConnector};

/// An error and its sources as one line, without repeating a source already in the message.
pub(crate) fn error_text(e: &(dyn std::error::Error + 'static)) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        let part = s.to_string();
        if !text.contains(&part) {
            text.push_str(": ");
            text.push_str(&part);
        }
        source = s.source();
    }
    text
}

#[cfg(test)]
mod tests;
