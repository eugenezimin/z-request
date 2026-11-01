//! The connection pool: one pool shared by every connection, of every protocol.
//!
//! [`ConnectionPool`] holds the limits and the bookkeeping for all targets (one target per
//! configured connection). Each protocol registers its connections with
//! [`ConnectionPool::typed`] and gets a [`TypedPool`], which implements [`spi::Pool`] for that
//! protocol's connection type. Under `scope: total` the cap covers every protocol together, and
//! a full pool can close an idle REST connection to make room for a gRPC one.
//!
//! | Setting | Behaviour |
//! |---|---|
//! | `max` | Request slots open or being opened, per connection (`per_uri`) or in total (`total`; each connection's own `max` still applies inside it). The transports map slots onto sockets: one each over HTTP/1.1, shared over HTTP/2. |
//! | `min` | Opened on demand, never up front. Once open, `hold` never closes a connection below it; under `total` `pool.min` is also a floor for all connections together. |
//! | `hold` | A released connection stays open this long, then the reaper closes it (up to 100 ms late, never early). `0s` closes it on release, unless `min` keeps it or a waiting request takes it directly. |
//! | `acquire_timeout` | How long a request waits for a free slot, then [`Error::AcquireTimeout`](crate::Error::AcquireTimeout). Opening the connection is bounded separately by the connection's `connect_timeout`. |
//!
//! Waiting requests are served first come, first served, and a released connection goes
//! straight to the oldest waiting request of its connection, so newcomers never overtake.
//! Under `total`, when the cap is reached, the [`EvictionPolicy`](crate::spi::EvictionPolicy)
//! (default [`LruEviction`]) picks an idle connection of another connection to close.
//!
//! A lease that ends with a broken connection ([`Connection::is_usable`] is `false`, or
//! [`Lease::discard`]) closes it and frees the slot. [`ConnectionPool::close`] shuts the pool
//! down.
//!
//! `acquire` and `close` need a tokio runtime with time enabled; building the pool does not.
//!
//! [`spi::Pool`]: crate::spi::Pool
//! [`Connection::is_usable`]: crate::spi::Connection::is_usable
//! [`Lease::discard`]: crate::spi::Lease::discard
//!
//! ```no_run
//! use z_request::config::{ConnectionConfig, ZRequestConfig};
//! use z_request::pool::ConnectionPool;
//! use z_request::spi::{Connection, Connector, Pool, Target};
//! use z_request::{Error, Protocol};
//!
//! // Stand-ins for the REST transport's connector and connection.
//! struct Http;
//! struct HttpConn;
//! impl Connection for HttpConn {
//!     fn is_usable(&self) -> bool { true }
//!     async fn close(self) {}
//! }
//! impl Connector for Http {
//!     type Conn = HttpConn;
//!     async fn connect(&self, _target: &Target) -> Result<HttpConn, Error> { Ok(HttpConn) }
//! }
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = ZRequestConfig::default();
//! config.connections.insert("users".into(), ConnectionConfig {
//!     protocol: Some(Protocol::Rest),
//!     endpoint: Some("http://users.internal:8080".into()),
//!     ..Default::default()
//! });
//! let resolved = config.resolve()?;
//!
//! let pool = ConnectionPool::new(&resolved.pool);
//! let rest = pool.typed(Http, &resolved.connections)?;
//!
//! let users = resolved.connections[0].target();
//! let conn = rest.acquire(&users).await?; // back to the pool when dropped
//! drop(conn);
//!
//! pool.close().await;
//! # Ok(())
//! # }
//! ```

mod core;
mod eviction;
mod reaper;
mod typed;

pub use self::core::{ConnectionPool, ConnectionPoolBuilder, TargetStats};
pub use self::eviction::LruEviction;
pub use self::typed::TypedPool;

#[cfg(test)]
mod tests;
