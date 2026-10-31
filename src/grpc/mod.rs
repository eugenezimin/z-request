//! The gRPC transport (feature `grpc`): tonic over HTTP/2, `https://` with feature `tls`.
//!
//! - [`GrpcConnector`] opens pooled [`GrpcConnection`]s: one tonic channel (one HTTP/2 socket)
//!   each. A call holds its connection exclusively, as a REST request does, so the pool's
//!   `max` is both the sockets and the concurrent calls to a peer.
//! - [`GrpcClient::call`] runs one call through [`Policies::run`](crate::policy::Policies::run)
//!   on a [`GrpcChannel`], which a generated client takes as is; the channel adds the call's
//!   credentials, trace context, initiator and `grpc-timeout`.
//! - [`GrpcClient::dedicated`] hands out a [`DedicatedChannel`] for a long-lived stream, such
//!   as a subscription.
//!
//! ```no_run
//! use std::sync::Arc;
//!
//! use z_request::config::{ConnectionConfig, ZRequestConfig};
//! use z_request::grpc::{GrpcClient, GrpcConnector};
//! use z_request::{AuthResolver, CallContext, ConnectionPool, ConsecutiveFailures, Protocol};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let mut config = ZRequestConfig::default();
//! config.connections.insert("users".into(), ConnectionConfig {
//!     protocol: Some(Protocol::Grpc),
//!     endpoint: Some("http://users.internal:9090".into()),
//!     ..Default::default()
//! });
//! let resolved = config.resolve()?;
//! let users = &resolved.connections[0];
//!
//! let pool = ConnectionPool::new(&resolved.pool);
//! let grpc = pool.typed(GrpcConnector::new(&resolved.connections)?, &resolved.connections)?;
//! let auth = Arc::new(AuthResolver::new(&resolved.connections));
//! let breaker = Arc::new(ConsecutiveFailures::new(&resolved.connections));
//! let client = GrpcClient::new(users, grpc, auth, breaker)?;
//!
//! let ctx = CallContext::user("<the caller's token>", Default::default());
//! let _sent = client
//!     .call(&ctx, true, |channel| async move {
//!         // e.g. `UsersClient::new(channel).get_user(request).await`
//!         let _ = channel;
//!         Ok::<_, z_request::grpc::tonic::Status>(())
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod client;
mod connector;

/// The tonic version this transport is built on, for generated clients to match.
pub use tonic;

pub use self::client::{CallMetadata, DedicatedChannel, GrpcChannel, GrpcClient};
pub use self::connector::{GrpcConnection, GrpcConnector};

#[cfg(test)]
mod tests;
