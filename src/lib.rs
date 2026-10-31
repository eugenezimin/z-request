//! # z-request
//!
//! Pooled outbound clients for services: any number of named **connections** to REST and gRPC
//! peers, configured declaratively from YAML and env vars, sending the **end user's own
//! credentials** (or, for calls without a user, nothing or a server-side secret).
//!
//! z-request is independent of z-auth and z-event and is meant to sit next to them: the host
//! passes the user's (z-auth-verified) token in a [`CallContext`], and can hand a dedicated gRPC
//! channel to z-event's subscriber.
//!
//! ## Status
//!
//! Build step 5 of 6: interfaces ([`spi`]), errors, configuration, the connection [`pool`], the
//! retry/breaker [`policy`] with the shared call loop, [`credentials`], and the REST (`rest`) and
//! gRPC (`grpc`) transports. One build step remains.
//!
//! The pool needs a tokio runtime with time enabled.
//!
//! ## Features
//!
//! - `rest`: REST transport (reqwest, HTTP/1.1): the `rest` module.
//! - `grpc`: gRPC transport (tonic, HTTP/2): the `grpc` module.
//! - `tls`: `https://` endpoints, custom CAs, client certificates.
//! - `yaml`: YAML configuration files (`ConfigSource::yaml_file` / `yaml_str`).
//!
//! All four are on by default.

pub mod config;
pub mod credentials;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod policy;
pub mod pool;
#[cfg(feature = "rest")]
pub mod rest;
#[cfg(test)]
mod test_support;
#[cfg(all(feature = "tls", any(feature = "rest", feature = "grpc")))]
mod tls;
mod utils;

pub use credentials::AuthResolver;
pub use policy::{BreakerState, ConsecutiveFailures, ExponentialBackoff, Policies};
pub use pool::{ConnectionPool, ConnectionPoolBuilder, LruEviction, TargetStats, TypedPool};
pub use utils::error::{ConfigError, Error, Problem};
pub use utils::spi;
pub use utils::spi::{
    CallContext, INITIATOR_HEADER, Initiator, InitiatorKind, InvalidInitiator, Protocol, Secret,
    Target, TraceContext,
};
