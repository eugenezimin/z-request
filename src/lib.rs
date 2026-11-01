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
//! ## Overview
//!
//! - [`config`]: the configuration, from YAML and env vars, validated and resolved.
//! - [`Connections`]: every configured connection, built in one call; hands out the REST and
//!   gRPC clients by name, and shuts everything down.
//! - [`pool`]: the one connection pool all connections share.
//! - [`policy`]: retries, circuit breaking, and the call loop both transports use.
//! - [`credentials`]: what each call sends, and the headers that carry it.
//! - `rest` and `grpc`: the transports.
//! - [`telemetry`]: spans and metrics, through the `tracing` and `metrics` facades.
//! - [`spi`]: the interfaces every part implements, to replace any of them.
//!
//! A tokio runtime with time enabled is required.
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
mod connections;
pub mod credentials;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod policy;
pub mod pool;
#[cfg(feature = "rest")]
pub mod rest;
#[cfg(any(feature = "rest", feature = "grpc"))]
mod sharing;
pub mod telemetry;
#[cfg(test)]
mod test_support;
#[cfg(all(feature = "tls", any(feature = "rest", feature = "grpc")))]
mod tls;
mod utils;

pub use connections::{Connections, ConnectionsBuilder};
pub use credentials::AuthResolver;
pub use policy::{BreakerState, ConsecutiveFailures, ExponentialBackoff, Policies};
pub use pool::{ConnectionPool, ConnectionPoolBuilder, LruEviction, TargetStats, TypedPool};
pub use utils::error::{ConfigError, Error, Problem};
pub use utils::spi;
pub use utils::spi::{
    CallContext, INITIATOR_HEADER, Initiator, InitiatorKind, InvalidInitiator, Protocol, Secret,
    Target, TraceContext,
};
