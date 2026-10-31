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
//! Build step 2 of 6: interfaces ([`spi`]), errors, configuration and the connection [`pool`].
//! The retry/breaker policies, credentials and the REST/gRPC transports follow.
//!
//! The pool needs a tokio runtime with time enabled.
//!
//! ## Features
//!
//! - `rest`: REST transport (reqwest).
//! - `grpc`: gRPC transport (tonic).
//! - `tls`: `https://` endpoints, custom CAs, client certificates.
//! - `yaml`: YAML configuration files (`ConfigSource::yaml_file` / `yaml_str`).
//!
//! All four are on by default.

pub mod config;
pub mod pool;
mod utils;

pub use pool::{ConnectionPool, ConnectionPoolBuilder, LruEviction, TargetStats, TypedPool};
pub use utils::error::{ConfigError, Error, Problem};
pub use utils::spi;
pub use utils::spi::{
    CallContext, INITIATOR_HEADER, Initiator, InitiatorKind, InvalidInitiator, Protocol, Secret,
    Target, TraceContext,
};
