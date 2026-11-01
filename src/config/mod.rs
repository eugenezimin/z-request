//! Declarative configuration from YAML and env vars.
//!
//! - [`ConfigSource`] layers YAML (feature `yaml`) and `<PREFIX>__…` env vars into a
//!   [`ZRequestConfig`] and reports structural problems section by section.
//! - [`ZRequestConfig::resolve`] validates it (limits, `hold` range, auth secrets, TLS,
//!   headers, compiled-in features), applies `defaults` to every connection and returns a
//!   [`ResolvedConfig`]. Every problem is reported at once.
//!
//! Hosts that already have a configuration system can embed [`ZRequestConfig`] (it is a plain
//! serde type) and call [`ZRequestConfig::resolve`] themselves.

pub mod duration;
mod env;
mod model;
pub mod size;
mod source;
mod validate;

pub use model::{
    AuthConfig, BackoffConfig, BackoffOverride, BreakerConfig, BreakerOverride, ConnectionConfig,
    DEFAULT_MAX_CONNECTION_AGE, DEFAULT_MAX_RESPONSE_SIZE, DEFAULT_STREAMS_PER_CONNECTION,
    DefaultsConfig, Http2Mode, Limit, PoolConfig, RetryConfig, RetryOverride, Scope, TlsConfig,
    ZRequestConfig,
};
pub use source::ConfigSource;
pub use validate::{
    AuthSettings, ConnectionSettings, Endpoint, MAX_HOLD, MAX_STREAMS_PER_CONNECTION,
    MIN_CONNECTION_AGE, PoolSettings, ResolvedConfig, parse_endpoint,
};
