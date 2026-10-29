//! The configuration as written in YAML or env vars.
//!
//! These are plain serde types, so a host can embed [`ZRequestConfig`] in its own configuration
//! instead of using [`ConfigSource`](crate::config::ConfigSource). Either way, call
//! [`ZRequestConfig::resolve`] to validate it and read the secrets.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::config::duration;
use crate::utils::spi::Protocol;

/// The whole z-request configuration: `pool`, `defaults` and `connections`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ZRequestConfig {
    /// The pool shared by every connection.
    pub pool: PoolConfig,
    /// Settings every connection inherits unless it overrides them.
    pub defaults: DefaultsConfig,
    /// The named connections, e.g. `users`, `registry`.
    pub connections: BTreeMap<String, ConnectionConfig>,
}

/// How `min`/`max` limits are counted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Scope {
    /// Each connection has its own `[min, max]`.
    #[default]
    PerUri,
    /// `pool.min`/`pool.max` are the floor and cap across all connections together.
    Total,
}

/// A connection count limit: a number, or `unlimited`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Limit {
    /// At most this many.
    Finite(u32),
    /// No limit.
    Unlimited,
}

impl Limit {
    /// Whether `count` is still below the limit.
    pub fn allows(self, count: u32) -> bool {
        match self {
            Self::Finite(max) => count < max,
            Self::Unlimited => true,
        }
    }

    /// The number, or `None` for `unlimited`.
    pub fn finite(self) -> Option<u32> {
        match self {
            Self::Finite(n) => Some(n),
            Self::Unlimited => None,
        }
    }
}

impl fmt::Display for Limit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Finite(n) => write!(f, "{n}"),
            Self::Unlimited => f.write_str("unlimited"),
        }
    }
}

impl Serialize for Limit {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Finite(n) => s.serialize_u32(*n),
            Self::Unlimited => s.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for Limit {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Number(u32),
            Text(String),
        }
        match Raw::deserialize(d)? {
            Raw::Number(n) => Ok(Self::Finite(n)),
            Raw::Text(t) if t.trim().eq_ignore_ascii_case("unlimited") => Ok(Self::Unlimited),
            Raw::Text(t) => t.trim().parse().map(Self::Finite).map_err(|_| {
                serde::de::Error::custom(format!(
                    "`{t}` is not a limit; use a whole number or `unlimited`"
                ))
            }),
        }
    }
}

/// `pool:`: the one pool shared by every connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolConfig {
    /// `per_uri` or `total`.
    pub scope: Scope,
    /// Connections kept open even when idle: per connection (`per_uri`) or in total (`total`).
    pub min: u32,
    /// Most connections open at once: per connection (`per_uri`) or in total (`total`).
    pub max: Limit,
    /// How long a released connection stays open for the next request (0–60 s).
    #[serde(with = "duration")]
    pub hold: Duration,
    /// How long a request waits for a free connection.
    #[serde(with = "duration")]
    pub acquire_timeout: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            scope: Scope::PerUri,
            min: 0,
            max: Limit::Finite(8),
            hold: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(2),
        }
    }
}

/// `defaults:`: settings every connection inherits.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DefaultsConfig {
    /// Time allowed to open a connection, including TLS.
    #[serde(with = "duration")]
    pub connect_timeout: Duration,
    /// Deadline for one attempt.
    #[serde(with = "duration")]
    pub request_timeout: Duration,
    /// Retries for idempotent calls.
    pub retry: RetryConfig,
    /// Circuit breaker per connection.
    pub breaker: BreakerConfig,
    /// What calls send: `public`, `user`, `env` or `mixed`.
    pub auth: AuthConfig,
}

impl Default for DefaultsConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(3),
            request_timeout: Duration::from_secs(5),
            retry: RetryConfig::default(),
            breaker: BreakerConfig::default(),
            auth: AuthConfig::Public,
        }
    }
}

/// `retry:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryConfig {
    /// Total attempts, counting the first. `1` never retries.
    pub max_attempts: u32,
    /// Waits between attempts.
    pub backoff: BackoffConfig,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            backoff: BackoffConfig::default(),
        }
    }
}

/// `retry.backoff:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffConfig {
    /// Wait before the first retry.
    #[serde(with = "duration")]
    pub initial: Duration,
    /// Cap on any wait.
    #[serde(with = "duration")]
    pub max: Duration,
    /// Each wait is the previous one times this.
    pub multiplier: f64,
    /// Shorten each wait randomly to 50–100 %, so clients don't retry in lockstep.
    pub jitter: bool,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(2),
            multiplier: 2.0,
            jitter: true,
        }
    }
}

/// `breaker:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BreakerConfig {
    /// Consecutive failures that open the breaker.
    pub failures: u32,
    /// How long it stays open before one trial call.
    #[serde(with = "duration")]
    pub open_for: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failures: 5,
            open_for: Duration::from_secs(30),
        }
    }
}

/// `auth:`: what a connection's calls send.
///
/// Short form `auth: public` / `auth: user`; long form `auth: { mode: env, envvar: NAME }` for
/// the modes that need a secret.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum AuthConfig {
    /// No credentials.
    #[default]
    Public,
    /// The caller's own token; a call without one fails.
    User,
    /// The service secret from the env var `envvar`.
    Env {
        /// The env var holding the secret.
        envvar: String,
    },
    /// The caller's token if the call has one, otherwise the secret from `envvar`.
    Mixed {
        /// The env var holding the secret.
        envvar: String,
    },
}

impl AuthConfig {
    /// The mode's name: `public`, `user`, `env` or `mixed`.
    pub fn mode(&self) -> &'static str {
        match self {
            Self::Public => "public",
            Self::User => "user",
            Self::Env { .. } => "env",
            Self::Mixed { .. } => "mixed",
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Public,
    User,
    Env,
    Mixed,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthLong {
    mode: Mode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    envvar: Option<String>,
}

impl Serialize for AuthConfig {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Public => Mode::Public.serialize(s),
            Self::User => Mode::User.serialize(s),
            Self::Env { envvar } => AuthLong {
                mode: Mode::Env,
                envvar: Some(envvar.clone()),
            }
            .serialize(s),
            Self::Mixed { envvar } => AuthLong {
                mode: Mode::Mixed,
                envvar: Some(envvar.clone()),
            }
            .serialize(s),
        }
    }
}

impl<'de> Deserialize<'de> for AuthConfig {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Short(String),
            Long(AuthLong),
        }
        let need_envvar = |mode: &str, envvar: Option<String>| {
            envvar.ok_or_else(|| {
                D::Error::custom(format!(
                    "auth mode `{mode}` needs `envvar`: auth: {{ mode: {mode}, envvar: NAME }}"
                ))
            })
        };
        match Raw::deserialize(d).map_err(|_| {
            D::Error::custom(
                "auth must be `public`, `user`, or `{ mode: env|mixed, envvar: NAME }`",
            )
        })? {
            Raw::Short(s) => match s.trim() {
                "public" => Ok(Self::Public),
                "user" => Ok(Self::User),
                m @ ("env" | "mixed") => Err(D::Error::custom(format!(
                    "auth mode `{m}` needs the long form: auth: {{ mode: {m}, envvar: NAME }}"
                ))),
                other => Err(D::Error::custom(format!(
                    "unknown auth mode `{other}`; use public, user, env or mixed"
                ))),
            },
            Raw::Long(AuthLong { mode, envvar }) => match mode {
                Mode::Public => Ok(Self::Public),
                Mode::User => Ok(Self::User),
                Mode::Env => Ok(Self::Env {
                    envvar: need_envvar("env", envvar)?,
                }),
                Mode::Mixed => Ok(Self::Mixed {
                    envvar: need_envvar("mixed", envvar)?,
                }),
            },
        }
    }
}

/// `connections.<name>:`: one peer.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConnectionConfig {
    /// `grpc` or `rest`. Required.
    pub protocol: Option<Protocol>,
    /// The peer's URL (for REST, the base URL). Required.
    pub endpoint: Option<String>,
    /// Overrides `pool.min` for this connection (always per connection).
    pub min: Option<u32>,
    /// Overrides `pool.max` for this connection (always per connection).
    pub max: Option<Limit>,
    /// Overrides `defaults.connect_timeout`.
    #[serde(with = "duration::option", skip_serializing_if = "Option::is_none")]
    pub connect_timeout: Option<Duration>,
    /// Overrides `defaults.request_timeout`.
    #[serde(with = "duration::option", skip_serializing_if = "Option::is_none")]
    pub request_timeout: Option<Duration>,
    /// Overrides fields of `defaults.retry`.
    pub retry: Option<RetryOverride>,
    /// Overrides fields of `defaults.breaker`.
    pub breaker: Option<BreakerOverride>,
    /// Overrides `defaults.auth`.
    pub auth: Option<AuthConfig>,
    /// TLS settings for `https://` endpoints.
    pub tls: Option<TlsConfig>,
    /// REST only: headers added to every request.
    #[serde(
        deserialize_with = "string_map",
        skip_serializing_if = "BTreeMap::is_empty"
    )]
    pub headers: BTreeMap<String, String>,
}

/// Field-by-field override of `defaults.retry`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetryOverride {
    /// Overrides `max_attempts`.
    pub max_attempts: Option<u32>,
    /// Overrides fields of `backoff`.
    pub backoff: Option<BackoffOverride>,
}

/// Field-by-field override of `defaults.retry.backoff`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackoffOverride {
    /// Overrides `initial`.
    #[serde(with = "duration::option", skip_serializing_if = "Option::is_none")]
    pub initial: Option<Duration>,
    /// Overrides `max`.
    #[serde(with = "duration::option", skip_serializing_if = "Option::is_none")]
    pub max: Option<Duration>,
    /// Overrides `multiplier`.
    pub multiplier: Option<f64>,
    /// Overrides `jitter`.
    pub jitter: Option<bool>,
}

/// Field-by-field override of `defaults.breaker`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BreakerOverride {
    /// Overrides `failures`.
    pub failures: Option<u32>,
    /// Overrides `open_for`.
    #[serde(with = "duration::option", skip_serializing_if = "Option::is_none")]
    pub open_for: Option<Duration>,
}

/// `tls:`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TlsConfig {
    /// A CA certificate (PEM) to trust in addition to the system roots.
    pub ca_file: Option<PathBuf>,
    /// Client certificate (PEM) for mTLS; requires `client_key_file`.
    pub client_cert_file: Option<PathBuf>,
    /// Client private key (PEM) for mTLS; requires `client_cert_file`.
    pub client_key_file: Option<PathBuf>,
    /// The name expected on the server's certificate, when it differs from the endpoint's host.
    pub server_name: Option<String>,
}

/// Header values may arrive as numbers or booleans from env vars; keep them as text.
fn string_map<'de, D: Deserializer<'de>>(d: D) -> Result<BTreeMap<String, String>, D::Error> {
    let raw = BTreeMap::<String, serde_json::Value>::deserialize(d)?;
    raw.into_iter()
        .map(|(k, v)| match v {
            serde_json::Value::String(s) => Ok((k, s)),
            serde_json::Value::Number(n) => Ok((k, n.to_string())),
            serde_json::Value::Bool(b) => Ok((k, b.to_string())),
            _ => Err(serde::de::Error::custom(format!(
                "header `{k}` must be a single text value"
            ))),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn auth_short_and_long_forms() {
        let parse = |v| serde_json::from_value::<AuthConfig>(v);
        assert_eq!(parse(json!("public")).unwrap(), AuthConfig::Public);
        assert_eq!(parse(json!("user")).unwrap(), AuthConfig::User);
        assert_eq!(
            parse(json!({"mode": "mixed", "envvar": "S"})).unwrap(),
            AuthConfig::Mixed { envvar: "S".into() }
        );
        assert!(
            parse(json!("env"))
                .unwrap_err()
                .to_string()
                .contains("long form")
        );
        assert!(
            parse(json!({"mode": "env"}))
                .unwrap_err()
                .to_string()
                .contains("needs `envvar`")
        );
        assert!(
            parse(json!("magic"))
                .unwrap_err()
                .to_string()
                .contains("unknown auth mode")
        );
    }

    #[test]
    fn limits_accept_numbers_text_and_unlimited() {
        let parse = |v| serde_json::from_value::<Limit>(v);
        assert_eq!(parse(json!(16)).unwrap(), Limit::Finite(16));
        assert_eq!(parse(json!("16")).unwrap(), Limit::Finite(16));
        assert_eq!(parse(json!("unlimited")).unwrap(), Limit::Unlimited);
        assert!(parse(json!("lots")).is_err());
        assert!(Limit::Unlimited.allows(u32::MAX));
        assert!(!Limit::Finite(2).allows(2));
    }

    #[test]
    fn defaults_match_the_documented_values() {
        let c = ZRequestConfig::default();
        assert_eq!(c.pool.scope, Scope::PerUri);
        assert_eq!((c.pool.min, c.pool.max), (0, Limit::Finite(8)));
        assert_eq!(c.pool.hold, Duration::from_secs(30));
        assert_eq!(c.defaults.retry.max_attempts, 3);
        assert_eq!(c.defaults.auth, AuthConfig::Public);
    }
}
