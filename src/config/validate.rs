//! Validation and resolution: defaults merged into each connection, secrets read from env,
//! every problem reported at once.

use std::time::Duration;

use crate::config::model::{
    AuthConfig, BackoffConfig, BreakerConfig, ConnectionConfig, Limit, RetryConfig, Scope,
    TlsConfig, ZRequestConfig,
};
use crate::utils::error::{ConfigError, Problem};
use crate::utils::spi::{Protocol, Secret, Target};

/// The longest `pool.hold` allowed.
pub const MAX_HOLD: Duration = Duration::from_secs(60);

/// Headers z-request sets itself; configuring them is an error.
const RESERVED_HEADERS: [&str; 4] = ["authorization", "traceparent", "tracestate", "x-request-id"];

/// A validated configuration, ready for building pools and connections.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// The pool settings.
    pub pool: PoolSettings,
    /// Every connection, sorted by name.
    pub connections: Vec<ConnectionSettings>,
}

/// Validated `pool:` settings.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolSettings {
    /// `per_uri` or `total`.
    pub scope: Scope,
    /// Under `total`: connections kept open across all connections. Under `per_uri` this is
    /// already applied to each connection's `min`.
    pub min: u32,
    /// Under `total`: the cap across all connections. Under `per_uri` it is applied to each
    /// connection's `max`.
    pub max: Limit,
    /// How long a released connection stays open.
    pub hold: Duration,
    /// How long a request waits for a free connection.
    pub acquire_timeout: Duration,
}

/// What a connection's calls send, with the secret already read.
#[derive(Clone, PartialEq, Eq)]
pub enum AuthSettings {
    /// No credentials.
    Public,
    /// The caller's token; a call without one fails.
    User,
    /// The service secret.
    Env(Secret),
    /// The caller's token if the call has one, otherwise the service secret.
    Mixed(Secret),
}

impl std::fmt::Debug for AuthSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Public => "Public",
            Self::User => "User",
            Self::Env(_) => "Env(<redacted>)",
            Self::Mixed(_) => "Mixed(<redacted>)",
        })
    }
}

/// A parsed endpoint URL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The URL as configured.
    pub url: String,
    /// `true` for `https://`.
    pub secure: bool,
    /// Host name or IP (IPv6 without brackets).
    pub host: String,
    /// Port, explicit or the scheme's default.
    pub port: u16,
    /// Path after the authority, without a trailing `/` (empty if none).
    pub base_path: String,
}

/// One validated connection with every default applied.
#[derive(Debug, Clone)]
pub struct ConnectionSettings {
    /// The connection's name.
    pub name: String,
    /// `grpc` or `rest`.
    pub protocol: Protocol,
    /// The peer's endpoint.
    pub endpoint: Endpoint,
    /// Connections kept open for this connection.
    pub min: u32,
    /// Most connections open for this connection.
    pub max: Limit,
    /// Time allowed to connect.
    pub connect_timeout: Duration,
    /// Deadline per attempt.
    pub request_timeout: Duration,
    /// Retry settings.
    pub retry: RetryConfig,
    /// Breaker settings.
    pub breaker: BreakerConfig,
    /// What calls send.
    pub auth: AuthSettings,
    /// TLS settings, if any.
    pub tls: Option<TlsConfig>,
    /// REST only: fixed headers.
    pub headers: Vec<(String, String)>,
}

impl ConnectionSettings {
    /// The pool target for this connection.
    pub fn target(&self) -> Target {
        Target::new(self.name.as_str(), self.endpoint.url.as_str())
    }
}

impl ZRequestConfig {
    /// Validates the configuration and reads the secrets from the process environment.
    pub fn resolve(&self) -> Result<ResolvedConfig, ConfigError> {
        self.resolve_with(|name| std::env::var(name).ok())
    }

    /// Like [`resolve`](Self::resolve), with secrets looked up through `lookup` (useful in tests).
    pub fn resolve_with(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<ResolvedConfig, ConfigError> {
        let mut p = Problems::default();
        let pool = &self.pool;

        if pool.hold > MAX_HOLD {
            p.add(
                "pool.hold",
                format!("must be between 0s and 60s, got {:?}", pool.hold),
            );
        }
        if pool.acquire_timeout.is_zero() {
            p.add("pool.acquire_timeout", "must be greater than 0s");
        }
        if let Some(max) = pool.max.finite()
            && pool.min > max
        {
            p.add("pool.min", format!("is {} but pool.max is {max}", pool.min));
        }

        let d = &self.defaults;
        positive(&mut p, "defaults.connect_timeout", d.connect_timeout);
        positive(&mut p, "defaults.request_timeout", d.request_timeout);
        check_retry(&mut p, "defaults.retry", &d.retry);
        check_breaker(&mut p, "defaults.breaker", &d.breaker);

        let mut connections = Vec::new();
        for (name, c) in &self.connections {
            if let Some(settings) = resolve_connection(&mut p, name, c, self, &lookup) {
                connections.push(settings);
            }
        }

        if pool.scope == Scope::Total {
            let own_min: u64 = self
                .connections
                .values()
                .filter_map(|c| c.min)
                .map(u64::from)
                .sum();
            if let Some(max) = pool.max.finite()
                && own_min > u64::from(max)
            {
                p.add(
                    "connections",
                    format!("the connections' own `min` add up to {own_min}, above pool.max {max}"),
                );
            }
        }

        match ConfigError::from_problems(p.0) {
            Some(e) => Err(e),
            None => Ok(ResolvedConfig {
                pool: PoolSettings {
                    scope: pool.scope,
                    min: pool.min,
                    max: pool.max,
                    hold: pool.hold,
                    acquire_timeout: pool.acquire_timeout,
                },
                connections,
            }),
        }
    }
}

#[derive(Default)]
struct Problems(Vec<Problem>);

impl Problems {
    fn add(&mut self, path: impl Into<String>, message: impl Into<String>) {
        self.0.push(Problem {
            path: path.into(),
            message: message.into(),
        });
    }
}

fn positive(p: &mut Problems, path: &str, d: Duration) {
    if d.is_zero() {
        p.add(path, "must be greater than 0s");
    }
}

fn check_retry(p: &mut Problems, path: &str, r: &RetryConfig) {
    if r.max_attempts == 0 {
        p.add(
            format!("{path}.max_attempts"),
            "must be at least 1 (1 = never retry)",
        );
    }
    check_backoff(p, &format!("{path}.backoff"), &r.backoff);
}

fn check_backoff(p: &mut Problems, path: &str, b: &BackoffConfig) {
    positive(p, &format!("{path}.initial"), b.initial);
    if b.initial > b.max {
        p.add(
            format!("{path}.max"),
            format!("{:?} is below initial {:?}", b.max, b.initial),
        );
    }
    if !(b.multiplier.is_finite() && b.multiplier >= 1.0) {
        p.add(
            format!("{path}.multiplier"),
            "must be a number of at least 1.0",
        );
    }
}

fn check_breaker(p: &mut Problems, path: &str, b: &BreakerConfig) {
    if b.failures == 0 {
        p.add(format!("{path}.failures"), "must be at least 1");
    }
    positive(p, &format!("{path}.open_for"), b.open_for);
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn valid_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn resolve_connection(
    p: &mut Problems,
    name: &str,
    c: &ConnectionConfig,
    root: &ZRequestConfig,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Option<ConnectionSettings> {
    let at = |field: &str| format!("connections.{name}.{field}");
    let before = p.0.len();

    if !valid_name(name) {
        p.add(
            format!("connections.{name}"),
            "names must be lower-case letters, digits and `_`, starting with a letter (they map to env vars)",
        );
    }

    let protocol = match c.protocol {
        None => {
            p.add(at("protocol"), "is required (grpc or rest)");
            None
        }
        Some(Protocol::Grpc) if !cfg!(feature = "grpc") => {
            p.add(at("protocol"), "grpc needs the `grpc` feature of z-request");
            None
        }
        Some(Protocol::Rest) if !cfg!(feature = "rest") => {
            p.add(at("protocol"), "rest needs the `rest` feature of z-request");
            None
        }
        Some(protocol) => Some(protocol),
    };

    let endpoint = match c.endpoint.as_deref() {
        None => {
            p.add(at("endpoint"), "is required");
            None
        }
        Some(url) => match parse_endpoint(url) {
            Ok(e) => {
                if e.secure && !cfg!(feature = "tls") {
                    p.add(
                        at("endpoint"),
                        "https:// needs the `tls` feature of z-request",
                    );
                }
                if protocol == Some(Protocol::Grpc) && !e.base_path.is_empty() {
                    p.add(
                        at("endpoint"),
                        "a gRPC endpoint has no path; use scheme://host:port",
                    );
                }
                Some(e)
            }
            Err(msg) => {
                p.add(at("endpoint"), msg);
                None
            }
        },
    };

    // Limits: under per_uri the pool's min/max are each connection's defaults; under total
    // they cap the whole pool and a connection is only bounded by its own values.
    let (min, max) = match root.pool.scope {
        Scope::PerUri => (
            c.min.unwrap_or(root.pool.min),
            c.max.unwrap_or(root.pool.max),
        ),
        Scope::Total => (c.min.unwrap_or(0), c.max.unwrap_or(Limit::Unlimited)),
    };
    if let Some(m) = max.finite() {
        if min > m {
            p.add(
                at("min"),
                format!("is {min} but this connection's max is {m}"),
            );
        }
        if m == 0 {
            p.add(at("max"), "must be at least 1 (or `unlimited`)");
        }
    }

    let d = &root.defaults;
    let connect_timeout = c.connect_timeout.unwrap_or(d.connect_timeout);
    let request_timeout = c.request_timeout.unwrap_or(d.request_timeout);
    positive(p, &at("connect_timeout"), connect_timeout);
    positive(p, &at("request_timeout"), request_timeout);

    let mut retry = d.retry.clone();
    if let Some(o) = &c.retry {
        if let Some(v) = o.max_attempts {
            retry.max_attempts = v;
        }
        if let Some(b) = &o.backoff {
            retry.backoff.initial = b.initial.unwrap_or(retry.backoff.initial);
            retry.backoff.max = b.max.unwrap_or(retry.backoff.max);
            retry.backoff.multiplier = b.multiplier.unwrap_or(retry.backoff.multiplier);
            retry.backoff.jitter = b.jitter.unwrap_or(retry.backoff.jitter);
        }
        check_retry(p, &at("retry"), &retry);
    }
    let mut breaker = d.breaker.clone();
    if let Some(o) = &c.breaker {
        breaker.failures = o.failures.unwrap_or(breaker.failures);
        breaker.open_for = o.open_for.unwrap_or(breaker.open_for);
        check_breaker(p, &at("breaker"), &breaker);
    }

    let auth_path = if c.auth.is_some() {
        at("auth")
    } else {
        "defaults.auth".into()
    };
    let auth = match c.auth.as_ref().unwrap_or(&d.auth) {
        AuthConfig::Public => Some(AuthSettings::Public),
        AuthConfig::User => Some(AuthSettings::User),
        AuthConfig::Env { envvar } => {
            secret(p, &auth_path, name, envvar, lookup).map(AuthSettings::Env)
        }
        AuthConfig::Mixed { envvar } => {
            secret(p, &auth_path, name, envvar, lookup).map(AuthSettings::Mixed)
        }
    };

    if let Some(tls) = &c.tls {
        if !cfg!(feature = "tls") {
            p.add(at("tls"), "needs the `tls` feature of z-request");
        }
        if endpoint.as_ref().is_some_and(|e| !e.secure) {
            p.add(
                at("tls"),
                "TLS settings on an http:// endpoint; use https://",
            );
        }
        if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
            p.add(
                at("tls"),
                "client_cert_file and client_key_file go together (mTLS)",
            );
        }
    }

    let mut headers = Vec::new();
    if !c.headers.is_empty() && protocol == Some(Protocol::Grpc) {
        p.add(at("headers"), "headers are for REST connections only");
    }
    for (k, v) in &c.headers {
        let lower = k.to_ascii_lowercase();
        if RESERVED_HEADERS.contains(&lower.as_str()) {
            p.add(
                format!("{}.{k}", at("headers")),
                "is set by z-request per call (credentials and trace context)",
            );
        } else if k.is_empty()
            || !k
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            p.add(
                format!("{}.{k}", at("headers")),
                "is not a valid header name",
            );
        } else if v.contains(['\r', '\n']) {
            p.add(
                format!("{}.{k}", at("headers")),
                "value must be a single line",
            );
        } else {
            headers.push((k.clone(), v.clone()));
        }
    }

    if p.0.len() != before {
        return None;
    }
    Some(ConnectionSettings {
        name: name.to_string(),
        protocol: protocol?,
        endpoint: endpoint?,
        min,
        max,
        connect_timeout,
        request_timeout,
        retry,
        breaker,
        auth: auth?,
        tls: c.tls.clone(),
        headers,
    })
}

fn secret(
    p: &mut Problems,
    path: &str,
    connection: &str,
    envvar: &str,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Option<Secret> {
    if !valid_env_name(envvar) {
        p.add(
            format!("{path}.envvar"),
            format!("`{envvar}` is not a valid env var name"),
        );
        return None;
    }
    match lookup(envvar) {
        Some(v) if !v.trim().is_empty() => Some(Secret::new(v.trim())),
        _ => {
            p.add(
                format!("{path}.envvar"),
                format!(
                    "env var `{envvar}` (secret for connection `{connection}`) is not set or empty"
                ),
            );
            None
        }
    }
}

/// Parses `http(s)://host[:port][/path]`.
pub fn parse_endpoint(url: &str) -> Result<Endpoint, String> {
    let (secure, rest) = if let Some(r) = url.strip_prefix("https://") {
        (true, r)
    } else if let Some(r) = url.strip_prefix("http://") {
        (false, r)
    } else {
        return Err(format!("`{url}` must start with http:// or https://"));
    };
    let (authority, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    if authority.contains('@') {
        return Err("credentials in the URL are not allowed; use `auth`".into());
    }
    if rest.contains(['?', '#']) {
        return Err(format!("`{url}`: no query or fragment in an endpoint"));
    }
    let (host, port) = if let Some(v6) = authority.strip_prefix('[') {
        let (h, tail) = v6
            .split_once(']')
            .ok_or_else(|| format!("`{url}`: unclosed `[`"))?;
        (h.to_string(), tail.strip_prefix(':'))
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) => (h.to_string(), Some(p)),
            None => (authority.to_string(), None),
        }
    };
    if host.is_empty() {
        return Err(format!("`{url}` has no host"));
    }
    let port = match port {
        Some(p) => p
            .parse::<u16>()
            .map_err(|_| format!("`{url}`: invalid port `{p}`"))?,
        None if secure => 443,
        None => 80,
    };
    Ok(Endpoint {
        url: url.to_string(),
        secure,
        host,
        port,
        base_path: path.trim_end_matches('/').to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoints() {
        let e = parse_endpoint("https://users.internal:9090").unwrap();
        assert_eq!(
            (e.secure, e.host.as_str(), e.port, e.base_path.as_str()),
            (true, "users.internal", 9090, "")
        );
        let e = parse_endpoint("http://registry/api/v1/").unwrap();
        assert_eq!((e.port, e.base_path.as_str()), (80, "/api/v1"));
        let e = parse_endpoint("https://[::1]:8443").unwrap();
        assert_eq!((e.host.as_str(), e.port), ("::1", 8443));
        assert!(parse_endpoint("users:9090").is_err());
        assert!(parse_endpoint("http://u:p@host").is_err());
        assert!(parse_endpoint("http://host:99999").is_err());
        assert!(parse_endpoint("http://host/x?y=1").is_err());
        assert!(parse_endpoint("http://:80").is_err());
    }
}
