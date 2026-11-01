//! Configuration: layering, auth forms and every validation rule.

// Helpers and imports are shared by tests gated on different features.
#![allow(unused_imports, dead_code)]

use std::time::Duration;

use z_request::config::{
    AuthConfig, AuthSettings, ConfigSource, Limit, ResolvedConfig, Scope, ZRequestConfig,
};
use z_request::{ConfigError, Protocol};

fn env(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn secrets(name: &str) -> Option<String> {
    match name {
        "USERS_SECRET" => Some("s3cr3t".into()),
        "EMPTY_SECRET" => Some("  ".into()),
        "MULTILINE_SECRET" => Some("abc\nx-evil: 1".into()),
        _ => None,
    }
}

/// Every problem message, as `path: message` lines.
fn problems(e: &ConfigError) -> Vec<String> {
    e.problems().iter().map(ToString::to_string).collect()
}

fn has(e: &ConfigError, needle: &str) -> bool {
    problems(e).iter().any(|p| p.contains(needle))
}

#[cfg(feature = "yaml")]
const EXAMPLE: &str = r#"
pool:
  scope: per_uri
  min: 0
  max: 8
  hold: 30s
  acquire_timeout: 2s

defaults:
  connect_timeout: 3s
  request_timeout: 5s
  retry:
    max_attempts: 3
    backoff:
      initial: 100ms
      max: 2s
      multiplier: 2.0
      jitter: true
  breaker:
    failures: 5
    open_for: 30s
  auth: public

connections:
  users:
    protocol: grpc
    endpoint: https://users.internal:9090
    min: 2
    max: 16
    auth:
      mode: mixed
      envvar: USERS_SECRET
    tls:
      ca_file: /etc/ssl/legaldocs-ca.pem
      client_cert_file: /etc/ssl/counterparties.pem
      client_key_file: /etc/ssl/counterparties.key
      server_name: users.internal
  registry:
    protocol: rest
    endpoint: https://registry.example/api
    request_timeout: 10s
    headers:
      Accept: application/json
"#;

#[cfg(feature = "yaml")]
fn resolve_yaml(yaml: &str, vars: &[(&str, &str)]) -> Result<ResolvedConfig, ConfigError> {
    ConfigSource::new()
        .yaml_str(yaml)?
        .with_env_vars("APP", env(vars))
        .load()?
        .resolve_with(secrets)
}

#[cfg(all(feature = "yaml", feature = "grpc", feature = "rest", feature = "tls"))]
#[test]
fn the_documented_example_resolves() {
    let r = resolve_yaml(EXAMPLE, &[]).unwrap();
    assert_eq!(r.pool.scope, Scope::PerUri);
    assert_eq!(r.pool.hold, Duration::from_secs(30));

    let names: Vec<&str> = r.connections.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, ["registry", "users"], "sorted by name");

    let registry = &r.connections[0];
    assert_eq!(registry.protocol, Protocol::Rest);
    assert_eq!(
        (registry.min, registry.max),
        (0, Limit::Finite(8)),
        "pool limits per connection"
    );
    assert_eq!(
        registry.request_timeout,
        Duration::from_secs(10),
        "override"
    );
    assert_eq!(
        registry.connect_timeout,
        Duration::from_secs(3),
        "inherited"
    );
    assert_eq!(registry.auth, AuthSettings::Public);
    assert_eq!(registry.endpoint.base_path, "/api");
    assert_eq!(
        registry.headers,
        vec![("Accept".to_string(), "application/json".to_string())]
    );

    let users = &r.connections[1];
    assert_eq!((users.min, users.max), (2, Limit::Finite(16)));
    assert!(matches!(&users.auth, AuthSettings::Mixed(s) if s.expose() == "s3cr3t"));
    assert!(
        !format!("{users:?}").contains("s3cr3t"),
        "secrets never print"
    );
    assert_eq!(users.target().name(), "users");
}

#[cfg(all(feature = "yaml", feature = "grpc", feature = "rest", feature = "tls"))]
#[test]
fn env_wins_over_yaml_at_any_depth() {
    let r = resolve_yaml(
        EXAMPLE,
        &[
            ("APP__POOL__HOLD", "15s"),
            ("APP__POOL__MAX", "unlimited"),
            ("APP__DEFAULTS__RETRY__BACKOFF__MULTIPLIER", "1.5"),
            ("APP__CONNECTIONS__USERS__MAX", "32"),
            ("APP__CONNECTIONS__USERS__AUTH", "user"),
            ("APP__CONNECTIONS__REGISTRY__HEADERS__X_API_VERSION", "2"),
        ],
    )
    .unwrap();
    assert_eq!(r.pool.hold, Duration::from_secs(15));
    assert_eq!(r.pool.max, Limit::Unlimited);
    let registry = &r.connections[0];
    assert_eq!(
        registry.max,
        Limit::Unlimited,
        "pool.max applies per connection"
    );
    assert_eq!(registry.retry.backoff.multiplier, 1.5);
    assert!(
        registry
            .headers
            .contains(&("x_api_version".to_string(), "2".to_string()))
    );
    let users = &r.connections[1];
    assert_eq!(users.max, Limit::Finite(32));
    assert_eq!(
        users.auth,
        AuthSettings::User,
        "a short form replaces the long one"
    );
}

#[cfg(feature = "rest")]
#[test]
fn a_connection_can_be_defined_from_env_alone() {
    let r = ConfigSource::new()
        .with_env_vars(
            "APP",
            env(&[
                ("APP__CONNECTIONS__REGISTRY__PROTOCOL", "rest"),
                (
                    "APP__CONNECTIONS__REGISTRY__ENDPOINT",
                    "http://registry:8080",
                ),
                ("APP__CONNECTIONS__REGISTRY__AUTH__MODE", "env"),
                ("APP__CONNECTIONS__REGISTRY__AUTH__ENVVAR", "USERS_SECRET"),
            ]),
        )
        .load()
        .unwrap()
        .resolve_with(secrets)
        .unwrap();
    assert_eq!(r.connections.len(), 1);
    assert_eq!(r.connections[0].endpoint.port, 8080);
    assert!(matches!(r.connections[0].auth, AuthSettings::Env(_)));
}

#[cfg(feature = "yaml")]
#[test]
fn structural_problems_are_reported_per_section() {
    let e = ConfigSource::new()
        .yaml_str(
            r#"
pool: { hold: 30, sizee: 1 }
defaults: { auth: env }
connections:
  users: { protocol: grpc, endpoint: "http://u:1", colour: blue }
  other: { protocol: smtp }
extra: true
"#,
        )
        .unwrap()
        .load()
        .unwrap_err();
    assert!(has(&e, "extra: unknown section"), "{e}");
    assert!(has(&e, "pool:"), "{e}");
    assert!(
        has(&e, "defaults: auth mode `env` needs the long form"),
        "{e}"
    );
    assert!(has(&e, "connections.users: unknown field `colour`"), "{e}");
    assert!(has(&e, "connections.other:"), "{e}");
    assert_eq!(e.problems().len(), 5, "{e}");
}

#[test]
fn every_semantic_rule_is_reported_at_once() {
    let mut c = ZRequestConfig::default();
    c.pool.hold = Duration::from_secs(61);
    c.pool.acquire_timeout = Duration::ZERO;
    c.pool.min = 9;
    c.defaults.retry.max_attempts = 0;
    c.defaults.retry.backoff.initial = Duration::from_secs(5);
    c.defaults.retry.backoff.multiplier = 0.5;
    c.defaults.breaker.failures = 0;
    c.defaults.request_timeout = Duration::ZERO;

    let bad = |f: &dyn Fn(&mut z_request::config::ConnectionConfig)| {
        let mut conn = z_request::config::ConnectionConfig::default();
        f(&mut conn);
        conn
    };
    c.connections.insert(
        "Users".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://users".into());
        }),
    );
    c.connections.insert(
        "noproto".into(),
        bad(&|x| x.endpoint = Some("http://a".into())),
    );
    c.connections.insert(
        "noendpoint".into(),
        bad(&|x| x.protocol = Some(Protocol::Rest)),
    );
    c.connections.insert(
        "badurl".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("ftp://a".into());
        }),
    );
    c.connections.insert(
        "limits".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.min = Some(5);
            x.max = Some(Limit::Finite(2));
        }),
    );
    c.connections.insert(
        "secret".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.auth = Some(AuthConfig::Env {
                envvar: "MISSING".into(),
            });
        }),
    );
    c.connections.insert(
        "blank".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.auth = Some(AuthConfig::Mixed {
                envvar: "EMPTY_SECRET".into(),
            });
        }),
    );
    c.connections.insert(
        "multiline".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.auth = Some(AuthConfig::Env {
                envvar: "MULTILINE_SECRET".into(),
            });
        }),
    );
    c.connections.insert(
        "headers".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.headers.insert("Authorization".into(), "Bearer x".into());
            x.headers
                .insert("X-Initiator".into(), "user:someone".into());
            x.headers.insert("bad header".into(), "x".into());
        }),
    );
    c.connections.insert(
        "tls".into(),
        bad(&|x| {
            x.protocol = Some(Protocol::Rest);
            x.endpoint = Some("http://a".into());
            x.tls = Some(z_request::config::TlsConfig {
                client_cert_file: Some("c.pem".into()),
                ..Default::default()
            });
        }),
    );

    let e = c.resolve_with(secrets).unwrap_err();
    for needle in [
        "pool.hold: must be between 0s and 60s",
        "pool.acquire_timeout: must be greater than 0s",
        "pool.min: is 9 but pool.max is 8",
        "defaults.request_timeout: must be greater than 0s",
        "defaults.retry.max_attempts: must be at least 1",
        "defaults.retry.backoff.max",
        "defaults.retry.backoff.multiplier",
        "defaults.breaker.failures",
        "connections.Users: names must be lower-case",
        "connections.noproto.protocol: is required",
        "connections.noendpoint.endpoint: is required",
        "connections.badurl.endpoint: `ftp://a` must start with http:// or https://",
        "connections.limits.min: is 5 but this connection's max is 2",
        "env var `MISSING` (secret for connection `secret`) is not set or empty",
        "env var `EMPTY_SECRET` (secret for connection `blank`) is not set or empty",
        "env var `MULTILINE_SECRET` (secret for connection `multiline`) holds characters that cannot be sent in a header",
        "connections.headers.headers.Authorization: is set by z-request per call",
        "connections.headers.headers.X-Initiator: is set by z-request per call",
        "connections.headers.headers.bad header: is not a valid header name",
        "connections.tls.tls: TLS settings on an http:// endpoint",
        "connections.tls.tls: client_cert_file and client_key_file go together",
    ] {
        assert!(has(&e, needle), "missing `{needle}` in:\n{e}");
    }
    assert!(!e.to_string().contains("x-evil"), "a secret leaked:\n{e}");
}

#[cfg(feature = "grpc")]
#[test]
fn grpc_connections_take_no_headers_or_paths() {
    let mut c = ZRequestConfig::default();
    let mut conn = z_request::config::ConnectionConfig {
        protocol: Some(Protocol::Grpc),
        endpoint: Some("http://users:9090/v1".into()),
        ..Default::default()
    };
    conn.headers.insert("Accept".into(), "x".into());
    c.connections.insert("users".into(), conn);
    let e = c.resolve_with(secrets).unwrap_err();
    assert!(has(&e, "a gRPC endpoint has no path"), "{e}");
    assert!(has(&e, "headers are for REST connections only"), "{e}");
}

#[cfg(feature = "rest")]
#[test]
fn total_scope_caps_the_sum_of_minimums() {
    let mut c = ZRequestConfig::default();
    c.pool.scope = Scope::Total;
    c.pool.max = Limit::Finite(4);
    for (name, min) in [("a", 3), ("b", 2)] {
        c.connections.insert(
            name.into(),
            z_request::config::ConnectionConfig {
                protocol: Some(Protocol::Rest),
                endpoint: Some(format!("http://{name}")),
                min: Some(min),
                ..Default::default()
            },
        );
    }
    let e = c.resolve_with(secrets).unwrap_err();
    assert!(has(&e, "add up to 5, above pool.max 4"), "{e}");

    // Under `total`, connections without their own limits are bounded only by the pool.
    c.connections.get_mut("a").unwrap().min = Some(1);
    let r = c.resolve_with(secrets).unwrap();
    assert_eq!(r.connections[0].max, Limit::Unlimited);
}

#[test]
fn hold_is_strictly_zero_to_sixty_seconds() {
    for (hold, ok) in [(0, true), (60, true), (61, false)] {
        let mut c = ZRequestConfig::default();
        c.pool.hold = Duration::from_secs(hold);
        assert_eq!(c.resolve_with(secrets).is_ok(), ok, "hold {hold}s");
    }
}

#[cfg(all(feature = "yaml", not(feature = "grpc")))]
#[test]
fn a_protocol_without_its_feature_is_reported() {
    let e = resolve_yaml(
        "connections: { users: { protocol: grpc, endpoint: 'http://u:1' } }",
        &[],
    )
    .unwrap_err();
    assert!(has(&e, "grpc needs the `grpc` feature"), "{e}");
}

#[cfg(all(feature = "yaml", feature = "grpc", feature = "rest", feature = "tls"))]
#[test]
fn the_shipped_example_file_resolves() {
    let r = ConfigSource::new()
        .yaml_file(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/z-request.example.yaml"
        ))
        .unwrap()
        .load()
        .unwrap()
        .resolve_with(|name| (name == "COUNTERPARTIES_USERS_SECRET").then(|| "x".to_string()))
        .unwrap();
    assert_eq!(r.connections.len(), 2);
}

#[cfg(feature = "rest")]
#[test]
fn max_response_size_is_a_size_with_a_unit() {
    assert_eq!(
        ZRequestConfig::default().defaults.max_response_size,
        z_request::config::DEFAULT_MAX_RESPONSE_SIZE
    );
    let c: ZRequestConfig = serde_json::from_value(serde_json::json!({
        "defaults": { "max_response_size": "64KiB" },
        "connections": {
            "small": { "protocol": "rest", "endpoint": "http://a" },
            "large": { "protocol": "rest", "endpoint": "http://b", "max_response_size": "5MB" },
        },
    }))
    .unwrap();
    let r = c.resolve_with(secrets).unwrap();
    let size = |name: &str| {
        r.connections
            .iter()
            .find(|c| c.name == name)
            .map(|c| c.max_response_size)
    };
    assert_eq!(
        (size("small"), size("large")),
        (Some(65_536), Some(5_000_000))
    );

    let bare = serde_json::from_value::<ZRequestConfig>(serde_json::json!({
        "defaults": { "max_response_size": 1024 }
    }));
    assert!(bare.unwrap_err().to_string().contains("no unit"));

    let mut zero = c.clone();
    zero.defaults.max_response_size = 0;
    let e = zero.resolve_with(secrets).unwrap_err();
    assert!(
        has(&e, "defaults.max_response_size: must be greater than 0B"),
        "{e}"
    );
}

#[cfg(all(feature = "rest", feature = "grpc"))]
#[test]
fn http2_sharing_and_connection_age_resolve_per_connection() {
    use z_request::config::{DEFAULT_STREAMS_PER_CONNECTION, Http2Mode};

    let c: ZRequestConfig = serde_json::from_value(serde_json::json!({
        "defaults": { "max_connection_age": "2m" },
        "connections": {
            "plain": { "protocol": "rest", "endpoint": "http://a" },
            "legacy": { "protocol": "rest", "endpoint": "http://b", "http2": "off" },
            "sidecar": {
                "protocol": "rest", "endpoint": "http://c",
                "http2": "prior_knowledge", "streams_per_connection": 8,
                "max_connection_age": "0s",
            },
            "users": { "protocol": "grpc", "endpoint": "http://d" },
        },
    }))
    .unwrap();
    let r = c.resolve_with(secrets).unwrap();
    let get = |name: &str| r.connections.iter().find(|c| c.name == name).unwrap();
    let plain = get("plain");
    assert_eq!(
        (
            plain.http2,
            plain.streams_per_connection,
            plain.max_connection_age
        ),
        (
            Http2Mode::Auto,
            DEFAULT_STREAMS_PER_CONNECTION,
            Some(Duration::from_secs(120))
        )
    );
    assert_eq!(get("legacy").http2, Http2Mode::Off);
    let sidecar = get("sidecar");
    assert_eq!(
        (
            sidecar.http2,
            sidecar.streams_per_connection,
            sidecar.max_connection_age
        ),
        (Http2Mode::PriorKnowledge, 8, None)
    );
    assert_eq!(
        get("users").http2,
        Http2Mode::PriorKnowledge,
        "gRPC is always HTTP/2"
    );

    let mut bad = c.clone();
    bad.defaults.streams_per_connection = 0;
    bad.defaults.max_connection_age = Duration::from_secs(3);
    let grpc = bad.connections.get_mut("users").unwrap();
    grpc.http2 = Some(Http2Mode::Off);
    grpc.streams_per_connection = Some(5000);
    let e = bad.resolve_with(secrets).unwrap_err();
    for needle in [
        "defaults.streams_per_connection: must be between 1 and 1000, got 0",
        "defaults.max_connection_age: must be 0s (off) or at least 10s",
        "connections.users.streams_per_connection: must be between 1 and 1000, got 5000",
        "connections.users.http2: gRPC always uses HTTP/2",
    ] {
        assert!(has(&e, needle), "missing `{needle}` in:\n{e}");
    }
}
