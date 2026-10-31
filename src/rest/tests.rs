//! REST transport tests against a small scripted HTTP/1.1 server (plain, or TLS with test
//! certificates). Real sockets, real time.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use super::*;
use crate::config::{AuthSettings, ConnectionSettings, Limit, PoolSettings, Scope, parse_endpoint};
use crate::policy::ConsecutiveFailures;
use crate::pool::{ConnectionPool, TypedPool};
use crate::test_support::connection;
use crate::utils::spi::{CallContext, Initiator, Pool, Protocol, Secret, TraceContext};
use crate::{AuthResolver, Error};

// ---------------------------------------------------------------------------------------------
// A scripted HTTP/1.1 server.

/// A request as the server saw it.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    target: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Seen {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

struct Reply {
    status: u16,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
    delay: Duration,
}

fn reply(status: u16, body: &str) -> Reply {
    Reply {
        status,
        headers: Vec::new(),
        body: body.as_bytes().to_vec(),
        delay: Duration::ZERO,
    }
}

type Handler = Arc<dyn Fn(&Seen, usize) -> Reply + Send + Sync>;

struct Server {
    addr: SocketAddr,
    /// Sockets accepted.
    accepted: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Seen>>>,
}

impl Server {
    async fn start(handler: impl Fn(&Seen, usize) -> Reply + Send + Sync + 'static) -> Self {
        Self::listen(Arc::new(handler), None).await
    }

    async fn listen(handler: Handler, tls: Option<tokio_rustls::TlsAcceptor>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (a, s) = (accepted.clone(), seen.clone());
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                a.fetch_add(1, SeqCst);
                let (handler, seen, tls) = (handler.clone(), s.clone(), tls.clone());
                tokio::spawn(async move {
                    match tls {
                        Some(tls) => {
                            if let Ok(socket) = tls.accept(socket).await {
                                serve(socket, handler, seen).await;
                            }
                        }
                        None => serve(socket, handler, seen).await,
                    }
                });
            }
        });
        Self {
            addr,
            accepted,
            seen,
        }
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn accepted(&self) -> usize {
        self.accepted.load(SeqCst)
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().unwrap().clone()
    }
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    socket: S,
    handler: Handler,
    seen: Arc<Mutex<Vec<Seen>>>,
) {
    let mut socket = BufReader::new(socket);
    loop {
        let mut line = String::new();
        if socket.read_line(&mut line).await.unwrap_or(0) == 0 {
            return;
        }
        let mut parts = line.split_whitespace();
        let (method, target) = (
            parts.next().unwrap_or_default().to_owned(),
            parts.next().unwrap_or_default().to_owned(),
        );
        let mut headers = Vec::new();
        loop {
            let mut h = String::new();
            if socket.read_line(&mut h).await.unwrap_or(0) == 0 {
                return;
            }
            let h = h.trim_end();
            if h.is_empty() {
                break;
            }
            if let Some((n, v)) = h.split_once(':') {
                headers.push((n.trim().to_ascii_lowercase(), v.trim().to_owned()));
            }
        }
        let length = headers
            .iter()
            .find(|(n, _)| n == "content-length")
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0; length];
        if socket.read_exact(&mut body).await.is_err() {
            return;
        }
        let request = Seen {
            method,
            target,
            headers,
            body,
        };
        let n = {
            let mut seen = seen.lock().unwrap();
            seen.push(request.clone());
            seen.len()
        };
        let r = handler(&request, n);
        tokio::time::sleep(r.delay).await;
        let mut out = format!(
            "HTTP/1.1 {} X\r\ncontent-length: {}\r\n",
            r.status,
            r.body.len()
        );
        for (n, v) in &r.headers {
            out.push_str(&format!("{n}: {v}\r\n"));
        }
        out.push_str("\r\n");
        let socket = socket.get_mut();
        if socket.write_all(out.as_bytes()).await.is_err()
            || socket.write_all(&r.body).await.is_err()
            || socket.flush().await.is_err()
        {
            return;
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Helpers.

fn settings(url: &str) -> ConnectionSettings {
    let mut c = connection("svc");
    c.endpoint = parse_endpoint(url).unwrap();
    c.max = Limit::Finite(4);
    c.request_timeout = Duration::from_secs(5);
    c.retry.backoff.initial = Duration::from_millis(10);
    c.retry.backoff.jitter = false;
    c
}

fn pool_settings(hold: Duration) -> PoolSettings {
    PoolSettings {
        scope: Scope::PerUri,
        min: 0,
        max: Limit::Finite(8),
        hold,
        acquire_timeout: Duration::from_secs(5),
    }
}

/// A client for `c` and its pool handle.
fn parts(c: &ConnectionSettings, pool: &PoolSettings) -> (RestClient, TypedPool<RestConnector>) {
    let pool = ConnectionPool::new(pool)
        .typed(RestConnector::new([c]).unwrap(), [c])
        .unwrap();
    let client = RestClient::new(
        c,
        pool.clone(),
        Arc::new(AuthResolver::new([c])),
        Arc::new(ConsecutiveFailures::new([c])),
    )
    .unwrap();
    (client, pool)
}

fn client_with(c: &ConnectionSettings, pool: &PoolSettings) -> RestClient {
    parts(c, pool).0
}

fn client(c: &ConnectionSettings) -> RestClient {
    client_with(c, &pool_settings(Duration::from_secs(30)))
}

fn anonymous() -> CallContext {
    CallContext::without_user(TraceContext::default())
}

// ---------------------------------------------------------------------------------------------
// Requests and responses.

#[tokio::test]
async fn a_get_carries_credentials_trace_and_fixed_headers() {
    let server = Server::start(|_, _| reply(200, r#"{"id":7}"#)).await;
    let mut c = settings(&server.url("/api"));
    c.auth = AuthSettings::User;
    c.headers = vec![("X-Tenant".into(), "t1".into())];
    let trace = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: None,
        request_id: Some("req-1".into()),
        initiator: Some(Initiator::user("u1").unwrap()),
    };
    let ctx = CallContext::user("user-token", trace);

    let body: serde_json::Value = client(&c)
        .get("/items/7")
        .query("fields", "a b&c")
        .send_json(&ctx)
        .await
        .unwrap();
    assert_eq!(body["id"], 7);

    let seen = &server.seen()[0];
    assert_eq!(seen.method, "GET");
    assert_eq!(seen.target, "/api/items/7?fields=a+b%26c");
    assert_eq!(seen.header("authorization"), Some("Bearer user-token"));
    assert_eq!(seen.header("x-tenant"), Some("t1"));
    assert_eq!(seen.header("x-request-id"), Some("req-1"));
    assert_eq!(seen.header("x-initiator"), Some("user:u1"));
    assert!(seen.header("traceparent").is_some());
    assert_eq!(seen.header("accept"), Some("application/json"));
}

#[tokio::test]
async fn a_post_sends_its_json_body_with_the_service_secret() {
    let server = Server::start(|_, _| reply(201, "")).await;
    let mut c = settings(&server.url(""));
    c.auth = AuthSettings::Env(Secret::new("svc-secret"));
    let response = client(&c)
        .post("/orders")
        .json(&serde_json::json!({ "qty": 2 }))
        .send(&anonymous())
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    let seen = &server.seen()[0];
    assert_eq!(seen.body, br#"{"qty":2}"#);
    assert_eq!(seen.header("content-type"), Some("application/json"));
    assert_eq!(seen.header("authorization"), Some("Bearer svc-secret"));
}

#[tokio::test]
async fn a_non_2xx_status_is_an_http_error_with_the_start_of_the_body() {
    let server = Server::start(|_, _| reply(404, "no such item")).await;
    let c = settings(&server.url(""));
    match client(&c).get("/items/9").send(&anonymous()).await {
        Err(Error::Http { status, body, .. }) => {
            assert_eq!((status, body.as_str()), (404, "no such item"))
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(server.seen().len(), 1, "a 404 is not retried");
}

#[tokio::test]
async fn redirects_are_not_followed() {
    let server = Server::start(|_, _| Reply {
        headers: vec![("location", "http://elsewhere.test/".into())],
        ..reply(302, "")
    })
    .await;
    let c = settings(&server.url(""));
    let result = client(&c).get("/x").send(&anonymous()).await;
    assert!(
        matches!(result, Err(Error::Http { status: 302, .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn idempotent_requests_are_retried_and_posts_are_not() {
    // Every first request answers 503, then 200.
    let flaky = |_: &Seen, n: usize| {
        if n % 2 == 1 {
            reply(503, "busy")
        } else {
            reply(200, "ok")
        }
    };

    let server = Server::start(flaky).await;
    let c = settings(&server.url(""));
    client(&c).get("/x").send(&anonymous()).await.unwrap();
    assert_eq!(server.seen().len(), 2);

    let server = Server::start(flaky).await;
    let c = settings(&server.url(""));
    let post = client(&c).post("/x").send(&anonymous()).await;
    assert!(matches!(post, Err(Error::Http { status: 503, .. })));
    assert_eq!(server.seen().len(), 1);

    let server = Server::start(flaky).await;
    let c = settings(&server.url(""));
    client(&c)
        .post("/x")
        .idempotent(true)
        .send(&anonymous())
        .await
        .unwrap();
    assert_eq!(server.seen().len(), 2);
}

#[tokio::test]
async fn a_response_over_max_response_size_is_rejected() {
    let server = Server::start(|_, _| reply(200, &"x".repeat(100))).await;
    let mut c = settings(&server.url(""));
    c.max_response_size = 16;
    match client(&c).get("/big").send(&anonymous()).await {
        Err(Error::Decode { reason, .. }) => assert!(reason.contains("16-byte limit"), "{reason}"),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn an_attempt_past_request_timeout_times_out() {
    let server = Server::start(|_, _| Reply {
        delay: Duration::from_secs(2),
        ..reply(200, "late")
    })
    .await;
    let mut c = settings(&server.url(""));
    c.request_timeout = Duration::from_millis(100);
    c.retry.max_attempts = 1;
    let result = client(&c).get("/slow").send(&anonymous()).await;
    assert!(matches!(result, Err(Error::Timeout { .. })), "{result:?}");
}

#[tokio::test]
async fn a_refused_connection_is_a_connect_error_and_frees_the_slot() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let c = settings(&format!("http://127.0.0.1:{port}"));
    let (client, pool) = parts(&c, &pool_settings(Duration::from_secs(30)));
    let result = client.post("/x").send(&anonymous()).await;
    assert!(matches!(result, Err(Error::Connect { .. })), "{result:?}");
    assert_eq!(pool.stats().busy, 0);
}

// ---------------------------------------------------------------------------------------------
// Sockets follow the pool.

#[tokio::test]
async fn the_pool_caps_and_reuses_sockets() {
    let server = Server::start(|_, _| Reply {
        delay: Duration::from_millis(30),
        ..reply(200, "ok")
    })
    .await;
    let mut c = settings(&server.url(""));
    c.max = Limit::Finite(2);
    let client = client(&c);
    let calls: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get("/x").send(&anonymous()).await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }
    assert_eq!(server.accepted(), 2, "max 2 means 2 sockets");
    for _ in 0..5 {
        client.get("/x").send(&anonymous()).await.unwrap();
    }
    assert_eq!(server.accepted(), 2, "and they are reused");
}

#[tokio::test]
async fn hold_zero_opens_a_socket_per_request() {
    let server = Server::start(|_, _| reply(200, "ok")).await;
    let c = settings(&server.url(""));
    let client = client_with(&c, &pool_settings(Duration::ZERO));
    for _ in 0..3 {
        client.get("/x").send(&anonymous()).await.unwrap();
    }
    assert_eq!(server.accepted(), 3);
}

// ---------------------------------------------------------------------------------------------
// Requests rejected before anything is sent.

#[tokio::test]
async fn invalid_requests_never_reach_the_server() {
    let server = Server::start(|_, _| reply(200, "ok")).await;
    let mut c = settings(&server.url("/api"));
    c.auth = AuthSettings::User;
    let client = client(&c);
    let user = CallContext::user("tok", TraceContext::default());

    for request in [
        client.get("/../admin"),
        client.get("/%2e%2e/admin"),
        client.get("no-slash"),
        client.get("/x?y=1"),
        client.get("/x").header("Authorization", "Bearer forged"),
        client.get("/x").header("x-initiator", "user:someone-else"),
        client.get("/x").header("Host", "evil.test"),
        client.get("/x").header("x-ok", "line\r\nx-evil: 1"),
    ] {
        let result = request.send(&user).await;
        assert!(
            matches!(result, Err(Error::InvalidRequest { .. })),
            "{result:?}"
        );
    }
    let forged = CallContext::user("tok\r\nx-evil: 1", TraceContext::default());
    let result = client.get("/x").send(&forged).await;
    assert!(
        matches!(result, Err(Error::InvalidCredentials { .. })),
        "{result:?}"
    );
    assert!(matches!(
        client.get("/x").send(&anonymous()).await,
        Err(Error::MissingUserCredentials { .. })
    ));
    assert!(server.seen().is_empty());
}

#[test]
fn grpc_connections_are_refused() {
    let mut c = settings("http://127.0.0.1:1");
    c.protocol = Protocol::Grpc;
    assert!(matches!(
        RestConnector::new([&c]),
        Err(Error::WrongProtocol { .. })
    ));
}

// ---------------------------------------------------------------------------------------------
// TLS.

#[cfg(feature = "tls")]
mod tls {
    use std::path::PathBuf;

    use rcgen::{
        BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer,
        KeyPair, KeyUsagePurpose,
    };
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::server::WebPkiClientVerifier;

    use super::*;
    use crate::config::TlsConfig;

    /// A test CA, a server certificate for `localhost` and `svc.internal`, and a client
    /// certificate, written as PEM files to a fresh directory.
    struct Pki {
        dir: PathBuf,
        ca: CertificateDer<'static>,
        server_chain: Vec<CertificateDer<'static>>,
        server_key: String,
    }

    impl Pki {
        fn new() -> Self {
            static N: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "z-request-tls-{}-{}",
                std::process::id(),
                N.fetch_add(1, SeqCst)
            ));
            std::fs::create_dir_all(&dir).unwrap();

            // Validity within the current and the next year (below the 825 days macOS allows).
            let year = 1970
                + (std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    / 31_556_952) as i32;
            let validity = |p: &mut CertificateParams| {
                p.not_before = rcgen::date_time_ymd(year, 1, 1);
                p.not_after = rcgen::date_time_ymd(year + 1, 12, 31);
            };

            let ca_key = KeyPair::generate().unwrap();
            let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
            ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
            ca.distinguished_name
                .push(DnType::CommonName, "z-request test CA");
            ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
            validity(&mut ca);
            let ca_cert = ca.self_signed(&ca_key).unwrap();
            let issuer = Issuer::new(ca, ca_key);

            let leaf = |names: Vec<String>, purpose: ExtendedKeyUsagePurpose| {
                let key = KeyPair::generate().unwrap();
                let mut p = CertificateParams::new(names).unwrap();
                p.extended_key_usages = vec![purpose];
                validity(&mut p);
                (p.signed_by(&key, &issuer).unwrap(), key)
            };
            let (server, server_key) = leaf(
                vec!["localhost".into(), "svc.internal".into()],
                ExtendedKeyUsagePurpose::ServerAuth,
            );
            let (client, client_key) =
                leaf(vec!["client".into()], ExtendedKeyUsagePurpose::ClientAuth);

            let write = |name: &str, pem: String| std::fs::write(dir.join(name), pem).unwrap();
            write("ca.pem", ca_cert.pem());
            write("client.pem", client.pem());
            write("client.key", client_key.serialize_pem());
            Self {
                dir,
                ca: ca_cert.der().clone(),
                server_chain: vec![server.der().clone()],
                server_key: server_key.serialize_pem(),
            }
        }

        fn file(&self, name: &str) -> PathBuf {
            self.dir.join(name)
        }

        async fn server(&self, require_client_cert: bool) -> Server {
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let builder = rustls::ServerConfig::builder_with_provider(provider.clone())
                .with_safe_default_protocol_versions()
                .unwrap();
            let builder = if require_client_cert {
                let mut roots = rustls::RootCertStore::empty();
                roots.add(self.ca.clone()).unwrap();
                let verifier =
                    WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
                        .build()
                        .unwrap();
                builder.with_client_cert_verifier(verifier)
            } else {
                builder.with_no_client_auth()
            };
            let key = PrivateKeyDer::from_pem_slice(self.server_key.as_bytes()).unwrap();
            let mut config = builder
                .with_single_cert(self.server_chain.clone(), key)
                .unwrap();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
            Server::listen(Arc::new(|_: &Seen, _| reply(200, "secure")), Some(acceptor)).await
        }
    }

    impl Drop for Pki {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn https(url: &str, tls: TlsConfig) -> ConnectionSettings {
        let mut c = settings(url);
        c.tls = Some(tls);
        c.retry.max_attempts = 1;
        c
    }

    #[tokio::test]
    async fn https_with_the_configured_ca() {
        let pki = Pki::new();
        let server = pki.server(false).await;
        let c = https(
            &format!("https://localhost:{}", server.addr.port()),
            TlsConfig {
                ca_file: Some(pki.file("ca.pem")),
                ..Default::default()
            },
        );
        let response = client(&c).get("/x").send(&anonymous()).await.unwrap();
        assert_eq!(response.text().unwrap(), "secure");
    }

    #[tokio::test]
    async fn an_untrusted_server_is_refused() {
        let pki = Pki::new();
        let server = pki.server(false).await;
        let c = https(
            &format!("https://localhost:{}", server.addr.port()),
            TlsConfig::default(),
        );
        match client(&c).get("/x").send(&anonymous()).await {
            Err(Error::Connect { reason, .. }) => {
                assert!(reason.to_lowercase().contains("certificate"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        assert!(server.seen().is_empty());
    }

    #[tokio::test]
    async fn server_name_checks_the_certificate_for_another_name() {
        let pki = Pki::new();
        let server = pki.server(false).await;
        let c = https(
            &format!("https://127.0.0.1:{}", server.addr.port()),
            TlsConfig {
                ca_file: Some(pki.file("ca.pem")),
                server_name: Some("svc.internal".into()),
                ..Default::default()
            },
        );
        client(&c).get("/x").send(&anonymous()).await.unwrap();
        let host = server.seen()[0].header("host").map(str::to_owned);
        assert_eq!(host, Some(format!("svc.internal:{}", server.addr.port())));
    }

    #[tokio::test]
    async fn a_client_certificate_is_presented_for_mtls() {
        let pki = Pki::new();
        let server = pki.server(true).await;
        let url = format!("https://localhost:{}", server.addr.port());
        let with_cert = https(
            &url,
            TlsConfig {
                ca_file: Some(pki.file("ca.pem")),
                client_cert_file: Some(pki.file("client.pem")),
                client_key_file: Some(pki.file("client.key")),
                ..Default::default()
            },
        );
        client(&with_cert)
            .get("/x")
            .send(&anonymous())
            .await
            .unwrap();

        let without = https(
            &url,
            TlsConfig {
                ca_file: Some(pki.file("ca.pem")),
                ..Default::default()
            },
        );
        assert!(client(&without).get("/x").send(&anonymous()).await.is_err());
        assert_eq!(server.seen().len(), 1);
    }

    #[test]
    fn unreadable_tls_files_are_configuration_errors() {
        let c = https(
            "https://localhost:1",
            TlsConfig {
                ca_file: Some("/nonexistent/ca.pem".into()),
                ..Default::default()
            },
        );
        match RestConnector::new([&c]) {
            Err(Error::Config(e)) => {
                assert_eq!(e.problems()[0].path, "connections.svc.tls");
                assert!(e.problems()[0].message.contains("ca_file"));
            }
            other => panic!("{other:?}"),
        }
    }
}
