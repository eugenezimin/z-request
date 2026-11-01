//! REST over HTTP/2: shared sockets, the HTTP/1.1 fallback, prior knowledge, downgrades and
//! connection age, against a hyper server that speaks both (plain, or TLS with a chosen ALPN).

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::Full;
use hyper::service::service_fn;
use hyper::{Request, Response, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

use super::*;
use crate::config::{ConnectionSettings, Http2Mode, Limit, PoolSettings, Scope, parse_endpoint};
use crate::policy::ConsecutiveFailures;
use crate::pool::{ConnectionPool, TypedPool};
use crate::test_support::connection;
use crate::utils::spi::{CallContext, Pool, TraceContext};
use crate::{AuthResolver, Error};

// ---------------------------------------------------------------------------------------------
// A server that speaks HTTP/1.1 and HTTP/2.

/// `/slow` answers after 2 s, anything else after `delay`.
#[derive(Clone)]
#[cfg_attr(not(feature = "tls"), allow(dead_code))] // the ALPN parts are for the TLS tests
struct Server {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    versions: Arc<Mutex<Vec<Version>>>,
    /// What TLS offers (`h2`, `http/1.1`), read for every new socket.
    alpn: Arc<Mutex<Vec<Vec<u8>>>>,
    sockets: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The certificates of a TLS server (kept alive with it).
    #[cfg(feature = "tls")]
    pki: Option<Arc<crate::test_support::Pki>>,
}

#[derive(Clone, Copy, Default)]
#[cfg_attr(not(feature = "tls"), allow(dead_code))] // the ALPN parts are for the TLS tests
struct Options {
    delay: Duration,
    max_streams: Option<u32>,
    tls: bool,
}

#[cfg_attr(not(feature = "tls"), allow(dead_code))] // the ALPN parts are for the TLS tests
const BOTH: &[&[u8]] = &[b"h2", b"http/1.1"];
#[cfg_attr(not(feature = "tls"), allow(dead_code))] // the ALPN parts are for the TLS tests
const HTTP1: &[&[u8]] = &[b"http/1.1"];

impl Server {
    async fn start(options: Options) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = Self {
            addr: listener.local_addr().unwrap(),
            accepted: Arc::default(),
            versions: Arc::default(),
            alpn: Arc::new(Mutex::new(BOTH.iter().map(|p| p.to_vec()).collect())),
            sockets: Arc::default(),
            #[cfg(feature = "tls")]
            pki: options
                .tls
                .then(|| Arc::new(crate::test_support::Pki::new())),
        };
        #[cfg(feature = "tls")]
        let tls = server.pki.clone();
        let s = server.clone();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                s.accepted.fetch_add(1, SeqCst);
                let versions = s.versions.clone();
                let service = service_fn(move |req: Request<hyper::body::Incoming>| {
                    let versions = versions.clone();
                    async move {
                        versions.lock().unwrap().push(req.version());
                        let delay = if req.uri().path() == "/slow" {
                            Duration::from_secs(2)
                        } else {
                            options.delay
                        };
                        tokio::time::sleep(delay).await;
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                    }
                });
                let builder = move || {
                    let mut builder = auto::Builder::new(TokioExecutor::new());
                    builder.http2().max_concurrent_streams(options.max_streams);
                    builder
                };
                #[cfg(feature = "tls")]
                let acceptor = tls
                    .as_ref()
                    .map(|pki| acceptor(pki, &s.alpn.lock().unwrap()));
                let task = tokio::spawn(async move {
                    #[cfg(feature = "tls")]
                    if let Some(acceptor) = acceptor {
                        if let Ok(socket) = acceptor.accept(socket).await {
                            let _ = builder()
                                .serve_connection(TokioIo::new(socket), service)
                                .await;
                        }
                        return;
                    }
                    let _ = builder()
                        .serve_connection(TokioIo::new(socket), service)
                        .await;
                });
                s.sockets.lock().unwrap().push(task);
            }
        });
        server
    }

    fn accepted(&self) -> usize {
        self.accepted.load(SeqCst)
    }

    fn versions(&self) -> Vec<Version> {
        self.versions.lock().unwrap().clone()
    }

    /// Offers only HTTP/1.1 from now on, and drops every open socket.
    #[cfg_attr(not(feature = "tls"), allow(dead_code))] // the ALPN parts are for the TLS tests
    fn downgrade(&self) {
        *self.alpn.lock().unwrap() = HTTP1.iter().map(|p| p.to_vec()).collect();
        for task in self.sockets.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[cfg(feature = "tls")]
fn acceptor(pki: &crate::test_support::Pki, alpn: &[Vec<u8>]) -> tokio_rustls::TlsAcceptor {
    use rustls::pki_types::PrivateKeyDer;
    use rustls::pki_types::pem::PemObject;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key = PrivateKeyDer::from_pem_slice(pki.server_key.as_bytes()).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(pki.server_chain.clone(), key)
        .unwrap();
    config.alpn_protocols = alpn.to_vec();
    tokio_rustls::TlsAcceptor::from(Arc::new(config))
}

// ---------------------------------------------------------------------------------------------
// Helpers.

fn settings(url: &str, mode: Http2Mode, streams: u32) -> ConnectionSettings {
    let mut c = connection("svc");
    c.endpoint = parse_endpoint(url).unwrap();
    c.max = Limit::Finite(16);
    c.request_timeout = Duration::from_secs(5);
    c.retry.backoff.initial = Duration::from_millis(10);
    c.retry.backoff.jitter = false;
    c.http2 = mode;
    c.streams_per_connection = streams;
    c
}

/// An `https://localhost` connection to a TLS `server`, trusting its test CA.
#[cfg(feature = "tls")]
fn https(server: &Server, mode: Http2Mode, streams: u32) -> ConnectionSettings {
    let mut c = settings(
        &format!("https://localhost:{}", server.addr.port()),
        mode,
        streams,
    );
    let pki = server.pki.as_ref().expect("a TLS server");
    c.tls = Some(crate::config::TlsConfig {
        ca_file: Some(pki.file("ca.pem")),
        ..Default::default()
    });
    c
}

fn parts(c: &ConnectionSettings) -> (RestClient, TypedPool<RestConnector>) {
    let pool = ConnectionPool::new(&PoolSettings {
        scope: Scope::PerUri,
        min: 0,
        max: Limit::Finite(32),
        hold: Duration::from_secs(30),
        acquire_timeout: Duration::from_secs(5),
    })
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

fn anonymous() -> CallContext {
    CallContext::without_user(TraceContext::default())
}

/// `n` GETs to `path` at once; all must succeed.
async fn burst(client: &RestClient, n: usize, path: &'static str) {
    let calls: Vec<_> = (0..n)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.get(path).send(&anonymous()).await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }
}

// ---------------------------------------------------------------------------------------------
// Plain HTTP/2 (prior knowledge).

#[tokio::test]
async fn prior_knowledge_shares_sockets_over_plain_http() {
    let server = Server::start(Options {
        delay: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let c = settings(
        &format!("http://{}", server.addr),
        Http2Mode::PriorKnowledge,
        4,
    );
    let (client, _) = parts(&c);
    burst(&client, 8, "/x").await;
    assert_eq!(server.accepted(), 2, "8 requests, 4 per socket");
    assert!(server.versions().iter().all(|v| *v == Version::HTTP_2));
}

#[tokio::test]
async fn plain_http_auto_stays_on_http1() {
    let server = Server::start(Options {
        delay: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let c = settings(&format!("http://{}", server.addr), Http2Mode::Auto, 4);
    let (client, _) = parts(&c);
    burst(&client, 6, "/x").await;
    assert_eq!(server.accepted(), 6, "one socket per request");
    assert!(server.versions().iter().all(|v| *v == Version::HTTP_11));
}

#[tokio::test]
async fn a_timeout_resets_the_stream_not_the_socket() {
    let server = Server::start(Options::default()).await;
    let mut c = settings(
        &format!("http://{}", server.addr),
        Http2Mode::PriorKnowledge,
        4,
    );
    c.request_timeout = Duration::from_millis(100);
    c.retry.max_attempts = 1;
    let (client, pool) = parts(&c);
    let slow = client.get("/slow").send(&anonymous()).await;
    assert!(matches!(slow, Err(Error::Timeout { .. })), "{slow:?}");
    client.get("/x").send(&anonymous()).await.unwrap();
    assert_eq!(server.accepted(), 1, "the socket survived the timeout");
    assert_eq!(pool.stats().busy, 0);
}

#[tokio::test]
async fn old_connections_are_replaced() {
    let server = Server::start(Options::default()).await;
    let mut c = settings(
        &format!("http://{}", server.addr),
        Http2Mode::PriorKnowledge,
        4,
    );
    c.max_connection_age = Some(Duration::from_millis(200));
    let (client, _) = parts(&c);
    client.get("/x").send(&anonymous()).await.unwrap();
    client.get("/x").send(&anonymous()).await.unwrap();
    assert_eq!(server.accepted(), 1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    client.get("/x").send(&anonymous()).await.unwrap();
    assert_eq!(
        server.accepted(),
        2,
        "a fresh socket after max_connection_age"
    );
}

#[tokio::test]
async fn more_requests_than_the_server_allows_per_socket_still_complete() {
    let server = Server::start(Options {
        delay: Duration::from_millis(50),
        max_streams: Some(2),
        ..Default::default()
    })
    .await;
    let c = settings(
        &format!("http://{}", server.addr),
        Http2Mode::PriorKnowledge,
        4,
    );
    let (client, _) = parts(&c);
    // The server takes 2 streams at a time; the other 2 queue inside the HTTP client.
    burst(&client, 4, "/x").await;
    assert_eq!(server.accepted(), 1);
}

// ---------------------------------------------------------------------------------------------
// HTTPS: negotiated in the TLS handshake.

#[cfg(feature = "tls")]
mod tls {
    use super::*;

    async fn start(delay: Duration) -> Server {
        Server::start(Options {
            delay,
            tls: true,
            ..Default::default()
        })
        .await
    }

    #[tokio::test]
    async fn http2_peers_share_sockets_once_learned() {
        let server = start(Duration::from_millis(50)).await;
        let (client, _) = parts(&https(&server, Http2Mode::Auto, 4));
        client.get("/x").send(&anonymous()).await.unwrap(); // learns HTTP/2
        burst(&client, 12, "/x").await;
        assert_eq!(server.accepted(), 3, "12 requests, 4 per socket");
        assert!(server.versions().iter().all(|v| *v == Version::HTTP_2));
    }

    #[tokio::test]
    async fn http1_peers_get_a_socket_per_request() {
        let server = start(Duration::from_millis(50)).await;
        server.downgrade(); // offers only HTTP/1.1
        let (client, _) = parts(&https(&server, Http2Mode::Auto, 4));
        client.get("/x").send(&anonymous()).await.unwrap();
        burst(&client, 6, "/x").await;
        assert_eq!(server.accepted(), 6);
        assert!(server.versions().iter().all(|v| *v == Version::HTTP_11));
    }

    #[tokio::test]
    async fn http2_off_never_shares() {
        let server = start(Duration::from_millis(50)).await;
        let (client, _) = parts(&https(&server, Http2Mode::Off, 4));
        burst(&client, 6, "/x").await;
        assert_eq!(server.accepted(), 6);
        assert!(server.versions().iter().all(|v| *v == Version::HTTP_11));
    }

    #[tokio::test]
    async fn a_downgraded_peer_falls_back_to_a_socket_per_request() {
        let server = start(Duration::from_millis(50)).await;
        let (client, _) = parts(&https(&server, Http2Mode::Auto, 4));
        client.get("/x").send(&anonymous()).await.unwrap();
        burst(&client, 8, "/x").await;
        let before = server.accepted();
        assert_eq!(before, 2, "shared while the peer spoke HTTP/2");

        server.downgrade();
        // GETs are retried, so these succeed while the client notices and negotiates again.
        for _ in 0..3 {
            client.get("/x").send(&anonymous()).await.unwrap();
        }
        let settled = server.accepted();
        burst(&client, 6, "/x").await;
        assert!(
            server.accepted() >= settled + 5,
            "a socket per request again: {} -> {}",
            settled,
            server.accepted()
        );
        assert_eq!(server.versions().last(), Some(&Version::HTTP_11));
    }
}
