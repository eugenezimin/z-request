//! gRPC transport tests against a scripted health-checking service (`grpc.health.v1`, so no
//! code generation is needed), plain or with TLS. Real sockets, real time.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;
use tonic_health::pb::health_server::{Health, HealthServer};
use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

use super::*;
use crate::config::{AuthSettings, ConnectionSettings, Limit, PoolSettings, Scope, parse_endpoint};
use crate::policy::ConsecutiveFailures;
use crate::pool::{ConnectionPool, TypedPool};
use crate::test_support::connection;
use crate::utils::spi::{CallContext, Initiator, Pool, PoolStats, Protocol, Secret, TraceContext};
use crate::{AuthResolver, Error};

// ---------------------------------------------------------------------------------------------
// A scripted health service.

type Script = Arc<dyn Fn(usize) -> Result<(), Status> + Send + Sync>;

#[cfg(feature = "tls")]
type ServerTls = tonic::transport::ServerTlsConfig;
#[cfg(not(feature = "tls"))]
type ServerTls = std::convert::Infallible;

#[derive(Clone)]
struct Scripted {
    calls: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<MetadataMap>>>,
    delay: Duration,
    script: Script,
}

fn serving() -> HealthCheckResponse {
    HealthCheckResponse {
        status: ServingStatus::Serving as i32,
    }
}

#[tonic::async_trait]
impl Health for Scripted {
    async fn check(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<HealthCheckResponse>, Status> {
        let n = self.calls.fetch_add(1, SeqCst) + 1;
        self.seen.lock().unwrap().push(request.metadata().clone());
        tokio::time::sleep(self.delay).await;
        (self.script)(n)?;
        Ok(Response::new(serving()))
    }

    type WatchStream =
        Pin<Box<dyn Stream<Item = Result<HealthCheckResponse, Status>> + Send + 'static>>;

    async fn watch(
        &self,
        request: Request<HealthCheckRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        self.seen.lock().unwrap().push(request.metadata().clone());
        let updates = tokio_stream::iter((0..3).map(|_| Ok(serving())));
        Ok(Response::new(Box::pin(updates)))
    }
}

struct Server {
    addr: SocketAddr,
    accepted: Arc<AtomicUsize>,
    service: Scripted,
    stop: Option<oneshot::Sender<()>>,
}

impl Server {
    async fn start(
        delay: Duration,
        script: impl Fn(usize) -> Result<(), Status> + Send + Sync + 'static,
    ) -> Self {
        Self::listen(delay, Arc::new(script), None).await
    }

    async fn healthy() -> Self {
        Self::start(Duration::ZERO, |_| Ok(())).await
    }

    async fn listen(
        delay: Duration,
        script: Script,
        #[cfg_attr(not(feature = "tls"), allow(unused_variables))] tls: Option<ServerTls>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let counter = accepted.clone();
        let incoming = TcpListenerStream::new(listener).map(move |socket| {
            counter.fetch_add(1, SeqCst);
            socket
        });
        let service = Scripted {
            calls: Arc::default(),
            seen: Arc::default(),
            delay,
            script,
        };
        let mut builder = tonic::transport::Server::builder();
        #[cfg(feature = "tls")]
        if let Some(tls) = tls {
            builder = builder.tls_config(tls).unwrap();
        }
        let (stop, stopped) = oneshot::channel::<()>();
        let router = builder.add_service(HealthServer::new(service.clone()));
        tokio::spawn(router.serve_with_incoming_shutdown(incoming, async {
            let _ = stopped.await;
        }));
        Self {
            addr,
            accepted,
            service,
            stop: Some(stop),
        }
    }

    fn accepted(&self) -> usize {
        self.accepted.load(SeqCst)
    }

    fn calls(&self) -> usize {
        self.service.calls.load(SeqCst)
    }

    fn seen(&self, n: usize) -> MetadataMap {
        self.service.seen.lock().unwrap()[n].clone()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Helpers.

fn settings(url: &str) -> ConnectionSettings {
    let mut c = connection("svc");
    c.protocol = Protocol::Grpc;
    c.endpoint = parse_endpoint(url).unwrap();
    c.max = Limit::Finite(4);
    c.request_timeout = Duration::from_secs(5);
    c.retry.backoff.initial = Duration::from_millis(10);
    c.retry.backoff.jitter = false;
    c
}

fn plain(server: &Server) -> ConnectionSettings {
    settings(&format!("http://{}", server.addr))
}

fn parts(c: &ConnectionSettings) -> (GrpcClient, TypedPool<GrpcConnector>) {
    let pool = ConnectionPool::new(&PoolSettings {
        scope: Scope::PerUri,
        min: 0,
        max: Limit::Finite(8),
        hold: Duration::from_secs(30),
        acquire_timeout: Duration::from_secs(5),
    })
    .typed(GrpcConnector::new([c]).unwrap(), [c])
    .unwrap();
    let client = GrpcClient::new(
        c,
        pool.clone(),
        Arc::new(AuthResolver::new([c])),
        Arc::new(ConsecutiveFailures::new([c])),
    )
    .unwrap();
    (client, pool)
}

fn client(c: &ConnectionSettings) -> GrpcClient {
    parts(c).0
}

fn anonymous() -> CallContext {
    CallContext::without_user(TraceContext::default())
}

/// One health check through `client.call`.
async fn check(client: &GrpcClient, ctx: &CallContext, idempotent: bool) -> Result<i32, Error> {
    client
        .call(ctx, idempotent, |channel| async move {
            HealthClient::new(channel)
                .check(HealthCheckRequest::default())
                .await
        })
        .await
        .map(|response| response.into_inner().status)
}

fn text<'a>(metadata: &'a MetadataMap, key: &str) -> Option<&'a str> {
    metadata.get(key).and_then(|v| v.to_str().ok())
}

// ---------------------------------------------------------------------------------------------
// Calls.

#[tokio::test]
async fn a_call_carries_credentials_trace_and_deadline() {
    let server = Server::healthy().await;
    let mut c = plain(&server);
    c.auth = AuthSettings::Env(Secret::new("svc-secret"));
    let trace = TraceContext {
        traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into()),
        tracestate: None,
        request_id: Some("req-1".into()),
        initiator: Some(Initiator::service("billing/nightly").unwrap()),
    };
    let ctx = CallContext::without_user(trace);
    let status = check(&client(&c), &ctx, true).await.unwrap();
    assert_eq!(status, ServingStatus::Serving as i32);

    let seen = server.seen(0);
    assert_eq!(text(&seen, "authorization"), Some("Bearer svc-secret"));
    assert_eq!(text(&seen, "x-initiator"), Some("service:billing/nightly"));
    assert_eq!(text(&seen, "x-request-id"), Some("req-1"));
    assert!(text(&seen, "traceparent").is_some());
    assert!(
        text(&seen, "grpc-timeout").is_some(),
        "request_timeout is sent"
    );
}

#[tokio::test]
async fn the_users_own_token_is_sent_and_required() {
    let server = Server::healthy().await;
    let mut c = plain(&server);
    c.auth = AuthSettings::User;
    let client = client(&c);
    let user = CallContext::user("user-token", TraceContext::default());
    check(&client, &user, true).await.unwrap();
    assert_eq!(
        text(&server.seen(0), "authorization"),
        Some("Bearer user-token")
    );
    assert!(matches!(
        check(&client, &anonymous(), true).await,
        Err(Error::MissingUserCredentials { .. })
    ));
    assert_eq!(server.calls(), 1, "nothing sent without the user's token");
}

#[tokio::test]
async fn a_status_is_retried_only_when_retryable_and_idempotent() {
    let first_unavailable = |n: usize| {
        if n == 1 {
            Err(Status::unavailable("starting"))
        } else {
            Ok(())
        }
    };

    let server = Server::start(Duration::ZERO, first_unavailable).await;
    check(&client(&plain(&server)), &anonymous(), true)
        .await
        .unwrap();
    assert_eq!(server.calls(), 2);

    let server = Server::start(Duration::ZERO, first_unavailable).await;
    let result = check(&client(&plain(&server)), &anonymous(), false).await;
    assert!(
        matches!(result, Err(Error::Status { code: 14, .. })),
        "{result:?}"
    );
    assert_eq!(server.calls(), 1);

    let server = Server::start(Duration::ZERO, |_| {
        Err(Status::not_found("no such service"))
    })
    .await;
    match check(&client(&plain(&server)), &anonymous(), true).await {
        Err(Error::Status { code, message, .. }) => {
            assert_eq!((code, message.as_str()), (5, "no such service"));
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(server.calls(), 1, "NOT_FOUND is not retried");
}

#[tokio::test]
async fn an_attempt_past_request_timeout_times_out() {
    let server = Server::start(Duration::from_secs(2), |_| Ok(())).await;
    let mut c = plain(&server);
    c.request_timeout = Duration::from_millis(100);
    c.retry.max_attempts = 1;
    let (client, pool) = parts(&c);
    let result = check(&client, &anonymous(), true).await;
    assert!(matches!(result, Err(Error::Timeout { .. })), "{result:?}");
    assert_eq!(
        pool.stats().held,
        1,
        "a deadline leaves the connection healthy"
    );
}

#[tokio::test]
async fn a_refused_connection_is_a_connect_error_and_frees_the_slot() {
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let c = settings(&format!("http://127.0.0.1:{port}"));
    let (client, pool) = parts(&c);
    let result = check(&client, &anonymous(), false).await;
    assert!(matches!(result, Err(Error::Connect { .. })), "{result:?}");
    assert_eq!(pool.stats().busy, 0);
}

#[tokio::test]
async fn repeated_failures_open_the_breaker() {
    let server = Server::start(Duration::ZERO, |_| Err(Status::unavailable("down"))).await;
    let mut c = plain(&server);
    c.retry.max_attempts = 1;
    c.breaker.failures = 2;
    let client = client(&c);
    for _ in 0..2 {
        assert!(check(&client, &anonymous(), true).await.is_err());
    }
    assert!(matches!(
        check(&client, &anonymous(), true).await,
        Err(Error::CircuitOpen { .. })
    ));
    assert_eq!(server.calls(), 2);
}

#[tokio::test]
async fn a_peer_that_went_away_is_a_connect_error_and_its_connection_is_closed() {
    let mut server = Server::healthy().await;
    let mut c = plain(&server);
    c.retry.max_attempts = 1;
    let (client, pool) = parts(&c);
    check(&client, &anonymous(), true).await.unwrap();
    assert_eq!(pool.stats().held, 1);
    drop(server.stop.take());
    tokio::time::sleep(Duration::from_millis(100)).await;
    // The channel tries to reconnect, is refused, and the request is never sent.
    let result = check(&client, &anonymous(), false).await;
    assert!(matches!(result, Err(Error::Connect { .. })), "{result:?}");
    tokio::task::yield_now().await;
    assert_eq!(
        pool.stats(),
        PoolStats {
            busy: 0,
            held: 0,
            waiting: 0
        },
        "closed, not kept"
    );
}

// ---------------------------------------------------------------------------------------------
// Sockets follow the pool.

#[tokio::test]
async fn the_pool_caps_and_reuses_sockets() {
    let server = Server::start(Duration::from_millis(30), |_| Ok(())).await;
    let mut c = plain(&server);
    c.max = Limit::Finite(2);
    let client = client(&c);
    let calls: Vec<_> = (0..8)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { check(&client, &anonymous(), true).await })
        })
        .collect();
    for call in calls {
        call.await.unwrap().unwrap();
    }
    assert_eq!(server.accepted(), 2, "max 2 means 2 sockets");
    for _ in 0..5 {
        check(&client, &anonymous(), true).await.unwrap();
    }
    assert_eq!(server.accepted(), 2, "and they are reused");
}

#[tokio::test]
async fn a_dedicated_channel_holds_its_connection_for_a_stream() {
    let server = Server::healthy().await;
    let mut c = plain(&server);
    c.auth = AuthSettings::Env(Secret::new("svc-secret"));
    let (client, pool) = parts(&c);
    let dedicated = client.dedicated(&anonymous()).await.unwrap();
    assert_eq!(pool.stats().busy, 1);

    let mut updates = HealthClient::new(dedicated.channel())
        .watch(HealthCheckRequest::default())
        .await
        .unwrap()
        .into_inner();
    let mut received = 0;
    while let Some(update) = updates.next().await {
        update.unwrap();
        received += 1;
    }
    assert_eq!(received, 3);
    let seen = server.seen(0);
    assert_eq!(text(&seen, "authorization"), Some("Bearer svc-secret"));
    assert!(
        text(&seen, "grpc-timeout").is_none(),
        "no deadline on a stream"
    );

    drop(dedicated);
    assert_eq!(
        pool.stats(),
        PoolStats {
            busy: 0,
            held: 1,
            waiting: 0
        }
    );
}

#[test]
fn rest_connections_are_refused() {
    let mut c = settings("http://127.0.0.1:1");
    c.protocol = Protocol::Rest;
    assert!(matches!(
        GrpcConnector::new([&c]),
        Err(Error::WrongProtocol { .. })
    ));
}

// ---------------------------------------------------------------------------------------------
// TLS.

#[cfg(feature = "tls")]
mod tls {
    use tonic::transport::{Certificate, Identity, ServerTlsConfig};

    use super::*;
    use crate::config::TlsConfig;
    use crate::test_support::Pki;

    async fn server(pki: &Pki, require_client_cert: bool) -> Server {
        let mut tls =
            ServerTlsConfig::new().identity(Identity::from_pem(&pki.server_pem, &pki.server_key));
        if require_client_cert {
            tls = tls.client_ca_root(Certificate::from_pem(&pki.ca_pem));
        }
        Server::listen(Duration::ZERO, Arc::new(|_| Ok(())), Some(tls)).await
    }

    fn https(url: &str, tls: TlsConfig) -> ConnectionSettings {
        let mut c = settings(url);
        c.tls = Some(tls);
        c.retry.max_attempts = 1;
        c
    }

    fn with_ca(pki: &Pki) -> TlsConfig {
        TlsConfig {
            ca_file: Some(pki.file("ca.pem")),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn https_with_the_configured_ca() {
        let pki = Pki::new();
        let server = server(&pki, false).await;
        let c = https(
            &format!("https://localhost:{}", server.addr.port()),
            with_ca(&pki),
        );
        check(&client(&c), &anonymous(), true).await.unwrap();
    }

    #[tokio::test]
    async fn an_untrusted_server_is_refused() {
        let pki = Pki::new();
        let server = server(&pki, false).await;
        let c = https(
            &format!("https://localhost:{}", server.addr.port()),
            TlsConfig::default(),
        );
        match check(&client(&c), &anonymous(), true).await {
            Err(Error::Connect { reason, .. }) => {
                assert!(reason.to_lowercase().contains("certificate"), "{reason}");
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(server.calls(), 0);
    }

    #[tokio::test]
    async fn server_name_checks_the_certificate_for_another_name() {
        let pki = Pki::new();
        let server = server(&pki, false).await;
        let c = https(
            &format!("https://127.0.0.1:{}", server.addr.port()),
            TlsConfig {
                server_name: Some("svc.internal".into()),
                ..with_ca(&pki)
            },
        );
        check(&client(&c), &anonymous(), true).await.unwrap();
    }

    #[tokio::test]
    async fn a_client_certificate_is_presented_for_mtls() {
        let pki = Pki::new();
        let server = server(&pki, true).await;
        let url = format!("https://localhost:{}", server.addr.port());
        let with_cert = https(
            &url,
            TlsConfig {
                client_cert_file: Some(pki.file("client.pem")),
                client_key_file: Some(pki.file("client.key")),
                ..with_ca(&pki)
            },
        );
        check(&client(&with_cert), &anonymous(), true)
            .await
            .unwrap();
        assert!(
            check(&client(&https(&url, with_ca(&pki))), &anonymous(), true)
                .await
                .is_err()
        );
        assert_eq!(server.calls(), 1);
    }

    #[test]
    fn unreadable_tls_files_are_configuration_errors() {
        let c = https(
            "https://localhost:1",
            TlsConfig {
                client_cert_file: Some("/nonexistent/client.pem".into()),
                client_key_file: Some("/nonexistent/client.key".into()),
                ..Default::default()
            },
        );
        match GrpcConnector::new([&c]) {
            Err(Error::Config(e)) => {
                assert_eq!(e.problems()[0].path, "connections.svc.tls");
                assert!(e.problems()[0].message.contains("client_cert_file"));
            }
            other => panic!("{other:?}"),
        }
    }
}
