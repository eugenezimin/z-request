//! The composition root, and the telemetry a call leaves behind.

#![cfg_attr(not(feature = "rest"), allow(unused_imports, dead_code))]

use std::time::Duration;

use super::*;
use crate::config::{ConnectionConfig, ZRequestConfig};

fn connection(protocol: Protocol, endpoint: &str) -> ConnectionConfig {
    ConnectionConfig {
        protocol: Some(protocol),
        endpoint: Some(endpoint.into()),
        ..Default::default()
    }
}

fn resolved(connections: &[(&str, ConnectionConfig)]) -> ResolvedConfig {
    let mut config = ZRequestConfig::default();
    config.defaults.retry.backoff.initial = Duration::from_millis(10);
    config.defaults.retry.backoff.jitter = false;
    for (name, c) in connections {
        config.connections.insert((*name).into(), c.clone());
    }
    config.resolve().unwrap()
}

#[cfg(all(feature = "rest", feature = "grpc"))]
#[test]
fn every_connection_is_found_by_name_and_protocol() {
    let config = resolved(&[
        (
            "registry",
            connection(Protocol::Rest, "http://127.0.0.1:1/api"),
        ),
        ("users", connection(Protocol::Grpc, "http://127.0.0.1:2")),
    ]);
    let connections = Connections::build(&config).unwrap();

    assert_eq!(
        connections.rest("registry").unwrap().target().name(),
        "registry"
    );
    assert_eq!(connections.grpc("users").unwrap().target().name(), "users");
    assert!(matches!(
        connections.rest("users"),
        Err(Error::WrongProtocol {
            expected: Protocol::Rest,
            actual: Protocol::Grpc,
            ..
        })
    ));
    assert!(matches!(
        connections.grpc("nope"),
        Err(Error::UnknownConnection { .. })
    ));
    let names: Vec<_> = connections.names().collect();
    assert_eq!(
        names,
        [("registry", Protocol::Rest), ("users", Protocol::Grpc)]
    );
    assert_eq!(connections.pool().stats().busy, 0, "nothing opened yet");
}

#[test]
fn an_empty_configuration_builds() {
    let connections = Connections::build(&resolved(&[])).unwrap();
    assert_eq!(connections.names().count(), 0);
}

#[cfg(feature = "rest")]
mod rest {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Subscriber, field};
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::registry::LookupSpan;

    use super::*;
    use crate::telemetry::{CALL_DURATION, CALLS, POOL_CONNECTIONS, POOL_WAITING, RETRIES};
    use crate::utils::spi::{CallContext, TraceContext};

    /// Answers every request on every socket with `200 ok`.
    async fn ok_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0; 4096];
                    while let Ok(n) = socket.read(&mut buf).await {
                        if n == 0 {
                            return;
                        }
                        let reply = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";
                        if socket.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        format!("http://{addr}")
    }

    fn refused() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        format!("http://{}", listener.local_addr().unwrap())
    }

    fn anonymous() -> CallContext {
        CallContext::without_user(TraceContext::default())
    }

    /// Runs `f` on a current-thread runtime, so thread-local recorders and subscribers apply.
    fn block_on<T>(f: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(f)
    }

    #[test]
    fn close_shuts_every_connection() {
        block_on(async {
            let url = ok_server().await;
            let connections =
                Connections::build(&resolved(&[("api", connection(Protocol::Rest, &url))]))
                    .unwrap();
            let api = connections.rest("api").unwrap();
            api.get("/ping").send(&anonymous()).await.unwrap();
            connections.close().await;
            assert!(connections.pool().is_closed());
            assert!(matches!(
                api.get("/ping").send(&anonymous()).await,
                Err(Error::PoolClosed { .. })
            ));
        });
    }

    // -----------------------------------------------------------------------------------------
    // Metrics.

    type Snapshot = Vec<(
        metrics_util::CompositeKey,
        Option<metrics::Unit>,
        Option<metrics::SharedString>,
        DebugValue,
    )>;

    /// The value of metric `name` with exactly `labels`. Take one snapshot per test: the
    /// debugging recorder drains histograms when it snapshots.
    fn metric<'a>(
        snapshot: &'a Snapshot,
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<&'a DebugValue> {
        snapshot
            .iter()
            .find(|(key, ..)| {
                let key = key.key();
                let mut have: Vec<_> = key.labels().map(|l| (l.key(), l.value())).collect();
                let mut want = labels.to_vec();
                have.sort();
                want.sort();
                key.name() == name && have == want
            })
            .map(|(.., value)| value)
    }

    #[test]
    fn a_call_leaves_metrics_by_connection_and_result() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            block_on(async {
                let url = ok_server().await;
                let connections = Connections::build(&resolved(&[
                    ("api", connection(Protocol::Rest, &url)),
                    ("down", connection(Protocol::Rest, &refused())),
                ]))
                .unwrap();
                connections
                    .rest("api")
                    .unwrap()
                    .get("/ping")
                    .send(&anonymous())
                    .await
                    .unwrap();
                let down = connections
                    .rest("down")
                    .unwrap()
                    .get("/ping")
                    .send(&anonymous())
                    .await;
                assert!(matches!(down, Err(Error::Connect { .. })));
            });
        });
        let snapshot = snapshotter.snapshot().into_vec();

        let api = |result| {
            [
                ("connection", "api"),
                ("protocol", "rest"),
                ("result", result),
                ("auth", "none"),
            ]
        };
        assert_eq!(
            metric(&snapshot, CALLS, &api("ok")),
            Some(&DebugValue::Counter(1))
        );
        assert!(matches!(
            metric(&snapshot, CALL_DURATION, &api("ok")[..3]),
            Some(DebugValue::Histogram(samples)) if samples.len() == 1
        ));
        let down = [
            ("connection", "down"),
            ("protocol", "rest"),
            ("result", "connect"),
            ("auth", "none"),
        ];
        assert_eq!(
            metric(&snapshot, CALLS, &down),
            Some(&DebugValue::Counter(1))
        );
        assert_eq!(
            metric(
                &snapshot,
                RETRIES,
                &[("connection", "down"), ("protocol", "rest")]
            ),
            Some(&DebugValue::Counter(2)),
            "3 attempts by default"
        );

        let gauge = |name, labels: &[(&str, &str)]| match metric(&snapshot, name, labels) {
            Some(DebugValue::Gauge(v)) => v.into_inner(),
            other => panic!("{name} {labels:?}: {other:?}"),
        };
        assert_eq!(
            gauge(
                POOL_CONNECTIONS,
                &[("connection", "api"), ("state", "idle")]
            ),
            1.0
        );
        assert_eq!(
            gauge(
                POOL_CONNECTIONS,
                &[("connection", "api"), ("state", "busy")]
            ),
            0.0
        );
        assert_eq!(
            gauge(
                POOL_CONNECTIONS,
                &[("connection", "down"), ("state", "idle")]
            ),
            0.0
        );
        assert_eq!(gauge(POOL_WAITING, &[("connection", "api")]), 0.0);
    }

    // -----------------------------------------------------------------------------------------
    // Spans.

    /// A closed span: its name and fields.
    type Closed = (String, HashMap<String, String>);

    /// Collects every closed span's name and fields.
    #[derive(Clone, Default)]
    struct Spans(Arc<Mutex<Vec<Closed>>>);

    struct Fields(HashMap<String, String>);

    impl field::Visit for Fields {
        fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
            self.0.insert(field.name().into(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &field::Field, value: &str) {
            self.0.insert(field.name().into(), value.into());
        }
    }

    impl<S: Subscriber + for<'a> LookupSpan<'a>> tracing_subscriber::Layer<S> for Spans {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let mut fields = Fields(HashMap::new());
            attrs.record(&mut fields);
            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(fields);
            }
        }

        fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
            if let Some(span) = ctx.span(id)
                && let Some(fields) = span.extensions_mut().get_mut::<Fields>()
            {
                values.record(fields);
            }
        }

        fn on_close(&self, id: Id, ctx: Context<'_, S>) {
            if let Some(span) = ctx.span(&id) {
                let fields = span
                    .extensions_mut()
                    .remove::<Fields>()
                    .map(|f| f.0)
                    .unwrap_or_default();
                self.0.lock().unwrap().push((span.name().into(), fields));
            }
        }
    }

    #[test]
    fn a_call_runs_in_a_span_with_its_outcome() {
        let spans = Spans::default();
        let subscriber = tracing_subscriber::registry().with(spans.clone());
        tracing::subscriber::with_default(subscriber, || {
            block_on(async {
                let url = ok_server().await;
                let connections =
                    Connections::build(&resolved(&[("api", connection(Protocol::Rest, &url))]))
                        .unwrap();
                let ctx = CallContext::user("secret-token", TraceContext::default());
                // `public`: the user's token is not sent, and never recorded.
                connections
                    .rest("api")
                    .unwrap()
                    .get("/ping")
                    .send(&ctx)
                    .await
                    .unwrap();
            });
        });

        let spans = spans.0.lock().unwrap();
        let (_, call) = spans
            .iter()
            .find(|(name, _)| name == "z_request.call")
            .expect("a z_request.call span");
        for (field, value) in [
            ("connection", "api"),
            ("protocol", "rest"),
            ("method", "GET"),
            ("path", "/ping"),
            ("auth", "none"),
            ("attempts", "1"),
            ("result", "ok"),
            ("status", "200"),
        ] {
            assert_eq!(
                call.get(field).map(String::as_str),
                Some(value),
                "{field} in {call:?}"
            );
        }
        assert!(!format!("{spans:?}").contains("secret-token"));
    }
}
