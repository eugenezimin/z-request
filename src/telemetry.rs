//! Spans, events and metrics, through the [`tracing`] and [`metrics`] facades.
//!
//! z-request installs no subscriber, recorder or exporter: the host does, or nothing is
//! emitted (the facades are then no-ops). Install the metrics recorder before building
//! [`Connections`](crate::Connections), so the pool's gauges register with it.
//!
//! # Metrics
//!
//! | Metric | Type | Labels |
//! |---|---|---|
//! | `z_request_calls_total` | counter | `connection`, `protocol`, `result`, `auth` |
//! | `z_request_call_duration_seconds` | histogram | `connection`, `protocol`, `result` |
//! | `z_request_retries_total` | counter | `connection`, `protocol` |
//! | `z_request_pool_connections` | gauge | `connection`, `state` (`busy`, `idle`, `connecting`) |
//! | `z_request_pool_waiting` | gauge | `connection` |
//!
//! `result` is `ok` or the error's [`kind`](crate::Error::kind) (`timeout`, `http`, `status`,
//! …); `auth` is what the call sent: `none`, `user`, `secret`, or `unresolved` when its
//! credentials could not be resolved. Every label has a small, fixed set of values: no paths,
//! ids or status codes.
//!
//! # Spans and events
//!
//! Each call runs in a `z_request.call` span (level INFO) with `connection`, `protocol`,
//! `method` and `path` (REST; the path without the query), and, once it ends, `auth`,
//! `attempts`, `result`, `status` (the HTTP or gRPC status code, when there is one) and `error`.
//! Each retry is a DEBUG event, a breaker that opens a WARN event and one that closes an INFO
//! event. Credentials are never recorded.
//!
//! z-request does not read the current span's trace context: the host passes it in the
//! [`CallContext`](crate::CallContext), which also lets it come from the inbound request.

use metrics::{describe_counter, describe_gauge, describe_histogram};

#[cfg(any(feature = "rest", feature = "grpc"))]
pub(crate) use self::call::{CallTelemetry, UNRESOLVED, error_status};

/// Calls made, by result.
pub const CALLS: &str = "z_request_calls_total";
/// How long calls took, all attempts and waits included.
pub const CALL_DURATION: &str = "z_request_call_duration_seconds";
/// Attempts after the first.
pub const RETRIES: &str = "z_request_retries_total";
/// Pooled connections, by state.
pub const POOL_CONNECTIONS: &str = "z_request_pool_connections";
/// Requests waiting for a pooled connection.
pub const POOL_WAITING: &str = "z_request_pool_waiting";

/// Registers the metrics' descriptions with the installed recorder.
/// [`Connections`](crate::Connections) does this when it is built; call it yourself when wiring
/// the parts by hand.
pub fn describe() {
    describe_counter!(CALLS, "Calls made by z-request, by connection and result.");
    describe_histogram!(
        CALL_DURATION,
        metrics::Unit::Seconds,
        "How long calls took, all attempts and backoff waits included."
    );
    describe_counter!(RETRIES, "Attempts after the first, by connection.");
    describe_gauge!(
        POOL_CONNECTIONS,
        "Pooled connections, by connection and state."
    );
    describe_gauge!(POOL_WAITING, "Requests waiting for a pooled connection.");
}

/// Call telemetry, for the transports.
#[cfg(any(feature = "rest", feature = "grpc"))]
mod call {
    use std::time::Instant;

    use metrics::{SharedString, counter, histogram};
    use tracing::Span;
    use tracing::field::Empty;

    use super::{CALL_DURATION, CALLS, RETRIES};
    use crate::utils::error::Error;
    use crate::utils::spi::Target;

    /// What the call sent, for the `auth` label before credentials are resolved.
    pub(crate) const UNRESOLVED: &str = "unresolved";

    /// Measures one call and records its span fields and metrics when it ends.
    pub(crate) struct CallTelemetry {
        connection: SharedString,
        protocol: &'static str,
        span: Span,
        start: Instant,
    }

    impl CallTelemetry {
        /// Starts measuring a call to `target`, in a new `z_request.call` span.
        pub(crate) fn start(target: &Target, protocol: &'static str) -> Self {
            let span = tracing::info_span!(
                "z_request.call",
                connection = target.name(),
                protocol,
                method = Empty,
                path = Empty,
                auth = Empty,
                attempts = Empty,
                result = Empty,
                status = Empty,
                error = Empty,
            );
            Self {
                connection: SharedString::from(target.shared_name()),
                protocol,
                span,
                start: Instant::now(),
            }
        }

        /// The call's span, to run the call in.
        pub(crate) fn span(&self) -> &Span {
            &self.span
        }

        /// Records the end of the call: span fields, then the metrics.
        pub(crate) fn finish<T>(
            self,
            auth: &'static str,
            attempts: u32,
            status: Option<i64>,
            result: &Result<T, Error>,
        ) {
            let outcome = match result {
                Ok(_) => "ok",
                Err(e) => e.kind(),
            };
            let span = &self.span;
            span.record("auth", auth);
            span.record("attempts", attempts);
            span.record("result", outcome);
            if let Some(status) = status {
                span.record("status", status);
            }
            if let Err(e) = result {
                span.record("error", tracing::field::display(e));
            }

            let (connection, protocol) = (self.connection, self.protocol);
            counter!(
                CALLS,
                "connection" => connection.clone(),
                "protocol" => protocol,
                "result" => outcome,
                "auth" => auth,
            )
            .increment(1);
            histogram!(
                CALL_DURATION,
                "connection" => connection.clone(),
                "protocol" => protocol,
                "result" => outcome,
            )
            .record(self.start.elapsed().as_secs_f64());
            if attempts > 1 {
                counter!(RETRIES, "connection" => connection, "protocol" => protocol)
                    .increment(u64::from(attempts - 1));
            }
        }
    }

    /// The status code an error carries, for the span's `status` field.
    pub(crate) fn error_status(error: &Error) -> Option<i64> {
        match error {
            Error::Http { status, .. } => Some(i64::from(*status)),
            Error::Status { code, .. } => Some(i64::from(*code)),
            _ => None,
        }
    }
}
