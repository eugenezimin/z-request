//! Errors: [`Error`] for requests, [`ConfigError`] for configuration.

use std::fmt;
use std::time::Duration;

use crate::utils::spi::Protocol;

/// Everything that can go wrong when using a connection.
///
/// Every variant names the connection it happened on (see [`Error::connection`]), and
/// [`Error::is_retryable`] tells whether trying again may succeed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The configuration is invalid.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// No connection with this name is configured.
    #[error("no connection named `{connection}` is configured")]
    UnknownConnection {
        /// The requested name.
        connection: String,
    },
    /// The connection exists but speaks another protocol.
    #[error("connection `{connection}` is {actual}, not {expected}")]
    WrongProtocol {
        /// The connection's name.
        connection: String,
        /// The protocol the caller asked for.
        expected: Protocol,
        /// The protocol the connection is configured with.
        actual: Protocol,
    },
    /// The connection's auth mode is `user` and the call carried no user token.
    #[error("connection `{connection}` requires the caller's credentials, but the call has none")]
    MissingUserCredentials {
        /// The connection's name.
        connection: String,
    },
    /// No connection became free within `pool.acquire_timeout`.
    #[error("connection `{connection}`: no pooled connection free within {waited:?}")]
    AcquireTimeout {
        /// The connection's name.
        connection: String,
        /// How long the request waited.
        waited: Duration,
    },
    /// The pool was closed ([`ConnectionPool::close`](crate::pool::ConnectionPool::close)), so it
    /// hands out no more connections.
    #[error("connection `{connection}`: the pool is closed")]
    PoolClosed {
        /// The connection's name.
        connection: String,
    },
    /// Opening a connection failed.
    #[error("connection `{connection}`: connect failed: {reason}")]
    Connect {
        /// The connection's name.
        connection: String,
        /// What the transport reported.
        reason: String,
    },
    /// The request did not finish within its deadline.
    #[error("connection `{connection}`: no answer within {after:?}")]
    Timeout {
        /// The connection's name.
        connection: String,
        /// The deadline that passed.
        after: Duration,
    },
    /// The circuit breaker is open: recent calls failed, so this one fails fast.
    #[error("connection `{connection}`: circuit open after repeated failures")]
    CircuitOpen {
        /// The connection's name.
        connection: String,
    },
    /// A REST peer answered with a non-2xx status.
    #[error("connection `{connection}`: HTTP {status}")]
    Http {
        /// The connection's name.
        connection: String,
        /// The HTTP status code.
        status: u16,
        /// The start of the response body, for diagnostics.
        body: String,
    },
    /// A gRPC peer answered with a non-OK status.
    #[error("connection `{connection}`: gRPC {} ({code}): {message}", grpc_code_name(*code))]
    Status {
        /// The connection's name.
        connection: String,
        /// The numeric gRPC status code.
        code: i32,
        /// The status message.
        message: String,
    },
    /// The connection broke while sending or receiving.
    #[error("connection `{connection}`: transport error: {reason}")]
    Transport {
        /// The connection's name.
        connection: String,
        /// What the transport reported.
        reason: String,
    },
    /// The response could not be decoded.
    #[error("connection `{connection}`: cannot decode the response: {reason}")]
    Decode {
        /// The connection's name.
        connection: String,
        /// What the decoder reported.
        reason: String,
    },
}

/// gRPC codes worth retrying: `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `UNAVAILABLE`.
const RETRYABLE_GRPC_CODES: [i32; 3] = [4, 8, 14];
/// HTTP statuses worth retrying: request timeout, too many requests, gateway errors.
const RETRYABLE_HTTP_STATUSES: [u16; 5] = [408, 429, 502, 503, 504];

impl Error {
    /// Whether trying the same call again may succeed.
    ///
    /// Retry policies use this, and only for idempotent calls.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Connect { .. } | Self::Timeout { .. } | Self::Transport { .. } => true,
            Self::Http { status, .. } => RETRYABLE_HTTP_STATUSES.contains(status),
            Self::Status { code, .. } => RETRYABLE_GRPC_CODES.contains(code),
            _ => false,
        }
    }

    /// The connection the error happened on, when there is one.
    pub fn connection(&self) -> Option<&str> {
        match self {
            Self::Config(_) => None,
            Self::UnknownConnection { connection }
            | Self::WrongProtocol { connection, .. }
            | Self::MissingUserCredentials { connection }
            | Self::AcquireTimeout { connection, .. }
            | Self::PoolClosed { connection }
            | Self::Connect { connection, .. }
            | Self::Timeout { connection, .. }
            | Self::CircuitOpen { connection }
            | Self::Http { connection, .. }
            | Self::Status { connection, .. }
            | Self::Transport { connection, .. }
            | Self::Decode { connection, .. } => Some(connection),
        }
    }
}

/// The canonical name of a gRPC status code, for messages.
pub fn grpc_code_name(code: i32) -> &'static str {
    match code {
        0 => "OK",
        1 => "CANCELLED",
        2 => "UNKNOWN",
        3 => "INVALID_ARGUMENT",
        4 => "DEADLINE_EXCEEDED",
        5 => "NOT_FOUND",
        6 => "ALREADY_EXISTS",
        7 => "PERMISSION_DENIED",
        8 => "RESOURCE_EXHAUSTED",
        9 => "FAILED_PRECONDITION",
        10 => "ABORTED",
        11 => "OUT_OF_RANGE",
        12 => "UNIMPLEMENTED",
        13 => "INTERNAL",
        14 => "UNAVAILABLE",
        15 => "DATA_LOSS",
        16 => "UNAUTHENTICATED",
        _ => "UNKNOWN_CODE",
    }
}

/// One configuration problem: where it is and what is wrong.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// Dotted path of the setting, e.g. `connections.users.endpoint`.
    pub path: String,
    /// What is wrong, in plain words.
    pub message: String,
}

impl fmt::Display for Problem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            f.write_str(&self.message)
        } else {
            write!(f, "{}: {}", self.path, self.message)
        }
    }
}

/// An invalid configuration: every problem found, not only the first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    problems: Vec<Problem>,
}

impl ConfigError {
    /// An error with a single problem.
    pub fn new(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            problems: vec![Problem {
                path: path.into(),
                message: message.into(),
            }],
        }
    }

    /// An error from collected problems, or `None` if there are none.
    pub fn from_problems(problems: Vec<Problem>) -> Option<Self> {
        (!problems.is_empty()).then_some(Self { problems })
    }

    /// Every problem found.
    pub fn problems(&self) -> &[Problem] {
        &self.problems
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid z-request configuration")?;
        for p in &self.problems {
            write!(f, "\n  - {p}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retryability_follows_the_status_tables() {
        let http = |status| Error::Http {
            connection: "c".into(),
            status,
            body: String::new(),
        };
        let grpc = |code| Error::Status {
            connection: "c".into(),
            code,
            message: String::new(),
        };
        assert!(http(503).is_retryable());
        assert!(http(429).is_retryable());
        assert!(!http(500).is_retryable());
        assert!(!http(404).is_retryable());
        assert!(grpc(14).is_retryable());
        assert!(!grpc(16).is_retryable(), "UNAUTHENTICATED is never retried");
        assert!(
            !grpc(7).is_retryable(),
            "PERMISSION_DENIED is never retried"
        );
        assert!(
            !Error::CircuitOpen {
                connection: "c".into()
            }
            .is_retryable()
        );
        let closed = Error::PoolClosed {
            connection: "c".into(),
        };
        assert!(!closed.is_retryable(), "a closed pool stays closed");
        assert_eq!(closed.connection(), Some("c"));
        assert_eq!(closed.to_string(), "connection `c`: the pool is closed");
    }

    #[test]
    fn config_error_lists_every_problem() {
        let e = ConfigError::from_problems(vec![
            Problem {
                path: "pool.hold".into(),
                message: "too long".into(),
            },
            Problem {
                path: String::new(),
                message: "general".into(),
            },
        ])
        .unwrap();
        assert_eq!(
            e.to_string(),
            "invalid z-request configuration\n  - pool.hold: too long\n  - general"
        );
        assert!(ConfigError::from_problems(vec![]).is_none());
    }
}
