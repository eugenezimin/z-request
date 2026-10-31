//! Retries and circuit breaking, and the loop that runs one call with them.
//!
//! - [`ExponentialBackoff`] is the default [`RetryPolicy`]: exponential waits with jitter,
//!   for idempotent calls (and for any call whose connection never opened).
//! - [`ConsecutiveFailures`] is the default [`CircuitBreaker`]: per connection, open after
//!   `breaker.failures` consecutive failures, one trial call after `breaker.open_for`.
//! - [`outcome`] decides what counts against a target.
//! - [`Policies::run`] runs one call: breaker check, a pooled connection, the attempt within
//!   `request_timeout`, the outcome recorded, and a retry after the backoff. Both transports use
//!   it, so the rules live in one place.

mod breaker;
mod retry;

use std::fmt;
use std::future::Future;
use std::time::Duration;

use tokio::time::{sleep, timeout};

pub use self::breaker::{BreakerState, ConsecutiveFailures};
pub use self::retry::ExponentialBackoff;
use crate::utils::error::Error;
use crate::utils::spi::{CircuitBreaker, Connection, Lease, Outcome, Pool, RetryPolicy, Target};

/// gRPC codes that say the peer is unhealthy or overloaded: `UNKNOWN`, `DEADLINE_EXCEEDED`,
/// `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS`.
const FAILING_GRPC_CODES: [i32; 6] = [2, 4, 8, 13, 14, 15];

/// What a call's result says about its target, for the circuit breaker.
///
/// | Result | Outcome |
/// |---|---|
/// | success, an application-level error (HTTP 4xx except 429, gRPC `NOT_FOUND`, `INVALID_ARGUMENT`, …), an undecodable response | `Success`: the peer answered |
/// | `Connect`, `Timeout`, `Transport`, HTTP 5xx or 429, gRPC `UNKNOWN`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS` | `Failure`: the peer is unhealthy or overloaded |
/// | local errors (`AcquireTimeout`, `PoolClosed`, `CircuitOpen`, credentials, configuration) | `None`: says nothing about the peer, not recorded |
pub fn outcome<T>(result: &Result<T, Error>) -> Option<Outcome> {
    let Err(error) = result else {
        return Some(Outcome::Success);
    };
    let failed = |yes: bool| {
        Some(if yes {
            Outcome::Failure
        } else {
            Outcome::Success
        })
    };
    match error {
        Error::Connect { .. } | Error::Timeout { .. } | Error::Transport { .. } => {
            Some(Outcome::Failure)
        }
        Error::Http { status, .. } => failed(*status >= 500 || *status == 429),
        Error::Status { code, .. } => failed(FAILING_GRPC_CODES.contains(code)),
        Error::Decode { .. } => Some(Outcome::Success),
        Error::Config(_)
        | Error::UnknownConnection { .. }
        | Error::WrongProtocol { .. }
        | Error::MissingUserCredentials { .. }
        | Error::InvalidCredentials { .. }
        | Error::InvalidRequest { .. }
        | Error::AcquireTimeout { .. }
        | Error::PoolClosed { .. }
        | Error::CircuitOpen { .. } => None,
    }
}

/// The policies of one connection, to run calls with.
pub struct Policies<'a, R: RetryPolicy + ?Sized, B: CircuitBreaker + ?Sized> {
    /// The connection's retry policy.
    pub retry: &'a R,
    /// The breaker (it keeps a state per target).
    pub breaker: &'a B,
    /// Deadline for one attempt.
    pub request_timeout: Duration,
}

impl<R: RetryPolicy + ?Sized, B: CircuitBreaker + ?Sized> fmt::Debug for Policies<'_, R, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Policies")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl<R: RetryPolicy + ?Sized, B: CircuitBreaker + ?Sized> Policies<'_, R, B> {
    /// Runs one call to `target`, attempt by attempt:
    ///
    /// 1. the breaker admits it, or the call fails fast with [`Error::CircuitOpen`] (never
    ///    retried);
    /// 2. a connection is acquired from `pool` (within `acquire_timeout`, and `connect_timeout`
    ///    if one is opened);
    /// 3. `call(lease, attempt)` runs within `request_timeout`, otherwise the attempt fails with
    ///    [`Error::Timeout`]; `attempt` starts at 1;
    /// 4. the [`outcome`] is recorded with the breaker;
    /// 5. on an error, the retry policy decides whether to wait and try again.
    ///
    /// `call` owns the lease: dropping it returns the connection to the pool, and it can keep
    /// the lease inside its result (for a streamed response). After a transport error it should
    /// [`discard`](Lease::discard) it. An attempt that times out is dropped with its lease, so a
    /// connection left mid-request must report [`is_usable`](Connection::is_usable) `false`.
    ///
    /// Resolve credentials before calling: they are the same for every attempt.
    pub async fn run<C, P, T, F, Fut>(
        &self,
        pool: &P,
        target: &Target,
        idempotent: bool,
        mut call: F,
    ) -> Result<T, Error>
    where
        C: Connection,
        P: Pool<C>,
        F: FnMut(Lease<C>, u32) -> Fut,
        Fut: Future<Output = Result<T, Error>>,
    {
        let mut attempt = 1;
        loop {
            self.breaker.admit(target)?;
            let result = match pool.acquire(target).await {
                Ok(lease) => timeout(self.request_timeout, call(lease, attempt))
                    .await
                    .unwrap_or_else(|_| {
                        Err(Error::Timeout {
                            connection: target.name().to_owned(),
                            after: self.request_timeout,
                        })
                    }),
                Err(e) => Err(e),
            };
            if let Some(o) = outcome(&result) {
                self.breaker.record(target, o);
            }
            let error = match result {
                Ok(value) => return Ok(value),
                Err(e) => e,
            };
            match self.retry.next_delay(attempt, &error, idempotent) {
                Some(delay) => {
                    sleep(delay).await;
                    attempt += 1;
                }
                None => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests;
