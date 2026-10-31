//! The outcome rules and the call loop, on paused time with a real pool.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use tokio::time::Instant;

use super::*;
use crate::config::{BackoffConfig, BreakerConfig, PoolSettings, RetryConfig, Scope};
use crate::pool::{ConnectionPool, TypedPool};
use crate::test_support::connection;
use crate::utils::spi::{Connector, PoolStats};

// ---------------------------------------------------------------------------------------------
// `outcome`.

fn http(status: u16) -> Error {
    Error::Http {
        connection: "c".into(),
        status,
        body: String::new(),
    }
}

fn grpc(code: i32) -> Error {
    Error::Status {
        connection: "c".into(),
        code,
        message: String::new(),
    }
}

fn reason(f: fn(String, String) -> Error) -> Error {
    f("c".into(), "x".into())
}

#[test]
fn outcomes_blame_the_peer_only_for_its_own_failures() {
    use Outcome::{Failure, Success};
    let c = || "c".to_owned();
    let cases: Vec<(Error, Option<Outcome>)> = vec![
        (
            reason(|connection, reason| Error::Connect { connection, reason }),
            Some(Failure),
        ),
        (
            reason(|connection, reason| Error::Transport { connection, reason }),
            Some(Failure),
        ),
        (
            Error::Timeout {
                connection: c(),
                after: Duration::ZERO,
            },
            Some(Failure),
        ),
        (http(500), Some(Failure)),
        (http(503), Some(Failure)),
        (http(429), Some(Failure)),
        (http(404), Some(Success)),
        (http(401), Some(Success)),
        (grpc(14), Some(Failure)),
        (grpc(13), Some(Failure)),
        (grpc(5), Some(Success)),
        (grpc(16), Some(Success)),
        (
            reason(|connection, reason| Error::Decode { connection, reason }),
            Some(Success),
        ),
        (
            Error::AcquireTimeout {
                connection: c(),
                waited: Duration::ZERO,
            },
            None,
        ),
        (Error::PoolClosed { connection: c() }, None),
        (Error::CircuitOpen { connection: c() }, None),
        (Error::UnknownConnection { connection: c() }, None),
        (Error::MissingUserCredentials { connection: c() }, None),
        (
            reason(|connection, reason| Error::InvalidCredentials { connection, reason }),
            None,
        ),
        (
            reason(|connection, reason| Error::InvalidRequest { connection, reason }),
            None,
        ),
    ];
    for (error, expected) in cases {
        let text = error.to_string();
        assert_eq!(outcome::<()>(&Err(error)), expected, "{text}");
    }
    assert_eq!(outcome(&Ok(1)), Some(Success));
}

// ---------------------------------------------------------------------------------------------
// The call loop.

struct Conn;

impl Connection for Conn {
    fn is_usable(&self) -> bool {
        true
    }
    async fn close(self) {}
}

#[derive(Clone, Default)]
struct Dial(Arc<AtomicUsize>);

impl Connector for Dial {
    type Conn = Conn;
    async fn connect(&self, _: &Target) -> Result<Conn, Error> {
        self.0.fetch_add(1, SeqCst);
        Ok(Conn)
    }
}

const WAIT: Duration = Duration::from_millis(100);

struct Fixture {
    pool: TypedPool<Dial>,
    dials: Arc<AtomicUsize>,
    target: Target,
    retry: ExponentialBackoff,
    breaker: ConsecutiveFailures,
}

impl Fixture {
    fn new(max_attempts: u32, breaker_failures: u32) -> Self {
        let mut c = connection("svc");
        c.retry = RetryConfig {
            max_attempts,
            backoff: BackoffConfig {
                initial: WAIT,
                max: Duration::from_secs(2),
                multiplier: 2.0,
                jitter: false,
            },
        };
        c.breaker = BreakerConfig {
            failures: breaker_failures,
            open_for: Duration::from_secs(30),
        };
        let settings = PoolSettings {
            scope: Scope::PerUri,
            min: 0,
            max: c.max,
            hold: Duration::from_secs(30),
            acquire_timeout: Duration::from_secs(1),
        };
        let dial = Dial::default();
        let dials = dial.0.clone();
        Self {
            pool: ConnectionPool::new(&settings).typed(dial, [&c]).unwrap(),
            dials,
            target: c.target(),
            retry: ExponentialBackoff::for_connection(&c),
            breaker: ConsecutiveFailures::new([&c]),
        }
    }

    fn policies(&self) -> Policies<'_, ExponentialBackoff, ConsecutiveFailures> {
        Policies {
            retry: &self.retry,
            breaker: &self.breaker,
            request_timeout: Duration::from_secs(1),
        }
    }

    /// Runs a call whose attempts return `script` in order, recording each attempt number.
    async fn run(
        &self,
        idempotent: bool,
        script: Vec<Result<u32, Error>>,
    ) -> (Result<u32, Error>, Vec<u32>) {
        let script = Mutex::new(VecDeque::from(script));
        let attempts = Mutex::new(Vec::new());
        let result = self
            .policies()
            .run(&self.pool, &self.target, idempotent, |lease, attempt| {
                attempts.lock().unwrap().push(attempt);
                let next = script.lock().unwrap().pop_front().unwrap_or(Ok(0));
                async move {
                    drop(lease);
                    next
                }
            })
            .await;
        (result, attempts.into_inner().unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn a_successful_call_runs_once() {
    let f = Fixture::new(3, 5);
    let (result, attempts) = f.run(true, vec![Ok(7)]).await;
    assert_eq!((result.unwrap(), attempts), (7, vec![1]));
    assert_eq!(
        f.pool.stats(),
        PoolStats {
            busy: 0,
            held: 1,
            waiting: 0
        }
    );
}

#[tokio::test(start_paused = true)]
async fn a_retryable_failure_is_retried_after_the_backoff() {
    let f = Fixture::new(3, 5);
    let start = Instant::now();
    let (result, attempts) = f
        .run(true, vec![Err(http(503)), Err(http(503)), Ok(7)])
        .await;
    assert_eq!((result.unwrap(), attempts), (7, vec![1, 2, 3]));
    assert_eq!(start.elapsed(), WAIT + WAIT * 2);
    assert_eq!(f.dials.load(SeqCst), 1, "the connection was reused");
    assert_eq!(f.pool.stats().busy, 0, "every lease went back");
}

#[tokio::test(start_paused = true)]
async fn the_last_error_is_returned_when_attempts_run_out() {
    let f = Fixture::new(2, 5);
    let (result, attempts) = f.run(true, vec![Err(http(503)), Err(http(502))]).await;
    assert!(matches!(result, Err(Error::Http { status: 502, .. })));
    assert_eq!(attempts, [1, 2]);
}

#[tokio::test(start_paused = true)]
async fn a_non_idempotent_call_is_not_retried() {
    let f = Fixture::new(3, 5);
    let (result, attempts) = f.run(false, vec![Err(http(503)), Ok(7)]).await;
    assert!(matches!(result, Err(Error::Http { status: 503, .. })));
    assert_eq!(attempts, [1]);
}

#[tokio::test(start_paused = true)]
async fn an_attempt_past_request_timeout_times_out_and_is_retried() {
    let f = Fixture::new(3, 5);
    let start = Instant::now();
    let result = f
        .policies()
        .run(&f.pool, &f.target, true, |lease, attempt| async move {
            if attempt == 1 {
                sleep(Duration::from_secs(10)).await;
            }
            drop(lease);
            Ok(attempt)
        })
        .await;
    assert_eq!(result.unwrap(), 2);
    assert_eq!(start.elapsed(), Duration::from_secs(1) + WAIT);
    assert_eq!(f.pool.stats().busy, 0);
}

#[tokio::test(start_paused = true)]
async fn repeated_failures_open_the_breaker_and_later_calls_fail_fast() {
    let f = Fixture::new(1, 2);
    let transport = || {
        Err(Error::Transport {
            connection: "svc".into(),
            reason: "reset".into(),
        })
    };
    for _ in 0..2 {
        assert!(f.run(true, vec![transport()]).await.0.is_err());
    }
    assert_eq!(f.breaker.state(&f.target), Some(BreakerState::Open));
    let (result, attempts) = f.run(true, vec![Ok(7)]).await;
    assert!(matches!(result, Err(Error::CircuitOpen { .. })));
    assert!(attempts.is_empty(), "the call never ran");
}

#[tokio::test(start_paused = true)]
async fn a_local_acquire_timeout_is_not_held_against_the_peer() {
    let f = Fixture::new(1, 1);
    let _busy = f.pool.acquire(&f.target).await.unwrap(); // max is 1
    let (result, attempts) = f.run(true, vec![Ok(7)]).await;
    assert!(matches!(result, Err(Error::AcquireTimeout { .. })));
    assert!(attempts.is_empty());
    assert_eq!(f.breaker.state(&f.target), Some(BreakerState::Closed));
}

#[test]
fn the_call_future_can_be_spawned() {
    fn assert_send<T: Send>(_: &T) {}
    let f = Fixture::new(1, 1);
    let policies = f.policies();
    let call = policies.run(&f.pool, &f.target, true, |lease, _| async move {
        drop(lease);
        Ok(())
    });
    assert_send(&call);
}
