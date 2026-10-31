//! The default [`RetryPolicy`].

use std::time::Duration;

use crate::config::{ConnectionSettings, RetryConfig};
use crate::utils::error::Error;
use crate::utils::spi::RetryPolicy;

/// Retries with exponentially growing waits: `initial`, `initial × multiplier`, … capped at
/// `max`, each shortened randomly to 50–100 % when `jitter` is on.
///
/// A call is retried only while attempts remain (`max_attempts` counts the first) and the error
/// [is retryable](Error::is_retryable). A call that is not idempotent is retried only after
/// [`Error::Connect`]: the connection never opened, so the request was never sent.
#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialBackoff {
    max_attempts: u32,
    initial: Duration,
    max: Duration,
    multiplier: f64,
    jitter: bool,
}

impl ExponentialBackoff {
    /// A policy with the given settings.
    pub fn new(config: &RetryConfig) -> Self {
        Self {
            max_attempts: config.max_attempts,
            initial: config.backoff.initial,
            max: config.backoff.max,
            multiplier: config.backoff.multiplier,
            jitter: config.backoff.jitter,
        }
    }

    /// The policy configured for `connection`.
    pub fn for_connection(connection: &ConnectionSettings) -> Self {
        Self::new(&connection.retry)
    }

    /// The wait before attempt `attempt + 1`, before jitter.
    pub fn base_delay(&self, attempt: u32) -> Duration {
        if self.initial.is_zero() {
            return Duration::ZERO;
        }
        let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        // In f64 and capped before converting, so a huge attempt number or multiplier (even
        // infinite) gives `max` instead of overflowing.
        let secs = (self.initial.as_secs_f64() * self.multiplier.powi(exponent))
            .min(self.max.as_secs_f64());
        Duration::try_from_secs_f64(secs).unwrap_or(self.max)
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn next_delay(&self, attempt: u32, error: &Error, idempotent: bool) -> Option<Duration> {
        if attempt >= self.max_attempts || !error.is_retryable() {
            return None;
        }
        if !idempotent && !matches!(error, Error::Connect { .. }) {
            return None;
        }
        let delay = self.base_delay(attempt);
        Some(if self.jitter {
            delay.mul_f64(0.5 + fastrand::f64() * 0.5)
        } else {
            delay
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BackoffConfig;

    fn policy(jitter: bool) -> ExponentialBackoff {
        ExponentialBackoff::new(&RetryConfig {
            max_attempts: 6,
            backoff: BackoffConfig {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(2),
                multiplier: 2.0,
                jitter,
            },
        })
    }

    fn unavailable() -> Error {
        Error::Http {
            connection: "c".into(),
            status: 503,
            body: String::new(),
        }
    }

    fn connect() -> Error {
        Error::Connect {
            connection: "c".into(),
            reason: "refused".into(),
        }
    }

    #[test]
    fn waits_grow_exponentially_up_to_max() {
        let p = policy(false);
        let waits: Vec<_> = (1..6)
            .map(|a| p.next_delay(a, &unavailable(), true).unwrap().as_millis())
            .collect();
        assert_eq!(waits, [100, 200, 400, 800, 1600]);
        assert_eq!(p.base_delay(6), Duration::from_secs(2), "capped");
    }

    #[test]
    fn stops_after_max_attempts() {
        let p = policy(false);
        assert!(p.next_delay(5, &unavailable(), true).is_some());
        assert!(p.next_delay(6, &unavailable(), true).is_none());
        let once = ExponentialBackoff::new(&RetryConfig {
            max_attempts: 1,
            ..RetryConfig::default()
        });
        assert!(once.next_delay(1, &unavailable(), true).is_none());
    }

    #[test]
    fn only_retryable_errors_and_unsent_requests_of_non_idempotent_calls() {
        let p = policy(false);
        let not_found = Error::Http {
            connection: "c".into(),
            status: 404,
            body: String::new(),
        };
        assert!(p.next_delay(1, &not_found, true).is_none());
        assert!(
            p.next_delay(1, &unavailable(), false).is_none(),
            "maybe sent"
        );
        assert!(p.next_delay(1, &connect(), false).is_some(), "never sent");
    }

    #[test]
    fn jitter_shortens_each_wait_to_half_or_more() {
        let p = policy(true);
        for _ in 0..1000 {
            let d = p.next_delay(3, &unavailable(), true).unwrap();
            assert!(
                (Duration::from_millis(200)..=Duration::from_millis(400)).contains(&d),
                "{d:?}"
            );
        }
    }

    #[test]
    fn extreme_settings_never_panic() {
        let mut p = policy(true);
        p.max_attempts = u32::MAX;
        assert_eq!(p.base_delay(u32::MAX), Duration::from_secs(2));
        p.multiplier = f64::INFINITY;
        assert_eq!(p.base_delay(3), Duration::from_secs(2));
        assert!(p.next_delay(u32::MAX - 1, &unavailable(), true).is_some());
        p.initial = Duration::ZERO;
        assert_eq!(p.base_delay(40), Duration::ZERO);
    }
}
