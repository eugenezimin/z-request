//! The default [`CircuitBreaker`].

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

use crate::config::ConnectionSettings;
use crate::utils::error::Error;
use crate::utils::spi::{CircuitBreaker, Outcome, Target};

/// Where a target's breaker stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum BreakerState {
    /// Calls go ahead.
    Closed,
    /// Calls fail fast with [`Error::CircuitOpen`].
    Open,
    /// One trial call is out; the others fail fast until it is recorded.
    HalfOpen,
}

/// A breaker per connection that opens after `breaker.failures` consecutive failures.
///
/// While open, calls fail fast with [`Error::CircuitOpen`]. After `breaker.open_for`, one trial
/// call is let through: its success closes the breaker, its failure opens it for another
/// `open_for`. A trial that is never recorded (the call was cancelled) is replaced by a new one
/// after `open_for`, so the breaker cannot get stuck. Any success closes it, including one from
/// a call that started before it opened.
///
/// What counts as a failure is up to the caller; [`outcome`](super::outcome) is the rule
/// [`Policies::run`](super::Policies::run) uses.
#[derive(Debug, Default)]
pub struct ConsecutiveFailures {
    targets: HashMap<Target, Entry>,
}

#[derive(Debug)]
struct Entry {
    failures: u32,
    open_for: Duration,
    state: Mutex<State>,
}

#[derive(Debug, Clone, Copy)]
enum State {
    Closed { failures: u32 },
    Open { until: Instant },
    HalfOpen { trial_since: Instant },
}

impl Entry {
    fn lock(&self) -> MutexGuard<'_, State> {
        // The state is a `Copy` value replaced whole, so a poisoned lock still holds a valid one.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl ConsecutiveFailures {
    /// A breaker for each of `connections`, with its `breaker` settings.
    pub fn new<'a>(connections: impl IntoIterator<Item = &'a ConnectionSettings>) -> Self {
        Self {
            targets: connections
                .into_iter()
                .map(|c| {
                    let entry = Entry {
                        failures: c.breaker.failures,
                        open_for: c.breaker.open_for,
                        state: Mutex::new(State::Closed { failures: 0 }),
                    };
                    (c.target(), entry)
                })
                .collect(),
        }
    }

    /// Where `target`'s breaker stands, or `None` for an unknown target.
    pub fn state(&self, target: &Target) -> Option<BreakerState> {
        let entry = self.targets.get(target)?;
        Some(match *entry.lock() {
            State::Closed { .. } => BreakerState::Closed,
            State::Open { .. } => BreakerState::Open,
            State::HalfOpen { .. } => BreakerState::HalfOpen,
        })
    }
}

impl CircuitBreaker for ConsecutiveFailures {
    fn admit(&self, target: &Target) -> Result<(), Error> {
        let entry = self
            .targets
            .get(target)
            .ok_or_else(|| Error::UnknownConnection {
                connection: target.name().to_owned(),
            })?;
        let now = Instant::now();
        let mut state = entry.lock();
        match *state {
            State::Closed { .. } => Ok(()),
            State::Open { until } if now < until => Err(open(target)),
            State::HalfOpen { trial_since } if now < trial_since + entry.open_for => {
                Err(open(target))
            }
            // Open long enough, or the trial was lost: this call is the trial.
            State::Open { .. } | State::HalfOpen { .. } => {
                *state = State::HalfOpen { trial_since: now };
                Ok(())
            }
        }
    }

    fn record(&self, target: &Target, outcome: Outcome) {
        let Some(entry) = self.targets.get(target) else {
            return;
        };
        let mut state = entry.lock();
        let reopen = || State::Open {
            until: Instant::now() + entry.open_for,
        };
        *state = match (outcome, *state) {
            (Outcome::Success, _) => State::Closed { failures: 0 },
            (Outcome::Failure, State::Closed { failures }) => {
                let failures = failures.saturating_add(1);
                if failures >= entry.failures {
                    reopen()
                } else {
                    State::Closed { failures }
                }
            }
            (Outcome::Failure, State::HalfOpen { .. }) => reopen(),
            (Outcome::Failure, open @ State::Open { .. }) => open,
        };
    }
}

fn open(target: &Target) -> Error {
    Error::CircuitOpen {
        connection: target.name().to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::sleep;

    use super::*;
    use crate::config::BreakerConfig;
    use crate::test_support::connection;

    const OPEN_FOR: Duration = Duration::from_secs(30);

    fn breaker() -> (ConsecutiveFailures, Target, Target) {
        let mut a = connection("a");
        a.breaker = BreakerConfig {
            failures: 3,
            open_for: OPEN_FOR,
        };
        let b = connection("b");
        let breaker = ConsecutiveFailures::new([&a, &b]);
        (breaker, a.target(), b.target())
    }

    fn fail(b: &ConsecutiveFailures, t: &Target, times: usize) {
        for _ in 0..times {
            b.admit(t).unwrap();
            b.record(t, Outcome::Failure);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn opens_after_consecutive_failures_only() {
        let (b, a, _) = breaker();
        fail(&b, &a, 2);
        b.record(&a, Outcome::Success);
        fail(&b, &a, 2);
        assert_eq!(
            b.state(&a),
            Some(BreakerState::Closed),
            "the success reset the count"
        );
        fail(&b, &a, 1);
        assert_eq!(b.state(&a), Some(BreakerState::Open));
        assert!(matches!(b.admit(&a), Err(Error::CircuitOpen { .. })));
    }

    #[tokio::test(start_paused = true)]
    async fn lets_one_trial_through_after_open_for() {
        let (b, a, _) = breaker();
        fail(&b, &a, 3);
        sleep(OPEN_FOR - Duration::from_millis(1)).await;
        assert!(b.admit(&a).is_err(), "still open");
        sleep(Duration::from_millis(1)).await;
        b.admit(&a).unwrap();
        assert_eq!(b.state(&a), Some(BreakerState::HalfOpen));
        assert!(b.admit(&a).is_err(), "only one trial");
        b.record(&a, Outcome::Success);
        assert_eq!(b.state(&a), Some(BreakerState::Closed));
        b.admit(&a).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_trial_opens_it_again() {
        let (b, a, _) = breaker();
        fail(&b, &a, 3);
        sleep(OPEN_FOR).await;
        b.admit(&a).unwrap();
        b.record(&a, Outcome::Failure);
        assert_eq!(b.state(&a), Some(BreakerState::Open));
        sleep(OPEN_FOR - Duration::from_millis(1)).await;
        assert!(b.admit(&a).is_err(), "open for a full period again");
    }

    #[tokio::test(start_paused = true)]
    async fn a_lost_trial_is_replaced() {
        let (b, a, _) = breaker();
        fail(&b, &a, 3);
        sleep(OPEN_FOR).await;
        b.admit(&a).unwrap(); // never recorded
        sleep(OPEN_FOR).await;
        b.admit(&a).unwrap();
        assert_eq!(b.state(&a), Some(BreakerState::HalfOpen));
    }

    #[tokio::test(start_paused = true)]
    async fn targets_are_independent_and_unknown_ones_rejected() {
        let (b, a, other) = breaker();
        fail(&b, &a, 3);
        b.admit(&other).unwrap();
        let unknown = Target::new("zzz", "http://zzz");
        assert!(matches!(
            b.admit(&unknown),
            Err(Error::UnknownConnection { .. })
        ));
        b.record(&unknown, Outcome::Failure);
        assert_eq!(b.state(&unknown), None);
    }
}
