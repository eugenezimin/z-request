//! Pool tests. Every locked section checks the pool's invariants on unlock (see
//! `Core::check_invariants`), so each test also exercises them.
//!
//! Time is paused (`start_paused`): sleeps advance the clock instantly and deterministically.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Semaphore;
use tokio::time::sleep;

use super::core::{REAP_RESOLUTION, slot_target};
use super::*;
use crate::config::{
    AuthSettings, BreakerConfig, ConnectionSettings, Endpoint, Limit, PoolSettings, RetryConfig,
    Scope,
};
use crate::utils::spi::{
    Connection, Connector, EvictionPolicy, IdleView, Lease, Pool, Protocol, SlotId, Target,
};
use crate::{Error, utils::spi::PoolStats};

// ---------------------------------------------------------------------------------------------
// A fake transport.

#[derive(Default)]
struct FakeState {
    /// `connect` calls.
    attempts: AtomicUsize,
    /// Connections opened.
    opened: AtomicUsize,
    /// `close` calls (counted in the synchronous part of `close`).
    closed: AtomicUsize,
    /// Fail this many upcoming connects.
    fail_next: AtomicUsize,
    /// Connects wait for a permit here, when set.
    gate: Mutex<Option<Arc<Semaphore>>>,
    /// Connects take this long.
    delay_ms: AtomicU64,
}

impl FakeState {
    fn opened(&self) -> usize {
        self.opened.load(SeqCst)
    }
    fn closed(&self) -> usize {
        self.closed.load(SeqCst)
    }
    /// Blocks connects until permits are added to the returned semaphore.
    fn gate(&self) -> Arc<Semaphore> {
        let gate = Arc::new(Semaphore::new(0));
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
}

/// `KIND` makes distinct connection types, to stand in for two protocols.
struct FakeConnector<const KIND: u8>(Arc<FakeState>);

struct FakeConn<const KIND: u8> {
    id: usize,
    usable: Arc<AtomicBool>,
    state: Arc<FakeState>,
}

impl<const KIND: u8> Connection for FakeConn<KIND> {
    fn is_usable(&self) -> bool {
        self.usable.load(SeqCst)
    }

    fn close(self) -> impl Future<Output = ()> + Send {
        self.state.closed.fetch_add(1, SeqCst);
        async {}
    }
}

impl<const KIND: u8> Connector for FakeConnector<KIND> {
    type Conn = FakeConn<KIND>;

    async fn connect(&self, target: &Target) -> Result<FakeConn<KIND>, Error> {
        let s = &self.0;
        s.attempts.fetch_add(1, SeqCst);
        let delay = s.delay_ms.load(SeqCst);
        if delay > 0 {
            sleep(Duration::from_millis(delay)).await;
        }
        let gate = s.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        if s.fail_next
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(Error::Connect {
                connection: target.name().into(),
                reason: "refused".into(),
            });
        }
        let id = s.opened.fetch_add(1, SeqCst) + 1;
        Ok(FakeConn {
            id,
            usable: Arc::new(AtomicBool::new(true)),
            state: s.clone(),
        })
    }
}

type Fake = TypedPool<FakeConnector<0>>;

// ---------------------------------------------------------------------------------------------
// Helpers.

const ACQUIRE: Duration = Duration::from_secs(2);
const CONNECT: Duration = Duration::from_secs(1);
const HOLD: Duration = Duration::from_secs(30);

fn settings(scope: Scope, min: u32, max: Limit, hold: Duration) -> PoolSettings {
    PoolSettings {
        scope,
        min,
        max,
        hold,
        acquire_timeout: ACQUIRE,
    }
}

fn per_uri(hold: Duration) -> PoolSettings {
    settings(Scope::PerUri, 0, Limit::Finite(8), hold)
}

fn total(min: u32, max: u32) -> PoolSettings {
    settings(Scope::Total, min, Limit::Finite(max), HOLD)
}

fn conn(name: &str, min: u32, max: Limit) -> ConnectionSettings {
    ConnectionSettings {
        name: name.into(),
        protocol: Protocol::Rest,
        endpoint: Endpoint {
            url: format!("http://{name}.test"),
            secure: false,
            host: format!("{name}.test"),
            port: 80,
            base_path: String::new(),
        },
        min,
        max,
        connect_timeout: CONNECT,
        max_response_size: crate::config::DEFAULT_MAX_RESPONSE_SIZE,
        http2: crate::config::Http2Mode::Auto,
        streams_per_connection: crate::config::DEFAULT_STREAMS_PER_CONNECTION,
        max_connection_age: None,
        request_timeout: Duration::from_secs(5),
        retry: RetryConfig::default(),
        breaker: BreakerConfig::default(),
        auth: AuthSettings::Public,
        tls: None,
        headers: Vec::new(),
    }
}

fn limited(name: &str, max: u32) -> ConnectionSettings {
    conn(name, 0, Limit::Finite(max))
}

fn unlimited(name: &str) -> ConnectionSettings {
    conn(name, 0, Limit::Unlimited)
}

/// A pool with one handle over `conns`.
fn pool<'a>(
    settings: PoolSettings,
    conns: impl IntoIterator<Item = &'a ConnectionSettings>,
) -> (Fake, Arc<FakeState>) {
    let state = Arc::new(FakeState::default());
    let pool = ConnectionPool::new(&settings)
        .typed(FakeConnector::<0>(state.clone()), conns)
        .unwrap();
    (pool, state)
}

fn target(c: &ConnectionSettings) -> Target {
    c.target()
}

/// Lets spawned tasks (waiters, the reaper, background closes) run.
async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

fn stats(busy: usize, held: usize, waiting: usize) -> PoolStats {
    PoolStats {
        busy,
        held,
        waiting,
    }
}

fn spawn_acquire(
    p: &Fake,
    t: &Target,
) -> tokio::task::JoinHandle<Result<Lease<FakeConn<0>>, Error>> {
    let (p, t) = (p.clone(), t.clone());
    tokio::spawn(async move { p.acquire(&t).await })
}

// ---------------------------------------------------------------------------------------------
// Reuse and limits.

#[tokio::test(start_paused = true)]
async fn reuses_a_released_connection() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let first = p.acquire(&target(&a)).await.unwrap();
    let slot = first.slot();
    drop(first);
    assert_eq!(p.stats(), stats(0, 1, 0));
    let again = p.acquire(&target(&a)).await.unwrap();
    assert_eq!(again.slot(), slot);
    assert_eq!(s.opened(), 1);
}

#[tokio::test(start_paused = true)]
async fn reuse_is_last_in_first_out() {
    let a = limited("a", 4);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let x = p.acquire(&target(&a)).await.unwrap();
    let y = p.acquire(&target(&a)).await.unwrap();
    let y_slot = y.slot();
    drop(x);
    drop(y);
    assert_eq!(p.acquire(&target(&a)).await.unwrap().slot(), y_slot);
}

#[tokio::test(start_paused = true)]
async fn min_is_lazy() {
    let a = conn("a", 3, Limit::Finite(4));
    let (p, s) = pool(per_uri(HOLD), &[a]);
    settle().await;
    assert_eq!(s.attempts.load(SeqCst), 0);
    assert_eq!(p.stats(), stats(0, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn at_max_requests_wait_and_get_the_released_connection() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let slot = held.slot();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    assert_eq!(p.stats(), stats(1, 0, 1));
    drop(held);
    let got = waiter.await.unwrap().unwrap();
    assert_eq!(got.slot(), slot, "handed over, not reopened");
    assert_eq!(s.opened(), 1);
    assert_eq!(p.stats(), stats(1, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn acquire_times_out_and_reports_the_wait() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let _held = p.acquire(&target(&a)).await.unwrap();
    match p.acquire(&target(&a)).await {
        Err(Error::AcquireTimeout { connection, waited }) => {
            assert_eq!(connection, "a");
            assert_eq!(waited, ACQUIRE);
        }
        other => panic!("expected AcquireTimeout, got {:?}", other.map(|l| l.slot())),
    }
    assert_eq!(
        p.stats(),
        stats(1, 0, 0),
        "the timed-out request left the queue"
    );
}

#[tokio::test(start_paused = true)]
async fn waiters_are_served_in_arrival_order() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let first = spawn_acquire(&p, &target(&a));
    settle().await;
    let second = spawn_acquire(&p, &target(&a));
    settle().await;
    drop(held);
    let lease = first.await.unwrap().unwrap();
    settle().await;
    assert!(!second.is_finished(), "the second waiter is still queued");
    drop(lease);
    second.await.unwrap().unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_newcomer_cannot_take_a_connection_meant_for_a_waiter() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    drop(held); // handed to the waiter right here
    let newcomer = tokio::time::timeout(Duration::from_millis(10), p.acquire(&target(&a))).await;
    assert!(newcomer.is_err(), "the newcomer had to wait");
    waiter.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------------------------
// Hold and min.

#[tokio::test(start_paused = true)]
async fn an_idle_connection_closes_after_hold() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    drop(p.acquire(&target(&a)).await.unwrap());
    sleep(HOLD - Duration::from_millis(1)).await;
    settle().await;
    assert_eq!((s.closed(), p.stats().held), (0, 1));
    sleep(Duration::from_millis(2)).await;
    settle().await;
    assert_eq!((s.closed(), p.stats().held), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn reuse_restarts_the_hold_clock() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    drop(p.acquire(&target(&a)).await.unwrap());
    sleep(Duration::from_secs(20)).await;
    drop(p.acquire(&target(&a)).await.unwrap());
    sleep(Duration::from_secs(15)).await; // 35 s after the first release, 15 s after the second
    settle().await;
    assert_eq!(s.closed(), 0);
    sleep(Duration::from_secs(16)).await;
    settle().await;
    assert_eq!(s.closed(), 1);
}

#[tokio::test(start_paused = true)]
async fn hold_zero_closes_on_release() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(Duration::ZERO), [&a]);
    drop(p.acquire(&target(&a)).await.unwrap());
    settle().await;
    assert_eq!((s.closed(), p.stats()), (1, stats(0, 0, 0)));
    drop(p.acquire(&target(&a)).await.unwrap());
    assert_eq!(s.opened(), 2, "a new connection for every request");
}

#[tokio::test(start_paused = true)]
async fn hold_zero_still_hands_over_to_a_waiter() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(Duration::ZERO), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    drop(held);
    let _lease = waiter.await.unwrap().unwrap();
    assert_eq!((s.opened(), s.closed()), (1, 0));
}

#[tokio::test(start_paused = true)]
async fn min_connections_outlive_hold() {
    let a = conn("a", 2, Limit::Finite(4));
    let (p, s) = pool(per_uri(Duration::from_secs(1)), [&a]);
    let leases = [
        p.acquire(&target(&a)).await.unwrap(),
        p.acquire(&target(&a)).await.unwrap(),
        p.acquire(&target(&a)).await.unwrap(),
    ];
    drop(leases);
    sleep(Duration::from_secs(60)).await;
    settle().await;
    assert_eq!((s.closed(), p.stats().held), (1, 2));
}

#[tokio::test(start_paused = true)]
async fn hold_zero_keeps_min_connections() {
    let a = conn("a", 1, Limit::Finite(4));
    let (p, s) = pool(per_uri(Duration::ZERO), [&a]);
    let x = p.acquire(&target(&a)).await.unwrap();
    let y = p.acquire(&target(&a)).await.unwrap();
    drop(x);
    drop(y);
    settle().await;
    assert_eq!((s.closed(), p.stats().held), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn total_min_is_a_floor_for_all_connections() {
    let (a, b) = (unlimited("a"), unlimited("b"));
    let (p, s) = pool(total(1, 4), [&a, &b]);
    let x = p.acquire(&target(&a)).await.unwrap();
    let y = p.acquire(&target(&b)).await.unwrap();
    drop(x);
    sleep(Duration::from_secs(1)).await;
    drop(y);
    sleep(HOLD * 3).await;
    settle().await;
    assert_eq!(s.closed(), 1, "one connection stays for the floor");
    assert_eq!(
        p.target_stats(&target(&a)).unwrap().held,
        0,
        "the oldest went"
    );
    assert_eq!(p.target_stats(&target(&b)).unwrap().held, 1);
}

#[tokio::test(start_paused = true)]
async fn lifting_min_protection_wakes_the_reaper() {
    let (a, b) = (unlimited("a"), unlimited("b"));
    let (p, s) = pool(total(1, 4), [&a, &b]);
    drop(p.acquire(&target(&a)).await.unwrap()); // protected by the floor
    sleep(HOLD * 2).await;
    settle().await;
    assert_eq!(s.closed(), 0);
    let _b = p.acquire(&target(&b)).await.unwrap(); // the floor is met without `a`'s connection
    sleep(REAP_RESOLUTION).await;
    settle().await;
    assert_eq!(s.closed(), 1);
}

// ---------------------------------------------------------------------------------------------
// Scope `total` and eviction.

#[tokio::test(start_paused = true)]
async fn the_total_cap_counts_every_connection() {
    let (a, b) = (unlimited("a"), unlimited("b"));
    let (p, _) = pool(total(0, 2), [&a, &b]);
    let _x = p.acquire(&target(&a)).await.unwrap();
    let _y = p.acquire(&target(&b)).await.unwrap();
    assert!(matches!(
        p.acquire(&target(&a)).await,
        Err(Error::AcquireTimeout { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn a_connections_own_max_applies_inside_the_total() {
    let a = limited("a", 1);
    let (p, _) = pool(total(0, 4), [&a]);
    let _x = p.acquire(&target(&a)).await.unwrap();
    assert!(matches!(
        p.acquire(&target(&a)).await,
        Err(Error::AcquireTimeout { .. })
    ));
}

#[tokio::test(start_paused = true)]
async fn at_the_total_cap_the_oldest_idle_connection_of_another_connection_closes() {
    let (a, b, c) = (unlimited("a"), unlimited("b"), unlimited("c"));
    let (p, s) = pool(total(0, 2), [&a, &b, &c]);
    let x = p.acquire(&target(&a)).await.unwrap();
    let y = p.acquire(&target(&b)).await.unwrap();
    drop(x);
    sleep(Duration::from_secs(1)).await;
    drop(y);
    let _z = p.acquire(&target(&c)).await.unwrap();
    assert_eq!(s.closed(), 1);
    assert_eq!(p.target_stats(&target(&a)).unwrap().held, 0);
    assert_eq!(p.target_stats(&target(&b)).unwrap().held, 1);
}

#[tokio::test(start_paused = true)]
async fn eviction_keeps_connections_needed_for_min() {
    let (a, b) = (conn("a", 1, Limit::Unlimited), unlimited("b"));
    let (p, s) = pool(total(0, 1), [&a, &b]);
    drop(p.acquire(&target(&a)).await.unwrap());
    assert!(matches!(
        p.acquire(&target(&b)).await,
        Err(Error::AcquireTimeout { .. })
    ));
    assert_eq!(s.closed(), 0);
}

#[tokio::test(start_paused = true)]
async fn eviction_works_across_protocols() {
    let (a, b) = (unlimited("a"), unlimited("b"));
    let pool = ConnectionPool::new(&total(0, 1));
    let (sa, sb) = (
        Arc::new(FakeState::default()),
        Arc::new(FakeState::default()),
    );
    let pa = pool.typed(FakeConnector::<0>(sa.clone()), [&a]).unwrap();
    let pb = pool.typed(FakeConnector::<1>(sb.clone()), [&b]).unwrap();
    drop(pa.acquire(&target(&a)).await.unwrap());
    let lease = pb.acquire(&target(&b)).await.unwrap();
    assert_eq!(lease.id, 1);
    assert_eq!(
        (sa.closed(), sb.opened()),
        (1, 1),
        "a's connection closed as its own type"
    );
    assert_eq!(pool.stats(), stats(1, 0, 0));
}

#[tokio::test(start_paused = true)]
async fn a_release_at_the_total_cap_serves_a_waiter_of_another_connection() {
    let (a, b) = (unlimited("a"), unlimited("b"));
    let (p, s) = pool(total(0, 1), [&a, &b]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&b));
    settle().await;
    assert_eq!(p.stats().waiting, 1);
    drop(held);
    waiter.await.unwrap().unwrap();
    assert_eq!((s.closed(), s.opened()), (1, 2));
}

#[tokio::test(start_paused = true)]
async fn a_custom_eviction_policy_decides() {
    struct Never(Arc<AtomicUsize>);
    impl EvictionPolicy for Never {
        fn choose(&self, _: &Target, idle: &[IdleView<'_>]) -> Option<SlotId> {
            assert!(!idle.is_empty());
            self.0.fetch_add(1, SeqCst);
            None
        }
    }
    let (a, b) = (unlimited("a"), unlimited("b"));
    let calls = Arc::new(AtomicUsize::new(0));
    let pool = ConnectionPool::builder(&total(0, 1))
        .eviction(Never(calls.clone()))
        .build();
    let p = pool
        .typed(FakeConnector::<0>(Arc::default()), [&a, &b])
        .unwrap();
    drop(p.acquire(&target(&a)).await.unwrap());
    assert!(matches!(
        p.acquire(&target(&b)).await,
        Err(Error::AcquireTimeout { .. })
    ));
    assert!(calls.load(SeqCst) >= 1);
}

// ---------------------------------------------------------------------------------------------
// Broken connections and failed connects.

#[tokio::test(start_paused = true)]
async fn an_unusable_connection_is_closed_on_release() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let lease = p.acquire(&target(&a)).await.unwrap();
    lease.usable.store(false, SeqCst);
    drop(lease);
    assert_eq!((s.closed(), p.stats()), (1, stats(0, 0, 0)));
}

#[tokio::test(start_paused = true)]
async fn discard_frees_the_slot_for_a_waiter() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    held.discard();
    let got = waiter.await.unwrap().unwrap();
    assert_eq!((got.id, s.closed()), (2, 1));
}

#[tokio::test(start_paused = true)]
async fn a_connection_that_broke_while_idle_is_skipped() {
    let a = limited("a", 4);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let x = p.acquire(&target(&a)).await.unwrap();
    let y = p.acquire(&target(&a)).await.unwrap();
    let (x_id, y_usable) = (x.id, y.usable.clone());
    drop(x);
    drop(y);
    y_usable.store(false, SeqCst);
    let got = p.acquire(&target(&a)).await.unwrap();
    assert_eq!((got.id, s.closed()), (x_id, 1));
}

#[tokio::test(start_paused = true)]
async fn a_failed_connect_frees_the_slot_for_the_next_waiter() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let gate = s.gate();
    let first = spawn_acquire(&p, &target(&a));
    settle().await;
    let second = spawn_acquire(&p, &target(&a));
    settle().await;
    assert_eq!(p.stats(), stats(1, 0, 1), "connecting counts as busy");
    s.fail_next.store(1, SeqCst);
    gate.add_permits(2);
    assert!(matches!(first.await.unwrap(), Err(Error::Connect { .. })));
    second.await.unwrap().unwrap();
    assert_eq!(s.attempts.load(SeqCst), 2);
}

#[tokio::test(start_paused = true)]
async fn a_connect_timeout_frees_the_slot() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let gate = s.gate();
    match p.acquire(&target(&a)).await {
        Err(Error::Connect { reason, .. }) => assert!(reason.contains("no connection within")),
        other => panic!("expected Connect, got {:?}", other.map(|l| l.slot())),
    }
    assert_eq!(p.stats(), stats(0, 0, 0));
    gate.add_permits(1);
    p.acquire(&target(&a)).await.unwrap();
}

// ---------------------------------------------------------------------------------------------
// Cancellation.

#[tokio::test(start_paused = true)]
async fn a_cancelled_waiter_leaves_nothing_behind() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let gave_up = tokio::time::timeout(Duration::from_millis(10), p.acquire(&target(&a))).await;
    assert!(gave_up.is_err());
    assert_eq!(p.stats(), stats(1, 0, 0));
    drop(held);
    assert_eq!(p.stats(), stats(0, 1, 0));
}

#[tokio::test(start_paused = true)]
async fn a_connection_handed_to_a_cancelled_waiter_comes_back() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    drop(held); // sent to the waiter, which never looks at it
    waiter.abort();
    settle().await;
    assert_eq!(p.stats(), stats(0, 1, 0));
}

#[tokio::test(start_paused = true)]
async fn a_slot_granted_to_a_cancelled_waiter_is_freed() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let held = p.acquire(&target(&a)).await.unwrap();
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;
    held.discard(); // the freed slot goes to the waiter as a permit
    waiter.abort();
    settle().await;
    assert_eq!(p.stats(), stats(0, 0, 0));
    assert_eq!(
        s.attempts.load(SeqCst),
        1,
        "the cancelled waiter never connected"
    );
}

#[tokio::test(start_paused = true)]
async fn cancelling_a_connect_frees_the_slot() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let _gate = s.gate();
    let connecting = spawn_acquire(&p, &target(&a));
    settle().await;
    assert_eq!(p.target_stats(&target(&a)).unwrap().connecting, 1);
    connecting.abort();
    settle().await;
    assert_eq!(p.stats(), stats(0, 0, 0));
}

// ---------------------------------------------------------------------------------------------
// Routing, slots, statistics.

#[tokio::test(start_paused = true)]
async fn unknown_targets_are_rejected() {
    let (a, b) = (limited("a", 1), limited("b", 1));
    let pool = ConnectionPool::new(&per_uri(HOLD));
    let pa = pool
        .typed(FakeConnector::<0>(Arc::default()), [&a])
        .unwrap();
    let _pb = pool
        .typed(FakeConnector::<1>(Arc::default()), [&b])
        .unwrap();
    for t in [target(&b), Target::new("zzz", "http://zzz")] {
        assert!(matches!(
            pa.acquire(&t).await,
            Err(Error::UnknownConnection { .. })
        ));
    }
    assert!(pa.target_stats(&target(&b)).is_none());
}

#[test]
fn a_connection_registers_once() {
    let a = limited("a", 1);
    let pool = ConnectionPool::new(&per_uri(HOLD));
    pool.typed(FakeConnector::<0>(Arc::default()), [&a])
        .unwrap();
    match pool.typed(FakeConnector::<1>(Arc::default()), [&a]) {
        Err(Error::Config(e)) => assert_eq!(e.problems()[0].path, "connections.a"),
        other => panic!("expected a config error, got {other:?}"),
    }
    assert!(
        pool.typed(
            FakeConnector::<0>(Arc::default()),
            [&limited("b", 1), &limited("b", 1)]
        )
        .is_err()
    );
}

#[tokio::test(start_paused = true)]
async fn slot_ids_are_unique_and_name_their_target() {
    let (a, b) = (limited("a", 4), limited("b", 4));
    let (p, _) = pool(per_uri(HOLD), [&a, &b]);
    let mut leases = Vec::new();
    for (t, n) in [(&a, 3), (&b, 2)] {
        for _ in 0..n {
            leases.push(p.acquire(&target(t)).await.unwrap());
        }
    }
    let slots: Vec<SlotId> = leases.iter().map(|l| l.slot()).collect();
    let mut unique = slots.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), 5);
    let targets: Vec<usize> = slots.iter().map(|s| slot_target(*s)).collect();
    assert_eq!(targets, [0, 0, 0, 1, 1]);
}

#[tokio::test(start_paused = true)]
async fn stats_count_connecting_as_busy() {
    let a = limited("a", 2);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let gate = s.gate();
    let connecting = spawn_acquire(&p, &target(&a));
    settle().await;
    assert_eq!(p.stats(), stats(1, 0, 0));
    let t = p.target_stats(&target(&a)).unwrap();
    assert_eq!((t.busy, t.connecting), (0, 1));
    gate.add_permits(1);
    let _lease = connecting.await.unwrap().unwrap();
    let t = p.target_stats(&target(&a)).unwrap();
    assert_eq!((t.busy, t.connecting), (1, 0));
}

// ---------------------------------------------------------------------------------------------
// Shutdown and lifecycle.

#[tokio::test(start_paused = true)]
async fn close_fails_waiters_closes_idle_and_rejects_new_requests() {
    let (a, b) = (limited("a", 1), limited("b", 1));
    let (p, s) = pool(per_uri(HOLD), [&a, &b]);
    let busy = p.acquire(&target(&a)).await.unwrap();
    drop(p.acquire(&target(&b)).await.unwrap()); // idle
    let waiter = spawn_acquire(&p, &target(&a));
    settle().await;

    p.pool().close().await;
    assert!(p.pool().is_closed());
    assert!(matches!(
        waiter.await.unwrap(),
        Err(Error::PoolClosed { .. })
    ));
    assert_eq!(s.closed(), 1, "the idle connection");
    assert!(matches!(
        p.acquire(&target(&b)).await,
        Err(Error::PoolClosed { .. })
    ));

    drop(busy);
    settle().await;
    assert_eq!(s.closed(), 2, "the busy one, on release");
    assert_eq!(p.stats(), stats(0, 0, 0));
    p.pool().close().await; // again: nothing happens
}

#[tokio::test(start_paused = true)]
async fn a_connect_that_finishes_after_close_is_closed() {
    let a = limited("a", 1);
    let (p, s) = pool(per_uri(HOLD), [&a]);
    let gate = s.gate();
    let connecting = spawn_acquire(&p, &target(&a));
    settle().await;
    p.pool().close().await;
    gate.add_permits(1);
    assert!(matches!(
        connecting.await.unwrap(),
        Err(Error::PoolClosed { .. })
    ));
    assert_eq!((s.opened(), s.closed()), (1, 1));
    assert_eq!(p.stats(), stats(0, 0, 0));
}

#[test]
fn a_pool_can_be_built_outside_a_runtime() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), &[a]);
    assert_eq!(p.stats(), stats(0, 0, 0));
    assert!(format!("{:?} {p:?}", p.pool()).starts_with("ConnectionPool"));
}

#[tokio::test(start_paused = true)]
async fn the_reaper_stops_when_the_pool_is_dropped() {
    let a = limited("a", 1);
    let (p, _) = pool(per_uri(HOLD), [&a]);
    drop(p.acquire(&target(&a)).await.unwrap());
    settle().await;
    let alive = p.pool().core.reaper_alive.clone();
    assert!(alive.load(SeqCst));
    drop(p);
    settle().await;
    assert!(!alive.load(SeqCst));
}

// ---------------------------------------------------------------------------------------------
// Stress: random traffic with cancellations, timeouts and broken connections. The invariants
// are checked on every unlock; at the end nothing may be left busy, connecting or waiting.

/// A small deterministic generator (no `rand` dependency).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, n: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % n
    }
}

async fn stress(settings: PoolSettings, conns: Vec<ConnectionSettings>, seed: u64) {
    let (p, s) = pool(settings, &conns);
    s.delay_ms.store(3, SeqCst);
    let targets: Vec<Target> = conns.iter().map(target).collect();
    let mut rng = Lcg(seed);
    let mut tasks = Vec::new();
    for _ in 0..300 {
        let t = targets[rng.next(targets.len() as u64) as usize].clone();
        let (p, s) = (p.clone(), s.clone());
        let (hold_ms, action, fail) = (rng.next(40), rng.next(10), rng.next(20) == 0);
        tasks.push(tokio::spawn(async move {
            if fail {
                s.fail_next.fetch_add(1, SeqCst);
            }
            let Ok(lease) = p.acquire(&t).await else {
                return;
            };
            sleep(Duration::from_millis(hold_ms)).await;
            match action {
                0 => lease.discard(),
                1 => lease.usable.store(false, SeqCst),
                _ => drop(lease),
            }
        }));
        if rng.next(4) == 0 {
            sleep(Duration::from_millis(rng.next(5))).await;
        }
        if rng.next(15) == 0
            && let Some(task) = tasks.get(rng.next(tasks.len() as u64) as usize)
        {
            task.abort();
        }
    }
    for task in tasks {
        let _ = task.await;
    }
    settle().await;
    let end = p.stats();
    assert_eq!((end.busy, end.waiting), (0, 0), "{end:?}");
    sleep(HOLD * 3).await;
    settle().await;
    let open = s.opened() - s.closed();
    assert_eq!(open, p.stats().held, "every connection not held was closed");
}

#[tokio::test(start_paused = true)]
async fn stress_per_uri() {
    let conns = vec![
        limited("a", 2),
        conn("b", 1, Limit::Finite(3)),
        limited("c", 1),
    ];
    let mut settings = per_uri(Duration::from_millis(20));
    settings.acquire_timeout = Duration::from_millis(30);
    stress(settings, conns, 7).await;
}

#[tokio::test(start_paused = true)]
async fn stress_total() {
    let conns = vec![
        unlimited("a"),
        conn("b", 1, Limit::Finite(2)),
        unlimited("c"),
    ];
    let mut settings = total(1, 3);
    settings.hold = Duration::from_millis(20);
    settings.acquire_timeout = Duration::from_millis(30);
    stress(settings, conns, 11).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_multi_thread_keeps_the_books() {
    for settings in [
        per_uri(Duration::from_millis(5)),
        settings(Scope::Total, 1, Limit::Finite(3), Duration::from_millis(5)),
    ] {
        let mut settings = settings;
        settings.acquire_timeout = Duration::from_secs(30); // never times out
        let conns = vec![limited("a", 2), limited("b", 2), unlimited("c")];
        let (p, _) = pool(settings, &conns);
        let targets: Vec<Target> = conns.iter().map(target).collect();
        let mut tasks = Vec::new();
        for i in 0..64 {
            let (p, targets) = (p.clone(), targets.clone());
            tasks.push(tokio::spawn(async move {
                for j in 0..50 {
                    let lease = p.acquire(&targets[(i + j) % 3]).await.unwrap();
                    tokio::task::yield_now().await;
                    if (i * j) % 17 == 0 {
                        lease.discard();
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let end = p.stats();
        assert_eq!((end.busy, end.waiting), (0, 0), "{end:?}");
    }
}
