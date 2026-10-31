//! The protocol-independent part of the pool: counts, limits, waiting order, eviction, the hold
//! timer and shutdown, for every target of every protocol.
//!
//! Each target's connections and waiters live in a [`TargetStore`] whose real type
//! (`typed::Store<C>`) belongs to the [`TypedPool`](super::TypedPool) that registered the
//! target. The core sees stores through that erased interface, so one core can count, evict and
//! close connections of different types.
//!
//! # Locking
//!
//! All state sits behind one `std::sync::Mutex`, held only for short sections and never across
//! an `.await`. A section decides what to do and collects what must run outside the lock in a
//! [`Deferred`]: closing connections, dropping leases and permits (whose `Drop` takes the lock
//! again) and waking the reaper. One lock is enough here: every hot-path section is O(1), and
//! `scope: total` needs check-and-reserve across all targets anyway.
//!
//! # Invariants
//!
//! Hold whenever the lock is free; test builds assert them on every unlock. Per target: `idle`,
//! `busy`, `connecting` (reserved, being opened) and `waiting`; `used = idle + busy + connecting`.
//!
//! 1. A target's `used` never exceeds its `max`; under `total`, the pool's `used` never exceeds
//!    `pool.max`. Slots being opened count.
//! 2. The totals are the sums of the targets' counts, which match their stores.
//! 3. A target with a live waiter has no idle connection, and no room to open one (under
//!    `total`: the cap is reached and the eviction policy found nothing to close).
//! 4. Every reserved slot and busy connection has exactly one owner (a `ConnectPermit`, a
//!    `Lease`, or a grant in flight to a waiter, which `WaitGuard` takes back if the waiter is
//!    gone), so cancelling a request at any point leaks nothing.
//! 5. A closed pool has no idle connections and no waiters.

use std::any::Any;
use std::fmt;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::Notify;
use tokio::time::Instant;

use super::eviction::LruEviction;
use super::reaper;
use crate::config::{Limit, PoolSettings, Scope};
use crate::utils::spi::{Connection, EvictionPolicy, IdleView, PoolStats, SlotId, Target};

/// The reaper wakes at most this often, so expiry can be this late (never early).
pub(crate) const REAP_RESOLUTION: Duration = Duration::from_millis(100);

/// Low bits of a [`SlotId`]: the per-target sequence. High bits: the target's index.
const SEQ_BITS: u32 = 48;
const SEQ_MASK: u64 = (1 << SEQ_BITS) - 1;
/// Most targets one pool can hold (the slot id keeps the target index in 16 bits).
pub(crate) const MAX_TARGETS: usize = 1 << (64 - SEQ_BITS);

/// The slot id of connection number `seq` of target `target`.
pub(crate) fn slot_id(target: usize, seq: u64) -> SlotId {
    SlotId(((target as u64) << SEQ_BITS) | (seq & SEQ_MASK))
}

/// The target a slot id belongs to.
pub(crate) fn slot_target(slot: SlotId) -> usize {
    (slot.0 >> SEQ_BITS) as usize
}

/// Connection counts of one target, or of the whole pool.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Counts {
    /// Open, held for the next request.
    pub(crate) idle: u32,
    /// Leased to a request.
    pub(crate) busy: u32,
    /// Reserved and being opened.
    pub(crate) connecting: u32,
    /// Requests queued for a connection.
    pub(crate) waiting: u32,
}

impl Counts {
    /// Connections that are open.
    pub(crate) fn open(&self) -> u32 {
        self.idle + self.busy
    }

    /// Slots counted against the limits: open plus being opened.
    pub(crate) fn used(&self) -> u32 {
        self.open() + self.connecting
    }
}

/// A snapshot of one target's state ([`TypedPool::target_stats`](super::TypedPool::target_stats)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TargetStats {
    /// Connections serving a request right now.
    pub busy: usize,
    /// Connections being opened.
    pub connecting: usize,
    /// Connections open and idle, kept for the next request.
    pub held: usize,
    /// Requests waiting for a connection.
    pub waiting: usize,
}

impl From<Counts> for TargetStats {
    fn from(c: Counts) -> Self {
        Self {
            busy: c.busy as usize,
            connecting: c.connecting as usize,
            held: c.idle as usize,
            waiting: c.waiting as usize,
        }
    }
}

/// A future that closes a connection.
pub(crate) type CloseFuture = Pin<Box<dyn Future<Output = ()> + Send>>;

trait CloseBoxed: Send {
    fn close_boxed(self: Box<Self>) -> CloseFuture;
}

impl<C: Connection> CloseBoxed for C {
    fn close_boxed(self: Box<Self>) -> CloseFuture {
        Box::pin((*self).close())
    }
}

/// A connection of any type, which the core can only close. Used on cold paths (eviction,
/// expiry, shutdown, broken connections), so the box costs nothing on the hot path.
pub(crate) struct ErasedConn(Box<dyn CloseBoxed>);

impl ErasedConn {
    pub(crate) fn new<C: Connection>(conn: C) -> Self {
        Self(Box::new(conn))
    }

    /// Starts closing. The synchronous part of `Connection::close` runs now.
    pub(crate) fn close(self) -> CloseFuture {
        self.0.close_boxed()
    }
}

/// The core's view of one target's idle connections and waiters, whatever their type.
///
/// No method runs user code except dropping connections, and none drops a lease or permit, so
/// all are safe to call under the lock.
pub(crate) trait TargetStore: Send + 'static {
    /// For the owning [`TypedPool`](super::TypedPool) to reach its typed store.
    fn as_any_mut(&mut self) -> &mut dyn Any;
    /// When the oldest idle connection was released.
    fn oldest_idle(&self) -> Option<Instant>;
    /// Calls `f` with the slot and release time of each idle connection, oldest first.
    fn idle_entries(&self, f: &mut dyn FnMut(SlotId, Instant));
    /// Removes the idle connection in `slot`.
    fn take_idle(&mut self, slot: SlotId) -> Option<ErasedConn>;
    /// Removes the oldest idle connection.
    fn pop_oldest_idle(&mut self) -> Option<ErasedConn>;
    /// Removes every idle connection.
    fn drain_idle(&mut self, out: &mut Vec<(ErasedConn, Duration)>, budget: Duration);
    /// Drops waiters that gave up from the front of the queue; returns the ticket of the first
    /// live one and how many were dropped.
    fn live_head(&mut self) -> (Option<u64>, u32);
    /// Hands a connect permit to the first live waiter. Returns the permit if no waiter took
    /// it, and how many waiters were removed from the queue (served or given up).
    fn send_permit(&mut self, core: &Arc<Core>, permit: RawPermit) -> (Result<(), RawPermit>, u32);
    /// Removes the waiter with `ticket`; `false` if it is no longer queued.
    fn remove_waiter(&mut self, ticket: u64) -> bool;
    /// Drops every waiter (their requests fail with `PoolClosed`); returns how many.
    fn drop_waiters(&mut self) -> u32;
    /// Lengths and slots, for the invariant check.
    #[cfg(test)]
    fn check(&self) -> StoreCheck;
}

/// What [`TargetStore::check`] reports.
#[cfg(test)]
pub(crate) struct StoreCheck {
    pub(crate) idle: usize,
    pub(crate) waiters: usize,
    pub(crate) live_waiters: usize,
    pub(crate) idle_slots: Vec<SlotId>,
}

/// One registered target.
pub(crate) struct TargetState {
    pub(crate) target: Target,
    /// Connections this target keeps open even when idle.
    pub(crate) min: u32,
    /// Most connections this target may use.
    pub(crate) max: Limit,
    /// Bounds connecting and closing.
    pub(crate) connect_timeout: Duration,
    pub(crate) counts: Counts,
    /// The next slot sequence number.
    pub(crate) next_seq: u64,
    pub(crate) store: Box<dyn TargetStore>,
    /// The counts as metrics, set when the lock is released.
    pub(crate) gauges: PoolGauges,
    /// The counts changed since the gauges were last set.
    pub(crate) dirty: bool,
}

/// One target's pool gauges ([`telemetry`](crate::telemetry)).
pub(crate) struct PoolGauges {
    busy: metrics::Gauge,
    idle: metrics::Gauge,
    connecting: metrics::Gauge,
    waiting: metrics::Gauge,
}

impl PoolGauges {
    /// Registers the gauges of `target` with the installed recorder.
    pub(crate) fn new(target: &Target) -> Self {
        use crate::telemetry::{POOL_CONNECTIONS, POOL_WAITING};
        let connection = metrics::SharedString::from(target.shared_name());
        let state = |state: &'static str| metrics::gauge!(POOL_CONNECTIONS, "connection" => connection.clone(), "state" => state);
        Self {
            busy: state("busy"),
            idle: state("idle"),
            connecting: state("connecting"),
            waiting: metrics::gauge!(POOL_WAITING, "connection" => connection.clone()),
        }
    }

    fn set(&self, c: Counts) {
        self.busy.set(f64::from(c.busy));
        self.idle.set(f64::from(c.idle));
        self.connecting.set(f64::from(c.connecting));
        self.waiting.set(f64::from(c.waiting));
    }
}

/// Everything behind the lock.
pub(crate) struct State {
    pub(crate) closed: bool,
    /// Indexed by target index; only grows.
    pub(crate) targets: Vec<TargetState>,
    /// The sums of every target's counts.
    pub(crate) totals: Counts,
    /// Orders waiters across targets (first come, first served).
    pub(crate) next_ticket: u64,
    /// When the reaper will look next; `None` while it waits for a notification.
    pub(crate) reaper_wake: Option<Instant>,
    /// Targets whose counts changed in this locked section.
    pub(crate) dirty: Vec<usize>,
}

impl State {
    /// Applies `f` to target `t`'s counts and to the totals.
    pub(crate) fn bump(&mut self, t: usize, f: impl Fn(&mut Counts)) {
        if let Some(ts) = self.targets.get_mut(t) {
            f(&mut ts.counts);
        }
        f(&mut self.totals);
        self.mark(t);
    }

    /// Notes that target `t`'s counts changed, so its gauges are set on unlock.
    pub(crate) fn mark(&mut self, t: usize) {
        if let Some(ts) = self.targets.get_mut(t)
            && !ts.dirty
        {
            ts.dirty = true;
            self.dirty.push(t);
        }
    }

    /// Sets the gauges of the targets whose counts changed.
    fn publish(&mut self) {
        for t in self.dirty.drain(..) {
            if let Some(ts) = self.targets.get_mut(t) {
                ts.dirty = false;
                ts.gauges.set(ts.counts);
            }
        }
    }

    /// Takes the next slot id of target `t`.
    pub(crate) fn next_slot(&mut self, t: usize) -> SlotId {
        let seq = self.targets.get_mut(t).map_or(0, |ts| {
            let seq = ts.next_seq;
            ts.next_seq += 1;
            seq
        });
        slot_id(t, seq)
    }
}

/// A reserved slot (counted as `connecting`) not yet owned by anyone. Has no `Drop`, so it is
/// safe to create and discard under the lock.
pub(crate) struct RawPermit {
    pub(crate) target: usize,
    /// An evicted connection to close before connecting.
    pub(crate) close_first: Option<ErasedConn>,
}

/// A reserved slot owned by a request. Dropping it frees the slot and serves the next waiter;
/// [`complete`](Self::complete) turns it into a busy connection.
pub(crate) struct ConnectPermit {
    core: Arc<Core>,
    target: usize,
    close_first: Option<ErasedConn>,
    armed: bool,
}

impl ConnectPermit {
    /// Gives `raw` an owner.
    pub(crate) fn arm(core: Arc<Core>, raw: RawPermit) -> Self {
        Self {
            core,
            target: raw.target,
            close_first: raw.close_first,
            armed: true,
        }
    }

    /// Takes the permit back without freeing the slot (a waiter did not take it).
    pub(crate) fn disarm(mut self) -> RawPermit {
        self.armed = false;
        RawPermit {
            target: self.target,
            close_first: self.close_first.take(),
        }
    }

    /// The evicted connection to close before connecting, if any.
    pub(crate) fn take_close_first(&mut self) -> Option<ErasedConn> {
        self.close_first.take()
    }

    /// The connection is open: the slot becomes busy. `None` if the pool was closed meanwhile;
    /// the slot is freed and the caller closes the connection.
    pub(crate) fn complete(mut self) -> Option<SlotId> {
        self.armed = false;
        let core = Arc::clone(&self.core);
        let mut d = Deferred::default();
        let slot = {
            let mut guard = core.lock();
            let st = &mut *guard;
            st.bump(self.target, |c| c.connecting -= 1);
            if st.closed {
                None
            } else {
                st.bump(self.target, |c| c.busy += 1);
                let slot = st.next_slot(self.target);
                // One more open connection can lift `min` protection from idle ones.
                core.reschedule_reaper(st, self.target, &mut d);
                Some(slot)
            }
        };
        d.run(&core);
        slot
    }
}

impl Drop for ConnectPermit {
    fn drop(&mut self) {
        // Disarmed (taken back under the lock, or completed) with nothing to close: no lock.
        if !self.armed && self.close_first.is_none() {
            return;
        }
        let core = Arc::clone(&self.core);
        let mut d = Deferred::default();
        {
            let mut guard = core.lock();
            let st = &mut *guard;
            let budget = st
                .targets
                .get(self.target)
                .map_or(Duration::ZERO, |ts| ts.connect_timeout);
            if let Some(victim) = self.close_first.take() {
                d.close(victim, budget);
            }
            if self.armed {
                st.bump(self.target, |c| c.connecting -= 1);
                Core::dispatch(&core, st, Some(self.target), &mut d);
            }
        }
        d.run(&core);
    }
}

/// Work collected under the lock, run after it is released.
#[derive(Default)]
pub(crate) struct Deferred {
    closes: Vec<(ErasedConn, Duration)>,
    /// Values whose `Drop` takes the lock (leases and permits a waiter did not take).
    drops: Vec<Box<dyn Send>>,
    wake_reaper: bool,
}

impl Deferred {
    /// Closes `conn` in the background, giving up after `budget`.
    pub(crate) fn close(&mut self, conn: ErasedConn, budget: Duration) {
        self.closes.push((conn, budget));
    }

    /// Drops `value` once the lock is released.
    pub(crate) fn drop_later(&mut self, value: impl Send + 'static) {
        self.drops.push(Box::new(value));
    }

    /// Runs the collected work. Call only after the lock is released.
    pub(crate) fn run(self, core: &Core) {
        let Self {
            closes,
            drops,
            wake_reaper,
        } = self;
        drop(drops);
        for (conn, budget) in closes {
            core.spawn_close(conn, budget);
        }
        if wake_reaper {
            core.reaper_notify.notify_one();
        }
    }
}

#[cfg(test)]
thread_local! {
    /// Set while this thread holds the pool lock, to turn a re-entrant lock (a deadlock) into
    /// a test failure.
    static IN_LOCK: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// The locked state. Releasing it sets the gauges of the targets that changed and, in test
/// builds, checks the pool's invariants.
pub(crate) struct Locked<'a> {
    guard: MutexGuard<'a, State>,
    #[cfg(test)]
    core: &'a Core,
}

impl Deref for Locked<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        &self.guard
    }
}

impl DerefMut for Locked<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.guard
    }
}

impl Drop for Locked<'_> {
    fn drop(&mut self) {
        if !self.guard.dirty.is_empty() {
            self.guard.publish();
        }
        #[cfg(test)]
        {
            if !std::thread::panicking() {
                self.core.check_invariants(&self.guard);
            }
            IN_LOCK.with(|f| f.set(false));
        }
    }
}

/// The shared pool state and settings.
pub(crate) struct Core {
    state: Mutex<State>,
    pub(crate) scope: Scope,
    /// `pool.min` under `total`, otherwise 0.
    pub(crate) global_min: u32,
    /// `pool.max` under `total`, otherwise unlimited.
    pub(crate) global_max: Limit,
    pub(crate) hold: Duration,
    pub(crate) acquire_timeout: Duration,
    /// Called under the lock.
    eviction: Box<dyn EvictionPolicy>,
    /// Outside `Core`'s lifetime so the reaper can wait on it holding only a `Weak<Core>`.
    pub(crate) reaper_notify: Arc<Notify>,
    /// The runtime of the first `acquire`, for the reaper and background closes.
    runtime: OnceLock<Handle>,
    /// Whether the reaper task is running (tests only).
    #[cfg(test)]
    pub(crate) reaper_alive: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for Core {
    fn drop(&mut self) {
        // The reaper wakes, fails to upgrade its `Weak<Core>` and exits.
        self.reaper_notify.notify_one();
    }
}

impl Core {
    fn new(settings: &PoolSettings, eviction: Box<dyn EvictionPolicy>) -> Self {
        let (global_min, global_max) = match settings.scope {
            Scope::PerUri => (0, Limit::Unlimited),
            Scope::Total => (settings.min, settings.max),
        };
        Self {
            state: Mutex::new(State {
                closed: false,
                targets: Vec::new(),
                totals: Counts::default(),
                next_ticket: 0,
                reaper_wake: None,
                dirty: Vec::new(),
            }),
            scope: settings.scope,
            global_min,
            global_max,
            hold: settings.hold,
            acquire_timeout: settings.acquire_timeout,
            eviction,
            reaper_notify: Arc::new(Notify::new()),
            runtime: OnceLock::new(),
            #[cfg(test)]
            reaper_alive: Arc::default(),
        }
    }

    /// Takes the lock. A poisoned lock is taken anyway: user code runs under it only before any
    /// count changes (`is_usable`, `EvictionPolicy::choose`), so the state is consistent.
    pub(crate) fn lock(&self) -> Locked<'_> {
        #[cfg(test)]
        IN_LOCK.with(|f| {
            assert!(
                !f.get(),
                "the pool lock was taken again on the same thread: a lease or permit was dropped under the lock"
            );
        });
        let guard = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        IN_LOCK.with(|f| f.set(true));
        Locked {
            guard,
            #[cfg(test)]
            core: self,
        }
    }

    /// Captures the current tokio runtime on first use and starts the reaper there. `false`
    /// when called outside a tokio runtime.
    pub(crate) fn ensure_runtime(self: &Arc<Self>) -> bool {
        if self.runtime.get().is_some() {
            return true;
        }
        let Ok(handle) = Handle::try_current() else {
            return false;
        };
        if self.runtime.set(handle.clone()).is_ok() {
            handle.spawn(reaper::run(
                Arc::downgrade(self),
                Arc::clone(&self.reaper_notify),
            ));
        }
        true
    }

    /// Closes `conn` in a background task, giving up after `budget`. Without a runtime (no
    /// `acquire` ever ran) the connection is just dropped.
    pub(crate) fn spawn_close(&self, conn: ErasedConn, budget: Duration) {
        let handle = self
            .runtime
            .get()
            .cloned()
            .or_else(|| Handle::try_current().ok());
        if let Some(handle) = handle {
            let closing = conn.close();
            handle.spawn(async move {
                let _ = tokio::time::timeout(budget, closing).await;
            });
        }
    }

    /// Whether target `t` may open one more connection.
    pub(crate) fn can_open(&self, st: &State, t: usize) -> bool {
        st.targets.get(t).is_some_and(|ts| {
            ts.max.allows(ts.counts.used())
                && (self.scope == Scope::PerUri || self.global_max.allows(st.totals.used()))
        })
    }

    /// Whether closing one idle connection of `t` would go below a `min`. `uncounted` is 1 when
    /// the connection in question is not in the counts yet (it is being released).
    pub(crate) fn protected(&self, st: &State, t: usize, uncounted: u32) -> bool {
        st.targets.get(t).is_some_and(|ts| {
            ts.counts.open() + uncounted <= ts.min
                || (self.scope == Scope::Total && st.totals.open() + uncounted <= self.global_min)
        })
    }

    /// Reserves a slot for target `t`.
    pub(crate) fn reserve(st: &mut State, t: usize, close_first: Option<ErasedConn>) -> RawPermit {
        st.bump(t, |c| c.connecting += 1);
        RawPermit {
            target: t,
            close_first,
        }
    }

    /// Under `total`: asks the eviction policy for an idle connection of another target to
    /// close so that `t` can open one, and removes it from the pool.
    pub(crate) fn evict_for(&self, st: &mut State, t: usize) -> Option<ErasedConn> {
        let chosen = {
            let requested = &st.targets.get(t)?.target;
            let mut views = Vec::with_capacity(st.totals.idle as usize);
            for (i, ts) in st.targets.iter().enumerate() {
                if i == t || ts.counts.idle == 0 {
                    continue;
                }
                let at_min = ts.counts.open() <= ts.min;
                ts.store.idle_entries(&mut |slot, since| {
                    views.push(IdleView {
                        slot,
                        target: &ts.target,
                        idle_since: since.into_std(),
                        at_min,
                    });
                });
            }
            if views.is_empty() {
                return None;
            }
            self.eviction.choose(requested, &views)?
        };
        let victim = slot_target(chosen);
        if victim == t {
            return None;
        }
        let conn = st.targets.get_mut(victim)?.store.take_idle(chosen)?;
        st.bump(victim, |c| c.idle -= 1);
        Some(conn)
    }

    /// Under `total`: whether some target has a live waiter and room under its own `max`.
    pub(crate) fn has_servable_waiters(&self, st: &mut State) -> bool {
        let mut pruned = 0;
        let mut found = false;
        for (i, ts) in st.targets.iter_mut().enumerate() {
            if ts.counts.waiting == 0 || !ts.max.allows(ts.counts.used()) {
                continue;
            }
            let (head, dropped) = ts.store.live_head();
            ts.counts.waiting -= dropped;
            pruned += dropped;
            if dropped > 0 && !ts.dirty {
                ts.dirty = true;
                st.dirty.push(i);
            }
            if head.is_some() {
                found = true;
                break;
            }
        }
        st.totals.waiting -= pruned;
        found
    }

    /// Serves waiting requests from freed capacity. Runs after every event that frees a slot
    /// or, under `total`, adds an idle connection that could be evicted. O(1) when nobody
    /// waits.
    pub(crate) fn dispatch(
        core: &Arc<Core>,
        st: &mut State,
        hint: Option<usize>,
        d: &mut Deferred,
    ) {
        if st.totals.waiting == 0 {
            return;
        }
        match core.scope {
            // Capacity is per target, so only the target that freed it can use it.
            Scope::PerUri => {
                let Some(t) = hint else { return };
                while core.can_open(st, t) {
                    let Some(ts) = st.targets.get_mut(t) else {
                        return;
                    };
                    let (head, dropped) = ts.store.live_head();
                    st.bump(t, |c| c.waiting -= dropped);
                    if head.is_none() {
                        return;
                    }
                    let permit = Core::reserve(st, t, None);
                    if !Self::grant(core, st, permit, d) {
                        return;
                    }
                }
            }
            // Serve the earliest live waiter among targets with room under their own `max`.
            Scope::Total => loop {
                let mut best: Option<(u64, usize)> = None;
                let mut pruned = 0;
                for (i, ts) in st.targets.iter_mut().enumerate() {
                    if ts.counts.waiting == 0 || !ts.max.allows(ts.counts.used()) {
                        continue;
                    }
                    let (head, dropped) = ts.store.live_head();
                    ts.counts.waiting -= dropped;
                    pruned += dropped;
                    if dropped > 0 && !ts.dirty {
                        ts.dirty = true;
                        st.dirty.push(i);
                    }
                    if let Some(ticket) = head
                        && best.is_none_or(|(b, _)| ticket < b)
                    {
                        best = Some((ticket, i));
                    }
                }
                st.totals.waiting -= pruned;
                let Some((_, t)) = best else { return };
                let permit = if core.global_max.allows(st.totals.used()) {
                    Core::reserve(st, t, None)
                } else if st.totals.idle > 0 {
                    match core.evict_for(st, t) {
                        Some(victim) => Core::reserve(st, t, Some(victim)),
                        None => return,
                    }
                } else {
                    return;
                };
                // A failed grant means target `t` has no live waiter left; the next round
                // looks at the other targets.
                Self::grant(core, st, permit, d);
            },
        }
    }

    /// Hands a reserved slot to the first live waiter of its target. On failure the slot is
    /// freed (and an evicted connection closed); returns whether a waiter took it.
    fn grant(core: &Arc<Core>, st: &mut State, permit: RawPermit, d: &mut Deferred) -> bool {
        let t = permit.target;
        let Some(ts) = st.targets.get_mut(t) else {
            return false;
        };
        let budget = ts.connect_timeout;
        let (result, removed) = ts.store.send_permit(core, permit);
        st.bump(t, |c| c.waiting -= removed);
        match result {
            Ok(()) => true,
            Err(raw) => {
                st.bump(t, |c| c.connecting -= 1);
                if let Some(victim) = raw.close_first {
                    d.close(victim, budget);
                }
                false
            }
        }
    }

    /// The earliest time an idle connection may expire: of target `only`, or of any target.
    fn next_expiry(&self, st: &State, only: Option<usize>) -> Option<Instant> {
        if self.scope == Scope::Total && st.totals.open() <= self.global_min {
            return None;
        }
        let candidates: &[TargetState] = match only {
            Some(t) => st.targets.get(t..=t).unwrap_or_default(),
            None => &st.targets,
        };
        candidates
            .iter()
            .filter(|ts| ts.counts.idle > 0 && ts.counts.open() > ts.min)
            .filter_map(|ts| ts.store.oldest_idle())
            .min()
            .map(|since| since + self.hold)
    }

    /// Wakes the reaper if target `t` now has an idle connection that expires before the
    /// reaper would look again.
    pub(crate) fn reschedule_reaper(&self, st: &mut State, t: usize, d: &mut Deferred) {
        // Under `total` with a floor, one more open connection can unprotect every target.
        let only = (self.scope == Scope::PerUri || self.global_min == 0).then_some(t);
        if let Some(at) = self.next_expiry(st, only)
            && st.reaper_wake.is_none_or(|wake| at < wake)
        {
            st.reaper_wake = Some(at);
            d.wake_reaper = true;
        }
    }

    /// Closes idle connections that have outlived `hold` (keeping `min`), oldest first.
    /// Returns when to look next and whether the reaper should exit.
    pub(crate) fn reap(
        core: &Arc<Core>,
        st: &mut State,
        now: Instant,
        d: &mut Deferred,
    ) -> (Option<Instant>, bool) {
        if st.closed {
            st.reaper_wake = None;
            return (None, true);
        }
        let mut closed_any = false;
        loop {
            let mut oldest: Option<(Instant, usize)> = None;
            for (i, ts) in st.targets.iter().enumerate() {
                if ts.counts.idle == 0 || core.protected(st, i, 0) {
                    continue;
                }
                if let Some(since) = ts.store.oldest_idle()
                    && since + core.hold <= now
                    && oldest.is_none_or(|(o, _)| since < o)
                {
                    oldest = Some((since, i));
                }
            }
            let Some((_, t)) = oldest else { break };
            let Some(ts) = st.targets.get_mut(t) else {
                break;
            };
            let Some(conn) = ts.store.pop_oldest_idle() else {
                break;
            };
            d.close(conn, ts.connect_timeout);
            st.bump(t, |c| c.idle -= 1);
            closed_any = true;
        }
        if closed_any && core.scope == Scope::Total {
            // Freed room under the total cap may serve a waiter on another target.
            Core::dispatch(core, st, None, d);
        }
        let next = core
            .next_expiry(st, None)
            .map(|at| at.max(now + REAP_RESOLUTION));
        st.reaper_wake = next;
        (next, false)
    }

    /// Asserts the invariants listed in the module docs.
    #[cfg(test)]
    fn check_invariants(&self, st: &State) {
        let mut sum = Counts::default();
        for (i, ts) in st.targets.iter().enumerate() {
            let c = ts.counts;
            sum.idle += c.idle;
            sum.busy += c.busy;
            sum.connecting += c.connecting;
            sum.waiting += c.waiting;
            let s = ts.store.check();
            assert_eq!(
                s.idle,
                c.idle as usize,
                "idle count of {}",
                ts.target.name()
            );
            assert_eq!(
                s.waiters,
                c.waiting as usize,
                "waiting count of {}",
                ts.target.name()
            );
            if let Some(max) = ts.max.finite() {
                assert!(
                    c.used() <= max,
                    "{} uses {} > max {max}",
                    ts.target.name(),
                    c.used()
                );
            }
            if s.live_waiters > 0 {
                assert_eq!(
                    c.idle,
                    0,
                    "{} has idle connections and waiters",
                    ts.target.name()
                );
                assert!(
                    !self.can_open(st, i),
                    "{} has room and waiters: {c:?}, totals {:?}",
                    ts.target.name(),
                    st.totals
                );
            }
            for slot in s.idle_slots {
                assert_eq!(
                    slot_target(slot),
                    i,
                    "slot {slot:?} filed under the wrong target"
                );
            }
        }
        assert_eq!(sum, st.totals, "totals");
        if self.scope == Scope::Total
            && let Some(max) = self.global_max.finite()
        {
            assert!(
                st.totals.used() <= max,
                "the pool uses {} > max {max}",
                st.totals.used()
            );
        }
        if st.closed {
            assert_eq!((st.totals.idle, st.totals.waiting), (0, 0), "closed pool");
        }
    }
}

/// The pool shared by every connection: limits, waiting, `hold` and eviction for all targets
/// of all protocols.
///
/// Register each protocol's connections with [`typed`](Self::typed) to get a
/// [`TypedPool`](super::TypedPool) that hands out that protocol's connections. Clones share the
/// same pool.
///
/// Needs a tokio runtime with time enabled for `acquire` and `close`; building it does not.
#[derive(Clone)]
pub struct ConnectionPool {
    pub(crate) core: Arc<Core>,
}

impl ConnectionPool {
    /// A pool with the given settings and the default eviction policy ([`LruEviction`]).
    pub fn new(settings: &PoolSettings) -> Self {
        Self::builder(settings).build()
    }

    /// A builder, to replace the eviction policy.
    pub fn builder(settings: &PoolSettings) -> ConnectionPoolBuilder {
        ConnectionPoolBuilder {
            settings: settings.clone(),
            eviction: Box::new(LruEviction),
        }
    }

    /// The state of the whole pool, every target together. Connections being opened count as
    /// busy.
    pub fn stats(&self) -> PoolStats {
        let t = self.core.lock().totals;
        PoolStats {
            busy: (t.busy + t.connecting) as usize,
            held: t.idle as usize,
            waiting: t.waiting as usize,
        }
    }

    /// Whether [`close`](Self::close) was called.
    pub fn is_closed(&self) -> bool {
        self.core.lock().closed
    }

    /// Closes the pool: new requests and those waiting fail with
    /// [`Error::PoolClosed`](crate::Error::PoolClosed), idle connections are closed now (each
    /// within its `connect_timeout`), and busy ones when their lease ends. Calling it again
    /// does nothing.
    ///
    /// Dropping the last handle without closing drops the idle connections without closing
    /// them gracefully.
    pub async fn close(&self) {
        let mut closes = Vec::new();
        {
            let mut guard = self.core.lock();
            let st = &mut *guard;
            if st.closed {
                return;
            }
            st.closed = true;
            for (i, ts) in st.targets.iter_mut().enumerate() {
                ts.store.drain_idle(&mut closes, ts.connect_timeout);
                ts.store.drop_waiters();
                ts.counts.idle = 0;
                ts.counts.waiting = 0;
                if !ts.dirty {
                    ts.dirty = true;
                    st.dirty.push(i);
                }
            }
            st.totals.idle = 0;
            st.totals.waiting = 0;
            st.reaper_wake = None;
        }
        self.core.reaper_notify.notify_one();
        for (conn, budget) in closes {
            let _ = tokio::time::timeout(budget, conn.close()).await;
        }
    }
}

impl fmt::Debug for ConnectionPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPool")
            .field("scope", &self.core.scope)
            .field("hold", &self.core.hold)
            .field("acquire_timeout", &self.core.acquire_timeout)
            .finish_non_exhaustive()
    }
}

/// Builds a [`ConnectionPool`] with a custom eviction policy.
pub struct ConnectionPoolBuilder {
    settings: PoolSettings,
    eviction: Box<dyn EvictionPolicy>,
}

impl ConnectionPoolBuilder {
    /// Replaces the eviction policy (default [`LruEviction`]).
    ///
    /// The policy runs while the pool is locked: it must be quick and must not call back into
    /// the pool.
    pub fn eviction(mut self, policy: impl EvictionPolicy) -> Self {
        self.eviction = Box::new(policy);
        self
    }

    /// The pool.
    pub fn build(self) -> ConnectionPool {
        ConnectionPool {
            core: Arc::new(Core::new(&self.settings, self.eviction)),
        }
    }
}

impl fmt::Debug for ConnectionPoolBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionPoolBuilder")
            .field("settings", &self.settings)
            .finish_non_exhaustive()
    }
}
