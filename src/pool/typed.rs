//! The per-protocol handle: [`TypedPool`] opens connections with its [`Connector`], stores them
//! with their real type, and implements [`Pool`] on top of the shared [`Core`].

use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::time::{Instant, timeout, timeout_at};

use super::core::{
    ConnectPermit, ConnectionPool, Core, Deferred, ErasedConn, MAX_TARGETS, RawPermit, State,
    TargetState, TargetStats, TargetStore, slot_target,
};
use crate::config::{ConnectionSettings, Limit};
use crate::utils::error::{ConfigError, Error, Problem};
use crate::utils::spi::{Connection, Connector, Lease, Pool, PoolStats, Release, SlotId, Target};

/// Hands out one protocol's connections from a shared [`ConnectionPool`].
///
/// Made by [`ConnectionPool::typed`]. Clones share the same handle. Each target belongs to
/// exactly one handle; other targets are unknown to it.
pub struct TypedPool<K: Connector>(Arc<TypedInner<K>>);

impl<K: Connector> Clone for TypedPool<K> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Connector> fmt::Debug for TypedPool<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TypedPool")
            .field("targets", &self.0.index.len())
            .finish_non_exhaustive()
    }
}

struct TypedInner<K: Connector> {
    pool: ConnectionPool,
    connector: K,
    /// Read without the lock; fixed at registration.
    index: HashMap<Target, LocalTarget>,
    /// Leases need an `Arc<dyn Release>` of this handle, also inside `release(&self)`.
    me: Weak<Self>,
}

#[derive(Debug, Clone, Copy)]
struct LocalTarget {
    idx: usize,
    connect_timeout: Duration,
}

/// What a waiting request receives.
enum Grant<C: Connection> {
    /// A connection released by another request.
    Conn(Lease<C>),
    /// A reserved slot to open a connection in.
    Connect(ConnectPermit),
}

struct Waiter<C: Connection> {
    ticket: u64,
    tx: oneshot::Sender<Grant<C>>,
}

struct IdleConn<C> {
    conn: C,
    slot: SlotId,
    since: Instant,
}

/// One target's idle connections (oldest first) and waiters (in ticket order).
struct Store<C: Connection> {
    idle: VecDeque<IdleConn<C>>,
    waiters: VecDeque<Waiter<C>>,
}

impl<C: Connection> Store<C> {
    fn new(max: Limit) -> Self {
        let held = max.finite().map_or(16, |m| m.min(64)) as usize;
        Self {
            idle: VecDeque::with_capacity(held),
            waiters: VecDeque::new(),
        }
    }
}

impl<C: Connection> TargetStore for Store<C> {
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn oldest_idle(&self) -> Option<Instant> {
        self.idle.front().map(|i| i.since)
    }

    fn idle_entries(&self, f: &mut dyn FnMut(SlotId, Instant)) {
        for i in &self.idle {
            f(i.slot, i.since);
        }
    }

    fn take_idle(&mut self, slot: SlotId) -> Option<ErasedConn> {
        let pos = self.idle.iter().position(|i| i.slot == slot)?;
        self.idle.remove(pos).map(|i| ErasedConn::new(i.conn))
    }

    fn pop_oldest_idle(&mut self) -> Option<ErasedConn> {
        self.idle.pop_front().map(|i| ErasedConn::new(i.conn))
    }

    fn drain_idle(&mut self, out: &mut Vec<(ErasedConn, Duration)>, budget: Duration) {
        out.extend(
            self.idle
                .drain(..)
                .map(|i| (ErasedConn::new(i.conn), budget)),
        );
    }

    fn live_head(&mut self) -> (Option<u64>, u32) {
        let mut dropped = 0;
        while let Some(w) = self.waiters.front() {
            if !w.tx.is_closed() {
                return (Some(w.ticket), dropped);
            }
            self.waiters.pop_front();
            dropped += 1;
        }
        (None, dropped)
    }

    fn send_permit(&mut self, core: &Arc<Core>, permit: RawPermit) -> (Result<(), RawPermit>, u32) {
        let mut permit = permit;
        let mut removed = 0;
        while let Some(w) = self.waiters.pop_front() {
            removed += 1;
            if w.tx.is_closed() {
                continue;
            }
            match w
                .tx
                .send(Grant::Connect(ConnectPermit::arm(Arc::clone(core), permit)))
            {
                Ok(()) => return (Ok(()), removed),
                // The waiter gave up between the check and the send; the value comes back.
                Err(Grant::Connect(p)) => permit = p.disarm(),
                Err(Grant::Conn(_)) => unreachable!("a permit was sent"),
            }
        }
        (Err(permit), removed)
    }

    fn remove_waiter(&mut self, ticket: u64) -> bool {
        match self.waiters.binary_search_by_key(&ticket, |w| w.ticket) {
            Ok(i) => self.waiters.remove(i).is_some(),
            Err(_) => false,
        }
    }

    fn drop_waiters(&mut self) -> u32 {
        let n = self.waiters.len() as u32;
        self.waiters.clear();
        n
    }

    #[cfg(test)]
    fn check(&self) -> super::core::StoreCheck {
        super::core::StoreCheck {
            idle: self.idle.len(),
            waiters: self.waiters.len(),
            live_waiters: self.waiters.iter().filter(|w| !w.tx.is_closed()).count(),
            idle_slots: self.idle.iter().map(|i| i.slot).collect(),
        }
    }
}

/// Target `t`'s store as its real type.
fn store_mut<C: Connection>(st: &mut State, t: usize) -> Option<&mut Store<C>> {
    st.targets.get_mut(t)?.store.as_any_mut().downcast_mut()
}

/// What `acquire` does after its locked section.
enum Step<C: Connection> {
    /// An idle connection.
    Ready(C, SlotId),
    /// A reserved slot to connect in.
    Connect(RawPermit),
    /// Queued.
    Wait(oneshot::Receiver<Grant<C>>, u64),
}

/// A queued request. Dropping it (the request timed out or was cancelled) takes it out of the
/// queue and gives back anything granted to it in the meantime.
struct WaitGuard<C: Connection> {
    rx: oneshot::Receiver<Grant<C>>,
    core: Arc<Core>,
    target: usize,
    ticket: u64,
    /// The grant was received, or the queue entry is already gone.
    done: bool,
}

impl<C: Connection> Drop for WaitGuard<C> {
    fn drop(&mut self) {
        if self.done {
            return;
        }
        // After `close`, a send fails and the sender keeps the value; a value sent before is
        // still received here.
        self.rx.close();
        let late = self.rx.try_recv().ok();
        {
            let mut guard = self.core.lock();
            let st = &mut *guard;
            let removed = st
                .targets
                .get_mut(self.target)
                .is_some_and(|ts| ts.store.remove_waiter(self.ticket));
            if removed {
                st.bump(self.target, |c| c.waiting -= 1);
            }
        }
        // Outside the lock: a lease goes back to the pool, a permit frees its slot.
        drop(late);
    }
}

enum WaitEnd<C: Connection> {
    Granted(Grant<C>),
    Closed,
    TimedOut,
}

impl<K: Connector> TypedInner<K> {
    async fn acquire(self: &Arc<Self>, target: &Target) -> Result<Lease<K::Conn>, Error> {
        let Some(&local) = self.index.get(target) else {
            return Err(Error::UnknownConnection {
                connection: target.name().to_owned(),
            });
        };
        let core = &self.pool.core;
        if !core.ensure_runtime() {
            return Err(Error::Connect {
                connection: target.name().to_owned(),
                reason: "the pool must be used inside a tokio runtime".into(),
            });
        }
        let start = Instant::now();
        let mut d = Deferred::default();
        let step = {
            let mut guard = core.lock();
            self.decide(&mut guard, local.idx, &mut d)
        };
        d.run(core);
        let grant = match step {
            None => return Err(pool_closed(target)),
            Some(Step::Ready(conn, slot)) => return Ok(self.lease(conn, slot)),
            Some(Step::Connect(raw)) => Grant::Connect(ConnectPermit::arm(Arc::clone(core), raw)),
            Some(Step::Wait(rx, ticket)) => {
                let mut guard = WaitGuard {
                    rx,
                    core: Arc::clone(core),
                    target: local.idx,
                    ticket,
                    done: false,
                };
                match wait(&mut guard, start + core.acquire_timeout).await {
                    WaitEnd::Granted(grant) => grant,
                    WaitEnd::Closed => return Err(pool_closed(target)),
                    WaitEnd::TimedOut => {
                        return Err(Error::AcquireTimeout {
                            connection: target.name().to_owned(),
                            waited: start.elapsed(),
                        });
                    }
                }
            }
        };
        match grant {
            Grant::Conn(lease) => Ok(lease),
            Grant::Connect(permit) => self.connect(permit, target, local).await,
        }
    }

    /// The locked part of `acquire`. `None` if the pool is closed.
    fn decide(&self, st: &mut State, t: usize, d: &mut Deferred) -> Option<Step<K::Conn>> {
        if st.closed {
            return None;
        }
        let core = &self.pool.core;
        // Serve earlier requests from any free capacity first, so this one cannot overtake them.
        let waiting = match core.scope {
            crate::config::Scope::PerUri => st.targets.get(t).map_or(0, |ts| ts.counts.waiting),
            crate::config::Scope::Total => st.totals.waiting,
        };
        if waiting > 0 {
            Core::dispatch(core, st, Some(t), d);
        }

        let budget = st.targets.get(t)?.connect_timeout;
        let mut closed_broken = false;
        let mut ready = None;
        while let Some(idle) = store_mut::<K::Conn>(st, t).and_then(|s| s.idle.pop_back()) {
            if idle.conn.is_usable() {
                st.bump(t, |c| {
                    c.idle -= 1;
                    c.busy += 1;
                });
                ready = Some(Step::Ready(idle.conn, idle.slot));
                break;
            }
            st.bump(t, |c| c.idle -= 1);
            d.close(ErasedConn::new(idle.conn), budget);
            closed_broken = true;
        }
        if closed_broken {
            Core::dispatch(core, st, Some(t), d);
        }
        if ready.is_some() {
            return ready;
        }

        if core.can_open(st, t) {
            return Some(Step::Connect(Core::reserve(st, t, None)));
        }
        if core.scope == crate::config::Scope::Total
            && st.totals.idle > 0
            && st
                .targets
                .get(t)?
                .max
                .allows(st.targets.get(t)?.counts.used())
            && !core.has_servable_waiters(st)
            && let Some(victim) = core.evict_for(st, t)
        {
            return Some(Step::Connect(Core::reserve(st, t, Some(victim))));
        }

        let (tx, rx) = oneshot::channel();
        let ticket = st.next_ticket;
        st.next_ticket += 1;
        store_mut::<K::Conn>(st, t)?
            .waiters
            .push_back(Waiter { ticket, tx });
        st.bump(t, |c| c.waiting += 1);
        Some(Step::Wait(rx, ticket))
    }

    /// Opens a connection in a reserved slot, within `connect_timeout` (which also covers
    /// closing an evicted connection first).
    async fn connect(
        self: &Arc<Self>,
        mut permit: ConnectPermit,
        target: &Target,
        local: LocalTarget,
    ) -> Result<Lease<K::Conn>, Error> {
        let victim = permit.take_close_first();
        let connector = &self.connector;
        let attempt = timeout(local.connect_timeout, async move {
            if let Some(victim) = victim {
                victim.close().await;
            }
            connector.connect(target).await
        })
        .await;
        // On error, dropping `permit` frees the slot and serves the next waiter.
        match attempt {
            Ok(Ok(conn)) => match permit.complete() {
                Some(slot) => Ok(self.lease(conn, slot)),
                None => {
                    let _ = timeout(local.connect_timeout, conn.close()).await;
                    Err(pool_closed(target))
                }
            },
            Ok(Err(e)) => Err(e),
            Err(_) => Err(Error::Connect {
                connection: target.name().to_owned(),
                reason: format!("no connection within {:?}", local.connect_timeout),
            }),
        }
    }

    fn lease(self: &Arc<Self>, conn: K::Conn, slot: SlotId) -> Lease<K::Conn> {
        let pool: Arc<dyn Release<K::Conn>> = Arc::clone(self) as _;
        Lease::new(conn, slot, pool)
    }

    /// Gives `conn` to the first live waiter of target `t`; returns it if nobody waits.
    fn hand_off(
        &self,
        st: &mut State,
        t: usize,
        conn: K::Conn,
        slot: SlotId,
        d: &mut Deferred,
    ) -> Option<K::Conn> {
        let Some(me) = self.me.upgrade() else {
            return Some(conn);
        };
        let Some(store) = store_mut::<K::Conn>(st, t) else {
            return Some(conn);
        };
        let mut removed = 0;
        let mut conn = Some(conn);
        while let Some(w) = store.waiters.pop_front() {
            removed += 1;
            if w.tx.is_closed() {
                continue;
            }
            if let Some(c) = conn.take() {
                let pool: Arc<dyn Release<K::Conn>> = me as _;
                if let Err(grant) = w.tx.send(Grant::Conn(Lease::new(c, slot, pool))) {
                    // The waiter gave up just now. The lease is still out (busy); dropping it
                    // after the lock releases it again, to the next waiter.
                    d.drop_later(grant);
                }
            }
            break;
        }
        st.bump(t, |c| c.waiting -= removed);
        if conn.is_none() {
            st.bump(t, |c| c.busy += 1);
        }
        conn
    }
}

impl<K: Connector> Release<K::Conn> for TypedInner<K> {
    fn release(&self, slot: SlotId, conn: K::Conn, usable: bool) {
        let core = &self.pool.core;
        let t = slot_target(slot);
        let mut d = Deferred::default();
        {
            let mut guard = core.lock();
            let st = &mut *guard;
            let Some(budget) = st.targets.get(t).map(|ts| ts.connect_timeout) else {
                drop(guard);
                d.drop_later(conn);
                d.run(core);
                return;
            };
            st.bump(t, |c| c.busy -= 1);
            if st.closed {
                d.close(ErasedConn::new(conn), budget);
            } else if !usable {
                d.close(ErasedConn::new(conn), budget);
                Core::dispatch(core, st, Some(t), &mut d);
            } else if let Some(conn) = self.hand_off(st, t, conn, slot, &mut d) {
                if core.hold.is_zero() && !core.protected(st, t, 1) {
                    d.close(ErasedConn::new(conn), budget);
                    Core::dispatch(core, st, Some(t), &mut d);
                } else if let Some(store) = store_mut::<K::Conn>(st, t) {
                    store.idle.push_back(IdleConn {
                        conn,
                        slot,
                        since: Instant::now(),
                    });
                    st.bump(t, |c| c.idle += 1);
                    core.reschedule_reaper(st, t, &mut d);
                    if core.scope == crate::config::Scope::Total {
                        // Another target waiting at the total cap may evict it.
                        Core::dispatch(core, st, Some(t), &mut d);
                    }
                }
            }
        }
        d.run(core);
    }
}

async fn wait<C: Connection>(guard: &mut WaitGuard<C>, deadline: Instant) -> WaitEnd<C> {
    match timeout_at(deadline, &mut guard.rx).await {
        Ok(Ok(grant)) => {
            guard.done = true;
            WaitEnd::Granted(grant)
        }
        // The pool dropped the queue entry: it was closed.
        Ok(Err(_)) => {
            guard.done = true;
            WaitEnd::Closed
        }
        Err(_) => {
            // A grant that arrived together with the deadline still counts.
            guard.rx.close();
            match guard.rx.try_recv() {
                Ok(grant) => {
                    guard.done = true;
                    WaitEnd::Granted(grant)
                }
                Err(_) => WaitEnd::TimedOut,
            }
        }
    }
}

fn pool_closed(target: &Target) -> Error {
    Error::PoolClosed {
        connection: target.name().to_owned(),
    }
}

impl<K: Connector> Pool<K::Conn> for TypedPool<K> {
    fn acquire(
        &self,
        target: &Target,
    ) -> impl Future<Output = Result<Lease<K::Conn>, Error>> + Send {
        self.0.acquire(target)
    }

    fn stats(&self) -> PoolStats {
        self.0.pool.stats()
    }
}

impl<K: Connector> TypedPool<K> {
    /// The state of one target, or `None` if it is not registered with this handle.
    pub fn target_stats(&self, target: &Target) -> Option<TargetStats> {
        let local = self.0.index.get(target)?;
        let guard = self.0.pool.core.lock();
        guard.targets.get(local.idx).map(|ts| ts.counts.into())
    }

    /// The shared pool this handle belongs to.
    pub fn pool(&self) -> &ConnectionPool {
        &self.0.pool
    }
}

impl ConnectionPool {
    /// Registers `connections` (all of the protocol `connector` speaks) and returns the handle
    /// that hands out their connections.
    ///
    /// Each connection is one target with its own `min`/`max` and `connect_timeout`. Nothing is
    /// opened yet: connections open on demand.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] if a connection is already registered with this pool, or if the pool
    /// would hold more than 65 536 connections.
    pub fn typed<'a, K: Connector>(
        &self,
        connector: K,
        connections: impl IntoIterator<Item = &'a ConnectionSettings>,
    ) -> Result<TypedPool<K>, Error> {
        let connections: Vec<&ConnectionSettings> = connections.into_iter().collect();
        let mut index = HashMap::with_capacity(connections.len());
        {
            let mut guard = self.core.lock();
            let st = &mut *guard;
            let mut problems = Vec::new();
            for (i, c) in connections.iter().enumerate() {
                let taken = st.targets.iter().any(|t| t.target.name() == c.name)
                    || connections.iter().take(i).any(|o| o.name == c.name);
                if taken {
                    problems.push(Problem {
                        path: format!("connections.{}", c.name),
                        message: "is already registered with this pool".into(),
                    });
                }
            }
            if st.targets.len() + connections.len() > MAX_TARGETS {
                problems.push(Problem {
                    path: "connections".into(),
                    message: format!("a pool holds at most {MAX_TARGETS} connections"),
                });
            }
            if let Some(e) = ConfigError::from_problems(problems) {
                return Err(e.into());
            }
            for c in connections {
                let target = c.target();
                let idx = st.targets.len();
                let gauges = super::core::PoolGauges::new(&target);
                index.insert(
                    target.clone(),
                    LocalTarget {
                        idx: st.targets.len(),
                        connect_timeout: c.connect_timeout,
                    },
                );
                st.targets.push(TargetState {
                    target,
                    min: c.min,
                    max: c.max,
                    connect_timeout: c.connect_timeout,
                    counts: Default::default(),
                    next_seq: 0,
                    store: Box::new(Store::<K::Conn>::new(c.max)),
                    gauges,
                    dirty: false,
                });
                st.mark(idx); // report the zeros from the start
            }
        }
        Ok(TypedPool(Arc::new_cyclic(|me| TypedInner {
            pool: self.clone(),
            connector,
            index,
            me: me.clone(),
        })))
    }
}
