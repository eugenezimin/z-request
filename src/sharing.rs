//! Connections shared by several pooled slots: how both transports map the pool's one-request
//! leases onto sockets.
//!
//! Each configured connection is a [`Peer`]. Its sockets are [`Group`]s: one HTTP client (a
//! reqwest client or a tonic channel) each, carrying one socket. A pooled slot holds a [`Share`]
//! of a group. Over HTTP/1.1 a group carries one slot; over HTTP/2 up to `streams_per_connection`
//! slots, each request being one stream on the shared socket. A group's socket closes when its
//! last share is dropped.
//!
//! A group older than `max_connection_age` (±10 %) takes no new slots, and its slots report
//! themselves unusable, so the pool closes them as they come back and later requests open a
//! fresh socket, which a load balancer may place on another replica.

// The transports adopt the groups in the following commits.
#![allow(dead_code)]

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};
use std::time::Duration;

use tokio::time::Instant;

use crate::utils::spi::Target;

/// What a peer is known to speak.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
pub(crate) enum Negotiated {
    /// Not known yet (no response seen).
    Unknown,
    /// HTTP/1.1: one request per socket.
    Http1,
    /// HTTP/2: requests share sockets.
    Http2,
}

/// One configured connection's sockets.
pub(crate) struct Peer<T> {
    name: Arc<str>,
    streams: u32,
    max_age: Option<Duration>,
    sockets: metrics::Gauge,
    state: Mutex<PeerState<T>>,
}

struct PeerState<T> {
    negotiated: Negotiated,
    /// The shared groups; a dead entry is a group whose last share was dropped.
    groups: Vec<Weak<Group<T>>>,
}

impl<T> fmt::Debug for Peer<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Peer")
            .field("connection", &self.name)
            .field("negotiated", &self.negotiated())
            .finish_non_exhaustive()
    }
}

/// One socket's client, shared by up to `capacity` slots.
pub(crate) struct Group<T> {
    client: T,
    peer: Arc<Peer<T>>,
    capacity: AtomicU32,
    slots: AtomicU32,
    retired: AtomicBool,
    retire_at: Option<Instant>,
}

impl<T> Drop for Group<T> {
    fn drop(&mut self) {
        self.peer.sockets.decrement(1.0);
    }
}

/// A pooled slot's share of a group.
pub(crate) struct Share<T> {
    group: Arc<Group<T>>,
}

impl<T> Drop for Share<T> {
    fn drop(&mut self) {
        self.group.slots.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<T> fmt::Debug for Share<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Share")
            .field("connection", &self.group.peer.name)
            .field("capacity", &self.group.capacity.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl<T> Share<T> {
    /// The group's client.
    pub(crate) fn client(&self) -> &T {
        &self.group.client
    }

    /// Whether the slot may serve another request: its group is neither retired nor too old.
    pub(crate) fn usable(&self) -> bool {
        self.group.usable(Instant::now())
    }

    /// Retires the group: it takes no new slots, and its slots are closed as they come back.
    pub(crate) fn retire(&self) {
        self.group.retired.store(true, Ordering::Release);
    }

    /// Whether this group carries more than one request at a time.
    #[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
    pub(crate) fn is_shared(&self) -> bool {
        self.group.capacity.load(Ordering::Acquire) > 1
    }

    /// The peer this share belongs to.
    #[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
    pub(crate) fn peer(&self) -> &Peer<T> {
        &self.group.peer
    }
}

impl<T> Group<T> {
    fn usable(&self, now: Instant) -> bool {
        !self.retired.load(Ordering::Acquire) && self.retire_at.is_none_or(|at| now < at)
    }
}

impl<T> Peer<T> {
    /// The sockets of `target`, `streams` requests per HTTP/2 socket, each socket retired after
    /// `max_age`. `negotiated` is what the peer is known to speak from the start.
    pub(crate) fn new(
        target: &Target,
        transport: &'static str,
        negotiated: Negotiated,
        streams: u32,
        max_age: Option<Duration>,
    ) -> Arc<Self> {
        let name = target.shared_name();
        let sockets = metrics::gauge!(
            crate::telemetry::SOCKETS,
            "connection" => metrics::SharedString::from(Arc::clone(&name)),
            "protocol" => transport,
        );
        sockets.set(0.0);
        Arc::new(Self {
            name,
            streams: streams.max(1),
            max_age,
            sockets,
            state: Mutex::new(PeerState {
                negotiated,
                groups: Vec::new(),
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, PeerState<T>> {
        // The state is replaced field by field with no user code in between: always valid.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// What the peer is known to speak.
    pub(crate) fn negotiated(&self) -> Negotiated {
        self.lock().negotiated
    }

    /// A share of a group with room for one more slot, if there is one.
    pub(crate) fn attach(&self) -> Option<Share<T>> {
        let now = Instant::now();
        let mut st = self.lock();
        st.groups.retain(|g| g.strong_count() > 0);
        for weak in &st.groups {
            let Some(group) = weak.upgrade() else {
                continue;
            };
            // Slots are only added under this lock, so the check and the add cannot race;
            // shares dropped meanwhile only make more room.
            if group.usable(now)
                && group.slots.load(Ordering::Acquire) < group.capacity.load(Ordering::Acquire)
            {
                group.slots.fetch_add(1, Ordering::AcqRel);
                return Some(Share { group });
            }
        }
        None
    }

    /// A new group around `client`, with its first share. Its capacity follows what the peer
    /// speaks: `streams` once HTTP/2 is known, otherwise 1. A `dedicated` group is never shared
    /// and never retired by age.
    pub(crate) fn add(self: &Arc<Self>, client: T, dedicated: bool) -> Share<T> {
        let mut st = self.lock();
        let capacity = match (dedicated, st.negotiated) {
            (false, Negotiated::Http2) => self.streams,
            _ => 1,
        };
        let retire_at = match (dedicated, self.max_age) {
            (false, Some(age)) => Some(Instant::now() + age.mul_f64(0.9 + fastrand::f64() * 0.2)),
            _ => None,
        };
        let group = Arc::new(Group {
            client,
            peer: Arc::clone(self),
            capacity: AtomicU32::new(capacity),
            slots: AtomicU32::new(1),
            retired: AtomicBool::new(false),
            retire_at,
        });
        if !dedicated {
            st.groups.push(Arc::downgrade(&group));
        }
        drop(st);
        self.sockets.increment(1.0);
        Share { group }
    }

    /// Records what a response on `share`'s socket was spoken in.
    ///
    /// The first HTTP/2 response lets every group carry `streams` requests. An HTTP/1.1
    /// response on a group that was sharing means the peer downgraded: the peer is treated as
    /// HTTP/1.1 again and its sharing groups are retired, because an HTTP/1.1 client would open
    /// a socket per concurrent request behind the pool's back.
    #[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
    pub(crate) fn learn(&self, share: &Share<T>, spoken: Negotiated) {
        let mut st = self.lock();
        match spoken {
            Negotiated::Http2 if st.negotiated != Negotiated::Http2 => {
                st.negotiated = Negotiated::Http2;
                for group in st.groups.iter().filter_map(Weak::upgrade) {
                    group.capacity.store(self.streams, Ordering::Release);
                }
                drop(st);
                tracing::info!(
                    connection = &*self.name,
                    streams = self.streams,
                    "peer speaks HTTP/2; sharing connections"
                );
            }
            Negotiated::Http1 if share.is_shared() || st.negotiated == Negotiated::Http2 => {
                st.negotiated = Negotiated::Http1;
                self.retire_shared(&st);
                drop(st);
                tracing::info!(connection = &*self.name, "peer downgraded to HTTP/1.1");
            }
            Negotiated::Http1 if st.negotiated == Negotiated::Unknown => {
                st.negotiated = Negotiated::Http1;
            }
            _ => {}
        }
    }

    /// Forgets what the peer speaks (a connection that expected HTTP/2 could not be made) and
    /// retires its sharing groups, so the next socket negotiates again.
    #[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
    pub(crate) fn forget(&self) {
        let mut st = self.lock();
        if st.negotiated == Negotiated::Http2 {
            st.negotiated = Negotiated::Unknown;
            self.retire_shared(&st);
            drop(st);
            tracing::info!(
                connection = &*self.name,
                "HTTP/2 connection failed; negotiating again"
            );
        }
    }

    #[cfg_attr(not(feature = "rest"), allow(dead_code))] // learning is REST's (gRPC is always HTTP/2)
    fn retire_shared(&self, st: &PeerState<T>) {
        for group in st.groups.iter().filter_map(Weak::upgrade) {
            if group.capacity.load(Ordering::Acquire) > 1 {
                group.retired.store(true, Ordering::Release);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(negotiated: Negotiated, streams: u32, age: Option<Duration>) -> Arc<Peer<u32>> {
        Peer::new(
            &Target::new("p", "http://p"),
            "rest",
            negotiated,
            streams,
            age,
        )
    }

    /// A share for the next slot: an existing group with room, or a new one.
    fn slot(peer: &Arc<Peer<u32>>, next: &mut u32) -> Share<u32> {
        peer.attach().unwrap_or_else(|| {
            *next += 1;
            peer.add(*next, false)
        })
    }

    #[test]
    fn http2_slots_share_sockets_up_to_the_stream_limit() {
        let p = peer(Negotiated::Http2, 3, None);
        let mut next = 0;
        let shares: Vec<_> = (0..7).map(|_| slot(&p, &mut next)).collect();
        let clients: Vec<u32> = shares.iter().map(|s| *s.client()).collect();
        assert_eq!(clients, [1, 1, 1, 2, 2, 2, 3]);
        drop(shares);
        assert!(p.attach().is_none(), "a group closes with its last share");
    }

    #[test]
    fn http1_slots_get_a_socket_each() {
        let p = peer(Negotiated::Http1, 32, None);
        let mut next = 0;
        let shares: Vec<_> = (0..3).map(|_| slot(&p, &mut next)).collect();
        assert_eq!(next, 3);
        assert!(shares.iter().all(|s| !s.is_shared()));
    }

    #[test]
    fn learning_http2_opens_existing_groups_to_sharing() {
        let p = peer(Negotiated::Unknown, 4, None);
        let mut next = 0;
        let first = slot(&p, &mut next);
        assert!(p.attach().is_none(), "unknown: one slot per socket");
        p.learn(&first, Negotiated::Http2);
        let second = slot(&p, &mut next);
        assert_eq!((*second.client(), next), (1, 1), "joined the first socket");
        assert_eq!(p.negotiated(), Negotiated::Http2);
    }

    #[test]
    fn a_downgrade_retires_sharing_groups() {
        let p = peer(Negotiated::Http2, 4, None);
        let mut next = 0;
        let a = slot(&p, &mut next);
        let b = slot(&p, &mut next);
        p.learn(&b, Negotiated::Http1);
        assert_eq!(p.negotiated(), Negotiated::Http1);
        assert!(!a.usable() && !b.usable(), "closed as they come back");
        let c = slot(&p, &mut next);
        assert_eq!(
            (next, c.is_shared()),
            (2, false),
            "a new socket, not shared"
        );
    }

    #[test]
    fn forgetting_http2_negotiates_again() {
        let p = peer(Negotiated::Http2, 4, None);
        let mut next = 0;
        let a = slot(&p, &mut next);
        p.forget();
        assert_eq!(p.negotiated(), Negotiated::Unknown);
        assert!(!a.usable());
    }

    #[tokio::test(start_paused = true)]
    async fn old_groups_take_no_new_slots() {
        let p = peer(Negotiated::Http2, 4, Some(Duration::from_secs(100)));
        let mut next = 0;
        let a = slot(&p, &mut next);
        tokio::time::sleep(Duration::from_secs(89)).await;
        assert!(a.usable(), "not before 90 % of the age");
        tokio::time::sleep(Duration::from_secs(22)).await;
        assert!(!a.usable(), "always by 110 % of the age");
        let b = slot(&p, &mut next);
        assert_eq!(*b.client(), 2, "a fresh socket");
    }

    #[test]
    fn dedicated_groups_are_neither_shared_nor_aged() {
        let p = peer(Negotiated::Http2, 4, Some(Duration::from_secs(10)));
        let d = p.add(9, true);
        assert!(!d.is_shared());
        assert!(p.attach().is_none(), "nobody joins a dedicated socket");
    }
}
