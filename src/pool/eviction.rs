//! The default [`EvictionPolicy`].

use crate::utils::spi::{EvictionPolicy, IdleView, SlotId, Target};

/// Closes the connection that has been idle longest, never one its connection needs to keep
/// its `min`. If every idle connection is needed, the request waits instead.
///
/// The pool asks only under `scope: total`, when the total cap is reached and a connection
/// needs room; under `per_uri` connections never compete for room.
#[derive(Debug, Default, Clone, Copy)]
pub struct LruEviction;

impl EvictionPolicy for LruEviction {
    fn choose(&self, _requested: &Target, idle: &[IdleView<'_>]) -> Option<SlotId> {
        idle.iter()
            .filter(|v| !v.at_min)
            .min_by_key(|v| v.idle_since)
            .map(|v| v.slot)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn picks_the_oldest_idle_connection_not_needed_for_min() {
        let a = Target::new("a", "http://a");
        let b = Target::new("b", "http://b");
        let t0 = Instant::now();
        let view = |slot, target, secs, at_min| IdleView {
            slot: SlotId(slot),
            target,
            idle_since: t0 + Duration::from_secs(secs),
            at_min,
        };
        let idle = [
            view(1, &a, 5, false),
            view(2, &b, 1, true), // oldest, but needed for `min`
            view(3, &b, 3, false),
        ];
        assert_eq!(LruEviction.choose(&a, &idle), Some(SlotId(3)));
        assert_eq!(
            LruEviction.choose(&a, &idle[1..2]),
            None,
            "all needed for min"
        );
        assert_eq!(LruEviction.choose(&a, &[]), None);
    }
}
