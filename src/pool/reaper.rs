//! The background task that closes idle connections once they outlive `hold`.
//!
//! One task per pool, started on the first `acquire`. It sleeps until the earliest expiry (or
//! until notified that an earlier one appeared), holds only a `Weak<Core>` while asleep, and
//! exits when the pool is dropped or closed.

use std::sync::{Arc, Weak};

use tokio::sync::Notify;
use tokio::time::{Instant, timeout_at};

use super::core::{Core, Deferred};

pub(crate) async fn run(core: Weak<Core>, notify: Arc<Notify>) {
    #[cfg(test)]
    let _alive = core.upgrade().map(|c| Alive::new(&c));
    loop {
        let next = {
            let Some(core) = core.upgrade() else { return };
            let mut d = Deferred::default();
            let (next, exit) = {
                let mut guard = core.lock();
                Core::reap(&core, &mut guard, Instant::now(), &mut d)
            };
            d.run(&core);
            if exit {
                return;
            }
            next
        };
        // `notify_one` stores a permit when nobody waits, so no wake-up is lost in between.
        match next {
            Some(at) => {
                let _ = timeout_at(at, notify.notified()).await;
            }
            None => notify.notified().await,
        }
    }
}

/// Marks the reaper as running for as long as it lives (tests only).
#[cfg(test)]
struct Alive(Arc<std::sync::atomic::AtomicBool>);

#[cfg(test)]
impl Alive {
    fn new(core: &Core) -> Self {
        core.reaper_alive
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Self(Arc::clone(&core.reaper_alive))
    }
}

#[cfg(test)]
impl Drop for Alive {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
