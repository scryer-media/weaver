// SPDX-License-Identifier: GPL-3.0-or-later

//! The session's connection budget: how many peer connections may be open at
//! once, and how many outgoing connects may begin each second. The listener
//! and every torrent's connect loop take from this one budget, so no torrent,
//! and no number of torrents, opens past it.
//!
//! Both halves serve waiters in arrival order. A torrent's connect loop waits
//! for one connection at a time, so arrival order hands freed slots and each
//! second's connects to the waiting torrents in turn.

use std::{num::NonZeroU32, sync::Arc, time::Duration};

use tokio::{
    sync::{AcquireError, Mutex, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// Peer connections, incoming and outgoing, open at once across the session.
/// Mirrors libtorrent's `connections_limit`.
pub(crate) const DEFAULT_CONNECTIONS_LIMIT: usize = 200;

/// Outgoing connects begun per second across the session. Mirrors
/// libtorrent's `connection_speed`.
pub(crate) const DEFAULT_CONNECTION_SPEED: NonZeroU32 = NonZeroU32::new(30).unwrap();

const WINDOW: Duration = Duration::from_secs(1);

pub(crate) struct ConnectionSlots {
    open: Arc<Semaphore>,
    connects: Mutex<ConnectWindow>,
    speed: NonZeroU32,
}

// The one-second window connects are counted in, and how many it has left.
struct ConnectWindow {
    start: Instant,
    left: u32,
}

/// One peer connection's place under the session cap, held from accept or
/// connect until the connection ends; dropping it frees the place.
pub(crate) struct ConnectionSlot {
    _permit: OwnedSemaphorePermit,
}

impl ConnectionSlots {
    pub(crate) fn new(limit: usize, speed: NonZeroU32) -> Self {
        Self {
            open: Arc::new(Semaphore::new(limit.min(Semaphore::MAX_PERMITS))),
            connects: Mutex::new(ConnectWindow {
                start: Instant::now(),
                left: speed.get(),
            }),
            speed,
        }
    }

    /// A place for a connection the listener accepted, or None when the
    /// session is at its cap and the connection is to be closed.
    pub(crate) fn try_open(&self) -> Option<ConnectionSlot> {
        let permit = self.open.clone().try_acquire_owned().ok()?;
        Some(ConnectionSlot { _permit: permit })
    }

    /// A place for an outgoing connection, once one is free.
    pub(crate) async fn open(&self) -> Result<ConnectionSlot, AcquireError> {
        let permit = self.open.clone().acquire_owned().await?;
        Ok(ConnectionSlot { _permit: permit })
    }

    /// Returns when one more outgoing connect may begin: at most `speed`
    /// begin in each one-second window.
    pub(crate) async fn begin_connect(&self) {
        let mut window = self.connects.lock().await;
        loop {
            let elapsed = Instant::now().duration_since(window.start);
            if elapsed >= WINDOW {
                let whole = u32::try_from(elapsed.as_secs()).unwrap_or(u32::MAX);
                window.start += WINDOW * whole;
                window.left = self.speed.get();
            }
            if window.left > 0 {
                window.left -= 1;
                return;
            }
            // Holding the lock: whoever waits behind it is next once this
            // window ends.
            tokio::time::sleep_until(window.start + WINDOW).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU32, sync::Arc};

    use futures::poll;
    use tokio::{sync::mpsc, time::Instant};

    use super::{ConnectionSlot, ConnectionSlots, WINDOW};

    fn slots(limit: usize, speed: u32) -> Arc<ConnectionSlots> {
        Arc::new(ConnectionSlots::new(limit, NonZeroU32::new(speed).unwrap()))
    }

    // A torrent's connect loop, one connection at a time: it takes a slot and
    // a connect, and hands the test the slot, which stands for the connection
    // until the test drops it.
    fn connect_loop(
        slots: Arc<ConnectionSlots>,
        torrent: char,
        granted: mpsc::UnboundedSender<(char, ConnectionSlot)>,
    ) {
        tokio::spawn(async move {
            loop {
                let slot = slots.open().await.unwrap();
                slots.begin_connect().await;
                if granted.send((torrent, slot)).is_err() {
                    return;
                }
            }
        });
    }

    #[tokio::test]
    async fn a_freed_slot_goes_to_the_waiter_not_a_newcomer() {
        let slots = slots(2, 1000);
        let a = slots.open().await.unwrap();
        let _b = slots.open().await.unwrap();
        assert!(slots.try_open().is_none(), "the cap is 2");

        let mut waiter = Box::pin(slots.open());
        assert!(poll!(&mut waiter).is_pending(), "the session is at its cap");
        drop(a);
        assert!(slots.try_open().is_none(), "the freed slot is the waiter's");
        let _handed = waiter.await.unwrap();
        assert!(slots.try_open().is_none());
    }

    #[tokio::test]
    async fn two_torrents_share_the_cap_and_freed_slots_alternate() {
        let slots = slots(2, 1000);
        let (granted_tx, mut granted) = mpsc::unbounded_channel();
        connect_loop(slots.clone(), 'a', granted_tx.clone());
        connect_loop(slots.clone(), 'b', granted_tx);

        // The cap holds across both torrents: two connections, then both wait.
        let mut open = vec![granted.recv().await.unwrap(), granted.recv().await.unwrap()];
        assert!(
            granted.try_recv().is_err(),
            "a third connection past the cap"
        );
        assert!(slots.try_open().is_none());

        // Each connection that ends frees a slot for the torrent next in turn.
        let mut order = Vec::new();
        for _ in 0..6 {
            open.remove(0);
            let next = granted.recv().await.unwrap();
            assert!(granted.try_recv().is_err(), "past the cap");
            order.push(next.0);
            open.push(next);
        }
        for pair in order.windows(2) {
            assert_ne!(pair[0], pair[1], "{order:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn connects_are_capped_per_second_and_shared_in_turn() {
        let slots = slots(1000, 3);
        let start = Instant::now();
        let (granted_tx, mut granted) = mpsc::unbounded_channel();
        connect_loop(slots.clone(), 'a', granted_tx.clone());
        connect_loop(slots.clone(), 'b', granted_tx);

        // The first window goes to whoever asks first, three connects, then
        // both torrents wait for the next second.
        let mut open = Vec::new();
        for _ in 0..3 {
            open.push(granted.recv().await.unwrap());
        }
        assert_eq!(Instant::now(), start, "no time passed");
        assert!(
            granted.try_recv().is_err(),
            "a fourth connect in the first second"
        );

        // Each later second: three more, served to the two torrents in turn.
        let mut order = Vec::new();
        for second in 1..=4u32 {
            tokio::time::advance(WINDOW).await;
            for _ in 0..3 {
                let next = granted.recv().await.unwrap();
                order.push(next.0);
                open.push(next);
            }
            assert_eq!(Instant::now(), start + WINDOW * second);
            assert!(
                granted.try_recv().is_err(),
                "more than 3 in second {second}"
            );
        }
        for pair in order.windows(2) {
            assert_ne!(pair[0], pair[1], "{order:?}");
        }
    }
}
