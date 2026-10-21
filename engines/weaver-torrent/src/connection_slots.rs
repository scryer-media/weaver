// SPDX-License-Identifier: GPL-3.0-or-later

//! The session's connection budget: how many peer connections may be open at
//! once, and how many outgoing connects may begin each second. The listener
//! and every torrent's connect loop take from this one budget, so no torrent,
//! and no number of torrents, opens past it.
//!
//! Both halves serve waiters in arrival order. A torrent's connect loop waits
//! for one connection at a time, so arrival order hands freed slots and each
//! second's connects to the waiting torrents in turn.
//!
//! Both limits change on a running session (libtorrent
//! `session_impl::update_connections_limit`, `update_connection_speed`). A
//! lower cap closes no connection: each one past it gives its place up as it
//! ends, and no new one opens until the session is under the cap.

use std::{
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicUsize, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex as SyncMutex;

use tokio::{
    sync::{AcquireError, Mutex, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

/// Peer connections, incoming and outgoing, open at once across the session.
/// Mirrors libtorrent's `connections_limit`.
pub(crate) const DEFAULT_CONNECTIONS_LIMIT: usize = 200;

/// Incoming connections past `connections_limit` the session takes, each to
/// replace a connection of a torrent that holds more than the incoming one's
/// does. Mirrors libtorrent's `connections_slack`.
pub(crate) const DEFAULT_CONNECTIONS_SLACK: usize = 10;

/// Outgoing connects begun per second across the session. Mirrors
/// libtorrent's `connection_speed`.
pub(crate) const DEFAULT_CONNECTION_SPEED: NonZeroU32 = NonZeroU32::new(30).unwrap();

const WINDOW: Duration = Duration::from_secs(1);

pub(crate) struct ConnectionSlots {
    cap: Arc<SyncMutex<Cap>>,
    open: Arc<Semaphore>,
    // The places past the cap an incoming connection may take until it
    // replaces another (libtorrent `connections_slack`).
    slack: Arc<Semaphore>,
    connects: Mutex<ConnectWindow>,
    speed: AtomicU32,
    // The callers of `begin_connect` not yet let begin, each holding the
    // slot of its connection.
    waiting: AtomicUsize,
}

// The cap, and the places past it that connections opened under a higher
// one still hold: each is given up, not freed, as its connection ends.
struct Cap {
    limit: usize,
    owed: usize,
}

// The one-second window connects are counted in, and how many it has left,
// set by the first connect that begins in it.
struct ConnectWindow {
    start: Instant,
    left: Option<u32>,
}

// A caller of `begin_connect`, counted while it waits.
struct Waiting<'a>(&'a AtomicUsize);

impl<'a> Waiting<'a> {
    fn new(waiting: &'a AtomicUsize) -> Self {
        waiting.fetch_add(1, Ordering::Relaxed);
        Self(waiting)
    }
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One peer connection's place under the session cap, or in the slack past
/// it, held from accept or connect until the connection ends; dropping it
/// frees the place, or gives it up when the cap was lowered past it.
pub(crate) struct ConnectionSlot {
    permit: Option<OwnedSemaphorePermit>,
    // The cap of a place under it; none for one in the slack.
    cap: Option<Arc<SyncMutex<Cap>>>,
}

impl Drop for ConnectionSlot {
    fn drop(&mut self) {
        let Some(cap) = &self.cap else {
            return;
        };
        // Under the cap's lock, so a lower cap counts this place either as
        // freed or as owed, never as neither.
        let mut cap = cap.lock();
        let permit = self.permit.take();
        if cap.owed > 0 {
            cap.owed -= 1;
            if let Some(permit) = permit {
                permit.forget();
            }
        } else {
            drop(permit);
        }
    }
}

impl ConnectionSlots {
    pub(crate) fn new(limit: usize, slack: usize, speed: NonZeroU32) -> Self {
        let limit = limit.min(Semaphore::MAX_PERMITS);
        Self {
            cap: Arc::new(SyncMutex::new(Cap { limit, owed: 0 })),
            open: Arc::new(Semaphore::new(limit)),
            slack: Arc::new(Semaphore::new(slack.min(Semaphore::MAX_PERMITS))),
            connects: Mutex::new(ConnectWindow {
                start: Instant::now(),
                left: None,
            }),
            speed: AtomicU32::new(speed.get()),
            waiting: AtomicUsize::new(0),
        }
    }

    /// Hold the session to `limit` connections from now on (libtorrent
    /// `session_impl::update_connections_limit`). Places freed above it are
    /// handed out at once; places in use below it are given up as their
    /// connections end.
    pub(crate) fn set_limit(&self, limit: usize) {
        let limit = limit.min(Semaphore::MAX_PERMITS);
        let mut cap = self.cap.lock();
        if limit >= cap.limit {
            let more = limit - cap.limit;
            let repaid = more.min(cap.owed);
            cap.owed -= repaid;
            self.open.add_permits(more - repaid);
        } else {
            let fewer = cap.limit - limit;
            cap.owed += fewer - self.open.forget_permits(fewer);
        }
        cap.limit = limit;
    }

    /// Begin at most `speed` outgoing connects each second from the next
    /// window on (libtorrent `connection_speed`).
    pub(crate) fn set_speed(&self, speed: NonZeroU32) {
        self.speed.store(speed.get(), Ordering::Relaxed);
    }

    /// The outgoing connects begun each second at most.
    pub(crate) fn speed(&self) -> NonZeroU32 {
        NonZeroU32::new(self.speed.load(Ordering::Relaxed)).unwrap_or(NonZeroU32::MIN)
    }

    fn under_cap(&self, permit: OwnedSemaphorePermit) -> ConnectionSlot {
        ConnectionSlot {
            permit: Some(permit),
            cap: Some(Arc::clone(&self.cap)),
        }
    }

    /// A free place under the cap, or None when the session is at it.
    pub(crate) fn try_open(&self) -> Option<ConnectionSlot> {
        let permit = self.open.clone().try_acquire_owned().ok()?;
        Some(self.under_cap(permit))
    }

    /// A place for a connection the listener accepted: one under the cap, or
    /// past it in the slack; None when the session holds `connections_limit
    /// + connections_slack` and the connection is to be closed (libtorrent
    /// `session_impl::incoming_connection`).
    pub(crate) fn try_accept(&self) -> Option<ConnectionSlot> {
        self.try_open().or_else(|| {
            let permit = self.slack.clone().try_acquire_owned().ok()?;
            Some(ConnectionSlot {
                permit: Some(permit),
                cap: None,
            })
        })
    }

    /// Whether `slot` is one of the slack's, past the cap (libtorrent
    /// `peer_connection::m_exceeded_limit`, set by
    /// `session_impl::incoming_connection`).
    pub(crate) fn exceeds_limit(&self, slot: &ConnectionSlot) -> bool {
        slot.cap.is_none()
    }

    /// How many more connections the session may open now (libtorrent
    /// `connections_limit` less `session_impl::num_connections`).
    pub(crate) fn free(&self) -> usize {
        self.open.available_permits()
    }

    /// The most connections the session may hold (libtorrent
    /// `connections_limit`).
    pub(crate) fn limit(&self) -> usize {
        self.cap.lock().limit
    }

    /// How many connections the session holds (libtorrent
    /// `session_impl::num_connections`).
    pub(crate) fn in_use(&self) -> usize {
        let cap = self.cap.lock();
        (cap.limit + cap.owed).saturating_sub(self.free())
    }

    /// A place for an outgoing connection, once one is free.
    pub(crate) async fn open(&self) -> Result<ConnectionSlot, AcquireError> {
        let permit = self.open.clone().acquire_owned().await?;
        Ok(self.under_cap(permit))
    }

    /// Returns when one more outgoing connect may begin, for a caller that
    /// holds the slot of its connection: at most `speed` begin in each
    /// one-second window, and no more than half the slots free when the
    /// window's first begins, rounded up, so that the session does not fill
    /// every slot at once and time them out together (libtorrent
    /// `session_impl::try_connect_more_peers`, `smooth_connects` on). The
    /// slot of a connect yet to begin is free: libtorrent counts no
    /// connection before its connect.
    pub(crate) async fn begin_connect(&self) {
        let _waiting = Waiting::new(&self.waiting);
        let mut window = self.connects.lock().await;
        loop {
            let elapsed = Instant::now().duration_since(window.start);
            if elapsed >= WINDOW {
                let whole = u32::try_from(elapsed.as_secs()).unwrap_or(u32::MAX);
                window.start += WINDOW * whole;
                window.left = None;
            }
            let left = window.left.get_or_insert_with(|| {
                let free = self.free() + self.waiting.load(Ordering::Relaxed);
                self.speed()
                    .get()
                    .min(u32::try_from(free.div_ceil(2)).unwrap_or(u32::MAX))
            });
            if *left > 0 {
                *left -= 1;
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
        Arc::new(ConnectionSlots::new(
            limit,
            0,
            NonZeroU32::new(speed).unwrap(),
        ))
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

    // libtorrent session_impl::update_connections_limit on a running
    // session: a lower cap is reached as connections end, a higher one frees
    // places at once.
    #[tokio::test]
    async fn a_changed_cap_holds_from_now_on() {
        let slots = slots(3, 1000);
        let mut held: Vec<_> = (0..3).map(|_| slots.try_open().unwrap()).collect();

        slots.set_limit(1);
        assert_eq!((slots.limit(), slots.in_use()), (1, 3));
        held.pop();
        assert!(slots.try_open().is_none(), "two held under a cap of 1");
        held.pop();
        assert!(slots.try_open().is_none(), "one held under a cap of 1");
        assert_eq!(slots.in_use(), 1);

        // Raised past what is owed: the rest is free at once.
        slots.set_limit(3);
        held.push(slots.try_open().unwrap());
        held.push(slots.try_open().unwrap());
        assert!(slots.try_open().is_none(), "three held under a cap of 3");

        // Lowered, then raised before the connections end: only what is
        // still owed is given up.
        slots.set_limit(1);
        slots.set_limit(2);
        held.pop();
        assert!(slots.try_open().is_none(), "two held under a cap of 2");
        held.pop();
        held.push(slots.try_open().unwrap());
        assert!(slots.try_open().is_none(), "two held under a cap of 2");
        assert_eq!((slots.limit(), slots.in_use()), (2, 2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_changed_speed_holds_from_the_next_second() {
        let slots = slots(1000, 1);
        let start = Instant::now();
        let (granted_tx, mut granted) = mpsc::unbounded_channel();
        connect_loop(slots.clone(), 'a', granted_tx);
        let mut open = vec![granted.recv().await.unwrap()];

        slots.set_speed(NonZeroU32::new(3).unwrap());
        tokio::task::yield_now().await;
        assert!(
            granted.try_recv().is_err(),
            "a second connect in the first second"
        );
        for _ in 0..3 {
            open.push(granted.recv().await.unwrap());
        }
        assert_eq!(Instant::now(), start + WINDOW);
        tokio::task::yield_now().await;
        assert!(
            granted.try_recv().is_err(),
            "a fourth connect in the second second"
        );
    }

    #[tokio::test(start_paused = true)]
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

    // libtorrent session_impl::try_connect_more_peers with smooth_connects:
    // a tick begins at most (free slots + 1) / 2 connects.
    #[tokio::test(start_paused = true)]
    async fn a_second_begins_connects_for_half_the_free_slots_rounded_up() {
        let slots = slots(20, 30);
        let start = Instant::now();
        let (granted_tx, mut granted) = mpsc::unbounded_channel();
        connect_loop(slots.clone(), 'a', granted_tx);

        // 20 free: 10 begin, then the loop waits for the next second.
        let mut open = Vec::new();
        for _ in 0..10 {
            open.push(granted.recv().await.unwrap());
        }
        assert_eq!(Instant::now(), start, "no time passed");
        tokio::task::yield_now().await;
        assert!(
            granted.try_recv().is_err(),
            "an 11th connect in the first second"
        );

        // 10 free then: 5 begin in the next.
        let next = granted.recv().await.unwrap();
        assert_eq!(Instant::now(), start + WINDOW);
        open.push(next);
        for _ in 0..4 {
            open.push(granted.recv().await.unwrap());
        }
        assert_eq!(Instant::now(), start + WINDOW);
        tokio::task::yield_now().await;
        assert!(
            granted.try_recv().is_err(),
            "a 6th connect in the second second"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_free_slot_begins_one_connect() {
        let slots = slots(2, 30);
        let _incoming = slots.try_open().unwrap();
        let start = Instant::now();
        let (granted_tx, mut granted) = mpsc::unbounded_channel();
        connect_loop(slots.clone(), 'a', granted_tx);
        let _open = granted.recv().await.unwrap();
        assert_eq!(Instant::now(), start, "no time passed");
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
