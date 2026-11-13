// SPDX-License-Identifier: GPL-3.0-or-later

//! The datagrams the DHT sends to each address, over a sliding window.
//!
//! A libtorrent node counts every datagram an address sends it, answers to
//! its own queries among them, and ignores the address for 5 minutes once 50
//! arrive within 10 seconds (`dos_blocker::incoming`, dos_blocker.cpp:52-99,
//! called for every datagram in dht_tracker.cpp:538, v2.0.15). Weaver keeps
//! what it sends to one address under SEND_LIMIT in any WINDOW, however many
//! searches want that address at once: a send that does not fit is lined up
//! and goes when the window has room for it. libtorrent itself keeps no such
//! count of what it sends; this is Weaver's own.
//!
//! Every datagram counts, replies to the address's own queries too, keyed by
//! IP as the remote node keys its count. A query waits for room; a reply is
//! counted but never waits, as a late reply is no reply.
//!
//! Each send costs one map lookup and constant work on that address's
//! window. A window holds the times of the datagrams sent in the last WINDOW,
//! so the windows together hold no more times than the DHT sent in the last
//! WINDOW; an address leaves the map once nothing it was sent is in its
//! window and nothing waits for it.

use std::{
    collections::{HashMap, VecDeque},
    net::IpAddr,
    time::Duration,
};

use tokio::time::Instant;

use crate::dht::query_limiter::{QUERY_LIMIT, WINDOW};

// The datagrams one address is sent in any WINDOW: 49, the most a libtorrent
// node takes in 10 seconds without ignoring the sender, which it does at its
// QUERY_LIMIT-th (dos_blocker.cpp:76, v2.0.15). This leaves no headroom for
// the replies Weaver sends that node: the node counts them, and Weaver
// counts them here but never holds them back. So when queries have filled
// an address's window and Weaver then answers a query from that address,
// the node sees QUERY_LIMIT datagrams and ignores Weaver for 5 minutes.
// The operator chose this limit on 2026-11-13, over a lower one that kept
// room for those replies.
pub(crate) const SEND_LIMIT: usize = 49;
const _: () = assert!(SEND_LIMIT < QUERY_LIMIT as usize);

// The least time between two sweeps of the closed windows.
pub(crate) const SWEEP_SPACING: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Window {
    // When each datagram of the last WINDOW was sent, oldest first.
    sent: VecDeque<Instant>,
    // The sends given room that have not gone yet: each counts as sent
    // until it goes or is given up.
    pending: usize,
    // The sends lined up, waiting for room.
    waiting: usize,
}

impl Window {
    fn prune(&mut self, now: Instant) {
        while self.sent.front().is_some_and(|&at| at + WINDOW <= now) {
            self.sent.pop_front();
        }
    }

    fn counted(&self) -> usize {
        self.sent.len() + self.pending
    }

    fn is_idle(&self) -> bool {
        self.sent.is_empty() && self.pending == 0 && self.waiting == 0
    }

    // When a send with `ahead` sends lined up before it has room: once
    // enough of the datagrams sent leave the window. A send given room that
    // has not gone yet leaves it at the earliest a WINDOW from now.
    fn ready(&self, ahead: usize, now: Instant) -> Instant {
        let leave = (self.counted() + ahead + 1).saturating_sub(SEND_LIMIT);
        match leave {
            0 => now,
            n => self.sent.get(n - 1).map_or(now + WINDOW, |&at| at + WINDOW),
        }
    }
}

pub(crate) struct SendWindows {
    windows: HashMap<IpAddr, Window>,
    // Whether the sweep waits for a datagram to be sent before it looks
    // again.
    sweep_idle: bool,
}

impl SendWindows {
    pub(crate) fn new() -> Self {
        Self {
            windows: HashMap::new(),
            sweep_idle: true,
        }
    }

    fn window(&mut self, ip: IpAddr, now: Instant) -> &mut Window {
        let window = self.windows.entry(ip).or_default();
        window.prune(now);
        window
    }

    fn forget_if_idle(&mut self, ip: IpAddr) {
        if self.windows.get(&ip).is_some_and(Window::is_idle) {
            self.windows.remove(&ip);
        }
    }

    // A datagram went: whether the sweep is to be woken.
    fn stamp(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.window(ip, now).sent.push_back(now);
        std::mem::take(&mut self.sweep_idle)
    }

    /// Room for a send to `ip`, or, when its window is full or others wait
    /// for it, the send is lined up behind them and this is when it may
    /// have room (see [`Self::take_lined_up`]).
    pub(crate) fn take(&mut self, ip: IpAddr, now: Instant) -> Result<(), Instant> {
        let window = self.window(ip, now);
        if window.counted() + window.waiting < SEND_LIMIT {
            window.pending += 1;
            return Ok(());
        }
        let ready = window.ready(window.waiting, now);
        window.waiting += 1;
        Err(ready)
    }

    /// Room for a send lined up for `ip`, or when to look again.
    pub(crate) fn take_lined_up(&mut self, ip: IpAddr, now: Instant) -> Result<(), Instant> {
        let window = self.window(ip, now);
        if window.counted() < SEND_LIMIT {
            window.waiting -= 1;
            window.pending += 1;
            return Ok(());
        }
        Err(window.ready(0, now))
    }

    /// A send lined up for `ip` is given up.
    pub(crate) fn leave(&mut self, ip: IpAddr) {
        if let Some(window) = self.windows.get_mut(&ip) {
            window.waiting -= 1;
        }
        self.forget_if_idle(ip);
    }

    /// A send given room has gone at `now`: whether the sweep is to be
    /// woken.
    pub(crate) fn sent(&mut self, ip: IpAddr, now: Instant) -> bool {
        let window = self.window(ip, now);
        window.pending -= 1;
        self.stamp(ip, now)
    }

    /// A send given room is given up before it went.
    pub(crate) fn release(&mut self, ip: IpAddr) {
        if let Some(window) = self.windows.get_mut(&ip) {
            window.pending -= 1;
        }
        self.forget_if_idle(ip);
    }

    /// A reply went to `ip` at `now`; it is counted, and never waits.
    /// Whether the sweep is to be woken.
    pub(crate) fn record(&mut self, ip: IpAddr, now: Instant) -> bool {
        self.stamp(ip, now)
    }

    /// Forgets the addresses whose windows have closed, and says when to
    /// look again: when the next window that holds a datagram closes, at
    /// most once a SWEEP_SPACING, or none until a datagram is sent.
    pub(crate) fn sweep(&mut self, now: Instant) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        self.windows.retain(|_, window| {
            window.prune(now);
            if let Some(&last) = window.sent.back() {
                let closes = last + WINDOW;
                next = Some(next.map_or(closes, |n| n.min(closes)));
            }
            !window.is_idle()
        });
        if self.windows.capacity() > 64 && self.windows.len() * 4 < self.windows.capacity() {
            self.windows.shrink_to(self.windows.len() * 2);
        }
        self.sweep_idle = next.is_none();
        next.map(|at| at.max(now + SWEEP_SPACING))
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.windows.len()
    }

    // The sends lined up for `ip`.
    #[cfg(test)]
    pub(crate) fn lined_up(&self, ip: IpAddr) -> usize {
        self.windows.get(&ip).map_or(0, |w| w.waiting)
    }
}

#[cfg(test)]
mod tests {
    use super::{SEND_LIMIT, SWEEP_SPACING, SendWindows};
    use crate::dht::query_limiter::WINDOW;
    use std::{net::IpAddr, time::Duration};
    use tokio::time::Instant;

    const A: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
    const B: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));

    fn limit() -> u32 {
        u32::try_from(SEND_LIMIT).unwrap()
    }

    fn send(windows: &mut SendWindows, ip: IpAddr, now: Instant) {
        windows.take(ip, now).unwrap();
        windows.sent(ip, now);
    }

    #[tokio::test(start_paused = true)]
    async fn test_an_address_is_sent_at_most_the_limit_in_any_window() {
        let mut windows = SendWindows::new();
        let start = Instant::now();
        let step = Duration::from_millis(100);
        for i in 0..limit() {
            send(&mut windows, A, start + step * i);
        }
        let now = start + step * limit();
        // Full until the first datagram leaves the window, a WINDOW after it.
        assert_eq!(windows.take(A, now), Err(start + WINDOW));
        // Another address is not held by it.
        assert_eq!(windows.take(B, now), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_lined_up_sends_have_room_in_order_and_none_jumps_the_line() {
        let mut windows = SendWindows::new();
        let start = Instant::now();
        let step = Duration::from_millis(100);
        for i in 0..limit() {
            send(&mut windows, A, start + step * i);
        }
        let now = start + Duration::from_secs(5);
        // Each lined up behind the last has room as one more datagram leaves.
        let first = windows.take(A, now).unwrap_err();
        let second = windows.take(A, now).unwrap_err();
        assert_eq!((first, second), (start + WINDOW, start + step + WINDOW));
        // At the first one's time a newcomer still waits behind the second.
        assert_eq!(
            windows.take(A, first),
            Err(start + step * 2 + WINDOW),
            "a newcomer jumped the line"
        );
        assert_eq!(windows.take_lined_up(A, first), Ok(()));
        windows.sent(A, first);
        // The second has no room before its time.
        assert_eq!(windows.take_lined_up(A, first), Err(second));
        assert_eq!(windows.take_lined_up(A, second), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_send_given_up_gives_its_room_back() {
        let mut windows = SendWindows::new();
        let now = Instant::now();
        for _ in 0..SEND_LIMIT {
            windows.take(A, now).unwrap();
        }
        // Not sent yet, they still count; once sent they would leave a
        // WINDOW from now at the earliest.
        assert_eq!(windows.take(A, now), Err(now + WINDOW));
        windows.leave(A);
        windows.release(A);
        assert_eq!(windows.take(A, now), Ok(()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_replies_count_and_never_wait() {
        let mut windows = SendWindows::new();
        let now = Instant::now();
        for _ in 0..SEND_LIMIT + 5 {
            windows.record(A, now);
        }
        assert_eq!(windows.take(A, now), Err(now + WINDOW));
    }

    #[tokio::test(start_paused = true)]
    async fn test_the_sweep_forgets_closed_windows_and_rests_once_empty() {
        let mut windows = SendWindows::new();
        let start = Instant::now();
        // The first datagram wakes the sweep; a second does not.
        windows.take(A, start).unwrap();
        assert!(windows.sent(A, start));
        assert!(!windows.record(B, start + Duration::from_secs(3)));
        assert_eq!(windows.sweep(start), Some(start + WINDOW));
        assert_eq!(windows.len(), 2);
        // A's window has closed; B's closes later.
        assert_eq!(
            windows.sweep(start + WINDOW),
            Some(start + Duration::from_secs(3) + WINDOW)
        );
        assert_eq!(windows.len(), 1);
        // Looked at no more than once a SWEEP_SPACING.
        let late = start + Duration::from_secs(13) - SWEEP_SPACING / 2;
        assert_eq!(windows.sweep(late), Some(late + SWEEP_SPACING));
        assert_eq!(windows.sweep(start + Duration::from_secs(13)), None);
        assert_eq!(windows.len(), 0);
        // Resting, the next datagram wakes it again.
        assert!(windows.record(A, start + Duration::from_secs(20)));
    }
}
