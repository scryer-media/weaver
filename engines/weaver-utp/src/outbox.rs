// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The datagrams a socket's streams hand to the one task that does the
// socket's I/O. libtorrent makes every send and receive of its uTP sockets
// on one network thread (v2.0.15 utp_stream.cpp:1810 sends from send_pkt,
// which runs on that thread; session_impl.cpp:2616-2677 receives there).
// Sends from many threads at once on one kernel socket contend for it in
// the kernel, and each costs several times what a send from one thread
// does. A stream copies each datagram into the batch being filled; the I/O
// task takes the whole batch at once and sends it in order without the
// lock, while the streams fill the other. Both batches are allocated once,
// on first use, and reused.

use std::{
    collections::VecDeque,
    io::IoSlice,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::Waker,
};

use parking_lot::Mutex;
use tokio::time::Instant;

use crate::traits::Fragmenting;

/// Bytes one batch holds. A socket has two: the one filled and the one sent.
pub(crate) const OUTBOX_BYTES: usize = 256 * 1024;
/// Datagrams one batch holds.
pub(crate) const OUTBOX_PACKETS: usize = 1024;
/// The most datagrams for several addresses that leave in one call.
pub(crate) const SEND_MANY_MOST: usize = 32;

#[derive(Debug)]
pub(crate) struct Queued {
    start: u32,
    len: u32,
    pub(crate) addr: SocketAddr,
    // The receive id of the stream that queued it: its key at the socket
    // with `addr`, where a failed send is reported.
    pub(crate) recv_id: u16,
    // Set when the host refused the datagram as too large; it then goes
    // again without don't-fragment.
    pub(crate) fragmenting: Option<Fragmenting>,
    // Where the I/O task tells the stream when the datagram left, for a
    // data packet the stream times from its send.
    pub(crate) departures: Option<Arc<Departures>>,
}

/// When each data packet a stream queued left the socket, in the order
/// queued: the instant of its send, or None when it failed for good.
/// libtorrent times a packet from its send (v2.0.15 utp_stream.cpp:1792,
/// 2223), so the stream takes its send time from here, not from the moment
/// it queued the packet. Holds no more than the packets the stream queued
/// since it last took them. The first departure recorded after a take wakes
/// the stream once, so its timers can run from the send.
#[derive(Debug, Default)]
pub(crate) struct Departures {
    recorded: Mutex<Recorded>,
    any: AtomicBool,
}

#[derive(Debug, Default)]
struct Recorded {
    left: VecDeque<Option<Instant>>,
    // The stream, while it waits for a departure.
    waker: Option<Waker>,
}

impl crate::traits::RecordsDeparture for Departures {
    fn record(&self, at: Option<Instant>) {
        self.push(at);
    }
}

impl Departures {
    pub(crate) fn push(&self, at: Option<Instant>) {
        self.push_run(at, 1);
    }

    /// Records `count` departures at once, all at `at`.
    pub(crate) fn push_run(&self, at: Option<Instant>, count: usize) {
        let waker = {
            let mut recorded = self.recorded.lock();
            let first = recorded.left.is_empty();
            recorded.left.extend(std::iter::repeat_n(at, count));
            self.any.store(true, Ordering::Release);
            if first { recorded.waker.take() } else { None }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Moves every departure recorded into `into`, in order.
    pub(crate) fn take(&self, into: &mut VecDeque<Option<Instant>>) {
        if !self.any.load(Ordering::Acquire) {
            return;
        }
        let mut recorded = self.recorded.lock();
        self.any.store(false, Ordering::Relaxed);
        into.append(&mut recorded.left);
    }

    /// The stream waits for a departure: the next one recorded wakes
    /// `waker`, or it is woken now if one was recorded already.
    pub(crate) fn wake_on_next(&self, waker: &Waker) {
        let mut recorded = self.recorded.lock();
        if !recorded.left.is_empty() {
            drop(recorded);
            waker.wake_by_ref();
            return;
        }
        match &mut recorded.waker {
            Some(registered) if registered.will_wake(waker) => {}
            slot => *slot = Some(waker.clone()),
        }
    }
}

/// Datagrams laid end to end in one buffer, sent from the front.
#[derive(Default)]
pub(crate) struct Batch {
    bytes: Vec<u8>,
    packets: Vec<Queued>,
    next: usize,
}

impl Batch {
    fn fits(&self, len: usize) -> bool {
        self.packets.len() < OUTBOX_PACKETS && self.bytes.len() + len <= OUTBOX_BYTES
    }

    fn push(
        &mut self,
        bufs: &[IoSlice<'_>],
        len: usize,
        addr: SocketAddr,
        recv_id: u16,
        departures: Option<&Arc<Departures>>,
    ) {
        if self.bytes.capacity() == 0 {
            self.bytes.reserve_exact(OUTBOX_BYTES);
            self.packets.reserve_exact(OUTBOX_PACKETS);
        }
        let start = self.bytes.len();
        for buf in bufs {
            self.bytes.extend_from_slice(buf);
        }
        // `fits` holds both below u32::MAX.
        self.packets.push(Queued {
            start: start as u32,
            len: len as u32,
            addr,
            recv_id,
            fragmenting: None,
            departures: departures.cloned(),
        });
    }

    /// Every datagram taken was sent or given up.
    pub(crate) fn is_drained(&self) -> bool {
        self.next == self.packets.len()
    }

    /// The next datagram to send, which stays next until `advance`.
    pub(crate) fn front(&mut self) -> Option<(&mut [u8], &mut Queued)> {
        let queued = self.packets.get_mut(self.next)?;
        let range = queued.start as usize..(queued.start + queued.len) as usize;
        Some((&mut self.bytes[range], queued))
    }

    pub(crate) fn advance(&mut self) {
        self.next += 1;
    }

    /// The datagrams from the front that one call can send, when they are
    /// more than one: all to one address, none marked for a send of its
    /// own, each as long as the first but the last, which may be shorter,
    /// and at most `most` of them. Their bytes, which lie end to end, the
    /// first's length, their count and the address.
    pub(crate) fn front_run(
        &mut self,
        most: usize,
    ) -> Option<(&mut [u8], usize, usize, SocketAddr)> {
        let first = self.packets.get(self.next)?;
        if first.fragmenting.is_some() || first.len == 0 {
            return None;
        }
        let (addr, segment) = (first.addr, first.len);
        let start = first.start as usize;
        let mut count = 1;
        let mut end = start + segment as usize;
        for queued in self.packets[self.next + 1..]
            .iter()
            .take(most.saturating_sub(1))
        {
            if queued.addr != addr
                || queued.fragmenting.is_some()
                || queued.len > segment
                || queued.len == 0
            {
                break;
            }
            count += 1;
            end += queued.len as usize;
            if queued.len < segment {
                break;
            }
        }
        (count > 1).then(|| (&mut self.bytes[start..end], segment as usize, count, addr))
    }

    /// The datagrams from the front that one call for several addresses
    /// can send, when they are more than one: none marked for a send of
    /// its own, none empty, at most `parts.len()` of them. With `runs`,
    /// they stop before two that lie together for one address at one
    /// length, which begin a run for [`Self::front_run`]. Each one's length
    /// and address is written to `parts`; their bytes, which lie end to
    /// end, and their count are returned.
    pub(crate) fn front_many(
        &mut self,
        parts: &mut [(usize, SocketAddr)],
        runs: bool,
    ) -> Option<(&mut [u8], usize)> {
        let waiting = &self.packets[self.next..];
        let start = waiting.first()?.start as usize;
        let mut count = 0;
        let mut end = start;
        for (i, queued) in waiting.iter().take(parts.len()).enumerate() {
            if queued.fragmenting.is_some() || queued.len == 0 {
                break;
            }
            if runs
                && waiting.get(i + 1).is_some_and(|next| {
                    next.addr == queued.addr && next.len == queued.len && next.fragmenting.is_none()
                })
            {
                break;
            }
            parts[count] = (queued.len as usize, queued.addr);
            count += 1;
            end += queued.len as usize;
        }
        (count > 1).then(|| (&mut self.bytes[start..end], count))
    }

    /// The first `count` datagrams left at `at`: each one's stream is
    /// told, the datagrams of one stream that lie together at once.
    pub(crate) fn advance_run(&mut self, count: usize, at: Instant) {
        let run = &self.packets[self.next..self.next + count];
        let mut i = 0;
        while i < run.len() {
            let Some(departures) = &run[i].departures else {
                i += 1;
                continue;
            };
            let mut same = 1;
            while run
                .get(i + same)
                .and_then(|queued| queued.departures.as_ref())
                .is_some_and(|other| Arc::ptr_eq(other, departures))
            {
                same += 1;
            }
            departures.push_run(Some(at), same);
            i += same;
        }
        self.next += count;
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.packets.clear();
        self.next = 0;
    }

    #[cfg(test)]
    pub(crate) fn capacity(&self) -> (usize, usize) {
        (self.bytes.capacity(), self.packets.capacity())
    }
}

struct Filling {
    batch: Batch,
    // Datagrams ever queued.
    queued: u64,
    // The transport refused the I/O task's send, and the batch it holds is
    // not sent yet. Streams are refused until it is, as each was refused
    // by the full socket when it sent itself.
    stalled: bool,
    // Woken by the first datagram queued after the I/O task found none.
    io_task: Option<Waker>,
}

pub(crate) struct Outbox {
    filling: Mutex<Filling>,
    // Datagrams ever sent or given up by the I/O task, in queue order.
    sent: AtomicU64,
}

impl Outbox {
    pub(crate) fn new() -> Self {
        Self {
            filling: Mutex::new(Filling {
                batch: Batch::default(),
                queued: 0,
                stalled: false,
                io_task: None,
            }),
            sent: AtomicU64::new(0),
        }
    }

    /// Queues one datagram, unless the transport refused the I/O task and
    /// its batch is not sent yet, or the batch filled is full. The ticket
    /// returned is the count of datagrams queued through this one. Its
    /// departure goes to `departures`, when given.
    pub(crate) fn try_queue(
        &self,
        bufs: &[IoSlice<'_>],
        addr: SocketAddr,
        recv_id: u16,
        departures: Option<&Arc<Departures>>,
    ) -> Option<u64> {
        let len: usize = bufs.iter().map(|buf| buf.len()).sum();
        let (ticket, io_task) = {
            let mut filling = self.filling.lock();
            if filling.stalled || !filling.batch.fits(len) {
                return None;
            }
            let first = filling.batch.packets.is_empty();
            filling.batch.push(bufs, len, addr, recv_id, departures);
            filling.queued += 1;
            let io_task = if first { filling.io_task.take() } else { None };
            (filling.queued, io_task)
        };
        if let Some(waker) = io_task {
            waker.wake();
        }
        Some(ticket)
    }

    /// Hands the I/O task the filled batch for its drained one, and ends a
    /// stall. False when nothing is queued; the first datagram queued then
    /// wakes `waker`.
    pub(crate) fn take(&self, drained: &mut Batch, waker: &Waker) -> bool {
        drained.clear();
        let mut filling = self.filling.lock();
        filling.stalled = false;
        if filling.batch.packets.is_empty() {
            match &mut filling.io_task {
                Some(registered) if registered.will_wake(waker) => {}
                slot => *slot = Some(waker.clone()),
            }
            return false;
        }
        std::mem::swap(&mut filling.batch, drained);
        true
    }

    /// The transport refused the I/O task's send.
    pub(crate) fn stall(&self) {
        self.filling.lock().stalled = true;
    }

    pub(crate) fn set_sent(&self, sent: u64) {
        self.sent.store(sent, Ordering::Release);
    }

    /// Datagrams ever sent or given up: every ticket up to it has left.
    pub(crate) fn sent(&self) -> u64 {
        self.sent.load(Ordering::Acquire)
    }

    /// Datagrams ever queued.
    #[cfg(test)]
    pub(crate) fn queued(&self) -> u64 {
        self.filling.lock().queued
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        task::Waker,
    };

    use super::*;

    const ADDR: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 1);

    fn queue(outbox: &Outbox, byte: u8, len: usize) -> Option<u64> {
        let payload = vec![byte; len];
        outbox.try_queue(&[IoSlice::new(&payload)], ADDR, 7, None)
    }

    #[test]
    fn batches_keep_queue_order_and_their_buffers() {
        let outbox = Outbox::new();
        let mut drained = Batch::default();
        for (i, byte) in [1u8, 2, 3].into_iter().enumerate() {
            assert_eq!(queue(&outbox, byte, 30), Some(i as u64 + 1));
        }
        assert!(outbox.take(&mut drained, Waker::noop()));
        let mut order = Vec::new();
        while let Some((buf, queued)) = drained.front() {
            assert_eq!(queued.recv_id, 7);
            order.push(buf[0]);
            drained.advance();
        }
        assert_eq!(order, [1, 2, 3]);
        let filled_capacity = drained.capacity();
        assert_eq!(filled_capacity, (OUTBOX_BYTES, OUTBOX_PACKETS));

        // The drained batch goes back to be filled, buffers and all; the
        // datagrams queued next come out after the first three.
        assert_eq!(queue(&outbox, 4, 30), Some(4));
        assert!(outbox.take(&mut drained, Waker::noop()));
        assert_eq!(drained.capacity(), (OUTBOX_BYTES, OUTBOX_PACKETS));
        assert_eq!(drained.front().unwrap().0[0], 4);
        drained.advance();
        assert!(drained.is_drained());
        assert!(!outbox.take(&mut drained, Waker::noop()));
        assert_eq!(drained.capacity(), filled_capacity);
    }

    #[test]
    fn a_full_batch_and_a_stalled_epoch_refuse() {
        let outbox = Outbox::new();
        for _ in 0..OUTBOX_PACKETS {
            assert!(queue(&outbox, 0, 20).is_some());
        }
        assert_eq!(queue(&outbox, 0, 20), None);
        let mut drained = Batch::default();
        assert!(outbox.take(&mut drained, Waker::noop()));
        assert!(queue(&outbox, 0, 20).is_some());

        // Bytes bound the batch as well as datagrams.
        let outbox = Outbox::new();
        assert!(queue(&outbox, 0, OUTBOX_BYTES - 10).is_some());
        assert_eq!(queue(&outbox, 0, 11), None);
        assert!(queue(&outbox, 0, 10).is_some());

        // A refusal refuses the streams until the batch held is sent.
        let outbox = Outbox::new();
        assert_eq!(queue(&outbox, 0, 20), Some(1));
        assert!(outbox.take(&mut drained, Waker::noop()));
        outbox.stall();
        assert_eq!(queue(&outbox, 0, 20), None);
        drained.advance();
        assert!(!outbox.take(&mut drained, Waker::noop()));
        assert_eq!(queue(&outbox, 0, 20), Some(2));
    }

    // A run is the datagrams from the front that go to one address and are
    // as long as the first, with at most one shorter one to end it.
    #[test]
    fn a_run_is_datagrams_of_one_length_for_one_address() {
        const OTHER: SocketAddr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 2);
        let outbox = Outbox::new();
        let departures = Arc::new(Departures::default());
        let to = |byte: u8, len: usize, addr, departures: Option<&Arc<Departures>>| {
            let payload = vec![byte; len];
            outbox
                .try_queue(&[IoSlice::new(&payload)], addr, 7, departures)
                .unwrap();
        };
        // Three of 30 bytes and a shorter one, then a longer one, then two
        // for another address, then one alone.
        to(1, 30, ADDR, Some(&departures));
        to(2, 30, ADDR, Some(&departures));
        to(3, 30, ADDR, None);
        to(4, 10, ADDR, Some(&departures));
        to(5, 40, ADDR, None);
        to(6, 40, OTHER, None);
        to(7, 40, OTHER, None);
        to(8, 40, ADDR, None);
        let mut batch = Batch::default();
        assert!(outbox.take(&mut batch, Waker::noop()));

        // No more than asked for.
        let (bytes, segment, count, addr) = batch.front_run(2).unwrap();
        assert_eq!((bytes.len(), segment, count, addr), (60, 30, 2, ADDR));

        let (bytes, segment, count, addr) = batch.front_run(64).unwrap();
        assert_eq!((segment, count, addr), (30, 4, ADDR));
        assert_eq!(bytes.len(), 100);
        assert_eq!([bytes[0], bytes[30], bytes[60], bytes[90]], [1, 2, 3, 4]);
        let at = Instant::now();
        batch.advance_run(4, at);
        let mut left = VecDeque::new();
        departures.take(&mut left);
        assert_eq!(left, [Some(at); 3]);

        // The longer datagram is followed by another address: no run.
        assert!(batch.front_run(64).is_none());
        assert_eq!(batch.front().unwrap().0[0], 5);
        batch.advance();
        let (bytes, segment, count, addr) = batch.front_run(64).unwrap();
        assert_eq!((bytes.len(), segment, count, addr), (80, 40, 2, OTHER));
        batch.advance_run(2, at);
        assert!(batch.front_run(64).is_none());
        batch.advance();
        assert!(batch.is_drained());
        assert!(batch.front_run(64).is_none());
    }

    // Datagrams for several addresses that lie together leave in one call:
    // no more than asked for, and none of a run for one address, which
    // leaves in a call of its own.
    #[test]
    fn datagrams_for_several_addresses_leave_together_up_to_a_run() {
        let other = |port| SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), port);
        let outbox = Outbox::new();
        let departures = Arc::new(Departures::default());
        let to = |byte: u8, len: usize, addr, departures: Option<&Arc<Departures>>| {
            let payload = vec![byte; len];
            outbox
                .try_queue(&[IoSlice::new(&payload)], addr, 7, departures)
                .unwrap();
        };
        // Three for three addresses, one of them longer, then two that
        // begin a run, then one alone.
        to(1, 20, other(2), Some(&departures));
        to(2, 26, other(3), None);
        to(3, 20, other(4), Some(&departures));
        to(4, 30, ADDR, None);
        to(5, 30, ADDR, None);
        to(6, 20, other(2), None);
        let mut batch = Batch::default();
        assert!(outbox.take(&mut batch, Waker::noop()));
        let mut parts = [(0, ADDR); 8];

        // No more than there is room for.
        let (bytes, count) = batch.front_many(&mut parts[..2], true).unwrap();
        assert_eq!((bytes.len(), count), (46, 2));

        let (bytes, count) = batch.front_many(&mut parts, true).unwrap();
        assert_eq!(count, 3);
        assert_eq!(bytes.len(), 66);
        assert_eq!([bytes[0], bytes[20], bytes[46]], [1, 2, 3]);
        assert_eq!(parts[..3], [(20, other(2)), (26, other(3)), (20, other(4))]);
        // A transport with no call for a run takes the run's datagrams too.
        assert_eq!(batch.front_many(&mut parts, false).unwrap().1, 6);

        let at = Instant::now();
        batch.advance_run(3, at);
        let mut left = VecDeque::new();
        departures.take(&mut left);
        assert_eq!(left, [Some(at); 2]);

        // The run is next: it is not taken here.
        assert!(batch.front_many(&mut parts, true).is_none());
        assert_eq!(batch.front_run(64).unwrap().2, 2);
        batch.advance_run(2, at);
        // One alone is not several.
        assert!(batch.front_many(&mut parts, true).is_none());
        batch.advance();
        assert!(batch.front_many(&mut parts, true).is_none());
    }

    // A datagram marked for a send of its own never rides in a run.
    #[test]
    fn a_datagram_sent_allowing_fragments_ends_a_run() {
        let outbox = Outbox::new();
        for byte in 0..3 {
            assert!(queue(&outbox, byte, 30).is_some());
        }
        let mut batch = Batch::default();
        assert!(outbox.take(&mut batch, Waker::noop()));
        batch.packets[2].fragmenting = Some(crate::Fragmenting::Allowed);
        let (_, _, count, _) = batch.front_run(64).unwrap();
        assert_eq!(count, 2);
        batch.advance_run(2, Instant::now());
        assert!(batch.front_run(64).is_none());
        assert!(batch.front().is_some());
    }
}
