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

impl Departures {
    pub(crate) fn push(&self, at: Option<Instant>) {
        let waker = {
            let mut recorded = self.recorded.lock();
            let first = recorded.left.is_empty();
            recorded.left.push_back(at);
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
}
