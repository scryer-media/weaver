// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-13.

/// The receiving side of uTP stream abstraction.
///
/// Users read from UtpStreamReadHalf. When there's nothing more to read
/// the task waits until the dispatcher puts more data in there.
///
/// Messages are assembled from out of order here too.
///
/// The dispatcher puts data here via add_remove(). If the messages were in-order, or
/// became ready for in-order delivery, they'll get put into UtpStreamReadHalf.
use std::{
    collections::VecDeque,
    io::IoSliceMut,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use msgq::MsgQueue;
use parking_lot::Mutex;
use tokio::io::AsyncRead;
use tracing::{debug, trace};

#[derive(Debug, PartialEq, Eq)]
enum UserRxMessage {
    Payload(Payload),
    Eof,
    Error(String),
}

impl UserRxMessage {
    pub fn len_bytes(&self) -> usize {
        match &self {
            UserRxMessage::Payload(payload) => payload.len(),
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OoqMessage {
    Payload(Payload),
    Eof,
}

impl Default for OoqMessage {
    fn default() -> Self {
        OoqMessage::Payload(Payload::empty())
    }
}

impl OoqMessage {
    pub fn len_bytes(&self) -> usize {
        match &self {
            OoqMessage::Payload(payload) => payload.len(),
            _ => 0,
        }
    }
}

mod msgq {
    use std::collections::VecDeque;

    use super::{OoqMessage, UserRxMessage};
    use crate::rx_buf::QUEUE_SMALL;

    pub struct MsgQueue {
        queue: VecDeque<UserRxMessage>,
        len_bytes: usize,
        capacity: usize,
    }

    impl MsgQueue {
        pub fn new(capacity: usize) -> Self {
            Self {
                queue: Default::default(),
                len_bytes: 0,
                capacity,
            }
        }

        #[cfg(test)]
        pub fn len_bytes(&self) -> usize {
            self.len_bytes
        }

        pub fn window(&self) -> usize {
            self.capacity.saturating_sub(self.len_bytes)
        }

        #[cfg(test)]
        pub fn is_full(&self) -> bool {
            self.len_bytes >= self.capacity
        }

        // Bytes taken off the queue but not yet read, still counted.
        pub fn hold(&mut self, n: usize) {
            self.len_bytes += n;
        }

        // Held bytes now read.
        pub fn release(&mut self, n: usize) {
            self.len_bytes -= n;
        }

        pub fn pop_front(&mut self) -> Option<UserRxMessage> {
            let msg = self.queue.pop_front()?;
            self.len_bytes -= msg.len_bytes();
            Some(msg)
        }

        pub fn try_push_back(&mut self, msg: OoqMessage) -> Result<(), OoqMessage> {
            let len = msg.len_bytes();
            if self.capacity - self.len_bytes < len {
                return Err(msg);
            }
            self.push_back(match msg {
                OoqMessage::Payload(payload) => UserRxMessage::Payload(payload),
                OoqMessage::Eof => UserRxMessage::Eof,
            });
            Ok(())
        }

        // The window counts payload bytes only, whatever buffers hold them.
        // Small payloads are packed into the room left in the buffer of the
        // one before, so every entry but the last holds more than
        // QUEUE_SMALL bytes, in a buffer of at most twice that: the buffers
        // held stay under twice the bytes queued, plus one buffer. A
        // payload past QUEUE_SMALL behind another such stays in its own
        // buffer, uncopied.
        pub(crate) fn push_back(&mut self, msg: UserRxMessage) {
            self.len_bytes += msg.len_bytes();
            let UserRxMessage::Payload(mut payload) = msg else {
                self.queue.push_back(msg);
                return;
            };
            if let Some(UserRxMessage::Payload(last)) = self.queue.back_mut()
                && (payload.len() <= QUEUE_SMALL || last.len() <= QUEUE_SMALL)
            {
                last.take_from(&mut payload);
                if payload.is_empty() {
                    return;
                }
            }
            payload.settle();
            self.queue.push_back(UserRxMessage::Payload(payload));
        }

        // Bytes of buffer the queue holds.
        #[cfg(test)]
        pub fn held(&self) -> usize {
            self.queue
                .iter()
                .map(|msg| match msg {
                    UserRxMessage::Payload(payload) => payload.held(),
                    _ => 0,
                })
                .sum()
        }
    }
}

use crate::{
    Error, Payload,
    message::UtpMessage,
    raw::{
        Type,
        selective_ack::{SACK_SENT_MAX_BITS, SelectiveAck},
    },
    utils::update_optional_waker,
};

pub struct UtpStreamReadHalf {
    current: Option<BeingRead>,
    // Taken off the shared queue and not yet read, in order.
    ready: VecDeque<UserRxMessage>,
    // Bytes taken off the shared queue and not yet read that the window
    // still counts.
    owed: usize,
    is_eof: bool,
    shared: Arc<UserRxShared>,
}

impl UtpStreamReadHalf {
    /// Fills `bufs` in order and returns how many bytes it wrote. The slices
    /// themselves are left as they came, describing the same memory, as a
    /// socket's readv leaves them: a caller (a decrypting wrapper, say) finds
    /// the bytes read in the first `n` bytes of `bufs` taken in order.
    pub fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let mut written = 0usize;
        let mut dispatcher_dead = false;
        let space: usize = bufs.iter().map(|b| b.len()).sum();
        // The dispatcher's waker, taken with the payloads that made room.
        let mut dispatcher_waker = None;
        // The slice being filled, and how far into it.
        let mut buf_idx = 0usize;
        let mut buf_pos = 0usize;

        while let Some(current_buf) = bufs.get_mut(buf_idx) {
            if buf_pos == current_buf.len() {
                buf_idx += 1;
                buf_pos = 0;
                continue;
            }
            let current_buf = &mut current_buf[buf_pos..];
            // If there was a previous message we haven't read till the end, do it.
            if let Some(current) = self.current.as_mut() {
                let payload = &current.payload[current.offset..];
                if payload.is_empty() {
                    return Poll::Ready(Err(std::io::Error::other(
                        "bug in UtpStreamReadHalf: payload is empty",
                    )));
                }

                let len = current_buf.len().min(payload.len());
                current_buf[..len].copy_from_slice(&payload[..len]);
                buf_pos += len;

                written += len;
                current.offset += len;
                if current.offset == current.payload.len() {
                    self.current = None;
                }
                continue;
            }

            if self.is_eof {
                break;
            }

            if self.ready.is_empty() {
                // One lock takes every payload the slices have room for
                // (the last perhaps in part), and what follows them.
                let this = &mut *self;
                let mut g = this.shared.locked.lock();
                // Every byte taken before is read by now.
                g.queue.release(std::mem::take(&mut this.owed));
                let mut room = space - written;
                while room > 0 {
                    match g.queue.pop_front() {
                        Some(UserRxMessage::Payload(payload)) => {
                            // The window opens by the bytes read, as
                            // libtorrent's does (utp_stream.cpp v2.0.15:677):
                            // the part of the last payload these slices
                            // cannot take stays counted until it is read.
                            this.owed = payload.len().saturating_sub(room);
                            g.queue.hold(this.owed);
                            room = room.saturating_sub(payload.len());
                            this.ready.push_back(UserRxMessage::Payload(payload));
                        }
                        Some(msg) => {
                            this.ready.push_back(msg);
                            break;
                        }
                        None => break,
                    }
                }
                if this.ready.is_empty() {
                    if g.vsock_closed {
                        dispatcher_dead = true;
                    } else {
                        update_optional_waker(&mut g.reader_waker, cx);
                    }
                    break;
                }
                if room < space - written && dispatcher_waker.is_none() {
                    dispatcher_waker = g.dispatcher_waker.take();
                }
            }
            match self.ready.pop_front() {
                Some(UserRxMessage::Eof) => {
                    self.is_eof = true;
                    break;
                }
                Some(UserRxMessage::Payload(payload)) => {
                    self.current = Some(BeingRead { payload, offset: 0 })
                }
                Some(UserRxMessage::Error(msg)) => {
                    if let Some(waker) = dispatcher_waker {
                        waker.wake();
                    }
                    return Poll::Ready(Err(std::io::Error::other(msg)));
                }
                None => break,
            }
        }

        // Bytes still counted against the window that were read since: the
        // window opens by them now. Only reads smaller than a payload get
        // here; a read that takes all it was given leaves none.
        if self.owed > 0 && self.ready.is_empty() {
            let unread = self
                .current
                .as_ref()
                .map_or(0, |current| current.payload.len() - current.offset);
            let read = self.owed.saturating_sub(unread);
            if read > 0 {
                self.owed -= read;
                let mut g = self.shared.locked.lock();
                g.queue.release(read);
                if dispatcher_waker.is_none() {
                    dispatcher_waker = g.dispatcher_waker.take();
                }
            }
        }
        if let Some(waker) = dispatcher_waker {
            waker.wake();
        }
        if written > 0 {
            return Poll::Ready(Ok(written));
        }

        if self.is_eof {
            return Poll::Ready(Ok(0));
        }

        if dispatcher_dead {
            return Poll::Ready(Err(std::io::Error::other("dispatcher dead")));
        }

        Poll::Pending
    }

    #[cfg(test)]
    pub async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
        let mut buf = vec![0u8; 2 * 1024 * 1024];
        let mut offset = 0;
        if let Some(current) = self.current.take() {
            let rest = &current.payload[current.offset..];
            buf[..rest.len()].copy_from_slice(rest);
            offset = rest.len();
        }
        let mut ready = std::mem::take(&mut self.ready);
        let mut g = self.shared.locked.lock();
        g.queue.release(std::mem::take(&mut self.owed));
        while let Some(m) = ready.pop_front().or_else(|| g.queue.pop_front()) {
            match m {
                UserRxMessage::Payload(payload) => {
                    buf[offset..offset + payload.len()].copy_from_slice(&payload);
                    offset += payload.len();
                }
                UserRxMessage::Eof => {
                    break;
                }
                UserRxMessage::Error(e) => return Err(std::io::Error::other(e)),
            }
        }
        buf.truncate(offset);
        Ok(buf)
    }
}

impl AsyncRead for UtpStreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut iovecs = [IoSliceMut::new(buf.initialize_unfilled())];
        let len = std::task::ready!(self.poll_read_vectored(cx, &mut iovecs)?);
        buf.advance(len);
        Poll::Ready(Ok(()))
    }
}

struct BeingRead {
    payload: Payload,
    offset: usize,
}

struct UserRxSharedLocked {
    reader_dropped: bool,
    vsock_closed: bool,
    queue: MsgQueue,
    dispatcher_waker: Option<Waker>,
    reader_waker: Option<Waker>,
}

struct UserRxShared {
    locked: Mutex<UserRxSharedLocked>,
}

impl Drop for UtpStreamReadHalf {
    fn drop(&mut self) {
        let mut g = self.shared.locked.lock();
        g.reader_dropped = true;
        let waker = g.dispatcher_waker.take();
        drop(g);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl Drop for UserRx {
    fn drop(&mut self) {
        self.mark_vsock_closed();
    }
}

impl UserRxShared {
    #[cfg(test)]
    pub fn is_full_test(&self) -> bool {
        self.locked.lock().queue.is_full()
    }
}

pub struct UserRx {
    shared: Arc<UserRxShared>,
    ooq: OutOfOrderQueue,
    max_incoming_payload: NonZeroUsize,
    last_remaining_rx_window: usize,
}

impl UserRx {
    pub fn build(
        max_rx_bytes: NonZeroUsize,
        max_incoming_payload: NonZeroUsize,
    ) -> (UserRx, UtpStreamReadHalf) {
        let shared = Arc::new(UserRxShared {
            locked: Mutex::new(UserRxSharedLocked {
                dispatcher_waker: None,
                reader_waker: None,
                queue: MsgQueue::new(max_rx_bytes.get()),
                reader_dropped: false,
                vsock_closed: false,
            }),
        });
        let read_half = UtpStreamReadHalf {
            current: None,
            ready: VecDeque::new(),
            owed: 0,
            shared: shared.clone(),
            is_eof: false,
        };
        let out_of_order_queue = OutOfOrderQueue::new(reorder_capacity(max_rx_bytes));
        let write_half = UserRx {
            shared,
            ooq: out_of_order_queue,
            max_incoming_payload,
            last_remaining_rx_window: max_rx_bytes.get(),
        };
        (write_half, read_half)
    }

    pub fn is_reader_dropped(&self) -> bool {
        self.shared.locked.lock().reader_dropped
    }

    /// How many bytes does the user half have available. If the user is not reading, and the buffer is filled up,
    /// this will be 0.
    pub fn remaining_rx_window(&self) -> usize {
        if self.is_reader_dropped() {
            0
        } else {
            self.last_remaining_rx_window
                .saturating_sub(self.ooq.stored_bytes())
        }
    }

    /// Inform the read half that the socket is closed - there will be no more data.
    pub fn mark_vsock_closed(&self) {
        let mut g = self.shared.locked.lock();
        if !g.vsock_closed {
            trace!("user_rx: marking vsock closed");
            g.vsock_closed = true;
            let waker = g.reader_waker.take();
            drop(g);
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }

    /// Flush the outstanding messages to user read half.
    /// Returns the number of bytes flushed.
    pub fn flush(&mut self, cx: &mut std::task::Context<'_>) -> crate::Result<usize> {
        let filled_front_bytes: usize = self.ooq.filled_front_bytes();
        // One lock for the window, every packet handed over and the reader's
        // waker.
        let mut g = self.shared.locked.lock();
        let mut remaining_rx_window = g.queue.window();
        if remaining_rx_window.saturating_sub(filled_front_bytes) < self.max_incoming_payload.get()
        {
            update_optional_waker(&mut g.dispatcher_waker, cx);
        }

        // Flush as many items as possible from the beginning of out of order queue to the user RX
        let mut flushed_bytes = 0;
        let mut flushed_packets = 0;

        while let Some(len) = self.ooq.send_front_if_fits(remaining_rx_window, |msg| {
            if g.reader_dropped {
                debug_every_ms!(5000, "reader is dead, could not send UtpMesage to it");
                return Err(msg);
            }
            g.queue.try_push_back(msg).unwrap();
            Ok(())
        }) {
            flushed_bytes += len;
            remaining_rx_window -= len;
            flushed_packets += 1;
        }
        let waker = if flushed_packets > 0 {
            g.reader_waker.take()
        } else {
            None
        };
        drop(g);

        if flushed_packets > 0 {
            if let Some(w) = waker {
                w.wake();
            }
            trace!(
                packets = flushed_packets,
                bytes = flushed_bytes,
                "flushed from out-of-order user RX"
            );
        }

        if self.ooq.filled_front > 0 {
            trace!(
                flushed_bytes,
                flushed_packets,
                out_of_order_filled_front = self.ooq.filled_front,
                remaining_rx_window,
                "did not flush everything"
            );
        }

        self.last_remaining_rx_window = remaining_rx_window;
        Ok(flushed_bytes)
    }

    /// Enqueue an error into read half to be consumed by the user.
    pub fn enqueue_error(&self, msg: String) {
        let mut g = self.shared.locked.lock();
        g.queue.push_back(UserRxMessage::Error(msg));
        let waker = g.reader_waker.take();
        if let Some(waker) = waker {
            drop(g);
            waker.wake();
        }
    }

    /// Generate a selective ACK message if there are out of order packets.
    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        self.ooq.selective_ack()
    }

    #[cfg(test)]
    pub fn len_test(&self) -> usize {
        self.shared.locked.lock().queue.len_bytes()
    }

    /// Is assembler empty
    pub fn assembler_empty(&self) -> bool {
        self.ooq.is_empty()
    }

    /// How many packets does ooq have
    pub fn assembler_packets(&self) -> usize {
        self.ooq.stored_packets()
    }

    /// The main function for the dispatcher - gets called when new data arrives at a particular offset.
    /// It's lazy - doesn't flush right away for performance. The caller needs to flush() periodically.
    pub fn add_remove(
        &mut self,
        cx: &mut std::task::Context<'_>,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        let remaining = self
            .shared
            .locked
            .lock()
            .queue
            .window()
            .saturating_sub(self.ooq.stored_bytes());
        if msg.payload().len() > remaining {
            return Ok(AssemblerAddRemoveResult::Unavailable);
        }
        match self.ooq.add_remove(msg, offset)? {
            res @ AssemblerAddRemoveResult::Consumed {
                sequence_numbers, ..
            } if sequence_numbers > 0 && self.ooq.is_full() => {
                self.flush(cx)?;
                Ok(res)
            }
            res => Ok(res),
        }
    }

    #[cfg(test)]
    async fn add_remove_test(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        let mut msg = Some(msg);
        let msg = &mut msg;
        std::future::poll_fn(move |cx| {
            let res = self.add_remove(cx, msg.take().unwrap(), offset);
            Poll::Ready(res)
        })
        .await
    }

    #[cfg(test)]
    pub fn is_flush_waker_registered(&self) -> bool {
        self.shared.locked.lock().dispatcher_waker.is_some()
    }

    #[cfg(test)]
    fn enqueue_test(&self, msg: UserRxMessage) {
        let mut g = self.shared.locked.lock();
        g.queue.push_back(msg);
    }
}

// Packets held out of order: one per 1100 bytes of the receive buffer,
// never fewer than 16. One further out is dropped.
pub(crate) fn reorder_capacity(max_rx_bytes: NonZeroUsize) -> NonZeroUsize {
    NonZeroUsize::new(
        (max_rx_bytes.get() / 1100).clamp(16, crate::constants::SEQUENCE_HALF_SPACE as usize - 1),
    )
    .unwrap_or(NonZeroUsize::MIN)
}

pub struct OutOfOrderQueue {
    data: VecDeque<OoqMessage>,
    filled_front: usize,
    // The payload of the `filled_front` packets, kept as they come and go.
    filled_front_bytes: usize,
    len: usize,
    len_bytes: usize,
    capacity: usize,
    // One bit per slot, set while the slot holds a packet: the SACK is
    // read from these words, not from the slots. A ring over slot
    // positions counted from the queue's start: slot `i` is position
    // `front + i`, and positions wrap at the ring's size, a power of two
    // no smaller than the capacity.
    occupied: Box<[u64]>,
    front: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AssemblerAddRemoveResult {
    Consumed {
        sequence_numbers: usize,
        bytes: usize,
    },
    AlreadyPresent,
    Unavailable,
}

fn ooq_slot_is_default(slot: &OoqMessage) -> bool {
    match slot {
        OoqMessage::Payload(payload) => payload.is_empty(),
        OoqMessage::Eof => false,
    }
}

impl OutOfOrderQueue {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let ring = capacity.get().next_power_of_two().max(64);
        Self {
            data: VecDeque::from(vec![Default::default(); capacity.get()]),
            filled_front: 0,
            filled_front_bytes: 0,
            len: 0,
            len_bytes: 0,
            capacity: capacity.get(),
            occupied: vec![0; ring / 64].into_boxed_slice(),
            front: 0,
        }
    }

    // The word and bit of slot `slot`.
    fn occupancy_bit(&self, slot: usize) -> (usize, u64) {
        let position = self.front.wrapping_add(slot) & (self.occupied.len() * 64 - 1);
        (position / 64, 1 << (position % 64))
    }

    // The slots from `start` to `end` that hold a packet, in order, read
    // from the occupancy words.
    fn occupied_slots(&self, start: usize, end: usize) -> impl Iterator<Item = usize> + '_ {
        let ring = self.occupied.len() * 64;
        let mut slot = start;
        let mut word = 0u64;
        let mut word_start = start;
        std::iter::from_fn(move || {
            loop {
                if word != 0 {
                    let found = word_start + word.trailing_zeros() as usize;
                    word &= word - 1;
                    return Some(found);
                }
                if slot >= end {
                    return None;
                }
                let position = self.front.wrapping_add(slot) % ring;
                let offset = position % 64;
                let take = (64 - offset).min(end - slot);
                let low = if take == 64 {
                    u64::MAX
                } else {
                    (1 << take) - 1
                };
                word = (self.occupied[position / 64] >> offset) & low;
                word_start = slot;
                slot += take;
            }
        })
    }

    fn filled_front_bytes(&self) -> usize {
        #[cfg(test)]
        assert_eq!(
            self.filled_front_bytes,
            self.data
                .iter()
                .take(self.filled_front)
                .map(|m| m.len_bytes())
                .sum::<usize>()
        );
        self.filled_front_bytes
    }

    fn send_front_if_fits(
        &mut self,
        window: usize,
        send_fn: impl FnOnce(OoqMessage) -> Result<(), OoqMessage>,
    ) -> Option<usize> {
        if self.filled_front == 0 {
            return None;
        }
        if self.data[0].len_bytes() > window {
            return None;
        }
        let msg = self.data.pop_front().unwrap();
        let len = msg.len_bytes();
        match send_fn(msg) {
            Ok(()) => {}
            Err(msg) => {
                self.data.push_front(msg);
                return None;
            }
        }
        self.filled_front -= 1;
        self.filled_front_bytes -= len;
        self.len -= 1;
        self.len_bytes -= len;
        let (word, bit) = self.occupancy_bit(0);
        self.occupied[word] &= !bit;
        self.front = self.front.wrapping_add(1);
        self.data.push_back(Default::default());
        Some(len)
    }

    pub fn is_empty(&self) -> bool {
        self.filled_front == self.len
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    #[cfg(test)]
    fn filled_front(&self) -> usize {
        self.filled_front
    }

    fn stored_packets(&self) -> usize {
        self.len
    }

    fn stored_bytes(&self) -> usize {
        self.len_bytes
    }

    // Bytes of buffer the slots hold.
    #[cfg(test)]
    fn held(&self) -> usize {
        self.data
            .iter()
            .map(|msg| match msg {
                OoqMessage::Payload(payload) => payload.held(),
                OoqMessage::Eof => 0,
            })
            .sum()
    }

    #[cfg(test)]
    fn debug_string(&self, with_data: bool) -> impl std::fmt::Display + '_ {
        struct D<'a> {
            q: &'a OutOfOrderQueue,
            with_data: bool,
        }
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "len={}, len_bytes={}",
                    self.q.stored_packets(),
                    self.q.stored_bytes(),
                )?;

                if !self.with_data {
                    return Ok(());
                }

                write!(f, ", queue={:?}", self.q.data)?;
                Ok(())
            }
        }
        D { q: self, with_data }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.is_empty() {
            return None;
        }

        let start = self.filled_front + 1;
        if start >= self.data.len() {
            return None;
        }
        // Only the slots a SACK can carry are read, and none past the last
        // packet held: every packet held lies after the first hole.
        let end = self.data.len().min(start + SACK_SENT_MAX_BITS);
        let held = self.len - self.filled_front;
        let unacked = self
            .occupied_slots(start, end)
            .map(|slot| slot - start)
            .take(held);
        let sack = SelectiveAck::new(unacked);

        // The words agree with the slots, every one of them. These checks
        // are cfg(test): they run in weaver-utp's own tests only, not when
        // another crate's tests drive a stream.
        #[cfg(test)]
        for (index, data) in self.data.iter().enumerate() {
            let (word, bit) = self.occupancy_bit(index);
            assert_eq!(
                self.occupied[word] & bit != 0,
                !ooq_slot_is_default(data),
                "slot {index}"
            );
        }
        #[cfg(test)]
        assert_eq!(
            sack,
            SelectiveAck::new(
                self.data
                    .range(start..)
                    .enumerate()
                    .filter(|(_, data)| !ooq_slot_is_default(data))
                    .map(|(idx, _)| idx)
            ),
            "the SACK read to the queue's end"
        );

        // Empty when every packet held lies past what a SACK can carry.
        (!sack.is_empty()).then_some(sack)
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> crate::Result<AssemblerAddRemoveResult> {
        if self.is_full() {
            debug!(offset, "assembler buffer full");
            return Ok(AssemblerAddRemoveResult::Unavailable);
        }

        let effective_offset = offset + self.filled_front;

        if effective_offset >= self.data.len() {
            trace!(
                offset,
                self.filled_front, effective_offset, "message is past assembler's window"
            );
            return Ok(AssemblerAddRemoveResult::Unavailable);
        }

        let msg = match msg.header.htype {
            Type::Data if msg.payload().is_empty() => return Err(Error::ZeroPayloadStData),
            Type::Data => OoqMessage::Payload(msg.data),
            Type::Fin => OoqMessage::Eof,
            _ => return Err(Error::BugInvalidMessageExpectedStDataOrFin),
        };

        let slot = self
            .data
            .get_mut(effective_offset)
            .ok_or(Error::BugAssemblerMissingSlot(effective_offset))?;
        if !ooq_slot_is_default(slot) {
            return Ok(AssemblerAddRemoveResult::AlreadyPresent);
        }

        self.len += 1;
        self.len_bytes += msg.len_bytes();
        *slot = msg;
        let (word, bit) = self.occupancy_bit(effective_offset);
        self.occupied[word] |= bit;

        let range = self.filled_front..self.data.len();
        // Advance "filled" if a contiguous data range was found.
        let (consumed_segments, consumed_bytes) = self
            .data
            .range(range)
            .take_while(|msg| !ooq_slot_is_default(msg))
            .fold((0, 0), |mut state, msg| {
                state.0 += 1;
                state.1 += msg.len_bytes();
                state
            });
        self.filled_front += consumed_segments;
        self.filled_front_bytes += consumed_bytes;
        Ok(AssemblerAddRemoveResult::Consumed {
            sequence_numbers: consumed_segments,
            bytes: consumed_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{future::poll_fn, num::NonZeroUsize, task::Poll};

    use tokio::io::AsyncReadExt;
    use tracing::trace;

    use crate::{
        message::UtpMessage,
        stream_rx::{AssemblerAddRemoveResult, OutOfOrderQueue, UserRxMessage},
        test_util::setup_test_logging,
    };

    use super::{UserRx, UtpStreamReadHalf};

    fn msg(seq_nr: u16, payload: &[u8]) -> UtpMessage {
        UtpMessage::new_test(
            crate::raw::UtpHeader {
                htype: crate::raw::Type::Data,
                seq_nr: seq_nr.into(),
                ..Default::default()
            },
            payload,
        )
    }

    #[tokio::test]
    async fn the_reorder_window_holds_one_packet_per_1100_bytes_and_never_fewer_than_16() {
        setup_test_logging();
        // 16 KiB holds 14 packets at 1100 bytes each: the floor of 16 stands.
        let (mut rx, _read) = user_rx(16 * 1024);
        assert!(matches!(
            rx.add_remove_test(msg(16, b"a"), 15).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                ..
            }
        ));
        assert!(matches!(
            rx.add_remove_test(msg(17, b"a"), 16).await.unwrap(),
            AssemblerAddRemoveResult::Unavailable
        ));

        // 1 MiB holds 953.
        let (mut rx, _read) = user_rx(1024 * 1024);
        assert!(matches!(
            rx.add_remove_test(msg(953, b"a"), 952).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                ..
            }
        ));
        assert!(matches!(
            rx.add_remove_test(msg(954, b"a"), 953).await.unwrap(),
            AssemblerAddRemoveResult::Unavailable
        ));
    }

    fn user_rx(capacity_bytes: usize) -> (UserRx, UtpStreamReadHalf) {
        UserRx::build(
            NonZeroUsize::new(capacity_bytes).unwrap(),
            NonZeroUsize::new(1500).unwrap(),
        )
    }

    /// A vectored read writes the first `n` bytes of the slices in order and
    /// leaves the slices themselves describing the memory they came with, as
    /// a socket's readv does: a wrapper that post-processes `bufs[..n]` (the
    /// stream cipher) must find the bytes there.
    #[tokio::test]
    async fn a_vectored_read_leaves_the_callers_slices_as_they_came() {
        setup_test_logging();
        let (user_rx, mut read) = user_rx(64);
        user_rx.enqueue_test(UserRxMessage::Payload(b"abcde".to_vec().into()));
        user_rx.enqueue_test(UserRxMessage::Payload(b"fghij".to_vec().into()));

        let mut a = [0u8; 3];
        let mut b = [0u8; 4];
        let mut c = [0u8; 8];
        let n = {
            let mut slices = [
                std::io::IoSliceMut::new(&mut a),
                std::io::IoSliceMut::new(&mut []),
                std::io::IoSliceMut::new(&mut b),
                std::io::IoSliceMut::new(&mut c),
            ];
            let n = poll_fn(|cx| std::pin::Pin::new(&mut read).poll_read_vectored(cx, &mut slices))
                .await
                .unwrap();
            // Every slice still has its own length and start.
            assert_eq!(
                slices.iter().map(|s| s.len()).collect::<Vec<_>>(),
                [3, 0, 4, 8]
            );
            let mut seen = Vec::new();
            let mut left = n;
            for s in &slices {
                let k = s.len().min(left);
                seen.extend_from_slice(&s[..k]);
                left -= k;
            }
            assert_eq!(seen, b"abcdefghij");
            n
        };
        assert_eq!(n, 10);
        assert_eq!(&a, b"abc");
        assert_eq!(&b, b"defg");
        assert_eq!(&c[..3], b"hij");
    }

    /// A read takes off the shared queue only the payloads the slices have
    /// room for, the last perhaps in part, and the window opens by the bytes
    /// read alone. What follows comes on the next reads in order, the end
    /// last. The four small payloads here share one buffer.
    #[tokio::test]
    async fn a_read_takes_only_the_payloads_it_has_room_for() {
        setup_test_logging();
        let (user_rx, mut read) = user_rx(64);
        for p in [&b"abcd"[..], b"efgh", b"ijkl", b"mnop"] {
            user_rx.enqueue_test(UserRxMessage::Payload(p.to_vec().into()));
        }
        user_rx.enqueue_test(UserRxMessage::Eof);
        async fn next(read: &mut UtpStreamReadHalf, len: usize) -> Vec<u8> {
            let mut buf = vec![0u8; len];
            let n = poll_fn(|cx| {
                std::pin::Pin::new(&mut *read)
                    .poll_read_vectored(cx, &mut [std::io::IoSliceMut::new(&mut buf)])
            })
            .await
            .unwrap();
            buf.truncate(n);
            buf
        }
        assert_eq!(next(&mut read, 6).await, b"abcdef");
        assert_eq!(user_rx.len_test(), 10);
        assert_eq!(next(&mut read, 3).await, b"ghi");
        assert_eq!(user_rx.len_test(), 7);
        assert_eq!(next(&mut read, 64).await, b"jklmnop");
        assert_eq!(user_rx.len_test(), 0);
        assert_eq!(next(&mut read, 64).await, b"");
    }

    #[test]
    fn test_asm_add_one_in_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b"a"), 0).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 1);
    }

    #[test]
    fn test_asm_add_one_out_of_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(100, b"a"), 1).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 0);
    }

    #[tokio::test]
    async fn test_asm_channel_full_asm_empty() {
        setup_test_logging();
        let (mut user_rx, mut read) = user_rx(1);
        let msg = msg(0, b"b");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(b"a".to_vec().into()));

        assert!(user_rx.shared.is_full_test());

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Unavailable
        );
        assert_eq!(user_rx.ooq.stored_packets(), 0);
        assert_eq!(user_rx.ooq.stored_bytes(), 0);
        assert!(user_rx.selective_ack().is_none());
        let mut bytes = [0; 1];
        read.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"a");
        assert_eq!(
            user_rx.add_remove_test(msg, 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 1);
            Poll::Ready(())
        })
        .await;
        read.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"b");
        assert_eq!(user_rx.ooq.stored_bytes() + user_rx.len_test(), 0);
    }

    #[tokio::test]
    async fn test_asm_combined_capacity_full_with_out_of_order_data() {
        let (mut user_rx, mut read) = user_rx(3);
        let out_of_order = msg(1, b"c");
        let missing = msg(0, b"b");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(b"aa".to_vec().into()));

        assert_eq!(
            user_rx.add_remove_test(out_of_order, 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );

        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.stored_bytes(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 0);
        assert_eq!(user_rx.ooq.stored_bytes() + user_rx.len_test(), 3);
        assert!(user_rx.selective_ack().is_some());

        assert_eq!(
            user_rx.add_remove_test(missing.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Unavailable
        );
        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 0);
        assert_eq!(user_rx.ooq.stored_bytes() + user_rx.len_test(), 3);
        let mut bytes = [0; 2];
        read.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"aa");

        assert_eq!(
            user_rx.add_remove_test(missing, 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 2,
                bytes: 2
            }
        );
        assert_eq!(user_rx.ooq.stored_packets(), 2);
        assert_eq!(user_rx.ooq.stored_bytes(), 2);
        assert_eq!(user_rx.ooq.filled_front(), 2);
        assert_eq!(user_rx.ooq.stored_bytes() + user_rx.len_test(), 2);
        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 2);
            Poll::Ready(())
        })
        .await;
        read.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"bc");
        assert_eq!(user_rx.ooq.stored_bytes() + user_rx.len_test(), 0);
    }

    #[tokio::test]
    async fn test_asm_out_of_order() {
        setup_test_logging();

        let (mut user_rx, mut read) = user_rx(100);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 1);

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 2).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 3,
                bytes: 14
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 3);
        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 14);
            Poll::Ready(())
        })
        .await;
        assert_eq!(user_rx.ooq.stored_packets(), 0);

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[tokio::test]
    async fn test_asm_inorder() {
        setup_test_logging();
        let (mut user_rx, mut read) = user_rx(100);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 1);

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 4
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 3);

        poll_fn(|cx| {
            assert_eq!(user_rx.flush(cx).unwrap(), 14);
            Poll::Ready(())
        })
        .await;

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[test]
    fn test_asm_write_out_of_bounds() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_2 = msg(2, b"test");
        let msg_3 = msg(3, b"test");

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);

        // A message that is out of bounds of the assembler should be dropped.
        assert_eq!(
            asm.add_remove(msg_3.clone(), 3).unwrap(),
            AssemblerAddRemoveResult::Unavailable
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);
    }

    // The SACK read from the occupancy words equals the one read from the
    // slots (asserted inside selective_ack) while the queue's start goes
    // round the words' ring many times, with holes of every width.
    #[test]
    fn the_sack_agrees_with_the_slots_as_the_queue_turns() {
        setup_test_logging();
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(40).unwrap());
        let mut drained = 0usize;
        for round in 0..300usize {
            // Packets past a hole at the front, every `step`-th slot.
            let step = 1 + round % 5;
            for offset in (1..39).step_by(step) {
                asm.add_remove(msg(0, b"x"), offset).unwrap();
            }
            let sack = asm.selective_ack().unwrap();
            assert!(sack.iter().any(|bit| bit), "round {round}");
            // Each hole filled in turn, everything held drains.
            while !asm.is_empty() {
                asm.add_remove(msg(0, b"x"), 0).unwrap();
                asm.selective_ack();
            }
            while asm.send_front_if_fits(usize::MAX, |_| Ok(())).is_some() {
                drained += 1;
            }
            assert!(asm.selective_ack().is_none());
            assert_eq!(asm.stored_packets(), 0);
        }
        assert!(drained > 64 * 10);
    }

    #[test]
    fn test_asm_duplicate_msg_ignored() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(10).unwrap());
        let msg_2 = msg(2, b"test");
        assert_eq!(
            asm.add_remove(msg_2, 2).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );

        let msg_2 = msg(2, b"test");
        assert_eq!(
            asm.add_remove(msg_2, 2).unwrap(),
            AssemblerAddRemoveResult::AlreadyPresent
        );
    }

    // A packet as the socket's receive path makes it: `len` payload bytes at
    // `at` in a pooled receive buffer, after the header.
    fn pooled(seq_nr: u16, at: usize, len: usize) -> UtpMessage {
        let mut buf = crate::rx_buf::take();
        buf[at..at + len].fill(seq_nr as u8);
        UtpMessage {
            header: crate::raw::UtpHeader {
                htype: crate::raw::Type::Data,
                seq_nr: seq_nr.into(),
                ..Default::default()
            },
            data: crate::Payload::in_buffer(buf, at, at + len),
            received: tokio::time::Instant::now(),
        }
    }

    // Delivers in-order packets of the given payload lengths, each at
    // `at` in its buffer, to a stream whose reader is not reading, until
    // the window is full. Returns the bytes accepted and the bytes of
    // buffer the stream's queue then holds.
    async fn fill_unread(window: usize, at: usize, lens: &[usize]) -> (usize, usize) {
        let (mut rx, _read) = user_rx(window);
        let mut accepted = 0;
        for (seq, &len) in lens.iter().cycle().enumerate() {
            match rx
                .add_remove_test(pooled(seq as u16, at, len), 0)
                .await
                .unwrap()
            {
                AssemblerAddRemoveResult::Consumed {
                    sequence_numbers: 1,
                    bytes,
                } => accepted += bytes,
                AssemblerAddRemoveResult::Unavailable => break,
                other => panic!("packet {seq}: {other:?}"),
            }
            poll_fn(|cx| {
                rx.flush(cx).unwrap();
                Poll::Ready(())
            })
            .await;
        }
        let g = rx.shared.locked.lock();
        assert_eq!(g.queue.len_bytes(), accepted);
        (accepted, g.queue.held())
    }

    // One-byte packets to a stream that is not read (stopped at the disk's
    // high mark, or by a rate limit): the window still takes one byte per
    // byte of it, as on the wire, and the buffers held stay under twice
    // the bytes queued plus one buffer, not one buffer per packet.
    #[tokio::test]
    async fn one_byte_packets_share_receive_buffers() {
        setup_test_logging();
        const WINDOW: usize = 16 * 1024;
        let (accepted, held) = fill_unread(WINDOW, 20, &[1]).await;
        assert_eq!(accepted, WINDOW);
        assert!(
            held < 2 * accepted + crate::rx_buf::RX_BUF_LEN,
            "{held} bytes of buffers for {accepted} bytes queued"
        );
    }

    // Small and large payloads in turn, and small payloads behind a header
    // that fills most of their buffer: still under twice plus one buffer.
    #[tokio::test]
    async fn mixed_and_long_headed_packets_share_receive_buffers() {
        setup_test_logging();
        const WINDOW: usize = 64 * 1024;
        for (at, lens) in [
            (20, &[1, 1025][..]),
            (20, &[1025, 1][..]),
            (20, &[9, 1400, 9, 9, 1400][..]),
            (2000, &[1, 47][..]),
            (1100, &[300, 948][..]),
        ] {
            let (accepted, held) = fill_unread(WINDOW, at, lens).await;
            assert!(accepted > WINDOW - 1500, "{at} {lens:?}: {accepted}");
            assert!(
                held < 2 * accepted + crate::rx_buf::RX_BUF_LEN,
                "{at} {lens:?}: {held} bytes of buffers for {accepted} bytes queued"
            );
        }
    }

    // Full-size packets behind one another are queued in the buffers they
    // were received into: no copy until the reader's.
    #[tokio::test]
    async fn full_size_packets_stay_in_their_buffers() {
        setup_test_logging();
        let (mut rx, _read) = user_rx(64 * 1024);
        let mut at = Vec::new();
        for seq in 0..8u16 {
            let msg = pooled(seq, 20, 1400);
            at.push(msg.data.as_ptr());
            rx.add_remove_test(msg, 0).await.unwrap();
            poll_fn(|cx| {
                rx.flush(cx).unwrap();
                Poll::Ready(())
            })
            .await;
        }
        let mut g = rx.shared.locked.lock();
        for (seq, at) in at.into_iter().enumerate() {
            match g.queue.pop_front() {
                Some(UserRxMessage::Payload(payload)) => {
                    assert_eq!(payload.as_ptr(), at, "packet {seq}");
                    assert_eq!(payload.len(), 1400);
                    assert!(payload.iter().all(|&b| b == seq as u8));
                }
                other => panic!("packet {seq}: {other:?}"),
            }
        }
    }

    // Packed payloads read back as the bytes sent, in order.
    #[tokio::test]
    async fn packed_payloads_read_back_in_order() {
        setup_test_logging();
        let (mut rx, mut read) = user_rx(64 * 1024);
        let lens = [1, 1025, 7, 1400, 3, 2, 1000, 1100, 1];
        let mut expected = Vec::new();
        for (seq, &len) in lens.iter().enumerate() {
            expected.extend(std::iter::repeat_n(seq as u8, len));
            rx.add_remove_test(pooled(seq as u16, 20, len), 0)
                .await
                .unwrap();
            poll_fn(|cx| {
                rx.flush(cx).unwrap();
                Poll::Ready(())
            })
            .await;
        }
        let mut got = vec![0; expected.len()];
        read.read_exact(&mut got).await.unwrap();
        assert_eq!(got, expected);
    }

    // Packed or not, the window opens by the bytes read, never by more:
    // two five-byte packets share a buffer, and reads of three bytes open
    // it three bytes at a time.
    #[tokio::test]
    async fn the_window_opens_by_the_bytes_read() {
        setup_test_logging();
        let (mut rx, mut read) = user_rx(64);
        for seq in 0..2u16 {
            rx.add_remove_test(pooled(seq, 20, 5), 0).await.unwrap();
            poll_fn(|cx| {
                rx.flush(cx).unwrap();
                Poll::Ready(())
            })
            .await;
        }
        let window = || rx.shared.locked.lock().queue.window();
        assert_eq!(window(), 54);
        let mut buf = [0u8; 3];
        for (step, open) in [57, 60, 63].into_iter().enumerate() {
            read.read_exact(&mut buf).await.unwrap();
            assert_eq!(window(), open, "after read {step}");
        }
        let mut last = [0u8; 1];
        read.read_exact(&mut last).await.unwrap();
        assert_eq!(window(), 64);
    }

    // The out-of-order queue holds one packet per slot, and has one slot
    // per 1100 bytes of the window, at least 16: one-byte packets there
    // hold at most 2 KiB per 1100 bytes of window, plus 16 buffers.
    #[tokio::test]
    async fn the_reorder_queue_holds_a_bounded_number_of_buffers() {
        setup_test_logging();
        for window in [16 * 1024, 1024 * 1024] {
            let (mut rx, _read) = user_rx(window);
            // Packet 0 never comes: everything waits out of order.
            for offset in 1.. {
                match rx
                    .add_remove_test(pooled(offset as u16, 20, 1), offset)
                    .await
                    .unwrap()
                {
                    AssemblerAddRemoveResult::Consumed { .. } => {}
                    AssemblerAddRemoveResult::Unavailable => break,
                    other => panic!("packet {offset}: {other:?}"),
                }
            }
            let held = rx.ooq.held();
            let slots = super::reorder_capacity(NonZeroUsize::new(window).unwrap()).get();
            assert_eq!(held, (slots - 1) * crate::rx_buf::RX_BUF_LEN);
            assert!(
                held <= window * 2048 / 1100 + 16 * crate::rx_buf::RX_BUF_LEN,
                "{window}: {held}"
            );
        }
    }
}
