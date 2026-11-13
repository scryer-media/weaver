// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Receive buffers. A datagram is read into a pooled buffer and its
//! payload stays there, behind the header, until the stream's reader
//! copies it out: one copy from the datagram to the reader's buffer, and
//! no allocation per datagram once the pool is warm.
//!
//! The socket's I/O thread takes buffers and the streams' threads give
//! them back, so each thread keeps a few at hand and trades whole groups
//! of them with a shared depot. Both are bounded: a thread keeps at most
//! two groups, the depot a fixed number, and a buffer past that is freed.

use std::cell::RefCell;

use parking_lot::Mutex;

/// Bytes of one receive buffer: any datagram of a 1500-byte path fits.
/// A longer one is read on into the socket's spill buffer.
pub(crate) const RX_BUF_LEN: usize = 2048;

// Buffers traded with the depot at a time.
const GROUP: usize = 64;
// Groups the depot keeps: 4,096 buffers, 8 MiB.
const DEPOT_GROUPS: usize = 64;

thread_local! {
    static LOCAL: RefCell<Vec<Box<[u8]>>> = const { RefCell::new(Vec::new()) };
}

static DEPOT: Mutex<Vec<Vec<Box<[u8]>>>> = Mutex::new(Vec::new());

/// A buffer of [`RX_BUF_LEN`] bytes, from the pool when it has one.
pub(crate) fn take() -> Box<[u8]> {
    LOCAL
        .try_with(|local| {
            let mut local = local.borrow_mut();
            if local.is_empty()
                && let Some(group) = DEPOT.lock().pop()
            {
                *local = group;
            }
            local.pop()
        })
        .ok()
        .flatten()
        .unwrap_or_else(|| vec![0; RX_BUF_LEN].into_boxed_slice())
}

/// Returns `buf` to the pool, or frees it when the pool is full.
fn give(buf: Box<[u8]>) {
    let _ = LOCAL.try_with(move |local| {
        let mut local = local.borrow_mut();
        local.push(buf);
        if local.len() >= 2 * GROUP {
            let group = local.split_off(GROUP);
            let mut depot = DEPOT.lock();
            if depot.len() < DEPOT_GROUPS {
                depot.push(group);
            }
        }
    });
}

/// A datagram's payload: a range of the buffer it was received into, or
/// bytes of its own. A pooled buffer goes back to the pool when the
/// payload is dropped.
pub struct Payload {
    buf: Box<[u8]>,
    start: usize,
    end: usize,
}

impl Payload {
    /// No bytes, and no buffer.
    pub(crate) fn empty() -> Self {
        Self {
            buf: Box::default(),
            start: 0,
            end: 0,
        }
    }

    /// The bytes `start..end` of `buf`.
    pub(crate) fn in_buffer(buf: Box<[u8]>, start: usize, end: usize) -> Self {
        debug_assert!(start <= end && end <= buf.len());
        Self { buf, start, end }
    }

    /// Bytes of buffer the payload holds, read or not.
    #[cfg(test)]
    pub(crate) fn held(&self) -> usize {
        self.buf.len()
    }

    /// Leaves out the first `n` bytes.
    pub(crate) fn advance(&mut self, n: usize) {
        self.start = (self.start + n).min(self.end);
    }

    // A buffer of the pool's size, given back to it when dropped.
    fn pooled(&self) -> bool {
        self.buf.len() == RX_BUF_LEN
    }

    /// Moves as much of `from`'s front as fits in this payload's pooled
    /// buffer after its bytes: one copy, no allocation.
    pub(crate) fn take_from(&mut self, from: &mut Payload) {
        let spare = if self.pooled() {
            RX_BUF_LEN - self.end
        } else {
            0
        };
        let n = spare.min(from.len());
        let end = self.end + n;
        self.buf[self.end..end].copy_from_slice(&from[..n]);
        self.end = end;
        from.advance(n);
    }

    /// Readies the payload to wait in a queue, holding at most twice its
    /// bytes once the payload after it has filled its room
    /// ([`QUEUE_SMALL`]):
    /// - in a pooled buffer, bytes that start past its first half move to
    ///   its start, so the buffer can fill past half;
    /// - bytes of their own of up to [`QUEUE_SMALL`] move to a pooled buffer;
    /// - longer bytes of their own are cut to exactly the payload.
    ///
    /// A packet as the socket received it, with a payload past
    /// [`QUEUE_SMALL`] bytes or a usual header, stays where it is.
    pub(crate) fn settle(&mut self) {
        let len = self.len();
        if len == 0 {
            *self = Self::empty();
        } else if self.pooled() {
            if self.start >= QUEUE_SMALL {
                self.buf.copy_within(self.start..self.end, 0);
                self.start = 0;
                self.end = len;
            }
        } else if len <= QUEUE_SMALL {
            let mut buf = take();
            buf[..len].copy_from_slice(self);
            *self = Self::in_buffer(buf, 0, len);
        } else if self.start > 0 || self.end < self.buf.len() {
            *self = Self::from(self.to_vec());
        }
    }
}

/// A payload of up to this many bytes shares a pooled buffer with the
/// payloads beside it while it waits in a stream's queue; a longer one keeps
/// the buffer it came in.
pub(crate) const QUEUE_SMALL: usize = RX_BUF_LEN / 2;

impl From<Vec<u8>> for Payload {
    fn from(bytes: Vec<u8>) -> Self {
        let end = bytes.len();
        Self::in_buffer(bytes.into_boxed_slice(), 0, end)
    }
}

impl Drop for Payload {
    fn drop(&mut self) {
        if self.buf.len() == RX_BUF_LEN {
            give(std::mem::take(&mut self.buf));
        }
    }
}

impl std::ops::Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.buf[self.start..self.end]
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::empty()
    }
}

// A copy has bytes of its own; only empty slots of the reorder queue are
// cloned outside tests.
impl Clone for Payload {
    fn clone(&self) -> Self {
        if self.is_empty() {
            return Self::empty();
        }
        Self::from(self.to_vec())
    }
}

impl PartialEq for Payload {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for Payload {}

impl std::fmt::Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (**self).fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A payload's buffer goes back to the pool and is the next one taken.
    #[test]
    fn a_dropped_payload_returns_its_buffer() {
        let buf = take();
        let at = buf.as_ptr();
        drop(Payload::in_buffer(buf, 20, 100));
        let again = take();
        assert_eq!(again.as_ptr(), at);
    }

    // A payload reads as its range of the buffer.
    #[test]
    fn a_payload_is_its_range() {
        let mut buf = take();
        buf[..6].copy_from_slice(b"hdrabc");
        let mut payload = Payload::in_buffer(buf, 0, 6);
        payload.advance(3);
        assert_eq!(&*payload, b"abc");
        assert_eq!(payload, Payload::from(b"abc".to_vec()));
    }

    // A payload of bytes of its own, cut from a long datagram, keeps only
    // its bytes: a small one moves to a pooled buffer, a long one to an
    // allocation of its length. A pooled payload far into its buffer moves
    // to the start; one near the start stays put.
    #[test]
    fn a_settled_payload_holds_little_more_than_its_bytes() {
        let datagram: Vec<u8> = (0..3000u32).map(|i| i as u8).collect();

        let mut small = Payload::from(datagram.clone());
        small.advance(2990);
        small.settle();
        assert_eq!(&*small, &datagram[2990..]);
        assert_eq!(small.held(), RX_BUF_LEN);

        let mut long = Payload::from(datagram.clone());
        long.advance(1000);
        long.settle();
        assert_eq!(&*long, &datagram[1000..]);
        assert_eq!(long.held(), 2000);

        let buf = take();
        let at = buf.as_ptr();
        let mut far = Payload::in_buffer(buf, 2000, 2010);
        far.settle();
        assert_eq!(far.as_ptr(), at);

        let buf = take();
        let at = buf.as_ptr();
        let mut near = Payload::in_buffer(buf, 20, 30);
        near.settle();
        assert_eq!(near.as_ptr(), at.wrapping_add(20));

        let mut gone = Payload::from(datagram);
        gone.advance(3000);
        gone.settle();
        assert_eq!(gone.held(), 0);
    }

    // A payload takes as much of the next one as its buffer has room for.
    #[test]
    fn a_payload_takes_what_fits_of_the_next() {
        let mut first = Payload::in_buffer(take(), 2040, 2044);
        let mut buf = take();
        buf[20..30].copy_from_slice(b"0123456789");
        let mut next = Payload::in_buffer(buf, 20, 30);
        first.take_from(&mut next);
        assert_eq!(&first[4..], b"0123");
        assert_eq!(&*next, b"456789");

        let mut own = Payload::from(vec![1, 2, 3]);
        own.take_from(&mut next);
        assert_eq!(&*own, &[1, 2, 3]);
        assert_eq!(&*next, b"456789");
    }

    // A thread that only gives keeps two groups at most, and the depot
    // takes the rest, up to its bound.
    #[test]
    fn the_pool_is_bounded() {
        std::thread::spawn(|| {
            for _ in 0..(2 * GROUP + 10) {
                give(vec![0; RX_BUF_LEN].into_boxed_slice());
            }
            LOCAL.with(|local| assert!(local.borrow().len() < 2 * GROUP));
            assert!(DEPOT.lock().len() <= DEPOT_GROUPS);
        })
        .join()
        .unwrap();
    }
}
