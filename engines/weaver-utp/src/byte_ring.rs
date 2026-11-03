// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::num::NonZeroUsize;

// A fixed-capacity FIFO of bytes: the writer pushes at the back, the
// dispatcher reads the queued bytes in place and drops them from the front
// once they are acknowledged.
pub(crate) struct ByteRing {
    buf: Box<[u8]>,
    head: usize,
    len: usize,
}

impl ByteRing {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            buf: vec![0u8; capacity.get()].into_boxed_slice(),
            head: 0,
            len: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    // The queued bytes, oldest first, as at most two runs.
    pub fn as_slices(&self) -> (&[u8], &[u8]) {
        let cap = self.buf.len();
        let first_len = self.len.min(cap - self.head);
        (
            &self.buf[self.head..self.head + first_len],
            &self.buf[..self.len - first_len],
        )
    }

    // Appends as much of `data` as fits; returns how much did.
    pub fn push_slice(&mut self, data: &[u8]) -> usize {
        let cap = self.buf.len();
        let count = data.len().min(cap - self.len);
        let tail = (self.head + self.len) % cap;
        let first = count.min(cap - tail);
        self.buf[tail..tail + first].copy_from_slice(&data[..first]);
        self.buf[..count - first].copy_from_slice(&data[first..count]);
        self.len += count;
        count
    }

    // Drops up to `count` bytes from the front; returns how many it dropped.
    pub fn skip(&mut self, count: usize) -> usize {
        let count = count.min(self.len);
        self.head = (self.head + count) % self.buf.len();
        self.len -= count;
        if self.len == 0 {
            self.head = 0;
        }
        count
    }

    // A ring of `capacity` holding the same bytes; None when they would not
    // fit.
    pub fn resized(&self, capacity: NonZeroUsize) -> Option<Self> {
        if capacity.get() < self.len {
            return None;
        }
        let mut ring = Self::new(capacity);
        let (first, second) = self.as_slices();
        ring.push_slice(first);
        ring.push_slice(second);
        Some(ring)
    }
}

#[cfg(test)]
mod tests {
    use super::ByteRing;
    use std::num::NonZeroUsize;

    fn ring(cap: usize) -> ByteRing {
        ByteRing::new(NonZeroUsize::new(cap).unwrap())
    }

    fn contents(r: &ByteRing) -> Vec<u8> {
        let (a, b) = r.as_slices();
        [a, b].concat()
    }

    #[test]
    fn pushes_up_to_capacity() {
        let mut r = ring(4);
        assert_eq!(r.push_slice(b"abcdef"), 4);
        assert_eq!(contents(&r), b"abcd");
        assert_eq!(r.push_slice(b"x"), 0);
    }

    #[test]
    fn wraps_and_reads_in_two_runs() {
        let mut r = ring(5);
        r.push_slice(b"abcd");
        assert_eq!(r.skip(3), 3);
        assert_eq!(r.push_slice(b"efgh"), 4);
        let (a, b) = r.as_slices();
        assert_eq!((a, b), (&b"de"[..], &b"fgh"[..]));
        assert_eq!(r.len(), 5);
    }

    #[test]
    fn skip_is_bounded_by_len() {
        let mut r = ring(3);
        r.push_slice(b"ab");
        assert_eq!(r.skip(10), 2);
        assert!(r.is_empty());
    }

    #[test]
    fn resize_keeps_order() {
        let mut r = ring(4);
        r.push_slice(b"abcd");
        r.skip(2);
        r.push_slice(b"ef");
        let grown = r.resized(NonZeroUsize::new(8).unwrap()).unwrap();
        assert_eq!(contents(&grown), b"cdef");
        assert_eq!(grown.capacity(), 8);
        assert!(r.resized(NonZeroUsize::new(3).unwrap()).is_none());
    }
}
