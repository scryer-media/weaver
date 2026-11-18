// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The received bit of every block, in the layout of the chunk tracker's
//! bitfield, readable and writable without the torrent's lock. A piece in
//! flight has its bits written only under its piece lock, with the slot that
//! lock guards; any other piece's, only under the torrent's write lock. A
//! write stores with release and a read loads with acquire, so a reader
//! under the torrent's lock sees every block a piece lock marked before it.

use std::ops::Range;
use std::sync::atomic::{AtomicU8, Ordering};

use bitvec::{order::Msb0, vec::BitVec};

use crate::type_aliases::{BF, BS};

pub(crate) struct Received {
    bytes: Box<[AtomicU8]>,
    len: usize,
}

// The byte and the mask of bit `at`, most significant bit first, as `BF`
// lays them out.
fn place(at: usize) -> (usize, u8) {
    (at / 8, 0x80 >> (at % 8))
}

impl Received {
    pub(crate) fn from_bits(bits: &BS) -> Self {
        let bytes = (0..bits.len().div_ceil(8))
            .map(|byte| {
                let mut value = 0u8;
                for bit in 0..8 {
                    if bits.get(byte * 8 + bit).is_some_and(|b| *b) {
                        value |= 0x80 >> bit;
                    }
                }
                AtomicU8::new(value)
            })
            .collect();
        Self {
            bytes,
            len: bits.len(),
        }
    }

    /// Whether block `at` was received; false past the end.
    pub(crate) fn get(&self, at: usize) -> bool {
        if at >= self.len {
            return false;
        }
        let (byte, mask) = place(at);
        self.bytes[byte].load(Ordering::Acquire) & mask != 0
    }

    /// Block `at` was received, or no longer is. Nothing past the end.
    pub(crate) fn set(&self, at: usize, received: bool) {
        if at >= self.len {
            return;
        }
        let (byte, mask) = place(at);
        if received {
            self.bytes[byte].fetch_or(mask, Ordering::AcqRel);
        } else {
            self.bytes[byte].fetch_and(!mask, Ordering::AcqRel);
        }
    }

    // `range`, cut to the bits there are; None when it starts past them, as
    // a bitfield's `get` of such a range is.
    fn within(&self, range: Range<usize>) -> Option<Range<usize>> {
        (range.start <= range.end && range.end <= self.len).then_some(range)
    }

    // Each byte `range` touches, with the mask of its bits in `range`: one
    // load a byte, not one a bit.
    fn bytes_of(&self, range: Range<usize>) -> impl Iterator<Item = (&AtomicU8, u8)> {
        let bytes = if range.is_empty() {
            0..0
        } else {
            range.start / 8..(range.end - 1) / 8 + 1
        };
        bytes.map(move |byte| {
            let lo = range.start.saturating_sub(byte * 8).min(8);
            let hi = (range.end - byte * 8).min(8);
            let mask = u8::try_from((0xffu16 >> lo) & !(0xffu16 >> hi)).expect("eight bits");
            (&self.bytes[byte], mask)
        })
    }

    /// How many blocks of `range` were received; 0 for a range past the end.
    pub(crate) fn count(&self, range: Range<usize>) -> usize {
        self.within(range).map_or(0, |range| {
            self.bytes_of(range)
                .map(|(byte, mask)| (byte.load(Ordering::Acquire) & mask).count_ones() as usize)
                .sum()
        })
    }

    /// Whether any block of `range` was received.
    pub(crate) fn any(&self, range: Range<usize>) -> Option<bool> {
        self.within(range).map(|range| {
            self.bytes_of(range)
                .any(|(byte, mask)| byte.load(Ordering::Acquire) & mask != 0)
        })
    }

    /// Whether every block of `range` was received.
    pub(crate) fn all(&self, range: Range<usize>) -> Option<bool> {
        self.within(range).map(|range| {
            self.bytes_of(range)
                .all(|(byte, mask)| byte.load(Ordering::Acquire) & mask == mask)
        })
    }

    /// No block of `range` is received any more.
    pub(crate) fn clear(&self, range: Range<usize>) {
        if let Some(range) = self.within(range) {
            for (byte, mask) in self.bytes_of(range) {
                byte.fetch_and(!mask, Ordering::AcqRel);
            }
        }
    }

    /// A copy, as the tracker's bitfield.
    pub(crate) fn to_bits(&self) -> BF {
        let mut bits: BitVec<u8, Msb0> = BitVec::from_vec(
            self.bytes
                .iter()
                .map(|byte| byte.load(Ordering::Acquire))
                .collect(),
        );
        bits.truncate(self.len);
        bits.into_boxed_bitslice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitvec::prelude::*;

    #[test]
    fn the_bits_keep_the_bitfield_layout_and_ranges_read_as_its_slices_do() {
        let mut bits: BitVec<u8, Msb0> = bitvec![u8, Msb0; 0; 13];
        bits.set(0, true);
        bits.set(9, true);
        bits.set(12, true);
        let bits = bits.into_boxed_bitslice();
        let received = Received::from_bits(&bits);
        assert_eq!(received.to_bits(), bits);
        assert_eq!(received.count(8..13), 2);
        assert_eq!(received.count(8..14), 0);
        assert_eq!(received.any(1..9), Some(false));
        assert_eq!(received.all(12..13), Some(true));
        assert_eq!(received.all(13..14), None);
        received.set(1, true);
        received.clear(9..13);
        assert!(!received.get(13));
        received.set(13, true);
        let mut want = bits.to_bitvec();
        want.set(1, true);
        want.set(9, false);
        want.set(12, false);
        assert_eq!(received.to_bits(), want.into_boxed_bitslice());
    }

    // Every range of every bit pattern up to three bytes and a bit reads,
    // and clears, as the bitfield's slice of it does, across byte edges.
    #[test]
    fn a_range_read_a_byte_at_a_time_matches_the_bitfield_bit_by_bit() {
        for len in 0..=25usize {
            for seed in [0u32, 0x5a5a_5a5a, 0xffff_ffff, 0x0123_4567, 0x8000_0001] {
                let mut bits: BitVec<u8, Msb0> = BitVec::repeat(false, len);
                for at in 0..len {
                    bits.set(at, seed.rotate_left(u32::try_from(at).unwrap()) & 1 == 1);
                }
                let bits = bits.into_boxed_bitslice();
                let received = Received::from_bits(&bits);
                for start in 0..=len {
                    for end in start..=len {
                        let slice = &bits[start..end];
                        assert_eq!(received.count(start..end), slice.count_ones());
                        assert_eq!(received.any(start..end), Some(slice.any()));
                        assert_eq!(received.all(start..end), Some(slice.all()));
                        let cleared = Received::from_bits(&bits);
                        cleared.clear(start..end);
                        let mut want = bits.to_bitvec();
                        want[start..end].fill(false);
                        assert_eq!(cleared.to_bits(), want.into_boxed_bitslice());
                    }
                }
            }
        }
    }
}
