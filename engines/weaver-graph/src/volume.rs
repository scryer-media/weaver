// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume is one unit of input as a parent sees it: one file of the NZB for
// the root, one `.partNN.rar` for a RAR object, one pack stream for a 7z
// object. It is addressed by index and byte offset, and that is all the
// interfaces know about it.

use std::fmt;
use std::ops::Range;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VolumeIndex(pub u32);

impl fmt::Display for VolumeIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "volume {}", self.0)
    }
}

// A half-open byte range `[start, end)`. Empty ranges are allowed and compare
// equal by their bounds, so a caller that needs "nothing" says so explicitly.
// Encoded as its two bounds; an inverted pair does not decode.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "[u64; 2]", into = "[u64; 2]")]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    // Panics on an inverted range: that is a bug in the caller, never input.
    pub fn new(start: u64, end: u64) -> Self {
        assert!(start <= end, "inverted byte range {start}..{end}");
        Self { start, end }
    }

    pub fn at(offset: u64, len: u64) -> Self {
        Self::new(offset, offset + len)
    }

    pub fn len(&self) -> u64 {
        self.end - self.start
    }

    pub fn is_empty(&self) -> bool {
        self.start == self.end
    }

    pub fn contains(&self, offset: u64) -> bool {
        offset >= self.start && offset < self.end
    }

    pub fn covers(&self, other: ByteRange) -> bool {
        other.start >= self.start && other.end <= self.end
    }

    // The overlap of two ranges, or `None` when they share no byte.
    pub fn intersect(&self, other: ByteRange) -> Option<ByteRange> {
        let start = self.start.max(other.start);
        let end = self.end.min(other.end);
        (start < end).then(|| Self::new(start, end))
    }

    // Whether two ranges touch or overlap, which is what makes them one run.
    pub fn abuts(&self, other: ByteRange) -> bool {
        self.start <= other.end && other.start <= self.end
    }

    pub fn shift(&self, by: i64) -> ByteRange {
        let start = self
            .start
            .checked_add_signed(by)
            .expect("range shifted below zero");
        let end = self
            .end
            .checked_add_signed(by)
            .expect("range shifted below zero");
        Self::new(start, end)
    }

    pub fn as_range(&self) -> Range<u64> {
        self.start..self.end
    }
}

impl TryFrom<[u64; 2]> for ByteRange {
    type Error = &'static str;

    fn try_from([start, end]: [u64; 2]) -> Result<Self, Self::Error> {
        if start > end {
            return Err("inverted byte range");
        }
        Ok(Self { start, end })
    }
}

impl From<ByteRange> for [u64; 2] {
    fn from(range: ByteRange) -> Self {
        [range.start, range.end]
    }
}

impl From<Range<u64>> for ByteRange {
    fn from(range: Range<u64>) -> Self {
        Self::new(range.start, range.end)
    }
}

impl fmt::Display for ByteRange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}..{}", self.start, self.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intersect_is_the_overlap_or_nothing() {
        let left = ByteRange::new(0, 10);
        assert_eq!(
            left.intersect(ByteRange::new(5, 20)),
            Some(ByteRange::new(5, 10))
        );
        assert_eq!(left.intersect(ByteRange::new(10, 20)), None);
        assert_eq!(left.intersect(ByteRange::new(20, 30)), None);
    }

    #[test]
    fn touching_ranges_abut_and_separated_ones_do_not() {
        assert!(ByteRange::new(0, 10).abuts(ByteRange::new(10, 20)));
        assert!(ByteRange::new(0, 10).abuts(ByteRange::new(5, 7)));
        assert!(!ByteRange::new(0, 10).abuts(ByteRange::new(11, 20)));
    }
}
