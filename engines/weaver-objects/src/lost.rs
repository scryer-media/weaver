// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ranges of a volume that will never arrive. A loss may be told more
// than once: a restore tells every loss again, and a quiet job tells its data
// objects every loss each time it settles. Each byte is held once, so what a
// loss costs is counted once however often, and in whatever runs, it is told.

use serde::{Deserialize, Serialize};
use weaver_graph::ByteRange;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "Vec<ByteRange>", into = "Vec<ByteRange>")]
pub(crate) struct Lost(Vec<ByteRange>);

impl Lost {
    // Hold `range`, and give back what of it was not held before: all of it
    // the first time, nothing when it is told again.
    pub(crate) fn hold(&mut self, range: ByteRange) -> Vec<ByteRange> {
        let mut fresh: Vec<ByteRange> = (!range.is_empty()).then_some(range).into_iter().collect();
        for held in &self.0 {
            fresh = fresh.iter().flat_map(|part| minus(*part, *held)).collect();
        }
        self.0.extend_from_slice(&fresh);
        fresh
    }

    // The range is lost no more: its bytes are coming.
    pub(crate) fn erase(&mut self, range: ByteRange) {
        self.0 = self.0.iter().flat_map(|held| minus(*held, range)).collect();
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &ByteRange> {
        self.0.iter()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    // Some byte of `range` is lost.
    pub(crate) fn meets(&self, range: ByteRange) -> bool {
        self.0.iter().any(|held| held.intersect(range).is_some())
    }
}

// Facts written before each byte was held once may hold a range twice.
impl From<Vec<ByteRange>> for Lost {
    fn from(ranges: Vec<ByteRange>) -> Self {
        let mut lost = Self::default();
        for range in ranges {
            lost.hold(range);
        }
        lost
    }
}

impl From<Lost> for Vec<ByteRange> {
    fn from(lost: Lost) -> Self {
        lost.0
    }
}

// What of `range` lies outside `cut`.
pub(crate) fn minus(range: ByteRange, cut: ByteRange) -> impl Iterator<Item = ByteRange> {
    [
        (range.start, range.end.min(cut.start)),
        (range.start.max(cut.end), range.end),
    ]
    .into_iter()
    .filter(|(start, end)| start < end)
    .map(|(start, end)| ByteRange::new(start, end))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Two losses told apart, then told again as the one run they make: the
    // run adds nothing, and a loss reaching past them adds only its tail.
    #[test]
    fn a_loss_told_again_is_held_once() {
        let mut lost = Lost::default();
        assert_eq!(
            lost.hold(ByteRange::new(0, 10)),
            vec![ByteRange::new(0, 10)]
        );
        assert_eq!(
            lost.hold(ByteRange::new(10, 20)),
            vec![ByteRange::new(10, 20)]
        );
        assert!(lost.hold(ByteRange::new(0, 20)).is_empty());
        assert!(lost.hold(ByteRange::new(5, 15)).is_empty());
        assert_eq!(
            lost.hold(ByteRange::new(15, 30)),
            vec![ByteRange::new(20, 30)]
        );
        let held: u64 = lost.iter().map(ByteRange::len).sum();
        assert_eq!(held, 30);
        lost.erase(ByteRange::new(5, 25));
        assert_eq!(
            Vec::from(lost.clone()),
            vec![ByteRange::new(0, 5), ByteRange::new(25, 30)]
        );
        let restored = Lost::from(vec![ByteRange::new(0, 5), ByteRange::new(0, 5)]);
        assert_eq!(Vec::from(restored), vec![ByteRange::new(0, 5)]);
    }
}
