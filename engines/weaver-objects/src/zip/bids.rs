// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The container ranges a zip wants ahead of the normal order, in the order
// it first wanted them, each once. A range wanted again is found through a
// set of the ranges, so a zip that wants a header for each of many entries
// does not look through every bid at each want.

use std::collections::HashSet;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use weaver_graph::{ByteRange, Weight};

#[derive(Default)]
pub(super) struct Bids {
    bids: Vec<(ByteRange, Weight)>,
    // The ranges of the first `indexed` bids; the rest are added at the
    // next want.
    ranges: HashSet<ByteRange>,
    indexed: usize,
}

// Stored as the list alone, as before the set; the set is built again at
// the first want after a restore.
impl Serialize for Bids {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.bids.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bids {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            bids: Vec::deserialize(deserializer)?,
            ..Self::default()
        })
    }
}

impl Bids {
    // Bid for a range at `weight`, unless it is bid for already.
    pub(super) fn bid(&mut self, range: ByteRange, weight: Weight) {
        for (wanted, _) in &self.bids[self.indexed..] {
            self.ranges.insert(*wanted);
        }
        self.indexed = self.bids.len();
        if self.ranges.insert(range) {
            self.bids.push((range, weight));
            self.indexed = self.bids.len();
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &(ByteRange, Weight)> {
        self.bids.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The bids as the linear look before the set kept them.
    fn bid_scanning(bids: &mut Vec<(ByteRange, Weight)>, range: ByteRange, weight: Weight) {
        if !bids.iter().any(|(wanted, _)| *wanted == range) {
            bids.push((range, weight));
        }
    }

    // Wants new and repeated, some across a snapshot and restore, leave the
    // same bids in the same order as the look through every bid did, and
    // the snapshot reads as the list did.
    #[test]
    fn bids_keep_as_the_look_through_every_bid_did() {
        let mut seed = 0x1b87_3593_u64;
        let mut next = move |bound: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % bound
        };
        let mut bids = Bids::default();
        let mut scanned = Vec::new();
        for step in 0..6_000 {
            let start = next(3_000) * 16;
            let range = ByteRange::at(start, 1 + next(3) * 15);
            let weight = Weight(next(3) as u32);
            bids.bid(range, weight);
            bid_scanning(&mut scanned, range, weight);
            if step % 997 == 0 {
                let bytes = rmp_serde::to_vec(&bids).unwrap();
                assert_eq!(bytes, rmp_serde::to_vec(&scanned).unwrap());
                bids = rmp_serde::from_slice(&bytes).unwrap();
            }
            assert_eq!(bids.bids, scanned, "step {step}");
        }
        assert!(scanned.len() > 2_000, "{}", scanned.len());
    }

    // The 13,000-entry shape: a header bid for each entry, then each bid
    // again, as a settle that wants every header once more does. Prints
    // the time of both.
    #[test]
    fn a_bid_for_each_of_many_headers_is_not_a_look_through_every_bid() {
        const ENTRIES: u64 = 13_000;
        let headers: Vec<ByteRange> = (0..ENTRIES)
            .map(|entry| ByteRange::at(entry * 4_096, 46))
            .collect();
        let began = std::time::Instant::now();
        let mut bids = Bids::default();
        for header in headers.iter().chain(&headers) {
            bids.bid(*header, Weight::FIRST);
        }
        let set = began.elapsed();
        let began = std::time::Instant::now();
        let mut scanned = Vec::new();
        for header in headers.iter().chain(&headers) {
            bid_scanning(&mut scanned, *header, Weight::FIRST);
        }
        let scanning = began.elapsed();
        assert_eq!(bids.bids, scanned);
        eprintln!(
            "{ENTRIES} headers bid twice: through the set in {set:?}, \
             through every bid in {scanning:?}"
        );
    }
}
