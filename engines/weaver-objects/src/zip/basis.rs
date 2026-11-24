// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The bytes a zip's map stands on, by range and checksum: the directory and
// its end records, and every local header and data descriptor read. A
// delivery that changes kept bytes looks again only at the ranges it meets,
// found through the blocks of the container they touch, so its cost follows
// the delivery and not the archive's entry count.
//
// Looking at those alone answers as looking at every range did. Each range
// was read from what is kept, and its checksum taken from those bytes, when
// it joined. Wanting a range only joins pieces, which keeps every byte and
// how it arrived. Absorbing a delivery changes kept bytes, or how they
// arrived, only inside the delivery, and every absorb that changes anything
// is followed by this look over the delivery's range. A range the delivery
// does not meet therefore still reads what it read when it joined; once one
// changes, the basis is emptied or the store gives up.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use weaver_graph::ByteRange;

use crate::grid::crc32;
use crate::kept::Kept;

// The container blocks the index files ranges under.
const BLOCK_SHIFT: u32 = 16;

#[derive(Default)]
pub(super) struct Basis {
    entries: Vec<(ByteRange, u32)>,
    // Each entry's position under every block its range touches, for the
    // first `indexed` entries; the rest are filed at the next look.
    blocks: BTreeMap<u64, Vec<u32>>,
    indexed: usize,
}

// Stored as the list of ranges and checksums alone, as before the index;
// the index is built again at the first look after a restore.
impl Serialize for Basis {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.entries.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Basis {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self {
            entries: Vec::deserialize(deserializer)?,
            ..Self::default()
        })
    }
}

fn blocks(range: ByteRange) -> std::ops::RangeInclusive<u64> {
    (range.start >> BLOCK_SHIFT)..=((range.end - 1) >> BLOCK_SHIFT)
}

impl Basis {
    pub(super) fn push(&mut self, entry: (ByteRange, u32)) {
        self.entries.push(entry);
    }

    pub(super) fn replace(&mut self, entries: impl IntoIterator<Item = (ByteRange, u32)>) {
        self.clear();
        self.entries.extend(entries);
    }

    pub(super) fn clear(&mut self) {
        self.entries.clear();
        self.blocks.clear();
        self.indexed = 0;
    }

    pub(super) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    // Whether any range a delivery over `range` meets reads otherwise than
    // when it joined; `None` when it meets none.
    pub(super) fn changed(&mut self, kept: &Kept, range: ByteRange) -> Option<bool> {
        let met = self.meeting(range);
        if met.is_empty() {
            return None;
        }
        Some(!met.into_iter().all(|at| {
            let (basis, crc) = self.entries[at];
            kept.read(basis).is_some_and(|bytes| crc32(bytes) == crc)
        }))
    }

    // The positions of the entries whose ranges meet `range`, ascending.
    fn meeting(&mut self, range: ByteRange) -> Vec<usize> {
        self.index();
        if range.is_empty() {
            return Vec::new();
        }
        let mut met: Vec<usize> = self
            .blocks
            .range(blocks(range))
            .flat_map(|(_, entries)| entries.iter().map(|at| *at as usize))
            .filter(|at| self.entries[*at].0.intersect(range).is_some())
            .collect();
        met.sort_unstable();
        met.dedup();
        met
    }

    // File the entries pushed since the last look.
    fn index(&mut self) {
        for at in self.indexed..self.entries.len() {
            let range = self.entries[at].0;
            if range.is_empty() {
                continue;
            }
            for block in blocks(range) {
                self.blocks.entry(block).or_default().push(at as u32);
            }
        }
        self.indexed = self.entries.len();
    }

    // The answer looking at every range gave, for a test to hold the
    // narrowed look to.
    #[cfg(test)]
    pub(super) fn changed_looking_at_every_range(
        &self,
        kept: &Kept,
        range: ByteRange,
    ) -> Option<bool> {
        if !self
            .entries
            .iter()
            .any(|(basis, _)| basis.intersect(range).is_some())
        {
            return None;
        }
        Some(
            !self
                .entries
                .iter()
                .all(|(basis, crc)| kept.read(*basis).is_some_and(|bytes| crc32(bytes) == *crc)),
        )
    }

    // The same, through the scan from the first kept piece that every read
    // made before its lookup started at the range.
    #[cfg(test)]
    pub(super) fn changed_by_the_old_scan(&self, kept: &Kept, range: ByteRange) -> Option<bool> {
        if !self
            .entries
            .iter()
            .any(|(basis, _)| basis.intersect(range).is_some())
        {
            return None;
        }
        Some(!self.entries.iter().all(|(basis, crc)| {
            kept.read_scanning(*basis)
                .is_some_and(|bytes| crc32(bytes) == *crc)
        }))
    }
}

impl Extend<(ByteRange, u32)> for Basis {
    fn extend<T: IntoIterator<Item = (ByteRange, u32)>>(&mut self, entries: T) {
        self.entries.extend(entries);
    }
}

#[cfg(test)]
mod tests {
    use weaver_graph::Provenance;

    use super::*;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self, bound: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % bound
        }
    }

    // The container's byte at `at`, as first written.
    fn byte(at: u64) -> u8 {
        (at.wrapping_mul(0x9e37_79b9) >> 7) as u8
    }

    fn bytes(range: ByteRange) -> Vec<u8> {
        (range.start..range.end).map(byte).collect()
    }

    // Deliveries in several orders, overlapping, repeated, damaged, repaired
    // and with the same bytes again, over a store that keeps headers, a
    // long directory and ranges inside and across them, as the zip store
    // keeps them: a range joins once read, a change empties the basis and
    // the ranges join again. At every absorb that changes anything the
    // narrowed look answers as the look at every range did.
    #[test]
    fn the_narrowed_look_answers_as_the_look_at_every_range() {
        let mut rng = Rng(0x2545_f491);
        let mut ranges = Vec::new();
        let mut at = 0;
        for _ in 0..2_000 {
            at += 1 + rng.next(3_000);
            let header = ByteRange::at(at, 30 + rng.next(70));
            ranges.push(header);
            // A descriptor, or a range inside or across another's.
            match rng.next(8) {
                0 => ranges.push(ByteRange::at(header.end + 1 + rng.next(500), 16)),
                1 => ranges.push(ByteRange::new(header.start + 1, header.end - 1)),
                2 => ranges.push(header),
                _ => {}
            }
            at = header.end;
        }
        let directory = ByteRange::at(at + 10, 300_000);
        ranges.push(directory);
        ranges.push(ByteRange::new(directory.end - 22, directory.end));
        let total = directory.end + 64;

        let mut kept = Kept::default();
        for range in &ranges {
            kept.want(*range);
        }
        let mut basis = Basis::default();
        let mut joined = vec![false; ranges.len()];
        let mut outcomes = [0usize; 3];
        let kinds = [Provenance::Fresh, Provenance::Damaged, Provenance::Repaired];
        let len = 70_000;
        let mut deliveries: Vec<ByteRange> = (0..total)
            .step_by(len)
            .map(|start| ByteRange::new(start, (start + len as u64).min(total)))
            .collect();
        let mut orders = Vec::new();
        orders.extend(deliveries.iter().copied());
        orders.extend(deliveries.iter().rev().copied());
        for index in (1..deliveries.len()).rev() {
            deliveries.swap(index, rng.next(index as u64 + 1) as usize);
        }
        orders.extend(deliveries.iter().copied());
        for _ in 0..1_500 {
            let start = rng.next(total);
            let range = ByteRange::new(start, (start + 1 + rng.next(90_000)).min(total));
            orders.push(range);
            if rng.next(4) == 0 {
                orders.push(range);
            }
        }
        for (step, range) in orders.into_iter().enumerate() {
            let mut delivered = bytes(range);
            let kind = kinds[rng.next(3) as usize];
            if rng.next(5) == 0 {
                let flip = rng.next(delivered.len() as u64) as usize;
                delivered[flip] ^= 0x5a;
            }
            if rng.next(40) == 0 {
                // A later want joins pieces and keeps what they held.
                let start = rng.next(total);
                kept.want(ByteRange::new(start, (start + rng.next(9_000)).min(total)));
            }
            if kept.absorb(range, &delivered, kind).is_some() {
                let changed = basis.changed(&kept, range);
                assert_eq!(
                    changed,
                    basis.changed_looking_at_every_range(&kept, range),
                    "step {step}, {range:?}"
                );
                outcomes[match changed {
                    None => 0,
                    Some(false) => 1,
                    Some(true) => 2,
                }] += 1;
                if changed == Some(true) {
                    basis.clear();
                    joined.fill(false);
                }
            }
            // What reads whole now joins, as the store reads it.
            for (index, range) in ranges.iter().enumerate() {
                if !joined[index]
                    && let Some(bytes) = kept.read(*range)
                {
                    basis.push((*range, crc32(bytes)));
                    joined[index] = true;
                }
            }
        }
        assert!(outcomes.iter().all(|count| *count > 20), "{outcomes:?}");
    }

    // A store standing on 13,000 local headers, as the archive that spun
    // did, hears a repair of every 700 KiB article once, in shuffled order.
    // The look at every range read each of them per article, each read
    // scanning from the first kept piece; the narrowed look reads the
    // headers the article meets. Same answers; the counts and times are
    // printed.
    #[test]
    fn an_article_looks_at_the_headers_it_meets_not_every_header() {
        const ENTRIES: u64 = 13_000;
        const ARTICLE: u64 = 700 * 1024;
        let mut rng = Rng(0x0dd_b1a5);
        let ranges: Vec<ByteRange> = (0..ENTRIES)
            .map(|index| ByteRange::at(index * (80 + 4_096), 80))
            .collect();
        let total = ENTRIES * (80 + 4_096);
        let mut kept = Kept::default();
        let mut basis = Basis::default();
        for range in &ranges {
            kept.want(*range);
            kept.absorb(*range, &bytes(*range), Provenance::Fresh);
            basis.push((*range, crc32(kept.read(*range).unwrap())));
        }
        let mut articles: Vec<ByteRange> = (0..total)
            .step_by(ARTICLE as usize)
            .map(|start| ByteRange::new(start, (start + ARTICLE).min(total)))
            .collect();
        for index in (1..articles.len()).rev() {
            articles.swap(index, rng.next(index as u64 + 1) as usize);
        }

        // Before: one article through the look at every range, each read
        // scanning from the first piece.
        let first = articles[0];
        kept.absorb(first, &bytes(first), Provenance::Repaired);
        let started = std::time::Instant::now();
        let before = basis.changed_by_the_old_scan(&kept, first);
        let before_one = started.elapsed();
        assert_eq!(before, Some(false));

        let mut looked = 0;
        let mut narrowed = std::time::Duration::ZERO;
        let mut every = std::time::Duration::ZERO;
        for (index, article) in articles.iter().enumerate() {
            if index > 0 {
                kept.absorb(*article, &bytes(*article), Provenance::Repaired);
            }
            looked += basis.meeting(*article).len();
            let started = std::time::Instant::now();
            let changed = basis.changed(&kept, *article);
            narrowed += started.elapsed();
            let started = std::time::Instant::now();
            let all = basis.changed_looking_at_every_range(&kept, *article);
            every += started.elapsed();
            assert_eq!(changed, all);
            assert_eq!(changed, Some(false));
        }
        let articles = articles.len();
        // Every header is met by the article or two it lies in.
        assert!(looked as u64 <= ENTRIES + articles as u64, "{looked}");
        eprintln!(
            "{ENTRIES} headers x {articles} articles: headers read {looked} narrowed, {} looking at every one; \
             narrowed {narrowed:?} in all; every range with the new lookup {every:?} in all; \
             every range with the old scan {before_one:?} for one article, about {:?} for all",
            ENTRIES as usize * articles,
            before_one * articles as u32,
        );
    }
}
