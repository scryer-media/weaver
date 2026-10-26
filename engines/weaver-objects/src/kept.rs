// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The bytes a store object reads itself: the signature header, the end
// header and whatever the header parse asks for, the bytes after the
// container, and the file a password is proved against. Each wanted range
// keeps its bytes and how they arrived; everything else passes through.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use weaver_graph::{ByteRange, Coverage, Provenance};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Piece {
    range: ByteRange,
    // The range's bytes from its start, as far as the last byte that
    // arrived: a wanted range holds only what reached it, however long it
    // is declared to be.
    bytes: Vec<u8>,
    coverage: Coverage,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct Kept {
    // Disjoint, keyed by start.
    pieces: BTreeMap<u64, Piece>,
    // Bumped whenever a range is wanted, so a delivery absorbed before its
    // range was wanted is absorbed again.
    generation: u64,
}

impl Piece {
    // The part of the range its bytes reach.
    fn held(&self) -> ByteRange {
        ByteRange::at(self.range.start, self.bytes.len() as u64)
    }
}

// A piece's bytes at `at..at + len`, grown to reach them.
fn reach(bytes: &mut Vec<u8>, at: usize, len: usize) -> &mut [u8] {
    if bytes.len() < at + len {
        bytes.resize(at + len, 0);
    }
    &mut bytes[at..at + len]
}

impl Kept {
    // Whether restored pieces are ones it could have kept: each under its
    // own start, disjoint, no longer than its range, covered only inside
    // the bytes it holds.
    pub(crate) fn sound(&self) -> bool {
        let mut end = 0;
        self.pieces.iter().all(|(start, piece)| {
            let held = piece.held();
            let fits = *start == piece.range.start
                && piece.range.start >= end
                && piece.range.covers(held)
                && piece.coverage.runs().all(|run| held.covers(run.range));
            end = piece.range.end;
            fits
        })
    }

    // How many bytes are kept.
    pub(crate) fn len(&self) -> u64 {
        self.pieces.values().map(|piece| piece.range.len()).sum()
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) fn want(&mut self, range: ByteRange) {
        if range.is_empty() {
            return;
        }
        let touching: Vec<u64> = self
            .pieces
            .values()
            .filter(|piece| piece.range.abuts(range))
            .map(|piece| piece.range.start)
            .collect();
        if let [start] = touching.as_slice()
            && self.pieces[start].range.covers(range)
        {
            return;
        }
        let mut merged = range;
        for start in &touching {
            let piece = &self.pieces[start];
            merged = ByteRange::new(
                merged.start.min(piece.range.start),
                merged.end.max(piece.range.end),
            );
        }
        let mut grown = Piece {
            range: merged,
            bytes: Vec::new(),
            coverage: Coverage::new(),
        };
        for start in touching {
            if let Some(piece) = self.pieces.remove(&start) {
                let at = (piece.range.start - merged.start) as usize;
                reach(&mut grown.bytes, at, piece.bytes.len()).copy_from_slice(&piece.bytes);
                for run in piece.coverage.runs() {
                    grown.coverage.write(run.range, run.provenance);
                }
            }
        }
        self.pieces.insert(merged.start, grown);
        self.generation += 1;
    }

    // The wanted ranges inside `range`, clipped to it, in order.
    pub(crate) fn ranges_in(&self, range: ByteRange) -> Vec<ByteRange> {
        self.overlapping(range)
            .filter_map(|piece| piece.range.intersect(range))
            .collect()
    }

    pub(crate) fn forget(&mut self, range: ByteRange) {
        let gone: Vec<u64> = self
            .overlapping(range)
            .filter(|piece| range.covers(piece.range))
            .map(|piece| piece.range.start)
            .collect();
        for start in gone {
            self.pieces.remove(&start);
        }
    }

    fn overlapping(&self, range: ByteRange) -> impl Iterator<Item = &Piece> {
        self.pieces
            .range(..range.end.max(range.start + 1))
            .map(|(_, piece)| piece)
            .filter(move |piece| piece.range.intersect(range).is_some())
    }

    // Keep what of a delivery lands in a wanted range. Whether anything kept
    // changed, in its bytes or in how far they can be trusted.
    pub(crate) fn absorb(
        &mut self,
        range: ByteRange,
        bytes: &[u8],
        provenance: Provenance,
    ) -> bool {
        let mut changed = false;
        for piece in self.pieces.values_mut() {
            let Some(overlap) = piece.range.intersect(range) else {
                continue;
            };
            let from = &bytes
                [(overlap.start - range.start) as usize..(overlap.end - range.start) as usize];
            let at = (overlap.start - piece.range.start) as usize;
            let into = reach(&mut piece.bytes, at, from.len());
            let same = piece.coverage.covered(overlap)
                && piece
                    .coverage
                    .overlapping(overlap)
                    .all(|run| run.provenance == provenance)
                && *into == *from;
            if !same {
                into.copy_from_slice(from);
                piece.coverage.write(overlap, provenance);
                changed = true;
            }
        }
        changed
    }

    // The bytes of a range, once every one of them is in.
    pub(crate) fn read(&self, range: ByteRange) -> Option<&[u8]> {
        let piece = self
            .overlapping(range)
            .find(|piece| piece.range.covers(range))?;
        if !piece.coverage.covered(range) {
            return None;
        }
        let at = (range.start - piece.range.start) as usize;
        Some(&piece.bytes[at..at + range.len() as usize])
    }

    pub(crate) fn verified(&self, range: ByteRange) -> bool {
        self.overlapping(range)
            .find(|piece| piece.range.covers(range))
            .is_some_and(|piece| piece.coverage.verified(range))
    }

    // Let a range go, handing back what was in hand of it, run by run.
    pub(crate) fn take(&mut self, range: ByteRange) -> Vec<(ByteRange, Vec<u8>, Provenance)> {
        let mut taken = Vec::new();
        for piece in self.overlapping(range) {
            for run in piece.coverage.overlapping(range) {
                let Some(part) = run.range.intersect(range) else {
                    continue;
                };
                let at = (part.start - piece.range.start) as usize;
                let bytes = piece.bytes[at..at + part.len() as usize].to_vec();
                taken.push((part, bytes, run.provenance));
            }
        }
        self.forget(range);
        taken
    }

    // The bytes in hand from `offset` on, as far as they run unbroken.
    pub(crate) fn from(&self, offset: u64) -> &[u8] {
        let Some(piece) = self
            .pieces
            .range(..=offset)
            .next_back()
            .map(|(_, piece)| piece)
            .filter(|piece| piece.range.contains(offset))
        else {
            return &[];
        };
        let Some(run) = piece.coverage.runs().find(|run| run.range.contains(offset)) else {
            return &[];
        };
        let at = (offset - piece.range.start) as usize;
        let end = (run.range.end - piece.range.start) as usize;
        &piece.bytes[at..end]
    }

    // Every run in hand, in order, with its bytes and how they arrived.
    pub(crate) fn runs(&self) -> Vec<(ByteRange, &[u8], Provenance)> {
        self.pieces
            .values()
            .flat_map(|piece| {
                piece.coverage.runs().map(move |run| {
                    let at = (run.range.start - piece.range.start) as usize;
                    let bytes = &piece.bytes[at..at + run.range.len() as usize];
                    (run.range, bytes, run.provenance)
                })
            })
            .collect()
    }

    // Where the next byte in hand at or after `offset` is, if any.
    pub(crate) fn next_in_hand(&self, offset: u64) -> Option<u64> {
        self.pieces
            .values()
            .flat_map(|piece| piece.coverage.runs())
            .filter(|run| run.range.end > offset)
            .map(|run| run.range.start.max(offset))
            .min()
    }

    // The bytes in hand that run unbroken to the end of a range, within it.
    pub(crate) fn suffix(&self, range: ByteRange) -> &[u8] {
        let mut gaps = Vec::new();
        self.missing(range, &mut gaps);
        let start = gaps.last().map_or(range.start, |gap| gap.end);
        self.read(ByteRange::new(start, range.end)).unwrap_or(&[])
    }

    // The parts of a range not in hand yet.
    pub(crate) fn missing(&self, range: ByteRange, out: &mut Vec<ByteRange>) {
        let mut cursor = range.start;
        while cursor < range.end {
            let run = self.from(cursor).len() as u64;
            if run > 0 {
                cursor += run;
                continue;
            }
            let next = self
                .next_in_hand(cursor)
                .map_or(range.end, |next| next.min(range.end));
            out.push(ByteRange::new(cursor, next));
            cursor = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_wanted_bytes_are_kept_and_a_later_want_keeps_what_was_kept() {
        let mut kept = Kept::default();
        kept.want(ByteRange::new(10, 20));
        assert!(kept.absorb(ByteRange::new(0, 30), &[1; 30], Provenance::Fresh));
        assert!(!kept.absorb(ByteRange::new(0, 30), &[1; 30], Provenance::Fresh));
        assert_eq!(kept.read(ByteRange::new(10, 20)), Some(&[1u8; 10][..]));
        kept.want(ByteRange::new(15, 40));
        assert_eq!(kept.read(ByteRange::new(10, 20)), Some(&[1u8; 10][..]));
        let mut missing = Vec::new();
        kept.missing(ByteRange::new(10, 40), &mut missing);
        assert_eq!(missing, vec![ByteRange::new(20, 40)]);
        assert!(kept.absorb(ByteRange::new(12, 13), &[9], Provenance::Repaired));
        assert!(kept.verified(ByteRange::new(10, 20)));
    }

    #[test]
    fn a_range_wanted_far_past_any_byte_holds_only_what_arrived() {
        let mut kept = Kept::default();
        let declared = ByteRange::new(32, 1 << 62);
        kept.want(declared);
        assert_eq!(kept.from(32), &[] as &[u8]);
        assert!(kept.absorb(ByteRange::new(0, 64), &[7; 64], Provenance::Fresh));
        assert_eq!(kept.read(ByteRange::new(32, 64)), Some(&[7u8; 32][..]));
        assert_eq!(kept.read(declared), None);
        kept.want(ByteRange::new(0, 40));
        assert_eq!(kept.from(32), &[7u8; 32][..]);
        assert!(kept.sound());
        let mut missing = Vec::new();
        kept.missing(declared, &mut missing);
        assert_eq!(missing, vec![ByteRange::new(64, 1 << 62)]);
    }
}
