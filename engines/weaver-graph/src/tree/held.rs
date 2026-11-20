// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The lost ranges of one node's volumes its object holds until it can place
// them, each with the node the loss was told at. Sorted by volume and
// offset, a range held later before one held earlier at the same offset;
// runs of one volume and one origin that touch or overlap are one run. A
// hold costs a lookup and the runs it merges, never a walk of the others.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

// Where a run sits: its volume, its start, and the order it was held in,
// the later first among runs that start together.
type At = (VolumeIndex, u64, Reverse<u64>);

#[derive(Debug, Default)]
pub(super) struct HeldLost {
    // Each run's end and origin, by where it sits.
    runs: BTreeMap<At, (u64, NodeId)>,
    // How many runs were held: the order of the next.
    held: u64,
    // Runs kept after others were taken out may touch across the gap
    // those left: the next hold merges them all, as a merge over the whole
    // list does. Otherwise no run would merge with the one after it.
    unmerged: bool,
    // The runs holds have looked at, for the cost test.
    #[cfg(test)]
    walked: u64,
}

impl HeldLost {
    pub(super) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    // The runs in their order.
    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (VolumeIndex, ByteRange, NodeId)> + '_ {
        self.runs
            .iter()
            .map(|(&(volume, start, _), &(end, origin))| {
                (volume, ByteRange::new(start, end), origin)
            })
    }

    // Hold a lost range, merged with the runs of its volume and origin it
    // touches or overlaps.
    pub(super) fn hold(&mut self, volume: VolumeIndex, range: ByteRange, origin: NodeId) {
        if range.is_empty() {
            return;
        }
        let at = (volume, range.start, Reverse(self.held));
        self.held += 1;
        if self.unmerged {
            self.runs.insert(at, (range.end, origin));
            self.merge_all();
            return;
        }
        // The run before it takes it in, or it sits as a run of its own.
        let joins = |(v, _, _): &At, (end, o): &(u64, NodeId)| {
            *v == volume && *o == origin && range.start <= *end
        };
        #[cfg(test)]
        {
            self.walked += 1;
        }
        let (at, mut end) = match self.runs.range(..at).next_back() {
            Some((before, run)) if joins(before, run) => (*before, run.0.max(range.end)),
            _ => (at, range.end),
        };
        // It takes in each run after it that it now reaches; the first it
        // does not reach was not reached before either.
        loop {
            let after = (Bound::Excluded(at), Bound::Unbounded);
            let Some((&next, &(next_end, next_origin))) = self.runs.range(after).next() else {
                break;
            };
            if next.0 != volume || next_origin != origin || next.1 > end {
                break;
            }
            #[cfg(test)]
            {
                self.walked += 1;
            }
            end = end.max(next_end);
            self.runs.remove(&next);
        }
        self.runs.insert(at, (end, origin));
    }

    // Merge the runs that touch, over the whole list.
    fn merge_all(&mut self) {
        #[cfg(test)]
        {
            self.walked += self.runs.len() as u64;
        }
        let mut merged: Vec<(At, (u64, NodeId))> = Vec::with_capacity(self.runs.len());
        for (at, (end, origin)) in std::mem::take(&mut self.runs) {
            match merged.last_mut() {
                Some(((v, _, _), (e, o))) if *v == at.0 && *o == origin && at.1 <= *e => {
                    *e = (*e).max(end);
                }
                _ => merged.push((at, (end, origin))),
            }
        }
        self.runs = merged.into_iter().collect();
        self.unmerged = false;
    }

    // The runs `placed` says now fall on a member, in their order, taken
    // out; the rest stay held in theirs.
    pub(super) fn take_placed(
        &mut self,
        mut placed: impl FnMut(VolumeIndex, ByteRange) -> bool,
    ) -> Vec<(VolumeIndex, ByteRange, NodeId)> {
        let mut taken = Vec::new();
        self.runs.retain(|&(volume, start, _), &mut (end, origin)| {
            let range = ByteRange::new(start, end);
            let place = placed(volume, range);
            if place {
                taken.push((volume, range, origin));
            }
            !place
        });
        self.unmerged |= !taken.is_empty();
        taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Run = (VolumeIndex, ByteRange, NodeId);

    // The list as it was kept before: each hold inserted where it sorts,
    // then the whole list merged again.
    #[derive(Default)]
    struct Reference(Vec<Run>);

    impl Reference {
        fn hold(&mut self, volume: VolumeIndex, range: ByteRange, origin: NodeId) -> u64 {
            if range.is_empty() {
                return 0;
            }
            let list = &mut self.0;
            let at = list.partition_point(|(v, r, _)| (*v, r.start) < (volume, range.start));
            let moved = (list.len() - at) as u64;
            list.insert(at, (volume, range, origin));
            let mut merged: Vec<Run> = Vec::with_capacity(list.len());
            let len = list.len() as u64;
            for (volume, range, origin) in list.drain(..) {
                match merged.last_mut() {
                    Some((v, r, o)) if *v == volume && *o == origin && range.start <= r.end => {
                        *r = ByteRange::new(r.start, r.end.max(range.end));
                    }
                    _ => merged.push((volume, range, origin)),
                }
            }
            *list = merged;
            moved + len
        }

        fn take_placed(&mut self, mut placed: impl FnMut(VolumeIndex, ByteRange) -> bool) {
            self.0.retain(|(volume, range, _)| !placed(*volume, *range));
        }
    }

    // A small seeded generator, so every run of the test sees one sequence.
    struct Seeded(u64);

    impl Seeded {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    // Losses scattered, adjacent, overlapping and out of order, over three
    // volumes and two origins, with runs placed and taken out between
    // them: after every step the list is the one the old merge kept.
    #[test]
    fn holds_keep_the_list_the_whole_merge_kept() {
        for seed in 1..=64_u64 {
            let mut random = Seeded(0x9e37_79b9_7f4a_7c15 ^ seed);
            let mut held = HeldLost::default();
            let mut reference = Reference::default();
            for step in 0..400 {
                let volume = VolumeIndex(random.below(3) as u32);
                let origin = NodeId(random.below(2) as u32);
                let start = match random.below(4) {
                    // Scattered.
                    0 => random.below(1 << 20),
                    // Adjacent to or overlapping one already held.
                    _ => reference
                        .0
                        .get(random.below(reference.0.len().max(1) as u64) as usize)
                        .map_or(0, |(_, r, _)| match random.below(3) {
                            0 => r.end,
                            1 => r.start + (r.end - r.start) / 2,
                            _ => r.start,
                        }),
                };
                let len = random.below(5_000);
                let range = ByteRange::new(start, start + len);
                held.hold(volume, range, origin);
                reference.hold(volume, range, origin);
                if random.below(16) == 0 {
                    let pick = random.next();
                    let placed = |volume: VolumeIndex, range: ByteRange| {
                        (u64::from(volume.0) ^ range.start ^ pick).is_multiple_of(3)
                    };
                    held.take_placed(placed);
                    reference.take_placed(placed);
                }
                assert_eq!(
                    held.iter().collect::<Vec<_>>(),
                    reference.0,
                    "seed {seed} step {step}"
                );
            }
        }
    }

    // Runs of different origins at one offset keep the order they had: the
    // later first.
    #[test]
    fn runs_at_one_offset_keep_their_order() {
        let mut held = HeldLost::default();
        let mut reference = Reference::default();
        for (origin, end) in [(1, 10), (2, 20), (1, 5), (3, 30)] {
            let range = ByteRange::new(0, end);
            held.hold(VolumeIndex(0), range, NodeId(origin));
            reference.hold(VolumeIndex(0), range, NodeId(origin));
        }
        assert_eq!(held.iter().collect::<Vec<_>>(), reference.0);
    }

    // 5,000 losses scattered over one volume, none touching: the old list
    // moved every run after the new one and walked the whole list per hold;
    // the map looks at the run before it. Prints both counts, and bounds
    // the runs looked at.
    #[test]
    fn scattered_holds_cost_a_lookup_each() {
        // An odd multiplier modulo 2^32 sends each index to its own block,
        // in an order that jumps about the volume.
        let losses: Vec<ByteRange> = (0..5_000_u64)
            .map(|index| {
                let start = (index.wrapping_mul(2_654_435_761) % (1 << 32)) << 16;
                ByteRange::new(start, start + 1_000)
            })
            .collect();
        let mut reference = Reference::default();
        let touched: u64 = losses
            .iter()
            .map(|range| reference.hold(VolumeIndex(0), *range, NodeId(0)))
            .sum();
        let mut held = HeldLost::default();
        for range in &losses {
            held.hold(VolumeIndex(0), *range, NodeId(0));
        }
        assert_eq!(held.iter().collect::<Vec<_>>(), reference.0);
        eprintln!(
            "5,000 scattered holds: old {touched} runs moved or walked; new {} runs walked",
            held.walked
        );
        assert!(
            held.walked <= 5_000,
            "a scattered hold looks at the run before it and no other: {} runs for 5,000",
            held.walked
        );
    }
}
