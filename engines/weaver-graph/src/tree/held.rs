// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The lost ranges of one node's volumes its object holds until it can place
// them, each with the node the loss was told at. The runs of one volume and
// one origin are kept apart from every other's, sorted by offset, and runs
// that touch or overlap are one run: no two runs of a volume and an origin
// share a byte. A hold costs a lookup and the runs it merges. Bytes that
// arrive after a loss was told are no loss: they come out of the runs, at a
// lookup for each origin that holds runs of the volume and the runs the
// bytes cover, never a walk of the others.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound;

use crate::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

// One run: its end, and the order it was held in.
type Run = (u64, u64);

// Where a run sorts among all: its volume, its start, and the order it was
// held in, the later first among runs that start together.
type At = (VolumeIndex, u64, Reverse<u64>);

#[derive(Debug, Default)]
pub(super) struct HeldLost {
    // The runs of each volume and origin, by where each starts. A volume
    // and origin with no run has no entry.
    runs: BTreeMap<(VolumeIndex, NodeId), BTreeMap<u64, Run>>,
    // How many runs were held: the order of the next.
    held: u64,
    // The runs holds and arrivals have looked at, for the cost tests.
    #[cfg(test)]
    walked: u64,
}

impl HeldLost {
    pub(super) fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    // Every run in its order: by volume and offset, a range held later
    // before one held earlier at the same offset.
    fn ordered(&self) -> Vec<(At, u64, NodeId)> {
        let mut all: Vec<(At, u64, NodeId)> = self
            .runs
            .iter()
            .flat_map(|(&(volume, origin), list)| {
                list.iter().map(move |(&start, &(end, order))| {
                    ((volume, start, Reverse(order)), end, origin)
                })
            })
            .collect();
        all.sort_unstable_by_key(|&(at, _, _)| at);
        all
    }

    #[cfg(test)]
    fn iter(&self) -> impl Iterator<Item = (VolumeIndex, ByteRange, NodeId)> + '_ {
        self.ordered()
            .into_iter()
            .map(|((volume, start, _), end, origin)| (volume, ByteRange::new(start, end), origin))
    }

    // Hold a lost range, merged with the runs of its volume and origin it
    // touches or overlaps. The run keeps the order of the one it starts
    // with.
    pub(super) fn hold(&mut self, volume: VolumeIndex, range: ByteRange, origin: NodeId) {
        if range.is_empty() {
            return;
        }
        let mut order = self.held;
        self.held += 1;
        let list = self.runs.entry((volume, origin)).or_default();
        let (mut start, mut end) = (range.start, range.end);
        #[cfg(test)]
        {
            self.walked += 1;
        }
        // The run before it takes it in, or it sits as a run of its own.
        if let Some((&before, &(before_end, before_order))) = list.range(..=start).next_back()
            && before_end >= start
        {
            start = before;
            end = end.max(before_end);
            order = before_order;
        }
        // It takes in each run after it that it now reaches.
        loop {
            let after = (Bound::Excluded(start), Bound::Unbounded);
            let Some((&next, &(next_end, _))) = list.range(after).next() else {
                break;
            };
            if next > end {
                break;
            }
            #[cfg(test)]
            {
                self.walked += 1;
            }
            end = end.max(next_end);
            list.remove(&next);
        }
        list.insert(start, (end, order));
    }

    // The runs `placed` says now fall on a member, in their order, taken
    // out; the rest stay held.
    pub(super) fn take_placed(
        &mut self,
        mut placed: impl FnMut(VolumeIndex, ByteRange) -> bool,
    ) -> Vec<(VolumeIndex, ByteRange, NodeId)> {
        let mut taken = Vec::new();
        for ((volume, start, _), end, origin) in self.ordered() {
            let range = ByteRange::new(start, end);
            if !placed(volume, range) {
                continue;
            }
            taken.push((volume, range, origin));
            if let Some(list) = self.runs.get_mut(&(volume, origin)) {
                list.remove(&start);
                if list.is_empty() {
                    self.runs.remove(&(volume, origin));
                }
            }
        }
        taken
    }

    // Bytes of the range arrived: they are no loss, whatever origin told
    // it. Each run they cover goes, and one they cover in part keeps the
    // part before them and the part after them, in its order. For each
    // origin that holds runs of the volume, the run that reaches into the
    // range and the runs that start in it are looked at, and no other.
    pub(super) fn arrived(&mut self, volume: VolumeIndex, range: ByteRange) {
        if range.is_empty() {
            return;
        }
        let origins = (volume, NodeId(0))..=(volume, NodeId(u32::MAX));
        let mut emptied = Vec::new();
        let mut covered = Vec::new();
        for (&of, list) in self.runs.range_mut(origins) {
            covered.clear();
            // The run that starts at or before the range and reaches into
            // it, then each that starts inside it.
            covered.extend(
                list.range(..=range.start)
                    .next_back()
                    .filter(|&(_, &(end, _))| end > range.start)
                    .map(|(&start, _)| start),
            );
            let inside = (Bound::Excluded(range.start), Bound::Excluded(range.end));
            covered.extend(list.range(inside).map(|(&start, _)| start));
            #[cfg(test)]
            {
                self.walked += covered.len() as u64;
            }
            for &start in &covered {
                let Some((end, order)) = list.remove(&start) else {
                    continue;
                };
                if start < range.start {
                    list.insert(start, (range.start, order));
                }
                if end > range.end {
                    list.insert(range.end, (end, order));
                }
            }
            if list.is_empty() {
                emptied.push(of);
            }
        }
        for of in emptied {
            self.runs.remove(&of);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Listed = (VolumeIndex, ByteRange, NodeId);

    // One volume and origin's runs the plain way: start, end, order.
    type Plain = Vec<(u64, u64, u64)>;

    // The list kept the plain way: every run of a volume and an origin in
    // one vector, sorted and merged over the whole of it after each hold,
    // and walked whole for bytes that arrive.
    #[derive(Default)]
    struct Reference {
        runs: BTreeMap<(VolumeIndex, NodeId), Plain>,
        held: u64,
    }

    impl Reference {
        fn hold(&mut self, volume: VolumeIndex, range: ByteRange, origin: NodeId) {
            if range.is_empty() {
                return;
            }
            let list = self.runs.entry((volume, origin)).or_default();
            list.push((range.start, range.end, self.held));
            self.held += 1;
            // Stable: of two runs that start together, the one held first
            // stays first, and a merged run keeps the order of its first.
            list.sort_by_key(|&(start, _, _)| start);
            let mut merged: Vec<(u64, u64, u64)> = Vec::with_capacity(list.len());
            for (start, end, order) in list.drain(..) {
                match merged.last_mut() {
                    Some((_, last_end, _)) if start <= *last_end => {
                        *last_end = (*last_end).max(end);
                    }
                    _ => merged.push((start, end, order)),
                }
            }
            *list = merged;
        }

        fn take_placed(&mut self, mut placed: impl FnMut(VolumeIndex, ByteRange) -> bool) {
            for (&(volume, _), list) in &mut self.runs {
                list.retain(|&(start, end, _)| !placed(volume, ByteRange::new(start, end)));
            }
            self.runs.retain(|_, list| !list.is_empty());
        }

        fn arrived(&mut self, volume: VolumeIndex, range: ByteRange) {
            for (&(of, _), list) in &mut self.runs {
                if of != volume {
                    continue;
                }
                let mut kept = Vec::with_capacity(list.len() + 1);
                for (start, end, order) in list.drain(..) {
                    if end <= range.start || start >= range.end {
                        kept.push((start, end, order));
                        continue;
                    }
                    if start < range.start {
                        kept.push((start, range.start, order));
                    }
                    if end > range.end {
                        kept.push((range.end, end, order));
                    }
                }
                *list = kept;
            }
            self.runs.retain(|_, list| !list.is_empty());
        }

        fn listed(&self) -> Vec<Listed> {
            let mut all: Vec<(At, u64, NodeId)> = self
                .runs
                .iter()
                .flat_map(|(&(volume, origin), list)| {
                    list.iter().map(move |&(start, end, order)| {
                        ((volume, start, Reverse(order)), end, origin)
                    })
                })
                .collect();
            all.sort_unstable_by_key(|&(at, _, _)| at);
            all.into_iter()
                .map(|((volume, start, _), end, origin)| {
                    (volume, ByteRange::new(start, end), origin)
                })
                .collect()
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
    // volumes and two origins, with runs placed and taken out and bytes
    // arriving between them: after every step the list is the one the
    // plain merge keeps, and what a placing takes is what it takes there.
    #[test]
    fn holds_keep_the_list_the_whole_merge_keeps() {
        for seed in 1..=64_u64 {
            let mut random = Seeded(0x9e37_79b9_7f4a_7c15 ^ seed);
            let mut held = HeldLost::default();
            let mut reference = Reference::default();
            for step in 0..400 {
                let volume = VolumeIndex(random.below(3) as u32);
                let origin = NodeId(random.below(2) as u32);
                let listed = reference.listed();
                let start = match random.below(4) {
                    // Scattered.
                    0 => random.below(1 << 20),
                    // Adjacent to or overlapping one already held.
                    _ => listed
                        .get(random.below(listed.len().max(1) as u64) as usize)
                        .map_or(0, |(_, r, _)| match random.below(3) {
                            0 => r.end,
                            1 => r.start + (r.end - r.start) / 2,
                            _ => r.start,
                        }),
                };
                let len = random.below(5_000);
                let range = ByteRange::new(start, start + len);
                match random.below(5) {
                    0 => {
                        held.arrived(volume, range);
                        reference.arrived(volume, range);
                    }
                    _ => {
                        held.hold(volume, range, origin);
                        reference.hold(volume, range, origin);
                    }
                }
                if random.below(16) == 0 {
                    let pick = random.next();
                    let placed = |volume: VolumeIndex, range: ByteRange| {
                        (u64::from(volume.0) ^ range.start ^ pick).is_multiple_of(3)
                    };
                    let before = reference.listed();
                    let taken = held.take_placed(placed);
                    reference.take_placed(placed);
                    let expected: Vec<Listed> = before
                        .into_iter()
                        .filter(|&(volume, range, _)| placed(volume, range))
                        .collect();
                    assert_eq!(taken, expected, "seed {seed} step {step}");
                }
                assert_eq!(
                    held.iter().collect::<Vec<_>>(),
                    reference.listed(),
                    "seed {seed} step {step}"
                );
                assert_eq!(held.is_empty(), reference.runs.is_empty());
            }
        }
    }

    fn runs(held: &HeldLost) -> Vec<(u32, u64, u64, u32)> {
        held.iter()
            .map(|(volume, range, origin)| (volume.0, range.start, range.end, origin.0))
            .collect()
    }

    fn holding(runs: &[(u32, u64, u64, u32)]) -> HeldLost {
        let mut held = HeldLost::default();
        for &(volume, start, end, origin) in runs {
            held.hold(
                VolumeIndex(volume),
                ByteRange::new(start, end),
                NodeId(origin),
            );
        }
        held
    }

    // Runs of different origins at one offset keep the order they had, the
    // later first, and a run of an origin merges with that origin's own
    // whatever another origin holds between them.
    #[test]
    fn runs_at_one_offset_keep_their_order() {
        let held = holding(&[(0, 0, 10, 1), (0, 0, 20, 2), (0, 0, 5, 1), (0, 0, 30, 3)]);
        assert_eq!(
            runs(&held),
            vec![(0, 0, 30, 3), (0, 0, 20, 2), (0, 0, 10, 1)]
        );
        let apart = holding(&[(0, 0, 100, 1), (0, 50, 60, 2), (0, 70, 200, 1)]);
        assert_eq!(runs(&apart), vec![(0, 0, 200, 1), (0, 50, 60, 2)]);
    }

    // Bytes that cover a run whole take it out; the runs beside it, and
    // the other volume's, stay.
    #[test]
    fn arrived_bytes_over_a_whole_run_take_it_out() {
        let mut held = holding(&[(0, 0, 100, 1), (0, 200, 300, 1), (1, 200, 300, 1)]);
        held.arrived(VolumeIndex(0), ByteRange::new(150, 350));
        assert_eq!(runs(&held), vec![(0, 0, 100, 1), (1, 200, 300, 1)]);
        held.arrived(VolumeIndex(0), ByteRange::new(0, 100));
        held.arrived(VolumeIndex(1), ByteRange::new(200, 300));
        assert!(held.is_empty());
    }

    // Bytes over the front, the middle or the back of a run leave the rest
    // of it held, of the same origin.
    #[test]
    fn arrived_bytes_over_part_of_a_run_leave_the_rest() {
        let mut front = holding(&[(0, 100, 200, 1)]);
        front.arrived(VolumeIndex(0), ByteRange::new(50, 150));
        assert_eq!(runs(&front), vec![(0, 150, 200, 1)]);

        let mut middle = holding(&[(0, 100, 200, 1)]);
        middle.arrived(VolumeIndex(0), ByteRange::new(120, 180));
        assert_eq!(runs(&middle), vec![(0, 100, 120, 1), (0, 180, 200, 1)]);

        let mut back = holding(&[(0, 100, 200, 1)]);
        back.arrived(VolumeIndex(0), ByteRange::new(150, 250));
        assert_eq!(runs(&back), vec![(0, 100, 150, 1)]);

        // The rest is held as any run is: a hold beside it merges in.
        middle.hold(VolumeIndex(0), ByteRange::new(120, 180), NodeId(1));
        assert_eq!(runs(&middle), vec![(0, 100, 200, 1)]);
    }

    // Two origins that lost one range both lose the bytes that came.
    #[test]
    fn arrived_bytes_leave_every_origin() {
        let mut held = holding(&[(0, 0, 100, 1), (0, 0, 100, 2)]);
        held.arrived(VolumeIndex(0), ByteRange::new(0, 60));
        assert_eq!(runs(&held), vec![(0, 60, 100, 2), (0, 60, 100, 1)]);
        held.arrived(VolumeIndex(0), ByteRange::new(60, 100));
        assert!(held.is_empty());
    }

    // Nothing held, or nothing held where the bytes came, changes nothing.
    #[test]
    fn arrived_bytes_where_nothing_is_held_change_nothing() {
        let mut empty = HeldLost::default();
        empty.arrived(VolumeIndex(0), ByteRange::new(0, 100));
        assert!(empty.is_empty());

        let mut held = holding(&[(0, 100, 200, 1)]);
        held.arrived(VolumeIndex(0), ByteRange::new(0, 100));
        held.arrived(VolumeIndex(0), ByteRange::new(200, 300));
        held.arrived(VolumeIndex(1), ByteRange::new(100, 200));
        held.arrived(VolumeIndex(0), ByteRange::new(150, 150));
        assert_eq!(runs(&held), vec![(0, 100, 200, 1)]);
    }

    // One origin lost the whole volume once, and another holds 5,000
    // scattered runs of it. Bytes that come over each scattered run look at
    // the run they reach and the whole-volume run, never at the other
    // 4,999: a long run does not widen what a later arrival walks.
    #[test]
    fn arrived_bytes_look_only_at_the_runs_they_cover() {
        let starts: Vec<u64> = (0..5_000_u64)
            .map(|index| (index.wrapping_mul(2_654_435_761) % (1 << 32)) << 16)
            .collect();
        let mut held = HeldLost::default();
        held.hold(VolumeIndex(0), ByteRange::new(0, u64::MAX), NodeId(1));
        for &start in &starts {
            held.hold(
                VolumeIndex(0),
                ByteRange::new(start, start + 1_000),
                NodeId(0),
            );
        }
        held.walked = 0;
        for &start in &starts {
            held.arrived(VolumeIndex(0), ByteRange::new(start + 400, start + 600));
        }
        assert_eq!(
            held.walked, 10_000,
            "each arrival looks at the scattered run it reaches and the whole-volume run"
        );
        // Each scattered run is two, and the whole-volume run 5,001 pieces.
        assert_eq!(held.iter().count(), 15_001);
    }

    // 5,000 losses scattered over one volume, none touching: each hold
    // looks at the run before it and no other.
    #[test]
    fn scattered_holds_cost_a_lookup_each() {
        // An odd multiplier modulo 2^32 sends each index to its own block,
        // in an order that jumps about the volume.
        let mut held = HeldLost::default();
        for index in 0..5_000_u64 {
            let start = (index.wrapping_mul(2_654_435_761) % (1 << 32)) << 16;
            held.hold(
                VolumeIndex(0),
                ByteRange::new(start, start + 1_000),
                NodeId(0),
            );
        }
        assert_eq!(held.iter().count(), 5_000);
        assert!(
            held.walked <= 5_000,
            "a scattered hold looks at the run before it and no other: {} runs for 5,000",
            held.walked
        );
    }
}
