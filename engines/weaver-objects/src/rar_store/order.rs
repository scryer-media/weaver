// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The order the headers prove for volumes held on trust. A RAR4 volume
// short of a numbered end record states no position past the first, so its
// name puts it where it is held; a name is a label, and the headers outrank
// it where they say something of the order. They do where a member runs
// across volumes: the volume holding a member's first part comes before
// every volume continuing it, and the volume holding its last part after
// every volume carrying it on, so every volume that only carries one member
// on comes before the one that closes it. A volume whose end record says no
// volume follows it comes after all the others. Nothing else in the headers
// orders two volumes: two that each only carry the same member on, as the
// middle volumes of one long member do, are alike, and so are two that each
// open on a member they do not continue. Their names order them.
//
// unrar reads a set in the order of its names, each next volume the one
// its name numbers next, and checks no order but the packed checksum of each
// part (`volume.cpp`, `MergeArchive`), so a set posted under swapped names
// fails its file checksum there. Weaver places the volumes by what their
// headers prove and keeps the names only where the headers are silent.
//
// A volume the headers put after volumes still to come has no place yet:
// they state no count of the volumes that carry a member on, and the store
// learns of a volume only as it arrives. So the order is laid once, when
// the job is quiet and nothing more will arrive, and until then nothing is
// mapped from the first pair of volumes held side by side the wrong way
// round. Each volume moves once at most. A set already in its headers'
// order is mapped as it arrives, as before.
//
// The pairs held the wrong way round are filed as volumes are touched: two
// neighbour pairs per volume, each one lookup, so an arrival costs
// O(log n). The order is laid over the run of volumes held on trust past
// the volumes the layout holds: each is filed once in a map by the member
// it continues, and the chain is read from the one the layout ends on, each
// step a lookup. For a run of `m` volumes that is O(m log m), once a set.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use unrar_rs::{RarVolumeFacts, RarVolumeMemberFacts};
use weaver_graph::{Emit, VolumeIndex};

use super::{RarStore, Volume, claim, layout};

// A member as the headers name it across volumes: its name and its size.
type Key<'a> = (&'a str, Option<u64>);

fn key(member: &RarVolumeMemberFacts) -> Key<'_> {
    (member.name.as_str(), member.unpacked_size)
}

// The member a volume opens on when it continues one begun before it, and
// the one it ends on when that goes on into the next.
fn opens(facts: &RarVolumeFacts) -> Option<Key<'_>> {
    facts
        .members
        .iter()
        .min_by_key(|member| member.order)
        .filter(|member| member.split_before)
        .map(key)
}

fn ends(facts: &RarVolumeFacts) -> Option<Key<'_>> {
    facts
        .members
        .iter()
        .max_by_key(|member| member.order)
        .filter(|member| member.split_after)
        .map(key)
}

// One volume of the run: where it is held, what its headers state, and
// whether its end record says no volume follows it.
pub(super) struct Held<'a> {
    pub(super) at: VolumeIndex,
    pub(super) facts: &'a RarVolumeFacts,
    pub(super) last: bool,
}

// The run's volumes in the order their headers prove, read on from
// `before`, the volume the run follows: where the headers are silent, in
// the order they are held, which is their names'. Every volume of the run
// is in the answer once.
pub(super) fn order(before: &RarVolumeFacts, run: &[Held<'_>]) -> Vec<VolumeIndex> {
    // By the member each volume continues: those that carry it on as well,
    // and those that end it or go on into another. A volume that says no
    // volume follows it goes last whatever it continues.
    let mut carrying: BTreeMap<Key<'_>, VecDeque<VolumeIndex>> = BTreeMap::new();
    let mut leaving: BTreeMap<Option<Key<'_>>, VecDeque<VolumeIndex>> = BTreeMap::new();
    let mut outs = BTreeMap::new();
    let mut lasts = Vec::new();
    for held in run {
        if held.last {
            lasts.push(held.at);
            continue;
        }
        let (from, to) = (opens(held.facts), ends(held.facts));
        outs.insert(held.at, to);
        match from.filter(|from| to == Some(*from)) {
            Some(from) => carrying.entry(from).or_default().push_back(held.at),
            None => leaving.entry(from).or_default().push_back(held.at),
        }
    }
    let mut placed = Vec::with_capacity(run.len());
    let mut state = ends(before);
    loop {
        // Every volume that only carries a member on comes before the one
        // that ends it; one between two members is ordered by its name.
        let next = state
            .and_then(|member| carrying.get_mut(&member)?.pop_front())
            .or_else(|| leaving.get_mut(&state)?.pop_front());
        let Some(next) = next else {
            break;
        };
        placed.push(next);
        state = outs[&next];
    }
    // What the chain does not reach the headers do not order: it keeps its
    // place among the rest, ahead of the volumes that end the set.
    let mut rest: Vec<VolumeIndex> = carrying
        .into_values()
        .chain(leaving.into_values())
        .flatten()
        .collect();
    rest.sort_unstable();
    placed.extend(rest);
    placed.extend(lasts);
    placed
}

impl RarStore {
    // File the pairs beside each volume touched: held the wrong way round
    // by their headers, or not.
    pub(super) fn file_inversions(&mut self, touched: &BTreeSet<VolumeIndex>) {
        for volume in touched {
            for at in [volume.0.checked_sub(1), Some(volume.0)]
                .into_iter()
                .flatten()
            {
                match claim::inverted(at, &self.volumes) {
                    true => self.inversions.insert(at),
                    false => self.inversions.remove(&at),
                };
            }
        }
    }

    // The first volume nothing is mapped from until the set is ordered.
    pub(super) fn held_out(&self) -> Option<u32> {
        self.inversions.first().copied()
    }

    // Quiet, with volumes held the wrong way round: the run past the layout
    // is laid in its headers' order, once. Whether any volume moved.
    pub(super) fn order_at_quiet(&mut self, out: &mut Vec<Emit>) -> bool {
        if self.inversions.is_empty() {
            return false;
        }
        let Some(from) = self.laid_through().map(VolumeIndex) else {
            return false;
        };
        let moved = self.reorder(from, out);
        if moved {
            self.settle(None, out);
        }
        moved
    }

    // Lay the volumes held on trust from `from`, the first place past what
    // the layout holds, in the order their headers prove, and give back
    // each one held anywhere else, marked with its place: it is claimed
    // there again, ahead of its name. The run is the volumes held side by
    // side from `from` whose headers state no position, none of them
    // mapped. A run whose volume before it is not held, or whose order is
    // the one held, moves nothing.
    fn reorder(&mut self, from: VolumeIndex, out: &mut Vec<Emit>) -> bool {
        let Some(before) = from
            .0
            .checked_sub(1)
            .and_then(|at| self.volumes.get(&VolumeIndex(at)))
            .and_then(Volume::stated)
        else {
            return false;
        };
        let mut run = Vec::new();
        for (at, found) in self.volumes.range(from..) {
            let next = from.0 as usize + run.len();
            let Some(facts) = found.stated() else {
                break;
            };
            if at.0 as usize != next
                || !layout::on_trust(found)
                || !self.unmapped.contains(at)
                || claim::misplaced(*at, facts)
            {
                break;
            }
            run.push(Held {
                at: *at,
                facts,
                last: found.walk.is_last(),
            });
        }
        #[cfg(test)]
        super::counts::bump(|counts| counts.orders += 1);
        let moves: Vec<(VolumeIndex, VolumeIndex)> = order(before, &run)
            .into_iter()
            .zip(run.iter().map(|held| held.at))
            .filter(|(held, place)| held != place)
            .collect();
        if moves.is_empty() {
            return false;
        }
        // The places change, so no place stays refuted on what it held.
        self.refuted = None;
        for (held, place) in &moves {
            let found = self.volumes.get_mut(held).expect("a volume of the run");
            found.ordered = Some((*place, found.walk.stamp));
        }
        self.give_back_held(moves.into_iter().map(|(held, _)| held).collect(), out);
        true
    }
}
