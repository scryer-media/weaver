// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store's placement number: one that moves whenever anything its
// `locate` reads may change, so its owner asks again only then. Every
// store's numbers come from one counter, so no two stores, a store restored
// in place of another included, ever share one.
//
// A move is store-wide, or of one volume alone: beside its number the store
// keeps the number it took at its last store-wide move, and each volume
// moved alone since, under the number its latest move took. A reader asks
// what moved since a number it was given, and asks again only the volumes
// listed. The record holds one entry per volume, and a store-wide move
// empties it.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use weaver_graph::{PlacementMoves, VolumeIndex};

static PLACED: AtomicU64 = AtomicU64::new(0);

fn fresh() -> u64 {
    PLACED.fetch_add(1, Ordering::Relaxed) + 1
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Placed {
    number: u64,
    // The number taken at the last store-wide move, the store's first
    // included: nothing said before it is the store's as it stands.
    all_at: u64,
    // Each volume moved alone since, under its latest move's number, and
    // the same by number.
    latest: BTreeMap<VolumeIndex, u64>,
    by_at: BTreeMap<u64, VolumeIndex>,
}

impl Default for Placed {
    fn default() -> Self {
        let number = fresh();
        Self {
            number,
            all_at: number,
            latest: BTreeMap::new(),
            by_at: BTreeMap::new(),
        }
    }
}

impl Placed {
    // Something `locate` reads may have changed, for any volume.
    pub(crate) fn moved(&mut self) {
        *self = Self::default();
    }

    // Something `locate` reads may have changed, for `volume` alone.
    pub(crate) fn moved_at(&mut self, volume: VolumeIndex) {
        self.number = fresh();
        if let Some(was) = self.latest.insert(volume, self.number) {
            self.by_at.remove(&was);
        }
        self.by_at.insert(self.number, volume);
    }

    pub(crate) fn number(&self) -> u64 {
        self.number
    }

    // What moved since the store said `number`: all of it when a
    // store-wide move came after, a store made after it included, or when
    // it is past the store's own, else the volumes moved alone after it.
    pub(crate) fn since(&self, number: u64) -> PlacementMoves {
        if number < self.all_at || number > self.number {
            return PlacementMoves::All;
        }
        PlacementMoves::Volumes(
            self.by_at
                .range(number.saturating_add(1)..)
                .map(|(_, volume)| *volume)
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_volume_moved_alone_is_the_one_said_moved() {
        let mut placed = Placed::default();
        let start = placed.number();
        assert_eq!(placed.since(start), PlacementMoves::Volumes(Vec::new()));
        placed.moved_at(VolumeIndex(3));
        let after_three = placed.number();
        assert_ne!(after_three, start, "the number moves on every move");
        placed.moved_at(VolumeIndex(1));
        placed.moved_at(VolumeIndex(3));
        assert_eq!(
            placed.since(start),
            PlacementMoves::Volumes(vec![VolumeIndex(1), VolumeIndex(3)]),
            "each volume once, in the order of its latest move"
        );
        assert_eq!(
            placed.since(after_three),
            PlacementMoves::Volumes(vec![VolumeIndex(1), VolumeIndex(3)])
        );
        assert_eq!(
            placed.since(placed.number()),
            PlacementMoves::Volumes(Vec::new())
        );
        assert_eq!(placed.latest.len(), 2, "one entry per volume");
        assert_eq!(placed.by_at.len(), 2);
    }

    #[test]
    fn a_store_wide_move_says_everything_moved_before_it() {
        let mut placed = Placed::default();
        let start = placed.number();
        placed.moved_at(VolumeIndex(2));
        placed.moved();
        let wide = placed.number();
        assert_eq!(placed.since(start), PlacementMoves::All);
        placed.moved_at(VolumeIndex(5));
        assert_eq!(
            placed.since(wide),
            PlacementMoves::Volumes(vec![VolumeIndex(5)])
        );
        assert_eq!(placed.latest.len(), 1, "the store-wide move emptied it");
        // A number another store said is never this one's.
        let other = Placed::default();
        assert_eq!(placed.since(other.number() + 1), PlacementMoves::All);
    }
}
