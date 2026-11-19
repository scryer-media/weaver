// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The places a set's volumes are held at, and the free places beside them:
// the only places a volume ordered by its links alone can take, since it
// goes beside a volume it links to. Its owner files a place each time what
// says whether a volume is held there changes, so a placement reads the few
// places at the edges of each run of held volumes, never every volume held.

use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Beside {
    held: BTreeSet<u32>,
    // The places not held with a held place next to them.
    free: BTreeSet<u32>,
}

impl Beside {
    // File whether a volume is held at `index` now. The place and the two
    // beside it are the only ones whose standing this can change.
    pub(crate) fn file(&mut self, index: u32, held: bool) {
        let changed = match held {
            true => self.held.insert(index),
            false => self.held.remove(&index),
        };
        if !changed {
            return;
        }
        for slot in [index.checked_sub(1), Some(index), index.checked_add(1)]
            .into_iter()
            .flatten()
        {
            let free = !self.held.contains(&slot)
                && [slot.checked_sub(1), slot.checked_add(1)]
                    .into_iter()
                    .flatten()
                    .any(|at| self.held.contains(&at));
            match free {
                true => self.free.insert(slot),
                false => self.free.remove(&slot),
            };
        }
    }

    // The free places beside a held one, lowest first.
    pub(crate) fn free(&self) -> impl Iterator<Item = u32> + '_ {
        self.free.iter().copied()
    }

    // Every place held, lowest first.
    pub(crate) fn held(&self) -> impl Iterator<Item = u32> + '_ {
        self.held.iter().copied()
    }

    pub(crate) fn holds(&self, index: u32) -> bool {
        self.held.contains(&index)
    }
}

#[cfg(test)]
mod tests {
    use super::Beside;
    use std::collections::BTreeSet;

    // The free places beside the held ones, from the held set alone.
    fn walked(held: &BTreeSet<u32>) -> Vec<u32> {
        let free: BTreeSet<u32> = held
            .iter()
            .flat_map(|index| [index.checked_sub(1), index.checked_add(1)])
            .flatten()
            .filter(|slot| !held.contains(slot))
            .collect();
        free.into_iter().collect()
    }

    // Places held and let go in a scrambled order, out to the ends of the
    // range, leave the same free places a walk over the held ones finds.
    #[test]
    fn filed_places_match_a_walk_over_the_held_ones() {
        let mut beside = Beside::default();
        let mut held = BTreeSet::new();
        let mut seed = 0x9e37_79b9_u64;
        let places = [0, 1, 2, 3, 5, 6, 9, 10, 11, u32::MAX - 1, u32::MAX];
        for _ in 0..2_000 {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let index = places[(seed % places.len() as u64) as usize];
            let hold = !(seed >> 32).is_multiple_of(3);
            beside.file(index, hold);
            match hold {
                true => held.insert(index),
                false => held.remove(&index),
            };
            assert_eq!(beside.free().collect::<Vec<_>>(), walked(&held));
            assert_eq!(
                beside.held().collect::<Vec<_>>(),
                held.iter().copied().collect::<Vec<_>>()
            );
        }
    }
}
