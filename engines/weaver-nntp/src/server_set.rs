// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A set of a pool's servers, one bit per pool index.
//
// A bit is a pool index of one pool generation: a set taken under one pool
// layout says nothing about another, and whoever holds one carries it to
// the new layout through the [`ServerRemap`] between the two when the pool
// is rebuilt.

// The most news servers Weaver takes, configured or not.
pub const MAX_PROVIDERS: usize = 50;

const _: () = assert!(MAX_PROVIDERS <= ServerSet::CAPACITY);

#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ServerSet(u64);

impl ServerSet {
    // The most servers one set can hold.
    pub const CAPACITY: usize = u64::BITS as usize;

    pub const EMPTY: Self = Self(0);

    pub const fn new() -> Self {
        Self(0)
    }

    // Every server of a pool of `count`.
    pub fn first(count: usize) -> Self {
        assert!(
            count <= Self::CAPACITY,
            "a pool holds at most {} servers",
            Self::CAPACITY
        );
        if count == Self::CAPACITY {
            Self(u64::MAX)
        } else {
            Self((1u64 << count) - 1)
        }
    }

    pub const fn of(server: usize) -> Self {
        assert!(server < Self::CAPACITY, "server index beyond a server set");
        Self(1u64 << server)
    }

    pub fn insert(&mut self, server: usize) -> bool {
        let bit = Self::of(server).0;
        let added = self.0 & bit == 0;
        self.0 |= bit;
        added
    }

    pub fn remove(&mut self, server: usize) -> bool {
        if server >= Self::CAPACITY {
            return false;
        }
        let bit = 1u64 << server;
        let had = self.0 & bit != 0;
        self.0 &= !bit;
        had
    }

    pub const fn contains(self, server: usize) -> bool {
        server < Self::CAPACITY && self.0 & (1u64 << server) != 0
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub fn clear(&mut self) {
        self.0 = 0;
    }

    // Whether every server of `other` is in this set.
    pub const fn covers(self, other: Self) -> bool {
        other.0 & !self.0 == 0
    }

    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    pub const fn intersection(self, other: Self) -> Self {
        Self(self.0 & other.0)
    }

    pub const fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    // The lowest server index in the set.
    pub const fn lowest(self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(self.0.trailing_zeros() as usize)
        }
    }

    // Server indices, lowest first.
    pub fn iter(self) -> impl Iterator<Item = usize> {
        let mut bits = self.0;
        std::iter::from_fn(move || {
            if bits == 0 {
                return None;
            }
            let server = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            Some(server)
        })
    }

    // The servers as an index list, lowest first: the form used where a set
    // crosses into storage, diagnostics or the API.
    pub fn to_vec(self) -> Vec<usize> {
        self.iter().collect()
    }

    // A set from an index list. An index the set cannot hold is never part
    // of a pool: naming one is a caller's defect, caught in a debug build
    // and left out in a release one.
    pub fn from_indices<'a>(servers: impl IntoIterator<Item = &'a usize>) -> Self {
        Self::of_those_held(servers.into_iter().inspect(|server| {
            debug_assert!(
                **server < Self::CAPACITY,
                "server index {server} is past what a pool holds"
            );
        }))
    }

    // The indices of a list that the set can hold.
    fn of_those_held<'a>(servers: impl IntoIterator<Item = &'a usize>) -> Self {
        servers
            .into_iter()
            .filter(|server| **server < Self::CAPACITY)
            .fold(Self::EMPTY, |set, server| set.union(Self::of(*server)))
    }
}

impl std::fmt::Debug for ServerSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

impl FromIterator<usize> for ServerSet {
    fn from_iter<I: IntoIterator<Item = usize>>(iter: I) -> Self {
        let mut set = Self::EMPTY;
        for server in iter {
            set.insert(server);
        }
        set
    }
}

impl Extend<usize> for ServerSet {
    fn extend<I: IntoIterator<Item = usize>>(&mut self, iter: I) {
        for server in iter {
            self.insert(server);
        }
    }
}

impl<const N: usize> From<[usize; N]> for ServerSet {
    fn from(servers: [usize; N]) -> Self {
        servers.into_iter().collect()
    }
}

impl PartialEq<[usize]> for ServerSet {
    fn eq(&self, other: &[usize]) -> bool {
        Self::of_those_held(other) == *self && other.len() == self.len()
    }
}

impl<const N: usize> PartialEq<[usize; N]> for ServerSet {
    fn eq(&self, other: &[usize; N]) -> bool {
        *self == other[..]
    }
}

impl<const N: usize> PartialEq<&[usize; N]> for ServerSet {
    fn eq(&self, other: &&[usize; N]) -> bool {
        *self == other[..]
    }
}

impl PartialEq<&[usize]> for ServerSet {
    fn eq(&self, other: &&[usize]) -> bool {
        *self == **other
    }
}

impl PartialEq<Vec<usize>> for ServerSet {
    fn eq(&self, other: &Vec<usize>) -> bool {
        *self == other[..]
    }
}

// Where each server of one pool layout sits in the next: the old index
// maps to the new one, or to nothing when the server is gone or counts as a
// different server. Built once per rebuild; carrying a set across is a walk
// of its bits, with nothing allocated.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct ServerRemap([u8; ServerSet::CAPACITY]);

impl ServerRemap {
    const GONE: u8 = u8::MAX;

    // Maps nothing: every set carried across comes out empty.
    pub const NONE: Self = Self([Self::GONE; ServerSet::CAPACITY]);

    // Maps every server of a pool of `count` to itself.
    pub fn identity(count: usize) -> Self {
        let mut remap = Self::NONE;
        for (index, slot) in remap.0.iter_mut().enumerate().take(count) {
            *slot = index as u8;
        }
        remap
    }

    // The map from `old` to `new`, both given as each index's identity, in
    // pool order: an old server maps to the new index whose identity is
    // equal, and to nothing when no new server has it. An identity held
    // twice in `new` maps to the first.
    pub fn between<T: Eq + std::hash::Hash>(old: &[T], new: &[T]) -> Self {
        let mut at: std::collections::HashMap<&T, usize> =
            std::collections::HashMap::with_capacity(new.len());
        for (index, identity) in new.iter().enumerate().take(ServerSet::CAPACITY) {
            at.entry(identity).or_insert(index);
        }
        let mut remap = Self::NONE;
        for (slot, identity) in remap.0.iter_mut().zip(old) {
            if let Some(index) = at.get(identity) {
                *slot = *index as u8;
            }
        }
        remap
    }

    // Where the old server `index` sits now.
    pub fn index(&self, index: usize) -> Option<usize> {
        self.0
            .get(index)
            .copied()
            .filter(|slot| *slot != Self::GONE)
            .map(usize::from)
    }

    // `set` carried to the new layout: a server that is gone drops out.
    pub fn set(&self, set: ServerSet) -> ServerSet {
        let mut bits = set.0;
        let mut out = 0u64;
        while bits != 0 {
            let index = bits.trailing_zeros() as usize;
            bits &= bits - 1;
            let slot = self.0[index];
            if slot != Self::GONE {
                out |= 1 << slot;
            }
        }
        ServerSet(out)
    }

    // This map followed by `next`: from this one's old layout to `next`'s
    // new one.
    pub fn then(&self, next: &Self) -> Self {
        let mut remap = Self::NONE;
        for (slot, through) in remap.0.iter_mut().zip(self.0) {
            if through != Self::GONE {
                *slot = next.0[usize::from(through)];
            }
        }
        remap
    }
}

impl std::fmt::Debug for ServerRemap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map()
            .entries((0..ServerSet::CAPACITY).filter_map(|index| Some((index, self.index(index)?))))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_remap_carries_sets_by_identity() {
        // A, B, C, D become C, B, A, E: D is gone and E is new.
        let remap = ServerRemap::between(&["a", "b", "c", "d"], &["c", "b", "a", "e"]);
        assert_eq!(remap.index(0), Some(2));
        assert_eq!(remap.index(1), Some(1));
        assert_eq!(remap.index(2), Some(0));
        assert_eq!(remap.index(3), None);
        assert_eq!(remap.index(63), None);
        assert_eq!(remap.set(ServerSet::from([0, 2, 3])), [0, 2]);
        assert_eq!(remap.set(ServerSet::EMPTY), ServerSet::EMPTY);
        assert_eq!(
            ServerRemap::NONE.set(ServerSet::from([0, 1])),
            ServerSet::EMPTY
        );
        assert_eq!(
            ServerRemap::identity(4).set(ServerSet::from([0, 3])),
            [0, 3]
        );
        // Then back: C, B, A, E become A, B, C.
        let back = ServerRemap::between(&["c", "b", "a", "e"], &["a", "b", "c"]);
        let round = remap.then(&back);
        assert_eq!(round.set(ServerSet::from([0, 1, 2, 3])), [0, 1, 2]);
    }

    #[test]
    fn holds_the_first_last_and_cap_bits() {
        for server in [0, 49, 63] {
            let mut set = ServerSet::new();
            assert!(!set.contains(server));
            assert!(set.insert(server));
            assert!(!set.insert(server));
            assert!(set.contains(server));
            assert_eq!(set.len(), 1);
            assert_eq!(set.lowest(), Some(server));
            assert_eq!(set.to_vec(), vec![server]);
            assert!(set.covers(ServerSet::of(server)));
            assert!(!ServerSet::EMPTY.covers(set));
            assert!(set.remove(server));
            assert!(set.is_empty());
        }
    }

    #[test]
    fn covers_is_a_superset_test() {
        let fill = ServerSet::from([0, 49, 63]);
        let mut refused = ServerSet::from([0, 49]);
        assert!(!refused.covers(fill));
        refused.insert(63);
        assert!(refused.covers(fill));
        refused.insert(5);
        assert!(refused.covers(fill));
        assert!(refused.covers(ServerSet::EMPTY));
    }

    #[test]
    fn a_whole_pool_up_to_the_capacity() {
        assert_eq!(ServerSet::first(0), ServerSet::EMPTY);
        assert_eq!(ServerSet::first(50).len(), 50);
        assert!(ServerSet::first(50).contains(49));
        assert!(!ServerSet::first(50).contains(50));
        assert_eq!(ServerSet::first(64).len(), 64);
        assert!(ServerSet::first(64).contains(63));
    }

    #[test]
    fn lists_round_trip_and_compare() {
        let set = ServerSet::from_indices(&[3, 0, 63, 3]);
        assert_eq!(set.to_vec(), vec![0, 3, 63]);
        assert_eq!(set, [0, 3, 63]);
        assert_ne!(set, [0, 3]);
        assert_eq!(format!("{set:?}"), "[0, 3, 63]");
        assert!(!set.contains(64));
        assert!(!set.contains(usize::MAX));
        assert_ne!(set, [0, 3, 64]);
    }
}
