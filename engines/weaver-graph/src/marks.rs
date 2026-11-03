// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The keys a state changed, each under the generation of its latest change,
// which rises with every change: what was read whole at generation `at` is
// brought up to date by reading again what changed since. A key that went
// away keeps its mark, so a reader since an earlier one learns it is gone.

use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Marks<K> {
    at: u64,
    latest: BTreeMap<K, u64>,
    by_at: BTreeMap<u64, K>,
}

impl<K> Default for Marks<K> {
    fn default() -> Self {
        Self {
            at: 0,
            latest: BTreeMap::new(),
            by_at: BTreeMap::new(),
        }
    }
}

impl<K: Ord + Copy> Marks<K> {
    pub fn mark(&mut self, key: K) {
        self.at += 1;
        if let Some(was) = self.latest.insert(key, self.at) {
            self.by_at.remove(&was);
        }
        self.by_at.insert(self.at, key);
    }

    pub fn at(&self) -> u64 {
        self.at
    }

    pub fn since(&self, at: u64) -> impl DoubleEndedIterator<Item = K> + '_ {
        self.by_at
            .range(at.saturating_add(1)..)
            .map(|(_, key)| *key)
    }
}

// A map that marks every entry it hands out to change, and every one it
// takes in or gives up: an owner asks which entries changed since a
// generation, and nothing that changes an entry can go around the mark.
#[derive(Debug)]
pub struct Marked<K, V> {
    map: BTreeMap<K, V>,
    marks: Marks<K>,
}

impl<K, V> Default for Marked<K, V> {
    fn default() -> Self {
        Self {
            map: BTreeMap::new(),
            marks: Marks::default(),
        }
    }
}

impl<K, V> std::ops::Deref for Marked<K, V> {
    type Target = BTreeMap<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl<K: Ord + Copy, V> Marked<K, V> {
    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let found = self.map.get_mut(key)?;
        self.marks.mark(*key);
        Some(found)
    }

    pub fn get_or_insert_with(&mut self, key: K, make: impl FnOnce() -> V) -> &mut V {
        self.marks.mark(key);
        self.map.entry(key).or_insert_with(make)
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        self.marks.mark(key);
        self.map.insert(key, value)
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        let removed = self.map.remove(key)?;
        self.marks.mark(*key);
        Some(removed)
    }

    // An entry changed by what is kept beside the map: an owner whose
    // entries are read together with other state marks the entry that
    // state changed for.
    pub fn mark(&mut self, key: K) {
        self.marks.mark(key);
    }

    pub fn marks(&self) -> &Marks<K> {
        &self.marks
    }

    // The entries, the marks given up with the map.
    pub fn into_map(self) -> BTreeMap<K, V> {
        self.map
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::deliver::{Posted, Provenance};
    use crate::member::MemberId;
    use crate::tree::record_tests::{Objects, three_levels};
    use crate::tree::{Mark, NodeId, Tree};

    fn deliver(tree: &mut Tree, member: MemberId, offset: u64, bytes: &[u8]) {
        let arrival = Posted {
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance: Provenance::Fresh,
        };
        tree.deliver(NodeId::ROOT, member, arrival, &mut Objects, &mut Vec::new());
    }

    // An article's bytes for one root member mark that member, and none of
    // its siblings; the root's own row is not marked.
    #[test]
    fn writing_one_members_coverage_marks_that_member_alone() {
        let mut tree = Tree::new();
        let mut effects = Vec::new();
        let members: Vec<MemberId> = ["a.bin", "b.bin", "c.bin"]
            .into_iter()
            .map(|name| tree.announce_root(name, Some(4), &mut effects))
            .collect();
        for member in &members {
            deliver(&mut tree, *member, 0, b"le");
        }
        let at = tree.generation();
        let namespace_at = tree.namespace(NodeId::ROOT).unwrap().generation();

        deliver(&mut tree, members[2], 2, b"af");

        let marked: Vec<Mark> = tree.marked_since(at).collect();
        assert_eq!(marked, vec![Mark::Members(NodeId::ROOT)]);
        let namespace = tree.namespace(NodeId::ROOT).unwrap();
        let changed: Vec<MemberId> = namespace.changed_since(namespace_at).collect();
        assert_eq!(changed, vec![members[2]]);
    }

    // Bytes an object takes reach it through its node, so its node's own
    // row, which carries its facts, is marked; the root's is not.
    #[test]
    fn writing_object_facts_marks_the_node() {
        let (mut tree, archive) = three_levels();
        let object = tree.child_of(NodeId::ROOT, archive).unwrap().0;
        let at = tree.generation();

        deliver(&mut tree, archive, 18, b"more");

        let marked: Vec<Mark> = tree.marked_since(at).collect();
        assert!(marked.contains(&Mark::Node(object)), "{marked:?}");
        assert!(!marked.contains(&Mark::Node(NodeId::ROOT)), "{marked:?}");
    }
}
