// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The objects bound in a namespace, counted by kind: each child once, by
// the kind it was bound as, however many of the namespace's members it
// owns or has windows on. Kept as the node's children and windows change,
// which only the node's own methods do, so "is this object alone of its
// kind here" is one lookup, not a walk of the namespace's children.

use std::collections::{BTreeMap, HashMap};

use super::NodeId;
use crate::object::Kind;

#[derive(Debug, Default)]
pub(super) struct BoundKinds {
    // Each bound child, the kind it was bound as, and how many of the
    // namespace's entries (owned members and windows) name it.
    refs: BTreeMap<NodeId, (Option<Kind>, u32)>,
    // How many distinct children of each kind.
    kinds: HashMap<Option<Kind>, u32>,
}

impl BoundKinds {
    // One more entry names `child`.
    pub(super) fn add(&mut self, child: NodeId, kind: Option<Kind>) {
        let entry = self.refs.entry(child).or_insert((kind, 0));
        if entry.1 == 0 {
            *self.kinds.entry(entry.0).or_insert(0) += 1;
        }
        entry.1 += 1;
    }

    // One entry naming `child` went.
    pub(super) fn remove(&mut self, child: NodeId) {
        let Some(entry) = self.refs.get_mut(&child) else {
            return;
        };
        entry.1 = entry.1.saturating_sub(1);
        if entry.1 > 0 {
            return;
        }
        let kind = entry.0;
        self.refs.remove(&child);
        if let Some(count) = self.kinds.get_mut(&kind) {
            *count -= 1;
            if *count == 0 {
                self.kinds.remove(&kind);
            }
        }
    }

    // Whether `child` is bound here and no other child of its kind is.
    pub(super) fn alone(&self, child: NodeId) -> bool {
        self.refs
            .get(&child)
            .is_some_and(|(kind, _)| self.kinds.get(kind) == Some(&1))
    }

    // How many entries name each child, for a recount to check.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn refs(&self) -> BTreeMap<NodeId, u32> {
        self.refs
            .iter()
            .map(|(child, (_, count))| (*child, *count))
            .collect()
    }

    // How many distinct children of each kind, for a recount to check.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) fn counts(&self) -> BTreeMap<String, u32> {
        self.kinds
            .iter()
            .map(|(kind, count)| (format!("{kind:?}"), *count))
            .collect()
    }
}
