// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree once the job is quiet: nothing more will arrive and no repair
// will come. Each data object hears the lost ranges of its volumes that
// nothing vouched for, then that the job is quiet, and rules for itself on
// what the tree records of it. The tree tells and reads; it chooses no
// reason. A recovery object heard its losses when they were marked, and
// the root came out of no object, so neither is told.

use super::{Builder, Effect, NodeId, Tree};
use crate::member::MemberId;
use crate::object::Standing;
use crate::recovery::RepairOutcome;
use crate::volume::{ByteRange, VolumeIndex};

impl Tree {
    // The object at `node` hears every lost range of its volumes that
    // nothing vouched for. What it says is applied as if it said it in
    // `deliver`.
    pub fn tell_lost(
        &mut self,
        node: NodeId,
        builder: &mut dyn Builder,
        effects: &mut Vec<Effect>,
    ) {
        if !self.told_when_quiet(node) {
            return;
        }
        let lost = self.lost(node);
        let Some(mut object) = self.take_object(node) else {
            return;
        };
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        for (volume, range) in lost {
            object.volume_lost(volume, range, &mut emits);
        }
        self.put_object(node, object);
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }

    // The object at `node` hears that the job is quiet, with what the tree
    // records of it, and rules: what it says is applied as if it said it in
    // `deliver`.
    pub fn quiet(&mut self, node: NodeId, builder: &mut dyn Builder, effects: &mut Vec<Effect>) {
        if !self.told_when_quiet(node) {
            return;
        }
        let Some(mut object) = self.take_object(node) else {
            return;
        };
        let mut emits = std::mem::take(&mut self.emits);
        emits.clear();
        object.quiet(&Stood { tree: self, node }, &mut emits);
        self.put_object(node, object);
        self.apply(node, &mut emits, builder, effects);
        self.emits = emits;
    }

    // A live data object: bound in a namespace, standing, and no recovery
    // object.
    fn told_when_quiet(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|found| !found.retired && found.binding.is_some())
            && !self.recovers(node)
    }

    // The ranges of the node's volumes held as damage, an article that never
    // came or a range an object said was damaged, in its own volume's
    // coordinates, unless a recovery object above vouched for what it came
    // out of.
    fn lost(&self, node: NodeId) -> Vec<(VolumeIndex, ByteRange)> {
        let Some((parent, volumes)) = self.volumes(node) else {
            return Vec::new();
        };
        let Some(namespace) = self.namespace(parent) else {
            return Vec::new();
        };
        if self.vouched_above(node) {
            return Vec::new();
        }
        volumes
            .iter()
            .filter_map(|(volume, member)| Some((*volume, namespace.get(*member)?)))
            .flat_map(|(volume, member)| {
                member
                    .coverage
                    .damaged_ranges()
                    .map(move |range| (volume, range))
            })
            .collect()
    }

    // Some member the namespace came out of, at any depth, is vouched for.
    fn vouched_above(&self, node: NodeId) -> bool {
        let mut at = node;
        while let Some((parent, volumes)) = self.volumes(at) {
            if volumes.values().any(|member| self.vouched(parent, *member)) {
                return true;
            }
            at = parent;
        }
        false
    }

    // A recovery object of the namespace covers the member and found it
    // whole or made it so.
    fn vouched(&self, node: NodeId, member: MemberId) -> bool {
        self.recovery_nodes(node).into_iter().any(|recovery_node| {
            self.recovery(recovery_node).is_some_and(|recovery| {
                recovery.covers(member)
                    && matches!(
                        recovery.outcome(),
                        Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                    )
            })
        })
    }
}

// The tree's record of one object, read while the object rules.
struct Stood<'a> {
    tree: &'a Tree,
    node: NodeId,
}

impl Standing for Stood<'_> {
    // A repair above rebuilt the bytes below, though the damage an object
    // marked on what it read before is still recorded.
    fn damaged(&self) -> bool {
        let (tree, node) = (self.tree, self.node);
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        !tree.vouched_above(node)
            && namespace.members().any(|member| {
                tree.child_of(node, member.id).is_none()
                    && member.coverage.damaged_ranges().next().is_some()
                    && !tree.retired.contains_key(&(node, member.id))
                    && !tree.given_back(node, member.id)
                    && !tree.vouched(node, member.id)
            })
    }

    // A range marked lost is held as damage, so a member open only for
    // bytes lost above it holds every byte it will: nothing left it
    // unplaced.
    fn unplaced(&self) -> bool {
        let (tree, node) = (self.tree, self.node);
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        namespace.is_empty()
            || namespace.members().any(|member| {
                !(member.closed
                    || member.is_complete()
                    || tree
                        .child_of(node, member.id)
                        .is_some_and(|(child, _)| tree.recovers(child)))
            })
    }

    fn lost(&self) -> bool {
        !self.tree.lost(self.node).is_empty()
    }
}
