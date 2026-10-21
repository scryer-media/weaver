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
use crate::object::{Beside, Standing};
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
    // coordinates, less what a recovery object proved, unless what the
    // namespace came out of was vouched for above.
    fn lost(&self, node: NodeId) -> Vec<(VolumeIndex, ByteRange)> {
        let Some((parent, volumes)) = self.volumes(node) else {
            return Vec::new();
        };
        if self.vouched_above(node) {
            return Vec::new();
        }
        volumes
            .iter()
            .flat_map(|(volume, member)| {
                self.unproven(parent, *member)
                    .into_iter()
                    .map(move |range| (*volume, range))
            })
            .collect()
    }

    // Every member the namespace came out of, at some depth, holds no
    // damage a recovery object did not prove, and one of them was found
    // whole or made so.
    fn vouched_above(&self, node: NodeId) -> bool {
        let mut at = node;
        while let Some((parent, volumes)) = self.volumes(at) {
            let proven = |member: &MemberId| {
                self.ruled(parent, *member, |outcome| {
                    matches!(
                        outcome,
                        RepairOutcome::Clean | RepairOutcome::Repaired { .. }
                    )
                })
            };
            if volumes.values().any(proven)
                && volumes
                    .values()
                    .all(|member| self.unproven(parent, *member).is_empty())
            {
                return true;
            }
            at = parent;
        }
        false
    }

    // The member's damaged ranges past what every recovery object that
    // found it whole or made it so proves of it: a set vouches only for
    // the bytes its checks reach.
    fn unproven(&self, node: NodeId, member: MemberId) -> Vec<ByteRange> {
        let Some(record) = self
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
        else {
            return Vec::new();
        };
        let proving: Vec<_> = self
            .recovery_nodes(node)
            .into_iter()
            .filter_map(|recovery_node| self.recovery(recovery_node))
            .filter(|recovery| {
                recovery.covers(member)
                    && recovery.outcome().is_some_and(|outcome| {
                        matches!(
                            outcome,
                            RepairOutcome::Clean | RepairOutcome::Repaired { .. }
                        )
                    })
            })
            .collect();
        let mut left: Vec<ByteRange> = record.coverage.damaged_ranges().collect();
        for recovery in proving {
            left = left
                .into_iter()
                .flat_map(|range| {
                    let length = record.length.unwrap_or(range.end);
                    without(range, recovery.proves(member, length))
                })
                .collect();
        }
        left
    }

    // A recovery object of the namespace covers the member and ruled on it
    // as `ruling` asks.
    fn ruled(
        &self,
        node: NodeId,
        member: MemberId,
        ruling: impl Fn(RepairOutcome) -> bool,
    ) -> bool {
        self.recovery_nodes(node).into_iter().any(|recovery_node| {
            self.recovery(recovery_node).is_some_and(|recovery| {
                recovery.covers(member) && recovery.outcome().is_some_and(&ruling)
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
    // marked on what it read before is still recorded. A member's damage a
    // recovery object covering it ruled on is that object's to judge: a
    // repair that failed leaves the member short, not the object that
    // wrote it.
    fn damaged(&self) -> bool {
        let (tree, node) = (self.tree, self.node);
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        !tree.vouched_above(node)
            && namespace.members().any(|member| {
                tree.child_of(node, member.id).is_none()
                    && member.coverage.damaged_ranges().next().is_some()
                    && !tree.nodes.retired().contains_key(&(node, member.id))
                    && !tree.given_back(node, member.id)
                    && !tree.ruled(node, member.id, |outcome| outcome != RepairOutcome::Waiting)
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

    fn beside(&self) -> Vec<Beside<'_>> {
        let tree = self.tree;
        let Some((parent, _)) = tree.volumes(self.node) else {
            return Vec::new();
        };
        let Some(namespace) = tree.namespace(parent) else {
            return Vec::new();
        };
        namespace
            .members()
            .filter(|member| tree.unowned(parent, member.id))
            .map(|member| Beside {
                name: &member.name,
                front: namespace.first_bytes(member.id),
                length: member.length,
                carried: tree.given.get(&(parent, member.id)),
            })
            .collect()
    }
}

// What of `range` lies outside `proven`.
fn without(range: ByteRange, proven: ByteRange) -> Vec<ByteRange> {
    if range.intersect(proven).is_none() {
        return vec![range];
    }
    [
        ByteRange::new(range.start, proven.start.max(range.start)),
        ByteRange::new(proven.end.min(range.end), range.end),
    ]
    .into_iter()
    .filter(|piece| !piece.is_empty())
    .collect()
}
