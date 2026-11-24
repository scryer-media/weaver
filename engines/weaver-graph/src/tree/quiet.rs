// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree once the job is quiet: nothing more will arrive and no repair
// will come. Each data object hears the lost ranges of its volumes that
// nothing vouched for, then that the job is quiet, and rules for itself on
// what the tree records of it. The tree tells and reads; it chooses no
// reason. A recovery object heard its losses when they were marked, so it
// hears only that the job is quiet, once its ruling is in; the root came
// out of no object, and is told neither.

use super::{Builder, Effect, NodeId, Tree};
use crate::member::MemberId;
use crate::object::{Beside, Standing};
use crate::recovery::{RepairOutcome, SliceVerdict};
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
        if !self.live_bound(node) {
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

    // The order live nodes hear that the job is quiet, deepest first, in
    // two tiers: first the nodes whose ruling may still change the
    // namespace above them (a data object alone of its kind in a namespace
    // below the root, which a volume may be lifted out of:
    // `Standing::liftable`), then the rest. The second tier is told only on
    // a pass the first changed nothing on, as what rises from below must
    // stand in a namespace before the objects of that namespace rule. One
    // lookup per live node.
    pub fn quiet_order(&self) -> (Vec<NodeId>, Vec<NodeId>) {
        self.live()
            .into_iter()
            .rev()
            .partition(|node| self.told_when_quiet(*node) && self.may_lift(*node))
    }

    // Whether the object at `node` is alone of its kind in a namespace below
    // the root that came out of a container, so a volume of its set may lie
    // one namespace up. Read from the namespace's count of what is bound in
    // it, by the kind each object was bound as.
    fn may_lift(&self, node: NodeId) -> bool {
        let Some((parent, _)) = self.volumes(node) else {
            return false;
        };
        parent != NodeId::ROOT
            && self.volumes(parent).is_some()
            && self
                .nodes
                .get(&parent)
                .is_some_and(|found| found.bound.alone(node))
    }

    // A live data object: bound in a namespace, standing, and no recovery
    // object.
    fn told_when_quiet(&self, node: NodeId) -> bool {
        self.live_bound(node) && !self.recovers(node)
    }

    // A live object: bound in a namespace and standing.
    fn live_bound(&self, node: NodeId) -> bool {
        self.nodes
            .get(&node)
            .is_some_and(|found| !found.retired && found.binding.is_some())
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
    pub fn vouched_above(&self, node: NodeId) -> bool {
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

    // A checking set below the root whose every disagreement is with an
    // archive volume whose object delivered every member whole against the
    // CRC its own headers state: the archive's checks answered the claim,
    // and the set makes none. A set that disagrees with an end file, with a
    // volume of an object that delivered any member short or unchecked, or
    // with nothing it can name keeps its claim.
    pub fn answered_below(&self, node: NodeId, set: NodeId) -> bool {
        if node == NodeId::ROOT {
            return false;
        }
        let (Some(namespace), Some(recovery)) = (self.namespace(node), self.recovery(set)) else {
            return false;
        };
        let mut verdicts = Vec::new();
        let mut disputed = namespace.members().filter(|member| {
            if !recovery.covers(member.id) {
                return false;
            }
            verdicts.clear();
            recovery.verdicts(member.id, &mut verdicts);
            verdicts.contains(&SliceVerdict::Damaged)
        });
        let extracted_whole = |member: MemberId| {
            self.child_of(node, member)
                .and_then(|(child, _)| self.namespace(child))
                .is_some_and(|below| {
                    !below.is_empty()
                        && below
                            .members()
                            .all(|member| member.is_whole() && member.crc32.is_some())
                })
        };
        let mut any = false;
        disputed.all(|member| {
            any = true;
            extracted_whole(member.id)
        }) && any
    }

    // The member's damaged ranges past what every recovery object that
    // found it whole or made it so proves of it: a set vouches only for
    // the bytes its checks reach.
    fn unproven(&self, node: NodeId, member: MemberId) -> Vec<ByteRange> {
        let Some(namespace) = self.namespace(node) else {
            return Vec::new();
        };
        let Some(record) = namespace.get(member) else {
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
                .flat_map(|range| without(range, namespace.proven(member, range, recovery)))
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

impl Stood<'_> {
    // Which members the object rules on. An object that places some of its
    // members itself and runs for the rest rules on its own: those its run
    // makes stand as the run left them. Every member is an engine's own,
    // as every member is any other object's.
    fn own(&self) -> impl Fn(MemberId) -> bool + '_ {
        let engine = self.tree.engine(self.node);
        let places_own = self.tree.places_own(self.node);
        move |member| !places_own || engine.is_none_or(|engine| !engine.makes(member))
    }
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
        let own = self.own();
        !tree.vouched_above(node)
            && namespace.members().any(|member| {
                own(member.id)
                    && tree.child_of(node, member.id).is_none()
                    && member.coverage.damaged_ranges().next().is_some()
                    && !tree.nodes.retired().contains_key(&(node, member.id))
                    && !tree.given_back(node, member.id)
                    && !tree.ruled(node, member.id, |outcome| {
                        !matches!(outcome, RepairOutcome::Waiting | RepairOutcome::Unread)
                    })
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
        let own = self.own();
        namespace.is_empty()
            || namespace.members().any(|member| {
                own(member.id)
                    && !(member.closed
                        || member.is_complete()
                        || tree
                            .child_of(node, member.id)
                            .is_some_and(|(child, _)| tree.recovers(child)))
            })
    }

    fn lost(&self) -> bool {
        !self.tree.lost(self.node).is_empty()
    }

    fn asking(&self) -> bool {
        self.tree.nodes.progress(self.node).asking
    }

    fn refuted(&self) -> bool {
        self.tree.nodes.progress(self.node).refuted
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

    fn liftable(&self) -> bool {
        self.tree.may_lift(self.node)
    }
}

// What of `range` lies outside the part of it `proven`.
fn without(range: ByteRange, proven: Option<ByteRange>) -> Vec<ByteRange> {
    let Some(proven) = proven else {
        return vec![range];
    };
    [
        ByteRange::new(range.start, proven.start.max(range.start)),
        ByteRange::new(proven.end.min(range.end), range.end),
    ]
    .into_iter()
    .filter(|piece| !piece.is_empty())
    .collect()
}
