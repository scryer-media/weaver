// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's recovery objects, at every depth, by one rule. Damage the source
// reports is marked down the tree to every member it lands in, and a
// recovery object whose own volume it reached is told. One walk over the
// namespaces, parents before children, lets each recovery object that has
// something to rule on rule, so a repair above is in place before anything
// below rules on bytes it rewrote. What a repair rewrote goes down the one
// write path as repaired bytes, to a leaf or to the object that owns the
// member. Once nothing more can change, a namespace left with damage no
// recovery object there vouched for is refused by the object it came out
// of, with that reason.

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Effect, Emit, MemberId, NodeId, Why};

use super::{GraphError, GraphJob};

impl GraphJob {
    // Every recovery object in the tree, with the namespace it lives in,
    // parents first.
    pub(super) fn recovery_objects(&self) -> Vec<(NodeId, NodeId)> {
        let tree = &self.tree;
        tree.live()
            .into_iter()
            .flat_map(|namespace| {
                tree.recovery_nodes(namespace)
                    .into_iter()
                    .map(move |node| (namespace, node))
            })
            .collect()
    }

    // Walk the namespaces in node order, which puts every parent before its
    // children, and let each one's recovery objects repair when one is due.
    // A namespace the walk's own repairs bring into being is walked too.
    pub(super) fn repair(&mut self) -> Result<(), GraphError> {
        let mut walked = None;
        while let Some(node) = self
            .tree
            .live()
            .into_iter()
            .find(|node| walked.is_none_or(|last| *node > last))
        {
            walked = Some(node);
            self.announce_absent(node)?;
            if !self.due(node) {
                continue;
            }
            let mut out = Vec::new();
            let outcomes = self.writer.repair(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                node,
                &mut out,
            )?;
            for (recovery, _) in outcomes {
                let closed = self.closed_volumes(recovery);
                self.ruled_at.insert(recovery, closed);
            }
            self.heard(out)?;
        }
        Ok(())
    }

    // A recovery object that has not ruled rules once every member it
    // covers is closed; one waiting on recovery bytes rules again once more
    // of its volumes are closed than when it last ruled.
    fn due(&self, node: NodeId) -> bool {
        let tree = &self.tree;
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        tree.recovery_nodes(node).into_iter().any(|recovery_node| {
            let Some(recovery) = tree.recovery(recovery_node) else {
                return false;
            };
            match recovery.outcome() {
                None => namespace
                    .members()
                    .all(|member| !recovery.covers(member.id) || member.closed),
                Some(RepairOutcome::Waiting) => {
                    let ruled = self.ruled_at.get(&recovery_node).copied().unwrap_or(0);
                    self.closed_volumes(recovery_node) > ruled
                }
                Some(_) => false,
            }
        })
    }

    fn closed_volumes(&self, recovery: NodeId) -> usize {
        self.tree.volumes(recovery).map_or(0, |(parent, volumes)| {
            volumes
                .values()
                .filter(|member| self.closed_in(parent, **member))
                .count()
        })
    }

    // Everything the namespace will get is in: the members that are its
    // object's volumes are closed, no run of it is still reading, and every
    // member but a recovery volume is closed.
    fn namespace_in(&self, node: NodeId) -> bool {
        let tree = &self.tree;
        let volumes_in = tree.volumes(node).is_none_or(|(parent, volumes)| {
            volumes
                .values()
                .all(|member| self.closed_in(parent, *member))
        });
        let members_in = tree.namespace(node).is_some_and(|namespace| {
            namespace.members().all(|member| {
                member.closed
                    || tree
                        .child_of(node, member.id)
                        .is_some_and(|(child, _)| tree.recovers(child))
            })
        });
        volumes_in && members_in && !self.runs.has(node)
    }

    // A file a recovery object of the namespace describes and no member
    // carries is announced once the namespace has every member it will get,
    // and closed at once, since nothing will arrive for it. Its recovery
    // object binds it by name, finds every slice of it missing, and the
    // repair writes it whole.
    fn announce_absent(&mut self, node: NodeId) -> Result<(), GraphError> {
        if !self.namespace_in(node) {
            return Ok(());
        }
        let mut absent = Vec::new();
        for recovery_node in self.tree.recovery_nodes(node) {
            if let Some(recovery) = self.tree.recovery(recovery_node) {
                recovery.absent(&mut absent);
            }
        }
        // A name some member already has is that member's, bound or not.
        if let Some(namespace) = self.tree.namespace(node) {
            absent.retain(|(name, _)| namespace.members().all(|member| member.name != *name));
        }
        for (name, length) in absent {
            let mut effects = Vec::new();
            let Some(member) = self
                .tree
                .announce_rebuilt(node, &name, length, &mut effects)
            else {
                continue;
            };
            self.settle(effects)?;
            let mut out = Vec::new();
            self.writer.close(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                node,
                member,
                &mut out,
            )?;
            self.heard(out)?;
        }
        Ok(())
    }

    // Once nothing more can change, every namespace with a damaged leaf no
    // recovery object there vouched for is refused by its object: the
    // members that were its volumes are damaged leaves of the namespace it
    // was bound in. A leaf that is itself a refused member refuses nothing
    // more, so a refusal stops where the damage first had no cover. The
    // root came out of no object, and keeps its damaged leaves. Asked only
    // once every run is started, since a repair's bytes reach a namespace
    // below by its run reading them again. Returns whether it refused.
    pub(super) fn refuse_unrepaired(&mut self) -> Result<bool, GraphError> {
        if !self.quiet() {
            return Ok(false);
        }
        let refusing: Vec<NodeId> = self
            .tree
            .live()
            .into_iter()
            .filter(|node| self.tree.volumes(*node).is_some() && self.unrepaired(*node))
            .collect();
        let mut effects = Vec::new();
        for node in refusing {
            self.tree.emitted(
                node,
                vec![Emit::Refuse(Why::Unrepairable)],
                &mut *self.builder,
                &mut effects,
            );
        }
        let refused = !effects.is_empty();
        self.settle(effects)?;
        Ok(refused)
    }

    // The payload is in, no run is reading, and every recovery object in the
    // tree has ruled on something other than waiting.
    fn quiet(&self) -> bool {
        !self.payload_waiting()
            && !self.runs.live()
            && self.recovery_objects().into_iter().all(|(_, node)| {
                self.tree
                    .recovery(node)
                    .and_then(|recovery| recovery.outcome())
                    .is_some_and(|outcome| outcome != RepairOutcome::Waiting)
            })
    }

    // A damaged leaf of the namespace that nothing vouched for: no recovery
    // object covering it, nor one covering a member the namespace came out
    // of at any depth above. A repair above rebuilt the bytes below, though
    // the damage an object marked on what it read before is still recorded.
    fn unrepaired(&self, node: NodeId) -> bool {
        let tree = &self.tree;
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        if self.vouched_above(node) {
            return false;
        }
        namespace.members().any(|member| {
            tree.child_of(node, member.id).is_none()
                && member.coverage.damaged_ranges().next().is_some()
                && self.writer.refused(node, member.id).is_none()
                && !self.vouched(node, member.id)
        })
    }

    // Some member the namespace came out of, at any depth, is vouched for.
    fn vouched_above(&self, node: NodeId) -> bool {
        let mut at = node;
        while let Some((parent, volumes)) = self.tree.volumes(at) {
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
        let tree = &self.tree;
        tree.recovery_nodes(node).into_iter().any(|recovery_node| {
            tree.recovery(recovery_node).is_some_and(|recovery| {
                recovery.covers(member)
                    && matches!(
                        recovery.outcome(),
                        Some(RepairOutcome::Clean | RepairOutcome::Repaired { .. })
                    )
            })
        })
    }

    // Ranges of lost articles become damage in the tree as soon as they can
    // be told, on every member they land in at every depth; whatever covers
    // them repairs them.
    pub(super) fn mark_damage(&mut self) -> Result<(), GraphError> {
        let mut effects = Vec::new();
        for (volume, range) in self.root.damage() {
            let from = effects.len();
            self.tree.damage(NodeId::ROOT, volume, range, &mut effects);
            let marked: Vec<(NodeId, MemberId, ByteRange)> = effects[from..]
                .iter()
                .filter_map(|effect| match effect {
                    Effect::Damaged {
                        node,
                        member,
                        range,
                    } => Some((*node, *member, *range)),
                    _ => None,
                })
                .collect();
            for (node, member, range) in marked {
                self.recovery_volume_lost(node, member, range, &mut effects);
            }
        }
        self.settle(effects)
    }

    // A lost range of a recovery object's own volume is gone for good, since
    // nothing recovers the recovery. The object hears it, or it waits on the
    // gap: a walk that never passes it finds none of the blocks beyond.
    // A data object is not told, as a repair may still bring its bytes back.
    fn recovery_volume_lost(
        &mut self,
        node: NodeId,
        member: MemberId,
        range: ByteRange,
        effects: &mut Vec<Effect>,
    ) {
        let Some((child, volume)) = self.tree.child_of(node, member) else {
            return;
        };
        if !self.tree.recovers(child) {
            return;
        }
        let Some(mut object) = self.tree.take_object(child) else {
            return;
        };
        let mut emits = Vec::new();
        object.volume_lost(volume, range, &mut emits);
        self.tree.put_object(child, object);
        self.tree.emitted(child, emits, &mut *self.builder, effects);
    }
}
