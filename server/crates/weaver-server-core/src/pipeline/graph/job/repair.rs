// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's recovery objects, at every depth, by one rule. Damage the source
// reports is marked down the tree to every member it lands in, and a
// recovery object whose own volume it reached is told. One walk over the
// namespaces, parents before children, lets each recovery object that has
// something to rule on rule, so a repair above is in place before anything
// below rules on bytes it rewrote. What a repair rewrote goes down the one
// write path as repaired bytes, to a leaf or to the object that owns the
// member. Once nothing more can change, every data object, deepest first,
// hears the lost ranges nothing vouched for and that the job is quiet, and
// an object that cannot finish refuses itself for its own reason. The
// server tells; it chooses no reason.

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Effect, Emit, MemberId, NodeId, Object};

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
            let due = self.due(node);
            if due.is_empty() {
                continue;
            }
            let mut out = Vec::new();
            let outcomes = self.writer.repair(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                node,
                &due,
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

    // The namespace's recovery objects due to rule, each judged alone. A
    // recovery object that has not ruled rules once every member it
    // covers is closed and the namespace has announced every member it
    // will get, since a volume of its own may still be on its way behind
    // them; one waiting on recovery bytes rules again once more of its
    // volumes are closed than when it last ruled. It learns what it covers
    // from its own bytes, so one that covers nothing yet rules only once a
    // volume of its own is in: a verdict on nothing would read as settled
    // to every member beside it, and a member it binds after would never
    // be repaired before an engine reads it.
    fn due(&self, node: NodeId) -> Vec<NodeId> {
        let tree = &self.tree;
        let Some(namespace) = tree.namespace(node) else {
            return Vec::new();
        };
        let mut due = tree.recovery_nodes(node);
        due.retain(|&recovery_node| {
            let Some(recovery) = tree.recovery(recovery_node) else {
                return false;
            };
            match recovery.outcome() {
                None => {
                    let covers = namespace.members().any(|member| recovery.covers(member.id));
                    self.members_known(node)
                        && (covers || self.closed_volumes(recovery_node) > 0)
                        && namespace
                            .members()
                            .all(|member| !recovery.covers(member.id) || member.closed)
                }
                Some(RepairOutcome::Waiting) => {
                    let ruled = self.ruled_at.get(&recovery_node).copied().unwrap_or(0);
                    self.closed_volumes(recovery_node) > ruled
                }
                Some(_) => false,
            }
        });
        due
    }

    fn closed_volumes(&self, recovery: NodeId) -> usize {
        self.tree.volumes(recovery).map_or(0, |(parent, volumes)| {
            volumes
                .values()
                .filter(|member| self.closed_in(parent, **member))
                .count()
        })
    }

    // The namespace has announced every member it will get: the members
    // that are its object's volumes are closed and no run of it is still
    // reading. The root's members are the post's, known from the start.
    fn members_known(&self, node: NodeId) -> bool {
        let volumes_in = self.tree.volumes(node).is_none_or(|(parent, volumes)| {
            volumes
                .values()
                .all(|member| self.closed_in(parent, *member))
        });
        volumes_in && !self.runs.reading(node)
    }

    // Everything the namespace will get is in: every member is announced,
    // and every member but a recovery volume is closed or holds every byte
    // it will ever hold. A range marked lost is held as damage, so a member
    // open only for bytes lost above it is in: nothing left it unplaced.
    fn namespace_in(&self, node: NodeId) -> bool {
        let tree = &self.tree;
        let Some(namespace) = tree.namespace(node) else {
            return false;
        };
        let members_in = namespace.members().all(|member| {
            member.closed
                || member.is_complete()
                || tree
                    .child_of(node, member.id)
                    .is_some_and(|(child, _)| tree.recovers(child))
        });
        self.members_known(node) && members_in
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

    // Once nothing more can change, every data object hears the lost
    // ranges of its volumes that nothing vouched for, then that the job is
    // quiet, and rules for itself: the tree tells it and reads it what the
    // tree records of it, and the server applies what it says. Children
    // come first, so the deepest object the damage reached rules before any
    // object above it, and a parent never speaks for bytes lost below it.
    // What the losses set going is settled before the object hears quiet.
    // Asked only once every run is started, since a repair's bytes reach a
    // namespace below by its run reading them again. Returns whether
    // anything changed.
    pub(super) fn tell_quiet(&mut self) -> Result<bool, GraphError> {
        if !self.quiet() {
            return Ok(false);
        }
        let mut changed = false;
        for node in self.tree.live().into_iter().rev() {
            let mut effects = Vec::new();
            self.tree.tell_lost(
                node,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            changed |= !effects.is_empty();
            self.settle(effects)?;
            let mut effects = Vec::new();
            self.tree.quiet(
                node,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            changed |= !effects.is_empty();
            self.settle(effects)?;
        }
        Ok(changed)
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
    // A data object hears its losses only once the job is quiet, as a
    // repair may still bring its bytes back.
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
        if self.tree.recovers(child) {
            self.hear(child, effects, |object, out| {
                object.volume_lost(volume, range, out);
            });
        }
    }

    // The node's object is told what will never arrive, and what it says
    // is applied.
    fn hear(
        &mut self,
        node: NodeId,
        effects: &mut Vec<Effect>,
        tell: impl FnOnce(&mut dyn Object, &mut Vec<Emit>),
    ) {
        let Some(mut object) = self.tree.take_object(node) else {
            return;
        };
        let mut emits = Vec::new();
        tell(object.as_mut(), &mut emits);
        self.tree.put_object(node, object);
        self.tree.emitted(
            node,
            emits,
            &mut self.writer.placing(&mut *self.builder),
            effects,
        );
    }
}
