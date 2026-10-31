// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree out as records and back from them. Rebuilt, every object is at
// the state its facts describe and every namespace holds its members with
// the ids, names and coverage they had, so the owner can deliver again
// whatever it replays from its partials. A record that could not have come
// from a tree is refused, never mended.

use std::collections::{BTreeMap, BTreeSet};

use super::{Binding, Mark, Node, NodeId, Nodes, Tree};
use crate::deliver::Handed;
use crate::facts::{Restore, RestoreError};
use crate::member::{Coverage, Member, MemberId};
use crate::records::{CoverageRecord, MemberRecord, NodeRecord, NodeState, TreeRecord};
use crate::volume::{ByteRange, VolumeIndex};

impl Tree {
    pub fn records(&self) -> TreeRecord {
        TreeRecord {
            // Node ids rise from parent to child, so id order is parents
            // first.
            nodes: self
                .nodes
                .iter()
                .map(|(&node, found)| self.node_record(node, found))
                .collect(),
            next_node: self.next_node,
        }
    }

    // The generation of the tree's nodes: a change to a node marks it under
    // a later one.
    pub fn generation(&self) -> u64 {
        self.nodes.generation()
    }

    // The nodes changed since generation `at`, a node gone since among them.
    pub fn changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.changed_since(at).map(|mark| match mark {
            Mark::Node(node) | Mark::Members(node) => node,
        })
    }

    // What changed since generation `at`: a node's own record, or which of
    // its members its namespace names since the generation it gave last.
    pub fn marked_since(&self, at: u64) -> impl Iterator<Item = Mark> + '_ {
        self.nodes.changed_since(at)
    }

    // One node's record, `None` for a node not in the tree.
    pub fn record(&self, node: NodeId) -> Option<NodeRecord> {
        Some(self.node_record(node, self.nodes.get(&node)?))
    }

    // One node's own record, its members and their coverage left out.
    pub fn own_record(&self, node: NodeId) -> Option<NodeRecord> {
        Some(self.own_node_record(node, self.nodes.get(&node)?))
    }

    // One member's record and its coverage, `None` for a member the node
    // does not have.
    pub fn member_record(
        &self,
        node: NodeId,
        member: MemberId,
    ) -> Option<(MemberRecord, Vec<CoverageRecord>)> {
        let found = self.nodes.get(&node)?;
        let member = found.namespace.get(member)?;
        Some((
            self.member_of(node, found, member),
            coverage_of(member).collect(),
        ))
    }

    pub fn next_node(&self) -> u32 {
        self.next_node
    }

    fn node_record(&self, node: NodeId, found: &Node) -> NodeRecord {
        let namespace = &found.namespace;
        NodeRecord {
            members: namespace
                .records()
                .map(|member| self.member_of(node, found, member))
                .collect(),
            coverage: namespace.records().flat_map(coverage_of).collect(),
            ..self.own_node_record(node, found)
        }
    }

    fn own_node_record(&self, node: NodeId, found: &Node) -> NodeRecord {
        let binding = found.binding.as_ref();
        NodeRecord {
            node,
            parent: binding.map(|binding| binding.parent),
            volumes: binding.map_or_else(Vec::new, |binding| {
                binding.volumes.iter().map(|(&v, &m)| (v, m)).collect()
            }),
            window: binding.and_then(|binding| binding.window),
            kind: self.kind(node),
            state: if found.retired {
                NodeState::Retired
            } else {
                NodeState::Live
            },
            reason: found.refused,
            // A lent object is recorded as it was when it left.
            facts: found
                .object
                .as_ref()
                .map(|object| object.facts())
                .or_else(|| self.lent_facts(node).cloned()),
            members: Vec::new(),
            coverage: Vec::new(),
            // Envelope bytes and cipher seeds live in the objects' facts.
            envelopes: Vec::new(),
            crypt: Vec::new(),
        }
    }

    fn member_of(&self, node: NodeId, found: &Node, member: &Member) -> MemberRecord {
        let retired = self.nodes.retired().get(&(node, member.id));
        MemberRecord {
            member: member.id,
            name: member.name.clone(),
            output_name: member.output_name.clone(),
            length: member.length,
            closed: member.closed,
            crc32: member.crc32,
            bound_to: member.bound_to,
            owner: found.children.get(&member.id).copied(),
            given_up: retired.copied().flatten(),
            buried: retired.is_some_and(Option::is_none),
            // A member given back with its parse waits to be offered again,
            // and the parse lives only in memory: a restore offers it again
            // from its front, as it does a member whose owner was never
            // decided.
            undecided: found.undecided.contains(&member.id)
                || self.given.contains_key(&(node, member.id)),
            copy_of: found.namespace.copy_of(member.id),
            fronted: member.fronted,
            refuted: member.refuted,
        }
    }

    pub fn rebuild(record: &TreeRecord, restore: &dyn Restore) -> Result<Tree, RestoreError> {
        let mut tree = Tree::new();
        tree.nodes = Nodes::default();
        for node in &record.nodes {
            tree.place_record(node, restore)?;
        }
        for node in &record.nodes {
            tree.link_record(node)?;
        }
        let refuse = |reason| RestoreError::Tree {
            node: NodeId::ROOT,
            reason,
        };
        if !tree.nodes.contains_key(&NodeId::ROOT) {
            return Err(refuse("the record has no root"));
        }
        if tree.nodes.keys().any(|node| node.0 >= record.next_node) {
            return Err(refuse("a node id is not below the next id"));
        }
        tree.next_node = record.next_node;
        tree.rebuild_names();
        Ok(tree)
    }

    // One node, its object and its namespace. Its parent is placed already.
    fn place_record(
        &mut self,
        record: &NodeRecord,
        restore: &dyn Restore,
    ) -> Result<(), RestoreError> {
        let refuse = |reason| RestoreError::Tree {
            node: record.node,
            reason,
        };
        if self.nodes.contains_key(&record.node) {
            return Err(refuse("appears twice"));
        }
        let binding = self.binding_of(record).map_err(refuse)?;
        let object = match (record.kind, &record.facts) {
            (None, None) if binding.is_none() => None,
            (Some(kind), Some(facts)) if binding.is_some() => {
                let object = restore.restore(facts)?;
                if object.kind() != kind {
                    return Err(refuse("its facts restore another kind"));
                }
                Some(object)
            }
            _ => {
                return Err(refuse(
                    "an object without a parent, or a parent without one",
                ));
            }
        };
        // A window is listed beside its member's owner, in the order the
        // windows were opened, which is id order.
        if record.window.is_some()
            && let Some(binding) = &binding
            && let Some(member) = binding.volumes.get(&VolumeIndex(0))
            && let Some(parent) = self.nodes.get_mut(&binding.parent)
        {
            parent.windows.entry(*member).or_default().push(record.node);
        }
        let mut node = Node::new(object, binding);
        node.retired = record.state == NodeState::Retired;
        if record.reason.is_some() && !node.retired {
            return Err(refuse("a live node has refused its volumes"));
        }
        node.refused = record.reason;
        // A refused object hears it again, as it heard it when it was.
        if node.refused.is_some()
            && let Some(object) = node.object.as_mut()
        {
            object.refused();
        }
        for member in &record.members {
            if node.namespace.get(member.member).is_some() {
                return Err(refuse("a member appears twice"));
            }
            if member.undecided && member.owner.is_some() {
                return Err(refuse("an undecided member has an owner"));
            }
            node.namespace.restore(Member {
                id: member.member,
                name: member.name.clone(),
                output_name: member.output_name.clone(),
                length: member.length,
                bound_to: member.bound_to,
                coverage: Coverage::new(),
                closed: member.closed,
                crc32: member.crc32,
                fronted: member.fronted,
                refuted: member.refuted,
            });
            if member.undecided {
                // Its front comes again with the replay.
                node.undecide(member.member);
            }
        }
        for member in &record.members {
            let Some(into) = member.copy_of else {
                continue;
            };
            if member.undecided || member.owner.is_some() || member.bound_to.is_some() {
                return Err(refuse("a further posting has an owner of its own"));
            }
            if !node.namespace.restore_copy(member.member, into) {
                return Err(refuse("a further posting of a member it does not have"));
            }
        }
        for run in &record.coverage {
            if run.start >= run.end {
                return Err(refuse("a coverage run is empty or inverted"));
            }
            let range = ByteRange::new(run.start, run.end);
            let member = node
                .namespace
                .get_mut(run.member)
                .ok_or(refuse("coverage of a member it does not have"))?;
            if member.coverage.overlapping(range).next().is_some() {
                return Err(refuse("coverage runs overlap"));
            }
            member.coverage.write(range, run.provenance);
            // Clean bytes at its front are a front that came, for a record
            // that keeps no word of it.
            if run.start == 0 && !run.provenance.is_damaged() {
                member.fronted = true;
            }
        }
        self.nodes.insert(record.node, node);
        self.rehome(record.node);
        Ok(())
    }

    fn binding_of(&self, record: &NodeRecord) -> Result<Option<Binding>, &'static str> {
        let Some(parent) = record.parent else {
            let root = record.node == NodeId::ROOT && record.volumes.is_empty();
            return match root && record.window.is_none() {
                true => Ok(None),
                false => Err("only the root has no parent, and it has no volumes"),
            };
        };
        if record.node == NodeId::ROOT {
            return Err("the root has a parent");
        }
        let namespace = &self
            .nodes
            .get(&parent)
            .ok_or("comes before its parent")?
            .namespace;
        let volumes: BTreeMap<_, _> = record.volumes.iter().copied().collect();
        if volumes.is_empty() || volumes.len() != record.volumes.len() {
            return Err("its volumes are none or repeat");
        }
        if volumes
            .values()
            .any(|member| namespace.get(*member).is_none())
        {
            return Err("a volume is a member its parent does not have");
        }
        if record.window.is_some() && volumes.keys().ne([VolumeIndex(0)].iter()) {
            return Err("a window has more than its one volume");
        }
        Ok(Some(Binding {
            parent,
            volumes,
            window: record.window,
            // A checkpoint is taken between passes, when no hand-over is owed.
            awaiting: BTreeSet::new(),
            // What was handed is never kept: the owner, rebuilt beside it,
            // hands it again as it opens.
            handed: Handed::default(),
        }))
    }

    // Which members are which child's volumes, and which a retired object
    // gave up, once every node is placed.
    fn link_record(&mut self, record: &NodeRecord) -> Result<(), RestoreError> {
        let refuse = |reason| RestoreError::Tree {
            node: record.node,
            reason,
        };
        for member in &record.members {
            if let Some((child, volume)) = member.owner {
                let binds = self
                    .nodes
                    .get(&child)
                    .and_then(|found| found.binding.as_ref())
                    .is_some_and(|binding| {
                        binding.parent == record.node
                            && binding.window.is_none()
                            && binding.volumes.get(&volume) == Some(&member.member)
                    });
                if !binds {
                    return Err(refuse("a member's owner does not bind it"));
                }
                if let Some(found) = self.nodes.get_mut(&record.node) {
                    found.own(member.member, (child, volume));
                }
            }
            if member.buried && member.given_up.is_some() {
                return Err(refuse("a member is both given up and buried"));
            }
            if let Some((node, volume)) = member.given_up {
                if !self.nodes.contains_key(&node) {
                    return Err(refuse("a member was given up by a node not in the record"));
                }
                self.nodes
                    .retire((record.node, member.member), Some((node, volume)));
            }
            if member.buried {
                self.nodes.retire((record.node, member.member), None);
            }
        }
        Ok(())
    }
}

fn coverage_of(member: &Member) -> impl Iterator<Item = CoverageRecord> + '_ {
    member.coverage.runs().map(|run| CoverageRecord {
        member: member.id,
        start: run.range.start,
        end: run.range.end,
        provenance: run.provenance,
    })
}

#[cfg(test)]
pub(crate) mod tests;
