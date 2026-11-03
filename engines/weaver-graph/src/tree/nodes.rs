// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree's nodes, and the members each gave up, under one generation:
// every way to change a node marks it, so a snapshot asks again only what
// was marked since the generation it took last. Nothing outside reaches a
// node to change it but through here. A mark is as fine as the rows a
// snapshot keeps: a node's own row (its object, binding and state) is marked
// apart from its members, and of its members only which namespace moved is
// marked here; the namespace says which of its members did, and a member's
// owner, whether it waits on one, and what it was given up as are marked
// there too.
// Beside them, unmarked, how each node's run goes: a run's progress is the
// tree's, read live and kept in no record, so a snapshot never asks for it
// and a rebuilt tree hears it again from the runs it starts.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use super::{MembersMut, Node, NodeId};
use crate::deliver::Why;
use crate::marks::Marks;
use crate::member::{MemberId, Namespace};
use crate::object::Object;
use crate::volume::VolumeIndex;

// What a change touched of a node: its own row, or its members, which its
// namespace names one by one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Mark {
    Node(NodeId),
    Members(NodeId),
}

// What a member given up was to the retired object that gave it up: its
// node, and which of its volumes; `None` once the owner buried it.
pub(super) type GivenUp = Option<(NodeId, VolumeIndex)>;

// How far a node's run has gone: the furthest volume it reached, and
// whether it asked for a password that no offer, and no run started since,
// has answered.
#[derive(Clone, Copy, Default)]
pub(super) struct Progress {
    pub(super) reached: Option<VolumeIndex>,
    pub(super) asking: bool,
}

#[derive(Default)]
pub(super) struct Nodes {
    nodes: BTreeMap<NodeId, Node>,
    marks: Marks<Mark>,
    // Members given up by a retired object, and what they were to it, until
    // the owner buries them. The entry outlives the burial, emptied,
    // because a member an object gave up is settled: no object bound later
    // is offered it.
    retired: BTreeMap<(NodeId, MemberId), GivenUp>,
    // The refused nodes, each under the node it is bound in. Filed where a
    // node comes, goes, or is refused, the only paths that change it.
    refused: BTreeSet<(NodeId, NodeId)>,
    // Each node's run progress, from its first report until the node goes.
    progress: BTreeMap<NodeId, Progress>,
}

impl Deref for Nodes {
    type Target = BTreeMap<NodeId, Node>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl Nodes {
    // The whole node, to change: its own row is marked, and its namespace
    // is asked again for what it marked.
    pub(super) fn get_mut(&mut self, node: &NodeId) -> Option<&mut Node> {
        let found = self.nodes.get_mut(node)?;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        Some(found)
    }
    // Only the node's namespace, to change: its own row is not marked, and
    // the namespace marks each member it changes.
    pub(super) fn namespace_mut(&mut self, node: NodeId) -> Option<&mut Namespace> {
        let found = self.nodes.get_mut(&node)?;
        self.marks.mark(Mark::Members(node));
        Some(&mut found.namespace)
    }

    // Only the node's members, to change: its namespace and what it keeps
    // of each member beside it. Its own row is not marked, and each member
    // changed marks itself.
    pub(super) fn members_mut(&mut self, node: NodeId) -> Option<MembersMut<'_>> {
        let found = self.nodes.get_mut(&node)?;
        self.marks.mark(Mark::Members(node));
        Some(found.members_mut())
    }

    // Only the node's namespace, to change: each member changed marks
    // itself there, and the node is marked for its members only when one
    // did.
    pub(super) fn with_namespace<R>(
        &mut self,
        node: NodeId,
        change: impl FnOnce(&mut Namespace) -> R,
    ) -> Option<R> {
        let found = self.nodes.get_mut(&node)?;
        let at = found.namespace.generation();
        let result = change(&mut found.namespace);
        if found.namespace.generation() != at {
            self.marks.mark(Mark::Members(node));
        }
        Some(result)
    }

    // The node's object steps out to run beside the rest of the tree, and
    // back in: the move changes nothing of the node, so it marks nothing.
    pub(super) fn object_out(&mut self, node: NodeId) -> Option<Box<dyn Object>> {
        self.nodes.get_mut(&node)?.object.take()
    }

    pub(super) fn object_in(&mut self, node: NodeId, object: Box<dyn Object>) {
        if let Some(found) = self.nodes.get_mut(&node) {
            found.object = Some(object);
        }
    }

    // The node's own row changed: what its object knows.
    pub(super) fn mark_own(&mut self, node: NodeId) {
        if self.nodes.contains_key(&node) {
            self.marks.mark(Mark::Node(node));
        }
    }

    // A member's rows changed by what the tree keeps of it beside its
    // namespace.
    pub(super) fn mark_member(&mut self, node: NodeId, member: MemberId) {
        if let Some(namespace) = self.namespace_mut(node) {
            namespace.mark(member);
        }
    }

    pub(super) fn progress(&self, node: NodeId) -> Progress {
        self.progress.get(&node).copied().unwrap_or_default()
    }

    // The progress of a node's run, to change: it marks nothing.
    pub(super) fn progress_mut(&mut self, node: NodeId) -> Option<&mut Progress> {
        self.nodes
            .contains_key(&node)
            .then(|| self.progress.entry(node).or_default())
    }

    pub(super) fn insert(&mut self, node: NodeId, found: Node) {
        self.progress.remove(&node);
        if let (Some(_), Some(binding)) = (found.refused, &found.binding) {
            self.refused.insert((binding.parent, node));
        }
        self.marks.mark(Mark::Node(node));
        self.marks.mark(Mark::Members(node));
        self.nodes.insert(node, found);
    }

    // The object at `node` refused its volumes, for `why`.
    pub(super) fn refuse(&mut self, node: &NodeId, why: Why) -> Option<&mut Node> {
        let found = self.nodes.get_mut(node)?;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        found.refused = Some(why);
        if let Some(binding) = &found.binding {
            self.refused.insert((binding.parent, *node));
        }
        Some(found)
    }

    // The refused nodes bound in `parent`'s namespace, in node order.
    pub(super) fn refused_below(&self, parent: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        self.refused
            .range((parent, NodeId(0))..=(parent, NodeId(u32::MAX)))
            .map(|(_, node)| *node)
    }

    // A node goes, and what it gave up with it.
    pub(super) fn remove(&mut self, node: &NodeId) -> Option<Node> {
        let removed = self.nodes.remove(node)?;
        self.marks.mark(Mark::Node(*node));
        self.marks.mark(Mark::Members(*node));
        self.progress.remove(node);
        self.retired.retain(|(at, _), _| at != node);
        if let Some(binding) = &removed.binding {
            self.refused.remove(&(binding.parent, *node));
        }
        Some(removed)
    }

    pub(super) fn retired(&self) -> &BTreeMap<(NodeId, MemberId), GivenUp> {
        &self.retired
    }

    pub(super) fn retire(&mut self, at: (NodeId, MemberId), given: GivenUp) {
        self.mark_member(at.0, at.1);
        self.retired.insert(at, given);
    }

    pub(super) fn unretire(&mut self, at: &(NodeId, MemberId)) {
        if self.retired.remove(at).is_some() {
            self.mark_member(at.0, at.1);
        }
    }

    pub(super) fn retired_mut(&mut self, at: &(NodeId, MemberId)) -> Option<&mut GivenUp> {
        if !self.retired.contains_key(at) {
            return None;
        }
        self.mark_member(at.0, at.1);
        self.retired.get_mut(at)
    }

    pub(super) fn generation(&self) -> u64 {
        self.marks.at()
    }

    pub(super) fn changed_since(&self, at: u64) -> impl Iterator<Item = Mark> + '_ {
        self.marks.since(at)
    }
}
