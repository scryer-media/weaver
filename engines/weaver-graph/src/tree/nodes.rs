// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree's nodes, and the members each gave up, under one generation:
// every way to change a node, or what it gave up, marks the node, so a
// snapshot asks again only the nodes marked since the generation it took
// last. Nothing outside reaches a node to change it but through here.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use super::{Node, NodeId};
use crate::deliver::Why;
use crate::engine::Engine;
use crate::marks::Marked;
use crate::member::MemberId;
use crate::volume::VolumeIndex;

// What a member given up was to the retired object that gave it up: its
// node, and which of its volumes; `None` once the owner buried it.
pub(super) type GivenUp = Option<(NodeId, VolumeIndex)>;

#[derive(Default)]
pub(super) struct Nodes {
    nodes: Marked<NodeId, Node>,
    // Members given up by a retired object, and what they were to it, until
    // the owner buries them. The entry outlives the burial, emptied,
    // because a member an object gave up is settled: no object bound later
    // is offered it.
    retired: BTreeMap<(NodeId, MemberId), GivenUp>,
    // The refused nodes, each under the node it is bound in. Filed where a
    // node comes, goes, or is refused, the only paths that change it.
    refused: BTreeSet<(NodeId, NodeId)>,
}

impl Deref for Nodes {
    type Target = BTreeMap<NodeId, Node>;

    fn deref(&self) -> &Self::Target {
        &self.nodes
    }
}

impl Nodes {
    pub(super) fn get_mut(&mut self, node: &NodeId) -> Option<&mut Node> {
        self.nodes.get_mut(node)
    }

    // The engine face of a node's object, to tell it how its run goes: how
    // far it read, and that it asked for a password. An object keeps no
    // run's progress in its facts, so this marks nothing a snapshot reads.
    pub(super) fn run_progress(&mut self, node: &NodeId) -> Option<&mut dyn Engine> {
        self.nodes
            .get_mut_unread(node)?
            .object
            .as_mut()?
            .engine_mut()
    }

    pub(super) fn insert(&mut self, node: NodeId, found: Node) {
        if let (Some(_), Some(binding)) = (found.refused, &found.binding) {
            self.refused.insert((binding.parent, node));
        }
        self.nodes.insert(node, found);
    }

    // The object at `node` refused its volumes, for `why`.
    pub(super) fn refuse(&mut self, node: &NodeId, why: Why) -> Option<&mut Node> {
        let found = self.nodes.get_mut(node)?;
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
        self.nodes.mark(at.0);
        self.retired.insert(at, given);
    }

    pub(super) fn unretire(&mut self, at: &(NodeId, MemberId)) {
        if self.retired.remove(at).is_some() {
            self.nodes.mark(at.0);
        }
    }

    pub(super) fn retired_mut(&mut self, at: &(NodeId, MemberId)) -> Option<&mut GivenUp> {
        let found = self.retired.get_mut(at)?;
        self.nodes.mark(at.0);
        Some(found)
    }

    pub(super) fn generation(&self) -> u64 {
        self.nodes.marks().at()
    }

    pub(super) fn changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.nodes.marks().since(at)
    }
}
