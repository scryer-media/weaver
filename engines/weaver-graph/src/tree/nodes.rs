// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tree's nodes, and the members each gave up, under one generation:
// every way to change a node, or what it gave up, marks the node, so a
// snapshot asks again only the nodes marked since the generation it took
// last. Nothing outside reaches a node to change it but through here.

use std::collections::BTreeMap;
use std::ops::Deref;

use super::{Node, NodeId};
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

    pub(super) fn insert(&mut self, node: NodeId, found: Node) {
        self.nodes.insert(node, found);
    }

    // A node goes, and what it gave up with it.
    pub(super) fn remove(&mut self, node: &NodeId) -> Option<Node> {
        let removed = self.nodes.remove(node)?;
        self.retired.retain(|(at, _), _| at != node);
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
