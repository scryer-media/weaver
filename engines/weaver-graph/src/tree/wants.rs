// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Each node's standing wants, kept from the round that read them. A round
// reads again only the nodes the change log names since the generation the
// kept wants were read at: a node marked for its own row or its members, a
// child or window of such a node, one whose run progress was handed out to
// change, and one that came to owe a rewalk or stopped owing one. Nothing
// else a node's wants read changes but on those paths. The first round of a
// tree, a rebuilt one included, reads every node.
//
// Under the tests every round also reads every node it did not read again,
// and stops on any that its kept wants do not match: each node is still
// read once a round, as a walk of every node reads it.

use std::collections::{BTreeMap, BTreeSet};

use super::{Mark, NodeId, Tree};
use crate::seek::Seek;

#[derive(Default)]
pub(super) struct Kept {
    // The generation the kept wants were read at; `None` before the first
    // round.
    at: Option<u64>,
    // Each node that wants anything, in node order, with what it wants in
    // the order it said it. At most one entry per node, each holding what
    // one walk of the node gives.
    by_node: BTreeMap<NodeId, Vec<Seek>>,
    // The nodes whose rewalk was asked, taken or ended since the last round.
    // Taken each round.
    rewalk_moved: BTreeSet<NodeId>,
    // What each node's run said beside the volume it reached as the last
    // round read it, to name the path in a mismatch.
    #[cfg(any(test, feature = "test-support"))]
    said: BTreeMap<NodeId, (bool, bool)>,
    // How many nodes the last round read again.
    #[cfg(any(test, feature = "test-support"))]
    pub(super) read: usize,
}

impl Kept {
    // The node's rewalk was asked, taken or ended: whether it owes one may
    // have changed.
    pub(super) fn rewalk_moved(&mut self, node: NodeId) {
        self.rewalk_moved.insert(node);
    }

    pub(super) fn by_node(&self) -> &BTreeMap<NodeId, Vec<Seek>> {
        &self.by_node
    }
}

impl Tree {
    // The nodes whose wants may have changed since the kept wants were
    // read, found from the change log and the two rings alone; `None` when
    // every node is to be read: before the first round, or for a generation
    // below the one read.
    fn wants_due(&mut self) -> Option<BTreeSet<NodeId>> {
        let progressed = self.nodes.take_progressed();
        let rewalked = std::mem::take(&mut self.wants.rewalk_moved);
        let at = self.wants.at.filter(|at| *at <= self.nodes.generation())?;
        let mut due = BTreeSet::new();
        for mark in self.nodes.changed_since(at) {
            let (Mark::Node(node) | Mark::Members(node)) = mark;
            due.insert(node);
            if let Some(found) = self.nodes.get(&node) {
                due.extend(found.children.values().map(|(child, _)| *child));
                due.extend(found.windows.values().flatten().copied());
            }
        }
        due.extend(progressed);
        due.extend(rewalked);
        Some(due)
    }

    // What the node wants now, as a walk of every node reads it: nothing for
    // a node gone, retired, owing a rewalk or without its object. A
    // restored recovery object still to hear what arrived of its volumes
    // again knows too little to ask for anything: it asks once it has heard
    // them, and the job fetches nothing meanwhile.
    fn node_wants(&self, id: NodeId, seeks: &mut Vec<Seek>) {
        seeks.clear();
        let Some(node) = self.nodes.get(&id) else {
            return;
        };
        if node.retired || self.rewalk_owed(id) {
            return;
        }
        let Some(object) = node.object.as_ref() else {
            return;
        };
        object.wants(seeks);
        if let (Some(engine), Some(reached)) = (object.engine(), self.nodes.progress(id).reached) {
            engine.bid(reached, seeks);
        }
    }

    // Brings the kept wants up to the tree: each node due is read again, and
    // a node gone, or one that wants nothing now, leaves none kept.
    pub(super) fn refresh_wants(&mut self) {
        let due = self.wants_due();
        let mut kept = std::mem::take(&mut self.wants);
        let ids: Vec<NodeId> = match due {
            Some(due) => due.into_iter().collect(),
            None => {
                kept.by_node.clear();
                self.nodes.keys().copied().collect()
            }
        };
        let mut seeks = Vec::new();
        for id in &ids {
            self.node_wants(*id, &mut seeks);
            if seeks.is_empty() {
                kept.by_node.remove(id);
            } else {
                kept.by_node.insert(*id, seeks.clone());
            }
        }
        kept.at = Some(self.nodes.generation());
        #[cfg(any(test, feature = "test-support"))]
        {
            kept.read = ids.len();
        }
        self.wants = kept;
        #[cfg(any(test, feature = "test-support"))]
        self.check_kept_wants(&ids);
    }

    // Every node the round did not read again, read as a walk of every node
    // reads it: its kept wants must be what it wants now. A difference
    // names the node, its kind, and the unmarked path that changed it.
    #[cfg(any(test, feature = "test-support"))]
    fn check_kept_wants(&mut self, read: &[NodeId]) {
        let read: BTreeSet<NodeId> = read.iter().copied().collect();
        let unmarked = self.nodes.take_unmarked_moves();
        let ids: BTreeSet<NodeId> = self
            .nodes
            .keys()
            .chain(self.wants.by_node.keys())
            .copied()
            .collect();
        let mut seeks = Vec::new();
        for id in ids.difference(&read) {
            self.node_wants(*id, &mut seeks);
            let kept = self.wants.by_node.get(id).map_or(&[][..], Vec::as_slice);
            if kept == seeks.as_slice() {
                continue;
            }
            let progress = self.nodes.progress(*id);
            let cause = if unmarked.contains(id) {
                "its object stepped out and in unmarked"
            } else if self.wants.said.get(id).copied().unwrap_or_default()
                != (progress.asking, progress.refuted)
            {
                "its run progress changed other than the volume reached"
            } else {
                "no tracked path (state inside the object, or a new path)"
            };
            let kind = self
                .kind(*id)
                .map_or_else(|| "none".to_owned(), |kind| kind.to_string());
            panic!(
                "the kept wants of node {} ({kind}) differ from a walk: {cause}; kept {kept:?}, walked {seeks:?}",
                id.0
            );
        }
        let said: BTreeMap<NodeId, (bool, bool)> = self
            .nodes
            .keys()
            .map(|id| {
                let progress = self.nodes.progress(*id);
                (*id, (progress.asking, progress.refuted))
            })
            .collect();
        self.wants.said = said;
    }
}
