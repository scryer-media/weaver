// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's password candidates, offered to an object that asks, one at a
// time and in the order they are tried. Which one each node was offered last
// is the job's to remember, not the tree's: the tree is format-blind and
// knows no secrets. An object asks again only after it tried the last offer,
// so one is in flight per node, and the cursor moving on is what keeps any
// candidate from being offered twice to the same node. A node that asks
// with none left is left asking: more of it may still arrive, and the
// object, which knows it is still asking, refuses itself once all of it
// has or once the job is quiet.
//
// The values are secrets. Nothing here prints them; Debug says how many.

use std::fmt;

use weaver_graph::{Builder, Effect, Marked, NodeId};

use super::GraphJob;

#[derive(Default)]
pub(super) struct Passwords {
    candidates: Vec<String>,
    // How many candidates each node was offered, marked for every node an
    // offer moves on, so a checkpoint reads again only those.
    offered: Marked<NodeId, usize>,
}

impl fmt::Debug for Passwords {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Passwords")
            .field("candidates", &self.candidates.len())
            .field("offered", &*self.offered)
            .finish()
    }
}

impl Passwords {
    pub(super) fn new(candidates: Vec<String>) -> Self {
        Self {
            candidates,
            ..Self::default()
        }
    }

    // The candidate the node was offered last, by index: a checkpoint keeps
    // the index, never the candidate.
    pub(super) fn last_offered(&self, node: NodeId) -> Option<usize> {
        self.offered.get(&node)?.checked_sub(1)
    }

    // The generation of the offers: an offer to a node marks it under a
    // later one.
    pub(super) fn generation(&self) -> u64 {
        self.offered.marks().at()
    }

    // The nodes offered a candidate since generation `at`.
    pub(super) fn changed_since(&self, at: u64) -> impl Iterator<Item = NodeId> + '_ {
        self.offered.marks().since(at)
    }

    // A restore takes up where the checkpoint left the node: the cursor is
    // past the candidate it was offered last, and that candidate is offered
    // again. `None` when the job no longer has it.
    pub(super) fn resume_at(&mut self, node: NodeId, index: usize) -> Option<&str> {
        let candidate = self.candidates.get(index)?;
        self.offered.insert(node, index + 1);
        Some(candidate)
    }

    // The candidate after the one the node was offered last, if any is left.
    fn next(&mut self, node: NodeId) -> Option<&str> {
        let offered = self.offered.get(&node).copied().unwrap_or_default();
        let candidate = self.candidates.get(offered)?;
        self.offered.insert(node, offered + 1);
        Some(candidate)
    }
}

impl GraphJob {
    // How many candidates each node has been offered, by node.
    #[cfg(test)]
    pub(crate) fn passwords_offered(&self) -> std::collections::BTreeMap<NodeId, usize> {
        (*self.passwords.offered).clone()
    }

    // The object at `node` asked for a password: it gets the next candidate,
    // and an engine's run starts over under it, by the transition a repair
    // behind the run takes. With none left, the node is left asking. True
    // when a candidate was offered.
    pub(super) fn offer_password(&mut self, node: NodeId, effects: &mut Vec<Effect>) -> bool {
        let builder: &mut dyn Builder = &mut *self.builder;
        let Some(secret) = self.passwords.next(node) else {
            return false;
        };
        self.tree
            .offer_password(node, secret, &mut self.writer.placing(builder), effects);
        if self.tree.engine(node).is_some() {
            self.writer.shelf(node).restart();
        }
        true
    }
}
