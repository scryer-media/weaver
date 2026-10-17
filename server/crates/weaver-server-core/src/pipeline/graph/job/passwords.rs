// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job's password candidates, offered to an object that asks, one at a
// time and in the order they are tried. Which one each node was offered last
// is the job's to remember, not the tree's: the tree is format-blind and
// knows no secrets. An object asks again only after it tried the last offer,
// so one is in flight per node, and the cursor moving on is what keeps any
// candidate from being offered twice to the same node.
//
// The values are secrets. Nothing here prints them; Debug says how many.

use std::collections::BTreeMap;
use std::fmt;

use weaver_graph::{Builder, Effect, Emit, NodeId, Why};

use super::GraphJob;

#[derive(Default)]
pub(super) struct Passwords {
    candidates: Vec<String>,
    offered: BTreeMap<NodeId, usize>,
}

impl fmt::Debug for Passwords {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Passwords")
            .field("candidates", &self.candidates.len())
            .field("offered", &self.offered)
            .finish()
    }
}

impl Passwords {
    pub(super) fn new(candidates: Vec<String>) -> Self {
        Self {
            candidates,
            offered: BTreeMap::new(),
        }
    }

    // The candidate after the one the node was offered last, if any is left.
    fn next(&mut self, node: NodeId) -> Option<&str> {
        let offered = self.offered.entry(node).or_default();
        let candidate = self.candidates.get(*offered)?;
        *offered += 1;
        Some(candidate)
    }
}

impl GraphJob {
    // The object at `node` asked for a password: it gets the next candidate,
    // and an engine's run starts over under it, by the transition a repair
    // behind the run takes. With none left, the object refuses, as it would
    // have had the job no candidates at all.
    pub(super) fn offer_password(&mut self, node: NodeId, effects: &mut Vec<Effect>) {
        let builder: &mut dyn Builder = &mut *self.builder;
        let Some(secret) = self.passwords.next(node) else {
            self.tree.emitted(
                node,
                vec![Emit::Refuse(Why::EncryptedHeader)],
                builder,
                effects,
            );
            return;
        };
        self.tree.offer_password(node, secret, builder, effects);
        if self.tree.engine(node).is_some() {
            self.writer.shelf(node).restart();
        }
    }
}
