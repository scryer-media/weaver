// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts
// with the job's count of repair reads and the phase it last reported, the
// writer's files and kept envelopes, and which candidate each node was
// offered last. A node is read on its own, so a checkpoint reads only the
// nodes the tree or the writer marked since the generations it read at.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{MemberId, NodeId, NodeRecord};

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Generations};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::root::facts::{Encoded, Encoding};

impl GraphJob {
    pub(crate) fn generations(&self) -> Generations {
        (self.tree.generation(), self.writer.files_generation())
    }

    // The nodes the tree or the writer changed since `at`: with `at` the
    // generations nothing was read at, every node the job ever had.
    pub(crate) fn changed_since(&self, (tree, files): Generations) -> BTreeSet<NodeId> {
        self.tree
            .changed_since(tree)
            .chain(self.writer.files_changed_since(files))
            .collect()
    }

    // One node's record, and what the writer holds of each of its members
    // it holds anything of; `None` for a node the tree no longer has.
    pub(crate) fn node(&self, node: NodeId) -> Option<(NodeRecord, BTreeMap<MemberId, FileState>)> {
        let record = self.tree.record(node)?;
        let files = record
            .members
            .iter()
            .map(|member| (member.member, self.writer.file_state(node, member.member)))
            .filter(|(_, state)| *state != FileState::default())
            .collect();
        Some((record, files))
    }

    pub(crate) fn next_node(&self) -> u32 {
        self.tree.next_node()
    }

    pub(crate) fn kept_records(&self) -> Vec<KeptRecord> {
        self.writer.kept_records()
    }

    pub(crate) fn last_offered(&self) -> BTreeMap<NodeId, usize> {
        self.passwords.last_offered().collect()
    }

    // The root's facts, with the job's count of repair reads and the phase
    // it last reported, encoded into `encoded`.
    pub(crate) fn encode_root(&self, encoded: &mut Encoded, next_node: u32) -> Encoding {
        self.root
            .encode(encoded, next_node, self.writer.read_backs(), self.reported)
    }
}
