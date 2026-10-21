// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts
// with the job's count of repair reads and the phase it last reported, the
// writer's files and kept
// envelopes, and which candidate each node was offered last.

use std::collections::BTreeMap;

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Records};
use crate::pipeline::graph::root::facts::{Encoded, Encoding};

impl GraphJob {
    pub(crate) fn records(&self) -> Records {
        let tree = self.tree.records();
        let mut files = BTreeMap::new();
        for node in &tree.nodes {
            for member in &node.members {
                let state = self.writer.file_state(node.node, member.member);
                if state != FileState::default() {
                    files.insert((node.node, member.member), state);
                }
            }
        }
        Records {
            tree,
            files,
            kept: self.writer.kept_records(),
            passwords: self.passwords.last_offered().collect(),
        }
    }

    // The root's facts, with the job's count of repair reads and the phase
    // it last reported, encoded into `encoded`.
    pub(crate) fn encode_root(&self, encoded: &mut Encoded, next_node: u32) -> Encoding {
        self.root
            .encode(encoded, next_node, self.writer.read_backs(), self.reported)
    }
}
