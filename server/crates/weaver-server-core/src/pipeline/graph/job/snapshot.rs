// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts,
// the job's count of repair reads and the phase it last reported, the
// writer's files and kept envelopes, and which candidate each node was
// offered last.

use std::collections::BTreeMap;

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Snapshot};
use crate::pipeline::graph::root::Root;
use crate::pipeline::graph::root::facts::{JobFacts, RootBlob};

impl<R: Root> GraphJob<R> {
    pub(crate) fn snapshot(&mut self) -> Snapshot {
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
        Snapshot {
            job: JobFacts {
                read_backs: self.writer.read_backs(),
                reported: self.reported,
            },
            root: RootBlob::of(&self.root, &mut self.kept),
            tree,
            files,
            kept: self.writer.kept_records(),
            passwords: self.passwords.last_offered().collect(),
        }
    }
}
