// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts,
// the job's count of repair reads and the phase it last reported, the
// writer's files and kept envelopes, and which candidate each node was
// offered last. A node is read on its own, so a checkpoint reads only the
// nodes the tree or the writer marked since the generations it read at.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{MemberId, NodeId, NodeRecord};

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Generations, Snapshot};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::root::Root;
use crate::pipeline::graph::root::facts::{JobFacts, Parts, RootBlob, RootRow};

impl<R: Root> GraphJob<R> {
    // The whole job, its root's facts encoded whole.
    pub(crate) fn snapshot(&self) -> Snapshot {
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
            job: self.job_facts(),
            root: RootBlob::of(&self.root),
            tree,
            files,
            kept: self.writer.kept_records(),
            passwords: self.passwords.last_offered().collect(),
        }
    }

    fn job_facts(&self) -> JobFacts {
        JobFacts {
            read_backs: self.writer.read_backs(),
            reported: self.reported,
        }
    }

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

    pub(crate) fn kept_records(&self) -> Vec<KeptRecord> {
        self.writer.kept_records()
    }

    pub(crate) fn last_offered(&self) -> BTreeMap<NodeId, usize> {
        self.passwords.last_offered().collect()
    }

    // The root row, its root's facts encoded again only where they changed
    // since the job last encoded them, and the parts that encoding names;
    // `whole` forgets what was kept, so the encoding reads the root whole
    // and names every part.
    pub(crate) fn encode_root(&mut self, whole: bool) -> (RootRow, Parts) {
        if whole {
            self.kept = R::Kept::default();
        }
        let (root, parts) = RootBlob::encode(&self.root, &mut self.kept);
        let row = RootRow {
            next_node: self.tree.next_node(),
            job: self.job_facts(),
            root,
        };
        (row, parts)
    }
}
