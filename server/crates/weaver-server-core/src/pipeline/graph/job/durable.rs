// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts,
// the job's count of repair reads and the phase it last reported, the
// writer's files, kept envelopes and holds on scratch, and which candidate
// each node was offered last. A node is read on its own, so a checkpoint
// reads only the nodes the tree, the writer, its envelopes or an offer
// marked since the generations it read at.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::holds::ScratchHold;
use weaver_graph::{MemberId, NodeId, NodeRecord};

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Generations, HoldRecord, Snapshot};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::namespace::WriteError;
use crate::pipeline::graph::root::Root;
use crate::pipeline::graph::root::facts::{JobFacts, Parts, RootBlob, RootRow};

impl<R: Root> GraphJob<R> {
    // The whole job, its root's facts encoded whole.
    pub(crate) fn snapshot(&self) -> Snapshot {
        let tree = self.tree.records();
        let mut files = BTreeMap::new();
        let mut kept = Vec::new();
        let mut holds = Vec::new();
        let mut passwords = BTreeMap::new();
        for node in &tree.nodes {
            for member in &node.members {
                let state = self.writer.file_state(node.node, member.member);
                if state != FileState::default() {
                    files.insert((node.node, member.member), state);
                }
            }
            kept.extend(self.kept_records(node.node));
            holds.extend(
                self.hold_records(node.node)
                    .into_iter()
                    .map(|held| HoldRecord {
                        node: node.node,
                        held,
                    }),
            );
            if let Some(index) = self.last_offered(node.node) {
                passwords.insert(node.node, index);
            }
        }
        Snapshot {
            job: self.job_facts(),
            root: RootBlob::of(&self.root),
            tree,
            files,
            kept,
            holds,
            passwords,
        }
    }

    fn job_facts(&self) -> JobFacts {
        JobFacts {
            read_backs: self.writer.read_backs(),
            reported: self.reported,
        }
    }

    pub(crate) fn generations(&self) -> Generations {
        Generations {
            tree: self.tree.generation(),
            files: self.writer.files_generation(),
            kept: self.writer.kept_generation(),
            offered: self.passwords.generation(),
        }
    }

    // The nodes the tree, the writer, its envelopes or an offer changed
    // since `at`: with `at` the generations nothing was read at, every node
    // the job ever had.
    pub(crate) fn changed_since(&self, at: Generations) -> BTreeSet<NodeId> {
        self.tree
            .changed_since(at.tree)
            .chain(self.writer.files_changed_since(at.files))
            .chain(self.writer.kept_changed_since(at.kept))
            .chain(self.passwords.changed_since(at.offered))
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

    // The node's kept envelopes.
    pub(crate) fn kept_records(&self, node: NodeId) -> Vec<KeptRecord> {
        self.writer.kept_records(node)
    }

    // The holds the node's namespace has on scratch.
    pub(crate) fn hold_records(&self, node: NodeId) -> Vec<ScratchHold> {
        self.writer.hold_records(node)
    }

    // A graceful stop: what the job holds in RAM goes to scratch, so the
    // checkpoint after it spares a restart fetching it again.
    pub(crate) fn page_out_holds(&mut self) -> Result<(), WriteError> {
        self.writer.page_out_holds()
    }

    // The candidate the node was offered last, by index.
    pub(crate) fn last_offered(&self, node: NodeId) -> Option<usize> {
        self.passwords.last_offered(node)
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

    // The bytes of the units the root's facts say this run does not fetch
    // again: what the facts encoded now spare a restore.
    pub(crate) fn settled_bytes(&self) -> u64 {
        self.root.settled_bytes()
    }
}
