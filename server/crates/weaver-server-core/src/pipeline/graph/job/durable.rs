// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job as a checkpoint takes it: the tree's records, the root's facts,
// the job's count of repair reads and the phase it last reported, the
// writer's files, kept envelopes and holds on scratch, and which candidate
// each node was offered last. A node's own record and each of its members
// are read on their own, so a checkpoint reads only what the tree, the
// writer, its envelopes or an offer marked since the generations it read at.

use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::holds::ScratchHold;
use weaver_graph::tree::Mark;
use weaver_graph::{CoverageRecord, MemberId, MemberRecord, NodeId, NodeRecord};

use super::GraphJob;
use crate::pipeline::graph::checkpoint::{FileState, Generations, MemberGenerations, Touched};
#[cfg(test)]
use crate::pipeline::graph::checkpoint::{HoldRecord, Snapshot};
use crate::pipeline::graph::envelope::durable::KeptRecord;
use crate::pipeline::graph::namespace::WriteError;
use crate::pipeline::graph::root::Root;
use crate::pipeline::graph::root::facts::{JobFacts, Parts, RootBlob, RootRow};

impl<R: Root> GraphJob<R> {
    // The whole job, its root's facts encoded whole.
    #[cfg(test)]
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
            pending_verifications: self.writer.pending_verifications(),
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

    // What the tree, the writer, its envelopes or an offer changed of each
    // node since `at`: with `at` the generations nothing was read at, every
    // node the job ever had.
    pub(crate) fn touched_since(&self, at: Generations) -> BTreeMap<NodeId, Touched> {
        let mut out: BTreeMap<NodeId, Touched> = BTreeMap::new();
        for mark in self.tree.marked_since(at.tree) {
            match mark {
                Mark::Node(node) => out.entry(node).or_default().own = true,
                Mark::Members(node) => out.entry(node).or_default().members = true,
            }
        }
        for node in self.writer.files_changed_since(at.files) {
            out.entry(node).or_default().files = true;
        }
        for node in self.writer.kept_changed_since(at.kept) {
            out.entry(node).or_default().kept = true;
        }
        for node in self.passwords.changed_since(at.offered) {
            out.entry(node).or_default().own = true;
        }
        out
    }

    // The node's own record, its members left out, and the candidate it
    // was offered last; `None` for a node the tree no longer has.
    pub(crate) fn own_record(&self, node: NodeId) -> Option<(NodeRecord, Option<usize>)> {
        Some((self.tree.own_record(node)?, self.last_offered(node)))
    }

    // The generations of the node's members: of its namespace, and of the
    // writer's files of them.
    pub(crate) fn member_generations(&self, node: NodeId) -> MemberGenerations {
        MemberGenerations {
            namespace: self
                .tree
                .namespace(node)
                .map_or(0, |namespace| namespace.generation()),
            files: self.writer.member_files_generation(node),
        }
    }

    // The node's members its namespace or the writer's files of them
    // changed since `at`; every member it has when `at` is `None`, or when
    // a generation went back, as no live node's does. `None` for a node the
    // tree no longer has.
    pub(crate) fn members_changed(
        &self,
        node: NodeId,
        at: Option<MemberGenerations>,
        touched: Touched,
    ) -> Option<BTreeSet<MemberId>> {
        let namespace = self.tree.namespace(node)?;
        let now = self.member_generations(node);
        match at {
            Some(at) if at.namespace <= now.namespace && at.files <= now.files => {
                let mut out = BTreeSet::new();
                if touched.members {
                    out.extend(namespace.changed_since(at.namespace));
                }
                if touched.files {
                    out.extend(self.writer.member_files_changed_since(node, at.files));
                }
                Some(out)
            }
            _ => Some(namespace.records().map(|member| member.id).collect()),
        }
    }

    // One member's record, its coverage and what the writer holds of it;
    // `None` for a member the node does not have.
    pub(crate) fn member(
        &self,
        node: NodeId,
        member: MemberId,
    ) -> Option<(MemberRecord, Vec<CoverageRecord>, FileState)> {
        let (record, coverage) = self.tree.member_record(node, member)?;
        Some((record, coverage, self.writer.file_state(node, member)))
    }

    // One node's record, and what the writer holds of each of its members
    // it holds anything of; `None` for a node the tree no longer has.
    #[cfg(test)]
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
