// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A finishing job's download summary, read once from what the job holds in
// memory as it is archived: its graph's tree and recovery sets, and its
// post. Nothing here runs per article, block or tick, and nothing in the
// engines carries a field for it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use weaver_graph::recovery::{RepairOutcome, SetRuling};
use weaver_graph::{Kind, NodeId, NodeState, Why};

use crate::jobs::download_summary::{
    DOWNLOAD_SUMMARY_VERSION, DownloadSummary, Layout, NODES_KEPT, NodeMode, RECOVERY_KEPT,
    SETS_KEPT, SummaryNode, SummaryPost, SummaryRecovery, SummarySet, bounded_name,
};
use crate::jobs::ids::JobId;
use crate::jobs::model::JobState;
use crate::pipeline::Pipeline;
use crate::pipeline::graph::checkpoint::{Generations, why_name};
use crate::pipeline::graph::job::GraphJob;

fn recovers(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::Par2 | Kind::Par3 | Kind::Sfv | Kind::RarRecovery | Kind::RarRecord
    )
}

fn extracts(kind: Kind) -> bool {
    matches!(
        kind,
        Kind::RarEngine | Kind::SevenZipEngine | Kind::ZipEngine
    )
}

fn outcome_name(outcome: RepairOutcome) -> (&'static str, u64) {
    match outcome {
        RepairOutcome::Clean => ("clean", 0),
        RepairOutcome::Repaired { slices } => ("repaired", slices),
        RepairOutcome::Impossible { .. } => ("impossible", 0),
        RepairOutcome::Waiting => ("waiting", 0),
        RepairOutcome::Unread => ("unread", 0),
    }
}

// What the job's graph says: its archive tree, at most `keep` nodes breadth
// first with the true count and depth, its layout and its recovery objects.
// A node a password opened names the source the job recorded for that
// password when its candidates were set.
pub(crate) fn graph_summary(job: &GraphJob, keep: usize) -> DownloadSummary {
    let tree = job.tree();
    // Every node the job ever had, once each, with the namespace it sits in.
    let mut every = BTreeSet::new();
    job.visit_touched_since(Generations::default(), |node, _| {
        every.insert(node);
    });
    let mut children: BTreeMap<NodeId, Vec<NodeId>> = BTreeMap::new();
    let mut node_count = 0u32;
    let mut max_depth = 0u32;
    for node in every {
        let (Some(kind), Some((parent, _))) = (tree.kind(node), tree.volumes(node)) else {
            continue;
        };
        if recovers(kind) {
            continue;
        }
        node_count += 1;
        max_depth = max_depth.max(tree.depth(node));
        children.entry(parent).or_default().push(node);
    }

    let mut nodes = Vec::new();
    let mut queue = VecDeque::from([NodeId::ROOT]);
    while let Some(parent) = queue.pop_front() {
        for &node in children.get(&parent).into_iter().flatten() {
            if nodes.len() == keep {
                break;
            }
            nodes.push(summary_node(job, node, parent));
            queue.push_back(node);
        }
        if nodes.len() == keep {
            break;
        }
    }

    let mut recovery = Vec::new();
    let mut rulings: Vec<SetRuling> = Vec::new();
    let objects = job.recovery_summary();
    for object in objects.iter().take(RECOVERY_KEPT) {
        rulings.clear();
        job.writer().rulings(tree, object.node, &mut rulings);
        recovery.push(SummaryRecovery {
            node: object.node.0,
            kind: object.kind.to_string(),
            sets: rulings
                .iter()
                .take(SETS_KEPT)
                .map(|ruling| {
                    let outcome = ruling.outcome.map(outcome_name);
                    SummarySet {
                        blocks_reachable: ruling.blocks_reachable,
                        damaged_slices: ruling.damaged_slices,
                        outcome: outcome.map(|(name, _)| name.to_owned()),
                        slices_repaired: outcome.map_or(0, |(_, slices)| slices),
                        read_back: ruling.read
                            && matches!(ruling.outcome, Some(RepairOutcome::Clean)),
                    }
                })
                .collect(),
            set_count: rulings.len() as u32,
            outcome: object
                .outcome
                .map(|outcome| outcome_name(outcome).0.to_owned()),
        });
    }

    DownloadSummary {
        version: DOWNLOAD_SUMMARY_VERSION,
        layout: Some(layout(job)),
        nodes,
        node_count,
        max_depth,
        recovery,
        recovery_count: objects.len() as u32,
        post: SummaryPost::default(),
    }
}

fn summary_node(job: &GraphJob, node: NodeId, parent: NodeId) -> SummaryNode {
    let tree = job.tree();
    let kind = tree.kind(node).expect("listed with its kind");
    let record = tree.own_record(node);
    let reason = record.as_ref().and_then(|record| record.reason);
    let given_up = record
        .as_ref()
        .is_some_and(|record| record.state == NodeState::Retired)
        || reason.is_some();
    let offered = job.last_offered(node).is_some();
    let locked = matches!(reason, Some(Why::Encrypted | Why::EncryptedHeader));
    SummaryNode {
        id: node.0,
        parent: (parent != NodeId::ROOT).then_some(parent.0),
        kind: kind.to_string(),
        name: job.writer().set_name(tree, node).map(bounded_name),
        volumes: tree
            .volumes(node)
            .map_or(0, |(_, volumes)| volumes.len() as u32),
        depth: tree.depth(node),
        mode: if extracts(kind) {
            NodeMode::Extracted
        } else {
            NodeMode::Streamed
        },
        given_up,
        reason: reason.map(|why| why_name(why).to_owned()),
        encrypted: offered || locked,
        encrypted_headers: matches!(reason, Some(Why::EncryptedHeader)),
        password_needed: offered || locked,
        password_source: offered
            .then(|| job.opened_source(node))
            .flatten()
            .map(|source| source.as_str().to_owned()),
    }
}

// Whether the post's payload is plain files, archives, or both. A recovery
// file is neither.
fn layout(job: &GraphJob) -> Layout {
    let tree = job.tree();
    let (mut archived, mut plain) = (false, false);
    for member in tree
        .namespace(NodeId::ROOT)
        .into_iter()
        .flat_map(|namespace| namespace.members())
    {
        match tree
            .child_of(NodeId::ROOT, member.id)
            .and_then(|(owner, _)| tree.kind(owner))
        {
            Some(kind) if recovers(kind) => {}
            Some(_) => archived = true,
            None => plain = true,
        }
        if archived && plain {
            return Layout::Mixed;
        }
    }
    if archived {
        Layout::Archive
    } else {
        Layout::Plain
    }
}

// What the post held, from the job's own state: its files and articles,
// the articles that never came, its dates and its groups.
pub(crate) fn post_summary(state: &JobState) -> SummaryPost {
    let files = &state.spec.files;
    let mut groups: BTreeSet<&str> = BTreeSet::new();
    let (mut oldest, mut newest) = (None::<u64>, None::<u64>);
    let mut articles = 0u64;
    for file in files {
        articles += file.segments.len() as u64;
        groups.extend(file.groups.iter().map(String::as_str));
        if let Some(at) = file.posted_at_epoch {
            oldest = Some(oldest.map_or(at, |old| old.min(at)));
            newest = Some(newest.map_or(at, |new| new.max(at)));
        }
    }
    SummaryPost {
        files: files.len() as u32,
        articles,
        missing_articles: state.support_facts.gaps.missing,
        failed_articles: state.support_facts.gaps.failed,
        oldest_post: oldest,
        newest_post: newest,
        groups: groups.len() as u32,
    }
}

// How many tree nodes a summary keeps; a test lowers it on its own thread,
// where the pipeline it drives runs.
#[cfg(test)]
thread_local! {
    pub(crate) static NODES_KEPT_HERE: std::cell::Cell<usize> =
        const { std::cell::Cell::new(NODES_KEPT) };
}

fn nodes_kept() -> usize {
    #[cfg(test)]
    {
        NODES_KEPT_HERE.with(std::cell::Cell::get)
    }
    #[cfg(not(test))]
    {
        NODES_KEPT
    }
}

impl Pipeline {
    // The finishing job's summary, from its graph when it ran one.
    pub(crate) fn download_summary(&self, job_id: JobId) -> Option<DownloadSummary> {
        self.download_summary_keeping(job_id, nodes_kept())
    }

    pub(crate) fn download_summary_keeping(
        &self,
        job_id: JobId,
        keep: usize,
    ) -> Option<DownloadSummary> {
        let state = self.jobs.get(&job_id)?;
        let graph = self
            .graph_jobs
            .get(&job_id)
            .or_else(|| self.retiring_graph_jobs.get(&job_id));
        let mut summary = match graph {
            Some(job) => graph_summary(job, keep),
            None => DownloadSummary {
                version: DOWNLOAD_SUMMARY_VERSION,
                ..DownloadSummary::default()
            },
        };
        summary.post = post_summary(state);
        Some(summary)
    }

    // Ahead of the archive, which copies it from the active row: a job write
    // that the archive, queued after it, waits out.
    pub(in crate::pipeline) fn persist_download_summary_before_archive(&self, job_id: JobId) {
        let Some(json) = self
            .download_summary(job_id)
            .and_then(|summary| summary.to_storage_json())
        else {
            return;
        };
        if let Err(error) =
            self.db
                .try_queue_job_write(job_id, "active_download_summary", move |db| {
                    db.save_active_download_summary(job_id, json)
                })
        {
            tracing::error!(%error, job_id = job_id.0, "failed to queue the download summary before archive");
        }
    }
}
