// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One graph job's runtime: its tree, its article root, the builder its
// objects come from, and the namespace writer its bytes go through. The
// pipeline holds one per job admitted under the graph engine and hands it
// what the download and decode paths learn; nothing else of the pipeline
// reaches into the tree, and nothing else writes the job's files.

mod passwords;
mod repair;
mod runs;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;
use weaver_graph::holds::HoldsAccountant;
use weaver_graph::recovery::{RepairOutcome, SliceVerdict};
use weaver_graph::{
    Builder, Effect, Kind, MemberId, Namespace, NodeId, Pull, Seek, SeekUp, Tree, VolumeIndex,
};

use self::passwords::Passwords;
use super::Engine;
use super::engine::{Runs, Said};
use super::namespace::{NamespaceWriter, WriteError};
use super::root::intake::Decoded;
use super::root::phase::{self, RootPhase};
use super::root::sample;
use super::root::source::Move;
use super::root::{ArticleRoot, Ask};
use crate::operations::disk::CapacityReader;

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphError {
    #[error("the namespace write path failed")]
    Write(#[from] WriteError),
}

// Where a graph job writes and what it may hold: its working directory, the
// process-wide holds accountant every job shares, the free space of the
// volume its scratch lives on, how many engines it may run at once, and
// where their runs speak.
pub(crate) struct Storage {
    pub(crate) working_dir: PathBuf,
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) free_disk: CapacityReader,
    pub(crate) slots: usize,
    pub(crate) mail: mpsc::Sender<Said>,
    // The job's password candidates, in the order they are tried.
    pub(crate) passwords: Vec<String>,
}

// What one recovery object says of the members it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    // The tree node the object is. One PAR2 object holds every PAR2 set of
    // its namespace, so this is per object, not per set id.
    pub(crate) node: NodeId,
    // The namespace it lives in, where the members it covers are.
    pub(crate) namespace: NodeId,
    pub(crate) kind: Kind,
    // Recovery blocks in hand or still on their way.
    pub(crate) blocks_reachable: u64,
    // Slices the evidence in hand says are damaged, over every member the
    // object covers.
    pub(crate) damaged_slices: u64,
    // What its last repair came to; `None` before it ran one.
    pub(crate) outcome: Option<RepairOutcome>,
}

pub(crate) struct GraphJob {
    tree: Tree,
    root: ArticleRoot,
    builder: Box<dyn Builder + Send + Sync>,
    writer: NamespaceWriter,
    runs: Runs,
    passwords: Passwords,
    // The phase last reported, so only a change is reported.
    reported: Option<RootPhase>,
    // Per recovery object, how many of its volumes were closed when it last
    // ruled: one waiting on recovery bytes rules again once more are.
    ruled_at: BTreeMap<NodeId, usize>,
}

// A root member as the post lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootFile {
    pub(crate) name: String,
    // Each article's ordinal and declared encoded size.
    pub(crate) articles: Vec<(u32, u32)>,
}

impl GraphJob {
    // The runtime a job admitted under `engine` gets, if any: a classic job
    // has none. `scope` is the job's process-wide id.
    pub(crate) fn for_engine(
        engine: Engine,
        scope: u64,
        files: &[RootFile],
        storage: Storage,
    ) -> Option<Self> {
        (engine == Engine::Graph).then(|| {
            let runs = Runs::new(
                scope,
                storage.slots,
                storage.mail,
                Arc::clone(&storage.accountant),
            );
            let writer = NamespaceWriter::new(
                scope,
                storage.working_dir,
                storage.accountant,
                storage.free_disk,
            );
            Self::new(files, Box::new(weaver_objects::Objects), writer, runs)
                .offering(storage.passwords)
        })
    }

    pub(crate) fn new(
        files: &[RootFile],
        mut builder: Box<dyn Builder + Send + Sync>,
        mut writer: NamespaceWriter,
        runs: Runs,
    ) -> Self {
        let mut tree = Tree::new();
        let volumes: Vec<_> = files.iter().map(|file| file.articles.clone()).collect();
        let root = ArticleRoot::new(&volumes);
        let mut out = Vec::new();
        for file in files {
            // Nothing is below the root yet, so no name moves; were one to,
            // its file would follow as any rename's does.
            let mut moved = Vec::new();
            let member = tree.announce_root(&file.name, None, &mut moved);
            // A member that cannot be announced still downloads; its first
            // write reports the same failure where a step can carry it.
            if let Err(error) = writer
                .apply_all(&mut tree, &mut *builder, &root, moved, &mut out)
                .and_then(|()| {
                    writer.announced(
                        &mut tree,
                        &mut *builder,
                        &root,
                        NodeId::ROOT,
                        member,
                        &mut out,
                    )
                })
            {
                warn!(member = member.0, error = %error, "graph member announcement failed");
            }
        }
        Self {
            tree,
            root,
            builder,
            writer,
            runs,
            passwords: Passwords::default(),
            reported: None,
            ruled_at: BTreeMap::new(),
        }
    }

    // The candidates offered to an object that asks for a password.
    pub(crate) fn offering(mut self, candidates: Vec<String>) -> Self {
        self.passwords = Passwords::new(candidates);
        self
    }

    #[cfg(test)]
    pub(crate) fn runs(&self) -> &Runs {
        &self.runs
    }

    #[cfg(test)]
    pub(crate) fn set_builder(&mut self, builder: Box<dyn Builder + Send + Sync>) {
        self.builder = builder;
    }

    pub(crate) fn tree(&self) -> &Tree {
        &self.tree
    }

    pub(crate) fn root(&self) -> &ArticleRoot {
        &self.root
    }

    pub(crate) fn writer(&self) -> &NamespaceWriter {
        &self.writer
    }

    // Seam for completion, status and health: where the job stands, read
    // off the tree. Stable.
    pub(crate) fn phase(&self) -> RootPhase {
        let tree = &self.tree;
        let activity = phase::activity(
            self.recovery_objects()
                .into_iter()
                .map(|(_, node)| tree.recovery(node).and_then(|recovery| recovery.outcome())),
        );
        let extracting = phase::extracting(tree) || self.runs.live();
        RootPhase::derive(self.payload_waiting(), activity, extracting)
    }

    // Seam for completion, status and health: the root namespace, which is
    // the job's claim on its members. Its members, their coverage,
    // `is_clean`, `damaged_ranges` and each member's `output_name` are the
    // job's files as the graph sees them. Stable.
    pub(crate) fn namespace(&self) -> &Namespace {
        self.tree
            .namespace(NodeId::ROOT)
            .expect("the root namespace lives as long as the tree")
    }

    // Seam for completion, status and health: each recovery object of every
    // namespace in the tree, parents first, with the blocks it can reach and
    // the slices it knows are damaged. Stable.
    pub(crate) fn recovery_summary(&self) -> Vec<RecoverySummary> {
        let tree = &self.tree;
        let mut verdicts = Vec::new();
        self.recovery_objects()
            .into_iter()
            .filter_map(|(namespace, node)| {
                let (kind, recovery) = (tree.kind(node)?, tree.recovery(node)?);
                verdicts.clear();
                for member in tree.namespace(namespace)?.members() {
                    if recovery.covers(member.id) {
                        recovery.verdicts(member.id, &mut verdicts);
                    }
                }
                let damaged_slices = verdicts
                    .iter()
                    .filter(|verdict| **verdict == SliceVerdict::Damaged)
                    .count() as u64;
                Some(RecoverySummary {
                    node,
                    namespace,
                    kind,
                    blocks_reachable: recovery.blocks_reachable(),
                    damaged_slices,
                    outcome: recovery.outcome(),
                })
            })
            .collect()
    }

    // Seam for completion, status and health: every root member written to
    // a file of its own, leaves and recovery volumes alike, with the path
    // it is written at. A member bound to an object that places its bytes
    // elsewhere has none. Stable.
    pub(crate) fn member_files(&self) -> Vec<(MemberId, PathBuf)> {
        self.writer
            .files(NodeId::ROOT)
            .map(|(member, path)| (member, path.to_path_buf()))
            .collect()
    }

    // Articles the queues do not hold were fetched by an earlier run.
    pub(crate) fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool) {
        self.root.map.arrived_before(queued);
    }

    // The first article of every member leads the payload.
    pub(crate) fn sample(&mut self) -> Vec<Move> {
        self.root.map.sample();
        self.root.map.take_moves()
    }

    // `limit` is the slot limit the pipeline reads now, which every step
    // that asks or frees a run follows.
    pub(crate) fn intake(&mut self, decoded: Decoded, limit: usize) -> Result<(), GraphError> {
        let volume = decoded.volume;
        let member = MemberId(volume.0);
        let length = decoded.length;
        let arrival = self.root.receive(decoded);
        if let Some(length) = length {
            self.writer
                .length_learned(&mut self.tree, &self.root, NodeId::ROOT, member, length)?;
        }
        let mut out = Vec::new();
        self.writer.deliver(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            arrival,
            &mut out,
        )?;
        self.heard(out)?;
        self.mark_damage()?;
        self.after_member(volume)?;
        self.step_runs(Vec::new(), limit)
    }

    // Every server failed the article; nothing will fetch it again.
    pub(crate) fn lost(
        &mut self,
        volume: VolumeIndex,
        ordinal: u32,
        limit: usize,
    ) -> Result<(), GraphError> {
        self.root.map.lost(volume, ordinal);
        self.mark_damage()?;
        self.after_member(volume)?;
        self.step_runs(Vec::new(), limit)
    }

    // After an article of `volume` was settled: a member with nothing left
    // to fetch is closed, and the recovery objects of every namespace
    // repair when what they cover is in or what they waited for arrived.
    fn after_member(&mut self, volume: VolumeIndex) -> Result<(), GraphError> {
        let member = MemberId(volume.0);
        if !self.root.map.volume_waiting(volume) && !self.closed(member) {
            let mut out = Vec::new();
            self.writer.close(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                NodeId::ROOT,
                member,
                &mut out,
            )?;
            self.heard(out)?;
        }
        self.repair()
    }

    fn closed(&self, member: MemberId) -> bool {
        self.closed_in(NodeId::ROOT, member)
    }

    fn closed_in(&self, node: NodeId, member: MemberId) -> bool {
        self.tree
            .namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|record| record.closed)
    }

    fn is_recovery_volume(&self, member: MemberId) -> Option<NodeId> {
        self.tree
            .child_of(NodeId::ROOT, member)
            .map(|(child, _)| child)
            .filter(|child| self.tree.recovers(*child))
    }

    // A root member that is not a recovery volume still has articles to
    // come. Recovery volumes are left out: their articles stay parked until
    // a repair asks for them. Root members are numbered as the volumes they
    // are.
    fn payload_waiting(&self) -> bool {
        self.namespace().members().any(|member| {
            self.is_recovery_volume(member.id).is_none()
                && self.root.map.volume_waiting(VolumeIndex(member.id.0))
        })
    }

    // One round of seeks: every object's standing wants lifted to the root,
    // answered together so the window draws from all of them at once, and
    // the answers handed back. Returns the class changes for the queue.
    pub(crate) fn seek_round(
        &mut self,
        queued: impl FnMut(VolumeIndex) -> bool,
    ) -> Result<Vec<Move>, GraphError> {
        sample::bootstrap(&self.tree, &self.root.map, &mut self.root.window);
        let mut wants = Vec::new();
        self.tree.collect_wants(&mut wants);
        let mut held = Vec::new();
        for (node, seek) in &wants {
            self.writer.seek_held(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                *node,
                *seek,
                &mut held,
            )?;
        }
        self.heard(held)?;
        let mut asks = Vec::new();
        let mut lifted = Vec::with_capacity(wants.len());
        for (node, seek) in wants {
            let start = asks.len();
            let mut recorder = Recorder {
                asks: &mut asks,
                bidder: node,
                in_order: self.tree.reads_in_order(node),
            };
            let pull = self.tree.lift(node, seek, &mut recorder);
            lifted.push((node, seek, pull, start..asks.len()));
        }
        let answers = self.root.answer(&asks, queued);
        let mut effects = Vec::new();
        for (node, seek, pull, span) in lifted {
            let pull = answers[span]
                .iter()
                .fold(pull, |pull, answer| pull.fold(*answer));
            self.tree
                .pulled(node, seek, pull, &mut *self.builder, &mut effects);
        }
        self.settle(effects)?;
        Ok(self.root.map.take_moves())
    }

    // The phase, when it changed since the last call.
    pub(crate) fn phase_change(&mut self) -> Option<RootPhase> {
        let phase = self.phase();
        (self.reported != Some(phase)).then(|| {
            self.reported = Some(phase);
            phase
        })
    }

    fn settle(&mut self, effects: Vec<Effect>) -> Result<(), GraphError> {
        if effects.is_empty() {
            return Ok(());
        }
        let mut out = Vec::new();
        self.writer.apply_all(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            effects,
            &mut out,
        )?;
        self.heard(out)
    }

    // What the writer passed on that is the job's to answer: an object that
    // asks for a password is offered one, and what it says back is applied
    // and answered the same way.
    fn heard(&mut self, mut out: Vec<Effect>) -> Result<(), GraphError> {
        loop {
            let mut effects = Vec::new();
            for effect in out.drain(..) {
                if let Effect::NeedsPassword { node } = effect {
                    self.offer_password(node, &mut effects);
                }
            }
            if effects.is_empty() {
                return Ok(());
            }
            self.writer.apply_all(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                effects,
                &mut out,
            )?;
        }
    }
}

// Lifts a seek and keeps what reached the root, to be answered with the
// round's other seeks.
struct Recorder<'a> {
    asks: &'a mut Vec<Ask>,
    bidder: NodeId,
    in_order: bool,
}

impl SeekUp for Recorder<'_> {
    fn seek(&mut self, seek: Seek) -> Pull {
        self.asks.push(Ask {
            seek,
            bidder: self.bidder,
            in_order: self.in_order,
        });
        Pull::Moved(0)
    }
}
