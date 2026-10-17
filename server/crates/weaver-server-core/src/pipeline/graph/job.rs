// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One graph job's runtime: its tree, its article root, the builder its
// objects come from, and the namespace writer its bytes go through. The
// pipeline holds one per job admitted under the graph engine and hands it
// what the download and decode paths learn; nothing else of the pipeline
// reaches into the tree, and nothing else writes the job's files.

mod runs;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;
use weaver_graph::holds::HoldsAccountant;
use weaver_graph::recovery::{RepairOutcome, SliceVerdict};
use weaver_graph::{
    Builder, ByteRange, Effect, Kind, MemberId, Namespace, NodeId, Pull, Seek, SeekUp, Tree,
    VolumeIndex,
};

use super::Engine;
use super::engine::{Runs, Said};
use super::namespace::{NamespaceWriter, WriteError};
use super::root::intake::Decoded;
use super::root::phase::{self, RootPhase};
use super::root::sample;
use super::root::source::Move;
use super::root::{ArticleRoot, Ask};
use super::stand_in::EnginesForStores;
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
}

// What one recovery object says of the members it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RecoverySummary {
    // The tree node the object is. One PAR2 object holds every PAR2 set of
    // its namespace, so this is per object, not per set id.
    pub(crate) node: NodeId,
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
    // The phase last reported, so only a change is reported.
    reported: Option<RootPhase>,
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
            let writer = NamespaceWriter::new(
                scope,
                storage.working_dir,
                storage.accountant,
                storage.free_disk,
            );
            let runs = Runs::new(scope, storage.slots, storage.mail);
            Self::new(
                files,
                Box::new(EnginesForStores(weaver_objects::Objects)),
                writer,
                runs,
            )
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
            let member = tree.announce_root(&file.name, None);
            // A member that cannot be announced still downloads; its first
            // write reports the same failure where a step can carry it.
            if let Err(error) = writer.announced(
                &mut tree,
                &mut *builder,
                &root,
                NodeId::ROOT,
                member,
                &mut out,
            ) {
                warn!(member = member.0, error = %error, "graph member announcement failed");
            }
        }
        Self {
            tree,
            root,
            builder,
            writer,
            runs,
            reported: None,
        }
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
            tree.recovery_nodes(NodeId::ROOT)
                .into_iter()
                .map(|node| tree.recovery(node).and_then(|recovery| recovery.outcome())),
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

    // Seam for completion, status and health: each recovery object of the
    // root namespace, with the blocks it can reach and the slices it knows
    // are damaged. Stable.
    pub(crate) fn recovery_summary(&self) -> Vec<RecoverySummary> {
        let tree = &self.tree;
        let mut verdicts = Vec::new();
        tree.recovery_nodes(NodeId::ROOT)
            .into_iter()
            .filter_map(|node| {
                let (kind, recovery) = (tree.kind(node)?, tree.recovery(node)?);
                verdicts.clear();
                for member in self.namespace().members() {
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

    pub(crate) fn intake(&mut self, decoded: Decoded) -> Result<(), GraphError> {
        let volume = decoded.volume;
        let member = MemberId(volume.0);
        let length = decoded.length;
        let arrival = self.root.receive(decoded);
        if let Some(length) = length {
            self.writer
                .length_learned(&mut self.tree, &self.root, NodeId::ROOT, member, length)?;
        }
        self.writer.deliver(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            arrival,
            &mut Vec::new(),
        )?;
        self.mark_damage()?;
        self.after_member(volume)?;
        self.step_runs()
    }

    // Every server failed the article; nothing will fetch it again.
    pub(crate) fn lost(&mut self, volume: VolumeIndex, ordinal: u32) -> Result<(), GraphError> {
        self.root.map.lost(volume, ordinal);
        self.mark_damage()?;
        self.after_member(volume)?;
        self.step_runs()
    }

    // After an article of `volume` was settled: a member with nothing left
    // to fetch is closed, and the root's recovery objects repair when what
    // they cover is in or what they waited for arrived.
    fn after_member(&mut self, volume: VolumeIndex) -> Result<(), GraphError> {
        let member = MemberId(volume.0);
        if !self.root.map.volume_waiting(volume) && !self.closed(member) {
            self.writer.close(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                NodeId::ROOT,
                member,
                &mut Vec::new(),
            )?;
        }
        self.repair(member)
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

    // Let the root's recovery objects that have something to say verify and
    // repair: one that has not ruled on the members it covers rules once
    // they are all closed, and one waiting on its own volumes rules again
    // when `settled`, the member just settled, is one of them. What they
    // rewrote comes back down as repaired bytes; what they still need they
    // ask for in the next seek round.
    fn repair(&mut self, settled: MemberId) -> Result<(), GraphError> {
        let tree = &self.tree;
        let due = tree.recovery_nodes(NodeId::ROOT).into_iter().any(|node| {
            let Some(recovery) = tree.recovery(node) else {
                return false;
            };
            match recovery.outcome() {
                None => self
                    .namespace()
                    .members()
                    .all(|member| !recovery.covers(member.id) || member.closed),
                Some(RepairOutcome::Waiting) => {
                    self.is_recovery_volume(settled) == Some(node) && self.closed(settled)
                }
                Some(_) => false,
            }
        });
        if !due {
            return Ok(());
        }
        self.writer.repair(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            NodeId::ROOT,
            &mut Vec::new(),
        )?;
        Ok(())
    }

    // Ranges of lost articles become damage in the tree as soon as they can
    // be told; whatever covers them repairs them.
    fn mark_damage(&mut self) -> Result<(), GraphError> {
        let mut effects = Vec::new();
        for (volume, range) in self.root.damage() {
            self.tree.damage(NodeId::ROOT, volume, range, &mut effects);
            self.recovery_volume_lost(volume, range, &mut effects);
        }
        self.settle(effects)
    }

    // A lost range of a recovery object's own volume is gone for good, since
    // nothing recovers the recovery. The object hears it, or it waits on the
    // gap: a walk that never passes it finds none of the blocks beyond.
    // A data object is not told, as a repair may still bring its bytes back.
    fn recovery_volume_lost(
        &mut self,
        volume: VolumeIndex,
        range: ByteRange,
        effects: &mut Vec<Effect>,
    ) {
        let Some((node, inner)) = self.tree.child_of(NodeId::ROOT, MemberId(volume.0)) else {
            return;
        };
        if !self.tree.recovers(node) {
            return;
        }
        let Some(mut object) = self.tree.take_object(node) else {
            return;
        };
        let mut emits = Vec::new();
        object.volume_lost(inner, range, &mut emits);
        self.tree.put_object(node, object);
        self.tree
            .emitted(node, emits, &mut *self.builder, effects);
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
        self.writer.apply_all(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            effects,
            &mut Vec::new(),
        )?;
        Ok(())
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
