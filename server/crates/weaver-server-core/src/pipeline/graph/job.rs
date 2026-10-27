// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One graph job's runtime: its tree, its article root, the builder its
// objects come from, and the namespace writer its bytes go through. The
// pipeline holds one per job admitted under the graph engine and hands it
// what the download and decode paths learn; nothing else of the pipeline
// reaches into the tree, and nothing else writes the job's files.

mod durable;
mod loans;
mod passwords;
mod repair;
pub(crate) mod restore;
mod runs;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::mpsc;
use tracing::warn;
use weaver_graph::holds::HoldsAccountant;
use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{
    Builder, Effect, MemberId, Namespace, NodeId, Pull, Seek, SeekUp, Tree, VolumeIndex,
};

use self::loans::Loans;
use self::passwords::Passwords;
use super::checkpoint::Pending;
use super::engine::{Runs, Said};
use super::leaf::LeafError;
use super::limits::Caps;
use super::namespace::{NamespaceWriter, SetState, WriteError};
use super::root::intake::Decoded;
use super::root::phase::{self, Activity, RootPhase};
use super::root::sample;
use super::root::source::{Class, Move};
use super::root::{ArticleRoot, Ask};
use super::status::RepairSlots;
use crate::operations::disk::CapacityReader;
use crate::pipeline::ExtractionError;

#[derive(Debug, thiserror::Error)]
pub(crate) enum GraphError {
    #[error("the namespace write path failed")]
    Write(#[from] WriteError),
}

impl GraphError {
    // What the failure is to the job: a file's I/O error classed as the
    // extractors class theirs, a refusal as the refusal, anything else
    // terminal.
    pub(crate) fn to_extraction(&self) -> ExtractionError {
        let Self::Write(write) = self;
        let message = format!("{self}: {write}");
        match write {
            WriteError::Leaf(LeafError::Io { source, .. }) => {
                ExtractionError::from_io(message, source)
            }
            WriteError::Refused(failure) => ExtractionError::rejected(failure.clone()),
            WriteError::Leaf(_) | WriteError::UnknownMember { .. } => {
                ExtractionError::terminal(message)
            }
        }
    }
}

// Where a graph job writes and what it may hold: its working directory, the
// process-wide holds accountant every job shares, the free space of the
// volume its scratch lives on, how many engines it may run at once, and
// where their runs speak, and the extraction budget its writes are held to.
pub(crate) struct Storage {
    pub(crate) working_dir: PathBuf,
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) free_disk: CapacityReader,
    pub(crate) slots: usize,
    pub(crate) mail: mpsc::Sender<Said>,
    // The job's password candidates, in the order they are tried.
    pub(crate) passwords: Vec<String>,
    // The job's extraction budget.
    pub(crate) caps: Caps,
    // How deep the job's namespaces may nest, the root's being 0.
    pub(crate) nesting: u32,
    // What the job's runs and repairs are granted as each starts.
    pub(crate) hardware: super::hardware::Hardware,
    // Where the job's writes publish what they take, in a pipeline.
    pub(crate) metrics: Option<Arc<crate::operations::metrics::PipelineMetrics>>,
}

pub(crate) struct GraphJob {
    tree: Tree,
    root: ArticleRoot,
    builder: Box<dyn Builder + Send + Sync>,
    writer: NamespaceWriter,
    runs: Runs,
    loans: Loans,
    passwords: Passwords,
    // The phase last reported, so only a change is reported.
    reported: Option<RootPhase>,
    // The checkpoint waiting for the database, if one is.
    pending: Pending,
    // What the job did since the pipeline last asked, for its events.
    notes: Vec<Note>,
}

// Something a step of the job did that the job's events say.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Note {
    // Every article of a posted file was fetched or lost.
    Landed(VolumeIndex),
}

// A root member as the post lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RootFile {
    pub(crate) name: String,
    // Each article's ordinal and declared encoded size.
    pub(crate) articles: Vec<(u32, u32)>,
}

impl GraphJob {
    // The runtime a job admitted fresh gets. `scope` is the job's
    // process-wide id.
    pub(crate) fn admit(scope: u64, files: &[RootFile], storage: Storage) -> Self {
        let runs = Runs::new(
            scope,
            storage.slots,
            storage.mail,
            Arc::clone(&storage.accountant),
        )
        .on(storage.hardware);
        let writer = NamespaceWriter::new(
            scope,
            storage.working_dir,
            storage.accountant,
            storage.free_disk,
        )
        .capped(storage.caps)
        .metered(storage.metrics);
        Self::new(
            files,
            Box::new(runs.hardware()),
            writer,
            runs,
            storage.nesting,
        )
        .offering(storage.passwords)
    }

    pub(crate) fn new(
        files: &[RootFile],
        mut builder: Box<dyn Builder + Send + Sync>,
        mut writer: NamespaceWriter,
        runs: Runs,
        nesting: u32,
    ) -> Self {
        let mut tree = Tree::new();
        tree.nest_at_most(nesting);
        let root = ArticleRoot::new(files);
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
            loans: Loans::new(runs.scope(), runs.mail(), runs.hardware()),
            runs,
            passwords: Passwords::default(),
            reported: None,
            pending: Pending::default(),
            notes: Vec::new(),
        }
    }

    // What the job did since this was last asked.
    pub(crate) fn take_notes(&mut self) -> Vec<Note> {
        std::mem::take(&mut self.notes)
    }

    // The phase the job last reported, if it reported one.
    pub(crate) fn reported_phase(&self) -> Option<RootPhase> {
        self.reported
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

    // Whether something of the job is out on a thread and will speak
    // through the run mail: a run, or a repair.
    pub(in crate::pipeline) fn awaits_mail(&self) -> bool {
        self.runs.in_flight() || self.loans.out()
    }

    // Keep ownership until stopped workers report that they ended.
    pub(in crate::pipeline) fn request_stop(&self) {
        self.runs.request_stop();
    }

    // The pipeline is going down. Every run stops where it stands and keeps
    // its generation, so no run starts under it again: what it read is on
    // its shelf, and the restored job reads it again. A repair out on its
    // thread still comes back through the mail.
    pub(in crate::pipeline) fn stop_runs(&mut self, limit: usize) {
        self.runs.stop(limit);
    }

    // Seam for status: whether a repair of the job is out on a thread.
    pub(crate) fn repair_out(&self) -> bool {
        self.loans.out()
    }

    #[cfg(test)]
    pub(crate) fn set_builder(&mut self, builder: Box<dyn Builder + Send + Sync>) {
        self.builder = builder;
    }

    pub(crate) fn tree(&self) -> &Tree {
        &self.tree
    }

    pub(crate) fn writer(&self) -> &NamespaceWriter {
        &self.writer
    }

    pub(crate) fn pending(&self) -> &Pending {
        &self.pending
    }

    // Seam for completion, status and health: where the job stands, read
    // off the tree. Stable.
    pub(crate) fn phase(&self) -> RootPhase {
        let extracting = phase::extracting(&self.tree) || self.runs.live();
        RootPhase::derive(self.payload_waiting(), self.activity(), extracting)
    }

    // What the recovery sets are doing: repairing while a repair run is out
    // over damage a set covers, which a run that only checks is not.
    fn activity(&self) -> Activity {
        let repairing = self.loans.out() && self.writer.damage_covered(&self.tree);
        phase::activity(self.writer.outcomes(&self.tree), repairing)
    }

    // Whether the root keeps decoder checksums for the member's volume.
    #[cfg(test)]
    pub(crate) fn checksums_held(&self, member: MemberId) -> bool {
        self.root.checksums_held(VolumeIndex(member.0))
    }

    // Seam for status: each posted file's encoded bytes not yet downloaded,
    // in the post's file order.
    pub(crate) fn remaining_bytes(&self) -> Vec<u64> {
        self.root.map.remaining()
    }

    // Seam for status: the job is fetching repair data. Its payload is in,
    // no repair run is out, and a set waits on recovery volumes the root is
    // still fetching for it.
    pub(crate) fn fetching_repair_data(&self) -> bool {
        !self.payload_waiting() && self.activity() == Activity::Fetching
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

    // Seam for completion, status and health: each recovery set of every
    // namespace in the tree, parents first, as its namespace reads it.
    // Stable.
    pub(crate) fn recovery_summary(&self) -> Vec<SetState> {
        self.writer
            .states(&self.tree)
            .into_iter()
            .map(|state| self.loans.state(state.node).unwrap_or(state))
            .collect()
    }

    // Seam for health: why the post is dead, once the first-article sample
    // says so. A post whose first articles are lost past the sample's share
    // is not one that is going to complete, unless the recovery the root can
    // still reach covers every file the sample lost, each taken as wholly
    // lost. `None` while the sample has not ruled, or its losses are covered.
    pub(crate) fn first_articles_dead(&self) -> Option<String> {
        let (lost, total) = sample::first_article_verdict(self.root.payload_heads())?;
        let lost_bytes = lost
            .iter()
            .map(|volume| self.root.map.encoded_len(*volume))
            .fold(0u64, u64::saturating_add);
        (lost_bytes > self.recovery_reachable())
            .then(|| sample::first_article_verdict_error(lost.len(), total))
    }

    // Seam for health: what the post lost that its recovery can no longer
    // rebuild, once every posted file was sampled and none of its payload
    // can still come whole. A payload member can while it holds no damage,
    // or while a recovery object covering it still reaches as many blocks
    // as it has damaged slices. `None` while one can, or the sample is out:
    // a recovery volume joins its set by its first bytes, so one still to
    // come could bring the blocks a set lacks.
    pub(crate) fn shortfall(&self) -> Option<Shortfall> {
        use super::status::{RootJob, failure};
        if !self.root.map.sampled() {
            return None;
        }
        let objects: Vec<(NodeId, Option<RepairOutcome>)> = self
            .tree
            .recovery_nodes(NodeId::ROOT)
            .into_iter()
            .map(|node| (node, self.short(node)))
            .collect();
        let tree = &self.tree;
        let whole_may_come = self
            .namespace()
            .members()
            .filter(|member| {
                !self.writer.carries_recovery(tree, NodeId::ROOT, member.id)
                    && !self.root.declares_recovery(VolumeIndex(member.id.0))
            })
            .any(|member| {
                member.coverage.damaged_ranges().next().is_none()
                    || objects.iter().any(|(node, short)| {
                        short.is_none() && self.writer.covers(tree, *node, member.id)
                    })
            });
        if whole_may_come {
            return None;
        }
        // The sets short of blocks, in the words a ruling that came up short
        // fails the job with.
        let mut summary = RootJob::recovery_summary(self);
        summary
            .sets
            .retain(|set| objects.iter().any(|(node, _)| *node == set.node));
        let outcomes: Vec<RepairOutcome> = summary
            .sets
            .iter()
            .map(|set| {
                objects
                    .iter()
                    .find(|(node, _)| *node == set.node)
                    .and_then(|(_, short)| *short)
                    .unwrap_or(RepairOutcome::Waiting)
            })
            .collect();
        let sets = failure(&summary, &outcomes, self)
            .filter(|failure| failure.rejection.is_none())
            .map(|failure| failure.message);
        Some(Shortfall {
            sets,
            reachable: self.recovery_reachable(),
        })
    }

    // The recovery object at `node` cannot rebuild the damage the root's
    // members it covers hold: each slice a damaged range touches is one
    // block, counted at the object's largest slice, against every block its
    // sets reach. Both lean toward reaching: no slice is counted twice, and
    // a block of any of its sets counts. `None` while it can, or while it
    // knows no slice size to count at.
    fn short(&self, node: NodeId) -> Option<RepairOutcome> {
        let mut rulings = Vec::new();
        self.writer.rulings(&self.tree, node, &mut rulings);
        let slice = rulings
            .iter()
            .map(|ruling| ruling.slice_size)
            .max()
            .filter(|slice| *slice > 0)?;
        let reachable = rulings
            .iter()
            .map(|ruling| ruling.blocks_reachable)
            .fold(0u64, u64::saturating_add);
        let mut damaged = 0u64;
        for member in self.namespace().members() {
            if !self.writer.covers(&self.tree, node, member.id) {
                continue;
            }
            // Damaged ranges come in offset order; a slice two of them
            // touch is counted once.
            let mut counted: Option<u64> = None;
            for range in member.coverage.damaged_ranges() {
                if range.is_empty() {
                    continue;
                }
                let first = (range.start / slice).max(counted.map_or(0, |last| last + 1));
                let last = (range.end - 1) / slice;
                if first <= last {
                    damaged += last - first + 1;
                    counted = Some(last);
                }
            }
        }
        (damaged > reachable).then_some(RepairOutcome::Impossible { damaged, reachable })
    }

    // Seam for the health probe: the files the post names as payload that
    // lost at least one article. A lost recovery volume is not damage to the
    // release.
    pub(crate) fn failing_files(&self) -> usize {
        self.root
            .map
            .failing()
            .filter(|volume| !self.root.declares_recovery(*volume))
            .count()
    }

    // Seam for the health probe: every posted file was sampled.
    pub(crate) fn sampled(&self) -> bool {
        self.root.map.sampled()
    }

    // Seam for the health probe: whether the root's recovery sets reach the
    // blocks `failed` bytes over `failing_files` files would take, counted
    // generously: the bytes rounded up to whole slices, and one slice more
    // for each file, since a hole never starts on a slice boundary. No set
    // read yet covers nothing.
    pub(crate) fn recovery_covers(&self, failed: u64, failing_files: usize) -> bool {
        let mut rulings = Vec::new();
        for state in self.recovery_summary() {
            if state.namespace == NodeId::ROOT {
                self.writer.rulings(&self.tree, state.node, &mut rulings);
            }
        }
        let Some(slice) = rulings
            .iter()
            .map(|ruling| ruling.slice_size)
            .max()
            .filter(|slice| *slice > 0)
        else {
            return false;
        };
        let needed = failed.div_ceil(slice).saturating_add(failing_files as u64);
        let reachable = rulings
            .iter()
            .map(|ruling| ruling.blocks_reachable)
            .fold(0u64, u64::saturating_add);
        reachable >= needed
    }

    // Seam for health: whether the post names any recovery data.
    pub(crate) fn declares_recovery(&self) -> bool {
        self.root.declares_any_recovery()
    }

    // Seam for health: whether the recovery the post may still obtain could
    // cover `failed` bytes, `reachable` being the recovery bytes the root can
    // still reach. Once a set of the root's is read, its reachable blocks
    // are the ceiling. Before, a recovery file the post names that no object
    // has claimed may still bring one while its first article was not lost;
    // and a post none of whose recovery has been read or named says nothing
    // either way, so it may.
    pub(crate) fn recovery_may_cover(&self, failed: u64, reachable: u64) -> bool {
        let mut rulings = Vec::new();
        for state in self.recovery_summary() {
            if state.namespace == NodeId::ROOT {
                self.writer.rulings(&self.tree, state.node, &mut rulings);
            }
        }
        if !rulings.is_empty() {
            return reachable > 0 && failed <= reachable;
        }
        let mut named = self.root.map.heads().filter(|(volume, _)| {
            self.root.declares_recovery(*volume)
                && self
                    .tree
                    .child_of(NodeId::ROOT, MemberId(volume.0))
                    .is_none()
        });
        named.next().is_none() || reachable > 0
    }

    // The recovery bytes the root namespace can still reach: every set of
    // its recovery objects, its reachable blocks at its slice size, and each
    // file the post names recovery data that no object has claimed yet and
    // whose first article was not lost, at its declared size.
    pub(crate) fn recovery_reachable(&self) -> u64 {
        let mut rulings = Vec::new();
        for state in self.recovery_summary() {
            if state.namespace == NodeId::ROOT {
                self.writer.rulings(&self.tree, state.node, &mut rulings);
            }
        }
        let sets = rulings
            .iter()
            .map(|ruling| ruling.blocks_reachable.saturating_mul(ruling.slice_size))
            .fold(0u64, u64::saturating_add);
        self.root
            .map
            .heads()
            .filter(|(volume, lost)| {
                !lost
                    && self.root.declares_recovery(*volume)
                    && self
                        .tree
                        .child_of(NodeId::ROOT, MemberId(volume.0))
                        .is_none()
            })
            .map(|(volume, _)| self.root.map.encoded_len(volume))
            .fold(sets, u64::saturating_add)
    }

    // Seam for the article decoder: the block sizes the root namespace's
    // sets fold their evidence at. An article leased now has its checksums
    // cut at their boundaries, so a slice its bytes tile is proven as it
    // passes rather than read back. Stable.
    pub(crate) fn block_sizes(&self) -> Vec<u64> {
        self.writer.block_sizes(&self.tree, NodeId::ROOT)
    }

    // Both recovery formats report progress to the same bootstrap gate.
    // Missing or invalid indexes settle once no article remains outstanding.
    pub(crate) fn indexes_to_come(
        &self,
        declared: impl IntoIterator<Item = (MemberId, super::metadata::RecoveryFormat)>,
    ) -> Option<Vec<MemberId>> {
        use super::metadata::{MetadataState, RecoveryFormat, RecoveryMetadata};
        let mut metadata = RecoveryMetadata::default();
        let declared: Vec<_> = declared.into_iter().collect();
        let mut sampled: Vec<_> = declared.iter().map(|(_, format)| *format).collect();
        let discovered = self
            .tree
            .namespace(NodeId::ROOT)
            .into_iter()
            .flat_map(|namespace| {
                namespace.members().filter_map(|member| {
                    // Named recovery volumes remain optional. Obfuscated indexes
                    // discovered from their fronts use the same coordinator.
                    if weaver_graph::sniff::names_recovery(&member.name) {
                        return None;
                    }
                    let (child, _) = self.tree.child_of(NodeId::ROOT, member.id)?;
                    let format = RecoveryFormat::signed_index(
                        self.tree.kind(child)?,
                        namespace.first_bytes(member.id),
                    )?;
                    Some((member.id, format))
                })
            })
            .filter(|(_, format)| {
                // At most one unnamed candidate per format, even when every
                // optional recovery volume repeats metadata at its front.
                if sampled.contains(format) {
                    return false;
                }
                sampled.push(*format);
                true
            });
        for (member, format) in declared.into_iter().chain(discovered) {
            let state = if self.to_come(VolumeIndex(member.0)) {
                MetadataState::Unresolved
            } else if self
                .tree
                .child_of(NodeId::ROOT, member)
                .is_some_and(|(child, _)| self.tree.kind(child) == Some(format.kind()))
            {
                MetadataState::Ready
            } else {
                MetadataState::Unavailable
            };
            metadata.observe(member, format, state);
        }
        metadata.bootstrap_files()
    }

    // Every root member written to a file of its own, leaves and recovery
    // volumes alike, with the path it is written at. A member bound to an
    // object that places its bytes elsewhere has none.
    #[cfg(test)]
    pub(crate) fn member_files(&self) -> Vec<(MemberId, PathBuf)> {
        self.writer
            .files(NodeId::ROOT)
            .map(|(member, path)| (member, path.to_path_buf()))
            .collect()
    }

    // Seam for status: whether the job spends the root member rather than
    // hand it over, as the tree rules: recovery data, bound or named so, and
    // the volumes an object consumed to make what it hands over.
    pub(crate) fn spends(&self, member: MemberId) -> bool {
        self.tree.spends(NodeId::ROOT, member)
    }

    // Seam for status: the files that hold the bytes of every root member
    // the job spends, its own file or what the object that consumed it kept
    // of it. Once the job delivers they are not part of the release.
    pub(crate) fn spent_files(&self) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = self
            .writer
            .files(NodeId::ROOT)
            .filter(|(member, _)| self.spends(*member))
            .map(|(_, path)| path.to_path_buf())
            .collect();
        files.extend(self.namespace().records().filter_map(|record| {
            let (child, volume) = self.tree.child_of(NodeId::ROOT, record.id)?;
            self.spends(record.id)
                .then(|| self.writer.kept_file(child, volume))
                .flatten()
        }));
        files
    }

    // Articles the queues do not hold were fetched by an earlier run.
    pub(crate) fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool) {
        self.root.map.arrived_before(queued);
    }

    // Whether the root holds the article or has ruled it lost.
    pub(crate) fn article_settled(&self, volume: VolumeIndex, ordinal: u32) -> bool {
        self.root.map.article_settled(volume, ordinal)
    }

    pub(crate) fn article_lost(&self, volume: VolumeIndex, ordinal: u32) -> bool {
        self.root.map.article_lost(volume, ordinal)
    }

    pub(crate) fn ordered_front(&self, volume: VolumeIndex) -> (u32, u64, bool) {
        self.root.map.ordered_front(volume)
    }

    pub(crate) fn article_class(&self, volume: VolumeIndex, ordinal: u32) -> Option<Class> {
        self.root.map.class(volume, ordinal)
    }

    // The first article of every member leads the payload.
    pub(crate) fn sample(&mut self) -> Vec<Move> {
        self.root.map.sample();
        self.root.map.take_moves()
    }

    pub(crate) fn resume_runs(
        &mut self,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        self.step_runs(Vec::new(), limit)?;
        self.lend(slots, limit)
    }

    // `limit` is the slot limit the pipeline reads now, which every step
    // that asks or frees a run follows, and a repair the step lends asks
    // `slots` under it. The runs step before the lend, so a run the step
    // starts counts as reading when the walk asks whether a set is due.
    pub(crate) fn intake(
        &mut self,
        decoded: Decoded,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        let volume = decoded.volume;
        let member = MemberId(volume.0);
        let length = decoded.length;
        let arrival = self.root.receive(decoded);
        let mut out = Vec::new();
        if let Some(length) = length {
            self.writer.length_learned(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                (NodeId::ROOT, member, length),
                &mut out,
            )?;
        }
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
        self.step_runs(Vec::new(), limit)?;
        self.lend(slots, limit)
    }

    // Every server failed the article; nothing will fetch it again.
    pub(crate) fn lost(
        &mut self,
        volume: VolumeIndex,
        ordinal: u32,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        self.root.map.lost(volume, ordinal);
        self.mark_damage()?;
        self.after_member(volume)?;
        self.step_runs(Vec::new(), limit)?;
        self.lend(slots, limit)
    }

    // After an article of `volume` was settled: a member with nothing left
    // to fetch is closed. The walk after the step's runs rules what that
    // completed.
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
            self.notes.push(Note::Landed(volume));
        }
        Ok(())
    }

    fn closed(&self, member: MemberId) -> bool {
        self.namespace()
            .get(member)
            .is_some_and(|record| record.closed)
    }

    // A root member that is not a recovery volume still has articles to
    // come: articles the queue fetches without being asked. A parked
    // article comes only when something asks for it, so a volume nothing
    // will ask for, such as one whose opening article was lost before
    // anything could name it, holds nothing back. Recovery volumes are left
    // out: what a repair asks of them is the repair's. Root members are
    // numbered as the volumes they are; a further posting of a member is
    // a volume of its own whose articles are the member's.
    fn payload_waiting(&self) -> bool {
        self.namespace().records().any(|member| {
            !self
                .writer
                .carries_recovery(&self.tree, NodeId::ROOT, member.id)
                && self.to_come(VolumeIndex(member.id.0))
        })
    }

    // Whether the volume has articles the queue fetches unasked still to
    // come.
    pub(crate) fn to_come(&self, volume: VolumeIndex) -> bool {
        self.root.map.volume_to_come(volume)
    }

    // One round of seeks: every object's standing wants lifted to the root,
    // answered together so the window draws from all of them at once, and
    // the answers handed back. Returns the class changes for the queue.
    pub(crate) fn seek_round(
        &mut self,
        queued: impl FnMut(VolumeIndex) -> bool,
    ) -> Result<Vec<Move>, GraphError> {
        sample::indexes(&self.root.indexes, &self.root.map, &mut self.root.window);
        sample::bootstrap(&self.tree, &self.root.map, &mut self.root.window);
        sample::fronts(&self.tree, &mut self.root.map);
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
            self.tree.pulled(
                node,
                seek,
                pull,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
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
    // and answered the same way. An offer goes through the writer even when
    // the object says nothing back: what it can place under the password
    // changed, and the writer is where held bytes go down.
    fn heard(&mut self, mut out: Vec<Effect>) -> Result<(), GraphError> {
        loop {
            let mut effects = Vec::new();
            let mut offered = false;
            for effect in out.drain(..) {
                if let Effect::NeedsPassword { node } = effect {
                    offered |= self.offer_password(node, &mut effects);
                }
            }
            if !offered {
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

// What the post lost past its recovery, as `GraphJob::shortfall` reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Shortfall {
    // The recovery sets short of blocks for what they cover, worded as the
    // job's failure; `None` when no set covers what was lost.
    pub(crate) sets: Option<String>,
    // Every recovery byte the root can still reach.
    pub(crate) reachable: u64,
}
