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
use weaver_graph::{
    Builder, Effect, MemberId, Namespace, NodeId, Pull, Seek, SeekUp, Tree, VolumeIndex,
};

use self::loans::Loans;
use self::passwords::Passwords;
use super::Engine;
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
            )
            .capped(storage.caps);
            Self::new(
                files,
                Box::new(weaver_objects::Objects),
                writer,
                runs,
                storage.nesting,
            )
            .offering(storage.passwords)
        })
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
            loans: Loans::new(runs.scope(), runs.mail()),
            runs,
            passwords: Passwords::default(),
            reported: None,
            pending: Pending::default(),
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

    // Whether something of the job is out on a thread and will speak
    // through the run mail: a run, or a repair.
    pub(in crate::pipeline) fn awaits_mail(&self) -> bool {
        self.runs.in_flight() || self.loans.out()
    }

    // Keep ownership until stopped workers report that they ended.
    pub(in crate::pipeline) fn request_stop(&self) {
        self.runs.request_stop();
    }

    // The pipeline is going down. Every run stops where it stands and is
    // forgotten: what it read is on its shelf, and the restored job reads it
    // again. A repair out on its thread still comes back through the mail.
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

    // Seam for the article decoder: the block sizes the root namespace's
    // sets fold their evidence at. An article leased now has its checksums
    // cut at their boundaries, so a slice its bytes tile is proven as it
    // passes rather than read back. Stable.
    pub(crate) fn block_sizes(&self) -> Vec<u64> {
        self.writer.block_sizes(&self.tree, NodeId::ROOT)
    }

    // Seam for the download scheduler: of the root members the post
    // declares as indexes, and those whose fronts say they are one whatever
    // their names, the ones the root namespace holds its payload for.
    // `None` once it holds for none.
    pub(crate) fn indexes_to_come(
        &self,
        declared: impl IntoIterator<Item = MemberId>,
    ) -> Option<Vec<MemberId>> {
        let mut indexes: Vec<MemberId> = declared.into_iter().collect();
        indexes.extend(self.tree.signed_indexes(NodeId::ROOT));
        indexes.sort_unstable();
        indexes.dedup();
        let held = self
            .writer
            .holding_for(indexes, |member| self.to_come(VolumeIndex(member.0)));
        (!held.is_empty()).then_some(held)
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

    // The class the root fetches an article under now.
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
