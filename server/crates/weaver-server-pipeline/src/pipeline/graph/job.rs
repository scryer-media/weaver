// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One graph job's runtime: its tree, its article root, the builder its
// objects come from, and the namespace writer its bytes go through. The
// pipeline holds one per job admitted under the graph engine and hands it
// what the download and decode paths learn; nothing else of the pipeline
// reaches into the tree, and nothing else writes the job's files.

mod durable;
pub mod loans;
pub mod passwords;
mod repair;
pub mod restore;
mod runs;
pub use runs::discard_unheard;
pub mod works;

use std::collections::BTreeMap;
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
pub(crate) use self::works::Worked;
use self::works::Works;
use super::checkpoint::Pending;
use super::engine::{Runs, Said};
use super::leaf::LeafError;
use super::limits::{Caps, Device};
use super::namespace::{Coming, NamespaceWriter, SetState, WriteError};
use super::root::intake::Decoded;
use super::root::phase::{self, Activity, RootPhase};
use super::root::sample;
use super::root::source::{Class, Inconsistent, Move};
use super::root::{ArticleRoot, Ask};
use super::status::RepairSlots;
use crate::pipeline::ExtractionError;
use weaver_server_disk::operations::disk::CapacityReader;

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("the namespace write path failed")]
    Write(#[from] WriteError),
    // The job's own record contradicts itself: it fails, and every other
    // job runs on.
    #[error("the graph job's record is inconsistent")]
    Inconsistent(#[from] Inconsistent),
}

impl GraphError {
    // The device a write found no room on, when that is all that failed:
    // the job waits for room there rather than failing.
    pub fn out_of_space(&self) -> Option<Device> {
        match self {
            Self::Write(WriteError::Leaf(LeafError::Io { source, device, .. }))
                if crate::operations::is_out_of_space(source) =>
            {
                Some(*device)
            }
            _ => None,
        }
    }

    // Whether the write that found no room was refused by a quota.
    pub(crate) fn quota(&self) -> bool {
        matches!(
            self,
            Self::Write(WriteError::Leaf(LeafError::Io { source, .. }))
                if crate::operations::is_quota_exceeded(source)
        )
    }

    // What the failure is to the job: a file's I/O error classed as the
    // extractors class theirs, and named as a full disk when it is one, a
    // refusal as the refusal, anything else terminal.
    pub(crate) fn to_extraction(&self) -> ExtractionError {
        let write = match self {
            Self::Write(write) => write,
            Self::Inconsistent(inconsistent) => {
                return ExtractionError::terminal(format!("{self}: {inconsistent}"));
            }
        };
        let message = format!("{self}: {write}");
        match write {
            WriteError::Leaf(LeafError::Io { source, .. }) => {
                let message = if crate::operations::is_out_of_space(source) {
                    format!("{message}: filesystem out of space or over quota")
                } else {
                    message
                };
                ExtractionError::from_io(message, source)
            }
            WriteError::Refused(failure) => ExtractionError::rejected(failure.clone()),
            WriteError::LeftAtStop
            | WriteError::UnknownMember { .. }
            | WriteError::StaleRecovery { .. }
            | WriteError::ProofUnreadable { .. } => ExtractionError::terminal(message),
        }
    }
}

// Where a graph job writes and what it may hold: its working directory, the
// process-wide holds accountant and decode pool every job shares, the free
// space of the volume its scratch lives on, how many engines it may run at
// once, and where their runs speak, and the extraction budget its writes are
// held to.
pub struct Storage {
    pub working_dir: PathBuf,
    // Where the files of the members it delivers lie, and the free space of
    // that device: the working directory unless the complete directory is
    // on another device.
    pub output: Option<(PathBuf, CapacityReader)>,
    pub accountant: Arc<HoldsAccountant>,
    pub decode: Arc<super::engine::DecodePool>,
    pub free_disk: CapacityReader,
    pub slots: usize,
    pub mail: mpsc::Sender<Said>,
    // The job's password candidates, in the order they are tried, each with
    // where it came from.
    pub passwords: Vec<crate::jobs::ArchivePasswordCandidate>,
    // The job's extraction budget.
    pub caps: Caps,
    // How deep the job's namespaces may nest, the root's being 0.
    pub nesting: u32,
    // What the job's runs and repairs are granted as each starts.
    pub hardware: super::hardware::Hardware,
    // Where the job's writes publish what they take, in a pipeline.
    pub metrics: Option<Arc<weaver_server_base::operations::metrics::PipelineMetrics>>,
    // The volume writers the job's files are written through; `None` for a
    // job that writes its files itself as each byte is placed.
    pub disk: Option<super::disk::JobDisk>,
    // Weaver's data folder and the free space of its disk: where a decode
    // window goes when the working directory cannot hold it.
    pub data_dir: Option<(
        PathBuf,
        CapacityReader,
        weaver_server_disk::scratch::ScratchLedger,
    )>,
    // What decode windows took from the working directory's disk, for
    // every job that lends on it.
    pub window_ledger: weaver_server_disk::scratch::ScratchLedger,
}

impl Storage {
    // The scratch memory job `scope` may borrow: scratch files in its
    // working directory, else in the data folder, out of the free space the
    // holds keep.
    pub fn scratch(&self, scope: u64) -> weaver_server_disk::scratch::Scratch {
        let scratch = weaver_server_disk::scratch::Scratch::new(
            self.working_dir.clone(),
            self.free_disk.clone(),
            self.window_ledger.clone(),
            Arc::clone(&self.accountant),
            weaver_server_disk::scratch::CEILING_BYTES,
        );
        match &self.data_dir {
            Some((dir, free_disk, ledger)) => {
                scratch.or_spare(dir.clone(), scope, free_disk.clone(), ledger.clone())
            }
            None => scratch,
        }
    }

    // The decode windows job `scope`'s runs may borrow, lent from its
    // scratch.
    pub(crate) fn windows(&self, scope: u64) -> Arc<super::window::Windows> {
        Arc::new(super::window::Windows::new(Arc::new(self.scratch(scope))))
    }
}

pub struct GraphJob {
    pub tree: Tree,
    pub root: ArticleRoot,
    builder: Box<dyn Builder + Send + Sync>,
    pub writer: NamespaceWriter,
    pub runs: Runs,
    pub loans: Loans,
    works: Works,
    pub passwords: Passwords,
    // The phase last reported, so only a change is reported.
    pub reported: Option<RootPhase>,
    // Whether the job reported Repairing since it was added, in this run or
    // one its checkpoint was taken in: its end says a repair failed only then.
    repair_reported: bool,
    // When, epoch milliseconds, the job last left Repairing in this run: the
    // end of the repair a failure at the job's end says. Not kept across a
    // restart; a job restored past its repair has none.
    repair_ended_at: Option<i64>,
    // The checkpoint waiting for the database, if one is.
    pending: Pending,
    // A member's file waits to move on a checkpoint the database has not
    // said it committed, and the run mail will say when it has.
    mail_owed: Option<u64>,
    // A thread is counting on the job's volumes what a release of held bytes
    // stands for (`pay_release`), and will mail it (`Heard::ReleasePaid`).
    releasing: bool,
    // A repair the stop ended while a volume's writer still held operations
    // of the job, kept until the stop has given that writer its time.
    stopped_repair: Option<Box<crate::pipeline::graph::namespace::Returned>>,
    // How many repairs the stop ended were kept and then taken back.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    stopped_repairs_taken_back: u32,
    // The checkpoints committed through this stamp while a rewalk read its
    // volumes' files where they lay as it was lent: what they move or
    // remove waits for the last piece to be heard.
    commit_held: Option<u64>,
    // What the job did since the pipeline last asked, for its events.
    notes: Vec<Note>,
    // What the steps brought the engines' shelves up to.
    fed: runs::Fed,
    // Where root members were last asked whether the sets proved them;
    // `None` before the first.
    sealed_at: Option<repair::Sealed>,
    // The give-up the job went quiet into, while it has volumes to read
    // back, one a turn.
    quieting: Option<repair::Quieting>,
    // Every give-up is applied in the one turn it was raised in.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    one_turn: bool,
    // Counts the mutable borrows of this job: nothing of it changes without
    // one, so what was derived from it at a count holds while the count does.
    moved: u64,
    // The recovery indexes still to come as of a count.
    indexes_kept: Option<(u64, Option<Arc<[u32]>>)>,
}

// Something a step of the job did that the job's events say.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Note {
    // Every article of a posted file was fetched or lost.
    Landed(VolumeIndex),
    // A set's repair came back and was applied, rebuilding these slices.
    Repaired(u64),
    // An archive was given up on as an archive for this member.
    GivenBack(super::namespace::GivenBack),
    // An article the root held is no longer where it was put and is
    // fetched again.
    Unread(VolumeIndex, u32),
}

// A root member as the post lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RootFile {
    pub name: String,
    // Each article's ordinal and declared encoded size.
    pub articles: Vec<(u32, u32)>,
}

impl GraphJob {
    // The runtime a job admitted fresh gets. `scope` is the job's
    // process-wide id.
    pub fn admit(scope: u64, files: &[RootFile], storage: Storage) -> Self {
        let windows = storage.windows(scope);
        let runs = Runs::new(scope, storage.slots, storage.mail, storage.decode)
            .on(storage.hardware)
            .lending(windows)
            .writing(storage.disk.clone());
        let writer = NamespaceWriter::new(
            scope,
            storage.working_dir,
            storage.accountant,
            storage.free_disk,
        )
        .delivering(storage.output)
        .writing_through(storage.disk)
        .capped(storage.caps)
        .metered(storage.metrics);
        Self::new(
            files,
            Box::new(runs.hardware()),
            writer,
            runs,
            storage.nesting,
        )
        .offering_candidates(storage.passwords)
    }

    pub fn new(
        files: &[RootFile],
        mut builder: Box<dyn Builder + Send + Sync>,
        mut writer: NamespaceWriter,
        runs: Runs,
        nesting: u32,
    ) -> Self {
        let mut tree = Tree::new();
        tree.nest_at_most(nesting);
        // A member with an unwanted extension is never opened.
        tree.never_open(writer.never_opens());
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
        // The deadline counts the time the runs and the loans decode.
        let runs = runs.timed(writer.decode_clock());
        let mut job = Self {
            tree,
            root,
            builder,
            writer,
            loans: Loans::new(runs.scope(), runs.mail(), runs.hardware()),
            works: Works::new(runs.scope(), runs.mail(), runs.hardware()).timed(runs.clock()),
            runs,
            passwords: Passwords::default(),
            reported: None,
            repair_reported: false,
            repair_ended_at: None,
            pending: Pending::default(),
            mail_owed: None,
            releasing: false,
            stopped_repair: None,
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            stopped_repairs_taken_back: 0,
            commit_held: None,
            notes: Vec::new(),
            fed: runs::Fed::default(),
            sealed_at: None,
            quieting: None,
            #[cfg(any(test, feature = "pipeline-test-fixtures"))]
            one_turn: false,
            moved: 0,
            indexes_kept: None,
        };
        // What the announcements released may stand for room.
        job.pay_release();
        job
    }

    // The job is about to be borrowed mutably: what was derived from it no
    // longer holds unasked.
    pub(crate) fn touched(&mut self) {
        self.moved = self.moved.wrapping_add(1);
    }

    // [`Self::indexes_to_come`] by posted file number, as one shared list
    // for as long as the job was not borrowed mutably. A borrow that left
    // the answer as it was keeps the list it had, so its holders still
    // match by pointer. The spec's declared roles do not change after the
    // job is built, so `declared` is read only when the list is made again;
    // a debug build makes it again on every ask and checks the kept one.
    pub(crate) fn indexes_to_come_kept(
        &mut self,
        declared: impl IntoIterator<Item = (MemberId, super::metadata::RecoveryFormat)>,
    ) -> Option<Arc<[u32]>> {
        let numbered =
            |held: Vec<MemberId>| -> Vec<u32> { held.into_iter().map(|member| member.0).collect() };
        if let Some((at, kept)) = &self.indexes_kept
            && *at == self.moved
        {
            debug_assert_eq!(
                self.indexes_to_come(declared).map(numbered).as_deref(),
                kept.as_deref(),
                "the recovery indexes to come changed without a mutable borrow of the job"
            );
            return kept.clone();
        }
        let now = self.indexes_to_come(declared).map(numbered);
        let kept = match (self.indexes_kept.take(), now) {
            (Some((_, Some(kept))), Some(now)) if *kept == *now => Some(kept),
            (_, now) => now.map(Arc::from),
        };
        self.indexes_kept = Some((self.moved, kept.clone()));
        kept
    }

    // The bytes promised on the disks when the job started, `working` on
    // the working disk and `output` on the complete one: its downloaded
    // bytes land against them.
    pub(crate) fn take_posted(&mut self, working: u64, output: u64) {
        self.writer.take_posted(working, output);
    }

    // What the start promised and did not land goes back.
    pub(crate) fn release_posted(&mut self) {
        self.writer.release_posted();
    }

    // What the start promised and has not landed, working and output.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn posted(&self) -> [u64; 2] {
        self.writer.posted()
    }

    // What the job did since this was last asked.
    pub fn take_notes(&mut self) -> Vec<Note> {
        let mut notes = std::mem::take(&mut self.notes);
        notes.extend(
            self.writer
                .take_given_back()
                .into_iter()
                .map(Note::GivenBack),
        );
        notes
    }

    // The phase the job last reported, if it reported one.
    pub(crate) fn reported_phase(&self) -> Option<RootPhase> {
        self.reported
    }

    // Whether the job reported Repairing since it was added.
    pub(crate) fn repair_reported(&self) -> bool {
        self.repair_reported
    }

    // When the job last left Repairing in this run, epoch milliseconds.
    pub(crate) fn repair_ended_at(&self) -> Option<i64> {
        self.repair_ended_at
    }

    // The candidates offered to an object that asks for a password.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn offering(mut self, candidates: Vec<String>) -> Self {
        self.passwords = Passwords::new(candidates);
        self
    }

    // The candidates offered to an object that asks for a password, each
    // with its source, which the download summary reads back.
    pub(crate) fn offering_candidates(
        mut self,
        candidates: Vec<crate::jobs::ArchivePasswordCandidate>,
    ) -> Self {
        self.passwords = Passwords::from_candidates(candidates);
        self
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn runs(&self) -> &Runs {
        &self.runs
    }

    // Whether mail is still owed by anything but runs that each wait on
    // their shelf for a change that has not come, and what rings as a run
    // begins to wait.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn awaits_mail_unless_blocked(&self) -> bool {
        self.loans.out()
            || self.works.out()
            || self.mail_owed.is_some()
            || self.runs.running().any(|node| {
                self.writer
                    .shelf_of(node)
                    .is_none_or(|shelf| !shelf.blocked())
            })
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn blocked(&self) -> std::sync::Arc<tokio::sync::Notify> {
        self.writer.blocked()
    }

    // How far the node's run read into each of its volumes, by index.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn run_reached(&self, node: NodeId) -> BTreeMap<u32, u64> {
        self.writer
            .shelf_of(node)
            .map_or_else(BTreeMap::new, |shelf| {
                shelf
                    .lock()
                    .kept
                    .iter()
                    .map(|(volume, kept)| (*volume, kept.reached))
                    .collect()
            })
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn current_repair(&self, said: &Said) -> bool {
        said.scope == self.runs.scope() && self.loans.current_repair(said.node, said.generation)
    }

    // Whether a repair, replay or check of the job is out on a thread.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn lent(&self) -> bool {
        self.loans.out()
    }

    // Whether an object of the job is out for its work, a give-up has
    // volumes left to read back, or a repair's return is being applied. No
    // checkpoint is taken meanwhile: its facts as it left hold the work
    // undone, which a restart could not always do without the tree, and a
    // tree half given up or half repaired is no state to restore. A restart
    // gives up or repairs again from the checkpoint before.
    pub fn working(&self) -> bool {
        self.held_by_work() || self.loans.applying()
    }

    // Whether an object of the job is out for its work or a give-up has
    // volumes left: what the loop takes back before the next article. A
    // return being applied speaks through the mail as a repair does.
    pub fn held_by_work(&self) -> bool {
        self.works.working() || self.quieting.is_some()
    }

    // Every give-up from here on is applied in the turn it was raised in.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn give_up_in_one_turn(&mut self) {
        self.one_turn = true;
    }

    // Every work loan from here on stays in the test's hand.
    #[cfg(any(test, feature = "test-support"))]
    pub fn hold_loans(&mut self) {
        self.works.hold_loans();
    }

    // How many objects went out for their work.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn loans_lent(&self) -> u64 {
        self.works.lent()
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn held_loans(&mut self) -> Vec<works::HeldLoan> {
        self.works.take_held()
    }

    // A held loan does its work and comes back through the run mail, as one
    // from the pool does.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn return_loan(
        &mut self,
        loan: works::HeldLoan,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        let said = self.worked_loan(loan);
        self.run_said(said, slots, limit)
    }

    // A held loan's work done, and the word it comes back with through the
    // run mail, as one from the pool does.
    #[cfg(any(test, feature = "test-support"))]
    pub fn worked_loan(
        &self,
        (node, serial, mut object, stop): works::HeldLoan,
    ) -> super::engine::Said {
        let mut said = Vec::new();
        {
            let _decoding = self.works.decoding();
            object.work_until(&stop, &mut said);
        }
        super::engine::Said {
            scope: self.works.scope(),
            node,
            generation: serial,
            heard: super::engine::Heard::Worked(Box::new(Worked { object, said })),
        }
    }

    // A checkpoint found an object out for its work and took nothing: the
    // work stays on the loop until one is recorded.
    pub fn owe_checkpoint(&mut self) {
        self.works.owe_checkpoint();
    }

    // Whether a checkpoint that found an object out is owed still.
    pub fn owes_checkpoint(&self) -> bool {
        self.works.owed()
    }

    // Whether something of the job is out on a thread and will speak
    // through the run mail: a run, or a repair.
    pub fn awaits_mail(&self) -> bool {
        self.runs.in_flight() || self.loans.out() || self.works.out() || self.mail_owed.is_some()
    }

    // Whether a stopped job must be kept: mail of it is still to come, or a
    // volume's writer still holds an operation on its files. Nothing may
    // remove, move or hand on those files before both are over.
    pub fn holds_retirement(&self) -> bool {
        self.awaits_mail() || self.writer.disk().is_some_and(|disk| !disk.quiet())
    }

    // The job's files are given up: nothing still queued for them runs.
    pub fn abandon_disk(&self) {
        if let Some(disk) = self.writer.disk() {
            disk.abandon();
        }
    }

    // The checkpoint `stamp` records every name the job's files want and
    // every name they left so far.
    pub fn recorded(&mut self, stamp: u64) {
        self.writer.recorded(stamp);
        self.works.checkpointed();
    }

    // Whether a member's file still waits to move, or a name a file left
    // waits to be free: the job is not done until every file sits at its
    // name.
    pub fn moves_waiting(&self) -> bool {
        self.writer.moves_waiting()
    }

    // The job is ready to deliver: each file moves to the name the ledger
    // gives its member now, as the checkpoints that record it commit.
    pub fn deliver_names(&mut self) {
        self.writer.deliver_names();
    }

    // Whether the job's files have begun moving to their names.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn delivering_names(&self) -> bool {
        self.writer.delivering_names()
    }

    // Whether a run's refusal stands only because the policy holds runs to
    // the profile's ceiling, so a reprocess with no memory limit could
    // finish what it withheld.
    pub(crate) fn held_to_ceiling(&self) -> bool {
        self.runs.held_to_ceiling()
    }

    // Whether anything of the job waits for a checkpoint to commit. Not
    // while a rewalk reads: no file moves under it, so nothing hurries a
    // checkpoint on, and the first one after its last piece commits.
    pub fn awaits_commit(&self) -> bool {
        self.writer.awaits_commit() && !self.works.rewalks_out()
    }

    // The checkpoint whose commit lets every waiting move run.
    pub(crate) fn awaited(&self) -> u64 {
        self.writer.awaited()
    }

    // The run mail will say when the awaited checkpoint commits. True when
    // it was not owed already, so no write queued yet is sure to say it.
    pub fn owe_mail(&mut self, stamp: u64) -> bool {
        self.mail_owed.replace(stamp).is_none()
    }

    // The checkpoints through `stamp` have committed: every move they
    // record runs, and the names the files left are free in the ledger.
    // True when the next checkpoint has something to record.
    pub fn committed(&mut self, stamp: u64) -> Result<bool, WriteError> {
        // No file moves or goes under a rewalk reading it: the commit waits
        // for the last piece, and runs then.
        let stamp = self
            .commit_held
            .take()
            .map_or(stamp, |held| held.max(stamp));
        if self.works.rewalks_out() {
            self.commit_held = Some(stamp);
            return Ok(false);
        }
        self.writer.commit_moves(&mut self.tree, stamp)
    }

    // The database's word on the checkpoints a move waited on: committed
    // through `stamp`, or a write failed and the next checkpoint carries
    // them again.
    fn acknowledge_checkpoint(&mut self, stamp: u64) {
        if self.mail_owed.is_some_and(|owed| owed <= stamp) {
            self.mail_owed = None;
        }
    }

    pub(crate) fn written(&mut self, stamp: Option<u64>, requested: u64) -> Result<(), GraphError> {
        self.acknowledge_checkpoint(stamp.unwrap_or(requested));
        if let Some(stamp) = stamp {
            self.committed(stamp)?;
        }
        Ok(())
    }

    // The file system refused the name of a file a volume's writer was to
    // make: its member is skipped as damaged, as a refusal met while
    // writing it would skip it.
    //
    // False when the refusal skips nothing and stands as a failure: the
    // job has no such file, or an object reads the member.
    pub(crate) fn name_refused(&mut self, file: u64) -> Result<bool, GraphError> {
        let mut out = Vec::new();
        let Some(node) = self.writer.name_refused(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            file,
            &mut out,
        )?
        else {
            return Ok(false);
        };
        // The object that made the member left it out: what it unpacked
        // is short of what it holds, by bytes nothing brings. A run that
        // meets such bytes refuses its object as it ends; this refusal may
        // come after the run has, so the object is refused here, and its
        // volumes are handed over as they stand.
        if node != NodeId::ROOT {
            self.writer.said(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                node,
                weaver_graph::Emit::Refuse(weaver_graph::Why::Unrepairable),
                &mut out,
            )?;
        }
        self.heard(out)?;
        Ok(true)
    }

    // Keep ownership until stopped workers report that they ended. A
    // give-up's volumes not yet read back are not: the job goes.
    pub fn request_stop(&mut self) {
        self.quieting = None;
        self.runs.request_stop();
        self.loans.request_stop();
        self.works.request_stop();
    }

    // The pipeline is going down. Every run stops where it stands and keeps
    // its generation, so no run starts under it again: what it read is on
    // its shelf, and the restored job reads it again. A repair out on its
    // thread still comes back through the mail.
    pub fn stop_runs(&mut self, limit: usize) {
        self.loans.request_stop();
        self.runs.stop(limit);
    }

    // Whether a run or a repair of the job is still out on its thread.
    pub fn workers_out(&self) -> bool {
        self.runs.in_flight() || self.loan_out()
    }

    // Whether a repair, a replay or a verification of the job is out on its
    // thread. A repair and a replay write the job's files as they go.
    pub(in crate::pipeline) fn loan_out(&self) -> bool {
        self.loans.out()
    }

    // Whether a repair's return is being applied a slice at a time.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn applying_repair(&self) -> bool {
        self.loans.applying()
    }

    // How many slices of the return being applied were placed so far.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn slices_applied(&self) -> u64 {
        self.writer.slices_applied()
    }

    // Seam for status: whether a repair of the job is out on a thread.
    pub fn repair_out(&self) -> bool {
        self.loans.repairing()
    }

    // Completion waits for every worker's acknowledgment and for replays
    // or checks queued behind admission, not just actual recovery work.
    pub fn work_pending(&self) -> bool {
        self.loans.out()
            || self.works.out()
            || self.quieting.is_some()
            || self.tree.rewalks_owed()
            || self.writer.rechecks_waiting()
            || self.verifications_due()
            || self.tree.work_due()
            || self.releasing
            || self.writer.release_owed().is_some()
            || self.writer.closes_owed()
    }

    // A release of held bytes stands for room on the job's volumes
    // (`NamespaceWriter::release_owed`): a thread counts it, off the loop,
    // and the release goes on as its credit is heard (`Heard::ReleasePaid`).
    // One at a time; while a return's tail is on, the tail is what goes on,
    // and what is owed is paid once it ends.
    pub(crate) fn pay_release(&mut self) {
        if self.releasing || self.writer.tail_into().is_some() {
            return;
        }
        let (Some((bytes, _)), Some(disk)) = (self.writer.release_owed(), self.writer.disk())
        else {
            return;
        };
        self.loans.pay_release(bytes, disk.clone());
        self.releasing = true;
    }

    // A checksum obligation something can still meet: one an ended
    // namespace can no longer check is not work.
    fn verifications_due(&self) -> bool {
        self.writer.verifications_due(&self.tree, &self.coming())
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn set_builder(&mut self, builder: Box<dyn Builder + Send + Sync>) {
        self.builder = builder;
    }

    pub fn tree(&self) -> &Tree {
        &self.tree
    }

    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn namespace_mut(&mut self, node: NodeId) -> Option<&mut Namespace> {
        self.tree.namespace_mut(node)
    }

    pub fn writer(&self) -> &NamespaceWriter {
        &self.writer
    }

    // The job paused or resumed: its extraction deadline does not run
    // while it is paused.
    pub fn hold_clock(&mut self, paused: bool) {
        self.writer.hold_clock(paused);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn clock_held(&self) -> bool {
        self.writer.clock_held()
    }

    // Read the job's content policy if it is still unread, and judge the
    // names that waited for it.
    pub(crate) fn learn_policy(&mut self) -> Result<(), String> {
        self.writer.learn_policy()
    }

    // Once the policy is read, the members the tree held undecided while it
    // was not are asked again, once, so an archive whose name the policy
    // does not list opens then. Nothing before the policy is read; nothing
    // to do for a job whose tree held none. True when what it applied made
    // work due that was not, as a check of a file it closed: the caller
    // lends it, as every step lends what it made due.
    pub(crate) fn open_after_policy(&mut self) -> Result<bool, GraphError> {
        if !self.writer.policy_known() {
            return Ok(false);
        }
        let pending = self.work_pending();
        let mut out = Vec::new();
        self.writer
            .rule_known(&mut self.tree, &mut *self.builder, &self.root, &mut out)?;
        self.heard(out)?;
        Ok(!pending && self.work_pending())
    }

    // Everything the job has submitted to its volumes' writers so far;
    // `None` for a job that writes its files itself.
    pub(crate) fn disk_barrier(&self) -> Option<super::disk::JobBarrier> {
        self.writer.disk().map(|disk| disk.barrier())
    }

    pub fn disk(&self) -> Option<&super::disk::JobDisk> {
        self.writer.disk()
    }

    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(crate) fn move_disk_for_test(&mut self, disk: super::disk::JobDisk) {
        self.writer.move_disk_for_test(disk);
    }

    pub fn pending(&self) -> &Pending {
        &self.pending
    }

    // Seam for completion, status and health: where the job stands, read
    // off the tree. Stable. Payload still to fetch wins over everything
    // else, so while it is there, which is every article but the last, the
    // walks of the sets and members are not taken.
    pub fn phase(&self) -> RootPhase {
        if self.payload_waiting() {
            return RootPhase::Downloading;
        }
        let extracting = phase::extracting(&self.tree) || self.runs.live();
        RootPhase::derive(false, self.activity(), extracting)
    }

    // The phase read the long way, every walk taken, for the tests that hold
    // the short way to it.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    fn full_phase(&self) -> RootPhase {
        let extracting = phase::extracting(&self.tree) || self.runs.live();
        RootPhase::derive(self.payload_waiting(), self.activity(), extracting)
    }

    // The phase, checked against the long way.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn checked_phase(&self) -> RootPhase {
        let phase = self.phase();
        assert_eq!(
            phase,
            self.full_phase(),
            "the short way reads another phase"
        );
        phase
    }

    // What the recovery sets are doing: repairing while a repair run is out
    // over damage a set covers, which a run that only checks is not, and
    // while a repair's return is applied off the loop (its slices read back,
    // or its tail standing for room): the slices it placed are off the
    // damage by then, and the repair is not done until it is applied. With
    // nothing else at work, a replay or checksum still to settle, or a
    // worker's answer not yet acknowledged, is verifying what the job holds.
    fn activity(&self) -> Activity {
        let repairing = self.loans.applying()
            || (self.loans.repairing() && self.writer.damage_covered(&self.tree));
        match phase::activity(self.writer.outcomes(&self.tree), repairing) {
            Activity::Idle if self.work_pending() => Activity::Verifying,
            activity => activity,
        }
    }

    // The volumes and articles the root's readers looked at since the last
    // call.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn article_visits(&self) -> (usize, usize) {
        self.root.map.visits()
    }

    // Whether the root keeps decoder checksums for the member's volume.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn checksums_held(&self, member: MemberId) -> bool {
        self.root.checksums_held(VolumeIndex(member.0))
    }

    // Seam for status: each posted file's encoded bytes still to come, in
    // the post's file order. A parked article is not still to come: it is
    // fetched only once something asks for it.
    pub(crate) fn remaining_bytes(&self) -> Vec<u64> {
        self.root.map.remaining()
    }

    // Seam for status: each posted file's encoded bytes not yet in hand nor
    // lost for good, parked or not, in the post's file order.
    pub(crate) fn waiting_bytes(&self) -> Vec<u64> {
        self.root.map.waiting()
    }

    // Seam for status: each posted file's encoded bytes held back until
    // something asks for them, in the post's file order.
    pub(crate) fn parked_bytes(&self) -> Vec<u64> {
        self.root.map.parked()
    }

    // Seam for status: the encoded bytes the download is out to bring, as
    // the post declares them: every article but those parked until
    // something asks for them.
    pub(crate) fn wanted_bytes(&self) -> u64 {
        self.root.map.wanted_bytes()
    }

    // Whether a restored recovery object is still to hear what arrived of
    // its volumes again: the job fetches nothing until none is.
    pub fn rewalks_owed(&self) -> bool {
        self.tree.rewalks_owed()
    }

    // Seam for status: the bytes still to come and out to bring of the
    // posted recovery file at `file_index` that nothing has decided on yet.
    // One whose first bytes open no recovery format is data, said by its
    // first article (`Namespace::recovery_by_front`).
    pub(crate) fn undecided_recovery(&self, file_index: u32) -> Option<(u64, u64)> {
        if self.namespace().recovery_by_front(MemberId(file_index)) == Some(false) {
            return None;
        }
        self.root.map.undecided(VolumeIndex(file_index))
    }

    // Seam for the feed: the posted file at `file_index` is a recovery
    // set's volume, or one that may be: its front has yet to say what it
    // is, or the graph is still judging it by its clean bytes past the hole
    // where its first article was lost. With no object bound beside it,
    // nothing can judge it, and it is lost as any other file.
    pub(crate) fn may_bring_blocks(&self, file_index: u32) -> bool {
        let member = MemberId(file_index);
        self.writer
            .carries_recovery(&self.tree, NodeId::ROOT, member)
            || (self.tree.objects_beside(NodeId::ROOT)
                && self.tree.is_undecided(NodeId::ROOT, member))
            || self.tree.judges_past_hole(NodeId::ROOT, member)
    }

    // Seam for the feed: a set that rebuilds what it covers holds the
    // posted file at `file_index` as one of its volumes, whatever its posted
    // name. The feed's critical line and its first-article gate count it
    // from then on. A listing of checksums brings nothing to repair with,
    // and is not counted.
    pub(crate) fn holds_as_recovery(&self, file_index: u32) -> bool {
        self.writer
            .carries_parity(&self.tree, NodeId::ROOT, MemberId(file_index))
    }

    // Seam for the post's health: the posted file at `file_index` showed a
    // front that opens no recovery format, so whatever its name says, it is
    // no recovery data (`Namespace::recovery_by_front`).
    pub(crate) fn front_refutes_recovery(&self, file_index: u32) -> bool {
        self.namespace().recovery_by_front(MemberId(file_index)) == Some(false)
    }

    // Seam for status: the posted file at `file_index` has an article
    // neither in hand nor lost for good, parked or not.
    pub(crate) fn file_waiting(&self, file_index: u32) -> bool {
        self.root.map.volume_waiting(VolumeIndex(file_index))
    }

    // Seam for status: how many posted files the download has brought
    // whole, every article in hand.
    pub(crate) fn whole_file_count(&self) -> usize {
        self.root.map.whole_count()
    }

    // Seam for status: the encoded bytes in hand of the posted file at
    // `file_index`, as the post declares them.
    pub(crate) fn arrived_bytes(&self, file_index: u32) -> u64 {
        self.root.map.arrived_bytes(VolumeIndex(file_index))
    }

    // Seam for status: the bytes the post declares for the posted file at
    // `file_index`, every article counted.
    pub(crate) fn posted_bytes(&self, file_index: u32) -> u64 {
        self.root.map.posted_bytes(VolumeIndex(file_index))
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
    pub fn namespace(&self) -> &Namespace {
        self.tree
            .namespace(NodeId::ROOT)
            .expect("the root namespace lives as long as the tree")
    }

    // Seam for completion, status and health: each recovery set of every
    // namespace in the tree, parents first, as its namespace reads it.
    // Stable.
    pub fn recovery_summary(&self) -> Vec<SetState> {
        self.writer
            .states(&self.tree)
            .into_iter()
            .map(|state| self.loans.state(state.node).unwrap_or(state))
            .collect()
    }

    // Every member of the root has said what it is: its first bytes came,
    // or were lost, and decided who owns it, and one whose first bytes were
    // lost has been judged by the clean bytes past that hole. Until then a
    // volume of a set may still join it and bring blocks the set lacks.
    pub(crate) fn identified(&self) -> bool {
        !self.tree.deciding(NodeId::ROOT)
            && self.namespace().members().all(|member| {
                member
                    .coverage
                    .runs()
                    .next()
                    .is_some_and(|run| run.range.start == 0)
            })
            && !self.tree.judging_past_hole(NodeId::ROOT)
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
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn member_files(&self) -> Vec<(MemberId, PathBuf)> {
        self.writer
            .files(NodeId::ROOT)
            .map(|(member, path)| (member, path.to_path_buf()))
            .collect()
    }

    // Hand storage to completion. The graph only classifies what was
    // consumed; its adapter releases every handle before returning paths
    // completion may remove. A logical volume close is not an OS close.
    pub fn release_delivery_files(&mut self) -> Result<Vec<PathBuf>, &'static str> {
        if self.work_pending() || self.runs.live() || self.moves_waiting() {
            return Err("graph storage is still in use before delivery");
        }
        // The spent rule holds in every namespace, not the root's alone: a
        // recovery set an archive held, or a volume an object below the root
        // consumed, is spent where it sits. Each namespace that holds a file
        // of its own, or one an object bound in it kept of a volume, is read
        // once.
        let mut homes: std::collections::BTreeSet<NodeId> = self.writer.file_nodes().collect();
        homes.insert(NodeId::ROOT);
        homes.extend(
            self.writer
                .kept_nodes()
                .filter_map(|kept| self.tree.home(kept)),
        );
        let mut files: Vec<PathBuf> = Vec::new();
        for home in homes {
            files.extend(
                self.writer
                    .files(home)
                    .filter(|(member, _)| self.tree.spends(home, *member))
                    .map(|(_, path)| path.to_path_buf()),
            );
            let Some(namespace) = self.tree.namespace(home) else {
                continue;
            };
            files.extend(namespace.records().filter_map(|record| {
                let (child, volume) = self.tree.child_of(home, record.id)?;
                self.tree
                    .spends(home, record.id)
                    .then(|| self.writer.kept_file(child, volume))
                    .flatten()
            }));
        }
        self.writer.release_handles();
        files.sort();
        files.dedup();
        Ok(files)
    }

    // A failed job gives its working directory back as a delivery does:
    // every handle goes first, and the names stay for its record.
    pub(crate) fn release_handles(&mut self) {
        self.writer.release_handles();
    }

    // Articles the queues do not hold were fetched by an earlier run.
    pub(crate) fn arrived_before(&mut self, queued: impl FnMut(VolumeIndex, u32) -> bool) {
        self.root.map.arrived_before(queued);
    }

    // An arrived article is sent back for another copy: its header placed
    // it where its neighbours showed it is not, so what came was never the
    // article's bytes. The root waits on it again, and the copy that comes
    // is taken as its first.
    pub fn send_back(
        &mut self,
        volume: VolumeIndex,
        ordinal: u32,
        limit: usize,
    ) -> Result<(), GraphError> {
        self.unarrive(volume, ordinal, limit)
    }

    // The root waits on an arrived article again, and what its bytes
    // covered of their member is covered no more, so no member reads as its
    // own bytes that may never have been. What uncovering it forgets past
    // it in the object bound to its volume, a cipher's tail made under keys
    // it fed, is read back first, under those keys, and said again once it
    // is uncovered: it waits there for the keys the article's next copy
    // makes.
    fn unarrive(
        &mut self,
        volume: VolumeIndex,
        ordinal: u32,
        limit: usize,
    ) -> Result<(), GraphError> {
        let range = self.root.map.arrived_range(volume, ordinal);
        let tail = match (range, self.tree.child_of(NodeId::ROOT, MemberId(volume.0))) {
            (Some(range), Some((child, bound))) => Some(self.writer.read_tail(
                &mut self.tree,
                &mut *self.builder,
                child,
                &[(bound, range)],
            )?),
            _ => None,
        };
        self.root.map.unarrive(volume, ordinal);
        if let Some(range) = range {
            self.tree.uncover(NodeId::ROOT, volume, range);
        }
        if let Some(tail) = tail {
            let mut out = Vec::new();
            self.writer.say_tail(
                &mut self.tree,
                &mut *self.builder,
                &self.root,
                tail,
                &mut out,
            )?;
            self.heard(out)?;
        }
        // Saying the tail again may make work due.
        self.lend_work(limit)
    }

    // A piece of a recovery object's volume its rewalk could not read back
    // from where it was put: what of the root's articles lies under it is
    // fetched again, as a restore fetches an article not on disk, and its
    // member waits on it again. The object hears the bytes as they come.
    pub(super) fn unread(
        &mut self,
        node: NodeId,
        volume: VolumeIndex,
        range: weaver_graph::ByteRange,
        limit: usize,
    ) -> Result<(), GraphError> {
        let mut asks = Vec::new();
        let mut recorder = Recorder {
            asks: &mut asks,
            bidder: node,
            in_order: false,
        };
        self.tree.lift(
            node,
            Seek::recovery(volume, range, weaver_graph::Weight::FIRST),
            &mut recorder,
        );
        let mut under = std::collections::BTreeSet::new();
        for ask in asks {
            let at = ask.seek.range;
            under.extend(
                self.root
                    .map
                    .arrived_ranges(ask.seek.volume)
                    .into_iter()
                    .filter(|(_, placed)| placed.start < at.end && at.start < placed.end)
                    .map(|(ordinal, _)| (ask.seek.volume, ordinal)),
            );
        }
        for (volume, ordinal) in under {
            self.unarrive(volume, ordinal, limit)?;
            if let Some(namespace) = self.tree.namespace_mut(NodeId::ROOT) {
                namespace.reopen(MemberId(volume.0));
            }
            self.notes.push(Note::Unread(volume, ordinal));
        }
        Ok(())
    }

    // The volume's articles the root holds intact, at the ranges they were
    // placed at.
    pub(crate) fn placed_intact(
        &self,
        volume: VolumeIndex,
    ) -> impl Iterator<Item = (u32, weaver_graph::ByteRange)> + '_ {
        self.root.map.placed_intact(volume)
    }

    // Whether the root holds the article or has ruled it lost.
    pub fn article_settled(&self, volume: VolumeIndex, ordinal: u32) -> bool {
        self.root.map.article_settled(volume, ordinal)
    }

    // The volumes whose first article began or stopped being waited on
    // since the last call; `None` on the first, when every one counts.
    pub(crate) fn take_first_changes(&mut self) -> Option<std::collections::BTreeSet<VolumeIndex>> {
        self.root.map.take_first_changes()
    }

    // Every article the root holds lost, with its declared encoded size.
    pub(crate) fn lost_articles(&self) -> impl Iterator<Item = (VolumeIndex, u32, u64)> + '_ {
        self.root.map.lost_articles()
    }

    pub(crate) fn ordered_front(&self, volume: VolumeIndex) -> (u32, u64, bool) {
        self.root.map.ordered_front(volume)
    }

    pub fn article_class(&self, volume: VolumeIndex, ordinal: u32) -> Option<Class> {
        self.root.map.class(volume, ordinal)
    }

    // The first article of every member leads the payload.
    pub(crate) fn sample(&mut self) -> Vec<Move> {
        self.root.map.sample();
        self.root.map.take_moves()
    }

    pub fn resume_runs(&mut self, slots: &mut RepairSlots, limit: usize) -> Result<(), GraphError> {
        self.step_runs(Vec::new(), limit)?;
        self.lend(slots, limit)
    }

    // Whether a run of the job waits for decode memory that was freed since
    // it was turned away, by this job or another.
    pub fn memory_freed(&self) -> bool {
        self.runs.wakeable()
    }

    // The device a file at `path` lies on, as a write there would say.
    pub(crate) fn device_of(&self, path: &std::path::Path) -> Device {
        self.writer.device_of(path)
    }

    // The ledger of the disk `device` is to this job: the one its writes
    // there spend from.
    pub(crate) fn disk_ledger(
        &self,
        device: Device,
    ) -> &weaver_server_disk::operations::disk::DiskLedger {
        self.writer.capacity(device).ledger()
    }

    // Whether the job waits for room on a disk, and its place among the
    // jobs that wait.
    pub fn disk_held(&self) -> bool {
        self.writer.disk_held()
    }

    // What the step that just failed gave back to the disk as it failed.
    pub(crate) fn take_gave_back(&mut self) -> Option<u64> {
        self.writer.take_gave_back()
    }

    // What was left to write of the file whose write just found no room.
    pub(crate) fn take_left_to_write(&mut self) -> Option<u64> {
        self.writer.take_left_to_write()
    }

    pub fn disk_ticket(&self) -> Option<u64> {
        self.writer.disk_ticket()
    }

    // A reading came: whether the job has room again. If it has, what it
    // owes is promised again, and the runs parked for room, queued again,
    // start on the next step. A job that waits idle wakes whatever the
    // disk reads.
    pub fn wake_disk(&mut self) -> bool {
        if self.disk_idle() {
            return self.writer.wake_idle();
        }
        self.writer.wake_disk()
    }

    // Whether the job waits for room though nothing of it would take any:
    // it owes nothing, has nothing to write again, and no run of it is
    // running or waits to. Its last write crossed the reserve and closed,
    // and it wakes on the next turn. A run still live could write again,
    // and keeps the job waiting for a reading with room.
    pub fn disk_idle(&self) -> bool {
        self.writer.disk_held() && self.writer.owes_nothing() && !self.runs.live()
    }

    // What the job waits for room for, and the folder it waits on.
    pub fn disk_wait(&self) -> Option<(super::limits::DiskWait, std::path::PathBuf)> {
        self.writer.disk_wait()
    }

    // An article of a posted file named the file in its header (`=ybegin
    // ... name=`, or a uuencode `begin` line), and is about to be taken in.
    // The file takes that name, as `weaver_nzb::header_name` decides it,
    // only while none of its bytes are in: the
    // first article taken in decides, and its write is under the name it
    // decided. A recovery set that already describes the file named it,
    // and its name stands; one that recognises the file by its content
    // later renames it as it renames any member. The rename is the tree's,
    // as a recovery set's is: the ledger places the name, the file follows,
    // and the name is a claim made again. The checkpoint that first records
    // the file's bytes records its name with them.
    pub(crate) fn header_named(
        &mut self,
        volume: VolumeIndex,
        header: &str,
    ) -> Result<(), GraphError> {
        let member = MemberId(volume.0);
        let Some(posted) = self
            .tree
            .namespace(NodeId::ROOT)
            .and_then(|namespace| namespace.get(member))
            .filter(|record| record.coverage.runs().next().is_none())
            .map(|record| record.name.as_str())
        else {
            return Ok(());
        };
        let Some(name) = weaver_nzb::deobfuscate::header_name(posted, header) else {
            return Ok(());
        };
        let described = self
            .tree
            .recovery_nodes(NodeId::ROOT)
            .into_iter()
            .any(|set| {
                self.tree
                    .consult_recovery(set, |recovery, _| recovery.covers(member))
                    .unwrap_or(false)
            });
        if described {
            return Ok(());
        }
        tracing::info!(
            job_id = self.runs.scope(),
            file = volume.0,
            posted = %posted,
            named = %name,
            "file named by its article header"
        );
        let mut out = Vec::new();
        self.writer.header_named(
            &mut self.tree,
            &mut *self.builder,
            &self.root,
            (member, &name),
            &mut out,
        )?;
        self.heard(out)
    }

    // `limit` is the slot limit the pipeline reads now, which every step
    // that asks or frees a run follows, and a repair the step lends asks
    // `slots` under it. The runs step before the lend, so a run the step
    // starts counts as reading when the walk asks whether a set is due.
    // True when the root took the article; false for a copy it dropped.
    pub fn intake(
        &mut self,
        decoded: Decoded,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<bool, GraphError> {
        let took = self.take_in(decoded, limit)?;
        if took {
            self.lend(slots, limit)?;
        }
        Ok(took)
    }

    // The article taken in and the runs stepped, with no lend: a caller
    // whose repair slots are shared lends after, holding them only for
    // that. True when the root took the article.
    pub fn take_in(&mut self, decoded: Decoded, limit: usize) -> Result<bool, GraphError> {
        let volume = decoded.volume;
        let member = MemberId(volume.0);
        let length = self.root.accepted_length(&decoded);
        let Some(arrival) = self.root.receive(decoded) else {
            return Ok(false);
        };
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
        Ok(true)
    }

    // Every server failed the article; nothing will fetch it again.
    pub fn lost(
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
    // completed. Only a member whose every article arrived has landed; one
    // that lost an article is closed without being said to have landed.
    fn after_member(&mut self, volume: VolumeIndex) -> Result<(), GraphError> {
        let member = MemberId(volume.0);
        if !self.root.map.volume_waiting(volume) && !self.closed(member) {
            if let Some(end) = self.root.map.reached(volume) {
                self.tree.reached(NodeId::ROOT, member, end);
            }
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
            if self.root.map.volume_whole(volume) {
                self.notes.push(Note::Landed(volume));
            }
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
    // out: they hold back only their own set's ruling. Root members are
    // numbered as the volumes they are; a further posting of a member is
    // a volume of its own whose articles are the member's.
    fn payload_waiting(&self) -> bool {
        self.writer.payload_coming(
            &self.tree,
            self.root.map.coming().map(|volume| MemberId(volume.0)),
        )
    }

    // What the root still fetches unasked, payload and the sets whose own
    // volumes are still arriving. Only the volumes with such an article are
    // asked.
    pub fn coming(&self) -> Coming {
        self.writer.coming(
            &self.tree,
            self.root.map.coming().map(|volume| MemberId(volume.0)),
        )
    }

    // Whether the volume has articles the queue fetches unasked still to
    // come.
    pub(crate) fn to_come(&self, volume: VolumeIndex) -> bool {
        self.root.map.volume_to_come(volume)
    }

    // One round of seeks: every object's standing wants lifted to the root,
    // answered together so the window draws from all of them at once, and
    // the answers handed back. A round that parks articles takes them out
    // of what the root still fetches unasked, which may end a namespace as
    // losing them would: the job is stepped as it is after a loss, and
    // what its runs want then is asked by the round their mail brings.
    // Returns the class changes for the queue.
    pub fn seek_round(
        &mut self,
        queued: impl FnMut(VolumeIndex) -> bool,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<Vec<Move>, GraphError> {
        let from = self.root.map.moves_pending();
        sample::indexes(&self.root.indexes, &self.root.map, &mut self.root.window);
        sample::bootstrap(
            &self.tree,
            &mut self.root.map,
            &mut self.root.leading,
            &mut self.root.window,
        );
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
        let mut settled = Vec::new();
        for (node, seek, pull, span) in lifted {
            let pull = answers[span]
                .iter()
                .fold(pull, |pull, answer| pull.fold(*answer));
            // A pull of a byte nothing will bring may settle the place of
            // the volume it was for, which a run may wait on.
            let open = !self.placed(node, seek.volume);
            self.tree.pulled(
                node,
                seek,
                pull,
                &mut self.writer.placing(&mut *self.builder),
                &mut effects,
            );
            if open && self.placed(node, seek.volume) {
                settled.push((node, seek.volume));
            }
        }
        self.settle(effects)?;
        for (node, volume) in settled {
            self.place_settled(node, volume)?;
        }
        if self.root.map.parked_since(from) {
            self.step_runs(Vec::new(), limit)?;
            self.lend(slots, limit)?;
        } else {
            // What the answers let an object hold may be due.
            self.lend_work(limit)?;
        }
        Ok(self.root.map.take_moves())
    }

    // The volumes whose place in their set the rounds learned since the last
    // call, each with the rank its articles are fetched at.
    pub(crate) fn take_volume_ranks(&mut self) -> Vec<(VolumeIndex, u32)> {
        self.root.leading.take_ranked()
    }

    // The phase, when it changed since the last call.
    // Under test every report is held to the long way, so each step of the
    // pipeline tests checks the short way too.
    pub fn phase_change(&mut self) -> Option<RootPhase> {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        let phase = self.checked_phase();
        #[cfg(not(any(test, feature = "pipeline-test-fixtures")))]
        let phase = self.phase();
        (self.reported != Some(phase)).then(|| {
            if self.reported == Some(RootPhase::Repairing) {
                self.repair_ended_at = Some(crate::jobs::epoch_ms_now() as i64);
            }
            self.reported = Some(phase);
            self.repair_reported |= phase == RootPhase::Repairing;
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

    // What an object back from its work said, through the writer even when
    // it said nothing: what it can place changed, as under an offer, and the
    // writer is where held bytes go down.
    fn settle_worked(&mut self, effects: Vec<Effect>) -> Result<(), GraphError> {
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
                self.fetch_unkept();
                self.pay_release();
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

    // What a give-back left of a root member not in hand comes again: the
    // root waits on the articles under it again, the queue fetches them as
    // first dispatched, and the member is open until they are in. Without
    // them the member never closes, and a set covering it never rules.
    fn fetch_unkept(&mut self) {
        let mut unkept: BTreeMap<MemberId, Vec<weaver_graph::ByteRange>> = BTreeMap::new();
        for (member, range) in self.writer.take_unkept() {
            unkept.entry(member).or_default().push(range);
        }
        for (member, ranges) in unkept {
            let volume = VolumeIndex(member.0);
            let under: Vec<(u32, weaver_graph::ByteRange)> = self
                .root
                .map
                .arrived_ranges(volume)
                .into_iter()
                .filter(|(_, placed)| {
                    ranges
                        .iter()
                        .any(|range| placed.start < range.end && range.start < placed.end)
                })
                .collect();
            if under.is_empty() {
                continue;
            }
            for (ordinal, placed) in under {
                self.root.map.unarrive(volume, ordinal);
                self.tree.uncover(NodeId::ROOT, volume, placed);
                self.notes.push(Note::Unread(volume, ordinal));
            }
            if let Some(namespace) = self.tree.namespace_mut(NodeId::ROOT) {
                namespace.reopen(member);
            }
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

// What the unit tests compiled apart in tests/pipeline_graph.rs reach in
// this module's private modules.
#[cfg(feature = "pipeline-test-fixtures")]
#[doc(hidden)]
pub mod units {
    pub mod repair {
        pub use super::super::repair::*;
    }
    pub mod runs {
        pub use super::super::runs::*;
    }
}
