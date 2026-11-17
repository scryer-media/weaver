// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The other sink of a torrent's verified pieces: a graph job over the
// torrent's archive files, which unpacks them as it unpacks a post. Only a
// torrent with a container volume among its files has one; a torrent of
// plain files never reaches the graph.
//
// The job's root members are the archive files, container volumes and the
// recovery files and SFVs beside them, in torrent order; a member's articles
// are its shares of the pieces it lies in, numbered from its first piece.
// A share goes to the job as a decoded article does, from the buffer its
// piece was hashed from: the piece check vouched for it, so it arrives
// intact and placed, and no byte of it is copied or hashed again. A piece
// the job lacks that the torrent already has, after a restart or once a
// seed-mode piece's check passes, is read from the torrent's files and
// handed to it the same way; no byte goes to it that no check passed.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use weaver_graph::slots::AskerId;
use weaver_graph::{Listed, VolumeIndex};

use super::layout::{PieceLayout, Span};
use super::relocate::MoveError;
use super::service::Signal;
use crate::jobs::graph_checkpoint::GraphCheckpoint;
use crate::jobs::ids::JobId;
use crate::persistence::checkpoint_write::CheckpointControl;
use crate::pipeline::graph::checkpoint::{self, checkpoint_job};
use crate::pipeline::graph::engine::{Heard, Hears, Outside, Said};
use crate::pipeline::graph::job::restore::RestoreFailure;
use crate::pipeline::graph::job::{GraphError, GraphJob, RootFile, Storage};
use crate::pipeline::graph::namespace::WriteError;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::status::RepairSlots;
use crate::runtime::panic_trap::JobScope;
use tokio::sync::mpsc::UnboundedSender;

#[cfg(test)]
mod outputs_tests;
#[cfg(test)]
mod tests;

// Which of a torrent's files go to the graph, by name alone, as the
// volumes they are; none when no file is a container volume, as for most
// torrents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Plan {
    volumes: Vec<Option<VolumeIndex>>,
}

impl Plan {
    pub(crate) fn of(layout: &PieceLayout) -> Option<Self> {
        let names: Vec<&str> = layout.files().map(|file| file.name.as_str()).collect();
        let listed: Vec<Option<Listed>> = layout
            .files()
            .zip(weaver_graph::listed(&names))
            .map(|(file, listed)| (!file.padding).then_some(listed))
            .collect();
        if !listed.contains(&Some(Listed::Container)) {
            return None;
        }
        let mut next = 0;
        let volumes = listed
            .into_iter()
            .map(|listed| {
                matches!(listed, Some(Listed::Container | Listed::Recovery)).then(|| {
                    next += 1;
                    VolumeIndex(next - 1)
                })
            })
            .collect();
        Some(Self { volumes })
    }

    // The volume the file is, if it goes to the graph.
    pub(crate) fn volume(&self, file: usize) -> Option<VolumeIndex> {
        self.volumes.get(file).copied().flatten()
    }

    // The files that go to the graph, as the job's root lists its members.
    pub(crate) fn root_files(&self, layout: &PieceLayout) -> Vec<RootFile> {
        layout
            .files()
            .enumerate()
            .filter(|(index, _)| self.volume(*index).is_some())
            .map(|(index, file)| {
                let first = layout.file_pieces(index).start;
                RootFile {
                    name: file.name.clone(),
                    articles: layout
                        .file_pieces(index)
                        .filter_map(|piece| {
                            let share = layout.share(index, piece)?;
                            Some((piece - first, share.len() as u32))
                        })
                        .collect(),
                }
            })
            .collect()
    }

    // The file's lengths, as the job's extraction budget declares them.
    pub(crate) fn declared(&self, layout: &PieceLayout) -> Vec<u64> {
        layout
            .files()
            .enumerate()
            .filter(|(index, _)| self.volume(*index).is_some())
            .map(|(_, file)| file.length)
            .collect()
    }
}

pub(crate) struct Tee {
    job: Mutex<GraphJob>,
    layout: Arc<PieceLayout>,
    plan: Plan,
    // Where the job writes: its own directory, apart from the torrent's.
    work: PathBuf,
    job_id: JobId,
    repairs: Arc<Mutex<RepairSlots>>,
    signal: Arc<Signal>,
    outside: Arc<Outside>,
    limit: usize,
    // Where the job's checkpoints go, and where the database's word on
    // them, and the wake for one come due, come back.
    db: crate::Database,
    mail: UnboundedSender<Said>,
    // The first step of the job that failed, and why: unpacking is over for
    // the torrent, which keeps its files and seeds on.
    broken: Mutex<Option<String>>,
    // Why the job's writer refused its output, kept each time the job's
    // lock is let go: what the torrent shows reads it without waiting on
    // the job, which an intake writing to disk may hold.
    refused: Mutex<Option<String>>,
    // Where the words for the job go, in the order its task takes them.
    words: tokio::sync::mpsc::UnboundedSender<Word>,
    // Counts the words for the job, taken.
    heard: tokio::sync::watch::Sender<u64>,
    // A lend an intake found the repair slots busy for, left to the
    // service's next settle.
    owes_lend: std::sync::atomic::AtomicBool,
    // The torrent is paused on its own: the job takes no slot.
    paused: std::sync::atomic::AtomicBool,
    // The ticket the job took from the disk ledger as it began to wait for
    // room, `NO_TICKET` while it does not wait: the pipeline orders it by
    // this among the jobs it wakes, without the job's lock.
    disk_ticket: std::sync::atomic::AtomicU64,
    // What the job waits for room for, and the folder it waits on, kept as
    // its wait begins: the pipeline says it then, without the job's lock.
    disk_wait: Mutex<Option<DiskWaitOf>>,
    // Counts the checkpoints asked of the job, each before it waits on the
    // job's lock.
    #[cfg(test)]
    pub(crate) asked: tokio::sync::watch::Sender<u64>,
    // Counts the words on the job's clock, taken.
    #[cfg(test)]
    clocked: tokio::sync::watch::Sender<u64>,
}

// A word for a tee's job: what its runs said, with the repair slots it may
// step with, every download paused or resumed, or the torrent paused or
// resumed on its own.
enum Word {
    Said(Said, usize),
    Clock(bool),
    Own(bool),
    // A disk the job waits on had a reading while the job was busy: it is
    // asked for room again on its own task.
    WakeDisk,
    // The job has room again: its runs and repairs go on.
    DiskWoke,
}

// The job waits for no room.
const NO_TICKET: u64 = u64::MAX;

// What a job waits for room for, and the folder it waits on.
type DiskWaitOf = (crate::pipeline::graph::limits::DiskWait, PathBuf);

// One share of a piece that the job has yet to take.
struct Share {
    span: Span,
    volume: VolumeIndex,
    ordinal: u32,
}

// Why a tee's job could not be built again from its checkpoint.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Unrestored {
    #[error(transparent)]
    Rows(#[from] crate::pipeline::graph::checkpoint::CheckpointError),
    #[error(transparent)]
    Job(#[from] RestoreFailure),
    #[error(transparent)]
    Storage(#[from] crate::pipeline::graph::leaf::LeafError),
}

impl Unrestored {
    // The rows are not a checkpoint this build makes a job of, found before
    // the rebuild touches a file, or records the rebuild found contradicting
    // themselves, which would fault again on every start
    // (`GraphRestoreError::not_this_builds`).
    pub(crate) fn not_this_builds(&self) -> bool {
        matches!(
            self,
            Self::Rows(_)
                | Self::Job(
                    RestoreFailure::Tree(_)
                        | RestoreFailure::Root(_)
                        | RestoreFailure::Post(_)
                        | RestoreFailure::Step(GraphError::Inconsistent(_))
                )
        )
    }
}

// What a tee is made of: the torrent's job and files, the archive plan,
// the directory its job writes in, and what it is fed from and speaks to.
pub(super) struct Parts {
    pub(super) job_id: JobId,
    pub(super) layout: Arc<PieceLayout>,
    pub(super) plan: Plan,
    pub(super) work: PathBuf,
    pub(super) repairs: Arc<Mutex<RepairSlots>>,
    pub(super) signal: Arc<Signal>,
    pub(super) outside: Arc<Outside>,
    pub(super) limit: usize,
    pub(super) db: crate::Database,
    pub(super) mail: UnboundedSender<Said>,
}

impl Tee {
    // The job `rows` restores over the plan's files, in `work`, joined to
    // the runs' mail before anything it does can start a run. A restore
    // starts none: the runs start at the first step.
    pub(super) fn restore(
        parts: Parts,
        storage: Storage,
        rows: GraphCheckpoint,
    ) -> Result<Arc<Self>, Unrestored> {
        let _job = JobScope::enter(parts.job_id);
        let files = parts.plan.root_files(&parts.layout);
        let mut snapshot = checkpoint::snapshot(&rows)?;
        retry_from_top(&mut snapshot.passwords);
        crate::pipeline::graph::restore::sweep_unnamed(&parts.work, &snapshot, &[])?;
        let job = GraphJob::restore(&snapshot, &files, parts.job_id.0, storage, 0)?.job;
        // The next checkpoint writes only what it changes of these.
        job.pending().written(rows);
        Ok(Self::joined(parts, job))
    }

    // A new job over the plan's files.
    pub(super) fn fresh(parts: Parts, storage: Storage) -> Arc<Self> {
        let _job = JobScope::enter(parts.job_id);
        let files = parts.plan.root_files(&parts.layout);
        let job = GraphJob::admit(parts.job_id.0, &files, storage);
        Self::joined(parts, job)
    }

    fn joined(parts: Parts, job: GraphJob) -> Arc<Self> {
        let (words, heard_words) = tokio::sync::mpsc::unbounded_channel();
        let tee = Arc::new(Self {
            job: Mutex::new(job),
            layout: parts.layout,
            plan: parts.plan,
            work: parts.work,
            job_id: parts.job_id,
            repairs: parts.repairs,
            signal: parts.signal,
            outside: parts.outside,
            limit: parts.limit,
            db: parts.db,
            mail: parts.mail,
            broken: Mutex::new(None),
            refused: Mutex::new(None),
            words: words.clone(),
            heard: tokio::sync::watch::Sender::new(0),
            owes_lend: std::sync::atomic::AtomicBool::new(false),
            paused: std::sync::atomic::AtomicBool::new(false),
            disk_ticket: std::sync::atomic::AtomicU64::new(NO_TICKET),
            disk_wait: Mutex::new(None),
            #[cfg(test)]
            asked: tokio::sync::watch::Sender::new(0),
            #[cfg(test)]
            clocked: tokio::sync::watch::Sender::new(0),
        });
        tokio::spawn(hear(Arc::downgrade(&tee), heard_words));
        tee.outside.join(
            parts.job_id.0,
            Arc::new(Hearer {
                mail: words,
                tee: Arc::downgrade(&tee),
            }),
        );
        tee
    }

    // The torrent paused or resumed on its own. Paused, the job takes no
    // slot, as a paused job of the pipeline's takes none, and its
    // extraction deadline does not run while it, or every download, is
    // paused; resumed, its runs start again.
    pub(crate) fn hold_own_clock(&self, paused: bool) {
        self.paused
            .store(paused, std::sync::atomic::Ordering::Release);
        let _ = self.words.send(Word::Own(paused));
    }

    // The slots the job may take of `offered`: none while the torrent is
    // paused on its own.
    fn slots(&self, offered: usize) -> usize {
        if self.paused.load(std::sync::atomic::Ordering::Acquire) {
            0
        } else {
            offered
        }
    }

    // One word for the job, taken working for it, with the holds on its
    // clock as they stood: those after it.
    fn take(&self, word: Word, (all, own): (bool, bool)) -> (bool, bool) {
        let _job = JobScope::enter(self.job_id);
        match word {
            Word::Said(said, slots) => {
                self.heard(said, slots);
                (all, own)
            }
            Word::Clock(all) => {
                self.held_clock((all, own));
                (all, own)
            }
            Word::Own(paused) => {
                if own && !paused {
                    let _ = self.resume();
                }
                self.held_clock((all, paused));
                (all, paused)
            }
            Word::WakeDisk => {
                let mut job = self.locked();
                if self.wake_disk_locked(&mut job) {
                    drop(job);
                    self.disk_woke();
                }
                self.heard.send_modify(|heard| *heard += 1);
                (all, own)
            }
            Word::DiskWoke => {
                self.disk_woke();
                self.heard.send_modify(|heard| *heard += 1);
                (all, own)
            }
        }
    }

    // A disk the job waits on had a reading, asked from the pipeline's
    // loop, which never waits on the job's lock: an intake writing to disk
    // may hold it. Held, the job is asked on its own task instead, and
    // keeps its place among the waiters until then. Woken, its runs go on
    // on its own task.
    fn wake_disk_now(&self) -> bool {
        let job = match self.job.try_lock() {
            Ok(job) => job,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => {
                let _ = self.words.send(Word::WakeDisk);
                return false;
            }
        };
        let mut job = Held { tee: self, job };
        if !self.wake_disk_locked(&mut job) {
            return false;
        }
        drop(job);
        let _ = self.words.send(Word::DiskWoke);
        true
    }

    // Whether the job has room again, as a job of the pipeline's is asked
    // (`GraphJob::wake_disk`): woken, it no longer waits.
    fn wake_disk_locked(&self, job: &mut GraphJob) -> bool {
        if job.disk_held() && !job.wake_disk() {
            return false;
        }
        self.disk_unheld();
        true
    }

    // The job woke from its wait for room: its runs start again and its
    // repairs are lent, as `disk_woke` does for a job of the pipeline's.
    fn disk_woke(&self) {
        if let Err(error) = self.resume() {
            tracing::warn!(job_id = self.job_id.0, error = %error, "a torrent's unpacking step failed");
        }
        self.signal.mark(self.job_id);
    }

    // The job waits for room on a disk after a step, or no longer does. It
    // begins to wait as a job of the pipeline's does, by a ticket from the
    // ledger of the disk, and the pipeline wakes it with its own, in ticket
    // order. One that waits owing nothing, its last write landed, wakes at
    // once, as the pipeline wakes its own.
    fn note_disk(&self, job: &mut GraphJob) {
        if !job.disk_held() {
            self.disk_unheld();
            return;
        }
        if job.disk_idle() && job.wake_disk() {
            self.disk_unheld();
            let _ = self.words.send(Word::DiskWoke);
            return;
        }
        let ticket = job.disk_ticket().unwrap_or(0);
        let was = self
            .disk_ticket
            .swap(ticket, std::sync::atomic::Ordering::AcqRel);
        if was == NO_TICKET {
            // Kept before the pipeline hears the wait begin, which reads it.
            *self.disk_wait_slot() = job.disk_wait();
            self.outside.disk_held(self.job_id.0);
            self.signal.mark(self.job_id);
        }
    }

    fn disk_unheld(&self) {
        let was = self
            .disk_ticket
            .swap(NO_TICKET, std::sync::atomic::Ordering::AcqRel);
        if was != NO_TICKET {
            self.outside.disk_unheld(self.job_id.0);
            *self.disk_wait_slot() = None;
        }
    }

    fn disk_wait_slot(&self) -> std::sync::MutexGuard<'_, Option<DiskWaitOf>> {
        self.disk_wait
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    // The count of the words for the job taken so far, which moves with
    // each one.
    #[cfg(test)]
    pub(crate) fn taken(&self) -> tokio::sync::watch::Receiver<u64> {
        self.heard.subscribe()
    }

    // The count of the words on the job's clock taken so far.
    #[cfg(test)]
    pub(crate) fn clocked(&self) -> tokio::sync::watch::Receiver<u64> {
        self.clocked.subscribe()
    }

    // What the job's runs said, or the database said of its checkpoint,
    // taken by the job.
    fn heard(&self, said: Said, slots: usize) {
        if let Heard::CheckpointBlocked {
            control,
            revision,
            error,
        } = &said.heard
        {
            self.blocked(control, *revision, error);
        } else if let Heard::CheckpointDue { control } = &said.heard {
            self.due(control);
        } else if let Err(error) = self.step(slots, |job, repairs, limit| {
            job.run_said(said, repairs, limit)
        }) {
            tracing::warn!(job_id = self.job_id.0, error = %error, "a torrent's unpacking step failed");
        }
        self.heard.send_modify(|heard| *heard += 1);
        self.signal.mark(self.job_id);
    }

    // The job's clock held while every download or the torrent itself is
    // paused, and running again once neither is.
    fn held_clock(&self, (all, own): (bool, bool)) {
        self.locked().hold_clock(all || own);
        #[cfg(test)]
        self.clocked.send_modify(|clocked| *clocked += 1);
        self.heard.send_modify(|heard| *heard += 1);
    }

    // Unpacking is over for the torrent, for `why`: the job takes nothing
    // more, and the torrent keeps its files.
    pub(crate) fn refuse(&self, why: String) {
        self.broken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get_or_insert(why);
    }

    fn is_broken(&self) -> bool {
        self.broken
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }

    pub(crate) fn work(&self) -> &Path {
        &self.work
    }

    fn locked(&self) -> Held<'_> {
        Held {
            tee: self,
            job: self.job.lock().unwrap_or_else(PoisonError::into_inner),
        }
    }

    pub(crate) fn with<T>(&self, look: impl FnOnce(&mut GraphJob) -> T) -> T {
        look(&mut self.locked())
    }

    // One step of the job under the repair slots every torrent's job
    // shares. The job's own lock is taken first, so a step waiting on its
    // job, which an intake writing to disk may hold, holds no slot every
    // other torrent's step waits on. A slot the step gave back may go to a
    // job that waited for it: that job is marked, and the service lends for
    // it. A step that fails is kept as the job's failure.
    pub(crate) fn step<T>(
        &self,
        limit: usize,
        step: impl FnOnce(&mut GraphJob, &mut RepairSlots, usize) -> Result<T, GraphError>,
    ) -> Result<T, GraphError> {
        let _job = JobScope::enter(self.job_id);
        let limit = self.slots(limit);
        let mut job = self.locked();
        let mut repairs = self.repairs.lock().unwrap_or_else(PoisonError::into_inner);
        let result = step(&mut job, &mut repairs, limit);
        drop(job);
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
        drop(repairs);
        if result.is_ok() {
            self.stepped(&mut self.locked());
        }
        self.kept(result)
    }

    // A step reached the job. Its checkpoint is taken once the write it
    // joins is due, by the bytes taken since the last or their age, or at
    // once while a move waits on a commit, as a job of the pipeline's is,
    // or while one that found an object out for its work is owed: the step
    // that takes the object back takes it, and work goes out again. Until
    // then nothing is read of the job.
    fn stepped(&self, job: &mut GraphJob) {
        if job.pending().control().cancelled() {
            return;
        }
        self.note_disk(job);
        if job.awaits_commit()
            || job.owes_checkpoint()
            || job.pending().stepped(self.job_id, &self.mail)
        {
            self.checkpointed(job, false);
        }
    }

    // The wake a step armed: the job's checkpoint is due. One of a writer
    // let go since, or of another, takes nothing.
    fn due(&self, control: &CheckpointControl) {
        let mut job = self.locked();
        let own = job.pending().control();
        if own.same(control) && !own.cancelled() {
            self.checkpointed(&mut job, false);
        }
    }

    // Checkpoint the job as it stands, again for as long as a file of its
    // moved: the move is recorded by the next one. The change is queued for
    // the database once its write is due, or at once when `force`.
    fn checkpointed(&self, job: &mut GraphJob, force: bool) {
        let _ = self.checkpoint_locked(job, force);
    }

    // A checkpoint that fails, as a file that could not move, fails the
    // job, as it fails a job of the pipeline's: it is kept as the job's
    // failure.
    fn checkpoint_locked(&self, job: &mut GraphJob, force: bool) -> Result<(), WriteError> {
        loop {
            if job.pending().control().cancelled() {
                return Ok(());
            }
            match checkpoint_job(
                job,
                self.job_id,
                &self.db,
                &self.mail,
                Vec::new(),
                None,
                force,
            ) {
                Ok(true) => {}
                Ok(false) => return Ok(()),
                Err(error) => {
                    self.refuse(error.to_string());
                    self.signal.mark(self.job_id);
                    return Err(error);
                }
            }
        }
    }

    // A failed step's error, kept as the job's failure.
    fn kept<T>(&self, result: Result<T, GraphError>) -> Result<T, GraphError> {
        if let Err(error) = &result {
            self.refuse(error.to_string());
        }
        result
    }

    // The shares of the piece that go to the job and that it has neither
    // taken nor ruled lost.
    fn unsettled(&self, job: &GraphJob, piece: u32) -> Vec<Share> {
        self.layout
            .spans(piece)
            .into_iter()
            .filter_map(|span| {
                let volume = self.plan.volume(span.file)?;
                let ordinal = piece - self.layout.file_pieces(span.file).start;
                (!job.article_settled(volume, ordinal)).then_some(Share {
                    span,
                    volume,
                    ordinal,
                })
            })
            .collect()
    }

    // Whether the job waits on any share of the piece.
    pub(crate) fn wants(&self, piece: u32) -> bool {
        let _job = JobScope::enter(self.job_id);
        !self.is_broken() && !self.unsettled(&self.locked(), piece).is_empty()
    }

    // The piece's shares the job waits on, each a slice of `held`, the
    // whole piece as it was hashed. A share the job already took, before a
    // restart or from a copy of the piece fetched again, is not taken
    // twice. True when it took any. The piece is the torrent's whatever
    // the job makes of it: a job that fails keeps why, for the service.
    //
    // The shares are written and the runs stepped under this job's lock
    // alone, and the lend after takes the repair slots every torrent's job
    // shares only when they are free: held by another torrent's step, the
    // lend is left to the service's next settle, which this marks. One
    // torrent's commit never waits on another's.
    pub(crate) fn deliver(&self, piece: u32, held: &Bytes) -> bool {
        let _job = JobScope::enter(self.job_id);
        if self.is_broken() {
            return false;
        }
        let took = self.kept(self.with(|job| {
            let mut took = false;
            for share in self.unsettled(job, piece) {
                let bytes = share.span.range.end - share.span.range.start;
                if job.take_in(self.decoded(&share, held), self.slots(self.limit))? {
                    job.pending().delivered(bytes);
                    took = true;
                }
            }
            if took {
                self.stepped(job);
            }
            Ok(took)
        }));
        match took {
            Ok(true) => self.lend_free().is_ok(),
            Ok(false) | Err(_) => false,
        }
    }

    // The piece's shares the job waits on, read from the torrent's files by
    // `read` into one buffer of the piece's length, then taken as a
    // verified piece's are: the one read a restart makes to bring the job
    // back level with the pieces the torrent has. True when it took any.
    pub(crate) fn replay(
        &self,
        piece: u32,
        read: impl Fn(usize, u64, &mut [u8]) -> std::io::Result<()>,
    ) -> std::io::Result<bool> {
        let _job = JobScope::enter(self.job_id);
        if self.is_broken() {
            return Ok(false);
        }
        let shares = self.unsettled(&self.locked(), piece);
        if shares.is_empty() {
            return Ok(false);
        }
        let mut bytes = vec![0; self.layout.piece_range(piece).len() as usize];
        for share in &shares {
            let at = share.span.at as usize;
            read(
                share.span.file,
                share.span.range.start,
                &mut bytes[at..at + share.span.range.len() as usize],
            )?;
        }
        Ok(self.deliver(piece, &Bytes::from(bytes)))
    }

    fn decoded(&self, share: &Share, held: &Bytes) -> Decoded {
        let at = share.span.at as usize;
        let length = self
            .layout
            .file(share.span.file)
            .map_or(share.span.range.end, |file| file.length);
        Decoded {
            volume: share.volume,
            ordinal: share.ordinal,
            offset: share.span.range.start,
            bytes: held.slice(at..at + share.span.range.len() as usize),
            intact: true,
            positioned: true,
            length: Some(length),
            declared_end: Some(share.span.range.end),
            segments: Vec::new(),
        }
    }

    // The shares of files the torrent does not want will never come: the
    // job rules them lost, as it does an article no server has, and goes
    // on with what it has.
    pub(crate) fn lose_unwanted(&self, wanted: impl Fn(usize) -> bool) -> Result<(), GraphError> {
        self.step(self.limit, |job, repairs, limit| {
            for file in 0..self.layout.file_count() {
                let Some(volume) = self.plan.volume(file) else {
                    continue;
                };
                if wanted(file) {
                    continue;
                }
                let pieces = self.layout.file_pieces(file);
                for ordinal in 0..pieces.end - pieces.start {
                    if !job.article_settled(volume, ordinal) {
                        job.lost(volume, ordinal, repairs, limit)?;
                    }
                }
            }
            Ok(())
        })
    }

    // Start the runs and lend the repairs the job may hold now.
    pub(crate) fn resume(&self) -> Result<(), GraphError> {
        self.step(self.limit, |job, repairs, limit| {
            job.resume_runs(repairs, limit)
        })
    }

    // Lend what the job made due, now if the repair slots are free, or at
    // the next settle.
    fn lend_free(&self) -> Result<(), GraphError> {
        let mut job = self.locked();
        let Ok(mut repairs) = self.repairs.try_lock() else {
            self.owes_lend
                .store(true, std::sync::atomic::Ordering::Release);
            self.signal.mark(self.job_id);
            return Ok(());
        };
        let result = job.lend(&mut repairs, self.slots(self.limit));
        drop(job);
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
        drop(repairs);
        self.kept(result)
    }

    // The lend an intake left to the settle, and a slot handed to the job
    // while it waited, which goes to the repair it could not lend for want
    // of one.
    pub(crate) fn lend_handed(&self) -> Result<(), GraphError> {
        let owed = self
            .owes_lend
            .swap(false, std::sync::atomic::Ordering::AcqRel);
        let handed = self
            .repairs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handed(AskerId(self.job_id.0));
        if !owed && !handed {
            return Ok(());
        }
        self.step(self.limit, |job, repairs, limit| job.lend(repairs, limit))
    }

    // Why unpacking refused the torrent's output, or failed, if it did.
    pub(crate) fn refusal(&self) -> Option<String> {
        let refused = self
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        refused.or_else(|| {
            self.broken
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        })
    }

    // Done: every share in or ruled lost, everything unpacked, and every
    // file at its name.
    pub(crate) fn done(&self) -> bool {
        let job = self.locked();
        job.phase() == RootPhase::Done && !job.moves_waiting()
    }

    // Checkpoint the job as it stands, its change queued for the database
    // at once when `force`, and otherwise once its write is due.
    pub(crate) fn checkpoint(&self, force: bool) -> Result<(), WriteError> {
        let _job = JobScope::enter(self.job_id);
        #[cfg(test)]
        self.asked.send_modify(|asked| *asked += 1);
        self.checkpoint_locked(&mut self.locked(), force)
    }

    // A stop's checkpoint, its change queued at once, on a thread that may
    // block. Every object of the job out for its work is waited on until
    // it is back first, as the pipeline's stop hears each back before its
    // last checkpoint, so the checkpoint holds what the work came to. The
    // checkpoint is owed meanwhile, so nothing more goes out.
    pub(crate) fn checkpoint_stopped(&self) -> Result<(), WriteError> {
        let _job = JobScope::enter(self.job_id);
        #[cfg(test)]
        self.asked.send_modify(|asked| *asked += 1);
        let mut heard = self.heard.subscribe();
        loop {
            let mut job = self.locked();
            if !job.working() {
                return self.checkpoint_locked(&mut job, true);
            }
            job.owe_checkpoint();
            drop(job);
            // The word that takes the object back moves the count; the tee
            // holds its sender, so the wait never ends on a closed one.
            let _ = tokio::runtime::Handle::current().block_on(heard.changed());
        }
    }

    // A checkpoint write of the job's that cannot succeed. As a job of the
    // pipeline pauses for one, unpacking ends for it: its checkpoint is let
    // go, so the writes queued behind it, the torrent's own among them, go
    // on, and the torrent keeps its files. A word for an earlier write, or
    // for one already let go, is not this one's.
    fn blocked(&self, control: &CheckpointControl, revision: u64, error: &str) {
        let own = self.locked().pending().control();
        if !own.same(control) || own.cancelled() || own.revision() != revision {
            return;
        }
        own.cancel();
        self.refuse(format!(
            "Checkpoint could not be persisted: {error}. Unpacking stopped; the torrent's files are kept."
        ));
    }

    // No checkpoint of the job is written from here on: its rows go.
    pub(crate) fn stop_checkpoints(&self) {
        self.locked().pending().control().cancel();
    }

    // The files the job spends rather than hands over: the archive volumes
    // it consumed and its recovery data. Every handle the job holds is
    // closed first, so what unpacking made can move and its directory go.
    pub(crate) fn spent(&self) -> Result<Vec<PathBuf>, &'static str> {
        let _job = JobScope::enter(self.job_id);
        self.locked().release_delivery_files()
    }

    // The job's runs and repairs stopped, and waited on until each has said
    // it ended: nothing of the job's writes in its directory after.
    pub(crate) async fn stop(&self) {
        let mut heard = self.heard.subscribe();
        self.locked().stop_runs(self.limit);
        loop {
            let out = self.locked().workers_out();
            if !out || heard.changed().await.is_err() {
                return;
            }
        }
    }

    // The job going away: it hears nothing more, and gives back its repair
    // slot or its place in the queue.
    pub(crate) fn forget(&self) {
        self.outside.leave(self.job_id.0);
        let asker = AskerId(self.job_id.0);
        let ticket = self.locked().forget_slot();
        let mut repairs = self.repairs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ticket) = ticket {
            repairs.release(ticket);
        }
        repairs.forget(asker);
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
    }
}

// The tee's job, held: letting it go keeps why its writer refused its
// output, if it did, where the tee reads it without the job.
struct Held<'a> {
    tee: &'a Tee,
    job: MutexGuard<'a, GraphJob>,
}

impl std::ops::Deref for Held<'_> {
    type Target = GraphJob;

    fn deref(&self) -> &GraphJob {
        &self.job
    }
}

impl std::ops::DerefMut for Held<'_> {
    fn deref_mut(&mut self) -> &mut GraphJob {
        &mut self.job
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        if let Some(refusal) = self.job.writer().refusal() {
            self.tee
                .refused
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .get_or_insert_with(|| refusal.job_failure_reason());
        }
    }
}

// The checkpoint names the candidate each node was offered last by its
// place in the list, and the list may have changed since: each node is
// offered the first candidate again, so it retries the list from the top.
fn retry_from_top(offered: &mut std::collections::BTreeMap<weaver_graph::NodeId, usize>) {
    for index in offered.values_mut() {
        *index = 0;
    }
}

// A tee's job hearing its runs, and the database's word on its checkpoints,
// through the mail the pipeline's jobs speak on. The pipeline hands each
// word over and goes on: the tee's own task takes it, under the job's lock,
// which an intake writing to disk may hold.
struct Hearer {
    mail: tokio::sync::mpsc::UnboundedSender<Word>,
    tee: std::sync::Weak<Tee>,
}

impl Hears for Hearer {
    fn said(&self, said: Said, slots: usize) {
        let _ = self.mail.send(Word::Said(said, slots));
    }

    fn hold_clock(&self, paused: bool) {
        let _ = self.mail.send(Word::Clock(paused));
    }

    fn disk_ticket(&self) -> Option<u64> {
        let tee = self.tee.upgrade()?;
        let ticket = tee.disk_ticket.load(std::sync::atomic::Ordering::Acquire);
        (ticket != NO_TICKET).then_some(ticket)
    }

    // A tee gone waits for nothing.
    fn wake_disk(&self) -> bool {
        self.tee.upgrade().is_none_or(|tee| tee.wake_disk_now())
    }

    fn disk_wait(&self) -> Option<DiskWaitOf> {
        let tee = self.tee.upgrade()?;
        tee.disk_wait_slot().clone()
    }
}

// The tee's task: the words it is handed, in the order they came, each
// taken on a thread that may block, working for the tee's job. The job's
// clock is held while every download is paused or the torrent is paused
// on its own. It ends when the tee leaves the mail.
async fn hear(tee: std::sync::Weak<Tee>, mut words: tokio::sync::mpsc::UnboundedReceiver<Word>) {
    let mut paused = (false, false);
    while let Some(word) = words.recv().await {
        let mut batch = vec![word];
        while let Ok(word) = words.try_recv() {
            batch.push(word);
        }
        let tee = tee.clone();
        let taken = tokio::task::spawn_blocking(move || {
            for word in batch {
                let Some(tee) = tee.upgrade() else {
                    break;
                };
                paused = tee.take(word, paused);
            }
            paused
        })
        .await;
        match taken {
            Ok(held) => paused = held,
            Err(_) => return,
        }
    }
}

// What unpacking made, moved from `work` to the same place under `dir`,
// beside the torrent's files and never over one. The job's own records,
// the files it spent, and its copies of the torrent's `listed` files stay
// behind.
//
// Operator ruling, 2026-11-16 (PR 129 question 10): an unpacked file whose
// name is taken is placed under a numbered name; it is never dropped and
// never replaces the file there. Do not ask again.
//
// So each output goes to the first form of its name
// (`weaver_graph::names::numbered_part`: "movie.mkv", "movie (1).mkv", ...)
// that no other output keeps as its own, that the torrent lists for no file
// of its own, that nothing holds, and that the move itself finds free.
// Only the file's own name is numbered, and a name too long for the
// filesystem gives up the end of its stem. A folder of its path whose name
// something that is not a folder holds is numbered the same way
// ("Extras (1)/a.mkv"), once for every output under it.
//
// A name that is not text is placed under the text it reads as, so the
// row, what the torrent holds and the disk have one and the same name for
// the file, and a removal with its files finds exactly it.
//
// Nothing holds a name for an output ahead of it. The move is the claim:
// `moved` moves a whole file to a name and never replaces what is there
// (`runtime::fs::move_file_exclusive`), so a name another writer took, a
// moment before or long ago, is answered `AlreadyExists`, their file stays
// as it was, and the output goes for the next form at once. An output that
// meets `GOES` names taken between the look and the move stays where
// unpacking made it, and the placing ends short of it.
//
// The torrent's row is written after the moves and only from `names`. A
// stop between the two (a crash, a panic) leaves whole output files beside
// the torrent's on no row: nothing removes them for the torrent, and a
// placing run again after a restart puts its outputs under the next
// numbers beside them. That errs toward never deleting a file the row
// cannot show this torrent placed.
//
// Of `kept`, the names an earlier placing put on the row, one with a file
// at it stays as it is, on disk and on the row; one with no file there is
// in `gone`, for the row to let go of.
pub(crate) struct Placed {
    // Each output this placing moved: where it was under the job's folder,
    // and where it is. In the order of `names`.
    moved: Vec<(PathBuf, PathBuf)>,
    // The place of each, relative to the torrent's folder, as the row keeps
    // it and as the disk holds it.
    pub(crate) names: Vec<String>,
    // The names the row kept that hold no file.
    pub(crate) gone: Vec<String>,
    // Why an output was not placed, when one was not: the first reason.
    pub(crate) failed: Option<MoveError>,
}

// How many names one output may find taken between the look that found
// each free and its move.
pub(crate) const GOES: usize = 8;

pub(crate) fn place_outputs(
    work: &Path,
    dir: &Path,
    listed: &HashSet<PathBuf>,
    spent: &HashSet<PathBuf>,
    kept: &[String],
    moved: impl Fn(&Path, &Path) -> std::io::Result<()>,
) -> Placed {
    let mut found = Vec::new();
    walk(work, Path::new(""), &mut found);
    let found: Vec<PathBuf> = found
        .into_iter()
        .filter(|relative| {
            let own = relative.components().next().is_some_and(|first| {
                crate::pipeline::graph::restore::the_jobs(Path::new(first.as_os_str()))
            });
            !own && !listed.contains(relative) && !spent.contains(&work.join(relative))
        })
        .collect();
    let mut placed = Placed {
        moved: Vec::with_capacity(found.len()),
        names: Vec::with_capacity(found.len()),
        gone: kept
            .iter()
            .filter(|name| std::fs::symlink_metadata(dir.join(name)).is_err())
            .cloned()
            .collect(),
        failed: None,
    };
    let mut names = Names::of(dir, listed, &found);
    for relative in found {
        let from = work.join(&relative);
        match names.place(&from, &relative, &moved) {
            // The one place an output's name is put on the list the row is
            // written from: a name goes on the row only for a file this
            // placing itself moved there with a never-replacing move. No
            // name is on it for a file looked at, planned, or found there.
            Ok(Some((to, name))) => {
                placed.moved.push((from, to));
                placed.names.push(name);
            }
            // Its file was gone from the job's folder before it could move.
            Ok(None) => {}
            Err(error) => {
                // One that found no free name stays behind alone; any other
                // failure ends the placing where it is.
                let stops = error.source.kind() != std::io::ErrorKind::AlreadyExists;
                placed.failed.get_or_insert(error);
                if stops {
                    break;
                }
            }
        }
    }
    placed
}

impl Placed {
    // Every output goes back where unpacking made it, each by `moved`, the
    // same never-replacing move, the last placed first. One that will not
    // go back stays where it is, on no row, and is said by its path; the
    // names of those are returned.
    pub(crate) fn put_back(
        self,
        moved: impl Fn(&Path, &Path) -> std::io::Result<()>,
    ) -> Vec<String> {
        let mut there = Vec::new();
        for ((from, to), name) in self.moved.into_iter().zip(self.names).rev() {
            if let Err(error) = moved(&to, &from) {
                tracing::warn!(path = %to.display(), error = %error, "an unpacked file placed beside its torrent could not be put back; it stays there, on no row");
                there.push(name);
            }
        }
        there
    }
}

// The names one torrent's outputs go to under `dir`, as they are placed.
struct Names {
    dir: PathBuf,
    // `dir` with every link in it followed, once it is there.
    root: Option<PathBuf>,
    // The files the torrent lists as its own, there yet or not, by the key
    // of each one's path under `dir`.
    listed: HashSet<String>,
    // By the key of its path under the job's folder: every output's own
    // name and folders, as each takes its own when it is free, and each
    // numbered form tried, so none is tried over a name another output
    // keeps.
    planned: HashSet<String>,
    // Every name an output was placed at in this run, as the row keeps it.
    taken: HashSet<String>,
    // Each folder of an output, by its path under the job's folder: the
    // folder under `dir` its files go in, made by then.
    folders: HashMap<PathBuf, PathBuf>,
}

fn key_of(path: &Path) -> String {
    weaver_graph::names::key(&path.to_string_lossy())
}

// What holds a folder's name.
enum Holder {
    Nothing,
    // A folder, or a link to one inside the torrent's folder.
    Folder,
    // Anything else.
    Other,
}

impl Names {
    fn of(dir: &Path, listed: &HashSet<PathBuf>, found: &[PathBuf]) -> Self {
        let mut planned = HashSet::with_capacity(found.len());
        for relative in found {
            // A folder that is in has every folder above it in.
            for path in relative.ancestors() {
                if path.as_os_str().is_empty() || !planned.insert(key_of(path)) {
                    break;
                }
            }
        }
        Self {
            dir: dir.to_path_buf(),
            root: None,
            listed: listed.iter().map(|path| key_of(path)).collect(),
            planned,
            taken: HashSet::with_capacity(found.len()),
            folders: HashMap::new(),
        }
    }

    // Whether the `n`th form of a name may be tried: `placed`, its path
    // under `dir`, is no file the torrent lists, and `wanted`, its path
    // under the job's folder, is the output's own or a form no other output
    // keeps or tried.
    fn free(&mut self, wanted: &Path, placed: &Path, n: usize) -> bool {
        let key = key_of(wanted);
        let listed = if wanted == placed {
            self.listed.contains(&key)
        } else {
            self.listed.contains(&key_of(placed))
        };
        !listed && (n == 0 || self.planned.insert(key))
    }

    // The output at `from`, `relative` under the job's folder, moved to the
    // first form of its name that is free and that `moved` finds free: its
    // path and its name as the row keeps it. None when its file is gone
    // from the job's folder, which places nothing.
    fn place(
        &mut self,
        from: &Path,
        relative: &Path,
        moved: &impl Fn(&Path, &Path) -> std::io::Result<()>,
    ) -> Result<Option<(PathBuf, String)>, MoveError> {
        if std::fs::symlink_metadata(from).is_err() {
            return Ok(None);
        }
        let above = relative.parent().unwrap_or(Path::new(""));
        let folder = self.folder(above)?;
        let leaf = relative
            .file_name()
            .unwrap_or(relative.as_os_str())
            .to_string_lossy();
        let (mut n, mut met) = (0, 0);
        loop {
            let part = weaver_graph::names::numbered_part(&leaf, n, false);
            let placed = folder.join(&part);
            // Every part of it is text, so this is the name on disk.
            let name = placed.to_string_lossy().into_owned();
            let to = self.dir.join(&placed);
            if !self.taken.contains(&name)
                && self.free(&above.join(&part), &placed, n)
                && std::fs::symlink_metadata(&to).is_err()
            {
                match moved(from, &to) {
                    Ok(()) => {
                        self.taken.insert(name.clone());
                        return Ok(Some((to, name)));
                    }
                    // Taken since the look: theirs, and the next form.
                    Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                        met += 1;
                        if met == GOES {
                            return Err(MoveError {
                                path: from.to_path_buf(),
                                source,
                            });
                        }
                    }
                    Err(_) if std::fs::symlink_metadata(from).is_err() => return Ok(None),
                    Err(source) => {
                        return Err(MoveError {
                            path: from.to_path_buf(),
                            source,
                        });
                    }
                }
            }
            n += 1;
        }
    }

    // The folder under `dir`, relative to it, that the outputs in the
    // folder `wanted` under the job's go in, there by now. Each part of its
    // path is settled once, for every output under it.
    fn folder(&mut self, wanted: &Path) -> Result<PathBuf, MoveError> {
        // The parts of `wanted` not settled yet, the deepest first.
        let mut parts = Vec::new();
        let mut known = wanted;
        let mut placed = loop {
            if let Some(placed) = self.folders.get(known) {
                break placed.clone();
            }
            let (Some(above), Some(part)) = (known.parent(), known.file_name()) else {
                // The torrent's own folder.
                let made = std::fs::create_dir_all(&self.dir)
                    .and_then(|()| std::fs::canonicalize(&self.dir));
                self.root = Some(made.map_err(|source| MoveError {
                    path: self.dir.clone(),
                    source,
                })?);
                self.folders.insert(known.to_path_buf(), PathBuf::new());
                break PathBuf::new();
            };
            parts.push(part);
            known = above;
        };
        let mut wanted = known.to_path_buf();
        for part in parts.into_iter().rev() {
            placed = self.folder_part(&wanted, &placed, &part.to_string_lossy())?;
            wanted.push(part);
            self.folders.insert(wanted.clone(), placed.clone());
        }
        Ok(placed)
    }

    // The first form of the folder `part`, wanted under `wanted` and going
    // under `placed`, that is a folder or can be made one: a name that
    // something not a folder holds is passed over, as a file's is.
    fn folder_part(
        &mut self,
        wanted: &Path,
        placed: &Path,
        part: &str,
    ) -> Result<PathBuf, MoveError> {
        let mut n = 0;
        loop {
            let name = weaver_graph::names::numbered_part(part, n, true);
            let placed = placed.join(&name);
            if self.free(&wanted.join(&name), &placed, n) {
                let path = self.dir.join(&placed);
                match self.held(&path) {
                    Holder::Folder => return Ok(placed),
                    Holder::Other => {}
                    Holder::Nothing => match std::fs::create_dir(&path) {
                        Ok(()) => return Ok(placed),
                        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                            if matches!(self.held(&path), Holder::Folder) {
                                return Ok(placed);
                            }
                        }
                        Err(source) => return Err(MoveError { path, source }),
                    },
                }
            }
            n += 1;
        }
    }

    // What holds the folder name `path`, under the torrent's folder.
    //
    // Operator ruling, 2026-11-16 (PR 129 question 1): nothing is written
    // through a link that points outside the save folder; a link that stays
    // inside it is fine.
    //
    // So a link is a folder only when what it leads to is one inside the
    // torrent's folder. Any other link holds the name as a file does, and
    // the outputs go under the next form of it.
    fn held(&self, path: &Path) -> Holder {
        let Ok(meta) = std::fs::symlink_metadata(path) else {
            return Holder::Nothing;
        };
        if meta.is_dir() {
            return Holder::Folder;
        }
        let inside = meta.is_symlink()
            && std::fs::canonicalize(path).is_ok_and(|target| {
                target.is_dir()
                    && self
                        .root
                        .as_deref()
                        .is_some_and(|root| target.starts_with(root))
            });
        if inside {
            Holder::Folder
        } else {
            Holder::Other
        }
    }
}

// Every file under `root`, relative to it.
fn walk(root: &Path, under: &Path, found: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(root.join(under)) else {
        return;
    };
    for entry in entries.flatten() {
        let relative = under.join(entry.file_name());
        match entry.file_type() {
            Ok(kind) if kind.is_dir() => walk(root, &relative, found),
            Ok(kind) if kind.is_file() => found.push(relative),
            _ => {}
        }
    }
}
