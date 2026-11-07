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

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;
use weaver_graph::slots::AskerId;
use weaver_graph::{Listed, VolumeIndex};

use super::layout::{PieceLayout, Span};
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
use tokio::sync::mpsc::UnboundedSender;

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
    // The first step of the job that failed, and why: unpacking is over for
    // the torrent, which keeps its files and seeds on.
    broken: Mutex<Option<String>>,
    // Counts the words the job's runs said, heard.
    heard: tokio::sync::watch::Sender<u64>,
    // A lend an intake found the repair slots busy for, left to the
    // service's next settle.
    owes_lend: std::sync::atomic::AtomicBool,
    // Counts the checkpoints asked of the job, each before it waits on the
    // job's lock.
    #[cfg(test)]
    pub(crate) asked: tokio::sync::watch::Sender<u64>,
}

// A word for a tee's job, with the repair slots it may step with.
type Word = (Said, usize);

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
        let files = parts.plan.root_files(&parts.layout);
        let job = GraphJob::admit(parts.job_id.0, &files, storage);
        Self::joined(parts, job)
    }

    fn joined(parts: Parts, job: GraphJob) -> Arc<Self> {
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
            broken: Mutex::new(None),
            heard: tokio::sync::watch::Sender::new(0),
            owes_lend: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            asked: tokio::sync::watch::Sender::new(0),
        });
        let (mail, words) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(hear(Arc::downgrade(&tee), words));
        tee.outside.join(parts.job_id.0, Arc::new(Hearer { mail }));
        tee
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
        } else if let Err(error) = self.step(slots, |job, repairs, limit| {
            job.run_said(said, repairs, limit)
        }) {
            tracing::warn!(job_id = self.job_id.0, error = %error, "a torrent's unpacking step failed");
        }
        self.heard.send_modify(|heard| *heard += 1);
        self.signal.mark(self.job_id);
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

    fn locked(&self) -> MutexGuard<'_, GraphJob> {
        self.job.lock().unwrap_or_else(PoisonError::into_inner)
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
        let mut job = self.locked();
        let mut repairs = self.repairs.lock().unwrap_or_else(PoisonError::into_inner);
        let result = step(&mut job, &mut repairs, limit);
        drop(job);
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
        self.kept(result)
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
        if self.is_broken() {
            return false;
        }
        let took = self.kept(self.with(|job| {
            let mut took = false;
            for share in self.unsettled(job, piece) {
                took |= job.take_in(self.decoded(&share, held), self.limit)?;
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
        let result = job.lend(&mut repairs, self.limit);
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
            .locked()
            .writer()
            .refusal()
            .map(|refusal| refusal.job_failure_reason());
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

    // Checkpoint the job as it stands, again for as long as a file of its
    // moved: the move is recorded by the next one. The database's word on
    // each comes through `mail`.
    pub(crate) fn checkpoint(
        &self,
        db: &crate::Database,
        mail: &UnboundedSender<Said>,
    ) -> Result<(), WriteError> {
        #[cfg(test)]
        self.asked.send_modify(|asked| *asked += 1);
        let mut job = self.locked();
        while !job.pending().control().cancelled()
            && checkpoint_job(&mut job, self.job_id, db, mail, Vec::new(), None, true)?
        {}
        Ok(())
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
}

impl Hears for Hearer {
    fn said(&self, said: Said, slots: usize) {
        let _ = self.mail.send((said, slots));
    }
}

// The tee's task: the words it is handed, in the order they came, each
// taken on a thread that may block. It ends when the tee leaves the mail.
async fn hear(tee: std::sync::Weak<Tee>, mut words: tokio::sync::mpsc::UnboundedReceiver<Word>) {
    while let Some(word) = words.recv().await {
        let mut batch = vec![word];
        while let Ok(word) = words.try_recv() {
            batch.push(word);
        }
        let tee = tee.clone();
        let taken = tokio::task::spawn_blocking(move || {
            for (said, slots) in batch {
                let Some(tee) = tee.upgrade() else {
                    return;
                };
                tee.heard(said, slots);
            }
        })
        .await;
        if taken.is_err() {
            return;
        }
    }
}

// What unpacking made, to move from `work` to the same place under `dir`,
// beside the torrent's files and never over one. The job's own records,
// the files it spent, and its copies of the torrent's `listed` files stay
// behind.
pub(crate) struct Outputs {
    moves: Vec<(PathBuf, PathBuf)>,
    // Each move's destination, relative to `dir`.
    pub(crate) names: Vec<String>,
}

pub(crate) fn plan_outputs(
    work: &Path,
    dir: &Path,
    listed: &[PathBuf],
    spent: &[PathBuf],
) -> Outputs {
    let mut found = Vec::new();
    walk(work, Path::new(""), &mut found);
    let moves: Vec<(PathBuf, PathBuf)> = found
        .into_iter()
        .filter(|relative| {
            let own = relative.components().next().is_some_and(|first| {
                crate::pipeline::graph::restore::the_jobs(Path::new(first.as_os_str()))
            });
            !own && !listed.contains(relative)
                && !spent.contains(&work.join(relative))
                && !dir.join(relative).exists()
        })
        .map(|relative| (work.join(&relative), dir.join(relative)))
        .collect();
    let names = moves
        .iter()
        .filter_map(|(_, to)| {
            let relative = to.strip_prefix(dir).ok()?;
            Some(relative.to_string_lossy().into_owned())
        })
        .collect();
    Outputs { moves, names }
}

impl Outputs {
    // Moved; false when a move failed, which leaves the rest where they were.
    pub(crate) fn place(&self, work: &Path) -> bool {
        match super::relocate::move_files(work, &self.moves, super::relocate::Existing::Keep) {
            Ok(_) => true,
            Err(error) => {
                tracing::warn!(error = %error, "a torrent's unpacked files did not move beside it");
                false
            }
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
