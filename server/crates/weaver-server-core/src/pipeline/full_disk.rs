// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job whose write found the disk full waits for room instead of failing.
//
// A step that could not write left the job's graph saying more than its
// files hold, so the graph goes at once, with no checkpoint taken of it. The
// job keeps its slot and fetches nothing more; once nothing of it is out it
// parks as a paused job does, and waits, shown waiting for disk space, on
// the same ledger and readings as a job the reserve holds. Only a good
// reading whose probe began after the write failed counts.
//
// What such a reading has to show depends on what the step that failed did
// with its own output. A step that removed what it had written as it failed
// (a final move's copy, a repair's output, a recovery's scratch) gave that
// room back, so the disk shows it free again with no room made: the job
// carries the bytes that step needs free to run again, and is let go only
// by a reading that shows at least that much, with more than the reserve
// free to spend. A step that gave nothing back (a download write) has one
// mark, what its disk showed as the write failed, and is let go by a reading
// that shows more than the mark and more than the reserve free to spend. A
// resume of the job by its user lets it go once, whatever the readings say.
// A disk with no good reading keeps it waiting.
//
// A job let go in its download has its articles read back and its graph
// restored from its last checkpoint, as after a restart, and what that
// checkpoint does not hold is written or fetched again. One let go in its
// final move goes on with the move from the working directory it has into
// the destination it had claimed.
//
// Nothing here runs unless a write failed for want of room: every check on
// a path that runs often is one test of an empty map.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{info, warn};

use super::graph::limits::Device;
use super::{ParkedBytes, Pipeline};
use crate::jobs::ids::JobId;
use crate::jobs::model::{FileSummaries, FileSummary, JobArticles, RunState};
use crate::operations::StorageRoot;

// The wait of the kept history archives: the folder read, the probe count
// as the wait began, what the folder's disk showed free then, and whether a
// probe was running then, as a job's hold has them.
pub(crate) struct ArchiveWait {
    dir: PathBuf,
    since: u64,
    mark: Option<u64>,
    running: bool,
}

// What a job waits for: room made on the disk its write failed on.
// The disk a held job waits on: a root's, or the disk of a path that lies
// on none of theirs.
#[derive(Clone, PartialEq, Eq)]
enum Disk {
    Root(StorageRoot),
    Path(PathBuf),
}

// Room promised to a job let go on the figure its step needs: the disk, and
// how much of what it showed is the job's until its step ends.
pub(crate) struct Promised {
    disk: Disk,
    bytes: u64,
    // The device of that disk as the capacity layer held it at the promise,
    // so a job held on a root and one held on a path of the same physical
    // disk are judged against each other. `None` while it was not known:
    // the promise then counts only for jobs held on the very same root or
    // path.
    device: Option<u64>,
}

impl Disk {
    // The device the capacity layer holds for this disk already; nothing is
    // looked at.
    fn device(&self, capacity: &crate::operations::StorageCapacity) -> Option<u64> {
        match self {
            Self::Root(root) => capacity.root_slot_device(*root),
            Self::Path(path) => capacity.held_path_device(path),
        }
    }
}

impl Promised {
    // Whether the promise is on the disk `disk`, whose device is `device`.
    fn on(&self, disk: &Disk, device: Option<u64>) -> bool {
        self.disk == *disk || (device.is_some() && self.device == device)
    }
}

// What the job was doing when its write failed, which is what it goes back
// to when let go, the way a restart takes a job stored in that stage.
pub(crate) enum Stage {
    // Fetching, verifying, repairing or unpacking: its graph is restored
    // from its last checkpoint.
    Graph,
    // Its final move, into this destination: the move goes on from what the
    // failed attempt left. With it, the names in the destination whose copy
    // found no room: whatever the destination holds under one of them is a
    // copy cut short, which the move replaces from the working directory.
    Moving(PathBuf, HashSet<PathBuf>),
    // Its final move had no room to claim a destination: the move claims
    // one and starts, leaving these consumed inputs where they are.
    Claim(HashSet<PathBuf>),
    // The same, for a move a restart found under way with no destination
    // recorded: it is taken up as the restart took it.
    Restored,
}

// What the write that failed says of the room the job waits for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Wants {
    // What the step needs free to run again, when it removed what it had
    // written as it failed, or never wrote it.
    pub(crate) needs: Option<u64>,
    // What was left to write of the file, spool or scratch whose write was
    // refused, when the step names no `needs`: the least a reading has to
    // show over the mark.
    pub(crate) left: Option<u64>,
    // The write was refused by a quota, not by the disk: the disk's free
    // figure already shows room the job cannot use.
    pub(crate) quota: bool,
}

impl Wants {
    // The write that failed with `error` wants `needs` free to run again.
    pub(crate) fn of(error: &std::io::Error, needs: Option<u64>) -> Self {
        Self {
            needs,
            left: None,
            quota: crate::operations::is_quota_exceeded(error),
        }
    }
}

impl From<Option<u64>> for Wants {
    fn from(needs: Option<u64>) -> Self {
        Self {
            needs,
            ..Self::default()
        }
    }
}

pub(crate) struct FullDisk {
    // The least a reading has to show over the mark, when the step names
    // no `needs`: what was left of the file whose write was refused.
    rise: Option<u64>,
    // The hold began with a quota error: the free figure has to rise from
    // the mark by what the step needs, since it showed that much already.
    quota: bool,
    disk: Disk,
    stage: Stage,
    // The disk's count of the probes it had begun as the write failed: only
    // a later probe's reading can show room made since.
    since: u64,
    // What the disk showed free when the write failed: the figure of its
    // newest reading whose probe began no later than the failure, even a
    // stale one. A reading has to show more than this to show room made.
    // It is this wait's alone, and ends with it. `None` while the disk has
    // shown nothing: the first reading since the failure then sets it.
    mark: Option<u64>,
    // What the hold itself gave back to the disk after the mark was taken:
    // the spool files it swept. A reading shows them as room, and they are
    // room the job fills again, so they count on top of the mark.
    freed: u64,
    // What the step that failed needs free to run again, when it removed
    // what it had written as it failed. A reading then has to show this
    // much, and the mark is not consulted.
    needs: Option<u64>,
    // A probe was running as the write failed: it began before the failure,
    // so the figure it publishes takes the mark's place, once.
    running: bool,
    // How many times the root waited on had been pointed elsewhere, and the
    // directory it was pointed at as the write failed: when the root is
    // pointed elsewhere, the wait goes on reading that directory's disk.
    moved: u64,
    dir: PathBuf,
    // Room was made, or the user asked: the job goes on once it has parked.
    go: bool,
    // What the job's graph said was left to fetch, taken before it went:
    // what the job's summaries say once it parks, with no graph to ask.
    left: Option<FileSummaries>,
    // The spool files of the held parts the job's last checkpoint names:
    // the ones its restore reads back, so the ones that stay on disk.
    named: HashSet<PathBuf>,
}

impl Pipeline {
    // The disk a graph job's write on `device` failed on: the one the job's
    // own storage judges a write there against, by the ledger it spends
    // from. The working disk for a job with no graph.
    pub(in crate::pipeline) fn full_disk_root(&self, job_id: JobId, device: Device) -> StorageRoot {
        let complete = self.storage_capacity.ledger(StorageRoot::Complete);
        match self.graph_jobs.get(&job_id) {
            Some(graph) if std::ptr::eq(graph.disk_ledger(device), complete) => {
                StorageRoot::Complete
            }
            _ => StorageRoot::Intermediate,
        }
    }

    // What a hold is made with. No hold is let go by less than the write
    // that was refused: a site that names neither what its step needs nor
    // what was left of its write waits for the free figure to rise by the
    // reserve. Refused by a quota with a `needs`, the mark is not what the
    // disk showed before: a reading from then may have been taken while the
    // step's own output was on disk, so the first reading whose probe began
    // after the failure, and after the step gave its output back, sets it.
    fn full_disk_terms(
        &self,
        wants: Wants,
        shown: Option<u64>,
        running: bool,
    ) -> (Wants, Option<u64>, bool) {
        let left = match (wants.needs, wants.left) {
            (None, None) => Some(self.extraction_limits.min_free_bytes.max(1)),
            (_, left) => left,
        };
        let wants = Wants { left, ..wants };
        if wants.quota && wants.needs.is_some() {
            (wants, None, false)
        } else {
            (wants, shown, running)
        }
    }

    // The directory `root` is pointed at.
    fn root_dir(&self, root: StorageRoot) -> PathBuf {
        match root {
            StorageRoot::Complete => self.complete_dir.clone(),
            _ => self.intermediate_dir.clone(),
        }
    }

    // Whether the job waits for room after a write found the disk full.
    pub(in crate::pipeline) fn held_for_full_disk(&self, job_id: JobId) -> bool {
        !self.full_disk.is_empty() && self.full_disk.contains_key(&job_id)
    }

    // A write of the job found `root` full: the job waits for room there.
    // Its graph goes now, so nothing more is written from what it says, and
    // with it everything that stood on that graph: the job is back at its
    // last checkpoint, in what it holds and in what it counts. The figures
    // it showed still to come are kept. Waiting on the ledger asks for a
    // reading. The job parks once nothing of it is out, on a later turn.
    // `needs` is what the step gave back to the disk as it failed, when it
    // removed its own output.
    pub(in crate::pipeline) fn hold_job_for_full_disk(
        &mut self,
        job_id: JobId,
        root: StorageRoot,
        wants: impl Into<Wants>,
    ) {
        if self.full_disk.contains_key(&job_id) {
            return;
        }
        let wants = wants.into();
        self.end_full_disk_promise(job_id);
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        // What was counted since the checkpoint is fetched, and counted,
        // again.
        let ahead = state
            .downloaded_bytes
            .saturating_sub(state.checkpointed_download_bytes);
        state.downloaded_bytes -= ahead;
        if let Some(category_bytes) = state.category_bytes.as_ref() {
            let _ = category_bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |bytes| {
                Some(bytes.saturating_sub(ahead))
            });
        }
        let mut named = HashSet::new();
        let mut left = None;
        if let Some(graph) = self.graph_jobs.get(&job_id) {
            // The job has in hand only what its last checkpoint holds: a
            // part its graph settled since is fetched again, so it counts
            // as left, in the parts and in the bytes the job shows.
            let mut past = 0u64;
            if matches!(state.articles, JobArticles::Active(_)) {
                let pending = graph.pending();
                left = Some(FileSummaries(
                    state
                        .spec
                        .files
                        .iter()
                        .zip(0u32..)
                        .map(|(file, index)| {
                            let saved = pending.checkpoint_settled(index as usize);
                            let volume = weaver_graph::VolumeIndex(index);
                            past += file
                                .segments
                                .iter()
                                .filter(|segment| {
                                    !saved.contains(&segment.ordinal)
                                        && graph.article_settled(volume, segment.ordinal)
                                })
                                .map(|segment| u64::from(segment.bytes))
                                .sum::<u64>();
                            FileSummary::left(file, |ordinal| {
                                saved.contains(&ordinal) && graph.article_settled(volume, ordinal)
                            })
                        })
                        .collect(),
                ));
            }
            let (remaining, wanted) = Self::graph_remaining(graph, &state.spec);
            let figures = (remaining.saturating_add(past).min(wanted), wanted);
            self.released_remaining.insert(job_id, figures);
            named = graph
                .pending()
                .held_spools()
                .into_iter()
                .map(|spool| state.working_dir.join(spool))
                .collect();
        }
        self.retire_graph_job(job_id);
        // What the disk showed free as the write failed is what a later
        // reading has to beat.
        let look = self.storage_capacity.look(root);
        self.storage_capacity.ledger(root).wait();
        let (
            Wants {
                needs,
                left: rise,
                quota,
            },
            mark,
            running,
        ) = self.full_disk_terms(wants, look.shown, look.probe < look.begun);
        self.full_disk.insert(
            job_id,
            FullDisk {
                rise,
                quota,
                disk: Disk::Root(root),
                stage: Stage::Graph,
                since: look.begun,
                mark,
                freed: 0,
                needs,
                running,
                moved: look.moved,
                dir: self.root_dir(root),
                go: false,
                left,
                named,
            },
        );
        self.forget_parts_past_checkpoint(job_id);
        self.parking_jobs.insert(job_id);
        self.download_dispatch_wake = true;
        self.report_job(job_id);
    }

    // The job held for a full disk lets go of everything kept between an
    // article's arrival and its placement, as it is held and again as it
    // parks: all of it stands where its graph stood before it went, and its
    // next graph, restored from the checkpoint, may stand earlier. A part
    // held in memory is fetched again. A spooled part the checkpoint names
    // stays on disk for the restore to take back; any other is one nothing
    // will read, on a disk with no room, and its file is removed, off this
    // thread, when it lies in the job's own spool directory.
    pub(in crate::pipeline) fn forget_parts_past_checkpoint(&mut self, job_id: JobId) {
        let parts = self.take_job_write_backlog(job_id);
        if parts.is_empty() {
            return;
        }
        let spool = self
            .jobs
            .get(&job_id)
            .map(|state| super::held::held_spool_dir(&state.working_dir));
        let mut stray = Vec::new();
        let mut freed = 0u64;
        for bytes in parts {
            if let ParkedBytes::Spilled {
                path,
                decoded_bytes,
            } = &bytes
                && path.parent() == spool.as_deref()
                && self
                    .full_disk
                    .get(&job_id)
                    .is_none_or(|hold| !hold.named.contains(path))
            {
                stray.push(path.clone());
                freed = freed.saturating_add(*decoded_bytes as u64);
            }
            self.forget_parked_bytes(bytes);
        }
        if stray.is_empty() {
            return;
        }
        // What the sweep gives back is room the job fills again as it
        // fetches those parts: no reading counts it as room made.
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            hold.freed = hold.freed.saturating_add(freed);
        }
        let sweep = tokio::task::spawn_blocking(move || {
            for path in stray {
                if let Err(error) = std::fs::remove_file(&path)
                    && error.kind() != std::io::ErrorKind::NotFound
                {
                    warn!(path = %path.display(), %error, "could not remove a held part's spool file");
                }
            }
        });
        #[cfg(test)]
        self.full_disk_sweeps.push(sweep);
        #[cfg(not(test))]
        drop(sweep);
    }

    // A reading came, a held job parked, or its old graph's last mail was
    // heard. Each wait has one mark: what its disk showed free as the write
    // failed, or what a probe running then published after. A reading
    // counts when it is good and its probe began after the write failed:
    // one that shows more than the mark, with more than the reserve free to
    // spend, shows room was made. A reading that does not changes nothing.
    // With no mark, nothing was shown before to compare with, and the first
    // reading that counts only sets it. A job with room made,
    // or told to try by its user, goes on once it has parked, is not paused
    // and nothing of its old graph is out: its activation is asked for here,
    // so a job with nothing left to fetch runs on with no lane asking.
    pub(in crate::pipeline) fn wake_full_disk(&mut self) {
        if self.full_disk.is_empty() {
            return;
        }
        let reserve = self.extraction_limits.min_free_bytes;
        let mut woken = Vec::new();
        // A promise to a job that left the pipeline ended with it.
        if !self.full_disk_promised.is_empty() {
            let jobs = &self.jobs;
            self.full_disk_promised
                .retain(|job_id, _| jobs.contains_key(job_id));
        }
        for (job_id, hold) in &mut self.full_disk {
            let capacity = &self.storage_capacity;
            // The root waited on was pointed at another directory: the wait
            // stays on the disk that failed, read from here as a path of
            // its own. Its mark stands, and readings count from now.
            let repointed = match &hold.disk {
                Disk::Root(root) if capacity.look(*root).moved != hold.moved => Some(*root),
                _ => None,
            };
            if let Some(root) = repointed {
                capacity.ledger(root).unwait();
                let look = capacity.hold_path(&hold.dir);
                hold.disk = Disk::Path(hold.dir.clone());
                hold.since = look.begun;
                hold.running = false;
                hold.moved = look.moved;
            }
            // What the disk shows now, and what is free on it to spend: for
            // a root, less what is promised on it.
            let (look, spendable) = match &hold.disk {
                Disk::Root(root) => (
                    Some(capacity.look(*root)),
                    capacity
                        .free(*root)
                        .filter(|free| !free.stale)
                        .map(|free| free.bytes),
                ),
                Disk::Path(path) => {
                    let look = capacity.path_look(path);
                    (look, look.and_then(|look| look.shown))
                }
            };
            if let Some(look) = look {
                // A probe running as the write failed began before it:
                // what it published is the newest figure from before the
                // failure, and takes the mark's place.
                if hold.running && look.probe >= hold.since {
                    if let Some(before) = look.figure_of(hold.since) {
                        hold.mark = Some(before);
                    }
                    hold.running = false;
                }
                // A good reading whose probe began after the failure
                // counts. With nothing shown before, it is the mark. One
                // that shows more than the mark, with more than the
                // reserve free to spend, shows room was made.
                if look.probe > hold.since
                    && look.good
                    && let Some(shown) = look.shown
                {
                    let spends = spendable.is_some_and(|free| free > reserve);
                    // What the reading has to show at least, and how much
                    // of it is the job's once it is let go. The step gave
                    // its output back as it failed: the room the step takes.
                    // Refused by a quota, the disk showed that room already:
                    // the figure has to rise by it from the mark. A step
                    // that names no need waits for the figure to rise by
                    // what was left of the file it was writing.
                    // What the hold swept counts on top of the mark.
                    let floor = match (hold.needs, hold.mark, hold.rise) {
                        (Some(needs), _, _) if !hold.quota => Some((needs, needs)),
                        (Some(bytes), Some(mark), _) | (None, Some(mark), Some(bytes)) => {
                            Some((mark.saturating_add(hold.freed).saturating_add(bytes), bytes))
                        }
                        _ => None,
                    };
                    match (floor, hold.mark) {
                        // On top of what is promised to the jobs let go
                        // before it whose steps have not ended: a job let
                        // go here is promised its figure at once, so the
                        // next held job of this pass is judged against it.
                        (Some((floor, bytes)), _) if !hold.go => {
                            let device = hold.disk.device(capacity);
                            let promised = self
                                .full_disk_promised
                                .values()
                                .filter(|promised| promised.on(&hold.disk, device))
                                .fold(0u64, |sum, promised| sum.saturating_add(promised.bytes));
                            if shown >= floor.saturating_add(promised) && spends {
                                hold.go = true;
                                self.full_disk_promised.insert(
                                    *job_id,
                                    Promised {
                                        disk: hold.disk.clone(),
                                        bytes,
                                        device,
                                    },
                                );
                            }
                        }
                        (Some(_), _) => {}
                        // Nothing was shown before to compare with: the
                        // first reading that counts only sets the mark.
                        (None, None) => hold.mark = Some(shown),
                        // Every hold has a figure once it has a mark.
                        (None, Some(_)) => {}
                    }
                }
            }
            // A move let go starts as a first move does, which waits while
            // post-processing is paused: the job stays held until then.
            let moves = !matches!(hold.stage, Stage::Graph);
            if moves && self.shared_state.is_post_processing_paused() {
                continue;
            }
            let paused = self
                .jobs
                .get(job_id)
                .is_some_and(|state| state.run_state == RunState::Paused);
            // A job let go on its figure and paused before it went gives
            // its promise up, so the other jobs held on that disk are not
            // judged against room it is not taking: it is judged anew, by a
            // later reading, once it is resumed.
            if paused && hold.go && self.full_disk_promised.remove(job_id).is_some() {
                hold.go = false;
            }
            let parked = !paused
                && match hold.stage {
                    Stage::Graph => {
                        !self.parking_jobs.contains(job_id)
                            && !self.retiring_graph_jobs.contains_key(job_id)
                            && self.jobs.get(job_id).is_none_or(|state| {
                                !matches!(state.articles, JobArticles::Active(_))
                            })
                    }
                    Stage::Moving(..) | Stage::Claim(_) | Stage::Restored => true,
                };
            if hold.go && parked {
                woken.push(*job_id);
            }
        }
        for job_id in woken {
            match self.drop_full_disk_hold(job_id) {
                Some(stage @ (Stage::Moving(..) | Stage::Claim(_) | Stage::Restored)) => {
                    info!(
                        job_id = job_id.0,
                        "the job held for a full disk goes on with its final move"
                    );
                    self.full_disk_moves.push((job_id, stage));
                }
                _ => {
                    info!(
                        job_id = job_id.0,
                        "the job held for a full disk runs on from its last checkpoint"
                    );
                    self.request_job_activation(job_id);
                    self.download_dispatch_wake = true;
                }
            }
            self.report_job(job_id);
        }
    }

    // The final move of a job found its destination full: the job waits
    // for room on the destination's disk, shown as any job held for a full
    // disk, and nothing of it is failed. What the move delivered stays in
    // the destination and what it did not stays where it was. The disk is a
    // root's only when the capacity cache already holds the device of the
    // path and of the root and they are one; nothing is looked at here. Any
    // other path is waited on as itself, the disk that failed, and read
    // while a job waits on it. `needs` is what the move needs free to go on,
    // when the copy that failed removed what it wrote.
    pub(in crate::pipeline) fn hold_move_for_full_disk(
        &mut self,
        job_id: JobId,
        stage: Stage,
        path: &std::path::Path,
        message: &str,
        wants: impl Into<Wants>,
    ) {
        if !self.jobs.contains_key(&job_id) || self.full_disk.contains_key(&job_id) {
            return;
        }
        let wants = wants.into();
        self.end_full_disk_promise(job_id);
        warn!(
            job_id = job_id.0,
            path = %path.display(),
            error = message,
            "the final move found the disk full; the job waits for room"
        );
        let disk = self
            .storage_capacity
            .known_root_of(path)
            .filter(|root| matches!(root, StorageRoot::Complete | StorageRoot::Intermediate))
            .map_or_else(|| Disk::Path(path.to_path_buf()), Disk::Root);
        let (look, dir) = match &disk {
            Disk::Root(root) => {
                let look = self.storage_capacity.look(*root);
                self.storage_capacity.ledger(*root).wait();
                (look, self.root_dir(*root))
            }
            Disk::Path(path) => (self.storage_capacity.hold_path(path), path.clone()),
        };
        let (
            Wants {
                needs,
                left: rise,
                quota,
            },
            mark,
            running,
        ) = self.full_disk_terms(wants, look.shown, look.probe < look.begun);
        self.full_disk.insert(
            job_id,
            FullDisk {
                rise,
                quota,
                disk,
                stage,
                since: look.begun,
                mark,
                freed: 0,
                needs,
                running,
                moved: look.moved,
                dir,
                go: false,
                left: None,
                named: HashSet::new(),
            },
        );
        self.report_job(job_id);
    }

    // The folder a database write refused for want of room waits on: the
    // folder of the SQLite file, read like any held path. A Postgres server
    // has no local folder: its disk cannot be read from here, so the job
    // waits on the data folder by the reserve rule and tries the write
    // again when that shows a rise.
    pub(in crate::pipeline) fn database_dir(&self) -> PathBuf {
        self.db
            .database_target()
            .sqlite_path()
            .ok()
            .flatten()
            .and_then(|file| file.parent().map(std::path::Path::to_path_buf))
            .filter(|folder| folder.is_absolute())
            .unwrap_or_else(|| self.script_data_dir.clone())
    }

    // The writer kept an archive for want of room, or a kept one committed.
    // While any is kept, each of their rows, retiring and with no runtime
    // left, says it waits for disk space, and the database's folder is read
    // as a held path. With none kept the path is let go.
    pub(in crate::pipeline) fn kept_archives_heard(&mut self) {
        let kept = self.db.kept_archive_jobs();
        if kept.is_empty() {
            if let Some(wait) = self.archive_wait.take() {
                self.storage_capacity.release_path(&wait.dir);
            }
            return;
        }
        self.shared_state.queue.write(|inner| {
            let mut shown = false;
            for job_id in &kept {
                if let Some(mut row) = inner.job_mut(*job_id)
                    && row.report.retiring
                    && row.report.job.download_wait_reason.is_none()
                {
                    row.report.job.download_wait_reason =
                        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON.into());
                    shown = true;
                }
            }
            if shown {
                crate::pipeline::queue::Change::changed(())
            } else {
                crate::pipeline::queue::Change::unchanged(())
            }
        });
        if self.archive_wait.is_none() {
            let dir = self.database_dir();
            let look = self.storage_capacity.hold_path(&dir);
            warn!(
                path = %dir.display(),
                "a history write found the database's disk full; it waits for room"
            );
            self.archive_wait = Some(ArchiveWait {
                dir,
                since: look.begun,
                mark: look.shown,
                running: look.probe < look.begun,
            });
        }
    }

    // A reading came while archives are kept. The rule is the one a hold
    // with no figure has: a good reading whose probe began after the wait
    // did, showing the reserve more than the mark and more than the reserve
    // free, rings the writer to try the kept archives, once. The mark moves
    // to that reading, so an archive refused again waits for a further rise.
    pub(in crate::pipeline) fn wake_kept_archives(&mut self) {
        let Some(wait) = &mut self.archive_wait else {
            return;
        };
        let Some(look) = self.storage_capacity.path_look(&wait.dir) else {
            return;
        };
        if wait.running && look.probe >= wait.since {
            if let Some(before) = look.figure_of(wait.since) {
                wait.mark = Some(before);
            }
            wait.running = false;
        }
        if look.probe > wait.since
            && look.good
            && let Some(shown) = look.shown
        {
            let reserve = self.extraction_limits.min_free_bytes;
            match wait.mark {
                None => wait.mark = Some(shown),
                Some(mark) if shown >= mark.saturating_add(reserve.max(1)) && shown > reserve => {
                    wait.mark = Some(shown);
                    self.db.retry_kept_archives();
                }
                Some(_) => {}
            }
        }
    }

    // The final moves of the jobs let go start here, at the top of a turn.
    // A move that had claimed its destination goes on into it from the
    // working directory the job has, both as they stood when it stopped. A
    // move that had not claims one, as it was doing.
    pub(in crate::pipeline) async fn resume_full_disk_moves(&mut self) {
        if self.full_disk_moves.is_empty() {
            return;
        }
        for (job_id, stage) in std::mem::take(&mut self.full_disk_moves) {
            if !self.jobs.contains_key(&job_id) {
                continue;
            }
            // A cancel taken while the job was held is being archived: the
            // move waits for its answer, and goes on only if the job is
            // handed back.
            if !self.begin_let_go_move(job_id) {
                self.full_disk_moves.push((job_id, stage));
                continue;
            }
            match stage {
                Stage::Graph => {}
                Stage::Moving(dest, again) => self.resume_held_move(job_id, dest, again).await,
                Stage::Claim(left_behind) => {
                    if let Err(error) = self.launch_move(job_id, None, left_behind).await {
                        self.fail_job(job_id, error);
                    }
                }
                Stage::Restored => self.resume_restored_move(job_id).await,
            }
            // A move that did not start, held again, failed or found
            // finished, is not running: a started one reserved its
            // destination.
            if !self.reserved_complete_destinations.contains_key(&job_id)
                && self.inflight_moves.remove(&job_id)
            {
                self.report_job(job_id);
            }
        }
    }

    // The move of a job let go begins, before anything is awaited: in one
    // write of the queue, a cancel already taken stops it, and with none the
    // job shows as moving, so a cancel from then on is refused as for any
    // moving job. No cancel is accepted between the two. The job counts as
    // moving from here, so every report of it says so until the move ends.
    // False when a cancel was taken first.
    pub(in crate::pipeline) fn begin_let_go_move(&mut self, job_id: JobId) -> bool {
        let begun = self.shared_state.queue.write(|inner| {
            let Some(mut row) = inner.job_mut(job_id) else {
                return crate::pipeline::queue::Change::unchanged(true);
            };
            if row.intents.cancelled.is_some() {
                return crate::pipeline::queue::Change::unchanged(false);
            }
            row.report.moving = true;
            crate::pipeline::queue::Change::changed(true)
        });
        if begun {
            self.inflight_moves.insert(job_id);
        }
        begun
    }

    // The held job parked with no graph to say what it has in hand: its
    // summaries are what its graph said was left as it went.
    pub(in crate::pipeline) fn settle_held_summaries(&mut self, job_id: JobId) {
        let Some(left) = self
            .full_disk
            .get_mut(&job_id)
            .and_then(|hold| hold.left.take())
        else {
            return;
        };
        if let Some(state) = self.jobs.get_mut(&job_id)
            && let JobArticles::Queued(summaries) = &mut state.articles
            && summaries.0.len() == left.0.len()
        {
            *summaries = left;
        }
    }

    // The user resumed the job: it tries its disk again now, once, whatever
    // the readings say. A write that fails again holds it again, with a mark
    // of its own.
    pub(in crate::pipeline) fn try_full_disk_now(&mut self, job_id: JobId) {
        if self.full_disk.is_empty() {
            return;
        }
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            // Its step takes the room it needs all the same, so that much
            // is promised to it like any job let go on its figure.
            if let (false, Some(bytes)) = (hold.go, hold.needs.or(hold.rise)) {
                let disk = hold.disk.clone();
                let device = disk.device(&self.storage_capacity);
                self.full_disk_promised.insert(
                    job_id,
                    Promised {
                        disk,
                        bytes,
                        device,
                    },
                );
            }
            hold.go = true;
        }
    }

    // The step of a job let go on the figure it needs ended: the job is held
    // again, reached its final move from its download, finished its move, or
    // is gone. The room promised to it is counted no longer, and the jobs
    // still held on that disk are judged on a reading taken from now, since
    // one from before shows room that step may have filled.
    pub(in crate::pipeline) fn end_full_disk_promise(&mut self, job_id: JobId) {
        if self.full_disk_promised.is_empty() {
            return;
        }
        let Some(ended) = self.full_disk_promised.remove(&job_id) else {
            return;
        };
        let begun = match &ended.disk {
            Disk::Root(root) => Some(self.storage_capacity.look(*root).begun),
            Disk::Path(path) => self.storage_capacity.path_look(path).map(|look| look.begun),
        };
        let Some(begun) = begun else {
            return;
        };
        // A job held on the same device under another root or path counts
        // its own disk's probes: readings of it count from now.
        let capacity = &self.storage_capacity;
        for hold in self.full_disk.values_mut() {
            if hold.go {
                continue;
            }
            let begun = if hold.disk == ended.disk {
                Some(begun)
            } else if ended.on(&hold.disk, hold.disk.device(capacity)) {
                match &hold.disk {
                    Disk::Root(root) => Some(capacity.look(*root).begun),
                    Disk::Path(path) => capacity.path_look(path).map(|look| look.begun),
                }
            } else {
                None
            };
            if let Some(begun) = begun {
                hold.since = hold.since.max(begun);
                hold.running = false;
            }
        }
    }

    // What is promised on the disks to the jobs let go, in all.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_promised_bytes(&self) -> u64 {
        self.full_disk_promised
            .values()
            .map(|promised| promised.bytes)
            .sum()
    }

    // The names in the destination whose copy the held move cut short.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_cut_short(&self, job_id: JobId) -> Vec<PathBuf> {
        match self.full_disk.get(&job_id).map(|hold| &hold.stage) {
            Some(Stage::Moving(_, again)) => again.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }

    // What a reading of the held job's disk has to show more than.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_mark(&self, job_id: JobId) -> Option<u64> {
        self.full_disk.get(&job_id).and_then(|hold| hold.mark)
    }

    // What a reading of the held job's disk has to show at least.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_needs(&self, job_id: JobId) -> Option<u64> {
        self.full_disk.get(&job_id).and_then(|hold| hold.needs)
    }

    // What a reading has to show over the mark, by what was left of the file
    // whose write was refused, and whether a quota refused it.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_rise(&self, job_id: JobId) -> (Option<u64>, bool) {
        self.full_disk
            .get(&job_id)
            .map_or((None, false), |hold| (hold.rise, hold.quota))
    }

    // Whether the held job waits on a path of its own, not on a root.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_on_path(&self, job_id: JobId) -> bool {
        self.full_disk
            .get(&job_id)
            .is_some_and(|hold| matches!(hold.disk, Disk::Path(_)))
    }

    // The job no longer waits for room after a full disk: it went on, or
    // left the pipeline.
    pub(in crate::pipeline) fn drop_full_disk_hold(&mut self, job_id: JobId) -> Option<Stage> {
        if self.full_disk.is_empty() {
            return None;
        }
        let hold = self.full_disk.remove(&job_id)?;
        match &hold.disk {
            Disk::Root(root) => self.storage_capacity.ledger(*root).unwait(),
            Disk::Path(path) => self.storage_capacity.release_path(path),
        }
        Some(hold.stage)
    }
}
