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
// job held in its download starts again as any job starts: by a fresh
// reading that leaves room for what it still has to fetch, which is
// promised to it. A resume by its user is judged the same way. A disk with
// no good reading keeps it waiting.
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
use crate::jobs::model::{FileSummaries, FileSummary, JobArticles, RunState};
use crate::operations::StorageRoot;
use weaver_server_base::jobs::ids::JobId;
pub use weaver_server_disk::operations::room_wait::Wants;
use weaver_server_disk::operations::room_wait::{Disk, Promised, RoomWait, Waiter};

// The wait of the kept history archives: the folder read, how many
// archives the writer said it keeps, the probe count as the wait began, and
// the least the folder's disk has shown free since the last attempt.
pub(crate) struct ArchiveWait {
    dir: PathBuf,
    pub(in crate::pipeline) kept: usize,
    since: u64,
    mark: Option<u64>,
}

// How much more than its mark the database's disk has to show free for the
// kept archives to be tried again. A history row is kilobytes: the figure
// says nothing of what a write needs, and only keeps a retry that was
// refused from being rung again by the noise of a busy disk.
pub(in crate::pipeline) const ARCHIVE_RETRY_RISE: u64 = 16 << 20;

// What the job was doing when its write failed, which is what it goes back
// to when let go, the way a restart takes a job stored in that stage.
pub enum Stage {
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

// A job held for a full disk: what it waits for, room made on the disk its
// write failed on, judged by the one wait every kind of job shares
// (operations/room_wait.rs), and what it goes back to when let go.
pub(crate) struct FullDisk {
    wait: RoomWait,
    stage: Stage,
    // Room was made, or the user asked: the job goes on once it has parked.
    go: bool,
    // Its user resumed it while it was held in its download: each wake
    // judges its start by room alone, as any start is judged, and not by
    // whether its disk showed more than at the failure, until it starts,
    // is paused again or is held anew.
    resumed: bool,
    // What the job's graph said was left to fetch, taken before it went:
    // what the job's summaries say once it parks, with no graph to ask.
    left: Option<FileSummaries>,
    // The spool files of the held parts the job's last checkpoint names:
    // the ones its restore reads back, so the ones that stay on disk.
    named: HashSet<PathBuf>,
    // What the job waits for on a root's ledger and has not been promised,
    // and the root: a new start there leaves it aside. Set while the job
    // waits on a root, not paused, not let go, and not refused by a quota.
    waiting: Option<(StorageRoot, u64)>,
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
        // Its graph gave back what its start promised and did not land; a
        // start promise its graph never took goes back too. Held, the job
        // holds no room: its start again is promised anew.
        self.release_start_promise(job_id);
        // What the disk showed free as the write failed is what a later
        // reading has to beat.
        let wait = RoomWait::begin(
            &self.storage_capacity,
            Disk::Root(root),
            self.root_dir(root),
            wants,
            self.extraction_limits.min_free_bytes,
        );
        self.full_disk.insert(
            job_id,
            FullDisk {
                wait,
                stage: Stage::Graph,
                go: false,
                resumed: false,
                left,
                named,
                waiting: None,
            },
        );
        self.note_full_disk_waiting(job_id);
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
            hold.wait.freed = hold.wait.freed.saturating_add(freed);
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
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        self.full_disk_sweeps.push(sweep);
        #[cfg(not(any(test, feature = "pipeline-test-fixtures")))]
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
        let promises = self.storage_capacity.promises();
        {
            let jobs = &self.jobs;
            promises.retain(Waiter::Usenet, |job_id| jobs.contains_key(&job_id));
        }
        // A torrent's step let go on its figure ended: the jobs held on its
        // disk count readings from then.
        for (ended, begun) in promises.take_ended(Waiter::Usenet) {
            for hold in self.full_disk.values_mut().filter(|hold| !hold.go) {
                hold.wait
                    .promise_ended(&self.storage_capacity, &ended, begun);
            }
        }
        // A job held in its download starts again as any job starts: once a
        // fresh reading whose probe began after its write failed leaves room
        // for what it still has to fetch, or what its step needs free when
        // that is more, less everything promised on the disk. Those bytes
        // are promised to it as it is let go; its step books its own room
        // when it runs, so that is judged and not promised. That reading
        // also has to show more than the disk did as the write failed, as
        // every wait's does: a disk whose free figure never moves, as one
        // whose free space is not what stops the write, does not send it
        // round. A resumed job is judged by room alone until it goes on. One
        // not paused is judged; one held on a path, its root pointed
        // elsewhere, waits as a move does. One a quota refused keeps waiting
        // for its figure to rise: the free figure shows room the quota does
        // not give it.
        //
        // First each such wait takes the mark a reading sets, so what it
        // has to beat is known before it is judged.
        for hold in self.full_disk.values_mut() {
            if matches!(hold.stage, Stage::Graph)
                && !hold.go
                && !hold.wait.quota
                && hold.wait.root_read_since(&self.storage_capacity).is_some()
            {
                hold.wait
                    .judge(&self.storage_capacity, reserve, true, |_, _| 0);
            }
        }
        let starts: Vec<(JobId, StorageRoot, u64, u64)> = self
            .full_disk
            .iter()
            .filter(|(job_id, hold)| {
                matches!(hold.stage, Stage::Graph)
                    && !hold.go
                    && !hold.wait.quota
                    && self
                        .jobs
                        .get(job_id)
                        .is_none_or(|state| state.run_state != RunState::Paused)
            })
            .filter_map(|(job_id, hold)| {
                let (root, read) = hold.wait.root_read_since(&self.storage_capacity)?;
                let read = read && (hold.resumed || hold.wait.shows_rise(&self.storage_capacity));
                let (remaining, need) = self.held_start_need(*job_id, &hold.wait);
                let size = self.held_start_size(*job_id).max(need);
                (read && self.held_start_room(root, need, size))
                    .then_some((*job_id, root, remaining, size))
            })
            .collect();
        for (job_id, root, remaining, size) in starts {
            if let Ok(promise) = self.book_held_start_promise_on(root, remaining, size) {
                self.release_start_promise(job_id);
                self.start_promises.insert(job_id, promise);
                // Promised now: what it waited for comes off in the same
                // step, so a start counts it once.
                if let Some(hold) = self.full_disk.get_mut(&job_id) {
                    hold.go = true;
                    set_waiting(&self.storage_capacity, hold, None);
                }
            }
        }
        let mut given_back = Vec::new();
        // Jobs held in their download let go here, by a quota's figure or a
        // path's: their start is promised what they have left, as one let
        // go above is, so their graph's bytes land on it.
        let mut judged_starts = Vec::new();
        for (job_id, hold) in &mut self.full_disk {
            let capacity = &self.storage_capacity;
            let graph_on_root = matches!(hold.stage, Stage::Graph)
                && !hold.wait.quota
                && hold.wait.root_read_since(capacity).is_some();
            // A job let go here is promised its figure at once, so the next
            // held job of this pass is judged against it. So is a torrent
            // let go on the same disk. A job held in its download on its
            // root was judged above; here only its mark moves.
            let room = hold.wait.judge(
                capacity,
                reserve,
                hold.go || graph_on_root,
                |disk, device| capacity.promises().on(disk, device),
            );
            if let Some((bytes, device)) = room {
                hold.go = true;
                let promised = Promised {
                    disk: hold.wait.disk.clone(),
                    bytes,
                    device,
                };
                // A start let go here is promised on the ledger as well,
                // below: a start judged on the ledger counts it there once.
                if matches!(hold.stage, Stage::Graph) {
                    judged_starts.push(*job_id);
                    capacity
                        .promises()
                        .insert_ledgered(Waiter::Usenet, *job_id, promised);
                } else {
                    capacity
                        .promises()
                        .insert(Waiter::Usenet, *job_id, promised);
                }
                // Promised now: what it waited for comes off in the same
                // step, so a start counts it once.
                set_waiting(capacity, hold, None);
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
            if paused
                && hold.go
                && self
                    .storage_capacity
                    .promises()
                    .remove(Waiter::Usenet, *job_id)
                    .is_some()
            {
                hold.go = false;
                if matches!(hold.stage, Stage::Graph) {
                    given_back.push(*job_id);
                }
            }
            // So does a job let go to start again in its download: its
            // start's promise goes back, and its resume is judged anew.
            if paused && hold.go && graph_on_root {
                hold.go = false;
                given_back.push(*job_id);
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
        for job_id in given_back {
            self.release_start_promise(job_id);
        }
        // What each job still held waits for, as it stands after this pass:
        // one paused or let go waits for nothing, one whose root was pointed
        // elsewhere waits on a path, which no ledger counts.
        let waits: Vec<(JobId, Option<(StorageRoot, u64)>)> = self
            .full_disk
            .iter()
            .map(|(job_id, hold)| (*job_id, self.full_disk_waiting_of(*job_id, hold)))
            .collect();
        for (job_id, waiting) in waits {
            if let Some(hold) = self.full_disk.get_mut(&job_id) {
                set_waiting(&self.storage_capacity, hold, waiting);
            }
        }
        for job_id in judged_starts {
            if self.full_disk.get(&job_id).is_some_and(|hold| hold.go) {
                let remaining = self
                    .full_disk
                    .get(&job_id)
                    .map_or(0, |hold| self.held_start_need(job_id, &hold.wait).0);
                let size = self.held_start_size(job_id).max(remaining);
                self.promise_start_unjudged(job_id, remaining, size);
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
        let dir = match &disk {
            Disk::Root(root) => self.root_dir(*root),
            Disk::Path(path) => path.clone(),
        };
        let wait = RoomWait::begin(
            &self.storage_capacity,
            disk,
            dir,
            wants,
            self.extraction_limits.min_free_bytes,
        );
        self.full_disk.insert(
            job_id,
            FullDisk {
                wait,
                stage,
                go: false,
                resumed: false,
                left: None,
                named: HashSet::new(),
                waiting: None,
            },
        );
        self.note_full_disk_waiting(job_id);
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

    // The writer said something of the archives it keeps, of one job. The
    // pipeline counts them from what it is told and never reads the
    // writer's list: each message costs one row looked up by its id.
    // While it counts any, the database's folder is read as a held path;
    // the path is let go when the count is back to none.
    pub(in crate::pipeline) fn archive_news(&mut self, news: crate::persistence::ArchiveNews) {
        use crate::persistence::ArchiveNews;
        match news {
            // The job's row, retiring and with no runtime left, says it
            // waits for disk space.
            ArchiveNews::Kept(job_id) => {
                self.show_archive_wait(job_id, true);
                if let Some(wait) = &mut self.archive_wait {
                    wait.kept += 1;
                    return;
                }
                let dir = self.database_dir();
                let look = self.storage_capacity.hold_path(&dir);
                warn!(
                    path = %dir.display(),
                    "a history write found the database's disk full; it waits for room"
                );
                self.archive_wait = Some(ArchiveWait {
                    dir,
                    kept: 1,
                    since: look.begun,
                    mark: None,
                });
            }
            // Written, the terminal path removes the row. Failed for
            // another reason, the archive is lost as any that fails so is:
            // the row stays retiring and says no more that it waits.
            ArchiveNews::LetGo { job_id, written } => {
                if !written {
                    self.show_archive_wait(job_id, false);
                }
                let Some(wait) = &mut self.archive_wait else {
                    return;
                };
                wait.kept = wait.kept.saturating_sub(1);
                if wait.kept == 0 {
                    self.storage_capacity.release_path(&wait.dir);
                    self.archive_wait = None;
                }
            }
            ArchiveNews::More { from } => {
                if self.archive_wait.is_some() {
                    self.db.retry_kept_archives(from);
                }
            }
        }
    }

    // Say on the job's retiring row that it waits for disk space, or that
    // it no longer does.
    fn show_archive_wait(&self, job_id: JobId, waits: bool) {
        let reason = waits.then(|| crate::jobs::handle::DISK_SPACE_WAIT_REASON.to_owned());
        self.shared_state
            .queue
            .write(|inner| match inner.job_mut(job_id) {
                Some(mut row)
                    if row.report.retiring && row.report.job.download_wait_reason != reason =>
                {
                    row.report.job.download_wait_reason = reason;
                    crate::pipeline::queue::Change::changed(())
                }
                _ => crate::pipeline::queue::Change::unchanged(()),
            });
    }

    // A reading came while archives are kept. The mark is the least a good
    // reading, from a probe begun after the wait did, has shown free since
    // the last attempt. A reading that shows [`ARCHIVE_RETRY_RISE`] more
    // than the mark rings the writer to try the kept archives, once, and
    // is the mark from then: an archive refused again waits for a further
    // rise. The download reserve has no part in it: the database's disk
    // may be smaller than the reserve. The first good reading of the wait
    // rings once whatever it shows: room made between the refusal and that
    // reading would otherwise be the mark and never be seen as a rise.
    pub(in crate::pipeline) fn wake_kept_archives(&mut self) {
        let Some(wait) = &mut self.archive_wait else {
            return;
        };
        let Some(look) = self.storage_capacity.path_look(&wait.dir) else {
            return;
        };
        if look.probe > wait.since
            && look.good
            && let Some(shown) = look.shown
        {
            let first = wait.mark.is_none();
            let least = wait.mark.map_or(shown, |mark| mark.min(shown));
            wait.mark = Some(least);
            if first || shown >= least.saturating_add(ARCHIVE_RETRY_RISE) {
                wait.mark = Some(shown);
                self.db.retry_kept_archives(0);
            }
        }
    }

    // Shutdown does not wait for room. The kept archives are tried once
    // more, behind the writes already waiting.
    pub(in crate::pipeline) fn retry_kept_archives_at_drain(&self) {
        if self.archive_wait.is_some() {
            self.db.retry_kept_archives(0);
        }
    }

    // After the drain's flush: the jobs whose archive is still kept are
    // named in one warning. Each stays terminal in the active table, and
    // start-up recovery archives it.
    pub(in crate::pipeline) fn warn_of_kept_archives(&self) -> Vec<JobId> {
        if self.archive_wait.is_none() {
            return Vec::new();
        }
        let kept = self.db.kept_archive_jobs();
        if !kept.is_empty() {
            warn!(
                job_ids = ?kept.iter().map(|job_id| job_id.0).collect::<Vec<_>>(),
                "history writes still wait for room on the database's disk at shutdown; \
                 the next start archives these jobs"
            );
        }
        kept
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

    // The step of a job let go on the figure it needs ended: the job is held
    // again, reached its final move from its download, finished its move, or
    // is gone. The room promised to it is counted no longer, and the jobs
    // still held on that disk are judged on a reading taken from now, since
    // one from before shows room that step may have filled.
    pub(in crate::pipeline) fn end_full_disk_promise(&mut self, job_id: JobId) {
        let capacity = &self.storage_capacity;
        let Some((ended, begun)) = capacity.promises().end(capacity, Waiter::Usenet, job_id) else {
            return;
        };
        // A job held on the same device under another root or path counts
        // its own disk's probes: readings of it count from now.
        for hold in self.full_disk.values_mut() {
            if hold.go {
                continue;
            }
            hold.wait.promise_ended(capacity, &ended, begun);
        }
    }

    // What is promised on the disks to the jobs let go, in all.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_promised_bytes(&self) -> u64 {
        self.storage_capacity.promises().bytes_of(Waiter::Usenet)
    }

    // The names in the destination whose copy the held move cut short.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_cut_short(&self, job_id: JobId) -> Vec<PathBuf> {
        match self.full_disk.get(&job_id).map(|hold| &hold.stage) {
            Some(Stage::Moving(_, again)) => again.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }

    // What a job held in its download still has to fetch, which its start
    // promises, and what it needs free past the reserve to start again:
    // that, or what its step needs when that is more. A step's `needs` is
    // the free figure it runs on, the reserve in it; what was left of a
    // refused file is bytes to write.
    fn held_start_need(&self, job_id: JobId, wait: &RoomWait) -> (u64, u64) {
        let remaining = self
            .released_remaining
            .get(&job_id)
            .map_or(0, |(remaining, _)| *remaining);
        let step = match wait.needs {
            Some(needs) => needs.saturating_sub(self.extraction_limits.min_free_bytes),
            None => wait.rise.unwrap_or(0),
        };
        (remaining, remaining.max(step))
    }

    // Its user resumed the held job: its start is judged by room from here,
    // at every wake, until it goes on.
    pub(in crate::pipeline) fn full_disk_resumed(&mut self, job_id: JobId) {
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            hold.resumed = true;
        }
    }

    // Its user paused the held job: a resume before ends here, and its
    // start waits again for its disk to show more than at the failure.
    pub(in crate::pipeline) fn full_disk_paused(&mut self, job_id: JobId) {
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            hold.resumed = false;
            // Paused, it waits for nothing a start need leave aside; the
            // wake once it is active again counts it anew.
            set_waiting(&self.storage_capacity, hold, None);
        }
    }

    // What the held job waits for on its root, as it stands: set as its
    // hold begins, and at each wake.
    fn note_full_disk_waiting(&mut self, job_id: JobId) {
        let Some(waiting) = self
            .full_disk
            .get(&job_id)
            .map(|hold| self.full_disk_waiting_of(job_id, hold))
        else {
            return;
        };
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            set_waiting(&self.storage_capacity, hold, waiting);
        }
    }

    // What a held job waits for past the reserve on the root it waits on,
    // which a new start there leaves aside: what a job held in its download
    // needs to start again, or what a held move needs free to go on. None
    // for one let go, paused, refused by a quota, whose free figure is not
    // its room, or waiting on a path.
    fn full_disk_waiting_of(&self, job_id: JobId, hold: &FullDisk) -> Option<(StorageRoot, u64)> {
        if hold.go || hold.wait.quota {
            return None;
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.run_state == RunState::Paused)
        {
            return None;
        }
        let (root, _) = hold.wait.root_read_since(&self.storage_capacity)?;
        let bytes = match hold.stage {
            Stage::Graph => self.held_start_need(job_id, &hold.wait).1,
            Stage::Moving(..) | Stage::Claim(_) | Stage::Restored => match hold.wait.needs {
                Some(needs) => needs.saturating_sub(self.extraction_limits.min_free_bytes),
                None => hold.wait.rise.unwrap_or(0),
            },
        };
        (bytes > 0).then_some((root, bytes))
    }

    // What a job held in its download delivers, which its start promises
    // on the complete folder's disk when that is another disk.
    fn held_start_size(&self, job_id: JobId) -> u64 {
        self.released_remaining
            .get(&job_id)
            .map_or(0, |(remaining, wanted)| (*wanted).max(*remaining))
    }

    // What a job held in its download needs past the reserve to start
    // again, and the room a fresh reading of its disk leaves now: the
    // figures its row shows. A hold a quota began, or one on a path, shows
    // none.
    pub(in crate::pipeline) fn full_disk_figures(
        &self,
        job_id: JobId,
    ) -> (Option<u64>, Option<u64>) {
        let Some(hold) = self.full_disk.get(&job_id) else {
            return (None, None);
        };
        if !matches!(hold.stage, Stage::Graph) || hold.wait.quota {
            return (None, None);
        }
        let Some((root, _)) = hold.wait.root_read_since(&self.storage_capacity) else {
            return (None, None);
        };
        let room = weaver_server_disk::operations::room_wait::StartCheck {
            root,
            waiters: false,
            ..self.working_disk_check()
        }
        .room_now(&self.storage_capacity);
        (Some(self.held_start_need(job_id, &hold.wait).1), room)
    }

    // Whether a fresh, good reading of `root` leaves `need` past the
    // reserve and every promise, and, when the complete folder is on
    // another disk, a reading of that disk leaves `size`. It books nothing.
    // It is judged without what the jobs held for a full disk wait for,
    // being one of them.
    fn held_start_room(&self, root: StorageRoot, need: u64, size: u64) -> bool {
        let check = weaver_server_disk::operations::room_wait::StartCheck {
            waiters: false,
            ..self.working_disk_check()
        };
        let room = |root, need| {
            weaver_server_disk::operations::room_wait::StartCheck { root, ..check }
                .room_now(&self.storage_capacity)
                .is_some_and(|room| room >= need)
        };
        match self.complete_on_other_disk() {
            true => room(check.root, need) && room(StorageRoot::Complete, size),
            false => room(root, need),
        }
    }

    // What a reading of the held job's disk has to leave free, past the
    // reserve and every promise, to let it start again in its download.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_start_need(&self, job_id: JobId) -> Option<u64> {
        self.full_disk
            .get(&job_id)
            .map(|hold| self.held_start_need(job_id, &hold.wait).1)
    }

    // What the held job's start again promises: what it has left to fetch.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_start_promise(&self, job_id: JobId) -> Option<u64> {
        self.full_disk
            .get(&job_id)
            .map(|hold| self.held_start_need(job_id, &hold.wait).0)
    }

    // What a reading of the held job's disk has to show more than.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_mark(&self, job_id: JobId) -> Option<u64> {
        self.full_disk.get(&job_id).and_then(|hold| hold.wait.mark)
    }

    // What a reading of the held job's disk has to show at least.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_needs(&self, job_id: JobId) -> Option<u64> {
        self.full_disk.get(&job_id).and_then(|hold| hold.wait.needs)
    }

    // What a reading has to show over the mark, by what was left of the file
    // whose write was refused, and whether a quota refused it.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_rise(&self, job_id: JobId) -> (Option<u64>, bool) {
        self.full_disk
            .get(&job_id)
            .map_or((None, false), |hold| (hold.wait.rise, hold.wait.quota))
    }

    // Whether the held job waits on a path of its own, not on a root.
    #[cfg(all(feature = "pipeline-test-fixtures", not(test)))]
    pub(in crate::pipeline) fn full_disk_on_path(&self, job_id: JobId) -> bool {
        self.full_disk
            .get(&job_id)
            .is_some_and(|hold| matches!(hold.wait.disk, Disk::Path(_)))
    }

    // The job no longer waits for room after a full disk: it went on, or
    // left the pipeline.
    pub(in crate::pipeline) fn drop_full_disk_hold(&mut self, job_id: JobId) -> Option<Stage> {
        if self.full_disk.is_empty() {
            return None;
        }
        let mut hold = self.full_disk.remove(&job_id)?;
        set_waiting(&self.storage_capacity, &mut hold, None);
        hold.wait.end(&self.storage_capacity);
        Some(hold.stage)
    }
}

// The held job waits for `waiting` on a root's ledger from here: what it
// waited for there before comes off, ringing, and the new figure goes on.
// Nothing moves while the figure stands.
fn set_waiting(
    capacity: &crate::operations::StorageCapacity,
    hold: &mut FullDisk,
    waiting: Option<(StorageRoot, u64)>,
) {
    if hold.waiting == waiting {
        return;
    }
    if let Some((root, bytes)) = hold.waiting.take() {
        capacity.ledger(root).end_waiting(bytes);
    }
    if let Some((root, bytes)) = waiting {
        capacity.ledger(root).hold_waiting(bytes);
        hold.waiting = Some((root, bytes));
    }
}
