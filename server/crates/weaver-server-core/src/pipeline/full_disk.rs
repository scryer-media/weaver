// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job whose write found the disk full waits for room instead of failing.
//
// A step that could not write left the job's graph saying more than its
// files hold, so the graph goes at once, with no checkpoint taken of it. The
// job keeps its slot and fetches nothing more; once nothing of it is out it
// parks as a paused job does, and waits, shown waiting for disk space, on
// the same ledger and readings as a job the reserve holds. Only a reading
// whose probe began after the write failed counts. One that shows more than
// the disk showed before the write failed, and more than the reserve, shows
// room was made, and lets the job go, as a resume of the job by its user
// does at once. Its
// articles are read back, its graph is restored from its last checkpoint, as
// after a restart, and what that checkpoint does not hold is written or
// fetched again. A job let go that fails again before it saved anything new
// waits for more room than it has yet seen, so a disk whose readings never
// change cannot send it round. A disk with no good reading keeps it waiting.
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

// What a job waits for: room made on the disk its write failed on.
// The disk a held job waits on: a root's, or the disk of a path that lies
// on none of theirs.
enum Disk {
    Root(StorageRoot),
    Path(PathBuf),
}

// What the job was doing when its write failed, which is what it goes back
// to when let go, the way a restart takes a job stored in that stage.
pub(in crate::pipeline) enum Stage {
    // Fetching, verifying, repairing or unpacking: its graph is restored
    // from its last checkpoint.
    Graph,
    // Its final move, into this destination: the move goes on from what the
    // failed attempt left.
    Moving(PathBuf),
}

pub(crate) struct FullDisk {
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
    pub(in crate::pipeline) fn hold_job_for_full_disk(&mut self, job_id: JobId, root: StorageRoot) {
        if self.full_disk.contains_key(&job_id) {
            return;
        }
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
        self.full_disk.insert(
            job_id,
            FullDisk {
                disk: Disk::Root(root),
                stage: Stage::Graph,
                since: look.begun,
                mark: look.shown,
                running: look.probe < look.begun,
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
        for bytes in parts {
            if let ParkedBytes::Spilled { path, .. } = &bytes
                && path.parent() == spool.as_deref()
                && self
                    .full_disk
                    .get(&job_id)
                    .is_none_or(|hold| !hold.named.contains(path))
            {
                stray.push(path.clone());
            }
            self.forget_parked_bytes(bytes);
        }
        if stray.is_empty() {
            return;
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
                    match hold.mark {
                        None => hold.mark = Some(shown),
                        Some(mark) => {
                            hold.go |= shown > mark && spendable.is_some_and(|free| free > reserve);
                        }
                    }
                }
            }
            let paused = self
                .jobs
                .get(job_id)
                .is_some_and(|state| state.run_state == RunState::Paused);
            let parked = !paused
                && match hold.stage {
                    Stage::Graph => {
                        !self.parking_jobs.contains(job_id)
                            && !self.retiring_graph_jobs.contains_key(job_id)
                            && self.jobs.get(job_id).is_none_or(|state| {
                                !matches!(state.articles, JobArticles::Active(_))
                            })
                    }
                    Stage::Moving(_) => true,
                };
            if hold.go && parked {
                woken.push(*job_id);
            }
        }
        for job_id in woken {
            match self.drop_full_disk_hold(job_id) {
                Some(Stage::Moving(dest)) => {
                    info!(
                        job_id = job_id.0,
                        "the job held for a full disk goes on with its final move"
                    );
                    self.full_disk_moves.push((job_id, dest));
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
    // root's when the destination lies on one, by the devices the capacity
    // cache already holds, and else the destination's own, read while a job
    // waits on it.
    pub(in crate::pipeline) async fn hold_move_for_full_disk(
        &mut self,
        job_id: JobId,
        dest: PathBuf,
        path: &std::path::Path,
        message: &str,
    ) {
        if !self.jobs.contains_key(&job_id) || self.full_disk.contains_key(&job_id) {
            return;
        }
        warn!(
            job_id = job_id.0,
            path = %path.display(),
            error = message,
            "the final move found the disk full; the job waits for room"
        );
        let mut disk = None;
        for (root, dir) in [
            (StorageRoot::Complete, self.complete_dir.clone()),
            (StorageRoot::Intermediate, self.intermediate_dir.clone()),
        ] {
            if self.storage_capacity.same_filesystem(path, &dir).await {
                disk = Some(Disk::Root(root));
                break;
            }
        }
        let disk = disk.unwrap_or_else(|| Disk::Path(path.to_path_buf()));
        let (look, dir) = match &disk {
            Disk::Root(root) => {
                let look = self.storage_capacity.look(*root);
                self.storage_capacity.ledger(*root).wait();
                (look, self.root_dir(*root))
            }
            Disk::Path(path) => (self.storage_capacity.hold_path(path), path.clone()),
        };
        self.full_disk.insert(
            job_id,
            FullDisk {
                disk,
                stage: Stage::Moving(dest),
                since: look.begun,
                mark: look.shown,
                running: look.probe < look.begun,
                moved: look.moved,
                dir,
                go: false,
                left: None,
                named: HashSet::new(),
            },
        );
        self.report_job(job_id);
    }

    // The final moves of the jobs let go start here, at the top of a turn,
    // the way a restart starts a move it found interrupted: the job's row
    // names the destination, and the move goes on into it.
    pub(in crate::pipeline) async fn resume_full_disk_moves(&mut self) {
        if self.full_disk_moves.is_empty() {
            return;
        }
        for (job_id, dest) in std::mem::take(&mut self.full_disk_moves) {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                continue;
            };
            state.working_dir = dest;
            self.resume_restored_move(job_id).await;
        }
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
            hold.go = true;
        }
    }

    // What a reading of the held job's disk has to show more than.
    #[cfg(test)]
    pub(in crate::pipeline) fn full_disk_mark(&self, job_id: JobId) -> Option<u64> {
        self.full_disk.get(&job_id).and_then(|hold| hold.mark)
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
