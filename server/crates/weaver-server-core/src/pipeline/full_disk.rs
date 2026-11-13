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
use std::collections::btree_map::Entry;
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use tracing::{info, warn};

use super::graph::limits::Device;
use super::{ParkedBytes, Pipeline};
use crate::jobs::ids::JobId;
use crate::jobs::model::{FileSummaries, FileSummary, JobArticles, RunState};
use crate::operations::StorageRoot;

// What a job waits for: room made on the disk its write failed on.
pub(crate) struct FullDisk {
    root: StorageRoot,
    // The disk's count of the probes it had begun as the write failed: only
    // a later probe's reading can show room made since.
    since: u64,
    // The last of the disk's probes this wait has counted: at first `since`,
    // so a probe already running as the write failed is never counted.
    counted: u64,
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
            if matches!(state.articles, JobArticles::Active(_)) {
                left = Some(FileSummaries(
                    state
                        .spec
                        .files
                        .iter()
                        .zip(0u32..)
                        .map(|(file, index)| {
                            FileSummary::left(file, |ordinal| {
                                graph.article_settled(weaver_graph::VolumeIndex(index), ordinal)
                            })
                        })
                        .collect(),
                ));
            }
            let figures = Self::graph_remaining(graph, &state.spec);
            self.released_remaining.insert(job_id, figures);
            named = graph
                .pending()
                .held_spools()
                .into_iter()
                .map(|spool| state.working_dir.join(spool))
                .collect();
        }
        self.retire_graph_job(job_id);
        // What the disk showed free before the write failed is what a later
        // reading has to beat. It never lowers what an earlier wait left.
        let (since, shown) = self.storage_capacity.failed_at(root);
        if let Some(shown) = shown {
            let high = self.full_disk_highs.entry(job_id).or_insert(shown);
            *high = (*high).max(shown);
        }
        self.storage_capacity.ledger(root).wait();
        self.full_disk.insert(
            job_id,
            FullDisk {
                root,
                since,
                counted: since,
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
    // heard. Each held job has a high: the most its disk has shown free, in
    // the reading published as its write failed, in one a probe running
    // then published after, and in every reading since. It counts its
    // disk's reading when it is one it has not counted and its probe began
    // after the write failed: a good one that shows more than the high, and
    // more than the reserve, shows room was made. With no high yet, nothing
    // was shown before to compare with, and the reading only sets it. The
    // high never falls while the job saves nothing new. A job with room made,
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
            // A probe running as the write failed began before it: what it
            // published can raise the high, never let the job go.
            if let Some(before) = self.storage_capacity.straddled(hold.root, hold.since) {
                let high = self.full_disk_highs.entry(*job_id).or_insert(before);
                *high = (*high).max(before);
            }
            if let Some((probe, reading)) = self
                .storage_capacity
                .reading_begun_after(hold.root, hold.counted)
            {
                hold.counted = probe;
                let free = self
                    .storage_capacity
                    .free(hold.root)
                    .filter(|free| !free.stale && !reading.stale);
                if let Some(free) = free {
                    match self.full_disk_highs.entry(*job_id) {
                        Entry::Vacant(high) => {
                            high.insert(reading.available_bytes);
                        }
                        Entry::Occupied(mut high) => {
                            if reading.available_bytes > *high.get() {
                                high.insert(reading.available_bytes);
                                hold.go |= free.bytes > reserve;
                            }
                        }
                    }
                }
            }
            let parked = !self.parking_jobs.contains(job_id)
                && !self.retiring_graph_jobs.contains_key(job_id)
                && self.jobs.get(job_id).is_none_or(|state| {
                    !matches!(state.articles, JobArticles::Active(_))
                        && state.run_state != RunState::Paused
                });
            if hold.go && parked {
                woken.push(*job_id);
            }
        }
        for job_id in woken {
            info!(
                job_id = job_id.0,
                "the job held for a full disk runs on from its last checkpoint"
            );
            self.drop_full_disk_hold(job_id);
            self.request_job_activation(job_id);
            self.download_dispatch_wake = true;
            self.report_job(job_id);
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
    // the readings say. A write that fails again holds it again, against the
    // same high.
    pub(in crate::pipeline) fn try_full_disk_now(&mut self, job_id: JobId) {
        if self.full_disk.is_empty() {
            return;
        }
        if let Some(hold) = self.full_disk.get_mut(&job_id) {
            hold.go = true;
        }
    }

    // The job saved something new, or left: what its disk showed free
    // before says nothing of its next wait.
    pub(in crate::pipeline) fn full_disk_progress(&mut self, job_id: JobId) {
        if !self.full_disk_highs.is_empty() {
            self.full_disk_highs.remove(&job_id);
        }
    }

    // The job no longer waits for room after a full disk: it went on, or
    // left the pipeline.
    pub(in crate::pipeline) fn drop_full_disk_hold(&mut self, job_id: JobId) {
        if self.full_disk.is_empty() {
            return;
        }
        if let Some(hold) = self.full_disk.remove(&job_id) {
            self.storage_capacity.ledger(hold.root).unwait();
            self.storage_capacity
                .failure_forgotten(hold.root, hold.since);
        }
    }
}
