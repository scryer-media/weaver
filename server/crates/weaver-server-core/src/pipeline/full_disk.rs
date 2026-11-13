// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job whose write found the disk full waits for room instead of failing.
//
// A step that could not write left the job's graph saying more than its
// files hold, so the graph goes at once, with no checkpoint taken of it. The
// job keeps its slot and fetches nothing more; once nothing of it is out it
// parks as a paused job does, and waits, shown waiting for disk space, on
// the same ledger, readings and wake as a job the reserve holds. A reading
// taken since the write failed that shows room over the reserve lets it go:
// the scheduler reads its articles back, its graph is restored from its last
// checkpoint, as after a restart, and what that checkpoint does not hold is
// written or fetched again.
//
// Nothing here runs unless a write failed for want of room: every check on
// a path that runs often is one test of an empty map.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tracing::{info, warn};

use super::graph::limits::Device;
use super::{ParkedBytes, Pipeline};
use crate::jobs::ids::JobId;
use crate::jobs::model::JobArticles;
use crate::operations::StorageRoot;

// What a job waits for: room on the disk its write failed on, read after
// that write.
pub(crate) struct FullDisk {
    root: StorageRoot,
    // When the reading cached as the write failed was taken: it may predate
    // what filled the disk, so it says nothing of room.
    reading: Option<Instant>,
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
        if let Some(graph) = self.graph_jobs.get(&job_id) {
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
        let reading = self
            .storage_capacity
            .current(root)
            .reading()
            .map(|reading| reading.sampled_at);
        self.storage_capacity.ledger(root).wait();
        self.full_disk.insert(
            job_id,
            FullDisk {
                root,
                reading,
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

    // A reading came, or a held job parked: each parked job held for a full
    // disk whose disk now reads room over the reserve, in a good reading
    // taken since its write failed, goes on. The scheduler reads its
    // articles back from there.
    pub(in crate::pipeline) fn wake_full_disk(&mut self) {
        if self.full_disk.is_empty() {
            return;
        }
        let reserve = self.extraction_limits.min_free_bytes;
        let woken: Vec<JobId> = self
            .full_disk
            .iter()
            .filter(|(job_id, hold)| {
                let parked = !self.parking_jobs.contains(job_id)
                    && self
                        .jobs
                        .get(job_id)
                        .is_none_or(|state| !matches!(state.articles, JobArticles::Active(_)));
                parked && self.room_since(hold, reserve)
            })
            .map(|(job_id, _)| *job_id)
            .collect();
        for job_id in woken {
            info!(
                job_id = job_id.0,
                "the disk has room again; the job runs on from its last checkpoint"
            );
            self.drop_full_disk_hold(job_id);
            self.download_dispatch_wake = true;
            self.report_job(job_id);
        }
    }

    // Whether the disk has room over the reserve in a good reading taken
    // since the write failed.
    fn room_since(&self, hold: &FullDisk, reserve: u64) -> bool {
        let Some(reading) = self.storage_capacity.current(hold.root).reading() else {
            return false;
        };
        if reading.stale || Some(reading.sampled_at) == hold.reading {
            return false;
        }
        self.storage_capacity
            .free(hold.root)
            .is_some_and(|free| !free.stale && free.bytes > reserve)
    }

    // The job no longer waits for room after a full disk: it went on, or
    // left the pipeline.
    pub(in crate::pipeline) fn drop_full_disk_hold(&mut self, job_id: JobId) {
        if self.full_disk.is_empty() {
            return;
        }
        if let Some(hold) = self.full_disk.remove(&job_id) {
            self.storage_capacity.ledger(hold.root).unwait();
        }
    }
}
