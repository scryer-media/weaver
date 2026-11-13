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

use std::path::Path;
use std::time::Instant;

use tracing::info;

use super::Pipeline;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::model::JobArticles;
use crate::operations::StorageRoot;

// What a job waits for: room on the disk its write failed on, read after
// that write.
pub(crate) struct FullDisk {
    root: StorageRoot,
    // When the reading cached as the write failed was taken: it may predate
    // what filled the disk, so it says nothing of room.
    reading: Option<Instant>,
}

impl Pipeline {
    // The disk a file lies on: the complete folder's, or the working one's.
    pub(in crate::pipeline) fn full_disk_root(&self, path: &Path) -> StorageRoot {
        if path.starts_with(&self.complete_dir) {
            StorageRoot::Complete
        } else {
            StorageRoot::Intermediate
        }
    }

    // Whether the job waits for room after a write found the disk full.
    pub(in crate::pipeline) fn held_for_full_disk(&self, job_id: JobId) -> bool {
        !self.full_disk.is_empty() && self.full_disk.contains_key(&job_id)
    }

    // A write of the job found `root` full: the job waits for room there.
    // Its graph goes now, so nothing more is written from what it says; the
    // figures it showed are kept. Waiting on the ledger asks for a reading.
    // The job parks once nothing of it is out, on a later turn.
    pub(in crate::pipeline) fn hold_job_for_full_disk(&mut self, job_id: JobId, root: StorageRoot) {
        if self.full_disk.contains_key(&job_id) || !self.jobs.contains_key(&job_id) {
            return;
        }
        if let (Some(graph), Some(state)) = (self.graph_jobs.get(&job_id), self.jobs.get(&job_id)) {
            let figures = Self::graph_remaining(graph, &state.spec);
            self.released_remaining.insert(job_id, figures);
        }
        self.retire_graph_job(job_id);
        let reading = self
            .storage_capacity
            .current(root)
            .reading()
            .map(|reading| reading.sampled_at);
        self.storage_capacity.ledger(root).wait();
        self.full_disk.insert(job_id, FullDisk { root, reading });
        self.parking_jobs.insert(job_id);
        self.download_dispatch_wake = true;
        self.report_job(job_id);
    }

    // The job held for a full disk lets go of its uuencode cursors as it
    // parks: they stand where its graph stood before it went, and its next
    // graph, restored from the checkpoint, may stand earlier. A part held in
    // memory is fetched again; one spooled stays on disk, for the checkpoint
    // that names it.
    pub(in crate::pipeline) fn forget_uu_cursors(&mut self, job_id: JobId) {
        let files: Vec<NzbFileId> = self
            .uu_files
            .keys()
            .filter(|file_id| file_id.job_id == job_id)
            .copied()
            .collect();
        for file_id in files {
            let Some(uu) = self.uu_files.remove(&file_id) else {
                continue;
            };
            for part in uu.parked.into_values() {
                self.forget_parked_bytes(part.bytes);
                self.release_uu_parked_segment();
            }
        }
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
