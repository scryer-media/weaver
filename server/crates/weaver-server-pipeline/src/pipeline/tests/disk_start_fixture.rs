// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Jobs of a declared size queued on disks whose free space, size and
// failures the test sets. The sampler reads them only when the test asks,
// its clock stopped, so a reading's age is what the test makes it.

use super::*;
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use std::sync::atomic::{AtomicBool, AtomicU64};
use weaver_server_disk::operations::disk::DiskProbeError;

pub const GIB: u64 = 1 << 30;
// Long enough that no reading is taken on its own while a test runs.
pub const INTERVAL: Duration = Duration::from_secs(60 * 60);

// One filesystem as its probe reads it.
#[derive(Clone)]
pub struct Volume {
    free: Arc<AtomicU64>,
    total: Arc<AtomicU64>,
    fails: Arc<AtomicBool>,
}

impl Volume {
    fn new(free: u64, total: u64) -> Self {
        Self {
            free: Arc::new(AtomicU64::new(free)),
            total: Arc::new(AtomicU64::new(total)),
            fails: Arc::new(AtomicBool::new(false)),
        }
    }

    fn read(&self) -> Result<DiskSpace, DiskProbeError> {
        if self.fails.load(Ordering::Acquire) {
            return Err(DiskProbeError::InvalidReading);
        }
        Ok(DiskSpace {
            total_bytes: self.total.load(Ordering::Acquire),
            available_bytes: self.free.load(Ordering::Acquire),
        })
    }
}

// The disks under a pipeline's roots: one filesystem, or the complete
// folder on a filesystem of its own.
pub struct StartDisk {
    working: Volume,
    complete: Volume,
    gate: crate::runtime::StartupDiskGate,
}

impl StartDisk {
    // Every root on one filesystem of `total` bytes with `free` free, the
    // jobs held to keep `reserve` free.
    pub fn under(pipeline: &mut Pipeline, reserve: u64, free: u64, total: u64) -> Self {
        let volume = Volume::new(free, total);
        Self::install(
            pipeline,
            reserve,
            (volume.clone(), volume),
            false,
            crate::runtime::StartupDiskGate::opened(),
        )
    }

    // The complete folder on a filesystem of its own, with `complete` free.
    pub fn split(
        pipeline: &mut Pipeline,
        reserve: u64,
        (working, complete): (u64, u64),
        total: u64,
    ) -> Self {
        Self::install(
            pipeline,
            reserve,
            (Volume::new(working, total), Volume::new(complete, total)),
            true,
            crate::runtime::StartupDiskGate::opened(),
        )
    }

    // As `under`, its sampler held by a start-up gate not yet open: no
    // reading comes until the test opens it.
    pub fn gated(pipeline: &mut Pipeline, reserve: u64, free: u64, total: u64) -> Self {
        let volume = Volume::new(free, total);
        Self::install(
            pipeline,
            reserve,
            (volume.clone(), volume),
            false,
            crate::runtime::StartupDiskGate::closed(),
        )
    }

    fn install(
        pipeline: &mut Pipeline,
        reserve: u64,
        (working, complete): (Volume, Volume),
        split: bool,
        gate: crate::runtime::StartupDiskGate,
    ) -> Self {
        let roots = [
            pipeline.script_data_dir.clone(),
            pipeline.intermediate_dir.clone(),
            pipeline.complete_dir.clone(),
        ];
        if split {
            use weaver_server_disk::runtime::fs::test_devices::assign;
            assign(&roots[0], 1);
            assign(&roots[1], 1);
            assign(&roots[2], 2);
        }
        let probe = {
            let complete_dir = pipeline.complete_dir.clone();
            let (working, complete) = (working.clone(), complete.clone());
            Arc::new(move |path: &std::path::Path| {
                if path.starts_with(&complete_dir) {
                    complete.read()
                } else {
                    working.read()
                }
            })
        };
        let capacity = StorageCapacity::with_probe_after(roots, INTERVAL, probe, gate.clone());
        capacity.stop_clock_for_test();
        if gate.is_open() {
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
        }
        pipeline.storage_capacity = Arc::new(capacity);
        Arc::make_mut(&mut pipeline.extraction_limits).min_free_bytes = reserve;
        Self {
            working,
            complete,
            gate,
        }
    }

    // Every root is read again, and the pipeline hears it as its loop
    // does. The test waits for the readings themselves.
    pub fn read_again(&self, pipeline: &mut Pipeline) {
        pipeline.storage_capacity.reread_roots();
        pipeline.disk_reading();
    }

    // The working disk reads `free` free, and that is heard.
    pub fn reads(&self, pipeline: &mut Pipeline, free: u64) {
        self.working.fails.store(false, Ordering::Release);
        self.working.free.store(free, Ordering::Release);
        self.read_again(pipeline);
    }

    // The complete folder's disk, when it is its own, reads `free` free.
    pub fn complete_reads(&self, pipeline: &mut Pipeline, free: u64) {
        self.complete.fails.store(false, Ordering::Release);
        self.complete.free.store(free, Ordering::Release);
        self.read_again(pipeline);
    }

    // The working disk's next probe fails: the last good reading is held,
    // stale.
    pub fn fails(&self, pipeline: &mut Pipeline) {
        self.working.fails.store(true, Ordering::Release);
        self.complete.fails.store(true, Ordering::Release);
        self.read_again(pipeline);
    }

    // Every reading grows older by `by`.
    pub fn ages(&self, pipeline: &Pipeline, by: Duration) {
        pipeline.storage_capacity.advance_clock(by);
    }

    // The start-up gate opens and the first readings come and are heard.
    pub fn open(&self, pipeline: &mut Pipeline) {
        self.gate.open();
        for root in StorageRoot::ALL {
            pipeline.storage_capacity.wait_for_probes(root, 1);
        }
        pipeline.disk_reading();
    }
}

// A job named for its id whose one file declares `gib` GiB still to fetch.
pub fn sized_spec(id: u64, gib: u64) -> JobSpec {
    // Articles of 64 MiB, sixteen to the GiB.
    let segments = vec![(GIB / 16) as u32; 16 * gib as usize];
    segmented_job_spec(&format!("Sized NZB {id}"), "payload.bin", &segments)
}

// The job is added, and the turn after it acts on it.
pub async fn add_sized(pipeline: &mut Pipeline, id: u64, gib: u64) {
    let spec = sized_spec(id, gib);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("sized-{id}.nzb")),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

// The jobs that hold a slot, in id order.
pub fn started(pipeline: &Pipeline) -> Vec<u64> {
    let mut ids: Vec<u64> = pipeline.nzb_admitted.iter().map(|id| id.0).collect();
    ids.sort_unstable();
    ids
}

// Everything promised on the disk the root lies on.
pub fn promised_on(pipeline: &Pipeline, root: StorageRoot) -> u64 {
    pipeline.storage_capacity.ledger(root).promised_total()
}

// What the root's own ledger holds promised.
pub fn promised_here(pipeline: &Pipeline, root: StorageRoot) -> u64 {
    pipeline.storage_capacity.ledger(root).promised()
}

// What the job's queue row shows: why it waits, and the figures with it.
pub fn shown(pipeline: &Pipeline, id: u64) -> (Option<String>, Option<u64>, Option<u64>) {
    pipeline.shared_state.queue.read(|inner| {
        let info = inner.job(JobId(id)).unwrap().info(&inner.globals);
        (
            info.download_wait_reason,
            info.disk_need_bytes,
            info.disk_free_bytes,
        )
    })
}

// What the job's row says it still has to fetch, if it has a row.
pub fn remaining(pipeline: &Pipeline, id: u64) -> Option<u64> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(JobId(id))
            .map(|row| row.report.job.remaining_bytes)
    })
}

// Whether the job has a graph in place.
pub fn has_graph(pipeline: &Pipeline, id: u64) -> bool {
    pipeline.graph_jobs.contains_key(&JobId(id))
}

// Whether jobs are kept from starting for the disk, and that hold waits on
// it for a reading.
pub fn kept_for_disk(pipeline: &Pipeline) -> bool {
    pipeline.admission_disk.held()
}

// `bytes` of what a started job was promised land on the working disk, as
// its graph settles them a slab at a time.
pub fn lands(pipeline: &Pipeline, bytes: u64) {
    pipeline
        .storage_capacity
        .ledger(StorageRoot::Intermediate)
        .landed(bytes, 0);
}

// How many times anything asked a sampler for a reading out of turn.
pub fn refresh_asks() -> u64 {
    weaver_server_disk::operations::disk::REFRESH_ASKS.load(Ordering::Relaxed)
}

// How many times anything took a ledger's booking lock in this process.
pub fn booking_locks() -> u64 {
    weaver_server_disk::operations::disk::BOOKING_LOCKS.load(Ordering::Relaxed)
}

// How many walks over the queue's rows ran on this thread.
pub fn walks() -> u64 {
    crate::pipeline::queue::ROW_WALKS.load(Ordering::Relaxed)
}

// The job's row says it has `left` still to fetch, as a job partly fetched
// before a pause does.
pub fn says_left(pipeline: &Pipeline, id: u64, left: u64) {
    pipeline.shared_state.queue.write(|inner| {
        if let Some(mut row) = inner.job_mut(JobId(id)) {
            row.report.job.remaining_bytes = left;
        }
        crate::pipeline::queue::Change::changed(())
    });
}

// The id of the torrent the tests let go.
const TORRENT: u64 = 77_001;

// A torrent held for a full disk on the working root is let go on its
// figure, `bytes` promised to its step, as the torrent service's wake does.
pub fn torrent_let_go_on_working(pipeline: &Pipeline, bytes: u64) {
    use weaver_server_disk::operations::room_wait::{Disk, Promised, Waiter};
    let disk = Disk::Root(StorageRoot::Intermediate);
    let device = disk.device(&pipeline.storage_capacity);
    pipeline.storage_capacity.promises().insert(
        Waiter::Torrent,
        JobId(TORRENT),
        Promised {
            disk,
            bytes,
            device,
        },
    );
}

// The step of the torrent let go by [`torrent_let_go_on_working`] ends: its
// promise goes, as the torrent service ends it.
pub fn torrent_step_ends(pipeline: &Pipeline) {
    use weaver_server_disk::operations::room_wait::Waiter;
    pipeline.storage_capacity.promises().end(
        &pipeline.storage_capacity,
        Waiter::Torrent,
        JobId(TORRENT),
    );
}
