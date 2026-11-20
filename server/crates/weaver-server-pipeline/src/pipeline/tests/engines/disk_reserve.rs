// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::actor_drive::drive;
use super::disk_archive::{PLENTY, on_disk_posting};
use super::harness::{ARTICLE, JOB, Post, Scenario, payload, serve_job};
use super::stream_fixtures::xz;
use crate::operations::StorageRoot;
use crate::{
    JobId,
    jobs::{model::JobStatus, model::RunState},
    pipeline::Pipeline,
};
use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use weaver_server_disk::operations::disk::DiskLedger;
// A stream the job decodes into a file, on disks that read `room` bytes
// over `RESERVE` at every reading, the job held to keep `RESERVE` free.
pub fn on_disk(room: &Arc<AtomicU64>) -> Scenario {
    on_disk_posting(
        room,
        vec![Post::plain(
            "ember.bin.xz",
            xz(&payload(3 * ARTICLE + 7, 91)),
        )],
    )
}

// The ledger of the disk the job's working folder is on.
pub fn working(pipeline: &Pipeline) -> Arc<DiskLedger> {
    Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Intermediate)
            .ledger(),
    )
}

// Serve the job and hear its runs until it waits for room: its run decodes
// into a file, which the disk has no room for.
pub async fn until_waiting(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, |pipeline| {
        pipeline.graph_disk_held(JOB)
    })
    .await;
    assert!(!pipeline.waits_for_disk_room(), "the loop has yet to look");
    pipeline.reconcile_disk_waits();
    assert!(pipeline.waits_for_disk_room(), "the job waits for room");
    assert!(working(pipeline).waited_on());
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
}

// Serve one article the job asks for: from here it has downloaded.
pub async fn serve_one(pipeline: &mut Pipeline, scenario: &Scenario) {
    let work = pipeline
        .jobs
        .get_mut(&JOB)
        .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        .expect("the job asks for an article");
    let article = (
        work.segment_id.file_id.file_index,
        work.segment_id.segment_number,
    );
    serve_job(pipeline, JOB, scenario, article).await;
    assert!(pipeline.jobs[&JOB].downloaded_bytes > 0, "the job started");
}

// The wait reason the job's queue row says.
pub fn row_reason(pipeline: &Pipeline, job: JobId) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == job)
            .and_then(|row| row.info(&inner.globals).download_wait_reason)
    })
}

// The disk has room: a reading is asked for and heard, and every job that
// waited goes on.
pub async fn room_freed(pipeline: &mut Pipeline, room: &AtomicU64) {
    room.store(PLENTY, Ordering::Release);
    pipeline
        .storage_capacity
        .request_refresh(StorageRoot::Intermediate);
    while pipeline.waits_for_disk_room() {
        pipeline.storage_capacity.readings().notified().await;
        pipeline.disk_reading();
    }
}

pub fn assert_released(pipeline: &Pipeline, ledger: &DiskLedger) {
    assert!(!pipeline.waits_for_disk_room());
    assert!(!ledger.waited_on(), "no one waits on the disk");
    assert_eq!(ledger.promised(), 0, "nothing is promised on the disk");
}

// What a job did as it was driven until it ended or waited for room with
// nothing of it left running: how many of its runs returned, whether it was
// ever held, and how many times it began to be held.
#[derive(Debug, Default)]
pub struct Settled {
    pub ended: bool,
    pub runs: usize,
    pub held: bool,
    pub holds: usize,
}

// Serve the job, hear its runs and reconcile its waits as the loop does,
// with no new reading of the disk, until it ends or waits for room with no
// run of it live.
pub async fn settle(pipeline: &mut Pipeline, scenario: &Scenario, settled: &mut Settled) {
    let mut was_held = pipeline.graph_disk_held(JOB);
    let mut look = |pipeline: &Pipeline, settled: &mut Settled| {
        let held = pipeline.graph_disk_held(JOB);
        settled.held |= held;
        if held && !was_held {
            settled.holds += 1;
        }
        was_held = held;
    };
    loop {
        drive(pipeline, scenario, JOB, |pipeline| {
            pipeline.graph_disk_held(JOB)
                || pipeline
                    .graph_jobs
                    .get(&JOB)
                    .is_some_and(|job| job.awaits_mail())
        })
        .await;
        look(pipeline, settled);
        if pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            if matches!(said.heard, crate::pipeline::graph::engine::Heard::Ended) {
                settled.runs += 1;
            }
            pipeline.graph_run_said(said);
            look(pipeline, settled);
            continue;
        }
        if !pipeline.graph_disk_held(JOB) {
            settled.ended = true;
            return;
        }
        pipeline.reconcile_disk_waits();
        look(pipeline, settled);
        if pipeline.graph_disk_held(JOB) {
            return;
        }
    }
}

// Every file under the job's complete directory, by its path there.
pub fn delivered(complete_dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    super::harness::collect(complete_dir, complete_dir, &mut files);
    files
}

pub fn file_named<'a>(
    files: &'a std::collections::BTreeMap<String, Vec<u8>>,
    name: &str,
) -> &'a [u8] {
    files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(name))
        .map(|(_, bytes)| bytes.as_slice())
        .unwrap_or_else(|| panic!("{name} is delivered: {:?}", files.keys()))
}

// An xz stream holding a tar whose first member is a tar of its own, then a
// member that spans several of the run's writes; and the length of the tar
// the run decodes, with the two files.
pub fn nested() -> (Vec<u8>, u64, Vec<u8>, Vec<u8>) {
    let a = vec![3u8; 1000];
    let b = vec![5u8; 200_000];
    let inner = super::stream_fixtures::tar(&[("a.bin", a.clone())]);
    let outer = super::stream_fixtures::tar(&[("inner.tar", inner), ("b.bin", b.clone())]);
    (xz(&outer), outer.len() as u64, a, b)
}

pub fn status(pipeline: &Pipeline) -> Option<JobStatus> {
    crate::pipeline::tests::job_status_for_assert(pipeline, JOB)
}
pub fn eligible(pipeline: &Pipeline) -> Vec<JobId> {
    pipeline.download_scheduler_eligible_jobs()
}
pub fn admitted(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.nzb_admitted.contains(&id)
}
pub fn dispatch_reason(pipeline: &Pipeline) -> Option<std::borrow::Cow<'static, str>> {
    pipeline.dispatch_wait_reason(JOB, pipeline.login_refusal_view())
}
pub fn has_graph(pipeline: &Pipeline) -> bool {
    pipeline.graph_jobs.contains_key(&JOB)
}
pub fn run_state(pipeline: &Pipeline) -> RunState {
    pipeline.jobs[&JOB].run_state
}
pub async fn restart_disk(
    root: &tempfile::TempDir,
    pipeline: Pipeline,
    scenario: &Scenario,
) -> Pipeline {
    super::restart_fixtures::restart(
        root,
        pipeline,
        scenario,
        &mut std::collections::BTreeSet::new(),
        &mut |_| {},
    )
    .await
}
pub async fn drain(pipeline: &mut Pipeline) {
    pipeline.drain().await
}
pub fn complete_ledger(pipeline: &Pipeline) -> Arc<DiskLedger> {
    Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    )
}
pub fn download_queue_len(pipeline: &Pipeline) -> usize {
    pipeline.jobs[&JOB]
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}
pub fn dispatch(pipeline: &mut Pipeline) {
    pipeline.dispatch_downloads();
}
pub async fn add_waiting_job(
    pipeline: &mut Pipeline,
    id: JobId,
    spec: crate::JobSpec,
    nzb: Vec<u8>,
) {
    pipeline
        .add_job(
            id,
            spec,
            std::path::PathBuf::from("cinder.nzb"),
            nzb,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Every disk the job reads at `RESERVE` and `room` bytes over it on its
// first reading, and `then` bytes over it on every reading after, the job
// held to keep `RESERVE` free. A reading after the first is one a job
// waiting for room asked for; `readings` counts them all.
pub fn disk_with_room(
    posts: Vec<Post>,
    room: u64,
    then: u64,
    readings: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> Scenario {
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};

    const RESERVE: u64 = 1 << 30;
    const NEVER: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);
    Scenario::new(posts)
        .limits(super::actor_drive::limits(|limits| {
            limits.min_free_bytes = RESERVE
        }))
        .setup(move |pipeline| {
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let first = roots.len();
            let readings = Arc::clone(&readings);
            let probe = Arc::new(move |_: &Path| {
                let over = if readings.fetch_add(1, Ordering::AcqRel) < first {
                    room
                } else {
                    then
                };
                Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: RESERVE + over,
                })
            });
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
}
