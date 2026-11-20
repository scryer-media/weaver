// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::actor_drive::drive;
use super::disk_archive::{PLENTY, RESERVE, SECOND, hears, on_disk_posting};
use super::harness::{ARTICLE, JOB, Post, Scenario, payload, serve_job};
use super::stream_fixtures::xz;
use crate::jobs::model::JobArticles;
use crate::operations::StorageRoot;
use crate::pipeline::graph::engine::{Hears, Outside};
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

// A reading of the working disk showing `value` bytes over the reserve is
// heard: the loop waits for that reading itself, not for an older one.
pub async fn reading_of(pipeline: &mut Pipeline, room: &AtomicU64, value: u64) {
    room.store(value, Ordering::Release);
    pipeline
        .storage_capacity
        .request_refresh(StorageRoot::Intermediate);
    pipeline
        .storage_capacity
        .wait_for_reading(StorageRoot::Intermediate, |reading| {
            reading.available_bytes == RESERVE + value
        });
    pipeline.disk_reading();
}

// Serve the job until a write finds the disk full, and let it park, as the
// loop's next turn does once nothing of it is out: it is held for room, not
// failed, waits on the working disk's ledger, says it waits for disk space,
// and fetches nothing.
pub async fn until_full(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert!(
        pipeline.held_for_full_disk(JOB),
        "a write found the disk full"
    );
    assert!(
        !pipeline.graph_jobs.contains_key(&JOB),
        "the graph that could not write went"
    );
    pipeline.park_drained_jobs();
    assert!(
        matches!(pipeline.jobs[&JOB].articles, JobArticles::Queued(_)),
        "it parked"
    );
    old_graph_heard(pipeline).await;
    assert_full_disk_held(pipeline);
}

// What the job's old graph still had out on a thread is heard, as the loop
// hears it.
pub async fn old_graph_heard(pipeline: &mut Pipeline) {
    while pipeline.retiring_graph_jobs.contains_key(&JOB) {
        let said = pipeline
            .graph_runs
            .recv()
            .await
            .expect("the retiring graph's runs answer");
        pipeline.graph_run_said(said);
    }
}

// The job let go reads its articles back and restores its graph: its
// release asked for that itself, with no lane asking for work.
pub async fn restarts(pipeline: &mut Pipeline) {
    assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
    assert!(
        matches!(pipeline.jobs[&JOB].articles, JobArticles::Activating(..)),
        "its release asked for its articles"
    );
    let done = pipeline
        .job_activated_rx
        .recv()
        .await
        .expect("the activation channel stays open");
    pipeline.handle_job_activated(done);
}

pub fn assert_full_disk_held(pipeline: &Pipeline) {
    assert!(pipeline.held_for_full_disk(JOB));
    assert!(
        !matches!(status(pipeline), Some(JobStatus::Failed { .. })),
        "the job is not failed"
    );
    assert!(pipeline.nzb_admitted.contains(&JOB), "it keeps its slot");
    assert!(pipeline.waits_for_disk_room());
    assert!(
        working(pipeline).waited_on(),
        "the wait hears every reading"
    );
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    assert!(
        !pipeline.download_scheduler_eligible_jobs().contains(&JOB),
        "it fetches nothing"
    );
}

// A job held for a full disk: a reading with no room keeps it held; once
// the disk has room, a reading lets it go, it reads its articles back,
// restored from its last checkpoint, and completes.
pub async fn full_disk_freed(pipeline: &mut Pipeline, scenario: &Scenario, room: &AtomicU64) {
    hears(pipeline, room, StorageRoot::Intermediate, 0);
    assert_full_disk_held(pipeline);

    fill_after(None);
    hears(pipeline, room, StorageRoot::Intermediate, 2 * PLENTY);
    // A job whose old graph still has something out goes on once that is
    // heard, not before.
    old_graph_heard(pipeline).await;
    assert!(!pipeline.held_for_full_disk(JOB));
    assert!(!pipeline.waits_for_disk_room());
    assert!(!working(pipeline).waited_on(), "no one waits on the disk");
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        None
    );

    restarts(pipeline).await;
    drive(pipeline, scenario, JOB, |_| false).await;
    assert_eq!(status(pipeline), Some(JobStatus::Complete));
    assert_released(pipeline, &working(pipeline));
}

pub fn fill_after(room: Option<u64>) {
    crate::pipeline::graph::leaf::counting::fill_after(room);
}

pub fn waits_said() -> u64 {
    crate::pipeline::graph::root::DISK_WAITS_SAID.with(std::cell::Cell::get)
}

pub fn wait_looks() -> u64 {
    crate::pipeline::graph::root::DISK_WAIT_LOOKS.with(std::cell::Cell::get)
}

pub fn reconcile_waits(pipeline: &mut Pipeline) {
    pipeline.reconcile_disk_waits();
}

pub fn graph_waiting(pipeline: &Pipeline) -> bool {
    pipeline.graph_disk_held(JOB)
}

pub fn in_disk_wait(pipeline: &Pipeline) -> bool {
    pipeline.disk_waiting.contains(&JOB)
}

pub fn graph_waiting_on_mail(pipeline: &Pipeline) -> bool {
    pipeline.graph_jobs[&JOB].awaits_mail()
}

pub async fn drive_until_graph_waiting(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, graph_waiting).await;
}

// What the job counts downloaded, while it runs and once it has ended.
pub fn counted(pipeline: &Pipeline) -> u64 {
    pipeline
        .jobs
        .get(&JOB)
        .map(|state| state.downloaded_bytes)
        .or_else(|| {
            pipeline
                .shared_state
                .get_job(JOB)
                .map(|job| job.downloaded_bytes)
        })
        .expect("the job is known")
}

pub fn hear_disk_on_busy_turn(pipeline: &mut Pipeline) {
    pipeline.hear_disk_on_busy_turn();
}

pub fn hear_disk_reading(pipeline: &mut Pipeline) {
    pipeline.disk_reading();
}

pub fn admission_held(pipeline: &Pipeline) -> bool {
    pipeline.admission_disk_held
}

pub fn capacity(pipeline: &Pipeline) -> &crate::operations::StorageCapacity {
    &pipeline.storage_capacity
}

pub fn full_disk_held(pipeline: &Pipeline) -> bool {
    pipeline.held_for_full_disk(JOB)
}

pub async fn drive_until_full_disk_held(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, full_disk_held).await;
}

pub fn full_disk_mark(pipeline: &Pipeline) -> Option<u64> {
    pipeline.full_disk_mark(JOB)
}

pub fn full_disk_rise(pipeline: &Pipeline) -> (Option<u64>, bool) {
    pipeline.full_disk_rise(JOB)
}

pub fn park_disk_held_jobs(pipeline: &mut Pipeline) {
    pipeline.park_drained_jobs();
}

pub fn active_download_queue_len(pipeline: &Pipeline) -> Option<usize> {
    pipeline.jobs[&JOB]
        .articles
        .active()
        .map(|articles| articles.download_queue.len())
}

pub fn queued_articles(pipeline: &Pipeline) -> usize {
    pipeline.jobs[&JOB].articles.queued_download_len()
}

pub fn articles_queued(pipeline: &Pipeline) -> bool {
    matches!(pipeline.jobs[&JOB].articles, JobArticles::Queued(_))
}

pub fn resume_disk_held_job(pipeline: &mut Pipeline) {
    pipeline.resume_job_runtime(JOB).unwrap();
}

pub fn hold_on_root(pipeline: &mut Pipeline, root: StorageRoot) {
    pipeline.hold_job_for_full_disk(JOB, root, None);
}

pub fn complete_directory(pipeline: &Pipeline) -> std::path::PathBuf {
    pipeline.complete_dir.clone()
}

pub fn promised_bytes(pipeline: &Pipeline) -> u64 {
    pipeline.full_disk_promised_bytes()
}

pub fn refuse_by_quota(quota: bool) {
    crate::pipeline::graph::leaf::counting::refuse_by_quota(quota);
}

pub fn drop_disk_hold(pipeline: &mut Pipeline) -> bool {
    pipeline.drop_full_disk_hold(JOB).is_some()
}

pub fn hold_step(pipeline: &mut Pipeline, needs: u64, quota: bool) {
    pipeline.hold_job_for_full_disk(
        JOB,
        StorageRoot::Intermediate,
        crate::pipeline::full_disk::Wants {
            needs: Some(needs),
            left: None,
            quota,
        },
    );
}

pub fn counted_and_checkpointed(pipeline: &Pipeline) -> (u64, u64) {
    let state = &pipeline.jobs[&JOB];
    (state.downloaded_bytes, state.checkpointed_download_bytes)
}

pub fn placed_articles(pipeline: &Pipeline) -> usize {
    let state = &pipeline.jobs[&JOB];
    let file = state
        .articles
        .assembly()
        .and_then(|assembly| {
            assembly.file(weaver_server_base::jobs::ids::NzbFileId {
                job_id: JOB,
                file_index: 0,
            })
        })
        .expect("the job holds its articles until it parks");
    (0..4u64)
        .filter(|ordinal| {
            file.placement_conflict(u32::MAX, ordinal * ARTICLE as u64, 1)
                .is_some()
        })
        .count()
}

pub async fn add_second(pipeline: &mut Pipeline) {
    let spec = crate::pipeline::tests::segmented_job_spec("cinder", "cinder.bin", &[512; 4]);
    let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
    pipeline
        .add_job(
            SECOND,
            spec,
            std::path::PathBuf::from("cinder.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.jobs.contains_key(&SECOND));
}

pub fn hold_claim(
    pipeline: &mut Pipeline,
    job: JobId,
    path: &Path,
    needs: Option<u64>,
    quota: bool,
) {
    use crate::pipeline::full_disk::{Stage, Wants};

    pipeline.hold_move_for_full_disk(
        job,
        Stage::Claim(Default::default()),
        path,
        "no room",
        Wants {
            needs,
            left: None,
            quota,
        },
    );
}

pub fn hold_on_quota(pipeline: &mut Pipeline) {
    use crate::pipeline::full_disk::Wants;

    pipeline.hold_job_for_full_disk(
        JOB,
        StorageRoot::Intermediate,
        Wants {
            quota: true,
            ..Wants::default()
        },
    );
}

pub fn refuse_spool_write(pipeline: &mut Pipeline, kind: std::io::ErrorKind, bytes: usize) {
    use crate::pipeline::decode::SegmentWriteError;
    use weaver_server_base::jobs::ids::NzbFileId;

    let file_id = NzbFileId {
        job_id: JOB,
        file_index: 0,
    };
    pipeline.fail_job_for_disk_write(
        SegmentWriteError::new(file_id, std::io::Error::from(kind)).of_bytes(bytes),
        "spooling a part",
    );
}

pub fn disk_held(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline.held_for_full_disk(job)
}

pub fn disk_held_on_path(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline.full_disk_on_path(job)
}

pub fn end_disk_promise(pipeline: &mut Pipeline) {
    pipeline.end_full_disk_promise(JOB);
}

pub fn disk_reserve_minimum(pipeline: &Pipeline) -> u64 {
    pipeline.extraction_limits.min_free_bytes
}

pub fn capacity_ref(pipeline: &Pipeline) -> Arc<crate::operations::StorageCapacity> {
    Arc::clone(&pipeline.storage_capacity)
}

pub fn pause_held_job(pipeline: &mut Pipeline) {
    pipeline.jobs.get_mut(&JOB).unwrap().run_state = RunState::Paused;
}

pub fn wake_full_disk(pipeline: &mut Pipeline) {
    pipeline.wake_full_disk();
}

pub fn prepare_held_spool(pipeline: &Pipeline) -> std::path::PathBuf {
    use crate::pipeline::held::{held_spool_dir, prepare_held_spool_dir};

    let spool = held_spool_dir(&pipeline.jobs[&JOB].working_dir);
    prepare_held_spool_dir(&spool).unwrap();
    spool
}

fn spool_segment(segment_number: u32) -> weaver_server_base::jobs::ids::SegmentId {
    use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number,
    }
}

pub fn spool_part(
    pipeline: &mut Pipeline,
    spool: &Path,
    name: &str,
    segment_number: u32,
) -> std::path::PathBuf {
    use crate::pipeline::held::HeldPart;

    let path = spool.join(name);
    std::fs::write(&path, b"ember").unwrap();
    pipeline.adopt_held_parts(vec![HeldPart {
        segment_id: spool_segment(segment_number),
        path: path.clone(),
        decoded_bytes: 5,
        article: None,
    }]);
    path
}

pub fn queue_spooled_releases(pipeline: &mut Pipeline) {
    pipeline.uu_park_requeues.insert(spool_segment(3), (1, 0));
    pipeline
        .unanchored_requeues
        .insert(spool_segment(3), (1, 0));
    pipeline.pending_unanchored_release.push(spool_segment(1));
}

pub struct HeldSpoolState {
    pub segments: usize,
    pub bytes: usize,
    pub has_file: bool,
    pub park_requeues: usize,
    pub unanchored_requeues: usize,
    pub pending_releases: usize,
}

pub fn held_spool_state(pipeline: &Pipeline) -> HeldSpoolState {
    HeldSpoolState {
        segments: pipeline.uu_spooled_segments,
        bytes: pipeline.uu_spooled_bytes,
        has_file: pipeline.uu_files.contains_key(&spool_segment(1).file_id),
        park_requeues: pipeline.uu_park_requeues.len(),
        unanchored_requeues: pipeline.unanchored_requeues.len(),
        pending_releases: pipeline.pending_unanchored_release.len(),
    }
}

pub fn hold_write(pipeline: &mut Pipeline, left: u64) {
    pipeline.hold_job_for_full_disk(
        JOB,
        StorageRoot::Intermediate,
        crate::pipeline::full_disk::Wants {
            left: Some(left),
            ..Default::default()
        },
    );
}

pub async fn finish_spool_sweeps(pipeline: &mut Pipeline) {
    for sweep in pipeline.full_disk_sweeps.drain(..) {
        sweep.await.unwrap();
    }
}

pub fn stage_on_disk(pipeline: &mut Pipeline, library: &Path) -> std::path::PathBuf {
    let staging = crate::pipeline::queue::staging_dir_for(library, JOB);
    crate::pipeline::queue::make_staging_dir(&staging, JOB).unwrap();
    pipeline.retire_graph_job(JOB);
    pipeline.jobs.get_mut(&JOB).unwrap().staging_dir = Some(staging.clone());
    pipeline.admit_graph_job(JOB, &[]);
    staging
}

pub async fn drive_until_graph_mail(pipeline: &mut Pipeline, scenario: &Scenario) {
    drive(pipeline, scenario, JOB, |pipeline| {
        pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
    })
    .await;
}

pub fn posted_segments(pipeline: &Pipeline) -> usize {
    pipeline.jobs[&JOB].spec.files[0].segments.len()
}

pub fn checkpointed_on_disk(pipeline: &Pipeline) -> usize {
    let pending = pipeline.graph_jobs[&JOB].pending();
    let file = &pipeline.jobs[&JOB].spec.files[0];
    file.segments
        .iter()
        .filter(|segment| pending.checkpoint_settled(0).contains(&segment.ordinal))
        .count()
}

pub fn stored_disk_segments(pipeline: &Pipeline) -> usize {
    pipeline.graph_jobs[&JOB]
        .pending()
        .checkpoint_settled(0)
        .len()
}

pub fn checkpoint_disk_job(pipeline: &mut Pipeline) {
    pipeline.checkpoint_graph_job(JOB);
}

pub async fn read_disk_checkpoint(pipeline: &mut Pipeline) {
    pipeline.pump_decode_queue();
    while pipeline
        .graph_jobs
        .get(&JOB)
        .is_some_and(|graph| graph.awaits_mail())
    {
        let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
        pipeline.graph_run_said(said);
    }
    pipeline.checkpoint_graph_job(JOB);
}

pub async fn reconcile_disk_completion(pipeline: &mut Pipeline) {
    pipeline.maybe_finish_download_pass(JOB);
    pipeline.reconcile_candidate_jobs().await;
}

pub fn no_pending_completion_checks(pipeline: &Pipeline) -> bool {
    pipeline.pending_completion_checks.is_empty()
}

pub fn has_live_graph(pipeline: &Pipeline) -> bool {
    pipeline.graph_jobs.contains_key(&JOB)
}

pub fn has_retiring_graph(pipeline: &Pipeline) -> bool {
    pipeline.retiring_graph_jobs.contains_key(&JOB)
}

pub fn retiring_graph_tally(pipeline: &Pipeline) -> (usize, usize, usize) {
    let tally = pipeline.retiring_graph_jobs[&JOB].pending().tally();
    (tally.checkpoints, tally.rows, tally.facts)
}

pub fn recover_cancelled_checkpoint(pipeline: &mut Pipeline) {
    pipeline.recover_cancelled_graph_checkpoint(JOB);
}

pub fn on_disk_probe(
    posts: Vec<Post>,
    reserve: u64,
    probe: weaver_server_disk::operations::disk::SharedProbeFn,
) -> Scenario {
    use crate::operations::StorageCapacity;

    Scenario::new(posts)
        .limits(super::actor_drive::limits(move |limits| {
            limits.min_free_bytes = reserve
        }))
        .setup(move |pipeline| {
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let capacity =
                StorageCapacity::with_probe(roots, super::disk_archive::NEVER, probe.clone());
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
}

struct Waits {
    scope: u64,
    ticket: u64,
    ledger: Arc<DiskLedger>,
    outside: std::sync::Weak<Outside>,
    asked: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}
impl Hears for Waits {
    fn said(&self, _said: crate::pipeline::graph::engine::Said, _slots: usize) {}

    fn hold_clock(&self, _paused: bool) {}

    fn disk_ticket(&self) -> Option<u64> {
        Some(self.ticket)
    }

    fn wake_disk(&self) -> bool {
        self.asked
            .lock()
            .unwrap()
            .push((self.scope, self.ledger.waiters()));
        if let Some(outside) = self.outside.upgrade() {
            outside.disk_unheld(self.scope);
        }
        self.ledger.unwait();
        true
    }

    fn disk_wait(&self) -> Option<(crate::pipeline::graph::limits::DiskWait, std::path::PathBuf)> {
        Some((
            crate::pipeline::graph::limits::DiskWait {
                device: crate::pipeline::graph::limits::Device::Working,
                owes: self.ticket,
                free: Some(0),
                reserve: 1,
            },
            std::path::PathBuf::from(format!("/waits/{}", self.scope)),
        ))
    }
}

pub fn disk_ticket(pipeline: &Pipeline) -> Option<u64> {
    pipeline.graph_jobs[&JOB].disk_ticket()
}

pub struct OutsideDiskWaiters {
    outside: Arc<Outside>,
    asked: Arc<std::sync::Mutex<Vec<(u64, u64)>>>,
}

pub fn outside_disk_waiters(pipeline: &Pipeline) -> OutsideDiskWaiters {
    OutsideDiskWaiters {
        outside: Arc::clone(&pipeline.graph_outside),
        asked: Default::default(),
    }
}

impl OutsideDiskWaiters {
    pub fn join(&self, scope: u64, ticket: u64, ledger: &Arc<DiskLedger>) {
        self.outside.join(
            scope,
            Arc::new(Waits {
                scope,
                ticket,
                ledger: Arc::clone(ledger),
                outside: Arc::downgrade(&self.outside),
                asked: Arc::clone(&self.asked),
            }),
        );
    }

    pub async fn disk_wait_began(&self) {
        self.outside.disk_wait_began().await;
    }

    pub fn disk_held(&self, scope: u64) {
        self.outside.disk_held(scope);
    }

    pub fn disk_waits_began(&self) -> Vec<u64> {
        self.outside.disk_waits_began()
    }

    pub fn wake_order(&self) -> Vec<(u64, u64)> {
        self.asked.lock().unwrap().clone()
    }

    pub fn waits_for_disk(&self) -> bool {
        self.outside.waits_for_disk()
    }
}
