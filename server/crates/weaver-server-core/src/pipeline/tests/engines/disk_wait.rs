// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job that waits for room on a disk: what each way out of the wait does.
// A cancel or a delete gives back every byte the job promised and stops it
// waiting; a pause of everything keeps it waiting, and once a reading shows
// room and the pause lifts it completes; a shutdown is not held by it; a
// restart restores it, and it completes. And what the reserve holds of
// fetching: a job that never downloaded waits to start, and a job that
// started fetches on, through a pause and a restart. And a write that finds
// the disk full: the job waits for room, not failed, and completes once
// there is room.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::harness::{ARTICLE, JOB, Post, Scenario, admit, payload, serve_job};
use super::lifecycle::drive;
use super::streams::xz;
use crate::jobs::model::{JobArticles, JobStatus, RunState};
use crate::operations::disk::DiskLedger;
use crate::operations::{DiskSpace, StorageCapacity, StorageRoot};
use crate::pipeline::graph::leaf::counting::fill_after;
use crate::pipeline::tests::job_status_for_assert;
use crate::pipeline::{Pipeline, PipelineEvent};
use crate::{JobId, SchedulerHandle};

const RESERVE: u64 = 1 << 30;
const PLENTY: u64 = 1 << 30;
// The room value at which a look at the disk fails.
const UNREADABLE: u64 = u64::MAX;
const NEVER: std::time::Duration = std::time::Duration::from_secs(365 * 24 * 60 * 60);

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

// A stream the job decodes into a file, on disks that read `room` bytes
// over `RESERVE` at every reading, the job held to keep `RESERVE` free.
fn on_disk(room: &Arc<AtomicU64>) -> Scenario {
    on_disk_posting(
        room,
        vec![Post::plain(
            "ember.bin.xz",
            xz(&payload(3 * ARTICLE + 7, 91)),
        )],
    )
}

fn on_disk_posting(room: &Arc<AtomicU64>, posts: Vec<Post>) -> Scenario {
    on_disk_keeping(room, posts, RESERVE)
}

// The same, the job held to keep `reserve` free and the disks reading
// `room` bytes over it.
fn on_disk_keeping(room: &Arc<AtomicU64>, posts: Vec<Post>, reserve: u64) -> Scenario {
    let room = Arc::clone(room);
    Scenario::new(posts)
        .limits(super::caps::limits(move |limits| {
            limits.min_free_bytes = reserve
        }))
        .setup(move |pipeline| {
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let room = Arc::clone(&room);
            let probe = Arc::new(move |_: &Path| match room.load(Ordering::Acquire) {
                UNREADABLE => Err(crate::operations::disk::DiskProbeError::InvalidPath),
                room => Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: reserve + room,
                }),
            });
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
}

// The ledger of the disk the job's working folder is on.
fn working(pipeline: &Pipeline) -> Arc<DiskLedger> {
    Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Intermediate)
            .ledger(),
    )
}

// Serve the job and hear its runs until it waits for room: its run decodes
// into a file, which the disk has no room for.
async fn until_waiting(pipeline: &mut Pipeline, scenario: &Scenario) {
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
async fn serve_one(pipeline: &mut Pipeline, scenario: &Scenario) {
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
fn row_reason(pipeline: &Pipeline, job: JobId) -> Option<String> {
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
async fn room_freed(pipeline: &mut Pipeline, room: &AtomicU64) {
    room.store(PLENTY, Ordering::Release);
    pipeline
        .storage_capacity
        .request_refresh(StorageRoot::Intermediate);
    while pipeline.waits_for_disk_room() {
        pipeline.storage_capacity.readings().notified().await;
        pipeline.disk_reading();
    }
}

fn assert_released(pipeline: &Pipeline, ledger: &DiskLedger) {
    assert!(!pipeline.waits_for_disk_room());
    assert!(!ledger.waited_on(), "no one waits on the disk");
    assert_eq!(ledger.promised(), 0, "nothing is promised on the disk");
}

// A job that holds its slot fetches on while the working disk reads no room
// over its reserve: the scheduler offers it, nothing waits on the disk, and
// the job completes.
#[tokio::test]
async fn a_started_job_fetches_on_under_the_reserve_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 93))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let ledger = working(&pipeline);

    assert_eq!(pipeline.download_scheduler_eligible_jobs(), vec![JOB]);
    assert!(!pipeline.waits_for_disk_room());
    assert!(!ledger.waited_on(), "nothing waits on the disk");
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        None
    );

    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// A job that never downloaded does not start while the working disk reads
// no room over its reserve: it holds no slot, and its row says it waits for
// disk space. A reading with room starts it; a job that started fetches on
// all along.
#[tokio::test]
async fn a_job_that_never_started_waits_for_room_to_start_and_starts_at_a_reading_with_room() {
    const OTHER: JobId = JobId(93_002);
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 95))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let ledger = working(&pipeline);
    let spec = crate::pipeline::tests::segmented_job_spec("cinder", "cinder.bin", &[512; 4]);
    let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
    pipeline
        .add_job(
            OTHER,
            spec,
            std::path::PathBuf::from("cinder.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;

    assert!(!pipeline.nzb_admitted.contains(&OTHER), "it did not start");
    assert!(pipeline.nzb_admitted.contains(&JOB));
    assert_eq!(
        row_reason(&pipeline, OTHER).as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON),
        "its row says it waits for disk space"
    );
    assert!(pipeline.waits_for_disk_room());
    assert!(ledger.waited_on(), "the wait hears every reading");
    let eligible = pipeline.download_scheduler_eligible_jobs();
    assert!(eligible.contains(&JOB), "the started job fetches on");
    assert!(!eligible.contains(&OTHER));
    let queued = |pipeline: &Pipeline| {
        pipeline.jobs[&JOB]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
    };
    let before = queued(&pipeline);
    pipeline.dispatch_downloads();
    assert!(
        queued(&pipeline) < before,
        "the dispatch pass hands the started job's articles out"
    );

    room_freed(&mut pipeline, &room).await;
    assert!(!ledger.waited_on());
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&OTHER), "it started");
    assert_ne!(
        row_reason(&pipeline, OTHER).as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
}

// A job that started and holds its slot when the pipeline goes down is
// restored by the next one as started: under the reserve it fetches on and
// completes.
#[tokio::test]
async fn a_started_job_restarted_under_the_reserve_fetches_on_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 96))],
    );
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    serve_one(&mut pipeline, &scenario).await;

    let mut pipeline = super::restart::restart(
        &temp_dir,
        pipeline,
        &scenario,
        &mut std::collections::BTreeSet::new(),
        &mut |_| {},
    )
    .await;
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&JOB), "restored as started");
    assert_eq!(pipeline.download_scheduler_eligible_jobs(), vec![JOB]);
    assert!(!pipeline.waits_for_disk_room());
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// A cancel, and a cancel that deletes the job's history, of a job waiting
// for room: it waits no more, and gives back all it promised.
#[tokio::test]
async fn a_job_cancelled_or_deleted_while_it_waits_for_room_gives_back_all_it_promised() {
    for delete in [false, true] {
        let room = Arc::new(AtomicU64::new(0));
        let scenario = on_disk(&room);
        let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
        until_waiting(&mut pipeline, &scenario).await;
        let ledger = working(&pipeline);

        let handle = handle(&pipeline);
        if delete {
            handle.cancel_job_and_delete_history(JOB).await.unwrap();
        } else {
            handle.cancel_job(JOB).await.unwrap();
        }
        pipeline.apply_queue_intents().await;
        super::super::finish_cancel_jobs(&mut pipeline).await;
        assert!(!pipeline.graph_jobs.contains_key(&JOB));
        assert_released(&pipeline, &ledger);
    }
}

// Every job paused at once while a job's run waits for room: the wait goes
// on. A reading with room wakes it, the pause still holds the job, and once
// the pause lifts it completes.
#[tokio::test]
async fn a_paused_job_waiting_for_room_completes_once_there_is_room_and_it_resumes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    until_waiting(&mut pipeline, &scenario).await;
    let ledger = working(&pipeline);

    let handle = handle(&pipeline);
    handle.pause_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.waits_for_disk_room(), "a pause ends no wait");

    room_freed(&mut pipeline, &room).await;
    assert!(!ledger.waited_on());
    assert!(
        !pipeline.download_scheduler_eligible_jobs().contains(&JOB),
        "the pause still holds the job"
    );

    handle.resume_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.waits_for_disk_room());
    assert_ne!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// A job that started, was paused, giving back its slot, and resumed while
// the working disk reads no room over its reserve counts as started: it
// takes its slot again, fetches on and completes.
#[tokio::test]
async fn a_started_job_paused_and_resumed_under_the_reserve_fetches_on_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 94))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    serve_one(&mut pipeline, &scenario).await;

    let handle = handle(&pipeline);
    handle.pause_job(JOB).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs[&JOB].run_state, RunState::Paused);
    assert!(
        !pipeline.nzb_admitted.contains(&JOB),
        "the pause gave back its slot"
    );

    handle.resume_job(JOB).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert!(
        pipeline.nzb_admitted.contains(&JOB),
        "it took its slot again"
    );
    assert_eq!(pipeline.jobs[&JOB].run_state, RunState::Active);
    assert!(!pipeline.waits_for_disk_room());
    assert_ne!(
        row_reason(&pipeline, JOB).as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    assert_eq!(pipeline.download_scheduler_eligible_jobs(), vec![JOB]);
    // A paused job let its articles go: it reads them back as it is reached.
    crate::pipeline::tests::activate_job(&mut pipeline, JOB).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// A shutdown asked for while a job waits for room ends the pipeline's run:
// the wait holds nothing up.
#[tokio::test]
async fn a_shutdown_is_not_held_by_a_job_waiting_for_room() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    until_waiting(&mut pipeline, &scenario).await;

    handle(&pipeline).shutdown().await.unwrap();
    pipeline.run().await;
    assert!(pipeline.waits_for_disk_room(), "nothing woke the job");
}

// A job that waits for room when the pipeline goes down is restored by the
// next one, which reads the disk afresh: with room there, it completes.
#[tokio::test]
async fn a_job_waiting_for_room_at_a_restart_is_restored_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    until_waiting(&mut pipeline, &scenario).await;

    pipeline.drain().await;
    room.store(PLENTY, Ordering::Release);
    let mut pipeline = super::restart::restart(
        &temp_dir,
        pipeline,
        &scenario,
        &mut std::collections::BTreeSet::new(),
        &mut |_| {},
    )
    .await;
    assert!(!pipeline.waits_for_disk_room());
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// What a job did as it was driven until it ended or waited for room with
// nothing of it left running: how many of its runs returned, whether it was
// ever held, and how many times it began to be held.
#[derive(Debug, Default)]
struct Settled {
    ended: bool,
    runs: usize,
    held: bool,
    holds: usize,
}

// Serve the job, hear its runs and reconcile its waits as the loop does,
// with no new reading of the disk, until it ends or waits for room with no
// run of it live.
async fn settle(pipeline: &mut Pipeline, scenario: &Scenario, settled: &mut Settled) {
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

// A reading of the working disk showing `value` bytes over the reserve is
// heard: the loop waits for that reading itself, not for an older one.
async fn reading_of(pipeline: &mut Pipeline, room: &AtomicU64, value: u64) {
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

// Every file under the job's complete directory, by its path there.
fn delivered(complete_dir: &Path) -> std::collections::BTreeMap<String, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    super::harness::collect(complete_dir, complete_dir, &mut files);
    files
}

fn file_named<'a>(files: &'a std::collections::BTreeMap<String, Vec<u8>>, name: &str) -> &'a [u8] {
    files
        .iter()
        .find(|(path, _)| path.rsplit('/').next() == Some(name))
        .map(|(_, bytes)| bytes.as_slice())
        .unwrap_or_else(|| panic!("{name} is delivered: {:?}", files.keys()))
}

// An xz stream holding a tar whose first member is a tar of its own, then a
// member that spans several of the run's writes; and the length of the tar
// the run decodes, with the two files.
fn nested() -> (Vec<u8>, u64, Vec<u8>, Vec<u8>) {
    let a = vec![3u8; 1000];
    let b = vec![5u8; 200_000];
    let inner = super::streams::tar(&[("a.bin", a.clone())]);
    let outer = super::streams::tar(&[("inner.tar", inner), ("b.bin", b.clone())]);
    (xz(&outer), outer.len() as u64, a, b)
}

// The run decodes an xz stream into a tar it keeps; the tar's first member
// is a tar, built and finished, its file written, before the run writes
// the second member, which the disk has no room for. The disk has room for
// the run's own member, the tar, but not for that and every member built
// under it: the stopped run writes them all again. The job waits; a reading
// at that room does not wake it, so no run goes out again, however many
// readings come. A reading with room wakes it once, and it completes.
#[tokio::test]
async fn a_run_stopped_for_room_waits_for_all_it_built_and_does_not_run_again_short_of_it() {
    let (posted, kept, a, b) = nested();
    let room = Arc::new(AtomicU64::new(kept + 200));
    let scenario = on_disk_posting(&room, vec![Post::plain("nest.tar.xz", posted)]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    let mut settled = Settled::default();
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(!settled.ended, "the job waits for room");
    assert_eq!(settled.runs, 1, "one run returned");
    assert!(pipeline.waits_for_disk_room());
    // Turns with nothing changed look at no waiting job.
    let looks = crate::pipeline::graph::root::DISK_WAIT_LOOKS.with(std::cell::Cell::get);
    for _ in 0..3 {
        pipeline.reconcile_disk_waits();
    }
    assert_eq!(
        crate::pipeline::graph::root::DISK_WAIT_LOOKS.with(std::cell::Cell::get),
        looks,
        "an idle turn looks at no job"
    );

    for _ in 0..3 {
        reading_of(&mut pipeline, &room, kept + 200).await;
        assert!(
            pipeline.graph_disk_held(JOB),
            "a reading short of it wakes nothing"
        );
        assert!(
            !pipeline.graph_jobs[&JOB].awaits_mail(),
            "no run went out again"
        );
        assert!(pipeline.waits_for_disk_room());
    }

    room_freed(&mut pipeline, &room).await;
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(settled.ended);
    assert_eq!(settled.runs, 2, "the run ran once more");
    assert_eq!(settled.holds, 1, "once woken it was not held again");
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let files = delivered(&complete_dir);
    assert_eq!(file_named(&files, "a.bin"), a.as_slice());
    assert_eq!(file_named(&files, "b.bin"), b.as_slice());
    assert_released(&pipeline, &working(&pipeline));
}

// A run's last write takes the disk under its reserve, and what the run
// says after it closes the file and writes nothing: the run is not stopped,
// the file it delivered stays, and the job, owing nothing, completes with
// no room freed.
#[tokio::test]
async fn a_run_whose_last_write_crosses_the_reserve_keeps_its_file_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(3 * ARTICLE + 7, 91);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin.xz", xz(&body))]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    let mut settled = Settled::default();
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(settled.held, "the write took the disk under its reserve");
    assert!(settled.ended, "the job did not wait");
    assert_eq!(settled.runs, 1, "the run was not stopped");
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
    assert_released(&pipeline, &working(&pipeline));
}

// A stored archive's member is written straight from the posts: no run
// writes it. Its first write takes the disk under the reserve; once it
// lands and closes the job owes nothing, so it completes with no room
// freed.
#[tokio::test]
async fn a_stored_archive_written_from_its_posts_completes_with_no_room_freed() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(16 * ARTICLE + 13, 41);
    let posts = crate::pipeline::tests::direct_store::single_member_store_set(
        "silver.horizon.mkv",
        &body,
        4,
    )
    .into_iter()
    .map(|(name, bytes)| Post::plain(&name, bytes))
    .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    let mut settled = Settled::default();
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(settled.held, "the member took the disk under its reserve");
    assert!(settled.ended, "the job did not wait");
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "silver.horizon.mkv"),
        body.as_slice()
    );
    assert_released(&pipeline, &working(&pipeline));
}

// The job's working and complete folders lie on one filesystem: their
// disks count together. The run is stopped once, the job wakes once, at
// the first reading with room for all it owes on both, and completes, with
// nothing left promised or waited on either.
#[tokio::test]
async fn a_job_on_two_folders_of_one_disk_is_stopped_once_and_woken_once() {
    let (posted, _, a, b) = nested();
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_posting(&room, vec![Post::plain("nest.tar.xz", posted)]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let output = Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    );
    let working = working(&pipeline);
    assert!(output.same_disk(&working), "the folders share a disk");

    let mut settled = Settled::default();
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(!settled.ended, "the job waits for room");
    assert_eq!(settled.runs, 1);

    room_freed(&mut pipeline, &room).await;
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(settled.ended);
    assert_eq!(settled.runs, 2, "stopped once");
    assert_eq!(settled.holds, 1, "held once, and woken once for good");
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let files = delivered(&complete_dir);
    assert_eq!(file_named(&files, "a.bin"), a.as_slice());
    assert_eq!(file_named(&files, "b.bin"), b.as_slice());
    assert_released(&pipeline, &working);
    assert_released(&pipeline, &output);
}

// A run stopped for room and run again delivers what one never stopped
// does: the same files, the same status and the same events. The disk
// reads room only once the job has begun to wait: its first hold asks for
// the reading that wakes it.
#[tokio::test]
async fn a_run_stopped_for_room_and_run_again_delivers_what_an_unstopped_run_does() {
    use super::harness::run;

    let outcome = async |start: u64| {
        let (posted, ..) = nested();
        let room = Arc::new(AtomicU64::new(start));
        let runs = Arc::new(AtomicU64::new(0));
        let heard_room = Arc::clone(&room);
        let heard_runs = Arc::clone(&runs);
        let scenario =
            on_disk_posting(&room, vec![Post::plain("nest.tar.xz", posted)]).heard(move |said| {
                // Any reading from here, the first that the hold asks for,
                // shows room.
                heard_room.store(PLENTY, Ordering::Release);
                if matches!(said.heard, crate::pipeline::graph::engine::Heard::Ended) {
                    heard_runs.fetch_add(1, Ordering::AcqRel);
                }
            });
        let outcome = run(&scenario).await;
        let runs = runs.load(Ordering::Acquire);
        (outcome, runs)
    };
    let (stopped, stopped_runs) = outcome(0).await;
    let (unstopped, unstopped_runs) = outcome(PLENTY).await;

    assert_eq!(unstopped_runs, 1);
    assert_eq!(stopped_runs, 2, "the run was stopped and ran again");
    assert_eq!(stopped.status, unstopped.status);
    assert!(stopped.completed(), "{:?}", stopped.failure);
    assert_eq!(stopped.files, unstopped.files);
    assert_eq!(
        format!("{:?}", stopped.events),
        format!("{:?}", unstopped.events)
    );
}

// A turn busy with results, which never waits on the next event, still
// hears a reading with room: the job that waits for room wakes, and the job
// kept from starting starts, though the loop never goes idle.
#[tokio::test]
async fn a_reading_with_room_is_heard_on_a_turn_busy_with_results() {
    const OTHER: JobId = JobId(93_011);
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    until_waiting(&mut pipeline, &scenario).await;
    let spec = crate::pipeline::tests::segmented_job_spec("cinder", "cinder.bin", &[512; 4]);
    let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
    pipeline
        .add_job(
            OTHER,
            spec,
            std::path::PathBuf::from("cinder.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.admission_disk_held, "it is kept from starting");
    assert!(!pipeline.nzb_admitted.contains(&OTHER));

    // Every reading so far heard, as a turn would: room 0 wakes no one.
    pipeline.hear_disk_on_busy_turn();
    assert!(pipeline.graph_disk_held(JOB));
    assert!(pipeline.admission_disk_held);

    // The reading with room is published; no turn waits for it.
    room.store(PLENTY, Ordering::Release);
    pipeline
        .storage_capacity
        .request_refresh(StorageRoot::Intermediate);
    pipeline
        .storage_capacity
        .wait_for_reading(StorageRoot::Intermediate, |reading| {
            reading.available_bytes == RESERVE + PLENTY
        });
    assert!(pipeline.graph_disk_held(JOB), "nothing heard it yet");

    pipeline.hear_disk_on_busy_turn();
    assert!(!pipeline.graph_disk_held(JOB), "the waiting job woke");
    assert!(!pipeline.waits_for_disk_room());
    assert!(!pipeline.admission_disk_held, "the hold ended");
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&OTHER), "it started");
}

// Serve the job until a write finds the disk full, and let it park, as the
// loop's next turn does once nothing of it is out: it is held for room, not
// failed, waits on the working disk's ledger, says it waits for disk space,
// and fetches nothing.
async fn until_full(pipeline: &mut Pipeline, scenario: &Scenario) {
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
async fn old_graph_heard(pipeline: &mut Pipeline) {
    while pipeline.retiring_graph_jobs.contains_key(&JOB) {
        let said = pipeline
            .graph_runs
            .recv()
            .await
            .expect("the retiring graph's runs answer");
        pipeline.graph_run_said(said);
    }
}

// `root` is looked at afresh, by a probe begun after this call, reading
// `value` bytes over the reserve, and the loop hears that reading.
fn hears(pipeline: &mut Pipeline, room: &AtomicU64, root: StorageRoot, value: u64) {
    room.store(value, Ordering::Release);
    let begun = pipeline.storage_capacity.probes_begun(root);
    pipeline.storage_capacity.request_refresh(root);
    pipeline
        .storage_capacity
        .wait_for_reading_begun_after(root, begun);
    pipeline.disk_reading();
}

// The job let go reads its articles back and restores its graph: its
// release asked for that itself, with no lane asking for work.
async fn restarts(pipeline: &mut Pipeline) {
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

fn assert_full_disk_held(pipeline: &Pipeline) {
    assert!(pipeline.held_for_full_disk(JOB));
    assert!(
        !matches!(
            job_status_for_assert(pipeline, JOB),
            Some(JobStatus::Failed { .. })
        ),
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
async fn full_disk_freed(pipeline: &mut Pipeline, scenario: &Scenario, room: &AtomicU64) {
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
    assert_eq!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_released(pipeline, &working(pipeline));
}

// A plain file's write finds the disk full part-way through its download,
// though the cached reading showed room: the job waits for room, and once
// there is room it fetches again what it lost and delivers the file whole.
#[tokio::test]
async fn a_plain_file_whose_write_finds_the_disk_full_waits_for_room_and_completes() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 97);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    // The disk is full from here; no reading has said so yet.
    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));

    until_full(&mut pipeline, &scenario).await;
    full_disk_freed(&mut pipeline, &scenario, &room).await;

    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A stored archive's member is unpacked straight from its posts; one of
// its writes finds the disk full part-way through the download: the job
// waits for room, and once there is room it writes again what it lost and
// delivers the member whole.
#[tokio::test]
async fn an_unpacking_job_whose_write_finds_the_disk_full_waits_for_room_and_completes() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(16 * ARTICLE + 13, 43);
    let posts = crate::pipeline::tests::direct_store::single_member_store_set(
        "silver.horizon.mkv",
        &body,
        4,
    )
    .into_iter()
    .map(|(name, bytes)| Post::plain(&name, bytes))
    .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    room.store(0, Ordering::Release);
    fill_after(Some(6 * ARTICLE as u64));

    until_full(&mut pipeline, &scenario).await;
    full_disk_freed(&mut pipeline, &scenario, &room).await;

    assert_eq!(
        file_named(&delivered(&complete_dir), "silver.horizon.mkv"),
        body.as_slice()
    );
}

// What the job counts downloaded, while it runs and once it has ended.
fn counted(pipeline: &Pipeline) -> u64 {
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

// Results still out as a write finds the disk full come back for a job
// with no graph to take them: each is dropped whole, neither placed nor
// counted, and the job counts what its last checkpoint holds. Once it has
// run on and completed it has counted exactly what a job never held counts.
#[tokio::test]
async fn results_that_come_back_for_a_job_held_for_a_full_disk_are_not_counted() {
    let body = payload(4 * ARTICLE, 97);
    let post = || vec![Post::plain("ember.bin", body.clone())];

    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(&room, post());
    let (_never_held_dir, mut never_held, _) = admit(&scenario).await;
    drive(&mut never_held, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&never_held, JOB),
        Some(JobStatus::Complete)
    );
    let whole = counted(&never_held);
    assert!(whole >= body.len() as u64);

    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(&room, post());
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));

    // Every article is served: those after the write that failed come back
    // for a job already held.
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert!(pipeline.held_for_full_disk(JOB));
    let state = &pipeline.jobs[&JOB];
    assert_eq!(
        state.downloaded_bytes, state.checkpointed_download_bytes,
        "the job counts what its checkpoint holds"
    );
    assert!(
        counted(&pipeline) <= ARTICLE as u64,
        "nothing that came back after the write failed is counted: {}",
        counted(&pipeline)
    );
    // The article written and the one whose write failed were placed; the
    // two that came back after were not.
    let file = state
        .articles
        .assembly()
        .and_then(|assembly| {
            assembly.file(crate::jobs::ids::NzbFileId {
                job_id: JOB,
                file_index: 0,
            })
        })
        .expect("the job holds its articles until it parks");
    let placed = (0..4u64)
        .filter(|ordinal| {
            file.placement_conflict(u32::MAX, ordinal * ARTICLE as u64, 1)
                .is_some()
        })
        .count();
    assert!(placed <= 2, "{placed} articles are placed");

    pipeline.park_drained_jobs();
    full_disk_freed(&mut pipeline, &scenario, &room).await;
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
    assert_eq!(
        counted(&pipeline),
        whole,
        "held and run on, the job counts what a job never held counts"
    );
}

// A job held for a full disk goes back to its last checkpoint in what it
// keeps between an article's arrival and its placement: its cursors, its
// requeue counts and the releases it owed all go, a spooled part that
// checkpoint names stays on disk for the restore, and one it does not name,
// which nothing would read again, is removed from the full disk.
#[tokio::test]
async fn a_job_held_for_a_full_disk_keeps_only_the_spooled_parts_its_checkpoint_names() {
    use crate::jobs::ids::{NzbFileId, SegmentId};
    use crate::pipeline::held::HeldPart;

    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let file_id = NzbFileId {
        job_id: JOB,
        file_index: 0,
    };
    let part = |segment_number| SegmentId {
        file_id,
        segment_number,
    };
    let spool = crate::pipeline::held::held_spool_dir(&pipeline.jobs[&JOB].working_dir);
    crate::pipeline::held::prepare_held_spool_dir(&spool).unwrap();
    let spooled = |pipeline: &mut Pipeline, name: &str, segment_number| {
        let path = spool.join(name);
        std::fs::write(&path, b"ember").unwrap();
        pipeline.adopt_held_parts(vec![HeldPart {
            segment_id: part(segment_number),
            path: path.clone(),
            decoded_bytes: 5,
            article: None,
        }]);
        path
    };
    // One part is spooled as the checkpoint is taken, one after it.
    let named = spooled(&mut pipeline, "held-named", 2);
    pipeline.checkpoint_graph_job(JOB);
    let stray = spooled(&mut pipeline, "held-stray", 3);
    pipeline.uu_park_requeues.insert(part(3), (1, 0));
    pipeline.unanchored_requeues.insert(part(3), (1, 0));
    pipeline.pending_unanchored_release.push(part(1));
    assert_eq!(pipeline.uu_spooled_segments, 2);

    // The write that failed had ten bytes left to write.
    pipeline.hold_job_for_full_disk(
        JOB,
        StorageRoot::Intermediate,
        crate::pipeline::full_disk::Wants {
            left: Some(10),
            ..Default::default()
        },
    );
    for sweep in pipeline.full_disk_sweeps.drain(..) {
        sweep.await.unwrap();
    }
    // The five bytes the hold swept show as free at the next reading. They
    // are not room made: the job fills them again as it fetches that part.
    // A reading that shows them, or them and nine bytes more, lets nothing
    // go; one that shows the ten over them does.
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE + PLENTY));
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 5);
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 14);
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 15);
    assert_eq!(pipeline.full_disk_promised_bytes(), 10);

    assert!(
        named.exists(),
        "the checkpoint's part stays for the restore"
    );
    assert!(!stray.exists(), "a part no checkpoint names is removed");
    assert!(!pipeline.uu_files.contains_key(&file_id));
    assert!(pipeline.uu_park_requeues.is_empty());
    assert!(pipeline.unanchored_requeues.is_empty());
    assert!(pipeline.pending_unanchored_release.is_empty());
    assert_eq!(
        pipeline.uu_spooled_segments, 0,
        "the spool is charged nothing"
    );
    assert_eq!(pipeline.uu_spooled_bytes, 0);
}

// A job whose destination lies outside the complete folder writes the files
// it delivers to a staging folder there, which its storage judges against
// the output disk. A write there that finds the disk full holds the job on
// that disk, not on the working one: a reading of the working disk with
// room does not let it go, and a reading of the output disk with room does.
#[tokio::test]
async fn a_job_delivering_outside_the_complete_folder_waits_on_the_disk_its_write_failed_on() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 97);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    // The job is given the staging a category folder on another disk gives
    // it, and its graph is built over that storage.
    let library = temp_dir.path().join("library-disk");
    let staging = crate::pipeline::queue::staging_dir_for(&library, JOB);
    crate::pipeline::queue::make_staging_dir(&staging, JOB).unwrap();
    assert!(!staging.starts_with(&pipeline.complete_dir));
    pipeline.retire_graph_job(JOB);
    pipeline.jobs.get_mut(&JOB).unwrap().staging_dir = Some(staging);
    pipeline.admit_graph_job(JOB);
    let output = Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    );
    let waits_before = (output.tickets(), working(&pipeline).tickets());

    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    fill_after(None);
    assert!(pipeline.held_for_full_disk(JOB));
    assert_eq!(
        (output.tickets(), working(&pipeline).tickets()),
        (waits_before.0 + 1, waits_before.1),
        "the job waits on the disk its write failed on"
    );
    pipeline.park_drained_jobs();

    old_graph_heard(&mut pipeline).await;

    // The working disk reads room made: the job's own disk has said nothing
    // of more room.
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    assert!(
        pipeline.held_for_full_disk(JOB),
        "its own disk has not read"
    );

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!pipeline.held_for_full_disk(JOB), "its disk has room");
    assert!(!output.waited_on());
}

// A job let go that finds its disk full again, having saved nothing new,
// waits for more room than its disk has yet shown: readings that never
// change, as of a disk whose free space is not what stops the write, do not
// send it round. A resume by its user lets it try once, whatever they say.
#[tokio::test]
async fn a_job_let_go_that_fails_again_waits_for_more_room_than_its_disk_has_shown() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 113);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    // Parked, it counts as left to fetch every article its last checkpoint
    // does not hold: what it fetches when it goes on.
    let left = pipeline.jobs[&JOB].articles.queued_download_len();
    assert!((1..=4).contains(&left), "{left} of 4 articles are left");

    // The disk reads the same room at every look: none was made.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    assert!(
        matches!(pipeline.jobs[&JOB].articles, JobArticles::Queued(_)),
        "no reading without more room restarts it"
    );

    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE + PLENTY),
        "it waits for more than its disk showed as the write failed"
    );

    // Its user resumes it, though it is not paused: it tries once, and the
    // write fails again with the disk showing what it showed.
    fill_after(Some(0));
    handle(&pipeline).resume_job(JOB).await.unwrap();
    pipeline.apply_queue_intents().await;
    restarts(&mut pipeline).await;
    assert_eq!(
        pipeline.jobs[&JOB]
            .articles
            .active()
            .map(|articles| articles.download_queue.len()),
        Some(left),
        "restored, it has left to fetch what it showed left while it waited"
    );
    until_full(&mut pipeline, &scenario).await;
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);

    // A reading that shows the room of what was left of its file over the
    // mark lets it go, once; a byte more than the mark does not. The write
    // fails again with the disk showing that: the same figure does not let
    // it go a second time.
    let first = pipeline.full_disk_rise(JOB).0.expect("what was left");
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 1);
    assert_full_disk_held(&pipeline);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + first,
    );
    restarts(&mut pipeline).await;
    until_full(&mut pipeline, &scenario).await;
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + first,
    );
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE + PLENTY + first),
        "the new wait has what the disk showed at its own failure to beat"
    );

    // More again: room was made, and this time the write lands.
    let second = pipeline.full_disk_rise(JOB).0.expect("what was left");
    fill_after(None);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + first + second,
    );
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A reading that shows more than the disk did as the write failed, with too
// little of it free to spend, lets no job go and changes nothing: when what
// is promised on the disk is given back, the same figure lets the job go.
#[tokio::test]
async fn room_shown_but_promised_to_others_keeps_the_job_waiting_until_it_is_free() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(4 * ARTICLE, 139);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE));

    // Another job has the room made promised to it.
    working(&pipeline).promise(PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE),
        "a reading that lets no job go raises nothing"
    );

    // That job gives it back: the disk shows the same figure.
    working(&pipeline).release(PLENTY);
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A job let go at a reading with much room finds the disk full again, with
// less shown by then: it waits for more than the disk showed at that
// failure, not for more than let it go before.
#[tokio::test]
async fn a_job_let_go_whose_disk_is_filled_again_goes_on_when_less_room_is_made() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(4 * ARTICLE, 149);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    fill_after(Some(0));
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;

    // Something else fills the disk before the job writes again.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE));

    // Less room than let it go before, and more than the disk showed.
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A job held on one disk, let go, and held again on another waits for more
// than that other disk showed, whatever the first one did.
#[tokio::test]
async fn a_second_wait_on_another_disk_takes_that_disks_figure() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(4 * ARTICLE, 151);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;

    // The output disk shows little, the working disk much.
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE + 2 * PLENTY));

    fill_after(None);
    pipeline.resume_job_runtime(JOB).unwrap();
    restarts(&mut pipeline).await;

    // Its next write fails on the output disk, which still shows little.
    room.store(0, Ordering::Release);
    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Complete, None);
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE),
        "the wait has its own disk's figure to beat"
    );
    pipeline.park_drained_jobs();
    old_graph_heard(&mut pipeline).await;
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert!(pipeline.held_for_full_disk(JOB));
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert!(
        !pipeline.held_for_full_disk(JOB),
        "more than the output disk showed lets it go"
    );
}

// The disk could not be read as the write failed: the last figure it showed,
// stale as it is, is what a reading has to beat. A further look that fails
// lets nothing go, and a good reading with more room does.
#[tokio::test]
async fn a_stale_figure_at_the_failure_is_what_a_reading_has_to_beat() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(4 * ARTICLE, 157);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    // A good reading, then a look that fails: the figure kept is stale.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert!(
        pipeline
            .storage_capacity
            .current(StorageRoot::Intermediate)
            .reading()
            .is_some_and(|reading| reading.stale)
    );
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE),
        "the stale figure is what the disk showed"
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert_full_disk_held(&pipeline);
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE));

    // The disk reads again, with room made meanwhile: the job goes at once.
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A disk that cannot be read, or whose last good reading is stale, lets no
// held job go: the job waits, shown waiting for disk space, until a good
// reading shows room was made.
#[tokio::test]
async fn a_job_held_for_a_full_disk_waits_on_while_its_disk_cannot_be_read() {
    let room = Arc::new(AtomicU64::new(UNREADABLE));
    let body = payload(4 * ARTICLE, 127);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;

    // No reading at all.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert!(
        pipeline
            .storage_capacity
            .current(StorageRoot::Intermediate)
            .reading()
            .is_none()
    );
    assert_full_disk_held(&pipeline);

    // A good reading, then looks that fail: the reading kept is stale.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert!(
        pipeline
            .storage_capacity
            .current(StorageRoot::Intermediate)
            .reading()
            .is_some_and(|reading| reading.stale)
    );
    assert_full_disk_held(&pipeline);

    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A job held for a full disk with every article already in, its stream
// still being decoded: it counts as left every article past its last
// checkpoint, and is not taken for a job whose download is done. Its
// release restarts it itself, it fetches those again, and it completes.
#[tokio::test]
async fn a_job_held_for_a_full_disk_at_its_end_counts_what_is_past_its_checkpoint_as_left() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
    })
    .await;
    assert!(
        pipeline.jobs[&JOB]
            .articles
            .active()
            .is_some_and(|articles| articles.download_queue.is_empty()),
        "every article is in"
    );
    let mut events = pipeline.event_tx.subscribe();
    let checkpointed = {
        let pending = pipeline.graph_jobs[&JOB].pending();
        let file = &pipeline.jobs[&JOB].spec.files[0];
        file.segments
            .iter()
            .filter(|segment| pending.checkpoint_settled(0).contains(&segment.ordinal))
            .count()
    };
    let articles = pipeline.jobs[&JOB].spec.files[0].segments.len();
    assert!(
        checkpointed < articles,
        "the graph is ahead of its last checkpoint"
    );
    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Intermediate, None);
    // Held with nothing queued and nothing out, it is not taken for a job
    // whose download is done.
    pipeline.maybe_finish_download_pass(JOB);
    pipeline.reconcile_candidate_jobs().await;
    pipeline.park_drained_jobs();
    old_graph_heard(&mut pipeline).await;
    pipeline.maybe_finish_download_pass(JOB);
    pipeline.reconcile_candidate_jobs().await;
    assert_full_disk_held(&pipeline);
    let left = pipeline.jobs[&JOB].articles.queued_download_len();
    assert_eq!(
        left,
        articles - checkpointed,
        "parked, it counts as left every article its last checkpoint does not hold"
    );
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "no completion check is asked of a held job"
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    assert_full_disk_held(&pipeline);
    let mut heard = Vec::new();
    while let Ok(event) = events.try_recv() {
        heard.push(event);
    }
    assert!(
        heard.iter().all(|event| !matches!(
            event,
            crate::pipeline::PipelineEvent::DownloadFinished { .. }
                | crate::pipeline::PipelineEvent::DownloadPipelineDrained { .. }
        )),
        "held, its download is neither finished nor drained: {heard:?}"
    );
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    assert_eq!(
        pipeline.jobs[&JOB]
            .articles
            .active()
            .map(|articles| articles.download_queue.len()),
        Some(left),
        "restored, it fetches what it showed left"
    );
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        payload(3 * ARTICLE + 7, 91).as_slice()
    );
}

// A job held, let go and restored from its stored checkpoint, whose write
// fails again before the run takes a checkpoint of its own: what the stored
// checkpoint holds still counts as in hand, and the job shows left what it
// showed left the first time.
#[tokio::test]
async fn a_restored_job_held_again_before_its_first_checkpoint_keeps_what_was_stored_in_hand() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 127);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body)]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let articles = pipeline.jobs[&JOB].spec.files[0].segments.len();

    // Two articles are in and written when a checkpoint is taken; the write
    // of the next finds the disk full.
    serve_one(&mut pipeline, &scenario).await;
    serve_one(&mut pipeline, &scenario).await;
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
    let stored = pipeline.graph_jobs[&JOB]
        .pending()
        .checkpoint_settled(0)
        .len();
    assert!(
        (1..articles).contains(&stored),
        "the checkpoint holds some of the job: {stored} of {articles}"
    );
    fill_after(Some(0));
    until_full(&mut pipeline, &scenario).await;
    let left = pipeline.jobs[&JOB].articles.queued_download_len();
    assert_eq!(left, articles - stored);

    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    assert!(pipeline.graph_jobs.contains_key(&JOB), "it was restored");

    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Intermediate, None);
    pipeline.park_drained_jobs();
    old_graph_heard(&mut pipeline).await;
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.jobs[&JOB].articles.queued_download_len(),
        left,
        "what the stored checkpoint holds is still in hand"
    );
}

// A held job's old graph, still to hear its mail, says more than the job's
// files hold: a cancel handed back for want of its archive takes no
// checkpoint of it.
#[tokio::test]
async fn a_cancel_handed_back_takes_no_checkpoint_of_a_held_jobs_old_graph() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;

    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline
            .graph_jobs
            .get(&JOB)
            .is_some_and(|job| job.awaits_mail())
    })
    .await;
    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Intermediate, None);
    assert!(
        pipeline.retiring_graph_jobs.contains_key(&JOB),
        "the old graph still has something out"
    );
    let taken = pipeline.retiring_graph_jobs[&JOB].pending().tally();

    pipeline.recover_cancelled_graph_checkpoint(JOB);
    assert_eq!(
        pipeline.retiring_graph_jobs[&JOB].pending().tally(),
        taken,
        "no checkpoint is taken of the graph the job was held away from"
    );
    old_graph_heard(&mut pipeline).await;
    assert!(pipeline.held_for_full_disk(JOB));
}

// The root a held job waits on is pointed at another directory: the job
// keeps waiting on the disk its write failed on, read from then as a path
// of its own, and goes on when that disk shows room made.
#[tokio::test]
async fn a_root_pointed_elsewhere_leaves_the_held_job_waiting_on_the_disk_that_failed() {
    let room = Arc::new(AtomicU64::new(0));
    let body = payload(4 * ARTICLE, 163);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);

    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    pipeline
        .storage_capacity
        .retarget(StorageRoot::Intermediate, elsewhere);
    pipeline.disk_reading();
    assert!(pipeline.held_for_full_disk(JOB), "the job still waits");
    assert_eq!(
        pipeline.storage_capacity.waited_paths(),
        1,
        "the disk that failed is read as a path of its own"
    );
    assert!(
        !working(&pipeline).waited_on(),
        "the root pointed elsewhere is no longer waited on"
    );
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE),
        "its mark stands"
    );

    // The disk that failed reads no more room, then room made.
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert!(pipeline.held_for_full_disk(JOB));
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(
        !pipeline.held_for_full_disk(JOB),
        "room on the disk that failed lets it go"
    );
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
}

// Room freed between the failed write and the next look at the disk: the
// first reading taken since the failure shows more than the disk showed
// before it, and lets the job go.
#[tokio::test]
async fn room_made_before_the_first_reading_since_the_failure_lets_the_job_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 131);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();

    // The disk shows nothing over the reserve before the write fails.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE),
        "held, it has what its disk showed before the write failed to beat"
    );

    // Room is made before the disk is looked at again.
    fill_after(None);
    room.store(2 * PLENTY, Ordering::Release);
    pipeline.park_drained_jobs();
    old_graph_heard(&mut pipeline).await;
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A look at the disk that can be held open: armed, the next probe says it
// began and waits to be let on before it reads the room.
struct Gate {
    armed: std::sync::atomic::AtomicBool,
    began: std::sync::Mutex<std::sync::mpsc::Sender<()>>,
    go: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

// A probe already running as the write fails began before the failure: the
// high figure it publishes afterwards raises what the job waits to see
// beaten, and neither it nor a later reading of the same figure lets the
// job go.
#[tokio::test]
async fn a_probe_running_as_the_write_fails_does_not_let_the_job_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 137);
    let (began_tx, began_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let gate = Arc::new(Gate {
        armed: std::sync::atomic::AtomicBool::new(false),
        began: std::sync::Mutex::new(began_tx),
        go: std::sync::Mutex::new(go_rx),
    });
    let scenario = {
        let room = Arc::clone(&room);
        let gate = Arc::clone(&gate);
        Scenario::new(vec![Post::plain("ember.bin", body.clone())])
            .limits(super::caps::limits(|limits| {
                limits.min_free_bytes = RESERVE
            }))
            .setup(move |pipeline| {
                let roots = [
                    pipeline.script_data_dir.clone(),
                    pipeline.intermediate_dir.clone(),
                    pipeline.complete_dir.clone(),
                ];
                let room = Arc::clone(&room);
                let gate = Arc::clone(&gate);
                let probe = Arc::new(move |_: &Path| {
                    if gate.armed.swap(false, Ordering::AcqRel) {
                        gate.began.lock().unwrap().send(()).unwrap();
                        gate.go.lock().unwrap().recv().unwrap();
                    }
                    Ok(DiskSpace {
                        total_bytes: RESERVE * 8,
                        available_bytes: RESERVE + room.load(Ordering::Acquire),
                    })
                });
                let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
                for root in StorageRoot::ALL {
                    capacity.wait_for_probes(root, 1);
                }
                pipeline.storage_capacity = Arc::new(capacity);
            })
    };
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);

    // A probe begins and is held open; the write fails while it runs.
    gate.armed.store(true, Ordering::Release);
    pipeline
        .storage_capacity
        .request_refresh(StorageRoot::Intermediate);
    began_rx.recv().unwrap();
    let running = pipeline
        .storage_capacity
        .probes_begun(StorageRoot::Intermediate);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;

    // The probe held open reads room and publishes it; the look the wait
    // asked for follows it and reads the same.
    room.store(2 * PLENTY, Ordering::Release);
    go_tx.send(()).unwrap();
    pipeline
        .storage_capacity
        .wait_for_reading_begun_after(StorageRoot::Intermediate, running);
    pipeline.disk_reading();
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(RESERVE + 2 * PLENTY),
        "what the probe begun before the failure showed takes the mark's place"
    );

    // A byte over that mark is not the room of what was left of the file.
    let left = pipeline.full_disk_rise(JOB).0.expect("what was left");
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        2 * PLENTY + 1,
    );
    assert_full_disk_held(&pipeline);
    fill_after(None);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        2 * PLENTY + left,
    );
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// Twelve plain files, each of one article.
fn twelve() -> Vec<(String, Vec<u8>)> {
    (0..12u8)
        .map(|n| (format!("harbor{n:02}.bin"), payload(ARTICLE, 50 + n)))
        .collect()
}

fn destination_room(room: Option<u32>) {
    *crate::pipeline::completion::finalize::DESTINATION_ROOM
        .lock()
        .unwrap() = room;
}

// The job's final move found its destination full: it waits, not failed,
// shown waiting for disk space, with no move running.
fn assert_move_held(pipeline: &Pipeline) {
    assert!(pipeline.held_for_full_disk(JOB));
    assert!(
        !matches!(
            job_status_for_assert(pipeline, JOB),
            Some(JobStatus::Failed { .. })
        ),
        "the job is not failed"
    );
    assert!(pipeline.waits_for_disk_room());
    assert!(!pipeline.inflight_moves.contains(&JOB));
    assert_eq!(
        pipeline
            .dispatch_wait_reason(JOB, pipeline.login_refusal_view())
            .as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
}

// The job let go starts its move again at the top of the loop's next turn,
// and runs to its end: every file is delivered once, whole, into `under`,
// and nothing is left where it was worked on.
async fn move_goes_on(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    under: &Path,
    working_dir: &Path,
) {
    assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
    pipeline.resume_full_disk_moves().await;
    assert!(
        pipeline.inflight_moves.contains(&JOB),
        "its move goes on: {:?}",
        job_status_for_assert(pipeline, JOB)
    );
    drive(pipeline, scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let files = delivered(under);
    assert_eq!(files.len(), 12, "each file once: {:?}", files.keys());
    for (name, body) in twelve() {
        assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
    }
    assert!(!working_dir.exists(), "the sources are gone");
    assert!(!pipeline.waits_for_disk_room());
}

// A job of twelve files is moved to its destination, which fills after the
// fifth: the job waits for room, shown waiting for disk space, and is not
// failed. A part for it that finds no room in the spool meanwhile is
// dropped, and does not fail it either. A reading with no more room keeps
// it waiting. Once room is made the move goes on from what it left: the
// file cut short is moved again whole, and the job completes with its
// twelve files, each once, and its working directory gone.
#[tokio::test]
async fn a_final_move_whose_destination_fills_waits_for_room_and_delivers_every_file() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    // The working directory carries the job's ownership marker, as every
    // directory a job is added with does.
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, JOB)
        .await
        .unwrap();
    let output = Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    );

    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    // The device of the file that found no room was never read: the job
    // waits on that file's own disk, not on a root taken from a folder
    // above it.
    assert!(!output.waited_on());
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);
    let whole = delivered(&complete_dir)
        .iter()
        .filter(|(_, bytes)| bytes.len() == ARTICLE)
        .count();
    assert_eq!(whole, 5, "five files moved before the disk filled");
    assert!(working_dir.exists(), "the rest stays where it was");

    // A part of the job finds no room in the spool: nothing reads it now.
    pipeline.fail_job_for_disk_write(
        crate::pipeline::decode::SegmentWriteError::new(
            crate::jobs::ids::NzbFileId {
                job_id: JOB,
                file_index: 0,
            },
            std::io::Error::from(std::io::ErrorKind::StorageFull),
        ),
        "UU spool write failed",
    );
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The job's category delivers to a folder on a disk of its own, which
// fills during the move: the job waits on that folder's disk, which is read
// only while the job waits on it. Room on the complete folder's disk does
// not let it go; room on its own does.
#[tokio::test]
async fn a_final_move_to_a_folder_on_its_own_disk_waits_on_that_disk() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let library_room = Arc::new(AtomicU64::new(0));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (probe_room, probe_library) = (Arc::clone(&room), Arc::clone(&library_room));
    let (temp_dir, mut pipeline, _) =
        super::harness::admit_with(&scenario, move |pipeline, temp| {
            let library = temp.join("library-disk");
            std::fs::create_dir_all(&library).unwrap();
            crate::runtime::fs::test_devices::assign(&pipeline.intermediate_dir, 1);
            crate::runtime::fs::test_devices::assign(&pipeline.complete_dir, 2);
            crate::runtime::fs::test_devices::assign(&library, 3);
            pipeline.config.try_write().unwrap().categories =
                vec![crate::categories::CategoryConfig {
                    id: 0,
                    name: "library".to_string(),
                    dest_dir: Some(library.display().to_string()),
                    aliases: String::new(),
                    upload_cap: Default::default(),
                    seeding: None,
                    torrent_route: false,
                    unpack: None,
                }];
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let probe = Arc::new(move |path: &Path| {
                let room = if path.starts_with(&library) {
                    &probe_library
                } else {
                    &probe_room
                };
                Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: RESERVE + room.load(Ordering::Acquire),
                })
            });
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
        .await;
    let library = temp_dir.path().join("library-disk");
    pipeline.jobs.get_mut(&JOB).unwrap().spec.category = Some("library".to_string());
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    // The working directory carries the job's ownership marker, as every
    // directory a job is added with does.
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, JOB)
        .await
        .unwrap();
    let output = Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    );
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);

    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert_eq!(
        pipeline.storage_capacity.waited_paths(),
        1,
        "the folder's disk is read while the job waits on it"
    );
    assert!(
        !output.waited_on() && !working(&pipeline).waited_on(),
        "it waits on neither root's disk"
    );
    destination_room(None);

    // The folder's disk is read with the complete root: its first reading
    // since the failure is all the job has to compare with.
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_move_held(&pipeline);
    // The roots read more room still; the folder's disk reads none made.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 4 * PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    assert_move_held(&pipeline);

    library_room.store(2 * PLENTY, Ordering::Release);
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    assert_eq!(
        pipeline.storage_capacity.waited_paths(),
        0,
        "no one waits on the folder's disk, and it is read no more"
    );
    move_goes_on(&mut pipeline, &scenario, &library, &working_dir).await;
}

// The reserve of a job whose one file is larger than it.
const KEPT: u64 = 1;

// One file, on disks that keep `KEPT` free, whose destination has no room
// for it: the job as its final move starts, and the file's size.
async fn one_large_file(
    room: &Arc<AtomicU64>,
) -> (
    tempfile::TempDir,
    Pipeline,
    std::path::PathBuf,
    Scenario,
    Vec<u8>,
) {
    let body = payload(3 * ARTICLE, 71);
    let scenario = on_disk_keeping(room, vec![Post::plain("harbor.bin", body.clone())], KEPT);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    destination_room(Some(0));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.inflight_moves.contains(&JOB)
    })
    .await;
    assert!(pipeline.inflight_moves.contains(&JOB), "the move started");
    (temp_dir, pipeline, complete_dir, scenario, body)
}

// The one file is delivered once, whole, and the job completes.
async fn one_file_delivered(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    complete_dir: &Path,
    body: &[u8],
) {
    assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
    pipeline.resume_full_disk_moves().await;
    assert!(pipeline.inflight_moves.contains(&JOB), "its move goes on");
    drive(pipeline, scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let files = delivered(complete_dir);
    assert_eq!(files.len(), 1, "the file once: {:?}", files.keys());
    assert_eq!(file_named(&files, "harbor.bin"), body);
    assert!(!pipeline.waits_for_disk_room());
}

// A file larger than the room its destination has: the copy fills the
// disk, fails, and removes what it wrote, so the disk shows that room free
// again with none made. A reading taken while the copy was writing showed
// less. The job is held once, and the room the copy gave back does not let
// it go, though it is more than the disk showed before: only a reading that
// shows the room the file takes does. The file is then delivered once.
#[tokio::test]
async fn a_move_whose_copy_gave_its_room_back_waits_for_the_room_the_file_takes() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, scenario, body) = one_large_file(&room).await;
    let size = body.len() as u64;

    // The disk is read while the copy is part written.
    hears(&mut pipeline, &room, StorageRoot::Complete, size / 3);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert_eq!(
        pipeline.full_disk_needs(JOB),
        Some(size),
        "the move needs the room its file takes"
    );
    assert!(
        delivered(&complete_dir).is_empty(),
        "the copy removed what it wrote"
    );

    // The disk shows what the copy gave back: more than it showed during
    // the write, less than the file takes.
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * size / 3);
    assert_move_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Complete, size - KEPT - 1);
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, size);
    one_file_delivered(&mut pipeline, &scenario, &complete_dir, &body).await;
}

// The disk read before the copy failed showed room enough for the file.
// That reading, heard again once the job is held, does not let it go: only
// a reading whose probe began after the failure counts.
#[tokio::test]
async fn a_reading_from_before_a_move_failed_does_not_let_the_job_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, scenario, body) = one_large_file(&room).await;
    // The disk fills as the copy is written; nothing reads it yet.
    room.store(body.len() as u64 / 3, Ordering::Release);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(pipeline.full_disk_needs(JOB).is_some());

    // The newest reading is the one from before the failure, or one taken
    // since that shows the disk as it is: neither lets the job go.
    pipeline.disk_reading();
    assert_move_held(&pipeline);

    destination_room(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    one_file_delivered(&mut pipeline, &scenario, &complete_dir, &body).await;
}

// Twelve files whose move is held after the fifth: the job, where it works,
// and where it delivers. `stamped` gives the working directory its
// ownership marker.
async fn held_after_five(
    room: &Arc<AtomicU64>,
    stamped: bool,
) -> (
    tempfile::TempDir,
    Pipeline,
    std::path::PathBuf,
    std::path::PathBuf,
    Scenario,
) {
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(room, posts);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    if stamped {
        crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, JOB)
            .await
            .unwrap();
    }
    destination_room(Some(5));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    destination_room(None);
    // The disk reads no room over the reserve.
    hears(&mut pipeline, room, StorageRoot::Complete, 0);
    assert_move_held(&pipeline);
    (temp_dir, pipeline, complete_dir, working_dir, scenario)
}

// The temp folder is changed while a job is held in its final move: the
// job's working directory stays where it is, and once room is made the move
// goes on from it and delivers every file, none left behind.
#[tokio::test]
async fn a_temp_folder_changed_during_a_held_move_loses_no_file() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;

    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    pipeline
        .storage_capacity
        .retarget(StorageRoot::Intermediate, elsewhere.clone());
    pipeline.intermediate_dir = elsewhere;
    pipeline.disk_reading();
    assert_move_held(&pipeline);
    assert!(working_dir.exists(), "the working directory stays");
    assert_eq!(pipeline.jobs[&JOB].working_dir, working_dir);

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The folder a held move delivers into is removed during the hold, as a
// user making room does, the five files already moved with it: once room
// shows, the move goes on, makes the folder again, and delivers each of the
// seven files still where the job worked, once and whole.
#[tokio::test]
async fn a_destination_removed_during_a_held_move_is_made_again() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    let moved: Vec<String> = delivered(&complete_dir)
        .iter()
        .filter(|(_, bytes)| bytes.len() == ARTICLE)
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(moved.len(), 5);
    for entry in std::fs::read_dir(&complete_dir).unwrap() {
        std::fs::remove_dir_all(entry.unwrap().path()).unwrap();
    }
    assert!(delivered(&complete_dir).is_empty());

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
    pipeline.resume_full_disk_moves().await;
    assert!(pipeline.inflight_moves.contains(&JOB), "its move goes on");
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    let files = delivered(&complete_dir);
    assert_eq!(files.len(), 7, "each file left, once: {:?}", files.keys());
    for (name, body) in twelve() {
        if !moved.iter().any(|gone| gone.ends_with(&name)) {
            assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
        }
    }
    assert!(!working_dir.exists(), "the sources are gone");
}

// A held move whose working directory carries no ownership marker goes on
// all the same: the job knows its own directory, and nothing looks for it.
#[tokio::test]
async fn a_held_move_whose_working_directory_has_no_marker_goes_on() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, false).await;
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// The folder a job delivers into cannot be made for want of room: the job
// is held, not failed, waiting on that disk. Once room is made the move
// claims its folder and delivers every file.
#[tokio::test]
async fn a_destination_that_cannot_be_made_for_want_of_room_holds_the_job() {
    use crate::pipeline::completion::finalize::CLAIM_FULL;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    let output = Arc::clone(
        pipeline
            .storage_capacity
            .reader(StorageRoot::Complete)
            .ledger(),
    );

    CLAIM_FULL.store(true, Ordering::Release);
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(output.waited_on(), "it waits on the destination's disk");
    assert_eq!(pipeline.full_disk_needs(JOB), None);
    // The job's completion is looked at again while it waits: no move
    // starts.
    pipeline.check_job_completion(JOB).await;
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert_move_held(&pipeline);

    CLAIM_FULL.store(false, Ordering::Release);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!output.waited_on());
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A path whose disk was never read is waited on as itself, with no look at
// its device as the job is held. So is a path below a root's folder whose
// own device was never read: the folder above it may lie on another disk.
// Only the root's own folder, whose device is known, is that root's disk.
#[tokio::test]
async fn a_failing_path_on_a_disk_not_known_is_waited_on_as_itself() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk(&room);
    let (temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let capacity = Arc::clone(&pipeline.storage_capacity);
    let unseen = temp_dir.path().join("never-read").join("folder");
    crate::runtime::fs::test_devices::assign(&pipeline.intermediate_dir, 1);
    crate::runtime::fs::test_devices::assign(&pipeline.complete_dir, 2);
    for root in StorageRoot::ALL {
        let begun = capacity.probes_begun(root);
        capacity.request_refresh(root);
        capacity.wait_for_reading_begun_after(root, begun);
    }
    assert_eq!(
        capacity.known_root_of(&complete_dir),
        Some(StorageRoot::Complete)
    );
    assert_eq!(
        capacity.known_root_of(&pipeline.intermediate_dir),
        Some(StorageRoot::Intermediate)
    );
    let below = complete_dir.join("ember").join("a.bin");
    assert_eq!(capacity.known_root_of(&below), None);
    assert_eq!(
        capacity.known_root_of(&pipeline.intermediate_dir.join("ember")),
        None
    );
    assert_eq!(capacity.known_root_of(&unseen), None);

    let asks = crate::operations::disk::same_filesystem_asks();
    pipeline.hold_move_for_full_disk(
        JOB,
        crate::pipeline::full_disk::Stage::Restored,
        &below,
        "no room",
        None,
    );
    assert!(
        pipeline.full_disk_on_path(JOB),
        "a path below a known folder is waited on as itself"
    );
    assert_eq!(capacity.waited_paths(), 1);
    assert!(pipeline.drop_full_disk_hold(JOB).is_some());
    assert_eq!(capacity.waited_paths(), 0);
    pipeline.hold_move_for_full_disk(
        JOB,
        crate::pipeline::full_disk::Stage::Restored,
        &unseen,
        "no room",
        None,
    );
    assert!(pipeline.full_disk_on_path(JOB), "the path is waited on");
    assert_eq!(capacity.waited_paths(), 1);
    assert_eq!(
        crate::operations::disk::same_filesystem_asks(),
        asks,
        "no device was asked after"
    );
}

// The cleanup of a cancelled job has run.
async fn cleaned_up(pipeline: &Pipeline) {
    while pipeline
        .shared_state
        .queue
        .read(|inner| inner.pending_cleanup.contains(&JOB))
    {
        tokio::task::yield_now().await;
    }
}

// A job held in its final move cannot be paused, and can be cancelled, with
// its history deleted or kept: no hold, no wait on any disk, no working
// directory and no part-filled destination are left.
#[tokio::test]
async fn a_job_held_in_its_final_move_is_cancelled_and_deleted_clean() {
    for delete in [false, true] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (_temp_dir, mut pipeline, complete_dir, working_dir, _scenario) =
            held_after_five(&room, true).await;
        let output = Arc::clone(
            pipeline
                .storage_capacity
                .reader(StorageRoot::Complete)
                .ledger(),
        );
        assert!(std::fs::read_dir(&complete_dir).unwrap().next().is_some());

        let handle = handle(&pipeline);
        assert!(
            handle.pause_job(JOB).await.is_err(),
            "a job held in its move is not paused"
        );
        if delete {
            handle.cancel_job_and_delete_history(JOB).await.unwrap();
        } else {
            handle.cancel_job(JOB).await.unwrap();
        }
        pipeline.apply_queue_intents().await;
        super::super::finish_cancel_jobs(&mut pipeline).await;
        assert!(!pipeline.held_for_full_disk(JOB), "no hold is left");
        assert!(!pipeline.waits_for_disk_room());
        assert!(!output.waited_on());
        assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
        assert!(!pipeline.reserved_complete_destinations.contains_key(&JOB));
        cleaned_up(&pipeline).await;
        assert!(!working_dir.exists(), "the working directory is gone");
        assert!(
            std::fs::read_dir(&complete_dir).unwrap().next().is_none(),
            "the part-filled destination is gone"
        );
    }
}

// Post-processing is paused while a job is held in its final move: room
// made does not start the move, as it starts no first move. The job goes on
// when post-processing resumes.
#[tokio::test]
async fn a_held_move_let_go_waits_while_post_processing_is_paused() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    let handle = handle(&pipeline);
    handle.pause_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert_move_held(&pipeline);
    pipeline.resume_full_disk_moves().await;
    assert!(!pipeline.inflight_moves.contains(&JOB), "no move starts");

    handle.resume_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A name the destination and the working directory both hold when a held
// move goes on: the destination keeps what it holds, as on a first move,
// and every other file is delivered.
#[tokio::test]
async fn a_held_move_keeps_a_name_the_destination_already_holds() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    // A name no copy was cut short on: the names that were are moved again.
    let waiting = "harbor-extra.bin";
    assert!(
        pipeline
            .full_disk_cut_short(JOB)
            .iter()
            .all(|path| !path.ends_with(waiting))
    );
    std::fs::write(working_dir.join(waiting), b"from the working directory").unwrap();
    let folder = std::fs::read_dir(&complete_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::write(folder.join(waiting), b"already here").unwrap();

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
    pipeline.resume_full_disk_moves().await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    let files = delivered(&complete_dir);
    assert_eq!(
        std::fs::read(folder.join(waiting)).unwrap(),
        b"already here",
        "{:?}",
        files.keys()
    );
    for (name, body) in twelve() {
        assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
    }
    assert!(
        !matches!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Failed { .. })
        ),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
}

// The copy that found no room left what it wrote in the destination. The
// hold records that name, and the move let go replaces it from the working
// directory: the file is whole, and there once.
#[tokio::test]
async fn a_copy_cut_short_that_stays_in_the_destination_is_delivered_whole() {
    use crate::pipeline::completion::finalize::PARTIAL_STAYS;
    let room = Arc::new(AtomicU64::new(PLENTY));
    PARTIAL_STAYS.store(true, Ordering::Release);
    let (_temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
        held_after_five(&room, true).await;
    PARTIAL_STAYS.store(false, Ordering::Release);
    // Every file past the fifth found no room.
    let cut_short = pipeline.full_disk_cut_short(JOB);
    assert_eq!(cut_short.len(), 7, "{cut_short:?}");
    for partial in &cut_short {
        assert_eq!(
            std::fs::metadata(partial).unwrap().len(),
            ARTICLE as u64 / 2,
            "half of the file stands in the destination"
        );
        assert!(
            working_dir.join(partial.file_name().unwrap()).exists(),
            "its source stays"
        );
    }

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A second job beside `JOB`, with one small file.
const SECOND: JobId = JobId(93_012);

async fn add_second(pipeline: &mut Pipeline) {
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

// Two jobs wait on one disk, each for 60 of the 100 a reading then shows.
// One goes, and its 60 stay counted against the disk: the other waits, on
// that reading and on any like it. When the first one's step has ended,
// here by its being held again, the other is judged on a reading taken
// after that, never on the one from before, and goes when that shows room.
#[tokio::test]
async fn room_promised_to_one_held_job_is_not_promised_to_the_next() {
    use crate::pipeline::full_disk::Stage;
    const UNIT: u64 = 1 << 20;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
        KEPT,
    );
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    for job in [JOB, SECOND] {
        pipeline.hold_move_for_full_disk(
            job,
            Stage::Claim(Default::default()),
            &complete_dir,
            "no room",
            Some(60 * UNIT),
        );
        assert!(pipeline.held_for_full_disk(job));
        assert!(!pipeline.full_disk_on_path(job), "both wait on the root");
    }

    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    let held = [JOB, SECOND].map(|job| pipeline.held_for_full_disk(job));
    assert_eq!(
        held.iter().filter(|held| **held).count(),
        1,
        "one goes and one waits: {held:?}"
    );
    let (went, waits) = if held[0] {
        (SECOND, JOB)
    } else {
        (JOB, SECOND)
    };
    assert_eq!(pipeline.full_disk_promised_bytes(), 60 * UNIT);
    // The same room read again is still promised.
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(pipeline.held_for_full_disk(waits));
    hears(&mut pipeline, &room, StorageRoot::Complete, 119 * UNIT);
    assert!(pipeline.held_for_full_disk(waits));

    // The first one's step ends: it found the disk full again, and waits
    // with no figure of its own. Its step filled the disk.
    room.store(59 * UNIT, Ordering::Release);
    pipeline.hold_move_for_full_disk(
        went,
        Stage::Claim(Default::default()),
        &complete_dir,
        "no room",
        None,
    );
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    // The reading from before that step ended showed room the step has
    // filled since: heard again, it lets no one go, and neither does a
    // reading taken since.
    pipeline.disk_reading();
    assert!(pipeline.held_for_full_disk(waits));
    assert!(pipeline.held_for_full_disk(went));

    // A new reading that shows too little keeps it; one that shows the room
    // lets it go, and that room is promised to it.
    hears(&mut pipeline, &room, StorageRoot::Complete, 59 * UNIT);
    assert!(pipeline.held_for_full_disk(waits));
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(!pipeline.held_for_full_disk(waits), "the other goes");
    assert!(pipeline.held_for_full_disk(went));
    assert_eq!(pipeline.full_disk_promised_bytes(), 60 * UNIT);
}

// A job held before it claimed its destination is let go. A cancel taken
// before its move begins wins: no move starts, and the job is cancelled
// clean. Once its move has begun, in the one step before anything is
// awaited, a cancel is refused as for any moving job. Never both.
#[tokio::test]
async fn a_cancel_and_a_move_let_go_never_both_win() {
    use crate::pipeline::completion::finalize::CLAIM_FULL;
    for cancel_first in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let posts = twelve()
            .into_iter()
            .map(|(name, body)| Post::plain(&name, body))
            .collect();
        let scenario = on_disk_posting(&room, posts);
        let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
        let working_dir = pipeline.jobs[&JOB].working_dir.clone();
        CLAIM_FULL.store(true, Ordering::Release);
        drive(&mut pipeline, &scenario, JOB, |pipeline| {
            pipeline.held_for_full_disk(JOB)
        })
        .await;
        assert_move_held(&pipeline);
        CLAIM_FULL.store(false, Ordering::Release);
        hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
        hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
        assert!(!pipeline.held_for_full_disk(JOB), "the job was let go");
        let handle = handle(&pipeline);

        if cancel_first {
            handle.cancel_job(JOB).await.unwrap();
            assert!(
                !pipeline.begin_let_go_move(JOB),
                "the cancel stops the move"
            );
            pipeline.resume_full_disk_moves().await;
            assert!(!pipeline.inflight_moves.contains(&JOB), "no move starts");
            assert!(!pipeline.reserved_complete_destinations.contains_key(&JOB));
            pipeline.apply_queue_intents().await;
            super::super::finish_cancel_jobs(&mut pipeline).await;
            cleaned_up(&pipeline).await;
            assert!(!working_dir.exists(), "the working directory is gone");
            assert!(delivered(&complete_dir).is_empty(), "nothing was delivered");
        } else {
            assert!(pipeline.begin_let_go_move(JOB), "the move begins");
            assert!(
                handle.cancel_job(JOB).await.is_err(),
                "a job whose move has begun is not cancelled"
            );
            // The move goes on as begun.
            move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
        }
    }
}

// A job held after part of its files were moved; the temp folder is then
// pointed elsewhere, and weaver starts again: the job comes back from its
// moving row, which names the destination and where the move comes from.
// With or without an ownership marker on the working directory, the move is
// not taken for finished: it goes on from the recorded source, and every
// file is delivered.
#[tokio::test]
async fn a_restart_during_a_held_move_delivers_every_file_from_the_recorded_source() {
    for stamped in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (temp_dir, mut pipeline, complete_dir, working_dir, scenario) =
            held_after_five(&room, stamped).await;
        assert_eq!(
            pipeline.jobs[&JOB].delivery_source.as_deref(),
            Some(working_dir.as_path()),
            "the move's source is kept with the job"
        );
        let facts = pipeline.fresh_post_processing_facts(
            JOB,
            crate::post_processing::model::PipelineOutcome::Succeeded,
        );
        assert_eq!(facts.source.as_deref(), Some(working_dir.as_path()));
        let stored: crate::post_processing::model::PostProcessingFacts =
            serde_json::from_str(&serde_json::to_string(&facts).unwrap()).unwrap();
        assert_eq!(
            stored.source.as_deref(),
            Some(working_dir.as_path()),
            "the moving row carries it"
        );
        let dest = pipeline.full_disk_cut_short(JOB)[0]
            .parent()
            .unwrap()
            .to_path_buf();

        let elsewhere = temp_dir.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        pipeline
            .storage_capacity
            .retarget(StorageRoot::Intermediate, elsewhere.clone());
        pipeline.intermediate_dir = elsewhere;

        // The job as a restart restores it from its moving row: no hold, no
        // destination reserved, the row's directory as where it stands, and
        // the source the row recorded.
        assert!(pipeline.drop_full_disk_hold(JOB).is_some());
        pipeline.reserved_complete_destinations.remove(&JOB);
        {
            let state = pipeline.jobs.get_mut(&JOB).unwrap();
            state.working_dir = dest;
            state.delivery_source = stored.source;
        }
        pipeline.resume_restored_move(JOB).await;
        assert!(
            pipeline.inflight_moves.contains(&JOB),
            "the move goes on: {:?}",
            job_status_for_assert(&pipeline, JOB)
        );
        drive(&mut pipeline, &scenario, JOB, |_| false).await;
        assert_eq!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Complete)
        );
        let files = delivered(&complete_dir);
        assert_eq!(files.len(), 12, "each file once: {:?}", files.keys());
        for (name, body) in twelve() {
            assert_eq!(file_named(&files, &name), body.as_slice(), "{name}");
        }
        assert!(!working_dir.exists(), "the sources are gone");
    }
}

// A plain file of four articles whose write finds the disk full after one
// and a half, the job held to keep `reserve` free: the job is held, and
// what was left of the file is what a reading has to show over the mark.
async fn held_with_a_file_part_written(
    room: &Arc<AtomicU64>,
    reserve: u64,
    seed: u8,
) -> (
    tempfile::TempDir,
    Pipeline,
    Scenario,
    std::path::PathBuf,
    Vec<u8>,
    u64,
) {
    let body = payload(4 * ARTICLE, seed);
    let scenario = on_disk_keeping(room, vec![Post::plain("ember.bin", body.clone())], reserve);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = pipeline.complete_dir.clone();
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    let (left, _) = pipeline.full_disk_rise(JOB);
    let left = left.expect("the write that failed knew what was left of its file");
    assert!(
        (ARTICLE as u64..=4 * ARTICLE as u64).contains(&left),
        "{left} bytes are left of a file of four articles, one written"
    );
    (temp_dir, pipeline, scenario, complete_dir, body, left)
}

// With nothing kept free, a few bytes freed on a full disk do not let a
// held download go: it waits for the room of what was left of the file it
// was writing, over what the disk showed as the write failed.
#[tokio::test]
async fn a_held_download_waits_for_the_room_of_what_was_left_of_its_file() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, scenario, complete_dir, body, left) =
        held_with_a_file_part_written(&room, 0, 151).await;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(PLENTY));
    assert_eq!(pipeline.full_disk_rise(JOB), (Some(left), false));

    // Another program removed a small file, a few bytes where the file
    // has more than an article left: that much more shows.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 4);
    assert_full_disk_held(&pipeline);
    // One byte short of what was left.
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left - 1,
    );
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.full_disk_mark(JOB),
        Some(PLENTY),
        "the mark stands"
    );

    // The room of what was left shows: the job goes on, and what it was
    // let go on is promised to it.
    fill_after(None);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left,
    );
    assert_eq!(pipeline.full_disk_promised_bytes(), left);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// With free space kept, a held download goes on only when both hold: the
// disk shows the room of what was left over the mark, and more than the
// reserve is free to spend.
#[tokio::test]
async fn a_held_download_with_a_reserve_waits_for_the_rise_and_the_reserve() {
    let room = Arc::new(AtomicU64::new(0));
    let (_temp_dir, mut pipeline, scenario, complete_dir, body, left) =
        held_with_a_file_part_written(&room, RESERVE, 157).await;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE));

    // More than the reserve is free, the rise is short by a byte.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, left - 1);
    assert_full_disk_held(&pipeline);
    // The rise shows, and all of it is promised to another job: no more
    // than the reserve is free to spend.
    working(&pipeline).promise(left);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, left);
    assert_full_disk_held(&pipeline);

    // Both hold.
    working(&pipeline).release(left);
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, left);
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A job a quota holds when the pipeline goes down: the next pipeline, made
// from the database's rows over the same folders, restores it free, the
// quota refuses its first write, and it is held again as a quota hold on a
// mark of this run. With the quota lifted and the figure risen it completes.
#[tokio::test]
async fn a_job_a_quota_holds_at_a_restart_is_restored_and_held_by_the_quota_again() {
    use crate::pipeline::graph::leaf::counting::refuse_by_quota;
    let room = Arc::new(AtomicU64::new(PLENTY));
    refuse_by_quota(true);
    let (temp_dir, mut pipeline, scenario, _complete_dir, _body, left) =
        held_with_a_file_part_written(&room, RESERVE, 163).await;
    assert_eq!(pipeline.full_disk_rise(JOB), (Some(left), true));

    pipeline.drain().await;
    fill_after(Some(0));
    let (mut pipeline, _) =
        super::restart::restart_as_it_stands(&temp_dir, pipeline, &scenario, &mut |_| {}).await;
    assert!(
        !pipeline.waits_for_disk_room(),
        "a restored job is not held"
    );
    until_full(&mut pipeline, &scenario).await;
    let (again, quota) = pipeline.full_disk_rise(JOB);
    assert!(quota, "the restored job's hold is a quota hold");
    let again = again.expect("the write that failed knew what was left");
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    let mark = pipeline
        .full_disk_mark(JOB)
        .expect("a quota hold has a mark");

    fill_after(None);
    refuse_by_quota(false);
    room.store(mark - RESERVE + again, Ordering::Release);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        mark - RESERVE + again,
    );
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
}

// A write a quota refused, on a disk that shows plenty free: the job is
// held, and no reading of that plenty lets it go. It goes when the figure
// has risen from the mark by what was left of its file. Let go and refused
// by the quota again, as a job restored after a restart is at its first
// write, it is held on a mark of its own and not let go at once.
#[tokio::test]
async fn a_download_a_quota_refused_waits_for_the_figure_to_rise_by_what_was_left() {
    use crate::pipeline::graph::leaf::counting::refuse_by_quota;
    let room = Arc::new(AtomicU64::new(PLENTY));
    refuse_by_quota(true);
    let (_temp_dir, mut pipeline, scenario, complete_dir, body, left) =
        held_with_a_file_part_written(&room, RESERVE, 163).await;
    let mark = RESERVE + PLENTY;
    assert_eq!(pipeline.full_disk_mark(JOB), Some(mark));
    assert_eq!(pipeline.full_disk_rise(JOB), (Some(left), true));

    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 1);
    assert_full_disk_held(&pipeline);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left - 1,
    );
    assert_full_disk_held(&pipeline);

    // The figure rose by what was left: it goes, and the quota refuses it
    // again at once.
    fill_after(Some(0));
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left,
    );
    restarts(&mut pipeline).await;
    until_full(&mut pipeline, &scenario).await;
    let (again, quota) = pipeline.full_disk_rise(JOB);
    assert!(quota, "the new hold is a quota hold");
    let again = again.expect("the write that failed knew what was left");
    assert_eq!(pipeline.full_disk_mark(JOB), Some(mark + left));
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left,
    );
    assert_full_disk_held(&pipeline);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left + again - 1,
    );
    assert_full_disk_held(&pipeline);

    fill_after(None);
    refuse_by_quota(false);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left + again,
    );
    restarts(&mut pipeline).await;
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert_eq!(
        job_status_for_assert(&pipeline, JOB),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A step that names what it needs, refused by a quota: the disk showed that
// much free already, so the job goes when the figure has risen by it from
// the mark, not at the mark and not a byte over it. The same step refused by
// the disk goes as soon as the disk shows what it needs.
#[tokio::test]
async fn a_step_a_quota_refused_waits_for_the_figure_to_rise_by_what_it_needs() {
    use crate::pipeline::full_disk::Wants;
    // Fifteen units of a file, on a disk that shows far more free.
    const NEEDS: u64 = 15 << 20;
    for quota in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (_temp_dir, mut pipeline, scenario, complete_dir, body, _) =
            held_with_a_file_part_written(&room, RESERVE, 167).await;
        // The same job, held as a step that needs `NEEDS` is held.
        pipeline.drop_full_disk_hold(JOB);
        // A reading taken while the step's own output was on disk shows
        // five units less: it is the newest as the step fails, and is not
        // what a quota hold measures the rise from.
        if quota {
            hears(
                &mut pipeline,
                &room,
                StorageRoot::Intermediate,
                PLENTY - (5 << 20),
            );
            // The step gives its output back as it fails.
            room.store(PLENTY, Ordering::Release);
        }
        pipeline.hold_job_for_full_disk(
            JOB,
            StorageRoot::Intermediate,
            Wants {
                needs: Some(NEEDS),
                left: None,
                quota,
            },
        );
        pipeline.park_drained_jobs();
        old_graph_heard(&mut pipeline).await;

        fill_after(None);
        // The disk shows what it showed: a step the disk refused goes on
        // that, one a quota refused does not.
        if quota {
            assert_ne!(
                pipeline.full_disk_mark(JOB),
                Some(RESERVE + PLENTY - (5 << 20)),
                "the mark is not the reading from before the step gave its output back"
            );
        }
        hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
        if quota {
            assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE + PLENTY));
            assert_full_disk_held(&pipeline);
            hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 1);
            assert_full_disk_held(&pipeline);
            hears(
                &mut pipeline,
                &room,
                StorageRoot::Intermediate,
                PLENTY + NEEDS - 1,
            );
            assert_full_disk_held(&pipeline);
            hears(
                &mut pipeline,
                &room,
                StorageRoot::Intermediate,
                PLENTY + NEEDS,
            );
        }
        assert_eq!(
            pipeline.full_disk_promised_bytes(),
            NEEDS,
            "the room it goes on is promised to it"
        );
        restarts(&mut pipeline).await;
        drive(&mut pipeline, &scenario, JOB, |_| false).await;
        assert_eq!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Complete)
        );
        assert_eq!(
            file_named(&delivered(&complete_dir), "ember.bin"),
            body.as_slice()
        );
    }
}

// The row that says a move began is refused for want of room on the
// database's disk: nothing moves, the job is held on the data folder's
// disk and shown waiting for disk space, not failed. A reading that shows
// no more keeps it waiting. Once more shows, the row is written, once, and
// the move delivers every file.
#[tokio::test]
async fn a_moving_row_the_database_disk_has_no_room_for_holds_the_job_and_is_written_again() {
    use crate::pipeline::completion::finalize::{MOVING_ROW_REFUSED, MOVING_ROWS_WRITTEN};
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, JOB)
        .await
        .unwrap();

    MOVING_ROW_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    // The harness keeps the data folder on the complete root's disk, so
    // the hold reads that root.
    assert_eq!(MOVING_ROWS_WRITTEN.load(Ordering::Relaxed), 0);
    assert!(
        delivered(&complete_dir).is_empty(),
        "nothing moved before the row was written"
    );
    assert!(working_dir.exists());

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert_move_held(&pipeline);

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
    assert_eq!(
        MOVING_ROWS_WRITTEN.load(Ordering::Relaxed),
        1,
        "the moving row was written once"
    );
}

// The database's folder is on a disk of its own, apart from the data
// folder's: a job whose moving row found no room waits on the database's
// disk. More room on every root, the data folder's among them, does not let
// it go; more on the database's disk does.
#[tokio::test]
async fn a_moving_row_refused_waits_on_the_database_folder_not_the_data_folder() {
    use crate::pipeline::completion::finalize::MOVING_ROW_REFUSED;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let database_room = Arc::new(AtomicU64::new(0));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (probe_room, probe_database) = (Arc::clone(&room), Arc::clone(&database_room));
    let (temp_dir, mut pipeline, complete_dir) =
        super::harness::admit_with(&scenario, move |pipeline, temp| {
            let database = temp.to_path_buf();
            let roots = [
                pipeline.script_data_dir.clone(),
                pipeline.intermediate_dir.clone(),
                pipeline.complete_dir.clone(),
            ];
            let probe = Arc::new(move |path: &Path| {
                let room = if path == database {
                    &probe_database
                } else {
                    &probe_room
                };
                Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: RESERVE + room.load(Ordering::Acquire),
                })
            });
            let capacity = StorageCapacity::with_probe(roots, NEVER, probe);
            for root in StorageRoot::ALL {
                capacity.wait_for_probes(root, 1);
            }
            pipeline.storage_capacity = Arc::new(capacity);
        })
        .await;
    assert_eq!(pipeline.database_dir(), temp_dir.path());
    assert_ne!(pipeline.database_dir(), pipeline.script_data_dir);
    let working_dir = pipeline.jobs[&JOB].working_dir.clone();
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, JOB)
        .await
        .unwrap();

    MOVING_ROW_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    drive(&mut pipeline, &scenario, JOB, |pipeline| {
        pipeline.held_for_full_disk(JOB)
    })
    .await;
    assert_move_held(&pipeline);
    assert!(pipeline.full_disk_on_path(JOB), "it waits on a path");
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);

    // Every root shows more, the data folder's with them: the database's
    // disk shows none made.
    for root in StorageRoot::ALL {
        hears(&mut pipeline, &room, root, 4 * PLENTY);
    }
    hears(&mut pipeline, &room, StorageRoot::Complete, 8 * PLENTY);
    assert_move_held(&pipeline);

    database_room.store(4 * PLENTY, Ordering::Release);
    hears(&mut pipeline, &room, StorageRoot::Complete, 8 * PLENTY);
    move_goes_on(&mut pipeline, &scenario, &complete_dir, &working_dir).await;
}

// A moving row whose write fails for any other reason fails the job, as
// before.
#[tokio::test]
async fn a_moving_row_refused_for_another_reason_fails_the_job() {
    use crate::pipeline::completion::finalize::MOVING_ROW_REFUSED;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let posts = twelve()
        .into_iter()
        .map(|(name, body)| Post::plain(&name, body))
        .collect();
    let scenario = on_disk_posting(&room, posts);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    MOVING_ROW_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::Database("the row is malformed".into()));
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    assert!(!pipeline.held_for_full_disk(JOB));
    assert!(
        matches!(
            job_status_for_assert(&pipeline, JOB),
            Some(JobStatus::Failed { .. })
        ),
        "{:?}",
        job_status_for_assert(&pipeline, JOB)
    );
    assert!(delivered(&complete_dir).is_empty());
}

// No hold is let go by less than the write that was refused. A spool write
// that found no room waits for the free figure to rise by the part it could
// not write. A site with no figure at all, here refused by a quota on a
// disk that shows plenty free, waits for it to rise by the reserve: a few
// bytes freed let nothing go.
#[tokio::test]
async fn a_hold_with_no_figure_is_not_let_go_by_a_few_bytes() {
    use crate::jobs::ids::NzbFileId;
    use crate::pipeline::decode::SegmentWriteError;
    use crate::pipeline::full_disk::Wants;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let file_id = NzbFileId {
        job_id: JOB,
        file_index: 0,
    };
    for (kind, quota) in [
        (std::io::ErrorKind::StorageFull, false),
        (std::io::ErrorKind::QuotaExceeded, true),
    ] {
        pipeline.fail_job_for_disk_write(
            SegmentWriteError::new(file_id, std::io::Error::from(kind)).of_bytes(4096),
            "spooling a part",
        );
        assert_eq!(pipeline.full_disk_rise(JOB), (Some(4096), quota));
        assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE + PLENTY));
        hears(
            &mut pipeline,
            &room,
            StorageRoot::Intermediate,
            PLENTY + 4095,
        );
        assert_eq!(pipeline.full_disk_promised_bytes(), 0);
        hears(
            &mut pipeline,
            &room,
            StorageRoot::Intermediate,
            PLENTY + 4096,
        );
        assert_eq!(pipeline.full_disk_promised_bytes(), 4096);
        pipeline.end_full_disk_promise(JOB);
        assert!(pipeline.drop_full_disk_hold(JOB).is_some());
        hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    }

    pipeline.hold_job_for_full_disk(
        JOB,
        StorageRoot::Intermediate,
        Wants {
            quota: true,
            ..Wants::default()
        },
    );
    assert_eq!(pipeline.full_disk_rise(JOB), (Some(RESERVE), true));
    assert_eq!(pipeline.full_disk_mark(JOB), Some(RESERVE + PLENTY));
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + 4096,
    );
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + RESERVE - 1,
    );
    assert_eq!(pipeline.full_disk_promised_bytes(), 0);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + RESERVE,
    );
    assert_eq!(pipeline.full_disk_promised_bytes(), RESERVE);
}

// A job held on a root and a job held on a path below it, on the same
// device: each waits for 60 of the 100 a reading then shows. One goes, and
// its 60 count against the other, though one reads the root and the other
// the path.
#[tokio::test]
async fn a_job_held_on_a_root_and_one_on_a_path_of_its_disk_are_promised_the_same_room_once() {
    use crate::pipeline::full_disk::Stage;
    const UNIT: u64 = 1 << 20;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
        KEPT,
    );
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    let below = complete_dir.join("ember");
    crate::runtime::fs::test_devices::assign(&complete_dir, 7);
    crate::runtime::fs::test_devices::assign(&below, 7);
    let capacity = Arc::clone(&pipeline.storage_capacity);
    for root in StorageRoot::ALL {
        let begun = capacity.probes_begun(root);
        capacity.request_refresh(root);
        capacity.wait_for_reading_begun_after(root, begun);
    }
    pipeline.hold_move_for_full_disk(
        JOB,
        Stage::Claim(Default::default()),
        &complete_dir,
        "no room",
        Some(60 * UNIT),
    );
    pipeline.hold_move_for_full_disk(
        SECOND,
        Stage::Claim(Default::default()),
        &below,
        "no room",
        Some(60 * UNIT),
    );
    assert!(!pipeline.full_disk_on_path(JOB), "one waits on the root");
    assert!(pipeline.full_disk_on_path(SECOND), "one on the path");

    for _ in 0..2 {
        hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
        let held = [JOB, SECOND].map(|job| pipeline.held_for_full_disk(job));
        assert_eq!(
            held.iter().filter(|held| **held).count(),
            1,
            "one goes and one waits: {held:?}"
        );
        assert_eq!(pipeline.full_disk_promised_bytes(), 60 * UNIT);
    }
    hears(&mut pipeline, &room, StorageRoot::Complete, 120 * UNIT);
    assert!(!pipeline.held_for_full_disk(JOB) && !pipeline.held_for_full_disk(SECOND));
    assert_eq!(pipeline.full_disk_promised_bytes(), 120 * UNIT);
}

// The user's resume lets a held job go, whatever the readings say: one
// that was paused while held goes at that one resume, and one a quota
// holds, not paused, goes too.
#[tokio::test]
async fn one_resume_lets_a_held_job_go_paused_or_not() {
    use crate::pipeline::full_disk::{Stage, Wants};
    const UNIT: u64 = 1 << 20;
    for paused in [true, false] {
        let room = Arc::new(AtomicU64::new(0));
        let scenario = on_disk_keeping(
            &room,
            vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
            KEPT,
        );
        let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
        pipeline.hold_move_for_full_disk(
            JOB,
            Stage::Claim(Default::default()),
            &complete_dir,
            "no room",
            Wants {
                needs: Some(60 * UNIT),
                left: None,
                quota: true,
            },
        );
        hears(&mut pipeline, &room, StorageRoot::Complete, 0);
        assert!(pipeline.held_for_full_disk(JOB), "paused: {paused}");
        if paused {
            pipeline.jobs.get_mut(&JOB).unwrap().run_state = RunState::Paused;
            pipeline.wake_full_disk();
            assert!(pipeline.held_for_full_disk(JOB));
        }
        pipeline.resume_job_runtime(JOB).unwrap();
        assert!(
            !pipeline.held_for_full_disk(JOB),
            "one resume lets it go (paused: {paused})"
        );
    }
}

// A held job its user paused takes no promise: the room a reading shows
// goes to the job beside it, which is not judged against room the paused
// one is not taking.
#[tokio::test]
async fn a_paused_held_job_is_promised_nothing() {
    use crate::pipeline::full_disk::Stage;
    const UNIT: u64 = 1 << 20;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
        KEPT,
    );
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    for job in [JOB, SECOND] {
        pipeline.hold_move_for_full_disk(
            job,
            Stage::Claim(Default::default()),
            &complete_dir,
            "no room",
            Some(60 * UNIT),
        );
    }
    // The paused one is judged first, by its id.
    assert!(JOB < SECOND);
    pipeline.jobs.get_mut(&JOB).unwrap().run_state = RunState::Paused;
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(pipeline.held_for_full_disk(JOB), "the paused one waits");
    assert!(!pipeline.held_for_full_disk(SECOND), "the other goes");
    assert_eq!(pipeline.full_disk_promised_bytes(), 60 * UNIT);
}

fn one_file(room: &Arc<AtomicU64>) -> Scenario {
    on_disk_posting(
        room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    )
}

// The next `count` history archives find no room on the database's disk.
fn refuse_archives(count: usize) {
    use crate::persistence::connection::ARCHIVE_REFUSED;
    let mut refused = ARCHIVE_REFUSED.lock().unwrap();
    for _ in 0..count {
        refused.push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    }
}

fn archives_yet_to_refuse() -> usize {
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .len()
}

fn archives_written() -> u64 {
    crate::persistence::connection::ARCHIVES_WRITTEN.load(Ordering::Relaxed)
}

// The loop hears what the database answered, until `until` holds.
async fn hears_database(pipeline: &mut Pipeline, until: impl Fn(&Pipeline) -> bool) {
    while !until(pipeline) {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// How many times the job was said to be complete since the last look.
fn completions(events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>) -> usize {
    let mut count = 0;
    while let Ok(event) = events.try_recv() {
        if matches!(event, PipelineEvent::JobCompleted { job_id } if job_id == JOB) {
            count += 1;
        }
    }
    count
}

// Every write queued so far has run, and the loop has heard all the writer
// said of them: the writer speaks before the flush behind it answers.
async fn writer_heard(pipeline: &mut Pipeline) {
    pipeline.db.flush_write_queue().await.unwrap();
    while let Ok(reply) = pipeline.db_reply_rx.try_recv() {
        pipeline.handle_db_reply(reply).await;
    }
}

// The job is said to be complete.
async fn said_complete(events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>) {
    loop {
        let event = events.recv().await.expect("the job is said to be complete");
        if matches!(event, PipelineEvent::JobCompleted { job_id } if job_id == JOB) {
            return;
        }
    }
}

// How many kept archives the pipeline counts, if it waits for any.
fn kept_count(pipeline: &Pipeline) -> Option<usize> {
    pipeline.archive_wait.as_ref().map(|wait| wait.kept)
}

fn wait_shown(pipeline: &Pipeline) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == JOB)
            .and_then(|row| row.report.job.download_wait_reason.clone())
    })
}

// The job ends with its archive refused `refused` times for want of room,
// and the pipeline has heard that the writer keeps it.
async fn kept_job(
    room: &Arc<AtomicU64>,
    refused: usize,
) -> (
    tempfile::TempDir,
    Pipeline,
    tokio::sync::broadcast::Receiver<PipelineEvent>,
) {
    let scenario = one_file(room);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    let events = pipeline.event_tx.subscribe();
    refuse_archives(refused);
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    hears_database(&mut pipeline, |pipeline| pipeline.archive_wait.is_some()).await;
    assert_eq!(kept_count(&pipeline), Some(1));
    (temp_dir, pipeline, events)
}

const RISE: u64 = crate::pipeline::full_disk::ARCHIVE_RETRY_RISE;

fn in_queue(pipeline: &Pipeline) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.jobs.iter().any(|row| row.id == JOB))
}

fn retiring(pipeline: &Pipeline) -> bool {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == JOB && row.report.retiring)
    })
}

// What of a history row does not depend on the run's clock or folder.
fn history_facts(row: &crate::JobHistoryRow) -> String {
    format!(
        "{:?}",
        (
            (row.job_id, &row.job_hash, &row.name, &row.status),
            (&row.error_message, row.total_bytes, row.downloaded_bytes),
            (row.optional_recovery_bytes, row.failed_bytes, row.health),
            (&row.category, &row.metadata, &row.server_attribution),
            (row.nzb_path.is_some(), row.output_dir.is_some()),
            row.archived_at.is_some(),
        )
    )
}

// The job's history archive is refused for want of room on the database's
// disk. The row stays in the queue, retiring and shown waiting for disk
// space; no history row is written and the job is not said to be complete.
// A reading that shows no more tries nothing. A reading that shows the
// reserve more tries once: refused again, the archive is kept again, and
// the same reading tries nothing more. A further rise writes it, once: the
// job is said to be complete once, its row leaves the queue, its history
// row is the one a first write gives, and the database's folder is read no
// longer.
#[tokio::test]
async fn an_archive_the_database_has_no_room_for_waits_and_is_written_when_room_shows() {
    let disk_space = Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON);
    // What a first write gives, on a pipeline of its own.
    let first_time = {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let scenario = one_file(&room);
        let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
        drive(&mut pipeline, &scenario, JOB, |_| false).await;
        pipeline.db.flush_write_queue().await.unwrap();
        pipeline.db.get_job_history(JOB.0).unwrap().unwrap()
    };
    assert_eq!(archives_written(), 1);

    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 3).await;
    let shown = pipeline.shared_state.get_job(JOB).expect("the row stays");
    assert_eq!(shown.status, JobStatus::Complete);
    assert_eq!(shown.download_wait_reason.as_deref(), disk_space);
    assert!(retiring(&pipeline));
    assert!(pipeline.waits_for_disk_room());
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);
    assert_eq!(pipeline.db.kept_archive_jobs(), [JOB]);
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_none());
    assert_eq!(completions(&mut events), 0);
    assert_eq!((archives_yet_to_refuse(), archives_written()), (2, 1));

    // The first good reading of the wait tries once, whatever it shows:
    // room made since the refusal shows as no rise. Refused, kept again.
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (1, 1));
    assert_eq!(kept_count(&pipeline), Some(1));
    // That reading is the mark; less than the rise over it tries nothing.
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Complete,
        PLENTY + RISE - 1,
    );
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (1, 1));

    // The rise shows: one try, refused, kept again, and nothing is told.
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 1));
    assert_eq!(pipeline.db.kept_archive_jobs(), [JOB]);
    assert_eq!(kept_count(&pipeline), Some(1));
    assert_eq!(wait_shown(&pipeline).as_deref(), disk_space);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);
    // The same reading rings nothing more.
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
    assert!(retiring(&pipeline));
    assert_eq!(completions(&mut events), 0);

    // A further rise: written.
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Complete,
        PLENTY + 2 * RISE,
    );
    said_complete(&mut events).await;
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 2);
    assert_eq!(completions(&mut events), 0, "said once");
    assert!(!in_queue(&pipeline), "the row left the queue");
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
    let written = pipeline.db.get_job_history(JOB.0).unwrap().unwrap();
    assert_eq!(history_facts(&written), history_facts(&first_time));
    // A reading with nothing kept tries nothing.
    hears(&mut pipeline, &room, StorageRoot::Complete, 8 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 2);
}

// The pipeline counts the kept archives from what the writer says of each
// job, and never from the writer's list. Two are kept; one is written. Its
// news is heard while the other is still kept: the count is one, the path
// is still read, and a later rise still rings the writer. The path is let
// go only when the last one is.
#[tokio::test]
async fn the_database_path_is_held_until_the_last_kept_archive_is_let_go() {
    use crate::persistence::ArchiveNews;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    // A second job's archive is kept: the writer says so, of that job.
    pipeline.archive_news(ArchiveNews::Kept(SECOND));
    assert_eq!(kept_count(&pipeline), Some(2));
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    said_complete(&mut events).await;
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    // The writer's list is empty, as it is in the middle of a pass; the
    // pipeline's count is not, and the wait stands.
    assert_eq!(kept_count(&pipeline), Some(1));
    assert_eq!(pipeline.storage_capacity.waited_paths(), 1);
    assert!(pipeline.waits_for_disk_room());
    // A later rise still rings the writer: a refusal waiting is used up
    // by nothing, since nothing is in the writer's list.
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Complete,
        PLENTY + 2 * RISE,
    );
    writer_heard(&mut pipeline).await;
    assert_eq!(kept_count(&pipeline), Some(1));

    pipeline.archive_news(ArchiveNews::LetGo {
        job_id: SECOND,
        written: false,
    });
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
}

// The mark follows the least the disk has shown since the last attempt,
// and the download reserve has no part. The first reading shows much; the
// disk then fills; a little room made over that low rings the writer,
// though the disk shows far less than it first did and far less than the
// reserve, which here no disk could meet.
#[tokio::test]
async fn a_kept_archive_is_tried_on_a_small_rise_over_the_lowest_reading() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 3).await;
    Arc::make_mut(&mut pipeline.extraction_limits).min_free_bytes = u64::MAX / 2;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (2, 0));

    // The first reading tries once and is refused.
    hears(&mut pipeline, &room, StorageRoot::Complete, 10 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (1, 0));
    // The disk fills: a lower reading tries nothing and is the mark.
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (1, 0));
    // Less than the rise over the low: nothing.
    hears(&mut pipeline, &room, StorageRoot::Complete, 5 * RISE - 1);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (1, 0));
    // The rise over the low: one try, refused; that reading is the mark.
    hears(&mut pipeline, &room, StorageRoot::Complete, 5 * RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert_eq!(kept_count(&pipeline), Some(1));
    // The disk fills again and room is made again: written.
    hears(&mut pipeline, &room, StorageRoot::Complete, RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * RISE);
    said_complete(&mut events).await;
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
}

// A kept archive whose retry fails for another reason is lost as any that
// fails so is: the row says no more that it waits, the count goes down and
// the path is let go. The row stays retiring, and nothing is said complete.
#[tokio::test]
async fn a_kept_archive_that_fails_otherwise_shows_no_wait() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    assert!(wait_shown(&pipeline).is_some());
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::Database("the row is malformed".into()));
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert_eq!(kept_count(&pipeline), None);
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_none());
    assert_eq!(completions(&mut events), 0);
}

// The thread of a pass over the kept archives fails and takes the archives
// with it. Each is said to be let go, unwritten: the row shows no wait, the
// count is back to none and the path is let go.
#[tokio::test]
async fn a_pass_whose_thread_fails_lets_its_archives_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    crate::persistence::connection::ARCHIVE_PASS_PANICS.store(true, Ordering::Relaxed);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert_eq!(kept_count(&pipeline), None);
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
    assert_eq!(completions(&mut events), 0);
}

// At the drain the kept archives are tried once more, with no reading and
// no wait. One refused again is named in the warning; one written is not.
#[tokio::test]
async fn the_drain_tries_kept_archives_once_and_names_those_still_kept() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, pipeline, _events) = kept_job(&room, 2).await;
    pipeline.retry_kept_archives_at_drain();
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert_eq!(pipeline.warn_of_kept_archives(), [JOB]);

    pipeline.retry_kept_archives_at_drain();
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(archives_written(), 1);
    assert!(pipeline.warn_of_kept_archives().is_empty());
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_some());
}

// With more kept than one pass takes, the writer says more remain and the
// pipeline queues the next pass from where the writer said: every archive
// is tried on the one ring.
#[tokio::test]
async fn a_ring_runs_pass_after_pass_while_each_writes_one() {
    use crate::persistence::ArchiveNews;
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    // Nothing waits past the pass: more is said only by the writer, and a
    // pass asked for from past the end tries nothing and says nothing.
    pipeline.archive_news(ArchiveNews::More { from: 1 });
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(kept_count(&pipeline), Some(1));
    // A pass asked for from the start writes the one kept.
    pipeline.archive_news(ArchiveNews::More { from: 0 });
    said_complete(&mut events).await;
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
    assert_eq!(kept_count(&pipeline), None);
}

// An archive that fails for any other reason is lost, as before: nothing is
// kept, no path is read, and the row stays retiring with no wait shown.
#[tokio::test]
async fn an_archive_refused_for_another_reason_is_not_kept() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = one_file(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let mut events = pipeline.event_tx.subscribe();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::Database("the row is malformed".into()));
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    // All the writer said of the archive is heard: had it said the archive
    // is kept, the wait, the path and the row's reason would show it.
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert!(pipeline.db.get_job_history(JOB.0).unwrap().is_none());
    assert_eq!(completions(&mut events), 0);
}

// An archive that commits the first time holds no path, tells the loop
// nothing and leaves nothing kept: a job that ends with room costs no
// reading and no write beyond its own.
#[tokio::test]
async fn an_archive_written_the_first_time_holds_no_path() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = one_file(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let mut events = pipeline.event_tx.subscribe();
    drive(&mut pipeline, &scenario, JOB, |_| false).await;
    said_complete(&mut events).await;
    assert!(!in_queue(&pipeline), "the row left the queue");
    // The archive committed, so all the writer said of it is out: nothing
    // of it reaches the loop.
    pipeline.db.flush_write_queue().await.unwrap();
    while let Ok(reply) = pipeline.db_reply_rx.try_recv() {
        assert!(
            !matches!(reply, crate::pipeline::db_reply::DbReply::Archive(_)),
            "a first write tells the loop nothing"
        );
        pipeline.handle_db_reply(reply).await;
    }
    assert_eq!(kept_count(&pipeline), None);
    assert!(pipeline.db.kept_archive_jobs().is_empty());
    assert_eq!(pipeline.storage_capacity.waited_paths(), 0);
    assert!(!pipeline.waits_for_disk_room());
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
}

// A job outside the pipeline, as a torrent's unpacking, that waits for room
// on a disk the pipeline's job waits on too. One reading wakes them all in
// the order they took their tickets from the disk's ledger: one that began
// to wait before the pipeline's job is asked while all three wait, and one
// that began after is asked once the pipeline's job no longer waits, alone.
// A job outside that begins to wait rings the pipeline's loop.
#[tokio::test]
async fn a_job_outside_the_pipeline_and_a_pipeline_job_waiting_wake_in_ticket_order() {
    use crate::pipeline::graph::engine::{Hears, Outside};

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
    }

    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let ledger = working(&pipeline);
    // One outside waiter takes its ticket before the pipeline's job, one
    // after.
    let early = ledger.wait();
    until_waiting(&mut pipeline, &scenario).await;
    let ticket = pipeline.graph_jobs[&JOB]
        .disk_ticket()
        .expect("the job took a ticket");
    let late = ledger.wait();
    assert!(early < ticket && ticket < late);
    assert_eq!(ledger.waiters(), 3);

    let outside = Arc::clone(&pipeline.graph_outside);
    let asked = Arc::new(std::sync::Mutex::new(Vec::new()));
    let (before, after) = (92_201, 92_202);
    for (scope, ticket) in [(after, late), (before, early)] {
        outside.join(
            scope,
            Arc::new(Waits {
                scope,
                ticket,
                ledger: Arc::clone(&ledger),
                outside: Arc::downgrade(&outside),
                asked: Arc::clone(&asked),
            }),
        );
    }
    let began = outside.disk_wait_began();
    outside.disk_held(after);
    outside.disk_held(before);
    began.await;
    pipeline.reconcile_disk_waits();
    assert!(
        outside.disk_waits_began().is_empty(),
        "the pipeline heard both begin"
    );
    assert!(pipeline.waits_for_disk_room());

    // The one reading heard is one begun after the room came, so the pass
    // it starts finds room for the pipeline's job.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    let asked = asked.lock().unwrap().clone();
    assert_eq!(asked.len(), 2, "each asked once: {asked:?}");
    assert_eq!(
        asked[0],
        (before, 3),
        "asked first, while the pipeline's job still waits"
    );
    assert_eq!(
        asked[1],
        (after, 1),
        "asked after the pipeline's job no longer waits"
    );
    assert!(!pipeline.graph_disk_held(JOB), "the pipeline's job goes on");
    assert!(!outside.waits_for_disk());
    assert!(!pipeline.waits_for_disk_room());
}
