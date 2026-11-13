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
use crate::pipeline::Pipeline;
use crate::pipeline::graph::leaf::counting::fill_after;
use crate::pipeline::tests::job_status_for_assert;
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
    let room = Arc::clone(room);
    Scenario::new(posts)
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
            let probe = Arc::new(move |_: &Path| match room.load(Ordering::Acquire) {
                UNREADABLE => Err(crate::operations::disk::DiskProbeError::InvalidPath),
                room => Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: RESERVE + room,
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
        pipeline.dispatch_wait_reason(JOB),
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
    assert_eq!(pipeline.dispatch_wait_reason(JOB), None);

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
        pipeline.dispatch_wait_reason(JOB),
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
        pipeline.dispatch_wait_reason(JOB),
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
    assert_eq!(pipeline.dispatch_wait_reason(JOB), None);

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

    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Intermediate);
    for sweep in pipeline.full_disk_sweeps.drain(..) {
        sweep.await.unwrap();
    }

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
    // Parked, it counts as left to fetch what its graph had not settled,
    // not every article: the figure a job put out of the window is rowed by.
    let left = pipeline.jobs[&JOB].articles.queued_download_len();
    assert!((1..4).contains(&left), "{left} of 4 articles are left");

    // The disk reads the same room at every look: none was made.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    assert!(
        matches!(pipeline.jobs[&JOB].articles, JobArticles::Queued(_)),
        "no reading without more room restarts it"
    );

    // Its user resumes it: it tries once, and the write fails again.
    fill_after(Some(0));
    pipeline.resume_job_runtime(JOB).unwrap();
    restarts(&mut pipeline).await;
    room.store(PLENTY - 1, Ordering::Release);
    until_full(&mut pipeline, &scenario).await;

    // It saved nothing new, so it still waits for more than the disk showed
    // before it was let go, not only for more than it shows now.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY - 1);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);

    // More room than it has ever shown: room was made.
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 1);
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
    assert!(
        pipeline.full_disk_highs.is_empty(),
        "a job that ran on keeps nothing of its wait"
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
// still being decoded: nothing of it is left to fetch, so no lane would ever
// ask for it. Its release restarts it itself, and it completes.
#[tokio::test]
async fn a_job_held_for_a_full_disk_after_its_download_restarts_with_nothing_to_fetch() {
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
    pipeline.hold_job_for_full_disk(JOB, StorageRoot::Intermediate);
    pipeline.park_drained_jobs();
    old_graph_heard(&mut pipeline).await;
    assert_full_disk_held(&pipeline);
    assert_eq!(
        pipeline.jobs[&JOB].articles.queued_download_len(),
        0,
        "parked with every article in, it counts none as left to fetch"
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
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
