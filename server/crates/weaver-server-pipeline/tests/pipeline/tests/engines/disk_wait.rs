// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_archive::*;
use super::disk_reserve::*;
use super::harness::JOB;
use super::harness::{ARTICLE, Post, Scenario, admit, payload};
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_composition::JobId;
use weaver_server_composition::{
    JobHistoryRow,
    operations::{DiskSpace, StorageRoot},
};
use weaver_server_pipeline::jobs::model::JobStatus;
use weaver_server_pipeline::pipeline::tests::engines::restart_regressions::restart_as_it_stands;
use weaver_server_pipeline::pipeline::tests::engines::stream_fixtures::xz;
use weaver_server_pipeline::pipeline::tests::{
    finish_cancel_jobs,
    queue_fixture::{apply_intents, handle},
};
use weaver_server_pipeline::{jobs::model::RunState, pipeline::Pipeline};
// The job is said to be complete.
async fn said_complete(events: &mut tokio::sync::broadcast::Receiver<PipelineEvent>) {
    loop {
        let event = events.recv().await.expect("the job is said to be complete");
        if matches!(event, PipelineEvent::JobCompleted { job_id } if job_id == JOB) {
            return;
        }
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

// What of a history row does not depend on the run's clock or folder.
fn history_facts(row: &JobHistoryRow) -> String {
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
    let disk_space = Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON);
    // What a first write gives, on a pipeline of its own.
    let first_time = {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let scenario = one_file(&room);
        let (_temp_dir, mut pipeline) = admit_archive(&scenario).await;
        finish_disk_job(&mut pipeline, &scenario).await;
        flush(&pipeline).await;
        history(&pipeline).unwrap()
    };
    assert_eq!(archives_written(), 1);

    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 3).await;
    // The row stays, still being finished: history cannot remove the job
    // until its archive is written, so no reader is told it ended.
    let shown = shown(&pipeline).expect("the row stays");
    assert_eq!(shown.status, JobStatus::PostProcessing);
    assert_eq!(shown.download_wait_reason.as_deref(), disk_space);
    assert!(retiring(&pipeline));
    assert!(waiting(&pipeline));
    assert_eq!(waited_paths(&pipeline), 1);
    assert_eq!(kept(&pipeline), [JOB]);
    assert!(history(&pipeline).is_none());
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
    assert_eq!(kept(&pipeline), [JOB]);
    assert_eq!(kept_count(&pipeline), Some(1));
    assert_eq!(wait_shown(&pipeline).as_deref(), disk_space);
    assert_eq!(waited_paths(&pipeline), 1);
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
    assert!(kept(&pipeline).is_empty());
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
    let written = history(&pipeline).unwrap();
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
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    // A second job's archive is kept: the writer says so, of that job.
    archive_kept(&mut pipeline, SECOND);
    assert_eq!(kept_count(&pipeline), Some(2));
    assert_eq!(waited_paths(&pipeline), 1);

    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    said_complete(&mut events).await;
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
    assert!(kept(&pipeline).is_empty());
    // The writer's list is empty, as it is in the middle of a pass; the
    // pipeline's count is not, and the wait stands.
    assert_eq!(kept_count(&pipeline), Some(1));
    assert_eq!(waited_paths(&pipeline), 1);
    assert!(waiting(&pipeline));
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

    archive_let_go(&mut pipeline, SECOND, false);
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
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
    keep_free(&mut pipeline, u64::MAX / 2);
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
    assert_eq!(waited_paths(&pipeline), 0);
}

// A kept archive whose retry fails for another reason is lost as any that
// fails so is: the row says no more that it waits, the count goes down and
// the path is let go. The row stays retiring, and nothing is said complete.
#[tokio::test]
async fn a_kept_archive_that_fails_otherwise_shows_no_wait() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    assert!(wait_shown(&pipeline).is_some());
    refuse_malformed();
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY + RISE);
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert_eq!(kept_count(&pipeline), None);
    assert!(kept(&pipeline).is_empty());
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
    assert!(history(&pipeline).is_none());
    assert_eq!(completions(&mut events), 0);
}

// The thread of a pass over the kept archives fails and takes the archives
// with it. Each is said to be let go, unwritten: the row shows no wait, the
// count is back to none and the path is let go.
#[tokio::test]
async fn a_pass_whose_thread_fails_lets_its_archives_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    panic_next_pass();
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert_eq!(kept_count(&pipeline), None);
    assert!(kept(&pipeline).is_empty());
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
    assert_eq!(completions(&mut events), 0);
}

// At the drain the kept archives are tried once more, with no reading and
// no wait. One refused again is named in the warning; one written is not.
#[tokio::test]
async fn the_drain_tries_kept_archives_once_and_names_those_still_kept() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, pipeline, _events) = kept_job(&room, 2).await;
    retry_at_drain(&pipeline);
    flush(&pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert_eq!(warn_of_kept(&pipeline), [JOB]);

    retry_at_drain(&pipeline);
    flush(&pipeline).await;
    assert_eq!(archives_written(), 1);
    assert!(warn_of_kept(&pipeline).is_empty());
    assert!(history(&pipeline).is_some());
}

// With more kept than one pass takes, the writer says more remain and the
// pipeline queues the next pass from where the writer said: every archive
// is tried on the one ring.
#[tokio::test]
async fn a_ring_runs_pass_after_pass_while_each_writes_one() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let (_temp_dir, mut pipeline, mut events) = kept_job(&room, 1).await;
    // Nothing waits past the pass: more is said only by the writer, and a
    // pass asked for from past the end tries nothing and says nothing.
    archive_more(&mut pipeline, 1);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(kept_count(&pipeline), Some(1));
    // A pass asked for from the start writes the one kept.
    archive_more(&mut pipeline, 0);
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
    let (_temp_dir, mut pipeline) = admit_archive(&scenario).await;
    let mut events = subscribe(&pipeline);
    refuse_malformed();
    finish_disk_job(&mut pipeline, &scenario).await;
    // All the writer said of the archive is heard: had it said the archive
    // is kept, the wait, the path and the row's reason would show it.
    writer_heard(&mut pipeline).await;
    assert_eq!((archives_yet_to_refuse(), archives_written()), (0, 0));
    assert!(kept(&pipeline).is_empty());
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 0);
    assert_eq!(kept_count(&pipeline), None);
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
    assert_eq!(wait_shown(&pipeline), None);
    assert!(retiring(&pipeline));
    assert!(history(&pipeline).is_none());
    assert_eq!(completions(&mut events), 0);
}

// An archive that commits the first time holds no path, tells the loop
// nothing and leaves nothing kept: a job that ends with room costs no
// reading and no write beyond its own.
#[tokio::test]
async fn an_archive_written_the_first_time_holds_no_path() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = one_file(&room);
    let (_temp_dir, mut pipeline) = admit_archive(&scenario).await;
    let mut events = subscribe(&pipeline);
    finish_disk_job(&mut pipeline, &scenario).await;
    said_complete(&mut events).await;
    assert!(!in_queue(&pipeline), "the row left the queue");
    // The archive committed, so all the writer said of it is out: nothing
    // of it reaches the loop.
    flush(&pipeline).await;
    assert!(
        first_write_silent(&mut pipeline).await,
        "a first write tells the loop nothing"
    );
    assert_eq!(kept_count(&pipeline), None);
    assert!(kept(&pipeline).is_empty());
    assert_eq!(waited_paths(&pipeline), 0);
    assert!(!waiting(&pipeline));
    hears(&mut pipeline, &room, StorageRoot::Complete, 4 * PLENTY);
    writer_heard(&mut pipeline).await;
    assert_eq!(archives_written(), 1);
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

    assert_eq!(eligible(&pipeline), vec![JOB]);
    assert!(!waiting(&pipeline));
    assert!(!ledger.waited_on(), "nothing waits on the disk");
    assert_eq!(dispatch_reason(&pipeline).as_deref(), None);

    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let spec = weaver_server_pipeline::pipeline::tests::segmented_job_spec(
        "cinder",
        "cinder.bin",
        &[512; 4],
    );
    let nzb_zstd = weaver_server_pipeline::pipeline::tests::nzb_zstd_for(&spec);
    add_waiting_job(&mut pipeline, OTHER, spec, nzb_zstd).await;
    apply_intents(&mut pipeline).await;

    assert!(!admitted(&pipeline, OTHER), "it did not start");
    assert!(admitted(&pipeline, JOB));
    assert_eq!(
        row_reason(&pipeline, OTHER).as_deref(),
        Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON),
        "its row says it waits for disk space"
    );
    assert!(waiting(&pipeline));
    assert!(ledger.waited_on(), "the wait hears every reading");
    let eligible = eligible(&pipeline);
    assert!(eligible.contains(&JOB), "the started job fetches on");
    assert!(!eligible.contains(&OTHER));

    let before = download_queue_len(&pipeline);
    dispatch(&mut pipeline);
    assert!(
        download_queue_len(&pipeline) < before,
        "the dispatch pass hands the started job's articles out"
    );

    room_freed(&mut pipeline, &room).await;
    assert!(!ledger.waited_on());
    apply_intents(&mut pipeline).await;
    assert!(admitted(&pipeline, OTHER), "it started");
    assert_ne!(
        row_reason(&pipeline, OTHER).as_deref(),
        Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON)
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

    let mut pipeline = restart_disk(&temp_dir, pipeline, &scenario).await;
    apply_intents(&mut pipeline).await;
    assert!(admitted(&pipeline, JOB), "restored as started");
    assert_eq!(eligible(&pipeline), vec![JOB]);
    assert!(!waiting(&pipeline));
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
        apply_intents(&mut pipeline).await;
        finish_cancel_jobs(&mut pipeline).await;
        assert!(!has_graph(&pipeline));
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
    apply_intents(&mut pipeline).await;
    assert!(waiting(&pipeline), "a pause ends no wait");

    room_freed(&mut pipeline, &room).await;
    assert!(!ledger.waited_on());
    assert!(
        !eligible(&pipeline).contains(&JOB),
        "the pause still holds the job"
    );

    handle.resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!waiting(&pipeline));
    assert_ne!(
        dispatch_reason(&pipeline).as_deref(),
        Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(run_state(&pipeline), RunState::Paused);
    assert!(!admitted(&pipeline, JOB), "the pause gave back its slot");

    handle.resume_job(JOB).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert!(admitted(&pipeline, JOB), "it took its slot again");
    assert_eq!(run_state(&pipeline), RunState::Active);
    assert!(!waiting(&pipeline));
    assert_ne!(
        row_reason(&pipeline, JOB).as_deref(),
        Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON)
    );
    assert_eq!(eligible(&pipeline), vec![JOB]);
    // A paused job let its articles go: it reads them back as it is reached.
    weaver_server_pipeline::pipeline::tests::activate_job(&mut pipeline, JOB).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    assert!(waiting(&pipeline), "nothing woke the job");
}

// A job that waits for room when the pipeline goes down is restored by the
// next one, which reads the disk afresh: with room there, it completes.
#[tokio::test]
async fn a_job_waiting_for_room_at_a_restart_is_restored_and_completes() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (temp_dir, mut pipeline, _) = admit(&scenario).await;
    until_waiting(&mut pipeline, &scenario).await;

    drain(&mut pipeline).await;
    room.store(PLENTY, Ordering::Release);
    let mut pipeline = restart_disk(&temp_dir, pipeline, &scenario).await;
    assert!(!waiting(&pipeline));
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let posts = weaver_server_pipeline::pipeline::tests::direct_store::single_member_store_set(
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
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let output = complete_ledger(&pipeline);
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
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    let files = delivered(&complete_dir);
    assert_eq!(file_named(&files, "a.bin"), a.as_slice());
    assert_eq!(file_named(&files, "b.bin"), b.as_slice());
    assert_released(&pipeline, &working);
    assert_released(&pipeline, &output);
}

// A job that waits for room the disk could make is said to wait once, as
// the wait begins: turns with nothing changed and readings short of room
// say nothing more, and once room comes it completes.
#[tokio::test]
async fn a_wait_for_room_is_said_once_as_it_begins() {
    let said = waits_said;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let before = said();
    until_waiting(&mut pipeline, &scenario).await;
    assert_eq!(said(), before + 1, "said as it began");

    for _ in 0..3 {
        reconcile_waits(&mut pipeline);
        reading_of(&mut pipeline, &room, 0).await;
    }
    assert!(graph_waiting(&pipeline));
    assert_eq!(said(), before + 1, "said once, not per turn or reading");

    room_freed(&mut pipeline, &room).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_released(&pipeline, &working(&pipeline));
}

// A disk whose whole size is its reserve could never hold what the job
// writes, as its reading tells it: the job is not failed for that. It waits,
// as any job short of room does, is said to once, and completes when a
// reading shows room.
#[tokio::test]
async fn a_job_on_a_disk_no_larger_than_its_reserve_waits_and_is_never_failed() {
    let said = waits_said;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain(
            "ember.bin.xz",
            xz(&payload(3 * ARTICLE + 7, 91)),
        )],
        RESERVE * 8,
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let before = said();
    drive_until_graph_waiting(&mut pipeline, &scenario).await;
    reconcile_waits(&mut pipeline);
    assert!(in_disk_wait(&pipeline), "it waits");
    assert_eq!(said(), before + 1, "said once, as it began");
    assert!(
        !matches!(status(&pipeline), Some(JobStatus::Failed { .. })),
        "nothing fails it"
    );

    room_freed(&mut pipeline, &room).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(said(), before + 1);
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
    assert!(waiting(&pipeline));
    // Turns with nothing changed look at no waiting job.
    let looks = wait_looks();
    for _ in 0..3 {
        reconcile_waits(&mut pipeline);
    }
    assert_eq!(wait_looks(), looks, "an idle turn looks at no job");

    for _ in 0..3 {
        reading_of(&mut pipeline, &room, kept + 200).await;
        assert!(
            graph_waiting(&pipeline),
            "a reading short of it wakes nothing"
        );
        assert!(!graph_waiting_on_mail(&pipeline), "no run went out again");
        assert!(waiting(&pipeline));
    }

    room_freed(&mut pipeline, &room).await;
    settle(&mut pipeline, &scenario, &mut settled).await;
    assert!(settled.ended);
    assert_eq!(settled.runs, 2, "the run ran once more");
    assert_eq!(settled.holds, 1, "once woken it was not held again");
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    let files = delivered(&complete_dir);
    assert_eq!(file_named(&files, "a.bin"), a.as_slice());
    assert_eq!(file_named(&files, "b.bin"), b.as_slice());
    assert_released(&pipeline, &working(&pipeline));
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
        let scenario = on_disk_posting(&room, vec![Post::plain("nest.tar.xz", posted)])
            .on_run_message(move |_, _, message| {
                // Any reading from here, the first that the hold asks for,
                // shows room.
                heard_room.store(PLENTY, Ordering::Release);
                if message == "Ended" {
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
    let posts = weaver_server_pipeline::pipeline::tests::direct_store::single_member_store_set(
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
    let spec = weaver_server_pipeline::pipeline::tests::segmented_job_spec(
        "cinder",
        "cinder.bin",
        &[512; 4],
    );
    let nzb_zstd = weaver_server_pipeline::pipeline::tests::nzb_zstd_for(&spec);
    add_waiting_job(&mut pipeline, OTHER, spec, nzb_zstd).await;
    apply_intents(&mut pipeline).await;
    assert!(admission_held(&pipeline), "it is kept from starting");
    assert!(!admitted(&pipeline, OTHER));

    // Every reading so far heard, as a turn would: room 0 wakes no one.
    hear_disk_on_busy_turn(&mut pipeline);
    assert!(graph_waiting(&pipeline));
    assert!(admission_held(&pipeline));

    // The reading with room is published; no turn waits for it.
    room.store(PLENTY, Ordering::Release);
    capacity(&pipeline).request_refresh(StorageRoot::Intermediate);
    capacity(&pipeline).wait_for_reading(StorageRoot::Intermediate, |reading| {
        reading.available_bytes == RESERVE + PLENTY
    });
    assert!(graph_waiting(&pipeline), "nothing heard it yet");

    hear_disk_on_busy_turn(&mut pipeline);
    assert!(!graph_waiting(&pipeline), "the waiting job woke");
    assert!(!waiting(&pipeline));
    assert!(!admission_held(&pipeline), "the hold ended");
    apply_intents(&mut pipeline).await;
    assert!(admitted(&pipeline, OTHER), "it started");
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
    finish_disk_job(&mut never_held, &scenario).await;
    assert_eq!(status(&never_held), Some(JobStatus::Complete));
    let whole = counted(&never_held);
    assert!(whole >= body.len() as u64);

    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(&room, post());
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));

    // Every article is served: those after the write that failed come back
    // for a job already held.
    drive_until_full_disk_held(&mut pipeline, &scenario).await;
    assert!(full_disk_held(&pipeline));
    let (downloaded, checkpointed) = counted_and_checkpointed(&pipeline);
    assert_eq!(
        downloaded, checkpointed,
        "the job counts what its checkpoint holds"
    );
    assert!(
        counted(&pipeline) <= ARTICLE as u64,
        "nothing that came back after the write failed is counted: {}",
        counted(&pipeline)
    );
    // The article written and the one whose write failed were placed; the
    // two that came back after were not.
    let placed = placed_articles(&pipeline);
    assert!(placed <= 2, "{placed} articles are placed");

    park_disk_held_jobs(&mut pipeline);
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

// A job let go that finds its disk full again, having saved nothing new,
// waits for more room than its disk has yet shown: readings that never
// change, as of a disk whose free space is not what stops the write, do not
// send it round. A resume by its user lets it try once, whatever they say.
#[tokio::test]
async fn a_job_let_go_that_fails_again_waits_for_more_room_than_its_disk_has_shown() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 113);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    // Parked, it counts as left to fetch every article its last checkpoint
    // does not hold: what it fetches when it goes on.
    let left = queued_articles(&pipeline);
    assert!((1..=4).contains(&left), "{left} of 4 articles are left");

    // The disk reads the same room at every look: none was made.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    assert!(
        articles_queued(&pipeline),
        "no reading without more room restarts it"
    );

    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE + PLENTY),
        "it waits for more than its disk showed as the write failed"
    );

    // Its user resumes it, though it is not paused: it tries once, and the
    // write fails again with the disk showing what it showed.
    fill_after(Some(0));
    handle(&pipeline).resume_job(JOB).await.unwrap();
    apply_intents(&mut pipeline).await;
    restarts(&mut pipeline).await;
    assert_eq!(
        active_download_queue_len(&pipeline),
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
    let first = full_disk_rise(&pipeline).0.expect("what was left");
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
        full_disk_mark(&pipeline),
        Some(RESERVE + PLENTY + first),
        "the new wait has what the disk showed at its own failure to beat"
    );

    // More again: room was made, and this time the write lands.
    let second = full_disk_rise(&pipeline).0.expect("what was left");
    fill_after(None);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + first + second,
    );
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE));

    // Another job has the room made promised to it.
    working(&pipeline).promise(PLENTY);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE),
        "a reading that lets no job go raises nothing"
    );

    // That job gives it back: the disk shows the same figure.
    working(&pipeline).release(PLENTY);
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    fill_after(Some(0));
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;

    // Something else fills the disk before the job writes again.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE));

    // Less room than let it go before, and more than the disk showed.
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE + 2 * PLENTY));

    fill_after(None);
    resume_disk_held_job(&mut pipeline);
    restarts(&mut pipeline).await;

    // Its next write fails on the output disk, which still shows little.
    room.store(0, Ordering::Release);
    hold_on_root(&mut pipeline, StorageRoot::Complete);
    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE),
        "the wait has its own disk's figure to beat"
    );
    park_disk_held_jobs(&mut pipeline);
    old_graph_heard(&mut pipeline).await;
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert!(full_disk_held(&pipeline));
    hears(&mut pipeline, &room, StorageRoot::Complete, PLENTY);
    assert!(
        !full_disk_held(&pipeline),
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
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    // A good reading, then a look that fails: the figure kept is stale.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert!(
        capacity(&pipeline)
            .current(StorageRoot::Intermediate)
            .reading()
            .is_some_and(|reading| reading.stale)
    );
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE),
        "the stale figure is what the disk showed"
    );

    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert_full_disk_held(&pipeline);
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE));

    // The disk reads again, with room made meanwhile: the job goes at once.
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;

    // No reading at all.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, UNREADABLE);
    assert!(
        capacity(&pipeline)
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
        capacity(&pipeline)
            .current(StorageRoot::Intermediate)
            .reading()
            .is_some_and(|reading| reading.stale)
    );
    assert_full_disk_held(&pipeline);

    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
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
    assert_eq!(capacity(&pipeline).waited_paths(), 0);

    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    capacity(&pipeline).retarget(StorageRoot::Intermediate, elsewhere);
    hear_disk_reading(&mut pipeline);
    assert!(full_disk_held(&pipeline), "the job still waits");
    assert_eq!(
        capacity(&pipeline).waited_paths(),
        1,
        "the disk that failed is read as a path of its own"
    );
    assert!(
        !working(&pipeline).waited_on(),
        "the root pointed elsewhere is no longer waited on"
    );
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE), "its mark stands");

    // The disk that failed reads no more room, then room made.
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    assert!(full_disk_held(&pipeline));
    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(
        !full_disk_held(&pipeline),
        "room on the disk that failed lets it go"
    );
    assert_eq!(capacity(&pipeline).waited_paths(), 0);
}

// Room freed between the failed write and the next look at the disk: the
// first reading taken since the failure shows more than the disk showed
// before it, and lets the job go.
#[tokio::test]
async fn room_made_before_the_first_reading_since_the_failure_lets_the_job_go() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let body = payload(4 * ARTICLE, 131);
    let scenario = on_disk_posting(&room, vec![Post::plain("ember.bin", body.clone())]);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;

    // The disk shows nothing over the reserve before the write fails.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    drive_until_full_disk_held(&mut pipeline, &scenario).await;
    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE),
        "held, it has what its disk showed before the write failed to beat"
    );

    // Room is made before the disk is looked at again.
    fill_after(None);
    room.store(2 * PLENTY, Ordering::Release);
    park_disk_held_jobs(&mut pipeline);
    old_graph_heard(&mut pipeline).await;
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
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
    let complete_dir = complete_directory(&pipeline);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;
    let (left, _) = full_disk_rise(&pipeline);
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
    assert_eq!(full_disk_mark(&pipeline), Some(PLENTY));
    assert_eq!(full_disk_rise(&pipeline), (Some(left), false));

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
    assert_eq!(full_disk_mark(&pipeline), Some(PLENTY), "the mark stands");

    // The room of what was left shows: the job goes on, and what it was
    // let go on is promised to it.
    fill_after(None);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + left,
    );
    assert_eq!(promised_bytes(&pipeline), left);
    restarts(&mut pipeline).await;
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE));

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
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
    assert_eq!(
        file_named(&delivered(&complete_dir), "ember.bin"),
        body.as_slice()
    );
}

// A write a quota refused, on a disk that shows plenty free: the job is
// held, and no reading of that plenty lets it go. It goes when the figure
// has risen from the mark by what was left of its file. Let go and refused
// by the quota again, as a job restored after a restart is at its first
// write, it is held on a mark of its own and not let go at once.
#[tokio::test]
async fn a_download_a_quota_refused_waits_for_the_figure_to_rise_by_what_was_left() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    refuse_by_quota(true);
    let (_temp_dir, mut pipeline, scenario, complete_dir, body, left) =
        held_with_a_file_part_written(&room, RESERVE, 163).await;
    let mark = RESERVE + PLENTY;
    assert_eq!(full_disk_mark(&pipeline), Some(mark));
    assert_eq!(full_disk_rise(&pipeline), (Some(left), true));

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
    let (again, quota) = full_disk_rise(&pipeline);
    assert!(quota, "the new hold is a quota hold");
    let again = again.expect("the write that failed knew what was left");
    assert_eq!(full_disk_mark(&pipeline), Some(mark + left));
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
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    // Fifteen units of a file, on a disk that shows far more free.
    const NEEDS: u64 = 15 << 20;
    for quota in [true, false] {
        let room = Arc::new(AtomicU64::new(PLENTY));
        let (_temp_dir, mut pipeline, scenario, complete_dir, body, _) =
            held_with_a_file_part_written(&room, RESERVE, 167).await;
        // The same job, held as a step that needs `NEEDS` is held.
        drop_disk_hold(&mut pipeline);
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
        hold_step(&mut pipeline, NEEDS, quota);
        park_disk_held_jobs(&mut pipeline);
        old_graph_heard(&mut pipeline).await;

        fill_after(None);
        // The disk shows what it showed: a step the disk refused goes on
        // that, one a quota refused does not.
        if quota {
            assert_ne!(
                full_disk_mark(&pipeline),
                Some(RESERVE + PLENTY - (5 << 20)),
                "the mark is not the reading from before the step gave its output back"
            );
        }
        hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
        if quota {
            assert_eq!(full_disk_mark(&pipeline), Some(RESERVE + PLENTY));
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
            promised_bytes(&pipeline),
            NEEDS,
            "the room it goes on is promised to it"
        );
        restarts(&mut pipeline).await;
        finish_disk_job(&mut pipeline, &scenario).await;
        assert_eq!(status(&pipeline), Some(JobStatus::Complete));
        assert_eq!(
            file_named(&delivered(&complete_dir), "ember.bin"),
            body.as_slice()
        );
    }
}

// The reserve of a job whose one file is larger than it.
const KEPT: u64 = 1;

// Two jobs wait on one disk, each for 60 of the 100 a reading then shows.
// One goes, and its 60 stay counted against the disk: the other waits, on
// that reading and on any like it. When the first one's step has ended,
// here by its being held again, the other is judged on a reading taken
// after that, never on the one from before, and goes when that shows room.
#[tokio::test]
async fn room_promised_to_one_held_job_is_not_promised_to_the_next() {
    const UNIT: u64 = 1 << 20;
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
        KEPT,
    );
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    assert_eq!(promised_bytes(&pipeline), 0);
    for job in [JOB, SECOND] {
        hold_claim(&mut pipeline, job, &complete_dir, Some(60 * UNIT), false);
        assert!(disk_held(&pipeline, job));
        assert!(!disk_held_on_path(&pipeline, job), "both wait on the root");
    }

    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    let held = [JOB, SECOND].map(|job| disk_held(&pipeline, job));
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
    assert_eq!(promised_bytes(&pipeline), 60 * UNIT);
    // The same room read again is still promised.
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(disk_held(&pipeline, waits));
    hears(&mut pipeline, &room, StorageRoot::Complete, 119 * UNIT);
    assert!(disk_held(&pipeline, waits));

    // The first one's step ends: it found the disk full again, and waits
    // with no figure of its own. Its step filled the disk.
    room.store(59 * UNIT, Ordering::Release);
    hold_claim(&mut pipeline, went, &complete_dir, None, false);
    assert_eq!(promised_bytes(&pipeline), 0);
    // The reading from before that step ended showed room the step has
    // filled since: heard again, it lets no one go, and neither does a
    // reading taken since.
    hear_disk_reading(&mut pipeline);
    assert!(disk_held(&pipeline, waits));
    assert!(disk_held(&pipeline, went));

    // A new reading that shows too little keeps it; one that shows the room
    // lets it go, and that room is promised to it.
    hears(&mut pipeline, &room, StorageRoot::Complete, 59 * UNIT);
    assert!(disk_held(&pipeline, waits));
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(!disk_held(&pipeline, waits), "the other goes");
    assert!(disk_held(&pipeline, went));
    assert_eq!(promised_bytes(&pipeline), 60 * UNIT);
}

// No hold is let go by less than the write that was refused. A spool write
// that found no room waits for the free figure to rise by the part it could
// not write. A site with no figure at all, here refused by a quota on a
// disk that shows plenty free, waits for it to rise by the reserve: a few
// bytes freed let nothing go.
#[tokio::test]
async fn a_hold_with_no_figure_is_not_let_go_by_a_few_bytes() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    for (kind, quota) in [
        (std::io::ErrorKind::StorageFull, false),
        (std::io::ErrorKind::QuotaExceeded, true),
    ] {
        refuse_spool_write(&mut pipeline, kind, 4096);
        assert_eq!(full_disk_rise(&pipeline), (Some(4096), quota));
        assert_eq!(full_disk_mark(&pipeline), Some(RESERVE + PLENTY));
        hears(
            &mut pipeline,
            &room,
            StorageRoot::Intermediate,
            PLENTY + 4095,
        );
        assert_eq!(promised_bytes(&pipeline), 0);
        hears(
            &mut pipeline,
            &room,
            StorageRoot::Intermediate,
            PLENTY + 4096,
        );
        assert_eq!(promised_bytes(&pipeline), 4096);
        end_disk_promise(&mut pipeline);
        assert!(drop_disk_hold(&mut pipeline));
        hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    }

    hold_on_quota(&mut pipeline);
    assert_eq!(full_disk_rise(&pipeline), (Some(RESERVE), true));
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE + PLENTY));
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + 4096,
    );
    assert_eq!(promised_bytes(&pipeline), 0);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + RESERVE - 1,
    );
    assert_eq!(promised_bytes(&pipeline), 0);
    hears(
        &mut pipeline,
        &room,
        StorageRoot::Intermediate,
        PLENTY + RESERVE,
    );
    assert_eq!(promised_bytes(&pipeline), RESERVE);
}

// A job held on a root and a job held on a path below it, on the same
// device: each waits for 60 of the 100 a reading then shows. One goes, and
// its 60 count against the other, though one reads the root and the other
// the path.
#[tokio::test]
async fn a_job_held_on_a_root_and_one_on_a_path_of_its_disk_are_promised_the_same_room_once() {
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
    weaver_server_disk::runtime::fs::test_devices::assign(&complete_dir, 7);
    weaver_server_disk::runtime::fs::test_devices::assign(&below, 7);
    let capacity = capacity_ref(&pipeline);
    for root in StorageRoot::ALL {
        let begun = capacity.probes_begun(root);
        capacity.request_refresh(root);
        capacity.wait_for_reading_begun_after(root, begun);
    }
    hold_claim(&mut pipeline, JOB, &complete_dir, Some(60 * UNIT), false);
    hold_claim(&mut pipeline, SECOND, &below, Some(60 * UNIT), false);
    assert!(!disk_held_on_path(&pipeline, JOB), "one waits on the root");
    assert!(disk_held_on_path(&pipeline, SECOND), "one on the path");

    for _ in 0..2 {
        hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
        let held = [JOB, SECOND].map(|job| disk_held(&pipeline, job));
        assert_eq!(
            held.iter().filter(|held| **held).count(),
            1,
            "one goes and one waits: {held:?}"
        );
        assert_eq!(promised_bytes(&pipeline), 60 * UNIT);
    }
    hears(&mut pipeline, &room, StorageRoot::Complete, 120 * UNIT);
    assert!(!disk_held(&pipeline, JOB) && !disk_held(&pipeline, SECOND));
    assert_eq!(promised_bytes(&pipeline), 120 * UNIT);
}

// A usenet job and a torrent held on one disk, each for 60 of the 100 a
// reading then shows, are promised that room once: both are judged beside
// the promises of either kind. Whichever goes, the other waits; once the
// step of the one that went ends, the other counts readings from then,
// and the next lets it go.
#[tokio::test]
async fn a_usenet_job_and_a_torrent_held_on_one_disk_are_promised_the_same_room_once() {
    use weaver_server_disk::operations::room_wait::{Disk, Promised, RoomWait, Waiter, Wants};
    const UNIT: u64 = 1 << 20;
    const TORRENT: JobId = JobId(93_013);
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk_keeping(
        &room,
        vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
        KEPT,
    );
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    add_second(&mut pipeline).await;
    let capacity = capacity_ref(&pipeline);
    let reserve = disk_reserve_minimum(&pipeline);
    // The torrent's wait, judged as the torrent service judges it.
    let judge = |wait: &mut RoomWait| {
        let shown = wait.judge(&capacity, reserve, false, |disk, device| {
            capacity.promises().on(disk, device)
        });
        if let Some((bytes, device)) = shown {
            capacity.promises().insert(
                Waiter::Torrent,
                TORRENT,
                Promised {
                    disk: wait.disk.clone(),
                    bytes,
                    device,
                },
            );
        }
        shown.is_some()
    };
    let mut torrent = RoomWait::begin(
        &capacity,
        Disk::Root(StorageRoot::Complete),
        complete_dir.clone(),
        Wants::from(Some(60 * UNIT)),
        reserve,
    );
    hold_claim(&mut pipeline, JOB, &complete_dir, Some(60 * UNIT), false);

    // The usenet job is judged first and goes; the torrent waits beside it.
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(!disk_held(&pipeline, JOB));
    assert!(
        !judge(&mut torrent),
        "the torrent waits beside the job's promise"
    );

    // The job's step ends: the next reading lets the torrent go.
    end_disk_promise(&mut pipeline);
    for (ended, begun) in capacity.promises().take_ended(Waiter::Torrent) {
        torrent.promise_ended(&capacity, &ended, begun);
    }
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(judge(&mut torrent), "the torrent goes");
    torrent.end(&capacity);

    // A usenet job held now waits beside the torrent's promise, until the
    // torrent's piece lands and ends it.
    hold_claim(&mut pipeline, SECOND, &complete_dir, Some(60 * UNIT), false);
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(
        disk_held(&pipeline, SECOND),
        "the job waits beside the torrent"
    );
    capacity
        .promises()
        .end(&capacity, Waiter::Torrent, TORRENT)
        .unwrap();
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(!disk_held(&pipeline, SECOND), "the job goes");
    assert_eq!(promised_bytes(&pipeline), 60 * UNIT);
}

// The user's resume lets a held job go, whatever the readings say: one
// that was paused while held goes at that one resume, and one a quota
// holds, not paused, goes too.
#[tokio::test]
async fn one_resume_lets_a_held_job_go_paused_or_not() {
    const UNIT: u64 = 1 << 20;
    for paused in [true, false] {
        let room = Arc::new(AtomicU64::new(0));
        let scenario = on_disk_keeping(
            &room,
            vec![Post::plain("harbor.bin", payload(ARTICLE, 7))],
            KEPT,
        );
        let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
        hold_claim(&mut pipeline, JOB, &complete_dir, Some(60 * UNIT), true);
        hears(&mut pipeline, &room, StorageRoot::Complete, 0);
        assert!(disk_held(&pipeline, JOB), "paused: {paused}");
        if paused {
            pause_held_job(&mut pipeline);
            wake_full_disk(&mut pipeline);
            assert!(disk_held(&pipeline, JOB));
        }
        resume_disk_held_job(&mut pipeline);
        assert!(
            !disk_held(&pipeline, JOB),
            "one resume lets it go (paused: {paused})"
        );
    }
}

// A held job its user paused takes no promise: the room a reading shows
// goes to the job beside it, which is not judged against room the paused
// one is not taking.
#[tokio::test]
async fn a_paused_held_job_is_promised_nothing() {
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
        hold_claim(&mut pipeline, job, &complete_dir, Some(60 * UNIT), false);
    }
    // The paused one is judged first, by its id.
    assert!(JOB < SECOND);
    pause_held_job(&mut pipeline);
    hears(&mut pipeline, &room, StorageRoot::Complete, 100 * UNIT);
    assert!(disk_held(&pipeline, JOB), "the paused one waits");
    assert!(!disk_held(&pipeline, SECOND), "the other goes");
    assert_eq!(promised_bytes(&pipeline), 60 * UNIT);
}

// A job held for a full disk goes back to its last checkpoint in what it
// keeps between an article's arrival and its placement: its cursors, its
// requeue counts and the releases it owed all go, a spooled part that
// checkpoint names stays on disk for the restore, and one it does not name,
// which nothing would read again, is removed from the full disk.
#[tokio::test]
async fn a_job_held_for_a_full_disk_keeps_only_the_spooled_parts_its_checkpoint_names() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let scenario = on_disk_posting(
        &room,
        vec![Post::plain("ember.bin", payload(4 * ARTICLE, 97))],
    );
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let spool = prepare_held_spool(&pipeline);
    // One part is spooled as the checkpoint is taken, one after it.
    let named = spool_part(&mut pipeline, &spool, "held-named", 2);
    checkpoint_disk_job(&mut pipeline);
    let stray = spool_part(&mut pipeline, &spool, "held-stray", 3);
    queue_spooled_releases(&mut pipeline);
    assert_eq!(held_spool_state(&pipeline).segments, 2);

    // The write that failed had ten bytes left to write.
    hold_write(&mut pipeline, 10);
    finish_spool_sweeps(&mut pipeline).await;
    // The five bytes the hold swept show as free at the next reading. They
    // are not room made: the job fills them again as it fetches that part.
    // A reading that shows them, or them and nine bytes more, lets nothing
    // go; one that shows the ten over them does.
    assert_eq!(full_disk_mark(&pipeline), Some(RESERVE + PLENTY));
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 5);
    assert_eq!(promised_bytes(&pipeline), 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 14);
    assert_eq!(promised_bytes(&pipeline), 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY + 15);
    assert_eq!(promised_bytes(&pipeline), 10);

    assert!(
        named.exists(),
        "the checkpoint's part stays for the restore"
    );
    assert!(!stray.exists(), "a part no checkpoint names is removed");
    assert!(!held_spool_state(&pipeline).has_file);
    assert!(held_spool_state(&pipeline).park_requeues == 0);
    assert!(held_spool_state(&pipeline).unanchored_requeues == 0);
    assert!(held_spool_state(&pipeline).pending_releases == 0);
    assert_eq!(
        held_spool_state(&pipeline).segments,
        0,
        "the spool is charged nothing"
    );
    assert_eq!(held_spool_state(&pipeline).bytes, 0);
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
    let staging = stage_on_disk(&mut pipeline, &library);
    assert!(!staging.starts_with(complete_directory(&pipeline)));
    let output = complete_ledger(&pipeline);
    let waits_before = (output.tickets(), working(&pipeline).tickets());

    room.store(0, Ordering::Release);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    drive_until_full_disk_held(&mut pipeline, &scenario).await;
    fill_after(None);
    assert!(full_disk_held(&pipeline));
    assert_eq!(
        (output.tickets(), working(&pipeline).tickets()),
        (waits_before.0 + 1, waits_before.1),
        "the job waits on the disk its write failed on"
    );
    park_disk_held_jobs(&mut pipeline);

    old_graph_heard(&mut pipeline).await;

    // The working disk reads room made: the job's own disk has said nothing
    // of more room.
    hears(&mut pipeline, &room, StorageRoot::Complete, 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    assert!(full_disk_held(&pipeline), "its own disk has not read");

    hears(&mut pipeline, &room, StorageRoot::Complete, 2 * PLENTY);
    assert!(!full_disk_held(&pipeline), "its disk has room");
    assert!(!output.waited_on());
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
    let complete_dir = complete_directory(&pipeline);

    drive_until_graph_mail(&mut pipeline, &scenario).await;
    assert!(
        active_download_queue_len(&pipeline) == Some(0),
        "every article is in"
    );
    let mut events = subscribe(&pipeline);
    let checkpointed = checkpointed_on_disk(&pipeline);
    let articles = posted_segments(&pipeline);
    assert!(
        checkpointed < articles,
        "the graph is ahead of its last checkpoint"
    );
    hold_on_root(&mut pipeline, StorageRoot::Intermediate);
    // Held with nothing queued and nothing out, it is not taken for a job
    // whose download is done.
    reconcile_disk_completion(&mut pipeline).await;
    park_disk_held_jobs(&mut pipeline);
    old_graph_heard(&mut pipeline).await;
    reconcile_disk_completion(&mut pipeline).await;
    assert_full_disk_held(&pipeline);
    let left = queued_articles(&pipeline);
    assert_eq!(
        left,
        articles - checkpointed,
        "parked, it counts as left every article its last checkpoint does not hold"
    );
    assert!(
        no_pending_completion_checks(&pipeline),
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
            PipelineEvent::DownloadFinished { .. } | PipelineEvent::DownloadPipelineDrained { .. }
        )),
        "held, its download is neither finished nor drained: {heard:?}"
    );
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    assert_eq!(
        active_download_queue_len(&pipeline),
        Some(left),
        "restored, it fetches what it showed left"
    );
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let articles = posted_segments(&pipeline);

    // Two articles are in and written when a checkpoint is taken; the write
    // of the next finds the disk full.
    serve_one(&mut pipeline, &scenario).await;
    serve_one(&mut pipeline, &scenario).await;
    read_disk_checkpoint(&mut pipeline).await;
    let stored = stored_disk_segments(&pipeline);
    assert!(
        (1..articles).contains(&stored),
        "the checkpoint holds some of the job: {stored} of {articles}"
    );
    fill_after(Some(0));
    until_full(&mut pipeline, &scenario).await;
    let left = queued_articles(&pipeline);
    assert_eq!(left, articles - stored);

    fill_after(None);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 2 * PLENTY);
    restarts(&mut pipeline).await;
    assert!(has_live_graph(&pipeline), "it was restored");

    hold_on_root(&mut pipeline, StorageRoot::Intermediate);
    park_disk_held_jobs(&mut pipeline);
    old_graph_heard(&mut pipeline).await;
    assert_full_disk_held(&pipeline);
    assert_eq!(
        queued_articles(&pipeline),
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

    drive_until_graph_mail(&mut pipeline, &scenario).await;
    hold_on_root(&mut pipeline, StorageRoot::Intermediate);
    assert!(
        has_retiring_graph(&pipeline),
        "the old graph still has something out"
    );
    let taken = retiring_graph_tally(&pipeline);

    recover_cancelled_checkpoint(&mut pipeline);
    assert_eq!(
        retiring_graph_tally(&pipeline),
        taken,
        "no checkpoint is taken of the graph the job was held away from"
    );
    old_graph_heard(&mut pipeline).await;
    assert!(full_disk_held(&pipeline));
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
        on_disk_probe(
            vec![Post::plain("ember.bin", body.clone())],
            RESERVE,
            Arc::new(move |_| {
                if gate.armed.swap(false, Ordering::AcqRel) {
                    gate.began.lock().unwrap().send(()).unwrap();
                    gate.go.lock().unwrap().recv().unwrap();
                }
                Ok(DiskSpace {
                    total_bytes: RESERVE * 8,
                    available_bytes: RESERVE + room.load(Ordering::Acquire),
                })
            }),
        )
    };
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let complete_dir = complete_directory(&pipeline);
    hears(&mut pipeline, &room, StorageRoot::Intermediate, 0);

    // A probe begins and is held open; the write fails while it runs.
    gate.armed.store(true, Ordering::Release);
    capacity(&pipeline).request_refresh(StorageRoot::Intermediate);
    began_rx.recv().unwrap();
    let running = capacity(&pipeline).probes_begun(StorageRoot::Intermediate);
    fill_after(Some(ARTICLE as u64 + ARTICLE as u64 / 2));
    until_full(&mut pipeline, &scenario).await;

    // The probe held open reads room and publishes it; the look the wait
    // asked for follows it and reads the same.
    room.store(2 * PLENTY, Ordering::Release);
    go_tx.send(()).unwrap();
    capacity(&pipeline).wait_for_reading_begun_after(StorageRoot::Intermediate, running);
    hear_disk_reading(&mut pipeline);
    assert_full_disk_held(&pipeline);
    assert_eq!(
        full_disk_mark(&pipeline),
        Some(RESERVE + 2 * PLENTY),
        "what the probe begun before the failure showed takes the mark's place"
    );

    // A byte over that mark is not the room of what was left of the file.
    let left = full_disk_rise(&pipeline).0.expect("what was left");
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
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
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
    let room = Arc::new(AtomicU64::new(PLENTY));
    refuse_by_quota(true);
    let (temp_dir, mut pipeline, scenario, _complete_dir, _body, left) =
        held_with_a_file_part_written(&room, RESERVE, 163).await;
    assert_eq!(full_disk_rise(&pipeline), (Some(left), true));

    drain(&mut pipeline).await;
    fill_after(Some(0));
    let (mut pipeline, _) = restart_as_it_stands(&temp_dir, pipeline, &scenario).await;
    assert!(!waiting(&pipeline), "a restored job is not held");
    until_full(&mut pipeline, &scenario).await;
    let (again, quota) = full_disk_rise(&pipeline);
    assert!(quota, "the restored job's hold is a quota hold");
    let again = again.expect("the write that failed knew what was left");
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    assert_full_disk_held(&pipeline);
    let mark = full_disk_mark(&pipeline).expect("a quota hold has a mark");

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
    finish_disk_job(&mut pipeline, &scenario).await;
    assert_eq!(status(&pipeline), Some(JobStatus::Complete));
}

// A job outside the pipeline, as a torrent's unpacking, that waits for room
// on a disk the pipeline's job waits on too. One reading wakes them all in
// the order they took their tickets from the disk's ledger: one that began
// to wait before the pipeline's job is asked while all three wait, and one
// that began after is asked once the pipeline's job no longer waits, alone.
// A job outside that begins to wait rings the pipeline's loop.
#[tokio::test]
async fn a_job_outside_the_pipeline_and_a_pipeline_job_waiting_wake_in_ticket_order() {
    let room = Arc::new(AtomicU64::new(0));
    let scenario = on_disk(&room);
    let (_temp_dir, mut pipeline, _) = admit(&scenario).await;
    let ledger = working(&pipeline);
    // One outside waiter takes its ticket before the pipeline's job, one
    // after.
    let early = ledger.wait();
    until_waiting(&mut pipeline, &scenario).await;
    let ticket = disk_ticket(&pipeline).expect("the job took a ticket");
    let late = ledger.wait();
    assert!(early < ticket && ticket < late);
    assert_eq!(ledger.waiters(), 3);

    let outside = outside_disk_waiters(&pipeline);
    let (before, after) = (92_201, 92_202);
    for (scope, ticket) in [(after, late), (before, early)] {
        outside.join(scope, ticket, &ledger);
    }
    let began = outside.disk_wait_began();
    outside.disk_held(after);
    outside.disk_held(before);
    began.await;
    let before_said = waits_said();
    reconcile_waits(&mut pipeline);
    assert!(
        outside.disk_waits_began().is_empty(),
        "the pipeline heard both begin"
    );
    assert_eq!(
        waits_said(),
        before_said + 2,
        "each wait said once, as the pipeline's own"
    );
    reconcile_waits(&mut pipeline);
    assert_eq!(
        waits_said(),
        before_said + 2,
        "a wait is said only as it begins"
    );
    assert!(waiting(&pipeline));

    // The one reading heard is one begun after the room came, so the pass
    // it starts finds room for the pipeline's job.
    hears(&mut pipeline, &room, StorageRoot::Intermediate, PLENTY);
    let asked = outside.wake_order();
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
    assert!(!graph_waiting(&pipeline), "the pipeline's job goes on");
    assert!(!outside.waits_for_disk());
    assert!(!waiting(&pipeline));
}

include!("disk_wait_move.rs");
