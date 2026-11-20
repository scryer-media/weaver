// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_archive::*;
use super::disk_reserve::*;
use super::harness::JOB;
use super::harness::{ARTICLE, Post, admit, payload};
use std::sync::atomic::Ordering;
use std::sync::{Arc, atomic::AtomicU64};
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_pipeline::pipeline::tests::engines::stream_fixtures::xz;
use weaver_server_pipeline::pipeline::tests::{
    finish_cancel_jobs,
    queue_fixture::{apply_intents, handle},
};
use weaver_server_pipeline::{JobHistoryRow, jobs::model::JobStatus, operations::StorageRoot};
use weaver_server_pipeline::{JobId, jobs::model::RunState};
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
