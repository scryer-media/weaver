// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A pipeline whose volumes each have their own writer thread, as outside
// tests, with that thread held by the test: what the loop does while a
// job's bytes wait for the disk, and what it does once they are down.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::*;
use crate::operations::disk::volume::Volume;
use crate::operations::disk::writer::Hold;
use crate::pipeline::graph::disk::threads;
use crate::pipeline::tests::{job_status_for_assert, settle_failed_releases};

// The volume the job writes to and a hold on its thread.
fn held(pipeline: &Pipeline, job_id: JobId) -> (Arc<Volume>, Hold) {
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .expect("the job writes through its volume's writer")
            .working()
            .volume(),
    );
    let hold = pipeline.volume_writers.hold(&volume);
    (volume, hold)
}

// The loop never stood still for the volume's writer.
fn no_loop_waits(volume: &Volume) {
    assert_eq!(volume.writer.loop_waits.load(Ordering::Relaxed), 0);
}

// Hear the job's mail until its stopped graph is gone.
async fn hear_retirement(pipeline: &mut Pipeline, job_id: JobId) {
    while pipeline.retiring_graph_jobs.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
}

// Every article of a file is taken while the volume's thread stands still:
// the loop is never held up by it, the bytes wait in memory, and they are
// on disk once the thread goes on.
#[tokio::test]
async fn a_held_volume_does_not_stop_the_loop() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_400);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    assert_eq!(
        volume.buffered_bytes.load(Ordering::Relaxed),
        posts[0].bytes.len() as u64
    );
    assert!(!matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    let barrier = pipeline.graph_jobs[&job_id].disk().unwrap().barrier();
    assert!(!barrier.returned());
    hold.open();
    barrier.wait_aside();
    assert!(barrier.passed());
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A write that fails on the volume's thread is heard in the job's mail and
// fails the job, once.
#[tokio::test]
async fn a_write_that_fails_on_a_volumes_thread_fails_the_job() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_401);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    hold.fail_next(std::io::Error::other("the disk said no"));
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.open();
    while !matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(
        matches!(&status, JobStatus::Failed { error } if error.contains("the disk said no")),
        "{status:?}"
    );
    settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.jobs.contains_key(&job_id));
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A delivery that stepped back for the job's writes, one of which then
// fails, does not go on: the job fails with the write's own error, once.
#[tokio::test]
async fn a_delivery_that_waited_for_a_failed_write_fails_the_job_once() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_407);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    hold.fail_next(std::io::Error::other("the disk said no"));
    let mut events = pipeline.event_tx.subscribe();
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    pipeline.start_move_to_complete(job_id).await.unwrap();
    assert!(pipeline.moves_awaiting_disk.contains(&job_id));
    assert!(!pipeline.inflight_moves.contains(&job_id));
    hold.open();
    while !matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(
        matches!(&status, JobStatus::Failed { error } if error.contains("the disk said no")),
        "{status:?}"
    );
    settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.moves_awaiting_disk.contains(&job_id));
    assert!(!pipeline.inflight_moves.contains(&job_id));
    pipeline.db.flush_write_queue().await.unwrap();
    let failed = std::iter::from_fn(|| events.try_recv().ok())
        .filter(|event| matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id))
        .count();
    assert_eq!(failed, 1);
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A repair lent while the bytes it reads still wait for the disk does not
// hold the loop: it waits on its own thread, reads the file once the
// volume's thread has put it down, and mends it.
#[tokio::test]
async fn a_repair_lent_while_its_bytes_are_queued_waits_on_its_own_thread() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_408);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode(&mut pipeline, job_id, &posts, 0, 0, false).await;
    for ordinal in 1..4 {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for index in 1..posts.len() as u32 {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(
        pipeline.graph_jobs[&job_id].repair_out(),
        "the repair went out while the volume stood still"
    );
    assert!(!pipeline.graph_jobs[&job_id].disk().unwrap().quiet());
    no_loop_waits(&volume);
    hold.open();
    hear_repair(&mut pipeline, job_id).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    assert_eq!(read(&working_dir.join("lantern.bin")), data);
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// The rename that delivers a file is made by the thread of the volume it
// lands on, once the job's own writes are down, and never by the loop.
#[tokio::test]
async fn a_delivery_is_renamed_by_the_thread_of_the_volume_it_lands_on() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_409);
    let posts = [Post::plain("lantern.bin", payload(2 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    // The delivery steps back until the job's writes and closes returned.
    pipeline.start_move_to_complete(job_id).await.unwrap();
    while !pipeline.inflight_moves.contains(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
        pipeline.start_move_to_complete(job_id).await.unwrap();
    }
    // Everything of the job's has run, and the move's task has yet to.
    let before = volume.writer.operations.load(Ordering::Relaxed);
    let done = pipeline.move_done_rx.recv().await.unwrap();
    assert!(volume.writer.operations.load(Ordering::Relaxed) > before);
    pipeline.handle_move_to_complete_done(done).await;
    assert!(!working_dir.join("lantern.bin").exists());
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// An archive volume whose bytes still wait for the disk when its engine's
// run starts: the run waits for them on its own thread, reads the volume
// once the volume's thread has put it down, and its output is whole.
#[tokio::test]
async fn a_run_reads_its_volume_once_the_volumes_thread_put_it_down() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_410);
    let body = payload(3 * ARTICLE + 5, 6);
    let posts = vec![echo_volume(0, &body)];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    assert!(pipeline.graph_jobs[&job_id].runs().live());
    no_loop_waits(&volume);
    hold.open();
    drain_runs(&mut pipeline, job_id).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    assert_eq!(read(&working_dir.join("echo-0.mkv")), body);
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A job paused while its bytes wait for the disk keeps its graph until the
// volume's thread has put them down, and they are put down.
#[tokio::test]
async fn a_job_paused_with_bytes_queued_is_kept_until_they_are_written() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_402);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    decode(&mut pipeline, job_id, &posts, 0, 1, true).await;
    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(!pipeline.graph_jobs.contains_key(&job_id));
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    assert_eq!(
        read(&working_dir.join("lantern.bin"))[..2 * ARTICLE],
        posts[0].bytes[..2 * ARTICLE]
    );
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Paused)
    ));
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A write of a paused job that fails after the pause still fails the job:
// the failure is heard while the stopped graph waits for its volume.
#[tokio::test]
async fn a_write_that_fails_after_its_job_stopped_fails_the_job() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_403);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    hold.fail_next(std::io::Error::other("the disk said no"));
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(
        matches!(&status, JobStatus::Failed { error } if error.contains("the disk said no")),
        "{status:?}"
    );
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A job cancelled while its bytes wait for the disk is not ended until the
// volume's thread has let go of its files; nothing queued for them is
// written after the cancel.
#[tokio::test]
async fn a_job_cancelled_with_bytes_queued_ends_once_its_volume_let_go() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_404);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.wait_held();
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.cancel_job(job_id).await.unwrap();
    pipeline.apply_queue_intents().await;
    match pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    {
        crate::pipeline::TerminalPostProcessingEvent::CancelJobsDone(done) => {
            pipeline.handle_cancel_jobs_done(done)
        }
        _ => panic!("expected cancellation archive reply"),
    }
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    assert!(pipeline.jobs.contains_key(&job_id));
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert!(!working_dir.join("lantern.bin").exists());
    assert_eq!(
        pipeline
            .db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A volume that holds the write budget stops dispatch for its job. The
// batch that brings it below the line wakes the loop, and the job is
// handed work again.
#[tokio::test]
async fn a_job_held_by_a_full_write_buffer_is_dispatched_again_once_woken() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_405);
    let posts = [Post::plain("lantern.bin", payload(4 * ARTICLE, 1))];
    // A pipeline with a server to hand work to.
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_with_backlog(
        &temp_dir,
        16 * 1024 * 1024,
        8,
    )
    .await;
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    pipeline.write_backlog_budget_bytes = ARTICLE;
    let (volume, hold) = held(&pipeline, job_id);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) >= ARTICLE as u64);
    assert!(pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_none());
    pipeline.download_dispatch_wake = false;
    hold.open();
    let report = pipeline.volume_reports.recv().await.unwrap();
    assert!(report.below);
    pipeline.volume_batch_returned(report);
    assert!(pipeline.download_dispatch_wake);
    assert!(!pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_some());
    no_loop_waits(&volume);
    retire_pipeline_database(pipeline).await;
}

// A graceful stop puts down what the volumes hold before the last
// checkpoint, and that checkpoint says the bytes are down.
#[tokio::test]
async fn a_graceful_stop_drains_the_volumes_and_then_checkpoints() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_406);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    decode(&mut pipeline, job_id, &posts, 0, 1, true).await;
    let pending = pipeline.graph_jobs[&job_id].pending().clone();
    assert_eq!(pending.settled_bytes(), 0);
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.shutdown().await.unwrap();
    hold.open();
    pipeline.run().await;
    assert_eq!(
        read(&working_dir.join("lantern.bin"))[..2 * ARTICLE],
        posts[0].bytes[..2 * ARTICLE]
    );
    assert_eq!(pending.settled_bytes(), 2 * ARTICLE as u64);
    // The loop's waits are not counted here: a test build reads every
    // article back from disk as each checkpoint is taken, to check what the
    // checkpoint names, and the last checkpoint is taken while the writes
    // it waits for may still be queued.
    let _ = volume;
    retire_pipeline_database(pipeline).await;
}
