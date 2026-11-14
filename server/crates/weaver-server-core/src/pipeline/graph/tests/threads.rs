// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A pipeline whose volumes each have their own writer thread, as outside
// tests, with that thread held by the test: what the loop does while a
// job's bytes wait for the disk, and what it does once they are down.

use std::path::PathBuf;
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

// The loop never stood still for the volume's writer. Asked only where the
// test forces the answer: while the volume's thread is held, so a wait of
// the loop would never return, or after work the loop took no part in.
// Once the thread runs free the loop may read a file whose last write is
// still queued, and whether it waits then is up to the machine.
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
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
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
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
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
    retire_pipeline_database(pipeline).await;
}

// A run whose volume holds its budget of bytes for its writer says nothing
// more until the volume is below it: the run stands on its own thread, the
// loop never does, and the output is whole once the volume's thread goes on.
#[tokio::test]
async fn a_run_waits_on_its_own_thread_while_its_volume_holds_the_write_budget() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_411);
    let body = payload(3 * ARTICLE + 5, 6);
    let posts = vec![echo_volume(0, &body)];
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    // The job's volume is sized on this figure as the job is admitted.
    pipeline.volume_buffers.set_figure(ARTICLE as u64, u64::MAX);
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    // The run is kept at its start until every article is with the volume.
    let gate = Arc::new(crate::pipeline::graph::engine::echo::RunGate::default());
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            gate: Some(Arc::clone(&gate)),
            ..Echoes::default()
        }));
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) > ARTICLE as u64);
    gate.started().await;
    gate.release();
    // The run stands before the first thing it has to say.
    hold.wait_below_waiters(1);
    no_loop_waits(&volume);
    hold.open();
    drain_runs(&mut pipeline, job_id).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    assert_eq!(read(&working_dir.join("echo-0.mkv")), body);
    retire_pipeline_database(pipeline).await;
}

// A job whose working directory and delivered files lie on two volumes:
// each volume's own thread writes what lies on it, the loop waits for
// neither, and the delivered file is whole where it was staged.
#[tokio::test]
async fn a_job_on_two_volumes_is_written_by_each_volumes_own_thread() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    crate::runtime::fs::test_devices::assign(&intermediate, 1);
    crate::runtime::fs::test_devices::assign(&complete, 2);
    crate::runtime::fs::test_devices::assign(&movies, 3);
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        intermediate,
        complete,
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        0,
        None,
    )
    .await;
    pipeline.config.write().await.categories = vec![crate::categories::CategoryConfig {
        id: 0,
        name: "movies".to_string(),
        dest_dir: Some(movies.display().to_string()),
        aliases: String::new(),
    }];
    let job_id = JobId(92_412);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let mut spec = spec("lantern joined", &posts, ARTICLE);
    spec.category = Some("movies".to_string());
    let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
    pipeline
        .add_job(
            job_id,
            spec,
            PathBuf::from("lantern.nzb"),
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
    crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    let staging_dir = crate::pipeline::queue::staging_dir_for(&movies, job_id);
    assert_eq!(pipeline.jobs[&job_id].staging_dir, Some(staging_dir.clone()));
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let working = Arc::clone(disk.working().volume());
    let output = Arc::clone(disk.under(&staging_dir).volume());
    assert!(!Arc::ptr_eq(&working, &output));
    let holds = [
        pipeline.volume_writers.hold(&working),
        pipeline.volume_writers.hold(&output),
    ];
    for index in 0..posts.len() as u32 {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    // Both volumes hold what was handed them, and the loop went on.
    assert!(working.buffered_bytes.load(Ordering::Relaxed) > 0);
    assert_eq!(
        output.buffered_bytes.load(Ordering::Relaxed),
        data.len() as u64
    );
    no_loop_waits(&working);
    no_loop_waits(&output);
    for hold in &holds {
        hold.open();
    }
    disk.barrier().wait_aside();
    assert!(disk.barrier().passed());
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert!(working.writer.operations.load(Ordering::Relaxed) > 0);
    assert!(output.writer.operations.load(Ordering::Relaxed) > 0);
    assert_eq!(working.buffered_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(output.buffered_bytes.load(Ordering::Relaxed), 0);
    no_loop_waits(&working);
    no_loop_waits(&output);
    retire_pipeline_database(pipeline).await;
}

// A repair's output is said again by the loop itself, slice after slice in
// one step. Onto a volume that stands still it queues no more than the
// budget and a slice: the loop waits there, counted, and the repair is
// whole once the volume's thread goes on.
#[tokio::test]
async fn a_repair_larger_than_the_write_budget_is_applied_within_it() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_414);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    // The job's volume is sized on this figure as the job is admitted.
    const BUDGET: u64 = 1;
    pipeline.volume_buffers.set_figure(BUDGET, u64::MAX);
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    // Two articles apart are lost: the repair rebuilds two runs.
    for ordinal in [1, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [0, 2] {
        pipeline.book_terminal_segment(segment_id(job_id, 0, ordinal), SegmentTerminalState::Missing);
    }
    for index in 1..posts.len() as u32 {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let volume = Arc::clone(disk.working().volume());
    // The job's mail is heard until the repair is back. The volume is held
    // only once the repair's return is in hand and everything before it is
    // down, so what the return queues is all that waits behind the hold.
    let mut watcher = None;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::Repaired(returned) if matches!(returned.outcome, Some(RepairOutcome::Repaired { .. }))) {
            disk.barrier().wait_aside();
            let hold = pipeline.volume_writers.hold(&volume);
            let watched = Arc::clone(&volume);
            watcher = Some(std::thread::spawn(move || {
                // The loop stands on the budget with one run queued.
                hold.wait_below_waiters(1);
                let queued = watched.buffered_bytes.load(Ordering::Relaxed);
                hold.open();
                queued
            }));
        }
        pipeline.graph_run_said(said);
    }
    let watcher = watcher.expect("the repair came back");
    let queued = watcher.join().unwrap();
    assert!(queued >= BUDGET, "{queued}");
    assert!(queued <= BUDGET + ARTICLE as u64, "{queued}");
    assert!(volume.writer.loop_waits.load(Ordering::Relaxed) >= 1);
    disk.barrier().wait_aside();
    assert!(disk.barrier().passed());
    assert_eq!(read(&working_dir.join("lantern.bin")), data);
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
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
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
    retire_pipeline_database(pipeline).await;
}

// A job paused with a run out and nothing waiting for the disk asks its
// volume for no word. What the run said before the pause is heard after it
// and queues writes: the job is kept for them, is told when they are down,
// and goes then.
#[tokio::test]
async fn a_job_paused_with_a_quiet_disk_is_kept_for_what_its_run_said_before() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_415);
    let body = payload(3 * ARTICLE + 5, 6);
    let posts = [echo_volume(0, &body)];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    echo_engines(&mut pipeline, job_id, 1);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    // Everything the run says, to its end, is taken and not yet heard.
    let mut unheard = Vec::new();
    loop {
        let said = pipeline.graph_runs.recv().await.unwrap();
        let ended = matches!(said.heard, Heard::Ended);
        unheard.push(said);
        if ended {
            break;
        }
    }
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_aside();
    assert!(disk.quiet());
    pipeline.pause_job_runtime(job_id).unwrap();
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    let volume = Arc::clone(disk.working().volume());
    let hold = pipeline.volume_writers.hold(&volume);
    for said in unheard {
        pipeline.graph_run_said(said);
    }
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) > 0);
    // The run is over, and the last of the job's mail is heard: only the
    // writes that mail queued hold the job.
    while pipeline.retiring_graph_jobs[&job_id].awaits_mail() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert!(!disk.quiet());
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    // Ending the job queued its own last operations.
    disk.barrier().wait_aside();
    assert!(disk.quiet());
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
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    let status = job_status_for_assert(&pipeline, job_id).unwrap();
    assert!(
        matches!(&status, JobStatus::Failed { error } if error.contains("the disk said no")),
        "{status:?}"
    );
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
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
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
    pipeline.volume_buffers.set_figure(ARTICLE as u64, u64::MAX);
    let (volume, hold) = held(&pipeline, job_id);
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) >= ARTICLE as u64);
    assert!(pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_none());
    pipeline.download_dispatch_wake = false;
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
    hold.open();
    let report = pipeline.volume_reports.recv().await.unwrap();
    assert!(report.below);
    pipeline.volume_batch_returned(report);
    assert!(pipeline.download_dispatch_wake);
    assert!(!pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_some());
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

// A stop while a volume stands still with a job's bytes: the stop gives the
// volume its span and goes on, the last checkpoint says nothing of the
// bytes it never saw put down, and the restarted job fetches them again.
#[tokio::test]
async fn a_restart_after_a_stop_that_left_writes_held_fetches_them_again() {
    use crate::operations::disk::writer::{Leaping, SettleBound};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_413);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (working_dir, hold) = {
        let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
        let (volume, hold) = held(&pipeline, job_id);
        decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
        decode(&mut pipeline, job_id, &posts, 0, 1, true).await;
        let pending = pipeline.graph_jobs[&job_id].pending().clone();
        // A clock a minute later at each look: the volume's span is over
        // the first time the stop finds it standing still.
        pipeline.shutdown_settle = SettleBound {
            clock: Arc::new(Leaping(std::sync::atomic::AtomicU64::new(0))),
            span: std::time::Duration::from_secs(30),
        };
        let handle =
            crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        handle.shutdown().await.unwrap();
        pipeline.run().await;
        assert_eq!(pending.settled_bytes(), 0);
        assert_eq!(
            volume.buffered_bytes.load(Ordering::Relaxed),
            2 * ARTICLE as u64
        );
        assert!(!working_dir.join("lantern.bin").exists());
        retire_pipeline_database(pipeline).await;
        (working_dir, hold)
    };

    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline
        .restore_job(crate::RestoreJobRequest {
            job_id,
            job_hash: [0; 32],
            spec: spec("lantern joined", &posts, ARTICLE),
            detected_archives: Default::default(),
            file_identities: Default::default(),
            status: JobStatus::Downloading,
            download_state: None,
            post_state: None,
            run_state: None,
            queued_repair_at_epoch_ms: None,
            queued_extract_at_epoch_ms: None,
            paused_resume_status: None,
            paused_resume_download_state: None,
            paused_resume_post_state: None,
            working_dir: working_dir.clone(),
            restart: None,
        })
        .await
        .unwrap();
    assert!(pipeline.graph_jobs.contains_key(&job_id));
    // Every article is wanted again, the two the old volume never put down
    // among them.
    let queue = &pipeline.jobs[&job_id].articles.active().unwrap().download_queue;
    for ordinal in 0..3 {
        let id = segment_id(job_id, 0, ordinal);
        assert_eq!(
            queue.count_matching(|work| work.segment_id == id),
            1,
            "article {ordinal}"
        );
    }
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    drop(hold);
    retire_pipeline_database(pipeline).await;
}

// Stop the pipeline with a clock a minute later at each look, so a volume
// that stands still is given up the first time the stop finds it so.
async fn stop_past_a_still_volume(pipeline: &mut Pipeline) {
    use crate::operations::disk::writer::{Leaping, SettleBound};
    pipeline.shutdown_settle = SettleBound {
        clock: Arc::new(Leaping(std::sync::atomic::AtomicU64::new(0))),
        span: std::time::Duration::from_secs(30),
    };
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.shutdown().await.unwrap();
    pipeline.run().await;
}

// A stop while a run stands on its own thread for a volume that holds the
// write budget and does not move: the run goes on, says nothing more, and
// the stop does not wait for the volume past its span.
#[tokio::test]
async fn a_stop_does_not_wait_for_a_run_held_by_the_write_budget_of_a_still_volume() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_416);
    let body = payload(3 * ARTICLE + 5, 6);
    let posts = vec![echo_volume(0, &body)];
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    pipeline.volume_buffers.set_figure(ARTICLE as u64, u64::MAX);
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let gate = Arc::new(crate::pipeline::graph::engine::echo::RunGate::default());
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            gate: Some(Arc::clone(&gate)),
            ..Echoes::default()
        }));
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    gate.started().await;
    gate.release();
    hold.wait_below_waiters(1);
    let queued = volume.buffered_bytes.load(Ordering::Relaxed);
    assert!(queued > ARTICLE as u64);
    stop_past_a_still_volume(&mut pipeline).await;
    // The run said nothing more: no byte joined what the volume held.
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), queued);
    assert!(!working_dir.join("echo-0.mkv").exists());
    assert_ne!(
        job_status_for_assert(&pipeline, job_id).map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    retire_pipeline_database(pipeline).await;
    drop(hold);
}

// A stop while a repair stands on its own thread for writes a volume holds
// and does not put down: the repair ends as stopped, having read nothing,
// and the stop does not wait for the volume past its span.
#[tokio::test]
async fn a_stop_does_not_wait_for_a_repair_held_by_a_still_volume() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_417);
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
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let queued = volume.buffered_bytes.load(Ordering::Relaxed);
    assert!(queued > 0);
    stop_past_a_still_volume(&mut pipeline).await;
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), queued);
    assert!(!working_dir.join("lantern.bin").exists());
    // The repair was not taken back: the job stays at its last checkpoint.
    assert!(pipeline.graph_jobs[&job_id].pending().given_up());
    assert_ne!(
        job_status_for_assert(&pipeline, job_id).map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    retire_pipeline_database(pipeline).await;
    drop(hold);
}

// A stop while a repair stands on its own thread for writes its volume has
// not put down yet, on a volume that does put them down within the stop's
// span: the repair ends as stopped while the writes are still out, and is
// taken back once the stop has waited for them. The job is paged out and
// checkpointed as any other, not left at an older checkpoint.
#[tokio::test]
async fn a_stopped_repair_is_taken_back_once_its_volume_has_settled() {
    use crate::operations::disk::writer::{HandClock, SettleBound};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_418);
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
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) > 0);
    // The stop's clock stands still: the settle ends only as the volume's
    // writes come down. The volume is let go once the settle stands still
    // for it, which is after the repair came back stopped and was kept.
    pipeline.shutdown_settle = SettleBound {
        clock: Arc::new(HandClock::default()),
        span: std::time::Duration::from_secs(30),
    };
    let release = std::thread::spawn(move || {
        hold.wait_settling();
        hold.open();
        hold
    });
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.shutdown().await.unwrap();
    pipeline.run().await;
    let hold = release.join().unwrap();

    let graph = &pipeline.graph_jobs[&job_id];
    // The repair did not end by itself: the stop ended it, the job kept it,
    // and the stop took it back after the settle.
    assert_eq!(graph.stopped_repairs_taken_back(), 1);
    assert!(!graph.keeps_stopped_repair());
    assert!(!graph.pending().given_up(), "the job was checkpointed at the stop");
    assert!(graph.disk().unwrap().quiet());
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert!(working_dir.join("lantern.bin").exists());
    assert_ne!(
        job_status_for_assert(&pipeline, job_id).map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    retire_pipeline_database(pipeline).await;
    drop(hold);
}

// A repair that comes back while the owner is stopping is applied within
// the volume's limit like any other: the stop that sends producing threads
// on does not send the loop on, so the loop stands still for the volume
// with one run queued. When the stop's span ends with the volume still at
// its limit the applying ends there: nothing more is queued, the job does
// not fail, and it is left at its last written checkpoint.
#[tokio::test]
async fn a_repair_applied_during_a_stop_keeps_the_volumes_limit_until_the_stops_end() {
    use crate::operations::disk::writer::HandClock;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_419);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    const BUDGET: u64 = 1;
    pipeline.volume_buffers.set_figure(BUDGET, u64::MAX);
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    // Two articles apart are lost: the repair rebuilds two runs.
    for ordinal in [1, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [0, 2] {
        pipeline.book_terminal_segment(segment_id(job_id, 0, ordinal), SegmentTerminalState::Missing);
    }
    for index in 1..posts.len() as u32 {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let volume = Arc::clone(disk.working().volume());
    let clock = Arc::new(HandClock::default());
    let span = std::time::Duration::from_secs(30);
    // The repair's return is in hand and everything before it is down when
    // the stop is rung and the volume stands still.
    let mut watcher = None;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::Repaired(returned) if matches!(returned.outcome, Some(RepairOutcome::Repaired { .. }))) {
            disk.barrier().wait_aside();
            let hold = pipeline.volume_writers.hold(&volume);
            let writers = Arc::clone(&pipeline.volume_writers);
            writers.end_loop_waits_at(Arc::clone(&clock) as _, span);
            writers.stop_waits();
            let (watched, clock) = (Arc::clone(&volume), Arc::clone(&clock));
            watcher = Some(std::thread::spawn(move || {
                // The loop stands on the limit with one run queued, the
                // stop notwithstanding.
                hold.wait_below_waiters(1);
                let queued = watched.buffered_bytes.load(Ordering::SeqCst);
                // The stop's span ends with the volume still at its limit.
                clock.move_to(span);
                writers.look_again();
                (hold, queued)
            }));
        }
        pipeline.graph_run_said(said);
    }
    let (hold, queued) = watcher.expect("the repair came back").join().unwrap();
    assert!(queued >= BUDGET, "{queued}");
    assert!(queued <= BUDGET + ARTICLE as u64, "{queued}");
    // Nothing joined what the volume held once the span was over.
    assert_eq!(volume.buffered_bytes.load(Ordering::SeqCst), queued);
    assert!(volume.writer.loop_waits.load(Ordering::Relaxed) >= 1);
    let graph = &pipeline.graph_jobs[&job_id];
    assert!(graph.disk().unwrap().left_at_stop());
    assert!(graph.pending().given_up(), "the job is left at its last checkpoint");
    assert_ne!(
        job_status_for_assert(&pipeline, job_id).map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    hold.open();
    pipeline.volume_writers.settle();
    retire_pipeline_database(pipeline).await;
    drop(hold);
}

// Two jobs, one whose delivered files lie on a volume of their own. That
// volume at its own hard limit stops its job and no other: the job on the
// other volume is handed work, and the stopped one is again once its volume
// has drained.
#[tokio::test]
async fn a_job_on_a_full_volume_stops_while_a_job_on_another_volume_is_dispatched() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let temp_dir = tempfile::tempdir().unwrap();
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    crate::runtime::fs::test_devices::assign(&intermediate, 1);
    crate::runtime::fs::test_devices::assign(&complete, 2);
    crate::runtime::fs::test_devices::assign(&movies, 3);
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        intermediate,
        complete,
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        8,
        None,
    )
    .await;
    pipeline.config.write().await.categories = vec![crate::categories::CategoryConfig {
        id: 0,
        name: "movies".to_string(),
        dest_dir: Some(movies.display().to_string()),
        aliases: String::new(),
    }];
    let (slow_job, other_job) = (JobId(92_420), JobId(92_421));
    let posts = [Post::plain("lantern.bin", payload(4 * ARTICLE, 1))];
    let mut slow_spec = spec("lantern slow", &posts, ARTICLE);
    slow_spec.category = Some("movies".to_string());
    for (job_id, spec) in [
        (slow_job, slow_spec),
        (other_job, spec("lantern other", &posts, ARTICLE)),
    ] {
        let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
        pipeline
            .add_job(
                job_id,
                spec,
                PathBuf::from("lantern.nzb"),
                nzb_zstd,
                crate::jobs::AddJobOptions::default(),
            )
            .await
            .unwrap();
        crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    }
    let staging_dir = crate::pipeline::queue::staging_dir_for(&movies, slow_job);
    let disk = pipeline.graph_jobs[&slow_job].disk().unwrap().clone();
    let slow = Arc::clone(disk.under(&staging_dir).volume());
    let shared = Arc::clone(disk.working().volume());
    assert!(!Arc::ptr_eq(&slow, &shared));
    assert!(Arc::ptr_eq(
        &shared,
        pipeline.graph_jobs[&other_job].disk().unwrap().working().volume()
    ));
    // Each volume has its own limits, given as its first job was admitted.
    let (_, hard) = slow.buffer_limits();
    assert!(hard < u64::MAX);
    assert!(shared.buffer_limits().1 < u64::MAX);

    // The slow volume holds its hard limit; the other holds nothing.
    slow.buffered_bytes.store(hard, Ordering::SeqCst);
    assert_eq!(pipeline.volume_intake(slow_job), VolumeIntake::Stopped);
    assert_eq!(pipeline.volume_intake(other_job), VolumeIntake::Open);
    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("the job on the other volume is handed work");
    assert_eq!(lease.job_id, other_job);
    assert!(pipeline.volume_holds_budget(slow_job));

    // Drained, the slow volume's job is handed work again.
    slow.buffered_bytes.store(0, Ordering::SeqCst);
    assert_eq!(pipeline.volume_intake(slow_job), VolumeIntake::Open);
    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("the drained volume's job is handed work");
    assert_eq!(lease.job_id, slow_job);
    retire_pipeline_database(pipeline).await;
}

// A volume shares the buffer pool only while a job's disk writes to it.
// Two volumes have the full figure each. A job on a third makes all three
// share, for as long as any copy of that job's disk lives. When the last
// copy is dropped the third volume shares nothing any more: the two still
// written to have the full figure again, and the pipeline's next tick is
// told that a limit changed.
#[tokio::test]
async fn a_volume_shares_the_buffer_pool_only_while_a_job_writes_to_it() {
    use crate::pipeline::graph::disk::job_disk;
    const FIGURE: u64 = 256 * 1024 * 1024;
    let temp_dir = tempfile::tempdir().unwrap();
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    crate::runtime::fs::test_devices::assign(&intermediate, 1);
    crate::runtime::fs::test_devices::assign(&complete, 2);
    crate::runtime::fs::test_devices::assign(&movies, 3);
    let (pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        intermediate.clone(),
        complete.clone(),
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        8,
        None,
    )
    .await;
    pipeline.volume_buffers.set_figure(FIGURE, u64::MAX);
    let disk_for = |key: u64, working: &std::path::Path, output: &std::path::Path| {
        job_disk(
            &pipeline.volume_writers,
            &pipeline.storage_capacity,
            &pipeline.volume_probes,
            key,
            working,
            Some(output),
            &pipeline.volume_buffers,
        )
    };
    let hard = |disk: &crate::pipeline::graph::disk::JobDisk, under: &std::path::Path| {
        disk.under(under).volume().buffer_limits().1
    };
    let (work_a, out_a) = (intermediate.join("a"), complete.join("a"));
    let first = disk_for(1, &work_a, &out_a);
    assert!(!Arc::ptr_eq(first.working().volume(), first.under(&out_a).volume()));
    assert_eq!((hard(&first, &work_a), hard(&first, &out_a)), (FIGURE, FIGURE));
    let _ = pipeline.volume_buffers.follow_rates();

    // A second job on the same two volumes changes nothing.
    let same = disk_for(2, &intermediate.join("b"), &complete.join("b"));
    assert_eq!((hard(&first, &work_a), hard(&first, &out_a)), (FIGURE, FIGURE));
    assert!(!pipeline.volume_buffers.follow_rates());

    // A job whose output lies on a third volume: the three share.
    let out_c = movies.join("c");
    let third = disk_for(3, &intermediate.join("c"), &out_c);
    let shared = hard(&first, &work_a);
    assert!(shared < FIGURE, "{shared}");
    assert!(hard(&third, &out_c) < FIGURE);
    assert!(pipeline.volume_buffers.follow_rates());

    // A copy of the job's disk still lives: the volume is still written to.
    let copy = third.clone();
    drop(third);
    assert_eq!(hard(&first, &work_a), shared);
    assert!(!pipeline.volume_buffers.follow_rates());

    // The last copy is gone: the third volume shares nothing any more.
    drop(copy);
    assert_eq!((hard(&first, &work_a), hard(&first, &out_a)), (FIGURE, FIGURE));
    assert!(pipeline.volume_buffers.follow_rates(), "the pipeline hears that a limit rose");
    assert!(!pipeline.volume_buffers.follow_rates());

    // A job leaving volumes others still write to changes nothing.
    drop(same);
    assert_eq!((hard(&first, &work_a), hard(&first, &out_a)), (FIGURE, FIGURE));
    assert!(!pipeline.volume_buffers.follow_rates());
    drop(first);
    retire_pipeline_database(pipeline).await;
}

// One lane asks for work, and holds what it is handed as a dispatch books
// it. The job of each article handed.
fn lane_asks(pipeline: &mut Pipeline, lane_id: u64) -> Vec<JobId> {
    use crate::pipeline::download::scheduler::{Handout, LaneShare};
    let pressure = pipeline.refresh_download_pressure();
    let lane_mode = pipeline.download_lane_mode_for_server(0, pressure, true);
    let want = pipeline.download_refill_want(lane_mode);
    assert!(want > 1, "{want}");
    // A pipe as deep as one ask.
    let lane = LaneShare {
        lane_id,
        depth: want,
    };
    let spill = pipeline.spill_job_in_flight_on(0);
    let works = match pipeline.next_works_for_lane(0, want, Some(lane), spill, pressure) {
        Handout::Works(works) => works,
        Handout::Idle => panic!("the lane is handed work, and was told there is none"),
        Handout::Yield(_) => panic!("the lane is handed work, and was told to yield"),
        Handout::Saturated(wake) => panic!("the lane is handed work, and was held: {wake:?}"),
    };
    let jobs = works
        .iter()
        .map(|work| work.segment_id.file_id.job_id)
        .collect();
    let lease = pipeline
        .lease_for_handout(lane_id, 0, lane_mode, pressure, works)
        .expect("the handout is leased");
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    jobs
}

// From a volume's soft limit a lane fetching for its job is handed one
// article at a time, and a full handout again once the volume is below it.
// A lane that holds its one and asks again, with no other job to hand from,
// is handed one more: the job is not passed by for nobody.
#[tokio::test]
async fn a_volume_at_its_soft_limit_cuts_a_lanes_ask_to_one_article() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_422);
    let posts = [Post::plain("lantern.bin", payload(8 * ARTICLE, 1))];
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_with_backlog(
        &temp_dir,
        16 * 1024 * 1024,
        8,
    )
    .await;
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    let volume = Arc::clone(pipeline.graph_jobs[&job_id].disk().unwrap().working().volume());
    let (soft, hard) = volume.buffer_limits();
    assert!(soft < hard && hard < u64::MAX);

    // What is being decoded for the volume's jobs reaches its soft limit.
    volume.arrived_bytes.store(soft, Ordering::SeqCst);
    assert_eq!(pipeline.volume_intake(job_id), VolumeIntake::OneAtATime);
    let lease = pipeline.lease_for_server_for_test(0).unwrap();
    assert_eq!(lease.works.len(), 1);
    let lane_id = Pipeline::next_download_lane_id();
    assert_eq!(lane_asks(&mut pipeline, lane_id), [job_id]);
    assert_eq!(lane_asks(&mut pipeline, lane_id), [job_id]);
    assert_eq!(pipeline.download_lane_holdings_of_job(lane_id, job_id), 2);

    volume.arrived_bytes.store(soft - 1, Ordering::SeqCst);
    assert_eq!(pipeline.volume_intake(job_id), VolumeIntake::Open);
    let lease = pipeline.lease_for_server_for_test(0).unwrap();
    assert!(lease.works.len() > 1, "{}", lease.works.len());
    volume.arrived_bytes.store(0, Ordering::SeqCst);
    retire_pipeline_database(pipeline).await;
}

// A volume's soft line holds its own job to one article on a lane, not the
// lane: the lane's remaining room goes to the next job, on a volume that is
// open. The lane is handed one article of the job at the soft line; asked
// again while it holds that one, it is handed the other job's.
#[tokio::test]
async fn a_volume_at_its_soft_limit_leaves_the_lanes_room_to_jobs_on_other_volumes() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let temp_dir = tempfile::tempdir().unwrap();
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    crate::runtime::fs::test_devices::assign(&intermediate, 1);
    crate::runtime::fs::test_devices::assign(&complete, 2);
    crate::runtime::fs::test_devices::assign(&movies, 3);
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        intermediate,
        complete,
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        8,
        None,
    )
    .await;
    pipeline.config.write().await.categories = vec![crate::categories::CategoryConfig {
        id: 0,
        name: "movies".to_string(),
        dest_dir: Some(movies.display().to_string()),
        aliases: String::new(),
    }];
    let (slow_job, other_job) = (JobId(92_424), JobId(92_425));
    let posts = [Post::plain("lantern.bin", payload(8 * ARTICLE, 1))];
    let mut slow_spec = spec("lantern slow", &posts, ARTICLE);
    slow_spec.category = Some("movies".to_string());
    for (job_id, spec) in [
        (slow_job, slow_spec),
        (other_job, spec("lantern other", &posts, ARTICLE)),
    ] {
        let nzb_zstd = crate::pipeline::tests::nzb_zstd_for(&spec);
        pipeline
            .add_job(
                job_id,
                spec,
                PathBuf::from("lantern.nzb"),
                nzb_zstd,
                crate::jobs::AddJobOptions::default(),
            )
            .await
            .unwrap();
        crate::pipeline::tests::activate_job(&mut pipeline, job_id).await;
    }
    // The first job in the queue is the one whose own volume is slow.
    assert_eq!(pipeline.download_scheduler_eligible_jobs(), [slow_job, other_job]);
    let staging_dir = crate::pipeline::queue::staging_dir_for(&movies, slow_job);
    let disk = pipeline.graph_jobs[&slow_job].disk().unwrap().clone();
    let slow = Arc::clone(disk.under(&staging_dir).volume());
    let shared = Arc::clone(disk.working().volume());
    assert!(!Arc::ptr_eq(&slow, &shared));
    let (soft, _) = slow.buffer_limits();
    slow.arrived_bytes.store(soft, Ordering::SeqCst);
    assert_eq!(pipeline.volume_intake(slow_job), VolumeIntake::OneAtATime);
    assert_eq!(pipeline.volume_intake(other_job), VolumeIntake::Open);

    let lane_id = Pipeline::next_download_lane_id();
    // Holding nothing, the lane is handed one article of the first job.
    assert_eq!(lane_asks(&mut pipeline, lane_id), [slow_job]);
    assert_eq!(pipeline.download_lane_holdings_of_job(lane_id, slow_job), 1);
    // Holding that one, its room goes to the job on the open volume.
    let rest = lane_asks(&mut pipeline, lane_id);
    assert!(rest.len() > 1, "{rest:?}");
    assert!(rest.iter().all(|job_id| *job_id == other_job), "{rest:?}");
    assert_eq!(pipeline.download_lane_holdings_of_job(lane_id, slow_job), 1);
    // A lane that holds none of the first job is handed its one.
    let second = Pipeline::next_download_lane_id();
    assert_eq!(lane_asks(&mut pipeline, second), [slow_job]);

    slow.arrived_bytes.store(0, Ordering::SeqCst);
    retire_pipeline_database(pipeline).await;
}

// What arrived for a job and waits for the loop counts on its volume. At
// the volume's hard limit the job is handed nothing, and the hand-on that
// brings the volume to its line wakes dispatch: the loop takes the wake
// itself, with no writer in it.
#[tokio::test]
async fn a_job_stopped_by_what_is_being_decoded_is_woken_as_the_loop_hands_it_on() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_423);
    let posts = [Post::plain("lantern.bin", payload(4 * ARTICLE, 1))];
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_with_backlog(
        &temp_dir,
        16 * 1024 * 1024,
        8,
    )
    .await;
    insert_active_job(&mut pipeline, job_id, spec("lantern joined", &posts, ARTICLE)).await;
    pipeline.volume_buffers.set_figure(2 * ARTICLE as u64, u64::MAX);
    let volume = Arc::clone(pipeline.graph_jobs[&job_id].disk().unwrap().working().volume());
    assert_eq!(volume.buffer_limits().1, 2 * ARTICLE as u64);

    for _ in 0..2 {
        pipeline.note_released_download_result_pending(job_id, ARTICLE as u64);
    }
    assert_eq!(volume.arrived_bytes.load(Ordering::SeqCst), 2 * ARTICLE as u64);
    assert!(pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_none());
    pipeline.download_dispatch_wake = false;

    // Both results are handed on. The first leaves the volume above its
    // line or wakes dispatch; the second leaves nothing and has woken it.
    pipeline.finish_released_download_result_processing(job_id, ARTICLE as u64);
    pipeline.finish_released_download_result_processing(job_id, ARTICLE as u64);
    assert_eq!(volume.arrived_bytes.load(Ordering::SeqCst), 0);
    assert!(pipeline.volume_arrived_by_job.is_empty());
    assert!(pipeline.download_dispatch_wake);
    assert!(!pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_some());
    retire_pipeline_database(pipeline).await;
}

// Another hardware profile in force sizes every volume's limits again, on
// that profile's figure.
#[tokio::test]
async fn a_profile_change_sizes_the_volume_limits_again() {
    use crate::pipeline::orchestrator::volume_buffer_figure;
    use crate::runtime::HardwareProfile;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_426);
    let posts = [Post::plain("lantern.bin", payload(ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let volume = Arc::clone(pipeline.graph_jobs[&job_id].disk().unwrap().working().volume());
    // The machine is the one the test pipeline was built on, never the host,
    // and the figure is the product's own, from that machine.
    let machine = pipeline.tuner.system_profile().clone();
    let connections = pipeline.tuner.connection_limit();
    let figure = |profile| volume_buffer_figure(profile, &machine, connections).0;
    // The test pipeline's machine has four cores and 8 GiB: the balanced
    // profile is in force, and the efficient one asks nothing of a machine.
    let before = pipeline.configured_hardware_profile;
    assert_eq!(before, HardwareProfile::Balanced);
    let other = HardwareProfile::Efficient;
    let (was, will) = (figure(before), figure(other));
    assert_ne!(was, will, "the pipeline's machine sizes these two profiles apart");
    assert_eq!(volume.buffer_limits(), (was * 70 / 100, was));

    pipeline.set_configured_hardware_profile(other);

    assert_eq!(volume.buffer_limits(), (will * 70 / 100, will));
    retire_pipeline_database(pipeline).await;
}
