// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A pipeline whose volumes each have their own writer thread, as outside
// tests, with that thread held by the test: what the loop does while a
// job's bytes wait for the disk, and what it does once they are down.

use crate::history::queries::HistoryQueries;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::*;
use crate::pipeline::graph::disk::VolumeLeases;
use crate::pipeline::graph::disk::threads;
use crate::pipeline::tests::{job_status_for_assert, settle_failed_releases};
use weaver_server_disk::operations::disk::volume::Volume;
use weaver_server_disk::operations::disk::writer::Hold;

mod landing;

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
    assert!(!barrier.unwritten());
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    // What a reader waits for is placed; the operations also returned,
    // and no refusal waits to be heard.
    barrier.wait_returned();
    assert!(barrier.passed());
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
    assert!(pipeline.moves_awaiting_disk.contains_key(&job_id));
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
    assert!(!pipeline.moves_awaiting_disk.contains_key(&job_id));
    assert!(!pipeline.inflight_moves.contains(&job_id));
    pipeline.db.flush_write_queue().await.unwrap();
    let failed = std::iter::from_fn(|| events.try_recv().ok())
        .filter(
            |event| matches!(event, PipelineEvent::JobFailed { job_id: id, .. } if *id == job_id),
        )
        .count();
    assert_eq!(failed, 1);
    retire_pipeline_database(pipeline).await;
}

// A delivery that stepped back for the job's writes is not started again
// by mail of the job's other operations, a checkpoint's, while its own are
// still out: only once the barrier it waits on returned.
#[tokio::test]
async fn a_waiting_delivery_looks_again_only_when_its_own_barrier_returned() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_408);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    pipeline.start_move_to_complete(job_id).await.unwrap();
    let barrier = pipeline.moves_awaiting_disk[&job_id].clone();
    assert!(!barrier.returned());
    pipeline.remove_pending_completion_check(job_id);

    // A checkpoint's mail while the volume stands still.
    pipeline.graph_disk_returned(job_id);
    assert!(pipeline.moves_awaiting_disk.contains_key(&job_id));
    assert!(!pipeline.pending_completion_checks.contains(&job_id));
    no_loop_waits(&volume);

    hold.open();
    while pipeline.moves_awaiting_disk.contains_key(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
    }
    assert!(barrier.passed());
    assert!(pipeline.pending_completion_checks.contains(&job_id));
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
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

// A run whose output fills its volume's budget says nothing more until the
// volume is below it: the run stands on its own thread, the loop never
// does, and the output is whole once the volume's thread goes on.
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
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
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
    // The archive is down before the volume stands still: the run reads
    // it, and stands only for room for what it says.
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    let (volume, hold) = held(&pipeline, job_id);
    let downloaded = volume.writer.loop_waits.load(Ordering::Relaxed);
    gate.started().await;
    gate.release();
    // The run counts its output up to the limit, and stands for the rest.
    hold.wait_below_waiters(1);
    assert_eq!(volume.unwritten_bytes(), ARTICLE as u64);
    assert_eq!(volume.writer.loop_waits.load(Ordering::Relaxed), downloaded);
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
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
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
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
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
    assert_eq!(
        pipeline.jobs[&job_id].staging_dir,
        Some(staging_dir.clone())
    );
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
    assert!(!disk.barrier().unwritten());
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    // Each volume's thread wrote: counted as each write call returned,
    // before what it wrote was placed. The operation counts are added
    // after a batch and may not be yet.
    assert!(working.written_bytes.load(Ordering::Relaxed) > 0);
    assert!(output.written_bytes.load(Ordering::Relaxed) > 0);
    assert_eq!(working.buffered_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(output.buffered_bytes.load(Ordering::Relaxed), 0);
    no_loop_waits(&working);
    no_loop_waits(&output);
    retire_pipeline_database(pipeline).await;
}

// A repair's output is read back on its own thread, slice after slice, each
// paid for on the volume it lands on before its mail. Onto a volume that
// stands still it counts no more than the budget and a slice: the reader
// stands there, the loop never does, and the repair is whole once the
// volume's thread goes on.
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
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    // Two articles apart are lost: the repair rebuilds two runs.
    for ordinal in [1, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [0, 2] {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
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
    let mut downloaded = 0;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::Repaired(returned) if matches!(returned.outcome, Some(RepairOutcome::Repaired { .. })))
        {
            disk.barrier().wait_aside();
            // What the downloads waited, before the repair.
            downloaded = budget_waits(&volume);
            let hold = pipeline.volume_writers.hold(&volume);
            let watched = Arc::clone(&volume);
            watcher = Some(std::thread::spawn(move || {
                // The reader stands on the budget with one slice counted.
                hold.wait_below_waiters(1);
                let counted = watched.unwritten_bytes();
                hold.open();
                counted
            }));
        }
        pipeline.graph_run_said(said);
    }
    let watcher = watcher.expect("the repair came back");
    let counted = watcher.join().unwrap();
    assert!(counted >= BUDGET, "{counted}");
    assert!(counted <= BUDGET + ARTICLE as u64, "{counted}");
    assert_eq!(
        budget_waits(&volume) - downloaded,
        0,
        "the loop stood still for the volume"
    );
    disk.barrier().wait_aside();
    assert!(!disk.barrier().unwritten());
    assert_eq!(read(&working_dir.join("lantern.bin")), data);
    // The repair's output is written: nothing is carried for it any more,
    // though the job lives on.
    assert_eq!(volume.carried_bytes(), 0);
    assert!(pipeline.graph_jobs.contains_key(&job_id));
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
    // Quiet asks what returned, which follows what a reader is shown.
    disk.barrier().wait_returned();
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
    // Quiet asks what returned, which follows what a reader is shown.
    disk.barrier().wait_returned();
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

// A job that fails while its volume's thread still holds operations on its
// files keeps them, unlike a cancelled one: a failed job keeps what it
// delivered, so the bytes queued for its file are written and a removal
// queued for a file that no longer belongs to it runs, both before its
// working directory is released.
#[tokio::test]
async fn a_job_failed_with_operations_queued_runs_them_before_its_release() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_416);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let leftover = working_dir.join("lantern.half");
    std::fs::write(&leftover, b"half of a decode").unwrap();
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    let removed = leftover.clone();
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .working()
        .run(None, leftover.clone(), move || {
            std::fs::remove_file(&removed)
        });
    hold.wait_held();
    pipeline.fail_job(job_id, "no file was delivered whole".to_string());
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    // Asked while the volume stands still: a wait of the loop would not
    // have come back.
    no_loop_waits(&volume);
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    settle_failed_releases(&mut pipeline).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assert!(!leftover.exists(), "the queued removal did not run");
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    retire_pipeline_database(pipeline).await;
}

// A job that fails while a move waits on a checkpoint that itself waits for
// the volume's writer: that checkpoint is never written now, and the move
// hears so at once, so the job's retirement ends once the writer returns
// instead of waiting for word of a commit that never comes.
#[tokio::test]
async fn a_job_failed_with_a_commit_owed_behind_its_writer_still_settles() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_417);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.wait_held();
    let mail = pipeline.graph_runs.written();
    // The checkpoint taken so far commits, so nothing of the job is on its
    // way to the database.
    let first = pipeline.graph_jobs[&job_id].pending().stamp();
    if pipeline.graph_jobs[&job_id]
        .pending()
        .await_commit(first, job_id.0, &mail, &pipeline.db)
        .is_none()
    {
        loop {
            let said = pipeline.graph_runs.recv().await.unwrap();
            let written = matches!(said.heard, Heard::Written { .. });
            pipeline.graph_run_said(said);
            if written {
                break;
            }
        }
    }
    // The next waits for the held writer, and a move waits on its commit,
    // as `checkpoint_graph_job` leaves one.
    pipeline.flush_graph_checkpoint(job_id);
    {
        let graph = pipeline.graph_jobs.get_mut(&job_id).unwrap();
        let pending = graph.pending().clone();
        assert!(pending.gated());
        assert!(
            pending
                .await_commit(pending.stamp(), job_id.0, &mail, &pipeline.db)
                .is_none()
        );
        graph.owe_mail(pending.stamp());
    }
    pipeline.fail_job(job_id, "no file was delivered whole".to_string());
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    no_loop_waits(&volume);
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    settle_failed_releases(&mut pipeline).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    retire_pipeline_database(pipeline).await;
}

// A job stopped while its last checkpoint waits for the volume's writer, as
// one held for a full disk is, waits on that checkpoint's commit. A cancel
// then stops its checkpoints, so the commit never comes: the stopped graph
// hears so at once and goes once the writer returns, and the cancel can
// end.
#[tokio::test]
async fn a_stopped_job_cancelled_with_a_commit_owed_behind_its_writer_still_goes() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_419);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.wait_held();
    let mail = pipeline.graph_runs.written();
    // The checkpoint taken so far commits, so nothing of the job is on its
    // way to the database.
    let first = pipeline.graph_jobs[&job_id].pending().stamp();
    if pipeline.graph_jobs[&job_id]
        .pending()
        .await_commit(first, job_id.0, &mail, &pipeline.db)
        .is_none()
    {
        loop {
            let said = pipeline.graph_runs.recv().await.unwrap();
            let written = matches!(said.heard, Heard::Written { .. });
            pipeline.graph_run_said(said);
            if written {
                break;
            }
        }
    }
    // The next waits for the held writer, and the stop waits on its commit.
    pipeline.flush_graph_checkpoint(job_id);
    assert!(pipeline.graph_jobs[&job_id].pending().gated());
    pipeline.retire_graph_job(job_id);
    assert!(pipeline.retiring_graph_jobs[&job_id].awaits_mail());
    pipeline.cancel_graph_checkpoint_retries(job_id);
    no_loop_waits(&volume);
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    retire_pipeline_database(pipeline).await;
}

// A job that fails while its volume's thread holds operations on its files,
// where the thread returns the last of them as the graph stops: the graph
// held something when the failure looked, and nothing once stopped. The
// failure is still carried out, by the release a graph that holds nothing
// goes to, and the job ends Failed instead of waiting for a retirement
// that was never kept.
#[tokio::test]
async fn a_job_whose_writer_returns_as_it_fails_still_settles() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_420);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.wait_held();
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    assert!(!disk.quiet());
    crate::pipeline::activation::before_kept::set(move || {
        hold.open();
        disk.barrier().wait_returned();
    });
    pipeline.fail_job(job_id, "no file was delivered whole".to_string());
    assert!(!pipeline.retiring_graph_jobs.contains_key(&job_id));
    assert!(
        pipeline.releasing_failed_jobs.contains(&job_id),
        "nothing carries the failure out"
    );
    settle_failed_releases(&mut pipeline).await;
    assert!(matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ));
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    retire_pipeline_database(pipeline).await;
}

// A job that failed with its writer already caught up is released at once,
// and its graph stopped later, as its runtime is purged. Whatever was
// queued for its files in between still runs: a failed job keeps its file
// operations whichever path stops its graph, and its working directory is
// released only once its writer has returned the release of its handles.
#[tokio::test]
async fn a_failed_job_stopped_later_still_runs_what_was_queued_for_it() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_418);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let leftover = working_dir.join("lantern.half");
    std::fs::write(&leftover, b"half of a decode").unwrap();
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    disk.barrier().wait_returned();
    let (volume, hold) = held(&pipeline, job_id);
    pipeline.fail_job(job_id, "no file was delivered whole".to_string());
    // Nothing held the graph, so the failure went straight to its release,
    // which waits for the writer to let the job's handles go.
    assert!(!pipeline.retiring_graph_jobs.contains_key(&job_id));
    assert!(pipeline.releasing_failed_jobs.contains(&job_id));
    assert!(pipeline.graph_jobs.contains_key(&job_id));
    let removed = leftover.clone();
    disk.working().run(None, leftover.clone(), move || {
        std::fs::remove_file(&removed)
    });
    pipeline.retire_graph_job(job_id);
    assert!(pipeline.retiring_graph_jobs.contains_key(&job_id));
    no_loop_waits(&volume);
    hold.open();
    hear_retirement(&mut pipeline, job_id).await;
    settle_failed_releases(&mut pipeline).await;
    assert!(!leftover.exists(), "the queued removal did not run");
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);
    assert!(!working_dir.join(".weaver-chunks").exists());
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
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
    let (mut pipeline, _, _) =
        crate::pipeline::tests::new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8)
            .await;
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
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
    // The writer's word of room: the line was crossed.
    pipeline.volume_rooms.recv().await.unwrap();
    pipeline.volume_room_heard();
    assert!(pipeline.download_dispatch_wake);
    assert!(!pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_some());
    retire_pipeline_database(pipeline).await;
}

// A job with nothing left to fetch whose bytes wait for its volume is
// listed as still writing out. With no tick and no other mail, the word its
// own returned writes send on the room channel drops it from the list at
// once, and what is published no longer names it.
#[tokio::test]
async fn a_draining_job_is_dropped_when_its_writes_return_with_the_loop_parked() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_431);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    // Nothing left to fetch: the decoder took every article without a lane.
    crate::pipeline::tests::queue_fixture::clear_download_queue(&mut pipeline, job_id);
    assert_eq!(
        volume.buffered_bytes.load(Ordering::SeqCst),
        posts[0].bytes.len() as u64
    );
    pipeline.tally_disk_vote(std::time::Instant::now());
    let listed = pipeline.shared_state.disk_limits();
    let write = listed
        .job(job_id)
        .expect("the job is listed as writing out");
    assert!(write.draining && !write.slow);
    assert_eq!(write.volume, volume.label);
    let rows = pipeline.shared_state.disk_rows_revision();
    hold.open();
    // Only the room channel is heard, as the parked loop hears it.
    while pipeline.shared_state.disk_limits().job(job_id).is_some() {
        pipeline.volume_rooms.recv().await.unwrap();
        pipeline.hear_disk_room();
    }
    assert!(pipeline.shared_state.disk_limits().is_empty());
    assert_ne!(pipeline.shared_state.disk_rows_revision(), rows);
    assert_eq!(volume.buffered_bytes.load(Ordering::SeqCst), 0);
    retire_pipeline_database(pipeline).await;
}

// A job listed as still writing out whose last operation does not return,
// as an unpack stuck on the volume's thread: once the bytes that waited in
// memory are down, the volume's room wake drops it from the list, with no
// tick, no other mail and its writes still out.
#[tokio::test]
async fn a_draining_job_with_a_stuck_operation_is_dropped_once_its_bytes_are_down() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_432);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    crate::pipeline::tests::queue_fixture::clear_download_queue(&mut pipeline, job_id);
    // An operation behind the writes that returns only when the test lets
    // it.
    let (unstick, stuck) = std::sync::mpsc::channel::<()>();
    pipeline.graph_jobs[&job_id].disk().unwrap().working().run(
        None,
        temp_dir.path().join("stuck"),
        move || {
            let _ = stuck.recv();
            Ok(())
        },
    );
    pipeline.tally_disk_vote(std::time::Instant::now());
    let write = pipeline
        .shared_state
        .disk_limits()
        .job(job_id)
        .expect("the job is listed as writing out");
    assert!(write.draining && !write.slow);
    hold.open();
    // Only the room channel is heard, as the parked loop hears it.
    while pipeline.shared_state.disk_limits().job(job_id).is_some() {
        pipeline.volume_rooms.recv().await.unwrap();
        pipeline.hear_disk_room();
    }
    assert!(pipeline.shared_state.disk_limits().is_empty());
    assert_eq!(volume.buffered_bytes.load(Ordering::SeqCst), 0);
    assert!(
        !pipeline.graph_jobs[&job_id].disk().unwrap().quiet(),
        "the stuck operation is still out"
    );
    unstick.send(()).unwrap();
    retire_pipeline_database(pipeline).await;
}

// A job listed as still writing out that begins to unpack shows its own
// status: the next word the loop hears drops it at once, within the second
// figures wait for.
#[tokio::test]
async fn a_draining_job_that_begins_to_unpack_is_dropped_at_once() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_433);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _) = graph_job(&temp_dir, job_id, &posts).await;
    let (_volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    crate::pipeline::tests::queue_fixture::clear_download_queue(&mut pipeline, job_id);
    pipeline.tally_disk_vote(std::time::Instant::now());
    assert!(
        pipeline
            .shared_state
            .disk_limits()
            .job(job_id)
            .is_some_and(|write| write.draining)
    );
    let rows = pipeline.shared_state.disk_rows_revision();
    pipeline.jobs.get_mut(&job_id).unwrap().post_state = crate::jobs::model::PostState::Extracting;
    pipeline.hear_disk_room();
    assert_eq!(pipeline.shared_state.disk_limits().job(job_id), None);
    assert_ne!(pipeline.shared_state.disk_rows_revision(), rows);
    hold.open();
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
    assert_eq!(pending.in_hand_bytes(), 0);
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.shutdown().await.unwrap();
    hold.open();
    pipeline.run().await;
    assert_eq!(
        read(&working_dir.join("lantern.bin"))[..2 * ARTICLE],
        posts[0].bytes[..2 * ARTICLE]
    );
    assert_eq!(pending.in_hand_bytes(), 2 * ARTICLE as u64);
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
    use weaver_server_disk::operations::disk::writer::SettleBound;
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
            clock: Arc::new(weaver_server_base::clock::ManualClock::stepping(
                std::time::Duration::from_secs(60),
            )),
            span: std::time::Duration::from_secs(30),
        };
        let handle =
            crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
        handle.shutdown().await.unwrap();
        pipeline.run().await;
        assert_eq!(pending.in_hand_bytes(), 0);
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
    let queue = &pipeline.jobs[&job_id]
        .articles
        .active()
        .unwrap()
        .download_queue;
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
    use weaver_server_disk::operations::disk::writer::SettleBound;
    pipeline.shutdown_settle = SettleBound {
        clock: Arc::new(weaver_server_base::clock::ManualClock::stepping(
            std::time::Duration::from_secs(60),
        )),
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
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
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
    // The archive is down before the volume stands still: the run reads
    // it, and stands only for room for what it says.
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait_aside();
    let (volume, hold) = held(&pipeline, job_id);
    gate.started().await;
    gate.release();
    hold.wait_below_waiters(1);
    let counted = volume.unwritten_bytes();
    assert_eq!(counted, ARTICLE as u64, "the run counted up to the limit");
    stop_past_a_still_volume(&mut pipeline).await;
    // The run said nothing more: the stop may hear what it said before it
    // stood, which it had counted, and no byte past that joins the volume.
    assert!(volume.buffered_bytes.load(Ordering::Relaxed) <= counted);
    assert!(volume.unwritten_bytes() <= counted);
    assert!(!working_dir.join("echo-0.mkv").exists());
    assert_ne!(
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
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
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
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
    use weaver_server_base::clock::ManualClock;
    use weaver_server_disk::operations::disk::writer::SettleBound;
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
        clock: Arc::new(ManualClock::default()),
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
    assert!(
        !graph.pending().given_up(),
        "the job was checkpointed at the stop"
    );
    assert!(graph.disk().unwrap().quiet());
    assert_eq!(volume.buffered_bytes.load(Ordering::Relaxed), 0);
    assert!(working_dir.join("lantern.bin").exists());
    assert_ne!(
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    retire_pipeline_database(pipeline).await;
    drop(hold);
}

// A repair that comes back while the owner is stopping is applied within
// the volume's limit like any other: the stop that sends producing threads
// on does not send the apply's reader on, so it stands still for the volume
// with one slice counted, and the loop never does. When the stop's span
// ends with the volume still at its limit the applying ends there: nothing
// more is queued, the job does not fail, and it is left at its last
// written checkpoint.
#[tokio::test]
async fn a_repair_applied_during_a_stop_keeps_the_volumes_limit_until_the_stops_end() {
    use weaver_server_base::clock::ManualClock;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_419);
    let data = payload(4 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 2));
    let (mut pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline(&temp_dir).await;
    const BUDGET: u64 = 1;
    pipeline.volume_buffers.set_figure(BUDGET, u64::MAX);
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    // Two articles apart are lost: the repair rebuilds two runs.
    for ordinal in [1, 3] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [0, 2] {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
    }
    for index in 1..posts.len() as u32 {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let volume = Arc::clone(disk.working().volume());
    let clock = Arc::new(ManualClock::default());
    let span = std::time::Duration::from_secs(30);
    // The repair's return is in hand and everything before it is down when
    // the stop is rung and the volume stands still.
    let mut watcher = None;
    let mut downloaded = 0;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::Repaired(returned) if matches!(returned.outcome, Some(RepairOutcome::Repaired { .. })))
        {
            disk.barrier().wait_aside();
            // What the downloads waited, before the repair.
            downloaded = budget_waits(&volume);
            let hold = pipeline.volume_writers.hold(&volume);
            let writers = Arc::clone(&pipeline.volume_writers);
            writers.end_waits_at(Arc::clone(&clock) as _, clock.instant(span));
            writers.stop_waits();
            let (watched, clock) = (Arc::clone(&volume), Arc::clone(&clock));
            watcher = Some(std::thread::spawn(move || {
                // The reader stands on the limit with one slice counted,
                // the stop notwithstanding.
                hold.wait_below_waiters(1);
                let queued = watched.unwritten_bytes();
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
    // Nothing joined what the volume counts once the span was over.
    assert_eq!(volume.unwritten_bytes(), queued);
    assert_eq!(
        budget_waits(&volume) - downloaded,
        0,
        "the loop stood still for the volume"
    );
    let graph = &pipeline.graph_jobs[&job_id];
    assert!(graph.disk().unwrap().left_at_stop());
    assert!(
        graph.pending().given_up(),
        "the job is left at its last checkpoint"
    );
    assert_ne!(
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
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
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
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
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
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
        pipeline.graph_jobs[&other_job]
            .disk()
            .unwrap()
            .working()
            .volume()
    ));
    // Each volume has its own limits, given as its first job was admitted.
    let (_, hard) = slow.buffer_limits();
    assert!(hard < u64::MAX);
    assert!(shared.buffer_limits().1 < u64::MAX);

    // The slow volume holds its hard limit; the other holds nothing.
    slow.count_for_test(hard);
    assert_eq!(pipeline.volume_intake(slow_job), VolumeIntake::Stopped);
    assert_eq!(pipeline.volume_intake(other_job), VolumeIntake::Open);
    let lease = pipeline
        .lease_for_server_for_test(0)
        .expect("the job on the other volume is handed work");
    assert_eq!(lease.job_id, other_job);
    assert!(pipeline.volume_holds_budget(slow_job));

    // Drained, the slow volume's job is handed work again.
    slow.count_for_test(0);
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
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
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
            &[],
        )
    };
    let hard = |disk: &crate::pipeline::graph::disk::JobDisk, under: &std::path::Path| {
        disk.under(under).volume().buffer_limits().1
    };
    let (work_a, out_a) = (intermediate.join("a"), complete.join("a"));
    let first = disk_for(1, &work_a, &out_a);
    assert!(!Arc::ptr_eq(
        first.working().volume(),
        first.under(&out_a).volume()
    ));
    assert_eq!(
        (hard(&first, &work_a), hard(&first, &out_a)),
        (FIGURE, FIGURE)
    );
    let _ = pipeline.volume_buffers.follow_rates();

    // A second job on the same two volumes changes nothing.
    let same = disk_for(2, &intermediate.join("b"), &complete.join("b"));
    assert_eq!(
        (hard(&first, &work_a), hard(&first, &out_a)),
        (FIGURE, FIGURE)
    );
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
    assert_eq!(
        (hard(&first, &work_a), hard(&first, &out_a)),
        (FIGURE, FIGURE)
    );
    assert!(
        pipeline.volume_buffers.follow_rates(),
        "the pipeline hears that a limit rose"
    );
    assert!(!pipeline.volume_buffers.follow_rates());

    // A job leaving volumes others still write to changes nothing.
    drop(same);
    assert_eq!(
        (hard(&first, &work_a), hard(&first, &out_a)),
        (FIGURE, FIGURE)
    );
    assert!(!pipeline.volume_buffers.follow_rates());
    drop(first);
    retire_pipeline_database(pipeline).await;
}

// A job's disk writes through the volumes its directories were read on off
// the loop, never through the stand-in memory answers for a path it has
// not read or has forgotten: the stand-in has limits of its own, so writes
// through it would not count against the limit of the volume they land on.
// A directory that was not read is asked after from memory, as before.
#[tokio::test]
async fn a_job_writes_through_the_volumes_read_for_its_directories() {
    use crate::pipeline::graph::disk::job_disk;
    use weaver_server_disk::operations::disk::volume::Volume;
    let temp_dir = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = crate::pipeline::tests::new_direct_pipeline_at_roots(
        temp_dir.path().join("data"),
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("weaver.db"),
        16 * 1024 * 1024,
        8,
        None,
    )
    .await;
    let elsewhere = temp_dir.path().join("elsewhere");
    let (working, output) = (elsewhere.join("work"), elsewhere.join("out"));
    weaver_server_disk::runtime::fs::test_devices::assign(&elsewhere, 9);
    // Nothing has read either path: memory answers the stand-in.
    let stand_in = pipeline.storage_capacity.volume(&working);
    assert!(
        stand_in.label.ends_with("(device unknown)"),
        "{}",
        stand_in.label
    );

    let read = Arc::new(Volume::new(Some(9), "elsewhere".to_owned()));
    let disk = job_disk(
        &pipeline.volume_writers,
        &pipeline.storage_capacity,
        &pipeline.volume_probes,
        1,
        &working,
        Some(&output),
        &pipeline.volume_buffers,
        &[
            (working.clone(), Arc::clone(&read)),
            (output.clone(), Arc::clone(&read)),
        ],
    );
    assert!(Arc::ptr_eq(disk.working().volume(), &read));
    assert!(Arc::ptr_eq(disk.under(&output).volume(), &read));
    assert!(!Arc::ptr_eq(disk.working().volume(), &stand_in));
    drop(disk);
    retire_pipeline_database(pipeline).await;
}

// One lane asks for work, and holds what it is handed as a dispatch books
// it. The handout, and the job of each article handed.
fn lane_handout(
    pipeline: &mut Pipeline,
    lane_id: u64,
) -> Result<Vec<JobId>, crate::pipeline::download::scheduler::SaturationWake> {
    use crate::pipeline::download::scheduler::{Handout, LaneShare};
    let pressure = pipeline.refresh_download_pressure();
    let lane_mode = pipeline.download_lane_mode_for_server(0, pressure, true);
    let want = pipeline.download_refill_want(lane_mode);
    assert!(want > 1, "{want}");
    // A pipe as deep as one ask: a whole pipe is one ask.
    let lane = LaneShare {
        lane_id,
        depth: want,
    };
    let spill = pipeline.spill_job_in_flight_on(0);
    let works = match pipeline.next_works_for_lane(0, want, Some(lane), spill, pressure) {
        Handout::Works(works) => works,
        Handout::Idle => panic!("the lane is handed work, and was told there is none"),
        Handout::Yield(_) => panic!("the lane is handed work, and was told to yield"),
        Handout::Saturated(wake) => return Err(wake),
    };
    let jobs = works
        .iter()
        .map(|work| work.segment_id.file_id.job_id)
        .collect();
    let lease = pipeline
        .lease_for_handout(lane_id, 0, lane_mode, pressure, works)
        .expect("the handout is leased");
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    Ok(jobs)
}

fn lane_asks(pipeline: &mut Pipeline, lane_id: u64) -> Vec<JobId> {
    lane_handout(pipeline, lane_id)
        .unwrap_or_else(|wake| panic!("the lane is handed work, and was held: {wake:?}"))
}

fn one_ask(pipeline: &mut Pipeline) -> usize {
    let pressure = pipeline.refresh_download_pressure();
    let lane_mode = pipeline.download_lane_mode_for_server(0, pressure, true);
    pipeline.download_refill_want(lane_mode)
}

// Below a volume's lease limit a lane fetching for its job is handed whole
// pipes, at the soft limit as anywhere. Where the volume has room for some
// articles and not a whole pipe, a lane with room in its part is handed what
// fits. Where it has room for none, no lane is handed any, the one holding
// nothing included: above the volume's resume line each is told to wait
// until the volume has fallen to that line.
#[tokio::test]
async fn a_lane_is_handed_whole_pipes_or_what_fits_below_the_lease_limit() {
    use crate::pipeline::download::scheduler::SaturationWake;
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_422);
    let posts = [Post::plain("lantern.bin", payload(64 * ARTICLE, 1))];
    let (mut pipeline, _, _) =
        crate::pipeline::tests::new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8)
            .await;
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    let (soft, hard) = volume.buffer_limits();
    assert!(soft < volume.lease_limit() && hard < u64::MAX);
    let whole = one_ask(&mut pipeline);
    assert!(whole > 2, "{whole}");
    // The job's own share of a lane is more than a pipe: only the volume
    // holds the lane back here.
    assert!(pipeline.lane_share_of_job(job_id, whole) > whole + 1);

    // At the soft limit the lane is handed a whole ask.
    volume.count_for_test(soft);
    assert_eq!(pipeline.volume_intake(job_id), VolumeIntake::Open);
    let lane_id = Pipeline::next_download_lane_id();
    assert_eq!(lane_asks(&mut pipeline, lane_id), vec![job_id; whole]);
    // Each article handed was reserved on the volume at its size.
    let article = (volume.unwritten_bytes() - soft) / whole as u64;
    assert!(article > 0);

    // Room for two articles: a lane holding nothing is handed the two that
    // fit, not a whole pipe and not nothing.
    volume.count_for_test(volume.lease_limit() - 2 * article);
    let second = Pipeline::next_download_lane_id();
    assert_eq!(lane_asks(&mut pipeline, second), vec![job_id; 2]);
    assert_eq!(volume.unwritten_bytes(), volume.lease_limit());

    // Room for less than one: the lane holding work waits for the line.
    volume.count_for_test(volume.lease_limit() - 1);
    assert_eq!(
        lane_handout(&mut pipeline, lane_id),
        Err(SaturationWake {
            of: Some(job_id),
            below: 0,
            room: true,
        })
    );
    assert_eq!(
        pipeline.download_lane_holdings_of_job(lane_id, job_id),
        whole
    );
    // So does a lane that holds nothing: no lane is handed past the room.
    let third = Pipeline::next_download_lane_id();
    assert!(
        matches!(
            lane_handout(&mut pipeline, third),
            Err(SaturationWake { room: true, .. })
        ),
        "a lane holding nothing is handed work past the volume's room"
    );
    assert_eq!(volume.unwritten_bytes(), volume.lease_limit() - 1);

    // With room again, the first lane is handed more.
    volume.count_for_test(0);
    assert!(!lane_asks(&mut pipeline, lane_id).is_empty());
    retire_pipeline_database(pipeline).await;
}

// A volume short of room holds its own job back on a lane, not the lane:
// the lane's room goes to the next job, on a volume that has room. Once
// the short volume has room again its job, first in the queue, is handed
// work again.
#[tokio::test]
async fn a_volume_short_of_room_leaves_the_lanes_room_to_jobs_on_other_volumes() {
    use crate::pipeline::download::volume_pace::VolumeIntake;
    let temp_dir = tempfile::tempdir().unwrap();
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
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
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
    }];
    let (slow_job, other_job) = (JobId(92_424), JobId(92_425));
    let posts = [Post::plain("lantern.bin", payload(64 * ARTICLE, 1))];
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
    // The first job in the queue is the one whose own volume is short.
    assert_eq!(
        pipeline.download_scheduler_eligible_jobs(),
        [slow_job, other_job]
    );
    let staging_dir = crate::pipeline::queue::staging_dir_for(&movies, slow_job);
    let disk = pipeline.graph_jobs[&slow_job].disk().unwrap().clone();
    let slow = Arc::clone(disk.under(&staging_dir).volume());
    let shared = Arc::clone(disk.working().volume());
    assert!(!Arc::ptr_eq(&slow, &shared));
    // The first job's own volume has no room for an article.
    slow.count_for_test(slow.lease_limit() - 1);
    assert_eq!(pipeline.volume_intake(slow_job), VolumeIntake::Open);
    assert_eq!(pipeline.volume_intake(other_job), VolumeIntake::Open);

    let lane_id = Pipeline::next_download_lane_id();
    // The lane's room goes to the job on the volume with room.
    let handed = lane_asks(&mut pipeline, lane_id);
    assert!(!handed.is_empty(), "{handed:?}");
    assert!(
        handed.iter().all(|job_id| *job_id == other_job),
        "{handed:?}"
    );
    assert_eq!(pipeline.download_lane_holdings_of_job(lane_id, slow_job), 0);

    // With room again, a lane is handed the first job's work.
    slow.count_for_test(0);
    let second = Pipeline::next_download_lane_id();
    let handed = lane_asks(&mut pipeline, second);
    assert!(handed.contains(&slow_job), "{handed:?}");
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
    let (mut pipeline, _, _) =
        crate::pipeline::tests::new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8)
            .await;
    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    pipeline
        .volume_buffers
        .set_figure(2 * ARTICLE as u64, u64::MAX);
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    assert_eq!(volume.buffer_limits().1, 2 * ARTICLE as u64);

    for _ in 0..2 {
        pipeline.note_arrived_result_for_test(job_id, ARTICLE as u64);
    }
    assert_eq!(
        volume.arrived_bytes.load(Ordering::SeqCst),
        2 * ARTICLE as u64
    );
    assert!(pipeline.volume_holds_budget(job_id));
    assert!(pipeline.lease_for_server_for_test(0).is_none());
    pipeline.download_dispatch_wake = false;

    // Both results are handed on. The first leaves the volume above its
    // line or wakes dispatch; the second leaves nothing and has woken it.
    pipeline.hand_on_result_for_test(job_id, ARTICLE as u64);
    pipeline.hand_on_result_for_test(job_id, ARTICLE as u64);
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
    let volume = Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
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
    assert_ne!(
        was, will,
        "the pipeline's machine sizes these two profiles apart"
    );
    assert_eq!(volume.buffer_limits(), (was * 70 / 100, was));

    pipeline.set_configured_hardware_profile(other);

    assert_eq!(volume.buffer_limits(), (will * 70 / 100, will));
    retire_pipeline_database(pipeline).await;
}

// A volume's probe waits for the first job's writes: it is queued as the job
// is given the volume's writer, and runs on the volume's thread once that
// writer first runs out of work, after the job's file is down whole. The
// rate it measures is the one extraction admission reads: on a disk whose
// writes run under 100 MB/s one extraction runs at a time, where two did
// before the volume was measured.
#[tokio::test]
async fn a_volume_is_probed_after_its_first_jobs_writes_and_a_slow_one_runs_one_extraction() {
    use std::sync::{Mutex, mpsc};
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_430);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (said, probed) = mpsc::channel();
    let said = Mutex::new(said);
    let job_file = Arc::new(std::sync::OnceLock::<PathBuf>::new());
    let file_of_job = Arc::clone(&job_file);
    pipeline.volume_probes = Arc::new(crate::pipeline::graph::disk::VolumeProbes::with(
        move |volume, _| {
            let said = said.lock().unwrap().clone();
            let file_of_job = Arc::clone(&file_of_job);
            Box::new(move || {
                // What the job's file holds as the probe begins.
                let held = file_of_job.get().and_then(|file| std::fs::read(file).ok());
                volume.write_rate.store(60_000_000, Ordering::Relaxed);
                let _ = said.send((Arc::clone(&volume), held));
                Ok(())
            })
        },
    ));
    // A drive with seeks to spare: two extractions while its writes are not
    // measured.
    pipeline.tuner.set_random_read_iops(600.0);
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.tuner.max_concurrent_extractions(), 2);

    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    job_file.set(working_dir.join("lantern.bin")).unwrap();
    let (volume, hold) = held(&pipeline, job_id);
    assert!(Arc::ptr_eq(
        &volume,
        &pipeline.storage_capacity.volume(&pipeline.intermediate_dir)
    ));
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    // The thread stands before the job's first write: the probe has not
    // run ahead of it.
    hold.wait_held();
    assert!(probed.try_recv().is_err(), "not ahead of the job's writes");
    hold.open();
    let (probed_volume, held_then) = probed.recv().unwrap();
    assert!(Arc::ptr_eq(&probed_volume, &volume));
    assert_eq!(
        held_then.as_deref(),
        Some(posts[0].bytes.as_slice()),
        "the probe ran once the job's file was down whole"
    );
    assert_eq!(read(&working_dir.join("lantern.bin")), posts[0].bytes);

    // The next turn reads the probed rate: one extraction at a time.
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.tuner.max_concurrent_extractions(), 1);
    no_loop_waits(&volume);
    drop(hold);
    retire_pipeline_database(pipeline).await;
}

// The volume's probe runs once a job's file is down, beside the job's
// delivery: held while its scratch file is there, the file is not among the
// entries of the job's folder the final move lists, and once let go it is
// gone and the delivery moves the job's own file alone.
#[tokio::test]
async fn a_probe_holding_its_scratch_file_leaves_the_jobs_folder_and_delivery_alone() {
    use std::sync::{Mutex, mpsc};
    use std::time::Duration;
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let data = tempfile::tempdir().unwrap();
    let job_id = JobId(92_431);
    let posts = [Post::plain("lantern.bin", payload(3 * ARTICLE, 1))];
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (made, scratch) = mpsc::channel::<PathBuf>();
    let (release, released) = mpsc::channel::<()>();
    let (made, released) = (Mutex::new(made), Mutex::new(released));
    let store =
        weaver_server_disk::operations::disk::ProbeStore::writing_with(data.path(), move |path| {
            made.lock().unwrap().send(path.to_path_buf()).unwrap();
            released.lock().unwrap().recv().unwrap();
            Some(weaver_server_disk::runtime::system_probe::ProbeWrite {
                bytes: 1_000_000,
                elapsed: Duration::from_millis(10),
            })
        });
    let (ended, probe_ended) = mpsc::channel::<()>();
    let ended = Mutex::new(ended);
    pipeline.volume_probes = Arc::new(crate::pipeline::graph::disk::VolumeProbes::with(
        move |volume, dir| {
            let probe =
                weaver_server_disk::operations::disk::probe_once(volume, dir, Arc::clone(&store));
            let ended = ended.lock().unwrap().clone();
            Box::new(move || {
                let probed = probe();
                let _ = ended.send(());
                probed
            })
        },
    ));

    insert_active_job(
        &mut pipeline,
        job_id,
        spec("lantern joined", &posts, ARTICLE),
    )
    .await;
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let (volume, hold) = held(&pipeline, job_id);
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    hold.wait_held();
    hold.open();

    // The probe made its file once the job's writes were down, and holds.
    let scratch = scratch.recv().unwrap();
    assert!(scratch.is_file());
    let listed: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !listed.iter().any(|name| name.contains("volume-probe")),
        "the job's folder lists {listed:?}"
    );
    assert!(listed.contains(&"lantern.bin".to_string()), "{listed:?}");
    assert!(!scratch.starts_with(&working_dir), "{}", scratch.display());
    assert_eq!(
        scratch.parent().and_then(Path::parent),
        Some(pipeline.intermediate_dir.as_path())
    );

    // Let go, the probe removes its file and its directory.
    release.send(()).unwrap();
    probe_ended.recv().unwrap();
    assert!(!scratch.exists());
    assert!(!scratch.parent().unwrap().exists());
    assert!(volume.write_rate.load(Ordering::Relaxed) > 0);

    pipeline.start_move_to_complete(job_id).await.unwrap();
    while !pipeline.inflight_moves.contains(&job_id) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
        pipeline.start_move_to_complete(job_id).await.unwrap();
    }
    let done = pipeline.move_done_rx.recv().await.unwrap();
    pipeline.handle_move_to_complete_done(done).await;
    let status = job_status_for_assert(&pipeline, job_id);
    assert!(
        !matches!(status, Some(JobStatus::Failed { .. })),
        "{status:?}"
    );
    let mut delivered = Vec::new();
    let mut dirs = vec![pipeline.complete_dir.clone()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                dirs.push(entry.path());
            } else {
                delivered.push((
                    entry.file_name().to_string_lossy().into_owned(),
                    std::fs::read(entry.path()).unwrap(),
                ));
            }
        }
    }
    delivered.retain(|(name, _)| !name.starts_with(".weaver-"));
    assert_eq!(
        delivered,
        vec![("lantern.bin".to_string(), posts[0].bytes.clone())]
    );
    drop(hold);
    retire_pipeline_database(pipeline).await;
}

// Hear the next report or mail the loop is sent.
async fn hear_next(pipeline: &mut Pipeline) {
    tokio::select! {
        Some(()) = pipeline.volume_rooms.recv() => pipeline.volume_room_heard(),
        Some(said) = pipeline.graph_runs.recv() => pipeline.graph_run_said(said),
    }
}

// A file the volume's writer cannot make for its name is skipped once. Each
// later write of it returns at hand-over, before its own refusal comes back:
// the member stays marked through it, and the refusal that follows skips
// nothing more, so it is logged and damaged once.
#[tokio::test]
async fn a_name_a_volumes_thread_refuses_is_skipped_once_over_later_writes() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_431);
    // One part of 300 bytes, past the 255 a part may hold.
    let name = format!("{}.bin", "a".repeat(296));
    let posts = [Post::plain(&name, payload(3 * ARTICLE, 1))];
    let (mut pipeline, working_dir) = graph_job(&temp_dir, job_id, &posts).await;
    let unnamable = |pipeline: &Pipeline| {
        pipeline.graph_jobs[&job_id]
            .writer()
            .unnamable(weaver_graph::NodeId::ROOT, MemberId(0))
    };
    decode(&mut pipeline, job_id, &posts, 0, 0, true).await;
    while !unnamable(&pipeline) {
        hear_next(&mut pipeline).await;
    }
    for ordinal in 1..3 {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
        assert!(unnamable(&pipeline), "article {ordinal}");
        let barrier = pipeline.graph_jobs[&job_id].disk().unwrap().barrier();
        while !barrier.passed() {
            hear_next(&mut pipeline).await;
        }
        assert!(unnamable(&pipeline), "article {ordinal}");
    }
    let job = &pipeline.graph_jobs[&job_id];
    assert_eq!(job.writer().names_skipped, 1);
    let whole = ByteRange::new(0, posts[0].bytes.len() as u64);
    assert!(
        job.namespace()
            .get(MemberId(0))
            .unwrap()
            .coverage
            .damaged(whole)
    );
    let listed: Vec<String> = std::fs::read_dir(&working_dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !listed.iter().any(|listed| listed.ends_with(".bin")),
        "{listed:?}"
    );
    retire_pipeline_database(pipeline).await;
}

// A job whose working directory and delivered files lie on two volumes of
// their own: scratch under the intermediate root, the delivered files in
// the movies category's folder. The volumes are sized on `figure` as the
// job is admitted. Where the delivered files are staged.
async fn delivered_apart(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    posts: &[Post],
    figure: Option<u64>,
) -> (Pipeline, PathBuf) {
    let (intermediate, complete, movies) = (
        temp_dir.path().join("intermediate"),
        temp_dir.path().join("complete"),
        temp_dir.path().join("movies"),
    );
    weaver_server_disk::runtime::fs::test_devices::assign(&intermediate, 1);
    weaver_server_disk::runtime::fs::test_devices::assign(&complete, 2);
    weaver_server_disk::runtime::fs::test_devices::assign(&movies, 3);
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
        seeding: None,
        torrent_route: false,
        unpack: None,
        upload_cap: Default::default(),
    }];
    if let Some(figure) = figure {
        pipeline.volume_buffers.set_figure(figure, u64::MAX);
    }
    let mut spec = spec("lantern joined", posts, ARTICLE);
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
    (pipeline, staging_dir)
}

// The job's scratch volume and the volume its delivered files land on,
// told apart.
fn working_and_output(
    pipeline: &Pipeline,
    job_id: JobId,
    staging_dir: &std::path::Path,
) -> (Arc<Volume>, Arc<Volume>) {
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap();
    let working = Arc::clone(disk.working().volume());
    let output = Arc::clone(disk.under(staging_dir).volume());
    assert!(!Arc::ptr_eq(&working, &output));
    (working, output)
}

// How often the loop stood still for the volume's write budget.
fn budget_waits(volume: &Volume) -> u64 {
    volume.writer.loop_waits_by_kind
        [weaver_server_disk::operations::disk::volume::LoopWait::Budget as usize]
        .load(Ordering::Relaxed)
}

// The first file of `posts` loses every other one of its eight articles;
// the others arrive, and so does every file after it up to `last`.
async fn lose_every_other_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    posts: &[Post],
    last: u32,
) {
    for ordinal in [1, 3, 5, 7] {
        decode(pipeline, job_id, posts, 0, ordinal, true).await;
    }
    for ordinal in [0, 2, 4, 6] {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
    }
    for index in 1..last {
        decode_all(pipeline, job_id, posts, index).await;
    }
}

// Hear the job's mail until a thread stands for room on the held volume.
// A producing thread counts its output on each of the job's volumes, and
// what it counted on those the output does not land on is freed only as
// the loop hears it: a test that stopped hearing would hold it there.
async fn hear_until_one_stands(pipeline: &mut Pipeline, hold: Hold) -> Hold {
    let (stood, mut standing) = tokio::sync::oneshot::channel();
    let watcher = std::thread::spawn(move || {
        hold.wait_below_waiters(1);
        let _ = stood.send(());
        hold
    });
    loop {
        tokio::select! {
            _ = &mut standing => break,
            Some(said) = pipeline.graph_runs.recv() => pipeline.graph_run_said(said),
        }
    }
    watcher.join().unwrap()
}

// Whether the mail is the return of a repair that rebuilt what it was for.
fn repaired(said: &crate::pipeline::graph::engine::Said) -> bool {
    matches!(&said.heard, Heard::Repaired(returned) if matches!(returned.outcome, Some(RepairOutcome::Repaired { .. })))
}

// The staged output of the job's repair, as the repair left it.
fn staged_repair(working_dir: &std::path::Path) -> Option<PathBuf> {
    std::fs::read_dir(working_dir.join(weaver_graph::names::INTERNAL_DIR))
        .ok()?
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "repaired"))
}

// An extraction whose output lands on a volume of its own that stands
// still, sized below what the extraction writes: the run pays for each
// output on its own thread, against the volume it lands on, and the loop
// never stands still for it. The volume never counts more than its limit,
// or than it held when the run first stood.
#[tokio::test]
async fn an_extraction_onto_a_still_output_volume_never_stops_the_loop() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_490);
    let body = payload(6 * ARTICLE + 5, 6);
    let posts = vec![echo_volume(0, &body)];
    // Above the 160 bytes the loop holds of a member before its first
    // write: a limit below that can never be met by a thread that pays.
    let (mut pipeline, staging_dir) =
        delivered_apart(&temp_dir, job_id, &posts, Some(4 * ARTICLE as u64)).await;
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .set_builder(Box::new(Echoes {
            volumes: 1,
            ..Echoes::default()
        }));
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let (_, output) = working_and_output(&pipeline, job_id, &staging_dir);
    let (_, hard) = output.buffer_limits();
    let hold = pipeline.volume_writers.hold(&output);
    let watched = Arc::clone(&output);
    let watcher = std::thread::spawn(move || {
        // The run stands on its own thread for the output volume.
        hold.wait_below_waiters(1);
        let seen = (
            watched.buffered_bytes.load(Ordering::Relaxed),
            watched.unwritten_bytes(),
        );
        hold.open();
        seen
    });
    decode_all(&mut pipeline, job_id, &posts, 0).await;
    drain_runs(&mut pipeline, job_id).await;
    let (buffered, counted) = watcher.join().unwrap();
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("echo-0.mkv")), body);
    assert_eq!(
        budget_waits(&output),
        0,
        "the loop stood still for the volume"
    );
    assert!(counted <= hard.max(buffered), "{counted} over {hard}");
    retire_pipeline_database(pipeline).await;
}

// A repair whose member lies on a volume of its own that stands still,
// sized below a slice: the return is read back and paid for on its own
// thread, a slice a mail, against the volume it lands on. The loop never
// stands still for the volume, and the volume never counts two slices.
#[tokio::test]
async fn a_repair_onto_a_still_output_volume_never_stops_the_loop() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_491);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, Some(1)).await;
    let last = posts.len() as u32;
    lose_every_other_article(&mut pipeline, job_id, &posts, last).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let (_, output) = working_and_output(&pipeline, job_id, &staging_dir);
    let (_, hard) = output.buffer_limits();
    // What the downloads waited, before any repair.
    let downloaded = budget_waits(&output);
    let mut watcher = None;
    let (mut slices, mut over) = (0u32, 0u64);
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if repaired(&said) {
            disk.barrier().wait_aside();
            let hold = pipeline.volume_writers.hold(&output);
            watcher = Some(std::thread::spawn(move || {
                // A disk that writes one slice each time the reader stands
                // on the volume; the third time it opens.
                for _ in 0..2 {
                    hold.wait_below_waiters(1);
                    hold.release(1);
                }
                hold.wait_below_waiters(1);
                hold.open();
            }));
        }
        if matches!(&said.heard, Heard::RepairSlice(_)) {
            slices += 1;
        }
        pipeline.graph_run_said(said);
        // One slice may stand alone over a limit smaller than it; two never.
        let counted = output.unwritten_bytes();
        if counted > hard.max(ARTICLE as u64) {
            over = over.max(counted);
        }
    }
    watcher.expect("the repair came back").join().unwrap();
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert_eq!(slices, 4);
    assert_eq!(
        budget_waits(&output) - downloaded,
        0,
        "the loop stood still for the output volume"
    );
    assert_eq!(over, 0, "more than one slice was counted over the limit");
    retire_pipeline_database(pipeline).await;
}

// A repair whose set proves good two articles that arrived marked damaged:
// the tail of its return delivers them again, read back, onto a volume of
// their own that is full as the tail begins: its limit counted by another
// producer and bytes waiting for its writer, as a slow disk leaves it. The
// loop never stands still for the volume: the tail stands for room on a
// thread of its own (`Heard::TailPaid`) and goes on as that thread counts
// the piece. Once the room is given back the repair ends, the member whole
// and nothing of it still marked damaged. The writer itself is never held:
// the loop reads back what it wrote, and that read waits for the writer.
#[tokio::test]
async fn a_repairs_tail_onto_a_full_output_volume_never_stops_the_loop() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_493);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, Some(1)).await;
    let last = posts.len() as u32;
    // Two articles lost, two apart from each other that arrive with their
    // own bytes but a failed checksum, the rest whole.
    for ordinal in [1, 3, 5, 7] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in [4, 6] {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, false).await;
    }
    for ordinal in [0, 2] {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
    }
    for index in 1..last {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let (_, output) = working_and_output(&pipeline, job_id, &staging_dir);
    // What the downloads waited, before any repair.
    let downloaded = budget_waits(&output);
    let (mut watcher, mut stood) = (None, 0u32);
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::RepairRead(_)) {
            // Every slice is down. Another producer counts the volume's
            // whole limit and the writer is said to hold bytes, until a
            // thread stands for room; then both go.
            disk.barrier().wait_aside();
            let submitter = Arc::clone(disk.under(&staging_dir));
            let (_, hard) = output.buffer_limits();
            assert!(hard < u64::MAX, "the volume is sized");
            let full = weaver_server_disk::operations::disk::writer::Credit::reserve(
                std::iter::once(&submitter),
                hard,
                weaver_server_disk::operations::disk::writer::Stand::UntilStop,
            );
            assert!(full.covers(hard));
            output.buffered_bytes.fetch_add(hard, Ordering::SeqCst);
            let (hold, filled) = (pipeline.volume_writers.hold(&output), Arc::clone(&output));
            hold.open();
            watcher = Some(std::thread::spawn(move || {
                hold.wait_below_waiters(1);
                filled.buffered_bytes.fetch_sub(hard, Ordering::SeqCst);
                drop(full);
            }));
        }
        if matches!(&said.heard, Heard::TailPaid(_)) {
            stood += 1;
        }
        pipeline.graph_run_said(said);
    }
    assert_eq!(
        budget_waits(&output) - downloaded,
        0,
        "the loop stood still for the output volume"
    );
    assert!(stood > 0, "the tail never stood for room");
    watcher.expect("the repair was read back").join().unwrap();
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert_eq!(
        pipeline.graph_jobs[&job_id]
            .namespace()
            .damaged_ranges()
            .count(),
        0
    );
    retire_pipeline_database(pipeline).await;
}

// One stored member, "inside.bin", in a 7z whose end header follows its
// body.
fn stored_7z(inside: &[u8]) -> Vec<u8> {
    use sevenz_turbo::{ArchiveEntry, ArchiveWriter, EncoderConfiguration, EncoderMethod};
    let mut archive = ArchiveWriter::new(std::io::Cursor::new(Vec::new())).unwrap();
    archive.set_content_methods(vec![EncoderConfiguration::new(EncoderMethod::COPY)]);
    let mut entry = ArchiveEntry::new_file("inside.bin");
    entry.size = inside.len() as u64;
    archive
        .push_archive_entry(entry, Some(std::io::Cursor::new(inside.to_vec())))
        .unwrap();
    archive.finish().unwrap().into_inner()
}

// A stored 7z whose articles from the end of its body on are lost: the body
// that came is held, with no place until the end header says where it goes.
// The repair rebuilds the header, and the tail of its return releases the
// held body, downloaded bytes and no run's output, onto the volume the
// member lands on, full as the tail begins. The loop never stands still for
// the volume: the tail stands for room on a thread of its own and goes on
// as that thread counts each span, and every byte lands.
#[tokio::test]
async fn a_repairs_tail_releasing_held_bytes_onto_a_full_output_volume_never_stops_the_loop() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_497);
    let inside = payload(8 * ARTICLE, 4);
    let archive = stored_7z(&inside);
    let footer = 32 + inside.len();
    let articles = archive.len().div_ceil(ARTICLE) as u32;
    let lost_from = (footer / ARTICLE) as u32;
    assert!(
        articles - lost_from <= 4,
        "the set rebuilds every lost article"
    );
    let mut posts = vec![Post::plain("lantern.7z", archive.clone())];
    posts.extend(par2_set("lantern", &[("lantern.7z", &archive)], 4));
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, Some(1)).await;
    let last = posts.len() as u32;
    for ordinal in 0..lost_from {
        decode(&mut pipeline, job_id, &posts, 0, ordinal, true).await;
    }
    for ordinal in lost_from..articles {
        pipeline.book_terminal_segment(
            segment_id(job_id, 0, ordinal),
            SegmentTerminalState::Missing,
        );
    }
    for index in 1..last {
        decode_all(&mut pipeline, job_id, &posts, index).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let (_, output) = working_and_output(&pipeline, job_id, &staging_dir);
    // What the downloads waited, before any repair.
    let downloaded = budget_waits(&output);
    let (mut watcher, mut stood) = (None, 0u32);
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if matches!(&said.heard, Heard::RepairRead(_)) {
            // Every slice is down, and the body is still held. Another
            // producer counts the volume's whole limit and the writer is
            // said to hold bytes, until a thread stands for room; then both
            // go.
            disk.barrier().wait_aside();
            assert!(
                pipeline.graph_jobs[&job_id]
                    .writer()
                    .held_bytes(weaver_graph::NodeId::ROOT)
                    > 0,
                "the body is held behind the header"
            );
            let submitter = Arc::clone(disk.under(&staging_dir));
            let (_, hard) = output.buffer_limits();
            assert!(hard < u64::MAX, "the volume is sized");
            let full = weaver_server_disk::operations::disk::writer::Credit::reserve(
                std::iter::once(&submitter),
                hard,
                weaver_server_disk::operations::disk::writer::Stand::UntilStop,
            );
            assert!(full.covers(hard));
            output.buffered_bytes.fetch_add(hard, Ordering::SeqCst);
            let (hold, filled) = (pipeline.volume_writers.hold(&output), Arc::clone(&output));
            hold.open();
            watcher = Some(std::thread::spawn(move || {
                hold.wait_below_waiters(1);
                filled.buffered_bytes.fetch_sub(hard, Ordering::SeqCst);
                drop(full);
            }));
        }
        if matches!(&said.heard, Heard::TailPaid(_)) {
            stood += 1;
        }
        pipeline.graph_run_said(said);
    }
    assert_eq!(
        budget_waits(&output) - downloaded,
        0,
        "the loop stood still for the output volume"
    );
    assert!(stood > 0, "the tail never stood for room");
    watcher.expect("the repair was read back").join().unwrap();
    disk.barrier().wait_aside();
    assert_eq!(
        pipeline.graph_jobs[&job_id]
            .writer()
            .held_bytes(weaver_graph::NodeId::ROOT),
        0
    );
    assert_eq!(read(&staging_dir.join("inside.bin")), inside);
    retire_pipeline_database(pipeline).await;
}

// An article of a file outside the set arrives between two slices of a
// repair being applied: both files end whole.
#[tokio::test]
async fn an_article_between_a_repairs_slices_lands_whole() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_492);
    let data = payload(8 * ARTICLE, 4);
    let other = payload(2 * ARTICLE, 9);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    posts.push(Post::plain("other.bin", other.clone()));
    let last = posts.len() as u32 - 1;
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, None).await;
    decode(&mut pipeline, job_id, &posts, last, 1, true).await;
    lose_every_other_article(&mut pipeline, job_id, &posts, last).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let mut between = false;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
        if !between && pipeline.graph_jobs[&job_id].slices_applied() == 2 {
            between = true;
            decode(&mut pipeline, job_id, &posts, last, 0, true).await;
        }
    }
    assert!(between, "the article arrived between two slices");
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert_eq!(read(&staging_dir.join("other.bin")), other);
    retire_pipeline_database(pipeline).await;
}

// An article of the member being repaired, one it had lost, arrives late
// between two slices of the repair: it is the bytes the repair writes, and
// the member ends whole.
#[tokio::test]
async fn a_lost_article_arriving_during_its_repair_leaves_the_member_whole() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_493);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let last = posts.len() as u32;
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, None).await;
    lose_every_other_article(&mut pipeline, job_id, &posts, last).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let mut between = false;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
        if !between && pipeline.graph_jobs[&job_id].slices_applied() == 2 {
            between = true;
            decode(&mut pipeline, job_id, &posts, 0, 6, true).await;
        }
    }
    assert!(between, "the article arrived between two slices");
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    assert_ne!(
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    retire_pipeline_database(pipeline).await;
}

// The last article of the recovery volume, one block more than the repair
// needs, arrives between two slices of the repair: the member ends whole.
#[tokio::test]
async fn a_recovery_article_arriving_during_a_repair_leaves_the_member_whole() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_494);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 5));
    let recovery = posts.len() as u32 - 1;
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, None).await;
    lose_every_other_article(&mut pipeline, job_id, &posts, recovery).await;
    let held_back = posts[recovery as usize].articles(ARTICLE).count() as u32 - 1;
    for ordinal in 0..held_back {
        decode(&mut pipeline, job_id, &posts, recovery, ordinal, true).await;
    }
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let mut between = false;
    while pipeline.graph_jobs[&job_id].repair_out() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        pipeline.graph_run_said(said);
        if !between && pipeline.graph_jobs[&job_id].slices_applied() == 2 {
            between = true;
            decode(&mut pipeline, job_id, &posts, recovery, held_back, true).await;
        }
    }
    assert!(between, "the article arrived between two slices");
    disk.barrier().wait_aside();
    assert_eq!(read(&staging_dir.join("lantern.bin")), data);
    retire_pipeline_database(pipeline).await;
}

// A stop while a repair's return is read back onto a volume of its own that
// does not put it down: the reader stands for the volume until the stop's
// span is over, the loop never does, and the stop does not wait for the
// volume past its span. The return is not applied, its staged output goes,
// and the job is left at its last checkpoint without failing.
#[tokio::test]
async fn a_stop_during_an_apply_onto_a_still_output_volume_leaves_the_job_at_its_checkpoint() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_495);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let (mut pipeline, staging_dir) = delivered_apart(&temp_dir, job_id, &posts, Some(1)).await;
    let last = posts.len() as u32;
    lose_every_other_article(&mut pipeline, job_id, &posts, last).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let disk = pipeline.graph_jobs[&job_id].disk().unwrap().clone();
    let (_, output) = working_and_output(&pipeline, job_id, &staging_dir);
    let downloaded = budget_waits(&output);
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let mut hold = None;
    while !pipeline.graph_jobs[&job_id].applying_repair() {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if repaired(&said) {
            disk.barrier().wait_aside();
            hold = Some(pipeline.volume_writers.hold(&output));
        }
        pipeline.graph_run_said(said);
    }
    let hold = hold.expect("the repair came back");
    // The reader's first slice is queued, and it stands for the second.
    let hold = hear_until_one_stands(&mut pipeline, hold).await;
    stop_past_a_still_volume(&mut pipeline).await;
    let graph = &pipeline.graph_jobs[&job_id];
    assert!(!graph.applying_repair());
    assert!(graph.disk().unwrap().left_at_stop());
    assert!(
        graph.pending().given_up(),
        "the job is left at its last checkpoint"
    );
    assert_eq!(staged_repair(&working_dir), None);
    assert_eq!(budget_waits(&output) - downloaded, 0);
    assert_ne!(
        job_status_for_assert(&pipeline, job_id)
            .map(|status| matches!(status, JobStatus::Failed { .. })),
        Some(true)
    );
    hold.open();
    pipeline.volume_writers.settle();
    retire_pipeline_database(pipeline).await;
}

// A repair whose staged output cannot be read back: the first slice fails,
// nothing more is read, the job fails with the read's error, and the staged
// output goes.
#[tokio::test]
async fn a_repair_whose_slice_cannot_be_read_back_fails_the_job() {
    let _threads = threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(92_496);
    let data = payload(8 * ARTICLE, 4);
    let mut posts = vec![Post::plain("lantern.bin", data.clone())];
    posts.extend(par2_set("lantern", &[("lantern.bin", &data)], 4));
    let (mut pipeline, _) = delivered_apart(&temp_dir, job_id, &posts, None).await;
    let last = posts.len() as u32;
    lose_every_other_article(&mut pipeline, job_id, &posts, last).await;
    assert!(pipeline.graph_jobs[&job_id].repair_out());
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let mut slices = 0u32;
    while !matches!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Failed { .. })
    ) {
        let said = pipeline.graph_runs.recv().await.unwrap();
        if repaired(&said) {
            // The output is cut short before the reader opens it.
            let staged = staged_repair(&working_dir).expect("the repair staged its output");
            std::fs::OpenOptions::new()
                .write(true)
                .open(staged)
                .unwrap()
                .set_len(0)
                .unwrap();
        }
        if matches!(&said.heard, Heard::RepairSlice(_)) {
            slices += 1;
        }
        pipeline.graph_run_said(said);
    }
    assert_eq!(slices, 1, "nothing was read after the failed slice");
    assert_eq!(staged_repair(&working_dir), None);
    settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.jobs.contains_key(&job_id));
    retire_pipeline_database(pipeline).await;
}
