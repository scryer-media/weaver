// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;


// The moving row, naming the destination, commits before the first file
// leaves the working directory: a restart at any point of the move finds the
// delivery instead of a job still downloading.
#[tokio::test]
async fn moving_row_commits_before_the_first_file_moves() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(&temp).await;
    let id = JobId(10301);
    let spec = minimal_job_state(id, "durable delivery", PathBuf::new()).spec;
    let working_dir = insert_active_job(&mut pipeline, id, spec).await;
    tokio::fs::write(working_dir.join("payload.bin"), b"payload")
        .await
        .unwrap();
    pipeline.report_all_for_test();

    let (seen_tx, seen_rx) = std::sync::mpsc::channel();
    let db = pipeline.db.clone();
    let payload = working_dir.join("payload.bin");
    *crate::pipeline::completion::finalize::BEFORE_FIRST_MOVE
        .lock()
        .unwrap() = Some(Box::new(move |job_id| {
        let row = db.load_active_job(job_id).unwrap().unwrap();
        let _ = seen_tx.send((row.status, row.output_dir, payload.exists()));
    }));

    pipeline.start_move_to_complete(id).await.unwrap();
    let done = pipeline.move_done_rx.recv().await.unwrap();
    let destination = complete_dir.join("durable delivery");
    let (status, output_dir, payload_unmoved) = seen_rx
        .try_recv()
        .expect("the move started without its moving row committed");
    assert_eq!(status, "moving");
    assert_eq!(output_dir, destination);
    assert!(payload_unmoved);
    pipeline.handle_move_to_complete_done(done).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        b"payload"
    );
    pipeline.db.flush_write_queue().await.unwrap();
}

// Runs the delivered one-article job up to its final move and hands back
// the move's done message, unhandled: what a restart finds is what the move
// left, with the terminal pass never begun.
async fn move_without_hearing_it(
    pipeline: &mut Pipeline,
    job_id: JobId,
    job_name: &str,
    payload: &[u8],
) -> (PathBuf, MoveToCompleteDone) {
    let working_dir =
        insert_delivered_job(pipeline, job_id, job_name, "payload.bin", payload).await;
    tokio::fs::write(working_dir.join("sidecar.nfo"), b"sidecar")
        .await
        .unwrap();
    // The job's working directory carries its ownership marker, as every
    // directory a job is added with does.
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, job_id)
        .await
        .unwrap();
    pipeline.check_job_completion(job_id).await;
    // The download's queue-script barrier runs no script and lets it on.
    while !pipeline.inflight_moves.contains(&job_id) {
        if let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            continue;
        }
        match pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::QueueAdmitted(id) => {
                pipeline.handle_queue_scripts_admitted(id);
            }
            TerminalPostProcessingEvent::QueueDone(id, result) => {
                pipeline.handle_queue_scripts_done(id, result).await;
            }
            _ => panic!("no script runs before the move"),
        }
    }
    let done = pipeline.move_done_rx.recv().await.unwrap();
    assert_eq!(done.job_id, job_id);
    (working_dir, done)
}

// Restarts on the same directories and restores the job as startup does.
async fn restart_and_restore(
    temp: &tempfile::TempDir,
    intermediate_dir: &std::path::Path,
    job_id: JobId,
) -> Pipeline {
    let (mut restored, _, _) = new_direct_pipeline(temp).await;
    let startup = crate::operations::recovery::recover_server_state(
        &restored.db,
        &temp.path().join("data"),
        intermediate_dir,
    )
    .await
    .unwrap();
    let request = startup
        .to_restore
        .into_iter()
        .find(|candidate| candidate.job_id == job_id)
        .expect("the job must come back as in-progress work")
        .request;
    assert_eq!(request.status, JobStatus::Moving);
    restored.restore_job(request).await.unwrap();
    // Every article is in hand: nothing is queued to fetch and no graph
    // is restored from a checkpoint. A job with nothing left to move may
    // already have finished.
    assert!(restored.jobs.get(&job_id).is_none_or(|state| matches!(
        state.articles,
        crate::jobs::model::JobArticles::Queued(_)
    )));
    assert!(!restored.graph_jobs.contains_key(&job_id));
    restored
}

async fn finished_once(restored: &mut Pipeline, job_id: JobId, complete_dir: &std::path::Path) {
    let finished = restored
        .finished_jobs()
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .expect("the job finishes");
    assert_eq!(finished.status, JobStatus::Complete);
    // One delivery, never a second `<name>.#<id>` claim.
    assert_eq!(std::fs::read_dir(complete_dir).unwrap().count(), 1);
}

// The move finished and the restart came before the terminal pass wrote
// anything: the job comes back in its terminal pass and completes on what
// was delivered, fetching nothing.
#[tokio::test]
async fn restart_after_the_final_move_completes_the_delivered_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10302);
    let job_name = "Slate Harbour Delivered";
    let payload = b"delivered payload".to_vec();
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(done.result.is_ok());
    assert!(!working_dir.exists());
    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(restored.inflight_moves.is_empty());
    settle_inflight_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
}

// A restart part way through the move: what is left in the working
// directory moves into the same destination, and the job completes.
#[tokio::test]
async fn restart_mid_move_finishes_into_the_same_destination() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp).await;
    let job_id = JobId(10303);
    let job_name = "Slate Harbour Midway";
    let payload = b"delivered payload".to_vec();
    let destination = complete_dir.join(crate::jobs::working_dir::sanitize_dirname(job_name));
    // The first file moves, and the move goes no further: the destination
    // now holds something, so the move refuses it and stops as the restart
    // would have stopped it.
    let first_moved = destination.join("payload.bin");
    let intermediate = intermediate_dir.clone();
    *crate::pipeline::completion::finalize::BEFORE_FIRST_MOVE
        .lock()
        .unwrap() = Some(Box::new(move |job_id| {
        let working_dir = crate::jobs::working_dir::find_job_working_dir(&intermediate, job_id)
            .unwrap()
            .unwrap();
        std::fs::rename(working_dir.join("payload.bin"), &first_moved).unwrap();
    }));
    let (working_dir, done) =
        move_without_hearing_it(&mut pipeline, job_id, job_name, &payload).await;
    assert!(done.result.is_err());
    assert!(working_dir.join("sidecar.nfo").exists());
    assert!(destination.join("payload.bin").exists());
    retire_pipeline_database(pipeline).await;

    let mut restored = restart_and_restore(&temp, &intermediate_dir, job_id).await;
    assert!(restored.inflight_moves.contains(&job_id));
    settle_inflight_moves(&mut restored).await;
    finished_once(&mut restored, job_id, &complete_dir).await;
    assert!(!working_dir.exists());
    assert_eq!(
        tokio::fs::read(destination.join("payload.bin"))
            .await
            .unwrap(),
        payload
    );
    assert_eq!(
        tokio::fs::read(destination.join("sidecar.nfo"))
            .await
            .unwrap(),
        b"sidecar"
    );
}
