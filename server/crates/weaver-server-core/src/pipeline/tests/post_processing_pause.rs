use super::*;

#[tokio::test]
async fn post_processing_pause_holds_admission_and_resume_rechecks_jobs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let repair = JobId(31001);
    let extract = JobId(31002);
    let untouched = JobId(31004);
    for id in [repair, extract, untouched] {
        insert_active_job(
            &mut pipeline,
            id,
            standalone_job_spec("held", &[("payload.bin".to_owned(), 512)]),
        )
        .await;
    }
    pipeline.pending_completion_checks.clear();
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PausePostProcessing { reply })
        .await;
    received.await.unwrap();
    assert!(pipeline.shared_state.is_post_processing_paused());
    assert!(!pipeline.maybe_start_repair(repair).await);
    assert!(!pipeline.maybe_start_extraction(extract).await);
    assert_eq!(pipeline.active_repair_jobs(), 0);
    assert_eq!(pipeline.active_extract_jobs(), 0);

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ResumePostProcessing { reply })
        .await;
    received.await.unwrap();
    assert!(!pipeline.shared_state.is_post_processing_paused());
    assert!(pipeline.pending_completion_checks.contains(&repair));
    assert!(pipeline.pending_completion_checks.contains(&extract));
    assert!(!pipeline.pending_completion_checks.contains(&untouched));
    pipeline.pending_completion_checks.clear();
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ResumePostProcessing { reply })
        .await;
    received.await.unwrap();
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "an already running post-processor must not rescan the queue"
    );
    assert!(pipeline.maybe_start_repair(repair).await);
    assert!(pipeline.maybe_start_extraction(extract).await);
}

#[tokio::test]
async fn post_processing_pause_defers_moves_without_cancelling_running_stages() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(31003);
    insert_active_job(
        &mut pipeline,
        id,
        standalone_job_spec("held move", &[("payload.bin".to_owned(), 512)]),
    )
    .await;
    assert!(pipeline.maybe_start_extraction(id).await);
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PausePostProcessing { reply })
        .await;
    received.await.unwrap();
    assert!(matches!(pipeline.jobs[&id].status, JobStatus::Extracting));
    pipeline.start_move_to_complete(id).await.unwrap();
    assert!(pipeline.deferred_moves.contains(&id));
    assert!(!pipeline.inflight_moves.contains(&id));
}
