// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

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
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.shared_state.is_post_processing_paused());
    assert!(!pipeline.maybe_start_repair(repair).await);
    assert!(!pipeline.maybe_start_extraction(extract).await);
    assert_eq!(pipeline.active_repair_jobs(), 0);
    assert_eq!(pipeline.active_extract_jobs(), 0);

    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .resume_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.shared_state.is_post_processing_paused());
    assert!(pipeline.pending_completion_checks.contains(&repair));
    assert!(pipeline.pending_completion_checks.contains(&extract));
    assert!(!pipeline.pending_completion_checks.contains(&untouched));
    pipeline.pending_completion_checks.clear();
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .resume_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(
        pipeline.pending_completion_checks.is_empty(),
        "an already running post-processor must not rescan the queue"
    );
    assert!(pipeline.maybe_start_repair(repair).await);
    assert!(pipeline.maybe_start_extraction(extract).await);
}

#[tokio::test]
async fn post_processing_resume_preserves_manual_pause_on_deferred_move() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(31012);
    insert_active_job(
        &mut pipeline,
        id,
        standalone_job_spec("manually held move", &[("payload.bin".to_owned(), 512)]),
    )
    .await;
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.start_move_to_complete(id).await.unwrap();
    assert!(pipeline.deferred_moves.contains(&id));
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.pause_job(id).await.unwrap();
    handle.resume_post_processing().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(matches!(pipeline.jobs[&id].status, JobStatus::Paused));
    assert!(!pipeline.inflight_moves.contains(&id));
    assert!(!pipeline.pending_completion_checks.contains(&id));

    handle.resume_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.pending_completion_checks.contains(&id));
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
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(matches!(pipeline.jobs[&id].status, JobStatus::Extracting));
    pipeline.start_move_to_complete(id).await.unwrap();
    assert!(pipeline.deferred_moves.contains(&id));
    assert!(!pipeline.inflight_moves.contains(&id));
}

#[tokio::test]
async fn post_processing_resume_requeues_deferred_completion_and_rar_gates() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let completion = JobId(31005);
    let rar = JobId(31006);
    for id in [completion, rar] {
        insert_active_job(
            &mut pipeline,
            id,
            standalone_job_spec("deferred completion", &[("payload.bin".to_owned(), 512)]),
        )
        .await;
    }
    pipeline.pending_completion_checks.clear();
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .pause_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.check_job_completion(completion).await;
    pipeline.try_rar_extraction(rar).await;
    assert!(pipeline.deferred_post_processing.contains(&completion));
    assert!(pipeline.deferred_post_processing.contains(&rar));
    assert!(pipeline.pending_completion_checks.is_empty());
    pipeline.report_all_for_test();
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
        .resume_post_processing()
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.pending_completion_checks.contains(&completion));
    assert!(pipeline.pending_completion_checks.contains(&rar));
    assert!(pipeline.deferred_post_processing.is_empty());
}
