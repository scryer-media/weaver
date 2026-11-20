// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

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
