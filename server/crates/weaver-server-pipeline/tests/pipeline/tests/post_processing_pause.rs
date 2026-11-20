// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_pipeline::pipeline::tests::{
    control_fixture::{completion_pending, move_deferred, move_in_flight, start_move_to_complete},
    insert_active_job, new_direct_pipeline,
    queue_fixture::{apply_intents, handle as scheduler, installed_status, report_all},
    standalone_job_spec,
};
use weaver_server_pipeline::{JobId, JobStatus};

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
    report_all(&pipeline);
    scheduler(&pipeline).pause_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;
    start_move_to_complete(&mut pipeline, id).await.unwrap();
    assert!(move_deferred(&pipeline, id));
    let handle = scheduler(&pipeline);
    handle.pause_job(id).await.unwrap();
    handle.resume_post_processing().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(matches!(
        installed_status(&pipeline, id),
        Some(JobStatus::Paused)
    ));
    assert!(!move_in_flight(&pipeline, id));
    assert!(!completion_pending(&pipeline, id));

    handle.resume_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(completion_pending(&pipeline, id));
}
