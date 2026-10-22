// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::future::Future;

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

#[tokio::test]
async fn repeated_article_cache_failure_refuses_admission_before_persistence() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9201);
    let mut spec = segmented_job_spec("cache admission fixture", "payload.bin", &[100, 100]);
    spec.files[0].segments[1].message_id = spec.files[0].segments[0].message_id.clone();
    // Occupy the unsuffixed name first, so admission selects the same
    // job-specific directory in which the cache failure is staged.
    let collision =
        crate::jobs::working_dir::compute_working_dir(&pipeline.intermediate_dir, id, &spec.name);
    tokio::fs::create_dir_all(collision).await.unwrap();
    let working_dir =
        crate::jobs::working_dir::compute_working_dir(&pipeline.intermediate_dir, id, &spec.name);
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    crate::jobs::working_dir::stamp_working_dir(&pipeline.intermediate_dir, &working_dir, id)
        .await
        .unwrap();
    let blocker = working_dir.join(".weaver-chunks");
    tokio::fs::write(&blocker, b"not a cache directory")
        .await
        .unwrap();

    let result = handle
        .add_job(
            id,
            spec.clone(),
            PathBuf::from("cache-fixture.nzb"),
            sample_nzb_zstd(),
        )
        .await;
    assert!(
        matches!(&result, Err(SchedulerError::InvalidInput(error)) if error == "article cache scratch path is not a directory"),
        "expected the staged cache preparation failure, got {result:?}"
    );
    assert!(handle.get_job(id).is_err());
    assert!(!pipeline.db.load_active_jobs().unwrap().contains_key(&id));
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_none());
    assert!(blocker.is_file());

    tokio::fs::remove_file(blocker).await.unwrap();
    handle
        .add_job(
            id,
            spec,
            PathBuf::from("cache-fixture.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.jobs.contains_key(&id));
    assert!(pipeline.repeated_articles.contains_key(&id));
}

#[tokio::test]
async fn shutdown_services_terminal_completion_needed_by_pending_readmission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9202);
    handle
        .add_job(
            id,
            segmented_job_spec("shutdown admission fixture", "payload.bin", &[100]),
            PathBuf::from("shutdown-fixture.nzb"),
            sample_nzb_zstd(),
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    let (cancelled, _) = tokio::sync::watch::channel(false);
    pipeline
        .terminal_post_processing_cancellations
        .insert(id, cancelled);
    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.pending_cleanup.contains(&id))
    );

    let mut readmission = std::pin::pin!(handle.redownload_job(id));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(readmission.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.admissions.contains(&id))
    );
    handle.shutdown().await.unwrap();
    pipeline
        .terminal_post_processing_done_tx
        .send(TerminalPostProcessingEvent::Done(
            TerminalPostProcessingDone {
                job_id: id,
                primary_failure: None,
                result: Ok(crate::post_processing::executor::JobPostProcessingReport {
                    summary: crate::post_processing::model::PostProcessingSummary::Interrupted,
                    results: Vec::new(),
                }),
            },
        ))
        .await
        .unwrap();

    let ((), result) = tokio::join!(pipeline.run(), readmission);
    result.unwrap();
    assert!(
        !pipeline
            .terminal_post_processing_cancellations
            .contains_key(&id)
    );
    assert!(
        pipeline.shared_state.queue.read(|inner| {
            !inner.admissions.contains(&id) && !inner.pending_cleanup.contains(&id)
        })
    );
    assert!(pipeline.db.load_active_jobs().unwrap().contains_key(&id));
}

#[tokio::test]
async fn retiring_jobs_refuse_new_mutations_before_archive_acknowledgement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9203);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("retiring mutation fixture", "payload.bin", &[100]),
    )
    .await;
    let (release, released) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_write("hold_retiring_archive", move |_| {
            let _ = released.recv();
            Ok(())
        })
        .unwrap();
    let mut events = handle.subscribe_events();
    pipeline.complete_job_after_terminal_post_processing(id);
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == id && row.report.retiring)
    }));
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.resume_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle
            .update_job(
                id,
                crate::JobUpdate {
                    category: crate::jobs::FieldUpdate::Set("changed-category".into()),
                    ..Default::default()
                }
            )
            .await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        handle.cancel_semantic_bad(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
    drop(release);
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCompleted { job_id } if job_id == id)
    {
    }
    assert_eq!(
        pipeline.db.get_job_history(id.0).unwrap().unwrap().status,
        "complete"
    );
}

#[tokio::test]
async fn extraction_pause_uses_current_download_activity() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9204);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("extracting pause fixture", "payload.bin", &[100]),
    )
    .await;
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.status = JobStatus::Extracting;
    state.download_state = crate::DownloadState::Complete;
    state.post_state = crate::jobs::model::PostState::Extracting;
    state.run_state = crate::jobs::model::RunState::Active;
    state.download_queue = DownloadQueue::new();
    pipeline.active_downloads_by_job.insert(id, 1);
    pipeline.report_job(id);
    assert_eq!(
        handle.get_job(id).unwrap().download_state,
        crate::DownloadState::Downloading
    );
    handle.pause_job(id).await.unwrap();
    handle.pause_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.jobs[&id].run_state,
        crate::jobs::model::RunState::Paused
    );

    pipeline.active_downloads_by_job.remove(&id);
    handle.resume_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
}

#[tokio::test]
async fn final_move_claim_refuses_pause_before_the_moving_status_is_reported() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9210);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("claimed move fixture", "payload.bin", &[100]),
    )
    .await;
    pipeline.report_job(id);
    // Destination preparation awaits after claiming the move and before the
    // actor transitions its legacy status or launches the move worker.
    pipeline.shared_state.queue.write(|inner| {
        let row = inner.jobs.iter_mut().find(|row| row.id == id).unwrap();
        assert_eq!(row.report.job.status, JobStatus::Downloading);
        row.report.moving = true;
        crate::pipeline::queue::Change::changed(())
    });
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
    pipeline.shared_state.queue.read(|inner| {
        let row = inner.jobs.iter().find(|row| row.id == id).unwrap();
        assert_eq!(row.intents.paused, None);
        assert_eq!(row.intents.seq, 0);
    });
    handle.resume_job(id).await.unwrap();
}

#[tokio::test]
async fn terminal_capture_keeps_updates_accepted_before_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9205);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("terminal update fixture", "payload.bin", &[100]),
    )
    .await;
    handle
        .update_job(
            id,
            crate::JobUpdate {
                name: Some("updated terminal title".into()),
                category: crate::jobs::FieldUpdate::Set("updated-category".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = handle.subscribe_events();
    pipeline.complete_job_after_terminal_post_processing(id);
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCompleted { job_id } if job_id == id)
    {
    }
    let history = pipeline.db.get_job_history(id.0).unwrap().unwrap();
    assert_eq!(history.name, "updated terminal title");
    assert_eq!(history.category.as_deref(), Some("updated-category"));
    let published = handle.get_job(id).unwrap();
    assert_eq!(published.name, history.name);
    assert_eq!(published.category, history.category);
}

#[tokio::test]
async fn terminal_capture_honors_cancellation_accepted_before_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9206);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("terminal cancellation fixture", "payload.bin", &[100]),
    )
    .await;
    let mut events = handle.subscribe_events();
    handle.cancel_job_keeping_files(id).await.unwrap();
    pipeline.complete_job_after_terminal_post_processing(id);
    loop {
        match events.recv().await.unwrap() {
            PipelineEvent::JobCompleted { job_id } if job_id == id => {
                panic!("accepted cancellation was overwritten by completion");
            }
            PipelineEvent::JobCancelled { job_id } if job_id == id => break,
            _ => {}
        }
    }
    assert_eq!(
        pipeline.db.get_job_history(id.0).unwrap().unwrap().status,
        "cancelled"
    );
    assert!(handle.get_job(id).is_err());
    assert!(!pipeline.jobs.contains_key(&id));
}

#[tokio::test]
async fn accepted_pause_survives_draining_downloads_and_resumes_the_deferred_move() {
    for (index, post_processing_paused, superseded_pause) in
        [(0, false, false), (1, true, false), (2, false, true)]
    {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let handle = handle(&pipeline);
        let id = JobId(9207 + index);
        let spec = minimal_job_state(id, "pause before delivery", PathBuf::new()).spec;
        let working_dir = insert_active_job(&mut pipeline, id, spec).await;
        tokio::fs::write(working_dir.join("payload.bin"), b"payload")
            .await
            .unwrap();
        let state = pipeline.jobs.get_mut(&id).unwrap();
        state.status = JobStatus::Extracting;
        state.download_state = crate::DownloadState::Complete;
        state.post_state = crate::jobs::model::PostState::Extracting;
        pipeline.active_downloads_by_job.insert(id, 1);
        pipeline.report_job(id);
        if post_processing_paused {
            handle.pause_post_processing().await.unwrap();
            pipeline.apply_queue_intents().await;
            pipeline.start_move_to_complete(id).await.unwrap();
        }
        handle.pause_job(id).await.unwrap();
        pipeline.active_downloads_by_job.remove(&id);
        if post_processing_paused {
            handle.resume_post_processing().await.unwrap();
        } else {
            pipeline.start_move_to_complete(id).await.unwrap();
        }
        if superseded_pause {
            handle.resume_job(id).await.unwrap();
        }
        pipeline.apply_queue_intents().await;
        assert!(pipeline.deferred_moves.contains(&id));
        assert!(!pipeline.inflight_moves.contains(&id));
        if !superseded_pause {
            assert_eq!(
                pipeline.jobs[&id].run_state,
                crate::jobs::model::RunState::Paused
            );
            handle.resume_job(id).await.unwrap();
            pipeline.apply_queue_intents().await;
        }
        assert!(pipeline.pending_completion_checks.contains(&id));
        let mut events = handle.subscribe_events();
        pipeline.check_job_completion(id).await;
        let done = pipeline.move_done_rx.recv().await.unwrap();
        assert_eq!(done.job_id, id);
        assert!(done.result.is_ok());
        pipeline.handle_move_to_complete_done(done).await;
        while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCompleted { job_id } if job_id == id)
        {
        }
        assert_eq!(
            pipeline.db.get_job_history(id.0).unwrap().unwrap().status,
            "complete"
        );
    }
}

#[tokio::test]
async fn extraction_with_queued_download_work_can_pause_without_current_activity() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9210);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("queued extraction pause", "payload.bin", &[100]),
    )
    .await;
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.status = JobStatus::Extracting;
    state.download_state = crate::DownloadState::Complete;
    state.post_state = crate::jobs::model::PostState::Extracting;
    assert!(!state.download_queue.is_empty());
    assert!(!pipeline.job_has_current_download_activity(id));
    pipeline.report_job(id);
    assert_eq!(
        handle.get_job(id).unwrap().download_state,
        crate::DownloadState::Complete
    );
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == id && row.report.download_work_pending)
    }));
    handle.pause_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.jobs[&id].run_state,
        crate::jobs::model::RunState::Paused
    );

    handle.resume_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.jobs.get_mut(&id).unwrap().download_queue = DownloadQueue::new();
    pipeline.report_job(id);
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
}
