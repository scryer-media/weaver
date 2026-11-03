// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::future::Future;

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

// A repeated-article cache that cannot be prepared fails the job when its
// articles are built, through the failure path, and never leaves it waiting.
#[tokio::test]
async fn a_repeated_article_cache_failure_fails_the_job_when_its_articles_are_built() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9201);
    // Two files posted from the same article: an NZB repeats an article
    // across files, never within one.
    let mut spec = standalone_job_spec(
        "cache admission fixture",
        &[
            ("payload.bin".to_string(), 100),
            ("copy.bin".to_string(), 100),
        ],
    );
    spec.files[1].segments[0].message_ids = spec.files[0].segments[0].message_ids.clone();
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
    let blocker = working_dir.join(weaver_graph::names::CACHE_DIR);
    tokio::fs::write(&blocker, b"not a cache directory")
        .await
        .unwrap();

    let nzb_zstd = nzb_zstd_for(&spec);
    handle
        .add_job(id, spec, PathBuf::from("cache-fixture.nzb"), nzb_zstd)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.repeated_articles.contains_key(&id));
    activate_job(&mut pipeline, id).await;
    assert!(!pipeline.repeated_articles.contains_key(&id));
    // The failed job leaves once its working directory is released off the
    // actor.
    settle_failed_releases(&mut pipeline).await;
    assert!(!pipeline.jobs.contains_key(&id));
    let status = pipeline
        .finished_jobs()
        .await
        .into_iter()
        .find(|job| job.job_id == id)
        .map(|job| job.status);
    assert!(
        matches!(&status, Some(JobStatus::Failed { error }) if error.contains("article cache scratch path is not a directory")),
        "expected the staged cache preparation failure, got {status:?}"
    );
    assert!(blocker.is_file());
}

// Renaming an idle job moves its working directory with the repeated-article
// cache inside it. The cache must follow: dropping it at archive cleans the
// directory under the new name, never a path the rename already vacated.
#[tokio::test]
async fn a_rename_carries_the_repeated_article_cache_to_the_new_working_dir() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9203);
    // Two files posted from the same article: an NZB repeats an article
    // across files, never within one.
    let mut spec = standalone_job_spec(
        "cache rename fixture",
        &[
            ("payload.bin".to_string(), 100),
            ("copy.bin".to_string(), 100),
        ],
    );
    spec.files[1].segments[0].message_ids = spec.files[0].segments[0].message_ids.clone();
    let nzb_zstd = nzb_zstd_for(&spec);
    handle
        .add_job(id, spec, PathBuf::from("cache-rename.nzb"), nzb_zstd)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    activate_job(&mut pipeline, id).await;
    assert!(pipeline.repeated_articles.contains_key(&id));
    let old_dir = pipeline.jobs[&id].working_dir.clone();
    let caches = |dir: &std::path::Path| -> Vec<PathBuf> {
        std::fs::read_dir(dir.join(weaver_graph::names::CACHE_DIR))
            .into_iter()
            .flatten()
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .is_some_and(|name| name.to_string_lossy().starts_with("article-cache-"))
            })
            .collect()
    };
    assert_eq!(caches(&old_dir).len(), 1);

    assert!(
        pipeline
            .rename_working_dir_when_quiet(id, "cache rename fixture renamed")
            .await
    );
    let new_dir = pipeline.jobs[&id].working_dir.clone();
    assert_ne!(new_dir, old_dir);
    assert!(!old_dir.exists());
    assert_eq!(caches(&new_dir).len(), 1);

    pipeline.repeated_articles.remove(&id);
    assert!(
        caches(&new_dir).is_empty(),
        "the dropped cache must remove its directory under the new name"
    );
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
            nzb_zstd_for(
                &(segmented_job_spec("shutdown admission fixture", "payload.bin", &[100])),
            ),
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
    finish_cancel_jobs(&mut pipeline).await;
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

// A cancelled job leaves the queue only when its row does: on the done event
// after its history row has committed. Until then every queue reader still
// lists it as it was, and mutations against it stay refused.
#[tokio::test]
async fn a_cancelled_job_stays_listed_until_its_history_row_commits() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9210);
    insert_active_job(
        &mut pipeline,
        id,
        segmented_job_spec("cancel visibility fixture", "payload.bin", &[100]),
    )
    .await;
    pipeline.report_all_for_test();
    let listed = |pipeline: &Pipeline| {
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.visible_jobs().any(|job| job.job_id == id))
    };
    let (release, released) = std::sync::mpsc::channel::<()>();
    pipeline
        .db
        .try_queue_write("hold_cancel_archive", move |_| {
            let _ = released.recv();
            Ok(())
        })
        .unwrap();
    let mut events = handle.subscribe_events();

    handle.cancel_job(id).await.unwrap();
    assert!(
        listed(&pipeline),
        "the cancel intent alone must not hide the job"
    );
    pipeline.apply_queue_intents().await;
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == id && row.report.retiring)
    }));
    assert!(
        listed(&pipeline),
        "the job stays listed while its archive is held"
    );
    assert!(handle.list_jobs().iter().any(|job| job.job_id == id));
    assert_eq!(handle.get_job(id).unwrap().job_id, id);
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::JobNotFound(_))
    ));

    drop(release);
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert!(!listed(&pipeline), "the done event drops the row");
    assert!(
        pipeline.db.get_job_history(id.0).unwrap().is_some(),
        "the row leaves only once its history row exists"
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
    state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
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

    // The article out on a lane kept the paused job's articles. It comes
    // back, and the job resumes before a turn parks it: it still holds its
    // articles, with nothing left to fetch.
    assert!(pipeline.jobs[&id].articles.active().is_some());
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
    finish_cancel_jobs(&mut pipeline).await;
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
async fn pause_refused_after_downloads_drain_clears_the_intent_and_releases_the_deferred_move() {
    for (index, post_processing_paused, superseded_pause) in
        [(0, false, false), (1, true, false), (2, false, true)]
    {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let handle = handle(&pipeline);
        let id = JobId(9207 + index);
        insert_delivered_job(
            &mut pipeline,
            id,
            "pause before delivery",
            "payload.bin",
            b"payload",
        )
        .await;
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
        let watermark = handle.job_revision();
        pipeline.apply_queue_intents().await;
        // The download work drained before the boundary, so the pipeline
        // refuses the pause: the job stays live, the view shows no pause
        // intent, and the move deferred behind it is released.
        assert!(handle.job_revision() > watermark);
        assert!(pipeline.deferred_moves.contains(&id));
        assert!(!pipeline.inflight_moves.contains(&id));
        assert_ne!(
            pipeline.jobs[&id].run_state,
            crate::jobs::model::RunState::Paused
        );
        assert!(pipeline.shared_state.queue.read(|inner| {
            inner
                .jobs
                .iter()
                .any(|row| row.id == id && row.intents.paused != Some(true))
        }));
        assert_ne!(handle.get_job(id).unwrap().status, JobStatus::Paused);
        assert!(pipeline.pending_completion_checks.contains(&id));
        let mut events = handle.subscribe_events();
        pipeline.check_job_completion(id).await;
        // The released move is in flight, so the wait below has a move to hear.
        assert!(
            pipeline.inflight_moves.contains(&id),
            "{:?}",
            pipeline.jobs.get(&id).map(|state| &state.status)
        );
        let done = pipeline.move_done_rx.recv().await.unwrap();
        assert_eq!(done.job_id, id);
        assert!(done.result.is_ok());
        pipeline.handle_move_to_complete_done(done).await;
        let terminal = loop {
            match events.recv().await.unwrap() {
                event @ (PipelineEvent::JobCompleted { job_id }
                | PipelineEvent::JobFailed { job_id, .. })
                    if job_id == id =>
                {
                    break event;
                }
                _ => {}
            }
        };
        assert!(
            matches!(terminal, PipelineEvent::JobCompleted { .. }),
            "{terminal:?}"
        );
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
    assert!(!state.articles.active().unwrap().download_queue.is_empty());
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

    // Nothing was out, so the pause let the articles go at once: the resumed
    // job is built again from its stored NZB, and its queue emptied as the
    // fixture's was.
    assert!(pipeline.jobs[&id].articles.active().is_none());
    handle.resume_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    crate::pipeline::tests::activate_job(&mut pipeline, id).await;
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.report_job(id);
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
}
