// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{future::Future, path::PathBuf};

use weaver_server_base::{events::model::PipelineEvent, jobs::ids::JobId};
use weaver_server_composition::{
    DownloadState, FieldUpdate, JobStatus, JobUpdate, RunState, SchedulerError,
};
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::pipeline::Pipeline;
use weaver_server_pipeline::pipeline::tests::{
    activate_job,
    control_fixture::*,
    finish_cancel_jobs,
    history_fixture::{
        check_completion, complete_after_post_processing, database, finished_jobs,
        hold_database_writer,
    },
    insert_active_job, insert_delivered_job, new_direct_pipeline, nzb_zstd_for,
    queue_fixture::{
        apply_intents, cleanup_pending, clear_download_queue, download_queue_len, handle,
        installed_status, report_all, report_job, set_article_in_flight, working_dir,
    },
    segmented_job_spec, settle_failed_releases, standalone_job_spec,
    window_fixture::{articles_active, has_installed, intermediate_dir, retiring},
};

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
    let collision = weaver_server_pipeline::jobs::working_dir::compute_working_dir(
        intermediate_dir(&pipeline),
        id,
        &spec.name,
    );
    tokio::fs::create_dir_all(collision).await.unwrap();
    let working_dir = weaver_server_pipeline::jobs::working_dir::compute_working_dir(
        intermediate_dir(&pipeline),
        id,
        &spec.name,
    );
    tokio::fs::create_dir_all(&working_dir).await.unwrap();
    weaver_server_pipeline::jobs::working_dir::stamp_working_dir(
        intermediate_dir(&pipeline),
        &working_dir,
        id,
    )
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
    apply_intents(&mut pipeline).await;
    assert!(!has_repeated_article_cache(&pipeline, id));
    activate_job(&mut pipeline, id).await;
    assert!(!has_repeated_article_cache(&pipeline, id));
    // The failed job leaves once its working directory is released off the
    // actor.
    settle_failed_releases(&mut pipeline).await;
    assert!(!has_installed(&pipeline, id));
    let status = finished_jobs(&mut pipeline)
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
async fn a_rename_of_an_active_job_keeps_its_folder_and_article_cache() {
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
    apply_intents(&mut pipeline).await;
    activate_job(&mut pipeline, id).await;
    assert!(has_repeated_article_cache(&pipeline, id));
    let old_dir = working_dir(&pipeline, id).clone();
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

    assert!(rename_when_quiet(&mut pipeline, id, "cache rename fixture renamed").await);
    assert_eq!(*working_dir(&pipeline, id), old_dir);
    assert_eq!(caches(&old_dir).len(), 1);

    drop_repeated_article_cache(&mut pipeline, id);
    assert!(
        caches(&old_dir).is_empty(),
        "the dropped cache must remove its directory"
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
    apply_intents(&mut pipeline).await;
    hold_terminal_post_processing(&mut pipeline, id);
    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert!(cleanup_pending(&pipeline, id));

    let mut readmission = std::pin::pin!(handle.redownload_job(id));
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(readmission.as_mut().poll(cx)))
            .await
            .is_pending()
    );
    assert!(admission_pending(&pipeline, id));
    handle.shutdown().await.unwrap();
    report_interrupted_terminal_post_processing(&pipeline, id).await;

    let ((), result) = tokio::join!(pipeline.run(), readmission);
    result.unwrap();
    assert!(!terminal_post_processing_held(&pipeline, id));
    assert!(admission_and_cleanup_finished(&pipeline, id));
    assert!(
        database(&pipeline)
            .load_active_jobs()
            .unwrap()
            .contains_key(&id)
    );
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
    report_all(&pipeline);
    let listed = |pipeline: &Pipeline| visible_job(pipeline, id);
    let release = hold_database_writer(&pipeline);
    let mut events = handle.subscribe_events();

    handle.cancel_job(id).await.unwrap();
    assert!(
        listed(&pipeline),
        "the cancel intent alone must not hide the job"
    );
    apply_intents(&mut pipeline).await;
    assert!(retiring(&pipeline, id));
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
        database(&pipeline).get_job_history(id.0).unwrap().is_some(),
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
    set_extracting(&mut pipeline, id);
    set_active_run_state(&mut pipeline, id);
    clear_download_queue(&mut pipeline, id);
    set_article_in_flight(&mut pipeline, id, true);
    report_job(&mut pipeline, id);
    assert_eq!(
        handle.get_job(id).unwrap().download_state,
        DownloadState::Downloading
    );
    handle.pause_job(id).await.unwrap();
    handle.pause_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(run_state(&pipeline, id), RunState::Paused);

    // The article out on a lane kept the paused job's articles. It comes
    // back, and the job resumes before a turn parks it: it still holds its
    // articles, with nothing left to fetch.
    assert!(articles_active(&pipeline, id));
    set_article_in_flight(&mut pipeline, id, false);
    handle.resume_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
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
    report_job(&mut pipeline, id);
    // Destination preparation awaits after claiming the move and before the
    // actor transitions its legacy status or launches the move worker.
    assert_eq!(claim_reported_move(&pipeline, id), JobStatus::Downloading);
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
    let (paused, seq) = pause_intent(&pipeline, id);
    assert_eq!(paused, None);
    assert_eq!(seq, 0);
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
            JobUpdate {
                name: Some("updated terminal title".into()),
                category: FieldUpdate::Set("updated-category".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let mut events = handle.subscribe_events();
    complete_after_post_processing(&mut pipeline, id);
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCompleted { job_id } if job_id == id)
    {
    }
    let history = database(&pipeline).get_job_history(id.0).unwrap().unwrap();
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
    complete_after_post_processing(&mut pipeline, id);
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
        database(&pipeline)
            .get_job_history(id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    assert!(handle.get_job(id).is_err());
    assert!(!has_installed(&pipeline, id));
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
        set_extracting(&mut pipeline, id);
        set_article_in_flight(&mut pipeline, id, true);
        report_job(&mut pipeline, id);
        if post_processing_paused {
            handle.pause_post_processing().await.unwrap();
            apply_intents(&mut pipeline).await;
            start_move_to_complete(&mut pipeline, id).await.unwrap();
        }
        handle.pause_job(id).await.unwrap();
        set_article_in_flight(&mut pipeline, id, false);
        if post_processing_paused {
            handle.resume_post_processing().await.unwrap();
        } else {
            start_move_to_complete(&mut pipeline, id).await.unwrap();
        }
        if superseded_pause {
            handle.resume_job(id).await.unwrap();
        }
        let watermark = handle.job_revision();
        apply_intents(&mut pipeline).await;
        // The download work drained before the boundary, so the pipeline
        // refuses the pause: the job stays live, the view shows no pause
        // intent, and the move deferred behind it is released.
        assert!(handle.job_revision() > watermark);
        assert!(move_deferred(&pipeline, id));
        assert!(!move_in_flight(&pipeline, id));
        assert_ne!(run_state(&pipeline, id), RunState::Paused);
        assert!(pause_intent(&pipeline, id).0 != Some(true));
        assert_ne!(handle.get_job(id).unwrap().status, JobStatus::Paused);
        assert!(completion_pending(&pipeline, id));
        let mut events = handle.subscribe_events();
        check_completion(&mut pipeline, id).await;
        // The released move is in flight, so the wait below has a move to hear.
        assert!(
            move_in_flight(&pipeline, id),
            "{:?}",
            installed_status(&pipeline, id)
        );
        let done = CompletedMove::receive(&mut pipeline).await;
        assert_eq!(done.job_id(), id);
        assert!(done.succeeded());
        done.deliver(&mut pipeline).await;
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
            database(&pipeline)
                .get_job_history(id.0)
                .unwrap()
                .unwrap()
                .status,
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
    set_extracting(&mut pipeline, id);
    assert!(download_queue_len(&pipeline, id) > 0);
    assert!(!has_current_download_activity(&pipeline, id));
    report_job(&mut pipeline, id);
    assert_eq!(
        handle.get_job(id).unwrap().download_state,
        DownloadState::Complete
    );
    assert!(download_work_pending(&pipeline, id));
    handle.pause_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(run_state(&pipeline, id), RunState::Paused);

    // Nothing was out, so the pause let the articles go at once: the resumed
    // job is built again from its stored NZB, and its queue emptied as the
    // fixture's was.
    assert!(!articles_active(&pipeline, id));
    handle.resume_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    activate_job(&mut pipeline, id).await;
    clear_download_queue(&mut pipeline, id);
    report_job(&mut pipeline, id);
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(id).await,
        Err(SchedulerError::Conflict(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
}
