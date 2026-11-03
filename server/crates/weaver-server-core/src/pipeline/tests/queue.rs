// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

async fn add(handle: &SchedulerHandle, id: JobId) {
    let spec = segmented_job_spec("queue fixture", "payload.bin", &[100]);
    let nzb_zstd = nzb_zstd_for(&spec);
    handle
        .add_job(id, spec, PathBuf::from("queue-fixture.nzb"), nzb_zstd)
        .await
        .unwrap();
}

#[tokio::test]
async fn admission_and_intents_answer_without_a_running_pipeline() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9001);
    let mut events = handle.subscribe_events();
    let watermark = handle.job_revision();
    add(&handle, id).await;
    assert!(handle.job_revision() > watermark);
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Queued);
    assert!(
        matches!(events.recv().await.unwrap(), PipelineEvent::JobCreated { job_id, .. } if job_id == id)
    );
    assert!(!pipeline.jobs.contains_key(&id));
    let watermark = handle.job_revision();
    handle.pause_job(id).await.unwrap();
    assert!(handle.job_revision() > watermark);
    // The queue already holds the job paused; the pipeline has not acted yet.
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Paused);
    assert!(!pipeline.jobs.contains_key(&id));
    pipeline.apply_queue_intents().await;
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Paused);
    assert_eq!(pipeline.jobs[&id].status, JobStatus::Paused);
    let path = pipeline.jobs[&id].working_dir.clone();
    let watermark = handle.job_revision();
    handle.cancel_job(id).await.unwrap();
    assert!(handle.job_revision() > watermark);
    // Still listed: the job leaves the queue only once its history row commits.
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Paused);
    assert!(path.exists());
    assert!(pipeline.jobs.contains_key(&id));
}

#[tokio::test]
async fn reports_preserve_unapplied_updates_and_queue_order() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let a = JobId(9002);
    let b = JobId(9003);
    add(&handle, a).await;
    add(&handle, b).await;
    pipeline.apply_queue_intents().await;
    pipeline.report_all_for_test();
    handle
        .update_job(
            a,
            crate::JobUpdate {
                category: crate::jobs::FieldUpdate::Set("series".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .update_job(
            a,
            crate::JobUpdate {
                password: crate::jobs::FieldUpdate::Set("fixture-secret".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .reorder_job(b, crate::jobs::handle::QueueMoveTarget::Top)
        .await
        .unwrap();
    pipeline.report_all_for_test();
    let rows = handle.list_jobs();
    assert_eq!(rows[0].job_id, b);
    let job = handle.get_job(a).unwrap();
    assert_eq!(job.category.as_deref(), Some("series"));
    assert_eq!(job.password.as_deref(), Some("fixture-secret"));
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs[&a].spec.category.as_deref(), Some("series"));
    assert_eq!(
        pipeline.jobs[&a].spec.password.as_deref(),
        Some("fixture-secret")
    );
}

#[tokio::test]
async fn duplicate_admission_and_unknown_mutations_refuse_synchronously() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9004);
    add(&handle, id).await;
    let error = handle
        .add_job(
            id,
            segmented_job_spec("duplicate", "payload.bin", &[100]),
            PathBuf::from("duplicate.nzb"),
            nzb_zstd_for(&(segmented_job_spec("duplicate", "payload.bin", &[100]))),
        )
        .await
        .unwrap_err();
    assert!(matches!(error, SchedulerError::JobExists(found) if found == id));
    let watermark = handle.job_revision();
    assert!(matches!(
        handle.pause_job(JobId(9005)).await,
        Err(SchedulerError::JobNotFound(_))
    ));
    assert_eq!(handle.job_revision(), watermark);
}

#[tokio::test]
async fn pause_and_semantic_cancel_refuse_post_processing_without_an_actor_turn() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9010);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    activate_job(&mut pipeline, id).await;
    pipeline
        .jobs
        .get_mut(&id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    for status in [JobStatus::Extracting, JobStatus::Repairing] {
        pipeline.jobs.get_mut(&id).unwrap().status = status;
        pipeline.report_job(id);
        let watermark = handle.job_revision();
        assert!(matches!(
            handle.pause_job(id).await,
            Err(SchedulerError::Conflict(_))
        ));
        assert!(matches!(
            handle.cancel_semantic_superseded(id).await,
            Err(SchedulerError::Conflict(_))
        ));
        assert_eq!(handle.job_revision(), watermark);
        assert!(handle.get_job(id).is_ok());
    }
}

#[tokio::test]
async fn history_delete_waits_for_retirement_but_never_needs_a_pipeline_turn() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9006);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    let mut events = handle.subscribe_events();
    handle.cancel_job_keeping_files(id).await.unwrap();
    assert!(matches!(
        handle.delete_history(id, false).await,
        Err(SchedulerError::Conflict(_))
    ));
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
        {
            break;
        }
    }
    assert!(handle.get_job(id).is_err());
    assert_eq!(
        pipeline.db.get_job_history(id.0).unwrap().unwrap().status,
        "cancelled"
    );
    // There is no actor task: history deletion owns its persistence and cleanup.
    handle.delete_history(id, false).await.unwrap();
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_none());
    pipeline.report_all_for_test();
    assert!(handle.get_job(id).is_err());
}

#[tokio::test]
async fn rename_waits_for_job_quiescence_and_preserves_written_payload() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9007);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    let old = pipeline.jobs[&id].working_dir.clone();
    std::fs::write(old.join("payload.bin"), b"partial payload").unwrap();
    pipeline.active_downloads_by_job.insert(id, 1);
    let watermark = handle.job_revision();
    handle
        .update_job(
            id,
            crate::JobUpdate {
                name: Some("first rename".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(handle.job_revision() > watermark);
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs[&id].working_dir, old);
    assert_eq!(handle.get_job(id).unwrap().name, "first rename");
    handle
        .update_job(
            id,
            crate::JobUpdate {
                name: Some("latest rename".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    pipeline.active_downloads_by_job.remove(&id);
    pipeline.active_decodes_by_job.insert(id, 1);
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs[&id].working_dir, old);
    pipeline.active_decodes_by_job.remove(&id);
    pipeline.apply_queue_intents().await;
    let new = &pipeline.jobs[&id].working_dir;
    assert_eq!(new.file_name().unwrap(), "latest rename");
    assert!(!old.exists());
    assert_eq!(
        std::fs::read(new.join("payload.bin")).unwrap(),
        b"partial payload"
    );
    assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(new));
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == id)
            .unwrap()
            .intents
            .rename_to
            .is_none()
    }));
    assert_eq!(
        pipeline.db.load_active_jobs().unwrap()[&id].output_dir,
        *new
    );
}

#[tokio::test]
async fn retry_waits_for_cancelled_directory_cleanup_before_readmission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9008);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    let path = pipeline.jobs[&id].working_dir.clone();
    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    assert!(handle.get_job(id).is_ok());
    assert!(path.exists());
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_some());
    handle.redownload_job(id).await.unwrap();
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| !inner.pending_cleanup.contains(&id))
    );
    pipeline.apply_queue_intents().await;
    let new = &pipeline.jobs[&id].working_dir;
    assert!(new.exists());
    assert!(crate::jobs::working_dir::is_weaver_owned_working_dir(new));
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_none());
}

#[tokio::test]
async fn an_explicit_redownload_clears_the_committed_feed_verdict() {
    redownload_clears_the_committed_feed_verdict(false).await;
}

#[tokio::test]
async fn a_cancelled_dead_post_redownload_clears_the_committed_feed_verdict() {
    redownload_clears_the_committed_feed_verdict(true).await;
}

async fn redownload_clears_the_committed_feed_verdict(cancel: bool) {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9012);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    activate_job(&mut pipeline, id).await;
    pipeline.db.flush_write_queue().await.unwrap();
    let mut verdict = crate::jobs::feed_verdict::FeedVerdict::new("fixture feed refusal".into());
    verdict.abandoned_files.insert(0);
    pipeline
        .db
        .save_graph_checkpoint(
            id,
            Arc::new(crate::jobs::graph_checkpoint::Delta {
                feed_verdict: Some(verdict.clone()),
                ..Default::default()
            }),
        )
        .unwrap();
    pipeline
        .post_probes
        .restore_verdict(id, Some(verdict.clone()));
    assert_eq!(
        pipeline
            .db
            .load_graph_checkpoint(id)
            .unwrap()
            .unwrap()
            .feed_verdict,
        Some(verdict)
    );

    if cancel {
        handle.cancel_job_keeping_files(id).await.unwrap();
        pipeline.apply_queue_intents().await;
        finish_cancel_jobs(&mut pipeline).await;
        while pipeline.retiring_graph_jobs.contains_key(&id) {
            hear_graph_mail(&mut pipeline, id).await;
        }
    } else {
        pipeline.fail_job(id, "fixture feed refusal".into());
        settle_failed_releases(&mut pipeline).await;
    }
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_some());
    handle.redownload_job(id).await.unwrap();
    pipeline.apply_queue_intents().await;
    activate_job(&mut pipeline, id).await;
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(!pipeline.post_probes.verdicts.contains_key(&id));
    assert!(!pipeline.post_probes.dead.contains_key(&id));
    assert!(
        pipeline
            .db
            .load_graph_checkpoint(id)
            .unwrap()
            .unwrap()
            .feed_verdict
            .is_none()
    );
    assert_eq!(
        pipeline.jobs[&id]
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        1
    );
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn metadata_intents_preserve_later_script_parameters_and_merge_per_key() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9011);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    for edits in [
        vec![("priority", Some("low")), ("removed", Some("old"))],
        vec![("PRIORITY", Some("high")), ("removed", None)],
    ] {
        handle
            .update_job(
                id,
                crate::JobUpdate {
                    metadata_edits: edits
                        .into_iter()
                        .map(|(key, value)| (key.into(), value.map(String::from)))
                        .collect(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let effects = crate::post_processing::effects::JobScriptEffects {
        parameters: [("script-value".into(), "preserved".into())].into(),
        ..Default::default()
    };
    pipeline.db.save_job_script_effects(id.0, &effects).unwrap();
    assert!(!pipeline.apply_queue_script_effects(id));
    pipeline.report_job(id);
    let assert_metadata = |mut metadata: Vec<(String, String)>| {
        metadata.sort();
        assert_eq!(
            metadata,
            vec![
                ("PRIORITY".into(), "high".into()),
                ("script-value".into(), "preserved".into()),
            ]
        );
    };
    assert_metadata(handle.get_job(id).unwrap().metadata);
    pipeline.apply_queue_intents().await;
    pipeline.db.flush_write_queue().await.unwrap();
    assert_metadata(pipeline.jobs[&id].spec.metadata.clone());
    assert_metadata(handle.get_job(id).unwrap().metadata);
    assert_metadata(
        pipeline.db.load_active_jobs().unwrap()[&id]
            .metadata
            .clone(),
    );
    // Replaying persisted script effects cannot resurrect a removed parameter.
    assert!(!pipeline.apply_queue_script_effects(id));
    assert_metadata(pipeline.jobs[&id].spec.metadata.clone());
}

#[tokio::test]
async fn shutdown_settles_recorded_cancel_and_refuses_new_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let db = pipeline.db.clone();
    let id = JobId(9011);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    handle.shutdown().await.unwrap();
    assert!(matches!(
        handle
            .add_job(
                JobId(9012),
                segmented_job_spec("late", "payload.bin", &[100]),
                PathBuf::from("late.nzb"),
                nzb_zstd_for(&(segmented_job_spec("late", "payload.bin", &[100])))
            )
            .await,
        Err(SchedulerError::ChannelClosed)
    ));
    let runner = tokio::spawn(async move { pipeline.run().await });
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    runner.await.unwrap();
    assert!(handle.is_closed());
    assert_eq!(
        db.get_job_history(id.0).unwrap().unwrap().status,
        "cancelled"
    );
}

#[tokio::test]
async fn superseded_admission_never_installs_a_queue_row() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let spec = segmented_job_spec("stale", "payload.bin", &[100]);
    let request = |spec: &JobSpec, hash, score| crate::DuplicateAdmissionRequest {
        evidence: crate::FingerprintEvidence::from_validated_spec(spec, [hash; 32]),
        mode: crate::DuplicateMode::Score,
        semantic: crate::SemanticDuplicate::from_source("control-plane-generation", score),
        semantic_source: Some(crate::SemanticCandidateSource {
            nzb_zstd: sample_nzb_zstd(),
            filename: Some("stale.nzb".into()),
            password: None,
            category: None,
            metadata: vec![],
        }),
        origin: crate::SubmissionOrigin::Api,
        idempotency: None,
        policy: crate::DuplicatePolicy::default(),
    };
    let crate::DuplicateAdmission::Accepted {
        job_id: id,
        semantic: Some(semantic),
        ..
    } = pipeline
        .db
        .admit_duplicate_submission(&request(&spec, 1, 10))
        .unwrap()
    else {
        panic!("initial candidate must be admitted");
    };
    let mut replacement = spec.clone();
    replacement.files[0].segments[0].message_ids =
        crate::jobs::MessageIds::new(vec!["replacement@fixture.invalid".into()]);
    assert!(matches!(
        pipeline
            .db
            .admit_duplicate_submission(&request(&replacement, 2, 20))
            .unwrap(),
        crate::DuplicateAdmission::Accepted { .. }
    ));
    let result = handle
        .add_job_with_options(
            id,
            spec,
            PathBuf::from("stale.nzb"),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions {
                semantic_materialization_generation: Some(semantic.materialization_generation),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(SchedulerError::SemanticSuperseded)));
    assert!(handle.get_job(id).is_err());
    assert!(pipeline.db.load_active_jobs().unwrap().is_empty());
    assert_eq!(
        pipeline
            .db
            .semantic_candidate_snapshot(id)
            .unwrap()
            .unwrap()
            .state,
        crate::SemanticCandidateState::Parked
    );
}

#[tokio::test]
async fn final_delete_archives_before_the_event_and_erases_history_after_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9009);
    add(&handle, id).await;
    pipeline.apply_queue_intents().await;
    let path = pipeline.jobs[&id].working_dir.clone();
    let script =
        crate::post_processing::events::hold_test_event_run(&pipeline.db, id.0, "cancel-cleanup");
    let mut events = handle.subscribe_events();
    let mut revisions = handle.subscribe_job_changes();
    handle.cancel_job_and_delete_history(id).await.unwrap();
    assert!(handle.get_job(id).is_ok());
    assert!(path.exists());
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_none());
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert_eq!(
        pipeline.db.get_job_history(id.0).unwrap().unwrap().status,
        "cancelled"
    );
    assert!(path.exists());
    drop(script);
    loop {
        revisions.mark_unchanged();
        if pipeline.db.get_job_history(id.0).unwrap().is_none()
            && pipeline.shared_state.queue.read(|inner| {
                !inner.pending_cleanup.contains(&id) && !inner.history_operations.contains(&id)
            })
        {
            break;
        }
        revisions.changed().await.unwrap();
    }
    assert!(!path.exists());
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline.report_all_for_test();
    assert!(handle.get_job(id).is_err());
    assert!(pipeline.db.get_job_history(id.0).unwrap().is_none());
}
