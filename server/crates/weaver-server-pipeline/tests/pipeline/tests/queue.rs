// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_composition::JobSpec;
use weaver_server_composition::post_processing::effects::PostProcessingEffectsQueries;
use weaver_server_composition::{JobStatus, SchedulerError};
use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::jobs::duplicate_persistence::JobsDuplicatePersistenceAdmitDuplicateSubmissionQueries;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::pipeline::tests::{
    activate_job, retire_pipeline_database, sample_nzb_zstd, settle_failed_releases,
};
use weaver_server_pipeline::pipeline::tests::{
    finish_cancel_jobs, history_fixture::database, new_direct_pipeline, nzb_zstd_for,
    queue_fixture::*, segmented_job_spec,
};

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
    assert!(installed_spec(&pipeline, id).is_none());
    let watermark = handle.job_revision();
    handle.pause_job(id).await.unwrap();
    assert!(handle.job_revision() > watermark);
    // The queue already holds the job paused; the pipeline has not acted yet.
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Paused);
    assert!(installed_spec(&pipeline, id).is_none());
    apply_intents(&mut pipeline).await;
    // Paused before a byte of it came, the job waits as its row.
    let info = handle.get_job(id).unwrap();
    assert_eq!(info.status, JobStatus::Paused);
    assert!(installed_spec(&pipeline, id).is_none());
    let path = std::path::PathBuf::from(info.output_dir.unwrap());
    let watermark = handle.job_revision();
    handle.cancel_job(id).await.unwrap();
    assert!(handle.job_revision() > watermark);
    // Still listed: the job leaves the queue only once its history row commits.
    assert_eq!(handle.get_job(id).unwrap().status, JobStatus::Paused);
    assert!(path.exists());
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
    apply_intents(&mut pipeline).await;
    report_all(&pipeline);
    handle
        .update_job(
            a,
            weaver_server_composition::JobUpdate {
                category: weaver_server_pipeline::jobs::FieldUpdate::Set("series".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .update_job(
            a,
            weaver_server_composition::JobUpdate {
                password: weaver_server_pipeline::jobs::FieldUpdate::Set("fixture-secret".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    handle
        .reorder_job(
            b,
            weaver_server_pipeline::jobs::handle::QueueMoveTarget::Top,
        )
        .await
        .unwrap();
    report_all(&pipeline);
    let rows = handle.list_jobs();
    assert_eq!(rows[0].job_id, b);
    let job = handle.get_job(a).unwrap();
    assert_eq!(job.category.as_deref(), Some("series"));
    assert_eq!(job.password.as_deref(), Some("fixture-secret"));
    apply_intents(&mut pipeline).await;
    assert_eq!(
        installed_spec(&pipeline, a).unwrap().category.as_deref(),
        Some("series")
    );
    assert_eq!(
        installed_spec(&pipeline, a).unwrap().password.as_deref(),
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
async fn shutdown_settles_recorded_cancel_and_refuses_new_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let db = database(&pipeline).clone();
    let id = JobId(9011);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
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
async fn history_delete_waits_for_retirement_but_never_needs_a_pipeline_turn() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9006);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
    let mut events = handle.subscribe_events();
    handle.cancel_job_keeping_files(id).await.unwrap();
    assert!(matches!(
        handle.delete_history(id, false).await,
        Err(SchedulerError::Conflict(_))
    ));
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
        {
            break;
        }
    }
    assert!(handle.get_job(id).is_err());
    assert_eq!(
        database(&pipeline)
            .get_job_history(id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    // There is no actor task: history deletion owns its persistence and cleanup.
    handle.delete_history(id, false).await.unwrap();
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_none());
    report_all(&pipeline);
    assert!(handle.get_job(id).is_err());
}

#[tokio::test]
async fn pause_and_semantic_cancel_refuse_post_processing_without_an_actor_turn() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9010);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
    activate_job(&mut pipeline, id).await;
    clear_download_queue(&mut pipeline, id);
    for status in [JobStatus::Extracting, JobStatus::Repairing] {
        report_status(&mut pipeline, id, status);
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
async fn rename_waits_for_job_quiescence_and_preserves_written_payload() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9007);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
    let old = working_dir(&pipeline, id).clone();
    std::fs::write(old.join("payload.bin"), b"partial payload").unwrap();
    set_article_in_flight(&mut pipeline, id, true);
    let watermark = handle.job_revision();
    handle
        .update_job(
            id,
            weaver_server_composition::JobUpdate {
                name: Some("first rename".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(handle.job_revision() > watermark);
    apply_intents(&mut pipeline).await;
    assert_eq!(*working_dir(&pipeline, id), old);
    assert_eq!(handle.get_job(id).unwrap().name, "first rename");
    handle
        .update_job(
            id,
            weaver_server_composition::JobUpdate {
                name: Some("latest rename".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    set_article_in_flight(&mut pipeline, id, false);
    set_active_decode(&mut pipeline, id, true);
    apply_intents(&mut pipeline).await;
    assert_eq!(*working_dir(&pipeline, id), old);
    set_active_decode(&mut pipeline, id, false);
    apply_intents(&mut pipeline).await;
    let new = working_dir(&pipeline, id);
    assert_eq!(new.file_name().unwrap(), "latest rename");
    assert!(!old.exists());
    assert_eq!(
        std::fs::read(new.join("payload.bin")).unwrap(),
        b"partial payload"
    );
    assert!(weaver_server_pipeline::jobs::working_dir::is_weaver_owned_working_dir(new));
    assert!(!rename_pending(&pipeline, id));
    assert_eq!(
        database(&pipeline).load_active_jobs().unwrap()[&id].output_dir,
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
    apply_intents(&mut pipeline).await;
    let path = working_dir(&pipeline, id).clone();
    let mut events = handle.subscribe_events();
    handle.cancel_job(id).await.unwrap();
    assert!(handle.get_job(id).is_ok());
    assert!(path.exists());
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_some());
    handle.redownload_job(id).await.unwrap();
    assert!(!cleanup_pending(&pipeline, id));
    apply_intents(&mut pipeline).await;
    let new = working_dir(&pipeline, id);
    assert!(new.exists());
    assert!(weaver_server_pipeline::jobs::working_dir::is_weaver_owned_working_dir(new));
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_none());
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
    apply_intents(&mut pipeline).await;
    activate_job(&mut pipeline, id).await;
    flush_checkpoint(&mut pipeline, id);
    database(&pipeline).flush_write_queue().await.unwrap();
    let verdict = save_feed_refusal(&mut pipeline, id, "fixture feed refusal".into(), 0);
    assert_eq!(recorded_verdict(&pipeline, id), Some(verdict));

    if cancel {
        handle.cancel_job_keeping_files(id).await.unwrap();
        apply_intents(&mut pipeline).await;
        finish_cancel_jobs(&mut pipeline).await;
        finish_retiring_graph(&mut pipeline, id).await;
    } else {
        fail_job(&mut pipeline, id, "fixture feed refusal".into());
        settle_failed_releases(&mut pipeline).await;
    }
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_some());
    handle.redownload_job(id).await.unwrap();
    apply_intents(&mut pipeline).await;
    activate_job(&mut pipeline, id).await;
    flush_checkpoint(&mut pipeline, id);
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(!has_feed_verdict(&pipeline, id));
    assert!(!is_dead_post(&pipeline, id));
    assert!(recorded_verdict(&pipeline, id).is_none());
    assert_eq!(download_queue_len(&pipeline, id), 1);
    retire_pipeline_database(pipeline).await;
}

#[tokio::test]
async fn metadata_intents_preserve_later_script_parameters_and_merge_per_key() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9011);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
    for edits in [
        vec![("priority", Some("low")), ("removed", Some("old"))],
        vec![("PRIORITY", Some("high")), ("removed", None)],
    ] {
        handle
            .update_job(
                id,
                weaver_server_composition::JobUpdate {
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
    let effects = weaver_server_composition::post_processing::effects::JobScriptEffects {
        parameters: [("script-value".into(), "preserved".into())].into(),
        ..Default::default()
    };
    database(&pipeline)
        .save_job_script_effects(id.0, &effects)
        .unwrap();
    assert!(!apply_script_effects(&mut pipeline, id));
    report_job(&mut pipeline, id);
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
    apply_intents(&mut pipeline).await;
    database(&pipeline).flush_write_queue().await.unwrap();
    assert_metadata(installed_spec(&pipeline, id).unwrap().metadata.clone());
    assert_metadata(handle.get_job(id).unwrap().metadata);
    assert_metadata(
        database(&pipeline).load_active_jobs().unwrap()[&id]
            .metadata
            .clone(),
    );
    // Replaying persisted script effects cannot resurrect a removed parameter.
    assert!(!apply_script_effects(&mut pipeline, id));
    assert_metadata(installed_spec(&pipeline, id).unwrap().metadata.clone());
}

#[tokio::test]
async fn superseded_admission_never_installs_a_queue_row() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let spec = segmented_job_spec("stale", "payload.bin", &[100]);
    let request =
        |spec: &JobSpec, hash, score| weaver_server_composition::DuplicateAdmissionRequest {
            evidence: weaver_server_composition::FingerprintEvidence::from_validated_spec(
                spec, [hash; 32],
            ),
            mode: weaver_server_composition::DuplicateMode::Score,
            semantic: weaver_server_composition::SemanticDuplicate::from_source(
                "control-plane-generation",
                score,
            ),
            semantic_source: Some(weaver_server_composition::SemanticCandidateSource {
                nzb_zstd: sample_nzb_zstd(),
                filename: Some("stale.nzb".into()),
                password: None,
                category: None,
                metadata: vec![],
            }),
            origin: weaver_server_composition::SubmissionOrigin::Api,
            idempotency: None,
            policy: weaver_server_composition::DuplicatePolicy::default(),
        };
    let weaver_server_composition::DuplicateAdmission::Accepted {
        job_id: id,
        semantic: Some(semantic),
        ..
    } = database(&pipeline)
        .admit_duplicate_submission(&request(&spec, 1, 10))
        .unwrap()
    else {
        panic!("initial candidate must be admitted");
    };
    let mut replacement = spec.clone();
    replacement.files[0].segments[0].message_ids =
        weaver_server_pipeline::jobs::MessageIds::new(vec!["replacement@fixture.invalid".into()]);
    assert!(matches!(
        database(&pipeline)
            .admit_duplicate_submission(&request(&replacement, 2, 20))
            .unwrap(),
        weaver_server_composition::DuplicateAdmission::Accepted { .. }
    ));
    let result = handle
        .add_job_with_options(
            id,
            spec,
            PathBuf::from("stale.nzb"),
            sample_nzb_zstd(),
            weaver_server_pipeline::jobs::AddJobOptions {
                semantic_materialization_generation: Some(semantic.materialization_generation),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(SchedulerError::SemanticSuperseded)));
    assert!(handle.get_job(id).is_err());
    assert!(database(&pipeline).load_active_jobs().unwrap().is_empty());
    assert_eq!(
        database(&pipeline)
            .semantic_candidate_snapshot(id)
            .unwrap()
            .unwrap()
            .state,
        weaver_server_composition::SemanticCandidateState::Parked
    );
}

#[tokio::test]
async fn final_delete_archives_before_the_event_and_erases_history_after_cleanup() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    let id = JobId(9009);
    add(&handle, id).await;
    apply_intents(&mut pipeline).await;
    let path = working_dir(&pipeline, id).clone();
    let script = weaver_server_composition::post_processing::events::hold_test_event_run(
        database(&pipeline),
        id.0,
        "cancel-cleanup",
    );
    let mut events = handle.subscribe_events();
    let mut revisions = handle.subscribe_job_changes();
    handle.cancel_job_and_delete_history(id).await.unwrap();
    assert!(handle.get_job(id).is_ok());
    assert!(path.exists());
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_none());
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    while !matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == id)
    {
    }
    assert_eq!(
        database(&pipeline)
            .get_job_history(id.0)
            .unwrap()
            .unwrap()
            .status,
        "cancelled"
    );
    assert!(path.exists());
    drop(script);
    loop {
        revisions.mark_unchanged();
        if database(&pipeline).get_job_history(id.0).unwrap().is_none()
            && !cleanup_or_history_pending(&pipeline, id)
        {
            break;
        }
        revisions.changed().await.unwrap();
    }
    assert!(!path.exists());
    database(&pipeline).flush_write_queue().await.unwrap();
    report_all(&pipeline);
    assert!(handle.get_job(id).is_err());
    assert!(database(&pipeline).get_job_history(id.0).unwrap().is_none());
}
