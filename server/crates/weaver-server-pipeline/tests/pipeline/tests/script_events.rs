// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_server_composition::post_processing::{
    effects::{JobScriptEffects, PostProcessingEffectsQueries},
    events::PostProcessingEventsCancelEventScriptsQueries,
    instances::{InstanceTrigger, PostProcessingInstancesQueries, ScriptInstanceDraft},
    model::{PostProcessingSettings, QueueEvent, ScriptEventLabel, ScriptName, ScriptStatus},
    output::PostProcessingOutputQueries,
    settings::PostProcessingSettingsQueries,
};
use weaver_server_composition::{JobId, JobStatus, StateError};
use weaver_server_pipeline::pipeline::tests::{
    history_fixture::{check_completion, database, finished_jobs},
    insert_active_job, new_direct_pipeline,
    nzb_admission_fixture::holds_arrival,
    queue_fixture::{fail_job, fail_on_health, installed_status, working_dir},
    script_fixture::*,
    set_job_status_for_test, settle_failed_releases, standalone_job_spec,
};

#[tokio::test]
async fn non_queue_script_does_not_publish_queue_wait_status() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = database(&pipeline)
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    std::fs::write(
        scripts.join("post.sh"),
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 93\n",
    )
    .unwrap();
    database(&pipeline)
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    database(&pipeline)
        .create_script_instance(ScriptInstanceDraft::new(
            ScriptName::new("post.sh").unwrap(),
            InstanceTrigger::PostProcessing,
        ))
        .unwrap();
    let job_id = JobId(168);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("no-queue", &[])).await;

    check_completion(&mut pipeline, job_id).await;
    assert_eq!(
        installed_status(&pipeline, job_id),
        Some(&JobStatus::Downloading)
    );
    let Some((id, result)) = receive_queue_done(&mut pipeline).await else {
        panic!("no queue script should be admitted");
    };
    assert_eq!(id, job_id);
    result.unwrap();
    assert_eq!(database(&pipeline).queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn file_events_stop_after_post_processing_begins() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(165);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("file-events", &[]),
    )
    .await;
    raise_queue_event(&pipeline, job_id, QueueEvent::FileDownloaded, None);
    assert_eq!(database(&pipeline).queue_script_count().unwrap(), 0);
    enable_queue_script(database(&pipeline), temp.path());
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::PostProcessing);
    raise_queue_event(&pipeline, job_id, QueueEvent::FileDownloaded, None);
    assert_eq!(database(&pipeline).queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn cancellation_cleanup_waits_for_the_jobs_deletion_event() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    let job_id = JobId(166);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("cancel-script", &[]),
    )
    .await;
    let context = queue_context(&pipeline, job_id, QueueEvent::NzbDeleted).unwrap();
    let run_id = database(&pipeline)
        .enqueue_script_event(&context, 1)
        .unwrap()
        .unwrap();
    claim_started_run(database(&pipeline), &run_id, "test_claim_deleted_script");
    let working_dir = working_dir(&pipeline, job_id).clone();
    let staging_dir = temp.path().join("cancel-staging");
    std::fs::create_dir_all(&working_dir).unwrap();
    std::fs::create_dir(&staging_dir).unwrap();
    let cleanup = cleanup_cancelled_dirs(
        database(&pipeline),
        job_id,
        &working_dir,
        Some(&staging_dir),
    );
    tokio::pin!(cleanup);
    tokio::select! {
        biased;
        result = &mut cleanup => panic!("cleanup crossed an unfinished deletion event: {result:?}"),
        () = std::future::ready(()) => {},
    }
    assert!(working_dir.is_dir());
    assert!(staging_dir.is_dir());
    finish_event(database(&pipeline), &run_id);
    cleanup.await.unwrap();
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
}

#[tokio::test]
async fn queue_scripts_that_cannot_run_do_not_fail_the_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    let job_id = JobId(169);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("script-infrastructure", &[]),
    )
    .await;
    check_completion(&mut pipeline, job_id).await;
    assert!(queue_script_waiting(&pipeline, job_id));
    queue_scripts_done(
        &mut pipeline,
        job_id,
        Err(StateError::Database("settings unavailable".into())),
    )
    .await;
    settle_terminal_scripts(&mut pipeline).await;
    assert!(!queue_script_waiting(&pipeline, job_id));
    assert!(queue_script_completed(&pipeline, job_id));
    assert!(
        !matches!(
            installed_status(&pipeline, job_id).unwrap(),
            JobStatus::Failed { .. }
        ),
        "{:?}",
        installed_status(&pipeline, job_id)
    );
}

#[tokio::test]
async fn a_late_queue_event_leaves_a_job_that_moved_on_alone() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(170);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("moved-on", &[])).await;
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Verifying);
    mark_queue_script_waiting(&mut pipeline, job_id);
    queue_scripts_admitted(&mut pipeline, job_id);
    assert_eq!(
        installed_status(&pipeline, job_id),
        Some(&JobStatus::Verifying)
    );
    queue_scripts_done(&mut pipeline, job_id, Ok(())).await;
    assert_eq!(
        installed_status(&pipeline, job_id),
        Some(&JobStatus::Verifying)
    );
    assert!(!queue_script_completed(&pipeline, job_id));
}

// An instance that blocks on a job's arrival, on NZB_ADDED or on JOB_ADDED,
// keeps the job from downloading until its run has ended.
#[tokio::test]
async fn a_new_job_waits_for_the_instances_that_block_on_its_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    // With its script gone the instance has nothing to start, so its run ends
    // as soon as its turn comes, with a result that says so.
    std::fs::remove_file(
        database(&pipeline)
            .post_processing_script_directory()
            .unwrap()
            .join("queue.sh"),
    )
    .unwrap();
    let job_id = JobId(171);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("held", &[])).await;
    database(&pipeline).flush_write_queue().await.unwrap();

    raise_arrival(&mut pipeline, job_id);
    assert!(holds_arrival(&pipeline, job_id));

    let Some((id, result)) = receive_arrival_done(&mut pipeline).await else {
        panic!("expected the end of the arrival scripts");
    };
    assert_eq!(id, job_id);
    result.as_ref().unwrap();
    // The hold is the pipeline's to release, in step with its other state.
    assert!(holds_arrival(&pipeline, job_id));
    arrival_done(&mut pipeline, id, result);
    assert!(!holds_arrival(&pipeline, job_id));
    assert!(!matches!(
        installed_status(&pipeline, job_id).unwrap(),
        JobStatus::Failed { .. }
    ));

    let mut results = database(&pipeline).event_script_results(job_id.0).unwrap();
    results.sort_by_key(|result| result.event.to_string());
    assert_eq!(
        results
            .iter()
            .map(|result| result.event.clone())
            .collect::<Vec<_>>(),
        [
            ScriptEventLabel::Queue(QueueEvent::JobAdded),
            ScriptEventLabel::Queue(QueueEvent::NzbAdded),
        ],
        "{results:?}"
    );
    for result in &results {
        assert_eq!(result.status, ScriptStatus::Warning);
        assert_eq!(result.instance_name.as_deref(), Some("queue.sh"));
    }

    // A second end for the same job changes nothing.
    arrival_done(&mut pipeline, job_id, Ok(()));
    assert!(!holds_arrival(&pipeline, job_id));
}

// What a blocking arrival script asked for is in force before the job is let
// go, so a job it marked bad never starts downloading.
#[tokio::test]
async fn a_job_marked_bad_on_arrival_fails_when_its_hold_ends() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(173);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("bad", &[])).await;
    mark_arrival_held(&mut pipeline, job_id);
    database(&pipeline)
        .save_job_script_effects(
            job_id.0,
            &JobScriptEffects {
                marked_bad: true,
                ..Default::default()
            },
        )
        .unwrap();
    arrival_done(&mut pipeline, job_id, Ok(()));
    assert!(!holds_arrival(&pipeline, job_id));
    let status = finished_jobs(&mut pipeline)
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .map(|job| job.status.clone())
        .or_else(|| installed_status(&pipeline, job_id).cloned());
    assert!(
        matches!(&status, Some(JobStatus::Failed { error }) if error.contains("FAILURE/BAD")),
        "{status:?}"
    );
}

// Scripts that could not be run at all decide nothing, so the job goes on.
#[tokio::test]
async fn arrival_scripts_that_cannot_run_let_the_job_go() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(174);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("let-go", &[])).await;
    mark_arrival_held(&mut pipeline, job_id);
    arrival_done(
        &mut pipeline,
        job_id,
        Err(StateError::Database("settings unavailable".into())),
    );
    assert!(!holds_arrival(&pipeline, job_id));
    assert!(!matches!(
        installed_status(&pipeline, job_id).unwrap(),
        JobStatus::Failed { .. }
    ));
}

// Only an instance that blocks, on this event, for this job's category, holds
// the job.
#[tokio::test]
async fn a_new_job_starts_at_once_when_nothing_blocks_on_its_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = database(&pipeline)
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    assert!(!scripts.join("queue.sh").exists());
    database(&pipeline)
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    for instance in [
        queue_instance(QueueEvent::NzbAdded).fire_and_forget(),
        queue_instance(QueueEvent::NzbAdded).category("tv"),
        queue_instance(QueueEvent::NzbAdded).disabled(),
        queue_instance(QueueEvent::NzbDeleted),
    ] {
        database(&pipeline)
            .create_script_instance(instance)
            .unwrap();
    }
    let job_id = JobId(172);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("not-held", &[])).await;

    raise_arrival(&mut pipeline, job_id);
    assert!(!holds_arrival(&pipeline, job_id));
}

#[tokio::test]
async fn file_only_scripts_hold_the_pre_move_barrier_without_downloaded_subscriber() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = database(&pipeline)
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    database(&pipeline)
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    database(&pipeline)
        .create_script_instance(queue_instance(QueueEvent::FileDownloaded))
        .unwrap();
    let job_id = JobId(9164);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("file-only-barrier", &[]),
    )
    .await;
    let context = queue_context(&pipeline, job_id, QueueEvent::FileDownloaded).unwrap();
    let run_id = database(&pipeline)
        .enqueue_script_event(&context, 1)
        .unwrap()
        .unwrap();
    // A claimed run cannot be superseded or executed by the worker. The test
    // releases it explicitly, independent of thread scheduling or script speed.
    claim_started_run(database(&pipeline), &run_id, "test_claim_file_script");
    let downloaded = queue_context(&pipeline, job_id, QueueEvent::NzbDownloaded).unwrap();
    let admission = admit_queue_event(database(&pipeline), downloaded, true).await;
    assert!(admission.is_none(), "no NZB_DOWNLOADED subscriber");
    let barrier = wait_for_download_scripts(database(&pipeline), job_id, admission.as_deref());
    tokio::pin!(barrier);
    tokio::select! {
        biased;
        result = &mut barrier => panic!("move barrier crossed an unfinished file event: {result:?}"),
        () = std::future::ready(()) => {},
    }
    finish_event(database(&pipeline), &run_id);
    barrier.await.unwrap();
}

// A job its post failed is deleted for health as NZBGet deletes one; any
// other failure, such as an extraction's, raises no deletion.
#[tokio::test]
async fn a_health_failure_raises_the_jobs_deletion_for_health() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    let health = JobId(173);
    let other = JobId(174);
    insert_active_job(&mut pipeline, health, standalone_job_spec("dead-post", &[])).await;
    insert_active_job(&mut pipeline, other, standalone_job_spec("withheld", &[])).await;
    let after_health = queue_context(&pipeline, health, QueueEvent::NzbMarked).unwrap();
    let after_other = queue_context(&pipeline, other, QueueEvent::NzbMarked).unwrap();

    fail_on_health(&mut pipeline, health, "first articles are missing".into());
    fail_job(
        &mut pipeline,
        other,
        "lantern.rar withheld for want of its password".into(),
    );
    settle_failed_releases(&mut pipeline).await;

    assert_eq!(
        deletions_queued(database(&pipeline), health, after_health).await,
        vec!["HEALTH".to_string()]
    );
    assert!(
        deletions_queued(database(&pipeline), other, after_other)
            .await
            .is_empty()
    );
}

// An NZB job raises NZB_ADDED, NZB_DOWNLOADED and NZB_DELETED, each with its
// JOB_ twin right after it and no TORRENT_ event. JOB_DOWNLOADED, queued
// after NZB_DOWNLOADED, leaves it queued.
#[tokio::test]
async fn an_nzb_job_raises_its_nzb_events_and_their_job_twins() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(database(&pipeline), temp.path());
    let (added, downloaded, deleted) = (JobId(9201), JobId(9202), JobId(9203));
    for (job_id, name) in [
        (added, "added"),
        (downloaded, "downloaded"),
        (deleted, "deleted"),
    ] {
        insert_active_job(&mut pipeline, job_id, standalone_job_spec(name, &[])).await;
    }

    raise_arrival(&mut pipeline, added);
    assert!(completion_gate(&mut pipeline, downloaded, true));
    raise_queue_event(&pipeline, deleted, QueueEvent::NzbDeleted, Some("MANUAL"));
    admitted_so_far(database(&pipeline)).await;

    assert_eq!(
        job_events(database(&pipeline), added),
        ["NZB_ADDED", "JOB_ADDED"]
    );
    assert_eq!(
        job_events(database(&pipeline), downloaded),
        ["NZB_DOWNLOADED", "JOB_DOWNLOADED"]
    );
    assert_eq!(
        job_events(database(&pipeline), deleted),
        ["NZB_DELETED", "JOB_DELETED"]
    );
    let generic = queued_script_events(database(&pipeline), QueueEvent::JobDeleted);
    let generic = generic
        .iter()
        .find(|context| context.job_id == Some(deleted.0))
        .unwrap();
    assert_eq!(generic.env["NZBNA_EVENT"], "JOB_DELETED");
    assert_eq!(generic.env["NZBNA_DELETESTATUS"], "MANUAL");
    assert_eq!(generic.env["NZBNA_NZBNAME"], "deleted");
}
