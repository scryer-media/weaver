// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::post_processing::effects::JobScriptEffects;
use crate::post_processing::instances::{InstanceTrigger, ScriptInstanceDraft};
use crate::post_processing::model::{PostProcessingSettings, QueueEvent, ScriptName, ScriptStatus};

fn queue_instance(event: QueueEvent) -> ScriptInstanceDraft {
    ScriptInstanceDraft::new(
        ScriptName::new("queue.sh").unwrap(),
        InstanceTrigger::Queue(event),
    )
}

pub(in crate::pipeline::tests) fn enable_queue_script(db: &Database, root: &Path) {
    let scripts = db
        .initialize_post_processing_script_directory(root, None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    db.save_post_processing_settings(&PostProcessingSettings {
        execution_enabled: true,
        ..Default::default()
    })
    .unwrap();
    // One instance for each thing that can happen to a download.
    for event in QueueEvent::ALL {
        db.create_script_instance(queue_instance(event)).unwrap();
    }
}

#[tokio::test]
async fn non_queue_script_does_not_publish_queue_wait_status() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = pipeline
        .db
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    std::fs::write(
        scripts.join("post.sh"),
        "#!/bin/sh\n### NZBGET POST-PROCESSING SCRIPT ###\nexit 93\n",
    )
    .unwrap();
    pipeline
        .db
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    pipeline
        .db
        .create_script_instance(ScriptInstanceDraft::new(
            ScriptName::new("post.sh").unwrap(),
            InstanceTrigger::PostProcessing,
        ))
        .unwrap();
    let job_id = JobId(168);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("no-queue", &[])).await;

    pipeline.check_job_completion(job_id).await;
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Downloading);
    let TerminalPostProcessingEvent::QueueDone(id, result) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("no queue script should be admitted");
    };
    assert_eq!(id, job_id);
    result.unwrap();
    assert_eq!(pipeline.db.queue_script_count().unwrap(), 0);
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
    pipeline.raise_queue_script_event(job_id, QueueEvent::FileDownloaded, None);
    assert_eq!(pipeline.db.queue_script_count().unwrap(), 0);
    enable_queue_script(&pipeline.db, temp.path());
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::PostProcessing);
    pipeline.raise_queue_script_event(job_id, QueueEvent::FileDownloaded, None);
    assert_eq!(pipeline.db.queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn cancellation_cleanup_waits_for_the_jobs_deletion_event() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    let job_id = JobId(166);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("cancel-script", &[]),
    )
    .await;
    let context = pipeline
        .queue_script_context(job_id, QueueEvent::NzbDeleted)
        .unwrap();
    let run_id = pipeline
        .db
        .enqueue_script_event(&context, 1)
        .unwrap()
        .unwrap();
    let datastore = pipeline.db.datastore();
    let claimed_run = run_id.clone();
    pipeline
        .db
        .run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "test_claim_deleted_script", |tx| {
                let claimed_run = claimed_run.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_event_queue SET state = 'started' WHERE run_id = {}",
                        &[SqlArg::Text(claimed_run)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
        .unwrap();
    let working_dir = pipeline.jobs[&job_id].working_dir.clone();
    let staging_dir = temp.path().join("cancel-staging");
    std::fs::create_dir_all(&working_dir).unwrap();
    std::fs::create_dir(&staging_dir).unwrap();
    let cleanup = Pipeline::cleanup_cancelled_job_directories(
        &pipeline.db,
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
    pipeline.db.finish_script_event_for_test(&run_id).unwrap();
    cleanup.await.unwrap();
    assert!(!working_dir.exists());
    assert!(!staging_dir.exists());
}

#[tokio::test]
async fn queue_scripts_that_cannot_run_do_not_fail_the_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    let job_id = JobId(169);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("script-infrastructure", &[]),
    )
    .await;
    pipeline.check_job_completion(job_id).await;
    assert!(pipeline.queue_script_waiters.contains(&job_id));
    pipeline
        .handle_queue_scripts_done(
            job_id,
            Err(crate::StateError::Database("settings unavailable".into())),
        )
        .await;
    assert!(!pipeline.queue_script_waiters.contains(&job_id));
    assert!(pipeline.queue_scripts_completed.contains(&job_id));
    assert!(
        !matches!(pipeline.jobs[&job_id].status, JobStatus::Failed { .. }),
        "{:?}",
        pipeline.jobs[&job_id].status
    );
}

#[tokio::test]
async fn a_late_queue_event_leaves_a_job_that_moved_on_alone() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(170);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("moved-on", &[])).await;
    set_job_status_for_test(&mut pipeline, job_id, JobStatus::Verifying);
    pipeline.queue_script_waiters.insert(job_id);
    pipeline.handle_queue_scripts_admitted(job_id);
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Verifying);
    pipeline.handle_queue_scripts_done(job_id, Ok(())).await;
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Verifying);
    assert!(!pipeline.queue_scripts_completed.contains(&job_id));
}

// An instance that blocks on a job's arrival keeps the job from downloading
// until its run has ended.
#[tokio::test]
async fn a_new_job_waits_for_the_instances_that_block_on_its_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    // With its script gone the instance has nothing to start, so its run ends
    // as soon as its turn comes, with a result that says so.
    std::fs::remove_file(
        pipeline
            .db
            .post_processing_script_directory()
            .unwrap()
            .join("queue.sh"),
    )
    .unwrap();
    let job_id = JobId(171);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("held", &[])).await;
    pipeline.db.flush_write_queue().await.unwrap();

    pipeline.raise_added_script_event(job_id);
    assert!(pipeline.held_for_added_scripts(job_id));

    let TerminalPostProcessingEvent::AddedScriptsDone(id, result) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected the end of the arrival scripts");
    };
    assert_eq!(id, job_id);
    result.as_ref().unwrap();
    // The hold is the pipeline's to release, in step with its other state.
    assert!(pipeline.held_for_added_scripts(job_id));
    pipeline.handle_added_scripts_done(id, result);
    assert!(!pipeline.held_for_added_scripts(job_id));
    assert!(!matches!(
        pipeline.jobs[&job_id].status,
        JobStatus::Failed { .. }
    ));

    let results = pipeline.db.event_script_results(job_id.0).unwrap();
    assert_eq!(results.len(), 1, "{results:?}");
    assert_eq!(results[0].status, ScriptStatus::Warning);
    assert_eq!(results[0].instance_name.as_deref(), Some("queue.sh"));

    // A second end for the same job changes nothing.
    pipeline.handle_added_scripts_done(job_id, Ok(()));
    assert!(!pipeline.held_for_added_scripts(job_id));
}

// What a blocking arrival script asked for is in force before the job is let
// go, so a job it marked bad never starts downloading.
#[tokio::test]
async fn a_job_marked_bad_on_arrival_fails_when_its_hold_ends() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(173);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("bad", &[])).await;
    pipeline.added_script_holds.insert(job_id);
    pipeline
        .db
        .save_job_script_effects(
            job_id.0,
            &JobScriptEffects {
                marked_bad: true,
                ..Default::default()
            },
        )
        .unwrap();
    pipeline.handle_added_scripts_done(job_id, Ok(()));
    assert!(!pipeline.held_for_added_scripts(job_id));
    let status = pipeline
        .finished_jobs()
        .await
        .into_iter()
        .find(|job| job.job_id == job_id)
        .map(|job| job.status.clone())
        .or_else(|| pipeline.jobs.get(&job_id).map(|state| state.status.clone()));
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
    pipeline.added_script_holds.insert(job_id);
    pipeline.handle_added_scripts_done(
        job_id,
        Err(crate::StateError::Database("settings unavailable".into())),
    );
    assert!(!pipeline.held_for_added_scripts(job_id));
    assert!(!matches!(
        pipeline.jobs[&job_id].status,
        JobStatus::Failed { .. }
    ));
}

// Only an instance that blocks, on this event, for this job's category, holds
// the job.
#[tokio::test]
async fn a_new_job_starts_at_once_when_nothing_blocks_on_its_arrival() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = pipeline
        .db
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    assert!(!scripts.join("queue.sh").exists());
    pipeline
        .db
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
        pipeline.db.create_script_instance(instance).unwrap();
    }
    let job_id = JobId(172);
    insert_active_job(&mut pipeline, job_id, standalone_job_spec("not-held", &[])).await;

    pipeline.raise_added_script_event(job_id);
    assert!(!pipeline.held_for_added_scripts(job_id));
}

#[tokio::test]
async fn file_only_scripts_hold_the_pre_move_barrier_without_downloaded_subscriber() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let scripts = pipeline
        .db
        .initialize_post_processing_script_directory(temp.path(), None)
        .unwrap();
    std::fs::write(
        scripts.join("queue.sh"),
        "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\nexit 0\n",
    )
    .unwrap();
    pipeline
        .db
        .save_post_processing_settings(&PostProcessingSettings {
            execution_enabled: true,
            ..Default::default()
        })
        .unwrap();
    pipeline
        .db
        .create_script_instance(queue_instance(QueueEvent::FileDownloaded))
        .unwrap();
    let job_id = JobId(9164);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("file-only-barrier", &[]),
    )
    .await;
    let context = pipeline
        .queue_script_context(job_id, QueueEvent::FileDownloaded)
        .unwrap();
    let run_id = pipeline
        .db
        .enqueue_script_event(&context, 1)
        .unwrap()
        .unwrap();
    // A claimed run cannot be superseded or executed by the worker. The test
    // releases it explicitly, independent of thread scheduling or script speed.
    let datastore = pipeline.db.datastore();
    let claimed_run = run_id.clone();
    pipeline
        .db
        .run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "test_claim_file_script", |tx| {
                let claimed_run = claimed_run.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_event_queue SET state = 'started' WHERE run_id = {}",
                        &[SqlArg::Text(claimed_run)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
        .unwrap();
    let downloaded = pipeline
        .queue_script_context(job_id, QueueEvent::NzbDownloaded)
        .unwrap();
    let admission = pipeline
        .db
        .admit_queue_script_event(downloaded, true)
        .await
        .unwrap()
        .unwrap();
    assert!(admission.is_none(), "no NZB_DOWNLOADED subscriber");
    let barrier =
        Pipeline::wait_for_download_queue_scripts(&pipeline.db, job_id, admission.as_deref());
    tokio::pin!(barrier);
    tokio::select! {
        biased;
        result = &mut barrier => panic!("move barrier crossed an unfinished file event: {result:?}"),
        () = std::future::ready(()) => {},
    }
    pipeline.db.finish_script_event_for_test(&run_id).unwrap();
    barrier.await.unwrap();
}
