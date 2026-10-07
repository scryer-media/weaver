use super::*;
use crate::post_processing::effects::JobScriptEffects;
use crate::post_processing::model::{
    PostProcessingSettings, QueueEvent, ScriptList, ScriptListEntry, ScriptLists, ScriptName,
};

fn enable_queue_script(db: &Database, root: &Path) {
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
    db.save_post_processing_script_lists(&ScriptLists {
        global: ScriptList::new(vec![ScriptListEntry::new(
            ScriptName::new("queue.sh").unwrap(),
        )])
        .unwrap(),
        ..Default::default()
    })
    .unwrap();
}

#[tokio::test]
async fn downloaded_barrier_persists_and_mark_bad_prevents_native_finalization() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    let job_id = JobId(164);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("script-barrier", &[]),
    )
    .await;
    let config = pipeline.config.clone();
    let _config_write = config.write().await;
    assert_eq!(
        pipeline
            .queue_script_context(job_id, QueueEvent::NzbDownloaded)
            .unwrap()
            .facts
            .data_dir
            .as_ref(),
        Some(&pipeline.script_data_dir)
    );
    drop(_config_write);
    // A restored job has no ADDED event. Model a downloaded run that was already
    // claimed so the worker cannot finish it before the barrier is inspected.
    assert_eq!(pipeline.db.queue_script_count().unwrap(), 0);
    let context = pipeline
        .queue_script_context(job_id, QueueEvent::NzbDownloaded)
        .unwrap();
    let run_id = pipeline
        .db
        .enqueue_script_event(&context, 1)
        .unwrap()
        .unwrap();
    let datastore = pipeline.db.datastore();
    pipeline
        .db
        .run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&datastore, "test_claim_script", |tx| {
                let run_id = run_id.clone();
                Box::pin(async move {
                    tx.execute(
                        "UPDATE script_event_queue SET state = 'started' WHERE run_id = {}",
                        &[SqlArg::Text(run_id)],
                    )
                    .await?;
                    Ok(())
                })
            })
            .await
        })
        .unwrap();
    pipeline.check_job_completion(job_id).await;
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Downloading);
    // Holding the barrier, before admission: the download is complete, so the
    // job neither pauses nor accepts a semantic cancel, and reports waiting.
    assert!(matches!(
        pipeline.pause_job_runtime(job_id),
        Err(crate::SchedulerError::Conflict(_))
    ));
    let (reply, cancelled) = tokio::sync::oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJob {
            job_id,
            origin: crate::jobs::handle::CancellationOrigin::SemanticSuperseded,
            reply,
        })
        .await;
    assert!(matches!(
        cancelled.await.unwrap(),
        Err(crate::SchedulerError::Conflict(_))
    ));
    let listed = pipeline
        .list_jobs()
        .into_iter()
        .find(|job| job.job_id == job_id)
        .unwrap();
    assert_eq!(listed.status, JobStatus::AwaitingQueueScripts);
    assert_eq!(listed.run_state, crate::jobs::model::RunState::Active);
    let TerminalPostProcessingEvent::QueueAdmitted(id) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue admission");
    };
    assert_eq!(id, job_id);
    pipeline.handle_queue_scripts_admitted(id);
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::AwaitingQueueScripts
    );
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(!pipeline.inflight_moves.contains(&job_id));
    pipeline.db.flush_write_queue().await.unwrap();
    let datastore = pipeline.db.datastore();
    let persisted = pipeline
        .db
        .run_sql_blocking_read(async move {
            SqlRuntime::fetch_optional(
                datastore.read_exec(),
                "SELECT status FROM active_jobs WHERE job_id = {}",
                &[SqlArg::I64(job_id.0 as i64)],
            )
            .await?
            .unwrap()
            .text("status")
        })
        .unwrap();
    assert_eq!(persisted, "awaiting_queue_scripts");

    pipeline
        .db
        .save_job_script_effects(
            job_id.0,
            &JobScriptEffects {
                marked_bad: true,
                parameters: [("Detector".into(), "bad".into())].into_iter().collect(),
                ..Default::default()
            },
        )
        .unwrap();
    // Restart recovery does not replay a started script, but its streamed
    // directives must still govern the next completion pass.
    pipeline.db.recover_script_events().unwrap();
    pipeline.db.notify_script_events_changed();
    let TerminalPostProcessingEvent::QueueDone(id, result) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue completion");
    };
    assert_eq!(id, job_id);
    result.as_ref().unwrap();
    pipeline.handle_queue_scripts_done(id, result).await;
    assert!(!pipeline.queue_script_waiters.contains(&job_id));
    assert!(pipeline.queue_scripts_completed.contains(&job_id));
    assert!(!pipeline.par2_verified.contains(&job_id));
    assert!(!pipeline.inflight_moves.contains(&job_id));
    assert!(pipeline.inflight_terminal_post_processing.contains(&job_id));
    assert!(
        pipeline.jobs[&job_id]
            .spec
            .metadata
            .contains(&("Detector".into(), "bad".into()))
    );
    loop {
        match pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::Started(id) => {
                pipeline.handle_terminal_post_processing_started(id)
            }
            TerminalPostProcessingEvent::Done(done) if done.job_id == job_id => {
                pipeline.handle_terminal_post_processing_done(done);
                break;
            }
            _ => {}
        }
    }
    pipeline.db.flush_write_queue().await.unwrap();
    let history = pipeline.db.get_job_history(job_id.0).unwrap().unwrap();
    assert!(history.error_message.unwrap().contains("FAILURE/BAD"));
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
        .save_post_processing_script_lists(&ScriptLists {
            global: ScriptList::new(vec![ScriptListEntry::new(
                ScriptName::new("post.sh").unwrap(),
            )])
            .unwrap(),
            ..Default::default()
        })
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

/// The live path: a job's last article is decoded by the real decode seam, and
/// the completion check that follows must hold the job at the NZB_DOWNLOADED
/// barrier instead of moving it to the complete folder.
#[tokio::test]
async fn the_last_decode_of_a_downloaded_job_holds_the_nzb_downloaded_barrier() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    let job_id = JobId(165);
    let payload = b"standalone payload bytes for the barrier".to_vec();
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "script-barrier-live",
            &[("payload.bin".to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    park_job_on_its_final_decode(
        &mut pipeline,
        SegmentId {
            file_id,
            segment_number: 0,
        },
        payload.len() as u64,
    );
    settle_queued_decode(&mut pipeline, file_id, 0, 0, &payload, "payload.bin").await;
    assert!(
        pipeline.queue_script_waiters.contains(&job_id),
        "the completion pass did not hold the barrier; status {:?}, pending download work {}",
        pipeline.jobs[&job_id].status,
        pipeline.job_has_pending_download_pipeline_work(job_id)
    );
    assert!(pipeline.inflight_moves.is_empty());
    let TerminalPostProcessingEvent::QueueAdmitted(id) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue admission");
    };
    assert_eq!(id, job_id);
    pipeline.handle_queue_scripts_admitted(id);
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::AwaitingQueueScripts
    );
}

/// The streamed shape: the job's last article was decoded on its download lane,
/// and the completion pass runs while that article's download result is still
/// booked as pending. The barrier must hold here all the same; this is the pass
/// that otherwise moves the job to the complete folder.
#[tokio::test]
async fn the_streamed_last_decode_of_a_downloaded_job_holds_the_nzb_downloaded_barrier() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_queue_script(&pipeline.db, temp.path());
    let job_id = JobId(166);
    let payload = b"standalone payload bytes decoded on the lane".to_vec();
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "script-barrier-streamed",
            &[("payload.bin".to_string(), payload.len() as u32)],
        ),
    )
    .await;
    let file_id = NzbFileId {
        job_id,
        file_index: 0,
    };
    pipeline.active_download_passes.insert(job_id);
    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state.download_queue = DownloadQueue::new();
        state.recovery_queue = DownloadQueue::new();
    }
    // What `process_released_download_done` holds open around the decode.
    pipeline.note_released_download_result_pending(job_id, payload.len() as u64);
    assert!(pipeline.job_has_pending_download_pipeline_work(job_id));
    submit_decoded_segment(&mut pipeline, file_id, 0, 0, &payload, "payload.bin", None).await;
    pipeline.finish_released_download_result_processing(job_id, payload.len() as u64);
    pipeline.maybe_finish_download_pass(job_id);
    assert!(
        pipeline.queue_script_waiters.contains(&job_id),
        "the completion pass did not hold the barrier; status {:?}",
        pipeline.jobs[&job_id].status
    );
    assert!(pipeline.inflight_moves.is_empty());
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Downloading);
    let TerminalPostProcessingEvent::QueueAdmitted(id) = pipeline
        .terminal_post_processing_done_rx
        .recv()
        .await
        .unwrap()
    else {
        panic!("expected queue admission");
    };
    assert_eq!(id, job_id);
    pipeline.handle_queue_scripts_admitted(id);
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::AwaitingQueueScripts
    );
    // The download pass that closes after the booking must not disturb the
    // held job.
    while let Some(next) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(next).await;
    }
    assert!(pipeline.inflight_moves.is_empty());
    assert_eq!(
        pipeline.jobs[&job_id].status,
        JobStatus::AwaitingQueueScripts
    );
}
