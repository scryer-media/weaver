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
    result.unwrap();
    pipeline.queue_script_waiters.remove(&job_id);
    pipeline.queue_scripts_completed.insert(job_id);
    assert!(pipeline.apply_queue_script_effects(job_id));
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
