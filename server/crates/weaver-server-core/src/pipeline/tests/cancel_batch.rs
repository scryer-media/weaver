// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use crate::jobs::handle::CancellationOrigin;

async fn insert_queued_jobs(pipeline: &mut Pipeline, ids: &[JobId]) {
    for job_id in ids {
        insert_active_job(
            pipeline,
            *job_id,
            standalone_job_spec(
                &format!("Batch {}", job_id.0),
                &[(format!("batch-{}.bin", job_id.0), 64)],
            ),
        )
        .await;
    }
    pipeline.report_all_for_test();
}

async fn cancel_batch(pipeline: &mut Pipeline, ids: &[JobId]) -> crate::jobs::CancelJobsOutcome {
    pipeline.report_all_for_test();
    let mut events = pipeline.event_tx.subscribe();
    let outcome = pipeline
        .shared_state
        .queue
        .cancel_jobs(ids.iter().copied().collect(), CancellationOrigin::User);
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(pipeline).await;
    // Every accepted id whose archive committed announces itself; one whose
    // archive failed is live again instead.
    let mut pending: BTreeSet<_> = outcome
        .iter()
        .filter_map(|(id, result)| result.is_ok().then_some(*id))
        .filter(|id| !pipeline.jobs.contains_key(id))
        .collect();
    while !pending.is_empty() {
        if let PipelineEvent::JobCancelled { job_id } = events.recv().await.unwrap() {
            pending.remove(&job_id);
        }
    }
    outcome
}

// Makes the history insert for `job_id` fail inside its archive transaction,
// so that job's archive rolls back while the rest of the batch commits.
async fn fail_history_insert_for(pipeline: &Pipeline, job_id: JobId) {
    let datastore = pipeline.db.datastore();
    let sql = format!(
        "CREATE TRIGGER reject_cancel_archive BEFORE INSERT ON job_history WHEN NEW.job_id = {} BEGIN SELECT RAISE(ABORT, 'synthetic archive failure'); END",
        job_id.0
    );
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "test_archive_fault",
        |tx| {
            let sql = sql.clone();
            Box::pin(async move {
                tx.execute(&sql, &[]).await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn cancel_batch_of_queued_jobs_archives_every_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids: Vec<JobId> = (31_001..31_006).map(JobId).collect();
    insert_queued_jobs(&mut pipeline, &ids).await;

    let outcome = cancel_batch(&mut pipeline, &ids).await;

    assert_eq!(outcome.len(), ids.len());
    assert!(outcome.values().all(Result::is_ok));
    let snapshot = pipeline.shared_state.list_jobs();
    for job_id in &ids {
        assert!(!pipeline.jobs.contains_key(job_id));
        assert!(snapshot.iter().all(|job| job.job_id != *job_id));
        assert_eq!(
            pipeline
                .db
                .get_job_history(job_id.0)
                .unwrap()
                .expect("archived")
                .status,
            "cancelled"
        );
    }
}

#[tokio::test]
async fn an_operator_cancel_is_recorded_as_cancelled_by_the_operator() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31_051);
    insert_queued_jobs(&mut pipeline, &[job_id]).await;

    let outcome = cancel_batch(&mut pipeline, &[job_id]).await;

    assert!(outcome[&job_id].is_ok());
    let row = pipeline
        .db
        .get_job_history(job_id.0)
        .unwrap()
        .expect("archived");
    assert_eq!(row.status, "cancelled");
    assert_eq!(
        row.error_message.as_deref(),
        Some(crate::jobs::model::OPERATOR_CANCELLED_ERROR)
    );
    assert_eq!(
        crate::jobs::model::job_status_from_persisted_str(
            &row.status,
            row.error_message.as_deref()
        ),
        JobStatus::Failed {
            error: "cancelled by operator".to_string()
        }
    );
}

#[tokio::test]
async fn cancel_batch_refusal_of_one_id_leaves_the_rest_cancelled() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids = [JobId(31_101), JobId(31_102), JobId(31_103)];
    insert_queued_jobs(&mut pipeline, &ids).await;
    let moving = JobId(31_102);
    pipeline.inflight_moves.insert(moving);
    let missing = JobId(31_199);

    let outcome = cancel_batch(&mut pipeline, &[ids[0], moving, ids[2], missing]).await;

    assert!(matches!(
        outcome[&moving],
        Err(crate::SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        outcome[&missing],
        Err(crate::SchedulerError::JobNotFound(id)) if id == missing
    ));
    assert!(pipeline.jobs.contains_key(&moving));
    assert!(pipeline.db.get_job_history(moving.0).unwrap().is_none());
    for job_id in [ids[0], ids[2]] {
        assert!(outcome[&job_id].is_ok());
        assert!(!pipeline.jobs.contains_key(&job_id));
        assert_eq!(
            pipeline
                .db
                .get_job_history(job_id.0)
                .unwrap()
                .expect("archived")
                .status,
            "cancelled"
        );
    }
}

#[tokio::test]
async fn cancel_event_follows_a_readable_history_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids = [JobId(31_201), JobId(31_202)];
    insert_queued_jobs(&mut pipeline, &ids).await;
    let mut events = pipeline.event_tx.subscribe();
    let db = pipeline.db.clone();
    // The watcher reads each row the moment its event arrives, before the
    // command's reply is awaited.
    let watcher = tokio::spawn(async move {
        let mut seen = BTreeMap::new();
        while seen.len() < ids.len() {
            if let PipelineEvent::JobCancelled { job_id } = events.recv().await.unwrap() {
                let status = db.get_job_history(job_id.0).unwrap().map(|row| row.status);
                seen.insert(job_id, status);
            }
        }
        seen
    });

    let outcome = cancel_batch(&mut pipeline, &ids).await;

    assert!(outcome.values().all(Result::is_ok));
    let seen = watcher.await.unwrap();
    for job_id in ids {
        assert_eq!(seen[&job_id].as_deref(), Some("cancelled"));
    }
}

#[tokio::test]
async fn cancel_batch_archive_failure_leaves_that_job_live_and_recoverable() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids = [JobId(31_401), JobId(31_402), JobId(31_403)];
    insert_queued_jobs(&mut pipeline, &ids).await;
    let failing = JobId(31_402);
    fail_history_insert_for(&pipeline, failing).await;
    let mut events = pipeline.event_tx.subscribe();

    let outcome = cancel_batch(&mut pipeline, &ids).await;

    // Every intent was recorded. The failed id stays the live job: in
    // memory, visible in the Queue with no cancel pending, with its active
    // row and no history row.
    assert!(outcome.values().all(Result::is_ok));
    assert!(pipeline.jobs.contains_key(&failing));
    assert!(
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .any(|job| job.job_id == failing)
    );
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner.jobs.iter().any(|row| {
            row.id == failing
                && row.intents.cancelled.is_none()
                && !row.intents.delete_history_after_cancel
                && !row.report.retiring
        })
    }));
    assert!(pipeline.db.get_job_history(failing.0).unwrap().is_none());
    assert!(
        pipeline
            .db
            .load_active_jobs()
            .unwrap()
            .contains_key(&failing)
    );
    // The rest of the batch committed and left, one `JobCancelled` each.
    for job_id in [ids[0], ids[2]] {
        assert!(!pipeline.jobs.contains_key(&job_id));
        assert_eq!(
            pipeline
                .db
                .get_job_history(job_id.0)
                .unwrap()
                .expect("archived")
                .status,
            "cancelled"
        );
    }
    let mut announced = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let PipelineEvent::JobCancelled { job_id } = event {
            announced.push(job_id);
        }
    }
    announced.sort();
    assert_eq!(announced, vec![ids[0], ids[2]]);

    // Recoverable: once the fault clears, the same job cancels cleanly.
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "test_archive_fault",
        |tx| {
            Box::pin(async move {
                tx.execute("DROP TRIGGER reject_cancel_archive", &[])
                    .await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    let retried = cancel_batch(&mut pipeline, &[failing]).await;
    assert!(retried[&failing].is_ok());
    assert!(!pipeline.jobs.contains_key(&failing));
    assert_eq!(
        pipeline
            .db
            .get_job_history(failing.0)
            .unwrap()
            .expect("archived")
            .status,
        "cancelled"
    );
}

#[tokio::test]
async fn failed_user_cancel_restores_repair_after_db_recovers() {
    use crate::pipeline::completion::finalize::check::run_file_descriptor_bounded_par2_repair;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let job_id = JobId(41906);
    insert_queued_jobs(&mut pipeline, &[job_id]).await;
    let old_token = pipeline.par2_cancellation_token(job_id);
    let set = build_repairable_par2_set("payload.bin", b"complete", 4, 2);
    let set_id = set.recovery_set_id;
    let session_dir = tempfile::tempdir().unwrap();
    let mut options =
        par2_rs::Par2RepairSessionOptions::new(session_dir.path().to_path_buf(), Vec::new());
    options.file_set = Some(set);
    options.cancel = Some(old_token.clone());
    let session = par2_rs::Par2RepairSession::open(options).unwrap();
    pipeline
        .ensure_par2_runtime(job_id)
        .ensure_set_runtime(set_id)
        .session = Some(session);
    pipeline.par2_analysis_in_flight.insert(
        job_id,
        Par2AnalysisWork {
            work_id: 77,
            recovery_set_id: set_id,
            submitted_at: Instant::now(),
        },
    );
    pipeline
        .metrics
        .verify_active
        .fetch_add(1, Ordering::Relaxed);
    fail_history_insert_for(&pipeline, job_id).await;
    let handle =
        crate::SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    // The reply records the intent; a failed archive restores the live row.
    let outcome = handle
        .cancel_jobs([job_id].into_iter().collect(), CancellationOrigin::User)
        .await
        .unwrap();
    assert!(outcome[&job_id].is_ok());
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    assert!(handle.get_job(job_id).is_ok());
    assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
    assert!(pipeline.jobs.contains_key(&job_id));
    let datastore = pipeline.db.datastore();
    SqlRuntime::run_in_transaction(&datastore, "verification_clear_archive_fault", |tx| {
        Box::pin(async move {
            tx.execute("DROP TRIGGER reject_cancel_archive", &[])
                .await?;
            Ok(())
        })
    })
    .await
    .unwrap();
    assert!(
        old_token.is_cancelled(),
        "the original worker stays cancelled"
    );
    assert!(!pipeline.shared_state.is_job_cancellation_requested(job_id));
    assert!(
        pipeline
            .par2_runtime(job_id)
            .unwrap()
            .set_runtime(set_id)
            .unwrap()
            .session
            .is_none()
    );
    assert!(!pipeline.par2_analysis_in_flight.contains_key(&job_id));
    assert_eq!(pipeline.metrics.verify_active.load(Ordering::Relaxed), 0);
    pipeline
        .handle_par2_analysis_done(Par2AnalysisWorkDone {
            job_id,
            work_id: 77,
            recovery_set_id: set_id,
            outcome:
                crate::pipeline::completion::finalize::check::Par2AnalysisTicketOutcome::TaskFailed(
                    "old cancelled worker".into(),
                ),
        })
        .await;
    assert!(!pipeline.par2_analysis_results.contains_key(&job_id));

    let token = pipeline.par2_cancellation_token(job_id);
    assert!(!token.is_cancelled());
    let repaired_dir = tempfile::tempdir().unwrap();
    let outcome = run_file_descriptor_bounded_par2_repair(
        repaired_dir.path().to_path_buf(),
        build_repairable_par2_set("payload.bin", b"complete", 4, 2),
        HashMap::new(),
        Vec::new(),
        64 * 1024 * 1024,
        token.clone(),
        None,
    )
    .unwrap();
    assert_eq!(outcome.status, par2_rs::Par2RepairStatus::Repaired);
    assert_eq!(
        std::fs::read(repaired_dir.path().join("payload.bin")).unwrap(),
        b"complete"
    );
    assert!(pipeline.shared_state.cancel_active_job_work(job_id));
    assert!(
        token.is_cancelled(),
        "subsequent cancellation still reaches new work"
    );
}
