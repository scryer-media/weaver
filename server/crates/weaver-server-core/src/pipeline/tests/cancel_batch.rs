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

// A cancelled job leaves its lane like a job that finishes or fails: the
// gauge of the jobs verifying counts it no more. One left that was never
// counted cannot take the gauge below zero.
#[tokio::test]
async fn cancelling_a_verifying_job_takes_it_off_the_verify_gauge() {
    use std::sync::atomic::Ordering;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(31_301);
    insert_queued_jobs(&mut pipeline, &[job_id]).await;
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let lane = state.post_state;
    state.status = JobStatus::Verifying;
    state.refresh_runtime_lanes_from_status();
    let entered = state.post_state;
    pipeline.metrics.post_state_moved(lane, entered);
    assert_eq!(pipeline.metrics.verify_active.load(Ordering::Relaxed), 1);

    let outcome = cancel_batch(&mut pipeline, &[job_id]).await;

    assert!(outcome[&job_id].is_ok());
    assert!(!pipeline.jobs.contains_key(&job_id));
    assert_eq!(pipeline.metrics.verify_active.load(Ordering::Relaxed), 0);
    pipeline
        .metrics
        .post_state_left(crate::jobs::model::PostState::Verifying);
    assert_eq!(pipeline.metrics.verify_active.load(Ordering::Relaxed), 0);
}
