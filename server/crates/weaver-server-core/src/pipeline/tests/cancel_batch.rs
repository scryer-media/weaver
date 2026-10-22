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
    pipeline.publish_snapshot();
}

async fn cancel_batch(pipeline: &mut Pipeline, ids: &[JobId]) -> crate::jobs::CancelJobsOutcome {
    cancel_jobs_command(pipeline, ids.iter().copied().collect(), CancellationOrigin::User).await
}

// Makes the history insert for `job_id` fail inside its archive transaction,
// so that job's archive rolls back while the rest of the batch commits.
async fn fail_history_insert_for(pipeline: &Pipeline, job_id: JobId) {
    let datastore = pipeline.db.datastore();
    let sql = format!(
        "CREATE TRIGGER reject_cancel_archive BEFORE INSERT ON job_history WHEN NEW.job_id = {} BEGIN SELECT RAISE(ABORT, 'synthetic archive failure'); END",
        job_id.0
    );
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_archive_fault", |tx| {
        let sql = sql.clone();
        Box::pin(async move {
            tx.execute(&sql, &[]).await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn cancel_batch_of_queued_jobs_archives_every_row_in_one_command() {
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
async fn cancel_batch_keeps_every_job_live_until_its_archive_commits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids = [JobId(31_301), JobId(31_302), JobId(31_303)];
    insert_queued_jobs(&mut pipeline, &ids).await;
    let mut events = pipeline.event_tx.subscribe();
    let (reply, mut answered) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::CancelJobs {
            job_ids: ids.iter().copied().collect(),
            origin: CancellationOrigin::User,
            reply,
        })
        .await;

    // Between the cancel and the commit every job is still the live job, in
    // memory and in the snapshot, and nothing has answered or been announced.
    assert!(answered.try_recv().is_err());
    let snapshot = pipeline.shared_state.list_jobs();
    for job_id in &ids {
        assert!(pipeline.jobs.contains_key(job_id));
        assert!(snapshot.iter().any(|job| job.job_id == *job_id));
    }
    // A second cancel of a job already cancelling is refused, not archived twice.
    let again = cancel_batch(&mut pipeline, &[ids[0]]).await;
    assert!(matches!(
        again[&ids[0]],
        Err(crate::SchedulerError::Conflict(_))
    ));

    finish_cancel_jobs(&mut pipeline).await;
    let outcome = answered.await.unwrap();
    assert_eq!(outcome.len(), ids.len());
    assert!(outcome.values().all(Result::is_ok));
    let snapshot = pipeline.shared_state.list_jobs();
    for job_id in &ids {
        assert!(!pipeline.jobs.contains_key(job_id));
        assert!(snapshot.iter().all(|job| job.job_id != *job_id));
    }
    assert!(pipeline.pending_cancellations.is_empty());
    let mut announced = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let PipelineEvent::JobCancelled { job_id } = event {
            announced.push(job_id);
        }
    }
    announced.sort();
    assert_eq!(announced, ids.to_vec());
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

    // The failed id says why, and stays the live job: in memory, in the
    // snapshot, with its active row and no history row.
    match &outcome[&failing] {
        Err(crate::SchedulerError::State(crate::StateError::Database(message))) => {
            assert!(message.contains("synthetic archive failure"), "{message}");
        }
        other => panic!("expected the archive failure, got {other:?}"),
    }
    assert!(pipeline.jobs.contains_key(&failing));
    assert!(
        pipeline
            .shared_state
            .list_jobs()
            .iter()
            .any(|job| job.job_id == failing)
    );
    assert!(pipeline.db.get_job_history(failing.0).unwrap().is_none());
    assert!(
        pipeline
            .db
            .load_active_jobs()
            .unwrap()
            .contains_key(&failing)
    );
    assert!(pipeline.pending_cancellations.is_empty());
    // The rest of the batch committed and left, one `JobCancelled` each.
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
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_archive_fault", |tx| {
        Box::pin(async move {
            tx.execute("DROP TRIGGER reject_cancel_archive", &[]).await?;
            Ok(())
        })
    })
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
