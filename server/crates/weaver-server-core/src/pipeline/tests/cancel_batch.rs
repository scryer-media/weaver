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
}

async fn cancel_batch(pipeline: &mut Pipeline, ids: &[JobId]) -> crate::jobs::CancelJobsOutcome {
    pipeline.report_all_for_test();
    let mut events = pipeline.event_tx.subscribe();
    let outcome = pipeline
        .shared_state
        .queue
        .cancel_jobs(ids.iter().copied().collect(), CancellationOrigin::User);
    pipeline.apply_queue_intents().await;
    let mut pending: BTreeSet<_> = outcome
        .iter()
        .filter_map(|(id, result)| result.is_ok().then_some(*id))
        .collect();
    while !pending.is_empty() {
        if let PipelineEvent::JobCancelled { job_id } = events.recv().await.unwrap() {
            pending.remove(&job_id);
        }
    }
    outcome
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
