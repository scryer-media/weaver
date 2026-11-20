// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::BTreeMap;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_base::jobs::ids::JobId;
use weaver_server_pipeline::Pipeline;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::pipeline::tests::{
    history_fixture::database, new_direct_pipeline, queue_fixture::*,
};
use weaver_server_pipeline::{JobStatus, history::queries::HistoryQueries};

#[tokio::test]
async fn cancel_batch_of_queued_jobs_archives_every_row() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids: Vec<JobId> = (31_001..31_006).map(JobId).collect();
    insert_queued_jobs(&mut pipeline, &ids).await;

    let outcome = cancel_batch(&mut pipeline, &ids).await;

    assert_eq!(outcome.len(), ids.len());
    assert!(outcome.values().all(Result::is_ok));
    let snapshot = handle(&pipeline).list_jobs();
    for job_id in &ids {
        assert!(installed_spec(&pipeline, *job_id).is_none());
        assert!(snapshot.iter().all(|job| job.job_id != *job_id));
        assert_eq!(
            database(&pipeline)
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
    let row = database(&pipeline)
        .get_job_history(job_id.0)
        .unwrap()
        .expect("archived");
    assert_eq!(row.status, "cancelled");
    assert_eq!(
        row.error_message.as_deref(),
        Some(weaver_server_pipeline::jobs::model::OPERATOR_CANCELLED_ERROR)
    );
    assert_eq!(
        weaver_server_pipeline::jobs::model::job_status_from_persisted_str(
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
    set_moving(&mut pipeline, moving);
    let missing = JobId(31_199);

    let outcome = cancel_batch(&mut pipeline, &[ids[0], moving, ids[2], missing]).await;

    assert!(matches!(
        outcome[&moving],
        Err(weaver_server_pipeline::SchedulerError::Conflict(_))
    ));
    assert!(matches!(
        outcome[&missing],
        Err(weaver_server_pipeline::SchedulerError::JobNotFound(id)) if id == missing
    ));
    assert!(installed_spec(&pipeline, moving).is_some());
    assert!(
        database(&pipeline)
            .get_job_history(moving.0)
            .unwrap()
            .is_none()
    );
    for job_id in [ids[0], ids[2]] {
        assert!(outcome[&job_id].is_ok());
        assert!(installed_spec(&pipeline, job_id).is_none());
        assert_eq!(
            database(&pipeline)
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
    let mut events = handle(&pipeline).subscribe_events();
    let db = database(&pipeline).clone();
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

// A cancelled job leaves its lane like a job that finishes or fails: the
// gauge of its lane counts it no more. One left that was never counted
// cannot take the gauge below zero.
async fn cancelling_a_job_takes_it_off_its_lane_gauge(
    job_id: JobId,
    status: JobStatus,
    gauge: fn(
        &weaver_server_base::operations::metrics::PipelineMetrics,
    ) -> &std::sync::atomic::AtomicUsize,
) {
    use std::sync::atomic::Ordering;

    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    insert_queued_jobs(&mut pipeline, &[job_id]).await;
    let entered = move_to_status(&mut pipeline, job_id, status);
    assert_eq!(gauge(metrics(&pipeline)).load(Ordering::Relaxed), 1);

    let outcome = cancel_batch(&mut pipeline, &[job_id]).await;

    assert!(outcome[&job_id].is_ok());
    assert!(installed_spec(&pipeline, job_id).is_none());
    assert_eq!(gauge(metrics(&pipeline)).load(Ordering::Relaxed), 0);
    metrics(&pipeline).post_state_left(entered);
    assert_eq!(gauge(metrics(&pipeline)).load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn cancelling_a_verifying_job_takes_it_off_the_verify_gauge() {
    cancelling_a_job_takes_it_off_its_lane_gauge(JobId(31_301), JobStatus::Verifying, |m| {
        &m.verify_active
    })
    .await;
}

#[tokio::test]
async fn cancelling_a_repairing_job_takes_it_off_the_repair_gauge() {
    cancelling_a_job_takes_it_off_its_lane_gauge(JobId(31_302), JobStatus::Repairing, |m| {
        &m.repair_active
    })
    .await;
}

#[tokio::test]
async fn cancelling_an_extracting_job_takes_it_off_the_extract_gauge() {
    cancelling_a_job_takes_it_off_its_lane_gauge(JobId(31_303), JobStatus::Extracting, |m| {
        &m.extract_active
    })
    .await;
}

// Makes the history insert for `job_id` fail inside its archive transaction,
// so that job's archive rolls back while the rest of the batch commits.
async fn fail_history_insert_for(pipeline: &Pipeline, job_id: JobId) {
    let datastore = database(pipeline).datastore();
    let sql = format!(
        "CREATE TRIGGER reject_cancel_archive BEFORE INSERT ON job_history WHEN NEW.job_id = {} BEGIN SELECT RAISE(ABORT, 'synthetic archive failure'); END",
        job_id.0
    );
    weaver_server_pipeline::persistence::sql_runtime::SqlRuntime::run_in_transaction(
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

// A cancelled job leaves neither its cause nor its refusing servers behind.
// A restore keeps the job id: the job under that id again, failing for want
// of articles, says nothing of the earlier run's refusal.
#[tokio::test]
async fn a_cancelled_job_restored_under_its_id_carries_no_refusal_from_the_earlier_run() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    set_retention_servers(&mut pipeline);
    let job_id = JobId(31_071);
    insert_queued_jobs(&mut pipeline, &[job_id]).await;
    note_missing_after_refusal(&mut pipeline, job_id, 1);
    assert!(login_refusal_words(&pipeline, job_id).is_some());

    let outcome = cancel_batch(&mut pipeline, &[job_id]).await;
    assert!(outcome[&job_id].is_ok());
    assert!(!has_login_refusals(&pipeline, job_id));
    assert!(!has_terminal_cause(&pipeline, job_id));

    insert_queued_jobs(&mut pipeline, &[job_id]).await;
    assert_eq!(login_refusal_words(&pipeline, job_id), None);
    fail_on_health(&mut pipeline, job_id, "the post cannot complete".into());
    assert!(matches!(
        installed_status(&pipeline, job_id),
        Some(JobStatus::Failed { error }) if error == "the post cannot complete"
    ));
    assert!(!has_terminal_cause(&pipeline, job_id));
}

#[tokio::test]
async fn cancel_batch_archive_failure_leaves_that_job_live_and_recoverable() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let ids = [JobId(31_401), JobId(31_402), JobId(31_403)];
    insert_queued_jobs(&mut pipeline, &ids).await;
    let failing = JobId(31_402);
    database(&pipeline).flush_write_queue().await.unwrap();
    let checkpoint = CancelCheckpoint::observe(&pipeline, failing);
    let revision = checkpoint.revision();
    let stamp = checkpoint.stamp();
    fail_history_insert_for(&pipeline, failing).await;
    let mut events = handle(&pipeline).subscribe_events();

    let outcome = cancel_batch(&mut pipeline, &ids).await;

    assert!(
        !checkpoint.cancelled(),
        "a failed archive restores checkpoint writes"
    );
    assert_ne!(
        checkpoint.revision(),
        revision,
        "old error mail becomes stale"
    );
    assert!(
        checkpoint.stamp() > stamp,
        "recovery records a fresh full checkpoint"
    );
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(checkpoint_node_count(&pipeline, failing) > 0);

    // Every intent was recorded. The failed id stays the live job: in
    // memory, visible in the Queue with no cancel pending, with its active
    // row and no history row.
    assert!(outcome.values().all(Result::is_ok));
    assert!(installed_spec(&pipeline, failing).is_some());
    assert!(
        handle(&pipeline)
            .list_jobs()
            .iter()
            .any(|job| job.job_id == failing)
    );
    assert_eq!(
        cancel_and_retire_flags(&pipeline, failing),
        Some((false, false, false))
    );
    assert!(
        database(&pipeline)
            .get_job_history(failing.0)
            .unwrap()
            .is_none()
    );
    assert!(
        database(&pipeline)
            .load_active_jobs()
            .unwrap()
            .contains_key(&failing)
    );
    // The rest of the batch committed and left, one `JobCancelled` each.
    for job_id in [ids[0], ids[2]] {
        assert!(installed_spec(&pipeline, job_id).is_none());
        assert_eq!(
            database(&pipeline)
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
    let datastore = database(&pipeline).datastore();
    weaver_server_pipeline::persistence::sql_runtime::SqlRuntime::run_in_transaction(
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
    assert!(installed_spec(&pipeline, failing).is_none());
    assert_eq!(
        database(&pipeline)
            .get_job_history(failing.0)
            .unwrap()
            .expect("archived")
            .status,
        "cancelled"
    );
}
