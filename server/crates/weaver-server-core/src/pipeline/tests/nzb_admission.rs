// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    pipeline
        .add_job(
            JobId(id),
            segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 8]),
            PathBuf::from(format!("queued-{id}.nzb")),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

fn handle(pipeline: &Pipeline) -> SchedulerHandle {
    SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone())
}

#[tokio::test]
async fn paused_reprocess_initialization_waits_for_resume_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    // Model the prepared history-reprocess ticket while it waits behind the cap.
    pipeline.shared_state.queue.write(|inner| {
        let row = inner
            .jobs
            .iter_mut()
            .find(|row| row.id == JobId(2))
            .unwrap();
        let prepared = row.admission.as_mut().unwrap();
        prepared.restart = Some(crate::pipeline::queue::RestartAdmission {
            reprocess: true,
            working_dir: prepared.working_dir.clone(),
            downloaded_bytes: prepared.spec.total_bytes,
        });
        crate::pipeline::queue::Change::changed(())
    });
    handle.pause_job(JobId(2)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_reprocess_pending.contains(&JobId(2)));
    assert!(!pipeline.nzb_admitted.contains(&JobId(2)));
    assert!(pipeline.jobs[&JobId(2)].download_queue.is_empty());
    handle.resume_job(JobId(2)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_reprocess_pending.contains(&JobId(2)));
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&JobId(2)));
    assert!(!pipeline.nzb_reprocess_pending.contains(&JobId(2)));
}

#[tokio::test]
async fn paused_unadmitted_jobs_cannot_launch_health_probes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    enqueue(&mut pipeline, 1).await;
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.nzb_admitted.contains(&JobId(1)));
    assert!(!pipeline.activate_health_probes(JobId(1)));
    assert!(!pipeline.jobs[&JobId(1)].health_probing);
    assert_eq!(pipeline.jobs[&JobId(1)].health_probe_round, 0);
    pipeline.jobs.get_mut(&JobId(1)).unwrap().failed_bytes = 4096;
    pipeline.check_health(JobId(1));
    assert_eq!(pipeline.jobs[&JobId(1)].status, JobStatus::Paused);
    assert!(!pipeline.jobs[&JobId(1)].health_probing);
}

#[tokio::test]
async fn twenty_four_providers_cannot_admit_more_than_ten_of_124_nzbs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let mut config = weaver_nntp::client::NntpClientConfig::single(
        weaver_nntp::ServerConfig {
            host: "127.0.0.1".into(),
            port: 1,
            tls: false,
            ..Default::default()
        },
        10,
    );
    config.servers = (0..24)
        .map(|_| {
            weaver_nntp::client::NntpClientConfig::single(
                weaver_nntp::ServerConfig {
                    host: "127.0.0.1".into(),
                    port: 1,
                    tls: false,
                    ..Default::default()
                },
                10,
            )
            .servers
            .remove(0)
        })
        .collect();
    pipeline.nntp = Arc::new(weaver_nntp::NntpClient::new(config));
    pipeline.tuner.set_connection_limit(240);
    for id in 1..=124 {
        enqueue(&mut pipeline, id).await;
    }
    assert_eq!(pipeline.nzb_admitted.len(), 10);
    assert_eq!(
        pipeline.db.load_nzb_admissions().unwrap(),
        pipeline.nzb_admitted
    );
    assert_eq!(pipeline.jobs.len(), 10);
    assert_eq!(
        pipeline.shared_state.queue.read(|inner| inner.jobs.len()),
        124
    );

    // Feed real transport failures through the result/retry path. Provider
    // unavailability and missing early articles must not expand admission.
    for (id, error) in [
        (
            1,
            weaver_nntp::NntpError::Tls(tokio_rustls::rustls::Error::General(
                "synthetic handshake failure".into(),
            )),
        ),
        (2, weaver_nntp::NntpError::ConnectionClosed),
        (3, weaver_nntp::NntpError::ArticleNotFound),
    ] {
        let job_id = JobId(id);
        let work = pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        let segment_id = work.segment_id;
        pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
        pipeline.hold_test_lane_works(vec![work], false);
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::Fetch(
                    DownloadFailure::from_lane_acquire_failure(Some(&error)),
                )),
                attempts: Vec::new(),
                lane_observation: None,
                source_server_idx: None,
                origin: DownloadResultOrigin::NormalPrimary,
                retry_count: 0,
                exclude_servers: Vec::new(),
                release_connection_slot: true,
            })
            .await;
        pipeline.apply_queue_intents().await;
        assert_eq!(pipeline.nzb_admitted.len(), 10);
        assert!(!pipeline.jobs.contains_key(&JobId(11)));
    }
    // Empty article queues and stalled downstream processing retain the same owners.
    for state in pipeline.jobs.values_mut() {
        state.download_queue = DownloadQueue::new();
        state.post_state = crate::PostState::WaitingForVolumes;
    }
    for _ in 0..24 {
        pipeline.apply_queue_intents().await;
        assert_eq!(pipeline.nzb_admitted.len(), 10);
        assert!(!pipeline.jobs.contains_key(&JobId(11)));
    }
    assert!(pipeline.shared_state.queue.read(|inner| {
        inner.jobs.iter().filter(|row| row.id.0 > 10).all(|row| {
            let info = row.info(&inner.globals);
            info.download_state == crate::DownloadState::Queued
                && info.download_wait_reason.as_deref() == Some("nzb_slot")
        })
    }));
}

#[tokio::test]
async fn live_limits_drain_and_manual_pause_releases_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 2);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 3).await;
    assert_eq!(pipeline.nzb_admitted.len(), 2);
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(2)]));
    handle.pause_job(JobId(2)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(3)]));
    handle.resume_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(3)]));
    assert_eq!(
        pipeline.db.load_nzb_admissions().unwrap(),
        pipeline.nzb_admitted
    );
}

#[tokio::test]
async fn automatic_pause_and_empty_queues_keep_their_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    pipeline.pause_job_runtime(JobId(1)).unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    assert!(!pipeline.jobs.contains_key(&JobId(2)));
}

#[tokio::test]
async fn manual_pause_waits_for_work_and_resume_during_drain_keeps_ownership() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    pipeline.active_decodes_by_job.insert(JobId(1), 1);
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    assert!(
        pipeline
            .db
            .load_nzb_pause_releases()
            .unwrap()
            .contains(&JobId(1))
    );
    handle.resume_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.nzb_pause_releases.contains(&JobId(1)));
    assert!(
        !pipeline
            .db
            .load_nzb_pause_releases()
            .unwrap()
            .contains(&JobId(1))
    );
    pipeline.active_decodes_by_job.remove(&JobId(1));
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(2)]));
}

#[tokio::test]
async fn global_pause_keeps_owners_and_blocks_new_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enqueue(&mut pipeline, 1).await;
    let handle = handle(&pipeline);
    handle.pause_all().await.unwrap();
    enqueue(&mut pipeline, 2).await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    assert!(!pipeline.jobs.contains_key(&JobId(2)));
    handle.resume_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1), JobId(2)]));
}

#[tokio::test]
async fn restart_restores_all_owners_before_queue_recovery_even_above_limit() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(12).unwrap())
        .await
        .unwrap();
    for id in 1..=12 {
        enqueue(&mut pipeline, id).await;
    }
    let owners = pipeline.nzb_admitted.clone();
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    assert_eq!(pipeline.nzb_admitted, owners);
    enqueue(&mut pipeline, 13).await;
    assert_eq!(pipeline.nzb_admitted, owners);
    assert!(!pipeline.jobs.contains_key(&JobId(13)));
    assert_eq!(pipeline.db.load_nzb_admissions().unwrap(), owners);
}

#[tokio::test]
async fn legacy_migration_grandfathers_started_work_but_not_paused_or_untouched_jobs() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE active_jobs (job_id INTEGER PRIMARY KEY, status TEXT, run_state TEXT);
         CREATE TABLE active_file_progress (job_id INTEGER);
         CREATE TABLE active_files (job_id INTEGER);
         INSERT INTO active_jobs VALUES
           (1, 'queued', 'active'), (2, 'downloading', 'active'),
           (3, 'extracting', 'active'), (4, 'paused', 'paused'),
           (5, 'queued', 'active'), (6, 'queued', 'active'),
           (7, 'complete', 'active'), (8, 'failed', 'active'),
           (9, 'cancelled', 'active'), (10, 'downloading', 'paused');
         INSERT INTO active_file_progress VALUES (4), (5);
         INSERT INTO active_files VALUES (6);",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../db/migrations/0066_nzb_admission/sqlite.sql"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let owners: Vec<i64> =
        sqlx::query_scalar("SELECT job_id FROM active_jobs WHERE nzb_admitted = 1 ORDER BY job_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(owners, vec![2, 3, 5, 6]);
}

#[tokio::test]
async fn waiting_priority_changes_do_not_evict_an_owner() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    for id in 1..=3 {
        enqueue(&mut pipeline, id).await;
    }
    handle
        .update_job(
            JobId(3),
            crate::jobs::JobUpdate {
                metadata_edits: vec![("priority".into(), Some("high".into()))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(3)]));
}

#[tokio::test]
async fn restart_between_pause_intent_and_drain_restores_without_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    handle(&pipeline).pause_job(JobId(2)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.nzb_admitted.contains(&JobId(2)));
    pipeline.db.set_nzb_pause_release(JobId(1), true).unwrap();
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle(&pipeline)
            .restore_job(candidate.request)
            .await
            .unwrap();
    }
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.is_empty());
    assert!(pipeline.db.load_nzb_admissions().unwrap().is_empty());
    assert_eq!(pipeline.jobs[&JobId(1)].run_state, crate::RunState::Paused);
    assert_eq!(pipeline.jobs[&JobId(2)].run_state, crate::RunState::Paused);
}

#[tokio::test]
async fn cancellation_releases_only_after_terminal_persistence_succeeds() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    let datastore = pipeline.db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_archive_fault", |tx| {
        Box::pin(async move {
            tx.execute("CREATE TRIGGER reject_nzb_archive BEFORE INSERT ON job_history BEGIN SELECT RAISE(ABORT, 'synthetic archive failure'); END", &[]).await?;
            Ok(())
        })
    }).await.unwrap();
    handle.cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    assert!(!pipeline.jobs.contains_key(&JobId(2)));
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(
        &datastore,
        "clear_archive_fault",
        |tx| {
            Box::pin(async move {
                tx.execute("DROP TRIGGER reject_nzb_archive", &[]).await?;
                Ok(())
            })
        },
    )
    .await
    .unwrap();
    let mut events = pipeline.event_tx.subscribe();
    handle.cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == JobId(1))
        {
            break;
        }
    }
    // A new run reusing the old ID must not inherit its actor-side slot
    // before the actor has observed the terminal queue removal.
    enqueue(&mut pipeline, 1).await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(2)]));
    assert_eq!(
        pipeline.db.load_nzb_admissions().unwrap(),
        pipeline.nzb_admitted
    );
}
