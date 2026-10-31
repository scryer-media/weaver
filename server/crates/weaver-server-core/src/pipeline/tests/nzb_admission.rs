// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 8]);
    let nzb_zstd = nzb_zstd_for(&spec);
    pipeline
        .add_job(
            JobId(id),
            spec,
            PathBuf::from(format!("queued-{id}.nzb")),
            nzb_zstd,
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
    pipeline
        .shared_state
        .queue
        .clone()
        .add(
            JobId(2),
            segmented_job_spec("Queued NZB 2", "payload.bin", &[512; 8]),
            PathBuf::from("queued-2.nzb"),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions::default(),
            pipeline.event_tx.clone(),
        )
        .await
        .unwrap();
    // Model the prepared history-reprocess ticket before the actor installs it.
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
    // Reprocessing refetches articles without a placement checkpoint, and
    // none of them may run yet.
    assert!(pipeline.jobs[&JobId(2)].articles.queued_download_len() > 0);
    assert!(pipeline.jobs[&JobId(2)].articles.active().is_none());
    assert!(
        pipeline
            .graph_jobs
            .get(&JobId(2))
            .is_none_or(|job| !job.awaits_mail())
    );
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
    assert_eq!(pipeline.jobs.len(), 124);
    assert_eq!(
        pipeline.shared_state.queue.read(|inner| inner.jobs.len()),
        124
    );

    // The admitted jobs' articles are built as their first handout builds
    // them.
    let mut admitted: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    admitted.sort_unstable();
    for job_id in admitted {
        activate_job(&mut pipeline, job_id).await;
    }

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
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .pop()
            .unwrap();
        let segment_id = work.segment_id;
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .articles
            .active_mut()
            .unwrap()
            .download_queue = DownloadQueue::new();
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
        assert!(
            !pipeline
                .download_scheduler_eligible_jobs()
                .contains(&JobId(11))
        );
    }
    // Empty article queues and stalled downstream processing retain the same
    // owners. Every job is installed, but only an admitted job has had its
    // articles built; the rest hold none until a slot admits them.
    for (id, state) in pipeline.jobs.iter_mut() {
        if pipeline.nzb_admitted.contains(id) {
            state.articles.active_mut().unwrap().download_queue = DownloadQueue::new();
        } else {
            assert!(state.articles.active().is_none());
        }
        state.post_state = crate::PostState::WaitingForVolumes;
    }
    for _ in 0..24 {
        pipeline.apply_queue_intents().await;
        assert_eq!(pipeline.nzb_admitted.len(), 10);
        assert!(
            !pipeline
                .download_scheduler_eligible_jobs()
                .contains(&JobId(11))
        );
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
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
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
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
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
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(13))
    );
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
           (9, 'cancelled', 'active'), (10, 'downloading', 'paused'),
           (11, 'partial', 'active');
         INSERT INTO active_file_progress VALUES (4), (5);
         INSERT INTO active_files VALUES (6);",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../db/migrations/0073_nzb_admission/sqlite.sql"
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
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
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
    assert!(!pipeline.blocked_restores.contains_key(&JobId(1)));
    assert!(!pipeline.blocked_restores.contains_key(&JobId(2)));
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
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
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

// Scripts for every queue event, whose script file is gone so that each run
// ends as soon as its turn comes.
fn enable_arrival_scripts(pipeline: &Pipeline, root: &std::path::Path) {
    super::script_events::enable_queue_script(&pipeline.db, root);
    std::fs::remove_file(
        pipeline
            .db
            .post_processing_script_directory()
            .unwrap()
            .join("queue.sh"),
    )
    .unwrap();
}

// Lets the pipeline release every arrival hold whose scripts end, until the
// holds of `ids` have all been released.
async fn finish_added_scripts(pipeline: &mut Pipeline, ids: &[JobId]) {
    let mut waiting: HashSet<_> = ids.iter().copied().collect();
    while !waiting.is_empty() {
        if let TerminalPostProcessingEvent::AddedScriptsDone(id, result) = pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            pipeline.handle_added_scripts_done(id, result);
            waiting.remove(&id);
        }
    }
}

// The row is the add: the job is installed with its files, its arrival was
// announced and its scripts ran, and none of its articles may go out.
async fn assert_added_but_not_dispatched(pipeline: &mut Pipeline, id: JobId) {
    assert_eq!(pipeline.jobs[&id].spec.files.len(), 1);
    let info = pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .find(|row| row.id == id)
            .unwrap()
            .info(&inner.globals)
    });
    assert_eq!(info.total_files, 1);
    assert_eq!(info.total_bytes, 512 * 8);
    assert!(pipeline.held_for_added_scripts(id));
    finish_added_scripts(pipeline, &[id]).await;
    let results = pipeline.db.event_script_results(id.0).unwrap();
    assert_eq!(results.len(), 1, "{results:?}");
    assert!(!pipeline.nzb_admitted.contains(&id));
    assert!(!pipeline.download_scheduler_eligible_jobs().contains(&id));
}

#[tokio::test]
async fn nzb_added_under_global_pause_is_installed_and_announced_before_resume() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = handle(&pipeline);
    handle.pause_all().await.unwrap();
    enqueue(&mut pipeline, 1).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(1)).await;
    pipeline.apply_queue_intents().await;
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(1))
    );
    handle.resume_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&JobId(1)));
    assert!(
        pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(1))
    );
    assert_eq!(pipeline.db.event_script_results(1).unwrap().len(), 1);
}

#[tokio::test]
async fn nzb_added_under_scheduled_pause_is_installed_and_announced_before_resume() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = handle(&pipeline);
    handle
        .apply_schedule_action(crate::bandwidth::ScheduleAction::Pause)
        .await
        .unwrap();
    assert!(
        pipeline
            .shared_state
            .queue
            .read(|inner| inner.globals.scheduled_pause)
    );
    enqueue(&mut pipeline, 1).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(1)).await;
    handle
        .apply_schedule_action(crate::bandwidth::ScheduleAction::Resume)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.contains(&JobId(1)));
    assert!(
        pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(1))
    );
    assert_eq!(pipeline.db.event_script_results(1).unwrap().len(), 1);
}

#[tokio::test]
async fn shutdown_installs_and_announces_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    pipeline
        .shared_state
        .queue
        .clone()
        .add(
            JobId(1),
            segmented_job_spec("Queued NZB 1", "payload.bin", &[512; 8]),
            PathBuf::from("queued-1.nzb"),
            sample_nzb_zstd(),
            crate::jobs::AddJobOptions::default(),
            pipeline.event_tx.clone(),
        )
        .await
        .unwrap();
    pipeline.shared_state.queue.write(|inner| {
        inner.globals.shutting_down = true;
        crate::pipeline::queue::Change::changed(())
    });
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.jobs.contains_key(&JobId(1)));
    assert!(!pipeline.held_for_added_scripts(JobId(1)));
    assert!(pipeline.nzb_admitted.is_empty());
    pipeline.db.flush_write_queue().await.unwrap();
    assert_eq!(pipeline.db.queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn nzb_added_with_every_slot_taken_is_announced_and_waits_to_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    finish_added_scripts(&mut pipeline, &[JobId(1)]).await;
    enqueue(&mut pipeline, 2).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(2)).await;
    pipeline.apply_queue_intents().await;
    assert!(pipeline.shared_state.queue.read(|inner| {
        let row = inner.jobs.iter().find(|row| row.id == JobId(2)).unwrap();
        row.info(&inner.globals).download_wait_reason.as_deref()
            == Some(crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON)
    }));
    handle.pause_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(2)]));
    assert!(
        pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
    assert_eq!(pipeline.db.event_script_results(2).unwrap().len(), 1);
}

// Adds a job whose spec is the one its saved NZB parses to, so a restart
// restores the same files.
async fn enqueue_saved(pipeline: &mut Pipeline, id: u64) {
    let nzb_zstd = sample_nzb_zstd();
    let path = PathBuf::from(format!("queued-{id}.nzb"));
    let nzb = crate::ingest::parse_persisted_nzb_bytes(&nzb_zstd).unwrap();
    let spec = crate::ingest::nzb_to_spec(&nzb, &path, None, Vec::new());
    pipeline
        .add_job(
            JobId(id),
            spec,
            path,
            nzb_zstd,
            crate::jobs::AddJobOptions::default(),
        )
        .await
        .unwrap();
}

// Restarts the pipeline over the same data and hands it every saved job,
// with the NZB cap set to one.
async fn restart_and_restore(temp: &tempfile::TempDir, pipeline: Pipeline) -> Pipeline {
    retire_pipeline_database(pipeline).await;
    let (pipeline, intermediate, _) = new_direct_pipeline(temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
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
    pipeline
}

// A restored row is installed with the files and sizes of its saved NZB and
// is no longer restoring.
fn assert_restored_installed(pipeline: &Pipeline, id: JobId) {
    let spec = &pipeline.jobs[&id].spec;
    assert!(!spec.files.is_empty());
    assert!(spec.total_bytes > 0);
    let (restoring, info) = pipeline.shared_state.queue.read(|inner| {
        let row = inner.jobs.iter().find(|row| row.id == id).unwrap();
        (row.report.restoring, row.info(&inner.globals))
    });
    assert!(!restoring);
    assert!(!pipeline.blocked_restores.contains_key(&id));
    assert_eq!(info.total_files as usize, spec.files.len());
    assert_eq!(info.total_bytes, spec.total_bytes);
    assert_ne!(
        info.download_wait_reason.as_deref(),
        Some("Restoring saved job")
    );
}

#[tokio::test]
async fn restart_installs_every_restored_job_and_admission_waits_for_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    let mut pipeline = restart_and_restore(&temp, pipeline).await;
    pipeline.apply_queue_intents().await;
    assert_restored_installed(&pipeline, JobId(1));
    assert_restored_installed(&pipeline, JobId(2));
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    assert!(
        !pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
    assert!(pipeline.shared_state.queue.read(|inner| {
        let row = inner.jobs.iter().find(|row| row.id == JobId(2)).unwrap();
        row.info(&inner.globals).download_wait_reason.as_deref()
            == Some(crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON)
    }));

    let mut events = pipeline.event_tx.subscribe();
    handle(&pipeline).cancel_job(JobId(1)).await.unwrap();
    pipeline.apply_queue_intents().await;
    finish_cancel_jobs(&mut pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == JobId(1))
        {
            break;
        }
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(2)]));
    assert!(
        pipeline
            .download_scheduler_eligible_jobs()
            .contains(&JobId(2))
    );
}

#[tokio::test]
async fn restart_under_global_pause_installs_every_restored_job_and_resume_admits_one() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let before = handle(&pipeline);
    before
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    before.pause_all().await.unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert!(pipeline.nzb_admitted.is_empty());
    retire_pipeline_database(pipeline).await;
    let (pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let after = handle(&pipeline);
    after
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    after.pause_all().await.unwrap();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        after.restore_job(candidate.request).await.unwrap();
    }
    let mut pipeline = pipeline;
    pipeline.apply_queue_intents().await;
    assert_restored_installed(&pipeline, JobId(1));
    assert_restored_installed(&pipeline, JobId(2));
    assert!(pipeline.nzb_admitted.is_empty());
    let eligible = pipeline.download_scheduler_eligible_jobs();
    assert!(!eligible.contains(&JobId(1)));
    assert!(!eligible.contains(&JobId(2)));
    after.resume_all().await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted, HashSet::from([JobId(1)]));
    let eligible = pipeline.download_scheduler_eligible_jobs();
    assert!(eligible.contains(&JobId(1)));
    assert!(!eligible.contains(&JobId(2)));
}
