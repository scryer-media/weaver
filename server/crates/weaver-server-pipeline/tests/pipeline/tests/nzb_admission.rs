// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::HashSet;
use std::path::PathBuf;

use weaver_server_base::events::model::PipelineEvent;
use weaver_server_pipeline::bandwidth::ScheduleAction;
use weaver_server_pipeline::jobs::{
    HoldOrigin,
    model::{FileSummaries, FileSummary},
};
use weaver_server_pipeline::operations::recover_server_state;
use weaver_server_pipeline::pipeline::tests::{
    activate_job,
    control_fixture::run_state,
    dispatch_order_fixture::eligible as dispatch_eligible,
    finish_cancel_jobs,
    history_fixture::{database, pause_runtime},
    new_direct_pipeline,
    nzb_admission_fixture::*,
    nzb_zstd_for,
    queue_fixture::{
        apply_intents, clear_download_queue, handle as queue_handle, installed_spec,
        set_active_decode,
    },
    queue_scale_fixture::{
        NZB_SLOT_WAIT_REASON, persisted_admissions, queued_summaries, restore_blocked,
        row_run_state, runtime_writes, sql,
    },
    retire_pipeline_database, sample_nzb_zstd, segmented_job_spec, standalone_with_par2_job_spec,
    window_fixture::{
        QUEUE_WINDOW, admitted, articles_active, has_installed, installed, installed_count, rows,
        set_progress,
    },
};
use weaver_server_pipeline::post_processing::events::PostProcessingEventsCancelEventScriptsQueries;
use weaver_server_pipeline::post_processing::output::PostProcessingOutputQueries;
use weaver_server_pipeline::{
    DownloadState, JobId, JobSpec, JobUpdate, MessageIds, Pipeline, RunState,
};

#[tokio::test]
async fn paused_reprocess_initialization_waits_for_resume_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    prepare_add(
        &pipeline,
        JobId(2),
        segmented_job_spec("Queued NZB 2", "payload.bin", &[512; 8]),
        PathBuf::from("queued-2.nzb"),
        sample_nzb_zstd(),
    )
    .await;
    mark_reprocess(&mut pipeline, JobId(2));
    handle.pause_job(JobId(2)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(reprocess_pending(&pipeline, JobId(2)));
    assert!(!admissions(&pipeline).contains(&JobId(2)));
    // Reprocessing refetches articles without a placement checkpoint, and
    // none of them may run yet.
    assert!(queued_download_len(&pipeline, JobId(2)) > 0);
    assert!(!articles_active(&pipeline, JobId(2)));
    assert!(!graph_awaits_mail(&pipeline, JobId(2)));
    handle.resume_job(JobId(2)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(reprocess_pending(&pipeline, JobId(2)));
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert!(admissions(&pipeline).contains(&JobId(2)));
    assert!(!reprocess_pending(&pipeline, JobId(2)));
}

// An id gone from the queue while its work is in flight does not call for a
// walk each turn: it is looked at alone, and forgotten once its work drains.
#[tokio::test]
async fn a_retired_id_with_work_in_flight_is_rechecked_without_a_walk() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let id = JobId(77);
    note_seen(&mut pipeline, id);
    note_pause_release(&mut pipeline, id);
    set_move_in_flight(&mut pipeline, id, true);

    assert!(reconcile(&mut pipeline, true));
    assert!(!walk_due(&pipeline));
    assert!(seen(&pipeline).contains(&id));
    assert!(retired_busy(&pipeline).contains(&id));

    assert!(!reconcile(&mut pipeline, false));
    assert!(!walk_due(&pipeline));
    assert!(seen(&pipeline).contains(&id));

    set_move_in_flight(&mut pipeline, id, false);
    assert!(!reconcile(&mut pipeline, false));
    assert!(!seen(&pipeline).contains(&id));
    assert!(!pause_releases(&pipeline).contains(&id));
    assert!(retired_busy(&pipeline).is_empty());
}

// A job that failed or completed is no longer a queue row and holds no
// slot: the walk forgets it, its stored resume with it.
#[tokio::test]
async fn the_walk_forgets_the_stored_resume_of_a_job_that_left() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let (failed, completed) = (JobId(78), JobId(79));
    for id in [failed, completed] {
        note_seen(&mut pipeline, id);
        note_resume(&mut pipeline, id);
    }

    reconcile(&mut pipeline, true);

    assert!(stored_resumes(&pipeline).is_empty());
    assert!(seen(&pipeline).is_empty());
}

#[tokio::test]
async fn twenty_four_providers_cannot_admit_more_than_ten_of_124_nzbs() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    configure_providers(&mut pipeline);
    for id in 1..=124 {
        enqueue(&mut pipeline, id).await;
    }
    assert_eq!(admissions(&pipeline).len(), 10);
    assert_eq!(
        persisted_admissions(database(&pipeline)),
        *admissions(&pipeline)
    );
    // Only the queue window is installed; every add keeps its row.
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW);
    assert_eq!(rows(&pipeline), 124);

    // The admitted jobs' articles are built as their first handout builds
    // them.
    let mut owners: Vec<JobId> = admissions(&pipeline).iter().copied().collect();
    owners.sort_unstable();
    for job_id in owners {
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
        deliver_fetch_failure(&mut pipeline, JobId(id), &error).await;
        apply_intents(&mut pipeline).await;
        assert_eq!(admissions(&pipeline).len(), 10);
        assert!(!dispatch_eligible(&pipeline).contains(&JobId(11)));
    }
    // Empty article queues and stalled downstream processing retain the same
    // owners. Every job in the window is installed, but only an admitted job has had its
    // articles built; the rest hold none until a slot admits them.
    for id in installed(&pipeline) {
        if admitted(&pipeline, id) {
            clear_download_queue(&mut pipeline, id);
        } else {
            assert!(!articles_active(&pipeline, id));
        }
        wait_for_volumes(&mut pipeline, id);
    }
    for _ in 0..24 {
        apply_intents(&mut pipeline).await;
        assert_eq!(admissions(&pipeline).len(), 10);
        assert!(!dispatch_eligible(&pipeline).contains(&JobId(11)));
    }
    assert!(
        queue_infos(&pipeline)
            .iter()
            .filter(|row| row.job_id.0 > 10)
            .all(|info| {
                info.download_state == DownloadState::Queued
                    && info.download_wait_reason.as_deref() == Some("nzb_slot")
            })
    );
}

#[tokio::test]
async fn live_limits_drain_and_manual_pause_releases_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(admissions(&pipeline).len(), 2);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 3).await;
    assert_eq!(admissions(&pipeline).len(), 2);
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
    handle.pause_job(JobId(2)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(3)]));
    handle.resume_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(3)]));
    assert_eq!(
        persisted_admissions(database(&pipeline)),
        *admissions(&pipeline)
    );
}

#[tokio::test]
async fn automatic_pause_and_empty_queues_keep_their_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    queue_handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    pause_runtime(&mut pipeline, JobId(1)).unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(2)));
}

#[tokio::test]
async fn manual_pause_waits_for_work_and_resume_during_drain_keeps_ownership() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    set_active_decode(&mut pipeline, JobId(1), true);
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(persisted_pause_releases(&pipeline).contains(&JobId(1)));
    handle.resume_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!pause_releases(&pipeline).contains(&JobId(1)));
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(!persisted_pause_releases(&pipeline).contains(&JobId(1)));
    set_active_decode(&mut pipeline, JobId(1), false);
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
}

#[tokio::test]
async fn global_pause_keeps_owners_and_blocks_new_admission() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enqueue(&mut pipeline, 1).await;
    let handle = queue_handle(&pipeline);
    handle.pause_all().await.unwrap();
    enqueue(&mut pipeline, 2).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(2)));
    handle.resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1), JobId(2)]));
}

#[tokio::test]
async fn restart_restores_all_owners_before_queue_recovery_even_above_limit() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    queue_handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(12).unwrap())
        .await
        .unwrap();
    for id in 1..=12 {
        enqueue(&mut pipeline, id).await;
    }
    let owners = admissions(&pipeline).clone();
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    assert_eq!(*admissions(&pipeline), owners);
    enqueue(&mut pipeline, 13).await;
    assert_eq!(*admissions(&pipeline), owners);
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(13)));
    assert_eq!(persisted_admissions(database(&pipeline)), owners);
}

#[tokio::test]
async fn legacy_migration_grandfathers_started_work_but_not_paused_or_untouched_jobs() {
    use weaver_server_db::persistence::{
        sql_runtime::{SqlRuntime, SqlTx, StoreDatastore},
        sqlite_writer::new_writer_gate,
    };
    use weaver_server_migrations::schema_migrations::execute_sql;

    let datastore = StoreDatastore::Sqlite {
        pool: sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap(),
        writer_gate: new_writer_gate(None),
    };
    SqlRuntime::run_in_transaction(&datastore, "legacy_nzb_admission", |tx| {
        Box::pin(async move {
            let SqlTx::Sqlite(tx) = tx else {
                unreachable!("fixture uses SQLite");
            };
            execute_sql(
                tx,
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
            .await?;
            execute_sql(
                tx,
                include_str!(
                    "../../../../weaver-server-migrations/src/db/migrations/0073_nzb_admission/sqlite.sql"
                ),
            )
            .await
        })
    })
    .await
    .unwrap();
    let owners: Vec<_> = SqlRuntime::fetch_all(
        datastore.read_exec(),
        "SELECT job_id FROM active_jobs WHERE nzb_admitted = 1 ORDER BY job_id",
        &[],
    )
    .await
    .unwrap()
    .into_iter()
    .map(|row| row.i64("job_id").unwrap())
    .collect();
    assert_eq!(owners, vec![2, 3, 5, 6]);
}

#[tokio::test]
async fn waiting_priority_changes_do_not_evict_an_owner() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
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
            JobUpdate {
                metadata_edits: vec![("priority".into(), Some("high".into()))],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(3)]));
}

#[tokio::test]
async fn restart_between_pause_intent_and_drain_restores_without_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    queue_handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    queue_handle(&pipeline).pause_job(JobId(2)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!admissions(&pipeline).contains(&JobId(2)));
    persist_pause_release(&pipeline, JobId(1));
    retire_pipeline_database(pipeline).await;
    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        queue_handle(&pipeline)
            .restore_job(candidate.request)
            .await
            .unwrap();
    }
    apply_intents(&mut pipeline).await;
    assert!(admissions(&pipeline).is_empty());
    assert!(persisted_admissions(database(&pipeline)).is_empty());
    assert_eq!(row_run_state(&pipeline, JobId(1)), RunState::Paused);
    assert_eq!(row_run_state(&pipeline, JobId(2)), RunState::Paused);
    assert!(!restore_blocked(&pipeline, JobId(1)));
    assert!(!restore_blocked(&pipeline, JobId(2)));
}

// A resume that waits for its slot is stored once, as it is asked for: the
// turns that find it still waiting write nothing, and a restart before the
// slot comes restores the job resumed, waiting for its slot.
#[tokio::test]
async fn a_resume_waiting_for_its_slot_survives_a_restart() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    // Job 1 has bytes in hand, so pausing it keeps it installed.
    set_progress(&mut pipeline, JobId(1), 512);
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
    let writes = runtime_writes();
    handle.resume_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(run_state(&pipeline, JobId(1)), RunState::Paused);
    assert_eq!(runtime_writes() - writes, 1);
    database(&pipeline).flush_write_queue().await.unwrap();

    let mut pipeline = restart_and_restore(&temp, pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(row_run_state(&pipeline, JobId(1)), RunState::Active);
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
}

#[tokio::test]
async fn cancellation_releases_only_after_terminal_persistence_succeeds() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    enqueue(&mut pipeline, 2).await;
    sql(
        database(&pipeline),
        "CREATE TRIGGER reject_nzb_archive BEFORE INSERT ON job_history BEGIN SELECT RAISE(ABORT, 'synthetic archive failure'); END",
    )
    .await;
    handle.cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(2)));
    sql(database(&pipeline), "DROP TRIGGER reject_nzb_archive").await;
    let mut events = queue_handle(&pipeline).subscribe_events();
    handle.cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
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
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
    assert_eq!(
        persisted_admissions(database(&pipeline)),
        *admissions(&pipeline)
    );
}

// The row is the add: the job is installed with its files, its arrival was
// announced and its scripts ran, and none of its articles may go out.
async fn assert_added_but_not_dispatched(pipeline: &mut Pipeline, id: JobId) {
    assert_eq!(installed_spec(pipeline, id).unwrap().files.len(), 1);
    let info = queue_info(pipeline, id);
    assert_eq!(info.total_files, 1);
    assert_eq!(info.total_bytes, 512 * 8);
    assert!(holds_arrival(pipeline, id));
    finish_added_scripts(pipeline, &[id]).await;
    let results = database(pipeline).event_script_results(id.0).unwrap();
    // An added NZB runs its own event's script and the one for a job of
    // either kind.
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(!admissions(pipeline).contains(&id));
    assert!(!dispatch_eligible(pipeline).contains(&id));
}

#[tokio::test]
async fn nzb_added_under_global_pause_is_installed_and_announced_before_resume() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = queue_handle(&pipeline);
    handle.pause_all().await.unwrap();
    enqueue(&mut pipeline, 1).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(1)).await;
    apply_intents(&mut pipeline).await;
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(1)));
    handle.resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(admissions(&pipeline).contains(&JobId(1)));
    assert!(dispatch_eligible(&pipeline).contains(&JobId(1)));
    assert_eq!(
        database(&pipeline).event_script_results(1).unwrap().len(),
        2
    );
}

#[tokio::test]
async fn nzb_added_under_scheduled_pause_is_installed_and_announced_before_resume() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = queue_handle(&pipeline);
    handle
        .apply_schedule_action(ScheduleAction::Pause)
        .await
        .unwrap();
    assert_eq!(hold_origin(&pipeline), Some(HoldOrigin::Scheduled));
    enqueue(&mut pipeline, 1).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(1)).await;
    handle
        .apply_schedule_action(ScheduleAction::Resume)
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    assert!(admissions(&pipeline).contains(&JobId(1)));
    assert!(dispatch_eligible(&pipeline).contains(&JobId(1)));
    assert_eq!(
        database(&pipeline).event_script_results(1).unwrap().len(),
        2
    );
}

#[tokio::test]
async fn shutdown_installs_and_announces_nothing() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    prepare_add(
        &pipeline,
        JobId(1),
        segmented_job_spec("Queued NZB 1", "payload.bin", &[512; 8]),
        PathBuf::from("queued-1.nzb"),
        sample_nzb_zstd(),
    )
    .await;
    mark_shutting_down(&pipeline);
    apply_intents(&mut pipeline).await;
    assert!(!has_installed(&pipeline, JobId(1)));
    assert!(!holds_arrival(&pipeline, JobId(1)));
    assert!(admissions(&pipeline).is_empty());
    database(&pipeline).flush_write_queue().await.unwrap();
    assert_eq!(database(&pipeline).queue_script_count().unwrap(), 0);
}

#[tokio::test]
async fn nzb_added_with_every_slot_taken_is_announced_and_waits_to_dispatch() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    enable_arrival_scripts(&pipeline, temp.path());
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue(&mut pipeline, 1).await;
    finish_added_scripts(&mut pipeline, &[JobId(1)]).await;
    enqueue(&mut pipeline, 2).await;
    assert_added_but_not_dispatched(&mut pipeline, JobId(2)).await;
    apply_intents(&mut pipeline).await;
    assert!(
        queue_info(&pipeline, JobId(2))
            .download_wait_reason
            .as_deref()
            == Some(NZB_SLOT_WAIT_REASON)
    );
    handle.pause_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
    assert!(dispatch_eligible(&pipeline).contains(&JobId(2)));
    assert_eq!(
        database(&pipeline).event_script_results(2).unwrap().len(),
        2
    );
}

// A restored row is installed with the files and sizes of its saved NZB and
// is no longer restoring.
fn assert_restored_installed(pipeline: &Pipeline, id: JobId) {
    let spec = installed_spec(pipeline, id).unwrap();
    assert!(!spec.files.is_empty());
    assert!(spec.total_bytes > 0);
    let restoring = report_is_restoring(pipeline, id);
    let info = queue_info(pipeline, id);
    assert!(!restoring);
    assert!(!restore_blocked(pipeline, id));
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
    queue_handle(&pipeline)
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    let mut pipeline = restart_and_restore(&temp, pipeline).await;
    apply_intents(&mut pipeline).await;
    assert_restored_installed(&pipeline, JobId(1));
    assert_restored_installed(&pipeline, JobId(2));
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    assert!(!dispatch_eligible(&pipeline).contains(&JobId(2)));
    assert!(
        queue_info(&pipeline, JobId(2))
            .download_wait_reason
            .as_deref()
            == Some(NZB_SLOT_WAIT_REASON)
    );

    let mut events = queue_handle(&pipeline).subscribe_events();
    queue_handle(&pipeline).cancel_job(JobId(1)).await.unwrap();
    apply_intents(&mut pipeline).await;
    finish_cancel_jobs(&mut pipeline).await;
    loop {
        if matches!(events.recv().await.unwrap(), PipelineEvent::JobCancelled { job_id } if job_id == JobId(1))
        {
            break;
        }
    }
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(2)]));
    assert!(dispatch_eligible(&pipeline).contains(&JobId(2)));
}

#[tokio::test]
async fn restart_under_global_pause_installs_every_restored_job_and_resume_admits_one() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let before = queue_handle(&pipeline);
    before
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    before.pause_all().await.unwrap();
    enqueue_saved(&mut pipeline, 1).await;
    enqueue_saved(&mut pipeline, 2).await;
    assert!(admissions(&pipeline).is_empty());
    retire_pipeline_database(pipeline).await;
    let (pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let after = queue_handle(&pipeline);
    after
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(1).unwrap())
        .await
        .unwrap();
    after.pause_all().await.unwrap();
    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        after.restore_job(candidate.request).await.unwrap();
    }
    let mut pipeline = pipeline;
    apply_intents(&mut pipeline).await;
    assert_restored_installed(&pipeline, JobId(1));
    assert_restored_installed(&pipeline, JobId(2));
    assert!(admissions(&pipeline).is_empty());
    let eligible = dispatch_eligible(&pipeline);
    assert!(!eligible.contains(&JobId(1)));
    assert!(!eligible.contains(&JobId(2)));
    after.resume_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(*admissions(&pipeline), HashSet::from([JobId(1)]));
    let eligible = dispatch_eligible(&pipeline);
    assert!(eligible.contains(&JobId(1)));
    assert!(!eligible.contains(&JobId(2)));
}

// A job's article lists, file by file, as (ordinal, article number, bytes,
// message ids).
fn article_lists(spec: &JobSpec) -> Vec<Vec<(u32, u32, u32, MessageIds)>> {
    spec.files
        .iter()
        .map(|file| {
            file.segments
                .iter()
                .map(|segment| {
                    (
                        segment.ordinal,
                        segment.article_number,
                        segment.bytes,
                        segment.message_ids.clone(),
                    )
                })
                .collect()
        })
        .collect()
}

// A payload of eight articles with a PAR2 index and a recovery volume, its
// article numbers as its stored NZB gives them.
fn payload_with_recovery(name: &str) -> JobSpec {
    let mut spec = segmented_job_spec(name, "payload.bin", &[512; 8]);
    spec.files.extend(
        standalone_with_par2_job_spec(name, 1, 64)
            .files
            .into_iter()
            .skip(1),
    );
    for segment in spec.files.iter_mut().flat_map(|file| &mut file.segments) {
        segment.article_number = segment.ordinal + 1;
    }
    spec.total_bytes = spec
        .files
        .iter()
        .flat_map(|file| &file.segments)
        .map(|segment| u64::from(segment.bytes))
        .sum();
    spec
}

// An add waiting in a paused queue holds no article list, neither before it
// is installed nor after, and its row and its job say what the lists held,
// as the full lists would.
#[tokio::test]
async fn an_add_waiting_in_a_paused_queue_holds_no_article_list() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    queue_handle(&pipeline).pause_all().await.unwrap();
    let spec = payload_with_recovery("Paused NZB");
    let summaries = FileSummaries(spec.files.iter().map(FileSummary::of).collect());
    prepare_add(
        &pipeline,
        JobId(1),
        spec.clone(),
        PathBuf::from("paused.nzb"),
        nzb_zstd_for(&spec),
    )
    .await;
    let (prepared_spec, prepared_summaries, report) = prepared_add(&pipeline, JobId(1));
    assert_eq!(prepared_spec.files.len(), spec.files.len());
    assert!(
        prepared_spec
            .files
            .iter()
            .all(|file| file.segments.is_empty())
    );
    assert_eq!(prepared_summaries, summaries);
    assert_eq!(report.wanted_bytes, spec.wanted_bytes());
    assert_eq!(report.remaining_bytes, spec.wanted_bytes());
    assert_eq!(report.total_bytes, spec.total_bytes);
    assert_eq!(report.total_files, spec.files.len() as u32);
    apply_intents(&mut pipeline).await;
    assert!(admissions(&pipeline).is_empty());
    let installed = installed_spec(&pipeline, JobId(1)).unwrap();
    assert!(installed.files.iter().all(|file| file.segments.is_empty()));
    assert_eq!(
        queued_summaries(&pipeline, JobId(1)),
        Some(summaries.0.as_slice())
    );
    assert_eq!(counts_par2_bytes(&pipeline, JobId(1)), spec.par2_bytes());
}

// A job whose lists were let go when its NZB was stored fetches, once it is
// handed work, the very articles it was added with, file by file.
#[tokio::test]
async fn a_job_added_without_its_lists_runs_the_articles_it_was_added_with() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let spec = payload_with_recovery("Lean NZB");
    add_lean(
        &mut pipeline,
        JobId(1),
        spec.clone(),
        PathBuf::from("lean.nzb"),
        nzb_zstd_for(&spec),
    )
    .await;
    assert!(admissions(&pipeline).contains(&JobId(1)));
    activate_job(&mut pipeline, JobId(1)).await;
    assert!(articles_active(&pipeline, JobId(1)));
    assert_eq!(
        article_lists(installed_spec(&pipeline, JobId(1)).unwrap()),
        article_lists(&spec)
    );
    assert_eq!(counts_par2_bytes(&pipeline, JobId(1)), spec.par2_bytes());
}
