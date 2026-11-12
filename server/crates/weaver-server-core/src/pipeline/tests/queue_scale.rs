// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use super::*;
use crate::jobs::handle::QueueMoveTarget;
use crate::jobs::phase_progress::JobPhase;
use crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON;
use crate::pipeline::queue_window::QUEUE_WINDOW;

const QUEUED: u64 = 200;

async fn enqueue(pipeline: &mut Pipeline, id: u64) {
    let spec = segmented_job_spec(&format!("Queued NZB {id}"), "payload.bin", &[512; 4]);
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

// A pipeline holding `QUEUED` jobs with `slots` of them admitted.
async fn long_queue(temp: &tempfile::TempDir, slots: u32) -> (Pipeline, SchedulerHandle) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(slots).unwrap())
        .await
        .unwrap();
    for id in 1..=QUEUED {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), slots as usize);
    (pipeline, handle)
}

fn row_walks() -> u64 {
    crate::pipeline::queue::ROW_WALKS.load(Ordering::Relaxed)
}

// Turns until the queue window has nothing left to install.
async fn settle_window(pipeline: &mut Pipeline) {
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
}

fn row_run_state(pipeline: &Pipeline, id: JobId) -> crate::RunState {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(id)
            .map(|row| row.info(&inner.globals).run_state)
            .unwrap()
    })
}

#[tokio::test]
async fn a_job_waiting_for_a_slot_stores_the_reason_the_dispatch_pass_computes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline.shared_state.queue.read(|inner| {
        for row in &inner.jobs {
            let stored = row.report.job.download_wait_reason.as_deref();
            assert_eq!(stored, pipeline.stored_wait_reason(row), "job {}", row.id.0);
            if !pipeline.nzb_admitted.contains(&row.id) {
                assert_eq!(stored, Some(NZB_SLOT_WAIT_REASON), "job {}", row.id.0);
            }
        }
    });
    // With the stored and computed reasons agreeing, a dispatch pass finds
    // nothing to re-report, so the queue does not change.
    pipeline.report_changed_dispatch_holds();
    let revision = pipeline.shared_state.queue.revision();
    pipeline.report_changed_dispatch_holds();
    pipeline.report_changed_dispatch_holds();
    assert_eq!(pipeline.shared_state.queue.revision(), revision);
}

#[tokio::test]
async fn a_turn_with_no_new_intent_walks_no_queue_row() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let settled = row_walks();
    for _ in 0..3 {
        pipeline.apply_queue_intents().await;
    }
    assert_eq!(row_walks(), settled);

    // An intent is acted on by the next turn's walk, and the turn after it
    // walks nothing again.
    let in_window = JobId(QUEUE_WINDOW as u64 / 2);
    handle.pause_job(in_window).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(row_walks() > settled);
    assert_eq!(pipeline.jobs[&in_window].run_state, crate::RunState::Paused);
    let acted = row_walks();
    pipeline.apply_queue_intents().await;
    assert_eq!(row_walks(), acted);

    // A job past the window is installed to act on its intent, which calls
    // for one more walk; after it, turns walk nothing again.
    handle.pause_job(JobId(QUEUED)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        row_run_state(&pipeline, JobId(QUEUED)),
        crate::RunState::Paused
    );
    assert!(!pipeline.jobs.contains_key(&JobId(QUEUED)));
    pipeline.apply_queue_intents().await;
    let acted = row_walks();
    pipeline.apply_queue_intents().await;
    assert_eq!(row_walks(), acted);
}

#[tokio::test]
async fn a_batch_pause_is_one_queue_change_and_pauses_every_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let queue = pipeline.shared_state.queue.clone();
    let revision = queue.revision();
    let epoch = queue.read(|inner| inner.intent_epoch());
    let outcomes = handle.set_jobs_paused((1..=QUEUED).map(JobId), true).await;
    assert_eq!(outcomes.len(), QUEUED as usize);
    assert!(outcomes.values().all(Result::is_ok));
    assert_eq!(queue.revision(), revision + 1);
    assert_eq!(queue.read(|inner| inner.intent_epoch()), epoch + 1);

    settle_window(&mut pipeline).await;
    for id in (1..=QUEUED).map(JobId) {
        assert_eq!(
            row_run_state(&pipeline, id),
            crate::RunState::Paused,
            "job {}",
            id.0
        );
    }
    // No paused job waits for a slot.
    queue.read(|inner| {
        for row in &inner.jobs {
            assert_ne!(
                row.report.job.download_wait_reason.as_deref(),
                Some(NZB_SLOT_WAIT_REASON),
                "job {}",
                row.id.0
            );
        }
    });
}

#[tokio::test]
async fn a_reorder_keeps_every_row_where_its_index_says() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    handle
        .reorder_jobs(vec![
            (JobId(QUEUED), QueueMoveTarget::Top),
            (JobId(1), QueueMoveTarget::Bottom),
            (JobId(50), QueueMoveTarget::Offset(-10)),
        ])
        .await
        .unwrap();
    let mut expected: Vec<JobId> = (2..QUEUED).map(JobId).collect();
    expected.insert(0, JobId(QUEUED));
    let fifty = expected.iter().position(|id| *id == JobId(50)).unwrap();
    expected.remove(fifty);
    expected.insert(fifty - 10, JobId(50));
    expected.push(JobId(1));
    pipeline.shared_state.queue.read(|inner| {
        let order: Vec<JobId> = inner.jobs.iter().map(|row| row.id).collect();
        assert_eq!(order, expected);
        for (position, id) in order.iter().enumerate() {
            assert_eq!(inner.job_position(*id), Some(position));
        }
    });
    settle_window(&mut pipeline).await;
    // The installed jobs keep the queue's order.
    let installed: Vec<JobId> = expected
        .iter()
        .copied()
        .filter(|id| pipeline.jobs.contains_key(id))
        .collect();
    assert_eq!(pipeline.job_order, installed);

    // The stored positions sort the rows into the same order, which is the
    // order a restart restores.
    pipeline.db.flush_write_queue().await.unwrap();
    let stored = pipeline.db.load_active_job_queue_positions().unwrap();
    let mut restored: Vec<JobId> = stored.keys().copied().collect();
    restored.sort_by_key(|id| stored[id]);
    assert_eq!(restored, expected);
}

#[tokio::test]
async fn per_job_metrics_cover_the_jobs_holding_a_slot() {
    use crate::settings::PerJobSeries;
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    let mut exported: Vec<JobId> = pipeline
        .shared_state
        .metrics_jobs(PerJobSeries::Active)
        .into_iter()
        .map(|job| job.job_id)
        .collect();
    exported.sort_unstable();
    let mut admitted: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    admitted.sort_unstable();
    assert_eq!(exported, admitted);
    assert_eq!(
        pipeline.shared_state.metrics_jobs(PerJobSeries::All).len(),
        QUEUED as usize
    );
    assert!(
        pipeline
            .shared_state
            .metrics_jobs(PerJobSeries::Off)
            .is_empty()
    );
}

fn nzb_parses() -> u64 {
    crate::ingest::NZB_PARSES.load(Ordering::Relaxed)
}

async fn sql(db: &crate::Database, statement: &'static str) {
    let datastore = db.datastore();
    crate::persistence::sql_runtime::SqlRuntime::run_in_transaction(&datastore, "test_sql", |tx| {
        Box::pin(async move {
            tx.execute(statement, &[]).await?;
            Ok(())
        })
    })
    .await
    .unwrap();
}

// A long queue restarted, its restores queued and none installed. One of
// the two jobs holding a slot was moved to the bottom first, so it sits
// last. Returns the restart's order and each job's summaries before it.
struct Restarted {
    pipeline: Pipeline,
    handle: SchedulerHandle,
    order: Vec<JobId>,
    summaries: HashMap<JobId, Vec<crate::jobs::model::FileSummary>>,
    recovery_parses: u64,
}

const LATE_HOLDER: JobId = JobId(1);

async fn restarted_long_queue(temp: &tempfile::TempDir) -> Restarted {
    restarted_long_queue_with(temp, false).await
}

// As `restarted_long_queue`; with `unoutlined`, every job is stored as one
// admitted before outlines were, with no outline.
async fn restarted_long_queue_with(temp: &tempfile::TempDir, unoutlined: bool) -> Restarted {
    let (mut pipeline, handle) = long_queue(temp, 2).await;
    assert!(pipeline.nzb_admitted.contains(&LATE_HOLDER));
    handle
        .reorder_jobs(vec![(LATE_HOLDER, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    let summaries = pipeline
        .jobs
        .iter()
        .map(|(id, state)| {
            let summaries = match &state.articles {
                crate::jobs::model::JobArticles::Queued(summaries) => summaries.0.clone(),
                _ => state
                    .spec
                    .files
                    .iter()
                    .map(crate::jobs::model::FileSummary::of)
                    .collect(),
            };
            (*id, summaries)
        })
        .collect();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(temp).await;
    // No list is read ahead, so every parse counted is the restart's.
    pipeline.next_up.depth = 0;
    if unoutlined {
        sql(&pipeline.db, "DELETE FROM active_job_outlines").await;
        sql(
            &pipeline.db,
            "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
        )
        .await;
    }
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let parses = nzb_parses();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    let recovery_parses = nzb_parses() - parses;
    assert_eq!(recovered.to_restore.len(), QUEUED as usize);
    // Recovery holds each job's files, never its article lists.
    for candidate in &recovered.to_restore {
        assert!(
            candidate
                .request
                .spec
                .files
                .iter()
                .all(|file| file.segments.is_empty()),
            "job {}",
            candidate.job_id.0
        );
    }
    let order: Vec<JobId> = recovered.to_restore.iter().map(|c| c.job_id).collect();
    assert_eq!(order.last(), Some(&LATE_HOLDER));
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    Restarted {
        pipeline,
        handle,
        order,
        summaries,
        recovery_parses,
    }
}

#[tokio::test]
async fn a_restart_restores_a_long_queue_a_batch_a_turn_holding_no_article_lists() {
    use crate::pipeline::completion::finalize::RUNTIME_WRITES;
    use crate::pipeline::orchestrator::RESTORE_BATCH;
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle: _handle,
        order,
        summaries: expected,
        recovery_parses,
    } = restarted_long_queue(&temp).await;
    // Recovery reads each job's stored outline and parses no stored NZB.
    assert_eq!(recovery_parses, 0);

    // Each turn installs one batch: the jobs holding a slot first, wherever
    // they stand, then the rest in queue order. It says more wait.
    let writes = RUNTIME_WRITES.load(Ordering::Relaxed);
    let parses = nzb_parses();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs.len(), RESTORE_BATCH);
    assert!(pipeline.restores_waiting);
    let mut first: Vec<JobId> = pipeline.nzb_admitted.iter().copied().collect();
    first.sort_unstable();
    first.extend(
        order
            .iter()
            .filter(|id| !pipeline.nzb_admitted.contains(id))
            .take(RESTORE_BATCH - first.len()),
    );
    assert!(first.contains(&LATE_HOLDER));
    assert!(first.iter().all(|id| pipeline.jobs.contains_key(id)));
    let mut turns = 1;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
        turns += 1;
    }
    // Only the window is installed: the first rows in queue order, and the
    // holder moved to the bottom. Every row stays.
    let window = QUEUE_WINDOW + 1;
    assert_eq!(pipeline.jobs.len(), window);
    assert!(pipeline.jobs.contains_key(&LATE_HOLDER));
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| pipeline.jobs.contains_key(id))
    );
    assert_eq!(
        pipeline.shared_state.queue.read(|inner| inner.jobs.len()),
        QUEUED as usize
    );
    assert_eq!(turns, window.div_ceil(RESTORE_BATCH));
    // A restore that changes nothing in its row writes nothing back.
    assert_eq!(RUNTIME_WRITES.load(Ordering::Relaxed), writes);
    // No job here ever started, so each waits its turn holding the
    // summaries its lists gave, and the install parses no NZB: a job's
    // lists are read when it is handed work.
    assert_eq!(nzb_parses(), parses);
    for (id, state) in &pipeline.jobs {
        match &state.articles {
            crate::jobs::model::JobArticles::Queued(summaries) => {
                assert_eq!(summaries.0, expected[id], "job {}", id.0)
            }
            _ => assert!(pipeline.nzb_admitted.contains(id), "job {}", id.0),
        }
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 2);
}

// A restart reads each job's file rows as it installs the job, not before
// the server answers: a row written after recovery is the one installed,
// for a job that was downloading and for one waiting its turn alike. The
// pass ends once, when the last restore is in.
#[tokio::test]
async fn a_restart_reads_each_jobs_file_rows_when_it_installs_it() {
    const DOWNLOADING: JobId = JobId(1);
    const WAITING: JobId = JobId(QUEUE_WINDOW as u64);
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline
        .db
        .set_active_job_runtime(
            DOWNLOADING,
            "downloading",
            Some("downloading"),
            Some("idle"),
            Some("active"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    assert_eq!(recovered.to_restore.len(), QUEUED as usize);
    assert!(
        recovered
            .to_restore
            .iter()
            .all(|candidate| candidate.request.file_identities.is_empty())
    );
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    let identity = |job_id: JobId| crate::jobs::ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some(format!("written-after-recovery-{}.bin", job_id.0)),
        classification: None,
        classification_source: crate::jobs::FileIdentitySource::Declared,
    };
    for job_id in [DOWNLOADING, WAITING] {
        pipeline
            .db
            .save_file_identities(job_id, &[identity(job_id)])
            .unwrap();
    }

    pipeline.apply_queue_intents().await;
    assert!(pipeline.restores_waiting);
    assert!(pipeline.restore_pass.is_some());
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    assert!(pipeline.restore_pass.is_none());
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW);
    assert_eq!(pipeline.jobs[&DOWNLOADING].status, JobStatus::Downloading);
    for job_id in [DOWNLOADING, WAITING] {
        assert_eq!(
            pipeline.jobs[&job_id].file_identities.get(&0),
            Some(&identity(job_id)),
            "job {}",
            job_id.0
        );
    }
    // Later turns start no second pass.
    pipeline.apply_queue_intents().await;
    assert!(pipeline.restore_pass.is_none());
}

// A file-row read that fails at install holds only that job, paused and
// blocked; resuming it reads again and installs it with its rows.
#[tokio::test]
async fn a_failed_file_row_read_blocks_the_job_and_resume_reads_again() {
    const DOWNLOADING: JobId = JobId(1);
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    pipeline
        .db
        .set_active_job_runtime(
            DOWNLOADING,
            "downloading",
            Some("downloading"),
            Some("idle"),
            Some("active"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
    let identity = crate::jobs::ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some("kept-across-the-block.bin".to_string()),
        classification: None,
        classification_source: crate::jobs::FileIdentitySource::Declared,
    };
    pipeline
        .db
        .save_file_identities(DOWNLOADING, std::slice::from_ref(&identity))
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    let rename = |db: &Database, sql: &'static str| {
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(datastore.write_exec(), sql, &[]).await?;
            Ok(())
        })
        .unwrap();
    };
    rename(
        &pipeline.db,
        "ALTER TABLE active_detected_archives RENAME TO hidden_detected_archives",
    );
    pipeline.apply_queue_intents().await;
    while pipeline.restores_waiting {
        pipeline.apply_queue_intents().await;
    }
    assert!(pipeline.blocked_restores.contains_key(&DOWNLOADING));
    assert_eq!(pipeline.jobs[&DOWNLOADING].status, JobStatus::Paused);
    let stored = pipeline.db.load_active_job_rows().unwrap();
    assert_eq!(stored[&DOWNLOADING].status, "paused");

    rename(
        &pipeline.db,
        "ALTER TABLE hidden_detected_archives RENAME TO active_detected_archives",
    );
    pipeline.resume_restored_job(DOWNLOADING).await.unwrap();
    assert!(!pipeline.blocked_restores.contains_key(&DOWNLOADING));
    assert_eq!(
        pipeline.jobs[&DOWNLOADING].file_identities.get(&0),
        Some(&identity)
    );
}

// Between a restart's batches the actor still hears what is ready: a decode
// that completes while restores wait is heard before they are done.
#[tokio::test]
async fn a_decode_done_during_a_restart_is_heard_between_batches() {
    use crate::pipeline::orchestrator::DECODES_HEARD_RESTORING;
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle,
        ..
    } = restarted_long_queue(&temp).await;
    pipeline
        .decode_done_tx
        .send(DecodeDone::Failed {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(QUEUED),
                    file_index: 0,
                },
                segment_number: 0,
            },
            raw_size: 0,
            error: "synthetic decode failure".to_string(),
            source_server_idx: None,
            exclude_servers: Vec::new(),
        })
        .await
        .unwrap();
    let queue = pipeline.shared_state.queue.clone();
    let mut revisions = queue.subscribe();
    let heard = DECODES_HEARD_RESTORING.load(Ordering::Relaxed);
    let runner = tokio::spawn(async move { pipeline.run().await });
    revisions
        .wait_for(|_| {
            queue.read(|inner| {
                inner
                    .jobs
                    .iter()
                    .all(|row| row.intents.restore.is_none() && !row.report.restoring)
            })
        })
        .await
        .unwrap();
    assert_eq!(DECODES_HEARD_RESTORING.load(Ordering::Relaxed), heard + 1);
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// A position write that does not commit leaves the stored order behind the
// queue's; the next reorder writes what the database lacks, so a restart
// restores the order the queue shows.
#[tokio::test]
async fn a_position_write_that_does_not_commit_is_written_by_the_next_reorder() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    pipeline.db.flush_write_queue().await.unwrap();
    sql(
        &pipeline.db,
        "CREATE TRIGGER reject_positions BEFORE UPDATE OF queue_position ON active_jobs \
         BEGIN SELECT RAISE(ABORT, 'synthetic position failure'); END",
    )
    .await;
    handle
        .reorder_jobs(vec![(JobId(QUEUED), QueueMoveTarget::Top)])
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.db.flush_write_queue().await.unwrap();
    assert!(pipeline.queue_positions_lost.load(Ordering::Acquire));
    sql(&pipeline.db, "DROP TRIGGER reject_positions").await;

    handle
        .reorder_jobs(vec![(JobId(1), QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    let expected: Vec<JobId> = pipeline
        .shared_state
        .queue
        .read(|inner| inner.jobs.iter().map(|row| row.id).collect());
    assert_eq!(expected.first(), Some(&JobId(QUEUED)));
    assert_eq!(expected.last(), Some(&JobId(1)));
    retire_pipeline_database(pipeline).await;

    let (pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    let restored: Vec<JobId> = recovered.to_restore.iter().map(|c| c.job_id).collect();
    assert_eq!(restored, expected);
}

// A slot write that fails on a queue with nothing else to do is tried again
// by the actor's own tick, with no intent to wake it.
#[tokio::test(start_paused = true)]
async fn a_failed_slot_write_on_an_idle_queue_is_retried_with_no_intent() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    sql(
        &pipeline.db,
        "CREATE TRIGGER reject_admission BEFORE UPDATE OF nzb_admitted ON active_jobs \
         WHEN NEW.nzb_admitted = 1 \
         BEGIN SELECT RAISE(ABORT, 'synthetic admission failure'); END",
    )
    .await;
    enqueue(&mut pipeline, 1).await;
    pipeline.apply_queue_intents().await;
    pipeline.apply_queue_intents().await;
    assert!(pipeline.nzb_admitted.is_empty());
    assert!(!pipeline.pipeline_is_live());
    // The failed write left a walk due, which is work to drive.
    assert!(pipeline.has_work_to_drive());

    let queue = pipeline.shared_state.queue.clone();
    let db = pipeline.db.clone();
    let mut drive = queue.subscribe_drive();
    let mut revisions = queue.subscribe();
    let runner = tokio::spawn(async move { pipeline.run().await });
    drive.wait_for(|driving| *driving).await.unwrap();
    sql(&db, "DROP TRIGGER reject_admission").await;
    let epoch = queue.read(|inner| inner.intent_epoch());
    revisions
        .wait_for(|_| {
            queue.read(|inner| {
                inner
                    .job(JobId(1))
                    .is_some_and(|row| row.report.nzb_admitted)
            })
        })
        .await
        .unwrap();
    assert_eq!(queue.read(|inner| inner.intent_epoch()), epoch);
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// An expired propagation hold on a job that is not a candidate is dropped
// by the walk that sees it, so the turns after it walk nothing.
#[tokio::test]
async fn an_expired_hold_on_a_paused_job_stops_calling_for_walks() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let paused = JobId(QUEUED);
    handle.pause_job(paused).await.unwrap();
    pipeline.apply_queue_intents().await;
    pipeline
        .propagation_ready_at
        .insert(paused, (std::time::Instant::now(), 0));
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.propagation_ready_at.contains_key(&paused));
    let settled = row_walks();
    for _ in 0..3 {
        pipeline.apply_queue_intents().await;
    }
    assert_eq!(row_walks(), settled);
}

// A queue held by a global pause has nothing to drive, even with a job
// caught mid-download: the pause leaves its download phase standing, but no
// byte moves. Once the pause is applied, the actor's periodic ticks and the
// counters ticker stay parked, so every turn after it is one an event asked
// for, however many ticks' worth of time passes, and only that event's own
// answer changes the queue. A resume wakes them again.
#[tokio::test(start_paused = true)]
async fn a_paused_queue_takes_no_tick_and_no_sample_between_events() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let downloading = *pipeline.nzb_admitted.iter().next().unwrap();
    pipeline.active_download_passes.insert(downloading);
    pipeline.phase_begin(downloading, JobPhase::Downloading, Some(2048));
    let queue = pipeline.shared_state.queue.clone();
    assert!(queue.read(|inner| inner.phased_jobs().next().is_some()));
    let metrics = pipeline.metrics.clone();
    let mut drive = queue.subscribe_drive();
    let runner = tokio::spawn(async move { pipeline.run().await });
    // Two jobs hold a slot and nothing holds the queue: there is work to
    // drive.
    drive.wait_for(|driving| *driving).await.unwrap();

    handle.pause_all().await.unwrap();
    drive.wait_for(|driving| !driving).await.unwrap();
    // One turn after parking, so whatever the parking turn set off is heard
    // before counting starts. A second measures what such an event turn
    // walks of its own accord.
    handle.pipeline_diagnostics().await.unwrap();
    let walks_before = row_walks();
    handle.pipeline_diagnostics().await.unwrap();
    let walks_per_event = row_walks() - walks_before;
    // The job is still mid-download.
    assert!(queue.read(|inner| inner.phased_jobs().next().is_some()));

    let turns = || metrics.pipeline_histograms.turn_duration.snapshot().count;
    let ticks = || crate::pipeline::orchestrator::PERIODIC_TICKS.load(Ordering::Relaxed);
    let samples = || crate::pipeline::queue::COUNTER_SAMPLES.load(Ordering::Relaxed);
    let (turns_parked, ticks_parked, samples_parked) = (turns(), ticks(), samples());
    let (walks_parked, revision_parked) = (row_walks(), queue.revision());
    for round in 1..=5u64 {
        // Past the stall check's period, the longest of any driver, so an
        // armed one would fire.
        tokio::time::advance(STALLED_DOWNLOAD_CHECK_INTERVAL + Duration::from_secs(1)).await;
        // One event, one turn, and nothing else.
        handle.pipeline_diagnostics().await.unwrap();
        assert_eq!(turns(), turns_parked + round, "round {round}");
        assert_eq!(ticks(), ticks_parked, "round {round}");
        assert_eq!(samples(), samples_parked, "round {round}");
        assert_eq!(
            row_walks(),
            walks_parked + round * walks_per_event,
            "round {round}"
        );
        // The answer to the request is the only change.
        assert_eq!(queue.revision(), revision_parked + round, "round {round}");
        assert!(!queue.driving(), "round {round}");
    }

    handle.resume_all().await.unwrap();
    drive.wait_for(|driving| *driving).await.unwrap();
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// A speed test books its bytes outside the pipeline while it holds the
// queue. That hold is work to drive, so the counters ticker publishes the
// bytes to the metrics snapshot even with the queue paused. Once the test
// lets go, the ticker publishes the bytes booked since its last tick and
// parks again.
#[tokio::test(start_paused = true)]
async fn a_speed_test_on_a_paused_queue_publishes_its_bytes_then_parks() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    let queue = pipeline.shared_state.queue.clone();
    let mut drive = queue.subscribe_drive();
    let mut revisions = queue.subscribe();
    let runner = tokio::spawn(async move { pipeline.run().await });
    handle.pause_all().await.unwrap();
    handle.pipeline_diagnostics().await.unwrap();
    assert!(!queue.driving());

    let booked = handle.pipeline_metrics();
    let published = || handle.get_metrics().bytes_downloaded;
    let before = published();
    handle.set_speed_test_pause(true).await.unwrap();
    drive.wait_for(|driving| *driving).await.unwrap();
    booked.bytes_downloaded.fetch_add(4_096, Ordering::Relaxed);
    revisions
        .wait_for(|_| published() >= before + 4_096)
        .await
        .unwrap();

    // The last articles land, then the test lets go of the queue.
    booked.bytes_downloaded.fetch_add(1_024, Ordering::Relaxed);
    handle.set_speed_test_pause(false).await.unwrap();
    drive.wait_for(|driving| !driving).await.unwrap();
    revisions
        .wait_for(|_| published() == before + 5_120)
        .await
        .unwrap();
    handle.pipeline_diagnostics().await.unwrap();

    let samples = || crate::pipeline::queue::COUNTER_SAMPLES.load(Ordering::Relaxed);
    let samples_parked = samples();
    for round in 1..=3 {
        tokio::time::advance(STALLED_DOWNLOAD_CHECK_INTERVAL + Duration::from_secs(1)).await;
        handle.pipeline_diagnostics().await.unwrap();
        assert_eq!(samples(), samples_parked, "round {round}");
        assert!(!queue.driving(), "round {round}");
    }
    assert_eq!(published(), before + 5_120);

    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

// The database's one writer held busy by a write that waits for the test,
// as a slow disk holds it. Sending on the returned channel lets it go.
async fn hold_writer(db: &crate::Database) -> std::sync::mpsc::Sender<()> {
    let (release, gate) = std::sync::mpsc::channel::<()>();
    let (held, holding) = tokio::sync::oneshot::channel();
    db.try_queue_write("test_hold_writer", move |_| {
        let _ = held.send(());
        let _ = gate.recv();
        Ok(())
    })
    .unwrap();
    holding.await.unwrap();
    release
}

// Whether one turn of the queue's intents runs to its end on its first
// poll: a turn that waited on the database would not.
fn turn_completes(pipeline: &mut Pipeline) -> bool {
    use std::future::Future;
    let mut turn = std::pin::pin!(pipeline.run_queue_intents());
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    turn.as_mut().poll(&mut cx).is_ready()
}

#[tokio::test]
async fn a_slot_is_taken_while_the_database_is_busy_and_held_once_it_commits() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(3).unwrap())
        .await
        .unwrap();
    let release = hold_writer(&pipeline.db).await;
    assert!(turn_completes(&mut pipeline));
    // The slot's write is out: it counts as held, and the job holds it
    // once the write commits.
    assert_eq!(pipeline.nzb_admitted.len(), 2);
    assert_eq!(pipeline.nzb_admitting.len(), 1);
    let taking = *pipeline.nzb_admitting.iter().next().unwrap();
    assert!(turn_completes(&mut pipeline));
    assert_eq!(pipeline.nzb_admitting.len(), 1);
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 3);
    assert!(pipeline.nzb_admitted.contains(&taking));
    assert!(pipeline.nzb_admitting.is_empty());
}

#[tokio::test]
async fn a_paused_job_lets_its_slot_go_while_the_database_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let paused = *pipeline.nzb_admitted.iter().min().unwrap();
    handle.pause_job(paused).await.unwrap();
    let release = hold_writer(&pipeline.db).await;
    // The pause, its marker, and the release of its slot each go on the
    // writer without the turn waiting for it.
    assert!(turn_completes(&mut pipeline));
    assert!(turn_completes(&mut pipeline));
    assert!(pipeline.nzb_releasing.contains(&paused));
    assert!(pipeline.nzb_admitted.contains(&paused));
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.nzb_admitted.contains(&paused));
    assert_eq!(pipeline.nzb_admitted.len(), 2);
}

#[tokio::test]
async fn a_job_enters_the_window_while_the_database_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let entering = JobId(QUEUED);
    assert!(pipeline.window_parked(entering));
    handle
        .reorder_jobs(vec![(entering, QueueMoveTarget::Top)])
        .await
        .unwrap();
    let release = hold_writer(&pipeline.db).await;
    // Its read back waits behind the job's writes, off the turn.
    assert!(turn_completes(&mut pipeline));
    assert!(pipeline.installing.contains(&entering));
    assert!(!pipeline.jobs.contains_key(&entering));
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.jobs.contains_key(&entering));
    assert!(!pipeline.window_parked(entering));
    assert!(pipeline.installing.is_empty());
}

#[tokio::test]
async fn a_restart_installs_its_restores_while_the_database_is_busy() {
    use crate::pipeline::orchestrator::RESTORE_BATCH;
    let temp = tempfile::tempdir().unwrap();
    let Restarted { mut pipeline, .. } = restarted_long_queue(&temp).await;
    let release = hold_writer(&pipeline.db).await;
    assert!(turn_completes(&mut pipeline));
    assert_eq!(pipeline.installing.len(), RESTORE_BATCH);
    assert!(pipeline.jobs.is_empty());
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.jobs.len(), RESTORE_BATCH);
}

#[tokio::test]
async fn a_reorder_after_a_lost_position_write_waits_on_no_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    // The stored positions are unknown once a write has not committed.
    pipeline
        .queue_positions_lost
        .store(true, std::sync::atomic::Ordering::Release);
    handle
        .reorder_jobs(vec![(JobId(2), QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    let release = hold_writer(&pipeline.db).await;
    assert!(turn_completes(&mut pipeline));
    release.send(()).unwrap();
    pipeline.apply_queue_intents().await;
    pipeline.db.flush_write_queue().await.unwrap();
    let stored = pipeline.db.load_active_job_queue_positions().unwrap();
    let order = pipeline
        .shared_state
        .queue
        .read(|inner| inner.jobs.iter().map(|row| row.id).collect::<Vec<_>>());
    let mut by_position = order.clone();
    by_position.sort_by_key(|id| stored[id]);
    assert_eq!(by_position, order);
}

// On a long queue, the turn that hears a download result and the turn that
// refills a lane visit no queue row and ask the database nothing: neither
// grows with the queue.
#[tokio::test]
async fn a_result_turn_and_a_refill_turn_walk_no_row_and_ask_no_database_on_a_long_queue() {
    use crate::persistence::connection::actor_thread;
    use crate::pipeline::orchestrator::actor_db;
    const LONG: u64 = 2_500;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(14).unwrap())
        .await
        .unwrap();
    for id in 1..=LONG {
        enqueue(&mut pipeline, id).await;
    }
    settle_window(&mut pipeline).await;
    assert_eq!(pipeline.nzb_admitted.len(), 14);
    let job_id = *pipeline.nzb_admitted.iter().min().unwrap();
    activate_job(&mut pipeline, job_id).await;
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
    let work = pipeline.dispatched(work);
    let segment_id = work.segment_id;
    pipeline.hold_test_lane_works(vec![work], false);

    let walks = row_walks();
    let awaited = actor_db::total();
    let called = actor_thread::total();
    let (response_tx, _response_rx) = tokio::sync::oneshot::channel();
    actor_thread::OnActor(Box::pin(async {
        pipeline
            .handle_download_done(DownloadResult {
                lane_id: 0,
                job_id,
                runtime_generation: 0,
                segment_id,
                data: Err(DownloadError::Fetch(
                    DownloadFailure::from_lane_acquire_failure(Some(
                        &weaver_nntp::NntpError::ConnectionClosed,
                    )),
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
        pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
            lane_id: 0,
            runtime_generation: 0,
            server_idx: 0,
            supports_pipelining: false,
            current_mode: DownloadLaneMode::Sequential,
            response_tx,
        });
    }))
    .await;
    assert_eq!(row_walks(), walks);
    assert_eq!(actor_db::total(), awaited);
    assert_eq!(actor_thread::total(), called);
}

// Hear the database's answers until no list read ahead is out.
async fn settle_next_up(pipeline: &mut Pipeline) {
    while pipeline.next_up.reading() {
        let reply = pipeline.db_reply_rx.recv().await.expect("database replies");
        pipeline.handle_db_reply(reply).await;
    }
}

// The jobs that take the next slots, past the ones holding them, have their
// lists read ahead, and a job that takes a slot keeps its lists until it
// activates. A job that drops out of them, as one paused does, drops its
// lists, and the next job in order has its own read. A job that takes a slot
// activates on the lists read ahead, the same lists a read at activation
// gives, with no parse of its own.
#[tokio::test]
async fn the_jobs_next_up_hold_their_lists_read_ahead() {
    use crate::pipeline::next_up::NEXT_UP_DEPTH;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    settle_next_up(&mut pipeline).await;
    // Jobs 1 and 2 hold the slots and have not activated.
    let next: Vec<JobId> = (1..3 + NEXT_UP_DEPTH as u64).map(JobId).collect();
    assert_eq!(pipeline.next_up.held(), next);

    let paused = JobId(5);
    handle.pause_job(paused).await.unwrap();
    pipeline.apply_queue_intents().await;
    settle_next_up(&mut pipeline).await;
    let next: Vec<JobId> = (1..4 + NEXT_UP_DEPTH as u64)
        .map(JobId)
        .filter(|id| *id != paused)
        .collect();
    assert_eq!(pipeline.next_up.held(), next);

    // A job holding a slot activates on the lists read ahead.
    let job_id = JobId(1);
    let (category, metadata) = {
        let spec = &pipeline.jobs[&job_id].spec;
        (spec.category.clone(), spec.metadata.clone())
    };
    let fresh =
        crate::pipeline::activation::read_stored_spec(&pipeline.db, job_id, category, metadata)
            .unwrap();
    let parses = nzb_parses();
    pipeline.request_job_activation(job_id);
    let done = pipeline.job_activated_rx.recv().await.unwrap();
    pipeline.handle_job_activated(done);
    assert_eq!(nzb_parses(), parses);
    let state = &pipeline.jobs[&job_id];
    assert!(matches!(
        state.articles,
        crate::jobs::model::JobArticles::Active(_)
    ));
    let lists = |spec: &JobSpec| -> Vec<Vec<(u32, u32, u32, crate::jobs::MessageIds)>> {
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
    };
    assert_eq!(lists(&state.spec), lists(&fresh));
    assert!(!pipeline.next_up.held().contains(&job_id));
}

// A job stored before outlines were restores with no NZB read before the
// server answers. Each such job installs, wherever it stands, reading its NZB
// once and storing the outline it reads; the jobs out of the window leave it
// again. Each job then holds the summaries it held before the restart, and
// the next restart reads no NZB.
#[tokio::test]
async fn a_restart_fills_the_outlines_of_jobs_stored_without_one() {
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle: _handle,
        order,
        summaries: expected,
        recovery_parses,
    } = restarted_long_queue_with(&temp, true).await;
    assert_eq!(recovery_parses, 0);
    let parses = nzb_parses();
    settle_window(&mut pipeline).await;
    assert_eq!(nzb_parses() - parses, QUEUED);
    assert_eq!(pipeline.jobs.len(), QUEUE_WINDOW + 1);
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| pipeline.jobs.contains_key(id))
    );
    for (id, state) in &pipeline.jobs {
        match &state.articles {
            crate::jobs::model::JobArticles::Queued(summaries) => {
                assert_eq!(summaries.0, expected[id], "job {}", id.0)
            }
            _ => assert!(pipeline.nzb_admitted.contains(id), "job {}", id.0),
        }
    }
    pipeline.db.flush_write_queue().await.unwrap();
    let outlines = pipeline.db.load_job_outlines(None).unwrap();
    assert_eq!(outlines.len(), QUEUED as usize);
    // Every job installed before the restart holds the same files.
    for (id, summaries) in &expected {
        let files: Vec<(u32, u64)> = outlines[id]
            .files
            .iter()
            .map(|file| (file.articles, file.bytes))
            .collect();
        let summaries: Vec<(u32, u64)> = summaries
            .iter()
            .map(|file| (file.articles, file.bytes))
            .collect();
        assert_eq!(files, summaries, "job {}", id.0);
    }
}

// A job stored before outlines were whose stored NZB is gone, or does not
// parse, restores with no NZB read before the server answers, and fails as
// it installs: it goes to history with the reason, and its active rows and
// its slot go. A readable job stored the same way installs beside it.
#[tokio::test]
async fn a_restart_fails_the_jobs_stored_without_an_outline_whose_nzb_cannot_be_read() {
    let temp = tempfile::tempdir().unwrap();
    let slots = std::num::NonZeroU32::new(3).unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    for id in 1..=3 {
        enqueue(&mut pipeline, id).await;
    }
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.nzb_admitted.len(), 3);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    pipeline.next_up.depth = 0;
    sql(&pipeline.db, "DELETE FROM active_job_outlines").await;
    sql(
        &pipeline.db,
        "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
    )
    .await;
    sql(
        &pipeline.db,
        "UPDATE job_nzbs SET nzb_zstd = x'' WHERE job_id = 1",
    )
    .await;
    sql(
        &pipeline.db,
        "UPDATE job_nzbs SET nzb_zstd = x'01020304' WHERE job_id = 2",
    )
    .await;
    let handle = SchedulerHandle::new(pipeline.event_tx.clone(), pipeline.shared_state.clone());
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    let parses = nzb_parses();
    let recovered = crate::operations::recover_server_state(
        &pipeline.db,
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    assert_eq!(nzb_parses(), parses);
    assert_eq!(recovered.to_restore.len(), 3);
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    settle_window(&mut pipeline).await;
    settle_failed_releases(&mut pipeline).await;
    // Each failed job's archive lands with the job's queued writes, and its
    // row leaves the queue then; the turn after lets its slot go, and hears
    // the slot's write.
    let failed = [JobId(1), JobId(2)];
    pipeline.db.flush_write_queue().await.unwrap();
    pipeline.shared_state.queue.read(|inner| {
        for id in failed {
            assert!(!inner.contains_job(id), "job {}", id.0);
        }
    });
    pipeline.apply_queue_intents().await;
    pipeline.db.flush_write_queue().await.unwrap();

    let active = pipeline.db.load_active_jobs().unwrap();
    let admitted = pipeline.db.load_nzb_admissions().unwrap();
    for id in failed {
        let history = pipeline
            .db
            .get_job_history(id.0)
            .unwrap()
            .expect("history row");
        assert_eq!(history.status, "failed", "job {}", id.0);
        assert!(
            history
                .error_message
                .as_deref()
                .is_some_and(|error| error.starts_with("Cannot recover persisted NZB: ")),
            "job {}: {:?}",
            id.0,
            history.error_message
        );
        assert!(!active.contains_key(&id), "job {}", id.0);
        assert!(!admitted.contains(&id), "job {}", id.0);
        assert!(!pipeline.nzb_admitted.contains(&id), "job {}", id.0);
        assert!(!pipeline.jobs.contains_key(&id), "job {}", id.0);
    }
    let readable = JobId(3);
    assert!(active.contains_key(&readable));
    assert!(pipeline.db.get_job_history(readable.0).unwrap().is_none());
    let state = &pipeline.jobs[&readable];
    assert!(!state.spec.files.is_empty());
    assert!(state.failure_error.is_none());
    assert!(!pipeline.blocked_restores.contains_key(&readable));
}
