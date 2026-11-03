// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use super::*;
use crate::jobs::handle::QueueMoveTarget;
use crate::jobs::phase_progress::JobPhase;
use crate::pipeline::nzb_admission::NZB_SLOT_WAIT_REASON;

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
    handle.pause_job(JobId(QUEUED)).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(row_walks() > settled);
    assert_eq!(
        pipeline.jobs[&JobId(QUEUED)].run_state,
        crate::RunState::Paused
    );
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

    pipeline.apply_queue_intents().await;
    for id in (1..=QUEUED).map(JobId) {
        assert_eq!(
            pipeline.jobs[&id].run_state,
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
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.job_order, expected);

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

    let (pipeline, intermediate, _) = new_direct_pipeline(temp).await;
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
    // Recovery parses each stored NZB once.
    assert_eq!(recovery_parses, QUEUED);

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
    assert_eq!(pipeline.jobs.len(), QUEUED as usize);
    assert_eq!(turns, (QUEUED as usize).div_ceil(RESTORE_BATCH));
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
    const WAITING: JobId = JobId(QUEUED);
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
    assert_eq!(pipeline.jobs.len(), QUEUED as usize);
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
            SqlRuntime::execute(datastore.read_exec(), sql, &[]).await?;
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
    handle
        .pause(crate::jobs::Hold {
            origin: crate::jobs::HoldOrigin::SpeedTest,
            scope: crate::jobs::HoldScope::Everything,
            owner: crate::jobs::HoldOwner::All,
        })
        .await
        .unwrap();
    drive.wait_for(|driving| *driving).await.unwrap();
    booked.bytes_downloaded.fetch_add(4_096, Ordering::Relaxed);
    revisions
        .wait_for(|_| published() >= before + 4_096)
        .await
        .unwrap();

    // The last articles land, then the test lets go of the queue.
    booked.bytes_downloaded.fetch_add(1_024, Ordering::Relaxed);
    handle
        .resume(crate::jobs::HoldOrigin::SpeedTest, crate::jobs::HoldOwner::All)
        .await
        .unwrap();
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
