// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Duration;

use weaver_server_pipeline::history::queries::HistoryQueries;
use weaver_server_pipeline::jobs::model::FileSummary;
use weaver_server_pipeline::jobs::persistence::JobsPersistenceQueries;
use weaver_server_pipeline::jobs::queries::JobsQueries;
use weaver_server_pipeline::jobs::{
    ActiveFileIdentity, FileIdentitySource, Hold, HoldOrigin, HoldOwner, HoldScope,
};
use weaver_server_pipeline::operations::recover_server_state;
use weaver_server_pipeline::pipeline::tests::{
    activate_job,
    history_fixture::database,
    new_direct_pipeline,
    queue_fixture::{apply_intents, handle as queue_handle, installed_spec, installed_status},
    queue_scale_fixture::*,
    retire_pipeline_database, settle_failed_releases,
    window_fixture::{
        ActivationReply, QUEUE_WINDOW, admitted, articles_active, has_installed, installed,
        installed_count, parked as is_parked, request_activation, rows, settle as settle_window,
    },
};
use weaver_server_pipeline::{
    JobId, JobSpec, JobStatus, MessageIds, Pipeline, QueueMoveTarget, RunState, SchedulerHandle,
    settings::PerJobSeries,
};

#[tokio::test]
async fn a_job_waiting_for_a_slot_stores_the_reason_the_dispatch_pass_computes() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _handle) = long_queue(&temp, 2).await;
    for row in wait_reasons(&pipeline) {
        assert_eq!(row.stored, row.computed, "job {}", row.id.0);
        if !row.admitted {
            assert_eq!(
                row.stored.as_deref(),
                Some(NZB_SLOT_WAIT_REASON),
                "job {}",
                row.id.0
            );
        }
    }
    // With the stored and computed reasons agreeing, a dispatch pass finds
    // nothing to re-report, so the queue does not change.
    report_changed_dispatch_holds(&mut pipeline);
    let revision = QueueProbe::observe(&pipeline).revision();
    report_changed_dispatch_holds(&mut pipeline);
    report_changed_dispatch_holds(&mut pipeline);
    assert_eq!(QueueProbe::observe(&pipeline).revision(), revision);
}

#[tokio::test]
async fn a_turn_with_no_new_intent_walks_no_queue_row() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let settled = row_walks();
    for _ in 0..3 {
        apply_intents(&mut pipeline).await;
    }
    assert_eq!(row_walks(), settled);

    // An intent is acted on by the next turn's walk, and the turn after it
    // walks nothing again.
    let in_window = JobId(QUEUE_WINDOW as u64 / 2);
    handle.pause_job(in_window).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert!(row_walks() > settled);
    assert_eq!(row_run_state(&pipeline, in_window), RunState::Paused);
    let acted = row_walks();
    apply_intents(&mut pipeline).await;
    assert_eq!(row_walks(), acted);

    // A job past the window is installed to act on its intent, which calls
    // for one more walk; after it, turns walk nothing again.
    handle.pause_job(JobId(QUEUED)).await.unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(row_run_state(&pipeline, JobId(QUEUED)), RunState::Paused);
    assert!(!has_installed(&pipeline, JobId(QUEUED)));
    apply_intents(&mut pipeline).await;
    let acted = row_walks();
    apply_intents(&mut pipeline).await;
    assert_eq!(row_walks(), acted);
}

#[tokio::test]
async fn a_batch_pause_is_one_queue_change_and_pauses_every_job() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let queue = QueueProbe::observe(&pipeline);
    let revision = queue.revision();
    let epoch = queue.intent_epoch();
    let outcomes = handle.set_jobs_paused((1..=QUEUED).map(JobId), true).await;
    assert_eq!(outcomes.len(), QUEUED as usize);
    assert!(outcomes.values().all(Result::is_ok));
    assert_eq!(queue.revision(), revision + 1);
    assert_eq!(queue.intent_epoch(), epoch + 1);

    settle_window(&mut pipeline).await;
    for id in (1..=QUEUED).map(JobId) {
        assert_eq!(
            row_run_state(&pipeline, id),
            RunState::Paused,
            "job {}",
            id.0
        );
    }
    // No paused job waits for a slot.
    for row in wait_reasons(&pipeline) {
        assert_ne!(
            row.stored.as_deref(),
            Some(NZB_SLOT_WAIT_REASON),
            "job {}",
            row.id.0
        );
    }
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
    let positions = QueueProbe::observe(&pipeline).positions();
    let order: Vec<_> = positions.iter().map(|(id, _)| *id).collect();
    assert_eq!(order, expected);
    for (position, (_, indexed)) in positions.into_iter().enumerate() {
        assert_eq!(indexed, Some(position));
    }
    settle_window(&mut pipeline).await;
    // The installed jobs keep the queue's order.
    let installed: Vec<JobId> = expected
        .iter()
        .copied()
        .filter(|id| has_installed(&pipeline, *id))
        .collect();
    assert_eq!(installed_order(&pipeline), installed);

    // The stored positions sort the rows into the same order, which is the
    // order a restart restores.
    database(&pipeline).flush_write_queue().await.unwrap();
    let stored = database(&pipeline)
        .load_active_job_queue_positions()
        .unwrap();
    let mut restored: Vec<JobId> = stored.keys().copied().collect();
    restored.sort_by_key(|id| stored[id]);
    assert_eq!(restored, expected);
}

#[tokio::test]
async fn per_job_metrics_cover_the_jobs_holding_a_slot() {
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, handle) = long_queue(&temp, 2).await;
    let mut exported: Vec<JobId> = handle
        .metrics_jobs(PerJobSeries::Active)
        .into_iter()
        .map(|job| job.job_id)
        .collect();
    exported.sort_unstable();
    let mut admitted: Vec<JobId> = admitted_ids(&pipeline);
    admitted.sort_unstable();
    assert_eq!(exported, admitted);
    assert_eq!(
        handle.metrics_jobs(PerJobSeries::All).len(),
        QUEUED as usize
    );
    assert!(handle.metrics_jobs(PerJobSeries::Off).is_empty());
}

// A position write that does not commit leaves the stored order behind the
// queue's; the next reorder writes what the database lacks, so a restart
// restores the order the queue shows.
#[tokio::test]
async fn a_position_write_that_does_not_commit_is_written_by_the_next_reorder() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    database(&pipeline).flush_write_queue().await.unwrap();
    sql(
        database(&pipeline),
        "CREATE TRIGGER reject_positions BEFORE UPDATE OF queue_position ON active_jobs \
         BEGIN SELECT RAISE(ABORT, 'synthetic position failure'); END",
    )
    .await;
    handle
        .reorder_jobs(vec![(JobId(QUEUED), QueueMoveTarget::Top)])
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    database(&pipeline).flush_write_queue().await.unwrap();
    assert!(positions_lost(&pipeline));
    sql(database(&pipeline), "DROP TRIGGER reject_positions").await;

    handle
        .reorder_jobs(vec![(JobId(1), QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    let expected: Vec<JobId> = QueueProbe::observe(&pipeline).ids();
    assert_eq!(expected.first(), Some(&JobId(QUEUED)));
    assert_eq!(expected.last(), Some(&JobId(1)));
    retire_pipeline_database(pipeline).await;

    let (pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let recovered = recover_server_state(
        database(&pipeline),
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
    let handle = queue_handle(&pipeline);
    sql(
        database(&pipeline),
        "CREATE TRIGGER reject_admission BEFORE UPDATE OF nzb_admitted ON active_jobs \
         WHEN NEW.nzb_admitted = 1 \
         BEGIN SELECT RAISE(ABORT, 'synthetic admission failure'); END",
    )
    .await;
    enqueue(&mut pipeline, 1).await;
    apply_intents(&mut pipeline).await;
    apply_intents(&mut pipeline).await;
    assert!(admitted_ids(&pipeline).is_empty());
    assert!(!pipeline_live(&pipeline));
    // The failed write left a walk due, which is work to drive.
    assert!(has_work_to_drive(&pipeline));

    let queue = QueueProbe::observe(&pipeline);
    let db = database(&pipeline).clone();
    let mut drive = queue.subscribe_drive();
    let mut revisions = queue.subscribe();
    let runner = tokio::spawn(async move { pipeline.run().await });
    drive.wait_for(|driving| *driving).await.unwrap();
    sql(&db, "DROP TRIGGER reject_admission").await;
    let epoch = queue.intent_epoch();
    revisions
        .wait_for(|_| queue.admitted(JobId(1)))
        .await
        .unwrap();
    assert_eq!(queue.intent_epoch(), epoch);
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
    apply_intents(&mut pipeline).await;
    add_expired_hold(&mut pipeline, paused);
    apply_intents(&mut pipeline).await;
    assert!(!has_propagation_hold(&pipeline, paused));
    let settled = row_walks();
    for _ in 0..3 {
        apply_intents(&mut pipeline).await;
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
    let downloading = admitted_ids(&pipeline)[0];
    phase_downloading(&mut pipeline, downloading, 2048);
    let queue = QueueProbe::observe(&pipeline);
    assert!(queue.has_phased_job());
    let metrics = handle.pipeline_metrics();
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
    assert!(queue.has_phased_job());

    let turns = || metrics.pipeline_histograms.turn_duration.snapshot().count;
    let ticks = periodic_ticks;
    let samples = counter_samples;
    let (turns_parked, ticks_parked, samples_parked) = (turns(), ticks(), samples());
    let (walks_parked, revision_parked) = (row_walks(), queue.revision());
    for round in 1..=5u64 {
        // Past the stall check's period, the longest of any driver, so an
        // armed one would fire.
        tokio::time::advance(STALL_CHECK_INTERVAL + Duration::from_secs(1)).await;
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
    let handle = queue_handle(&pipeline);
    let queue = QueueProbe::observe(&pipeline);
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
        .pause(Hold {
            origin: HoldOrigin::SpeedTest,
            scope: HoldScope::Everything,
            owner: HoldOwner::All,
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
        .resume(HoldOrigin::SpeedTest, HoldOwner::All)
        .await
        .unwrap();
    drive.wait_for(|driving| !driving).await.unwrap();
    revisions
        .wait_for(|_| published() == before + 5_120)
        .await
        .unwrap();
    handle.pipeline_diagnostics().await.unwrap();

    let samples = counter_samples;
    let samples_parked = samples();
    for round in 1..=3 {
        tokio::time::advance(STALL_CHECK_INTERVAL + Duration::from_secs(1)).await;
        handle.pipeline_diagnostics().await.unwrap();
        assert_eq!(samples(), samples_parked, "round {round}");
        assert!(!queue.driving(), "round {round}");
    }
    assert_eq!(published(), before + 5_120);

    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

#[tokio::test]
async fn a_slot_is_taken_while_the_database_is_busy_and_held_once_it_commits() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(3).unwrap())
        .await
        .unwrap();
    let release = hold_writer(database(&pipeline)).await;
    assert!(turn_completes(&mut pipeline));
    // The slot's write is out: it counts as held, and the job holds it
    // once the write commits.
    assert_eq!(admitted_ids(&pipeline).len(), 2);
    assert_eq!(admitting_ids(&pipeline).len(), 1);
    let taking = admitting_ids(&pipeline)[0];
    assert!(turn_completes(&mut pipeline));
    assert_eq!(admitting_ids(&pipeline).len(), 1);
    release.send(()).unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(admitted_ids(&pipeline).len(), 3);
    assert!(admitted(&pipeline, taking));
    assert!(admitting_ids(&pipeline).is_empty());
}

#[tokio::test]
async fn a_paused_job_lets_its_slot_go_while_the_database_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let paused = admitted_ids(&pipeline).into_iter().min().unwrap();
    handle.pause_job(paused).await.unwrap();
    let release = hold_writer(database(&pipeline)).await;
    // The pause, its marker, and the release of its slot each go on the
    // writer without the turn waiting for it.
    assert!(turn_completes(&mut pipeline));
    assert!(turn_completes(&mut pipeline));
    assert!(releasing(&pipeline, paused));
    assert!(admitted(&pipeline, paused));
    release.send(()).unwrap();
    apply_intents(&mut pipeline).await;
    assert!(!admitted(&pipeline, paused));
    assert_eq!(admitted_ids(&pipeline).len(), 2);
}

#[tokio::test]
async fn a_job_enters_the_window_while_the_database_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    let entering = JobId(QUEUED);
    assert!(is_parked(&pipeline, entering));
    handle
        .reorder_jobs(vec![(entering, QueueMoveTarget::Top)])
        .await
        .unwrap();
    let release = hold_writer(database(&pipeline)).await;
    // Its read back waits behind the job's writes, off the turn.
    assert!(turn_completes(&mut pipeline));
    assert!(installing_ids(&pipeline).contains(&entering));
    assert!(!has_installed(&pipeline, entering));
    release.send(()).unwrap();
    apply_intents(&mut pipeline).await;
    assert!(has_installed(&pipeline, entering));
    assert!(!is_parked(&pipeline, entering));
    assert!(installing_ids(&pipeline).is_empty());
}

#[tokio::test]
async fn a_reorder_after_a_lost_position_write_waits_on_no_read() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    // The stored positions are unknown once a write has not committed.
    mark_positions_lost(&pipeline);
    handle
        .reorder_jobs(vec![(JobId(2), QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    let release = hold_writer(database(&pipeline)).await;
    assert!(turn_completes(&mut pipeline));
    release.send(()).unwrap();
    apply_intents(&mut pipeline).await;
    database(&pipeline).flush_write_queue().await.unwrap();
    let stored = database(&pipeline)
        .load_active_job_queue_positions()
        .unwrap();
    let order = QueueProbe::observe(&pipeline).ids();
    let mut by_position = order.clone();
    by_position.sort_by_key(|id| stored[id]);
    assert_eq!(by_position, order);
}

// A long queue restarted, its restores queued and none installed. One of
// the two jobs holding a slot was moved to the bottom first, so it sits
// last. Returns the restart's order and each job's summaries before it.
struct Restarted {
    pipeline: Pipeline,
    handle: SchedulerHandle,
    order: Vec<JobId>,
    summaries: HashMap<JobId, Vec<FileSummary>>,
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
    assert!(admitted(&pipeline, LATE_HOLDER));
    handle
        .reorder_jobs(vec![(LATE_HOLDER, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    apply_intents(&mut pipeline).await;
    let summaries = installed_summaries(&pipeline);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(temp).await;
    // No list is read ahead, so every parse counted is the restart's.
    disable_read_ahead(&mut pipeline);
    if unoutlined {
        sql(database(&pipeline), "DELETE FROM active_job_outlines").await;
        sql(
            database(&pipeline),
            "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
        )
        .await;
    }
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(2).unwrap())
        .await
        .unwrap();
    let parses = nzb_parses();
    let recovered = recover_server_state(
        database(&pipeline),
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
    let writes = runtime_writes();
    let parses = nzb_parses();
    apply_intents(&mut pipeline).await;
    assert_eq!(installed_count(&pipeline), RESTORE_BATCH);
    assert!(restore_waiting(&pipeline));
    let mut first: Vec<JobId> = admitted_ids(&pipeline);
    first.sort_unstable();
    first.extend(
        order
            .iter()
            .filter(|id| !admitted(&pipeline, **id))
            .take(RESTORE_BATCH - first.len()),
    );
    assert!(first.contains(&LATE_HOLDER));
    assert!(first.iter().all(|id| has_installed(&pipeline, *id)));
    let mut turns = 1;
    while restore_waiting(&pipeline) {
        apply_intents(&mut pipeline).await;
        turns += 1;
    }
    // Only the window is installed: the first rows in queue order, and the
    // holder moved to the bottom. Every row stays.
    let window = QUEUE_WINDOW + 1;
    assert_eq!(installed_count(&pipeline), window);
    assert!(has_installed(&pipeline, LATE_HOLDER));
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| has_installed(&pipeline, *id))
    );
    assert_eq!(rows(&pipeline), QUEUED as usize);
    assert_eq!(turns, window.div_ceil(RESTORE_BATCH));
    // A restore that changes nothing in its row writes nothing back.
    assert_eq!(runtime_writes(), writes);
    // No job here ever started, so each waits its turn holding the
    // summaries its lists gave, and the install parses no NZB: a job's
    // lists are read when it is handed work.
    assert_eq!(nzb_parses(), parses);
    for id in installed(&pipeline) {
        match queued_summaries(&pipeline, id) {
            Some(summaries) => assert_eq!(summaries, expected[&id], "job {}", id.0),
            None => assert!(admitted(&pipeline, id), "job {}", id.0),
        }
    }
    apply_intents(&mut pipeline).await;
    assert_eq!(admitted_ids(&pipeline).len(), 2);
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
    database(&pipeline)
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
    let handle = queue_handle(&pipeline);
    let recovered = recover_server_state(
        database(&pipeline),
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
    let identity = |job_id: JobId| ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some(format!("written-after-recovery-{}.bin", job_id.0)),
        classification: None,
        classification_source: FileIdentitySource::Declared,
    };
    for job_id in [DOWNLOADING, WAITING] {
        database(&pipeline)
            .save_file_identities(job_id, &[identity(job_id)])
            .unwrap();
    }

    apply_intents(&mut pipeline).await;
    assert!(restore_waiting(&pipeline));
    assert!(restore_pass_active(&pipeline));
    while restore_waiting(&pipeline) {
        apply_intents(&mut pipeline).await;
    }
    assert!(!restore_pass_active(&pipeline));
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW);
    assert_eq!(
        installed_status(&pipeline, DOWNLOADING),
        Some(&JobStatus::Downloading)
    );
    for job_id in [DOWNLOADING, WAITING] {
        assert_eq!(
            file_identity(&pipeline, job_id, 0),
            Some(&identity(job_id)),
            "job {}",
            job_id.0
        );
    }
    // Later turns start no second pass.
    apply_intents(&mut pipeline).await;
    assert!(!restore_pass_active(&pipeline));
}

// A file-row read that fails at install holds only that job, paused and
// blocked; resuming it reads again and installs it with its rows.
#[tokio::test]
async fn a_failed_file_row_read_blocks_the_job_and_resume_reads_again() {
    const DOWNLOADING: JobId = JobId(1);
    let temp = tempfile::tempdir().unwrap();
    let (pipeline, _handle) = long_queue(&temp, 2).await;
    database(&pipeline)
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
    let identity = ActiveFileIdentity {
        file_index: 0,
        source_filename: "payload.bin".to_string(),
        current_filename: "payload.bin".to_string(),
        canonical_filename: Some("kept-across-the-block.bin".to_string()),
        classification: None,
        classification_source: FileIdentitySource::Declared,
    };
    database(&pipeline)
        .save_file_identities(DOWNLOADING, std::slice::from_ref(&identity))
        .unwrap();
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    let recovered = recover_server_state(
        database(&pipeline),
        &temp.path().join("data"),
        &intermediate,
    )
    .await
    .unwrap();
    for candidate in recovered.to_restore {
        handle.restore_job(candidate.request).await.unwrap();
    }
    sql_blocking(
        database(&pipeline),
        "ALTER TABLE active_detected_archives RENAME TO hidden_detected_archives",
    );
    apply_intents(&mut pipeline).await;
    while restore_waiting(&pipeline) {
        apply_intents(&mut pipeline).await;
    }
    assert!(restore_blocked(&pipeline, DOWNLOADING));
    assert_eq!(
        installed_status(&pipeline, DOWNLOADING),
        Some(&JobStatus::Paused)
    );
    let stored = database(&pipeline).load_active_job_rows().unwrap();
    assert_eq!(stored[&DOWNLOADING].status, "paused");

    sql_blocking(
        database(&pipeline),
        "ALTER TABLE hidden_detected_archives RENAME TO active_detected_archives",
    );
    resume_restored_job(&mut pipeline, DOWNLOADING)
        .await
        .unwrap();
    assert!(!restore_blocked(&pipeline, DOWNLOADING));
    assert_eq!(file_identity(&pipeline, DOWNLOADING, 0), Some(&identity));
}

// Between a restart's batches the actor still hears what is ready: a decode
// that completes while restores wait is heard before they are done.
#[tokio::test]
async fn a_decode_done_during_a_restart_is_heard_between_batches() {
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle,
        ..
    } = restarted_long_queue(&temp).await;
    enqueue_failed_decode(&pipeline, JobId(QUEUED)).await;
    let queue = QueueProbe::observe(&pipeline);
    let mut revisions = queue.subscribe();
    let heard = decodes_heard_while_restoring();
    let runner = tokio::spawn(async move { pipeline.run().await });
    revisions.wait_for(|_| queue.restored()).await.unwrap();
    assert_eq!(decodes_heard_while_restoring(), heard + 1);
    handle.shutdown().await.unwrap();
    runner.await.unwrap();
}

#[tokio::test]
async fn a_restart_installs_its_restores_while_the_database_is_busy() {
    let temp = tempfile::tempdir().unwrap();
    let Restarted { mut pipeline, .. } = restarted_long_queue(&temp).await;
    let release = hold_writer(database(&pipeline)).await;
    assert!(turn_completes(&mut pipeline));
    assert_eq!(installing_ids(&pipeline).len(), RESTORE_BATCH);
    assert!(installed_count(&pipeline) == 0);
    release.send(()).unwrap();
    apply_intents(&mut pipeline).await;
    assert_eq!(installed_count(&pipeline), RESTORE_BATCH);
}

// The jobs that take the next slots, past the ones holding them, have their
// lists read ahead, and a job that takes a slot keeps its lists until it
// activates. A job that drops out of them, as one paused does, drops its
// lists, and the next job in order has its own read. A job that takes a slot
// activates on the lists read ahead, the same lists a read at activation
// gives, with no parse of its own.
#[tokio::test]
async fn the_jobs_next_up_hold_their_lists_read_ahead() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, handle) = long_queue(&temp, 2).await;
    settle_next_up(&mut pipeline).await;
    // Jobs 1 and 2 hold the slots and have not activated.
    let next: Vec<JobId> = (1..3 + NEXT_UP_DEPTH as u64).map(JobId).collect();
    assert_eq!(held_ahead(&pipeline), next);

    let paused = JobId(5);
    handle.pause_job(paused).await.unwrap();
    apply_intents(&mut pipeline).await;
    settle_next_up(&mut pipeline).await;
    let next: Vec<JobId> = (1..4 + NEXT_UP_DEPTH as u64)
        .map(JobId)
        .filter(|id| *id != paused)
        .collect();
    assert_eq!(held_ahead(&pipeline), next);

    // A job holding a slot activates on the lists read ahead.
    let job_id = JobId(1);
    let fresh = stored_spec(&pipeline, job_id);
    let parses = nzb_parses();
    request_activation(&mut pipeline, job_id);
    let done = ActivationReply::receive(&mut pipeline).await;
    done.deliver(&mut pipeline);
    assert_eq!(nzb_parses(), parses);
    assert!(articles_active(&pipeline, job_id));
    let lists = |spec: &JobSpec| -> Vec<Vec<(u32, u32, u32, MessageIds)>> {
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
    assert_eq!(
        lists(installed_spec(&pipeline, job_id).unwrap()),
        lists(&fresh)
    );
    assert!(!held_ahead(&pipeline).contains(&job_id));
}

// A job stored before outlines were restores with no NZB read before the
// server answers. Each such job reads its NZB once, wherever it stands, and
// stores the outline it reads: a job in the window installs, and one out of
// it reads its outline alone and waits as its row, which states its files.
// Each job then holds the summaries it held before the restart, and the next
// restart reads no NZB.
#[tokio::test]
async fn a_restart_fills_the_outlines_of_jobs_stored_without_one() {
    let temp = tempfile::tempdir().unwrap();
    let Restarted {
        mut pipeline,
        handle,
        order,
        summaries: expected,
        recovery_parses,
    } = restarted_long_queue_with(&temp, true).await;
    assert_eq!(recovery_parses, 0);
    let parses = nzb_parses();
    let outline_only = outline_only_reads();
    settle_window(&mut pipeline).await;
    assert_eq!(nzb_parses() - parses, QUEUED);
    assert_eq!(installed_count(&pipeline), QUEUE_WINDOW + 1);
    // Only the window and the late slot holder were installed.
    assert_eq!(
        outline_only_reads() - outline_only,
        QUEUED - (QUEUE_WINDOW as u64 + 1)
    );
    let parked = order[QUEUE_WINDOW];
    assert!(is_parked(&pipeline, parked));
    assert!(
        order[..QUEUE_WINDOW]
            .iter()
            .all(|id| has_installed(&pipeline, *id))
    );
    for id in installed(&pipeline) {
        match queued_summaries(&pipeline, id) {
            Some(summaries) => assert_eq!(summaries, expected[&id], "job {}", id.0),
            None => assert!(admitted(&pipeline, id), "job {}", id.0),
        }
    }
    database(&pipeline).flush_write_queue().await.unwrap();
    let outlines = outlines(database(&pipeline));
    assert_eq!(outlines.len(), QUEUED as usize);
    // A job that waits as its row states the files its outline read.
    let row = handle.get_job(parked).unwrap();
    assert_eq!(row.total_files as usize, outlines[&parked].files.len());
    assert_eq!(
        row.total_bytes,
        outlines[&parked]
            .files
            .iter()
            .map(|file| file.bytes)
            .sum::<u64>()
    );
    assert!(row.total_bytes > 0);
    // Its wanted and remaining bytes are what the job states once it
    // installs.
    let parked_figures = (row.wanted_bytes, row.remaining_bytes);
    assert!(parked_figures.0 > 0);
    handle
        .reorder_jobs(vec![(parked, QueueMoveTarget::Top)])
        .await
        .unwrap();
    settle_window(&mut pipeline).await;
    assert!(has_installed(&pipeline, parked));
    let installed = handle.get_job(parked).unwrap();
    assert_eq!(
        (installed.wanted_bytes, installed.remaining_bytes),
        parked_figures
    );
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
    let handle = queue_handle(&pipeline);
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    for id in 1..=3 {
        enqueue(&mut pipeline, id).await;
    }
    apply_intents(&mut pipeline).await;
    assert_eq!(admitted_ids(&pipeline).len(), 3);
    retire_pipeline_database(pipeline).await;

    let (mut pipeline, intermediate, _) = new_direct_pipeline(&temp).await;
    disable_read_ahead(&mut pipeline);
    sql(database(&pipeline), "DELETE FROM active_job_outlines").await;
    sql(
        database(&pipeline),
        "UPDATE active_jobs SET nzb_title = NULL, nzb_password = NULL, nzb_files = NULL",
    )
    .await;
    sql(
        database(&pipeline),
        "UPDATE job_nzbs SET nzb_zstd = x'' WHERE job_id = 1",
    )
    .await;
    sql(
        database(&pipeline),
        "UPDATE job_nzbs SET nzb_zstd = x'01020304' WHERE job_id = 2",
    )
    .await;
    let handle = queue_handle(&pipeline);
    handle.set_max_concurrent_nzbs(slots).await.unwrap();
    let parses = nzb_parses();
    let recovered = recover_server_state(
        database(&pipeline),
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
    database(&pipeline).flush_write_queue().await.unwrap();
    let queue = QueueProbe::observe(&pipeline);
    for id in failed {
        assert!(!queue.contains(id), "job {}", id.0);
    }
    apply_intents(&mut pipeline).await;
    database(&pipeline).flush_write_queue().await.unwrap();

    let active = database(&pipeline).load_active_jobs().unwrap();
    let stored_admissions = persisted_admissions(database(&pipeline));
    for id in failed {
        let history = database(&pipeline)
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
        assert!(!stored_admissions.contains(&id), "job {}", id.0);
        assert!(!admitted(&pipeline, id), "job {}", id.0);
        assert!(!has_installed(&pipeline, id), "job {}", id.0);
    }
    let readable = JobId(3);
    assert!(active.contains_key(&readable));
    assert!(
        database(&pipeline)
            .get_job_history(readable.0)
            .unwrap()
            .is_none()
    );
    assert!(
        !installed_spec(&pipeline, readable)
            .unwrap()
            .files
            .is_empty()
    );
    assert!(failure_error(&pipeline, readable).is_none());
    assert!(!restore_blocked(&pipeline, readable));
}

// On a long queue, the turn that hears a download result and the turn that
// refills a lane visit no queue row and ask the database nothing: neither
// grows with the queue.
#[tokio::test]
async fn a_result_turn_and_a_refill_turn_walk_no_row_and_ask_no_database_on_a_long_queue() {
    const LONG: u64 = 2_500;
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = queue_handle(&pipeline);
    handle
        .set_max_concurrent_nzbs(std::num::NonZeroU32::new(14).unwrap())
        .await
        .unwrap();
    for id in 1..=LONG {
        enqueue(&mut pipeline, id).await;
    }
    settle_window(&mut pipeline).await;
    assert_eq!(admitted_ids(&pipeline).len(), 14);
    let job_id = admitted_ids(&pipeline).into_iter().min().unwrap();
    activate_job(&mut pipeline, job_id).await;
    let segment_id = hold_first_lane_work(&mut pipeline, job_id);

    let walks = row_walks();
    let awaited = actor_awaited_db_calls();
    let called = actor_thread_db_calls();
    hear_closed_lane_and_refill(&mut pipeline, job_id, segment_id).await;
    assert_eq!(row_walks(), walks);
    assert_eq!(actor_awaited_db_calls(), awaited);
    assert_eq!(actor_thread_db_calls(), called);
}

// A long queue on a working disk whose free space the test sets, held to a
// reserve. What each pass does is counted, never timed, so the counts hold
// at any length: the same test at ten thousand jobs passes too.
mod disk_room {
    use super::*;

    const LONG: u64 = 2_500;
    const SLOTS: u32 = 4;

    // Counted on every thread but a sampler's.
    fn disk_looks() -> u64 {
        weaver_server_disk::operations::disk::DISK_LOOKS.load(Ordering::Relaxed)
    }

    fn refresh_asks() -> u64 {
        weaver_server_disk::operations::disk::REFRESH_ASKS.load(Ordering::Relaxed)
    }

    fn visits() -> u64 {
        admission_visits()
    }

    fn restated(before: &[Option<String>], after: &[Option<String>]) -> usize {
        before.iter().zip(after).filter(|(a, b)| a != b).count()
    }

    fn waiting_for_disk(pipeline: &Pipeline) -> usize {
        reported_wait_reasons(pipeline)
            .iter()
            .filter(|reason| {
                reason.as_deref()
                    == Some(weaver_server_pipeline::jobs::handle::DISK_SPACE_WAIT_REASON)
            })
            .count()
    }

    // A burst of adds at the end of the queue and moves among the jobs out
    // of the window, as a client sends them.
    async fn burst(pipeline: &mut Pipeline, handle: &SchedulerHandle, first: u64) {
        for id in first..first + 500 {
            enqueue(pipeline, id).await;
        }
        let moves: Vec<_> = (0..100)
            .map(|step| (JobId(LONG - step), QueueMoveTarget::Offset(-3)))
            .collect();
        handle.reorder_jobs(moves).await.unwrap();
        apply_intents(pipeline).await;
    }

    // With the disk not short and then short, adds and moves of a long
    // queue ask the disk nothing on the pipeline's thread and
    // ring its sampler at most once, the walk looks at no more candidates
    // than the slots call for, and a hold that begins or ends restates no
    // more rows than there are slots.
    #[tokio::test]
    async fn a_long_queue_reads_the_disk_from_the_cache_short_or_not() {
        let temp = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
        let disk = ScaleDisk::under(&mut pipeline);
        let handle = queue_handle(&pipeline);
        handle
            .set_max_concurrent_nzbs(std::num::NonZeroU32::new(SLOTS).unwrap())
            .await
            .unwrap();
        for id in 1..=LONG {
            enqueue(&mut pipeline, id).await;
        }
        settle_window(&mut pipeline).await;
        activations_heard(&mut pipeline).await;
        assert_eq!(admitted_ids(&pipeline).len(), SLOTS as usize);

        // Not short.
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        burst(&mut pipeline, &handle, LONG + 1).await;
        assert_eq!(disk_looks(), looks, "nothing asked of the disk itself");
        assert_eq!(refresh_asks(), asks, "nor of its sampler");
        let walked = row_walks() - walks;
        assert!(walked > 0);
        assert!(
            visits() - visited <= walked,
            "each walk looked at one candidate, the slots being full"
        );
        assert!(!admission_disk_held(&pipeline));
        assert_eq!(waiting_for_disk(&pipeline), 0);

        // The disk goes short, and a slot comes free: the walk judges the
        // disk once, shows the next job waiting for it, and starts none.
        disk.reads(&pipeline, 0);
        let before = reported_wait_reasons(&pipeline);
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        handle
            .set_max_concurrent_nzbs(std::num::NonZeroU32::new(SLOTS + 1).unwrap())
            .await
            .unwrap();
        apply_intents(&mut pipeline).await;
        assert!(admission_disk_held(&pipeline), "the hold began");
        assert_eq!(
            admitted_ids(&pipeline).len(),
            SLOTS as usize,
            "no job started"
        );
        assert_eq!(disk_unadmitted(&pipeline), 1, "one free slot, one shown");
        assert_eq!(waiting_for_disk(&pipeline), 1);
        assert!(restated(&before, &reported_wait_reasons(&pipeline)) <= 1);
        assert_eq!(disk_looks(), looks);
        assert_eq!(refresh_asks() - asks, 1, "the hold asked one early reading");
        let walked = row_walks() - walks;
        assert!(visits() - visited <= walked * u64::from(SLOTS + 1));

        // Short: a burst asks the disk nothing, rings no sampler again, and
        // each walk looks at no more candidates than there are slots.
        let (looks, asks, walks, visited) = (disk_looks(), refresh_asks(), row_walks(), visits());
        burst(&mut pipeline, &handle, LONG + 501).await;
        assert_eq!(disk_looks(), looks);
        assert_eq!(refresh_asks(), asks, "the hold is rung once, not per job");
        let walked = row_walks() - walks;
        assert!(walked > 0);
        assert!(visits() - visited <= walked * u64::from(SLOTS + 1));
        assert!(disk_unadmitted(&pipeline) <= SLOTS as usize + 1);
        assert_eq!(admitted_ids(&pipeline).len(), SLOTS as usize);

        // A reading with room ends the hold: the job shown waiting starts,
        // and no more rows are restated than there are slots.
        disk.reads(&pipeline, 64 * DISK_RESERVE);
        let before = reported_wait_reasons(&pipeline);
        let looks = disk_looks();
        apply_disk_reading(&mut pipeline);
        assert_eq!(disk_looks(), looks, "the hold ends on the cached reading");
        apply_intents(&mut pipeline).await;
        assert!(!admission_disk_held(&pipeline));
        assert_eq!(waiting_for_disk(&pipeline), 0);
        assert!(restated(&before, &reported_wait_reasons(&pipeline)) <= SLOTS as usize + 1);
        // The job that starts reads its articles off the pipeline's
        // thread: that asks the disk nothing either, its staging device
        // and the kind of storage it logs included, each read from the
        // disk owner's cache.
        assert_eq!(admitted_ids(&pipeline).len(), SLOTS as usize + 1);
        activations_heard(&mut pipeline).await;
        assert_eq!(disk_looks(), looks);
        assert_eq!(
            disk.actor_probes(),
            0,
            "only the sampler's own thread probed"
        );
    }
}
