// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A queue many times longer than its slots. Each test pins what a pass
// visits or writes, never how long it takes.

use std::sync::atomic::Ordering;
use std::time::Duration;

use weaver_server_pipeline::jobs::persistence::JobsPersistenceQueries;
use weaver_server_pipeline::jobs::{Hold, HoldOrigin, HoldOwner, HoldScope};
use weaver_server_pipeline::operations::recover_server_state;
use weaver_server_pipeline::pipeline::tests::{
    history_fixture::database,
    new_direct_pipeline,
    queue_fixture::{apply_intents, handle as queue_handle},
    queue_scale_fixture::*,
    retire_pipeline_database,
    window_fixture::{QUEUE_WINDOW, admitted, has_installed, parked, settle as settle_window},
};
use weaver_server_pipeline::{JobId, QueueMoveTarget, RunState, settings::PerJobSeries};

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
    assert!(parked(&pipeline, entering));
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
    assert!(!parked(&pipeline, entering));
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
