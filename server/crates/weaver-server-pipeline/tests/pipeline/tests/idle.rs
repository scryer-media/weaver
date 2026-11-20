// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Exercise idle control-plane behavior through published events and metrics.

use std::sync::atomic::Ordering;
use weaver_server_base::events::model::PipelineEvent;
use weaver_server_composition::{JobId, JobStatus};
use weaver_server_pipeline::pipeline::tests::{
    decode_fixture::refresh_snapshot,
    idle_fixture::*,
    insert_active_job, new_direct_pipeline,
    queue_fixture::{apply_intents, handle as scheduler, metrics, report_all},
    queue_scale_fixture::pipeline_live,
    standalone_job_spec,
};

#[tokio::test(start_paused = true)]
async fn idle_pipeline_preserves_paused_jobs_and_history_when_global_intents_change() {
    let pipeline = idle_pipeline(64, long_history(), |_| {}).await;
    let before = pipeline.handle.list_jobs();
    let mut events = pipeline.handle.subscribe_events();
    let mut revisions = pipeline.handle.subscribe_job_changes();
    revisions.mark_unchanged();
    pipeline.handle.pause_all().await.unwrap();
    revisions.changed().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    let after = pipeline.handle.list_jobs();
    assert_eq!(after.len(), before.len());
    for id in 1..=64 {
        assert_eq!(
            pipeline.shared_state.get_job(JobId(id)).unwrap().status,
            JobStatus::Paused
        );
    }
    pipeline.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn idle_pipeline_records_production_turn_histogram() {
    let pipeline = idle_pipeline(0, Vec::new(), |_| {}).await;
    let shared = pipeline.shared_state.clone();
    let mut events = pipeline.handle.subscribe_events();
    pipeline.handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    pipeline.shutdown().await;
    assert!(
        shared
            .metrics()
            .pipeline_histograms
            .turn_duration
            .snapshot()
            .count
            > 0
    );
}

// The loop stamps its turns where the handle reads them: once the pipeline
// has stopped, turns have completed, none is in progress, and the last one
// to close counted the jobs it had installed.
#[tokio::test(start_paused = true)]
async fn idle_pipeline_stamps_its_turns_where_the_handle_reads_them() {
    use weaver_server_base::operations::instrumentation::PipelinePhase;

    let pipeline = idle_pipeline(2, Vec::new(), |_| {}).await;
    let handle = pipeline.handle.clone();
    let mut events = handle.subscribe_events();
    handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}
    pipeline.shutdown().await;

    let (queue, turn) = handle.pipeline_unanswered_state(std::time::Instant::now());
    assert_eq!(turn.phase, PipelinePhase::BetweenTurns);
    assert_eq!(turn.busy_for, None);
    assert!(turn.turns_completed > 0);
    assert!(turn.since_last_turn_ended.is_some());
    assert_eq!(turn.installed_jobs, Some(2));
    assert_eq!(queue.jobs, 2);
    assert_eq!(queue.jobs_by_status.count("paused"), 2.0);
    assert!(queue.global_paused);
}

// Graph mail that ends the wait (a run or repair reporting back) makes the
// turn busy under its own phase, as every other event does.
#[tokio::test(start_paused = true)]
async fn a_turn_woken_by_graph_mail_is_stamped_busy_with_graph_run() {
    use weaver_server_base::operations::instrumentation::PipelinePhase;

    let (pipeline, mail, mut stamps) = with_mail().await;
    let mut events = pipeline.handle.subscribe_events();
    pipeline.handle.pause_all().await.unwrap();
    while !matches!(events.recv().await.unwrap(), PipelineEvent::GlobalPaused) {}

    // Mail that is already waiting when a turn opens is heard before the
    // wait; only mail arriving during the wait reaches the select arm.
    let turn = &pipeline.shared_state.metrics().pipeline_turn;
    let mut yields = 0;
    while turn.read(std::time::Instant::now()).phase != PipelinePhase::WaitingForWork {
        yields += 1;
        assert!(yields < 100_000, "the pipeline never reached its wait");
        tokio::task::yield_now().await;
    }
    mail.ended(92_099, weaver_graph::NodeId(1), 0).await;

    let reading = stamps.recv().await.expect("the graph mail arm ran");
    assert_eq!(reading.phase, PipelinePhase::GraphRun);
    assert!(
        reading.busy_for.is_some(),
        "the turn is busy while it handles the mail"
    );
    pipeline.shutdown().await;
}

#[tokio::test]
async fn idle_footprint_tick_tracks_queue_edits_while_globally_paused() {
    let temp = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp).await;
    let handle = scheduler(&pipeline);
    handle.pause_all().await.unwrap();
    apply_intents(&mut pipeline).await;
    refresh_snapshot(&mut pipeline);
    assert!(!pipeline_live(&pipeline));
    assert_eq!(
        metrics(&pipeline)
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        0
    );

    let id = JobId(31020);
    insert_active_job(
        &mut pipeline,
        id,
        standalone_job_spec("idle queue edit", &[("payload.bin".into(), 512)]),
    )
    .await;
    report_all(&pipeline);
    clear_checks(&mut pipeline);
    assert!(!pipeline_live(&pipeline));
    refresh_snapshot(&mut pipeline);
    assert_eq!(
        metrics(&pipeline)
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics(&pipeline).download_jobs_hot.load(Ordering::Relaxed),
        1
    );

    handle.cancel_job(id).await.unwrap();
    assert!(!pipeline_live(&pipeline));
    refresh_snapshot(&mut pipeline);
    assert_eq!(
        metrics(&pipeline)
            .download_jobs_eligible
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        metrics(&pipeline).download_jobs_hot.load(Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn reconcile_checks_only_jobs_that_could_be_waiting_on_a_check() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
    let drained = seed_reconcile(&mut pipeline, &intermediate_dir);
    // The first pass checks the drained download and nothing queued.
    reconcile(&mut pipeline).await;
    assert_eq!(take_checks(&mut pipeline), vec![drained]);

    // Later passes with nothing changed check nothing: first because nothing
    // happened at all, then because the drained job looks as it did.
    reconcile(&mut pipeline).await;
    assert!(checks_empty(&pipeline));
    invalidate_reconciliation(&mut pipeline);
    reconcile(&mut pipeline).await;
    assert!(checks_empty(&pipeline));

    // Progress on the drained job makes it worth checking again.
    advance_downloaded(&mut pipeline, drained);
    invalidate_reconciliation(&mut pipeline);
    reconcile(&mut pipeline).await;
    assert_eq!(take_checks(&mut pipeline), vec![drained]);
}
