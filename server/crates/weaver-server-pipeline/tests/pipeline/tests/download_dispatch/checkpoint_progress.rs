// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use weaver_server_composition::{JobId, JobStatus};
use weaver_server_pipeline::DownloadQueue;
use weaver_server_pipeline::pipeline::{
    diagnostics::PipelineDiagnostics,
    tests::{
        dispatch_fixture::{DispatchFixture, checkpoint::restored_with_parked_retries},
        segmented_job_spec,
    },
};

fn queue(fixture: &DispatchFixture, id: JobId) -> &DownloadQueue {
    &fixture.job(id).articles.active().unwrap().download_queue
}

fn queue_mut(fixture: &mut DispatchFixture, id: JobId) -> &mut DownloadQueue {
    &mut fixture
        .job_mut(id)
        .articles
        .active_mut()
        .unwrap()
        .download_queue
}

#[tokio::test]
async fn checkpoint_deadline_is_consumed_when_capacity_pressure_or_pause_bypasses_selection() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    pipeline
        .add_job(
            PEER,
            segmented_job_spec("Ready peer", "peer.bin", &[100; 640]),
        )
        .await;
    pipeline.dispatch();
    for _ in 0..4 {
        pipeline.dispatch();
    }
    assert_eq!(pipeline.active_connections(), 4);
    assert_eq!(pipeline.held_connections(PEER).unwrap(), 3);
    for mode in 0..3 {
        pipeline.hold_downloads(mode == 1);
        pipeline
            .metrics()
            .write_buffered_bytes
            .store(if mode == 2 { u64::MAX } else { 0 }, Ordering::Relaxed);
        pipeline.schedule_checkpoint(HOT, Instant::now() - Duration::from_secs(1));
        assert_eq!(pipeline.checkpoint_delay(), Some(Duration::ZERO));
        pipeline.consume_due_checkpoints();
        for _ in 0..10 {
            pipeline.dispatch();
            assert!(
                pipeline
                    .checkpoint_delay()
                    .is_none_or(|delay| !delay.is_zero())
            );
        }
    }
    pipeline.schedule_checkpoint(HOT, Instant::now() + Duration::from_secs(3600));
    pipeline.pause_all_through_queue(true).await;
    assert!(pipeline.no_checkpoint_rechecks());
    pipeline.pause_all_through_queue(false).await;
    pipeline
        .metrics()
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);
    pipeline.pause_job_runtime(HOT).unwrap();
    assert!(!pipeline.checkpoint_retry_pending(HOT));
    pipeline.resume_job_runtime(HOT).unwrap();
    let result = pipeline.cancel_job(HOT).await;
    assert!(result.is_ok());
    assert!(!pipeline.checkpoint_retry_pending(HOT));
}

#[tokio::test]
async fn checkpoint_abandoned_lane_requeues_once_and_fences_late_events() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    let queued = queue(&pipeline, HOT).len();
    pipeline.dispatch();
    let (old_id, segment_id) = pipeline.checkpoint_booking(HOT).unwrap();
    let original = pipeline.outstanding_work(old_id, segment_id).clone();
    pipeline.mark_idle_past_stall(HOT);
    pipeline.pause_stalled_downloads();
    assert_eq!(pipeline.job(HOT).status, JobStatus::Paused);
    assert_eq!(queue(&pipeline, HOT).len(), queued);
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert!(pipeline.rate_reservations().is_empty());
    assert!(pipeline.checkpoint_booking(HOT).is_none());
    assert!(!pipeline.lane_owned(old_id));
    // Force reuse of the same segment to exercise the hardest stale-event
    // case; ordinary FIFO scheduling may select another queued article first.
    let hot_queue = queue_mut(&mut pipeline, HOT);
    let returned = hot_queue
        .pop_first_matching(|work| work.segment_id == segment_id)
        .unwrap();
    let other_work = std::mem::take(hot_queue);
    hot_queue.push(returned);
    pipeline.resume_job_runtime(HOT).unwrap();
    pipeline.dispatch();
    *queue_mut(&mut pipeline, HOT) = other_work;
    let (replacement, replacement_segment) = pipeline.checkpoint_booking(HOT).unwrap();
    assert_ne!(replacement, old_id);
    assert_eq!(replacement_segment, segment_id);
    let reservations = pipeline.rate_reservations().clone();
    pipeline
        .finish_retired_result(old_id, &original, Ok(b"late transport bytes"), false)
        .await;
    pipeline
        .finish_retired_result(old_id, &original, Err("old worker"), false)
        .await;
    assert!(pipeline.refill_sequential_lane(old_id).await.is_none());
    pipeline.return_unrequested(old_id, original);
    pipeline.park_on_error(old_id, HOT);
    assert_eq!(pipeline.articles_in_flight(), 1);
    assert_eq!(pipeline.active_connections(), 1);
    assert_eq!(pipeline.rate_reservations(), &reservations);
    assert_eq!(
        pipeline.checkpoint_booking(HOT).unwrap(),
        (replacement, segment_id)
    );
    assert_eq!(queue(&pipeline, HOT).len(), queued - 1);
    assert_eq!(pipeline.pending_retries(HOT).unwrap(), 130);
    assert_eq!(
        pipeline
            .outstanding_work(replacement, segment_id)
            .retry_count,
        0
    );
}

const HOT: JobId = JobId(49001);
const PEER: JobId = JobId(49002);

#[tokio::test]
async fn checkpoint_teardown_refunds_before_clearing_and_leaves_no_activity() {
    for cancel in [true, false] {
        let temp = tempfile::tempdir().unwrap();
        let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
        pipeline.dispatch();
        let (lane_id, segment_id) = pipeline.checkpoint_booking(HOT).unwrap();
        let original = pipeline.outstanding_work(lane_id, segment_id).clone();
        pipeline
            .add_job(
                PEER,
                segmented_job_spec("Ready peer", "peer.bin", &[100; 64]),
            )
            .await;
        let reserved = pipeline.rate_reservations()[&segment_id];
        // Keep a small unrelated debt after the refund: both a missing refund
        // and a duplicate refund produce a materially different balance.
        pipeline.set_rate(Some(1));
        pipeline.charge_rate(reserved + 100);
        if cancel {
            let done = pipeline.cancel_job(HOT).await;
            done.unwrap();
        } else {
            pipeline.job_mut(HOT).status = JobStatus::Failed {
                error: "removed".into(),
            };
            pipeline.purge_terminal_runtime(HOT);
        }
        assert!(pipeline.job_if_present(HOT).is_none());
        assert!(!pipeline.has_download_activity(HOT));
        assert!(!pipeline.lane_owned(lane_id));
        assert!(pipeline.rate_reservations().is_empty());
        assert_eq!(pipeline.articles_in_flight(), 0);
        assert_eq!(pipeline.active_connections(), 0);
        let remaining = pipeline.rate_ready_in().as_secs_f64();
        assert!(
            (95.0..=100.0).contains(&remaining),
            "refund debt: {remaining}"
        );
        for answer in [Ok(b"late bytes".as_slice()), Err("late error")] {
            pipeline
                .finish_retired_result(lane_id, &original, answer, true)
                .await;
        }
        assert!(pipeline.refill_sequential_lane(lane_id).await.is_none());
        pipeline.return_unrequested(lane_id, original);
        pipeline.park_on_error(lane_id, HOT);
        let remaining = pipeline.rate_ready_in().as_secs_f64();
        // Retired results still charge their ten actual transport bytes.
        assert!(
            (105.0..=110.0).contains(&remaining),
            "late events must only charge actual transport: {remaining}"
        );
        assert!(!pipeline.has_download_activity(HOT));
        assert!(pipeline.pending_retries(HOT).is_none());
        assert_eq!(pipeline.articles_in_flight(), 0);
        assert_eq!(pipeline.active_connections(), 0);
        pipeline.set_rate(None);
        pipeline.dispatch();
        assert!(pipeline.held_connections(PEER).unwrap() > 0);
    }
}

#[tokio::test]
async fn checkpoint_accepted_result_survives_pause_and_lane_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    let queued = queue(&pipeline, HOT).len();
    pipeline.dispatch();
    let (lane_id, segment_id) = pipeline.checkpoint_booking(HOT).unwrap();
    pipeline.pause_job_runtime(HOT).unwrap();
    assert_eq!(
        pipeline.articles_in_flight(),
        1,
        "ordinary pause preserves in-flight ownership"
    );
    assert!(pipeline.lane_owned(lane_id));
    let mut pending = pipeline.accept_unavailable_result(lane_id, segment_id);
    assert_eq!(pending.len(), 1);
    assert_eq!(pipeline.retire_stalled_lanes(HOT), 0);
    assert_eq!(pipeline.articles_in_flight(), 0);
    // The lane had nothing left in flight, so it is a hot worker between
    // leases: retirement leaves it up with its connection booked.
    assert_eq!(pipeline.active_connections(), 1);
    assert!(pipeline.lane_owned(lane_id));
    assert_eq!(queue(&pipeline, HOT).len(), queued - 1);
    assert_eq!(
        pipeline.checkpoint_booking(HOT).unwrap(),
        (lane_id, segment_id)
    );
    pipeline.process_released_result(&mut pending).await;
    assert!(pipeline.checkpoint_booking(HOT).is_none());
    assert_eq!(pipeline.pending_retries(HOT).unwrap(), 131);
    assert!(!pipeline.has_released_results(HOT));
}

#[tokio::test]
async fn checkpoint_progress_breaks_parked_retry_cycle_and_spills_to_a_peer() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    pipeline
        .add_job(
            PEER,
            segmented_job_spec("Ready peer", "peer.bin", &[100; 64]),
        )
        .await;
    pipeline.dispatch();
    assert_eq!(pipeline.tracked_in_flight(HOT), Some(1));
    assert_eq!(pipeline.pending_retries(HOT), Some(130));
    assert_eq!(pipeline.parked_retry_count(), 130);
    assert_eq!(pipeline.retry_deadline(), None);
    assert!(!pipeline.has_dispatchable_work(HOT));
    for _ in 0..3 {
        pipeline.dispatch();
    }
    assert_eq!(
        pipeline.tracked_in_flight(HOT),
        Some(1),
        "other connections cannot multiply the progress allowance"
    );
    assert!(
        pipeline.held_connections(PEER).unwrap_or(0) > 0,
        "a checkpoint-blocked hot queue must yield spare connections"
    );
    assert_eq!(pipeline.job(HOT).failed_bytes, 0);
}

#[tokio::test]
async fn checkpoint_progress_keeps_recovery_and_global_pressure_rules() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    pipeline.hold_downloads(true);
    pipeline.dispatch();
    assert_eq!(pipeline.articles_in_flight(), 0);
    pipeline.hold_downloads(false);
    pipeline
        .metrics()
        .write_buffered_bytes
        .store(u64::MAX, Ordering::Relaxed);
    pipeline.dispatch();
    assert_eq!(pipeline.articles_in_flight(), 0);
    pipeline
        .metrics()
        .write_buffered_bytes
        .store(0, Ordering::Relaxed);
    pipeline.set_active_decodes(HOT, 1);
    assert!(!pipeline.has_dispatchable_work(HOT));
    // Model promoted recovery behind a blocked critical primary article.
    // Recovery is exempt from the durable-lead gate.
    let mut recovery = pipeline.pop_checkpoint_work(HOT);
    recovery.is_recovery = true;
    recovery.completion_critical = true;
    let hot_queue = queue_mut(&mut pipeline, HOT);
    let mut critical_primary = hot_queue.pop_first_matching(|_| true).unwrap();
    critical_primary.completion_critical = true;
    hot_queue.push(critical_primary);
    queue_mut(&mut pipeline, HOT).push(recovery);
    assert!(
        queue(&pipeline, HOT)
            .peek_next_matching(|work| !work.is_recovery)
            .is_some(),
        "blocked payload remains ahead of recovery in the queue"
    );
    assert!(pipeline.has_dispatchable_work(HOT));
    let lease = pipeline.cut_server_lease(0).unwrap();
    assert_eq!(lease.works().len(), 1);
    assert!(lease.works()[0].is_recovery);
    assert!(lease.works()[0].completion_critical);
}

#[tokio::test]
async fn checkpoint_progress_diagnostics_track_staggered_retries_and_cancel() {
    let temp = tempfile::tempdir().unwrap();
    let mut pipeline = restored_with_parked_retries(&temp, HOT).await;
    pipeline.park_checkpoint_retry(HOT, Some(Duration::from_secs(60)));
    pipeline.park_checkpoint_retry(HOT, Some(Duration::from_secs(180)));
    let snapshot = pipeline.diagnostics();
    let job = &snapshot.jobs[0];
    assert_eq!(
        job.accepted_download_bytes,
        Some(pipeline.job(HOT).downloaded_bytes)
    );
    assert_eq!(job.durable_download_floor_bytes, Some(2904));
    assert_eq!(
        job.projected_undurable_download_bytes,
        Some(DispatchFixture::durable_lead_limit() + 2 * 1024 * 1024)
    );
    assert_eq!(
        job.checkpoint_admission.as_deref(),
        Some("progress_article")
    );
    assert_eq!(job.infrastructure_retries_timed, Some(2));
    assert_eq!(job.infrastructure_retries_indefinite, Some(130));
    let deadline = job.infrastructure_retry_next_deadline_epoch_ms.unwrap();
    assert!(
        deadline > snapshot.captured_at_epoch_ms
            && deadline < snapshot.captured_at_epoch_ms + 61_500.0
    );
    assert_eq!(
        pipeline.parked_retry_count(),
        132,
        "snapshot must be read-only"
    );

    let mut legacy = serde_json::to_value(&snapshot).unwrap();
    for name in [
        "accepted_download_bytes",
        "durable_download_floor_bytes",
        "projected_undurable_download_bytes",
        "checkpoint_lead_limit_bytes",
        "checkpoint_admission",
        "infrastructure_retries_timed",
        "infrastructure_retries_indefinite",
        "infrastructure_retry_next_deadline_epoch_ms",
    ] {
        legacy["jobs"][0].as_object_mut().unwrap().remove(name);
    }
    let legacy: PipelineDiagnostics = serde_json::from_value(legacy).unwrap();
    assert_eq!(legacy.jobs[0].checkpoint_admission, None);
    assert_eq!(legacy.jobs[0].accepted_download_bytes, None);

    let first_deadline = pipeline.retry_deadline().unwrap();
    let ready = pipeline.take_due_retries(first_deadline);
    assert_eq!(ready.len(), 1);
    let ready_id = ready[0].work().segment_id;
    for retry in ready {
        pipeline.requeue_retry(retry);
    }
    assert_eq!(pipeline.pending_retries(HOT), Some(131));
    assert_eq!(
        queue(&pipeline, HOT)
            .count_matching(|work| work.segment_id == ready_id && work.retry_count == 0),
        1
    );
    assert_eq!(
        pipeline.diagnostics().jobs[0].infrastructure_retries_timed,
        Some(1)
    );
    pipeline.dispatch();
    assert!(pipeline.checkpoint_booking(HOT).is_some());
    let result = pipeline.cancel_job(HOT).await;
    assert!(result.is_ok());
    assert_eq!(pipeline.parked_retry_count(), 0);
    assert!(pipeline.pending_retries(HOT).is_none());
    assert!(pipeline.checkpoint_booking(HOT).is_none());
    assert!(!pipeline.checkpoint_retry_pending(HOT));
}
