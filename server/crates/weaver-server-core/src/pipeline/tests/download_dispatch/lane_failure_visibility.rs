//! A lane that cannot be acquired must be visible, and must be retried.
//!
//! Both arms of the acquire-failure handler keep the download running — the
//! work is requeued or handed to an async lane — so the failure itself has to
//! announce that it happened. Nothing above debug did, which made a job running
//! on a fraction of its lanes look exactly like a slow server.

use super::*;

use crate::pipeline::download::transport::DownloadLaneMode;

fn lease_for(pipeline: &mut Pipeline, job_id: JobId) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works = state.download_queue.drain_all();
    assert!(!works.is_empty(), "the fixture job has queued work");
    let compatibility = DownloadBatchCompatibility::from_work(&works[0]);
    DownloadBatchLease {
        job_id,
        runtime_generation: 0,
        lane_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        compatibility,
        effective_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    }
}

/// Selection contention hands the work straight back and asks for another
/// dispatch pass. Without that wake the freed connection would sit idle until
/// the run loop next came round on its own — with the work that needs it
/// already queued.
#[tokio::test]
async fn a_contended_acquire_requeues_its_work_and_asks_for_another_pass() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41501);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Contended Acquire", &many_standalone_files("contended", 4)),
    )
    .await;

    let lease = lease_for(&mut pipeline, job_id);
    let leased = lease.works.len();
    pipeline.active_downloads = leased;
    pipeline.active_downloads_by_job.insert(job_id, leased);
    pipeline.active_download_connections = 1;
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);
    assert!(!pipeline.take_download_dispatch_wake());

    let mut pending = std::collections::VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::SelectionContended,
        },
        &mut pending,
    );

    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        leased,
        "contention is not a verdict about the servers: every work item goes \
         back to the scheduler"
    );
    assert!(
        pipeline.take_download_dispatch_wake(),
        "and the freed connection is offered to the next pass immediately"
    );
    assert_eq!(
        pipeline.active_download_connections, 0,
        "the connection slot must not be left booked against a lane that never \
         opened"
    );
    assert!(
        pipeline.last_owned_lane_acquire_failure_log_at.is_some(),
        "the failure must be reported rather than swallowed"
    );
}

/// The warning is rate limited: a server that refuses one acquire refuses the
/// next dispatch pass's too, and one line a minute is the budget.
#[tokio::test]
async fn repeated_acquire_failures_report_at_most_once_a_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41502);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Repeat Failures", &many_standalone_files("repeat", 4)),
    )
    .await;

    let mut pending = std::collections::VecDeque::new();
    let lease = lease_for(&mut pipeline, job_id);
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        },
        &mut pending,
    );
    let first = pipeline
        .last_owned_lane_acquire_failure_log_at
        .expect("the first failure of a window reports itself");

    let lease = lease_for(&mut pipeline, job_id);
    pipeline.handle_owned_download_lane_event(
        OwnedDownloadLaneEvent::AcquireFailed {
            lease,
            error: weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        },
        &mut pending,
    );
    assert_eq!(
        pipeline.last_owned_lane_acquire_failure_log_at,
        Some(first),
        "the second failure inside the window is counted, not logged again"
    );
}

/// Lanes below their cap are only reported once they have stayed there, and
/// only while there is work they could be carrying.
#[tokio::test]
async fn lanes_under_their_cap_are_reported_only_when_it_persists() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41503);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Under Cap", &many_standalone_files("under-cap", 4)),
    )
    .await;

    let start = Instant::now();
    pipeline.active_download_connections = 1;
    pipeline.last_owned_lane_acquire_failure_at = Some(start);
    pipeline.log_download_lanes_under_cap(start, 8);
    assert!(
        pipeline.download_lanes_under_cap_since.is_some(),
        "the window opens at the first pass that finds lanes unfilled"
    );
    assert!(
        pipeline.last_download_lanes_under_cap_log_at.is_none(),
        "a momentary gap between batches is not worth a line"
    );

    // Still under cap a window later, with a lane that failed to open inside
    // it: now it is worth saying.
    let later = start + Duration::from_secs(6);
    pipeline.log_download_lanes_under_cap(later, 8);
    assert_eq!(
        pipeline.last_download_lanes_under_cap_log_at,
        Some(later),
        "an underfill that outlives the window is reported"
    );

    // An underfill with no lane failure behind it is one of the ordinary
    // kinds — a bandwidth cap, pressure, a job's tail — and stays quiet.
    pipeline.last_download_lanes_under_cap_log_at = None;
    pipeline.download_lanes_under_cap_since = None;
    pipeline.last_owned_lane_acquire_failure_at = Some(start - Duration::from_secs(60));
    let quiet = later + Duration::from_secs(30);
    pipeline.log_download_lanes_under_cap(quiet, 8);
    pipeline.log_download_lanes_under_cap(quiet + Duration::from_secs(6), 8);
    assert!(
        pipeline.last_download_lanes_under_cap_log_at.is_none(),
        "lanes below their cap with no failed acquire are not a fault"
    );
    pipeline.last_owned_lane_acquire_failure_at = Some(quiet + Duration::from_secs(3));
    pipeline.log_download_lanes_under_cap(quiet + Duration::from_secs(7), 8);
    assert_eq!(
        pipeline.last_download_lanes_under_cap_log_at,
        Some(quiet + Duration::from_secs(7)),
        "a failure inside the window is what makes it worth a line"
    );

    // Filling the lanes closes the window, so the next underfill has to earn
    // its own.
    pipeline.active_download_connections = 8;
    pipeline.log_download_lanes_under_cap(later + Duration::from_secs(1), 8);
    assert!(pipeline.download_lanes_under_cap_since.is_none());

    // So does running out of work: idle lanes with an empty queue are the
    // ordinary end of a job, not a fault.
    pipeline.active_download_connections = 0;
    pipeline.jobs.get_mut(&job_id).unwrap().download_queue = DownloadQueue::new();
    pipeline.log_download_lanes_under_cap(later + Duration::from_secs(2), 8);
    assert!(pipeline.download_lanes_under_cap_since.is_none());
}
