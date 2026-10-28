// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A lane that cannot be acquired must be visible, and must be retried.
//
// The acquire-failure handler keeps the download running either way — the
// work is requeued, or the leased articles take the failure and are retried
// through the ordinary result path — so the failure itself has to announce
// that it happened. Nothing above debug did, which made a job running on a
// fraction of its lanes look exactly like a slow server.

use super::*;

use crate::pipeline::download::transport::DownloadLaneMode;

fn lease_for(pipeline: &mut Pipeline, job_id: JobId) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    assert!(!works.is_empty(), "the fixture job has queued work");
    let completion_critical = works.iter().any(|work| work.completion_critical);
    pipeline.hold_test_lane_works(works.clone(), false);
    DownloadBatchLease {
        lane_id: 0,
        job_id,
        runtime_generation: 0,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        completion_critical,
        effective_exclude_servers: Vec::new(),
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    }
}

// Selection contention hands the work straight back and asks for another
// dispatch pass. Without that wake the freed connection would sit idle until
// the run loop next came round on its own — with the work that needs it
// already queued.
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
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
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
        pipeline
            .owned_lane_acquire_failure_log_throttle
            .last_emitted_at(job_id)
            .is_some(),
        "the failure must be reported rather than swallowed"
    );
}

// The warning is rate limited: a server that refuses one acquire refuses the
// next dispatch pass's too, and one line a minute is the budget.
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
        .owned_lane_acquire_failure_log_throttle
        .last_emitted_at(job_id)
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
        pipeline
            .owned_lane_acquire_failure_log_throttle
            .last_emitted_at(job_id),
        Some(first),
        "the second failure inside the window is counted, not logged again"
    );
}

// Lanes below their cap are only reported once they have stayed there, and
// only while there is work they could be carrying.
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
    // kinds — a download quota, pressure, a job's tail — and stays quiet.
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
    pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue = DownloadQueue::new();
    pipeline.log_download_lanes_under_cap(later + Duration::from_secs(2), 8);
    assert!(pipeline.download_lanes_under_cap_since.is_none());
}

// Work whose only server is waiting on its recovery probe parks in its queue:
// no dispatch pass leases it to a lane that could only come back refused, so
// no refusal requeues it for the next pass to send again. The probe's answer
// is the one wake, and after it the server seats new work.
#[tokio::test]
async fn work_whose_server_waits_on_its_probe_parks_until_the_probe_answers() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    // The one server is a listener this test owns. It never answers, so a
    // lane seated after the probe's answer dials it and waits, here, on this
    // machine, for the rest of the test.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut server = weaver_nntp::ServerPoolConfig::new(weaver_nntp::ServerConfig {
        host: "127.0.0.1".into(),
        port: listener.local_addr().unwrap().port(),
        tls: false,
        ..Default::default()
    });
    server.max_connections = 2;
    pipeline.nntp = std::sync::Arc::new(weaver_nntp::client::NntpClient::new(
        weaver_nntp::client::NntpClientConfig {
            servers: vec![server],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(1),
        },
    ));
    pipeline.tuner.set_connection_limit(2);
    let job_id = JobId(41503);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Held Server", &many_standalone_files("held", 4)),
    )
    .await;
    let queued = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .len();
    assert!(queued > 0, "the fixture job has queued work");

    let probe = pipeline.nntp.pool().take_recovery_probe_for_test(0);
    let mut health = pipeline.nntp.pool().server_health_changes();
    for _ in 0..3 {
        pipeline.dispatch_downloads();
    }

    assert_eq!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        queued,
        "no pass leases work to a server whose new connections are held"
    );
    assert_eq!(pipeline.active_download_connections, 0);
    assert!(pipeline.download_lane_owners.is_empty());

    probe.answer();
    health.changed().await;
    assert_eq!(pipeline.nntp.pool().connection_hold(0, true), None);

    pipeline.dispatch_downloads();
    assert!(
        pipeline
            .jobs
            .get(&job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len()
            < queued,
        "the answered server seats the waiting work"
    );
    drop(listener);
}
