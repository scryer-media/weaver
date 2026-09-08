//! Lanes must not idle while a job still has work they could serve.
//!
//! Direct-store binding gives every RAR volume its own queue priority, so a
//! refill rule that required the queue head to match the lane's current batch
//! could only ever refill inside one volume. On a long link that parked every
//! lane at every volume boundary and left the connections idle a quarter of
//! the run with thousands of articles queued.

use super::*;

/// Retag the whole queue so its head is priority `head_priority` and nothing
/// matches `lane_priority`, then return a compatibility cut from a work item
/// at `lane_priority` — the batch a lane established before the boundary.
fn split_queue_priorities(
    pipeline: &mut Pipeline,
    job_id: JobId,
    lane_priority: u32,
    head_priority: u32,
) -> DownloadBatchCompatibility {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state.download_queue.drain_all();
    assert!(
        works.len() >= 2,
        "the boundary case needs a queue with work on both sides"
    );
    let mut lane_sample = works.pop().unwrap();
    lane_sample.priority = lane_priority;
    let compatibility = DownloadBatchCompatibility::from_work(&lane_sample);
    for work in &mut works {
        work.priority = head_priority;
    }
    for work in works {
        state.download_queue.push(work);
    }
    compatibility
}

fn refill_request(
    job_id: JobId,
    compatibility: DownloadBatchCompatibility,
    response_tx: oneshot::Sender<DownloadLaneRefillResponse>,
) -> DownloadLaneRefillRequest {
    DownloadLaneRefillRequest {
        runtime_generation: 0,
        job_id,
        server_idx: 0,
        remote_ip: "127.0.0.1".parse().unwrap(),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        compatibility,
        response_tx,
    }
}

/// The park this whole change exists to remove: the queue head is one volume
/// on from the lane's batch, so the old equality rule refused the refill and
/// the connection went idle with the rest of the job still queued.
#[tokio::test]
async fn lane_refill_crosses_a_priority_boundary_instead_of_parking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41001);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Volume Boundary Refill",
            &many_standalone_files("volume-boundary", 8),
        ),
    )
    .await;

    let compatibility = split_queue_priorities(&mut pipeline, job_id, 10, 11);
    let queued_before = pipeline.jobs.get(&job_id).unwrap().download_queue.len();
    let parked_before = pipeline
        .metrics
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(
        job_id,
        compatibility,
        response_tx,
    ));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("a refill must take the queue head across a priority boundary");
    assert!(!lease.works.is_empty());
    assert!(
        lease.works.iter().all(|work| work.priority == 11),
        "the lease is filled from the head, not from the lane's old priority"
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        parked_before,
        "a lane must never park while the job has work it can serve"
    );
    assert!(pipeline.jobs.get(&job_id).unwrap().download_queue.len() < queued_before);
}

/// Stage two: the head's exclude set differs from the lane's batch, but this
/// lane's own server is not in it, so the lane can serve the work and the
/// lease is re-cut around it rather than refused.
#[tokio::test]
async fn lane_refill_takes_head_work_its_own_server_can_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41002);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Foreign Exclude Refill",
            &many_standalone_files("foreign-exclude", 6),
        ),
    )
    .await;

    let compatibility = {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut works = state.download_queue.drain_all();
        let lane_sample = works.pop().unwrap();
        let compatibility = DownloadBatchCompatibility::from_work(&lane_sample);
        for work in &mut works {
            // Another server failed these; server 0 — this lane — still may.
            work.exclude_servers = vec![1];
        }
        for work in works {
            state.download_queue.push(work);
        }
        compatibility
    };
    assert!(compatibility.exclude_servers.is_empty());

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(
        job_id,
        compatibility,
        response_tx,
    ));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("work this lane's server can fetch must not park it");
    assert!(!lease.works.is_empty());
    assert_eq!(
        lease.compatibility.exclude_servers,
        vec![1],
        "the lease is re-cut around the work it actually carries, so results \
         book their failures against the right exclude set"
    );
    assert!(
        !lease
            .works
            .iter()
            .any(|work| work.exclude_servers.contains(&0))
    );
}

/// A park frees a connection immediately. Dispatch has to see that in the same
/// turn, not whenever the run loop next comes round: the measured restart gap
/// was 340 ms at the median and 2.3 s at the tail.
#[tokio::test]
async fn a_parked_lane_wakes_dispatch_without_a_loop_turn() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41003);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Park Wakes Dispatch",
            &many_standalone_files("park-wake", 4),
        ),
    )
    .await;

    assert!(
        !pipeline.take_download_dispatch_wake(),
        "nothing has parked yet"
    );
    pipeline.active_download_connections = 1;
    pipeline
        .active_download_connections_by_job
        .insert(job_id, 1);

    pipeline.handle_download_lane_parked(DownloadLaneParked {
        job_id,
        mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        completion_critical: false,
        reason: LaneParkReason::NoWork,
        release_connection_slot: true,
        release_ip_replacement_burst: false,
    });

    assert!(
        pipeline.take_download_dispatch_wake(),
        "releasing the connection must ask for a dispatch pass"
    );
    assert!(
        !pipeline.take_download_dispatch_wake(),
        "the wake is consumed once"
    );

    // A park that keeps the connection has nothing to redispatch onto.
    pipeline.handle_download_lane_parked(DownloadLaneParked {
        job_id,
        mode: DownloadLaneMode::Sequential,
        spillover_loan_kind: None,
        completion_critical: false,
        reason: LaneParkReason::NoWork,
        release_connection_slot: false,
        release_ip_replacement_burst: false,
    });
    assert!(!pipeline.take_download_dispatch_wake());
}

/// The dispatch pass answers the wake it was asked for, so it must clear it.
#[tokio::test]
async fn dispatch_clears_the_park_wake_it_serves() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41004);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Wake Cleared", &many_standalone_files("wake-cleared", 2)),
    )
    .await;

    pipeline.download_dispatch_wake = true;
    pipeline.dispatch_downloads();

    assert!(!pipeline.take_download_dispatch_wake());
}

/// The gauge is a diagnostic, and a lane that reports a mode it was never
/// booked under must not be able to turn it into `usize::MAX`.
#[tokio::test]
async fn lane_depth_gauges_never_wrap_below_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;

    // Nothing was ever booked as sequential, so this transition is exactly the
    // lost-booking case.
    pipeline.note_download_lane_mode_changed(
        DownloadLaneMode::Sequential,
        DownloadLaneMode::Pipelined { depth: 2 },
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lanes_sequential_active
            .load(Ordering::Relaxed),
        0,
        "a decrement below zero must clamp, not wrap"
    );

    pipeline.note_download_lane_started(DownloadLaneMode::Sequential);
    pipeline.note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::NoWork);
    pipeline.note_download_lane_released(DownloadLaneMode::Sequential, LaneParkReason::NoWork);
    assert_eq!(
        pipeline
            .metrics
            .download_lanes_sequential_active
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics
            .download_lanes_active
            .load(Ordering::Relaxed),
        0
    );
}

/// A refill ignores the group a lane was leased for only while the server has
/// not proven it needs one selected. Once it has, the connection was opened
/// with GROUP and may not be handed an article the selection does not cover.
#[tokio::test]
async fn refill_asks_about_groups_only_on_a_server_that_needs_a_selected_group() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41005);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Group Gate", &many_standalone_files("group-gate", 2)),
    )
    .await;

    let mut works = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .download_queue
        .drain_all();
    let lane_sample = works.pop().unwrap();
    let compatibility = DownloadBatchCompatibility::from_work(&lane_sample);
    let mut other_group = works.pop().unwrap();
    other_group.priority = lane_sample.priority + 1;
    other_group.groups = Arc::from(vec!["alt.binaries.elsewhere".to_string()]);
    assert_ne!(other_group.groups, compatibility.groups);

    let free = DownloadBatchSelector::new(
        &compatibility,
        DownloadBatchRule::Refill {
            match_groups: false,
        },
    );
    assert!(
        free.matches(&other_group),
        "a message-id fetch needs no group, so an ordinary server refills across them"
    );

    let gated = DownloadBatchSelector::new(
        &compatibility,
        DownloadBatchRule::Refill { match_groups: true },
    );
    assert!(
        !gated.matches(&other_group),
        "a server that needs GROUP keeps the refill inside the selected group"
    );
    let mut same_group = other_group;
    same_group.groups = compatibility.groups.clone();
    assert!(
        gated.matches(&same_group),
        "priority still does not gate the refill even when groups do"
    );
    assert!(
        !DownloadBatchSelector::initial(&compatibility).matches(&same_group),
        "the initial rule is the one that still asks about priority"
    );
}

/// Splits the queue so the completion-critical heap holds `critical` items and
/// the ordinary heap the rest, and returns the compatibility of an ordinary
/// work item — the batch a payload lane is carrying.
fn split_queue_classes(
    pipeline: &mut Pipeline,
    job_id: JobId,
    critical: usize,
) -> DownloadBatchCompatibility {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state.download_queue.drain_all();
    assert!(
        works.len() > critical,
        "the class split needs work left over for the payload lane"
    );
    let lane_sample = works.pop().unwrap();
    let compatibility = DownloadBatchCompatibility::from_work(&lane_sample);
    assert!(!compatibility.completion_critical);
    for work in works.iter_mut().take(critical) {
        work.completion_critical = true;
    }
    works.push(lane_sample);
    for work in works {
        state.download_queue.push(work);
    }
    compatibility
}

/// An established connection changes class rather than parking.
///
/// Completion-critical work leads the queue because something downstream is
/// waiting on it, and a lane that already holds an authenticated connection is
/// the cheapest way to fetch it — cheaper than the park, the dropped socket
/// and the fresh handshake the class split used to force.
#[tokio::test]
async fn lane_refill_changes_class_to_take_completion_critical_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41010);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Class Changing Refill",
            &many_standalone_files("class-change", 6),
        ),
    )
    .await;

    let compatibility = split_queue_classes(&mut pipeline, job_id, 2);
    // The lane is booked as a payload connection, exactly as dispatch left it.
    pipeline.active_download_connections = 1;
    *pipeline
        .active_download_connections_by_job
        .entry(job_id)
        .or_default() = 1;

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(
        job_id,
        compatibility,
        response_tx,
    ));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("critical work this lane can serve must not park it");
    assert!(
        lease.compatibility.completion_critical,
        "the critical heap leads, so the refill re-opens the lease around it"
    );
    assert!(lease.works.iter().all(|work| work.completion_critical));
    assert_eq!(
        pipeline.active_completion_critical_connections, 1,
        "the connection is now counted under the class it is running"
    );
    assert_eq!(
        pipeline
            .active_completion_critical_connections_by_job
            .get(&job_id)
            .copied(),
        Some(1),
        "and the per-job spread sees it, so critical demand is not sent twice"
    );
    assert_eq!(
        pipeline.active_download_connections, 1,
        "changing class does not add a connection; it re-books the one in hand"
    );
}

/// A lane that changes back releases the class it was counted under, so the
/// two ends of the booking agree and the park cannot underflow it.
#[tokio::test]
async fn a_lane_that_changes_class_back_releases_the_critical_booking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41011);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Class Returning Refill",
            &many_standalone_files("class-return", 4),
        ),
    )
    .await;

    let mut critical = split_queue_classes(&mut pipeline, job_id, 0);
    critical.completion_critical = true;
    pipeline.active_download_connections = 1;
    pipeline.active_completion_critical_connections = 1;
    *pipeline
        .active_completion_critical_connections_by_job
        .entry(job_id)
        .or_default() = 1;

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(job_id, critical, response_tx));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("the job still has ordinary payload for this lane");
    assert!(!lease.compatibility.completion_critical);
    assert_eq!(pipeline.active_completion_critical_connections, 0);
    assert!(
        pipeline
            .active_completion_critical_connections_by_job
            .get(&job_id)
            .is_none()
    );
}

/// The yield exists to get a connection to completion-critical work. When the
/// lane asking for a refill can serve that work itself, the demand is met here
/// and there is nothing to yield — no park, no dropped socket, no redial.
#[tokio::test]
async fn a_requested_yield_hands_the_lane_critical_work_instead_of_parking_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41012);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Yield To Critical", &many_standalone_files("yield-crit", 6)),
    )
    .await;

    let compatibility = split_queue_classes(&mut pipeline, job_id, 2);
    // The signal belongs to a running dispatch period: opening a new one is
    // what clears it, so the lane must arrive while this job is already hot.
    pipeline.hot_dispatch_job = Some(job_id);
    pipeline.hot_share_yield_signal.request();
    let parked_before = pipeline
        .metrics
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(
        job_id,
        compatibility,
        response_tx,
    ));

    let response = response_rx.await.unwrap();
    let lease = response.lease.expect("the yield is served by this lane");
    assert!(lease.compatibility.completion_critical);
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        parked_before,
        "a lane that can serve the critical demand must not park to make room \
         for itself"
    );
}

/// With no critical work for this lane to take, the yield still does what it
/// says: the ordinary payload goes back to the queue untouched and the
/// connection is returned to the dispatcher.
#[tokio::test]
async fn a_requested_yield_still_parks_when_no_critical_work_is_servable() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41013);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec(
            "Yield Without Critical",
            &many_standalone_files("yield-none", 5),
        ),
    )
    .await;

    let compatibility = split_queue_classes(&mut pipeline, job_id, 0);
    pipeline.hot_dispatch_job = Some(job_id);
    pipeline.hot_share_yield_signal.request();
    let queued_before = pipeline.jobs.get(&job_id).unwrap().download_queue.len();

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(
        job_id,
        compatibility,
        response_tx,
    ));

    let response = response_rx.await.unwrap();
    assert!(response.lease.is_none());
    assert_eq!(response.park_reason, LaneParkReason::HotShareYield);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
        queued_before,
        "the payload the refill had taken goes back to the queue whole"
    );
}
