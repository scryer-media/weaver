// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Lanes must not idle while a job still has work they could serve.
//
// An established connection asking for its next articles is the cheapest
// fetch a link has. Nothing about the articles a lane was carrying before may
// refuse it work the scheduler still holds for its server: not a priority
// boundary, not another server's failures, not the class the lane happens to
// be booked under.

use super::*;
use crate::pipeline::tests::new_scheduler_pipeline as new_direct_pipeline;

// Retag the queue so every remaining item sits at `head_priority`, and drop
// one item at `lane_priority` — the shape a direct-store volume boundary
// leaves behind when a lane's batch ends one volume short of the head.
fn split_queue_priorities(
    pipeline: &mut Pipeline,
    job_id: JobId,
    lane_priority: u32,
    head_priority: u32,
) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    assert!(
        works.len() >= 2,
        "the boundary case needs a queue with work on both sides"
    );
    let mut lane_sample = works.pop().unwrap();
    lane_sample.priority = lane_priority;
    for work in &mut works {
        work.priority = head_priority;
    }
    for work in works {
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
}

fn refill_request(
    response_tx: oneshot::Sender<DownloadLaneRefillResponse>,
) -> DownloadLaneRefillRequest {
    refill_request_on(0, 0, response_tx)
}

fn refill_request_on(
    lane_id: u64,
    server_idx: usize,
    response_tx: oneshot::Sender<DownloadLaneRefillResponse>,
) -> DownloadLaneRefillRequest {
    DownloadLaneRefillRequest {
        lane_id,
        runtime_generation: 0,
        server_idx,
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        response_tx,
    }
}

// A lane booked as `job_id`'s, holding a connection, so the refill has an
// owner entry to re-book and the connection gauges have something to move.
fn book_connected_lane(pipeline: &mut Pipeline, job_id: JobId, completion_critical: bool) -> u64 {
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            body_activity: Vec::new(),
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical,
            server_idx: Some(0),
            connection: true,
            outstanding: HashMap::new(),
            outstanding_bytes: 0,
        },
    );
    pipeline.active_download_connections = 1;
    *pipeline
        .active_download_connections_by_job
        .entry(job_id)
        .or_default() = 1;
    if completion_critical {
        pipeline.book_completion_critical_connection(job_id);
    }
    lane_id
}

// The park this whole change exists to remove: the queue head is one volume
// on from the lane's batch, so the old equality rule refused the refill and
// the connection went idle with the rest of the job still queued.
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

    split_queue_priorities(&mut pipeline, job_id, 10, 11);
    let queued_before = pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .len();
    let parked_before = pipeline
        .metrics
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(response_tx));

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
            < queued_before
    );
}

// Work another server failed is still this lane's to fetch, and the lease it
// comes back in says so in two separate places: the failure ledger sees only
// the job's retention exclusions, while the lane's dial is pinned to the one
// server the handout was cut for.
#[tokio::test]
async fn lane_refill_takes_head_work_its_own_server_can_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(NntpClient::new(NntpClientConfig {
        servers: (0..3)
            .map(|idx| weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: format!("server{idx}.example.invalid"),
                    port: 119,
                    tls: false,
                    ..Default::default()
                },
                max_connections: 2,
                ..Default::default()
            })
            .collect(),
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    }));
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

    {
        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        let mut works = state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .drain_all();
        works.pop().unwrap();
        for work in &mut works {
            // Another server failed these; server 0 — this lane — still may.
            work.exclude_servers = weaver_nntp::ServerSet::from([1]);
        }
        for work in works {
            state
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .push(work);
        }
    }

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request(response_tx));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("work this lane's server can fetch must not park it");
    assert!(!lease.works.is_empty());
    assert!(
        !lease
            .works
            .iter()
            .any(|work| work.exclude_servers.contains(0))
    );
    assert!(
        lease.effective_exclude_servers.is_empty(),
        "the ledger's exclusions are the job's retention rules, and this job \
         has none; one server's failures belong to the works that carry them"
    );
    assert_eq!(
        lease.dial_exclude_servers,
        vec![1, 2],
        "the handout was cut for server 0, so the lane may dial nowhere else"
    );
    assert_eq!(lease.server_modes, vec![(0, DownloadLaneMode::Sequential)]);
}

// A park frees a connection immediately. Dispatch has to see that in the same
// turn, not whenever the run loop next comes round: the measured restart gap
// was 340 ms at the median and 2.3 s at the tail.
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
        lane_id: 0,
        job_id,
        mode: DownloadLaneMode::Sequential,
        completion_critical: false,
        reason: LaneParkReason::NoWork,
        release_connection_slot: true,
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
        lane_id: 0,
        job_id,
        mode: DownloadLaneMode::Sequential,
        completion_critical: false,
        reason: LaneParkReason::NoWork,
        release_connection_slot: false,
    });
    assert!(!pipeline.take_download_dispatch_wake());
}

// The dispatch pass answers the wake it was asked for, so it must clear it.
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

// The gauge is a diagnostic, and a lane that reports a mode it was never
// booked under must not be able to turn it into `usize::MAX`.
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

// Splits the queue so the completion-critical heap holds `critical` items and
// the ordinary heap the rest.
fn split_queue_classes(pipeline: &mut Pipeline, job_id: JobId, critical: usize) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .drain_all();
    assert!(
        works.len() > critical,
        "the class split needs work left over for the payload lane"
    );
    for work in works.iter_mut().take(critical) {
        work.completion_critical = true;
    }
    for work in works {
        state
            .articles
            .active_mut()
            .unwrap()
            .download_queue
            .push(work);
    }
}

// An established connection changes class rather than parking.
//
// Completion-critical work leads its job's queue because something downstream
// is waiting on it, and a lane that already holds an authenticated connection
// is the cheapest way to fetch it — cheaper than the park, the dropped socket
// and the fresh handshake the class split used to force.
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

    split_queue_classes(&mut pipeline, job_id, 2);
    // The lane is booked as a payload connection, exactly as dispatch left it.
    let lane_id = book_connected_lane(&mut pipeline, job_id, false);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("critical work this lane can serve must not park it");
    assert!(
        lease.completion_critical,
        "the critical heap leads, so the refill re-opens the lease around it"
    );
    assert_eq!(
        lease
            .works
            .iter()
            .filter(|work| work.completion_critical)
            .count(),
        2,
        "the job's whole critical heap comes out in this handout"
    );
    assert!(
        lease.works[..2].iter().all(|work| work.completion_critical),
        "and it leads: no ordinary article is handed out ahead of it"
    );
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

// A lane that changes back releases the class it was counted under, so the
// two ends of the booking agree and the park cannot underflow it.
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

    split_queue_classes(&mut pipeline, job_id, 0);
    let lane_id = book_connected_lane(&mut pipeline, job_id, true);

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("the job still has ordinary payload for this lane");
    assert!(!lease.completion_critical);
    assert_eq!(pipeline.active_completion_critical_connections, 0);
    assert!(
        !pipeline
            .active_completion_critical_connections_by_job
            .contains_key(&job_id)
    );
}

// A lane belongs to a server, not to the job it last carried. When the job it
// was leased for is behind another in dispatch order, its next articles come
// from the hot job: the alternative is an authenticated connection sitting
// idle beside a queue it is allowed to fetch from.
#[tokio::test]
async fn a_refill_on_a_lane_of_a_job_that_is_not_hot_is_answered_from_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let hot_job_id = JobId(41014);
    let other_job_id = JobId(41015);
    insert_active_job(
        &mut pipeline,
        hot_job_id,
        standalone_job_spec("Hot Job Payload", &many_standalone_files("hot-payload", 4)),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        other_job_id,
        standalone_job_spec(
            "Other Job Critical Lane",
            &many_standalone_files("other-critical", 4),
        ),
    )
    .await;
    assert_eq!(pipeline.current_hot_job(), Some(hot_job_id));

    let lane_id = book_connected_lane(&mut pipeline, other_job_id, true);
    let queued_before = pipeline
        .jobs
        .get(&other_job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .len();

    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));

    let response = response_rx.await.unwrap();
    let lease = response
        .lease
        .expect("an established connection is never sent away while work is queued");
    assert_eq!(
        lease.job_id, hot_job_id,
        "the hot job takes every handout on a server it can serve"
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&other_job_id)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        queued_before,
        "and the job the lane came from keeps its queue whole"
    );
    assert_eq!(
        pipeline.download_lane_owners[&lane_id].job_id, hot_job_id,
        "the lane is re-booked against the job it is now carrying"
    );
    assert_eq!(
        pipeline
            .active_download_connections_by_job
            .get(&hot_job_id)
            .copied(),
        Some(1)
    );
    assert!(
        !pipeline
            .active_download_connections_by_job
            .contains_key(&other_job_id)
    );
}

#[tokio::test]
async fn a_lane_between_leases_outlives_the_job_it_last_served() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41016);
    let lane_id = book_connected_lane(&mut pipeline, job_id, false);

    assert_eq!(pipeline.retire_stalled_download_lanes(job_id), 0);

    assert!(
        pipeline.download_lane_owners.contains_key(&lane_id),
        "an idle lane is pool property, not the finished job's"
    );
    assert_eq!(pipeline.active_download_connections, 1);
}

// A lane at its share of the hot job is neither refused nor sent on to the
// next job: its ask waits, and is answered as soon as its holdings fall
// below the share. Rule 7 of the scheduler, seen from the lane's side.
#[tokio::test]
async fn a_saturated_refill_waits_for_the_lane_to_drain_below_its_share() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.tuner.set_connection_limit(100);
    let hot = JobId(41301);
    let next = JobId(41302);
    insert_active_job(
        &mut pipeline,
        hot,
        segmented_job_spec("Shared Reel", "payload.bin", &vec![512u32; 600]),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        next,
        segmented_job_spec("Shared Reel Two", "payload.bin", &vec![512u32; 600]),
    )
    .await;
    let lane_id = book_connected_lane(&mut pipeline, hot, false);
    let share = pipeline.lane_share_of_job(hot, 1);

    // Fill the lane to its share, one refill at a time; each is answered
    // from the hot job, and none is sent on to the next.
    let mut first_segment = None;
    for _ in 0..share {
        if pipeline.download_lane_holdings(lane_id) >= share {
            break;
        }
        let (response_tx, response_rx) = oneshot::channel();
        pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
        let lease = response_rx
            .await
            .unwrap()
            .lease
            .expect("a lane below its share is served");
        assert_eq!(lease.job_id, hot);
        first_segment.get_or_insert(lease.works[0].segment_id);
    }
    assert_eq!(pipeline.download_lane_holdings(lane_id), share);
    let drained = first_segment.unwrap();

    // At its share, the next ask is held: no lease, no park, and the next
    // job is not opened for it.
    let parked_before = pipeline
        .metrics
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);
    let (response_tx, mut response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    assert!(
        response_rx.try_recv().is_err(),
        "a saturated lane's ask is held, not answered"
    );
    assert_eq!(pipeline.held_download_refills.len(), 1);
    assert_eq!(
        pipeline
            .metrics
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        parked_before
    );
    assert_eq!(
        pipeline
            .jobs
            .get(&next)
            .unwrap()
            .articles
            .active()
            .unwrap()
            .download_queue
            .len(),
        600
    );

    // Servicing the hold while the lane still holds its share changes
    // nothing; the ask stays held past any idle deadline because the lane is
    // busy, not idle.
    pipeline.service_held_download_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.held_download_refills.len(), 1);

    // One result lands: the lane is below its share and the ask is answered
    // with exactly the difference, from the hot job.
    pipeline
        .download_lane_owners
        .get_mut(&lane_id)
        .unwrap()
        .outstanding
        .remove(&drained);
    pipeline.service_held_download_refills();
    let lease = response_rx
        .await
        .unwrap()
        .lease
        .expect("a lane below its share is served on the next service pass");
    assert_eq!(lease.job_id, hot);
    assert_eq!(lease.works.len(), 1);
    assert!(pipeline.held_download_refills.is_empty());
}

// A lane whose ask found every job stopped by a volume at its hard limit
// waits for the volume, not for the next pass: a pass with the volume still
// full puts its ask to no job again, and the pass after the volume has room
// answers it.
#[tokio::test]
async fn a_lane_held_for_a_full_volume_is_asked_again_only_once_it_has_room() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41401);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Full Volume", "payload.bin", &vec![512u32; 600]),
    )
    .await;
    let lane_id = book_connected_lane(&mut pipeline, job_id, false);
    pipeline.volume_buffers.set_figure(1024, u64::MAX);
    let volume = std::sync::Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    assert_eq!(volume.buffer_limits().1, 1024);
    let refused = |pipeline: &Pipeline| {
        pipeline.metrics.download_scheduler_hot_blocked_total
            [crate::SchedulerBlockClause::WriteBuffer.index()]
            .load(Ordering::Relaxed)
    };

    // Two results wait for the loop: the volume holds its limit, and the
    // lane's ask is refused and held.
    for _ in 0..2 {
        pipeline.note_released_download_result_pending(job_id, 512);
    }
    assert!(pipeline.volume_holds_budget(job_id));
    let (response_tx, mut response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.held_download_refills.len(), 1);
    let refused_once = refused(&pipeline);
    assert!(refused_once > 0, "the volume refused the job");

    // Passes while the volume is still full leave the hold as it is.
    for _ in 0..3 {
        pipeline.service_held_download_refills();
    }
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.held_download_refills.len(), 1);
    assert_eq!(refused(&pipeline), refused_once, "the job was not asked again");

    // The loop hands both results on: the volume has room, dispatch is
    // woken, and the next pass answers the lane.
    pipeline.download_dispatch_wake = false;
    pipeline.finish_released_download_result_processing(job_id, 512);
    pipeline.finish_released_download_result_processing(job_id, 512);
    assert!(pipeline.download_dispatch_wake);
    pipeline.service_held_download_refills();
    let lease = response_rx
        .await
        .unwrap()
        .lease
        .expect("a lane held for a volume is served once it has room");
    assert_eq!(lease.job_id, job_id);
    assert!(pipeline.held_download_refills.is_empty());
}

// A lane with no room for a whole pipe on a volume above its resume line
// is held for the volume, not for what it holds: passes while the volume
// is still above the line leave it held even once it holds nothing, and
// the pass after the volume has fallen to the line answers it. One wake
// for the crossing, counted held once and woken once.
#[tokio::test]
async fn a_lane_held_for_room_is_asked_again_once_its_volume_falls_to_its_resume_line() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41402);
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Room Line", "payload.bin", &vec![512u32; 600]),
    )
    .await;
    let lane_id = book_connected_lane(&mut pipeline, job_id, false);
    let hard = 64 * 512;
    pipeline.volume_buffers.set_figure(hard, u64::MAX);
    let volume = std::sync::Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    assert_eq!(volume.buffer_limits().1, hard);
    let line = volume.resume_line();
    let counted = |pipeline: &Pipeline| {
        (
            pipeline.metrics.download_lane_room_held_total.load(Ordering::Relaxed),
            pipeline.metrics.download_lane_room_woken_total.load(Ordering::Relaxed),
        )
    };

    // With room, the lane is handed a lease.
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    response_rx.await.unwrap().lease.expect("a lane with room is served");
    assert!(pipeline.download_lane_holdings(lane_id) > 0);

    // One article of room left, above the resume line: the next ask is
    // held for the volume.
    for _ in 0..63 {
        pipeline.note_released_download_result_pending(job_id, 512);
    }
    assert!(volume.unwritten_bytes() > line && volume.unwritten_bytes() < hard);
    let (response_tx, mut response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.held_download_refills.len(), 1);
    assert_eq!(counted(&pipeline), (1, 0));

    // The lane fetches everything it holds down: still above the line,
    // passes leave the hold as it is.
    let owner = pipeline.download_lane_owners.get_mut(&lane_id).unwrap();
    owner.outstanding.clear();
    owner.outstanding_bytes = 0;
    pipeline.service_held_download_refills();
    assert!(response_rx.try_recv().is_err(), "held while above the line");

    // Results are handed on down to one above the line: no wake, and the
    // hold stands.
    pipeline.download_dispatch_wake = false;
    while volume.unwritten_bytes() - 512 > line {
        pipeline.finish_released_download_result_processing(job_id, 512);
    }
    assert!(!pipeline.download_dispatch_wake, "no wake above the line");
    pipeline.service_held_download_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(counted(&pipeline), (1, 0));

    // The next one takes the volume to its line: dispatch is woken once,
    // and the pass answers the lane.
    pipeline.finish_released_download_result_processing(job_id, 512);
    assert!(volume.unwritten_bytes() <= line);
    assert!(pipeline.download_dispatch_wake, "the crossing wakes dispatch");
    pipeline.service_held_download_refills();
    let lease = response_rx
        .await
        .unwrap()
        .lease
        .expect("a lane held for room is served once its volume is at its line");
    assert_eq!(lease.job_id, job_id);
    assert!(pipeline.held_download_refills.is_empty());
    assert_eq!(counted(&pipeline), (1, 1));
}

// A lane held for a volume above its resume line that has since fetched
// everything it held down: the volume one article below its hard limit,
// and the lane's ask still held for it, the lane's pipe empty. The lane,
// its answer and the volume are returned.
async fn room_held_idle_lane(
    pipeline: &mut Pipeline,
    job_id: JobId,
) -> (
    u64,
    oneshot::Receiver<DownloadLaneRefillResponse>,
    std::sync::Arc<crate::operations::disk::volume::Volume>,
) {
    insert_active_job(
        pipeline,
        job_id,
        segmented_job_spec("Room One", "payload.bin", &vec![512u32; 600]),
    )
    .await;
    let lane_id = book_connected_lane(pipeline, job_id, false);
    let hard = 64 * 512;
    pipeline.volume_buffers.set_figure(hard, u64::MAX);
    let volume = std::sync::Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    response_rx.await.unwrap().lease.expect("a lane with room is served");
    for _ in 0..63 {
        pipeline.note_released_download_result_pending(job_id, 512);
    }
    assert!(volume.unwritten_bytes() > volume.resume_line());
    let (response_tx, mut response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(refill_request_on(lane_id, 0, response_tx));
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.room_held_download_refills_for_test(), 1);
    // The lane fetches what it holds down; its pipe is empty. While every
    // job waits for the volume, a pass leaves the hold.
    let owner = pipeline.download_lane_owners.get_mut(&lane_id).unwrap();
    owner.outstanding.clear();
    owner.outstanding_bytes = 0;
    pipeline.service_held_download_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.room_held_download_refills_for_test(), 1);
    (lane_id, response_rx, volume)
}

// A lane held for one job's volume that holds nothing is an idle lane: once
// a second job on a volume with room has work, the first pass asks the
// scheduler for the lane again and it is served, with the first volume
// still above its line. Which job it is handed is the scheduler's choice;
// a lane that holds nothing is never short of room for a whole pipe.
#[tokio::test]
async fn a_lane_held_for_room_is_served_once_a_job_on_a_volume_with_room_has_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let first = JobId(41403);
    let (_lane_id, mut response_rx, volume) = room_held_idle_lane(&mut pipeline, first).await;

    let second = JobId(41404);
    // The second job's folder lies on a device of its own.
    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    crate::runtime::fs::test_devices::assign(&elsewhere, 41_404);
    let intermediate = std::mem::replace(&mut pipeline.intermediate_dir, elsewhere);
    insert_active_job(
        &mut pipeline,
        second,
        segmented_job_spec("Room Two", "payload.bin", &vec![512u32; 60]),
    )
    .await;
    pipeline.intermediate_dir = intermediate;
    let other = std::sync::Arc::clone(
        pipeline.graph_jobs[&second]
            .disk()
            .unwrap()
            .working()
            .volume(),
    );
    assert!(!std::sync::Arc::ptr_eq(&volume, &other), "another volume");
    pipeline.service_held_download_refills();
    let lease = response_rx
        .try_recv()
        .expect("the held lane is answered on the pass")
        .lease
        .expect("the held lane is served");
    assert!(lease.job_id == first || lease.job_id == second);
    assert!(volume.unwritten_bytes() > volume.resume_line());
    assert_eq!(pipeline.room_held_download_refills_for_test(), 0);
}

// A lane held for room that holds nothing yields to a probe waiting for a
// seat on its server, as an idle lane does.
#[tokio::test]
async fn a_lane_held_for_room_yields_its_seat_to_a_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let (_lane_id, mut response_rx, _volume) =
        room_held_idle_lane(&mut pipeline, JobId(41405)).await;
    pipeline.probe_waits_on_for_test = Some(0);
    pipeline.service_held_download_refills();
    let answer = response_rx.try_recv().expect("the lane is answered");
    assert!(answer.lease.is_none());
    assert_eq!(answer.park_reason, LaneParkReason::ProbeYield);
    assert!(pipeline.held_download_refills.is_empty());
}

// A job removed while a lane holding nothing is held for its volume leaves
// no hold for the volume: the lane is held only as any idle lane is, and
// parks once the idle window is over.
#[tokio::test]
async fn a_job_removed_while_its_lane_is_held_for_room_leaves_no_standing_hold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41406);
    let (_lane_id, mut response_rx, _volume) = room_held_idle_lane(&mut pipeline, job_id).await;
    pipeline.report_all_for_test();
    let outcome = pipeline
        .shared_state
        .queue
        .cancel_jobs([job_id].into_iter().collect(), crate::jobs::handle::CancellationOrigin::User);
    assert!(outcome.iter().all(|(_, result)| result.is_ok()));
    pipeline.apply_queue_intents().await;
    crate::pipeline::tests::finish_cancel_jobs(&mut pipeline).await;
    pipeline.service_held_download_refills();
    assert_eq!(pipeline.room_held_download_refills_for_test(), 0);
    if pipeline.held_download_refills.is_empty() {
        assert!(response_rx.try_recv().is_ok(), "the lane was answered");
        return;
    }
    pipeline.age_held_download_refills_past_idle_window_for_test();
    pipeline.service_held_download_refills();
    let answer = response_rx.try_recv().expect("the idle window is over");
    assert!(answer.lease.is_none());
    assert!(pipeline.held_download_refills.is_empty());
}
