//! Lanes must not idle while a job still has work they could serve.
//!
//! An established connection asking for its next articles is the cheapest
//! fetch a link has. Nothing about the articles a lane was carrying before may
//! refuse it work the scheduler still holds for its server: not a priority
//! boundary, not another server's failures, not the class the lane happens to
//! be booked under.

use super::*;

/// Retag the queue so every remaining item sits at `head_priority`, and drop
/// one item at `lane_priority` — the shape a direct-store volume boundary
/// leaves behind when a lane's batch ends one volume short of the head.
fn split_queue_priorities(
    pipeline: &mut Pipeline,
    job_id: JobId,
    lane_priority: u32,
    head_priority: u32,
) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state.download_queue.drain_all();
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
        state.download_queue.push(work);
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
        remote_ip: Some("127.0.0.1".parse().unwrap()),
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        response_tx,
    }
}

/// A lane booked as `job_id`'s, holding a connection, so the refill has an
/// owner entry to re-book and the connection gauges have something to move.
fn book_connected_lane(pipeline: &mut Pipeline, job_id: JobId, completion_critical: bool) -> u64 {
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            job_id,
            mode: DownloadLaneMode::Sequential,
            completion_critical,
            server_idx: Some(0),
            connection: true,
            ip_replacement: false,
            outstanding: HashMap::new(),
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

    split_queue_priorities(&mut pipeline, job_id, 10, 11);
    let queued_before = pipeline.jobs.get(&job_id).unwrap().download_queue.len();
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
    assert!(pipeline.jobs.get(&job_id).unwrap().download_queue.len() < queued_before);
}

/// Work another server failed is still this lane's to fetch, and the lease it
/// comes back in says so in two separate places: the failure ledger sees only
/// the job's retention exclusions, while the lane's dial is pinned to the one
/// server the handout was cut for.
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
        let mut works = state.download_queue.drain_all();
        works.pop().unwrap();
        for work in &mut works {
            // Another server failed these; server 0 — this lane — still may.
            work.exclude_servers = vec![1];
        }
        for work in works {
            state.download_queue.push(work);
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
            .any(|work| work.exclude_servers.contains(&0))
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
        lane_id: 0,
        job_id,
        mode: DownloadLaneMode::Sequential,
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
        lane_id: 0,
        job_id,
        mode: DownloadLaneMode::Sequential,
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

/// Splits the queue so the completion-critical heap holds `critical` items and
/// the ordinary heap the rest.
fn split_queue_classes(pipeline: &mut Pipeline, job_id: JobId, critical: usize) {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let mut works = state.download_queue.drain_all();
    assert!(
        works.len() > critical,
        "the class split needs work left over for the payload lane"
    );
    for work in works.iter_mut().take(critical) {
        work.completion_critical = true;
    }
    for work in works {
        state.download_queue.push(work);
    }
}

/// An established connection changes class rather than parking.
///
/// Completion-critical work leads its job's queue because something downstream
/// is waiting on it, and a lane that already holds an authenticated connection
/// is the cheapest way to fetch it — cheaper than the park, the dropped socket
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

/// A lane belongs to a server, not to the job it last carried. When the job it
/// was leased for is behind another in dispatch order, its next articles come
/// from the hot job: the alternative is an authenticated connection sitting
/// idle beside a queue it is allowed to fetch from.
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
