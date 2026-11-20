// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Lanes must not idle while a job still has work they could serve.
//
// An established connection asking for its next articles is the cheapest
// fetch a link has. Nothing about the articles a lane was carrying before may
// refuse it work the scheduler still holds for its server: not a priority
// boundary, not another server's failures, not the class the lane happens to
// be booked under.

use std::sync::{Arc, atomic::Ordering};
use std::time::Duration;
use weaver_nntp::client::{NntpClient, NntpClientConfig};
use weaver_server_composition::{JobId, SchedulerBlockClause};
use weaver_server_disk::operations::disk::volume::Volume;
use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::{
        DispatchFixture,
        sequential_refill::{SequentialRefill, volume_lease_room},
        volume_budget::volume_lease_limit,
    },
    segmented_job_spec,
};

fn queued(pipeline: &DispatchFixture, id: JobId) -> usize {
    pipeline
        .job(id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

// The park this whole change exists to remove: the queue head is one volume
// on from the lane's batch, so the old equality rule refused the refill and
// the connection went idle with the rest of the job still queued.
#[tokio::test]
async fn lane_refill_crosses_a_priority_boundary_instead_of_parking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41001);
    pipeline
        .add_files(job_id, "Volume Boundary Refill", "volume-boundary", 8)
        .await;

    pipeline.split_queue_priorities(job_id, 10, 11);
    let queued_before = queued(&pipeline, job_id);
    let parked_before = pipeline
        .metrics()
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);

    let response_rx = pipeline.ask_sequential_refill(0);

    let response = response_rx.answer().await;
    let lease = response
        .lease
        .expect("a refill must take the queue head across a priority boundary");
    assert!(!lease.works().is_empty());
    assert!(
        lease.works().iter().all(|work| work.priority == 11),
        "the lease is filled from the head, not from the lane's old priority"
    );
    assert_eq!(
        pipeline
            .metrics()
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        parked_before,
        "a lane must never park while the job has work it can serve"
    );
    assert!(queued(&pipeline, job_id) < queued_before);
}

// Work another server failed is still this lane's to fetch, and the lease it
// comes back in says so in two separate places: the failure ledger sees only
// the job's retention exclusions, while the lane's dial is pinned to the one
// server the handout was cut for.
#[tokio::test]
async fn lane_refill_takes_head_work_its_own_server_can_serve() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_client(NntpClient::new(NntpClientConfig {
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
    pipeline
        .add_files(job_id, "Foreign Exclude Refill", "foreign-exclude", 6)
        .await;

    {
        let state = pipeline.job_mut(job_id);
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

    let response_rx = pipeline.ask_sequential_refill(0);

    let response = response_rx.answer().await;
    let lease = response
        .lease
        .expect("work this lane's server can fetch must not park it");
    assert!(!lease.works().is_empty());
    assert!(
        !lease
            .works()
            .iter()
            .any(|work| work.exclude_servers.contains(0))
    );
    assert!(
        lease.effective_excludes().is_empty(),
        "the ledger's exclusions are the job's retention rules, and this job \
         has none; one server's failures belong to the works that carry them"
    );
    assert_eq!(
        lease.dial_excludes(),
        vec![1, 2],
        "the handout was cut for server 0, so the lane may dial nowhere else"
    );
    assert_eq!(lease.server_depths(), vec![(0, None)]);
}

// A park frees a connection immediately. Dispatch has to see that in the same
// turn, not whenever the run loop next comes round: the measured restart gap
// was 340 ms at the median and 2.3 s at the tail.
#[tokio::test]
async fn a_parked_lane_wakes_dispatch_without_a_loop_turn() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41003);
    pipeline
        .add_files(job_id, "Park Wakes Dispatch", "park-wake", 4)
        .await;

    assert!(!pipeline.take_dispatch_wake(), "nothing has parked yet");
    pipeline.set_active_connections(1);
    pipeline.set_job_active_connections(job_id, 1);

    pipeline.park_no_work(job_id, true);

    assert!(
        pipeline.take_dispatch_wake(),
        "releasing the connection must ask for a dispatch pass"
    );
    assert!(!pipeline.take_dispatch_wake(), "the wake is consumed once");

    // A park that keeps the connection has nothing to redispatch onto.
    pipeline.park_no_work(job_id, false);
    assert!(!pipeline.take_dispatch_wake());
}

// The dispatch pass answers the wake it was asked for, so it must clear it.
#[tokio::test]
async fn dispatch_clears_the_park_wake_it_serves() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41004);
    pipeline
        .add_files(job_id, "Wake Cleared", "wake-cleared", 2)
        .await;

    pipeline.set_dispatch_wake(true);
    pipeline.dispatch();

    assert!(!pipeline.take_dispatch_wake());
}

// The gauge is a diagnostic, and a lane that reports a mode it was never
// booked under must not be able to turn it into `usize::MAX`.
#[tokio::test]
async fn lane_depth_gauges_never_wrap_below_zero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;

    // Nothing was ever booked as sequential, so this transition is exactly the
    // lost-booking case.
    pipeline.change_sequential_lane_mode(2);
    assert_eq!(
        pipeline
            .metrics()
            .download_lanes_sequential_active
            .load(Ordering::Relaxed),
        0,
        "a decrement below zero must clamp, not wrap"
    );

    pipeline.start_sequential_lane();
    pipeline.release_sequential_lane();
    pipeline.release_sequential_lane();
    assert_eq!(
        pipeline
            .metrics()
            .download_lanes_sequential_active
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        pipeline
            .metrics()
            .download_lanes_active
            .load(Ordering::Relaxed),
        0
    );
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
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41010);
    pipeline
        .add_files(job_id, "Class Changing Refill", "class-change", 6)
        .await;

    pipeline.split_queue_classes(job_id, 2);
    // The lane is booked as a payload connection, exactly as dispatch left it.
    let lane_id = pipeline.book_connected_lane(job_id, false);

    let response_rx = pipeline.ask_sequential_refill(lane_id);

    let response = response_rx.answer().await;
    let lease = response
        .lease
        .expect("critical work this lane can serve must not park it");
    assert!(
        lease.critical(),
        "the critical heap leads, so the refill re-opens the lease around it"
    );
    assert_eq!(
        lease
            .works()
            .iter()
            .filter(|work| work.completion_critical)
            .count(),
        2,
        "the job's whole critical heap comes out in this handout"
    );
    assert!(
        lease.works()[..2]
            .iter()
            .all(|work| work.completion_critical),
        "and it leads: no ordinary article is handed out ahead of it"
    );
    assert_eq!(
        pipeline.active_critical_connections(),
        1,
        "the connection is now counted under the class it is running"
    );
    assert_eq!(
        pipeline.active_job_critical_connections(job_id),
        Some(1),
        "and the per-job spread sees it, so critical demand is not sent twice"
    );
    assert_eq!(
        pipeline.active_connections(),
        1,
        "changing class does not add a connection; it re-books the one in hand"
    );
}

// A lane that changes back releases the class it was counted under, so the
// two ends of the booking agree and the park cannot underflow it.
#[tokio::test]
async fn a_lane_that_changes_class_back_releases_the_critical_booking() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41011);
    pipeline
        .add_files(job_id, "Class Returning Refill", "class-return", 4)
        .await;

    pipeline.split_queue_classes(job_id, 0);
    let lane_id = pipeline.book_connected_lane(job_id, true);

    let response_rx = pipeline.ask_sequential_refill(lane_id);

    let response = response_rx.answer().await;
    let lease = response
        .lease
        .expect("the job still has ordinary payload for this lane");
    assert!(!lease.critical());
    assert_eq!(pipeline.active_critical_connections(), 0);
    assert!(pipeline.active_job_critical_connections(job_id).is_none());
}

// A lane belongs to a server, not to the job it last carried. When the job it
// was leased for is behind another in dispatch order, its next articles come
// from the hot job: the alternative is an authenticated connection sitting
// idle beside a queue it is allowed to fetch from.
#[tokio::test]
async fn a_refill_on_a_lane_of_a_job_that_is_not_hot_is_answered_from_the_hot_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let hot_job_id = JobId(41014);
    let other_job_id = JobId(41015);
    pipeline
        .add_files(hot_job_id, "Hot Job Payload", "hot-payload", 4)
        .await;
    pipeline
        .add_files(other_job_id, "Other Job Critical Lane", "other-critical", 4)
        .await;
    assert_eq!(pipeline.hot_job(), Some(hot_job_id));

    let lane_id = pipeline.book_connected_lane(other_job_id, true);
    let queued_before = queued(&pipeline, other_job_id);

    let response_rx = pipeline.ask_sequential_refill(lane_id);

    let response = response_rx.answer().await;
    let lease = response
        .lease
        .expect("an established connection is never sent away while work is queued");
    assert_eq!(
        lease.job_id(),
        hot_job_id,
        "the hot job takes every handout on a server it can serve"
    );
    assert_eq!(
        queued(&pipeline, other_job_id),
        queued_before,
        "and the job the lane came from keeps its queue whole"
    );
    assert_eq!(
        pipeline.lane_owner_job(lane_id),
        hot_job_id,
        "the lane is re-booked against the job it is now carrying"
    );
    assert_eq!(pipeline.held_connections(hot_job_id), Some(1));
    assert!(pipeline.held_connections(other_job_id).is_none());
}

#[tokio::test]
async fn a_lane_between_leases_outlives_the_job_it_last_served() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41016);
    let lane_id = pipeline.book_connected_lane(job_id, false);

    assert_eq!(pipeline.retire_stalled_lanes(job_id), 0);

    assert!(
        pipeline.lane_owned(lane_id),
        "an idle lane is pool property, not the finished job's"
    );
    assert_eq!(pipeline.active_connections(), 1);
}

// A lane at its share of the hot job is neither refused nor sent on to the
// next job: its ask waits, and is answered as soon as its holdings fall
// below the share. Rule 7 of the scheduler, seen from the lane's side.
#[tokio::test]
async fn a_saturated_refill_waits_for_the_lane_to_drain_below_its_share() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    pipeline.set_connection_limit(100);
    let hot = JobId(41301);
    let next = JobId(41302);
    pipeline
        .add_job(
            hot,
            segmented_job_spec("Shared Reel", "payload.bin", &vec![512u32; 600]),
        )
        .await;
    pipeline
        .add_job(
            next,
            segmented_job_spec("Shared Reel Two", "payload.bin", &vec![512u32; 600]),
        )
        .await;
    let lane_id = pipeline.book_connected_lane(hot, false);
    let share = pipeline.lane_share_of_job(hot, 1);

    // Fill the lane to its share, one refill at a time; each is answered
    // from the hot job, and none is sent on to the next.
    let mut first_segment = None;
    for _ in 0..share {
        if pipeline.lane_holdings(lane_id) >= share {
            break;
        }
        let response_rx = pipeline.ask_sequential_refill(lane_id);
        let lease = response_rx
            .answer()
            .await
            .lease
            .expect("a lane below its share is served");
        assert_eq!(lease.job_id(), hot);
        first_segment.get_or_insert(lease.works()[0].segment_id);
    }
    assert_eq!(pipeline.lane_holdings(lane_id), share);
    let drained = first_segment.unwrap();

    // At its share, the next ask is held: no lease, no park, and the next
    // job is not opened for it.
    let parked_before = pipeline
        .metrics()
        .download_lane_refill_parked_total
        .load(Ordering::Relaxed);
    let mut response_rx = pipeline.ask_sequential_refill(lane_id);
    assert!(
        response_rx.try_recv().is_err(),
        "a saturated lane's ask is held, not answered"
    );
    assert_eq!(pipeline.held_refills(), 1);
    assert_eq!(
        pipeline
            .metrics()
            .download_lane_refill_parked_total
            .load(Ordering::Relaxed),
        parked_before
    );
    assert_eq!(queued(&pipeline, next), 600);

    // Servicing the hold while the lane still holds its share changes
    // nothing; the ask stays held past any idle deadline because the lane is
    // busy, not idle.
    pipeline.service_held_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.held_refills(), 1);

    // One result lands: the lane is below its share and the ask is answered
    // with exactly the difference, from the hot job.
    pipeline.remove_lane_outstanding(lane_id, drained);
    pipeline.service_held_refills();
    let lease = response_rx
        .answer()
        .await
        .lease
        .expect("a lane below its share is served on the next service pass");
    assert_eq!(lease.job_id(), hot);
    assert_eq!(lease.works().len(), 1);
    assert!(pipeline.held_refills() == 0);
}

// A lane whose ask found every job stopped by a volume at its hard limit
// waits for the volume, not for the next pass: a pass with the volume still
// full puts its ask to no job again, and the pass after the volume has room
// answers it.
#[tokio::test]
async fn a_lane_held_for_a_full_volume_is_asked_again_only_once_it_has_room() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41401);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Full Volume", "payload.bin", &vec![512u32; 600]),
        )
        .await;
    let lane_id = pipeline.book_connected_lane(job_id, false);
    pipeline.set_volume_budget(1024);
    let volume = pipeline.volume(job_id);
    assert_eq!(volume.buffer_limits().1, 1024);
    let refused = |pipeline: &DispatchFixture| {
        pipeline.metrics().download_scheduler_hot_blocked_total
            [SchedulerBlockClause::WriteBuffer.index()]
        .load(Ordering::Relaxed)
    };

    // Two results wait for the loop: the volume holds its limit, and the
    // lane's ask is refused and held.
    for _ in 0..2 {
        pipeline.note_arrived_result(job_id, 512);
    }
    assert!(pipeline.volume_holds_budget(job_id));
    let mut response_rx = pipeline.ask_sequential_refill(lane_id);
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.held_refills(), 1);
    let refused_once = refused(&pipeline);
    assert!(refused_once > 0, "the volume refused the job");

    // Passes while the volume is still full leave the hold as it is.
    for _ in 0..3 {
        pipeline.service_held_refills();
    }
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.held_refills(), 1);
    assert_eq!(
        refused(&pipeline),
        refused_once,
        "the job was not asked again"
    );

    // The loop hands both results on: the volume has room, dispatch is
    // woken, and the next pass answers the lane.
    pipeline.set_dispatch_wake(false);
    pipeline.hand_on_result(job_id, 512);
    pipeline.hand_on_result(job_id, 512);
    assert!(pipeline.dispatch_wake());
    pipeline.service_held_refills();
    let lease = response_rx
        .answer()
        .await
        .lease
        .expect("a lane held for a volume is served once it has room");
    assert_eq!(lease.job_id(), job_id);
    assert!(pipeline.held_refills() == 0);
}

// A lane with no room for a whole pipe on a volume above its resume line
// is held for the volume, not for what it holds: passes while the volume
// is still above the line leave it held even once it holds nothing, and
// the pass after the volume has fallen to the line answers it. One wake
// for the crossing, counted held once and woken once.
#[tokio::test]
async fn a_lane_held_for_room_is_asked_again_once_its_volume_falls_to_its_resume_line() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41402);
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Room Line", "payload.bin", &vec![512u32; 600]),
        )
        .await;
    let lane_id = pipeline.book_connected_lane(job_id, false);
    let hard = 64 * 512;
    pipeline.set_volume_budget(hard);
    let volume = pipeline.volume(job_id);
    assert_eq!(volume.buffer_limits().1, hard);
    let line = volume.resume_line();
    let counted = |pipeline: &DispatchFixture| {
        (
            pipeline
                .metrics()
                .download_lane_room_held_total
                .load(Ordering::Relaxed),
            pipeline
                .metrics()
                .download_lane_room_woken_total
                .load(Ordering::Relaxed),
        )
    };

    // With room, the lane is handed a lease.
    let response_rx = pipeline.ask_sequential_refill(lane_id);
    response_rx
        .answer()
        .await
        .lease
        .expect("a lane with room is served");
    assert!(pipeline.lane_holdings(lane_id) > 0);

    // Less than an article of room left, above the resume line: the next
    // ask is held for the volume.
    fill_to_room_short(&mut pipeline, job_id, &volume);
    assert!(volume.unwritten_bytes() > line && volume.unwritten_bytes() < hard);
    let mut response_rx = pipeline.ask_sequential_refill(lane_id);
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.held_refills(), 1);
    assert_eq!(counted(&pipeline), (1, 0));

    // The lane fetches everything it holds down: still above the line,
    // passes leave the hold as it is.
    fetch_down(&mut pipeline, lane_id, job_id);
    pipeline.service_held_refills();
    assert!(response_rx.try_recv().is_err(), "held while above the line");

    // Results are handed on down to one above the line: no wake, and the
    // hold stands.
    pipeline.set_dispatch_wake(false);
    while volume.unwritten_bytes() - 512 > line {
        pipeline.hand_on_result(job_id, 512);
    }
    assert!(!pipeline.dispatch_wake(), "no wake above the line");
    pipeline.service_held_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(counted(&pipeline), (1, 0));

    // The next one takes the volume to its line: dispatch is woken once,
    // and the pass answers the lane.
    pipeline.hand_on_result(job_id, 512);
    assert!(volume.unwritten_bytes() <= line);
    assert!(pipeline.dispatch_wake(), "the crossing wakes dispatch");
    pipeline.service_held_refills();
    let lease = response_rx
        .answer()
        .await
        .lease
        .expect("a lane held for room is served once its volume is at its line");
    assert_eq!(lease.job_id(), job_id);
    assert!(pipeline.held_refills() == 0);
    assert_eq!(counted(&pipeline), (1, 1));
}

// Results of 512 bytes arrive for the job until less than one more article
// fits below its volume's lease limit: one byte of room is left.
fn fill_to_room_short(pipeline: &mut DispatchFixture, job_id: JobId, volume: &Volume) {
    let limit = volume_lease_limit(volume);
    while volume.unwritten_bytes() + 512 < limit {
        pipeline.note_arrived_result(job_id, 512);
    }
    let rest = limit - 1 - volume.unwritten_bytes();
    if rest != 0 {
        pipeline.note_arrived_result(job_id, rest);
    }
    assert_eq!(volume_lease_room(volume), 1);
}

// Every article the lane holds arrives whole: its reservation is counted
// on as arrived, and the result waits for the loop.
fn fetch_down(pipeline: &mut DispatchFixture, lane_id: u64, job_id: JobId) {
    pipeline.note_all_lane_results_arrived(lane_id, job_id);
    assert_eq!(pipeline.lane_holdings(lane_id), 0);
}

// A lane held for a volume above its resume line that has since fetched
// everything it held down: the volume one byte below its lease limit,
// and the lane's ask still held for it, the lane's pipe empty. The lane,
// its answer and the volume are returned.
async fn room_held_idle_lane(
    pipeline: &mut DispatchFixture,
    job_id: JobId,
) -> (u64, SequentialRefill, Arc<Volume>) {
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Room One", "payload.bin", &vec![512u32; 600]),
        )
        .await;
    let lane_id = pipeline.book_connected_lane(job_id, false);
    let hard = 64 * 512;
    pipeline.set_volume_budget(hard);
    let volume = pipeline.volume(job_id);
    let response_rx = pipeline.ask_sequential_refill(lane_id);
    response_rx
        .answer()
        .await
        .lease
        .expect("a lane with room is served");
    fill_to_room_short(pipeline, job_id, &volume);
    assert!(volume.unwritten_bytes() > volume.resume_line());
    let mut response_rx = pipeline.ask_sequential_refill(lane_id);
    assert!(response_rx.try_recv().is_err(), "the ask is held");
    assert_eq!(pipeline.room_held_refills(), 1);
    // The lane fetches what it holds down; its pipe is empty. While every
    // job waits for the volume, a pass leaves the hold.
    fetch_down(pipeline, lane_id, job_id);
    pipeline.service_held_refills();
    assert!(response_rx.try_recv().is_err());
    assert_eq!(pipeline.room_held_refills(), 1);
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
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let first = JobId(41403);
    let (_lane_id, mut response_rx, volume) = room_held_idle_lane(&mut pipeline, first).await;

    let second = JobId(41404);
    // The second job's folder lies on a device of its own.
    let elsewhere = temp_dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    weaver_server_disk::runtime::fs::test_devices::assign(&elsewhere, 41_404);
    pipeline
        .add_job_on_another_device(
            second,
            segmented_job_spec("Room Two", "payload.bin", &vec![512u32; 60]),
            elsewhere,
        )
        .await;
    let other = pipeline.volume(second);
    assert!(!Arc::ptr_eq(&volume, &other), "another volume");
    pipeline.service_held_refills();
    let lease = response_rx
        .try_recv()
        .expect("the held lane is answered on the pass")
        .lease
        .expect("the held lane is served");
    assert!(lease.job_id() == first || lease.job_id() == second);
    assert!(volume.unwritten_bytes() > volume.resume_line());
    assert_eq!(pipeline.room_held_refills(), 0);
}

// A lane held for room that holds nothing yields to a probe waiting for a
// seat on its server, as an idle lane does.
#[tokio::test]
async fn a_lane_held_for_room_yields_its_seat_to_a_probe() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let (_lane_id, mut response_rx, _volume) =
        room_held_idle_lane(&mut pipeline, JobId(41405)).await;
    pipeline.set_waiting_probe(0);
    pipeline.service_held_refills();
    let answer = response_rx.try_recv().expect("the lane is answered");
    assert!(answer.lease.is_none());
    assert!(answer.yielded_to_probe());
    assert!(pipeline.held_refills() == 0);
}

// A job removed while a lane holding nothing is held for its volume leaves
// no hold for the volume: the lane is held only as any idle lane is, and
// parks once the idle window is over.
#[tokio::test]
async fn a_job_removed_while_its_lane_is_held_for_room_leaves_no_standing_hold() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::scheduler(&temp_dir).await;
    let job_id = JobId(41406);
    let (_lane_id, mut response_rx, _volume) = room_held_idle_lane(&mut pipeline, job_id).await;
    assert!(pipeline.request_job_cancel(job_id));
    pipeline.finish_requested_cancellations().await;
    pipeline.service_held_refills();
    assert_eq!(pipeline.room_held_refills(), 0);
    if pipeline.held_refills() == 0 {
        assert!(response_rx.try_recv().is_ok(), "the lane was answered");
        return;
    }
    pipeline.age_held_refills_past_idle_window();
    pipeline.service_held_refills();
    let answer = response_rx.try_recv().expect("the idle window is over");
    assert!(answer.lease.is_none());
    assert!(pipeline.held_refills() == 0);
}
