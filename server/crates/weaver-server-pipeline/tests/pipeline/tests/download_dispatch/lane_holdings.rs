// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job owes only the downloads its live lanes hold: every in-flight gauge is
// raised when an article enters a lane's record and lowered when it leaves,
// so no path can leave a job waiting on articles nobody is fetching.

use weaver_server_pipeline::pipeline::tests::{
    dispatch_fixture::DispatchFixture, segmented_job_spec,
};
use weaver_server_pipeline::{JobId, JobStatus};

fn assert_gauges_match_lanes(pipeline: &DispatchFixture) {
    let discrepancies = pipeline.lane_holdings_discrepancies();
    assert!(discrepancies.is_empty(), "{discrepancies:#?}");
}

fn queued(pipeline: &DispatchFixture, job_id: JobId) -> usize {
    pipeline
        .job(job_id)
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

// T1, a lane that is not live: the result is refused, and the lane that does
// hold the article still owes it.
#[tokio::test]
async fn a_result_from_a_lane_that_is_not_live_moves_no_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41701);
    pipeline.add_files(job_id, "Not Live", "not-live", 3).await;
    let lease = pipeline.lease_to_new_lane(job_id, 2);
    let held = lease.works()[0].segment_id;
    assert_eq!(pipeline.in_flight(job_id), 2);

    let stranger = DispatchFixture::next_lane_id();
    pipeline.fail_transport(stranger, held).await;
    assert_eq!(pipeline.in_flight(job_id), 2);
    assert_gauges_match_lanes(&pipeline);

    pipeline.fail_transport(lease.id(), held).await;
    assert_eq!(pipeline.in_flight(job_id), 1);
    assert_gauges_match_lanes(&pipeline);
}

// T1, an article the lane does not hold: handing it back moves nothing and
// queues nothing, and the article the lane does hold comes back once.
#[tokio::test]
async fn an_article_the_lane_does_not_hold_is_refused_without_moving_a_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41702);
    pipeline.add_files(job_id, "Not Held", "not-held", 3).await;
    let mut lease = pipeline.lease_to_new_lane(job_id, 1);
    let stray = pipeline.pop_download_work(job_id);
    let queued_before = queued(&pipeline, job_id);

    let ack_rx = pipeline.finish_transport_batch(lease.id(), &[], vec![stray]);
    assert!(ack_rx.try_recv().is_ok());
    assert_eq!(pipeline.in_flight(job_id), 1);
    assert_eq!(queued(&pipeline, job_id), queued_before);

    let held = lease.works_mut().pop().unwrap();
    let _ack_rx = pipeline.finish_transport_batch(lease.id(), &[], vec![held]);
    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(queued(&pipeline, job_id), queued_before + 1);
    assert_gauges_match_lanes(&pipeline);
}

// T1, an article the lane already answered: a lease that names one article
// twice owes one result for it, so the second answer is refused and the
// count neither stays raised nor goes below zero.
#[tokio::test]
async fn a_lease_that_names_an_article_twice_owes_one_result() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41703);
    pipeline.add_files(job_id, "Named Twice", "twice", 2).await;
    let work = pipeline.pop_download_work(job_id);
    let segment_id = work.segment_id;
    let lease = pipeline.lease_works_to_new_lane(job_id, vec![work.clone(), work]);
    assert_eq!(pipeline.in_flight(job_id), 1);
    assert_eq!(pipeline.articles_in_flight(), 1);

    pipeline.fail_transport(lease.id(), segment_id).await;
    pipeline.fail_transport(lease.id(), segment_id).await;
    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_gauges_match_lanes(&pipeline);
}

// T1, a job that is gone: the cancel takes the job's articles off the lanes,
// so the late result is refused and nothing is owed to a job that no longer
// exists.
#[tokio::test]
async fn a_result_for_a_cancelled_job_is_refused_and_nothing_is_owed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41704);
    pipeline
        .add_files(job_id, "Cancelled", "cancelled", 2)
        .await;
    let lease = pipeline.lease_to_new_lane(job_id, 2);
    let held = lease.works()[0].segment_id;

    pipeline.cancel_job(job_id).await.unwrap();
    assert_eq!(pipeline.in_flight(job_id), 0);
    assert!(!pipeline.lane_owned(lease.id()));

    pipeline.fail_transport(lease.id(), held).await;
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_gauges_match_lanes(&pipeline);
}

// T1, an article taken back from a lane that has moved on: the field case of
// issue 97. The lane was re-booked to another job while still carrying this
// job's tail. The stall release takes the tail back to this job's queue and
// leaves the lane fetching for its new job; the tail's late result is
// refused.
#[tokio::test]
async fn a_stall_release_takes_back_the_tail_a_lane_carries_for_its_previous_job() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let tail_job = JobId(41705);
    let next_job = JobId(41706);
    pipeline.add_files(tail_job, "Tail", "tail", 1).await;
    pipeline.add_files(next_job, "Next", "next", 2).await;
    let lease = pipeline.lease_to_new_lane(tail_job, 1);
    let tail = lease.works()[0].segment_id;
    let refill = (0..2)
        .map(|_| pipeline.pop_download_work(next_job))
        .collect();
    pipeline.rebook_sequential_lane(lease.id(), next_job, refill);
    assert_eq!(pipeline.in_flight(tail_job), 1);
    assert_eq!(pipeline.in_flight(next_job), 2);
    assert_gauges_match_lanes(&pipeline);

    pipeline.mark_idle_past_stall(tail_job);
    pipeline.pause_stalled_downloads();
    assert_eq!(pipeline.job(tail_job).status, JobStatus::Paused);
    assert_eq!(pipeline.in_flight(tail_job), 0);
    assert_eq!(queued(&pipeline, tail_job), 1, "the tail is queued again");
    assert_eq!(pipeline.in_flight(next_job), 2);
    assert!(pipeline.lane_owned(lease.id()));
    assert_gauges_match_lanes(&pipeline);

    pipeline.fail_transport(lease.id(), tail).await;
    assert_eq!(pipeline.in_flight(tail_job), 0);
    assert_eq!(pipeline.in_flight(next_job), 2);
    assert_gauges_match_lanes(&pipeline);
}

// T2, a lease the pool stopped before running it: everything it held goes
// back to the queue and nothing stays owed.
#[tokio::test]
async fn a_stopped_lease_hands_back_everything_it_held() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41707);
    pipeline.add_files(job_id, "Stopped", "stopped", 3).await;
    let lease = pipeline.lease_to_new_lane(job_id, 3);
    let lane_id = lease.id();
    assert_eq!(pipeline.in_flight(job_id), 3);
    assert_eq!(pipeline.active_connections(), 1);

    pipeline.restore_stopped_lease(lease);

    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(pipeline.articles_in_flight(), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert_eq!(queued(&pipeline, job_id), 3);
    assert!(!pipeline.lane_owned(lane_id));
    assert_gauges_match_lanes(&pipeline);
}

// T2, a lane that fails mid-batch: its last batch answers one article and
// hands back the rest, then it parks on the error owing nothing.
#[tokio::test]
async fn a_lane_that_parks_on_error_owes_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41708);
    pipeline
        .add_files(job_id, "Error Park", "error-park", 3)
        .await;
    let mut lease = pipeline.lease_to_new_lane(job_id, 3);
    let lane_id = lease.id();
    let failed = lease.works_mut().remove(0);

    let ack_rx = pipeline.finish_transport_batch(
        lane_id,
        &[failed.segment_id],
        std::mem::take(lease.works_mut()),
    );
    assert!(ack_rx.try_recv().is_ok());
    pipeline.park_on_error(lane_id, job_id);

    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert!(!pipeline.lane_owned(lane_id));
    assert_gauges_match_lanes(&pipeline);
}

// T2, a stall release with work outstanding: the lane is parked on the
// error, the job's articles are queued again, and the job owes nothing.
#[tokio::test]
async fn a_stall_release_parks_the_lane_and_leaves_nothing_owed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41709);
    pipeline
        .add_files(job_id, "Stall Release", "stall-release", 3)
        .await;
    let lease = pipeline.lease_to_new_lane(job_id, 2);

    pipeline.mark_idle_past_stall(job_id);
    pipeline.pause_stalled_downloads();

    assert_eq!(pipeline.job(job_id).status, JobStatus::Paused);
    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(pipeline.active_connections(), 0);
    assert_eq!(queued(&pipeline, job_id), 3);
    assert!(!pipeline.lane_owned(lease.id()));
    assert_gauges_match_lanes(&pipeline);
}

// T3: the last article's result is released with the queue empty, so the
// job owes no download, its completion gate opens, and the stall watchdog
// has nothing to pause however long it waits.
#[tokio::test]
async fn a_job_whose_last_result_is_released_passes_its_gate_and_is_never_paused() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41710);
    let filename = "last-result.bin";
    let payload = b"the last article of the job".to_vec();
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Last Result", filename, &[payload.len() as u32]),
        )
        .await;
    let lease = pipeline.lease_to_new_lane(job_id, 1);
    assert_eq!(queued(&pipeline, job_id), 0);

    pipeline
        .download_decoded_file(lease.id(), lease.works()[0].segment_id, filename, &payload)
        .await;

    assert_eq!(pipeline.in_flight(job_id), 0);
    assert!(
        !pipeline.pending_download_work(job_id),
        "the job owes the download stage nothing"
    );
    pipeline.mark_idle_past_stall(job_id);
    pipeline.pause_stalled_downloads();
    assert_ne!(pipeline.job(job_id).status, JobStatus::Paused);
    assert_gauges_match_lanes(&pipeline);
}

// T4: paused with an article on a lane, the job keeps its articles while the
// article is out. The result lands while the job is paused and is taken, and
// the resume finds an empty queue and nothing held: the completion check runs
// in the resume pass, with no restart and nothing fetched again.
#[tokio::test]
async fn a_result_that_lands_while_paused_is_taken_and_the_resume_checks_completion_at_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41711);
    let filename = "paused-tail.bin";
    let payload = b"the tail that lands while paused".to_vec();
    pipeline
        .add_job(
            job_id,
            segmented_job_spec("Paused Tail", filename, &[payload.len() as u32]),
        )
        .await;
    let lease = pipeline.lease_to_new_lane(job_id, 1);
    assert_eq!(pipeline.job(job_id).status, JobStatus::Downloading);
    pipeline.pause_job_runtime(job_id).unwrap();
    assert_eq!(pipeline.in_flight(job_id), 1);
    assert!(pipeline.job(job_id).articles.active().is_some());

    pipeline
        .download_decoded_file(lease.id(), lease.works()[0].segment_id, filename, &payload)
        .await;
    assert_eq!(pipeline.in_flight(job_id), 0);
    assert_eq!(queued(&pipeline, job_id), 0);
    assert_eq!(
        pipeline.job(job_id).downloaded_bytes,
        payload.len() as u64,
        "the result is taken"
    );
    pipeline.clear_completion_checks();

    pipeline.resume_job_runtime(job_id).unwrap();

    assert_eq!(
        pipeline.completion_checks(),
        vec![job_id],
        "the resume pass queues the completion check"
    );
    assert!(!pipeline.pending_download_work(job_id));
    assert_gauges_match_lanes(&pipeline);
}

// T5: the assertion the dispatch fixtures run after every pass and batch can
// fail. An article counted with no lane holding it is reported, and the next
// audit point fails the test.
#[tokio::test]
#[should_panic(expected = "in-flight gauges disagree with what the live lanes hold")]
async fn the_lane_holdings_audit_fails_on_an_article_no_lane_holds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = DispatchFixture::new(&temp_dir).await;
    let job_id = JobId(41712);
    pipeline.add_files(job_id, "Phantom", "phantom", 1).await;
    assert_gauges_match_lanes(&pipeline);
    pipeline.count_unheld_article(job_id);
    assert_eq!(pipeline.lane_holdings_discrepancies().len(), 2);

    pipeline.dispatch();
}
