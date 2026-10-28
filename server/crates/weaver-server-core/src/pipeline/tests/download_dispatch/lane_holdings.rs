// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A job owes only the downloads its live lanes hold: every in-flight gauge is
// raised when an article enters a lane's record and lowered when it leaves,
// so no path can leave a job waiting on articles nobody is fetching.

use super::*;

// Lease `take` of the job's queued articles to a fresh lane, booked the way a
// dispatch pass books it.
fn lease_to_new_lane(pipeline: &mut Pipeline, job_id: JobId, take: usize) -> DownloadBatchLease {
    let state = pipeline.jobs.get_mut(&job_id).unwrap();
    let works: Vec<DownloadWork> = (0..take)
        .map(|_| {
            state
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .pop()
                .expect("the job has queued work")
        })
        .collect();
    lease_works_to_new_lane(pipeline, job_id, works)
}

fn lease_works_to_new_lane(
    pipeline: &mut Pipeline,
    job_id: JobId,
    works: Vec<DownloadWork>,
) -> DownloadBatchLease {
    let lease = DownloadBatchLease {
        lane_id: Pipeline::next_download_lane_id(),
        job_id,
        runtime_generation: pipeline.pool_generation,
        lane_mode: DownloadLaneMode::Sequential,
        server_modes: vec![(0, DownloadLaneMode::Sequential)],
        completion_critical: false,
        effective_exclude_servers: Vec::new(),
        dial_exclude_servers: Vec::new(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
        pressure_clear: true,
        works,
    };
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    lease
}

// A transport failure for `segment_id` from `lane_id`, which keeps its
// connection for the rest of its batch.
fn transport_failure(lane_id: u64, segment_id: SegmentId) -> DownloadResult {
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Err(DownloadError::fetch(
            DownloadFailureKind::EstablishedTransport,
            "connection reset",
        )),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: false,
    }
}

// The whole of a one-part file, already decoded by `lane_id`.
fn decoded_whole_file(
    lane_id: u64,
    segment_id: SegmentId,
    filename: &str,
    payload: &[u8],
) -> DownloadResult {
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Ok(DownloadPayload::Decoded(DecodeResult {
            encoding: SegmentEncoding::Yenc,
            segment_id,
            raw_size: payload.len() as u64,
            yenc_layout: YencLayoutAssertions {
                file_size: payload.len() as u64,
                part: Some(1),
                total: Some(1),
                begin: Some(1),
                end: Some(payload.len() as u64),
            },
            crc_valid: true,
            part_crc_verified: false,
            part_crc: par2_rs::checksum::crc32(payload),
            truncation_suspected: false,
            expected_file_crc: None,
            data: DecodedChunk::from(payload.to_vec()),
            yenc_name: filename.to_string(),
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        })),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: Vec::new(),
        release_connection_slot: true,
    }
}

fn batch_complete(
    lane_id: u64,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
) -> (OwnedDownloadLaneEvent, std::sync::mpsc::Receiver<()>) {
    let (ack, ack_rx) = std::sync::mpsc::sync_channel(1);
    (
        OwnedDownloadLaneEvent::BatchComplete {
            lane_id,
            results,
            unrequested_works,
            stats: weaver_nntp::blocking::BlockingLaneStats::default(),
            ack: Some(ack),
        },
        ack_rx,
    )
}

fn in_flight(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .active_downloads_by_job
        .get(&job_id)
        .copied()
        .unwrap_or(0)
}

fn assert_gauges_match_lanes(pipeline: &Pipeline) {
    let discrepancies = pipeline.lane_holdings_discrepancies();
    assert!(discrepancies.is_empty(), "{discrepancies:#?}");
}

fn queued(pipeline: &Pipeline, job_id: JobId) -> usize {
    pipeline
        .jobs
        .get(&job_id)
        .unwrap()
        .articles
        .active()
        .unwrap()
        .download_queue
        .len()
}

fn mark_idle_past_the_stall_threshold(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );
}

// T1, a lane that is not live: the result is refused, and the lane that does
// hold the article still owes it.
#[tokio::test]
async fn a_result_from_a_lane_that_is_not_live_moves_no_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41701);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Not Live", &many_standalone_files("not-live", 3)),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 2);
    let held = lease.works[0].segment_id;
    assert_eq!(in_flight(&pipeline, job_id), 2);

    let stranger = Pipeline::next_download_lane_id();
    pipeline
        .handle_download_done(transport_failure(stranger, held))
        .await;
    assert_eq!(in_flight(&pipeline, job_id), 2);
    assert_gauges_match_lanes(&pipeline);

    pipeline
        .handle_download_done(transport_failure(lease.lane_id, held))
        .await;
    assert_eq!(in_flight(&pipeline, job_id), 1);
    assert_gauges_match_lanes(&pipeline);
}

// T1, an article the lane does not hold: handing it back moves nothing and
// queues nothing, and the article the lane does hold comes back once.
#[tokio::test]
async fn an_article_the_lane_does_not_hold_is_refused_without_moving_a_gauge() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41702);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Not Held", &many_standalone_files("not-held", 3)),
    )
    .await;
    let mut lease = lease_to_new_lane(&mut pipeline, job_id, 1);
    let stray = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let queued_before = queued(&pipeline, job_id);

    let mut pending = VecDeque::new();
    let (event, ack_rx) = batch_complete(lease.lane_id, Vec::new(), vec![stray]);
    pipeline.handle_owned_download_lane_event(event, &mut pending);
    assert!(ack_rx.try_recv().is_ok());
    assert_eq!(in_flight(&pipeline, job_id), 1);
    assert_eq!(queued(&pipeline, job_id), queued_before);

    let held = lease.works.pop().unwrap();
    let (event, _ack_rx) = batch_complete(lease.lane_id, Vec::new(), vec![held]);
    pipeline.handle_owned_download_lane_event(event, &mut pending);
    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(queued(&pipeline, job_id), queued_before + 1);
    assert_gauges_match_lanes(&pipeline);
}

// T1, an article the lane already answered: a lease that names one article
// twice owes one result for it, so the second answer is refused and the
// count neither stays raised nor goes below zero.
#[tokio::test]
async fn a_lease_that_names_an_article_twice_owes_one_result() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41703);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Named Twice", &many_standalone_files("twice", 2)),
    )
    .await;
    let work = pipeline
        .jobs
        .get_mut(&job_id)
        .unwrap()
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .pop()
        .unwrap();
    let segment_id = work.segment_id;
    let lease = lease_works_to_new_lane(&mut pipeline, job_id, vec![work.clone(), work]);
    assert_eq!(in_flight(&pipeline, job_id), 1);
    assert_eq!(pipeline.active_downloads, 1);

    pipeline
        .handle_download_done(transport_failure(lease.lane_id, segment_id))
        .await;
    pipeline
        .handle_download_done(transport_failure(lease.lane_id, segment_id))
        .await;
    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(pipeline.active_downloads, 0);
    assert_gauges_match_lanes(&pipeline);
}

// T1, a job that is gone: the cancel takes the job's articles off the lanes,
// so the late result is refused and nothing is owed to a job that no longer
// exists.
#[tokio::test]
async fn a_result_for_a_cancelled_job_is_refused_and_nothing_is_owed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41704);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Cancelled", &many_standalone_files("cancelled", 2)),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 2);
    let held = lease.works[0].segment_id;

    cancel_one_job(
        &mut pipeline,
        job_id,
        crate::jobs::handle::CancellationOrigin::User,
    )
    .await
    .unwrap();
    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert!(!pipeline.download_lane_owners.contains_key(&lease.lane_id));

    pipeline
        .handle_download_done(transport_failure(lease.lane_id, held))
        .await;
    assert_eq!(pipeline.active_downloads, 0);
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
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let tail_job = JobId(41705);
    let next_job = JobId(41706);
    insert_active_job(
        &mut pipeline,
        tail_job,
        standalone_job_spec("Tail", &many_standalone_files("tail", 1)),
    )
    .await;
    insert_active_job(
        &mut pipeline,
        next_job,
        standalone_job_spec("Next", &many_standalone_files("next", 2)),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, tail_job, 1);
    let tail = lease.works[0].segment_id;
    let refill: Vec<DownloadWork> = (0..2)
        .map(|_| {
            pipeline
                .jobs
                .get_mut(&next_job)
                .unwrap()
                .articles
                .active_mut()
                .unwrap()
                .download_queue
                .pop()
                .unwrap()
        })
        .collect();
    pipeline.rebook_download_lane_owner(
        lease.lane_id,
        next_job,
        0,
        DownloadLaneMode::Sequential,
        false,
        refill,
    );
    assert_eq!(in_flight(&pipeline, tail_job), 1);
    assert_eq!(in_flight(&pipeline, next_job), 2);
    assert_gauges_match_lanes(&pipeline);

    mark_idle_past_the_stall_threshold(&mut pipeline, tail_job);
    pipeline.auto_pause_stalled_downloads();
    assert_eq!(
        pipeline.jobs.get(&tail_job).unwrap().status,
        JobStatus::Paused
    );
    assert_eq!(in_flight(&pipeline, tail_job), 0);
    assert_eq!(queued(&pipeline, tail_job), 1, "the tail is queued again");
    assert_eq!(in_flight(&pipeline, next_job), 2);
    assert!(pipeline.download_lane_owners.contains_key(&lease.lane_id));
    assert_gauges_match_lanes(&pipeline);

    pipeline
        .handle_download_done(transport_failure(lease.lane_id, tail))
        .await;
    assert_eq!(in_flight(&pipeline, tail_job), 0);
    assert_eq!(in_flight(&pipeline, next_job), 2);
    assert_gauges_match_lanes(&pipeline);
}

// T2, a lease the pool stopped before running it: everything it held goes
// back to the queue and nothing stays owed.
#[tokio::test]
async fn a_stopped_lease_hands_back_everything_it_held() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41707);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Stopped", &many_standalone_files("stopped", 3)),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 3);
    let lane_id = lease.lane_id;
    assert_eq!(in_flight(&pipeline, job_id), 3);
    assert_eq!(pipeline.active_download_connections, 1);

    pipeline.restore_stopped_owned_lane_lease(lease);

    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(pipeline.active_downloads, 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(queued(&pipeline, job_id), 3);
    assert!(!pipeline.download_lane_owners.contains_key(&lane_id));
    assert_gauges_match_lanes(&pipeline);
}

// T2, a lane that fails mid-batch: its last batch answers one article and
// hands back the rest, then it parks on the error owing nothing.
#[tokio::test]
async fn a_lane_that_parks_on_error_owes_nothing() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41708);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Error Park", &many_standalone_files("error-park", 3)),
    )
    .await;
    let mut lease = lease_to_new_lane(&mut pipeline, job_id, 3);
    let lane_id = lease.lane_id;
    let failed = lease.works.remove(0);

    let mut pending = VecDeque::new();
    let (event, ack_rx) = batch_complete(
        lane_id,
        vec![transport_failure(lane_id, failed.segment_id)],
        std::mem::take(&mut lease.works),
    );
    pipeline.handle_owned_download_lane_event(event, &mut pending);
    assert!(ack_rx.try_recv().is_ok());
    pipeline.handle_download_lane_parked(DownloadLaneParked {
        lane_id,
        job_id,
        mode: DownloadLaneMode::Sequential,
        completion_critical: false,
        reason: LaneParkReason::Error,
        release_connection_slot: true,
    });

    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert!(!pipeline.download_lane_owners.contains_key(&lane_id));
    assert_gauges_match_lanes(&pipeline);
}

// T2, a stall release with work outstanding: the lane is parked on the
// error, the job's articles are queued again, and the job owes nothing.
#[tokio::test]
async fn a_stall_release_parks_the_lane_and_leaves_nothing_owed() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41709);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Stall Release", &many_standalone_files("stall-release", 3)),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 2);

    mark_idle_past_the_stall_threshold(&mut pipeline, job_id);
    pipeline.auto_pause_stalled_downloads();

    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().status,
        JobStatus::Paused
    );
    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(pipeline.active_download_connections, 0);
    assert_eq!(queued(&pipeline, job_id), 3);
    assert!(!pipeline.download_lane_owners.contains_key(&lease.lane_id));
    assert_gauges_match_lanes(&pipeline);
}

// T3: the last article's result is released with the queue empty, so the
// job owes no download, its completion gate opens, and the stall watchdog
// has nothing to pause however long it waits.
#[tokio::test]
async fn a_job_whose_last_result_is_released_passes_its_gate_and_is_never_paused() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41710);
    let filename = "last-result.bin";
    let payload = b"the last article of the job".to_vec();
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Last Result", filename, &[payload.len() as u32]),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 1);
    assert_eq!(queued(&pipeline, job_id), 0);

    pipeline
        .handle_download_done(decoded_whole_file(
            lease.lane_id,
            lease.works[0].segment_id,
            filename,
            &payload,
        ))
        .await;

    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert!(
        !pipeline.job_has_pending_download_pipeline_work(job_id),
        "the job owes the download stage nothing"
    );
    mark_idle_past_the_stall_threshold(&mut pipeline, job_id);
    pipeline.auto_pause_stalled_downloads();
    assert_ne!(
        pipeline.jobs.get(&job_id).unwrap().status,
        JobStatus::Paused
    );
    assert_gauges_match_lanes(&pipeline);
}

// T4: paused with an article on a lane, the result lands while the job is
// paused, and the resume finds an empty queue and nothing held: the
// completion check runs in the resume pass, with no restart.
#[tokio::test]
async fn a_resume_with_nothing_queued_or_held_checks_completion_at_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41711);
    let filename = "paused-tail.bin";
    let payload = b"the tail that lands while paused".to_vec();
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Paused Tail", filename, &[payload.len() as u32]),
    )
    .await;
    let lease = lease_to_new_lane(&mut pipeline, job_id, 1);
    assert_eq!(
        pipeline.jobs.get(&job_id).unwrap().status,
        JobStatus::Downloading
    );
    pipeline.pause_job_runtime(job_id).unwrap();
    assert_eq!(in_flight(&pipeline, job_id), 1);

    pipeline
        .handle_download_done(decoded_whole_file(
            lease.lane_id,
            lease.works[0].segment_id,
            filename,
            &payload,
        ))
        .await;
    assert_eq!(in_flight(&pipeline, job_id), 0);
    assert_eq!(queued(&pipeline, job_id), 0);
    pipeline.pending_completion_checks.clear();

    pipeline.resume_job_runtime(job_id).unwrap();

    assert_eq!(
        pipeline
            .pending_completion_checks
            .iter()
            .copied()
            .collect::<Vec<_>>(),
        vec![job_id],
        "the resume pass queues the completion check"
    );
    assert!(!pipeline.job_has_pending_download_pipeline_work(job_id));
    assert_gauges_match_lanes(&pipeline);
}

// T5: the assertion the dispatch fixtures run after every pass and batch can
// fail. An article counted with no lane holding it is reported, and the next
// audit point fails the test.
#[tokio::test]
#[should_panic(expected = "in-flight gauges disagree with what the live lanes hold")]
async fn the_lane_holdings_audit_fails_on_an_article_no_lane_holds() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let job_id = JobId(41712);
    insert_active_job(
        &mut pipeline,
        job_id,
        standalone_job_spec("Phantom", &many_standalone_files("phantom", 1)),
    )
    .await;
    assert_gauges_match_lanes(&pipeline);
    pipeline.active_downloads += 1;
    *pipeline.active_downloads_by_job.entry(job_id).or_default() += 1;
    assert_eq!(pipeline.lane_holdings_discrepancies().len(), 2);

    pipeline.dispatch_downloads();
}
