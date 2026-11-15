// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A volume's hard buffer limit is a budget the volume never goes over. An
// article is reserved on its job's volume, at its size in the post, as it is
// leased to a lane; the reservation is given back exactly once however the
// article leaves the lane, and what arrived for it counts in its place until
// the writer has written it. The one exception: a volume that counts nothing
// grants one article, whatever its size.

use super::*;
use crate::operations::disk::volume::Volume;

const ARTICLE: u32 = 512;

fn volume_of(pipeline: &Pipeline, job_id: JobId) -> Arc<Volume> {
    Arc::clone(
        pipeline.graph_jobs[&job_id]
            .disk()
            .expect("the job writes through its volume's writer")
            .working()
            .volume(),
    )
}

fn filename(job_id: JobId) -> String {
    format!("budget-{}.bin", job_id.0)
}

async fn add_job(pipeline: &mut Pipeline, job_id: JobId, articles: usize) {
    insert_active_job(
        pipeline,
        job_id,
        segmented_job_spec(
            &format!("Budget {}", job_id.0),
            &filename(job_id),
            &vec![ARTICLE; articles],
        ),
    )
    .await;
}

// The bytes every live lane's articles are reserved at, against what the
// pipeline holds reserved for them: one and the same, always.
fn assert_reservations_match_lanes(pipeline: &Pipeline) {
    let held: u64 = pipeline
        .download_lane_owners
        .values()
        .flat_map(|owner| owner.outstanding.values())
        .map(|work| Pipeline::reserved_bytes_of(work.byte_estimate))
        .sum();
    let reserved: u64 = pipeline
        .volume_reserved_by_job
        .values()
        .map(|(_, bytes)| *bytes)
        .sum();
    assert_eq!(reserved, held, "reservations disagree with the lanes");
}

// Cut one lease for server 0 as a dispatch pass does and book it on a lane.
fn book_lease(pipeline: &mut Pipeline) -> Option<DownloadBatchLease> {
    let lease = pipeline.lease_for_server_for_test(0)?;
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    Some(lease)
}

// The decoded result of `work`, from `lane_id`: the article whole, at its
// place in its file.
fn arrived(pipeline: &Pipeline, lane_id: u64, work: &DownloadWork) -> DownloadResult {
    let segment_id = work.segment_id;
    let file = pipeline.jobs[&segment_id.file_id.job_id]
        .articles
        .assembly()
        .and_then(|assembly| assembly.file(segment_id.file_id))
        .expect("active test file assembly");
    let offset = u64::from(segment_id.segment_number) * u64::from(ARTICLE);
    let payload = vec![segment_id.segment_number as u8; ARTICLE as usize];
    DownloadResult {
        lane_id,
        job_id: segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id,
        data: Ok(DownloadPayload::Decoded(DecodeResult {
            encoding: SegmentEncoding::Yenc,
            segment_id,
            raw_size: u64::from(ARTICLE),
            yenc_layout: YencLayoutAssertions {
                file_size: file.total_bytes(),
                part: Some(segment_id.segment_number + 1),
                total: Some(file.total_segments()),
                begin: Some(offset + 1),
                end: Some(offset + u64::from(ARTICLE)),
            },
            crc_valid: true,
            part_crc_verified: false,
            part_crc: par2_rs::checksum::crc32(&payload),
            truncation_suspected: false,
            expected_file_crc: None,
            data: DecodedChunk::from(payload),
            yenc_name: filename(segment_id.file_id.job_id),
            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            segments: Vec::new(),
        })),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

fn failed(lane_id: u64, work: &DownloadWork, kind: DownloadFailureKind) -> DownloadResult {
    DownloadResult {
        lane_id,
        job_id: work.segment_id.file_id.job_id,
        runtime_generation: 0,
        segment_id: work.segment_id,
        data: Err(DownloadError::fetch(kind, "refused")),
        attempts: Vec::new(),
        lane_observation: None,
        source_server_idx: Some(0),
        origin: DownloadResultOrigin::NormalPrimary,
        retry_count: 0,
        exclude_servers: weaver_nntp::ServerSet::EMPTY,
        release_connection_slot: false,
    }
}

// A result comes back the way the loop takes one in: released, booked as
// waiting, then handed on.
async fn hand_in(pipeline: &mut Pipeline, result: DownloadResult) {
    if !pipeline.release_download_result(&result) {
        return;
    }
    pipeline.note_released_download_result_pending(
        result.job_id,
        Pipeline::released_download_result_lead_bytes(&result),
    );
    pipeline.process_released_download_done(result).await;
}

// Wait until the volume's writer has written everything the job queued.
fn written(pipeline: &Pipeline, job_id: JobId) {
    pipeline.graph_jobs[&job_id]
        .disk()
        .unwrap()
        .barrier()
        .wait();
}

fn batch_complete(
    lane_id: u64,
    results: Vec<DownloadResult>,
    unrequested_works: Vec<DownloadWork>,
) -> OwnedDownloadLaneEvent {
    OwnedDownloadLaneEvent::BatchComplete {
        lane_id,
        results,
        unrequested_works,
        stats: weaver_nntp::blocking::BlockingLaneStats::default(),
        ack: None,
    }
}

// A pipeline with one job of `articles` on a volume sized at `hard`.
async fn sized_job(temp_dir: &TempDir, job_id: JobId, articles: usize, hard: u64) -> Pipeline {
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(temp_dir, 16 * 1024 * 1024, 8).await;
    add_job(&mut pipeline, job_id, articles).await;
    pipeline.volume_buffers.set_figure(hard, u64::MAX);
    assert_eq!(volume_of(&pipeline, job_id).buffer_limits().1, hard);
    pipeline
}

// Arrived: the reservation becomes what arrived, in one step, and the
// writer's write takes it off.
#[tokio::test]
async fn an_article_that_arrives_counts_once_until_it_is_written() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42101);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let lease = book_lease(&mut pipeline).expect("room for a lease");
    let leased = lease.works.len() as u64 * u64::from(ARTICLE);
    assert_eq!(volume.unwritten_bytes(), leased);
    assert_reservations_match_lanes(&pipeline);
    for work in &lease.works {
        let result = arrived(&pipeline, lease.lane_id, work);
        hand_in(&mut pipeline, result).await;
        assert!(volume.unwritten_bytes() <= leased);
        assert_reservations_match_lanes(&pipeline);
    }
    written(&pipeline, job_id);
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    assert!(pipeline.volume_arrived_by_job.is_empty());
    assert_eq!(volume.peak_bytes(), leased);
}

// Failed, missing, or to be tried on another server: the reservation is
// given back whole, and taken again when the article is leased again.
#[tokio::test]
async fn an_article_that_fails_or_is_missing_gives_its_reservation_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42102);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let lease = book_lease(&mut pipeline).expect("room for a lease");
    assert!(lease.works.len() >= 2);
    let kinds = [
        DownloadFailureKind::EstablishedTransport,
        DownloadFailureKind::ArticleNotFound,
    ];
    let mut left = lease.works.len() as u64 * u64::from(ARTICLE);
    for (work, kind) in lease.works.iter().zip(kinds) {
        pipeline
            .handle_download_done(failed(lease.lane_id, work, kind))
            .await;
        left -= u64::from(ARTICLE);
        assert_eq!(volume.unwritten_bytes(), left);
        assert_reservations_match_lanes(&pipeline);
    }
    for work in lease.works.iter().skip(2) {
        pipeline
            .handle_download_done(failed(
                lease.lane_id,
                work,
                DownloadFailureKind::EstablishedTransport,
            ))
            .await;
    }
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    // Leased again, on whichever server: reserved again.
    let again = book_lease(&mut pipeline).expect("the articles are leased again");
    assert_eq!(
        volume.unwritten_bytes(),
        again.works.len() as u64 * u64::from(ARTICLE)
    );
    assert_reservations_match_lanes(&pipeline);
}

// Returned unasked by the lane, or the whole lease handed back when the
// pool stops: given back once.
#[tokio::test]
async fn an_article_a_lane_returns_gives_its_reservation_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42103);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let mut first = book_lease(&mut pipeline).expect("room for a lease");
    let returned = first.works.pop().unwrap();
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        batch_complete(first.lane_id, Vec::new(), vec![returned.clone()]),
        &mut pending,
    );
    assert_reservations_match_lanes(&pipeline);
    // The same article returned twice is given back once.
    pipeline.handle_owned_download_lane_event(
        batch_complete(first.lane_id, Vec::new(), vec![returned]),
        &mut pending,
    );
    assert_reservations_match_lanes(&pipeline);
    let second = book_lease(&mut pipeline).expect("room for a second lease");
    pipeline.restore_stopped_owned_lane_lease(second);
    pipeline.restore_stopped_owned_lane_lease(first);
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
}

// A lane that parks on an error hands back what it held.
#[tokio::test]
async fn a_lane_that_parks_on_an_error_gives_its_reservations_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42104);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let mut lease = book_lease(&mut pipeline).expect("room for a lease");
    let first = lease.works.remove(0);
    let mut pending = VecDeque::new();
    pipeline.handle_owned_download_lane_event(
        batch_complete(
            lease.lane_id,
            vec![failed(
                lease.lane_id,
                &first,
                DownloadFailureKind::EstablishedTransport,
            )],
            std::mem::take(&mut lease.works),
        ),
        &mut pending,
    );
    pipeline.handle_download_lane_parked(DownloadLaneParked {
        lane_id: lease.lane_id,
        job_id,
        mode: lease.lane_mode,
        completion_critical: false,
        reason: LaneParkReason::Error,
        release_connection_slot: true,
    });
    for result in pending {
        pipeline.process_released_download_done(result).await;
    }
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    assert_reservations_match_lanes(&pipeline);
}

// A lane that cannot get a connection hands its lease back whether the
// refusal says "ask again" (the articles go back to the queue) or is a
// verdict (each article takes the failure): either way the volume counts
// nothing for them, and a volume with room for one lease grants the next.
#[tokio::test]
async fn a_lane_refused_a_connection_gives_its_reservations_back() {
    let errors = [
        weaver_nntp::client::BlockingBodyLaneAcquireError::LocalCapacity,
        weaver_nntp::client::BlockingBodyLaneAcquireError::Other(weaver_nntp::NntpError::Timeout),
    ];
    for (index, error) in errors.into_iter().enumerate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let job_id = JobId(42120 + index as u64);
        let mut pipeline = sized_job(&temp_dir, job_id, 40, 8 * u64::from(ARTICLE)).await;
        let volume = volume_of(&pipeline, job_id);
        let lease = book_lease(&mut pipeline).expect("room for a lease");
        assert!(volume.unwritten_bytes() > 0);
        assert_reservations_match_lanes(&pipeline);
        let mut pending = VecDeque::new();
        pipeline.handle_owned_download_lane_event(
            OwnedDownloadLaneEvent::AcquireFailed { lease, error },
            &mut pending,
        );
        for result in pending {
            pipeline.process_released_download_done(result).await;
        }
        assert_eq!(volume.unwritten_bytes(), 0);
        assert!(pipeline.volume_reserved_by_job.is_empty());
        assert_reservations_match_lanes(&pipeline);
        book_lease(&mut pipeline).expect("the volume has room for the next lease");
        assert_reservations_match_lanes(&pipeline);
    }
}

// A cancelled job's articles come off its lanes, and their reservations
// with them; the late result is refused and counts nothing.
#[tokio::test]
async fn a_cancelled_job_gives_its_reservations_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42105);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let lease = book_lease(&mut pipeline).expect("room for a lease");
    assert!(volume.unwritten_bytes() > 0);
    cancel_one_job(
        &mut pipeline,
        job_id,
        crate::jobs::handle::CancellationOrigin::User,
    )
    .await
    .unwrap();
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    let late = failed(
        lease.lane_id,
        &lease.works[0],
        DownloadFailureKind::EstablishedTransport,
    );
    pipeline.handle_download_done(late).await;
    assert_eq!(volume.unwritten_bytes(), 0);
}

// A job paused by the stall watchdog has its articles taken back to its
// queue, and their reservations given back.
#[tokio::test]
async fn a_job_paused_with_articles_out_gives_their_reservations_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42106);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    book_lease(&mut pipeline).expect("room for a lease");
    pipeline.job_last_download_activity.insert(
        job_id,
        std::time::Instant::now() - STALLED_DOWNLOAD_IDLE_THRESHOLD - Duration::from_secs(1),
    );
    pipeline.auto_pause_stalled_downloads();
    assert_eq!(pipeline.jobs[&job_id].status, JobStatus::Paused);
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
}

// A lease whose lane went away before it was answered is rolled back, and
// its reservations with it.
#[tokio::test]
async fn a_lease_rolled_back_gives_its_reservations_back() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42107);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let lane_id = book_lane(&mut pipeline, job_id);
    let (response_tx, response_rx) = oneshot::channel();
    drop(response_rx);
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        lane_id,
        runtime_generation: 0,
        server_idx: 0,
        supports_pipelining: false,
        current_mode: DownloadLaneMode::Sequential,
        response_tx,
    });
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    assert_reservations_match_lanes(&pipeline);
}

// A lease that names one article twice holds it once and reserves it once.
#[tokio::test]
async fn an_article_named_twice_is_reserved_once() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42108);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, 64 * u64::from(ARTICLE)).await;
    let volume = volume_of(&pipeline, job_id);
    let mut lease = pipeline
        .lease_for_server_for_test(0)
        .expect("room for a lease");
    for work in lease.works.split_off(1) {
        pipeline.release_lane_work(&work);
    }
    let work = lease.works[0].clone();
    assert_eq!(volume.unwritten_bytes(), u64::from(ARTICLE));
    // Reserved again for the copy, as a lease cut twice would be.
    assert!(pipeline.reserve_lane_work(&work));
    lease.works.push(work.clone());
    pipeline.activate_download_batch_lease(&lease, &Pipeline::activation_items(&lease), true);
    for _ in 0..2 {
        pipeline
            .handle_download_done(failed(
                lease.lane_id,
                &work,
                DownloadFailureKind::EstablishedTransport,
            ))
            .await;
    }
    assert_eq!(volume.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    assert_reservations_match_lanes(&pipeline);
}

// The one exception: a volume that counts nothing grants one article larger
// than its whole limit, and nothing more while it counts it.
#[tokio::test]
async fn an_empty_volume_grants_one_article_larger_than_its_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42109);
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8).await;
    insert_active_job(
        &mut pipeline,
        job_id,
        segmented_job_spec("Budget Large", "large.bin", &[4096, 4096, 4096]),
    )
    .await;
    pipeline.volume_buffers.set_figure(1024, u64::MAX);
    let volume = volume_of(&pipeline, job_id);
    let lease = book_lease(&mut pipeline).expect("an empty volume grants one article");
    assert_eq!(lease.works.len(), 1);
    assert_eq!(volume.unwritten_bytes(), 4096);
    assert!(lease_for_test_none(&mut pipeline), "nothing past it");
    assert_eq!(volume.peak_bytes(), 4096);
    pipeline
        .handle_download_done(failed(
            lease.lane_id,
            &lease.works[0],
            DownloadFailureKind::EstablishedTransport,
        ))
        .await;
    assert_eq!(volume.unwritten_bytes(), 0);
    assert_eq!(
        book_lease(&mut pipeline).map(|lease| lease.works.len()),
        Some(1)
    );
}

// A result released and booked as waiting for the loop, not yet handed on.
fn release_waiting(pipeline: &mut Pipeline, result: &DownloadResult) {
    assert!(pipeline.release_download_result(result));
    pipeline.note_released_download_result_pending(
        result.job_id,
        Pipeline::released_download_result_lead_bytes(result),
    );
}

// A job's working volume changes while it has bytes counted on the old one,
// arrived and waiting for the loop, and leases reserved on the new one: each
// volume gives back what it counted, and both end counting nothing.
#[tokio::test]
async fn a_job_whose_volume_changes_gives_each_volume_back_what_it_counted() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42110);
    let hard = 64 * u64::from(ARTICLE);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, hard).await;
    let first = volume_of(&pipeline, job_id);
    let lease = book_lease(&mut pipeline).expect("room for a lease");
    // One article arrives and waits for the loop; the rest come back.
    let early = arrived(&pipeline, lease.lane_id, &lease.works[0]);
    release_waiting(&mut pipeline, &early);
    for work in &lease.works[1..] {
        pipeline
            .handle_download_done(failed(
                lease.lane_id,
                work,
                DownloadFailureKind::EstablishedTransport,
            ))
            .await;
    }
    assert_eq!(first.unwritten_bytes(), u64::from(ARTICLE));
    assert!(pipeline.volume_reserved_by_job.is_empty());

    // The job's files are written to another volume from here.
    let second = Arc::new(Volume::new(Some(u64::MAX - 1), "/moved".into()));
    second.set_buffer_limits(hard, hard);
    let submitter = pipeline.volume_writers.submitter(&second, job_id.0);
    pipeline
        .graph_jobs
        .get_mut(&job_id)
        .unwrap()
        .move_disk_for_test(crate::pipeline::graph::disk::JobDisk::new(submitter, None));
    let later = book_lease(&mut pipeline).expect("room on the second volume");
    let leased = later.works.len() as u64 * u64::from(ARTICLE);
    assert_eq!(second.unwritten_bytes(), leased);
    // One of them arrives too, and waits with the first: both count on the
    // second volume now, and the first counts nothing.
    let next = arrived(&pipeline, later.lane_id, &later.works[0]);
    release_waiting(&mut pipeline, &next);
    assert_eq!(first.unwritten_bytes(), 0);
    assert_eq!(second.unwritten_bytes(), leased + u64::from(ARTICLE));

    pipeline.process_released_download_done(early).await;
    pipeline.process_released_download_done(next).await;
    for work in &later.works[1..] {
        pipeline
            .handle_download_done(failed(
                later.lane_id,
                work,
                DownloadFailureKind::EstablishedTransport,
            ))
            .await;
    }
    written(&pipeline, job_id);
    assert_eq!(first.unwritten_bytes(), 0);
    assert_eq!(second.unwritten_bytes(), 0);
    assert!(pipeline.volume_reserved_by_job.is_empty());
    assert!(pipeline.volume_arrived_by_job.is_empty());
}

// A lane carrying the tail of another job, of larger articles, asked last
// for a job whose volume has room for a few of its own smaller ones: it
// takes those, sized by the job's articles, not by what it carries, and is
// never held to a line it cannot reach.
#[tokio::test]
async fn a_lane_carrying_another_jobs_larger_tail_takes_what_fits_of_the_next() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8).await;
    let tail_job = JobId(42111);
    insert_active_job(
        &mut pipeline,
        tail_job,
        segmented_job_spec("Budget Tail", "tail.bin", &[4096]),
    )
    .await;
    let hard = 96 * u64::from(ARTICLE);
    pipeline.volume_buffers.set_figure(hard, u64::MAX);
    let tail = book_lease(&mut pipeline).expect("room for the tail");
    assert_eq!(tail.job_id, tail_job);
    let job_id = JobId(42112);
    add_job(&mut pipeline, job_id, 40).await;
    let volume = volume_of(&pipeline, job_id);
    assert!(Arc::ptr_eq(&volume, &volume_of(&pipeline, tail_job)));
    // Room below the lease limit for six of the job's articles and none of
    // the tail's, with the volume at or under its resume line: no room wake
    // is asked for the job's next article.
    let room = 6 * u64::from(ARTICLE) + 428;
    volume.count_for_test(volume.lease_limit() - room);
    assert!(volume.unwritten_bytes() <= volume.resume_line());
    let mut answer = ask(&mut pipeline, tail.lane_id);
    let lease = answer
        .try_recv()
        .expect("answered at once")
        .lease
        .expect("the lane takes what fits");
    assert_eq!(lease.job_id, job_id);
    assert!(!lease.works.is_empty() && lease.works.len() <= 6);
    assert!(volume.unwritten_bytes() <= volume.lease_limit());
}

// A volume whose limit holds only a few articles: lanes take what fits until
// nothing more does, the next lane waits for the volume's room, is not
// answered while the volume stays full, and is woken and served once the
// writer has written.
#[tokio::test]
async fn lanes_on_a_volume_of_a_few_articles_take_what_fits_then_wait_for_room() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(42113);
    let hard = 6 * u64::from(ARTICLE);
    let mut pipeline = sized_job(&temp_dir, job_id, 40, hard).await;
    let volume = volume_of(&pipeline, job_id);
    let fits = (volume.lease_limit() / u64::from(ARTICLE)) as usize;
    let mut leases = Vec::new();
    let mut waiting = loop {
        assert!(leases.len() <= fits, "more leases than articles fit");
        let lane_id = book_lane(&mut pipeline, job_id);
        let mut answer = ask(&mut pipeline, lane_id);
        assert!(volume.unwritten_bytes() <= volume.lease_limit());
        match answer.try_recv() {
            Ok(response) => leases.push(response.lease.expect("the lane takes what fits")),
            Err(_) => break answer,
        }
    };
    let taken: usize = leases.iter().map(|lease| lease.works.len()).sum();
    assert_eq!(taken, fits, "every article that fits was taken");
    let (holds, wakes) = room_counts(&pipeline);
    assert_eq!((holds, wakes), (1, 0), "the last lane waits for room");
    pipeline.service_held_download_refills();
    assert!(waiting.try_recv().is_err(), "still full: not answered");
    for lease in &leases {
        for work in &lease.works {
            let result = arrived(&pipeline, lease.lane_id, work);
            hand_in(&mut pipeline, result).await;
            assert!(volume.unwritten_bytes() <= hard);
        }
    }
    written(&pipeline, job_id);
    assert_eq!(volume.unwritten_bytes(), 0);
    pipeline.service_held_download_refills();
    let woken = waiting
        .try_recv()
        .expect("woken once the volume has room")
        .lease
        .expect("served from the room");
    assert!(!woken.works.is_empty() && woken.works.len() <= fits);
    assert!(volume.unwritten_bytes() <= volume.lease_limit());
    assert_eq!(room_counts(&pipeline).1, 1);
}

fn lease_for_test_none(pipeline: &mut Pipeline) -> bool {
    pipeline.lease_for_server_for_test(0).is_none()
}

// A lane booked on server 0, holding a connection, with nothing held.
fn book_lane(pipeline: &mut Pipeline, job_id: JobId) -> u64 {
    let lane_id = Pipeline::next_download_lane_id();
    pipeline.download_lane_owners.insert(
        lane_id,
        DownloadLaneOwner {
            body_activity: Vec::new(),
            job_id,
            mode: DownloadLaneMode::Pipelined { depth: 8 },
            completion_critical: false,
            server_idx: Some(0),
            connection: true,
            outstanding: HashMap::new(),
            outstanding_bytes: 0,
        },
    );
    pipeline.active_download_connections += 1;
    *pipeline
        .active_download_connections_by_job
        .entry(job_id)
        .or_default() += 1;
    lane_id
}

fn ask(pipeline: &mut Pipeline, lane_id: u64) -> oneshot::Receiver<DownloadLaneRefillResponse> {
    let (response_tx, response_rx) = oneshot::channel();
    pipeline.handle_download_lane_refill_request(DownloadLaneRefillRequest {
        lane_id,
        runtime_generation: 0,
        server_idx: 0,
        supports_pipelining: true,
        current_mode: DownloadLaneMode::Pipelined { depth: 8 },
        response_tx,
    });
    response_rx
}

fn room_counts(pipeline: &Pipeline) -> (u64, u64) {
    (
        pipeline
            .metrics
            .download_lane_room_held_total
            .load(Ordering::Relaxed),
        pipeline
            .metrics
            .download_lane_room_woken_total
            .load(Ordering::Relaxed),
    )
}

// Several lanes at pipe depth 8, for several jobs on one volume whose writer
// is held: the counted bytes never pass the limit, at any step. Lanes lease
// until nothing fits, every result is handed into the held writer, and the
// lanes left without room wait for the volume. Once the writer writes, the
// lanes are woken and served again, still within the limit. Every job gets
// lanes, and each held lane is woken once, not per write.
#[tokio::test]
async fn lanes_against_a_full_volume_never_count_past_its_limit() {
    // Each volume's writer runs on its own thread, as outside tests, so the
    // hold stands the writer still and never the loop.
    let _threads = crate::pipeline::graph::disk::threads::real();
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) =
        new_direct_pipeline_with_backlog(&temp_dir, 16 * 1024 * 1024, 8).await;
    pipeline.nntp = Arc::new(NntpClient::new(NntpClientConfig {
        servers: vec![weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: "budget.example.invalid".to_string(),
                port: 119,
                tls: false,
                pipelining: weaver_nntp::PipeliningCapability::Known(true),
                pipelining_depth: Some(8),
                ..Default::default()
            },
            max_connections: 8,
            ..Default::default()
        }],
        max_idle_age: Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: Duration::from_secs(1),
    }));
    pipeline.seed_download_lane_explorers();
    pipeline.tuner.set_connection_limit(8);
    let jobs = [JobId(42201), JobId(42202), JobId(42203)];
    // The first two jobs fit within the volume's limit and the third does
    // not: it is served from the room the others leave, then from what the
    // writer frees.
    for job_id in jobs {
        add_job(&mut pipeline, job_id, 40).await;
    }
    let volume = volume_of(&pipeline, jobs[0]);
    for job_id in &jobs[1..] {
        assert!(Arc::ptr_eq(&volume, &volume_of(&pipeline, *job_id)));
    }
    let hard = 96 * u64::from(ARTICLE);
    pipeline.volume_buffers.set_figure(hard, u64::MAX);
    assert_eq!(volume.buffer_limits().1, hard);
    let within = |volume: &Volume| {
        assert!(
            volume.unwritten_bytes() <= hard,
            "{} counted against {hard}",
            volume.unwritten_bytes()
        );
    };
    let hold = pipeline.volume_writers.hold(&volume);
    let lanes: Vec<u64> = (0..8).map(|_| book_lane(&mut pipeline, jobs[0])).collect();
    let mut served = std::collections::HashSet::new();
    let mut held = Vec::new();

    // The writer is held: lanes lease and hand in what they fetch until the
    // volume is full and every lane waits for it.
    loop {
        let mut leased = false;
        for &lane_id in &lanes {
            if held.iter().any(|(id, _)| *id == lane_id) {
                continue;
            }
            let mut answer = ask(&mut pipeline, lane_id);
            within(&volume);
            assert_reservations_match_lanes(&pipeline);
            match answer.try_recv() {
                Ok(response) => {
                    let Some(lease) = response.lease else {
                        continue;
                    };
                    assert!(lease.works.len() <= pipeline.download_refill_want(lease.lane_mode));
                    served.insert(lease.job_id);
                    leased = true;
                    for work in &lease.works {
                        let result = arrived(&pipeline, lane_id, work);
                        hand_in(&mut pipeline, result).await;
                        within(&volume);
                    }
                    assert_reservations_match_lanes(&pipeline);
                }
                Err(_) => held.push((lane_id, answer)),
            }
        }
        if !leased {
            break;
        }
    }
    assert!(!held.is_empty(), "lanes wait for the full volume");
    assert!(volume.unwritten_bytes() > volume.resume_line());
    assert!(served.len() >= 2, "a second job got lanes: {served:?}");
    let (holds, wakes) = room_counts(&pipeline);
    assert_eq!(wakes, 0);

    // Passes while the writer is still held answer nobody.
    for _ in 0..3 {
        pipeline.service_held_download_refills();
    }
    assert!(
        held.iter_mut()
            .all(|(_, answer)| answer.try_recv().is_err())
    );
    assert_eq!(room_counts(&pipeline), (holds, 0));

    // The writer writes everything: the volume falls past its line, and
    // the held lanes are woken, at most once per hold, and served within
    // the limit.
    hold.open();
    for job_id in jobs {
        written(&pipeline, job_id);
    }
    assert!(volume.unwritten_bytes() <= volume.resume_line());
    pipeline.service_held_download_refills();
    within(&volume);
    let mut answered = 0;
    let mut woken_for = std::collections::HashSet::new();
    for (_, answer) in &mut held {
        if let Ok(response) = answer.try_recv() {
            answered += 1;
            if let Some(lease) = response.lease {
                woken_for.insert(lease.job_id);
            }
        }
    }
    // The woken lanes took what was left; a lane still held is an idle
    // lane, with nothing left to hand it.
    assert!(answered > 0, "no held lane was answered");
    if answered < held.len() {
        for job_id in jobs {
            assert_eq!(
                pipeline.jobs[&job_id]
                    .articles
                    .download_queue()
                    .map(|queue| queue.len()),
                Some(0),
                "a lane is held while {job_id:?} has work"
            );
        }
    }
    // The room the writer freed went to the job still with work.
    assert!(
        woken_for.contains(&jobs[2]),
        "the last job got the freed room: {woken_for:?}"
    );
    let (_, wakes) = room_counts(&pipeline);
    assert!(
        wakes > 0 && wakes <= holds,
        "{wakes} wakes for {holds} holds"
    );
    assert!(volume.peak_bytes() <= hard, "peak {}", volume.peak_bytes());
    assert_reservations_match_lanes(&pipeline);
}
