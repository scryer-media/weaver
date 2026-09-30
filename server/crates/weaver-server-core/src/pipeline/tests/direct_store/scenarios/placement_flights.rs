//! A routed article's destination writes run off the pipeline task. These pin
//! what that must not change: the pipeline keeps serving while a write is
//! held open, a demotion that overtakes a write hands its article back, and a
//! shutdown checkpoint does not run ahead of a write that is still out.

use super::*;

use std::sync::Arc;
use tokio::sync::Semaphore;

const ARTICLES: usize = 2;

fn fixture() -> Vec<(String, Vec<u8>)> {
    let payload: Vec<u8> = (0..120_000u32).map(|index| (index * 7 + 3) as u8).collect();
    single_member_store_set("feature.mkv", &payload, 3)
}

fn segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

fn committed(pipeline: &Pipeline, segment_id: SegmentId) -> bool {
    pipeline
        .jobs
        .get(&segment_id.file_id.job_id)
        .and_then(|state| state.assembly.file(segment_id.file_id))
        .is_some_and(|file| file.has_segment(segment_id.segment_number))
}

/// A pipeline with one direct set admitted and every placement task held at
/// its first step until the returned semaphore gets a permit.
async fn held_pipeline(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) -> (Pipeline, PathBuf, Arc<Semaphore>) {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", volumes, ARTICLES);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    (pipeline, working_dir, hold)
}

/// Delivers one decoded article the way the decode stage does, and returns
/// as soon as the pipeline does — without waiting for the placement it
/// starts, which is what every other submit helper settles.
async fn route_article(
    pipeline: &mut Pipeline,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
    file_index: u32,
    segment_number: u32,
) {
    take_queued_segment(
        pipeline,
        job_id,
        segment(job_id, file_index, segment_number),
    );
    let (filename, bytes) = &volumes[file_index as usize];
    let (start, end) = article_extent(bytes.len(), segment_number, ARTICLES);
    let data = &bytes[start..end];
    let file_id = NzbFileId { job_id, file_index };
    let total_segments = pipeline
        .jobs
        .get(&job_id)
        .and_then(|state| state.assembly.file(file_id))
        .expect("active test file assembly")
        .total_segments();
    let checkpoint_plan = pipeline.par2_checkpoint_plan(job_id);
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: segment(job_id, file_index, segment_number),
                raw_size: data.len() as u64,
                yenc_layout: YencLayoutAssertions {
                    file_size: bytes.len() as u64,
                    part: Some(segment_number + 1),
                    total: Some(total_segments),
                    begin: Some(start as u64 + 1),
                    end: Some(end as u64),
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(data),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(data.to_vec()),
                yenc_name: filename.clone(),
                checkpoint_plan,
                segments: vec![weaver_yenc::Segment {
                    file_offset: start as u64,
                    len: data.len() as u64,
                    crc32: par2_rs::checksum::crc32(data),
                }],
            },
            SegmentSource {
                source_server_idx: None,
                exclude_servers: Vec::new(),
            },
        )
        .await;
}

#[tokio::test]
async fn a_held_placement_write_leaves_the_pipeline_serving_and_commits_when_it_returns() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41801);
    let volumes = fixture();
    let (mut pipeline, _, hold) = held_pipeline(&temp_dir, job_id, &volumes).await;

    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    assert!(
        pipeline.direct_placement_lanes.contains_key(&(job_id, 0)),
        "premise: the first article routed bytes whose writes are now held open"
    );
    assert!(!committed(&pipeline, segment(job_id, 0, 0)));

    // What the select loop does with a pause, barrier demand included. Were
    // the pipeline task awaiting the held write, neither call would return.
    pipeline.demand_direct_store_barriers_for_pause(None).await;
    let (reply, paused) = tokio::sync::oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PauseAll { reply })
        .await;
    paused.await.unwrap();

    // The set's next article routes too, and queues behind the held one.
    route_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    assert!(!committed(&pipeline, segment(job_id, 0, 0)));
    assert!(!committed(&pipeline, segment(job_id, 0, 1)));

    hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;

    assert!(committed(&pipeline, segment(job_id, 0, 0)));
    assert!(committed(&pipeline, segment(job_id, 0, 1)));
    assert!(pipeline.direct_placement_lanes.is_empty());
    assert_eq!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .expect("the set is still direct")
            .volume_coverage(0)
            .end(),
        volumes[0].1.len() as u64,
        "both articles' writes are admitted as coverage once they return"
    );
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn a_placement_overtaken_by_a_demotion_is_dropped_and_its_article_handed_back() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41802);
    let volumes = fixture();
    let (mut pipeline, working_dir, hold) = held_pipeline(&temp_dir, job_id, &volumes).await;

    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    assert!(
        pipeline.direct_placement_lanes.contains_key(&(job_id, 0)),
        "premise: the article's writes are out"
    );

    // The write returns, but the set demotes before its done message is
    // handled: the demotion takes the article as a handoff.
    hold.add_permits(1);
    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded)
        .await;
    assert!(pipeline.direct_placement_lanes.is_empty());

    // The flight's done message still arrives, and is dropped.
    let done = pipeline
        .direct_placement_done_rx
        .recv()
        .await
        .expect("the placement channel stays open");
    assert_eq!((done.job_id, done.set_index), (job_id, 0));
    pipeline.handle_direct_placement_done(done).await;
    assert!(pipeline.direct_placement_lanes.is_empty());

    settle_direct_demotion_work(&mut pipeline).await;
    pipeline.flush_quiescent_write_backlog().await;

    // Handed back conventionally: written into the volume and committed there,
    // and not fetched a second time.
    assert!(committed(&pipeline, segment(job_id, 0, 0)));
    assert!(!queued_segments(&mut pipeline, job_id).contains(&(0, 0)));
    let (start, end) = article_extent(volumes[0].1.len(), 0, ARTICLES);
    let volume = std::fs::read(working_dir.join(&volumes[0].0)).unwrap();
    assert_eq!(&volume[start..end], &volumes[0].1[start..end]);
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn a_shutdown_checkpoint_waits_for_a_placement_that_is_still_writing() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41803);
    let volumes = fixture();
    let (mut pipeline, _, hold) = held_pipeline(&temp_dir, job_id, &volumes).await;

    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    assert!(
        pipeline.direct_placement_lanes.contains_key(&(job_id, 0)),
        "premise: the article's writes are out"
    );

    hold.add_permits(1);
    // No done message is handled here: the demand itself joins the flight.
    pipeline
        .demand_direct_store_barriers_for_all_jobs(BarrierDemand::Shutdown)
        .await;
    assert!(pipeline.direct_placement_lanes.is_empty());
    assert!(committed(&pipeline, segment(job_id, 0, 0)));
    let snapshot = coverage_snapshot_of(&pipeline, job_id);
    assert!(
        snapshot.floor_for_volume(0).is_some_and(|floor| floor > 0),
        "the checkpoint claims the joined article's bytes: {snapshot:?}"
    );

    // The done message that follows finds nothing left to do.
    let done = pipeline
        .direct_placement_done_rx
        .recv()
        .await
        .expect("the placement channel stays open");
    pipeline.handle_direct_placement_done(done).await;
    assert!(committed(&pipeline, segment(job_id, 0, 0)));
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

#[tokio::test]
async fn a_placement_task_that_panics_fails_its_placement_and_hands_the_article_back() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41804);
    let volumes = fixture();
    let (mut pipeline, working_dir, hold) = held_pipeline(&temp_dir, job_id, &volumes).await;
    pipeline.direct_placement_panics = true;

    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    assert!(
        pipeline.direct_placement_lanes.contains_key(&(job_id, 0)),
        "premise: the article's placement task is out"
    );

    // The task panics past the hold; its flight still resolves, as a failed
    // write, and the set demotes the way any failed write demotes it.
    hold.add_permits(1);
    settle_direct_demotion_work(&mut pipeline).await;
    pipeline.flush_quiescent_write_backlog().await;

    assert!(pipeline.direct_placement_lanes.is_empty());
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| set.is_demoted()),
        "a failed placement demotes its set"
    );
    // Handed back conventionally: written into the volume and committed
    // there, and not fetched a second time.
    assert!(committed(&pipeline, segment(job_id, 0, 0)));
    assert!(!queued_segments(&mut pipeline, job_id).contains(&(0, 0)));
    let (start, end) = article_extent(volumes[0].1.len(), 0, ARTICLES);
    let volume = std::fs::read(working_dir.join(&volumes[0].0)).unwrap();
    assert_eq!(&volume[start..end], &volumes[0].1[start..end]);
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

/// Swaps the hold new placement tasks wait at, and returns it closed.
fn hold_next_flights(pipeline: &mut Pipeline) -> Arc<Semaphore> {
    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    hold
}

/// Handles the next placement done message, as the select loop would.
async fn land_next_flight(pipeline: &mut Pipeline) {
    let done = pipeline
        .direct_placement_done_rx
        .recv()
        .await
        .expect("the placement channel stays open");
    pipeline.handle_direct_placement_done(done).await;
}

#[tokio::test]
async fn a_completed_volumes_trailing_region_lands_through_the_lane_while_the_pipeline_serves() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41805);
    let volumes = fixture();
    let (mut pipeline, _, open_hold) = held_pipeline(&temp_dir, job_id, &volumes).await;
    // The last volume is the one whose trailing region only its completion
    // can release: nothing after it can confirm it earlier.
    let last = (volumes.len() - 1) as u32;
    let volume_len = volumes[last as usize].1.len() as u64;

    open_hold.add_permits(1);
    for file_index in 0..last {
        for segment_number in 0..ARTICLES as u32 {
            route_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        }
    }
    settle_direct_placement_work(&mut pipeline).await;

    // The last volume's two articles route: the first writes, the second
    // queues behind it.
    let first_hold = hold_next_flights(&mut pipeline);
    route_article(&mut pipeline, job_id, &volumes, last, 0).await;
    route_article(&mut pipeline, job_id, &volumes, last, 1).await;
    let second_hold = hold_next_flights(&mut pipeline);
    first_hold.add_permits(1);
    land_next_flight(&mut pipeline).await;
    assert!(committed(&pipeline, segment(job_id, last, 0)));

    // The second article's landing completes the volume, and the region its
    // confirming parse releases goes out as a flight of its own.
    let tail_hold = hold_next_flights(&mut pipeline);
    second_hold.add_permits(1);
    land_next_flight(&mut pipeline).await;
    assert!(committed(&pipeline, segment(job_id, last, 1)));
    assert!(
        pipeline.direct_set_has_pending_volume_tail(job_id, 0),
        "premise: the completed volume's trailing region is writing"
    );
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set is still direct");
    let covered = set.volume_coverage(last).end();
    assert!(
        covered < volume_len,
        "the trailing region is not coverage before it lands: {covered} of {volume_len}"
    );
    assert!(
        !set.is_finalized(),
        "the set does not finalize ahead of its trailing region"
    );

    // The pipeline keeps serving while it writes: a pause, barrier demand
    // included, answers.
    pipeline.demand_direct_store_barriers_for_pause(None).await;
    let (reply, paused) = tokio::sync::oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::PauseAll { reply })
        .await;
    paused.await.unwrap();
    assert!(pipeline.direct_set_has_pending_volume_tail(job_id, 0));

    tail_hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;

    assert!(!pipeline.direct_set_has_pending_volume_tail(job_id, 0));
    let set = pipeline
        .direct_store
        .set(job_id, 0)
        .expect("the set is still known");
    assert!(!set.is_demoted());
    assert!(
        set.is_finalized(),
        "the landing finishes the volume's completion, and with every byte its \
         coverage the set finalizes"
    );
    assert_eq!(pipeline.write_buffered_bytes, 0);
}
