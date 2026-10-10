// A routed article's destination writes run off the pipeline task. These pin
// what that must not change: the pipeline keeps serving while a write is
// held open, a demotion that overtakes a write hands its article back, and
// neither a shutdown checkpoint nor a PAR3 publication runs ahead of a write
// that is still out.

use super::*;

use std::sync::Arc;
use tokio::sync::Semaphore;

const ARTICLES: usize = 2;

mod schedules;

// A placement routed into a set retires the set's PAR3 sources before its
// writes leave. A refresh while those writes are held must not publish one
// over a destination about to change.
#[tokio::test]
async fn par3_publication_waits_for_direct_placement() {
    use crate::pipeline::repair::par3::work::Coordinator;

    let root = TempDir::new().unwrap();
    let job_id = JobId(41990);
    let volumes = fixture();
    let (mut pipeline, _, initial_hold) = held_pipeline(&root, job_id, &volumes).await;
    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    initial_hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        Arc::clone(&pipeline.metrics),
    );
    coordinator.admit(job_id).unwrap();
    pipeline.par3_runtime = Some(Box::new(coordinator));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;

    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    route_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    assert!(pipeline.has_direct_placements(job_id));
    assert!(!committed(&pipeline, segment(job_id, 0, 1)));
    pipeline.refresh_par3_sources(job_id).unwrap();
    let published_while_writing = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .has_worker_in_flight(job_id);
    settle_par3_work(&mut pipeline, job_id).await;
    hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;
    assert!(
        !published_while_writing,
        "PAR3 dispatched a source snapshot while its direct placement was held before writing"
    );
}

// Every volume of a split member reads the same destination, so a placement
// into one volume writes under the PAR3 images of the others. A refresh
// while it is out must not take a sibling's snapshot over that destination.
#[tokio::test]
async fn par3_sibling_snapshot_stays_fresh_after_placement() {
    use crate::pipeline::repair::par3::work::Coordinator;
    use par3_rs::source::SourceId;

    let root = TempDir::new().unwrap();
    let job_id = JobId(41992);
    let volumes = fixture();
    let (mut pipeline, _, initial_hold) = held_pipeline(&root, job_id, &volumes).await;
    initial_hold.add_permits(1);
    for ordinal in 0..2 {
        route_article(&mut pipeline, job_id, &volumes, 0, ordinal).await;
        settle_direct_placement_work(&mut pipeline).await;
    }
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        Arc::clone(&pipeline.metrics),
    );
    coordinator.admit(job_id).unwrap();
    pipeline.par3_runtime = Some(Box::new(coordinator));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    assert!(
        pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .source_snapshot(job_id, SourceId(0))
            .unwrap()
            .is_some()
    );

    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    route_article(&mut pipeline, job_id, &volumes, 1, 0).await;
    assert!(pipeline.has_direct_placements(job_id));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    let snapshot = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .source_snapshot(job_id, SourceId(0));
    assert!(
        matches!(snapshot, Ok(Some(_))),
        "a completed sibling retained a stale snapshot after another volume's write: {snapshot:?}"
    );
}

#[tokio::test]
async fn par3_partial_source_gains_committed_ranges() {
    par3_partial_publication(None).await;
}

#[tokio::test]
async fn par3_encrypted_source_gains_committed_ranges() {
    par3_partial_publication(Some(false)).await;
}

#[tokio::test]
async fn par3_keyed_checksum_encrypted_source_gains_committed_ranges() {
    par3_partial_publication(Some(true)).await;
}

// A volume's second article lands — its writes returned, its commit not yet
// applied — across a PAR3 refresh. The image published after the commit
// must read what the set has committed, not the coverage of before it.
// `encrypted` is `None` for a plain set, or whether an encrypted set keys
// its final part's checksum.
async fn par3_partial_publication(encrypted: Option<bool>) {
    use crate::pipeline::repair::par3::work::Coordinator;
    use par3_rs::source::SourceId;

    let root = TempDir::new().unwrap();
    let job_id = JobId(41993);
    let volumes = match encrypted {
        None => fixture(),
        Some(keyed_checksum) => {
            let payload: Vec<u8> = (0..120_000u32).map(|i| (i * 7 + 3) as u8).collect();
            encrypted_store_set(
                "feature.mkv",
                &payload,
                3,
                "moonlit-harbour",
                Some("moonlit-harbour"),
                keyed_checksum,
            )
        }
    };
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Publication diagnostic", &volumes, 3);
    if encrypted.is_some() {
        spec.password = Some("moonlit-harbour".into());
    }
    insert_active_job(&mut pipeline, job_id, spec).await;
    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    settle_direct_placement_work(&mut pipeline).await;
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        Arc::clone(&pipeline.metrics),
    );
    coordinator.admit(job_id).unwrap();
    pipeline.par3_runtime = Some(Box::new(coordinator));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    let before = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .source_ranges(job_id, SourceId(0))
        .unwrap();

    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    route_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    hold.add_permits(1);
    pipeline.await_direct_placement_io(job_id, 0).await;
    assert!(pipeline.has_direct_placements(job_id));
    assert!(!committed(&pipeline, segment(job_id, 0, 1)));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    settle_direct_placement_work(&mut pipeline).await;
    assert!(committed(&pipeline, segment(job_id, 0, 1)));
    assert!(!committed(&pipeline, segment(job_id, 0, 2)));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    let after = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .source_ranges(job_id, SourceId(0))
        .unwrap();
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    let len = set.virtual_volume_len(0, 0);
    let expected =
        set.virtual_volumes(&std::collections::BTreeMap::from([(0, len)]))[0].readable_ranges();
    assert_ne!(
        before, expected,
        "the committed article must grow readable coverage"
    );
    assert_eq!(
        after, expected,
        "PAR3 retained incomplete coverage after placement committed; before={before:?}, after={after:?}"
    );
}

// A demotion that overtakes a run of placements hands every one of those
// articles to the conventional path, where they wait in the volume's write
// buffer until the sweep's handback drains it. The volume completes when the
// last of them is written, and is read back for its checksum exactly once
// then — not once more for every handed-back article written after an
// earlier one already made the file look complete.
#[tokio::test]
async fn a_demotion_handback_hashes_its_volume_once() {
    let root = TempDir::new().unwrap();
    let job_id = JobId(41991);
    let payload: Vec<u8> = (0..1_200_000u32).map(|i| (i * 7 + 3) as u8).collect();
    let volumes = single_member_store_set("feature.mkv", &payload, 1);
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Demotion handback", &volumes, 12);
    let working = insert_active_job(&mut pipeline, job_id, spec).await;
    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    settle_direct_placement_work(&mut pipeline).await;
    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    for ordinal in 1..12 {
        route_article(&mut pipeline, job_id, &volumes, 0, ordinal).await;
    }
    assert!(pipeline.has_direct_placements(job_id));
    let path = working.join(&volumes[0].0);
    let before = Pipeline::completed_file_checksum_reads(&path);
    hold.add_permits(1);
    pipeline
        .demote_direct_set(job_id, 0, DemotionReason::HoldsBudgetExceeded)
        .await;
    assert!(pipeline.direct_demotion_in_flight.contains_key(&job_id));
    settle_direct_demotion_work(&mut pipeline).await;
    let reads = Pipeline::completed_file_checksum_reads(&path) - before;
    assert_eq!(std::fs::read(&path).unwrap(), volumes[0].1);
    assert_eq!(
        reads, 1,
        "a handback must not rehash the entire volume for every parked article"
    );
}

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

// A pipeline with one direct set admitted and every placement task held at
// its first step until the returned semaphore gets a permit.
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

// Delivers one decoded article the way the decode stage does, and returns
// as soon as the pipeline does — without waiting for the placement it
// starts, which is what every other submit helper settles.
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
    let articles = pipeline.jobs[&job_id].spec.files[file_index as usize]
        .segments
        .len();
    let (start, end) = article_extent(bytes.len(), segment_number, articles);
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

// Swaps the hold new placement tasks wait at, and returns it closed.
fn hold_next_flights(pipeline: &mut Pipeline) -> Arc<Semaphore> {
    let hold = Arc::new(Semaphore::new(0));
    pipeline.direct_placement_hold = Some(Arc::clone(&hold));
    hold
}

// Handles the next placement done message, as the select loop would.
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

// A pipeline with one direct set admitted, also returning the directory a
// finished member is published into. Placement tasks run free until a test
// swaps a closed hold in with [`hold_next_flights`].
async fn publishing_pipeline(
    temp_dir: &TempDir,
    job_id: JobId,
    volumes: &[(String, Vec<u8>)],
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, _, complete_dir) = new_direct_pipeline(temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", volumes, ARTICLES);
    let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
    (pipeline, working_dir, complete_dir)
}

// What the download stage does once a job's last article is in, and the
// completion checks that queues, run as the select loop's tick runs them.
// The placement lanes are left alone.
async fn finish_download_pass_and_check(pipeline: &mut Pipeline, job_id: JobId) {
    pipeline.maybe_finish_download_pass(job_id);
    while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
        pipeline.check_job_completion(queued).await;
    }
}

// Nothing judged the job: it is still downloading, with no extraction
// started over a set that has no volume on disk.
fn assert_still_active(pipeline: &Pipeline, job_id: JobId) {
    assert_eq!(
        job_status_for_assert(pipeline, job_id),
        Some(JobStatus::Downloading),
        "nothing may judge the job while a placement is still writing"
    );
    assert!(
        !pipeline.inflight_extractions.contains_key(&job_id),
        "nothing may extract a set whose placements are still writing"
    );
    assert!(
        pipeline.job_has_pending_download_pipeline_work(job_id),
        "a placement still writing is download work the job is owed"
    );
}

// The completion check that deferred to the placements has run again: the
// landing either ran it inline, after the set finalized, and the job is
// moving its output, or queued it for the next tick. Neither leaves a check
// parked on placements that are gone.
fn assert_carried_forward(pipeline: &Pipeline, job_id: JobId) {
    let status = job_status_for_assert(pipeline, job_id);
    assert!(
        status == Some(JobStatus::Moving) || pipeline.pending_completion_checks.contains(&job_id),
        "the last placement's landing carries the job on: {status:?}"
    );
    assert!(
        !pipeline
            .completion_checks_awaiting_placements
            .contains(&job_id)
    );
}

// Lands every placement, checks that the landing itself carried the job on,
// and drives it to its end.
async fn land_and_finish(
    pipeline: &mut Pipeline,
    job_id: JobId,
    complete_dir: &Path,
    working_dir: &Path,
    volumes: &[(String, Vec<u8>)],
    payload: &[u8],
) {
    settle_direct_placement_work(pipeline).await;
    assert!(pipeline.direct_placement_lanes.is_empty());
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| { !set.is_demoted() && set.is_finalized() }),
        "the last placement's landing completes the volume and finalizes the set"
    );
    assert_carried_forward(pipeline, job_id);

    drain_rar_refreshes(pipeline).await;
    drive_extractions_to_terminal(pipeline, job_id, 64).await;
    assert_eq!(
        job_status_for_assert(pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        member_after_gate(complete_dir, working_dir, "feature.mkv"),
        (Some(payload.to_vec()), Some("complete")),
        "the member is published from its direct partial"
    );
    assert!(
        no_volume_file(working_dir, volumes),
        "the job completed direct: no volume was ever assembled"
    );
    assert_eq!(pipeline.write_buffered_bytes, 0);
}

fn payload() -> Vec<u8> {
    (0..120_000u32).map(|index| (index * 7 + 3) as u8).collect()
}

#[tokio::test]
async fn a_completion_check_waits_for_the_placements_of_a_one_volume_set() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41806);
    let payload = payload();
    let volumes = single_member_store_set("feature.mkv", &payload, 1);
    let (mut pipeline, working_dir, complete_dir) =
        publishing_pipeline(&temp_dir, job_id, &volumes).await;
    let hold = hold_next_flights(&mut pipeline);

    // Every article of the only volume is downloaded and routed; none of
    // their writes has returned, so the volume has nothing recorded yet.
    for segment_number in 0..ARTICLES as u32 {
        route_article(&mut pipeline, job_id, &volumes, 0, segment_number).await;
    }
    assert!(
        pipeline.direct_placement_lanes.contains_key(&(job_id, 0)),
        "premise: the articles' writes are held open"
    );
    assert!(!committed(&pipeline, segment(job_id, 0, 0)));
    assert!(!committed(&pipeline, segment(job_id, 0, 1)));

    // The download pass is over. Read as drained, this check would find the
    // volume short of every article, no PAR2 to repair it, and fail the job.
    finish_download_pass_and_check(&mut pipeline, job_id).await;
    assert_still_active(&pipeline, job_id);
    assert!(!committed(&pipeline, segment(job_id, 0, 0)));

    hold.add_permits(1);
    land_and_finish(
        &mut pipeline,
        job_id,
        &complete_dir,
        &working_dir,
        &volumes,
        &payload,
    )
    .await;
}

#[tokio::test]
async fn a_completion_check_waits_for_the_trailing_region_of_a_sets_last_volume() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41807);
    let payload = payload();
    let volumes = single_member_store_set("feature.mkv", &payload, 3);
    let last = (volumes.len() - 1) as u32;
    let (mut pipeline, working_dir, complete_dir) =
        publishing_pipeline(&temp_dir, job_id, &volumes).await;

    // Every article but the very last lands.
    for file_index in 0..=last {
        for segment_number in 0..ARTICLES as u32 {
            if (file_index, segment_number) == (last, ARTICLES as u32 - 1) {
                continue;
            }
            route_article(&mut pipeline, job_id, &volumes, file_index, segment_number).await;
        }
    }
    settle_direct_placement_work(&mut pipeline).await;

    // The last article's own write returns, and its commit completes the
    // volume: the region that completion releases goes out as a flight of its
    // own, held open.
    route_article(&mut pipeline, job_id, &volumes, last, ARTICLES as u32 - 1).await;
    let tail_hold = hold_next_flights(&mut pipeline);
    land_next_flight(&mut pipeline).await;
    assert!(committed(
        &pipeline,
        segment(job_id, last, ARTICLES as u32 - 1)
    ));
    assert!(
        pipeline.direct_set_has_pending_volume_tail(job_id, 0),
        "premise: the completed volume's trailing region is writing"
    );

    // Every article is recorded, so the files read as whole; the set is not,
    // and only the trailing region's landing finalizes it.
    finish_download_pass_and_check(&mut pipeline, job_id).await;
    assert_still_active(&pipeline, job_id);
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && !set.is_finalized()),
        "the set neither demotes nor finalizes ahead of its trailing region"
    );

    tail_hold.add_permits(1);
    land_and_finish(
        &mut pipeline,
        job_id,
        &complete_dir,
        &working_dir,
        &volumes,
        &payload,
    )
    .await;
}

#[tokio::test]
async fn placements_a_barrier_settles_still_carry_a_deferred_completion_on() {
    let temp_dir = TempDir::new().unwrap();
    let job_id = JobId(41808);
    let payload = payload();
    let volumes = single_member_store_set("feature.mkv", &payload, 1);
    let (mut pipeline, working_dir, complete_dir) =
        publishing_pipeline(&temp_dir, job_id, &volumes).await;
    let hold = hold_next_flights(&mut pipeline);

    let first_flight = pipeline.next_direct_placement_flight_id;
    for segment_number in 0..ARTICLES as u32 {
        route_article(&mut pipeline, job_id, &volumes, 0, segment_number).await;
    }
    finish_download_pass_and_check(&mut pipeline, job_id).await;
    assert_still_active(&pipeline, job_id);

    // A demanded barrier applies the set's placements itself, the trailing
    // region's included, ahead of their done messages.
    hold.add_permits(1);
    pipeline
        .demand_direct_store_barriers(job_id, BarrierDemand::PhaseChange)
        .await;
    assert!(pipeline.direct_placement_lanes.is_empty());
    assert!(committed(
        &pipeline,
        segment(job_id, 0, ARTICLES as u32 - 1)
    ));
    assert!(
        pipeline
            .direct_store
            .set(job_id, 0)
            .is_some_and(|set| !set.is_demoted() && set.is_finalized()),
        "the settle completes the volume and finalizes the set"
    );

    // Every flight it settled still answers, and finds nothing left to do.
    let flights = pipeline.next_direct_placement_flight_id - first_flight;
    for _ in 0..flights {
        land_next_flight(&mut pipeline).await;
    }
    assert_carried_forward(&pipeline, job_id);

    drain_rar_refreshes(&mut pipeline).await;
    drive_extractions_to_terminal(&mut pipeline, job_id, 64).await;
    assert_eq!(
        job_status_for_assert(&pipeline, job_id),
        Some(JobStatus::Complete)
    );
    assert_eq!(
        member_after_gate(&complete_dir, &working_dir, "feature.mkv"),
        (Some(payload.clone()), Some("complete"))
    );
    assert!(no_volume_file(&working_dir, &volumes));
}

// A refresh held back while a placement was out is owed once it lands, and
// no completion check is bound to follow the landing while the job still
// downloads: the landing itself publishes what the refresh did not.
#[tokio::test]
async fn a_placement_landing_publishes_the_par3_sources_it_held_back() {
    use crate::pipeline::repair::par3::work::Coordinator;
    use par3_rs::source::SourceId;

    let root = TempDir::new().unwrap();
    let job_id = JobId(41994);
    let volumes = fixture();
    let (mut pipeline, _, _) = new_direct_pipeline(&root).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, 3);
    insert_active_job(&mut pipeline, job_id, spec).await;
    route_article(&mut pipeline, job_id, &volumes, 0, 0).await;
    settle_direct_placement_work(&mut pipeline).await;
    let mut coordinator = Coordinator::new(
        pipeline.repair_work_done_tx.clone(),
        Arc::clone(&pipeline.metrics),
    );
    coordinator.admit(job_id).unwrap();
    pipeline.par3_runtime = Some(Box::new(coordinator));
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;

    let hold = hold_next_flights(&mut pipeline);
    route_article(&mut pipeline, job_id, &volumes, 0, 1).await;
    pipeline.refresh_par3_sources(job_id).unwrap();
    let runtime = pipeline.par3_runtime.as_ref().unwrap();
    assert!(
        !runtime.has_worker_in_flight(job_id),
        "premise: the refresh held the set's publication back"
    );
    assert!(
        runtime
            .source_ranges(job_id, SourceId(0))
            .unwrap()
            .is_empty(),
        "premise: the routing withdrew the first volume's image"
    );

    hold.add_permits(1);
    settle_direct_placement_work(&mut pipeline).await;
    assert!(committed(&pipeline, segment(job_id, 0, 1)));
    assert!(
        !committed(&pipeline, segment(job_id, 0, 2)),
        "premise: the volume is still short, so no completion of it publishes anything"
    );
    assert!(
        pipeline
            .par3_runtime
            .as_ref()
            .unwrap()
            .has_worker_in_flight(job_id),
        "the landing must publish what the refresh before it held back"
    );
    settle_par3_work(&mut pipeline, job_id).await;
    let published = pipeline
        .par3_runtime
        .as_ref()
        .unwrap()
        .source_ranges(job_id, SourceId(0))
        .unwrap();
    let set = pipeline.direct_store.set(job_id, 0).unwrap();
    let len = set.virtual_volume_len(0, 0);
    let expected =
        set.virtual_volumes(&std::collections::BTreeMap::from([(0, len)]))[0].readable_ranges();
    assert_eq!(
        published, expected,
        "the first volume's image must read both landed articles"
    );
}

// A direct set protected by PAR3, with one article of its middle volume
// held at its placement while everything else of the set has landed, and
// the middle volume's last article lost for good.
//
// A completion check's PAR3 refresh runs while the held article is out —
// before its write lands when `landed_first` is false, after the write
// returned but before its done message is applied when it is true — and
// the next one after the landing. The verdict the runtime then holds must
// be the one a fresh publication of everything committed gives: the lost
// article and nothing more.
async fn par3_refresh_across_a_held_placement(job_id: JobId, landed_first: bool) {
    const ARTICLES_EACH: usize = 8;
    let payload: Vec<u8> = (0..48_000u32).map(|index| (index * 13 + 5) as u8).collect();
    let volumes = single_member_store_set("feature.mkv", &payload, 3);
    let carriers = par3_carriers_over(&volumes, PAR2_SLICE_BYTES, 64);
    let index = carriers
        .iter()
        .position(|(filename, _)| !filename.contains(".vol"))
        .expect("the PAR3 index");

    let temp_dir = TempDir::new().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    pipeline.direct_store.set_gate(DirectStoreGate::Enabled);
    let mut spec = direct_store_job_spec_with_articles("Silver Horizon", &volumes, ARTICLES_EACH);
    let carrier_indices = append_single_article_files(&mut spec, &carriers);
    insert_active_job(&mut pipeline, job_id, spec).await;

    let (filename, bytes) = &carriers[index];
    submit_decoded_segment_declaring(
        &mut pipeline,
        NzbFileId {
            job_id,
            file_index: carrier_indices[index],
        },
        0,
        0,
        bytes,
        filename,
        None,
        true,
        None,
        bytes.len() as u64,
    )
    .await;
    settle_par3_work(&mut pipeline, job_id).await;

    let lost = (1u32, ARTICLES_EACH as u32 - 1);
    let held = (1u32, 3u32);
    for ordinal in 0..volumes.len() as u32 {
        for article in 0..ARTICLES_EACH as u32 {
            if (ordinal, article) == lost || (ordinal, article) == held {
                continue;
            }
            submit_volume_article_indexed_of(
                &mut pipeline,
                job_id,
                &volumes,
                ordinal,
                ordinal,
                article,
                ARTICLES_EACH,
            )
            .await;
            pipeline.refresh_par3_sources(job_id).unwrap();
            settle_par3_work(&mut pipeline, job_id).await;
        }
    }
    assert!(
        par3_lost_blocks(&pipeline, job_id).is_some_and(|lost| lost > 0),
        "premise: the runtime holds a view over the set before the held article"
    );

    let hold = hold_next_flights(&mut pipeline);
    route_article(&mut pipeline, job_id, &volumes, held.0, held.1).await;
    assert!(
        pipeline.direct_set_has_placements(job_id, 0),
        "premise: the held article's writes are out"
    );
    if landed_first {
        // The writes return; nothing has applied them.
        hold.add_permits(1);
        pipeline.await_direct_placement_io(job_id, 0).await;
        assert!(!committed(&pipeline, segment(job_id, held.0, held.1)));
    }

    // A completion check between the routing and the commit.
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;

    if !landed_first {
        hold.add_permits(1);
    }
    settle_direct_placement_work(&mut pipeline).await;
    assert!(committed(&pipeline, segment(job_id, held.0, held.1)));

    // The completion check after the landing.
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    let reached = par3_lost_blocks(&pipeline, job_id).expect("a settled PAR3 view");

    // Every image retired and published afresh over what is committed.
    pipeline.invalidate_par3_direct_set(job_id, 0);
    pipeline.refresh_par3_sources(job_id).unwrap();
    settle_par3_work(&mut pipeline, job_id).await;
    let fresh = par3_lost_blocks(&pipeline, job_id).expect("a settled PAR3 view");

    assert!(fresh > 0, "premise: the lost article is real damage");
    assert_eq!(
        reached, fresh,
        "the held article's blocks verify once it lands: an image published while \
         its write was out must not stand (lost {reached}, fresh publication {fresh})"
    );
}

#[tokio::test]
async fn a_par3_refresh_while_a_placement_is_writing_does_not_miss_its_bytes() {
    par3_refresh_across_a_held_placement(JobId(41809), false).await;
}

#[tokio::test]
async fn a_par3_refresh_before_a_landed_placement_is_applied_does_not_miss_its_bytes() {
    par3_refresh_across_a_held_placement(JobId(41811), true).await;
}
