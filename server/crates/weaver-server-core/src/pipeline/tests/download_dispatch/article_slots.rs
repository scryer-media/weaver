// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One article slot per part number: the slot fetches its first message-id and
// moves to the next only once the id in use is spent.

use super::*;

const FILENAME: &str = "slot.bin";
const FIRST: &str = "first@example.invalid";
const SECOND: &str = "second@example.invalid";

fn slot_segment(job_id: JobId) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id,
            file_index: 0,
        },
        segment_number: 0,
    }
}

// A one-server pipeline holding a job whose first part carries `ids`. The
// second part keeps the job downloading whatever happens to the first.
async fn slot_pipeline(temp_dir: &tempfile::TempDir, job_id: JobId, ids: &[&str]) -> Pipeline {
    let (mut pipeline, _, _) = new_direct_pipeline(temp_dir).await;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let mut spec = segmented_job_spec("Article Slot", FILENAME, &[128; 10]);
    spec.files[0].segments[0].message_ids =
        crate::jobs::MessageIds::new(ids.iter().map(|id| id.to_string()).collect());
    insert_active_job(&mut pipeline, job_id, spec).await;
    pipeline
}

// The ids queued for the slot, taken off the queues as a lane would lease them.
fn lease_slot(pipeline: &mut Pipeline, job_id: JobId) -> Vec<(String, Vec<usize>)> {
    let segment_id = slot_segment(job_id);
    let Some(state) = pipeline.jobs.get_mut(&job_id) else {
        return Vec::new();
    };
    let mut leased = state
        .articles
        .active_mut()
        .unwrap()
        .download_queue
        .extract_matching(|work| work.segment_id == segment_id);
    leased.extend(
        state
            .articles
            .active_mut()
            .unwrap()
            .recovery_queue
            .extract_matching(|work| work.segment_id == segment_id),
    );
    leased
        .into_iter()
        .map(|work| (work.message_id.0.to_string(), work.exclude_servers))
        .collect()
}

async fn finish_download(
    pipeline: &mut Pipeline,
    job_id: JobId,
    data: Result<DownloadPayload, DownloadError>,
) {
    hold_on_test_lane(pipeline, &[slot_segment(job_id)]);
    pipeline.active_download_passes.insert(job_id);
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id,
            runtime_generation: 0,
            segment_id: slot_segment(job_id),
            data,
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(0),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: Vec::new(),
            release_connection_slot: true,
        })
        .await;
}

fn slot_body() -> Bytes {
    encode_article_part(FILENAME, &[7u8; 128], 1, 2, 1, 256)
}

fn not_found() -> Result<DownloadPayload, DownloadError> {
    Err(DownloadError::fetch(
        DownloadFailureKind::ArticleNotFound,
        "article not found",
    ))
}

fn articles_not_found(pipeline: &Pipeline) -> u64 {
    pipeline
        .metrics
        .articles_not_found
        .load(std::sync::atomic::Ordering::Relaxed)
}

#[tokio::test]
async fn a_crc_failure_on_the_first_id_fetches_the_second() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40302);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);

    // One body byte changed: the article decodes to the wrong bytes and
    // fails its part CRC.
    let mut body = slot_body().to_vec();
    let lines_end = body
        .windows(2)
        .enumerate()
        .filter(|(_, pair)| *pair == b"\r\n")
        .nth(1)
        .map(|(at, _)| at + 2)
        .unwrap();
    body[lines_end] = if body[lines_end] == b'a' { b'b' } else { b'a' };
    finish_download(
        &mut pipeline,
        job_id,
        Ok(DownloadPayload::Raw(Bytes::from(body))),
    )
    .await;
    drain_decode_results(&mut pipeline, 1).await;

    // The second id goes out to every server, the one that served the bad
    // body included: the id is new, the server is not at fault.
    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
}

#[tokio::test]
async fn a_first_id_missing_on_every_server_fetches_the_second() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40303);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);

    finish_download(&mut pipeline, job_id, not_found()).await;

    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    assert_eq!(articles_not_found(&pipeline), 0);
    assert_eq!(pipeline.jobs[&job_id].failed_bytes, 0);
}

// Every id spent is a lost article, booked exactly as a single-id article
// that every server is missing.
#[tokio::test]
async fn a_slot_with_every_id_spent_is_lost_like_a_single_id() {
    let temp_dir = tempfile::tempdir().unwrap();
    let job_id = JobId(40304);
    let mut pipeline = slot_pipeline(&temp_dir, job_id, &[FIRST, SECOND]).await;
    lease_slot(&mut pipeline, job_id);
    finish_download(&mut pipeline, job_id, not_found()).await;
    assert_eq!(
        lease_slot(&mut pipeline, job_id),
        [(SECOND.to_string(), vec![])]
    );
    finish_download(&mut pipeline, job_id, not_found()).await;

    let single_dir = tempfile::tempdir().unwrap();
    let single_id = JobId(40305);
    let mut single = slot_pipeline(&single_dir, single_id, &[FIRST]).await;
    lease_slot(&mut single, single_id);
    finish_download(&mut single, single_id, not_found()).await;

    for (pipeline, job_id) in [(&mut pipeline, job_id), (&mut single, single_id)] {
        assert!(lease_slot(pipeline, job_id).is_empty());
        assert_eq!(articles_not_found(pipeline), 1);
        assert!(
            pipeline
                .jobs
                .get(&job_id)
                .is_none_or(|state| state.failed_bytes == 128),
            "the lost article's bytes are booked failed (the job may already be archived)"
        );
    }
}
