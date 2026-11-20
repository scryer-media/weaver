// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const FILENAME: &str = "slot.bin";

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
pub async fn slot_pipeline(
    temp_dir: &tempfile::TempDir,
    job_id: JobId,
    ids: &[&str],
) -> DispatchFixture {
    let mut fixture = DispatchFixture::new(temp_dir).await;
    let pipeline = &mut fixture.0;
    pipeline.nntp = std::sync::Arc::new(retention_client(&[0]));
    let mut spec = segmented_job_spec("Article Slot", FILENAME, &[128; 10]);
    spec.files[0].segments[0].message_ids =
        crate::jobs::MessageIds::new(ids.iter().map(|id| id.to_string()).collect());
    insert_active_job(pipeline, job_id, spec).await;
    fixture
}

// The ids queued for the slot, taken off the queues as a lane would lease them.
pub fn lease_slot(fixture: &mut DispatchFixture, job_id: JobId) -> Vec<(String, Vec<usize>)> {
    let pipeline = &mut fixture.0;
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
        .map(|item| item.dispatched(&state.spec))
        .map(|work| (work.message_id.0.to_string(), work.exclude_servers.to_vec()))
        .collect()
}

async fn finish_download(
    pipeline: &mut Pipeline,
    job_id: JobId,
    data: Result<DownloadPayload, DownloadError>,
) {
    super::results::hold_on_test_lane(pipeline, &[slot_segment(job_id)]);
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
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        })
        .await;
}

pub fn slot_body() -> Bytes {
    encode_article_part(FILENAME, &[7u8; 128], 1, 2, 1, 256)
}

pub async fn finish_slot_body(fixture: &mut DispatchFixture, job_id: JobId, body: Bytes) {
    finish_download(&mut fixture.0, job_id, Ok(DownloadPayload::Raw(body))).await;
}

pub async fn finish_slot_missing(fixture: &mut DispatchFixture, job_id: JobId) {
    finish_download(
        &mut fixture.0,
        job_id,
        Err(DownloadError::fetch(
            DownloadFailureKind::ArticleNotFound,
            "article not found",
        )),
    )
    .await;
}

impl DispatchFixture {
    pub async fn drain_decode(&mut self, expected: usize) {
        drain_decode_results(&mut self.0, expected).await;
    }
}
