// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

pub fn compute_decode_backlog_budget_bytes(
    profile: &SystemProfile,
    base: usize,
    write: usize,
) -> usize {
    crate::pipeline::orchestrator::compute_decode_backlog_budget_bytes(profile, base, write)
}
pub fn compute_write_backlog_budget_bytes(profile: &SystemProfile, base: usize) -> usize {
    crate::pipeline::orchestrator::compute_write_backlog_budget_bytes(profile, base)
}
pub fn system_profile(pipeline: &Pipeline) -> &SystemProfile {
    pipeline.tuner.system_profile()
}
pub fn write_limit(pipeline: &Pipeline) -> usize {
    pipeline.write_backlog_budget_bytes
}
pub fn spool_limit(pipeline: &Pipeline) -> usize {
    pipeline.uu_spool_max_bytes
}
pub fn decode_limit(pipeline: &Pipeline) -> usize {
    pipeline.decode_backlog_budget_bytes
}
pub async fn run_queue_intents(pipeline: &mut Pipeline) {
    pipeline.run_queue_intents().await;
}
pub fn set_storage_classification(
    pipeline: &Pipeline,
    storage: StorageClass,
    filesystem: FilesystemType,
) {
    pipeline.shared_state.queue.write_intent(|inner| {
        inner.globals.storage_classification = Some((storage, filesystem));
        crate::pipeline::queue::Change::changed(())
    });
}

pub const MAX_RETRIES: u32 = MAX_SEGMENT_RETRIES;
pub fn queue_decode(pipeline: &mut Pipeline, id: SegmentId, raw: Bytes) {
    pipeline.pending_decode.push_back(PendingDecodeWork {
        segment_id: id,
        raw,
        servers: PoolServers::stamped(
            pipeline.pool_generation,
            None,
            weaver_nntp::ServerSet::EMPTY,
        ),
    });
}
pub fn pump(pipeline: &mut Pipeline) {
    pipeline.pump_decode_queue();
}
pub fn pending_count(pipeline: &Pipeline) -> usize {
    pipeline.pending_decode.len()
}
pub fn decode_threads(pipeline: &Pipeline) -> usize {
    pipeline.tuner.params().decode_thread_count
}
pub fn sum_active(pipeline: &Pipeline) -> usize {
    pipeline.active_decodes_by_job.values().sum()
}
pub fn walk_count(pipeline: &Pipeline) -> u64 {
    pipeline.active_decodes_by_job.walks()
}
pub fn note_started(pipeline: &mut Pipeline, id: SegmentId, bytes: u64) {
    pipeline.note_decode_started(id, bytes);
}
pub fn remove_active(pipeline: &mut Pipeline, id: JobId) {
    pipeline.active_decodes_by_job.remove(&id);
}
pub fn publish_active(pipeline: &Pipeline) {
    pipeline.publish_active_stage_metrics();
}
pub fn set_decode_limit(pipeline: &mut Pipeline, limit: usize) {
    pipeline.decode_backlog_budget_bytes = limit;
}
pub fn retry_exclusions(
    excluded: weaver_nntp::ServerSet,
    source: Option<usize>,
) -> weaver_nntp::ServerSet {
    Pipeline::decode_retry_exclude_servers(excluded, source)
}
pub fn decode_failure(
    pipeline: &mut Pipeline,
    id: SegmentId,
    message: &str,
    source: Option<usize>,
    excluded: weaver_nntp::ServerSet,
) {
    pipeline.handle_decode_failure(
        id,
        message,
        PoolServers::stamped(pipeline.pool_generation, source, excluded),
    );
}
pub fn installed_failed_bytes(pipeline: &Pipeline, id: JobId) -> Option<u64> {
    pipeline.jobs.get(&id).map(|state| state.failed_bytes)
}
pub fn pending_retry(pipeline: &Pipeline, id: SegmentId) -> bool {
    pipeline.pending_retries_by_segment.contains_key(&id)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Source(PoolServers);

pub fn source(
    pipeline: &Pipeline,
    index: Option<usize>,
    excluded: weaver_nntp::ServerSet,
) -> Source {
    Source(PoolServers::stamped(
        pipeline.pool_generation,
        index,
        excluded,
    ))
}
pub fn current_source(
    pipeline: &Pipeline,
    source: Source,
) -> (Option<usize>, weaver_nntp::ServerSet) {
    source
        .0
        .under(&pipeline.pool_layouts, pipeline.pool_generation)
}
pub fn generation(pipeline: &Pipeline) -> u64 {
    pipeline.pool_generation
}
pub fn rebuild(pipeline: &mut Pipeline, hosts: &[usize], connections: usize, generation: u64) {
    pipeline.activate_nntp(hosts_client(hosts), connections, generation);
}
pub fn fail_from(pipeline: &mut Pipeline, id: SegmentId, msg: &str, source: Source) {
    pipeline.handle_decode_failure(id, msg, source.0);
}
pub async fn next_generation_retry(pipeline: &mut Pipeline) -> (u64, DownloadWork) {
    let retry = pipeline
        .retry_rx
        .recv()
        .await
        .expect("the server that has not had it should draw a retry");
    (retry.scheduled_pool_generation, retry.work)
}
pub struct Completion(DecodeDone);
impl Completion {
    pub fn failure(&self) -> (SegmentId, u64, Source) {
        match &self.0 {
            DecodeDone::Failed {
                segment_id,
                raw_size,
                servers,
                ..
            } => (*segment_id, *raw_size, Source(*servers)),
            _ => panic!("expected decode failure"),
        }
    }
    pub async fn apply(self, pipeline: &mut Pipeline) {
        pipeline.handle_decode_done(self.0).await;
    }
}
pub async fn next_completion(pipeline: &mut Pipeline) -> Completion {
    Completion(
        pipeline
            .decode_done_rx
            .recv()
            .await
            .expect("decode channel should stay open"),
    )
}
pub fn no_completion_ready(pipeline: &mut Pipeline) -> bool {
    pipeline.decode_done_rx.try_recv().is_err()
}
pub fn is_paused(pipeline: &Pipeline) -> bool {
    pipeline.global_pause().is_some()
}
pub fn reported_progress(pipeline: &Pipeline, id: JobId) -> (u64, bool) {
    pipeline.shared_state.queue.read(|inner| {
        let job = &inner.job(id).expect("the job's row").report.job;
        (job.failed_bytes, job.finalizing_download)
    })
}
pub fn set_failed_bytes(pipeline: &mut Pipeline, id: JobId, bytes: u64) {
    pipeline.jobs.get_mut(&id).unwrap().failed_bytes = bytes;
}
pub fn set_finalizing(pipeline: &mut Pipeline, id: JobId) {
    pipeline.jobs_finalizing_download.insert(id);
}
pub fn report_soon(pipeline: &mut Pipeline, id: JobId) {
    pipeline.report_job_soon(id);
}
pub fn reports_due(pipeline: &Pipeline) -> &std::collections::HashSet<JobId> {
    &pipeline.reports_due
}
pub fn refresh_snapshot(pipeline: &mut Pipeline) {
    pipeline.refresh_periodic_snapshot();
}
pub fn queue_revision(pipeline: &Pipeline) -> u64 {
    pipeline.shared_state.queue.revision()
}
pub fn held_file(pipeline: &Pipeline, id: NzbFileId) -> bool {
    pipeline.unanchored_parked.contains_key(&id)
}
pub fn downloaded_bytes(pipeline: &Pipeline, id: JobId) -> Option<u64> {
    pipeline.jobs.get(&id).map(|s| s.downloaded_bytes)
}

pub fn park_for_backlog(pipeline: &mut Pipeline, file_id: NzbFileId) {
    let data = vec![3u8; 4096];
    let parked = DecodeResult {
        encoding: SegmentEncoding::Yenc,
        segment_id: SegmentId {
            file_id,
            segment_number: 1,
        },
        raw_size: 4096,
        yenc_layout: YencLayoutAssertions {
            file_size: 0,
            part: None,
            total: None,
            begin: None,
            end: None,
        },
        crc_valid: true,
        part_crc_verified: true,
        part_crc: par2_rs::checksum::crc32(&data),
        truncation_suspected: false,
        expected_file_crc: None,
        segments: Vec::new(),
        data: DecodedChunk::from(Vec::<u8>::new()),
        yenc_name: "stalled.bin".to_string(),
        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
    };
    let parked_len = data.len();
    pipeline.unanchored_parked.insert(
        file_id,
        1,
        crate::pipeline::UnanchoredPart {
            result: parked,
            bytes: crate::pipeline::ParkedBytes::Memory(DecodedChunk::from(data)),
            source: SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    None,
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        },
    );
    pipeline.note_write_buffered(parked_len, 1);
}

pub async fn write_blocked_file(pipeline: &mut Pipeline, file_id: NzbFileId) {
    pipeline
        .handle_decode_success(
            DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id: SegmentId {
                    file_id,
                    segment_number: 0,
                },
                raw_size: 4,
                yenc_layout: YencLayoutAssertions {
                    file_size: 4,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"fail"),
                truncation_suspected: false,
                expected_file_crc: None,
                segments: Vec::new(),
                data: DecodedChunk::from(b"fail".to_vec()),
                yenc_name: "blocked.bin".to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
            },
            SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    None,
                    weaver_nntp::ServerSet::EMPTY,
                ),
            },
        )
        .await;
    // The volume's writer says the write failed in the job's mail.
    pipeline.hear_graph_runs();
}

pub async fn submit_queued_layout_mismatch(
    pipeline: &mut Pipeline,
    segment_id: SegmentId,
    filename: &str,
) {
    pipeline.metrics.note_decode_task_started(4);
    pipeline
        .handle_decode_done(DecodeDone::Success {
            result: DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size: 4,
                yenc_layout: YencLayoutAssertions {
                    file_size: 4,
                    part: Some(1),
                    total: Some(1),
                    begin: Some(u64::MAX),
                    end: Some(4),
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"data"),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(b"data".to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            },
            source: SegmentSource {
                servers: PoolServers::stamped(
                    pipeline.pool_generation,
                    Some(1),
                    weaver_nntp::ServerSet::from([2]),
                ),
            },
        })
        .await;
}

pub async fn submit_fused_layout_mismatch(
    pipeline: &mut Pipeline,
    segment_id: SegmentId,
    filename: &str,
) {
    pipeline.articles_in_flight += 1;
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Decoded(DecodeResult {
                encoding: SegmentEncoding::Yenc,
                segment_id,
                raw_size: 8,
                // No declared size of its own, so nothing can justify a range
                // that runs past every envelope the job knows.
                yenc_layout: YencLayoutAssertions {
                    file_size: 0,
                    part: None,
                    total: None,
                    begin: None,
                    end: None,
                },
                crc_valid: true,
                part_crc_verified: true,
                part_crc: par2_rs::checksum::crc32(b"data!"),
                truncation_suspected: false,
                expected_file_crc: None,
                data: DecodedChunk::from(b"data!".to_vec()),
                yenc_name: filename.to_string(),
                checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                segments: Vec::new(),
            })),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: Some(2),
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::from([3]),
            release_connection_slot: true,
        })
        .await;
}

pub async fn submit_streamed_failure(pipeline: &mut Pipeline, segment_id: SegmentId) {
    pipeline.articles_in_flight += 1;
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Err(DownloadError::Decode {
                raw_size: 19,
                error: "missing =ybegin header".to_string(),
            }),
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

pub async fn raw_download(
    pipeline: &mut Pipeline,
    segment_id: SegmentId,
    raw: Bytes,
    index: Option<usize>,
) {
    pipeline.articles_in_flight += 1;
    pipeline
        .handle_download_done(DownloadResult {
            lane_id: 0,
            job_id: segment_id.file_id.job_id,
            runtime_generation: 0,
            segment_id,
            data: Ok(DownloadPayload::Raw(raw)),
            attempts: Vec::new(),
            lane_observation: None,
            source_server_idx: index,
            origin: DownloadResultOrigin::NormalPrimary,
            retry_count: 0,
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
            release_connection_slot: true,
        })
        .await;
}
