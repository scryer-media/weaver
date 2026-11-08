// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// Every pipeline these tests build checks, after each dispatch pass and each
// lane result batch, that its in-flight gauges are what its live lanes hold.
// These shadow the shared fixtures of the same names for this module tree.
async fn new_direct_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, intermediate_dir, complete_dir) = super::new_direct_pipeline(temp_dir).await;
    pipeline.audit_lane_holdings = true;
    (pipeline, intermediate_dir, complete_dir)
}

async fn new_direct_pipeline_with_buffers(
    temp_dir: &TempDir,
    buffer_config: BufferPoolConfig,
    total_connections: usize,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, intermediate_dir, complete_dir) =
        super::new_direct_pipeline_with_buffers(temp_dir, buffer_config, total_connections).await;
    pipeline.audit_lane_holdings = true;
    (pipeline, intermediate_dir, complete_dir)
}

// Hand `segments` to lane 0 as a dispatched lease would, so the result a
// test then injects from lane 0 releases an article a lane holds.
fn hold_on_test_lane(pipeline: &mut Pipeline, segments: &[SegmentId]) {
    pipeline.hold_test_lane_works(held_works(segments), false);
}

// Ordinary payload works for `segments`, as a lease would carry them.
fn held_works(segments: &[SegmentId]) -> Vec<DownloadWork> {
    segments
        .iter()
        .map(|&segment_id| DownloadWork {
            counters: None,
            segment_id,
            message_id: MessageId::new(&format!(
                "held-{:?}-{}-{}@example.com",
                segment_id.file_id.job_id, segment_id.file_id.file_index, segment_id.segment_number
            )),
            groups: std::sync::Arc::from(vec!["alt.binaries.test".to_string()]),
            priority: 0,
            byte_estimate: 1024,
            retry_count: 0,
            is_recovery: false,
            completion_critical: false,
            exclude_servers: Vec::new(),
            avoid_server: None,
        })
        .collect()
}

// Segment `segment_number` of file `file_index` of `job_id`.
fn held_segment(job_id: JobId, file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId { job_id, file_index },
        segment_number,
    }
}

// Seconds since the epoch, as the NZB's `date` attribute carries it.
fn now_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0)
}

// A one-file job whose files carry `posted_at` (or no date at all).
fn posted_job_spec(name: &str, posted_at: Option<u64>) -> JobSpec {
    let mut spec = standalone_job_spec(name, &[("queued.bin".to_string(), 512u32)]);
    for file in &mut spec.files {
        file.posted_at_epoch = posted_at;
    }
    spec
}

mod article_scheduler;
mod article_slots;
mod checkpoint_progress;
mod credentials_refusal;
mod dispatch_ineligible_log;
mod dispatch_server_order;
mod emits_download_pipeline_drained;
mod first_article_gate;
mod gapless_lanes;
mod lane_depth_seeding;
mod lane_failure_visibility;
mod lane_holdings;
mod owned_download_lane_pool;
mod propagation_delay;
mod stall_recovery;
mod stall_retry;
