// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use super::dispatch_fixture::{
    audited_pipeline as new_direct_pipeline,
    audited_pipeline_with_backlog as new_direct_pipeline_with_backlog,
};

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
            exclude_servers: weaver_nntp::ServerSet::EMPTY,
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

use super::propagation_fixture::now_epoch_secs;

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
mod blocking_lane_guards;
mod busy_turn_completions;
mod checkpoint_progress;
mod credentials_refusal;
mod emits_download_pipeline_drained;
mod gapless_lanes;
mod lane_failure_visibility;
mod lane_holdings;
mod owned_download_lane_pool;
mod propagation_delay;
mod server_ladder;
mod stall_recovery;
mod stall_retry;
mod volume_budget;
