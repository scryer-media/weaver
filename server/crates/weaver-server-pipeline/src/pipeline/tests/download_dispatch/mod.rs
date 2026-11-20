// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

use super::dispatch_fixture::{
    audited_pipeline as new_direct_pipeline,
    audited_pipeline_with_backlog as new_direct_pipeline_with_backlog,
};

use super::dispatch_fixture::results::{held_works, hold_on_test_lane};

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

mod emits_download_pipeline_drained;
mod gapless_lanes;
mod owned_download_lane_pool;
mod propagation_delay;
mod stall_recovery;
mod volume_budget;
