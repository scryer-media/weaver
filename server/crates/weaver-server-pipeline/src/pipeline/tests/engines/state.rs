// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::super::{JobId, JobStatus, Pipeline, job_status_for_assert};

// Every run is its own pipeline, so one id serves every scenario.
pub const JOB: JobId = JobId(93_001);

pub fn finished(pipeline: &Pipeline) -> bool {
    matches!(
        job_status_for_assert(pipeline, JOB),
        Some(JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }) | None
    )
}
