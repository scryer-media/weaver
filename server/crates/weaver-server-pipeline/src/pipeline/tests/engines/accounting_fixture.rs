// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::JobId;
use crate::jobs::{JobPhase, JobPhaseProgress};
use crate::pipeline::tests::finished_job_info;
use crate::pipeline::{
    Pipeline,
    queue::{Change, JobRow},
};

fn rows(pipeline: &Pipeline, job_id: JobId) -> Vec<JobPhaseProgress> {
    pipeline
        .shared_state
        .get_job(job_id)
        .unwrap()
        .phase_progress
}

pub fn phase_rows(pipeline: &mut Pipeline, job_id: JobId) -> Vec<JobPhaseProgress> {
    pipeline.shared_state.queue.write(|inner| {
        inner.push_job(JobRow::new(finished_job_info(job_id)));
        Change::changed(())
    });
    pipeline.phase_begin_unsampled(job_id, JobPhase::Repairing);
    rows(pipeline, job_id)
}

pub fn pipeline_live(pipeline: &Pipeline) -> bool {
    pipeline.pipeline_is_live()
}

pub fn sample_phase_rows(pipeline: &Pipeline, job_id: JobId) -> Vec<JobPhaseProgress> {
    pipeline.shared_state.queue.sample_counters(
        &pipeline.metrics,
        &pipeline.event_tx,
        std::time::Instant::now(),
    );
    rows(pipeline, job_id)
}

pub fn finish_phase(pipeline: &mut Pipeline, job_id: JobId) -> Vec<JobPhaseProgress> {
    pipeline.phase_end(job_id, JobPhase::Repairing);
    rows(pipeline, job_id)
}
