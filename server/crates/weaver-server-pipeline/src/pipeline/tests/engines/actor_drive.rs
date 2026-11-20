// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{Scenario, serve_job};
use crate::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::tests::{job_status_for_assert, settle_inflight_moves};
use crate::pipeline::{Pipeline, extraction::ExtractionLimits};

pub(super) fn ended(pipeline: &Pipeline, job: JobId) -> bool {
    matches!(
        job_status_for_assert(pipeline, job),
        Some(JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }) | None
    )
}

// Plays the server for `job` and runs its completion gate, a run's mail and
// the moves it starts, as the orchestrator does, until `until` holds or the
// job has ended. A round that changes nothing twice over is a stall.
pub(super) async fn drive(
    pipeline: &mut Pipeline,
    scenario: &Scenario,
    job: JobId,
    until: impl Fn(&Pipeline) -> bool,
) {
    let look = |pipeline: &Pipeline| {
        let queued = pipeline.jobs.get(&job).map(|state| {
            (
                state.articles.active().unwrap().download_queue.len(),
                state.articles.active().unwrap().recovery_queue.len(),
                state.status.clone(),
            )
        });
        format!(
            "{queued:?} {} {}",
            pipeline.inflight_moves.contains(&job),
            pipeline.pending_completion_checks.len()
        )
    };
    let mut stalled = None;
    loop {
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve_job(pipeline, job, scenario, article).await;
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        let before = look(pipeline);
        pipeline.check_job_completion(job).await;
        pipeline.pump_decode_queue();
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
            pipeline.pump_decode_queue();
        }
        if until(pipeline) || ended(pipeline, job) {
            return;
        }
        if pipeline.inflight_moves.contains(&job) {
            settle_inflight_moves(pipeline).await;
            continue;
        }
        if pipeline
            .graph_jobs
            .get(&job)
            .is_some_and(|graph| graph.awaits_mail())
        {
            let said = pipeline.graph_runs.recv().await.expect("a live run speaks");
            pipeline.graph_run_said(said);
            continue;
        }
        let after = look(pipeline);
        if before == after && stalled.replace(after.clone()) == Some(after.clone()) {
            panic!("the job stalled: {after}");
        }
    }
}

// Limits no row here comes near, with `edit` applied.
pub fn limits(edit: impl FnOnce(&mut ExtractionLimits)) -> ExtractionLimits {
    const GIB: u64 = 1024 * 1024 * 1024;
    let mut limits = ExtractionLimits {
        max_job_bytes: 2 * 1024 * GIB,
        max_member_bytes: 1024 * GIB,
        max_entries: 1_000_000,
        max_ratio: 100,
        ratio_floor_bytes: 1 << 30,
        max_seconds: 43_200,
        min_free_bytes: 1,
        max_memory_bytes: 4 * GIB,
    };
    edit(&mut limits);
    limits
}
