// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::harness::{JOB, Scenario, serve_job};
use crate::JobId;
use crate::pipeline::tests::{
    job_status_for_assert, pump_pipeline_runtime_queues, settle_script_admissions,
};
use crate::pipeline::{MoveToCompleteDone, Pipeline};
use weaver_graph::{MemberId, NodeId};

// Serve `job` and run its completion gate until its delivery move is out.
pub async fn drive_to_move(pipeline: &mut Pipeline, job: JobId, scenario: &Scenario) {
    let mut idle = 0;
    while !pipeline.inflight_moves.contains(&job) {
        let mut served = false;
        while let Some(work) = pipeline
            .jobs
            .get_mut(&job)
            .and_then(|state| state.articles.active_mut().unwrap().download_queue.pop())
        {
            served = true;
            let article = (
                work.segment_id.file_id.file_index,
                work.segment_id.segment_number,
            );
            serve_job(pipeline, job, scenario, article).await;
        }
        if pipeline.inflight_moves.contains(&job) {
            break;
        }
        pipeline.check_job_completion(job).await;
        while let Some(queued) = pipeline.pending_completion_checks.pop_front() {
            pipeline.check_job_completion(queued).await;
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
        idle = if served { 0 } else { idle + 1 };
        assert!(
            idle < 2 || pipeline.inflight_moves.contains(&job),
            "{job:?} stalled: {:?}",
            job_status_for_assert(pipeline, job)
        );
    }
}

pub fn repaired(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline
        .graph_recovery_summary(job)
        .is_some_and(|summary| summary.repaired())
}

pub fn moving(pipeline: &Pipeline, job: JobId) -> bool {
    pipeline.inflight_moves.contains(&job)
}

pub struct FinishedMove(MoveToCompleteDone);

impl FinishedMove {
    pub fn job(&self) -> JobId {
        self.0.job_id
    }

    pub async fn acknowledge(self, pipeline: &mut Pipeline) {
        pipeline.handle_move_to_complete_done(self.0).await;
        settle_script_admissions(pipeline).await;
    }
}

pub async fn next_move(pipeline: &mut Pipeline) -> FinishedMove {
    FinishedMove(pipeline.move_done_rx.recv().await.expect("a move reports"))
}

pub async fn park_recovery_head(pipeline: &mut Pipeline, scenario: &Scenario) -> bool {
    serve_job(pipeline, JOB, scenario, (2, 0)).await;
    let active = pipeline
        .jobs
        .get_mut(&JOB)
        .unwrap()
        .articles
        .active_mut()
        .unwrap();
    while active.download_queue.pop().is_some() {}
    !active.recovery_queue.is_empty()
}

pub fn dispatch(pipeline: &mut Pipeline) {
    pipeline.dispatch_downloads();
}

pub fn completion_pending(pipeline: &Pipeline) -> bool {
    pipeline.pending_completion_checks.contains(&JOB)
}

pub fn offer_password(pipeline: &mut Pipeline, password: &str) {
    let job = pipeline.graph_jobs.remove(&JOB).unwrap();
    pipeline
        .graph_jobs
        .insert(JOB, job.offering(vec![password.to_owned()]));
}

pub fn awaits_mail(pipeline: &Pipeline) -> bool {
    pipeline.graph_jobs[&JOB].awaits_mail()
}

pub async fn hear_owner(pipeline: &mut Pipeline) {
    let said = pipeline
        .graph_runs
        .recv()
        .await
        .expect("the owning worker answers");
    pipeline.graph_run_said(said);
    pump_pipeline_runtime_queues(pipeline).await;
}

pub fn root_member_name(pipeline: &Pipeline, member: MemberId) -> &str {
    &pipeline.graph_jobs[&JOB]
        .tree()
        .namespace(NodeId::ROOT)
        .unwrap()
        .get(member)
        .unwrap()
        .name
}
