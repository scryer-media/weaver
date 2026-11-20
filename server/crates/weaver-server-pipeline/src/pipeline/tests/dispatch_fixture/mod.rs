// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
pub mod acquire_failures;
pub mod article_slots;
pub mod blocking_lane;
mod booking;
pub mod busy_turns;
pub mod checkpoint;
pub mod ladder_failures;
pub mod ladder_quota;
pub mod scheduler_lanes;
pub mod scheduler_activation;
pub(in crate::pipeline::tests) mod ladder;
pub mod lane_holdings;
pub mod lanes;
mod logs;
pub mod owned_failures;
pub(in crate::pipeline::tests) mod results;
pub mod retries;
mod seeded_walk;
pub(in crate::pipeline::tests) mod scheduler;

// Every pipeline these tests build checks, after each dispatch pass and each
// lane result batch, that its in-flight gauges are what its live lanes hold.
pub(super) async fn audited_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, intermediate_dir, complete_dir) = super::new_direct_pipeline(temp_dir).await;
    pipeline.audit_lane_holdings = true;
    (pipeline, intermediate_dir, complete_dir)
}

pub(super) async fn audited_pipeline_with_backlog(
    temp_dir: &TempDir,
    backlog_base_bytes: usize,
    total_connections: usize,
) -> (Pipeline, PathBuf, PathBuf) {
    let (mut pipeline, intermediate_dir, complete_dir) =
        super::new_direct_pipeline_with_backlog(temp_dir, backlog_base_bytes, total_connections)
            .await;
    pipeline.audit_lane_holdings = true;
    (pipeline, intermediate_dir, complete_dir)
}

pub struct DispatchFixture(Pipeline);

impl DispatchFixture {
    pub async fn new(temp_dir: &TempDir) -> Self {
        Self(audited_pipeline(temp_dir).await.0)
    }

    pub async fn add_job(&mut self, id: JobId, spec: JobSpec) {
        insert_active_job(&mut self.0, id, spec).await;
    }

    pub fn status(&self, id: JobId) -> Option<JobStatus> {
        job_status_for_assert(&self.0, id)
    }

    pub fn book_missing(&mut self, segment: SegmentId) {
        self.0.book_terminal_segment(segment, SegmentTerminalState::Missing);
    }

    pub async fn add_files(&mut self, id: JobId, name: &str, prefix: &str, count: usize) {
        let spec = standalone_job_spec(name, &many_standalone_files(prefix, count));
        insert_active_job(&mut self.0, id, spec).await;
    }

    pub fn job(&self, id: JobId) -> &JobState {
        &self.0.jobs[&id]
    }

    pub fn job_mut(&mut self, id: JobId) -> &mut JobState {
        self.0.jobs.get_mut(&id).unwrap()
    }

    pub fn dispatch(&mut self) {
        self.0.dispatch_downloads();
    }

    pub fn dispatch_at(&mut self, now: Instant) {
        self.0.dispatch_downloads_at(now);
    }

    pub fn admits(&self, id: JobId) -> bool {
        self.0.nzb_admitted.contains(&id)
    }

    pub fn admit(&mut self, id: JobId) {
        self.0.nzb_admitted.insert(id);
    }

    pub fn release_slot(&mut self, id: JobId) {
        self.0.nzb_admitted.remove(&id);
    }

    pub fn articles_in_flight(&self) -> usize {
        self.0.articles_in_flight
    }

    pub fn request_pause(&mut self, id: JobId) {
        self.0.shared_state.queue.write(|inner| {
            inner.job_mut(id).unwrap().intents.paused = Some(true);
            crate::pipeline::queue::Change::changed(())
        });
    }
}
