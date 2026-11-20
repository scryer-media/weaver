// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use tokio::time::Instant;

pub fn install_rename_job(pipeline: &mut Pipeline, job_id: JobId, name: &str, old_dir: &Path) {
    let mut state = minimal_job_state(job_id, name, old_dir.to_owned());
    state.download_state = crate::jobs::model::DownloadState::Queued;
    pipeline.jobs.insert(job_id, state);
}

pub fn request_rename(pipeline: &Pipeline, job_id: JobId, name: &str) {
    pipeline.shared_state.queue.write(|inner| {
        let mut row = crate::pipeline::queue::JobRow::new(finished_job_info(job_id));
        row.intents.rename_to = Some(name.to_string());
        inner.push_job(row);
        crate::pipeline::queue::Change::changed(())
    });
}

pub fn requested_name(pipeline: &Pipeline, job_id: JobId) -> Option<String> {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .job(job_id)
            .map(|row| row.intents.rename_to.clone())
            .expect("the job's row")
    })
}

#[derive(Default)]
pub struct RenameBackoff(crate::pipeline::RenameRetry);

impl RenameBackoff {
    pub fn failed(&mut self, now: Instant) -> Instant {
        self.0.failed(now);
        self.0.retry_at.unwrap()
    }
}

pub fn set_retry_at(pipeline: &mut Pipeline, id: JobId, at: Option<Instant>) {
    pipeline.pending_renames.entry(id).or_default().retry_at = at;
}

pub fn retry_state(pipeline: &Pipeline, id: JobId) -> Option<(u32, Option<Instant>)> {
    pipeline
        .pending_renames
        .get(&id)
        .map(|retry| (retry.failures, retry.retry_at))
}

pub fn no_pending_renames(pipeline: &Pipeline) -> bool {
    pipeline.pending_renames.is_empty()
}

pub async fn retry_pending(pipeline: &mut Pipeline) {
    pipeline.retry_pending_renames().await;
}

pub fn next_retry_at(pipeline: &Pipeline) -> Option<Instant> {
    pipeline.next_rename_retry_at()
}

pub fn queue_walk_due(pipeline: &Pipeline) -> bool {
    pipeline.queue_walk_due
}

pub fn hold_move(pipeline: &mut Pipeline, id: JobId) {
    pipeline.inflight_moves.insert(id);
}

pub fn release_move(pipeline: &mut Pipeline, id: JobId) {
    pipeline.inflight_moves.remove(&id);
}
