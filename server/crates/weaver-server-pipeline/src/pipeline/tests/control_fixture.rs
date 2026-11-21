// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::{DownloadState, PostState, RunState};

pub fn has_repeated_article_cache(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.repeated_articles.contains_key(&id)
}

pub async fn rename_when_quiet(pipeline: &mut Pipeline, id: JobId, name: &str) -> bool {
    pipeline.rename_working_dir_when_quiet(id, name).await
}

pub fn drop_repeated_article_cache(pipeline: &mut Pipeline, id: JobId) {
    pipeline.repeated_articles.remove(&id);
}

pub fn hold_terminal_post_processing(pipeline: &mut Pipeline, id: JobId) {
    let (cancelled, _) = tokio::sync::watch::channel(false);
    pipeline
        .terminal_post_processing_cancellations
        .insert(id, cancelled);
}

pub fn terminal_post_processing_held(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .terminal_post_processing_cancellations
        .contains_key(&id)
}

pub async fn report_interrupted_terminal_post_processing(pipeline: &Pipeline, id: JobId) {
    pipeline
        .terminal_post_processing_done_tx
        .send(TerminalPostProcessingEvent::Done(
            TerminalPostProcessingDone {
                job_id: id,
                primary_failure: None,
                result: Ok(crate::post_processing::executor::JobPostProcessingReport {
                    summary: crate::post_processing::model::PostProcessingSummary::Interrupted,
                    results: Vec::new(),
                }),
            },
        ))
        .await
        .unwrap();
}

pub fn admission_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.admissions.contains(&id))
}

pub fn admission_and_cleanup_finished(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| !inner.admissions.contains(&id) && !inner.pending_cleanup.contains(&id))
}

pub fn visible_job(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline
        .shared_state
        .queue
        .read(|inner| inner.visible_jobs().any(|job| job.job_id == id))
}

pub fn set_extracting(pipeline: &mut Pipeline, id: JobId) {
    let state = pipeline.jobs.get_mut(&id).unwrap();
    state.status = JobStatus::Extracting;
    state.download_state = DownloadState::Complete;
    state.post_state = PostState::Extracting;
}

pub fn set_active_run_state(pipeline: &mut Pipeline, id: JobId) {
    pipeline.jobs.get_mut(&id).unwrap().run_state = RunState::Active;
}

pub fn run_state(pipeline: &Pipeline, id: JobId) -> RunState {
    pipeline.jobs[&id].run_state
}

pub fn claim_reported_move(pipeline: &Pipeline, id: JobId) -> JobStatus {
    pipeline.shared_state.queue.write(|inner| {
        let row = inner.jobs.iter_mut().find(|row| row.id == id).unwrap();
        let status = row.report.job.status.clone();
        row.report.moving = true;
        crate::pipeline::queue::Change::changed(status)
    })
}

pub fn pause_intent(pipeline: &Pipeline, id: JobId) -> (Option<bool>, u64) {
    pipeline.shared_state.queue.read(|inner| {
        let row = inner.jobs.iter().find(|row| row.id == id).unwrap();
        (row.intents.paused, row.intents.seq)
    })
}

pub async fn start_move_to_complete(pipeline: &mut Pipeline, id: JobId) -> Result<(), String> {
    pipeline.start_move_to_complete(id).await
}

pub fn move_deferred(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.deferred_moves.contains(&id)
}

pub fn move_in_flight(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.inflight_moves.contains(&id)
}

pub fn completion_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.pending_completion_checks.contains(&id)
}

pub struct CompletedMove(MoveToCompleteDone);

impl CompletedMove {
    pub async fn receive(pipeline: &mut Pipeline) -> Self {
        Self(pipeline.move_done_rx.recv().await.unwrap())
    }

    pub fn job_id(&self) -> JobId {
        self.0.job_id
    }

    pub fn succeeded(&self) -> bool {
        self.0.result.is_ok()
    }

    pub async fn deliver(self, pipeline: &mut Pipeline) {
        self.deliver_without_settling(pipeline).await;
        super::settle_script_admissions(pipeline).await;
    }

    pub async fn deliver_without_settling(self, pipeline: &mut Pipeline) {
        pipeline.handle_move_to_complete_done(self.0).await;
    }
}

pub fn has_current_download_activity(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.job_has_current_download_activity(id)
}

pub fn download_work_pending(pipeline: &Pipeline, id: JobId) -> bool {
    pipeline.shared_state.queue.read(|inner| {
        inner
            .jobs
            .iter()
            .any(|row| row.id == id && row.report.download_work_pending)
    })
}
