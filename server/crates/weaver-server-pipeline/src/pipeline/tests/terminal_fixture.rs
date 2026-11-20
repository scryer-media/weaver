// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::post_processing::executor::{
    JobPostProcessingReport, PostProcessingExecutor, PostProcessingExecutorError,
};
use crate::post_processing::model::PostProcessingFacts;
use crate::post_processing::runner::JobExecutionContext;

pub fn configure_executor(pipeline: &mut Pipeline, scripts: PathBuf, supervisor: Option<PathBuf>) {
    let mut executor = PostProcessingExecutor::new(pipeline.db.clone(), scripts);
    if let Some(supervisor) = supervisor {
        executor = executor.with_supervisor_executable(supervisor);
    }
    pipeline.terminal_post_processing_executor = executor;
}

pub fn pause_executor(pipeline: &Pipeline) {
    pipeline.terminal_post_processing_executor.pause();
}

pub fn resume_executor(pipeline: &Pipeline) {
    pipeline.terminal_post_processing_executor.resume();
}

pub async fn settle_moves(pipeline: &mut Pipeline) {
    super::settle_inflight_moves(pipeline).await;
}

pub enum TerminalEvent {
    Started(JobId),
    Done(TerminalDone),
    Other,
}

pub struct TerminalDone(TerminalPostProcessingDone);

impl TerminalDone {
    pub fn job_id(&self) -> JobId {
        self.0.job_id
    }

    pub fn primary_failure(&self) -> Option<&str> {
        self.0.primary_failure.as_deref()
    }

    pub fn report(&self) -> Result<&JobPostProcessingReport, &PostProcessingExecutorError> {
        self.0.result.as_ref()
    }

    pub fn deliver(self, pipeline: &mut Pipeline) {
        pipeline.handle_terminal_post_processing_done(self.0);
    }
}

impl TerminalEvent {
    pub async fn receive(pipeline: &mut Pipeline) -> Self {
        match pipeline
            .terminal_post_processing_done_rx
            .recv()
            .await
            .unwrap()
        {
            TerminalPostProcessingEvent::Started(id) => Self::Started(id),
            TerminalPostProcessingEvent::Done(done) => Self::Done(TerminalDone(done)),
            _ => Self::Other,
        }
    }
}

pub fn terminal_started(pipeline: &mut Pipeline, id: JobId) {
    pipeline.handle_terminal_post_processing_started(id);
}

pub fn terminal_in_flight(pipeline: &Pipeline) -> bool {
    !pipeline.inflight_terminal_post_processing.is_empty()
}

pub fn any_move_in_flight(pipeline: &Pipeline) -> bool {
    !pipeline.inflight_moves.is_empty()
}

pub fn try_receive_move(pipeline: &mut Pipeline) -> bool {
    pipeline.move_done_rx.try_recv().is_ok()
}

pub fn reserved_destination(pipeline: &Pipeline, id: JobId) -> &Path {
    &pipeline.reserved_complete_destinations[&id]
}

pub fn minimal_spec(id: JobId, name: &str) -> JobSpec {
    minimal_job_state(id, name, PathBuf::new()).spec
}

pub fn terminal_context(
    pipeline: &Pipeline,
    id: JobId,
    facts: &PostProcessingFacts,
) -> Option<JobExecutionContext> {
    pipeline.terminal_post_processing_context(id, facts)
}
