use super::*;
use crate::post_processing::events::{EventContext, wait_for_event};
use crate::post_processing::model::{PipelineOutcome, QueueEvent};
use crate::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

impl Pipeline {
    pub(crate) fn queue_script_context(
        &self,
        job_id: JobId,
        event: QueueEvent,
    ) -> Option<EventContext> {
        let state = self.jobs.get(&job_id)?;
        Some(EventContext::from_job(
            &JobExecutionContext {
                job_id: job_id.0,
                name: state.spec.name.clone(),
                nzb_filename: format!("{}.nzb", state.spec.name),
                category: state.spec.category.clone(),
                group: None,
                source_url: state
                    .spec
                    .metadata
                    .iter()
                    .find(|(key, _)| key == crate::post_processing::scan::SOURCE_URL_KEY)
                    .map(|(_, value)| value.clone()),
                working_directory: state.working_dir.clone(),
                final_directory: state.working_dir.clone(),
                pipeline_outcome: PipelineOutcome::Succeeded,
                par_status: 0,
                unpack_status: 0,
                compatibility: CompatibilityFacts {
                    parameters: state.spec.metadata.clone(),
                    password: state.spec.password.clone(),
                    total_bytes: state.spec.total_bytes,
                    downloaded_bytes: state.downloaded_bytes,
                    intermediate_dir: Some(self.intermediate_dir.clone()),
                    complete_dir: Some(self.complete_dir.clone()),
                    data_dir: Some(self.script_data_dir.clone()),
                    ..Default::default()
                },
            },
            event,
        ))
    }

    pub(crate) fn raise_queue_script_event(
        &self,
        job_id: JobId,
        event: QueueEvent,
        delete_status: Option<&str>,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if !self.db.queue_scripts_possible(&state.spec.metadata) {
            return;
        }
        if event == QueueEvent::FileDownloaded
            && self.jobs.get(&job_id).is_none_or(|state| {
                !matches!(
                    state.status,
                    JobStatus::Queued
                        | JobStatus::Downloading
                        | JobStatus::Checking
                        | JobStatus::Paused
                )
            })
        {
            return;
        }
        let Some(mut context) = self.queue_script_context(job_id, event) else {
            return;
        };
        if let Some(status) = delete_status {
            context
                .env
                .insert("NZBNA_DELETESTATUS".into(), status.into());
        }
        drop(self.db.admit_queue_script_event(context, false));
    }

    /// Returns true while the completion pass must yield to its queue scripts.
    pub(crate) fn queue_script_completion_gate(&mut self, job_id: JobId) -> bool {
        if self.job_has_pending_download_pipeline_work(job_id) {
            return false;
        }
        if self.queue_script_waiters.contains(&job_id) {
            return true;
        }
        if self.queue_scripts_completed.contains(&job_id) {
            return false;
        }
        if self.jobs.get(&job_id).is_some_and(|state| {
            !self
                .db
                .queue_barrier_possible(job_id.0, &state.spec.metadata)
        }) {
            return false;
        }
        let Some(context) = self.queue_script_context(job_id, QueueEvent::NzbDownloaded) else {
            return false;
        };
        let admission = self.db.admit_queue_script_event(context, true);
        self.queue_script_waiters.insert(job_id);
        let db = self.db.clone();
        let sender = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let result = async {
                if let Some(run_id) = admission.await.map_err(|error| {
                    crate::StateError::Database(format!("queue script admission lost: {error}"))
                })?? {
                    sender
                        .send(TerminalPostProcessingEvent::QueueAdmitted(job_id))
                        .await
                        .map_err(|error| crate::StateError::Database(error.to_string()))?;
                    wait_for_event(&db, &run_id).await?;
                }
                Ok(())
            }
            .await;
            let _ = sender
                .send(TerminalPostProcessingEvent::QueueDone(job_id, result))
                .await;
        });
        true
    }

    pub(crate) fn apply_queue_script_effects(&mut self, job_id: JobId) -> bool {
        if !self.jobs.contains_key(&job_id) {
            return false;
        }
        match self.db.job_script_effects(job_id.0) {
            Ok(effects) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    effects.merge_parameters(&mut state.spec.metadata);
                    if let Some(directory) = &effects.directory {
                        state.working_dir = directory.clone();
                    }
                }
                if effects.marked_bad
                    && !self.inflight_terminal_post_processing.contains(&job_id)
                    && self
                        .jobs
                        .get(&job_id)
                        .is_some_and(|state| !matches!(state.status, JobStatus::Failed { .. }))
                {
                    self.raise_queue_script_event(job_id, QueueEvent::NzbDeleted, Some("BAD"));
                    self.fail_job(job_id, "FAILURE/BAD: marked bad by script".into());
                    return true;
                }
            }
            Err(error) => {
                self.fail_job(
                    job_id,
                    format!("queue script directives could not be restored: {error}"),
                );
                return true;
            }
        }
        false
    }
}
