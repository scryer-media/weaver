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
        if !self.jobs.contains_key(&job_id) || !self.db.queue_scripts_possible() {
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

    /// Raise the event for a job's arrival, and keep the job from downloading
    /// while an instance that blocks on it runs.
    ///
    /// The hold is taken only when such an instance is wired up for this job's
    /// category, so a job nobody is waiting on starts at once. It is released
    /// by the end of the run, however that run ends: what a script decides is
    /// in its directives, and a script that could not run decides nothing.
    pub(crate) fn raise_added_script_event(&mut self, job_id: JobId) {
        if !self.jobs.contains_key(&job_id) || !self.db.queue_scripts_possible() {
            return;
        }
        let Some(context) = self.queue_script_context(job_id, QueueEvent::NzbAdded) else {
            return;
        };
        let blocks = self
            .db
            .script_instances_for(&context.event, context.category.as_deref())
            .is_ok_and(|instances| instances.iter().any(|instance| instance.blocking));
        let admission = self.db.admit_queue_script_event(context, false);
        if !blocks {
            return;
        }
        self.added_script_holds.insert(job_id);
        let db = self.db.clone();
        let sender = self.terminal_post_processing_done_tx.clone();
        tokio::spawn(async move {
            let result = async {
                if let Some(run_id) = admission.await.map_err(|error| {
                    crate::StateError::Database(format!("queue script admission lost: {error}"))
                })?? {
                    wait_for_event(&db, &run_id).await?;
                }
                Ok(())
            }
            .await;
            let _ = sender
                .send(TerminalPostProcessingEvent::AddedScriptsDone(
                    job_id, result,
                ))
                .await;
        });
    }

    /// Whether a job is being kept from downloading by the scripts that run on
    /// its arrival.
    pub(crate) fn held_for_added_scripts(&self, job_id: JobId) -> bool {
        self.added_script_holds.contains(&job_id)
    }

    pub(crate) fn handle_added_scripts_done(
        &mut self,
        job_id: JobId,
        result: Result<(), crate::StateError>,
    ) {
        if !self.added_script_holds.remove(&job_id) {
            return;
        }
        if let Err(error) = result {
            tracing::warn!(job_id = job_id.0, %error, "scripts for a new job could not run; starting the download");
        }
        // What the scripts asked for has to be in force before the first
        // article is fetched.
        self.apply_queue_script_effects(job_id);
        self.publish_snapshot();
    }

    /// Returns true while the completion pass must yield to its queue scripts.
    ///
    /// The barrier is raised once nothing more is coming off the wire, or once
    /// every data file is complete. The second clause matters: the streamed
    /// decode of a job's last article runs the completion pass from inside the
    /// booking of that article's own download result, so the result still
    /// counts as pending download work while the pass, with every file
    /// complete, goes on to finalize the job. Deferring on pending work there
    /// would let the final move run with the barrier never raised.
    pub(crate) fn queue_script_completion_gate(
        &mut self,
        job_id: JobId,
        data_files_complete: bool,
    ) -> bool {
        if !data_files_complete && self.job_has_pending_download_pipeline_work(job_id) {
            return false;
        }
        if self.queue_script_waiters.contains(&job_id) {
            return true;
        }
        if self.queue_scripts_completed.contains(&job_id) {
            return false;
        }
        if self.jobs.contains_key(&job_id) && !self.db.queue_barrier_possible(job_id.0) {
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

    /// A job whose download is complete and that holds the queue-script
    /// barrier. Until the scripts finish it is not downloading: it cannot be
    /// paused, a semantic cancel is not safe, and it reports as waiting.
    pub(crate) fn awaiting_queue_script_barrier(&self, job_id: JobId) -> bool {
        self.queue_script_waiters.contains(&job_id)
    }

    pub(crate) fn handle_queue_scripts_admitted(&mut self, job_id: JobId) {
        if self.queue_script_waiters.contains(&job_id)
            && self
                .jobs
                .get(&job_id)
                .is_some_and(|job| queue_barrier_status(&job.status))
        {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::AwaitingQueueScripts,
                Some("waiting for queue scripts"),
            );
            self.persist_active_runtime(job_id);
            self.publish_snapshot();
        }
    }

    pub(crate) async fn handle_queue_scripts_done(
        &mut self,
        job_id: JobId,
        result: Result<(), crate::StateError>,
    ) {
        let was_waiting = self.queue_script_waiters.remove(&job_id);
        if !was_waiting
            || !self
                .jobs
                .get(&job_id)
                .is_some_and(|job| queue_barrier_status(&job.status))
        {
            return;
        }
        // The scripts' own outcomes are recorded with their runs. A failure to
        // run them at all (a lost admission, an unreadable setting) is not the
        // job's fault, so the job carries on.
        if let Err(error) = result {
            tracing::warn!(job_id = job_id.0, %error, "queue scripts could not run; continuing the job");
        }
        self.queue_scripts_completed
            .retain(|id| self.jobs.contains_key(id));
        self.queue_scripts_completed.insert(job_id);
        if self.apply_queue_script_effects(job_id) {
            return;
        }
        if self
            .jobs
            .get(&job_id)
            .is_some_and(|job| job.status == JobStatus::AwaitingQueueScripts)
        {
            self.transition_postprocessing_status(job_id, JobStatus::Downloading, None);
        }
        self.check_job_completion(job_id).await;
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
                    // No NZB_DELETED event: a job a script marked bad runs no
                    // further queue scripts except NZB_MARKED.
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

/// The statuses a job holding the queue-script barrier can be in. Anything
/// else means the job moved on, and a late event must not act on it.
fn queue_barrier_status(status: &JobStatus) -> bool {
    matches!(
        status,
        JobStatus::AwaitingQueueScripts | JobStatus::Downloading | JobStatus::Queued
    )
}
