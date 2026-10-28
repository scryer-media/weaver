// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    pub(crate) fn critical_health_milli(total: u64, par2_bytes: u64) -> u32 {
        if par2_bytes == 0 {
            850
        } else if par2_bytes * 2 > total {
            0
        } else {
            let denom = total.saturating_sub(par2_bytes);
            total
                .saturating_sub(par2_bytes * 2)
                .saturating_mul(1000)
                .checked_div(denom)
                .unwrap_or(0) as u32
        }
    }

    // Reject untrusted delivery content or an incomplete security check
    // without exposing the working tree to terminal scripts or publication.
    pub(super) fn fail_delivery_security_check(&mut self, job_id: JobId, error: String) {
        let release = self.prepare_failed_job_runtime(job_id, &error, false);
        self.finish_failed_job(job_id, error, release);
    }

    // Mark a job as failed and purge its queued segments.
    pub(super) fn fail_job(&mut self, job_id: JobId, error: String) {
        self.fail_job_with_rejection(job_id, error, false);
    }

    // Mark a job as failed because its post failed it: articles were lost
    // past what its recovery could rebuild, or nothing arrived whole. Queue
    // scripts hear the job deleted for health before it fails as any job
    // does. A failure an extraction made is not a deletion and goes through
    // `fail_job` or `fail_job_on_extraction_error` instead.
    pub(super) fn fail_job_on_health(&mut self, job_id: JobId, error: String) {
        self.raise_queue_script_event(
            job_id,
            crate::post_processing::model::QueueEvent::NzbDeleted,
            Some("HEALTH"),
        );
        self.fail_job(job_id, error);
    }

    // Mark a job as failed on an extraction error, routed on its class.
    pub(super) fn fail_job_on_extraction_error(&mut self, job_id: JobId, error: ExtractionError) {
        let reason = error.job_failure_reason();
        let extraction_rejected = error.rejection().is_some();
        self.fail_job_with_rejection(job_id, reason, extraction_rejected);
    }

    fn fail_job_with_rejection(&mut self, job_id: JobId, error: String, extraction_rejected: bool) {
        tracing::error!(job_id = job_id.0, reason = %error, "job failed");
        let failure_stage = self
            .jobs
            .get(&job_id)
            .map(|state| match state.status {
                JobStatus::Verifying => crate::post_processing::model::PipelineFailureStage::Verify,
                JobStatus::QueuedRepair | JobStatus::Repairing => {
                    crate::post_processing::model::PipelineFailureStage::Repair
                }
                JobStatus::QueuedExtract | JobStatus::Extracting => {
                    crate::post_processing::model::PipelineFailureStage::Extract
                }
                JobStatus::Moving => crate::post_processing::model::PipelineFailureStage::Move,
                _ => crate::post_processing::model::PipelineFailureStage::Download,
            })
            .unwrap_or(crate::post_processing::model::PipelineFailureStage::Download);
        // The first failure wins: a later fail_job for the same job comes
        // from cleanup, not from the stage that broke.
        self.pipeline_failure_stages
            .entry(job_id)
            .or_insert(failure_stage);
        // Scripts run on failure too, so the job's own failure survives them:
        // the primary failure is carried through and re-applied afterwards.
        let scripts_may_run = self
            .terminal_post_processing_executor
            .execution_enabled()
            .unwrap_or_else(|settings_error| {
                tracing::warn!(
                    job_id = job_id.0,
                    error = %settings_error,
                    "could not load post-processing settings; preserving legacy failure finalization"
                );
                false
            });
        let should_defer = !extraction_rejected
            && scripts_may_run
            && self.jobs.contains_key(&job_id)
            && !self.inflight_terminal_post_processing.contains(&job_id);
        let release = self.prepare_failed_job_runtime(job_id, &error, should_defer);

        if should_defer {
            self.start_terminal_post_processing_with_outcome(
                job_id,
                crate::post_processing::model::PipelineOutcome::Failed {
                    stage: failure_stage,
                    code: "PIPELINE_FAILURE".to_string(),
                    message: error.clone(),
                },
                Some(error),
            );
            return;
        }

        self.finish_failed_job(job_id, error, release);
    }

    pub(crate) fn finalize_failed_job_after_terminal_post_processing(
        &mut self,
        job_id: JobId,
        error: String,
    ) {
        let release = self.prepare_failed_job_runtime(job_id, &error, false);
        self.finish_failed_job(job_id, error, release);
    }

    // Returns the working directory to release, unless the staging is kept
    // for the scripts that run on the failure first.
    fn prepare_failed_job_runtime(
        &mut self,
        job_id: JobId,
        error: &str,
        preserve_staging: bool,
    ) -> Option<PathBuf> {
        let (staging_dir, working_dir, released) = if let Some(state) = self.jobs.get_mut(&job_id) {
            let released = state.post_state;
            state.queued_repair_at_epoch_ms = None;
            state.queued_extract_at_epoch_ms = None;
            state.paused_resume_status = None;
            state.paused_resume_download_state = None;
            state.paused_resume_post_state = None;
            state.set_failure(error.to_string());
            // Clear per-job queues to free memory.
            if let Some(active) = state.articles.active_mut() {
                active.download_queue = DownloadQueue::new();
                active.recovery_queue = DownloadQueue::new();
            }
            let (staging_dir, working_dir) = if preserve_staging {
                (None, None)
            } else {
                (state.staging_dir.take(), Some(state.working_dir.clone()))
            };
            (staging_dir, working_dir, Some(released))
        } else {
            (None, None, None)
        };
        if let Some(released) = released {
            self.metrics
                .post_state_moved(released, crate::jobs::model::PostState::Failed);
        }
        // Clean up staging directory if it was created.
        if let Some(staging) = staging_dir {
            tokio::spawn(async move {
                if let Err(e) = tokio::fs::remove_dir_all(&staging).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        dir = %staging.display(),
                        error = %e,
                        "failed to clean up staging directory on job failure"
                    );
                }
            });
        }
        self.active_download_passes.remove(&job_id);
        self.jobs_finalizing_download.remove(&job_id);
        self.active_download_connections_by_job.remove(&job_id);
        self.active_completion_critical_connections_by_job
            .remove(&job_id);
        self.active_decodes_by_job.remove(&job_id);
        self.active_decodes_by_file
            .retain(|file_id, _| file_id.job_id != job_id);
        self.active_decode_bytes
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.rate_limit_reservations
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.remove_pending_completion_check(job_id);
        self.clear_job_phase_progress_runtime(job_id);
        self.clear_job_retention_excludes(job_id);
        self.clear_job_write_backlog(job_id);
        working_dir
    }

    // What the job leaves in its working directory is what it delivered:
    // the directory is released off the actor before the history records
    // it, so the record never lists the graph's internals.
    fn finish_failed_job(&mut self, job_id: JobId, error: String, release: Option<PathBuf>) {
        let Some(working_dir) = release else {
            self.record_failed_job(job_id, error);
            return;
        };
        let root = self.intermediate_dir.clone();
        let done = self.failed_release_tx.clone();
        self.releasing_failed_jobs.insert(job_id);
        tokio::spawn(async move {
            let dir = working_dir.clone();
            let released = tokio::task::spawn_blocking(move || {
                crate::jobs::working_dir::release_failed_working_dir(&root, &dir, job_id)
            })
            .await;
            let failure = match released {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            };
            if let Some(failure) = failure {
                tracing::warn!(
                    job_id = job_id.0,
                    dir = %working_dir.display(),
                    error = %failure,
                    "failed to release a failed job's working directory"
                );
            }
            // A closed channel means the pipeline is gone.
            let _ = done.send(super::FailedJobReleased { job_id, error }).await;
        });
    }

    pub(super) fn handle_failed_job_released(&mut self, released: super::FailedJobReleased) {
        self.releasing_failed_jobs.remove(&released.job_id);
        self.record_failed_job(released.job_id, released.error);
    }

    fn record_failed_job(&mut self, job_id: JobId, error: String) {
        self.record_job_history(job_id, Some(PipelineEvent::JobFailed { job_id, error }));
        self.job_order.retain(|id| *id != job_id);
        self.report_job(job_id);
    }
}
