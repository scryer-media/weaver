// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

impl Pipeline {
    #[cfg(test)]
    pub(crate) fn status_enter_failpoint_for_transition(
        old_post_state: crate::jobs::model::PostState,
        old_run_state: crate::jobs::model::RunState,
        new_post_state: crate::jobs::model::PostState,
        new_run_state: crate::jobs::model::RunState,
    ) -> Option<&'static str> {
        if old_run_state != new_run_state
            && matches!(new_run_state, crate::jobs::model::RunState::Paused)
        {
            return Some(crate::e2e_failpoint::STATUS_ENTER_PAUSED);
        }
        if old_post_state == new_post_state {
            return None;
        }
        match new_post_state {
            crate::jobs::model::PostState::Verifying => {
                Some(Self::par2_verification_started_failpoint_name())
            }
            crate::jobs::model::PostState::Repairing => {
                Some(crate::e2e_failpoint::STATUS_ENTER_REPAIRING)
            }
            crate::jobs::model::PostState::QueuedRepair => {
                Some(crate::e2e_failpoint::STATUS_ENTER_QUEUED_REPAIR)
            }
            crate::jobs::model::PostState::QueuedExtract => {
                Some(crate::e2e_failpoint::STATUS_ENTER_QUEUED_EXTRACT)
            }
            _ => None,
        }
    }

    pub(crate) fn par2_verification_started_failpoint_name() -> &'static str {
        crate::e2e_failpoint::STATUS_ENTER_VERIFYING
    }

    pub(crate) fn persist_active_runtime(&self, job_id: JobId) {
        self.persist_active_runtime_at(job_id, None);
    }

    // [`Self::persist_active_runtime`] that also records `output_dir` as the
    // job's output location in the same ordered write.
    pub(crate) fn persist_active_runtime_at(
        &self,
        job_id: JobId,
        output_dir: Option<std::path::PathBuf>,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let output_dir = output_dir.map(|dir| dir.to_string_lossy().into_owned());
        let status = Self::persist_active_status_for(&state.status).to_string();
        let error = match &state.status {
            JobStatus::Failed { error } => Some(error.clone()),
            _ => state.failure_error.clone(),
        };
        let queued_repair_at_epoch_ms = state.queued_repair_at_epoch_ms;
        let queued_extract_at_epoch_ms = state.queued_extract_at_epoch_ms;
        let paused_resume_status = state
            .paused_resume_status
            .as_ref()
            .map(Self::persist_active_status_for)
            .map(str::to_string);
        let download_state = state.download_state.as_str().to_string();
        let post_state = state.post_state.as_str().to_string();
        let run_state = state.run_state.as_str().to_string();
        let paused_resume_download_state = state
            .paused_resume_download_state
            .as_ref()
            .map(|download_state| download_state.as_str())
            .map(str::to_string)
            .or_else(|| {
                state
                    .paused_resume_status
                    .as_ref()
                    .map(crate::jobs::model::runtime_lanes_from_status_snapshot)
                    .map(|(download_state, _, _)| download_state.as_str().to_string())
            });
        let paused_resume_post_state = state
            .paused_resume_post_state
            .as_ref()
            .map(|post_state| post_state.as_str())
            .map(str::to_string)
            .or_else(|| {
                state
                    .paused_resume_status
                    .as_ref()
                    .map(crate::jobs::model::runtime_lanes_from_status_snapshot)
                    .map(|(_, post_state, _)| post_state.as_str().to_string())
            });

        // Route through the ordered writer queue rather than a detached
        // fire-and-forget task: two rapid runtime transitions for the same job
        // (last-write-wins columns: run_state / paused_resume_* / status) must
        // land in enqueue order, or a restored job could resume stale state.
        if let Err(error) =
            self.db
                .try_queue_job_write(job_id, "set_active_job_runtime", move |db| {
                    db.set_active_job_runtime_at(
                        job_id,
                        output_dir.as_deref(),
                        &status,
                        Some(&download_state),
                        Some(&post_state),
                        Some(&run_state),
                        error.as_deref(),
                        queued_repair_at_epoch_ms,
                        queued_extract_at_epoch_ms,
                        paused_resume_status.as_deref(),
                        paused_resume_download_state.as_deref(),
                        paused_resume_post_state.as_deref(),
                    )
                })
        {
            tracing::error!(
                error = %error,
                "failed to queue active job runtime write"
            );
        }
    }

    pub(crate) fn schedule_job_completion_check(&mut self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if matches!(
            state.status,
            JobStatus::Paused
                | JobStatus::Checking
                | JobStatus::Moving
                | JobStatus::Complete
                | JobStatus::Partial
                | JobStatus::Failed { .. }
        ) {
            return;
        }
        if !self.pending_completion_checks.contains(&job_id) {
            self.pending_completion_checks.push_back(job_id);
        }
    }

    pub(crate) fn remove_pending_completion_check(&mut self, job_id: JobId) {
        self.pending_completion_checks
            .retain(|queued| *queued != job_id);
    }

    pub(crate) fn persist_active_status_for(status: &JobStatus) -> &'static str {
        match status {
            JobStatus::AwaitingQueueScripts => "awaiting_queue_scripts",
            JobStatus::Queued => "queued",
            JobStatus::Downloading => "downloading",
            JobStatus::Checking => "checking",
            JobStatus::Verifying => "verifying",
            JobStatus::QueuedRepair => "queued_repair",
            JobStatus::Repairing => "repairing",
            JobStatus::QueuedExtract => "queued_extract",
            JobStatus::Extracting => "extracting",
            JobStatus::Moving => "moving",
            JobStatus::QueuedPostProcessing => "queued_post_processing",
            JobStatus::PostProcessing => "post_processing",
            JobStatus::Complete => "complete",
            JobStatus::Partial => "partial",
            JobStatus::Failed { .. } => "failed",
            JobStatus::Paused => "paused",
        }
    }

    fn transition_runtime_snapshot(
        &mut self,
        job_id: JobId,
        new_status: JobStatus,
        new_download_state: crate::jobs::model::DownloadState,
        new_post_state: crate::jobs::model::PostState,
        new_run_state: crate::jobs::model::RunState,
        persist_status: Option<&'static str>,
    ) {
        fn should_persist_runtime_to_db(
            old_run_state: crate::jobs::model::RunState,
            new_status: &JobStatus,
            new_run_state: crate::jobs::model::RunState,
        ) -> bool {
            matches!(
                new_status,
                JobStatus::Complete
                    | JobStatus::Partial
                    | JobStatus::Failed { .. }
                    | JobStatus::Paused
            ) || matches!(
                (old_run_state, new_run_state),
                (
                    crate::jobs::model::RunState::Paused,
                    crate::jobs::model::RunState::Active
                )
            )
        }

        let failpoint_name = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            if state.run_state != new_run_state
                && matches!(new_run_state, crate::jobs::model::RunState::Paused)
            {
                Some(crate::e2e_failpoint::STATUS_ENTER_PAUSED)
            } else if state.post_state == new_post_state {
                None
            } else {
                match new_post_state {
                    crate::jobs::model::PostState::Verifying => {
                        Some(Self::par2_verification_started_failpoint_name())
                    }
                    crate::jobs::model::PostState::Repairing => {
                        Some(crate::e2e_failpoint::STATUS_ENTER_REPAIRING)
                    }
                    crate::jobs::model::PostState::QueuedRepair => {
                        Some(crate::e2e_failpoint::STATUS_ENTER_QUEUED_REPAIR)
                    }
                    crate::jobs::model::PostState::QueuedExtract => {
                        Some(crate::e2e_failpoint::STATUS_ENTER_QUEUED_EXTRACT)
                    }
                    _ => None,
                }
            }
        };
        let (transitioned, persisted_snapshot_changed, persist_runtime_to_db, moved) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let old_status = state.status.clone();
            let old_download_state = state.download_state;
            let old_post_state = state.post_state;
            let old_run_state = state.run_state;
            let old_failure_error = state.failure_error.clone();
            let queued_repair_at = state.queued_repair_at_epoch_ms;
            let queued_extract_at = state.queued_extract_at_epoch_ms;
            let old_paused_resume_status = state.paused_resume_status.clone();
            let old_paused_resume_download_state = state.paused_resume_download_state;
            let old_paused_resume_post_state = state.paused_resume_post_state;
            let transitioned = old_status != new_status
                || old_download_state != new_download_state
                || old_post_state != new_post_state
                || old_run_state != new_run_state;
            let now = crate::jobs::model::epoch_ms_now();
            if let JobStatus::Failed { error } = &new_status {
                state.failure_error = Some(error.clone());
            } else if !matches!(new_status, JobStatus::Failed { .. }) {
                state.failure_error = None;
            }
            state.queued_repair_at_epoch_ms =
                if matches!(new_post_state, crate::jobs::model::PostState::QueuedRepair) {
                    queued_repair_at.or(Some(now))
                } else if matches!(new_run_state, crate::jobs::model::RunState::Paused)
                    && matches!(
                        state.paused_resume_post_state,
                        Some(crate::jobs::model::PostState::QueuedRepair)
                    )
                {
                    queued_repair_at
                } else {
                    None
                };
            state.queued_extract_at_epoch_ms =
                if matches!(new_post_state, crate::jobs::model::PostState::QueuedExtract) {
                    queued_extract_at.or(Some(now))
                } else if matches!(new_run_state, crate::jobs::model::RunState::Paused)
                    && matches!(
                        state.paused_resume_post_state,
                        Some(crate::jobs::model::PostState::QueuedExtract)
                    )
                {
                    queued_extract_at
                } else {
                    None
                };
            if !matches!(new_run_state, crate::jobs::model::RunState::Paused) {
                state.paused_resume_status = None;
                state.paused_resume_download_state = None;
                state.paused_resume_post_state = None;
            }
            state.status = new_status;
            state.download_state = new_download_state;
            state.post_state = new_post_state;
            state.run_state = new_run_state;
            let persisted_snapshot_changed = transitioned
                || state.failure_error != old_failure_error
                || state.queued_repair_at_epoch_ms != queued_repair_at
                || state.queued_extract_at_epoch_ms != queued_extract_at
                || state.paused_resume_status != old_paused_resume_status
                || state.paused_resume_download_state != old_paused_resume_download_state
                || state.paused_resume_post_state != old_paused_resume_post_state;
            let persist_runtime_to_db =
                should_persist_runtime_to_db(old_run_state, &state.status, state.run_state);
            (
                transitioned,
                persisted_snapshot_changed,
                persist_runtime_to_db,
                (old_post_state, new_post_state),
            )
        };

        self.metrics.post_state_moved(moved.0, moved.1);
        if let Some(persist_status) = persist_status {
            let changed_label = if persisted_snapshot_changed {
                "changed"
            } else {
                "unchanged"
            };
            crate::runtime::perf_probe::record_owned(
                format!(
                    "pipeline.persist_active_runtime.requested.{changed_label}.{persist_status}"
                ),
                std::time::Duration::from_nanos(1),
            );
            if persist_runtime_to_db {
                self.persist_active_runtime(job_id);
            } else {
                crate::runtime::perf_probe::record_owned(
                    format!("pipeline.persist_active_runtime.skipped.transient.{persist_status}"),
                    std::time::Duration::ZERO,
                );
            }
        }
        if let (true, Some(name)) = (transitioned, failpoint_name) {
            crate::e2e_failpoint::maybe_delay(name);
            crate::e2e_failpoint::maybe_trip(name);
        }
    }

    pub(crate) async fn reconcile_job_progress(&mut self, job_id: JobId) {
        self.schedule_job_completion_check(job_id);
    }

    pub(crate) fn transition_completed_runtime(&mut self, job_id: JobId) {
        // The job rules on what it delivered.
        let status = self
            .graph_terminal_status(job_id)
            .unwrap_or(JobStatus::Complete);
        let persisted = status.persisted_status();
        self.transition_postprocessing_status(job_id, status, Some(persisted));
    }

    pub(crate) fn pause_job_runtime(&mut self, job_id: JobId) -> Result<(), crate::SchedulerError> {
        let (previous_status, previous_download_state, previous_post_state, previous_run_state) =
            match self.jobs.get(&job_id) {
                Some(state) => (
                    state.status.clone(),
                    state.download_state,
                    state.post_state,
                    state.run_state,
                ),
                None => return Err(crate::SchedulerError::JobNotFound(job_id)),
            };
        if matches!(previous_run_state, crate::jobs::model::RunState::Paused) {
            return Ok(());
        }
        if self.awaiting_queue_script_barrier(job_id) {
            return Err(crate::SchedulerError::Conflict(
                "the download is complete and waiting for queue scripts".into(),
            ));
        }
        let pauseable_download_lane = matches!(
            previous_download_state,
            crate::jobs::model::DownloadState::Queued
                | crate::jobs::model::DownloadState::Downloading
        );
        let extracting_with_download_work = matches!(
            previous_post_state,
            crate::jobs::model::PostState::Extracting
        ) && self
            .job_has_pending_download_pipeline_work(job_id);
        if !(pauseable_download_lane || extracting_with_download_work) {
            return Err(crate::SchedulerError::Conflict(format!(
                "pause is only supported in queued or downloading states (current: {:?})",
                previous_status
            )));
        }

        let resume_status = crate::jobs::model::derive_legacy_job_status(
            previous_download_state,
            previous_post_state,
            crate::jobs::model::RunState::Active,
            None,
        );
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.paused_resume_status = Some(resume_status);
            state.paused_resume_download_state = Some(previous_download_state);
            state.paused_resume_post_state = Some(previous_post_state);
        }
        self.remove_pending_completion_check(job_id);
        self.transition_postprocessing_status(job_id, JobStatus::Paused, Some("paused"));
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);
        Ok(())
    }

    pub(crate) fn resume_job_runtime(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        if self.blocked_restores.contains_key(&job_id) {
            return Err(crate::SchedulerError::Conflict(
                "placement recovery must succeed before resume".into(),
            ));
        }
        let (resume_status, resume_download_state, resume_post_state) =
            match self.jobs.get_mut(&job_id) {
                Some(state) => {
                    if !matches!(state.run_state, crate::jobs::model::RunState::Paused) {
                        return Ok(());
                    }
                    let resume_status = state
                        .paused_resume_status
                        .take()
                        .unwrap_or(JobStatus::Downloading);
                    let (default_download_state, default_post_state, _) =
                        crate::jobs::model::runtime_lanes_from_status_snapshot(&resume_status);
                    let resume_download_state = state
                        .paused_resume_download_state
                        .take()
                        .unwrap_or(default_download_state);
                    let resume_post_state = state
                        .paused_resume_post_state
                        .take()
                        .unwrap_or(default_post_state);
                    let resume_status = crate::jobs::model::derive_legacy_job_status(
                        resume_download_state,
                        resume_post_state,
                        crate::jobs::model::RunState::Active,
                        None,
                    );
                    (resume_status, resume_download_state, resume_post_state)
                }
                None => return Err(crate::SchedulerError::JobNotFound(job_id)),
            };

        if matches!(
            resume_status,
            JobStatus::Complete
                | JobStatus::Partial
                | JobStatus::Failed { .. }
                | JobStatus::Moving
                | JobStatus::Paused
        ) {
            return Ok(());
        }
        let persist_status = Self::persist_active_status_for(&resume_status);
        self.transition_runtime_snapshot(
            job_id,
            resume_status.clone(),
            resume_download_state,
            resume_post_state,
            crate::jobs::model::RunState::Active,
            Some(persist_status),
        );
        self.resume_graph_work(job_id);
        if matches!(
            resume_download_state,
            crate::jobs::model::DownloadState::Downloading
        ) {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }

    pub(crate) fn transition_postprocessing_status(
        &mut self,
        job_id: JobId,
        new_status: JobStatus,
        persist_status: Option<&'static str>,
    ) {
        let (new_download_state, new_post_state, new_run_state) =
            crate::jobs::model::runtime_lanes_from_status_snapshot(&new_status);
        self.transition_runtime_snapshot(
            job_id,
            new_status,
            new_download_state,
            new_post_state,
            new_run_state,
            persist_status,
        );
    }
}
