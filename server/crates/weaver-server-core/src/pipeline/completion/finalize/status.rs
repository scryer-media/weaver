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

    pub(crate) fn trip_par2_verification_started_failpoint() {
        let name = Self::par2_verification_started_failpoint_name();
        crate::e2e_failpoint::maybe_delay(name);
        crate::e2e_failpoint::maybe_trip(name);
    }

    pub(crate) fn persist_active_runtime(&self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
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

        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.set_active_job_runtime(
                job_id,
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
            ) {
                tracing::error!(
                    error = %error,
                    status,
                    "db write failed for active job runtime"
                );
            }
        });
    }

    pub(crate) fn active_repair_jobs(&self) -> usize {
        self.jobs
            .values()
            .filter(|state| matches!(state.status, JobStatus::Repairing))
            .count()
    }

    pub(crate) fn active_extract_jobs(&self) -> usize {
        self.jobs
            .values()
            .filter(|state| matches!(state.status, JobStatus::Extracting))
            .count()
    }

    pub(crate) fn job_has_active_extraction_tasks(&self, job_id: JobId) -> bool {
        self.has_active_rar_workers(job_id)
            || self
                .inflight_extractions
                .get(&job_id)
                .is_some_and(|sets| !sets.is_empty())
    }

    fn next_queued_repair_job(&self) -> Option<JobId> {
        self.jobs
            .iter()
            .filter(|(_, state)| matches!(state.status, JobStatus::QueuedRepair))
            .min_by(|(job_id_a, state_a), (job_id_b, state_b)| {
                state_a
                    .queued_repair_at_epoch_ms
                    .unwrap_or(state_a.created_at_epoch_ms)
                    .total_cmp(
                        &state_b
                            .queued_repair_at_epoch_ms
                            .unwrap_or(state_b.created_at_epoch_ms),
                    )
                    .then_with(|| job_id_a.0.cmp(&job_id_b.0))
            })
            .map(|(job_id, _)| *job_id)
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
                | JobStatus::Failed { .. }
        ) {
            return;
        }
        if !self.pending_completion_checks.contains(&job_id) {
            self.pending_completion_checks.push_back(job_id);
        }
    }

    pub(crate) fn schedule_job_completion_check_if_download_pipeline_drained(
        &mut self,
        job_id: JobId,
        reason: &'static str,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        if matches!(
            state.status,
            JobStatus::Paused
                | JobStatus::Checking
                | JobStatus::Moving
                | JobStatus::Complete
                | JobStatus::Failed { .. }
        ) {
            return;
        }
        if self.job_has_pending_download_pipeline_work(job_id)
            || self.pending_completion_checks.contains(&job_id)
        {
            return;
        }

        debug!(
            job_id = job_id.0,
            reason, "scheduling completion check after download pipeline drained"
        );
        self.pending_completion_checks.push_back(job_id);
    }

    pub(crate) fn remove_pending_completion_check(&mut self, job_id: JobId) {
        self.pending_completion_checks
            .retain(|queued| *queued != job_id);
    }

    pub(crate) fn persist_active_status_for(status: &JobStatus) -> &'static str {
        match status {
            JobStatus::Queued => "queued",
            JobStatus::Downloading => "downloading",
            JobStatus::Checking => "checking",
            JobStatus::Verifying => "verifying",
            JobStatus::QueuedRepair => "queued_repair",
            JobStatus::Repairing => "repairing",
            JobStatus::QueuedExtract => "queued_extract",
            JobStatus::Extracting => "extracting",
            JobStatus::Moving => "moving",
            JobStatus::Complete => "complete",
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
                JobStatus::Complete | JobStatus::Failed { .. } | JobStatus::Paused
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
        let (
            transitioned,
            persisted_snapshot_changed,
            persist_runtime_to_db,
            released_repair,
            released_extract,
            entered_repair,
            entered_extract,
        ) = {
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
            let released_repair =
                matches!(old_post_state, crate::jobs::model::PostState::Repairing)
                    && !matches!(new_post_state, crate::jobs::model::PostState::Repairing);
            let released_extract =
                matches!(old_post_state, crate::jobs::model::PostState::Extracting)
                    && !matches!(new_post_state, crate::jobs::model::PostState::Extracting);
            let entered_repair =
                !matches!(old_post_state, crate::jobs::model::PostState::Repairing)
                    && matches!(new_post_state, crate::jobs::model::PostState::Repairing);
            let entered_extract =
                !matches!(old_post_state, crate::jobs::model::PostState::Extracting)
                    && matches!(new_post_state, crate::jobs::model::PostState::Extracting);
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
                released_repair,
                released_extract,
                entered_repair,
                entered_extract,
            )
        };

        if released_repair {
            self.metrics.repair_active.fetch_sub(1, Ordering::Relaxed);
        }
        if released_extract {
            self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
        }
        if entered_repair {
            self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
        }
        if entered_extract {
            self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
        }
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
        if released_repair {
            self.promote_queued_repairs();
        }
        if released_extract {
            self.promote_queued_extractions();
        }
    }

    pub(crate) async fn reconcile_extracted_outputs_for_completion(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let Some(existing_members) = self.extracted_members.get(&job_id).cloned() else {
            return false;
        };
        if existing_members.is_empty() {
            return false;
        }

        let initial_missing_members: Vec<String> = existing_members
            .iter()
            .filter(|member| {
                self.resolve_job_input_path(job_id, member)
                    .is_none_or(|path| !path.exists())
            })
            .cloned()
            .collect();
        if initial_missing_members.is_empty() {
            return false;
        }

        let missing_members: Vec<String> = existing_members
            .iter()
            .filter(|member| {
                self.resolve_job_input_path(job_id, member)
                    .is_none_or(|path| !path.exists())
            })
            .cloned()
            .collect();
        if missing_members.is_empty() {
            return false;
        }

        warn!(
            job_id = job_id.0,
            missing_members = ?missing_members,
            "reconciling stale extracted output records"
        );

        let missing_set: std::collections::HashSet<String> =
            missing_members.iter().cloned().collect();
        if let Some(extracted_members) = self.extracted_members.get_mut(&job_id) {
            extracted_members.retain(|member| !missing_set.contains(member));
            if extracted_members.is_empty() {
                self.extracted_members.remove(&job_id);
            }
        }
        if let Some(pending_concat) = self.pending_concat.get_mut(&job_id) {
            pending_concat.retain(|member| !missing_set.contains(member));
            if pending_concat.is_empty() {
                self.pending_concat.remove(&job_id);
            }
        }
        self.extracted_archives.remove(&job_id);
        self.clear_persisted_extracted_members(job_id);
        let set_names = self.rar_set_names_for_job(job_id);
        for member in &missing_members {
            if let Err(error) = self.db.clear_member_chunks_for_all_sets(job_id, member) {
                warn!(
                    job_id = job_id.0,
                    member = %member,
                    error = %error,
                    "failed to clear stale extracted member chunks after reconciliation"
                );
            }
        }

        if let Some(remaining_members) = self.extracted_members.get(&job_id) {
            for member in remaining_members {
                if let Some(path) = self.resolve_job_input_path(job_id, member)
                    && let Err(error) = self.db.add_extracted_member(job_id, member, &path)
                {
                    warn!(
                        job_id = job_id.0,
                        member = %member,
                        error = %error,
                        "failed to rebuild persisted extracted member after reconciliation"
                    );
                }
            }
        }

        for set_name in set_names {
            if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                warn!(
                    job_id = job_id.0,
                    set_name = %set_name,
                    error,
                    "failed to recompute RAR set after extracted output reconciliation"
                );
            }
        }

        true
    }

    pub(crate) async fn reconcile_job_progress(&mut self, job_id: JobId) {
        self.schedule_job_completion_check(job_id);
    }

    pub(crate) fn transition_completed_runtime(&mut self, job_id: JobId) {
        self.transition_postprocessing_status(job_id, JobStatus::Complete, Some("complete"));
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
        Ok(())
    }

    pub(crate) fn resume_job_runtime(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
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
            JobStatus::Complete | JobStatus::Failed { .. } | JobStatus::Moving | JobStatus::Paused
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

    pub(crate) async fn maybe_start_repair(&mut self, job_id: JobId) -> bool {
        let Some(status) = self.jobs.get(&job_id).map(|state| state.status.clone()) else {
            return false;
        };
        if matches!(
            status,
            JobStatus::Paused
                | JobStatus::Checking
                | JobStatus::Moving
                | JobStatus::Complete
                | JobStatus::Failed { .. }
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        ) {
            return false;
        }
        if matches!(status, JobStatus::Repairing) {
            return true;
        }
        if self.active_repair_jobs() >= MAX_CONCURRENT_REPAIRS {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::QueuedRepair,
                Some("queued_repair"),
            );
            return false;
        }

        self.transition_postprocessing_status(job_id, JobStatus::Repairing, Some("repairing"));
        let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });
        true
    }

    pub(crate) async fn maybe_start_extraction(&mut self, job_id: JobId) -> bool {
        let Some(status) = self.jobs.get(&job_id).map(|state| state.status.clone()) else {
            return false;
        };
        if matches!(
            status,
            JobStatus::Paused
                | JobStatus::Checking
                | JobStatus::Moving
                | JobStatus::Complete
                | JobStatus::Failed { .. }
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
        ) {
            return false;
        }
        if matches!(status, JobStatus::Extracting) {
            return true;
        }
        if self.active_extract_jobs() >= self.tuner.max_concurrent_extractions() {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::QueuedExtract,
                Some("queued_extract"),
            );
            return false;
        }

        self.transition_postprocessing_status(job_id, JobStatus::Extracting, Some("extracting"));
        let _ = self
            .event_tx
            .send(PipelineEvent::ExtractionReady { job_id });
        info!(job_id = job_id.0, "extraction ready");
        true
    }

    pub(crate) fn promote_queued_repairs(&mut self) {
        if self.active_repair_jobs() >= MAX_CONCURRENT_REPAIRS {
            return;
        }
        let Some(job_id) = self.next_queued_repair_job() else {
            return;
        };
        self.transition_postprocessing_status(job_id, JobStatus::Repairing, Some("repairing"));
        let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });
        self.schedule_job_completion_check(job_id);
    }

    pub(crate) fn promote_queued_extractions(&mut self) {
        let available = self
            .tuner
            .max_concurrent_extractions()
            .saturating_sub(self.active_extract_jobs());
        if available == 0 {
            return;
        }
        let mut candidates: Vec<(JobId, f64)> = self
            .jobs
            .iter()
            .filter_map(|(job_id, state)| {
                matches!(state.status, JobStatus::QueuedExtract).then_some((
                    *job_id,
                    state
                        .queued_extract_at_epoch_ms
                        .unwrap_or(state.created_at_epoch_ms),
                ))
            })
            .collect();
        candidates.sort_by(|(job_id_a, queued_at_a), (job_id_b, queued_at_b)| {
            queued_at_a
                .total_cmp(queued_at_b)
                .then_with(|| job_id_a.0.cmp(&job_id_b.0))
        });
        for (job_id, _) in candidates.into_iter().take(available) {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Extracting,
                Some("extracting"),
            );
            let _ = self
                .event_tx
                .send(PipelineEvent::ExtractionReady { job_id });
            self.schedule_job_completion_check(job_id);
        }
    }

    pub(super) async fn cleanup_par2_files(&self, job_id: JobId) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let cleanup_dir = state.working_dir.clone();
        let par2_files: Vec<String> = state
            .assembly
            .files()
            .filter(|f| matches!(f.role(), weaver_model::files::FileRole::Par2 { .. }))
            .map(|f| self.current_filename_for_file(job_id, f))
            .collect();
        if par2_files.is_empty() {
            return;
        }

        let mut removed = 0u32;
        for filename in &par2_files {
            let path = cleanup_dir.join(filename);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "failed to delete PAR2 file");
                }
            }
        }
        if removed > 0 {
            info!(
                job_id = job_id.0,
                removed,
                total = par2_files.len(),
                "deleted PAR2 files"
            );
        }
    }

    pub(crate) fn clear_par2_runtime_state(&mut self, job_id: JobId) {
        self.par2_runtime.remove(&job_id);
        self.par2_verified.remove(&job_id);
        self.unavailable_promoted_recovery_segments
            .retain(|segment_id| segment_id.file_id.job_id != job_id);
    }
}
