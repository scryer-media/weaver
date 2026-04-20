use super::*;

impl Pipeline {
    pub(crate) fn persist_active_runtime(&self, job_id: JobId) {
        let Some((
            status,
            error,
            queued_repair_at_epoch_ms,
            queued_extract_at_epoch_ms,
            paused_resume_status,
        )) = self.jobs.get(&job_id).and_then(|state| {
            let status = Self::persist_active_status_for(&state.status)?;
            Some((
                status.to_string(),
                match &state.status {
                    JobStatus::Failed { error } => Some(error.clone()),
                    _ => None,
                },
                state.queued_repair_at_epoch_ms,
                state.queued_extract_at_epoch_ms,
                state
                    .paused_resume_status
                    .as_ref()
                    .and_then(Self::persist_active_status_for)
                    .map(str::to_string),
            ))
        })
        else {
            return;
        };

        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.set_active_job_runtime(
                job_id,
                &status,
                error.as_deref(),
                queued_repair_at_epoch_ms,
                queued_extract_at_epoch_ms,
                paused_resume_status.as_deref(),
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

    pub(crate) fn remove_pending_completion_check(&mut self, job_id: JobId) {
        self.pending_completion_checks
            .retain(|queued| *queued != job_id);
    }

    fn paused_resume_target(previous_status: &JobStatus) -> JobStatus {
        match previous_status {
            JobStatus::Queued => JobStatus::Queued,
            JobStatus::Downloading | JobStatus::Checking | JobStatus::Verifying => {
                JobStatus::Downloading
            }
            JobStatus::QueuedRepair | JobStatus::Repairing => JobStatus::QueuedRepair,
            JobStatus::QueuedExtract | JobStatus::Extracting => JobStatus::QueuedExtract,
            JobStatus::Paused => JobStatus::Downloading,
            JobStatus::Moving | JobStatus::Complete | JobStatus::Failed { .. } => {
                previous_status.clone()
            }
        }
    }

    fn persist_active_status_for(status: &JobStatus) -> Option<&'static str> {
        match status {
            JobStatus::Queued => Some("queued"),
            JobStatus::Downloading => Some("downloading"),
            JobStatus::Checking => Some("checking"),
            JobStatus::Verifying => Some("verifying"),
            JobStatus::QueuedRepair => Some("queued_repair"),
            JobStatus::Repairing => Some("repairing"),
            JobStatus::QueuedExtract => Some("queued_extract"),
            JobStatus::Extracting => Some("extracting"),
            JobStatus::Paused => Some("paused"),
            JobStatus::Moving | JobStatus::Complete | JobStatus::Failed { .. } => None,
        }
    }

    pub(crate) fn pause_job_runtime(&mut self, job_id: JobId) -> Result<(), crate::SchedulerError> {
        let previous_status = match self.jobs.get(&job_id) {
            Some(state) => state.status.clone(),
            None => return Err(crate::SchedulerError::JobNotFound(job_id)),
        };
        if matches!(previous_status, JobStatus::Paused) {
            return Ok(());
        }
        if matches!(
            previous_status,
            JobStatus::Complete | JobStatus::Failed { .. }
        ) {
            return Err(crate::SchedulerError::Conflict(format!(
                "cannot pause job in {:?} state",
                previous_status
            )));
        }
        if matches!(previous_status, JobStatus::Repairing) {
            return Err(crate::SchedulerError::Conflict(
                "cannot pause while PAR2 repair is running".to_string(),
            ));
        }
        if matches!(previous_status, JobStatus::Extracting)
            && self.job_has_active_extraction_tasks(job_id)
        {
            return Err(crate::SchedulerError::Conflict(
                "cannot pause while extraction tasks are running".to_string(),
            ));
        }

        let resume_status = Self::paused_resume_target(&previous_status);
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.paused_resume_status = Some(resume_status);
        }
        self.remove_pending_completion_check(job_id);
        self.transition_postprocessing_status(job_id, JobStatus::Paused, Some("paused"));
        Ok(())
    }

    pub(crate) fn resume_job_runtime(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        let resume_status = match self.jobs.get_mut(&job_id) {
            Some(state) => {
                if !matches!(state.status, JobStatus::Paused) {
                    return Ok(());
                }
                state
                    .paused_resume_status
                    .take()
                    .unwrap_or(JobStatus::Downloading)
            }
            None => return Err(crate::SchedulerError::JobNotFound(job_id)),
        };

        if matches!(
            resume_status,
            JobStatus::Complete | JobStatus::Failed { .. } | JobStatus::Moving | JobStatus::Paused
        ) {
            return Ok(());
        }

        self.transition_postprocessing_status(
            job_id,
            resume_status.clone(),
            Self::persist_active_status_for(&resume_status),
        );
        if matches!(
            resume_status,
            JobStatus::Downloading
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
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
        let failpoint_name = match &new_status {
            JobStatus::Verifying => Some("status.enter_verifying"),
            JobStatus::Repairing => Some("status.enter_repairing"),
            JobStatus::QueuedRepair => Some("status.enter_queued_repair"),
            JobStatus::QueuedExtract => Some("status.enter_queued_extract"),
            JobStatus::Paused => Some("status.enter_paused"),
            _ => None,
        };
        let (released_repair, released_extract, entered_repair, entered_extract) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let old_status = state.status.clone();
            let queued_repair_at = state.queued_repair_at_epoch_ms;
            let queued_extract_at = state.queued_extract_at_epoch_ms;
            let released_repair = matches!(old_status, JobStatus::Repairing)
                && !matches!(new_status, JobStatus::Repairing);
            let released_extract = matches!(old_status, JobStatus::Extracting)
                && !matches!(new_status, JobStatus::Extracting);
            let entered_repair = !matches!(old_status, JobStatus::Repairing)
                && matches!(new_status, JobStatus::Repairing);
            let entered_extract = !matches!(old_status, JobStatus::Extracting)
                && matches!(new_status, JobStatus::Extracting);
            let now = crate::jobs::model::epoch_ms_now();
            state.status = new_status;
            state.queued_repair_at_epoch_ms = if matches!(state.status, JobStatus::QueuedRepair) {
                queued_repair_at.or(Some(now))
            } else if matches!(state.status, JobStatus::Paused)
                && matches!(state.paused_resume_status, Some(JobStatus::QueuedRepair))
            {
                queued_repair_at
            } else {
                None
            };
            state.queued_extract_at_epoch_ms = if matches!(state.status, JobStatus::QueuedExtract) {
                queued_extract_at.or(Some(now))
            } else if matches!(state.status, JobStatus::Paused)
                && matches!(state.paused_resume_status, Some(JobStatus::QueuedExtract))
            {
                queued_extract_at
            } else {
                None
            };
            if !matches!(state.status, JobStatus::Paused) {
                state.paused_resume_status = None;
            }
            (
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
        if persist_status.is_some() {
            self.persist_active_runtime(job_id);
        }
        if let Some(name) = failpoint_name {
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
    }
}
