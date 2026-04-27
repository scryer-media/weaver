use super::*;

impl Pipeline {
    fn member_artifact_root_for_restart_reconcile(&self, job_id: JobId) -> Option<PathBuf> {
        let state = self.jobs.get(&job_id)?;
        if let Some(staging_dir) = state.staging_dir.as_ref()
            && staging_dir.exists()
        {
            return Some(staging_dir.clone());
        }

        let deterministic_staging_dir = self.deterministic_extraction_staging_dir(job_id);
        if deterministic_staging_dir.exists() {
            return Some(deterministic_staging_dir);
        }

        Some(state.working_dir.clone())
    }

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
            return Some("status.enter_paused");
        }
        if old_post_state == new_post_state {
            return None;
        }
        match new_post_state {
            crate::jobs::model::PostState::Verifying => Some("status.enter_verifying"),
            crate::jobs::model::PostState::Repairing => Some("status.enter_repairing"),
            crate::jobs::model::PostState::QueuedRepair => Some("status.enter_queued_repair"),
            crate::jobs::model::PostState::QueuedExtract => Some("status.enter_queued_extract"),
            _ => None,
        }
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
        let (download_state, post_state, run_state) =
            crate::jobs::model::runtime_lanes_from_status_snapshot(&state.status);
        let download_state = download_state.as_str().to_string();
        let post_state = post_state.as_str().to_string();
        let run_state = run_state.as_str().to_string();
        let (paused_resume_download_state, paused_resume_post_state) = state
            .paused_resume_status
            .as_ref()
            .map(crate::jobs::model::runtime_lanes_from_status_snapshot)
            .map(|(download_state, post_state, _)| {
                (
                    Some(download_state.as_str().to_string()),
                    Some(post_state.as_str().to_string()),
                )
            })
            .unwrap_or((None, None));

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

    fn paused_resume_target(previous_status: &JobStatus) -> JobStatus {
        match previous_status {
            JobStatus::Queued => JobStatus::Queued,
            _ => JobStatus::Downloading,
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

        for member in &initial_missing_members {
            match self
                .recover_extracted_member_from_checkpoint(job_id, member)
                .await
            {
                Ok(true) => {
                    info!(
                        job_id = job_id.0,
                        member = %member,
                        "recovered missing extracted output from finalized checkpoint"
                    );
                }
                Ok(false) => {}
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        member = %member,
                        error = %error,
                        "failed to recover missing extracted output from checkpoint"
                    );
                }
            }
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

        for set_name in self.rar_set_names_for_job(job_id) {
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

    async fn recover_extracted_member_from_checkpoint(
        &mut self,
        job_id: JobId,
        member_name: &str,
    ) -> Result<bool, String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(false);
        };

        let Some((set_name, first_volume, last_volume, unpacked_size)) = state
            .assembly
            .archive_topologies()
            .iter()
            .find_map(|(set_name, topology)| {
                topology.members.iter().find_map(|member| {
                    (member.name == member_name).then_some((
                        set_name.clone(),
                        member.first_volume,
                        member.last_volume,
                        member.unpacked_size,
                    ))
                })
            })
        else {
            return Ok(false);
        };

        let Some(artifact_root) = self.member_artifact_root_for_restart_reconcile(job_id) else {
            return Ok(false);
        };
        let (out_path, partial_path) = Self::member_output_paths(&artifact_root, member_name);
        if out_path.exists() || !partial_path.exists() {
            return Ok(false);
        }

        let manifest_rows: Vec<crate::ExtractionChunk> = self
            .db
            .get_extraction_chunks(job_id, &set_name)
            .map_err(|error| format!("failed to load extraction checkpoint rows: {error}"))?
            .into_iter()
            .filter(|chunk| chunk.member_name == member_name)
            .collect();
        let Some(checkpoint) =
            Self::validate_member_extraction_manifest(&manifest_rows, first_volume, last_volume)
                .map_err(|error| format!("invalid extraction checkpoint manifest: {error}"))?
        else {
            return Ok(false);
        };
        info!(
            job_id = job_id.0,
            set_name = %set_name,
            member = %member_name,
            first_volume,
            last_volume,
            checkpoint_rows = manifest_rows.len(),
            checkpoint_manifest = ?manifest_rows
                .iter()
                .map(|chunk| (
                    chunk.volume_index,
                    chunk.bytes_written,
                    chunk.start_offset,
                    chunk.end_offset,
                ))
                .collect::<Vec<_>>(),
            next_offset = checkpoint.next_offset,
            unpacked_size,
            partial_path = %partial_path.display(),
            out_path = %out_path.display(),
            "attempting checkpoint-based extracted member recovery"
        );
        if checkpoint.next_offset < unpacked_size {
            return Ok(false);
        }

        let chunk_dir = Self::member_chunk_dir(&artifact_root, &set_name, member_name);
        let finalized_size = Self::finalize_member_output_paths(
            &self.db,
            &self.event_tx,
            job_id,
            &set_name,
            member_name,
            &partial_path,
            &out_path,
            &chunk_dir,
        )?;
        if finalized_size != unpacked_size {
            return Err(format!(
                "checkpoint finalized {finalized_size} bytes for {member_name}, expected {unpacked_size}"
            ));
        }

        self.extracted_members
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
        self.db
            .add_extracted_member(job_id, member_name, &out_path)
            .map_err(|error| format!("failed to persist recovered extracted member: {error}"))?;

        Ok(true)
    }

    pub(crate) async fn reconcile_job_progress(&mut self, job_id: JobId) {
        self.schedule_job_completion_check(job_id);
    }

    pub(crate) fn transition_completed_runtime(&mut self, job_id: JobId) {
        self.transition_postprocessing_status(job_id, JobStatus::Complete, Some("complete"));
    }

    pub(crate) fn pause_job_runtime(&mut self, job_id: JobId) -> Result<(), crate::SchedulerError> {
        let previous_status = match self.jobs.get(&job_id) {
            Some(state) => state.status.clone(),
            None => return Err(crate::SchedulerError::JobNotFound(job_id)),
        };
        if matches!(previous_status, JobStatus::Paused) {
            return Ok(());
        }
        if !matches!(previous_status, JobStatus::Queued | JobStatus::Downloading) {
            return Err(crate::SchedulerError::Conflict(format!(
                "pause is only supported in queued or downloading states (current: {:?})",
                previous_status
            )));
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
        let persist_status = Self::persist_active_status_for(&resume_status);
        self.transition_postprocessing_status(job_id, resume_status.clone(), Some(persist_status));
        if matches!(resume_status, JobStatus::Downloading) {
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
        let (transitioned, released_repair, released_extract, entered_repair, entered_extract) = {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            let old_status = state.status.clone();
            let queued_repair_at = state.queued_repair_at_epoch_ms;
            let queued_extract_at = state.queued_extract_at_epoch_ms;
            let transitioned = old_status != new_status;
            let released_repair = matches!(old_status, JobStatus::Repairing)
                && !matches!(new_status, JobStatus::Repairing);
            let released_extract = matches!(old_status, JobStatus::Extracting)
                && !matches!(new_status, JobStatus::Extracting);
            let entered_repair = !matches!(old_status, JobStatus::Repairing)
                && matches!(new_status, JobStatus::Repairing);
            let entered_extract = !matches!(old_status, JobStatus::Extracting)
                && matches!(new_status, JobStatus::Extracting);
            let now = crate::jobs::model::epoch_ms_now();
            if let JobStatus::Failed { error } = &new_status {
                state.failure_error = Some(error.clone());
            } else if !matches!(new_status, JobStatus::Failed { .. }) {
                state.failure_error = None;
            }
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
                state.paused_resume_download_state = None;
                state.paused_resume_post_state = None;
            }
            state.refresh_runtime_lanes_from_status();
            (
                transitioned,
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
    }
}
