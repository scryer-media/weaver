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
        let error = state.failure_error.clone();
        let queued_repair_at_epoch_ms = state.queued_repair_at_epoch_ms;
        let queued_extract_at_epoch_ms = state.queued_extract_at_epoch_ms;
        let paused_resume_status = state
            .paused_resume_download_state
            .zip(state.paused_resume_post_state)
            .map(|(download_state, post_state)| {
                crate::jobs::model::derive_legacy_job_status(
                    download_state,
                    post_state,
                    crate::jobs::model::RunState::Active,
                    state.failure_error.as_deref(),
                )
            })
            .map(|status| Self::persist_active_status_for(&status).to_string());
        let download_state = state.download_state.as_str().to_string();
        let post_state = state.post_state.as_str().to_string();
        let run_state = state.run_state.as_str().to_string();
        let paused_resume_download_state = state
            .paused_resume_download_state
            .map(|value| value.as_str().to_string());
        let paused_resume_post_state = state
            .paused_resume_post_state
            .map(|value| value.as_str().to_string());

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
            .filter(|state| {
                matches!(state.run_state, crate::jobs::model::RunState::Active)
                    && matches!(state.post_state, crate::jobs::model::PostState::Repairing)
            })
            .count()
    }

    pub(crate) fn active_extract_jobs(&self) -> usize {
        self.jobs
            .values()
            .filter(|state| {
                matches!(state.run_state, crate::jobs::model::RunState::Active)
                    && matches!(state.post_state, crate::jobs::model::PostState::Extracting)
            })
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
            .filter(|(_, state)| {
                matches!(state.run_state, crate::jobs::model::RunState::Active)
                    && matches!(
                        state.post_state,
                        crate::jobs::model::PostState::QueuedRepair
                    )
            })
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
        if matches!(state.run_state, crate::jobs::model::RunState::Paused)
            || matches!(
                state.download_state,
                crate::jobs::model::DownloadState::Checking
                    | crate::jobs::model::DownloadState::Failed
            )
            || matches!(
                state.post_state,
                crate::jobs::model::PostState::Finalizing
                    | crate::jobs::model::PostState::Completed
                    | crate::jobs::model::PostState::Failed
            )
        {
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

    fn infer_download_state_from_queues(
        download_queue: &crate::pipeline::download::queue::DownloadQueue,
        recovery_queue: &crate::pipeline::download::queue::DownloadQueue,
    ) -> crate::jobs::model::DownloadState {
        if download_queue.is_empty() && recovery_queue.is_empty() {
            crate::jobs::model::DownloadState::Complete
        } else {
            crate::jobs::model::DownloadState::Downloading
        }
    }

    fn download_stage_has_runtime_activity(&self, job_id: JobId) -> bool {
        self.active_download_passes.contains(&job_id)
            || self.job_has_pending_download_pipeline_work(job_id)
    }

    fn derive_post_state_from_runtime(
        &self,
        job_id: JobId,
        current_post_state: crate::jobs::model::PostState,
    ) -> crate::jobs::model::PostState {
        if matches!(
            current_post_state,
            crate::jobs::model::PostState::Repairing
                | crate::jobs::model::PostState::QueuedRepair
                | crate::jobs::model::PostState::AwaitingRepair
                | crate::jobs::model::PostState::Verifying
                | crate::jobs::model::PostState::Finalizing
                | crate::jobs::model::PostState::Completed
                | crate::jobs::model::PostState::Failed
        ) {
            return current_post_state;
        }

        if self.job_has_active_extraction_tasks(job_id)
            || self
                .rar_sets
                .iter()
                .any(|((jid, _), state)| *jid == job_id && state.active_workers > 0)
        {
            return crate::jobs::model::PostState::Extracting;
        }

        let mut has_waiting_for_volumes = false;
        let mut has_awaiting_repair = false;
        let mut has_ready_extract = false;

        for set_name in self.rar_set_names_for_job(job_id) {
            let Some(set_state) = self.rar_sets.get(&(job_id, set_name)) else {
                continue;
            };
            let phase = set_state
                .plan
                .as_ref()
                .map(|plan| plan.phase)
                .unwrap_or(set_state.phase);
            match phase {
                crate::pipeline::archive::rar_state::RarSetPhase::WaitingForVolumes => {
                    has_waiting_for_volumes = true;
                }
                crate::pipeline::archive::rar_state::RarSetPhase::AwaitingRepair => {
                    has_awaiting_repair = true;
                }
                crate::pipeline::archive::rar_state::RarSetPhase::Ready
                | crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet => {
                    has_ready_extract = true;
                }
                crate::pipeline::archive::rar_state::RarSetPhase::Extracting
                | crate::pipeline::archive::rar_state::RarSetPhase::Complete => {}
            }
        }

        if has_awaiting_repair {
            crate::jobs::model::PostState::AwaitingRepair
        } else if has_waiting_for_volumes {
            crate::jobs::model::PostState::WaitingForVolumes
        } else if matches!(
            current_post_state,
            crate::jobs::model::PostState::QueuedExtract
        ) {
            if self.active_extract_jobs() >= self.tuner.max_concurrent_extractions()
                || has_ready_extract
            {
                crate::jobs::model::PostState::QueuedExtract
            } else {
                crate::jobs::model::PostState::Idle
            }
        } else {
            crate::jobs::model::PostState::Idle
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
        let Some((
            current_download_state,
            current_post_state,
            current_run_state,
            health_probing,
            download_queue_empty,
            recovery_queue_empty,
        )) = self.jobs.get(&job_id).map(|state| {
            (
                state.download_state,
                state.post_state,
                state.run_state,
                state.health_probing,
                state.download_queue.is_empty(),
                state.recovery_queue.is_empty(),
            )
        })
        else {
            return;
        };

        if matches!(current_run_state, crate::jobs::model::RunState::Paused)
            || matches!(
                current_post_state,
                crate::jobs::model::PostState::Finalizing
                    | crate::jobs::model::PostState::Completed
                    | crate::jobs::model::PostState::Failed
            )
            || matches!(
                current_download_state,
                crate::jobs::model::DownloadState::Failed
            )
        {
            return;
        }

        let next_download_state = if health_probing {
            crate::jobs::model::DownloadState::Checking
        } else if matches!(
            current_download_state,
            crate::jobs::model::DownloadState::Queued
        ) && self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            == 0
            && self
                .active_decodes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                == 0
            && self
                .pending_retries_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                == 0
            && !self.active_download_passes.contains(&job_id)
        {
            if download_queue_empty && recovery_queue_empty {
                crate::jobs::model::DownloadState::Complete
            } else {
                crate::jobs::model::DownloadState::Queued
            }
        } else if self.download_stage_has_runtime_activity(job_id) {
            crate::jobs::model::DownloadState::Downloading
        } else {
            crate::jobs::model::DownloadState::Complete
        };

        let next_post_state = self.derive_post_state_from_runtime(job_id, current_post_state);

        if next_download_state == current_download_state && next_post_state == current_post_state {
            if self.download_stage_has_runtime_activity(job_id) {
                self.schedule_job_completion_check(job_id);
            }
            return;
        }

        let Some((released_repair, released_extract, entered_repair, entered_extract)) =
            self.jobs.get_mut(&job_id).map(|state| {
                Self::apply_runtime_state_update(
                    state,
                    next_download_state,
                    next_post_state,
                    current_run_state,
                )
            })
        else {
            return;
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

        self.persist_active_runtime(job_id);

        if released_repair {
            self.promote_queued_repairs();
        }
        if released_extract {
            self.promote_queued_extractions();
        }

        if matches!(
            next_post_state,
            crate::jobs::model::PostState::WaitingForVolumes
                | crate::jobs::model::PostState::AwaitingRepair
                | crate::jobs::model::PostState::Extracting
                | crate::jobs::model::PostState::QueuedExtract
                | crate::jobs::model::PostState::Verifying
                | crate::jobs::model::PostState::QueuedRepair
                | crate::jobs::model::PostState::Repairing
        ) || matches!(
            next_download_state,
            crate::jobs::model::DownloadState::Queued
                | crate::jobs::model::DownloadState::Downloading
                | crate::jobs::model::DownloadState::Checking
        ) {
            self.schedule_job_completion_check(job_id);
        }
    }

    fn apply_runtime_state_update(
        state: &mut JobState,
        new_download_state: crate::jobs::model::DownloadState,
        new_post_state: crate::jobs::model::PostState,
        new_run_state: crate::jobs::model::RunState,
    ) -> (bool, bool, bool, bool) {
        let old_post_state = state.post_state;
        let released_repair = matches!(old_post_state, crate::jobs::model::PostState::Repairing)
            && !matches!(new_post_state, crate::jobs::model::PostState::Repairing);
        let released_extract = matches!(old_post_state, crate::jobs::model::PostState::Extracting)
            && !matches!(new_post_state, crate::jobs::model::PostState::Extracting);
        let entered_repair = !matches!(old_post_state, crate::jobs::model::PostState::Repairing)
            && matches!(new_post_state, crate::jobs::model::PostState::Repairing);
        let entered_extract = !matches!(old_post_state, crate::jobs::model::PostState::Extracting)
            && matches!(new_post_state, crate::jobs::model::PostState::Extracting);
        let now = crate::jobs::model::epoch_ms_now();

        state.download_state = new_download_state;
        state.post_state = new_post_state;
        state.run_state = new_run_state;
        state.queued_repair_at_epoch_ms = if matches!(
            state.post_state,
            crate::jobs::model::PostState::QueuedRepair
        ) {
            state.queued_repair_at_epoch_ms.or(Some(now))
        } else if matches!(state.run_state, crate::jobs::model::RunState::Paused)
            && matches!(
                state.paused_resume_post_state,
                Some(crate::jobs::model::PostState::QueuedRepair)
            )
        {
            state.queued_repair_at_epoch_ms
        } else {
            None
        };
        state.queued_extract_at_epoch_ms = if matches!(
            state.post_state,
            crate::jobs::model::PostState::QueuedExtract
        ) {
            state.queued_extract_at_epoch_ms.or(Some(now))
        } else if matches!(state.run_state, crate::jobs::model::RunState::Paused)
            && matches!(
                state.paused_resume_post_state,
                Some(crate::jobs::model::PostState::QueuedExtract)
            )
        {
            state.queued_extract_at_epoch_ms
        } else {
            None
        };
        if !matches!(state.run_state, crate::jobs::model::RunState::Paused) {
            state.paused_resume_download_state = None;
            state.paused_resume_post_state = None;
        }
        state.refresh_legacy_status();

        (
            released_repair,
            released_extract,
            entered_repair,
            entered_extract,
        )
    }

    pub(crate) fn transition_runtime_state(
        &mut self,
        job_id: JobId,
        new_download_state: crate::jobs::model::DownloadState,
        new_post_state: crate::jobs::model::PostState,
        new_run_state: crate::jobs::model::RunState,
    ) -> bool {
        let Some((
            changed,
            released_repair,
            released_extract,
            entered_repair,
            entered_extract,
            transition_failpoint,
        )) = self.jobs.get_mut(&job_id).map(|state| {
            let changed = state.download_state != new_download_state
                || state.post_state != new_post_state
                || state.run_state != new_run_state;
            let transition_failpoint = Self::status_enter_failpoint_for_transition(
                state.post_state,
                state.run_state,
                new_post_state,
                new_run_state,
            );
            let (released_repair, released_extract, entered_repair, entered_extract) =
                Self::apply_runtime_state_update(
                    state,
                    new_download_state,
                    new_post_state,
                    new_run_state,
                );
            (
                changed,
                released_repair,
                released_extract,
                entered_repair,
                entered_extract,
                transition_failpoint,
            )
        })
        else {
            return false;
        };
        if !changed {
            return false;
        }
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
        self.persist_active_runtime(job_id);
        if let Some(name) = transition_failpoint {
            crate::e2e_failpoint::maybe_delay(name);
            crate::e2e_failpoint::maybe_trip(name);
        }
        if released_repair {
            self.promote_queued_repairs();
        }
        if released_extract {
            self.promote_queued_extractions();
        }
        true
    }

    pub(crate) fn transition_completed_runtime(&mut self, job_id: JobId) {
        let _ = self.transition_runtime_state(
            job_id,
            crate::jobs::model::DownloadState::Complete,
            crate::jobs::model::PostState::Completed,
            crate::jobs::model::RunState::Active,
        );
    }

    pub(crate) fn transition_download_state(
        &mut self,
        job_id: JobId,
        new_download_state: crate::jobs::model::DownloadState,
    ) {
        let Some((current_post_state, current_run_state)) = self
            .jobs
            .get(&job_id)
            .map(|state| (state.post_state, state.run_state))
        else {
            return;
        };
        self.transition_runtime_state(
            job_id,
            new_download_state,
            current_post_state,
            current_run_state,
        );
    }

    pub(crate) fn transition_post_state(
        &mut self,
        job_id: JobId,
        new_post_state: crate::jobs::model::PostState,
    ) {
        let Some((current_download_state, current_run_state)) = self
            .jobs
            .get(&job_id)
            .map(|state| (state.download_state, state.run_state))
        else {
            return;
        };
        self.transition_runtime_state(
            job_id,
            current_download_state,
            new_post_state,
            current_run_state,
        );
    }

    pub(crate) fn transition_run_state(
        &mut self,
        job_id: JobId,
        new_run_state: crate::jobs::model::RunState,
    ) {
        let Some((current_download_state, current_post_state)) = self
            .jobs
            .get(&job_id)
            .map(|state| (state.download_state, state.post_state))
        else {
            return;
        };
        self.transition_runtime_state(
            job_id,
            current_download_state,
            current_post_state,
            new_run_state,
        );
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
        if matches!(
            previous_status,
            JobStatus::Complete | JobStatus::Failed { .. }
        ) {
            return Err(crate::SchedulerError::Conflict(format!(
                "cannot pause job in {:?} state",
                previous_status
            )));
        }
        if matches!(
            previous_post_state,
            crate::jobs::model::PostState::Repairing
        ) {
            return Err(crate::SchedulerError::Conflict(
                "cannot pause while PAR2 repair is running".to_string(),
            ));
        }
        if self.job_has_active_extraction_tasks(job_id) {
            return Err(crate::SchedulerError::Conflict(
                "cannot pause while extraction tasks are running".to_string(),
            ));
        }

        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.paused_resume_download_state = Some(previous_download_state);
            state.paused_resume_post_state = Some(previous_post_state);
        }
        self.remove_pending_completion_check(job_id);
        self.transition_run_state(job_id, crate::jobs::model::RunState::Paused);
        Ok(())
    }

    pub(crate) fn resume_job_runtime(
        &mut self,
        job_id: JobId,
    ) -> Result<(), crate::SchedulerError> {
        let (resume_download_state, resume_post_state) = match self.jobs.get_mut(&job_id) {
            Some(state) => {
                if !matches!(state.run_state, crate::jobs::model::RunState::Paused) {
                    return Ok(());
                }
                (
                    state
                        .paused_resume_download_state
                        .take()
                        .unwrap_or(crate::jobs::model::DownloadState::Downloading),
                    state
                        .paused_resume_post_state
                        .take()
                        .unwrap_or(crate::jobs::model::PostState::Idle),
                )
            }
            None => return Err(crate::SchedulerError::JobNotFound(job_id)),
        };

        self.transition_runtime_state(
            job_id,
            resume_download_state,
            resume_post_state,
            crate::jobs::model::RunState::Active,
        );
        if matches!(
            resume_post_state,
            crate::jobs::model::PostState::Verifying
                | crate::jobs::model::PostState::QueuedRepair
                | crate::jobs::model::PostState::QueuedExtract
                | crate::jobs::model::PostState::Extracting
                | crate::jobs::model::PostState::WaitingForVolumes
                | crate::jobs::model::PostState::AwaitingRepair
        ) || matches!(
            resume_download_state,
            crate::jobs::model::DownloadState::Queued
                | crate::jobs::model::DownloadState::Downloading
        ) {
            self.schedule_job_completion_check(job_id);
        }
        Ok(())
    }

    pub(crate) fn transition_postprocessing_status(
        &mut self,
        job_id: JobId,
        new_status: JobStatus,
        _persist_status: Option<&'static str>,
    ) {
        let Some((new_download_state, new_post_state, new_run_state)) =
            self.jobs.get_mut(&job_id).map(|state| match new_status {
                JobStatus::Queued => (
                    crate::jobs::model::DownloadState::Queued,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Downloading => (
                    crate::jobs::model::DownloadState::Downloading,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Checking => (
                    crate::jobs::model::DownloadState::Checking,
                    crate::jobs::model::PostState::Idle,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Verifying => (
                    Self::infer_download_state_from_queues(
                        &state.download_queue,
                        &state.recovery_queue,
                    ),
                    crate::jobs::model::PostState::Verifying,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::QueuedRepair => (
                    Self::infer_download_state_from_queues(
                        &state.download_queue,
                        &state.recovery_queue,
                    ),
                    crate::jobs::model::PostState::QueuedRepair,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Repairing => (
                    Self::infer_download_state_from_queues(
                        &state.download_queue,
                        &state.recovery_queue,
                    ),
                    crate::jobs::model::PostState::Repairing,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::QueuedExtract => (
                    state.download_state,
                    crate::jobs::model::PostState::QueuedExtract,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Extracting => (
                    state.download_state,
                    crate::jobs::model::PostState::Extracting,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Moving => (
                    crate::jobs::model::DownloadState::Complete,
                    crate::jobs::model::PostState::Finalizing,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Complete => (
                    crate::jobs::model::DownloadState::Complete,
                    crate::jobs::model::PostState::Completed,
                    crate::jobs::model::RunState::Active,
                ),
                JobStatus::Failed { error } => {
                    state.failure_error = Some(error);
                    (
                        crate::jobs::model::DownloadState::Failed,
                        crate::jobs::model::PostState::Failed,
                        crate::jobs::model::RunState::Active,
                    )
                }
                JobStatus::Paused => (
                    state.download_state,
                    state.post_state,
                    crate::jobs::model::RunState::Paused,
                ),
            })
        else {
            return;
        };

        let _ = self.transition_runtime_state(
            job_id,
            new_download_state,
            new_post_state,
            new_run_state,
        );
    }

    pub(crate) async fn maybe_start_repair(&mut self, job_id: JobId) -> bool {
        let Some((run_state, download_state, post_state)) = self
            .jobs
            .get(&job_id)
            .map(|state| (state.run_state, state.download_state, state.post_state))
        else {
            return false;
        };
        if matches!(run_state, crate::jobs::model::RunState::Paused)
            || matches!(
                download_state,
                crate::jobs::model::DownloadState::Checking
                    | crate::jobs::model::DownloadState::Failed
            )
            || matches!(
                post_state,
                crate::jobs::model::PostState::Finalizing
                    | crate::jobs::model::PostState::Completed
                    | crate::jobs::model::PostState::Failed
            )
        {
            return false;
        }
        if matches!(post_state, crate::jobs::model::PostState::Repairing) {
            return true;
        }
        if self.active_repair_jobs() >= MAX_CONCURRENT_REPAIRS {
            self.transition_post_state(job_id, crate::jobs::model::PostState::QueuedRepair);
            return false;
        }

        self.transition_post_state(job_id, crate::jobs::model::PostState::Repairing);
        let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });
        true
    }

    pub(crate) async fn maybe_start_extraction(&mut self, job_id: JobId) -> bool {
        let Some((run_state, download_state, post_state)) = self
            .jobs
            .get(&job_id)
            .map(|state| (state.run_state, state.download_state, state.post_state))
        else {
            return false;
        };
        if matches!(run_state, crate::jobs::model::RunState::Paused)
            || matches!(
                download_state,
                crate::jobs::model::DownloadState::Checking
                    | crate::jobs::model::DownloadState::Failed
            )
            || matches!(
                post_state,
                crate::jobs::model::PostState::Finalizing
                    | crate::jobs::model::PostState::Completed
                    | crate::jobs::model::PostState::Failed
            )
        {
            return false;
        }
        if matches!(post_state, crate::jobs::model::PostState::Extracting) {
            return true;
        }
        if self.active_extract_jobs() >= self.tuner.max_concurrent_extractions() {
            self.transition_post_state(job_id, crate::jobs::model::PostState::QueuedExtract);
            return false;
        }

        self.transition_post_state(job_id, crate::jobs::model::PostState::Extracting);
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
                (matches!(state.run_state, crate::jobs::model::RunState::Active)
                    && matches!(
                        state.post_state,
                        crate::jobs::model::PostState::QueuedExtract
                    ))
                .then_some((
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
            self.transition_post_state(job_id, crate::jobs::model::PostState::Extracting);
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
