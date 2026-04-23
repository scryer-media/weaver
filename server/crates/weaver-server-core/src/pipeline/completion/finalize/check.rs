use super::*;
use std::collections::{HashMap, HashSet};

impl Pipeline {
    pub(crate) fn job_has_pending_download_pipeline_work(&self, job_id: JobId) -> bool {
        let has_queued_work = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.health_probing || !state.download_queue.is_empty());
        let has_inflight_downloads = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_inflight_decodes = self
            .active_decodes_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_delayed_retries = self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;
        let has_pending_decode = self
            .pending_decode
            .iter()
            .any(|work| work.segment_id.file_id.job_id == job_id);
        let has_buffered_segments = self
            .write_buffers
            .iter()
            .any(|(file_id, write_buf)| file_id.job_id == job_id && write_buf.buffered_len() > 0);

        has_queued_work
            || has_inflight_downloads
            || has_inflight_decodes
            || has_delayed_retries
            || has_pending_decode
            || has_buffered_segments
    }
}

impl Pipeline {
    async fn try_deobfuscate_files_with_par2(&mut self, job_id: JobId) -> usize {
        let Some(par2) = self.par2_set(job_id).cloned() else {
            return 0;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let rename_dir = state.working_dir.clone();

        if weaver_nzb::is_protected_media_structure(&rename_dir) {
            info!(
                job_id = job_id.0,
                "skipping PAR2 rename inside protected media structure"
            );
            return 0;
        }

        let suggestions = match weaver_par2::scan_for_renames(&rename_dir, &par2) {
            Ok(suggestions) => suggestions,
            Err(error) => {
                warn!(
                    job_id = job_id.0,
                    error = %error,
                    "PAR2 rename scan failed"
                );
                return 0;
            }
        };

        let file_rows: Vec<(NzbFileId, crate::jobs::record::ActiveFileIdentity, bool)> = state
            .assembly
            .files()
            .filter_map(|file| {
                self.effective_file_identity(job_id, file.file_id())
                    .map(|identity| (file.file_id(), identity, file.is_complete()))
            })
            .collect();
        let mut by_current = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_source = HashMap::<String, (NzbFileId, bool)>::new();
        let mut by_canonical = HashMap::<String, (NzbFileId, bool)>::new();
        for (file_id, identity, is_complete) in &file_rows {
            by_current.insert(identity.current_filename.clone(), (*file_id, *is_complete));
            by_source.insert(identity.source_filename.clone(), (*file_id, *is_complete));
            if let Some(canonical) = identity.canonical_filename.as_ref() {
                by_canonical.insert(canonical.clone(), (*file_id, *is_complete));
            }
        }
        let _ = state;

        let mut renamed = 0usize;
        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        for suggestion in &suggestions {
            let old = &suggestion.current_path;
            let new = old.parent().unwrap().join(&suggestion.correct_name);
            let old_name = old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            let matched = by_current
                .get(&old_name)
                .copied()
                .or_else(|| by_source.get(&old_name).copied())
                .or_else(|| by_canonical.get(&old_name).copied());
            if old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                == Some(suggestion.correct_name.clone())
            {
                continue;
            }

            match std::fs::rename(old, &new) {
                Ok(()) => {
                    renamed += 1;
                    info!(
                        job_id = job_id.0,
                        from = %old.file_name().unwrap().to_string_lossy(),
                        to = %suggestion.correct_name,
                        "deobfuscated file via PAR2 metadata"
                    );
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        from = %old.display(),
                        to = %new.display(),
                        error = %error,
                        "PAR2 rename failed"
                    );
                }
            }

            if let Some((file_id, is_complete)) = matched {
                let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
                    .cloned()
                else {
                    continue;
                };
                let old_current_filename = identity.current_filename.clone();
                let old_rar_set_name =
                    identity.classification.as_ref().and_then(|classification| {
                        matches!(
                            classification.kind,
                            crate::jobs::assembly::DetectedArchiveKind::Rar
                        )
                        .then(|| classification.set_name.clone())
                    });
                let classification =
                    Self::canonical_archive_identity_from_filename(&suggestion.correct_name)
                        .or(identity.classification.clone());
                if let Some(set_name) = old_rar_set_name {
                    touched_rar_files
                        .entry(set_name)
                        .or_default()
                        .insert(old_current_filename);
                }
                let mut rebound_identity = identity;
                rebound_identity.current_filename = suggestion.correct_name.clone();
                rebound_identity.canonical_filename = Some(suggestion.correct_name.clone());
                rebound_identity.classification = classification;
                rebound_identity.classification_source =
                    crate::jobs::record::FileIdentitySource::Par2;
                if let Err(error) = self.set_file_identity(job_id, rebound_identity) {
                    warn!(
                        job_id = job_id.0,
                        file_index = file_id.file_index,
                        error = %error,
                        "failed to persist PAR2 deobfuscation identity"
                    );
                } else if is_complete {
                    touched_files.push(file_id);
                }
            }
        }

        for (set_name, touched_filenames) in &touched_rar_files {
            self.invalidate_archive_set_for_identity_rebind(job_id, set_name, touched_filenames);
        }
        for file_id in touched_files {
            self.refresh_archive_state_for_completed_file(job_id, file_id, false)
                .await;
        }

        if renamed > 0 {
            info!(job_id = job_id.0, renamed, "PAR2 deobfuscation complete");
        }

        renamed
    }

    async fn verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<weaver_par2::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
        emit_events: bool,
    ) -> Result<(weaver_par2::VerificationResult, weaver_par2::PlacementPlan), String> {
        if emit_events {
            if !preserve_repairing_status {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Verifying,
                    Some("verifying"),
                );
            } else {
                info!(
                    job_id = job_id.0,
                    "rerunning PAR2 verification while preserving restored repair slot"
                );
            }
            let _ = self
                .event_tx
                .send(PipelineEvent::JobVerificationStarted { job_id });
            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
            });
        }

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 verification started");

        let verify_dir = working_dir.clone();
        let pp_pool = self.pp_pool.clone();
        let verify_result = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                let plan = weaver_par2::scan_placement(&verify_dir, &par2_set)
                    .map_err(|error| format!("placement scan failed: {error}"))?;
                if !plan.conflicts.is_empty() {
                    return Err(format!(
                        "placement scan found {} conflicting file matches",
                        plan.conflicts.len()
                    ));
                }

                let file_access =
                    weaver_par2::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                Ok((weaver_par2::verify_all(&par2_set, &file_access), plan))
            })
        })
        .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        let (mut verification, placement_plan) = match verify_result {
            Ok(Ok(result)) => result,
            Ok(Err(message)) => return Err(message),
            Err(error) => return Err(format!("verification task panicked: {error}")),
        };
        Self::log_placement_plan(job_id, &placement_plan);

        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, &mut verification);
        if skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks, "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks, "retained suspect eagerly-deleted volumes in damage count"
            );
        }

        self.recompute_volume_safety_from_verification(job_id, &verification);

        if emit_events {
            let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
                job_id,
                passed: verification.total_missing_blocks == 0,
            });
        }

        Ok((verification, placement_plan))
    }

    async fn reconcile_verified_par2_files(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) -> Result<usize, String> {
        let files_to_complete: Vec<(NzbFileId, String, u64)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(0);
            };

            let mut by_name = HashMap::<String, (NzbFileId, u64, bool)>::new();
            for file in state.assembly.files() {
                let file_id = file.file_id();
                let total_bytes = file.total_bytes();
                let is_complete = file.is_complete();
                let identity = self.effective_file_identity(job_id, file_id);
                let current_filename = identity
                    .as_ref()
                    .map(|value| value.current_filename.clone())
                    .unwrap_or_else(|| file.filename().to_string());
                by_name
                    .entry(current_filename)
                    .or_insert((file_id, total_bytes, is_complete));
                if let Some(identity) = identity {
                    by_name.entry(identity.source_filename.clone()).or_insert((
                        file_id,
                        total_bytes,
                        is_complete,
                    ));
                    if let Some(canonical) = identity.canonical_filename {
                        by_name
                            .entry(canonical)
                            .or_insert((file_id, total_bytes, is_complete));
                    }
                }
            }

            let mut matched = HashMap::<NzbFileId, (String, u64)>::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    weaver_par2::verify::FileStatus::Complete
                        | weaver_par2::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }

                let mut candidate_names = vec![file_verification.filename.clone()];
                if let weaver_par2::verify::FileStatus::Renamed(path) = &file_verification.status
                    && let Some(filename) = path.file_name()
                {
                    candidate_names.push(filename.to_string_lossy().to_string());
                }

                for candidate_name in &candidate_names {
                    let Some((file_id, total_bytes, is_complete)) =
                        by_name.get(candidate_name).copied()
                    else {
                        continue;
                    };
                    if is_complete {
                        break;
                    }
                    let current_filename = self
                        .current_filename_for_file_id(job_id, file_id)
                        .unwrap_or_else(|| candidate_name.clone());
                    matched
                        .entry(file_id)
                        .or_insert((current_filename, total_bytes));
                    break;
                }
            }

            matched
                .into_iter()
                .map(|(file_id, (filename, total_bytes))| (file_id, filename, total_bytes))
                .collect()
        };

        if files_to_complete.is_empty() {
            return Ok(0);
        }

        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(0);
            };
            for (file_id, _, _) in &files_to_complete {
                let Some(file) = state.assembly.file_mut(*file_id) else {
                    continue;
                };
                file.mark_complete();
            }
        }

        for (file_id, filename, total_bytes) in &files_to_complete {
            self.note_file_progress_floor(*file_id, *total_bytes, true);
            let file_index = file_id.file_index;
            let current_filename = filename.clone();
            self.db_blocking(move |db| {
                db.complete_file(job_id, file_index, &current_filename, &[0u8; 16])
            })
            .await
            .map_err(|error| {
                format!(
                    "failed to persist PAR2-reconciled file {}: {error}",
                    filename
                )
            })?;
            self.pending_file_progress.remove(file_id);
            self.persisted_file_progress.remove(file_id);
            self.refresh_archive_state_for_completed_file(job_id, *file_id, true)
                .await;
        }

        Ok(files_to_complete.len())
    }

    async fn check_rar_job_completion(&mut self, job_id: JobId) {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return;
        }

        let has_active_worker = set_names.iter().any(|set_name| {
            self.rar_sets
                .get(&(job_id, set_name.clone()))
                .is_some_and(|state| state.active_workers > 0)
        });
        if has_active_worker {
            if self.jobs.get(&job_id).is_some_and(|state| {
                !matches!(state.post_state, crate::jobs::model::PostState::Extracting)
            }) {
                self.transition_post_state(job_id, crate::jobs::model::PostState::Extracting);
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
            }
            return;
        }

        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let mut forced_recompute = false;
        let (fallback_sets, has_incomplete_sets, has_ready_incremental_work) = loop {
            let mut fallback_sets = Vec::new();
            let mut has_incomplete_sets = false;
            let mut has_ready_incremental_work = false;
            let mut impossible_sets = Vec::new();

            for set_name in &set_names {
                let set_state = self.rar_sets.get(&(job_id, set_name.clone()));
                let set_complete = extracted_archives.contains(set_name)
                    || set_state
                        .and_then(|state| state.plan.as_ref())
                        .is_some_and(|plan| {
                            !plan.member_names.is_empty()
                                && plan
                                    .member_names
                                    .iter()
                                    .all(|member| extracted.contains(member))
                        });
                if set_complete {
                    self.extracted_archives
                        .entry(job_id)
                        .or_default()
                        .insert(set_name.clone());
                    continue;
                }

                has_incomplete_sets = true;
                if let Some(state) = set_state
                    && let Some(plan) = state.plan.as_ref()
                {
                    if matches!(
                        plan.phase,
                        crate::pipeline::archive::rar_state::RarSetPhase::FallbackFullSet
                    ) {
                        fallback_sets.push(set_name.clone());
                    } else if !plan.ready_members.is_empty() {
                        has_ready_incremental_work = true;
                    } else if plan.waiting_on_volumes.is_empty() {
                        impossible_sets.push(set_name.clone());
                    }
                } else {
                    fallback_sets.push(set_name.clone());
                }
            }

            if impossible_sets.is_empty() {
                break (
                    fallback_sets,
                    has_incomplete_sets,
                    has_ready_incremental_work,
                );
            }

            if forced_recompute {
                let set_list = impossible_sets.join(", ");
                let msg = format!(
                    "invalid RAR state after recompute: sets [{set_list}] are incomplete with no ready members, no fallback, and no waiting volumes"
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            forced_recompute = true;
            for set_name in impossible_sets {
                if let Err(error) = self.recompute_rar_set_state(job_id, &set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error,
                        "failed forced RAR recompute for impossible state"
                    );
                }
            }
        };

        if has_incomplete_sets {
            if (has_ready_incremental_work || !fallback_sets.is_empty())
                && !self.maybe_start_extraction(job_id).await
            {
                return;
            }

            if has_ready_incremental_work {
                self.try_rar_extraction(job_id).await;
                return;
            }

            for set_name in &fallback_sets {
                if let Err(error) = self.extract_rar_set(job_id, set_name).await {
                    warn!(
                        job_id = job_id.0,
                        set_name = %set_name,
                        error = %error,
                        "failed to start RAR full-set extraction"
                    );
                    self.fail_job(job_id, error);
                    return;
                }
            }
            if !fallback_sets.is_empty() {
                return;
            }

            self.reconcile_job_progress(job_id).await;
            return;
        }

        self.finalize_completed_archive_job(job_id).await;
    }

    fn only_archive_residuals_or_loaded_par2_index_are_incomplete(&self, job_id: JobId) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        if self.job_has_active_extraction_tasks(job_id) {
            return false;
        }

        let extracted_archives = self
            .extracted_archives
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let extracted_members = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let par2_loaded = self.par2_set(job_id).is_some();
        let mut saw_incomplete = false;

        for file in state.assembly.files() {
            if file.is_complete() {
                continue;
            }
            match self.classified_role_for_file(job_id, file) {
                weaver_model::files::FileRole::Par2 {
                    is_index: false, ..
                } => {}
                weaver_model::files::FileRole::Par2 { is_index: true, .. } if par2_loaded => {
                    saw_incomplete = true;
                }
                _ => {
                    let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file)
                    else {
                        return false;
                    };
                    let set_complete = extracted_archives.contains(&set_name)
                        || self
                            .rar_sets
                            .get(&(job_id, set_name.clone()))
                            .and_then(|state| state.plan.as_ref())
                            .is_some_and(|plan| {
                                !plan.member_names.is_empty()
                                    && plan
                                        .member_names
                                        .iter()
                                        .all(|member| extracted_members.contains(member))
                            });
                    if !set_complete {
                        return false;
                    }
                    saw_incomplete = true;
                }
            }
        }

        saw_incomplete
    }

    /// Check if all data files in a job are complete, and trigger post-processing.
    ///
    /// PAR2 is treated as a repair tool only — damage is detected via yEnc CRC
    /// (per-segment) and RAR CRC (per-member extraction). If
    /// CRC failures occur, recovery files are promoted for download and repair
    /// runs from disk using `verify_all` + `plan_repair` + `execute_repair`.
    pub(crate) async fn check_job_completion(&mut self, job_id: JobId) {
        let current_status = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state.status.clone()
        };
        let (total_data_files, complete_data_files, failed_bytes, queued_downloads) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            (
                state.assembly.data_file_count(),
                state.assembly.complete_data_file_count(),
                state.failed_bytes,
                !state.download_queue.is_empty(),
            )
        };
        let has_incomplete_data_files = complete_data_files < total_data_files;

        // Step 1: Are all data files (non-recovery) complete?
        {
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
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total_data_files == 0
                && matches!(
                    state.download_state,
                    crate::jobs::model::DownloadState::Queued
                        | crate::jobs::model::DownloadState::Downloading
                )
                && queued_downloads
            {
                return;
            }
        }

        if matches!(current_status, JobStatus::QueuedRepair) {
            if self.active_repair_jobs() == 0 {
                self.promote_queued_repairs();
            }
            return;
        }

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        let par2_validation_needed = self.par2_set(job_id).is_some()
            && !par2_bypassed
            && !self.par2_verified.contains(&job_id)
            && download_pipeline_exhausted
            && !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && self.job_has_only_rar_archives(job_id)
            && self.jobs.get(&job_id).is_some_and(|state| {
                matches!(
                    state.post_state,
                    crate::jobs::model::PostState::WaitingForVolumes
                )
            });

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());
        let needs_completion_repair_evaluation = has_crc_failures
            || (has_incomplete_data_files && download_pipeline_exhausted)
            || rar_waiting_for_missing_volumes
            || par2_validation_needed;

        if has_incomplete_data_files
            && !download_pipeline_exhausted
            && !self.job_has_active_extraction_tasks(job_id)
        {
            return;
        }

        // Don't finalize while concatenation is still pending.
        if self
            .pending_concat
            .get(&job_id)
            .is_some_and(|s| !s.is_empty())
        {
            debug!(
                job_id = job_id.0,
                "deferring completion — pending concatenation"
            );
            return;
        }

        if !has_crc_failures
            && self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id)
        {
            self.finalize_completed_archive_job(job_id).await;
            return;
        }

        if needs_completion_repair_evaluation && !par2_bypassed {
            if self.has_active_rar_workers(job_id) {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active RAR extraction workers"
                );
                return;
            }

            let par2_set = self.par2_set(job_id).cloned();
            if let Some(par2_set) = par2_set {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                let (verification, placement_plan) = match self
                    .verify_par2_with_placement(
                        job_id,
                        Arc::clone(&par2_set),
                        working_dir.clone(),
                        matches!(current_status, JobStatus::Repairing),
                        true,
                    )
                    .await
                {
                    Ok(result) => result,
                    Err(message) => {
                        warn!(job_id = job_id.0, error = %message);
                        self.fail_job(job_id, message);
                        return;
                    }
                };
                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);

                if damaged == 0 {
                    info!(
                        job_id = job_id.0,
                        "PAR2 verification passed — no damaged slices"
                    );

                    // Rename obfuscated files using PAR2 metadata even when
                    // verification is clean (files may be intact but obfuscated).
                    self.try_deobfuscate_files_with_par2(job_id).await;
                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }
                    self.retry_par2_authoritative_identity(job_id).await;
                    if let Err(error) = self
                        .reconcile_verified_par2_files(job_id, &verification)
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }

                    let still_incomplete = self.jobs.get(&job_id).is_some_and(|state| {
                        state.assembly.complete_data_file_count() < state.assembly.data_file_count()
                    });
                    if still_incomplete && !has_crc_failures {
                        let msg = "clean PAR2 verification but job still has incomplete data files after reconciliation".to_string();
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }
                    self.par2_verified.insert(job_id);

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        self.set_normalization_retried_state(job_id, true);
                        let failed_members = self
                            .failed_extractions
                            .get(&job_id)
                            .cloned()
                            .unwrap_or_default();
                        self.replace_failed_extraction_members(job_id, HashSet::new());
                        let cleared = failed_members.len();
                        self.recompute_rar_retry_frontier(job_id).await;
                        if let Some(reason) = self.invalid_rar_retry_frontier_reason(job_id) {
                            if !failed_members.is_empty() {
                                self.replace_failed_extraction_members(job_id, failed_members);
                            }
                            let msg = format!(
                                "invalid RAR retry frontier after placement correction: {reason}"
                            );
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }
                        info!(
                            job_id = job_id.0,
                            cleared,
                            "cleared failed extractions after authoritative verify — retrying"
                        );

                        self.retry_archive_extraction_after_verify_or_repair(job_id)
                            .await;
                        return;
                    }

                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                } else {
                    info!(
                        job_id = job_id.0,
                        damaged,
                        recovery_now,
                        total_recovery_capacity,
                        "PAR2 verification — damage detected"
                    );

                    if let Err(error) = self
                        .apply_placement_plan_for_retry_or_repair(
                            job_id,
                            working_dir.clone(),
                            &placement_plan,
                        )
                        .await
                    {
                        self.fail_job(job_id, error);
                        return;
                    }

                    if total_recovery_capacity < damaged {
                        self.fail_job(
                            job_id,
                            format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        );
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(job_id, damaged);
                        let targeted_total = self.recovery_blocks_available_or_targeted(job_id);

                        // If all available/targeted recovery is still insufficient,
                        // fail immediately instead of waiting for downloads that
                        // won't help.
                        if targeted_total < damaged {
                            let msg = format!(
                                "not repairable: {damaged} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            warn!(job_id = job_id.0, %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        self.transition_runtime_state(
                            job_id,
                            crate::jobs::model::DownloadState::Downloading,
                            crate::jobs::model::PostState::AwaitingRepair,
                            crate::jobs::model::RunState::Active,
                        );
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    let par2_for_repair = Arc::clone(&par2_set);
                    let repair_dir = working_dir.clone();

                    let pp_pool = self.pp_pool.clone();
                    let repair_result = tokio::task::spawn_blocking(move || {
                        pp_pool.install(move || {
                            crate::e2e_failpoint::maybe_delay("repair.task_start");
                            let mut file_access =
                                weaver_par2::DiskFileAccess::new(repair_dir, &par2_for_repair);
                            let plan = weaver_par2::plan_repair(&par2_for_repair, &verification)
                                .map_err(|e| format!("repair planning failed: {e}"))?;
                            let slices = plan.missing_slices.len() as u32;
                            weaver_par2::execute_repair(&plan, &par2_for_repair, &mut file_access)
                                .map_err(|e| format!("repair execution failed: {e}"))?;
                            Ok(slices)
                        })
                    })
                    .await;

                    let repair_outcome = match repair_result {
                        Ok(Ok(slices)) => Ok(slices),
                        Ok(Err(e)) => Err(e),
                        Err(e) => Err(format!("repair task panicked: {e}")),
                    };

                    match repair_outcome {
                        Ok(slices_repaired) => {
                            info!(job_id = job_id.0, slices_repaired, "PAR2 repair complete");
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            let (post_repair_verification, post_repair_placement_plan) = match self
                                .verify_par2_with_placement(
                                    job_id,
                                    Arc::clone(&par2_set),
                                    working_dir.clone(),
                                    true,
                                    false,
                                )
                                .await
                            {
                                Ok(result) => result,
                                Err(message) => {
                                    warn!(job_id = job_id.0, error = %message);
                                    self.fail_job(job_id, message);
                                    return;
                                }
                            };

                            if post_repair_verification.total_missing_blocks > 0 {
                                let msg = format!(
                                    "PAR2 repair completed but {} damaged slices remain",
                                    post_repair_verification.total_missing_blocks
                                );
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }

                            // Rename obfuscated files using PAR2 metadata (16KB hash matching).
                            // Must happen after repair and before extraction retry/finalize.
                            self.try_deobfuscate_files_with_par2(job_id).await;
                            if let Err(error) = self
                                .apply_placement_plan_for_retry_or_repair(
                                    job_id,
                                    working_dir.clone(),
                                    &post_repair_placement_plan,
                                )
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }
                            self.retry_par2_authoritative_identity(job_id).await;
                            if let Err(error) = self
                                .reconcile_verified_par2_files(job_id, &post_repair_verification)
                                .await
                            {
                                self.fail_job(job_id, error);
                                return;
                            }

                            let still_incomplete = self.jobs.get(&job_id).is_some_and(|state| {
                                state.assembly.complete_data_file_count()
                                    < state.assembly.data_file_count()
                            });
                            if still_incomplete && !has_crc_failures {
                                let msg = "PAR2 repair completed but job still has incomplete data files after reconciliation".to_string();
                                warn!(job_id = job_id.0, error = %msg);
                                self.fail_job(job_id, msg);
                                return;
                            }
                            self.par2_verified.insert(job_id);

                            if has_crc_failures {
                                let cleared =
                                    self.failed_extractions.get(&job_id).map_or(0, HashSet::len);
                                self.replace_failed_extraction_members(job_id, HashSet::new());
                                if cleared > 0 {
                                    info!(
                                        job_id = job_id.0,
                                        cleared, "cleared failed extractions for post-repair retry"
                                    );
                                }

                                self.retry_archive_extraction_after_verify_or_repair(job_id)
                                    .await;
                                return;
                            }

                            self.reconcile_job_progress(job_id).await;
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        Err(error_msg) => {
                            warn!(job_id = job_id.0, error = %error_msg, "PAR2 repair failed");
                            let _ = self.event_tx.send(PipelineEvent::RepairFailed {
                                job_id,
                                error: error_msg.clone(),
                            });
                            self.fail_job(job_id, error_msg);
                            return;
                        }
                    }
                }
            } else {
                if has_incomplete_data_files {
                    let msg = format!(
                        "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete and no PAR2 metadata is available for repair"
                    );
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if rar_waiting_for_missing_volumes {
                    let reason = self.invalid_rar_retry_frontier_reason(job_id).unwrap_or_else(|| {
                        "RAR extraction stalled waiting for missing volumes after downloads finished"
                            .to_string()
                    });
                    let msg = format!("{reason}; no PAR2 metadata is available for repair");
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                match self.retry_failed_archive_sources_without_par2(job_id).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => {
                        self.fail_job(job_id, error);
                        return;
                    }
                }

                let failed_members: Vec<String> = self
                    .failed_extractions
                    .get(&job_id)
                    .map(|s| s.iter().cloned().collect())
                    .unwrap_or_default();
                let msg = format!(
                    "extraction CRC failures with no PAR2 data: {:?}",
                    failed_members
                );
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }
        } else if has_incomplete_data_files {
            let repair_context = if par2_bypassed {
                "PAR2 recovery is bypassed"
            } else if self.par2_set(job_id).is_some() {
                "PAR2 recovery did not become eligible"
            } else {
                "no PAR2 metadata is available for repair"
            };
            let byte_detail = if failed_bytes > 0 {
                format!(", {failed_bytes} bytes unavailable")
            } else {
                String::new()
            };
            let msg = format!(
                "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete{byte_detail}, {repair_context}"
            );
            warn!(job_id = job_id.0, error = %msg);
            self.fail_job(job_id, msg);
            return;
        }

        if self.job_has_only_rar_archives(job_id) {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        // Check extraction readiness.
        let readiness = self.extraction_readiness_for_job(job_id);
        match readiness {
            ExtractionReadiness::NotApplicable => {
                // A complete non-archive payload can still have explicitly
                // promoted PAR2 recovery segments in flight. Do not let stale
                // completion checks finalize damaged direct/gzip/etc. payloads
                // before those recovery files are decoded and repaired.
                if !download_pipeline_exhausted {
                    debug!(
                        job_id = job_id.0,
                        "deferring completion — pending download pipeline work"
                    );
                    return;
                }
                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                if !par2_bypassed {
                    self.cleanup_par2_files(job_id).await;
                }
                // No archives — move to complete and finish.
                self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }
                self.transition_completed_runtime(job_id);
                if self.active_download_passes.remove(&job_id) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::DownloadFinished { job_id });
                }
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Ready => {
                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
                let already_extracted = self
                    .extracted_archives
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let already_spawned = self
                    .inflight_extractions
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let sets_to_extract: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    state
                        .assembly
                        .archive_topologies()
                        .iter()
                        .filter(|(name, _)| {
                            !already_extracted.contains(*name) && !already_spawned.contains(*name)
                        })
                        .map(|(name, topo)| (name.clone(), topo.archive_type))
                        .collect()
                };

                // If extractions are still in-flight, wait for them to complete.
                if !already_spawned.is_empty() && sets_to_extract.is_empty() {
                    return;
                }

                if !sets_to_extract.is_empty() {
                    // Spawn extraction tasks in the background.
                    // handle_extraction_done will re-enter check_job_completion
                    // when each set finishes, and we'll reach the empty branch below.
                    if !self.maybe_start_extraction(job_id).await {
                        return;
                    }

                    self.spawn_extractions(job_id, &sets_to_extract).await;
                    // Return — extraction runs in background.
                    // handle_extraction_done will call check_job_completion again.
                    return;
                }

                if self
                    .reconcile_extracted_outputs_for_completion(job_id)
                    .await
                {
                    self.reconcile_job_progress(job_id).await;
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                let cleanup_files: HashSet<String> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    let mut cleanup_files: HashSet<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                self.classified_role_for_file(job_id, f),
                                weaver_model::files::FileRole::Par2 { .. }
                                    | weaver_model::files::FileRole::RarVolume { .. }
                                    | weaver_model::files::FileRole::SevenZipArchive
                                    | weaver_model::files::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| self.current_filename_for_file(job_id, f))
                        .collect();
                    for topology in state.assembly.archive_topologies().values() {
                        cleanup_files.extend(topology.volume_map.keys().cloned());
                    }
                    cleanup_files
                };
                let nested_decision = match self.maybe_start_nested_extraction(job_id).await {
                    Ok(decision) => decision,
                    Err(error) => {
                        self.fail_job(job_id, error);
                        return;
                    }
                };
                match nested_decision {
                    NestedExtractionDecision::Started
                    | NestedExtractionDecision::NoNestedArchives => {
                        let mut removed = 0u32;
                        for filename in &cleanup_files {
                            let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                                continue;
                            };
                            match tokio::fs::remove_file(&path).await {
                                Ok(()) => removed += 1,
                                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                Err(e) => {
                                    warn!(
                                        file = %path.display(),
                                        error = %e,
                                        "failed to clean up source file"
                                    );
                                }
                            }
                        }
                        info!(
                            job_id = job_id.0,
                            removed,
                            total = cleanup_files.len(),
                            "post-extraction cleanup complete"
                        );
                        if matches!(nested_decision, NestedExtractionDecision::Started) {
                            return;
                        }
                    }
                    NestedExtractionDecision::PreserveOutputsAtDepthLimit => {}
                }

                self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
                info!(job_id = job_id.0, "extraction complete");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Move extracted files to complete directory.
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }

                self.transition_completed_runtime(job_id);
                if self.active_download_passes.remove(&job_id) {
                    let _ = self
                        .event_tx
                        .send(PipelineEvent::DownloadFinished { job_id });
                }
                self.clear_par2_runtime_state(job_id);
                self.clear_job_rar_runtime(job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Blocked { reason } => {
                self.fail_job(job_id, reason);
            }
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                // Some archives are ready (e.g. all 7z split files arrived)
                // while others are still downloading. Spawn what we can.
                let already_done = self
                    .extracted_archives
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let already_inflight = self
                    .inflight_extractions
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let to_spawn: Vec<(String, crate::jobs::assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    extractable
                        .iter()
                        .filter(|name| {
                            !already_done.contains(*name) && !already_inflight.contains(*name)
                        })
                        .filter_map(|name| {
                            state
                                .assembly
                                .archive_topology_for(name)
                                .map(|topo| (name.clone(), topo.archive_type))
                        })
                        .collect()
                };

                if to_spawn.is_empty() {
                    return;
                }

                if !self.maybe_start_extraction(job_id).await {
                    return;
                }

                let spawned = self.spawn_extractions(job_id, &to_spawn).await;
                info!(
                    job_id = job_id.0,
                    spawned,
                    waiting = ?waiting_on,
                    "started extraction for ready archives, waiting on remaining"
                );
            }
        }
    }
}
