use super::*;
use std::collections::HashSet;

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
    fn try_deobfuscate_files_with_par2(&self, job_id: JobId) -> usize {
        let Some(par2) = self.par2_set(job_id).cloned() else {
            return 0;
        };
        let Some(rename_dir) = self
            .jobs
            .get(&job_id)
            .map(|state| state.working_dir.clone())
        else {
            return 0;
        };

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

        let mut renamed = 0usize;
        for suggestion in &suggestions {
            let old = &suggestion.current_path;
            let new = old.parent().unwrap().join(&suggestion.correct_name);
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
        }

        if renamed > 0 {
            info!(job_id = job_id.0, renamed, "PAR2 deobfuscation complete");
        }

        renamed
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
            if self
                .jobs
                .get(&job_id)
                .is_some_and(|state| !matches!(state.status, JobStatus::Extracting))
            {
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Extracting,
                    Some("extracting"),
                );
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

            return;
        }

        self.finalize_completed_archive_job(job_id).await;
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

        // Step 1: Are all data files (non-recovery) complete?
        {
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
            let total = state.assembly.data_file_count();
            let complete = state.assembly.complete_data_file_count();
            if complete < total {
                if matches!(state.status, JobStatus::Queued | JobStatus::Downloading)
                    && state.failed_bytes > 0
                    && !self.job_has_pending_download_pipeline_work(job_id)
                {
                    let failed_bytes = state.failed_bytes;
                    let error = format!(
                        "download incomplete after exhausting retries: {complete}/{total} data files complete, {failed_bytes} bytes unavailable"
                    );
                    warn!(
                        job_id = job_id.0,
                        complete,
                        total,
                        failed_bytes,
                        "failing incomplete job after download pipeline exhaustion"
                    );
                    self.fail_job(job_id, error);
                }
                return;
            }
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total == 0
                && matches!(state.status, JobStatus::Queued | JobStatus::Downloading)
                && !state.download_queue.is_empty()
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

        let par2_bypassed = self.par2_bypassed.contains(&job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());

        if has_crc_failures && !par2_bypassed {
            if self.has_active_rar_workers(job_id) {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active RAR extraction workers"
                );
                return;
            }

            let par2_set = self.par2_set(job_id).cloned();
            if let Some(par2_set) = par2_set {
                if !matches!(current_status, JobStatus::Repairing) {
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
                self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
                info!(job_id = job_id.0, "par2 verification started");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::JobVerificationStarted { job_id });
                let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                });

                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                let par2_for_verify = Arc::clone(&par2_set);
                let verify_dir = working_dir.clone();

                let pp_pool = self.pp_pool.clone();
                let verify_result = tokio::task::spawn_blocking(move || {
                    pp_pool.install(move || {
                        let plan = weaver_par2::scan_placement(&verify_dir, &par2_for_verify)
                            .map_err(|e| format!("placement scan failed: {e}"))?;
                        if !plan.conflicts.is_empty() {
                            return Err(format!(
                                "placement scan found {} conflicting file matches",
                                plan.conflicts.len()
                            ));
                        }

                        let file_access = weaver_par2::PlacementFileAccess::from_plan(
                            verify_dir,
                            &par2_for_verify,
                            &plan,
                        );
                        Ok((
                            weaver_par2::verify_all(&par2_for_verify, &file_access),
                            plan,
                        ))
                    })
                })
                .await;

                self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

                let (mut verification, placement_plan) = match verify_result {
                    Ok(Ok(v)) => v,
                    Ok(Err(msg)) => {
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }
                    Err(e) => {
                        let msg = format!("verification task panicked: {e}");
                        warn!(job_id = job_id.0, error = %msg);
                        self.fail_job(job_id, msg);
                        return;
                    }
                };
                Self::log_placement_plan(job_id, &placement_plan);

                let (skipped_blocks, retained_suspect_blocks) =
                    self.apply_eager_delete_exclusions(job_id, &mut verification);
                if skipped_blocks > 0 {
                    info!(
                        job_id = job_id.0,
                        skipped_blocks,
                        "excluded eagerly-deleted CRC-verified volumes from damage count"
                    );
                }
                if retained_suspect_blocks > 0 {
                    info!(
                        job_id = job_id.0,
                        retained_suspect_blocks,
                        "retained suspect eagerly-deleted volumes in damage count"
                    );
                }

                self.recompute_volume_safety_from_verification(job_id, &verification);

                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);
                let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
                    job_id,
                    passed: damaged == 0,
                });

                if damaged == 0 {
                    info!(
                        job_id = job_id.0,
                        "PAR2 verification passed — no damaged slices"
                    );

                    // Rename obfuscated files using PAR2 metadata even when
                    // verification is clean (files may be intact but obfuscated).
                    self.try_deobfuscate_files_with_par2(job_id);

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            self.fail_job(job_id, msg);
                            return;
                        }

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
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
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

                            // Rename obfuscated files using PAR2 metadata (16KB hash matching).
                            // Must happen after repair and before extraction retry.
                            self.try_deobfuscate_files_with_par2(job_id);

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
        }

        if self.job_has_only_rar_archives(job_id) {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        // Check extraction readiness.
        let readiness = {
            let state = self.jobs.get(&job_id).unwrap();
            state.assembly.extraction_readiness()
        };
        match readiness {
            ExtractionReadiness::NotApplicable => {
                if !par2_bypassed {
                    self.cleanup_par2_files(job_id).await;
                }
                // No archives — move to complete and finish.
                self.transition_postprocessing_status(job_id, JobStatus::Moving, None);
                if let Err(error) = self.move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                    return;
                }
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Complete;
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

                // All sets extracted — finish the job.
                // Clean up archive source files before moving to complete.
                {
                    let state = self.jobs.get(&job_id).unwrap();
                    let mut cleanup_files: HashSet<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                f.effective_role(),
                                weaver_model::files::FileRole::Par2 { .. }
                                    | weaver_model::files::FileRole::RarVolume { .. }
                                    | weaver_model::files::FileRole::SevenZipArchive
                                    | weaver_model::files::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| f.filename().to_string())
                        .collect();
                    for topology in state.assembly.archive_topologies().values() {
                        cleanup_files.extend(topology.volume_map.keys().cloned());
                    }
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
                }

                match self.maybe_start_nested_extraction(job_id).await {
                    Ok(true) => return,
                    Ok(false) => {}
                    Err(error) => {
                        self.fail_job(job_id, error);
                        return;
                    }
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

                {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.status = JobStatus::Complete;
                }
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
