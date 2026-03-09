use super::*;

impl Pipeline {
    /// Move extracted/completed files from the intermediate working directory
    /// to the complete directory, organized by category.
    ///
    /// Layout: `{complete_dir}/[{category}/]{job_name}/`
    ///
    /// Uses rename() for same-filesystem moves, falls back to copy+delete for cross-FS.
    pub(super) async fn move_to_complete(&mut self, job_id: JobId) {
        let (working_dir, job_name, category) = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            (
                state.working_dir.clone(),
                state.spec.name.clone(),
                state.spec.category.clone(),
            )
        };

        // Build destination path: complete_dir/[category/]job_name/
        let dir_name = job::sanitize_dirname(&job_name);
        let mut dest = self.complete_dir.clone();
        if let Some(ref cat) = category
            && !cat.is_empty()
        {
            dest = dest.join(cat);
        }
        dest = dest.join(&dir_name);

        // Ensure destination exists.
        if let Err(e) = tokio::fs::create_dir_all(&dest).await {
            warn!(
                job_id = job_id.0,
                dest = %dest.display(),
                error = %e,
                "failed to create complete directory"
            );
            return;
        }

        // List files in the working directory and move them.
        let mut entries = match tokio::fs::read_dir(&working_dir).await {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    job_id = job_id.0,
                    dir = %working_dir.display(),
                    error = %e,
                    "failed to read working directory for move"
                );
                return;
            }
        };

        let mut moved = 0u32;
        while let Ok(Some(entry)) = entries.next_entry().await {
            let src = entry.path();
            let file_name = entry.file_name();
            let dst = dest.join(&file_name);

            // Try rename first (fast, same filesystem).
            match tokio::fs::rename(&src, &dst).await {
                Ok(()) => {
                    moved += 1;
                }
                Err(_rename_err) => {
                    // Cross-filesystem fallback: copy + delete.
                    match tokio::fs::copy(&src, &dst).await {
                        Ok(_) => {
                            let _ = tokio::fs::remove_file(&src).await;
                            moved += 1;
                        }
                        Err(e) => {
                            warn!(
                                file = %file_name.to_string_lossy(),
                                error = %e,
                                "failed to move file to complete directory"
                            );
                        }
                    }
                }
            }
        }

        // Remove the now-empty intermediate directory.
        let _ = tokio::fs::remove_dir(&working_dir).await;

        // Update working_dir to point to the complete path (for API reporting).
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.working_dir = dest.clone();
        }

        info!(
            job_id = job_id.0,
            moved,
            dest = %dest.display(),
            "moved files to complete directory"
        );
    }

    fn apply_eager_delete_exclusions(
        &self,
        job_id: JobId,
        verification: &mut weaver_par2::VerificationResult,
    ) -> u32 {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();

        let mut skipped_blocks = 0u32;
        for file_verification in &mut verification.files {
            if matches!(
                file_verification.status,
                weaver_par2::verify::FileStatus::Missing
            ) && eagerly_deleted_names.contains(file_verification.filename.as_str())
            {
                skipped_blocks += file_verification.missing_slice_count;
                file_verification.status = weaver_par2::verify::FileStatus::Complete;
                file_verification.valid_slices.fill(true);
                file_verification.missing_slice_count = 0;
            }
        }
        verification.total_missing_blocks = verification
            .total_missing_blocks
            .saturating_sub(skipped_blocks);
        skipped_blocks
    }

    fn recompute_volume_safety_from_verification(
        &mut self,
        job_id: JobId,
        verification: &weaver_par2::VerificationResult,
    ) {
        let eagerly_deleted_names: HashSet<&str> = self
            .eagerly_deleted
            .get(&job_id)
            .map(|s| s.iter().map(String::as_str).collect())
            .unwrap_or_default();

        let status_by_name: HashMap<&str, &weaver_par2::FileVerification> = verification
            .files
            .iter()
            .map(|file| (file.filename.as_str(), file))
            .collect();

        let plans: Vec<(String, HashSet<u32>, HashSet<u32>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            state
                .assembly
                .archive_topologies()
                .iter()
                .map(|(set_name, topo)| {
                    let mut clean = HashSet::new();
                    let mut suspect = HashSet::new();
                    for (filename, &volume_number) in &topo.volume_map {
                        if let Some(file) = status_by_name.get(filename.as_str()) {
                            match file.status {
                                weaver_par2::verify::FileStatus::Complete
                                | weaver_par2::verify::FileStatus::Renamed(_) => {
                                    clean.insert(volume_number);
                                }
                                weaver_par2::verify::FileStatus::Missing
                                    if eagerly_deleted_names.contains(filename.as_str()) =>
                                {
                                    clean.insert(volume_number);
                                }
                                weaver_par2::verify::FileStatus::Missing
                                | weaver_par2::verify::FileStatus::Damaged(_) => {
                                    suspect.insert(volume_number);
                                }
                            }
                        }
                    }
                    (set_name.clone(), clean, suspect)
                })
                .collect()
        };

        for (set_name, clean, suspect) in plans {
            let key = (job_id, set_name.clone());
            if clean.is_empty() {
                self.clean_volumes.remove(&key);
            } else {
                self.clean_volumes.insert(key.clone(), clean);
            }
            if suspect.is_empty() {
                self.suspect_volumes.remove(&key);
            } else {
                self.suspect_volumes.insert(key, suspect);
            }
        }
    }

    async fn refresh_rar_topology_after_normalization(
        &mut self,
        job_id: JobId,
        normalized_files: &HashSet<String>,
    ) {
        if normalized_files.is_empty() {
            return;
        }

        let refresh_plan: Vec<(String, bool, Vec<(u32, NzbFileId)>)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };

            let mut set_to_files: HashMap<String, HashSet<String>> = HashMap::new();
            for (set_name, topo) in state.assembly.archive_topologies() {
                let touched: HashSet<String> = topo
                    .volume_map
                    .keys()
                    .filter(|filename| normalized_files.contains(*filename))
                    .cloned()
                    .collect();
                if !touched.is_empty() {
                    set_to_files.insert(set_name.clone(), touched);
                }
            }

            set_to_files
                .into_iter()
                .map(|(set_name, touched_files)| {
                    let mut file_ids = Vec::new();
                    let mut has_volume_zero = false;

                    for file in state.assembly.files() {
                        if let weaver_core::classify::FileRole::RarVolume { volume_number } =
                            file.role()
                            && let Some(base_name) = weaver_core::classify::archive_base_name(
                                file.filename(),
                                file.role(),
                            )
                            && base_name == set_name
                        {
                            if touched_files.contains(file.filename()) {
                                file_ids.push((*volume_number, file.file_id()));
                            }
                            if *volume_number == 0
                                && file.is_complete()
                                && state.working_dir.join(file.filename()).exists()
                            {
                                has_volume_zero = true;
                            }
                        }
                    }

                    if has_volume_zero {
                        file_ids.clear();
                        for file in state.assembly.files() {
                            if let weaver_core::classify::FileRole::RarVolume { volume_number } =
                                file.role()
                                && let Some(base_name) = weaver_core::classify::archive_base_name(
                                    file.filename(),
                                    file.role(),
                                )
                                && base_name == set_name
                                && file.is_complete()
                                && state.working_dir.join(file.filename()).exists()
                            {
                                file_ids.push((*volume_number, file.file_id()));
                            }
                        }
                    }

                    file_ids.sort_by_key(|(volume_number, _)| *volume_number);
                    (set_name, has_volume_zero, file_ids)
                })
                .collect()
        };

        for (set_name, full_rebuild, file_ids) in refresh_plan {
            if file_ids.is_empty() {
                continue;
            }

            if let Some(state) = self.jobs.get_mut(&job_id) {
                if full_rebuild {
                    state.assembly.archive_topologies_mut().remove(&set_name);
                } else if let Some(topo) = state.assembly.archive_topology_for_mut(&set_name) {
                    let touched: HashSet<u32> = file_ids
                        .iter()
                        .map(|(volume_number, _)| *volume_number)
                        .collect();
                    topo.members
                        .retain(|member| !touched.contains(&member.first_volume));
                    for volume_number in &touched {
                        topo.complete_volumes.remove(volume_number);
                    }
                }
            }

            for (_, file_id) in file_ids {
                self.try_update_archive_topology(job_id, file_id).await;
            }

            info!(
                job_id = job_id.0,
                set_name = %set_name,
                full_rebuild,
                "refreshed RAR topology after normalization"
            );
        }
    }

    /// Check if all data files in a job are complete, and trigger post-processing.
    ///
    /// PAR2 is treated as a repair tool only — damage is detected via yEnc CRC
    /// (per-segment) and RAR CRC (per-member during streaming extraction). If
    /// CRC failures occur, recovery files are promoted for download and repair
    /// runs from disk using `verify_all` + `plan_repair` + `execute_repair`.
    pub(super) async fn check_job_completion(&mut self, job_id: JobId) {
        // Step 1: Are all data files (non-recovery) complete?
        {
            let Some(state) = self.jobs.get(&job_id) else {
                return;
            };
            if state.assembly.complete_data_file_count() < state.assembly.data_file_count() {
                return;
            }
        }

        // All data files downloaded — transition out of Downloading so the UI
        // reflects post-processing (verify/extract/cleanup).
        {
            let state = self.jobs.get_mut(&job_id).unwrap();
            if state.status == JobStatus::Downloading {
                state.status = JobStatus::Extracting;
                if let Err(e) = self.db.set_active_job_status(job_id, "extracting", None) {
                    error!(error = %e, "db write failed for extracting status");
                }
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
            }
        }

        let par2_bypassed = self.par2_bypassed.contains(&job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|f| !f.is_empty());

        if has_crc_failures && !par2_bypassed {
            // Don't start verify/repair while streaming extraction is still active.
            // Providers hold file handles — renaming files would break extraction.
            let has_active_streams = self
                .streaming_providers
                .keys()
                .any(|(jid, _, _)| *jid == job_id);
            if has_active_streams {
                debug!(
                    job_id = job_id.0,
                    "deferring verify — active streaming providers"
                );
                return;
            }

            let par2_set = self.par2_sets.get(&job_id).cloned();
            if let Some(par2_set) = par2_set {
                {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    if state.status == JobStatus::Extracting {
                        self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                    }
                    state.status = JobStatus::Verifying;
                }
                if let Err(e) = self.db.set_active_job_status(job_id, "verifying", None) {
                    error!(error = %e, "db write failed for verifying status");
                }
                self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
                let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                });

                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                let par2_for_verify = Arc::clone(&par2_set);
                let verify_dir = working_dir.clone();

                let verify_result = tokio::task::spawn_blocking(move || {
                    let plan = weaver_par2::scan_placement(&verify_dir, &par2_for_verify)
                        .map_err(|e| format!("placement scan failed: {e}"))?;
                    if !plan.conflicts.is_empty() {
                        return Err(format!(
                            "placement scan found {} conflicting file matches",
                            plan.conflicts.len()
                        ));
                    }

                    let mut normalized_files = HashSet::new();
                    if !plan.swaps.is_empty() || !plan.renames.is_empty() {
                        for (a, b) in &plan.swaps {
                            normalized_files.insert(a.correct_name.clone());
                            normalized_files.insert(b.correct_name.clone());
                        }
                        for entry in &plan.renames {
                            normalized_files.insert(entry.correct_name.clone());
                        }

                        let moved = weaver_par2::apply_placement_plan(&verify_dir, &plan)
                            .map_err(|e| format!("placement normalization failed: {e}"))?;
                        tracing::info!(
                            swaps = plan.swaps.len(),
                            renames = plan.renames.len(),
                            moved,
                            "normalized file placement before verify"
                        );
                    }

                    let file_access =
                        weaver_par2::DiskFileAccess::new(verify_dir, &par2_for_verify);
                    Ok((
                        weaver_par2::verify_all(&par2_for_verify, &file_access),
                        normalized_files,
                    ))
                })
                .await;

                self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

                let (mut verification, normalized_files) = match verify_result {
                    Ok(Ok(v)) => v,
                    Ok(Err(msg)) => {
                        warn!(job_id = job_id.0, error = %msg);
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed { error: msg.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::JobFailed { job_id, error: msg });
                        self.record_job_history(job_id);
                        return;
                    }
                    Err(e) => {
                        let msg = format!("verification task panicked: {e}");
                        warn!(job_id = job_id.0, error = %msg);
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed { error: msg.clone() };
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::JobFailed { job_id, error: msg });
                        self.record_job_history(job_id);
                        return;
                    }
                };

                if !normalized_files.is_empty() {
                    self.refresh_rar_topology_after_normalization(job_id, &normalized_files)
                        .await;
                }

                let skipped_blocks = self.apply_eager_delete_exclusions(job_id, &mut verification);
                if skipped_blocks > 0 {
                    info!(
                        job_id = job_id.0,
                        skipped_blocks,
                        "excluded eagerly-deleted CRC-verified volumes from damage count"
                    );
                }

                self.recompute_volume_safety_from_verification(job_id, &verification);

                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity = self.total_recovery_block_capacity(job_id);

                if damaged == 0 {
                    info!(
                        job_id = job_id.0,
                        "PAR2 verification passed — no damaged slices"
                    );

                    if has_crc_failures {
                        if self.normalization_retried.contains(&job_id) {
                            let msg =
                                "clean PAR2 verification but extraction still failing after retry"
                                    .to_string();
                            warn!(job_id = job_id.0, error = %msg);
                            let state = self.jobs.get_mut(&job_id).unwrap();
                            state.status = JobStatus::Failed { error: msg.clone() };
                            let _ = self
                                .event_tx
                                .send(PipelineEvent::JobFailed { job_id, error: msg });
                            self.record_job_history(job_id);
                            return;
                        }

                        self.normalization_retried.insert(job_id);
                        let cleared = self
                            .failed_extractions
                            .remove(&job_id)
                            .map_or(0, |s| s.len());
                        info!(
                            job_id = job_id.0,
                            cleared,
                            "cleared failed extractions after authoritative verify — retrying"
                        );

                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.status = JobStatus::Downloading;
                        }
                        if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                            error!(error = %e, "db write failed");
                        }
                        self.try_partial_extraction(job_id).await;
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

                    if total_recovery_capacity < damaged {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed {
                            error: format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        };
                        let _ = self.event_tx.send(PipelineEvent::JobFailed {
                            job_id,
                            error: format!(
                                "not repairable: {damaged} damaged, {total_recovery_capacity} recovery"
                            ),
                        });
                        self.record_job_history(job_id);
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(job_id, damaged);
                        let targeted_total = self.recovery_blocks_available_or_targeted(job_id);
                        info!(
                            job_id = job_id.0,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.status = JobStatus::Downloading;
                        }
                        if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                            error!(error = %e, "db write failed for downloading status");
                        }
                        return;
                    }

                    {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Repairing;
                    }
                    if let Err(e) = self.db.set_active_job_status(job_id, "repairing", None) {
                        error!(error = %e, "db write failed for repairing status");
                    }
                    self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
                    let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });

                    let par2_for_repair = Arc::clone(&par2_set);
                    let repair_dir = working_dir.clone();

                    let repair_result = tokio::task::spawn_blocking(move || {
                        let mut file_access =
                            weaver_par2::DiskFileAccess::new(repair_dir, &par2_for_repair);
                        let plan = weaver_par2::plan_repair(&par2_for_repair, &verification)
                            .map_err(|e| format!("repair planning failed: {e}"))?;
                        let slices = plan.missing_slices.len() as u32;
                        weaver_par2::execute_repair(&plan, &par2_for_repair, &mut file_access)
                            .map_err(|e| format!("repair execution failed: {e}"))?;
                        Ok(slices)
                    })
                    .await;

                    self.metrics.repair_active.fetch_sub(1, Ordering::Relaxed);

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

                            let cleared = self
                                .failed_extractions
                                .remove(&job_id)
                                .map_or(0, |s| s.len());
                            if cleared > 0 {
                                info!(
                                    job_id = job_id.0,
                                    cleared, "cleared failed extractions for post-repair retry"
                                );
                            }

                            if let Some(state) = self.jobs.get_mut(&job_id) {
                                state.status = JobStatus::Downloading;
                            }
                            if let Err(e) =
                                self.db.set_active_job_status(job_id, "downloading", None)
                            {
                                error!(error = %e, "db write failed for downloading status");
                            }

                            self.try_partial_extraction(job_id).await;
                            return;
                        }
                        Err(error_msg) => {
                            warn!(job_id = job_id.0, error = %error_msg, "PAR2 repair failed");
                            let Some(state) = self.jobs.get_mut(&job_id) else {
                                return;
                            };
                            state.status = JobStatus::Failed {
                                error: error_msg.clone(),
                            };
                            let _ = self.event_tx.send(PipelineEvent::RepairFailed {
                                job_id,
                                error: error_msg.clone(),
                            });
                            self.record_job_history(job_id);
                            return;
                        }
                    }
                }
            } else {
                // CRC failures but no PAR2 set — fail the job.
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
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Failed { error: msg.clone() };
                let _ = self
                    .event_tx
                    .send(PipelineEvent::JobFailed { job_id, error: msg });
                self.record_job_history(job_id);
                return;
            }
        }

        // Clean up PAR2 files — no longer needed.
        // Skip if PAR2 was bypassed (files already deleted by drain_recovery_and_bypass_par2).
        if !par2_bypassed {
            if let Some(state) = self.jobs.get(&job_id) {
                let cleanup_dir = state.working_dir.clone();
                let par2_files: Vec<String> = state
                    .assembly
                    .files()
                    .filter(|f| matches!(f.role(), weaver_core::classify::FileRole::Par2 { .. }))
                    .map(|f| f.filename().to_string())
                    .collect();
                if !par2_files.is_empty() {
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
            }
        }

        // Drop retained Par2FileSet — no longer needed.
        self.par2_sets.remove(&job_id);

        // Check extraction readiness.
        let readiness = {
            let state = self.jobs.get(&job_id).unwrap();
            state.assembly.extraction_readiness()
        };
        match readiness {
            ExtractionReadiness::NotApplicable => {
                // No archives — move to complete and finish.
                self.move_to_complete(job_id).await;
                let state = self.jobs.get_mut(&job_id).unwrap();
                if state.status == JobStatus::Extracting {
                    self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                }
                state.status = JobStatus::Complete;
                self.promoted_recovery_files.remove(&job_id);
                self.eagerly_deleted.remove(&job_id);
                self.clean_volumes.retain(|(jid, _), _| *jid != job_id);
                self.suspect_volumes.retain(|(jid, _), _| *jid != job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Ready => {
                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
                let already_extracted = self
                    .extracted_sets
                    .get(&job_id)
                    .cloned()
                    .unwrap_or_default();
                let sets_to_extract: Vec<(String, weaver_assembly::ArchiveType)> = {
                    let state = self.jobs.get(&job_id).unwrap();
                    state
                        .assembly
                        .archive_topologies()
                        .iter()
                        .filter(|(name, _)| !already_extracted.contains(*name))
                        .map(|(name, topo)| (name.clone(), topo.archive_type))
                        .collect()
                };

                if !sets_to_extract.is_empty() {
                    // If streaming extractions are still active for this job, let them
                    // finish naturally instead of starting full-set extraction (which
                    // would race: full-set completes → deletes volumes → streaming loses data).
                    let has_active_streaming = self
                        .streaming_providers
                        .keys()
                        .any(|(jid, _, _)| *jid == job_id);
                    if has_active_streaming {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        if state.status != JobStatus::Extracting {
                            state.status = JobStatus::Extracting;
                            if let Err(e) =
                                self.db.set_active_job_status(job_id, "extracting", None)
                            {
                                error!(error = %e, "db write failed for extracting status");
                            }
                            self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                            let _ = self
                                .event_tx
                                .send(PipelineEvent::ExtractionReady { job_id });
                        }
                        // Feed all providers and mark finished — all volumes are available.
                        let active_set_names: HashSet<String> = self
                            .streaming_providers
                            .keys()
                            .filter(|(jid, _, _)| *jid == job_id)
                            .map(|(_, sn, _)| sn.clone())
                            .collect();
                        for sn in &active_set_names {
                            self.feed_streaming_volumes(job_id, sn);
                        }
                        for (key, provider) in &self.streaming_providers {
                            if key.0 == job_id {
                                provider.mark_finished();
                            }
                        }
                        info!(
                            job_id = job_id.0,
                            "waiting for streaming extractions to finish before full-set extraction"
                        );
                        return;
                    }

                    // Spawn extraction tasks in the background.
                    // handle_extraction_done will re-enter check_job_completion
                    // when each set finishes, and we'll reach the empty branch below.
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    if state.status != JobStatus::Extracting {
                        state.status = JobStatus::Extracting;
                        if let Err(e) = self.db.set_active_job_status(job_id, "extracting", None) {
                            error!(error = %e, "db write failed for extracting status");
                        }
                        self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionReady { job_id });
                        info!(job_id = job_id.0, "extraction ready");
                    }

                    for (set_name, archive_type) in &sets_to_extract {
                        let result = match archive_type {
                            weaver_assembly::ArchiveType::SevenZip => {
                                self.extract_7z_set(job_id, set_name).await
                            }
                            weaver_assembly::ArchiveType::Rar => {
                                self.extract_rar_set(job_id, set_name).await
                            }
                        };
                        if let Err(e) = result {
                            warn!(job_id = job_id.0, set_name = %set_name, error = %e, "failed to start extraction");
                        }
                    }
                    // Return — extraction runs in background.
                    // handle_extraction_done will call check_job_completion again.
                    return;
                }

                // All sets extracted — finish the job.
                if self
                    .jobs
                    .get(&job_id)
                    .is_some_and(|s| s.status == JobStatus::Extracting)
                {
                    self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
                }
                info!(job_id = job_id.0, "extraction complete");
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Clean up archive source files before moving to complete.
                {
                    let state = self.jobs.get(&job_id).unwrap();
                    let cleanup_dir = state.working_dir.clone();
                    let cleanup_files: Vec<String> = state
                        .assembly
                        .files()
                        .filter(|f| {
                            matches!(
                                f.role(),
                                weaver_core::classify::FileRole::Par2 { .. }
                                    | weaver_core::classify::FileRole::RarVolume { .. }
                                    | weaver_core::classify::FileRole::SevenZipArchive
                                    | weaver_core::classify::FileRole::SevenZipSplit { .. }
                            )
                        })
                        .map(|f| f.filename().to_string())
                        .collect();
                    let mut removed = 0u32;
                    for filename in &cleanup_files {
                        let path = cleanup_dir.join(filename);
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

                // Move extracted files to complete directory.
                self.move_to_complete(job_id).await;

                {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.status = JobStatus::Complete;
                }
                self.streaming_providers
                    .retain(|(jid, _, _), _| *jid != job_id);
                self.streaming_volume_bases
                    .retain(|(jid, _, _), _| *jid != job_id);
                self.promoted_recovery_files.remove(&job_id);
                self.eagerly_deleted.remove(&job_id);
                self.clean_volumes.retain(|(jid, _), _| *jid != job_id);
                self.suspect_volumes.retain(|(jid, _), _| *jid != job_id);
                self.normalization_retried.remove(&job_id);
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Blocked { reason } => {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Failed {
                    error: reason.clone(),
                };
                let _ = self.event_tx.send(PipelineEvent::JobFailed {
                    job_id,
                    error: reason.clone(),
                });
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Partial {
                extractable,
                waiting_on,
            } => {
                debug!(
                    job_id = job_id.0,
                    extractable = ?extractable,
                    waiting = ?waiting_on,
                    "partial extraction possible — waiting for remaining volumes"
                );
            }
        }
    }

    /// Extract a single RAR archive set using streaming extraction.
    ///
    /// Uses a `WaitingVolumeProvider` so volumes are opened on demand as the
    /// decompressor needs them, rather than requiring all volumes upfront.
    /// Already-complete volumes are fed immediately; if called during download,
    /// remaining volumes will be fed as they complete via `feed_streaming_volumes`.
    pub(super) async fn extract_rar_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (first_vol_path, password, working_dir) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for RAR set '{set_name}'"))?;

            // Find the first volume path.
            let first_vol_filename = topo
                .volume_map
                .iter()
                .find(|&(_, &vn)| vn == 0)
                .map(|(name, _)| name.clone())
                .ok_or_else(|| "no volume 0 in topology".to_string())?;

            let path = state.working_dir.join(&first_vol_filename);
            (path, state.spec.password.clone(), state.working_dir.clone())
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();

        // Create or reuse a WaitingVolumeProvider for this set (full-set extraction).
        let set_key = (job_id, set_name.to_string(), String::new());
        let provider = if let Some(existing) = self.streaming_providers.get(&set_key) {
            Arc::clone(existing)
        } else {
            let p = Arc::new(weaver_rar::WaitingVolumeProvider::new());
            self.streaming_providers
                .insert(set_key.clone(), Arc::clone(&p));
            p
        };

        // Feed all currently-complete volumes into the provider.
        self.feed_streaming_volumes(job_id, set_name);

        // If all files are downloaded, mark provider finished so it doesn't block forever.
        {
            let state = self.jobs.get(&job_id).unwrap();
            let all_done = state.assembly.files().all(|f| f.is_complete());
            if all_done {
                provider.mark_finished();
            }
        }

        // Collect already-extracted members so we skip them.
        let already_extracted: HashSet<String> = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_owned = set_name.to_string();
        let extraction_provider = Arc::clone(&provider);
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let first_file = std::fs::File::open(&first_vol_path)
                    .map_err(|e| format!("failed to open first volume: {e}"))?;
                let mut archive = if let Some(ref pw) = password {
                    weaver_rar::RarArchive::open_with_password(first_file, pw)
                } else {
                    weaver_rar::RarArchive::open(first_file)
                }
                .map_err(|e| format!("failed to open RAR archive: {e}"))?;

                let meta = archive.metadata();
                let options = weaver_rar::ExtractOptions {
                    verify: true,
                    password: password.clone(),
                };

                let mut extracted_count = 0u32;
                let mut failed_members: Vec<String> = Vec::new();
                for (idx, member) in meta.members.iter().enumerate() {
                    if already_extracted.contains(&member.name) {
                        extracted_count += 1;
                        continue;
                    }

                    if member.is_directory {
                        let dir_path = output_dir.join(&member.name);
                        std::fs::create_dir_all(&dir_path)
                            .map_err(|e| format!("failed to create dir {}: {e}", member.name))?;
                        continue;
                    }

                    let out_path = output_dir.join(&member.name);
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent)
                            .map_err(|e| format!("failed to create parent dir: {e}"))?;
                    }

                    let mut out_file = std::io::BufWriter::new(
                        std::fs::File::create(&out_path)
                            .map_err(|e| format!("failed to create {}: {e}", member.name))?
                    );
                    match archive.extract_member_streaming(idx, &options, extraction_provider.as_ref(), &mut out_file) {
                        Ok(bytes_written) => {
                            let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                                job_id,
                                member: member.name.clone(),
                                bytes_written,
                                total_bytes: member.unpacked_size.unwrap_or(0),
                            });
                            extracted_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!(member = %member.name, error = %e, "member extraction failed, continuing with remaining members");
                            // Clean up partial output file.
                            let _ = std::fs::remove_file(&out_path);
                            failed_members.push(member.name.clone());
                        }
                    }
                }

                Ok((extracted_count, failed_members))
            })
            .await;

            let result = match result {
                Ok(Ok(v)) => Ok(v),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(format!("extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_owned,
                    result,
                })
                .await;
        });

        // Extraction runs in background — result comes through extract_done_tx channel.
        Ok(0)
    }

    /// Extract a single 7z archive set. Only collects files belonging to the named set.
    pub(super) async fn extract_7z_set(
        &mut self,
        job_id: JobId,
        set_name: &str,
    ) -> Result<u32, String> {
        let (file_paths, password, working_dir) = {
            let state = self
                .jobs
                .get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state
                .assembly
                .archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            // Collect files belonging to this set using the topology's volume_map.
            let set_filenames: std::collections::HashSet<&str> =
                topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename()) {
                    let vol = topo
                        .volume_map
                        .get(file_asm.filename())
                        .copied()
                        .unwrap_or(0);
                    parts.push((vol, state.working_dir.join(file_asm.filename())));
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (
                paths,
                state.spec.password.clone(),
                state.working_dir.clone(),
            )
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();

        let extract_done_tx = self.extract_done_tx.clone();
        let set_name_for_channel = set_name.to_string();
        tokio::task::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                if file_paths.is_empty() {
                    return Err(format!("no 7z files found for set '{set_name_owned}'"));
                }

                let pw = if let Some(ref p) = password {
                    sevenz_rust2::Password::new(p)
                } else {
                    sevenz_rust2::Password::empty()
                };

                let mut extracted_count = 0u32;
                let extracted_count_ref = &mut extracted_count;
                let event_tx_ref = &event_tx;
                let output_dir_ref = &output_dir;

                let extract_fn = |entry: &sevenz_rust2::ArchiveEntry,
                                  reader: &mut dyn std::io::Read,
                                  _dest: &PathBuf|
                 -> Result<bool, sevenz_rust2::Error> {
                    if entry.is_directory() {
                        let dir_path = output_dir_ref.join(entry.name());
                        std::fs::create_dir_all(&dir_path)?;
                        return Ok(true);
                    }

                    let out_path = output_dir_ref.join(entry.name());
                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent)?;
                    }

                    let mut file = std::fs::File::create(&out_path)?;
                    let bytes_written = std::io::copy(reader, &mut file)?;

                    let _ = event_tx_ref.send(PipelineEvent::ExtractionProgress {
                        job_id,
                        member: entry.name().to_string(),
                        bytes_written,
                        total_bytes: entry.size(),
                    });

                    *extracted_count_ref += 1;
                    Ok(true)
                };

                if file_paths.len() == 1 {
                    let file = std::fs::File::open(&file_paths[0])
                        .map_err(|e| format!("failed to open 7z file: {e}"))?;
                    sevenz_rust2::decompress_with_extract_fn_and_password(
                        file,
                        &output_dir,
                        pw,
                        extract_fn,
                    )
                    .map_err(|e| format!("7z extraction failed: {e}"))?;
                } else {
                    let reader = weaver_core::split_reader::SplitFileReader::open(&file_paths)
                        .map_err(|e| format!("failed to open 7z split files: {e}"))?;
                    sevenz_rust2::decompress_with_extract_fn_and_password(
                        reader,
                        &output_dir,
                        pw,
                        extract_fn,
                    )
                    .map_err(|e| format!("7z extraction failed: {e}"))?;
                }

                Ok((extracted_count, Vec::new()))
            })
            .await;

            let result = match result {
                Ok(r) => r,
                Err(e) => Err(format!("7z extraction task panicked: {e}")),
            };
            let _ = extract_done_tx
                .send(ExtractionDone::FullSet {
                    job_id,
                    set_name: set_name_for_channel,
                    result,
                })
                .await;
        });

        // Return Ok(0) for now — actual result comes through the channel.
        Ok(0)
    }

    /// Delete RAR volumes that are no longer needed by any active extraction.
    ///
    /// Called after a member finishes streaming extraction with CRC pass.
    /// A volume can be deleted when no active streaming provider still needs it.
    /// Deleted filenames are tracked in `eagerly_deleted` so PAR2 verify excludes
    /// them from the damage count (they passed CRC during extraction).
    pub(super) fn try_delete_volumes(&mut self, job_id: JobId, set_name: &str) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let Some(topo) = state.assembly.archive_topology_for(set_name) else {
            return;
        };

        // A volume is deletable when every member spanning it has been extracted.
        let extracted = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();
        let deletable = topo.deletable_volumes(&extracted);

        // Active streaming providers still need volumes at or above their base
        // (open-ended because we don't know when the stream will finish).
        let stream_bases: Vec<u32> = self
            .streaming_volume_bases
            .iter()
            .filter(|((jid, sn, _), _)| *jid == job_id && sn == set_name)
            .map(|(_, &base)| base)
            .collect();
        let clean = self
            .clean_volumes
            .get(&(job_id, set_name.to_string()))
            .cloned()
            .unwrap_or_default();
        let suspect = self
            .suspect_volumes
            .get(&(job_id, set_name.to_string()))
            .cloned()
            .unwrap_or_default();

        let working_dir = state.working_dir.clone();
        let mut deleted = 0u32;

        for (filename, &vol_num) in &topo.volume_map {
            if !deletable.contains(&vol_num) {
                continue;
            }
            if !clean.contains(&vol_num) {
                continue;
            }
            if suspect.contains(&vol_num) {
                continue;
            }
            if stream_bases.iter().any(|&base| vol_num >= base) {
                continue;
            }

            let path = working_dir.join(filename);
            match std::fs::remove_file(&path) {
                Ok(()) => {
                    deleted += 1;
                    self.eagerly_deleted
                        .entry(job_id)
                        .or_default()
                        .insert(filename.clone());
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    warn!(file = %path.display(), error = %e, "failed to delete volume");
                }
            }
        }

        if deleted > 0 {
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                deleted,
                "eagerly deleted RAR volumes"
            );
        }
    }
}
