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
            && !cat.is_empty() {
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

    /// Build a VerificationResult from incremental assembly state, avoiding
    /// a full re-read and re-verify from disk.
    pub(super) fn build_verification_from_assembly(&self, job_id: JobId) -> Option<VerificationResult> {
        let state = self.jobs.get(&job_id)?;
        let par2_set = self.par2_sets.get(&job_id)?;

        let recovery_blocks_available = par2_set.recovery_block_count();
        let mut files = Vec::new();
        let mut total_missing_blocks = 0u32;

        // For each file described in the PAR2 set, find the corresponding assembly
        // file and extract its slice verification state.
        for file_desc in par2_set.recovery_files() {
            let slice_count = par2_set.slice_count_for_file(file_desc.length) as usize;

            // Find the assembly file by matching filename.
            let asm_file = state.assembly.files().find(|f| f.filename() == file_desc.filename);

            let (valid_slices, missing_count) = if let Some(asm) = asm_file {
                if let Some(results) = asm.slice_verification_results() {
                    let mut valid = vec![false; slice_count];
                    let mut reported = vec![false; slice_count];
                    let mut missing = 0u32;
                    for &(idx, verified) in &results {
                        if (idx as usize) < slice_count {
                            reported[idx as usize] = true;
                            let is_valid = verified.unwrap_or(false);
                            valid[idx as usize] = is_valid;
                            if !is_valid {
                                missing += 1;
                            }
                        }
                    }
                    // Count slices that weren't reported at all as missing.
                    for &was_reported in reported.iter().take(slice_count) {
                        if !was_reported {
                            missing += 1;
                        }
                    }
                    (valid, missing)
                } else {
                    // No slice states — treat all slices as unverified/missing.
                    (vec![false; slice_count], slice_count as u32)
                }
            } else {
                // File not in assembly — missing entirely.
                (vec![false; slice_count], slice_count as u32)
            };

            let status = if missing_count == 0 {
                FileStatus::Complete
            } else if asm_file.map(|f| f.is_complete()).unwrap_or(false) {
                FileStatus::Damaged(missing_count)
            } else {
                FileStatus::Missing
            };

            total_missing_blocks += missing_count;

            files.push(FileVerification {
                file_id: file_desc.file_id,
                filename: file_desc.filename.clone(),
                status,
                valid_slices,
                missing_slice_count: missing_count,
            });
        }

        let repairable = if total_missing_blocks == 0 {
            Repairability::NotNeeded
        } else if recovery_blocks_available >= total_missing_blocks {
            Repairability::Repairable {
                blocks_needed: total_missing_blocks,
                blocks_available: recovery_blocks_available,
            }
        } else {
            Repairability::Insufficient {
                blocks_needed: total_missing_blocks,
                blocks_available: recovery_blocks_available,
                deficit: total_missing_blocks - recovery_blocks_available,
            }
        };

        Some(VerificationResult {
            files,
            recovery_blocks_available,
            total_missing_blocks,
            repairable,
        })
    }

    /// Check if all files in a job are complete, and trigger post-processing.
    pub(super) async fn check_job_completion(&mut self, job_id: JobId) {
        // Check completeness.
        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return;
            };
            if state.assembly.complete_file_count() < state.assembly.total_file_count() {
                return;
            }
        }

        // All files downloaded. Run verification if we have PAR2 metadata.
        let has_par2 = self
            .jobs
            .get(&job_id)
            .is_some_and(|s| s.assembly.repair_confidence().is_some());

        if has_par2 {
            {
                let state = self.jobs.get_mut(&job_id).unwrap();
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

            // Most verification is done incrementally during download via
            // FileAssembly::commit_segment(). However, segments that arrived
            // before PAR2 metadata was loaded used commit_segment_meta() and
            // their data was dropped without feeding slice checksums.
            // Re-read those unverified slices from disk now.
            self.verify_unverified_slices_from_disk(job_id).await;

            self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

            // Now get final results after disk re-verification.
            let (damaged, _total, recovery) = self
                .jobs
                .get(&job_id)
                .and_then(|s| s.assembly.repair_confidence())
                .unwrap_or((0, 0, 0));

            if damaged == 0 {
                info!(
                    job_id = job_id.0,
                    total_slices = _total,
                    recovery_available = recovery,
                    "PAR2 verification passed — no damaged slices"
                );
            } else {
                info!(
                    job_id = job_id.0,
                    damaged,
                    total_slices = _total,
                    recovery_available = recovery,
                    "PAR2 verification complete — damage detected"
                );
            }

            if damaged > 0 {
                if recovery >= damaged {
                    {
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Repairing;
                    }
                    if let Err(e) = self.db.set_active_job_status(job_id, "repairing", None) {
                        error!(error = %e, "db write failed for repairing status");
                    }
                    self.metrics.repair_active.fetch_add(1, Ordering::Relaxed);
                    let _ = self.event_tx.send(PipelineEvent::RepairStarted { job_id });
                    info!(
                        job_id = job_id.0,
                        damaged, recovery, "repair needed and possible"
                    );

                    // Build VerificationResult from assembly state (no re-read from disk).
                    let verification = self.build_verification_from_assembly(job_id);

                    // Use the retained Par2FileSet (already merged with recovery volumes).
                    let par2_set = self.par2_sets.get(&job_id).cloned();
                    let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();

                    let repair_result = tokio::task::spawn_blocking(move || {
                        let par2_set = par2_set.ok_or_else(|| {
                            "PAR2 file set not retained — cannot repair".to_string()
                        })?;

                        let verification = verification.ok_or_else(|| {
                            "could not build verification from assembly state".to_string()
                        })?;

                        // Build DiskFileAccess for repair I/O.
                        let mut file_access =
                            weaver_par2::DiskFileAccess::new(working_dir, &par2_set);

                        // Plan repair from incremental verification state.
                        let plan = weaver_par2::plan_repair(&par2_set, &verification)
                            .map_err(|e| format!("repair planning failed: {e}"))?;

                        let slices_to_repair = plan.missing_slices.len() as u32;

                        // Execute repair.
                        weaver_par2::execute_repair(&plan, &par2_set, &mut file_access)
                            .map_err(|e| format!("repair execution failed: {e}"))?;

                        Ok(slices_to_repair)
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
                            info!(
                                job_id = job_id.0,
                                slices_repaired, "PAR2 repair complete"
                            );
                            let _ = self.event_tx.send(PipelineEvent::RepairComplete {
                                job_id,
                                slices_repaired,
                            });

                            // Re-fetch state and fall through to extraction check below.
                            let Some(state) = self.jobs.get_mut(&job_id) else {
                                return;
                            };
                            state.status = JobStatus::Downloading;
                            if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                                error!(error = %e, "db write failed for downloading status");
                            }
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
                } else {
                    let state = self.jobs.get_mut(&job_id).unwrap();
                    state.status = JobStatus::Failed {
                        error: format!(
                            "not repairable: {damaged} damaged slices, only {recovery} recovery blocks"
                        ),
                    };
                    let _ = self.event_tx.send(PipelineEvent::JobFailed {
                        job_id,
                        error: format!("not repairable: {damaged} damaged, {recovery} recovery"),
                    });
                    self.record_job_history(job_id);
                    return;
                }
            }
        }

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
                state.status = JobStatus::Complete;
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                self.record_job_history(job_id);
            }
            ExtractionReadiness::Ready => {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Extracting;
                if let Err(e) = self.db.set_active_job_status(job_id, "extracting", None) {
                    error!(error = %e, "db write failed for extracting status");
                }
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
                info!(job_id = job_id.0, "extraction ready");

                // Collect sets that still need extraction (some may have been
                // extracted during the partial extraction phase).
                let already_extracted = self.extracted_sets.get(&job_id).cloned().unwrap_or_default();
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

                let mut extract_outcome: Result<u32, String> = Ok(0);
                for (set_name, archive_type) in &sets_to_extract {
                    let result = match archive_type {
                        weaver_assembly::ArchiveType::SevenZip => {
                            self.extract_7z_set(job_id, set_name).await
                        }
                        weaver_assembly::ArchiveType::Rar => {
                            self.extract_rar_set(job_id, set_name).await
                        }
                    };
                    match result {
                        Ok(count) => {
                            if let Ok(ref mut total) = extract_outcome {
                                *total += count;
                            }
                        }
                        Err(e) => {
                            extract_outcome = Err(e);
                            break;
                        }
                    }
                }

                // If no sets needed extraction (all done in partial phase), still succeed.
                if sets_to_extract.is_empty() {
                    extract_outcome = Ok(0);
                }

                self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);

                match extract_outcome {
                    Ok(count) => {
                        info!(job_id = job_id.0, members = count, "extraction complete");
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
                        self.streaming_providers.retain(|(jid, _), _| *jid != job_id);
                        self.job_order.retain(|id| *id != job_id);
                        let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                        self.record_job_history(job_id);
                    }
                    Err(error_str) => {
                        warn!(job_id = job_id.0, error = %error_str, "extraction failed");
                        let _ = self.event_tx.send(PipelineEvent::ExtractionFailed {
                            job_id,
                            error: error_str.clone(),
                        });
                        let state = self.jobs.get_mut(&job_id).unwrap();
                        state.status = JobStatus::Failed {
                            error: error_str.clone(),
                        };
                        self.record_job_history(job_id);
                    }
                }
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

    /// Extract a single RAR archive set. Only collects volumes belonging to the named set.
    pub(super) async fn extract_rar_set(&mut self, job_id: JobId, set_name: &str) -> Result<u32, String> {
        let (volume_paths, password, working_dir) = {
            let state = self.jobs.get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state.assembly.archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for RAR set '{set_name}'"))?;

            // Collect only volumes belonging to this set via the topology's volume_map.
            let set_filenames: std::collections::HashSet<&str> = topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut vols: Vec<(u32, PathBuf)> = Vec::new();
            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename())
                    && let weaver_core::classify::FileRole::RarVolume { volume_number } =
                        file_asm.role()
                    {
                        vols.push((
                            *volume_number,
                            state.working_dir.join(file_asm.filename()),
                        ));
                    }
            }
            vols.sort_by_key(|(vn, _)| *vn);
            let paths: Vec<PathBuf> = vols.into_iter().map(|(_, p)| p).collect();
            (paths, state.spec.password.clone(), state.working_dir.clone())
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();

        // Mark streaming provider as finished (all volumes available).
        let set_key = (job_id, set_name.to_string());
        if let Some(provider) = self.streaming_providers.get(&set_key) {
            provider.mark_finished();
        }

        // Collect already-extracted members so we skip them.
        let already_extracted: HashSet<String> = self
            .extracted_members
            .get(&job_id)
            .cloned()
            .unwrap_or_default();

        let extract_result = tokio::task::spawn_blocking(move || {
            if volume_paths.is_empty() {
                return Err("no RAR volumes found".to_string());
            }

            let first_file = std::fs::File::open(&volume_paths[0])
                .map_err(|e| format!("failed to open first volume: {e}"))?;
            let mut archive = if let Some(ref pw) = password {
                weaver_rar::RarArchive::open_with_password(first_file, pw)
            } else {
                weaver_rar::RarArchive::open(first_file)
            }
            .map_err(|e| format!("failed to open RAR archive: {e}"))?;

            for (i, path) in volume_paths.iter().enumerate().skip(1) {
                let vol_file = std::fs::File::open(path)
                    .map_err(|e| format!("failed to open volume {i}: {e}"))?;
                archive
                    .add_volume(i, Box::new(vol_file))
                    .map_err(|e| format!("failed to add volume {i}: {e}"))?;
            }

            let meta = archive.metadata();
            let options = weaver_rar::ExtractOptions {
                verify: true,
                password: password.clone(),
            };

            let mut extracted_count = 0u32;
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
                let bytes_written = archive
                    .extract_member_to_file(idx, &options, None, &out_path)
                    .map_err(|e| format!("failed to extract {}: {e}", member.name))?;

                let _ = event_tx.send(PipelineEvent::ExtractionProgress {
                    job_id,
                    member: member.name.clone(),
                    bytes_written,
                    total_bytes: member.unpacked_size.unwrap_or(0),
                });

                extracted_count += 1;
            }

            Ok(extracted_count)
        })
        .await;

        match extract_result {
            Ok(Ok(count)) => Ok(count),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("extraction task panicked: {e}")),
        }
    }

    /// Extract a single 7z archive set. Only collects files belonging to the named set.
    pub(super) async fn extract_7z_set(&mut self, job_id: JobId, set_name: &str) -> Result<u32, String> {
        let (file_paths, password, working_dir) = {
            let state = self.jobs.get(&job_id)
                .ok_or_else(|| format!("job {job_id:?} not found"))?;
            let topo = state.assembly.archive_topology_for(set_name)
                .ok_or_else(|| format!("no topology for set '{set_name}'"))?;

            // Collect files belonging to this set using the topology's volume_map.
            let set_filenames: std::collections::HashSet<&str> = topo.volume_map.keys().map(|s| s.as_str()).collect();
            let mut parts: Vec<(u32, PathBuf)> = Vec::new();

            for file_asm in state.assembly.files() {
                if set_filenames.contains(file_asm.filename()) {
                    let vol = topo.volume_map.get(file_asm.filename()).copied().unwrap_or(0);
                    parts.push((vol, state.working_dir.join(file_asm.filename())));
                }
            }
            parts.sort_by_key(|(n, _)| *n);
            let paths: Vec<PathBuf> = parts.into_iter().map(|(_, p)| p).collect();
            (paths, state.spec.password.clone(), state.working_dir.clone())
        };

        let output_dir = working_dir;
        let event_tx = self.event_tx.clone();
        let set_name_owned = set_name.to_string();

        let extract_result = tokio::task::spawn_blocking(move || {
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

            let extract_fn =
                |entry: &sevenz_rust2::ArchiveEntry,
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
                let reader =
                    weaver_core::split_reader::SplitFileReader::open(&file_paths)
                        .map_err(|e| format!("failed to open 7z split files: {e}"))?;
                sevenz_rust2::decompress_with_extract_fn_and_password(
                    reader,
                    &output_dir,
                    pw,
                    extract_fn,
                )
                .map_err(|e| format!("7z extraction failed: {e}"))?;
            }

            Ok(extracted_count)
        })
        .await;

        match extract_result {
            Ok(Ok(count)) => Ok(count),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(format!("7z extraction task panicked: {e}")),
        }
    }
}
