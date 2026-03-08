use super::*;

impl Pipeline {
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
                    for i in 0..slice_count {
                        if !reported[i] {
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
            .map_or(false, |s| s.assembly.repair_confidence().is_some());

        if has_par2 {
            {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Verifying;
            }
            let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                job_id,
                status: weaver_state::PersistedJobStatus::Verifying,
                timestamp: timestamp_secs(),
            }).await;
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
                    let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                        job_id,
                        status: weaver_state::PersistedJobStatus::Repairing,
                        timestamp: timestamp_secs(),
                    }).await;
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
                    let output_dir = self.output_dir.clone();

                    let repair_result = tokio::task::spawn_blocking(move || {
                        let par2_set = par2_set.ok_or_else(|| {
                            "PAR2 file set not retained — cannot repair".to_string()
                        })?;

                        let verification = verification.ok_or_else(|| {
                            "could not build verification from assembly state".to_string()
                        })?;

                        // Build DiskFileAccess for repair I/O.
                        let mut file_access =
                            weaver_par2::DiskFileAccess::new(output_dir, &par2_set);

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
                            let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                                job_id,
                                status: weaver_state::PersistedJobStatus::Downloading,
                                timestamp: timestamp_secs(),
                            }).await;
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
                            let _ = self
                                .journal
                                .append(&JournalEntry::JobStatusChanged {
                                    job_id,
                                    status: weaver_state::PersistedJobStatus::Failed {
                                        error: error_msg,
                                    },
                                    timestamp: timestamp_secs(),
                                })
                                .await;
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
                    let _ = self
                        .journal
                        .append(&JournalEntry::JobStatusChanged {
                            job_id,
                            status: weaver_state::PersistedJobStatus::Failed {
                                error: format!(
                                    "not repairable: {damaged} damaged, {recovery} recovery"
                                ),
                            },
                            timestamp: timestamp_secs(),
                        })
                        .await;
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
                // No archives — job is done.
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Complete;
                self.job_order.retain(|id| *id != job_id);
                let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                info!(job_id = job_id.0, "job completed (no archives)");
                let _ = self
                    .journal
                    .append(&JournalEntry::JobStatusChanged {
                        job_id,
                        status: weaver_state::PersistedJobStatus::Complete,
                        timestamp: timestamp_secs(),
                    })
                    .await;
            }
            ExtractionReadiness::Ready => {
                let state = self.jobs.get_mut(&job_id).unwrap();
                state.status = JobStatus::Extracting;
                let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                    job_id,
                    status: weaver_state::PersistedJobStatus::Extracting,
                    timestamp: timestamp_secs(),
                }).await;
                self.metrics.extract_active.fetch_add(1, Ordering::Relaxed);
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionReady { job_id });
                info!(job_id = job_id.0, "extraction ready");

                // Collect RAR volume paths in order and the password.
                let (volume_paths, password) = {
                    let state = self.jobs.get(&job_id).unwrap();
                    let mut vols: Vec<(u32, PathBuf)> = Vec::new();
                    for file_asm in state.assembly.files() {
                        if let weaver_core::classify::FileRole::RarVolume { volume_number } =
                            file_asm.role()
                        {
                            vols.push((
                                *volume_number,
                                self.output_dir.join(file_asm.filename()),
                            ));
                        }
                    }
                    vols.sort_by_key(|(vn, _)| *vn);
                    let paths: Vec<PathBuf> = vols.into_iter().map(|(_, p)| p).collect();
                    (paths, state.spec.password.clone())
                };

                let output_dir = self.output_dir.clone();
                let event_tx = self.event_tx.clone();

                // Mark streaming provider as finished (all volumes available).
                if let Some(provider) = self.streaming_providers.get(&job_id) {
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

                    // Open the first volume.
                    let first_file = std::fs::File::open(&volume_paths[0])
                        .map_err(|e| format!("failed to open first volume: {e}"))?;
                    let mut archive = if let Some(ref pw) = password {
                        weaver_rar::RarArchive::open_with_password(first_file, pw)
                    } else {
                        weaver_rar::RarArchive::open(first_file)
                    }
                    .map_err(|e| format!("failed to open RAR archive: {e}"))?;

                    // Add remaining volumes.
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
                        // Skip members already extracted by streaming extraction.
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

                self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);

                let extract_outcome = match extract_result {
                    Ok(Ok(count)) => Ok(count),
                    Ok(Err(e)) => Err(e),
                    Err(e) => Err(format!("extraction task panicked: {e}")),
                };

                match extract_outcome {
                    Ok(count) => {
                        info!(job_id = job_id.0, members = count, "extraction complete");
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::ExtractionComplete { job_id });

                        // Clean up PAR2 and RAR source files — no longer needed.
                        {
                            let state = self.jobs.get(&job_id).unwrap();
                            let cleanup_files: Vec<String> = state
                                .assembly
                                .files()
                                .filter(|f| {
                                    matches!(
                                        f.role(),
                                        weaver_core::classify::FileRole::Par2 { .. }
                                        | weaver_core::classify::FileRole::RarVolume { .. }
                                    )
                                })
                                .map(|f| f.filename().to_string())
                                .collect();
                            let output_dir = self.output_dir.clone();
                            let jid = job_id.0;
                            tokio::spawn(async move {
                                let mut removed = 0u32;
                                for filename in &cleanup_files {
                                    let path = output_dir.join(filename);
                                    match tokio::fs::remove_file(&path).await {
                                        Ok(()) => removed += 1,
                                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                                        Err(e) => {
                                            tracing::warn!(
                                                file = %path.display(),
                                                error = %e,
                                                "failed to clean up source file"
                                            );
                                        }
                                    }
                                }
                                tracing::info!(
                                    job_id = jid,
                                    removed,
                                    total = cleanup_files.len(),
                                    "post-extraction cleanup complete"
                                );
                            });
                        }

                        {
                            let state = self.jobs.get_mut(&job_id).unwrap();
                            state.status = JobStatus::Complete;
                        }
                        self.streaming_providers.remove(&job_id);
                        self.job_order.retain(|id| *id != job_id);
                        let _ = self.event_tx.send(PipelineEvent::JobCompleted { job_id });
                        let _ = self
                            .journal
                            .append(&JournalEntry::JobStatusChanged {
                                job_id,
                                status: weaver_state::PersistedJobStatus::Complete,
                                timestamp: timestamp_secs(),
                            })
                            .await;
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
                        let _ = self
                            .journal
                            .append(&JournalEntry::JobStatusChanged {
                                job_id,
                                status: weaver_state::PersistedJobStatus::Failed {
                                    error: error_str,
                                },
                                timestamp: timestamp_secs(),
                            })
                            .await;
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
                let _ = self
                    .journal
                    .append(&JournalEntry::JobStatusChanged {
                        job_id,
                        status: weaver_state::PersistedJobStatus::Failed { error: reason },
                        timestamp: timestamp_secs(),
                    })
                    .await;
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
}
