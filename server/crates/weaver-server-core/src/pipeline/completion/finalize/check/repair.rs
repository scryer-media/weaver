//! Continuation of the `impl Pipeline` block from `finalize/check.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    pub(in crate::pipeline) async fn try_deobfuscate_files_with_par2(
        &mut self,
        job_id: JobId,
    ) -> Par2DeobfuscationOutcome {
        let Some(state) = self.jobs.get(&job_id) else {
            return Par2DeobfuscationOutcome::default();
        };
        let rename_dir = state.working_dir.clone();

        if weaver_nzb::is_protected_media_structure(&rename_dir) {
            info!(
                job_id = job_id.0,
                "skipping PAR2 rename inside protected media structure"
            );
            return Par2DeobfuscationOutcome::default();
        }

        let mut suggestions = Vec::new();
        let mut seen_current_paths = HashSet::new();
        let mut seen_hashes = HashSet::<[u8; 16]>::new();
        let mut ambiguous_hashes = HashSet::<[u8; 16]>::new();
        let mut descriptions_by_name =
            HashMap::<String, Vec<(par2_rs::RecoverySetId, par2_rs::FileId, u64, [u8; 16])>>::new();
        let set_ids = self
            .par2_runtime(job_id)
            .map(crate::pipeline::Par2RuntimeState::ordered_set_ids)
            .unwrap_or_default();
        for set_id in set_ids {
            let Some(par2) = self.par2_set_for(job_id, set_id) else {
                continue;
            };
            for description in par2.files.values() {
                if !seen_hashes.insert(description.hash_16k) {
                    ambiguous_hashes.insert(description.hash_16k);
                }
                descriptions_by_name
                    .entry(sanitize_download_filename(&description.filename))
                    .or_default()
                    .push((
                        set_id,
                        description.file_id,
                        description.length,
                        description.hash_16k,
                    ));
            }

            let set_suggestions = match par2_rs::scan_for_renames(&rename_dir, par2) {
                Ok(suggestions) => suggestions,
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        recovery_set_id = %set_id,
                        error = %error,
                        "PAR2 rename scan failed"
                    );
                    continue;
                }
            };
            for suggestion in set_suggestions {
                if seen_current_paths.insert(suggestion.current_path.clone()) {
                    suggestions.push((set_id, suggestion));
                }
            }
        }

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
        let mut occupied_filenames = HashSet::<String>::new();
        for (_, identity, _) in &file_rows {
            reserve_identity_filenames(identity, &mut occupied_filenames);
        }
        reserve_directory_filenames(&state.working_dir, &mut occupied_filenames);
        let _ = state;

        let mut outcome = Par2DeobfuscationOutcome::default();
        let mut touched_files = Vec::<NzbFileId>::new();
        let mut touched_rar_files = HashMap::<String, HashSet<String>>::new();
        for (set_id, suggestion) in &suggestions {
            let old = &suggestion.current_path;
            let requested_correct_name = sanitize_download_filename(&suggestion.correct_name);
            let old_name = old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .unwrap_or_default();
            let matched = by_current
                .get(&old_name)
                .copied()
                .or_else(|| by_source.get(&old_name).copied())
                .or_else(|| by_canonical.get(&old_name).copied());
            let Some(description) = self
                .par2_set_for(job_id, *set_id)
                .and_then(|set| set.file_description(&suggestion.file_id))
            else {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename with an unknown description"
                );
                continue;
            };
            if ambiguous_hashes.contains(&description.hash_16k) {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename with an ambiguous 16 KiB hash"
                );
                continue;
            }
            if descriptions_by_name
                .get(&requested_correct_name)
                .is_some_and(|descriptions| {
                    descriptions
                        .iter()
                        .any(|(described_set_id, _, length, hash_16k)| {
                            *described_set_id != *set_id
                                && (*length != description.length
                                    || *hash_16k != description.hash_16k)
                        })
                })
            {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %requested_correct_name,
                    "refusing PAR2 rename with conflicting recovery-set descriptions"
                );
                continue;
            }
            if crate::pipeline::is_split_fragment_of(&old_name, &suggestion.correct_name) {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %suggestion.correct_name,
                    "refusing PAR2 rename of a split fragment"
                );
                continue;
            }
            let disk_len = match std::fs::metadata(old) {
                Ok(metadata) => metadata.len(),
                Err(error) => {
                    debug!(
                        job_id = job_id.0,
                        from = %old.display(),
                        error = %error,
                        "refusing PAR2 rename whose source length could not be read"
                    );
                    continue;
                }
            };
            let length_contradicts = match matched {
                Some((_, true)) | None => disk_len != description.length,
                Some((_, false)) => disk_len > description.length,
            };
            if length_contradicts {
                debug!(
                    job_id = job_id.0,
                    from = %old.display(),
                    observed_length = disk_len,
                    described_length = description.length,
                    "refusing PAR2 rename with a contradictory file length"
                );
                continue;
            }
            let mut target_occupied = occupied_filenames.clone();
            if let Some((file_id, _)) = matched
                && let Some((_, identity, _)) = file_rows
                    .iter()
                    .find(|(candidate_file_id, _, _)| *candidate_file_id == file_id)
            {
                forget_identity_filenames(identity, &mut target_occupied);
            }
            let correct_name =
                allocate_unique_download_filename(&requested_correct_name, &mut target_occupied);
            // A suggestion whose source is not an NZB entry is a stray on disk
            // — most often the pre-repair backup par2-rs leaves behind, whose
            // first 16 KiB still match the description because the damage the
            // repair fixed lay further in. When the canonical name is already
            // taken, renaming it here mints a `.duplicateN` sibling of the file
            // that just passed verification, and the final move — which
            // relocates the whole working directory — delivers both. That is
            // how a 962 MB damaged copy shipped beside its repaired original.
            //
            // Renaming an unmatched stray into a *free* canonical slot is still
            // allowed, so genuine deobfuscation is untouched; only the
            // duplicate-minting branch is closed.
            if matched.is_none() && correct_name != requested_correct_name {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    requested = %requested_correct_name,
                    "refusing to rename a file the NZB never declared into a duplicate name"
                );
                continue;
            }
            if old.strip_prefix(&rename_dir).is_err() {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    "refusing PAR2 rename whose source escapes the job directory"
                );
                continue;
            }
            let new = old.parent().unwrap().join(&correct_name);
            if new.strip_prefix(&rename_dir).is_err() {
                warn!(
                    job_id = job_id.0,
                    to = %new.display(),
                    "refusing PAR2 rename whose target escapes the job directory"
                );
                continue;
            }
            if old
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                == Some(correct_name.clone())
            {
                continue;
            }

            if new.exists() && !runtime_fs::paths_equivalent_for_placement(old, &new) {
                warn!(
                    job_id = job_id.0,
                    from = %old.display(),
                    to = %new.display(),
                    "PAR2 rename target already exists"
                );
                continue;
            }

            let renamed_successfully = match runtime_fs::rename_no_overwrite(old, &new) {
                Ok(()) => {
                    outcome.renamed += 1;
                    // The path a chase holds for this part has just gone.
                    if let Some(name) = old.file_name().and_then(|name| name.to_str()) {
                        let name = name.to_string();
                        self.taint_direct_unpack_for_file(job_id, &name);
                    }
                    if correct_name == requested_correct_name
                        && let Some(descriptions) =
                            descriptions_by_name.get(&requested_correct_name)
                    {
                        for (set_id, file_id, _, _) in descriptions {
                            outcome
                                .canonical_description_file_ids
                                .entry(*set_id)
                                .or_default()
                                .insert(*file_id);
                        }
                    }
                    reserve_download_filename(&correct_name, &mut occupied_filenames);
                    info!(
                        job_id = job_id.0,
                        from = %old.file_name().unwrap().to_string_lossy(),
                        to = %correct_name,
                        "deobfuscated file via PAR2 metadata"
                    );
                    true
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        from = %old.display(),
                        to = %new.display(),
                        error = %error,
                        "PAR2 rename failed"
                    );
                    false
                }
            };
            if !renamed_successfully {
                continue;
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
                let classification = Self::canonical_archive_identity_from_filename(&correct_name)
                    .or(identity.classification.clone());
                let new_rar_set_name = classification.as_ref().and_then(|classification| {
                    matches!(
                        classification.kind,
                        crate::jobs::assembly::DetectedArchiveKind::Rar
                    )
                    .then(|| classification.set_name.clone())
                });
                for set_name in [old_rar_set_name, new_rar_set_name].into_iter().flatten() {
                    touched_rar_files
                        .entry(set_name)
                        .or_default()
                        .insert(old_current_filename.clone());
                }
                let mut rebound_identity = identity;
                rebound_identity.current_filename = correct_name.clone();
                rebound_identity.canonical_filename = Some(correct_name.clone());
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

        if outcome.renamed > 0 {
            // Identity changed, not the bytes owned by this NzbFileId. The
            // binding resolver revalidates names live; raw grid evidence stays
            // available for every recovery set.
            //
            // A parked damaged-path verdict does not survive it: that verdict
            // names files by path, and a repair standing on it would hand the
            // post-repair read-back names nothing carries any more.
            self.clear_pending_par2_repairs_for_job(job_id);
            info!(
                job_id = job_id.0,
                renamed = outcome.renamed,
                "PAR2 deobfuscation complete"
            );
        }

        outcome
    }

    /// `verification` is the analysis result that led here, and is the
    /// file-level half of the direct-unpack vouching evidence. `None` for a
    /// preview run, which rewrites nothing and so parks nothing.
    pub(super) async fn run_par2_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        repair: bool,
        verification: Option<&par2_rs::VerificationResult>,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
        let set_id = par2_set.recovery_set_id;
        if repair {
            self.fence_par3_before_par2_repair(job_id, set_id, verification)?;
            // The repairer is about to rewrite damaged sources in place. A
            // chase that consumed only bytes the recovery set positively found
            // Intact is safe to leave parked through that — repair cannot
            // rewrite what it has already read — and resumes afterwards over
            // the repaired file. Anything else is tainted, exactly as it was
            // before: par2-rs reports what it rewrote as its own file ids
            // rather than as weaver filenames, so an unvouched chase gets no
            // benefit of the doubt.
            // INVARIANT: every exit from this function below this line must
            // settle the sets this leaves parked, via
            // `settle_direct_unpack_after_repair`. A set parked through a
            // repair is held under a damage cap that only that call lifts, and
            // a chase left under one waits on a frontier nothing will advance —
            // silently, until the job is torn down. If you add an early return
            // below, settle first.
            self.decide_direct_unpack_before_repair(job_id, verification);
            // What the directory held before the repairer touched it, so the
            // artefacts it leaves behind can be named afterwards by difference
            // rather than by guessing at a backup-suffix convention that lives
            // in another crate.
            //
            // The FIRST such state is the baseline for the whole job, not the
            // most recent one. A second set's repair runs with the first set's
            // backups already on disk, and re-snapshotting here would enrol
            // them as though they had always been there — which is precisely
            // how a damaged original survives into the delivered output.
            self.par2_pre_repair_dir_entries
                .entry(job_id)
                .or_insert_with(|| directory_entry_names(&working_dir));
        }

        #[cfg(test)]
        {
            if repair {
                self.par2_repairer_execute_calls += 1;
            } else {
                self.par2_repairer_analyze_calls += 1;
            }
        }

        // Repair retires the live grid below, because its verdicts describe
        // the pre-repair file generation. Keep one local snapshot solely for
        // the descriptor-bounded retry: that retry validates every source
        // slice as it reads it and never returns the evidence to runtime state.
        let repair_slice_evidence =
            repair.then(|| self.in_stream_slice_evidence_paths_for_set(job_id, set_id));
        let repair_placement_overrides =
            repair.then(|| self.par2_filesystem_placement_overrides(job_id, set_id, &working_dir));

        if repair {
            // Retire only files the repairing set can write. Other parsed
            // sets may still have byte-exact evidence for their own files.
            let files: Vec<_> = self
                .jobs
                .get(&job_id)
                .map(|state| state.assembly.files().map(|file| file.file_id()).collect())
                .unwrap_or_default();
            for file_id in files {
                if self
                    .resolve_par2_file_binding(file_id)
                    .is_some_and(|binding| binding.recovery_set_id == set_id)
                {
                    self.block_crcs.forget_file(file_id);
                }
            }
        }

        let memory_limit = configured_par2_repair_memory_limit_bytes();
        let phase_counters = repair.then(|| self.phase_begin(job_id, JobPhase::Repairing, None));
        let session_progress = phase_counters.as_ref().map(|counters| {
            let counters = Arc::clone(counters);
            Arc::new(move |update: par2_rs::ProgressUpdate| {
                if !matches!(
                    update.stage,
                    par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
                ) {
                    return;
                }
                counters
                    .completed_bytes
                    .fetch_max(update.bytes_processed, Ordering::Relaxed);
                if let Some(total_bytes) = update.total_bytes {
                    counters
                        .total_bytes
                        .fetch_max(total_bytes, Ordering::Relaxed);
                }
            }) as par2_rs::ProgressCallback
        });

        let retained_session = match self
            .take_or_open_par2_repair_session(
                job_id,
                set_id,
                working_dir.clone(),
                memory_limit,
                session_progress.clone(),
                // By this point the repairer reads and writes real files: any
                // set still routing here materialized before it arrived.
                None,
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable; using one-shot repairer");
                None
            }
        };
        if let Some((session, newly_opened)) = retained_session {
            if newly_opened {
                #[cfg(test)]
                {
                    self.par2_session_opens += 1;
                }
                self.ensure_par2_runtime(job_id)
                    .set_runtime_mut(set_id)
                    .expect("PAR2 session evidence belongs to the active recovery set")
                    .session_evidence_file_ids
                    .clear();
            }
            let candidates = match self
                .par2_session_evidence_candidates(job_id, set_id, &par2_set)
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    self.restore_par2_repair_session(job_id, set_id, session);
                    if repair {
                        self.phase_end(job_id, JobPhase::Repairing);
                    }
                    let outcome = Err(error);
                    self.settle_direct_unpack_after_repair(job_id, repair, &outcome);
                    return outcome;
                }
            };
            // Analysis reads the live grid here. Repair uses the local
            // pre-retirement snapshot captured above; it never puts those
            // verdicts back after the file generation changes.
            let slice_evidence = repair_slice_evidence
                .unwrap_or_else(|| self.in_stream_slice_evidence_paths_for_set(job_id, set_id));
            let bounded_repair = repair.then(|| {
                let evidence = slice_evidence
                    .iter()
                    .flat_map(|(_, evidence)| evidence.iter().copied())
                    .collect::<Vec<_>>();
                (repair_placement_overrides.unwrap_or_default(), evidence)
            });
            let mut repair_task = tokio::task::spawn_blocking(move || {
                if repair {
                    crate::e2e_failpoint::maybe_delay("repair.task_start");
                }
                run_retained_par2_session(session, candidates, slice_evidence, repair)
            });
            let repair_result = if repair {
                loop {
                    tokio::select! {
                        result = &mut repair_task => break result,
                        _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                            self.sample_phase_progress();
                        }
                    }
                }
            } else {
                repair_task.await
            };
            let retained_outcome = match repair_result {
                Ok((session, Ok((outcome, admitted_file_ids, retried_source_change)))) => {
                    #[cfg(test)]
                    {
                        self.par2_session_source_scan_passes = self
                            .par2_session_source_scan_passes
                            .max(session.diagnostics().source_scan_passes);
                    }
                    self.restore_par2_repair_session(job_id, set_id, session);
                    let set_runtime = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(set_id)
                        .expect("PAR2 session evidence belongs to the active recovery set");
                    if repair || retried_source_change {
                        set_runtime.session_evidence_file_ids.clear();
                        if repair && let Some(session) = set_runtime.session.as_mut() {
                            session.invalidate_all_sources();
                        }
                    } else {
                        set_runtime
                            .session_evidence_file_ids
                            .extend(admitted_file_ids);
                    }
                    ensure_par2_repair_completed(&outcome, repair).map(|()| outcome)
                }
                Ok((session, Err(error))) if repair && error.file_descriptor_exhausted => {
                    let assessment = session
                        .assessment()
                        .ok()
                        .map(|outcome| outcome.verification.clone());
                    // The failed constructor drops every handle it opened.
                    // Do not retain this path-backed session: the retry reads
                    // through PlacementFileAccess and its assessment belongs
                    // to a different source kind.
                    drop(session);
                    let set_runtime = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(set_id)
                        .expect("PAR2 fallback belongs to the active recovery set");
                    set_runtime.session = None;
                    set_runtime.session_last_used = None;
                    set_runtime.session_evidence_file_ids.clear();

                    let (placement_overrides, evidence) = bounded_repair
                        .expect("a repairing retained session has bounded retry inputs");
                    if !assessment.as_ref().is_some_and(|verification| {
                        bounded_repair_evidence_covers_assessment(verification, &evidence)
                    }) {
                        warn!(
                            job_id = job_id.0,
                            error = %error.message,
                            evidence_slices = evidence.len(),
                            "filesystem PAR2 repair exhausted file descriptors; the bounded retry lacks a complete source map"
                        );
                        self.phase_end(job_id, JobPhase::Repairing);
                        let outcome = Err(error.message);
                        self.settle_direct_unpack_after_repair(job_id, repair, &outcome);
                        return outcome;
                    }
                    warn!(
                        job_id = job_id.0,
                        error = %error.message,
                        evidence_slices = evidence.len(),
                        "filesystem PAR2 repair exhausted file descriptors; retrying with bounded source access"
                    );
                    let cancellation = self.par2_cancellation_token(job_id);
                    let fallback_working_dir = working_dir.clone();
                    let fallback_set = (*par2_set).clone();
                    let fallback_progress = session_progress.clone();
                    let mut fallback_task = tokio::task::spawn_blocking(move || {
                        run_file_descriptor_bounded_par2_repair(
                            fallback_working_dir,
                            fallback_set,
                            placement_overrides,
                            evidence,
                            memory_limit,
                            cancellation,
                            fallback_progress,
                        )
                    });
                    let fallback_result = loop {
                        tokio::select! {
                            result = &mut fallback_task => break result,
                            _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                                self.sample_phase_progress();
                            }
                        }
                    };
                    match fallback_result {
                        Ok(result) => result,
                        Err(error) => Err(format!(
                            "bounded filesystem PAR2 fallback task panicked: {error}"
                        )),
                    }
                }
                Ok((session, Err(error))) => {
                    self.restore_par2_repair_session(job_id, set_id, session);
                    Err(error.message)
                }
                Err(error) => Err(format!("retained PAR2 session task panicked: {error}")),
            };
            if repair {
                self.phase_end(job_id, JobPhase::Repairing);
            }
            self.settle_direct_unpack_after_repair(job_id, repair, &retained_outcome);
            return retained_outcome;
        }

        let cancellation = self.par2_cancellation_token(job_id);
        // Files of this job that provably belong to something else. See
        // `par2_extra_scan_exclusions`; the retained-session arm above gets the
        // same list through `take_or_open_par2_repair_session`.
        let exclude_paths = self.par2_extra_scan_exclusions(job_id, set_id);
        // The carry the previous pass over this set left behind — a repairer
        // analysis, a repair, or this module's own authoritative verification.
        // Seeding it is what keeps analysis → repair (and host verify →
        // analysis) from re-reading bytes the earlier pass already hashed;
        // par2-rs stat-gates the carry and re-checks bytes before mutating,
        // so a stale one degrades to the full scan this call always did.
        //
        // Seeded only when the exclusion list has not moved since the carry was
        // taken: a carry is a complete account of the tree only for a pass that
        // was allowed to look at the same files, and its locations can name a
        // file this pass has just been told belongs to something else.
        let carry_set_id = par2_set.recovery_set_id;
        let carry_exclusions = exclude_paths.clone();
        let seeded_scan_carry = self
            .ensure_par2_runtime(job_id)
            .set_runtime_mut(carry_set_id)
            .filter(|set_runtime| set_runtime.scan_carry_exclusions == carry_exclusions)
            .and_then(|set_runtime| set_runtime.scan_carry.clone());
        #[cfg(test)]
        if seeded_scan_carry.is_some() {
            self.par2_scan_carry_seeded_calls += 1;
        }
        let mut repair_task = tokio::task::spawn_blocking(move || {
            if repair {
                crate::e2e_failpoint::maybe_delay("repair.task_start");
            }
            let mut options = par2_rs::Par2RepairerOptions::new(working_dir, Vec::new());
            options.file_set = Some((*par2_set).clone());
            options.repair = repair;
            options.memory_limit = Some(memory_limit);
            options.cancel = Some(cancellation);
            options.exclude_paths = exclude_paths;
            options.scan_carry = seeded_scan_carry;
            if let Some(counters) = phase_counters {
                options.progress = Some(Arc::new(move |update: par2_rs::ProgressUpdate| {
                    if !matches!(
                        update.stage,
                        par2_rs::ProgressStage::Repairing | par2_rs::ProgressStage::WritingRepaired
                    ) {
                        return;
                    }
                    counters
                        .completed_bytes
                        .fetch_max(update.bytes_processed, Ordering::Relaxed);
                    if let Some(total_bytes) = update.total_bytes {
                        counters
                            .total_bytes
                            .fetch_max(total_bytes, Ordering::Relaxed);
                    }
                }));
            }
            let repairer = par2_rs::Par2Repairer::new(options);
            let (outcome, scan_carry) = repairer
                .verify_or_repair_carrying()
                .map_err(|e| format!("PAR2 repairer failed: {e}"))?;
            ensure_par2_repair_completed(&outcome, repair)?;
            Ok((outcome, scan_carry))
        });
        let repair_result = if repair {
            loop {
                tokio::select! {
                    result = &mut repair_task => break result,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(100)) => {
                        self.sample_phase_progress();
                    }
                }
            }
        } else {
            repair_task.await
        };

        if repair {
            self.phase_end(job_id, JobPhase::Repairing);
        }

        match repair_result {
            Ok(Ok((outcome, scan_carry))) => {
                // Replaced unconditionally: a pass that produced no carry may
                // have moved bytes, which makes any older stash a lie about
                // the layout on disk.
                #[cfg(test)]
                if scan_carry.is_some() {
                    self.par2_scan_carry_stashed_calls += 1;
                }
                if let Some(set_runtime) = self
                    .ensure_par2_runtime(job_id)
                    .set_runtime_mut(carry_set_id)
                {
                    set_runtime.scan_carry = scan_carry;
                    set_runtime.scan_carry_exclusions = carry_exclusions;
                }
                let outcome = Ok(outcome);
                self.settle_direct_unpack_after_repair(job_id, repair, &outcome);
                outcome
            }
            Ok(Err(error)) => {
                let outcome = Err(error);
                self.settle_direct_unpack_after_repair(job_id, repair, &outcome);
                outcome
            }
            Err(error) => {
                let outcome = Err(format!("repair task panicked: {error}"));
                self.settle_direct_unpack_after_repair(job_id, repair, &outcome);
                outcome
            }
        }
    }

    /// Hold a damaged-path verdict across the wait for targeted recovery.
    ///
    /// Called only where the gate has just decided to park: the analysis has
    /// run, the damage is real, and the one thing standing between this job and
    /// its repair is recovery still on the wire.
    pub(in crate::pipeline) fn park_par2_repair_verdict(
        &mut self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
        blocks_needed: u32,
        damaged: u32,
        verification: &par2_rs::VerificationResult,
    ) {
        if blocks_needed == 0 {
            return;
        }
        let set_id = par2_set.recovery_set_id;
        let pending = PendingPar2Repair {
            recovery_set_id: set_id,
            slice_size: par2_set.slice_size,
            described_file_ids: par2_set.recovery_file_ids.clone(),
            blocks_needed,
            damaged,
            verification: verification.clone(),
        };
        if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set_runtime.pending_repair = Some(pending);
        }
    }

    /// Whether this set is holding a parked repair verdict.
    pub(in crate::pipeline) fn has_pending_par2_repair(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        self.par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(set_id))
            .is_some_and(|set_runtime| set_runtime.pending_repair.is_some())
    }

    /// Drop a parked verdict. Every path that settles, fails, re-analyses or
    /// re-shapes a set goes through here: a verdict outliving the state it
    /// describes would put a stale `pre_repair` in front of the post-repair
    /// read-back, which is the one thing this shortcut must never do.
    pub(in crate::pipeline) fn clear_pending_par2_repair(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) {
        if let Some(set_runtime) = self
            .par2_runtime
            .get_mut(&job_id)
            .and_then(|runtime| runtime.set_runtime_mut(set_id))
        {
            set_runtime.pending_repair = None;
        }
    }

    /// Every set of this job forgets its parked verdict. Used where the change
    /// is job-shaped rather than set-shaped — a direct-store demotion rewrites
    /// which files exist at all.
    pub(in crate::pipeline) fn clear_pending_par2_repairs_for_job(&mut self, job_id: JobId) {
        if let Some(runtime) = self.par2_runtime.get_mut(&job_id) {
            for set_runtime in runtime.sets.values_mut() {
                set_runtime.pending_repair = None;
            }
        }
    }

    /// The parked verdict, if this entry may repair on it instead of analysing
    /// again.
    ///
    /// Three things have to hold. The verdict must still describe the set about
    /// to be repaired — same recovery set, same slice size, same described
    /// files, so a metadata merge that changed the protected file set is
    /// refused. The recovery it waited for must have *landed*: every promoted
    /// PAR2 file complete and nothing promoted still moving, which is the same
    /// drain the analysis arm below waits for. And the merged set must now hold
    /// the blocks the verdict asked for, which is the one number a second
    /// analysis could have told us and the repair reads off the set directly.
    ///
    /// Returned as a borrow; the caller clones the few kilobytes it needs before
    /// `self` is borrowed mutably for the repair, which is nothing against the
    /// whole-file read the verdict replaces.
    pub(super) fn ready_pending_par2_repair(
        &self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
    ) -> Option<&PendingPar2Repair> {
        let pending = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(par2_set.recovery_set_id))
            .and_then(|set_runtime| set_runtime.pending_repair.as_ref())?;
        if pending.recovery_set_id != par2_set.recovery_set_id
            || pending.slice_size != par2_set.slice_size
            || pending.described_file_ids != par2_set.recovery_file_ids
        {
            return None;
        }
        if par2_set.recovery_block_count() < pending.blocks_needed {
            return None;
        }
        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
        if promoted_recovery.incomplete_promoted_par2_files > 0
            || promoted_recovery.has_pending_work()
        {
            return None;
        }
        Some(pending)
    }

    /// This set's damaged-path analysis, run off the pipeline task.
    ///
    /// `Ok(None)` means a ticket is outstanding and the caller must return: the
    /// read is a whole-directory hash of every described file plus a rolling
    /// scan of whatever else the directory holds, and awaiting it inline held
    /// the actor for its entire duration — no other job's articles dispatched,
    /// no decode result processed, no newly submitted NZB even parsed. The
    /// ticket's completion re-enters the completion check, which reaches this
    /// call again and finds the verdict parked.
    ///
    /// The one-time prologue — retiring the parked repair verdict, the status
    /// transition, the verification-started events — runs at submission, not on
    /// the resuming pass, so a job does not announce that it started verifying
    /// twice for one read.
    pub(super) async fn analyze_par2_with_repairer(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
    ) -> Result<Option<par2_rs::Par2RepairOutcome>, String> {
        let set_id = par2_set.recovery_set_id;
        let outcome = match self.take_parked_par2_analysis(job_id, set_id) {
            Some(result) => result?,
            None => {
                if self.par2_analysis_in_flight.contains_key(&job_id) {
                    debug!(
                        job_id = job_id.0,
                        "a PAR2 damaged-path analysis is already in flight for this job; waiting"
                    );
                    return Ok(None);
                }
                // A fresh read supersedes whatever was parked: from here the
                // gate's verdict is this pass's, and the old one must not be
                // able to reach a repair behind it.
                self.clear_pending_par2_repair(job_id, set_id);
                if !preserve_repairing_status {
                    self.transition_postprocessing_status(
                        job_id,
                        JobStatus::Verifying,
                        Some("verifying"),
                    );
                } else {
                    info!(
                        job_id = job_id.0,
                        "rerunning PAR2 analysis while preserving restored repair slot"
                    );
                }
                self.emit_job_verification_started(job_id);
                let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                });
                self.submit_par2_analysis_ticket(job_id, par2_set, working_dir)
                    .await?;
                return Ok(None);
            }
        };
        let mut outcome = outcome;

        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, &mut outcome.verification);
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
        // The same forgiveness the verify path applies. This
        // pass is reachable with a finalized direct set whenever `par2_verified`
        // was cleared underneath it — an extension asking for PAR re-entry does
        // exactly that — and a finalized set's source volumes are absent by
        // design.
        let forgiven_direct_blocks =
            self.forgive_finalized_direct_volumes(job_id, &mut outcome.verification);
        if forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks, "excluded finalized direct-store volumes from damage count"
            );
        }

        outcome.missing_blocks = outcome.verification.total_missing_blocks;
        self.recompute_volume_safety_from_verification(job_id, &outcome.verification);
        self.record_par2_set_verification_observation(job_id, &outcome.verification);
        let _ = self.event_tx.send(PipelineEvent::JobVerificationComplete {
            job_id,
            passed: !par2_verification_needs_repair(&outcome.verification),
        });

        Ok(Some(outcome))
    }

    /// The verdict a ticket left behind, if it belongs to the set this pass is
    /// deciding.
    ///
    /// A result tagged for another set is put back rather than read as this
    /// set's own: the gate serves one recovery set at a time, and a verdict
    /// names files by path against the set that produced it.
    pub(super) fn take_parked_par2_analysis(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> Option<Result<par2_rs::Par2RepairOutcome, String>> {
        let (parked_set_id, result) = self.par2_analysis_results.remove(&job_id)?;
        if parked_set_id == set_id {
            return Some(result);
        }
        self.par2_analysis_results
            .insert(job_id, (parked_set_id, result));
        None
    }

    /// Snapshot everything this set's analysis needs and hand it to a blocking
    /// worker.
    ///
    /// Returns once the ticket is running. `Err` is reserved for a failure that
    /// happened *here*, on the actor, before any read started — the ticket does
    /// not exist in that case and the caller owns the failure.
    pub(super) async fn submit_par2_analysis_ticket(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
    ) -> Result<(), String> {
        let set_id = par2_set.recovery_set_id;
        #[cfg(test)]
        {
            self.par2_repairer_analyze_calls += 1;
        }
        let plan = self
            .prepare_par2_analysis_work(job_id, par2_set, working_dir)
            .await?;

        self.next_par2_analysis_work_id = self.next_par2_analysis_work_id.wrapping_add(1);
        let work_id = self.next_par2_analysis_work_id;
        self.par2_analysis_in_flight.insert(
            job_id,
            Par2AnalysisWork {
                work_id,
                recovery_set_id: set_id,
                submitted_at: Instant::now(),
            },
        );
        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(
            job_id = job_id.0,
            work_id,
            retained_session = matches!(plan, Par2AnalysisWorkPlan::Retained { .. }),
            "par2 damaged-path analysis started"
        );

        let done_tx = self.repair_work_done_tx.clone();
        tokio::spawn(async move {
            let joined = tokio::task::spawn_blocking(move || run_par2_analysis_work(plan)).await;
            let outcome = joined.unwrap_or_else(|error| {
                Par2AnalysisTicketOutcome::TaskFailed(format!(
                    "PAR2 damaged-path analysis task panicked: {error}"
                ))
            });
            let _ = done_tx
                .send(RepairWorkDone::Par2(Par2AnalysisWorkDone {
                    job_id,
                    work_id,
                    recovery_set_id: set_id,
                    outcome,
                }))
                .await;
        });
        Ok(())
    }

    /// The actor-side half of starting an analysis: take the retained session,
    /// collect the evidence the read may stand on, and turn it all into an
    /// owned plan the worker can run without touching pipeline state.
    pub(super) async fn prepare_par2_analysis_work(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
    ) -> Result<Par2AnalysisWorkPlan, String> {
        let set_id = par2_set.recovery_set_id;
        let memory_limit = configured_par2_repair_memory_limit_bytes();
        let retained_session = match self
            .take_or_open_par2_repair_session(
                job_id,
                set_id,
                working_dir.clone(),
                memory_limit,
                // Analysis reports no repair progress, so it needs no progress
                // sink; and by this point the repairer reads real files, so any
                // set still routing here materialized before it arrived.
                None,
                None,
            )
            .await
        {
            Ok(session) => session,
            Err(error) => {
                warn!(job_id = job_id.0, error = %error, "retained PAR2 session unavailable; using one-shot repairer");
                None
            }
        };
        if let Some((session, newly_opened)) = retained_session {
            if newly_opened {
                #[cfg(test)]
                {
                    self.par2_session_opens += 1;
                }
                self.ensure_par2_runtime(job_id)
                    .set_runtime_mut(set_id)
                    .expect("PAR2 session evidence belongs to the active recovery set")
                    .session_evidence_file_ids
                    .clear();
            }
            let candidates = match self
                .par2_session_evidence_candidates(job_id, set_id, &par2_set)
                .await
            {
                Ok(candidates) => candidates,
                Err(error) => {
                    // Nothing was read and no ticket exists, so the session
                    // goes straight back where it came from.
                    self.restore_par2_repair_session(job_id, set_id, session);
                    return Err(error);
                }
            };
            let slice_evidence = self.in_stream_slice_evidence_paths_for_set(job_id, set_id);
            return Ok(Par2AnalysisWorkPlan::Retained {
                session: Box::new(session),
                candidates,
                slice_evidence,
            });
        }

        let cancellation = self.par2_cancellation_token(job_id);
        let exclude_paths = self.par2_extra_scan_exclusions(job_id, set_id);
        // Seeded only when the exclusion list has not moved since the carry was
        // taken; see the same gate on the one-shot repair path.
        let seeded_scan_carry = self
            .ensure_par2_runtime(job_id)
            .set_runtime_mut(set_id)
            .filter(|set_runtime| set_runtime.scan_carry_exclusions == exclude_paths)
            .and_then(|set_runtime| set_runtime.scan_carry.clone());
        #[cfg(test)]
        if seeded_scan_carry.is_some() {
            self.par2_scan_carry_seeded_calls += 1;
        }
        let mut options = par2_rs::Par2RepairerOptions::new(working_dir, Vec::new());
        options.file_set = Some((*par2_set).clone());
        options.repair = false;
        options.memory_limit = Some(memory_limit);
        options.cancel = Some(cancellation);
        options.exclude_paths = exclude_paths;
        options.scan_carry = seeded_scan_carry;
        Ok(Par2AnalysisWorkPlan::OneShot {
            options: Box::new(options),
        })
    }

    /// A finished analysis ticket, back on the pipeline task.
    ///
    /// Puts the retained session away, applies the bookkeeping the inline call
    /// used to apply on return, parks the verdict for the completion check that
    /// asked for it, and re-enters that check. A ticket the teardown, cancel or
    /// rebind seams already forgot is discarded by the fence.
    pub(in crate::pipeline) async fn handle_par2_analysis_done(
        &mut self,
        done: Par2AnalysisWorkDone,
    ) {
        let Some(in_flight) = self.par2_analysis_in_flight.get(&done.job_id) else {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                "discarding a PAR2 damaged-path analysis whose ticket was forgotten"
            );
            return;
        };
        if in_flight.work_id != done.work_id || in_flight.recovery_set_id != done.recovery_set_id {
            debug!(
                job_id = done.job_id.0,
                work_id = done.work_id,
                "discarding a stale PAR2 damaged-path analysis ticket"
            );
            return;
        }
        let elapsed = in_flight.submitted_at.elapsed();
        self.par2_analysis_in_flight.remove(&done.job_id);
        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        let result =
            self.settle_par2_analysis_ticket(done.job_id, done.recovery_set_id, done.outcome);
        info!(
            job_id = done.job_id.0,
            work_id = done.work_id,
            elapsed_ms = elapsed.as_millis() as u64,
            outcome = if result.is_ok() { "analyzed" } else { "error" },
            "par2 damaged-path analysis ticket completed"
        );
        crate::runtime::perf_probe::record("par2.authoritative.analysis", elapsed);
        if !self.jobs.contains_key(&done.job_id) {
            return;
        }
        self.par2_analysis_results
            .insert(done.job_id, (done.recovery_set_id, result));
        self.schedule_job_completion_check(done.job_id);
    }

    /// Everything the inline analysis did with its raw result before returning
    /// it: put the session back, record what the session may take on trust next
    /// time, stash the carry, and refuse a terminal non-repair status.
    pub(super) fn settle_par2_analysis_ticket(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        outcome: Par2AnalysisTicketOutcome,
    ) -> Result<par2_rs::Par2RepairOutcome, String> {
        let result = match outcome {
            Par2AnalysisTicketOutcome::Retained { session, result } => {
                #[cfg(test)]
                {
                    self.par2_session_source_scan_passes = self
                        .par2_session_source_scan_passes
                        .max(session.diagnostics().source_scan_passes);
                }
                self.restore_par2_repair_session(job_id, set_id, *session);
                match result {
                    Ok((outcome, admitted_file_ids, retried_source_change)) => {
                        let set_runtime = self
                            .ensure_par2_runtime(job_id)
                            .set_runtime_mut(set_id)
                            .expect("PAR2 session evidence belongs to the active recovery set");
                        if retried_source_change {
                            set_runtime.session_evidence_file_ids.clear();
                        } else {
                            set_runtime
                                .session_evidence_file_ids
                                .extend(admitted_file_ids);
                        }
                        ensure_par2_repair_completed(&outcome, false).map(|()| outcome)
                    }
                    // The file-descriptor fallback is a repair-only path: an
                    // analysis opens sources to read them, never the whole
                    // write set at once, so exhaustion here is an ordinary
                    // failure with no bounded retry to fall back to.
                    Err(error) => Err(error.message),
                }
            }
            Par2AnalysisTicketOutcome::OneShot(Ok((outcome, scan_carry))) => {
                #[cfg(test)]
                if scan_carry.is_some() {
                    self.par2_scan_carry_stashed_calls += 1;
                }
                let exclusions = self.par2_extra_scan_exclusions(job_id, set_id);
                if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id)
                {
                    // Replaced unconditionally: a pass that produced no carry
                    // may have moved bytes, which makes any older stash a lie
                    // about the layout on disk.
                    set_runtime.scan_carry = scan_carry;
                    set_runtime.scan_carry_exclusions = exclusions;
                }
                Ok(outcome)
            }
            Par2AnalysisTicketOutcome::OneShot(Err(message))
            | Par2AnalysisTicketOutcome::TaskFailed(message) => Err(message),
        };
        // A no-op for an analysis — nothing was parked under a repair's damage
        // cap — but called on every exit so the settle rule stays one rule.
        self.settle_direct_unpack_after_repair(job_id, false, &result);
        result
    }

    /// Forgets a job's damaged-path analysis ticket and any parked verdict.
    ///
    /// The detached worker keeps running to its end; its done message then
    /// finds no taker and is discarded by the fence, and the retained session
    /// it was carrying is dropped with it. That is the point: a verdict names
    /// files by path, and every caller of this is a seam where those paths, or
    /// the recovery set behind them, stop meaning what the read assumed.
    pub(crate) fn forget_par2_analysis_work(&mut self, job_id: JobId) {
        if self.par2_analysis_in_flight.remove(&job_id).is_some() {
            self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);
        }
        self.par2_analysis_results.remove(&job_id);
    }

    pub(super) async fn verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        preserve_repairing_status: bool,
        emit_events: bool,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
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
            self.emit_job_verification_started(job_id);
            let _ = self.event_tx.send(PipelineEvent::VerificationStarted {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
            });
        }

        let (mut verification, placement_plan) = self
            .run_par2_placement_pass(job_id, par2_set, working_dir, Par2PassScope::WholeSet)
            .await?;
        Self::log_placement_plan(job_id, &placement_plan);
        self.settle_par2_pass_result(job_id, &mut verification, emit_events);
        Ok((verification, placement_plan))
    }

    /// The post-repair authoritative pass, reading only the files the repair
    /// rewrote and standing in for the rest with the pre-repair pass's own
    /// entries.
    ///
    /// The pre-repair pass read every file in this set minutes ago, in this
    /// same flow, and the repair only ever writes the files that pass could not
    /// call complete ([`par2_repair_write_set`]). Re-reading and re-hashing the
    /// files it left alone answers a question that was already answered by
    /// reading the same bytes.
    pub(in crate::pipeline) async fn verify_repaired_par2_files_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        pre_repair: &par2_rs::VerificationResult,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
        let write_set = par2_repair_write_set(pre_repair);
        #[cfg(test)]
        {
            self.par2_post_repair_read_splits.push((
                pre_repair.files.len().saturating_sub(write_set.len()),
                write_set.len(),
            ));
        }
        info!(
            job_id = job_id.0,
            carried = pre_repair.files.len().saturating_sub(write_set.len()),
            rewritten = write_set.len(),
            "post-repair PAR2 verification reads only what the repair rewrote"
        );

        let (fresh, _) = self
            .run_par2_placement_pass(
                job_id,
                Arc::clone(&par2_set),
                working_dir,
                Par2PassScope::Selected(write_set),
            )
            .await?;

        // The carried entries are the pre-repair pass's, verbatim: same status,
        // same filename, same `valid_slices`. Everything downstream —
        // reconciliation, the authoritative digest refresh, placement — has to
        // see exactly what a full pass over unchanged bytes would have
        // reported, and the cheapest way to guarantee that is to hand it the
        // report that pass already made.
        //
        // The residual this accepts, stated rather than hidden: a file the
        // repair did not touch could be corrupted by something outside this job
        // in the minutes between the two passes, and this merge would not catch
        // it. That is the same window, and the same trust class, as the
        // in-stream claims the quick paths already stand a whole verification on
        // — evidence proven by reading the bytes, then relied on after the read.
        // It is unconditional for that reason: a knob would only offer the
        // choice of paying for a re-read that answers a different question than
        // the one this pass is asked.
        let mut verification =
            par2_rs::verify::merge_verification_results(&par2_set, pre_repair, fresh);
        self.settle_par2_pass_result(job_id, &mut verification, false);
        let placement_plan = placement_plan_from_verification(&verification);
        Self::log_placement_plan(job_id, &placement_plan);
        Ok((verification, placement_plan))
    }

    /// Everything an authoritative pass does with its raw result before a
    /// caller may read it: the direct-set damage adjustments, the volume-safety
    /// recomputation and, when the caller is emitting them, the verdict events.
    pub(super) fn settle_par2_pass_result(
        &mut self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
        emit_events: bool,
    ) {
        let adjustments = self.apply_direct_damage_adjustments(job_id, verification);
        if adjustments.skipped_blocks > 0 {
            info!(
                job_id = job_id.0,
                skipped_blocks = adjustments.skipped_blocks,
                "excluded eagerly-deleted CRC-verified volumes from damage count"
            );
        }
        if adjustments.retained_suspect_blocks > 0 {
            info!(
                job_id = job_id.0,
                retained_suspect_blocks = adjustments.retained_suspect_blocks,
                "retained suspect eagerly-deleted volumes in damage count"
            );
        }
        if adjustments.forgiven_direct_blocks > 0 {
            info!(
                job_id = job_id.0,
                forgiven_direct_blocks = adjustments.forgiven_direct_blocks,
                "excluded finalized direct-store volumes from damage count"
            );
        }

        self.recompute_volume_safety_from_verification(job_id, verification);
        self.record_par2_set_verification_observation(job_id, verification);

        if emit_events {
            let passed = !par2_verification_needs_repair(verification);
            let _ = self
                .event_tx
                .send(PipelineEvent::JobVerificationComplete { job_id, passed });
        }
    }

    /// The authoritative PAR2 read, in whichever of its shapes
    /// [`Par2PassScope`] asks for. Returns the raw result and the plan the pass
    /// read through; settling it is the caller's, so the selective shape can
    /// merge first and settle once over the combined set.
    pub(super) async fn run_par2_placement_pass(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        scope: Par2PassScope,
    ) -> Result<(par2_rs::VerificationResult, par2_rs::PlacementPlan), String> {
        #[cfg(test)]
        {
            // Counted by what the pass reads, not by how it resolves names: both
            // whole-set shapes read every described file, only the selective
            // ones read fewer.
            if matches!(
                scope,
                Par2PassScope::Selected(_) | Par2PassScope::SelectedProposed(_, _)
            ) {
                self.par2_selective_verify_calls += 1;
            } else {
                self.par2_authoritative_verify_calls += 1;
            }
        }

        self.metrics.verify_active.fetch_add(1, Ordering::Relaxed);
        info!(job_id = job_id.0, "par2 verification started");

        let pass_set_id = par2_set.recovery_set_id;
        let verify_dir = working_dir.clone();
        let pp_pool = self.pp_pool.clone();
        // A direct set's source volumes are not on disk, so the
        // pass reads them through the hybrid virtual-volume provider. Everything
        // else in the job — the PAR2 volumes, any conventional data file, a
        // demoted set's materialized volumes — keeps reading through
        // `PlacementFileAccess` exactly as before.
        let direct = self.direct_par2_overlay(job_id);
        let verify_result = tokio::task::spawn_blocking(move || {
            pp_pool.install(move || {
                // The whole-set shape PROPOSES placement from 16 KiB prefixes
                // and lets the verify below prove it (see
                // `build_prefix_placement_proposal` for why the library scan's
                // full-file MD5 confirmation was a second whole-set read this
                // pass never needed); the planned-selected shape reads through
                // a proposal its caller already built; the other two assert
                // placement. An empty plan *is* the identity placement for
                // reading, because `PlacementFileAccess::from_plan` takes its
                // overrides from `swaps` and `renames` alone and otherwise
                // resolves a file at the name its description gives it — which
                // is also why a post-repair pass never scans: this job applied
                // the pre-repair plan before repairing, and par2-rs installs
                // every file it rewrote at that file's canonical name.
                let mut plan = match &scope {
                    Par2PassScope::WholeSet => build_prefix_placement_proposal(
                        &verify_dir,
                        &par2_set,
                        None,
                        &HashSet::new(),
                    ),
                    Par2PassScope::SelectedProposed(file_ids, claimed_names) => {
                        let restrict: HashSet<par2_rs::FileId> = file_ids.iter().copied().collect();
                        build_prefix_placement_proposal(
                            &verify_dir,
                            &par2_set,
                            Some(&restrict),
                            claimed_names,
                        )
                    }
                    Par2PassScope::Selected(_) => par2_rs::PlacementPlan {
                        exact: Vec::new(),
                        swaps: Vec::new(),
                        renames: Vec::new(),
                        unresolved: Vec::new(),
                        conflicts: Vec::new(),
                    },
                };

                let Some(direct) = direct else {
                    let file_access = par2_rs::PlacementFileAccess::from_plan(
                        verify_dir.clone(),
                        &par2_set,
                        &plan,
                    );
                    let verification = verify_in_scope(&scope, &par2_set, &file_access);
                    // A damaged whole-set verdict is about to send this job to
                    // the repairer, whose analysis would re-read every byte
                    // this pass just hashed. Hand the pass across the boundary
                    // instead — only when every file sits at its described
                    // name, because that is the layout the carry attests.
                    let host_carry = if matches!(scope, Par2PassScope::WholeSet)
                        && plan.swaps.is_empty()
                        && plan.renames.is_empty()
                        && verification.needs_repair()
                    {
                        build_host_verification_carry(&verify_dir, &par2_set, &verification)
                    } else {
                        None
                    };
                    return Ok((verification, plan, host_carry));
                };

                // The scan walked a directory the direct volumes are absent
                // from, so it left every one of them `unresolved`. Reclassifying
                // them `exact` changes no behaviour today — the plan's only
                // consumers are `PlacementFileAccess::from_plan`,
                // `apply_placement_plan` and the plan log, and all three read
                // `swaps` and `renames` only, never `exact` or `unresolved`. It
                // is kept as defence: a direct volume *is* at its declared name
                // by construction (its identity is resolved by that name, in
                // `direct_par2_overlay`) and has no file to move, so the moment
                // anything does start reading these two lists it must see the
                // classification a correctly placed volume would have had, not
                // the one that invites a rename of a file that is not there.
                let direct_ids: HashSet<par2_rs::FileId> = direct
                    .volumes
                    .iter()
                    .map(|volume| volume.par2_file_id)
                    .collect();
                plan.unresolved
                    .retain(|file_id| !direct_ids.contains(file_id));
                for file_id in &direct.volumes {
                    if !plan.exact.contains(&file_id.par2_file_id) {
                        plan.exact.push(file_id.par2_file_id);
                    }
                }

                let inner = par2_rs::PlacementFileAccess::from_plan(verify_dir, &par2_set, &plan);
                // Taken before the provider is moved into the
                // access: for an encrypted set the pass reads posted bytes the
                // overlay re-derives, and these are the only numbers that say
                // what that cost. Zero for every unencrypted set.
                let cipher = direct.provider.cipher_counters();
                let file_access =
                    crate::pipeline::direct_store::par2_access::DirectVolumeFileAccess::new(
                        inner,
                        direct.provider,
                        &direct.volumes,
                    );
                let counters = file_access.counters();
                let verification = verify_in_scope(&scope, &par2_set, &file_access);
                debug!(
                    virtual_volumes = direct.volumes.len(),
                    sequential_opens = counters.sequential_opens(),
                    // Volumes whose interior holes made the
                    // sequential sweep a lie, so the pass took the per-slice
                    // ranged path instead. Non-zero means the job paid for an
                    // accurate damage count, which is what a repair is sized
                    // from.
                    sequential_refusals = counters.sequential_refusals(),
                    ranged_reads = counters.ranged_reads(),
                    // The checkpoint bound, in production. `chained_bytes`
                    // is what checkpoint misses cost — bytes re-encrypted only
                    // to reach a ranged read's CBC seed and then discarded — and
                    // `seeded_from_start` counts the reads that had no reachable
                    // checkpoint at all. Both large against `reencrypted_bytes`
                    // means the checkpoint stride is too wide for this shape.
                    reencrypted_bytes = cipher.reencrypted_bytes(),
                    chained_bytes = cipher.chained_bytes(),
                    seeded_from_checkpoint = cipher.seeded_from_checkpoint(),
                    seeded_from_start = cipher.seeded_from_start(),
                    // A read the overlay would not answer: unreproducible posted
                    // bytes, which the pass sees as damage.
                    cipher_refusals = cipher.refusals(),
                    "authoritative PAR2 pass read a direct set's volumes virtually"
                );
                // No host carry from the virtual pass: its volumes are not on
                // disk, so there is nothing a stat fingerprint could attest.
                Ok((verification, plan, None))
            })
        })
        .await;

        self.metrics.verify_active.fetch_sub(1, Ordering::Relaxed);

        match verify_result {
            Ok(Ok((verification, plan, host_carry))) => {
                if let Some(carry) = host_carry {
                    #[cfg(test)]
                    {
                        self.par2_host_carry_builds += 1;
                    }
                    // This carry was built from a verification of the set's own
                    // described files, so it holds no extra-candidate location
                    // at all and is compatible with any exclusion list. It is
                    // still stamped with the current one, because the seeding
                    // gate compares lists rather than reasoning about what a
                    // carry happens to contain.
                    let exclusions = self.par2_extra_scan_exclusions(job_id, pass_set_id);
                    if let Some(set_runtime) = self
                        .ensure_par2_runtime(job_id)
                        .set_runtime_mut(pass_set_id)
                    {
                        set_runtime.scan_carry = Some(carry);
                        set_runtime.scan_carry_exclusions = exclusions;
                    }
                }
                Ok((verification, plan))
            }
            Ok(Err(message)) => Err(message),
            Err(error) => Err(format!("verification task panicked: {error}")),
        }
    }

    /// What [`Pipeline::apply_direct_damage_adjustments`] moved, so each caller
    /// can log it in its own voice.
    ///
    /// Counts rather than a bool: "how many blocks were forgiven" is the number
    /// the operator needs to tell a job that was never damaged from one whose
    /// damage was excused.
    pub(crate) fn apply_direct_damage_adjustments(
        &self,
        job_id: JobId,
        verification: &mut par2_rs::VerificationResult,
    ) -> DamageAdjustments {
        let (skipped_blocks, retained_suspect_blocks) =
            self.apply_eager_delete_exclusions(job_id, verification);
        // A *finalized* direct set's source volumes were never written and never
        // will be: its partials are at their destinations and its envelopes are
        // gone, and the whole-member CRC32 gates plus this job's own earlier
        // PAR2 verdict are what let it commit in the first place. Every later
        // pass — and one conventional set failing extraction after the direct
        // set finalized is enough to cause one — would otherwise report those
        // volumes missing and either fail the job as unrepairable or have the
        // repairer write source volumes the job already finished without. Same
        // justification, same shape and the same position in the pass as
        // `apply_eager_delete_exclusions` above.
        let forgiven_direct_blocks = self.forgive_finalized_direct_volumes(job_id, verification);
        DamageAdjustments {
            skipped_blocks,
            retained_suspect_blocks,
            forgiven_direct_blocks,
        }
    }

    pub(in crate::pipeline) fn par2_servable_set_ids(
        &self,
        job_id: JobId,
    ) -> Vec<par2_rs::RecoverySetId> {
        self.par2_runtime(job_id)
            .map(|runtime| {
                runtime
                    .ordered_set_ids()
                    .into_iter()
                    .filter(|set_id| {
                        // A parsed set necessarily carries its descriptions;
                        // a set known only by sighting has no parsed set.
                        runtime
                            .set_runtime(*set_id)
                            .is_some_and(|set_runtime| set_runtime.set.is_some())
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Discovery closes when no bounded collection bootstrap remains. Sibling
    /// recovery volumes stay cold after one carrier has supplied usable
    /// metadata, instead of being treated as completion-critical work.
    pub(in crate::pipeline) fn par2_metadata_discovery_closed(&self, job_id: JobId) -> bool {
        let candidates = self.par2_metadata_candidate_indices(job_id);
        if candidates.is_empty() {
            return true;
        }
        if candidates.iter().any(|(file_index, _, _)| {
            self.par2_discovery_state_for_candidate(job_id, *file_index)
                .work_is_queued()
        }) {
            return false;
        }
        self.next_par2_metadata_action(job_id).is_none()
    }

    /// Whether every servable set has reached a final answer and no later
    /// index can add one. A failed set is settled, but not verified.
    pub(in crate::pipeline) fn par2_gate_settlement_complete(&self, job_id: JobId) -> bool {
        let set_ids = self.par2_servable_set_ids(job_id);
        !set_ids.is_empty()
            && self.par2_metadata_discovery_closed(job_id)
            && self.par2_runtime(job_id).is_some_and(|runtime| {
                set_ids.iter().all(|set_id| {
                    runtime
                        .set_runtime(*set_id)
                        .is_some_and(|set_runtime| set_runtime.settled)
                })
            })
    }

    /// Recompute the job-level verification answer from immutable per-set
    /// answers. This is intentionally the sole writer of `par2_verified`: a
    /// newly parsed set can reopen the aggregate without invalidating a verdict
    /// another set has already reached.
    pub(super) fn recompute_par2_verified(&mut self, job_id: JobId) -> bool {
        let set_ids = self.par2_servable_set_ids(job_id);
        let verified = !set_ids.is_empty()
            && self.par2_metadata_discovery_closed(job_id)
            && self.par2_runtime(job_id).is_some_and(|runtime| {
                set_ids.iter().all(|set_id| {
                    runtime.set_runtime(*set_id).is_some_and(|set_runtime| {
                        set_runtime.settled && set_runtime.failure.is_none()
                    })
                })
            });
        if verified {
            self.par2_verified.insert(job_id);
        } else {
            self.par2_verified.remove(&job_id);
        }
        verified
    }

    /// Mark one set settled, reset only that set's re-entry latch, then update
    /// the aggregate.  Direct outputs remain held until every servable set and
    /// metadata discovery have reached a final answer.
    pub(in crate::pipeline) async fn settle_par2_set(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        reason: Par2SetSettlementReason,
    ) -> SetGateOutcome {
        let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) else {
            return SetGateOutcome::Waiting;
        };
        set_runtime.settled = true;
        set_runtime.failure = None;
        set_runtime.alternate_repair = None;
        set_runtime.post_verdict_reconcile_attempts = 0;
        // A settled set owes no repair.
        set_runtime.pending_repair = None;
        self.mark_par2_verified(job_id).await;
        if let Par2SetSettlementReason::Clean {
            slice_size,
            verification_mode,
        } = reason
        {
            log_clean_par2_verification_source(job_id, set_id, slice_size, verification_mode);
            // A chase gated on this set's damage evidence has just been
            // contradicted by a verdict that read the files. Every mode but
            // the strong-decode claim did read them — and that claim cannot
            // reach here while a chase is gated, so the guard is documentary.
            if !matches!(verification_mode, CleanPar2VerificationMode::StrongDecode) {
                self.release_direct_unpack_after_clean_verification(job_id, set_id);
            }
        }
        SetGateOutcome::Settled
    }

    /// Records a set-local failure without aborting its siblings' passes.
    pub(super) fn mark_par2_set_failed(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        message: String,
    ) -> SetGateOutcome {
        let index_filename = self
            .par2_runtime(job_id)
            .and_then(|runtime| runtime.set_runtime(set_id))
            .map(|set_runtime| set_runtime.summary.index_filename.clone())
            .filter(|filename| !filename.is_empty())
            .unwrap_or_else(|| set_id.to_string());
        if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set_runtime.settled = true;
            set_runtime.failure = Some(message.clone());
            set_runtime.alternate_repair = None;
            set_runtime.post_verdict_reconcile_attempts = 0;
            // A failed set owes no repair.
            set_runtime.pending_repair = None;
        }
        self.recompute_par2_verified(job_id);
        self.note_aggregate_par2_verification_result(job_id);
        warn!(
            job_id = job_id.0,
            recovery_set_id = %set_id,
            index_filename = %index_filename,
            error = %message,
            "PAR2 recovery set failed after its own repair ladder"
        );
        SetGateOutcome::Failed(message)
    }

    pub(super) async fn finish_par2_set_failure(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        message: String,
    ) {
        let _ = self.mark_par2_set_failed(job_id, set_id, message);
        self.finish_or_rearm_after_par2_set_failure(job_id);
    }

    pub(super) async fn finish_par2_set_with_alternate(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        message: String,
        reason: crate::pipeline::repair::backend::AlternateRepairReason,
    ) {
        let _ = self.mark_par2_set_failed(job_id, set_id, message);
        if let Some(set) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set.alternate_repair = Some(reason);
        }
        self.finish_or_rearm_after_par2_set_failure(job_id);
    }

    /// Sibling PAR2 sets and explicitly eligible alternate work get their own
    /// attempt. A PAR2-only job retains its immediate terminal failure behavior.
    pub(super) fn finish_or_rearm_after_par2_set_failure(&mut self, job_id: JobId) {
        if let Some(message) = self.aggregate_par2_failure_message(job_id)
            && !self.par3_has_work_after_par2_failure(job_id)
        {
            self.fail_job(job_id, message);
        } else {
            self.schedule_job_completion_check(job_id);
        }
    }

    /// Make the earliest unsettled servable set the compatibility view used by
    /// existing repair helpers.  The selection changes only at a set boundary;
    /// a settled set is never selected again merely because another set arrives.
    pub(super) fn activate_next_par2_gate_set(
        &mut self,
        job_id: JobId,
    ) -> Option<par2_rs::RecoverySetId> {
        let next_set_id = self
            .par2_servable_set_ids(job_id)
            .into_iter()
            .find(|set_id| {
                self.par2_runtime(job_id)
                    .and_then(|runtime| runtime.set_runtime(*set_id))
                    .is_some_and(|set_runtime| !set_runtime.settled)
            });
        if let Some(set_id) = next_set_id
            && let Some(runtime) = self.par2_runtime.get_mut(&job_id)
        {
            runtime.served = Some(set_id);
        }
        next_set_id
    }

    pub(super) fn served_par2_set_needs_reconciliation(&self, job_id: JobId) -> bool {
        self.par2_served_set_id(job_id).is_some_and(|set_id| {
            self.par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| {
                    set_runtime.settled
                        && set_runtime.failure.is_none()
                        && self.settled_verdict_left_only_proven_protected_files(job_id, set_id)
                })
        })
    }

    /// A recovery set with no assembly binding and no bytes at any described
    /// path has nothing this job can verify or repair.  The binding condition
    /// is deliberately conservative: an empty but known assembly file still
    /// takes the ordinary pass, because it may be waiting for recoverable data.
    pub(in crate::pipeline) fn par2_set_is_absent_from_job(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(par2_set) = self.par2_set_for(job_id, set_id) else {
            return false;
        };
        let has_assembly_binding = state.assembly.files().any(|file| {
            self.resolve_par2_file_binding_in_set(file.file_id(), set_id)
                .is_some()
        });
        if has_assembly_binding {
            return false;
        }
        // A split topology can assemble the described output even when none
        // of its individual fragments binds to that description.  Treat that
        // relationship as evidence rather than skipping a recovery pass.
        let described_names = par2_set
            .files
            .values()
            .map(|description| sanitize_download_filename(&description.filename))
            .collect::<HashSet<_>>();
        if state
            .assembly
            .archive_topologies()
            .keys()
            .any(|name| described_names.contains(&sanitize_download_filename(name)))
        {
            return false;
        }
        par2_set.files.values().all(|description| {
            let path = state
                .working_dir
                .join(sanitize_download_filename(&description.filename));
            std::fs::metadata(path)
                .ok()
                .is_none_or(|metadata| metadata.len() == 0)
        })
    }

    pub(super) fn record_par2_set_verification_observation(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return;
        };
        if let Some(set_runtime) = self.ensure_par2_runtime(job_id).set_runtime_mut(set_id) {
            set_runtime.missing_blocks = verification.total_missing_blocks;
            set_runtime.needed_repair |= par2_verification_needs_repair(verification);
        }
    }

    pub(super) fn note_aggregate_par2_verification_result(&mut self, job_id: JobId) {
        if !self.par2_gate_settlement_complete(job_id)
            || self.jobs_with_verification_outcome.contains(&job_id)
        {
            return;
        }
        let (passed, missing_blocks) = self
            .par2_runtime(job_id)
            .map(|runtime| {
                self.par2_servable_set_ids(job_id).into_iter().fold(
                    (true, 0u32),
                    |(passed, missing_blocks), set_id| {
                        let set_runtime = runtime
                            .set_runtime(set_id)
                            .expect("servable recovery set remains in its runtime");
                        (
                            passed && !set_runtime.needed_repair && set_runtime.failure.is_none(),
                            missing_blocks.saturating_add(set_runtime.missing_blocks),
                        )
                    },
                )
            })
            .unwrap_or((true, 0));
        self.note_job_verification_result(job_id, passed, missing_blocks);
    }

    /// Records the aggregate verdict and releases direct outputs exactly when
    /// every servable set has verified. A per-set repair must not commit
    /// neighbouring set B before B has had its own opportunity to verify or
    /// repair.
    pub(in crate::pipeline) async fn mark_par2_verified(&mut self, job_id: JobId) {
        let was_verified = self.par2_verified.contains(&job_id);
        if !self.recompute_par2_verified(job_id) {
            return;
        }
        self.note_aggregate_par2_verification_result(job_id);
        if !was_verified {
            self.finalize_ready_direct_sets(job_id).await;
            // The aggregate has just settled, so every set that was going to
            // rewrite this directory has done so and the leftovers can be named
            // by difference. This is the only place that is true: a job whose
            // last set settled *clean* never re-enters the repair tail, so
            // purging only from there leaves the earlier sets' backups on disk.
            // Ordered after direct finalization so a set that renames its
            // partials into place is already wearing its final names when the
            // keep-set is built from the assembly.
            self.purge_par2_repair_leftovers(job_id);
        }
    }

    pub(in crate::pipeline) fn aggregate_par2_failure_message(
        &self,
        job_id: JobId,
    ) -> Option<String> {
        if !self.par2_metadata_discovery_closed(job_id) {
            return None;
        }
        let runtime = self.par2_runtime(job_id)?;
        let set_ids = runtime.ordered_set_ids();
        if set_ids.is_empty() {
            return (!self.par2_metadata_candidate_indices(job_id).is_empty()
                && !self.par3_verifies_all_payloads(job_id))
            .then(|| {
                "PAR2 metadata discovery exhausted without finding a recovery set".to_string()
            });
        }
        if set_ids.iter().any(|set_id| {
            runtime
                .set_runtime(*set_id)
                .is_some_and(|set_runtime| set_runtime.set.is_some() && !set_runtime.settled)
        }) {
            return None;
        }
        let failures = set_ids
            .into_iter()
            .filter_map(|set_id| {
                let set_runtime = runtime.set_runtime(set_id)?;
                let index = if set_runtime.summary.index_filename.is_empty() {
                    set_id.to_string()
                } else {
                    set_runtime.summary.index_filename.clone()
                };
                if set_runtime.set.is_none() {
                    return Some(format!(
                        "{index}: metadata discovery exhausted before the recovery set could be parsed"
                    ));
                }
                let failure = set_runtime.failure.as_ref()?;
                Some(format!(
                    "{index} ({}): {failure}",
                    set_runtime.summary.described_filenames.join(", ")
                ))
            })
            .collect::<Vec<_>>();
        (!failures.is_empty()).then(|| {
            format!(
                "PAR2 recovery failed for {} set(s): {}",
                failures.len(),
                failures.join("; ")
            )
        })
    }

    pub(in super::super) fn emit_job_verification_started(&mut self, job_id: JobId) {
        // Low-frequency: a job enters PAR2 verification a handful of times, so
        // arming the stage timer here costs one clock read per pass and never
        // touches an article path.
        self.note_stage_started(
            job_id,
            crate::operations::instrumentation::JobStageKind::Verify,
        );
        let _ = self
            .event_tx
            .send(PipelineEvent::JobVerificationStarted { job_id });
    }

    /// Fold one job-level PAR2 verification verdict into the lifecycle metrics
    /// and close the verify stage timer.
    ///
    /// Low-frequency: one call per verification pass, never per segment. The
    /// four-way label is derived from what the pass actually produced — a pass
    /// that needs repair and found nothing at all on disk is `missing`, one
    /// that needs repair with blocks present is `damaged`.
    pub(in super::super) fn note_job_verification_result(
        &mut self,
        job_id: JobId,
        passed: bool,
        missing_blocks: u32,
    ) {
        use crate::operations::instrumentation::{JobStageKind, VerificationOutcomeKind};
        let outcome = if passed {
            VerificationOutcomeKind::Intact
        } else if missing_blocks > 0 {
            VerificationOutcomeKind::Missing
        } else {
            VerificationOutcomeKind::Damaged
        };
        // Claim the job before recording, so a later `unverifiable` fallback
        // cannot add a second row for a job an actual pass already ruled on.
        // Re-verification of the same job (verify, repair, verify again) is a
        // real second outcome and still counts, which is why the claim gates
        // only the fallback and not this.
        self.jobs_with_verification_outcome.insert(job_id);
        self.metrics.job_lifecycle.note_verification(outcome);
        self.note_stage_finished(job_id, JobStageKind::Verify);
    }

    /// Record that this job ended with no PAR2 verdict to be had.
    ///
    /// A job with no recovery set can never produce `intact`, `damaged` or
    /// `missing`: there is nothing to verify the payload against. Without
    /// this, such jobs contribute nothing at all to
    /// `weaver_verifications_total`, and the ratio of verified to unverified
    /// downloads — the thing an operator actually wants from that series — is
    /// unanswerable.
    ///
    /// Low-frequency: at most one call per job, at the terminal transition.
    /// The guard set is the same per-job set a real verdict claims, so the two
    /// can never both fire for one job.
    pub(in crate::pipeline) fn note_job_verification_unavailable(&mut self, job_id: JobId) {
        use crate::operations::instrumentation::VerificationOutcomeKind;
        if self.jobs_with_verification_outcome.insert(job_id) {
            self.metrics
                .job_lifecycle
                .note_verification(VerificationOutcomeKind::Unverifiable);
        }
    }

    /// Called at the two terminal transitions — the final move and job failure
    /// — to attribute a job that never had a recovery set.
    pub(in crate::pipeline) fn note_job_unverifiable_if_no_par2_set(&mut self, job_id: JobId) {
        if self.par2_set(job_id).is_none() {
            self.note_job_verification_unavailable(job_id);
        }
    }
}
