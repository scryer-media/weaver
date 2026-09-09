//! Continuation of the `impl Pipeline` block from `finalize/check.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    pub(super) async fn quick_verify_par2_with_placement(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        _working_dir: std::path::PathBuf,
    ) -> Result<QuickPar2Outcome, String> {
        // A live direct set's source volumes are not files. This pass answers in
        // *placement* terms — it hands back a plan whose swaps and renames move
        // real paths, and a clean verdict from it skips the direct-aware pass
        // entirely — so a volume that exists only as routed member partials and
        // envelopes has no business being decided here. The direct pass
        // (`verify_direct_sets_quietly`) is the one that knows how to stand in
        // for a grid-adjudicated virtual volume and how to read the rest, and it
        // reaches the same zero-I/O conclusion from the same evidence.
        //
        // Before the grid was fed from the direct seam this refusal happened by
        // accident: a direct volume carried neither a block verdict nor a
        // completed-file digest, so the loop below fell through its
        // `no_current_generation_digest` arm. It is stated rather than inherited
        // now that the first of those two is no longer true.
        if self
            .direct_store
            .sets_for(job_id)
            .iter()
            .any(|set| !set.is_demoted() && !set.is_finalized())
        {
            crate::runtime::perf_probe::record(
                "completion.quick_verify.rejected.live_direct_set",
                std::time::Duration::from_nanos(1),
            );
            return Ok(QuickPar2Outcome::Inconclusive);
        }
        let completed_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let runtime_checksums = self
            .par2_runtime(job_id)
            .map(|runtime| runtime.completed_checksums.clone())
            .unwrap_or_default();
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(QuickPar2Outcome::Inconclusive);
        };

        let mut current_hashes_by_name = HashMap::<String, [u8; 16]>::new();
        // Three arms decide a file, strongest first: the dual-CRC grid's
        // per-slice proof, the streamed whole-file CRC32 against the CRC the
        // description's own slice checksums fold to, and the persisted/runtime
        // whole-file MD5. None of them computes anything over the payload —
        // all three read evidence already in hand — and an in-stream `Damaged`
        // verdict vetoes all of them before any runs.
        let mut grid_matches_by_name = HashMap::<String, (par2_rs::FileId, String)>::new();
        let mut file_crc_matches_by_name = HashMap::<String, (par2_rs::FileId, String)>::new();
        // Built on first use: a set whose every file the grid already covered
        // never pays for the fold.
        let mut padded_file_crc_lookup: Option<PaddedFileCrcLookup> = None;
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let file_id = file.file_id();
            // The file's measured digest, by generation: a runtime checksum
            // entry always speaks for the CURRENT generation (`Some` = its
            // digest, `None` = it has none — a CRC-metadata completion or
            // the sentinel a failed finalize records after a duplicate
            // rewrite), and the persisted row stands in only when the
            // runtime holds no entry at all (the restart shape). An older
            // row must never outrank or revive over the current generation.
            let measured_md5 = match runtime_checksums.get(&file_id) {
                Some(current) => current.md5,
                None => completed_hashes.get(&file_id.file_index).copied(),
            };
            // In-stream IFSC verdicts veto every quick arm below. A Damaged
            // block means the dual-CRC grid saw bytes that contradict the
            // recovery set, so any hash that still matches — a stale trusted
            // row, say — is exactly what must not conclude verification here.
            // Conflicted files go to the authoritative pass, which reads the
            // real bytes.
            if self.block_crc_verdicts(file_id).is_some_and(|verdicts| {
                verdicts.values().any(|verdict| {
                    matches!(verdict, crate::pipeline::integrity::BlockVerdict::Damaged)
                })
            }) {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.rejected.damaged_in_stream_verdict",
                    std::time::Duration::from_nanos(1),
                );
                return Ok(QuickPar2Outcome::Inconclusive);
            }
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.as_str())
                .unwrap_or_else(|| file.filename());
            // Clean dual-CRC arm. Metadata-early downloads deliberately
            // stream no MD5 — the article/IFSC grids carry verification — so
            // a clean file has no digest anywhere and would otherwise fall to
            // the authoritative pass and re-read every byte it just wrote.
            // The grid match demands every described slice Intact with
            // independent coverage at exact length; the Damaged veto above
            // already refused conflicted files before any arm ran.
            if let Some(grid_match) = self.in_stream_verified_par2_match(file_id, &par2_set) {
                // A measured digest outranks the grid. When a trusted MD5
                // exists for this file — streamed, re-read after a duplicate,
                // or verified — and it disagrees with the description the
                // grid selected, the CRC evidence has been contradicted by a
                // stronger instrument and only the authoritative pass may
                // adjudicate. No digest is ever computed for this check; it
                // compares bytes already in hand.
                if let Some(measured) = measured_md5
                    && par2_set
                        .file_description(&grid_match.0)
                        .is_some_and(|description| description.hash_full != measured)
                {
                    crate::runtime::perf_probe::record(
                        "completion.quick_verify.rejected.grid_contradicts_measured_md5",
                        std::time::Duration::from_nanos(1),
                    );
                    return Ok(QuickPar2Outcome::Inconclusive);
                }
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.par2_match.in_stream_grid",
                    std::time::Duration::from_nanos(1),
                );
                grid_matches_by_name.insert(current_filename.to_string(), grid_match);
                continue;
            }
            // Whole-file-CRC arm. The grid proves a file slice by slice and
            // needs every slice; this proves the same file in one comparison
            // and needs none of them, so it picks up exactly what the grid
            // could not cover — a file whose articles all verified their yEnc
            // part CRC but whose bytes never composed onto the block grid.
            // Those files already stopped streaming an MD5, so without this
            // they fall to the authoritative pass and are re-read whole.
            //
            // The streamed CRC32 is a fold of part CRCs in arrival order, so it
            // means what it says only over a gapless, duplicate-free,
            // in-order assembly — the same three conditions the committed
            // evidence path requires before it will call an assembly
            // contiguous.
            if let Some(streamed) = runtime_checksums.get(&file_id).filter(|checksum| {
                checksum.all_parts_crc_verified
                    && !file.has_duplicate_segments()
                    && file.contiguous_placements_proven()
            }) {
                let measured_length = file.received_bytes();
                let slice_count = par2_set.slice_count_for_file(measured_length);
                let lookup = padded_file_crc_lookup
                    .get_or_insert_with(|| par2_padded_file_crc_lookup(&par2_set));
                let padded = pad_measured_file_crc32_to_slice_grid(
                    streamed.crc32,
                    measured_length,
                    u64::from(slice_count),
                    par2_set.slice_size,
                );
                match lookup
                    .get(&(measured_length, padded))
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                {
                    [] => {
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.skipped.file_crc_no_match",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                    [(par2_file_id, correct_name)] => {
                        // A measured digest outranks the CRC, exactly as it
                        // outranks the grid: a trusted MD5 for this generation
                        // that disagrees with the description this arm picked
                        // is a stronger instrument contradicting a weaker one,
                        // and only the authoritative pass may adjudicate that.
                        if let Some(measured) = measured_md5
                            && par2_set
                                .file_description(par2_file_id)
                                .is_some_and(|description| description.hash_full != measured)
                        {
                            crate::runtime::perf_probe::record(
                                "completion.quick_verify.rejected.file_crc_contradicts_measured_md5",
                                std::time::Duration::from_nanos(1),
                            );
                            return Ok(QuickPar2Outcome::Inconclusive);
                        }
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.par2_match.file_crc",
                            std::time::Duration::from_nanos(1),
                        );
                        file_crc_matches_by_name.insert(
                            current_filename.to_string(),
                            (*par2_file_id, correct_name.clone()),
                        );
                        continue;
                    }
                    _ => {
                        // Two descriptions of the same length folding to the
                        // same CRC32 is exactly where a 32-bit binding stops
                        // being a proof. Neither may claim the file.
                        crate::runtime::perf_probe::record(
                            "completion.quick_verify.skipped.file_crc_ambiguous",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                }
            } else {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.skipped.file_crc_unproven_assembly",
                    std::time::Duration::from_nanos(1),
                );
            }
            // `measured_md5` is generation-ordered (runtime first) and the
            // persisted side is provenance-filtered: a row without trusted
            // `md5_provenance` (legacy — possibly a PAR2 expectation
            // recorded by the removed substitution) never loads. An
            // evidence-less file may belong to another recovery set, so it is
            // not itself a reason to reject this set. Any protected file that
            // remains unproved is caught by the unresolved check below.
            let Some(file_hash) = measured_md5 else {
                crate::runtime::perf_probe::record(
                    "completion.quick_verify.skipped.no_current_generation_digest",
                    std::time::Duration::from_nanos(1),
                );
                continue;
            };
            current_hashes_by_name.insert(current_filename.to_string(), file_hash);
        }

        let mut all_file_ids: Vec<par2_rs::FileId> = par2_set
            .recovery_file_ids
            .iter()
            .chain(par2_set.non_recovery_file_ids.iter())
            .copied()
            .collect();
        all_file_ids.sort_unstable_by_key(|file_id| *file_id.as_bytes());
        all_file_ids.dedup();

        let mut hash_lookup = HashMap::<[u8; 16], Vec<(par2_rs::FileId, String)>>::new();
        for file_id in &all_file_ids {
            let Some(desc) = par2_set.file_description(file_id) else {
                continue;
            };
            hash_lookup
                .entry(desc.hash_full)
                .or_default()
                .push((*file_id, sanitize_download_filename(&desc.filename)));
        }

        let mut matches = grid_matches_by_name;
        let had_grid_match = !matches.is_empty();
        let had_file_crc_match = !file_crc_matches_by_name.is_empty();
        matches.extend(file_crc_matches_by_name);
        let mut digest_matched_description = false;
        let mut match_counts = HashMap::<par2_rs::FileId, u32>::new();
        for (file_id, _) in matches.values() {
            *match_counts.entry(*file_id).or_default() += 1;
        }
        for (current_name, file_hash) in current_hashes_by_name {
            let Some(candidates) = hash_lookup.get(&file_hash) else {
                continue;
            };

            if let Some((file_id, correct_name)) = candidates.first() {
                matches.insert(current_name.clone(), (*file_id, correct_name.clone()));
                *match_counts.entry(*file_id).or_default() += 1;
                digest_matched_description = true;
            }
        }
        // Weakest arm that contributed. The `Digest` fallback also owns the
        // "nothing matched at all" shape — a set with no descriptions to match
        // — which claims no zero-read evidence and never did.
        let evidence = if digest_matched_description || !(had_grid_match || had_file_crc_match) {
            QuickPar2Evidence::Digest
        } else if had_file_crc_match {
            QuickPar2Evidence::FileCrc
        } else {
            QuickPar2Evidence::Grid
        };

        let conflict_ids: HashSet<par2_rs::FileId> = match_counts
            .iter()
            .filter(|(_, count)| **count > 1)
            .map(|(file_id, _)| *file_id)
            .collect();
        matches.retain(|_, (file_id, _)| !conflict_ids.contains(file_id));

        let mut id_to_disk = HashMap::<par2_rs::FileId, String>::new();
        for (disk_name, (file_id, _)) in &matches {
            id_to_disk.insert(*file_id, disk_name.clone());
        }

        let mut files = Vec::new();
        let mut exact = Vec::new();
        let mut swaps = Vec::new();
        let mut renames = Vec::new();
        let mut unresolved = Vec::new();
        let mut seen_swap = HashSet::<par2_rs::FileId>::new();
        for file_id in all_file_ids.iter().copied() {
            let Some(desc) = par2_set.file_description(&file_id).cloned() else {
                continue;
            };
            let correct_filename = sanitize_download_filename(&desc.filename);

            if conflict_ids.contains(&file_id) {
                continue;
            }

            let Some(disk_name) = id_to_disk.get(&file_id).cloned() else {
                unresolved.push(file_id);
                continue;
            };

            if disk_name == correct_filename {
                exact.push(file_id);
            } else if !seen_swap.contains(&file_id) {
                let other_file_id = matches.get(correct_filename.as_str()).map(|(id, _)| *id);
                if let Some(other_id) = other_file_id
                    && other_id != file_id
                    && id_to_disk
                        .get(&other_id)
                        .is_some_and(|name| name == &correct_filename)
                {
                    let Some(other_desc) = par2_set.file_description(&other_id) else {
                        return Ok(QuickPar2Outcome::Inconclusive);
                    };
                    let other_correct_filename = sanitize_download_filename(&other_desc.filename);
                    swaps.push((
                        par2_rs::PlacementEntry {
                            file_id,
                            current_name: disk_name.clone(),
                            correct_name: correct_filename.clone(),
                        },
                        par2_rs::PlacementEntry {
                            file_id: other_id,
                            current_name: correct_filename.clone(),
                            correct_name: other_correct_filename,
                        },
                    ));
                    seen_swap.insert(file_id);
                    seen_swap.insert(other_id);
                } else {
                    renames.push(par2_rs::PlacementEntry {
                        file_id,
                        current_name: disk_name.clone(),
                        correct_name: correct_filename.clone(),
                    });
                }
            }

            let slice_count = par2_set.slice_count_for_file(desc.length) as usize;
            files.push(par2_rs::verify::FileVerification {
                file_id,
                filename: correct_filename,
                status: par2_rs::verify::FileStatus::Complete,
                valid_slices: vec![true; slice_count],
                missing_slice_count: 0,
            });
        }

        if !conflict_ids.is_empty() {
            // Two disk files matched one description: the evidence is
            // internally contradictory, and standing ANY of it in would build
            // a verdict on an identification this pass could not make. The
            // authoritative pass reads everything, which is the correct price
            // for ambiguity.
            return Ok(QuickPar2Outcome::Inconclusive);
        }

        if !unresolved.is_empty() {
            // Unproven is not distrusted. Every entry in `files` carries a
            // zero-read proof that stands on its own; only the remainder needs
            // a read, and throwing the proven entries away here is what used
            // to turn one evidence-less file into a whole-set re-read.
            let recovery_ids: HashSet<par2_rs::FileId> =
                par2_set.recovery_file_ids.iter().copied().collect();
            let unproven_recovery: Vec<par2_rs::FileId> = unresolved
                .iter()
                .copied()
                .filter(|file_id| recovery_ids.contains(file_id))
                .collect();
            if files.is_empty() {
                return Ok(QuickPar2Outcome::Inconclusive);
            }
            #[cfg(test)]
            {
                self.par2_quick_partial_verify_calls += 1;
            }
            let claimed_disk_names: HashSet<String> = matches.keys().cloned().collect();
            return Ok(QuickPar2Outcome::Partial(QuickPar2PartialEvidence {
                proven: files,
                proven_plan: par2_rs::PlacementPlan {
                    exact,
                    swaps,
                    renames,
                    unresolved: Vec::new(),
                    conflicts: Vec::new(),
                },
                unproven_recovery,
                claimed_disk_names,
            }));
        }

        #[cfg(test)]
        {
            self.par2_quick_verify_calls += 1;
        }

        Ok(QuickPar2Outcome::Full(
            par2_rs::VerificationResult {
                files,
                recovery_blocks_available: par2_set.recovery_block_count(),
                total_missing_blocks: 0,
                repairable: par2_rs::verify::Repairability::NotNeeded,
            },
            par2_rs::PlacementPlan {
                exact,
                swaps,
                renames,
                unresolved,
                conflicts: conflict_ids.into_iter().collect(),
            },
            evidence,
        ))
    }

    /// Test-only entry onto [`Self::quick_verify_par2_with_placement`].
    ///
    /// The quick pass is private to this module, but its `Some`/`None` verdict
    /// and the placement plan it hands back are exactly what a diagnostic for a
    /// misplaced-payload shape needs to read first-hand, rather than inferring
    /// them from the completion gate's downstream effects. Compiled only under
    /// test, so it adds nothing to the shipped path.
    #[cfg(test)]
    pub(in crate::pipeline) async fn quick_verify_par2_with_placement_for_test(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
    ) -> Result<
        Option<(
            par2_rs::VerificationResult,
            par2_rs::PlacementPlan,
            QuickPar2Evidence,
        )>,
        String,
    > {
        Ok(
            match self
                .quick_verify_par2_with_placement(job_id, par2_set, working_dir)
                .await?
            {
                QuickPar2Outcome::Full(verification, plan, evidence) => {
                    Some((verification, plan, evidence))
                }
                QuickPar2Outcome::Partial(_) | QuickPar2Outcome::Inconclusive => None,
            },
        )
    }

    /// Shared completion handling for a clean PAR2 verdict.
    ///
    /// Every fast path that proves a job clean without the authoritative pass
    /// funnels through here, so their downstream effects — placement, identity,
    /// reconciliation, `par2_verified`, status transitions — are the same code,
    /// not parallel copies that can drift.
    pub(super) async fn finish_clean_par2_verification(
        &mut self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        working_dir: std::path::PathBuf,
        outcome: CleanPar2Verification,
        has_crc_failures: bool,
        archive_extraction_applicable: bool,
    ) {
        let CleanPar2Verification {
            verification,
            placement_plan,
            slice_size,
            verification_mode,
            reconcile_context,
            retry_message,
        } = outcome;
        Self::log_placement_plan(job_id, &placement_plan);

        self.try_deobfuscate_files_with_par2(job_id).await;
        if let Err(error) = self
            .apply_placement_plan_for_retry_or_repair(job_id, working_dir, &placement_plan)
            .await
        {
            self.finish_par2_set_failure(job_id, set_id, error).await;
            return;
        }
        self.retry_par2_authoritative_identity(job_id).await;
        // Before refreshing topologies, adopt any RAR volume PAR2 rebuilt that
        // the NZB never carried. 0.7.9 calls this at each of its repair exits;
        // 0.8 funnels them through here, so one call covers them all. Without
        // it a repaired interior volume sits on disk under a name the assembly
        // has never heard of, extraction goes on waiting for it, and the repair
        // that just succeeded changes nothing.
        let registration = match self
            .register_verified_par2_rar_outputs(job_id, &verification)
            .await
        {
            Ok(registration) => registration,
            Err(error) => {
                self.finish_par2_set_failure(job_id, set_id, error).await;
                return;
            }
        };
        // No repair ran on this arm, so nothing was rewritten; a rebuilt volume
        // the NZB never carried still invalidates its set's plan.
        self.refresh_verified_complete_archive_topologies(job_id, &verification, &HashSet::new())
            .await;
        self.invalidate_rar_plans_for_repaired_sets(job_id, registration.set_names);
        if let Err(error) = self
            .reconcile_and_classify_par2_verification(
                job_id,
                &verification,
                has_crc_failures,
                reconcile_context,
            )
            .await
        {
            self.finish_par2_set_failure(job_id, set_id, error).await;
            return;
        }

        let settled = self
            .settle_par2_set(
                job_id,
                set_id,
                Par2SetSettlementReason::Clean {
                    slice_size,
                    verification_mode,
                },
            )
            .await;
        if settled == SetGateOutcome::Settled
            && verification_mode == CleanPar2VerificationMode::Grid
        {
            info!(
                job_id = job_id.0,
                recovery_set_id = %set_id,
                slice_size,
                verdict = "clean",
                verification_read_bytes = 0u64,
                "PAR2 set settled clean from in-stream grid evidence"
            );
        }
        self.continue_after_aggregate_clean_par2_settlement(
            job_id,
            has_crc_failures,
            archive_extraction_applicable,
            retry_message,
        )
        .await;
    }

    /// Run the pre-existing job-level continuation exactly once, after the
    /// final clean set has settled. Earlier sets re-arm the gate instead, so a
    /// one-set job still follows this path in the same completion check.
    pub(super) async fn continue_after_aggregate_clean_par2_settlement(
        &mut self,
        job_id: JobId,
        has_crc_failures: bool,
        archive_extraction_applicable: bool,
        retry_message: &str,
    ) {
        if !self.par2_verified.contains(&job_id) {
            self.schedule_job_completion_check(job_id);
            return;
        }

        if has_crc_failures {
            if self.normalization_retried.contains(&job_id) {
                let msg = "clean PAR2 verification but extraction still failing after retry";
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg.to_string());
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
                let msg =
                    format!("invalid RAR retry frontier after placement correction: {reason}");
                warn!(job_id = job_id.0, error = %msg);
                self.fail_job(job_id, msg);
                return;
            }

            info!(
                job_id = job_id.0,
                cleared, retry_message, "cleared failed extractions after clean PAR2 verification"
            );

            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        if archive_extraction_applicable {
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        self.reconcile_job_progress(job_id).await;
        self.schedule_job_completion_check(job_id);
    }

    /// Bind a PAR2 verification back onto the assembly and promote every file
    /// it vouches for.
    ///
    /// # Identity, not string equality
    ///
    /// Binding runs through [`Self::resolve_par2_file_binding`] — the same
    /// resolver the dual-CRC grid measures its in-stream block verdicts
    /// against — so it inherits the sanitized comparison, the full alias set
    /// (posted, current, source, canonical), the 16 KiB content fallback that
    /// binds an obfuscated post by its bytes, and outright refusal of
    /// ambiguity.
    ///
    /// What this replaces compared *raw* assembly names against descriptions
    /// that had already been sanitized on the way in, so every name that needed
    /// sanitizing silently bound to nothing; it resolved a duplicate alias
    /// first-writer-wins; and it answered with a bare count, which cannot tell
    /// "nothing needed doing" apart from "a repaired, re-verified file bound to
    /// nothing and is still sitting incomplete". The caller needs that
    /// distinction to classify its veto, so the report carries it.
    ///
    /// A name-keyed fallback is kept for the files the resolver declines, but
    /// it is sanitized on both sides and refuses duplicates rather than taking
    /// the first.
    pub(in crate::pipeline) async fn reconcile_verified_par2_files(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> Result<Par2Reconciliation, String> {
        let existing_hashes = self.load_existing_complete_file_hashes(job_id).await?;
        let mut report = Par2Reconciliation::default();
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return Ok(report);
        };

        let files_to_complete: Vec<(NzbFileId, String)> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(report);
            };
            let working_dir = state.working_dir.clone();
            let assembly_file_ids: Vec<NzbFileId> =
                state.assembly.files().map(|file| file.file_id()).collect();

            // Identity map, inverted. A description that two assembly files
            // both answer to is contested and binds to neither; `None` records
            // the contest rather than dropping the entry, so it stays visible
            // instead of degrading into a name match that would guess.
            let mut by_identity = HashMap::<par2_rs::FileId, Option<NzbFileId>>::new();
            for file_id in &assembly_file_ids {
                let Some(binding) = self.resolve_par2_file_binding(*file_id) else {
                    continue;
                };
                if binding.recovery_set_id != par2_set.recovery_set_id {
                    continue;
                }
                by_identity
                    .entry(binding.par2_file_id)
                    .and_modify(|slot| {
                        if *slot != Some(*file_id) {
                            *slot = None;
                        }
                    })
                    .or_insert(Some(*file_id));
            }

            // Sanitized alias map, for the files identity could not bind.
            let mut by_name = HashMap::<String, Option<NzbFileId>>::new();
            for file in state.assembly.files() {
                let file_id = file.file_id();
                let mut aliases = vec![file.filename().to_string()];
                if let Some(identity) = self.effective_file_identity(job_id, file_id) {
                    aliases.push(identity.current_filename.clone());
                    aliases.push(identity.source_filename.clone());
                    if let Some(canonical) = identity.canonical_filename.clone() {
                        aliases.push(canonical);
                    }
                }
                for alias in aliases {
                    let key = sanitize_download_filename(&alias);
                    if key.is_empty() {
                        continue;
                    }
                    match by_name.entry(key) {
                        std::collections::hash_map::Entry::Occupied(mut slot) => {
                            if *slot.get() != Some(file_id) {
                                slot.insert(None);
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(slot) => {
                            slot.insert(Some(file_id));
                        }
                    }
                }
            }

            // The PAR2 files that legitimately have no file on disk: volumes of
            // a direct set that is still routing, which are verified through
            // the set's own access layer and are never written under their own
            // name. A finalized or demoted set has put its bytes at a real
            // path, and every conventional file always had one, so for those
            // absence contradicts the verdict rather than explaining it.
            //
            // Built once per pass: the overlay is derived, not cached.
            let live_virtual_par2_files: HashSet<par2_rs::FileId> = self
                .direct_par2_overlay(job_id)
                .map(|overlay| {
                    par2_set
                        .files
                        .keys()
                        .copied()
                        .filter(|par2_file_id| {
                            overlay.owner_of(par2_file_id).is_some_and(|index| {
                                self.direct_store
                                    .set(job_id, index)
                                    .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();

            let mut matched = HashMap::<NzbFileId, String>::new();
            // Assembly files that more than one description laid claim to. Kept
            // apart from `matched` so a third claimant is refused too, rather
            // than filling the slot the second one vacated.
            let mut contested_file_ids = HashSet::<NzbFileId>::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }

                let bound = match by_identity.get(&file_verification.file_id) {
                    Some(Some(file_id)) => Some(*file_id),
                    // Contested identity. Content cannot break a tie that names
                    // and bytes both answer to, so it is refused, not guessed.
                    Some(None) => {
                        report.contested.push(file_verification.filename.clone());
                        continue;
                    }
                    None => {
                        let mut candidate_names = vec![file_verification.filename.clone()];
                        if let par2_rs::verify::FileStatus::Renamed(path) =
                            &file_verification.status
                            && let Some(filename) = path.file_name()
                        {
                            candidate_names.push(filename.to_string_lossy().to_string());
                        }
                        let mut found = None;
                        for candidate_name in &candidate_names {
                            match by_name.get(&sanitize_download_filename(candidate_name)) {
                                Some(Some(file_id)) => {
                                    found = Some(*file_id);
                                    break;
                                }
                                Some(None) => {
                                    report.contested.push(file_verification.filename.clone());
                                    break;
                                }
                                None => {}
                            }
                        }
                        found
                    }
                };

                let Some(file_id) = bound else {
                    // A verdict vouching for bytes on disk that names no
                    // assembly entry. Harmless when nothing is waiting on it,
                    // so the caller decides: only it knows whether the job
                    // still has incomplete files to answer for.
                    report.unbound.push(file_verification.filename.clone());
                    continue;
                };

                if state
                    .assembly
                    .file(file_id)
                    .is_some_and(|file| file.is_complete())
                {
                    continue;
                }

                // The described length is the only length worth checking here.
                // An NZB's declared total is yEnc-*encoded* — about 1.03x the
                // decoded bytes, and about 1.38x for uuencode — so it can never
                // equal `desc.length` for a real post.
                let Some(described_length) = par2_set
                    .file_description(&file_verification.file_id)
                    .map(|desc| desc.length)
                else {
                    report.unbound.push(file_verification.filename.clone());
                    continue;
                };

                let current_filename = self
                    .current_filename_for_file_id(job_id, file_id)
                    .unwrap_or_else(|| file_verification.filename.clone());
                let canonical_filename = sanitize_download_filename(&file_verification.filename);

                // Confirm the bytes the verdict vouched for are the bytes at
                // the name we are about to call complete — when there is a file
                // there to look at. Placement has already run by the time this
                // is reached, so the canonical name is where a repaired file
                // lives and the current name is the fallback for one the plan
                // left alone.
                //
                // Absence excuses only a live virtual volume. Requiring a file
                // for *every* binding refused every routing direct-store volume
                // PAR2 had just proven; excusing every binding went too far the
                // other way and would call a file complete on the strength of a
                // verdict about bytes that are no longer anywhere. The
                // exemption is therefore exactly as wide as the thing that
                // earns it.
                let installed = [canonical_filename.as_str(), current_filename.as_str()]
                    .into_iter()
                    .filter(|name| !name.is_empty())
                    .find_map(|name| {
                        let path = working_dir.join(name);
                        std::fs::metadata(&path)
                            .ok()
                            .filter(|meta| meta.is_file())
                            .map(|meta| (name.to_string(), path, meta.len()))
                    });
                let verified_filename = match installed {
                    Some((_, path, length)) if length != described_length => {
                        report.length_mismatch.push(format!(
                            "{} (on disk {length} bytes, PAR2 describes {described_length})",
                            path.display()
                        ));
                        continue;
                    }
                    Some((filename, _, _)) => filename,
                    None if live_virtual_par2_files.contains(&file_verification.file_id) => {
                        current_filename.clone()
                    }
                    None => {
                        report.length_mismatch.push(format!(
                            "{canonical_filename} (verdict vouched for bytes that are at neither \
                             the canonical nor the current name, and no live direct set owns them)"
                        ));
                        continue;
                    }
                };

                if contested_file_ids.contains(&file_id) {
                    report.contested.push(file_verification.filename.clone());
                    continue;
                }
                match matched.entry(file_id) {
                    std::collections::hash_map::Entry::Occupied(slot) => {
                        // Two descriptions claiming one assembly file — the
                        // mirror of the `by_identity` contest above, and refused
                        // the same way. Silently keeping the first would call
                        // the file complete under one of two names with no
                        // reason to prefer either.
                        slot.remove();
                        contested_file_ids.insert(file_id);
                        report.contested.push(file_verification.filename.clone());
                    }
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(verified_filename);
                    }
                }
            }

            matched.into_iter().collect()
        };

        if files_to_complete.is_empty() {
            return Ok(report);
        }

        for (file_id, verified_filename) in &files_to_complete {
            let Some(mut identity) = self.effective_file_identity(job_id, *file_id) else {
                continue;
            };
            if identity.current_filename == *verified_filename {
                continue;
            }
            identity.current_filename = verified_filename.clone();
            identity.canonical_filename = Some(verified_filename.clone());
            if let Some(classification) =
                Self::canonical_archive_identity_from_filename(verified_filename)
            {
                identity.classification = Some(classification);
            }
            identity.classification_source = crate::jobs::record::FileIdentitySource::Par2;
            self.set_file_identity(job_id, identity)?;
        }

        {
            let Some(state) = self.jobs.get_mut(&job_id) else {
                return Ok(report);
            };
            for (file_id, _) in &files_to_complete {
                let Some(file) = state.assembly.file_mut(*file_id) else {
                    continue;
                };
                file.mark_complete();
            }
        }

        let complete_entries: Vec<(u32, String, Option<[u8; 16]>)> = files_to_complete
            .iter()
            .map(|(file_id, filename)| {
                crate::runtime::perf_probe::record(
                    "download.file_progress.complete_file_row_covers_restart",
                    std::time::Duration::ZERO,
                );
                (
                    file_id.file_index,
                    filename.clone(),
                    Self::expected_hash_for_verified_file(*file_id, &existing_hashes),
                )
            })
            .collect();
        self.db_blocking(move |db| {
            db.complete_files(
                job_id,
                &complete_entries,
                crate::jobs::persistence::CompletedHashProvenance::Verified,
            )
        })
        .await
        .map_err(|error| format!("failed to persist PAR2-reconciled files: {error}"))?;

        for (file_id, _filename) in &files_to_complete {
            self.pending_file_progress.remove(file_id);
            self.persisted_file_progress.remove(file_id);
            self.file_hash_states.remove(file_id);
            self.expected_file_crcs.remove(file_id);
            self.file_hash_reread_required.remove(file_id);
            self.refresh_archive_state_for_completed_file(job_id, *file_id, true)
                .await;
        }

        report.completed = files_to_complete.len();
        Ok(report)
    }

    /// Classify a job that still has incomplete data files after a PAR2 pass
    /// reconciled — into the failure it actually is, or into no failure at all.
    ///
    /// # Why a bare count was the wrong question
    ///
    /// The veto this replaces compared `complete_data_file_count()` against
    /// `data_file_count()` and failed the job on the difference, so every cause
    /// reported identically: a genuinely undownloadable unprotected file, an
    /// obfuscated name the reconciler could not bind, and a contested alias
    /// were one message. Job 11737 was the middle case wearing the first one's
    /// clothes — a standalone MKV that PAR2 had repaired and re-verified, failed
    /// for an article bitmap that the repair had already made irrelevant.
    ///
    /// # The invariant
    ///
    /// Once PAR2 has repaired and re-verified a protected output, that
    /// verification is authoritative. Missing article state remains diagnostic
    /// history; it cannot independently fail the repaired file.
    ///
    /// # Nothing here fails the job
    ///
    /// The invariant is about the *pass*, not about one file: once a PAR2
    /// verification has succeeded, no article-completeness state may fail the
    /// job — protected or unprotected.
    ///
    /// The concrete case that forced this: a 1.09 GB job whose payload PAR2
    /// repaired and re-verified, failed because a 738 KB `.nfo` — which no
    /// recovery set ever covered — was short a few articles. Health 999. Both
    /// oracles deliver that job. So does the final move, which relocates the
    /// working directory wholesale rather than a completeness-filtered
    /// selection, so the bytes reach the user either way and refusing them buys
    /// nothing.
    ///
    /// What survives is the *distinction*. An unprotected file short of
    /// articles is ordinary Usenet damage: warn, deliver, never fail. A
    /// protected file left incomplete after an authoritative pass is our own
    /// reconciliation failing — the recovery set had a verdict for it either
    /// way — and what to do about that turns on one question: are the verified
    /// bytes still reachable?
    ///
    /// If they are (a real file of the described length, or a volume of a
    /// direct set still routing), the defect is bookkeeping. Warn loudly, keep
    /// the download. If they are not, the verdict is vouching for bytes that
    /// are nowhere, and delivering the job would ship a hole as if it were
    /// verified — so that, and only that, still fails.
    pub(in crate::pipeline) fn classify_incomplete_after_par2(
        &self,
        job_id: JobId,
        reconciliation: &Par2Reconciliation,
        context: &str,
    ) -> Option<Par2IncompleteReport> {
        let state = self.jobs.get(&job_id)?;
        let incomplete: Vec<NzbFileId> = state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        } | weaver_model::files::FileRole::Par3 { .. }
                    )
                    // A part of a split set the verdict already joined is a
                    // spent input, not an outstanding file: its bytes are
                    // inside the output that was vouched for.
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .map(|file| file.file_id())
            .collect();
        if incomplete.is_empty() {
            return None;
        }

        // Every parsed, described set receives its own gate pass.  A binding to
        // any such set is protected, regardless of which set happens to be the
        // compatibility view during this particular re-entry.
        let servable_set_ids = self.par2_servable_set_ids(job_id);
        let (protected, unprotected): (Vec<_>, Vec<_>) =
            incomplete.into_iter().partition(|file_id| {
                self.resolve_par2_file_binding(*file_id)
                    .is_some_and(|binding| servable_set_ids.contains(&binding.recovery_set_id))
            });

        // A set without a posted index cannot receive a pass at all.  Keep that
        // distinct from an ordinary unprotected file in the diagnostic.
        let (unservable_set, unprotected): (Vec<_>, Vec<_>) =
            unprotected.into_iter().partition(|file_id| {
                self.file_is_described_only_by_an_unservable_recovery_set(*file_id)
            });

        // Furniture the recovery set happens to cover. It is delivered as it
        // stands — the verdict arm above already declined to spend a full-set
        // read repairing it — so it never reaches the proven/unproven question
        // that decides whether a protected file can fail a job.
        let ignore_extensions = self.par2_ignore_extensions();
        let (ignorable, protected): (Vec<_>, Vec<_>) = protected.into_iter().partition(|file_id| {
            self.par2_bound_file_is_ignorable(job_id, *file_id, &ignore_extensions)
        });

        let names = |ids: &[NzbFileId]| -> String {
            ids.iter()
                .filter_map(|file_id| self.current_filename_for_file_id(job_id, *file_id))
                .collect::<Vec<_>>()
                .join(", ")
        };

        let (proven, unproven): (Vec<_>, Vec<_>) = protected
            .into_iter()
            .partition(|file_id| self.par2_output_presence_proven(job_id, *file_id));

        let mut parts = Vec::new();
        if !unprotected.is_empty() {
            // Ordinary Usenet damage on a file no recovery set covered. It is
            // delivered as-is, short articles and all, exactly as both oracles
            // deliver it.
            parts.push(format!(
                "delivering {} unprotected file(s) short of articles, unrepairable by design: {}",
                unprotected.len(),
                names(&unprotected)
            ));
        }
        if !unservable_set.is_empty() {
            parts.push(format!(
                "delivering {} file(s) covered only by a recovery set with no posted index: {}",
                unservable_set.len(),
                names(&unservable_set)
            ));
        }
        if !ignorable.is_empty() {
            parts.push(format!(
                "delivering {} damaged ignorable file(s) the recovery set covered: {}",
                ignorable.len(),
                names(&ignorable)
            ));
        }
        let detail = || {
            if reconciliation.has_failures() {
                reconciliation.failure_detail()
            } else {
                "no PAR2 verdict claimed them".to_string()
            }
        };
        if !proven.is_empty() {
            parts.push(format!(
                "BUG: {} PAR2-protected file(s) stayed incomplete after an authoritative \
                 verification vouched for them ({}): {}. The verified bytes are on disk and \
                 are delivered; this is a reconciliation defect, not a download failure",
                proven.len(),
                names(&proven),
                detail()
            ));
        }
        if !unproven.is_empty() {
            parts.push(format!(
                "{} PAR2-protected file(s) stayed incomplete and their verified bytes are \
                 nowhere on disk ({}): {}",
                unproven.len(),
                names(&unproven),
                detail()
            ));
        }
        Some(Par2IncompleteReport {
            message: format!("{context}: {}", parts.join("; ")),
            unproven_protected: unproven.len(),
        })
    }

    /// Incomplete data files the recovery set actually describes.
    ///
    /// The completion gate's question after a PAR2 verdict is not "is every
    /// file whole" but "is anything left that PAR2 could still act on".
    /// Ignorable furniture is not: it is delivered as it stands, so counting it
    /// here would re-arm the gate on a file nothing is going to change.
    pub(in crate::pipeline) fn incomplete_par2_protected_data_file_count(
        &self,
        job_id: JobId,
    ) -> usize {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let ignore_extensions = self.par2_ignore_extensions();
        let servable_set_ids = self.par2_servable_set_ids(job_id);
        state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    )
                    && self
                        .resolve_par2_file_binding(file.file_id())
                        .is_some_and(|binding| servable_set_ids.contains(&binding.recovery_set_id))
                    && !self.par2_bound_file_is_ignorable(
                        job_id,
                        file.file_id(),
                        &ignore_extensions,
                    )
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .count()
    }

    /// Whether a settled verdict has left protected files outstanding whose
    /// verified bytes are demonstrably still on disk.
    ///
    /// The current set's portion of
    /// [`Self::incomplete_par2_protected_data_file_count`], narrowed to an
    /// incomplete file whose verified bytes can be shown to be present at its
    /// described length.
    ///
    /// That narrowing carries the whole distinction. Bytes that are present
    /// under a verdict which already vouched for them mean the download is
    /// sound and our own binding is not, and re-reading the recovery set cannot
    /// change either fact. Bytes that are absent or short mean something really
    /// is missing, which is a question the authoritative pass alone can answer.
    pub(super) fn settled_verdict_left_only_proven_protected_files(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
    ) -> bool {
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let ignore_extensions = self.par2_ignore_extensions();
        let outstanding: Vec<NzbFileId> = state
            .assembly
            .files()
            .filter(|file| {
                !file.is_complete()
                    && !matches!(
                        file.role(),
                        weaver_model::files::FileRole::Par2 {
                            is_index: false,
                            ..
                        }
                    )
                    && self
                        .resolve_par2_file_binding(file.file_id())
                        .is_some_and(|binding| binding.recovery_set_id == set_id)
                    && !self.par2_bound_file_is_ignorable(
                        job_id,
                        file.file_id(),
                        &ignore_extensions,
                    )
                    && !self.par2_join_consumed_split_part(job_id, file.file_id())
            })
            .map(|file| file.file_id())
            .collect();
        !outstanding.is_empty()
            && outstanding
                .into_iter()
                .all(|file_id| self.par2_output_presence_proven(job_id, file_id))
    }

    /// The ignore-extension list in force for this job.
    ///
    /// Process-global configuration, read where it is used rather than cached,
    /// exactly as the repair memory limit is. The test hook exists so the
    /// "override disables it" case can be exercised without mutating a
    /// process-global environment other tests are reading concurrently.
    pub(in crate::pipeline) fn par2_ignore_extensions(&self) -> Vec<String> {
        #[cfg(test)]
        if let Some(extensions) = self.par2_ignore_extensions_override.as_ref() {
            return extensions.clone();
        }
        configured_par2_ignore_extensions()
    }

    /// Whether the file this assembly entry binds to is ignorable furniture,
    /// judged by the name the recovery set describes it under as well as the
    /// name it currently carries.
    pub(super) fn par2_bound_file_is_ignorable(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
        ignore_extensions: &[String],
    ) -> bool {
        if ignore_extensions.is_empty() {
            return false;
        }
        if self
            .current_filename_for_file_id(job_id, file_id)
            .is_some_and(|filename| par2_damage_ignorable(&filename, ignore_extensions))
        {
            return true;
        }
        self.resolve_par2_file_binding(file_id)
            .and_then(|binding| {
                self.par2_set_for(job_id, binding.recovery_set_id)
                    .and_then(|set| set.file_description(&binding.par2_file_id))
                    .map(|description| description.filename.clone())
            })
            .is_some_and(|filename| par2_damage_ignorable(&filename, ignore_extensions))
    }

    /// The damaged and missing descriptions in a verdict, when every one of
    /// them is ignorable furniture.
    ///
    /// `None` means the ordinary repair/fail ladder applies: either the verdict
    /// carries no damage at all, or something that is not furniture is damaged
    /// too. That second case still fails on short recovery, and deliberately —
    /// a payload file's missing slices are unknowns in every equation the solve
    /// has, so the furniture's blocks cannot be excused out of it. Both
    /// reference downloaders draw the line in the same place.
    ///
    /// A `Renamed` verdict is not damage but it is not this function's business
    /// either: placement decides what to do with it, so its presence sends the
    /// verdict down the ordinary path untouched.
    pub(super) fn par2_damage_is_only_ignorable(
        &self,
        verification: &par2_rs::VerificationResult,
    ) -> Option<Vec<String>> {
        let ignore_extensions = self.par2_ignore_extensions();
        if ignore_extensions.is_empty() {
            return None;
        }
        let mut ignorable = Vec::new();
        for file in &verification.files {
            if matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                continue;
            }
            if !matches!(
                file.status,
                par2_rs::verify::FileStatus::Damaged(_) | par2_rs::verify::FileStatus::Missing
            ) {
                return None;
            }
            if !par2_damage_ignorable(&file.filename, &ignore_extensions) {
                return None;
            }
            ignorable.push(file.filename.clone());
        }
        (!ignorable.is_empty()).then_some(ignorable)
    }

    /// Whether the bytes a PAR2 verdict vouched for are still reachable.
    ///
    /// Two ways they can be: a real file of the described length at the
    /// canonical or the current name, or a volume of a direct set that is still
    /// routing — those are verified through the set's own access layer and are
    /// never written under their own name, so having no file is what correct
    /// looks like for them.
    pub(super) fn par2_output_presence_proven(&self, job_id: JobId, file_id: NzbFileId) -> bool {
        let Some(binding) = self.resolve_par2_file_binding(file_id) else {
            return false;
        };
        if self
            .direct_par2_overlay(job_id)
            .and_then(|overlay| overlay.owner_of(&binding.par2_file_id))
            .and_then(|index| self.direct_store.set(job_id, index))
            .is_some_and(|set| !set.is_demoted() && !set.is_finalized())
        {
            return true;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let canonical = self
            .par2_set_for(job_id, binding.recovery_set_id)
            .and_then(|set| set.file_description(&binding.par2_file_id))
            .map(|desc| {
                state
                    .working_dir
                    .join(sanitize_download_filename(&desc.filename))
            });
        canonical
            .into_iter()
            .chain(std::iter::once(binding.path))
            .any(|path| {
                std::fs::metadata(&path)
                    .ok()
                    .filter(|meta| meta.is_file())
                    .is_some_and(|meta| meta.len() == binding.described_length)
            })
    }

    /// Reconcile a PAR2 verification onto the assembly, then decide whether what
    /// is left standing is a failure.
    ///
    /// Every PAR2 exit — the clean fast paths and both repair paths — funnels
    /// through here, so the binding rules and the classification are stated once
    /// instead of once per exit. They had already drifted apart across five
    /// copies; each copy is a place for the next one to drift again.
    pub(in crate::pipeline) async fn reconcile_and_classify_par2_verification(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        has_crc_failures: bool,
        context: &str,
    ) -> Result<(), String> {
        let reconciliation = self
            .reconcile_verified_par2_files(job_id, verification)
            .await?;
        if reconciliation.completed > 0 || reconciliation.has_failures() {
            info!(
                job_id = job_id.0,
                completed = reconciliation.completed,
                unbound = reconciliation.unbound.len(),
                contested = reconciliation.contested.len(),
                length_mismatch = reconciliation.length_mismatch.len(),
                context,
                "PAR2 reconciliation"
            );
        }
        // An extraction retry is still owed a pass, so an incomplete count here
        // is not a verdict yet — the retry is what decides. Reconciliation still
        // had to run first: the retry reads the files this pass just promoted.
        if has_crc_failures {
            return Ok(());
        }
        if let Some(report) = self.classify_incomplete_after_par2(job_id, &reconciliation, context)
        {
            if report.unproven_protected > 0 {
                return Err(report.message);
            }
            warn!(job_id = job_id.0, "{}", report.message);
        }
        Ok(())
    }
}
