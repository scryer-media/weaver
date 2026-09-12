//! Continuation of the `impl Pipeline` block from `finalize/check.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    /// Everything that happens after the repairer returns, for every path that
    /// runs it.
    ///
    /// # Why it is shared
    ///
    /// Both repair call sites used to carry their own copy of this tail, and the
    /// copies had drifted into different answers to the same question. One
    /// re-read the installed output and judged *that*; the other trusted the
    /// repairer's staged `outcome.verification` and never looked at what
    /// actually landed on disk. A repair that does not verify what it installed
    /// is not verified, so both now run the authoritative pass — which is also
    /// where the direct-set damage adjustments and the volume-safety
    /// recomputation live, so the path that skipped it was missing those too.
    ///
    /// # Ordering
    ///
    /// `RepairComplete` is emitted last: after canonical placement, post-repair
    /// verification, identity reconciliation and durable persistence have all
    /// succeeded. Any failure among them emits `RepairFailed` instead. Emitting
    /// completion first produced the contradictory sequence this replaces —
    /// `RepairComplete`, then the job failing a moment later, with nothing on
    /// the event stream to say the repair had not held.
    ///
    /// This mirrors what both reference implementations announce: SABnzbd runs
    /// an explicit "verifying repaired files" phase before it accepts a repair,
    /// and NZBGet reaches `psRepaired` only from a `Process(true)` that came
    /// back successful, having passed through `ptVerifyingRepaired` first.
    pub(in crate::pipeline) async fn finish_par2_repair(
        &mut self,
        job_id: JobId,
        par2_set: Arc<par2_rs::Par2FileSet>,
        working_dir: std::path::PathBuf,
        pre_repair: &par2_rs::VerificationResult,
        outcome: par2_rs::Par2RepairOutcome,
        has_crc_failures: bool,
    ) {
        // Covers the whole tail including every failure exit below, which the
        // per-stage stamps cannot: a repair that is rejected still spent the
        // time it spent.
        let _finish_scope = crate::runtime::perf_probe::scope("par2_repair.finish");
        match self
            .recover_placement_before_verification(job_id, working_dir.clone())
            .await
        {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => return self.fail_par2_repair(job_id, error),
        }
        let mut stage_start = std::time::Instant::now();
        let slices_repaired = par2_repair_slices_repaired(pre_repair);
        info!(
            job_id = job_id.0,
            status = ?outcome.status,
            slices_repaired,
            bytes_copied = outcome.bytes_copied,
            bytes_reconstructed = outcome.bytes_reconstructed,
            files_complete = outcome.files_complete,
            files_renamed = outcome.files_renamed,
            files_damaged = outcome.files_damaged,
            files_missing = outcome.files_missing,
            "PAR2 repair wrote its outputs — verifying what was installed"
        );

        self.emit_job_verification_started(job_id);
        let (mut post_repair_verification, post_repair_placement_plan) = match self
            .verify_repaired_par2_files_with_placement(
                job_id,
                Arc::clone(&par2_set),
                working_dir.clone(),
                pre_repair,
            )
            .await
        {
            Ok(result) => result,
            Err(message) => return self.fail_par2_repair(job_id, message),
        };
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.verify_repaired", stage_start);

        if let Some(msg) = par2_post_repair_damage_failure(&post_repair_verification) {
            return self.fail_par2_repair(job_id, msg);
        }

        // Rename obfuscated files using PAR2 metadata (16KB hash matching).
        // Must happen after repair and before extraction retry/finalize.
        let deobfuscation = self.try_deobfuscate_files_with_par2(job_id).await;
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.deobfuscate", stage_start);
        let deobfuscation_canonical_file_ids = deobfuscation
            .canonical_description_file_ids
            .get(&par2_set.recovery_set_id)
            .map(|file_ids| file_ids.iter().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let deobfuscation_moved_current_canonical = !deobfuscation_canonical_file_ids.is_empty();
        let placement_moves_paths = !post_repair_placement_plan.swaps.is_empty()
            || !post_repair_placement_plan.renames.is_empty();
        match self
            .apply_placement_plan_for_retry_or_repair(
                job_id,
                working_dir.clone(),
                &post_repair_placement_plan,
            )
            .await
        {
            Ok(placement::ApplyOutcome::Applied) => {}
            Ok(placement::ApplyOutcome::Reverify) => return,
            Err(error) => {
                return self.fail_par2_repair(job_id, error);
            }
        }
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.apply_placement", stage_start);

        // A content rename invalidates this set's verdict only when it populated
        // one of this set's canonical description paths. Moving a damaged source
        // aside as `.duplicateN` leaves the selectively verified canonical bytes
        // untouched, while a rename into a free canonical path was identified
        // from its 16 KiB prefix and still needs strict whole-file proof.
        //
        // When no canonical description path moved, the earlier answer still
        // describes the disk exactly, and re-reading would throw away the whole
        // point of the selective post-repair pass: it reads only what the repair
        // rewrote.
        // `misplaced_before_placement` closes the gap the other two conditions
        // leave: a file can verify as `Renamed` and still produce no plan entry
        // (a rename whose path has no file name lands in `unresolved`), and
        // accepting a repair whose set was never re-read where it now sits is
        // exactly what this pass exists to prevent.
        let misplaced_before_placement = post_repair_verification
            .files
            .iter()
            .any(|file| !matches!(file.status, par2_rs::verify::FileStatus::Complete));
        if deobfuscation_moved_current_canonical
            || placement_moves_paths
            || misplaced_before_placement
        {
            // Only the moved files owe a re-read. A rename moves no bytes:
            // every Complete entry whose canonical path did not change was
            // proven where it sat minutes ago, in this same flow, and reading
            // it again answers a question the disk already answered. What
            // genuinely needs strict proof at its canonical name is exactly
            // the moved set — deobfuscation targets identified from a 16 KiB
            // prefix, placement-plan swaps and renames, and anything the
            // selective pass could not call Complete where it stood.
            let mut must_read: HashSet<par2_rs::FileId> =
                deobfuscation_canonical_file_ids.iter().copied().collect();
            for entry in &post_repair_placement_plan.renames {
                must_read.insert(entry.file_id);
            }
            for (left, right) in &post_repair_placement_plan.swaps {
                must_read.insert(left.file_id);
                must_read.insert(right.file_id);
            }
            for file in &post_repair_verification.files {
                if !matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                    must_read.insert(file.file_id);
                }
            }
            let mut must_read: Vec<par2_rs::FileId> = must_read.into_iter().collect();
            must_read.sort_unstable_by_key(|file_id| *file_id.as_bytes());
            info!(
                job_id = job_id.0,
                deobfuscated = deobfuscation.renamed,
                deobfuscation_moved_current_canonical,
                placement_moves_paths,
                misplaced_before_placement,
                moved_files_reread = must_read.len(),
                carried = post_repair_verification
                    .files
                    .len()
                    .saturating_sub(must_read.len()),
                "paths moved after repair — re-verifying the moved files where they now sit"
            );
            let settled =
                match self
                    .run_par2_placement_pass(
                        job_id,
                        Arc::clone(&par2_set),
                        working_dir.clone(),
                        Par2PassScope::Selected(must_read),
                    )
                    .await
                {
                    Ok((fresh, _)) => {
                        // The merge below iterates the BASE's files, so a fresh
                        // entry for a description outside it — a deobfuscation
                        // extra the recovery data does not protect — would drop
                        // out of the merged result and out of the damage check
                        // with it. Every file this pass just read must be Complete
                        // where it now sits, extras included, so they are judged
                        // here on the fresh result directly.
                        if let Some(failed) = fresh.files.iter().find(|file| {
                            !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                        }) {
                            let msg = format!(
                                "PAR2 repair completed but {} was not intact at its canonical \
                             path after deobfuscation and placement",
                                failed.filename
                            );
                            return self.fail_par2_repair(job_id, msg);
                        }
                        // Merge THEN settle, the order the selective post-repair
                        // pass documents as load-bearing: the merge recomputes
                        // totals over carried and fresh entries alike, and settle
                        // re-forgives what the recompute resurrected.
                        let mut merged = par2_rs::verify::merge_verification_results(
                            &par2_set,
                            &post_repair_verification,
                            fresh,
                        );
                        self.settle_par2_pass_result(job_id, &mut merged, false);
                        merged
                    }
                    Err(message) => return self.fail_par2_repair(job_id, message),
                };
            if par2_verification_needs_repair(&settled) {
                let msg = format!(
                    "PAR2 repair completed but verification after deobfuscation and placement \
                     found {} damaged slices or file placements remaining",
                    settled.total_missing_blocks
                );
                return self.fail_par2_repair(job_id, msg);
            }
            post_repair_verification = settled;
            stage_start = note_par2_repair_stage(
                job_id,
                "par2_repair.finish.verify_after_placement",
                stage_start,
            );
        }
        self.retry_par2_authoritative_identity(job_id).await;
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.identity", stage_start);
        let registration = match self
            .register_verified_par2_rar_outputs(job_id, &post_repair_verification)
            .await
        {
            Ok(registration) => registration,
            Err(error) => return self.fail_par2_repair(job_id, error),
        };
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.register_outputs", stage_start);
        // The descriptions the repair wrote, taken from the pre-repair verdict
        // the repairer itself acted on rather than re-derived from names here.
        let rewritten: HashSet<par2_rs::FileId> =
            par2_repair_write_set(pre_repair).into_iter().collect();
        self.refresh_verified_complete_archive_topologies(
            job_id,
            &post_repair_verification,
            &rewritten,
        )
        .await;
        // Outputs the NZB never carried have no file id to travel through the
        // refresh set, so their sets are invalidated from the registration.
        let adopted_sevenz_parts = registration.sevenz_parts;
        self.invalidate_rar_plans_for_repaired_sets(job_id, registration.set_names);
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.refresh_topologies", stage_start);
        if let Err(error) = self
            .reconcile_and_classify_par2_verification(
                job_id,
                &post_repair_verification,
                has_crc_failures,
                "PAR2 repair",
            )
            .await
        {
            return self.fail_par2_repair(job_id, error);
        }
        stage_start = note_par2_repair_stage(job_id, "par2_repair.finish.reconcile", stage_start);
        // Repair rewrote bytes; digests streamed before it describe content
        // that is gone. Every `Complete` entry in the merged result is vouched
        // by a pass that read the disk — the files the repair rewrote by the
        // selective pass just above, the ones it left alone by the pre-repair
        // pass that decided they were complete — so the described digests are
        // proven observations, not expectations.
        if let Err(error) = self
            .refresh_authoritative_verified_hashes(job_id, &par2_set, &post_repair_verification)
            .await
        {
            return self.fail_par2_repair(job_id, error);
        }
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.refresh_hashes", stage_start);

        // Only now is the repair a fact worth announcing.
        //
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Complete,
            u64::from(slices_repaired),
        );
        let _ = self.event_tx.send(PipelineEvent::RepairComplete {
            job_id,
            slices_repaired,
        });

        let set_id = par2_set.recovery_set_id;
        let _ = self
            .settle_par2_set(job_id, set_id, Par2SetSettlementReason::Repaired)
            .await;
        stage_start =
            note_par2_repair_stage(job_id, "par2_repair.finish.mark_verified", stage_start);
        if !self.par2_verified.contains(&job_id) {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            self.schedule_job_completion_check(job_id);
            return;
        }

        // Leftovers are purged when the aggregate settles — see
        // `mark_par2_verified`, which is reached from here through
        // `settle_par2_set` and also from a clean final set that never
        // runs this tail at all.
        self.transition_postprocessing_status(job_id, JobStatus::Downloading, Some("downloading"));

        if has_crc_failures {
            let cleared = self.failed_extractions.get(&job_id).map_or(0, HashSet::len);
            self.replace_failed_extraction_members(job_id, HashSet::new());
            if cleared > 0 {
                info!(
                    job_id = job_id.0,
                    cleared, "cleared failed extractions for post-repair retry"
                );
            }
        }

        // A repaired interior RAR volume is only visible to the incremental
        // scheduler after its synchronous refresh. Do that before scheduling
        // another completion check, or a stale WaitingForVolumes plan can
        // re-enter PAR2 forever.
        //
        // A 7z part the repair rebuilt and the topology just adopted is the
        // same situation in the other archive's terms: the set was short a
        // part, now is not, and the extraction that was never attempted (or
        // failed on the truncated set) is what this job is waiting for.
        if has_crc_failures
            || adopted_sevenz_parts > 0
            || self.job_has_live_rar_waiting_for_missing_volumes(job_id)
        {
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            note_par2_repair_stage(job_id, "par2_repair.finish.retry_extraction", stage_start);
            return;
        }

        self.reconcile_job_progress(job_id).await;
        note_par2_repair_stage(job_id, "par2_repair.finish.reconcile_progress", stage_start);
        self.schedule_job_completion_check(job_id);
    }

    /// Fail a job that was mid-repair, announcing it as a repair failure.
    ///
    /// Every non-success exit from [`Self::finish_par2_repair`] comes through
    /// here, so a job can no longer fail silently after `RepairComplete` has
    /// already told the UI that the repair held.
    /// Remove what the repair left behind, now that the repair has been
    /// accepted.
    ///
    /// Mirrors NZBGet's `DeleteLeftovers()`, which it reaches only from the
    /// branch where `Process(true)` came back successful, and SABnzbd's
    /// `deletables` after a finished repair. The gating is the whole point: on
    /// any failure these files are the evidence, and this is not reached.
    ///
    /// Only entries that *appeared during* the repair are candidates, and only
    /// when they are neither an NZB entry under any of its names nor a file the
    /// recovery set describes. A file the repair reconstructed from `Missing`
    /// lands at a described name and is therefore never a candidate — the test
    /// is membership, not a suffix convention borrowed from another crate.
    pub(in crate::pipeline) fn purge_par2_repair_leftovers(&mut self, job_id: JobId) {
        let Some(before) = self.par2_pre_repair_dir_entries.remove(&job_id) else {
            return;
        };
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let working_dir = state.working_dir.clone();

        for name in self.par2_repair_leftover_names(job_id, &before) {
            let path = working_dir.join(&name);
            match std::fs::remove_file(&path) {
                Ok(()) => info!(
                    job_id = job_id.0,
                    path = %path.display(),
                    "removed repair leftover after acceptance"
                ),
                Err(error) => warn!(
                    job_id = job_id.0,
                    path = %path.display(),
                    error = %error,
                    "could not remove repair leftover"
                ),
            }
        }
    }

    /// The working-directory files a repair left behind, named by difference
    /// against `before`, the directory listing taken before the first repair
    /// touched it: an entry that was not there then, that no NZB file answers
    /// to under any of its names, and that no servable set describes. This is
    /// the one rule for what a leftover is; [`Self::purge_par2_repair_leftovers`]
    /// removes them once the job has settled, and until then the extra scan of
    /// every set keeps them out of its candidates. Sorted, so two calls over an
    /// unchanged directory compare equal.
    pub(in crate::pipeline) fn par2_repair_leftover_names(
        &self,
        job_id: JobId,
        before: &HashSet<String>,
    ) -> Vec<String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };
        let working_dir = state.working_dir.clone();

        let mut keep = HashSet::<String>::new();
        for file in state.assembly.files() {
            keep.insert(sanitize_download_filename(file.filename()));
            if let Some(identity) = self.effective_file_identity(job_id, file.file_id()) {
                keep.insert(sanitize_download_filename(&identity.current_filename));
                keep.insert(sanitize_download_filename(&identity.source_filename));
                if let Some(canonical) = identity.canonical_filename.as_ref() {
                    keep.insert(sanitize_download_filename(canonical));
                }
            }
        }
        for set_id in self.par2_servable_set_ids(job_id) {
            if let Some(set) = self.par2_set_for(job_id, set_id) {
                for desc in set.files.values() {
                    keep.insert(sanitize_download_filename(&desc.filename));
                }
            }
        }

        let mut leftovers: Vec<String> = directory_entry_names(&working_dir)
            .into_iter()
            .filter(|name| {
                !before.contains(name) && !keep.contains(&sanitize_download_filename(name))
            })
            .filter(|name| working_dir.join(name).is_file())
            .collect();
        leftovers.sort();
        leftovers
    }

    pub(in crate::pipeline) fn fail_par2_repair(&mut self, job_id: JobId, error: String) {
        // Dropped unread: the artefacts stay on disk for diagnosis, and the
        // snapshot must not survive into the next attempt.
        self.par2_pre_repair_dir_entries.remove(&job_id);
        warn!(job_id = job_id.0, error = %error, "PAR2 repair failed");
        // Low-frequency: one observation per job-level repair, never on a
        // per-segment path. Records the metric next to the event that already
        // announces the same fact.
        self.metrics.job_lifecycle.note_repair(
            crate::operations::instrumentation::StageOutcomeKind::Failed,
            0,
        );
        let _ = self.event_tx.send(PipelineEvent::RepairFailed {
            job_id,
            error: error.clone(),
        });
        if let Some(set_id) = self.par2_served_set_id(job_id) {
            let _ = self.mark_par2_set_failed(job_id, set_id, error);
            self.finish_or_rearm_after_par2_set_failure(job_id);
        } else {
            self.fail_job(job_id, error);
        }
    }

    /// After an *authoritative* post-repair verification, re-persist every
    /// confirmed file's digest from the recovery set with `Verified`
    /// provenance.
    ///
    /// Repair rewrites bytes in place, so a digest streamed before the
    /// rewrite describes content that is gone; left standing, a restart
    /// would load it as trusted and compare stale bytes' MD5 against the
    /// recovery set forever. Persisting the description's digest is sound
    /// here — and only here — because every `Complete` entry the caller hands
    /// over is vouched by a pass that read the bytes off disk. The quick paths'
    /// synthetic all-valid results, which read nothing, must never reach this
    /// function.
    ///
    /// # The vouching is two passes, not one
    ///
    /// The post-repair result is a merge, and each half is proven by its own
    /// read:
    ///
    /// - the files the repair **rewrote** are proven by the post-repair pass,
    ///   which read them back after the repair installed them;
    /// - the files it **did not touch** are proven by the pre-repair pass,
    ///   which read them in this same flow — that pass is what decided they
    ///   were complete, and being complete is exactly why the repair left them
    ///   alone.
    ///
    /// Both halves are measured bytes; neither is a description standing in for
    /// a read. What the merge accepts is a *window* rather than a gap in
    /// evidence: an untouched file that some other writer corrupts between the
    /// two passes still carries its earlier verdict. That is the same trust
    /// class as an in-stream claim relied on across the same interval, and it
    /// is stated at the merge site in
    /// [`Pipeline::verify_repaired_par2_files_with_placement`].
    ///
    /// A `Verified` digest is attached only through an UNAMBIGUOUS identity:
    /// every alias a name resolves to is kept (never first-wins), a
    /// `Renamed` result prefers the actual verified path over the expected
    /// description name, each verification entry must resolve to exactly one
    /// assembly file, no two entries may claim the same file, and the file
    /// on disk must measure exactly the described length. Anything short of
    /// that keeps whatever digest state already exists.
    pub(crate) async fn refresh_authoritative_verified_hashes(
        &mut self,
        job_id: JobId,
        par2_set: &par2_rs::Par2FileSet,
        verification: &par2_rs::VerificationResult,
    ) -> Result<(), String> {
        struct ResolvedRefresh {
            file_index: u32,
            filename: String,
            path: std::path::PathBuf,
            described_length: u64,
            hash: [u8; 16],
        }

        let resolved: Vec<ResolvedRefresh> = {
            let Some(state) = self.jobs.get(&job_id) else {
                return Ok(());
            };
            let working_dir = state.working_dir.clone();

            // Every alias each name could mean — never collapsed first-wins.
            // Current filenames are also kept separately: a `Renamed` result
            // names a physical path, and physical paths may only resolve
            // against where files live NOW. The path the pre-plan
            // verification saw a file at can, by refresh time, be nothing
            // but some other file's immutable source alias — resolving a
            // renamed result through source/canonical aliases is how a
            // digest lands on the wrong file.
            let mut by_name = HashMap::<String, Vec<NzbFileId>>::new();
            let mut by_current = HashMap::<String, Vec<NzbFileId>>::new();
            for file in state.assembly.files() {
                if !file.is_complete() {
                    continue;
                }
                let file_id = file.file_id();
                let mut aliases: Vec<String> = Vec::new();
                let current_name;
                if let Some(identity) = self.effective_file_identity(job_id, file_id) {
                    current_name = identity.current_filename.clone();
                    aliases.push(identity.current_filename);
                    aliases.push(identity.source_filename);
                    if let Some(canonical) = identity.canonical_filename {
                        aliases.push(canonical);
                    }
                } else {
                    current_name = file.filename().to_string();
                    aliases.push(current_name.clone());
                }
                aliases.sort();
                aliases.dedup();
                for alias in aliases {
                    let ids = by_name.entry(alias).or_default();
                    if !ids.contains(&file_id) {
                        ids.push(file_id);
                    }
                }
                let ids = by_current.entry(current_name).or_default();
                if !ids.contains(&file_id) {
                    ids.push(file_id);
                }
            }

            let mut matched = HashMap::<NzbFileId, (String, u64, [u8; 16])>::new();
            let mut contested: HashSet<NzbFileId> = HashSet::new();
            for file_verification in &verification.files {
                if !matches!(
                    file_verification.status,
                    par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
                ) {
                    continue;
                }
                let Some(description) = par2_set.file_description(&file_verification.file_id)
                else {
                    continue;
                };

                // The name the verified bytes actually live under outranks
                // the name the description expected: for `Renamed`, that is
                // the renamed path. Renamed results are physical-path
                // claims, so they resolve against CURRENT names only —
                // never through source or canonical aliases.
                let mut candidate_names: Vec<String> = Vec::new();
                let name_map = match &file_verification.status {
                    par2_rs::verify::FileStatus::Renamed(path) => {
                        if let Some(filename) = path.file_name() {
                            candidate_names.push(filename.to_string_lossy().to_string());
                        }
                        &by_current
                    }
                    _ => &by_name,
                };
                candidate_names.push(file_verification.filename.clone());

                // The first name that resolves at all decides — and it must
                // resolve to exactly one file, or this entry is ambiguous
                // and attaches nothing.
                let resolved_id = candidate_names.iter().find_map(|name| {
                    let ids = name_map.get(name)?;
                    Some(if ids.len() == 1 { Some(ids[0]) } else { None })
                });
                let Some(Some(file_id)) = resolved_id else {
                    if resolved_id.is_some() {
                        crate::runtime::perf_probe::record(
                            "completion.post_repair.refresh_skipped.ambiguous_alias",
                            std::time::Duration::from_nanos(1),
                        );
                    }
                    continue;
                };
                // Two verification entries claiming one assembly file prove
                // the mapping is not one-to-one; neither may attach.
                if matched.remove(&file_id).is_some() || contested.contains(&file_id) {
                    contested.insert(file_id);
                    crate::runtime::perf_probe::record(
                        "completion.post_repair.refresh_skipped.contested_file",
                        std::time::Duration::from_nanos(1),
                    );
                    continue;
                }
                let current_filename = self
                    .current_filename_for_file_id(job_id, file_id)
                    .unwrap_or_else(|| file_verification.filename.clone());
                matched.insert(
                    file_id,
                    (current_filename, description.length, description.hash_full),
                );
            }

            matched
                .into_iter()
                .map(
                    |(file_id, (filename, described_length, hash))| ResolvedRefresh {
                        file_index: file_id.file_index,
                        path: working_dir.join(&filename),
                        filename,
                        described_length,
                        hash,
                    },
                )
                .collect()
        };

        // The digest may only attach to a file whose bytes measure exactly
        // the described length. `received_bytes` is unusable here — repair
        // reconciliation marks files complete with the encoded NZB total —
        // so ask the filesystem; the authoritative pass just read these
        // files, and this is one bounded stat per confirmed file on the
        // exceptional post-repair path.
        let mut entries: Vec<(u32, String, Option<[u8; 16]>)> = Vec::new();
        for refresh in resolved {
            match tokio::fs::metadata(&refresh.path).await {
                Ok(metadata) if metadata.len() == refresh.described_length => {
                    entries.push((refresh.file_index, refresh.filename, Some(refresh.hash)));
                }
                Ok(_) | Err(_) => {
                    crate::runtime::perf_probe::record(
                        "completion.post_repair.refresh_skipped.length_mismatch",
                        std::time::Duration::from_nanos(1),
                    );
                }
            }
        }

        if entries.is_empty() {
            return Ok(());
        }
        crate::runtime::perf_probe::record(
            "completion.post_repair.verified_hashes_refreshed",
            std::time::Duration::from_nanos(1),
        );
        self.db_blocking(move |db| {
            db.complete_files(
                job_id,
                &entries,
                crate::jobs::persistence::CompletedHashProvenance::Verified,
            )
        })
        .await
        .map_err(|error| format!("failed to refresh post-repair verified hashes: {error}"))
    }

    /// `rewritten` is the repair's write set, empty on a pass that repaired
    /// nothing. See
    /// [`Self::verified_complete_archive_file_ids_needing_refresh`].
    pub(in crate::pipeline) async fn refresh_verified_complete_archive_topologies(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> usize {
        let targets =
            self.verified_complete_archive_refresh_targets(job_id, verification, rewritten);
        let file_ids: Vec<NzbFileId> = targets.iter().map(|(file_id, _)| *file_id).collect();
        if !file_ids.is_empty() {
            info!(
                job_id = job_id.0,
                files = file_ids.len(),
                "refreshing archive topology from verified PAR2 outputs"
            );
        }
        // Only the files the repair actually rewrote invalidate a plan. A
        // renamed file, or one whose set is being given its first topology, is
        // ordinary progress and the refresh walk below is the whole of it.
        let rewritten_file_ids: Vec<NzbFileId> = targets
            .iter()
            .filter_map(|(file_id, was_rewritten)| was_rewritten.then_some(*file_id))
            .collect();
        let rewritten_set_names = self.rar_set_names_for_files(job_id, &rewritten_file_ids);
        for file_id in &file_ids {
            self.refresh_archive_state_for_completed_file(job_id, *file_id, false)
                .await;
        }
        self.invalidate_rar_plans_for_repaired_sets(job_id, rewritten_set_names);
        // Every arm that accepts a verdict passes through here on its way to
        // reconciliation, so a set the verdict has already joined is retired in
        // one place rather than at each of them.
        self.retire_par2_joined_split_topologies(job_id, verification);
        file_ids.len()
    }

    /// The RAR set names owning a list of the job's files, deduplicated.
    pub(in crate::pipeline) fn rar_set_names_for_files(
        &self,
        job_id: JobId,
        file_ids: &[NzbFileId],
    ) -> BTreeSet<String> {
        let Some(state) = self.jobs.get(&job_id) else {
            return BTreeSet::new();
        };
        file_ids
            .iter()
            .filter_map(|file_id| {
                let file = state.assembly.file(*file_id)?;
                if !matches!(
                    self.classified_role_for_file(job_id, file),
                    weaver_model::files::FileRole::RarVolume { .. }
                ) {
                    return None;
                }
                self.classified_archive_set_name_for_file(job_id, file)
            })
            .collect()
    }

    /// Force a header-level plan rebuild for every set a repair touched, and
    /// hold extraction until it lands.
    ///
    /// Registering the repaired volume's facts is not enough on its own. The
    /// derived plan — the member chain, and with it the volume range extraction
    /// opens — was computed while those volumes were missing, and nothing about
    /// installing new facts retires it. Re-deriving from the facts alone would
    /// not do either: the member chain comes from the volumes' *headers*, which
    /// is exactly what the repair rewrote.
    ///
    /// [`RefreshReason::IdentityRebind`] is the existing reason for "the bytes
    /// behind this set are not what the plan was built from". It marks the
    /// refresh state `structure_dirty`, which is what makes
    /// `rar_member_refresh_request` demand a rebuild before a member may start,
    /// and it leaves the request `in_flight`, which is what
    /// `job_has_pending_rar_refresh_for_current_sets` reports and the
    /// `pending_rar_refresh` arm of the completion gate already defers on. No
    /// new gate: the repaired set becomes pending in the one the extraction path
    /// has always honoured.
    pub(in crate::pipeline) fn invalidate_rar_plans_for_repaired_sets(
        &mut self,
        job_id: JobId,
        set_names: BTreeSet<String>,
    ) {
        for set_name in set_names {
            let target = self.latest_completed_rar_volume(job_id, &set_name);
            info!(
                job_id = job_id.0,
                set_name = %set_name,
                target_completed_volume = target,
                "rebuilding a repaired RAR set's plan from its repaired headers"
            );
            // "From its repaired headers" is only true once the pre-repair
            // snapshot is gone. The rebuild the refresh triggers reads
            // `load_rar_snapshot` first, and that snapshot was serialized while
            // the set still had the hole the repair just filled — so the plan
            // came back describing the old, short set and extraction opened a
            // member span that ended at the last volume the snapshot knew
            // about. Same failure the recovery-volume restore hit, same fix:
            // drop both copies so the recompute reads the volumes on disk.
            self.invalidate_rar_snapshot(job_id, &set_name);
            self.enqueue_rar_set_refresh(
                job_id,
                &set_name,
                target,
                crate::pipeline::RefreshReason::IdentityRebind,
            );
        }
    }

    /// Retire the split topologies a verdict has already produced the output of.
    ///
    /// A plain split posting ships `<name>.001/.002/.003` while its recovery
    /// data is computed over `<name>` — a file the posting never carries. The
    /// recovery pass reads the parts as one file and installs `<name>` itself,
    /// so by the time a verdict vouches for it the join has happened. Running
    /// the joiner afterwards writes the parts' bytes back over the output that
    /// was just verified, and waiting for every part to be whole fails a job
    /// whose payload is already on disk and proven.
    ///
    /// The match is a plain key lookup: a split topology is named by
    /// `archive_base_name` of its parts, which is exactly the joined-output
    /// name, so a verdict naming the *parts* — the ordinary shape, where the
    /// recovery set protects what the posting actually carries — finds no
    /// topology and retires nothing.
    ///
    /// Paths are checked before use, as they are for rebuilt RAR volumes: a
    /// PAR2 description names its own file, so an absolute path or one
    /// containing `..` is refused rather than resolved.
    pub(super) fn retire_par2_joined_split_topologies(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> usize {
        let Some(par2_set) = self.par2_set(job_id).cloned() else {
            return 0;
        };

        let mut retired = 0usize;
        for file in &verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Complete) {
                continue;
            }
            let path = Path::new(&file.filename);
            if file.filename.is_empty()
                || !path.is_relative()
                || !path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                continue;
            }
            let is_split_set = self.jobs.get(&job_id).is_some_and(|state| {
                state
                    .assembly
                    .archive_topology_for(&file.filename)
                    .is_some_and(|topology| {
                        topology.archive_type == crate::jobs::assembly::ArchiveType::Split
                    })
            });
            if !is_split_set {
                continue;
            }

            // The verdict says the bytes hashed; this says they are still where
            // the job can deliver them from, at the length the set describes.
            let described_length = par2_set
                .files
                .values()
                .find(|description| description.filename == file.filename)
                .map(|description| description.length);
            let output_present = self
                .resolve_job_input_path(job_id, &file.filename)
                .and_then(|path| std::fs::metadata(path).ok())
                .is_some_and(|metadata| {
                    metadata.is_file() && Some(metadata.len()) == described_length
                });
            if !output_present {
                continue;
            }

            let Some(state) = self.jobs.get_mut(&job_id) else {
                return retired;
            };
            let Some(topology) = state.assembly.remove_archive_topology(&file.filename) else {
                continue;
            };
            // The parts this set was chasing are about to be deleted from disk.
            self.direct_unpack_abort_set(
                job_id,
                &file.filename,
                "split topology retired by its recovery data",
                crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
                crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
            );
            let parts: HashSet<String> = topology.volume_map.keys().cloned().collect();
            info!(
                job_id = job_id.0,
                set_name = %file.filename,
                parts = parts.len(),
                "recovery set produced the joined output — retiring the split topology"
            );
            self.par2_joined_split_sets
                .entry(job_id)
                .or_default()
                .insert(file.filename.clone(), parts);
            retired += 1;
        }

        retired
    }

    /// Whether a file is a posted part of a split set a verdict has already
    /// joined.
    ///
    /// Such a part is a consumed input, not payload the job is short of: its
    /// bytes are inside the output the recovery set vouched for, and there is
    /// nothing left to download, repair or wait for. It therefore belongs in
    /// none of the post-verdict incomplete buckets — which matters most for the
    /// *first* part, whose 16 KiB prefix is the joined file's own, so PAR2
    /// content identity answers the joined description with it.
    pub(in crate::pipeline) fn par2_join_consumed_split_part(
        &self,
        job_id: JobId,
        file_id: NzbFileId,
    ) -> bool {
        let Some(sets) = self.par2_joined_split_sets.get(&job_id) else {
            return false;
        };
        if sets.is_empty() {
            return false;
        }
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file) = state.assembly.file(file_id) else {
            return false;
        };
        let current = self.current_filename_for_file(job_id, file);
        sets.values()
            .any(|parts| parts.contains(&current) || parts.contains(file.filename()))
    }

    /// The parts of every split set a verdict has joined for this job.
    pub(in crate::pipeline) fn par2_joined_split_part_names(&self, job_id: JobId) -> Vec<String> {
        self.par2_joined_split_sets
            .get(&job_id)
            .map(|sets| sets.values().flatten().cloned().collect())
            .unwrap_or_default()
    }

    /// Delete the parts a verified join consumed, before finalization ships
    /// them alongside the file they joined into.
    ///
    /// This is the same removal the post-extraction cleanups perform for the
    /// sources of an archive that was extracted, on the same unconditional
    /// terms: the join happened, so the parts are spent inputs.
    pub(in crate::pipeline) async fn cleanup_par2_joined_split_parts(&mut self, job_id: JobId) {
        let parts = self.par2_joined_split_part_names(job_id);
        if parts.is_empty() {
            return;
        }
        let mut removed = 0u32;
        for filename in &parts {
            let Some(path) = self.resolve_job_input_path(job_id, filename) else {
                continue;
            };
            match tokio::fs::remove_file(&path).await {
                Ok(()) => removed += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => warn!(
                    file = %path.display(),
                    error = %error,
                    "failed to clean up a joined split part"
                ),
            }
        }
        info!(
            job_id = job_id.0,
            removed,
            total = parts.len(),
            "removed the split parts a verified join consumed"
        );
    }

    /// Register RAR volumes that PAR2 *rebuilt* and that the NZB never carried.
    ///
    /// Ported from release-0.7.9. A missing interior volume repaired from
    /// recovery blocks lands on disk under a name the job's assembly has never
    /// heard of, so extraction goes on believing the volume is absent and the
    /// repair achieves nothing. This walks a clean verification result, keeps
    /// the entries that are RAR volumes the job does not already know, and
    /// persists their parsed facts against the set.
    ///
    /// Paths are checked before use: a PAR2 description names its own file, so
    /// an absolute path or one containing `..` is refused rather than resolved.
    pub(crate) async fn register_verified_par2_rar_outputs(
        &mut self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
    ) -> Result<Par2RarOutputRegistration, String> {
        let registered_filenames = self
            .jobs
            .get(&job_id)
            .map(|state| {
                state
                    .assembly
                    .files()
                    .map(|file| file.filename().to_string())
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let mut registration = Par2RarOutputRegistration::default();
        for file in &verification.files {
            if !matches!(file.status, par2_rs::verify::FileStatus::Complete)
                || registered_filenames.contains(&file.filename)
            {
                continue;
            }

            let path = Path::new(&file.filename);
            if file.filename.is_empty()
                || !path.is_relative()
                || !path
                    .components()
                    .all(|component| matches!(component, std::path::Component::Normal(_)))
            {
                return Err(format!(
                    "refusing unsafe PAR2-verified RAR output path {:?}",
                    file.filename
                ));
            }

            let role = weaver_model::files::FileRole::from_filename(&file.filename);
            let volume_number = match role {
                weaver_model::files::FileRole::RarVolume { volume_number } => volume_number,
                weaver_model::files::FileRole::SevenZipSplit { number } => {
                    if self.adopt_verified_par2_sevenz_part(
                        job_id,
                        &file.filename,
                        &role,
                        number,
                    )? {
                        registration.sevenz_parts += 1;
                    }
                    continue;
                }
                _ => continue,
            };
            let Some(set_name) = weaver_model::files::archive_base_name(&file.filename, &role)
            else {
                continue;
            };
            let path = self
                .resolve_job_input_path(job_id, &file.filename)
                .ok_or_else(|| {
                    format!(
                        "PAR2 verified RAR output {} has no active job directory",
                        file.filename
                    )
                })?;
            if !path.is_file() {
                return Err(format!(
                    "PAR2 verified RAR output {} is missing from staging",
                    path.display()
                ));
            }

            let password_candidates = self.archive_password_candidates_for_set(job_id, &set_name);
            let facts = Self::parse_rar_volume_facts_from_path(path, password_candidates)
                .await
                .map_err(|error| {
                    format!(
                        "failed to parse PAR2-verified RAR output {}: {error}",
                        file.filename
                    )
                })?;
            if self.persist_rar_volume_facts(
                job_id,
                &set_name,
                &file.filename,
                Some(volume_number),
                facts,
            )? {
                registration.registered += 1;
                // A registered output is a volume the plan was built without.
                // Its set's member chain is therefore derived from a strictly
                // smaller set of headers than the one now on disk, whether or
                // not any NZB file of that set was itself rewritten.
                registration.set_names.insert(set_name);
            }
        }

        if registration.registered > 0 {
            info!(
                job_id = job_id.0,
                registered = registration.registered,
                "registered PAR2-verified RAR outputs absent from the NZB"
            );
        }
        Ok(registration)
    }

    /// Adopt a `.7z.NNN` part the recovery set proved Complete into its set's
    /// topology, when the NZB never carried it.
    ///
    /// The RAR arm of [`Self::register_verified_par2_rar_outputs`] persists
    /// header facts and lets the plan rebuild from them. A 7z split set has no
    /// header chain: its topology is the numbering of the parts the assembly
    /// registered, which is exactly what a part the NZB never carried is
    /// missing from. So the part goes straight into the topology — its name
    /// into `volume_map`, its number marked complete, and the expected count
    /// raised when it lies past the end, since a withheld *last* part is one
    /// the topology never counted. `sevenz_set_part_paths` then hands it to the
    /// extractor off the same map.
    ///
    /// `Ok(false)` when the part belongs to no 7z set this job knows, or the
    /// set already lists it under this or an NZB file's current name. `Err`
    /// when the verdict says Complete but the bytes are not where the
    /// description puts them — the same refusal the RAR arm makes.
    pub(super) fn adopt_verified_par2_sevenz_part(
        &mut self,
        job_id: JobId,
        filename: &str,
        role: &weaver_model::files::FileRole,
        number: u32,
    ) -> Result<bool, String> {
        let Some(set_name) = weaver_model::files::archive_base_name(filename, role) else {
            return Ok(false);
        };
        let set_key = sanitize_download_filename(&set_name);
        let Some(state) = self.jobs.get(&job_id) else {
            return Ok(false);
        };
        let Some(topology_name) = state
            .assembly
            .archive_topologies()
            .iter()
            .find(|(name, topology)| {
                topology.archive_type == crate::jobs::assembly::ArchiveType::SevenZip
                    && sanitize_download_filename(name) == set_key
            })
            .map(|(name, _)| name.clone())
        else {
            return Ok(false);
        };
        let already_listed = state
            .assembly
            .archive_topology_for(&topology_name)
            .is_some_and(|topology| topology.volume_map.contains_key(filename))
            || state
                .assembly
                .files()
                .any(|file| self.current_filename_for_file(job_id, file) == filename);
        if already_listed {
            return Ok(false);
        }
        let path = self
            .resolve_job_input_path(job_id, filename)
            .ok_or_else(|| {
                format!("PAR2 verified 7z part {filename} has no active job directory")
            })?;
        if !path.is_file() {
            return Err(format!(
                "PAR2 verified 7z part {} is missing from staging",
                path.display()
            ));
        }
        let Some(topology) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| state.assembly.archive_topology_for_mut(&topology_name))
        else {
            return Ok(false);
        };
        topology.volume_map.insert(filename.to_string(), number);
        topology.complete_volumes.insert(number);
        let expected = topology
            .expected_volume_count
            .map_or(number.saturating_add(1), |expected| {
                expected.max(number.saturating_add(1))
            });
        topology.expected_volume_count = Some(expected);
        for member in &mut topology.members {
            member.last_volume = member.last_volume.max(number);
        }
        info!(
            job_id = job_id.0,
            set_name = %topology_name,
            part = %filename,
            volume = number,
            expected_volumes = expected,
            "adopted a PAR2-verified 7z part the NZB never carried"
        );
        Ok(true)
    }

    /// The archive files whose topology must be rebuilt from what a verdict
    /// proved about the disk.
    ///
    /// `rewritten` names the descriptions a repair just wrote — the write set of
    /// [`par2_repair_write_set`], empty on every pass that repaired nothing.
    /// Those files are included **unconditionally**, and that is the whole
    /// reason the parameter exists.
    ///
    /// A repaired volume re-verifies as `Complete`, not `Renamed`, and its set
    /// already carries a plan — one derived from cached headers back while the
    /// volume was still missing. So neither of the two conditions that admit a
    /// file here holds for precisely the files whose bytes just changed, and the
    /// refresh walks past them: the plan a repair exists to correct is the one
    /// left standing. A member chain that ended at the repaired volume keeps
    /// ending there, extraction opens the truncated volume range, and the packed
    /// data fails its CRC against bytes that are in fact perfect.
    ///
    /// `needs_refresh` asks whether a set has a plan *at all*, which is a
    /// question about existence where this needs one about staleness. Repair is
    /// the one place staleness is known rather than inferred — the repairer says
    /// which descriptions it wrote — so the answer is threaded in rather than
    /// re-derived from names here.
    /// The refresh set without the rewritten flags — the shape the tests assert
    /// against. Production reads
    /// [`Self::verified_complete_archive_refresh_targets`], which keeps them.
    #[cfg(test)]
    pub(crate) fn verified_complete_archive_file_ids_needing_refresh(
        &self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> Vec<NzbFileId> {
        self.verified_complete_archive_refresh_targets(job_id, verification, rewritten)
            .into_iter()
            .map(|(file_id, _)| file_id)
            .collect()
    }

    /// The refresh set, each entry flagged with whether the repair rewrote it.
    ///
    /// The flag is what separates "rebuild this file's topology" from "this
    /// file's set was built from bytes that no longer exist". Only the latter
    /// may invalidate a set's plan: a rename or a first-time topology build is
    /// ordinary progress, and forcing a header-level rebuild for those would
    /// re-derive a plan from the same headers it already holds.
    pub(super) fn verified_complete_archive_refresh_targets(
        &self,
        job_id: JobId,
        verification: &par2_rs::VerificationResult,
        rewritten: &HashSet<par2_rs::FileId>,
    ) -> Vec<(NzbFileId, bool)> {
        let Some(state) = self.jobs.get(&job_id) else {
            return Vec::new();
        };

        let mut by_name = HashMap::<String, (NzbFileId, bool)>::new();
        for file in state.assembly.files() {
            if !file.is_complete() {
                continue;
            }

            let role = self.classified_role_for_file(job_id, file);
            if !Self::role_refreshes_archive_topology(&role) {
                continue;
            }

            let needs_refresh = self.archive_topology_needs_refresh(job_id, file, &role);
            let file_id = file.file_id();
            let identity = self.effective_file_identity(job_id, file_id);
            let current_filename = identity
                .as_ref()
                .map(|value| value.current_filename.clone())
                .unwrap_or_else(|| file.filename().to_string());
            Self::insert_par2_name_candidates(
                &mut by_name,
                &current_filename,
                file_id,
                needs_refresh,
            );
            if let Some(identity) = identity {
                Self::insert_par2_name_candidates(
                    &mut by_name,
                    &identity.source_filename,
                    file_id,
                    needs_refresh,
                );
                if let Some(canonical) = &identity.canonical_filename {
                    Self::insert_par2_name_candidates(
                        &mut by_name,
                        canonical,
                        file_id,
                        needs_refresh,
                    );
                }
            }
        }

        let mut matched = HashMap::<NzbFileId, bool>::new();
        for file_verification in &verification.files {
            let renamed = matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Renamed(_)
            );
            if !matches!(
                file_verification.status,
                par2_rs::verify::FileStatus::Complete | par2_rs::verify::FileStatus::Renamed(_)
            ) {
                continue;
            }

            // Additive: the two original conditions are untouched, and a file
            // the repair rewrote joins them regardless of what either says.
            let was_rewritten = rewritten.contains(&file_verification.file_id);
            for candidate_name in Self::par2_verification_candidate_names(file_verification) {
                let Some((file_id, needs_refresh)) = by_name.get(&candidate_name).copied() else {
                    continue;
                };
                if renamed || needs_refresh || was_rewritten {
                    // A file can match more than one description name; it is
                    // rewritten if any of them says so.
                    *matched.entry(file_id).or_insert(false) |= was_rewritten;
                }
                break;
            }
        }

        let mut targets = matched.into_iter().collect::<Vec<_>>();
        targets.sort_by_key(|(file_id, _)| file_id.file_index);
        targets
    }

    pub(super) fn role_refreshes_archive_topology(role: &weaver_model::files::FileRole) -> bool {
        matches!(
            role,
            weaver_model::files::FileRole::RarVolume { .. }
                | weaver_model::files::FileRole::SevenZipArchive
                | weaver_model::files::FileRole::SevenZipSplit { .. }
                | weaver_model::files::FileRole::SplitFile { .. }
                | weaver_model::files::FileRole::ZipArchive
                | weaver_model::files::FileRole::TarArchive
                | weaver_model::files::FileRole::TarGzArchive
                | weaver_model::files::FileRole::TarBz2Archive
                | weaver_model::files::FileRole::TarXzArchive
                | weaver_model::files::FileRole::GzArchive
                | weaver_model::files::FileRole::DeflateArchive
                | weaver_model::files::FileRole::BrotliArchive
                | weaver_model::files::FileRole::ZstdArchive
                | weaver_model::files::FileRole::Bzip2Archive
                | weaver_model::files::FileRole::XzArchive
        )
    }

    pub(super) fn archive_topology_needs_refresh(
        &self,
        job_id: JobId,
        file: &crate::jobs::assembly::FileAssembly,
        role: &weaver_model::files::FileRole,
    ) -> bool {
        let Some(set_name) = self.classified_archive_set_name_for_file(job_id, file) else {
            return false;
        };

        if matches!(role, weaver_model::files::FileRole::RarVolume { .. }) {
            return self
                .rar_sets
                .get(&(job_id, set_name))
                .is_none_or(|set_state| set_state.plan.is_none());
        }

        self.jobs
            .get(&job_id)
            .is_some_and(|state| state.assembly.archive_topology_for(&set_name).is_none())
    }

    pub(super) fn insert_par2_name_candidates(
        by_name: &mut HashMap<String, (NzbFileId, bool)>,
        name: &str,
        file_id: NzbFileId,
        needs_refresh: bool,
    ) {
        for candidate in Self::name_and_basename_candidates(name) {
            by_name.entry(candidate).or_insert((file_id, needs_refresh));
        }
    }

    pub(super) fn par2_verification_candidate_names(
        file_verification: &par2_rs::verify::FileVerification,
    ) -> Vec<String> {
        let mut candidates = Self::name_and_basename_candidates(&file_verification.filename);
        if let par2_rs::verify::FileStatus::Renamed(path) = &file_verification.status {
            Self::push_name_candidate(&mut candidates, &path.to_string_lossy());
            if let Some(filename) = path.file_name() {
                Self::push_name_candidate(&mut candidates, &filename.to_string_lossy());
            }
        }
        candidates
    }

    pub(super) fn name_and_basename_candidates(name: &str) -> Vec<String> {
        let mut candidates = Vec::new();
        Self::push_name_candidate(&mut candidates, name);
        if let Some(basename) = name.rsplit(['/', '\\']).next()
            && basename != name
        {
            Self::push_name_candidate(&mut candidates, basename);
        }
        candidates
    }

    pub(super) fn push_name_candidate(candidates: &mut Vec<String>, name: &str) {
        if name.is_empty() || candidates.iter().any(|candidate| candidate == name) {
            return;
        }
        candidates.push(name.to_string());
    }
}
