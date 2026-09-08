//! Continuation of the `impl Pipeline` block from `finalize/check.rs`.
//! Split out mechanically to keep the parent file readable; no behavior lives here
//! that is not simply a method of the same type.

use super::*;

impl Pipeline {
    pub(super) async fn check_rar_job_completion(&mut self, job_id: JobId) {
        let set_names = self.rar_set_names_for_job(job_id);
        if set_names.is_empty() {
            return;
        }

        if self.has_active_rar_workers(job_id) {
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
                    } else if plan.ready_members.iter().any(|ready_member| {
                        self.rar_member_can_start_extraction(job_id, set_name, &ready_member.name)
                    }) {
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

    pub(in crate::pipeline) fn only_archive_residuals_or_loaded_par2_index_are_incomplete(
        &self,
        job_id: JobId,
    ) -> bool {
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
        // An index that can still arrive is not a residual: it may carry a
        // recovery set of its own, and this job would finalize without ever
        // verifying or repairing what that set covers. Only an index nothing
        // can deliver any more is furniture.
        let metadata_discovery_closed = self.par2_metadata_discovery_closed(job_id);
        let mut saw_incomplete = false;

        for file in state.assembly.files() {
            if file.is_complete() {
                continue;
            }
            match self.classified_role_for_file(job_id, file) {
                weaver_model::files::FileRole::Par2 {
                    is_index: false, ..
                } => {}
                weaver_model::files::FileRole::Par2 { is_index: true, .. }
                    if par2_loaded && metadata_discovery_closed =>
                {
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
        // Once PAR2 has ruled on this job, a data file the recovery set never
        // described stops being something this gate can act on: there is no
        // repair for it and no download left to try, so counting it here only
        // keeps the job out of finalization for good. That is the livelock
        // removing the veto exposed — an unprotected `.nfo` three articles
        // short kept `needs_completion_repair_evaluation` true, and the gate
        // re-ran a full authoritative pass over a gigabyte every two seconds,
        // forever.
        //
        // Both oracles stop counting at the same place. NZBGet's health
        // failure requires par to have been *skipped*; SABnzbd's verdict is the
        // PAR result alone. A file PAR2 *does* describe still counts, because
        // for that one a verdict and a repair are genuinely still possible.
        let has_incomplete_data_files = if self.par2_verified.contains(&job_id) {
            self.incomplete_par2_protected_data_file_count(job_id) > 0
        } else {
            complete_data_files < total_data_files
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
            // If no data files registered yet but there are still segments queued,
            // downloads haven't really started — don't prematurely leave Downloading.
            if total_data_files == 0 && queued_downloads {
                return;
            }
        }

        if matches!(current_status, JobStatus::QueuedRepair) {
            if self.active_repair_jobs() == 0 {
                self.promote_queued_repairs();
            }
            return;
        }

        // The strict half of the direct-unpack settle. The lenient half ran when
        // the download drained, but decode results for the last articles are
        // processed after that point, so it deliberately left any part the
        // assembly could not yet describe alone. By here those commits have
        // landed, so a part still without a length is one that will never have
        // one — and its chase ends by name instead of parking forever.
        // Idempotent: later completion checks find nothing left to settle.
        self.settle_direct_unpack_at_completion(job_id);

        self.reapply_promoted_recovery_queue(job_id);
        // Restored jobs retain completed bytes but not the bounded decode
        // prefix cache. Inspect a single header here, never during startup,
        // so an obfuscated PAR2 carrier can rejoin normal discovery.
        self.probe_restored_par2_headers(job_id).await;

        let par2_bypassed = self.par2_bypassed.contains(&job_id);
        if !par2_bypassed
            && self.job_spec_has_par2_file(job_id)
            && !self.par2_metadata_discovery_closed(job_id)
            && self.promote_par2_metadata(job_id)
        {
            info!(
                job_id = job_id.0,
                "waiting for bounded PAR2 metadata discovery before finalization"
            );
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
            return;
        }
        if !par2_bypassed && !self.served_par2_set_needs_reconciliation(job_id) {
            self.activate_next_par2_gate_set(job_id);
            let has_settled_set = self.par2_runtime(job_id).is_some_and(|runtime| {
                runtime
                    .ordered_set_ids()
                    .into_iter()
                    .any(|set_id| runtime.set_runtime(set_id).is_some_and(|set| set.settled))
            });
            if has_settled_set {
                // Extraction topology maintenance may clear the compatibility
                // bit without discarding the per-set answers. Recompute from
                // those answers here rather than asking a clean set to run
                // again.
                self.mark_par2_verified(job_id).await;
            }
        }

        // A direct set whose job carries no PAR2 set to verify
        // against — bypassed, or no recovery article ever landed — would
        // otherwise wait forever for a verdict that is not coming. Asked here,
        // once per completion check, because this is where the job's PAR2 state
        // is settled; `mark_par2_verified` covers the verdict case.
        self.finalize_ready_direct_sets(job_id).await;
        // A tolerated extraction detached from that pass. Its set is
        // byte-complete and gate-passed but not committed — neither finalized
        // nor a conventional archive — and nothing below may judge the job
        // until it is one or the other. The ticket's completion re-enters the
        // finalization pass and schedules this check again.
        if self.direct_tolerated_in_flight.contains_key(&job_id) {
            return;
        }
        // A demotion sweep detached from the demotion that started it. Its
        // volumes are half materialized: the routed bytes are still in member
        // partials, the volume files are being written, and neither the direct
        // coverage row nor the legacy floors describe the set yet. Judging the
        // job here would read that intermediate state as "articles are missing"
        // and fail a job whose bytes are all present. The ticket's completion
        // applies the bookkeeping and schedules this check again.
        if self.direct_demotion_in_flight.contains_key(&job_id) {
            return;
        }

        if let Some(message) = self.aggregate_par2_failure_message(job_id) {
            self.fail_job(job_id, message);
            return;
        }
        let par2_loaded = !self.par2_servable_set_ids(job_id).is_empty();
        let download_pipeline_exhausted = !self.job_has_pending_download_pipeline_work(job_id);
        if download_pipeline_exhausted {
            self.emit_download_pipeline_drained_if_pending(job_id);
            if has_incomplete_data_files {
                // The download pass is over and files are still short of
                // segments: this is where "cannot be assembled from articles"
                // becomes a fact rather than a race with work still in flight.
                self.note_incomplete_files_after_download_drain(job_id);
            }
        }
        let only_rar_archives = self.job_has_only_rar_archives(job_id);

        // A RAR set that PAR2 owes a verdict on, in the two shapes that reach
        // this while the downloads have not drained. Ordinarily an incomplete
        // data file defers validation until they do, which is right — PAR2
        // cannot tell a file still arriving from a damaged one — but in both
        // of these the wait is for something that is not coming, and the
        // authoritative pass is what says so.
        // Hoisted here by the 0.7.9 port: the repair-readiness predicates below
        // need it, and it used to be defined further down at "Step 2".
        let has_crc_failures = self
            .failed_extractions
            .get(&job_id)
            .is_some_and(|failed| !failed.is_empty());
        // A clean PAR2 verdict says the described files hashed correctly. It does
        // not say the archives open. When extraction fails afterwards *and* the
        // set is left waiting on a volume, that verdict is stale evidence and
        // PAR2 has to be allowed to rule again.
        //
        // Declining 0.7.9's relaxation outright (2efa19d9) livelocked every PAR2
        // repair scenario in the corpus. `has_crc_failures` does re-open repair
        // *evaluation* — which is why the unit suite was satisfied — but every
        // PAR2 route inside that block is gated on `par2_validation_needed`, so
        // the check re-entered forever with nothing able to act: 1865 identical
        // completion checkpoints on one job, no repair, no verdict.
        //
        // Narrower than 0.7.9's blanket `!verified`, deliberately, and on the
        // *absent*-volumes predicate rather than the waiting phase: a job that
        // merely finalized its direct sets has a conventional failed member and
        // no set waiting on anything, so its verdict stands and the
        // repair-first branch stays skipped
        // (`a_finalized_direct_sets_volumes_are_not_missing_on_a_later_par2_pass`),
        // and a swap-corrected set whose volumes are all present is left to the
        // retry frontier rather than dragged back through PAR2.
        let par2_verdict_stale_after_failed_extraction = self.par2_verified.contains(&job_id)
            && has_crc_failures
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id);
        if par2_verdict_stale_after_failed_extraction
            && let Some(runtime) = self.par2_runtime.get_mut(&job_id)
            && let Some(set_runtime) = runtime.served_mut()
        {
            // A reopened verdict is owed a fresh pass, so the post-verdict
            // re-entry budget starts over with it.
            set_runtime.post_verdict_reconcile_attempts = 0;
        }
        let served_set_settled = self.par2_served_set_id(job_id).is_some_and(|set_id| {
            self.par2_runtime(job_id)
                .and_then(|runtime| runtime.set_runtime(set_id))
                .is_some_and(|set_runtime| {
                    (set_runtime.settled && set_runtime.failure.is_none())
                        // A retained job-level verdict predating per-set
                        // runtime answers is already authoritative for its
                        // one served set. Runtime reconstruction supplies the
                        // set-local answer on the next metadata replay.
                        || self.par2_verified.contains(&job_id)
                })
        });
        let par2_verdict_open = !served_set_settled || par2_verdict_stale_after_failed_extraction;
        let par2_may_still_rule = par2_loaded && !par2_bypassed && par2_verdict_open;
        let extraction_settled = !self.job_has_active_extraction_tasks(job_id);
        // One: extraction was attempted and failed. The archives cannot be
        // opened now, and the reason may well be the volume that never came.
        // This is the same question the scheduler asks before it latches the
        // failed member out, and it is asked through the same predicate so the
        // two cannot answer differently — a latch with no verdict coming is a
        // stalled job.
        let failed_rar_par2_repair_ready = self.par2_recovery_evaluation_pending(job_id);
        // Two: extraction was never attempted, because a volume is missing.
        // Nothing lands in `failed_extractions` for this shape — there was no
        // failure, only an absence — so it needs naming separately or a job
        // whose interior volume never posted waits forever with the recovery
        // blocks that would rebuild it sitting right there.
        let missing_rar_volume_par2_repair_ready = par2_may_still_rule
            && extraction_settled
            && self.job_has_live_rar_waiting_for_absent_volumes(job_id)
            && (self.par2_served_set_id(job_id).is_some_and(|set_id| {
                self.recovery_blocks_available_or_targeted(job_id, set_id) > 0
            }) || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.recovery_queue.has_recovery_work()));
        // Three: the 7z shape of two. A split 7z set has no header chain, so
        // the scheduler never parks it as WaitingForVolumes and nothing above
        // names its hole: an interior part missing leaves the topology short
        // of ready for good, and a missing last part is not even known to be
        // missing — the topology counted what it saw, extraction opens a
        // truncated set and fails, and the strong-decode fast path had already
        // settled the set as clean. The recovery-availability gate is the RAR
        // arm's: a set with no blocks to spend is short, not repair-ready.
        let missing_sevenz_volume_par2_repair_ready = par2_may_still_rule
            && extraction_settled
            && self.job_has_sevenz_set_waiting_for_absent_volumes(job_id)
            && (self.par2_served_set_id(job_id).is_some_and(|set_id| {
                self.recovery_blocks_available_or_targeted(job_id, set_id) > 0
            }) || self
                .jobs
                .get(&job_id)
                .is_some_and(|state| state.recovery_queue.has_recovery_work()));
        let missing_archive_volume_par2_repair_ready =
            missing_rar_volume_par2_repair_ready || missing_sevenz_volume_par2_repair_ready;
        // 0.8 only: a direct set still taking articles has holes where its
        // outstanding ranges will go, and PAR2 cannot tell a hole from
        // corruption. Declaring a verdict owed while one is filling walks the
        // job into the authoritative branch, which then defers on exactly this
        // condition and returns — so the pass never runs and the escape has
        // achieved nothing but a later re-check.
        //
        // Named for the RAR case it was written for; the 7z absent-part arm
        // rides the same flag because every consumer below wants the same
        // answer from it — a verdict is owed on an archive set that is short a
        // volume nothing but recovery blocks can produce.
        let rar_par2_repair_ready = (failed_rar_par2_repair_ready
            || missing_archive_volume_par2_repair_ready)
            && self.direct_sets_ready_for_authoritative_par2(job_id);

        let par2_primary_payload_ready =
            !has_incomplete_data_files || download_pipeline_exhausted || rar_par2_repair_ready;
        let par2_validation_needed = par2_loaded
            && !par2_bypassed
            // The other half of the coupled pair above. 0.7.9 relaxes this to
            // `(rar_par2_repair_ready || !verified)`; this is the same
            // relaxation narrowed to the state that actually needs it — a
            // verified job whose extraction failed and whose set is still
            // waiting on a volume. Moving only one of the two halves does
            // nothing: `rar_par2_repair_ready` cannot become true while
            // `par2_may_still_rule` is false, and validation cannot run while
            // this is false.
            && par2_verdict_open
            && par2_primary_payload_ready
            // The residuals check reads a job still missing archive pieces as
            // nothing to validate yet. That is the very state a repair-ready
            // RAR set is in, so it cannot be what turns validation away.
            && (rar_par2_repair_ready
                || !self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id));
        let rar_waiting_for_missing_volumes = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_live_rar_waiting_for_missing_volumes(job_id);
        let pending_rar_refresh = download_pipeline_exhausted
            && only_rar_archives
            && self.job_has_pending_rar_refresh_for_current_sets(job_id);

        // Step 2: Check for CRC failures that need PAR2 repair.
        let clean_par2_integrity_gate = self.clean_par2_integrity_gate(job_id);
        let archive_extraction_applicable = self.extraction_readiness_for_job(job_id)
            != ExtractionReadiness::NotApplicable
            || only_rar_archives;
        // A direct-unpack chase gated on damage is evidence already in hand:
        // the recovery data has named a slice of this archive as wrong, and
        // the chase is parked on that slice waiting for repair. `StrongDecode`
        // above is a claim about the archive *type* — that a clean extraction
        // would prove integrity — and a claim cannot stand against evidence.
        // If it did, the skip below would settle the set as clean, no repair
        // would ever be summoned, and the chase would wait out its whole
        // consumption deadline on vouches that were never coming.
        let direct_unpack_gated_sets = self.direct_unpack_gated_sets(job_id);
        // Same shape as the gated chase above, from the other seam: a set
        // whose bytes were already found wrong — a part CRC32 that did not
        // match what was routed for it, and every later fact of the same kind
        // — is evidence, and `StrongDecode` is a claim about the archive
        // *type*. Without this the fast path would refuse both the
        // authoritative and the quick pass for a stored RAR set (the quick
        // one is gated on `has_crc_failures`, which nothing has produced yet
        // because extraction is being held), and the job would sit with its
        // damage on record and no verdict coming.
        // A CRC-rejected chase has already been tainted, so it no longer
        // appears in `direct_unpack_gated_sets`. Keep the measured mismatch
        // authoritative even after that worker has gone away.
        let known_file_crc_damage = self.par2_runtime(job_id).is_some_and(|runtime| {
            runtime
                .completed_checksums
                .iter()
                .any(|(file_id, checksum)| {
                    self.expected_file_crcs
                        .get(file_id)
                        .is_some_and(|expected| *expected != checksum.crc32)
                })
        });
        let known_archive_damage =
            self.archive_extraction_held_for_known_damage(job_id) || known_file_crc_damage;
        let authoritative_par2_verification_owed = rar_par2_repair_ready
            || known_archive_damage
            || has_crc_failures
            || (has_incomplete_data_files && download_pipeline_exhausted)
            || rar_waiting_for_missing_volumes
            || matches!(current_status, JobStatus::Repairing)
            || matches!(
                clean_par2_integrity_gate,
                CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None
            );
        let authoritative_par2_verification_needed = par2_validation_needed
            && (authoritative_par2_verification_owed || !direct_unpack_gated_sets.is_empty());
        if authoritative_par2_verification_needed && !authoritative_par2_verification_owed {
            info!(
                job_id = job_id.0,
                gated_sets = ?direct_unpack_gated_sets,
                "a gated direct unpack chase forces the authoritative PAR2 pass — recovery data \
                 reported damage, so the clean strong-decode verdict cannot stand"
            );
        }
        // Shared by every fast path that skips the authoritative pass, so the
        // live short-circuit can never fire where the quick path would be
        // refused.
        let clean_par2_integrity_gate_allows_fast_path = match clean_par2_integrity_gate {
            CleanPar2IntegrityGate::StrongDecode => {
                only_rar_archives && (has_crc_failures || rar_waiting_for_missing_volumes)
            }
            CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => true,
        };
        // What the quick pass can be blocked by: a file the recovery set
        // *describes*, not any short file in the posting.
        //
        // Post-verdict this is already the question `has_incomplete_data_files`
        // asks; asking it the same way before a verdict exists is what stops a
        // clean payload beside a short unprotected file from paying for a
        // whole-set read every time. Nothing is loosened by it: the quick pass
        // skips incomplete files outright, so a described file that is short
        // lands in its `unresolved` bucket and the pass declines, exactly as it
        // does today. Only a file the set never describes — which the pass
        // could not have spoken for either way — stops standing in the way.
        let described_data_files_incomplete = if par2_loaded {
            self.incomplete_par2_protected_data_file_count(job_id) > 0
        } else {
            has_incomplete_data_files
        };
        let quick_par2_verification_allowed = par2_validation_needed
            && !matches!(current_status, JobStatus::Repairing)
            // A set waiting on a volume that never posted needs the
            // *authoritative* analyzer: only that names the exact missing
            // blocks a recovery promotion has to target, and the quick pass has
            // nothing to answer from — those bytes are absent, not wrong.
            //
            // Narrower than 0.7.9's, deliberately. A *failed* extraction whose
            // files are all present is a case 0.8's quick pass settles on its
            // own — swap correction, and the eager-delete retry frontier — and
            // forcing the authoritative pass there loses both.
            && !missing_archive_volume_par2_repair_ready
            && (!described_data_files_incomplete || !download_pipeline_exhausted)
            // The pass that consumes a parked damaged-path verdict is a second
            // entry into this gate: the first submitted the read and returned.
            // The quick pass exists to make the authoritative read unnecessary,
            // and that read has already happened, so running it again is a
            // repeat of work whose answer is superseded before it is asked.
            && !self.par2_analysis_results.contains_key(&job_id)
            && clean_par2_integrity_gate_allows_fast_path;
        let needs_completion_repair_evaluation = has_crc_failures
            || (has_incomplete_data_files && download_pipeline_exhausted)
            || rar_waiting_for_missing_volumes
            || par2_validation_needed;
        let exhausted_rar_activity = if download_pipeline_exhausted && only_rar_archives {
            let inflight_extractions = self
                .inflight_extractions
                .get(&job_id)
                .map_or(0, HashSet::len);
            let has_active_rar_workers = self.has_active_rar_workers(job_id);
            Some((
                inflight_extractions,
                has_active_rar_workers,
                has_active_rar_workers || inflight_extractions > 0,
            ))
        } else {
            None
        };
        let has_exhausted_rar_active_extraction_tasks =
            exhausted_rar_activity.is_some_and(|(_, _, active)| active);

        if download_pipeline_exhausted && only_rar_archives {
            let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
            let (inflight_extractions, has_active_rar_workers, has_active_extraction_tasks) =
                exhausted_rar_activity.unwrap_or((0, false, false));
            let only_archive_residuals =
                self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id);
            let mut rar_set_state = self
                .rar_sets
                .iter()
                .filter(|((rar_job_id, _), _)| *rar_job_id == job_id)
                .map(|((_, set_name), set_state)| summarize_rar_set_phase(set_name, set_state))
                .collect::<Vec<_>>();
            rar_set_state.sort();
            let mut failed_extractions = self
                .failed_extractions
                .get(&job_id)
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            failed_extractions.sort();

            info!(
                job_id = job_id.0,
                status = ?current_status,
                complete_data_files,
                total_data_files,
                failed_bytes,
                par2_loaded,
                has_crc_failures,
                rar_waiting_for_missing_volumes,
                pending_rar_refresh,
                has_active_rar_workers,
                inflight_extractions,
                has_active_extraction_tasks,
                only_archive_residuals,
                queued_downloads = promoted_recovery.download_queue_len,
                download_queue_has_recovery = promoted_recovery.download_queue_has_recovery,
                queued_promoted_recovery = promoted_recovery.download_queue_promoted_recovery,
                parked_recovery = promoted_recovery.recovery_queue_len,
                parked_promoted_recovery = promoted_recovery.parked_promoted_recovery,
                promoted_par2_files = promoted_recovery.promoted_par2_files,
                incomplete_promoted_par2_files = promoted_recovery.incomplete_promoted_par2_files,
                active_promoted_downloads = promoted_recovery.active_promoted_downloads,
                pending_promoted_retries = promoted_recovery.pending_promoted_retries,
                pending_promoted_decode = promoted_recovery.pending_promoted_decode,
                active_promoted_decodes = promoted_recovery.active_promoted_decodes,
                write_buffered_promoted_recovery =
                    promoted_recovery.write_buffered_promoted_recovery,
                unavailable_promoted_recovery_segments =
                    promoted_recovery.unavailable_promoted_recovery_segments,
                promoted_recovery_pending = promoted_recovery.has_pending_work(),
                failed_extractions = ?failed_extractions,
                rar_set_state = ?rar_set_state,
                "RAR completion checkpoint"
            );
        }

        if has_incomplete_data_files
            && !download_pipeline_exhausted
            && !self.job_has_active_extraction_tasks(job_id)
            // ...unless PAR2 owes this job's RAR sets a verdict. Waiting for
            // downloads that are not coming is how such a job stalls, and this
            // return is what keeps PAR2 from ever being asked.
            && !rar_par2_repair_ready
        {
            return;
        }

        if pending_rar_refresh {
            debug!(
                job_id = job_id.0,
                "deferring completion — RAR topology refresh pending"
            );
            return;
        }

        // Standalone `.rev` files can rebuild a volume that never posted just as
        // well as one that failed CRC. Gating this on `has_crc_failures` alone
        // meant a set whose hole was known *before* any extraction was tried
        // — the common case, since the topology usually knows the missing
        // index from the neighbouring volumes' headers — went straight to "no
        // retryable work remains" without the recovery volumes ever being read.
        if download_pipeline_exhausted
            && only_rar_archives
            && (has_crc_failures || rar_waiting_for_missing_volumes)
            && !self.job_has_active_extraction_tasks(job_id)
            && self.job_has_rar_recovery_volume_files(job_id)
        {
            match self.try_restore_rar_recovery_volumes(job_id).await {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => warn!(
                    job_id = job_id.0,
                    error = %error,
                    "RAR recovery-volume restore failed; continuing with normal repair evaluation"
                ),
            }
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

        if let Some(error) = self.ownerless_live_rar_plan_error_for_job(job_id) {
            self.fail_job(job_id, error);
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && !has_crc_failures
            && !par2_validation_needed
            && !has_exhausted_rar_active_extraction_tasks
            && self.job_has_idle_startable_rar_work(job_id)
            && matches!(
                current_status,
                JobStatus::Downloading | JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "restarting idle RAR extraction work"
            );
            self.try_rar_extraction(job_id).await;
            return;
        }

        if !has_crc_failures
            && self.only_archive_residuals_or_loaded_par2_index_are_incomplete(job_id)
        {
            self.finalize_completed_archive_job(job_id).await;
            return;
        }

        if download_pipeline_exhausted
            && only_rar_archives
            && has_crc_failures
            && !has_exhausted_rar_active_extraction_tasks
            && matches!(
                current_status,
                JobStatus::QueuedExtract | JobStatus::Extracting
            )
        {
            info!(
                job_id = job_id.0,
                status = ?current_status,
                "normalizing idle RAR extraction status before repair evaluation"
            );
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }

        if rar_waiting_for_missing_volumes && self.job_has_incoherent_rar_waiting_state(job_id) {
            info!(
                job_id = job_id.0,
                "healing incoherent RAR waiting state before PAR2 verification"
            );
            self.retry_archive_extraction_after_verify_or_repair(job_id)
                .await;
            return;
        }

        if needs_completion_repair_evaluation && !par2_bypassed {
            // Restored from 0.7.9 after an e2e run showed the cost of removing
            // it: one job re-ran `par2 damaged-path analysis` 21 times while
            // waiting for its promoted recovery to arrive, at ~64 slow par2
            // file scans, starving 75 other jobs into harness timeouts. The
            // analysis is what is expensive, and it has to be skipped *before*
            // it runs — 0.8's `job_has_promoted_recovery_pipeline_work` guard
            // below is too late to prevent the rescan.
            // Each pass of this analysis is a full, slow PAR2 scan, so the only
            // question that matters is whether re-running it can learn anything
            // new. Four e2e runs mapped the edges:
            //
            //  - Defer while the *parked* pool (`recovery_queue`) is non-empty
            //    and the job deadlocks: it is this analysis that promotes parked
            //    blocks, so the pool never drains (~1800 identical completion
            //    checkpoints per job, every PAR2-repair scenario in the corpus).
            //  - Defer on any narrower signal — recovery on the wire, parked
            //    work after a promotion — and heavy damage storms: the verdict
            //    keeps changing while the payload is still arriving, so the
            //    check re-scans on every tick that slips through (30+ slow
            //    scans, suite starved to 21 of 92).
            //
            // The scan can learn something exactly twice: once before any wave
            // has been promoted (it is the pass that decides what to promote),
            // and once after everything that could change its answer has
            // landed. In between — the wave still arriving, or the payload
            // still filling the very holes being counted — the verdict cannot
            // move, and the pass is pure cost. `job_has_pending_download_
            // pipeline_work` deliberately excludes the parked pool, so this
            // cannot re-create the deadlock above: a job whose only remaining
            // work is parked recovery is not "pending" here, analyses run, and
            // promotion drains the pool.
            let promoted_recovery_state = self.promoted_recovery_pipeline_state(job_id);
            if rar_par2_repair_ready
                && promoted_recovery_state.promoted_par2_files > 0
                && (promoted_recovery_state.incomplete_promoted_par2_files > 0
                    || self.job_has_pending_download_pipeline_work(job_id))
            {
                debug!(
                    job_id = job_id.0,
                    promoted_par2_files = promoted_recovery_state.promoted_par2_files,
                    incomplete_promoted_par2_files =
                        promoted_recovery_state.incomplete_promoted_par2_files,
                    "deferring repair evaluation — promoted recovery or payload is still arriving"
                );
                return;
            }
            if self.job_has_promoted_recovery_pipeline_work(job_id, "verify") {
                return;
            }

            let has_active_extraction_tasks = if download_pipeline_exhausted && only_rar_archives {
                has_exhausted_rar_active_extraction_tasks
            } else {
                self.job_has_active_extraction_tasks(job_id)
            };
            if has_active_extraction_tasks {
                info!(
                    job_id = job_id.0,
                    "deferring verify — active extraction workers"
                );
                return;
            }

            // Every promoted recovery wave has settled by here, so a volume
            // still short of articles is short for good. Its surviving packets
            // are read back before the set is cloned for the passes below:
            // whichever of them runs, its view of how much recovery this job
            // has is the merged set, and so is the fail-fast arithmetic that
            // decides whether to wait, repair, or give up.
            self.salvage_partial_promoted_recovery_volumes(job_id).await;

            // Latched, so an indexless recovery set is named once rather than
            // on every entry to this gate.
            self.warn_unservable_recovery_sets_once(job_id);

            let par2_set = self.par2_set(job_id).cloned();
            let par2_set_id = self.par2_served_set_id(job_id);

            if let Some(set_id) = par2_set_id
                && !self.demoted_materializations_ready_for_par2(job_id, set_id)
            {
                debug!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    "deferring PAR2 settlement — a demoted direct set is still materializing"
                );
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                return;
            }

            if let Some(set_id) = par2_set_id
                && self.par2_set_is_absent_from_job(job_id, set_id)
            {
                let index_filename = self
                    .par2_runtime(job_id)
                    .and_then(|runtime| runtime.set_runtime(set_id))
                    .map(|set_runtime| set_runtime.summary.index_filename.clone())
                    .filter(|filename| !filename.is_empty())
                    .unwrap_or_else(|| set_id.to_string());
                info!(
                    job_id = job_id.0,
                    recovery_set_id = %set_id,
                    index_filename = %index_filename,
                    "skipping absent PAR2 recovery set with no bound payload bytes"
                );
                let _ = self
                    .settle_par2_set(
                        job_id,
                        set_id,
                        Par2SetSettlementReason::AbsentUnboundPayload,
                    )
                    .await;
                self.continue_after_aggregate_clean_par2_settlement(
                    job_id,
                    has_crc_failures,
                    archive_extraction_applicable,
                    "skipped absent PAR2 recovery set",
                )
                .await;
                return;
            }

            if par2_set.is_some() {
                // What the dual-CRC grid managed to claim off the download for
                // this job, recorded once at the verification gate — the point
                // where its work is finished and every arm below is about to
                // read it. `blocks_claimed` is the read the download path
                // already paid for; `articles_without_usable_segments` is the
                // shortfall, articles whose yEnc segmentation could not be
                // rebased onto the block grid and which therefore claim
                // nothing.
                debug!(
                    job_id = job_id.0,
                    blocks_claimed_in_stream = self.block_crcs.blocks_derived(),
                    articles_without_usable_segments = self.block_crcs.rebased_articles(),
                    "in-stream block verification diagnostics"
                );
            }

            // Partial quick evidence survives past its own arm: when the flow
            // below decides an authoritative pass is owed, this is what lets
            // that pass read only the unproven remainder instead of the set.
            let mut quick_partial: Option<QuickPar2PartialEvidence> = None;
            if quick_par2_verification_allowed && let Some(par2_set) = par2_set.as_ref() {
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                Self::trip_par2_verification_started_failpoint();
                match self
                    .quick_verify_par2_with_placement(
                        job_id,
                        Arc::clone(par2_set),
                        working_dir.clone(),
                    )
                    .await
                {
                    Ok(QuickPar2Outcome::Full(verification, placement_plan, evidence)) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification passed for clean exhausted job"
                        );
                        self.finish_clean_par2_verification(
                            job_id,
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID"),
                            working_dir.clone(),
                            CleanPar2Verification {
                                verification,
                                placement_plan,
                                slice_size: par2_set.slice_size,
                                verification_mode: evidence.verification_mode(),
                                reconcile_context: "clean PAR2 quick verification",
                                retry_message:
                                    "cleared failed extractions after quick verify — retrying",
                            },
                            has_crc_failures,
                            archive_extraction_applicable,
                        )
                        .await;
                        return;
                    }
                    Ok(QuickPar2Outcome::Partial(partial)) => {
                        info!(
                            job_id = job_id.0,
                            proven = partial.proven.len(),
                            unproven = partial.unproven_recovery.len(),
                            "quick PAR2 verification proved part of the set — any \
                             authoritative pass below reads only the remainder"
                        );
                        quick_partial = Some(partial);
                    }
                    Ok(QuickPar2Outcome::Inconclusive) => {
                        info!(
                            job_id = job_id.0,
                            "quick PAR2 verification was inconclusive — falling back to authoritative verify"
                        );
                    }
                    Err(message) => {
                        let set_id =
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID");
                        self.finish_par2_set_failure(job_id, set_id, message).await;
                        return;
                    }
                }
            }

            if par2_validation_needed && !authoritative_par2_verification_needed {
                match clean_par2_integrity_gate {
                    CleanPar2IntegrityGate::StrongDecode => {
                        info!(
                            job_id = job_id.0,
                            "skipping authoritative PAR2 verify for clean exhausted strong-decode job"
                        );

                        self.try_deobfuscate_files_with_par2(job_id).await;
                        self.retry_par2_authoritative_identity(job_id).await;
                        let set_id =
                            par2_set_id.expect("loaded PAR2 set has an active recovery-set ID");
                        let slice_size = par2_set
                            .as_ref()
                            .expect("PAR2 validation has a parsed recovery set")
                            .slice_size;
                        let _ = self
                            .settle_par2_set(
                                job_id,
                                set_id,
                                Par2SetSettlementReason::Clean {
                                    slice_size,
                                    verification_mode: CleanPar2VerificationMode::StrongDecode,
                                },
                            )
                            .await;

                        if !self.par2_verified.contains(&job_id) {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }

                        if archive_extraction_applicable {
                            self.retry_archive_extraction_after_verify_or_repair(job_id)
                                .await;
                            return;
                        }

                        self.reconcile_job_progress(job_id).await;
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    CleanPar2IntegrityGate::WeakTransform | CleanPar2IntegrityGate::None => {}
                }
            }

            if let Some(par2_set) = par2_set {
                let set_id = par2_set.recovery_set_id;
                let working_dir = self.jobs.get(&job_id).unwrap().working_dir.clone();
                // Two direct-store preconditions for *any*
                // authoritative pass below, whichever branch it takes. Both are
                // no-ops for a job with no live direct set, so a conventional
                // job reaches the same code it always did.
                //
                // A set that is still receiving articles has holes where its
                // outstanding ranges will go, and PAR2 cannot tell a hole from
                // corruption — so the pass waits for the same thing the branch
                // above waits for in `par2_primary_payload_ready`: the payload,
                // or the download pipeline draining. `needs_completion_repair_
                // evaluation` can be true well before either (another set's
                // extraction failing is enough), which is how a healthy
                // mid-download set would otherwise be demoted for damage that
                // is just bytes in flight.
                if !self.direct_sets_ready_for_authoritative_par2(job_id) {
                    debug!(
                        job_id = job_id.0,
                        "deferring PAR2 verification — a direct set is still downloading"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A volume with no unambiguous PAR2 identity cannot be put
                // behind the overlay *or* attributed back to its set
                // afterwards, so it leaves direct mode before the pass rather
                // than being discovered as unattributable damage inside it.
                if self.demote_unbindable_direct_sets(job_id).await {
                    self.schedule_job_completion_check(job_id);
                    return;
                }
                // A live direct set reaches this branch as a matter of course,
                // not as defence in depth: it contributes nothing to
                // `clean_par2_integrity_gate` — a direct set never enters the
                // archive topology — so a `None` gate sends it straight here,
                // damaged or not. The repairer cannot read a virtual volume, so
                // before it is allowed to force a whole-set materialization the
                // sets get one direct-aware verdict of their own: damage repairs
                // in place, and a clean verdict skips the repairer so the
                // ordinary verify path below can record it.
                let mut run_par2_repairer = authoritative_par2_verification_needed;
                // Stashed rather than discarded when the direct gate reaches
                // `Clean`: the verdict below is the same one the ordinary
                // whole-set pass would have reached over the same virtual
                // volumes, and asking that pass to read them again would be
                // the second whole-set read this gate exists to avoid. See
                // where `direct_verdict` is consumed, further down, for how it
                // stands in for `verify_par2_with_placement`.
                let mut direct_verdict: Option<par2_rs::VerificationResult> = None;
                if authoritative_par2_verification_needed {
                    match self
                        .resolve_direct_sets_before_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                        )
                        .await
                    {
                        _ if self.shared_state.is_job_cancellation_requested(job_id) => {
                            return;
                        }
                        DirectPar2Resolution::Repaired | DirectPar2Resolution::Demoted => {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        DirectPar2Resolution::Clean(verification) => {
                            run_par2_repairer = false;
                            direct_verdict = Some(*verification);
                        }
                        DirectPar2Resolution::Pending => return,
                        DirectPar2Resolution::Deferred => {
                            // The same wait the analysis below performs when it
                            // promotes recovery, reported the same way: the job
                            // is downloading again, because that is literally
                            // what it is doing.
                            //
                            // Deliberately *not* re-armed. Nothing this gate can
                            // do moves the answer — the sets are waiting on
                            // articles — and each lap costs a full PAR2 scan.
                            // The re-arm comes from the recovery itself: a
                            // completing PAR2 file merges its slices and checks
                            // the job, which is the one event that changes the
                            // verdict.
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );
                            return;
                        }
                        DirectPar2Resolution::Unresolved => {}
                    }
                }
                if run_par2_repairer {
                    // The filesystem analysis below is a whole-directory
                    // authoritative read, handed to a blocking worker rather
                    // than run here. Running it while this job still has wire
                    // work in flight buys nothing — a damaged verdict cannot
                    // repair better than the same verdict after the remaining
                    // articles land, and an insufficient-recovery verdict
                    // parks on exactly the drain this gate waits for. Without
                    // the gate every completing recovery volume re-runs the
                    // full pass — many concurrent whole-directory reads, all of them
                    // answering "still waiting".
                    // The direct-store arm above waits for the same drain in
                    // `direct_sets_ready_for_authoritative_par2`; this is the
                    // conventional path's mirror of it. The re-arm is the
                    // drain itself: the last completing file schedules a
                    // completion check, and the quiescent flush sweeps a
                    // parked tail.
                    if self.job_has_pending_download_pipeline_work(job_id) {
                        info!(
                            job_id = job_id.0,
                            "deferring PAR2 damaged-path analysis until the job's downloads drain"
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }
                    // The repairer reads and *writes* volume files through
                    // `DiskFileAccess`, which a virtual volume has none of. So
                    // any set still routing here — one whose repair refused, or
                    // one whose damage is not the reason the job is in this
                    // branch — materializes first, and the repairer sees real
                    // files.
                    if self.demote_live_direct_sets_for_par2_repair(job_id).await {
                        // Re-armed rather than left to the 30 s reconcile
                        // sweep: the job is one pass away from its verdict and
                        // the materialized volumes are already on disk.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    // The entry that finds the parked recovery landed. The
                    // analysis below already read every damaged file and named
                    // every damaged slice; a recovery volume carries no source
                    // bytes, so running it again returns the verdict it already
                    // returned plus one number — how much recovery is now
                    // available — which the repair pass reads off the merged set
                    // for itself.
                    //
                    // Fires once, and only after the drain the arm above waits
                    // for: `ready_pending_par2_repair` requires every promoted
                    // PAR2 file complete with nothing promoted still moving, and
                    // the verdict is taken here rather than left standing, so a
                    // job cannot lap this shortcut.
                    //
                    // Correctness does not rest on it. par2-rs proves every
                    // repair input against the fingerprints its own scan
                    // recorded and re-scans the set from scratch when one has
                    // drifted, so the worst a verdict trusted here can cost is
                    // the read it was trying to save.
                    let parked_verdict =
                        self.ready_pending_par2_repair(job_id, &par2_set)
                            .map(|pending| {
                                (
                                    pending.blocks_needed,
                                    pending.damaged,
                                    pending.verification.clone(),
                                )
                            });
                    if let Some((blocks_needed, damaged, verification)) = parked_verdict {
                        info!(
                            job_id = job_id.0,
                            damaged,
                            blocks_needed,
                            recovery_now = par2_set.recovery_block_count(),
                            "targeted recovery landed — repairing on the analysis that asked for it"
                        );
                        // Left parked if the repair slot is busy: this entry
                        // repaired nothing, and the queued-repair promotion is
                        // what brings the job back here.
                        if !self.maybe_start_repair(job_id).await {
                            return;
                        }
                        self.clear_pending_par2_repair(job_id, par2_set.recovery_set_id);
                        #[cfg(test)]
                        {
                            self.par2_repairs_from_parked_verdict += 1;
                        }
                        match self
                            .run_par2_repairer(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                true,
                                Some(&verification),
                            )
                            .await
                        {
                            Ok(outcome) => {
                                self.finish_par2_repair(
                                    job_id,
                                    Arc::clone(&par2_set),
                                    working_dir.clone(),
                                    &verification,
                                    outcome,
                                    has_crc_failures,
                                )
                                .await;
                                return;
                            }
                            // Covers the recovery that arrived and turned out
                            // to be unusable: `run_par2_repairer` refuses any
                            // terminal non-repair status, so an Insufficient or
                            // ResourceLimited repair lands here rather than
                            // silently reporting success.
                            Err(error_msg) => {
                                self.fail_par2_repair(job_id, error_msg);
                                return;
                            }
                        }
                    }
                    let repair_analysis = match self
                        .analyze_par2_with_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            matches!(current_status, JobStatus::Repairing),
                        )
                        .await
                    {
                        Ok(Some(outcome)) => outcome,
                        // The read is running on a blocking worker. Its
                        // completion re-enters this gate, which reaches this
                        // call again and takes the parked verdict; the job
                        // stays in Verifying until then.
                        Ok(None) => return,
                        Err(_) if self.shared_state.is_job_cancellation_requested(job_id) => {
                            return;
                        }
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    };
                    let verification = &repair_analysis.verification;
                    let damaged = verification.total_missing_blocks;
                    let recovery_now = repair_analysis.recovery_blocks_available;
                    let total_recovery_capacity =
                        self.total_recovery_block_capacity(job_id, par2_set.recovery_set_id);
                    let blocks_needed = match &verification.repairable {
                        par2_rs::verify::Repairability::NotNeeded => 0,
                        par2_rs::verify::Repairability::Repairable { blocks_needed, .. }
                        | par2_rs::verify::Repairability::Insufficient { blocks_needed, .. } => {
                            *blocks_needed
                        }
                        par2_rs::verify::Repairability::ResourceLimited { .. } => 0,
                    };

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                        &verification.repairable
                    {
                        let msg = par2_resource_limit_message(reason);
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    // Damage confined to furniture is delivered rather than
                    // repaired: rebuilding one slice of an `.nfo` would force a
                    // read of the whole set to assemble the decode matrix, and
                    // the file is shipped as-is either way if the blocks are
                    // short. Anything else damaged alongside it sends the
                    // verdict down the ordinary ladder, which repairs the
                    // furniture in the same pass at no extra cost.
                    let ignorable_damage = self.par2_damage_is_only_ignorable(verification);
                    // A verdict with nothing damaged, nothing missing and no
                    // slice to reconstruct has no repair in it: every described
                    // file's content is on disk and whole, and the only work
                    // left is moving some of it onto the names the descriptions
                    // give it. Running the repairer over that shape is how a set
                    // that was never damaged ends up with a directory full of
                    // `<name>.N` backups — it installs each file at its
                    // canonical name and moves the occupant aside — and the
                    // placement that follows is then asked to rename files onto
                    // names those installs already filled.
                    //
                    // So it takes the road the verify arm takes for a clean
                    // verdict instead. The plan has to come from a directory
                    // scan: a pairwise swap is only visible to something that
                    // looks at what is actually on each name, and the plan
                    // derived from statuses can express it only as two renames
                    // into occupied targets. The scan's own read is what stands
                    // as this set's verdict from here on.
                    //
                    // Ordered after the furniture rule above, which owns any
                    // verdict whose damage is all ignorable; a `Renamed` entry
                    // sends that rule to `None`, so the two never contend.
                    let placement_only = par2_verification_needs_repair(verification)
                        && ignorable_damage.is_none()
                        && par2_verification_is_placement_only(verification);
                    let mut placement_pass: Option<par2_rs::VerificationResult> = None;
                    if placement_only {
                        info!(
                            job_id = job_id.0,
                            files_renamed = repair_analysis.files_renamed,
                            "PAR2 analysis — placement only, nothing to repair"
                        );
                        let (scanned, placement_plan) = match self
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
                                self.finish_par2_set_failure(job_id, set_id, message).await;
                                return;
                            }
                        };
                        // The scan is a second read of a directory the analysis
                        // described a moment ago, and it is the one this arm
                        // stands its verdict on. If the two disagree —
                        // `verify_all` cannot report misplacement, so a
                        // disagreement is damage or an absence that appeared in
                        // between — then "placement only" was concluded from a
                        // state that no longer holds, and marking the set
                        // verified on it would accept a verdict nothing re-read.
                        // Nothing is moved and the gate is re-armed instead: the
                        // next lap analyses the disk as it is now and takes
                        // whichever ladder that answer deserves.
                        if par2_verification_needs_repair(&scanned) {
                            warn!(
                                job_id = job_id.0,
                                damaged = scanned.total_missing_blocks,
                                "PAR2 placement pass disagreed with the analysis it stood in \
                                 for — re-checking before placing anything"
                            );
                            self.schedule_job_completion_check(job_id);
                            return;
                        }
                        self.try_deobfuscate_files_with_par2(job_id).await;
                        if let Err(error) = self
                            .apply_placement_plan_for_retry_or_repair(
                                job_id,
                                working_dir.clone(),
                                &placement_plan,
                            )
                            .await
                        {
                            self.finish_par2_set_failure(job_id, set_id, error).await;
                            return;
                        }
                        placement_pass = Some(scanned);
                    }
                    // From here the placement-only arm carries the scanned
                    // pass's answer, which read every file through the plan it
                    // just applied. Every other arm is unchanged: nothing was
                    // scanned, so this resolves to the analysis result itself.
                    let verification = placement_pass.as_ref().unwrap_or(verification);
                    if !par2_verification_needs_repair(verification)
                        || ignorable_damage.is_some()
                        || placement_only
                    {
                        if let Some(ignorable) = ignorable_damage.as_ref() {
                            warn!(
                                job_id = job_id.0,
                                "delivering {} damaged ignorable file(s) without repair: {}",
                                ignorable.len(),
                                ignorable.join(", ")
                            );
                        } else if !placement_only {
                            info!(job_id = job_id.0, "PAR2 analysis passed — no repair needed");
                        }

                        self.retry_par2_authoritative_identity(job_id).await;
                        // A clean verdict repaired nothing.
                        self.refresh_verified_complete_archive_topologies(
                            job_id,
                            verification,
                            &HashSet::new(),
                        )
                        .await;
                        if let Err(error) = self
                            .reconcile_and_classify_par2_verification(
                                job_id,
                                verification,
                                has_crc_failures,
                                "clean PAR2 verification",
                            )
                            .await
                        {
                            self.finish_par2_set_failure(job_id, set_id, error).await;
                            return;
                        }
                        let _ = self
                            .settle_par2_set(
                                job_id,
                                set_id,
                                Par2SetSettlementReason::Clean {
                                    slice_size: par2_set.slice_size,
                                    verification_mode: CleanPar2VerificationMode::Authoritative,
                                },
                            )
                            .await;

                        if !self.par2_verified.contains(&job_id) {
                            self.schedule_job_completion_check(job_id);
                            return;
                        }

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
                                "cleared failed extractions after PAR2 analysis — retrying"
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
                        return;
                    }

                    // What the authoritative analysis of a damaged job actually
                    // read. Evidence seeding is supposed to make this the size
                    // of the damaged files rather than the size of the job, and
                    // this is the only number that says whether it does.
                    //
                    // It is also the number that decides whether intra-file
                    // slice skipping is ever worth building: while a damaged
                    // file is read whole, the floor is its length, and only a
                    // figure that stays far above the damaged bytes would
                    // justify going finer than per-file.
                    let authoritative_bytes_read = repair_analysis.scan.bytes_scanned;
                    crate::runtime::perf_probe::record_value(
                        "par2.authoritative.bytes_read",
                        authoritative_bytes_read,
                    );
                    #[cfg(test)]
                    self.par2_authoritative_bytes_read
                        .push(authoritative_bytes_read);
                    info!(
                        job_id = job_id.0,
                        damaged,
                        blocks_needed,
                        recovery_now,
                        total_recovery_capacity,
                        files_renamed = repair_analysis.files_renamed,
                        files_damaged = repair_analysis.files_damaged,
                        files_missing = repair_analysis.files_missing,
                        authoritative_bytes_read,
                        "PAR2 analysis — repair required"
                    );

                    if total_recovery_capacity < blocks_needed {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            blocks_needed,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();
                        if recovery_still_settling {
                            info!(
                                job_id = job_id.0,
                                blocks_needed,
                                total_recovery_capacity,
                                promoted_blocks = promoted,
                                "waiting for targeted recovery downloads before repair"
                            );
                            // The verdict this pass paid for, held for the entry
                            // that finds the recovery landed.
                            self.park_par2_repair_verdict(
                                job_id,
                                &par2_set,
                                blocks_needed,
                                damaged,
                                verification,
                            );
                            self.transition_postprocessing_status(
                                job_id,
                                JobStatus::Downloading,
                                Some("downloading"),
                            );
                            return;
                        }
                        self.finish_par2_set_failure(
                            job_id,
                            set_id,
                            format!(
                                "not repairable: {blocks_needed} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        )
                        .await;
                        return;
                    }

                    if recovery_now < blocks_needed {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            blocks_needed,
                        );
                        let targeted_total = self.recovery_blocks_available_or_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();

                        if targeted_total < blocks_needed && !recovery_still_settling {
                            let msg = format!(
                                "not repairable: {blocks_needed} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        if let Some(msg) = par2_unreachable_recovery_failure(
                            job_id,
                            blocks_needed,
                            recovery_now,
                            targeted_total,
                            promoted,
                            recovery_still_settling,
                            promoted_recovery.parked_promoted_recovery,
                        ) {
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        info!(
                            job_id = job_id.0,
                            blocks_needed,
                            recovery_now,
                            targeted_total,
                            promoted_blocks = promoted,
                            "waiting for targeted recovery downloads before repair"
                        );
                        // The verdict this pass paid for, held for the entry
                        // that finds the recovery landed.
                        self.park_par2_repair_verdict(
                            job_id,
                            &par2_set,
                            blocks_needed,
                            damaged,
                            verification,
                        );
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }

                    if matches!(
                        &verification.repairable,
                        par2_rs::verify::Repairability::Insufficient { .. }
                            | par2_rs::verify::Repairability::ResourceLimited { .. }
                    ) {
                        let msg = format!(
                            "not repairable: PAR2 analysis found incomplete critical repair metadata or unusable recovery despite {recovery_now} available recovery blocks"
                        );
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    if !self.maybe_start_repair(job_id).await {
                        return;
                    }

                    match self
                        .run_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            true,
                            Some(verification),
                        )
                        .await
                    {
                        Ok(outcome) => {
                            self.finish_par2_repair(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                verification,
                                outcome,
                                has_crc_failures,
                            )
                            .await;
                            return;
                        }
                        Err(error_msg) => {
                            self.fail_par2_repair(job_id, error_msg);
                            return;
                        }
                    }
                }

                // A settled verdict re-entered because a *protected* file is
                // still incomplete while its verified bytes sit on disk.
                //
                // Nothing about the recovery set has changed, so reading it
                // again — seconds of it, over the whole payload — can only
                // return the verdict it already returned, and the reconciler
                // that failed to bind the file will fail to bind it again. The
                // recovery set had an answer for that file either way, so the
                // only thing that can put a job here is our own bookkeeping. It
                // gets exactly one more lap to settle and is then reported as
                // the bug it is: a named, reproducible failure beats an
                // invisible loop burning a core.
                //
                // Two neighbouring states deliberately keep their read:
                //
                //  - A protected file whose bytes are not on disk, or are short
                //    of the described length. Something really is missing
                //    there, and the authoritative pass is the only thing that
                //    names which blocks a recovery promotion has to fetch — a
                //    set waiting on a volume that never posted arrives at this
                //    gate in exactly that shape.
                //  - A failed extraction. The pass it runs is also what applies
                //    the placement correction that can make the retry succeed,
                //    and there is no way to obtain that without a verification.
                //    That path carries a single-retry latch of its own.
                if !par2_verdict_open
                    && !has_crc_failures
                    && self.settled_verdict_left_only_proven_protected_files(job_id, set_id)
                {
                    let attempts = {
                        let set_runtime = self.ensure_par2_runtime(job_id).served_mut().expect(
                            "post-verdict reconciliation belongs to the served recovery set",
                        );
                        set_runtime.post_verdict_reconcile_attempts = set_runtime
                            .post_verdict_reconcile_attempts
                            .saturating_add(1);
                        set_runtime.post_verdict_reconcile_attempts
                    };
                    if attempts > 1 {
                        let message = self
                            .classify_incomplete_after_par2(
                                job_id,
                                &Par2Reconciliation::default(),
                                "PAR2 verdict settled but reconciliation left files outstanding",
                            )
                            .map(|report| report.message)
                            .unwrap_or_else(|| {
                                "BUG: PAR2-protected file(s) stayed incomplete after a settled \
                                 verification"
                                    .to_string()
                            });
                        warn!(job_id = job_id.0, error = %message);
                        self.fail_job(job_id, message);
                        return;
                    }
                    info!(
                        job_id = job_id.0,
                        "PAR2 verdict is settled — retrying reconciliation once instead of \
                         re-reading the recovery set"
                    );
                    self.reconcile_job_progress(job_id).await;
                    return;
                }

                let emit_verification_events = !has_crc_failures
                    || !self.par2_verified.contains(&job_id)
                    || authoritative_par2_verification_needed
                    || matches!(current_status, JobStatus::Repairing);
                let (verification, placement_plan) = match direct_verdict {
                    Some(mut verification) => {
                        // The direct gate already read this set — virtually,
                        // through the overlay — and reached exactly this
                        // verdict. Asking `verify_par2_with_placement` to read
                        // it again would be the second whole-set pass this
                        // gate exists to avoid, so its observable effects are
                        // replicated here instead of its read: the same status
                        // transition and verification-started announcement
                        // `emit_events` would have produced, then the one
                        // settlement this verdict gets — `verify_direct_sets_quietly`
                        // adjusts direct damage before returning but never
                        // settles, so this is the first and only settle call
                        // this verification instance sees.
                        if emit_verification_events {
                            if !matches!(current_status, JobStatus::Repairing) {
                                self.transition_postprocessing_status(
                                    job_id,
                                    JobStatus::Verifying,
                                    Some("verifying"),
                                );
                            } else {
                                info!(
                                    job_id = job_id.0,
                                    "rerunning PAR2 verification while preserving restored \
                                     repair slot"
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
                        self.settle_par2_pass_result(
                            job_id,
                            &mut verification,
                            emit_verification_events,
                        );
                        let plan = placement_plan_from_verification(&verification);
                        Self::log_placement_plan(job_id, &plan);
                        (verification, plan)
                    }
                    None if quick_partial.is_some() => {
                        // The quick pass proved part of this set from zero-read
                        // evidence and left only a remainder unproven. Reading
                        // the whole set here would throw that proof away, so
                        // the pass reads exactly the remainder — through a
                        // 16 KiB-prefix placement proposal, since an unproven
                        // file may sit under an obfuscated name — and the
                        // proven entries are carried into the merged verdict,
                        // the same merge-then-settle discipline as every other
                        // selective pass in this gate.
                        let partial = quick_partial.take().expect("checked by the match guard");
                        if emit_verification_events {
                            if !matches!(current_status, JobStatus::Repairing) {
                                self.transition_postprocessing_status(
                                    job_id,
                                    JobStatus::Verifying,
                                    Some("verifying"),
                                );
                            } else {
                                info!(
                                    job_id = job_id.0,
                                    "rerunning PAR2 verification while preserving restored \
                                     repair slot"
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
                        let (fresh, fresh_plan) = if partial.unproven_recovery.is_empty() {
                            // Every recovery member was proven; nothing to
                            // read. (Only non-recovery descriptions were
                            // unresolved, and the authoritative pass never
                            // read those either.)
                            (
                                par2_rs::VerificationResult {
                                    files: Vec::new(),
                                    recovery_blocks_available: par2_set.recovery_block_count(),
                                    total_missing_blocks: 0,
                                    repairable: par2_rs::verify::Repairability::NotNeeded,
                                },
                                par2_rs::PlacementPlan {
                                    exact: Vec::new(),
                                    swaps: Vec::new(),
                                    renames: Vec::new(),
                                    unresolved: Vec::new(),
                                    conflicts: Vec::new(),
                                },
                            )
                        } else {
                            match self
                                .run_par2_placement_pass(
                                    job_id,
                                    Arc::clone(&par2_set),
                                    working_dir.clone(),
                                    Par2PassScope::SelectedProposed(
                                        partial.unproven_recovery.clone(),
                                        partial.claimed_disk_names.clone(),
                                    ),
                                )
                                .await
                            {
                                Ok(result) => result,
                                Err(message) => {
                                    self.finish_par2_set_failure(job_id, set_id, message).await;
                                    return;
                                }
                            }
                        };
                        let base = quick_partial_base_verification(&par2_set, &partial);
                        let mut verification =
                            par2_rs::verify::merge_verification_results(&par2_set, &base, fresh);
                        self.settle_par2_pass_result(
                            job_id,
                            &mut verification,
                            emit_verification_events,
                        );
                        let plan = merge_partial_placement_plan(partial.proven_plan, fresh_plan);
                        Self::log_placement_plan(job_id, &plan);
                        (verification, plan)
                    }
                    None => match self
                        .verify_par2_with_placement(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            matches!(current_status, JobStatus::Repairing),
                            emit_verification_events,
                        )
                        .await
                    {
                        Ok(result) => result,
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    },
                };
                // Damage on a virtual volume has nothing to repair *into* — the
                // bytes live in a member's partial and an envelope, and a
                // recovered slice belongs to neither — so the set materializes
                // **only its damaged volumes** into scratch, repairs those
                // while every clean volume is still read virtually, routes the
                // repaired spans back through the router and throws the scratch
                // away. The set stays direct and loses no output. Every refusal
                // along the way falls back to the whole-set demotion, which
                // materializes everything and hands the job to the conventional
                // repair path. Either way the job's next move is this gate
                // again, over bytes that changed.
                match self
                    .resolve_direct_sets_with_par2_damage(job_id, &verification)
                    .await
                {
                    DirectDamageResolution::Resolved => {
                        // Re-armed rather than left to the 30 s reconcile sweep:
                        // the repaired bytes are already in the partials, or the
                        // demotion has already materialized (or queued the
                        // refetch of) the volumes, so the next pass can run
                        // immediately.
                        self.schedule_job_completion_check(job_id);
                        return;
                    }
                    DirectDamageResolution::Deferred => {
                        // Damage the merged recovery cannot cover but the
                        // recovery *set* can. The sets keep their outputs and
                        // their virtual volumes while the targeted recovery
                        // downloads; the job is reported as what it is doing.
                        // No re-arm — the completing recovery file merges its
                        // slices and checks the job itself, and a lap of this
                        // gate in the meantime is a slow scan that cannot reach
                        // a different answer.
                        self.transition_postprocessing_status(
                            job_id,
                            JobStatus::Downloading,
                            Some("downloading"),
                        );
                        return;
                    }
                    DirectDamageResolution::Unresolved => {}
                }
                let damaged = verification.total_missing_blocks;
                let recovery_now = verification.recovery_blocks_available;
                let total_recovery_capacity =
                    self.total_recovery_block_capacity(job_id, par2_set.recovery_set_id);

                if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                    &verification.repairable
                {
                    let msg = par2_resource_limit_message(reason);
                    self.finish_par2_set_failure(job_id, set_id, msg).await;
                    return;
                }

                // The same furniture rule the analysis arm applies, stated once
                // per arm because each owns its own ladder.
                let ignorable_damage = self.par2_damage_is_only_ignorable(&verification);
                if !par2_verification_needs_repair(&verification) || ignorable_damage.is_some() {
                    if let Some(ignorable) = ignorable_damage.as_ref() {
                        warn!(
                            job_id = job_id.0,
                            "delivering {} damaged ignorable file(s) without repair: {}",
                            ignorable.len(),
                            ignorable.join(", ")
                        );
                    } else {
                        info!(
                            job_id = job_id.0,
                            "PAR2 verification passed — no damaged slices"
                        );
                    }

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
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }
                    self.retry_par2_authoritative_identity(job_id).await;
                    // A clean verdict repaired nothing.
                    self.refresh_verified_complete_archive_topologies(
                        job_id,
                        &verification,
                        &HashSet::new(),
                    )
                    .await;
                    if let Err(error) = self
                        .reconcile_and_classify_par2_verification(
                            job_id,
                            &verification,
                            has_crc_failures,
                            "clean PAR2 verification",
                        )
                        .await
                    {
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }
                    let _ = self
                        .settle_par2_set(
                            job_id,
                            set_id,
                            Par2SetSettlementReason::Clean {
                                slice_size: par2_set.slice_size,
                                verification_mode: CleanPar2VerificationMode::Authoritative,
                            },
                        )
                        .await;

                    if !self.par2_verified.contains(&job_id) {
                        self.schedule_job_completion_check(job_id);
                        return;
                    }

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

                    if archive_extraction_applicable {
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
                        self.finish_par2_set_failure(job_id, set_id, error).await;
                        return;
                    }

                    let repair_preview = match self
                        .run_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            false,
                            None,
                        )
                        .await
                    {
                        Ok(outcome) => outcome,
                        Err(message) => {
                            self.finish_par2_set_failure(job_id, set_id, message).await;
                            return;
                        }
                    };
                    let repairer_damaged = repair_preview.verification.total_missing_blocks;
                    let repairer_recovery_now = repair_preview.recovery_blocks_available;
                    if repairer_damaged != damaged || repairer_recovery_now != recovery_now {
                        info!(
                            job_id = job_id.0,
                            placement_damaged = damaged,
                            repairer_damaged,
                            placement_recovery = recovery_now,
                            repairer_recovery = repairer_recovery_now,
                            files_renamed = repair_preview.files_renamed,
                            available_blocks = repair_preview.available_blocks,
                            "PAR2 repairer scan adjusted repair requirements"
                        );
                    }
                    let damaged = repairer_damaged;
                    let recovery_now = repairer_recovery_now;

                    if let par2_rs::verify::Repairability::ResourceLimited { reason } =
                        &repair_preview.verification.repairable
                    {
                        let msg = par2_resource_limit_message(reason);
                        self.finish_par2_set_failure(job_id, set_id, msg).await;
                        return;
                    }

                    if total_recovery_capacity < damaged {
                        self.finish_par2_set_failure(
                            job_id,
                            set_id,
                            format!(
                                "not repairable: {damaged} damaged slices, only {total_recovery_capacity} recovery blocks advertised"
                            ),
                        )
                        .await;
                        return;
                    }

                    if recovery_now < damaged {
                        let promoted = self.promote_recovery_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                            damaged,
                        );
                        let targeted_total = self.recovery_blocks_available_or_targeted(
                            job_id,
                            par2_set.recovery_set_id,
                        );
                        let promoted_recovery = self.promoted_recovery_pipeline_state(job_id);
                        let recovery_still_settling = promoted > 0
                            || self.job_has_pending_download_pipeline_work(job_id)
                            || promoted_recovery.has_pending_work();

                        // If all available/targeted recovery is still insufficient,
                        // fail immediately instead of waiting for downloads that
                        // won't help.
                        if targeted_total < damaged && !recovery_still_settling {
                            let msg = format!(
                                "not repairable: {damaged} damaged slices, \
                                 only {targeted_total} recovery blocks available in NZB"
                            );
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
                            return;
                        }

                        if let Some(msg) = par2_unreachable_recovery_failure(
                            job_id,
                            damaged,
                            recovery_now,
                            targeted_total,
                            promoted,
                            recovery_still_settling,
                            promoted_recovery.parked_promoted_recovery,
                        ) {
                            self.finish_par2_set_failure(job_id, set_id, msg).await;
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

                    match self
                        .run_par2_repairer(
                            job_id,
                            Arc::clone(&par2_set),
                            working_dir.clone(),
                            true,
                            Some(&verification),
                        )
                        .await
                    {
                        Ok(outcome) => {
                            self.finish_par2_repair(
                                job_id,
                                Arc::clone(&par2_set),
                                working_dir.clone(),
                                &verification,
                                outcome,
                                has_crc_failures,
                            )
                            .await;
                            return;
                        }
                        Err(error_msg) => {
                            self.fail_par2_repair(job_id, error_msg);
                            return;
                        }
                    }
                }
            } else {
                if !par2_bypassed && self.promote_par2_metadata(job_id) {
                    info!(
                        job_id = job_id.0,
                        "waiting for PAR2 metadata download before repair evaluation"
                    );
                    self.transition_postprocessing_status(
                        job_id,
                        JobStatus::Downloading,
                        Some("downloading"),
                    );
                    return;
                }
                if has_incomplete_data_files {
                    let msg = format!(
                        "download incomplete after exhausting retries: {complete_data_files}/{total_data_files} data files complete and no PAR2 metadata is available for repair"
                    );
                    warn!(job_id = job_id.0, error = %msg);
                    self.fail_job(job_id, msg);
                    return;
                }
                if has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
                    }
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
                if !has_crc_failures {
                    match self.retry_failed_archive_sources_without_par2(job_id).await {
                        Ok(true) => return,
                        Ok(false) => {}
                        Err(error) => {
                            self.fail_job(job_id, error);
                            return;
                        }
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
            if !download_pipeline_exhausted || self.job_has_active_extraction_tasks(job_id) {
                return;
            }
            if !par2_bypassed && self.promote_par2_metadata(job_id) {
                info!(
                    job_id = job_id.0,
                    "waiting for PAR2 metadata download before incomplete-download failure"
                );
                self.transition_postprocessing_status(
                    job_id,
                    JobStatus::Downloading,
                    Some("downloading"),
                );
                return;
            }
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

        // Every branch above this line either returns or leaves the job with
        // all of its data files complete and its PAR2 question settled, and
        // every branch below dispatches the job onward — to extraction, or
        // straight to the final move. So this is the one point a job with no
        // recovery set passes through on its way to completion, and it is
        // where a `.sfv` listing is both readable and still meaningful:
        //
        //  - after the PAR2 block, so a job with a set is adjudicated by it and
        //    the fallback's own scope guard sees a settled answer rather than
        //    racing one;
        //  - after deobfuscation, which only ever runs off PAR2 metadata, so
        //    the names a listing is matched against are the job's final ones;
        //  - before extraction, which is what consumes the posted files — the
        //    RAR volumes and split parts a listing actually names — and whose
        //    cleanup deletes them;
        //  - before the move to complete, so the working directory paths still
        //    resolve;
        //  - before the terminal transition records history, so a verdict
        //    reaches history and the UI through the same family PAR2 verdicts
        //    use rather than arriving after the job is already filed.
        if let Some(error) = self.verify_par2_less_job_with_sfv(job_id).await {
            self.fail_job(job_id, error);
            return;
        }

        if only_rar_archives {
            self.check_rar_job_completion(job_id).await;
            return;
        }

        if self.job_has_promoted_recovery_pipeline_work(job_id, "extraction") {
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
                if self.job_has_promoted_recovery_pipeline_work(job_id, "completion") {
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
                // A split set the recovery data joined for us lands here rather
                // than in the extraction arm, so its spent parts are removed
                // here too — the final move relocates the whole directory, and
                // the parts are not part of the release.
                self.cleanup_par2_joined_split_parts(job_id).await;
                // No archives — move to complete and finish.
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
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
                    cleanup_files.extend(self.par2_joined_split_part_names(job_id));
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

                info!(job_id = job_id.0, "extraction complete");
                // Low-frequency: one observation per job-level extraction, never on a
                // per-segment path. Records the metric next to the event that already
                // announces the same fact.
                self.metrics.job_lifecycle.note_extraction(
                    crate::operations::instrumentation::StageOutcomeKind::Complete,
                );
                let _ = self
                    .event_tx
                    .send(PipelineEvent::ExtractionComplete { job_id });

                // Move extracted files to complete directory.
                if let Err(error) = self.start_move_to_complete(job_id).await {
                    self.fail_job(job_id, error);
                }
            }
            ExtractionReadiness::Blocked { reason } => {
                if reason.starts_with("archive topology not yet available") {
                    info!(
                        job_id = job_id.0,
                        reason = %reason,
                        "deferring completion until archive topology is available"
                    );
                    self.schedule_job_completion_check(job_id);
                    return;
                }
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
