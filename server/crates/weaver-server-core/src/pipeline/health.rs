use super::*;

const HEALTH_PROBE_REARM_MIN_BYTES: u64 = 128 * 1024 * 1024;
const HEALTH_PROBE_REARM_PAYLOAD_DIVISOR: u64 = 200;

/// How far ahead of the delivered payload the losses must run before damage
/// confined to a single file is allowed to look like a dead release.
///
/// One withheld volume in an otherwise healthy posting is a hole PAR2 covers,
/// not a release nobody uploaded, and sampling the rest of it answers a
/// question no one asked. A release that really is gone looks different: almost
/// nothing lands while the failures pile up.
const HEALTH_PROBE_DEAD_RELEASE_LANDED_DIVISOR: u64 = 4;

/// Whether early PAR2 promotion runs on the terminal-segment edge.
///
/// On by default. The switch exists so the behaviour can be taken out in one
/// place if promoting before the completion checkpoint ever proves to conflict
/// with the direct-store PAR2 wiring, without unpicking the call sites.
const EARLY_RECOVERY_PROMOTION: bool = true;

impl Pipeline {
    fn health_tracked_bytes(total_bytes: u64, par2_bytes: u64) -> u64 {
        total_bytes.saturating_sub(par2_bytes)
    }

    pub(crate) fn critical_health_milli(total: u64, par2_bytes: u64) -> u32 {
        if par2_bytes == 0 {
            850
        } else if par2_bytes * 2 > total {
            0
        } else {
            let denom = total.saturating_sub(par2_bytes);
            total
                .saturating_sub(par2_bytes * 2)
                .saturating_mul(1000)
                .checked_div(denom)
                .unwrap_or(0) as u32
        }
    }

    /// The failed-byte figure the health policy decides on.
    ///
    /// The ledger is derived from per-segment terminal states and is what the
    /// job reports; a completed probe round contributes a *projection* over its
    /// sample, which is a health signal and nothing else. Taking the larger of
    /// the two keeps the early abort on a dead release while leaving the
    /// reported ledger a sum of facts.
    pub(crate) fn health_decision_failed_bytes(state: &crate::jobs::model::JobState) -> u64 {
        state.failed_bytes.max(state.probe_projected_failed_bytes)
    }

    fn next_health_probe_failed_bytes(
        state: &crate::jobs::model::JobState,
        missed: usize,
        inconclusive: bool,
    ) -> u64 {
        let decided = Self::health_decision_failed_bytes(state);
        let immediate_rearm = decided.saturating_add(1);
        if inconclusive || missed > 0 {
            return immediate_rearm;
        }

        let tracked_bytes = Self::health_tracked_bytes(state.spec.total_bytes, state.par2_bytes);
        let rearm_delta = (tracked_bytes / HEALTH_PROBE_REARM_PAYLOAD_DIVISOR)
            .max(HEALTH_PROBE_REARM_MIN_BYTES)
            .max(1);
        let critical = Self::critical_health_milli(state.spec.total_bytes, state.par2_bytes);
        let critical_failed_bytes = state
            .spec
            .total_bytes
            .saturating_sub(((state.spec.total_bytes as u128 * critical as u128) / 1000) as u64);
        let critical_rearm = critical_failed_bytes.saturating_add(1);

        decided
            .saturating_add(rearm_delta)
            .min(critical_rearm)
            .max(immediate_rearm)
    }

    /// How many health-counted files this job has already lost at least one
    /// segment of.
    ///
    /// Maintained on the same booking edge as `failed_bytes`, so the two agree
    /// by construction, and on the same rule: a missing recovery volume is not
    /// damage to the release.
    pub(crate) fn health_failing_file_count(&self, job_id: JobId) -> usize {
        self.jobs
            .get(&job_id)
            .map_or(0, |state| state.health_failing_files.len())
    }

    /// Recovery blocks it would take to cover `failed_bytes` of lost payload,
    /// for the recovery set this job is served by.
    ///
    /// Deliberately generous: the lost bytes rounded up to whole slices, plus
    /// one slice for each damaged file, because a hole never starts on a slice
    /// boundary. Both callers want to be wrong in this direction — the probe
    /// policy only stands down when the recovery covers the *over*-estimate,
    /// and early promotion asks for at least as much as the repair will.
    fn par2_shortfall_blocks(
        &self,
        job_id: JobId,
        set_id: par2_rs::RecoverySetId,
        failed_bytes: u64,
    ) -> Option<u32> {
        if failed_bytes == 0 {
            return Some(0);
        }
        let slice_size = self.par2_set_for(job_id, set_id)?.slice_size;
        if slice_size == 0 {
            return None;
        }
        let damaged_files = self.health_failing_file_count(job_id).max(1) as u64;
        let blocks = failed_bytes
            .div_ceil(slice_size)
            .saturating_add(damaged_files);
        Some(blocks.min(u64::from(u32::MAX)) as u32)
    }

    /// Whether a loaded recovery set already answers the question a probe would
    /// ask.
    ///
    /// `par2_bytes > 0` was never enough: it says a job declared recovery
    /// files, not that any of them describe this damage or that there are
    /// enough of them. This asks the parsed set instead — the shortfall in
    /// blocks against what the posting's recovery volumes can actually supply.
    fn par2_recovery_covers_health_damage(&self, job_id: JobId, failed_bytes: u64) -> bool {
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return false;
        };
        let Some(blocks_needed) = self.par2_shortfall_blocks(job_id, set_id, failed_bytes) else {
            return false;
        };
        self.total_recovery_block_capacity(job_id, set_id) >= blocks_needed
    }

    /// Put the recovery this job already knows it needs on the wire now.
    ///
    /// Damage is known the moment a segment reaches a terminal state, but the
    /// blocks that repair it used to stay parked in `recovery_queue` until the
    /// completion checkpoint ran — which is to say until the payload had
    /// finished. The two downloads are then strictly sequential for no reason:
    /// promoted recovery is ordinary completion-critical work and rides the
    /// same lanes, so it can just as well arrive while the rest of the payload
    /// is still coming.
    ///
    /// Idempotent by construction: `promote_recovery_targeted` subtracts what
    /// is already merged or already on its way before selecting anything, and
    /// skips files it has promoted before. Damage discovered later — a CRC
    /// failure, a verification verdict — still reaches the checkpoint-time
    /// promotion exactly as before; this only front-runs the part that was
    /// knowable at booking time.
    pub(in crate::pipeline) fn promote_recovery_for_known_damage(&mut self, job_id: JobId) {
        if !EARLY_RECOVERY_PROMOTION {
            return;
        }
        let failed_bytes = match self.jobs.get(&job_id) {
            Some(state)
                if !matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete) =>
            {
                state.failed_bytes
            }
            _ => return,
        };
        if failed_bytes == 0 {
            return;
        }
        let Some(set_id) = self.par2_served_set_id(job_id) else {
            return;
        };
        let Some(blocks_needed) = self.par2_shortfall_blocks(job_id, set_id, failed_bytes) else {
            return;
        };
        if blocks_needed == 0 {
            return;
        }
        // Selecting blocks walks the parked and queued recovery work, and this
        // runs on every terminal segment. It is worth walking only when the
        // shortfall has grown past what was last asked for — and never past
        // what the posting's volumes can supply, so a release that has lost
        // more than its recovery covers stops asking once everything is on
        // its way rather than re-walking the queues for every further loss.
        let capacity = self.total_recovery_block_capacity(job_id, set_id);
        let target = blocks_needed.min(capacity);
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return;
        };
        if target <= state.early_recovery_requested_blocks {
            return;
        }
        state.early_recovery_requested_blocks = target;
        let promoted = self.promote_recovery_targeted(job_id, set_id, blocks_needed);
        if promoted > 0 {
            info!(
                job_id = job_id.0,
                failed_bytes,
                blocks_needed,
                promoted_blocks = promoted,
                "promoted PAR2 recovery on booked damage, alongside the payload"
            );
        }
    }

    pub(crate) fn health_probe_candidates(spec: &crate::jobs::model::JobSpec) -> Vec<String> {
        spec.files
            .iter()
            .filter(|file_spec| file_spec.role.counts_toward_health())
            .flat_map(|file_spec| {
                file_spec
                    .segments
                    .iter()
                    .map(|segment| segment.message_id.clone())
            })
            .collect()
    }

    fn projected_probe_failed_bytes(
        state: &crate::jobs::model::JobState,
        total: usize,
        missed: usize,
    ) -> Option<u64> {
        if total == 0 || missed == 0 {
            return None;
        }

        let tracked_bytes = Self::health_tracked_bytes(state.spec.total_bytes, state.par2_bytes);
        if tracked_bytes == 0 {
            return None;
        }

        Some((tracked_bytes as u128 * missed as u128 / total as u128) as u64)
    }

    pub(crate) fn health_probe_sample_indices(total_segs: usize, probe_round: u32) -> Vec<usize> {
        if total_segs == 0 {
            return Vec::new();
        }

        let probe_count = (total_segs * 8 / 100).max(10).min(total_segs);
        let stride = (total_segs / probe_count).max(1);
        let offset = if stride > 1 {
            probe_round as usize % stride
        } else {
            0
        };

        (offset..total_segs).step_by(stride).collect()
    }

    /// Whether the damage has the shape a probe exists to catch.
    ///
    /// The probe's job is to abandon a release nobody posted before a gigabyte
    /// proves it. Two shapes qualify:
    ///
    /// * losses spanning more than one file — a posting that is coming apart in
    ///   several places is unlikely to be coming apart in only those places;
    /// * losses that dwarf what has landed — the job is trying and getting
    ///   nothing back, whether or not it has reached a second file yet.
    ///
    /// One file failing while the rest of the posting arrives cleanly is
    /// neither. That is a hole, and the recovery set — or, without one, the
    /// completion checkpoint — decides what to do about it. Sampling the files
    /// that are already arriving cannot add anything.
    fn health_damage_looks_like_a_dead_release(
        state: &crate::jobs::model::JobState,
        failing_files: usize,
        failed_bytes: u64,
    ) -> bool {
        if failing_files > 1 {
            return true;
        }
        state
            .downloaded_bytes
            .saturating_mul(HEALTH_PROBE_DEAD_RELEASE_LANDED_DIVISOR)
            < failed_bytes
    }

    /// Check job health and abort if below critical threshold.
    ///
    /// Health = (total_bytes - failed_bytes) / total_bytes × 1000.
    /// Critical health is derived from available PAR2 recovery data:
    ///   critical = (total - 2 × par2_bytes) / (total - par2_bytes) × 1000
    /// If no PAR2 data, defaults to 850 (85%).
    pub(super) fn check_health(&mut self, job_id: JobId) {
        if self.jobs.get(&job_id).is_none_or(|state| {
            matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete)
                || state.spec.total_bytes == 0
        }) {
            return;
        }

        // Damage the job has just booked is damage a loaded recovery set can
        // already be asked to cover. Nothing about that answer waits on a
        // checkpoint, so it does not wait on one.
        self.promote_recovery_for_known_damage(job_id);

        let failing_files = self.health_failing_file_count(job_id);
        let decided_failed_bytes = self
            .jobs
            .get(&job_id)
            .map_or(0, Self::health_decision_failed_bytes);
        let recovery_covers_damage =
            self.par2_recovery_covers_health_damage(job_id, decided_failed_bytes);

        // Extract all needed values upfront so the borrow on self.jobs is dropped
        // before we call activate_health_probes or mutate state.
        let (health, critical, failed_bytes, total, par2_bytes, needs_probes, health_probing) = {
            let state = match self.jobs.get(&job_id) {
                Some(s) => s,
                None => return,
            };

            let total = state.spec.total_bytes;
            let failed_bytes = decided_failed_bytes;
            let health = health_milli(total, failed_bytes);

            let par2_bytes = state.par2_bytes;

            let critical = Self::critical_health_milli(total, par2_bytes);

            let critical_failed_bytes =
                total.saturating_sub(((total as u128 * critical as u128) / 1000) as u64);
            let within_critical_probe_fence =
                failed_bytes <= critical_failed_bytes.saturating_add(1);
            // Re-arm on a state change, not on every terminal segment: the
            // failed-byte watermark, or a file that had not failed before. A
            // second dead article in a volume already known to be missing says
            // nothing a round has not already been armed for.
            let rearmed = failed_bytes >= state.next_health_probe_failed_bytes
                || failing_files > state.health_probe_failing_files;
            let needs_probes = health < 980
                && !state.health_probing
                && rearmed
                && (health > critical || within_critical_probe_fence)
                // A probe that cannot change the answer is round trips spent on
                // a question already settled.
                && !recovery_covers_damage
                && Self::health_damage_looks_like_a_dead_release(state, failing_files, failed_bytes);
            (
                health,
                critical,
                failed_bytes,
                total,
                par2_bytes,
                needs_probes,
                state.health_probing,
            )
        };

        if health <= critical && par2_bytes > 0 {
            info!(
                job_id = job_id.0,
                health_pct = health as f64 / 10.0,
                critical_pct = critical as f64 / 10.0,
                failed_bytes,
                total_bytes = total,
                par2_bytes,
                "deferring health failure to PAR2 recovery evaluation"
            );
            self.schedule_job_completion_check(job_id);
            return;
        }

        // Activate health probes when health drops 2% — sample segments across the
        // NZB to get a fast overall health estimate instead of waiting for sequential
        // processing to naturally reach damaged areas.
        if needs_probes && self.activate_health_probes(job_id) {
            return;
        }

        if health_probing && health <= critical {
            info!(
                job_id = job_id.0,
                health_pct = health as f64 / 10.0,
                critical_pct = critical as f64 / 10.0,
                failed_bytes,
                total_bytes = total,
                "deferring health failure while probe confirmation is pending"
            );
            return;
        }

        if health <= critical {
            warn!(
                job_id = job_id.0,
                health_pct = health as f64 / 10.0,
                critical_pct = critical as f64 / 10.0,
                failed_bytes,
                total_bytes = total,
                "aborting job: health below critical threshold"
            );
            let error = format!(
                "health {:.1}% below critical {:.1}%",
                health as f64 / 10.0,
                critical as f64 / 10.0
            );
            self.fail_job(job_id, error);
        }
    }

    /// Handle a probe update (partial or final).
    ///
    /// Final updates either confirm a new payload-health estimate or discard
    /// the round if probe confirmation was inconclusive. If probe activation
    /// parked work for a given job, it is restored before resuming.
    pub(super) fn handle_probe_update(&mut self, update: ProbeUpdate) {
        let ProbeUpdate {
            job_id,
            probe_round,
            total,
            missed,
            done,
            inconclusive,
        } = update;

        if let Some(state) = self.jobs.get(&job_id) {
            if matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete) {
                return;
            }
        } else {
            return;
        }

        // Only the round this job is still waiting on may act. A retired round
        // (see `retire_health_probe_if_download_pipeline_drained`) or one
        // superseded by a later activation must not fold a stale projection
        // into an already-settled ledger, fail a job whose real terminal states
        // are in, or shove the status back to `Downloading` after the
        // checkpoint has moved the job on. `health_probe_round` is incremented
        // past `probe_round` when that round was armed.
        let is_live_round = self.jobs.get(&job_id).is_some_and(|state| {
            state.health_probing && state.health_probe_round == probe_round.wrapping_add(1)
        });
        if !is_live_round {
            return;
        }

        let miss_pct = missed.saturating_mul(100).checked_div(total).unwrap_or(0);

        if done {
            info!(
                job_id = job_id.0,
                total, missed, miss_pct, inconclusive, "health probe complete"
            );
        }

        // All probes missed → release is completely gone.
        let par2_recovery_potential = self
            .jobs
            .get(&job_id)
            .is_some_and(|state| state.par2_bytes > 0);

        if done && !inconclusive && missed == total && total > 0 && !par2_recovery_potential {
            let error = format!(
                "health probe: all {} samples missing — release is unavailable",
                total
            );
            warn!(job_id = job_id.0, "{error}");
            self.fail_job(job_id, error);
            return;
        }

        if done {
            // Re-enqueue held segments into the job's queues. The status is not
            // touched: a probe no longer moves the job into `Checking`, so
            // there is nothing to move it back from — and a round landing after
            // the checkpoint has moved the job on to verification or repair
            // must not drag it back to `Downloading`.
            if let Some(state) = self.jobs.get_mut(&job_id) {
                if !inconclusive
                    && let Some(projected) =
                        Self::projected_probe_failed_bytes(state, total, missed)
                {
                    // The projection is a health signal, never a ledger entry:
                    // it is an extrapolation from a sample and the segments it
                    // stands for have not reached a terminal state yet. Folding
                    // it into `failed_bytes` is what let a job book a
                    // projection and then add the real terminal failures on top
                    // of it.
                    state.probe_projected_failed_bytes =
                        state.probe_projected_failed_bytes.max(projected);
                }
                state.last_health_probe_failed_bytes = Self::health_decision_failed_bytes(state);
                state.next_health_probe_failed_bytes =
                    Self::next_health_probe_failed_bytes(state, missed, inconclusive);
                state.health_probing = false;
                let held = std::mem::take(&mut state.held_segments);
                if !held.is_empty() {
                    info!(
                        job_id = job_id.0,
                        restored = held.len(),
                        "restoring held segments"
                    );
                    for work in held {
                        if work.is_recovery {
                            state.recovery_queue.push(work);
                        } else {
                            state.download_queue.push(work);
                        }
                    }
                }
            }
            self.check_health(job_id);
            if self.jobs.contains_key(&job_id)
                && !self.job_has_pending_download_pipeline_work(job_id)
            {
                self.schedule_job_completion_check(job_id);
            }
        }
    }

    /// Retire an in-flight health probe once the job has nothing left to
    /// download but the probe itself.
    ///
    /// A probe is an *early* estimate: it samples the release while segments
    /// are still arriving so an entirely missing release can be abandoned
    /// before a gigabyte proves it. Once the queue has drained and every
    /// segment has reached a terminal state the job holds the real answer the
    /// probe was approximating, and the round has nothing left to say.
    ///
    /// It no longer holds anything up — a probe is not pending pipeline work
    /// and no longer moves the status — so this is now about the round itself:
    /// dropping it re-arms the hysteresis against the settled ledger instead of
    /// leaving a moot round to fold a sampled projection into a job whose real
    /// terminal states are already in.
    ///
    /// The round is abandoned rather than awaited: `handle_probe_update` drops
    /// a result whose round the job is no longer waiting on.
    ///
    /// Held segments (the decode breaker's, not the probe's) still count as
    /// work, so a job parking any is left alone.
    pub(crate) fn retire_health_probe_if_download_pipeline_drained(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let probing = self.jobs.get(&job_id).is_some_and(|state| {
            state.health_probing
                && state.held_segments.is_empty()
                && !matches!(state.status, JobStatus::Failed { .. } | JobStatus::Complete)
        });
        if !probing || self.job_has_pending_download_work_beyond_health_probe(job_id) {
            return false;
        }

        let Some(state) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        // Re-arm as if the round had come back clean: the ledger already says
        // what the sample would have, so an immediate re-arm would only start
        // another round against the same settled figure.
        state.last_health_probe_failed_bytes = Self::health_decision_failed_bytes(state);
        state.next_health_probe_failed_bytes =
            Self::next_health_probe_failed_bytes(state, 0, false);
        state.health_probing = false;
        // A job restored mid-probe can still carry the status a previous
        // release put it in; nothing else sets it here any more.
        let was_checking = matches!(state.status, JobStatus::Checking);

        info!(
            job_id = job_id.0,
            "retiring health probe — every segment already reached a terminal state"
        );

        if was_checking {
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }
        true
    }

    /// Reject untrusted delivery content or an incomplete security check
    /// without exposing the working tree to terminal scripts or publication.
    pub(super) fn fail_delivery_security_check(&mut self, job_id: JobId, error: String) {
        if let Some(budget) = self.extraction_budgets.get(&job_id) {
            budget.reject_content_policy(error.clone());
        }
        let (released_repair, released_extract) =
            self.prepare_failed_job_runtime(job_id, &error, false);
        self.finish_failed_job(job_id, error, released_repair, released_extract);
    }

    /// Mark a job as failed and purge its queued segments.
    pub(super) fn fail_job(&mut self, job_id: JobId, error: String) {
        // Terminal transition: a job dying without a recovery set never had a
        // PAR2 verdict available to it. No-op when a pass already ruled.
        self.note_job_unverifiable_if_no_par2_set(job_id);
        let failure_stage = self
            .jobs
            .get(&job_id)
            .map(|state| match state.status {
                JobStatus::Verifying => crate::post_processing::model::PipelineFailureStage::Verify,
                JobStatus::QueuedRepair | JobStatus::Repairing => {
                    crate::post_processing::model::PipelineFailureStage::Repair
                }
                JobStatus::QueuedExtract | JobStatus::Extracting => {
                    crate::post_processing::model::PipelineFailureStage::Extract
                }
                JobStatus::Moving => crate::post_processing::model::PipelineFailureStage::Move,
                _ => crate::post_processing::model::PipelineFailureStage::Download,
            })
            .unwrap_or(crate::post_processing::model::PipelineFailureStage::Download);
        // Scripts run on failure too, so the job's own failure survives them:
        // the primary failure is carried through and re-applied afterwards.
        let scripts_may_run = self
            .terminal_post_processing_executor
            .execution_enabled()
            .unwrap_or_else(|settings_error| {
                tracing::warn!(
                    job_id = job_id.0,
                    error = %settings_error,
                    "could not load post-processing settings; preserving legacy failure finalization"
                );
                false
            });
        let extraction_rejected = JobExtractionBudget::is_rejection(&error);
        if extraction_rejected && let Some(budget) = self.extraction_budgets.get(&job_id) {
            budget.cancel_with_error(&error);
        }
        let should_defer = !extraction_rejected
            && scripts_may_run
            && self.jobs.contains_key(&job_id)
            && !self.inflight_terminal_post_processing.contains(&job_id);
        let (released_repair, released_extract) =
            self.prepare_failed_job_runtime(job_id, &error, should_defer);

        if should_defer {
            if released_repair {
                self.promote_queued_repairs();
            }
            if released_extract {
                self.promote_queued_extractions();
            }
            self.start_terminal_post_processing_with_outcome(
                job_id,
                crate::post_processing::model::PipelineOutcome::Failed {
                    stage: failure_stage,
                    code: "PIPELINE_FAILURE".to_string(),
                    message: error.clone(),
                },
                Some(error),
            );
            return;
        }

        self.finish_failed_job(job_id, error, released_repair, released_extract);
    }

    pub(crate) fn finalize_failed_job_after_terminal_post_processing(
        &mut self,
        job_id: JobId,
        error: String,
    ) {
        let (released_repair, released_extract) =
            self.prepare_failed_job_runtime(job_id, &error, false);
        self.finish_failed_job(job_id, error, released_repair, released_extract);
    }

    fn prepare_failed_job_runtime(
        &mut self,
        job_id: JobId,
        error: &str,
        preserve_staging: bool,
    ) -> (bool, bool) {
        // Hooked here rather than at the terminal purge: when post-processing
        // scripts are configured the purge is deferred until they finish, and
        // by then this function has already emptied the download queues and
        // taken the staging directory. A chase must not outlive that.
        self.direct_unpack_abort_job(
            job_id,
            "job failed",
            crate::pipeline::direct_unpack::wiring::AbortLatch::Permanent,
            crate::pipeline::direct_unpack::wiring::DemotionReason::DownloadEnded,
        );

        let (staging_dir, released_repair, released_extract) =
            if let Some(state) = self.jobs.get_mut(&job_id) {
                let released_repair = matches!(state.status, JobStatus::Repairing);
                let released_extract = matches!(state.status, JobStatus::Extracting);
                state.queued_repair_at_epoch_ms = None;
                state.queued_extract_at_epoch_ms = None;
                state.paused_resume_status = None;
                state.paused_resume_download_state = None;
                state.paused_resume_post_state = None;
                state.set_failure(error.to_string());
                state.held_segments.clear();
                // Clear per-job queues to free memory.
                state.download_queue = DownloadQueue::new();
                state.recovery_queue = DownloadQueue::new();
                let staging_dir = if preserve_staging {
                    None
                } else {
                    state.staging_dir.take()
                };
                (staging_dir, released_repair, released_extract)
            } else {
                (None, false, false)
            };
        let extraction_budget = if preserve_staging {
            None
        } else {
            self.extraction_budgets.remove(&job_id)
        };
        if !preserve_staging {
            self.unacceptable_extension_policies.remove(&job_id);
        }
        if released_repair {
            self.metrics.repair_active.fetch_sub(1, Ordering::Relaxed);
        }
        if released_extract {
            self.metrics.extract_active.fetch_sub(1, Ordering::Relaxed);
        }
        // Clean up staging directory if it was created.
        if let Some(staging) = staging_dir {
            tokio::spawn(async move {
                if let Some(budget) = extraction_budget {
                    let _ = tokio::task::spawn_blocking(move || budget.wait_for_idle()).await;
                }
                // Direct-store's member payload is written into the staging root
                // through the cached-handle pool, so the handles have to go
                // before the directory does.
                crate::pipeline::close_cached_write_handles_under(&staging).await;
                if let Err(e) = tokio::fs::remove_dir_all(&staging).await
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(
                        dir = %staging.display(),
                        error = %e,
                        "failed to clean up staging directory on job failure"
                    );
                }
            });
        }
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.active_download_passes.remove(&job_id);
        self.jobs_finalizing_download.remove(&job_id);
        self.active_downloads_by_job.remove(&job_id);
        self.active_download_connections_by_job.remove(&job_id);
        self.active_completion_critical_connections_by_job
            .remove(&job_id);
        self.active_downloads_by_file
            .retain(|file_id, _| file_id.job_id != job_id);
        self.active_decodes_by_job.remove(&job_id);
        self.active_decodes_by_file
            .retain(|file_id, _| file_id.job_id != job_id);
        self.active_decode_bytes
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.file_hash_states
            .retain(|file_id, _| file_id.job_id != job_id);
        self.expected_file_crcs
            .retain(|file_id, _| file_id.job_id != job_id);
        self.file_hash_reread_required
            .retain(|file_id| file_id.job_id != job_id);
        self.unverified_segments
            .retain(|file_id, _| file_id.job_id != job_id);
        self.file_crc_recoveries
            .retain(|file_id, _| file_id.job_id != job_id);
        self.pending_retries_by_job.remove(&job_id);
        self.pending_retries_by_segment
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.rate_limit_reservations
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        self.remove_pending_completion_check(job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_phase_progress_runtime(job_id);
        self.clear_job_retention_excludes(job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        (released_repair, released_extract)
    }

    fn finish_failed_job(
        &mut self,
        job_id: JobId,
        error: String,
        released_repair: bool,
        released_extract: bool,
    ) {
        self.record_job_history(job_id, Some(PipelineEvent::JobFailed { job_id, error }));
        self.job_order.retain(|id| *id != job_id);
        if released_repair {
            self.promote_queued_repairs();
        }
        if released_extract {
            self.promote_queued_extractions();
        }
        self.publish_snapshot();
    }

    /// Spawn a dedicated STAT probe task to quickly estimate job health.
    ///
    /// Instead of pushing probes into the download queue (where they compete
    /// with 150k+ regular segments), this issues batched STAT checks through
    /// the high-level NNTP client so probes inherit the normal server ordering,
    /// failover, and soft-timeout semantics used by real downloads.
    ///
    /// e.g. 150k segs × 8% = ~12k probes / 50 per batch = ~240 batched checks.
    /// If every single probe returns 430 across the usable server set, the job
    /// is failed immediately.
    ///
    /// The job's status is deliberately left alone. A probe is a handful of
    /// STAT round trips running alongside a download that is still going; the
    /// job is downloading, and saying so kept it out of dispatch decisions,
    /// completion scheduling and the capacity budget that all read the status.
    pub(super) fn activate_health_probes(&mut self, job_id: JobId) -> bool {
        // The failing-file count this round was armed on: another file failing
        // is what re-arms the next one, and a second dead article in a file
        // already known to be missing is not that.
        let failing_files = self.health_failing_file_count(job_id);
        // Set the flag, then collect probes separately to avoid borrow conflicts.
        let probe_round = match self.jobs.get_mut(&job_id) {
            Some(s) => {
                s.health_probing = true;
                s.health_probe_failing_files = failing_files;
                let probe_round = s.health_probe_round;
                s.health_probe_round = s.health_probe_round.wrapping_add(1);
                probe_round
            }
            None => return false,
        };

        // Build probe list from the immutable job spec.
        let state = self.jobs.get(&job_id).unwrap();
        let all_segments = Self::health_probe_candidates(&state.spec);

        let total_segs = all_segments.len();
        if total_segs == 0 {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                let decided = Self::health_decision_failed_bytes(state);
                state.last_health_probe_failed_bytes = decided;
                state.next_health_probe_failed_bytes = decided.saturating_add(1);
                state.health_probing = false;
                let held = std::mem::take(&mut state.held_segments);
                for work in held {
                    if work.is_recovery {
                        state.recovery_queue.push(work);
                    } else {
                        state.download_queue.push(work);
                    }
                }
            }
            if !self.job_has_pending_download_pipeline_work(job_id) {
                self.schedule_job_completion_check(job_id);
            }
            return false;
        }

        // Rotate evenly strided samples across rounds so repeat probes widen
        // coverage instead of re-checking the same optimistic slice forever.
        let probe_indexes = Self::health_probe_sample_indices(total_segs, probe_round);

        // Collect the message ids for each probe.
        let probes: Vec<String> = probe_indexes
            .into_iter()
            .map(|i| all_segments[i].clone())
            .collect();

        let probe_count = probes.len();
        let nntp = Arc::clone(&self.nntp);
        let probe_tx = self.probe_result_tx.clone();
        // The probe rides the download lanes rather than competing with them.
        // An owned lane that has run out of work keeps its connection — and
        // with it one of the server's connection permits — cached, so a probe
        // that wanted a connection of its own had to take a permit off an idle
        // lane and then pay a full cold dial for it. Asking the lane to run
        // the STAT batch on the socket it is already holding costs neither.
        let owned_lane_probe = self.owned_download_lane_pool.probe_handle();

        info!(
            job_id = job_id.0,
            probe_round,
            probes = probe_count,
            total_segments = total_segs,
            "health probe activated — batched STAT sampling"
        );

        // Spawn a dedicated task that checks articles in batches via the NNTP
        // client. The client handles batching per server, load-aware ordering,
        // failover, and soft timeouts so probes match the real downloader path.
        tokio::spawn(async move {
            info!(
                job_id = job_id.0,
                probe_round,
                probes = probe_count,
                "health probe starting"
            );

            let mut missed: usize = 0;
            let mut checked: usize = 0;
            const BATCH_SIZE: usize = 50;
            const UPDATE_INTERVAL: usize = 10;
            let mut batches_since_update: usize = 0;

            for batch in probes.chunks(BATCH_SIZE) {
                // Idle owned lanes answer on their own warm connections; the
                // async client is the fallback for whatever they cannot settle
                // — a server with no lane of its own, or a lane that faulted.
                let results = owned_lane_probe
                    .confirm_exists_for_probe(&nntp, batch)
                    .await;
                if results.inconclusive {
                    warn!("health probe: confirmation batch inconclusive, aborting probe");
                    let _ = probe_tx
                        .send(ProbeUpdate {
                            job_id,
                            probe_round,
                            total: checked,
                            missed,
                            done: true,
                            inconclusive: true,
                        })
                        .await;
                    return;
                }

                for exists in results.exists {
                    checked += 1;
                    if !exists {
                        missed += 1;
                    }
                }

                batches_since_update += 1;
                if batches_since_update >= UPDATE_INTERVAL {
                    batches_since_update = 0;
                    let _ = probe_tx
                        .send(ProbeUpdate {
                            job_id,
                            probe_round,
                            total: checked,
                            missed,
                            done: false,
                            inconclusive: false,
                        })
                        .await;
                }
            }

            info!(
                job_id = job_id.0,
                probes = probe_count,
                missed,
                "health probe complete"
            );
            let _ = probe_tx
                .send(ProbeUpdate {
                    job_id,
                    probe_round,
                    total: probe_count,
                    missed,
                    done: true,
                    inconclusive: false,
                })
                .await;
        });
        true
    }
}
