use super::*;

impl Pipeline {
    pub(in crate::pipeline::download) fn status_allows_download_dispatch(
        status: &JobStatus,
    ) -> bool {
        matches!(
            status,
            JobStatus::Queued
                | JobStatus::Downloading
                | JobStatus::Checking
                | JobStatus::Verifying
                | JobStatus::QueuedRepair
                | JobStatus::Repairing
                | JobStatus::QueuedExtract
                | JobStatus::Extracting
        )
    }

    pub(in crate::pipeline::download::worker) fn hot_dispatch_job_priority(
        &self,
        job_id: JobId,
    ) -> Option<u8> {
        self.jobs.get(&job_id).map(Self::job_dispatch_priority)
    }

    pub(in crate::pipeline::download::worker) fn job_has_dispatchable_work(
        &mut self,
        job_id: JobId,
    ) -> bool {
        let uu_capped = !self.uu_files.is_empty() && self.uu_spool_dispatch_capped();
        let direct_admission = self.direct_store_admission(job_id, &[]);
        let sweep_held = self.demotion_sweep_held_file_indices(job_id);
        let checkpoint = self.checkpoint_admission(job_id);
        self.jobs.get(&job_id).is_some_and(|state| {
            if !Self::status_allows_download_dispatch(&state.status) {
                return false;
            }
            // A retained-byte cap is local to its set. Reporting raw queue
            // occupancy here would prevent spillover from using free lanes.
            if !direct_admission.is_empty() || sweep_held.is_some() || checkpoint.enforced {
                return state
                    .download_queue
                    .peek_first_matching(|work| {
                        direct_admission.iter().all(|set| set.allows(work))
                            && checkpoint.decision(work, &[]).allows()
                            && sweep_held.as_deref().is_none_or(|held| {
                                !held.contains(&work.segment_id.file_id.file_index)
                            })
                            && (!uu_capped
                                || self
                                    .uu_files
                                    .get(&work.segment_id.file_id)
                                    .is_none_or(|uu| {
                                        uu.next_index == work.segment_id.segment_number
                                    }))
                    })
                    .is_some();
            }
            if !uu_capped {
                return !state.download_queue.is_empty();
            }
            // A blocked UU head must not hide another encoding deeper in
            // either heap. Reuse per-file counts instead of scanning articles
            // or allocating a cursor map for this availability check.
            let queued_uu: usize = self
                .uu_files
                .keys()
                .filter(|file_id| file_id.job_id == job_id)
                .map(|file_id| state.download_queue.queued_count_for_file(*file_id) as usize)
                .sum();
            state.download_queue.len() > queued_uu
                || state
                    .download_queue
                    .peek_next_matching(|work| {
                        self.uu_files
                            .get(&work.segment_id.file_id)
                            .is_none_or(|uu| uu.next_index == work.segment_id.segment_number)
                    })
                    .is_some()
        })
    }

    #[cfg(test)]
    pub(in crate::pipeline) fn job_has_dispatchable_work_for_test(
        &mut self,
        job_id: JobId,
    ) -> bool {
        self.job_has_dispatchable_work(job_id)
    }

    pub(in crate::pipeline::download::worker) fn job_has_completion_critical_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state.download_queue.has_completion_critical_work()
                && Self::status_allows_download_dispatch(&state.status)
        })
    }

    pub(in crate::pipeline::download::worker) fn job_has_noncritical_download_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state.download_queue.has_noncritical_work()
                && Self::status_allows_download_dispatch(&state.status)
        })
    }

    pub(in crate::pipeline::download::worker) fn job_has_active_download_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0
            || self
                .active_download_connections_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0)
                > 0
    }

    pub(in crate::pipeline::download::worker) fn job_can_remain_hot(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            Self::status_allows_download_dispatch(&state.status)
                && (!state.download_queue.is_empty() || self.job_has_active_download_work(job_id))
        })
    }

    pub(in crate::pipeline::download::worker) fn start_hot_dispatch_period(
        &mut self,
        job_id: JobId,
        now: Instant,
    ) {
        if self.hot_dispatch_job == Some(job_id) {
            if self.hot_dispatch_started_at.is_none() {
                self.hot_dispatch_started_at = Some(now);
            }
            return;
        }

        self.hot_dispatch_job = Some(job_id);
        self.hot_dispatch_started_at = Some(now);
        self.hot_dispatch_last_lend_at = None;
        self.hot_dispatch_underfill_since = None;
        self.hot_dispatch_throughput_window.clear();
        self.hot_dispatch_spillover_loans.clear();
        self.hot_share_yield_signal.clear();
    }

    pub(in crate::pipeline::download::worker) fn clear_hot_dispatch_period(&mut self) {
        self.hot_dispatch_job = None;
        self.hot_dispatch_started_at = None;
        self.hot_dispatch_last_lend_at = None;
        self.hot_dispatch_underfill_since = None;
        self.hot_dispatch_throughput_window.clear();
        self.hot_dispatch_spillover_loans.clear();
        self.hot_share_yield_signal.clear();
        self.refresh_hot_dispatch_loans(Instant::now());
    }

    pub(in crate::pipeline::download::worker) fn active_spillover_connections(&self) -> usize {
        let Some(hot_job_id) = self.hot_dispatch_job else {
            return 0;
        };
        let hot_connections = self
            .active_download_connections_by_job
            .get(&hot_job_id)
            .copied()
            .unwrap_or(0);
        self.active_download_connections
            .saturating_sub(hot_connections)
    }

    pub(in crate::pipeline::download::worker) fn hot_dispatch_speed_bps(
        &mut self,
        now: Instant,
    ) -> u64 {
        self.hot_dispatch_throughput_window.bps(now)
    }

    pub(in crate::pipeline::download::worker) fn update_spillover_loan_measurement(
        &mut self,
        now: Instant,
        hot_speed_bps: u64,
    ) {
        self.hot_dispatch_spillover_loans.update_speed_harm(
            now,
            hot_speed_bps,
            HOT_DISPATCH_SPILLOVER_HARM_PERCENT,
        );
    }

    pub(in crate::pipeline::download::worker) fn hot_spillover_reclaim_pending_for(
        &self,
        job_id: JobId,
    ) -> bool {
        self.hot_dispatch_spillover_loans
            .reclaim_pending_for(job_id)
    }

    pub(in crate::pipeline::download::worker) fn clear_spillover_loan_if_idle(&mut self) {
        if self.active_spillover_connections() == 0 {
            self.hot_dispatch_spillover_loans.clear();
        }
    }

    /// Advance the hot job's throughput estimate and re-judge the outstanding
    /// spillover loans against it, so a loan that is costing the hot job speed
    /// is marked for reclaim before the next refill decides.
    pub(in crate::pipeline::download::worker) fn refresh_hot_dispatch_loans(
        &mut self,
        now: Instant,
    ) {
        let hot_speed_bps = self.hot_dispatch_speed_bps(now);
        self.update_spillover_loan_measurement(now, hot_speed_bps);
    }

    pub(in crate::pipeline::download) fn job_dispatch_priority(state: &JobState) -> u8 {
        state
            .spec
            .metadata
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("priority"))
            .map(|(_, value)| {
                if value.eq_ignore_ascii_case("high") {
                    0
                } else if value.eq_ignore_ascii_case("low") {
                    2
                } else {
                    1
                }
            })
            .unwrap_or(1)
    }

    /// Whether the hot job can still use more capacity: either it has queued
    /// dispatchable work and connections remain to give it (`LaneCapacityAvailable`,
    /// meaning dispatch just hasn't caught up yet), or it has queued work and
    /// capacity is already full of it (`HotHasQueuedPrimary`). Either way, the
    /// hot job is not "slow" — spillover only ever engages once this returns
    /// `None`. Per-server pipeline-depth proving (`choose_download_lane_mode_for_server`)
    /// still runs on its own schedule elsewhere; it is protocol capability
    /// detection, not a reason to withhold spillover, so it plays no part here.
    pub(in crate::pipeline::download::worker) fn hot_best_mode_block_reason(
        &mut self,
        hot_job_id: JobId,
        max_connections: usize,
    ) -> HotBestModeBlockReason {
        if self.job_has_dispatchable_work(hot_job_id) {
            if self.active_download_connections < max_connections {
                return HotBestModeBlockReason::LaneCapacityAvailable;
            }
            return HotBestModeBlockReason::HotHasQueuedPrimary;
        }
        HotBestModeBlockReason::None
    }

    pub(in crate::pipeline::download::worker) fn select_hot_dispatch_job(
        &mut self,
        eligible: &[(u8, usize, JobId)],
        now: Instant,
    ) -> Option<(u8, JobId)> {
        // A job whose only queued work is completion-critical has nothing
        // left for the fill phase once phase 1 drains it — critical demand
        // is unconditional now, so it drains regardless of who is "hot".
        // Preferring candidates that still have regular work avoids handing
        // hot status to such a job only to relinquish it again next pass,
        // which would otherwise reset hot-dispatch state (throughput window,
        // spillover loans) on every toggle. Fall back to the full eligible
        // set when nothing has regular work — there is nothing to prefer.
        let has_completion_critical_work = eligible
            .iter()
            .any(|(_, _, job_id)| self.job_has_completion_critical_work(*job_id));
        let noncritical_candidates = has_completion_critical_work
            .then(|| {
                eligible
                    .iter()
                    .copied()
                    .filter(|(_, _, job_id)| self.job_has_noncritical_download_work(*job_id))
                    .collect::<Vec<_>>()
            })
            .filter(|candidates| !candidates.is_empty());
        if noncritical_candidates.is_some()
            && self
                .hot_dispatch_job
                .is_some_and(|job_id| !self.job_has_noncritical_download_work(job_id))
        {
            self.clear_hot_dispatch_period();
        }
        let candidates = noncritical_candidates.as_deref().unwrap_or(eligible);

        let top_eligible_priority = candidates.first().map(|(priority, _, _)| *priority);

        if let Some(current_job_id) = self.hot_dispatch_job
            && self.job_can_remain_hot(current_job_id)
            && let Some(current_priority) = self.hot_dispatch_job_priority(current_job_id)
            && top_eligible_priority.is_none_or(|priority| current_priority <= priority)
        {
            self.start_hot_dispatch_period(current_job_id, now);
            return Some((current_priority, current_job_id));
        }

        let (priority, _, job_id) = candidates.first().copied()?;
        self.start_hot_dispatch_period(job_id, now);
        Some((priority, job_id))
    }

    pub(in crate::pipeline::download::worker) fn start_spillover_loan(
        &mut self,
        job_id: JobId,
        now: Instant,
        hot_speed_bps: u64,
        kind: SpilloverLoanKind,
    ) {
        self.hot_dispatch_spillover_loans
            .start_or_extend(job_id, now, hot_speed_bps, kind);
        self.hot_dispatch_last_lend_at = Some(now);
    }
}
