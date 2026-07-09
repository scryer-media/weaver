use super::*;

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn status_allows_download_dispatch(
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
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            !state.download_queue.is_empty() && Self::status_allows_download_dispatch(&state.status)
        })
    }

    pub(in crate::pipeline::download::worker) fn job_has_primary_download_work(
        &self,
        job_id: JobId,
    ) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            state.download_queue.has_primary_work()
                && Self::status_allows_download_dispatch(&state.status)
        })
    }

    pub(in crate::pipeline::download::worker) fn hot_share_yield_unmet(
        &self,
        plan: &BoundedSameBandSharePlan,
    ) -> bool {
        self.active_download_connections_by_job
            .get(&plan.hot_job_id)
            .copied()
            .unwrap_or(0)
            > plan.hot_target
            && self.hot_dispatch_spillover_loans.bounded_lent_connections() < plan.share_target
    }

    pub(in crate::pipeline::download::worker) fn update_hot_share_yield_signal(
        &self,
        plan: Option<&BoundedSameBandSharePlan>,
    ) {
        if let Some(plan) = plan
            && self.hot_share_yield_unmet(plan)
        {
            self.hot_share_yield_signal.request(plan.hot_job_id);
            return;
        }
        self.hot_share_yield_signal.clear();
    }

    pub(in crate::pipeline::download::worker) fn set_hot_best_mode_block_reason(
        &self,
        reason: HotBestModeBlockReason,
    ) {
        self.metrics
            .hot_dispatch_best_mode_block_reason
            .store(reason.as_code(), Ordering::Relaxed);
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
        self.hot_dispatch_successes = 0;
        self.hot_dispatch_exclusive_peak_bps = 0;
        self.hot_dispatch_last_lend_at = None;
        self.hot_dispatch_mode = DispatchShareMode::Exclusive;
        self.hot_dispatch_underfill_since = None;
        self.hot_dispatch_last_spillover_decision = SpilloverDecision::None;
        self.hot_dispatch_throughput_window.clear();
        self.hot_dispatch_exclusive_window.clear();
        self.hot_dispatch_expansion_window.clear();
        self.hot_dispatch_spillover_loans.clear();
        self.hot_share_yield_signal.clear();
        self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
    }

    pub(in crate::pipeline::download::worker) fn clear_hot_dispatch_period(&mut self) {
        self.hot_dispatch_job = None;
        self.hot_dispatch_started_at = None;
        self.hot_dispatch_successes = 0;
        self.hot_dispatch_exclusive_peak_bps = 0;
        self.hot_dispatch_last_lend_at = None;
        self.hot_dispatch_mode = DispatchShareMode::Exclusive;
        self.hot_dispatch_underfill_since = None;
        self.hot_dispatch_last_spillover_decision = SpilloverDecision::None;
        self.hot_dispatch_throughput_window.clear();
        self.hot_dispatch_exclusive_window.clear();
        self.hot_dispatch_expansion_window.clear();
        self.hot_dispatch_spillover_loans.clear();
        self.hot_share_yield_signal.clear();
        self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(in crate::pipeline::download::worker) fn hot_dispatch_warmup_complete(
        &self,
        now: Instant,
    ) -> bool {
        let Some(started_at) = self.hot_dispatch_started_at else {
            return false;
        };
        let elapsed = now.saturating_duration_since(started_at);
        elapsed >= HOT_DISPATCH_WARMUP_MIN_DURATION
            && (self.hot_dispatch_successes >= HOT_DISPATCH_MIN_SUCCESSFUL_PRIMARY_BODIES
                || elapsed >= HOT_DISPATCH_FORCE_UNDERFILL_AFTER)
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
        if self.hot_dispatch_spillover_loans.update_speed_harm(
            now,
            hot_speed_bps,
            HOT_DISPATCH_SPILLOVER_HARM_PERCENT,
        ) {
            self.record_spillover_decision(SpilloverDecision::ReclaimedSpeedHarm);
        }
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

    pub(in crate::pipeline::download::worker) fn publish_hot_dispatch_metrics(
        &mut self,
        now: Instant,
    ) {
        let hot_job_id = self.hot_dispatch_job.map(|id| id.0).unwrap_or(0);
        let active_non_hot_connections = self.active_spillover_connections();
        let active_lent_connections = self.hot_dispatch_spillover_loans.active_lent_connections();
        let published_mode = if active_non_hot_connections > 0 {
            DispatchShareMode::Shared
        } else {
            self.hot_dispatch_mode
        };
        let underfill_ms = self
            .hot_dispatch_underfill_since
            .map(|started_at| {
                now.saturating_duration_since(started_at)
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64
            })
            .unwrap_or(0);
        let warmup_complete = self.hot_dispatch_warmup_complete(now);
        let hot_speed_bps = self.hot_dispatch_speed_bps(now);
        self.hot_dispatch_expansion_window
            .refresh(now, hot_speed_bps);
        let recent_expansion_improvement_pct = self
            .hot_dispatch_expansion_window
            .recent_improvement_pct(now);
        self.update_spillover_loan_measurement(now, hot_speed_bps);

        if hot_job_id != 0 && published_mode == DispatchShareMode::Exclusive {
            self.hot_dispatch_exclusive_peak_bps =
                self.hot_dispatch_exclusive_peak_bps.max(hot_speed_bps);
            self.hot_dispatch_exclusive_window.record(hot_speed_bps);
        }

        self.metrics
            .hot_dispatch_job_id
            .store(hot_job_id, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_mode
            .store(published_mode.as_code(), Ordering::Relaxed);
        self.metrics
            .hot_dispatch_underfill_ms
            .store(underfill_ms, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_lent_connections
            .store(active_lent_connections, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_warmup_complete
            .store(usize::from(warmup_complete), Ordering::Relaxed);
        self.metrics.hot_dispatch_last_spillover_decision.store(
            self.hot_dispatch_last_spillover_decision.as_code(),
            Ordering::Relaxed,
        );
        self.metrics
            .hot_dispatch_hot_speed_bps
            .store(hot_speed_bps, Ordering::Relaxed);
        self.metrics.hot_dispatch_exclusive_peak_bps.store(
            self.hot_dispatch_exclusive_window.peak_bps(),
            Ordering::Relaxed,
        );
        let (pre_lend, post_lend, active_loans) =
            self.hot_dispatch_spillover_loans.speed_snapshot();
        self.metrics
            .hot_dispatch_spillover_pre_speed_bps
            .store(pre_lend, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_spillover_post_speed_bps
            .store(post_lend, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_spillover_active_loans
            .store(active_loans, Ordering::Relaxed);
        self.metrics
            .hot_dispatch_recent_expansion_improvement_pct
            .store(recent_expansion_improvement_pct, Ordering::Relaxed);
        if let Some(event) = self.hot_dispatch_expansion_window.last_event() {
            self.metrics
                .hot_dispatch_last_expansion_kind
                .store(event.kind.as_code(), Ordering::Relaxed);
            self.metrics
                .hot_dispatch_last_expansion_before_bps
                .store(event.before_bps, Ordering::Relaxed);
            self.metrics
                .hot_dispatch_last_expansion_after_bps
                .store(event.after_bps.unwrap_or(0), Ordering::Relaxed);
        } else {
            self.metrics
                .hot_dispatch_last_expansion_kind
                .store(0, Ordering::Relaxed);
            self.metrics
                .hot_dispatch_last_expansion_before_bps
                .store(0, Ordering::Relaxed);
            self.metrics
                .hot_dispatch_last_expansion_after_bps
                .store(0, Ordering::Relaxed);
        }
    }

    pub(in crate::pipeline::download::worker) fn record_spillover_decision(
        &mut self,
        decision: SpilloverDecision,
    ) {
        self.hot_dispatch_last_spillover_decision = decision;
        self.metrics
            .hot_dispatch_last_spillover_decision
            .store(decision.as_code(), Ordering::Relaxed);
        match decision {
            SpilloverDecision::None => {}
            SpilloverDecision::BlockedWarmup => {
                self.metrics
                    .hot_dispatch_spillover_blocked_warmup_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedPressure => {
                self.metrics
                    .hot_dispatch_spillover_blocked_pressure_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedNearCap => {
                self.metrics
                    .hot_dispatch_spillover_blocked_near_cap_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedHotCanUseCapacity => {
                self.metrics
                    .hot_dispatch_spillover_blocked_hot_can_use_capacity_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::AllowedUnderfill => {
                self.metrics
                    .hot_dispatch_spillover_allowed_underfill_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::AllowedMeasuredUnderfill => {
                self.metrics
                    .hot_dispatch_spillover_allowed_measured_underfill_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::AllowedBoundedSameBand => {
                self.metrics
                    .hot_dispatch_spillover_allowed_bounded_same_band_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::Reclaimed => {
                self.metrics
                    .hot_dispatch_spillover_reclaimed_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::ReclaimedSpeedHarm => {
                self.metrics
                    .hot_dispatch_spillover_reclaimed_speed_harm_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedBestModePending => {
                self.metrics
                    .hot_dispatch_spillover_blocked_best_mode_pending_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedRecentExpansionHelped => {
                self.metrics
                    .hot_dispatch_spillover_blocked_recent_expansion_helped_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            SpilloverDecision::BlockedCapSpeed => {
                self.metrics
                    .hot_dispatch_spillover_blocked_cap_speed_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(in crate::pipeline::download::worker) fn block_or_reclaim_spillover(
        &mut self,
        decision: SpilloverDecision,
    ) {
        if self.hot_dispatch_mode == DispatchShareMode::Shared {
            let reclaim_decision = match decision {
                SpilloverDecision::ReclaimedSpeedHarm => SpilloverDecision::ReclaimedSpeedHarm,
                _ => SpilloverDecision::Reclaimed,
            };
            self.record_spillover_decision(reclaim_decision);
        } else {
            self.record_spillover_decision(decision);
        }
        self.hot_dispatch_mode = DispatchShareMode::Exclusive;
    }

    pub(in crate::pipeline::download::worker) fn job_dispatch_priority(state: &JobState) -> u8 {
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

    pub(in crate::pipeline::download::worker) fn bounded_same_band_share_plan(
        &self,
        eligible: &[(u8, usize, JobId)],
        hot_priority: u8,
        hot_job_id: JobId,
        max_connections: usize,
        pressure: DownloadPressure,
        bandwidth_cap_tight: bool,
    ) -> Option<BoundedSameBandSharePlan> {
        if pressure.state != DownloadPressureState::Clear
            || bandwidth_cap_tight
            || max_connections < HOT_DISPATCH_BOUNDED_SHARE_MIN_CONNECTIONS
            || !self.job_has_primary_download_work(hot_job_id)
        {
            return None;
        }

        let peer_jobs = eligible
            .iter()
            .filter_map(|(priority, _, job_id)| {
                (*job_id != hot_job_id
                    && *priority == hot_priority
                    && self.job_has_primary_download_work(*job_id))
                .then_some(*job_id)
            })
            .collect::<Vec<_>>();
        if peer_jobs.is_empty() {
            return None;
        }

        let share_target = (max_connections / 4).min(HOT_DISPATCH_BOUNDED_SHARE_MAX_LANES);
        if share_target == 0 {
            return None;
        }

        Some(BoundedSameBandSharePlan {
            hot_job_id,
            hot_target: max_connections.saturating_sub(share_target),
            share_target,
            peer_jobs,
        })
    }

    pub(in crate::pipeline::download::worker) fn hot_job_has_pending_pipeline_promotion(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
    ) -> bool {
        if pressure.state != DownloadPressureState::Clear {
            return false;
        }
        let Some(profile) = self.ensure_job_transport_profile(job_id) else {
            return false;
        };
        let job_class = profile.class();
        let median_body_bytes = profile.median_body_bytes();
        let now = Instant::now();
        let server_count = self.nntp.pool().server_count();
        (0..server_count).any(|server_idx| {
            let default_proof;
            let proof =
                if let Some(proof) = self.download_lane_runtime.server_proof.get(&server_idx) {
                    proof
                } else {
                    default_proof = ServerPipelineProof::default();
                    &default_proof
                };
            let current = proof.state(now);
            let mode = self.choose_download_lane_mode_for_server(
                now,
                server_idx,
                proof,
                job_class,
                median_body_bytes,
                true,
            );
            matches!(
                (current, mode),
                (
                    ServerPipelineState::Unknown | ServerPipelineState::SequentialOnly,
                    DownloadLaneMode::PipelineDepth2
                ) | (
                    ServerPipelineState::PipelineProvenDepth2,
                    DownloadLaneMode::PipelineDepth4
                )
            )
        })
    }

    pub(in crate::pipeline::download::worker) fn hot_best_mode_block_reason(
        &mut self,
        hot_job_id: JobId,
        max_connections: usize,
        pressure: DownloadPressure,
        recent_expansion_helped: bool,
    ) -> HotBestModeBlockReason {
        if self.job_has_dispatchable_work(hot_job_id) {
            if self.active_download_connections < max_connections {
                return HotBestModeBlockReason::LaneCapacityAvailable;
            }
            return HotBestModeBlockReason::HotHasQueuedPrimary;
        }
        if self.hot_job_has_pending_pipeline_promotion(hot_job_id, pressure) {
            return HotBestModeBlockReason::PipelinePromotionPending;
        }
        if recent_expansion_helped {
            return HotBestModeBlockReason::RecentExpansionHelped;
        }
        HotBestModeBlockReason::None
    }

    pub(in crate::pipeline::download::worker) fn select_hot_dispatch_job(
        &mut self,
        eligible: &[(u8, usize, JobId)],
        now: Instant,
    ) -> Option<(u8, JobId)> {
        let top_eligible_priority = eligible.first().map(|(priority, _, _)| *priority);

        if let Some(current_job_id) = self.hot_dispatch_job
            && self.job_can_remain_hot(current_job_id)
            && let Some(current_priority) = self.hot_dispatch_job_priority(current_job_id)
            && top_eligible_priority.is_none_or(|priority| current_priority <= priority)
        {
            self.start_hot_dispatch_period(current_job_id, now);
            return Some((current_priority, current_job_id));
        }

        let (priority, _, job_id) = eligible.first().copied()?;
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
