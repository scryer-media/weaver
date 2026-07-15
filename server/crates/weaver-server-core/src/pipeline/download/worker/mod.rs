use super::*;
use crate::pipeline::download::transport::{
    JobTransportClass, ServerPipelineProof, ServerPipelineState,
};
use weaver_nntp::client::FetchAttemptOutcome;

mod completion;
mod hot;
mod ip_replacement;
mod lanes;
mod leases;
mod metrics;
mod pressure;
mod refill;
mod spawn;

#[cfg(test)]
pub(in crate::pipeline) use ip_replacement::{
    is_ip_replacement_policy_stop, should_neutrally_park_ip_replacement,
};
#[cfg(test)]
pub(in crate::pipeline) use spawn::lane_acquire_failure_for_work;

enum DispatchAttempt {
    Dispatched,
    NoWork,
    StopAll,
}

#[derive(Debug, Clone)]
struct BoundedSameBandSharePlan {
    hot_job_id: JobId,
    hot_target: usize,
    share_target: usize,
    peer_jobs: Vec<JobId>,
}

#[derive(Debug, Default, Clone, Copy)]
struct DownloadPipelineBacklog {
    active_downloads: usize,
    active_connections: usize,
    active_decodes: usize,
    delayed_retries: usize,
    released_results: usize,
    released_result_bytes: u64,
    pending_decodes: usize,
    buffered_write_segments: usize,
    buffered_write_bytes: u64,
}

const DOWNLOAD_PRESSURE_SOFT_PERCENT: u64 = 70;
const SOFT_PRESSURE_DISPATCH_MAX_DELAY: Duration = Duration::from_millis(150);
const SOFT_PRESSURE_DISPATCH_MIN_DELAY: Duration = Duration::from_millis(1);
const SAB_BODY_PIPELINE_DEPTH: usize = 2;
const HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT: usize = 64;
const HOT_LEASE_TARGET_RUNWAY_SECS: u64 = 2;
const HOT_LEASE_WARMUP_WORK_LIMIT: usize = 16;
const NO_ELIGIBLE_SERVER_WARN_INTERVAL: Duration = Duration::from_secs(60);
const BODY_FETCH_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
const HOT_DISPATCH_WARMUP_MIN_DURATION: Duration = Duration::from_secs(2);
const HOT_DISPATCH_FORCE_UNDERFILL_AFTER: Duration = Duration::from_secs(5);
const HOT_DISPATCH_MIN_SUCCESSFUL_PRIMARY_BODIES: u64 = 24;
const HOT_DISPATCH_UNDERFILL_WINDOW: Duration = Duration::from_secs(2);
const HOT_DISPATCH_EXPANSION_HELPFUL_PERCENT: u64 = 5;
const HOT_DISPATCH_SPILLOVER_HARM_PERCENT: u64 = 7;
const HOT_DISPATCH_BOUNDED_SHARE_MIN_CONNECTIONS: usize = 4;
const HOT_DISPATCH_BOUNDED_SHARE_MAX_LANES: usize = 3;
const LANE_REFILL_GRACE: Duration = Duration::from_millis(5);
const IP_REPLACEMENT_MIN_OLD_SAMPLES: u16 = 16;
const IP_REPLACEMENT_MIN_OLD_AGE: Duration = Duration::from_secs(30);
const IP_REPLACEMENT_BASELINE_MIN_SAMPLES: u16 = 8;
const IP_REPLACEMENT_BASELINE_RECENT: Duration = Duration::from_secs(10 * 60);
const IP_REPLACEMENT_OLD_SLOWER_RATIO: f64 = 1.25;
const IP_REPLACEMENT_OLD_SLOWER_MS: f64 = 75.0;
const IP_REPLACEMENT_TRIAL_SAMPLES: usize = 4;
const IP_REPLACEMENT_CANDIDATE_BETTER_RATIO: f64 = 0.85;
const IP_REPLACEMENT_CANDIDATE_BETTER_MS: f64 = 40.0;
const DOWNLOAD_RESTART_DURABLE_LEAD_RETRY_DELAY: Duration = Duration::from_millis(250);
const BODY_LANE_UNAVAILABLE_RETRY_DELAY: Duration = Duration::from_millis(250);
const DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub(crate) struct DownloadPressure {
    state: DownloadPressureState,
    reason: DownloadPressureReason,
    decode_backlog_bytes: u64,
    write_buffered_bytes: u64,
    decode_hard_limit_bytes: u64,
    write_hard_limit_bytes: u64,
}

impl Pipeline {
    fn try_dispatch_download_for_job(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
        spillover_loan_kind: Option<SpilloverLoanKind>,
    ) -> DispatchAttempt {
        if let Some(ready_at) = self
            .download_restart_durable_lead_retry_after
            .get(&job_id)
            .copied()
            && ready_at > Instant::now()
        {
            let backlog = self.download_pipeline_backlog_for_job(job_id);
            if backlog.has_durable_catch_up_work() {
                if self.next_queued_download_exceeds_restart_durable_lead(job_id) {
                    self.flush_file_progress_batch(
                        "download.file_progress.flush.restart_durable_lead_retry_recheck",
                    );
                }
                if self.next_queued_download_exceeds_restart_durable_lead(job_id) {
                    self.update_queue_metrics();
                    return DispatchAttempt::NoWork;
                }
            }
            self.download_restart_durable_lead_retry_after
                .remove(&job_id);
        }
        self.apply_rar_unlock_priorities_if_dirty(job_id);
        let mut lease = match self.try_lease_initial_download_batch(job_id, pressure) {
            Ok(Some(lease)) => lease,
            Ok(None) => return DispatchAttempt::NoWork,
            Err(attempt) => return attempt,
        };
        lease.spillover_loan_kind = spillover_loan_kind;
        let activation_items = Self::activation_items(&lease);
        self.activate_download_batch_lease(&lease, &activation_items, true);
        self.spawn_download_batch(lease);
        DispatchAttempt::Dispatched
    }

    fn mark_download_pass_started(&mut self, job_id: JobId) {
        // Transition Queued → Downloading when the first segment is dispatched.
        if let Some(state) = self.jobs.get_mut(&job_id)
            && matches!(state.status, JobStatus::Queued)
        {
            let _ = state;
            self.transition_postprocessing_status(
                job_id,
                JobStatus::Downloading,
                Some("downloading"),
            );
        }
        self.note_download_activity(job_id);
        if self.active_download_passes.insert(job_id) {
            let total = self
                .jobs
                .get(&job_id)
                .map(|state| state.spec.total_bytes)
                .unwrap_or(0);
            let tuner_max = self.tuner.params().max_concurrent_downloads;
            info!(
                job_id = job_id.0,
                total_bytes = total,
                configured_server_count = self.nntp.pool().server_count(),
                tuner_max_connections = tuner_max,
                connection_ramp = self.connection_ramp,
                effective_connection_capacity =
                    self.effective_download_connection_capacity(tuner_max),
                "NNTP download pass started"
            );
            self.phase_begin(job_id, JobPhase::Downloading, Some(total));
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadStarted { job_id });
        }
    }

    pub(crate) fn maybe_finish_download_pass(&mut self, job_id: JobId) {
        let in_flight = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        let has_remaining_work = self.jobs.get(&job_id).is_some_and(|state| {
            // Optional recovery files remain parked in `recovery_queue` until
            // explicitly promoted, so they must not keep a download pass open
            // once all dispatchable work has drained.
            !state.download_queue.is_empty()
        }) || self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;

        if in_flight == 0 && !has_remaining_work {
            self.emit_download_finished_if_active(job_id);
            self.schedule_job_completion_check(job_id);
        }
    }

    pub(crate) fn dispatch_downloads(&mut self) {
        let now = Instant::now();
        if self.global_paused || self.rate_limiter.should_wait() {
            self.hot_share_yield_signal.clear();
            if self.active_downloads == 0 {
                debug!(
                    global_paused = self.global_paused,
                    rate_wait = self.rate_limiter.should_wait(),
                    "dispatch blocked: paused/rate"
                );
            }
            self.publish_hot_dispatch_metrics(now);
            return;
        }
        if let Err(error) = self.refresh_bandwidth_cap_window() {
            error!(error = %error, "failed to refresh ISP bandwidth cap state");
            self.hot_share_yield_signal.clear();
            self.publish_hot_dispatch_metrics(now);
            return;
        }
        if self.bandwidth_cap.cap_enabled() && self.bandwidth_cap.remaining_bytes() == 0 {
            self.hot_share_yield_signal.clear();
            self.update_queue_metrics();
            if self.active_downloads == 0 {
                debug!("dispatch blocked: bandwidth cap exhausted");
            }
            self.publish_hot_dispatch_metrics(now);
            return;
        }

        let pressure = self.refresh_download_pressure();
        if pressure.is_hard() {
            self.hot_share_yield_signal.clear();
            self.update_queue_metrics();
            if self.active_downloads == 0 {
                debug!(
                    pressure_state = pressure.state.as_str(),
                    pressure_reason = pressure.reason.as_str(),
                    decode_backlog_bytes = pressure.decode_backlog_bytes,
                    decode_hard_limit_bytes = pressure.decode_hard_limit_bytes,
                    write_buffered_bytes = pressure.write_buffered_bytes,
                    write_hard_limit_bytes = pressure.write_hard_limit_bytes,
                    "dispatch blocked: byte pressure"
                );
            }
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedPressure);
            self.publish_hot_dispatch_metrics(now);
            return;
        }
        let soft_dispatch_delay = self.soft_pressure_dispatch_delay(pressure);
        let mut dispatch_budget = usize::MAX;
        if let Some(delay) = soft_dispatch_delay {
            let now = Instant::now();
            if self
                .download_pressure_soft_dispatch_after
                .is_some_and(|ready_at| ready_at > now)
            {
                self.hot_share_yield_signal.clear();
                self.update_queue_metrics();
                if self.active_downloads == 0 {
                    debug!(
                        pressure_state = pressure.state.as_str(),
                        pressure_reason = pressure.reason.as_str(),
                        decode_backlog_bytes = pressure.decode_backlog_bytes,
                        write_buffered_bytes = pressure.write_buffered_bytes,
                        "dispatch delayed: soft byte pressure"
                    );
                }
                self.block_or_reclaim_spillover(SpilloverDecision::BlockedPressure);
                self.publish_hot_dispatch_metrics(now);
                return;
            }
            self.download_pressure_soft_dispatch_after = Some(now + delay);
            dispatch_budget = 1;
        } else {
            self.download_pressure_soft_dispatch_after = None;
        }

        let params = self.tuner.params();
        let tuner_max = params.max_concurrent_downloads;
        let max = self.effective_download_connection_capacity(tuner_max);

        // Soft byte pressure keeps the current hot job moving, but avoids
        // expanding into spillover work until memory pressure drains.
        let suppress_spillover = pressure.suppresses_spillover();
        // When the bandwidth cap is within 15% of exhaustion, also revert to
        // single-job dispatch so remaining quota goes to the highest-priority job.
        let bandwidth_cap_tight = self.bandwidth_cap.cap_enabled()
            && self.bandwidth_cap.remaining_bytes() <= self.bandwidth_cap.limit_bytes() * 15 / 100;

        // Prefer higher submitted priority first. Within the top runnable band,
        // keep the already-active job hot when possible; otherwise choose FIFO
        // submission order. This matches NZBGet/SAB-style hot reuse more closely
        // than same-band round-robin.
        let mut eligible = self
            .job_order
            .iter()
            .enumerate()
            .filter_map(|(index, id)| {
                let state = self.jobs.get(id)?;
                if state.download_queue.is_empty()
                    || !Self::status_allows_download_dispatch(&state.status)
                {
                    return None;
                }
                Some((Self::job_dispatch_priority(state), index, *id))
            })
            .collect::<Vec<_>>();
        eligible.sort_unstable();

        if eligible.is_empty() && self.active_downloads == 0 {
            let mut drained_parked_recovery_jobs = Vec::new();
            for (i, jid) in self.job_order.iter().enumerate() {
                if let Some(s) = self.jobs.get(jid) {
                    let parked_recovery_only =
                        s.download_queue.is_empty() && !s.recovery_queue.is_empty();
                    let status_allows_dispatch = Self::status_allows_download_dispatch(&s.status);
                    let extraction_only_idle = parked_recovery_only
                        && matches!(s.status, JobStatus::QueuedExtract | JobStatus::Extracting);
                    let download_pipeline_draining = s.download_queue.is_empty()
                        && status_allows_dispatch
                        && self.job_has_pending_download_pipeline_work(*jid);
                    let should_schedule_completion = parked_recovery_only
                        && status_allows_dispatch
                        && !self.job_has_pending_download_pipeline_work(*jid);
                    if extraction_only_idle {
                        debug!(
                            job_id = jid.0,
                            idx = i,
                            status = ?s.status,
                            queue_len = s.download_queue.len(),
                            recovery_len = s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                            "dispatch idle: extraction-only recovery queued"
                        );
                    } else if should_schedule_completion {
                        debug!(
                            job_id = jid.0,
                            idx = i,
                            status = ?s.status,
                            queue_len = s.download_queue.len(),
                            recovery_len = s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                            "dispatch idle: parked recovery queued; scheduling completion check"
                        );
                        drained_parked_recovery_jobs.push(*jid);
                    } else if download_pipeline_draining {
                        debug!(
                            job_id = jid.0,
                            idx = i,
                            status = ?s.status,
                            queue_len = s.download_queue.len(),
                            recovery_len = s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                            "dispatch idle: download pipeline draining"
                        );
                    } else {
                        warn!(
                            job_id = jid.0,
                            idx = i,
                            status = ?s.status,
                            queue_len = s.download_queue.len(),
                            recovery_len = s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                            "dispatch stall: job not eligible"
                        );
                    }
                }
            }
            for job_id in drained_parked_recovery_jobs {
                self.schedule_job_completion_check_if_download_pipeline_drained(
                    job_id,
                    "parked_recovery_idle",
                );
            }
        }

        if !eligible.is_empty() && self.active_downloads == 0 {
            debug!(
                eligible_count = eligible.len(),
                max,
                tuner_max,
                connection_ramp = self.connection_ramp,
                rate_wait = self.rate_limiter.should_wait(),
                "dispatch: eligible jobs found, attempting dispatch"
            );
        }

        let eligible_count = eligible.len();
        let Some((hot_priority, hot_job_id)) = self.select_hot_dispatch_job(&eligible, now) else {
            self.clear_hot_dispatch_period();
            self.update_queue_metrics();
            return;
        };
        let bounded_share_plan = self.bounded_same_band_share_plan(
            &eligible,
            hot_priority,
            hot_job_id,
            max,
            pressure,
            bandwidth_cap_tight,
        );
        self.update_hot_share_yield_signal(bounded_share_plan.as_ref());

        let active_connections_before_dispatch = self.active_download_connections;
        let hot_target = bounded_share_plan
            .as_ref()
            .map(|plan| plan.hot_target)
            .unwrap_or(max);
        while self.active_download_connections < max
            && self
                .active_download_connections_by_job
                .get(&hot_job_id)
                .copied()
                .unwrap_or(0)
                < hot_target
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.try_dispatch_download_for_job(hot_job_id, pressure, None) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => break,
                DispatchAttempt::StopAll => {
                    self.publish_hot_dispatch_metrics(now);
                    return;
                }
            }
        }

        if let Some(plan) = bounded_share_plan.as_ref() {
            let mut bounded_slots = plan
                .share_target
                .saturating_sub(self.hot_dispatch_spillover_loans.bounded_lent_connections())
                .min(max.saturating_sub(self.active_download_connections))
                .min(dispatch_budget);
            let mut skipped_peer_jobs = Vec::new();
            while bounded_slots > 0
                && self.active_download_connections < max
                && !self.rate_limiter.should_wait()
                && dispatch_budget > 0
            {
                let Some(peer_job_id) = plan
                    .peer_jobs
                    .iter()
                    .enumerate()
                    .filter(|(_, job_id)| self.job_has_primary_download_work(**job_id))
                    .filter(|(_, job_id)| !skipped_peer_jobs.contains(*job_id))
                    .min_by_key(|(index, job_id)| {
                        (
                            self.active_download_connections_by_job
                                .get(job_id)
                                .copied()
                                .unwrap_or(0),
                            *index,
                        )
                    })
                    .map(|(_, job_id)| *job_id)
                else {
                    break;
                };

                match self.try_dispatch_download_for_job(
                    peer_job_id,
                    pressure,
                    Some(SpilloverLoanKind::BoundedSameBand),
                ) {
                    DispatchAttempt::Dispatched => {
                        self.hot_dispatch_mode = DispatchShareMode::Shared;
                        let hot_speed_bps = self.hot_dispatch_speed_bps(now);
                        self.start_spillover_loan(
                            peer_job_id,
                            now,
                            hot_speed_bps,
                            SpilloverLoanKind::BoundedSameBand,
                        );
                        self.record_spillover_decision(SpilloverDecision::AllowedBoundedSameBand);
                        dispatch_budget = dispatch_budget.saturating_sub(1);
                        bounded_slots = bounded_slots.saturating_sub(1);
                    }
                    DispatchAttempt::NoWork => {
                        skipped_peer_jobs.push(peer_job_id);
                    }
                    DispatchAttempt::StopAll => {
                        self.publish_hot_dispatch_metrics(now);
                        return;
                    }
                }
            }

            while self.active_download_connections < max
                && !self.rate_limiter.should_wait()
                && dispatch_budget > 0
            {
                match self.try_dispatch_download_for_job(hot_job_id, pressure, None) {
                    DispatchAttempt::Dispatched => {
                        dispatch_budget = dispatch_budget.saturating_sub(1)
                    }
                    DispatchAttempt::NoWork => break,
                    DispatchAttempt::StopAll => {
                        self.publish_hot_dispatch_metrics(now);
                        return;
                    }
                }
            }
            self.update_hot_share_yield_signal(Some(plan));
        }

        let hot_speed_bps = self.hot_dispatch_speed_bps(now);
        self.hot_dispatch_expansion_window
            .refresh(now, hot_speed_bps);
        let has_unused_capacity = self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0;
        let recent_expansion_improvement_pct = self
            .hot_dispatch_expansion_window
            .recent_improvement_pct(now);
        let recent_expansion_helped =
            recent_expansion_improvement_pct >= HOT_DISPATCH_EXPANSION_HELPFUL_PERCENT;
        let best_mode_block_reason =
            self.hot_best_mode_block_reason(hot_job_id, max, pressure, recent_expansion_helped);

        let spillover_allowed = if suppress_spillover {
            self.hot_share_yield_signal.clear();
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedPressure);
            false
        } else if bandwidth_cap_tight {
            self.hot_share_yield_signal.clear();
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedNearCap);
            false
        } else if bounded_share_plan.is_some() {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            if self.hot_dispatch_spillover_loans.active_lent_connections() == 0 {
                self.hot_dispatch_mode = DispatchShareMode::Exclusive;
            }
            false
        } else if best_mode_block_reason == HotBestModeBlockReason::HotHasQueuedPrimary {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(best_mode_block_reason);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedHotCanUseCapacity);
            false
        } else if matches!(
            best_mode_block_reason,
            HotBestModeBlockReason::LaneCapacityAvailable
                | HotBestModeBlockReason::PipelinePromotionPending
        ) {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(best_mode_block_reason);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedBestModePending);
            false
        } else if !has_unused_capacity {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            if self.hot_dispatch_spillover_loans.active_lent_connections() == 0 {
                self.hot_dispatch_mode = DispatchShareMode::Exclusive;
            }
            false
        } else if !self.hot_dispatch_warmup_complete(now) {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedWarmup);
            false
        } else if best_mode_block_reason == HotBestModeBlockReason::RecentExpansionHelped {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(best_mode_block_reason);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedRecentExpansionHelped);
            false
        } else {
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            let underfill_started_at = *self.hot_dispatch_underfill_since.get_or_insert(now);
            if now.saturating_duration_since(underfill_started_at) >= HOT_DISPATCH_UNDERFILL_WINDOW
            {
                self.hot_dispatch_mode = DispatchShareMode::Shared;
                true
            } else {
                self.block_or_reclaim_spillover(SpilloverDecision::BlockedHotCanUseCapacity);
                false
            }
        };

        if spillover_allowed {
            dispatch_budget = dispatch_budget.min(1);
            for (_, _, job_id) in eligible {
                if job_id == hot_job_id {
                    continue;
                }
                while self.active_download_connections < max
                    && !self.rate_limiter.should_wait()
                    && dispatch_budget > 0
                {
                    match self.try_dispatch_download_for_job(
                        job_id,
                        pressure,
                        Some(SpilloverLoanKind::MeasuredUnderfill),
                    ) {
                        DispatchAttempt::Dispatched => {
                            self.start_spillover_loan(
                                job_id,
                                now,
                                hot_speed_bps,
                                SpilloverLoanKind::MeasuredUnderfill,
                            );
                            self.record_spillover_decision(
                                SpilloverDecision::AllowedMeasuredUnderfill,
                            );
                            dispatch_budget = dispatch_budget.saturating_sub(1)
                        }
                        DispatchAttempt::NoWork => break,
                        DispatchAttempt::StopAll => {
                            self.publish_hot_dispatch_metrics(now);
                            return;
                        }
                    }
                }
                if self.active_download_connections >= max
                    || self.rate_limiter.should_wait()
                    || dispatch_budget == 0
                {
                    break;
                }
            }
        }

        if active_connections_before_dispatch == 0 && self.active_download_connections == 0 {
            self.log_download_dispatch_liveness_stall(now, pressure, max, eligible_count);
        }

        self.maybe_start_ip_replacement_trial(hot_job_id, pressure, max);
        self.update_queue_metrics();
        self.publish_hot_dispatch_metrics(now);
    }
}
