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

/// How the completion-critical dispatch phase ended, which is what decides the
/// yield signal. Only demand that a freed connection could actually serve may
/// ask lanes to yield: critical work that is queued but undispatchable — a
/// propagation hold, a durable-lead backlog, every server excluded — must not
/// park the rest of the queue behind work no yielded lane can be handed to.
enum CriticalDispatchPhase {
    /// The dispatch loop must stop entirely (lane spawn hit a stop-all).
    StopAll,
    /// Dispatchable critical demand remains, and capacity, rate limiting, or
    /// the pass budget is what stopped the phase from taking it. Non-critical
    /// lanes should yield so the next pass can hand their connections here.
    CapacityStarved,
    /// Every job's critical demand was dispatched or is not currently
    /// dispatchable. Nothing a yielded lane could serve remains.
    Drained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DownloadWorkSelection {
    Any,
    CompletionCritical,
    NonCritical,
}

impl DownloadWorkSelection {
    fn matches(self, work: &DownloadWork) -> bool {
        match self {
            Self::Any => true,
            Self::CompletionCritical => work.completion_critical,
            Self::NonCritical => !work.completion_critical,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct DownloadBatchClass {
    is_recovery: bool,
    completion_critical: bool,
}

impl From<&DownloadBatchCompatibility> for DownloadBatchClass {
    fn from(compatibility: &DownloadBatchCompatibility) -> Self {
        Self {
            is_recovery: compatibility.is_recovery,
            completion_critical: compatibility.completion_critical,
        }
    }
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
const HOT_LEASE_COLD_START_WORK_LIMIT: usize = 16;
const NO_ELIGIBLE_SERVER_WARN_INTERVAL: Duration = Duration::from_secs(60);
const BODY_FETCH_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
// Short debounce before the first spillover lane opens: this is slowness
// DETECTION, not easing. A hot job hitting a brief refill hiccup should not
// spray a lane onto another job for the few hundred milliseconds it takes to
// recover; a genuinely idle or capacity-starved hot job clears this window
// almost immediately and spillover engages at full speed from there.
const HOT_DISPATCH_SLOWNESS_WINDOW: Duration = Duration::from_millis(500);
const HOT_DISPATCH_SPILLOVER_HARM_PERCENT: u64 = 7;
/// At most this many distinct non-hot jobs may hold a spillover loan at once.
/// Lanes concentrate on the jobs already holding a loan before a new job is
/// admitted, so spillover deepens a small number of jobs instead of fanning
/// out across the whole queue.
const HOT_DISPATCH_SPILLOVER_MAX_JOBS: usize = 2;
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
    /// Resident bytes: this alone controls hard write pressure.
    write_buffered_bytes: u64,
    /// Resident plus UU-spooled bytes: this controls soft pacing.
    write_pending_bytes: u64,
    /// Aggregate UU spool admission is capped; only cursor-closing work may run.
    uu_spool_admission_capped: bool,
    decode_hard_limit_bytes: u64,
    write_hard_limit_bytes: u64,
}

impl Pipeline {
    fn try_dispatch_download_for_job(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
        spillover_loan_kind: Option<SpilloverLoanKind>,
        selection: DownloadWorkSelection,
    ) -> DispatchAttempt {
        // Too young to fetch: its articles are still propagating, and asking for
        // them now produces not-founds that are indistinguishable from missing
        // articles. Ahead of every other gate because it is a statement about
        // the post rather than about the pipeline's own capacity.
        if self.propagation_hold_until(job_id).is_some() {
            return DispatchAttempt::NoWork;
        }
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
        let mut lease = match self.try_lease_initial_download_batch(job_id, pressure, selection) {
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
            // No more bytes are coming for this job. A chase whose parts all
            // finished runs on to the end; one still missing a part would park
            // forever, so it is ended here.
            self.settle_direct_unpack_after_download(job_id);
            self.emit_download_finished_if_active(job_id);
            self.schedule_job_completion_check(job_id);
        }
    }

    /// Completion-critical dispatch: unconditional and first, every pass.
    ///
    /// Completion-critical work (PAR2 completion reads, the direct-store
    /// identity probe wave) always leads ordinary queue bytes, on every job
    /// including the hot job's own — there is no lane cap here, unlike the
    /// regular hot/spillover split below. Demand spreads to the
    /// least-loaded critical job first (ties break on `eligible`'s existing
    /// priority/submission order), so no single job's critical backlog
    /// starves another's.
    fn dispatch_completion_critical_work(
        &mut self,
        eligible: &[(u8, usize, JobId)],
        pressure: DownloadPressure,
        max_connections: usize,
        dispatch_budget: &mut usize,
    ) -> CriticalDispatchPhase {
        let mut skipped = Vec::new();
        while self.active_download_connections < max_connections
            && !self.rate_limiter.should_wait()
            && *dispatch_budget > 0
        {
            let Some(job_id) = eligible
                .iter()
                .enumerate()
                .filter(|(_, (_, _, job_id))| self.job_has_completion_critical_work(*job_id))
                .filter(|(_, (_, _, job_id))| !skipped.contains(job_id))
                .min_by_key(|(index, (_, _, job_id))| {
                    (
                        self.active_completion_critical_connections_by_job
                            .get(job_id)
                            .copied()
                            .unwrap_or(0),
                        *index,
                    )
                })
                .map(|(_, (_, _, job_id))| *job_id)
            else {
                // No candidate is left: everything queued was dispatched or
                // answered `NoWork` this pass. Queued-but-skipped work is
                // deliberately NOT capacity starvation — a yielded lane could
                // not have been handed to it.
                return CriticalDispatchPhase::Drained;
            };
            match self.try_dispatch_download_for_job(
                job_id,
                pressure,
                None,
                DownloadWorkSelection::CompletionCritical,
            ) {
                DispatchAttempt::Dispatched => {
                    *dispatch_budget = dispatch_budget.saturating_sub(1);
                }
                DispatchAttempt::NoWork => skipped.push(job_id),
                DispatchAttempt::StopAll => return CriticalDispatchPhase::StopAll,
            }
        }
        // Capacity, rate limiting, or the pass budget ended the phase. Only
        // jobs this pass did not already prove undispatchable count as the
        // starved remainder.
        let starved_remainder = eligible.iter().any(|(_, _, job_id)| {
            !skipped.contains(job_id) && self.job_has_completion_critical_work(*job_id)
        });
        if starved_remainder {
            CriticalDispatchPhase::CapacityStarved
        } else {
            CriticalDispatchPhase::Drained
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
                rate_wait = self.rate_limiter.should_wait(),
                "dispatch: eligible jobs found, attempting dispatch"
            );
        }

        let eligible_count = eligible.len();
        let Some((_hot_priority, hot_job_id)) = self.select_hot_dispatch_job(&eligible, now) else {
            self.update_queue_metrics();
            return;
        };

        let active_connections_before_dispatch = self.active_download_connections;

        // Phase 1: completion-critical work, unconditionally, ahead of every
        // regular byte on every job — the hot job's own included. No lane
        // cap: critical demand takes every connection it can use.
        //
        // The phase's outcome owns the yield signal: capacity starvation asks
        // every non-critical lane to return its unrequested tail so the next
        // pass can hand those connections back here (owned_lane checks the
        // signal every few articles; refill checks it too). Drained demand —
        // including demand that is queued but undispatchable — clears it, so
        // a held or server-starved critical job can never park the rest of
        // the queue behind work no yielded lane could serve.
        let critical_capacity_starved = match self.dispatch_completion_critical_work(
            &eligible,
            pressure,
            max,
            &mut dispatch_budget,
        ) {
            CriticalDispatchPhase::StopAll => {
                self.publish_hot_dispatch_metrics(now);
                return;
            }
            CriticalDispatchPhase::CapacityStarved => true,
            CriticalDispatchPhase::Drained => false,
        };
        if critical_capacity_starved {
            self.hot_share_yield_signal.request();
        } else {
            self.hot_share_yield_signal.clear();
        }

        // Phase 2: the hot job fills everything else, full speed, no ramp.
        // `Any` selection leads with whatever critical work of its own phase
        // 1 didn't reach (there is none unless capacity ran out first), then
        // falls through to its ordinary queue.
        while self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.try_dispatch_download_for_job(
                hot_job_id,
                pressure,
                None,
                DownloadWorkSelection::Any,
            ) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => break,
                DispatchAttempt::StopAll => {
                    self.publish_hot_dispatch_metrics(now);
                    return;
                }
            }
        }

        // Phase 3: spillover — only once the hot job genuinely cannot use
        // its capacity (it has no queued dispatchable work left), never
        // merely because dispatch hasn't caught up to it yet.
        let has_unused_capacity = self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0;
        let best_mode_block_reason = self.hot_best_mode_block_reason(hot_job_id, max);

        let spillover_allowed = if suppress_spillover {
            // Spillover is suppressed, but the critical yield lever is not:
            // capacity-starved critical demand keeps its request so the next
            // pass's (budgeted) dispatch can still hand freed lanes to it.
            if !critical_capacity_starved {
                self.hot_share_yield_signal.clear();
            }
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedPressure);
            false
        } else if bandwidth_cap_tight {
            if !critical_capacity_starved {
                self.hot_share_yield_signal.clear();
            }
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedNearCap);
            false
        } else if best_mode_block_reason == HotBestModeBlockReason::HotHasQueuedPrimary {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(best_mode_block_reason);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedHotCanUseCapacity);
            false
        } else if best_mode_block_reason == HotBestModeBlockReason::LaneCapacityAvailable {
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
        } else {
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            let underfill_started_at = *self.hot_dispatch_underfill_since.get_or_insert(now);
            if now.saturating_duration_since(underfill_started_at) >= HOT_DISPATCH_SLOWNESS_WINDOW {
                self.hot_dispatch_mode = DispatchShareMode::Shared;
                true
            } else {
                self.block_or_reclaim_spillover(SpilloverDecision::BlockedHotCanUseCapacity);
                false
            }
        };

        if spillover_allowed {
            let hot_speed_bps = self.hot_dispatch_speed_bps(now);
            // A hot job with nothing in flight has nothing to measure and
            // nothing to harm, so the per-pass growth cap (one new loan
            // connection per pass, paced against the 7% speed-harm reclaim
            // in `update_spillover_loan_measurement`) does not apply — spill
            // up to full capacity immediately instead of trickling in.
            let hot_idle = !self.job_has_active_download_work(hot_job_id);
            if !hot_idle {
                dispatch_budget = dispatch_budget.min(1);
            }

            // Prefer jobs that already hold a loan so lanes concentrate
            // instead of fanning out, then fall back to priority order for a
            // fresh job — bounded to HOT_DISPATCH_SPILLOVER_MAX_JOBS distinct
            // jobs at a time.
            let mut spill_targets: Vec<JobId> = eligible
                .iter()
                .map(|(_, _, job_id)| *job_id)
                .filter(|job_id| *job_id != hot_job_id)
                .collect();
            spill_targets
                .sort_by_key(|job_id| !self.hot_dispatch_spillover_loans.holds_loan(*job_id));

            for job_id in spill_targets {
                if self.active_download_connections >= max
                    || self.rate_limiter.should_wait()
                    || dispatch_budget == 0
                {
                    break;
                }
                if !self.hot_dispatch_spillover_loans.holds_loan(job_id)
                    && self.hot_dispatch_spillover_loans.distinct_loan_jobs()
                        >= HOT_DISPATCH_SPILLOVER_MAX_JOBS
                {
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
                        DownloadWorkSelection::Any,
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
