use super::*;
use crate::pipeline::download::transport::{
    JobTransportClass, ServerPipelineProof, ServerPipelineState,
};
use weaver_nntp::client::FetchAttemptOutcome;

enum DispatchAttempt {
    Dispatched,
    NoWork,
    StopAll,
}

#[derive(Debug, Default, Clone, Copy)]
struct DownloadPipelineBacklog {
    active_downloads: usize,
    active_connections: usize,
    active_decodes: usize,
    delayed_retries: usize,
    released_results: usize,
    pending_decodes: usize,
    buffered_write_segments: usize,
    buffered_write_bytes: u64,
}

impl DownloadPipelineBacklog {
    fn has_durable_catch_up_work(self) -> bool {
        self.active_decodes != 0
            || self.delayed_retries != 0
            || self.released_results != 0
            || self.pending_decodes != 0
    }
}

const DOWNLOAD_PRESSURE_SOFT_PERCENT: u64 = 70;
const SOFT_PRESSURE_DISPATCH_MAX_DELAY: Duration = Duration::from_millis(150);
const SOFT_PRESSURE_DISPATCH_MIN_DELAY: Duration = Duration::from_millis(1);
const SAB_BODY_PIPELINE_DEPTH: usize = 2;
const HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT: usize = 64;
const HOT_DISPATCH_WARMUP_MIN_DURATION: Duration = Duration::from_secs(2);
const HOT_DISPATCH_FORCE_UNDERFILL_AFTER: Duration = Duration::from_secs(5);
const HOT_DISPATCH_MIN_SUCCESSFUL_PRIMARY_BODIES: u64 = 24;
const HOT_DISPATCH_UNDERFILL_WINDOW: Duration = Duration::from_secs(2);
const HOT_DISPATCH_EXPANSION_HELPFUL_PERCENT: u64 = 5;
const HOT_DISPATCH_SPILLOVER_HARM_PERCENT: u64 = 7;
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

impl DownloadPressure {
    fn is_hard(self) -> bool {
        self.state == DownloadPressureState::Hard
    }

    fn suppresses_spillover(self) -> bool {
        self.state == DownloadPressureState::Soft
    }
}

impl Pipeline {
    fn download_pressure_limits(&self) -> (u64, u64, u64, u64) {
        let decode_hard = (self.decode_backlog_budget_bytes as u64).max(1);
        let decode_soft = (decode_hard.saturating_mul(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100)
            .max(1)
            .min(decode_hard);
        let write_hard = (self.write_backlog_budget_bytes as u64).max(1);
        let write_soft = (write_hard.saturating_mul(DOWNLOAD_PRESSURE_SOFT_PERCENT) / 100)
            .max(1)
            .min(write_hard);
        (decode_soft, decode_hard, write_soft, write_hard)
    }

    fn pressure_reason(decode_pressure: bool, write_pressure: bool) -> DownloadPressureReason {
        match (decode_pressure, write_pressure) {
            (true, true) => DownloadPressureReason::DecodeAndWrite,
            (true, false) => DownloadPressureReason::Decode,
            (false, true) => DownloadPressureReason::Write,
            (false, false) => DownloadPressureReason::None,
        }
    }

    fn elapsed_ms(started_at: Instant) -> u64 {
        started_at.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }

    fn normal_download_connection_capacity_available(&self) -> bool {
        let params = self.tuner.params();
        let total = self.effective_download_connection_capacity(params.max_concurrent_downloads);
        self.active_download_connections < self.normal_download_connection_capacity_limit(total)
    }

    pub(crate) fn restart_durable_lead_limit_bytes() -> u64 {
        download_restart_checkpoint_bytes()
            .saturating_mul(DOWNLOAD_RESTART_MAX_DURABLE_LEAD_MULTIPLIER)
    }

    fn durable_download_floor_bytes_for_job(&self, job_id: JobId) -> u64 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let floor = state
            .assembly
            .files()
            .map(|file| {
                if file.is_complete() {
                    return file.total_bytes();
                }
                let file_id = file.file_id();
                self.persisted_file_progress
                    .get(&file_id)
                    .copied()
                    .unwrap_or(0)
                    .max(
                        self.pending_file_progress
                            .get(&file_id)
                            .copied()
                            .unwrap_or(0),
                    )
                    .min(file.total_bytes())
            })
            .sum::<u64>();
        floor.max(state.restored_download_floor_bytes)
    }

    fn estimated_undurable_download_bytes_for_job(&self, job_id: JobId) -> u64 {
        let Some(state) = self.jobs.get(&job_id) else {
            return 0;
        };
        let durable_floor = self.durable_download_floor_bytes_for_job(job_id);
        let accepted_bytes = state.downloaded_bytes.saturating_sub(durable_floor);
        let active_items = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            .saturating_add(
                self.pending_released_download_results_by_job
                    .get(&job_id)
                    .copied()
                    .unwrap_or(0),
            )
            .saturating_add(
                self.active_decodes_by_job
                    .get(&job_id)
                    .copied()
                    .unwrap_or(0),
            );
        accepted_bytes.saturating_add((active_items as u64).saturating_mul(1024 * 1024))
    }

    fn download_pipeline_backlog_for_job(&self, job_id: JobId) -> DownloadPipelineBacklog {
        let pending_decodes = self
            .pending_decode
            .iter()
            .filter(|work| work.segment_id.file_id.job_id == job_id)
            .count();
        let (buffered_write_segments, buffered_write_bytes) = self
            .write_buffers
            .iter()
            .filter(|(file_id, _)| file_id.job_id == job_id)
            .fold((0usize, 0u64), |(segments, bytes), (_, write_buf)| {
                (
                    segments.saturating_add(write_buf.buffered_len()),
                    bytes.saturating_add(write_buf.buffered_bytes() as u64),
                )
            });

        DownloadPipelineBacklog {
            active_downloads: self
                .active_downloads_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            active_connections: self
                .active_download_connections_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            active_decodes: self
                .active_decodes_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            delayed_retries: self
                .pending_retries_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            released_results: self
                .pending_released_download_results_by_job
                .get(&job_id)
                .copied()
                .unwrap_or(0),
            pending_decodes,
            buffered_write_segments,
            buffered_write_bytes,
        }
    }

    pub(crate) fn primary_download_within_restart_durable_lead(
        &self,
        job_id: JobId,
        work: &DownloadWork,
    ) -> bool {
        self.restart_durable_lead_block(job_id, work).is_none()
    }

    fn restart_durable_lead_block(&self, job_id: JobId, work: &DownloadWork) -> Option<(u64, u64)> {
        if work.is_recovery {
            return None;
        }
        let limit = Self::restart_durable_lead_limit_bytes();
        if limit == 0 {
            return None;
        }
        let projected = self
            .estimated_undurable_download_bytes_for_job(job_id)
            .saturating_add(work.byte_estimate as u64);
        (projected > limit).then_some((projected, limit))
    }

    fn normal_download_connection_capacity_limit(&self, effective_total: usize) -> usize {
        let params = self.tuner.params();
        let recovery_reserve = params
            .recovery_slots
            .saturating_sub(self.active_recovery)
            .min(effective_total);
        effective_total.saturating_sub(recovery_reserve)
    }

    fn effective_download_connection_capacity(&self, configured_max: usize) -> usize {
        let active_probes = self
            .jobs
            .values()
            .filter(|s| matches!(s.status, JobStatus::Checking))
            .count();
        configured_max
            .min(self.connection_ramp)
            .saturating_sub(active_probes)
    }

    fn observe_ip_rtt_attempts(&mut self, attempts: &[weaver_nntp::client::FetchAttemptTrace]) {
        if self.ip_replacement_trial_extra_connections == 0 {
            return;
        }

        let now = Instant::now();
        let mut changed = false;
        for attempt in attempts {
            if attempt.outcome != FetchAttemptOutcome::Success {
                continue;
            }
            let Some(ip) = attempt.remote_ip else {
                continue;
            };
            let key = ServerIpKey {
                server_idx: attempt.server_idx,
                ip,
            };
            if !self.ip_rtt_ewma.contains_key(&key)
                && self.ip_rtt_ewma.len() >= MAX_IP_RTT_EWMA_ENTRIES
                && let Some(oldest_key) = self
                    .ip_rtt_ewma
                    .iter()
                    .min_by_key(|(_, state)| state.last_seen)
                    .map(|(key, _)| *key)
            {
                self.ip_rtt_ewma.remove(&oldest_key);
            }

            self.ip_rtt_ewma
                .entry(key)
                .and_modify(|state| state.observe(now, attempt.elapsed))
                .or_insert_with(|| IpRttEwma::new(now, attempt.elapsed));
            changed = true;
        }

        if changed {
            let slowest_ms = self
                .ip_rtt_ewma
                .values()
                .map(|state| state.ewma_ms.ceil() as u64)
                .max()
                .unwrap_or(0);
            self.metrics
                .set_ip_rtt_ewma_summary(self.ip_rtt_ewma.len(), slowest_ms);
        }
    }

    fn ip_replacement_baseline_ms(&self, key: ServerIpKey, now: Instant) -> Option<f64> {
        self.ip_rtt_ewma
            .iter()
            .filter(|(candidate_key, state)| {
                candidate_key.server_idx == key.server_idx
                    && candidate_key.ip != key.ip
                    && state.samples >= IP_REPLACEMENT_BASELINE_MIN_SAMPLES
                    && now.saturating_duration_since(state.last_seen)
                        <= IP_REPLACEMENT_BASELINE_RECENT
            })
            .map(|(_, state)| state.ewma_ms)
            .min_by(|a, b| a.total_cmp(b))
            .or_else(|| {
                self.download_lane_runtime
                    .server_rtt
                    .get(&key.server_idx)
                    .and_then(|window| window.ewma())
                    .map(|duration| duration.as_secs_f64() * 1000.0)
            })
    }

    fn select_ip_replacement_candidate(&self, now: Instant) -> Option<IpReplacementCandidate> {
        if self.ip_replacement_trial_extra_connections == 0 || self.ip_replacement_burst_active {
            return None;
        }

        self.ip_rtt_ewma
            .iter()
            .filter_map(|(key, state)| {
                if state.samples < IP_REPLACEMENT_MIN_OLD_SAMPLES
                    || now.saturating_duration_since(state.first_seen) < IP_REPLACEMENT_MIN_OLD_AGE
                {
                    return None;
                }
                let baseline_ms = self.ip_replacement_baseline_ms(*key, now)?;
                let old_is_slow = state.ewma_ms >= baseline_ms * IP_REPLACEMENT_OLD_SLOWER_RATIO
                    && state.ewma_ms - baseline_ms >= IP_REPLACEMENT_OLD_SLOWER_MS;
                old_is_slow.then_some(IpReplacementCandidate {
                    old_key: *key,
                    old_ewma_ms: state.ewma_ms,
                    baseline_ms,
                })
            })
            .max_by(|a, b| {
                let a_delta = a.old_ewma_ms - a.baseline_ms;
                let b_delta = b.old_ewma_ms - b.baseline_ms;
                a_delta.total_cmp(&b_delta)
            })
    }

    fn soft_pressure_dispatch_delay(&self, pressure: DownloadPressure) -> Option<Duration> {
        if pressure.state != DownloadPressureState::Soft {
            return None;
        }

        let (decode_soft, decode_hard, write_soft, write_hard) = self.download_pressure_limits();
        let decode_delay =
            Self::soft_pressure_delay_for(pressure.decode_backlog_bytes, decode_soft, decode_hard);
        let write_delay =
            Self::soft_pressure_delay_for(pressure.write_buffered_bytes, write_soft, write_hard);
        decode_delay.max(write_delay)
    }

    fn soft_pressure_delay_for(bytes: u64, soft: u64, hard: u64) -> Option<Duration> {
        if bytes < soft || hard <= soft {
            return None;
        }

        let numerator = bytes.saturating_sub(soft).min(hard - soft);
        let denominator = hard - soft;
        let max_ms = SOFT_PRESSURE_DISPATCH_MAX_DELAY.as_millis() as u64;
        let delay_ms = numerator.saturating_mul(max_ms) / denominator;
        Some(SOFT_PRESSURE_DISPATCH_MIN_DELAY.max(Duration::from_millis(delay_ms.max(1))))
    }

    fn update_download_pressure_stall_metrics(&mut self, state: DownloadPressureState) {
        if state == DownloadPressureState::Hard {
            let started_at = match self.download_pressure_hard_stall_started_at {
                Some(started_at) => started_at,
                None => {
                    let started_at = Instant::now();
                    self.download_pressure_hard_stall_started_at = Some(started_at);
                    self.metrics
                        .download_pressure_stalls_total
                        .fetch_add(1, Ordering::Relaxed);
                    started_at
                }
            };
            self.metrics
                .download_pressure_current_stall_ms
                .store(Self::elapsed_ms(started_at), Ordering::Relaxed);
            return;
        }

        if let Some(started_at) = self.download_pressure_hard_stall_started_at.take() {
            let elapsed_ms = Self::elapsed_ms(started_at);
            self.metrics
                .download_pressure_stall_duration_ms
                .fetch_add(elapsed_ms, Ordering::Relaxed);
        }
        self.metrics
            .download_pressure_current_stall_ms
            .store(0, Ordering::Relaxed);
    }

    pub(crate) fn refresh_download_pressure(&mut self) -> DownloadPressure {
        let (decode_soft, decode_hard, write_soft, write_hard) = self.download_pressure_limits();
        let decode_bytes = self
            .metrics
            .decode_pending_bytes
            .load(Ordering::Relaxed)
            .saturating_add(self.metrics.decode_active_bytes.load(Ordering::Relaxed));
        let write_bytes = self.metrics.write_buffered_bytes.load(Ordering::Relaxed);

        if decode_bytes >= decode_hard {
            self.download_decode_hard_pressure_latched = true;
        } else if decode_bytes < decode_soft {
            self.download_decode_hard_pressure_latched = false;
        }
        if write_bytes >= write_hard {
            self.download_write_hard_pressure_latched = true;
        } else if write_bytes < write_soft {
            self.download_write_hard_pressure_latched = false;
        }

        let decode_hard_pressure = self.download_decode_hard_pressure_latched;
        let write_hard_pressure = self.download_write_hard_pressure_latched;
        let decode_soft_pressure = decode_bytes >= decode_soft;
        let write_soft_pressure = write_bytes >= write_soft;

        let (state, reason) = if decode_hard_pressure || write_hard_pressure {
            (
                DownloadPressureState::Hard,
                Self::pressure_reason(decode_hard_pressure, write_hard_pressure),
            )
        } else if decode_soft_pressure || write_soft_pressure {
            (
                DownloadPressureState::Soft,
                Self::pressure_reason(decode_soft_pressure, write_soft_pressure),
            )
        } else {
            (DownloadPressureState::Clear, DownloadPressureReason::None)
        };

        self.metrics
            .decode_pressure_soft_limit_bytes
            .store(decode_soft, Ordering::Relaxed);
        self.metrics
            .decode_pressure_hard_limit_bytes
            .store(decode_hard, Ordering::Relaxed);
        self.metrics
            .write_pressure_soft_limit_bytes
            .store(write_soft, Ordering::Relaxed);
        self.metrics
            .write_pressure_hard_limit_bytes
            .store(write_hard, Ordering::Relaxed);
        self.metrics
            .download_pressure_state
            .store(state.as_code(), Ordering::Relaxed);
        self.metrics
            .download_pressure_reason
            .store(reason.as_code(), Ordering::Relaxed);
        self.update_download_pressure_stall_metrics(state);

        DownloadPressure {
            state,
            reason,
            decode_backlog_bytes: decode_bytes,
            write_buffered_bytes: write_bytes,
            decode_hard_limit_bytes: decode_hard,
            write_hard_limit_bytes: write_hard,
        }
    }

    fn status_allows_download_dispatch(status: &JobStatus) -> bool {
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

    fn hot_dispatch_job_priority(&self, job_id: JobId) -> Option<u8> {
        self.jobs.get(&job_id).map(Self::job_dispatch_priority)
    }

    fn job_has_dispatchable_work(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            !state.download_queue.is_empty() && Self::status_allows_download_dispatch(&state.status)
        })
    }

    fn hot_job_has_pending_pipeline_promotion(
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

    fn set_hot_best_mode_block_reason(&self, reason: HotBestModeBlockReason) {
        self.metrics
            .hot_dispatch_best_mode_block_reason
            .store(reason.as_code(), Ordering::Relaxed);
    }

    fn hot_best_mode_block_reason(
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

    fn job_has_active_download_work(&self, job_id: JobId) -> bool {
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

    fn job_can_remain_hot(&self, job_id: JobId) -> bool {
        self.jobs.get(&job_id).is_some_and(|state| {
            Self::status_allows_download_dispatch(&state.status)
                && (!state.download_queue.is_empty() || self.job_has_active_download_work(job_id))
        })
    }

    fn start_hot_dispatch_period(&mut self, job_id: JobId, now: Instant) {
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
        self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
    }

    fn clear_hot_dispatch_period(&mut self) {
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
        self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    fn select_hot_dispatch_job(
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

    fn hot_dispatch_warmup_complete(&self, now: Instant) -> bool {
        let Some(started_at) = self.hot_dispatch_started_at else {
            return false;
        };
        let elapsed = now.saturating_duration_since(started_at);
        elapsed >= HOT_DISPATCH_WARMUP_MIN_DURATION
            && (self.hot_dispatch_successes >= HOT_DISPATCH_MIN_SUCCESSFUL_PRIMARY_BODIES
                || elapsed >= HOT_DISPATCH_FORCE_UNDERFILL_AFTER)
    }

    fn active_spillover_connections(&self) -> usize {
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

    fn hot_dispatch_speed_bps(&mut self, now: Instant) -> u64 {
        self.hot_dispatch_throughput_window.bps(now)
    }

    fn update_spillover_loan_measurement(&mut self, now: Instant, hot_speed_bps: u64) {
        if self.hot_dispatch_spillover_loans.update_speed_harm(
            now,
            hot_speed_bps,
            HOT_DISPATCH_SPILLOVER_HARM_PERCENT,
        ) {
            self.record_spillover_decision(SpilloverDecision::ReclaimedSpeedHarm);
        }
    }

    fn hot_spillover_reclaim_pending_for(&self, job_id: JobId) -> bool {
        self.hot_dispatch_spillover_loans
            .reclaim_pending_for(job_id)
    }

    fn start_spillover_loan(&mut self, job_id: JobId, now: Instant, hot_speed_bps: u64) {
        self.hot_dispatch_spillover_loans
            .start_or_extend(job_id, now, hot_speed_bps);
        self.hot_dispatch_last_lend_at = Some(now);
    }

    fn clear_spillover_loan_if_idle(&mut self) {
        if self.active_spillover_connections() == 0 {
            self.hot_dispatch_spillover_loans.clear();
        }
    }

    fn publish_hot_dispatch_metrics(&mut self, now: Instant) {
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

    fn record_spillover_decision(&mut self, decision: SpilloverDecision) {
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

    fn block_or_reclaim_spillover(&mut self, decision: SpilloverDecision) {
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

    fn job_dispatch_priority(state: &JobState) -> u8 {
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

    pub(crate) fn note_retry_scheduled(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        *self.pending_retries_by_job.entry(job_id).or_default() += 1;
        *self
            .pending_retries_by_segment
            .entry(segment_id)
            .or_default() += 1;
    }

    pub(crate) fn note_retry_requeued(&mut self, segment_id: SegmentId) {
        let job_id = segment_id.file_id.job_id;
        let Some(pending) = self.pending_retries_by_job.get_mut(&job_id) else {
            return;
        };
        *pending = pending.saturating_sub(1);
        if *pending == 0 {
            self.pending_retries_by_job.remove(&job_id);
        }
        if let Some(pending) = self.pending_retries_by_segment.get_mut(&segment_id) {
            *pending = pending.saturating_sub(1);
            if *pending == 0 {
                self.pending_retries_by_segment.remove(&segment_id);
            }
        }
    }

    fn restore_download_result_work_without_retry(
        &mut self,
        segment_id: SegmentId,
        retry_count: u32,
        exclude_servers: Vec<usize>,
    ) -> bool {
        let job_id = segment_id.file_id.job_id;
        let file_idx = segment_id.file_id.file_index as usize;
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        let Some(file_spec) = state.spec.files.get(file_idx) else {
            return false;
        };
        let Some(seg_spec) = file_spec
            .segments
            .iter()
            .find(|seg| seg.ordinal == segment_id.segment_number)
        else {
            return false;
        };
        let work = DownloadWork {
            segment_id,
            message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
            groups: file_spec.groups.clone(),
            priority: file_spec.role.download_priority(),
            byte_estimate: seg_spec.bytes,
            retry_count,
            is_recovery: file_spec.role.is_recovery(),
            exclude_servers,
        };
        if work.is_recovery {
            state.recovery_queue.push(work);
        } else {
            state.download_queue.push(work);
        }
        self.update_queue_metrics();
        true
    }

    fn schedule_download_work_without_retry_budget(
        &mut self,
        segment_id: SegmentId,
        retry_count: u32,
        exclude_servers: Vec<usize>,
        delay: Duration,
    ) -> bool {
        let job_id = segment_id.file_id.job_id;
        let file_idx = segment_id.file_id.file_index as usize;
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file_spec) = state.spec.files.get(file_idx) else {
            return false;
        };
        let Some(seg_spec) = file_spec
            .segments
            .iter()
            .find(|seg| seg.ordinal == segment_id.segment_number)
        else {
            return false;
        };
        let work = DownloadWork {
            segment_id,
            message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
            groups: file_spec.groups.clone(),
            priority: file_spec.role.download_priority(),
            byte_estimate: seg_spec.bytes,
            retry_count,
            is_recovery: file_spec.role.is_recovery(),
            exclude_servers,
        };
        self.note_retry_scheduled(segment_id);
        let retry_tx = self.retry_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let _ = retry_tx.send(work).await;
        });
        true
    }

    fn bandwidth_reservation_estimate(decoded_bytes: u32) -> u64 {
        let decoded = decoded_bytes as u64;
        decoded.saturating_add(decoded / 16).saturating_add(1024)
    }

    fn reserve_rate_limit_for_dispatch(&mut self, segment_id: SegmentId, estimate_bytes: u64) {
        self.rate_limiter.consume(estimate_bytes);
        self.rate_limit_reservations
            .insert(segment_id, estimate_bytes);
    }

    fn reconcile_rate_limit_for_download(
        &mut self,
        segment_id: SegmentId,
        actual_bytes: Option<u64>,
    ) {
        let Some(estimated_bytes) = self.rate_limit_reservations.remove(&segment_id) else {
            return;
        };
        match actual_bytes {
            Some(actual_bytes) => self.rate_limiter.reconcile(estimated_bytes, actual_bytes),
            None => self.rate_limiter.refund(estimated_bytes),
        }
    }

    fn ensure_job_transport_profile(&mut self, job_id: JobId) -> Option<&JobTransportProfile> {
        if !self.job_transport_profiles.contains_key(&job_id) {
            let profile = self
                .jobs
                .get(&job_id)
                .map(|state| JobTransportProfile::classify(&state.spec))?;
            self.job_transport_profiles.insert(job_id, profile);
        }
        self.job_transport_profiles.get(&job_id)
    }

    fn choose_download_lane_mode(
        &mut self,
        job_id: JobId,
        is_recovery: bool,
        pressure: DownloadPressure,
    ) -> DownloadLaneMode {
        if is_recovery {
            return DownloadLaneMode::Sequential;
        }

        let Some(profile) = self.ensure_job_transport_profile(job_id) else {
            return DownloadLaneMode::Sequential;
        };
        let job_class = profile.class();
        let median_body_bytes = profile.median_body_bytes();
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        let now = Instant::now();
        self.download_lane_runtime
            .server_proof
            .iter()
            .map(|(server_idx, proof)| {
                self.choose_download_lane_mode_for_server(
                    now,
                    *server_idx,
                    proof,
                    job_class,
                    median_body_bytes,
                    pressure_clear,
                )
            })
            .max_by_key(|mode| mode.max_depth())
            .unwrap_or(DownloadLaneMode::Sequential)
    }

    fn download_lane_server_modes(
        &mut self,
        job_id: JobId,
        is_recovery: bool,
        pressure: DownloadPressure,
    ) -> Vec<(usize, DownloadLaneMode)> {
        if is_recovery {
            return Vec::new();
        }
        let Some(profile) = self.ensure_job_transport_profile(job_id) else {
            return Vec::new();
        };
        let job_class = profile.class();
        let median_body_bytes = profile.median_body_bytes();
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        let now = Instant::now();
        self.download_lane_runtime
            .server_proof
            .iter()
            .map(|(server_idx, proof)| {
                (
                    *server_idx,
                    self.choose_download_lane_mode_for_server(
                        now,
                        *server_idx,
                        proof,
                        job_class,
                        median_body_bytes,
                        pressure_clear,
                    ),
                )
            })
            .collect()
    }

    fn choose_download_lane_mode_for_server(
        &self,
        now: Instant,
        server_idx: usize,
        proof: &ServerPipelineProof,
        job_class: JobTransportClass,
        median_body_bytes: u64,
        pressure_clear: bool,
    ) -> DownloadLaneMode {
        let rtt = self
            .download_lane_runtime
            .server_rtt
            .get(&server_idx)
            .and_then(|window| window.ewma());
        proof.choose_mode(now, job_class, median_body_bytes, rtt, pressure_clear)
    }

    fn note_download_lane_started(&mut self, mode: DownloadLaneMode) {
        debug_assert_eq!(
            DownloadLaneMode::PipelineDepth2.max_depth(),
            SAB_BODY_PIPELINE_DEPTH
        );
        let lane_id = DownloadLaneId(self.download_lane_runtime.next_lane_id);
        self.download_lane_runtime.next_lane_id =
            self.download_lane_runtime.next_lane_id.saturating_add(1);
        debug!(lane_id = lane_id.0, mode = ?mode, "download lane started");
        self.metrics
            .download_lanes_active
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .download_lanes_issuing_active
            .fetch_add(1, Ordering::Relaxed);
        match mode {
            DownloadLaneMode::Sequential => self
                .metrics
                .download_lanes_sequential_active
                .fetch_add(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth2 => self
                .metrics
                .download_lanes_depth2_active
                .fetch_add(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth4 => self
                .metrics
                .download_lanes_depth4_active
                .fetch_add(1, Ordering::Relaxed),
        };
        *self
            .download_lane_runtime
            .active_by_mode
            .entry(mode)
            .or_default() += 1;
        *self
            .download_lane_runtime
            .active_by_state
            .entry(DownloadLaneState::Issuing)
            .or_default() += 1;
    }

    fn note_download_lane_released(&mut self, mode: DownloadLaneMode, reason: LaneParkReason) {
        self.metrics
            .download_lanes_active
            .fetch_sub(1, Ordering::Relaxed);
        self.metrics
            .download_lanes_issuing_active
            .fetch_sub(1, Ordering::Relaxed);
        match mode {
            DownloadLaneMode::Sequential => self
                .metrics
                .download_lanes_sequential_active
                .fetch_sub(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth2 => self
                .metrics
                .download_lanes_depth2_active
                .fetch_sub(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth4 => self
                .metrics
                .download_lanes_depth4_active
                .fetch_sub(1, Ordering::Relaxed),
        };
        match reason {
            LaneParkReason::NoWork => self
                .metrics
                .download_lane_parks_no_work_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::Pressure => self
                .metrics
                .download_lane_parks_pressure_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::ProbeYield => self
                .metrics
                .download_lane_parks_probe_yield_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::HotReclaim => self
                .metrics
                .download_lane_parks_hot_reclaim_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::SpilloverWithdraw => self
                .metrics
                .download_lane_parks_spillover_withdraw_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::SpilloverSpeedHarm => self
                .metrics
                .download_lane_parks_spillover_speed_harm_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::IpReplacementRetired => self
                .metrics
                .download_lane_parks_ip_replacement_retired_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::ServerTierChanged => self
                .metrics
                .download_lane_parks_server_tier_changed_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::ProofFailure => self
                .metrics
                .download_lane_parks_proof_failure_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::Error => self
                .metrics
                .download_lane_parks_error_total
                .fetch_add(1, Ordering::Relaxed),
        };
        if let Some(active) = self.download_lane_runtime.active_by_mode.get_mut(&mode) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.download_lane_runtime.active_by_mode.remove(&mode);
            }
        }
        if let Some(active) = self
            .download_lane_runtime
            .active_by_state
            .get_mut(&DownloadLaneState::Issuing)
        {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.download_lane_runtime
                    .active_by_state
                    .remove(&DownloadLaneState::Issuing);
            }
        }
        *self
            .download_lane_runtime
            .park_counts
            .entry(reason)
            .or_default() += 1;
    }

    fn note_download_lane_mode_changed(
        &mut self,
        previous: DownloadLaneMode,
        next: DownloadLaneMode,
    ) {
        if previous == next {
            return;
        }

        if next.max_depth() > previous.max_depth() {
            let now = Instant::now();
            let speed = self.hot_dispatch_speed_bps(now);
            self.hot_dispatch_expansion_window.record(
                now,
                HotExpansionKind::PipelinePromotion,
                speed,
            );
        }

        match previous {
            DownloadLaneMode::Sequential => self
                .metrics
                .download_lanes_sequential_active
                .fetch_sub(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth2 => self
                .metrics
                .download_lanes_depth2_active
                .fetch_sub(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth4 => self
                .metrics
                .download_lanes_depth4_active
                .fetch_sub(1, Ordering::Relaxed),
        };
        match next {
            DownloadLaneMode::Sequential => self
                .metrics
                .download_lanes_sequential_active
                .fetch_add(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth2 => self
                .metrics
                .download_lanes_depth2_active
                .fetch_add(1, Ordering::Relaxed),
            DownloadLaneMode::PipelineDepth4 => self
                .metrics
                .download_lanes_depth4_active
                .fetch_add(1, Ordering::Relaxed),
        };

        if let Some(active) = self.download_lane_runtime.active_by_mode.get_mut(&previous) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.download_lane_runtime.active_by_mode.remove(&previous);
            }
        }
        *self
            .download_lane_runtime
            .active_by_mode
            .entry(next)
            .or_default() += 1;
    }

    fn reserve_download_work_for_dispatch(
        &mut self,
        job_id: JobId,
        work: DownloadWork,
        stop_on_cap_block: bool,
    ) -> Result<Option<DownloadWork>, DispatchAttempt> {
        if !self.primary_download_within_restart_durable_lead(job_id, &work) {
            let (projected_before_flush, limit) = self
                .restart_durable_lead_block(job_id, &work)
                .expect("blocked durable lead must include projected bytes");
            self.flush_file_progress_batch("download.file_progress.flush.restart_durable_lead");
            if let Some((projected_after_flush, _)) = self.restart_durable_lead_block(job_id, &work)
            {
                let backlog = self.download_pipeline_backlog_for_job(job_id);
                if !backlog.has_durable_catch_up_work() {
                    debug!(
                        job_id = job_id.0,
                        segment = ?work.segment_id,
                        projected_before_flush,
                        projected_after_flush,
                        limit,
                        "dispatch continuing: restart durable lead exceeded but download pipeline is idle"
                    );
                } else {
                    self.metrics
                        .download_restart_durable_lead_blocked_total
                        .fetch_add(1, Ordering::Relaxed);
                    self.download_restart_durable_lead_retry_after.insert(
                        job_id,
                        Instant::now() + DOWNLOAD_RESTART_DURABLE_LEAD_RETRY_DELAY,
                    );
                    debug!(
                        job_id = job_id.0,
                        segment = ?work.segment_id,
                        projected_before_flush,
                        projected_after_flush,
                        limit,
                        active_downloads = backlog.active_downloads,
                        active_connections = backlog.active_connections,
                        active_decodes = backlog.active_decodes,
                        delayed_retries = backlog.delayed_retries,
                        released_results = backlog.released_results,
                        pending_decodes = backlog.pending_decodes,
                        buffered_write_segments = backlog.buffered_write_segments,
                        buffered_write_bytes = backlog.buffered_write_bytes,
                        "dispatch delayed: restart durable lead"
                    );
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.download_queue.push(work);
                    }
                    self.update_queue_metrics();
                    return Ok(None);
                }
            }
        }
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);

        let reservation_estimate = Self::bandwidth_reservation_estimate(work.byte_estimate);
        match self.reserve_bandwidth_for_dispatch(work.segment_id, reservation_estimate) {
            Ok(true) => Ok(Some(work)),
            Ok(false) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                if self.bandwidth_cap.cap_enabled() {
                    use crate::jobs::handle::{DownloadBlockKind, DownloadBlockState};
                    self.shared_state.set_download_block(DownloadBlockState {
                        kind: DownloadBlockKind::IspCap,
                        ..self
                            .bandwidth_cap
                            .to_download_block_state(self.global_paused)
                    });
                }
                self.update_queue_metrics();
                if stop_on_cap_block {
                    Err(DispatchAttempt::StopAll)
                } else {
                    Ok(None)
                }
            }
            Err(error) => {
                error!(error = %error, "failed to reserve ISP bandwidth for dispatch");
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                self.update_queue_metrics();
                if stop_on_cap_block {
                    Err(DispatchAttempt::StopAll)
                } else {
                    Ok(None)
                }
            }
        }
    }

    fn pop_download_work_for_batch(
        &mut self,
        job_id: JobId,
        compatibility: Option<&DownloadBatchCompatibility>,
    ) -> Option<DownloadWork> {
        self.jobs.get_mut(&job_id).and_then(|state| {
            if let Some(compatibility) = compatibility {
                state
                    .download_queue
                    .pop_next_matching(|work| compatibility.matches(work))
            } else {
                state.download_queue.pop()
            }
        })
    }

    fn try_lease_initial_download_batch(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        let Some(first) = self.pop_download_work_for_batch(job_id, None) else {
            return Ok(None);
        };
        if !first.is_recovery && !self.normal_download_connection_capacity_available() {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.download_queue.push(first);
            }
            return Ok(None);
        }
        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, true)? else {
            return Ok(None);
        };

        let lane_mode = self.choose_download_lane_mode(job_id, first.is_recovery, pressure);
        let compatibility = DownloadBatchCompatibility::from_work(&first);
        Ok(Some(self.finish_download_batch_lease(
            job_id,
            lane_mode,
            compatibility,
            first,
            pressure,
        )))
    }

    fn try_lease_refill_download_batch(
        &mut self,
        job_id: JobId,
        compatibility: DownloadBatchCompatibility,
        pressure: DownloadPressure,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        let lane_mode = self.choose_download_lane_mode(job_id, compatibility.is_recovery, pressure);
        let Some(first) = self.pop_download_work_for_batch(job_id, Some(&compatibility)) else {
            return Ok(None);
        };
        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, false)? else {
            return Ok(None);
        };

        Ok(Some(self.finish_download_batch_lease(
            job_id,
            lane_mode,
            compatibility,
            first,
            pressure,
        )))
    }

    fn try_lease_ip_replacement_trial_batch(
        &mut self,
        job_id: JobId,
        server_idx: usize,
    ) -> Result<Option<DownloadBatchLease>, DispatchAttempt> {
        let Some(first) = self.pop_download_work_for_batch(job_id, None) else {
            return Ok(None);
        };
        if first.is_recovery || first.exclude_servers.contains(&server_idx) {
            if let Some(state) = self.jobs.get_mut(&job_id) {
                state.download_queue.push(first);
            }
            return Ok(None);
        }

        let Some(first) = self.reserve_download_work_for_dispatch(job_id, first, true)? else {
            return Ok(None);
        };
        let compatibility = DownloadBatchCompatibility::from_work(&first);
        if compatibility.is_recovery {
            let lease = DownloadBatchLease {
                job_id,
                lane_mode: DownloadLaneMode::Sequential,
                server_modes: Vec::new(),
                compatibility,
                works: vec![first],
            };
            self.rollback_download_batch_lease(lease);
            return Ok(None);
        }

        let mut works = vec![first];
        while works.len() < IP_REPLACEMENT_TRIAL_SAMPLES {
            let Some(next) = self.pop_download_work_for_batch(job_id, Some(&compatibility)) else {
                break;
            };
            if next.is_recovery {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(next);
                }
                break;
            }
            match self.reserve_download_work_for_dispatch(job_id, next, false) {
                Ok(Some(next)) => works.push(next),
                Ok(None) | Err(DispatchAttempt::StopAll) | Err(DispatchAttempt::NoWork) => break,
                Err(DispatchAttempt::Dispatched) => unreachable!("reserve helper never dispatches"),
            }
        }

        let lease = DownloadBatchLease {
            job_id,
            lane_mode: DownloadLaneMode::Sequential,
            server_modes: Vec::new(),
            compatibility,
            works,
        };
        if lease.works.len() < IP_REPLACEMENT_TRIAL_SAMPLES {
            self.rollback_download_batch_lease(lease);
            return Ok(None);
        }

        Ok(Some(lease))
    }

    fn finish_download_batch_lease(
        &mut self,
        job_id: JobId,
        lane_mode: DownloadLaneMode,
        compatibility: DownloadBatchCompatibility,
        first: DownloadWork,
        pressure: DownloadPressure,
    ) -> DownloadBatchLease {
        let work_limit = self.download_lane_lease_work_limit(job_id, lane_mode, pressure);
        let mut works = vec![first];
        while works.len() < work_limit {
            let Some(next) = self.pop_download_work_for_batch(job_id, Some(&compatibility)) else {
                break;
            };
            match self.reserve_download_work_for_dispatch(job_id, next, false) {
                Ok(Some(next)) => works.push(next),
                Ok(None) | Err(DispatchAttempt::StopAll) | Err(DispatchAttempt::NoWork) => break,
                Err(DispatchAttempt::Dispatched) => unreachable!("reserve helper never dispatches"),
            }
        }

        let server_modes =
            self.download_lane_server_modes(job_id, compatibility.is_recovery, pressure);
        DownloadBatchLease {
            job_id,
            lane_mode,
            server_modes,
            compatibility,
            works,
        }
    }

    fn download_lane_lease_work_limit(
        &self,
        job_id: JobId,
        lane_mode: DownloadLaneMode,
        pressure: DownloadPressure,
    ) -> usize {
        if pressure.state == DownloadPressureState::Clear && self.hot_dispatch_job == Some(job_id) {
            return HOT_CLEAR_PRESSURE_LANE_LEASE_WORK_LIMIT.max(lane_mode.max_depth());
        }
        lane_mode.max_depth()
    }

    fn actual_download_lane_mode(
        lease_mode: DownloadLaneMode,
        server_modes: &[(usize, DownloadLaneMode)],
        server_idx: usize,
        supports_pipelining: bool,
    ) -> DownloadLaneMode {
        let server_mode = server_modes
            .iter()
            .find_map(|(idx, mode)| (*idx == server_idx).then_some(*mode))
            .unwrap_or(DownloadLaneMode::Sequential);
        if !supports_pipelining || server_mode == DownloadLaneMode::Sequential {
            return DownloadLaneMode::Sequential;
        }
        if server_mode.max_depth() <= lease_mode.max_depth() {
            server_mode
        } else {
            lease_mode
        }
    }

    fn activation_items(lease: &DownloadBatchLease) -> Vec<(SegmentId, NzbFileId, u64)> {
        lease
            .works
            .iter()
            .map(|work| {
                (
                    work.segment_id,
                    work.segment_id.file_id,
                    work.byte_estimate as u64,
                )
            })
            .collect()
    }

    fn activate_download_batch_lease(
        &mut self,
        lease: &DownloadBatchLease,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        self.activate_download_batch(
            lease.job_id,
            lease.compatibility.is_recovery,
            lease.lane_mode,
            lease.works.len(),
            activation_items,
            starts_connection,
        );
    }

    fn activate_download_batch(
        &mut self,
        job_id: JobId,
        is_recovery: bool,
        lane_mode: DownloadLaneMode,
        work_count: usize,
        activation_items: &[(SegmentId, NzbFileId, u64)],
        starts_connection: bool,
    ) {
        if work_count == 0 {
            return;
        }

        self.active_downloads += work_count;
        self.metrics
            .download_lane_lease_items_total
            .fetch_add(work_count as u64, Ordering::Relaxed);
        if starts_connection {
            if self.hot_dispatch_job == Some(job_id) {
                let now = Instant::now();
                let speed = self.hot_dispatch_speed_bps(now);
                self.hot_dispatch_expansion_window
                    .record(now, HotExpansionKind::LaneStart, speed);
            }
            self.active_download_connections += 1;
            self.note_download_lane_started(lane_mode);
            *self
                .active_download_connections_by_job
                .entry(job_id)
                .or_default() += 1;
        }
        if is_recovery {
            self.active_recovery += work_count;
        }
        *self.active_downloads_by_job.entry(job_id).or_default() += work_count;
        for (segment_id, file_id, estimate) in activation_items {
            *self.active_downloads_by_file.entry(*file_id).or_default() += 1;
            self.reserve_rate_limit_for_dispatch(*segment_id, *estimate);
        }
        self.mark_download_pass_started(job_id);
        self.publish_active_stage_metrics();
    }

    fn rollback_download_batch_lease(&mut self, lease: DownloadBatchLease) {
        for work in lease.works {
            if let Err(error) = self.release_bandwidth_reservation(work.segment_id) {
                error!(error = %error, segment = %work.segment_id, "failed to roll back download bandwidth reservation");
            }
            if let Some(state) = self.jobs.get_mut(&lease.job_id) {
                state.download_queue.push(work);
            }
        }
        self.update_queue_metrics();
    }

    fn record_download_lane_observation(&mut self, result: &DownloadResult) {
        let Some(observation) = result.lane_observation.as_ref() else {
            return;
        };
        let Some(server_idx) = observation.server_idx else {
            return;
        };

        if let Some(rtt) = observation.rtt {
            self.download_lane_runtime
                .server_rtt
                .entry(server_idx)
                .or_default()
                .note(rtt);
        }

        let proof = self
            .download_lane_runtime
            .server_proof
            .entry(server_idx)
            .or_default();
        if observation.mode == DownloadLaneMode::Sequential {
            proof.note_sequential_result(result.data.is_ok(), observation.supports_pipelining);
        } else if observation.batch_complete {
            let now = Instant::now();
            let transition = proof.note_pipeline_batch(
                now,
                observation.mode,
                observation.batch_clean,
                observation.batch_response_count,
            );
            if observation.batch_clean {
                self.metrics
                    .download_pipeline_trial_success_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                self.metrics
                    .download_pipeline_trial_failure_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            if observation.unresolved_count > 0 {
                self.metrics
                    .download_pipeline_replay_items_total
                    .fetch_add(observation.unresolved_count, Ordering::Relaxed);
            }
            if matches!(
                transition,
                Some(ServerPipelineState::PipelineProvenDepth2)
                    | Some(ServerPipelineState::PipelineProvenDepth4)
            ) {
                self.metrics
                    .download_pipeline_proof_pass_total
                    .fetch_add(1, Ordering::Relaxed);
            } else if matches!(transition, Some(ServerPipelineState::PipelineBlocked)) {
                self.metrics
                    .download_pipeline_cooldown_total
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn try_dispatch_download_for_job(
        &mut self,
        job_id: JobId,
        pressure: DownloadPressure,
    ) -> DispatchAttempt {
        if self
            .download_restart_durable_lead_retry_after
            .get(&job_id)
            .is_some_and(|ready_at| *ready_at > Instant::now())
        {
            self.update_queue_metrics();
            return DispatchAttempt::NoWork;
        }
        self.apply_rar_unlock_priorities_if_dirty(job_id);
        let lease = match self.try_lease_initial_download_batch(job_id, pressure) {
            Ok(Some(lease)) => lease,
            Ok(None) => return DispatchAttempt::NoWork,
            Err(attempt) => return attempt,
        };
        let activation_items = Self::activation_items(&lease);
        self.activate_download_batch_lease(&lease, &activation_items, true);
        self.spawn_download_batch(lease);
        DispatchAttempt::Dispatched
    }

    fn maybe_start_ip_replacement_trial(
        &mut self,
        hot_job_id: JobId,
        pressure: DownloadPressure,
        configured_download_capacity: usize,
    ) {
        if self.ip_replacement_trial_extra_connections == 0 || self.ip_replacement_burst_active {
            return;
        }
        let normal_download_capacity =
            self.normal_download_connection_capacity_limit(configured_download_capacity);
        if pressure.suppresses_spillover()
            || configured_download_capacity == 0
            || self.active_download_connections != normal_download_capacity
            || self.active_recovery > 0
            || !self.job_has_dispatchable_work(hot_job_id)
        {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        }

        let now = Instant::now();
        let Some(candidate) = self.select_ip_replacement_candidate(now) else {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        };

        let Some(groups) =
            self.ip_replacement_trial_groups(hot_job_id, candidate.old_key.server_idx)
        else {
            self.metrics.note_ip_replacement_trial_blocked();
            return;
        };

        self.ip_replacement_burst_active = true;
        self.metrics.set_ip_replacement_burst_active(true);
        self.metrics.note_ip_replacement_trial_started();
        self.spawn_ip_replacement_candidate_acquire(hot_job_id, candidate, groups);
    }

    fn ip_replacement_trial_groups(&self, job_id: JobId, server_idx: usize) -> Option<Vec<String>> {
        self.jobs.get(&job_id).and_then(|state| {
            state
                .download_queue
                .peek_next_matching(|work| {
                    !work.is_recovery && !work.exclude_servers.contains(&server_idx)
                })
                .map(|work| work.groups.clone())
        })
    }

    fn spawn_ip_replacement_candidate_acquire(
        &self,
        job_id: JobId,
        candidate: IpReplacementCandidate,
        groups: Vec<String>,
    ) {
        let nntp = Arc::clone(&self.nntp);
        let trial_tx = self.ip_replacement_trial_tx.clone();

        tokio::spawn(async move {
            let server = weaver_nntp::ServerId(candidate.old_key.server_idx);
            match nntp
                .acquire_extra_body_lane_excluding(server, &groups, &[candidate.old_key.ip])
                .await
            {
                Ok(lane) => {
                    let candidate_ip = lane.remote_ip();
                    if candidate_ip == candidate.old_key.ip {
                        lane.discard().await;
                        let _ = trial_tx.send(IpReplacementTrialEvent::SameIpRejected).await;
                    } else {
                        let _ = trial_tx
                            .send(IpReplacementTrialEvent::CandidateAcquired {
                                job_id,
                                candidate,
                                candidate_ip,
                                lane: Box::new(lane),
                            })
                            .await;
                    }
                }
                Err(_) => {
                    let _ = trial_tx.send(IpReplacementTrialEvent::AcquireFailed).await;
                }
            }
        });
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

    fn spawn_decode_task(&self, work: PendingDecodeWork, output: Option<BufferHandle>) {
        let tx = self.decode_done_tx.clone();
        let PendingDecodeWork {
            segment_id,
            raw,
            source_server_idx,
            exclude_servers,
        } = work;
        let raw_size = raw.len() as u64;
        let metrics = Arc::clone(&self.metrics);
        metrics.note_decode_task_started(raw_size);

        tokio::task::spawn_blocking(move || {
            crate::runtime::perf_probe::record(
                "download.decode.task.enter",
                Duration::from_nanos(1),
            );
            let _profile_scope = crate::runtime::perf_probe::scope("download.decode.task");
            let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.task");
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();

            let send_decode_failure = |error: String| {
                let _profile_scope =
                    crate::runtime::perf_probe::scope("download.decode.send_failure");
                let _cpu_scope =
                    crate::runtime::perf_probe::cpu_scope("download.decode.send_failure");
                let _ = tx.blocking_send(DecodeDone::Failed {
                    segment_id,
                    raw_size,
                    error,
                    source_server_idx,
                    exclude_servers: exclude_servers.clone(),
                });
            };

            if let Some(mut output) = output {
                let Some(output_buf) = output.as_mut_slice() else {
                    let error = "failed to get unique pooled decode buffer".to_string();
                    metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                    warn!(segment = %segment_id, error, "yEnc decode failed");
                    send_decode_failure(error);
                    return;
                };

                let decode_result = {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.yenc");
                    weaver_yenc::decode_nntp(&raw, output_buf)
                };
                match decode_result {
                    Ok(decode_result) => {
                        output.set_len(decode_result.bytes_written);
                        metrics
                            .bytes_decoded
                            .fetch_add(decode_result.bytes_written as u64, Ordering::Relaxed);
                        metrics.segments_decoded.fetch_add(1, Ordering::Relaxed);

                        let file_offset = decode_result
                            .metadata
                            .begin
                            .map(|b| b.saturating_sub(1))
                            .unwrap_or(0);

                        let decoded = {
                            let _cpu_scope = crate::runtime::perf_probe::cpu_scope(
                                "download.decode.copy_to_owned",
                            );
                            DecodedChunk::from(output.as_slice().to_vec())
                        };

                        let _profile_scope =
                            crate::runtime::perf_probe::scope("download.decode.send_success");
                        let _cpu_scope =
                            crate::runtime::perf_probe::cpu_scope("download.decode.send_success");
                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            part_crc_verified: decode_result.expected_part_crc.is_some()
                                && decode_result.crc_valid,
                            part_crc: decode_result.part_crc,
                            expected_file_crc: decode_result.expected_file_crc,
                            data: decoded,
                            yenc_name: decode_result.metadata.name,
                        }));
                    }
                    Err(e) => {
                        if let weaver_yenc::YencError::CrcMismatch { .. } = &e {
                            metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        send_decode_failure(error);
                    }
                }
            } else {
                let mut output = {
                    let _cpu_scope =
                        crate::runtime::perf_probe::cpu_scope("download.decode.alloc_vec");
                    vec![0u8; raw.len()]
                };
                let decode_result = {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.yenc");
                    weaver_yenc::decode_nntp(&raw, &mut output)
                };
                match decode_result {
                    Ok(decode_result) => {
                        output.truncate(decode_result.bytes_written);
                        metrics
                            .bytes_decoded
                            .fetch_add(decode_result.bytes_written as u64, Ordering::Relaxed);
                        metrics.segments_decoded.fetch_add(1, Ordering::Relaxed);

                        let file_offset = decode_result
                            .metadata
                            .begin
                            .map(|b| b.saturating_sub(1))
                            .unwrap_or(0);

                        let _profile_scope =
                            crate::runtime::perf_probe::scope("download.decode.send_success");
                        let _cpu_scope =
                            crate::runtime::perf_probe::cpu_scope("download.decode.send_success");
                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            part_crc_verified: decode_result.expected_part_crc.is_some()
                                && decode_result.crc_valid,
                            part_crc: decode_result.part_crc,
                            expected_file_crc: decode_result.expected_file_crc,
                            data: DecodedChunk::from(output),
                            yenc_name: decode_result.metadata.name,
                        }));
                    }
                    Err(e) => {
                        if let weaver_yenc::YencError::CrcMismatch { .. } = &e {
                            metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        send_decode_failure(error);
                    }
                }
            }
        });
    }

    pub(crate) fn pump_decode_queue(&mut self) {
        if self.pending_decode.is_empty() {
            return;
        }

        let mut remaining = VecDeque::with_capacity(self.pending_decode.len());
        let decode_limit = self.tuner.params().decode_thread_count.max(1);
        let active_decodes = self.active_decodes_by_job.values().sum::<usize>();
        let mut available_decode_slots = decode_limit.saturating_sub(active_decodes);
        while let Some(work) = self.pending_decode.pop_front() {
            let job_id = work.segment_id.file_id.job_id;
            if self
                .jobs
                .get(&job_id)
                .is_none_or(|state| is_terminal_status(&state.status))
            {
                self.metrics
                    .note_decode_work_released(work.raw.len() as u64);
                debug!(
                    job_id = job_id.0,
                    segment = %work.segment_id,
                    "discarding queued decode work for inactive job"
                );
                continue;
            }

            if available_decode_slots == 0 {
                remaining.push_back(work);
                break;
            }

            if work.raw.len() > crate::runtime::buffers::BufferTier::Large.size_bytes() {
                self.note_decode_started(work.segment_id);
                self.spawn_decode_task(work, None);
                available_decode_slots -= 1;
                continue;
            }

            let tier = crate::runtime::buffers::BufferTier::for_size(work.raw.len());
            let Some(output) = self.buffers.try_acquire(tier) else {
                remaining.push_back(work);
                continue;
            };

            self.note_decode_started(work.segment_id);
            self.spawn_decode_task(work, Some(output));
            available_decode_slots -= 1;
        }

        remaining.extend(self.pending_decode.drain(..));
        self.pending_decode = remaining;
    }

    fn log_download_dispatch_liveness_stall(
        &mut self,
        now: Instant,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) {
        if self.active_downloads != 0 || self.active_download_connections != 0 {
            return;
        }

        let queue_depth = self
            .jobs
            .values()
            .map(|state| state.download_queue.len() + state.recovery_queue.len())
            .sum::<usize>();
        if queue_depth == 0 {
            self.last_download_dispatch_stall_log_at = None;
            return;
        }
        if self
            .last_download_dispatch_stall_log_at
            .is_some_and(|last| {
                now.saturating_duration_since(last) < DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL
            })
        {
            return;
        }

        self.last_download_dispatch_stall_log_at = Some(now);
        let hot_job_id = self.hot_dispatch_job.map(|id| id.0).unwrap_or_default();
        if let Some(job_id) = self.job_order.iter().copied().find(|job_id| {
            self.jobs.get(job_id).is_some_and(|state| {
                !state.download_queue.is_empty() || !state.recovery_queue.is_empty()
            })
        }) {
            let state = self
                .jobs
                .get(&job_id)
                .expect("job from job_order must exist while logging dispatch stall");
            let durable_retry_ms = self
                .download_restart_durable_lead_retry_after
                .get(&job_id)
                .map(|ready_at| ready_at.saturating_duration_since(now).as_millis() as u64)
                .unwrap_or_default();
            let backlog = self.download_pipeline_backlog_for_job(job_id);
            warn!(
                job_id = job_id.0,
                status = ?state.status,
                download_queue_len = state.download_queue.len(),
                recovery_queue_len = state.recovery_queue.len(),
                status_allows_dispatch = Self::status_allows_download_dispatch(&state.status),
                eligible_count,
                queue_depth,
                active_downloads = self.active_downloads,
                active_connections = self.active_download_connections,
                max_connections,
                pressure_state = pressure.state.as_str(),
                pressure_reason = pressure.reason.as_str(),
                hot_job_id,
                restart_durable_lead_retry_ms = durable_retry_ms,
                active_downloads_by_job = backlog.active_downloads,
                active_connections_by_job = backlog.active_connections,
                active_decodes_by_job = backlog.active_decodes,
                delayed_retries_by_job = backlog.delayed_retries,
                released_results_by_job = backlog.released_results,
                pending_decodes_by_job = backlog.pending_decodes,
                buffered_write_segments_by_job = backlog.buffered_write_segments,
                buffered_write_bytes_by_job = backlog.buffered_write_bytes,
                "download dispatch liveness stall: queued work but no active downloads"
            );
        } else {
            warn!(
                eligible_count,
                queue_depth,
                active_downloads = self.active_downloads,
                active_connections = self.active_download_connections,
                max_connections,
                pressure_state = pressure.state.as_str(),
                pressure_reason = pressure.reason.as_str(),
                hot_job_id,
                "download dispatch liveness stall: queued work but no active downloads"
            );
        }
    }

    /// Dispatch downloads across eligible jobs with hot-job-first ordering.
    ///
    /// Eligible jobs are grouped by submitted priority metadata (`HIGH`,
    /// `NORMAL`, `LOW`). Within the top runnable band, an already-active job
    /// keeps ownership so pooled NNTP connections stay hot; otherwise the
    /// earliest submitted runnable job becomes hot.
    ///
    /// This lets a newly submitted higher-priority job take newly freed slots
    /// immediately while preserving already in-flight work on lower-priority
    /// jobs. Within a priority band, the hot job gets all usable capacity
    /// first; spare capacity is shared only after that job cannot fill another
    /// slot from its own queue.
    ///
    /// A job is eligible if it is `Downloading` with queued segments, or in a
    /// post-processing state (`QueuedExtract`/`Extracting`/`Verifying`/
    /// `QueuedRepair`/`Repairing`) with promoted recovery segments in its
    /// download queue.
    ///
    /// This is hot-job-first by default. The current hot job receives usable
    /// capacity first; same/lower-band spillover is delayed until the hot job
    /// has completed warmup and an unused-capacity window proves underfill.
    ///
    /// When a bandwidth cap is enabled and within 15% of exhaustion, reverts
    /// to single-job dispatch to avoid wasting scarce remaining quota on
    /// lower-priority work.
    pub(crate) fn dispatch_downloads(&mut self) {
        let now = Instant::now();
        if self.global_paused || self.rate_limiter.should_wait() {
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
            self.publish_hot_dispatch_metrics(now);
            return;
        }
        if self.bandwidth_cap.cap_enabled() && self.bandwidth_cap.remaining_bytes() == 0 {
            self.update_queue_metrics();
            if self.active_downloads == 0 {
                debug!("dispatch blocked: bandwidth cap exhausted");
            }
            self.publish_hot_dispatch_metrics(now);
            return;
        }

        let pressure = self.refresh_download_pressure();
        if pressure.is_hard() {
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
        let Some((_hot_priority, hot_job_id)) = self.select_hot_dispatch_job(&eligible, now) else {
            self.clear_hot_dispatch_period();
            self.update_queue_metrics();
            return;
        };

        let active_connections_before_dispatch = self.active_download_connections;
        while self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.try_dispatch_download_for_job(hot_job_id, pressure) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => break,
                DispatchAttempt::StopAll => {
                    self.publish_hot_dispatch_metrics(now);
                    return;
                }
            }
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
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedPressure);
            false
        } else if bandwidth_cap_tight {
            self.hot_dispatch_underfill_since = None;
            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
            self.block_or_reclaim_spillover(SpilloverDecision::BlockedNearCap);
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
                    match self.try_dispatch_download_for_job(job_id, pressure) {
                        DispatchAttempt::Dispatched => {
                            self.start_spillover_loan(job_id, now, hot_speed_bps);
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

    /// Update shared atomic queue depth metrics from per-job queues.
    pub(crate) fn update_queue_metrics(&self) {
        let (total, recovery) = self.jobs.values().fold((0usize, 0usize), |(t, r), s| {
            (
                t + s.download_queue.len() + s.recovery_queue.len(),
                r + s.recovery_queue.len(),
            )
        });
        self.metrics
            .download_queue_depth
            .store(total, Ordering::Relaxed);
        self.metrics
            .recovery_queue_depth
            .store(recovery, Ordering::Relaxed);
        self.publish_active_stage_metrics();
    }

    fn download_data_from_decoded_trace(
        segment_id: SegmentId,
        trace: weaver_nntp::client::DecodedBodyTrace,
    ) -> (
        std::result::Result<DownloadPayload, DownloadError>,
        Vec<weaver_nntp::client::FetchAttemptTrace>,
        Option<usize>,
    ) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.result.translate");
        let successful_server_idx = trace.attempts.iter().rev().find_map(|attempt| {
            matches!(
                attempt.outcome,
                weaver_nntp::client::FetchAttemptOutcome::Success
            )
            .then_some(attempt.server_idx)
        });
        let attempted_server_idx = trace
            .attempts
            .iter()
            .rev()
            .map(|attempt| attempt.server_idx)
            .next();
        let source_server_idx = successful_server_idx.or(attempted_server_idx);
        let data = match trace.result {
            Ok(decoded) => {
                let weaver_nntp::client::DecodedBody {
                    raw_size,
                    decoded,
                    result,
                    cpu,
                    io,
                } = decoded;
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.parse_decode",
                    cpu.raw_decode,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.response_line",
                    cpu.response_line,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.yenc_header",
                    cpu.yenc_header,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.body_decode",
                    cpu.body_decode,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.yend_line",
                    cpu.yend_line,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.nntp_terminator",
                    cpu.nntp_terminator,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.nntp.read_poll",
                    cpu.read_poll,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.output_callback",
                    cpu.feed,
                );
                crate::runtime::perf_probe::record_cpu_sample("download.fused.finish", cpu.finish);
                crate::runtime::perf_probe::record_value("download.nntp.read.bytes", io.read_bytes);
                crate::runtime::perf_probe::record_value("download.nntp.read.calls", io.read_calls);
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.readiness_waits",
                    io.transport_read.readiness_waits,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.empty_readiness_wakes",
                    io.transport_read.empty_readiness_wakes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.calls",
                    io.transport_read.try_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.would_block",
                    io.transport_read.try_read_would_block,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.bytes",
                    io.transport_read.try_read_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.tls.read_calls",
                    io.transport_read.tls_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.tls.process_packets_calls",
                    io.transport_read.tls_process_packets_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.drain_calls",
                    io.transport_read.plaintext_drain_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.reader_calls",
                    io.transport_read.plaintext_reader_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.reader_would_block",
                    io.transport_read.plaintext_reader_would_block,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.bytes",
                    io.transport_read.plaintext_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.cached_returns",
                    io.transport_read.cached_plaintext_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.read_calls",
                    io.transport_read.s2n_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.read_bytes",
                    io.transport_read.s2n_read_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.target_full_returns",
                    io.transport_read.s2n_target_full_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.pending_empty_returns",
                    io.transport_read.s2n_pending_empty_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.pending_after_bytes_returns",
                    io.transport_read.s2n_pending_after_bytes_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.zero_returns",
                    io.transport_read.s2n_zero_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.s2n.bytes_per_read_call",
                    if io.transport_read.s2n_read_calls == 0 {
                        0
                    } else {
                        io.transport_read.s2n_read_bytes / io.transport_read.s2n_read_calls
                    },
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.read_calls",
                    io.transport_read.boring_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.read_bytes",
                    io.transport_read.boring_read_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.target_full_returns",
                    io.transport_read.boring_target_full_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.pending_empty_returns",
                    io.transport_read.boring_pending_empty_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.pending_after_bytes_returns",
                    io.transport_read.boring_pending_after_bytes_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.zero_returns",
                    io.transport_read.boring_zero_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.boring.bytes_per_read_call",
                    if io.transport_read.boring_read_calls == 0 {
                        0
                    } else {
                        io.transport_read.boring_read_bytes / io.transport_read.boring_read_calls
                    },
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.read.bytes_per_call",
                    if io.read_calls == 0 {
                        0
                    } else {
                        io.read_bytes / io.read_calls
                    },
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.buffer.leftover_after_terminator",
                    io.leftover_bytes_after_terminator,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.buffer.compactions",
                    io.buffer_compactions,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.decode.calls",
                    io.decode_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.input.chunks",
                    io.input_chunks,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.crc.update_calls",
                    io.crc_update_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.output.batches",
                    io.output_batches,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.encoded.bytes",
                    io.encoded_bytes_consumed,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.decoded.bytes",
                    io.decoded_bytes_written,
                );
                let file_offset = result
                    .metadata
                    .begin
                    .map(|b| b.saturating_sub(1))
                    .unwrap_or(0);

                let data = {
                    let _cpu =
                        crate::runtime::perf_probe::cpu_scope("download.inline_decode.into_chunk");
                    DecodedChunk::from(decoded)
                };

                Ok(DownloadPayload::Decoded(DecodeResult {
                    segment_id,
                    raw_size: raw_size as u64,
                    file_offset,
                    decoded_size: result.bytes_written as u32,
                    crc_valid: result.crc_valid,
                    part_crc_verified: result.expected_part_crc.is_some() && result.crc_valid,
                    part_crc: result.part_crc,
                    expected_file_crc: result.expected_file_crc,
                    data,
                    yenc_name: result.metadata.name,
                }))
            }
            Err(weaver_nntp::client::DecodedBodyError::Nntp(error)) => {
                Err(DownloadError::from_nntp(error))
            }
            Err(weaver_nntp::client::DecodedBodyError::Decode { raw_size, error }) => {
                let crc_mismatch = matches!(error, weaver_yenc::YencError::CrcMismatch { .. });
                Err(DownloadError::Decode {
                    raw_size: raw_size as u64,
                    error: error.to_string(),
                    crc_mismatch,
                })
            }
        };

        (data, trace.attempts, source_server_idx)
    }

    pub(crate) fn spawn_download_batch(&mut self, initial_lease: DownloadBatchLease) {
        if initial_lease.works.is_empty() {
            return;
        }

        debug_assert!(
            initial_lease
                .works
                .iter()
                .all(|work| initial_lease.compatibility.matches(work))
        );

        let nntp = Arc::clone(&self.nntp);
        let tx = self.download_done_tx.clone();
        let refill_tx = self.download_refill_tx.clone();
        let parked_tx = self.download_lane_parked_tx.clone();

        tokio::spawn(async move {
            let fetch_started = Instant::now();
            let mut lease = initial_lease;
            let mut recorded_mode = lease.lane_mode;
            let mut current_job_id: JobId;
            let park_reason: LaneParkReason;

            let mut lane = None;
            let mut acquire_error = None;
            for server in nntp
                .body_server_order(&lease.compatibility.exclude_servers)
                .await
            {
                match nntp
                    .acquire_body_lane(server, &lease.compatibility.groups)
                    .await
                {
                    Ok(acquired) => {
                        lane = Some(acquired);
                        break;
                    }
                    Err(error) => acquire_error = Some(error),
                }
            }

            let Some(mut lane) = lane else {
                let failure = DownloadFailure::from_lane_acquire_failure(acquire_error.as_ref());
                let is_recovery = lease.compatibility.is_recovery;
                let exclude_servers = lease.compatibility.exclude_servers.clone();
                let mode = lease.lane_mode;
                let job_id = lease.job_id;
                for work in lease.works {
                    let _ = tx
                        .send(DownloadResult {
                            segment_id: work.segment_id,
                            data: Err(DownloadError::Fetch(failure.clone())),
                            attempts: Vec::new(),
                            lane_observation: Some(DownloadLaneObservation {
                                server_idx: None,
                                mode,
                                supports_pipelining: false,
                                rtt: None,
                                batch_complete: true,
                                batch_clean: false,
                                batch_response_count: 0,
                                unresolved_count: 1,
                                connection_discarded: false,
                            }),
                            source_server_idx: None,
                            origin: DownloadResultOrigin::from_recovery(is_recovery),
                            retry_count: work.retry_count,
                            exclude_servers: exclude_servers.clone(),
                            release_connection_slot: false,
                        })
                        .await;
                }
                let _ = parked_tx
                    .send(DownloadLaneParked {
                        job_id,
                        mode,
                        reason: LaneParkReason::Error,
                        release_connection_slot: true,
                        release_ip_replacement_burst: false,
                    })
                    .await;
                crate::runtime::perf_probe::record("download.fetch_body", fetch_started.elapsed());
                return;
            };

            loop {
                let DownloadBatchLease {
                    job_id,
                    lane_mode,
                    server_modes,
                    compatibility,
                    works,
                } = lease;
                current_job_id = job_id;
                let server_idx = lane.server_id().0;
                let supports_pipelining = lane.supports_pipelining();
                let actual_mode = Self::actual_download_lane_mode(
                    lane_mode,
                    &server_modes,
                    server_idx,
                    supports_pipelining,
                );
                let is_recovery = compatibility.is_recovery;
                let exclude_servers = compatibility.exclude_servers.clone();
                let mut batch_clean_for_refill = true;
                let mut pending_works: std::collections::VecDeque<DownloadWork> =
                    works.into_iter().collect();

                while let Some(first_work) = pending_works.pop_front() {
                    let batch_depth = actual_mode.max_depth();
                    let mut batch_works = Vec::with_capacity(batch_depth);
                    batch_works.push(first_work);
                    while batch_works.len() < batch_depth {
                        let Some(work) = pending_works.pop_front() else {
                            break;
                        };
                        batch_works.push(work);
                    }

                    let message_ids: Vec<String> = batch_works
                        .iter()
                        .map(|work| work.message_id.to_string())
                        .collect();
                    let total = batch_works.len();
                    let mut completed = 0usize;
                    let mut works_by_index: Vec<Option<DownloadWork>> =
                        batch_works.into_iter().map(Some).collect();

                    match actual_mode {
                        DownloadLaneMode::Sequential => {
                            for (idx, message_id) in message_ids.iter().enumerate() {
                                let trace = lane.fetch_decoded_sequential(message_id).await;
                                completed += 1;
                                let work = works_by_index[idx]
                                    .take()
                                    .expect("download lane result emitted once per work item");
                                let segment_id = work.segment_id;
                                let retry_count = work.retry_count;
                                let (data, attempts, source_server_idx) =
                                    Self::download_data_from_decoded_trace(segment_id, trace);
                                let batch_clean = data.is_ok();
                                batch_clean_for_refill &= batch_clean;
                                let observation = DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: DownloadLaneMode::Sequential,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean,
                                    batch_response_count: 1,
                                    unresolved_count: 0,
                                    connection_discarded: !batch_clean,
                                };
                                let _ = tx
                                    .send(DownloadResult {
                                        segment_id,
                                        data,
                                        attempts,
                                        lane_observation: Some(observation),
                                        source_server_idx,
                                        origin: DownloadResultOrigin::from_recovery(is_recovery),
                                        retry_count,
                                        exclude_servers: exclude_servers.clone(),
                                        release_connection_slot: false,
                                    })
                                    .await;
                            }
                        }
                        DownloadLaneMode::PipelineDepth2 | DownloadLaneMode::PipelineDepth4 => {
                            let rtt = lane.rtt_ewma();
                            let tx_for_trace = tx.clone();
                            let exclude_servers_for_trace = exclude_servers.clone();
                            let stats = if actual_mode == DownloadLaneMode::PipelineDepth4 {
                                lane.fetch_decoded_pipeline_depth4(
                                    &message_ids,
                                    |idx, trace, meta| {
                                        completed += 1;
                                        let work = works_by_index[idx].take().expect(
                                            "download lane result emitted once per work item",
                                        );
                                        let segment_id = work.segment_id;
                                        let retry_count = work.retry_count;
                                        let (data, attempts, source_server_idx) =
                                            Self::download_data_from_decoded_trace(
                                                segment_id, trace,
                                            );
                                        let observation = DownloadLaneObservation {
                                            server_idx: Some(server_idx),
                                            mode: actual_mode,
                                            supports_pipelining,
                                            rtt,
                                            batch_complete: meta.batch_complete,
                                            batch_clean: meta.batch_clean,
                                            batch_response_count: meta.batch_response_count,
                                            unresolved_count: meta.unresolved_count,
                                            connection_discarded: meta.connection_discarded,
                                        };
                                        let tx = tx_for_trace.clone();
                                        let exclude_servers = exclude_servers_for_trace.clone();
                                        async move {
                                            let _ = tx
                                                .send(DownloadResult {
                                                    segment_id,
                                                    data,
                                                    attempts,
                                                    lane_observation: Some(observation),
                                                    source_server_idx,
                                                    origin: DownloadResultOrigin::from_recovery(
                                                        is_recovery,
                                                    ),
                                                    retry_count,
                                                    exclude_servers,
                                                    release_connection_slot: false,
                                                })
                                                .await;
                                        }
                                    },
                                )
                                .await
                            } else {
                                lane.fetch_decoded_pipeline_depth2(
                                    &message_ids,
                                    |idx, trace, meta| {
                                        completed += 1;
                                        let work = works_by_index[idx].take().expect(
                                            "download lane result emitted once per work item",
                                        );
                                        let segment_id = work.segment_id;
                                        let retry_count = work.retry_count;
                                        let (data, attempts, source_server_idx) =
                                            Self::download_data_from_decoded_trace(
                                                segment_id, trace,
                                            );
                                        let observation = DownloadLaneObservation {
                                            server_idx: Some(server_idx),
                                            mode: actual_mode,
                                            supports_pipelining,
                                            rtt,
                                            batch_complete: meta.batch_complete,
                                            batch_clean: meta.batch_clean,
                                            batch_response_count: meta.batch_response_count,
                                            unresolved_count: meta.unresolved_count,
                                            connection_discarded: meta.connection_discarded,
                                        };
                                        let tx = tx_for_trace.clone();
                                        let exclude_servers = exclude_servers_for_trace.clone();
                                        async move {
                                            let _ = tx
                                                .send(DownloadResult {
                                                    segment_id,
                                                    data,
                                                    attempts,
                                                    lane_observation: Some(observation),
                                                    source_server_idx,
                                                    origin: DownloadResultOrigin::from_recovery(
                                                        is_recovery,
                                                    ),
                                                    retry_count,
                                                    exclude_servers,
                                                    release_connection_slot: false,
                                                })
                                                .await;
                                        }
                                    },
                                )
                                .await
                            };

                            let batch_clean = !stats.connection_discarded
                                && !stats.response_order_mismatch
                                && stats.unresolved == 0;
                            batch_clean_for_refill &= batch_clean;
                        }
                    }

                    let unresolved_count = total.saturating_sub(completed);
                    if unresolved_count > 0 {
                        batch_clean_for_refill = false;
                    }
                    for work in works_by_index.into_iter().flatten() {
                        let _ = tx
                            .send(DownloadResult {
                                segment_id: work.segment_id,
                                data: Err(DownloadError::Fetch(DownloadFailure {
                                    kind: DownloadFailureKind::Transient,
                                    message: "batch ended without result".to_string(),
                                })),
                                attempts: Vec::new(),
                                lane_observation: Some(DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: actual_mode,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean: false,
                                    batch_response_count: completed as u64,
                                    unresolved_count: unresolved_count as u64,
                                    connection_discarded: true,
                                }),
                                source_server_idx: None,
                                origin: DownloadResultOrigin::from_recovery(is_recovery),
                                retry_count: work.retry_count,
                                exclude_servers: exclude_servers.clone(),
                                release_connection_slot: false,
                            })
                            .await;
                    }

                    if !batch_clean_for_refill {
                        break;
                    }
                }

                if !batch_clean_for_refill {
                    let tail_count = pending_works.len() as u64;
                    for work in pending_works {
                        let _ = tx
                            .send(DownloadResult {
                                segment_id: work.segment_id,
                                data: Err(DownloadError::Fetch(DownloadFailure {
                                    kind: DownloadFailureKind::Transient,
                                    message: "lane parked before leased article was requested"
                                        .to_string(),
                                })),
                                attempts: Vec::new(),
                                lane_observation: Some(DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: actual_mode,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean: false,
                                    batch_response_count: 0,
                                    unresolved_count: tail_count,
                                    connection_discarded: true,
                                }),
                                source_server_idx: None,
                                origin: DownloadResultOrigin::from_recovery(is_recovery),
                                retry_count: work.retry_count,
                                exclude_servers: exclude_servers.clone(),
                                release_connection_slot: false,
                            })
                            .await;
                    }
                    park_reason = LaneParkReason::Error;
                    break;
                }

                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if refill_tx
                    .send(DownloadLaneRefillRequest {
                        job_id,
                        server_idx,
                        remote_ip: lane.remote_ip(),
                        supports_pipelining,
                        current_mode: recorded_mode,
                        compatibility,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    park_reason = LaneParkReason::Error;
                    break;
                }

                match tokio::time::timeout(LANE_REFILL_GRACE, response_rx).await {
                    Ok(Ok(response)) => {
                        if let Some(next_lease) = response.lease
                            && !next_lease.works.is_empty()
                        {
                            recorded_mode = Self::actual_download_lane_mode(
                                next_lease.lane_mode,
                                &next_lease.server_modes,
                                server_idx,
                                supports_pipelining,
                            );
                            lease = next_lease;
                            continue;
                        }
                        park_reason = response.park_reason;
                        break;
                    }
                    _ => {
                        park_reason = LaneParkReason::ProbeYield;
                        break;
                    }
                }
            }

            lane.park();
            let _ = parked_tx
                .send(DownloadLaneParked {
                    job_id: current_job_id,
                    mode: recorded_mode,
                    reason: park_reason,
                    release_connection_slot: true,
                    release_ip_replacement_burst: false,
                })
                .await;
            crate::runtime::perf_probe::record("download.fetch_body", fetch_started.elapsed());
        });
    }

    pub(crate) fn handle_ip_replacement_trial_event(&mut self, event: IpReplacementTrialEvent) {
        match event {
            IpReplacementTrialEvent::CandidateAcquired {
                job_id,
                candidate,
                candidate_ip,
                lane,
            } => {
                if self.ip_replacement_trial_extra_connections == 0
                    || !self.ip_replacement_burst_active
                {
                    tokio::spawn(async move {
                        lane.discard().await;
                    });
                    self.metrics.note_ip_replacement_trial_rejected();
                    self.ip_replacement_burst_active = false;
                    self.metrics.set_ip_replacement_burst_active(false);
                    return;
                }

                let lease = match self
                    .try_lease_ip_replacement_trial_batch(job_id, candidate.old_key.server_idx)
                {
                    Ok(Some(lease)) => lease,
                    Ok(None) | Err(_) => {
                        let trial_tx = self.ip_replacement_trial_tx.clone();
                        tokio::spawn(async move {
                            let lane = lane;
                            lane.discard().await;
                            let _ = trial_tx
                                .send(IpReplacementTrialEvent::CandidateRejected)
                                .await;
                        });
                        return;
                    }
                };

                let activation_items = Self::activation_items(&lease);
                self.activate_download_batch_lease(&lease, &activation_items, false);
                self.spawn_ip_replacement_trial(lease, candidate, candidate_ip, *lane);
            }
            IpReplacementTrialEvent::AcquireFailed => {
                self.metrics.note_ip_replacement_trial_acquire_failed();
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::SameIpRejected => {
                self.metrics.note_ip_replacement_trial_same_ip_rejected();
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::CandidateRejected => {
                self.metrics.note_ip_replacement_trial_rejected();
                self.ip_replacement_burst_active = false;
                self.metrics.set_ip_replacement_burst_active(false);
            }
            IpReplacementTrialEvent::CandidateAccepted { old_key, samples } => {
                if self.ip_replacement_trial_extra_connections == 0
                    || !self.ip_replacement_burst_active
                {
                    return;
                }
                self.ip_replacement_retired_ips.insert(old_key);
                self.observe_ip_rtt_attempts(&samples);
            }
        }
    }

    fn spawn_ip_replacement_trial(
        &self,
        lease: DownloadBatchLease,
        candidate: IpReplacementCandidate,
        candidate_ip: IpAddr,
        mut lane: weaver_nntp::BodyLaneLease,
    ) {
        let nntp = Arc::clone(&self.nntp);
        let metrics = Arc::clone(&self.metrics);
        let tx = self.download_done_tx.clone();
        let parked_tx = self.download_lane_parked_tx.clone();
        let trial_tx = self.ip_replacement_trial_tx.clone();

        tokio::spawn(async move {
            let fetch_started = Instant::now();
            let server = weaver_nntp::ServerId(candidate.old_key.server_idx);
            let mode = DownloadLaneMode::Sequential;
            let exclude_servers = lease.compatibility.exclude_servers.clone();
            let job_id = lease.job_id;
            let work_count = lease.works.len();
            let mut trial_attempts = Vec::new();
            let mut park_reason = LaneParkReason::NoWork;

            let mut remaining = work_count;
            for work in lease.works {
                let trace = lane
                    .fetch_decoded_sequential(&work.message_id.to_string())
                    .await;
                trial_attempts.extend(trace.attempts.iter().cloned());
                let segment_id = work.segment_id;
                let retry_count = work.retry_count;
                let (data, attempts, source_server_idx) =
                    Self::download_data_from_decoded_trace(segment_id, trace);
                remaining = remaining.saturating_sub(1);
                if data.is_err() {
                    park_reason = LaneParkReason::Error;
                }
                let _ = tx
                    .send(DownloadResult {
                        segment_id,
                        data,
                        attempts,
                        lane_observation: None,
                        source_server_idx,
                        origin: DownloadResultOrigin::IpReplacementTrial,
                        retry_count,
                        exclude_servers: exclude_servers.clone(),
                        release_connection_slot: false,
                    })
                    .await;
            }

            let candidate_rtts = trial_attempts
                .iter()
                .filter(|attempt| {
                    attempt.outcome == FetchAttemptOutcome::Success
                        && attempt.server_idx == candidate.old_key.server_idx
                        && attempt.remote_ip == Some(candidate_ip)
                })
                .map(|attempt| attempt.elapsed.as_secs_f64() * 1000.0)
                .collect::<Vec<_>>();

            let accepted = if candidate_rtts.len() >= IP_REPLACEMENT_TRIAL_SAMPLES {
                let candidate_ewma_ms =
                    candidate_rtts.iter().sum::<f64>() / candidate_rtts.len() as f64;
                let better_by_ratio = candidate_ewma_ms
                    <= candidate.old_ewma_ms * IP_REPLACEMENT_CANDIDATE_BETTER_RATIO;
                let better_by_ms =
                    candidate.old_ewma_ms - candidate_ewma_ms >= IP_REPLACEMENT_CANDIDATE_BETTER_MS;
                if better_by_ratio && better_by_ms {
                    nntp.retire_server_ip(server, candidate.old_key.ip).await;
                    metrics.note_ip_replacement_trial_accepted();
                    metrics.note_ip_replacement_old_connection_retired();
                    debug!(
                        server = candidate.old_key.server_idx,
                        old_ip = %candidate.old_key.ip,
                        candidate_ip = %candidate_ip,
                        old_ewma_ms = candidate.old_ewma_ms,
                        candidate_ewma_ms,
                        baseline_ms = candidate.baseline_ms,
                        "accepted IP replacement trial"
                    );
                    true
                } else {
                    metrics.note_ip_replacement_trial_rejected();
                    debug!(
                        server = candidate.old_key.server_idx,
                        old_ip = %candidate.old_key.ip,
                        candidate_ip = %candidate_ip,
                        old_ewma_ms = candidate.old_ewma_ms,
                        candidate_ewma_ms,
                        baseline_ms = candidate.baseline_ms,
                        "rejected IP replacement trial"
                    );
                    false
                }
            } else {
                metrics.note_ip_replacement_trial_rejected();
                debug!(
                    server = candidate.old_key.server_idx,
                    old_ip = %candidate.old_key.ip,
                    candidate_ip = %candidate_ip,
                    successful_samples = candidate_rtts.len(),
                    "rejected IP replacement trial without enough distinct-IP proof"
                );
                false
            };

            if accepted {
                let accepted_samples = trial_attempts
                    .iter()
                    .filter(|attempt| {
                        attempt.outcome == FetchAttemptOutcome::Success
                            && attempt.server_idx == candidate.old_key.server_idx
                            && attempt.remote_ip == Some(candidate_ip)
                    })
                    .cloned()
                    .collect();
                let _ = trial_tx
                    .send(IpReplacementTrialEvent::CandidateAccepted {
                        old_key: candidate.old_key,
                        samples: accepted_samples,
                    })
                    .await;
                lane.park();
            } else {
                lane.discard().await;
            }
            let _ = parked_tx
                .send(DownloadLaneParked {
                    job_id,
                    mode,
                    reason: if accepted {
                        park_reason
                    } else {
                        LaneParkReason::ProofFailure
                    },
                    release_connection_slot: false,
                    release_ip_replacement_burst: true,
                })
                .await;
            crate::runtime::perf_probe::record(
                "download.ip_replacement_trial",
                fetch_started.elapsed(),
            );
        });
    }

    /// Handle a completed download — queue for decode.
    pub(crate) fn release_download_result(&mut self, result: &DownloadResult) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.release_result");
        self.record_download_lane_observation(result);
        self.active_downloads = self.active_downloads.saturating_sub(1);
        if result.release_connection_slot {
            if let Some(observation) = result.lane_observation.as_ref() {
                let reason = if observation.connection_discarded || !observation.batch_clean {
                    LaneParkReason::Error
                } else {
                    LaneParkReason::NoWork
                };
                self.note_download_lane_released(observation.mode, reason);
            }
            self.active_download_connections = self.active_download_connections.saturating_sub(1);
            let job_id = result.segment_id.file_id.job_id;
            if let Some(in_flight) = self.active_download_connections_by_job.get_mut(&job_id) {
                *in_flight = in_flight.saturating_sub(1);
                if *in_flight == 0 {
                    self.active_download_connections_by_job.remove(&job_id);
                }
            }
            self.clear_spillover_loan_if_idle();
        }
        if result.origin.is_recovery() {
            self.active_recovery = self.active_recovery.saturating_sub(1);
        }

        let job_id = result.segment_id.file_id.job_id;
        self.note_download_activity(job_id);
        if let Some(in_flight) = self.active_downloads_by_job.get_mut(&job_id) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_downloads_by_job.remove(&job_id);
            }
        }
        if let Some(in_flight) = self
            .active_downloads_by_file
            .get_mut(&result.segment_id.file_id)
        {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_downloads_by_file
                    .remove(&result.segment_id.file_id);
            }
        }
        if result.origin.counts_for_hot_primary()
            && self.hot_dispatch_job == Some(job_id)
            && result.data.is_ok()
        {
            self.hot_dispatch_successes = self.hot_dispatch_successes.saturating_add(1);
        }
        if result.origin.counts_for_hot_primary()
            && let Ok(DownloadPayload::Decoded(decoded)) = &result.data
        {
            self.ensure_job_transport_profile(job_id);
            if let Some(profile) = self.job_transport_profiles.get_mut(&job_id) {
                profile.note_body_size(decoded.raw_size);
            }
        }
        self.publish_active_stage_metrics();
        if let Err(error) = self.release_bandwidth_reservation(result.segment_id) {
            error!(error = %error, segment = %result.segment_id, "failed to release ISP bandwidth reservation");
        }
        let actual_raw_bytes = match &result.data {
            Ok(DownloadPayload::Raw(raw)) => Some(raw.len() as u64),
            Ok(DownloadPayload::Decoded(decoded)) => Some(decoded.raw_size),
            Err(DownloadError::Decode { raw_size, .. }) => Some(*raw_size),
            Err(_) => None,
        };
        if result.origin.counts_for_hot_primary()
            && self.hot_dispatch_job == Some(job_id)
            && let Ok(payload) = &result.data
        {
            let raw_bytes = match payload {
                DownloadPayload::Raw(raw) => raw.len() as u64,
                DownloadPayload::Decoded(decoded) => decoded.raw_size,
            };
            self.hot_dispatch_throughput_window
                .record(Instant::now(), raw_bytes);
        }
        self.reconcile_rate_limit_for_download(result.segment_id, actual_raw_bytes);
        if let Some(raw_bytes) = actual_raw_bytes
            && let Err(error) = self.record_download_bandwidth_usage(raw_bytes)
        {
            error!(
                error = %error,
                segment = %result.segment_id,
                "failed to record ISP bandwidth usage"
            );
        }
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(crate) fn handle_download_lane_parked(&mut self, parked: DownloadLaneParked) {
        if parked.release_connection_slot {
            self.note_download_lane_released(parked.mode, parked.reason);
            self.active_download_connections = self.active_download_connections.saturating_sub(1);
            if self.hot_dispatch_job != Some(parked.job_id) {
                self.hot_dispatch_spillover_loans.release_one(parked.job_id);
            }
            if let Some(in_flight) = self
                .active_download_connections_by_job
                .get_mut(&parked.job_id)
            {
                *in_flight = in_flight.saturating_sub(1);
                if *in_flight == 0 {
                    self.active_download_connections_by_job
                        .remove(&parked.job_id);
                }
            }
            self.clear_spillover_loan_if_idle();
        }
        if parked.release_ip_replacement_burst {
            self.ip_replacement_burst_active = false;
            self.metrics.set_ip_replacement_burst_active(false);
        }
        self.publish_active_stage_metrics();
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(crate) fn handle_download_lane_refill_request(
        &mut self,
        request: DownloadLaneRefillRequest,
    ) {
        let DownloadLaneRefillRequest {
            job_id,
            server_idx,
            remote_ip,
            supports_pipelining,
            current_mode,
            compatibility,
            response_tx,
        } = request;
        let now = Instant::now();
        let mut park_reason = LaneParkReason::NoWork;
        let mut allow_refill = !self.global_paused && !self.rate_limiter.should_wait();
        let lane_ip_key = ServerIpKey {
            server_idx,
            ip: remote_ip,
        };
        if self.ip_replacement_retired_ips.contains(&lane_ip_key) {
            allow_refill = false;
            park_reason = LaneParkReason::IpReplacementRetired;
        }
        if !allow_refill && park_reason == LaneParkReason::NoWork {
            park_reason = LaneParkReason::ProbeYield;
        }
        if allow_refill && let Err(error) = self.refresh_bandwidth_cap_window() {
            error!(error = %error, "failed to refresh ISP bandwidth cap state for lane refill");
            allow_refill = false;
            park_reason = LaneParkReason::Error;
        }
        if allow_refill
            && self.bandwidth_cap.cap_enabled()
            && self.bandwidth_cap.remaining_bytes() == 0
        {
            allow_refill = false;
            park_reason = LaneParkReason::Pressure;
        }

        let pressure = self.refresh_download_pressure();
        if allow_refill && pressure.state != DownloadPressureState::Clear {
            allow_refill = false;
            park_reason = LaneParkReason::Pressure;
        }

        if allow_refill {
            self.apply_rar_unlock_priorities_if_dirty(job_id);

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

            let requested_priority = eligible.iter().find_map(|(priority, _, eligible_job_id)| {
                (*eligible_job_id == job_id).then_some(*priority)
            });
            match self.select_hot_dispatch_job(&eligible, now) {
                Some((_hot_priority, hot_job_id)) if hot_job_id == job_id => {}
                Some((hot_priority, hot_job_id)) => {
                    let hot_speed_bps = self.hot_dispatch_speed_bps(now);
                    self.hot_dispatch_expansion_window
                        .refresh(now, hot_speed_bps);
                    let recent_expansion_helped = self
                        .hot_dispatch_expansion_window
                        .recent_improvement_pct(now)
                        >= HOT_DISPATCH_EXPANSION_HELPFUL_PERCENT;
                    let effective_capacity = self.effective_download_connection_capacity(
                        self.tuner.params().max_concurrent_downloads,
                    );
                    let best_mode_block_reason = self.hot_best_mode_block_reason(
                        hot_job_id,
                        effective_capacity,
                        pressure,
                        recent_expansion_helped,
                    );
                    let can_keep_lent_lane = requested_priority.is_some_and(|request_priority| {
                        self.hot_dispatch_mode == DispatchShareMode::Shared
                            && request_priority >= hot_priority
                            && best_mode_block_reason == HotBestModeBlockReason::None
                            && !self.hot_spillover_reclaim_pending_for(job_id)
                    });
                    if self.hot_spillover_reclaim_pending_for(job_id) {
                        allow_refill = false;
                        park_reason = LaneParkReason::SpilloverSpeedHarm;
                        self.block_or_reclaim_spillover(SpilloverDecision::ReclaimedSpeedHarm);
                    } else if can_keep_lent_lane {
                        self.hot_dispatch_last_lend_at = Some(now);
                    } else {
                        allow_refill = false;
                        if best_mode_block_reason == HotBestModeBlockReason::HotHasQueuedPrimary {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedHotCanUseCapacity,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if best_mode_block_reason
                            == HotBestModeBlockReason::RecentExpansionHelped
                        {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedRecentExpansionHelped,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if matches!(
                            best_mode_block_reason,
                            HotBestModeBlockReason::LaneCapacityAvailable
                                | HotBestModeBlockReason::PipelinePromotionPending
                        ) {
                            self.set_hot_best_mode_block_reason(best_mode_block_reason);
                            self.block_or_reclaim_spillover(
                                SpilloverDecision::BlockedBestModePending,
                            );
                            park_reason = LaneParkReason::SpilloverWithdraw;
                        } else if requested_priority.is_none() {
                            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
                            park_reason = LaneParkReason::NoWork;
                        } else {
                            self.set_hot_best_mode_block_reason(HotBestModeBlockReason::None);
                            park_reason = LaneParkReason::HotReclaim;
                        }
                    }
                }
                None => {
                    allow_refill = false;
                    self.clear_hot_dispatch_period();
                    park_reason = LaneParkReason::NoWork;
                }
            }
        }

        let lease = if allow_refill {
            match self.try_lease_refill_download_batch(job_id, compatibility, pressure) {
                Ok(lease) => lease,
                Err(DispatchAttempt::StopAll) => {
                    park_reason = LaneParkReason::Pressure;
                    None
                }
                Err(DispatchAttempt::NoWork) => None,
                Err(DispatchAttempt::Dispatched) => unreachable!("refill lease never dispatches"),
            }
        } else {
            None
        };

        let Some(lease) = lease else {
            self.metrics
                .download_lane_refill_parked_total
                .fetch_add(1, Ordering::Relaxed);
            let _ = response_tx.send(DownloadLaneRefillResponse {
                lease: None,
                park_reason,
            });
            self.update_queue_metrics();
            self.publish_hot_dispatch_metrics(now);
            return;
        };

        let activation_items = Self::activation_items(&lease);
        let next_mode = Self::actual_download_lane_mode(
            lease.lane_mode,
            &lease.server_modes,
            server_idx,
            supports_pipelining,
        );
        let is_recovery = lease.compatibility.is_recovery;
        let work_count = lease.works.len();
        match response_tx.send(DownloadLaneRefillResponse {
            lease: Some(lease),
            park_reason: LaneParkReason::NoWork,
        }) {
            Ok(()) => {
                self.metrics
                    .download_lane_refill_granted_total
                    .fetch_add(1, Ordering::Relaxed);
                self.activate_download_batch(
                    job_id,
                    is_recovery,
                    next_mode,
                    work_count,
                    &activation_items,
                    false,
                );
                self.note_download_lane_mode_changed(current_mode, next_mode);
            }
            Err(response) => {
                if let Some(lease) = response.lease {
                    self.rollback_download_batch_lease(lease);
                }
            }
        }
        self.update_queue_metrics();
        self.publish_hot_dispatch_metrics(now);
    }

    pub(crate) async fn handle_download_done(&mut self, result: DownloadResult) {
        self.release_download_result(&result);
        self.process_download_done(result).await;
    }

    pub(crate) fn note_released_download_result_pending(&mut self, job_id: JobId) {
        *self
            .pending_released_download_results_by_job
            .entry(job_id)
            .or_insert(0) += 1;
    }

    pub(crate) fn finish_released_download_result_processing(&mut self, job_id: JobId) {
        let remove = if let Some(pending) = self
            .pending_released_download_results_by_job
            .get_mut(&job_id)
        {
            *pending = pending.saturating_sub(1);
            *pending == 0
        } else {
            false
        };
        if remove {
            self.pending_released_download_results_by_job
                .remove(&job_id);
        }
    }

    pub(crate) async fn process_released_download_done(&mut self, result: DownloadResult) {
        let job_id = result.segment_id.file_id.job_id;
        self.process_download_done(result).await;
        self.finish_released_download_result_processing(job_id);
    }

    pub(crate) async fn process_download_done(&mut self, result: DownloadResult) {
        let job_id = result.segment_id.file_id.job_id;
        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
        {
            debug!(
                job_id = job_id.0,
                segment = %result.segment_id,
                "discarding download result for inactive job"
            );
            self.maybe_finish_download_pass(job_id);
            return;
        }

        let (excluded_servers, source_server_idx) = {
            let _cpu_scope =
                crate::runtime::perf_probe::cpu_scope("download.process_done.pre_match");
            let excluded_servers = result.exclude_servers.clone();
            let source_server_idx = result.source_server_idx;
            if result.origin != DownloadResultOrigin::IpReplacementTrial {
                self.observe_ip_rtt_attempts(&result.attempts);
            }

            for (attempt_index, attempt) in result.attempts.iter().enumerate() {
                let outcome = match attempt.outcome {
                    FetchAttemptOutcome::Success => {
                        crate::events::model::ServerAttemptOutcome::Success
                    }
                    FetchAttemptOutcome::NotFound => {
                        crate::events::model::ServerAttemptOutcome::NotFound
                    }
                    FetchAttemptOutcome::AuthenticationFailure => {
                        crate::events::model::ServerAttemptOutcome::AuthenticationFailure
                    }
                    FetchAttemptOutcome::TransientFailure => {
                        crate::events::model::ServerAttemptOutcome::TransientFailure
                    }
                    FetchAttemptOutcome::PermanentFailure => {
                        crate::events::model::ServerAttemptOutcome::PermanentFailure
                    }
                };
                let _ = self.event_tx.send(PipelineEvent::ServerAttempt {
                    segment_id: result.segment_id,
                    server_id: crate::ServerId(attempt.server_idx as u16),
                    attempt: (attempt_index as u32) + 1,
                    retry_count: result.retry_count,
                    latency_ms: attempt.elapsed.as_millis().min(u64::MAX as u128) as u64,
                    outcome,
                    error: attempt.error.clone(),
                    is_recovery: result.origin.is_recovery(),
                });
            }

            (excluded_servers, source_server_idx)
        };

        match result.data {
            Ok(DownloadPayload::Raw(raw)) => {
                let raw_size_bytes = raw.len() as u64;
                let raw_size = raw.len() as u32;
                self.metrics
                    .bytes_downloaded
                    .fetch_add(raw_size_bytes, Ordering::Relaxed);
                self.metrics
                    .segments_downloaded
                    .fetch_add(1, Ordering::Relaxed);

                // (Per-job byte tracking moved to handle_decode_done to use decoded size.)

                let _ = self.event_tx.send(PipelineEvent::ArticleDownloaded {
                    segment_id: result.segment_id,
                    raw_size,
                });

                self.metrics.note_decode_work_queued(raw_size_bytes);
                self.pending_decode.push_back(PendingDecodeWork {
                    segment_id: result.segment_id,
                    raw,
                    source_server_idx,
                    exclude_servers: excluded_servers,
                });
                self.pump_decode_queue();
            }
            Ok(DownloadPayload::Decoded(decoded)) => {
                let raw_size_bytes = decoded.raw_size;
                let raw_size = raw_size_bytes.min(u64::from(u32::MAX)) as u32;
                let decoded_size = decoded.decoded_size;
                {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope(
                        "download.process_done.decoded_pre_success",
                    );
                    self.metrics
                        .bytes_downloaded
                        .fetch_add(raw_size_bytes, Ordering::Relaxed);
                    self.metrics
                        .segments_downloaded
                        .fetch_add(1, Ordering::Relaxed);
                    self.metrics
                        .bytes_decoded
                        .fetch_add(decoded_size as u64, Ordering::Relaxed);
                    self.metrics
                        .segments_decoded
                        .fetch_add(1, Ordering::Relaxed);

                    let _ = self.event_tx.send(PipelineEvent::ArticleDownloaded {
                        segment_id: result.segment_id,
                        raw_size,
                    });
                }

                self.handle_decode_success(decoded).await;
            }
            Err(DownloadError::Decode {
                raw_size,
                error,
                crc_mismatch,
            }) => {
                let raw_size_for_event = raw_size.min(u64::from(u32::MAX)) as u32;
                self.metrics
                    .bytes_downloaded
                    .fetch_add(raw_size, Ordering::Relaxed);
                self.metrics
                    .segments_downloaded
                    .fetch_add(1, Ordering::Relaxed);
                if crc_mismatch {
                    self.metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                }
                self.metrics.decode_errors.fetch_add(1, Ordering::Relaxed);

                let _ = self.event_tx.send(PipelineEvent::ArticleDownloaded {
                    segment_id: result.segment_id,
                    raw_size: raw_size_for_event,
                });
                warn!(segment = %result.segment_id, error = %error, "streamed yEnc decode failed");
                self.handle_decode_failure(
                    result.segment_id,
                    &error,
                    &excluded_servers,
                    source_server_idx,
                );
            }
            Err(DownloadError::Fetch(failure)) => {
                if result.origin == DownloadResultOrigin::IpReplacementTrial
                    && !matches!(
                        failure.kind,
                        DownloadFailureKind::ArticleNotFound
                            | DownloadFailureKind::Auth
                            | DownloadFailureKind::Permanent
                    )
                {
                    if self.restore_download_result_work_without_retry(
                        result.segment_id,
                        result.retry_count,
                        excluded_servers,
                    ) {
                        debug!(
                            segment = %result.segment_id,
                            error = %failure.message,
                            "restored IP replacement trial work without consuming retry budget"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                if failure.kind == DownloadFailureKind::LaneUnavailable {
                    self.metrics
                        .download_failures_capacity_unavailable
                        .fetch_add(1, Ordering::Relaxed);
                    if self.schedule_download_work_without_retry_budget(
                        result.segment_id,
                        result.retry_count,
                        excluded_servers,
                        BODY_LANE_UNAVAILABLE_RETRY_DELAY,
                    ) {
                        debug!(
                            job_id = job_id.0,
                            segment = %result.segment_id,
                            retry_count = result.retry_count,
                            retry_delay_ms = BODY_LANE_UNAVAILABLE_RETRY_DELAY.as_millis() as u64,
                            error = %failure.message,
                            "body lane unavailable; requeued without consuming article retry budget"
                        );
                    } else {
                        warn!(
                            job_id = job_id.0,
                            segment = %result.segment_id,
                            error = %failure.message,
                            "body lane unavailable (job gone, not retrying)"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                match &failure.kind {
                    DownloadFailureKind::ArticleNotFound => self
                        .metrics
                        .download_failures_article_not_found
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::CapacityUnavailable
                    | DownloadFailureKind::LaneUnavailable => self
                        .metrics
                        .download_failures_capacity_unavailable
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::Transient => self
                        .metrics
                        .download_failures_transient
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::Auth => self
                        .metrics
                        .download_failures_auth
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::Permanent => self
                        .metrics
                        .download_failures_permanent
                        .fetch_add(1, Ordering::Relaxed),
                };
                let article_not_found_server_idx =
                    if failure.kind == DownloadFailureKind::ArticleNotFound {
                        source_server_idx.or_else(|| {
                            result.attempts.iter().rev().find_map(|attempt| {
                                matches!(
                                    attempt.outcome,
                                    weaver_nntp::client::FetchAttemptOutcome::NotFound
                                )
                                .then_some(attempt.server_idx)
                            })
                        })
                    } else {
                        source_server_idx
                    };
                let source_not_found = failure.kind == DownloadFailureKind::ArticleNotFound
                    && article_not_found_server_idx.is_some();
                let retry_exclude_servers = if failure.kind == DownloadFailureKind::ArticleNotFound
                {
                    Self::decode_retry_exclude_servers(
                        &excluded_servers,
                        article_not_found_server_idx,
                    )
                } else {
                    excluded_servers.clone()
                };
                let article_not_found_exhausted = failure.kind
                    == DownloadFailureKind::ArticleNotFound
                    && (retry_exclude_servers.is_empty() || {
                        let server_count = self.nntp.pool().server_count();
                        server_count > 0 && retry_exclude_servers.len() >= server_count
                    });

                if article_not_found_exhausted {
                    self.metrics
                        .articles_not_found
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = self.event_tx.send(PipelineEvent::ArticleNotFound {
                        segment_id: result.segment_id,
                    });
                    // Track failed bytes for health calculation.
                    let seg_id = result.segment_id;
                    let job_id = seg_id.file_id.job_id;
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        let file_idx = seg_id.file_id.file_index as usize;
                        if let Some(file_spec) = state.spec.files.get(file_idx)
                            && file_spec.role.counts_toward_health()
                            && let Some(seg_spec) = file_spec
                                .segments
                                .iter()
                                .find(|s| s.ordinal == seg_id.segment_number)
                        {
                            state.failed_bytes += seg_spec.bytes as u64;
                        }
                    }
                    self.mark_promoted_recovery_segment_unavailable(seg_id);
                    self.check_health(job_id);
                } else {
                    let seg_id = result.segment_id;
                    let capacity_unavailable =
                        failure.kind == DownloadFailureKind::CapacityUnavailable;
                    let next_retry = if capacity_unavailable || source_not_found {
                        result.retry_count
                    } else {
                        result.retry_count + 1
                    };

                    if next_retry > MAX_SEGMENT_RETRIES {
                        warn!(
                            segment = %seg_id,
                            error = %failure.message,
                            retries = MAX_SEGMENT_RETRIES,
                            "segment failed permanently after max retries"
                        );
                        self.metrics
                            .segments_failed_permanent
                            .fetch_add(1, Ordering::Relaxed);
                    } else if let Some(state) = self.jobs.get(&job_id) {
                        let file_idx = seg_id.file_id.file_index as usize;
                        if let Some(file_spec) = state.spec.files.get(file_idx)
                            && let Some(seg_spec) = file_spec
                                .segments
                                .iter()
                                .find(|s| s.ordinal == seg_id.segment_number)
                        {
                            let priority = file_spec.role.download_priority();
                            let work = DownloadWork {
                                segment_id: seg_id,
                                message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
                                groups: file_spec.groups.clone(),
                                priority,
                                byte_estimate: seg_spec.bytes,
                                retry_count: next_retry,
                                is_recovery: file_spec.role.is_recovery(),
                                exclude_servers: retry_exclude_servers.clone(),
                            };
                            self.metrics
                                .segments_retried
                                .fetch_add(1, Ordering::Relaxed);
                            let delay = if capacity_unavailable {
                                std::time::Duration::from_secs(1)
                            } else if source_not_found {
                                std::time::Duration::ZERO
                            } else {
                                std::time::Duration::from_secs(1 << (next_retry - 1))
                            };
                            self.note_retry_scheduled(seg_id);
                            debug!(
                                segment = %seg_id,
                                error = %failure.message,
                                retry = next_retry,
                                delay_secs = delay.as_secs(),
                                "download failed, scheduling retry"
                            );
                            let retry_tx = self.retry_tx.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                let _ = retry_tx.send(work).await;
                            });
                        }
                    } else {
                        warn!(
                            segment = %seg_id,
                            error = %failure.message,
                            "download failed (job gone, not retrying)"
                        );
                    }
                }
            }
        }

        self.maybe_finish_download_pass(job_id);
    }
}
