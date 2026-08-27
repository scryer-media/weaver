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
const HOT_LEASE_WARMUP_WORK_LIMIT: usize = 16;
const NO_ELIGIBLE_SERVER_WARN_INTERVAL: Duration = Duration::from_secs(60);
const BODY_FETCH_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
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
        let lease = match self.try_lease_initial_download_batch(job_id, pressure, selection) {
            Ok(Some(lease)) => lease,
            Ok(None) => return DispatchAttempt::NoWork,
            Err(attempt) => return attempt,
        };
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
            self.publish_hot_dispatch_metrics(now);
            return;
        }

        let mut dispatch_budget = usize::MAX;
        if let Some(delay) = self.soft_pressure_dispatch_delay(pressure) {
            if self
                .download_pressure_soft_dispatch_after
                .is_some_and(|ready_at| ready_at > now)
            {
                self.update_queue_metrics();
                self.publish_hot_dispatch_metrics(now);
                return;
            }
            self.download_pressure_soft_dispatch_after = Some(now + delay);
            dispatch_budget = 1;
        } else {
            self.download_pressure_soft_dispatch_after = None;
        }

        let tuner_max = self.tuner.params().max_concurrent_downloads;
        let max = self.effective_download_connection_capacity(tuner_max);
        let mut eligible = self
            .job_order
            .iter()
            .enumerate()
            .filter_map(|(index, id)| {
                let state = self.jobs.get(id)?;
                (!state.download_queue.is_empty()
                    && Self::status_allows_download_dispatch(&state.status))
                .then_some((Self::job_dispatch_priority(state), index, *id))
            })
            .collect::<Vec<_>>();
        eligible.sort_unstable();

        if eligible.is_empty() && self.active_downloads == 0 {
            for job_id in self.job_order.clone() {
                let Some(state) = self.jobs.get(&job_id) else {
                    continue;
                };
                let parked_recovery_only =
                    state.download_queue.is_empty() && !state.recovery_queue.is_empty();
                let extraction_only_idle = parked_recovery_only
                    && matches!(
                        state.status,
                        JobStatus::QueuedExtract | JobStatus::Extracting
                    );
                if parked_recovery_only
                    && Self::status_allows_download_dispatch(&state.status)
                    && !extraction_only_idle
                    && !self.job_has_pending_download_pipeline_work(job_id)
                {
                    self.schedule_job_completion_check_if_download_pipeline_drained(
                        job_id,
                        "parked_recovery_idle",
                    );
                }
            }
        }

        let Some(hot_job_id) = self.select_hot_dispatch_job(&eligible) else {
            self.clear_hot_dispatch_period();
            self.update_queue_metrics();
            return;
        };

        let eligible_count = eligible.len();
        let active_connections_before_dispatch = self.active_download_connections;
        let mut hot_saturated = false;
        while self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.try_dispatch_download_for_job(
                hot_job_id,
                pressure,
                DownloadWorkSelection::Any,
            ) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => {
                    hot_saturated = true;
                    break;
                }
                DispatchAttempt::StopAll => {
                    self.publish_hot_dispatch_metrics(now);
                    return;
                }
            }
        }

        if hot_saturated {
            let successors =
                Self::hot_dispatch_successors(&eligible, hot_job_id).collect::<Vec<_>>();
            for successor_job_id in successors {
                if !self.can_dispatch_successor(hot_job_id, successor_job_id) {
                    continue;
                }
                while self.active_download_connections < max
                    && !self.rate_limiter.should_wait()
                    && dispatch_budget > 0
                {
                    match self.try_dispatch_download_for_job(
                        successor_job_id,
                        pressure,
                        DownloadWorkSelection::Any,
                    ) {
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
