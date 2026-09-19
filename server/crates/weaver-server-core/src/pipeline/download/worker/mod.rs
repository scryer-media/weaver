use super::*;
use crate::pipeline::download::scheduler::Handout;
use crate::pipeline::download::transport::{RungChange, ServerPipelineExplorer};
use weaver_nntp::client::FetchAttemptOutcome;

mod completion;
mod direct_store;
mod eligibility;
mod ip_replacement;
mod lanes;
mod leases;
mod metrics;
mod ownership;
mod pressure;
mod refill;
mod spawn;

#[cfg(test)]
pub(in crate::pipeline) use ip_replacement::{
    is_ip_replacement_policy_stop, should_neutrally_park_ip_replacement,
};
pub(in crate::pipeline) use refill::HeldDownloadRefill;
#[cfg(test)]
pub(in crate::pipeline) use spawn::lane_acquire_failure_for_work;

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
    released_result_bytes: u64,
    pending_decodes: usize,
    buffered_write_segments: usize,
    buffered_write_bytes: u64,
}

const DOWNLOAD_PRESSURE_SOFT_PERCENT: u64 = 70;
const SOFT_PRESSURE_DISPATCH_MAX_DELAY: Duration = Duration::from_millis(150);
const SOFT_PRESSURE_DISPATCH_MIN_DELAY: Duration = Duration::from_millis(1);
const NO_ELIGIBLE_SERVER_WARN_INTERVAL: Duration = Duration::from_secs(60);
const BODY_LANE_CAPACITY_LOG_INTERVAL: Duration = Duration::from_secs(60);
const BODY_FETCH_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
const OWNED_LANE_ACQUIRE_FAILURE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// How often one job may report that a dispatch pass found it ineligible.
///
/// A dispatch wake costs a pass, and passes come in bursts: a job in a phase
/// that dispatches nothing — extracting, repairing, moving — is re-visited by
/// every one of them. Unthrottled that is hundreds of identical lines a second
/// for as long as the phase lasts, written synchronously on the pipeline actor
/// thread, which is the same thread the stall itself needs to get work moving.
const DISPATCH_INELIGIBLE_LOG_INTERVAL: Duration = Duration::from_secs(60);
/// How often a dispatch pass re-asks for its first server ranking while the
/// health lock is held by a lane worker, and how long it waits between asks.
/// The critical sections behind that lock are microseconds long, so the whole
/// budget is well under a millisecond and nearly every ask after the first
/// lands.
const PASS_RANKING_CONTENTION_RETRIES: usize = 8;
const PASS_RANKING_CONTENTION_PAUSE: Duration = Duration::from_micros(50);

/// How many jobs may hold a throttle window at once.
///
/// Nothing is asked to tidy up after a job that leaves, so the map bounds
/// itself: expired windows go first, and if every window is still live the
/// whole map goes. Losing a window costs one extra log line, nothing more.
const JOB_LOG_THROTTLE_MAX_JOBS: usize = 256;

/// One log-rate window per job.
///
/// These throttles used to share a single instant across the worker, so the
/// one job failing in a loop spent the window and every other job's *first*
/// line was dropped along with it — silently, which also left the real rate
/// unreadable from the log. Each job gets its own window here, and whatever a
/// closed window swallowed is counted and reported by the next line that gets
/// through it.
#[derive(Debug, Default)]
pub(crate) struct JobLogThrottle {
    windows: HashMap<JobId, JobLogWindow>,
}

#[derive(Debug, Clone, Copy)]
struct JobLogWindow {
    emitted_at: Instant,
    suppressed: u64,
}

impl JobLogThrottle {
    /// Whether this job may log now, and how many of its emissions the closed
    /// window swallowed since the last one that got through.
    pub(crate) fn admit(&mut self, job_id: JobId, interval: Duration) -> Option<u64> {
        self.admit_at(job_id, interval, Instant::now())
    }

    fn admit_at(&mut self, job_id: JobId, interval: Duration, now: Instant) -> Option<u64> {
        match self.windows.get_mut(&job_id) {
            Some(window) if now.duration_since(window.emitted_at) < interval => {
                window.suppressed = window.suppressed.saturating_add(1);
                None
            }
            Some(window) => {
                let suppressed = window.suppressed;
                window.emitted_at = now;
                window.suppressed = 0;
                Some(suppressed)
            }
            None => {
                self.bound_windows(interval, now);
                self.windows.insert(
                    job_id,
                    JobLogWindow {
                        emitted_at: now,
                        suppressed: 0,
                    },
                );
                Some(0)
            }
        }
    }

    fn bound_windows(&mut self, interval: Duration, now: Instant) {
        if self.windows.len() < JOB_LOG_THROTTLE_MAX_JOBS {
            return;
        }
        self.windows
            .retain(|_, window| now.duration_since(window.emitted_at) < interval);
        if self.windows.len() >= JOB_LOG_THROTTLE_MAX_JOBS {
            self.windows.clear();
        }
    }

    #[cfg(test)]
    pub(crate) fn last_emitted_at(&self, job_id: JobId) -> Option<Instant> {
        self.windows.get(&job_id).map(|window| window.emitted_at)
    }
}

/// How long the servers must stay below their connection cap, with work
/// queued, before that is reported. Short enough to catch a lane that never
/// opens, long enough that ordinary refill gaps between batches say nothing.
const DOWNLOAD_LANES_UNDER_CAP_WINDOW: Duration = Duration::from_secs(5);
const DOWNLOAD_LANES_UNDER_CAP_LOG_INTERVAL: Duration = Duration::from_secs(60);
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
const BODY_SERVER_BLOCKED_RECHECK_DELAY: Duration = Duration::from_secs(5);
const DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub(crate) struct DownloadPressure {
    pub(in crate::pipeline) state: DownloadPressureState,
    reason: DownloadPressureReason,
    decode_backlog_bytes: u64,
    /// Resident bytes control shared write pressure.
    write_buffered_bytes: u64,
    /// Known UU files may dispatch only their cursor-closing work while capped.
    uu_spool_admission_capped: bool,
    decode_hard_limit_bytes: u64,
    write_hard_limit_bytes: u64,
}

impl Pipeline {
    /// The job every handout comes from while it can serve the asking
    /// server: the first eligible job in dispatch order.
    pub(in crate::pipeline) fn current_hot_job(&self) -> Option<JobId> {
        self.download_scheduler_eligible_jobs().first().copied()
    }

    /// The servers a dispatch pass hands leases to, best first. A pass's
    /// first ranking is worth a short wait when the health lock is busy: the
    /// only fallback is the idle connections, and a pass that starts with
    /// none of those would otherwise send nothing at all.
    fn rank_servers_for_pass(&self, first_of_pass: bool) -> Option<Vec<usize>> {
        let attempts = if first_of_pass {
            PASS_RANKING_CONTENTION_RETRIES
        } else {
            1
        };
        for attempt in 0..attempts {
            if let Some(order) = self.nntp.blocking_body_server_order(&[]) {
                return Some(order.into_iter().map(|server| server.0).collect());
            }
            if attempt + 1 < attempts {
                std::thread::sleep(PASS_RANKING_CONTENTION_PAUSE);
            }
        }
        None
    }

    /// Start one more connection: choose the server, ask the scheduler what
    /// that server should fetch, lease it and hand it to a worker.
    ///
    /// The server comes first because the scheduler's answer is per server:
    /// the hot job may have nothing left that server A may fetch while server
    /// B could still carry it. Servers are tried in the pool's own ranking,
    /// those where an idle worker already holds a connection ahead of the
    /// rest, and a server is only asked while it can still seat the lease:
    /// an idle connection or a free permit beyond what this pass has already
    /// sent it. Without that, a pass that opens several lanes would send two
    /// dials at a server with one free permit, and the second would sit in
    /// the pool's contention loop instead of fetching. The seat count is the
    /// one the pass started with, not a fresh reading — see the retain below.
    fn dispatch_one_download_lane(
        &mut self,
        pressure: DownloadPressure,
        sent_this_pass: &mut HashMap<usize, usize>,
        headroom_at_pass_start: &mut HashMap<usize, usize>,
        ranked_this_pass: &mut Option<Vec<usize>>,
    ) -> DispatchAttempt {
        let mut idle_by_server: HashMap<usize, usize> = HashMap::new();
        for server in self
            .owned_download_lane_pool
            .idle_lane_servers()
            .into_iter()
            .flatten()
        {
            *idle_by_server.entry(server).or_default() += 1;
        }
        let backfill_flags = self.nntp.pool().server_backfill_flags();
        let mut servers: Vec<usize> = match self.rank_servers_for_pass(ranked_this_pass.is_none()) {
            Some(order) => {
                *ranked_this_pass = Some(order.clone());
                order
            }
            // The ranking is contended right now, most likely by the lane
            // workers this very pass just started, which take the same health
            // lock as they dial. The order the pass last ranked is still good
            // for the seats it started with; only a pass that never ranked
            // falls back to the idle connections, and the next pass ranks
            // again. Giving up here instead cut a pass short at whatever
            // point the contention landed, and at a different one each run.
            None => match ranked_this_pass {
                Some(order) => order.clone(),
                None => idle_by_server.keys().copied().collect(),
            },
        };
        servers.sort_by_key(|server| !idle_by_server.contains_key(server));
        // The backfill tier comes last, and only for what the fill tier has
        // given up on: the scheduler's filter holds a backfill server to
        // articles every available fill server is excluded from.
        if !servers.iter().any(|server| backfill_flags[*server]) {
            let fill: Vec<usize> = (0..backfill_flags.len())
                .filter(|idx| !backfill_flags[*idx])
                .collect();
            if let Some(order) = self.nntp.blocking_body_server_order(&fill) {
                for server in order {
                    if backfill_flags[server.0] && !servers.contains(&server.0) {
                        servers.push(server.0);
                    }
                }
            }
        }
        servers.retain(|server| {
            let idle = idle_by_server.get(server).copied().unwrap_or(0);
            let (available, _) = self.nntp.pool().server_load(*server);
            // The seat count is read once per server per pass, the first time
            // the pass looks at it and so before it has sent anything there.
            // Re-reading it would charge each lease twice: once as a lease in
            // `sent`, and again as the permit its lane has meanwhile taken —
            // the lane worker runs on its own thread, so how much of that has
            // happened by the next iteration is a matter of thread scheduling.
            // A pass that measured itself that way stopped around half its
            // configured capacity, and stopped at a different place each run.
            let seats = *headroom_at_pass_start
                .entry(*server)
                .or_insert(idle + available);
            let sent = sent_this_pass.get(server).copied().unwrap_or(0);
            seats > sent
        });
        for server_idx in servers {
            let spill_in_flight = self.spill_job_in_flight_on(server_idx);
            let lane_mode = self.download_lane_mode_for_server(server_idx, pressure, true);
            let want = self.download_refill_want(lane_mode);
            let works = match self.next_works(server_idx, want, spill_in_flight, pressure) {
                Handout::Idle => continue,
                Handout::Yield(_) => return DispatchAttempt::StopAll,
                Handout::Works(works) => works,
            };
            let lane_id = Self::next_download_lane_id();
            let Some(lease) =
                self.lease_for_handout(lane_id, server_idx, lane_mode, pressure, works)
            else {
                return DispatchAttempt::NoWork;
            };
            let job_id = lease.job_id;
            let activation_items = Self::activation_items(&lease);
            self.activate_download_batch_lease(&lease, &activation_items, true);
            if let Some(owner) = self.download_lane_owners.get_mut(&lane_id) {
                owner.server_idx = Some(server_idx);
            }
            self.spawn_download_batch(lease);
            *sent_this_pass.entry(server_idx).or_default() += 1;
            self.warm_idle_download_lanes_for_barrier(job_id);
            return DispatchAttempt::Dispatched;
        }
        DispatchAttempt::NoWork
    }

    /// Open the connections a barred job is about to need, while it is barred.
    ///
    /// A job whose first wave is held behind a barrier — the PAR2 index
    /// bootstrap is the standing case — may only lease the barrier's own work,
    /// so dispatch cuts one batch and stops. Nothing in the barrier requires
    /// the *connections* to wait: without this, the moment the grid publishes
    /// and payload leases go out, each lane pays a TCP, TLS, greeting and
    /// authentication exchange before its first BODY, one after another, on a
    /// job that has been waiting on exactly those bytes.
    ///
    /// A warm lane is idle, not active: it is counted in no connection gauge,
    /// and if the barrier never lifts it parks with the rest of the pool. The
    /// dial is asked for after the lease has been handed to a worker, and each
    /// warm runs on its own worker thread, so nothing already leased waits on
    /// one.
    fn warm_idle_download_lanes_for_barrier(&mut self, job_id: JobId) {
        let capacity = self
            .effective_download_connection_capacity(self.tuner.params().max_concurrent_downloads);
        let free = capacity.saturating_sub(self.active_download_connections);
        if free == 0 {
            return;
        }
        // Only while the job is actually holding work back. Ordinary dispatch
        // needs no help: it leases into every free connection itself.
        if self.par2_metadata_bootstrap_files(job_id).is_none() {
            return;
        }
        // Sized for a payload article, falling back to the barrier's own
        // class when the payload queue has not been built yet.
        let Some(byte_estimate) = self.jobs.get(&job_id).and_then(|state| {
            state
                .download_queue
                .peek_in_class(false)
                .or_else(|| state.download_queue.peek_in_class(true))
                .map(|work| work.byte_estimate)
        }) else {
            return;
        };
        let exclude_servers: Arc<[usize]> = Arc::from(self.effective_exclude_servers(job_id, &[]));
        let warmed = self.owned_download_lane_pool.warm(
            &self.nntp,
            exclude_servers,
            Self::bandwidth_reservation_estimate(byte_estimate),
            free,
        );
        if warmed > 0 {
            debug!(
                job_id = job_id.0,
                lanes = warmed,
                "warming idle download lanes behind a job barrier"
            );
        }
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
            //
            // A health probe still in flight is in the same position: its
            // estimate has been overtaken by the job's own terminal states, and
            // leaving it running only holds the job in `Checking` — and the
            // completion checkpoint with it — until the probe times out.
            self.retire_health_probe_if_download_pipeline_drained(job_id);
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
    pub(crate) fn dispatch_downloads(&mut self) {
        let now = Instant::now();
        self.download_dispatch_wake = false;
        // Lanes already connected and waiting in the actor are answered
        // before any dial: an established socket outranks a new one.
        self.service_held_download_refills();
        if self.global_paused || self.rate_limiter.should_wait() {
            if self.active_downloads == 0 {
                debug!(
                    global_paused = self.global_paused,
                    rate_wait = self.rate_limiter.should_wait(),
                    "dispatch blocked: paused/rate"
                );
            }
            return;
        }
        if self.nntp_handoff_draining {
            return;
        }
        if let Err(error) = self.refresh_bandwidth_cap_window() {
            error!(error = %error, "failed to refresh ISP bandwidth cap state");
            return;
        }
        if self.bandwidth_cap.cap_enabled() && self.bandwidth_cap.remaining_bytes() == 0 {
            self.update_queue_metrics();
            if self.active_downloads == 0 {
                debug!("dispatch blocked: bandwidth cap exhausted");
            }
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
            return;
        }
        let soft_dispatch_delay = self.soft_pressure_dispatch_delay(pressure);
        let mut dispatch_budget = usize::MAX;
        if let Some(delay) = soft_dispatch_delay {
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
        let eligible = self.download_scheduler_eligible_jobs();
        if eligible.is_empty() && self.active_downloads == 0 {
            let mut drained_parked_recovery_jobs = Vec::new();
            // Collected rather than logged in place: the warning is throttled
            // per job, and the throttle needs `&mut self` while this walk
            // borrows `job_order`.
            let mut ineligible_jobs = Vec::new();
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
                    } else if matches!(
                        s.status,
                        JobStatus::Paused | JobStatus::Complete | JobStatus::Failed { .. }
                    ) {
                        // A job someone paused, or one that is already over, is
                        // not dispatching because it was told not to. Warning
                        // about it once per job per pass is how a single paused
                        // job fills a log.
                        debug!(
                            job_id = jid.0,
                            idx = i,
                            status = ?s.status,
                            queue_len = s.download_queue.len(),
                            recovery_len = s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                            "dispatch idle: job not eligible by status"
                        );
                    } else {
                        ineligible_jobs.push((
                            *jid,
                            i,
                            s.status.clone(),
                            s.download_queue.len(),
                            s.recovery_queue.len(),
                            parked_recovery_only,
                            status_allows_dispatch,
                        ));
                    }
                }
            }
            for (
                job_id,
                idx,
                status,
                queue_len,
                recovery_len,
                parked_recovery_only,
                status_allows_dispatch,
            ) in ineligible_jobs
            {
                let Some(suppressed_since_last) = self
                    .dispatch_ineligible_log_throttle
                    .admit(job_id, DISPATCH_INELIGIBLE_LOG_INTERVAL)
                else {
                    continue;
                };
                warn!(
                    job_id = job_id.0,
                    idx,
                    status = ?status,
                    queue_len,
                    recovery_len,
                    parked_recovery_only,
                    status_allows_dispatch,
                    suppressed_since_last,
                    "dispatch stall: job not eligible"
                );
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
        let Some(hot_job_id) = eligible.first().copied() else {
            self.update_queue_metrics();
            return;
        };
        let active_connections_before_dispatch = self.active_download_connections;
        let mut sent_this_pass: HashMap<usize, usize> = HashMap::new();
        let mut headroom_at_pass_start: HashMap<usize, usize> = HashMap::new();
        let mut ranked_this_pass: Option<Vec<usize>> = None;
        while self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.dispatch_one_download_lane(
                pressure,
                &mut sent_this_pass,
                &mut headroom_at_pass_start,
                &mut ranked_this_pass,
            ) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => break,
                DispatchAttempt::StopAll => return,
            }
        }
        if active_connections_before_dispatch == 0 && self.active_download_connections == 0 {
            self.log_download_dispatch_liveness_stall(now, pressure, max, eligible_count);
        }
        self.log_download_lanes_under_cap(now, max);
        self.maybe_start_ip_replacement_trial(hot_job_id, pressure, max);
        self.update_queue_metrics();
    }
}

#[cfg(test)]
mod job_log_throttle_tests {
    use super::*;

    const WINDOW: Duration = Duration::from_secs(60);

    /// A job failing in a loop must not spend another job's first line, and
    /// what its own window swallowed must be readable from the next line.
    #[test]
    fn a_noisy_job_does_not_throttle_a_quiet_one() {
        let mut throttle = JobLogThrottle::default();
        let noisy = JobId(9001);
        let quiet = JobId(9002);
        let start = Instant::now();

        assert_eq!(
            throttle.admit_at(noisy, WINDOW, start),
            Some(0),
            "the first emission of a window always gets through"
        );
        for _ in 0..500 {
            assert_eq!(
                throttle.admit_at(noisy, WINDOW, start),
                None,
                "repeats inside the window are counted, not emitted"
            );
        }

        assert_eq!(
            throttle.admit_at(quiet, WINDOW, start),
            Some(0),
            "another job's first emission is its own window, not the noisy job's"
        );

        assert_eq!(
            throttle.admit_at(noisy, WINDOW, start + WINDOW),
            Some(500),
            "the next line that gets through reports what the window swallowed"
        );
        assert_eq!(
            throttle.admit_at(noisy, WINDOW, start + WINDOW * 2),
            Some(0),
            "and the count starts again from the line that reported it"
        );
    }

    /// The map is bounded, so a long-lived worker cannot accumulate a window
    /// for every job it has ever seen.
    #[test]
    fn the_throttle_bounds_the_jobs_it_remembers() {
        let mut throttle = JobLogThrottle::default();
        let start = Instant::now();
        for job in 0..(JOB_LOG_THROTTLE_MAX_JOBS as u64 * 2) {
            throttle.admit_at(JobId(job), WINDOW, start);
        }
        assert!(throttle.windows.len() <= JOB_LOG_THROTTLE_MAX_JOBS);
    }
}
