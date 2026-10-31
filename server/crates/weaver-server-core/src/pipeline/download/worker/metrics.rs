// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// Record checkpoint geometry after an article has finished decoding. This is
// intentionally a completion-side probe: disabled profiling does one cached
// enable check and the decoder's cut loop stays free of locks, clocks, and
// metric allocations.
fn record_checkpoint_observability(result: &weaver_yenc::DecodeResult) {
    if !crate::runtime::perf_probe::enabled() {
        return;
    }

    let shape = match &result.checkpoint_plan {
        weaver_yenc::CheckpointPlan::None => "download.checkpoint_plan.none",
        weaver_yenc::CheckpointPlan::Single(_) => "download.checkpoint_plan.single",
        weaver_yenc::CheckpointPlan::Multi(_) => "download.checkpoint_plan.multi",
    };
    crate::runtime::perf_probe::record_value(shape, 1);
    crate::runtime::perf_probe::record_value(
        "download.checkpoint_plan.grid_count",
        result.checkpoint_plan.grid_count() as u64,
    );
    crate::runtime::perf_probe::record_value(
        "download.checkpoint_segments.emitted",
        result.segments.len() as u64,
    );

    let Some(reason) = result.checkpoint_collapse_reason else {
        return;
    };
    let label = match reason {
        weaver_yenc::CheckpointCollapseReason::SegmentCount => {
            "download.checkpoint_collapse.segment_count"
        }
        weaver_yenc::CheckpointCollapseReason::SegmentStorage => {
            "download.checkpoint_collapse.segment_storage"
        }
        weaver_yenc::CheckpointCollapseReason::OfferWork => {
            "download.checkpoint_collapse.offer_work"
        }
        weaver_yenc::CheckpointCollapseReason::BoundaryOverflow => {
            "download.checkpoint_collapse.boundary_overflow"
        }
        weaver_yenc::CheckpointCollapseReason::MissingBoundary => {
            "download.checkpoint_collapse.missing_boundary"
        }
        weaver_yenc::CheckpointCollapseReason::NonAdvancingBoundary => {
            "download.checkpoint_collapse.nonadvancing_boundary"
        }
        weaver_yenc::CheckpointCollapseReason::OffsetOverflow => {
            "download.checkpoint_collapse.offset_overflow"
        }
    };
    crate::runtime::perf_probe::record_value(label, 1);
}

impl Pipeline {
    // When the timed waits behind a dispatch stall end, if every stalled job
    // has one.
    //
    // A stalled job is one dispatch would serve: eligible, with queued
    // download work. Its wait is the earliest future deadline among the
    // holds the scheduler keeps on it — a propagation deferral, a restart
    // durable-lead retry, a timed infrastructure retry of one of its
    // articles — or the soft byte-pressure delay every job shares. A job
    // whose articles are being built waits until now, so the stall counts
    // from now while that lasts. A stalled job with none of these is waiting
    // on nothing anyone scheduled, and the stall is unexplained: `None`.
    // Otherwise the earliest wait across the stalled jobs, which is when the
    // stall starts counting.
    //
    // Retries that sleep in their own task (a body-lane or blocked-server
    // recheck, a server quota wait) keep no deadline here, so they do not
    // explain a stall; their articles are not in the queue either.
    pub(in crate::pipeline) fn earliest_explained_dispatch_wait(
        &self,
        now: Instant,
    ) -> Option<Instant> {
        let stalled = self.download_scheduler_eligible_jobs();
        let waits = self.explained_dispatch_waits(now);
        let mut earliest: Option<Instant> = None;
        for job_id in stalled {
            let wait = *waits.get(&job_id)?;
            earliest = Some(earliest.map_or(wait, |current| current.min(wait)));
        }
        earliest
    }

    // A wait protects its own job even when another eligible job is wedged.
    fn explained_dispatch_waits(&self, now: Instant) -> HashMap<JobId, Instant> {
        let mut retry_deadlines: HashMap<JobId, Instant> = HashMap::new();
        for (deadline, retry) in self.infrastructure_retries.iter_with_deadlines() {
            let Some(deadline) = deadline.map(tokio::time::Instant::into_std) else {
                continue;
            };
            retry_deadlines
                .entry(retry.work.segment_id.file_id.job_id)
                .and_modify(|earliest| *earliest = (*earliest).min(deadline))
                .or_insert(deadline);
        }
        let mut waits = HashMap::new();
        for job_id in self.download_scheduler_eligible_jobs() {
            // A job whose articles are being built waits on that, which has
            // no deadline: the clock restarts while it lasts, as under a
            // quota, and a slow read never kicks the other jobs.
            if self.awaits_job_articles(job_id) {
                waits.insert(job_id, now);
                continue;
            }
            let job_wait = [
                self.propagation_ready_at
                    .get(&job_id)
                    .map(|(ready_at, _)| *ready_at),
                self.download_restart_durable_lead_retry_after
                    .get(&job_id)
                    .copied(),
                retry_deadlines.get(&job_id).copied(),
                self.download_pressure_soft_dispatch_after,
            ]
            .into_iter()
            .flatten()
            .filter(|deadline| *deadline > now)
            .min();
            if let Some(wait) = job_wait {
                waits.insert(job_id, wait);
            }
        }
        waits
    }

    // The servers a new download lane could be sent to right now, asked the
    // way the dispatch pass asks; 0 while the pool's health state is
    // contended, since that answer says nothing about them.
    fn dispatchable_server_count(&self) -> usize {
        self.nntp
            .blocking_body_server_order(&[])
            .map_or(0, |order| order.len())
    }

    // Times a dispatch stall — queued work, no download or connection open
    // at either end of a pass — and escalates one nothing explains.
    //
    // The stall ends when an article completes anywhere or no job is left
    // that dispatch would serve. A timed wait behind every stalled job (see
    // `earliest_explained_dispatch_wait`) holds the clock at the wait's
    // end; a download quota block or no dispatchable server restarts it.
    // Past `DOWNLOAD_DISPATCH_STALL_KICK_AFTER` the stall is kicked; past
    // `DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER` after the kick, every stalled
    // job whose bytes have not moved is released.
    //
    // Returns whether it kicked, so the pass runs again against what the
    // kick cleared. Does nothing when recovery is switched off.
    pub(in crate::pipeline::download::worker) fn watch_download_dispatch_stall(
        &mut self,
        now: Instant,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) -> bool {
        if !self.download_stall_recovery {
            return false;
        }
        if self.active_downloads != 0
            || self.active_download_connections != 0
            || eligible_count == 0
        {
            self.download_dispatch_stall = None;
            return false;
        }
        let segments_downloaded = self.metrics.segments_downloaded.load(Ordering::Relaxed);
        if self
            .download_dispatch_stall
            .as_ref()
            .is_none_or(|watch| watch.segments_downloaded != segments_downloaded)
        {
            self.download_dispatch_stall = Some(download::DispatchStallWatch {
                since: now,
                segments_downloaded,
                kick: None,
            });
        }
        if self.shared_state.server_quota_blocked()
            || self.shared_state.egress_quota_block().is_some()
        {
            // A quota is a wait the operator can see and lift; it has no
            // deadline, so the clock restarts whenever one holds.
            if let Some(watch) = self.download_dispatch_stall.as_mut() {
                watch.since = now;
                watch.kick = None;
            }
            return false;
        }
        // No server to dispatch to is an outage, not a wedge: the clock
        // restarts while every server is cooling down, disabled or
        // unroutable, and the kick window opens only once one is back. A
        // contended ranking says nothing about the servers and holds the
        // clock too. Accepted narrower case: while any server is available,
        // a stalled job whose articles only a cooled-down server holds still
        // escalates after the windows.
        if self.dispatchable_server_count() == 0 {
            if let Some(watch) = self.download_dispatch_stall.as_mut() {
                watch.since = now;
                watch.kick = None;
            }
            return false;
        }
        let Some(watch) = self.download_dispatch_stall.as_ref() else {
            return false;
        };
        let stalled_for = now.saturating_duration_since(watch.since);
        let escalation_due = match &watch.kick {
            None => stalled_for >= DOWNLOAD_DISPATCH_STALL_KICK_AFTER,
            Some((kicked_at, _)) => {
                now.saturating_duration_since(*kicked_at) >= DOWNLOAD_DISPATCH_STALL_RELEASE_AFTER
            }
        };
        if !escalation_due {
            return false;
        }
        // Asked only when an escalation is due: the walk over every stalled
        // job and parked retry is not paid on each stalled pass.
        if let Some(wait_ends) = self.earliest_explained_dispatch_wait(now) {
            if let Some(watch) = self.download_dispatch_stall.as_mut() {
                watch.since = watch.since.max(wait_ends);
                watch.kick = None;
            }
            return false;
        }
        match self
            .download_dispatch_stall
            .as_ref()
            .map(|watch| watch.kick.is_some())
        {
            Some(false) => {
                self.kick_download_dispatch_stall(
                    now,
                    stalled_for,
                    pressure,
                    max_connections,
                    eligible_count,
                );
                true
            }
            Some(true) => {
                let kicked = self
                    .download_dispatch_stall
                    .take()
                    .and_then(|watch| watch.kick)
                    .map(|(_, bytes)| bytes)
                    .unwrap_or_default();
                self.release_download_dispatch_stall(
                    now,
                    stalled_for,
                    kicked,
                    pressure,
                    max_connections,
                    eligible_count,
                );
                false
            }
            None => false,
        }
    }

    // Clears dispatch-side holds only for unexplained jobs: parked
    // infrastructure retries go back to their jobs' queues, the stalled
    // jobs' durable-lead and propagation deadlines, the rotation hints on
    // their queued articles and the soft-pressure delay are dropped. What
    // holds a server back (health cooldowns, quota)
    // lives in the NNTP pool and lifts on its own clock.
    fn kick_download_dispatch_stall(
        &mut self,
        now: Instant,
        stalled_for: Duration,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) {
        let waits = self.explained_dispatch_waits(now);
        let stalled = self
            .download_scheduler_eligible_jobs()
            .into_iter()
            .filter(|job_id| !waits.contains_key(job_id))
            .collect::<HashSet<_>>();
        let requeued_retries = self.wake_infrastructure_retries_for_jobs(&stalled);
        let mut rotation_hints_cleared = 0;
        for job_id in &stalled {
            self.download_restart_durable_lead_retry_after
                .remove(job_id);
            self.propagation_ready_at.remove(job_id);
            // A hint only says one attempt failed on a server; exclusions are
            // facts about the article and stay.
            if let Some(active) = self
                .jobs
                .get_mut(job_id)
                .and_then(|state| state.articles.active_mut())
            {
                rotation_hints_cleared += active.download_queue.clear_rotation_hints()
                    + active.recovery_queue.clear_rotation_hints();
            }
        }
        self.download_pressure_soft_dispatch_after = None;
        // The pass after the kick reports what is left, not the throttle.
        self.last_download_dispatch_stall_log_at = None;
        self.metrics
            .download_dispatch_stall_kicks
            .fetch_add(1, Ordering::Relaxed);
        let stalled = self
            .download_scheduler_eligible_jobs()
            .into_iter()
            .filter(|job_id| stalled.contains(job_id))
            .collect::<Vec<_>>();
        let downloaded_bytes = stalled
            .iter()
            .filter_map(|job_id| {
                self.jobs
                    .get(job_id)
                    .map(|state| (*job_id, state.downloaded_bytes))
            })
            .collect::<HashMap<_, _>>();
        self.log_download_dispatch_stall_recovery(
            "kick",
            &stalled,
            stalled_for,
            requeued_retries,
            rotation_hints_cleared,
            pressure,
            max_connections,
            eligible_count,
        );
        if let Some(watch) = self.download_dispatch_stall.as_mut() {
            watch.since = now;
            watch.kick = Some((now, downloaded_bytes));
        }
    }

    // Books every queued article of each job the kick did not move as work
    // no server can serve, so the job leaves its download phase with them
    // missing and its completion check decides what it becomes.
    fn release_download_dispatch_stall(
        &mut self,
        now: Instant,
        stalled_for: Duration,
        downloaded_bytes_at_kick: HashMap<JobId, u64>,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) {
        let waits = self.explained_dispatch_waits(now);
        let released = self
            .download_scheduler_eligible_jobs()
            .into_iter()
            .filter(|job_id| !waits.contains_key(job_id))
            // A job still waiting for its articles to be read is not stalled
            // in dispatch: it has nothing built to hand out yet.
            .filter(|job_id| {
                self.jobs
                    .get(job_id)
                    .is_some_and(|state| state.articles.active().is_some())
            })
            .filter(|job_id| {
                downloaded_bytes_at_kick
                    .get(job_id)
                    .zip(self.jobs.get(job_id))
                    .is_some_and(|(at_kick, state)| *at_kick == state.downloaded_bytes)
            })
            .collect::<Vec<_>>();
        if released.is_empty() {
            return;
        }
        self.metrics
            .download_dispatch_stall_releases
            .fetch_add(1, Ordering::Relaxed);
        self.log_download_dispatch_stall_recovery(
            "release",
            &released,
            stalled_for,
            0,
            0,
            pressure,
            max_connections,
            eligible_count,
        );
        for job_id in released {
            let Some(active) = self
                .jobs
                .get_mut(&job_id)
                .and_then(|state| state.articles.active_mut())
            else {
                continue;
            };
            let queued = active.download_queue.extract_matching(|_| true);
            for work in &queued {
                self.book_unservable_work(work);
            }
            self.update_queue_metrics();
            self.maybe_finish_download_pass(job_id);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn log_download_dispatch_stall_recovery(
        &self,
        recovery: &'static str,
        jobs: &[JobId],
        stalled_for: Duration,
        requeued_retries: usize,
        rotation_hints_cleared: usize,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) {
        let queue_depth = self
            .jobs
            .values()
            .map(|state| {
                state.articles.queued_download_len() + state.articles.queued_recovery_len()
            })
            .sum::<usize>();
        let hot_job_id = self.current_hot_job().map(|id| id.0).unwrap_or_default();
        for job_id in jobs {
            let Some(state) = self.jobs.get(job_id) else {
                continue;
            };
            let backlog = self.download_pipeline_backlog_for_job(*job_id);
            if recovery == "release" {
                error!(
                    recovery,
                    job_id = job_id.0,
                    status = ?state.status,
                    download_queue_len = state.articles.queued_download_len(),
                    recovery_queue_len = state.articles.queued_recovery_len(),
                    downloaded_bytes = state.downloaded_bytes,
                    stalled_secs = stalled_for.as_secs(),
                    eligible_count,
                    queue_depth,
                    active_downloads = self.active_downloads,
                    active_connections = self.active_download_connections,
                    max_connections,
                    pressure_state = pressure.state.as_str(),
                    pressure_reason = pressure.reason.as_str(),
                    hot_job_id,
                    delayed_retries_by_job = backlog.delayed_retries,
                    pending_decodes_by_job = backlog.pending_decodes,
                    "download dispatch stall: no bytes moved after the kick; booking the job's \
                     queued articles missing so its completion check can run"
                );
            } else {
                warn!(
                    recovery,
                    job_id = job_id.0,
                    status = ?state.status,
                    download_queue_len = state.articles.queued_download_len(),
                    recovery_queue_len = state.articles.queued_recovery_len(),
                    downloaded_bytes = state.downloaded_bytes,
                    stalled_secs = stalled_for.as_secs(),
                    requeued_retries,
                    rotation_hints_cleared,
                    eligible_count,
                    queue_depth,
                    active_downloads = self.active_downloads,
                    active_connections = self.active_download_connections,
                    max_connections,
                    pressure_state = pressure.state.as_str(),
                    pressure_reason = pressure.reason.as_str(),
                    hot_job_id,
                    delayed_retries_by_job = backlog.delayed_retries,
                    pending_decodes_by_job = backlog.pending_decodes,
                    "download dispatch stall: nothing explains it; clearing dispatch holds and \
                     dispatching again"
                );
            }
        }
    }

    pub(in crate::pipeline::download::worker) fn log_download_dispatch_liveness_stall(
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
            .map(|state| {
                state.articles.queued_download_len() + state.articles.queued_recovery_len()
            })
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
        let hot_job_id = self.current_hot_job().map(|id| id.0).unwrap_or_default();
        // How long until the timed waits behind the stall end; 0 when nothing
        // scheduled explains it, which is the stall the watchdog escalates.
        let explained_wait_ms = self
            .earliest_explained_dispatch_wait(now)
            .map(|ends| ends.saturating_duration_since(now).as_millis() as u64)
            .unwrap_or_default();
        let stall_recovery = self.download_stall_recovery;
        let servers_available = self.dispatchable_server_count();
        if let Some(job_id) = self.job_order.iter().copied().find(|job_id| {
            self.jobs.get(job_id).is_some_and(|state| {
                state.articles.has_queued_download_work()
                    || state.articles.queued_recovery_len() > 0
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
                download_queue_len = state.articles.queued_download_len(),
                recovery_queue_len = state.articles.queued_recovery_len(),
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
                explained_wait_ms,
                servers_available,
                stall_recovery,
                active_downloads_by_job = backlog.active_downloads,
                active_connections_by_job = backlog.active_connections,
                active_decodes_by_job = backlog.active_decodes,
                delayed_retries_by_job = backlog.delayed_retries,
                released_results_by_job = backlog.released_results,
                released_result_bytes_by_job = backlog.released_result_bytes,
                pending_decodes_by_job = backlog.pending_decodes,
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
                explained_wait_ms,
                servers_available,
                stall_recovery,
                "download dispatch liveness stall: queued work but no active downloads"
            );
        }
    }

    // Reports a pipeline that is retrying hard and finishing nothing.
    //
    // The liveness stall above only speaks when nothing is in flight, so a
    // loop that keeps a lane busy re-fetching the same articles is invisible
    // to it: connections are up, downloads are active, and the only sign is
    // a retry counter climbing without a download counter to match.
    pub(in crate::pipeline::download::worker) fn log_download_retry_storm(&mut self, now: Instant) {
        let retried = self.metrics.segments_retried.load(Ordering::Relaxed);
        let downloaded = self.metrics.segments_downloaded.load(Ordering::Relaxed);
        let Some((opened_at, retried_at_open, downloaded_at_open)) =
            self.download_retry_storm_window
        else {
            self.download_retry_storm_window = Some((now, retried, downloaded));
            return;
        };
        if now.saturating_duration_since(opened_at) < DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL {
            return;
        }
        let retries = retried.saturating_sub(retried_at_open);
        let downloads = downloaded.saturating_sub(downloaded_at_open);
        self.download_retry_storm_window = Some((now, retried, downloaded));
        if downloads != 0 || retries < DOWNLOAD_RETRY_STORM_THRESHOLD {
            return;
        }
        warn!(
            retries,
            window_secs = DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL.as_secs(),
            active_downloads = self.active_downloads,
            active_connections = self.active_download_connections,
            articles_not_found_without_new_server = self
                .metrics
                .articles_not_found_without_new_server
                .load(Ordering::Relaxed),
            "download retry storm: articles are being retried without any completing"
        );
    }

    // Reports lanes that are not being filled: work is queued, dispatch is
    // running, and yet a server has been carrying fewer connections than it is
    // configured for throughout the window.
    //
    // The liveness stall above only fires when there are **no** active
    // downloads at all, so the shape that hides here is the partial one — a
    // job running on two of its eight lanes because the other six fail to
    // open, which is indistinguishable from a slow server in every metric the
    // job exposes.
    //
    // Lanes sit below their cap for many ordinary reasons — a download quota,
    // byte pressure, a hot job whose spillover is blocked, the tail of a job
    // with fewer articles left than lanes — so an underfill alone says
    // nothing. It is reported only when a lane actually failed to open inside
    // the window, which is the one cause this line exists to name.
    pub(in crate::pipeline) fn log_download_lanes_under_cap(
        &mut self,
        now: Instant,
        max_connections: usize,
    ) {
        let queue_depth = self
            .jobs
            .values()
            .map(|state| {
                state.articles.queued_download_len() + state.articles.queued_recovery_len()
            })
            .sum::<usize>();
        // Nothing to fill the lanes with, or the lanes are full: either way
        // this is not the condition, and the window restarts from here.
        if queue_depth == 0 || self.active_download_connections >= max_connections {
            self.download_lanes_under_cap_since = None;
            return;
        }
        let since = *self.download_lanes_under_cap_since.get_or_insert(now);
        if now.saturating_duration_since(since) < DOWNLOAD_LANES_UNDER_CAP_WINDOW {
            return;
        }
        if !self
            .last_owned_lane_acquire_failure_at
            .is_some_and(|failed_at| failed_at >= since)
        {
            return;
        }
        if self
            .last_download_lanes_under_cap_log_at
            .is_some_and(|last| {
                now.saturating_duration_since(last) < DOWNLOAD_LANES_UNDER_CAP_LOG_INTERVAL
            })
        {
            return;
        }
        self.last_download_lanes_under_cap_log_at = Some(now);
        let pool = self.nntp.pool();
        let per_server: Vec<(usize, usize, usize)> = (0..pool.server_count())
            .map(|idx| {
                let (available, configured) = pool.server_load(idx);
                (idx, configured.saturating_sub(available), configured)
            })
            .collect();
        info!(
            queue_depth,
            active_connections = self.active_download_connections,
            active_downloads = self.active_downloads,
            max_connections,
            under_cap_for_ms = now.saturating_duration_since(since).as_millis() as u64,
            per_server_active_of_configured = ?per_server,
            "download lanes have stayed below their connection cap with work queued"
        );
    }

    // Update shared atomic queue depth metrics from per-job queues, and the
    // server quota block that reads them.
    pub(crate) fn update_queue_metrics(&self) {
        let (total, recovery) = self.jobs.values().fold((0usize, 0usize), |(t, r), s| {
            (
                t + s.articles.queued_download_len() + s.articles.queued_recovery_len(),
                r + s.articles.queued_recovery_len(),
            )
        });
        self.metrics
            .download_queue_depth
            .store(total, Ordering::Relaxed);
        self.metrics
            .recovery_queue_depth
            .store(recovery, Ordering::Relaxed);
        self.publish_active_stage_metrics();
        self.refresh_server_quota_block_presentation();
    }

    // Payload delivered and the wall time it took, with deliberate throttle
    // waits already excluded by the lane. Zero for anything but a decoded
    // article, so a failure cannot look like free bytes.
    pub(in crate::pipeline::download) fn decoded_trace_throughput_sample(
        trace: &weaver_nntp::client::DecodedBodyTrace,
    ) -> (u64, Duration) {
        let Ok(decoded) = trace.result.as_ref() else {
            return (0, Duration::ZERO);
        };
        let elapsed = trace
            .attempts
            .last()
            .map_or(Duration::ZERO, |attempt| attempt.elapsed);
        (decoded.io.decoded_bytes_written, elapsed)
    }

    pub(in crate::pipeline) fn download_data_from_decoded_trace(
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
                    body,
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
                    "download.nntp.transport.backend.recv_calls",
                    io.transport_read.backend_recv_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.recv_bytes",
                    io.transport_read.backend_recv_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.target_full_returns",
                    io.transport_read.backend_target_full_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.pending_empty_returns",
                    io.transport_read.backend_pending_empty_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.pending_after_bytes_returns",
                    io.transport_read.backend_pending_after_bytes_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.zero_returns",
                    io.transport_read.backend_zero_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.bytes_per_recv_call",
                    io.transport_read
                        .backend_recv_bytes
                        .checked_div(io.transport_read.backend_recv_calls)
                        .unwrap_or(0),
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.read.bytes_per_call",
                    io.read_bytes.checked_div(io.read_calls).unwrap_or(0),
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
                let data = {
                    let _cpu =
                        crate::runtime::perf_probe::cpu_scope("download.inline_decode.into_chunk");
                    DecodedChunk::from(decoded)
                };

                Ok(DownloadPayload::Decoded(match body {
                    weaver_nntp::fused_yenc::FusedArticleBody::Yenc(result) => {
                        record_checkpoint_observability(&result);
                        // Block evidence needs a known starting offset. An
                        // article whose own begin was unusable has none.
                        let offset_known = result.metadata.file_offset_is_known();
                        let truncation_suspected =
                            crate::pipeline::yenc_truncation_suspected(&result);
                        let yenc_layout = YencLayoutAssertions {
                            file_size: result.metadata.size,
                            part: result.metadata.part,
                            total: result.metadata.total,
                            begin: result.metadata.begin,
                            end: result.metadata.end,
                        };

                        DecodeResult {
                            segment_id,
                            raw_size: raw_size as u64,
                            encoding: SegmentEncoding::Yenc,
                            yenc_layout,
                            crc_valid: crate::pipeline::crc_not_mismatched(result.crc_status),
                            truncation_suspected,
                            part_crc_verified: result.crc_status
                                == weaver_yenc::CrcVerification::Verified,
                            part_crc: result.part_crc,
                            expected_file_crc: result.expected_file_crc,
                            data,
                            yenc_name: result.metadata.name,
                            checkpoint_plan: result.checkpoint_plan,
                            segments: if offset_known {
                                result.segments
                            } else {
                                Vec::new()
                            },
                        }
                    }
                    // uuencode declares no offsets, no size and no checksum, so
                    // every field that would carry one is left at its "nothing
                    // to say" value. In particular `segments` is empty: the
                    // dual-CRC grid can only be fed block-aligned CRC evidence,
                    // and there is none to give it here.
                    weaver_nntp::fused_yenc::FusedArticleBody::Uu(outcome) => {
                        let facts = crate::pipeline::UuSegmentFacts {
                            damaged: outcome.damaged,
                            ended: outcome.ended,
                        };
                        DecodeResult {
                            segment_id,
                            raw_size: raw_size as u64,
                            encoding: SegmentEncoding::Uu(facts),
                            yenc_layout: YencLayoutAssertions {
                                file_size: 0,
                                part: None,
                                total: None,
                                begin: None,
                                end: None,
                            },
                            crc_valid: facts.clean(),
                            truncation_suspected: false,
                            part_crc_verified: false,
                            part_crc: 0,
                            expected_file_crc: None,
                            data,
                            yenc_name: outcome.filename.unwrap_or_default(),
                            checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                            segments: Vec::new(),
                        }
                    }
                }))
            }
            Err(weaver_nntp::client::DecodedBodyError::Nntp(error)) => {
                Err(DownloadError::from_nntp(error))
            }
            Err(weaver_nntp::client::DecodedBodyError::Decode { raw_size, error }) => {
                Err(DownloadError::Decode {
                    raw_size: raw_size as u64,
                    error: error.to_string(),
                })
            }
        };

        (data, trace.attempts, source_server_idx)
    }
}
