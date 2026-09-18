//! A live lane asking for its next articles.
//!
//! The worker owns the socket and the ring; the actor owns the queues. When
//! a lane's pending tail runs low it sends one [`DownloadLaneRefillRequest`]
//! and keeps reading the ring while the answer is on its way. The answer is
//! one handout from the article scheduler — one job, at most `want` articles,
//! chosen by the hot-job rule in `download/scheduler.rs` — booked onto the
//! lane and sent back as a [`DownloadBatchLease`].
//!
//! A lane the scheduler has nothing for is not sent away at once. Idle is
//! usually momentary — the hot job's next article is a decode away from being
//! admitted, a spill job is about to be promoted — and an established
//! connection is worth more than the round trip that re-dials it. The
//! request is held in the actor and answered again on the next dispatch wake,
//! for at most [`DOWNLOAD_REFILL_IDLE_HOLD`]; after that the lane parks on
//! `NoWork`, keeps its socket cached, and the pool's idle loop takes over.

use super::*;
use crate::pipeline::download::scheduler::{Handout, YieldReason};

/// How long a refill that found nothing waits in the actor before its lane is
/// told to park. A fixed window: long enough to ride out a decode or a
/// promotion, short enough that a truly idle lane frees its permit for a
/// probe or another server's dial.
pub(in crate::pipeline) const DOWNLOAD_REFILL_IDLE_HOLD: Duration = Duration::from_secs(2);

/// A refill the scheduler could not fill yet, waiting for a wake.
pub(crate) struct HeldDownloadRefill {
    request: DownloadLaneRefillRequest,
    since: Instant,
}

impl Pipeline {
    /// How many articles one refill hands a lane running at `depth`.
    ///
    /// The worker asks again once its pending tail falls to `2 * depth + 1`
    /// (see `refill_deadline` beside the ring), so one more than that keeps
    /// the cadence at one refill per ring turn without pre-leasing articles
    /// that other lanes of the same job could be fetching now.
    pub(in crate::pipeline::download::worker) fn download_refill_want(
        &self,
        lane_mode: DownloadLaneMode,
    ) -> usize {
        // A limited link activates its reservations after the lease is
        // finalized; single-article leases let every refill see the updated
        // token balance instead of pre-leasing past it.
        if self.rate_limiter.is_limited() {
            return 1;
        }
        2 * lane_mode.max_depth().max(1) + 2
    }

    /// The depth a lane on `server_idx` should run at for its next batch.
    pub(in crate::pipeline::download::worker) fn download_lane_mode_for_server(
        &self,
        server_idx: usize,
        pressure: DownloadPressure,
        supports_pipelining: bool,
    ) -> DownloadLaneMode {
        if !supports_pipelining {
            return DownloadLaneMode::Sequential;
        }
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        self.download_lane_runtime
            .servers
            .get(&server_idx)
            .map(|explorer| explorer.choose_mode(pressure_clear))
            .unwrap_or(DownloadLaneMode::Sequential)
    }

    /// The job, other than the hot one, that already has articles out on
    /// `server_idx` — the one a spill must go back to rather than fan out
    /// from. When more than one such job is in flight (the hot job changed
    /// underneath them), the earliest in dispatch order is the one named.
    pub(in crate::pipeline) fn spill_job_in_flight_on(&self, server_idx: usize) -> Option<JobId> {
        let hot = self.current_hot_job();
        let mut chosen: Option<(usize, JobId)> = None;
        for owner in self.download_lane_owners.values() {
            if owner.server_idx != Some(server_idx)
                || owner.outstanding.is_empty()
                || Some(owner.job_id) == hot
            {
                continue;
            }
            if chosen.is_some_and(|(_, job_id)| job_id == owner.job_id) {
                continue;
            }
            let Some(index) = self.job_order.iter().position(|id| *id == owner.job_id) else {
                continue;
            };
            if chosen.is_none_or(|(best, _)| index < best) {
                chosen = Some((index, owner.job_id));
            }
        }
        chosen.map(|(_, job_id)| job_id)
    }

    pub(crate) fn handle_download_lane_refill_request(
        &mut self,
        request: DownloadLaneRefillRequest,
    ) {
        self.answer_download_lane_refill(request, Instant::now(), None);
    }

    /// Re-run every held refill against the queues. Called at the top of a
    /// dispatch pass, which is where a wake lands, and from the periodic tick
    /// so a hold expires even when nothing else wakes the actor.
    pub(in crate::pipeline) fn service_held_download_refills(&mut self) {
        if self.held_download_refills.is_empty() {
            return;
        }
        let now = Instant::now();
        let held = std::mem::take(&mut self.held_download_refills);
        for HeldDownloadRefill { request, since } in held {
            self.answer_download_lane_refill(request, now, Some(since));
        }
    }

    /// Every held refill is parked now. Used when the pool is reset and the
    /// lanes those requests came from are gone.
    pub(in crate::pipeline) fn drop_held_download_refills(&mut self) {
        for HeldDownloadRefill { request, .. } in std::mem::take(&mut self.held_download_refills) {
            let _ = request.response_tx.send(DownloadLaneRefillResponse {
                lease: None,
                park_reason: LaneParkReason::Error,
            });
        }
    }

    fn answer_download_lane_refill(
        &mut self,
        request: DownloadLaneRefillRequest,
        now: Instant,
        held_since: Option<Instant>,
    ) {
        if request.response_tx.is_closed() {
            // The worker stopped waiting (an async lane's grace ran out, or
            // the lane died); nothing to answer and nothing was booked.
            return;
        }
        let lane_id = request.lane_id;
        let server_idx = request.server_idx;
        if request.runtime_generation != self.pool_generation
            || !self.download_lane_is_live(lane_id)
        {
            let _ = request.response_tx.send(DownloadLaneRefillResponse {
                lease: None,
                park_reason: LaneParkReason::Error,
            });
            return;
        }

        let mut park_reason = None;
        if request.remote_ip.is_some_and(|ip| {
            self.ip_replacement_retired_ips
                .contains(&ServerIpKey { server_idx, ip })
        }) {
            park_reason = Some(LaneParkReason::IpReplacementRetired);
        }
        if park_reason.is_none()
            && let Err(error) = self.refresh_bandwidth_cap_window()
        {
            error!(error = %error, "failed to refresh ISP bandwidth cap state for lane refill");
            park_reason = Some(LaneParkReason::Error);
        }
        if let Some(reason) = park_reason {
            self.park_download_lane_refill(request, reason, held_since, now);
            return;
        }

        let pressure = self.refresh_download_pressure();
        let lane_mode = self.download_lane_mode_for_server(
            server_idx,
            Self::refill_mode_pressure(pressure),
            request.supports_pipelining,
        );
        let want = self.download_refill_want(lane_mode);
        let spill_in_flight = self.spill_job_in_flight_on(server_idx);
        let works = match self.next_works(server_idx, want, spill_in_flight, pressure) {
            Handout::Works(works) => works,
            Handout::Idle => {
                self.hold_or_park_idle_download_lane_refill(request, pressure, held_since, now);
                return;
            }
            Handout::Yield(reason) => {
                let park = match reason {
                    // Hard pressure parks the lane instead of holding the
                    // request: a lane blocked on its refill keeps its socket
                    // and its pool permit for as long as the backlog takes to
                    // drain, and nothing bounds that wait. Parking releases
                    // the permit; the wake once pressure clears redispatches.
                    YieldReason::HardPressure => {
                        self.metrics
                            .download_lane_refill_deferred_total
                            .fetch_add(1, Ordering::Relaxed);
                        LaneParkReason::Pressure
                    }
                    YieldReason::BandwidthCapExhausted => LaneParkReason::Pressure,
                    YieldReason::Paused
                    | YieldReason::RateLimited
                    | YieldReason::HandoffDraining => LaneParkReason::ProbeYield,
                };
                self.park_download_lane_refill(request, park, held_since, now);
                return;
            }
        };

        let Some(lease) = self.lease_for_handout(lane_id, server_idx, lane_mode, pressure, works)
        else {
            // The first article the scheduler handed out was refused by a
            // reservation (durable lead, ISP cap) and is back in the queue.
            // That clears the way it clears for an idle answer: on a wake.
            self.hold_or_park_idle_download_lane_refill(request, pressure, held_since, now);
            return;
        };

        let DownloadLaneRefillRequest {
            current_mode,
            response_tx,
            ..
        } = request;
        let job_id = lease.job_id;
        let completion_critical = lease.completion_critical;
        let activation_items = Self::activation_items(&lease);
        let progress_article = self.checkpoint_progress_article_for_lease(&lease);
        let work_count = lease.works.len();
        let recovery_count = lease.works.iter().filter(|work| work.is_recovery).count();
        let booked_works = lease.works.clone();
        match response_tx.send(DownloadLaneRefillResponse {
            lease: Some(lease),
            park_reason: LaneParkReason::NoWork,
        }) {
            Ok(()) => {
                self.rebook_download_lane_owner(
                    lane_id,
                    job_id,
                    server_idx,
                    lane_mode,
                    completion_critical,
                    booked_works,
                );
                if let Some(segment_id) = progress_article {
                    self.checkpoint_progress_articles
                        .insert(job_id, (lane_id, segment_id));
                }
                self.metrics
                    .download_lane_refill_granted_total
                    .fetch_add(1, Ordering::Relaxed);
                self.activate_download_batch(
                    job_id,
                    recovery_count,
                    completion_critical,
                    lane_mode,
                    work_count,
                    &activation_items,
                    false,
                );
                self.note_download_lane_mode_changed(current_mode, lane_mode);
            }
            Err(response) => {
                if let Some(lease) = response.lease {
                    self.rollback_download_batch_lease(lease);
                }
            }
        }
        self.update_queue_metrics();
    }

    /// Turn a scheduler handout into a lease for `lane_id`: reserve each
    /// article's bandwidth and durable lead, and return whatever could not be
    /// reserved to its queue. `None` when nothing survived.
    pub(in crate::pipeline) fn lease_for_handout(
        &mut self,
        lane_id: u64,
        server_idx: usize,
        lane_mode: DownloadLaneMode,
        pressure: DownloadPressure,
        works: Vec<DownloadWork>,
    ) -> Option<DownloadBatchLease> {
        let job_id = works.first()?.segment_id.file_id.job_id;
        let mut reserved = Vec::with_capacity(works.len());
        let mut works = works.into_iter();
        for work in works.by_ref() {
            match self.reserve_download_work_for_dispatch(job_id, work, false) {
                Ok(Some(work)) => reserved.push(work),
                // The helper has already returned the refused article; the
                // rest of the handout follows it below so the batch keeps
                // the scheduler's order.
                Ok(None) | Err(_) => break,
            }
        }
        let mut returned = 0;
        if let Some(state) = self.jobs.get_mut(&job_id) {
            for work in works {
                state.download_queue.push(work);
                returned += 1;
            }
        }
        if returned > 0 {
            self.update_queue_metrics();
        }
        if reserved.is_empty() {
            return None;
        }
        let completion_critical = reserved.iter().any(|work| work.completion_critical);
        let dial_exclude_servers = (0..self.nntp.pool().server_count())
            .filter(|idx| *idx != server_idx)
            .collect();
        Some(DownloadBatchLease {
            lane_id,
            job_id,
            runtime_generation: self.pool_generation,
            lane_mode,
            server_modes: vec![(server_idx, lane_mode)],
            completion_critical,
            effective_exclude_servers: self.effective_exclude_servers(job_id, &[]),
            dial_exclude_servers,
            checkpoint_plan: self.par2_checkpoint_plan(job_id),
            pressure_clear: pressure.state == DownloadPressureState::Clear,
            works: reserved,
        })
    }

    /// A refill nothing could be cut for right now. The lane keeps its
    /// socket and waits in the actor for the next wake — unless another
    /// server could serve queued work now, in which case the slot this lane
    /// is holding is worth more to a dial there than to a wait here, or the
    /// hold has already run its course.
    fn hold_or_park_idle_download_lane_refill(
        &mut self,
        request: DownloadLaneRefillRequest,
        pressure: DownloadPressure,
        held_since: Option<Instant>,
        now: Instant,
    ) {
        let since = held_since.unwrap_or(now);
        let hold_expired = now.saturating_duration_since(since) >= DOWNLOAD_REFILL_IDLE_HOLD;
        if hold_expired || self.servable_work_on_other_server(request.server_idx, pressure) {
            self.park_download_lane_refill(request, LaneParkReason::NoWork, held_since, now);
            return;
        }
        self.held_download_refills
            .push(HeldDownloadRefill { request, since });
    }

    /// Cut one lease for `server_idx` the way a dispatch pass would: one
    /// scheduler handout, reserved and pinned to that server. `None` when the
    /// scheduler had nothing for the server or nothing survived reservation.
    #[cfg(test)]
    pub(in crate::pipeline) fn lease_for_server_for_test(
        &mut self,
        server_idx: usize,
    ) -> Option<DownloadBatchLease> {
        let pressure = self.refresh_download_pressure();
        let lane_mode = self.download_lane_mode_for_server(server_idx, pressure, true);
        let want = self.download_refill_want(lane_mode);
        let spill_in_flight = self.spill_job_in_flight_on(server_idx);
        let Handout::Works(works) = self.next_works(server_idx, want, spill_in_flight, pressure)
        else {
            return None;
        };
        self.lease_for_handout(
            Self::next_download_lane_id(),
            server_idx,
            lane_mode,
            pressure,
            works,
        )
    }

    fn park_download_lane_refill(
        &mut self,
        request: DownloadLaneRefillRequest,
        reason: LaneParkReason,
        held_since: Option<Instant>,
        now: Instant,
    ) {
        self.metrics
            .download_lane_refill_parked_total
            .fetch_add(1, Ordering::Relaxed);
        // A park is a lost round trip on an established connection, so say
        // why. The counters alone could not tell a link with nothing queued
        // from one holding thousands of articles this server was refused.
        debug!(
            server = request.server_idx,
            lane_mode = ?request.current_mode,
            reason = ?reason,
            held_ms = held_since.map(|since| now.saturating_duration_since(since).as_millis() as u64),
            queued = self
                .jobs
                .values()
                .map(|state| state.download_queue.len())
                .sum::<usize>(),
            "download lane refill denied; lane parking"
        );
        let _ = request.response_tx.send(DownloadLaneRefillResponse {
            lease: None,
            park_reason: reason,
        });
        self.update_queue_metrics();
    }
}
