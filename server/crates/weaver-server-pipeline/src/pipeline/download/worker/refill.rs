// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A live lane asking for its next articles.
//
// The worker owns the socket and the ring; the actor owns the queues. When
// a lane's pending tail runs low it sends one [`DownloadLaneRefillRequest`]
// and keeps reading the ring while the answer is on its way. The answer is
// one handout from the article scheduler — one job, at most `want` articles,
// chosen by the hot-job rule in `download/scheduler.rs` — booked onto the
// lane and sent back as a [`DownloadBatchLease`].
//
// A lane the scheduler has nothing for is not sent away at once. Idle is
// usually momentary — the hot job's next article is a decode away from being
// admitted, a spill job is about to be promoted — and an established
// connection is worth more than the round trip that re-dials it. The
// request is held in the actor and answered again on the next dispatch wake,
// for at most [`DOWNLOAD_REFILL_IDLE_HOLD`]; after that the lane parks on
// `NoWork`, keeps its socket cached, and the pool's idle loop takes over.

use super::*;
use crate::pipeline::download::scheduler::{Handout, LaneShare, SaturationWake, YieldReason};

// How long a refill that found nothing waits in the actor before its lane is
// told to park. A fixed window: long enough to ride out a decode or a
// promotion, short enough that a truly idle lane frees its permit for a
// probe or another server's dial.
pub(in crate::pipeline) const DOWNLOAD_REFILL_IDLE_HOLD: Duration = Duration::from_secs(2);

// A refill the scheduler could not fill yet, waiting for a wake.
pub(crate) struct HeldDownloadRefill {
    request: DownloadLaneRefillRequest,
    since: Instant,
    // Set for a lane that was saturated rather than out of work: the request
    // is answered again once the lane has fetched down to the wake's count.
    // Such a lane is fetching, not idle, so its hold has no deadline.
    wake: Option<SaturationWake>,
    // For a lane held for its volume's room: since when it has held
    // nothing. From then on it is an idle lane, held by the idle rules
    // (`idle_hold_stands`), with its volume's line as one more wake.
    idle_since: Option<Instant>,
    // An idle lane held while a volume held its job's budget
    // (`Pipeline::volume_holds_budget`): counted as held for room, and as
    // woken when it is answered once the volumes have room.
    for_room: bool,
}

impl Pipeline {
    // How many articles one refill hands a lane running at `depth`.
    //
    // The worker asks again once its pending tail falls to `2 * depth + 1`
    // (see `refill_deadline` beside the ring), so one more than that keeps
    // the cadence at one refill per ring turn without pre-leasing articles
    // that other lanes of the same job could be fetching now.
    pub(in crate::pipeline) fn download_refill_want(&self, lane_mode: DownloadLaneMode) -> usize {
        // A limited link activates its reservations after the lease is
        // finalized; single-article leases let every refill see the updated
        // token balance instead of pre-leasing past it.
        if self.rate_limiter.is_limited() {
            return 1;
        }
        2 * lane_mode.max_depth().max(1) + 2
    }

    // The depth the lanes on `server_idx` run at, whichever lane asks: on a
    // pin's trial, the depth of the lanes that are not running it.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub(in crate::pipeline) fn download_lane_mode_for_server(
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

    // The depth `lane_id`, a lane on `server_idx`, should run at for its
    // next batch. It differs from the server's depth only while the server
    // is on the trial a lapsed pin gets, which one lane alone runs: see
    // `ServerPipelineExplorer::choose_mode_for_lane`. One map lookup more
    // than the server's mode, and only during a trial.
    pub(in crate::pipeline) fn download_lane_mode_for_lane(
        &mut self,
        server_idx: usize,
        pressure: DownloadPressure,
        supports_pipelining: bool,
        lane_id: u64,
    ) -> DownloadLaneMode {
        if !supports_pipelining {
            return DownloadLaneMode::Sequential;
        }
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        let owners = &self.download_lane_owners;
        self.download_lane_runtime
            .servers
            .get_mut(&server_idx)
            .map(|explorer| {
                explorer.choose_mode_for_lane(pressure_clear, lane_id, |holder| {
                    owners.contains_key(&holder)
                })
            })
            .unwrap_or(DownloadLaneMode::Sequential)
    }

    // The job, other than the hot one, that already has articles out on
    // `server_idx` — the one a spill must go back to rather than fan out
    // from. When more than one such job is in flight (the hot job changed
    // underneath them), the earliest in dispatch order is the one named.
    pub(in crate::pipeline) fn spill_job_in_flight_on(&self, server_idx: usize) -> Option<JobId> {
        let hot = self.current_hot_job();
        let mut owners: Vec<JobId> = self
            .download_lane_owners
            .values()
            .filter(|owner| {
                owner.server_idx == Some(server_idx)
                    && !owner.outstanding.is_empty()
                    && Some(owner.job_id) != hot
            })
            .map(|owner| owner.job_id)
            .collect();
        owners.sort_unstable();
        owners.dedup();
        // The owner first in dispatch order, ranked as the hot job is, each
        // found in the queue by its index.
        self.shared_state.queue.read(|inner| {
            owners
                .into_iter()
                .filter_map(|job_id| {
                    let state = self.jobs.get(&job_id)?;
                    let position = inner.dispatch_position(job_id, &self.job_order)?;
                    Some(crate::pipeline::queue::order::DispatchKey::new(
                        Self::job_dispatch_priority(state),
                        position,
                        job_id,
                    ))
                })
                .min()
                .map(|key| key.job_id())
        })
    }

    pub(crate) fn handle_download_lane_refill_request(
        &mut self,
        request: DownloadLaneRefillRequest,
    ) {
        self.answer_download_lane_refill(request, Instant::now(), None);
        #[cfg(test)]
        self.audit_lane_holdings();
    }

    // Re-run every held refill against the queues. Called at the top of a
    // dispatch pass, which is where a wake lands, and from the periodic tick
    // so a hold expires even when nothing else wakes the actor.
    pub(in crate::pipeline) fn service_held_download_refills(&mut self) {
        if self.held_download_refills.is_empty() {
            return;
        }
        let now = Instant::now();
        // Whether every idle hold would hear the same answer again. Asked
        // once for the pass, and only when a hold is idle.
        let volumes_refuse = self
            .held_download_refills
            .iter()
            .any(|held| held.wake.is_none())
            && self.volumes_hold_every_job(false);
        // Whether every job waits for a volume's room, asked once for the
        // pass and only when a lane held for room holds nothing.
        let mut every_job_waits_for_room = None;
        let held = std::mem::take(&mut self.held_download_refills);
        for HeldDownloadRefill {
            request,
            since,
            wake,
            mut idle_since,
            for_room,
        } in held
        {
            let Some(SaturationWake { of, below, room }) = wake else {
                if volumes_refuse && self.idle_hold_stands(&request, since, now) {
                    self.held_download_refills.push(HeldDownloadRefill {
                        request,
                        since,
                        wake,
                        idle_since,
                        for_room,
                    });
                    continue;
                }
                if for_room && !volumes_refuse {
                    self.metrics
                        .download_lane_room_woken_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.answer_download_lane_refill(request, now, Some(since));
                continue;
            };
            // A lane held for its volume's room is not asked again while
            // the volume is above its resume line: its wake at that line is
            // asked for again (a load, when it stands) and rings dispatch
            // when the volume falls there. While it still holds articles it
            // is fetching and stands. Once it holds nothing it is an idle
            // lane like any other: it stands only while every job waits for
            // a volume and the idle rules let it, so a probe, a pool change,
            // the idle window or a job with work elsewhere answers it. A job
            // that is gone, or writes to no sized volume, holds nothing.
            if room {
                let job_id = of.expect("a room wake names its job");
                let above = self.ask_room_wake(job_id);
                let stands = !request.response_tx.is_closed()
                    && above
                    && (self.download_lane_holdings(request.lane_id) > 0 || {
                        let idle = *idle_since.get_or_insert(now);
                        *every_job_waits_for_room
                            .get_or_insert_with(|| self.volumes_hold_every_job(true))
                            && self.idle_hold_stands(&request, idle, now)
                    });
                if stands {
                    self.held_download_refills.push(HeldDownloadRefill {
                        request,
                        since,
                        wake,
                        idle_since,
                        for_room: false,
                    });
                    continue;
                }
                if !above {
                    self.metrics
                        .download_lane_room_woken_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.answer_download_lane_refill(request, now, idle_since);
                continue;
            }
            // A lane still saturated costs one owner lookup per pass, not a
            // walk of the queues. Only the articles the wake names count, so
            // a lane at its share of one job is not held past that by the
            // tail of another it is still carrying. A lane that is gone
            // falls through and is told so by the answer path.
            let holding = match of {
                Some(job_id) => self.download_lane_holdings_of_job(request.lane_id, job_id),
                None => self.download_lane_holdings(request.lane_id),
            };
            if !request.response_tx.is_closed() && holding >= below {
                self.held_download_refills.push(HeldDownloadRefill {
                    request,
                    since,
                    wake,
                    idle_since,
                    for_room: false,
                });
                continue;
            }
            // Whatever this answer finds, the lane was busy until now: an
            // idle hold that starts here starts its own window.
            self.answer_download_lane_refill(request, now, None);
        }
    }

    // Whether no job can be handed an article until a volume has room:
    // the link is open, and each job dispatch could draw from writes to a
    // volume that holds its hard limit of unwritten bytes, or, with
    // `room`, one above its resume line, where a lane holding nothing was
    // held for room. Every such lane's ask is answered the same way then,
    // whichever job it is put to, and the wake each such volume was asked
    // for here wakes dispatch when the answer can change. Until then a pass
    // asks the volumes once, not every job for every held lane. A job on a
    // volume with room answers false, and the held lanes are asked again.
    fn volumes_hold_every_job(&mut self, room: bool) -> bool {
        let pressure = self.refresh_download_pressure();
        if self.download_scheduler_link_gate(pressure).is_some() {
            return false;
        }
        let eligible = self.download_scheduler_eligible_jobs();
        // Every job is asked, so every stopped volume has its wake.
        let mut all = !eligible.is_empty();
        for job_id in &eligible {
            all &= self.volume_refuses_job(*job_id) || (room && self.ask_room_wake(*job_id));
        }
        all
    }

    // Whether an idle hold is left as it is while the volumes refuse every
    // job: its ask would be put to the scheduler, be refused there and be
    // held again. A hold whose lane is gone, whose window ran out, or whose
    // lane a probe batch waits for is answered as on any pass.
    fn idle_hold_stands(
        &self,
        request: &DownloadLaneRefillRequest,
        since: Instant,
        now: Instant,
    ) -> bool {
        !request.response_tx.is_closed()
            && request.runtime_generation == self.pool_generation
            && self.download_lane_is_live(request.lane_id)
            && now.saturating_duration_since(since) < DOWNLOAD_REFILL_IDLE_HOLD
            && !self.probe_waits_for_seat_on(request.server_idx)
    }

    // Articles `lane_id` has been handed and not yet produced a result for.
    pub(in crate::pipeline) fn download_lane_holdings(&self, lane_id: u64) -> usize {
        self.download_lane_owners
            .get(&lane_id)
            .map_or(0, |owner| owner.outstanding.len())
    }

    // [`Self::download_lane_holdings`], counting only `job_id`'s articles.
    // The articles a lane holds, of every job, and their estimated bytes.
    pub(in crate::pipeline) fn download_lane_held_bytes(&self, lane_id: u64) -> (usize, u64) {
        self.download_lane_owners
            .get(&lane_id)
            .map_or((0, 0), |owner| {
                (owner.outstanding.len(), owner.outstanding_bytes)
            })
    }

    pub(in crate::pipeline) fn download_lane_holdings_of_job(
        &self,
        lane_id: u64,
        job_id: JobId,
    ) -> usize {
        self.download_lane_owners.get(&lane_id).map_or(0, |owner| {
            owner
                .outstanding
                .keys()
                .filter(|segment| segment.file_id.job_id == job_id)
                .count()
        })
    }

    // Every held refill is parked now. Used when the pool is reset and the
    // lanes those requests came from are gone.
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
            // The lane stopped waiting for an answer (it died, or its own
            // refill deadline ran out); nothing to answer and nothing was
            // booked.
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
        // A health probe batch waits for a seat on this server and has none
        // free anywhere it may go. The lane is given no more articles: it
        // finishes the ones it holds and parks keeping its connection, and
        // the next dispatch pass seats the batch there. The batch never
        // shares the lane with articles in flight.
        if self.probe_waits_for_seat_on(server_idx) {
            self.park_download_lane_refill(request, LaneParkReason::ProbeYield, held_since, now);
            return;
        }

        let pressure = self.refresh_download_pressure();
        let lane_mode = self.download_lane_mode_for_lane(
            server_idx,
            Self::refill_mode_pressure(pressure),
            request.supports_pipelining,
            lane_id,
        );
        let want = self.download_refill_want(lane_mode);
        let spill_in_flight = self.spill_job_in_flight_on(server_idx);
        let lane_share = LaneShare {
            lane_id,
            depth: lane_mode.max_depth(),
        };
        let works = match self.next_works_for_lane(
            server_idx,
            want,
            Some(lane_share),
            spill_in_flight,
            pressure,
        ) {
            Handout::Works(works) => works,
            Handout::Idle => {
                self.hold_or_park_idle_download_lane_refill(request, pressure, held_since, now);
                return;
            }
            Handout::Saturated(wake) => {
                // The lane keeps fetching what it holds with this ask
                // outstanding. It blocks on the answer only once its pipe is
                // dry, or on a park after returning every article it still
                // holds. A count wake is below anything it holds by then and
                // has been answered. A room wake is not: a lane is held for
                // room when not one more article fits below its volume's
                // lease limit, and once it holds nothing it is held as an
                // idle lane is (`service_held_download_refills`): its wait
                // is bounded by the idle window and ends for a probe, a pool
                // change or a job with work on a volume that has room.
                self.metrics
                    .download_lane_refill_saturated_total
                    .fetch_add(1, Ordering::Relaxed);
                if wake.room {
                    self.metrics
                        .download_lane_room_held_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.held_download_refills.push(HeldDownloadRefill {
                    request,
                    since: held_since.unwrap_or(now),
                    wake: Some(wake),
                    idle_since: None,
                    for_room: false,
                });
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
            // reservation (durable lead, download quota) and is back in the queue.
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
                    completion_critical,
                    lane_mode,
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

    // Turn a scheduler handout into a lease for `lane_id`: reserve each
    // article's bandwidth and durable lead, and return whatever could not be
    // reserved to its queue. `None` when nothing survived.
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
            match self.reserve_download_work_for_dispatch(job_id, work) {
                // The article is reserved on its job's volume, or goes back
                // with the rest of the handout: a lease holds only what fits
                // in the room the volume has left.
                Ok(Some(work)) if self.reserve_lane_work(&work) => reserved.push(work),
                Ok(Some(work)) => {
                    self.enqueue_download_work(work);
                    // Nothing fit: dispatch looks again once the volume has
                    // fallen to where the article fits.
                    if reserved.is_empty() {
                        self.volume_budget_held
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                        self.ask_room_wake(job_id);
                    }
                    break;
                }
                // The helper has already returned the refused article; the
                // rest of the handout follows it below so the batch keeps
                // the scheduler's order.
                Ok(None) | Err(_) => break,
            }
        }
        let mut returned = 0;
        for work in works {
            self.enqueue_download_work(work);
            returned += 1;
        }
        if returned > 0 {
            self.update_queue_metrics();
        }
        if reserved.is_empty() {
            return None;
        }
        // The handout was cut for `server_idx` alone, so the lease is pinned
        // there and no article in it may have failed on that server.
        debug_assert!(
            reserved
                .iter()
                .all(|work| !work.exclude_servers.contains(server_idx)),
            "a lease holds an article already excluded from the server it is pinned to"
        );
        let completion_critical = reserved.iter().any(|work| work.completion_critical);
        let dial_exclude_servers = (0..self.nntp.pool().server_count())
            .filter(|idx| *idx != server_idx)
            .collect();
        Some(DownloadBatchLease {
            body_activity: Arc::default(),
            lane_id,
            job_id,
            runtime_generation: self.pool_generation,
            lane_mode,
            server_modes: vec![(server_idx, lane_mode)],
            completion_critical,
            effective_exclude_servers: self
                .effective_exclude_servers(job_id, weaver_nntp::ServerSet::EMPTY),
            dial_exclude_servers,
            checkpoint_plan: self
                .graph_jobs
                .get(&job_id)
                .map_or(weaver_yenc::CheckpointPlan::None, |graph| {
                    weaver_yenc::CheckpointPlan::from_slice_sizes(graph.block_sizes()).plan
                }),
            pressure_clear: pressure.state == DownloadPressureState::Clear,
            repeated: self.repeated_articles.get(&job_id).cloned(),
            works: reserved,
        })
    }

    // A refill nothing could be cut for right now. The lane keeps its
    // socket and waits in the actor for the next wake — unless another
    // server could serve queued work now, in which case the slot this lane
    // is holding is worth more to a dial there than to a wait here, or the
    // hold has already run its course.
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
        // Held while a volume holds its job's budget: a hold for room.
        let for_room = self
            .volume_budget_held
            .load(std::sync::atomic::Ordering::Relaxed);
        if for_room {
            self.metrics
                .download_lane_room_held_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.held_download_refills.push(HeldDownloadRefill {
            request,
            since,
            wake: None,
            idle_since: None,
            for_room,
        });
    }

    // Move every held refill's clock back by the idle window, as if each
    // had waited that much longer.
    #[cfg(test)]
    pub(in crate::pipeline) fn age_held_download_refills_past_idle_window_for_test(&mut self) {
        let by = DOWNLOAD_REFILL_IDLE_HOLD;
        for held in &mut self.held_download_refills {
            held.since -= by;
            if let Some(idle) = &mut held.idle_since {
                *idle -= by;
            }
        }
    }

    // Whether a held refill waits for its volume's room.
    #[cfg(test)]
    pub(in crate::pipeline) fn room_held_download_refills_for_test(&self) -> usize {
        self.held_download_refills
            .iter()
            .filter(|held| held.wake.is_some_and(|wake| wake.room))
            .count()
    }

    // Cut one lease for `server_idx` the way a dispatch pass would: one
    // scheduler handout, reserved and pinned to that server. `None` when the
    // scheduler had nothing for the server or nothing survived reservation.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
                .map(|state| state.articles.queued_download_len())
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
