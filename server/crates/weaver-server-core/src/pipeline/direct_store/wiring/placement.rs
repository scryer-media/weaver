//! Direct-store placements off the pipeline task: a routed article's
//! destination writes run on a task of their own, and its commit is applied
//! when they return. See [`crate::pipeline::DirectPlacementFlight`].

use super::commit::prepare_direct_destination_paths;
use super::*;
use crate::pipeline::{
    DirectPlacement, DirectPlacementDone, DirectPlacementFlight, DirectPlacementFlightState,
    DirectPlacementKind, DirectPlacementOutcome,
};

/// Collects a placement task's outcome. A task that went away without
/// answering (a panic) reports a failed write: nothing it wrote is known to
/// have landed.
async fn collect_placement_outcome(
    outcome: tokio::sync::oneshot::Receiver<DirectPlacementOutcome>,
) -> DirectPlacementOutcome {
    outcome.await.unwrap_or_else(|_| DirectPlacementOutcome {
        prepared: Vec::new(),
        result: Err(DirectPlacementError::Write(std::io::Error::other(
            "the direct-store placement task did not complete",
        ))),
    })
}

impl Pipeline {
    /// Whether a completed volume's trailing region is still waiting to land
    /// in this set. Until it has, the set's coverage is short of its volumes.
    pub(in crate::pipeline) fn direct_set_has_pending_volume_tail(
        &self,
        job_id: JobId,
        set_index: usize,
    ) -> bool {
        self.direct_placement_lanes
            .get(&(job_id, set_index))
            .is_some_and(|lane| {
                lane.flight
                    .iter()
                    .flat_map(|flight| flight.placements.iter())
                    .chain(lane.queued.iter())
                    .any(|placement| {
                        matches!(placement.kind, DirectPlacementKind::VolumeTail { .. })
                    })
            })
    }

    /// Whether any placement of this job is still waiting for its
    /// destination writes or its commit.
    ///
    /// A flight being applied has already taken the placement it is
    /// committing off its list. Counting that one would make the commit's own
    /// follow-on — a volume completing, a set finalizing, the completion check
    /// after it — wait on itself.
    pub(crate) fn has_direct_placements(&self, job_id: JobId) -> bool {
        self.direct_placement_lanes
            .iter()
            .any(|((owner, _), lane)| {
                *owner == job_id
                    && (!lane.queued.is_empty()
                        || lane.flight.as_ref().is_some_and(|flight| {
                            !matches!(flight.state, DirectPlacementFlightState::Applying)
                                || !flight.placements.is_empty()
                        }))
            })
    }

    /// Re-queues a completion check that ran while this job had a placement
    /// out, once it has none. The drain sequence re-runs only once the
    /// download stage is idle; a check a placement's own commit ran inline —
    /// the one after a set finalizes — can defer while other downloads are
    /// still queued, and nothing else would run it again.
    fn wake_completion_check_awaiting_placements(&mut self, job_id: JobId) {
        if self.has_direct_placements(job_id)
            || !self.completion_checks_awaiting_placements.remove(&job_id)
        {
            return;
        }
        self.schedule_job_completion_check(job_id);
    }

    /// Queues a routed article's placement behind its set, sending it at once
    /// when the set has nothing writing.
    pub(super) fn enqueue_direct_placement(
        &mut self,
        job_id: JobId,
        set_index: usize,
        placement: DirectPlacement,
    ) {
        if !placement.spans.is_empty() {
            self.invalidate_par3_direct_set(job_id, set_index);
        }
        self.note_write_buffered(placement.buffered_len, 1);
        let lane = self
            .direct_placement_lanes
            .entry((job_id, set_index))
            .or_default();
        lane.queued.push_back(placement);
        if lane.flight.is_none() {
            self.launch_direct_placement_flight(job_id, set_index);
        }
    }

    /// Sends everything queued behind a set as one flight. Everything here is
    /// bookkeeping — grouping the spans into per-destination batches of
    /// refcounted views, and naming the destinations not yet created — and
    /// the task spawned at the end does all of the I/O.
    ///
    /// A flight with no span to write — articles that went wholly into the
    /// holds while an earlier one was writing — starts out resolved, and the
    /// caller applies it.
    fn launch_direct_placement_flight(&mut self, job_id: JobId, set_index: usize) {
        let key = (job_id, set_index);
        let Some(set_name) = self
            .direct_store
            .set(job_id, set_index)
            .map(|set| set.set_name().to_string())
        else {
            return;
        };
        let placements = match self.direct_placement_lanes.get_mut(&key) {
            Some(lane) if lane.flight.is_none() && !lane.queued.is_empty() => {
                std::mem::take(&mut lane.queued)
            }
            _ => return,
        };
        let id = self.next_direct_placement_flight_id;
        self.next_direct_placement_flight_id = self.next_direct_placement_flight_id.wrapping_add(1);
        // Refcount bumps, not payload: the views the router handed out are
        // what reaches the write syscall.
        let spans: Vec<RoutedSpan> = placements
            .iter()
            .flat_map(|placement| placement.spans.iter().cloned())
            .collect();
        let state = if spans.is_empty() {
            DirectPlacementFlightState::Resolved(DirectPlacementOutcome {
                prepared: Vec::new(),
                result: Ok(()),
            })
        } else {
            let batches = self.direct_write_batches(job_id, set_index, &spans);
            drop(spans);
            // Registers the staging root on the job state, as the inline
            // preparation always has; see `prepare_direct_destinations`.
            let _ = self.extraction_staging_dir(job_id);
            let unprepared = self.unprepared_direct_destinations(job_id, &batches);
            let marking = self.direct_store.sparse_marking();
            let ticket = crate::pipeline::orchestrator::register_direct_placement(
                batches.iter().map(|(path, _)| path.clone()).collect(),
            );
            let done_tx = self.direct_placement_done_tx.clone();
            #[cfg(test)]
            let hold = self.direct_placement_hold.clone();
            #[cfg(test)]
            let panics = self.direct_placement_panics;
            let (outcome_tx, outcome) = tokio::sync::oneshot::channel();
            // The I/O runs on a task of its own, and this one only reports
            // it: a panic in the I/O is a failed placement the pipeline
            // applies, not a flight left writing forever with nothing to
            // resolve it.
            let writes = tokio::spawn(async move {
                #[cfg(test)]
                if let Some(hold) = hold {
                    let _ = hold.acquire().await;
                }
                #[cfg(test)]
                if panics {
                    panic!("test hook: the direct-store placement task panicked");
                }
                let (prepared, result) =
                    prepare_direct_destination_paths(job_id, unprepared, marking).await;
                let result = match result {
                    Ok(()) => crate::pipeline::orchestrator::write_direct_batches(batches)
                        .await
                        .map_err(DirectPlacementError::Write),
                    Err(failure) => Err(failure),
                };
                // Every write has returned; a handle close waiting on these
                // destinations may go ahead. On a panic the ticket drops
                // while the task unwinds, still before the outcome is sent.
                drop(ticket);
                DirectPlacementOutcome { prepared, result }
            });
            tokio::spawn(async move {
                let outcome = writes.await.unwrap_or_else(|error| DirectPlacementOutcome {
                    prepared: Vec::new(),
                    result: Err(DirectPlacementError::Write(std::io::Error::other(format!(
                        "the direct-store placement task did not complete: {error}"
                    )))),
                });
                // The outcome first, so a join never waits on the done
                // channel, which only the pipeline task drains.
                let _ = outcome_tx.send(outcome);
                // A closed channel is the pipeline gone; the flight goes with it.
                let _ = done_tx
                    .send(DirectPlacementDone {
                        job_id,
                        set_index,
                        flight_id: id,
                    })
                    .await;
            });
            DirectPlacementFlightState::Pending(outcome)
        };
        if let Some(lane) = self.direct_placement_lanes.get_mut(&key) {
            lane.flight = Some(DirectPlacementFlight {
                id,
                set_name,
                state,
                placements,
            });
        }
    }

    /// A placement task's writes returned: apply them, then send whatever
    /// queued behind them.
    pub(in crate::pipeline) async fn handle_direct_placement_done(
        &mut self,
        done: DirectPlacementDone,
    ) {
        let current = self
            .direct_placement_lanes
            .get(&(done.job_id, done.set_index))
            .and_then(|lane| lane.flight.as_ref())
            .is_some_and(|flight| flight.id == done.flight_id);
        if !current {
            // Its set let it go first — a demotion, a job removal, a join —
            // and whoever did that already accounted for its articles.
            debug!(
                job_id = done.job_id.0,
                set_index = done.set_index,
                flight = done.flight_id,
                "a direct-store placement landed after its set let it go"
            );
            // A barrier's settle applies the flight on its own and does not
            // re-run the drain sequence; whatever deferred to this placement
            // meanwhile has nothing else to wake it.
            if self.jobs.contains_key(&done.job_id) && self.job_decode_stage_drained(done.job_id) {
                self.maybe_finish_download_pass(done.job_id);
            }
            self.wake_completion_check_awaiting_placements(done.job_id);
            return;
        }
        // The outcome was sent before this message; this does not wait.
        self.await_direct_placement_io(done.job_id, done.set_index)
            .await;
        self.advance_direct_placements(done.job_id, done.set_index)
            .await;
        // The decode-done path asks this once a job's last article settles,
        // and a routed article only settles here.
        if self.jobs.contains_key(&done.job_id) && self.job_decode_stage_drained(done.job_id) {
            self.maybe_finish_download_pass(done.job_id);
        }
        self.wake_completion_check_awaiting_placements(done.job_id);
        // Bytes that waited here were counted as write backlog; a hard latch
        // they helped raise has nothing else to lift it until the next tick.
        self.relieve_latched_write_backlog().await;
    }

    /// Waits for the writes of a set's flight, if one is out, and keeps the
    /// outcome for the done message to apply. For anything that must not run
    /// alongside those writes but does not need their commits.
    pub(in crate::pipeline) async fn await_direct_placement_io(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) {
        let key = (job_id, set_index);
        let Some(flight) = self
            .direct_placement_lanes
            .get_mut(&key)
            .and_then(|lane| lane.flight.as_mut())
        else {
            return;
        };
        if !matches!(flight.state, DirectPlacementFlightState::Pending(_)) {
            return;
        }
        let id = flight.id;
        let DirectPlacementFlightState::Pending(outcome) =
            std::mem::replace(&mut flight.state, DirectPlacementFlightState::Applying)
        else {
            return;
        };
        let mut outcome = collect_placement_outcome(outcome).await;
        self.note_prepared_direct_destinations(job_id, std::mem::take(&mut outcome.prepared));
        if let Some(flight) = self
            .direct_placement_lanes
            .get_mut(&key)
            .and_then(|lane| lane.flight.as_mut())
            .filter(|flight| flight.id == id)
        {
            flight.state = DirectPlacementFlightState::Resolved(outcome);
        }
    }

    /// Applies a resolved flight and sends the next, until the set has a
    /// flight writing or nothing left.
    async fn advance_direct_placements(&mut self, job_id: JobId, set_index: usize) {
        let key = (job_id, set_index);
        loop {
            let Some(lane) = self.direct_placement_lanes.get_mut(&key) else {
                return;
            };
            match lane.flight.as_mut() {
                Some(flight) => {
                    // Pending: its done message comes back here. Applying: a
                    // frame further up is doing it.
                    if !matches!(flight.state, DirectPlacementFlightState::Resolved(_)) {
                        return;
                    }
                    let id = flight.id;
                    let DirectPlacementFlightState::Resolved(outcome) =
                        std::mem::replace(&mut flight.state, DirectPlacementFlightState::Applying)
                    else {
                        return;
                    };
                    self.apply_direct_placement_flight(job_id, set_index, id, outcome)
                        .await;
                    if let Some(lane) = self.direct_placement_lanes.get_mut(&key)
                        && lane.flight.as_ref().is_some_and(|flight| flight.id == id)
                    {
                        lane.flight = None;
                    }
                }
                None => {
                    if lane.queued.is_empty() {
                        self.direct_placement_lanes.remove(&key);
                        return;
                    }
                    if !self.direct_set_takes_placements(job_id, set_index, None) {
                        let stale = self.take_lane_placements(key);
                        self.retire_stale_direct_placements(job_id, set_index, stale)
                            .await;
                        return;
                    }
                    self.launch_direct_placement_flight(job_id, set_index);
                }
            }
        }
    }

    /// Whether a set can still commit a placement: the job is here, the set
    /// is the one the placement was routed into, and it has neither demoted
    /// nor finalized.
    fn direct_set_takes_placements(
        &self,
        job_id: JobId,
        set_index: usize,
        set_name: Option<&str>,
    ) -> bool {
        self.jobs.contains_key(&job_id)
            && self.direct_store.set(job_id, set_index).is_some_and(|set| {
                !set.is_demoted()
                    && !set.is_finalized()
                    && set_name.is_none_or(|name| set.set_name() == name)
            })
    }

    /// Commits a flight whose writes all returned, one article at a time in
    /// routing order; or, for one whose writes did not, runs the placement
    /// failure policy and hands every article behind the set back.
    ///
    /// Articles are taken off the flight one by one rather than all at once,
    /// so a commit that demotes the set — a volume whose CRC disagrees, say —
    /// finds the rest still on the lane, where the demotion collects them as
    /// handoffs before it plans its sweep.
    async fn apply_direct_placement_flight(
        &mut self,
        job_id: JobId,
        set_index: usize,
        flight_id: u64,
        outcome: DirectPlacementOutcome,
    ) {
        let key = (job_id, set_index);
        self.note_prepared_direct_destinations(job_id, outcome.prepared);
        let Some(set_name) = self
            .direct_placement_lanes
            .get(&key)
            .and_then(|lane| lane.flight.as_ref())
            .filter(|flight| flight.id == flight_id)
            .map(|flight| flight.set_name.clone())
        else {
            return;
        };
        if let Err(failure) = outcome.result {
            // Everything behind the set goes back, not only this flight: the
            // queued articles were routed into a set that is about to demote,
            // or into a job that is about to fail.
            let placements = self.take_lane_placements(key);
            if self.direct_set_takes_placements(job_id, set_index, Some(&set_name)) {
                let handoffs: Vec<SegmentId> = placements
                    .iter()
                    .filter_map(DirectPlacement::article)
                    .collect();
                self.handle_direct_placement_failure(job_id, set_index, &handoffs, failure)
                    .await;
                self.hand_back_direct_placements(placements).await;
            } else {
                self.retire_stale_direct_placements(job_id, set_index, placements)
                    .await;
            }
            return;
        }
        loop {
            let Some(placement) = self
                .direct_placement_lanes
                .get_mut(&key)
                .and_then(|lane| lane.flight.as_mut())
                .filter(|flight| flight.id == flight_id)
                .and_then(|flight| flight.placements.pop_front())
            else {
                return;
            };
            self.release_write_buffered(placement.buffered_len, 1);
            if !self.direct_set_takes_placements(job_id, set_index, Some(&set_name)) {
                self.retire_stale_direct_placements(job_id, set_index, vec![placement])
                    .await;
                continue;
            }
            let DirectPlacement {
                spans,
                kind,
                buffered_len: _,
            } = placement;
            self.record_direct_placement(job_id, set_index, &spans);
            drop(spans);
            match kind {
                DirectPlacementKind::Article {
                    segment,
                    volume_index,
                    file_offset,
                } => {
                    Box::pin(self.commit_direct_segment(
                        segment.segment_id,
                        segment.decoded_size,
                        set_index,
                        volume_index,
                        file_offset,
                        segment.part_crc,
                        segment.part_crc_verified,
                        &segment.checkpoint_plan,
                        &segment.segments,
                    ))
                    .await;
                    self.note_mixed_rar_commit(job_id, set_index);
                }
                DirectPlacementKind::VolumeTail { volume_index } => {
                    debug!(
                        job_id = job_id.0,
                        set_index,
                        volume = volume_index,
                        "a completed volume's trailing region landed"
                    );
                    Box::pin(self.finish_direct_volume_completion(job_id, set_index)).await;
                }
            }
        }
    }

    /// Takes every placement behind a set off its lane — the flight's and the
    /// queue's, in routing order — and releases their backlog. Does not wait
    /// for a flight's writes; see [`Self::take_direct_placements`].
    fn take_lane_placements(&mut self, key: (JobId, usize)) -> Vec<DirectPlacement> {
        let Some(lane) = self.direct_placement_lanes.remove(&key) else {
            return Vec::new();
        };
        let mut placements: Vec<DirectPlacement> = lane
            .flight
            .into_iter()
            .flat_map(|flight| flight.placements)
            .collect();
        placements.extend(lane.queued);
        for placement in &placements {
            self.release_write_buffered(placement.buffered_len, 1);
        }
        placements
    }

    /// Takes every placement behind a set, after the flight's writes have
    /// returned. A demotion calls this before anything else: its sweep
    /// deletes the destinations those writes target, and the articles become
    /// the demotion's handoffs.
    pub(super) async fn take_direct_placements(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) -> Vec<DirectPlacement> {
        self.await_direct_placement_io(job_id, set_index).await;
        self.take_lane_placements((job_id, set_index))
    }

    /// Placements whose set stopped taking them. A finalized set's are
    /// duplicates of articles it already committed and are discarded, as the
    /// decode seam discards one; any other set's go back to the conventional
    /// path, like a failed placement.
    async fn retire_stale_direct_placements(
        &mut self,
        job_id: JobId,
        set_index: usize,
        placements: Vec<DirectPlacement>,
    ) {
        if placements.is_empty() {
            return;
        }
        let finalized = self
            .direct_store
            .set(job_id, set_index)
            .is_some_and(DirectSet::is_finalized);
        debug!(
            job_id = job_id.0,
            set_index,
            articles = placements.len(),
            finalized,
            "direct-store placements outlived their set's direct phase"
        );
        if finalized {
            return;
        }
        self.hand_back_direct_placements(placements).await;
    }

    /// Hands routed articles to the conventional path, exactly as the decode
    /// seam hands back one whose set demoted under it.
    ///
    /// A volume's trailing region carries no article to hand back; the
    /// demotion that let it go owns those bytes, as it owns any the set had
    /// not recorded as coverage.
    pub(super) async fn hand_back_direct_placements(&mut self, placements: Vec<DirectPlacement>) {
        for placement in placements {
            let DirectPlacementKind::Article {
                segment,
                file_offset,
                ..
            } = placement.kind
            else {
                continue;
            };
            let segment_id = segment.segment_id;
            let file_id = segment_id.file_id;
            let Some(file) = self
                .jobs
                .get_mut(&file_id.job_id)
                .and_then(|state| state.assembly.file_mut(file_id))
            else {
                continue;
            };
            // Demotion rebuilds conventional assembly with `reset`, which
            // also clears the placement this article recorded.
            file.record_placement(segment_id.segment_number, file_offset, segment.decoded_size);
            crate::runtime::perf_probe::record(
                "direct_store.article.demoted",
                std::time::Duration::from_nanos(1),
            );
            Box::pin(self.buffer_decoded_segment_conventionally(
                segment_id,
                file_offset,
                segment,
                true,
            ))
            .await;
        }
    }

    /// Drives a set's placements to empty — applying every flight, sending
    /// and applying what queued behind it — for a demanded barrier that must
    /// describe them. Returns early when a frame further up is applying the
    /// set's flight: the writes of that flight have already returned.
    pub(in crate::pipeline) async fn settle_direct_placements(
        &mut self,
        job_id: JobId,
        set_index: usize,
    ) {
        let key = (job_id, set_index);
        loop {
            let Some(lane) = self.direct_placement_lanes.get(&key) else {
                return;
            };
            match lane.flight.as_ref().map(|flight| &flight.state) {
                Some(DirectPlacementFlightState::Applying) => return,
                Some(DirectPlacementFlightState::Pending(_)) => {
                    self.await_direct_placement_io(job_id, set_index).await;
                }
                Some(DirectPlacementFlightState::Resolved(_)) | None => {
                    self.advance_direct_placements(job_id, set_index).await;
                }
            }
        }
    }

    /// [`Self::settle_direct_placements`] for every set with anything out.
    /// The shutdown drain's last word on routed articles.
    pub(crate) async fn settle_all_direct_placements(&mut self) {
        let keys: Vec<(JobId, usize)> = self.direct_placement_lanes.keys().copied().collect();
        for (job_id, set_index) in keys {
            self.settle_direct_placements(job_id, set_index).await;
        }
    }

    /// Forgets a removed job's placements. Their tasks run to the end on
    /// their own, and a close of the job's roots waits for them
    /// ([`crate::pipeline::close_cached_write_handles_under`]).
    pub(crate) fn drop_direct_placements_for_job(&mut self, job_id: JobId) {
        self.completion_checks_awaiting_placements.remove(&job_id);
        let keys: Vec<(JobId, usize)> = self
            .direct_placement_lanes
            .keys()
            .filter(|(owner, _)| *owner == job_id)
            .copied()
            .collect();
        for key in keys {
            drop(self.take_lane_placements(key));
        }
    }
}
