use super::*;

impl Pipeline {
    pub(in crate::pipeline) fn bandwidth_reservation_estimate(decoded_bytes: u32) -> u64 {
        let decoded = decoded_bytes as u64;
        decoded.saturating_add(decoded / 16).saturating_add(1024)
    }

    pub(in crate::pipeline::download::worker) fn reserve_rate_limit_for_dispatch(
        &mut self,
        segment_id: SegmentId,
        estimate_bytes: u64,
    ) {
        self.rate_limiter.consume(estimate_bytes);
        self.rate_limit_reservations
            .insert(segment_id, estimate_bytes);
    }

    /// The gauge that counts lanes running at this depth. Depths off the rung
    /// ladder round down to the rung they behave like.
    fn lane_depth_gauge(&self, mode: DownloadLaneMode) -> &std::sync::atomic::AtomicUsize {
        match mode.depth() {
            0 | 1 => &self.metrics.download_lanes_sequential_active,
            2 | 3 => &self.metrics.download_lanes_depth2_active,
            4..=7 => &self.metrics.download_lanes_depth4_active,
            _ => &self.metrics.download_lanes_depth8_active,
        }
    }

    /// Decrement a lane gauge without letting it wrap.
    ///
    /// The gauges are balanced by construction — every lane carries the mode it
    /// was booked under and reports that mode back at each transition and at
    /// park — but a gauge is a diagnostic, and a diagnostic that reads
    /// `18446744073709551615` because one path lost a transition is worse than
    /// one that reads zero.
    fn release_lane_gauge(gauge: &std::sync::atomic::AtomicUsize) {
        let _ = gauge.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            Some(count.saturating_sub(1))
        });
    }

    pub(in crate::pipeline) fn note_download_lane_started(&mut self, mode: DownloadLaneMode) {
        debug!(mode = ?mode, "download lane started");
        self.metrics
            .download_lanes_active
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .download_lanes_issuing_active
            .fetch_add(1, Ordering::Relaxed);
        self.lane_depth_gauge(mode).fetch_add(1, Ordering::Relaxed);
    }

    pub(in crate::pipeline) fn note_download_lane_released(
        &mut self,
        mode: DownloadLaneMode,
        reason: LaneParkReason,
    ) {
        Self::release_lane_gauge(&self.metrics.download_lanes_active);
        Self::release_lane_gauge(&self.metrics.download_lanes_issuing_active);
        Self::release_lane_gauge(self.lane_depth_gauge(mode));
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
            LaneParkReason::HotShareYield => self
                .metrics
                .download_lane_parks_hot_share_yield_total
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
            LaneParkReason::ProofFailure => self
                .metrics
                .download_lane_parks_proof_failure_total
                .fetch_add(1, Ordering::Relaxed),
            LaneParkReason::Capacity | LaneParkReason::ServerQuota => 0,
            LaneParkReason::Error => self
                .metrics
                .download_lane_parks_error_total
                .fetch_add(1, Ordering::Relaxed),
        };
    }

    /// A wire outcome retired this segment: no server has it, or the budget
    /// for asking ran out.
    pub(in crate::pipeline) fn book_failed_segment(&mut self, seg_id: SegmentId) {
        self.book_terminal_segment(seg_id, SegmentTerminalState::Missing);
    }

    /// Move a segment into its one terminal state.
    ///
    /// # Why this is a transition and not an increment
    ///
    /// `failed_bytes` used to be an accumulator that several paths added to:
    /// the terminal booking here, and a health probe that overwrote it with a
    /// sampled *projection* over the whole payload. A job could therefore book
    /// a projection first and then add real terminal failures on top of it, and
    /// job 10220 did exactly that — 1.99 GB of failed bytes against a 1.21 GB
    /// job, an impossibility that nothing in the arithmetic could refuse.
    ///
    /// So the ledger is no longer written to. It is *derived*: a segment enters
    /// exactly one terminal state on the pending→terminal edge fired here, and
    /// `failed_bytes` is the sum of the declared sizes of the segments holding
    /// one. The size is the NZB's declaration, never a served size, because a
    /// segment that fails has no served size worth the name — and because
    /// health has to mean the same thing whatever a hostile server sends.
    ///
    /// Every path that retires a segment comes through here, including the ones
    /// with no wire outcome at all (see the foreign-layout breaker). Returns
    /// whether this call is the edge.
    pub(in crate::pipeline) fn book_terminal_segment(
        &mut self,
        seg_id: SegmentId,
        terminal_state: SegmentTerminalState,
    ) -> bool {
        let job_id = seg_id.file_id.job_id;
        // A segment can become a PAR2 metadata probe after an earlier ordinary
        // attempt already exhausted it. The ledger stays idempotent below,
        // but discovery must still learn that its newly queued probe cannot
        // produce metadata or it will wait forever in `work_is_queued`.
        self.mark_promoted_recovery_segment_unavailable(seg_id);
        if self.segment_already_delivered(seg_id) {
            // Delivery is a terminal state too, and it is held in the assembly
            // bitmap. A late failure result for an ordinal that already landed
            // — a losing racer, a stale retry — must not book bytes the job has
            // on disk.
            return false;
        }
        let declared_bytes = self.health_counted_segment_bytes(seg_id);
        // The state a segment holds is the first one it reached, and a later
        // path must not overwrite it: a retry that exhausts after the article
        // was already ruled missing does not change what happened to it.
        match self.segment_terminal_states.entry(seg_id) {
            std::collections::hash_map::Entry::Occupied(_) => return false,
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert(terminal_state);
            }
        }
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.failed_bytes = state.failed_bytes.saturating_add(declared_bytes);
        }
        self.skip_failed_uu_segment(seg_id);
        self.check_health(job_id);
        true
    }

    /// Whether the assembly already holds this segment's bytes.
    fn segment_already_delivered(&self, seg_id: SegmentId) -> bool {
        self.jobs
            .get(&seg_id.file_id.job_id)
            .and_then(|state| state.assembly.file(seg_id.file_id))
            .is_some_and(|file| file.has_segment(seg_id.segment_number))
    }

    /// The job's failed bytes as the terminal states alone define them.
    ///
    /// The running figure on `JobState` is maintained by the single edge in
    /// [`Self::book_terminal_segment`], so the two agree by construction; this
    /// is what settlement re-derives against rather than trusting the running
    /// figure it is about to persist forever.
    pub(in crate::pipeline) fn derived_failed_bytes(&self, job_id: JobId) -> u64 {
        self.segment_terminal_states
            .keys()
            .filter(|seg_id| seg_id.file_id.job_id == job_id)
            .map(|seg_id| self.health_counted_segment_bytes(*seg_id))
            .sum()
    }

    /// The failed bytes to write into the terminal record, pinned to what the
    /// job could possibly have lost.
    ///
    /// `failed_bytes <= total_bytes` is not a style preference: job 10220 was
    /// archived with 1.99 GB failed against 1.21 GB total, and every consumer
    /// downstream — the health percentage, the API's granular failure fields,
    /// the automation reading them — silently produced nonsense from it. The
    /// derived ledger cannot exceed the total by construction, so a breach here
    /// means an invariant broke upstream: say so, and refuse to persist the
    /// impossible number either way.
    pub(in crate::pipeline) fn settled_failed_bytes(&self, job_id: JobId, total_bytes: u64) -> u64 {
        let failed_bytes = self
            .jobs
            .get(&job_id)
            .map_or(0, |state| state.failed_bytes)
            .max(self.derived_failed_bytes(job_id));
        debug_assert!(
            failed_bytes <= total_bytes,
            "job {} settled with failed_bytes {failed_bytes} above total_bytes {total_bytes}",
            job_id.0
        );
        if failed_bytes > total_bytes {
            warn!(
                job_id = job_id.0,
                failed_bytes,
                total_bytes,
                "failed bytes exceeded the job size at settlement; clamping the terminal record"
            );
            return total_bytes;
        }
        failed_bytes
    }

    /// The declared bytes of this file's segments that reached a terminal
    /// state without arriving.
    pub(in crate::pipeline) fn file_terminal_failed_bytes(&self, file_id: NzbFileId) -> u64 {
        self.segment_terminal_states
            .keys()
            .filter(|seg_id| seg_id.file_id == file_id)
            .map(|seg_id| self.health_counted_segment_bytes(*seg_id))
            .sum()
    }

    /// Unwedge a uuencode file whose cursor is waiting on a part that will
    /// never arrive.
    ///
    /// Sequential assembly has no way past a permanently missing part on its
    /// own: every later part's offset is defined by that part's decoded length,
    /// which is now unknowable. The choice is to wedge the file forever or to
    /// close the gap, and closing it is what both reference downloaders do.
    ///
    /// The consequence is stated plainly: every part after the hole is written
    /// one hole-width early, so the file's bytes past that point are
    /// **misaligned**, not merely incomplete. The file is marked damaged and
    /// PAR2 is the authority on whether it can be recovered. Without PAR2 the
    /// file is simply wrong, which is still better than a job that never
    /// finishes — and the damage flag is what tells the truth about it.
    pub(in crate::pipeline) fn skip_failed_uu_segment(&mut self, seg_id: SegmentId) {
        let Some(uu) = self.uu_files.get_mut(&seg_id.file_id) else {
            return;
        };
        if seg_id.segment_number != uu.next_index {
            // Only the ordinal the cursor is actually waiting on can wedge it.
            // A later failure is handled when the cursor reaches it.
            return;
        }
        uu.damaged = true;
        uu.next_index = uu.next_index.saturating_add(1);
    }

    /// Drop the job's terminal states, and the ledger derived from them.
    ///
    /// The two move together on purpose. Clearing the states alone would leave
    /// the job carrying bytes nothing accounts for, and the next booking of the
    /// same segment — a restarted job re-fetches every one of them — would add
    /// those bytes a second time.
    pub(crate) fn clear_terminal_segment_failures(&mut self, job_id: JobId) {
        self.segment_terminal_states
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.failed_bytes = 0;
            state.probe_projected_failed_bytes = 0;
        }
        self.foreign_layout_watches
            .retain(|file_id, _| file_id.job_id != job_id);
        self.files_counted_missing
            .retain(|file_id| file_id.job_id != job_id);
    }

    /// Count the files this job could not assemble from articles.
    ///
    /// Called once the download pipeline has drained with data files still
    /// incomplete: that is the moment the remaining segments are known to be
    /// unavailable across every configured server rather than merely late.
    /// A file is counted **once**, with the number of segments still missing
    /// at that moment — not once per failed segment — and the per-job guard
    /// set keeps the many re-entries of the completion check from counting it
    /// again. PAR2 may still rebuild the file afterwards; that is a repair,
    /// and `weaver_repairs_total` is where it is accounted for. What this
    /// counter answers is how much of the payload Usenet itself could not
    /// supply.
    ///
    /// Per-file, per-job: a `HashMap` lookup and a `HashSet` insert here are
    /// the same class of work `check_job_completion` around it already does,
    /// and nothing on a per-segment path reaches this.
    pub(in crate::pipeline) fn note_incomplete_files_after_download_drain(
        &mut self,
        job_id: JobId,
    ) {
        let Some(state) = self.jobs.get(&job_id) else {
            return;
        };
        let incomplete: Vec<(NzbFileId, u64)> = state
            .assembly
            .files()
            .filter(|file| !file.is_complete())
            // Recovery volumes are optional by design: an absent PAR2 block is
            // not a file the job failed to assemble, it is recovery capacity
            // that was never needed or never fetched.
            .filter(|file| {
                !matches!(
                    file.role(),
                    weaver_model::files::FileRole::Par2 {
                        is_index: false,
                        ..
                    }
                )
            })
            .map(|file| (file.file_id(), u64::from(file.missing_count())))
            .collect();
        for (file_id, missing_segments) in incomplete {
            if self.files_counted_missing.insert(file_id) {
                self.metrics
                    .job_lifecycle
                    .note_file_missing(missing_segments);
            }
        }
    }

    fn health_counted_segment_bytes(&self, segment_id: SegmentId) -> u64 {
        self.jobs
            .get(&segment_id.file_id.job_id)
            .and_then(|state| {
                let file_spec = state
                    .spec
                    .files
                    .get(segment_id.file_id.file_index as usize)?;
                file_spec.role.counts_toward_health().then(|| {
                    file_spec
                        .segments
                        .iter()
                        .find(|segment| segment.ordinal == segment_id.segment_number)
                        .map_or(0, |segment| segment.bytes as u64)
                })
            })
            .unwrap_or(0)
    }

    pub(in crate::pipeline::download::worker) fn refill_mode_pressure(
        pressure: DownloadPressure,
    ) -> DownloadPressure {
        if pressure.state == DownloadPressureState::Soft {
            DownloadPressure {
                state: DownloadPressureState::Clear,
                ..pressure
            }
        } else {
            pressure
        }
    }

    pub(in crate::pipeline::download::worker) fn activation_items(
        lease: &DownloadBatchLease,
    ) -> Vec<(SegmentId, NzbFileId, u64)> {
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

    pub(in crate::pipeline::download::worker) fn rollback_download_batch_lease(
        &mut self,
        lease: DownloadBatchLease,
    ) {
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

    pub(in crate::pipeline::download::worker) fn record_download_lane_observation(
        &mut self,
        result: &DownloadResult,
    ) {
        let Some(observation) = result.lane_observation.as_ref() else {
            return;
        };
        let Some(server_idx) = observation.server_idx else {
            return;
        };

        let now = Instant::now();
        let pressure_clear = observation.pressure_clear;
        // Seeding needs the persisted rung and the connection test's latency;
        // both are cold-path lookups, so they only run for a server the lanes
        // have not seen yet.
        let seed = if self.download_lane_runtime.servers.contains_key(&server_idx) {
            None
        } else {
            Some((
                self.persisted_pipelining_depth(server_idx),
                self.probe_latency(server_idx),
            ))
        };
        let explorer = self
            .download_lane_runtime
            .servers
            .entry(server_idx)
            .or_insert_with(|| {
                let (proven_depth, probe_latency) = seed.unwrap_or_default();
                ServerPipelineExplorer::seeded(proven_depth, probe_latency)
            });
        explorer.note_supports_pipelining(observation.supports_pipelining);
        if let Some(latency) = observation.latency {
            explorer.note_latency(latency);
        }
        if let Some(transfer) = observation.transfer {
            explorer.note_transfer(transfer);
        }

        let mut change = None;
        if result.data.is_ok() {
            change = explorer.note_response(
                now,
                observation.mode.depth(),
                observation.payload_bytes,
                observation.policy_elapsed,
                pressure_clear,
            );
        }

        if observation.mode != DownloadLaneMode::Sequential && observation.batch_complete {
            if observation.batch_clean {
                self.metrics
                    .download_pipeline_trial_success_total
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                change = explorer.note_unclean_batch(now).or(change);
                self.metrics
                    .download_pipeline_trial_failure_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            if observation.unresolved_count > 0 {
                self.metrics
                    .download_pipeline_replay_items_total
                    .fetch_add(observation.unresolved_count, Ordering::Relaxed);
            }
        }

        let Some(change) = change else {
            return;
        };
        let target = explorer.target_rung();
        // A step up is still on trial, so it must not be written through: a
        // restart would resume at a depth nothing has yet paid for. Every other
        // outcome is a settled verdict, including the ones that lower the rung
        // — a persisted depth that over-promises is the one worth correcting.
        let persist_depth = if matches!(change, RungChange::Stepped { from, to } if to > from) {
            None
        } else {
            explorer.take_persist_request()
        };
        match change {
            RungChange::Stepped { from, to } => {
                self.metrics
                    .download_pipeline_proof_pass_total
                    .fetch_add(1, Ordering::Relaxed);
                info!(
                    server = server_idx,
                    from, to, target, "download pipeline depth stepped"
                );
            }
            RungChange::Kept { depth } => {
                self.metrics
                    .download_pipeline_proof_pass_total
                    .fetch_add(1, Ordering::Relaxed);
                info!(server = server_idx, depth, "download pipeline depth kept");
            }
            RungChange::Reverted { from, to } => {
                info!(
                    server = server_idx,
                    from, to, "download pipeline depth reverted; server held"
                );
            }
            RungChange::Dropped { from, to } => {
                self.metrics
                    .download_pipeline_cooldown_total
                    .fetch_add(1, Ordering::Relaxed);
                info!(
                    server = server_idx,
                    from, to, "download pipeline batch was unclean; depth dropped"
                );
            }
            RungChange::PinnedSequential => {
                self.metrics
                    .download_pipeline_cooldown_total
                    .fetch_add(1, Ordering::Relaxed);
                warn!(
                    server = server_idx,
                    "second unclean pipelined batch; server pinned to sequential BODY fetches"
                );
            }
        }
        if let Some(depth) = persist_depth {
            self.persist_pipelining_depth(server_idx, depth);
        }
    }

    /// Write a proven rung back to the servers table so the next start does not
    /// have to rediscover it.
    fn persist_pipelining_depth(&self, server_idx: usize, depth: u8) {
        let Some(stable_id) = self
            .nntp
            .pool()
            .stable_server_id(weaver_nntp::pool::ServerId(server_idx))
        else {
            return;
        };
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.update_server_pipelining_depth(stable_id.0, Some(depth)) {
                warn!(error = %error, server = stable_id.0, depth, "failed to persist proven pipelining depth");
            }
        });
    }

    /// Hand the control plane what the lanes have learned about each server's
    /// BODY transport. Called on the tuning tick, never per response.
    pub(in crate::pipeline) fn publish_download_transport_health(&self) {
        let mut health: Vec<crate::ServerTransportHealth> = self
            .download_lane_runtime
            .servers
            .iter()
            .map(|(server_idx, explorer)| crate::ServerTransportHealth {
                server_idx: *server_idx,
                latency_ms: explorer
                    .latency()
                    .map(|latency| latency.as_secs_f64() * 1000.0),
                transfer_ms: explorer
                    .transfer()
                    .map(|transfer| transfer.as_secs_f64() * 1000.0),
                latency_band: explorer.latency_band().map(|band| band.label().to_string()),
                pipeline_depth: u32::from(explorer.current_depth()),
                pinned_sequential: explorer.pinned_sequential(),
            })
            .collect();
        health.sort_unstable_by_key(|entry| entry.server_idx);
        self.shared_state.set_download_transport_health(health);
    }

    /// The rung a previous run proved for this server, as loaded into the
    /// pool's server configuration.
    fn persisted_pipelining_depth(&self, server_idx: usize) -> Option<u8> {
        self.nntp
            .pool()
            .server_configs()
            .get(server_idx)
            .and_then(|config| config.pipelining_depth)
    }

    /// First-byte latency the most recent connection test measured for this
    /// server, if one ran since the process started.
    fn probe_latency(&self, server_idx: usize) -> Option<Duration> {
        let stable_id = self
            .nntp
            .pool()
            .stable_server_id(weaver_nntp::pool::ServerId(server_idx))?;
        self.shared_state.server_probe_latency(stable_id.0)
    }

    pub(in crate::pipeline::download::worker) fn should_use_owned_blocking_lane(
        &self,
        lease: &DownloadBatchLease,
    ) -> bool {
        if lease.compatibility.is_recovery {
            return false;
        }
        if lease.works.iter().any(|work| work.is_recovery) {
            return false;
        }
        self.nntp
            .has_blocking_body_lane_candidate(&lease.effective_exclude_servers)
    }

    pub(in crate::pipeline::download::worker) fn reconcile_rate_limit_for_download(
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

    pub(in crate::pipeline::download::worker) fn choose_download_lane_mode(
        &mut self,
        job_id: JobId,
        is_recovery: bool,
        pressure: DownloadPressure,
    ) -> DownloadLaneMode {
        let _ = job_id;
        if is_recovery {
            return DownloadLaneMode::Sequential;
        }
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        self.download_lane_runtime
            .servers
            .values()
            .map(|explorer| explorer.choose_mode(pressure_clear))
            .max_by_key(|mode| mode.max_depth())
            .unwrap_or(DownloadLaneMode::Sequential)
    }

    pub(in crate::pipeline::download::worker) fn download_lane_server_modes(
        &mut self,
        job_id: JobId,
        is_recovery: bool,
        pressure: DownloadPressure,
    ) -> Vec<(usize, DownloadLaneMode)> {
        let _ = job_id;
        if is_recovery {
            return Vec::new();
        }
        let pressure_clear = pressure.state == DownloadPressureState::Clear;
        self.download_lane_runtime
            .servers
            .iter()
            .map(|(server_idx, explorer)| (*server_idx, explorer.choose_mode(pressure_clear)))
            .collect()
    }

    pub(in crate::pipeline) fn note_download_lane_mode_changed(
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

        Self::release_lane_gauge(self.lane_depth_gauge(previous));
        self.lane_depth_gauge(next).fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn handle_owned_download_lane_event(
        &mut self,
        event: OwnedDownloadLaneEvent,
        pending: &mut VecDeque<DownloadResult>,
    ) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.owned_lane.event");
        match event {
            OwnedDownloadLaneEvent::AcquireFailed { lease, error } => {
                // Capacity admission and health-mutex contention are both
                // "ask again shortly": the work goes back to the scheduler on
                // the owned fast path instead of being demoted to an async
                // lane on what is not a verdict about the servers at all.
                if error.should_requeue_owned_work() {
                    let DownloadBatchLease {
                        job_id,
                        lane_mode,
                        spillover_loan_kind,
                        compatibility,
                        works,
                        ..
                    } = lease;
                    debug!(
                        job_id = job_id.0,
                        works = works.len(),
                        error = %error,
                        "owned blocking lane unavailable now; returning work to scheduler"
                    );
                    crate::runtime::perf_probe::record_value(
                        "download.owned_lane.acquire_failed_capacity_requeue",
                        1,
                    );
                    for work in works {
                        self.restore_owned_lane_unrequested_work(work);
                    }
                    self.handle_download_lane_parked(DownloadLaneParked {
                        job_id,
                        mode: lane_mode,
                        spillover_loan_kind,
                        completion_critical: compatibility.completion_critical,
                        reason: LaneParkReason::Capacity,
                        release_connection_slot: true,
                        release_ip_replacement_burst: false,
                    });
                    return;
                }
                debug!(
                    job_id = lease.job_id.0,
                    works = lease.works.len(),
                    error = %error,
                    "owned blocking lane unavailable; falling back to async download lane"
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.acquire_failed_fallback",
                    1,
                );
                self.spawn_async_download_batch(lease);
            }
            OwnedDownloadLaneEvent::BatchComplete {
                results,
                unrequested_works,
                stats,
                ack,
            } => {
                // Ack on receipt: the lane only needs delivery confirmation for
                // backpressure, not completion of per-result ingest. Holding
                // the ack through ingest serialized every lane behind this
                // loop at batch boundaries and stalled all downloads at once.
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.batch.results",
                    results.len() as u64,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.batch.unrequested_requeued",
                    unrequested_works.len() as u64,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.socket.reads",
                    stats.socket_reads,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.socket.writes",
                    stats.socket_writes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.tls.recv_calls",
                    stats.tls_recv_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.tls.send_calls",
                    stats.tls_send_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.body_responses",
                    stats.body_responses,
                );
                crate::runtime::perf_probe::record_value(
                    "download.owned_lane.decoded_articles",
                    stats.decoded_articles,
                );
                for result in results {
                    self.release_download_result(&result);
                    self.note_released_download_result_pending(
                        result.segment_id.file_id.job_id,
                        Self::released_download_result_lead_bytes(&result),
                    );
                    pending.push_back(result);
                }
                for work in unrequested_works {
                    self.restore_owned_lane_unrequested_work(work);
                }
            }
        }
    }

    pub(in crate::pipeline::download::worker) fn restore_owned_lane_unrequested_work(
        &mut self,
        work: DownloadWork,
    ) {
        let job_id = work.segment_id.file_id.job_id;
        self.active_downloads = self.active_downloads.saturating_sub(1);
        if work.is_recovery {
            self.active_recovery = self.active_recovery.saturating_sub(1);
        }
        self.note_download_activity(job_id);
        if let Some(in_flight) = self.active_downloads_by_job.get_mut(&job_id) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_downloads_by_job.remove(&job_id);
            }
        }
        if let Some(in_flight) = self
            .active_downloads_by_file
            .get_mut(&work.segment_id.file_id)
        {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_downloads_by_file
                    .remove(&work.segment_id.file_id);
            }
        }
        if let Err(error) = self.release_bandwidth_reservation(work.segment_id) {
            error!(error = %error, segment = %work.segment_id, "failed to release ISP bandwidth reservation for unrequested owned lane work");
        }

        if let Some(state) = self.jobs.get_mut(&job_id)
            && !is_terminal_status(&state.status)
        {
            if work.is_recovery && !work.completion_critical {
                state.recovery_queue.push(work);
            } else {
                state.download_queue.push(work);
            }
        }
        self.update_queue_metrics();
        self.publish_active_stage_metrics();
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    /// Whether a lane park has left the run loop owing a dispatch pass, and
    /// clears the debt. Consumed by the run loop and by `dispatch_downloads`
    /// itself, so a pass that has already run cannot be asked for twice.
    pub(crate) fn take_download_dispatch_wake(&mut self) -> bool {
        std::mem::take(&mut self.download_dispatch_wake)
    }

    pub(crate) fn handle_download_lane_parked(&mut self, parked: DownloadLaneParked) {
        debug!(
            job_id = parked.job_id.0,
            mode = ?parked.mode,
            reason = ?parked.reason,
            completion_critical = parked.completion_critical,
            released_connection = parked.release_connection_slot,
            "download lane parked"
        );
        if parked.release_connection_slot {
            // The connection is back in the pool now, not at the next turn of
            // the run loop.
            self.download_dispatch_wake = true;
            self.note_download_lane_released(parked.mode, parked.reason);
            self.active_download_connections = self.active_download_connections.saturating_sub(1);
            if let Some(kind) = parked.spillover_loan_kind {
                self.hot_dispatch_spillover_loans
                    .release_one(parked.job_id, kind);
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
            if parked.completion_critical {
                self.active_completion_critical_connections = self
                    .active_completion_critical_connections
                    .saturating_sub(1);
                if let Some(in_flight) = self
                    .active_completion_critical_connections_by_job
                    .get_mut(&parked.job_id)
                {
                    *in_flight = in_flight.saturating_sub(1);
                    if *in_flight == 0 {
                        self.active_completion_critical_connections_by_job
                            .remove(&parked.job_id);
                    }
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
}
