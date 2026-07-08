use super::*;

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn bandwidth_reservation_estimate(
        decoded_bytes: u32,
    ) -> u64 {
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

    pub(in crate::pipeline::download::worker) fn ensure_job_transport_profile(
        &mut self,
        job_id: JobId,
    ) -> Option<&JobTransportProfile> {
        if !self.job_transport_profiles.contains_key(&job_id) {
            let profile = self
                .jobs
                .get(&job_id)
                .map(|state| JobTransportProfile::classify(&state.spec))?;
            self.job_transport_profiles.insert(job_id, profile);
        }
        self.job_transport_profiles.get(&job_id)
    }

    pub(in crate::pipeline::download::worker) fn note_download_lane_started(
        &mut self,
        mode: DownloadLaneMode,
    ) {
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

    pub(in crate::pipeline::download::worker) fn note_download_lane_released(
        &mut self,
        mode: DownloadLaneMode,
        reason: LaneParkReason,
    ) {
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

    pub(in crate::pipeline::download::worker) fn book_failed_segment(&mut self, seg_id: SegmentId) {
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

    pub(in crate::pipeline::download::worker) fn download_lane_server_modes(
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

    pub(in crate::pipeline::download::worker) fn choose_download_lane_mode_for_server(
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

    pub(in crate::pipeline::download::worker) fn note_download_lane_mode_changed(
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

    pub(crate) fn handle_owned_download_lane_event(
        &mut self,
        event: OwnedDownloadLaneEvent,
        pending: &mut VecDeque<DownloadResult>,
    ) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.owned_lane.event");
        match event {
            OwnedDownloadLaneEvent::AcquireFailed { lease, error } => {
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
                let _ = ack.send(());
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
            if work.is_recovery {
                state.recovery_queue.push(work);
            } else {
                state.download_queue.push(work);
            }
        }
        self.update_queue_metrics();
        self.publish_active_stage_metrics();
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(crate) fn handle_download_lane_parked(&mut self, parked: DownloadLaneParked) {
        if parked.release_connection_slot {
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
