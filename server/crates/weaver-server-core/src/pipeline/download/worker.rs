use super::*;
use weaver_nntp::client::FetchAttemptOutcome;

enum DispatchAttempt {
    Dispatched,
    NoWork,
    StopAll,
}

const DOWNLOAD_PRESSURE_SOFT_PERCENT: u64 = 70;
const SOFT_PRESSURE_DISPATCH_MAX_DELAY: Duration = Duration::from_millis(150);
const SOFT_PRESSURE_DISPATCH_MIN_DELAY: Duration = Duration::from_millis(1);
const SAB_BODY_PIPELINE_DEPTH: usize = 2;

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

    fn try_dispatch_download_for_job(&mut self, job_id: JobId) -> DispatchAttempt {
        let Some(work) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| state.download_queue.pop())
        else {
            return DispatchAttempt::NoWork;
        };

        let reservation_estimate = Self::bandwidth_reservation_estimate(work.byte_estimate);
        match self.reserve_bandwidth_for_dispatch(work.segment_id, reservation_estimate) {
            Ok(true) => {}
            Ok(false) => {
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                // Remaining bytes are non-zero but too small for any
                // segment — force the UI to show the cap as the blocker.
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
                return DispatchAttempt::StopAll;
            }
            Err(error) => {
                error!(error = %error, "failed to reserve ISP bandwidth for dispatch");
                if let Some(state) = self.jobs.get_mut(&job_id) {
                    state.download_queue.push(work);
                }
                self.update_queue_metrics();
                return DispatchAttempt::StopAll;
            }
        }

        let mut works = vec![work];
        if works.len() < SAB_BODY_PIPELINE_DEPTH
            && let Some(next) = self.jobs.get_mut(&job_id).and_then(|state| {
                state
                    .download_queue
                    .pop_next_pipelining_compatible_with(&works[0])
            })
        {
            let reservation_estimate = Self::bandwidth_reservation_estimate(next.byte_estimate);
            match self.reserve_bandwidth_for_dispatch(next.segment_id, reservation_estimate) {
                Ok(true) => works.push(next),
                Ok(false) => {
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.download_queue.push(next);
                    }
                }
                Err(error) => {
                    error!(error = %error, "failed to reserve ISP bandwidth for pipelined dispatch");
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        state.download_queue.push(next);
                    }
                }
            }
        }

        let reservations: Vec<_> = works
            .iter()
            .map(|work| (work.segment_id, work.byte_estimate as u64))
            .collect();
        let is_recovery = works[0].is_recovery;
        self.spawn_download_batch(works, is_recovery);
        for (segment_id, estimate) in reservations {
            self.reserve_rate_limit_for_dispatch(segment_id, estimate);
        }
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
            let _profile_scope = crate::runtime::perf_probe::scope("download.decode.task");
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();

            let send_decode_failure = |error: String| {
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

                match weaver_yenc::decode_nntp(&raw, output_buf) {
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

                        let decoded = DecodedChunk::from(output.as_slice().to_vec());

                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
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
                let mut output = vec![0u8; raw.len()];
                match weaver_yenc::decode_nntp(&raw, &mut output) {
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

                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
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
    /// This enables two key behaviors:
    /// - **Prefetch**: when the primary job enters extraction, the next job
    ///   immediately inherits all connections.
    /// - **Tail overlap**: when the primary job has fewer queued segments than
    ///   available connections, the next job fills the spare slots.
    ///
    /// When a bandwidth cap is enabled and within 15% of exhaustion, reverts
    /// to single-job dispatch to avoid wasting scarce remaining quota on
    /// lower-priority work.
    pub(crate) fn dispatch_downloads(&mut self) {
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
                return;
            }
            self.download_pressure_soft_dispatch_after = Some(now + delay);
            dispatch_budget = 1;
        } else {
            self.download_pressure_soft_dispatch_after = None;
        }

        let params = self.tuner.params();
        let tuner_max = params.max_concurrent_downloads;
        // Reserve one connection slot per active health probe so the probe
        // task can acquire a pool permit without being starved by downloads.
        let active_probes = self
            .jobs
            .values()
            .filter(|s| matches!(s.status, JobStatus::Checking))
            .count();
        let max = tuner_max
            .min(self.connection_ramp)
            .saturating_sub(active_probes);

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

        let Some((hot_priority, _, mut hot_job_id)) = eligible.first().copied() else {
            self.update_queue_metrics();
            return;
        };

        let top_band_end = eligible
            .iter()
            .position(|(priority, _, _)| *priority != hot_priority)
            .unwrap_or(eligible.len());
        let mut hot_active = self
            .active_downloads_by_job
            .get(&hot_job_id)
            .copied()
            .unwrap_or(0);
        for (_, _, candidate_job_id) in &eligible[..top_band_end] {
            let candidate_active = self
                .active_downloads_by_job
                .get(candidate_job_id)
                .copied()
                .unwrap_or(0);
            if candidate_active > hot_active {
                hot_active = candidate_active;
                hot_job_id = *candidate_job_id;
            }
        }

        while self.active_download_connections < max
            && !self.rate_limiter.should_wait()
            && dispatch_budget > 0
        {
            match self.try_dispatch_download_for_job(hot_job_id) {
                DispatchAttempt::Dispatched => dispatch_budget = dispatch_budget.saturating_sub(1),
                DispatchAttempt::NoWork => break,
                DispatchAttempt::StopAll => return,
            }
        }

        if !suppress_spillover && !bandwidth_cap_tight && dispatch_budget > 0 {
            for (_, _, job_id) in eligible {
                if job_id == hot_job_id {
                    continue;
                }
                while self.active_download_connections < max
                    && !self.rate_limiter.should_wait()
                    && dispatch_budget > 0
                {
                    match self.try_dispatch_download_for_job(job_id) {
                        DispatchAttempt::Dispatched => {
                            dispatch_budget = dispatch_budget.saturating_sub(1)
                        }
                        DispatchAttempt::NoWork => break,
                        DispatchAttempt::StopAll => return,
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

        self.update_queue_metrics();
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
        let source_server_idx = trace.attempts.iter().rev().find_map(|attempt| {
            matches!(
                attempt.outcome,
                weaver_nntp::client::FetchAttemptOutcome::Success
            )
            .then_some(attempt.server_idx)
        });
        let data = match trace.result {
            Ok(decoded) => {
                let weaver_nntp::client::DecodedBody {
                    raw_size,
                    decoded,
                    result,
                } = decoded;
                let file_offset = result
                    .metadata
                    .begin
                    .map(|b| b.saturating_sub(1))
                    .unwrap_or(0);

                Ok(DownloadPayload::Decoded(DecodeResult {
                    segment_id,
                    raw_size: raw_size as u64,
                    file_offset,
                    decoded_size: result.bytes_written as u32,
                    crc_valid: result.crc_valid,
                    expected_file_crc: result.expected_file_crc,
                    data: DecodedChunk::from(decoded),
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

    pub(crate) fn spawn_download_batch(&mut self, works: Vec<DownloadWork>, is_recovery: bool) {
        if works.is_empty() {
            return;
        }

        debug_assert!(works.iter().all(|work| work.is_recovery == is_recovery
            && work.groups == works[0].groups
            && work.exclude_servers == works[0].exclude_servers));

        let job_id = works[0].segment_id.file_id.job_id;
        self.active_downloads += works.len();
        self.active_download_connections += 1;
        if is_recovery {
            self.active_recovery += works.len();
        }
        *self.active_downloads_by_job.entry(job_id).or_default() += works.len();
        *self
            .active_download_connections_by_job
            .entry(job_id)
            .or_default() += 1;
        for work in &works {
            *self
                .active_downloads_by_file
                .entry(work.segment_id.file_id)
                .or_default() += 1;
        }
        self.mark_download_pass_started(job_id);
        self.publish_active_stage_metrics();

        let nntp = Arc::clone(&self.nntp);
        let tx = self.download_done_tx.clone();
        let groups = works[0].groups.clone();
        let exclude_servers = works[0].exclude_servers.clone();
        let message_ids: Vec<String> = works
            .iter()
            .map(|work| work.message_id.to_string())
            .collect();

        tokio::spawn(async move {
            let total = works.len();
            let mut completed = 0usize;
            let mut works_by_index: Vec<Option<DownloadWork>> =
                works.into_iter().map(Some).collect();
            let fetch_started = Instant::now();
            nntp.fetch_bodies_decoded_with_groups_prefer_excluding_traced_each(
                &message_ids,
                &groups,
                &exclude_servers,
                |idx, trace| {
                    completed += 1;
                    let release_connection_slot = completed == total;
                    let work = works_by_index[idx]
                        .take()
                        .expect("download batch result emitted once per work item");
                    let tx = tx.clone();
                    let exclude_servers = exclude_servers.clone();
                    async move {
                        let segment_id = work.segment_id;
                        let retry_count = work.retry_count;
                        let (data, attempts, source_server_idx) =
                            Self::download_data_from_decoded_trace(segment_id, trace);
                        let _ = tx
                            .send(DownloadResult {
                                segment_id,
                                data,
                                attempts,
                                source_server_idx,
                                is_recovery,
                                retry_count,
                                exclude_servers,
                                release_connection_slot,
                            })
                            .await;
                    }
                },
            )
            .await;
            crate::runtime::perf_probe::record("download.fetch_body", fetch_started.elapsed());

            for work in works_by_index.into_iter().flatten() {
                let segment_id = work.segment_id;
                let retry_count = work.retry_count;
                let _ = tx
                    .send(DownloadResult {
                        segment_id,
                        data: Err(DownloadError::Fetch(DownloadFailure {
                            kind: DownloadFailureKind::Transient,
                            message: "batch ended without result".to_string(),
                        })),
                        attempts: Vec::new(),
                        source_server_idx: None,
                        is_recovery,
                        retry_count,
                        exclude_servers: exclude_servers.clone(),
                        release_connection_slot: completed + 1 == total,
                    })
                    .await;
                completed += 1;
            }
        });
    }

    /// Handle a completed download — queue for decode.
    pub(crate) fn release_download_result(&mut self, result: &DownloadResult) {
        self.active_downloads = self.active_downloads.saturating_sub(1);
        if result.release_connection_slot {
            self.active_download_connections = self.active_download_connections.saturating_sub(1);
            let job_id = result.segment_id.file_id.job_id;
            if let Some(in_flight) = self.active_download_connections_by_job.get_mut(&job_id) {
                *in_flight = in_flight.saturating_sub(1);
                if *in_flight == 0 {
                    self.active_download_connections_by_job.remove(&job_id);
                }
            }
        }
        if result.is_recovery {
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
        self.reconcile_rate_limit_for_download(result.segment_id, actual_raw_bytes);
        if let Some(raw_bytes) = actual_raw_bytes {
            if let Err(error) = self.record_download_bandwidth_usage(raw_bytes) {
                error!(
                    error = %error,
                    segment = %result.segment_id,
                    "failed to record ISP bandwidth usage"
                );
            }
        }
    }

    pub(crate) async fn handle_download_done(&mut self, result: DownloadResult) {
        self.release_download_result(&result);
        self.process_download_done(result).await;
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

        let excluded_servers = result.exclude_servers.clone();
        let source_server_idx = result.source_server_idx;

        for (attempt_index, attempt) in result.attempts.iter().enumerate() {
            let outcome = match attempt.outcome {
                FetchAttemptOutcome::Success => crate::events::model::ServerAttemptOutcome::Success,
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
                is_recovery: result.is_recovery,
            });
        }

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
                match &failure.kind {
                    DownloadFailureKind::ArticleNotFound => self
                        .metrics
                        .download_failures_article_not_found
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::CapacityUnavailable => self
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
                if failure.kind == DownloadFailureKind::ArticleNotFound
                    && excluded_servers.is_empty()
                {
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
                    let next_retry = if capacity_unavailable {
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
                                exclude_servers: excluded_servers.clone(),
                            };
                            self.metrics
                                .segments_retried
                                .fetch_add(1, Ordering::Relaxed);
                            let delay = if capacity_unavailable {
                                std::time::Duration::from_secs(1)
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
