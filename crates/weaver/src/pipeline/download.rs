use super::*;

impl Pipeline {
    pub(crate) fn note_retry_scheduled(&mut self, job_id: JobId) {
        *self.pending_retries_by_job.entry(job_id).or_default() += 1;
    }

    pub(crate) fn note_retry_requeued(&mut self, job_id: JobId) {
        let Some(pending) = self.pending_retries_by_job.get_mut(&job_id) else {
            return;
        };
        *pending = pending.saturating_sub(1);
        if *pending == 0 {
            self.pending_retries_by_job.remove(&job_id);
        }
    }

    fn bandwidth_reservation_estimate(decoded_bytes: u32) -> u64 {
        let decoded = decoded_bytes as u64;
        decoded.saturating_add(decoded / 16).saturating_add(1024)
    }

    fn mark_download_pass_started(&mut self, job_id: JobId) {
        // Transition Queued → Downloading when the first segment is dispatched.
        if let Some(state) = self.jobs.get_mut(&job_id)
            && state.status == JobStatus::Queued
        {
            state.status = JobStatus::Downloading;
            self.db_fire_and_forget(move |db| {
                if let Err(e) = db.set_active_job_status(job_id, "downloading", None) {
                    tracing::error!(error = %e, "db write failed for queued→downloading");
                }
            });
        }
        if self.active_download_passes.insert(job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadStarted { job_id });
        }
    }

    fn maybe_finish_download_pass(&mut self, job_id: JobId) {
        let in_flight = self
            .active_downloads_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0);
        let has_remaining_work = self.jobs.get(&job_id).is_some_and(|state| {
            !state.download_queue.is_empty() || !state.recovery_queue.is_empty()
        }) || self
            .pending_retries_by_job
            .get(&job_id)
            .copied()
            .unwrap_or(0)
            > 0;

        if in_flight == 0 && !has_remaining_work && self.active_download_passes.remove(&job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadFinished { job_id });
            self.schedule_job_completion_check(job_id);
        }
    }

    fn spawn_decode_task(&self, work: PendingDecodeWork, output: Option<BufferHandle>) {
        let tx = self.decode_done_tx.clone();
        let segment_id = work.segment_id;
        let metrics = Arc::clone(&self.metrics);

        tokio::task::spawn_blocking(move || {
            if let Some(mut output) = output {
                let Some(output_buf) = output.as_mut_slice() else {
                    let error = "failed to get unique pooled decode buffer".to_string();
                    metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                    warn!(segment = %segment_id, error, "yEnc decode failed");
                    let _ = tx.blocking_send(DecodeDone::Failed { segment_id, error });
                    return;
                };

                match weaver_yenc::decode_nntp(&work.raw, output_buf) {
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

                        let crc32 = decode_result.part_crc;
                        let decoded = DecodedChunk::from(output.as_slice().to_vec());

                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            crc32,
                            data: decoded,
                            yenc_name: decode_result.metadata.name,
                        }));
                    }
                    Err(e) => {
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        let _ = tx.blocking_send(DecodeDone::Failed { segment_id, error });
                    }
                }
            } else {
                let mut output = vec![0u8; work.raw.len()];
                match weaver_yenc::decode_nntp(&work.raw, &mut output) {
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

                        let crc32 = decode_result.part_crc;

                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            crc32,
                            data: DecodedChunk::from(output),
                            yenc_name: decode_result.metadata.name,
                        }));
                    }
                    Err(e) => {
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        let _ = tx.blocking_send(DecodeDone::Failed { segment_id, error });
                    }
                }
            }
        });
    }

    pub(super) fn pump_decode_queue(&mut self) {
        if self.pending_decode.is_empty() {
            return;
        }

        let mut remaining = VecDeque::with_capacity(self.pending_decode.len());
        while let Some(work) = self.pending_decode.pop_front() {
            let job_id = work.segment_id.file_id.job_id;
            if self
                .jobs
                .get(&job_id)
                .is_none_or(|state| is_terminal_status(&state.status))
            {
                self.metrics.decode_pending.fetch_sub(1, Ordering::Relaxed);
                debug!(
                    job_id = job_id.0,
                    segment = %work.segment_id,
                    "discarding queued decode work for inactive job"
                );
                continue;
            }

            if work.raw.len() > weaver_core::buffer::BufferTier::Large.size_bytes() {
                self.note_decode_started(job_id);
                self.spawn_decode_task(work, None);
                continue;
            }

            let tier = weaver_core::buffer::BufferTier::for_size(work.raw.len());
            let Some(output) = self.buffers.try_acquire(tier) else {
                remaining.push_back(work);
                continue;
            };

            self.note_decode_started(job_id);
            self.spawn_decode_task(work, Some(output));
        }

        self.pending_decode = remaining;
    }

    /// Dispatch downloads across eligible jobs with FIFO priority.
    ///
    /// Iterates `job_order` and fills connections greedily: the first eligible
    /// job gets first pick of connections, the next job fills the remainder, etc.
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
    pub(super) fn dispatch_downloads(&mut self) {
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

        let params = self.tuner.params();
        let decode_pending = self.metrics.decode_pending.load(Ordering::Relaxed);
        if decode_pending >= params.max_decode_queue {
            self.update_queue_metrics();
            if self.active_downloads == 0 {
                debug!(
                    decode_pending,
                    max = params.max_decode_queue,
                    "dispatch blocked: decode backpressure"
                );
            }
            return;
        }

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

        // When the bandwidth cap is within 15% of exhaustion, revert to
        // single-job dispatch so remaining quota goes to the highest-priority job.
        let cap_tight = self.bandwidth_cap.cap_enabled()
            && self.bandwidth_cap.remaining_bytes() <= self.bandwidth_cap.limit_bytes() * 15 / 100;

        // Collect eligible jobs in FIFO order. Earlier jobs get first pick of
        // connections; later jobs fill whatever remains.
        let eligible: Vec<JobId> = self
            .job_order
            .iter()
            .filter(|id| {
                self.jobs.get(id).is_some_and(|s| match s.status {
                    JobStatus::Queued | JobStatus::Downloading => !s.download_queue.is_empty(),
                    // Post-processing jobs may have promoted recovery segments.
                    JobStatus::QueuedExtract
                    | JobStatus::Extracting
                    | JobStatus::Verifying
                    | JobStatus::QueuedRepair
                    | JobStatus::Repairing => !s.download_queue.is_empty(),
                    _ => false,
                })
            })
            .copied()
            .take(if cap_tight { 1 } else { usize::MAX })
            .collect();

        if eligible.is_empty() && self.active_downloads == 0 {
            for (i, jid) in self.job_order.iter().enumerate() {
                if let Some(s) = self.jobs.get(jid) {
                    warn!(
                        job_id = jid.0,
                        idx = i,
                        status = ?s.status,
                        queue_len = s.download_queue.len(),
                        recovery_len = s.recovery_queue.len(),
                        "dispatch stall: job not eligible"
                    );
                }
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

        for job_id in eligible {
            if self.active_downloads >= max || self.rate_limiter.should_wait() {
                break;
            }

            while self.active_downloads < max {
                if self.rate_limiter.should_wait() {
                    break;
                }
                let Some(work) = self
                    .jobs
                    .get_mut(&job_id)
                    .and_then(|state| state.download_queue.pop())
                else {
                    break;
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
                            use weaver_scheduler::handle::{DownloadBlockKind, DownloadBlockState};
                            self.shared_state.set_download_block(DownloadBlockState {
                                kind: DownloadBlockKind::IspCap,
                                ..self
                                    .bandwidth_cap
                                    .to_download_block_state(self.global_paused)
                            });
                        }
                        // Bandwidth cap is the bottleneck — stop all dispatch.
                        self.update_queue_metrics();
                        return;
                    }
                    Err(error) => {
                        error!(error = %error, "failed to reserve ISP bandwidth for dispatch");
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.download_queue.push(work);
                        }
                        self.update_queue_metrics();
                        return;
                    }
                }
                let estimate = work.byte_estimate as u64;
                let is_recovery = work.is_recovery;
                self.spawn_download(work, is_recovery);
                self.rate_limiter.consume(estimate);
            }
        }

        self.update_queue_metrics();
    }

    /// Update shared atomic queue depth metrics from per-job queues.
    pub(super) fn update_queue_metrics(&self) {
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
    }

    /// Spawn a download task for a single segment.
    pub(super) fn spawn_download(&mut self, work: DownloadWork, is_recovery: bool) {
        let job_id = work.segment_id.file_id.job_id;
        self.active_downloads += 1;
        if is_recovery {
            self.active_recovery += 1;
        }
        *self.active_downloads_by_job.entry(job_id).or_default() += 1;
        self.mark_download_pass_started(job_id);

        let nntp = Arc::clone(&self.nntp);
        let tx = self.download_done_tx.clone();
        let segment_id = work.segment_id;
        let message_id = work.message_id.to_string();
        let groups = work.groups;
        let retry_count = work.retry_count;
        let exclude_servers = work.exclude_servers;

        tokio::spawn(async move {
            let data = if exclude_servers.is_empty() {
                nntp.fetch_body_with_groups(&message_id, &groups)
                    .await
                    .map_err(|e| e.to_string())
            } else {
                nntp.fetch_body_with_groups_excluding(&message_id, &groups, &exclude_servers)
                    .await
                    .map_err(|e| e.to_string())
            };

            let _ = tx
                .send(DownloadResult {
                    segment_id,
                    data,
                    is_recovery,
                    retry_count,
                })
                .await;
        });
    }

    /// Handle a completed download — queue for decode.
    pub(super) async fn handle_download_done(&mut self, result: DownloadResult) {
        self.active_downloads -= 1;
        if result.is_recovery {
            self.active_recovery -= 1;
        }

        let job_id = result.segment_id.file_id.job_id;
        if let Some(in_flight) = self.active_downloads_by_job.get_mut(&job_id) {
            *in_flight = in_flight.saturating_sub(1);
            if *in_flight == 0 {
                self.active_downloads_by_job.remove(&job_id);
            }
        }
        if let Err(error) = self.release_bandwidth_reservation(result.segment_id) {
            error!(error = %error, segment = %result.segment_id, "failed to release ISP bandwidth reservation");
        }
        if let Ok(raw) = &result.data
            && let Err(error) = self.record_download_bandwidth_usage(raw.len() as u64)
        {
            error!(
                error = %error,
                segment = %result.segment_id,
                "failed to record ISP bandwidth usage"
            );
        }
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

        match result.data {
            Ok(raw) => {
                let raw_size = raw.len() as u32;
                self.metrics
                    .bytes_downloaded
                    .fetch_add(raw_size as u64, Ordering::Relaxed);
                self.metrics
                    .segments_downloaded
                    .fetch_add(1, Ordering::Relaxed);

                // (Per-job byte tracking moved to handle_decode_done to use decoded size.)

                let _ = self.event_tx.send(PipelineEvent::ArticleDownloaded {
                    segment_id: result.segment_id,
                    raw_size,
                });

                self.metrics.decode_pending.fetch_add(1, Ordering::Relaxed);
                self.pending_decode.push_back(PendingDecodeWork {
                    segment_id: result.segment_id,
                    raw,
                });
                self.pump_decode_queue();
            }
            Err(e) => {
                if e.contains("no such article") || e.contains("article not found") {
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
                            && let Some(seg_spec) = file_spec
                                .segments
                                .iter()
                                .find(|s| s.number == seg_id.segment_number)
                        {
                            state.failed_bytes += seg_spec.bytes as u64;
                        }
                    }
                    self.check_health(job_id);
                } else {
                    // Transient failure — re-queue for retry if under limit.
                    let seg_id = result.segment_id;
                    let next_retry = result.retry_count + 1;

                    if next_retry > MAX_SEGMENT_RETRIES {
                        warn!(
                            segment = %seg_id,
                            error = %e,
                            retries = MAX_SEGMENT_RETRIES,
                            "segment failed permanently after max retries"
                        );
                        self.metrics
                            .segments_failed_permanent
                            .fetch_add(1, Ordering::Relaxed);
                        // Track failed bytes for health calculation.
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            let file_idx = seg_id.file_id.file_index as usize;
                            if let Some(file_spec) = state.spec.files.get(file_idx)
                                && let Some(seg_spec) = file_spec
                                    .segments
                                    .iter()
                                    .find(|s| s.number == seg_id.segment_number)
                            {
                                state.failed_bytes += seg_spec.bytes as u64;
                            }
                        }
                        self.check_health(job_id);
                    } else if let Some(state) = self.jobs.get(&job_id) {
                        let file_idx = seg_id.file_id.file_index as usize;
                        if let Some(file_spec) = state.spec.files.get(file_idx)
                            && let Some(seg_spec) = file_spec
                                .segments
                                .iter()
                                .find(|s| s.number == seg_id.segment_number)
                        {
                            let priority = file_spec.role.download_priority();
                            let work = DownloadWork {
                                segment_id: seg_id,
                                message_id: weaver_core::id::MessageId::new(&seg_spec.message_id),
                                groups: file_spec.groups.clone(),
                                priority,
                                byte_estimate: seg_spec.bytes,
                                retry_count: next_retry,
                                is_recovery: file_spec.role.is_recovery(),
                                exclude_servers: vec![],
                            };
                            self.metrics
                                .segments_retried
                                .fetch_add(1, Ordering::Relaxed);
                            let delay = std::time::Duration::from_secs(1 << (next_retry - 1));
                            self.note_retry_scheduled(job_id);
                            debug!(
                                segment = %seg_id,
                                error = %e,
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
                            error = %e,
                            "download failed (job gone, not retrying)"
                        );
                    }
                }
            }
        }

        self.maybe_finish_download_pass(job_id);
    }
}
