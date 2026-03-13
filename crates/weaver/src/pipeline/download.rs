use super::*;

impl Pipeline {
    fn bandwidth_reservation_estimate(decoded_bytes: u32) -> u64 {
        let decoded = decoded_bytes as u64;
        decoded.saturating_add(decoded / 16).saturating_add(1024)
    }

    fn mark_download_pass_started(&mut self, job_id: JobId) {
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
        let has_queued_work = self.jobs.get(&job_id).is_some_and(|state| {
            !state.download_queue.is_empty() || !state.recovery_queue.is_empty()
        });

        if in_flight == 0 && !has_queued_work && self.active_download_passes.remove(&job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadFinished { job_id });
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

                match weaver_yenc::decode(&work.raw, output_buf) {
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

                        let crc32 = checksum::crc32(output.as_slice());
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
                match weaver_yenc::decode(&work.raw, &mut output) {
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

                        let crc32 = checksum::crc32(&output);

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
                self.spawn_decode_task(work, None);
                continue;
            }

            let tier = weaver_core::buffer::BufferTier::for_size(work.raw.len());
            let Some(output) = self.buffers.try_acquire(tier) else {
                remaining.push_back(work);
                continue;
            };

            self.spawn_decode_task(work, Some(output));
        }

        self.pending_decode = remaining;
    }

    /// Dispatch downloads using job-sequential scheduling.
    ///
    /// Picks the first job in `job_order` that is Downloading and serves only
    /// its segments. Jobs are processed to completion before moving to the next.
    /// Within a job, primary data fills connections first, recovery blocks use
    /// spare capacity.
    pub(super) fn dispatch_downloads(&mut self) {
        if self.global_paused || self.rate_limiter.should_wait() {
            return;
        }
        if let Err(error) = self.refresh_bandwidth_cap_window() {
            error!(error = %error, "failed to refresh ISP bandwidth cap state");
            return;
        }
        if self.bandwidth_cap.cap_enabled() && self.bandwidth_cap.remaining_bytes() == 0 {
            self.update_queue_metrics();
            return;
        }

        let params = self.tuner.params();
        let decode_pending = self.metrics.decode_pending.load(Ordering::Relaxed);
        if decode_pending >= params.max_decode_queue {
            self.update_queue_metrics();
            return;
        }

        let tuner_max = params.max_concurrent_downloads;
        let max = tuner_max.min(self.connection_ramp);

        // Find the active job: first in job_order with Downloading status
        // that still has work to do. Skip jobs whose queues are empty
        // (they're waiting on retries or are effectively done).
        let active_id = self
            .job_order
            .iter()
            .find(|id| {
                self.jobs.get(id).is_some_and(|s| {
                    s.status == JobStatus::Downloading
                        && (!s.download_queue.is_empty() || !s.recovery_queue.is_empty())
                })
            })
            .copied();

        if let Some(job_id) = active_id {
            // Phase 1: Primary segments (from download_queue).
            // Recovery segments promoted for targeted repair also land here.
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
                        break;
                    }
                    Err(error) => {
                        error!(error = %error, "failed to reserve ISP bandwidth for dispatch");
                        if let Some(state) = self.jobs.get_mut(&job_id) {
                            state.download_queue.push(work);
                        }
                        break;
                    }
                }
                let estimate = work.byte_estimate as u64;
                let is_recovery = work.is_recovery;
                self.spawn_download(work, is_recovery);
                self.rate_limiter.consume(estimate);
            }

            // Recovery files are NOT downloaded proactively. They are only
            // downloaded when CRC failures trigger targeted recovery promotion, which
            // moves needed recovery segments from recovery_queue into
            // download_queue (handled by Phase 1 above).
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

        tokio::spawn(async move {
            let data = nntp
                .fetch_body_with_groups(&message_id, &groups)
                .await
                .map_err(|e| e.to_string());

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
                            };
                            self.metrics
                                .segments_retried
                                .fetch_add(1, Ordering::Relaxed);
                            let delay = std::time::Duration::from_secs(1 << (next_retry - 1));
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
