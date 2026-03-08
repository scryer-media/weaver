use super::*;

impl Pipeline {
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

        let tuner_max = self.tuner.params().max_concurrent_downloads;
        let max = tuner_max.min(self.connection_ramp);
        let recovery_slots = self.tuner.params().recovery_slots;

        // Find the active job: first in job_order with Downloading status
        // that still has work to do. Skip jobs whose queues are empty
        // (they're waiting on retries or are effectively done).
        let active_id = self.job_order.iter()
            .find(|id| {
                self.jobs.get(id)
                    .is_some_and(|s| {
                        s.status == JobStatus::Downloading
                            && (!s.download_queue.is_empty() || !s.recovery_queue.is_empty())
                    })
            })
            .copied();

        if let Some(job_id) = active_id {
            // Phase 1: Primary segments.
            while self.active_downloads < max {
                if self.rate_limiter.should_wait() { break; }
                let Some(state) = self.jobs.get_mut(&job_id) else { break };
                let Some(work) = state.download_queue.pop() else { break };
                let estimate = work.byte_estimate as u64;
                self.spawn_download(work, false);
                self.rate_limiter.consume(estimate);
            }

            // Phase 2: Recovery segments using spare bandwidth.
            while self.active_recovery < recovery_slots && self.active_downloads < max {
                if self.rate_limiter.should_wait() { break; }
                let Some(state) = self.jobs.get_mut(&job_id) else { break };
                let Some(work) = state.recovery_queue.pop() else { break };
                let estimate = work.byte_estimate as u64;
                self.spawn_download(work, true);
                self.rate_limiter.consume(estimate);
            }
        }

        self.update_queue_metrics();
    }

    /// Update shared atomic queue depth metrics from per-job queues.
    pub(super) fn update_queue_metrics(&self) {
        let (total, recovery) = self.jobs.values().fold((0usize, 0usize), |(t, r), s| {
            (t + s.download_queue.len() + s.recovery_queue.len(), r + s.recovery_queue.len())
        });
        self.metrics.download_queue_depth.store(total, Ordering::Relaxed);
        self.metrics.recovery_queue_depth.store(recovery, Ordering::Relaxed);
    }

    /// Spawn a download task for a single segment.
    pub(super) fn spawn_download(&mut self, work: DownloadWork, is_recovery: bool) {
        self.active_downloads += 1;
        if is_recovery {
            self.active_recovery += 1;
        }

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

            let _ = tx.send(DownloadResult { segment_id, data, is_recovery, retry_count }).await;
        });
    }

    /// Handle a completed download — queue for decode.
    pub(super) async fn handle_download_done(&mut self, result: DownloadResult) {
        self.active_downloads -= 1;
        if result.is_recovery {
            self.active_recovery -= 1;
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

                // Queue decode on blocking thread pool.
                let tx = self.decode_done_tx.clone();
                let segment_id = result.segment_id;
                let metrics = Arc::clone(&self.metrics);

                tokio::task::spawn_blocking(move || {
                    let mut output = vec![0u8; raw.len()];
                    match weaver_yenc::decode(&raw, &mut output) {
                        Ok(decode_result) => {
                            output.truncate(decode_result.bytes_written);
                            metrics
                                .bytes_decoded
                                .fetch_add(decode_result.bytes_written as u64, Ordering::Relaxed);
                            metrics.segments_decoded.fetch_add(1, Ordering::Relaxed);

                            let file_offset = decode_result
                                .metadata
                                .begin
                                .map(|b| b.saturating_sub(1)) // yEnc begin is 1-based
                                .unwrap_or(0);

                            let crc32 = checksum::crc32(&output);

                            let _ = tx.blocking_send(DecodeResult {
                                segment_id,
                                file_offset,
                                decoded_size: decode_result.bytes_written as u32,
                                crc_valid: decode_result.crc_valid,
                                crc32,
                                data: output,
                            });
                        }
                        Err(e) => {
                            metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                            warn!(
                                segment = %segment_id,
                                error = %e,
                                "yEnc decode failed"
                            );
                        }
                    }
                });
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
                            && let Some(seg_spec) = file_spec.segments.iter().find(|s| s.number == seg_id.segment_number) {
                                state.failed_bytes += seg_spec.bytes as u64;
                            }
                    }
                    self.check_health(job_id);
                } else {
                    // Transient failure — re-queue for retry if under limit.
                    let seg_id = result.segment_id;
                    let job_id = seg_id.file_id.job_id;
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
                                && let Some(seg_spec) = file_spec.segments.iter().find(|s| s.number == seg_id.segment_number) {
                                    state.failed_bytes += seg_spec.bytes as u64;
                                }
                        }
                        self.check_health(job_id);
                    } else if let Some(state) = self.jobs.get(&job_id) {
                        let file_idx = seg_id.file_id.file_index as usize;
                        if let Some(file_spec) = state.spec.files.get(file_idx)
                            && let Some(seg_spec) = file_spec.segments.iter().find(|s| s.number == seg_id.segment_number) {
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
    }
}
