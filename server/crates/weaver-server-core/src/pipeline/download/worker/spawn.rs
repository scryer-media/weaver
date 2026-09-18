use super::*;

pub(in crate::pipeline) fn lane_acquire_failure_for_work(
    failure: &DownloadFailure,
    work_index: usize,
) -> DownloadFailure {
    if failure.kind == DownloadFailureKind::ServerQuota && work_index != 0 {
        DownloadFailure::new(
            DownloadFailureKind::Unrequested,
            "BODY not requested because the lease's first work exceeded server quota",
        )
    } else {
        failure.clone()
    }
}

/// Closes one decode task's wall-clock measurement, on whichever path the task
/// leaves by.
///
/// This is the single, deliberate exception to "no clock reads on a
/// per-segment path". `perf_probe::scope` already pays an unconditional
/// `Instant::now()` when the task starts; this guard adds the matching read at
/// the end and feeds the *same* `Duration` to both the profile bucket and the
/// histogram, so profiling on or off the cost is one extra clock read per
/// decode task — one per decoded article, roughly one per 750 KB of work, not
/// one per byte. Everything else on this path stays `Relaxed`-atomic only.
struct DecodeTaskTimer {
    scope: Option<crate::runtime::perf_probe::Scope>,
    metrics: Arc<crate::operations::metrics::PipelineMetrics>,
}

impl DecodeTaskTimer {
    fn start(metrics: Arc<crate::operations::metrics::PipelineMetrics>) -> Self {
        Self {
            scope: Some(crate::runtime::perf_probe::scope("download.decode.task")),
            metrics,
        }
    }
}

impl Drop for DecodeTaskTimer {
    fn drop(&mut self) {
        if let Some(scope) = self.scope.take() {
            self.metrics
                .pipeline_histograms
                .observe_decode_task(scope.finish());
        }
    }
}

fn send_blocking_decode_failure(
    tx: &mpsc::Sender<DecodeDone>,
    segment_id: SegmentId,
    raw_size: u64,
    source_server_idx: Option<usize>,
    exclude_servers: Vec<usize>,
    error: String,
) {
    let _profile_scope = crate::runtime::perf_probe::scope("download.decode.send_failure");
    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.send_failure");
    let send_started = Instant::now();
    let _ = tx.blocking_send(DecodeDone::Failed {
        segment_id,
        raw_size,
        error,
        source_server_idx,
        exclude_servers,
    });
    crate::runtime::perf_probe::record(
        "download.decode.done_channel.blocking_send",
        send_started.elapsed(),
    );
}

impl Pipeline {
    /// Decode an article that arrived as an undecoded buffer.
    ///
    /// This is the buffer-decode path, reached only from
    /// [`DownloadPayload::Raw`]. Production never takes it: the download lanes
    /// decode inline and hand back [`DownloadPayload::Decoded`], so `Raw` is
    /// constructed only by tests.
    ///
    /// It is therefore yEnc-only. The uuencode sniffer lives in the fused
    /// streaming decoder, which this path does not use, so a uuencode article
    /// routed through here would fail its decode exactly as it did before
    /// uuencode support existed. If `Raw` is ever made production-reachable
    /// again, this path needs the same sniffer the fused decoder has.
    pub(in crate::pipeline::download::worker) fn spawn_decode_task(
        &self,
        work: PendingDecodeWork,
        output: Option<BufferHandle>,
    ) {
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
        let queued_at = Instant::now();
        crate::runtime::perf_probe::record_value("download.decode.spawn_blocking.submitted", 1);
        crate::runtime::perf_probe::record_value(
            "download.decode.spawn_blocking.raw_bytes",
            raw_size,
        );

        tokio::task::spawn_blocking(move || {
            let task_entered = Instant::now();
            crate::runtime::perf_probe::record(
                "download.decode.spawn_blocking.queue_wait",
                task_entered.duration_since(queued_at),
            );
            crate::runtime::perf_probe::record(
                "download.decode.task.enter",
                Duration::from_nanos(1),
            );
            let _decode_task_timer = DecodeTaskTimer::start(Arc::clone(&metrics));
            let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.task");
            crate::runtime::affinity::pin_current_thread_for_hot_download_path();

            if let Some(mut output) = output {
                let Some(output_buf) = output.as_mut_slice() else {
                    let error = "failed to get unique pooled decode buffer".to_string();
                    metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                    warn!(segment = %segment_id, error, "yEnc decode failed");
                    send_blocking_decode_failure(
                        &tx,
                        segment_id,
                        raw_size,
                        source_server_idx,
                        exclude_servers,
                        error,
                    );
                    return;
                };

                let decode_result = {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.yenc");
                    weaver_yenc::decode_nntp(&raw, output_buf)
                };
                match decode_result {
                    Ok(decode_result) => {
                        output.set_len(decode_result.bytes_written);
                        let yenc_layout = YencLayoutAssertions {
                            file_size: decode_result.metadata.size,
                            part: decode_result.metadata.part,
                            total: decode_result.metadata.total,
                            begin: decode_result.metadata.begin,
                            end: decode_result.metadata.end,
                        };

                        let decoded = {
                            let _cpu_scope = crate::runtime::perf_probe::cpu_scope(
                                "download.decode.copy_to_owned",
                            );
                            DecodedChunk::from(output.as_slice().to_vec())
                        };

                        let _profile_scope =
                            crate::runtime::perf_probe::scope("download.decode.send_success");
                        let _cpu_scope =
                            crate::runtime::perf_probe::cpu_scope("download.decode.send_success");
                        let send_started = Instant::now();
                        let crc_valid =
                            crate::pipeline::crc_not_mismatched(decode_result.crc_status);
                        let part_crc_verified =
                            decode_result.expected_part_crc.is_some() && crc_valid;
                        let _ = tx.blocking_send(DecodeDone::Success {
                            result: DecodeResult {
                                segment_id,
                                raw_size,
                                // This buffer-decode path is yEnc-only; see the note at
                                // its call site.
                                encoding: SegmentEncoding::Yenc,
                                yenc_layout,
                                crc_valid,
                                part_crc_verified,
                                part_crc: decode_result.part_crc,
                                expected_file_crc: decode_result.expected_file_crc,
                                data: decoded,
                                yenc_name: decode_result.metadata.name,
                                checkpoint_plan: decode_result.checkpoint_plan,
                                segments: decode_result.segments,
                            },
                            source: SegmentSource {
                                source_server_idx,
                                exclude_servers,
                            },
                        });
                        crate::runtime::perf_probe::record(
                            "download.decode.done_channel.blocking_send",
                            send_started.elapsed(),
                        );
                    }
                    Err(e) => {
                        if let weaver_yenc::YencError::CrcMismatch { .. } = &e {
                            metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        send_blocking_decode_failure(
                            &tx,
                            segment_id,
                            raw_size,
                            source_server_idx,
                            exclude_servers,
                            error,
                        );
                    }
                }
            } else {
                let mut output = {
                    let _cpu_scope =
                        crate::runtime::perf_probe::cpu_scope("download.decode.alloc_vec");
                    Vec::with_capacity(raw.len())
                };
                let decode_result = {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.decode.yenc");
                    weaver_yenc::decode_nntp_append(&raw, &mut output)
                };
                match decode_result {
                    Ok(decode_result) => {
                        let yenc_layout = YencLayoutAssertions {
                            file_size: decode_result.metadata.size,
                            part: decode_result.metadata.part,
                            total: decode_result.metadata.total,
                            begin: decode_result.metadata.begin,
                            end: decode_result.metadata.end,
                        };

                        let _profile_scope =
                            crate::runtime::perf_probe::scope("download.decode.send_success");
                        let _cpu_scope =
                            crate::runtime::perf_probe::cpu_scope("download.decode.send_success");
                        let send_started = Instant::now();
                        let crc_valid =
                            crate::pipeline::crc_not_mismatched(decode_result.crc_status);
                        let part_crc_verified =
                            decode_result.expected_part_crc.is_some() && crc_valid;
                        let _ = tx.blocking_send(DecodeDone::Success {
                            result: DecodeResult {
                                segment_id,
                                raw_size,
                                // This buffer-decode path is yEnc-only; see the note at
                                // its call site.
                                encoding: SegmentEncoding::Yenc,
                                yenc_layout,
                                crc_valid,
                                part_crc_verified,
                                part_crc: decode_result.part_crc,
                                expected_file_crc: decode_result.expected_file_crc,
                                data: DecodedChunk::from(output),
                                yenc_name: decode_result.metadata.name,
                                checkpoint_plan: decode_result.checkpoint_plan,
                                segments: decode_result.segments,
                            },
                            source: SegmentSource {
                                source_server_idx,
                                exclude_servers,
                            },
                        });
                        crate::runtime::perf_probe::record(
                            "download.decode.done_channel.blocking_send",
                            send_started.elapsed(),
                        );
                    }
                    Err(e) => {
                        if let weaver_yenc::YencError::CrcMismatch { .. } = &e {
                            metrics.crc_errors.fetch_add(1, Ordering::Relaxed);
                        }
                        let error = e.to_string();
                        metrics.decode_errors.fetch_add(1, Ordering::Relaxed);
                        warn!(segment = %segment_id, error = %error, "yEnc decode failed");
                        send_blocking_decode_failure(
                            &tx,
                            segment_id,
                            raw_size,
                            source_server_idx,
                            exclude_servers,
                            error,
                        );
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
                self.note_decode_started(work.segment_id, work.raw.len() as u64);
                self.spawn_decode_task(work, None);
                available_decode_slots -= 1;
                continue;
            }

            let tier = crate::runtime::buffers::BufferTier::for_size(work.raw.len());
            let Some(output) = self.buffers.try_acquire(tier) else {
                remaining.push_back(work);
                continue;
            };

            self.note_decode_started(work.segment_id, work.raw.len() as u64);
            self.spawn_decode_task(work, Some(output));
            available_decode_slots -= 1;
        }

        remaining.extend(self.pending_decode.drain(..));
        self.pending_decode = remaining;
    }

    /// Send a lease to the download engine.
    ///
    /// There is exactly one engine: the owned blocking lanes. Every server a
    /// config can describe is lane-served — plaintext, implicit TLS, STARTTLS,
    /// and a TLS server whose preferred backend cannot build trust for it,
    /// which the rustls backend picks up — so no server needs a second
    /// download path and none exists. The only lease that leaves here by
    /// another route is one whose articles are already in the repeated-article
    /// cache, which is answered from memory rather than fetched at all.
    pub(crate) fn spawn_download_batch(&mut self, initial_lease: DownloadBatchLease) {
        if initial_lease.works.is_empty() {
            return;
        }

        if !self.repeated_articles.is_empty()
            && let Some(cache) = self.repeated_articles.get(&initial_lease.job_id)
        {
            self.spawn_repeated_download_batch(initial_lease, Arc::clone(cache));
            return;
        }

        // Every server is lane-served, so there is one download engine and one
        // place a lease can go. A pool that refuses the submission has been
        // stopped — a shutdown or a runtime reset — and that is not a verdict
        // about the articles: the works go back to the scheduler exactly as a
        // reset returns the leases it reclaims.
        if let Err(lease) = self.owned_download_lane_pool.submit(
            Arc::clone(&self.nntp),
            self.owned_download_lane_event_tx.clone(),
            self.download_refill_tx.clone(),
            self.download_lane_parked_tx.clone(),
            initial_lease,
        ) {
            warn!("owned blocking lane pool stopped; returning leased work to the scheduler");
            self.restore_stopped_owned_lane_lease(lease);
        }
    }
}
