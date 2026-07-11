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
            let _profile_scope = crate::runtime::perf_probe::scope("download.decode.task");
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
                        metrics
                            .bytes_decoded
                            .fetch_add(decode_result.bytes_written as u64, Ordering::Relaxed);
                        metrics.segments_decoded.fetch_add(1, Ordering::Relaxed);

                        let file_offset = decode_result
                            .metadata
                            .begin
                            .map(|b| b.saturating_sub(1))
                            .unwrap_or(0);

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
                        let part_crc_verified =
                            decode_result.expected_part_crc.is_some() && decode_result.crc_valid;
                        let unverified_provenance = (!part_crc_verified).then(|| {
                            Box::new(UnverifiedSegmentProvenance {
                                source_server_idx,
                                exclude_servers,
                            })
                        });
                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            unverified_provenance,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            part_crc_verified,
                            part_crc: decode_result.part_crc,
                            expected_file_crc: decode_result.expected_file_crc,
                            data: decoded,
                            yenc_name: decode_result.metadata.name,
                        }));
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
                        metrics
                            .bytes_decoded
                            .fetch_add(decode_result.bytes_written as u64, Ordering::Relaxed);
                        metrics.segments_decoded.fetch_add(1, Ordering::Relaxed);

                        let file_offset = decode_result
                            .metadata
                            .begin
                            .map(|b| b.saturating_sub(1))
                            .unwrap_or(0);

                        let _profile_scope =
                            crate::runtime::perf_probe::scope("download.decode.send_success");
                        let _cpu_scope =
                            crate::runtime::perf_probe::cpu_scope("download.decode.send_success");
                        let send_started = Instant::now();
                        let part_crc_verified =
                            decode_result.expected_part_crc.is_some() && decode_result.crc_valid;
                        let unverified_provenance = (!part_crc_verified).then(|| {
                            Box::new(UnverifiedSegmentProvenance {
                                source_server_idx,
                                exclude_servers,
                            })
                        });
                        let _ = tx.blocking_send(DecodeDone::Success(DecodeResult {
                            segment_id,
                            raw_size,
                            unverified_provenance,
                            file_offset,
                            decoded_size: decode_result.bytes_written as u32,
                            crc_valid: decode_result.crc_valid,
                            part_crc_verified,
                            part_crc: decode_result.part_crc,
                            expected_file_crc: decode_result.expected_file_crc,
                            data: DecodedChunk::from(output),
                            yenc_name: decode_result.metadata.name,
                        }));
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

    pub(crate) fn spawn_download_batch(&mut self, initial_lease: DownloadBatchLease) {
        if initial_lease.works.is_empty() {
            return;
        }

        debug_assert!(
            initial_lease
                .works
                .iter()
                .all(|work| initial_lease.compatibility.matches(work))
        );

        if self.should_use_owned_blocking_lane(&initial_lease) {
            if let Err(lease) = self.owned_download_lane_pool.submit(
                Arc::clone(&self.nntp),
                self.owned_download_lane_event_tx.clone(),
                self.download_refill_tx.clone(),
                self.download_lane_parked_tx.clone(),
                Arc::clone(&self.hot_share_yield_signal),
                initial_lease,
            ) {
                warn!("owned blocking lane pool stopped; falling back to async download lane");
                self.spawn_async_download_batch(lease);
            }
            return;
        }

        self.spawn_async_download_batch(initial_lease);
    }

    pub(in crate::pipeline::download::worker) fn spawn_async_download_batch(
        &mut self,
        initial_lease: DownloadBatchLease,
    ) {
        if initial_lease.works.is_empty() {
            return;
        }

        let nntp = Arc::clone(&self.nntp);
        let tx = self.download_done_tx.clone();
        let refill_tx = self.download_refill_tx.clone();
        let parked_tx = self.download_lane_parked_tx.clone();

        tokio::spawn(async move {
            let fetch_started = Instant::now();
            let mut lease = initial_lease;
            let mut recorded_mode = lease.lane_mode;
            let mut current_spillover_loan_kind: Option<SpilloverLoanKind>;
            let mut current_job_id: JobId;
            let park_reason: LaneParkReason;

            let mut lane = None;
            let initial_estimate = Self::bandwidth_reservation_estimate(
                lease
                    .works
                    .first()
                    .expect("download lease must contain work")
                    .byte_estimate,
            );
            let selection = nntp
                .body_server_selection_with_estimate(
                    &lease.effective_exclude_servers,
                    initial_estimate,
                )
                .await;
            let mut acquire_error = if selection.eligible.is_empty() {
                selection
                    .quota_blocked
                    .map(weaver_nntp::NntpError::quota_blocked)
            } else {
                None
            };
            for server in selection.eligible {
                match nntp
                    .acquire_body_lane(server, &lease.compatibility.groups)
                    .await
                {
                    Ok(acquired) => {
                        lane = Some(acquired);
                        break;
                    }
                    Err(error) => acquire_error = Some(error),
                }
            }

            let Some(mut lane) = lane else {
                let failure = DownloadFailure::from_lane_acquire_failure(acquire_error.as_ref());
                let policy_blocked = failure.kind == DownloadFailureKind::ServerQuota;
                let is_recovery = lease.compatibility.is_recovery;
                let exclude_servers = lease.compatibility.exclude_servers.clone();
                let mode = lease.lane_mode;
                let spillover_loan_kind = lease.spillover_loan_kind;
                let job_id = lease.job_id;
                for (work_index, work) in lease.works.into_iter().enumerate() {
                    let work_failure = lane_acquire_failure_for_work(&failure, work_index);
                    let policy_outcome = matches!(
                        work_failure.kind,
                        DownloadFailureKind::ServerQuota | DownloadFailureKind::Unrequested
                    );
                    let _ = tx
                        .send(DownloadResult {
                            segment_id: work.segment_id,
                            data: Err(DownloadError::Fetch(work_failure)),
                            attempts: Vec::new(),
                            lane_observation: Some(DownloadLaneObservation {
                                server_idx: None,
                                mode,
                                supports_pipelining: false,
                                rtt: None,
                                batch_complete: true,
                                batch_clean: policy_outcome,
                                batch_response_count: 0,
                                unresolved_count: u64::from(!policy_outcome),
                                connection_discarded: false,
                            }),
                            source_server_idx: None,
                            origin: DownloadResultOrigin::from_recovery(is_recovery),
                            retry_count: work.retry_count,
                            exclude_servers: exclude_servers.clone(),
                            release_connection_slot: false,
                        })
                        .await;
                }
                let _ = parked_tx
                    .send(DownloadLaneParked {
                        job_id,
                        mode,
                        spillover_loan_kind,
                        reason: if policy_blocked {
                            LaneParkReason::ServerQuota
                        } else {
                            LaneParkReason::Error
                        },
                        release_connection_slot: true,
                        release_ip_replacement_burst: false,
                    })
                    .await;
                crate::runtime::perf_probe::record("download.fetch_body", fetch_started.elapsed());
                return;
            };

            loop {
                let DownloadBatchLease {
                    job_id,
                    lane_mode,
                    spillover_loan_kind,
                    server_modes,
                    compatibility,
                    effective_exclude_servers: _,
                    works,
                } = lease;
                current_job_id = job_id;
                current_spillover_loan_kind = spillover_loan_kind;
                let server_idx = lane.server_id().0;
                let supports_pipelining = lane.supports_pipelining();
                let actual_mode = Self::actual_download_lane_mode(
                    lane_mode,
                    &server_modes,
                    server_idx,
                    supports_pipelining,
                );
                let is_recovery = compatibility.is_recovery;
                let exclude_servers = compatibility.exclude_servers.clone();
                let mut batch_clean_for_refill = true;
                let mut policy_blocked_for_refill = false;
                let mut pending_works: std::collections::VecDeque<DownloadWork> =
                    works.into_iter().collect();

                while let Some(first_work) = pending_works.pop_front() {
                    let batch_depth = actual_mode.max_depth();
                    let mut batch_works = Vec::with_capacity(batch_depth);
                    batch_works.push(first_work);
                    while batch_works.len() < batch_depth {
                        let Some(work) = pending_works.pop_front() else {
                            break;
                        };
                        batch_works.push(work);
                    }

                    let message_ids: Vec<String> = batch_works
                        .iter()
                        .map(|work| work.message_id.to_string())
                        .collect();
                    let total = batch_works.len();
                    let mut completed = 0usize;
                    let mut works_by_index: Vec<Option<DownloadWork>> =
                        batch_works.into_iter().map(Some).collect();

                    match actual_mode {
                        DownloadLaneMode::Sequential => {
                            for (idx, message_id) in message_ids.iter().enumerate() {
                                let estimate = Self::bandwidth_reservation_estimate(
                                    works_by_index[idx]
                                        .as_ref()
                                        .expect("download work exists until its BODY result")
                                        .byte_estimate,
                                );
                                let trace = lane
                                    .fetch_decoded_sequential_with_estimate(message_id, estimate)
                                    .await;
                                completed += 1;
                                let work = works_by_index[idx]
                                    .take()
                                    .expect("download lane result emitted once per work item");
                                let segment_id = work.segment_id;
                                let retry_count = work.retry_count;
                                let (data, attempts, source_server_idx) =
                                    Self::download_data_from_decoded_trace(segment_id, trace);
                                let policy_outcome = matches!(
                                    &data,
                                    Err(DownloadError::Fetch(failure))
                                        if matches!(
                                            failure.kind,
                                            DownloadFailureKind::ServerQuota
                                                | DownloadFailureKind::Unrequested
                                        )
                                );
                                let batch_clean = data.is_ok() || policy_outcome;
                                batch_clean_for_refill &= batch_clean;
                                policy_blocked_for_refill |= policy_outcome;
                                let observation = DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: DownloadLaneMode::Sequential,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean,
                                    batch_response_count: 1,
                                    unresolved_count: 0,
                                    connection_discarded: !batch_clean,
                                };
                                let _ = tx
                                    .send(DownloadResult {
                                        segment_id,
                                        data,
                                        attempts,
                                        lane_observation: Some(observation),
                                        source_server_idx,
                                        origin: DownloadResultOrigin::from_recovery(is_recovery),
                                        retry_count,
                                        exclude_servers: exclude_servers.clone(),
                                        release_connection_slot: false,
                                    })
                                    .await;
                            }
                        }
                        DownloadLaneMode::PipelineDepth2 | DownloadLaneMode::PipelineDepth4 => {
                            let rtt = lane.rtt_ewma();
                            let tx_for_trace = tx.clone();
                            let exclude_servers_for_trace = exclude_servers.clone();
                            let estimated_body_bytes = works_by_index
                                .iter()
                                .map(|work| {
                                    Self::bandwidth_reservation_estimate(
                                        work.as_ref()
                                            .expect("download work exists before BODY issue")
                                            .byte_estimate,
                                    )
                                })
                                .collect::<Vec<_>>();
                            let stats = if actual_mode == DownloadLaneMode::PipelineDepth4 {
                                lane.fetch_decoded_pipeline_depth4_with_estimates(
                                    &message_ids,
                                    &estimated_body_bytes,
                                    |idx, trace, meta| {
                                        completed += 1;
                                        let work = works_by_index[idx].take().expect(
                                            "download lane result emitted once per work item",
                                        );
                                        let segment_id = work.segment_id;
                                        let retry_count = work.retry_count;
                                        let (data, attempts, source_server_idx) =
                                            Self::download_data_from_decoded_trace(
                                                segment_id, trace,
                                            );
                                        policy_blocked_for_refill |= matches!(
                                            &data,
                                            Err(DownloadError::Fetch(failure))
                                                if matches!(
                                                    failure.kind,
                                                    DownloadFailureKind::ServerQuota
                                                        | DownloadFailureKind::Unrequested
                                                )
                                        );
                                        let observation = DownloadLaneObservation {
                                            server_idx: Some(server_idx),
                                            mode: actual_mode,
                                            supports_pipelining,
                                            rtt,
                                            batch_complete: meta.batch_complete,
                                            batch_clean: meta.batch_clean,
                                            batch_response_count: meta.batch_response_count,
                                            unresolved_count: meta.unresolved_count,
                                            connection_discarded: meta.connection_discarded,
                                        };
                                        let tx = tx_for_trace.clone();
                                        let exclude_servers = exclude_servers_for_trace.clone();
                                        async move {
                                            let _ = tx
                                                .send(DownloadResult {
                                                    segment_id,
                                                    data,
                                                    attempts,
                                                    lane_observation: Some(observation),
                                                    source_server_idx,
                                                    origin: DownloadResultOrigin::from_recovery(
                                                        is_recovery,
                                                    ),
                                                    retry_count,
                                                    exclude_servers,
                                                    release_connection_slot: false,
                                                })
                                                .await;
                                        }
                                    },
                                )
                                .await
                            } else {
                                lane.fetch_decoded_pipeline_depth2_with_estimates(
                                    &message_ids,
                                    &estimated_body_bytes,
                                    |idx, trace, meta| {
                                        completed += 1;
                                        let work = works_by_index[idx].take().expect(
                                            "download lane result emitted once per work item",
                                        );
                                        let segment_id = work.segment_id;
                                        let retry_count = work.retry_count;
                                        let (data, attempts, source_server_idx) =
                                            Self::download_data_from_decoded_trace(
                                                segment_id, trace,
                                            );
                                        policy_blocked_for_refill |= matches!(
                                            &data,
                                            Err(DownloadError::Fetch(failure))
                                                if matches!(
                                                    failure.kind,
                                                    DownloadFailureKind::ServerQuota
                                                        | DownloadFailureKind::Unrequested
                                                )
                                        );
                                        let observation = DownloadLaneObservation {
                                            server_idx: Some(server_idx),
                                            mode: actual_mode,
                                            supports_pipelining,
                                            rtt,
                                            batch_complete: meta.batch_complete,
                                            batch_clean: meta.batch_clean,
                                            batch_response_count: meta.batch_response_count,
                                            unresolved_count: meta.unresolved_count,
                                            connection_discarded: meta.connection_discarded,
                                        };
                                        let tx = tx_for_trace.clone();
                                        let exclude_servers = exclude_servers_for_trace.clone();
                                        async move {
                                            let _ = tx
                                                .send(DownloadResult {
                                                    segment_id,
                                                    data,
                                                    attempts,
                                                    lane_observation: Some(observation),
                                                    source_server_idx,
                                                    origin: DownloadResultOrigin::from_recovery(
                                                        is_recovery,
                                                    ),
                                                    retry_count,
                                                    exclude_servers,
                                                    release_connection_slot: false,
                                                })
                                                .await;
                                        }
                                    },
                                )
                                .await
                            };

                            let batch_clean = !stats.connection_discarded
                                && !stats.response_order_mismatch
                                && stats.unresolved == 0;
                            batch_clean_for_refill &= batch_clean;
                        }
                    }

                    let unresolved_count = total.saturating_sub(completed);
                    if unresolved_count > 0 {
                        batch_clean_for_refill = false;
                    }
                    for work in works_by_index.into_iter().flatten() {
                        let _ = tx
                            .send(DownloadResult {
                                segment_id: work.segment_id,
                                data: Err(DownloadError::Fetch(DownloadFailure::new(
                                    DownloadFailureKind::Transient,
                                    "batch ended without result",
                                ))),
                                attempts: Vec::new(),
                                lane_observation: Some(DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: actual_mode,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean: false,
                                    batch_response_count: completed as u64,
                                    unresolved_count: unresolved_count as u64,
                                    connection_discarded: true,
                                }),
                                source_server_idx: None,
                                origin: DownloadResultOrigin::from_recovery(is_recovery),
                                retry_count: work.retry_count,
                                exclude_servers: exclude_servers.clone(),
                                release_connection_slot: false,
                            })
                            .await;
                    }

                    if !batch_clean_for_refill || policy_blocked_for_refill {
                        break;
                    }
                }

                if !batch_clean_for_refill || policy_blocked_for_refill {
                    let policy_only = batch_clean_for_refill && policy_blocked_for_refill;
                    let tail_count = pending_works.len() as u64;
                    for work in pending_works {
                        let _ = tx
                            .send(DownloadResult {
                                segment_id: work.segment_id,
                                data: Err(DownloadError::Fetch(DownloadFailure::new(
                                    DownloadFailureKind::Unrequested,
                                    "lane parked before leased article was requested",
                                ))),
                                attempts: Vec::new(),
                                lane_observation: Some(DownloadLaneObservation {
                                    server_idx: Some(server_idx),
                                    mode: actual_mode,
                                    supports_pipelining,
                                    rtt: lane.rtt_ewma(),
                                    batch_complete: true,
                                    batch_clean: policy_only,
                                    batch_response_count: 0,
                                    unresolved_count: if policy_only { 0 } else { tail_count },
                                    connection_discarded: !policy_only,
                                }),
                                source_server_idx: None,
                                origin: DownloadResultOrigin::from_recovery(is_recovery),
                                retry_count: work.retry_count,
                                exclude_servers: exclude_servers.clone(),
                                release_connection_slot: false,
                            })
                            .await;
                    }
                    park_reason = if policy_only {
                        LaneParkReason::ServerQuota
                    } else {
                        LaneParkReason::Error
                    };
                    break;
                }

                let (response_tx, response_rx) = tokio::sync::oneshot::channel();
                if refill_tx
                    .send(DownloadLaneRefillRequest {
                        job_id,
                        server_idx,
                        remote_ip: lane.remote_ip(),
                        supports_pipelining,
                        current_mode: recorded_mode,
                        spillover_loan_kind,
                        compatibility,
                        response_tx,
                    })
                    .await
                    .is_err()
                {
                    park_reason = LaneParkReason::Error;
                    break;
                }

                match tokio::time::timeout(LANE_REFILL_GRACE, response_rx).await {
                    Ok(Ok(response)) => {
                        if let Some(next_lease) = response.lease
                            && !next_lease.works.is_empty()
                        {
                            recorded_mode = Self::actual_download_lane_mode(
                                next_lease.lane_mode,
                                &next_lease.server_modes,
                                server_idx,
                                supports_pipelining,
                            );
                            lease = next_lease;
                            continue;
                        }
                        park_reason = response.park_reason;
                        break;
                    }
                    _ => {
                        park_reason = LaneParkReason::ProbeYield;
                        break;
                    }
                }
            }

            lane.park();
            let _ = parked_tx
                .send(DownloadLaneParked {
                    job_id: current_job_id,
                    mode: recorded_mode,
                    spillover_loan_kind: current_spillover_loan_kind,
                    reason: park_reason,
                    release_connection_slot: true,
                    release_ip_replacement_burst: false,
                })
                .await;
            crate::runtime::perf_probe::record("download.fetch_body", fetch_started.elapsed());
        });
    }
}
