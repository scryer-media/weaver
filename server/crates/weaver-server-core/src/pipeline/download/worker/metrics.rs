use super::*;

/// Record checkpoint geometry after an article has finished decoding. This is
/// intentionally a completion-side probe: disabled profiling does one cached
/// enable check and the decoder's cut loop stays free of locks, clocks, and
/// metric allocations.
fn record_checkpoint_observability(result: &weaver_yenc::DecodeResult) {
    if !crate::runtime::perf_probe::enabled() {
        return;
    }

    let shape = match &result.checkpoint_plan {
        weaver_yenc::CheckpointPlan::None => "download.checkpoint_plan.none",
        weaver_yenc::CheckpointPlan::Single(_) => "download.checkpoint_plan.single",
        weaver_yenc::CheckpointPlan::Multi(_) => "download.checkpoint_plan.multi",
    };
    crate::runtime::perf_probe::record_value(shape, 1);
    crate::runtime::perf_probe::record_value(
        "download.checkpoint_plan.grid_count",
        result.checkpoint_plan.grid_count() as u64,
    );
    crate::runtime::perf_probe::record_value(
        "download.checkpoint_segments.emitted",
        result.segments.len() as u64,
    );

    let Some(reason) = result.checkpoint_collapse_reason else {
        return;
    };
    let label = match reason {
        weaver_yenc::CheckpointCollapseReason::SegmentCount => {
            "download.checkpoint_collapse.segment_count"
        }
        weaver_yenc::CheckpointCollapseReason::SegmentStorage => {
            "download.checkpoint_collapse.segment_storage"
        }
        weaver_yenc::CheckpointCollapseReason::OfferWork => {
            "download.checkpoint_collapse.offer_work"
        }
        weaver_yenc::CheckpointCollapseReason::BoundaryOverflow => {
            "download.checkpoint_collapse.boundary_overflow"
        }
        weaver_yenc::CheckpointCollapseReason::MissingBoundary => {
            "download.checkpoint_collapse.missing_boundary"
        }
        weaver_yenc::CheckpointCollapseReason::NonAdvancingBoundary => {
            "download.checkpoint_collapse.nonadvancing_boundary"
        }
        weaver_yenc::CheckpointCollapseReason::OffsetOverflow => {
            "download.checkpoint_collapse.offset_overflow"
        }
    };
    crate::runtime::perf_probe::record_value(label, 1);
}

impl Pipeline {
    pub(in crate::pipeline::download::worker) fn log_download_dispatch_liveness_stall(
        &mut self,
        now: Instant,
        pressure: DownloadPressure,
        max_connections: usize,
        eligible_count: usize,
    ) {
        if self.active_downloads != 0 || self.active_download_connections != 0 {
            return;
        }

        let queue_depth = self
            .jobs
            .values()
            .map(|state| state.download_queue.len() + state.recovery_queue.len())
            .sum::<usize>();
        if queue_depth == 0 {
            self.last_download_dispatch_stall_log_at = None;
            return;
        }
        if self
            .last_download_dispatch_stall_log_at
            .is_some_and(|last| {
                now.saturating_duration_since(last) < DOWNLOAD_DISPATCH_STALL_LOG_INTERVAL
            })
        {
            return;
        }

        self.last_download_dispatch_stall_log_at = Some(now);
        let hot_job_id = self.hot_dispatch_job.map(|id| id.0).unwrap_or_default();
        if let Some(job_id) = self.job_order.iter().copied().find(|job_id| {
            self.jobs.get(job_id).is_some_and(|state| {
                !state.download_queue.is_empty() || !state.recovery_queue.is_empty()
            })
        }) {
            let state = self
                .jobs
                .get(&job_id)
                .expect("job from job_order must exist while logging dispatch stall");
            let durable_retry_ms = self
                .download_restart_durable_lead_retry_after
                .get(&job_id)
                .map(|ready_at| ready_at.saturating_duration_since(now).as_millis() as u64)
                .unwrap_or_default();
            let backlog = self.download_pipeline_backlog_for_job(job_id);
            warn!(
                job_id = job_id.0,
                status = ?state.status,
                download_queue_len = state.download_queue.len(),
                recovery_queue_len = state.recovery_queue.len(),
                status_allows_dispatch = Self::status_allows_download_dispatch(&state.status),
                eligible_count,
                queue_depth,
                active_downloads = self.active_downloads,
                active_connections = self.active_download_connections,
                max_connections,
                pressure_state = pressure.state.as_str(),
                pressure_reason = pressure.reason.as_str(),
                hot_job_id,
                restart_durable_lead_retry_ms = durable_retry_ms,
                active_downloads_by_job = backlog.active_downloads,
                active_connections_by_job = backlog.active_connections,
                active_decodes_by_job = backlog.active_decodes,
                delayed_retries_by_job = backlog.delayed_retries,
                released_results_by_job = backlog.released_results,
                released_result_bytes_by_job = backlog.released_result_bytes,
                pending_decodes_by_job = backlog.pending_decodes,
                buffered_write_segments_by_job = backlog.buffered_write_segments,
                buffered_write_bytes_by_job = backlog.buffered_write_bytes,
                "download dispatch liveness stall: queued work but no active downloads"
            );
        } else {
            warn!(
                eligible_count,
                queue_depth,
                active_downloads = self.active_downloads,
                active_connections = self.active_download_connections,
                max_connections,
                pressure_state = pressure.state.as_str(),
                pressure_reason = pressure.reason.as_str(),
                hot_job_id,
                "download dispatch liveness stall: queued work but no active downloads"
            );
        }
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

    pub(in crate::pipeline::download) fn download_data_from_decoded_trace(
        segment_id: SegmentId,
        trace: weaver_nntp::client::DecodedBodyTrace,
    ) -> (
        std::result::Result<DownloadPayload, DownloadError>,
        Vec<weaver_nntp::client::FetchAttemptTrace>,
        Option<usize>,
    ) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.result.translate");
        let successful_server_idx = trace.attempts.iter().rev().find_map(|attempt| {
            matches!(
                attempt.outcome,
                weaver_nntp::client::FetchAttemptOutcome::Success
            )
            .then_some(attempt.server_idx)
        });
        let attempted_server_idx = trace
            .attempts
            .iter()
            .rev()
            .map(|attempt| attempt.server_idx)
            .next();
        let source_server_idx = successful_server_idx.or(attempted_server_idx);
        let data = match trace.result {
            Ok(decoded) => {
                let weaver_nntp::client::DecodedBody {
                    raw_size,
                    decoded,
                    body,
                    cpu,
                    io,
                } = decoded;
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.parse_decode",
                    cpu.raw_decode,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.response_line",
                    cpu.response_line,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.yenc_header",
                    cpu.yenc_header,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.body_decode",
                    cpu.body_decode,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.yend_line",
                    cpu.yend_line,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.nntp_terminator",
                    cpu.nntp_terminator,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.nntp.read_poll",
                    cpu.read_poll,
                );
                crate::runtime::perf_probe::record_cpu_sample(
                    "download.fused.output_callback",
                    cpu.feed,
                );
                crate::runtime::perf_probe::record_cpu_sample("download.fused.finish", cpu.finish);
                crate::runtime::perf_probe::record_value("download.nntp.read.bytes", io.read_bytes);
                crate::runtime::perf_probe::record_value("download.nntp.read.calls", io.read_calls);
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.readiness_waits",
                    io.transport_read.readiness_waits,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.empty_readiness_wakes",
                    io.transport_read.empty_readiness_wakes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.calls",
                    io.transport_read.try_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.would_block",
                    io.transport_read.try_read_would_block,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.try_read.bytes",
                    io.transport_read.try_read_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.tls.read_calls",
                    io.transport_read.tls_read_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.tls.process_packets_calls",
                    io.transport_read.tls_process_packets_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.drain_calls",
                    io.transport_read.plaintext_drain_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.reader_calls",
                    io.transport_read.plaintext_reader_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.reader_would_block",
                    io.transport_read.plaintext_reader_would_block,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.bytes",
                    io.transport_read.plaintext_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.plaintext.cached_returns",
                    io.transport_read.cached_plaintext_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.recv_calls",
                    io.transport_read.backend_recv_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.recv_bytes",
                    io.transport_read.backend_recv_bytes,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.target_full_returns",
                    io.transport_read.backend_target_full_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.pending_empty_returns",
                    io.transport_read.backend_pending_empty_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.pending_after_bytes_returns",
                    io.transport_read.backend_pending_after_bytes_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.zero_returns",
                    io.transport_read.backend_zero_returns,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.transport.backend.bytes_per_recv_call",
                    io.transport_read
                        .backend_recv_bytes
                        .checked_div(io.transport_read.backend_recv_calls)
                        .unwrap_or(0),
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.read.bytes_per_call",
                    io.read_bytes.checked_div(io.read_calls).unwrap_or(0),
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.buffer.leftover_after_terminator",
                    io.leftover_bytes_after_terminator,
                );
                crate::runtime::perf_probe::record_value(
                    "download.nntp.buffer.compactions",
                    io.buffer_compactions,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.decode.calls",
                    io.decode_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.input.chunks",
                    io.input_chunks,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.crc.update_calls",
                    io.crc_update_calls,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.output.batches",
                    io.output_batches,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.encoded.bytes",
                    io.encoded_bytes_consumed,
                );
                crate::runtime::perf_probe::record_value(
                    "download.fused.decoded.bytes",
                    io.decoded_bytes_written,
                );
                let data = {
                    let _cpu =
                        crate::runtime::perf_probe::cpu_scope("download.inline_decode.into_chunk");
                    DecodedChunk::from(decoded)
                };

                Ok(DownloadPayload::Decoded(match body {
                    weaver_nntp::fused_yenc::FusedArticleBody::Yenc(result) => {
                        record_checkpoint_observability(&result);
                        let yenc_layout = YencLayoutAssertions {
                            file_size: result.metadata.size,
                            part: result.metadata.part,
                            total: result.metadata.total,
                            begin: result.metadata.begin,
                            end: result.metadata.end,
                        };

                        DecodeResult {
                            segment_id,
                            raw_size: raw_size as u64,
                            encoding: SegmentEncoding::Yenc,
                            yenc_layout,
                            crc_valid: crate::pipeline::crc_not_mismatched(result.crc_status),
                            part_crc_verified: result.expected_part_crc.is_some()
                                && crate::pipeline::crc_not_mismatched(result.crc_status),
                            part_crc: result.part_crc,
                            expected_file_crc: result.expected_file_crc,
                            data,
                            yenc_name: result.metadata.name,
                            checkpoint_plan: result.checkpoint_plan,
                            segments: result.segments,
                        }
                    }
                    // uuencode declares no offsets, no size and no checksum, so
                    // every field that would carry one is left at its "nothing
                    // to say" value. In particular `segments` is empty: the
                    // dual-CRC grid can only be fed block-aligned CRC evidence,
                    // and there is none to give it here.
                    weaver_nntp::fused_yenc::FusedArticleBody::Uu(outcome) => DecodeResult {
                        segment_id,
                        raw_size: raw_size as u64,
                        encoding: SegmentEncoding::Uu(crate::pipeline::UuSegmentFacts {
                            damaged: outcome.damaged,
                            ended: outcome.ended,
                        }),
                        yenc_layout: YencLayoutAssertions {
                            file_size: 0,
                            part: None,
                            total: None,
                            begin: None,
                            end: None,
                        },
                        // "Not known bad" — there is nothing to check against,
                        // which is different from having checked and passed.
                        crc_valid: true,
                        part_crc_verified: false,
                        part_crc: 0,
                        expected_file_crc: None,
                        data,
                        yenc_name: outcome.filename.unwrap_or_default(),
                        checkpoint_plan: weaver_yenc::CheckpointPlan::None,
                        segments: Vec::new(),
                    },
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
}
