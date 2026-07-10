use super::*;

impl Pipeline {
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

    pub(in crate::pipeline::download::worker) fn restore_download_result_work_without_retry(
        &mut self,
        segment_id: SegmentId,
        retry_count: u32,
        exclude_servers: Vec<usize>,
    ) -> bool {
        let job_id = segment_id.file_id.job_id;
        let file_idx = segment_id.file_id.file_index as usize;
        let Some(state) = self.jobs.get_mut(&job_id) else {
            return false;
        };
        let Some(file_spec) = state.spec.files.get(file_idx) else {
            return false;
        };
        let Some(seg_spec) = file_spec
            .segments
            .iter()
            .find(|seg| seg.ordinal == segment_id.segment_number)
        else {
            return false;
        };
        let work = DownloadWork {
            segment_id,
            message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
            groups: file_spec.groups.clone(),
            priority: file_spec.role.download_priority(),
            byte_estimate: seg_spec.bytes,
            retry_count,
            is_recovery: file_spec.role.is_recovery(),
            exclude_servers,
        };
        if work.is_recovery {
            state.recovery_queue.push(work);
        } else {
            state.download_queue.push(work);
        }
        self.update_queue_metrics();
        true
    }

    pub(in crate::pipeline) fn refresh_server_quota_block_presentation(&self) {
        let blocked = !self.server_quota_parked.is_empty()
            && self.nntp.all_normal_fill_servers_quota_blocked();
        self.shared_state.set_server_quota_blocked(blocked);
    }

    pub(in crate::pipeline::download::worker) fn schedule_download_work_without_retry_budget(
        &mut self,
        segment_id: SegmentId,
        retry_count: u32,
        exclude_servers: Vec<usize>,
        delay: Option<Duration>,
        wake_on_server_policy_change: bool,
        quota_rejection: Option<weaver_nntp::transfer::QuotaRejection>,
    ) -> bool {
        let job_id = segment_id.file_id.job_id;
        let file_idx = segment_id.file_id.file_index as usize;
        let Some(state) = self.jobs.get(&job_id) else {
            return false;
        };
        let Some(file_spec) = state.spec.files.get(file_idx) else {
            return false;
        };
        let Some(seg_spec) = file_spec
            .segments
            .iter()
            .find(|seg| seg.ordinal == segment_id.segment_number)
        else {
            return false;
        };
        let work = DownloadWork {
            segment_id,
            message_id: crate::jobs::ids::MessageId::new(&seg_spec.message_id),
            groups: file_spec.groups.clone(),
            priority: file_spec.role.download_priority(),
            byte_estimate: seg_spec.bytes,
            retry_count,
            is_recovery: file_spec.role.is_recovery(),
            exclude_servers,
        };
        self.note_retry_scheduled(segment_id);
        if wake_on_server_policy_change {
            self.server_quota_parked.insert(segment_id);
            self.refresh_server_quota_block_presentation();
        }
        let retry_tx = self.retry_tx.clone();
        let scheduled_pool_generation = self.pool_generation;
        let server_transfer_policy = wake_on_server_policy_change
            .then(|| self.shared_state.server_transfer_policy())
            .flatten();
        let server_policy_changes = server_transfer_policy
            .as_ref()
            .map(|policy| policy.subscribe_changes());
        let server_capacity_changes = server_transfer_policy
            .as_ref()
            .map(|policy| policy.subscribe_capacity_changes());
        let rejection_is_current = server_transfer_policy
            .as_ref()
            .zip(quota_rejection.as_ref())
            .is_none_or(|(policy, rejection)| policy.quota_rejection_is_current(rejection));
        let mut delay = if rejection_is_current {
            delay
        } else {
            Some(Duration::ZERO)
        };
        if delay.is_none() && server_policy_changes.is_none() && server_capacity_changes.is_none() {
            delay = Some(Duration::from_secs(1));
        }
        let shared_state = self.shared_state.clone();
        let mut job_changes = shared_state.subscribe_job_changes();
        tokio::spawn(async move {
            let wait_for_delay = async move {
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let wait_for_policy = async move {
                if let Some(mut changes) = server_policy_changes {
                    let _ = changes.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let wait_for_capacity = async move {
                if let Some(mut changes) = server_capacity_changes {
                    let _ = changes.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            let wait_until_retry = async move {
                tokio::select! {
                    _ = wait_for_delay => {}
                    _ = wait_for_policy => {}
                    _ = wait_for_capacity => {}
                }
            };
            tokio::pin!(wait_until_retry);
            loop {
                tokio::select! {
                    _ = &mut wait_until_retry => break,
                    _ = retry_tx.closed() => return,
                    changed = job_changes.changed() => {
                        let job_inactive = shared_state
                            .get_job(job_id)
                            .is_none_or(|job| is_terminal_status(&job.status));
                        if changed.is_err() || job_inactive {
                            break;
                        }
                    }
                }
            }
            let _ = retry_tx
                .send(crate::pipeline::RetryWork {
                    scheduled_pool_generation,
                    work,
                })
                .await;
        });
        true
    }

    pub(crate) fn release_download_result(&mut self, result: &DownloadResult) {
        let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.release_result");
        let policy_outcome = matches!(
            &result.data,
            Err(DownloadError::Fetch(failure))
                if matches!(
                    failure.kind,
                    DownloadFailureKind::ServerQuota | DownloadFailureKind::Unrequested
                )
        );
        if !policy_outcome {
            self.record_download_lane_observation(result);
        }
        self.active_downloads = self.active_downloads.saturating_sub(1);
        if result.release_connection_slot {
            if let Some(observation) = result.lane_observation.as_ref() {
                let reason = if observation.connection_discarded || !observation.batch_clean {
                    LaneParkReason::Error
                } else {
                    LaneParkReason::NoWork
                };
                self.note_download_lane_released(observation.mode, reason);
            }
            self.active_download_connections = self.active_download_connections.saturating_sub(1);
            let job_id = result.segment_id.file_id.job_id;
            if let Some(in_flight) = self.active_download_connections_by_job.get_mut(&job_id) {
                *in_flight = in_flight.saturating_sub(1);
                if *in_flight == 0 {
                    self.active_download_connections_by_job.remove(&job_id);
                }
            }
            self.clear_spillover_loan_if_idle();
        }
        if result.origin.is_recovery() {
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
        if result.origin.counts_for_hot_primary()
            && self.hot_dispatch_job == Some(job_id)
            && result.data.is_ok()
        {
            self.hot_dispatch_successes = self.hot_dispatch_successes.saturating_add(1);
        }
        if result.origin.counts_for_hot_primary()
            && let Ok(DownloadPayload::Decoded(decoded)) = &result.data
        {
            self.ensure_job_transport_profile(job_id);
            if let Some(profile) = self.job_transport_profiles.get_mut(&job_id) {
                profile.note_body_size(decoded.raw_size);
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
        if result.origin.counts_for_hot_primary()
            && self.hot_dispatch_job == Some(job_id)
            && let Ok(payload) = &result.data
        {
            let raw_bytes = match payload {
                DownloadPayload::Raw(raw) => raw.len() as u64,
                DownloadPayload::Decoded(decoded) => decoded.raw_size,
            };
            self.hot_dispatch_throughput_window
                .record(Instant::now(), raw_bytes);
        }
        self.reconcile_rate_limit_for_download(result.segment_id, actual_raw_bytes);
        if let Some(raw_bytes) = actual_raw_bytes
            && let Err(error) = self.record_download_bandwidth_usage(raw_bytes)
        {
            error!(
                error = %error,
                segment = %result.segment_id,
                "failed to record ISP bandwidth usage"
            );
        }
        self.publish_hot_dispatch_metrics(Instant::now());
    }

    pub(crate) async fn handle_download_done(&mut self, result: DownloadResult) {
        self.release_download_result(&result);
        self.process_download_done(result).await;
        self.maybe_service_deferred_lane_refills();
    }

    pub(crate) fn released_download_result_lead_bytes(result: &DownloadResult) -> u64 {
        match &result.data {
            Ok(DownloadPayload::Raw(bytes)) => bytes.len() as u64,
            Ok(DownloadPayload::Decoded(decoded)) => decoded.decoded_size as u64,
            Err(_) => 0,
        }
    }

    pub(crate) fn note_released_download_result_pending(&mut self, job_id: JobId, bytes: u64) {
        *self
            .pending_released_download_results_by_job
            .entry(job_id)
            .or_insert(0) += 1;
        if bytes != 0 {
            *self
                .pending_released_download_result_bytes_by_job
                .entry(job_id)
                .or_insert(0) += bytes;
        }
    }

    pub(crate) fn finish_released_download_result_processing(&mut self, job_id: JobId, bytes: u64) {
        let remove = if let Some(pending) = self
            .pending_released_download_results_by_job
            .get_mut(&job_id)
        {
            *pending = pending.saturating_sub(1);
            *pending == 0
        } else {
            false
        };
        if remove {
            self.pending_released_download_results_by_job
                .remove(&job_id);
        }
        if bytes != 0 {
            let remove_bytes = if let Some(pending_bytes) = self
                .pending_released_download_result_bytes_by_job
                .get_mut(&job_id)
            {
                *pending_bytes = pending_bytes.saturating_sub(bytes);
                *pending_bytes == 0
            } else {
                false
            };
            if remove_bytes {
                self.pending_released_download_result_bytes_by_job
                    .remove(&job_id);
            }
        }
    }

    pub(crate) async fn process_released_download_done(&mut self, result: DownloadResult) {
        let job_id = result.segment_id.file_id.job_id;
        let lead_bytes = Self::released_download_result_lead_bytes(&result);
        self.process_download_done(result).await;
        self.finish_released_download_result_processing(job_id, lead_bytes);
        self.maybe_service_deferred_lane_refills();
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

        let (excluded_servers, source_server_idx) = {
            let _cpu_scope =
                crate::runtime::perf_probe::cpu_scope("download.process_done.pre_match");
            let excluded_servers = result.exclude_servers.clone();
            let source_server_idx = result.source_server_idx;
            if result.origin != DownloadResultOrigin::IpReplacementTrial {
                self.observe_ip_rtt_attempts(&result.attempts);
            }

            for (attempt_index, attempt) in result.attempts.iter().enumerate() {
                let outcome = match attempt.outcome {
                    FetchAttemptOutcome::QuotaBlocked | FetchAttemptOutcome::QuotaUnrequested => {
                        continue;
                    }
                    FetchAttemptOutcome::Success => {
                        crate::events::model::ServerAttemptOutcome::Success
                    }
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
                    is_recovery: result.origin.is_recovery(),
                });
            }

            (excluded_servers, source_server_idx)
        };

        if result.data.is_ok() {
            self.refresh_server_quota_block_presentation();
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
                {
                    let _cpu_scope = crate::runtime::perf_probe::cpu_scope(
                        "download.process_done.decoded_pre_success",
                    );
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
                }

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
                if result.origin == DownloadResultOrigin::IpReplacementTrial
                    && !matches!(
                        failure.kind,
                        DownloadFailureKind::ArticleNotFound
                            | DownloadFailureKind::Auth
                            | DownloadFailureKind::Permanent
                    )
                {
                    if self.restore_download_result_work_without_retry(
                        result.segment_id,
                        result.retry_count,
                        excluded_servers,
                    ) {
                        debug!(
                            segment = %result.segment_id,
                            error = %failure.message,
                            "restored IP replacement trial work without consuming retry budget"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                if failure.kind == DownloadFailureKind::ServerQuota {
                    let mut retry_after = failure.retry_after;
                    let mut quota_rejection = failure.quota_rejection;
                    let mut park_for_quota = source_server_idx.is_none();
                    if source_server_idx.is_some()
                        && let Some(rejection) = quota_rejection.as_ref()
                    {
                        let effective_excludes =
                            self.effective_exclude_servers(job_id, &excluded_servers);
                        let selection = self
                            .nntp
                            .body_server_selection_with_estimate(
                                &effective_excludes,
                                rejection.requested_body_bytes,
                            )
                            .await;
                        if selection.eligible.is_empty()
                            && let Some(current) = selection.quota_blocked
                        {
                            retry_after = current
                                .retry_at
                                .map(|deadline| deadline.saturating_duration_since(Instant::now()));
                            quota_rejection = Some(current);
                            park_for_quota = true;
                        }
                    }

                    if park_for_quota {
                        if self.schedule_download_work_without_retry_budget(
                            result.segment_id,
                            result.retry_count,
                            excluded_servers,
                            retry_after,
                            true,
                            quota_rejection,
                        ) {
                            debug!(
                                segment = %result.segment_id,
                                retry_after_ms = ?retry_after.map(|value| value.as_millis()),
                                "server quota blocked this work's eligible servers; parked without consuming retry budget"
                            );
                        }
                    } else if let Some(source_server_idx) = source_server_idx
                        && self.schedule_download_work_without_retry_budget(
                            result.segment_id,
                            result.retry_count,
                            excluded_servers,
                            Some(Duration::ZERO),
                            false,
                            None,
                        )
                    {
                        debug!(
                            segment = %result.segment_id,
                            source_server_idx,
                            "server quota rejected BODY admission; retrying eligible fill servers without unlocking backfill"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                if failure.kind == DownloadFailureKind::Unrequested {
                    if self.restore_download_result_work_without_retry(
                        result.segment_id,
                        result.retry_count,
                        excluded_servers,
                    ) {
                        debug!(
                            segment = %result.segment_id,
                            error = %failure.message,
                            "restored unrequested download work without consuming retry budget"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                if failure.kind == DownloadFailureKind::LaneUnavailable {
                    // A work item whose effective exclusions (failure plus
                    // retention) cover every server can never be served: the
                    // order it builds is empty, so it would requeue through
                    // this arm forever without a fetch to fail. A job older
                    // than every server's retention window hits exactly this.
                    // Book it as missing instead.
                    let server_count = self.nntp.pool().server_count();
                    if server_count > 0
                        && self.unavailable_server_count(job_id, &excluded_servers) >= server_count
                    {
                        self.metrics
                            .articles_not_found
                            .fetch_add(1, Ordering::Relaxed);
                        let _ = self.event_tx.send(PipelineEvent::ArticleNotFound {
                            segment_id: result.segment_id,
                        });
                        self.book_failed_segment(result.segment_id);
                        self.maybe_finish_download_pass(job_id);
                        return;
                    }
                    // Servers exist that could serve this article but none is
                    // eligible right now. When that is a health condition
                    // rather than saturation (empty order, not just busy
                    // permits), surface it — a fill server stuck in
                    // auth-disable cycles otherwise stalls the queue silently.
                    if self
                        .last_no_eligible_server_warn
                        .is_none_or(|at| at.elapsed() >= NO_ELIGIBLE_SERVER_WARN_INTERVAL)
                    {
                        let effective = self.effective_exclude_servers(job_id, &excluded_servers);
                        if self.nntp.body_server_order(&effective).await.is_empty() {
                            self.last_no_eligible_server_warn = Some(Instant::now());
                            warn!(
                                job_id = job_id.0,
                                "downloads waiting: no eligible news server (cooling down, disabled, or outside retention); check server health and credentials"
                            );
                        }
                    }
                    self.metrics
                        .download_failures_capacity_unavailable
                        .fetch_add(1, Ordering::Relaxed);
                    if self.schedule_download_work_without_retry_budget(
                        result.segment_id,
                        result.retry_count,
                        excluded_servers,
                        Some(BODY_LANE_UNAVAILABLE_RETRY_DELAY),
                        false,
                        None,
                    ) {
                        debug!(
                            job_id = job_id.0,
                            segment = %result.segment_id,
                            retry_count = result.retry_count,
                            retry_delay_ms = BODY_LANE_UNAVAILABLE_RETRY_DELAY.as_millis() as u64,
                            error = %failure.message,
                            "body lane unavailable; requeued without consuming article retry budget"
                        );
                    } else {
                        warn!(
                            job_id = job_id.0,
                            segment = %result.segment_id,
                            error = %failure.message,
                            "body lane unavailable (job gone, not retrying)"
                        );
                    }
                    self.maybe_finish_download_pass(job_id);
                    return;
                }
                let terminal_cause = match &failure.kind {
                    DownloadFailureKind::ArticleNotFound => {
                        crate::jobs::SemanticTerminalCause::MissingArticlesOrLowHealth
                    }
                    DownloadFailureKind::Auth
                    | DownloadFailureKind::ServerQuota
                    | DownloadFailureKind::CapacityUnavailable
                    | DownloadFailureKind::LaneUnavailable
                    | DownloadFailureKind::Transient
                    | DownloadFailureKind::Unrequested => {
                        crate::jobs::SemanticTerminalCause::ServerAuthQuotaOrOutage
                    }
                    DownloadFailureKind::Permanent => {
                        crate::jobs::SemanticTerminalCause::DecodeOrCorruptData
                    }
                };
                self.semantic_terminal_causes.insert(job_id, terminal_cause);
                match &failure.kind {
                    DownloadFailureKind::ArticleNotFound => self
                        .metrics
                        .download_failures_article_not_found
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::CapacityUnavailable
                    | DownloadFailureKind::LaneUnavailable => self
                        .metrics
                        .download_failures_capacity_unavailable
                        .fetch_add(1, Ordering::Relaxed),
                    DownloadFailureKind::Unrequested | DownloadFailureKind::ServerQuota => 0,
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
                let article_not_found_server_idx =
                    if failure.kind == DownloadFailureKind::ArticleNotFound {
                        source_server_idx.or_else(|| {
                            result.attempts.iter().rev().find_map(|attempt| {
                                matches!(
                                    attempt.outcome,
                                    weaver_nntp::client::FetchAttemptOutcome::NotFound
                                )
                                .then_some(attempt.server_idx)
                            })
                        })
                    } else {
                        source_server_idx
                    };
                let source_not_found = failure.kind == DownloadFailureKind::ArticleNotFound
                    && article_not_found_server_idx.is_some();
                let retry_exclude_servers = if failure.kind == DownloadFailureKind::ArticleNotFound
                {
                    Self::decode_retry_exclude_servers(
                        &excluded_servers,
                        article_not_found_server_idx,
                    )
                } else {
                    excluded_servers.clone()
                };
                let article_not_found_exhausted = failure.kind
                    == DownloadFailureKind::ArticleNotFound
                    && (retry_exclude_servers.is_empty() || {
                        let server_count = self.nntp.pool().server_count();
                        server_count > 0 && {
                            // Retention-excluded servers can never 430; they
                            // count as unavailable or exhaustion could not
                            // complete while one is configured.
                            self.unavailable_server_count(job_id, &retry_exclude_servers)
                                >= server_count
                        }
                    });

                if article_not_found_exhausted {
                    self.metrics
                        .articles_not_found
                        .fetch_add(1, Ordering::Relaxed);
                    let _ = self.event_tx.send(PipelineEvent::ArticleNotFound {
                        segment_id: result.segment_id,
                    });
                    self.book_failed_segment(result.segment_id);
                } else {
                    let seg_id = result.segment_id;
                    let capacity_unavailable =
                        failure.kind == DownloadFailureKind::CapacityUnavailable;
                    let next_retry = if capacity_unavailable || source_not_found {
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
                        // Retry exhaustion is as terminal as a missing
                        // article: without this, health stays optimistic and
                        // recovery promotion waits for post-download verify.
                        self.book_failed_segment(seg_id);
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
                                exclude_servers: retry_exclude_servers.clone(),
                            };
                            self.metrics
                                .segments_retried
                                .fetch_add(1, Ordering::Relaxed);
                            let delay = if capacity_unavailable {
                                std::time::Duration::from_secs(1)
                            } else if source_not_found {
                                std::time::Duration::ZERO
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
                            let scheduled_pool_generation = self.pool_generation;
                            tokio::spawn(async move {
                                tokio::time::sleep(delay).await;
                                let _ = retry_tx
                                    .send(crate::pipeline::RetryWork {
                                        scheduled_pool_generation,
                                        work,
                                    })
                                    .await;
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
