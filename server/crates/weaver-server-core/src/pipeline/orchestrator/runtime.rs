use super::*;

impl Pipeline {
    const METRICS_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
    const STATE_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    const SHUTDOWN_DRAIN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    /// Bound on how long `drain` waits to join detached fire-and-forget writes.
    const FIRE_AND_FORGET_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    const DOWNLOAD_COMPLETION_DRAIN_LIMIT: usize = 128;
    const INITIAL_CONNECTION_RAMP_LIMIT: usize = 20;

    /// Create a new pipeline.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        cmd_rx: mpsc::Receiver<SchedulerCommand>,
        event_tx: broadcast::Sender<PipelineEvent>,
        nntp: NntpClient,
        buffers: Arc<BufferPool>,
        profile: SystemProfile,
        data_dir: PathBuf,
        intermediate_dir: PathBuf,
        complete_dir: PathBuf,
        total_connections: usize,
        write_buf_max_pending: usize,
        mut initial_history: Vec<JobInfo>,
        initial_global_paused: bool,
        shared_state: SharedPipelineState,
        db: crate::Database,
        config: crate::settings::SharedConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = Arc::clone(shared_state.metrics());
        initial_history.truncate(crate::jobs::FINISHED_JOBS_RUNTIME_CAP);
        let write_backlog_budget_bytes = compute_write_backlog_budget_bytes(&profile, &buffers);
        let decode_backlog_budget_bytes =
            compute_decode_backlog_budget_bytes(&profile, &buffers, write_backlog_budget_bytes);
        let tuner = RuntimeTuner::with_connection_limit(profile, total_connections);
        let (initial_bandwidth_policy, ip_replacement_trial_extra_connections) = {
            let cfg = config.read().await;
            (
                cfg.isp_bandwidth_cap.clone(),
                cfg.ip_replacement_trial_extra_connections(),
            )
        };
        metrics.set_ip_replacement_trial_extra_connections(ip_replacement_trial_extra_connections);
        info!(
            max_downloads = tuner.params().max_concurrent_downloads,
            decode_backlog_budget_mb = decode_backlog_budget_bytes / (1024 * 1024),
            write_backlog_budget_mb = write_backlog_budget_bytes / (1024 * 1024),
            total_connections,
            "pipeline tuner initialized"
        );

        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::create_dir_all(&intermediate_dir).await?;
        tokio::fs::create_dir_all(&complete_dir).await?;

        let (download_done_tx, download_done_rx) = mpsc::channel(256);
        let (download_refill_tx, download_refill_rx) = mpsc::channel(256);
        let (download_lane_parked_tx, download_lane_parked_rx) = mpsc::channel(256);
        let (owned_download_lane_event_tx, owned_download_lane_event_rx) = mpsc::channel(128);
        let (ip_replacement_trial_tx, ip_replacement_trial_rx) = mpsc::channel(16);
        let (decode_done_tx, decode_done_rx) = mpsc::channel(256);
        let (retry_tx, retry_rx) = mpsc::channel(256);
        let (probe_result_tx, probe_result_rx) = mpsc::channel(16);
        let (extract_done_tx, extract_done_rx) = mpsc::channel(32);
        let (rar_refresh_done_tx, rar_refresh_done_rx) = mpsc::channel(32);
        let (rar_capacity_retry_tx, rar_capacity_retry_rx) = mpsc::channel(32);
        let (verified_suspect_persist_done_tx, verified_suspect_persist_done_rx) =
            mpsc::channel(32);
        let (move_done_tx, move_done_rx) = mpsc::channel(32);
        let nntp = Arc::new(nntp);
        let owned_download_lane_pool =
            download::owned_lane::OwnedDownloadLanePool::new(total_connections.max(1));
        shared_state.set_paused(initial_global_paused);
        let pp_pool = crate::runtime::postprocess_pool::build_postprocess_pool(
            tuner.params().extract_thread_count,
        );
        let mut bandwidth_cap = BandwidthCapRuntime::default();
        bandwidth_cap.set_policy(initial_bandwidth_policy);

        let mut pipeline = Self {
            cmd_rx,
            event_tx,
            nntp,
            buffers,
            tuner,
            metrics,
            jobs: HashMap::new(),
            archive_password_winners: HashMap::new(),
            job_order: Vec::new(),
            active_downloads: 0,
            active_download_connections: 0,
            active_recovery: 0,
            hot_dispatch_job: None,
            hot_dispatch_started_at: None,
            hot_dispatch_successes: 0,
            hot_dispatch_exclusive_peak_bps: 0,
            hot_dispatch_last_lend_at: None,
            hot_dispatch_mode: DispatchShareMode::Exclusive,
            hot_dispatch_underfill_since: None,
            hot_dispatch_last_spillover_decision: SpilloverDecision::None,
            hot_dispatch_throughput_window: HotJobThroughputWindow::default(),
            hot_dispatch_exclusive_window: HotExclusiveWindow::default(),
            hot_dispatch_expansion_window: HotExpansionWindow::default(),
            hot_dispatch_spillover_loans: SpilloverLoanBook::default(),
            hot_share_yield_signal: Arc::new(HotShareYieldSignal::default()),
            job_transport_profiles: HashMap::new(),
            download_lane_runtime: DownloadLaneRuntimeState::default(),
            deferred_lane_refills: std::collections::VecDeque::new(),
            ip_replacement_trial_extra_connections,
            ip_rtt_ewma: HashMap::new(),
            ip_replacement_retired_ips: HashSet::new(),
            ip_replacement_burst_active: false,
            active_download_passes: HashSet::new(),
            jobs_finalizing_download: HashSet::new(),
            pending_released_download_results_by_job: HashMap::new(),
            pending_released_download_result_bytes_by_job: HashMap::new(),
            active_downloads_by_job: HashMap::new(),
            active_download_connections_by_job: HashMap::new(),
            active_downloads_by_file: HashMap::new(),
            active_decodes_by_job: HashMap::new(),
            active_decodes_by_file: HashMap::new(),
            job_last_download_activity: HashMap::new(),
            pending_retries_by_job: HashMap::new(),
            pending_retries_by_segment: HashMap::new(),
            server_quota_parked: HashSet::new(),
            intermediate_dir,
            complete_dir,
            nzb_dir: data_dir.join(".weaver-nzbs"),
            pending_file_progress: HashMap::new(),
            persisted_file_progress: HashMap::new(),
            file_hash_states: HashMap::new(),
            deferred_file_hash_data: HashMap::new(),
            deferred_file_hash_data_bytes: 0,
            deferred_file_hash_ranges: HashMap::new(),
            expected_file_crcs: HashMap::new(),
            file_hash_reread_required: HashSet::new(),
            #[cfg(test)]
            try_update_archive_topology_calls: 0,
            #[cfg(test)]
            par2_lower_bound_preflight_calls: 0,
            #[cfg(test)]
            par2_authoritative_verify_calls: 0,
            #[cfg(test)]
            par2_repairer_analyze_calls: 0,
            #[cfg(test)]
            par2_repairer_execute_calls: 0,
            pending_decode: VecDeque::new(),
            pending_completion_checks: VecDeque::new(),
            download_done_tx,
            download_done_rx,
            download_refill_tx,
            download_refill_rx,
            download_lane_parked_tx,
            download_lane_parked_rx,
            owned_download_lane_event_tx,
            owned_download_lane_event_rx,
            owned_download_lane_pool,
            ip_replacement_trial_tx,
            ip_replacement_trial_rx,
            decode_done_tx,
            decode_done_rx,
            retry_tx,
            retry_rx,
            pool_generation: 0,
            probe_result_tx,
            probe_result_rx,
            extract_done_tx,
            extract_done_rx,
            rar_refresh_done_tx,
            rar_refresh_done_rx,
            rar_capacity_retry_tx,
            rar_capacity_retry_rx,
            verified_suspect_persist_done_tx,
            verified_suspect_persist_done_rx,
            move_done_tx,
            move_done_rx,
            global_paused: initial_global_paused,
            bandwidth_cap,
            bandwidth_reservations: HashMap::new(),
            rate_limit_reservations: HashMap::new(),
            connection_ramp: total_connections.min(Self::INITIAL_CONNECTION_RAMP_LIMIT),
            rate_limiter: TokenBucket::new(0),
            write_buf_max_pending,
            decode_backlog_budget_bytes,
            write_backlog_budget_bytes,
            download_decode_hard_pressure_latched: false,
            download_write_hard_pressure_latched: false,
            download_pressure_hard_stall_started_at: None,
            download_pressure_soft_dispatch_after: None,
            download_restart_durable_lead_retry_after: HashMap::new(),
            last_download_dispatch_stall_log_at: None,
            write_buffered_bytes: 0,
            write_buffered_segments: 0,
            write_buffers: HashMap::new(),
            par2_runtime: HashMap::new(),
            extracted_members: HashMap::new(),
            extracted_archives: HashMap::new(),
            decode_retries: HashMap::new(),
            inflight_extractions: HashMap::new(),
            phase_progress: HashMap::new(),
            phase_progress_snapshots: HashMap::new(),
            phase_publish_state: HashMap::new(),
            phase_extraction_member_totals: HashSet::new(),
            job_retention_exclude_cache: HashMap::new(),
            last_no_eligible_server_warn: None,
            inflight_moves: HashSet::new(),
            reserved_complete_destinations: HashMap::new(),
            failed_extractions: HashMap::new(),
            eagerly_deleted: HashMap::new(),
            rar_sets: HashMap::new(),
            rar_refresh_state: HashMap::new(),
            rar_unlock_priority_dirty_jobs: HashSet::new(),
            rar_unlock_boosted_files: HashMap::new(),
            pending_rar_capacity_retries: HashSet::new(),
            verified_suspect_persist_state: HashMap::new(),
            rar_waiting_members: HashMap::new(),
            normalization_retried: HashSet::new(),
            pending_concat: HashMap::new(),
            par2_bypassed: HashSet::new(),
            par2_verified: HashSet::new(),
            unavailable_promoted_recovery_segments: HashSet::new(),
            finished_jobs: initial_history,
            shared_state,
            db,
            fire_and_forget_tasks: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            config,
            pp_pool,
        };
        let _ = pipeline.refresh_bandwidth_cap_window();
        pipeline.refresh_download_pressure();
        Ok(pipeline)
    }

    pub(crate) async fn db_blocking<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::Database) -> R + Send + 'static,
        R: Send + 'static,
    {
        let db = self.db.clone();
        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            let task_started = Instant::now();
            let result = f(&db);
            crate::runtime::perf_probe::record("pipeline.db_blocking.task", task_started.elapsed());
            result
        })
        .await
        .expect("db task panicked");
        crate::runtime::perf_probe::record("pipeline.db_blocking.await", started.elapsed());
        result
    }

    pub(crate) fn db_fire_and_forget<F>(&self, f: F)
    where
        F: FnOnce(&crate::Database) + Send + 'static,
    {
        let db = self.db.clone();
        let started = Instant::now();
        {
            let mut tasks = self
                .fire_and_forget_tasks
                .lock()
                .expect("fire-and-forget task set poisoned");
            // Opportunistically reap finished handles so the set does not grow
            // unbounded across a long-running pipeline; only spawn/drain here,
            // never await while holding the lock.
            while tasks.try_join_next().is_some() {}
            tasks.spawn_blocking(move || {
                let task_started = Instant::now();
                f(&db);
                crate::runtime::perf_probe::record(
                    "pipeline.db_fire_and_forget.task",
                    task_started.elapsed(),
                );
            });
        }
        crate::runtime::perf_probe::record("pipeline.db_fire_and_forget.submit", started.elapsed());
    }

    /// Join all outstanding [`Self::db_fire_and_forget`] writes with a bounded
    /// timeout so shutdown does not drop in-flight persistence. Called from
    /// `drain` after the explicit awaited flushes. The JoinSet is moved out from
    /// under the lock first so no lock is held across the await.
    async fn join_fire_and_forget_tasks(&self) {
        let mut tasks = {
            let mut guard = self
                .fire_and_forget_tasks
                .lock()
                .expect("fire-and-forget task set poisoned");
            std::mem::take(&mut *guard)
        };
        if tasks.is_empty() {
            return;
        }
        let drain = async { while tasks.join_next().await.is_some() {} };
        if tokio::time::timeout(Self::FIRE_AND_FORGET_DRAIN_TIMEOUT, drain)
            .await
            .is_err()
        {
            warn!(
                remaining = tasks.len(),
                timeout_secs = Self::FIRE_AND_FORGET_DRAIN_TIMEOUT.as_secs(),
                "timed out joining fire-and-forget database writes during shutdown"
            );
        }
    }

    pub fn nntp_pool(&self) -> Arc<weaver_nntp::pool::NntpPool> {
        self.nntp.pool().clone()
    }

    pub(crate) fn effective_downloaded_bytes(state: &JobState) -> u64 {
        state
            .downloaded_bytes
            .max(state.restored_download_floor_bytes)
            .min(state.spec.total_bytes)
    }

    pub(crate) fn effective_progress(state: &JobState) -> f64 {
        let byte_progress = if state.spec.total_bytes == 0 {
            0.0
        } else {
            Self::effective_downloaded_bytes(state) as f64 / state.spec.total_bytes as f64
        };
        state.assembly.progress().max(byte_progress).clamp(0.0, 1.0)
    }

    pub(crate) fn note_file_progress_floor(
        &mut self,
        file_id: NzbFileId,
        contiguous_bytes_written: u64,
        force_flush: bool,
    ) {
        let current = self
            .pending_file_progress
            .get(&file_id)
            .copied()
            .or_else(|| self.persisted_file_progress.get(&file_id).copied())
            .unwrap_or(0);
        if contiguous_bytes_written <= current {
            return;
        }
        self.pending_file_progress
            .insert(file_id, contiguous_bytes_written);
        let threshold_flush =
            contiguous_bytes_written.saturating_sub(current) >= download_restart_checkpoint_bytes();
        if force_flush || threshold_flush {
            let label = if force_flush {
                "download.file_progress.flush.force"
            } else {
                "download.file_progress.flush.threshold"
            };
            self.flush_file_progress_batch(label);
            if threshold_flush {
                crate::e2e_failpoint::maybe_delay("download.after_progress_floor_flush");
            }
        }
    }

    pub(crate) fn flush_file_progress_batch(&mut self, label: &'static str) {
        if self.pending_file_progress.is_empty() {
            return;
        }
        let _probe = crate::runtime::perf_probe::scope(label);
        let pending = std::mem::take(&mut self.pending_file_progress);
        for (file_id, bytes) in &pending {
            self.persisted_file_progress.insert(*file_id, *bytes);
        }
        let batch: Vec<ActiveFileProgress> = pending
            .into_iter()
            .map(|(file_id, contiguous_bytes_written)| ActiveFileProgress {
                job_id: file_id.job_id,
                file_index: file_id.file_index,
                contiguous_bytes_written,
            })
            .collect();
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.upsert_file_progress_batch(&batch) {
                tracing::error!(count = batch.len(), error = %error, "failed to persist file progress floors");
            }
        });
    }

    pub(crate) async fn flush_file_progress_batch_awaited(
        &mut self,
        label: &'static str,
    ) -> Result<(), crate::StateError> {
        if self.pending_file_progress.is_empty() {
            return Ok(());
        }
        let _probe = crate::runtime::perf_probe::scope(label);
        let pending = std::mem::take(&mut self.pending_file_progress);
        for (file_id, bytes) in &pending {
            self.persisted_file_progress.insert(*file_id, *bytes);
        }
        let batch: Vec<ActiveFileProgress> = pending
            .into_iter()
            .map(|(file_id, contiguous_bytes_written)| ActiveFileProgress {
                job_id: file_id.job_id,
                file_index: file_id.file_index,
                contiguous_bytes_written,
            })
            .collect();
        self.db_blocking(move |db| db.upsert_file_progress_batch(&batch))
            .await
    }

    pub(crate) fn clear_job_progress_floor_runtime(&mut self, job_id: JobId) {
        self.pending_file_progress
            .retain(|file_id, _| file_id.job_id != job_id);
        self.persisted_file_progress
            .retain(|file_id, _| file_id.job_id != job_id);
        self.file_hash_states
            .retain(|file_id, _| file_id.job_id != job_id);
        self.expected_file_crcs
            .retain(|file_id, _| file_id.job_id != job_id);
        self.file_hash_reread_required
            .retain(|file_id| file_id.job_id != job_id);
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);
    }

    pub(crate) fn note_download_activity(&mut self, job_id: JobId) {
        self.job_last_download_activity
            .insert(job_id, Instant::now());
    }

    pub(crate) fn emit_download_finished_if_active(&mut self, job_id: JobId) {
        if self.active_download_passes.remove(&job_id) {
            self.phase_end(job_id, JobPhase::Downloading);
            let finalization_pending = self.job_has_pending_download_pipeline_work(job_id);
            if finalization_pending {
                self.jobs_finalizing_download.insert(job_id);
            } else {
                self.jobs_finalizing_download.remove(&job_id);
            }
            let _ = self.event_tx.send(PipelineEvent::DownloadFinished {
                job_id,
                finalization_pending,
            });
        }
    }

    pub(crate) fn emit_download_pipeline_drained_if_pending(&mut self, job_id: JobId) {
        if self.jobs_finalizing_download.remove(&job_id) {
            let _ = self
                .event_tx
                .send(PipelineEvent::DownloadPipelineDrained { job_id });
        }
    }

    fn release_stalled_download_runtime(&mut self, job_id: JobId) -> usize {
        let in_flight = self.active_downloads_by_job.remove(&job_id).unwrap_or(0);
        let in_flight_connections = self
            .active_download_connections_by_job
            .remove(&job_id)
            .unwrap_or(0);
        self.active_downloads = self.active_downloads.saturating_sub(in_flight);
        self.active_download_connections = self
            .active_download_connections
            .saturating_sub(in_flight_connections);
        self.active_downloads_by_file
            .retain(|file_id, _| file_id.job_id != job_id);

        let reserved_segments: Vec<_> = self
            .bandwidth_reservations
            .keys()
            .copied()
            .filter(|segment_id| segment_id.file_id.job_id == job_id)
            .collect();
        for segment_id in reserved_segments {
            if let Err(error) = self.release_bandwidth_reservation(segment_id) {
                error!(
                    error = %error,
                    segment = %segment_id,
                    "failed to release stalled job bandwidth reservation"
                );
            }
        }
        self.rate_limit_reservations
            .retain(|segment_id, _| segment_id.file_id.job_id != job_id);

        in_flight
    }

    pub(crate) fn auto_pause_stalled_downloads(&mut self) {
        let stalled_jobs: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(job_id, state)| {
                if !matches!(state.status, JobStatus::Downloading) || state.health_probing {
                    return None;
                }

                let in_flight = self
                    .active_downloads_by_job
                    .get(job_id)
                    .copied()
                    .unwrap_or(0);
                if in_flight == 0 {
                    return None;
                }

                let elapsed = self.job_last_download_activity.get(job_id)?.elapsed();
                (elapsed >= STALLED_DOWNLOAD_IDLE_THRESHOLD).then_some((
                    *job_id,
                    in_flight,
                    elapsed,
                    state.download_queue.len(),
                ))
            })
            .collect();

        for (job_id, in_flight, elapsed, queued_segments) in stalled_jobs {
            warn!(
                job_id = job_id.0,
                in_flight,
                queued_segments,
                idle_secs = elapsed.as_secs(),
                "auto-pausing stalled download job"
            );

            match self.pause_job_runtime(job_id) {
                Ok(()) => {
                    let released = self.release_stalled_download_runtime(job_id);
                    self.emit_download_finished_if_active(job_id);
                    let _ = self.event_tx.send(PipelineEvent::JobPaused { job_id });
                    info!(
                        job_id = job_id.0,
                        released_downloads = released,
                        "stalled download job paused"
                    );
                }
                Err(error) => {
                    warn!(
                        job_id = job_id.0,
                        error = %error,
                        "failed to auto-pause stalled download job"
                    );
                }
            }
        }
    }

    pub async fn run(&mut self) {
        let mut metrics_snapshot_interval = tokio::time::interval(Self::METRICS_SNAPSHOT_INTERVAL);
        metrics_snapshot_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut tune_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut stalled_download_interval = tokio::time::interval(STALLED_DOWNLOAD_CHECK_INTERVAL);
        stalled_download_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut state_reconcile_interval = tokio::time::interval(Self::STATE_RECONCILE_INTERVAL);
        state_reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pending_download_results = VecDeque::new();

        info!("pipeline started");

        let startup_jobs: Vec<_> = self.jobs.keys().copied().collect();
        for job_id in startup_jobs {
            self.reconcile_job_progress(job_id).await;
        }

        'run_loop: loop {
            self.drain_ready_download_results(&mut pending_download_results);
            self.drain_ready_lane_control_messages();
            self.pump_decode_queue();

            let pending_completion_checks = self.pending_completion_checks.len();
            for _ in 0..pending_completion_checks {
                let Some(job_id) = self.pending_completion_checks.pop_front() else {
                    break;
                };
                self.check_job_completion(job_id).await;
                self.pump_decode_queue();
            }

            self.dispatch_downloads();

            let rate_delay = self.rate_limiter.time_until_ready();
            let rate_sleep = tokio::time::sleep(rate_delay);
            let durable_lead_retry_delay = self.next_restart_durable_lead_retry_delay();
            let durable_lead_retry_sleep = tokio::time::sleep(
                durable_lead_retry_delay.unwrap_or_else(|| std::time::Duration::from_secs(3600)),
            );

            loop {
                match self.cmd_rx.try_recv() {
                    Ok(SchedulerCommand::Shutdown) => {
                        info!("pipeline shutting down");
                        break 'run_loop;
                    }
                    Ok(cmd) => self.handle_command(cmd).await,
                    Err(_) => break,
                }
            }

            self.drain_ready_lane_control_messages();

            if let Some(result) = pending_download_results.pop_front() {
                self.process_released_download_done(result).await;
            } else {
                tokio::select! {
                    cmd = self.cmd_rx.recv() => {
                        match cmd {
                            Some(SchedulerCommand::Shutdown) | None => {
                                info!("pipeline shutting down");
                                break;
                            }
                            Some(cmd) => self.handle_command(cmd).await,
                        }
                    }
                    Some(result) = self.download_done_rx.recv() => {
                        self.release_download_result(&result);
                        self.note_released_download_result_pending(
                            result.segment_id.file_id.job_id,
                            Self::released_download_result_lead_bytes(&result),
                        );
                        pending_download_results.push_back(result);
                    }
                    Some(request) = self.download_refill_rx.recv() => {
                        self.drain_ready_download_results(&mut pending_download_results);
                        self.handle_download_lane_refill_request(request);
                    }
                    Some(parked) = self.download_lane_parked_rx.recv() => {
                        self.handle_download_lane_parked(parked);
                    }
                    Some(event) = self.owned_download_lane_event_rx.recv() => {
                        self.handle_owned_download_lane_event(
                            event,
                            &mut pending_download_results,
                        );
                    }
                    Some(event) = self.ip_replacement_trial_rx.recv() => {
                        self.handle_ip_replacement_trial_event(event);
                    }
                    Some(result) = self.decode_done_rx.recv() => {
                        crate::runtime::perf_probe::record(
                            "download.decode.done_rx.received",
                            std::time::Duration::from_nanos(1),
                        );
                        self.handle_decode_done(result).await;
                    }
                    Some(update) = self.probe_result_rx.recv() => {
                        self.handle_probe_update(update);
                    }
                    Some(done) = self.extract_done_rx.recv() => {
                        self.handle_extraction_done(done).await;
                    }
                    Some(done) = self.rar_refresh_done_rx.recv() => {
                        self.handle_rar_refresh_done(done).await;
                    }
                    Some(retry) = self.rar_capacity_retry_rx.recv() => {
                        self.handle_rar_capacity_retry(retry).await;
                    }
                    Some(done) = self.verified_suspect_persist_done_rx.recv() => {
                        self.handle_verified_suspect_persist_done(done);
                    }
                    Some(done) = self.move_done_rx.recv() => {
                        self.handle_move_to_complete_done(done);
                    }
                    Some(retry) = self.retry_rx.recv() => {
                        self.receive_retry_work(retry);
                    }
                    _ = metrics_snapshot_interval.tick() => {
                        self.sample_phase_progress();
                        self.shared_state.refresh_metrics_snapshot();
                        // Fallback wake for refills held under hard pressure, in
                        // case the backlog drained without a download event.
                        self.maybe_service_deferred_lane_refills();
                    }
                    _ = rate_sleep, if !rate_delay.is_zero() => {}
                    _ = durable_lead_retry_sleep, if durable_lead_retry_delay.is_some() => {}
                    _ = tune_interval.tick() => {
                        self.flush_quiescent_write_backlog().await;
                        self.refresh_download_pressure();

                        let snapshot = self.metrics.snapshot();
                        let not_found = snapshot.articles_not_found;
                        let min_health: Option<u32> = self.jobs.values()
                            .filter(|s| !matches!(s.status, JobStatus::Failed { .. } | JobStatus::Complete))
                            .filter(|s| s.spec.total_bytes > 0)
                            .map(|s| (s.spec.total_bytes.saturating_sub(s.failed_bytes) * 1000 / s.spec.total_bytes) as u32)
                            .min();
                        debug!(
                            active = self.active_downloads,
                            queue = snapshot.download_queue_depth,
                            speed_mbps = format!("{:.1}", snapshot.current_download_speed as f64 / (1024.0 * 1024.0)),
                            downloaded_mb = snapshot.bytes_downloaded / (1024 * 1024),
                            segments = snapshot.segments_downloaded,
                            decode_pending = snapshot.decode_pending,
                            decode_pending_mb = snapshot.decode_pending_bytes / (1024 * 1024),
                            decode_active_mb = snapshot.decode_active_bytes / (1024 * 1024),
                            write_backlog_mb = snapshot.write_buffered_bytes / (1024 * 1024),
                            write_backlog_segments = snapshot.write_buffered_segments,
                            pressure_state = snapshot.download_pressure_state.as_str(),
                            pressure_reason = snapshot.download_pressure_reason.as_str(),
                            direct_write_evictions = snapshot.direct_write_evictions,
                            hot_dispatch_job_id = snapshot.hot_dispatch_job_id,
                            hot_dispatch_mode = snapshot.hot_dispatch_mode.as_str(),
                            hot_dispatch_lent_connections = snapshot.hot_dispatch_lent_connections,
                            hot_dispatch_underfill_ms = snapshot.hot_dispatch_underfill_ms,
                            hot_dispatch_warmup_complete = snapshot.hot_dispatch_warmup_complete,
                            hot_dispatch_speed_bps = snapshot.hot_dispatch_hot_speed_bps,
                            hot_dispatch_exclusive_peak_bps =
                                snapshot.hot_dispatch_exclusive_peak_bps,
                            hot_dispatch_spillover_pre_speed_bps =
                                snapshot.hot_dispatch_spillover_pre_speed_bps,
                            hot_dispatch_spillover_post_speed_bps =
                                snapshot.hot_dispatch_spillover_post_speed_bps,
                            hot_dispatch_spillover_active_loans =
                                snapshot.hot_dispatch_spillover_active_loans,
                            hot_dispatch_recent_expansion_improvement_pct =
                                snapshot.hot_dispatch_recent_expansion_improvement_pct,
                            hot_dispatch_best_mode_block_reason =
                                snapshot.hot_dispatch_best_mode_block_reason,
                            hot_dispatch_last_expansion_kind =
                                snapshot.hot_dispatch_last_expansion_kind,
                            hot_dispatch_last_expansion_before_bps =
                                snapshot.hot_dispatch_last_expansion_before_bps,
                            hot_dispatch_last_expansion_after_bps =
                                snapshot.hot_dispatch_last_expansion_after_bps,
                            hot_dispatch_last_spillover_decision = snapshot
                                .hot_dispatch_last_spillover_decision
                                .as_str(),
                            not_found,
                            health = min_health.map(|h| format!("{:.1}%", h as f64 / 10.0)).unwrap_or_default(),
                            "pipeline tick"
                        );

                        let max = self.tuner.params().max_concurrent_downloads;
                        if self.connection_ramp < max {
                            self.connection_ramp = (self.connection_ramp + 5).min(max);
                            info!(connection_ramp = self.connection_ramp, "ramping up connections");
                        }

                        if self.tuner.adjust(&snapshot) {
                            info!(
                                max_downloads = self.tuner.params().max_concurrent_downloads,
                                "tuner adjusted parameters"
                            );
                        }
                    }
                    _ = stalled_download_interval.tick() => {
                        self.auto_pause_stalled_downloads();
                    }
                    _ = state_reconcile_interval.tick() => {
                        let job_ids: Vec<_> = self.jobs.keys().copied().collect();
                        for (index, job_id) in job_ids.into_iter().enumerate() {
                            self.reconcile_job_progress(job_id).await;
                            if index % 16 == 15 {
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                }
            }

            self.pump_decode_queue();
            self.publish_snapshot();
        }

        self.drain().await;
        info!("pipeline stopped");
    }

    pub(crate) fn next_restart_durable_lead_retry_delay(&self) -> Option<std::time::Duration> {
        let now = Instant::now();
        self.download_restart_durable_lead_retry_after
            .values()
            .copied()
            .min()
            .map(|ready_at| ready_at.saturating_duration_since(now))
    }

    pub(crate) fn publish_snapshot(&mut self) {
        let _ = self.refresh_bandwidth_cap_window();
        self.shared_state.publish_jobs(self.list_jobs());
    }

    fn drain_ready_download_results(&mut self, pending: &mut VecDeque<DownloadResult>) {
        while pending.len() < Self::DOWNLOAD_COMPLETION_DRAIN_LIMIT {
            if let Ok(event) = self.owned_download_lane_event_rx.try_recv() {
                self.handle_owned_download_lane_event(event, pending);
                self.drain_ready_lane_control_messages();
                continue;
            }
            let Ok(result) = self.download_done_rx.try_recv() else {
                break;
            };
            self.release_download_result(&result);
            self.note_released_download_result_pending(
                result.segment_id.file_id.job_id,
                Self::released_download_result_lead_bytes(&result),
            );
            pending.push_back(result);
            self.drain_ready_lane_control_messages();
        }
    }

    fn drain_ready_lane_control_messages(&mut self) {
        loop {
            match self.download_lane_parked_rx.try_recv() {
                Ok(parked) => self.handle_download_lane_parked(parked),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        loop {
            match self.ip_replacement_trial_rx.try_recv() {
                Ok(event) => self.handle_ip_replacement_trial_event(event),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }

        loop {
            match self.download_refill_rx.try_recv() {
                Ok(request) => self.handle_download_lane_refill_request(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
    }

    /// Re-enter a delayed retry into the download queue, dropping stale server
    /// exclusions if the NNTP pool was rebuilt between scheduling and re-entry.
    ///
    /// `exclude_servers` holds pool indices computed under the generation the
    /// retry was scheduled in. A `RebuildNntp` bumps `pool_generation` and can
    /// reshape the pool (shrink/grow/reorder), so those indices may now point at
    /// a different server — excluding the wrong one, or even falsely satisfying
    /// the fill-servers-exhausted gate and spilling onto backfill. When the
    /// generation no longer matches we discard the exclusions and let the retry
    /// fall back to a clean attempt; retention excludes are recomputed fresh
    /// from the new pool at order time, so nothing durable is lost.
    pub(in crate::pipeline) fn receive_retry_work(&mut self, retry: crate::pipeline::RetryWork) {
        let mut work = retry.work;
        if self.server_quota_parked.remove(&work.segment_id) {
            self.refresh_server_quota_block_presentation();
        }
        if retry.scheduled_pool_generation != self.pool_generation
            && !work.exclude_servers.is_empty()
        {
            debug!(
                segment = %work.segment_id,
                scheduled_generation = retry.scheduled_pool_generation,
                current_generation = self.pool_generation,
                "dropping stale server exclusions on retry after NNTP pool rebuild"
            );
            work.exclude_servers.clear();
        }
        self.requeue_retry_work(work);
    }

    pub(crate) fn requeue_retry_work(&mut self, work: DownloadWork) {
        let job_id = work.segment_id.file_id.job_id;
        let segment_id = work.segment_id;
        self.note_retry_requeued(segment_id);
        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
        {
            return;
        }
        let promoted_recovery = work.is_recovery
            && self.is_promoted_recovery_file(job_id, segment_id.file_id.file_index);
        let mark_rar_unlock_dirty =
            !promoted_recovery && self.rar_unlock_requeued_work_is_relevant(&work);
        if let Some(state) = self.jobs.get_mut(&job_id) {
            if promoted_recovery {
                let mut work = work;
                work.priority = crate::pipeline::repair::PROMOTED_RECOVERY_PRIORITY;
                state.download_queue.push(work);
            } else if work.is_recovery {
                state.recovery_queue.push(work);
            } else {
                state.download_queue.push(work);
                if mark_rar_unlock_dirty {
                    self.mark_rar_unlock_priorities_dirty(job_id);
                }
            }
        }
    }

    pub(crate) async fn drain(&mut self) {
        // Unblock lanes waiting on deferred refills so they can finish their
        // batches and exit; dropping the senders answers them with an error.
        self.deferred_lane_refills.clear();
        self.drain_inflight_download_and_decode_work().await;
        self.flush_quiescent_write_backlog().await;

        if self.active_downloads > 0
            || !self.active_decodes_by_job.is_empty()
            || !self.pending_decode.is_empty()
        {
            warn!(
                active_downloads = self.active_downloads,
                active_decodes = self.active_decodes_by_job.values().sum::<usize>(),
                pending_decode = self.pending_decode.len(),
                "shutdown drain ended with in-flight pipeline work"
            );
        } else {
            info!("shutdown drain completed active download/decode work");
        }

        if self.active_downloads > 0 {
            info!(
                active = self.active_downloads,
                "draining in-flight downloads"
            );
        }
        if let Err(error) = self
            .flush_file_progress_batch_awaited("download.file_progress.flush.drain")
            .await
        {
            warn!(error = %error, "failed to flush file progress floors during drain");
        }
        if let Err(error) = self.flush_download_bandwidth_usage() {
            warn!(error = %error, "failed to flush pending bandwidth usage during drain");
        }

        // Join any detached fire-and-forget writes still in flight (e.g. file
        // progress floors flushed above, or runtime writes) so shutdown does not
        // drop them. Bounded so a stuck write cannot hang shutdown forever.
        self.join_fire_and_forget_tasks().await;
    }

    fn has_download_or_decode_drain_work(&self) -> bool {
        self.active_downloads > 0
            || !self.active_decodes_by_job.is_empty()
            || !self.pending_decode.is_empty()
            || self.metrics.decode_pending.load(Ordering::Relaxed) > 0
    }

    async fn drain_inflight_download_and_decode_work(&mut self) {
        self.pump_decode_queue();

        while self.has_download_or_decode_drain_work() {
            let event = tokio::time::timeout(Self::SHUTDOWN_DRAIN_IDLE_TIMEOUT, async {
                tokio::select! {
                    result = self.download_done_rx.recv(), if self.active_downloads > 0 => {
                        result.map(ShutdownDrainEvent::Download)
                    }
                    result = self.owned_download_lane_event_rx.recv(), if self.active_downloads > 0 => {
                        result.map(ShutdownDrainEvent::OwnedDownload)
                    }
                    result = self.decode_done_rx.recv(), if !self.active_decodes_by_job.is_empty()
                        || !self.pending_decode.is_empty()
                        || self.metrics.decode_pending.load(Ordering::Relaxed) > 0 => {
                        result.map(ShutdownDrainEvent::Decode)
                    }
                }
            })
            .await;

            match event {
                Ok(Some(ShutdownDrainEvent::Download(result))) => {
                    self.handle_download_done(result).await;
                    self.pump_decode_queue();
                }
                Ok(Some(ShutdownDrainEvent::OwnedDownload(event))) => {
                    let mut pending = VecDeque::new();
                    self.handle_owned_download_lane_event(event, &mut pending);
                    while let Some(result) = pending.pop_front() {
                        self.process_released_download_done(result).await;
                    }
                    self.pump_decode_queue();
                }
                Ok(Some(ShutdownDrainEvent::Decode(result))) => {
                    self.handle_decode_done(result).await;
                    self.pump_decode_queue();
                }
                Ok(None) => {
                    warn!("shutdown drain channel closed before in-flight work completed");
                    break;
                }
                Err(_) => {
                    warn!(
                        active_downloads = self.active_downloads,
                        active_decodes = self.active_decodes_by_job.values().sum::<usize>(),
                        pending_decode = self.pending_decode.len(),
                        decode_pending = self.metrics.decode_pending.load(Ordering::Relaxed),
                        idle_timeout_secs = Self::SHUTDOWN_DRAIN_IDLE_TIMEOUT.as_secs(),
                        "timed out waiting for in-flight download/decode work during shutdown"
                    );
                    break;
                }
            }
        }
    }
}

enum ShutdownDrainEvent {
    Download(DownloadResult),
    OwnedDownload(OwnedDownloadLaneEvent),
    Decode(DecodeDone),
}

pub(crate) async fn write_segment_to_disk(
    path: &std::path::Path,
    offset: u64,
    segment: BufferedDecodedSegment,
) -> Result<BufferedDecodedSegment, std::io::Error> {
    let path = path.to_owned();
    let started = Instant::now();
    let result = disk_write_owner_pool()
        .write_batch(path, vec![(offset, segment)])
        .await
        .and_then(|mut written| {
            written
                .pop()
                .map(|(_, segment)| segment)
                .ok_or_else(|| SegmentWriteBatchError {
                    source: std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        "disk write owner returned no written segment",
                    ),
                    written,
                    unwritten: Vec::new(),
                })
        })
        .map_err(|error| error.source);
    crate::runtime::perf_probe::record("download.disk_write.await", started.elapsed());
    result
}

pub(crate) struct SegmentWriteBatchError {
    pub(crate) source: std::io::Error,
    pub(crate) written: Vec<(u64, BufferedDecodedSegment)>,
    pub(crate) unwritten: Vec<(u64, BufferedDecodedSegment)>,
}

const DISK_WRITE_OWNER_THREADS: usize = 8;
// Cached write handles per owner thread. Batches are routed by path hash, so a
// file's handle lives on exactly one thread and the cap bounds total fds at
// DISK_WRITE_OWNER_THREADS * DISK_WRITE_HANDLE_CACHE_CAP.
const DISK_WRITE_HANDLE_CACHE_CAP: usize = 16;
// Backstop for eviction seams that have no explicit close hook (failed jobs,
// maintenance sweepers): an idle handle never outlives the TTL, so a deleted
// file's inode is released and its path becomes safe to reuse shortly after.
const DISK_WRITE_HANDLE_IDLE_TTL: std::time::Duration = std::time::Duration::from_secs(30);
const DISK_WRITE_OWNER_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

struct DiskWriteOwnerPool {
    senders: Vec<std::sync::mpsc::Sender<DiskWriteCommand>>,
}

enum DiskWriteCommand {
    Batch {
        path: std::path::PathBuf,
        segments: Vec<(u64, BufferedDecodedSegment)>,
        queued_at: Instant,
        response: tokio::sync::oneshot::Sender<
            Result<Vec<(u64, BufferedDecodedSegment)>, SegmentWriteBatchError>,
        >,
    },
    CloseHandles {
        scope: CloseHandleScope,
        ack: Option<tokio::sync::oneshot::Sender<()>>,
    },
}

#[derive(Clone)]
enum CloseHandleScope {
    Path(std::path::PathBuf),
    Prefix(std::path::PathBuf),
}

impl CloseHandleScope {
    fn matches(&self, path: &std::path::Path) -> bool {
        match self {
            Self::Path(target) => path == target,
            Self::Prefix(prefix) => path.starts_with(prefix),
        }
    }
}

impl DiskWriteOwnerPool {
    fn new() -> Self {
        let mut senders = Vec::with_capacity(DISK_WRITE_OWNER_THREADS);
        for index in 0..DISK_WRITE_OWNER_THREADS {
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::Builder::new()
                .name(format!("weaver-disk-wr-{index}"))
                .spawn(move || run_disk_write_owner(rx))
                .expect("failed to spawn Weaver disk write owner thread");
            senders.push(tx);
        }

        Self { senders }
    }

    // Batches for a path must all land on the owner thread caching its handle;
    // per-thread FIFO then guarantees a queued CloseHandles runs after every
    // batch submitted before it.
    fn owner_index_for_path(&self, path: &std::path::Path) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        path.hash(&mut hasher);
        (hasher.finish() as usize) % self.senders.len()
    }

    async fn write_batch(
        &self,
        path: std::path::PathBuf,
        segments: Vec<(u64, BufferedDecodedSegment)>,
    ) -> Result<Vec<(u64, BufferedDecodedSegment)>, SegmentWriteBatchError> {
        let queued_at = Instant::now();
        crate::runtime::perf_probe::record_value("download.disk_write.owner.submitted", 1);
        let (response, response_rx) = tokio::sync::oneshot::channel();
        let sender_index = self.owner_index_for_path(&path);
        let command = DiskWriteCommand::Batch {
            path,
            segments,
            queued_at,
            response,
        };

        if let Err(error) = self.senders[sender_index].send(command) {
            let DiskWriteCommand::Batch { segments, .. } = error.0 else {
                unreachable!("disk write batch send returned a different command");
            };
            return Err(SegmentWriteBatchError {
                source: std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "disk write owner thread stopped",
                ),
                written: Vec::new(),
                unwritten: segments,
            });
        }

        response_rx.await.unwrap_or_else(|error| {
            Err(SegmentWriteBatchError {
                source: std::io::Error::other(error),
                written: Vec::new(),
                unwritten: Vec::new(),
            })
        })
    }

    fn release_handle(&self, path: &std::path::Path) {
        let index = self.owner_index_for_path(path);
        let _ = self.senders[index].send(DiskWriteCommand::CloseHandles {
            scope: CloseHandleScope::Path(path.to_path_buf()),
            ack: None,
        });
    }

    async fn close_handles_matching(&self, scope: CloseHandleScope) {
        let mut acks = Vec::with_capacity(self.senders.len());
        for sender in &self.senders {
            let (ack, ack_rx) = tokio::sync::oneshot::channel();
            let command = DiskWriteCommand::CloseHandles {
                scope: scope.clone(),
                ack: Some(ack),
            };
            // A send error means the owner thread is gone and its cache (and
            // handles) dropped with it, which is exactly the state we want.
            if sender.send(command).is_ok() {
                acks.push(ack_rx);
            }
        }
        for ack in acks {
            let _ = ack.await;
        }
    }
}

static DISK_WRITE_OWNER_POOL: std::sync::OnceLock<DiskWriteOwnerPool> = std::sync::OnceLock::new();

fn disk_write_owner_pool() -> &'static DiskWriteOwnerPool {
    DISK_WRITE_OWNER_POOL.get_or_init(DiskWriteOwnerPool::new)
}

/// Drop the cached write handle for `path` on its owner thread, if any.
/// Fire-and-forget: queued behind any in-flight batches for the path, so the
/// handle closes only after those writes land.
pub(crate) fn release_cached_write_handle(path: &std::path::Path) {
    let Some(pool) = DISK_WRITE_OWNER_POOL.get() else {
        return;
    };
    pool.release_handle(path);
}

/// Close every cached write handle for paths under `dir` and wait until the
/// owner threads acknowledge. Must be awaited before renaming, moving, or
/// deleting a job's working files: working-dir paths are reused verbatim after
/// deletion, and a stale cached handle would silently swallow a later job's
/// writes into the old unlinked inode.
pub(crate) async fn close_cached_write_handles_under(dir: &std::path::Path) {
    let Some(pool) = DISK_WRITE_OWNER_POOL.get() else {
        return;
    };
    pool.close_handles_matching(CloseHandleScope::Prefix(dir.to_path_buf()))
        .await;
}

struct CachedDiskWriteHandle {
    file: std::fs::File,
    last_used: Instant,
}

#[derive(Default)]
struct DiskWriteHandleCache {
    entries: std::collections::HashMap<std::path::PathBuf, CachedDiskWriteHandle>,
}

impl DiskWriteHandleCache {
    fn open_or_reuse(&mut self, path: &std::path::Path) -> std::io::Result<&mut std::fs::File> {
        if !self.entries.contains_key(path) {
            crate::runtime::perf_probe::record_value("download.disk_write.handle_cache.miss", 1);
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(path)?;
            self.evict_to_cap();
            self.entries.insert(
                path.to_path_buf(),
                CachedDiskWriteHandle {
                    file,
                    last_used: Instant::now(),
                },
            );
        } else {
            crate::runtime::perf_probe::record_value("download.disk_write.handle_cache.hit", 1);
        }
        let entry = self
            .entries
            .get_mut(path)
            .expect("cached disk write handle present after insert");
        entry.last_used = Instant::now();
        Ok(&mut entry.file)
    }

    fn discard(&mut self, path: &std::path::Path) {
        self.entries.remove(path);
    }

    fn close_matching(&mut self, scope: &CloseHandleScope) {
        self.entries.retain(|path, _| !scope.matches(path));
    }

    fn close_idle(&mut self, ttl: std::time::Duration) {
        if self.entries.is_empty() {
            return;
        }
        let now = Instant::now();
        self.entries
            .retain(|_, entry| now.duration_since(entry.last_used) < ttl);
    }

    fn evict_to_cap(&mut self) {
        while self.entries.len() >= DISK_WRITE_HANDLE_CACHE_CAP {
            let Some(least_recent) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(path, _)| path.clone())
            else {
                break;
            };
            crate::runtime::perf_probe::record_value("download.disk_write.handle_cache.evicted", 1);
            self.entries.remove(&least_recent);
        }
    }
}

fn run_disk_write_owner(rx: std::sync::mpsc::Receiver<DiskWriteCommand>) {
    crate::runtime::affinity::pin_current_thread_for_hot_download_path();
    let mut handles = DiskWriteHandleCache::default();
    loop {
        match rx.recv_timeout(DISK_WRITE_OWNER_SWEEP_INTERVAL) {
            Ok(DiskWriteCommand::Batch {
                path,
                segments,
                queued_at,
                response,
            }) => {
                let result =
                    write_segments_to_disk_blocking(&mut handles, path, segments, queued_at);
                let _ = response.send(result);
            }
            Ok(DiskWriteCommand::CloseHandles { scope, ack }) => {
                handles.close_matching(&scope);
                if let Some(ack) = ack {
                    let _ = ack.send(());
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
        handles.close_idle(DISK_WRITE_HANDLE_IDLE_TTL);
    }
}

fn write_segments_to_disk_blocking(
    handles: &mut DiskWriteHandleCache,
    path: std::path::PathBuf,
    segments: Vec<(u64, BufferedDecodedSegment)>,
    queued_at: Instant,
) -> Result<Vec<(u64, BufferedDecodedSegment)>, SegmentWriteBatchError> {
    let task_started = Instant::now();
    crate::runtime::perf_probe::record(
        "download.disk_write.owner.queue_wait",
        task_started.duration_since(queued_at),
    );
    let _cpu_scope = crate::runtime::perf_probe::cpu_scope("download.disk_write.task");

    let file = match handles.open_or_reuse(&path) {
        Ok(file) => file,
        Err(source) => {
            return Err(SegmentWriteBatchError {
                source,
                written: Vec::new(),
                unwritten: segments,
            });
        }
    };

    let result = write_segments_into_file(file, segments);
    if result.is_err() {
        // A failed seek/write leaves the handle in an unknown state (the path
        // may even have vanished); drop it so the next batch reopens fresh,
        // matching the old open-per-batch recovery behavior.
        handles.discard(&path);
        return result;
    }

    crate::runtime::perf_probe::record("download.disk_write.task", task_started.elapsed());
    result
}

fn write_segments_into_file(
    file: &mut std::fs::File,
    segments: Vec<(u64, BufferedDecodedSegment)>,
) -> Result<Vec<(u64, BufferedDecodedSegment)>, SegmentWriteBatchError> {
    use std::io::Seek;

    let mut written = Vec::with_capacity(segments.len());
    let mut next_file_offset = None;
    let mut remaining = segments.into_iter();
    while let Some((offset, segment)) = remaining.next() {
        let segment_len = segment.data.len_bytes() as u64;
        if next_file_offset != Some(offset)
            && let Err(source) = file.seek(std::io::SeekFrom::Start(offset))
        {
            let mut unwritten = Vec::with_capacity(1 + remaining.size_hint().0);
            unwritten.push((offset, segment));
            unwritten.extend(remaining);
            return Err(SegmentWriteBatchError {
                source,
                written,
                unwritten,
            });
        }
        if let Err(source) = segment.data.write_to(file) {
            let mut unwritten = Vec::with_capacity(1 + remaining.size_hint().0);
            unwritten.push((offset, segment));
            unwritten.extend(remaining);
            return Err(SegmentWriteBatchError {
                source,
                written,
                unwritten,
            });
        }
        next_file_offset = Some(offset + segment_len);
        written.push((offset, segment));
    }

    Ok(written)
}

pub(crate) async fn write_segments_to_disk(
    path: &std::path::Path,
    segments: Vec<(u64, BufferedDecodedSegment)>,
) -> Result<Vec<(u64, BufferedDecodedSegment)>, SegmentWriteBatchError> {
    let path = path.to_owned();
    let started = Instant::now();
    let result = disk_write_owner_pool().write_batch(path, segments).await;
    crate::runtime::perf_probe::record("download.disk_write.await", started.elapsed());
    result
}

pub(crate) fn compute_write_backlog_budget_bytes(
    profile: &SystemProfile,
    buffers: &Arc<BufferPool>,
) -> usize {
    use crate::runtime::system_profile::StorageClass;

    // The write backlog stays scratch-anchored: growing it with memory only
    // defers flushing into a larger end-of-job drain tail (measured as added
    // wall variance), while decode-side scaling is what prevents pressure
    // latch cycles during download waves.
    let scratch_bytes = buffer_pool_total_bytes(buffers);
    let available_bytes = profile.memory.available_bytes.max(256 * 1024 * 1024) as usize;
    let base = scratch_bytes
        .max(64 * 1024 * 1024)
        .min((available_bytes / 8).max(64 * 1024 * 1024));

    match profile.disk.storage_class {
        StorageClass::Ssd => base,
        StorageClass::Hdd => (base.saturating_mul(2)).min((available_bytes / 4).max(base)),
        StorageClass::Network | StorageClass::Unknown => {
            (base.saturating_mul(3) / 2).min((available_bytes / 5).max(base))
        }
    }
}

pub(crate) fn compute_decode_backlog_budget_bytes(
    profile: &SystemProfile,
    buffers: &Arc<BufferPool>,
    write_backlog_budget_bytes: usize,
) -> usize {
    const DECODE_BACKLOG_POOL_MULTIPLIER: usize = 3;
    const DECODE_BACKLOG_MEMORY_FRACTION: usize = 8;
    const DECODE_BACKLOG_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;

    let scratch_bytes = buffer_pool_total_bytes(buffers);
    let mut available_bytes = profile.memory.available_bytes.max(256 * 1024 * 1024);
    if let Some(cgroup_limit) = profile.memory.cgroup_limit {
        available_bytes = available_bytes.min(cgroup_limit);
    }
    let available_bytes = available_bytes as usize;
    // A full download wave (every lane's leased runway decoding at once) must
    // fit under the soft limit on machines with memory to spare, or the
    // pressure latch cycles on every wave; scale the budget with available
    // memory instead of pinning it to buffer-pool scratch alone.
    let scaled = available_bytes / 12;
    let memory_cap = (available_bytes / DECODE_BACKLOG_MEMORY_FRACTION)
        .max(write_backlog_budget_bytes)
        .min(DECODE_BACKLOG_MAX_BYTES);
    let target = scratch_bytes
        .saturating_mul(DECODE_BACKLOG_POOL_MULTIPLIER)
        .max(scaled)
        .max(write_backlog_budget_bytes);

    target.min(memory_cap).max(1)
}

fn buffer_pool_total_bytes(buffers: &Arc<BufferPool>) -> usize {
    use crate::runtime::buffers::BufferTier;

    let metrics = buffers.metrics();
    metrics.small_total * BufferTier::Small.size_bytes()
        + metrics.medium_total * BufferTier::Medium.size_bytes()
        + metrics.large_total * BufferTier::Large.size_bytes()
}

pub(crate) fn check_disk_space(output_dir: &std::path::Path, needed_bytes: u64) {
    match crate::operations::disk_space(output_dir) {
        Some(space) => {
            let available = space.available_bytes;
            if available < needed_bytes {
                let avail_mb = available / (1024 * 1024);
                let need_mb = needed_bytes / (1024 * 1024);
                warn!(
                    available_mb = avail_mb,
                    needed_mb = need_mb,
                    "output directory may not have enough free disk space"
                );
            } else {
                let avail_mb = available / (1024 * 1024);
                debug!(available_mb = avail_mb, "disk space check passed");
            }
        }
        None => debug!("could not check free disk space"),
    }
}

pub(crate) fn timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn is_terminal_status(status: &JobStatus) -> bool {
    matches!(status, JobStatus::Complete | JobStatus::Failed { .. })
}

#[cfg(test)]
mod disk_write_handle_cache_tests {
    use super::*;
    use crate::jobs::ids::{JobId, NzbFileId, SegmentId};

    fn segment(bytes: &[u8]) -> BufferedDecodedSegment {
        BufferedDecodedSegment {
            segment_id: SegmentId {
                file_id: NzbFileId {
                    job_id: JobId(1),
                    file_index: 0,
                },
                segment_number: 1,
            },
            decoded_size: bytes.len() as u32,
            data: DecodedChunk::from(bytes.to_vec()),
            part_crc: 0,
            part_crc_verified: false,
            yenc_name: String::new(),
        }
    }

    #[test]
    fn batches_reuse_cached_handle_and_preserve_prior_writes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("part.bin");
        let mut cache = DiskWriteHandleCache::default();

        let first = write_segments_to_disk_blocking(
            &mut cache,
            path.clone(),
            vec![(0, segment(b"hello"))],
            Instant::now(),
        )
        .map_err(|error| error.source)
        .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(cache.entries.len(), 1);

        let second = write_segments_to_disk_blocking(
            &mut cache,
            path.clone(),
            vec![(5, segment(b" world"))],
            Instant::now(),
        )
        .map_err(|error| error.source)
        .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(cache.entries.len(), 1);

        assert_eq!(std::fs::read(&path).unwrap(), b"hello world");
    }

    #[test]
    fn open_failure_returns_all_segments_unwritten_and_caches_nothing() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("missing-dir").join("part.bin");
        let mut cache = DiskWriteHandleCache::default();

        let error = write_segments_to_disk_blocking(
            &mut cache,
            path,
            vec![(0, segment(b"a")), (1, segment(b"b"))],
            Instant::now(),
        )
        .map(|written| written.len())
        .unwrap_err();

        assert!(error.written.is_empty());
        assert_eq!(error.unwritten.len(), 2);
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn write_failure_splits_batch_and_discards_cached_handle() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("part.bin");
        std::fs::write(&path, b"seed").unwrap();

        // A read-only handle makes every write fail while seeks still succeed,
        // standing in for a handle that went bad mid-batch.
        let mut cache = DiskWriteHandleCache::default();
        cache.entries.insert(
            path.clone(),
            CachedDiskWriteHandle {
                file: std::fs::File::open(&path).unwrap(),
                last_used: Instant::now(),
            },
        );

        let error = write_segments_to_disk_blocking(
            &mut cache,
            path.clone(),
            vec![(0, segment(b"a")), (1, segment(b"b"))],
            Instant::now(),
        )
        .map(|written| written.len())
        .unwrap_err();

        assert!(error.written.is_empty());
        assert_eq!(error.unwritten.len(), 2);
        assert!(
            cache.entries.is_empty(),
            "failed handle must be dropped so the next batch reopens the path"
        );
    }

    #[test]
    fn cache_evicts_least_recent_handle_at_cap() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = DiskWriteHandleCache::default();

        for index in 0..DISK_WRITE_HANDLE_CACHE_CAP + 2 {
            let path = temp.path().join(format!("part-{index}.bin"));
            cache.open_or_reuse(&path).unwrap();
        }

        assert_eq!(cache.entries.len(), DISK_WRITE_HANDLE_CACHE_CAP);
        let newest = temp
            .path()
            .join(format!("part-{}.bin", DISK_WRITE_HANDLE_CACHE_CAP + 1));
        assert!(cache.entries.contains_key(&newest));
    }

    #[test]
    fn close_matching_honors_path_and_prefix_scopes() {
        let temp = tempfile::tempdir().unwrap();
        let job_a = temp.path().join("job-a");
        let job_b = temp.path().join("job-b");
        std::fs::create_dir_all(&job_a).unwrap();
        std::fs::create_dir_all(&job_b).unwrap();

        let mut cache = DiskWriteHandleCache::default();
        let a1 = job_a.join("part-1.bin");
        let a2 = job_a.join("part-2.bin");
        let b1 = job_b.join("part-1.bin");
        for path in [&a1, &a2, &b1] {
            cache.open_or_reuse(path).unwrap();
        }

        cache.close_matching(&CloseHandleScope::Prefix(job_a.clone()));
        assert_eq!(cache.entries.len(), 1);
        assert!(cache.entries.contains_key(&b1));

        cache.close_matching(&CloseHandleScope::Path(b1.clone()));
        assert!(cache.entries.is_empty());
    }

    #[test]
    fn close_idle_drops_handles_past_ttl() {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = DiskWriteHandleCache::default();
        cache.open_or_reuse(&temp.path().join("part.bin")).unwrap();

        cache.close_idle(std::time::Duration::from_secs(3600));
        assert_eq!(cache.entries.len(), 1);

        cache.close_idle(std::time::Duration::ZERO);
        assert!(cache.entries.is_empty());
    }
}
