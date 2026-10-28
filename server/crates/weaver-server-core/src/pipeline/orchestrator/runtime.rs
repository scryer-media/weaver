// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const UU_SPOOL_MIN_BYTES: usize = 64 * 1024 * 1024;
const UU_SPOOL_MAX_BYTES: usize = 4 * 1024 * 1024 * 1024;
const UU_SPOOL_WRITE_BACKLOG_MULTIPLIER: usize = 8;
const UU_SPOOL_MIN_SEGMENTS: usize = 1_024;
const UU_SPOOL_MAX_SEGMENTS: usize = 16_384;
const UU_SPOOL_SEGMENT_MULTIPLIER: usize = 64;
const UU_SPOOL_MIN_FREE_BYTES: u64 = 512 * 1024 * 1024;

fn compute_uu_spool_max_bytes(write_backlog_budget_bytes: usize) -> usize {
    write_backlog_budget_bytes
        .saturating_mul(UU_SPOOL_WRITE_BACKLOG_MULTIPLIER)
        .clamp(UU_SPOOL_MIN_BYTES, UU_SPOOL_MAX_BYTES)
}

fn compute_uu_spool_max_segments(write_buf_max_pending: usize) -> usize {
    write_buf_max_pending
        .saturating_mul(UU_SPOOL_SEGMENT_MULTIPLIER)
        .clamp(UU_SPOOL_MIN_SEGMENTS, UU_SPOOL_MAX_SEGMENTS)
}

impl Pipeline {
    const METRICS_SNAPSHOT_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
    // The metrics tick while nothing is moving: often enough for the speed
    // gauges to decay and for a time-lifted dispatch gate to be noticed,
    // rare enough that an idle pipeline costs next to nothing.
    pub(crate) const IDLE_SNAPSHOT_INTERVAL: std::time::Duration =
        std::time::Duration::from_secs(1);
    const STATE_RECONCILE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
    const SHUTDOWN_DRAIN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    // Bound on how long `drain` waits to join detached fire-and-forget writes.
    const FIRE_AND_FORGET_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    const DOWNLOAD_COMPLETION_DRAIN_LIMIT: usize = 128;
    // Released results processed per orchestrator turn before the turn's
    // dispatch pass, barrier poll and chase reap run again. One result per
    // turn made every lane batch of 64 articles cost 64 full dispatch passes
    // that, with the lanes refilling themselves, almost never had a
    // connection to fill. Sixteen bounds the added dispatch latency to the
    // time it takes to ingest sixteen results while dividing the pass count
    // by the same factor.
    const DOWNLOAD_RESULTS_PER_TURN: usize = 16;

    // Create a new pipeline.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
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
        let (hardware_profile, hold_settings, propagation_delay) = {
            let cfg = config.read().await;
            // An install that never chose one — every upgraded install — runs
            // the profile this machine is recommended. A saved choice the
            // machine can no longer honour falls back the same way.
            let hardware_profile = cfg
                .hardware_profile
                .filter(|chosen| chosen.unmet_requirement(&profile).is_none())
                .unwrap_or_else(|| crate::runtime::HardwareProfile::recommended(&profile));
            // The resident holds default is sized once, under the profile in
            // force at startup, and every hold setting is resolved once and
            // held for the life of the pipeline.
            let resident_default_cap = hardware_profile
                .tuning(&profile)
                .direct_store_resident_default_cap_bytes;
            (
                hardware_profile,
                crate::pipeline::graph::limits::HoldSettings::resolve(
                    &cfg,
                    crate::pipeline::graph::limits::HostFacts::probe(
                        &intermediate_dir,
                        resident_default_cap,
                    ),
                ),
                Duration::from_secs(u64::from(cfg.propagation_delay_secs())),
            )
        };
        let nzb_limit = config.read().await.max_concurrent_nzbs();
        shared_state.queue.write(|inner| {
            inner.globals.max_concurrent_nzbs = std::num::NonZeroU32::new(nzb_limit);
            queue::Change::changed(())
        });
        let admission_db = db.clone();
        let nzb_admitted =
            tokio::task::spawn_blocking(move || admission_db.load_nzb_admissions()).await??;
        let admission_db = db.clone();
        let nzb_pause_releases =
            tokio::task::spawn_blocking(move || admission_db.load_nzb_pause_releases()).await??;
        let download_stall_recovery = download::download_stall_recovery_from_env_value(
            std::env::var(download::DOWNLOAD_STALL_RECOVERY_ENV).ok(),
        );
        info!(
            download_stall_recovery,
            env = download::DOWNLOAD_STALL_RECOVERY_ENV,
            "download dispatch stall recovery {} (on by default; {}=0, false or off disables it)",
            if download_stall_recovery {
                "enabled"
            } else {
                "disabled"
            },
            download::DOWNLOAD_STALL_RECOVERY_ENV,
        );
        let profile_tuning = hardware_profile.tuning(&profile);
        let tuner = RuntimeTuner::with_profile_tuning(profile, total_connections, profile_tuning);
        shared_state.set_sevenz_decode_memory_bytes(profile_tuning.sevenz_decode_memory_bytes);
        shared_state.set_hardware_profile_in_force(crate::HardwareProfileInForce {
            active: hardware_profile,
            scheduled: None,
        });
        info!(
            hardware_profile = hardware_profile.as_str(),
            max_downloads = tuner.params().max_concurrent_downloads,
            decode_threads = tuner.params().decode_thread_count,
            extract_threads = tuner.params().extract_thread_count,
            sevenz_decode_memory_mb = profile_tuning.sevenz_decode_memory_bytes / (1024 * 1024),
            decode_backlog_budget_mb = decode_backlog_budget_bytes / (1024 * 1024),
            write_backlog_budget_mb = write_backlog_budget_bytes / (1024 * 1024),
            total_connections,
            "pipeline tuner initialized"
        );
        info!(
            holds_scratch_ceiling_bytes = hold_settings.scratch_ceiling_bytes,
            holds_resident_limit_bytes = hold_settings.resident_limit_bytes,
            holds_scratch_total_bytes = hold_settings.scratch_total_bytes,
            holds_disk_reserve_bytes = hold_settings.disk_reserve_bytes,
            "graph engine hold limits resolved"
        );

        tokio::fs::create_dir_all(&data_dir).await?;
        // A download folder on a drive that is gone must not stop Weaver from
        // starting, or it could never be pointed at another one.
        for dir in [&intermediate_dir, &complete_dir] {
            if let Err(error) = tokio::fs::create_dir_all(dir).await {
                warn!(path = %dir.display(), error = %error, "download folder unavailable");
            }
        }
        let storage_capacity = Arc::new(crate::operations::StorageCapacity::start(
            data_dir.clone(),
            intermediate_dir.clone(),
            complete_dir.clone(),
        ));
        let uu_spool_root = intermediate_dir.join(".uu-park");
        let cleanup_root = uu_spool_root.clone();
        if let Err(error) =
            tokio::task::spawn_blocking(move || clear_stale_uu_park_root(&cleanup_root)).await?
        {
            warn!(path = %uu_spool_root.display(), error = %error, "failed to clear the stale UU spool");
        }
        let extraction_limits = Arc::new(ExtractionLimits::from_env_with_profile_ceiling(
            &complete_dir,
            profile_tuning.extraction_memory_bytes,
        )?);
        let process_memory_budget =
            Arc::new(ProcessMemoryBudget::new(extraction_limits.max_memory_bytes));

        let (download_done_tx, download_done_rx) = mpsc::channel(256);
        let (download_refill_tx, download_refill_rx) = mpsc::channel(256);
        let (download_lane_parked_tx, download_lane_parked_rx) = mpsc::channel(256);
        let (owned_download_lane_event_tx, owned_download_lane_event_rx) = mpsc::channel(128);
        let (decode_done_tx, decode_done_rx) = mpsc::channel(256);
        let (retry_tx, retry_rx) = mpsc::channel(256);
        let (nntp_handoff_drained_tx, nntp_handoff_drained_rx) = mpsc::channel(4);
        let (move_done_tx, move_done_rx) = mpsc::channel(32);
        let (job_activated_tx, job_activated_rx) = mpsc::channel(32);
        let (terminal_post_processing_done_tx, terminal_post_processing_done_rx) =
            mpsc::channel(32);
        let script_effects_rx = db.subscribe_script_effects();
        {
            let db = db.clone();
            tokio::task::spawn_blocking(move || {
                db.warm_script_effects()?;
                db.warm_script_dispatch_jobs()
            })
            .await??;
        }
        let scripts_directory = db
            .initialize_post_processing_script_directory(
                &std::path::PathBuf::from(&data_dir),
                None,
            )
            .unwrap_or_else(|error| {
                warn!(error = %error, "failed to initialize post-processing scripts directory; using default");
                std::path::PathBuf::from(&data_dir).join("scripts")
            });
        let terminal_post_processing_executor =
            crate::post_processing::executor::PostProcessingExecutor::new(
                db.clone(),
                scripts_directory,
            );
        if let Err(error) = terminal_post_processing_executor.recover_interrupted() {
            warn!(error = %error, "failed to mark interrupted post-processing jobs");
        }
        if let Err(error) = db.recover_script_events() {
            warn!(%error, "failed to mark interrupted queue scripts");
        }
        if let Err(error) = crate::post_processing::events::refresh_admission_hint(&db) {
            warn!(%error, "failed to initialize queue script admission hint");
        }
        crate::post_processing::events::wake_queue(db.clone());
        let nntp = Arc::new(nntp);
        shared_state.set_nntp_runtime_activation(NntpRuntimeActivation {
            generation: 0,
            configured_connections: total_connections,
        });
        let server_counters = Self::activate_server_counters(&metrics, &nntp);
        let owned_download_lane_pool =
            download::owned_lane::OwnedDownloadLanePool::new(total_connections.max(1));
        shared_state.set_paused(initial_global_paused);
        let graph_hardware = crate::pipeline::graph::hardware::Hardware::of(&tuner);

        let graph_holds = Arc::new(weaver_graph::holds::HoldsAccountant::new(
            hold_settings.budgets(),
        ));
        let mut pipeline = Self {
            _control_runtime: crate::pipeline::queue::RuntimeGuard(shared_state.queue.clone()),
            event_tx,
            nntp,
            buffers,
            tuner,
            configured_hardware_profile: hardware_profile,
            scheduled_hardware_profile: None,
            metrics,
            jobs: HashMap::new(),
            graph_jobs: HashMap::new(),
            graph_status: Default::default(),
            graph_holds,
            graph_runs: Default::default(),
            post_probes: Default::default(),
            nzb_admitted,
            nzb_seen: HashSet::new(),
            nzb_pause_releases,
            nzb_added_scripts_pending: HashSet::new(),
            nzb_reprocess_pending: HashSet::new(),
            semantic_terminal_causes: HashMap::new(),
            pipeline_failure_stages: HashMap::new(),
            job_order: Vec::new(),
            active_downloads: 0,
            active_download_connections: 0,
            active_completion_critical_connections: 0,
            active_recovery: 0,
            download_lane_runtime: DownloadLaneRuntimeState::default(),
            held_download_refills: Vec::new(),
            download_dispatch_wake: false,
            nntp_handoff_draining: false,
            active_download_passes: HashSet::new(),
            jobs_finalizing_download: HashSet::new(),
            pending_released_download_results_by_job: HashMap::new(),
            pending_released_download_result_bytes_by_job: HashMap::new(),
            active_downloads_by_job: HashMap::new(),
            active_download_connections_by_job: HashMap::new(),
            active_completion_critical_connections_by_job: HashMap::new(),
            active_downloads_by_file: HashMap::new(),
            active_decodes_by_job: HashMap::new(),
            active_decodes_by_file: HashMap::new(),
            active_decode_bytes: HashMap::new(),
            job_last_download_activity: HashMap::new(),
            pending_retries_by_job: HashMap::new(),
            pending_retries_by_segment: HashMap::new(),
            transport_failure_streaks: HashMap::new(),
            download_wait_by_job: HashMap::new(),
            segment_terminal_states: HashMap::new(),
            terminal_reconciliations: HashMap::new(),
            files_counted_missing: HashSet::new(),
            server_quota_parked: HashSet::new(),
            egress_quota_parked: HashMap::new(),
            uu_spool_capacity: storage_capacity
                .reader(crate::operations::StorageRoot::Intermediate),
            uu_spool_debits: crate::operations::CapacityDebits::default(),
            uu_spool_blocked_spill_bytes: None,
            intermediate_dir,
            complete_dir,
            nzb_dir: data_dir.join(".weaver-nzbs"),
            uu_spool_root,
            uu_spool_max_bytes: compute_uu_spool_max_bytes(write_backlog_budget_bytes),
            uu_spool_max_segments: compute_uu_spool_max_segments(write_buf_max_pending),
            uu_park_max_segments: compute_uu_spool_max_segments(write_buf_max_pending),
            uu_spool_min_free_bytes: UU_SPOOL_MIN_FREE_BYTES,
            #[cfg(test)]
            uu_spool_available_bytes_for_test: None,
            dirty_server_attribution: HashSet::new(),
            dirty_support_facts: HashSet::new(),
            server_attribution_checkpoint_at: Instant::now(),
            #[cfg(test)]
            par2_ignore_extensions_override: None,
            pending_decode: VecDeque::new(),
            pending_completion_checks: VecDeque::new(),
            blocked_restores: HashMap::new(),
            download_done_tx,
            download_done_rx,
            download_refill_tx,
            download_refill_rx,
            download_lane_parked_tx,
            download_lane_parked_rx,
            owned_download_lane_event_tx,
            owned_download_lane_event_rx,
            owned_download_lane_pool,
            decode_done_tx,
            decode_done_rx,
            retry_tx,
            retry_rx,
            infrastructure_retries: infrastructure_retry::InfrastructureRetryQueue::default(),
            pool_generation: 0,
            server_counters,
            job_stage_started_at: HashMap::new(),
            nntp_handoff_drained_tx,
            nntp_handoff_drained_rx,
            job_activated_tx,
            job_activated_rx,
            move_done_tx,
            move_done_rx,
            terminal_post_processing_done_tx,
            terminal_post_processing_done_rx,
            script_effects_rx,
            terminal_post_processing_executor,
            inflight_terminal_post_processing: HashSet::new(),
            queue_script_waiters: HashSet::new(),
            queue_scripts_completed: HashSet::new(),
            added_script_holds: HashSet::new(),
            script_data_dir: data_dir.clone(),
            terminal_post_processing_cancellations: HashMap::new(),
            global_paused: initial_global_paused,
            applied_speed_test_pause: false,
            applied_random_read_iops: None,
            scheduled_pause: false,
            rate_limit_reservations: HashMap::new(),
            configured_rate_limit: 0,
            scheduled_rate_limit: None,
            rate_limiter: TokenBucket::new(0),
            write_buf_max_pending,
            decode_backlog_budget_bytes,
            write_backlog_budget_bytes,
            download_decode_hard_pressure_latched: false,
            download_write_hard_pressure_latched: false,
            download_pressure_hard_stall_started_at: None,
            download_pressure_soft_dispatch_after: None,
            download_dispatch_retry: true,
            metrics_rates_moving: false,
            post_download_phases: 0,
            reconcile_clean: false,
            reconcile_signatures: HashMap::new(),
            download_restart_durable_lead_retry_after: HashMap::new(),
            checkpoint_progress_articles: HashMap::new(),
            download_lane_owners: HashMap::new(),
            #[cfg(test)]
            audit_lane_holdings: false,
            propagation_ready_at: HashMap::new(),
            propagation_delay,
            #[cfg(test)]
            propagation_delay_forced: None,
            last_download_dispatch_stall_log_at: None,
            download_dispatch_stall: None,
            download_stall_recovery,
            download_pass_fill_servers: None,
            download_pass_provider_policy: None,
            download_retry_storm_window: None,
            owned_lane_acquire_failure_log_throttle: Default::default(),
            dispatch_ineligible_log_throttle: Default::default(),
            last_owned_lane_acquire_failure_at: None,
            download_lanes_under_cap_since: None,
            last_download_lanes_under_cap_log_at: None,
            write_buffered_bytes: 0,
            write_buffered_segments: 0,
            uu_spooled_bytes: 0,
            uu_spooled_segments: 0,
            uu_parked_segments: 0,
            uu_files: HashMap::new(),
            uu_park_requeues: HashMap::new(),
            unanchored_parked: HashMap::new(),
            unanchored_requeues: HashMap::new(),
            pending_unanchored_release: Vec::new(),
            storage_capacity,
            extraction_limits,
            process_memory_budget,
            job_article_memory: HashMap::new(),
            repeated_articles: HashMap::new(),
            decode_retries: HashMap::new(),
            phase_progress: HashMap::new(),
            job_retention_exclude_cache: HashMap::new(),
            no_eligible_server_warn_throttle: Default::default(),
            body_lane_capacity_log_throttle: Default::default(),
            body_fetch_failure_log_throttle: Default::default(),
            inflight_moves: HashSet::new(),
            deferred_moves: HashSet::new(),
            deferred_post_processing: HashSet::new(),
            reserved_complete_destinations: HashMap::new(),
            shared_state,
            db,
            fire_and_forget_tasks: Arc::new(std::sync::Mutex::new(tokio::task::JoinSet::new())),
            config,
            graph_hardware,
        };
        let _ = pipeline
            .shared_state
            .queue
            .resources
            .set(pipeline.admission_resources());
        let initial_history: Arc<[JobInfo]> = initial_history.into();
        pipeline.shared_state.queue.write(|inner| {
            inner.retain_jobs(|row| !is_terminal_status(&row.report.job.status));
            pipeline
                .shared_state
                .queue
                .replace_history(inner, initial_history);
            inner.globals.paused = initial_global_paused;
            inner.globals.speed_limit = pipeline.configured_rate_limit;
            inner.globals.scheduled_speed_limit = pipeline.scheduled_rate_limit;
            inner.globals.propagation_delay = pipeline.propagation_delay;
            inner.globals.hardware_profile = Some(pipeline.configured_hardware_profile);
            inner.globals.scheduled_hardware_profile = pipeline.scheduled_hardware_profile;
            crate::pipeline::queue::Change::changed(())
        });
        pipeline.publish_download_block();
        pipeline.refresh_download_pressure();
        // The very first lease a lane takes must already know the depth its
        // server can run at; discovering it from the first response means the
        // whole opening of every download is sequential.
        pipeline.seed_download_lane_explorers();
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

    // Join all outstanding [`Self::db_fire_and_forget`] writes with a bounded
    // timeout so shutdown does not drop in-flight persistence. Called from
    // `drain` after the explicit awaited flushes. The JoinSet is moved out from
    // under the lock first so no lock is held across the await.
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

    // The background free-space samplers for the data, intermediate and
    // complete roots, for readers outside the pipeline (metrics, the NZBGet
    // status). Reading them never touches the filesystem.
    pub fn storage_capacity(&self) -> Arc<crate::operations::StorageCapacity> {
        Arc::clone(&self.storage_capacity)
    }

    // Point the per-server metric counters at the servers of `nntp`.
    //
    // Called only when an NNTP runtime generation is activated (startup and
    // every `RebuildNntp`), never on an article path. The registry re-uses the
    // counters of any stable server id it has already seen, so a server that
    // survives a config reload keeps its lifetime totals even though its
    // runtime index may have moved.
    pub(in crate::pipeline) fn activate_server_counters(
        metrics: &PipelineMetrics,
        nntp: &NntpClient,
    ) -> Vec<Arc<crate::operations::instrumentation::ServerCounters>> {
        let pool = nntp.pool();
        let stable_ids = (0..pool.server_count())
            .map(|index| {
                pool.stable_server_id(weaver_nntp::pool::ServerId(index))
                    .map(|stable| stable.0)
                    // A pool entry with no durable identity can only come from a
                    // synthetic config; index it by position so its counters are
                    // still addressable rather than dropping the server.
                    .unwrap_or(index as u32)
            })
            .collect::<Vec<_>>();
        metrics.server_metrics.activate(&stable_ids)
    }

    pub fn post_processing_executor(
        &self,
    ) -> crate::post_processing::executor::PostProcessingExecutor {
        self.terminal_post_processing_executor.clone()
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
        state.assembly_progress().max(byte_progress).clamp(0.0, 1.0)
    }

    pub(crate) fn clear_job_progress_floor_runtime(&mut self, job_id: JobId) {
        self.dirty_server_attribution.remove(&job_id);
        self.dirty_support_facts.remove(&job_id);
        self.blocked_restores.remove(&job_id);
        self.download_restart_durable_lead_retry_after
            .remove(&job_id);
        self.checkpoint_progress_articles.remove(&job_id);
        self.propagation_ready_at.remove(&job_id);
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
            self.emit_job_event(PipelineEvent::DownloadFinished {
                job_id,
                finalization_pending,
            });
        }
    }

    pub(crate) fn emit_download_pipeline_drained_if_pending(&mut self, job_id: JobId) {
        if self.jobs_finalizing_download.remove(&job_id) {
            self.emit_job_event(PipelineEvent::DownloadPipelineDrained { job_id });
        }
    }

    fn release_stalled_download_runtime(&mut self, job_id: JobId) -> usize {
        self.retire_stalled_download_lanes(job_id)
    }

    // The periodic safety net for a completion check that no event
    // scheduled.
    //
    // Only a job that could be waiting on such a check is a candidate: one
    // queued or downloading whose queue has drained, one with parked runs
    // still to release, one with container sets in flight, or one in a phase
    // past the download. A queued or downloading candidate that looks
    // exactly as it did when this last checked it is skipped, and when
    // nothing at all has happened since the last pass the pass is skipped,
    // so an idle pipeline pays nothing here however many jobs it holds.
    pub(crate) async fn reconcile_candidate_jobs(&mut self) {
        if self.reconcile_clean && !self.pipeline_is_live() {
            return;
        }
        self.reconcile_clean = true;
        let unanchored: HashSet<JobId> = self
            .pending_unanchored_release
            .iter()
            .map(|segment_id| segment_id.file_id.job_id)
            .collect();
        let mut due = Vec::new();
        let mut candidates = HashSet::new();
        for (job_id, state) in &self.jobs {
            let fetching = matches!(state.status, JobStatus::Queued | JobStatus::Downloading);
            if !fetching {
                if !matches!(state.status, JobStatus::Paused) && !is_terminal_status(&state.status)
                {
                    due.push(*job_id);
                }
                continue;
            }
            let candidate =
                !state.articles.has_queued_download_work() || unanchored.contains(job_id);
            if !candidate {
                continue;
            }
            candidates.insert(*job_id);
            let signature = (
                state.downloaded_bytes,
                state.downloaded_wire_bytes,
                state.failed_bytes,
                state.articles.queued_download_len(),
                state.articles.queued_recovery_len(),
            );
            if self.reconcile_signatures.get(job_id) != Some(&signature) {
                self.reconcile_signatures.insert(*job_id, signature);
                due.push(*job_id);
            }
        }
        self.reconcile_signatures
            .retain(|job_id, _| candidates.contains(job_id));
        for (index, job_id) in due.into_iter().enumerate() {
            self.reconcile_job_progress(job_id).await;
            if index % 16 == 15 {
                tokio::task::yield_now().await;
            }
        }
    }

    pub(crate) fn auto_pause_stalled_downloads(&mut self) {
        let stalled_jobs: Vec<_> = self
            .jobs
            .iter()
            .filter_map(|(job_id, state)| {
                if !matches!(state.status, JobStatus::Downloading) {
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
                    state.articles.queued_download_len(),
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
                    self.emit_job_event(PipelineEvent::JobPaused { job_id });
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

    // Whether anything is moving: transfers, decodes, writes, phases past
    // the download, checks or container-set work in flight, or rate gauges
    // that have not yet settled to zero. Every term is a length or a flag,
    // so asking costs the same however many jobs and history rows exist.
    pub(crate) fn pipeline_is_live(&self) -> bool {
        self.active_downloads > 0
            || !self.active_download_connections_by_job.is_empty()
            || !self.pending_decode.is_empty()
            || !self.active_decodes_by_job.is_empty()
            || !self.held_download_refills.is_empty()
            || !self.rate_limit_reservations.is_empty()
            || !self.pending_completion_checks.is_empty()
            || !self.pending_released_download_results_by_job.is_empty()
            || !self.pending_unanchored_release.is_empty()
            || self.write_buffered_bytes > 0
            || !self.dirty_server_attribution.is_empty()
            || self.metrics_rates_moving
            || self.post_download_phases > 0
    }

    fn metrics_snapshot_period(live: bool) -> std::time::Duration {
        if live {
            Self::METRICS_SNAPSHOT_INTERVAL
        } else {
            Self::IDLE_SNAPSHOT_INTERVAL
        }
    }

    fn metrics_snapshot_timer(live: bool) -> tokio::time::Interval {
        let period = Self::metrics_snapshot_period(live);
        let mut interval = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        interval
    }

    async fn handle_terminal_post_processing_event(&mut self, event: TerminalPostProcessingEvent) {
        match event {
            TerminalPostProcessingEvent::QueueAdmitted(job_id) => {
                self.handle_queue_scripts_admitted(job_id);
            }
            TerminalPostProcessingEvent::QueueDone(job_id, result) => {
                self.handle_queue_scripts_done(job_id, result).await;
            }
            TerminalPostProcessingEvent::AddedScriptsDone(job_id, result) => {
                self.handle_added_scripts_done(job_id, result);
            }
            TerminalPostProcessingEvent::Started(job_id) => {
                self.handle_terminal_post_processing_started(job_id);
            }
            TerminalPostProcessingEvent::Done(done) => {
                self.handle_terminal_post_processing_done(done);
            }
            TerminalPostProcessingEvent::CancelJobsDone(done) => {
                self.handle_cancel_jobs_done(done);
            }
        }
    }

    pub async fn run(&mut self) {
        let mut metrics_live = true;
        let mut metrics_snapshot_interval = Self::metrics_snapshot_timer(metrics_live);
        // Whether the previous turn did anything that dispatch must follow.
        let mut dispatch_due = true;
        let mut tune_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut stalled_download_interval = tokio::time::interval(STALLED_DOWNLOAD_CHECK_INTERVAL);
        stalled_download_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut state_reconcile_interval = tokio::time::interval(Self::STATE_RECONCILE_INTERVAL);
        state_reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pending_download_results = VecDeque::new();
        let queue = self.shared_state.queue.clone();
        let turn_metrics = self.metrics.clone();

        info!("pipeline started");

        let startup_jobs: Vec<_> = self.jobs.keys().copied().collect();
        for job_id in startup_jobs {
            self.reconcile_job_progress(job_id).await;
        }

        // One watch for the pool's life, so a server's recovery that changes
        // at any point after a dispatch pass, even while the loop is busy with
        // another arm, wakes the next wait.
        let mut server_health = self.nntp.pool().server_health_changes();
        loop {
            let mut turn_timer = crate::operations::instrumentation::TurnTimer::new(
                &turn_metrics.pipeline_histograms.turn_duration,
            );
            // A replaced pool brings its own servers; watch those, and give
            // their waiting work a pass.
            if !server_health.follows(self.nntp.pool()) {
                server_health = self.nntp.pool().server_health_changes();
                self.download_dispatch_wake = true;
            }
            if queue.read(|inner| inner.globals.shutting_down) {
                // Readmission can wait for cancellation cleanup, whose terminal
                // completion event releases the last script cancellation sender.
                // Keep servicing those events while accepted admissions settle.
                loop {
                    tokio::select! {
                        _ = queue.finish_admissions() => break,
                        Some(event) = self.terminal_post_processing_done_rx.recv() => {
                            self.handle_terminal_post_processing_event(event).await;
                        }
                    }
                }
                self.apply_queue_intents().await;
                // A recorded cancel is settled before exit: its archive decides
                // the job, and that answer returns on the done channel.
                while queue.read(|inner| {
                    inner
                        .jobs
                        .iter()
                        .any(|row| row.intents.cancelled.is_some() && row.report.retiring)
                }) {
                    let Some(event) = self.terminal_post_processing_done_rx.recv().await else {
                        break;
                    };
                    self.handle_terminal_post_processing_event(event).await;
                }
                break;
            }
            self.apply_queue_intents().await;
            // Whether this turn was woken by, or found, anything other than a
            // periodic tick. Only such a turn can have changed what the job
            // snapshot shows or what dispatch would hand out.
            let mut turn_active = false;
            self.drain_ready_download_results(&mut pending_download_results);
            self.drain_ready_lane_control_messages();
            turn_active |= self.hear_graph_runs();
            self.pump_decode_queue();

            // An ordinal can be retired from outside the result handlers — a
            // server removed, work no lane may seat — and a part parked behind
            // its damaged bytes must be written before completion is weighed,
            // or the job would be judged with that part neither placed nor
            // given up.
            if !self.pending_unanchored_release.is_empty() {
                turn_active = true;
                self.release_settled_unanchored_runs().await;
                self.pump_decode_queue();
            }

            let pending_completion_checks = self.pending_completion_checks.len();
            turn_active |= pending_completion_checks > 0;
            for _ in 0..pending_completion_checks {
                let Some(job_id) = self.pending_completion_checks.pop_front() else {
                    break;
                };
                self.check_job_completion(job_id).await;
                self.pump_decode_queue();
            }

            self.consume_due_checkpoint_rechecks();
            // A pass over the eligible jobs follows only something that could
            // change its answer: activity, a wake, live transfers, or a gate
            // that lifts with time. An idle turn would find what the last one
            // found.
            if dispatch_due
                || turn_active
                || self.download_dispatch_wake
                || self.download_dispatch_retry
                || self.pipeline_is_live()
            {
                self.dispatch_downloads();
            }

            let rate_delay = self.rate_limiter.time_until_ready();
            let rate_sleep = tokio::time::sleep(rate_delay);
            let durable_lead_retry_delay = self.next_restart_durable_lead_retry_delay();
            let durable_lead_retry_sleep = tokio::time::sleep(
                durable_lead_retry_delay.unwrap_or_else(|| std::time::Duration::from_secs(3600)),
            );
            // A deferred job wakes the loop exactly when its post is old
            // enough, rather than being rediscovered by polling.
            let propagation_delay = self.next_propagation_delay();
            let propagation_sleep = tokio::time::sleep(
                propagation_delay.unwrap_or_else(|| std::time::Duration::from_secs(3600)),
            );
            let infrastructure_retry_deadline = self.infrastructure_retries.next_deadline();
            let infrastructure_retry_sleep =
                tokio::time::sleep_until(infrastructure_retry_deadline.unwrap_or_else(|| {
                    tokio::time::Instant::now() + std::time::Duration::from_secs(3600)
                }));
            // A held server admits work again on its own at this instant.
            let connection_hold_deadline = self.nntp.pool().next_connection_hold_deadline();
            let connection_hold_sleep =
                tokio::time::sleep_until(connection_hold_deadline.map_or_else(
                    || tokio::time::Instant::now() + std::time::Duration::from_secs(3600),
                    tokio::time::Instant::from_std,
                ));

            self.drain_ready_lane_control_messages();
            if self.take_download_dispatch_wake() {
                self.dispatch_downloads();
            }

            let processed_results = self
                .process_download_result_turn(
                    &mut pending_download_results,
                    &mut metrics_snapshot_interval,
                )
                .await;
            turn_active |= processed_results > 0;
            if processed_results == 0 {
                // Set by the periodic arms that found nothing to do; every
                // other arm is an event.
                let mut idle_tick = false;
                turn_timer.pause();
                tokio::select! {
                    _ = queue.intent_changed() => { turn_timer.resume(); }
                    Some(result) = self.download_done_rx.recv() => {
                        turn_timer.resume();
                        if !self.release_download_result(&result) { continue; }
                        self.note_released_download_result_pending(
                            result.job_id,
                            Self::released_download_result_lead_bytes(&result),
                        );
                        pending_download_results.push_back(result);
                    }
                    Some(request) = self.download_refill_rx.recv() => {
                        turn_timer.resume();
                        self.drain_ready_download_results(&mut pending_download_results);
                        self.handle_download_lane_refill_request(request);
                    }
                    Some(parked) = self.download_lane_parked_rx.recv() => {
                        turn_timer.resume();
                        self.handle_download_lane_parked(parked);
                    }
                    Some(event) = self.owned_download_lane_event_rx.recv() => {
                        turn_timer.resume();
                        self.handle_owned_download_lane_event(
                            event,
                            &mut pending_download_results,
                        );
                    }
                    Some(said) = self.graph_runs.recv() => {
                        self.graph_run_said(said);
                    }
                    Some(update) = self.post_probes.recv() => {
                        turn_timer.resume();
                        self.post_probe_heard(update);
                    }
                    Some(result) = self.decode_done_rx.recv() => {
                        turn_timer.resume();
                        crate::runtime::perf_probe::record(
                            "download.decode.done_rx.received",
                            std::time::Duration::from_nanos(1),
                        );
                        self.handle_decode_done(result).await;
                    }
                    Some(generation) = self.nntp_handoff_drained_rx.recv() => {
                        turn_timer.resume();
                        self.handle_nntp_handoff_drained(generation);
                    }
                    Some(done) = self.job_activated_rx.recv() => {
                        turn_timer.resume();
                        self.handle_job_activated(done);
                    }
                    Some(done) = self.move_done_rx.recv() => {
                        turn_timer.resume();
                        let reported_job = done.job_id;
                        self.handle_move_to_complete_done(done).await;
                        self.report_job(reported_job);
                    }
                    Ok(()) = self.script_effects_rx.changed() => {
                        turn_timer.resume();
                        for job_id in self.db.take_script_effects() {
                            self.apply_queue_script_effects(JobId(job_id));
                            self.report_job(JobId(job_id));
                        }
                    }
                    Some(event) = self.terminal_post_processing_done_rx.recv() => {
                        turn_timer.resume();
                        self.handle_terminal_post_processing_event(event).await;
                    }
                    Some(retry) = self.retry_rx.recv() => {
                        turn_timer.resume();
                        let reported_job = retry.work.segment_id.file_id.job_id;
                        self.receive_retry_work(retry);
                        self.report_job(reported_job);
                    }
                    _ = metrics_snapshot_interval.tick() => {
                        turn_timer.resume();
                        idle_tick = true;
                        self.refresh_periodic_snapshot();
                        // Lanes whose refill found nothing wait here for the
                        // next wake; the tick is what ends the wait when no
                        // wake comes.
                        self.service_held_download_refills();
                    }
                    _ = rate_sleep, if !rate_delay.is_zero() => { turn_timer.resume(); }
                    _ = durable_lead_retry_sleep, if durable_lead_retry_delay.is_some() => {
                        turn_timer.resume();
                        self.consume_due_checkpoint_rechecks();
                    }
                    _ = propagation_sleep, if propagation_delay.is_some() => { turn_timer.resume(); }
                    _ = infrastructure_retry_sleep, if infrastructure_retry_deadline.is_some() => {
                        turn_timer.resume();
                        self.requeue_due_infrastructure_retries();
                    }
                    () = server_health.changed() => {
                        turn_timer.resume();
                        self.download_dispatch_wake = true;
                    }
                    _ = connection_hold_sleep, if connection_hold_deadline.is_some() => {
                        turn_timer.resume();
                        self.download_dispatch_wake = true;
                    }
                    _ = tune_interval.tick() => {
                        turn_timer.resume();
                        idle_tick = true;
                        self.refresh_download_pressure();
                        self.publish_download_transport_health();

                        if tracing::enabled!(tracing::Level::DEBUG) {
                        let live = self.pipeline_is_live();
                        let snapshot = self.metrics.raw_snapshot();
                        let not_found = snapshot.articles_not_found;
                        // Folded over every job, so only while something
                        // is moving; an idle pipeline's health is unchanged.
                        let min_health: Option<u32> = live.then(|| self.jobs.values()
                            .filter(|s| !matches!(s.status, JobStatus::Failed { .. } | JobStatus::Complete | JobStatus::Partial))
                            .filter(|s| s.spec.total_bytes > 0)
                            .map(|s| (s.spec.total_bytes.saturating_sub(s.failed_bytes) * 1000 / s.spec.total_bytes) as u32)
                            .min()).flatten();
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
                            not_found,
                            health = min_health.map(|h| format!("{:.1}%", h as f64 / 10.0)).unwrap_or_default(),
                            "pipeline tick"
                        );
                        }
                    }
                    _ = stalled_download_interval.tick() => {
                        turn_timer.resume();
                        // A stall needs a transfer in flight; with none the
                        // walk over every job would find nothing.
                        idle_tick = self.active_downloads_by_job.is_empty();
                        if !idle_tick {
                            self.auto_pause_stalled_downloads();
                        }
                    }
                    _ = state_reconcile_interval.tick() => {
                        turn_timer.resume();
                        idle_tick = true;
                        self.reconcile_candidate_jobs().await;
                    }
                }
                turn_active |= !idle_tick;
            }

            self.pump_decode_queue();
            let live = self.pipeline_is_live();
            if turn_active {
                self.reconcile_clean = false;
            }
            dispatch_due = turn_active;
            if live != metrics_live {
                metrics_live = live;
                metrics_snapshot_interval = Self::metrics_snapshot_timer(live);
            }
        }

        self.drain().await;
        info!("pipeline stopped");
    }

    pub(crate) async fn process_download_result_turn(
        &mut self,
        pending: &mut VecDeque<DownloadResult>,
        metrics_interval: &mut tokio::time::Interval,
    ) -> usize {
        let mut processed = 0;
        // One report per job per turn, however many of its results landed.
        let mut reported = BTreeSet::new();
        while processed < Self::DOWNLOAD_RESULTS_PER_TURN {
            let Some(result) = pending.pop_front() else {
                break;
            };
            reported.insert(result.job_id);
            self.process_released_download_done(result).await;
            processed += 1;
            // Return freed connections during ingestion rather than waiting
            // until the entire result batch has been processed.
            self.drain_ready_lane_control_messages();
            if self.take_download_dispatch_wake() {
                self.dispatch_downloads();
            }
        }
        for job_id in reported {
            self.report_job(job_id);
        }
        if processed != 0 {
            // A backlog can keep every actor operation immediately ready.
            // Let the timer driver and transport tasks run between batches.
            tokio::task::yield_now().await;
        }
        // Busy turns skip the blocking select. Poll the same interval here
        // without waiting so sustained results cannot starve UI refreshes.
        let due = std::future::poll_fn(|cx| {
            std::task::Poll::Ready(metrics_interval.poll_tick(cx).is_ready())
        })
        .await;
        if due {
            self.refresh_periodic_snapshot();
        }
        processed
    }

    pub(in crate::pipeline) fn refresh_periodic_snapshot(&mut self) {
        self.checkpoint_server_attribution_if_due();
        // Queue edits can change eligibility while the data plane is idle.
        // The existing active/idle tick cadence bounds these small walks.
        self.publish_download_footprint_metrics();
        self.metrics_rates_moving = self.shared_state.metrics_snapshot().current_download_speed > 0;
    }

    // Publish the download side's memory footprint.
    //
    // Both readings are walks — the dispatch reservations for the bytes, the
    // job order for the ranking — so they are taken on the snapshot tick and
    // never on an article path. The reservation map is the authoritative
    // record of articles handed to a lane and not yet reconciled, which is
    // exactly the raw bytes the lanes are holding.
    fn publish_download_footprint_metrics(&self) {
        self.metrics.download_lane_inflight_bytes.store(
            self.rate_limit_reservations.values().sum::<u64>(),
            std::sync::atomic::Ordering::Relaxed,
        );
        let eligible = self.download_scheduler_eligible_jobs().len();
        self.metrics
            .download_jobs_eligible
            .store(eligible, std::sync::atomic::Ordering::Relaxed);
        self.metrics.download_jobs_hot.store(
            usize::from(eligible > 0),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    pub(crate) fn consume_due_checkpoint_rechecks(&mut self) {
        let now = Instant::now();
        let due: Vec<JobId> = self
            .download_restart_durable_lead_retry_after
            .iter()
            .filter(|(_, ready_at)| **ready_at <= now)
            .map(|(job_id, _)| *job_id)
            .collect();
        for job_id in due {
            self.advance_durable_download_floor(job_id);
        }
        self.download_restart_durable_lead_retry_after
            .retain(|_, ready_at| *ready_at > now);
    }

    pub(crate) fn next_restart_durable_lead_retry_delay(&self) -> Option<std::time::Duration> {
        let now = Instant::now();
        self.download_restart_durable_lead_retry_after
            .values()
            .copied()
            .min()
            .map(|ready_at| ready_at.saturating_duration_since(now))
    }

    fn update_parked_infrastructure_metric(&self) {
        self.metrics
            .parked_infrastructure_work
            .store(self.infrastructure_retries.len(), Ordering::Relaxed);
    }

    pub(in crate::pipeline) fn schedule_infrastructure_retry(
        &mut self,
        delay: Option<std::time::Duration>,
        retry: RetryWork,
    ) {
        self.infrastructure_retries.schedule(delay, retry);
        self.update_parked_infrastructure_metric();
    }

    pub(in crate::pipeline) fn cancel_infrastructure_retries_for_job(&mut self, job_id: JobId) {
        self.infrastructure_retries
            .cancel_where(|retry| retry.work.segment_id.file_id.job_id == job_id);
        self.update_parked_infrastructure_metric();
    }

    pub(in crate::pipeline) fn wake_all_infrastructure_retries(&mut self) -> usize {
        let ready = self.infrastructure_retries.wake_all();
        self.update_parked_infrastructure_metric();
        self.requeue_infrastructure_retry_batch(ready)
    }

    pub(in crate::pipeline) fn wake_infrastructure_retries_for_jobs(
        &mut self,
        jobs: &std::collections::HashSet<JobId>,
    ) -> usize {
        let ready = self
            .infrastructure_retries
            .wake_where(|retry| jobs.contains(&retry.work.segment_id.file_id.job_id));
        self.update_parked_infrastructure_metric();
        self.requeue_infrastructure_retry_batch(ready)
    }

    fn requeue_due_infrastructure_retries(&mut self) -> usize {
        let ready = self
            .infrastructure_retries
            .take_due(tokio::time::Instant::now());
        self.update_parked_infrastructure_metric();
        self.requeue_infrastructure_retry_batch(ready)
    }

    fn requeue_infrastructure_retry_batch(&mut self, ready: Vec<RetryWork>) -> usize {
        let count = ready.len();
        let mut jobs = HashSet::new();
        for retry in ready {
            jobs.insert(retry.work.segment_id.file_id.job_id);
            self.receive_retry_work(retry);
        }
        for job_id in jobs {
            self.report_job(job_id);
        }
        count
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
            if !self.release_download_result(&result) {
                continue;
            }
            self.note_released_download_result_pending(
                result.job_id,
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
            match self.download_refill_rx.try_recv() {
                Ok(request) => self.handle_download_lane_refill_request(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
    }

    // Re-enter a delayed retry into the download queue, dropping stale server
    // exclusions if the NNTP pool was rebuilt between scheduling and re-entry.
    //
    // `exclude_servers` holds pool indices computed under the generation the
    // retry was scheduled in. A `RebuildNntp` bumps `pool_generation` and can
    // reshape the pool (shrink/grow/reorder), so those indices may now point at
    // a different server — excluding the wrong one, or even falsely satisfying
    // the fill-servers-exhausted gate and spilling onto backfill. When the
    // generation no longer matches we discard the exclusions and let the retry
    // fall back to a clean attempt; retention excludes are recomputed fresh
    // from the new pool at order time, so nothing durable is lost.
    pub(in crate::pipeline) fn receive_retry_work(&mut self, retry: crate::pipeline::RetryWork) {
        let crate::pipeline::RetryWork {
            scheduled_pool_generation,
            infrastructure_retry,
            mut work,
        } = retry;
        let egress_parked = self.egress_quota_parked.remove(&work.segment_id).is_some();
        if self.server_quota_parked.remove(&work.segment_id) || egress_parked {
            self.refresh_server_quota_block_presentation();
        }
        if scheduled_pool_generation != self.pool_generation
            && (!work.exclude_servers.is_empty() || work.avoid_server.is_some())
        {
            debug!(
                segment = %work.segment_id,
                scheduled_generation = scheduled_pool_generation,
                current_generation = self.pool_generation,
                "dropping stale server exclusions on retry after NNTP pool rebuild"
            );
            work.exclude_servers.clear();
            work.avoid_server = None;
        }
        if infrastructure_retry {
            self.note_infrastructure_retry_requeued(work.segment_id.file_id.job_id);
        }
        self.requeue_retry_work(work);
    }

    // Re-enters a retry that `note_retry_scheduled` booked, draining its
    // pending-retry counters before the work goes back on the queue.
    pub(crate) fn requeue_retry_work(&mut self, work: DownloadWork) {
        self.note_retry_requeued(work.segment_id);
        self.enqueue_download_work(work);
    }

    // Queues download work that no scheduled retry stands behind.
    //
    // Leaves the pending-retry counters alone: those count retries still in
    // flight, and the pass-end gates read the job-wide one. Draining it here
    // for fresh work would cancel some other segment's real pending retry and
    // let the download pass end before that retry fires.
    pub(crate) fn enqueue_download_work(&mut self, work: DownloadWork) {
        let job_id = work.segment_id.file_id.job_id;
        let segment_id = work.segment_id;
        if self
            .jobs
            .get(&job_id)
            .is_none_or(|state| is_terminal_status(&state.status))
        {
            debug!(
                job_id = job_id.0,
                segment = %segment_id,
                "dropping download work for a job that is gone or terminal"
            );
            return;
        }
        // Work every server is excluded from can never be seated by a lane:
        // the queue would hold it, and the job with it, indefinitely. Every
        // producer of a re-queue passes through here, so this is where the
        // article is retired instead.
        if self.no_server_can_serve(job_id, &work.exclude_servers) {
            self.book_unservable_work(&work);
            self.update_queue_metrics();
            self.maybe_finish_download_pass(job_id);
            return;
        }
        // The job's root classes the work.
        let mut work = work;
        self.graph_requeued(&mut work);
        let parks = self.requeue_parks(&work);
        if let Some(active) = self
            .jobs
            .get_mut(&job_id)
            .and_then(|state| state.articles.active_mut())
        {
            if parks {
                active.recovery_queue.push(work);
            } else {
                active.download_queue.push(work);
            }
        }
    }

    pub(crate) async fn drain(&mut self) {
        // Unblock lanes waiting on deferred refills so they can finish their
        // batches and exit; dropping the senders answers them with an error.
        self.drain_inflight_download_and_decode_work().await;
        // A graph job's run or repair still out on a thread speaks through
        // the run mail; heard here, so nothing of a job is out when the
        // pipeline is down and the checkpoint holds what it came to.
        self.drain_graph_runs().await;
        self.checkpoint_graph_holds();

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
        self.flush_server_attribution();
        self.flush_support_facts();
        if let Err(error) = self.shared_state.queue.sample_bandwidth(true) {
            warn!(error = %error, "failed to flush pending bandwidth usage during drain");
        }

        // Join any detached fire-and-forget writes still in flight (runtime
        // writes such as a proven pipelining depth) so shutdown does not drop
        // them. Bounded so a stuck write cannot hang shutdown forever.
        self.join_fire_and_forget_tasks().await;
        match tokio::time::timeout(
            Self::FIRE_AND_FORGET_DRAIN_TIMEOUT,
            self.db.flush_write_queue(),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!(%error, "failed to flush ordered database writes during drain"),
            Err(_) => warn!("timed out flushing ordered database writes during drain"),
        }
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
                        let reported_job = result.job_id;
                        self.process_released_download_done(result).await;
                        self.report_job(reported_job);
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

    // Every graph job pages what it holds in RAM to scratch and is
    // checkpointed, before the database write queue is flushed: a restart
    // then reads those holds back instead of fetching them again. A job whose
    // page-out failed keeps the checkpoint it last wrote, which describes
    // only bytes that were written.
    fn checkpoint_graph_holds(&mut self) {
        let jobs: Vec<JobId> = self.graph_jobs.keys().copied().collect();
        for job_id in jobs {
            let Some(graph) = self.graph_jobs.get_mut(&job_id) else {
                continue;
            };
            if let Err(error) = graph.page_out_holds() {
                warn!(job_id = job_id.0, %error, "could not page graph holds out at shutdown");
                continue;
            }
            self.checkpoint_graph_job(job_id);
        }
    }

    // Every run is heard out and stopped where it stands: a run waits for
    // bytes, or for a member's owner, that nothing will bring once the
    // pipeline is down, and what it read is on its shelf for the restored
    // job. A repair out on its thread is heard to its end, and whatever its
    // ruling starts is stopped in turn.
    async fn drain_graph_runs(&mut self) {
        loop {
            self.hear_graph_runs();
            let limit = self.graph_slot_limit();
            for job in self.graph_jobs.values_mut() {
                job.stop_runs(limit);
            }
            if !self.graph_jobs.values().any(|job| job.awaits_mail()) {
                return;
            }
            match self.graph_runs.recv().await {
                Some(said) => self.graph_run_said(said),
                None => {
                    warn!("shutdown drain channel closed before in-flight work completed");
                    return;
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
    let available_bytes = backlog_memory_ceiling_bytes(profile);
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
    let available_bytes = backlog_memory_ceiling_bytes(profile);
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

// The memory both backlog budgets are carved from: available memory with a
// 256 MiB floor, capped by the container's limit when there is one.
fn backlog_memory_ceiling_bytes(profile: &SystemProfile) -> usize {
    let available_bytes = profile.memory.available_bytes.max(256 * 1024 * 1024);
    match profile.memory.cgroup_limit {
        Some(cgroup_limit) => available_bytes.min(cgroup_limit) as usize,
        None => available_bytes as usize,
    }
}

fn buffer_pool_total_bytes(buffers: &Arc<BufferPool>) -> usize {
    use crate::runtime::buffers::BufferTier;

    let metrics = buffers.metrics();
    metrics.small_total * BufferTier::Small.size_bytes()
        + metrics.medium_total * BufferTier::Medium.size_bytes()
        + metrics.large_total * BufferTier::Large.size_bytes()
}

pub(crate) fn timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(crate) fn is_terminal_status(status: &JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Complete | JobStatus::Partial | JobStatus::Failed { .. }
    )
}
