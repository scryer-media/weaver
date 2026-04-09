use super::*;

impl Pipeline {
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
        initial_history: Vec<JobInfo>,
        initial_global_paused: bool,
        shared_state: SharedPipelineState,
        db: crate::Database,
        config: crate::settings::SharedConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = Arc::clone(shared_state.metrics());
        let write_backlog_budget_bytes = compute_write_backlog_budget_bytes(&profile, &buffers);
        let tuner = RuntimeTuner::with_connection_limit(profile, total_connections);
        let initial_bandwidth_policy = config.read().await.isp_bandwidth_cap.clone();
        info!(
            max_downloads = tuner.params().max_concurrent_downloads,
            write_backlog_budget_mb = write_backlog_budget_bytes / (1024 * 1024),
            total_connections,
            "pipeline tuner initialized"
        );

        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::create_dir_all(&intermediate_dir).await?;
        tokio::fs::create_dir_all(&complete_dir).await?;

        let nzb_dir = data_dir.join(".weaver-nzbs");
        let (download_done_tx, download_done_rx) = mpsc::channel(256);
        let (decode_done_tx, decode_done_rx) = mpsc::channel(256);
        let (retry_tx, retry_rx) = mpsc::channel(256);
        let (probe_result_tx, probe_result_rx) = mpsc::channel(16);
        let (extract_done_tx, extract_done_rx) = mpsc::channel(32);
        shared_state.set_paused(initial_global_paused);
        let pp_pool = crate::runtime::postprocess_pool::build_postprocess_pool(
            tuner.params().extract_thread_count,
        );
        let mut bandwidth_cap = BandwidthCapRuntime::default();
        bandwidth_cap.set_policy(initial_bandwidth_policy);

        let mut pipeline = Self {
            cmd_rx,
            event_tx,
            nntp: Arc::new(nntp),
            buffers,
            tuner,
            metrics,
            jobs: HashMap::new(),
            job_order: Vec::new(),
            active_downloads: 0,
            active_recovery: 0,
            active_download_passes: HashSet::new(),
            active_downloads_by_job: HashMap::new(),
            active_decodes_by_job: HashMap::new(),
            pending_retries_by_job: HashMap::new(),
            intermediate_dir,
            complete_dir,
            nzb_dir,
            segment_batch: Vec::new(),
            pending_file_progress: HashMap::new(),
            persisted_file_progress: HashMap::new(),
            pending_decode: VecDeque::new(),
            pending_completion_checks: VecDeque::new(),
            download_done_tx,
            download_done_rx,
            decode_done_tx,
            decode_done_rx,
            retry_tx,
            retry_rx,
            probe_result_tx,
            probe_result_rx,
            extract_done_tx,
            extract_done_rx,
            global_paused: initial_global_paused,
            bandwidth_cap,
            bandwidth_reservations: HashMap::new(),
            connection_ramp: total_connections.min(5),
            rate_limiter: TokenBucket::new(0),
            write_buf_max_pending,
            write_backlog_budget_bytes,
            write_buffered_bytes: 0,
            write_buffered_segments: 0,
            write_buffers: HashMap::new(),
            par2_runtime: HashMap::new(),
            extracted_members: HashMap::new(),
            extracted_archives: HashMap::new(),
            decode_retries: HashMap::new(),
            inflight_extractions: HashMap::new(),
            failed_extractions: HashMap::new(),
            eagerly_deleted: HashMap::new(),
            rar_sets: HashMap::new(),
            normalization_retried: HashSet::new(),
            pending_concat: HashMap::new(),
            par2_bypassed: HashSet::new(),
            finished_jobs: initial_history,
            shared_state,
            db,
            config,
            pp_pool,
        };
        let _ = pipeline.refresh_bandwidth_cap_window();
        Ok(pipeline)
    }

    pub(crate) async fn db_blocking<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&crate::Database) -> R + Send + 'static,
        R: Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .expect("db task panicked")
    }

    pub(crate) fn db_fire_and_forget<F>(&self, f: F)
    where
        F: FnOnce(&crate::Database) + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db));
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
        if force_flush
            || contiguous_bytes_written.saturating_sub(current) >= FILE_PROGRESS_FLUSH_DELTA_BYTES
        {
            self.flush_file_progress_batch();
        }
    }

    pub(crate) fn flush_file_progress_batch(&mut self) {
        if self.pending_file_progress.is_empty() {
            return;
        }
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

    pub(crate) fn clear_job_progress_floor_runtime(&mut self, job_id: JobId) {
        self.pending_file_progress
            .retain(|file_id, _| file_id.job_id != job_id);
        self.persisted_file_progress
            .retain(|file_id, _| file_id.job_id != job_id);
    }

    pub async fn run(&mut self) {
        let mut tune_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("pipeline started");

        loop {
            self.pump_decode_queue();

            while let Some(job_id) = self.pending_completion_checks.pop_front() {
                self.check_job_completion(job_id).await;
                self.pump_decode_queue();
            }

            self.dispatch_downloads();

            let rate_delay = self.rate_limiter.time_until_ready();
            let rate_sleep = tokio::time::sleep(rate_delay);

            loop {
                match self.cmd_rx.try_recv() {
                    Ok(SchedulerCommand::Shutdown) => {
                        info!("pipeline shutting down");
                        return;
                    }
                    Ok(cmd) => self.handle_command(cmd).await,
                    Err(_) => break,
                }
            }

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
                    self.handle_download_done(result).await;
                }
                Some(result) = self.decode_done_rx.recv() => {
                    self.handle_decode_done(result).await;
                }
                Some(update) = self.probe_result_rx.recv() => {
                    self.handle_probe_update(update);
                }
                Some(done) = self.extract_done_rx.recv() => {
                    self.handle_extraction_done(done).await;
                }
                Some(work) = self.retry_rx.recv() => {
                    let job_id = work.segment_id.file_id.job_id;
                    self.note_retry_requeued(job_id);
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        if work.is_recovery {
                            state.recovery_queue.push(work);
                        } else {
                            state.download_queue.push(work);
                        }
                    }
                }
                _ = rate_sleep, if !rate_delay.is_zero() => {}
                _ = tune_interval.tick() => {
                    self.flush_quiescent_write_backlog().await;

                    let snapshot = self.metrics.snapshot();
                    let not_found = snapshot.articles_not_found;
                    let min_health: Option<u32> = self.jobs.values()
                        .filter(|s| !matches!(s.status, JobStatus::Failed { .. } | JobStatus::Complete))
                        .filter(|s| s.spec.total_bytes > 0)
                        .map(|s| (s.spec.total_bytes.saturating_sub(s.failed_bytes) * 1000 / s.spec.total_bytes) as u32)
                        .min();
                    info!(
                        active = self.active_downloads,
                        queue = snapshot.download_queue_depth,
                        speed_mbps = format!("{:.1}", snapshot.current_download_speed as f64 / (1024.0 * 1024.0)),
                        downloaded_mb = snapshot.bytes_downloaded / (1024 * 1024),
                        segments = snapshot.segments_downloaded,
                        decode_pending = snapshot.decode_pending,
                        write_backlog_mb = snapshot.write_buffered_bytes / (1024 * 1024),
                        write_backlog_segments = snapshot.write_buffered_segments,
                        direct_write_evictions = snapshot.direct_write_evictions,
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
            }

            self.pump_decode_queue();
            self.publish_snapshot();
        }

        self.drain().await;
        info!("pipeline stopped");
    }

    pub(crate) fn publish_snapshot(&mut self) {
        let _ = self.refresh_bandwidth_cap_window();
        self.shared_state.publish_jobs(self.list_jobs());
    }

    async fn drain(&mut self) {
        if self.active_downloads > 0 {
            info!(
                active = self.active_downloads,
                "draining in-flight downloads"
            );
        }
        self.flush_segment_batch();
        self.flush_file_progress_batch();
    }

    pub(crate) fn flush_segment_batch(&mut self) {
        if self.segment_batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.segment_batch);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = db.commit_segments(&batch) {
                tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
            } else {
                crate::e2e_failpoint::maybe_trip("download.after_batch_commit_before_ack");
            }
        });
    }
}

pub(crate) async fn write_segment_to_disk(
    path: &std::path::Path,
    offset: u64,
    data: &[u8],
) -> Result<(), std::io::Error> {
    let path = path.to_owned();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || {
        crate::runtime::affinity::pin_current_thread_for_hot_download_path();

        use std::io::{Seek, Write};
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)?;
        file.seek(std::io::SeekFrom::Start(offset))?;
        file.write_all(&data)?;
        Ok(())
    })
    .await
    .unwrap_or_else(|e| Err(std::io::Error::other(e)))
}

pub(crate) fn compute_write_backlog_budget_bytes(
    profile: &SystemProfile,
    buffers: &Arc<BufferPool>,
) -> usize {
    use crate::runtime::buffers::BufferTier;
    use crate::runtime::system_profile::StorageClass;

    let metrics = buffers.metrics();
    let scratch_bytes = metrics.small_total * BufferTier::Small.size_bytes()
        + metrics.medium_total * BufferTier::Medium.size_bytes()
        + metrics.large_total * BufferTier::Large.size_bytes();
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

pub(crate) fn check_disk_space(output_dir: &std::path::Path, needed_bytes: u64) {
    #[cfg(unix)]
    {
        let path_cstr = match std::ffi::CString::new(output_dir.to_str().unwrap_or(".").as_bytes())
        {
            Ok(c) => c,
            Err(_) => return,
        };

        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(path_cstr.as_ptr(), &mut stat) == 0 {
                #[allow(clippy::unnecessary_cast)]
                let available = stat.f_bavail as u64 * stat.f_frsize as u64;
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
            } else {
                debug!("could not check free disk space (statvfs failed)");
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (output_dir, needed_bytes);
        debug!("disk space check not available on this platform");
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
