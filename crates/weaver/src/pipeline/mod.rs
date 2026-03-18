mod bandwidth_cap;
mod completion;
mod decode;
mod download;
mod extraction;
mod health;
mod job;
mod metadata;
mod query;
mod rar_state;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use std::collections::HashSet;

use weaver_assembly::write_buffer::{BufferedChunk, WriteReorderBuffer};
use weaver_assembly::{ExtractionReadiness, JobAssembly};
use weaver_core::buffer::{BufferHandle, BufferPool};
#[cfg(test)]
use weaver_core::checksum;
use weaver_core::event::PipelineEvent;
use weaver_core::id::{JobId, NzbFileId, SegmentId};
use weaver_core::system::SystemProfile;
use weaver_nntp::NntpClient;
use weaver_par2::par2_set::Par2FileSet;
use weaver_scheduler::{
    DownloadQueue, DownloadWork, JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics,
    RuntimeTuner, SchedulerCommand, SchedulerError, SharedPipelineState, TokenBucket,
};
use weaver_state::CommittedSegment;

use self::bandwidth_cap::BandwidthCapRuntime;
use self::rar_state::RarSetState;

/// Maximum number of retries for a single segment before giving up.
const MAX_SEGMENT_RETRIES: u32 = 3;

/// Result of a download task.
pub(super) struct DownloadResult {
    pub(super) segment_id: SegmentId,
    pub(super) data: Result<Bytes, String>,
    /// Whether this was a speculative recovery download.
    pub(super) is_recovery: bool,
    /// How many times this segment has been retried so far.
    pub(super) retry_count: u32,
}

/// Successful download payload waiting for decode scheduling.
pub(super) struct PendingDecodeWork {
    pub(super) segment_id: SegmentId,
    pub(super) raw: Bytes,
}

/// Progress update from a health probe task.
pub(super) struct ProbeUpdate {
    pub(super) job_id: JobId,
    /// Total probes attempted so far.
    pub(super) total: usize,
    /// Number of missing articles found so far.
    pub(super) missed: usize,
    /// True when the probe is complete (final update).
    pub(super) done: bool,
}

/// Result of a background extraction task.
pub(super) struct BatchExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<(String, String)>,
}

pub(super) struct FullSetExtractionOutcome {
    pub(super) extracted: Vec<String>,
    pub(super) failed: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct Par2FileRuntime {
    pub(super) filename: String,
    pub(super) recovery_blocks: u32,
    pub(super) promoted: bool,
}

#[derive(Debug, Clone, Default)]
pub(super) struct Par2RuntimeState {
    pub(super) set: Option<Arc<Par2FileSet>>,
    pub(super) files: HashMap<u32, Par2FileRuntime>,
}

pub(super) enum ExtractionDone {
    /// Batch extraction of specific members completed.
    Batch {
        job_id: JobId,
        set_name: String,
        attempted: Vec<String>,
        result: Result<BatchExtractionOutcome, String>,
    },
    /// Full set extraction completed (all volumes present).
    FullSet {
        job_id: JobId,
        set_name: String,
        result: Result<FullSetExtractionOutcome, String>,
    },
}

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) crc32: u32,
    pub(super) data: DecodedChunk,
    /// Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
}

/// Completion of a decode task, including explicit failures so backlog
/// accounting is always drained.
pub(super) enum DecodeDone {
    Success(DecodeResult),
    Failed {
        segment_id: SegmentId,
        error: String,
    },
}

pub(super) struct DecodedChunk(Box<[u8]>);

impl DecodedChunk {
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(super) fn len_bytes(&self) -> usize {
        self.0.len()
    }
}

impl From<Vec<u8>> for DecodedChunk {
    fn from(value: Vec<u8>) -> Self {
        Self(value.into_boxed_slice())
    }
}

pub(super) struct BufferedDecodedSegment {
    pub(super) segment_id: SegmentId,
    pub(super) decoded_size: u32,
    pub(super) crc32: u32,
    pub(super) data: DecodedChunk,
    pub(super) yenc_name: String,
}

impl BufferedChunk for BufferedDecodedSegment {
    fn len_bytes(&self) -> usize {
        self.data.len_bytes()
    }
}

/// The pipeline engine. Owns the scheduler loop and drives work through
/// download → decode → commit → verify → repair → extract stages.
pub struct Pipeline {
    /// Receives commands from SchedulerHandle.
    pub(super) cmd_rx: mpsc::Receiver<SchedulerCommand>,
    /// Broadcasts pipeline events to subscribers (API, journal, etc).
    pub(super) event_tx: broadcast::Sender<PipelineEvent>,
    /// NNTP client for fetching articles.
    pub(super) nntp: Arc<NntpClient>,
    /// Buffer pool used as decode scratch space only.
    pub(super) buffers: Arc<BufferPool>,
    /// Runtime tuner for adaptive concurrency.
    pub(super) tuner: RuntimeTuner,
    /// Shared atomic metrics.
    pub(super) metrics: Arc<PipelineMetrics>,
    /// Per-job state.
    pub(super) jobs: HashMap<JobId, JobState>,
    /// Job dispatch order (FIFO by submission). First Downloading job is active.
    pub(super) job_order: Vec<JobId>,
    /// Number of in-flight downloads (primary + recovery).
    pub(super) active_downloads: usize,
    /// Number of in-flight recovery downloads (subset of active_downloads).
    pub(super) active_recovery: usize,
    /// Jobs currently inside an active article download pass.
    pub(super) active_download_passes: HashSet<JobId>,
    /// In-flight article download count per job.
    pub(super) active_downloads_by_job: HashMap<JobId, usize>,
    /// Directory for active downloads (per-job subdirectories).
    pub(super) intermediate_dir: PathBuf,
    /// Directory for completed downloads (category subdirectories).
    pub(super) complete_dir: PathBuf,
    /// Directory where persisted NZB files live (for reprocessing after restart).
    pub(super) nzb_dir: PathBuf,
    /// Pending segment commits (flushed to SQLite in batches).
    pub(super) segment_batch: Vec<CommittedSegment>,
    /// Downloaded article bodies waiting for decode scheduling.
    pub(super) pending_decode: VecDeque<PendingDecodeWork>,
    /// Channels for pipeline stage results.
    pub(super) download_done_tx: mpsc::Sender<DownloadResult>,
    pub(super) download_done_rx: mpsc::Receiver<DownloadResult>,
    pub(super) decode_done_tx: mpsc::Sender<DecodeDone>,
    pub(super) decode_done_rx: mpsc::Receiver<DecodeDone>,
    /// Channel for delayed retries — segments sleep then come back here.
    pub(super) retry_tx: mpsc::Sender<DownloadWork>,
    pub(super) retry_rx: mpsc::Receiver<DownloadWork>,
    /// Channel for health probe results: (job_id, total_probes, missed_count).
    pub(super) probe_result_tx: mpsc::Sender<ProbeUpdate>,
    pub(super) probe_result_rx: mpsc::Receiver<ProbeUpdate>,
    /// Channel for background extraction results.
    pub(super) extract_done_tx: mpsc::Sender<ExtractionDone>,
    pub(super) extract_done_rx: mpsc::Receiver<ExtractionDone>,
    /// Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    /// ISP bandwidth cap runtime state.
    bandwidth_cap: BandwidthCapRuntime,
    /// Conservative byte reservations for in-flight downloads used to enforce the
    /// ISP bandwidth cap before actual payload bytes are known.
    bandwidth_reservations: HashMap<SegmentId, u64>,
    /// Bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    /// Gradual connection ramp-up limit (increases each tick).
    pub(super) connection_ramp: usize,
    /// Max pending segments per write reorder buffer (memory-adaptive).
    pub(super) write_buf_max_pending: usize,
    /// Max in-memory decoded backlog before degrading to direct offset writes.
    pub(super) write_backlog_budget_bytes: usize,
    /// Current in-memory decoded backlog retained for sequential write ordering.
    pub(super) write_buffered_bytes: usize,
    /// Current in-memory decoded segment count retained for sequential write ordering.
    pub(super) write_buffered_segments: usize,
    /// Per-file write reorder buffers for decoded segments waiting on write order.
    pub(super) write_buffers: HashMap<NzbFileId, WriteReorderBuffer<BufferedDecodedSegment>>,
    /// Authoritative PAR2 runtime state per job.
    pub(super) par2_runtime: HashMap<JobId, Par2RuntimeState>,
    /// Members already extracted per job (for partial extraction).
    pub(super) extracted_members: HashMap<JobId, HashSet<String>>,
    /// Archive sets already extracted per job (for multi-set 7z).
    pub(super) extracted_sets: HashMap<JobId, HashSet<String>>,
    /// Members whose incremental extraction failed (corrupt volume, CRC error, etc).
    /// Prevents immediate retry during download; cleared after PAR2 repair so
    /// the post-repair extraction path can re-extract them.
    pub(super) failed_extractions: HashMap<JobId, HashSet<String>>,
    /// Filenames eagerly deleted per job after CRC-verified extraction.
    /// Used to distinguish truly-missing files from intentionally-deleted ones
    /// during PAR2 verification.
    pub(super) eagerly_deleted: HashMap<JobId, HashSet<String>>,
    /// Pipeline-owned RAR scheduling state derived from immutable completed-volume facts.
    rar_sets: HashMap<(JobId, String), RarSetState>,
    /// Jobs that have already attempted normalization retry (one-shot guard).
    pub(super) normalization_retried: HashSet<JobId>,
    /// Members where extraction CRC passed and chunks are in the DB, but the
    /// output file isn't fully concatenated yet.  Separate from `extracted_members`
    /// to prevent `try_delete_volumes` from treating them as fully extracted.
    pub(super) pending_concat: HashMap<JobId, HashSet<String>>,
    /// Jobs where all archive members extracted with CRC pass — PAR2
    /// verification/repair is unnecessary.
    pub(super) par2_bypassed: HashSet<JobId>,
    /// Finished jobs (Complete/Failed) from recovery — surfaced in list/get queries.
    pub(super) finished_jobs: Vec<JobInfo>,
    /// Shared state for control plane reads (API handlers read without channel round-trip).
    pub(super) shared_state: SharedPipelineState,
    /// SQLite database for durable history.
    pub(super) db: weaver_state::Database,
    /// Shared config for runtime category lookups (dest_dir overrides).
    pub(super) config: weaver_core::config::SharedConfig,
    /// Dedicated low-priority rayon thread pool for post-processing
    /// (extraction, PAR2 verify/repair). Niced on Unix so the OS scheduler
    /// prefers download/decode threads when CPU is contended.
    pub(super) pp_pool: Arc<rayon::ThreadPool>,
}

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
        db: weaver_state::Database,
        config: weaver_core::config::SharedConfig,
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

        // Ensure directories exist.
        tokio::fs::create_dir_all(&data_dir).await?;
        tokio::fs::create_dir_all(&intermediate_dir).await?;
        tokio::fs::create_dir_all(&complete_dir).await?;

        let nzb_dir = data_dir.join(".weaver-nzbs");

        // Internal channels for pipeline stage results.
        let (download_done_tx, download_done_rx) = mpsc::channel(256);
        let (decode_done_tx, decode_done_rx) = mpsc::channel(256);
        let (retry_tx, retry_rx) = mpsc::channel(256);
        let (probe_result_tx, probe_result_rx) = mpsc::channel(16);
        let (extract_done_tx, extract_done_rx) = mpsc::channel(32);
        shared_state.set_paused(initial_global_paused);
        let pp_pool =
            weaver_core::threadpool::build_postprocess_pool(tuner.params().extract_thread_count);
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
            intermediate_dir,
            complete_dir,
            nzb_dir,
            segment_batch: Vec::new(),
            pending_decode: VecDeque::new(),
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
            extracted_sets: HashMap::new(),
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

    /// Run a DB operation on the blocking pool and await its result.
    /// Prevents SQLite Mutex contention from blocking the tokio async thread.
    async fn db_blocking<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&weaver_state::Database) -> R + Send + 'static,
        R: Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db))
            .await
            .expect("db task panicked")
    }

    /// Fire-and-forget DB write on the blocking pool.
    /// Used for non-critical writes (caches, UI metadata).
    fn db_fire_and_forget<F>(&self, f: F)
    where
        F: FnOnce(&weaver_state::Database) + Send + 'static,
    {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || f(&db));
    }

    /// Run the pipeline main loop until shutdown.
    pub async fn run(&mut self) {
        let mut tune_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("pipeline started");

        loop {
            // Drain pending decode backlog first so buffer pressure relieves
            // before dispatching more downloads.
            self.pump_decode_queue();

            // Dispatch pending downloads up to concurrency limit.
            self.dispatch_downloads();

            let rate_delay = self.rate_limiter.time_until_ready();
            let rate_sleep = tokio::time::sleep(rate_delay);

            // Drain all pending commands first to prevent starvation from
            // high-throughput download/decode events.
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
                // Commands from the SchedulerHandle.
                cmd = self.cmd_rx.recv() => {
                    match cmd {
                        Some(SchedulerCommand::Shutdown) | None => {
                            info!("pipeline shutting down");
                            break;
                        }
                        Some(cmd) => self.handle_command(cmd).await,
                    }
                }

                // Download stage completed.
                Some(result) = self.download_done_rx.recv() => {
                    self.handle_download_done(result).await;
                }

                // Decode stage completed.
                Some(result) = self.decode_done_rx.recv() => {
                    self.handle_decode_done(result).await;
                }

                // Health probe completed — check if job should be failed.
                Some(update) = self.probe_result_rx.recv() => {
                    self.handle_probe_update(update);
                }

                // Background extraction completed.
                Some(done) = self.extract_done_rx.recv() => {
                    self.handle_extraction_done(done).await;
                }

                // Delayed retries arriving after backoff sleep.
                Some(work) = self.retry_rx.recv() => {
                    let job_id = work.segment_id.file_id.job_id;
                    if let Some(state) = self.jobs.get_mut(&job_id) {
                        if work.is_recovery {
                            state.recovery_queue.push(work);
                        } else {
                            state.download_queue.push(work);
                        }
                    }
                }

                // Rate limiter tokens refilled — dispatch will run at top of loop.
                _ = rate_sleep, if !rate_delay.is_zero() => {}

                // Periodic tuning.
                _ = tune_interval.tick() => {
                    self.flush_quiescent_write_backlog().await;

                    let snapshot = self.metrics.snapshot();
                    let not_found = snapshot.articles_not_found;
                    // Compute min health across active jobs for the tick log.
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

                    // Ramp up connection limit gradually.
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

            // Publish updated job snapshot to shared state so the control plane
            // (API handlers, subscriptions) always has fresh data without
            // going through the command channel.
            self.publish_snapshot();
        }

        // Graceful shutdown: wait for in-flight work to drain.
        self.drain().await;
        info!("pipeline stopped");
    }

    /// Publish current job list to shared state for lock-free control plane reads.
    fn publish_snapshot(&mut self) {
        let _ = self.refresh_bandwidth_cap_window();
        self.shared_state.publish_jobs(self.list_jobs());
    }

    /// Process a scheduler command.
    async fn handle_command(&mut self, cmd: SchedulerCommand) {
        match cmd {
            SchedulerCommand::AddJob {
                job_id,
                spec,
                nzb_path,
                reply,
            } => {
                let result = self.add_job(job_id, spec, nzb_path).await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::RestoreJob {
                job_id,
                spec,
                committed_segments,
                extracted_members,
                status,
                working_dir,
                reply,
            } => {
                let result = self
                    .restore_job(
                        job_id,
                        spec,
                        committed_segments,
                        extracted_members,
                        status,
                        working_dir,
                    )
                    .await;
                if result.is_ok() {
                    self.publish_snapshot();
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Paused);
                if result.is_ok() {
                    if self.active_download_passes.remove(&job_id) {
                        let _ = self
                            .event_tx
                            .send(PipelineEvent::DownloadFinished { job_id });
                    }
                    let _ = self.event_tx.send(PipelineEvent::JobPaused { job_id });
                    self.db_fire_and_forget(move |db| {
                        if let Err(e) = db.set_active_job_status(job_id, "paused", None) {
                            error!(error = %e, "db write failed for PauseJob");
                        }
                    });
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::ResumeJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Downloading);
                if result.is_ok() {
                    let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });
                    self.db_fire_and_forget(move |db| {
                        if let Err(e) = db.set_active_job_status(job_id, "downloading", None) {
                            error!(error = %e, "db write failed for ResumeJob");
                        }
                    });
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::CancelJob { job_id, reply } => {
                let result = if let Some(state) = self.jobs.remove(&job_id) {
                    // Per-job queues are dropped with the JobState.
                    self.job_order.retain(|id| *id != job_id);
                    self.update_queue_metrics();

                    // Archive cancelled job: move to history + delete active state.
                    let now = timestamp_secs() as i64;
                    let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
                    let created_at = now - elapsed_secs;
                    let total = state.spec.total_bytes;
                    let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
                        state.assembly.optional_recovery_bytes();
                    let health = if total == 0 {
                        1000
                    } else {
                        ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
                    };
                    let row = weaver_state::JobHistoryRow {
                        job_id: job_id.0,
                        name: state.spec.name.clone(),
                        status: "cancelled".to_string(),
                        error_message: None,
                        total_bytes: total,
                        downloaded_bytes: state.downloaded_bytes,
                        optional_recovery_bytes,
                        optional_recovery_downloaded_bytes,
                        failed_bytes: state.failed_bytes,
                        health,
                        category: state.spec.category.clone(),
                        output_dir: Some(state.working_dir.display().to_string()),
                        nzb_path: None,
                        created_at,
                        completed_at: now,
                        metadata: if state.spec.metadata.is_empty() {
                            None
                        } else {
                            serde_json::to_string(&state.spec.metadata).ok()
                        },
                    };
                    let db = self.db.clone();
                    let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.archive_job(job_id, &row) {
                            tracing::error!(job_id = job_id.0, error = %e, "failed to archive cancelled job");
                        }
                        if let Err(e) = std::fs::remove_file(&nzb_path)
                            && e.kind() != std::io::ErrorKind::NotFound
                        {
                            tracing::warn!(path = %nzb_path.display(), error = %e, "failed to remove NZB file");
                        }
                    });

                    // Clean up per-job caches.
                    self.clear_par2_runtime_state(job_id);
                    self.clear_job_extraction_runtime(job_id);
                    self.active_download_passes.remove(&job_id);
                    self.active_downloads_by_job.remove(&job_id);
                    self.clear_job_rar_runtime(job_id);
                    self.clear_job_write_backlog(job_id);

                    // Delete per-job working and staging directories.
                    let working_dir = state.working_dir.clone();
                    let staging_dir = state.staging_dir.clone();
                    tokio::spawn(async move {
                        if let Err(e) = tokio::fs::remove_dir_all(&working_dir).await
                            && e.kind() != std::io::ErrorKind::NotFound
                        {
                            tracing::warn!(
                                dir = %working_dir.display(),
                                error = %e,
                                "failed to clean up cancelled job directory"
                            );
                        }
                        if let Some(staging) = staging_dir
                            && let Err(e) = tokio::fs::remove_dir_all(&staging).await
                            && e.kind() != std::io::ErrorKind::NotFound
                        {
                            tracing::warn!(
                                dir = %staging.display(),
                                error = %e,
                                "failed to clean up cancelled job staging directory"
                            );
                        }
                    });

                    Ok(())
                } else {
                    Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::UpdateJob {
                job_id,
                category,
                metadata,
                reply,
            } => {
                let result = if let Some(state) = self.jobs.get_mut(&job_id) {
                    if let Some(cat) = &category {
                        state.spec.category = cat.clone();
                    }
                    if let Some(meta) = &metadata {
                        state.spec.metadata = meta.clone();
                    }
                    self.db_fire_and_forget(move |db| {
                        let cat_ref = category.as_ref().map(|c| c.as_deref());
                        let meta_ref = metadata.as_deref();
                        if let Err(e) = db.update_active_job(job_id, cat_ref, meta_ref) {
                            error!(error = %e, "db write failed for UpdateJob");
                        }
                    });
                    self.publish_snapshot();
                    Ok(())
                } else {
                    Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseAll { reply } => {
                self.global_paused = true;
                self.shared_state.set_paused(true);
                self.shared_state
                    .set_download_block(self.bandwidth_cap.to_download_block_state(true));
                if let Err(e) = self
                    .db_blocking(move |db| db.set_setting("global_paused", "true"))
                    .await
                {
                    error!(error = %e, "db write failed for PauseAll");
                }
                let _ = self.event_tx.send(PipelineEvent::GlobalPaused);
                let _ = reply.send(());
            }
            SchedulerCommand::ResumeAll { reply } => {
                self.global_paused = false;
                self.shared_state.set_paused(false);
                let _ = self.refresh_bandwidth_cap_window();
                if let Err(e) = self
                    .db_blocking(move |db| db.set_setting("global_paused", "false"))
                    .await
                {
                    error!(error = %e, "db write failed for ResumeAll");
                }
                let _ = self.event_tx.send(PipelineEvent::GlobalResumed);
                let _ = reply.send(());
            }
            SchedulerCommand::SetSpeedLimit {
                bytes_per_sec,
                reply,
            } => {
                self.rate_limiter.set_rate(bytes_per_sec);
                let _ = reply.send(());
            }
            SchedulerCommand::SetBandwidthCapPolicy { policy, reply } => {
                let result = self.apply_bandwidth_cap_policy(policy);
                let _ = reply.send(result);
            }
            SchedulerCommand::RebuildNntp {
                client,
                total_connections,
                reply,
            } => {
                if let Ok(new_client) = client.downcast::<NntpClient>() {
                    self.nntp = Arc::new(*new_client);
                    self.connection_ramp = total_connections.min(5);
                    self.tuner.set_connection_limit(total_connections);
                    info!(
                        total_connections,
                        max_downloads = self.tuner.params().max_concurrent_downloads,
                        "NNTP client rebuilt with new server config"
                    );
                }
                let _ = reply.send(());
            }
            SchedulerCommand::UpdateRuntimePaths {
                data_dir,
                intermediate_dir,
                complete_dir,
                reply,
            } => {
                let result = std::fs::create_dir_all(&data_dir)
                    .and_then(|_| std::fs::create_dir_all(&intermediate_dir))
                    .and_then(|_| std::fs::create_dir_all(&complete_dir))
                    .map_err(|e| weaver_scheduler::SchedulerError::Other(e.to_string()))
                    .map(|_| {
                        self.intermediate_dir = intermediate_dir;
                        self.complete_dir = complete_dir;
                        self.nzb_dir = data_dir.join(".weaver-nzbs");
                        let _ = std::fs::create_dir_all(&self.nzb_dir);
                    });
                let _ = reply.send(result);
            }
            SchedulerCommand::ReprocessJob { job_id, reply } => {
                let result = self.reprocess_job(job_id).await;
                let _ = reply.send(result);
            }
            SchedulerCommand::DeleteHistory {
                job_id,
                delete_files,
                reply,
            } => {
                let history_cleanup_dirs = match self.history_cleanup_dirs_for_job(job_id).await {
                    Ok(dirs) => dirs,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                // Collect the output dir before we remove the job from memory.
                let output_dir = if delete_files {
                    self.output_dir_for_job(job_id).await
                } else {
                    None
                };
                let result = match self.jobs.get(&job_id).map(|state| state.status.clone()) {
                    Some(status) if !is_terminal_status(&status) => {
                        Err(weaver_scheduler::SchedulerError::Other(
                            "cannot delete active job — cancel it first".into(),
                        ))
                    }
                    Some(_) => {
                        if let Err(error) = self
                            .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                            .await
                        {
                            let _ = reply.send(Err(error));
                            return;
                        }
                        self.cleanup_output_dir(output_dir.as_deref()).await;
                        self.purge_terminal_job_runtime(job_id);
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            let _ = db.delete_job_history(job_id.0);
                            let _ = db.delete_job_events(job_id.0);
                        });
                        self.publish_snapshot();
                        Ok(())
                    }
                    None => {
                        if let Err(error) = self
                            .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                            .await
                        {
                            let _ = reply.send(Err(error));
                            return;
                        }
                        self.cleanup_output_dir(output_dir.as_deref()).await;
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        let delete_result =
                            tokio::task::spawn_blocking(move || -> Result<(), String> {
                                db.delete_job_history(job_id.0)
                                    .map_err(|e| format!("failed to delete history row: {e}"))?;
                                db.delete_job_events(job_id.0)
                                    .map_err(|e| format!("failed to delete job events: {e}"))?;
                                Ok(())
                            })
                            .await;
                        match delete_result {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => {
                                let _ = reply.send(Err(SchedulerError::Other(error)));
                                return;
                            }
                            Err(error) => {
                                let _ = reply.send(Err(SchedulerError::Other(format!(
                                    "failed to join history delete task: {error}"
                                ))));
                                return;
                            }
                        }
                        self.publish_snapshot();
                        Ok(())
                    }
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::DeleteAllHistory {
                delete_files,
                reply,
            } => {
                let history_cleanup_dirs = match self.all_history_cleanup_dirs().await {
                    Ok(dirs) => dirs,
                    Err(error) => {
                        let _ = reply.send(Err(error));
                        return;
                    }
                };
                if let Err(error) = self
                    .cleanup_history_intermediate_dirs(&history_cleanup_dirs)
                    .await
                {
                    let _ = reply.send(Err(error));
                    return;
                }
                if delete_files {
                    let output_dirs = self.all_output_dirs().await;
                    for dir in &output_dirs {
                        self.cleanup_output_dir(Some(dir)).await;
                    }
                }
                let terminal_job_ids: Vec<JobId> = self
                    .jobs
                    .iter()
                    .filter_map(|(job_id, state)| {
                        is_terminal_status(&state.status).then_some(*job_id)
                    })
                    .collect();
                for job_id in terminal_job_ids {
                    self.purge_terminal_job_runtime(job_id);
                }
                self.finished_jobs.clear();
                let db = self.db.clone();
                let delete_result = tokio::task::spawn_blocking(move || -> Result<(), String> {
                    db.delete_all_job_history()
                        .map_err(|e| format!("failed to delete all job history: {e}"))?;
                    db.delete_all_job_events()
                        .map_err(|e| format!("failed to delete all job events: {e}"))?;
                    Ok(())
                })
                .await;
                match delete_result {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let _ = reply.send(Err(SchedulerError::Other(error)));
                        return;
                    }
                    Err(error) => {
                        let _ = reply.send(Err(SchedulerError::Other(format!(
                            "failed to join delete-all-history task: {error}"
                        ))));
                        return;
                    }
                }
                self.publish_snapshot();
                let _ = reply.send(Ok(()));
            }
            SchedulerCommand::Shutdown => unreachable!("handled in select"),
        }
    }

    fn cleanupable_history_output_dir(&self, output_dir: &std::path::Path) -> Option<PathBuf> {
        output_dir
            .strip_prefix(&self.intermediate_dir)
            .ok()
            .filter(|suffix| !suffix.as_os_str().is_empty())
            .map(|_| output_dir.to_path_buf())
    }

    async fn history_cleanup_dirs_for_job(
        &self,
        job_id: JobId,
    ) -> Result<BTreeSet<PathBuf>, weaver_scheduler::SchedulerError> {
        let mut dirs = BTreeSet::new();
        if let Some(state) = self.jobs.get(&job_id)
            && is_terminal_status(&state.status)
            && let Some(path) = self.cleanupable_history_output_dir(&state.working_dir)
        {
            dirs.insert(path);
        }

        let db = self.db.clone();
        let row = tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
            .await
            .map_err(|e| weaver_scheduler::SchedulerError::Other(e.to_string()))?
            .map_err(|e| weaver_scheduler::SchedulerError::Other(e.to_string()))?;
        if let Some(row) = row
            && let Some(output_dir) = row.output_dir
            && let Some(path) =
                self.cleanupable_history_output_dir(std::path::Path::new(&output_dir))
        {
            dirs.insert(path);
        }

        Ok(dirs)
    }

    async fn all_history_cleanup_dirs(
        &self,
    ) -> Result<BTreeSet<PathBuf>, weaver_scheduler::SchedulerError> {
        let mut dirs = BTreeSet::new();
        for state in self.jobs.values() {
            if is_terminal_status(&state.status)
                && let Some(path) = self.cleanupable_history_output_dir(&state.working_dir)
            {
                dirs.insert(path);
            }
        }

        let db = self.db.clone();
        let rows = tokio::task::spawn_blocking(move || {
            db.list_job_history(&weaver_state::HistoryFilter::default())
        })
        .await
        .map_err(|e| weaver_scheduler::SchedulerError::Other(e.to_string()))?
        .map_err(|e| weaver_scheduler::SchedulerError::Other(e.to_string()))?;
        for row in rows {
            if let Some(output_dir) = row.output_dir
                && let Some(path) =
                    self.cleanupable_history_output_dir(std::path::Path::new(&output_dir))
            {
                dirs.insert(path);
            }
        }

        Ok(dirs)
    }

    async fn cleanup_history_intermediate_dirs(
        &self,
        dirs: &BTreeSet<PathBuf>,
    ) -> Result<(), weaver_scheduler::SchedulerError> {
        for dir in dirs {
            match tokio::fs::remove_dir_all(dir).await {
                Ok(()) => {
                    info!(dir = %dir.display(), "removed historical intermediate directory");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(weaver_scheduler::SchedulerError::Other(format!(
                        "failed to remove historical intermediate directory '{}': {error}",
                        dir.display()
                    )));
                }
            }
        }

        Ok(())
    }

    /// Get the output directory for a single job (from memory or DB).
    async fn output_dir_for_job(&self, job_id: JobId) -> Option<PathBuf> {
        // Try in-memory finished_jobs first.
        if let Some(dir) = self
            .finished_jobs
            .iter()
            .find(|j| j.job_id == job_id)
            .and_then(|j| j.output_dir.as_ref())
        {
            return Some(PathBuf::from(dir));
        }
        // Try in-memory job state.
        if let Some(state) = self.jobs.get(&job_id) {
            return Some(state.working_dir.clone());
        }
        // Fall back to DB history row.
        let db = self.db.clone();
        let row = tokio::task::spawn_blocking(move || db.get_job_history(job_id.0))
            .await
            .ok()?
            .ok()?;
        row.and_then(|r| r.output_dir).map(PathBuf::from)
    }

    /// Collect output directories for all history jobs.
    async fn all_output_dirs(&self) -> Vec<PathBuf> {
        let mut dirs = Vec::new();
        for job in &self.finished_jobs {
            if let Some(dir) = &job.output_dir {
                dirs.push(PathBuf::from(dir));
            }
        }
        let db = self.db.clone();
        if let Ok(Ok(rows)) = tokio::task::spawn_blocking(move || {
            db.list_job_history(&weaver_state::HistoryFilter::default())
        })
        .await
        {
            for row in rows {
                if let Some(dir) = row.output_dir {
                    let path = PathBuf::from(&dir);
                    if !dirs.contains(&path) {
                        dirs.push(path);
                    }
                }
            }
        }
        dirs
    }

    /// Delete an output directory, logging warnings on failure.
    async fn cleanup_output_dir(&self, dir: Option<&std::path::Path>) {
        let Some(dir) = dir else { return };
        match tokio::fs::remove_dir_all(dir).await {
            Ok(()) => {
                info!(dir = %dir.display(), "removed complete output directory");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                warn!(
                    dir = %dir.display(),
                    error = %error,
                    "failed to remove complete output directory"
                );
            }
        }
    }

    /// Wait for in-flight work to finish during shutdown.
    async fn drain(&mut self) {
        if self.active_downloads > 0 {
            info!(
                active = self.active_downloads,
                "draining in-flight downloads"
            );
        }
        // Flush any pending segment commits.
        self.flush_segment_batch();
    }

    /// Flush the pending segment batch to SQLite.
    pub(super) fn flush_segment_batch(&mut self) {
        if self.segment_batch.is_empty() {
            return;
        }
        let batch = std::mem::take(&mut self.segment_batch);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = db.commit_segments(&batch) {
                tracing::error!(count = batch.len(), error = %e, "failed to commit segment batch");
            }
        });
    }
}

/// Write decoded segment data to the correct offset in the output file.
pub(super) async fn write_segment_to_disk(
    path: &std::path::Path,
    offset: u64,
    data: &[u8],
) -> Result<(), std::io::Error> {
    // Use std::fs in a single spawn_blocking call instead of tokio::fs which
    // internally spawns 3 separate blocking tasks (open, seek, write).
    // Under high throughput this reduces blocking pool pressure by ~66%.
    let path = path.to_owned();
    let data = data.to_vec();
    tokio::task::spawn_blocking(move || {
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

fn compute_write_backlog_budget_bytes(profile: &SystemProfile, buffers: &Arc<BufferPool>) -> usize {
    use weaver_core::buffer::BufferTier;
    use weaver_core::system::StorageClass;

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

/// Check if the output directory has enough free disk space for the job.
/// Logs a warning if space appears insufficient; does not hard-fail since
/// estimates from NZB metadata may be inaccurate.
pub(super) fn check_disk_space(output_dir: &std::path::Path, needed_bytes: u64) {
    let path_cstr = match std::ffi::CString::new(output_dir.to_str().unwrap_or(".").as_bytes()) {
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

/// Current timestamp in seconds since Unix epoch.
pub(super) fn timestamp_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn is_terminal_status(status: &JobStatus) -> bool {
    matches!(status, JobStatus::Complete | JobStatus::Failed { .. })
}

impl Pipeline {
    /// Get or create the extraction staging directory for a job.
    ///
    /// Staging lives under `{complete_dir}/.weaver-staging/{job_id}/` so that
    /// extracted chunks and assembled files are written directly to the
    /// complete filesystem (typically NFS/NAS), keeping local intermediate
    /// storage usage to just RAR volumes.
    pub(super) fn extraction_staging_dir(&mut self, job_id: JobId) -> PathBuf {
        if let Some(state) = self.jobs.get(&job_id)
            && let Some(ref staging) = state.staging_dir
        {
            return staging.clone();
        }
        let staging = self
            .complete_dir
            .join(".weaver-staging")
            .join(job_id.0.to_string());
        if let Some(state) = self.jobs.get_mut(&job_id) {
            state.staging_dir = Some(staging.clone());
        }
        staging
    }

    pub(super) fn note_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes += bytes;
        self.write_buffered_segments += segments;
        self.publish_write_backlog_metrics();
    }

    pub(super) fn release_write_buffered(&mut self, bytes: usize, segments: usize) {
        self.write_buffered_bytes = self.write_buffered_bytes.saturating_sub(bytes);
        self.write_buffered_segments = self.write_buffered_segments.saturating_sub(segments);
        self.publish_write_backlog_metrics();
    }

    pub(super) fn publish_write_backlog_metrics(&self) {
        self.metrics
            .write_buffered_bytes
            .store(self.write_buffered_bytes as u64, Ordering::Relaxed);
        self.metrics
            .write_buffered_segments
            .store(self.write_buffered_segments, Ordering::Relaxed);
    }

    pub(super) fn clear_job_write_backlog(&mut self, job_id: JobId) {
        let file_ids: Vec<NzbFileId> = self
            .write_buffers
            .keys()
            .copied()
            .filter(|file_id| file_id.job_id == job_id)
            .collect();

        let mut released_bytes = 0usize;
        let mut released_segments = 0usize;
        for file_id in file_ids {
            if let Some(buf) = self.write_buffers.remove(&file_id) {
                released_bytes += buf.buffered_bytes();
                released_segments += buf.buffered_len();
            }
        }

        if released_bytes > 0 || released_segments > 0 {
            self.release_write_buffered(released_bytes, released_segments);
        }
    }

    pub(super) fn clear_job_extraction_runtime(&mut self, job_id: JobId) {
        self.extracted_members.remove(&job_id);
        self.extracted_sets.remove(&job_id);
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.par2_bypassed.remove(&job_id);
    }

    pub(super) fn clear_job_rar_runtime(&mut self, job_id: JobId) {
        self.eagerly_deleted.remove(&job_id);
        self.rar_sets.retain(|(jid, _), _| *jid != job_id);
        self.normalization_retried.remove(&job_id);
    }

    pub(super) fn set_failed_extraction_member(&mut self, job_id: JobId, member_name: &str) {
        self.failed_extractions
            .entry(job_id)
            .or_default()
            .insert(member_name.to_string());
        let member_owned = member_name.to_string();
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.add_failed_extraction(job_id, &member_owned) {
                error!(
                    job_id = job_id.0,
                    member = %member_owned,
                    error = %error,
                    "failed to persist failed extraction member"
                );
            }
        });
    }

    pub(super) fn replace_failed_extraction_members(
        &mut self,
        job_id: JobId,
        members: HashSet<String>,
    ) {
        if members.is_empty() {
            self.failed_extractions.remove(&job_id);
        } else {
            self.failed_extractions.insert(job_id, members.clone());
        }
        let members_clone = members.clone();
        self.db_fire_and_forget(move |db| {
            if let Err(error) = db.replace_failed_extractions(job_id, &members_clone) {
                error!(
                    job_id = job_id.0,
                    error = %error,
                    "failed to persist failed extraction member set"
                );
            }
        });
    }

    pub(super) fn set_normalization_retried_state(
        &mut self,
        job_id: JobId,
        normalization_retried: bool,
    ) {
        if normalization_retried {
            self.normalization_retried.insert(job_id);
        } else {
            self.normalization_retried.remove(&job_id);
        }
        if let Err(error) = self
            .db
            .set_active_job_normalization_retried(job_id, normalization_retried)
        {
            error!(
                job_id = job_id.0,
                normalization_retried,
                error = %error,
                "failed to persist normalization retry state"
            );
        }
    }

    pub(super) fn persist_verified_suspect_volumes(
        &self,
        job_id: JobId,
        set_name: &str,
        volumes: &HashSet<u32>,
    ) {
        if let Err(error) = self
            .db
            .replace_verified_suspect_volumes(job_id, set_name, volumes)
        {
            error!(
                job_id = job_id.0,
                set_name,
                error = %error,
                "failed to persist verified suspect RAR volumes"
            );
        }
    }

    pub(super) fn write_target_for_file(
        &self,
        file_id: NzbFileId,
    ) -> Option<(JobId, String, PathBuf, PathBuf)> {
        let job_id = file_id.job_id;
        let state = self.jobs.get(&job_id)?;
        let file_asm = state.assembly.file(file_id)?;
        let filename = file_asm.filename().to_string();
        let working_dir = state.working_dir.clone();
        let file_path = working_dir.join(&filename);
        Some((job_id, filename, working_dir, file_path))
    }

    fn purge_terminal_job_runtime(&mut self, job_id: JobId) {
        self.jobs.remove(&job_id);
        self.job_order.retain(|id| *id != job_id);
        self.clear_par2_runtime_state(job_id);
        self.clear_job_extraction_runtime(job_id);
        self.active_download_passes.remove(&job_id);
        self.active_downloads_by_job.remove(&job_id);
        self.clear_job_rar_runtime(job_id);
        self.clear_job_write_backlog(job_id);
        self.update_queue_metrics();
    }

    /// Write a terminal job to SQLite history and add it to the finished_jobs list.
    pub(super) fn record_job_history(&mut self, job_id: JobId) {
        let state = match self.jobs.get(&job_id) {
            Some(s) => s,
            None => return,
        };

        let (status_str, error_message) = match &state.status {
            JobStatus::Complete => ("complete".to_string(), None),
            JobStatus::Failed { error } => ("failed".to_string(), Some(error.clone())),
            _ => return, // Not terminal — nothing to record.
        };

        let now = timestamp_secs() as i64;
        let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
        let created_at = now - elapsed_secs;
        let total = state.spec.total_bytes;
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        let health = if total == 0 {
            1000
        } else {
            ((total.saturating_sub(state.failed_bytes)) * 1000 / total) as u32
        };

        let row = weaver_state::JobHistoryRow {
            job_id: job_id.0,
            name: state.spec.name.clone(),
            status: status_str,
            error_message,
            total_bytes: total,
            downloaded_bytes: state.downloaded_bytes,
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            failed_bytes: state.failed_bytes,
            health,
            category: state.spec.category.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            nzb_path: None,
            created_at,
            completed_at: now,
            metadata: if state.spec.metadata.is_empty() {
                None
            } else {
                serde_json::to_string(&state.spec.metadata).ok()
            },
        };

        // Keep only the latest terminal snapshot for this job in runtime history.
        self.finished_jobs.retain(|j| j.job_id != job_id);
        let (optional_recovery_bytes, optional_recovery_downloaded_bytes) =
            state.assembly.optional_recovery_bytes();
        self.finished_jobs.push(JobInfo {
            job_id,
            name: state.spec.name.clone(),
            error: if let JobStatus::Failed { error } = &state.status {
                Some(error.clone())
            } else {
                None
            },
            status: state.status.clone(),
            progress: state.assembly.progress(),
            total_bytes: total,
            downloaded_bytes: state.downloaded_bytes,
            optional_recovery_bytes,
            optional_recovery_downloaded_bytes,
            failed_bytes: state.failed_bytes,
            health,
            password: state.spec.password.clone(),
            category: state.spec.category.clone(),
            metadata: state.spec.metadata.clone(),
            output_dir: Some(state.working_dir.display().to_string()),
            created_at_epoch_ms: state.created_at_epoch_ms,
        });

        let db = self.db.clone();
        let nzb_path = self.nzb_dir.join(format!("{}.nzb", job_id.0));
        tokio::task::spawn_blocking(move || {
            if let Err(e) = db.archive_job(job_id, &row) {
                tracing::error!(job_id = row.job_id, error = %e, "failed to archive job to history");
            }
            // Clean up the stored NZB file — no longer needed after archival.
            if let Err(e) = std::fs::remove_file(&nzb_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                tracing::warn!(path = %nzb_path.display(), error = %e, "failed to remove NZB file");
            }
        });
        self.purge_terminal_job_runtime(job_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use chrono::Timelike;
    use tempfile::TempDir;
    use tokio::sync::{RwLock, oneshot};
    use weaver_api::submit_nzb_bytes;
    use weaver_core::buffer::{BufferPool, BufferPoolConfig};
    use weaver_core::classify::FileRole;
    use weaver_core::config::{Config, SharedConfig};
    use weaver_core::system::{
        CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
        SystemProfile,
    };
    use weaver_nntp::client::{NntpClient, NntpClientConfig};
    use weaver_scheduler::{
        FileSpec, PipelineMetrics, SchedulerHandle, SegmentSpec, SharedPipelineState,
    };
    use weaver_state::Database;

    struct TestHarness {
        _temp_dir: TempDir,
        data_dir: PathBuf,
        handle: SchedulerHandle,
        config: SharedConfig,
        db: Database,
        pipeline_task: tokio::task::JoinHandle<()>,
    }

    impl TestHarness {
        async fn new() -> Self {
            let temp_dir = tempfile::tempdir().unwrap();
            let data_dir = temp_dir.path().join("data");
            let intermediate_dir = temp_dir.path().join("intermediate");
            let complete_dir = temp_dir.path().join("complete");
            let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
            let config: SharedConfig = Arc::new(RwLock::new(Config {
                data_dir: data_dir.display().to_string(),
                intermediate_dir: Some(intermediate_dir.display().to_string()),
                complete_dir: Some(complete_dir.display().to_string()),
                buffer_pool: None,
                tuner: None,
                servers: vec![],
                categories: vec![],
                retry: None,
                max_download_speed: None,
                isp_bandwidth_cap: None,
                cleanup_after_extract: Some(true),
                config_path: None,
            }));

            let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
            let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
            let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
            let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

            let nntp = NntpClient::new(NntpClientConfig {
                servers: vec![],
                max_idle_age: Duration::from_secs(1),
                max_retries_per_server: 1,
            });
            let profile = SystemProfile {
                cpu: CpuProfile {
                    physical_cores: 4,
                    logical_cores: 4,
                    simd: SimdSupport::default(),
                    cgroup_limit: None,
                },
                memory: MemoryProfile {
                    total_bytes: 8 * 1024 * 1024 * 1024,
                    available_bytes: 8 * 1024 * 1024 * 1024,
                    cgroup_limit: None,
                },
                disk: DiskProfile {
                    storage_class: StorageClass::Ssd,
                    filesystem: FilesystemType::Apfs,
                    sequential_write_mbps: 1000.0,
                    random_read_iops: 50_000.0,
                    same_filesystem: true,
                },
            };
            let buffers = BufferPool::new(BufferPoolConfig {
                small_count: 8,
                medium_count: 4,
                large_count: 2,
            });

            let mut pipeline = Pipeline::new(
                cmd_rx,
                event_tx,
                nntp,
                buffers,
                profile,
                data_dir.clone(),
                intermediate_dir,
                complete_dir,
                0,
                4,
                vec![],
                false,
                shared_state,
                db.clone(),
                config.clone(),
            )
            .await
            .unwrap();
            let pipeline_task = tokio::spawn(async move {
                pipeline.run().await;
            });

            Self {
                _temp_dir: temp_dir,
                data_dir,
                handle,
                config,
                db,
                pipeline_task,
            }
        }

        async fn shutdown(self) {
            self.handle.shutdown().await.unwrap();
            self.pipeline_task.await.unwrap();
        }
    }

    fn sample_nzb_bytes() -> Vec<u8> {
        br#"<?xml version="1.0" encoding="UTF-8"?>
        <nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
          <file poster="poster" date="1700000000" subject="Frieren.Sample.rar">
            <groups><group>alt.binaries.test</group></groups>
            <segments><segment bytes="100" number="1">msgid@example.com</segment></segments>
          </file>
        </nzb>"#
            .to_vec()
    }

    fn minimal_job_state(job_id: JobId, name: &str, working_dir: PathBuf) -> JobState {
        JobState {
            job_id,
            spec: JobSpec {
                name: name.to_string(),
                password: None,
                files: vec![],
                total_bytes: 0,
                category: None,
                metadata: vec![],
            },
            status: JobStatus::Downloading,
            assembly: JobAssembly::new(job_id),
            created_at: std::time::Instant::now(),
            created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
            working_dir,
            downloaded_bytes: 0,
            failed_bytes: 0,
            health_probing: false,
            held_segments: Vec::new(),
            download_queue: DownloadQueue::new(),
            recovery_queue: DownloadQueue::new(),
            staging_dir: None,
        }
    }

    fn history_row_with_output_dir(
        job_id: JobId,
        name: &str,
        status: &str,
        output_dir: PathBuf,
    ) -> weaver_state::JobHistoryRow {
        weaver_state::JobHistoryRow {
            job_id: job_id.0,
            name: name.to_string(),
            status: status.to_string(),
            error_message: None,
            total_bytes: 1024,
            downloaded_bytes: 1024,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some(output_dir.display().to_string()),
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
        }
    }

    async fn new_direct_pipeline_with_buffers(
        temp_dir: &TempDir,
        buffer_config: BufferPoolConfig,
        total_connections: usize,
    ) -> (Pipeline, PathBuf, PathBuf) {
        let data_dir = temp_dir.path().join("data");
        let intermediate_dir = temp_dir.path().join("intermediate");
        let complete_dir = temp_dir.path().join("complete");
        let db = Database::open(&temp_dir.path().join("weaver.db")).unwrap();
        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: data_dir.display().to_string(),
            intermediate_dir: Some(intermediate_dir.display().to_string()),
            complete_dir: Some(complete_dir.display().to_string()),
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            isp_bandwidth_cap: None,
            cleanup_after_extract: Some(true),
            config_path: None,
        }));

        let (_cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
        let shared_state = SharedPipelineState::new(PipelineMetrics::new(), vec![]);
        let nntp = NntpClient::new(NntpClientConfig {
            servers: vec![],
            max_idle_age: Duration::from_secs(1),
            max_retries_per_server: 1,
        });
        let profile = SystemProfile {
            cpu: CpuProfile {
                physical_cores: 4,
                logical_cores: 4,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 1000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        };
        let buffers = BufferPool::new(buffer_config);

        let pipeline = Pipeline::new(
            cmd_rx,
            event_tx,
            nntp,
            buffers,
            profile,
            data_dir,
            intermediate_dir.clone(),
            complete_dir.clone(),
            total_connections,
            4,
            vec![],
            false,
            shared_state,
            db,
            config,
        )
        .await
        .unwrap();

        (pipeline, intermediate_dir, complete_dir)
    }

    async fn new_direct_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
        new_direct_pipeline_with_buffers(
            temp_dir,
            BufferPoolConfig {
                small_count: 8,
                medium_count: 4,
                large_count: 2,
            },
            0,
        )
        .await
    }

    fn encode_article_part(
        filename: &str,
        payload: &[u8],
        part: u32,
        total: u32,
        begin: u64,
        file_size: u64,
    ) -> Bytes {
        let mut encoded = Vec::new();
        weaver_yenc::encode_part(
            payload,
            &mut encoded,
            128,
            filename,
            part,
            total,
            begin,
            begin + payload.len() as u64 - 1,
            file_size,
        )
        .unwrap();
        Bytes::from(encoded)
    }

    const TEST_RAR5_SIG: [u8; 8] = [0x52, 0x61, 0x72, 0x21, 0x1A, 0x07, 0x01, 0x00];

    fn encode_test_rar_vint(mut value: u64) -> Vec<u8> {
        let mut result = Vec::new();
        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            result.push(byte);
            if value == 0 {
                break;
            }
        }
        result
    }

    fn build_test_rar_header(
        header_type: u64,
        common_flags: u64,
        type_body: &[u8],
        extra: &[u8],
    ) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(header_type));

        let mut flags = common_flags;
        if !extra.is_empty() {
            flags |= 0x0001;
        }
        body.extend_from_slice(&encode_test_rar_vint(flags));
        if !extra.is_empty() {
            body.extend_from_slice(&encode_test_rar_vint(extra.len() as u64));
        }
        body.extend_from_slice(type_body);
        body.extend_from_slice(extra);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_test_rar_main_header(archive_flags: u64, volume_number: Option<u64>) -> Vec<u8> {
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(archive_flags));
        if let Some(volume_number) = volume_number {
            type_body.extend_from_slice(&encode_test_rar_vint(volume_number));
        }
        build_test_rar_header(1, 0, &type_body, &[])
    }

    fn build_test_rar_end_header(more_volumes: bool) -> Vec<u8> {
        let end_flags: u64 = if more_volumes { 0x0001 } else { 0 };
        build_test_rar_header(5, 0, &encode_test_rar_vint(end_flags), &[])
    }

    fn build_test_rar_file_header(
        filename: &str,
        common_flags_extra: u64,
        data_size: u64,
        unpacked_size: u64,
        data_crc: Option<u32>,
    ) -> Vec<u8> {
        let file_flags: u64 = if data_crc.is_some() { 0x0004 } else { 0 };
        let mut type_body = Vec::new();
        type_body.extend_from_slice(&encode_test_rar_vint(file_flags));
        type_body.extend_from_slice(&encode_test_rar_vint(unpacked_size));
        type_body.extend_from_slice(&encode_test_rar_vint(0o644));
        if let Some(data_crc) = data_crc {
            type_body.extend_from_slice(&data_crc.to_le_bytes());
        }
        type_body.extend_from_slice(&encode_test_rar_vint(0));
        type_body.extend_from_slice(&encode_test_rar_vint(1));
        type_body.extend_from_slice(&encode_test_rar_vint(filename.len() as u64));
        type_body.extend_from_slice(filename.as_bytes());

        let mut body = Vec::new();
        body.extend_from_slice(&encode_test_rar_vint(2));
        body.extend_from_slice(&encode_test_rar_vint(0x0002 | common_flags_extra));
        body.extend_from_slice(&encode_test_rar_vint(data_size));
        body.extend_from_slice(&type_body);

        let header_size = body.len() as u64;
        let header_size_bytes = encode_test_rar_vint(header_size);
        let crc = checksum::crc32(&[header_size_bytes.as_slice(), body.as_slice()].concat());

        let mut result = Vec::new();
        result.extend_from_slice(&crc.to_le_bytes());
        result.extend_from_slice(&header_size_bytes);
        result.extend_from_slice(&body);
        result
    }

    fn build_multifile_multivolume_rar_set() -> Vec<(String, Vec<u8>)> {
        let episode_a = b"episode-a-payload";
        let episode_b = b"episode-b-payload";
        let episode_a_crc = checksum::crc32(episode_a);
        let episode_b_crc = checksum::crc32(episode_b);

        let a_part1 = &episode_a[..8];
        let a_part2 = &episode_a[8..];
        let b_part1 = &episode_b[..8];
        let b_part2 = &episode_b[8..];

        let mut part01 = Vec::new();
        part01.extend_from_slice(&TEST_RAR5_SIG);
        part01.extend_from_slice(&build_test_rar_main_header(0x0001, None));
        part01.extend_from_slice(&build_test_rar_file_header(
            "E01.mkv",
            0x0010,
            a_part1.len() as u64,
            episode_a.len() as u64,
            None,
        ));
        part01.extend_from_slice(a_part1);
        part01.extend_from_slice(&build_test_rar_end_header(true));

        let mut part02 = Vec::new();
        part02.extend_from_slice(&TEST_RAR5_SIG);
        part02.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(1)));
        part02.extend_from_slice(&build_test_rar_file_header(
            "E01.mkv",
            0x0008,
            a_part2.len() as u64,
            episode_a.len() as u64,
            Some(episode_a_crc),
        ));
        part02.extend_from_slice(a_part2);
        part02.extend_from_slice(&build_test_rar_end_header(true));

        let mut part03 = Vec::new();
        part03.extend_from_slice(&TEST_RAR5_SIG);
        part03.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(2)));
        part03.extend_from_slice(&build_test_rar_file_header(
            "E02.mkv",
            0x0010,
            b_part1.len() as u64,
            episode_b.len() as u64,
            None,
        ));
        part03.extend_from_slice(b_part1);
        part03.extend_from_slice(&build_test_rar_end_header(true));

        let mut part04 = Vec::new();
        part04.extend_from_slice(&TEST_RAR5_SIG);
        part04.extend_from_slice(&build_test_rar_main_header(0x0001 | 0x0002, Some(3)));
        part04.extend_from_slice(&build_test_rar_file_header(
            "E02.mkv",
            0x0008,
            b_part2.len() as u64,
            episode_b.len() as u64,
            Some(episode_b_crc),
        ));
        part04.extend_from_slice(b_part2);
        part04.extend_from_slice(&build_test_rar_end_header(false));

        vec![
            ("show.part01.rar".to_string(), part01),
            ("show.part02.rar".to_string(), part02),
            ("show.part03.rar".to_string(), part03),
            ("show.part04.rar".to_string(), part04),
        ]
    }

    fn dummy_rar_volume_facts(volume_number: u32) -> weaver_rar::RarVolumeFacts {
        weaver_rar::RarVolumeFacts {
            format: 5,
            volume_number,
            more_volumes: true,
            is_solid: false,
            is_encrypted: false,
            members: Vec::new(),
        }
    }

    fn rar_job_spec(name: &str, files: &[(String, Vec<u8>)]) -> JobSpec {
        JobSpec {
            name: name.to_string(),
            password: None,
            total_bytes: files.iter().map(|(_, bytes)| bytes.len() as u64).sum(),
            category: None,
            metadata: vec![],
            files: files
                .iter()
                .enumerate()
                .map(|(index, (filename, bytes))| FileSpec {
                    filename: filename.clone(),
                    role: FileRole::from_filename(filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: bytes.len() as u32,
                        message_id: format!("rar-{index}@example.com"),
                    }],
                })
                .collect(),
        }
    }

    fn standalone_job_spec(name: &str, files: &[(String, u32)]) -> JobSpec {
        JobSpec {
            name: name.to_string(),
            password: None,
            total_bytes: files.iter().map(|(_, bytes)| *bytes as u64).sum(),
            category: None,
            metadata: vec![],
            files: files
                .iter()
                .enumerate()
                .map(|(index, (filename, bytes))| FileSpec {
                    filename: filename.clone(),
                    role: FileRole::Standalone,
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: *bytes,
                        message_id: format!("segment-{index}@example.com"),
                    }],
                })
                .collect(),
        }
    }

    fn minimal_par2_file_set() -> Par2FileSet {
        Par2FileSet {
            recovery_set_id: weaver_par2::RecoverySetId::from_bytes([0; 16]),
            slice_size: 1,
            recovery_file_ids: Vec::new(),
            non_recovery_file_ids: Vec::new(),
            files: HashMap::new(),
            slice_checksums: HashMap::new(),
            recovery_slices: std::collections::BTreeMap::new(),
            creator: None,
        }
    }

    fn placement_par2_file_set(files: &[(String, Vec<u8>)]) -> Par2FileSet {
        let slice_size = files
            .iter()
            .map(|(_, bytes)| bytes.len() as u64)
            .max()
            .unwrap_or(1)
            .max(1);
        let mut recovery_file_ids = Vec::new();
        let mut descriptions = HashMap::new();

        for (index, (filename, bytes)) in files.iter().enumerate() {
            let mut raw_id = [0u8; 16];
            raw_id[12..].copy_from_slice(&((index as u32) + 1).to_be_bytes());
            let file_id = weaver_par2::FileId::from_bytes(raw_id);
            let hash_full = weaver_core::checksum::md5(bytes);
            let hash_16k = weaver_core::checksum::md5(&bytes[..bytes.len().min(16 * 1024)]);
            recovery_file_ids.push(file_id);
            descriptions.insert(
                file_id,
                weaver_par2::FileDescription {
                    file_id,
                    hash_full,
                    hash_16k,
                    length: bytes.len() as u64,
                    filename: filename.clone(),
                },
            );
        }

        Par2FileSet {
            recovery_set_id: weaver_par2::RecoverySetId::from_bytes([9; 16]),
            slice_size,
            recovery_file_ids,
            non_recovery_file_ids: Vec::new(),
            files: descriptions,
            slice_checksums: HashMap::new(),
            recovery_slices: std::collections::BTreeMap::new(),
            creator: None,
        }
    }

    fn install_test_par2_runtime(
        pipeline: &mut Pipeline,
        job_id: JobId,
        par2_set: Par2FileSet,
        files: &[(u32, &str, u32, bool)],
    ) {
        let runtime = pipeline.ensure_par2_runtime(job_id);
        runtime.set = Some(Arc::new(par2_set));
        runtime.files.clear();
        for (file_index, filename, recovery_blocks, promoted) in files {
            runtime.files.insert(
                *file_index,
                Par2FileRuntime {
                    filename: (*filename).to_string(),
                    recovery_blocks: *recovery_blocks,
                    promoted: *promoted,
                },
            );
        }
    }

    fn build_test_par2_packet(
        packet_type: &[u8; 16],
        body: &[u8],
        recovery_set_id: [u8; 16],
    ) -> Vec<u8> {
        let length = (weaver_par2::packet::header::HEADER_SIZE + body.len()) as u64;
        let mut hash_input = Vec::new();
        hash_input.extend_from_slice(&recovery_set_id);
        hash_input.extend_from_slice(packet_type);
        hash_input.extend_from_slice(body);
        let packet_hash = checksum::md5(&hash_input);

        let mut data = Vec::new();
        data.extend_from_slice(weaver_par2::packet::header::MAGIC);
        data.extend_from_slice(&length.to_le_bytes());
        data.extend_from_slice(&packet_hash);
        data.extend_from_slice(&recovery_set_id);
        data.extend_from_slice(packet_type);
        data.extend_from_slice(body);
        data
    }

    fn build_test_par2_index(filename: &str, file_data: &[u8], slice_size: u64) -> Vec<u8> {
        let file_length = file_data.len() as u64;
        let hash_full = checksum::md5(file_data);
        let hash_16k = checksum::md5(&file_data[..file_data.len().min(16 * 1024)]);

        let mut file_id_input = Vec::new();
        file_id_input.extend_from_slice(&hash_16k);
        file_id_input.extend_from_slice(&file_length.to_le_bytes());
        file_id_input.extend_from_slice(filename.as_bytes());
        let file_id_bytes = checksum::md5(&file_id_input);

        let num_slices = if file_length == 0 {
            0
        } else {
            file_length.div_ceil(slice_size) as usize
        };

        let mut checksums = Vec::new();
        for slice_index in 0..num_slices {
            let start = slice_index as u64 * slice_size;
            let end = ((start + slice_size) as usize).min(file_data.len());
            let slice_data = &file_data[start as usize..end];
            let mut state = weaver_par2::SliceChecksumState::new();
            state.update(slice_data);
            let (crc32, md5) =
                state.finalize(((slice_data.len() as u64) < slice_size).then_some(slice_size));
            checksums.push(weaver_par2::SliceChecksum { crc32, md5 });
        }

        let mut main_body = Vec::new();
        main_body.extend_from_slice(&slice_size.to_le_bytes());
        main_body.extend_from_slice(&1u32.to_le_bytes());
        main_body.extend_from_slice(&file_id_bytes);
        let recovery_set_id = checksum::md5(&main_body);

        let mut file_desc_body = Vec::new();
        file_desc_body.extend_from_slice(&file_id_bytes);
        file_desc_body.extend_from_slice(&hash_full);
        file_desc_body.extend_from_slice(&hash_16k);
        file_desc_body.extend_from_slice(&file_length.to_le_bytes());
        file_desc_body.extend_from_slice(filename.as_bytes());
        while file_desc_body.len() % 4 != 0 {
            file_desc_body.push(0);
        }

        let mut ifsc_body = Vec::new();
        ifsc_body.extend_from_slice(&file_id_bytes);
        for checksum in checksums {
            ifsc_body.extend_from_slice(&checksum.md5);
            ifsc_body.extend_from_slice(&checksum.crc32.to_le_bytes());
        }

        let mut stream = Vec::new();
        stream.extend_from_slice(&build_test_par2_packet(
            weaver_par2::packet::header::TYPE_MAIN,
            &main_body,
            recovery_set_id,
        ));
        stream.extend_from_slice(&build_test_par2_packet(
            weaver_par2::packet::header::TYPE_FILE_DESC,
            &file_desc_body,
            recovery_set_id,
        ));
        stream.extend_from_slice(&build_test_par2_packet(
            weaver_par2::packet::header::TYPE_IFSC,
            &ifsc_body,
            recovery_set_id,
        ));
        stream
    }

    fn par2_only_job_spec(name: &str, filename: &str, bytes: u32) -> JobSpec {
        JobSpec {
            name: name.to_string(),
            password: None,
            total_bytes: bytes as u64,
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: filename.to_string(),
                role: FileRole::from_filename(filename),
                groups: vec!["alt.binaries.test".to_string()],
                segments: vec![SegmentSpec {
                    number: 0,
                    bytes,
                    message_id: "par2-0@example.com".to_string(),
                }],
            }],
        }
    }

    fn build_empty_rar_volume() -> Vec<u8> {
        let mut volume = Vec::new();
        volume.extend_from_slice(&TEST_RAR5_SIG);
        volume.extend_from_slice(&build_test_rar_main_header(0, None));
        volume.extend_from_slice(&build_test_rar_end_header(false));
        volume
    }

    fn segmented_job_spec(name: &str, filename: &str, segment_sizes: &[u32]) -> JobSpec {
        JobSpec {
            name: name.to_string(),
            password: None,
            total_bytes: segment_sizes.iter().map(|size| *size as u64).sum(),
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: filename.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                segments: segment_sizes
                    .iter()
                    .enumerate()
                    .map(|(index, bytes)| SegmentSpec {
                        number: index as u32,
                        bytes: *bytes,
                        message_id: format!("segment-{index}@example.com"),
                    })
                    .collect(),
            }],
        }
    }

    async fn insert_active_job(pipeline: &mut Pipeline, job_id: JobId, spec: JobSpec) -> PathBuf {
        let dir_name = job::sanitize_dirname(&spec.name);
        let candidate = pipeline.intermediate_dir.join(&dir_name);
        let working_dir = if candidate.exists() {
            pipeline
                .intermediate_dir
                .join(format!("{}.#{}", dir_name, job_id.0))
        } else {
            candidate
        };
        tokio::fs::create_dir_all(&working_dir).await.unwrap();
        pipeline
            .db
            .create_active_job(&weaver_state::ActiveJob {
                job_id,
                nzb_hash: [0; 32],
                nzb_path: working_dir.join(format!("{}.nzb", job_id.0)),
                output_dir: working_dir.clone(),
                created_at: 0,
                category: spec.category.clone(),
                metadata: spec.metadata.clone(),
            })
            .unwrap();
        let (assembly, download_queue, recovery_queue) =
            Pipeline::build_job_assembly(job_id, &spec, &HashSet::new());
        pipeline.jobs.insert(
            job_id,
            JobState {
                job_id,
                spec,
                status: JobStatus::Downloading,
                assembly,
                created_at: std::time::Instant::now(),
                created_at_epoch_ms: weaver_scheduler::job::epoch_ms_now(),
                working_dir: working_dir.clone(),
                downloaded_bytes: 0,
                failed_bytes: 0,
                health_probing: false,
                held_segments: Vec::new(),
                download_queue,
                recovery_queue,
                staging_dir: None,
            },
        );
        pipeline.job_order.push(job_id);
        working_dir
    }

    async fn write_and_complete_rar_volume(
        pipeline: &mut Pipeline,
        job_id: JobId,
        file_index: u32,
        filename: &str,
        bytes: &[u8],
    ) {
        let working_dir = pipeline.jobs.get(&job_id).unwrap().working_dir.clone();
        tokio::fs::write(working_dir.join(filename), bytes)
            .await
            .unwrap();

        let file_id = NzbFileId { job_id, file_index };
        {
            let state = pipeline.jobs.get_mut(&job_id).unwrap();
            state
                .assembly
                .file_mut(file_id)
                .unwrap()
                .commit_segment(0, bytes.len() as u32)
                .unwrap();
        }

        pipeline.try_update_archive_topology(job_id, file_id).await;
    }

    fn member_span(
        pipeline: &Pipeline,
        job_id: JobId,
        set_name: &str,
        member_name: &str,
    ) -> Option<(u32, u32)> {
        pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for(set_name))
            .and_then(|topology| {
                topology
                    .members
                    .iter()
                    .find(|member| member.name == member_name)
                    .map(|member| (member.first_volume, member.last_volume))
            })
    }

    fn unresolved_spans(pipeline: &Pipeline, job_id: JobId, set_name: &str) -> Vec<(u32, u32)> {
        let mut spans: Vec<(u32, u32)> = pipeline
            .jobs
            .get(&job_id)
            .and_then(|state| state.assembly.archive_topology_for(set_name))
            .map(|topology| {
                topology
                    .unresolved_spans
                    .iter()
                    .map(|span| (span.first_volume, span.last_volume))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        spans.sort_unstable();
        spans
    }

    fn job_status_for_assert(pipeline: &Pipeline, job_id: JobId) -> Option<JobStatus> {
        pipeline
            .jobs
            .get(&job_id)
            .map(|state| state.status.clone())
            .or_else(|| {
                pipeline
                    .finished_jobs
                    .iter()
                    .find(|job| job.job_id == job_id)
                    .map(|job| job.status.clone())
            })
    }

    async fn next_extraction_done(pipeline: &mut Pipeline) -> ExtractionDone {
        tokio::time::timeout(Duration::from_secs(2), pipeline.extract_done_rx.recv())
            .await
            .expect("extraction result should arrive")
            .expect("extraction channel should stay open")
    }

    async fn wait_until(
        timeout_duration: Duration,
        mut predicate: impl FnMut() -> bool,
    ) -> Result<(), &'static str> {
        let deadline = tokio::time::Instant::now() + timeout_duration;
        loop {
            if predicate() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("condition timed out");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn drain_decode_results(pipeline: &mut Pipeline, expected: usize) {
        for _ in 0..expected {
            let done = tokio::time::timeout(Duration::from_secs(5), pipeline.decode_done_rx.recv())
                .await
                .expect("decode result should arrive")
                .expect("decode channel should stay open");
            pipeline.handle_decode_done(done).await;
        }
    }

    #[tokio::test]
    async fn submit_nzb_persists_zstd_and_creates_active_job() {
        let harness = TestHarness::new().await;
        let nzb_bytes = sample_nzb_bytes();

        let submitted = tokio::time::timeout(
            Duration::from_secs(2),
            submit_nzb_bytes(
                &harness.handle,
                &harness.config,
                &nzb_bytes,
                Some("Frieren.Sample.nzb".to_string()),
                None,
                None,
                vec![("source".to_string(), "test".to_string())],
            ),
        )
        .await
        .unwrap()
        .unwrap();

        wait_until(Duration::from_secs(2), || {
            harness
                .db
                .load_active_jobs()
                .map(|jobs| jobs.contains_key(&submitted.job_id))
                .unwrap_or(false)
        })
        .await
        .unwrap();

        let info = harness.handle.get_job(submitted.job_id).unwrap();
        assert_eq!(info.status, JobStatus::Downloading);
        assert!(
            info.metadata
                .contains(&("source".to_string(), "test".to_string()))
        );
        assert!(
            info.metadata.iter().any(|(key, value)| {
                key == "weaver.original_title" && value == "Frieren.Sample"
            })
        );

        let stored_nzb = tokio::fs::read(
            harness
                .data_dir
                .join(".weaver-nzbs")
                .join(format!("{}.nzb", submitted.job_id.0)),
        )
        .await
        .unwrap();
        assert!(stored_nzb.starts_with(&[0x28, 0xB5, 0x2F, 0xFD]));
        assert_eq!(crate::decompress_nzb(&stored_nzb), nzb_bytes);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn move_to_complete_uses_unique_destination_for_duplicate_job_names() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(10067);
        let job_name = "Frieren Beyond Journeys End";
        let payload_dir = "Frieren.Beyond.Journeys.End.S01.1080p.BluRay.Opus2.0.x265.DUAL-Anitsu";
        let episode_name = "Frieren.Beyond.Journeys.End.S01E01.mkv";

        let existing_dest = complete_dir.join(job::sanitize_dirname(job_name));
        tokio::fs::create_dir_all(existing_dest.join(payload_dir))
            .await
            .unwrap();
        tokio::fs::write(
            existing_dest.join(payload_dir).join(episode_name),
            b"existing",
        )
        .await
        .unwrap();

        let working_dir =
            intermediate_dir.join(format!("{}.#{}", job::sanitize_dirname(job_name), job_id.0));
        tokio::fs::create_dir_all(working_dir.join(payload_dir))
            .await
            .unwrap();
        tokio::fs::write(working_dir.join(payload_dir).join(episode_name), b"new")
            .await
            .unwrap();

        pipeline.jobs.insert(
            job_id,
            minimal_job_state(job_id, job_name, working_dir.clone()),
        );

        let dest = pipeline.move_to_complete(job_id).await.unwrap();
        let expected_dest =
            complete_dir.join(format!("{}.#{}", job::sanitize_dirname(job_name), job_id.0));

        assert_eq!(dest, expected_dest);
        assert_eq!(
            pipeline.jobs.get(&job_id).unwrap().working_dir,
            expected_dest
        );
        assert!(!working_dir.exists());
        assert_eq!(
            tokio::fs::read(dest.join(payload_dir).join(episode_name))
                .await
                .unwrap(),
            b"new"
        );
        assert_eq!(
            tokio::fs::read(existing_dest.join(payload_dir).join(episode_name))
                .await
                .unwrap(),
            b"existing"
        );
    }

    #[tokio::test]
    async fn failed_final_move_marks_job_failed_instead_of_complete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(10068);
        let job_name = "Frieren Broken Final Move";
        let missing_working_dir = intermediate_dir.join("missing");

        pipeline.jobs.insert(
            job_id,
            minimal_job_state(job_id, job_name, missing_working_dir),
        );

        pipeline.check_job_completion(job_id).await;

        let status = job_status_for_assert(&pipeline, job_id).unwrap();
        assert!(matches!(status, JobStatus::Failed { .. }));
        let JobStatus::Failed { error } = &status else {
            unreachable!();
        };
        assert!(error.contains("failed to read working directory"));
        assert!(!complete_dir.join(job::sanitize_dirname(job_name)).exists());
    }

    #[tokio::test]
    async fn tiny_write_budget_evicts_out_of_order_segments_and_job_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 1,
                medium_count: 1,
                large_count: 1,
            },
            4,
        )
        .await;
        let job_id = JobId(20001);
        let filename = "episode.bin";
        let payload_size =
            (weaver_core::buffer::BufferTier::Small.size_bytes() + 256 * 1024) as u32;
        let spec = segmented_job_spec(
            "Write Backlog Budget",
            filename,
            &[payload_size, payload_size, payload_size],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline.write_backlog_budget_bytes = payload_size as usize;

        let total_size = payload_size as u64 * 3;
        for segment_number in [1u32, 2u32] {
            let payload = vec![segment_number as u8 + 1; payload_size as usize];
            let raw = encode_article_part(
                filename,
                &payload,
                segment_number + 1,
                3,
                segment_number as u64 * payload_size as u64 + 1,
                total_size,
            );
            let segment_id = SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            };

            pipeline.active_downloads += 1;
            tokio::time::timeout(
                Duration::from_secs(1),
                pipeline.handle_download_done(DownloadResult {
                    segment_id,
                    data: Ok(raw),
                    is_recovery: false,
                    retry_count: 0,
                }),
            )
            .await
            .expect("download completion should not block");
        }

        drain_decode_results(&mut pipeline, 2).await;

        assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
        assert_eq!(
            pipeline
                .buffers
                .available(weaver_core::buffer::BufferTier::Medium),
            1
        );
        assert!(
            pipeline
                .metrics
                .direct_write_evictions
                .load(Ordering::Relaxed)
                >= 1,
            "tiny write budget should degrade to direct writes"
        );
        assert!(
            pipeline
                .metrics
                .write_buffered_bytes
                .load(Ordering::Relaxed)
                <= payload_size as u64
        );

        let payload = vec![1u8; payload_size as usize];
        let raw = encode_article_part(filename, &payload, 1, 3, 1, total_size);
        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                segment_id: SegmentId {
                    file_id: NzbFileId {
                        job_id,
                        file_index: 0,
                    },
                    segment_number: 0,
                },
                data: Ok(raw),
                is_recovery: false,
                retry_count: 0,
                decoded_inline: false,
            })
            .await;
        drain_decode_results(&mut pipeline, 1).await;

        assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
        assert_eq!(pipeline.write_buffered_bytes, 0);
        assert_eq!(pipeline.write_buffered_segments, 0);
        assert_eq!(
            pipeline
                .metrics
                .write_buffered_bytes
                .load(Ordering::Relaxed),
            0
        );
        assert!(matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ));
    }

    #[tokio::test]
    async fn in_order_segments_keep_write_cursor_until_file_completes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 2,
                medium_count: 1,
                large_count: 1,
            },
            4,
        )
        .await;
        let job_id = JobId(20007);
        let segment_count = 6u32;
        let payload_size = 128u32;
        let total_size = segment_count * payload_size;
        let filename = "cursor.bin";
        let spec = JobSpec {
            name: "Write Cursor".to_string(),
            password: None,
            total_bytes: total_size as u64,
            category: None,
            metadata: vec![],
            files: vec![FileSpec {
                filename: filename.to_string(),
                role: FileRole::Standalone,
                groups: vec!["alt.binaries.test".to_string()],
                segments: (0..segment_count)
                    .map(|number| SegmentSpec {
                        number,
                        bytes: payload_size,
                        message_id: format!("cursor-{number}@example.com"),
                    })
                    .collect(),
            }],
        };
        insert_active_job(&mut pipeline, job_id, spec).await;

        for segment_number in 0..segment_count {
            let payload = vec![segment_number as u8 + 1; payload_size as usize];
            let raw = encode_article_part(
                filename,
                &payload,
                segment_number + 1,
                segment_count,
                segment_number as u64 * payload_size as u64 + 1,
                total_size as u64,
            );
            let segment_id = SegmentId {
                file_id: NzbFileId {
                    job_id,
                    file_index: 0,
                },
                segment_number,
            };

            pipeline.active_downloads += 1;
            pipeline
                .handle_download_done(DownloadResult {
                    segment_id,
                    data: Ok(raw),
                    is_recovery: false,
                    retry_count: 0,
                })
                .await;
        }

        drain_decode_results(&mut pipeline, segment_count as usize).await;

        assert_eq!(pipeline.write_buffered_bytes, 0);
        assert_eq!(pipeline.write_buffered_segments, 0);
        assert!(!pipeline.write_buffers.contains_key(&NzbFileId {
            job_id,
            file_index: 0,
        }));
        assert_eq!(
            pipeline.metrics.segments_committed.load(Ordering::Relaxed),
            segment_count as u64
        );
        assert!(matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ));
    }

    #[tokio::test]
    async fn dispatch_downloads_respects_decode_backpressure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 2,
                medium_count: 1,
                large_count: 1,
            },
            4,
        )
        .await;
        let job_id = JobId(20002);
        let files = vec![("queued.bin".to_string(), 512u32)];
        let spec = standalone_job_spec("Decode Queue Limit", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        pipeline
            .metrics
            .decode_pending
            .store(pipeline.tuner.params().max_decode_queue, Ordering::Relaxed);
        pipeline.dispatch_downloads();

        assert_eq!(pipeline.active_downloads, 0);
        assert_eq!(
            pipeline.jobs.get(&job_id).unwrap().download_queue.len(),
            files.len()
        );
    }

    #[tokio::test]
    async fn dispatch_downloads_ignores_write_backlog_when_raw_decode_queue_is_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 1,
                medium_count: 1,
                large_count: 1,
            },
            2,
        )
        .await;
        let job_id = JobId(20004);
        let spec = standalone_job_spec(
            "Write Backlog Does Not Gate Dispatch",
            &[("queued.bin".to_string(), 512u32)],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline.connection_ramp = 1;

        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let buffered = BufferedDecodedSegment {
            segment_id: SegmentId {
                file_id,
                segment_number: 99,
            },
            decoded_size: 4096,
            crc32: 0,
            data: DecodedChunk::from(vec![7u8; 4096]),
            yenc_name: "queued.bin".to_string(),
        };
        let buffered_len = buffered.len_bytes();
        pipeline
            .write_buffers
            .entry(file_id)
            .or_insert_with(|| WriteReorderBuffer::new(4))
            .insert(4096, buffered);
        pipeline.note_write_buffered(buffered_len, 1);

        pipeline.dispatch_downloads();

        assert_eq!(pipeline.active_downloads, 1);
        assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
        assert_eq!(
            pipeline
                .metrics
                .write_buffered_segments
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test]
    async fn dispatch_downloads_blocks_when_isp_bandwidth_cap_is_hit() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 1,
                medium_count: 1,
                large_count: 1,
            },
            2,
        )
        .await;
        let job_id = JobId(20005);
        let spec = standalone_job_spec("ISP Cap Gate", &[("queued.bin".to_string(), 512u32)]);
        insert_active_job(&mut pipeline, job_id, spec).await;
        pipeline.connection_ramp = 1;

        let now = chrono::Local::now();
        let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
        pipeline
            .db
            .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 1024)
            .unwrap();
        pipeline
            .apply_bandwidth_cap_policy(Some(weaver_core::config::IspBandwidthCapConfig {
                enabled: true,
                period: weaver_core::config::IspBandwidthCapPeriod::Daily,
                limit_bytes: 512,
                reset_time_minutes_local: reset_minutes,
                weekly_reset_weekday: weaver_core::config::IspBandwidthCapWeekday::Mon,
                monthly_reset_day: 1,
            }))
            .unwrap();

        pipeline.dispatch_downloads();

        assert_eq!(pipeline.active_downloads, 0);
        assert_eq!(pipeline.jobs.get(&job_id).unwrap().download_queue.len(), 1);
        assert_eq!(
            pipeline.shared_state.download_block().kind,
            weaver_scheduler::handle::DownloadBlockKind::IspCap
        );
    }

    #[tokio::test]
    async fn set_bandwidth_cap_policy_recomputes_current_window_usage_from_ledger() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 1,
                medium_count: 1,
                large_count: 1,
            },
            1,
        )
        .await;

        let now = chrono::Local::now();
        let reset_minutes = (now.hour() as u16 * 60 + now.minute() as u16).saturating_sub(1);
        pipeline
            .db
            .add_bandwidth_usage_minute(now.timestamp().div_euclid(60), 4096)
            .unwrap();

        pipeline
            .apply_bandwidth_cap_policy(Some(weaver_core::config::IspBandwidthCapConfig {
                enabled: false,
                period: weaver_core::config::IspBandwidthCapPeriod::Daily,
                limit_bytes: 10_000,
                reset_time_minutes_local: reset_minutes,
                weekly_reset_weekday: weaver_core::config::IspBandwidthCapWeekday::Mon,
                monthly_reset_day: 1,
            }))
            .unwrap();

        let block = pipeline.shared_state.download_block();
        assert_eq!(block.used_bytes, 4096);
        assert_eq!(block.remaining_bytes, 10_000 - 4096);
        assert!(!block.cap_enabled);
    }

    #[tokio::test]
    async fn decode_failure_drains_backlog_and_keeps_commands_responsive() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline_with_buffers(
            &temp_dir,
            BufferPoolConfig {
                small_count: 1,
                medium_count: 1,
                large_count: 1,
            },
            4,
        )
        .await;
        let job_id = JobId(20003);
        let files = vec![("broken.bin".to_string(), 64u32)];
        let spec = standalone_job_spec("Decode Failure", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        let segment_id = SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        };
        pipeline.active_downloads += 1;
        pipeline
            .handle_download_done(DownloadResult {
                segment_id,
                data: Ok(Bytes::from_static(b"not a yenc article")),
                is_recovery: false,
                retry_count: 0,
                decoded_inline: false,
            })
            .await;

        assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 1);

        let done = tokio::time::timeout(Duration::from_secs(2), pipeline.decode_done_rx.recv())
            .await
            .expect("decode failure should arrive")
            .expect("decode channel should stay open");
        let DecodeDone::Failed {
            segment_id: failed_segment,
            ..
        } = &done
        else {
            panic!("expected decode failure");
        };
        assert_eq!(*failed_segment, segment_id);

        pipeline.handle_decode_done(done).await;

        assert_eq!(pipeline.metrics.decode_pending.load(Ordering::Relaxed), 0);
        assert_eq!(pipeline.metrics.decode_errors.load(Ordering::Relaxed), 1);

        let (reply, recv) = oneshot::channel();
        pipeline
            .handle_command(SchedulerCommand::PauseAll { reply })
            .await;
        tokio::time::timeout(Duration::from_secs(1), recv)
            .await
            .expect("pause reply should arrive")
            .unwrap();
        assert!(pipeline.global_paused);
        assert_eq!(
            pipeline.db.get_setting("global_paused").unwrap().as_deref(),
            Some("true")
        );

        let (reply, recv) = oneshot::channel();
        pipeline
            .handle_command(SchedulerCommand::ResumeAll { reply })
            .await;
        tokio::time::timeout(Duration::from_secs(1), recv)
            .await
            .expect("resume reply should arrive")
            .unwrap();
        assert!(!pipeline.global_paused);
        assert_eq!(
            pipeline.db.get_setting("global_paused").unwrap().as_deref(),
            Some("false")
        );
    }

    #[tokio::test]
    async fn delete_history_removes_intermediate_output_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30020);
        let output_dir = intermediate_dir.join("history-cleanup-job");
        tokio::fs::create_dir_all(&output_dir).await.unwrap();
        tokio::fs::write(output_dir.join("leftover.bin"), b"leftover")
            .await
            .unwrap();

        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                job_id,
                "History Cleanup",
                "failed",
                output_dir.clone(),
            ))
            .unwrap();

        let (reply, recv) = oneshot::channel();
        pipeline
            .handle_command(SchedulerCommand::DeleteHistory {
                job_id,
                delete_files: false,
                reply,
            })
            .await;
        tokio::time::timeout(Duration::from_secs(1), recv)
            .await
            .expect("delete history reply should arrive")
            .unwrap()
            .unwrap();

        assert!(!output_dir.exists());
        assert!(pipeline.db.get_job_history(job_id.0).unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_all_history_keeps_complete_output_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, intermediate_dir, complete_dir) = new_direct_pipeline(&temp_dir).await;
        let failed_job_id = JobId(30021);
        let complete_job_id = JobId(30022);
        let failed_output_dir = intermediate_dir.join("failed-history-job");
        let complete_output_dir = complete_dir.join("complete-history-job");

        tokio::fs::create_dir_all(&failed_output_dir).await.unwrap();
        tokio::fs::create_dir_all(&complete_output_dir)
            .await
            .unwrap();
        tokio::fs::write(failed_output_dir.join("partial.mkv"), b"partial")
            .await
            .unwrap();
        tokio::fs::write(complete_output_dir.join("episode.mkv"), b"complete")
            .await
            .unwrap();

        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                failed_job_id,
                "Failed History Cleanup",
                "failed",
                failed_output_dir.clone(),
            ))
            .unwrap();
        pipeline
            .db
            .insert_job_history(&history_row_with_output_dir(
                complete_job_id,
                "Complete History Cleanup",
                "complete",
                complete_output_dir.clone(),
            ))
            .unwrap();

        let (reply, recv) = oneshot::channel();
        pipeline
            .handle_command(SchedulerCommand::DeleteAllHistory {
                delete_files: false,
                reply,
            })
            .await;
        tokio::time::timeout(Duration::from_secs(1), recv)
            .await
            .expect("delete all history reply should arrive")
            .unwrap()
            .unwrap();

        assert!(!failed_output_dir.exists());
        assert!(complete_output_dir.exists());
        assert!(
            pipeline
                .db
                .list_job_history(&weaver_state::HistoryFilter::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn hdd_profile_allocates_more_write_backlog_than_ssd() {
        let profile = SystemProfile {
            cpu: CpuProfile {
                physical_cores: 4,
                logical_cores: 4,
                simd: SimdSupport::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 8 * 1024 * 1024 * 1024,
                available_bytes: 8 * 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Ssd,
                filesystem: FilesystemType::Apfs,
                sequential_write_mbps: 1000.0,
                random_read_iops: 50_000.0,
                same_filesystem: true,
            },
        };
        let mut hdd_profile = profile.clone();
        hdd_profile.disk.storage_class = StorageClass::Hdd;
        let buffers = BufferPool::new(BufferPoolConfig {
            small_count: 64,
            medium_count: 8,
            large_count: 2,
        });

        let ssd_budget = compute_write_backlog_budget_bytes(&profile, &buffers);
        let hdd_budget = compute_write_backlog_budget_bytes(&hdd_profile, &buffers);

        assert!(hdd_budget > ssd_budget);
    }

    #[tokio::test]
    async fn fail_job_clears_write_backlog_accounting() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(20005);
        let spec =
            standalone_job_spec("Fail Clears Backlog", &[("stalled.bin".to_string(), 64u32)]);
        insert_active_job(&mut pipeline, job_id, spec).await;

        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let buffered = BufferedDecodedSegment {
            segment_id: SegmentId {
                file_id,
                segment_number: 0,
            },
            decoded_size: 4096,
            crc32: 0,
            data: DecodedChunk::from(vec![3u8; 4096]),
            yenc_name: "stalled.bin".to_string(),
        };
        let buffered_len = buffered.len_bytes();
        pipeline
            .write_buffers
            .entry(file_id)
            .or_insert_with(|| WriteReorderBuffer::new(4))
            .insert(8192, buffered);
        pipeline.note_write_buffered(buffered_len, 1);

        pipeline.fail_job(job_id, "forced failure".to_string());

        assert!(!pipeline.write_buffers.contains_key(&file_id));
        assert_eq!(pipeline.write_buffered_bytes, 0);
        assert_eq!(pipeline.write_buffered_segments, 0);
        assert_eq!(
            pipeline
                .metrics
                .write_buffered_bytes
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            pipeline
                .metrics
                .write_buffered_segments
                .load(Ordering::Relaxed),
            0
        );
    }

    #[tokio::test]
    async fn quiescent_tail_flush_completes_data_file_with_only_recovery_left() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(20006);
        let spec = JobSpec {
            name: "Tail Flush".to_string(),
            password: None,
            total_bytes: 112,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: "episode.bin".to_string(),
                    role: FileRole::Standalone,
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "data@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: "repair.par2".to_string(),
                    role: FileRole::Par2 {
                        is_index: true,
                        recovery_block_count: 0,
                    },
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: 16,
                        message_id: "index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: "repair.vol00+01.par2".to_string(),
                    role: FileRole::Par2 {
                        is_index: false,
                        recovery_block_count: 1,
                    },
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: 32,
                        message_id: "repair@example.com".to_string(),
                    }],
                },
            ],
        };
        insert_active_job(&mut pipeline, job_id, spec).await;

        let file_id = NzbFileId {
            job_id,
            file_index: 0,
        };
        let buffered = BufferedDecodedSegment {
            segment_id: SegmentId {
                file_id,
                segment_number: 0,
            },
            decoded_size: 64,
            crc32: 0,
            data: DecodedChunk::from(vec![9u8; 64]),
            yenc_name: "episode.bin".to_string(),
        };
        let buffered_len = buffered.len_bytes();
        pipeline
            .write_buffers
            .entry(file_id)
            .or_insert_with(|| WriteReorderBuffer::new(4))
            .insert(0, buffered);
        pipeline.note_write_buffered(buffered_len, 1);

        let state = pipeline.jobs.get_mut(&job_id).unwrap();
        state
            .assembly
            .file_mut(NzbFileId {
                job_id,
                file_index: 1,
            })
            .unwrap()
            .commit_segment(0, 16)
            .unwrap();
        state.download_queue = DownloadQueue::new();
        assert_eq!(state.recovery_queue.len(), 1);

        pipeline.flush_quiescent_write_backlog().await;

        assert_eq!(pipeline.write_buffered_bytes, 0);
        assert_eq!(pipeline.write_buffered_segments, 0);
        assert!(matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ));
    }

    #[tokio::test]
    async fn out_of_order_rar_completion_keeps_pending_continuation_until_start_arrives() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30001);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Snapshot Topology", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;

        assert_eq!(member_span(&pipeline, job_id, "show", "E02.mkv"), None);
        assert_eq!(
            unresolved_spans(&pipeline, job_id, "show"),
            vec![(1, 1), (3, 3)]
        );
        let fact_volumes: Vec<u32> = pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .unwrap()
            .iter()
            .map(|(volume, _)| *volume)
            .collect();
        assert_eq!(fact_volumes, vec![0, 1, 3]);

        write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;

        assert_eq!(
            member_span(&pipeline, job_id, "show", "E01.mkv"),
            Some((0, 1))
        );
        assert_eq!(
            member_span(&pipeline, job_id, "show", "E02.mkv"),
            Some((2, 3))
        );
        assert!(unresolved_spans(&pipeline, job_id, "show").is_empty());
        let fact_volumes: Vec<u32> = pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .unwrap()
            .iter()
            .map(|(volume, _)| *volume)
            .collect();
        assert_eq!(fact_volumes, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn eager_delete_preserves_later_member_volumes_after_out_of_order_completion() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30002);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Eager Delete", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in [
            (0usize, &files[0]),
            (1, &files[1]),
            (3, &files[3]),
            (2, &files[2]),
        ] {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "show")
            .await
            .unwrap();

        pipeline.try_delete_volumes(job_id, "show");

        let deletion_eligible = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_ref())
            .map(|plan| plan.deletion_eligible.clone())
            .unwrap();
        assert!(deletion_eligible.contains(&0));
        assert!(deletion_eligible.contains(&1));
        assert!(!deletion_eligible.contains(&2));
        assert!(!deletion_eligible.contains(&3));
        assert!(!working_dir.join("show.part01.rar").exists());
        assert!(!working_dir.join("show.part02.rar").exists());
        assert!(working_dir.join("show.part03.rar").exists());
        assert!(working_dir.join("show.part04.rar").exists());
        // Yield to let fire-and-forget db writes complete.
        tokio::task::yield_now().await;
        let deleted_rows = pipeline.db.load_deleted_volume_statuses(job_id).unwrap();
        assert_eq!(
            deleted_rows,
            vec![("show".to_string(), 0), ("show".to_string(), 1)]
        );
    }

    #[tokio::test]
    async fn restore_job_reuses_persisted_rar_volume_facts_after_restart() {
        let temp_dir = tempfile::tempdir().unwrap();
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Snapshot Restore", &files);
        let job_id = JobId(30003);
        let working_dir = {
            let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
            let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

            for (file_index, (filename, bytes)) in [
                (0usize, &files[0]),
                (1, &files[1]),
                (3, &files[3]),
                (2, &files[2]),
            ] {
                write_and_complete_rar_volume(
                    &mut pipeline,
                    job_id,
                    file_index as u32,
                    filename,
                    bytes,
                )
                .await;
            }

            pipeline
                .extracted_members
                .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
            pipeline
                .db
                .add_extracted_member(job_id, "E01.mkv", &working_dir.join("E01.mkv"))
                .unwrap();
            pipeline.try_delete_volumes(job_id, "show");
            working_dir
        };

        let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
        let committed_segments = Pipeline::all_segment_ids(job_id, &spec);
        restored
            .restore_job(
                job_id,
                spec,
                committed_segments,
                ["E01.mkv".to_string()].into_iter().collect(),
                JobStatus::Downloading,
                working_dir.clone(),
            )
            .await
            .unwrap();

        assert_eq!(
            member_span(&restored, job_id, "show", "E01.mkv"),
            Some((0, 1))
        );
        assert_eq!(
            member_span(&restored, job_id, "show", "E02.mkv"),
            Some((2, 3))
        );
        assert_eq!(
            restored
                .db
                .load_all_rar_volume_facts(job_id)
                .unwrap()
                .get("show")
                .map(|facts| facts.len()),
            Some(4)
        );
        assert!(!working_dir.join("show.part01.rar").exists());
        assert!(!working_dir.join("show.part02.rar").exists());
        assert!(working_dir.join("show.part03.rar").exists());
        assert!(working_dir.join("show.part04.rar").exists());
    }

    #[tokio::test]
    async fn record_job_history_purges_terminal_job_runtime_and_queue_metrics() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("Terminal Runtime Cleanup", &files);
        let job_id = JobId(30033);
        insert_active_job(&mut pipeline, job_id, spec).await;

        pipeline.update_queue_metrics();
        assert!(
            pipeline
                .metrics
                .download_queue_depth
                .load(Ordering::Relaxed)
                > 0
        );

        pipeline.jobs.get_mut(&job_id).unwrap().status = JobStatus::Complete;
        pipeline.record_job_history(job_id);

        assert!(!pipeline.jobs.contains_key(&job_id));
        assert_eq!(
            pipeline
                .metrics
                .download_queue_depth
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            pipeline
                .metrics
                .recovery_queue_depth
                .load(Ordering::Relaxed),
            0
        );
        assert!(
            pipeline
                .finished_jobs
                .iter()
                .any(|job| job.job_id == job_id)
        );
    }

    #[tokio::test]
    async fn swapped_rar_volume_arrival_uses_parsed_volume_identity_for_claims() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30032);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Swapped Live Mapping", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 1, &files[1].0, &files[1].1).await;

        // `show.part04.rar` arrives first but actually contains logical volume 2.
        write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[2].1).await;

        let key = (job_id, "show".to_string());
        let plan = pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .expect("RAR plan should exist after swapped volume arrival");
        assert!(
            plan.delete_decisions
                .values()
                .all(|decision| !decision.owners.is_empty())
        );
        assert!(plan.waiting_on_volumes.contains(&3));
        assert_eq!(
            Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
            Some("show.part04.rar")
        );

        // The counterpart arrives under the opposite filename and should complete the mapping.
        write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[3].1).await;

        let plan = pipeline
            .rar_sets
            .get(&key)
            .and_then(|state| state.plan.as_ref())
            .expect("RAR plan should exist after swapped pair arrival");
        assert!(
            plan.delete_decisions
                .values()
                .all(|decision| !decision.owners.is_empty())
        );
        assert_eq!(
            Pipeline::rar_volume_filename(&plan.topology.volume_map, 2),
            Some("show.part04.rar")
        );
        assert_eq!(
            Pipeline::rar_volume_filename(&plan.topology.volume_map, 3),
            Some("show.part03.rar")
        );
        assert_eq!(
            member_span(&pipeline, job_id, "show", "E02.mkv"),
            Some((2, 3))
        );
    }

    #[tokio::test]
    async fn restore_job_reloads_par2_metadata_from_disk_after_restart() {
        let temp_dir = tempfile::tempdir().unwrap();
        let par2_filename = "repair.par2";
        let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
        let spec = par2_only_job_spec("PAR2 Restore", par2_filename, par2_bytes.len() as u32);
        let job_id = JobId(30030);
        let working_dir = {
            let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
            let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
            tokio::fs::write(working_dir.join(par2_filename), &par2_bytes)
                .await
                .unwrap();
            working_dir
        };

        let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
        restored
            .restore_job(
                job_id,
                spec,
                Pipeline::all_segment_ids(
                    job_id,
                    &JobSpec {
                        name: "PAR2 Restore".to_string(),
                        password: None,
                        total_bytes: par2_bytes.len() as u64,
                        category: None,
                        metadata: vec![],
                        files: vec![FileSpec {
                            filename: par2_filename.to_string(),
                            role: FileRole::from_filename(par2_filename),
                            groups: vec!["alt.binaries.test".to_string()],
                            segments: vec![SegmentSpec {
                                number: 0,
                                bytes: par2_bytes.len() as u32,
                                message_id: "par2-0@example.com".to_string(),
                            }],
                        }],
                    },
                ),
                HashSet::new(),
                JobStatus::Downloading,
                working_dir,
            )
            .await
            .unwrap();

        assert!(restored.par2_set(job_id).is_some());
        let par2_set = restored.par2_set(job_id).unwrap();
        assert_eq!(par2_set.files.len(), 1);
        assert_eq!(par2_set.recovery_block_count(), 0);
    }

    #[tokio::test]
    async fn restore_job_reapplies_only_promoted_recovery_segments() {
        let temp_dir = tempfile::tempdir().unwrap();
        let index_filename = "repair.par2";
        let recovery_filename = "repair.vol00+01.par2";
        let par2_bytes = build_test_par2_index("payload.bin", b"payload-data", 8);
        let spec = JobSpec {
            name: "PAR2 Promote Restore".to_string(),
            password: None,
            total_bytes: par2_bytes.len() as u64 + 64,
            category: None,
            metadata: vec![],
            files: vec![
                FileSpec {
                    filename: index_filename.to_string(),
                    role: FileRole::from_filename(index_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: par2_bytes.len() as u32,
                        message_id: "par2-index@example.com".to_string(),
                    }],
                },
                FileSpec {
                    filename: recovery_filename.to_string(),
                    role: FileRole::from_filename(recovery_filename),
                    groups: vec!["alt.binaries.test".to_string()],
                    segments: vec![SegmentSpec {
                        number: 0,
                        bytes: 64,
                        message_id: "par2-recovery@example.com".to_string(),
                    }],
                },
            ],
        };
        let job_id = JobId(30032);
        let working_dir = {
            let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
            let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
            tokio::fs::write(working_dir.join(index_filename), &par2_bytes)
                .await
                .unwrap();
            pipeline
                .db
                .upsert_par2_file(job_id, 1, recovery_filename, 1, true)
                .unwrap();
            working_dir
        };

        let committed_segments = [SegmentId {
            file_id: NzbFileId {
                job_id,
                file_index: 0,
            },
            segment_number: 0,
        }]
        .into_iter()
        .collect();

        let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
        restored
            .restore_job(
                job_id,
                spec,
                committed_segments,
                HashSet::new(),
                JobStatus::Downloading,
                working_dir,
            )
            .await
            .unwrap();

        assert!(restored.par2_set(job_id).is_some());
        assert_eq!(
            restored
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&1))
                .map(|file| (file.recovery_blocks, file.promoted)),
            Some((1, true))
        );

        let state = restored.jobs.get_mut(&job_id).unwrap();
        let mut queued = state.download_queue.drain_all();
        queued.sort_by_key(|work| work.segment_id.file_id.file_index);
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].segment_id.file_id.file_index, 1);
        assert!(state.recovery_queue.is_empty());
    }

    #[tokio::test]
    async fn restore_job_rehydrates_failed_members_and_verified_suspect_state() {
        let temp_dir = tempfile::tempdir().unwrap();
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Restart Runtime Restore", &files);
        let job_id = JobId(30034);
        let working_dir = {
            let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
            let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;

            for (file_index, (filename, bytes)) in files.iter().enumerate() {
                write_and_complete_rar_volume(
                    &mut pipeline,
                    job_id,
                    file_index as u32,
                    filename,
                    bytes,
                )
                .await;
            }

            pipeline
                .db
                .add_failed_extraction(job_id, "E10.mkv")
                .unwrap();
            pipeline
                .db
                .add_failed_extraction(job_id, "E15.mkv")
                .unwrap();
            pipeline
                .db
                .set_active_job_normalization_retried(job_id, true)
                .unwrap();
            pipeline
                .db
                .replace_verified_suspect_volumes(
                    job_id,
                    "show",
                    &std::collections::HashSet::from([1u32, 2u32]),
                )
                .unwrap();
            working_dir
        };

        let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
        restored
            .restore_job(
                job_id,
                spec,
                Pipeline::all_segment_ids(
                    job_id,
                    &rar_job_spec("RAR Restart Runtime Restore", &files),
                ),
                HashSet::new(),
                JobStatus::Downloading,
                working_dir,
            )
            .await
            .unwrap();

        assert_eq!(
            restored.failed_extractions.get(&job_id).cloned(),
            Some(HashSet::from([
                "E10.mkv".to_string(),
                "E15.mkv".to_string(),
            ]))
        );
        assert!(restored.normalization_retried.contains(&job_id));
        assert_eq!(
            restored
                .rar_sets
                .get(&(job_id, "show".to_string()))
                .map(|state| state.verified_suspect_volumes.clone()),
            Some(HashSet::from([1u32, 2u32]))
        );
    }

    #[tokio::test]
    async fn restore_job_skips_eager_delete_for_ownerless_restored_volumes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let empty_rar = build_empty_rar_volume();
        let files = vec![("ownerless.part01.rar".to_string(), empty_rar)];
        let spec = rar_job_spec("RAR Ownerless Restore", &files);
        let job_id = JobId(30031);
        let working_dir = {
            let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
            let working_dir = insert_active_job(&mut pipeline, job_id, spec.clone()).await;
            write_and_complete_rar_volume(&mut pipeline, job_id, 0, &files[0].0, &files[0].1).await;
            working_dir
        };

        let (mut restored, _, _) = new_direct_pipeline(&temp_dir).await;
        restored
            .restore_job(
                job_id,
                spec,
                Pipeline::all_segment_ids(job_id, &rar_job_spec("RAR Ownerless Restore", &files)),
                HashSet::new(),
                JobStatus::Downloading,
                working_dir.clone(),
            )
            .await
            .unwrap();

        assert!(working_dir.join("ownerless.part01.rar").exists());
        assert!(
            !restored
                .eagerly_deleted
                .get(&job_id)
                .is_some_and(|deleted| deleted.contains("ownerless.part01.rar"))
        );
        let plan = restored
            .rar_sets
            .get(&(job_id, "ownerless".to_string()))
            .and_then(|state| state.plan.as_ref())
            .expect("ownerless RAR restore should produce a plan");
        let decision = plan
            .delete_decisions
            .get(&0)
            .expect("ownerless restore should keep volume 0 audited");
        assert!(decision.owners.is_empty());
        assert!(!decision.ownership_eligible);
    }

    #[tokio::test]
    async fn normalization_refresh_rebuilds_rar_snapshot_from_disk() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30004);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Normalize Refresh", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in [
            (0usize, &files[0]),
            (1, &files[1]),
            (3, &files[3]),
            (2, &files[2]),
        ] {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        tokio::fs::write(working_dir.join("show.part03.rar"), &files[2].1)
            .await
            .unwrap();
        pipeline
            .refresh_rar_topology_after_normalization(
                job_id,
                &["show.part03.rar".to_string()].into_iter().collect(),
            )
            .await
            .unwrap();

        assert_eq!(
            member_span(&pipeline, job_id, "show", "E02.mkv"),
            Some((2, 3))
        );
        assert!(
            pipeline
                .db
                .load_all_rar_volume_facts(job_id)
                .unwrap()
                .contains_key("show")
        );
    }

    #[tokio::test]
    async fn live_rebuild_failure_retains_previous_rar_volume_facts_and_topology() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30005);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Rebuild Failure", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        let original_facts = pipeline
            .db
            .load_all_rar_volume_facts(job_id)
            .unwrap()
            .get("show")
            .cloned()
            .expect("good facts should be persisted");

        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline.try_delete_volumes(job_id, "show");

        assert!(working_dir.join("show.part01.rar").exists());
        assert!(working_dir.join("show.part02.rar").exists());

        let corrupt_part04 = vec![0u8; files[3].1.len()];
        tokio::fs::write(working_dir.join(&files[3].0), &corrupt_part04)
            .await
            .unwrap();

        pipeline
            .try_update_archive_topology(
                job_id,
                NzbFileId {
                    job_id,
                    file_index: 3,
                },
            )
            .await;

        assert_eq!(
            pipeline
                .db
                .load_all_rar_volume_facts(job_id)
                .unwrap()
                .get("show")
                .cloned(),
            Some(original_facts)
        );
        assert_eq!(
            member_span(&pipeline, job_id, "show", "E01.mkv"),
            Some((0, 1))
        );
        assert_eq!(
            member_span(&pipeline, job_id, "show", "E02.mkv"),
            Some((2, 3))
        );
    }

    #[tokio::test]
    async fn incremental_rar_batches_survive_eager_delete_of_earlier_volumes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30006);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Incremental Batches", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline.try_partial_extraction(job_id).await;
        let first_done = next_extraction_done(&mut pipeline).await;
        match &first_done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            _ => panic!("expected batch extraction completion"),
        }
        pipeline.handle_extraction_done(first_done).await;

        assert!(!working_dir.join("show.part01.rar").exists());
        assert!(!working_dir.join("show.part02.rar").exists());

        write_and_complete_rar_volume(&mut pipeline, job_id, 2, &files[2].0, &files[2].1).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 3, &files[3].0, &files[3].1).await;
        pipeline.try_partial_extraction(job_id).await;

        let second_done = next_extraction_done(&mut pipeline).await;
        match &second_done {
            ExtractionDone::Batch {
                job_id: done_job_id,
                attempted,
                result,
                ..
            } => {
                assert_eq!(*done_job_id, job_id);
                assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            _ => panic!("expected batch extraction completion"),
        }
        pipeline.handle_extraction_done(second_done).await;

        assert!(matches!(
            job_status_for_assert(&pipeline, job_id),
            Some(JobStatus::Complete)
        ));
    }

    #[tokio::test]
    async fn non_solid_incremental_rar_batches_cleanup_chunks_after_finalize() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _complete_dir) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30007);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Incremental Chunks", &files);
        let _working_dir = insert_active_job(&mut pipeline, job_id, spec).await;
        let staging_dir = pipeline.extraction_staging_dir(job_id);

        for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline.try_partial_extraction(job_id).await;
        let done = next_extraction_done(&mut pipeline).await;
        match &done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            _ => panic!("expected batch extraction completion"),
        }
        pipeline.handle_extraction_done(done).await;

        let chunks = pipeline.db.get_extraction_chunks(job_id, "show").unwrap();
        assert!(chunks.iter().all(|chunk| chunk.member_name != "E01.mkv"));
        assert!(staging_dir.join("E01.mkv").exists());
        assert!(
            !staging_dir
                .join(".weaver-chunks")
                .join("show")
                .join("E01.mkv")
                .exists()
        );
    }

    #[tokio::test]
    async fn non_solid_rar_set_dispatches_two_members_concurrently() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.tuner = RuntimeTuner::with_connection_limit(
            weaver_core::system::SystemProfile {
                cpu: weaver_core::system::CpuProfile {
                    physical_cores: 8,
                    logical_cores: 8,
                    simd: weaver_core::system::SimdSupport::default(),
                    cgroup_limit: None,
                },
                memory: weaver_core::system::MemoryProfile {
                    total_bytes: 8 * 1024 * 1024 * 1024,
                    available_bytes: 8 * 1024 * 1024 * 1024,
                    cgroup_limit: None,
                },
                disk: weaver_core::system::DiskProfile {
                    storage_class: weaver_core::system::StorageClass::Ssd,
                    filesystem: weaver_core::system::FilesystemType::Apfs,
                    sequential_write_mbps: 2000.0,
                    random_read_iops: 50_000.0,
                    same_filesystem: true,
                },
            },
            4,
        );

        let job_id = JobId(30008);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Concurrent Members", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline.try_partial_extraction(job_id).await;

        let set_state = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .expect("set state should exist");
        assert_eq!(set_state.active_workers, 2);
        assert_eq!(
            set_state.in_flight_members,
            ["E01.mkv".to_string(), "E02.mkv".to_string()]
                .into_iter()
                .collect()
        );

        let first_done = next_extraction_done(&mut pipeline).await;
        let second_done = next_extraction_done(&mut pipeline).await;
        let mut attempted_members = Vec::new();
        for done in [&first_done, &second_done] {
            match done {
                ExtractionDone::Batch {
                    attempted, result, ..
                } => {
                    assert_eq!(attempted.len(), 1);
                    assert!(
                        result
                            .as_ref()
                            .is_ok_and(|outcome| outcome.failed.is_empty())
                    );
                    attempted_members.push(attempted[0].clone());
                }
                _ => panic!("expected batch extraction completion"),
            }
        }
        attempted_members.sort();
        assert_eq!(
            attempted_members,
            vec!["E01.mkv".to_string(), "E02.mkv".to_string()]
        );

        pipeline.handle_extraction_done(first_done).await;
        pipeline.handle_extraction_done(second_done).await;
    }

    #[tokio::test]
    async fn non_solid_rar_scheduler_skips_duplicate_ready_members() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        pipeline.tuner = RuntimeTuner::with_connection_limit(
            weaver_core::system::SystemProfile {
                cpu: weaver_core::system::CpuProfile {
                    physical_cores: 8,
                    logical_cores: 8,
                    simd: weaver_core::system::SimdSupport::default(),
                    cgroup_limit: None,
                },
                memory: weaver_core::system::MemoryProfile {
                    total_bytes: 8 * 1024 * 1024 * 1024,
                    available_bytes: 8 * 1024 * 1024 * 1024,
                    cgroup_limit: None,
                },
                disk: weaver_core::system::DiskProfile {
                    storage_class: weaver_core::system::StorageClass::Ssd,
                    filesystem: weaver_core::system::FilesystemType::Apfs,
                    sequential_write_mbps: 2000.0,
                    random_read_iops: 50_000.0,
                    same_filesystem: true,
                },
            },
            4,
        );

        let job_id = JobId(30010);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Duplicate Ready Members", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("set state should exist");
        let plan = set_state
            .plan
            .as_mut()
            .expect("ready plan should exist after all volumes complete");
        let first = plan.ready_members[0].name.clone();
        let second = plan.ready_members[1].name.clone();
        plan.ready_members = vec![
            crate::pipeline::rar_state::RarReadyMember {
                name: first.clone(),
            },
            crate::pipeline::rar_state::RarReadyMember {
                name: first.clone(),
            },
            crate::pipeline::rar_state::RarReadyMember {
                name: second.clone(),
            },
        ];

        pipeline.try_partial_extraction(job_id).await;

        let set_state = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .expect("set state should exist");
        assert_eq!(set_state.active_workers, 2);
        assert_eq!(
            set_state.in_flight_members,
            [first.clone(), second.clone()].into_iter().collect()
        );

        let first_done = next_extraction_done(&mut pipeline).await;
        let second_done = next_extraction_done(&mut pipeline).await;
        let mut attempted_members = Vec::new();
        for done in [&first_done, &second_done] {
            match done {
                ExtractionDone::Batch {
                    attempted, result, ..
                } => {
                    assert_eq!(attempted.len(), 1);
                    assert!(
                        result
                            .as_ref()
                            .is_ok_and(|outcome| outcome.failed.is_empty())
                    );
                    attempted_members.push(attempted[0].clone());
                }
                _ => panic!("expected batch extraction completion"),
            }
        }
        attempted_members.sort();
        assert_eq!(attempted_members, vec![first, second]);

        pipeline.handle_extraction_done(first_done).await;
        pipeline.handle_extraction_done(second_done).await;
    }

    #[tokio::test]
    async fn solid_rar_keeps_later_members_ready_after_earlier_failure() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30009);
        let fixture_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../weaver-rar/tests/fixtures/rar5/rar5_solid.rar");
        let fixture_bytes = tokio::fs::read(&fixture_path).await.unwrap();
        let archive =
            weaver_rar::RarArchive::open(std::fs::File::open(&fixture_path).unwrap()).unwrap();
        let member_names = archive.member_names();
        assert_eq!(member_names.len(), 4);

        let spec = rar_job_spec(
            "Solid Failure Continuation",
            &[("solid.rar".to_string(), fixture_bytes.clone())],
        );
        insert_active_job(&mut pipeline, job_id, spec).await;
        write_and_complete_rar_volume(&mut pipeline, job_id, 0, "solid.rar", &fixture_bytes).await;

        // Rebuild with extracted + failed state that mirrors a solid archive
        // where earlier members were attempted before later members.
        pipeline
            .extracted_members
            .insert(job_id, [member_names[0].to_string()].into_iter().collect());
        pipeline
            .failed_extractions
            .insert(job_id, [member_names[1].to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "solid")
            .await
            .unwrap();

        let plan = pipeline
            .rar_sets
            .get(&(job_id, "solid".to_string()))
            .and_then(|state| state.plan.as_ref())
            .cloned()
            .expect("solid set plan should exist");

        assert!(plan.is_solid);
        assert_eq!(plan.phase, crate::pipeline::rar_state::RarSetPhase::Ready);
        let ready_members: Vec<String> = plan
            .ready_members
            .into_iter()
            .map(|member| member.name)
            .collect();
        assert_eq!(
            ready_members,
            member_names[2..]
                .iter()
                .map(|member| member.to_string())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn check_job_completion_retains_par2_until_rar_extraction_finishes() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30011);
        let mut files = build_multifile_multivolume_rar_set();
        files.push((
            "repair.vol00+01.par2".to_string(),
            b"retained-par2-placeholder".to_vec(),
        ));
        let spec = rar_job_spec("RAR Keeps PAR2 During Extraction", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().take(4).enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        let par2_filename = &files[4].0;
        tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
            .await
            .unwrap();
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            minimal_par2_file_set(),
            &[(4, par2_filename, 1, true)],
        );

        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist after volume facts are built");
        set_state.active_workers = 1;
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        if let Some(plan) = set_state.plan.as_mut() {
            plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        }

        pipeline.check_job_completion(job_id).await;

        assert_eq!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting)
        );
        assert!(working_dir.join(par2_filename).exists());
        assert!(pipeline.par2_set(job_id).is_some());
        assert_eq!(
            pipeline
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&4))
                .map(|file| file.recovery_blocks),
            Some(1)
        );
        assert_eq!(
            pipeline
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&4))
                .map(|file| file.promoted),
            Some(true)
        );
    }

    #[tokio::test]
    async fn check_job_completion_defers_verify_while_rar_workers_are_active() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30015);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Verify Barrier", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
        pipeline
            .failed_extractions
            .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());

        let set_state = pipeline
            .rar_sets
            .get_mut(&(job_id, "show".to_string()))
            .expect("RAR set state should exist");
        set_state.active_workers = 1;
        set_state.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        if let Some(plan) = set_state.plan.as_mut() {
            plan.phase = crate::pipeline::rar_state::RarSetPhase::Extracting;
        }

        pipeline.check_job_completion(job_id).await;

        assert_eq!(
            pipeline.jobs.get(&job_id).map(|state| state.status.clone()),
            Some(JobStatus::Extracting)
        );
        assert_eq!(
            pipeline
                .metrics
                .verify_active
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert!(!pipeline.normalization_retried.contains(&job_id));
    }

    #[tokio::test]
    async fn partial_rar_extraction_does_not_bypass_par2_early() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30012);
        let mut files = build_multifile_multivolume_rar_set();
        files.push((
            "repair.vol00+01.par2".to_string(),
            b"retained-par2-placeholder".to_vec(),
        ));
        let spec = rar_job_spec("RAR Partial Keeps PAR2", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().take(2).enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        let par2_filename = &files[4].0;
        tokio::fs::write(working_dir.join(par2_filename), &files[4].1)
            .await
            .unwrap();
        install_test_par2_runtime(
            &mut pipeline,
            job_id,
            minimal_par2_file_set(),
            &[(4, par2_filename, 1, true)],
        );

        pipeline.try_partial_extraction(job_id).await;
        let done = next_extraction_done(&mut pipeline).await;
        match &done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted, &vec!["E01.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            _ => panic!("expected batch extraction completion"),
        }
        pipeline.handle_extraction_done(done).await;

        assert!(!pipeline.par2_bypassed.contains(&job_id));
        assert!(working_dir.join(par2_filename).exists());
        assert!(pipeline.par2_set(job_id).is_some());
        assert_eq!(
            pipeline
                .par2_runtime(job_id)
                .and_then(|runtime| runtime.files.get(&4))
                .map(|file| file.promoted),
            Some(true)
        );
    }

    #[tokio::test]
    async fn rar_completion_prefers_incremental_batches_over_full_set_after_eager_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30013);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Completion Uses Batch", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "show")
            .await
            .unwrap();
        pipeline.try_delete_volumes(job_id, "show");

        assert!(!working_dir.join("show.part01.rar").exists());
        assert!(!working_dir.join("show.part02.rar").exists());

        pipeline.check_job_completion(job_id).await;

        let done = next_extraction_done(&mut pipeline).await;
        match &done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            ExtractionDone::FullSet { .. } => {
                panic!("RAR completion should not fall back to full-set extraction here")
            }
        }
        pipeline.handle_extraction_done(done).await;
    }

    #[tokio::test]
    async fn eager_delete_exclusions_do_not_hide_suspect_deleted_rar_damage() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30014);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Verify Keeps Suspect Deleted Volumes", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        pipeline.eagerly_deleted.insert(
            job_id,
            ["show.part02.rar".to_string(), "show.part04.rar".to_string()]
                .into_iter()
                .collect(),
        );
        pipeline
            .extracted_members
            .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
        pipeline
            .failed_extractions
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "show")
            .await
            .unwrap();

        let mut verification = weaver_par2::VerificationResult {
            files: vec![
                weaver_par2::verify::FileVerification {
                    file_id: weaver_par2::FileId::from_bytes([1; 16]),
                    filename: "show.part02.rar".to_string(),
                    status: weaver_par2::verify::FileStatus::Missing,
                    valid_slices: vec![false; 3],
                    missing_slice_count: 3,
                },
                weaver_par2::verify::FileVerification {
                    file_id: weaver_par2::FileId::from_bytes([2; 16]),
                    filename: "show.part04.rar".to_string(),
                    status: weaver_par2::verify::FileStatus::Missing,
                    valid_slices: vec![false; 2],
                    missing_slice_count: 2,
                },
            ],
            recovery_blocks_available: 3,
            total_missing_blocks: 5,
            repairable: weaver_par2::verify::Repairability::Insufficient {
                blocks_needed: 5,
                blocks_available: 3,
                deficit: 2,
            },
        };

        let (skipped_blocks, retained_suspect_blocks) =
            pipeline.apply_eager_delete_exclusions(job_id, &mut verification);

        assert_eq!(skipped_blocks, 2);
        assert_eq!(retained_suspect_blocks, 3);
        assert_eq!(verification.total_missing_blocks, 3);
        assert!(matches!(
            verification.files[0].status,
            weaver_par2::verify::FileStatus::Missing
        ));
        assert_eq!(verification.files[0].missing_slice_count, 3);
        assert!(matches!(
            verification.files[1].status,
            weaver_par2::verify::FileStatus::Complete
        ));
        assert_eq!(verification.files[1].missing_slice_count, 0);
        assert!(matches!(
            verification.repairable,
            weaver_par2::verify::Repairability::Repairable {
                blocks_needed: 3,
                blocks_available: 3
            }
        ));

        pipeline.recompute_volume_safety_from_verification(job_id, &verification);

        let verified_suspect = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .map(|state| state.verified_suspect_volumes.clone())
            .unwrap_or_default();
        assert!(verified_suspect.contains(&1));
        assert!(!verified_suspect.contains(&3));
    }

    #[tokio::test]
    async fn clean_member_keeps_failed_neighbor_boundary_volume_suspect() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30015);
        let files = vec![
            ("show.part01.rar".to_string(), vec![1u8]),
            ("show.part02.rar".to_string(), vec![2u8]),
            ("show.part03.rar".to_string(), vec![3u8]),
        ];
        let spec = rar_job_spec("RAR Boundary Suspect Claims", &files);
        insert_active_job(&mut pipeline, job_id, spec).await;

        let topology = weaver_assembly::ArchiveTopology {
            archive_type: weaver_assembly::ArchiveType::Rar,
            volume_map: std::collections::HashMap::from([
                ("show.part01.rar".to_string(), 0),
                ("show.part02.rar".to_string(), 1),
                ("show.part03.rar".to_string(), 2),
            ]),
            complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
            expected_volume_count: Some(3),
            members: vec![
                weaver_assembly::ArchiveMember {
                    name: "E10.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 1,
                    unpacked_size: 0,
                },
                weaver_assembly::ArchiveMember {
                    name: "E11.mkv".to_string(),
                    first_volume: 1,
                    last_volume: 2,
                    unpacked_size: 0,
                },
            ],
            unresolved_spans: Vec::new(),
        };
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .assembly
            .set_archive_topology("show".to_string(), topology.clone());

        pipeline.rar_sets.insert(
            (job_id, "show".to_string()),
            rar_state::RarSetState {
                facts: std::collections::BTreeMap::from([
                    (0u32, dummy_rar_volume_facts(0)),
                    (1u32, dummy_rar_volume_facts(1)),
                    (2u32, dummy_rar_volume_facts(2)),
                ]),
                volume_files: std::collections::BTreeMap::new(),
                cached_headers: None,
                verified_suspect_volumes: std::collections::HashSet::from([1u32]),
                active_workers: 0,
                in_flight_members: std::collections::HashSet::new(),
                phase: rar_state::RarSetPhase::Ready,
                plan: Some(rar_state::RarDerivedPlan {
                    phase: rar_state::RarSetPhase::Ready,
                    is_solid: false,
                    ready_members: Vec::new(),
                    member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                    waiting_on_volumes: std::collections::HashSet::new(),
                    deletion_eligible: std::collections::HashSet::new(),
                    delete_decisions: std::collections::BTreeMap::from([(
                        1u32,
                        rar_state::RarVolumeDeleteDecision {
                            owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                            clean_owners: vec!["E11.mkv".to_string()],
                            failed_owners: vec!["E10.mkv".to_string()],
                            pending_owners: Vec::new(),
                            unresolved_boundary: false,
                            ownership_eligible: false,
                        },
                    )]),
                    topology,
                    fallback_reason: None,
                }),
            },
        );

        let suspect = pipeline.suspect_rar_volumes_for_job(job_id);
        let decision = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_ref())
            .and_then(|plan| plan.delete_decisions.get(&1))
            .unwrap();

        assert!(suspect.contains(&1));
        assert!(!Pipeline::claim_clean_rar_volume(decision));
    }

    #[tokio::test]
    async fn normalization_refresh_preserves_deleted_untouched_rar_facts() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30016);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Normalization Keeps Facts", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        tokio::fs::remove_file(working_dir.join("show.part01.rar"))
            .await
            .unwrap();
        tokio::fs::remove_file(working_dir.join("show.part02.rar"))
            .await
            .unwrap();

        pipeline
            .refresh_rar_topology_after_normalization(
                job_id,
                &["show.part03.rar".to_string(), "show.part04.rar".to_string()]
                    .into_iter()
                    .collect(),
            )
            .await
            .unwrap();

        let facts: Vec<u32> = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .expect("RAR set should exist")
            .facts
            .keys()
            .copied()
            .collect();
        assert_eq!(facts, vec![0, 1, 2, 3]);
        assert_eq!(
            pipeline
                .db
                .load_all_rar_volume_facts(job_id)
                .unwrap()
                .get("show")
                .map(|rows| rows.len()),
            Some(4)
        );
    }

    #[tokio::test]
    async fn clean_verify_after_swap_correction_preserves_retry_frontier_after_eager_delete() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30017);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Swap Retry Frontier", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        let part03 = working_dir.join("show.part03.rar");
        let part04 = working_dir.join("show.part04.rar");
        let swap_tmp = working_dir.join("show.swap.tmp");
        tokio::fs::rename(&part03, &swap_tmp).await.unwrap();
        tokio::fs::rename(&part04, &part03).await.unwrap();
        tokio::fs::rename(&swap_tmp, &part04).await.unwrap();

        tokio::fs::remove_file(working_dir.join("show.part01.rar"))
            .await
            .unwrap();
        tokio::fs::remove_file(working_dir.join("show.part02.rar"))
            .await
            .unwrap();

        pipeline.eagerly_deleted.insert(
            job_id,
            ["show.part01.rar".to_string(), "show.part02.rar".to_string()]
                .into_iter()
                .collect(),
        );
        install_test_par2_runtime(&mut pipeline, job_id, placement_par2_file_set(&files), &[]);
        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .failed_extractions
            .insert(job_id, ["E02.mkv".to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "show")
            .await
            .unwrap();

        pipeline.check_job_completion(job_id).await;

        let plan = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_ref())
            .cloned()
            .expect("RAR plan should still exist after normalization retry");
        assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_none());
        assert!(!plan.waiting_on_volumes.contains(&0));
        assert!(plan.waiting_on_volumes.is_disjoint(&plan.deletion_eligible));
        assert!(
            plan.ready_members
                .iter()
                .any(|member| member.name == "E02.mkv")
        );
        assert_eq!(
            plan.delete_decisions
                .get(&2)
                .expect("volume 2 decision should exist")
                .owners,
            vec!["E02.mkv".to_string()]
        );

        let done = next_extraction_done(&mut pipeline).await;
        match &done {
            ExtractionDone::Batch {
                attempted, result, ..
            } => {
                assert_eq!(attempted, &vec!["E02.mkv".to_string()]);
                assert!(
                    result
                        .as_ref()
                        .is_ok_and(|outcome| outcome.failed.is_empty())
                );
            }
            _ => panic!("expected incremental retry batch"),
        }
    }

    #[tokio::test]
    async fn rar_retry_frontier_rejects_waiting_on_deleted_volume() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30018);
        let files = build_multifile_multivolume_rar_set();
        let spec = rar_job_spec("RAR Retry Frontier Rejects Deleted Waiting Volume", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (file_index, (filename, bytes)) in files.iter().enumerate() {
            write_and_complete_rar_volume(
                &mut pipeline,
                job_id,
                file_index as u32,
                filename,
                bytes,
            )
            .await;
        }

        tokio::fs::remove_file(working_dir.join("show.part04.rar"))
            .await
            .unwrap();
        pipeline.eagerly_deleted.insert(
            job_id,
            ["show.part04.rar".to_string()].into_iter().collect(),
        );
        pipeline
            .extracted_members
            .insert(job_id, ["E01.mkv".to_string()].into_iter().collect());
        pipeline
            .recompute_rar_set_state(job_id, "show")
            .await
            .unwrap();

        let plan = pipeline
            .rar_sets
            .get(&(job_id, "show".to_string()))
            .and_then(|state| state.plan.as_ref())
            .cloned()
            .expect("RAR plan should exist");
        assert!(plan.waiting_on_volumes.contains(&3));
        assert!(!plan.deletion_eligible.contains(&3));
        assert_eq!(
            pipeline.invalid_rar_retry_frontier_reason(job_id),
            Some("set 'show' waiting volumes already deleted: [3]".to_string())
        );
        assert!(pipeline.invalid_rar_retry_frontier_reason(job_id).is_some());
    }

    #[tokio::test]
    async fn eager_delete_retains_volume_with_failed_member_claim() {
        let temp_dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
        let job_id = JobId(30019);
        let files = vec![
            ("show.part01.rar".to_string(), vec![1u8]),
            ("show.part02.rar".to_string(), vec![2u8]),
            ("show.part03.rar".to_string(), vec![3u8]),
        ];
        let spec = rar_job_spec("RAR Failed Claim Delete Guard", &files);
        let working_dir = insert_active_job(&mut pipeline, job_id, spec).await;

        for (filename, bytes) in &files {
            tokio::fs::write(working_dir.join(filename), bytes)
                .await
                .unwrap();
        }

        let topology = weaver_assembly::ArchiveTopology {
            archive_type: weaver_assembly::ArchiveType::Rar,
            volume_map: std::collections::HashMap::from([
                ("show.part01.rar".to_string(), 0),
                ("show.part02.rar".to_string(), 1),
                ("show.part03.rar".to_string(), 2),
            ]),
            complete_volumes: [0u32, 1u32, 2u32].into_iter().collect(),
            expected_volume_count: Some(3),
            members: vec![
                weaver_assembly::ArchiveMember {
                    name: "E10.mkv".to_string(),
                    first_volume: 0,
                    last_volume: 1,
                    unpacked_size: 0,
                },
                weaver_assembly::ArchiveMember {
                    name: "E11.mkv".to_string(),
                    first_volume: 1,
                    last_volume: 2,
                    unpacked_size: 0,
                },
            ],
            unresolved_spans: Vec::new(),
        };
        pipeline
            .jobs
            .get_mut(&job_id)
            .unwrap()
            .assembly
            .set_archive_topology("show".to_string(), topology.clone());
        pipeline
            .failed_extractions
            .insert(job_id, ["E10.mkv".to_string()].into_iter().collect());
        pipeline.rar_sets.insert(
            (job_id, "show".to_string()),
            rar_state::RarSetState {
                facts: std::collections::BTreeMap::from([(1u32, dummy_rar_volume_facts(1))]),
                volume_files: std::collections::BTreeMap::new(),
                cached_headers: None,
                verified_suspect_volumes: std::collections::HashSet::new(),
                active_workers: 0,
                in_flight_members: std::collections::HashSet::new(),
                phase: rar_state::RarSetPhase::Ready,
                plan: Some(rar_state::RarDerivedPlan {
                    phase: rar_state::RarSetPhase::Ready,
                    is_solid: false,
                    ready_members: Vec::new(),
                    member_names: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                    waiting_on_volumes: std::collections::HashSet::new(),
                    deletion_eligible: [1u32].into_iter().collect(),
                    delete_decisions: std::collections::BTreeMap::from([(
                        1u32,
                        rar_state::RarVolumeDeleteDecision {
                            owners: vec!["E10.mkv".to_string(), "E11.mkv".to_string()],
                            clean_owners: vec!["E11.mkv".to_string()],
                            failed_owners: vec!["E10.mkv".to_string()],
                            pending_owners: Vec::new(),
                            unresolved_boundary: false,
                            ownership_eligible: false,
                        },
                    )]),
                    topology,
                    fallback_reason: None,
                }),
            },
        );

        pipeline.try_delete_volumes(job_id, "show");

        assert!(working_dir.join("show.part02.rar").exists());
        assert!(
            !pipeline
                .eagerly_deleted
                .get(&job_id)
                .is_some_and(|deleted| deleted.contains("show.part02.rar"))
        );
    }
}
