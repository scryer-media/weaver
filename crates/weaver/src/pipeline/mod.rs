mod completion;
mod decode;
mod download;
mod extraction;
mod health;
mod job;
mod metadata;
mod query;

use std::collections::HashMap;
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
use weaver_core::checksum;
use weaver_core::event::PipelineEvent;
use weaver_core::id::{JobId, NzbFileId, SegmentId};
use weaver_core::system::SystemProfile;
use weaver_nntp::NntpClient;
use weaver_par2::par2_set::Par2FileSet;
use weaver_scheduler::{
    DownloadQueue, DownloadWork, JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics,
    RuntimeTuner, SchedulerCommand, SharedPipelineState, TokenBucket,
};
use weaver_state::CommittedSegment;

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
pub(super) enum ExtractionDone {
    /// Batch extraction of specific members completed.
    Batch {
        job_id: JobId,
        set_name: String,
        extracted: Result<Vec<String>, String>,
    },
    /// Full set extraction completed (all volumes present).
    /// Result contains (extracted_count, failed_member_names).
    FullSet {
        job_id: JobId,
        set_name: String,
        result: Result<(u32, Vec<String>), String>,
    },
    /// Streaming extraction of a single member completed.
    /// Result contains (volume_index, bytes_written, temp_path) per chunk.
    Streaming {
        job_id: JobId,
        set_name: String,
        member: String,
        result: Result<Vec<(usize, u64, String)>, String>,
    },
}

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) crc32: u32,
    pub(super) data: DecodedData,
    /// Original filename from the yEnc header (for swap detection observability).
    pub(super) yenc_name: String,
}

pub(super) enum DecodedData {
    Pooled(BufferHandle),
    Owned(Vec<u8>),
}

impl DecodedData {
    pub(super) fn as_slice(&self) -> &[u8] {
        match self {
            Self::Pooled(buffer) => buffer.as_slice(),
            Self::Owned(data) => data.as_slice(),
        }
    }
}

impl BufferedChunk for DecodedData {
    fn len_bytes(&self) -> usize {
        self.as_slice().len()
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
    /// Buffer pool shared across decode and write stages.
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
    /// Directory for active downloads (per-job subdirectories).
    pub(super) intermediate_dir: PathBuf,
    /// Directory for completed downloads (category subdirectories).
    pub(super) complete_dir: PathBuf,
    /// Directory where persisted NZB files live (for reprocessing after restart).
    pub(super) nzb_dir: PathBuf,
    /// Pending segment commits (flushed to SQLite in batches).
    pub(super) segment_batch: Vec<CommittedSegment>,
    /// Channels for pipeline stage results.
    pub(super) download_done_tx: mpsc::Sender<DownloadResult>,
    pub(super) download_done_rx: mpsc::Receiver<DownloadResult>,
    pub(super) decode_done_tx: mpsc::Sender<DecodeResult>,
    pub(super) decode_done_rx: mpsc::Receiver<DecodeResult>,
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
    /// Bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    /// Gradual connection ramp-up limit (increases each tick).
    pub(super) connection_ramp: usize,
    /// Max pending segments per write reorder buffer (memory-adaptive).
    pub(super) write_buf_max_pending: usize,
    /// Per-file write reorder buffers for sequential disk writes.
    pub(super) write_buffers: HashMap<NzbFileId, WriteReorderBuffer<DecodedData>>,
    /// Retained PAR2 file sets per job, avoiding re-read/re-parse from disk.
    pub(super) par2_sets: HashMap<JobId, Arc<Par2FileSet>>,
    /// Members already extracted per job (for partial extraction).
    pub(super) extracted_members: HashMap<JobId, HashSet<String>>,
    /// Archive sets already extracted per job (for multi-set 7z).
    pub(super) extracted_sets: HashMap<JobId, HashSet<String>>,
    /// Members whose streaming extraction failed (corrupt volume, CRC error, etc).
    /// Prevents immediate retry during download; cleared after PAR2 repair so
    /// the post-repair extraction path can re-extract them.
    pub(super) failed_extractions: HashMap<JobId, HashSet<String>>,
    /// Authoritative recovery block counts per PAR2 file index.
    pub(super) recovery_block_counts: HashMap<JobId, HashMap<u32, u32>>,
    /// Recovery PAR2 file indices already promoted or completed for a job.
    /// Used to avoid double-promotion and to count targeted recovery capacity.
    pub(super) promoted_recovery_files: HashMap<JobId, HashSet<u32>>,
    /// Active streaming extraction providers per (job, archive set, member).
    /// The `WaitingVolumeProvider` is fed volume paths as they complete.
    /// Multiple providers can coexist for parallel member extraction.
    pub(super) streaming_providers:
        HashMap<(JobId, String, String), Arc<weaver_rar::WaitingVolumeProvider>>,
    /// Base volume number for each streaming provider, used to re-index volumes.
    /// When extracting a member that starts at volume N, the provider's indices
    /// are offset so that local index 0 maps to real volume N.
    pub(super) streaming_volume_bases: HashMap<(JobId, String, String), u32>,
    /// Filenames eagerly deleted per job after CRC-verified streaming extraction.
    /// Used to distinguish truly-missing files from intentionally-deleted ones
    /// during PAR2 verification.
    pub(super) eagerly_deleted: HashMap<JobId, HashSet<String>>,
    /// Volumes proven clean by CRC-verified extraction or authoritative PAR2 verify.
    pub(super) clean_volumes: HashMap<(JobId, String), HashSet<u32>>,
    /// Volumes touched by failed extraction or damaged in authoritative verify.
    pub(super) suspect_volumes: HashMap<(JobId, String), HashSet<u32>>,
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
        shared_state: SharedPipelineState,
        db: weaver_state::Database,
        config: weaver_core::config::SharedConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = Arc::clone(shared_state.metrics());
        let tuner = RuntimeTuner::with_connection_limit(profile, total_connections);
        info!(
            max_downloads = tuner.params().max_concurrent_downloads,
            total_connections, "pipeline tuner initialized"
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

        Ok(Self {
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
            intermediate_dir,
            complete_dir,
            nzb_dir,
            segment_batch: Vec::new(),
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
            global_paused: false,
            connection_ramp: total_connections.min(5),
            rate_limiter: TokenBucket::new(0),
            write_buf_max_pending,
            write_buffers: HashMap::new(),
            par2_sets: HashMap::new(),
            extracted_members: HashMap::new(),
            extracted_sets: HashMap::new(),
            failed_extractions: HashMap::new(),
            recovery_block_counts: HashMap::new(),
            promoted_recovery_files: HashMap::new(),
            streaming_providers: HashMap::new(),
            streaming_volume_bases: HashMap::new(),
            eagerly_deleted: HashMap::new(),
            clean_volumes: HashMap::new(),
            suspect_volumes: HashMap::new(),
            normalization_retried: HashSet::new(),
            pending_concat: HashMap::new(),
            par2_bypassed: HashSet::new(),
            finished_jobs: initial_history,
            shared_state,
            db,
            config,
        })
    }

    /// Run the pipeline main loop until shutdown.
    pub async fn run(&mut self) {
        let mut tune_interval = tokio::time::interval(std::time::Duration::from_secs(5));
        tune_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        info!("pipeline started");

        loop {
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
    fn publish_snapshot(&self) {
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
                let _ = reply.send(result);
            }
            SchedulerCommand::RestoreJob {
                job_id,
                spec,
                committed_segments,
                status,
                working_dir,
                reply,
            } => {
                let result =
                    self.restore_job(job_id, spec, committed_segments, status, working_dir);
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Paused);
                if result.is_ok() {
                    let _ = self.event_tx.send(PipelineEvent::JobPaused { job_id });
                    if let Err(e) = self.db.set_active_job_status(job_id, "paused", None) {
                        error!(error = %e, "db write failed for PauseJob");
                    }
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::ResumeJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Downloading);
                if result.is_ok() {
                    let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });
                    if let Err(e) = self.db.set_active_job_status(job_id, "downloading", None) {
                        error!(error = %e, "db write failed for ResumeJob");
                    }
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::CancelJob { job_id, reply } => {
                let result = if let Some(state) = self.jobs.remove(&job_id) {
                    // Per-job queues are dropped with the JobState.
                    self.job_order.retain(|id| *id != job_id);

                    // Archive cancelled job: move to history + delete active state.
                    let now = timestamp_secs() as i64;
                    let elapsed_secs = state.created_at.elapsed().as_secs() as i64;
                    let created_at = now - elapsed_secs;
                    let total = state.spec.total_bytes;
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
                        if let Err(e) = std::fs::remove_file(&nzb_path) {
                            if e.kind() != std::io::ErrorKind::NotFound {
                                tracing::warn!(path = %nzb_path.display(), error = %e, "failed to remove NZB file");
                            }
                        }
                    });

                    // Clean up per-job caches.
                    self.par2_sets.remove(&job_id);
                    self.extracted_members.remove(&job_id);
                    self.extracted_sets.remove(&job_id);
                    self.failed_extractions.remove(&job_id);
                    self.pending_concat.remove(&job_id);
                    self.recovery_block_counts.remove(&job_id);
                    self.promoted_recovery_files.remove(&job_id);
                    self.par2_bypassed.remove(&job_id);
                    self.eagerly_deleted.remove(&job_id);
                    self.clean_volumes.retain(|(jid, _), _| *jid != job_id);
                    self.suspect_volumes.retain(|(jid, _), _| *jid != job_id);
                    self.cancel_streaming_extraction(job_id);
                    self.write_buffers.retain(|fid, _| fid.job_id != job_id);

                    // Delete per-job working directory.
                    let working_dir = state.working_dir.clone();
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
                    });

                    Ok(())
                } else {
                    Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseAll { reply } => {
                self.global_paused = true;
                self.shared_state.set_paused(true);
                let _ = self.event_tx.send(PipelineEvent::GlobalPaused);
                let _ = reply.send(());
            }
            SchedulerCommand::ResumeAll { reply } => {
                self.global_paused = false;
                self.shared_state.set_paused(false);
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
            SchedulerCommand::RebuildNntp {
                client,
                total_connections,
                reply,
            } => {
                if let Ok(new_client) = client.downcast::<NntpClient>() {
                    self.nntp = Arc::new(*new_client);
                    self.connection_ramp = total_connections.min(5);
                    info!(
                        total_connections,
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
            SchedulerCommand::DeleteHistory { job_id, reply } => {
                let result = match self.jobs.get(&job_id).map(|state| state.status.clone()) {
                    Some(status) if !is_terminal_status(&status) => {
                        Err(weaver_scheduler::SchedulerError::Other(
                            "cannot delete active job — cancel it first".into(),
                        ))
                    }
                    Some(_) => {
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
                        self.finished_jobs.retain(|j| j.job_id != job_id);
                        let db = self.db.clone();
                        tokio::task::spawn_blocking(move || {
                            let _ = db.delete_job_history(job_id.0);
                            let _ = db.delete_job_events(job_id.0);
                        });
                        self.publish_snapshot();
                        Ok(())
                    }
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::DeleteAllHistory { reply } => {
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
                tokio::task::spawn_blocking(move || {
                    let _ = db.delete_all_job_history();
                    let _ = db.delete_all_job_events();
                });
                self.publish_snapshot();
                let _ = reply.send(Ok(()));
            }
            SchedulerCommand::Shutdown => unreachable!("handled in select"),
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
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(path)
        .await?;

    file.seek(std::io::SeekFrom::Start(offset)).await?;
    file.write_all(data).await?;
    Ok(())
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
    fn purge_terminal_job_runtime(&mut self, job_id: JobId) {
        self.jobs.remove(&job_id);
        self.job_order.retain(|id| *id != job_id);
        self.par2_sets.remove(&job_id);
        self.extracted_members.remove(&job_id);
        self.extracted_sets.remove(&job_id);
        self.failed_extractions.remove(&job_id);
        self.pending_concat.remove(&job_id);
        self.recovery_block_counts.remove(&job_id);
        self.promoted_recovery_files.remove(&job_id);
        self.par2_bypassed.remove(&job_id);
        self.eagerly_deleted.remove(&job_id);
        self.clean_volumes.retain(|(jid, _), _| *jid != job_id);
        self.suspect_volumes.retain(|(jid, _), _| *jid != job_id);
        self.normalization_retried.remove(&job_id);
        self.cancel_streaming_extraction(job_id);
        self.write_buffers.retain(|fid, _| fid.job_id != job_id);
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
            if let Err(e) = std::fs::remove_file(&nzb_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %nzb_path.display(), error = %e, "failed to remove NZB file");
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::sync::RwLock;
    use weaver_api::submit_nzb_bytes;
    use weaver_core::buffer::{BufferPool, BufferPoolConfig};
    use weaver_core::config::{Config, SharedConfig};
    use weaver_core::system::{
        CpuProfile, DiskProfile, FilesystemType, MemoryProfile, SimdSupport, StorageClass,
        SystemProfile,
    };
    use weaver_nntp::client::{NntpClient, NntpClientConfig};
    use weaver_scheduler::{PipelineMetrics, SchedulerHandle, SharedPipelineState};
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
        }
    }

    async fn new_direct_pipeline(temp_dir: &TempDir) -> (Pipeline, PathBuf, PathBuf) {
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
        let buffers = BufferPool::new(BufferPoolConfig {
            small_count: 8,
            medium_count: 4,
            large_count: 2,
        });

        let pipeline = Pipeline::new(
            cmd_rx,
            event_tx,
            nntp,
            buffers,
            profile,
            data_dir,
            intermediate_dir.clone(),
            complete_dir.clone(),
            0,
            4,
            vec![],
            shared_state,
            db,
            config,
        )
        .await
        .unwrap();

        (pipeline, intermediate_dir, complete_dir)
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

        let state = pipeline.jobs.get(&job_id).unwrap();
        assert!(matches!(state.status, JobStatus::Failed { .. }));
        let JobStatus::Failed { error } = &state.status else {
            unreachable!();
        };
        assert!(error.contains("failed to read working directory"));
        assert!(!complete_dir.join(job::sanitize_dirname(job_name)).exists());
    }

    #[tokio::test]
    async fn missing_workdir_fails_job_and_does_not_block_future_commands() {
        let harness = TestHarness::new().await;
        let nzb_bytes = sample_nzb_bytes();
        let mut events = harness.handle.subscribe_events();

        let first = tokio::time::timeout(
            Duration::from_secs(2),
            submit_nzb_bytes(
                &harness.handle,
                &harness.config,
                &nzb_bytes,
                Some("Frieren.Broken.nzb".to_string()),
                None,
                None,
                vec![],
            ),
        )
        .await
        .unwrap()
        .unwrap();

        let output_dir = loop {
            if let Ok(info) = harness.handle.get_job(first.job_id)
                && let Some(output_dir) = info.output_dir
            {
                break PathBuf::from(output_dir);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        };
        tokio::fs::remove_dir_all(&output_dir).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), harness.handle.pause_all())
            .await
            .unwrap()
            .unwrap();

        let failure = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                match events.recv().await {
                    Ok(PipelineEvent::JobFailed { job_id, error }) if job_id == first.job_id => {
                        break error;
                    }
                    Ok(_) => {}
                    Err(_) => continue,
                }
            }
        })
        .await
        .unwrap();
        assert!(failure.contains("working directory missing"));

        wait_until(Duration::from_secs(2), || {
            matches!(
                harness.handle.get_job(first.job_id).map(|info| info.status),
                Ok(JobStatus::Failed { .. })
            )
        })
        .await
        .unwrap();

        tokio::time::timeout(Duration::from_secs(2), harness.handle.resume_all())
            .await
            .unwrap()
            .unwrap();

        let second = tokio::time::timeout(
            Duration::from_secs(2),
            submit_nzb_bytes(
                &harness.handle,
                &harness.config,
                &nzb_bytes,
                Some("Frieren.Recovery.nzb".to_string()),
                None,
                None,
                vec![],
            ),
        )
        .await
        .unwrap()
        .unwrap();

        wait_until(Duration::from_secs(2), || {
            harness
                .db
                .load_active_jobs()
                .map(|jobs| jobs.contains_key(&second.job_id))
                .unwrap_or(false)
        })
        .await
        .unwrap();
        assert_eq!(
            harness.handle.get_job(second.job_id).unwrap().status,
            JobStatus::Downloading
        );
        assert!(!harness.handle.is_globally_paused());

        harness.shutdown().await;
    }
}
