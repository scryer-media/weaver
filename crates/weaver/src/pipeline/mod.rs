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

use weaver_assembly::write_buffer::WriteReorderBuffer;
use weaver_assembly::{ExtractionReadiness, JobAssembly};
use weaver_core::buffer::BufferPool;
use weaver_core::checksum;
use weaver_core::event::PipelineEvent;
use weaver_core::id::{JobId, NzbFileId, SegmentId};
use weaver_core::system::SystemProfile;
use weaver_nntp::NntpClient;
use weaver_par2::par2_set::Par2FileSet;
use weaver_par2::verify::{
    FileStatus, FileVerification, Repairability, VerificationResult,
};
use weaver_scheduler::{
    DownloadQueue, DownloadWork, JobInfo, JobSpec, JobState, JobStatus, PipelineMetrics,
    RuntimeTuner, SchedulerCommand, TokenBucket,
};
use weaver_state::{JournalEntry, JournalWriter};


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

/// Result of a decode task.
pub(super) struct DecodeResult {
    pub(super) segment_id: SegmentId,
    pub(super) file_offset: u64,
    pub(super) decoded_size: u32,
    pub(super) crc_valid: bool,
    pub(super) crc32: u32,
    pub(super) data: Vec<u8>,
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
    /// Buffer pool for decode stage.
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
    /// Output directory for downloaded files.
    pub(super) output_dir: PathBuf,
    /// Crash-recovery journal.
    pub(super) journal: JournalWriter,
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
    /// Whether all downloads are globally paused.
    pub(super) global_paused: bool,
    /// Bandwidth rate limiter.
    pub(super) rate_limiter: TokenBucket,
    /// Gradual connection ramp-up limit (increases each tick).
    pub(super) connection_ramp: usize,
    /// Max pending segments per write reorder buffer (memory-adaptive).
    pub(super) write_buf_max_pending: usize,
    /// Per-file write reorder buffers for sequential disk writes.
    pub(super) write_buffers: HashMap<NzbFileId, WriteReorderBuffer>,
    /// Retained PAR2 file sets per job, avoiding re-read/re-parse from disk.
    pub(super) par2_sets: HashMap<JobId, Arc<Par2FileSet>>,
    /// Members already extracted per job (for partial extraction).
    pub(super) extracted_members: HashMap<JobId, HashSet<String>>,
    /// Active streaming extraction providers per job.
    /// The `WaitingVolumeProvider` is fed volume paths as they complete.
    pub(super) streaming_providers: HashMap<JobId, Arc<weaver_rar::WaitingVolumeProvider>>,
    /// Finished jobs (Complete/Failed) from recovery — surfaced in list/get queries.
    pub(super) finished_jobs: Vec<JobInfo>,
}

impl Pipeline {
    /// Create a new pipeline.
    pub async fn new(
        cmd_rx: mpsc::Receiver<SchedulerCommand>,
        event_tx: broadcast::Sender<PipelineEvent>,
        nntp: NntpClient,
        buffers: Arc<BufferPool>,
        profile: SystemProfile,
        output_dir: PathBuf,
        total_connections: usize,
        write_buf_max_pending: usize,
        initial_history: Vec<JobInfo>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metrics = PipelineMetrics::new();
        let tuner = RuntimeTuner::with_connection_limit(profile, total_connections);
        info!(
            max_downloads = tuner.params().max_concurrent_downloads,
            total_connections,
            "pipeline tuner initialized"
        );

        // Ensure output directory exists.
        tokio::fs::create_dir_all(&output_dir).await?;

        // Open state journal.
        let journal_path = output_dir.join(".weaver-journal");
        let journal = JournalWriter::open(&journal_path).await?;
        info!(path = %journal_path.display(), "journal opened");

        // Internal channels for pipeline stage results.
        let (download_done_tx, download_done_rx) = mpsc::channel(256);
        let (decode_done_tx, decode_done_rx) = mpsc::channel(256);
        let (retry_tx, retry_rx) = mpsc::channel(256);
        let (probe_result_tx, probe_result_rx) = mpsc::channel(16);

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
            output_dir,
            journal,
            download_done_tx,
            download_done_rx,
            decode_done_tx,
            decode_done_rx,
            retry_tx,
            retry_rx,
            probe_result_tx,
            probe_result_rx,
            global_paused: false,
            connection_ramp: total_connections.min(5),
            rate_limiter: TokenBucket::new(0),
            write_buf_max_pending,
            write_buffers: HashMap::new(),
            par2_sets: HashMap::new(),
            extracted_members: HashMap::new(),
            streaming_providers: HashMap::new(),
            finished_jobs: initial_history,
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
        }

        // Graceful shutdown: wait for in-flight work to drain.
        self.drain().await;
        info!("pipeline stopped");
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
                reply,
            } => {
                let result = self.restore_job(job_id, spec, committed_segments, status);
                let _ = reply.send(result);
            }
            SchedulerCommand::PauseJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Paused);
                if result.is_ok() {
                    let _ = self.event_tx.send(PipelineEvent::JobPaused { job_id });
                    let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                        job_id,
                        status: weaver_state::PersistedJobStatus::Paused,
                        timestamp: timestamp_secs(),
                    }).await;
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::ResumeJob { job_id, reply } => {
                let result = self.set_job_status(job_id, JobStatus::Downloading);
                if result.is_ok() {
                    let _ = self.event_tx.send(PipelineEvent::JobResumed { job_id });
                    let _ = self.journal.append(&JournalEntry::JobStatusChanged {
                        job_id,
                        status: weaver_state::PersistedJobStatus::Downloading,
                        timestamp: timestamp_secs(),
                    }).await;
                }
                let _ = reply.send(result);
            }
            SchedulerCommand::CancelJob { job_id, reply } => {
                let result = if let Some(state) = self.jobs.remove(&job_id) {
                    // Per-job queues are dropped with the JobState.
                    self.job_order.retain(|id| *id != job_id);

                    // Clean up per-job caches.
                    self.par2_sets.remove(&job_id);
                    self.extracted_members.remove(&job_id);
                    self.cancel_streaming_extraction(job_id);
                    self.write_buffers.retain(|fid, _| fid.job_id != job_id);

                    // Delete incomplete files from disk.
                    let filenames: Vec<String> = state
                        .assembly
                        .files()
                        .map(|f| f.filename().to_string())
                        .collect();
                    let output_dir = self.output_dir.clone();
                    tokio::spawn(async move {
                        for filename in filenames {
                            let path = output_dir.join(&filename);
                            if let Err(e) = tokio::fs::remove_file(&path).await {
                                if e.kind() != std::io::ErrorKind::NotFound {
                                    tracing::warn!(
                                        file = %path.display(),
                                        error = %e,
                                        "failed to clean up cancelled job file"
                                    );
                                }
                            }
                        }
                    });

                    Ok(())
                } else {
                    Err(weaver_scheduler::SchedulerError::JobNotFound(job_id))
                };
                let _ = reply.send(result);
            }
            SchedulerCommand::GetJobStatus { job_id, reply } => {
                let result = self.get_job_info(job_id);
                let _ = reply.send(result);
            }
            SchedulerCommand::ListJobs { reply } => {
                let list = self.list_jobs();
                let _ = reply.send(list);
            }
            SchedulerCommand::GetMetrics { reply } => {
                let _ = reply.send(self.metrics.snapshot());
            }
            SchedulerCommand::PauseAll { reply } => {
                self.global_paused = true;
                let _ = self.event_tx.send(PipelineEvent::GlobalPaused);
                let _ = reply.send(());
            }
            SchedulerCommand::ResumeAll { reply } => {
                self.global_paused = false;
                let _ = self.event_tx.send(PipelineEvent::GlobalResumed);
                let _ = reply.send(());
            }
            SchedulerCommand::GetGlobalPauseState { reply } => {
                let _ = reply.send(self.global_paused);
            }
            SchedulerCommand::SetSpeedLimit { bytes_per_sec, reply } => {
                self.rate_limiter.set_rate(bytes_per_sec);
                let _ = reply.send(());
            }
            SchedulerCommand::RebuildNntp { client, total_connections, reply } => {
                if let Ok(new_client) = client.downcast::<NntpClient>() {
                    self.nntp = Arc::new(*new_client);
                    self.connection_ramp = total_connections.min(5);
                    info!(total_connections, "NNTP client rebuilt with new server config");
                }
                let _ = reply.send(());
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
        // Flush the journal before exiting.
        if let Err(e) = self.journal.sync().await {
            error!(error = %e, "journal sync failed during shutdown");
        }
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
    let path_cstr = match std::ffi::CString::new(
        output_dir.to_str().unwrap_or(".").as_bytes(),
    ) {
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

/// Convert a scheduler JobStatus to a persisted journal status.
#[allow(dead_code)]
pub(super) fn to_persisted(status: &JobStatus) -> weaver_state::PersistedJobStatus {
    match status {
        JobStatus::Queued | JobStatus::Downloading | JobStatus::Checking => {
            weaver_state::PersistedJobStatus::Downloading
        }
        JobStatus::Verifying => weaver_state::PersistedJobStatus::Verifying,
        JobStatus::Repairing => weaver_state::PersistedJobStatus::Repairing,
        JobStatus::Extracting => weaver_state::PersistedJobStatus::Extracting,
        JobStatus::Complete => weaver_state::PersistedJobStatus::Complete,
        JobStatus::Failed { error } => weaver_state::PersistedJobStatus::Failed {
            error: error.clone(),
        },
        JobStatus::Paused => weaver_state::PersistedJobStatus::Paused,
    }
}
