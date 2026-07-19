use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::bandwidth::{IspBandwidthCapConfig, IspBandwidthCapPeriod};
use crate::events::model::PipelineEvent;
use crate::jobs::assembly::DetectedArchiveIdentity;
use crate::jobs::ids::{JobId, NzbFileId};
use crate::jobs::record::ActiveFileIdentity;

use crate::jobs::error::SchedulerError;
use crate::jobs::model::{JobSpec, JobStatus, JobUpdate};
use crate::operations::metrics::{MetricsSnapshot, PipelineMetrics};

pub const FINISHED_JOBS_RUNTIME_CAP: usize = 1_000;

/// Shared read-only view of pipeline state for the control plane.
///
/// Written by the pipeline loop after each event, read by API handlers
/// without going through the command channel.
#[derive(Clone)]
pub struct SharedPipelineState {
    jobs: Arc<RwLock<Vec<JobInfo>>>,
    job_revision: tokio::sync::watch::Sender<u64>,
    paused: Arc<AtomicBool>,
    post_processing_paused: Arc<AtomicBool>,
    metrics: Arc<PipelineMetrics>,
    metrics_snapshot: Arc<RwLock<MetricsSnapshot>>,
    download_block: Arc<RwLock<DownloadBlockState>>,
    server_quota_blocked: Arc<AtomicBool>,
    server_transfer_policy:
        Arc<RwLock<Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>>>>,
    nntp_pool: Arc<RwLock<Option<Arc<weaver_nntp::pool::NntpPool>>>>,
}

impl SharedPipelineState {
    pub fn new(metrics: Arc<PipelineMetrics>, initial_jobs: Vec<JobInfo>) -> Self {
        let metrics_snapshot = metrics.snapshot();
        let (job_revision, _) = tokio::sync::watch::channel(0);
        Self {
            jobs: Arc::new(RwLock::new(initial_jobs)),
            job_revision,
            paused: Arc::new(AtomicBool::new(false)),
            post_processing_paused: Arc::new(AtomicBool::new(false)),
            metrics,
            metrics_snapshot: Arc::new(RwLock::new(metrics_snapshot)),
            download_block: Arc::new(RwLock::new(DownloadBlockState::default())),
            server_quota_blocked: Arc::new(AtomicBool::new(false)),
            server_transfer_policy: Arc::new(RwLock::new(None)),
            nntp_pool: Arc::new(RwLock::new(None)),
        }
    }

    // --- Reader methods (called by API handlers) ---

    pub fn list_jobs(&self) -> Vec<JobInfo> {
        self.jobs.read().unwrap().clone()
    }

    pub fn get_job(&self, job_id: JobId) -> Option<JobInfo> {
        self.jobs
            .read()
            .unwrap()
            .iter()
            .find(|j| j.job_id == job_id)
            .cloned()
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Relaxed)
    }

    pub fn is_post_processing_paused(&self) -> bool {
        self.post_processing_paused.load(Ordering::Relaxed)
    }

    pub fn set_post_processing_paused(&self, paused: bool) {
        self.post_processing_paused.store(paused, Ordering::Relaxed);
    }

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics_snapshot.read().unwrap().clone()
    }

    pub fn raw_metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.raw_snapshot()
    }

    pub fn metrics(&self) -> &Arc<PipelineMetrics> {
        &self.metrics
    }

    pub fn download_block(&self) -> DownloadBlockState {
        self.download_block.read().unwrap().clone()
    }

    // --- Writer methods (called by pipeline loop only) ---

    pub fn publish_jobs(&self, jobs: Vec<JobInfo>) {
        *self.jobs.write().unwrap() = jobs;
        self.job_revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    pub fn subscribe_job_changes(&self) -> tokio::sync::watch::Receiver<u64> {
        self.job_revision.subscribe()
    }

    pub fn refresh_metrics_snapshot(&self) {
        *self.metrics_snapshot.write().unwrap() = self.metrics.snapshot();
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
    }

    pub fn set_download_block(&self, mut state: DownloadBlockState) {
        let mut current = self.download_block.write().unwrap();
        if self.server_quota_blocked.load(Ordering::Relaxed)
            && matches!(
                state.kind,
                DownloadBlockKind::None
                    | DownloadBlockKind::Scheduled
                    | DownloadBlockKind::ServerQuota
            )
        {
            state.kind = DownloadBlockKind::ServerQuota;
        }
        *current = state;
    }

    pub fn server_quota_blocked(&self) -> bool {
        self.server_quota_blocked.load(Ordering::Relaxed)
    }

    pub fn set_server_quota_blocked(&self, blocked: bool) {
        self.server_quota_blocked.store(blocked, Ordering::Relaxed);
        let mut state = self.download_block.write().unwrap();
        if blocked {
            if matches!(
                state.kind,
                DownloadBlockKind::None
                    | DownloadBlockKind::Scheduled
                    | DownloadBlockKind::ServerQuota
            ) {
                state.kind = DownloadBlockKind::ServerQuota;
            }
        } else if state.kind == DownloadBlockKind::ServerQuota {
            state.kind = if state.cap_enabled && state.remaining_bytes == 0 {
                DownloadBlockKind::IspCap
            } else if state.scheduled_speed_limit > 0 {
                DownloadBlockKind::Scheduled
            } else {
                DownloadBlockKind::None
            };
        }
    }

    pub fn set_server_transfer_policy(
        &self,
        registry: Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
    ) {
        *self.server_transfer_policy.write().unwrap() = Some(registry);
    }

    pub fn server_transfer_policy(
        &self,
    ) -> Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>> {
        self.server_transfer_policy.read().unwrap().clone()
    }

    pub fn set_nntp_pool(&self, pool: Arc<weaver_nntp::pool::NntpPool>) {
        *self.nntp_pool.write().unwrap() = Some(pool);
    }

    pub fn nntp_pool(&self) -> Option<Arc<weaver_nntp::pool::NntpPool>> {
        self.nntp_pool.read().unwrap().clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadBlockKind {
    None,
    ManualPause,
    Scheduled,
    IspCap,
    ServerQuota,
}

/// Where a manual queue reorder should land a job. The order only breaks ties
/// within a dispatch priority band: a LOW job moved to the top still yields to
/// HIGH/NORMAL work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueMoveTarget {
    Top,
    Bottom,
    Offset(i64),
}

/// Splice `job_id` to its new position in the manual queue order. Pure so the
/// clamp/splice arithmetic is unit-testable apart from pipeline state.
pub fn splice_job_order(
    order: &mut Vec<JobId>,
    job_id: JobId,
    target: QueueMoveTarget,
) -> Result<bool, SchedulerError> {
    let Some(current) = order.iter().position(|id| *id == job_id) else {
        return Err(SchedulerError::JobNotFound(job_id));
    };
    let last = order.len() - 1;
    let new_index = match target {
        QueueMoveTarget::Top => 0,
        QueueMoveTarget::Bottom => last,
        QueueMoveTarget::Offset(delta) => {
            (current as i64).saturating_add(delta).clamp(0, last as i64) as usize
        }
    };
    if new_index == current {
        return Ok(false);
    }
    let id = order.remove(current);
    order.insert(new_index, id);
    Ok(true)
}

#[cfg(test)]
mod splice_job_order_tests {
    use super::*;

    fn order(ids: &[u64]) -> Vec<JobId> {
        ids.iter().copied().map(JobId).collect()
    }

    #[test]
    fn splices_top_bottom_and_clamped_offsets() {
        let mut jobs = order(&[1, 2, 3, 4]);
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Top).unwrap());
        assert_eq!(jobs, order(&[3, 1, 2, 4]));

        assert!(splice_job_order(&mut jobs, JobId(1), QueueMoveTarget::Bottom).unwrap());
        assert_eq!(jobs, order(&[3, 2, 4, 1]));

        assert!(splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Offset(-1)).unwrap());
        assert_eq!(jobs, order(&[3, 4, 2, 1]));

        // Offsets clamp at the edges instead of erroring.
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(-5)).is_ok());
        assert_eq!(jobs, order(&[3, 4, 2, 1]));
        assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(99)).unwrap());
        assert_eq!(jobs, order(&[4, 2, 1, 3]));

        // No-op moves report false; unknown ids error.
        assert!(!splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Top).unwrap());
        assert!(matches!(
            splice_job_order(&mut jobs, JobId(9), QueueMoveTarget::Top),
            Err(SchedulerError::JobNotFound(JobId(9)))
        ));
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DownloadBlockState {
    pub kind: DownloadBlockKind,
    pub cap_enabled: bool,
    pub period: Option<IspBandwidthCapPeriod>,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub remaining_bytes: u64,
    pub reserved_bytes: u64,
    pub window_starts_at_epoch_ms: Option<f64>,
    pub window_ends_at_epoch_ms: Option<f64>,
    pub timezone_name: String,
    /// Speed limit imposed by the active schedule (0 = no scheduled limit).
    #[serde(default)]
    pub scheduled_speed_limit: u64,
}

impl Default for DownloadBlockState {
    fn default() -> Self {
        Self {
            kind: DownloadBlockKind::None,
            cap_enabled: false,
            period: None,
            used_bytes: 0,
            limit_bytes: 0,
            remaining_bytes: 0,
            reserved_bytes: 0,
            window_starts_at_epoch_ms: None,
            window_ends_at_epoch_ms: None,
            timezone_name: chrono::Local::now().offset().to_string(),
            scheduled_speed_limit: 0,
        }
    }
}

/// Commands sent to the scheduler's main loop.
pub struct RestoreJobRequest {
    pub job_id: JobId,
    pub job_hash: [u8; 32],
    pub spec: JobSpec,
    pub complete_files: HashSet<NzbFileId>,
    pub file_progress: HashMap<u32, u64>,
    pub detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    pub file_identities: HashMap<u32, ActiveFileIdentity>,
    pub extracted_members: HashSet<String>,
    pub status: JobStatus,
    pub download_state: Option<crate::jobs::model::DownloadState>,
    pub post_state: Option<crate::jobs::model::PostState>,
    pub run_state: Option<crate::jobs::model::RunState>,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<JobStatus>,
    pub paused_resume_download_state: Option<crate::jobs::model::DownloadState>,
    pub paused_resume_post_state: Option<crate::jobs::model::PostState>,
    pub working_dir: PathBuf,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AddJobOptions {
    pub initially_paused: bool,
    /// SCORE admission generation that must still be authoritative when the
    /// scheduler durably materializes this job.
    pub semantic_materialization_generation: Option<i64>,
    /// Promotion-lease generation that owns this candidate materialization.
    /// Unlike admission generations, this must also still hold an unexpired
    /// durable promotion lease when the active job row is inserted.
    pub semantic_promotion_generation: Option<i64>,
}

/// Internal provenance for cancellation. Semantic cancellation is intentionally
/// narrower than a user cancel: it may interrupt only download-lane work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationOrigin {
    User,
    SemanticBad,
    SemanticSuperseded,
}

pub enum SchedulerCommand {
    /// Submit a new job.
    AddJob {
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: AddJobOptions,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Restore a job from the journal (crash recovery).
    RestoreJob {
        request: Box<RestoreJobRequest>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Pause a job.
    PauseJob {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Resume a paused job.
    ResumeJob {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Cancel and remove a job.
    CancelJob {
        job_id: JobId,
        origin: CancellationOrigin,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Update a job's category and/or metadata.
    UpdateJob {
        job_id: JobId,
        update: JobUpdate,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Move a job within the manual queue order.
    ReorderJob {
        job_id: JobId,
        target: QueueMoveTarget,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Batch-move multiple jobs within the manual queue order in a single
    /// round trip. Applied all-or-nothing: if any job id is unknown, no move
    /// is applied. Persists the resulting order and publishes a snapshot
    /// once for the whole batch rather than once per move.
    ReorderJobs {
        moves: Vec<(JobId, QueueMoveTarget)>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Pause all jobs globally (pipeline-wide).
    PauseAll { reply: oneshot::Sender<()> },
    /// Resume all jobs globally.
    ResumeAll { reply: oneshot::Sender<()> },
    /// Pause admission of queued extension post-processing attempts.
    PausePostProcessing { reply: oneshot::Sender<()> },
    /// Resume admission of queued extension post-processing attempts.
    ResumePostProcessing { reply: oneshot::Sender<()> },
    /// Cancel the queued or active extension attempt for a job.
    CancelPostProcessing {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Set global speed limit (bytes/sec). 0 means unlimited.
    SetSpeedLimit {
        bytes_per_sec: u64,
        reply: oneshot::Sender<()>,
    },
    /// Set global over-max latent-IP replacement burst budget. v1 allows 0 or 1.
    SetIpReplacementTrialExtraConnections {
        extra_connections: u8,
        reply: oneshot::Sender<()>,
    },
    /// Apply a scheduled action (pause, resume, or speed limit).
    /// Sent by the schedule evaluator background task.
    ApplyScheduleAction {
        action: crate::bandwidth::ScheduleAction,
        reply: oneshot::Sender<()>,
    },
    /// Clear any schedule-imposed pause or speed limit.
    ClearScheduleAction { reply: oneshot::Sender<()> },
    /// Set or clear the ISP bandwidth cap policy.
    SetBandwidthCapPolicy {
        policy: Option<IspBandwidthCapConfig>,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Replace the NNTP client at runtime (hot-reload after server config change).
    /// The client is boxed as `dyn Any` to avoid coupling weaver-scheduler to weaver-nntp.
    RebuildNntp {
        client: Box<dyn Any + Send>,
        total_connections: usize,
        reply: oneshot::Sender<()>,
    },
    /// Update runtime storage directories after a restore.
    UpdateRuntimePaths {
        data_dir: PathBuf,
        intermediate_dir: PathBuf,
        complete_dir: PathBuf,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Reprocess a completed or failed job (re-run post-download stages without re-downloading).
    ReprocessJob {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Re-download a completed or failed job from its persisted NZB under the same job ID.
    RedownloadJob {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Delete a completed/failed/cancelled job from history.
    DeleteHistory {
        job_id: JobId,
        delete_files: bool,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Delete all completed/failed/cancelled jobs from history.
    DeleteAllHistory {
        delete_files: bool,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Shutdown the scheduler gracefully.
    Shutdown,
}

/// Summary info about a job (returned by queries).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobInfo {
    pub job_id: JobId,
    pub job_hash: Option<[u8; 32]>,
    pub name: String,
    pub status: JobStatus,
    pub download_state: crate::jobs::model::DownloadState,
    pub post_state: crate::jobs::model::PostState,
    pub run_state: crate::jobs::model::RunState,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    #[serde(default)]
    pub phase_progress: Vec<crate::jobs::phase_progress::JobPhaseProgress>,
    /// Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    /// Job health 0-1000 (1000 = perfect). Drops as articles fail.
    pub health: u32,
    /// Total files in the NZB (all roles).
    #[serde(default)]
    pub total_files: u32,
    /// Files fully downloaded and committed.
    #[serde(default)]
    pub completed_files: u32,
    /// PAR2 recovery volumes not yet fetched (fetched on demand).
    #[serde(default)]
    pub remaining_par_files: u32,
    pub password: Option<String>,
    /// Optional category (e.g. "tv", "movies").
    pub category: Option<String>,
    /// Arbitrary key-value metadata from the submitting client.
    pub metadata: Vec<(String, String)>,
    /// Output directory where extracted files land.
    pub output_dir: Option<String>,
    /// Error message (only set when status is Failed).
    pub error: Option<String>,
    /// Wall-clock creation time (Unix epoch milliseconds).
    pub created_at_epoch_ms: f64,
}

/// Handle for sending commands to the scheduler.
/// Cloneable, can be shared across tasks.
#[derive(Clone)]
pub struct SchedulerHandle {
    cmd_tx: mpsc::Sender<SchedulerCommand>,
    event_tx: broadcast::Sender<PipelineEvent>,
    state: SharedPipelineState,
}

impl SchedulerHandle {
    /// Create a new handle from a command sender, event broadcast sender,
    /// and shared pipeline state.
    pub fn new(
        cmd_tx: mpsc::Sender<SchedulerCommand>,
        event_tx: broadcast::Sender<PipelineEvent>,
        state: SharedPipelineState,
    ) -> Self {
        Self {
            cmd_tx,
            event_tx,
            state,
        }
    }

    /// Submit a new download job.
    pub async fn add_job(
        &self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
    ) -> Result<(), SchedulerError> {
        self.add_job_with_options(job_id, spec, nzb_path, nzb_zstd, AddJobOptions::default())
            .await
    }

    /// Submit a new download job with explicit scheduler options.
    pub async fn add_job_with_options(
        &self,
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
        nzb_zstd: Vec<u8>,
        options: AddJobOptions,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::AddJob {
                job_id,
                spec,
                nzb_path,
                nzb_zstd,
                options,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Restore a job from crash-recovery journal.
    pub async fn restore_job(&self, request: RestoreJobRequest) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::RestoreJob {
                request: Box::new(request),
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Pause a job.
    pub async fn pause_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::PauseJob { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Resume a paused job.
    pub async fn resume_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ResumeJob { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Cancel a job.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::User)
            .await
    }

    pub async fn cancel_semantic_bad(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::SemanticBad)
            .await
    }

    pub async fn cancel_semantic_superseded(&self, job_id: JobId) -> Result<(), SchedulerError> {
        self.cancel_job_with_origin(job_id, CancellationOrigin::SemanticSuperseded)
            .await
    }

    async fn cancel_job_with_origin(
        &self,
        job_id: JobId,
        origin: CancellationOrigin,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::CancelJob {
                job_id,
                origin,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Update a job's category and/or metadata.
    pub async fn update_job(&self, job_id: JobId, update: JobUpdate) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::UpdateJob {
                job_id,
                update,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Move a job within the manual queue order. Durable and reflected in
    /// queue listings; breaks ties within a dispatch priority band rather than
    /// overriding HIGH/NORMAL/LOW.
    pub async fn reorder_job(
        &self,
        job_id: JobId,
        target: QueueMoveTarget,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ReorderJob {
                job_id,
                target,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Move multiple jobs within the manual queue order in a single round
    /// trip. All moves are applied atomically: if any job id is unknown, no
    /// move is applied and `Err(SchedulerError::JobNotFound)` is returned.
    /// The resulting order is persisted and the job snapshot is published
    /// once for the whole batch, rather than once per move.
    pub async fn reorder_jobs(
        &self,
        moves: Vec<(JobId, QueueMoveTarget)>,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ReorderJobs { moves, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Reprocess a completed or failed job (re-run post-download stages without re-downloading).
    pub async fn reprocess_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ReprocessJob { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Re-download a completed or failed job from its persisted NZB under the same job ID.
    pub async fn redownload_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::RedownloadJob { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Delete a completed/failed/cancelled job from history.
    pub async fn delete_history(
        &self,
        job_id: JobId,
        delete_files: bool,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::DeleteHistory {
                job_id,
                delete_files,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Delete all completed/failed/cancelled jobs from history.
    pub async fn delete_all_history(&self, delete_files: bool) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::DeleteAllHistory {
                delete_files,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Get info about a specific job (reads from shared state, no channel round-trip).
    pub fn get_job(&self, job_id: JobId) -> Result<JobInfo, SchedulerError> {
        self.state
            .get_job(job_id)
            .ok_or(SchedulerError::JobNotFound(job_id))
    }

    /// List all jobs (reads from shared state, no channel round-trip).
    pub fn list_jobs(&self) -> Vec<JobInfo> {
        self.state.list_jobs()
    }

    /// Get current metrics (reads from shared state, no channel round-trip).
    pub fn get_metrics(&self) -> MetricsSnapshot {
        self.state.metrics_snapshot()
    }

    /// Get a fresh atomics-based metrics snapshot without advancing the shared
    /// speed tracker.
    pub fn get_live_metrics(&self) -> MetricsSnapshot {
        self.state.raw_metrics_snapshot()
    }

    pub fn get_download_block(&self) -> DownloadBlockState {
        self.state.download_block()
    }

    /// Pause all download dispatch globally.
    pub async fn pause_all(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::PauseAll { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    /// Resume all download dispatch globally.
    pub async fn resume_all(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ResumeAll { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    pub async fn pause_post_processing(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::PausePostProcessing { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    pub async fn resume_post_processing(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ResumePostProcessing { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    pub async fn cancel_post_processing(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::CancelPostProcessing { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Check whether the pipeline is globally paused (reads from shared state).
    pub fn is_globally_paused(&self) -> bool {
        self.state.is_paused()
    }

    pub fn is_post_processing_paused(&self) -> bool {
        self.state.is_post_processing_paused()
    }

    pub async fn set_bandwidth_cap_policy(
        &self,
        policy: Option<IspBandwidthCapConfig>,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::SetBandwidthCapPolicy { policy, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Set the global download speed limit. 0 means unlimited.
    pub async fn set_speed_limit(&self, bytes_per_sec: u64) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::SetSpeedLimit {
                bytes_per_sec,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    pub async fn set_ip_replacement_trial_extra_connections(
        &self,
        extra_connections: u8,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::SetIpReplacementTrialExtraConnections {
                extra_connections: extra_connections.min(1),
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    /// Apply a scheduled action (called by the schedule evaluator).
    pub async fn apply_schedule_action(
        &self,
        action: crate::bandwidth::ScheduleAction,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ApplyScheduleAction { action, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    /// Clear any schedule-imposed pause or speed limit.
    pub async fn clear_schedule_action(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ClearScheduleAction { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    pub fn set_server_transfer_policy(
        &self,
        registry: Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>,
    ) {
        self.state.set_server_transfer_policy(registry);
    }

    pub fn server_transfer_policy(
        &self,
    ) -> Option<Arc<crate::servers::transfer_policy::ServerTransferPolicyRegistry>> {
        self.state.server_transfer_policy()
    }

    pub fn set_nntp_pool(&self, pool: Arc<weaver_nntp::pool::NntpPool>) {
        self.state.set_nntp_pool(pool);
    }

    pub fn nntp_pool(&self) -> Option<Arc<weaver_nntp::pool::NntpPool>> {
        self.state.nntp_pool()
    }

    /// Replace the NNTP client at runtime (e.g. after server config changes).
    pub async fn rebuild_nntp<T: Send + 'static>(
        &self,
        client: T,
        total_connections: usize,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::RebuildNntp {
                client: Box::new(client),
                total_connections,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }

    /// Update the pipeline's runtime directories for future jobs.
    pub async fn update_runtime_paths(
        &self,
        data_dir: PathBuf,
        intermediate_dir: PathBuf,
        complete_dir: PathBuf,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::UpdateRuntimePaths {
                data_dir,
                intermediate_dir,
                complete_dir,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Replace the shared job snapshot without a pipeline command round-trip.
    pub fn replace_jobs_snapshot(&self, jobs: Vec<JobInfo>) {
        self.state.publish_jobs(jobs);
    }

    /// Subscribe to pipeline events.
    pub fn subscribe_events(&self) -> broadcast::Receiver<PipelineEvent> {
        self.event_tx.subscribe()
    }

    /// Signal shutdown.
    pub async fn shutdown(&self) -> Result<(), SchedulerError> {
        self.cmd_tx
            .send(SchedulerCommand::Shutdown)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
