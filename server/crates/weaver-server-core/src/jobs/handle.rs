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
use crate::jobs::ids::{JobId, SegmentId};
use crate::jobs::record::ActiveFileIdentity;

use crate::jobs::error::SchedulerError;
use crate::jobs::model::{JobSpec, JobStatus, JobUpdate};
use crate::operations::metrics::{MetricsSnapshot, PipelineMetrics};

/// Shared read-only view of pipeline state for the control plane.
///
/// Written by the pipeline loop after each event, read by API handlers
/// without going through the command channel.
#[derive(Clone)]
pub struct SharedPipelineState {
    jobs: Arc<RwLock<Vec<JobInfo>>>,
    paused: Arc<AtomicBool>,
    metrics: Arc<PipelineMetrics>,
    download_block: Arc<RwLock<DownloadBlockState>>,
}

impl SharedPipelineState {
    pub fn new(metrics: Arc<PipelineMetrics>, initial_jobs: Vec<JobInfo>) -> Self {
        Self {
            jobs: Arc::new(RwLock::new(initial_jobs)),
            paused: Arc::new(AtomicBool::new(false)),
            metrics,
            download_block: Arc::new(RwLock::new(DownloadBlockState::default())),
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

    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
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
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Relaxed);
    }

    pub fn set_download_block(&self, state: DownloadBlockState) {
        *self.download_block.write().unwrap() = state;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadBlockKind {
    None,
    ManualPause,
    Scheduled,
    IspCap,
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
    pub spec: JobSpec,
    pub committed_segments: HashSet<SegmentId>,
    pub file_progress: HashMap<u32, u64>,
    pub detected_archives: HashMap<u32, DetectedArchiveIdentity>,
    pub file_identities: HashMap<u32, ActiveFileIdentity>,
    pub extracted_members: HashSet<String>,
    pub status: JobStatus,
    pub queued_repair_at_epoch_ms: Option<f64>,
    pub queued_extract_at_epoch_ms: Option<f64>,
    pub paused_resume_status: Option<JobStatus>,
    pub working_dir: PathBuf,
}

pub enum SchedulerCommand {
    /// Submit a new job.
    AddJob {
        job_id: JobId,
        spec: JobSpec,
        nzb_path: PathBuf,
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
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Update a job's category and/or metadata.
    UpdateJob {
        job_id: JobId,
        update: JobUpdate,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Pause all jobs globally (pipeline-wide).
    PauseAll { reply: oneshot::Sender<()> },
    /// Resume all jobs globally.
    ResumeAll { reply: oneshot::Sender<()> },
    /// Set global speed limit (bytes/sec). 0 means unlimited.
    SetSpeedLimit {
        bytes_per_sec: u64,
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
    /// Reprocess a failed job (re-run post-download stages without re-downloading).
    ReprocessJob {
        job_id: JobId,
        reply: oneshot::Sender<Result<(), SchedulerError>>,
    },
    /// Re-download a failed job from its persisted NZB under the same job ID.
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
    pub name: String,
    pub status: JobStatus,
    pub progress: f64,
    pub total_bytes: u64,
    pub downloaded_bytes: u64,
    pub optional_recovery_bytes: u64,
    pub optional_recovery_downloaded_bytes: u64,
    /// Bytes from segments that are permanently lost (430 / max retries).
    pub failed_bytes: u64,
    /// Job health 0-1000 (1000 = perfect). Drops as articles fail.
    pub health: u32,
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
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::AddJob {
                job_id,
                spec,
                nzb_path,
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
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::CancelJob { job_id, reply: tx })
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

    /// Reprocess a failed job (re-run post-download stages without re-downloading).
    pub async fn reprocess_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ReprocessJob { job_id, reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Re-download a failed job from its persisted NZB under the same job ID.
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

    /// Check whether the pipeline is globally paused (reads from shared state).
    pub fn is_globally_paused(&self) -> bool {
        self.state.is_paused()
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
