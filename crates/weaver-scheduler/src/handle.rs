use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, mpsc, oneshot};

use weaver_core::config::{IspBandwidthCapConfig, IspBandwidthCapPeriod};
use weaver_core::event::PipelineEvent;
use weaver_core::id::{JobId, SegmentId};

use crate::error::SchedulerError;
use crate::job::{JobSpec, JobStatus};
use crate::metrics::{MetricsSnapshot, PipelineMetrics};

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
        category: Option<Option<String>>,
        metadata: Option<Vec<(String, String)>>,
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
        action: weaver_core::config::ScheduleAction,
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
    pub async fn update_job(
        &self,
        job_id: JobId,
        category: Option<Option<String>>,
        metadata: Option<Vec<(String, String)>>,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::UpdateJob {
                job_id,
                category,
                metadata,
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
        action: weaver_core::config::ScheduleAction,
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
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use weaver_assembly::JobAssembly;
    use weaver_core::event::PipelineEvent;
    use weaver_core::id::JobId;

    use super::*;
    use crate::download_queue::DownloadQueue;
    use crate::job::{JobSpec, JobState, JobStatus};
    use crate::metrics::PipelineMetrics;

    fn build_job_list(jobs: &HashMap<JobId, JobState>) -> Vec<JobInfo> {
        jobs.values()
            .map(|state| JobInfo {
                job_id: state.job_id,
                name: state.spec.name.clone(),
                error: if let JobStatus::Failed { error } = &state.status {
                    Some(error.clone())
                } else {
                    None
                },
                status: state.status.clone(),
                progress: state.assembly.progress(),
                total_bytes: state.spec.total_bytes,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1000,
                password: state.spec.password.clone(),
                category: state.spec.category.clone(),
                metadata: state.spec.metadata.clone(),
                output_dir: None,
                created_at_epoch_ms: state.created_at_epoch_ms,
            })
            .collect()
    }

    /// Create a test scheduler handle with a minimal background loop.
    fn test_scheduler() -> (SchedulerHandle, tokio::task::JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);
        let metrics = PipelineMetrics::new();
        let shared_state = SharedPipelineState::new(metrics, vec![]);

        let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

        let task = tokio::spawn(async move {
            let mut jobs: HashMap<JobId, JobState> = HashMap::new();

            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    SchedulerCommand::AddJob {
                        job_id,
                        spec,
                        nzb_path: _,
                        reply,
                    } => {
                        if jobs.contains_key(&job_id) {
                            let _ = reply.send(Err(SchedulerError::JobExists(job_id)));
                            continue;
                        }
                        let assembly = JobAssembly::new(job_id);
                        let par2_bytes = spec.par2_bytes();
                        let state = JobState {
                            job_id,
                            spec,
                            status: JobStatus::Queued,
                            assembly,
                            extraction_depth: 0,
                            created_at: std::time::Instant::now(),
                            created_at_epoch_ms: crate::job::epoch_ms_now(),
                            queued_repair_at_epoch_ms: None,
                            queued_extract_at_epoch_ms: None,
                            working_dir: PathBuf::from("/tmp/test"),
                            downloaded_bytes: 0,
                            restored_download_floor_bytes: 0,
                            failed_bytes: 0,
                            par2_bytes,
                            health_probing: false,
                            last_health_probe_failed_bytes: 0,
                            held_segments: Vec::new(),
                            download_queue: DownloadQueue::new(),
                            recovery_queue: DownloadQueue::new(),
                            staging_dir: None,
                            paused_resume_status: None,
                        };
                        let _ = event_tx.send(PipelineEvent::JobCreated {
                            job_id,
                            name: state.spec.name.clone(),
                            total_files: state.spec.files.len() as u32,
                            total_bytes: state.spec.total_bytes,
                        });
                        jobs.insert(job_id, state);
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::PauseJob { job_id, reply } => {
                        let result = match jobs.get_mut(&job_id) {
                            Some(state) => {
                                state.status = JobStatus::Paused;
                                let _ = event_tx.send(PipelineEvent::JobPaused { job_id });
                                Ok(())
                            }
                            None => Err(SchedulerError::JobNotFound(job_id)),
                        };
                        let _ = reply.send(result);
                    }
                    SchedulerCommand::ResumeJob { job_id, reply } => {
                        let result = match jobs.get_mut(&job_id) {
                            Some(state) => {
                                state.status = JobStatus::Downloading;
                                let _ = event_tx.send(PipelineEvent::JobResumed { job_id });
                                Ok(())
                            }
                            None => Err(SchedulerError::JobNotFound(job_id)),
                        };
                        let _ = reply.send(result);
                    }
                    SchedulerCommand::CancelJob { job_id, reply } => {
                        let result = if jobs.remove(&job_id).is_some() {
                            Ok(())
                        } else {
                            Err(SchedulerError::JobNotFound(job_id))
                        };
                        let _ = reply.send(result);
                    }
                    SchedulerCommand::PauseAll { reply } => {
                        shared_state.set_paused(true);
                        let _ = reply.send(());
                    }
                    SchedulerCommand::ResumeAll { reply } => {
                        shared_state.set_paused(false);
                        let _ = reply.send(());
                    }
                    SchedulerCommand::SetSpeedLimit { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::SetBandwidthCapPolicy { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::RebuildNntp { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::UpdateRuntimePaths { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::ApplyScheduleAction { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::ClearScheduleAction { reply } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::RestoreJob { request, reply } => {
                        let RestoreJobRequest {
                            job_id,
                            spec,
                            status,
                            working_dir,
                            ..
                        } = *request;
                        let assembly = JobAssembly::new(job_id);
                        let par2_bytes = spec.par2_bytes();
                        let state = JobState {
                            job_id,
                            spec,
                            status,
                            assembly,
                            extraction_depth: 0,
                            created_at: std::time::Instant::now(),
                            created_at_epoch_ms: crate::job::epoch_ms_now(),
                            queued_repair_at_epoch_ms: None,
                            queued_extract_at_epoch_ms: None,
                            working_dir,
                            downloaded_bytes: 0,
                            restored_download_floor_bytes: 0,
                            failed_bytes: 0,
                            par2_bytes,
                            health_probing: false,
                            last_health_probe_failed_bytes: 0,
                            held_segments: Vec::new(),
                            download_queue: DownloadQueue::new(),
                            recovery_queue: DownloadQueue::new(),
                            staging_dir: None,
                            paused_resume_status: None,
                        };
                        jobs.insert(job_id, state);
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::DeleteHistory { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::DeleteAllHistory { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::ReprocessJob { job_id, reply } => {
                        let result = match jobs.get_mut(&job_id) {
                            Some(state) if matches!(state.status, JobStatus::Failed { .. }) => {
                                state.status = JobStatus::Downloading;
                                Ok(())
                            }
                            Some(_) => Err(SchedulerError::Conflict(format!(
                                "job {} is not failed",
                                job_id.0
                            ))),
                            None => Err(SchedulerError::JobNotFound(job_id)),
                        };
                        let _ = reply.send(result);
                    }
                    SchedulerCommand::UpdateJob { reply, .. } => {
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::Shutdown => break,
                }
                // Publish updated job list to shared state after every command.
                shared_state.publish_jobs(build_job_list(&jobs));
            }
        });

        (handle, task)
    }

    fn make_spec(name: &str) -> JobSpec {
        JobSpec {
            name: name.to_string(),
            password: None,
            files: vec![],
            total_bytes: 1_000_000,
            category: None,
            metadata: vec![],
        }
    }

    #[tokio::test]
    async fn add_and_list_jobs() {
        let (handle, task) = test_scheduler();

        handle
            .add_job(JobId(1), make_spec("Job 1"), PathBuf::from("test.nzb"))
            .await
            .unwrap();
        handle
            .add_job(JobId(2), make_spec("Job 2"), PathBuf::from("test2.nzb"))
            .await
            .unwrap();

        let jobs = handle.list_jobs();
        assert_eq!(jobs.len(), 2);

        let info = handle.get_job(JobId(1)).unwrap();
        assert_eq!(info.name, "Job 1");
        assert_eq!(info.status, JobStatus::Queued);

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn pause_resume() {
        let (handle, task) = test_scheduler();

        handle
            .add_job(JobId(1), make_spec("Test Job"), PathBuf::from("test.nzb"))
            .await
            .unwrap();

        handle.pause_job(JobId(1)).await.unwrap();
        let info = handle.get_job(JobId(1)).unwrap();
        assert_eq!(info.status, JobStatus::Paused);

        handle.resume_job(JobId(1)).await.unwrap();
        let info = handle.get_job(JobId(1)).unwrap();
        assert_eq!(info.status, JobStatus::Downloading);

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn cancel_job() {
        let (handle, task) = test_scheduler();

        handle
            .add_job(JobId(1), make_spec("To Cancel"), PathBuf::from("test.nzb"))
            .await
            .unwrap();
        assert_eq!(handle.list_jobs().len(), 1);

        handle.cancel_job(JobId(1)).await.unwrap();
        assert_eq!(handle.list_jobs().len(), 0);

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn job_not_found() {
        let (handle, task) = test_scheduler();

        let err = handle.get_job(JobId(999)).unwrap_err();
        assert!(matches!(err, SchedulerError::JobNotFound(_)));

        let err = handle.pause_job(JobId(999)).await.unwrap_err();
        assert!(matches!(err, SchedulerError::JobNotFound(_)));

        let err = handle.cancel_job(JobId(999)).await.unwrap_err();
        assert!(matches!(err, SchedulerError::JobNotFound(_)));

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn event_subscription() {
        let (handle, task) = test_scheduler();

        let mut rx = handle.subscribe_events();

        handle
            .add_job(
                JobId(1),
                make_spec("Evented Job"),
                PathBuf::from("test.nzb"),
            )
            .await
            .unwrap();

        let event = rx.recv().await.unwrap();
        match event {
            PipelineEvent::JobCreated { job_id, name, .. } => {
                assert_eq!(job_id, JobId(1));
                assert_eq!(name, "Evented Job");
            }
            other => panic!("expected JobCreated, got {other:?}"),
        }

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_job_rejected() {
        let (handle, task) = test_scheduler();

        handle
            .add_job(JobId(1), make_spec("First"), PathBuf::from("test.nzb"))
            .await
            .unwrap();
        let err = handle
            .add_job(JobId(1), make_spec("Duplicate"), PathBuf::from("test.nzb"))
            .await
            .unwrap_err();
        assert!(matches!(err, SchedulerError::JobExists(_)));

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn password_passthrough() {
        let (handle, task) = test_scheduler();

        let mut spec = make_spec("Protected");
        spec.password = Some("secret123".to_string());

        handle
            .add_job(JobId(1), spec, PathBuf::from("test.nzb"))
            .await
            .unwrap();

        let info = handle.get_job(JobId(1)).unwrap();
        assert_eq!(info.password, Some("secret123".to_string()));

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }
}
