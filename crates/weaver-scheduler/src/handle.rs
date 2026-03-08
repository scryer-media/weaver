use std::any::Any;
use std::collections::HashSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};

use weaver_core::event::PipelineEvent;
use weaver_core::id::{JobId, SegmentId};

use crate::error::SchedulerError;
use crate::job::{JobSpec, JobStatus};
use crate::metrics::MetricsSnapshot;

/// Commands sent to the scheduler's main loop.
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
        job_id: JobId,
        spec: JobSpec,
        committed_segments: HashSet<SegmentId>,
        status: JobStatus,
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
    /// Get job status.
    GetJobStatus {
        job_id: JobId,
        reply: oneshot::Sender<Result<JobInfo, SchedulerError>>,
    },
    /// List all jobs.
    ListJobs {
        reply: oneshot::Sender<Vec<JobInfo>>,
    },
    /// Get current metrics snapshot.
    GetMetrics {
        reply: oneshot::Sender<MetricsSnapshot>,
    },
    /// Pause all jobs globally (pipeline-wide).
    PauseAll { reply: oneshot::Sender<()> },
    /// Resume all jobs globally.
    ResumeAll { reply: oneshot::Sender<()> },
    /// Query global pause state.
    GetGlobalPauseState { reply: oneshot::Sender<bool> },
    /// Set global speed limit (bytes/sec). 0 means unlimited.
    SetSpeedLimit {
        bytes_per_sec: u64,
        reply: oneshot::Sender<()>,
    },
    /// Replace the NNTP client at runtime (hot-reload after server config change).
    /// The client is boxed as `dyn Any` to avoid coupling weaver-scheduler to weaver-nntp.
    RebuildNntp {
        client: Box<dyn Any + Send>,
        total_connections: usize,
        reply: oneshot::Sender<()>,
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
}

/// Handle for sending commands to the scheduler.
/// Cloneable, can be shared across tasks.
#[derive(Clone)]
pub struct SchedulerHandle {
    cmd_tx: mpsc::Sender<SchedulerCommand>,
    event_tx: broadcast::Sender<PipelineEvent>,
}

impl SchedulerHandle {
    /// Create a new handle from a command sender and event broadcast sender.
    pub fn new(
        cmd_tx: mpsc::Sender<SchedulerCommand>,
        event_tx: broadcast::Sender<PipelineEvent>,
    ) -> Self {
        Self { cmd_tx, event_tx }
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
    pub async fn restore_job(
        &self,
        job_id: JobId,
        spec: JobSpec,
        committed_segments: HashSet<SegmentId>,
        status: JobStatus,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::RestoreJob {
                job_id,
                spec,
                committed_segments,
                status,
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
            .send(SchedulerCommand::PauseJob {
                job_id,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Resume a paused job.
    pub async fn resume_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ResumeJob {
                job_id,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Cancel a job.
    pub async fn cancel_job(&self, job_id: JobId) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::CancelJob {
                job_id,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// Get info about a specific job.
    pub async fn get_job(&self, job_id: JobId) -> Result<JobInfo, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::GetJobStatus {
                job_id,
                reply: tx,
            })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        rx.await.map_err(|_| SchedulerError::ChannelClosed)?
    }

    /// List all jobs.
    pub async fn list_jobs(&self) -> Result<Vec<JobInfo>, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::ListJobs { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(rx.await.map_err(|_| SchedulerError::ChannelClosed)?)
    }

    /// Get current metrics.
    pub async fn get_metrics(&self) -> Result<MetricsSnapshot, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::GetMetrics { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(rx.await.map_err(|_| SchedulerError::ChannelClosed)?)
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

    /// Check whether the pipeline is globally paused.
    pub async fn is_globally_paused(&self) -> Result<bool, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(SchedulerCommand::GetGlobalPauseState { reply: tx })
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;
        Ok(rx.await.map_err(|_| SchedulerError::ChannelClosed)?)
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    use weaver_assembly::JobAssembly;
    use weaver_core::event::PipelineEvent;
    use weaver_core::id::JobId;

    use super::*;
    use crate::download_queue::DownloadQueue;
    use crate::job::{JobSpec, JobState, JobStatus};
    use crate::metrics::PipelineMetrics;

    /// Create a test scheduler handle with a minimal background loop.
    fn test_scheduler() -> (SchedulerHandle, tokio::task::JoinHandle<()>) {
        let (cmd_tx, mut cmd_rx) = mpsc::channel(64);
        let (event_tx, _) = broadcast::channel(256);

        let handle = SchedulerHandle::new(cmd_tx, event_tx.clone());
        let metrics = PipelineMetrics::new();
        let downloaded_bytes = Arc::new(AtomicU64::new(0));

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
                        let state = JobState {
                            job_id,
                            spec,
                            status: JobStatus::Queued,
                            assembly,
                            created_at: std::time::Instant::now(),
                            downloaded_bytes: 0,
                            failed_bytes: 0,
                            health_probing: false,
                            held_segments: Vec::new(),
                            download_queue: DownloadQueue::new(),
                            recovery_queue: DownloadQueue::new(),
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
                    SchedulerCommand::GetJobStatus { job_id, reply } => {
                        let result = match jobs.get(&job_id) {
                            Some(state) => Ok(JobInfo {
                                job_id: state.job_id,
                                name: state.spec.name.clone(),
                                status: state.status.clone(),
                                progress: state.assembly.progress(),
                                total_bytes: state.spec.total_bytes,
                                downloaded_bytes: downloaded_bytes.load(Ordering::Relaxed),
                                failed_bytes: 0,
                                health: 1000,
                                password: state.spec.password.clone(),
                                category: state.spec.category.clone(),
                                metadata: state.spec.metadata.clone(),
                                output_dir: None,
                            }),
                            None => Err(SchedulerError::JobNotFound(job_id)),
                        };
                        let _ = reply.send(result);
                    }
                    SchedulerCommand::ListJobs { reply } => {
                        let list: Vec<JobInfo> = jobs
                            .values()
                            .map(|state| JobInfo {
                                job_id: state.job_id,
                                name: state.spec.name.clone(),
                                status: state.status.clone(),
                                progress: state.assembly.progress(),
                                total_bytes: state.spec.total_bytes,
                                downloaded_bytes: 0,
                                failed_bytes: 0,
                                health: 1000,
                                password: state.spec.password.clone(),
                                category: state.spec.category.clone(),
                                metadata: state.spec.metadata.clone(),
                                output_dir: None,
                            })
                            .collect();
                        let _ = reply.send(list);
                    }
                    SchedulerCommand::GetMetrics { reply } => {
                        let _ = reply.send(metrics.snapshot());
                    }
                    SchedulerCommand::PauseAll { reply } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::ResumeAll { reply } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::GetGlobalPauseState { reply } => {
                        let _ = reply.send(false);
                    }
                    SchedulerCommand::SetSpeedLimit { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::RebuildNntp { reply, .. } => {
                        let _ = reply.send(());
                    }
                    SchedulerCommand::RestoreJob { job_id, spec, status, reply, .. } => {
                        let assembly = JobAssembly::new(job_id);
                        let state = JobState {
                            job_id,
                            spec,
                            status,
                            assembly,
                            created_at: std::time::Instant::now(),
                            downloaded_bytes: 0,
                            failed_bytes: 0,
                            health_probing: false,
                            held_segments: Vec::new(),
                            download_queue: DownloadQueue::new(),
                            recovery_queue: DownloadQueue::new(),
                        };
                        jobs.insert(job_id, state);
                        let _ = reply.send(Ok(()));
                    }
                    SchedulerCommand::Shutdown => break,
                }
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

        let jobs = handle.list_jobs().await.unwrap();
        assert_eq!(jobs.len(), 2);

        let info = handle.get_job(JobId(1)).await.unwrap();
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
        let info = handle.get_job(JobId(1)).await.unwrap();
        assert_eq!(info.status, JobStatus::Paused);

        handle.resume_job(JobId(1)).await.unwrap();
        let info = handle.get_job(JobId(1)).await.unwrap();
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
        assert_eq!(handle.list_jobs().await.unwrap().len(), 1);

        handle.cancel_job(JobId(1)).await.unwrap();
        assert_eq!(handle.list_jobs().await.unwrap().len(), 0);

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn job_not_found() {
        let (handle, task) = test_scheduler();

        let err = handle.get_job(JobId(999)).await.unwrap_err();
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
            .add_job(JobId(1), make_spec("Evented Job"), PathBuf::from("test.nzb"))
            .await
            .unwrap();

        let event = rx.recv().await.unwrap();
        match event {
            PipelineEvent::JobCreated {
                job_id, name, ..
            } => {
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

        handle.add_job(JobId(1), spec, PathBuf::from("test.nzb")).await.unwrap();

        let info = handle.get_job(JobId(1)).await.unwrap();
        assert_eq!(info.password, Some("secret123".to_string()));

        handle.shutdown().await.unwrap();
        task.await.unwrap();
    }
}
