use std::collections::HashMap;
use std::path::PathBuf;

use crate::events::model::PipelineEvent;
use crate::jobs::assembly::JobAssembly;
use crate::jobs::ids::JobId;

use super::*;
use crate::jobs::model::{FieldUpdate, JobSpec, JobState, JobStatus, JobUpdate, epoch_ms_now};
use crate::operations::metrics::PipelineMetrics;
use crate::pipeline::download::queue::DownloadQueue;

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
                        created_at_epoch_ms: epoch_ms_now(),
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
                        created_at_epoch_ms: epoch_ms_now(),
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
                SchedulerCommand::UpdateJob {
                    job_id,
                    update,
                    reply,
                } => {
                    let result = match jobs.get_mut(&job_id) {
                        Some(state) => {
                            update.apply_to_spec(&mut state.spec);
                            Ok(())
                        }
                        None => Err(SchedulerError::JobNotFound(job_id)),
                    };
                    let _ = reply.send(result);
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
async fn update_job_applies_explicit_patch() {
    let (handle, task) = test_scheduler();

    handle
        .add_job(JobId(1), make_spec("Patch Me"), PathBuf::from("test.nzb"))
        .await
        .unwrap();

    handle
        .update_job(
            JobId(1),
            JobUpdate {
                category: FieldUpdate::Set("movies".to_string()),
                metadata: FieldUpdate::Set(vec![("priority".to_string(), "high".to_string())]),
            },
        )
        .await
        .unwrap();

    let info = handle.get_job(JobId(1)).unwrap();
    assert_eq!(info.category.as_deref(), Some("movies"));
    assert_eq!(
        info.metadata,
        vec![("priority".to_string(), "high".to_string())]
    );

    handle
        .update_job(
            JobId(1),
            JobUpdate {
                category: FieldUpdate::Clear,
                metadata: FieldUpdate::Clear,
            },
        )
        .await
        .unwrap();

    let cleared = handle.get_job(JobId(1)).unwrap();
    assert!(cleared.category.is_none());
    assert!(cleared.metadata.is_empty());

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
