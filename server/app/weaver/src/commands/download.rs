use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};

use crate::{shutdown, wiring};
use weaver_server_core::Database;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::ingest::{self, SubmissionOptions, SubmitNzbError};
use weaver_server_core::jobs::{DuplicateMode, JobId, JobStatus, SubmissionOrigin};
use weaver_server_core::settings::Config;
use weaver_server_core::{Pipeline, SchedulerCommand, SchedulerHandle};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    config: &mut Config,
    db: &Database,
    nzb_path: &Path,
    output: Option<&Path>,
    password: Option<&str>,
    report_path: Option<&Path>,
    report_ack_path: Option<&Path>,
    force: bool,
    data_dir: &Path,
    intermediate_dir: &Path,
    complete_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if report_ack_path.is_some() && report_path.is_none() {
        return Err("--report-ack requires --report".into());
    }
    let effective_intermediate_dir = output.unwrap_or(intermediate_dir);

    let nzb_bytes = std::fs::read(nzb_path)?;
    info!(path = %nzb_path.display(), force, "starting standalone NZB submission");

    let wiring::RuntimeContext {
        profile,
        buffers,
        write_buf_max,
    } = wiring::build_runtime_context(data_dir);

    // Detect server capabilities (pipelining, etc.) and build NNTP client.
    wiring::detect_server_capabilities(config, db).await;
    let policy_db = db.clone();
    let policy_servers = config.servers.clone();
    let server_transfer_policy = std::sync::Arc::new(
        tokio::task::spawn_blocking(move || {
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                policy_db,
                &policy_servers,
            )
        })
        .await??,
    );
    let server_transfer_maintenance = server_transfer_policy.spawn_maintenance();
    let nntp = wiring::build_nntp_client(config, &profile, &server_transfer_policy);
    let initial_global_paused = weaver_server_core::runtime::load_global_pause_from_db(db).await?;

    // Set up scheduler channels and shared control-plane state.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let mut completion_rx = event_tx.subscribe();
    let metrics = weaver_server_core::PipelineMetrics::new();
    let shared_state = weaver_server_core::SharedPipelineState::new(metrics, vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());
    handle.set_server_transfer_policy(std::sync::Arc::clone(&server_transfer_policy));
    handle.set_nntp_pool(std::sync::Arc::clone(nntp.pool()));

    // Subscribe to events for progress logging.
    let mut event_rx = event_tx.subscribe();
    let log_task = tokio::spawn(async move {
        while let Ok(event) = event_rx.recv().await {
            match &event {
                PipelineEvent::JobCreated {
                    name,
                    total_files,
                    total_bytes,
                    ..
                } => {
                    info!(
                        name,
                        files = total_files,
                        bytes = total_bytes,
                        "job created"
                    );
                }
                PipelineEvent::FileComplete {
                    filename,
                    total_bytes,
                    ..
                } => {
                    info!(filename, bytes = total_bytes, "file complete");
                }
                PipelineEvent::JobCompleted { job_id, .. } => {
                    info!(job_id = job_id.0, "job completed");
                }
                PipelineEvent::JobFailed { job_id, error, .. } => {
                    error!(job_id = job_id.0, error, "job failed");
                }
                _ => {}
            }
        }
    });

    // Create and start the pipeline.
    let total_connections: usize = config
        .servers
        .iter()
        .map(|server| server.connections as usize)
        .sum();
    let standalone_config: weaver_server_core::settings::SharedConfig =
        std::sync::Arc::new(tokio::sync::RwLock::new(config.clone()));
    let submission_config = standalone_config.clone();
    let mut pipeline = Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir.to_path_buf(),
        effective_intermediate_dir.to_path_buf(),
        complete_dir.to_path_buf(),
        total_connections,
        write_buf_max,
        vec![],
        initial_global_paused,
        shared_state,
        db.clone(),
        standalone_config,
    )
    .await?;

    // Start the pipeline BEFORE submitting the job; add_job awaits a reply
    // from the pipeline loop, so the loop must be running first.
    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    let submitted = match ingest::submit_nzb_bytes_with_options(
        db,
        &handle,
        &submission_config,
        &nzb_bytes,
        nzb_path
            .file_name()
            .and_then(|value| value.to_str())
            .map(str::to_string),
        password.map(str::to_owned),
        None,
        vec![("source".to_string(), "cli".to_string())],
        SubmissionOptions {
            duplicate_mode: if force {
                DuplicateMode::Force
            } else {
                DuplicateMode::Enforce
            },
            origin: SubmissionOrigin::Cli,
            ..SubmissionOptions::default()
        },
    )
    .await
    {
        Ok(submitted) => {
            info!(
                job_id = submitted.job_id.0,
                job = %submitted.summary.name,
                files = submitted.summary.file_count,
                bytes = submitted.summary.total_bytes,
                "submitted standalone NZB job"
            );
            submitted
        }
        Err(error @ SubmitNzbError::DuplicateBlocked { .. })
        | Err(error @ SubmitNzbError::IdempotencyConflict { .. }) => {
            return Err(format!("submission rejected: {error}").into());
        }
        Err(error) => return Err(error.into()),
    };
    let queued_at = timestamp_now();
    let job_id = submitted.job_id;

    tokio::select! {
        terminal = wait_for_job_terminal(&mut completion_rx, &handle, job_id) => {
            let completed_at = timestamp_now();
            let report_result = match terminal {
                Ok(()) => {
                    if let Some(report_path) = report_path {
                        match write_report(report_path, job_id, &queued_at, &completed_at) {
                            Ok(()) => match report_ack_path {
                                Some(report_ack_path) => wait_for_report_ack(report_ack_path).await,
                                None => Ok(()),
                            },
                            Err(error) => Err(error),
                        }
                    } else {
                        Ok(())
                    }
                }
                Err(error) => Err(std::io::Error::other(error)),
            };
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed after terminal job status");
            }
            flush_writer_queue_on_exit(db).await;
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                std::sync::Arc::clone(&server_transfer_policy),
                "download command terminal job status",
            )
            .await;
            log_task.abort();
            report_result?;
            Ok(())
        }
        _ = shutdown::wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            flush_writer_queue_on_exit(db).await;
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                std::sync::Arc::clone(&server_transfer_policy),
                "download command shutdown",
            )
            .await;
            log_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = shutdown::pipeline_exit_error(result);
            flush_writer_queue_on_exit(db).await;
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                std::sync::Arc::clone(&server_transfer_policy),
                "download command pipeline exit",
            )
            .await;
            log_task.abort();
            Err(error.into())
        }
    }
}

// DownloadReport is deliberately small and machine-readable so one-shot
// callers can time the same queue-acceptance-to-completion boundary as API
// clients without starting Weaver's HTTP server.
#[derive(Serialize)]
struct DownloadReport<'a> {
    schema_version: u32,
    job_id: u64,
    queued_at: &'a str,
    completion_at: &'a str,
    status: &'static str,
}

fn timestamp_now() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn write_report(
    path: &Path,
    job_id: JobId,
    queued_at: &str,
    completion_at: &str,
) -> std::io::Result<()> {
    let report = DownloadReport {
        schema_version: 1,
        job_id: job_id.0,
        queued_at,
        completion_at,
        status: "complete",
    };
    let contents = serde_json::to_vec_pretty(&report).map_err(std::io::Error::other)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&contents)?;
    file.write_all(b"\n")?;
    file.sync_all()
}

async fn wait_for_report_ack(path: &Path) -> std::io::Result<()> {
    const REPORT_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
    const REPORT_ACK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(25);

    match tokio::time::timeout(REPORT_ACK_TIMEOUT, async {
        loop {
            match tokio::fs::metadata(path).await {
                Ok(metadata) if metadata.is_file() => return Ok(()),
                Ok(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!(
                            "report acknowledgement {} is not a regular file",
                            path.display()
                        ),
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
            tokio::time::sleep(REPORT_ACK_POLL_INTERVAL).await;
        }
    })
    .await
    {
        Ok(result) => result,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            format!(
                "timed out waiting for report acknowledgement {}",
                path.display()
            ),
        )),
    }
}

async fn wait_for_job_terminal(
    events: &mut broadcast::Receiver<PipelineEvent>,
    handle: &SchedulerHandle,
    job_id: JobId,
) -> Result<(), String> {
    let mut status_tick = tokio::time::interval(std::time::Duration::from_millis(250));
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(PipelineEvent::JobCompleted { job_id: completed }) if completed == job_id => return Ok(()),
                Ok(PipelineEvent::JobFailed { job_id: failed, error }) if failed == job_id => return Err(error),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(count)) => {
                    tracing::warn!(job_id = job_id.0, count, "standalone download event receiver lagged; checking job status");
                }
                Err(broadcast::error::RecvError::Closed) => return Err("pipeline event stream closed before the job reached a terminal status".to_string()),
            },
            _ = status_tick.tick() => match handle.get_job(job_id) {
                Ok(job) => match &job.status {
                    JobStatus::Complete => return Ok(()),
                    JobStatus::Failed { error } => return Err(error.clone()),
                    _ => {}
                },
                Err(error) => return Err(format!("could not read standalone job status: {error}")),
            },
        }
    }
}

/// Drain the database writer queue before the standalone `download` command
/// exits. The pipeline it runs enqueues durable writes onto that queue
/// (job-history archival via `try_queue_archive_job`, active-runtime state via
/// `try_queue_write`); unlike `serve`, this path has no event-persistence task
/// to run the final flush, so it must flush here or those writes can be dropped
/// at process exit. Bounded so a stuck flush cannot hang the CLI.
async fn flush_writer_queue_on_exit(db: &Database) {
    const WRITER_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    match tokio::time::timeout(WRITER_FLUSH_TIMEOUT, db.flush_write_queue()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "failed to flush database writer queue on exit");
        }
        Err(_) => {
            tracing::warn!(
                timeout_secs = WRITER_FLUSH_TIMEOUT.as_secs(),
                "timed out flushing database writer queue on exit"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobId, wait_for_report_ack, write_report};

    #[test]
    fn report_is_complete_and_never_overwrites_existing_evidence() {
        let directory = tempfile::tempdir().expect("temporary report directory");
        let path = directory.path().join("result.json");

        write_report(
            &path,
            JobId(42),
            "2026-08-03T00:00:00.000000000Z",
            "2026-08-03T00:00:01.000000000Z",
        )
        .expect("write report");
        let contents = std::fs::read_to_string(&path).expect("read report");
        assert!(contents.contains("\"schema_version\": 1"));
        assert!(contents.contains("\"job_id\": 42"));
        assert!(contents.contains("\"status\": \"complete\""));
        assert!(write_report(&path, JobId(43), "a", "b").is_err());
    }

    #[tokio::test]
    async fn report_ack_accepts_a_regular_file() {
        let directory = tempfile::tempdir().expect("temporary acknowledgement directory");
        let path = directory.path().join("result.ack");
        std::fs::write(&path, []).expect("write acknowledgement");

        wait_for_report_ack(&path)
            .await
            .expect("accept acknowledgement file");
    }
}
