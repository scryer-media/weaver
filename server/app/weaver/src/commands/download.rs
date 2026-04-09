use std::path::{Path, PathBuf};

use tokio::sync::{broadcast, mpsc};
use tracing::{error, info};

use crate::{shutdown, wiring};
use weaver_server_core::Database;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::ingest;
use weaver_server_core::settings::Config;
use weaver_server_core::{Pipeline, SchedulerCommand, SchedulerHandle};

pub(crate) async fn run(
    config: &mut Config,
    db: &Database,
    nzb_path: &Path,
    output: Option<&Path>,
    data_dir: &Path,
    intermediate_dir: &Path,
    complete_dir: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let effective_intermediate_dir = output.unwrap_or(intermediate_dir);

    // Read and parse NZB.
    let nzb_bytes = std::fs::read(nzb_path)?;
    let (job_id, job_spec) = ingest::import_nzb(&nzb_bytes, nzb_path)?;

    info!(
        job = %job_spec.name,
        files = job_spec.files.len(),
        bytes = job_spec.total_bytes,
        "starting download"
    );

    let wiring::RuntimeContext {
        profile,
        buffers,
        write_buf_max,
    } = wiring::build_runtime_context(data_dir);

    // Detect server capabilities (pipelining, etc.) and build NNTP client.
    wiring::detect_server_capabilities(config, db).await;
    let nntp = wiring::build_nntp_client(config, &profile);
    let initial_global_paused = weaver_server_core::runtime::load_global_pause_from_db(db).await?;

    // Set up scheduler channels and shared control-plane state.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let metrics = weaver_server_core::PipelineMetrics::new();
    let shared_state = weaver_server_core::SharedPipelineState::new(metrics, vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

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

    // Submit the job via the handle.
    handle
        .add_job(job_id, job_spec, PathBuf::from(nzb_path))
        .await?;

    tokio::select! {
        _ = shutdown::wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            log_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = shutdown::pipeline_exit_error(result);
            log_task.abort();
            Err(error.into())
        }
    }
}
