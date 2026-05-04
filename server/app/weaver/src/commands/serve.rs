use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{error, info};

use crate::{http, shutdown, wiring};
use weaver_server_core::Database;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Pipeline, SchedulerCommand, SchedulerHandle};

pub(crate) async fn run(
    mut config: Config,
    db: Database,
    port: u16,
    base_url: &str,
    log_ring_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
) -> Result<(), Box<dyn std::error::Error>> {
    let data_dir = PathBuf::from(&config.data_dir);
    let intermediate_dir = PathBuf::from(config.intermediate_dir());
    let complete_dir = PathBuf::from(config.complete_dir());

    // Normalize: ensure leading slash, strip trailing slashes.
    let base_url = format!("/{}", base_url.trim_matches('/'));
    let base_url = if base_url == "/" {
        String::new()
    } else {
        base_url
    };

    let wiring::RuntimeContext {
        profile,
        buffers,
        write_buf_max,
    } = wiring::build_runtime_context(&data_dir);

    // Detect server capabilities (pipelining, etc.) and build NNTP client.
    wiring::detect_server_capabilities(&mut config, &db).await;
    let nntp = wiring::build_nntp_client(&config, &profile);
    let total_connections: usize = config
        .servers
        .iter()
        .filter(|server| server.active)
        .map(|server| server.connections as usize)
        .sum();

    // Wrap config for shared runtime access.
    let shared_config: SharedConfig = Arc::new(RwLock::new(config));

    // Set up scheduler channels and shared control-plane state.
    let (cmd_tx, cmd_rx) = mpsc::channel::<SchedulerCommand>(64);
    let (event_tx, _) = broadcast::channel::<PipelineEvent>(1024);
    let metrics = weaver_server_core::PipelineMetrics::new();
    // SharedPipelineState starts empty; initial_history is published below after recovery.
    let shared_state = weaver_server_core::SharedPipelineState::new(metrics, vec![]);
    let handle = SchedulerHandle::new(cmd_tx, event_tx.clone(), shared_state.clone());

    let recovered_state =
        weaver_server_core::operations::recover_server_state(&db, &data_dir, &intermediate_dir)
            .await?;
    let initial_history = recovered_state.initial_history;
    let to_restore = recovered_state.to_restore;
    let initial_global_paused = recovered_state.initial_global_paused;

    // Publish recovered history to shared state so the API has data immediately.
    shared_state.publish_jobs(initial_history.clone());

    let rss = weaver_server_api::RssService::new(handle.clone(), shared_config.clone(), db.clone());
    let backup = weaver_server_api::BackupService::new(
        handle.clone(),
        shared_config.clone(),
        db.clone(),
        rss.clone(),
    );
    let login_auth_cache =
        weaver_server_core::auth::LoginAuthCache::from_credentials(db.get_auth_credentials()?);
    let api_key_cache =
        weaver_server_core::auth::ApiKeyCache::from_rows(db.list_api_key_auth_rows()?);

    // Load schedules from DB and spawn the schedule evaluator.
    let shared_schedules: weaver_server_core::bandwidth::schedule::SharedSchedules = {
        let initial = db.list_schedules().unwrap_or_default();
        std::sync::Arc::new(tokio::sync::RwLock::new(initial))
    };
    weaver_server_core::bandwidth::schedule::spawn_evaluator(
        handle.clone(),
        shared_schedules.clone(),
    );

    // Build GraphQL schema with shared config and database.
    let pipeline_config = shared_config.clone();
    let schema = weaver_server_api::build_schema(weaver_server_api::SchemaContext {
        handle: handle.clone(),
        config: shared_config,
        db: db.clone(),
        auth_cache: login_auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        rss: rss.clone(),
        schedules: shared_schedules,
        log_buffer: log_ring_buffer,
    });

    // Spawn event persistence subscriber (records meaningful events to SQLite).
    {
        wiring::spawn_event_persistence_task(event_tx.subscribe(), db.clone());
    }

    // Create and start the pipeline.
    let mut pipeline = Pipeline::new(
        cmd_rx,
        event_tx,
        nntp,
        buffers,
        profile,
        data_dir,
        intermediate_dir,
        complete_dir,
        total_connections,
        write_buf_max,
        initial_history,
        initial_global_paused,
        shared_state,
        db.clone(),
        pipeline_config,
    )
    .await?;

    let nntp_pool = pipeline.nntp_pool();
    let metrics_exporter = http::PrometheusMetricsExporter::new(handle.clone(), nntp_pool);

    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });

    let rss_task = rss.start_background_loop();
    let metrics_history_task =
        shutdown::spawn_metrics_history_task(metrics_exporter.clone(), db.clone());

    // Run HTTP server.
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let server_runtime = http::ServerRuntime {
        schema,
        handle: handle.clone(),
        db: db.clone(),
        auth_cache: login_auth_cache,
        api_key_cache,
        backup,
        metrics_exporter,
        base_url,
    };
    let mut server_task = tokio::spawn(http::run_server(server_runtime, addr));

    // Restore in-progress jobs from SQLite after the listener is available so
    // long restore passes do not block process readiness.
    for candidate in to_restore {
        match handle.restore_job(candidate.request).await {
            Ok(()) => info!(
                job_id = candidate.job_id.0,
                committed_count = candidate.committed_count,
                "job restored"
            ),
            Err(e) => error!(job_id = candidate.job_id.0, error = %e, "failed to restore job"),
        }
    }

    tokio::select! {
        _ = shutdown::wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            server_task.abort();
            rss_task.abort();
            metrics_history_task.abort();
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = shutdown::pipeline_exit_error(result);
            server_task.abort();
            rss_task.abort();
            metrics_history_task.abort();
            Err(error.into())
        }
        result = &mut server_task => {
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during HTTP shutdown");
            }
            rss_task.abort();
            metrics_history_task.abort();
            match result {
                Ok(Ok(())) => Err("HTTP server exited unexpectedly".into()),
                Ok(Err(error)) => Err(error),
                Err(join_error) => Err(format!("HTTP server task failed: {join_error}").into()),
            }
        }
    }
}
