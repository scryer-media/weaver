use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::{RwLock, broadcast, mpsc};
use tracing::{error, info, warn};

use crate::{http, shutdown, wiring};
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::security::RuntimeSecurityConfig;
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Database, Pipeline, SchedulerCommand, SchedulerHandle};

pub(crate) async fn run(
    mut config: Config,
    db: Database,
    restore_locator_dir: PathBuf,
    port: u16,
    base_url: &str,
    log_ring_buffer: weaver_server_core::runtime::log_buffer::LogRingBuffer,
) -> Result<(), Box<dyn std::error::Error>> {
    let security = RuntimeSecurityConfig::from_env()?;
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
    let policy_db = db.clone();
    let policy_servers = config.servers.clone();
    let server_transfer_policy = Arc::new(
        tokio::task::spawn_blocking(move || {
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                policy_db,
                &policy_servers,
            )
        })
        .await??,
    );
    let server_transfer_maintenance = server_transfer_policy.spawn_maintenance();
    let nntp = wiring::build_nntp_client(&config, &profile, &server_transfer_policy);
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
    handle.set_server_transfer_policy(Arc::clone(&server_transfer_policy));
    handle.set_nntp_pool(Arc::clone(nntp.pool()));

    let recovered_state =
        weaver_server_core::operations::recover_server_state(&db, &data_dir, &intermediate_dir)
            .await?;
    let initial_history = recovered_state.initial_history;
    let to_restore = recovered_state.to_restore;
    let initial_global_paused = recovered_state.initial_global_paused;

    // Publish recovered history to shared state so the API has data immediately.
    shared_state.publish_jobs(initial_history.clone());

    let rss = weaver_server_api::RssService::new(handle.clone(), shared_config.clone(), db.clone());
    let watch_folder = weaver_server_core::watch_folder::WatchFolderService::new(
        db.clone(),
        handle.clone(),
        shared_config.clone(),
    );
    let backup = weaver_server_api::BackupService::new(
        handle.clone(),
        shared_config.clone(),
        db.clone(),
        rss.clone(),
        restore_locator_dir,
    );
    let jwt_secret = db.get_or_create_jwt_signing_secret()?;
    let auth_credentials = db.get_auth_credentials()?;
    let login_enabled = auth_credentials.is_some();
    if let Some(message) = security.strict_security_violation(login_enabled) {
        return Err(message.into());
    }
    if security.exposes_admin_without_login(login_enabled) {
        warn!(
            bind_address = %security.http_bind_address,
            "Weaver HTTP admin is bound beyond loopback without login protection"
        );
    }
    let login_auth_cache =
        weaver_server_core::auth::LoginAuthCache::from_credentials(auth_credentials, jwt_secret);
    let api_key_cache =
        weaver_server_core::auth::ApiKeyCache::from_rows(db.list_api_key_auth_rows()?);

    // Load schedules from DB and spawn the schedule evaluator.
    let shared_schedules: weaver_server_core::bandwidth::schedule::SharedSchedules = {
        let initial = db.list_schedules().unwrap_or_default();
        std::sync::Arc::new(tokio::sync::RwLock::new(initial))
    };
    weaver_server_core::bandwidth::schedule::spawn_evaluator_with_watch_folder(
        handle.clone(),
        shared_schedules.clone(),
        Some(watch_folder.clone()),
    );

    let pipeline_config = shared_config.clone();

    // Spawn event persistence subscriber (records meaningful events to SQLite).
    // Keep its handle and an explicit shutdown signal so the shutdown path can
    // await its final writer-queue flush deterministically: the broadcast
    // senders outlive the pipeline (held by `handle` and its service clones),
    // so the task never sees the channel close on its own.
    let event_persistence_shutdown = Arc::new(tokio::sync::Notify::new());
    let event_persistence_task = wiring::spawn_event_persistence_task(
        event_tx.subscribe(),
        db.clone(),
        event_persistence_shutdown.clone(),
    );

    // Create and start the pipeline.
    let maintenance_complete_dir = complete_dir.clone();
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
    let post_processing_service = pipeline.post_processing_service();
    let scheduled_resume =
        weaver_server_api::ScheduledResumeCoordinator::new(db.clone(), handle.clone());

    // Build the GraphQL schema now that the live NNTP pool exists (for server-health metrics).
    let schema = weaver_server_api::build_schema(weaver_server_api::SchemaContext {
        handle: handle.clone(),
        scheduled_resume: scheduled_resume.clone(),
        config: shared_config.clone(),
        db: db.clone(),
        server_transfer_policy: Arc::clone(&server_transfer_policy),
        auth_cache: login_auth_cache.clone(),
        api_key_cache: api_key_cache.clone(),
        rss: rss.clone(),
        watch_folder: watch_folder.clone(),
        schedules: shared_schedules,
        log_buffer: log_ring_buffer,
        nntp_pool: Some(nntp_pool.clone()),
        spawn_history_delete_worker: true,
        post_processing_service: Some(post_processing_service),
    });

    let metrics_exporter = http::PrometheusMetricsExporter::new(
        handle.clone(),
        db.clone(),
        nntp_pool,
        Arc::clone(&server_transfer_policy),
    );

    let mut pipeline_task = tokio::spawn(async move {
        pipeline.run().await;
    });
    scheduled_resume.recover().await?;

    let rss_task = rss.start_background_loop();
    watch_folder.reconcile_from_config().await?;
    let metrics_history_task = shutdown::spawn_metrics_history_task(handle.clone(), db.clone());
    let maintenance_task = weaver_server_core::operations::spawn_maintenance_worker(
        db.clone(),
        maintenance_complete_dir,
    );

    // Run HTTP server.
    let addr = SocketAddr::new(security.http_bind_address, port);
    let server_runtime = http::ServerRuntime {
        schema,
        handle: handle.clone(),
        scheduled_resume,
        db: db.clone(),
        auth_cache: login_auth_cache,
        api_key_cache,
        backup,
        rss: rss.clone(),
        watch_folder: watch_folder.clone(),
        metrics_exporter,
        config: shared_config.clone(),
        base_url,
        security,
    };
    let mut server_task = tokio::spawn(http::run_server(server_runtime, addr));

    // Restore in-progress jobs from SQLite after the listener is available so
    // long restore passes do not block process readiness.
    for candidate in to_restore {
        match handle.restore_job(candidate.request).await {
            Ok(()) => info!(job_id = candidate.job_id.0, "job restored"),
            Err(e) => error!(job_id = candidate.job_id.0, error = %e, "failed to restore job"),
        }
    }
    match weaver_server_core::ingest::reconcile_semantic_promotions(&db, &handle).await {
        Ok(count) if count > 0 => info!(count, "recovered semantic promotion claims"),
        Ok(_) => {}
        Err(error) => error!(error = %error, "failed to reconcile semantic promotion claims"),
    }
    match weaver_server_core::ingest::reconcile_duplicate_fingerprint_backfill(&db, 4).await {
        Ok(report) if report.scanned > 0 || report.skipped > 0 => info!(
            scanned = report.scanned,
            backfilled = report.backfilled,
            skipped = report.skipped,
            completed = report.completed,
            "ran bounded duplicate fingerprint backfill"
        ),
        Ok(_) => {}
        Err(error) => error!(error = %error, "failed to run duplicate fingerprint backfill"),
    }
    let semantic_promotion_task = {
        let db = db.clone();
        let handle = handle.clone();
        tokio::spawn(async move {
            let mut delay = std::time::Duration::from_secs(2);
            loop {
                tokio::time::sleep(delay).await;
                match weaver_server_core::ingest::reconcile_semantic_promotions(&db, &handle).await
                {
                    Ok(count) if count > 0 => delay = std::time::Duration::from_millis(100),
                    Ok(_) => delay = std::time::Duration::from_secs(2),
                    Err(error) => {
                        error!(error = %error, "semantic promotion worker retrying after failure");
                        delay = std::time::Duration::from_secs(5);
                    }
                }
            }
        })
    };

    tokio::select! {
        _ = shutdown::wait_for_shutdown() => {
            info!("received shutdown signal, shutting down");
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during shutdown");
            }
            finalize_event_persistence(event_persistence_task, &event_persistence_shutdown).await;
            server_task.abort();
            rss_task.abort();
            watch_folder.stop().await;
            metrics_history_task.abort();
            maintenance_task.abort();
            semantic_promotion_task.abort();
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                Arc::clone(&server_transfer_policy),
                "serve shutdown",
            )
            .await;
            Ok(())
        }
        result = &mut pipeline_task => {
            let error = shutdown::pipeline_exit_error(result);
            finalize_event_persistence(event_persistence_task, &event_persistence_shutdown).await;
            server_task.abort();
            rss_task.abort();
            watch_folder.stop().await;
            metrics_history_task.abort();
            maintenance_task.abort();
            semantic_promotion_task.abort();
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                Arc::clone(&server_transfer_policy),
                "serve pipeline exit",
            )
            .await;
            Err(error.into())
        }
        result = &mut server_task => {
            handle.shutdown().await.ok();
            if let Err(join_error) = pipeline_task.await {
                error!(error = %join_error, "pipeline task failed during HTTP shutdown");
            }
            finalize_event_persistence(event_persistence_task, &event_persistence_shutdown).await;
            rss_task.abort();
            watch_folder.stop().await;
            metrics_history_task.abort();
            maintenance_task.abort();
            semantic_promotion_task.abort();
            server_transfer_maintenance.abort();
            wiring::flush_server_transfer_usage(
                Arc::clone(&server_transfer_policy),
                "serve HTTP exit",
            )
            .await;
            match result {
                Ok(Ok(())) => Err("HTTP server exited unexpectedly".into()),
                Ok(Err(error)) => Err(error),
                Err(join_error) => Err(format!("HTTP server task failed: {join_error}").into()),
            }
        }
    }
}

/// Signal the event-persistence task to stop and await its final
/// `flush_write_queue`, bounded so a stuck flush cannot hang process exit.
/// Called after the pipeline task has completed (its broadcast sender dropped),
/// so the only remaining reason the task is still running is the long-lived
/// senders held by `SchedulerHandle` and its service clones.
async fn finalize_event_persistence(
    task: tokio::task::JoinHandle<()>,
    shutdown: &Arc<tokio::sync::Notify>,
) {
    const EVENT_PERSISTENCE_FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
    // `notify_one` (not `notify_waiters`) stores a permit if the task is mid-batch
    // rather than parked on `notified()`, so the shutdown signal cannot be lost.
    shutdown.notify_one();
    match tokio::time::timeout(EVENT_PERSISTENCE_FLUSH_TIMEOUT, task).await {
        Ok(Ok(())) => {}
        Ok(Err(join_error)) => {
            error!(error = %join_error, "event persistence task failed during shutdown");
        }
        Err(_) => {
            warn!(
                timeout_secs = EVENT_PERSISTENCE_FLUSH_TIMEOUT.as_secs(),
                "timed out flushing event persistence during shutdown"
            );
        }
    }
}
