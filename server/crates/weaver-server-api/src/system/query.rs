use super::*;
use crate::observability::with_timed_config_read;
use crate::system::metrics_history::{build_metrics_history, tier_for_range};
use crate::system::types::MetricsHistoryRangeGql;
use std::sync::Arc;
use weaver_nntp::pool::NntpPool;

#[derive(Default)]
pub(crate) struct SystemQuery;

#[Object]
impl SystemQuery {
    /// The running weaver binary version.
    async fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    /// System status facade for integrations.
    #[graphql(guard = "ReadGuard")]
    async fn system_status(&self, ctx: &Context<'_>) -> Result<SystemStatus> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        let items: Vec<QueueItem> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .collect();
        let metrics = handle.get_metrics();
        let max_download_speed = with_timed_config_read(
            config,
            "system.query.system_status.max_download_speed",
            |cfg| cfg.max_download_speed.unwrap_or(0),
        )
        .await;
        let global_state = global_queue_state(
            handle.is_globally_paused(),
            &handle.get_download_block(),
            max_download_speed,
        );
        Ok(SystemStatus {
            version: env!("CARGO_PKG_VERSION").to_string(),
            global_state,
            summary: queue_summary(&items, &metrics),
        })
    }
    /// System metrics facade for integrations.
    #[graphql(guard = "ReadGuard")]
    async fn system_metrics(&self, ctx: &Context<'_>) -> Result<Metrics> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(metrics_from_snapshot(&handle.get_metrics()))
    }
    /// Tiered local metrics history for the built-in monitoring UI.
    #[graphql(guard = "ReadGuard")]
    async fn metrics_history(
        &self,
        ctx: &Context<'_>,
        range: MetricsHistoryRangeGql,
    ) -> Result<MetricsHistoryResult> {
        let db = ctx.data::<Database>()?.clone();
        let now_epoch_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let since_epoch_sec = now_epoch_sec - range.window_sec();
        let tier = tier_for_range(range);

        tokio::task::spawn_blocking(move || {
            let history = db
                .read_metrics_history(tier, since_epoch_sec, now_epoch_sec)
                .map_err(|error| error.to_string())?;
            build_metrics_history(history)
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error))
    }
    #[graphql(guard = "AdminGuard")]
    async fn browse_directories(
        &self,
        ctx: &Context<'_>,
        path: Option<String>,
    ) -> Result<DirectoryBrowseResult> {
        let config = ctx.data::<SharedConfig>()?;
        let default_path = with_timed_config_read(
            config,
            "system.query.browse_directories.default_path",
            |cfg| cfg.complete_dir(),
        )
        .await;
        let explicit_path = path
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let requested_path = if let Some(path) = explicit_path {
            std::path::PathBuf::from(path)
        } else {
            absolutize_default_browse_path(default_path)
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        };

        let listing = tokio::task::spawn_blocking(move || {
            weaver_server_core::operations::browse_directories(&requested_path)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|error| match error {
            weaver_server_core::operations::BrowseDirectoryError::InvalidInput(message) => {
                graphql_error("INVALID_INPUT", message)
            }
            weaver_server_core::operations::BrowseDirectoryError::Internal(message) => {
                graphql_error("INTERNAL", message)
            }
        })?;

        Ok(listing.into())
    }
    /// Return recent log lines from the in-memory ring buffer.
    #[graphql(guard = "AdminGuard")]
    async fn service_logs(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 250)] limit: i32,
    ) -> Result<ServiceLogsPayload> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let lines = weaver_server_core::operations::snapshot_service_logs(buffer, limit);
        let count = lines.len() as i32;
        Ok(ServiceLogsPayload { lines, count })
    }
    /// Get current pipeline metrics.
    async fn metrics(&self, ctx: &Context<'_>) -> Result<Metrics> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let snapshot = handle.get_metrics();
        Ok(Metrics::from(&snapshot))
    }
    /// Check whether the pipeline is globally paused.
    async fn is_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(handle.is_globally_paused())
    }
    /// Current global download block state (manual pause or ISP cap).
    async fn download_block(&self, ctx: &Context<'_>) -> Result<DownloadBlock> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(DownloadBlock::from(&handle.get_download_block()))
    }

    /// Live per-server NNTP health (connections, latency, state) for the monitoring dashboard.
    #[graphql(guard = "ReadGuard")]
    async fn server_health(&self, ctx: &Context<'_>) -> Result<Vec<ServerHealth>> {
        let handle = ctx.data::<weaver_server_core::SchedulerHandle>()?;
        let live_pool = handle.nntp_pool();
        let runtime_generation = handle
            .nntp_runtime_activation()
            .map(|activation| activation.generation)
            .unwrap_or(0);
        let fallback_pool = ctx
            .data_opt::<Option<Arc<NntpPool>>>()
            .and_then(Clone::clone);
        match live_pool.or(fallback_pool) {
            Some(pool) => Ok(collect_server_health(&pool, runtime_generation).await),
            None => Ok(Vec::new()),
        }
    }

    /// Filesystem capacity for the configured storage directories (data / intermediate / complete).
    #[graphql(guard = "ReadGuard")]
    async fn disk_usage(&self, ctx: &Context<'_>) -> Result<Vec<DiskUsage>> {
        let config = ctx.data::<SharedConfig>()?;
        let dirs = with_timed_config_read(config, "system.query.disk_usage", |cfg| {
            vec![
                ("Data".to_string(), cfg.data_dir.clone()),
                ("Intermediate downloads".to_string(), cfg.intermediate_dir()),
                ("Complete library".to_string(), cfg.complete_dir()),
            ]
        })
        .await;

        let usage = tokio::task::spawn_blocking(move || {
            dirs.into_iter()
                .filter_map(|(label, path)| -> Option<DiskUsage> {
                    let space =
                        weaver_server_core::operations::disk_space(std::path::Path::new(&path))?;
                    Some(DiskUsage {
                        label,
                        total_bytes: space.total_bytes,
                        used_bytes: space.used_bytes(),
                        free_bytes: space.available_bytes,
                        path,
                    })
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        Ok(usage)
    }
}

/// Snapshot per-server health from the live NNTP pool. Mirrors the per-server fields
/// emitted by the Prometheus exporter (`collect_server_health` in the app binary), shaped
/// for the GraphQL monitoring API. The connection pool orders servers by priority, so the
/// first entry is the primary and the rest are backups.
async fn collect_server_health(pool: &NntpPool, runtime_generation: u64) -> Vec<ServerHealth> {
    struct ServerLoadSnapshot {
        host: String,
        port: u16,
        tier: String,
        active: usize,
        effective: usize,
        configured: usize,
        penalty_until: Option<u64>,
    }

    let configs = pool.server_configs();
    // Read connection load outside the health lock.
    let pre: Vec<ServerLoadSnapshot> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let (_, effective) = pool.server_load(idx);
            let active = pool.active_connections(idx);
            let configured = pool
                .configured_connections(weaver_nntp::ServerId(idx))
                .unwrap_or(effective);
            let penalty_until = pool.capacity_penalty_until_epoch_ms(weaver_nntp::ServerId(idx));
            let tier = if idx == 0 { "PRIMARY" } else { "BACKUP" };
            ServerLoadSnapshot {
                host: cfg.host.clone(),
                port: cfg.port,
                tier: tier.to_string(),
                active,
                effective,
                configured,
                penalty_until,
            }
        })
        .collect();

    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(|(idx, snapshot)| {
            let srv = health.server(idx);
            let state = match srv.state() {
                weaver_nntp::ServerState::Healthy => "healthy",
                weaver_nntp::ServerState::Degraded { .. } => "degraded",
                weaver_nntp::ServerState::CoolingDown { .. } => "cooling_down",
                weaver_nntp::ServerState::Disabled { .. } => "disabled",
            };
            ServerHealth {
                label: format!("{}:{}", snapshot.host, snapshot.port),
                host: snapshot.host,
                port: snapshot.port,
                tier: snapshot.tier,
                state: state.to_string(),
                connections_active: snapshot.active as u32,
                connections_max: snapshot.effective as u32,
                connections_configured: snapshot.configured as u32,
                connections_effective: snapshot.effective as u32,
                capacity_penalty_until_epoch_ms: snapshot.penalty_until,
                runtime_generation,
                latency_ms: health.latency_ms(idx),
                success_count: srv.success_count,
                failure_count: srv.failure_count,
                consecutive_failures: srv.consecutive_failures,
                premature_deaths: health.recent_premature_deaths(idx) as u32,
            }
        })
        .collect()
}

fn absolutize_default_browse_path(path: String) -> std::io::Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        std::env::current_dir().map(|cwd| cwd.join(path))
    }
}
