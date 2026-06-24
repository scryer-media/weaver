use super::*;
use crate::observability::with_timed_config_read;
use crate::system::metrics_history::{build_metrics_history, tier_for_range};
use crate::system::types::MetricsHistoryRangeGql;

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
}

fn absolutize_default_browse_path(path: String) -> std::io::Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        std::env::current_dir().map(|cwd| cwd.join(path))
    }
}
