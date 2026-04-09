use super::*;

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
        let cfg = config.read().await;
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
        let global_state = global_queue_state(
            handle.is_globally_paused(),
            &handle.get_download_block(),
            cfg.max_download_speed.unwrap_or(0),
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
    /// Historical Prometheus metrics, parsed on demand from compressed SQLite scrapes.
    #[graphql(guard = "ReadGuard")]
    async fn metrics_history(
        &self,
        ctx: &Context<'_>,
        minutes: i32,
        metrics: Vec<String>,
    ) -> Result<MetricsHistoryResult> {
        let clamped_minutes = minutes.clamp(1, 1440) as i64;
        let db = ctx.data::<Database>()?.clone();
        let since_epoch_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
            - clamped_minutes * 60;

        tokio::task::spawn_blocking(move || {
            let rows = db
                .list_metrics_scrapes_since(since_epoch_sec)
                .map_err(|error| error.to_string())?;
            build_metrics_history(rows, &metrics)
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
        let default_path = {
            let cfg = config.read().await;
            cfg.complete_dir()
        };
        let requested_path = path
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .unwrap_or(default_path);

        let listing = tokio::task::spawn_blocking(move || {
            weaver_server_core::operations::browse_directories(Path::new(&requested_path))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error))?;

        Ok(DirectoryBrowseResult {
            current_path: listing.current_path,
            parent_path: listing.parent_path,
            entries: listing
                .entries
                .into_iter()
                .map(|entry| DirectoryBrowseEntry {
                    name: entry.name,
                    path: entry.path,
                })
                .collect(),
        })
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
