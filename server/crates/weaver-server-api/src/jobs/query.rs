use std::path::PathBuf;

use super::*;

#[derive(Default)]
pub(crate) struct JobsQuery;

#[Object]
impl JobsQuery {
    /// Public queue facade for active or in-flight items.
    #[graphql(guard = "ReadGuard")]
    async fn queue_items(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = first.unwrap_or(u32::MAX) as usize;

        let items = handle
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
            .filter(|item| matches_queue_filter(item, filter.as_ref()))
            .skip(offset)
            .take(limit)
            .collect();
        Ok(items)
    }
    /// Public queue facade for one active item.
    #[graphql(guard = "ReadGuard")]
    async fn queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<QueueItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let Some(info) = handle.list_jobs().into_iter().find(|info| {
            info.job_id.0 == id
                && !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
        }) else {
            return Ok(None);
        };
        Ok(Some(queue_item_from_job(&info)))
    }
    /// Summary of the active queue and live throughput.
    #[graphql(guard = "ReadGuard")]
    async fn queue_summary(&self, ctx: &Context<'_>) -> Result<QueueSummary> {
        let handle = ctx.data::<SchedulerHandle>()?;
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
        Ok(queue_summary(&items, &handle.get_metrics()))
    }
    /// Atomic queue bootstrap snapshot for initial page load and reconnect polling.
    #[graphql(guard = "ReadGuard")]
    async fn queue_snapshot(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<QueueSnapshot> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let config = ctx.data::<SharedConfig>()?.clone();
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();

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
            .filter(|item| matches_queue_filter(item, filter.as_ref()))
            .collect();
        let metrics = handle.get_metrics();
        let latest_cursor = replay.latest_cursor().await;
        let cfg = config.read().await;

        Ok(QueueSnapshot {
            summary: queue_summary(&items, &metrics),
            metrics: metrics_from_snapshot(&metrics),
            global_state: global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                cfg.max_download_speed.unwrap_or(0),
            ),
            items,
            latest_cursor,
            generated_at: chrono::Utc::now(),
        })
    }
    /// Current global queue state facade.
    #[graphql(guard = "ReadGuard")]
    async fn global_queue_state(&self, ctx: &Context<'_>) -> Result<GlobalQueueState> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(global_queue_state(
            handle.is_globally_paused(),
            &handle.get_download_block(),
            cfg.max_download_speed.unwrap_or(0),
        ))
    }
    /// Compatibility cursor for the live-only `queueEvents` stream.
    #[graphql(guard = "ReadGuard")]
    async fn latest_queue_cursor(&self, ctx: &Context<'_>) -> Result<String> {
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        Ok(replay.latest_cursor().await)
    }
    /// List jobs, optionally filtered by status, category, or metadata key.
    /// Supports pagination via `limit` and `offset`.
    async fn jobs(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<Vec<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let infos = handle.list_jobs();
        let filtered = infos.iter().filter(|info| {
            if let Some(ref statuses) = status {
                let gql_status = JobStatusGql::from(&info.status);
                if !statuses.contains(&gql_status) {
                    return false;
                }
            }
            if let Some(ref cat) = category
                && info.category.as_ref() != Some(cat)
            {
                return false;
            }
            if let Some(ref key) = has_metadata_key
                && !info.metadata.iter().any(|(k, _)| k == key)
            {
                return false;
            }
            true
        });
        let jobs = if let Some(lim) = limit {
            filtered
                .skip(offset.unwrap_or(0) as usize)
                .take(lim as usize)
                .map(Job::from)
                .collect()
        } else {
            filtered.map(Job::from).collect()
        };
        Ok(jobs)
    }
    /// Count jobs matching the given filters (same filters as `jobs`).
    async fn job_count(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
    ) -> Result<u32> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let infos = handle.list_jobs();
        let count = infos
            .iter()
            .filter(|info| {
                if let Some(ref statuses) = status {
                    let gql_status = JobStatusGql::from(&info.status);
                    if !statuses.contains(&gql_status) {
                        return false;
                    }
                }
                if let Some(ref cat) = category
                    && info.category.as_ref() != Some(cat)
                {
                    return false;
                }
                if let Some(ref key) = has_metadata_key
                    && !info.metadata.iter().any(|(k, _)| k == key)
                {
                    return false;
                }
                true
            })
            .count();
        Ok(count as u32)
    }
    /// Get a specific job by ID.
    async fn job(&self, ctx: &Context<'_>, id: u64) -> Result<Option<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        match handle.get_job(weaver_server_core::jobs::ids::JobId(id)) {
            Ok(info) => Ok(Some(Job::from(&info))),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    /// List files in a completed job's output directory.
    async fn job_output_files(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<Option<JobOutputResult>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let output_dir = match handle.get_job(weaver_server_core::jobs::ids::JobId(job_id)) {
            Ok(info) => info.output_dir.clone(),
            Err(weaver_server_core::SchedulerError::JobNotFound(_)) => {
                let db = ctx.data::<Database>()?.clone();
                tokio::task::spawn_blocking(move || db.get_job_history(job_id))
                    .await
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?
                    .map_err(|e| async_graphql::Error::new(e.to_string()))?
                    .and_then(|row| row.output_dir)
            }
            Err(e) => return Err(e.into()),
        };
        let Some(output_dir) = output_dir else {
            return Ok(None);
        };

        let dir_path = PathBuf::from(&output_dir);
        let result = tokio::task::spawn_blocking(move || list_output_files(&dir_path))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))??;
        Ok(Some(result))
    }
}

fn list_output_files(dir: &std::path::Path) -> Result<JobOutputResult> {
    let output_dir = dir.to_string_lossy().into_owned();

    if !dir.is_dir() {
        return Ok(JobOutputResult {
            output_dir,
            files: Vec::new(),
            total_bytes: 0,
        });
    }

    let mut files = Vec::new();
    collect_files_recursive(dir, &mut files)?;
    files.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
    });
    let total_bytes = files.iter().map(|f| f.size_bytes).sum();

    Ok(JobOutputResult {
        output_dir,
        files,
        total_bytes,
    })
}

fn collect_files_recursive(dir: &std::path::Path, out: &mut Vec<JobOutputFile>) -> Result<()> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| async_graphql::Error::new(format!("failed to read directory: {e}")))?;
    for entry in entries.flatten() {
        let path = entry.path();
        let meta = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_dir() {
            collect_files_recursive(&path, out)?;
        } else {
            out.push(JobOutputFile {
                name: path
                    .file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
                path: path.to_string_lossy().into_owned(),
                size_bytes: meta.len(),
            });
        }
    }
    Ok(())
}
