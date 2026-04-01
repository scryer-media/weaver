use std::path::{Path, PathBuf};

use async_graphql::{Context, Object, Result};

use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_scheduler::{JobInfo, JobStatus, SchedulerHandle};
use weaver_state::{Database, JobHistoryRow};

use crate::auth::{AdminGuard, ReadGuard, graphql_error};
use crate::facade::{
    GlobalQueueState, HistoryItem, QueueEvent, QueueFilterInput, QueueItem, QueueSummary,
    SystemStatus, decode_event_cursor, decode_offset_cursor, global_queue_state,
    history_item_from_row, matches_history_filter, matches_queue_filter, metrics_from_snapshot,
    queue_event_from_record, queue_item_from_job, queue_summary,
};
use crate::metrics_history::build_metrics_history;
use crate::timeline::build_job_timeline;
use weaver_core::log_buffer::LogRingBuffer;

use crate::types::{
    ApiKey, ApiKeyScope, Category, DirectoryBrowseEntry, DirectoryBrowseResult, DownloadBlock,
    EventKind, GeneralSettings, Job, JobEvent, JobOutputFile, JobOutputResult, JobStatusGql,
    JobTimeline, Metrics, MetricsHistoryResult, RssFeed, RssSeenItem, Server, ServiceLogsPayload,
};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// The running weaver binary version.
    async fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

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
                    weaver_scheduler::JobStatus::Complete
                        | weaver_scheduler::JobStatus::Failed { .. }
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
                    weaver_scheduler::JobStatus::Complete
                        | weaver_scheduler::JobStatus::Failed { .. }
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
                    weaver_scheduler::JobStatus::Complete
                        | weaver_scheduler::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .collect();
        Ok(queue_summary(&items, &handle.get_metrics()))
    }

    /// Public history facade.
    #[graphql(guard = "ReadGuard")]
    async fn history_items(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<HistoryItem>> {
        let offset = decode_offset_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let limit = first.unwrap_or(u32::MAX) as usize;
        let db = ctx.data::<Database>()?.clone();
        let items = tokio::task::spawn_blocking(move || {
            let rows = db.list_job_history(&weaver_state::HistoryFilter::default())?;
            Ok::<_, weaver_state::StateError>(
                rows.into_iter()
                    .map(|row| history_item_from_row(&row))
                    .filter(|item| matches_history_filter(item, filter.as_ref()))
                    .skip(offset)
                    .take(limit)
                    .collect::<Vec<_>>(),
            )
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(items)
    }

    /// Public history facade for one completed or failed item.
    #[graphql(guard = "ReadGuard")]
    async fn history_item(&self, ctx: &Context<'_>, id: u64) -> Result<Option<HistoryItem>> {
        let db = ctx.data::<Database>()?.clone();
        let row = tokio::task::spawn_blocking(move || db.get_job_history(id))
            .await
            .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
            .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(row.as_ref().map(history_item_from_row))
    }

    /// Count history items exposed by the public history facade.
    #[graphql(guard = "ReadGuard")]
    async fn history_items_count(
        &self,
        ctx: &Context<'_>,
        filter: Option<QueueFilterInput>,
    ) -> Result<u32> {
        let db = ctx.data::<Database>()?.clone();
        let count = tokio::task::spawn_blocking(move || {
            let rows = db.list_job_history(&weaver_state::HistoryFilter::default())?;
            Ok::<_, weaver_state::StateError>(
                rows.into_iter()
                    .map(|row| history_item_from_row(&row))
                    .filter(|item| matches_history_filter(item, filter.as_ref()))
                    .count() as u32,
            )
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;
        Ok(count)
    }

    /// Semantic lifecycle history for a queue/history item.
    #[graphql(guard = "ReadGuard")]
    async fn history_events(
        &self,
        ctx: &Context<'_>,
        item_id: u64,
        first: Option<u32>,
        after: Option<String>,
    ) -> Result<Vec<QueueEvent>> {
        let cursor = decode_event_cursor(after.as_deref())
            .map_err(|message| graphql_error("CURSOR_INVALID", message))?;
        let db = ctx.data::<Database>()?.clone();
        let rows = tokio::task::spawn_blocking(move || {
            db.list_integration_events_after(cursor, Some(item_id), first)
        })
        .await
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
        .map_err(|e| graphql_error("INTERNAL", e.to_string()))?;

        rows.into_iter()
            .map(|row| {
                let record = serde_json::from_str(&row.payload_json).map_err(|e| {
                    graphql_error(
                        "INTERNAL",
                        format!("invalid integration event payload: {e}"),
                    )
                })?;
                Ok(queue_event_from_record(row.id, record))
            })
            .collect()
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
                    weaver_scheduler::JobStatus::Complete
                        | weaver_scheduler::JobStatus::Failed { .. }
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

        tokio::task::spawn_blocking(move || browse_directories(Path::new(&requested_path)))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
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
        match handle.get_job(weaver_core::id::JobId(id)) {
            Ok(info) => Ok(Some(Job::from(&info))),
            Err(weaver_scheduler::SchedulerError::JobNotFound(_)) => Ok(None),
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
        let output_dir = match handle.get_job(weaver_core::id::JobId(job_id)) {
            Ok(info) => info.output_dir.clone(),
            Err(weaver_scheduler::SchedulerError::JobNotFound(_)) => {
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

    /// Return recent log lines from the in-memory ring buffer.
    #[graphql(guard = "AdminGuard")]
    async fn service_logs(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 250)] limit: i32,
    ) -> Result<ServiceLogsPayload> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let clamped = limit.clamp(1, 2000) as usize;
        let lines = buffer.snapshot(clamped);
        let count = lines.len() as i32;
        Ok(ServiceLogsPayload { lines, count })
    }

    /// Get synthesized waterfall/timeline data for a job.
    async fn job_timeline(&self, ctx: &Context<'_>, job_id: u64) -> Result<Option<JobTimeline>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_state::Database>()?.clone();

        let live_job = match handle.get_job(JobId(job_id)) {
            Ok(info) => Some(info),
            Err(weaver_scheduler::SchedulerError::JobNotFound(_)) => None,
            Err(e) => return Err(e.into()),
        };

        let (events, history) = tokio::task::spawn_blocking(move || {
            let events = db.get_job_events(job_id)?;
            let history = db.get_job_history(job_id)?;
            Ok::<_, weaver_state::StateError>((events, history))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        let Some(job) = live_job.or_else(|| history.as_ref().map(job_info_from_history_row)) else {
            return Ok(None);
        };

        Ok(Some(build_job_timeline(&job, history.as_ref(), &events)))
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

    /// List all configured NNTP servers.
    #[graphql(guard = "AdminGuard")]
    async fn servers(&self, ctx: &Context<'_>) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg.servers.iter().map(Server::from).collect())
    }

    /// List all configured categories.
    #[graphql(guard = "AdminGuard")]
    async fn categories(&self, ctx: &Context<'_>) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg.categories.iter().map(Category::from).collect())
    }

    /// Get the event log for a specific job.
    async fn job_events(&self, ctx: &Context<'_>, job_id: u64) -> Result<Vec<JobEvent>> {
        let db = ctx.data::<weaver_state::Database>()?;
        let db = db.clone();
        let events = tokio::task::spawn_blocking(move || db.get_job_events(job_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(events
            .into_iter()
            .map(|e| JobEvent {
                kind: e.kind.parse::<EventKind>().unwrap_or(EventKind::JobCreated),
                job_id: e.job_id,
                file_id: e.file_id,
                message: e.message,
                timestamp: e.timestamp as f64,
            })
            .collect())
    }

    /// Admin-facing auth status moved to query semantics.
    #[graphql(guard = "AdminGuard")]
    async fn admin_login_status(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::mutation::LoginStatusResult> {
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        Ok(match auth_cache.snapshot() {
            Some(creds) => crate::mutation::LoginStatusResult {
                enabled: true,
                username: Some(creds.username),
            },
            None => crate::mutation::LoginStatusResult {
                enabled: false,
                username: None,
            },
        })
    }

    /// List all API keys (without raw key values).
    #[graphql(guard = "AdminGuard")]
    async fn api_keys(&self, ctx: &Context<'_>) -> Result<Vec<ApiKey>> {
        let db = ctx.data::<weaver_state::Database>()?;
        let db = db.clone();
        let rows = tokio::task::spawn_blocking(move || db.list_api_keys())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|r| ApiKey {
                id: r.id,
                name: r.name,
                scope: match r.scope.as_str() {
                    "read" => ApiKeyScope::Read,
                    "control" | "integration" => ApiKeyScope::Control,
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Control,
                },
                created_at: r.created_at as f64 * 1000.0,
                last_used_at: r.last_used_at.map(|t| t as f64 * 1000.0),
            })
            .collect())
    }

    /// Get general settings.
    #[graphql(guard = "AdminGuard")]
    async fn settings(&self, ctx: &Context<'_>) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(GeneralSettings {
            data_dir: cfg.data_dir.clone(),
            intermediate_dir: cfg.intermediate_dir(),
            complete_dir: cfg.complete_dir(),
            cleanup_after_extract: cfg.cleanup_after_extract(),
            max_download_speed: cfg.max_download_speed.unwrap_or(0),
            max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
            isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
        })
    }

    /// List configured RSS feeds.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feeds(&self, ctx: &Context<'_>) -> Result<Vec<RssFeed>> {
        let db = ctx.data::<weaver_state::Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let feeds = db.list_rss_feeds()?;
            let mut out = Vec::with_capacity(feeds.len());
            for feed in feeds {
                let rules = db
                    .list_rss_rules(feed.id)?
                    .iter()
                    .map(crate::types::RssRule::from_row)
                    .collect();
                out.push(RssFeed::from_row(&feed, rules));
            }
            Ok::<_, weaver_state::StateError>(out)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Get a single RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn rss_feed(&self, ctx: &Context<'_>, id: u32) -> Result<Option<RssFeed>> {
        let db = ctx.data::<weaver_state::Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let Some(feed) = db.get_rss_feed(id)? else {
                return Ok(None);
            };
            let rules = db
                .list_rss_rules(feed.id)?
                .iter()
                .map(crate::types::RssRule::from_row)
                .collect();
            Ok::<_, weaver_state::StateError>(Some(RssFeed::from_row(&feed, rules)))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// List recently seen RSS items, optionally scoped to one feed.
    #[graphql(guard = "AdminGuard")]
    async fn rss_seen_items(
        &self,
        ctx: &Context<'_>,
        feed_id: Option<u32>,
        limit: Option<u32>,
    ) -> Result<Vec<RssSeenItem>> {
        let db = ctx.data::<weaver_state::Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let items = db.list_rss_seen_items(feed_id, limit)?;
            Ok::<_, weaver_state::StateError>(
                items
                    .iter()
                    .map(crate::types::RssSeenItem::from_row)
                    .collect(),
            )
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    async fn schedules(&self, ctx: &Context<'_>) -> Result<Vec<crate::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let entries: Vec<weaver_core::config::ScheduleEntry> =
            tokio::task::spawn_blocking(move || db.list_schedules())
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(entries
            .into_iter()
            .map(crate::types::Schedule::from)
            .collect())
    }
}

fn browse_directories(path: &Path) -> Result<DirectoryBrowseResult> {
    let metadata = std::fs::metadata(path).map_err(|e| {
        async_graphql::Error::new(format!("failed to read directory metadata: {e}"))
    })?;
    if !metadata.is_dir() {
        return Err(async_graphql::Error::new("path is not a directory"));
    }

    let mut entries = std::fs::read_dir(path)
        .map_err(|e| async_graphql::Error::new(format!("failed to read directory: {e}")))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            let entry_path = entry.path();
            let metadata = std::fs::metadata(&entry_path).ok()?;
            if !metadata.is_dir() {
                return None;
            }
            Some(DirectoryBrowseEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry_path.to_string_lossy().into_owned(),
            })
        })
        .collect::<Vec<_>>();

    entries.sort_by(|left, right| {
        left.name
            .to_ascii_lowercase()
            .cmp(&right.name.to_ascii_lowercase())
            .then_with(|| left.name.cmp(&right.name))
    });

    let current_path = path.to_string_lossy().into_owned();
    let parent_path = PathBuf::from(path)
        .parent()
        .map(|parent| parent.to_string_lossy().into_owned())
        .filter(|parent| parent != &current_path);

    Ok(DirectoryBrowseResult {
        current_path,
        parent_path,
        entries,
    })
}

fn job_info_from_history_row(row: &JobHistoryRow) -> JobInfo {
    let metadata = row
        .metadata
        .as_deref()
        .and_then(|value| serde_json::from_str::<Vec<(String, String)>>(value).ok())
        .unwrap_or_default();
    let error = row.error_message.clone();
    let status = match row.status.to_ascii_lowercase().as_str() {
        "complete" => JobStatus::Complete,
        "paused" => JobStatus::Paused,
        "cancelled" => JobStatus::Failed {
            error: error
                .clone()
                .unwrap_or_else(|| "item was cancelled".to_string()),
        },
        _ => JobStatus::Failed {
            error: error.clone().unwrap_or_else(|| "job failed".to_string()),
        },
    };
    let progress = if row.total_bytes == 0 {
        0.0
    } else {
        (row.downloaded_bytes as f64 / row.total_bytes as f64).clamp(0.0, 1.0)
    };

    JobInfo {
        job_id: JobId(row.job_id),
        name: row.name.clone(),
        status,
        progress,
        total_bytes: row.total_bytes,
        downloaded_bytes: row.downloaded_bytes,
        optional_recovery_bytes: row.optional_recovery_bytes,
        optional_recovery_downloaded_bytes: row.optional_recovery_downloaded_bytes,
        failed_bytes: row.failed_bytes,
        health: row.health,
        password: None,
        category: row.category.clone(),
        metadata,
        output_dir: row.output_dir.clone(),
        error,
        created_at_epoch_ms: row.created_at as f64 * 1000.0,
    }
}

fn list_output_files(dir: &Path) -> Result<JobOutputResult> {
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

fn collect_files_recursive(dir: &Path, out: &mut Vec<JobOutputFile>) -> Result<()> {
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
