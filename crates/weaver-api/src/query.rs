use async_graphql::{Context, Object, Result};

use weaver_core::config::SharedConfig;
use weaver_scheduler::SchedulerHandle;

use crate::auth::AdminGuard;
use crate::timeline::build_job_timeline;
use crate::types::{
    ApiKey, ApiKeyScope, Category, DownloadBlock, EventKind, GeneralSettings, Job, JobEvent,
    JobStatusGql, JobTimeline, Metrics, RssFeed, RssSeenItem, Server,
};

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// List jobs, optionally filtered by status, category, or metadata key.
    async fn jobs(
        &self,
        ctx: &Context<'_>,
        status: Option<Vec<JobStatusGql>>,
        category: Option<String>,
        has_metadata_key: Option<String>,
    ) -> Result<Vec<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let infos = handle.list_jobs();
        let jobs = infos
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
            .map(Job::from)
            .collect();
        Ok(jobs)
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

    /// Get synthesized waterfall/timeline data for a job.
    async fn job_timeline(&self, ctx: &Context<'_>, job_id: u64) -> Result<Option<JobTimeline>> {
        let handle = ctx.data::<SchedulerHandle>()?.clone();
        let db = ctx.data::<weaver_state::Database>()?.clone();

        let job = match handle.get_job(weaver_core::id::JobId(job_id)) {
            Ok(info) => info,
            Err(weaver_scheduler::SchedulerError::JobNotFound(_)) => return Ok(None),
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
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Integration,
                },
                created_at: r.created_at as f64,
                last_used_at: r.last_used_at.map(|t| t as f64),
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
}
