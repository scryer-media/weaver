use async_graphql::{Context, Object, Result};

use weaver_core::config::SharedConfig;
use weaver_scheduler::SchedulerHandle;

use crate::types::{EventKind, GeneralSettings, Job, JobEvent, JobStatusGql, Metrics, Server};

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
        let infos = handle.list_jobs().await?;
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
                    && info.category.as_ref() != Some(cat) {
                        return false;
                    }
                if let Some(ref key) = has_metadata_key
                    && !info.metadata.iter().any(|(k, _)| k == key) {
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
        match handle.get_job(weaver_core::id::JobId(id)).await {
            Ok(info) => Ok(Some(Job::from(&info))),
            Err(weaver_scheduler::SchedulerError::JobNotFound(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get current pipeline metrics.
    async fn metrics(&self, ctx: &Context<'_>) -> Result<Metrics> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let snapshot = handle.get_metrics().await?;
        Ok(Metrics::from(&snapshot))
    }

    /// Check whether the pipeline is globally paused.
    async fn is_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(handle.is_globally_paused().await?)
    }

    /// List all configured NNTP servers.
    async fn servers(&self, ctx: &Context<'_>) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(cfg.servers.iter().map(Server::from).collect())
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

    /// Get general settings.
    async fn settings(&self, ctx: &Context<'_>) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let cfg = config.read().await;
        Ok(GeneralSettings {
            data_dir: cfg.data_dir.clone(),
            intermediate_dir: cfg.intermediate_dir(),
            complete_dir: cfg.complete_dir(),
            cleanup_after_extract: cfg.cleanup_after_extract(),
            max_download_speed: cfg.max_download_speed.unwrap_or(0),
            max_retries: cfg
                .retry
                .as_ref()
                .and_then(|r| r.max_retries)
                .unwrap_or(3),
        })
    }
}
