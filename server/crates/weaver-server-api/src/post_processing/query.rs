use super::*;
use weaver_server_core::post_processing::model::{AttemptId, RunId, RunStatus};

#[derive(Default)]
pub(crate) struct PostProcessingQuery;

#[Object]
impl PostProcessingQuery {
    #[graphql(guard = "AdminGuard")]
    async fn post_processing_settings(
        &self,
        ctx: &Context<'_>,
    ) -> Result<PostProcessingSettingsGql> {
        let db = ctx.data::<Database>()?.clone();
        let settings = tokio::task::spawn_blocking(move || db.post_processing_settings())
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(settings.into())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_revisions(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<PostProcessingExtensionRevision>> {
        let db = ctx.data::<Database>()?.clone();
        let revisions = tokio::task::spawn_blocking(move || db.list_extension_revisions())
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(revisions.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_profiles(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<PostProcessingProfile>> {
        let db = ctx.data::<Database>()?.clone();
        let profiles = tokio::task::spawn_blocking(move || db.list_post_processing_profiles())
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(profiles.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_job_plan(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<Option<PostProcessingJobPlan>> {
        let db = ctx.data::<Database>()?.clone();
        let plan = tokio::task::spawn_blocking(move || db.frozen_post_processing_plan(job_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(plan.map(|plan| PostProcessingJobPlan {
            job_id,
            definition: async_graphql::Json(
                serde_json::to_value(plan).unwrap_or(serde_json::Value::Null),
            ),
        }))
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_runs(
        &self,
        ctx: &Context<'_>,
        job_id: Option<u64>,
        #[graphql(default = 100)] limit: u32,
    ) -> Result<Vec<PostProcessingRun>> {
        let db = ctx.data::<Database>()?.clone();
        let runs = tokio::task::spawn_blocking(move || db.list_post_processing_runs(job_id, limit))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(runs.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_queue(&self, ctx: &Context<'_>) -> Result<Vec<PostProcessingRun>> {
        let db = ctx.data::<Database>()?.clone();
        let runs = tokio::task::spawn_blocking(move || db.list_post_processing_runs(None, 500))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let queue_order = ctx
            .data::<weaver_server_core::post_processing::service::PostProcessingService>()?
            .queued_run_ids();
        let mut runs = runs
            .into_iter()
            .filter(|run| matches!(run.status, RunStatus::Queued | RunStatus::Running))
            .collect::<Vec<_>>();
        runs.sort_by_key(|run| match run.status {
            RunStatus::Running => (0, 0),
            RunStatus::Queued => (
                1,
                queue_order
                    .iter()
                    .position(|run_id| run_id == run.run_id.as_str())
                    .unwrap_or(usize::MAX),
            ),
            _ => (2, usize::MAX),
        });
        Ok(runs.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_run(
        &self,
        ctx: &Context<'_>,
        run_id: String,
    ) -> Result<Option<PostProcessingRun>> {
        let run_id =
            RunId::new(run_id).map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let run = tokio::task::spawn_blocking(move || db.post_processing_run(&run_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(run.map(Into::into))
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_attempts(
        &self,
        ctx: &Context<'_>,
        run_id: String,
    ) -> Result<Vec<PostProcessingAttempt>> {
        let run_id =
            RunId::new(run_id).map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let attempts = tokio::task::spawn_blocking(move || db.post_processing_attempts(&run_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(attempts.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_artifacts(
        &self,
        ctx: &Context<'_>,
        run_id: String,
    ) -> Result<Vec<PostProcessingArtifact>> {
        let run_id =
            RunId::new(run_id).map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let artifacts = tokio::task::spawn_blocking(move || db.post_processing_artifacts(&run_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(artifacts.into_iter().map(Into::into).collect())
    }

    #[graphql(guard = "ReadGuard")]
    async fn post_processing_logs(
        &self,
        ctx: &Context<'_>,
        attempt_id: String,
        cursor: Option<u64>,
        #[graphql(default = 200)] limit: u32,
    ) -> Result<PostProcessingLogPageGql> {
        let attempt_id = AttemptId::new(attempt_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let page = tokio::task::spawn_blocking(move || {
            db.post_processing_logs(&attempt_id, cursor, limit as usize)
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(page.into())
    }
}
