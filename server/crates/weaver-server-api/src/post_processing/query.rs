use super::*;
use crate::auth::graphql_error;
use weaver_server_core::post_processing::executor::strict_security_enabled;
use weaver_server_core::post_processing::listing::list_scripts;
use weaver_server_core::post_processing::output::ScriptRunFilter;

/// The scripts directory as it is now. A directory that cannot be read is not
/// an error here: it is the reason every script job reports for being unable to
/// run.
pub(crate) fn directory_view(db: &Database) -> ScriptDirectoryView {
    ScriptDirectoryView::new(
        db.post_processing_script_directory()
            .map_err(|error| error.to_string())
            .and_then(|directory| list_scripts(&directory).map_err(|error| error.to_string())),
    )
}

const SCRIPT_RUNS_PAGE: i32 = 50;
const SCRIPT_RUNS_PAGE_MAX: i32 = 200;

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
        let (settings, script_directory) = tokio::task::spawn_blocking(move || {
            Ok::<_, weaver_server_core::StateError>((
                db.post_processing_settings()?,
                db.post_processing_script_directory()?,
            ))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            script_directory.to_string_lossy(),
            strict_security_enabled(),
        ))
    }

    /// Every saved script job, in run order.
    #[graphql(guard = "AdminGuard")]
    async fn script_instances(&self, ctx: &Context<'_>) -> Result<Vec<ScriptInstanceGql>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let directory = directory_view(&db);
            Ok::<_, weaver_server_core::StateError>(
                db.script_instances()?
                    .into_iter()
                    .map(|instance| directory.instance(instance))
                    .collect(),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))
    }

    /// One saved script job, or null when there is none by that id.
    #[graphql(guard = "AdminGuard")]
    async fn script_instance(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<Option<ScriptInstanceGql>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            Ok::<_, weaver_server_core::StateError>(
                db.script_instance(&id)?
                    .map(|instance| directory_view(&db).instance(instance)),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))
    }

    /// Every named secret, by name, with the script jobs that link it. Values
    /// are never returned.
    #[graphql(guard = "AdminGuard")]
    async fn secrets(&self, ctx: &Context<'_>) -> Result<Vec<SecretGql>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.secrets())
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map(|secrets| secrets.into_iter().map(Into::into).collect())
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))
    }

    /// Live listing of the scripts directory, each script with the preset its
    /// header offers.
    ///
    /// Nothing is cached: the directory is the source of truth, so a script
    /// added a second ago is listed and one deleted a second ago is not.
    #[graphql(guard = "AdminGuard")]
    async fn discovered_scripts(&self, ctx: &Context<'_>) -> Result<ScriptListingGql> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let script_directory = db
                .post_processing_script_directory()
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let listing = list_scripts(&script_directory)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            Ok(ScriptListingGql {
                scripts: listing.scripts.iter().map(ScriptGql::new).collect(),
                problems: listing.problems.into_iter().map(Into::into).collect(),
            })
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
    }

    /// Script results recorded for a job, from the live row or from history.
    #[graphql(guard = "ReadGuard")]
    async fn post_processing_results(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
    ) -> Result<Vec<ScriptResultGql>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let mut results = db.job_post_processing_results(job_id)?;
            results.extend(db.event_script_results(job_id)?);
            let retained_ids = db.retained_script_output_ids(job_id)?;
            results
                .into_iter()
                .map(|result| {
                    let retained = result
                        .output_id
                        .as_deref()
                        .map(|id| retained_ids.contains(id))
                        .unwrap_or(false);
                    let mut result = ScriptResultGql::from(result);
                    result.output_retained = retained;
                    Ok::<_, weaver_server_core::StateError>(result)
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))
    }

    /// Recorded script runs, latest first, whatever started them. Runs that
    /// belong to no job are listed here and nowhere else.
    #[graphql(guard = "ReadGuard")]
    #[allow(clippy::too_many_arguments)]
    async fn script_runs(
        &self,
        ctx: &Context<'_>,
        limit: Option<i32>,
        before: Option<String>,
        kind: Option<ScriptKindGql>,
        script: Option<String>,
        job_id: Option<u64>,
        status: Option<ScriptStatusGql>,
    ) -> Result<ScriptRunPageGql> {
        let db = ctx.data::<Database>()?.clone();
        let limit = limit
            .unwrap_or(SCRIPT_RUNS_PAGE)
            .clamp(1, SCRIPT_RUNS_PAGE_MAX) as usize;
        let before = before
            .map(|before| before.parse::<i64>())
            .transpose()
            .map_err(|_| async_graphql::Error::new("before is not a position in the list"))?;
        let filter = ScriptRunFilter {
            job_id,
            script,
            kind: kind.map(Into::into),
            status: status.map(Into::into),
        };
        // One more than asked for says whether another page follows.
        let (mut runs, total, status_counts) = tokio::task::spawn_blocking(move || {
            let total = db.script_run_count(&filter)?;
            let status_counts = db.script_run_status_counts(&filter)?;
            db.script_runs(filter, before, limit as u32 + 1)
                .map(|runs| (runs, total, status_counts))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let more = runs.len() > limit;
        runs.truncate(limit);
        Ok(ScriptRunPageGql {
            next_before: runs.last().filter(|_| more).map(|run| run.seq.to_string()),
            runs: runs.into_iter().map(Into::into).collect(),
            total,
            status_counts: status_counts
                .into_iter()
                .map(|(status, count)| ScriptRunStatusCountGql {
                    status: status.into(),
                    count,
                })
                .collect(),
        })
    }

    /// A test run as it stands, or null when there is no such run or it is no
    /// longer kept. Test runs are held in memory and are not recorded runs.
    #[graphql(guard = "AdminGuard")]
    async fn script_test_run(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<Option<ScriptTestRunGql>> {
        Ok(ctx.data::<Database>()?.script_test(&id).map(Into::into))
    }

    /// The run the caller is. Only for a running script that calls with the
    /// token its run was handed as `WEAVER_RUN_TOKEN`; an error for anyone
    /// else, and once that run has ended.
    async fn script_run(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::post_processing::script_run::LiveScriptRunGql> {
        use crate::post_processing::script_run::{LiveScriptRunGql, calling_run};
        Ok(LiveScriptRunGql(calling_run(ctx)?))
    }

    #[graphql(guard = "ReadGuard")]
    async fn script_output(&self, ctx: &Context<'_>, output_id: String) -> Result<Option<String>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.script_output(&output_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))
    }
}
