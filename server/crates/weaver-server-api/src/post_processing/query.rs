use super::*;
use weaver_server_core::post_processing::executor::strict_security_enabled;
use weaver_server_core::post_processing::listing::list_scripts;

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
        let (settings, lists, script_directory) = tokio::task::spawn_blocking(move || {
            Ok::<_, weaver_server_core::StateError>((
                db.post_processing_settings()?,
                db.post_processing_script_lists()?,
                db.post_processing_script_directory()?,
            ))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            lists,
            script_directory.to_string_lossy(),
            strict_security_enabled(),
        ))
    }

    /// Live listing of the configured scripts directory, plus stored option values.
    ///
    /// Nothing is cached: the directory is the source of truth, so a script
    /// added a second ago is listed and one deleted a second ago is not.
    #[graphql(guard = "AdminGuard")]
    async fn scripts(&self, ctx: &Context<'_>) -> Result<ScriptListingGql> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let script_directory = db
                .post_processing_script_directory()
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let listing = list_scripts(&script_directory)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let mut scripts = Vec::with_capacity(listing.scripts.len());
            for script in &listing.scripts {
                let stored = db
                    .post_processing_script_options(&script.name)
                    .map_err(|error| async_graphql::Error::new(error.to_string()))?;
                scripts.push(ScriptGql::new(script, &stored));
            }
            Ok(ScriptListingGql {
                scripts,
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

    #[graphql(guard = "ReadGuard")]
    async fn script_output(&self, ctx: &Context<'_>, output_id: String) -> Result<Option<String>> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.script_output(&output_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))
    }
}
