use std::path::PathBuf;

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
        let (settings, lists) = tokio::task::spawn_blocking(move || {
            Ok::<_, weaver_server_core::StateError>((
                db.post_processing_settings()?,
                db.post_processing_script_lists()?,
            ))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            lists,
            strict_security_enabled(),
        ))
    }

    /// Live listing of `data_dir/scripts`, plus any stored option values.
    ///
    /// Nothing is cached: the directory is the source of truth, so a script
    /// added a second ago is listed and one deleted a second ago is not.
    #[graphql(guard = "AdminGuard")]
    async fn scripts(&self, ctx: &Context<'_>) -> Result<ScriptListingGql> {
        let db = ctx.data::<Database>()?.clone();
        let data_dir = {
            let config = ctx.data::<SharedConfig>()?;
            PathBuf::from(config.read().await.data_dir.clone())
        };
        tokio::task::spawn_blocking(move || {
            let listing = list_scripts(&data_dir)
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
        let results = tokio::task::spawn_blocking(move || db.job_post_processing_results(job_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(results.into_iter().map(Into::into).collect())
    }
}
