use std::path::PathBuf;

use super::*;
use weaver_server_core::post_processing::executor::{
    PostProcessingExecutor, strict_security_enabled,
};
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    PipelineOutcome, PostProcessingSettings, ScriptName,
};
use weaver_server_core::post_processing::runner::{CompatibilityFacts, JobExecutionContext};

fn parse_script_name(value: String) -> Result<ScriptName> {
    ScriptName::new(value).map_err(|error| async_graphql::Error::new(error.to_string()))
}

#[derive(Default)]
pub(crate) struct PostProcessingMutation;

#[Object]
impl PostProcessingMutation {
    #[graphql(guard = "AdminGuard")]
    async fn set_post_processing_settings(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingSettingsInput,
    ) -> Result<PostProcessingSettingsGql> {
        let settings = PostProcessingSettings::from(input);
        settings
            .validate()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        // Refused here as well as at run time: an operator who turns the switch
        // on under strict security should be told immediately, not discover it
        // in a job log later.
        if settings.execution_enabled && strict_security_enabled() {
            return Err(async_graphql::Error::new(
                "WEAVER_STRICT_SECURITY=1 refuses post-processing script execution",
            ));
        }
        let db = ctx.data::<Database>()?.clone();
        let saved = settings.clone();
        let lists = tokio::task::spawn_blocking(move || {
            db.save_post_processing_settings(&saved)?;
            db.post_processing_script_lists()
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

    /// Replace the global default list and every per-category override.
    #[graphql(guard = "AdminGuard")]
    async fn set_script_lists(
        &self,
        ctx: &Context<'_>,
        input: ScriptListsInput,
    ) -> Result<ScriptListsGql> {
        let lists = input.into_domain().map_err(async_graphql::Error::new)?;
        let db = ctx.data::<Database>()?.clone();
        let saved = lists.clone();
        tokio::task::spawn_blocking(move || db.save_post_processing_script_lists(&saved))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(lists.into())
    }

    /// Replace the stored option values for one script, validated against its manifest.
    #[graphql(guard = "AdminGuard")]
    async fn set_script_options(
        &self,
        ctx: &Context<'_>,
        script: String,
        options: Vec<ScriptOptionInput>,
    ) -> Result<ScriptGql> {
        let script = parse_script_name(script)?;
        let supplied = options
            .into_iter()
            .map(ScriptOptionInput::into_domain)
            .collect::<Result<Vec<_>, _>>()
            .map_err(async_graphql::Error::new)?;
        let db = ctx.data::<Database>()?.clone();
        let data_dir = {
            let config = ctx.data::<SharedConfig>()?;
            PathBuf::from(config.read().await.data_dir.clone())
        };
        tokio::task::spawn_blocking(move || {
            let discovered = resolve_script(&data_dir, &script)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            discovered
                .manifest
                .resolve_options(&supplied)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            db.save_post_processing_script_options(&script, &supplied)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let stored = db
                .post_processing_script_options(&script)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            Ok(ScriptGql::new(&discovered, &stored))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
    }

    /// Execute the job's script list again against its retained output directory.
    #[graphql(guard = "ControlGuard")]
    async fn rerun_post_processing(&self, ctx: &Context<'_>, job_id: u64) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let executor = ctx.data::<PostProcessingExecutor>()?.clone();
        let db_for_load = db.clone();
        let history = tokio::task::spawn_blocking(move || db_for_load.get_job_history(job_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .ok_or_else(|| {
                async_graphql::Error::new("post-processing reruns require a terminal history job")
            })?;
        if !executor
            .execution_enabled()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
        {
            return Err(async_graphql::Error::new(
                "post-processing script execution is disabled",
            ));
        }
        let metadata = history
            .metadata
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<(String, String)>>(raw).ok())
            .unwrap_or_default();
        let scripts = executor
            .resolve_job_scripts(history.category.as_deref(), &metadata)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        if scripts.enabled_entries().next().is_none() {
            return Err(async_graphql::Error::new(
                "no post-processing scripts are configured for this job",
            ));
        }
        let working_directory = history
            .output_dir
            .as_deref()
            .map(PathBuf::from)
            .ok_or_else(|| {
                async_graphql::Error::new("terminal job has no retained output directory")
            })?;
        let (data_dir, intermediate_dir, complete_dir) = {
            let config = ctx.data::<SharedConfig>()?.read().await;
            (
                PathBuf::from(&config.data_dir),
                PathBuf::from(config.intermediate_dir()),
                PathBuf::from(config.complete_dir()),
            )
        };
        let context = JobExecutionContext {
            job_id,
            name: history.name.clone(),
            nzb_filename: format!("{}.nzb", history.name),
            category: history.category.clone(),
            group: None,
            source_url: None,
            working_directory: working_directory.clone(),
            final_directory: working_directory,
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts {
                total_bytes: history.total_bytes,
                downloaded_bytes: history.downloaded_bytes,
                health_milli: history.health,
                critical_health_milli: 0,
                password: None,
                failure_message: history.error_message.clone(),
                data_dir: Some(data_dir),
                intermediate_dir: Some(intermediate_dir),
                complete_dir: Some(complete_dir),
                temp_dir: Some(std::env::temp_dir()),
                app_dir: std::env::current_exe()
                    .ok()
                    .and_then(|path| path.parent().map(PathBuf::from)),
                previous_script_status: Default::default(),
            },
        };
        tokio::spawn(async move {
            if let Err(error) = executor
                .execute_job(job_id, scripts, context, None, None)
                .await
            {
                tracing::error!(job_id, error = %error, "post-processing rerun failed");
            }
        });
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn cancel_job_post_processing(&self, ctx: &Context<'_>, job_id: u64) -> Result<bool> {
        if ctx.data::<PostProcessingExecutor>()?.cancel_job(job_id) {
            return Ok(true);
        }
        ctx.data::<SchedulerHandle>()?
            .cancel_post_processing(JobId(job_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }
}
