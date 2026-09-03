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
use weaver_server_core::post_processing::settings::normalize_script_directory;

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
        let PostProcessingSettingsInput {
            execution_enabled,
            concurrency,
            termination_grace_seconds,
            python_interpreter,
            powershell_interpreter,
            batch_interpreter,
            unacceptable_extensions,
        } = input;
        if unacceptable_extensions.is_null() {
            return Err(async_graphql::Error::new(
                "unacceptableExtensions must be omitted or a list, not null",
            ));
        }
        // Refused here as well as at run time: an operator who turns the switch
        // on under strict security should be told immediately, not discover it
        // in a job log later.
        if execution_enabled && strict_security_enabled() {
            return Err(async_graphql::Error::new(
                "WEAVER_STRICT_SECURITY=1 refuses post-processing script execution",
            ));
        }
        let db = ctx.data::<Database>()?.clone();
        let (settings, lists, script_directory) = tokio::task::spawn_blocking(move || {
            let (unacceptable_extensions, preserve_extensions) = match unacceptable_extensions {
                async_graphql::MaybeUndefined::Undefined => (Vec::new(), true),
                async_graphql::MaybeUndefined::Value(extensions) => (extensions, false),
                async_graphql::MaybeUndefined::Null => unreachable!("checked before worker"),
            };
            let settings = PostProcessingSettings {
                execution_enabled,
                concurrency,
                termination_grace_seconds,
                python_interpreter,
                powershell_interpreter,
                batch_interpreter,
                unacceptable_extensions,
            };
            let settings = db.save_post_processing_settings_preserving_extensions(
                settings,
                preserve_extensions,
            )?;
            Ok::<_, weaver_server_core::StateError>((
                settings,
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

    /// Select the sole live source of post-processing scripts. Changing it
    /// clears name-based assignments and option values, never script files.
    #[graphql(guard = "AdminGuard")]
    async fn set_post_processing_script_directory(
        &self,
        ctx: &Context<'_>,
        directory: String,
    ) -> Result<PostProcessingSettingsGql> {
        let requested = PathBuf::from(directory.trim());
        let script_directory = tokio::task::spawn_blocking(move || {
            normalize_script_directory(&requested)
                .map_err(|error| async_graphql::Error::new(error.to_string()))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))??;

        let db = ctx.data::<Database>()?.clone();
        let saved_directory = script_directory.clone();
        let (settings, lists, script_directory, changed) = tokio::task::spawn_blocking(move || {
            let changed = db.replace_post_processing_script_directory(&saved_directory)?;
            Ok::<_, weaver_server_core::StateError>((
                db.post_processing_settings()?,
                db.post_processing_script_lists()?,
                db.post_processing_script_directory()?,
                changed,
            ))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        if changed {
            ctx.data::<PostProcessingExecutor>()?
                .set_script_directory(script_directory.clone());
        }
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            lists,
            script_directory.to_string_lossy(),
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
        tokio::task::spawn_blocking(move || {
            let script_directory = db
                .post_processing_script_directory()
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let discovered = resolve_script(&script_directory, &script)
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
        let metadata = history
            .metadata
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<(String, String)>>(raw).ok())
            .unwrap_or_default();
        let admission = executor
            .admit_job_scripts(history.category.as_deref(), &metadata)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .ok_or_else(|| {
                async_graphql::Error::new("post-processing script execution is disabled")
            })?;
        if !admission.has_enabled_entries() {
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
                .execute_admitted_job(job_id, admission, context, None, None)
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
