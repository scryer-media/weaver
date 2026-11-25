// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use weaver_server_core::history::queries::HistoryQueries;
use weaver_server_core::post_processing::events::PostProcessingEventsCancelEventScriptsQueries;
use weaver_server_core::post_processing::instances::PostProcessingInstancesQueries;
use weaver_server_core::post_processing::preset::PostProcessingPresetQueries;
use weaver_server_core::post_processing::secrets::PostProcessingSecretsQueries;
use weaver_server_core::post_processing::settings::PostProcessingSettingsQueries;
use weaver_server_core::post_processing::test_run::PostProcessingTestRunQueries;

use super::*;
use crate::auth::{AdminGuard, FreshAdminGuard, graphql_error, invalid_input, not_found};
use crate::observability::{scheduler_error, spawn_blocking_with, state_code, state_error};
use weaver_server_core::post_processing::executor::{
    PostProcessingExecutor, strict_security_enabled,
};
use weaver_server_core::post_processing::instances::ScriptInstance;
use weaver_server_core::post_processing::instances::ScriptInstanceError;
use weaver_server_core::post_processing::listing::ListingError;
use weaver_server_core::post_processing::listing::resolve_script;
use weaver_server_core::post_processing::model::{
    PipelineOutcome, PostProcessingSettings, ScriptName,
};
use weaver_server_core::post_processing::preset::HeaderSetupError;
use weaver_server_core::post_processing::runner::{CompatibilityFacts, JobExecutionContext};
use weaver_server_core::post_processing::secrets::SecretError;
use weaver_server_core::post_processing::settings::ScriptDirectoryError;
use weaver_server_core::post_processing::settings::normalize_script_directory;
use weaver_server_core::post_processing::test_run::ScriptTestError;
use weaver_server_core::post_processing::test_run::start_script_test;

fn parse_script_name(value: String) -> Result<ScriptName> {
    ScriptName::new(value).map_err(|error| invalid_input(error.to_string()))
}

// Run blocking store work off the async threads; its errors are already the
// answer.
async fn blocking<T, F>(work: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    spawn_blocking_with(work, |error| error).await
}

// A script job can only be pointed at a script that is there to run. One whose
// script has since gone can still be edited, so long as it keeps naming it.
fn require_script(db: &Database, script: &ScriptName) -> Result<()> {
    let directory = db.post_processing_script_directory().map_err(state_error)?;
    resolve_script(&directory, script)
        .map(|_| ())
        .map_err(listing_error)
}

// A named script that is not there to run. A manifest the script directory
// holds that does not parse fits no code: it is neither the caller's input
// nor a fault inside Weaver.
pub(super) fn listing_error(error: ListingError) -> async_graphql::Error {
    let message = error.to_string();
    match error {
        ListingError::NotFound(_) => not_found(message),
        ListingError::Validation(_) => invalid_input(message),
        ListingError::Io(_) => graphql_error("INTERNAL", message),
        ListingError::Manifest(_) => async_graphql::Error::new(message),
    }
}

fn instance_error(error: ScriptInstanceError) -> async_graphql::Error {
    let message = error.to_string();
    match error {
        ScriptInstanceError::Invalid(_) | ScriptInstanceError::NoOwnSecret(_) => {
            invalid_input(message)
        }
        ScriptInstanceError::NotFound => not_found(message),
        ScriptInstanceError::Storage(state) => state_error(state),
    }
}

// A header the script carries that cannot be read, or declares nothing, fits
// no code: the first arrives as a sentence with its cause already lost.
fn header_error(error: HeaderSetupError) -> async_graphql::Error {
    match error {
        HeaderSetupError::Instance(error) => instance_error(error),
        HeaderSetupError::Storage(state) => state_error(state),
        error @ (HeaderSetupError::Script(_) | HeaderSetupError::NothingDeclared) => {
            async_graphql::Error::new(error.to_string())
        }
    }
}

// A test the settings refuse, one whose script is not there, and one past
// the limit of runs fit no code; a test that could not be set up is a fault.
fn script_test_error(error: ScriptTestError) -> async_graphql::Error {
    let message = error.to_string();
    match error {
        ScriptTestError::NotFound => not_found(message),
        ScriptTestError::Setup(_) => graphql_error("INTERNAL", message),
        ScriptTestError::Refused(_) | ScriptTestError::Unavailable(_) | ScriptTestError::Busy => {
            async_graphql::Error::new(message)
        }
    }
}

// The rerun's preconditions on the job and its scripts.
fn rerun_refused(message: &'static str) -> async_graphql::Error {
    graphql_error("CONFLICT", message)
}

// The messages carry names only, never a secret's value.
fn secret_error(error: SecretError) -> async_graphql::Error {
    let message = error.to_string();
    match error {
        SecretError::Invalid(_) => graphql_error("INVALID_INPUT", message),
        SecretError::NameTaken => graphql_error("NAME_TAKEN", message),
        SecretError::InUse(_) => graphql_error("SECRET_IN_USE", message),
        SecretError::NotFound => graphql_error("NOT_FOUND", message),
        SecretError::Storage(state) => graphql_error(state_code(&state), message),
    }
}

fn view(db: &Database, instance: ScriptInstance) -> ScriptInstanceGql {
    crate::post_processing::query::directory_view(db).instance(instance)
}

#[derive(Default)]
pub(crate) struct PostProcessingMutation;

#[Object]
impl PostProcessingMutation {
    #[graphql(guard = "FreshAdminGuard")]
    async fn set_post_processing_settings(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingSettingsInput,
    ) -> Result<PostProcessingSettingsGql> {
        let PostProcessingSettingsInput {
            event_script_timeout_seconds,
            file_downloaded_event_interval,
            script_output_runs_per_job,
            script_output_failed_runs_per_job,
            execution_enabled,
            concurrency,
            termination_grace_seconds,
            python_interpreter,
            powershell_interpreter,
            batch_interpreter,
            go_interpreter,
            unacceptable_extensions,
            global_scripts_run,
        } = input;
        if unacceptable_extensions.is_null() {
            return Err(invalid_input(
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
        let (settings, script_directory) = spawn_blocking_with(
            move || {
                let (unacceptable_extensions, preserve_extensions) = match unacceptable_extensions {
                    async_graphql::MaybeUndefined::Undefined => (Vec::new(), true),
                    async_graphql::MaybeUndefined::Value(extensions) => (extensions, false),
                    async_graphql::MaybeUndefined::Null => unreachable!("checked before worker"),
                };
                let current = db.post_processing_settings()?;
                let mut event_scripts = current.event_scripts;
                if let Some(value) = event_script_timeout_seconds {
                    event_scripts.event_script_timeout_seconds = value;
                }
                if let Some(value) = file_downloaded_event_interval {
                    event_scripts.file_downloaded_event_interval = value;
                }
                if let Some(value) = script_output_runs_per_job {
                    event_scripts.script_output_runs_per_job = value;
                }
                if let Some(value) = script_output_failed_runs_per_job {
                    event_scripts.script_output_failed_runs_per_job = value;
                }
                let settings = PostProcessingSettings {
                    event_scripts,
                    execution_enabled,
                    concurrency,
                    termination_grace_seconds,
                    python_interpreter,
                    powershell_interpreter,
                    batch_interpreter,
                    go_interpreter,
                    unacceptable_extensions,
                    // Saved on its own (`set_unwanted_extension_action`); the
                    // save keeps the stored one.
                    unwanted_extension_action: current.unwanted_extension_action,
                    global_scripts_run: global_scripts_run
                        .map(Into::into)
                        .unwrap_or(current.global_scripts_run),
                };
                let settings = db.save_post_processing_settings_preserving_extensions(
                    settings,
                    preserve_extensions,
                )?;
                Ok::<_, weaver_server_core::StateError>((
                    settings,
                    db.post_processing_script_directory()?,
                ))
            },
            state_error,
        )
        .await?;
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            script_directory.to_string_lossy(),
            strict_security_enabled(),
        ))
    }

    /// Replace the unwanted extension list and nothing else. An empty list
    /// turns the check off. Returns the list as saved, normalized.
    #[graphql(guard = "AdminGuard")]
    async fn set_unacceptable_extensions(
        &self,
        ctx: &Context<'_>,
        extensions: Vec<String>,
    ) -> Result<Vec<String>> {
        let db = ctx.data::<Database>()?.clone();
        let settings = spawn_blocking_with(
            move || db.save_unacceptable_extensions(extensions),
            state_error,
        )
        .await?;
        Ok(settings.unacceptable_extensions)
    }

    /// Choose what a download holding a file with an unwanted extension
    /// comes to, and nothing else. Returns the choice as saved.
    #[graphql(guard = "AdminGuard")]
    async fn set_unwanted_extension_action(
        &self,
        ctx: &Context<'_>,
        action: UnwantedExtensionActionGql,
    ) -> Result<UnwantedExtensionActionGql> {
        let db = ctx.data::<Database>()?.clone();
        let settings = spawn_blocking_with(
            move || db.save_unwanted_extension_action(action.into()),
            state_error,
        )
        .await?;
        Ok(settings.unwanted_extension_action.into())
    }

    /// Select the sole live source of scripts. Changing it turns every
    /// script job off, because a name in the new directory is not the script
    /// that was wired up. What was saved in them is kept, and script files
    /// are never touched.
    #[graphql(guard = "FreshAdminGuard")]
    async fn set_post_processing_script_directory(
        &self,
        ctx: &Context<'_>,
        directory: String,
    ) -> Result<PostProcessingSettingsGql> {
        let requested = PathBuf::from(directory.trim());
        // A path that is not absolute, or not a directory, is the caller's; one
        // that could not be created or listed is a fault.
        let script_directory = spawn_blocking_with(
            move || normalize_script_directory(&requested),
            |error| {
                let message = error.to_string();
                match error {
                    ScriptDirectoryError::RelativePath | ScriptDirectoryError::NotDirectory(_) => {
                        invalid_input(message)
                    }
                    ScriptDirectoryError::Io(_) => graphql_error("INTERNAL", message),
                }
            },
        )
        .await?;

        let db = ctx.data::<Database>()?.clone();
        let saved_directory = script_directory.clone();
        let (settings, script_directory, changed) = spawn_blocking_with(
            move || {
                let changed = db.replace_post_processing_script_directory(&saved_directory)?;
                Ok::<_, weaver_server_core::StateError>((
                    db.post_processing_settings()?,
                    db.post_processing_script_directory()?,
                    changed,
                ))
            },
            state_error,
        )
        .await?;
        if changed {
            ctx.data::<PostProcessingExecutor>()?
                .set_script_directory(script_directory.clone());
        }
        Ok(PostProcessingSettingsGql::from_settings(
            settings,
            script_directory.to_string_lossy(),
            strict_security_enabled(),
        ))
    }

    /// Wire a script to one trigger. The script has to be in the scripts
    /// directory; what its header declares is not checked, because any script
    /// may be given any trigger and any inputs.
    #[graphql(guard = "FreshAdminGuard")]
    async fn create_script_instance(
        &self,
        ctx: &Context<'_>,
        input: ScriptInstanceInput,
    ) -> Result<ScriptInstanceGql> {
        let draft = input.into_draft().map_err(invalid_input)?;
        let db = ctx.data::<Database>()?.clone();
        blocking(move || {
            require_script(&db, &draft.script)?;
            let instance = db.create_script_instance(draft).map_err(instance_error)?;
            Ok(view(&db, instance))
        })
        .await
    }

    /// Keep a value encrypted under a name, for script inputs to link. The
    /// value can be replaced but never read back. Adding one changes nothing
    /// already saved, so it asks for no recent password check; changing or
    /// removing one does.
    #[graphql(guard = "AdminGuard")]
    async fn create_secret(
        &self,
        ctx: &Context<'_>,
        name: String,
        value: String,
    ) -> Result<SecretGql> {
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(move || db.create_secret(&name, &value), secret_error)
            .await
            .map(Into::into)
    }

    /// Rename a secret, replace its value, or both. Whatever links it follows
    /// along: the next run is handed the new value.
    #[graphql(guard = "FreshAdminGuard")]
    async fn update_secret(
        &self,
        ctx: &Context<'_>,
        id: String,
        name: Option<String>,
        value: Option<String>,
    ) -> Result<SecretGql> {
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(
            move || db.update_secret(&id, name.as_deref(), value.as_deref()),
            secret_error,
        )
        .await
        .map(Into::into)
    }

    /// Remove a secret. Refused while any script job links it.
    #[graphql(guard = "FreshAdminGuard")]
    async fn delete_secret(&self, ctx: &Context<'_>, id: String) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(move || db.delete_secret(&id), secret_error)
            .await
            .map(|()| true)
    }

    /// Replace everything saved in a script job. Each input is sent as a value,
    /// as the secret it links, or as a secret of the script job's own; one of
    /// those sent without a value keeps the secret already saved under that
    /// name. A schedule left out keeps the saved one; a script job that no
    /// longer runs on a schedule loses its run times.
    #[graphql(guard = "FreshAdminGuard")]
    async fn update_script_instance(
        &self,
        ctx: &Context<'_>,
        id: String,
        input: ScriptInstanceInput,
    ) -> Result<ScriptInstanceGql> {
        let keep_schedule = input.schedule.is_none();
        let mut draft = input.into_draft().map_err(invalid_input)?;
        let db = ctx.data::<Database>()?.clone();
        blocking(move || {
            let existing = db
                .script_instance(&id)
                .map_err(state_error)?
                .ok_or_else(|| not_found("script job does not exist"))?;
            if keep_schedule {
                draft.schedule = existing.schedule.clone();
            }
            if existing.script != draft.script {
                require_script(&db, &draft.script)?;
            }
            let instance = db
                .update_script_instance(&id, draft)
                .map_err(instance_error)?;
            Ok(view(&db, instance))
        })
        .await
    }

    /// Remove a script job. False when there was no such script job.
    #[graphql(guard = "FreshAdminGuard")]
    async fn delete_script_instance(&self, ctx: &Context<'_>, id: String) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        spawn_blocking_with(move || db.delete_script_instance(&id), state_error).await
    }

    /// Put the script jobs of one trigger in the order given. They keep the
    /// places they hold among the others, and any that are not named follow
    /// the ones that are.
    #[graphql(guard = "FreshAdminGuard")]
    async fn reorder_script_instances(
        &self,
        ctx: &Context<'_>,
        trigger: ScriptKindGql,
        ids: Vec<String>,
    ) -> Result<Vec<ScriptInstanceGql>> {
        let db = ctx.data::<Database>()?.clone();
        blocking(move || {
            let all = db.script_instances().map_err(state_error)?;
            let in_group =
                |instance: &ScriptInstance| ScriptKindGql::from(instance.trigger.kind()) == trigger;
            let mut named = std::collections::BTreeSet::new();
            for id in &ids {
                if !all
                    .iter()
                    .any(|instance| &instance.id == id && in_group(instance))
                {
                    return Err(invalid_input(format!(
                        "'{id}' is not a script job with that trigger"
                    )));
                }
                if !named.insert(id.as_str()) {
                    return Err(invalid_input(format!("'{id}' is named more than once")));
                }
            }
            let mut group = ids.iter().cloned().chain(
                all.iter()
                    .filter(|instance| in_group(instance) && !named.contains(instance.id.as_str()))
                    .map(|instance| instance.id.clone()),
            );
            let order = all
                .iter()
                .map(|instance| {
                    if in_group(instance) {
                        group.next().expect("one id for each place in the group")
                    } else {
                        instance.id.clone()
                    }
                })
                .collect::<Vec<_>>();
            db.reorder_script_instances(&order).map_err(state_error)?;
            let directory = crate::post_processing::query::directory_view(&db);
            Ok(db
                .script_instances()
                .map_err(state_error)?
                .into_iter()
                .map(|instance| directory.instance(instance))
                .collect())
        })
        .await
    }

    /// Create every script job a script's header asks for and does not have
    /// yet: one per declared trigger, filled from the header, a new schedule
    /// script job carrying every declared run time. Returns the script jobs it
    /// added. After this the header is not read again.
    #[graphql(guard = "FreshAdminGuard")]
    async fn set_up_script_from_header(
        &self,
        ctx: &Context<'_>,
        script: String,
    ) -> Result<Vec<ScriptInstanceGql>> {
        let script = parse_script_name(script)?;
        let db = ctx.data::<Database>()?.clone();
        blocking(move || {
            let setup = db
                .set_up_script_from_header(&script)
                .map_err(header_error)?;
            let directory = crate::post_processing::query::directory_view(&db);
            Ok(setup
                .instances
                .into_iter()
                .map(|instance| directory.instance(instance))
                .collect::<Vec<_>>())
        })
        .await
    }

    /// Bring one script job's inputs back in line with its script's header:
    /// every declared input, at the value already saved or else its default,
    /// and nothing the header no longer declares.
    #[graphql(guard = "FreshAdminGuard")]
    async fn reapply_script_header(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<ScriptInstanceGql> {
        let db = ctx.data::<Database>()?.clone();
        blocking(move || {
            let instance = db.reapply_script_header(&id).map_err(header_error)?;
            Ok(view(&db, instance))
        })
        .await
    }

    /// Run a script job once against made-up inputs: a download that does not
    /// exist, in a scratch directory that is removed afterwards. The script job
    /// supplies the script, the trigger and its saved inputs. Commands the
    /// script prints are reported and never applied. Read the run again with
    /// `scriptTestRun`.
    #[graphql(guard = "AdminGuard")]
    async fn test_script_instance(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<ScriptTestRunGql> {
        start_script_test(
            ctx.data::<Database>()?,
            ctx.data::<SharedConfig>()?,
            id,
            None,
        )
        .await
        .map(Into::into)
        .map_err(script_test_error)
    }

    /// Stop a running test. False when there is no such run or it has ended.
    #[graphql(guard = "AdminGuard")]
    async fn cancel_script_test(&self, ctx: &Context<'_>, id: String) -> Result<bool> {
        Ok(ctx.data::<Database>()?.cancel_script_test(&id))
    }

    /// What the calling run may ask weaver to do for it. Only for a running
    /// script that calls with the token its run was handed as
    /// `WEAVER_RUN_TOKEN`; an error for anyone else, and once that run has
    /// ended.
    async fn script_run(
        &self,
        ctx: &Context<'_>,
    ) -> Result<crate::post_processing::script_run::ScriptRunActionsGql> {
        use crate::post_processing::script_run::{ScriptRunActionsGql, calling_run};
        Ok(ScriptRunActionsGql(calling_run(ctx)?))
    }

    /// Run the job's post-processing script jobs again against its retained
    /// output directory.
    #[graphql(guard = "ControlGuard")]
    async fn rerun_post_processing(&self, ctx: &Context<'_>, job_id: u64) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let executor = ctx.data::<PostProcessingExecutor>()?.clone();
        let db_for_load = db.clone();
        let history = spawn_blocking_with(move || db_for_load.get_job_history(job_id), state_error)
            .await?
            .ok_or_else(|| {
                rerun_refused("post-processing reruns require a terminal history job")
            })?;
        let metadata = history
            .metadata
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<(String, String)>>(raw).ok())
            .unwrap_or_default();
        let admission = executor
            .admit_job_scripts(history.category.as_deref())
            .map_err(state_error)?
            .ok_or_else(|| rerun_refused("post-processing script execution is disabled"))?;
        if !admission.has_enabled_entries() {
            return Err(rerun_refused(
                "no post-processing scripts are configured for this job",
            ));
        }
        let working_directory = history
            .output_dir
            .as_deref()
            .map(PathBuf::from)
            .ok_or_else(|| rerun_refused("terminal job has no retained output directory"))?;
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
                tags: Vec::new(),
                parameters: metadata,
                marked_bad: false,
                final_directory_override: None,
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
        // A job can have scripts nothing waits for and a pass still to stop,
        // so stopping the first does not answer for the second.
        let background = ctx.data::<Database>()?.cancel_background_scripts(job_id);
        if ctx.data::<PostProcessingExecutor>()?.cancel_job(job_id) {
            return Ok(true);
        }
        let pass = ctx
            .data::<SchedulerHandle>()?
            .cancel_post_processing(JobId(job_id))
            .await;
        if !background {
            pass.map_err(scheduler_error)?;
        }
        Ok(true)
    }
}
