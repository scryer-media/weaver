use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use super::*;
use chrono::Utc;
use weaver_server_core::post_processing::discovery::{
    DiscoveryOptions, approve_discovered_extension, discover_and_record_extensions,
};
use weaver_server_core::post_processing::model::{
    AttemptStatus, ExtensionAdapter, ExtensionId, ExtensionRevisionId, NonZeroTimeoutSeconds,
    PipelineOutcome, PostProcessingSummary, ProfileId, ResolvedOption, RunId, RunStatus,
    TimeoutPolicy, TrustState,
};
use weaver_server_core::post_processing::runner::{
    ExecutionDisposition, ExtensionExecutionRequest, InterpreterConfig, JobExecutionContext,
    execute_extension,
};

#[derive(Default)]
pub(crate) struct PostProcessingMutation;

#[Object]
impl PostProcessingMutation {
    #[graphql(guard = "AdminGuard")]
    async fn update_post_processing_settings(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingSettingsInput,
    ) -> Result<PostProcessingSettingsGql> {
        let settings =
            weaver_server_core::post_processing::model::PostProcessingSettings::from(input);
        settings
            .validate()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let saved = settings.clone();
        tokio::task::spawn_blocking(move || db.save_post_processing_settings(&saved))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(settings.into())
    }

    #[graphql(guard = "AdminGuard")]
    async fn discover_post_processing_extensions(
        &self,
        ctx: &Context<'_>,
        bare_script_adapter: Option<PostProcessingAdapterInput>,
    ) -> Result<Vec<PostProcessingExtensionRevision>> {
        let db = ctx.data::<Database>()?.clone();
        let settings = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.post_processing_settings())
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
        };
        if !settings.discovery_enabled {
            return Err(async_graphql::Error::new(
                "post-processing discovery is disabled",
            ));
        }
        let data_dir = {
            let config = ctx.data::<SharedConfig>()?;
            PathBuf::from(config.read().await.data_dir.clone())
        };
        let discovered = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || {
                discover_and_record_extensions(
                    &db,
                    &data_dir,
                    DiscoveryOptions {
                        enabled: true,
                        bare_script_adapter: bare_script_adapter.map(Into::into),
                    },
                    Utc::now().timestamp_millis(),
                )
            })
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
        };
        let mut records = Vec::with_capacity(discovered.len());
        for extension in discovered {
            let revision = extension.manifest.revision();
            let extension_id = revision.extension_id().clone();
            let revision_id = revision.revision_id().clone();
            let db = db.clone();
            if let Some(record) = tokio::task::spawn_blocking(move || {
                db.extension_revision(&extension_id, &revision_id)
            })
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            {
                records.push(record.into());
            }
        }
        Ok(records)
    }

    #[graphql(guard = "AdminGuard")]
    async fn run_post_processing_diagnostic(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingDiagnosticInput,
    ) -> Result<PostProcessingDiagnosticResult> {
        let extension_id = ExtensionId::new(input.extension_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let revision_id = ExtensionRevisionId::new(input.revision_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let (record, settings) = tokio::task::spawn_blocking(move || {
            let record = db
                .extension_revision(&extension_id, &revision_id)?
                .ok_or_else(|| {
                    weaver_server_core::StateError::Database(
                        "post-processing revision not found".into(),
                    )
                })?;
            let settings = db.post_processing_settings()?;
            Ok::<_, weaver_server_core::StateError>((record, settings))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        if record.trust_state != TrustState::Approved {
            return Err(async_graphql::Error::new(
                "diagnostics require an approved extension revision",
            ));
        }
        match record.manifest.adapter() {
            ExtensionAdapter::Webhook if !settings.webhooks_enabled => {
                return Err(async_graphql::Error::new("webhook extensions are disabled"));
            }
            _ => {}
        }
        if !record
            .manifest
            .commands()
            .iter()
            .any(|command| command.name().as_str() == input.command)
        {
            return Err(async_graphql::Error::new(
                "diagnostic command is not declared by this revision",
            ));
        }
        let managed_path = record
            .managed_path
            .map(PathBuf::from)
            .ok_or_else(|| async_graphql::Error::new("approved package is unavailable"))?;

        let mut supplied = input
            .options
            .into_iter()
            .map(PostProcessingOptionInput::into_domain)
            .collect::<Result<Vec<_>, _>>()
            .map_err(async_graphql::Error::new)?;
        let mut seen = HashSet::new();
        if supplied
            .iter()
            .any(|option| !seen.insert(option.name().as_str().to_string()))
        {
            return Err(async_graphql::Error::new(
                "diagnostic options contain a duplicate name",
            ));
        }
        let mut options = Vec::with_capacity(record.manifest.options().len());
        for definition in record.manifest.options() {
            if let Some(index) = supplied
                .iter()
                .position(|option| option.name() == definition.name())
            {
                let option = supplied.remove(index);
                if !option.value().matches_type(definition.option_type()) {
                    return Err(async_graphql::Error::new(format!(
                        "diagnostic option {} has the wrong type",
                        definition.name().as_str()
                    )));
                }
                options.push(option);
            } else if let Some(default) = definition.default() {
                options.push(ResolvedOption::new(
                    definition.name().clone(),
                    default.clone(),
                ));
            } else if definition.required() {
                return Err(async_graphql::Error::new(format!(
                    "diagnostic option {} is required",
                    definition.name().as_str()
                )));
            }
        }
        if let Some(option) = supplied.first() {
            return Err(async_graphql::Error::new(format!(
                "diagnostic option {} is not declared by this revision",
                option.name().as_str()
            )));
        }

        let interpreters = InterpreterConfig {
            python: settings.python_interpreter.map(PathBuf::from),
            powershell: settings.powershell_interpreter.map(PathBuf::from),
            batch: settings.batch_interpreter.map(PathBuf::from),
        };
        let request = ExtensionExecutionRequest {
            attempt_id: format!("diagnostic-{}", Utc::now().timestamp_millis()),
            manifest: record.manifest,
            managed_path: managed_path.clone(),
            options,
            approved_roots: vec![],
            context: JobExecutionContext {
                job_id: 0,
                name: "Extension diagnostic".into(),
                nzb_filename: String::new(),
                category: None,
                group: None,
                source_url: None,
                working_directory: managed_path.clone(),
                final_directory: managed_path,
                pipeline_outcome: PipelineOutcome::Succeeded,
                par_status: 0,
                unpack_status: 0,
            },
            timeout_policy: TimeoutPolicy::Finite(
                NonZeroTimeoutSeconds::new(300).expect("five minutes is non-zero"),
            ),
            termination_grace: Duration::from_secs(settings.termination_grace_seconds),
            interpreters,
            control_token: None,
            diagnostic_command: Some(input.command),
            supervisor_executable: None,
        };
        let result = execute_extension(request, None)
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(PostProcessingDiagnosticResult {
            succeeded: matches!(result.disposition, ExecutionDisposition::Succeeded),
            exit_code: result.exit_code,
            error_message: result.error_message,
            output_truncated: result.output_truncated,
            output: result
                .output
                .into_iter()
                .map(|line| PostProcessingDiagnosticOutputLine {
                    sequence: line.sequence,
                    stream: format!("{:?}", line.stream).to_ascii_uppercase(),
                    text: String::from_utf8_lossy(&line.bytes).into_owned(),
                })
                .collect(),
        })
    }

    #[graphql(guard = "AdminGuard")]
    async fn approve_post_processing_revision(
        &self,
        ctx: &Context<'_>,
        extension_id: String,
        revision_id: String,
    ) -> Result<PostProcessingExtensionRevision> {
        let extension_id = ExtensionId::new(extension_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let revision_id = ExtensionRevisionId::new(revision_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let data_dir = {
            let config = ctx.data::<SharedConfig>()?;
            PathBuf::from(config.read().await.data_dir.clone())
        };
        let db = ctx.data::<Database>()?.clone();
        let db_for_approval = db.clone();
        let approval_extension_id = extension_id.clone();
        let approval_revision_id = revision_id.clone();
        tokio::task::spawn_blocking(move || {
            approve_discovered_extension(
                &db_for_approval,
                &data_dir,
                &approval_extension_id,
                &approval_revision_id,
                Utc::now().timestamp_millis(),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let record =
            tokio::task::spawn_blocking(move || db.extension_revision(&extension_id, &revision_id))
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
                .ok_or_else(|| async_graphql::Error::new("approved revision disappeared"))?;
        Ok(record.into())
    }

    #[graphql(guard = "AdminGuard")]
    async fn disable_post_processing_revision(
        &self,
        ctx: &Context<'_>,
        extension_id: String,
        revision_id: String,
    ) -> Result<bool> {
        set_revision_enabled(ctx, extension_id, revision_id, false).await
    }

    #[graphql(guard = "AdminGuard")]
    async fn revoke_post_processing_revision(
        &self,
        ctx: &Context<'_>,
        extension_id: String,
        revision_id: String,
    ) -> Result<bool> {
        set_revision_enabled(ctx, extension_id, revision_id, true).await
    }

    #[graphql(guard = "AdminGuard")]
    async fn save_post_processing_profile(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingProfileInput,
    ) -> Result<PostProcessingProfile> {
        let db = ctx.data::<Database>()?.clone();
        let settings = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.post_processing_settings())
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
        };
        let (profile, enabled) = input
            .into_domain(&settings)
            .map_err(async_graphql::Error::new)?;
        let profile_id = profile.id().clone();
        let db_for_save = db.clone();
        tokio::task::spawn_blocking(move || {
            db_for_save.save_post_processing_profile(
                &profile,
                enabled,
                Utc::now().timestamp_millis(),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let record = tokio::task::spawn_blocking(move || db.post_processing_profile(&profile_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .ok_or_else(|| async_graphql::Error::new("saved profile disappeared"))?;
        Ok(record.into())
    }

    #[graphql(guard = "AdminGuard")]
    async fn delete_post_processing_profile(
        &self,
        ctx: &Context<'_>,
        profile_id: String,
    ) -> Result<bool> {
        let profile_id = ProfileId::new(profile_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.delete_post_processing_profile(&profile_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))
    }

    #[graphql(guard = "AdminGuard")]
    async fn assign_global_post_processing_profile(
        &self,
        ctx: &Context<'_>,
        profile_id: Option<String>,
    ) -> Result<bool> {
        let profile_id = profile_id
            .map(ProfileId::new)
            .transpose()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            db.assign_global_post_processing_profile(profile_id.as_ref())
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "AdminGuard")]
    async fn assign_category_post_processing_profile(
        &self,
        ctx: &Context<'_>,
        category: String,
        profile_id: Option<String>,
    ) -> Result<bool> {
        let profile_id = profile_id
            .map(ProfileId::new)
            .transpose()
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            db.assign_category_post_processing_profile(&category, profile_id.as_ref())
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn set_job_post_processing_selection(
        &self,
        ctx: &Context<'_>,
        job_id: u64,
        selection: PostProcessingSelectionInput,
    ) -> Result<PostProcessingJobPlan> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let job = handle
            .get_job(JobId(job_id))
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let selection = selection.into_domain().map_err(async_graphql::Error::new)?;
        let category = job.category.clone();
        let db = ctx.data::<Database>()?.clone();
        let db_for_resolution = db.clone();
        let plan = tokio::task::spawn_blocking(move || {
            db_for_resolution.resolve_post_processing_plan(Some(&selection), category.as_deref())
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let plan_for_save = plan.clone();
        let replaced = tokio::task::spawn_blocking(move || {
            db.replace_frozen_post_processing_plan(
                job_id,
                &plan_for_save,
                Utc::now().timestamp_millis(),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        if !replaced {
            return Err(async_graphql::Error::new(
                "job plan is unavailable or post-processing has already started",
            ));
        }
        Ok(PostProcessingJobPlan {
            job_id,
            definition: async_graphql::Json(
                serde_json::to_value(plan).unwrap_or(serde_json::Value::Null),
            ),
        })
    }

    #[graphql(guard = "ControlGuard")]
    async fn pause_post_processing_queue(&self, ctx: &Context<'_>) -> Result<bool> {
        ctx.data::<SchedulerHandle>()?
            .pause_post_processing()
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn resume_post_processing_queue(&self, ctx: &Context<'_>) -> Result<bool> {
        ctx.data::<SchedulerHandle>()?
            .resume_post_processing()
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn reorder_post_processing_queue(
        &self,
        ctx: &Context<'_>,
        run_ids: Vec<String>,
    ) -> Result<Vec<String>> {
        let service =
            ctx.data::<weaver_server_core::post_processing::service::PostProcessingService>()?;
        service
            .reorder_queued_runs(&run_ids)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(service.queued_run_ids())
    }

    #[graphql(guard = "ControlGuard")]
    async fn cancel_job_post_processing(&self, ctx: &Context<'_>, job_id: u64) -> Result<bool> {
        ctx.data::<SchedulerHandle>()?
            .cancel_post_processing(JobId(job_id))
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        Ok(true)
    }

    #[graphql(guard = "ControlGuard")]
    async fn rerun_post_processing(
        &self,
        ctx: &Context<'_>,
        input: PostProcessingRerunInput,
    ) -> Result<PostProcessingRun> {
        let source_run_id = RunId::new(input.run_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        let db = ctx.data::<Database>()?.clone();
        let db_for_load = db.clone();
        let source_run_id_for_load = source_run_id.clone();
        let (source_run, attempts, history, settings) = tokio::task::spawn_blocking(move || {
            let source_run = db_for_load
                .post_processing_run(&source_run_id_for_load)?
                .ok_or_else(|| {
                    weaver_server_core::StateError::Database(
                        "post-processing run not found".to_string(),
                    )
                })?;
            let attempts = db_for_load.post_processing_attempts(&source_run_id_for_load)?;
            let history = db_for_load
                .get_job_history(source_run.job_id)?
                .ok_or_else(|| {
                    weaver_server_core::StateError::Database(
                        "script-only reruns require a terminal history job".to_string(),
                    )
                })?;
            let settings = db_for_load.post_processing_settings()?;
            Ok::<_, weaver_server_core::StateError>((source_run, attempts, history, settings))
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        if !settings.execution_enabled {
            return Err(async_graphql::Error::new(
                "post-processing execution is disabled",
            ));
        }

        let plan = if input.rebind_to_latest_approved {
            let db_for_resolution = db.clone();
            let historical_plan = source_run.plan.clone();
            tokio::task::spawn_blocking(move || {
                db_for_resolution.rebind_frozen_post_processing_plan(&historical_plan)
            })
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
        } else {
            source_run.plan.clone()
        };

        let selected_steps: HashSet<u32> = match input.mode {
            PostProcessingRerunModeInput::All => {
                plan.steps().iter().map(|step| step.index()).collect()
            }
            PostProcessingRerunModeInput::Selected => {
                if input.step_indexes.is_empty() {
                    return Err(async_graphql::Error::new(
                        "SELECTED reruns require at least one step index",
                    ));
                }
                input.step_indexes.into_iter().collect()
            }
            PostProcessingRerunModeInput::FailedAndLater => {
                let failed_index = attempts
                    .iter()
                    .filter(|attempt| {
                        matches!(
                            attempt.status,
                            AttemptStatus::Failed
                                | AttemptStatus::TimedOut
                                | AttemptStatus::Cancelled
                                | AttemptStatus::Interrupted
                        )
                    })
                    .map(|attempt| attempt.step_index)
                    .min()
                    .ok_or_else(|| {
                        async_graphql::Error::new("source run has no failed or interrupted attempt")
                    })?;
                plan.steps()
                    .iter()
                    .filter(|step| step.index() >= failed_index)
                    .map(|step| step.index())
                    .collect()
            }
        };
        if !selected_steps
            .iter()
            .all(|index| plan.steps().iter().any(|step| step.index() == *index))
        {
            return Err(async_graphql::Error::new(
                "rerun contains a step index that is not present in the frozen plan",
            ));
        }

        let working_directory = history
            .output_dir
            .as_deref()
            .map(PathBuf::from)
            .ok_or_else(|| {
                async_graphql::Error::new("terminal job has no retained output directory")
            })?;
        let pipeline_outcome = source_run.pipeline_outcome.clone();
        let context = weaver_server_core::post_processing::runner::JobExecutionContext {
            job_id: source_run.job_id,
            name: history.name.clone(),
            nzb_filename: format!("{}.nzb", history.name),
            category: history.category.clone(),
            group: None,
            source_url: None,
            working_directory: working_directory.clone(),
            final_directory: working_directory,
            pipeline_outcome: pipeline_outcome.clone(),
            par_status: i32::from(matches!(pipeline_outcome, PipelineOutcome::Succeeded)) * 2,
            unpack_status: i32::from(matches!(
                source_run.pipeline_outcome,
                PipelineOutcome::Succeeded
            )) * 2,
        };
        let new_run_id = {
            let db_for_create = db.clone();
            let plan_for_create = plan.clone();
            let pipeline_outcome = source_run.pipeline_outcome.clone();
            let terminal_intent = source_run.terminal_intent;
            let source_run_id = source_run.run_id.clone();
            let job_id = source_run.job_id;
            tokio::task::spawn_blocking(move || {
                db_for_create.create_post_processing_run(
                    job_id,
                    &plan_for_create,
                    &pipeline_outcome,
                    terminal_intent,
                    Some(&source_run_id),
                    Utc::now().timestamp_millis(),
                )
            })
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
        };
        let service = ctx
            .data::<weaver_server_core::post_processing::service::PostProcessingService>()?
            .clone();
        let interpreters = weaver_server_core::post_processing::runner::InterpreterConfig {
            python: settings.python_interpreter.map(PathBuf::from),
            powershell: settings.powershell_interpreter.map(PathBuf::from),
            batch: settings.batch_interpreter.map(PathBuf::from),
        };
        let run_id_for_task = new_run_id.clone();
        let db_for_task = db.clone();
        tokio::spawn(async move {
            if let Err(error) = service
                .execute_existing_selected(
                    &run_id_for_task,
                    context,
                    interpreters,
                    None,
                    selected_steps,
                )
                .await
            {
                tracing::error!(run_id = %run_id_for_task.as_str(), error = %error, "script-only post-processing rerun failed");
                if db_for_task
                    .post_processing_run(&run_id_for_task)
                    .ok()
                    .flatten()
                    .is_some_and(|run| {
                        matches!(
                            run.status,
                            RunStatus::Queued | RunStatus::Starting | RunStatus::Running
                        )
                    })
                {
                    let _ = db_for_task.finish_post_processing_run(
                        &run_id_for_task,
                        RunStatus::Interrupted,
                        PostProcessingSummary::Interrupted,
                        Utc::now().timestamp_millis(),
                    );
                }
            }
        });
        let run = db
            .post_processing_run(&new_run_id)?
            .ok_or_else(|| async_graphql::Error::new("created rerun disappeared"))?;
        Ok(run.into())
    }
}

async fn set_revision_enabled(
    ctx: &Context<'_>,
    extension_id: String,
    revision_id: String,
    revoke: bool,
) -> Result<bool> {
    let extension_id = ExtensionId::new(extension_id)
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    let revision_id = ExtensionRevisionId::new(revision_id)
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    let db = ctx.data::<Database>()?.clone();
    tokio::task::spawn_blocking(move || {
        if revoke {
            db.revoke_extension_revision(&extension_id, &revision_id)
        } else {
            db.disable_extension_revision(&extension_id, &revision_id)
        }
    })
    .await
    .map_err(|error| async_graphql::Error::new(error.to_string()))?
    .map_err(|error| async_graphql::Error::new(error.to_string()))
}
