use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use super::*;
use chrono::Utc;
use weaver_server_core::jobs::{AddJobOptions, JobSpec};
use weaver_server_core::post_processing::discovery::{
    DiscoveryOptions, approve_discovered_extension, discover_and_record_extensions,
};
use weaver_server_core::post_processing::model::{
    AttemptStatus, ExtensionAdapter, ExtensionId, ExtensionRevisionId, FrozenPlan,
    NonZeroTimeoutSeconds, PipelineOutcome, PostProcessingSummary, ProfileId, ResolvedOption,
    RunId, RunStatus, SubmissionPlanSelection, TimeoutPolicy, TrustState,
};
use weaver_server_core::post_processing::persistence::TerminalIntent;
use weaver_server_core::post_processing::runner::{
    CompatibilityFacts, ExecutionDisposition, ExtensionExecutionRequest, InterpreterConfig,
    JobExecutionContext, execute_extension,
};

#[derive(Default)]
pub(crate) struct PostProcessingMutation;

#[derive(SimpleObject)]
struct E2ePostProcessingSeed {
    job_ids: Vec<u64>,
    run_ids: Vec<String>,
}

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
                compatibility:
                    weaver_server_core::post_processing::runner::CompatibilityFacts::default(),
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

    /// Creates deterministic, metadata-only jobs and real queued
    /// post-processing runs for the isolated browser release gate. The first
    /// call omits `job_ids` and prepares two paused jobs so the browser can
    /// exercise selection controls. The second call supplies those exact IDs
    /// after the browser pauses the post-processing queue.
    #[graphql(guard = "AdminGuard")]
    async fn seed_e2e_post_processing_runs(
        &self,
        ctx: &Context<'_>,
        profile_id: String,
        job_ids: Option<Vec<u64>>,
    ) -> Result<E2ePostProcessingSeed> {
        require_e2e_mode()?;
        let profile_id = ProfileId::new(profile_id)
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        match job_ids {
            None => prepare_e2e_post_processing_jobs(ctx, profile_id).await,
            Some(job_ids) => enqueue_e2e_post_processing_runs(ctx, profile_id, job_ids).await,
        }
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
        let (data_dir, intermediate_dir, complete_dir) = {
            let config = ctx.data::<SharedConfig>()?.read().await;
            (
                PathBuf::from(&config.data_dir),
                PathBuf::from(config.intermediate_dir()),
                PathBuf::from(config.complete_dir()),
            )
        };
        let app_dir = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(PathBuf::from));
        let (par_status, unpack_status) = match &pipeline_outcome {
            PipelineOutcome::Failed {
                stage:
                    weaver_server_core::post_processing::model::PipelineFailureStage::Verify
                    | weaver_server_core::post_processing::model::PipelineFailureStage::Repair,
                ..
            } => (1, 0),
            PipelineOutcome::Failed {
                stage: weaver_server_core::post_processing::model::PipelineFailureStage::Extract,
                ..
            } => (0, 1),
            _ => (0, 0),
        };
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
            par_status,
            unpack_status,
            compatibility: weaver_server_core::post_processing::runner::CompatibilityFacts {
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
                app_dir,
                previous_script_status: Default::default(),
            },
        };
        let new_run_id = {
            let db_for_create = db.clone();
            let plan_for_create = plan.clone();
            let pipeline_outcome = source_run.pipeline_outcome.clone();
            let terminal_intent = source_run.terminal_intent;
            let source_run_id = source_run.run_id.clone();
            let job_id = source_run.job_id;
            tokio::task::spawn_blocking(move || {
                db_for_create.create_history_post_processing_rerun(
                    job_id,
                    &plan_for_create,
                    &pipeline_outcome,
                    terminal_intent,
                    &source_run_id,
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

const E2E_MODE_ENV: &str = "WEAVER_E2E_MODE";
const E2E_JOB_CATEGORY: &str = "e2e-post-processing";
const E2E_FIXTURE_METADATA_KEY: &str = "e2e_fixture";
const E2E_FIXTURE_METADATA_VALUE: &str = "post-processing";
const E2E_PROFILE_METADATA_KEY: &str = "e2e_profile_id";

fn e2e_mode_value_enabled(value: Option<&str>) -> bool {
    value.is_some_and(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true"))
}

fn require_e2e_mode() -> Result<()> {
    let value = std::env::var(E2E_MODE_ENV).ok();
    if e2e_mode_value_enabled(value.as_deref()) {
        Ok(())
    } else {
        Err(async_graphql::Error::new(
            "post-processing e2e seeding requires WEAVER_E2E_MODE=1",
        ))
    }
}

fn validate_e2e_seed_job_ids(job_ids: &[u64]) -> Result<()> {
    if job_ids.len() != 2 || job_ids[0] == job_ids[1] || job_ids.contains(&0) {
        return Err(async_graphql::Error::new(
            "post-processing e2e seeding requires exactly two distinct non-zero job IDs",
        ));
    }
    Ok(())
}

fn e2e_post_processing_metadata(profile_id: &ProfileId) -> Vec<(String, String)> {
    vec![
        (
            E2E_FIXTURE_METADATA_KEY.to_string(),
            E2E_FIXTURE_METADATA_VALUE.to_string(),
        ),
        (
            E2E_PROFILE_METADATA_KEY.to_string(),
            profile_id.as_str().to_string(),
        ),
    ]
}

fn e2e_post_processing_job_spec(ordinal: u8, profile_id: &ProfileId) -> JobSpec {
    JobSpec {
        name: format!("E2E post-processing job {ordinal}"),
        password: None,
        files: vec![],
        total_bytes: 0,
        category: Some(E2E_JOB_CATEGORY.to_string()),
        metadata: e2e_post_processing_metadata(profile_id),
    }
}

fn validate_e2e_job_identity(
    metadata: &[(String, String)],
    category: Option<&str>,
    total_files: u32,
    total_bytes: u64,
    profile_id: &ProfileId,
) -> Result<()> {
    let expected = e2e_post_processing_metadata(profile_id)
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let actual = metadata.iter().cloned().collect::<BTreeMap<_, _>>();
    if actual != expected
        || actual.len() != metadata.len()
        || category != Some(E2E_JOB_CATEGORY)
        || total_files != 0
        || total_bytes != 0
    {
        return Err(async_graphql::Error::new(
            "post-processing e2e seeding rejected a job not owned by this fixture and profile",
        ));
    }
    Ok(())
}

fn validate_e2e_job_plan(actual: &FrozenPlan, expected: &FrozenPlan) -> Result<()> {
    if actual != expected {
        return Err(async_graphql::Error::new(
            "post-processing e2e job frozen plan does not match the requested profile",
        ));
    }
    Ok(())
}

fn e2e_metadata_json(metadata: &[(String, String)]) -> Result<String> {
    let metadata = metadata.iter().cloned().collect::<BTreeMap<_, _>>();
    serde_json::to_string(&metadata).map_err(|error| async_graphql::Error::new(error.to_string()))
}

async fn cleanup_prepared_e2e_jobs(
    db: &Database,
    handle: &SchedulerHandle,
    job_ids: &[u64],
) -> Vec<String> {
    let mut cleanup_errors = Vec::new();
    for job_id in job_ids.iter().rev().copied() {
        if let Err(error) = handle.cancel_job(JobId(job_id)).await {
            cleanup_errors.push(format!("cancel job {job_id}: {error}"));
        }
        let db_for_cleanup = db.clone();
        match tokio::task::spawn_blocking(move || {
            let mut errors = Vec::new();
            if let Err(error) = db_for_cleanup.delete_job_history(job_id) {
                errors.push(format!("delete history {job_id}: {error}"));
            }
            if let Err(error) = db_for_cleanup.delete_frozen_post_processing_plan(job_id) {
                errors.push(format!("delete frozen plan {job_id}: {error}"));
            }
            errors
        })
        .await
        {
            Ok(errors) => cleanup_errors.extend(errors),
            Err(error) => cleanup_errors.push(format!("join cleanup for job {job_id}: {error}")),
        }
    }
    cleanup_errors
}

async fn interrupt_created_e2e_runs(db: &Database, run_ids: &[RunId]) -> Vec<String> {
    let mut cleanup_errors = Vec::new();
    for run_id in run_ids {
        let db_for_cleanup = db.clone();
        let run_id_for_cleanup = run_id.clone();
        let run_id_label = run_id.as_str().to_string();
        match tokio::task::spawn_blocking(move || {
            db_for_cleanup.finish_post_processing_run(
                &run_id_for_cleanup,
                RunStatus::Interrupted,
                PostProcessingSummary::Interrupted,
                Utc::now().timestamp_millis(),
            )
        })
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(error)) => cleanup_errors.push(format!(
                "interrupt post-processing run {}: {error}",
                run_id_label
            )),
            Err(error) => cleanup_errors.push(format!(
                "join cleanup for post-processing run {}: {error}",
                run_id_label
            )),
        }
    }
    cleanup_errors
}

fn error_with_cleanup_failures(
    error: async_graphql::Error,
    cleanup_errors: Vec<String>,
) -> async_graphql::Error {
    if cleanup_errors.is_empty() {
        error
    } else {
        async_graphql::Error::new(format!(
            "{}; e2e cleanup also failed: {}",
            error.message,
            cleanup_errors.join("; ")
        ))
    }
}

async fn prepare_e2e_post_processing_jobs(
    ctx: &Context<'_>,
    profile_id: ProfileId,
) -> Result<E2ePostProcessingSeed> {
    let db = ctx.data::<Database>()?.clone();
    let db_for_plan = db.clone();
    let selection = SubmissionPlanSelection::profile(profile_id.clone());
    let plan = tokio::task::spawn_blocking(move || {
        db_for_plan.resolve_post_processing_plan(Some(&selection), None)
    })
    .await
    .map_err(|error| async_graphql::Error::new(error.to_string()))?
    .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    if plan.steps().is_empty() {
        return Err(async_graphql::Error::new(
            "post-processing e2e profile must contain at least one step",
        ));
    }

    let handle = ctx.data::<SchedulerHandle>()?.clone();
    let data_dir = PathBuf::from(ctx.data::<SharedConfig>()?.read().await.data_dir.clone());
    let mut job_ids = Vec::with_capacity(2);
    let prepare_result: Result<()> = async {
        for ordinal in 1_u8..=2 {
            let db_for_id = db.clone();
            let job_id = tokio::task::spawn_blocking(move || db_for_id.reserve_next_job_id())
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let spec = e2e_post_processing_job_spec(ordinal, &profile_id);
            let name = spec.name.clone();
            let history_metadata = e2e_metadata_json(&spec.metadata)?;
            let nzb_path = data_dir.join(format!("e2e-post-processing-{}.nzb", job_id.0));
            job_ids.push(job_id.0);
            handle
                .add_job_with_options(
                    job_id,
                    spec,
                    nzb_path,
                    vec![],
                    AddJobOptions {
                        initially_paused: true,
                        ..AddJobOptions::default()
                    },
                )
                .await
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;

            let info = handle
                .get_job(job_id)
                .map_err(|error| async_graphql::Error::new(error.to_string()))?;
            let output_dir = info.output_dir.clone().ok_or_else(|| {
                async_graphql::Error::new("post-processing e2e job has no working directory")
            })?;
            let db_for_state = db.clone();
            let plan_for_state = plan.clone();
            let history = JobHistoryRow {
                job_id: job_id.0,
                job_hash: info.job_hash.map(|hash| hash.to_vec()),
                name,
                status: "complete".to_string(),
                error_message: None,
                total_bytes: 0,
                downloaded_bytes: 0,
                optional_recovery_bytes: 0,
                optional_recovery_downloaded_bytes: 0,
                failed_bytes: 0,
                health: 1_000,
                category: info.category,
                output_dir: Some(output_dir),
                nzb_path: None,
                created_at: Utc::now().timestamp(),
                completed_at: Utc::now().timestamp(),
                metadata: Some(history_metadata),
            };
            tokio::task::spawn_blocking(move || {
                db_for_state.save_frozen_post_processing_plan(
                    job_id.0,
                    &plan_for_state,
                    Utc::now().timestamp_millis(),
                )?;
                db_for_state.insert_job_history(&history)
            })
            .await
            .map_err(|error| async_graphql::Error::new(error.to_string()))?
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = prepare_result {
        let cleanup_errors = cleanup_prepared_e2e_jobs(&db, &handle, &job_ids).await;
        return Err(error_with_cleanup_failures(error, cleanup_errors));
    }

    Ok(E2ePostProcessingSeed {
        job_ids,
        run_ids: vec![],
    })
}

async fn enqueue_e2e_post_processing_runs(
    ctx: &Context<'_>,
    profile_id: ProfileId,
    job_ids: Vec<u64>,
) -> Result<E2ePostProcessingSeed> {
    validate_e2e_seed_job_ids(&job_ids)?;
    let service = ctx
        .data::<weaver_server_core::post_processing::service::PostProcessingService>()?
        .clone();
    if !service.is_paused() {
        return Err(async_graphql::Error::new(
            "pause the post-processing queue before e2e runs are seeded",
        ));
    }

    let db = ctx.data::<Database>()?.clone();
    let db_for_plan = db.clone();
    let selection = SubmissionPlanSelection::profile(profile_id.clone());
    let expected_plan = tokio::task::spawn_blocking(move || {
        db_for_plan.resolve_post_processing_plan(Some(&selection), None)
    })
    .await
    .map_err(|error| async_graphql::Error::new(error.to_string()))?
    .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    if expected_plan.steps().is_empty() {
        return Err(async_graphql::Error::new(
            "post-processing e2e profile must contain at least one step",
        ));
    }

    let db_for_settings = db.clone();
    let settings = tokio::task::spawn_blocking(move || db_for_settings.post_processing_settings())
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    if !settings.execution_enabled {
        return Err(async_graphql::Error::new(
            "post-processing execution must be enabled before e2e runs are seeded",
        ));
    }

    let handle = ctx.data::<SchedulerHandle>()?.clone();
    let mut prepared_runs = Vec::with_capacity(job_ids.len());
    for job_id in &job_ids {
        let info = handle
            .get_job(JobId(*job_id))
            .map_err(|error| async_graphql::Error::new(error.to_string()))?;
        validate_e2e_job_identity(
            &info.metadata,
            info.category.as_deref(),
            info.total_files,
            info.total_bytes,
            &profile_id,
        )?;
        let working_directory = info
            .output_dir
            .as_deref()
            .map(PathBuf::from)
            .ok_or_else(|| {
                async_graphql::Error::new("post-processing e2e job has no working directory")
            })?;
        let db_for_plan = db.clone();
        let job_id_for_plan = *job_id;
        let plan = tokio::task::spawn_blocking(move || {
            db_for_plan.frozen_post_processing_plan(job_id_for_plan)
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .map_err(|error| async_graphql::Error::new(error.to_string()))?
        .ok_or_else(|| async_graphql::Error::new("post-processing e2e job plan disappeared"))?;
        validate_e2e_job_plan(&plan, &expected_plan)?;
        let context = JobExecutionContext {
            job_id: *job_id,
            name: info.name,
            nzb_filename: format!("e2e-post-processing-{job_id}.nzb"),
            category: info.category,
            group: None,
            source_url: None,
            working_directory: working_directory.clone(),
            final_directory: working_directory,
            pipeline_outcome: PipelineOutcome::Succeeded,
            par_status: 0,
            unpack_status: 0,
            compatibility: CompatibilityFacts::default(),
        };
        prepared_runs.push((*job_id, plan, context));
    }

    let mut created_runs: Vec<(RunId, JobExecutionContext)> =
        Vec::with_capacity(prepared_runs.len());
    for (job_id, plan, context) in prepared_runs {
        let db_for_run = db.clone();
        let creation_result = tokio::task::spawn_blocking(move || {
            db_for_run.create_post_processing_run(
                job_id,
                &plan,
                &PipelineOutcome::Succeeded,
                TerminalIntent::Complete,
                None,
                Utc::now().timestamp_millis(),
            )
        })
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))
        .and_then(|result| result.map_err(|error| async_graphql::Error::new(error.to_string())));
        let run_id = match creation_result {
            Ok(run_id) => run_id,
            Err(error) => {
                let created_run_ids = created_runs
                    .iter()
                    .map(|(run_id, _)| run_id.clone())
                    .collect::<Vec<_>>();
                let cleanup_errors = interrupt_created_e2e_runs(&db, &created_run_ids).await;
                return Err(error_with_cleanup_failures(error, cleanup_errors));
            }
        };
        created_runs.push((run_id, context));
    }

    let created_run_ids = created_runs
        .iter()
        .map(|(run_id, _)| run_id.clone())
        .collect::<Vec<_>>();
    let run_ids = created_run_ids
        .iter()
        .map(|run_id| run_id.as_str().to_string())
        .collect::<Vec<_>>();
    for (run_id, context) in created_runs {
        let interpreters = InterpreterConfig {
            python: settings.python_interpreter.clone().map(PathBuf::from),
            powershell: settings.powershell_interpreter.clone().map(PathBuf::from),
            batch: settings.batch_interpreter.clone().map(PathBuf::from),
        };
        let service_for_task = service.clone();
        let db_for_task = db.clone();
        let run_id_for_task = run_id.clone();
        tokio::spawn(async move {
            if let Err(error) = service_for_task
                .execute_existing(&run_id_for_task, context, interpreters, None)
                .await
            {
                tracing::error!(
                    run_id = %run_id_for_task.as_str(),
                    error = %error,
                    "e2e post-processing run failed"
                );
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
    }

    let expected = run_ids.iter().cloned().collect::<HashSet<_>>();
    let entered_queue = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let queued = service.queued_run_ids().into_iter().collect::<HashSet<_>>();
            if expected.is_subset(&queued) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    if entered_queue.is_err() {
        for job_id in &job_ids {
            service.cancel_job(*job_id);
        }
        let cleanup_errors = interrupt_created_e2e_runs(&db, &created_run_ids).await;
        return Err(error_with_cleanup_failures(
            async_graphql::Error::new("post-processing e2e runs did not enter the queue"),
            cleanup_errors,
        ));
    }

    Ok(E2ePostProcessingSeed { job_ids, run_ids })
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

#[cfg(test)]
mod e2e_seed_tests {
    use super::{
        E2E_JOB_CATEGORY, e2e_mode_value_enabled, e2e_post_processing_job_spec,
        validate_e2e_job_identity, validate_e2e_job_plan, validate_e2e_seed_job_ids,
    };
    use weaver_server_core::post_processing::model::{FrozenPlan, FrozenPlanProvenance, ProfileId};

    #[test]
    fn e2e_seed_mode_requires_an_explicit_true_value() {
        assert!(e2e_mode_value_enabled(Some("1")));
        assert!(e2e_mode_value_enabled(Some(" TRUE ")));
        assert!(!e2e_mode_value_enabled(None));
        assert!(!e2e_mode_value_enabled(Some("")));
        assert!(!e2e_mode_value_enabled(Some("false")));
        assert!(!e2e_mode_value_enabled(Some("yes")));
        assert!(!e2e_mode_value_enabled(Some("0")));
    }

    #[test]
    fn e2e_seed_queue_contract_requires_two_distinct_jobs() {
        assert!(validate_e2e_seed_job_ids(&[10_000, 10_001]).is_ok());
        assert!(validate_e2e_seed_job_ids(&[]).is_err());
        assert!(validate_e2e_seed_job_ids(&[10_000]).is_err());
        assert!(validate_e2e_seed_job_ids(&[10_000, 10_000]).is_err());
        assert!(validate_e2e_seed_job_ids(&[0, 10_001]).is_err());
    }

    #[test]
    fn e2e_seed_jobs_are_zero_file_metadata_only_fixtures() {
        let profile_id = ProfileId::new("e2e-profile").unwrap();
        let spec = e2e_post_processing_job_spec(1, &profile_id);

        assert!(
            spec.files.is_empty(),
            "zero files means no NNTP article work"
        );
        assert_eq!(spec.total_bytes, 0);
        assert!(
            validate_e2e_job_identity(
                &spec.metadata,
                spec.category.as_deref(),
                0,
                spec.total_bytes,
                &profile_id,
            )
            .is_ok()
        );
    }

    #[test]
    fn e2e_seed_job_ownership_is_exact_and_profile_bound() {
        let profile_id = ProfileId::new("e2e-profile").unwrap();
        let other_profile_id = ProfileId::new("other-profile").unwrap();
        let spec = e2e_post_processing_job_spec(1, &profile_id);

        assert!(
            validate_e2e_job_identity(
                &spec.metadata,
                spec.category.as_deref(),
                0,
                0,
                &other_profile_id,
            )
            .is_err()
        );

        let mut extra_metadata = spec.metadata.clone();
        extra_metadata.push(("unowned".to_string(), "value".to_string()));
        assert!(
            validate_e2e_job_identity(&extra_metadata, Some(E2E_JOB_CATEGORY), 0, 0, &profile_id,)
                .is_err()
        );
        assert!(
            validate_e2e_job_identity(&spec.metadata, Some("downloads"), 0, 0, &profile_id,)
                .is_err()
        );
        assert!(
            validate_e2e_job_identity(&spec.metadata, Some(E2E_JOB_CATEGORY), 1, 1, &profile_id,)
                .is_err()
        );
    }

    #[test]
    fn e2e_seed_rejects_a_different_frozen_plan() {
        let expected = FrozenPlan::new(FrozenPlanProvenance::Empty, vec![]).unwrap();
        let different = FrozenPlan::new(FrozenPlanProvenance::Disabled, vec![]).unwrap();

        assert!(validate_e2e_job_plan(&expected, &expected).is_ok());
        assert!(validate_e2e_job_plan(&different, &expected).is_err());
    }
}
