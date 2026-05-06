use std::collections::{HashMap, HashSet};

use async_graphql::{Context, Object, Result, UploadValue};
use base64::Engine;

use crate::auth::{CallerIdentity, ControlGuard, graphql_error};
#[cfg(weaver_diagnostics)]
use crate::history::types::DiagnosticRedownloadAcceptance;
use crate::history::types::{
    AcceptHistoryDeleteInput, HistoryCommandResult, HistoryDeleteAcceptance, HistoryItem,
    history_delete_row_state_from_core, history_item_from_row,
};
use crate::jobs::staging::{StagedUploadManager, normalize_uploaded_nzb_reader};
use crate::jobs::types::{
    PRIORITY_ATTRIBUTE_KEY, PersistedQueueEvent, QueueCommandResult, QueueEventKind,
    StageNzbUploadInput, StagedNzbSubmissionResult, StagedNzbUploadResult, SubmissionResult,
    SubmitNzbInput, SubmitStagedNzbsInput, SubmitStagedNzbsResult, global_queue_state,
    normalize_priority_value, queue_item_from_job, queue_item_from_submission, submit_metadata,
};
use weaver_server_core::ingest::{
    SubmitNzbError, SubmittedJob, fetch_nzb_from_url, submit_nzb_bytes, submit_staged_nzb_zstd,
    submit_uploaded_nzb_reader,
};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{Database, FieldUpdate, JobUpdate, SchedulerError, SchedulerHandle};

#[derive(Default)]
pub(crate) struct JobsMutation;

fn upsert_metadata_entry(metadata: &mut Vec<(String, String)>, key: &str, value: String) {
    let canonical_key = key.to_string();
    let mut updated = Vec::with_capacity(metadata.len() + 1);
    let mut replaced = false;

    for (existing_key, existing_value) in metadata.drain(..) {
        if existing_key.eq_ignore_ascii_case(key) {
            if !replaced {
                updated.push((canonical_key.clone(), value.clone()));
                replaced = true;
            }
        } else {
            updated.push((existing_key, existing_value));
        }
    }

    if !replaced {
        updated.push((canonical_key, value));
    }

    *metadata = updated;
}

#[Object]
impl JobsMutation {
    /// Submit an NZB for download through the public integration facade.
    #[graphql(guard = "ControlGuard")]
    async fn submit_nzb(
        &self,
        ctx: &Context<'_>,
        input: SubmitNzbInput,
    ) -> Result<SubmissionResult> {
        submit_from_facade_input(ctx, input).await
    }
    #[graphql(guard = "ControlGuard")]
    async fn stage_nzb_upload(
        &self,
        ctx: &Context<'_>,
        input: StageNzbUploadInput,
    ) -> Result<StagedNzbUploadResult> {
        let manager = ctx.data::<StagedUploadManager>()?;
        let caller_identity = caller_identity(ctx)?;
        let filename = input.filename.clone();
        let upload = match input.nzb_upload.value(ctx) {
            Ok(upload) => upload,
            Err(error) => {
                return Ok(rejected_stage_upload_result(
                    filename,
                    format!("invalid upload: {error}"),
                ));
            }
        };

        match manager
            .stage_upload(caller_identity, upload, input.filename)
            .await
        {
            Ok(staged) => Ok(StagedNzbUploadResult {
                accepted: true,
                staged_upload_id: Some(staged.staged_upload_id),
                filename: Some(staged.filename),
                display_name: Some(staged.display_name),
                total_files: Some(staged.total_files),
                total_bytes: Some(staged.total_bytes),
                error: None,
            }),
            Err(error) => Ok(rejected_stage_upload_result(filename, error.to_string())),
        }
    }
    #[graphql(guard = "ControlGuard")]
    async fn submit_staged_nzbs(
        &self,
        ctx: &Context<'_>,
        input: SubmitStagedNzbsInput,
    ) -> Result<SubmitStagedNzbsResult> {
        if input.staged_upload_ids.is_empty() {
            return Err(graphql_error(
                "INVALID_INPUT",
                "submitStagedNzbs requires at least one staged upload id",
            ));
        }

        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        let manager = ctx.data::<StagedUploadManager>()?;
        let caller_identity = caller_identity(ctx)?;
        let client_request_id = input.client_request_id.clone();
        let metadata = submit_metadata(input.attributes, client_request_id.clone())
            .map_err(|message| graphql_error("INVALID_INPUT", message))?;
        let category = input.category.clone();
        let password = input.password.clone();

        let (entries, missing) =
            manager.take_for_submit(&caller_identity, &input.staged_upload_ids);
        let mut found_by_id = entries
            .into_iter()
            .map(|entry| (entry.id.clone(), entry))
            .collect::<HashMap<_, _>>();
        let missing_ids = missing.into_iter().collect::<HashSet<_>>();
        let mut accepted_count = 0u32;
        let mut results = Vec::with_capacity(input.staged_upload_ids.len());

        for staged_upload_id in input.staged_upload_ids {
            let Some(entry) = found_by_id.remove(&staged_upload_id) else {
                let error = if missing_ids.contains(&staged_upload_id) {
                    "staged upload expired; re-add file".to_string()
                } else {
                    "staged upload not found".to_string()
                };
                results.push(StagedNzbSubmissionResult {
                    staged_upload_id,
                    accepted: false,
                    retained: false,
                    item: None,
                    error: Some(error),
                });
                continue;
            };

            match submit_staged_nzb_zstd(
                handle,
                config,
                entry.nzb_zstd.clone(),
                Some(entry.filename.clone()),
                password.clone(),
                category.clone(),
                metadata.clone(),
            )
            .await
            {
                Ok(submitted) => {
                    accepted_count += 1;
                    let item = submission_result_item(handle, &submitted).await;
                    results.push(StagedNzbSubmissionResult {
                        staged_upload_id,
                        accepted: true,
                        retained: false,
                        item: Some(item),
                        error: None,
                    });
                }
                Err(error) => {
                    manager.restore_entry(entry);
                    results.push(StagedNzbSubmissionResult {
                        staged_upload_id,
                        accepted: false,
                        retained: true,
                        item: None,
                        error: Some(error.to_string()),
                    });
                }
            }
        }

        Ok(SubmitStagedNzbsResult {
            accepted_count,
            client_request_id,
            results,
        })
    }
    #[graphql(guard = "ControlGuard")]
    async fn discard_staged_nzbs(&self, ctx: &Context<'_>, ids: Vec<String>) -> Result<bool> {
        let manager = ctx.data::<StagedUploadManager>()?;
        let caller_identity = caller_identity(ctx)?;
        manager.discard_owned(&caller_identity, &ids);
        Ok(true)
    }
    /// Pause a running job.
    #[graphql(guard = "ControlGuard")]
    async fn pause_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.pause_job(JobId(id)).await?;
        Ok(true)
    }
    /// Resume a paused job.
    #[graphql(guard = "ControlGuard")]
    async fn resume_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.resume_job(JobId(id)).await?;
        Ok(true)
    }
    /// Cancel and remove a job.
    #[graphql(guard = "ControlGuard")]
    async fn cancel_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.cancel_job(JobId(id)).await?;
        Ok(true)
    }
    /// Reprocess a completed or failed job (re-run post-download stages without re-downloading).
    #[graphql(guard = "ControlGuard")]
    async fn reprocess_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.reprocess_job(JobId(id)).await?;
        Ok(true)
    }
    /// Re-download a completed or failed job from its persisted NZB under the same job ID.
    #[graphql(guard = "ControlGuard")]
    async fn redownload_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.redownload_job(JobId(id)).await?;
        Ok(true)
    }
    /// Re-download a terminal history item under a fresh linked job ID and collect a
    /// diagnostic bundle for support.
    #[cfg(weaver_diagnostics)]
    #[graphql(guard = "ControlGuard")]
    async fn start_diagnostic_redownload(
        &self,
        ctx: &Context<'_>,
        id: u64,
        #[graphql(default = true)] include_server_hostnames: bool,
    ) -> Result<DiagnosticRedownloadAcceptance> {
        let manager = ctx.data::<crate::history::diagnostics::DiagnosticManager>()?;
        manager
            .start_diagnostic_redownload(id, include_server_hostnames)
            .await
    }
    /// Delete a completed/failed/cancelled job from history.
    /// Returns the remaining history jobs after deletion.
    #[graphql(guard = "ControlGuard")]
    async fn accept_history_delete(
        &self,
        ctx: &Context<'_>,
        input: AcceptHistoryDeleteInput,
    ) -> Result<HistoryDeleteAcceptance> {
        let manager = ctx.data::<crate::history::delete_ops::HistoryDeleteManager>()?;
        manager.accept_history_delete(input).await
    }
    /// Delete a completed/failed/cancelled job from history.
    /// Returns the remaining history jobs after deletion.
    async fn delete_history(
        &self,
        ctx: &Context<'_>,
        id: u64,
        #[graphql(default = false)] delete_files: bool,
    ) -> Result<Vec<HistoryItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?.clone();
        handle.delete_history(JobId(id), delete_files).await?;
        history_items_from_db(db).await
    }
    /// Delete multiple completed/failed/cancelled jobs from history by ID.
    /// Returns the remaining history jobs after deletion.
    #[graphql(guard = "ControlGuard")]
    async fn delete_history_batch(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        #[graphql(default = false)] delete_files: bool,
    ) -> Result<Vec<HistoryItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?.clone();
        for &id in &ids {
            handle.delete_history(JobId(id), delete_files).await?;
        }
        history_items_from_db(db).await
    }
    /// Delete all completed/failed/cancelled jobs from history.
    #[graphql(guard = "ControlGuard")]
    async fn delete_all_history(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = false)] delete_files: bool,
    ) -> Result<Vec<HistoryItem>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?.clone();
        handle.delete_all_history(delete_files).await?;
        history_items_from_db(db).await
    }
    /// Update category and/or priority for one or more jobs.
    #[graphql(guard = "ControlGuard")]
    async fn update_jobs(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        category: Option<String>,
        priority: Option<String>,
    ) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let mut base_update = JobUpdate::default();
        if let Some(category) = category {
            base_update.category = if category.is_empty() {
                FieldUpdate::Clear
            } else {
                FieldUpdate::Set(category)
            };
        }

        let normalized_priority = priority
            .map(|value| {
                normalize_priority_value(&value)
                    .map_err(|message| graphql_error("INVALID_INPUT", message))
            })
            .transpose()?;

        for &id in &ids {
            let mut update = base_update.clone();
            if let Some(priority) = normalized_priority.as_ref() {
                let mut metadata = map_scheduler_result(handle.get_job(JobId(id)))?.metadata;
                upsert_metadata_entry(&mut metadata, PRIORITY_ATTRIBUTE_KEY, priority.clone());
                update.metadata = FieldUpdate::Set(metadata);
            }
            handle.update_job(JobId(id), update.clone()).await?;
        }
        Ok(true)
    }
    /// Pause all download dispatch globally.
    #[graphql(guard = "ControlGuard")]
    async fn pause_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.pause_all().await?;
        Ok(true)
    }
    /// Resume all download dispatch globally.
    #[graphql(guard = "ControlGuard")]
    async fn resume_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.resume_all().await?;
        Ok(true)
    }
    /// Set the global download speed limit in bytes/sec. 0 means unlimited.
    #[graphql(guard = "ControlGuard")]
    async fn set_speed_limit(&self, ctx: &Context<'_>, bytes_per_sec: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.set_speed_limit(bytes_per_sec).await?;
        Ok(true)
    }
    /// Pause one queue item and return its updated facade state.
    #[graphql(guard = "ControlGuard")]
    async fn pause_queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        map_scheduler_result(handle.pause_job(JobId(id)).await)?;
        let item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item,
            global_state: None,
        })
    }
    /// Resume one queue item and return its updated facade state.
    #[graphql(guard = "ControlGuard")]
    async fn resume_queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        map_scheduler_result(handle.resume_job(JobId(id)).await)?;
        let item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item,
            global_state: None,
        })
    }
    /// Cancel one queue item.
    #[graphql(guard = "ControlGuard")]
    async fn cancel_queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        // Capture the pre-cancel item so the compatibility ITEM_REMOVED event
        // lands after any final pipeline-driven state changes emitted by cancel_job.
        let previous_item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        map_scheduler_result(handle.cancel_job(JobId(id)).await)?;
        replay
            .append(PersistedQueueEvent {
                occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                kind: QueueEventKind::ItemRemoved,
                item_id: Some(id),
                item: previous_item.clone(),
                state: None,
                previous_state: previous_item.as_ref().map(|item| item.state),
                attention: previous_item
                    .as_ref()
                    .and_then(|item| item.attention.clone()),
                global_state: None,
            })
            .await;
        Ok(QueueCommandResult {
            success: true,
            message: Some("item cancelled".to_string()),
            item: None,
            global_state: None,
        })
    }
    /// Reprocess a failed queue item.
    #[graphql(guard = "ControlGuard")]
    async fn reprocess_queue_item(&self, ctx: &Context<'_>, id: u64) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        map_scheduler_result(handle.reprocess_job(JobId(id)).await)?;
        let item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item,
            global_state: None,
        })
    }
    /// Re-download a failed queue item under the same job ID.
    #[graphql(guard = "ControlGuard")]
    async fn redownload_queue_item(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        map_scheduler_result(handle.redownload_job(JobId(id)).await)?;
        let item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item,
            global_state: None,
        })
    }
    /// Remove history items by ID.
    #[graphql(guard = "ControlGuard")]
    async fn remove_history_items(
        &self,
        ctx: &Context<'_>,
        ids: Vec<u64>,
        #[graphql(default = false)] delete_files: bool,
    ) -> Result<HistoryCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let replay = ctx.data::<crate::jobs::replay::QueueEventReplay>()?.clone();
        for id in &ids {
            map_scheduler_result(handle.delete_history(JobId(*id), delete_files).await)?;
            replay
                .append(PersistedQueueEvent {
                    occurred_at_ms: chrono::Utc::now().timestamp_millis(),
                    kind: QueueEventKind::ItemRemoved,
                    item_id: Some(*id),
                    item: None,
                    state: None,
                    previous_state: None,
                    attention: None,
                    global_state: None,
                })
                .await;
        }
        Ok(HistoryCommandResult {
            success: true,
            removed_ids: ids,
        })
    }
    /// Pause the queue globally.
    #[graphql(guard = "ControlGuard")]
    async fn pause_queue(&self, ctx: &Context<'_>) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        map_scheduler_result(handle.pause_all().await)?;
        let cfg = config.read().await;
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item: None,
            global_state: Some(global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                cfg.max_download_speed.unwrap_or(0),
            )),
        })
    }
    /// Resume the queue globally.
    #[graphql(guard = "ControlGuard")]
    async fn resume_queue(&self, ctx: &Context<'_>) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        map_scheduler_result(handle.resume_all().await)?;
        let cfg = config.read().await;
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item: None,
            global_state: Some(global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                cfg.max_download_speed.unwrap_or(0),
            )),
        })
    }
    /// Set the queue speed limit in bytes/sec. 0 means unlimited.
    #[graphql(guard = "ControlGuard")]
    async fn set_queue_speed_limit(
        &self,
        ctx: &Context<'_>,
        bytes_per_sec: u64,
    ) -> Result<QueueCommandResult> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        map_scheduler_result(handle.set_speed_limit(bytes_per_sec).await)?;
        let cfg = config.read().await;
        Ok(QueueCommandResult {
            success: true,
            message: None,
            item: None,
            global_state: Some(global_queue_state(
                handle.is_globally_paused(),
                &handle.get_download_block(),
                cfg.max_download_speed.unwrap_or(bytes_per_sec),
            )),
        })
    }
}

fn scheduler_graphql_error(error: SchedulerError) -> async_graphql::Error {
    match error {
        SchedulerError::JobNotFound(job_id) => {
            graphql_error("NOT_FOUND", format!("job {job_id} not found"))
        }
        SchedulerError::JobExists(job_id) => {
            graphql_error("CONFLICT", format!("job {job_id} already exists"))
        }
        SchedulerError::Conflict(message) => graphql_error("CONFLICT", message),
        SchedulerError::InvalidInput(message) => graphql_error("INVALID_INPUT", message),
        SchedulerError::Internal(message) => graphql_error("INTERNAL", message),
        SchedulerError::Other(message) => graphql_error("INTERNAL", message),
        SchedulerError::ChannelClosed => graphql_error("INTERNAL", "channel closed"),
        SchedulerError::Assembly(error) => {
            graphql_error("INTERNAL", format!("assembly error: {error}"))
        }
        SchedulerError::State(error) => graphql_error("INTERNAL", format!("state error: {error}")),
        SchedulerError::Io(error) => graphql_error("INTERNAL", format!("i/o error: {error}")),
    }
}

fn map_scheduler_result<T>(result: std::result::Result<T, SchedulerError>) -> Result<T> {
    result.map_err(scheduler_graphql_error)
}

fn caller_identity(ctx: &Context<'_>) -> Result<CallerIdentity> {
    ctx.data::<CallerIdentity>()
        .cloned()
        .map_err(|_| graphql_error("INTERNAL", "missing caller identity"))
}

fn rejected_stage_upload_result(filename: Option<String>, error: String) -> StagedNzbUploadResult {
    StagedNzbUploadResult {
        accepted: false,
        staged_upload_id: None,
        filename,
        display_name: None,
        total_files: None,
        total_bytes: None,
        error: Some(error),
    }
}

async fn submission_result_item(
    handle: &SchedulerHandle,
    submitted: &SubmittedJob,
) -> crate::jobs::types::QueueItem {
    if let Ok(info) = handle.get_job(submitted.job_id) {
        return queue_item_from_job(&info);
    }

    tokio::task::yield_now().await;
    if let Ok(info) = handle.get_job(submitted.job_id) {
        return queue_item_from_job(&info);
    }

    queue_item_from_submission(submitted)
}

async fn submit_from_facade_input(
    ctx: &Context<'_>,
    input: SubmitNzbInput,
) -> Result<SubmissionResult> {
    let handle = ctx.data::<SchedulerHandle>()?;
    let config = ctx.data::<SharedConfig>()?;

    let (nzb_bytes, upload, filename) = match (input.nzb_base64, input.url, input.nzb_upload) {
        (Some(b64), None, None) => {
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(&b64)
                .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid base64: {e}")))?;
            (Some(bytes), None, input.filename)
        }
        (None, Some(url), None) => {
            let client = ctx.data::<reqwest::Client>()?;
            let (bytes, url_filename) = fetch_nzb_from_url(client, &url)
                .await
                .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?;
            (Some(bytes), None, input.filename.or(url_filename))
        }
        (None, None, Some(upload)) => {
            let upload = upload
                .value(ctx)
                .map_err(|e| graphql_error("INVALID_INPUT", format!("invalid upload: {e}")))?;
            let upload_filename =
                (!upload.filename.trim().is_empty()).then_some(upload.filename.clone());
            (None, Some(upload), input.filename.or(upload_filename))
        }
        _ => {
            return Err(graphql_error(
                "INVALID_INPUT",
                "submitNzb requires exactly one of url, nzbBase64, or nzbUpload",
            ));
        }
    };

    let client_request_id = input.client_request_id.clone();
    let category = input.category.clone();
    let metadata = submit_metadata(input.attributes, input.client_request_id.clone())
        .map_err(|message| graphql_error("INVALID_INPUT", message))?;

    let submitted = if let Some(upload) = upload {
        submit_uploaded_nzb(
            handle,
            config,
            upload,
            filename,
            input.password,
            category,
            metadata,
        )
        .await
        .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?
    } else {
        submit_nzb_bytes(
            handle,
            config,
            &nzb_bytes.expect("nzb bytes should be present"),
            filename,
            input.password,
            category,
            metadata,
        )
        .await
        .map_err(|e| graphql_error("INVALID_INPUT", e.to_string()))?
    };

    Ok(SubmissionResult {
        accepted: true,
        client_request_id,
        item: submission_result_item(handle, &submitted).await,
    })
}

async fn submit_uploaded_nzb(
    handle: &SchedulerHandle,
    config: &SharedConfig,
    upload: UploadValue,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
) -> Result<SubmittedJob, SubmitNzbError> {
    submit_uploaded_nzb_reader(
        handle,
        config,
        normalize_uploaded_nzb_reader(upload)?,
        filename,
        password,
        category,
        metadata,
    )
    .await
}

async fn history_items_from_db(db: Database) -> Result<Vec<HistoryItem>> {
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
        let ids = rows.iter().map(|row| row.job_id).collect::<Vec<_>>();
        let delete_states = db.list_history_delete_row_states(&ids)?;
        let diagnostic_states = db
            .list_pending_diagnostic_runs()?
            .into_iter()
            .map(|row| (row.source_job_id, row))
            .collect::<std::collections::HashMap<_, _>>();
        Ok::<_, weaver_server_core::StateError>(
            rows.into_iter()
                .map(|row| {
                    let diagnostic_run = diagnostic_states
                        .get(&row.job_id)
                        .cloned()
                        .map(crate::history::types::history_diagnostic_run_from_core);
                    let delete_state = delete_states
                        .get(&row.job_id)
                        .cloned()
                        .map(history_delete_row_state_from_core);
                    history_item_from_row(&row, diagnostic_run, delete_state)
                })
                .collect(),
        )
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))
}
