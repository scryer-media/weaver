use std::collections::{HashMap, HashSet};

use async_graphql::{Context, Object, Result, UploadValue};
use base64::Engine;

use crate::auth::{CallerIdentity, ControlGuard, graphql_error};
use crate::history::types::{
    AcceptHistoryDeleteInput, HistoryCommandResult, HistoryDeleteAcceptance, HistoryItem,
    history_delete_row_state_from_core, history_item_from_row,
};
use crate::jobs::staging::{StagedUploadManager, normalize_uploaded_nzb_reader};
use crate::jobs::types::{
    DuplicateDecisionInfo, DuplicateModeGql, PRIORITY_ATTRIBUTE_KEY, PersistedQueueEvent,
    QueueCommandResult, QueueEventKind, QueueMoveKind, SemanticDuplicateAdmissionInfo,
    SemanticPromotionResult, StageNzbUploadInput, StagedNzbSubmissionResult, StagedNzbUploadResult,
    SubmissionResult, SubmissionStatus, SubmitNzbInput, SubmitStagedNzbsInput,
    SubmitStagedNzbsResult, global_queue_state, normalize_priority_value, queue_item_from_job,
    queue_item_from_submission, submit_metadata,
};
use crate::{ScheduledResumeCoordinator, ScheduledResumeError};
use weaver_server_core::ingest::{
    SubmissionDuplicateOutcome, SubmissionOptions, SubmitNzbError, SubmittedJob,
    fetch_nzb_from_url, materialize_semantic_promotion, submit_nzb_bytes_with_options,
    submit_staged_nzb_zstd_with_options, submit_uploaded_nzb_reader_with_options,
};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::jobs::{
    CallerScopedIdempotency, DuplicateAction, DuplicateMode, SemanticDuplicate, SubmissionOrigin,
};
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{
    Database, FieldUpdate, JobUpdate, QueueMoveTarget, SchedulerError, SchedulerHandle,
};

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
        let db = ctx.data::<Database>()?;
        let config = ctx.data::<SharedConfig>()?;
        let manager = ctx.data::<StagedUploadManager>()?;
        let caller_identity = caller_identity(ctx)?;
        let client_request_id = input.client_request_id.clone();
        let metadata = submit_metadata(input.attributes, client_request_id.clone())
            .map_err(|message| graphql_error("INVALID_INPUT", message))?;
        let category = input.category.clone();
        let password = input.password.clone();
        let idempotency_key = input.idempotency_key.clone();
        let force = input.force;
        let dupe_key = input.dupe_key.clone();
        let dupe_score = input.dupe_score;
        let dupe_mode = input.dupe_mode;
        let post_processing_selection = input
            .post_processing
            .clone()
            .map(crate::post_processing::types::PostProcessingSelectionInput::into_domain)
            .transpose()
            .map_err(|message| graphql_error("INVALID_INPUT", message))?;
        let db_for_plan = db.clone();
        let frozen_post_processing_plan = tokio::task::spawn_blocking(move || {
            db_for_plan.freeze_submission_post_processing_plan(post_processing_selection.as_ref())
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INVALID_INPUT", error.to_string()))?;

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
                    status: None,
                    item: None,
                    semantic_duplicate: None,
                    error: Some(error),
                });
                continue;
            };

            let staged_idempotency_key = idempotency_key
                .as_deref()
                .or(client_request_id.as_deref())
                .map(|key| format!("{key}:staged:{staged_upload_id}"));
            let mut options = graphql_submission_options(
                &caller_identity,
                None,
                staged_idempotency_key.as_deref(),
                force,
                dupe_key.as_deref(),
                dupe_score,
                dupe_mode,
            );
            options.frozen_post_processing_plan = frozen_post_processing_plan.clone();

            match submit_staged_nzb_zstd_with_options(
                db,
                handle,
                config,
                entry.nzb_zstd.clone(),
                Some(entry.filename.clone()),
                password.clone(),
                category.clone(),
                metadata.clone(),
                options,
            )
            .await
            {
                Ok(submitted) => {
                    let result = submission_result_from_submitted(
                        handle,
                        client_request_id.clone(),
                        &submitted,
                    )
                    .await;
                    if result.accepted {
                        accepted_count += 1;
                    }
                    results.push(StagedNzbSubmissionResult {
                        staged_upload_id,
                        accepted: result.accepted,
                        retained: false,
                        status: Some(result.status),
                        item: result.item,
                        semantic_duplicate: result.semantic_duplicate,
                        error: None,
                    });
                }
                Err(error) => {
                    let structured = submission_result_from_error(client_request_id.clone(), error);
                    manager.restore_entry(entry);
                    let (status, semantic_duplicate, error) = match structured {
                        Ok(result) => (
                            Some(result.status),
                            result.semantic_duplicate,
                            result.message.unwrap_or_else(|| {
                                result
                                    .error_code
                                    .unwrap_or_else(|| "submission rejected".to_string())
                            }),
                        ),
                        Err(error) => (None, None, error.to_string()),
                    };
                    results.push(StagedNzbSubmissionResult {
                        staged_upload_id,
                        accepted: false,
                        retained: true,
                        status,
                        item: None,
                        semantic_duplicate,
                        error: Some(error),
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
    #[graphql(guard = "ControlGuard")]
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
    /// Explicitly forget durable duplicate identity for a job without deleting history or files.
    #[graphql(guard = "ControlGuard")]
    async fn forget_duplicate_identity(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.forget_duplicate_identity(JobId(id)))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))
    }
    #[graphql(guard = "ControlGuard")]
    async fn mark_duplicate_good(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.mark_semantic_candidate_good(JobId(id)))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))
    }

    #[graphql(guard = "ControlGuard")]
    async fn mark_duplicate_bad(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<SemanticPromotionResult> {
        let db = ctx.data::<Database>()?.clone();
        let handle = ctx.data::<SchedulerHandle>()?;
        let candidate = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.semantic_candidate_snapshot(JobId(id))
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        if candidate.is_none()
            || candidate.is_some_and(|candidate| {
                matches!(
                    candidate.state,
                    weaver_server_core::SemanticCandidateState::Suppressed
                )
            })
        {
            return Ok(SemanticPromotionResult {
                accepted: false,
                job_id: None,
                message: Some("semantic candidate not found or suppressed".to_string()),
            });
        }
        if let Ok(info) = handle.get_job(JobId(id))
            && !matches!(
                (info.download_state, info.post_state),
                (
                    weaver_server_core::DownloadState::Queued
                        | weaver_server_core::DownloadState::Downloading,
                    weaver_server_core::PostState::Idle
                )
            )
        {
            return Ok(SemanticPromotionResult {
                accepted: false,
                job_id: None,
                message: Some(
                    "semantic mark-bad cannot interrupt post-processing or a paused job"
                        .to_string(),
                ),
            });
        }
        // The scheduler performs the same check in its command loop. Keep the
        // database unchanged unless that final, race-free cancellation succeeds.
        match handle.cancel_semantic_bad(JobId(id)).await {
            Ok(()) | Err(SchedulerError::JobNotFound(_)) => {}
            Err(SchedulerError::Conflict(message)) => {
                return Ok(SemanticPromotionResult {
                    accepted: false,
                    job_id: None,
                    message: Some(message),
                });
            }
            Err(error) => return Err(graphql_error("CONFLICT", error.to_string())),
        }
        let marked = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.mark_semantic_candidate_bad(JobId(id))
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
        if !marked {
            return Ok(SemanticPromotionResult {
                accepted: false,
                job_id: None,
                message: Some("semantic candidate not found or already suppressed".to_string()),
            });
        }
        materialize_claim_for_api(&db, handle, JobId(id)).await
    }

    #[graphql(guard = "ControlGuard")]
    async fn promote_duplicate_candidate(
        &self,
        ctx: &Context<'_>,
        id: u64,
    ) -> Result<SemanticPromotionResult> {
        let db = ctx.data::<Database>()?.clone();
        let handle = ctx.data::<SchedulerHandle>()?;
        materialize_claim_for_api(&db, handle, JobId(id)).await
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
    /// Set or clear a queued job's archive/unpack password. Passing null or
    /// an empty string removes the password (even one that came from the NZB
    /// itself); the override is durable across restarts. The password never
    /// appears in job attributes — only `hasPassword` reflects it.
    #[graphql(guard = "ControlGuard")]
    async fn set_job_password(
        &self,
        ctx: &Context<'_>,
        id: u64,
        password: Option<String>,
    ) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let password = password.filter(|value| !value.is_empty());
        let update = JobUpdate {
            password: match password {
                Some(password) => FieldUpdate::Set(password),
                None => FieldUpdate::Clear,
            },
            ..JobUpdate::default()
        };
        map_scheduler_result(handle.update_job(JobId(id), update).await)?;
        Ok(true)
    }
    /// Move a queue item within the manual queue order. The move breaks ties
    /// within a dispatch priority band; HIGH/NORMAL/LOW still dominates.
    /// `offset` is required when `kind` is OFFSET (negative moves toward the
    /// front).
    #[graphql(guard = "ControlGuard")]
    async fn reorder_queue_item(
        &self,
        ctx: &Context<'_>,
        id: u64,
        kind: QueueMoveKind,
        offset: Option<i64>,
    ) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let target = match kind {
            QueueMoveKind::Top => QueueMoveTarget::Top,
            QueueMoveKind::Bottom => QueueMoveTarget::Bottom,
            QueueMoveKind::Offset => QueueMoveTarget::Offset(offset.ok_or_else(|| {
                graphql_error("INVALID_INPUT", "offset is required when kind is OFFSET")
            })?),
        };
        map_scheduler_result(handle.reorder_job(JobId(id), target).await)?;
        Ok(true)
    }
    /// Pause all download dispatch globally.
    #[graphql(guard = "ControlGuard")]
    async fn pause_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let scheduled_resume = ctx.data::<ScheduledResumeCoordinator>()?;
        map_scheduled_resume_result(scheduled_resume.pause_all().await)?;
        Ok(true)
    }
    /// Resume all download dispatch globally.
    #[graphql(guard = "ControlGuard")]
    async fn resume_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let scheduled_resume = ctx.data::<ScheduledResumeCoordinator>()?;
        map_scheduled_resume_result(scheduled_resume.resume_all().await)?;
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
        let scheduled_resume = ctx.data::<ScheduledResumeCoordinator>()?;
        map_scheduled_resume_result(scheduled_resume.pause_all().await)?;
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
        let scheduled_resume = ctx.data::<ScheduledResumeCoordinator>()?;
        map_scheduled_resume_result(scheduled_resume.resume_all().await)?;
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
        SchedulerError::SemanticSuperseded => graphql_error(
            "PARKED",
            "semantic candidate was superseded before materialization",
        ),
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

fn map_scheduled_resume_result<T>(
    result: std::result::Result<T, ScheduledResumeError>,
) -> Result<T> {
    result.map_err(|error| graphql_error("SCHEDULER_ERROR", error.to_string()))
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

fn caller_idempotency_scope(caller: &CallerIdentity) -> String {
    match caller {
        CallerIdentity::Local(value) => format!("graphql:local:{}", hex::encode(value)),
        CallerIdentity::Jwt(value) => format!("graphql:jwt:{}", hex::encode(value)),
        CallerIdentity::ApiKey(value) => format!("graphql:api-key:{}", hex::encode(value)),
    }
}

async fn materialize_claim_for_api(
    db: &Database,
    handle: &SchedulerHandle,
    trigger_job_id: JobId,
) -> Result<SemanticPromotionResult> {
    let db_for_claim = db.clone();
    let claim =
        tokio::task::spawn_blocking(move || db_for_claim.claim_semantic_promotion(trigger_job_id))
            .await
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
            .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;
    let Some(claim) = claim else {
        return Ok(SemanticPromotionResult {
            accepted: false,
            job_id: None,
            message: Some("no eligible parked semantic candidate".to_string()),
        });
    };
    match materialize_semantic_promotion(db, handle, claim).await {
        Ok(job_id) => Ok(SemanticPromotionResult {
            accepted: true,
            job_id: Some(job_id.0),
            message: None,
        }),
        Err(error) => Ok(SemanticPromotionResult {
            accepted: false,
            job_id: None,
            message: Some(error.to_string()),
        }),
    }
}

fn graphql_submission_options(
    caller: &CallerIdentity,
    client_request_id: Option<&str>,
    idempotency_key: Option<&str>,
    force: Option<bool>,
    dupe_key: Option<&str>,
    dupe_score: Option<i64>,
    dupe_mode: Option<DuplicateModeGql>,
) -> SubmissionOptions {
    let key = idempotency_key
        .or(client_request_id)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|key| CallerScopedIdempotency::new(caller_idempotency_scope(caller), key));

    SubmissionOptions {
        duplicate_mode: if force.unwrap_or(false) {
            DuplicateMode::Force
        } else {
            dupe_mode.map(Into::into).unwrap_or(DuplicateMode::Enforce)
        },
        semantic_duplicate: dupe_key
            .and_then(|key| SemanticDuplicate::from_source(key, dupe_score.unwrap_or_default())),
        origin: SubmissionOrigin::Api,
        idempotency: key,
        ..SubmissionOptions::default()
    }
}

fn submission_status(outcome: &SubmissionDuplicateOutcome) -> SubmissionStatus {
    match outcome {
        SubmissionDuplicateOutcome::IdempotentReplay => SubmissionStatus::IdempotentReplay,
        SubmissionDuplicateOutcome::Parked { .. } => SubmissionStatus::Parked,
        SubmissionDuplicateOutcome::Accepted { decision, .. } if decision.force_bypassed => {
            SubmissionStatus::ForceAccepted
        }
        SubmissionDuplicateOutcome::Accepted { decision, .. } => match decision.action {
            DuplicateAction::Accept => SubmissionStatus::Accepted,
            DuplicateAction::Warn => SubmissionStatus::Warned,
            DuplicateAction::Pause => SubmissionStatus::Paused,
            DuplicateAction::Block => SubmissionStatus::Blocked,
        },
    }
}

async fn submission_result_from_submitted(
    handle: &SchedulerHandle,
    client_request_id: Option<String>,
    submitted: &SubmittedJob,
) -> SubmissionResult {
    let status = submission_status(&submitted.duplicate_outcome);
    let item = match submitted.duplicate_outcome {
        SubmissionDuplicateOutcome::IdempotentReplay => handle
            .get_job(submitted.job_id)
            .ok()
            .map(|info| queue_item_from_job(&info)),
        SubmissionDuplicateOutcome::Accepted { .. } => {
            Some(submission_result_item(handle, submitted).await)
        }
        SubmissionDuplicateOutcome::Parked { .. } => None,
    };
    let duplicate_decision = match &submitted.duplicate_outcome {
        SubmissionDuplicateOutcome::Accepted { decision, .. }
        | SubmissionDuplicateOutcome::Parked { decision, .. } => {
            Some(DuplicateDecisionInfo::from(decision))
        }
        SubmissionDuplicateOutcome::IdempotentReplay => None,
    };
    let semantic_duplicate = match &submitted.duplicate_outcome {
        SubmissionDuplicateOutcome::Accepted {
            semantic: Some(semantic),
            ..
        }
        | SubmissionDuplicateOutcome::Parked { semantic, .. } => {
            Some(SemanticDuplicateAdmissionInfo::from(semantic))
        }
        SubmissionDuplicateOutcome::Accepted { semantic: None, .. }
        | SubmissionDuplicateOutcome::IdempotentReplay => None,
    };

    SubmissionResult {
        accepted: !matches!(status, SubmissionStatus::IdempotentReplay),
        status,
        job_id: Some(submitted.job_id.0),
        client_request_id,
        item,
        duplicate_decision,
        semantic_duplicate,
        error_code: None,
        message: matches!(status, SubmissionStatus::Parked)
            .then(|| "semantic duplicate candidate parked".to_string()),
    }
}

fn submission_result_from_error(
    client_request_id: Option<String>,
    error: SubmitNzbError,
) -> std::result::Result<SubmissionResult, SubmitNzbError> {
    match error {
        SubmitNzbError::DuplicateBlocked { decision } => Ok(SubmissionResult {
            accepted: false,
            status: SubmissionStatus::Blocked,
            job_id: None,
            client_request_id,
            item: None,
            duplicate_decision: Some(DuplicateDecisionInfo::from(&decision)),
            semantic_duplicate: None,
            error_code: Some("DUPLICATE_BLOCKED".to_string()),
            message: Some("duplicate submission blocked".to_string()),
        }),
        SubmitNzbError::IdempotencyConflict { job_id } => Ok(SubmissionResult {
            accepted: false,
            status: SubmissionStatus::IdempotencyConflict,
            job_id: Some(job_id),
            client_request_id,
            item: None,
            duplicate_decision: None,
            semantic_duplicate: None,
            error_code: Some("IDEMPOTENCY_CONFLICT".to_string()),
            message: Some(format!("idempotency key is already bound to job {job_id}")),
        }),
        other => Err(other),
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
    mut input: SubmitNzbInput,
) -> Result<SubmissionResult> {
    let handle = ctx.data::<SchedulerHandle>()?;
    let db = ctx.data::<Database>()?;
    let config = ctx.data::<SharedConfig>()?;
    let caller = caller_identity(ctx)?;
    let post_processing_selection = input
        .post_processing
        .take()
        .map(crate::post_processing::types::PostProcessingSelectionInput::into_domain)
        .transpose()
        .map_err(|message| graphql_error("INVALID_INPUT", message))?;
    let db_for_plan = db.clone();
    let frozen_post_processing_plan = tokio::task::spawn_blocking(move || {
        db_for_plan.freeze_submission_post_processing_plan(post_processing_selection.as_ref())
    })
    .await
    .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
    .map_err(|error| graphql_error("INVALID_INPUT", error.to_string()))?;

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
    let mut options = graphql_submission_options(
        &caller,
        client_request_id.as_deref(),
        input.idempotency_key.as_deref(),
        input.force,
        input.dupe_key.as_deref(),
        input.dupe_score,
        input.dupe_mode,
    );
    options.frozen_post_processing_plan = frozen_post_processing_plan;
    let metadata = submit_metadata(input.attributes, input.client_request_id.clone())
        .map_err(|message| graphql_error("INVALID_INPUT", message))?;

    let submitted = if let Some(upload) = upload {
        submit_uploaded_nzb(
            db,
            handle,
            config,
            upload,
            filename,
            input.password,
            category,
            metadata,
            options,
        )
        .await
    } else {
        submit_nzb_bytes_with_options(
            db,
            handle,
            config,
            &nzb_bytes.expect("nzb bytes should be present"),
            filename,
            input.password,
            category,
            metadata,
            options,
        )
        .await
    };

    match submitted {
        Ok(submitted) => {
            Ok(submission_result_from_submitted(handle, client_request_id, &submitted).await)
        }
        Err(error) => submission_result_from_error(client_request_id, error)
            .map_err(|e| graphql_error("INVALID_INPUT", e.to_string())),
    }
}

#[allow(clippy::too_many_arguments)]
async fn submit_uploaded_nzb(
    db: &Database,
    handle: &SchedulerHandle,
    config: &SharedConfig,
    upload: UploadValue,
    filename: Option<String>,
    password: Option<String>,
    category: Option<String>,
    metadata: Vec<(String, String)>,
    options: SubmissionOptions,
) -> Result<SubmittedJob, SubmitNzbError> {
    submit_uploaded_nzb_reader_with_options(
        db,
        handle,
        config,
        normalize_uploaded_nzb_reader(upload)?,
        filename,
        password,
        category,
        metadata,
        options,
    )
    .await
}

async fn history_items_from_db(db: Database) -> Result<Vec<HistoryItem>> {
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&weaver_server_core::HistoryFilter::default())?;
        let ids = rows.iter().map(|row| row.job_id).collect::<Vec<_>>();
        let delete_states = db.list_history_delete_row_states(&ids)?;
        Ok::<_, weaver_server_core::StateError>(
            rows.into_iter()
                .map(|row| {
                    let delete_state = delete_states
                        .get(&row.job_id)
                        .cloned()
                        .map(history_delete_row_state_from_core);
                    history_item_from_row(&row, delete_state)
                })
                .collect(),
        )
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))
}
