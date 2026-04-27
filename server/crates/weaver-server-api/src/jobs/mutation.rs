use std::io::Read;

use async_graphql::{Context, Object, Result, UploadValue};
use base64::Engine;

use crate::auth::{ControlGuard, graphql_error};
use crate::history::types::{HistoryCommandResult, HistoryItem, history_item_from_row};
use crate::jobs::types::{
    PersistedQueueEvent, QueueCommandResult, QueueEventKind, QueueItem, QueueItemState,
    SubmissionResult, SubmitNzbInput, global_queue_state, queue_item_from_job,
    queue_item_from_submission, submit_metadata,
};
use weaver_server_core::ingest::{
    SubmitNzbError, SubmittedJob, fetch_nzb_from_url, submit_nzb_bytes, submit_uploaded_nzb_reader,
};
use weaver_server_core::jobs::ids::JobId;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{
    Database, FieldUpdate, IntegrationEventRow, JobUpdate, SchedulerError, SchedulerHandle,
};

#[derive(Default)]
pub(crate) struct JobsMutation;

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
        let mut update = JobUpdate::default();
        if let Some(category) = category {
            update.category = if category.is_empty() {
                FieldUpdate::Clear
            } else {
                FieldUpdate::Set(category)
            };
        }
        if let Some(priority) = priority {
            update.metadata = FieldUpdate::Set(vec![("priority".to_string(), priority)]);
        }
        for &id in &ids {
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
        let removed_item = handle
            .get_job(JobId(id))
            .ok()
            .map(|info| queue_item_from_job(&info));
        map_scheduler_result(handle.cancel_job(JobId(id)).await)?;
        persist_removed_event(
            ctx,
            id,
            removed_item.clone(),
            removed_item.as_ref().map(|value| value.state),
        )
        .await?;
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
        for id in &ids {
            map_scheduler_result(handle.delete_history(JobId(*id), delete_files).await)?;
            persist_removed_event(ctx, *id, None, None).await?;
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

async fn persist_removed_event(
    ctx: &Context<'_>,
    id: u64,
    item: Option<QueueItem>,
    state: Option<QueueItemState>,
) -> Result<()> {
    let db = ctx.data::<Database>()?.clone();
    let occurred_at_ms = chrono::Utc::now().timestamp_millis();
    let record = PersistedQueueEvent {
        occurred_at_ms,
        kind: QueueEventKind::ItemRemoved,
        item_id: Some(id),
        item,
        state,
        previous_state: None,
        attention: None,
        global_state: None,
    };
    let payload_json = serde_json::to_string(&record).map_err(|error| {
        graphql_error(
            "INTERNAL",
            format!("failed to encode removal event: {error}"),
        )
    })?;
    tokio::task::spawn_blocking(move || {
        db.insert_integration_events(&[IntegrationEventRow {
            id: 0,
            timestamp: occurred_at_ms,
            kind: "ITEM_REMOVED".to_string(),
            item_id: Some(id),
            payload_json,
        }])
    })
    .await
    .map_err(|error| {
        graphql_error(
            "INTERNAL",
            format!("failed to persist removal event: {error}"),
        )
    })?
    .map_err(|error| {
        graphql_error(
            "INTERNAL",
            format!("failed to persist removal event: {error}"),
        )
    })?;
    Ok(())
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
    let metadata = submit_metadata(input.attributes, input.client_request_id.clone());

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

    if let Ok(info) = handle.get_job(submitted.job_id) {
        return Ok(SubmissionResult {
            accepted: true,
            client_request_id,
            item: queue_item_from_job(&info),
        });
    }

    tokio::task::yield_now().await;
    if let Ok(info) = handle.get_job(submitted.job_id) {
        return Ok(SubmissionResult {
            accepted: true,
            client_request_id,
            item: queue_item_from_job(&info),
        });
    }

    Ok(SubmissionResult {
        accepted: true,
        client_request_id,
        item: queue_item_from_submission(&submitted),
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum UploadEncoding {
    Plain,
    Zstd,
    Gzip,
    Brotli,
    Deflate,
}

fn detect_upload_encoding(upload: &UploadValue) -> UploadEncoding {
    let filename = upload.filename.trim().to_ascii_lowercase();
    if filename.ends_with(".zst") {
        return UploadEncoding::Zstd;
    }
    if filename.ends_with(".gz") || filename.ends_with(".gzip") {
        return UploadEncoding::Gzip;
    }
    if filename.ends_with(".br") {
        return UploadEncoding::Brotli;
    }
    if filename.ends_with(".deflate") {
        return UploadEncoding::Deflate;
    }

    let Some(content_type) = upload.content_type.as_deref() else {
        return UploadEncoding::Plain;
    };
    let normalized = content_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "application/zstd" | "application/x-zstd" | "application/octet-stream+zstd" => {
            UploadEncoding::Zstd
        }
        "application/gzip" | "application/x-gzip" | "application/octet-stream+gzip" => {
            UploadEncoding::Gzip
        }
        "application/brotli" | "application/x-brotli" | "application/octet-stream+brotli" => {
            UploadEncoding::Brotli
        }
        "application/deflate" | "application/x-deflate" | "application/octet-stream+deflate" => {
            UploadEncoding::Deflate
        }
        _ => UploadEncoding::Plain,
    }
}

fn normalize_uploaded_nzb_reader(
    upload: UploadValue,
) -> Result<Box<dyn Read + Send>, SubmitNzbError> {
    let encoding = detect_upload_encoding(&upload);
    let source = upload.into_read();

    match encoding {
        UploadEncoding::Plain => Ok(Box::new(source)),
        UploadEncoding::Zstd => {
            let decoder =
                zstd::stream::read::Decoder::new(source).map_err(SubmitNzbError::Upload)?;
            Ok(Box::new(decoder))
        }
        UploadEncoding::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(source))),
        UploadEncoding::Brotli => Ok(Box::new(brotli::Decompressor::new(source, 64 * 1024))),
        UploadEncoding::Deflate => Ok(Box::new(flate2::read::DeflateDecoder::new(source))),
    }
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
        Ok::<_, weaver_server_core::StateError>(
            rows.into_iter()
                .map(|row| history_item_from_row(&row))
                .collect(),
        )
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))
}
