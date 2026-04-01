use std::path::PathBuf;

use async_graphql::{Context, MaybeUndefined, Object, Result};
use base64::Engine;
use regex::Regex;
use tracing::info;

use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_scheduler::{SchedulerError, SchedulerHandle};
use weaver_state::{Database, IntegrationEventRow, RssFeedRow, RssRuleRow};

use crate::auth::{AdminGuard, ControlGuard, generate_api_key, graphql_error, hash_api_key};
use crate::facade::{
    HistoryCommandResult, HistoryItem, PersistedQueueEvent, QueueCommandResult, QueueEventKind,
    QueueItem, QueueItemState, SubmissionResult, SubmitNzbInput, global_queue_state,
    history_item_from_row, queue_item_from_job, queue_item_from_submission, submit_metadata,
};
use crate::rss::RssService;
use crate::runtime::rebuild_nntp_from_config;
use crate::submit::{fetch_nzb_from_url, submit_nzb_bytes, submit_uploaded_nzb};
use crate::types::{
    ApiKey, ApiKeyScope, Category, CategoryInput, CreateApiKeyResult, GeneralSettings,
    GeneralSettingsInput, RssFeed, RssFeedInput, RssRule, RssRuleActionGql, RssRuleInput,
    RssSyncReport, Server, ServerInput, TestConnectionResult,
};

pub struct MutationRoot;

/// Result of the loginStatus query.
#[derive(async_graphql::SimpleObject)]
pub struct LoginStatusResult {
    pub enabled: bool,
    pub username: Option<String>,
}

fn normalize_settings_path_update(input: &MaybeUndefined<String>) -> MaybeUndefined<String> {
    match input {
        MaybeUndefined::Undefined => MaybeUndefined::Undefined,
        MaybeUndefined::Null => MaybeUndefined::Null,
        MaybeUndefined::Value(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                MaybeUndefined::Null
            } else {
                MaybeUndefined::Value(trimmed.to_string())
            }
        }
    }
}

#[derive(Clone)]
struct NormalizedServerInput {
    host: String,
    port: u16,
    tls: bool,
    username: Option<String>,
    password: Option<String>,
    connections: u16,
    active: bool,
    priority: u16,
    tls_ca_cert: Option<PathBuf>,
}

impl NormalizedServerInput {
    fn from_input(
        input: ServerInput,
        fallback_password: Option<String>,
    ) -> std::result::Result<Self, String> {
        let host = input.host.trim().to_string();
        if host.is_empty() {
            return Err("server host must not be empty".to_string());
        }

        let password = normalize_optional_string(input.password).or(fallback_password);

        Ok(Self {
            host,
            port: input.port,
            tls: input.tls,
            username: normalize_optional_string(input.username),
            password,
            connections: input.connections,
            active: input.active,
            priority: input.priority,
            tls_ca_cert: normalize_optional_string(input.tls_ca_cert).map(PathBuf::from),
        })
    }

    fn as_runtime_server_config(&self, id: u32) -> weaver_core::config::ServerConfig {
        weaver_core::config::ServerConfig {
            id,
            host: self.host.clone(),
            port: self.port,
            tls: self.tls,
            username: self.username.clone(),
            password: self.password.clone(),
            connections: self.connections,
            active: self.active,
            supports_pipelining: false,
            priority: self.priority as u32,
            tls_ca_cert: self.tls_ca_cert.clone(),
        }
    }

    fn as_nntp_server_config(&self) -> weaver_nntp::ServerConfig {
        weaver_nntp::ServerConfig {
            host: self.host.clone(),
            port: self.port,
            tls: self.tls,
            username: self.username.clone(),
            password: self.password.clone(),
            tls_ca_cert: self.tls_ca_cert.clone(),
            ..Default::default()
        }
    }
}

async fn probe_server_connection(config: weaver_nntp::ServerConfig) -> TestConnectionResult {
    let start = std::time::Instant::now();
    match weaver_nntp::NntpConnection::connect(&config).await {
        Ok(mut conn) => {
            let latency = start.elapsed().as_millis() as u64;
            let pipelining = conn.capabilities().supports_pipelining();
            let _ = conn.quit().await;
            TestConnectionResult {
                success: true,
                message: "Connected successfully".to_string(),
                latency_ms: Some(latency),
                supports_pipelining: pipelining,
            }
        }
        Err(error) => TestConnectionResult {
            success: false,
            message: format!("{error}"),
            latency_ms: None,
            supports_pipelining: false,
        },
    }
}

async fn validate_server_before_save(input: &NormalizedServerInput) -> Result<()> {
    if !input.active {
        return Ok(());
    }

    let result = probe_server_connection(input.as_nntp_server_config()).await;
    if result.success {
        Ok(())
    } else {
        Err(async_graphql::Error::new(format!(
            "server connection test failed: {}",
            result.message
        )))
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
    let config = ctx.data::<weaver_core::config::SharedConfig>()?;

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

#[Object]
impl MutationRoot {
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

    /// Reprocess a failed job (re-run post-download stages without re-downloading).
    #[graphql(guard = "ControlGuard")]
    async fn reprocess_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.reprocess_job(JobId(id)).await?;
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
        let cat = category.map(|c| if c.is_empty() { None } else { Some(c) });
        let meta = priority.map(|p| vec![("priority".to_string(), p)]);
        for &id in &ids {
            handle
                .update_job(JobId(id), cat.clone(), meta.clone())
                .await?;
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

    /// Add a new NNTP server.
    #[graphql(guard = "AdminGuard")]
    async fn add_server(&self, ctx: &Context<'_>, input: ServerInput) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
        let normalized =
            NormalizedServerInput::from_input(input, None).map_err(async_graphql::Error::new)?;

        validate_server_before_save(&normalized).await?;

        let id = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.next_server_id())
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?
        };

        let server = normalized.as_runtime_server_config(id);

        {
            let db = db.clone();
            let server = server.clone();
            tokio::task::spawn_blocking(move || db.insert_server(&server))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
        }

        {
            let mut cfg = config.write().await;
            cfg.servers.push(server);
            info!(id, "server added");
        }

        // Rebuild pool — this also detects capabilities for all servers.
        rebuild_nntp_from_config(config, handle, db).await;

        let cfg = config.read().await;
        let server = cfg.servers.iter().next_back().unwrap();
        Ok(Server::from(server))
    }

    /// Update an existing NNTP server by ID.
    #[graphql(guard = "AdminGuard")]
    async fn update_server(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: ServerInput,
    ) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;

        let existing_password = {
            let cfg = config.read().await;
            cfg.servers
                .iter()
                .find(|s| s.id == id)
                .map(|s| s.password.clone())
                .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))?
        };
        let normalized = NormalizedServerInput::from_input(input, existing_password)
            .map_err(async_graphql::Error::new)?;

        validate_server_before_save(&normalized).await?;

        {
            let mut cfg = config.write().await;
            let s = cfg
                .servers
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))?;
            s.host = normalized.host.clone();
            s.port = normalized.port;
            s.tls = normalized.tls;
            s.username = normalized.username.clone();
            s.password = normalized.password.clone();
            s.connections = normalized.connections;
            s.active = normalized.active;
            s.priority = normalized.priority as u32;
            s.tls_ca_cert = normalized.tls_ca_cert.clone();

            let server = s.clone();
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.update_server(&server))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "server updated");
        }

        // Rebuild pool — this also detects capabilities for all servers.
        rebuild_nntp_from_config(config, handle, db).await;

        let cfg = config.read().await;
        let server = cfg.servers.iter().find(|s| s.id == id).unwrap();
        Ok(Server::from(server))
    }

    /// Remove an NNTP server by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_server(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;

        {
            let mut cfg = config.write().await;
            let before = cfg.servers.len();
            cfg.servers.retain(|s| s.id != id);
            if cfg.servers.len() == before {
                return Err(async_graphql::Error::new(format!("server {id} not found")));
            }

            let db = db.clone();
            tokio::task::spawn_blocking(move || db.delete_server(id))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "server removed");
        }

        rebuild_nntp_from_config(config, handle, db).await;
        let cfg = config.read().await;
        Ok(cfg.servers.iter().map(Server::from).collect())
    }

    /// Test connectivity to an NNTP server without saving it.
    #[graphql(guard = "AdminGuard")]
    async fn test_connection(
        &self,
        _ctx: &Context<'_>,
        input: ServerInput,
    ) -> Result<TestConnectionResult> {
        let normalized = match NormalizedServerInput::from_input(input, None) {
            Ok(normalized) => normalized,
            Err(message) => {
                return Ok(TestConnectionResult {
                    success: false,
                    message,
                    latency_ms: None,
                    supports_pipelining: false,
                });
            }
        };

        Ok(probe_server_connection(normalized.as_nntp_server_config()).await)
    }

    // ── Categories ────────────────────────────────────────────────────

    /// Add a new category.
    #[graphql(guard = "AdminGuard")]
    async fn add_category(&self, ctx: &Context<'_>, input: CategoryInput) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        let id = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.next_category_id())
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?
        };

        let cat = weaver_core::config::CategoryConfig {
            id,
            name: name.clone(),
            dest_dir: input.dest_dir.filter(|s| !s.is_empty()),
            aliases: input.aliases,
        };

        {
            let mut cfg = config.write().await;
            if cfg
                .categories
                .iter()
                .any(|c| c.name.eq_ignore_ascii_case(&name))
            {
                return Err(async_graphql::Error::new(format!(
                    "category '{name}' already exists"
                )));
            }

            let c = cat.clone();
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.insert_category(&c))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;

            cfg.categories.push(cat);
            info!(id, name = %name, "category added");
        }

        let cfg = config.read().await;
        let cat = cfg.categories.iter().find(|c| c.id == id).unwrap();
        Ok(Category::from(cat))
    }

    /// Update a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn update_category(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: CategoryInput,
    ) -> Result<Category> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;

        let name = input.name.trim().to_string();
        if name.is_empty() {
            return Err(async_graphql::Error::new("category name must not be empty"));
        }

        {
            let mut cfg = config.write().await;
            let c = cfg
                .categories
                .iter_mut()
                .find(|c| c.id == id)
                .ok_or_else(|| async_graphql::Error::new(format!("category {id} not found")))?;

            // Uniqueness check if name changed.
            if !c.name.eq_ignore_ascii_case(&name)
                && cfg
                    .categories
                    .iter()
                    .any(|other| other.id != id && other.name.eq_ignore_ascii_case(&name))
            {
                return Err(async_graphql::Error::new(format!(
                    "category '{name}' already exists"
                )));
            }

            // Re-borrow mutably after the uniqueness check.
            let c = cfg.categories.iter_mut().find(|c| c.id == id).unwrap();
            c.name = name;
            c.dest_dir = input.dest_dir.filter(|s| !s.is_empty());
            c.aliases = input.aliases;

            let cat = c.clone();
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.update_category(&cat))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "category updated");
        }

        let cfg = config.read().await;
        let cat = cfg.categories.iter().find(|c| c.id == id).unwrap();
        Ok(Category::from(cat))
    }

    /// Remove a category by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_category(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Category>> {
        let config = ctx.data::<SharedConfig>()?;
        let db = ctx.data::<Database>()?;

        {
            let mut cfg = config.write().await;
            let before = cfg.categories.len();
            cfg.categories.retain(|c| c.id != id);
            if cfg.categories.len() == before {
                return Err(async_graphql::Error::new(format!(
                    "category {id} not found"
                )));
            }

            let db = db.clone();
            tokio::task::spawn_blocking(move || db.delete_category(id))
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;
            info!(id, "category removed");
        }

        let cfg = config.read().await;
        Ok(cfg.categories.iter().map(Category::from).collect())
    }

    /// Update general settings.
    #[graphql(guard = "AdminGuard")]
    async fn update_settings(
        &self,
        ctx: &Context<'_>,
        input: GeneralSettingsInput,
    ) -> Result<GeneralSettings> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
        let normalized_intermediate_dir = normalize_settings_path_update(&input.intermediate_dir);
        let normalized_complete_dir = normalize_settings_path_update(&input.complete_dir);

        let settings = {
            let mut cfg = config.write().await;

            match &normalized_intermediate_dir {
                MaybeUndefined::Undefined => {}
                MaybeUndefined::Null => cfg.intermediate_dir = None,
                MaybeUndefined::Value(intermediate_dir) => {
                    cfg.intermediate_dir = Some(intermediate_dir.clone());
                }
            }
            match &normalized_complete_dir {
                MaybeUndefined::Undefined => {}
                MaybeUndefined::Null => cfg.complete_dir = None,
                MaybeUndefined::Value(complete_dir) => {
                    cfg.complete_dir = Some(complete_dir.clone());
                }
            }
            if let Some(cleanup) = input.cleanup_after_extract {
                cfg.cleanup_after_extract = Some(cleanup);
            }
            if let Some(speed) = input.max_download_speed {
                cfg.max_download_speed = Some(speed);
            }
            if let Some(retries) = input.max_retries {
                let retry = cfg
                    .retry
                    .get_or_insert(weaver_core::config::RetryOverrides {
                        max_retries: None,
                        base_delay_secs: None,
                        multiplier: None,
                    });
                retry.max_retries = Some(retries);
            }
            if let Some(cap) = input.isp_bandwidth_cap.clone() {
                cfg.isp_bandwidth_cap = Some(cap.into());
            }

            // Persist to SQLite.
            let db = db.clone();
            let input_clone = (
                normalized_intermediate_dir.clone(),
                normalized_complete_dir.clone(),
                input.cleanup_after_extract,
                input.max_download_speed,
                input.max_retries,
                input.isp_bandwidth_cap.clone(),
            );
            tokio::task::spawn_blocking(
                move || -> std::result::Result<(), weaver_state::StateError> {
                    match &input_clone.0 {
                        MaybeUndefined::Undefined => {}
                        MaybeUndefined::Null => db.delete_setting("intermediate_dir")?,
                        MaybeUndefined::Value(v) => db.set_setting("intermediate_dir", v)?,
                    }
                    match &input_clone.1 {
                        MaybeUndefined::Undefined => {}
                        MaybeUndefined::Null => db.delete_setting("complete_dir")?,
                        MaybeUndefined::Value(v) => db.set_setting("complete_dir", v)?,
                    }
                    if let Some(v) = input_clone.2 {
                        db.set_setting("cleanup_after_extract", &v.to_string())?;
                    }
                    if let Some(v) = input_clone.3 {
                        db.set_setting("max_download_speed", &v.to_string())?;
                    }
                    if let Some(v) = input_clone.4 {
                        db.set_setting("retry.max_retries", &v.to_string())?;
                    }
                    if let Some(ref cap) = input_clone.5 {
                        db.set_setting("bandwidth_cap.enabled", &cap.enabled.to_string())?;
                        db.set_setting(
                            "bandwidth_cap.period",
                            match cap.period {
                                crate::types::IspBandwidthCapPeriodGql::Daily => "daily",
                                crate::types::IspBandwidthCapPeriodGql::Weekly => "weekly",
                                crate::types::IspBandwidthCapPeriodGql::Monthly => "monthly",
                            },
                        )?;
                        db.set_setting("bandwidth_cap.limit_bytes", &cap.limit_bytes.to_string())?;
                        db.set_setting(
                            "bandwidth_cap.reset_time_minutes_local",
                            &cap.reset_time_minutes_local.to_string(),
                        )?;
                        db.set_setting(
                            "bandwidth_cap.weekly_reset_weekday",
                            match cap.weekly_reset_weekday {
                                crate::types::IspBandwidthCapWeekdayGql::Mon => "mon",
                                crate::types::IspBandwidthCapWeekdayGql::Tue => "tue",
                                crate::types::IspBandwidthCapWeekdayGql::Wed => "wed",
                                crate::types::IspBandwidthCapWeekdayGql::Thu => "thu",
                                crate::types::IspBandwidthCapWeekdayGql::Fri => "fri",
                                crate::types::IspBandwidthCapWeekdayGql::Sat => "sat",
                                crate::types::IspBandwidthCapWeekdayGql::Sun => "sun",
                            },
                        )?;
                        db.set_setting(
                            "bandwidth_cap.monthly_reset_day",
                            &cap.monthly_reset_day.to_string(),
                        )?;
                    }
                    Ok(())
                },
            )
            .await
            .map_err(|e| async_graphql::Error::new(format!("{e}")))?
            .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?;

            GeneralSettings {
                data_dir: cfg.data_dir.clone(),
                intermediate_dir: cfg.intermediate_dir(),
                complete_dir: cfg.complete_dir(),
                cleanup_after_extract: cfg.cleanup_after_extract(),
                max_download_speed: cfg.max_download_speed.unwrap_or(0),
                max_retries: cfg.retry.as_ref().and_then(|r| r.max_retries).unwrap_or(3),
                isp_bandwidth_cap: cfg.isp_bandwidth_cap.as_ref().map(Into::into),
            }
        };

        // Apply speed limit immediately.
        if let Some(speed) = input.max_download_speed {
            let _ = handle.set_speed_limit(speed).await;
        }
        if let Some(cap) = input.isp_bandwidth_cap {
            let _ = handle.set_bandwidth_cap_policy(Some(cap.into())).await;
        }

        // Apply directory changes immediately so new jobs use them without restart.
        if !normalized_intermediate_dir.is_undefined() || !normalized_complete_dir.is_undefined() {
            let cfg = config.read().await;
            let _ = handle
                .update_runtime_paths(
                    PathBuf::from(&cfg.data_dir),
                    PathBuf::from(cfg.intermediate_dir()),
                    PathBuf::from(cfg.complete_dir()),
                )
                .await;
        }

        Ok(settings)
    }

    /// Create a new API key. Returns the raw key (shown only once).
    #[graphql(guard = "AdminGuard")]
    async fn create_api_key(
        &self,
        ctx: &Context<'_>,
        name: String,
        scope: ApiKeyScope,
    ) -> Result<CreateApiKeyResult> {
        let db = ctx.data::<Database>()?;
        let raw_key = generate_api_key();
        let key_hash = hash_api_key(&raw_key);
        let scope_str = match scope {
            ApiKeyScope::Read => "read",
            ApiKeyScope::Control => "control",
            ApiKeyScope::Admin => "admin",
        };
        let db = db.clone();
        let name_clone = name.clone();
        let id = tokio::task::spawn_blocking(move || {
            db.insert_api_key(&name_clone, &key_hash, scope_str)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        info!(id, name = %name, scope = scope_str, "API key created");

        Ok(CreateApiKeyResult {
            key: ApiKey {
                id,
                name,
                scope,
                created_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64,
                last_used_at: None,
            },
            raw_key,
        })
    }

    /// Delete an API key by ID.
    #[graphql(guard = "AdminGuard")]
    async fn delete_api_key(&self, ctx: &Context<'_>, id: i64) -> Result<Vec<ApiKey>> {
        let db = ctx.data::<Database>()?;
        let db = db.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_api_key(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        if deleted {
            info!(id, "API key deleted");
        }
        let db = ctx.data::<Database>()?.clone();
        let rows = tokio::task::spawn_blocking(move || db.list_api_keys())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|row| ApiKey {
                id: row.id,
                name: row.name,
                scope: match row.scope.as_str() {
                    "read" => ApiKeyScope::Read,
                    "control" | "integration" => ApiKeyScope::Control,
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Control,
                },
                created_at: row.created_at as f64 * 1000.0,
                last_used_at: row.last_used_at.map(|value| value as f64 * 1000.0),
            })
            .collect())
    }

    /// Enable login protection with a username and password.
    #[graphql(guard = "AdminGuard")]
    async fn enable_login(
        &self,
        ctx: &Context<'_>,
        username: String,
        password: String,
    ) -> Result<bool> {
        if username.is_empty() || password.is_empty() {
            return Err(async_graphql::Error::new(
                "username and password must not be empty",
            ));
        }
        let hash = tokio::task::spawn_blocking(move || crate::auth::hash_password(&password))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(async_graphql::Error::new)?;
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        let username_for_db = username.clone();
        let hash_for_db = hash.clone();
        tokio::task::spawn_blocking(move || {
            db.set_auth_credentials(&username_for_db, &hash_for_db)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(username, hash)));
        info!("login protection enabled");
        Ok(true)
    }

    /// Disable login protection.
    #[graphql(guard = "AdminGuard")]
    async fn disable_login(&self, ctx: &Context<'_>) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        tokio::task::spawn_blocking(move || db.clear_auth_credentials())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.clear();
        info!("login protection disabled");
        Ok(true)
    }

    /// Change the login password. Requires the current password for verification.
    #[graphql(guard = "AdminGuard")]
    async fn change_password(
        &self,
        ctx: &Context<'_>,
        current_password: String,
        new_password: String,
    ) -> Result<bool> {
        if new_password.is_empty() {
            return Err(async_graphql::Error::new("new password must not be empty"));
        }
        let db = ctx.data::<Database>()?.clone();
        let auth_cache = ctx.data::<crate::auth::LoginAuthCache>()?.clone();
        let db2 = db.clone();
        let creds = auth_cache
            .snapshot()
            .ok_or_else(|| async_graphql::Error::new("login is not enabled"))?;

        let hash = creds.password_hash.clone();
        let current = current_password.clone();
        let valid =
            tokio::task::spawn_blocking(move || crate::auth::verify_password(&current, &hash))
                .await
                .unwrap_or(false);
        if !valid {
            return Err(async_graphql::Error::new("current password is incorrect"));
        }

        let new_hash =
            tokio::task::spawn_blocking(move || crate::auth::hash_password(&new_password))
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?
                .map_err(async_graphql::Error::new)?;
        let username = creds.username.clone();
        let new_hash_for_db = new_hash.clone();
        tokio::task::spawn_blocking(move || db2.set_auth_credentials(&username, &new_hash_for_db))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        auth_cache.replace(Some(crate::auth::CachedLoginAuth::new(
            creds.username,
            new_hash,
        )));
        info!("login password changed");
        Ok(true)
    }

    /// Add a new RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn add_rss_feed(&self, ctx: &Context<'_>, input: RssFeedInput) -> Result<RssFeed> {
        validate_feed_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        let feed = tokio::task::spawn_blocking(move || {
            let id = db.next_rss_feed_id()?;
            let row = rss_feed_row_from_create(id, input);
            db.insert_rss_feed(&row)?;
            let rules = db
                .list_rss_rules(row.id)?
                .iter()
                .map(RssRule::from_row)
                .collect();
            Ok::<_, weaver_state::StateError>(RssFeed::from_row(&row, rules))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(feed)
    }

    /// Update an RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn update_rss_feed(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: RssFeedInput,
    ) -> Result<RssFeed> {
        validate_feed_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        let feed = tokio::task::spawn_blocking(move || {
            let Some(existing) = db.get_rss_feed(id)? else {
                return Err(weaver_state::StateError::Database(format!(
                    "RSS feed {id} not found"
                )));
            };
            let row = rss_feed_row_from_update(existing, input);
            db.update_rss_feed(&row)?;
            let rules = db
                .list_rss_rules(row.id)?
                .iter()
                .map(RssRule::from_row)
                .collect();
            Ok::<_, weaver_state::StateError>(RssFeed::from_row(&row, rules))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        Ok(feed)
    }

    /// Delete an RSS feed.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_feed(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_rss_feed(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(deleted)
    }

    /// Add a new RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn add_rss_rule(
        &self,
        ctx: &Context<'_>,
        feed_id: u32,
        input: RssRuleInput,
    ) -> Result<RssRule> {
        validate_rule_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            if db.get_rss_feed(feed_id)?.is_none() {
                return Err(weaver_state::StateError::Database(format!(
                    "RSS feed {feed_id} not found"
                )));
            }
            let id = db.next_rss_rule_id()?;
            let row = rss_rule_row_from_input(id, feed_id, input);
            db.insert_rss_rule(&row)?;
            Ok::<_, weaver_state::StateError>(RssRule::from_row(&row))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Update an RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn update_rss_rule(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: RssRuleInput,
    ) -> Result<RssRule> {
        validate_rule_input(&input)?;

        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || {
            let Some(existing) = db.get_rss_rule(id)? else {
                return Err(weaver_state::StateError::Database(format!(
                    "RSS rule {id} not found"
                )));
            };
            let row = rss_rule_row_from_input(id, existing.feed_id, input);
            db.update_rss_rule(&row)?;
            Ok::<_, weaver_state::StateError>(RssRule::from_row(&row))
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Delete an RSS rule.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_rule(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_rss_rule(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(deleted)
    }

    /// Run RSS sync immediately for all enabled feeds or one specific feed.
    #[graphql(guard = "AdminGuard")]
    async fn run_rss_sync(&self, ctx: &Context<'_>, feed_id: Option<u32>) -> Result<RssSyncReport> {
        let rss = ctx.data::<RssService>()?;
        let report = rss
            .run_sync(feed_id)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(RssSyncReport::from_domain(&report))
    }

    /// Forget one seen RSS item so it can be reconsidered on a future sync.
    #[graphql(guard = "AdminGuard")]
    async fn delete_rss_seen_item(
        &self,
        ctx: &Context<'_>,
        feed_id: u32,
        item_id: String,
    ) -> Result<bool> {
        let db = ctx.data::<Database>()?.clone();
        tokio::task::spawn_blocking(move || db.delete_rss_seen_item(feed_id, &item_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Clear seen RSS items, either globally or for one feed.
    #[graphql(guard = "AdminGuard")]
    async fn clear_rss_seen_items(&self, ctx: &Context<'_>, feed_id: Option<u32>) -> Result<u32> {
        let db = ctx.data::<Database>()?.clone();
        let cleared = tokio::task::spawn_blocking(move || db.clear_rss_seen_items(feed_id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(cleared as u32)
    }

    // ── Schedules ───────────────────────────────────────────────────────

    async fn create_schedule(
        &self,
        ctx: &Context<'_>,
        input: crate::types::ScheduleInput,
    ) -> Result<Vec<crate::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_scheduler::schedule::SharedSchedules>()?
            .clone();
        let entry = input.into_entry();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.push(entry);
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::types::Schedule::from)
            .collect())
    }

    async fn update_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
        input: crate::types::ScheduleInput,
    ) -> Result<Vec<crate::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_scheduler::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            let updated = input.into_entry();
            existing.enabled = updated.enabled;
            existing.label = updated.label;
            existing.days = updated.days;
            existing.time = updated.time;
            existing.action = updated.action;
        }
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::types::Schedule::from)
            .collect())
    }

    async fn delete_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
    ) -> Result<Vec<crate::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_scheduler::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        entries.retain(|e| e.id != id);
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::types::Schedule::from)
            .collect())
    }

    async fn toggle_schedule(
        &self,
        ctx: &Context<'_>,
        id: String,
        enabled: bool,
    ) -> Result<Vec<crate::types::Schedule>> {
        let db = ctx.data::<Database>()?.clone();
        let schedules_state = ctx
            .data::<weaver_scheduler::schedule::SharedSchedules>()?
            .clone();
        let mut entries = tokio::task::spawn_blocking({
            let db = db.clone();
            move || db.list_schedules()
        })
        .await??;
        if let Some(existing) = entries.iter_mut().find(|e| e.id == id) {
            existing.enabled = enabled;
        }
        let entries_for_save = entries.clone();
        tokio::task::spawn_blocking(move || db.save_schedules(&entries_for_save)).await??;
        *schedules_state.write().await = entries.clone();
        Ok(entries
            .into_iter()
            .map(crate::types::Schedule::from)
            .collect())
    }
}

async fn history_items_from_db(db: Database) -> Result<Vec<HistoryItem>> {
    tokio::task::spawn_blocking(move || {
        let rows = db.list_job_history(&weaver_state::HistoryFilter::default())?;
        Ok::<_, weaver_state::StateError>(
            rows.into_iter()
                .map(|row| history_item_from_row(&row))
                .collect(),
        )
    })
    .await
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))?
    .map_err(|e| graphql_error("INTERNAL", e.to_string()))
}

fn validate_feed_input(input: &RssFeedInput) -> Result<()> {
    let url = reqwest::Url::parse(&input.url)
        .map_err(|e| async_graphql::Error::new(format!("invalid RSS feed URL: {e}")))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(async_graphql::Error::new(format!(
                "unsupported RSS feed URL scheme: {other}"
            )));
        }
    }
    if let Some(interval) = input.poll_interval_secs
        && interval == 0
    {
        return Err(async_graphql::Error::new(
            "poll_interval_secs must be greater than 0",
        ));
    }
    Ok(())
}

fn validate_rule_input(input: &RssRuleInput) -> Result<()> {
    if let Some(pattern) = &input.title_regex {
        Regex::new(pattern)
            .map_err(|e| async_graphql::Error::new(format!("invalid title_regex: {e}")))?;
    }
    if let (Some(min), Some(max)) = (input.min_size_bytes, input.max_size_bytes)
        && min > max
    {
        return Err(async_graphql::Error::new(
            "min_size_bytes cannot be greater than max_size_bytes",
        ));
    }
    Ok(())
}

fn rss_feed_row_from_create(id: u32, input: RssFeedInput) -> RssFeedRow {
    RssFeedRow {
        id,
        name: input.name,
        url: input.url,
        enabled: input.enabled,
        poll_interval_secs: input.poll_interval_secs.unwrap_or(900),
        username: normalize_optional_string(input.username),
        password: normalize_optional_string(input.password),
        default_category: normalize_optional_string(input.default_category),
        default_metadata: metadata_input_to_pairs(input.default_metadata),
        etag: None,
        last_modified: None,
        last_polled_at: None,
        last_success_at: None,
        last_error: None,
        consecutive_failures: 0,
    }
}

fn rss_feed_row_from_update(existing: RssFeedRow, input: RssFeedInput) -> RssFeedRow {
    RssFeedRow {
        id: existing.id,
        name: input.name,
        url: input.url,
        enabled: input.enabled,
        poll_interval_secs: input
            .poll_interval_secs
            .unwrap_or(existing.poll_interval_secs.max(1)),
        username: merge_optional_string(existing.username, input.username),
        password: merge_optional_string(existing.password, input.password),
        default_category: merge_optional_string(existing.default_category, input.default_category),
        default_metadata: input
            .default_metadata
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| (entry.key, entry.value))
                    .collect()
            })
            .unwrap_or(existing.default_metadata),
        etag: existing.etag,
        last_modified: existing.last_modified,
        last_polled_at: existing.last_polled_at,
        last_success_at: existing.last_success_at,
        last_error: existing.last_error,
        consecutive_failures: existing.consecutive_failures,
    }
}

fn rss_rule_row_from_input(id: u32, feed_id: u32, input: RssRuleInput) -> RssRuleRow {
    RssRuleRow {
        id,
        feed_id,
        sort_order: input.sort_order,
        enabled: input.enabled,
        action: match input.action {
            RssRuleActionGql::Accept => weaver_state::RssRuleAction::Accept,
            RssRuleActionGql::Reject => weaver_state::RssRuleAction::Reject,
        },
        title_regex: normalize_optional_string(input.title_regex),
        item_categories: input.item_categories.unwrap_or_default(),
        min_size_bytes: input.min_size_bytes,
        max_size_bytes: input.max_size_bytes,
        category_override: normalize_optional_string(input.category_override),
        metadata: metadata_input_to_pairs(input.metadata),
    }
}

fn metadata_input_to_pairs(
    entries: Option<Vec<crate::types::MetadataInput>>,
) -> Vec<(String, String)> {
    entries
        .unwrap_or_default()
        .into_iter()
        .map(|entry| (entry.key, entry.value))
        .collect()
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn merge_optional_string(existing: Option<String>, incoming: Option<String>) -> Option<String> {
    match incoming {
        Some(value) => normalize_optional_string(Some(value)),
        None => existing,
    }
}
