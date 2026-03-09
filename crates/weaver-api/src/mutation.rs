use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use async_graphql::{Context, Object, Result};
use base64::Engine;
use tracing::info;

use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_scheduler::{FileSpec, JobSpec, SchedulerHandle, SegmentSpec};
use weaver_state::Database;

use crate::auth::{AdminGuard, generate_api_key, hash_api_key};
use crate::types::{ApiKey, ApiKeyScope, CreateApiKeyResult, GeneralSettings, GeneralSettingsInput, Job, Server, ServerInput, TestConnectionResult};

/// Global counter for generating unique job IDs via the API.
static NEXT_API_JOB_ID: AtomicU64 = AtomicU64::new(10_000);

/// Seed the job ID counter so IDs are stable across restarts.
/// Call this on startup with the max job ID found in the journal + 1.
pub fn init_job_counter(start: u64) {
    NEXT_API_JOB_ID.store(start.max(10_000), Ordering::Relaxed);
}

pub struct MutationRoot;

#[Object]
impl MutationRoot {
    /// Submit an NZB file for download.
    ///
    /// The NZB content must be base64-encoded. Returns the created job.
    async fn submit_nzb(
        &self,
        ctx: &Context<'_>,
        nzb_base64: String,
        filename: Option<String>,
        password: Option<String>,
        category: Option<String>,
        metadata: Option<Vec<crate::types::MetadataInput>>,
    ) -> Result<Job> {
        let handle = ctx.data::<SchedulerHandle>()?;

        let nzb_bytes = base64::engine::general_purpose::STANDARD
            .decode(&nzb_base64)
            .map_err(|e| async_graphql::Error::new(format!("invalid base64: {e}")))?;

        let nzb = weaver_nzb::parse_nzb(&nzb_bytes)
            .map_err(|e| async_graphql::Error::new(format!("NZB parse error: {e}")))?;

        if nzb.files.is_empty() {
            return Err(async_graphql::Error::new("NZB contains no files"));
        }

        let job_id = JobId(NEXT_API_JOB_ID.fetch_add(1, Ordering::Relaxed));
        // Prefer the NZB filename over internal metadata — indexers use readable
        // names for the .nzb file while subjects/titles inside are often obfuscated.
        let name = filename
            .as_deref()
            .and_then(|f| f.strip_suffix(".nzb"))
            .filter(|n| !n.is_empty())
            .map(String::from)
            .or(nzb.meta.title.clone())
            .unwrap_or_else(|| "Untitled".to_string());

        // Use provided password, or extract from NZB meta.
        let pw = password.or_else(|| {
            nzb.meta.password.as_ref().filter(|p| !p.is_empty()).cloned()
        });

        let mut files = Vec::with_capacity(nzb.files.len());
        let mut total_bytes: u64 = 0;

        for nzb_file in &nzb.files {
            let fname = nzb_file.filename().unwrap_or("unknown").to_string();
            let role = nzb_file.role();

            let segments: Vec<SegmentSpec> = nzb_file
                .segments
                .iter()
                .map(|seg| SegmentSpec {
                    number: seg.number.saturating_sub(1),
                    bytes: seg.bytes,
                    message_id: seg.message_id.clone(),
                })
                .collect();

            let file_bytes: u64 = nzb_file.total_bytes();
            total_bytes += file_bytes;

            files.push(FileSpec {
                filename: fname,
                role,
                groups: nzb_file.groups.clone(),
                segments,
            });
        }

        let meta_vec: Vec<(String, String)> = metadata
            .as_ref()
            .map(|m| m.iter().map(|mi| (mi.key.clone(), mi.value.clone())).collect())
            .unwrap_or_default();

        let spec = JobSpec {
            name: name.clone(),
            password: pw.clone(),
            files,
            total_bytes,
            category: category.clone(),
            metadata: meta_vec.clone(),
        };

        // Persist the NZB to disk so recovery can re-parse it after restart.
        let config = ctx.data::<SharedConfig>()?;
        let data_dir = {
            let cfg = config.read().await;
            PathBuf::from(&cfg.data_dir)
        };
        let nzb_dir = data_dir.join(".weaver-nzbs");
        tokio::fs::create_dir_all(&nzb_dir).await.map_err(|e| {
            async_graphql::Error::new(format!("failed to create NZB storage dir: {e}"))
        })?;
        let nzb_path = nzb_dir.join(format!("{}.nzb", job_id.0));
        let compressed = zstd::bulk::compress(&nzb_bytes, 19).map_err(|e| {
            async_graphql::Error::new(format!("failed to compress NZB: {e}"))
        })?;
        tokio::fs::write(&nzb_path, &compressed).await.map_err(|e| {
            async_graphql::Error::new(format!("failed to save NZB: {e}"))
        })?;

        handle.add_job(job_id, spec, nzb_path).await?;

        Ok(Job {
            id: job_id.0,
            name,
            status: crate::types::JobStatusGql::Queued,
            error: None,
            progress: 0.0,
            total_bytes,
            downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            has_password: pw.is_some(),
            category,
            metadata: meta_vec
                .into_iter()
                .map(|(k, v)| crate::types::MetadataEntry { key: k, value: v })
                .collect(),
            output_dir: None,
            created_at: Some(weaver_scheduler::job::epoch_ms_now()),
        })
    }

    /// Pause a running job.
    async fn pause_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.pause_job(JobId(id)).await?;
        Ok(true)
    }

    /// Resume a paused job.
    async fn resume_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.resume_job(JobId(id)).await?;
        Ok(true)
    }

    /// Cancel and remove a job.
    async fn cancel_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.cancel_job(JobId(id)).await?;
        Ok(true)
    }

    /// Reprocess a failed job (re-run post-download stages without re-downloading).
    async fn reprocess_job(&self, ctx: &Context<'_>, id: u64) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.reprocess_job(JobId(id)).await?;
        Ok(true)
    }

    /// Delete a completed/failed/cancelled job from history.
    /// Returns the remaining history jobs after deletion.
    async fn delete_history(&self, ctx: &Context<'_>, id: u64) -> Result<Vec<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.delete_history(JobId(id)).await?;

        let jobs = handle
            .list_jobs()
            .iter()
            .filter(|info| {
                matches!(
                    &info.status,
                    weaver_scheduler::JobStatus::Complete
                        | weaver_scheduler::JobStatus::Failed { .. }
                )
            })
            .map(Job::from)
            .collect();
        Ok(jobs)
    }

    /// Delete all completed/failed/cancelled jobs from history.
    async fn delete_all_history(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.delete_all_history().await?;
        Ok(true)
    }

    /// Pause all download dispatch globally.
    async fn pause_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.pause_all().await?;
        Ok(true)
    }

    /// Resume all download dispatch globally.
    async fn resume_all(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.resume_all().await?;
        Ok(true)
    }

    /// Set the global download speed limit in bytes/sec. 0 means unlimited.
    async fn set_speed_limit(
        &self,
        ctx: &Context<'_>,
        bytes_per_sec: u64,
    ) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.set_speed_limit(bytes_per_sec).await?;
        Ok(true)
    }

    /// Add a new NNTP server.
    #[graphql(guard = "AdminGuard")]
    async fn add_server(&self, ctx: &Context<'_>, input: ServerInput) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;

        let id = {
            let db = db.clone();
            tokio::task::spawn_blocking(move || db.next_server_id())
                .await
                .map_err(|e| async_graphql::Error::new(format!("{e}")))?
                .map_err(|e| async_graphql::Error::new(format!("db error: {e}")))?
        };

        let server = weaver_core::config::ServerConfig {
            id,
            host: input.host,
            port: input.port,
            tls: input.tls,
            username: input.username,
            password: input.password,
            connections: input.connections,
            active: input.active,
            supports_pipelining: false,
        };

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
    async fn update_server(&self, ctx: &Context<'_>, id: u32, input: ServerInput) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;

        {
            let mut cfg = config.write().await;
            let s = cfg
                .servers
                .iter_mut()
                .find(|s| s.id == id)
                .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))?;
            s.host = input.host;
            s.port = input.port;
            s.tls = input.tls;
            s.username = input.username;
            s.password = input.password;
            s.connections = input.connections;
            s.active = input.active;

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
    async fn remove_server(&self, ctx: &Context<'_>, id: u32) -> Result<bool> {
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
        Ok(true)
    }

    /// Test connectivity to an NNTP server without saving it.
    #[graphql(guard = "AdminGuard")]
    async fn test_connection(&self, _ctx: &Context<'_>, input: ServerInput) -> Result<TestConnectionResult> {
        let nntp_config = weaver_nntp::ServerConfig {
            host: input.host,
            port: input.port,
            tls: input.tls,
            username: input.username,
            password: input.password,
            ..Default::default()
        };

        let start = std::time::Instant::now();
        match weaver_nntp::NntpConnection::connect(&nntp_config).await {
            Ok(mut conn) => {
                let latency = start.elapsed().as_millis() as u64;
                let pipelining = conn.capabilities().supports_pipelining();
                let _ = conn.quit().await;
                Ok(TestConnectionResult {
                    success: true,
                    message: "Connected successfully".to_string(),
                    latency_ms: Some(latency),
                    supports_pipelining: pipelining,
                })
            }
            Err(e) => Ok(TestConnectionResult {
                success: false,
                message: format!("{e}"),
                latency_ms: None,
                supports_pipelining: false,
            }),
        }
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

        let settings = {
            let mut cfg = config.write().await;

            if let Some(ref intermediate_dir) = input.intermediate_dir {
                cfg.intermediate_dir = Some(intermediate_dir.clone());
            }
            if let Some(ref complete_dir) = input.complete_dir {
                cfg.complete_dir = Some(complete_dir.clone());
            }
            if let Some(cleanup) = input.cleanup_after_extract {
                cfg.cleanup_after_extract = Some(cleanup);
            }
            if let Some(speed) = input.max_download_speed {
                cfg.max_download_speed = Some(speed);
            }
            if let Some(retries) = input.max_retries {
                let retry = cfg.retry.get_or_insert(weaver_core::config::RetryOverrides {
                    max_retries: None,
                    base_delay_secs: None,
                    multiplier: None,
                });
                retry.max_retries = Some(retries);
            }

            // Persist to SQLite.
            let db = db.clone();
            let input_clone = (
                input.intermediate_dir.clone(),
                input.complete_dir.clone(),
                input.cleanup_after_extract,
                input.max_download_speed,
                input.max_retries,
            );
            tokio::task::spawn_blocking(move || -> std::result::Result<(), weaver_state::StateError> {
                if let Some(ref v) = input_clone.0 {
                    db.set_setting("intermediate_dir", v)?;
                }
                if let Some(ref v) = input_clone.1 {
                    db.set_setting("complete_dir", v)?;
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
                Ok(())
            })
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
            }
        };

        // Apply speed limit immediately.
        if let Some(speed) = input.max_download_speed {
            let _ = handle.set_speed_limit(speed).await;
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
            ApiKeyScope::Integration => "integration",
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
    async fn delete_api_key(&self, ctx: &Context<'_>, id: i64) -> Result<bool> {
        let db = ctx.data::<Database>()?;
        let db = db.clone();
        let deleted = tokio::task::spawn_blocking(move || db.delete_api_key(id))
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;

        if deleted {
            info!(id, "API key deleted");
        }
        Ok(deleted)
    }
}

/// Rebuild the NNTP client from the current config and send it to the pipeline.
///
/// Also probes each active server for capability detection (pipelining, etc.)
/// and updates the stored config.
async fn rebuild_nntp_from_config(config: &SharedConfig, handle: &SchedulerHandle, db: &Database) {
    use weaver_nntp::client::{NntpClient, NntpClientConfig};
    use weaver_nntp::pool::ServerPoolConfig;

    // Detect capabilities for all active servers.
    {
        let mut cfg = config.write().await;
        for server in cfg.servers.iter_mut().filter(|s| s.active) {
            let nntp_config = weaver_nntp::ServerConfig {
                host: server.host.clone(),
                port: server.port,
                tls: server.tls,
                username: server.username.clone(),
                password: server.password.clone(),
                ..Default::default()
            };
            match weaver_nntp::NntpConnection::connect(&nntp_config).await {
                Ok(mut conn) => {
                    server.supports_pipelining = conn.capabilities().supports_pipelining();
                    tracing::info!(
                        host = %server.host,
                        pipelining = server.supports_pipelining,
                        "detected server capabilities"
                    );
                    let _ = conn.quit().await;
                }
                Err(e) => {
                    tracing::info!(
                        host = %server.host,
                        error = %e,
                        "capability detection failed, assuming no pipelining"
                    );
                    server.supports_pipelining = false;
                }
            }

            // Persist capabilities to DB.
            let s = server.clone();
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || db.update_server(&s)).await;
        }
    }

    let (client, total) = {
        let cfg = config.read().await;
        let servers: Vec<ServerPoolConfig> = cfg
            .servers
            .iter()
            .filter(|s| s.active)
            .map(|s| ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: s.host.clone(),
                    port: s.port,
                    tls: s.tls,
                    username: s.username.clone(),
                    password: s.password.clone(),
                    ..Default::default()
                },
                max_connections: s.connections as usize,
            })
            .collect();

        let total: usize = servers.iter().map(|s| s.max_connections).sum();
        let client = NntpClient::new(NntpClientConfig {
            servers,
            max_idle_age: std::time::Duration::from_secs(300),
            max_retries_per_server: 1,
        });
        (client, total)
    };

    if let Err(e) = handle.rebuild_nntp(client, total).await {
        tracing::error!("failed to rebuild NNTP client: {e}");
    }
}
