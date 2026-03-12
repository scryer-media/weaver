use async_graphql::{Context, Object, Result};
use base64::Engine;
use regex::Regex;
use tracing::info;

use weaver_core::config::SharedConfig;
use weaver_core::id::JobId;
use weaver_core::release_name::{derive_release_name, original_release_title, parse_job_release};
use weaver_scheduler::SchedulerHandle;
use weaver_state::{Database, RssFeedRow, RssRuleRow};

use crate::auth::{AdminGuard, generate_api_key, hash_api_key};
use crate::rss::RssService;
use crate::runtime::rebuild_nntp_from_config;
use crate::submit::submit_nzb_bytes;
use crate::types::{
    ApiKey, ApiKeyScope, Category, CategoryInput, CreateApiKeyResult, GeneralSettings,
    GeneralSettingsInput, Job, RssFeed, RssFeedInput, RssRule, RssRuleActionGql, RssRuleInput,
    RssSyncReport, Server, ServerInput, TestConnectionResult,
};

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
        let config = ctx.data::<weaver_core::config::SharedConfig>()?;

        let nzb_bytes = base64::engine::general_purpose::STANDARD
            .decode(&nzb_base64)
            .map_err(|e| async_graphql::Error::new(format!("invalid base64: {e}")))?;

        let meta_vec: Vec<(String, String)> = metadata
            .as_ref()
            .map(|m| {
                m.iter()
                    .map(|mi| (mi.key.clone(), mi.value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let submitted = submit_nzb_bytes(
            handle,
            config,
            &nzb_bytes,
            filename,
            password,
            category.clone(),
            meta_vec.clone(),
        )
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        if let Ok(info) = handle.get_job(submitted.job_id) {
            return Ok(Job::from(&info));
        }
        tokio::task::yield_now().await;
        if let Ok(info) = handle.get_job(submitted.job_id) {
            return Ok(Job::from(&info));
        }

        let original_title = original_release_title(&submitted.spec.name, &submitted.spec.metadata);
        let display_title = derive_release_name(Some(&original_title), Some(&submitted.spec.name));
        let parsed_release = crate::types::ParsedRelease::from(parse_job_release(
            &submitted.spec.name,
            &submitted.spec.metadata,
        ));

        Ok(Job {
            id: submitted.job_id.0,
            name: submitted.spec.name.clone(),
            display_title,
            original_title,
            parsed_release,
            status: crate::types::JobStatusGql::Queued,
            error: None,
            progress: 0.0,
            total_bytes: submitted.spec.total_bytes,
            downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            has_password: submitted.spec.password.is_some(),
            category,
            metadata: submitted
                .spec
                .metadata
                .iter()
                .map(|(k, v)| crate::types::MetadataEntry {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            output_dir: None,
            created_at: Some(submitted.created_at_epoch_ms),
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
    async fn delete_all_history(&self, ctx: &Context<'_>) -> Result<Vec<Job>> {
        let handle = ctx.data::<SchedulerHandle>()?;
        handle.delete_all_history().await?;
        Ok(history_jobs_from_handle(handle))
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
    async fn set_speed_limit(&self, ctx: &Context<'_>, bytes_per_sec: u64) -> Result<bool> {
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
            priority: input.priority as u32,
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
    async fn update_server(
        &self,
        ctx: &Context<'_>,
        id: u32,
        input: ServerInput,
    ) -> Result<Server> {
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
            // Preserve existing password when the client sends null (edit form
            // leaves it blank unless the user explicitly re-enters it).
            if input.password.is_some() {
                s.password = input.password;
            }
            s.connections = input.connections;
            s.active = input.active;
            s.priority = input.priority as u32;

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
                let retry = cfg
                    .retry
                    .get_or_insert(weaver_core::config::RetryOverrides {
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
            tokio::task::spawn_blocking(
                move || -> std::result::Result<(), weaver_state::StateError> {
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
                    "admin" => ApiKeyScope::Admin,
                    _ => ApiKeyScope::Integration,
                },
                created_at: row.created_at as f64,
                last_used_at: row.last_used_at.map(|value| value as f64),
            })
            .collect())
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
}

fn history_jobs_from_handle(handle: &SchedulerHandle) -> Vec<Job> {
    handle
        .list_jobs()
        .iter()
        .filter(|info| {
            matches!(
                &info.status,
                weaver_scheduler::JobStatus::Complete | weaver_scheduler::JobStatus::Failed { .. }
            )
        })
        .map(Job::from)
        .collect()
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
