use std::path::PathBuf;
use std::sync::LazyLock;

use async_graphql::{Context, Object, Result};
use tracing::info;

use crate::auth::AdminGuard;
use crate::observability::{
    persist_then_update_config, spawn_blocking_db, with_timed_config_read, with_timed_config_write,
};
use crate::servers::types::{Server, ServerInput, TestConnectionResult};
use crate::system::runtime::rebuild_nntp_from_config;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{Database, SchedulerHandle};

static SERVER_MUTATION_GUARD: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

#[derive(Default)]
pub(crate) struct ServersMutation;

#[Object]
impl ServersMutation {
    /// Add a new NNTP server.
    #[graphql(guard = "AdminGuard")]
    async fn add_server(&self, ctx: &Context<'_>, input: ServerInput) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
        let policy = ctx.data::<std::sync::Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >>()?;
        let _mutation_guard = SERVER_MUTATION_GUARD.lock().await;
        let normalized =
            NormalizedServerInput::from_input(input, None).map_err(async_graphql::Error::new)?;

        let validated_capabilities = validate_server_before_save(&normalized).await?;

        let id = {
            let db = db.clone();
            spawn_blocking_db("servers.mutation.add_server.next_id", move || {
                db.next_server_id()
            })
            .await?
        };

        let mut server = normalized.as_runtime_server_config(id);
        server.supports_pipelining = validated_capabilities.unwrap_or(false);

        let added = {
            let persisted_server = server.clone();
            let db = db.clone();
            let persist = async move {
                spawn_blocking_db("servers.mutation.add_server.persist", move || {
                    db.insert_server(&persisted_server)
                })
                .await
            };
            persist_then_update_config(
                config,
                "servers.mutation.add_server.apply",
                persist,
                move |cfg| {
                    if let Some(existing) = cfg
                        .servers
                        .iter_mut()
                        .find(|candidate| candidate.id == server.id)
                    {
                        *existing = server.clone();
                        existing.clone()
                    } else {
                        cfg.servers.push(server.clone());
                        server.clone()
                    }
                },
            )
            .await?
        };

        info!(id, "server added");

        activate_nntp_runtime("add_server", id, config, handle).await?;
        let snapshot = policy.snapshot(added.id);
        Ok(Server::from_config(&added, snapshot.as_ref()))
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
        let policy = ctx.data::<std::sync::Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >>()?;
        let _mutation_guard = SERVER_MUTATION_GUARD.lock().await;

        let existing =
            with_timed_config_read(config, "servers.mutation.update_server.existing", |cfg| {
                cfg.servers
                    .iter()
                    .find(|s| s.id == id)
                    .cloned()
                    .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))
            })
            .await?;
        let normalized = NormalizedServerInput::from_input(input, Some(&existing))
            .map_err(async_graphql::Error::new)?;

        let validated_capabilities = validate_server_before_save(&normalized).await?;
        let mut server = normalized.as_runtime_server_config(id);
        server.supports_pipelining = validated_capabilities.unwrap_or(false);

        {
            let db = db.clone();
            let persisted_server = server.clone();
            spawn_blocking_db("servers.mutation.update_server.persist", move || {
                db.update_server(&persisted_server)
            })
            .await?;
        }

        let updated =
            with_timed_config_write(config, "servers.mutation.update_server.apply", move |cfg| {
                if let Some(current) = cfg.servers.iter_mut().find(|candidate| candidate.id == id) {
                    *current = server.clone();
                    current.clone()
                } else {
                    cfg.servers.push(server.clone());
                    server.clone()
                }
            })
            .await;

        info!(id, "server updated");

        activate_nntp_runtime("update_server", id, config, handle).await?;
        let snapshot = policy.snapshot(updated.id);
        Ok(Server::from_config(&updated, snapshot.as_ref()))
    }
    /// Remove an NNTP server by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_server(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
        let policy = ctx.data::<std::sync::Arc<
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
        >>()?;
        let _mutation_guard = SERVER_MUTATION_GUARD.lock().await;

        with_timed_config_read(config, "servers.mutation.remove_server.validate", |cfg| {
            if cfg.servers.iter().any(|server| server.id == id) {
                Ok(())
            } else {
                Err(async_graphql::Error::new(format!("server {id} not found")))
            }
        })
        .await?;

        {
            let policy = std::sync::Arc::clone(policy);
            spawn_blocking_db("servers.mutation.remove_server.flush_usage", move || {
                policy.flush_usage()
            })
            .await?;
        }

        {
            let db = db.clone();
            let deleted = spawn_blocking_db("servers.mutation.remove_server.persist", move || {
                db.delete_server(id)
            })
            .await?;
            if !deleted {
                return Err(async_graphql::Error::new(format!("server {id} not found")));
            }
        }

        let remaining =
            with_timed_config_write(config, "servers.mutation.remove_server.apply", move |cfg| {
                cfg.servers.retain(|server| server.id != id);
                cfg.servers.clone()
            })
            .await;

        info!(id, "server removed");

        activate_nntp_runtime("remove_server", id, config, handle).await?;
        let snapshots = policy
            .snapshots()
            .into_iter()
            .map(|snapshot| (snapshot.server_id, snapshot))
            .collect::<std::collections::HashMap<_, _>>();
        Ok(remaining
            .iter()
            .map(|server| Server::from_config(server, snapshots.get(&server.id)))
            .collect())
    }
    /// Reset this server's quota baseline without clearing lifetime usage.
    #[graphql(guard = "AdminGuard")]
    async fn reset_server_download_quota_usage(
        &self,
        ctx: &Context<'_>,
        id: u32,
    ) -> Result<Server> {
        let config = ctx.data::<SharedConfig>()?;
        let policy = ctx
            .data::<std::sync::Arc<
                weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
            >>()?
            .clone();
        let _mutation_guard = SERVER_MUTATION_GUARD.lock().await;
        let server = with_timed_config_read(
            config,
            "servers.mutation.reset_server_download_quota_usage.server",
            |cfg| {
                cfg.servers
                    .iter()
                    .find(|server| server.id == id)
                    .cloned()
                    .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))
            },
        )
        .await?;
        let snapshot = spawn_blocking_db(
            "servers.mutation.reset_server_download_quota_usage.persist",
            move || policy.reset_usage(id),
        )
        .await?;
        Ok(Server::from_config(&server, Some(&snapshot)))
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

        Ok(weaver_server_core::servers::probe_server_connection(
            &normalized.as_runtime_server_config(0),
        )
        .await
        .into())
    }

    // ── Categories ────────────────────────────────────────────────────
}

async fn activate_nntp_runtime(
    operation: &'static str,
    server_id: u32,
    config: &SharedConfig,
    handle: &SchedulerHandle,
) -> Result<()> {
    let activation = rebuild_nntp_from_config(config, handle).await.map_err(|error| {
        async_graphql::Error::new(format!(
            "server setting was saved, but the runtime remains on the prior NNTP generation; retry the change or restart Weaver ({error})"
        ))
    })?;
    info!(
        operation,
        server_id,
        runtime_generation = activation.generation,
        configured_connections = activation.configured_connections,
        effective_connections = activation.effective_connections,
        "server change activated in NNTP runtime"
    );
    Ok(())
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
    backfill: bool,
    retention_days: u32,
    max_download_speed: u64,
    download_quota: weaver_server_core::servers::ServerDownloadQuotaConfig,
    tls_ca_cert: Option<PathBuf>,
}

impl NormalizedServerInput {
    fn from_input(
        input: ServerInput,
        fallback: Option<&weaver_server_core::servers::ServerConfig>,
    ) -> std::result::Result<Self, String> {
        let host = input.host.trim().to_string();
        if host.is_empty() {
            return Err("server host must not be empty".to_string());
        }

        let password = normalize_optional_string(input.password)
            .or_else(|| fallback.and_then(|server| server.password.clone()));
        let max_download_speed = input
            .max_download_speed
            .or_else(|| fallback.map(|server| server.max_download_speed))
            .unwrap_or(0);
        if max_download_speed > weaver_server_core::servers::MAX_PERSISTED_SERVER_DOWNLOAD_BYTES {
            return Err("server max download speed exceeds database range".to_string());
        }
        let download_quota = input
            .download_quota
            .map(weaver_server_core::servers::ServerDownloadQuotaConfig::try_from)
            .transpose()?
            .or_else(|| fallback.map(|server| server.download_quota.clone()))
            .unwrap_or_default();

        Ok(Self {
            host,
            port: input.port,
            tls: input.tls,
            username: normalize_optional_string(input.username),
            password,
            connections: input.connections,
            active: input.active,
            priority: input.priority,
            backfill: input.backfill,
            retention_days: input.retention_days,
            max_download_speed,
            download_quota,
            tls_ca_cert: normalize_optional_string(input.tls_ca_cert).map(PathBuf::from),
        })
    }

    fn as_runtime_server_config(&self, id: u32) -> weaver_server_core::servers::ServerConfig {
        weaver_server_core::servers::ServerConfig {
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
            backfill: self.backfill,
            retention_days: self.retention_days,
            max_download_speed: self.max_download_speed,
            download_quota: self.download_quota.clone(),
            tls_ca_cert: self.tls_ca_cert.clone(),
        }
    }
}

async fn validate_server_before_save(input: &NormalizedServerInput) -> Result<Option<bool>> {
    if !input.active {
        return Ok(None);
    }

    let result =
        weaver_server_core::servers::probe_server_connection(&input.as_runtime_server_config(0))
            .await;
    if result.success {
        Ok(Some(result.supports_pipelining))
    } else {
        Err(async_graphql::Error::new(format!(
            "server connection test failed: {}",
            result.message
        )))
    }
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{RwLock, oneshot};
    use tokio::time::timeout;

    use crate::observability::{persist_then_update_config, with_timed_config_read};

    use super::*;

    fn test_config() -> SharedConfig {
        Arc::new(RwLock::new(weaver_server_core::settings::Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: weaver_server_core::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: weaver_server_core::jobs::DuplicatePolicy::default(),
            config_path: None,
        }))
    }

    fn inactive_server_input() -> ServerInput {
        ServerInput {
            host: "news.example.com".to_string(),
            port: 119,
            tls: false,
            username: None,
            password: None,
            connections: 4,
            active: false,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: None,
            download_quota: None,
            tls_ca_cert: None,
        }
    }

    #[test]
    fn normalization_rejects_download_limits_outside_database_range() {
        let mut input = inactive_server_input();
        input.max_download_speed =
            Some(weaver_server_core::servers::MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1);
        let error = NormalizedServerInput::from_input(input, None)
            .err()
            .expect("out-of-range speed should be rejected");
        assert!(error.contains("max download speed exceeds database range"));

        let mut input = inactive_server_input();
        input.download_quota = Some(crate::servers::types::ServerDownloadQuotaInput {
            enabled: false,
            limit_bytes: weaver_server_core::servers::MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1,
            period: crate::servers::types::ServerDownloadQuotaPeriodGql::OneTime,
            reset_time_minutes_local: 0,
            weekly_reset_weekday: crate::servers::types::ServerDownloadQuotaWeekdayGql::Mon,
            monthly_reset_day: 1,
        });
        let error = NormalizedServerInput::from_input(input, None)
            .err()
            .expect("out-of-range quota should be rejected");
        assert!(error.contains("download quota limit exceeds database range"));
    }

    #[tokio::test]
    async fn slow_persist_does_not_block_server_reads() {
        let config = test_config();
        let (release_tx, release_rx) = oneshot::channel();

        let update_task = tokio::spawn({
            let config = config.clone();
            async move {
                persist_then_update_config(
                    &config,
                    "tests.servers.persist_then_update",
                    async move {
                        release_rx.await.expect("release signal should arrive");
                        Ok(())
                    },
                    |cfg| {
                        cfg.servers.push(weaver_server_core::servers::ServerConfig {
                            id: 1,
                            host: "news.example.com".to_string(),
                            port: 563,
                            tls: true,
                            username: None,
                            password: None,
                            connections: 20,
                            active: true,
                            supports_pipelining: true,
                            priority: 0,
                            backfill: false,
                            retention_days: 0,
                            max_download_speed: 0,
                            download_quota:
                                weaver_server_core::servers::ServerDownloadQuotaConfig::default(),
                            tls_ca_cert: None,
                        });
                    },
                )
                .await
                .expect("server update should succeed");
            }
        });

        tokio::task::yield_now().await;

        let server_count = timeout(
            Duration::from_millis(50),
            with_timed_config_read(&config, "tests.servers.read", |cfg| cfg.servers.len()),
        )
        .await
        .expect("server read should not block on slow persist");
        assert_eq!(server_count, 0);

        release_tx
            .send(())
            .expect("update task should still be waiting");
        update_task
            .await
            .expect("update task should finish cleanly");
    }
}
