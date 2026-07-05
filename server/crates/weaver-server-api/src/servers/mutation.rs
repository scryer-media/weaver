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
                        Server::from(&*existing)
                    } else {
                        cfg.servers.push(server.clone());
                        Server::from(&server)
                    }
                },
            )
            .await?
        };

        info!(id, "server added");

        // Rebuild pool — this also detects capabilities for all servers.
        rebuild_nntp_from_config(config, handle).await;
        Ok(added)
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
        let _mutation_guard = SERVER_MUTATION_GUARD.lock().await;

        let existing_password = with_timed_config_read(
            config,
            "servers.mutation.update_server.existing_password",
            |cfg| {
                cfg.servers
                    .iter()
                    .find(|s| s.id == id)
                    .map(|s| s.password.clone())
                    .ok_or_else(|| async_graphql::Error::new(format!("server {id} not found")))
            },
        )
        .await?;
        let normalized = NormalizedServerInput::from_input(input, existing_password)
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
                    Server::from(&*current)
                } else {
                    cfg.servers.push(server.clone());
                    Server::from(&server)
                }
            })
            .await;

        info!(id, "server updated");

        // Rebuild pool — this also detects capabilities for all servers.
        rebuild_nntp_from_config(config, handle).await;
        Ok(updated)
    }
    /// Remove an NNTP server by ID.
    #[graphql(guard = "AdminGuard")]
    async fn remove_server(&self, ctx: &Context<'_>, id: u32) -> Result<Vec<Server>> {
        let config = ctx.data::<SharedConfig>()?;
        let handle = ctx.data::<SchedulerHandle>()?;
        let db = ctx.data::<Database>()?;
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
                cfg.servers.iter().map(Server::from).collect()
            })
            .await;

        info!(id, "server removed");

        rebuild_nntp_from_config(config, handle).await;
        Ok(remaining)
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
            config_path: None,
        }))
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
