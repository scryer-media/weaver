use std::path::PathBuf;

use async_graphql::{Context, Object, Result};
use tracing::info;

use crate::auth::AdminGuard;
use crate::servers::types::{Server, ServerInput, TestConnectionResult};
use crate::system::runtime::rebuild_nntp_from_config;
use weaver_server_core::settings::SharedConfig;
use weaver_server_core::{Database, SchedulerHandle};

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

async fn validate_server_before_save(input: &NormalizedServerInput) -> Result<()> {
    if !input.active {
        return Ok(());
    }

    let result =
        weaver_server_core::servers::probe_server_connection(&input.as_runtime_server_config(0))
            .await;
    if result.success {
        Ok(())
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
