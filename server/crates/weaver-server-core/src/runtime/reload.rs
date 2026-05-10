use crate::Database;
use crate::SchedulerHandle;
use crate::servers::probe_server_connection;
use crate::settings::{Config, SharedConfig};

pub async fn load_global_pause_from_db(db: &Database) -> Result<bool, String> {
    let db = db.clone();
    let value = tokio::task::spawn_blocking(move || db.get_setting("global_paused"))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;

    Ok(value
        .as_deref()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(false))
}

pub async fn refresh_server_capabilities_from_config(config: &SharedConfig, db: &Database) {
    let active_servers = {
        let cfg = config.read().await;
        cfg.servers
            .iter()
            .filter(|server| server.active)
            .cloned()
            .collect::<Vec<_>>()
    };

    if active_servers.is_empty() {
        return;
    }

    let mut capability_updates = Vec::with_capacity(active_servers.len());
    for mut server in active_servers {
        let result = probe_server_connection(&server).await;
        server.supports_pipelining = result.supports_pipelining;
        if result.success {
            tracing::info!(
                host = %server.host,
                pipelining = server.supports_pipelining,
                "detected server capabilities"
            );
        } else {
            tracing::info!(
                host = %server.host,
                error = %result.message,
                "capability detection failed, assuming no pipelining"
            );
        }

        capability_updates.push((server.id, server.supports_pipelining));

        let persisted = server;
        let db = db.clone();
        if let Err(join_error) =
            tokio::task::spawn_blocking(move || db.update_server(&persisted)).await
        {
            tracing::error!(error = %join_error, "failed to persist server capabilities");
        }
    }

    let mut cfg = config.write().await;
    for (server_id, supports_pipelining) in capability_updates {
        if let Some(server) = cfg.servers.iter_mut().find(|server| server.id == server_id) {
            server.supports_pipelining = supports_pipelining;
        }
    }
}

pub async fn rebuild_nntp_from_config(config: &SharedConfig, handle: &SchedulerHandle) {
    use weaver_nntp::client::{NntpClient, NntpClientConfig};
    use weaver_nntp::pool::ServerPoolConfig;

    let (client, total) = {
        let cfg = config.read().await;
        let mut active: Vec<&crate::servers::ServerConfig> =
            cfg.servers.iter().filter(|server| server.active).collect();
        active.sort_by_key(|server| (server.priority, server.id));
        let servers: Vec<ServerPoolConfig> = active
            .iter()
            .map(|server| ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    host: server.host.clone(),
                    port: server.port,
                    tls: server.tls,
                    username: server.username.clone(),
                    password: server.password.clone(),
                    tls_ca_cert: server.tls_ca_cert.clone(),
                    ..Default::default()
                },
                max_connections: server.connections as usize,
                group: server.priority,
            })
            .collect();

        let total: usize = servers.iter().map(|server| server.max_connections).sum();
        let client = NntpClient::new(NntpClientConfig {
            servers,
            max_idle_age: std::time::Duration::from_mins(5),
            max_retries_per_server: 1,
            soft_timeout: std::time::Duration::from_secs(15),
        });
        (client, total)
    };

    if let Err(error) = handle.rebuild_nntp(client, total).await {
        tracing::error!("failed to rebuild NNTP client: {error}");
    }
}

pub async fn reload_runtime_from_db(
    config: &SharedConfig,
    handle: &SchedulerHandle,
    db: &Database,
) -> Result<Config, String> {
    let loaded = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || db.load_config())
            .await
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?
    };

    if let Err(errors) = loaded.validate() {
        return Err(errors.join("; "));
    }

    {
        let mut cfg = config.write().await;
        *cfg = loaded.clone();
    }

    refresh_server_capabilities_from_config(config, db).await;
    rebuild_nntp_from_config(config, handle).await;
    handle
        .set_speed_limit(loaded.max_download_speed.unwrap_or(0))
        .await
        .map_err(|error| error.to_string())?;
    handle
        .set_bandwidth_cap_policy(loaded.isp_bandwidth_cap.clone())
        .await
        .map_err(|error| error.to_string())?;
    if load_global_pause_from_db(db).await? {
        handle
            .pause_all()
            .await
            .map_err(|error| error.to_string())?;
    } else {
        handle
            .resume_all()
            .await
            .map_err(|error| error.to_string())?;
    }

    Ok(loaded)
}
