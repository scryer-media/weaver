use weaver_core::config::{Config, SharedConfig};
use weaver_scheduler::SchedulerHandle;
use weaver_state::Database;

pub async fn load_global_pause_from_db(db: &Database) -> Result<bool, String> {
    let db = db.clone();
    let value = tokio::task::spawn_blocking(move || db.get_setting("global_paused"))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| e.to_string())?;

    Ok(value
        .as_deref()
        .and_then(|raw| raw.parse::<bool>().ok())
        .unwrap_or(false))
}

pub async fn rebuild_nntp_from_config(
    config: &SharedConfig,
    handle: &SchedulerHandle,
    db: &Database,
) {
    use weaver_nntp::client::{NntpClient, NntpClientConfig};
    use weaver_nntp::pool::ServerPoolConfig;

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

            let s = server.clone();
            let db = db.clone();
            let _ = tokio::task::spawn_blocking(move || db.update_server(&s)).await;
        }
    }

    let (client, total) = {
        let cfg = config.read().await;
        let mut active: Vec<&weaver_core::config::ServerConfig> =
            cfg.servers.iter().filter(|s| s.active).collect();
        active.sort_by_key(|s| (s.priority, s.id));
        let servers: Vec<ServerPoolConfig> = active
            .iter()
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
                group: s.priority,
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

pub async fn reload_runtime_from_db(
    config: &SharedConfig,
    handle: &SchedulerHandle,
    db: &Database,
) -> Result<Config, String> {
    let loaded = {
        let db = db.clone();
        tokio::task::spawn_blocking(move || db.load_config())
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?
    };

    if let Err(errors) = loaded.validate() {
        return Err(errors.join("; "));
    }

    {
        let mut cfg = config.write().await;
        *cfg = loaded.clone();
    }

    rebuild_nntp_from_config(config, handle, db).await;
    handle
        .set_speed_limit(loaded.max_download_speed.unwrap_or(0))
        .await
        .map_err(|e| e.to_string())?;
    if load_global_pause_from_db(db).await? {
        handle.pause_all().await.map_err(|e| e.to_string())?;
    } else {
        handle.resume_all().await.map_err(|e| e.to_string())?;
    }

    Ok(loaded)
}
