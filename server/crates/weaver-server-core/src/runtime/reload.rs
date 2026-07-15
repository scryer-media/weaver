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
        let server_id = persisted.id;
        let db = db.clone();
        match tokio::task::spawn_blocking(move || db.update_server(&persisted)).await {
            Ok(Err(error)) => {
                tracing::error!(server_id, error = %error, "failed to persist server capabilities");
            }
            Err(join_error) => {
                tracing::error!(server_id, error = %join_error, "failed to persist server capabilities");
            }
            Ok(Ok(())) => {}
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
    use weaver_nntp::transfer::{ServerTransferRegistry, StableServerId};

    let policy_registry = handle.server_transfer_policy();
    let transfer_registry = policy_registry
        .as_ref()
        .map(|registry| registry.transfer_registry())
        .unwrap_or_else(|| std::sync::Arc::new(ServerTransferRegistry::new()));

    let configured_servers = config.read().await.servers.clone();
    let policies_ready = match policy_registry.as_ref() {
        Some(registry) => {
            let registry = std::sync::Arc::clone(registry);
            let servers = configured_servers.clone();
            match tokio::task::spawn_blocking(move || registry.reconfigure(&servers)).await {
                Ok(Ok(())) => true,
                Ok(Err(error)) => {
                    tracing::error!(
                        error = %error,
                        "failed to reconfigure server transfer policies; disabling NNTP servers for this runtime generation"
                    );
                    false
                }
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "server transfer policy reconfiguration task failed; disabling NNTP servers for this runtime generation"
                    );
                    false
                }
            }
        }
        None => {
            tracing::error!(
                "server transfer policy registry unavailable; disabling NNTP servers for this runtime generation"
            );
            false
        }
    };

    let (client, total) = {
        let mut active: Vec<&crate::servers::ServerConfig> = if policies_ready {
            configured_servers
                .iter()
                .filter(|server| server.active)
                .collect()
        } else {
            Vec::new()
        };
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
                backfill: server.backfill,
                retention_days: server.retention_days,
                stable_id: StableServerId(server.id),
                transfer_control: Some(transfer_registry.control(StableServerId(server.id))),
            })
            .collect();

        let total: usize = servers.iter().map(|server| server.max_connections).sum();
        tracing::info!(
            active_server_count = servers.len(),
            total_connections = total,
            policies_ready,
            "building NNTP runtime generation"
        );
        let client = NntpClient::new(NntpClientConfig {
            servers,
            max_idle_age: std::time::Duration::from_mins(5),
            max_retries_per_server: 1,
            soft_timeout: std::time::Duration::from_secs(15),
        });
        (client, total)
    };

    let pool = std::sync::Arc::clone(client.pool());
    match handle.rebuild_nntp(client, total).await {
        Ok(()) => handle.set_nntp_pool(pool),
        Err(error) => tracing::error!("failed to rebuild NNTP client: {error}"),
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::{RwLock, broadcast, mpsc};

    use super::*;
    use crate::events::model::PipelineEvent;
    use crate::jobs::SchedulerCommand;
    use crate::persistence::sql_runtime::SqlRuntime;
    use crate::servers::{ServerConfig, ServerDownloadQuotaConfig};
    use crate::{PipelineMetrics, SharedPipelineState};

    fn server(id: u32) -> ServerConfig {
        ServerConfig {
            id,
            host: format!("news-{id}.example.com"),
            port: 563,
            tls: true,
            username: None,
            password: None,
            connections: 2,
            active: true,
            supports_pipelining: true,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig::default(),
            tls_ca_cert: None,
        }
    }

    #[tokio::test]
    async fn policy_reconfigure_failure_rebuilds_with_servers_disabled() {
        let db = Database::open_in_memory().unwrap();
        let server = server(41);
        db.insert_server(&server).unwrap();
        let policy = Arc::new(
            crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                db.clone(),
                std::slice::from_ref(&server),
            )
            .unwrap(),
        );
        let datastore = db.datastore();
        db.run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "DROP TABLE server_download_usage",
                &[],
            )
            .await?;
            Ok(())
        })
        .unwrap();

        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: "/tmp/weaver-runtime-reload-test".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![server],
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        }));
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1);
        let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state);
        handle.set_server_transfer_policy(policy);
        let command_task = tokio::spawn(async move {
            let Some(SchedulerCommand::RebuildNntp {
                client,
                total_connections,
                reply,
            }) = cmd_rx.recv().await
            else {
                panic!("expected NNTP rebuild command");
            };
            let client = client
                .downcast::<weaver_nntp::client::NntpClient>()
                .expect("rebuild must carry an NNTP client");
            assert_eq!(total_connections, 0);
            assert_eq!(client.pool().server_count(), 0);
            let _ = reply.send(());
        });

        rebuild_nntp_from_config(&config, &handle).await;
        command_task.await.unwrap();

        assert_eq!(handle.nntp_pool().unwrap().server_count(), 0);
    }

    #[tokio::test]
    async fn missing_policy_registry_rebuilds_with_servers_disabled() {
        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: "/tmp/weaver-runtime-reload-missing-policy-test".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![server(42)],
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            config_path: None,
        }));
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1);
        let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state);
        let command_task = tokio::spawn(async move {
            let Some(SchedulerCommand::RebuildNntp {
                client,
                total_connections,
                reply,
            }) = cmd_rx.recv().await
            else {
                panic!("expected NNTP rebuild command");
            };
            let client = client
                .downcast::<weaver_nntp::client::NntpClient>()
                .expect("rebuild must carry an NNTP client");
            assert_eq!(total_connections, 0);
            assert_eq!(client.pool().server_count(), 0);
            let _ = reply.send(());
        });

        rebuild_nntp_from_config(&config, &handle).await;
        command_task.await.unwrap();

        assert_eq!(handle.nntp_pool().unwrap().server_count(), 0);
    }
}
