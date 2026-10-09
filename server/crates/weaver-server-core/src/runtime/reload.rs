use crate::Database;
use crate::settings::{Config, SharedConfig};
use crate::{NntpRuntimeActivation, SchedulerError, SchedulerHandle};

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

/// Pool settings for every active server, in dial order. Startup and every
/// rebuild take their NNTP client from here, so a server's route, adopted
/// certificate and proven pipelining depth apply from the first connection
/// after a restart exactly as they do after a change.
pub fn nntp_server_pool_configs(
    configured_servers: &[crate::servers::ServerConfig],
    proxy_runtime: Option<&crate::proxies::ProxyRuntime>,
    transfer_registry: &weaver_nntp::transfer::ServerTransferRegistry,
    buffer_profile: weaver_nntp::connection::NntpBufferProfile,
) -> Result<Vec<weaver_nntp::pool::ServerPoolConfig>, String> {
    use weaver_nntp::transfer::StableServerId;

    let mut active: Vec<&crate::servers::ServerConfig> = configured_servers
        .iter()
        .filter(|server| server.active)
        .collect();
    active.sort_by_key(|server| (server.priority, server.id));
    active
        .iter()
        .map(|server| {
            Ok(weaver_nntp::pool::ServerPoolConfig {
                server: weaver_nntp::ServerConfig {
                    dialer: proxy_runtime
                        .map(|runtime| {
                            runtime.network.nntp_dialer(
                                server.id,
                                server.connections,
                                std::time::Duration::from_secs(30),
                            )
                        })
                        .transpose()?,
                    host: server.host.clone(),
                    port: server.port,
                    tls: server.tls,
                    username: server.username.clone(),
                    password: server.password.clone(),
                    tls_ca_cert: server.tls_ca_cert.clone(),
                    tls_name_mismatch_certificate_der: server
                        .tls_name_mismatch_certificate_der
                        .clone(),
                    buffer_profile,
                    pipelining: weaver_nntp::PipeliningCapability::Known(
                        server.supports_pipelining,
                    ),
                    pipelining_depth: server.pipelining_depth,
                    ..Default::default()
                },
                max_connections: server.connections as usize,
                group: server.priority,
                backfill: server.backfill,
                retention_days: server.retention_days,
                stable_id: StableServerId(server.id),
                transfer_control: Some(transfer_registry.control(StableServerId(server.id))),
            })
        })
        .collect()
}

/// The NNTP client for one generation of pool settings.
pub fn nntp_client(
    servers: Vec<weaver_nntp::pool::ServerPoolConfig>,
) -> weaver_nntp::client::NntpClient {
    weaver_nntp::client::NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers,
        max_idle_age: std::time::Duration::from_mins(5),
        max_retries_per_server: 1,
        soft_timeout: std::time::Duration::from_secs(15),
    })
}

pub async fn rebuild_nntp_from_config(
    config: &SharedConfig,
    handle: &SchedulerHandle,
) -> Result<NntpRuntimeActivation, SchedulerError> {
    let policy_registry = handle.server_transfer_policy().ok_or_else(|| {
        SchedulerError::Internal("server transfer policy registry unavailable".to_string())
    })?;
    let transfer_registry = policy_registry.transfer_registry();
    let proxy_runtime = handle.proxy_runtime();

    let configured_servers = config.read().await.servers.clone();
    let registry = std::sync::Arc::clone(&policy_registry);
    let servers = configured_servers.clone();
    tokio::task::spawn_blocking(move || registry.reconfigure(&servers))
        .await
        .map_err(|error| {
            SchedulerError::Internal(format!(
                "server transfer policy reconfiguration task failed: {error}"
            ))
        })?
        .map_err(|error| {
            SchedulerError::Internal(format!(
                "failed to reconfigure server transfer policies: {error}"
            ))
        })?;

    let servers = nntp_server_pool_configs(
        &configured_servers,
        proxy_runtime.as_deref(),
        &transfer_registry,
        Default::default(),
    )
    .map_err(SchedulerError::Internal)?;
    let total: usize = servers.iter().map(|server| server.max_connections).sum();
    tracing::info!(
        active_server_count = servers.len(),
        total_connections = total,
        "building NNTP runtime generation"
    );
    let client = nntp_client(servers);

    let pool = std::sync::Arc::clone(client.pool());
    let activation = handle.rebuild_nntp(client, total).await?;
    handle.set_nntp_pool(pool);
    Ok(activation)
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

    if let Some(proxies) = handle.proxy_runtime() {
        proxies.reload().await?;
    }
    rebuild_nntp_from_config(config, handle)
        .await
        .map_err(|error| error.to_string())?;
    handle
        .set_speed_limit(loaded.max_download_speed.unwrap_or(0))
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
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: ServerDownloadQuotaConfig::default(),
            tls_ca_cert: None,
        }
    }

    #[tokio::test]
    async fn policy_reconfigure_failure_preserves_prior_generation() {
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
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            servers: vec![server],
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            propagation_delay_secs: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }));
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1);
        let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state);
        handle.set_server_transfer_policy(policy);
        let error = rebuild_nntp_from_config(&config, &handle)
            .await
            .expect_err("policy failure must not replace the active generation");
        assert!(error.to_string().contains("transfer policies"));
        assert!(cmd_rx.try_recv().is_err());
        assert!(handle.nntp_pool().is_none());
    }

    #[tokio::test]
    async fn missing_policy_registry_preserves_prior_generation() {
        let config: SharedConfig = Arc::new(RwLock::new(Config {
            data_dir: "/tmp/weaver-runtime-reload-missing-policy-test".to_string(),
            hardware_profile: None,
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            servers: vec![server(42)],
            categories: Vec::new(),
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            propagation_delay_secs: None,
            watch_folder: crate::watch_folder::WatchFolderConfig::default(),
            duplicate_policy: Default::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }));
        let (cmd_tx, mut cmd_rx) = mpsc::channel(1);
        let (event_tx, _) = broadcast::channel::<PipelineEvent>(1);
        let state = SharedPipelineState::new(PipelineMetrics::new(), Vec::new());
        let handle = SchedulerHandle::new(cmd_tx, event_tx, state);
        let error = rebuild_nntp_from_config(&config, &handle)
            .await
            .expect_err("missing policy registry must not replace the active generation");
        assert!(error.to_string().contains("registry unavailable"));
        assert!(cmd_rx.try_recv().is_err());
        assert!(handle.nntp_pool().is_none());
    }
    #[tokio::test]
    async fn scheduled_server_activation_failure_is_retried_not_treated_as_unchanged() {
        let db = Database::open_in_memory().unwrap();
        db.insert_server(&server(42)).unwrap();
        let config = Arc::new(RwLock::new(db.load_config().unwrap()));
        let (commands, _received) = mpsc::channel(1);
        let (events, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            commands,
            events,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        let service =
            crate::servers::service::ServersService::new(db.clone(), config.clone(), handle);
        // No transfer-policy registry: activation must fail before a new generation,
        // and the stored row is put back so a restart retries the same change.
        for _ in 0..2 {
            assert!(service.set_active(42, false).await.is_err());
            assert!(db.load_config().unwrap().servers[0].active);
            assert!(config.read().await.servers[0].active);
        }
    }

    #[tokio::test]
    async fn scheduled_server_activation_gates_startup_readiness() {
        use crate::bandwidth::schedule::{ScheduleServices, spawn_evaluator_with_services};
        let db = Database::open_in_memory().unwrap();
        let provider = server(42);
        db.insert_server(&provider).unwrap();
        let config = Arc::new(RwLock::new(db.load_config().unwrap()));
        let registry = Arc::new(
            crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                db.clone(),
                &[provider],
            )
            .unwrap(),
        );
        let (commands, mut received) = mpsc::channel(1);
        let (events, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            commands,
            events,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        handle.set_server_transfer_policy(registry);
        let schedules = Arc::new(RwLock::new(vec![crate::bandwidth::ScheduleEntry {
            id: "provider-hold".into(),
            enabled: true,
            label: String::new(),
            days: vec![],
            time: "00:00".into(),
            times: vec![],
            every_hour_at_minute: None,
            action: crate::bandwidth::ScheduleAction::SetServerActive {
                server_id: 42,
                active: false,
            },
        }]));
        let (task, mut ready) = spawn_evaluator_with_services(
            handle.clone(),
            schedules,
            ScheduleServices {
                servers: Some(crate::servers::service::ServersService::new(
                    db.clone(),
                    config,
                    handle,
                )),
                db: Some(db),
                ..Default::default()
            },
        );
        let crate::SchedulerCommand::RebuildNntp { reply, .. } = received.recv().await.unwrap()
        else {
            panic!("expected server runtime rebuild");
        };
        tokio::select! {
            biased;
            _ = &mut ready => panic!("startup became ready before server hold activated"),
            () = std::future::ready(()) => {}
        }
        reply
            .send(Ok(NntpRuntimeActivation {
                generation: 1,
                configured_connections: 0,
            }))
            .unwrap();
        ready.await.unwrap().unwrap();
        task.shutdown().await;
    }

    #[tokio::test]
    async fn scheduled_server_activation_persists_and_only_rebuilds_nntp_generations() {
        let db = Database::open_in_memory().unwrap();
        let provider = server(42);
        db.insert_server(&provider).unwrap();
        let config = Arc::new(RwLock::new(db.load_config().unwrap()));
        let registry = Arc::new(
            crate::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                db.clone(),
                std::slice::from_ref(&provider),
            )
            .unwrap(),
        );
        let (commands, mut received) = mpsc::channel(2);
        let (events, _) = broadcast::channel(1);
        let handle = SchedulerHandle::new(
            commands,
            events,
            SharedPipelineState::new(PipelineMetrics::new(), vec![]),
        );
        handle.set_server_transfer_policy(registry);
        let (generations, mut activations) = mpsc::channel(2);
        let pipeline = tokio::spawn(async move {
            let mut generation = 0;
            while let Some(command) = received.recv().await {
                match command {
                    crate::SchedulerCommand::RebuildNntp {
                        total_connections,
                        reply,
                        ..
                    } => {
                        generation += 1;
                        let activation = NntpRuntimeActivation {
                            generation,
                            configured_connections: total_connections,
                        };
                        generations
                            .send((generation, total_connections))
                            .await
                            .unwrap();
                        reply.send(Ok(activation)).unwrap();
                    }
                    _ => panic!(
                        "a server toggle must not alter download, speed, profile or quota tracks"
                    ),
                }
            }
        });
        let service =
            crate::servers::service::ServersService::new(db.clone(), config.clone(), handle);
        service.set_active(42, false).await.unwrap();
        assert_eq!(activations.recv().await.unwrap(), (1, 0));
        assert!(!db.load_config().unwrap().servers[0].active);
        service.set_active(42, true).await.unwrap();
        assert_eq!(activations.recv().await.unwrap(), (2, 2));
        assert!(db.load_config().unwrap().servers[0].active);
        assert!(config.read().await.servers[0].active);
        service.set_active(42, true).await.unwrap();
        assert!(
            activations.try_recv().is_err(),
            "an unchanged active state must not rebuild"
        );
        config.write().await.servers.clear();
        assert!(service.set_active(42, false).await.is_err());
        assert!(
            db.load_config().unwrap().servers[0].active,
            "missing runtime entry must not mutate persistence"
        );
        assert!(service.set_active(999, true).await.is_err());
        drop(service);
        pipeline.await.unwrap();
    }

    #[test]
    fn deleting_server_removes_only_its_schedules() {
        use crate::bandwidth::{ScheduleAction, ScheduleEntry};
        let db = Database::open_in_memory().unwrap();
        for id in [42, 43] {
            db.insert_server(&server(id)).unwrap();
        }
        let rules: Vec<_> = [42, 43]
            .into_iter()
            .map(|server_id| ScheduleEntry {
                id: format!("provider-{server_id}"),
                enabled: true,
                label: String::new(),
                days: vec![],
                time: "08:00".into(),
                times: vec![],
                every_hour_at_minute: None,
                action: ScheduleAction::SetServerActive {
                    server_id,
                    active: false,
                },
            })
            .collect();
        db.save_schedules(&rules).unwrap();
        assert!(db.delete_server(42).unwrap());
        assert_eq!(db.list_schedules().unwrap(), rules[1..]);
        assert_eq!(db.load_config().unwrap().servers[0].id, 43);
        assert!(db.save_schedules(&rules).is_err());
        assert_eq!(db.list_schedules().unwrap(), rules[1..]);
    }
}
