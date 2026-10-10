use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info};

use weaver_nntp::client::NntpClient;
use weaver_server_core::Database;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::events::publish::should_record_job_event;
use weaver_server_core::runtime::buffers::{BufferPool, BufferPoolConfig};
use weaver_server_core::runtime::system_profile::SystemProfile;
use weaver_server_core::settings::Config;

pub(crate) struct RuntimeContext {
    pub profile: SystemProfile,
    pub buffers: Arc<BufferPool>,
    pub write_buf_max: usize,
}

pub(crate) fn build_runtime_context(profile: SystemProfile) -> RuntimeContext {
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        iops = format!("{:.0}", profile.disk.random_read_iops),
        "system profile"
    );

    let buffer_sizing_memory = BufferPoolConfig::runtime_sizing_memory_bytes(
        profile.memory.available_bytes,
        profile.memory.cgroup_limit,
    );
    let buf_config = BufferPoolConfig::for_runtime_memory(
        profile.memory.available_bytes,
        profile.memory.cgroup_limit,
    );
    let write_buf_max = buf_config.write_buffer_max_pending();
    info!(
        available_mb = profile.memory.available_bytes / (1024 * 1024),
        cgroup_limit_mb = profile
            .memory
            .cgroup_limit
            .map(|value| value / (1024 * 1024)),
        sizing_mb = buffer_sizing_memory / (1024 * 1024),
        total_mb = buf_config.total_bytes() / (1024 * 1024),
        small = buf_config.small_count,
        medium = buf_config.medium_count,
        large = buf_config.large_count,
        write_buf_max,
        "buffer pool initialized (memory-adaptive)"
    );

    RuntimeContext {
        profile,
        buffers: BufferPool::new(buf_config),
        write_buf_max,
    }
}

pub(crate) fn build_nntp_client(
    config: &Config,
    profile: &SystemProfile,
    policy_registry: &weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry,
    proxies: &weaver_server_core::proxies::ProxyRuntime,
) -> Result<NntpClient, String> {
    let total_connections: usize = config
        .servers
        .iter()
        .filter(|server| server.active)
        .map(|server| server.connections as usize)
        .sum();
    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buffer_profile =
        weaver_nntp::connection::NntpBufferProfile::adaptive(effective_memory, total_connections);
    let servers = weaver_server_core::runtime::reload::nntp_server_pool_configs(
        &config.servers,
        Some(proxies),
        &policy_registry.transfer_registry(),
        buffer_profile,
    )?;
    Ok(weaver_server_core::runtime::reload::nntp_client(servers))
}

pub(crate) async fn flush_server_transfer_usage(
    policy: Arc<weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry>,
    context: &'static str,
) {
    match tokio::task::spawn_blocking(move || policy.flush_usage()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            error!(error = %error, context, "failed to flush server download usage");
        }
        Err(error) => {
            error!(error = %error, context, "server download usage flush task failed");
        }
    }
}

// Spawn the event-persistence subscriber and return its `JoinHandle` so the
// shutdown path can await the final `flush_write_queue`. `shutdown` is an
// explicit exit signal: the broadcast channel's senders (held by the long-lived
// `SchedulerHandle` and its clones in the GraphQL schema, RSS/backup/watch
// services, etc.) outlive the pipeline, so `rx.recv()` never observes `Closed`
// at shutdown — the caller notifies `shutdown` to make the task drain and exit.
pub(crate) fn spawn_event_persistence_task(
    event_rx: broadcast::Receiver<PipelineEvent>,
    db: Database,
    shutdown: Arc<tokio::sync::Notify>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(panic) = tokio::spawn(persist_events(event_rx, db, shutdown)).await {
            tracing::error!(
                error = %panic,
                "CRITICAL: event persistence task panicked - events will not be recorded"
            );
        }
    })
}

async fn persist_events(
    mut rx: broadcast::Receiver<PipelineEvent>,
    db: Database,
    shutdown: Arc<tokio::sync::Notify>,
) {
    use weaver_server_api::PipelineEventGql;

    let mut batch: Vec<weaver_server_core::JobEvent> = Vec::new();
    let flush_interval = tokio::time::Duration::from_secs(1);
    let mut flush_tick = tokio::time::interval(flush_interval);
    flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    flush_tick.tick().await;

    loop {
        let recv = tokio::select! {
            result = rx.recv() => result,
            _ = flush_tick.tick(), if !batch.is_empty() => {
                flush_job_event_batch(&db, &mut batch).await;
                continue;
            }
            _ = shutdown.notified() => break,
        };

        match recv {
            Ok(event) => {
                let barrier = matches!(
                    event,
                    PipelineEvent::RepairComplete { .. }
                        | PipelineEvent::EmbeddedProtectionReplaced { .. }
                        | PipelineEvent::JobCompleted { .. }
                        | PipelineEvent::JobFailed { .. }
                        | PipelineEvent::JobCancelled { .. }
                );
                if should_record_job_event(&event) {
                    let gql = PipelineEventGql::from(&event);
                    if let Some(job_id) = gql.job_id {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        batch.push(weaver_server_core::JobEvent {
                            job_id,
                            timestamp: now,
                            kind: format!("{:?}", gql.kind),
                            message: gql.message,
                            file_id: gql.file_id,
                        });
                    }
                }

                if batch.len() >= 50 || barrier {
                    flush_job_event_batch(&db, &mut batch).await;
                }
                if barrier && let Err(error) = db.flush_write_queue().await {
                    tracing::warn!(%error, "failed to flush lifecycle event boundary");
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::debug!(skipped = n, "event persistence lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    if !batch.is_empty() {
        flush_job_event_batch(&db, &mut batch).await;
    }
    if let Err(error) = db.flush_write_queue().await {
        tracing::warn!(error = %error, "failed to flush final job event writes");
    }
}

async fn flush_job_event_batch(db: &Database, batch: &mut Vec<weaver_server_core::JobEvent>) {
    if batch.is_empty() {
        return;
    }
    let events = std::mem::take(batch);
    if let Err(error) = db.queue_job_events(events).await {
        tracing::warn!(error = %error, "failed to queue job events");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use weaver_server_core::jobs::ids::JobId;

    fn test_profile() -> SystemProfile {
        use weaver_server_core::runtime::system_profile::{
            CpuProfile, DiskProfile, FilesystemType, MemoryProfile, StorageClass,
        };
        SystemProfile {
            cpu: CpuProfile {
                physical_cores: 2,
                logical_cores: 2,
                simd: Default::default(),
                cgroup_limit: None,
            },
            memory: MemoryProfile {
                total_bytes: 1024 * 1024 * 1024,
                available_bytes: 1024 * 1024 * 1024,
                cgroup_limit: None,
            },
            disk: DiskProfile {
                storage_class: StorageClass::Unknown,
                filesystem: FilesystemType::Unknown("test".into()),
                sequential_write_mbps: 0.0,
                random_read_iops: 0.0,
                same_filesystem: true,
            },
        }
    }

    #[tokio::test]
    async fn startup_client_dials_through_the_server_route() {
        use tokio::io::AsyncWriteExt;
        use weaver_server_core::proxies::{
            Consumer, EgressBinding, EgressInterface, LegPath, ProxyRuntime, RouteLeg,
            RoutingPolicy,
        };

        // A reachable server: a client that ignored the route would connect
        // here on the system route and read the greeting.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let _ = socket.write_all(b"200 ready\r\n").await;
            }
        });

        let db = Database::open_in_memory().unwrap();
        db.insert_server(&weaver_server_core::servers::ServerConfig {
            id: 7,
            host: "127.0.0.1".into(),
            port,
            tls: false,
            username: None,
            password: None,
            connections: 2,
            active: true,
            supports_pipelining: true,
            pipelining_depth: Some(4),
            tls_name_mismatch_certificate_der: Some(vec![1, 2, 3]),
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: Default::default(),
            tls_ca_cert: None,
        })
        .unwrap();
        // The only leg leaves through an interface this host does not have.
        let egress = db
            .create_egress_interface(&EgressInterface {
                id: 0,
                name: "tunnel".into(),
                binding: EgressBinding::Interface {
                    name: "weaver-absent0".into(),
                },
                enabled: true,
                max_download_speed: 0,
                download_quota: Default::default(),
            })
            .unwrap();
        db.save_proxy_routing_policy(
            Consumer::Server(7),
            &RoutingPolicy {
                legs: vec![RouteLeg {
                    egress_id: egress.id,
                    weight: 100,
                    path: LegPath::Direct,
                }],
                ..Default::default()
            },
        )
        .unwrap();

        let config = db.load_config().unwrap();
        let registry =
            weaver_server_core::servers::transfer_policy::ServerTransferPolicyRegistry::new(
                db.clone(),
                &config.servers,
            )
            .unwrap();
        let proxies = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
        let client = build_nntp_client(&config, &test_profile(), &registry, &proxies).unwrap();

        let server = &client.pool().server_configs()[0];
        assert!(server.dialer.is_some(), "startup must use the route dialer");
        assert_eq!(server.pipelining_depth, Some(4));
        assert_eq!(
            server.tls_name_mismatch_certificate_der.as_deref(),
            Some(&[1, 2, 3][..])
        );
        assert!(
            weaver_nntp::connection::NntpConnection::connect(server)
                .await
                .is_err(),
            "a server whose route cannot leave must not fall back to the system route"
        );
        accept.abort();
    }

    #[tokio::test]
    async fn persist_events_keeps_job_events_and_skips_integration_events() {
        let db = Database::open_in_memory().unwrap();
        let (tx, rx) = broadcast::channel(8);

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(persist_events(rx, db.clone(), shutdown));
        tx.send(PipelineEvent::JobCreated {
            job_id: JobId(7),
            name: "test-job".to_string(),
            total_files: 1,
            total_bytes: 1024,
        })
        .unwrap();
        tx.send(PipelineEvent::JobPaused { job_id: JobId(7) })
            .unwrap();
        drop(tx);

        task.await.unwrap();

        let job_events = db.get_job_events(7).unwrap();
        assert_eq!(job_events.len(), 2);
        assert!(
            db.list_integration_events_after(None, None, None)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn embedded_repair_warning_survives_database_reopen() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("events.db");
        let db = Database::open(&path).unwrap();
        let (tx, rx) = broadcast::channel(8);
        let task = tokio::spawn(persist_events(
            rx,
            db.clone(),
            Arc::new(tokio::sync::Notify::new()),
        ));
        let warning = PipelineEvent::EmbeddedProtectionReplaced {
            job_id: JobId(7),
            blocks_repaired: 3,
        };
        assert_eq!(
            weaver_server_core::events::publish::pipeline_job_id(&warning),
            Some(7)
        );
        tx.send(warning).unwrap();
        tx.send(PipelineEvent::JobCompleted { job_id: JobId(7) })
            .unwrap();
        drop(tx);
        task.await.unwrap();
        drop(db);

        let reopened = Database::open(&path).unwrap();
        let events = reopened.get_job_events(7).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "RepairComplete");
        assert!(events[0].message.starts_with("3 blocks repaired."));
        assert!(
            events[0]
                .message
                .contains("original carrier could not be restored byte for byte")
        );
        assert_eq!(events[1].kind, "JobCompleted");
        assert!(events[0].file_id.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn persist_events_flushes_partial_batches_while_events_continue() {
        let db = Database::open_in_memory().unwrap();
        let (tx, rx) = broadcast::channel(64);

        let shutdown = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(persist_events(rx, db.clone(), shutdown));
        // Keep the stream busy on the paused clock: one event per 50 ms, never
        // idle, never closed and never near the 50-event batch size. Only the
        // periodic flush can make an event visible.
        let mut sent = 0;
        loop {
            tx.send(PipelineEvent::JobPaused { job_id: JobId(7) })
                .unwrap();
            sent += 1;
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            db.flush_write_queue().await.unwrap();
            if !db.get_job_events(7).unwrap().is_empty() {
                break;
            }
        }
        assert!(
            sent < 50,
            "event persistence should flush partial batches without waiting for an idle event stream ({sent} sent)"
        );

        drop(tx);
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn repair_boundary_flushes_before_the_periodic_deadline_or_shutdown() {
        let db = Database::open_in_memory().unwrap();
        let (tx, rx) = broadcast::channel(64);
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(persist_events(rx, db.clone(), shutdown.clone()));
        // Let the interval's immediate first tick finish before sending a batch.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        let started = tokio::time::Instant::now();
        for id in [7, 8] {
            tx.send(PipelineEvent::JobPaused { job_id: JobId(id) })
                .unwrap();
            tx.send(PipelineEvent::RepairComplete {
                job_id: JobId(id),
                slices_repaired: 3,
            })
            .unwrap();
        }
        // Yield to the subscriber and SQLite worker, without advancing time,
        // closing the channel, reaching the batch size or notifying shutdown.
        while db.get_job_events(8).unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
        assert_eq!(tokio::time::Instant::now(), started);
        for id in [7, 8] {
            let events = db.get_job_events(id).unwrap();
            assert_eq!(events.len(), 2);
            assert_eq!(events[1].kind, "RepairComplete");
        }
        shutdown.notify_one();
        task.await.unwrap();
    }
}
