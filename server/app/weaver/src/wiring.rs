use std::path::Path;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info};

use weaver_nntp::client::NntpClient;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::events::publish::{pipeline_job_id, should_record_job_event};
use weaver_server_core::runtime::buffers::{BufferPool, BufferPoolConfig};
use weaver_server_core::runtime::system_profile::SystemProfile;
use weaver_server_core::servers::{ServerConfig, ServerConnectivityResult};
use weaver_server_core::settings::{Config, SharedConfig};
use weaver_server_core::{Database, SchedulerHandle};

pub(crate) struct RuntimeContext {
    pub profile: SystemProfile,
    pub buffers: Arc<BufferPool>,
    pub write_buf_max: usize,
}

pub(crate) fn build_runtime_context(output_dir: &Path) -> RuntimeContext {
    let profile = weaver_server_core::runtime::detect_system_profile(output_dir);
    info!(
        cores = profile.cpu.physical_cores,
        storage = ?profile.disk.storage_class,
        iops = format!("{:.0}", profile.disk.random_read_iops),
        "system profile"
    );

    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buf_config = BufferPoolConfig::for_available_memory(effective_memory);
    let write_buf_max = buf_config.write_buffer_max_pending();
    info!(
        available_mb = effective_memory / (1024 * 1024),
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

pub(crate) async fn detect_server_capabilities(config: &mut Config, db: &Database) {
    for server in config.servers.iter_mut().filter(|server| server.active) {
        let ServerConnectivityResult {
            success,
            message,
            supports_pipelining,
            ..
        } = weaver_server_core::servers::probe_server_connection(server).await;
        server.supports_pipelining = supports_pipelining;
        if success {
            info!(
                host = %server.host,
                supports_pipelining,
                "detected server capabilities"
            );
        } else {
            info!(
                host = %server.host,
                error = %message,
                "capability detection failed, assuming no pipelining"
            );
        }

        let persisted = server.clone();
        let db = db.clone();
        if let Err(join_error) =
            tokio::task::spawn_blocking(move || db.update_server(&persisted)).await
        {
            error!(error = %join_error, "failed to persist server capabilities");
        }
    }
}

pub(crate) fn build_nntp_client(config: &Config, profile: &SystemProfile) -> NntpClient {
    let mut active: Vec<&ServerConfig> = config
        .servers
        .iter()
        .filter(|server| server.active)
        .collect();
    active.sort_by_key(|server| (server.priority, server.id));
    let total_connections: usize = active
        .iter()
        .map(|server| server.connections as usize)
        .sum();
    let effective_memory = profile
        .memory
        .cgroup_limit
        .unwrap_or(profile.memory.available_bytes);
    let buffer_profile =
        weaver_nntp::connection::NntpBufferProfile::adaptive(effective_memory, total_connections);
    let servers = active
        .iter()
        .map(|server| weaver_nntp::pool::ServerPoolConfig {
            server: weaver_nntp::ServerConfig {
                host: server.host.clone(),
                port: server.port,
                tls: server.tls,
                username: server.username.clone(),
                password: server.password.clone(),
                tls_ca_cert: server.tls_ca_cert.clone(),
                buffer_profile,
                ..Default::default()
            },
            max_connections: server.connections as usize,
            group: server.priority,
        })
        .collect();

    NntpClient::new(weaver_nntp::client::NntpClientConfig {
        servers,
        max_idle_age: std::time::Duration::from_secs(300),
        max_retries_per_server: 1,
        soft_timeout: std::time::Duration::from_secs(15),
    })
}

pub(crate) fn spawn_event_persistence_task(
    event_rx: broadcast::Receiver<PipelineEvent>,
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
) {
    tokio::spawn(async move {
        if let Err(panic) = tokio::spawn(persist_events(event_rx, db, handle, config)).await {
            tracing::error!(
                error = %panic,
                "CRITICAL: event persistence task panicked - events will not be recorded"
            );
        }
    });
}

async fn persist_events(
    mut rx: broadcast::Receiver<PipelineEvent>,
    db: Database,
    handle: SchedulerHandle,
    config: SharedConfig,
) {
    use weaver_server_api::{
        PersistedQueueEvent, PipelineEventGql, QueueEventKind, QueueItemState, global_queue_state,
        queue_item_from_job,
    };

    let mut batch: Vec<weaver_server_core::JobEvent> = Vec::new();
    let mut last_states: std::collections::HashMap<u64, QueueItemState> =
        std::collections::HashMap::new();
    let mut last_progress_buckets: std::collections::HashMap<u64, u8> =
        std::collections::HashMap::new();
    let mut last_attention: std::collections::HashMap<u64, Option<(String, String)>> =
        std::collections::HashMap::new();
    let mut integration_batch: Vec<weaver_server_core::IntegrationEventRow> = Vec::new();
    let flush_interval = tokio::time::Duration::from_secs(1);

    loop {
        let recv = if batch.is_empty() && integration_batch.is_empty() {
            tokio::select! {
                result = rx.recv() => result,
            }
        } else {
            tokio::select! {
                result = rx.recv() => result,
                _ = tokio::time::sleep(flush_interval) => {
                    let events = std::mem::take(&mut batch);
                    let integration_events = std::mem::take(&mut integration_batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                        if let Err(e) = db.insert_integration_events(&integration_events) {
                            tracing::warn!(error = %e, "failed to persist integration events");
                        }
                    });
                    continue;
                }
            }
        };

        match recv {
            Ok(event) => {
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

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                if matches!(
                    event,
                    PipelineEvent::GlobalPaused | PipelineEvent::GlobalResumed
                ) {
                    let cfg = config.read().await;
                    let global_state = global_queue_state(
                        handle.is_globally_paused(),
                        &handle.get_download_block(),
                        cfg.max_download_speed.unwrap_or(0),
                    );
                    let record = PersistedQueueEvent {
                        occurred_at_ms: now,
                        kind: QueueEventKind::GlobalStateChanged,
                        item_id: None,
                        item: None,
                        state: None,
                        previous_state: None,
                        attention: None,
                        global_state: Some(global_state),
                    };
                    if let Ok(payload_json) = serde_json::to_string(&record) {
                        integration_batch.push(weaver_server_core::IntegrationEventRow {
                            id: 0,
                            timestamp: now,
                            kind: "GLOBAL_STATE_CHANGED".to_string(),
                            item_id: None,
                            payload_json,
                        });
                    }
                }

                if let Some(job_id) = pipeline_job_id(&event)
                    && let Ok(info) = handle.get_job(weaver_server_core::jobs::ids::JobId(job_id))
                {
                    let item = queue_item_from_job(&info);
                    let should_evict = matches!(
                        item.state,
                        QueueItemState::Completed | QueueItemState::Failed
                    );
                    let previous_state = last_states.insert(job_id, item.state);
                    let progress_bucket = item.progress_percent.floor() as u8;
                    let previous_progress = last_progress_buckets.insert(job_id, progress_bucket);
                    let attention_signature = item
                        .attention
                        .as_ref()
                        .map(|value| (value.code.clone(), value.message.clone()));
                    let previous_attention =
                        last_attention.insert(job_id, attention_signature.clone());

                    let mut records: Vec<PersistedQueueEvent> = Vec::new();
                    if matches!(event, PipelineEvent::JobCreated { .. }) {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemCreated,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state,
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    if let Some(previous_state) = previous_state
                        && previous_state != item.state
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: if item.state == QueueItemState::Completed {
                                QueueEventKind::ItemCompleted
                            } else {
                                QueueEventKind::ItemStateChanged
                            },
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: Some(previous_state),
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    if item.state != QueueItemState::Completed
                        && item.state != QueueItemState::Failed
                        && progress_bucket > 0
                        && previous_progress.is_none_or(|value| progress_bucket > value)
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemProgress,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: None,
                            attention: None,
                            global_state: None,
                        });
                    }

                    if attention_signature.is_some()
                        && attention_signature != previous_attention.flatten()
                    {
                        records.push(PersistedQueueEvent {
                            occurred_at_ms: now,
                            kind: QueueEventKind::ItemAttention,
                            item_id: Some(job_id),
                            item: Some(item.clone()),
                            state: Some(item.state),
                            previous_state: None,
                            attention: item.attention.clone(),
                            global_state: None,
                        });
                    }

                    for record in records {
                        if let Ok(payload_json) = serde_json::to_string(&record) {
                            integration_batch.push(weaver_server_core::IntegrationEventRow {
                                id: 0,
                                timestamp: now,
                                kind: format!("{:?}", record.kind),
                                item_id: record.item_id,
                                payload_json,
                            });
                        }
                    }

                    if should_evict {
                        last_states.remove(&job_id);
                        last_progress_buckets.remove(&job_id);
                        last_attention.remove(&job_id);
                    }
                }

                if batch.len() >= 50 || integration_batch.len() >= 50 {
                    let events = std::mem::take(&mut batch);
                    let integration_events = std::mem::take(&mut integration_batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
                        }
                        if let Err(e) = db.insert_integration_events(&integration_events) {
                            tracing::warn!(error = %e, "failed to persist integration events");
                        }
                    });
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::debug!(skipped = n, "event persistence lagged");
            }
            Err(broadcast::error::RecvError::Closed) => break,
        }
    }

    if !batch.is_empty() {
        let _ = db.insert_job_events(&batch);
    }
    if !integration_batch.is_empty() {
        let _ = db.insert_integration_events(&integration_batch);
    }
}
