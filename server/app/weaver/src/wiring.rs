use std::path::Path;
use std::sync::Arc;

use tokio::sync::broadcast;
use tracing::{error, info};

use weaver_nntp::client::NntpClient;
use weaver_server_core::Database;
use weaver_server_core::events::model::PipelineEvent;
use weaver_server_core::events::publish::should_record_job_event;
use weaver_server_core::runtime::buffers::{BufferPool, BufferPoolConfig};
use weaver_server_core::runtime::system_profile::SystemProfile;
use weaver_server_core::servers::{ServerConfig, ServerConnectivityResult};
use weaver_server_core::settings::Config;

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
) {
    tokio::spawn(async move {
        if let Err(panic) = tokio::spawn(persist_events(event_rx, db)).await {
            tracing::error!(
                error = %panic,
                "CRITICAL: event persistence task panicked - events will not be recorded"
            );
        }
    });
}

async fn persist_events(mut rx: broadcast::Receiver<PipelineEvent>, db: Database) {
    use weaver_server_api::PipelineEventGql;

    let mut batch: Vec<weaver_server_core::JobEvent> = Vec::new();
    let flush_interval = tokio::time::Duration::from_secs(1);

    loop {
        let recv = if batch.is_empty() {
            tokio::select! {
                result = rx.recv() => result,
            }
        } else {
            tokio::select! {
                result = rx.recv() => result,
                _ = tokio::time::sleep(flush_interval) => {
                    let events = std::mem::take(&mut batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
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

                if batch.len() >= 50 {
                    let events = std::mem::take(&mut batch);
                    let db = db.clone();
                    tokio::task::spawn_blocking(move || {
                        if let Err(e) = db.insert_job_events(&events) {
                            tracing::warn!(error = %e, "failed to persist job events");
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use weaver_server_core::jobs::ids::JobId;

    #[tokio::test]
    async fn persist_events_keeps_job_events_and_skips_integration_events() {
        let db = Database::open_in_memory().unwrap();
        let (tx, rx) = broadcast::channel(8);

        let task = tokio::spawn(persist_events(rx, db.clone()));
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
}
