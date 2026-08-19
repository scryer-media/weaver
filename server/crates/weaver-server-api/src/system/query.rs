use super::*;
use crate::observability::with_timed_config_read;
use crate::system::metrics_history::{build_metrics_history, tier_for_range};
use crate::system::types::{
    ConfiguredStorage, DatabaseEngineGql, DecoderTierGql, DeploymentEnvironmentGql, DiskCapacity,
    MetricsHistoryRangeGql, OperatingSystemGql, ServerRestartCapability, SystemComputeInfo,
    SystemInfo, SystemMemoryInfo, SystemStorageProfile,
};
use std::path::PathBuf;
use std::sync::Arc;
use weaver_nntp::pool::NntpPool;

#[derive(Default)]
pub(crate) struct SystemQuery;

#[Object]
impl SystemQuery {
    /// The running weaver binary version.
    async fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    /// Safe runtime and storage facts for the built-in troubleshooting UI.
    #[graphql(guard = "ReadGuard")]
    async fn system_info(&self, ctx: &Context<'_>) -> Result<SystemInfo> {
        let runtime = ctx.data::<crate::context::SystemRuntimeContext>()?;
        let config = ctx.data::<SharedConfig>()?;
        let database = ctx.data::<Database>()?;
        let storage_inputs = with_timed_config_read(config, "system.query.system_info", |cfg| {
            configured_storage_inputs(cfg)
        })
        .await;
        let configured_storage = tokio::task::spawn_blocking(move || {
            storage_inputs
                .into_iter()
                .map(probe_configured_storage)
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        let environment = weaver_server_core::runtime::environment::detect_runtime_environment();
        let profile = &runtime.profile;
        let simd = &profile.cpu.simd;
        let mut simd_features = Vec::new();
        if simd.sse42 {
            simd_features.push("SSE 4.2".to_string());
        }
        if simd.avx2 {
            simd_features.push("AVX2".to_string());
        }
        if simd.avx512 {
            simd_features.push("AVX-512".to_string());
        }
        if simd.neon {
            simd_features.push("NEON".to_string());
        }

        let effective_limit_bytes = profile
            .memory
            .cgroup_limit
            .map(|limit| profile.memory.total_bytes.min(limit))
            .unwrap_or(profile.memory.total_bytes);

        Ok(SystemInfo {
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: runtime.started_at.elapsed().as_secs_f64(),
            deployment: deployment_environment_gql(environment.deployment),
            operating_system: operating_system_gql(environment.operating_system),
            architecture: environment.architecture.to_string(),
            database_engine: if database.engine_name() == "postgres" {
                DatabaseEngineGql::Postgres
            } else {
                DatabaseEngineGql::Sqlite
            },
            compute: SystemComputeInfo {
                physical_cores: u32::try_from(profile.cpu.physical_cores).unwrap_or(u32::MAX),
                logical_cores: u32::try_from(profile.cpu.logical_cores).unwrap_or(u32::MAX),
                cgroup_limit: profile.cpu.cgroup_limit,
                decoder_tier: decoder_tier_gql(weaver_yenc::simd::selected_decoder_tier()),
                simd_features,
            },
            memory: SystemMemoryInfo {
                total_bytes: profile.memory.total_bytes,
                available_at_startup_bytes: profile.memory.available_bytes,
                cgroup_limit_bytes: profile.memory.cgroup_limit,
                effective_limit_bytes,
            },
            primary_storage: SystemStorageProfile {
                storage_class: storage_class_name(&profile.disk.storage_class).to_string(),
                filesystem: filesystem_name(&profile.disk.filesystem),
                startup_random_read_iops: profile.disk.random_read_iops,
            },
            configured_storage,
        })
    }
    /// Whether Weaver can restart itself here, and the deployment that decided
    /// it — everything the security wizard needs in one field.
    ///
    /// Deliberately its own small field rather than part of `systemInfo`,
    /// whose resolver probes storage: the wizard asks this on every app load,
    /// so the answer has to stay cheap.
    #[graphql(guard = "AdminGuard")]
    async fn server_restart(&self) -> ServerRestartCapability {
        use weaver_server_core::runtime::restart::{
            resolvable_executable, restart_capability, ui_restart_enabled,
        };

        // One detection answers both the restart rule and the deployment the
        // wizard's bind question branches on.
        let environment = weaver_server_core::runtime::environment::detect_runtime_environment();
        let capability = restart_capability(
            &environment,
            resolvable_executable().as_deref(),
            ui_restart_enabled(),
        );
        ServerRestartCapability {
            supported: capability.supported,
            reason: capability.reason,
            deployment: deployment_environment_gql(environment.deployment),
        }
    }
    /// System status facade for integrations.
    #[graphql(guard = "ReadGuard")]
    async fn system_status(&self, ctx: &Context<'_>) -> Result<SystemStatus> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let config = ctx.data::<SharedConfig>()?;
        let items: Vec<QueueItem> = handle
            .list_jobs()
            .into_iter()
            .filter(|info| {
                !matches!(
                    info.status,
                    weaver_server_core::JobStatus::Complete
                        | weaver_server_core::JobStatus::Failed { .. }
                )
            })
            .map(|info| queue_item_from_job(&info))
            .collect();
        let metrics = handle.get_metrics();
        let max_download_speed = with_timed_config_read(
            config,
            "system.query.system_status.max_download_speed",
            |cfg| cfg.max_download_speed.unwrap_or(0),
        )
        .await;
        let global_state = global_queue_state(
            handle.is_globally_paused(),
            &handle.get_download_block(),
            max_download_speed,
        );
        Ok(SystemStatus {
            version: env!("CARGO_PKG_VERSION").to_string(),
            global_state,
            summary: queue_summary(&items, &metrics),
        })
    }
    /// System metrics facade for integrations.
    #[graphql(guard = "ReadGuard")]
    async fn system_metrics(&self, ctx: &Context<'_>) -> Result<Metrics> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(metrics_from_snapshot(&handle.get_metrics()))
    }
    /// Tiered local metrics history for the built-in monitoring UI.
    #[graphql(guard = "ReadGuard")]
    async fn metrics_history(
        &self,
        ctx: &Context<'_>,
        range: MetricsHistoryRangeGql,
    ) -> Result<MetricsHistoryResult> {
        let db = ctx.data::<Database>()?.clone();
        let now_epoch_sec = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let since_epoch_sec = now_epoch_sec - range.window_sec();
        let tier = tier_for_range(range);

        tokio::task::spawn_blocking(move || {
            let history = db
                .read_metrics_history(tier, since_epoch_sec, now_epoch_sec)
                .map_err(|error| error.to_string())?;
            build_metrics_history(history)
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        .map_err(|error| graphql_error("INTERNAL", error))
    }
    #[graphql(guard = "AdminGuard")]
    async fn browse_directories(
        &self,
        ctx: &Context<'_>,
        path: Option<String>,
    ) -> Result<DirectoryBrowseResult> {
        let config = ctx.data::<SharedConfig>()?;
        let default_path = with_timed_config_read(
            config,
            "system.query.browse_directories.default_path",
            |cfg| cfg.complete_dir(),
        )
        .await;
        let explicit_path = path
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
        let requested_path = if let Some(path) = explicit_path {
            std::path::PathBuf::from(path)
        } else {
            absolutize_default_browse_path(default_path)
                .map_err(|error| graphql_error("INTERNAL", error.to_string()))?
        };

        let listing = tokio::task::spawn_blocking(move || {
            weaver_server_core::operations::browse_directories(&requested_path)
        })
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?
        .map_err(|error| match error {
            weaver_server_core::operations::BrowseDirectoryError::InvalidInput(message) => {
                graphql_error("INVALID_INPUT", message)
            }
            weaver_server_core::operations::BrowseDirectoryError::Internal(message) => {
                graphql_error("INTERNAL", message)
            }
        })?;

        Ok(listing.into())
    }
    /// Return recent log lines from the in-memory ring buffer.
    #[graphql(guard = "AdminGuard")]
    async fn service_logs(
        &self,
        ctx: &Context<'_>,
        #[graphql(default = 250)] limit: i32,
    ) -> Result<ServiceLogsPayload> {
        let buffer = ctx.data::<LogRingBuffer>()?;
        let lines = weaver_server_core::operations::snapshot_service_logs(buffer, limit);
        let count = lines.len() as i32;
        Ok(ServiceLogsPayload { lines, count })
    }
    /// Get current pipeline metrics.
    async fn metrics(&self, ctx: &Context<'_>) -> Result<Metrics> {
        let handle = ctx.data::<SchedulerHandle>()?;
        let snapshot = handle.get_metrics();
        Ok(Metrics::from(&snapshot))
    }
    /// Check whether the pipeline is globally paused.
    async fn is_paused(&self, ctx: &Context<'_>) -> Result<bool> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(handle.is_globally_paused())
    }
    /// Current global download block state (manual pause or ISP cap).
    async fn download_block(&self, ctx: &Context<'_>) -> Result<DownloadBlock> {
        let handle = ctx.data::<SchedulerHandle>()?;
        Ok(DownloadBlock::from(&handle.get_download_block()))
    }

    /// Live per-server NNTP health (connections, latency, state) for the monitoring dashboard.
    #[graphql(guard = "ReadGuard")]
    async fn server_health(&self, ctx: &Context<'_>) -> Result<Vec<ServerHealth>> {
        let handle = ctx.data::<weaver_server_core::SchedulerHandle>()?;
        let live_pool = handle.nntp_pool();
        let runtime_generation = handle
            .nntp_runtime_activation()
            .map(|activation| activation.generation)
            .unwrap_or(0);
        let fallback_pool = ctx
            .data_opt::<Option<Arc<NntpPool>>>()
            .and_then(Clone::clone);
        match live_pool.or(fallback_pool) {
            Some(pool) => Ok(collect_server_health(&pool, runtime_generation).await),
            None => Ok(Vec::new()),
        }
    }

    /// Filesystem capacity for the configured storage directories (data / intermediate / complete).
    #[graphql(guard = "ReadGuard")]
    async fn disk_usage(&self, ctx: &Context<'_>) -> Result<Vec<DiskUsage>> {
        let config = ctx.data::<SharedConfig>()?;
        let dirs = with_timed_config_read(config, "system.query.disk_usage", |cfg| {
            vec![
                ("Data".to_string(), cfg.data_dir.clone()),
                ("Intermediate downloads".to_string(), cfg.intermediate_dir()),
                ("Complete library".to_string(), cfg.complete_dir()),
            ]
        })
        .await;

        let usage = tokio::task::spawn_blocking(move || {
            dirs.into_iter()
                .filter_map(|(label, path)| -> Option<DiskUsage> {
                    let space =
                        weaver_server_core::operations::disk_space(std::path::Path::new(&path))?;
                    Some(DiskUsage {
                        label,
                        total_bytes: space.total_bytes,
                        used_bytes: space.used_bytes(),
                        free_bytes: space.available_bytes,
                        path,
                    })
                })
                .collect::<Vec<_>>()
        })
        .await
        .map_err(|error| graphql_error("INTERNAL", error.to_string()))?;

        Ok(usage)
    }
}

#[derive(Debug)]
struct ConfiguredStorageInput {
    labels: Vec<String>,
    path: PathBuf,
    error: Option<String>,
}

fn configured_storage_inputs(
    config: &weaver_server_core::settings::Config,
) -> Vec<ConfiguredStorageInput> {
    let complete_dir = PathBuf::from(config.complete_dir());
    let mut inputs = Vec::new();
    push_storage_input(&mut inputs, "Data", PathBuf::from(&config.data_dir));
    push_storage_input(
        &mut inputs,
        "Intermediate downloads",
        PathBuf::from(config.intermediate_dir()),
    );
    push_storage_input(&mut inputs, "Complete library", complete_dir.clone());

    let mut categories = config.categories.iter().collect::<Vec<_>>();
    categories.sort_by_key(|category| category.name.to_ascii_lowercase());
    for category in categories {
        let label = format!("Category: {}", category.name);
        match weaver_server_core::categories::completion_parent(
            &complete_dir,
            &config.categories,
            Some(&category.name),
        ) {
            Ok(path) => push_storage_input(&mut inputs, label, path),
            Err(error) => inputs.push(ConfiguredStorageInput {
                labels: vec![label],
                path: category
                    .dest_dir
                    .as_deref()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| complete_dir.join(&category.name)),
                error: Some(error),
            }),
        }
    }
    inputs
}

fn push_storage_input(
    inputs: &mut Vec<ConfiguredStorageInput>,
    label: impl Into<String>,
    path: PathBuf,
) {
    let label = label.into();
    if let Some(existing) = inputs
        .iter_mut()
        .find(|input| input.error.is_none() && input.path == path)
    {
        existing.labels.push(label);
    } else {
        inputs.push(ConfiguredStorageInput {
            labels: vec![label],
            path,
            error: None,
        });
    }
}

fn probe_configured_storage(input: ConfiguredStorageInput) -> ConfiguredStorage {
    let path = input.path.display().to_string();
    if let Some(error) = input.error {
        return ConfiguredStorage {
            labels: input.labels,
            path,
            capacity: None,
            error: Some(error),
        };
    }

    match weaver_server_core::operations::disk_space(&input.path) {
        Some(space) => ConfiguredStorage {
            labels: input.labels,
            path,
            capacity: Some(DiskCapacity {
                total_bytes: space.total_bytes,
                used_bytes: space.used_bytes(),
                free_bytes: space.available_bytes,
            }),
            error: None,
        },
        None => ConfiguredStorage {
            labels: input.labels,
            path,
            capacity: None,
            error: Some("Filesystem capacity is unavailable for this path".to_string()),
        },
    }
}

fn deployment_environment_gql(
    value: weaver_server_core::runtime::environment::DeploymentEnvironment,
) -> DeploymentEnvironmentGql {
    use weaver_server_core::runtime::environment::DeploymentEnvironment;
    match value {
        DeploymentEnvironment::Native => DeploymentEnvironmentGql::Native,
        DeploymentEnvironment::Docker => DeploymentEnvironmentGql::Docker,
        DeploymentEnvironment::Container => DeploymentEnvironmentGql::Container,
    }
}

fn operating_system_gql(
    value: weaver_server_core::runtime::environment::OperatingSystem,
) -> OperatingSystemGql {
    use weaver_server_core::runtime::environment::OperatingSystem;
    match value {
        OperatingSystem::Linux => OperatingSystemGql::Linux,
        OperatingSystem::Macos => OperatingSystemGql::Macos,
        OperatingSystem::Windows => OperatingSystemGql::Windows,
        OperatingSystem::Unknown => OperatingSystemGql::Unknown,
    }
}

fn decoder_tier_gql(value: weaver_yenc::simd::SelectedDecoderTier) -> DecoderTierGql {
    use weaver_yenc::simd::SelectedDecoderTier;
    match value {
        SelectedDecoderTier::Avx512Vbmi2 => DecoderTierGql::Avx512Vbmi2,
        SelectedDecoderTier::Avx2 => DecoderTierGql::Avx2,
        SelectedDecoderTier::Avx => DecoderTierGql::Avx,
        SelectedDecoderTier::Sse41 => DecoderTierGql::Sse41,
        SelectedDecoderTier::Ssse3 => DecoderTierGql::Ssse3,
        SelectedDecoderTier::Sse2 => DecoderTierGql::Sse2,
        SelectedDecoderTier::Neon => DecoderTierGql::Neon,
        SelectedDecoderTier::Scalar => DecoderTierGql::Scalar,
    }
}

fn storage_class_name(
    value: &weaver_server_core::runtime::system_profile::StorageClass,
) -> &'static str {
    use weaver_server_core::runtime::system_profile::StorageClass;
    match value {
        StorageClass::Ssd => "SSD",
        StorageClass::Hdd => "HDD",
        StorageClass::Network => "Network",
        StorageClass::Unknown => "Unknown",
    }
}

fn filesystem_name(value: &weaver_server_core::runtime::system_profile::FilesystemType) -> String {
    use weaver_server_core::runtime::system_profile::FilesystemType;
    match value {
        FilesystemType::Ext4 => "ext4".to_string(),
        FilesystemType::Xfs => "XFS".to_string(),
        FilesystemType::Zfs => "ZFS".to_string(),
        FilesystemType::Btrfs => "Btrfs".to_string(),
        FilesystemType::Apfs => "APFS".to_string(),
        FilesystemType::Ntfs => "NTFS".to_string(),
        FilesystemType::Nfs => "NFS".to_string(),
        FilesystemType::Smb => "SMB".to_string(),
        FilesystemType::Unknown(name) if !name.is_empty() => name.clone(),
        FilesystemType::Unknown(_) => "Unknown".to_string(),
    }
}

/// Snapshot per-server health from the live NNTP pool. Mirrors the per-server fields
/// emitted by the Prometheus exporter (`collect_server_health` in the app binary), shaped
/// for the GraphQL monitoring API. The connection pool orders servers by priority, so the
/// first entry is the primary and the rest are backups.
async fn collect_server_health(pool: &NntpPool, runtime_generation: u64) -> Vec<ServerHealth> {
    struct ServerLoadSnapshot {
        host: String,
        port: u16,
        tier: String,
        active: usize,
        effective: usize,
        configured: usize,
        penalty_until: Option<u64>,
    }

    let configs = pool.server_configs();
    // Read connection load outside the health lock.
    let pre: Vec<ServerLoadSnapshot> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let (_, effective) = pool.server_load(idx);
            let active = pool.active_connections(idx);
            let configured = pool
                .configured_connections(weaver_nntp::ServerId(idx))
                .unwrap_or(effective);
            let penalty_until = pool.capacity_penalty_until_epoch_ms(weaver_nntp::ServerId(idx));
            let tier = if idx == 0 { "PRIMARY" } else { "BACKUP" };
            ServerLoadSnapshot {
                host: cfg.host.clone(),
                port: cfg.port,
                tier: tier.to_string(),
                active,
                effective,
                configured,
                penalty_until,
            }
        })
        .collect();

    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(|(idx, snapshot)| {
            let srv = health.server(idx);
            let state = match srv.state() {
                weaver_nntp::ServerState::Healthy => "healthy",
                weaver_nntp::ServerState::Degraded { .. } => "degraded",
                weaver_nntp::ServerState::CoolingDown { .. } => "cooling_down",
                weaver_nntp::ServerState::Disabled { .. } => "disabled",
            };
            ServerHealth {
                label: format!("{}:{}", snapshot.host, snapshot.port),
                host: snapshot.host,
                port: snapshot.port,
                tier: snapshot.tier,
                state: state.to_string(),
                connections_active: snapshot.active as u32,
                connections_max: snapshot.effective as u32,
                connections_configured: snapshot.configured as u32,
                connections_effective: snapshot.effective as u32,
                capacity_penalty_until_epoch_ms: snapshot.penalty_until,
                runtime_generation,
                latency_ms: health.latency_ms(idx),
                success_count: srv.success_count,
                failure_count: srv.failure_count,
                consecutive_failures: srv.consecutive_failures,
                premature_deaths: health.recent_premature_deaths(idx) as u32,
            }
        })
        .collect()
}

fn absolutize_default_browse_path(path: String) -> std::io::Result<std::path::PathBuf> {
    let path = std::path::PathBuf::from(path);
    if path.is_absolute() {
        Ok(path)
    } else {
        std::env::current_dir().map(|cwd| cwd.join(path))
    }
}
