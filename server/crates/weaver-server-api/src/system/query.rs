use super::*;
use crate::observability::with_timed_config_read;
use crate::system::metrics_history::{build_metrics_history, tier_for_range};
use crate::system::types::{
    ApplicationUpgradeStatus, ConfiguredStorage, DatabaseEngineGql, DecoderTierGql,
    DeploymentEnvironmentGql, DiskCapacity, KernelComponentGql, KernelSelectionInfo,
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
    /// Latest stable Weaver release information observed by the background checker.
    #[graphql(guard = "ReadGuard")]
    async fn update_status(&self, ctx: &Context<'_>) -> Result<UpdateStatus> {
        let service = ctx.data::<weaver_server_core::update_check::UpdateCheckService>()?;
        Ok(service.status().into())
    }
    /// In-application upgrade availability and this installation's eligibility.
    #[graphql(guard = "ReadGuard")]
    async fn application_upgrade_status(
        &self,
        ctx: &Context<'_>,
    ) -> Result<ApplicationUpgradeStatus> {
        let service =
            ctx.data::<weaver_server_core::application_upgrade::ApplicationUpgradeService>()?;
        Ok(service.snapshot().into())
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
        let profile = runtime
            .profile
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
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
                kernels: weaver_server_core::runtime::kernels::selected_kernels()
                    .into_iter()
                    .map(kernel_selection_info)
                    .collect(),
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
    /// How full the disk under a folder is, for a folder that is not saved yet.
    ///
    /// A folder that does not exist yet reports the disk it would be created
    /// on. The answer is best effort: a network mount that does not answer
    /// within a few seconds reports an error instead of holding the request.
    #[graphql(guard = "AdminGuard")]
    async fn path_storage(&self, path: String) -> ConfiguredStorage {
        let requested = PathBuf::from(path.trim());
        let label = requested.display().to_string();
        let probe = tokio::task::spawn_blocking(move || {
            probe_configured_storage(ConfiguredStorageInput {
                labels: Vec::new(),
                path: nearest_existing_ancestor(&requested),
                error: None,
            })
        });
        let result = match tokio::time::timeout(PATH_STORAGE_TIMEOUT, probe).await {
            Ok(Ok(storage)) => storage,
            Ok(Err(error)) => ConfiguredStorage {
                labels: Vec::new(),
                path: label.clone(),
                capacity: None,
                error: Some(error.to_string()),
            },
            Err(_) => ConfiguredStorage {
                labels: Vec::new(),
                path: label.clone(),
                capacity: None,
                error: Some("Filesystem capacity did not answer in time.".to_string()),
            },
        };
        ConfiguredStorage {
            path: label,
            ..result
        }
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
        let transport = handle.download_transport_health();
        // Open sockets only mean "preparing" while a job is waiting to fetch
        // through them; the pool's keep-alive after a finished download, or a
        // paused queue, holds the same sockets open with nothing to prepare.
        let work_waiting = !handle.is_globally_paused()
            && handle.list_jobs().iter().any(|job| {
                matches!(
                    job.status,
                    weaver_server_core::JobStatus::Queued
                        | weaver_server_core::JobStatus::Downloading
                        | weaver_server_core::JobStatus::Checking
                )
            });
        match live_pool.or(fallback_pool) {
            Some(pool) => {
                Ok(
                    collect_server_health(&pool, runtime_generation, &transport, work_waiting)
                        .await,
                )
            }
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
                    let space = weaver_server_core::operations::probe_disk_space(
                        std::path::Path::new(&path),
                    )
                    .map_err(|error| {
                        tracing::debug!(%label, %path, %error, "disk usage row omitted");
                    })
                    .ok()?;
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

const PATH_STORAGE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);

/// The folder itself when it exists, otherwise the deepest parent that does.
fn nearest_existing_ancestor(path: &std::path::Path) -> PathBuf {
    path.ancestors()
        .find(|candidate| !candidate.as_os_str().is_empty() && candidate.exists())
        .unwrap_or(path)
        .to_path_buf()
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

    match weaver_server_core::operations::probe_disk_space(&input.path) {
        Ok(space) => ConfiguredStorage {
            labels: input.labels,
            path,
            capacity: Some(DiskCapacity {
                total_bytes: space.total_bytes,
                used_bytes: space.used_bytes(),
                free_bytes: space.available_bytes,
            }),
            error: None,
        },
        Err(error) => ConfiguredStorage {
            labels: input.labels,
            path,
            capacity: None,
            error: Some(format!(
                "Filesystem capacity is unavailable for this path: {error}"
            )),
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

fn kernel_selection_info(
    value: weaver_server_core::runtime::kernels::KernelSelection,
) -> KernelSelectionInfo {
    use weaver_server_core::runtime::kernels::KernelComponent;
    KernelSelectionInfo {
        component: match value.component {
            KernelComponent::YencDecode => KernelComponentGql::YencDecode,
            KernelComponent::YencCrc32 => KernelComponentGql::YencCrc32,
            KernelComponent::Par2Repair => KernelComponentGql::Par2Repair,
            KernelComponent::Par2Md5 => KernelComponentGql::Par2Md5,
            KernelComponent::Par2Crc32 => KernelComponentGql::Par2Crc32,
            KernelComponent::RarRecovery => KernelComponentGql::RarRecovery,
            KernelComponent::RarCrc32 => KernelComponentGql::RarCrc32,
            KernelComponent::RarSha1 => KernelComponentGql::RarSha1,
            KernelComponent::RarAes => KernelComponentGql::RarAes,
        },
        library: value.library.to_string(),
        ladder: value.ladder.into_iter().map(str::to_string).collect(),
        kernel: value.kernel.to_string(),
        pinned_by: value.pinned_by.map(str::to_string),
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

/// Rebase a monotonic deadline on the wall clock, so a browser can count down
/// to it. A deadline already in the past has nothing left to show.
fn instant_to_epoch_ms(until: std::time::Instant) -> Option<u64> {
    let remaining = until.saturating_duration_since(std::time::Instant::now());
    if remaining.is_zero() {
        return None;
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_millis();
    u64::try_from(now.saturating_add(remaining.as_millis())).ok()
}

/// How many sockets one server has connected, and how many of those are
/// carrying a request.
///
/// The socket budget owns physical sockets, so it is the only place that can
/// separate "a socket exists" from "a socket is carrying a request": a lane
/// parked on an open connection is neither a free permit nor a fetch in
/// flight. A socket still dialing is not open yet; everything else that is
/// not idling or closing is busy. `serverHealth` and the live metrics stream
/// both read through here so the two never disagree.
pub(crate) fn server_socket_counts(pool: &NntpPool, idx: usize) -> (u32, u32) {
    let sockets = pool.socket_budget_snapshot(idx);
    let open = sockets.physical.saturating_sub(sockets.dialing);
    let busy = open
        .saturating_sub(sockets.async_idle)
        .saturating_sub(sockets.owned_idle)
        .saturating_sub(sockets.closing);
    (open as u32, busy as u32)
}

/// Snapshot per-server health from the live NNTP pool. Mirrors the per-server fields
/// emitted by the Prometheus exporter (`collect_server_health` in the app binary), shaped
/// for the GraphQL monitoring API. The connection pool orders servers by priority, so the
/// first entry is the primary and the rest are backups.
async fn collect_server_health(
    pool: &NntpPool,
    runtime_generation: u64,
    transport: &[weaver_server_core::ServerTransportHealth],
    work_waiting: bool,
) -> Vec<ServerHealth> {
    struct ServerLoadSnapshot {
        host: String,
        port: u16,
        tier: String,
        active: usize,
        configured: usize,
        penalty_until: Option<u64>,
        open: u32,
        busy: u32,
    }

    let configs = pool.server_configs();
    // Read connection load outside the health lock.
    let pre: Vec<ServerLoadSnapshot> = configs
        .iter()
        .enumerate()
        .map(|(idx, cfg)| {
            let (_, max_connections) = pool.server_load(idx);
            let active = pool.active_connections(idx);
            let configured = pool
                .configured_connections(weaver_nntp::ServerId(idx))
                .unwrap_or(max_connections);
            let penalty_until = pool.over_limit_until_epoch_ms(weaver_nntp::ServerId(idx));
            let (open, busy) = server_socket_counts(pool, idx);
            let tier = if idx == 0 { "PRIMARY" } else { "BACKUP" };
            ServerLoadSnapshot {
                host: cfg.host.clone(),
                port: cfg.port,
                tier: tier.to_string(),
                active,
                configured,
                penalty_until,
                open,
                busy,
            }
        })
        .collect();

    // Whether any server in the pool is carrying a request right now. Open
    // sockets on a server that is not are only "preparing" when nothing else
    // is fetching either; behind a busy primary they are simply waiting.
    let pool_busy = pre.iter().any(|snapshot| snapshot.busy > 0);

    let health = pool.health().lock().await;
    pre.into_iter()
        .enumerate()
        .map(|(idx, snapshot)| {
            let srv = health.server(idx);
            let body = transport.iter().find(|entry| entry.server_idx == idx);
            let state = match srv.state() {
                weaver_nntp::ServerState::Healthy => "healthy",
                weaver_nntp::ServerState::Degraded { .. } => "degraded",
                weaver_nntp::ServerState::CoolingDown { .. } => "cooling_down",
                weaver_nntp::ServerState::Disabled { .. } => "disabled",
            };
            let open = snapshot.open;
            let busy = snapshot.busy;
            let activity = crate::system::types::server_activity(
                state,
                snapshot.penalty_until.is_some(),
                work_waiting,
                pool_busy,
                open,
                busy,
            );
            // The holdoff already carries a wall-clock deadline; a cooldown or
            // a quarantine carries a monotonic one, which only means anything
            // to the browser once it is rebased on the wall clock here.
            let activity_until = match activity {
                "over_limit" => snapshot.penalty_until,
                "cooling_down" => match srv.state() {
                    weaver_nntp::ServerState::CoolingDown { until, .. } => {
                        instant_to_epoch_ms(*until)
                    }
                    _ => None,
                },
                "disabled" => match srv.state() {
                    weaver_nntp::ServerState::Disabled { until, .. } => {
                        instant_to_epoch_ms(*until)
                    }
                    _ => None,
                },
                _ => None,
            };
            ServerHealth {
                label: format!("{}:{}", snapshot.host, snapshot.port),
                host: snapshot.host,
                port: snapshot.port,
                tier: snapshot.tier,
                state: state.to_string(),
                activity: activity.to_string(),
                activity_until_epoch_ms: activity_until,
                connections_open: open,
                connections_busy: busy,
                connections_active: snapshot.active as u32,
                connections_max: snapshot.configured as u32,
                connections_configured: snapshot.configured as u32,
                capacity_penalty_until_epoch_ms: snapshot.penalty_until,
                runtime_generation,
                latency_ms: health.latency_ms(idx),
                body_latency_ms: body.and_then(|entry| entry.latency_ms),
                body_transfer_ms: body.and_then(|entry| entry.transfer_ms),
                body_latency_band: body.and_then(|entry| entry.latency_band.clone()),
                // A server the lanes have not touched yet reads as sequential
                // rather than as a hole in the card.
                body_pipeline_depth: body.map_or(1, |entry| entry.pipeline_depth),
                body_pipelining_pinned_sequential: body
                    .is_some_and(|entry| entry.pinned_sequential),
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
