use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::bandwidth::IspBandwidthCapConfig;
use crate::categories::CategoryConfig;
use crate::jobs::DuplicatePolicy;
use crate::servers::ServerConfig;
use crate::watch_folder::WatchFolderConfig;

/// Shared config handle for runtime reads and writes.
pub type SharedConfig = Arc<RwLock<Config>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    /// Directory for active downloads (per-job subdirectories).
    /// Defaults to `{data_dir}/intermediate`.
    pub intermediate_dir: Option<String>,
    /// Directory for completed downloads (category subdirectories).
    /// Defaults to `{data_dir}/complete`.
    pub complete_dir: Option<String>,
    #[serde(default)]
    pub buffer_pool: Option<BufferPoolOverrides>,
    #[serde(default)]
    pub tuner: Option<TunerOverrides>,
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    #[serde(default)]
    pub categories: Vec<CategoryConfig>,
    #[serde(default)]
    pub retry: Option<RetryOverrides>,
    /// Maximum download speed in bytes/sec. 0 or absent means unlimited.
    #[serde(default)]
    pub max_download_speed: Option<u64>,
    /// Whether to delete intermediate files (NZB articles, PAR2, RAR volumes)
    /// after successful extraction. Defaults to true.
    #[serde(default)]
    pub cleanup_after_extract: Option<bool>,
    /// Optional ISP bandwidth cap policy.
    #[serde(default)]
    pub isp_bandwidth_cap: Option<IspBandwidthCapConfig>,
    /// Optional global burst for make-before-break latent-IP replacement trials.
    /// Defaults to 0 and is capped at 1.
    #[serde(default)]
    pub ip_replacement_trial_extra_connections: Option<u8>,
    /// Watched-folder NZB intake settings.
    #[serde(default)]
    pub watch_folder: WatchFolderConfig,
    /// Duplicate admission handling policy.
    #[serde(default)]
    pub duplicate_policy: DuplicatePolicy,
    /// RAR direct-store routing. Absent means "every default".
    #[serde(default)]
    pub direct_store: Option<DirectStoreOverrides>,
    /// Naming policy for the files a finished job delivers. Absent means
    /// "every default".
    #[serde(default)]
    pub delivery_naming: Option<DeliveryNamingOverrides>,
    /// Prometheus exposition knobs.
    #[serde(default)]
    pub metrics: MetricsConfig,
    /// Path to the config file on disk. Not serialized to TOML.
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

impl Config {
    /// Returns the intermediate directory for active downloads.
    /// Defaults to `{data_dir}/intermediate`.
    pub fn intermediate_dir(&self) -> String {
        self.intermediate_dir
            .clone()
            .unwrap_or_else(|| format!("{}/intermediate", self.data_dir))
    }

    /// Returns the complete directory for finished downloads.
    /// Defaults to `{data_dir}/complete`.
    pub fn complete_dir(&self) -> String {
        self.complete_dir
            .clone()
            .unwrap_or_else(|| format!("{}/complete", self.data_dir))
    }

    /// Whether to clean up intermediate files after successful extraction.
    /// Defaults to `true` when not explicitly configured.
    pub fn cleanup_after_extract(&self) -> bool {
        self.cleanup_after_extract.unwrap_or(true)
    }

    /// Whether a finished job may rename a delivered member that still wears an
    /// obfuscated name. Defaults to `true`: an obfuscated member is unusable to
    /// every downstream tool, and the pass refuses itself whenever the job's own
    /// name is no better.
    pub fn deobfuscate_delivered_members(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.deobfuscate_delivered_members)
            .unwrap_or(true)
    }

    /// Whether an obfuscated member may be looked up by CRC32 in the public
    /// srrdb release index before falling back to the job's own name.
    ///
    /// Defaults to `false`. This is the only part of completion that leaves the
    /// operator's network, so it stays an explicit opt-in even though the
    /// request carries nothing but a checksum.
    ///
    /// **The `WEAVER_SRRDB_LOOKUP` environment switch overrides this in both
    /// directions and is how the rung is turned on today** — this row is the
    /// durable home the settings UI will eventually own.
    pub fn enable_srrdb_lookup(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.enable_srrdb_lookup)
            .unwrap_or(false)
    }

    pub fn ip_replacement_trial_extra_connections(&self) -> u8 {
        self.ip_replacement_trial_extra_connections
            .unwrap_or(0)
            .min(1)
    }

    /// Validate the configuration, returning any issues found.
    /// Empty server list is allowed (users add servers via UI).
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        for (i, server) in self.servers.iter().enumerate() {
            if server.port == 0 {
                errors.push(format!("server[{i}] ({}) port must be > 0", server.host));
            }
            if server.connections == 0 {
                errors.push(format!(
                    "server[{i}] ({}) connections must be > 0",
                    server.host
                ));
            }
            if let Err(error) = server.validate_download_limits() {
                errors.push(format!("server[{i}] ({}) {error}", server.host));
            }
        }

        if self.data_dir.is_empty() {
            errors.push("data_dir must not be empty".to_string());
        }

        if self.ip_replacement_trial_extra_connections.unwrap_or(0) > 1 {
            errors.push("ip_replacement_trial_extra_connections must be 0 or 1".to_string());
        }

        if let Err(error) = self.watch_folder.validate() {
            errors.push(error);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Save the config back to disk.
    pub fn save(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| std::io::Error::other("no config path set"))?;
        let toml_str = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, toml_str)
    }

    /// Return the next available server ID.
    pub fn next_server_id(&self) -> u32 {
        self.servers.iter().map(|s| s.id).max().unwrap_or(0) + 1
    }

    /// Assign IDs to any servers that have id == 0 (backward compat with old TOML files).
    pub fn assign_server_ids(&mut self) {
        let mut next = self.next_server_id();
        for server in &mut self.servers {
            if server.id == 0 {
                server.id = next;
                next += 1;
            }
        }
    }

    /// Return the next available category ID.
    pub fn next_category_id(&self) -> u32 {
        self.categories.iter().map(|c| c.id).max().unwrap_or(0) + 1
    }

    /// Assign IDs to any categories that have id == 0.
    pub fn assign_category_ids(&mut self) {
        let mut next = self.next_category_id();
        for cat in &mut self.categories {
            if cat.id == 0 {
                cat.id = next;
                next += 1;
            }
        }
    }
}

/// Operator-facing switches for RAR direct-store routing.
///
/// These answer the plan's open question 1 — config, not env-only — while
/// keeping an env override for incident response. Precedence is
/// **environment over config over default**, and it is resolved in
/// `pipeline::direct_store::DirectStoreSettings::resolve`; see that type for
/// the exact variable names.
///
/// Every field is optional so an absent `[direct_store]` table, a partially
/// filled one and an older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectStoreOverrides {
    /// Route eligible unencrypted RAR `Store` sets straight to their final
    /// destinations, so their volumes never exist as files.
    ///
    /// **Defaults to off.** Turning the default on is a release decision, not a
    /// config default change.
    pub enabled: Option<bool>,
    /// Per-set ceiling on the holds scratch file, in bytes. Decoded bytes whose
    /// destination is not yet known are held in RAM and paged here on a breach;
    /// breaching *this* ceiling demotes that one set. Defaults to 512 MiB.
    pub holds_scratch_ceiling_bytes: Option<u64>,
}

/// Operator-facing switches for how a finished job names what it delivers
/// (`[delivery_naming]`).
///
/// Every field is optional so an absent table, a partially filled one and an
/// older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeliveryNamingOverrides {
    /// Rename a delivered member that still wears an obfuscated name to the
    /// job's own name. **Defaults to on.**
    pub deobfuscate_delivered_members: Option<bool>,
    /// Before falling back to the job name, ask the public srrdb release index
    /// what release the member's CRC32 belongs to.
    ///
    /// **Defaults to off.** Completion is otherwise entirely local, so anything
    /// that reaches outside the operator's network is opt-in.
    pub enable_srrdb_lookup: Option<bool>,
}

/// Prometheus exposition knobs (`[metrics]`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// How much per-job detail `/metrics` carries. Per-job series are the
    /// exporter's only unbounded label dimension: the runtime keeps up to a
    /// thousand finished jobs, and each one would otherwise mint a full set of
    /// value series that never goes away.
    #[serde(default)]
    pub per_job_series: PerJobSeries,
}

/// Which jobs get their own `weaver_job_*` series.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerJobSeries {
    /// Only jobs that are still moving (everything but complete and failed).
    #[default]
    Active,
    /// Every job the runtime still remembers, finished ones included.
    All,
    /// No per-job series at all; `weaver_pipeline_jobs{status}` still reports
    /// the aggregate queue mix.
    Off,
}

impl PerJobSeries {
    pub const ALL: [Self; 3] = [Self::Active, Self::All, Self::Off];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::All => "all",
            Self::Off => "off",
        }
    }

    /// Parse a persisted setting value; unknown text falls back to the default
    /// so a typo degrades to the safe cardinality rather than failing startup.
    pub fn from_str_or_default(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Self::All,
            "off" => Self::Off,
            _ => Self::Active,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolOverrides {
    pub small_count: Option<usize>,
    pub medium_count: Option<usize>,
    pub large_count: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryOverrides {
    pub max_retries: Option<u32>,
    pub base_delay_secs: Option<f64>,
    pub multiplier: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TunerOverrides {
    pub max_concurrent_downloads: Option<usize>,
    pub decode_thread_count: Option<usize>,
    /// Number of threads in the post-processing pool (extraction, PAR2 verify/repair).
    /// Defaults to `(physical_cores / 2).max(1)`.
    pub extract_thread_count: Option<usize>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servers::{MAX_PERSISTED_SERVER_DOWNLOAD_BYTES, ServerDownloadQuotaConfig};

    fn config_with_server() -> Config {
        Config {
            data_dir: "/tmp/weaver".to_string(),
            intermediate_dir: None,
            complete_dir: None,
            buffer_pool: None,
            tuner: None,
            servers: vec![ServerConfig {
                id: 1,
                host: "news.example.com".to_string(),
                port: 563,
                tls: true,
                username: None,
                password: None,
                connections: 4,
                active: false,
                supports_pipelining: false,
                tls_name_mismatch_certificate_der: None,
                priority: 0,
                backfill: false,
                retention_days: 0,
                max_download_speed: 0,
                download_quota: ServerDownloadQuotaConfig::default(),
                tls_ca_cert: None,
            }],
            categories: vec![],
            retry: None,
            max_download_speed: None,
            cleanup_after_extract: None,
            isp_bandwidth_cap: None,
            ip_replacement_trial_extra_connections: None,
            watch_folder: WatchFolderConfig::default(),
            duplicate_policy: DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            metrics: Default::default(),
            config_path: None,
        }
    }

    #[test]
    fn per_job_series_parses_persisted_values_and_defaults_safely() {
        assert_eq!(PerJobSeries::from_str_or_default("all"), PerJobSeries::All);
        assert_eq!(
            PerJobSeries::from_str_or_default(" OFF "),
            PerJobSeries::Off
        );
        assert_eq!(
            PerJobSeries::from_str_or_default("active"),
            PerJobSeries::Active
        );
        // A typo must not fail startup or silently pick the highest-cardinality
        // setting; it falls back to the default.
        assert_eq!(
            PerJobSeries::from_str_or_default("evrything"),
            PerJobSeries::Active
        );
        assert_eq!(PerJobSeries::default(), PerJobSeries::Active);
        for mode in PerJobSeries::ALL {
            assert_eq!(PerJobSeries::from_str_or_default(mode.as_str()), mode);
        }
        assert_eq!(
            config_with_server().metrics.per_job_series,
            PerJobSeries::Active
        );
    }

    #[test]
    fn validate_rejects_server_download_limits_outside_database_range() {
        let mut config = config_with_server();
        config.servers[0].max_download_speed = MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1;
        let errors = config.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("max download speed exceeds database range"))
        );

        config.servers[0].max_download_speed = 0;
        config.servers[0].download_quota.limit_bytes = MAX_PERSISTED_SERVER_DOWNLOAD_BYTES + 1;
        let errors = config.validate().unwrap_err();
        assert!(
            errors
                .iter()
                .any(|error| error.contains("download quota limit exceeds database range"))
        );
    }
}
