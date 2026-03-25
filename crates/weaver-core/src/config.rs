use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

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
        }

        if self.data_dir.is_empty() {
            errors.push("data_dir must not be empty".to_string());
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IspBandwidthCapPeriod {
    Daily,
    Weekly,
    Monthly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IspBandwidthCapWeekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IspBandwidthCapConfig {
    #[serde(default)]
    pub enabled: bool,
    pub period: IspBandwidthCapPeriod,
    pub limit_bytes: u64,
    pub reset_time_minutes_local: u16,
    pub weekly_reset_weekday: IspBandwidthCapWeekday,
    pub monthly_reset_day: u8,
}

// ── Download schedules ──────────────────────────────────────────────────────

/// A time-based rule that pauses, resumes, or changes the speed limit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScheduleEntry {
    pub id: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub label: String,
    /// Days this schedule applies. Empty = every day.
    #[serde(default)]
    pub days: Vec<Weekday>,
    /// Time of day (HH:MM, 24-hour, local time).
    pub time: String,
    pub action: ScheduleAction,
}

/// What a schedule entry does when it fires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScheduleAction {
    Pause,
    Resume,
    SpeedLimit {
        /// Bytes per second. 0 = unlimited.
        bytes_per_sec: u64,
    },
}

/// Day of week for schedule entries. Reuses the same serialization as
/// [`IspBandwidthCapWeekday`] but is a separate type to avoid coupling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Weekday {
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
    Sun,
}

impl Weekday {
    /// Convert from `chrono::Weekday`.
    pub fn from_chrono(w: chrono::Weekday) -> Self {
        match w {
            chrono::Weekday::Mon => Self::Mon,
            chrono::Weekday::Tue => Self::Tue,
            chrono::Weekday::Wed => Self::Wed,
            chrono::Weekday::Thu => Self::Thu,
            chrono::Weekday::Fri => Self::Fri,
            chrono::Weekday::Sat => Self::Sat,
            chrono::Weekday::Sun => Self::Sun,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    pub host: String,
    pub port: u16,
    pub tls: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub connections: u16,
    /// Whether this server is enabled. Defaults to true.
    #[serde(default = "default_true")]
    pub active: bool,
    /// Whether the server supports NNTP command pipelining (RFC 4644).
    /// Auto-detected when the server is added or tested.
    #[serde(default)]
    pub supports_pipelining: bool,
    /// Priority group (0 = primary, 1+ = backfill). Lower values tried first.
    #[serde(default)]
    pub priority: u32,
    /// Optional path to a PEM-encoded CA certificate to trust for TLS
    /// connections to this server (e.g. self-signed or internal CAs).
    #[serde(default)]
    pub tls_ca_cert: Option<std::path::PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CategoryConfig {
    /// Stable identifier for CRUD operations.
    #[serde(default)]
    pub id: u32,
    /// Unique category name (canonical form).
    pub name: String,
    /// Optional destination directory override. If absent, uses
    /// `{complete_dir}/{name}/` as the default.
    #[serde(default)]
    pub dest_dir: Option<String>,
    /// Comma-separated aliases for matching from RSS/URL/API submissions.
    /// Supports glob-style wildcards (`*` and `?`).
    #[serde(default)]
    pub aliases: String,
}

/// Resolve a category string to a canonical category name.
///
/// 1. Exact case-insensitive match on category names.
/// 2. Glob match against comma-separated aliases.
///
/// Returns the canonical name of the first match, or `None`.
pub fn resolve_category(categories: &[CategoryConfig], input: &str) -> Option<String> {
    let input_lower = input.trim();
    if input_lower.is_empty() {
        return None;
    }

    // Exact name match (case-insensitive).
    for cat in categories {
        if cat.name.eq_ignore_ascii_case(input_lower) {
            return Some(cat.name.clone());
        }
    }

    // Alias glob match.
    for cat in categories {
        for alias in cat.aliases.split(',') {
            let alias = alias.trim();
            if !alias.is_empty() && glob_match_ci(alias, input_lower) {
                return Some(cat.name.clone());
            }
        }
    }

    None
}

/// Simple case-insensitive glob matcher supporting `*` (any sequence) and `?` (any single char).
fn glob_match_ci(pattern: &str, input: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = input.chars().collect();
    let (mut pi, mut si) = (0, 0);
    let (mut star_pi, mut star_si) = (usize::MAX, 0);

    while si < s.len() {
        if pi < p.len() && p[pi] == '*' {
            star_pi = pi;
            star_si = si;
            pi += 1;
        } else if pi < p.len() && (p[pi] == '?' || p[pi].eq_ignore_ascii_case(&s[si])) {
            pi += 1;
            si += 1;
        } else if star_pi != usize::MAX {
            pi = star_pi + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
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
    pub max_decode_queue: Option<usize>,
    pub decode_thread_count: Option<usize>,
    /// Number of threads in the post-processing pool (extraction, PAR2 verify/repair).
    /// Defaults to `(physical_cores / 2).max(1)`.
    pub extract_thread_count: Option<usize>,
}
