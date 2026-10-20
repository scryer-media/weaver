// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::categories::CategoryConfig;
use crate::jobs::DuplicatePolicy;
use crate::runtime::hardware_profile::HardwareProfile;
use crate::servers::ServerConfig;
use crate::watch_folder::WatchFolderConfig;

// Shared config handle for runtime reads and writes.
pub type SharedConfig = Arc<RwLock<Config>>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub data_dir: String,
    // Directory for active downloads (per-job subdirectories).
    // Defaults to `{data_dir}/intermediate`.
    pub intermediate_dir: Option<String>,
    // Directory for completed downloads (category subdirectories).
    // Defaults to `{data_dir}/complete`.
    pub complete_dir: Option<String>,
    #[serde(default)]
    pub buffer_pool: Option<BufferPoolOverrides>,
    #[serde(default)]
    pub servers: Vec<ServerConfig>,
    #[serde(default)]
    pub categories: Vec<CategoryConfig>,
    #[serde(default)]
    pub retry: Option<RetryOverrides>,
    // The global bandwidth limits, kept in networking.
    #[serde(default)]
    pub bandwidth: crate::proxies::Bandwidth,
    // Minimum post age before downloading. Zero disables the hold.
    #[serde(default)]
    pub propagation_delay_secs: Option<u32>,
    // Whether to delete intermediate files (NZB articles, PAR2, RAR volumes)
    // after successful extraction. Defaults to true.
    #[serde(default)]
    pub cleanup_after_extract: Option<bool>,
    // Watched-folder NZB intake settings.
    #[serde(default)]
    pub watch_folder: WatchFolderConfig,
    // Duplicate admission handling policy.
    #[serde(default)]
    pub duplicate_policy: DuplicatePolicy,
    // RAR direct-store routing. Absent means "every default".
    #[serde(default)]
    pub direct_store: Option<DirectStoreOverrides>,
    // 7z and ZIP/ZIP64 direct unpack. Absent means "every default".
    #[serde(default)]
    pub direct_unpack: Option<DirectUnpackOverrides>,
    // Naming policy for the files a finished job delivers. Absent means
    // "every default".
    #[serde(default)]
    pub delivery_naming: Option<DeliveryNamingOverrides>,
    // How hard Weaver leans on this machine. Absent means the operator has
    // never chosen one, and the machine's recommendation stands in.
    #[serde(default)]
    pub hardware_profile: Option<HardwareProfile>,
    // Prometheus exposition knobs.
    #[serde(default)]
    pub metrics: MetricsConfig,
    // The global seeding policy. Absent means no goal: a torrent seeds until
    // it is stopped.
    #[serde(default)]
    pub seeding: Option<SeedingPolicy>,
    #[serde(default)]
    pub torrent: TorrentConfig,
    // Path to the config file on disk. Not serialized to TOML.
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

impl Config {
    // Returns the intermediate directory for active downloads.
    // Defaults to `{data_dir}/intermediate`.
    pub fn intermediate_dir(&self) -> String {
        self.intermediate_dir
            .clone()
            .unwrap_or_else(|| format!("{}/intermediate", self.data_dir))
    }

    // Returns the complete directory for finished downloads.
    // Defaults to `{data_dir}/complete`.
    pub fn complete_dir(&self) -> String {
        self.complete_dir
            .clone()
            .unwrap_or_else(|| format!("{}/complete", self.data_dir))
    }

    // Whether to clean up intermediate files after successful extraction.
    // Defaults to `true` when not explicitly configured.
    pub fn cleanup_after_extract(&self) -> bool {
        self.cleanup_after_extract.unwrap_or(true)
    }

    // Whether a finished job may rename a delivered member that still wears an
    // obfuscated name. Defaults to `true`: an obfuscated member is unusable to
    // every downstream tool, and the pass refuses itself whenever the job's own
    // name is no better.
    pub fn deobfuscate_delivered_members(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.deobfuscate_delivered_members)
            .unwrap_or(true)
    }

    // Whether an obfuscated member may be looked up by CRC32 in the public
    // srrdb release index before falling back to the job's own name.
    //
    // Defaults to `false`. This is the only part of completion that leaves the
    // operator's network, so it stays an explicit opt-in even though the
    // request carries nothing but a checksum.
    //
    pub fn enable_srrdb_lookup(&self) -> bool {
        self.delivery_naming
            .as_ref()
            .and_then(|naming| naming.enable_srrdb_lookup)
            .unwrap_or(false)
    }

    // A saved setting takes precedence over the legacy environment override.
    pub fn propagation_delay_secs(&self) -> u32 {
        self.propagation_delay_secs
            .or_else(|| {
                std::env::var("WEAVER_PROPAGATION_DELAY_SECS")
                    .ok()
                    .and_then(|value| value.trim().parse().ok())
            })
            .unwrap_or(0)
    }

    // Validate the configuration, returning any issues found.
    // Empty server list is allowed (users add servers via UI).
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

        if let Err(error) = self.watch_folder.validate() {
            errors.push(error);
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    // Save the config back to disk.
    pub fn save(&self) -> std::io::Result<()> {
        let path = self
            .config_path
            .as_ref()
            .ok_or_else(|| std::io::Error::other("no config path set"))?;
        let toml_str = toml::to_string_pretty(self).map_err(std::io::Error::other)?;
        std::fs::write(path, toml_str)
    }

    // Return the next available server ID.
    pub fn next_server_id(&self) -> u32 {
        self.servers.iter().map(|s| s.id).max().unwrap_or(0) + 1
    }

    // Assign IDs to any servers that have id == 0 (backward compat with old TOML files).
    pub fn assign_server_ids(&mut self) {
        let mut next = self.next_server_id();
        for server in &mut self.servers {
            if server.id == 0 {
                server.id = next;
                next += 1;
            }
        }
    }

    // Return the next available category ID.
    pub fn next_category_id(&self) -> u32 {
        self.categories.iter().map(|c| c.id).max().unwrap_or(0) + 1
    }

    // Assign IDs to any categories that have id == 0.
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

// Operator-facing switches for RAR direct-store routing.
//
// These answer the plan's open question 1 — config, not env-only — while
// keeping an env override for incident response. Precedence is
// **environment over config over default**, and it is resolved in
// `pipeline::direct_store::DirectStoreSettings::resolve`; see that type for
// the exact variable names.
//
// Every field is optional so an absent `[direct_store]` table, a partially
// filled one and an older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectStoreOverrides {
    // Route eligible unencrypted RAR `Store` sets straight to their final
    // destinations, so their volumes never exist as files.
    //
    // **Defaults to off.** Turning the default on is a release decision, not a
    // config default change.
    pub enabled: Option<bool>,
    // Per-set ceiling on the holds scratch file, in bytes. Decoded bytes whose
    // destination is not yet known are held in RAM and paged here on a breach;
    // breaching *this* ceiling demotes that one set. Defaults to 1 GiB.
    pub holds_scratch_ceiling_bytes: Option<u64>,
    // Process-wide ceiling on RAM-resident holds across every set, in bytes.
    // Over it, the set that is routing pages its holds to scratch. Defaults
    // to a sixteenth of the memory the process can use, between 64 MiB and
    // 1 GiB.
    pub holds_resident_limit_bytes: Option<u64>,
    // Process-wide ceiling on holds scratch across every set, in bytes. A
    // spill that would exceed it demotes the set that asked. Defaults to four
    // times the per-set ceiling.
    pub holds_scratch_total_bytes: Option<u64>,
    // Free space the working directory's filesystem must keep, in bytes. A
    // spill that would leave less demotes the set that asked. Defaults to a
    // twentieth of the filesystem, between 512 MiB and 20 GiB; zero disables
    // the check.
    pub holds_disk_reserve_bytes: Option<u64>,
}

// Operator-facing switches for 7z and ZIP/ZIP64 direct unpack (`[direct_unpack]`).
//
// Same precedence as `[direct_store]` — **environment over config over
// default** — resolved in `pipeline::direct_unpack::DirectUnpackSettings::resolve`;
// see that type for the variable name.
//
// Every field is optional so an absent `[direct_unpack]` table, a partially
// filled one and an older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectUnpackOverrides {
    // Begin extracting a 7z or ZIP set while its parts are still downloading, instead
    // of waiting for the whole set to land.
    //
    // **Defaults to on.** Set to `false` here, or export the environment
    // override, to fall back to extracting after the whole set has landed.
    pub enabled: Option<bool>,
}

// Operator-facing switches for how a finished job names what it delivers
// (`[delivery_naming]`).
//
// Every field is optional so an absent table, a partially filled one and an
// older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeliveryNamingOverrides {
    // Rename a delivered member that still wears an obfuscated name to the
    // job's own name. **Defaults to on.**
    pub deobfuscate_delivered_members: Option<bool>,
    // Before falling back to the job name, ask the public srrdb release index
    // what release the member's CRC32 belongs to.
    //
    // **Defaults to off.** Completion is otherwise entirely local, so anything
    // that reaches outside the operator's network is opt-in.
    pub enable_srrdb_lookup: Option<bool>,
}

// Prometheus exposition knobs (`[metrics]`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricsConfig {
    // How much per-job detail `/metrics` carries. Per-job series are the
    // exporter's only unbounded label dimension: the runtime keeps up to a
    // thousand finished jobs, and each one would otherwise mint a full set of
    // value series that never goes away.
    #[serde(default)]
    pub per_job_series: PerJobSeries,
}

// Which jobs get their own `weaver_job_*` series.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerJobSeries {
    // Only jobs that are still moving (everything but complete and failed).
    #[default]
    Active,
    // Every job the runtime still remembers, finished ones included.
    All,
    // No per-job series at all; `weaver_pipeline_jobs{status}` still reports
    // the aggregate queue mix.
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

    // Parse a persisted setting value; unknown text falls back to the default
    // so a typo degrades to the safe cardinality rather than failing startup.
    pub fn from_str_or_default(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "all" => Self::All,
            "off" => Self::Off,
            _ => Self::Active,
        }
    }
}

// When a torrent has seeded enough, at one level: the global config, a
// category, or one torrent. A `None` field is unset at this level and
// inherits from the next one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SeedingPolicy {
    pub ratio_goal: Option<f32>,
    pub seed_time_goal_minutes: Option<u32>,
    pub inactive_goal_minutes: Option<u32>,
    pub mode: SeedingGoalMode,
    pub action: SeedingAction,
    pub max_upload_bps: Option<u64>,
}

// Whether any one goal ends seeding, or only all of them together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedingGoalMode {
    #[default]
    Any,
    All,
}

impl SeedingGoalMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Any => "any",
            Self::All => "all",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "any" => Some(Self::Any),
            "all" => Some(Self::All),
            _ => None,
        }
    }
}

// What happens to a torrent once its goal is met.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedingAction {
    #[default]
    Stop,
    Remove,
    RemoveWithData,
}

impl SeedingAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stop => "stop",
            Self::Remove => "remove",
            Self::RemoveWithData => "remove_with_data",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "stop" => Some(Self::Stop),
            "remove" => Some(Self::Remove),
            "remove_with_data" => Some(Self::RemoveWithData),
            _ => None,
        }
    }
}

// The policy a torrent seeds under: its own override, then its tracker
// rule's, then its category's, then the global one. Optional goals resolve
// field by field, each from the most specific level that sets it. `mode` and
// `action` are always set, so they come from the most specific level present.
// `None` when no level has a policy.
pub fn resolve_seeding_policy(
    torrent: Option<&SeedingPolicy>,
    rule: Option<&SeedingPolicy>,
    category: Option<&SeedingPolicy>,
    global: Option<&SeedingPolicy>,
) -> Option<SeedingPolicy> {
    let levels = || [torrent, rule, category, global].into_iter().flatten();
    let most_specific = levels().next()?;
    Some(SeedingPolicy {
        ratio_goal: levels().find_map(|p| p.ratio_goal),
        seed_time_goal_minutes: levels().find_map(|p| p.seed_time_goal_minutes),
        inactive_goal_minutes: levels().find_map(|p| p.inactive_goal_minutes),
        mode: most_specific.mode,
        action: most_specific.action,
        max_upload_bps: levels().find_map(|p| p.max_upload_bps),
    })
}

// The seeding terms of every torrent that announces to a host a pattern
// names: goals that sit between the torrent's own and its category's, and
// whether the schedule's torrent pause may stop the torrent while they are
// unmet.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackerRule {
    pub id: u32,
    // A host, or `*.` and a host, which names that host and every host
    // under it.
    pub host_pattern: String,
    pub seeding: Option<SeedingPolicy>,
    pub never_pause: bool,
    // Whether the archives of a torrent under this rule are extracted;
    // `None` inherits.
    pub unpack: Option<bool>,
}

impl TrackerRule {
    // The pattern as a rule keeps it: trimmed and lower case, without a
    // trailing dot. An error when it names no host.
    pub fn pattern(pattern: &str) -> Result<String, String> {
        let pattern = pattern.trim().trim_end_matches('.').to_ascii_lowercase();
        let host = pattern.strip_prefix("*.").unwrap_or(&pattern);
        let named = !host.is_empty()
            && host.split('.').all(|label| {
                !label.is_empty()
                    && label
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            });
        if named {
            Ok(pattern)
        } else {
            Err(format!("{pattern} is not a host, nor *. and a host"))
        }
    }

    // How closely the pattern names `host`, or `None` when it does not: a
    // host closer than any wildcard, and a longer wildcard closer than a
    // shorter one.
    fn closeness(&self, host: &str) -> Option<usize> {
        match self.host_pattern.strip_prefix("*.") {
            Some(domain) => (host == domain
                || host
                    .strip_suffix(domain)
                    .is_some_and(|rest| rest.ends_with('.')))
            .then_some(domain.len()),
            None => (host == self.host_pattern).then_some(usize::MAX),
        }
    }
}

// The rule a torrent announcing to `trackers` seeds under: the one whose
// pattern names one of their hosts most closely, the lower id on a tie.
pub fn tracker_rule_for<'r, 't>(
    rules: &'r [TrackerRule],
    trackers: impl IntoIterator<Item = &'t str>,
) -> Option<&'r TrackerRule> {
    let hosts: Vec<String> = trackers
        .into_iter()
        .filter_map(|url| {
            let url = reqwest::Url::parse(url.trim()).ok()?;
            Some(url.host_str()?.trim_end_matches('.').to_ascii_lowercase())
        })
        .collect();
    rules
        .iter()
        .filter_map(|rule| {
            let closeness = hosts.iter().filter_map(|host| rule.closeness(host)).max()?;
            Some((rule, closeness))
        })
        .min_by_key(|(rule, closeness)| (std::cmp::Reverse(*closeness), rule.id))
        .map(|(rule, _)| rule)
}

// The torrent client (`[torrent]`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct TorrentConfig {
    pub enabled: bool,
    // The first route's listen port; each further route listens on the
    // next port its slot names.
    pub base_port: u16,
    // The port announced to trackers, when it differs from the one listened
    // on (a forwarded port).
    pub announce_port: Option<u16>,
    pub upnp: bool,
    pub encryption: TorrentEncryption,
    pub max_active_downloads: u32,
    pub max_peers_total: u32,
    // Where each route's session keeps its DHT state. Absent means under
    // the data directory.
    pub session_dir: Option<PathBuf>,
    // Whether a torrent's archives are extracted, unless its category, its
    // tracker rule or the torrent itself says otherwise.
    pub unpack: bool,
}

impl Default for TorrentConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_port: 6881,
            announce_port: None,
            upnp: true,
            encryption: TorrentEncryption::Prefer,
            max_active_downloads: 3,
            max_peers_total: 500,
            session_dir: None,
            unpack: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentEncryption {
    #[default]
    Prefer,
    Require,
}

impl TorrentEncryption {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prefer => "prefer",
            Self::Require => "require",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "prefer" => Some(Self::Prefer),
            "require" => Some(Self::Require),
            _ => None,
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
                pipelining_depth: None,
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
            bandwidth: Default::default(),
            cleanup_after_extract: None,
            propagation_delay_secs: None,
            watch_folder: WatchFolderConfig::default(),
            duplicate_policy: DuplicatePolicy::default(),
            direct_store: None,
            direct_unpack: None,
            delivery_naming: None,
            hardware_profile: None,
            metrics: Default::default(),
            seeding: None,
            torrent: TorrentConfig::default(),
            config_path: None,
        }
    }

    fn policy(ratio_goal: Option<f32>, seed_time_goal_minutes: Option<u32>) -> SeedingPolicy {
        SeedingPolicy {
            ratio_goal,
            seed_time_goal_minutes,
            ..SeedingPolicy::default()
        }
    }

    #[test]
    fn seeding_policy_resolves_field_by_field() {
        let torrent = SeedingPolicy {
            mode: SeedingGoalMode::All,
            ..policy(Some(2.5), None)
        };
        let category = SeedingPolicy {
            inactive_goal_minutes: Some(30),
            action: SeedingAction::Remove,
            ..policy(Some(1.0), Some(60))
        };
        let global = SeedingPolicy {
            max_upload_bps: Some(1_000),
            action: SeedingAction::RemoveWithData,
            ..policy(Some(0.5), Some(120))
        };

        let resolved =
            resolve_seeding_policy(Some(&torrent), None, Some(&category), Some(&global)).unwrap();

        assert_eq!(resolved.ratio_goal, Some(2.5));
        assert_eq!(resolved.seed_time_goal_minutes, Some(60));
        assert_eq!(resolved.inactive_goal_minutes, Some(30));
        assert_eq!(resolved.max_upload_bps, Some(1_000));
        assert_eq!(resolved.mode, SeedingGoalMode::All);
        assert_eq!(resolved.action, SeedingAction::Stop);

        // A tracker rule sits between the torrent and its category.
        let rule = SeedingPolicy {
            inactive_goal_minutes: Some(45),
            action: SeedingAction::Remove,
            ..policy(None, Some(600))
        };
        let resolved =
            resolve_seeding_policy(None, Some(&rule), Some(&category), Some(&global)).unwrap();
        assert_eq!(resolved.ratio_goal, Some(1.0));
        assert_eq!(resolved.seed_time_goal_minutes, Some(600));
        assert_eq!(resolved.inactive_goal_minutes, Some(45));
        assert_eq!(resolved.max_upload_bps, Some(1_000));
        assert_eq!(resolved.action, SeedingAction::Remove);
        let resolved =
            resolve_seeding_policy(Some(&torrent), Some(&rule), Some(&category), None).unwrap();
        assert_eq!(resolved.ratio_goal, Some(2.5));
        assert_eq!(resolved.seed_time_goal_minutes, Some(600));
        assert_eq!(resolved.action, SeedingAction::Stop);
    }

    fn rule(id: u32, host_pattern: &str) -> TrackerRule {
        TrackerRule {
            id,
            host_pattern: TrackerRule::pattern(host_pattern).unwrap(),
            seeding: None,
            never_pause: false,
            unpack: None,
        }
    }

    #[test]
    fn a_tracker_rule_names_a_host_or_every_host_under_one() {
        assert_eq!(
            TrackerRule::pattern(" *.Example.ORG. ").as_deref(),
            Ok("*.example.org")
        );
        for bad in [
            "",
            "*.",
            "http://example.org",
            "example.org/announce",
            "a..b",
            "*",
        ] {
            assert!(TrackerRule::pattern(bad).is_err(), "{bad}");
        }
        let rules = [
            rule(1, "*.example.org"),
            rule(2, "tracker.example.org"),
            rule(3, "*.tracker.example.org"),
            rule(4, "other.invalid"),
        ];
        let id = |trackers: &[&str]| {
            tracker_rule_for(&rules, trackers.iter().copied()).map(|rule| rule.id)
        };
        assert_eq!(id(&["https://tracker.example.org/announce"]), Some(2));
        assert_eq!(id(&["udp://Tracker.Example.org.:1337"]), Some(2));
        assert_eq!(id(&["http://a.tracker.example.org/x"]), Some(3));
        assert_eq!(id(&["http://example.org/x"]), Some(1));
        assert_eq!(id(&["http://cdn.example.org/x"]), Some(1));
        assert_eq!(id(&["http://notexample.org/x"]), None);
        assert_eq!(id(&["http://public.invalid/x", "not a url"]), None);
        // The closest pattern over every tracker wins, wherever it is listed.
        assert_eq!(
            id(&["http://cdn.example.org/x", "http://other.invalid/x"]),
            Some(4)
        );
        assert_eq!(id(&[]), None);
        // On a tie, the lower id.
        let tied = [rule(9, "*.example.org"), rule(5, "*.example.org")];
        assert_eq!(
            tracker_rule_for(&tied, ["http://a.example.org/"]).map(|rule| rule.id),
            Some(5)
        );
    }

    #[test]
    fn seeding_policy_skips_absent_levels() {
        let global = SeedingPolicy {
            action: SeedingAction::Remove,
            ..policy(None, Some(90))
        };

        assert_eq!(
            resolve_seeding_policy(None, None, None, Some(&global)),
            Some(global)
        );
        let category = policy(Some(1.5), None);
        let resolved = resolve_seeding_policy(None, None, Some(&category), Some(&global)).unwrap();
        assert_eq!(resolved.ratio_goal, Some(1.5));
        assert_eq!(resolved.seed_time_goal_minutes, Some(90));
        assert_eq!(resolved.action, SeedingAction::Stop);
        assert_eq!(resolve_seeding_policy(None, None, None, None), None);
    }

    #[test]
    fn a_config_without_torrent_keys_loads_the_defaults_and_round_trips() {
        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[[servers]]
id = 1
host = "news.example.com"
port = 443
tls = true
username = "user"
password = "pass"
connections = 10
active = true

[[categories]]
id = 1
name = "tv"
"#,
        )
        .unwrap();

        assert_eq!(config.seeding, None);
        assert_eq!(config.torrent, TorrentConfig::default());
        assert!(!config.torrent.enabled);
        assert_eq!(config.categories[0].seeding, None);
        assert!(!config.categories[0].torrent_route);

        let reparsed: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();
        assert_eq!(reparsed.seeding, None);
        assert_eq!(reparsed.torrent, TorrentConfig::default());
        assert_eq!(reparsed.categories[0].seeding, None);
        assert!(!reparsed.categories[0].torrent_route);
    }

    #[test]
    fn torrent_and_seeding_tables_round_trip_through_toml() {
        let mut config = config_with_server();
        config.seeding = Some(SeedingPolicy {
            inactive_goal_minutes: Some(15),
            mode: SeedingGoalMode::All,
            action: SeedingAction::RemoveWithData,
            max_upload_bps: Some(65_536),
            ..policy(Some(2.0), Some(1_440))
        });
        config.torrent = TorrentConfig {
            enabled: true,
            base_port: 51_413,
            announce_port: Some(40_000),
            upnp: false,
            encryption: TorrentEncryption::Require,
            max_active_downloads: 5,
            max_peers_total: 200,
            session_dir: Some(PathBuf::from("/var/lib/weaver/torrents")),
            unpack: false,
        };

        let reparsed: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();

        assert_eq!(reparsed.seeding, config.seeding);
        assert_eq!(reparsed.torrent, config.torrent);
    }

    #[test]
    fn a_partial_seeding_table_fills_the_rest_with_defaults() {
        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[seeding]
ratio_goal = 1.0

[torrent]
enabled = true
"#,
        )
        .unwrap();

        assert_eq!(config.seeding, Some(policy(Some(1.0), None)));
        assert_eq!(
            config.torrent,
            TorrentConfig {
                enabled: true,
                ..TorrentConfig::default()
            }
        );
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
