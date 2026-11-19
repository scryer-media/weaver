// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::categories::CategoryConfig;
use crate::jobs::DuplicatePolicy;
use crate::servers::ServerConfig;
pub use crate::settings::levels::{Leaf, Level, Levels};
use crate::torrents::identity::{ClientIdentity, IDENTITY};
use crate::watch_folder::WatchFolderConfig;
use weaver_server_base::runtime::hardware_profile::HardwareProfile;
use weaver_torrent::tracker::qbittorrent;

// The NZB admission limit when none is saved, for new and upgraded installations.
pub const DEFAULT_MAX_CONCURRENT_NZBS: std::num::NonZeroU32 =
    std::num::NonZeroU32::new(10).unwrap();

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
    // NZBs admitted through terminal completion, across every Usenet provider.
    #[serde(default)]
    pub max_concurrent_nzbs: Option<std::num::NonZeroU32>,
    // Whether to delete intermediate files (NZB articles, PAR2, RAR volumes)
    // after successful extraction. Defaults to true.
    #[serde(default)]
    pub cleanup_after_extract: Option<bool>,
    // Whether housekeeping deletes what a failed job left in the intermediate
    // and staging directories a day after it failed. Defaults to true.
    #[serde(default)]
    pub delete_failed_job_files: Option<bool>,
    // How many days a history entry is kept after its job finished, before
    // housekeeping deletes the entry. Its delivered files stay. 0 keeps
    // history forever. Defaults to 90.
    #[serde(default)]
    pub history_retention_days: Option<u32>,
    // Watched-folder NZB intake settings.
    #[serde(default)]
    pub watch_folder: WatchFolderConfig,
    // Duplicate admission handling policy.
    #[serde(default)]
    pub duplicate_policy: DuplicatePolicy,
    // Limits on the bytes the graph engine holds while it waits to place
    // them. Absent means "every default".
    #[serde(default)]
    pub direct_store: Option<DirectStoreOverrides>,
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
    // The upload cap each torrent takes unless its category or tracker rule
    // sets its own; absent is none.
    #[serde(default)]
    pub upload_cap: Option<u64>,
    #[serde(default)]
    pub torrent: TorrentConfig,
    // Path to the config file on disk. Not serialized to TOML.
    #[serde(skip)]
    pub config_path: Option<PathBuf>,
}

// The history retention, in days, when none is configured.
pub const DEFAULT_HISTORY_RETENTION_DAYS: u32 = 90;
// The longest history retention, in days, a setting may ask for.
pub const MAX_HISTORY_RETENTION_DAYS: u32 = 36_500;

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

    // Whether housekeeping deletes a failed job's files. Defaults to `true`
    // when not explicitly configured.
    pub fn delete_failed_job_files(&self) -> bool {
        self.delete_failed_job_files.unwrap_or(true)
    }

    // How many days history is kept. 0 keeps it forever. Defaults to 90 when
    // not explicitly configured.
    pub fn history_retention_days(&self) -> u32 {
        self.history_retention_days
            .unwrap_or(DEFAULT_HISTORY_RETENTION_DAYS)
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

    pub fn max_concurrent_nzbs(&self) -> u32 {
        self.max_concurrent_nzbs
            .unwrap_or(DEFAULT_MAX_CONCURRENT_NZBS)
            .get()
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

        if let Err(torrent) = self.torrent.validate() {
            errors.extend(torrent);
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

// Operator-facing limits on the graph engine's holds: decoded bytes whose
// place in an output is not yet known, kept in RAM and paged to scratch.
//
// Precedence is **environment over config over default**, resolved in
// `pipeline::graph::limits::HoldSettings`; see that type for the exact
// variable names.
//
// Every field is optional so an absent `[direct_store]` table, a partially
// filled one and an older config file all mean "use the defaults".
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DirectStoreOverrides {
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
    // twentieth of the filesystem, between 512 MiB and 5 GiB; zero disables
    // the check.
    pub holds_disk_reserve_bytes: Option<u64>,
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

// Which jobs get their own `weaver_job_*` series, and which torrents their
// own `weaver_torrent_*` ones, their trackers' included.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerJobSeries {
    // Only jobs the pipeline is working on: those holding an NZB slot and
    // those past their download, until they finish. Jobs waiting their turn
    // get none. A torrent has its series only while it downloads (stalled
    // included), checks its files, fetches its metadata or moves; one that
    // seeds, is queued, paused, stopped or otherwise held has none, and is
    // still counted in `weaver_torrents{route,state}` and its route's
    // session series.
    #[default]
    Active,
    // Every job the runtime still remembers, finished ones included, and
    // every torrent.
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

// When a torrent has seeded enough, at one level: the global settings, a
// category, a tracker rule, or one torrent. Each field is a leaf the levels
// resolve one by one (`Levels`): inherited, off, or set at this level. A
// policy whose every leaf inherits is no policy, and is kept as none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SeedingPolicy {
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub ratio_goal: Leaf<f32>,
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub seed_time_goal_minutes: Leaf<u32>,
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub inactive_goal_minutes: Leaf<u32>,
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub mode: Leaf<SeedingGoalMode>,
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub action: Leaf<SeedingAction>,
    #[serde(skip_serializing_if = "Leaf::is_inherit")]
    pub seed_time_counting: Leaf<SeedTimeCounting>,
}

impl SeedingPolicy {
    /// Whether every leaf inherits.
    pub fn inherits(&self) -> bool {
        *self == Self::default()
    }

    /// The policy as a level keeps it: none when every leaf inherits.
    pub fn kept(policy: Option<Self>) -> Option<Self> {
        policy.filter(|policy| !policy.inherits())
    }

    /// The global policy, where there is no level above: an inherited goal
    /// is no goal, and an inherited mode or action is the default one.
    pub fn at_global(self) -> Self {
        Self {
            ratio_goal: self.ratio_goal.at_global(),
            seed_time_goal_minutes: self.seed_time_goal_minutes.at_global(),
            inactive_goal_minutes: self.inactive_goal_minutes.at_global(),
            mode: Leaf::Set(self.mode.set().unwrap_or_default()),
            action: Leaf::Set(self.action.set().unwrap_or_default()),
            seed_time_counting: Leaf::Set(self.seed_time_counting.set().unwrap_or_default()),
        }
    }

    /// A mode, action or seed-time counting is set or inherited; none is
    /// ever off.
    pub fn validate(&self) -> Result<(), String> {
        if matches!(self.mode, Leaf::Off)
            || matches!(self.action, Leaf::Off)
            || matches!(self.seed_time_counting, Leaf::Off)
        {
            return Err(
                "a seeding mode, action and seed-time counting are set or inherited, never off"
                    .to_owned(),
            );
        }
        if let Leaf::Set(ratio) = self.ratio_goal
            && (!ratio.is_finite() || ratio < 0.0)
        {
            return Err("a ratio goal must be zero or more".to_owned());
        }
        Ok(())
    }
}

/// The seeding terms a torrent is held to, each resolved over its levels.
/// A goal that is `None` is not one.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Seeding {
    pub ratio_goal: Option<f32>,
    pub seed_time_goal_minutes: Option<u32>,
    pub inactive_goal_minutes: Option<u32>,
    pub mode: SeedingGoalMode,
    pub action: SeedingAction,
    pub seed_time_counting: SeedTimeCounting,
}

impl Seeding {
    /// The seconds the seed-time goal is measured against: every second
    /// seeded, or those of the current unbroken run.
    pub fn seeded_seconds(&self, counters: &crate::torrents::TorrentCounters) -> u64 {
        match self.seed_time_counting {
            SeedTimeCounting::Cumulative => counters.finished_duration_s,
            SeedTimeCounting::Consecutive => counters.consecutive_seeding_s,
        }
    }
}

// Which seconds the seed-time goal counts: every second the torrent has
// seeded, or only those since it last stopped seeding.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedTimeCounting {
    #[default]
    Cumulative,
    Consecutive,
}

impl SeedTimeCounting {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cumulative => "cumulative",
            Self::Consecutive => "consecutive",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "cumulative" => Some(Self::Cumulative),
            "consecutive" => Some(Self::Consecutive),
            _ => None,
        }
    }
}

// Whether any one goal ends seeding, or only all of them together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

// The seeding terms of every torrent that announces to a host a pattern
// names: goals that sit between the torrent's own and its category's, and
// whether the schedule's torrent pause may stop the torrent while they are
// unmet.
#[derive(Debug, Clone, Default, PartialEq)]
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
    // The client a torrent under this rule names itself as; `None` is
    // Weaver.
    pub client: Option<Client>,
    // The rule's torrents route through `torrent:tracker:<id>` instead of
    // their category's route or `torrent:default`.
    pub torrent_route: bool,
    // Whether its torrents look for peers on the DHT, on the local network
    // and by peer exchange, each on its own as libtorrent's per-torrent
    // `disable_dht`, `disable_lsd` and `disable_pex` flags are; `None`
    // inherits. A private torrent does none of the three whatever these
    // say. All three `Some(false)` holds the torrents to their trackers as
    // a private torrent is held.
    pub dht: Option<bool>,
    pub pex: Option<bool>,
    pub lsd: Option<bool>,
    // The encryption its torrents' connections are made under, over the
    // global one; `None` inherits.
    pub encryption: Option<TorrentEncryption>,
    // The transports its torrents' outgoing connections dial, and the one
    // each tries first while downloading and while seeding, field by field
    // over the global ones. What the session listens on stays global.
    pub transports: TransportRule,
    // The upload cap each of its torrents takes, over its category's and
    // the global one.
    pub upload_cap: Leaf<u64>,
    // Whether a torrent admitted under it starts forced, past the queue.
    pub forced: bool,
    // The identity the tracker issued for its SSL torrents, presented to
    // their peers and in their HTTPS announces; `None` leaves an SSL torrent
    // under the rule waiting for one.
    pub ssl: Option<TrackerCertificate>,
    // The tracker file that filled the rule, while it binds it; `None` once
    // unbound or when none did.
    pub bound: Option<super::tracker_file::Binding>,
    // The passwords its torrents' archives are tried with, before the
    // global list; kept sealed with the database's key.
    pub archive_passwords: ArchivePasswords,
}

// A tracker's archive passwords, in the order they are tried: blanks and
// repeats dropped, the first of a repeat kept, each kept exactly as given.
// The values are secrets: Debug says how many.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ArchivePasswords(Vec<String>);

impl std::fmt::Debug for ArchivePasswords {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ArchivePasswords({})", self.0.len())
    }
}

impl ArchivePasswords {
    // The most a rule holds, by count and by bytes in all.
    pub const MAX: usize = 10;
    pub const MAX_BYTES: usize = 64 * 1024;

    // The list `values` makes; an error past either bound, saying which.
    pub fn new(values: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut kept: Vec<String> = Vec::new();
        for value in values {
            if !value.trim().is_empty() && !kept.contains(&value) {
                kept.push(value);
            }
        }
        if kept.len() > Self::MAX {
            return Err(format!("holds more than {} passwords.", Self::MAX));
        }
        if kept.iter().map(String::len).sum::<usize>() > Self::MAX_BYTES {
            return Err(format!("is larger than {} KiB.", Self::MAX_BYTES / 1024));
        }
        Ok(Self(kept))
    }

    pub fn as_slice(&self) -> &[String] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// An identity a tracker issued: a certificate chain and its private key,
// both PEM.
#[derive(Clone, PartialEq, Eq)]
pub struct TrackerCertificate {
    pub certificate_pem: String,
    pub private_key_pem: String,
}

impl std::fmt::Debug for TrackerCertificate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerCertificate")
            .field("certificate_pem", &self.certificate_pem)
            .finish_non_exhaustive()
    }
}

impl TrackerCertificate {
    // The identity its torrents present; an error when either PEM does not
    // parse or the key is not the certificate's.
    pub fn identity(&self) -> Result<weaver_torrent::SslIdentity, String> {
        let identity = weaver_torrent::SslIdentity::from_pem(
            self.certificate_pem.as_bytes(),
            self.private_key_pem.as_bytes(),
        )
        .map_err(|error| format!("{error:#}"))?;
        identity.check().map_err(|error| format!("{error:#}"))?;
        Ok(identity)
    }

    // The subject and the end of the validity of its first certificate, the
    // one its torrents present.
    pub fn summary(&self) -> Option<CertificateSummary> {
        let identity = weaver_torrent::SslIdentity::from_pem(
            self.certificate_pem.as_bytes(),
            self.private_key_pem.as_bytes(),
        )
        .ok()?;
        let (_, cert) =
            x509_parser::parse_x509_certificate(identity.certificate_chain.first()?).ok()?;
        Some(CertificateSummary {
            subject: cert.subject().to_string(),
            not_after_ms: cert.validity().not_after.timestamp().saturating_mul(1000),
        })
    }
}

// What a certificate says of whom it names and until when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertificateSummary {
    pub subject: String,
    pub not_after_ms: i64,
}

// A client a torrent may name itself as, for the trackers that admit only
// some clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Client {
    Weaver,
    QBittorrent,
}

impl Client {
    pub const ALL: [Self; 2] = [Self::Weaver, Self::QBittorrent];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Weaver => "weaver",
            Self::QBittorrent => "qbittorrent",
        }
    }

    // A kept `transmission` names no client a torrent may go as, and reads
    // as Weaver.
    pub fn parse(value: &str) -> Option<Self> {
        if value == "transmission" {
            return Some(Self::Weaver);
        }
        Self::ALL
            .into_iter()
            .find(|client| client.as_str() == value)
    }

    // Each client as its current stable release names itself.
    pub const fn name(self) -> ClientIdentity {
        match self {
            Self::Weaver => IDENTITY,
            Self::QBittorrent => ClientIdentity::new(
                qbittorrent::PEER_ID_PREFIX,
                qbittorrent::USER_AGENT,
                qbittorrent::VERSION,
                &qbittorrent::SHAPE,
            ),
        }
    }
}

impl TrackerRule {
    // The rule as it is kept, its pattern as `pattern` keeps it; an error
    // when the pattern names no host.
    pub fn checked(mut self) -> Result<Self, String> {
        self.host_pattern = Self::pattern(&self.host_pattern)?;
        if let Some(ssl) = &self.ssl {
            ssl.identity()
                .map_err(|error| format!("the SSL certificate is refused: {error}"))?;
        }
        Ok(self)
    }

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

// The host an announce URL names, as a rule matches it and as it is stored:
// read as an http URL reads it whatever the tracker's scheme, so lower case,
// an international name in punycode and an IPv4 address in dotted decimal,
// without a trailing dot. A udp URL's host is otherwise kept as written.
pub fn tracker_host(url: &str) -> Option<String> {
    let url = reqwest::Url::parse(url.trim()).ok()?;
    let host = reqwest::Url::parse(&format!("http://{}/", url.host_str()?)).ok()?;
    let host = host.host_str()?.trim_end_matches('.');
    (!host.is_empty()).then(|| host.to_owned())
}

// The rule a torrent announcing to `trackers` seeds under: the one whose
// pattern names one of their hosts most closely, the lower id on a tie.
pub fn tracker_rule_for<'r, 't>(
    rules: &'r [TrackerRule],
    trackers: impl IntoIterator<Item = &'t str>,
) -> Option<&'r TrackerRule> {
    let hosts: Vec<String> = trackers.into_iter().filter_map(tracker_host).collect();
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
    // Peer connections each route's session holds at once, over all its
    // torrents (libtorrent `connections_limit`).
    pub connections_limit: TorrentLimit,
    // Peers each route's session uploads to at once (libtorrent
    // `unchoke_slots_limit`).
    pub unchoke_slots_limit: TorrentLimit,
    // Outgoing connects each route's session begins per second (libtorrent
    // `connection_speed`).
    pub connection_speed: NonZeroU32,
    // Whether peers at one address may each hold a connection to a torrent
    // (libtorrent `allow_multiple_connections_per_ip`).
    pub allow_multiple_connections_per_ip: bool,
    // The transports peers are reached and taken on, and the one an
    // outgoing connection tries first while downloading and while seeding.
    pub transports: PeerTransports,
    // The DHT's bootstrap nodes, `host:port` separated by commas, as
    // libtorrent's `dht_bootstrap_nodes` reads them.
    pub dht_bootstrap_nodes: String,
    // Whether the blocklist is applied (qBittorrent `IPFilteringEnabled`).
    pub blocklist_enabled: bool,
    // The list of address ranges no peer is taken from or dialed at: an
    // http, https or file URL, or a path (qBittorrent `IPFilterFile`).
    pub blocklist: Option<String>,
    // How long a `stopped` announce may take; 0 sends none (libtorrent
    // `stop_tracker_timeout`).
    pub stop_tracker_timeout_secs: u32,
    // Addresses no peer is taken from or dialed at, beside the blocklist:
    // each one address or `first - last` (qBittorrent `BannedIPs`).
    pub banned_ips: Vec<String>,
    // Whether peers on ports below 1024 are neither dialed nor taken
    // (libtorrent `no_connect_privileged_ports`).
    pub block_privileged_ports: bool,
    // Whether trackers are held to the blocklist and banned addresses
    // (libtorrent `apply_ip_filter_to_trackers`).
    pub filter_trackers: bool,
    // Whether SSL torrents' peers are taken, on a listener of their own.
    pub ssl_enabled: bool,
    // The first route's SSL listen port; each further route listens on the
    // next port its slot names. Absent, the sessions choose one at random
    // and keep it.
    pub ssl_port: Option<u16>,
}

impl TorrentConfig {
    // The bootstrap nodes as the DHT takes them (libtorrent
    // `session_impl::update_dht_bootstrap_nodes`, `parse_comma_separated_string_port`).
    pub fn dht_bootstrap_nodes(&self) -> Vec<String> {
        self.dht_bootstrap_nodes
            .split(',')
            .map(str::trim)
            .filter(|node| !node.is_empty())
            .map(str::to_owned)
            .collect()
    }

    // The blocklist sessions apply, while it is on and names one.
    pub fn blocklist_source(&self) -> Option<BlocklistSource> {
        let blocklist = self
            .blocklist
            .as_deref()
            .filter(|_| self.blocklist_enabled)?;
        Some(BlocklistSource::of(blocklist))
    }

    // The banned addresses, each from its first to its last.
    pub fn banned_ranges(&self) -> Vec<(IpAddr, IpAddr)> {
        self.banned_ips
            .iter()
            .filter_map(|entry| banned_range(entry))
            .collect()
    }

    // The settings as they are kept, the bootstrap nodes as one list, a
    // blank blocklist as none and each banned entry in one form, sorted and
    // once (qBittorrent `SessionImpl::setBannedIPs`); or what is wrong with
    // them.
    pub fn checked(mut self) -> Result<Self, Vec<String>> {
        let mut banned: Vec<(Option<(IpAddr, IpAddr)>, String)> = self
            .banned_ips
            .iter()
            .map(|entry| entry.trim())
            .filter(|entry| !entry.is_empty())
            .map(|entry| match banned_range(entry) {
                Some(range) => (Some(range), banned_entry(range)),
                None => (None, entry.to_owned()),
            })
            .collect();
        banned.sort();
        banned.dedup();
        self.banned_ips = banned.into_iter().map(|(_, entry)| entry).collect();
        self.dht_bootstrap_nodes = self.dht_bootstrap_nodes().join(",");
        self.blocklist = self
            .blocklist
            .as_deref()
            .map(str::trim)
            .filter(|blocklist| !blocklist.is_empty())
            .map(str::to_owned);
        self.validate()?;
        Ok(self)
    }

    // What is wrong with the session settings, if anything.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();
        if let Err(error) = self.transports.checked() {
            errors.push(error);
        }
        for node in self.dht_bootstrap_nodes() {
            let port = node
                .rsplit_once(':')
                .filter(|(host, _)| !host.is_empty())
                .and_then(|(_, port)| port.parse::<u16>().ok());
            if port.is_none() {
                errors.push(format!(
                    "torrent.dht_bootstrap_nodes: {node} is not host:port"
                ));
            }
        }
        for entry in &self.banned_ips {
            if banned_range(entry).is_none() {
                errors.push(format!(
                    "torrent.banned_ips: {entry} is not an address, a range or a subnet"
                ));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
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
            max_peers_total: 100,
            session_dir: None,
            unpack: true,
            connections_limit: TorrentLimit::Count(NonZeroU32::new(200).unwrap()),
            unchoke_slots_limit: TorrentLimit::Count(NonZeroU32::new(8).unwrap()),
            connection_speed: NonZeroU32::new(30).unwrap(),
            allow_multiple_connections_per_ip: false,
            transports: PeerTransports::default(),
            // The routers the engine bootstraps from when told none.
            dht_bootstrap_nodes: weaver_torrent::dht::DHT_BOOTSTRAP.join(","),
            blocklist_enabled: false,
            blocklist: None,
            stop_tracker_timeout_secs: 5,
            // qBittorrent's defaults: none banned, privileged ports and
            // trackers left alone.
            banned_ips: Vec::new(),
            block_privileged_ports: false,
            filter_trackers: false,
            ssl_enabled: false,
            ssl_port: None,
        }
    }
}

// The addresses a banned entry names, first to last (qBittorrent
// `Utils::Net::parseIPRange`): one address, `first - last` or a subnet, of
// one family, first not after last.
pub fn banned_range(entry: &str) -> Option<(IpAddr, IpAddr)> {
    let entry = entry.trim();
    let (first, last) = if let Some((first, last)) = entry.split_once('-') {
        (first.trim().parse().ok()?, last.trim().parse().ok()?)
    } else if let Some((addr, prefix)) = entry.split_once('/') {
        subnet(addr.trim().parse().ok()?, prefix.trim().parse().ok()?)?
    } else {
        let addr = entry.parse().ok()?;
        (addr, addr)
    };
    let ordered = match (first, last) {
        (IpAddr::V4(first), IpAddr::V4(last)) => first <= last,
        (IpAddr::V6(first), IpAddr::V6(last)) => first <= last,
        _ => false,
    };
    ordered.then_some((first, last))
}

// The first and last address of a subnet (qBittorrent
// `Utils::Net::subnetToIPRange`).
fn subnet(addr: IpAddr, prefix: u32) -> Option<(IpAddr, IpAddr)> {
    match addr {
        IpAddr::V4(addr) if prefix <= u32::BITS => {
            let host = u32::MAX.checked_shr(prefix).unwrap_or(0);
            let first = addr.to_bits() & !host;
            Some((
                Ipv4Addr::from_bits(first).into(),
                Ipv4Addr::from_bits(first | host).into(),
            ))
        }
        IpAddr::V6(addr) if prefix <= u128::BITS => {
            let host = u128::MAX.checked_shr(prefix).unwrap_or(0);
            let first = addr.to_bits() & !host;
            Some((
                Ipv6Addr::from_bits(first).into(),
                Ipv6Addr::from_bits(first | host).into(),
            ))
        }
        _ => None,
    }
}

// A banned range as it is kept (qBittorrent `Utils::Net::ipRangeToString`):
// the one address, or `first - last`.
fn banned_entry((first, last): (IpAddr, IpAddr)) -> String {
    if first == last {
        first.to_string()
    } else {
        format!("{first} - {last}")
    }
}

// A count, or no limit at all, which libtorrent takes as a count of its own
// (`session_impl::update_connections_limit`, `update_unchoke_limit`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentLimit {
    Unlimited,
    Count(NonZeroU32),
}

impl TorrentLimit {
    const UNLIMITED: &'static str = "unlimited";

    pub const fn count(self) -> Option<NonZeroU32> {
        match self {
            Self::Unlimited => None,
            Self::Count(count) => Some(count),
        }
    }

    pub const fn of(count: Option<NonZeroU32>) -> Self {
        match count {
            None => Self::Unlimited,
            Some(count) => Self::Count(count),
        }
    }
}

impl std::fmt::Display for TorrentLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unlimited => f.write_str(Self::UNLIMITED),
            Self::Count(count) => count.fmt(f),
        }
    }
}

impl std::str::FromStr for TorrentLimit {
    type Err = std::num::ParseIntError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value == Self::UNLIMITED {
            return Ok(Self::Unlimited);
        }
        value.parse().map(Self::Count)
    }
}

impl Serialize for TorrentLimit {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Unlimited => serializer.serialize_str(Self::UNLIMITED),
            Self::Count(count) => serializer.serialize_u32(count.get()),
        }
    }
}

impl<'de> Deserialize<'de> for TorrentLimit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Kept {
            Count(NonZeroU32),
            Word(String),
        }
        match Kept::deserialize(deserializer)? {
            Kept::Count(count) => Ok(Self::Count(count)),
            Kept::Word(word) => word.parse().map_err(serde::de::Error::custom),
        }
    }
}

// Where a blocklist is read from. An http, https or file URL is one; anything
// else, a Windows path among them, is a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlocklistSource {
    Http(reqwest::Url),
    File(PathBuf),
}

impl BlocklistSource {
    pub fn of(blocklist: &str) -> Self {
        match reqwest::Url::parse(blocklist) {
            Ok(url) if matches!(url.scheme(), "http" | "https") => Self::Http(url),
            Ok(url) if url.scheme() == "file" => {
                Self::File(url.to_file_path().unwrap_or_else(|()| blocklist.into()))
            }
            _ => Self::File(blocklist.into()),
        }
    }
}

// One transport peers are reached and taken on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerTransport {
    Tcp,
    Utp,
}

impl PeerTransport {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tcp => "tcp",
            Self::Utp => "utp",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "tcp" => Some(Self::Tcp),
            "utp" => Some(Self::Utp),
            _ => None,
        }
    }
}

// The transports peers are reached and taken on, as qBittorrent's three
// protocol modes set libtorrent's `enable_incoming_tcp`,
// `enable_outgoing_tcp`, `enable_incoming_utp` and `enable_outgoing_utp`: a
// transport turned off is neither listened on nor dialed. With both on, an
// outgoing connection tries the preferred one first: TCP first dials TCP
// and races uTP behind it; uTP first dials uTP alone and TCP once that
// fails, as libtorrent's `torrent::connect_to_peer` does. A torrent that
// holds every piece it wants dials with the seeding preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct PeerTransports {
    pub tcp: bool,
    pub utp: bool,
    pub prefer_downloading: PeerTransport,
    pub prefer_seeding: PeerTransport,
}

impl Default for PeerTransports {
    fn default() -> Self {
        Self {
            tcp: true,
            utp: true,
            prefer_downloading: PeerTransport::Tcp,
            prefer_seeding: PeerTransport::Utp,
        }
    }
}

impl PeerTransports {
    // The transports as they are kept, preferences as set; an error when
    // both are off.
    pub fn checked(self) -> Result<Self, String> {
        if self.tcp || self.utp {
            Ok(self)
        } else {
            Err("torrent.transports: TCP and uTP cannot both be off".to_owned())
        }
    }

    // The transports as they are used and shown: with one off, both
    // preferences are the one left on. The kept preferences come back when
    // both are on again.
    pub const fn effective(self) -> Self {
        let only = match (self.tcp, self.utp) {
            (true, false) => Some(PeerTransport::Tcp),
            (false, true) => Some(PeerTransport::Utp),
            _ => None,
        };
        match only {
            Some(only) => Self {
                prefer_downloading: only,
                prefer_seeding: only,
                ..self
            },
            None => self,
        }
    }

    // These, each field `rule` sets taken from it.
    pub fn under(self, rule: &TransportRule) -> Self {
        Self {
            tcp: rule.tcp.unwrap_or(self.tcp),
            utp: rule.utp.unwrap_or(self.utp),
            prefer_downloading: rule.prefer_downloading.unwrap_or(self.prefer_downloading),
            prefer_seeding: rule.prefer_seeding.unwrap_or(self.prefer_seeding),
        }
    }

    // The orders an outgoing connection tries the transports in, while its
    // torrent downloads and while it seeds.
    pub const fn dial_orders(self) -> weaver_torrent::DialOrders {
        let effective = self.effective();
        weaver_torrent::DialOrders {
            downloading: self.order(effective.prefer_downloading),
            seeding: self.order(effective.prefer_seeding),
        }
    }

    const fn order(self, prefer: PeerTransport) -> weaver_torrent::DialOrder {
        use weaver_torrent::DialOrder;
        match (self.tcp, self.utp, prefer) {
            (true, true, PeerTransport::Tcp) => DialOrder::TcpFirst,
            (true, true, PeerTransport::Utp) => DialOrder::UtpFirst,
            (true, false, _) => DialOrder::TcpOnly,
            (false, _, _) => DialOrder::UtpOnly,
        }
    }
}

// A tracker rule's transports, each field `None` to take the global one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportRule {
    pub tcp: Option<bool>,
    pub utp: Option<bool>,
    pub prefer_downloading: Option<PeerTransport>,
    pub prefer_seeding: Option<PeerTransport>,
}

impl TransportRule {
    pub const fn is_set(&self) -> bool {
        self.tcp.is_some()
            || self.utp.is_some()
            || self.prefer_downloading.is_some()
            || self.prefer_seeding.is_some()
    }

    // What a torrent under the rule runs with, over `global`. A rule that
    // global settings changed since its save leave with both off is not
    // applied: its torrents take the global transports.
    // The preferences are as kept; `effective` and the dial orders lock
    // them to the transport left on.
    pub fn resolve(&self, global: PeerTransports) -> PeerTransports {
        global.under(self).checked().unwrap_or(global)
    }

    // The rule as it is kept against `global`, preferences as set: an
    // error when it leaves both transports off.
    pub fn checked(self, global: PeerTransports) -> Result<Self, String> {
        global.under(&self).checked().map_err(|_| {
            "transports: TCP and uTP cannot both be off, with the global transports".to_owned()
        })?;
        Ok(self)
    }
}

// Peer connection encryption, as qBittorrent's three modes set libtorrent:
// prefer is `pe_enabled` (each peer tried with the handshake it last
// completed, plain first), require is `pe_forced` (encrypted only, a plain
// handshake refused either way) and disabled is `pe_disabled` (plain only;
// a new peer PEX says was reached encrypted is not dialled).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TorrentEncryption {
    #[default]
    Prefer,
    Require,
    Disabled,
}

impl TorrentEncryption {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Prefer => "prefer",
            Self::Require => "require",
            Self::Disabled => "disabled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "prefer" => Some(Self::Prefer),
            "require" => Some(Self::Require),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }
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
            delete_failed_job_files: None,
            history_retention_days: None,
            propagation_delay_secs: None,
            max_concurrent_nzbs: None,
            watch_folder: WatchFolderConfig::default(),
            duplicate_policy: DuplicatePolicy::default(),
            direct_store: None,
            delivery_naming: None,
            hardware_profile: None,
            metrics: Default::default(),
            seeding: None,
            upload_cap: None,
            torrent: TorrentConfig::default(),
            config_path: None,
        }
    }

    fn policy(ratio_goal: Option<f32>, seed_time_goal_minutes: Option<u32>) -> SeedingPolicy {
        SeedingPolicy {
            ratio_goal: Leaf::of(ratio_goal),
            seed_time_goal_minutes: Leaf::of(seed_time_goal_minutes),
            ..SeedingPolicy::default()
        }
    }

    // A policy kept before its fields were leaves reads as one: a goal it
    // left unset inherits, and its mode and action are set.
    #[test]
    fn a_policy_kept_before_leaves_reads_as_leaves() {
        let kept: SeedingPolicy = serde_json::from_str(
            r#"{"ratio_goal":2.0,"seed_time_goal_minutes":null,"inactive_goal_minutes":null,"mode":"all","action":"remove"}"#,
        )
        .unwrap();
        assert_eq!(
            kept,
            SeedingPolicy {
                ratio_goal: Leaf::Set(2.0),
                mode: Leaf::Set(SeedingGoalMode::All),
                action: Leaf::Set(SeedingAction::Remove),
                ..SeedingPolicy::default()
            }
        );
        let off = SeedingPolicy {
            seed_time_goal_minutes: Leaf::Off,
            ..kept
        };
        let json = serde_json::to_string(&off).unwrap();
        assert_eq!(
            json,
            r#"{"ratio_goal":2.0,"seed_time_goal_minutes":"off","mode":"all","action":"remove"}"#
        );
        assert_eq!(serde_json::from_str::<SeedingPolicy>(&json).unwrap(), off);
        assert_eq!(SeedingPolicy::kept(Some(SeedingPolicy::default())), None);
        assert!(
            SeedingPolicy {
                mode: Leaf::Off,
                ..SeedingPolicy::default()
            }
            .validate()
            .is_err()
        );
    }

    fn rule(id: u32, host_pattern: &str) -> TrackerRule {
        TrackerRule {
            transports: TransportRule::default(),
            archive_passwords: Default::default(),
            bound: None,
            id,
            host_pattern: TrackerRule::pattern(host_pattern).unwrap(),
            seeding: None,
            never_pause: false,
            unpack: None,
            client: None,
            torrent_route: false,
            dht: None,
            pex: None,
            lsd: None,
            encryption: None,
            upload_cap: Leaf::Inherit,
            forced: false,
            ssl: None,
        }
    }

    // A certificate for `subject` until the start of 2031, and its key.
    fn tracker_certificate(subject: &str) -> TrackerCertificate {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, subject);
        params.not_after = rcgen::date_time_ymd(2031, 1, 1);
        TrackerCertificate {
            certificate_pem: params.self_signed(&key).unwrap().pem(),
            private_key_pem: key.serialize_pem(),
        }
    }

    // A rule keeps a certificate whose key is its own, and says whom it
    // names and until when; a PEM that does not parse, or a key of another
    // certificate, is refused.
    #[test]
    fn a_rules_certificate_is_checked_against_its_key() {
        let issued = tracker_certificate("user-1");
        let with = |ssl: TrackerCertificate| {
            TrackerRule {
                ssl: Some(ssl),
                ..rule(0, "tracker.example")
            }
            .checked()
        };
        assert_eq!(with(issued.clone()).unwrap().ssl.as_ref(), Some(&issued));
        let summary = issued.summary().unwrap();
        assert_eq!(summary.subject, "CN=user-1");
        assert_eq!(summary.not_after_ms, 1_924_992_000_000);

        let other = tracker_certificate("user-2");
        let refused = [
            TrackerCertificate {
                private_key_pem: other.private_key_pem.clone(),
                ..issued.clone()
            },
            TrackerCertificate {
                certificate_pem: "not a certificate".to_owned(),
                ..issued.clone()
            },
            TrackerCertificate {
                private_key_pem: "not a key".to_owned(),
                ..issued.clone()
            },
        ];
        for ssl in refused {
            let error = with(ssl).unwrap_err();
            assert!(
                error.starts_with("the SSL certificate is refused"),
                "{error}"
            );
        }
        // The key never shows in what is logged.
        assert!(!format!("{issued:?}").contains(&issued.private_key_pem));
    }

    // Each row's prefix, User-Agent and `v` name one version, encoded as
    // that client encodes it.
    #[test]
    fn each_client_names_one_version_its_own_way() {
        fn version(text: &str, separator: char) -> Vec<u32> {
            let (_, version) = text.rsplit_once(separator).unwrap();
            version
                .split('.')
                .map(|part| part.parse().unwrap())
                .collect()
        }
        // libtorrent: a digit below ten, then `A` onwards.
        fn libtorrent(code: &[u8; 2], parts: &[u32]) -> [u8; 8] {
            let digit = |v: u32| {
                if v < 10 {
                    b'0' + v as u8
                } else {
                    b'A' + (v - 10) as u8
                }
            };
            let mut parts = parts.to_vec();
            parts.resize(4, 0);
            [
                b'-',
                code[0],
                code[1],
                digit(parts[0]),
                digit(parts[1]),
                digit(parts[2]),
                digit(parts[3]),
                b'-',
            ]
        }
        let qb = Client::QBittorrent.name();
        assert_eq!(
            qb.prefix(),
            &libtorrent(b"qB", &version(qb.user_agent(), '/'))
        );
        assert_eq!(qb.prefix(), b"-qB5300-");
        assert_eq!(qb.version(), qb.user_agent());
        assert_eq!(qb.identity().shape, &qbittorrent::SHAPE);
        assert_eq!(&libtorrent(b"qB", &[5, 1, 2]), b"-qB5120-");
        assert_eq!(&libtorrent(b"qB", &[5, 10, 0]), b"-qB5A00-");

        let weaver = Client::Weaver.name();
        assert_eq!(weaver, IDENTITY);
        assert_eq!(weaver.version(), weaver.user_agent());
        assert_eq!(
            weaver.identity().shape,
            &weaver_torrent::tracker::AnnounceShape::WEAVER
        );

        for client in Client::ALL {
            assert_eq!(Client::parse(client.as_str()), Some(client));
        }
        assert_eq!(Client::parse("deluge"), None);
        assert_eq!(Client::parse("transmission"), Some(Client::Weaver));
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

    // A rule's handshake and dial order overrides are keyed by host alone: an
    // announce URL carrying a passkey finds them by its host, and a rule
    // cannot be given the URL, so its passkey is never kept with them.
    #[test]
    fn handshake_and_dial_order_overrides_follow_the_host_never_the_passkey() {
        let passkeyed = "https://Private.Example.ORG/0123456789abcdef/announce?passkey=secret";
        assert!(TrackerRule::pattern(passkeyed).is_err());
        let private = TrackerRule {
            encryption: Some(TorrentEncryption::Disabled),
            transports: TransportRule {
                utp: Some(false),
                ..TransportRule::default()
            },
            ..rule(1, "private.example.org")
        };
        let public = TrackerRule {
            encryption: Some(TorrentEncryption::Require),
            ..rule(2, "*.public.test")
        };
        let rules = [private, public];
        let ruled = tracker_rule_for(&rules, [passkeyed]).unwrap();
        assert_eq!(ruled.host_pattern, "private.example.org");
        assert_eq!(
            (ruled.encryption, ruled.transports.utp),
            (Some(TorrentEncryption::Disabled), Some(false))
        );
        let open = tracker_rule_for(&rules, ["udp://tracker.public.test:1337/announce"]).unwrap();
        assert_eq!(
            (open.encryption, open.transports),
            (Some(TorrentEncryption::Require), TransportRule::default())
        );
        assert!(!format!("{rules:?}").contains("secret"));
    }

    fn transports(
        tcp: bool,
        utp: bool,
        downloading: PeerTransport,
        seeding: PeerTransport,
    ) -> PeerTransports {
        PeerTransports {
            tcp,
            utp,
            prefer_downloading: downloading,
            prefer_seeding: seeding,
        }
    }

    // Each combination of the switches and preferences gives the orders
    // libtorrent's `enable_outgoing_*` and `torrent::connect_to_peer` would:
    // with both on the preferred one first, with one on that one alone.
    #[test]
    fn each_combination_of_transports_gives_its_dial_orders() {
        use PeerTransport::{Tcp, Utp};
        use weaver_torrent::DialOrder::{TcpFirst, TcpOnly, UtpFirst, UtpOnly};
        let orders = |downloading, seeding| weaver_torrent::DialOrders {
            downloading,
            seeding,
        };
        for (set, expected) in [
            (transports(true, true, Tcp, Tcp), orders(TcpFirst, TcpFirst)),
            (transports(true, true, Tcp, Utp), orders(TcpFirst, UtpFirst)),
            (transports(true, true, Utp, Tcp), orders(UtpFirst, TcpFirst)),
            (transports(true, true, Utp, Utp), orders(UtpFirst, UtpFirst)),
            (transports(true, false, Utp, Utp), orders(TcpOnly, TcpOnly)),
            (transports(false, true, Tcp, Tcp), orders(UtpOnly, UtpOnly)),
        ] {
            assert_eq!(set.checked().unwrap().dial_orders(), expected, "{set:?}");
        }
    }

    // A save keeps both preferences as set whatever the switches say, and
    // refuses both off; with one off, the effective preferences are the one
    // left on, and the kept ones come back when both are on again.
    #[test]
    fn a_save_keeps_the_preferences_and_refuses_both_off() {
        use PeerTransport::{Tcp, Utp};
        for set in [
            transports(true, false, Utp, Utp),
            transports(false, true, Tcp, Utp),
            transports(true, true, Utp, Tcp),
        ] {
            assert_eq!(set.checked(), Ok(set));
        }
        assert!(transports(false, false, Tcp, Utp).checked().is_err());
        assert_eq!(
            transports(true, false, Utp, Utp).effective(),
            transports(true, false, Tcp, Tcp)
        );
        assert_eq!(
            transports(false, true, Tcp, Utp).effective(),
            transports(false, true, Utp, Utp)
        );
        assert_eq!(
            transports(true, true, Utp, Tcp).effective(),
            transports(true, true, Utp, Tcp)
        );

        let saved = TorrentConfig {
            transports: transports(false, true, Tcp, Tcp),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(saved.transports, transports(false, true, Tcp, Tcp));
        assert_eq!(
            saved.transports.dial_orders(),
            weaver_torrent::DialOrders::both(weaver_torrent::DialOrder::UtpOnly)
        );
        let again = PeerTransports {
            tcp: true,
            ..saved.transports
        };
        assert_eq!(
            again.dial_orders(),
            weaver_torrent::DialOrders::both(weaver_torrent::DialOrder::TcpFirst)
        );
        let refused = TorrentConfig {
            transports: transports(false, false, Tcp, Utp),
            ..TorrentConfig::default()
        };
        assert_eq!(
            refused.clone().checked(),
            Err(vec![
                "torrent.transports: TCP and uTP cannot both be off".to_owned()
            ])
        );
        assert!(refused.validate().is_err());
    }

    // A rule's fields each override the global one, so a rule may broaden
    // or narrow the global transports; its save keeps its preferences and
    // is refused when both end off over the global ones, and a rule a later
    // global change leaves with both off is not applied.
    #[test]
    fn a_rules_transports_broaden_or_narrow_the_global_ones() {
        use PeerTransport::{Tcp, Utp};
        use weaver_torrent::DialOrder::{TcpFirst, TcpOnly, UtpFirst, UtpOnly};
        let utp_only = transports(false, true, Utp, Utp);
        let tcp_only = transports(true, false, Tcp, Tcp);
        let both = PeerTransports::default();
        let rule = |tcp, utp, downloading, seeding| TransportRule {
            tcp,
            utp,
            prefer_downloading: downloading,
            prefer_seeding: seeding,
        };
        // Broadened: TCP on over uTP alone, TCP first while downloading.
        let tcp_first = rule(Some(true), None, Some(Tcp), None);
        let resolved = tcp_first.resolve(utp_only);
        assert_eq!(
            (
                resolved.dial_orders().downloading,
                resolved.dial_orders().seeding
            ),
            (TcpFirst, UtpFirst)
        );
        // Broadened: uTP on over TCP alone.
        let resolved = rule(None, Some(true), None, Some(Utp)).resolve(tcp_only);
        assert_eq!(
            (
                resolved.dial_orders().downloading,
                resolved.dial_orders().seeding
            ),
            (TcpFirst, UtpFirst)
        );
        // Narrowed: uTP alone, or TCP alone, over both.
        let utp_alone = rule(Some(false), None, None, None);
        assert_eq!(
            utp_alone.resolve(both).dial_orders(),
            weaver_torrent::DialOrders::both(UtpOnly)
        );
        assert_eq!(
            rule(None, Some(false), None, None)
                .resolve(both)
                .dial_orders(),
            weaver_torrent::DialOrders::both(TcpOnly)
        );
        // Each preference on its own.
        assert_eq!(
            rule(None, None, Some(Utp), Some(Tcp))
                .resolve(both)
                .dial_orders(),
            weaver_torrent::DialOrders {
                downloading: UtpFirst,
                seeding: TcpFirst,
            }
        );

        // Saved: a set preference for a transport left off is kept, and
        // the effective one is the transport left on.
        let kept = rule(Some(false), None, Some(Tcp), None);
        assert_eq!(kept.checked(both), Ok(kept));
        assert_eq!(kept.resolve(both).prefer_downloading, Tcp);
        assert_eq!(kept.resolve(both).effective().prefer_downloading, Utp);
        let kept = rule(None, None, Some(Utp), Some(Utp));
        assert_eq!(kept.checked(tcp_only), Ok(kept));
        assert_eq!(
            kept.resolve(tcp_only).dial_orders(),
            weaver_torrent::DialOrders::both(TcpOnly)
        );
        // Refused when both end off, on its own or over the global ones.
        assert!(
            rule(Some(false), Some(false), None, None)
                .checked(both)
                .is_err()
        );
        assert!(utp_alone.checked(tcp_only).is_err());
        // Saved over both, then left with both off by a global change: the
        // torrents take the global transports.
        assert_eq!(utp_alone.resolve(tcp_only), tcp_only);
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
        assert_eq!(config.torrent.max_peers_total, 100);
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
            inactive_goal_minutes: Leaf::Off,
            mode: Leaf::Set(SeedingGoalMode::All),
            action: Leaf::Set(SeedingAction::RemoveWithData),
            ..policy(Some(2.0), Some(1_440))
        });
        config.upload_cap = Some(65_536);
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
            connections_limit: TorrentLimit::Unlimited,
            unchoke_slots_limit: TorrentLimit::Count(NonZeroU32::new(20).unwrap()),
            connection_speed: NonZeroU32::new(50).unwrap(),
            allow_multiple_connections_per_ip: true,
            transports: PeerTransports {
                tcp: true,
                utp: false,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Tcp,
            },
            dht_bootstrap_nodes: "router.example:6881".to_owned(),
            blocklist_enabled: true,
            blocklist: Some("https://lists.example/level1.p2p".to_owned()),
            stop_tracker_timeout_secs: 0,
            banned_ips: vec!["192.0.2.7".to_owned(), "10.0.0.0 - 10.0.0.255".to_owned()],
            block_privileged_ports: true,
            filter_trackers: true,
            ssl_enabled: true,
            ssl_port: Some(4_433),
        };

        let reparsed: Config = toml::from_str(&toml::to_string_pretty(&config).unwrap()).unwrap();

        assert_eq!(reparsed.seeding, config.seeding);
        assert_eq!(reparsed.upload_cap, config.upload_cap);
        assert_eq!(reparsed.torrent, config.torrent);
    }

    #[test]
    fn torrent_session_settings_default_to_libtorrents() {
        // libtorrent settings_pack.cpp: TCP first, uTP raced behind it.
        let count = |count| TorrentLimit::Count(NonZeroU32::new(count).unwrap());
        let torrent = TorrentConfig::default();
        assert_eq!(torrent.connections_limit, count(200));
        assert_eq!(torrent.unchoke_slots_limit, count(8));
        assert_eq!(torrent.connection_speed.get(), 30);
        assert!(!torrent.allow_multiple_connections_per_ip);
        // Both transports on, TCP first while downloading and uTP first
        // while seeding.
        assert_eq!(
            torrent.transports,
            PeerTransports {
                tcp: true,
                utp: true,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Utp,
            }
        );
        assert_eq!(
            torrent.transports.dial_orders(),
            weaver_torrent::DialOrders {
                downloading: weaver_torrent::DialOrder::TcpFirst,
                seeding: weaver_torrent::DialOrder::UtpFirst,
            }
        );
        // The engine's own routers, as before the setting.
        assert_eq!(
            torrent.dht_bootstrap_nodes,
            "dht.transmissionbt.com:6881,dht.libtorrent.org:25401"
        );
        assert!(!torrent.blocklist_enabled);
        assert_eq!(torrent.blocklist, None);
        assert_eq!(torrent.stop_tracker_timeout_secs, 5);
        // qBittorrent's: none banned, privileged ports and trackers left
        // alone.
        assert!(torrent.banned_ips.is_empty());
        assert!(!torrent.block_privileged_ports);
        assert!(!torrent.filter_trackers);
        assert_eq!(torrent.validate(), Ok(()));

        let config: Config = toml::from_str(
            r#"
data_dir = "/tmp/weaver-data"

[torrent]
enabled = true
connections_limit = 80
unchoke_slots_limit = "unlimited"

[torrent.transports]
tcp = false
prefer_seeding = "tcp"
"#,
        )
        .unwrap();
        assert_eq!(config.torrent.connections_limit, count(80));
        // A key left out is its default.
        assert_eq!(
            config.torrent.transports,
            PeerTransports {
                tcp: false,
                utp: true,
                prefer_downloading: PeerTransport::Tcp,
                prefer_seeding: PeerTransport::Tcp,
            }
        );
        assert_eq!(config.torrent.unchoke_slots_limit, TorrentLimit::Unlimited);
        assert_eq!(config.torrent.connection_speed.get(), 30);
        assert_eq!(config.torrent.stop_tracker_timeout_secs, 5);

        // A count is never zero.
        let zero = toml::from_str::<Config>(
            r#"
data_dir = "/tmp/weaver-data"

[torrent]
connections_limit = 0
"#,
        );
        assert!(zero.is_err());
    }

    #[test]
    fn a_blocklist_is_a_url_or_a_path_and_applies_while_on() {
        let source = |blocklist: &str| BlocklistSource::of(blocklist);
        assert!(matches!(
            source("https://lists.example/level1.p2p"),
            BlocklistSource::Http(_)
        ));
        assert_eq!(
            source("file:///var/lib/weaver/level1.p2p"),
            BlocklistSource::File("/var/lib/weaver/level1.p2p".into())
        );
        // Every other scheme is a path, a Windows drive letter among them.
        for path in [
            r"C:\lists\level1.p2p",
            "/var/lib/weaver/level1.p2p",
            "lists/level1.p2p",
        ] {
            assert_eq!(source(path), BlocklistSource::File(path.into()), "{path}");
        }

        let named = TorrentConfig {
            blocklist: Some("/var/lib/weaver/level1.p2p".to_owned()),
            ..TorrentConfig::default()
        };
        assert_eq!(named.blocklist_source(), None, "off");
        let on = TorrentConfig {
            blocklist_enabled: true,
            ..named
        };
        assert_eq!(
            on.blocklist_source(),
            Some(BlocklistSource::File("/var/lib/weaver/level1.p2p".into()))
        );
        let unnamed = TorrentConfig {
            blocklist: None,
            ..on
        };
        assert_eq!(unnamed.blocklist_source(), None, "on, naming none");
    }

    #[test]
    fn checked_settings_keep_one_bootstrap_list_and_no_blank_blocklist() {
        let checked = TorrentConfig {
            dht_bootstrap_nodes: " a.example:6881 ,, b.example:25401 ".to_owned(),
            blocklist: Some("  ".to_owned()),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            checked.dht_bootstrap_nodes,
            "a.example:6881,b.example:25401"
        );
        assert_eq!(checked.blocklist, None);

        let trimmed = TorrentConfig {
            blocklist: Some(" /var/lib/weaver/level1.p2p ".to_owned()),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            trimmed.blocklist.as_deref(),
            Some("/var/lib/weaver/level1.p2p")
        );
    }

    // qBittorrent `SessionImpl::setBannedIPs`: each entry an address, a
    // range or a subnet, kept in one form, sorted and once.
    #[test]
    fn banned_entries_are_kept_in_one_form_sorted_and_once() {
        let checked = TorrentConfig {
            banned_ips: [
                " 192.0.2.7 ",
                "",
                "2001:0db8:0000::1",
                "10.0.0.0/24",
                "10.0.0.0-10.0.0.255",
                "192.0.2.7",
                "2001:db8::/120",
                "172.16.0.9 - 172.16.0.9",
            ]
            .map(str::to_owned)
            .to_vec(),
            ..TorrentConfig::default()
        }
        .checked()
        .unwrap();
        assert_eq!(
            checked.banned_ips,
            [
                "10.0.0.0 - 10.0.0.255",
                "172.16.0.9",
                "192.0.2.7",
                "2001:db8:: - 2001:db8::ff",
                "2001:db8::1",
            ]
        );
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        assert_eq!(
            checked.banned_ranges()[0],
            (ip("10.0.0.0"), ip("10.0.0.255"))
        );
        assert_eq!(
            banned_range("0.0.0.0/0"),
            Some((ip("0.0.0.0"), ip("255.255.255.255")))
        );
        assert_eq!(banned_range("::1/128"), Some((ip("::1"), ip("::1"))));

        for refused in [
            "192.0.2.300",
            "10.0.0.9 - 10.0.0.1",
            "10.0.0.1 - ::1",
            "10.0.0.0/33",
            "10.0.0.1 - 10.0.0.2 - 10.0.0.3",
            "example.org",
        ] {
            let errors = TorrentConfig {
                banned_ips: vec![refused.to_owned()],
                ..TorrentConfig::default()
            }
            .checked()
            .unwrap_err();
            assert_eq!(errors.len(), 1, "{refused}");
        }
    }

    #[test]
    fn torrent_session_settings_out_of_range_are_refused() {
        let refused = |torrent: TorrentConfig| torrent.validate().unwrap_err();
        for torrent in [
            TorrentConfig {
                dht_bootstrap_nodes: "dht.libtorrent.org".to_owned(),
                ..TorrentConfig::default()
            },
            TorrentConfig {
                dht_bootstrap_nodes: "a.example:6881,:6881".to_owned(),
                ..TorrentConfig::default()
            },
            TorrentConfig {
                dht_bootstrap_nodes: "a.example:70000".to_owned(),
                ..TorrentConfig::default()
            },
        ] {
            assert_eq!(refused(torrent.clone()).len(), 1, "{torrent:?}");
        }

        let mut config = config_with_server();
        config.torrent.dht_bootstrap_nodes = "dht.libtorrent.org".to_owned();
        assert!(config.validate().is_err());

        // No bootstrap nodes, no stopped announces, no limits and a
        // blocklist path are all settings libtorrent takes.
        let accepted = TorrentConfig {
            dht_bootstrap_nodes: String::new(),
            stop_tracker_timeout_secs: 0,
            connections_limit: TorrentLimit::Unlimited,
            unchoke_slots_limit: TorrentLimit::Unlimited,
            blocklist_enabled: true,
            blocklist: Some("lists/level1.p2p".to_owned()),
            ..TorrentConfig::default()
        };
        assert_eq!(accepted.validate(), Ok(()));
        assert!(accepted.dht_bootstrap_nodes().is_empty());
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
