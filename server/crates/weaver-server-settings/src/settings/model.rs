// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod config;
pub use crate::settings::levels::{Leaf, Level, Levels};
use crate::torrents::identity::{ClientIdentity, IDENTITY};
pub use config::*;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::num::NonZeroU32;
use std::path::PathBuf;
use weaver_torrent::tracker::qbittorrent;
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
    pub ratio_goal: Leaf<f64>,
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

    /// Whether it sets a ratio, seed-time or inactive goal of its own.
    pub fn sets_a_goal(&self) -> bool {
        self.ratio_goal.set().is_some()
            || self.seed_time_goal_minutes.set().is_some()
            || self.inactive_goal_minutes.set().is_some()
    }

    /// Its own goal mode: a mode is never inherited, and one kept as
    /// inherited reads as Any.
    pub fn own_mode(&self) -> SeedingGoalMode {
        self.mode.set().unwrap_or_default()
    }

    /// The goal mode in force for a torrent, from the policy at each of its
    /// levels, most specific first (torrent, tracker rule, category,
    /// global): the own mode of the first level that sets a goal value. A
    /// level that sets no goal gives no mode; with a goal at none, Any at
    /// the global level.
    pub fn mode_in_force(levels: [(Level, Option<Self>); 4]) -> (SeedingGoalMode, Level) {
        levels
            .into_iter()
            .find_map(|(level, policy)| {
                policy
                    .filter(Self::sets_a_goal)
                    .map(|policy| (policy.own_mode(), level))
            })
            .unwrap_or((SeedingGoalMode::default(), Level::Global))
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
    pub ratio_goal: Option<f64>,
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

// The seeding terms of every torrent that announces to a host one of its
// patterns names: goals that sit between the torrent's own and its
// category's, and whether the schedule's torrent pause may stop the torrent
// while they are unmet.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TrackerRule {
    pub id: u32,
    // What the operator calls the tracker; unique whatever its case.
    pub name: String,
    // The hosts it covers, each a host, or `*.` and a domain, which names
    // that domain and every host under it. A pattern belongs to one rule.
    pub host_patterns: Vec<String>,
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
    // The most patterns a rule holds, the longest a pattern is, and the
    // longest a name is, in characters.
    pub const MAX_PATTERNS: usize = 64;
    pub const MAX_PATTERN_BYTES: usize = 253;
    pub const MAX_NAME: usize = 64;

    // The rule as it is kept: its name trimmed, its patterns as `pattern`
    // keeps them, repeats dropped and the first kept. An error for a name
    // that is blank, too long or holds a control character, for a pattern
    // that names no host, and for none or too many patterns.
    pub fn checked(mut self) -> Result<Self, String> {
        self.name = Self::name(&self.name)?;
        let mut patterns: Vec<String> = Vec::with_capacity(self.host_patterns.len());
        for pattern in &self.host_patterns {
            let pattern = Self::pattern(pattern)?;
            if !patterns.contains(&pattern) {
                patterns.push(pattern);
            }
        }
        if patterns.is_empty() {
            return Err("a tracker rule needs a host".to_owned());
        }
        if patterns.len() > Self::MAX_PATTERNS {
            return Err(format!(
                "a tracker rule names {} hosts at most",
                Self::MAX_PATTERNS
            ));
        }
        self.host_patterns = patterns;
        if let Some(ssl) = &self.ssl {
            ssl.identity()
                .map_err(|error| format!("the SSL certificate is refused: {error}"))?;
        }
        Ok(self)
    }

    // The name as a rule keeps it: trimmed. An error when it is blank, longer
    // than `MAX_NAME` characters or holds a control character.
    pub fn name(name: &str) -> Result<String, String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("a tracker rule needs a name".to_owned());
        }
        if name.chars().count() > Self::MAX_NAME {
            return Err(format!(
                "a tracker rule's name is {} characters at most",
                Self::MAX_NAME
            ));
        }
        if name.chars().any(char::is_control) {
            return Err("a tracker rule's name holds a control character".to_owned());
        }
        Ok(name.to_owned())
    }

    // The key two names collide under.
    pub fn name_key(name: &str) -> String {
        name.trim().to_lowercase()
    }

    // The pattern as a rule keeps it: trimmed, without a trailing dot or a
    // port, and the host as an announce URL's reads (lower case, an
    // international name in punycode). An error when it names no host, when
    // it is an IPv6 address, or when it puts `*.` before an address, a
    // single label or a public suffix, which would cover hosts of many
    // owners.
    pub fn pattern(pattern: &str) -> Result<String, String> {
        let typed = pattern.trim();
        let refused = || format!("{typed} is not a host, nor *. and a domain");
        let (wildcard, host) = match typed.strip_prefix("*.") {
            Some(host) => (true, host),
            None => (false, typed),
        };
        if host.contains('[') || host.matches(':').count() > 1 {
            return Err(format!(
                "{typed} is an IPv6 address; name the tracker's host"
            ));
        }
        let host = match host.rsplit_once(':') {
            Some((host, port)) if port.chars().all(|c| c.is_ascii_digit()) => host,
            Some(_) => return Err(refused()),
            None => host,
        };
        let host = host.trim_end_matches('.');
        if host.is_empty() || host.len() > Self::MAX_PATTERN_BYTES {
            return Err(refused());
        }
        let host = match url::Host::parse(host).map_err(|_| refused())? {
            url::Host::Domain(domain) => domain,
            url::Host::Ipv4(address) if !wildcard => return Ok(address.to_string()),
            url::Host::Ipv4(_) => {
                return Err(format!(
                    "{typed} puts *. before an address; name the address"
                ));
            }
            url::Host::Ipv6(_) => {
                return Err(format!(
                    "{typed} is an IPv6 address; name the tracker's host"
                ));
            }
        };
        let named = host.split('.').all(|label| {
            !label.is_empty()
                && label
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        });
        if !named || host.len() > Self::MAX_PATTERN_BYTES {
            return Err(refused());
        }
        if wildcard && shared_suffix(&host) {
            return Err(format!(
                "{typed} covers every site under {host}; name the tracker's own domain"
            ));
        }
        Ok(if wildcard { format!("*.{host}") } else { host })
    }

    // The tracker host the rule shows itself by: its first pattern, without
    // `*.`.
    pub fn first_host(&self) -> Option<&str> {
        self.host_patterns
            .first()
            .map(|pattern| pattern.strip_prefix("*.").unwrap_or(pattern))
    }
}

// Whether a domain is shared by sites of many owners, so `*.` before it
// would cover them all: a single label (`com`, `localhost`) or a suffix on
// the public suffix list, its ICANN and private parts alike (`co.uk`,
// `github.io`), or one whose every child is (`kawasaki.jp`, under the list's
// `*.kawasaki.jp`). `domain` is lower case and in punycode, as the list is.
fn shared_suffix(domain: &str) -> bool {
    let suffix = |name: &str| {
        psl::suffix(name.as_bytes()).is_some_and(|suffix| suffix.as_bytes() == name.as_bytes())
    };
    !domain.contains('.') || suffix(domain) || suffix(&format!("wvr-probe.{domain}"))
}

// How closely `pattern` names `host`, or `None` when it does not: a host
// closer than any wildcard, and a longer wildcard closer than a shorter one.
// The linear reading `TrackerRules` answers by lookup.
#[cfg(test)]
pub(crate) fn closeness(pattern: &str, host: &str) -> Option<usize> {
    match pattern.strip_prefix("*.") {
        Some(domain) => (host == domain
            || host
                .strip_suffix(domain)
                .is_some_and(|rest| rest.ends_with('.')))
        .then_some(domain.len()),
        None => (host == pattern).then_some(usize::MAX),
    }
}

// The tracker rules, with each pattern looked up by the hosts it can name.
#[derive(Debug, Clone, Default)]
pub struct TrackerRules {
    list: Vec<TrackerRule>,
    at: std::collections::HashMap<u32, usize>,
    by_pattern: std::collections::HashMap<String, usize>,
}

// The rule a torrent follows, and the pattern of it that named the
// torrent's host most closely.
#[derive(Debug, Clone, Copy)]
pub struct RuleMatch<'r> {
    pub rule: &'r TrackerRule,
    pub pattern: &'r str,
}

impl TrackerRules {
    pub fn new(list: Vec<TrackerRule>) -> Self {
        let mut at = std::collections::HashMap::with_capacity(list.len());
        let mut by_pattern = std::collections::HashMap::new();
        for (place, rule) in list.iter().enumerate() {
            at.entry(rule.id).or_insert(place);
        }
        for (place, rule) in list.iter().enumerate() {
            for pattern in &rule.host_patterns {
                by_pattern
                    .entry(pattern.clone())
                    .and_modify(|held: &mut usize| {
                        if list[*held].id > rule.id {
                            *held = place;
                        }
                    })
                    .or_insert(place);
            }
        }
        Self {
            list,
            at,
            by_pattern,
        }
    }

    /// The rule of id `id`, while it stands.
    pub fn of(&self, id: Option<u32>) -> Option<&TrackerRule> {
        self.list.get(*self.at.get(&id?)?)
    }

    // The rule a torrent announcing to `trackers` follows: the one with the
    // pattern that names one of their hosts most closely, the lower id on a
    // tie. Each host is looked up as itself and under each of its domains,
    // so the cost is the hosts' labels, not the rules.
    pub fn matched<'t>(
        &self,
        trackers: impl IntoIterator<Item = &'t str>,
    ) -> Option<RuleMatch<'_>> {
        // The closest pattern yet: its closeness, its rule's id and place.
        type Best<'s> = Option<(usize, u32, usize, &'s str)>;
        fn offer<'s>(
            best: &mut Best<'s>,
            closeness: usize,
            id: u32,
            place: usize,
            pattern: &'s str,
        ) {
            let wins = best.is_none_or(|(held, held_id, _, _)| {
                closeness > held || (closeness == held && id < held_id)
            });
            if wins {
                *best = Some((closeness, id, place, pattern));
            }
        }
        let mut best: Best<'_> = None;
        let mut wildcard = String::new();
        for host in trackers.into_iter().filter_map(tracker_host) {
            if let Some((pattern, &place)) = self.by_pattern.get_key_value(host.as_str()) {
                offer(&mut best, usize::MAX, self.list[place].id, place, pattern);
            }
            let mut domain = host.as_str();
            loop {
                wildcard.clear();
                wildcard.push_str("*.");
                wildcard.push_str(domain);
                if let Some((pattern, &place)) = self.by_pattern.get_key_value(wildcard.as_str()) {
                    offer(&mut best, domain.len(), self.list[place].id, place, pattern);
                }
                match domain.split_once('.') {
                    Some((_, rest)) => domain = rest,
                    None => break,
                }
            }
        }
        best.map(|(_, _, place, pattern)| RuleMatch {
            rule: &self.list[place],
            pattern,
        })
    }

    pub fn rule_for<'t>(
        &self,
        trackers: impl IntoIterator<Item = &'t str>,
    ) -> Option<&TrackerRule> {
        self.matched(trackers).map(|found| found.rule)
    }
}

impl From<Vec<TrackerRule>> for TrackerRules {
    fn from(list: Vec<TrackerRule>) -> Self {
        Self::new(list)
    }
}

impl std::ops::Deref for TrackerRules {
    type Target = [TrackerRule];

    fn deref(&self) -> &[TrackerRule] {
        &self.list
    }
}

// The host an announce URL names, as a rule matches it and as it is stored:
// read as an http URL reads it whatever the tracker's scheme, so lower case,
// an international name in punycode and an IPv4 address in dotted decimal,
// without a trailing dot. A udp URL's host is otherwise kept as written.
pub fn tracker_host(url: &str) -> Option<String> {
    let url = url::Url::parse(url.trim()).ok()?;
    let host = url::Url::parse(&format!("http://{}/", url.host_str()?)).ok()?;
    let host = host.host_str()?.trim_end_matches('.');
    (!host.is_empty()).then(|| host.to_owned())
}

// The rule a torrent announcing to `trackers` seeds under, read by walking
// every rule's every pattern: the reference `TrackerRules::matched` is
// tested against.
#[cfg(test)]
pub(crate) fn linear_rule_for<'r, 't>(
    rules: &'r [TrackerRule],
    trackers: impl IntoIterator<Item = &'t str>,
) -> Option<&'r TrackerRule> {
    let hosts: Vec<String> = trackers.into_iter().filter_map(tracker_host).collect();
    rules
        .iter()
        .filter_map(|rule| {
            let closeness = hosts
                .iter()
                .flat_map(|host| {
                    rule.host_patterns
                        .iter()
                        .filter_map(|pattern| closeness(pattern, host))
                })
                .max()?;
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
            unpack: false,
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
    Http(url::Url),
    File(PathBuf),
}

impl BlocklistSource {
    pub fn of(blocklist: &str) -> Self {
        match url::Url::parse(blocklist) {
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
