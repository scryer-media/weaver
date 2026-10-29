// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use weaver_graph::VolumeIndex;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_torrent::api::TorrentIdOrHash;
use weaver_torrent::core::{Id20, Id32, InfoHashes};
use weaver_torrent::limits::LimitsConfig;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Checkpoint, Identity, Magnet,
    ManagedTorrent, PeerConnectionOptions, ResumePeers, Session, SslIdentity, TorrentTotals,
    TorrentVersion, TrackerAnnounce, torrent_from_bytes, tracker::Tracker,
};

use crate::SchedulerError;
use crate::StateError;
use crate::categories::CategoryConfig;
use crate::categories::model::{completion_parent, resolve_category_config};
use crate::jobs::JobId;
use crate::jobs::handle::{
    CancellationOrigin, HoldOrigin, HoldSet, JobInfo, QueueAction, QueueMoveTarget, QueueOwner,
    SchedulerHandle, TorrentRate,
};
use crate::jobs::model::{
    FieldUpdate, JobStatus, JobUpdate, PRIORITY_ATTRIBUTE_KEY, Protocol,
    job_status_from_persisted_str,
};
use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress};
use crate::jobs::report::JobReport;
use crate::operations::disk::CapacityReader;
use crate::persistence::Database;
use crate::pipeline::claim_complete_destination_path;
use crate::pipeline::graph::checkpoint;
use crate::pipeline::graph::engine::{Hears, Outside, RunMail, Said};
use crate::pipeline::graph::job::restore::Restored;
use crate::pipeline::graph::job::{GraphJob, Storage};
use crate::pipeline::graph::leaf::LeafError;
use crate::pipeline::graph::limits::{self, Caps, ExtractionLimits};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::{PieceLayout, PieceRoot};
use crate::pipeline::graph::status::RepairSlots;
use crate::proxies::{
    Consumer, LegTenancy, LegTenant, NetworkRuntime, ResolvedBy, SessionTraffic, TorrentScope,
};
use crate::settings::model::{TrackerCertificate, TrackerRule, tracker_rule_for};
use crate::settings::{
    Client, Config, SeedingAction, SeedingPolicy, TorrentConfig, TorrentEncryption,
    TrackerUploadLimit, resolve_seeding_policy,
};
use crate::torrents::admission::{self, Manifest, Refusal};
use crate::torrents::identity::{ClientIdentity, TorrentIdentity};
use crate::torrents::intake::{Heard as IntakeHeard, Holds, Intake, Room};
use crate::torrents::metrics::{
    Counters, FetchOutcome, Published, SeedingSeries, SessionSeries, TorrentEvent, TorrentMetrics,
    TorrentSeries,
};
use crate::torrents::rate::limit_kind;
use crate::torrents::sessions::{
    NetworkRoutes, RouteFailure, RouteKey, RoutePlan, SessionEnv, SessionError, SessionView,
    Sessions, TorrentRouteReason, encryption_policy, unplanned_key,
};
use crate::torrents::status::{self, Engine, Held, Observed, Queued};
use crate::torrents::storage::{PieceStorageFactory, Pieces, TorrentJob, initial_have, layout_of};
use crate::torrents::{
    FilePriority, Infohash, NamedBy, PeerInfo, TorrentCounters, TorrentFile, TorrentInsert,
    TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
};

#[cfg(test)]
mod tests;
pub mod updates;

/// How often torrents are read and their routes checked.
pub const TICK: Duration = Duration::from_secs(10);

/// The longest a torrent's peers go unwritten while they change. Its ledger
/// is written at every tick it changed, as its checkpoint is.
pub const PEERS_EVERY_MS: i64 = 60_000;

/// The most pieces a torrent's checkpoint goes without between ticks: once
/// this many pieces reached its job, or left it, since the checkpoint was
/// last written, it is written at once, so a crash fetches again at most
/// these and those that verified while that write was on its way. A
/// download is never more than a few pieces a write behind, at any rate:
/// a faster one only waits on the write in flight, and its next write
/// takes everything verified meanwhile.
pub const CHECKPOINT_PIECES: u32 = 16;

/// The time torrents are stamped with.
pub trait Clock: Send + Sync {
    fn now_ms(&self) -> i64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}

/// The settings torrents are kept under.
#[derive(Debug, Clone)]
pub struct TorrentSettings {
    pub complete_dir: PathBuf,
    pub categories: Vec<CategoryConfig>,
    pub seeding: Option<SeedingPolicy>,
    pub torrent: TorrentConfig,
}

impl TorrentSettings {
    pub fn of(config: &Config) -> Self {
        Self {
            complete_dir: PathBuf::from(config.complete_dir()),
            categories: config.categories.clone(),
            seeding: config.seeding.clone(),
            torrent: config.torrent.clone(),
        }
    }

    fn category(&self, id: Option<u32>) -> Option<&CategoryConfig> {
        id.and_then(|id| self.categories.iter().find(|category| category.id == id))
    }

    // The scope whose route a torrent takes: its tracker rule's when the
    // rule routes its torrents itself, else its category's when that does,
    // else the default.
    fn scope(&self, id: Option<u32>, rule: Option<&TrackerRule>) -> TorrentScope {
        match (rule, self.category(id)) {
            (Some(rule), _) if rule.torrent_route => TorrentScope::TrackerRule(rule.id),
            (_, Some(category)) if category.torrent_route => TorrentScope::Category(category.id),
            _ => TorrentScope::Default,
        }
    }

    fn policy(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> Option<SeedingPolicy> {
        resolve_seeding_policy(
            record.policy_override.as_ref(),
            rule.and_then(|rule| rule.seeding.as_ref()),
            self.category_seeding(record),
            self.seeding.as_ref(),
        )
    }

    // The policy the torrent would have without its own override.
    fn inherited(
        &self,
        record: &TorrentRecord,
        rule: Option<&TrackerRule>,
    ) -> Option<SeedingPolicy> {
        resolve_seeding_policy(
            None,
            rule.and_then(|rule| rule.seeding.as_ref()),
            self.category_seeding(record),
            self.seeding.as_ref(),
        )
    }

    // Whether the torrent's archives are extracted: its own value, else its
    // tracker rule's, else its category's, else the global default.
    fn unpack(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> bool {
        record
            .unpack
            .or_else(|| rule.and_then(|rule| rule.unpack))
            .or_else(|| {
                self.category(record.category_id)
                    .and_then(|category| category.unpack)
            })
            .unwrap_or(self.torrent.unpack)
    }

    fn category_seeding(&self, record: &TorrentRecord) -> Option<&SeedingPolicy> {
        self.category(record.category_id)
            .and_then(|category| category.seeding.as_ref())
    }

    // The most specific level that sets a policy.
    fn seeding_source(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> SeedingSource {
        let rule = rule.filter(|rule| rule.seeding.is_some());
        if record.policy_override.is_some() {
            SeedingSource::Torrent
        } else if let Some(rule) = rule {
            SeedingSource::TrackerRule(rule.host_pattern.clone())
        } else if self.category_seeding(record).is_some() {
            SeedingSource::Category
        } else {
            SeedingSource::Global
        }
    }
}

// What a torrent's session holds its peers to under its tracker rule:
// whether it takes them from its trackers alone, and the encryption its
// connections are made under over the session's. A change of either takes
// the torrent through its session again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerTerms {
    trackers_only: bool,
    encryption: Option<TorrentEncryption>,
}

impl PeerTerms {
    fn of(rule: Option<&TrackerRule>) -> Self {
        rule.map_or_else(Self::default, |rule| Self {
            trackers_only: rule.trackers_only(),
            encryption: rule.encryption,
        })
    }

    // `options` under these terms.
    fn apply(self, options: &mut AddTorrentOptions) {
        options.trackers_only = self.trackers_only;
        options.peer_opts = self.encryption.map(|encryption| PeerConnectionOptions {
            encryption: Some(encryption_policy(encryption)),
            ..Default::default()
        });
    }
}

// The certificate an SSL torrent under `rule` presents; `None` leaves it
// waiting for one.
fn certificate_of(rule: Option<&TrackerRule>) -> Option<TrackerCertificate> {
    rule.and_then(|rule| rule.ssl.clone())
}

// The engine's identity for `ssl`. A rule keeps only a certificate its key
// matches; one that no longer parses leaves the torrent waiting, as none
// does.
fn ssl_identity(ssl: Option<&TrackerCertificate>) -> Option<SslIdentity> {
    ssl.and_then(|ssl| match ssl.identity() {
        Ok(identity) => Some(identity),
        Err(error) => {
            tracing::warn!(error = %error, "a tracker rule's SSL certificate is refused");
            None
        }
    })
}

// The tracker rule of id `id`, while it stands.
fn rule_of(id: Option<u32>, rules: &[TrackerRule]) -> Option<&TrackerRule> {
    let id = id?;
    rules.iter().find(|rule| rule.id == id)
}

// The client a torrent under `rule` claims to be on every surface that
// names one: the client its tracker rule names, else Weaver.
fn client_of(rule: Option<&TrackerRule>) -> ClientIdentity {
    rule.and_then(|rule| rule.client)
        .unwrap_or(Client::Weaver)
        .name()
}

// How a torrent that is `own` under `rule` names itself to peers and
// trackers: as its client, under its own peer id and tracker key.
fn identity_of(rule: Option<&TrackerRule>, own: &TorrentIdentity) -> Identity {
    client_of(rule).identity_of(own)
}

// The limits `record`'s session holds it to: its own, and on upload a cap
// where that is tighter: its own policy's, else its tracker rule's upload
// limit, else the cap its seeding policy resolves to. A rule's `Unlimited`
// leaves its category's and the global policy's caps out. Each is a channel
// the torrent's peers draw from beside the session's, as each peer class
// over a libtorrent peer is (`bandwidth_manager::request_bandwidth`).
fn ratelimits_of(
    record: &TorrentRecord,
    policy: Option<SeedingPolicy>,
    rule: Option<&TrackerRule>,
) -> LimitsConfig {
    let cap = record
        .policy_override
        .and_then(|policy| policy.max_upload_bps)
        .map(Some)
        .or_else(|| {
            rule.and_then(|rule| rule.upload_limit)
                .map(TrackerUploadLimit::bps)
        })
        .unwrap_or_else(|| policy.and_then(|policy| policy.max_upload_bps));
    LimitsConfig {
        upload_bps: limit_kind(record.upload_limit).tighter(limit_kind(cap)),
        download_bps: limit_kind(record.download_limit),
    }
}

/// Where a torrent's seeding policy comes from: the most specific level that
/// sets one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeedingSource {
    Torrent,
    /// The tracker rule its trackers matched, by its host pattern.
    TrackerRule(String),
    Category,
    Global,
}

/// The torrents the schedule's pause leaves seeding under one tracker rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleSeeding {
    pub host_pattern: String,
    pub torrents: u32,
}

/// A torrent route as it stands: the scopes that run on it, with their
/// names, and its session, open or held, once one was asked for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentRoute {
    pub key: RouteKey,
    pub scopes: Vec<(TorrentScope, Option<String>)>,
    pub session: Option<SessionView>,
    /// Why its leg is down, when it is.
    pub down: Option<RouteFailure>,
    /// Who resolves the names its sessions look up.
    pub names: Option<ResolvedBy>,
}

/// What torrent jobs are built with.
#[derive(Clone)]
pub struct JobEnv {
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) free_disk: CapacityReader,
    pub(crate) slots: usize,
    pub(crate) limits: Arc<ExtractionLimits>,
    // Where the jobs' runs speak, and where a job joins to hear them.
    pub(crate) mail: mpsc::Sender<Said>,
    pub(crate) outside: Arc<Outside>,
    // The repair slots every torrent job shares, as the pipeline's jobs
    // share theirs.
    pub(crate) repairs: Arc<Mutex<RepairSlots>>,
    // With no pipeline to hear the mail, the mail itself, until the service
    // hears it in the pipeline's stead.
    alone: Arc<Mutex<Option<RunMail>>>,
}

impl JobEnv {
    /// The pipeline's own budget: its holds, slots, extraction limits and
    /// the complete root's free space.
    pub fn of_pipeline(pipeline: &crate::pipeline::Pipeline) -> Self {
        let budget = pipeline.graph_budget();
        Self {
            accountant: budget.accountant,
            free_disk: budget.free_disk,
            slots: budget.slots.max(1),
            limits: budget.limits,
            mail: budget.mail,
            outside: budget.outside,
            repairs: Arc::default(),
            alone: Arc::default(),
        }
    }

    /// No budget at all: one slot, no limits, free space unknown.
    pub fn unbounded() -> Self {
        let mail = RunMail::default();
        Self {
            accountant: Arc::new(HoldsAccountant::new(HoldBudgets {
                resident_limit: u64::MAX,
                scratch_total: u64::MAX,
                scratch_ceiling: u64::MAX,
                disk_reserve: 0,
            })),
            free_disk: CapacityReader::unknown(),
            slots: 1,
            limits: Arc::new(ExtractionLimits::unbounded()),
            mail: mail.sender(),
            outside: Arc::default(),
            repairs: Arc::default(),
            alone: Arc::new(Mutex::new(Some(mail))),
        }
    }

    // Hear the mail no pipeline hears, delivering it as the pipeline would.
    fn hear_alone(&self) -> Option<tokio::task::JoinHandle<()>> {
        let mut mail = locked(&self.alone).take()?;
        let (outside, slots) = (Arc::clone(&self.outside), self.slots);
        Some(tokio::spawn(async move {
            while let Some(said) = mail.recv().await {
                outside.deliver(said, slots);
            }
        }))
    }
}

/// How long a torrent leg waits to dial through its route.
const ROUTE_TIMEOUT: Duration = Duration::from_secs(30);

impl TorrentService {
    /// The server's torrent client, started: sessions on the proxy
    /// runtime's routes while the settings turn torrents on, jobs on the
    /// pipeline's budget, and its torrents in the pipeline's queue.
    pub async fn for_server(
        config: &Config,
        db: Database,
        proxies: &Arc<crate::proxies::ProxyRuntime>,
        pipeline: &crate::pipeline::Pipeline,
        queue: SchedulerHandle,
    ) -> Result<Self, String> {
        let routes = NetworkRoutes::new(db.clone(), Arc::clone(proxies), ROUTE_TIMEOUT);
        let sessions = Sessions::new(Arc::new(routes), SessionEnv::of(config), db.clone());
        let service = Self::new(
            db,
            sessions,
            TorrentSettings::of(config),
            JobEnv::of_pipeline(pipeline),
            Arc::new(SystemClock),
        );
        service.publish_to(queue);
        service.inner.sessions.turn(config.torrent.enabled).await;
        service.start().await?;
        Ok(service)
    }
}

/// Where a torrent comes from.
#[derive(Debug, Clone)]
pub enum AddSource {
    Metainfo(Vec<u8>),
    Magnet(String),
}

#[derive(Debug, Clone)]
pub struct AddRequest {
    pub source: AddSource,
    pub category: Option<String>,
    pub policy_override: Option<SeedingPolicy>,
    pub paused: bool,
    pub forced: bool,
    // Whether its archives are extracted; `None` inherits.
    pub unpack: Option<bool>,
    // The attributes its job carries.
    pub metadata: Vec<(String, String)>,
    // Its own rate limits in bytes a second, none being no limit.
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum AddError {
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    Refused(#[from] Refusal),
    #[error("already added as job {}", .0.0)]
    Duplicate(JobId),
    #[error("no network for this torrent: {0}")]
    NoNetwork(String),
    #[error("{}", status::OFF)]
    Off,
    #[error("{0}")]
    State(String),
}

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum ActionError {
    #[error("no torrent {}", .0.0)]
    NotFound(JobId),
    #[error("a private torrent's seeding goal is not met: {0}")]
    UnmetGoal(String),
    #[error("{0}")]
    Invalid(String),
    #[error("{0}")]
    State(String),
}

fn state_error(error: StateError) -> String {
    error.to_string()
}

/// A torrent as consumers read it.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentView {
    pub job_id: JobId,
    pub name: String,
    /// The hash it goes by: its v1 hash, or its v2 hash cut to 20 bytes
    /// when it has no v1 hash.
    pub infohash: String,
    pub infohash_v1: Option<String>,
    pub infohash_v2: Option<String>,
    /// Which hashes verify its pieces, once its metainfo is in hand.
    pub version: Option<TorrentVersion>,
    /// The link to it: both its hashes and its name.
    pub magnet: String,
    pub source: TorrentSource,
    pub state: TorrentState,
    pub detail: Option<String>,
    /// Why its route holds it, while the state comes from that hold.
    pub route_reason: Option<TorrentRouteReason>,
    /// Who holds it stopped, while a hold does.
    pub paused_by: Option<HoldOrigin>,
    pub category: Option<String>,
    /// The scope selected by its tracker rule, category, or the default.
    pub scope: TorrentScope,
    pub private: bool,
    pub forced: bool,
    pub progress: f64,
    pub total_bytes: u64,
    pub have_bytes: u64,
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub ratio: f64,
    pub download_bps: u64,
    pub upload_bps: u64,
    /// The torrent's own upload and download limits in bytes per second;
    /// None is unlimited and zero holds that direction closed.
    pub upload_limit: Option<u64>,
    pub download_limit: Option<u64>,
    pub eta_seconds: Option<u64>,
    pub peers: u32,
    pub seeds: u32,
    pub leechers: u32,
    /// The most peers a torrent connects to.
    pub max_peers: u32,
    /// The swarm's seeds and leechers as its trackers last counted them.
    pub swarm_seeds: Option<u32>,
    pub swarm_leechers: Option<u32>,
    /// Seconds until the next tracker announce.
    pub reannounce_seconds: Option<u64>,
    /// Seconds spent running in a session.
    pub active_seconds: u64,
    /// The bytes moved over the time active.
    pub download_avg_bps: u64,
    pub upload_avg_bps: u64,
    /// Bytes received and thrown away this run: pieces that failed their
    /// check, and blocks that arrived when already in hand. Counts from zero
    /// each time the torrent starts.
    pub wasted_bytes: u64,
    /// The last time a complete copy was in reach.
    pub last_seen_complete_ms: Option<i64>,
    /// The torrent's pieces, once its metainfo is in hand.
    pub pieces: Option<TorrentPieces>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub comment: Option<String>,
    pub added_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    pub seeding_left: Option<String>,
    /// The torrent's own seeding policy, when it sets one, and what it
    /// would have without it.
    pub seeding_override: Option<SeedingPolicy>,
    pub seeding_inherited: Option<SeedingPolicy>,
    /// The seeding policy in force, and the level it comes from.
    pub seeding_policy: SeedingPolicy,
    pub seeding_source: SeedingSource,
    /// Whether its archives are extracted.
    pub unpack: bool,
    pub route: Option<String>,
    pub files: Vec<TorrentFile>,
    /// Whether its pieces are fetched in order.
    pub sequential: bool,
    /// Whether each file's first and last pieces are fetched first.
    pub first_last_piece_first: bool,
    /// Each file's bytes in hand, by its index in the torrent.
    pub file_have: Vec<u64>,
    pub trackers: Vec<TorrentTracker>,
    /// The attributes its job was added with.
    pub metadata: Vec<(String, String)>,
}

/// What a support report reads of a torrent beyond its view.
#[derive(Debug, Clone)]
pub struct TorrentSupport {
    pub view: TorrentView,
    /// The one copy of its metainfo, once it is in hand.
    pub metainfo: Option<Bytes>,
    /// The ledger as of this reading.
    pub counters: TorrentCounters,
    /// Of the bytes wasted this run, those of pieces that failed their
    /// check, and those of blocks that arrived when already in hand.
    pub failed_bytes: u64,
    pub redundant_bytes: u64,
    /// Its established peers, the seeders among them, and the peers still
    /// in their handshake.
    pub peers: (u32, u32, u32),
    pub peer_kinds: PeerKinds,
    /// Where its metadata fetch asks for peers, while one runs.
    pub fetch: Option<weaver_torrent::FetchPhase>,
    pub in_session: bool,
    pub leg: Option<TorrentLeg>,
    /// Whether its job is built; until it is, the pieces its checkpoint
    /// has in hand.
    pub has_job: bool,
    pub kept_pieces: u32,
    /// The bytes of the files it wants, and those of them in hand.
    pub wanted_bytes: u64,
    pub wanted_have_bytes: u64,
    /// The snapshot the exporter renders, as last published.
    pub metrics: TorrentMetrics,
}

/// The leg of its route a torrent's session runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentLeg {
    pub position: usize,
    pub egress_id: u32,
    pub own_sockets: bool,
}

/// A torrent's connections to peers, counted by how they were made.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PeerKinds {
    pub tcp: u32,
    pub utp: u32,
    pub outgoing: u32,
    pub incoming: u32,
    pub encrypted: u32,
}

impl PeerKinds {
    pub fn of(peers: &[weaver_torrent::PeerInfo]) -> Self {
        let mut kinds = Self::default();
        for peer in peers {
            match peer.connection {
                weaver_torrent::ConnectionKind::Tcp => kinds.tcp += 1,
                weaver_torrent::ConnectionKind::Utp => kinds.utp += 1,
            }
            if peer.outgoing {
                kinds.outgoing += 1;
            } else {
                kinds.incoming += 1;
            }
            if peer.encryption != weaver_torrent::Encryption::None {
                kinds.encrypted += 1;
            }
        }
        kinds
    }
}

/// A torrent's pieces: how many, how long each is, and how many verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TorrentPieces {
    pub count: u32,
    pub size: u64,
    pub have: u32,
}

impl TorrentView {
    /// Where the torrent's output is: the folder that holds it, and the
    /// output itself, its file when it is one file named as the torrent is,
    /// else its folder. Both are empty until weaver has placed the torrent.
    pub fn paths(&self) -> (String, String) {
        let Some(dir) = &self.storage_dir else {
            return (String::new(), String::new());
        };
        let dir = PathBuf::from(dir);
        let single = match self.files.as_slice() {
            [file] if file.path == self.name => Path::new(&file.path).file_name(),
            _ => None,
        };
        let content = match single {
            Some(name) => dir.join(name),
            None => dir,
        };
        let save = content
            .parent()
            .map_or_else(|| content.clone(), Path::to_path_buf);
        (
            save.to_string_lossy().into_owned(),
            content.to_string_lossy().into_owned(),
        )
    }

    /// How much of `file` is in hand, from 0 to 1.
    pub fn file_progress(&self, file: &TorrentFile) -> f64 {
        let have = self
            .file_have
            .get(file.index as usize)
            .copied()
            .unwrap_or(0);
        if file.length == 0 {
            1.0
        } else {
            (have as f64 / file.length as f64).min(1.0)
        }
    }
}

// A torrent's metainfo, read.
struct Parsed {
    hashes: InfoHashes,
    manifest: Manifest,
    layout: Arc<PieceLayout>,
    private: bool,
    // The metainfo names a CA its peers' certificates are signed by.
    ssl: bool,
    trackers: Vec<TrackerEntry>,
    total: u64,
    about: About,
}

// What a torrent's metainfo says of where it came from.
#[derive(Debug, Clone, Default)]
struct About {
    version: Option<TorrentVersion>,
    created_by: Option<String>,
    created_at_ms: Option<i64>,
    comment: Option<String>,
}

fn text(bytes: Option<&[u8]>) -> Option<String> {
    let text = String::from_utf8_lossy(bytes?).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

// A tracker the torrent's metainfo names.
fn tracker(tier: u32, url: &[u8]) -> Option<TrackerEntry> {
    let url = std::str::from_utf8(url).ok()?.trim();
    (!url.is_empty()).then(|| TrackerEntry {
        tier,
        url: url.to_owned(),
        named_by: NamedBy::Torrent,
    })
}

// `trackers` as a torrent lists them: by tier, in the order given within a
// tier, each URL once where, and as whoever, first named it. A tracker that
// is no URL is left out, as a session leaves it out.
fn tidy_trackers(trackers: Vec<TrackerEntry>) -> Vec<TrackerEntry> {
    let mut tidy: Vec<TrackerEntry> = Vec::with_capacity(trackers.len());
    for entry in trackers {
        let url = entry.url.trim();
        if weaver_torrent::Url::parse(url).is_err() {
            continue;
        }
        if tidy.iter().all(|known| known.url != url) {
            tidy.push(TrackerEntry {
                tier: entry.tier,
                url: url.to_owned(),
                named_by: entry.named_by,
            });
        }
    }
    tidy.sort_by_key(|entry| entry.tier);
    tidy
}

// Refuse an edit naming a tracker that is no URL.
fn tracker_urls(trackers: &[TrackerEntry]) -> Result<(), ActionError> {
    trackers
        .iter()
        .map(|tracker| tracker.url.trim())
        .filter(|url| !url.is_empty())
        .try_for_each(|url| {
            weaver_torrent::Url::parse(url)
                .map(drop)
                .map_err(|error| ActionError::Invalid(format!("tracker {url}: {error}")))
        })
}

// The tiers of `trackers`, listed by tier, as a session announces to them,
// each with who named it.
fn tracker_tiers(trackers: &[TrackerEntry]) -> Vec<Vec<Tracker>> {
    let mut tiers: Vec<(u32, Vec<Tracker>)> = Vec::new();
    for entry in trackers {
        let Ok(url) = weaver_torrent::Url::parse(&entry.url) else {
            continue;
        };
        let tracker = Tracker {
            url,
            named_by: entry.named_by,
        };
        match tiers.last_mut() {
            Some((tier, named)) if *tier == entry.tier => named.push(tracker),
            _ => tiers.push((entry.tier, vec![tracker])),
        }
    }
    tiers.into_iter().map(|(_, named)| named).collect()
}

// The hashes a duplicate of a torrent going by `hashes` would share.
fn infohashes(hashes: InfoHashes) -> Vec<Infohash> {
    hashes
        .v1
        .map(|hash| Infohash::V1(hash.0))
        .into_iter()
        .chain(hashes.v2.map(|hash| Infohash::V2(hash.0)))
        .collect()
}

// The hashes `record` goes by.
fn hashes_of(record: &TorrentRecord) -> InfoHashes {
    InfoHashes {
        v1: record.infohash_v1.map(Id20::new),
        v2: record.infohash_v2.map(Id32::new),
    }
}

fn parse(bytes: &[u8]) -> Result<Parsed, AddError> {
    let invalid =
        |error: &dyn std::fmt::Display| AddError::Invalid(format!("not a torrent: {error}"));
    let metainfo = torrent_from_bytes(bytes).map_err(|error| invalid(&error))?;
    let info = metainfo
        .info
        .data
        .validate()
        .map_err(|error| invalid(&error))?;
    let layout = Arc::new(layout_of(&info).map_err(|error| invalid(&error))?);
    let name = info
        .name()
        .map(|name| name.into_owned())
        .unwrap_or_default();
    let mut trackers: Vec<TrackerEntry> = metainfo
        .announce_list
        .iter()
        .enumerate()
        .flat_map(|(tier, urls)| {
            urls.iter()
                .filter_map(move |url| tracker(tier as u32, url.as_ref()))
        })
        .collect();
    if trackers.is_empty() {
        trackers.extend(
            metainfo
                .announce
                .as_ref()
                .and_then(|url| tracker(0, url.as_ref())),
        );
    }
    Ok(Parsed {
        hashes: metainfo.info_hashes,
        manifest: Manifest {
            name,
            single_file: info.is_single_file(),
            paths: layout
                .files()
                .filter(|file| !file.padding)
                .map(|file| file.name.clone())
                .collect(),
        },
        total: layout
            .files()
            .filter(|file| !file.padding)
            .map(|file| file.length)
            .sum(),
        private: info.info().private,
        ssl: info.info().ssl_cert.is_some(),
        layout,
        trackers,
        about: About {
            version: Some(info.version()),
            created_by: text(metainfo.created_by.as_ref().map(|by| by.as_ref())),
            created_at_ms: metainfo
                .creation_date
                .and_then(|secs| i64::try_from(secs).ok())
                .and_then(|secs| secs.checked_mul(1000)),
            comment: text(metainfo.comment.as_ref().map(|comment| comment.as_ref())),
        },
    })
}

// A torrent's place in its session.
struct Attached {
    session: Arc<Session>,
    handle: Arc<ManagedTorrent>,
    // The terms the session took it under.
    terms: PeerTerms,
    // The certificate an SSL torrent was given from its rule.
    ssl: Option<TrackerCertificate>,
    // The leg the session runs it on, which counts what it moves.
    leg: Option<OnLeg>,
}

// The leg of a scope's route a session runs on.
struct OnLeg {
    network: Arc<NetworkRuntime>,
    egress_id: u32,
    // Whether the session dials from the leg's own sockets, past any route.
    own_sockets: bool,
    // The torrent's place on the leg, which counts what it holds there
    // until the torrent leaves its session.
    tenancy: Arc<LegTenancy>,
}

// A torrent on its leg, as its run holds it now: its established peers
// count on every leg. A session on the leg's own sockets passes no route,
// so the leg hears from the torrent its peers in their handshake and what
// it receives too.
struct LegTorrent {
    handle: Arc<ManagedTorrent>,
    own_sockets: bool,
}

impl LegTenant for LegTorrent {
    fn traffic(&self) -> SessionTraffic {
        let Some(run) = self.handle.gauges() else {
            return SessionTraffic::default();
        };
        let peers = run.peers();
        let (opening, bytes_per_second) = if self.own_sockets {
            (u64::from(peers.connecting), run.download_bps())
        } else {
            (0, 0)
        };
        SessionTraffic {
            open: u64::from(peers.live),
            opening,
            bytes_per_second,
        }
    }
}

// A magnet's metadata fetch on its route's session, for the run that
// follows it: the session, the identity its trackers heard `started` under,
// and its tracker task.
struct Fetched {
    session: Arc<Session>,
    identity: Identity,
    trackers: weaver_torrent::tracker::TrackerHandover,
}

// A magnet's metadata fetch: running, or ended by a pause, which a resume
// starts again. Removing or pausing the torrent aborts a running fetch,
// which lets its trackers go with one `stopped`.
enum Resolving {
    Fetching(tokio::task::AbortHandle),
    Paused,
}

// What the service keeps of each torrent while it lives.
struct Live {
    record: TorrentRecord,
    // Who it is to its peers and trackers from its add to its remove, in
    // every session it runs in.
    identity: TorrentIdentity,
    // The tracker rule its trackers match, picked again whenever they or
    // the rules change: the trackers are what is kept, not the rule.
    rule_id: Option<u32>,
    // Whether its metainfo marks it private; read from the metainfo, never
    // kept apart from it.
    private: bool,
    // The peers and bans its next start begins with: the one place a ban
    // made here lives until it is written.
    resume: ResumePeers,
    // What the database holds of its ledger and peers.
    ledger: Ledger,
    names: Vec<String>,
    total: u64,
    layout: Option<Arc<PieceLayout>>,
    job: Option<Arc<TorrentJob>>,
    intake: Option<Arc<Intake>>,
    attached: Option<Attached>,
    // A magnet's metadata fetch, while it runs: its announce table, whose
    // trackers are announced to before the torrent is in a session, and
    // where it asks for peers.
    fetching: Option<weaver_torrent::MetadataFetch>,
    // A magnet's metadata fetch, until it ends.
    resolving: Option<Resolving>,
    route: Option<RouteKey>,
    held: Option<Held>,
    about: About,
    counters: TorrentCounters,
    // What the session reported at the last kept reading, for the ledger's
    // deltas; the next session it runs in counts on from here.
    seen: TorrentTotals,
    // The instant its durations were last advanced to, with what each ran
    // past its whole seconds.
    timed: Timed,
    engine: Engine,
    // Why the session last refused to pause or resume the torrent, until it
    // takes the next change.
    run_error: Option<String>,
    queued: bool,
    // Who holds it stopped, while a hold does.
    paused_by: Option<HoldOrigin>,
    // The operator's level for each file, as stored.
    levels: Vec<FilePriority>,
    speeds: (u64, u64),
    // Its established peers, the seeders among them, and the peers still
    // in their handshake.
    peers: (u32, u32, u32),
    // The directory claimed for a move, while the move waits or is under
    // way; the job does the moving.
    moving: Option<PathBuf>,
    // Each file's bytes in hand: the checkpoint's until the session has
    // checked the files, then the session's.
    file_have: Vec<u64>,
    // The pieces its checkpoint has in hand, until its job is back.
    kept_pieces: u32,
    // Why unpacking refused it, as its history kept it when it was
    // restored: a restored job does not say.
    failed: Option<String>,
}

// `torrent` as the download queue and its holds see it. A seeding torrent
// under a tracker rule that never pauses seeds through the schedule's holds
// until its goal is met.
fn queued(settings: &TorrentSettings, rules: &[TrackerRule], torrent: &Live, now: i64) -> Queued {
    let seeding = torrent.seeding();
    // A torrent under a never-pause rule still owes its tracker while it
    // downloads, and while it seeds with a goal left.
    let rule = torrent.rule(rules);
    let runs_through_schedule = rule.is_some_and(|rule| rule.never_pause)
        && (!seeding
            || status::goal_left(
                settings.policy(&torrent.record, rule).as_ref(),
                &torrent.counters,
                torrent.total,
                now,
            )
            .is_some());
    Queued {
        job_id: torrent.record.job_id,
        seeding,
        forced: torrent.record.forced,
        paused: !torrent.resumed(),
        runs_through_schedule,
    }
}

impl Live {
    // Its files: the metainfo's names and lengths, at the levels it holds.
    fn files(&self) -> Vec<TorrentFile> {
        self.layout
            .iter()
            .flat_map(|layout| layout.files())
            .enumerate()
            .map(|(index, file)| TorrentFile {
                index: index as u32,
                path: file.name.clone(),
                length: file.length,
                priority: self.levels.get(index).copied().unwrap_or_default(),
            })
            .collect()
    }

    // The tracker rule it is under, while that rule stands.
    fn rule<'r>(&self, rules: &'r [TrackerRule]) -> Option<&'r TrackerRule> {
        rule_of(self.rule_id, rules)
    }

    // A torrent whose ledger and peers the database holds as given, as of
    // `now`.
    fn new(
        record: TorrentRecord,
        counters: TorrentCounters,
        resume: ResumePeers,
        now: i64,
    ) -> Self {
        Self {
            record,
            identity: TorrentIdentity::fresh(),
            rule_id: None,
            private: false,
            ledger: Ledger {
                written: Some((counters, resume.clone())),
                peers_at_ms: now,
                lost: Arc::default(),
            },
            resume,
            names: Vec::new(),
            total: 0,
            layout: None,
            job: None,
            intake: None,
            attached: None,
            fetching: None,
            resolving: None,
            route: None,
            held: None,
            about: About::default(),
            counters,
            seen: TorrentTotals::default(),
            timed: Timed::at(now),
            engine: Engine::Absent,
            run_error: None,
            queued: false,
            paused_by: None,
            levels: Vec::new(),
            speeds: (0, 0),
            peers: (0, 0, 0),
            moving: None,
            file_have: Vec::new(),
            kept_pieces: 0,
            failed: None,
        }
    }

    fn complete(&self) -> bool {
        self.record.completed_at_ms.is_some()
    }

    // Complete, or with every piece it wants had as the session last said.
    fn seeding(&self) -> bool {
        status::seeding(self.complete(), &self.engine)
    }

    // What it shows until its job is back, from what was stored: what the
    // root its checkpoint keeps has in hand, as the job restored from it
    // will, and the failure its history keeps.
    fn kept(&mut self, failed: Option<String>, root: Option<PieceRoot>) {
        if let Some(root) = root {
            self.file_have = root.file_have();
            self.kept_pieces = root.have().count_ones() as u32;
        }
        self.failed = failed;
    }

    // Fold what the session moved since the last reading into the ledger,
    // and run its durations to `now`: taken before anything changes what
    // the torrent is doing, so what came before the change counts as what
    // the torrent was doing then.
    fn settle(&mut self, now: i64) {
        let reading = self.reading(now);
        self.keep(reading);
    }

    // Keep a reading as what the session last said; the bytes it moved
    // since the reading before are counted on the egress its leg runs on.
    fn keep(&mut self, reading: Reading) {
        let (received, sent) = reading.moved;
        if let Some(leg) = self.attached.as_ref().and_then(|a| a.leg.as_ref()) {
            leg.network
                .record_torrent_bytes(leg.egress_id, received, sent);
        }
        self.file_have = reading.file_have;
        self.counters = reading.counters;
        self.seen = reading.seen;
        self.timed = reading.timed;
    }

    // What of the ledger and peers the database does not hold yet, taken
    // as written, and none when nothing changed. The ledger goes whenever
    // it changed, so after a crash the torrent shows what its last tick
    // knew, as its pieces do. The peers go once a minute has passed since
    // they were last taken, or at once when `at_once`; they are the
    // session's while it runs the torrent, and one stopped by an error
    // keeps those last taken.
    fn take_resume(&mut self, now: i64, at_once: bool) -> Option<Resume> {
        if self.ledger.lost.swap(false, Ordering::AcqRel) {
            self.ledger.written = None;
        }
        if at_once || now - self.ledger.peers_at_ms >= PEERS_EVERY_MS {
            if let Some(peers) = self
                .attached
                .as_ref()
                .and_then(|attached| attached.handle.resume_peers())
            {
                self.resume = peers;
            }
            self.ledger.peers_at_ms = now;
        }
        let (counters, peers) = match &self.ledger.written {
            Some((counters, peers)) => (
                (*counters != self.counters).then_some(self.counters),
                (!same_peers(peers, &self.resume)).then(|| self.resume.clone()),
            ),
            None => (Some(self.counters), Some(self.resume.clone())),
        };
        self.ledger.written = Some((self.counters, self.resume.clone()));
        (counters.is_some() || peers.is_some()).then(|| Resume {
            counters,
            peers,
            lost: Arc::clone(&self.ledger.lost),
        })
    }

    fn resumed(&self) -> bool {
        self.record.intent == TorrentIntent::Resumed
    }

    // The session took a change to the torrent's run, or refused it.
    fn ran(&mut self, result: anyhow::Result<()>) {
        if let Err(error) = &result {
            tracing::warn!(job_id = self.record.job_id.0, error = %error, "a torrent's run did not change");
        }
        self.run_error = result.err().map(|error| format!("{error:#}"));
    }

    // Why unpacking refused the torrent's output, while that stands: the
    // job's word, or the one its history keeps.
    fn refusal(&self) -> Option<String> {
        self.job
            .as_ref()
            .and_then(|job| {
                job.with(|job| {
                    job.writer()
                        .refusal()
                        .map(|refusal| refusal.job_failure_reason())
                })
            })
            .or_else(|| self.failed.clone())
    }

    // A write found no room: the session fetches nothing for the torrent
    // and keeps seeding it, as it last reported.
    fn upload_mode(&self) -> bool {
        matches!(
            self.engine,
            Engine::Live {
                upload_mode: true,
                ..
            }
        )
    }

    // The pieces, and how many of them the job holds verified; until its
    // job is back, how many its checkpoint has.
    fn pieces(&self) -> Option<TorrentPieces> {
        let layout = self.layout.as_ref()?;
        let have = match &self.job {
            Some(job) => job.with(|job| job.have().count_ones()) as u32,
            None => self.kept_pieces,
        };
        Some(TorrentPieces {
            count: layout.piece_count(),
            size: layout.piece_length(),
            have,
        })
    }

    // How announcing to each tracker went: in the session's latest run, or
    // while a magnet's metadata is fetched, in the fetch.
    fn announces(&self) -> Vec<TrackerAnnounce> {
        match &self.attached {
            Some(attached) => attached.handle.tracker_announces(),
            None => self
                .fetching
                .as_ref()
                .and_then(|fetch| fetch.trackers.as_ref())
                .map(|table| table.announces())
                .unwrap_or_default(),
        }
    }

    // What the session says of the torrent now, over what was kept of it:
    // the one reading the views show and the ledger keeps. Its durations
    // run to `now` as its bytes do: the time since the last kept reading
    // is active while the session runs the torrent, and finished while it
    // seeds.
    fn reading(&self, now: i64) -> Reading {
        let mut reading = self.sampled(now);
        let (state, _) = status::project(&self.observed(reading.engine.clone()));
        reading.timed = self.timed.advanced(
            now,
            matches!(reading.engine, Engine::Live { .. }),
            state == TorrentState::Seeding,
            &mut reading.counters,
        );
        reading
    }

    // The session's figures for the torrent now, over what was kept of it,
    // with its durations as last kept.
    fn sampled(&self, now: i64) -> Reading {
        let Some(attached) = &self.attached else {
            return Reading {
                engine: Engine::Absent,
                file_have: self.file_have.clone(),
                counters: self.counters,
                seen: self.seen,
                timed: self.timed,
                moved: (0, 0),
                speeds: (0, 0),
                peers: (0, 0, 0),
            };
        };
        let stats = attached.handle.stats();
        let engine = Engine::of(&stats);
        let mut file_have = self.file_have.clone();
        take_file_have(&mut file_have, &engine, &stats.file_progress);
        let live = stats.live.as_ref();
        // The session counts on from what the torrent had moved when it
        // took it, through every pause.
        let seen = attached.handle.totals();
        let (up, down) = (
            seen.uploaded_bytes.saturating_sub(self.seen.uploaded_bytes),
            seen.fetched_bytes.saturating_sub(self.seen.fetched_bytes),
        );
        // On the wire: the blocks and every other message around them.
        let moved = (
            down + seen
                .fetched_protocol_bytes
                .saturating_sub(self.seen.fetched_protocol_bytes),
            up + seen
                .uploaded_protocol_bytes
                .saturating_sub(self.seen.uploaded_protocol_bytes),
        );
        let mut counters = self.counters;
        counters.uploaded_bytes += up;
        counters.downloaded_bytes += down;
        if up + down > 0 {
            counters.last_activity_ms = Some(now);
        }
        // The session's sighting of a complete copy, ours or a connected
        // peer's (libtorrent `torrent::status`'s `last_seen_complete`, the
        // `m_swarm_last_seen_complete` of `second_tick`); the latest kept
        // stands, as one from a run before or a peer since gone.
        let seen_complete = attached
            .handle
            .live()
            .and_then(|live| live.swarm_complete_ago())
            .map(|ago| now.saturating_sub(i64::try_from(ago.as_millis()).unwrap_or(i64::MAX)));
        counters.last_seen_complete_ms = counters.last_seen_complete_ms.max(seen_complete);
        Reading {
            engine,
            file_have,
            counters,
            seen,
            timed: self.timed,
            moved,
            speeds: live.map_or((0, 0), |live| {
                (live.download_speed.as_bytes(), live.upload_speed.as_bytes())
            }),
            peers: live.map_or((0, 0, 0), |live| {
                (
                    live.snapshot.peer_stats.live,
                    live.snapshot.peer_stats.live_seeders,
                    live.snapshot.peer_stats.connecting,
                )
            }),
        }
    }

    // Everything the state is read from, with the session reporting
    // `engine`.
    fn observed(&self, engine: Engine) -> Observed {
        Observed {
            engine: self.run_error.clone().map_or(engine, Engine::Error),
            has_metadata: self.layout.is_some(),
            fetch: self.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            complete: self.complete(),
            intent: self.record.intent,
            queued: self.queued,
            paused_by: self.paused_by,
            failed: self.refusal(),
            held: self.held.clone(),
            moving: self.moving.is_some(),
        }
    }
}

// A torrent as the session reports it now.
struct Reading {
    engine: Engine,
    // Each file's bytes in hand.
    file_have: Vec<u64>,
    // The ledger with what the session moved since it was last kept.
    counters: TorrentCounters,
    // The session's totals, which the ledger's deltas are taken from.
    seen: TorrentTotals,
    // The instant the counters' durations run to.
    timed: Timed,
    // The bytes received and sent on the wire since the last kept reading.
    moved: (u64, u64),
    speeds: (u64, u64),
    peers: (u32, u32, u32),
}

// What the exporter reads of a torrent beside its view.
struct Unviewed {
    wanted_bytes: u64,
    hash_failed_bytes: u64,
    redundant_bytes: u64,
    seeding_seconds: u64,
    peers_connecting: u32,
    leg: Option<(String, usize)>,
    fetch: Option<weaver_torrent::FetchPhase>,
    seeding: SeedingSeries,
}

impl Unviewed {
    // The torrent's figures: its view's, with these and its trackers.
    fn series(self, view: &TorrentView, announces: &[TrackerAnnounce]) -> TorrentSeries {
        TorrentSeries {
            job_id: view.job_id,
            category: view.category.clone().unwrap_or_default(),
            route: view.route.clone().unwrap_or_default(),
            state: view.state,
            have_bytes: view.have_bytes,
            total_bytes: view.total_bytes,
            wanted_bytes: self.wanted_bytes,
            pieces: view.pieces,
            downloaded_bytes: view.downloaded_bytes,
            uploaded_bytes: view.uploaded_bytes,
            hash_failed_bytes: self.hash_failed_bytes,
            redundant_bytes: self.redundant_bytes,
            active_seconds: view.active_seconds,
            seeding_seconds: self.seeding_seconds,
            download_bps: view.download_bps,
            upload_bps: view.upload_bps,
            ratio: view.ratio,
            peers_established: view.peers,
            peers_connecting: self.peers_connecting,
            peer_seeders: view.seeds,
            leg: self.leg,
            fetch: self.fetch,
            seeding: self.seeding,
            trackers: crate::torrents::metrics::trackers(&view.trackers, announces),
        }
    }
}

// Bytes a torrent received and threw away since it was added in this
// process: pieces that failed their check, and blocks that arrived when
// already in hand.
fn wasted(totals: &TorrentTotals) -> u64 {
    totals.failed_bytes + totals.redundant_bytes
}

// The instant a torrent's durations run to, and the milliseconds each ran
// past the whole seconds its counter holds: a reading carries them over,
// so no part of a second is lost between readings.
#[derive(Debug, Clone, Copy)]
struct Timed {
    at_ms: i64,
    active_ms: u64,
    finished_ms: u64,
}

impl Timed {
    fn at(now: i64) -> Self {
        Self {
            at_ms: now,
            active_ms: 0,
            finished_ms: 0,
        }
    }

    // Run on to `now`: the time since runs into the active duration while
    // `active`, and into the finished one while `seeding`. A clock stepped
    // back credits nothing for that reading, and the next runs from `now`.
    fn advanced(
        self,
        now: i64,
        active: bool,
        seeding: bool,
        counters: &mut TorrentCounters,
    ) -> Self {
        let elapsed = u64::try_from(now - self.at_ms).unwrap_or(0);
        let run = |part: u64, running: bool, seconds: &mut u64| {
            if !running {
                return part;
            }
            let ms = part.saturating_add(elapsed);
            *seconds = seconds.saturating_add(ms / 1000);
            ms % 1000
        };
        Self {
            at_ms: now,
            active_ms: run(self.active_ms, active, &mut counters.active_duration_s),
            finished_ms: run(self.finished_ms, seeding, &mut counters.finished_duration_s),
        }
    }
}

// What the database holds of a torrent's ledger and peers, and when its
// peers were last taken.
struct Ledger {
    // `None` while what it holds is not known: the next write is all of it.
    written: Option<(TorrentCounters, ResumePeers)>,
    // When the peers were last taken from the session to be written.
    peers_at_ms: i64,
    // Set by a write that failed, so the next one writes all of it.
    lost: Arc<AtomicBool>,
}

// A write of what changed in a torrent's ledger and peers.
struct Resume {
    counters: Option<TorrentCounters>,
    peers: Option<ResumePeers>,
    lost: Arc<AtomicBool>,
}

// A write of a torrent's checkpoint, with its ledger and peers when they
// changed, queued; `landed` is none when nothing changed.
struct CheckpointSave {
    job_id: JobId,
    landed: Option<oneshot::Receiver<Result<(), StateError>>>,
    pending: Option<checkpoint::Pending>,
    lost: Option<Arc<AtomicBool>>,
}

// A write of a torrent's ledger and peers, queued.
struct Landing {
    job_id: JobId,
    landed: oneshot::Receiver<Result<bool, StateError>>,
    lost: Arc<AtomicBool>,
}

// The same peers and bans, in whatever order: the session lists the peers
// it never reached in no fixed order.
fn same_peers(a: &ResumePeers, b: &ResumePeers) -> bool {
    fn sorted<T: Ord + Copy>(items: &[T]) -> Vec<T> {
        let mut items = items.to_vec();
        items.sort_unstable();
        items
    }
    sorted(&a.peers) == sorted(&b.peers) && sorted(&a.banned) == sorted(&b.banned)
}

// What the job side asks the service to look at: the jobs that changed,
// the ones whose checkpoint is behind them with how many pieces came or
// went since it was written, and the writes a job's intake could not make.
#[derive(Default)]
pub(super) struct Signal {
    dirty: Mutex<BTreeSet<JobId>>,
    unsaved: Mutex<BTreeMap<JobId, u32>>,
    refused: Mutex<BTreeMap<JobId, String>>,
    wake: Notify,
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Signal {
    pub(super) fn mark(&self, job_id: JobId) {
        self.marked(job_id, 0);
    }

    fn marked(&self, job_id: JobId, pieces: u32) {
        locked(&self.dirty).insert(job_id);
        *locked(&self.unsaved).entry(job_id).or_default() += pieces;
        self.wake.notify_one();
    }

    fn heard(&self, job_id: JobId, heard: IntakeHeard) {
        match heard {
            IntakeHeard::Changed => self.marked(job_id, 1),
            IntakeHeard::Refused(error) => {
                locked(&self.refused).entry(job_id).or_insert(error);
                self.mark(job_id);
            }
        }
    }

    fn take(&self) -> BTreeSet<JobId> {
        std::mem::take(&mut *locked(&self.dirty))
    }

    fn take_refused(&self, job_id: JobId) -> Option<String> {
        locked(&self.refused).remove(&job_id)
    }

    fn take_unsaved(&self) -> BTreeSet<JobId> {
        std::mem::take(&mut *locked(&self.unsaved))
            .into_keys()
            .collect()
    }

    fn take_unsaved_one(&self, job_id: JobId) -> bool {
        locked(&self.unsaved).remove(&job_id).is_some()
    }

    // The jobs whose checkpoint is [`CHECKPOINT_PIECES`] pieces behind.
    fn take_due(&self) -> BTreeSet<JobId> {
        let mut unsaved = locked(&self.unsaved);
        let due: BTreeSet<JobId> = unsaved
            .iter()
            .filter(|(_, pieces)| **pieces >= CHECKPOINT_PIECES)
            .map(|(job_id, _)| *job_id)
            .collect();
        unsaved.retain(|job_id, _| !due.contains(job_id));
        due
    }
}

// A torrent job hearing its runs through the pipeline's mail.
struct Hearer {
    job: Arc<TorrentJob>,
    signal: Arc<Signal>,
}

impl Hears for Hearer {
    fn said(&self, said: Said, slots: usize) {
        let job_id = JobId(said.scope);
        if let Err(error) = self.job.step(slots, |job, repairs, limit| {
            job.run_said(said, repairs, limit)
        }) {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent job's run failed");
        }
        self.signal.mark(job_id);
    }
}

/// What the service has written so far, and the checkpoints it has read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    pub ledgers: u64,
    pub checkpoints: u64,
    pub checkpoints_read: u64,
}

struct Inner {
    db: Database,
    sessions: Sessions,
    settings: RwLock<TorrentSettings>,
    // The tracker rules, as the database last held them.
    rules: RwLock<Arc<[TrackerRule]>>,
    env: JobEnv,
    clock: Arc<dyn Clock>,
    live: tokio::sync::Mutex<BTreeMap<JobId, Live>>,
    // Held while a torrent's job is restored, from the check that it waits
    // for one to the job given: one restore at a time. The blocking restore
    // owns it, so a restore whose awaiting task ends still holds it.
    restoring: Arc<tokio::sync::Mutex<()>>,
    signal: Arc<Signal>,
    ticks: watch::Sender<u64>,
    retired: watch::Sender<u64>,
    written: watch::Sender<Written>,
    queue: Mutex<Option<SchedulerHandle>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    tick: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Rung by a settings change: the tick runs then, not at its next turn.
    settings_changed: Notify,
    // The torrents and their sessions as last published, for the exporter.
    published: watch::Sender<Arc<Published>>,
    // Each route's session as last read. The tick reads them, and turning
    // torrents on or off, under the sessions' lock each takes anyway: a
    // publish from an action never waits on a session that is opening.
    sessions_read: Mutex<Vec<SessionSeries>>,
    counters: Counters,
}

#[derive(Clone)]
pub struct TorrentService {
    inner: Arc<Inner>,
}

impl TorrentService {
    pub fn new(
        db: Database,
        sessions: Sessions,
        settings: TorrentSettings,
        env: JobEnv,
        clock: Arc<dyn Clock>,
    ) -> Self {
        sessions.set_config(settings.torrent.clone());
        Self {
            inner: Arc::new(Inner {
                db,
                sessions,
                settings: RwLock::new(settings),
                rules: RwLock::new(Arc::from([])),
                env,
                clock,
                live: tokio::sync::Mutex::new(BTreeMap::new()),
                restoring: Arc::new(tokio::sync::Mutex::new(())),
                signal: Arc::new(Signal::default()),
                ticks: watch::channel(0).0,
                retired: watch::channel(0).0,
                written: watch::channel(Written::default()).0,
                queue: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
                tick: Mutex::new(None),
                settings_changed: Notify::new(),
                published: watch::channel(Arc::default()).0,
                sessions_read: Mutex::default(),
                counters: Counters::default(),
            }),
        }
    }

    fn settings(&self) -> TorrentSettings {
        self.inner
            .settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn rules(&self) -> Arc<[TrackerRule]> {
        Arc::clone(
            &self
                .inner
                .rules
                .read()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }

    // The failure a torrent's history keeps for it: unpacking refused it.
    fn stored_failure(&self, job_id: JobId) -> Option<String> {
        match self.inner.db.get_job_history(job_id.0) {
            Ok(row) => row
                .filter(|row| row.status == "failed")
                .and_then(|row| row.error_message),
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's history");
                None
            }
        }
    }

    // The rule a torrent announcing to `trackers` follows.
    fn rule_id<'t>(&self, trackers: impl IntoIterator<Item = &'t TrackerEntry>) -> Option<u32> {
        let rules = self.rules();
        tracker_rule_for(
            &rules,
            trackers.into_iter().map(|tracker| tracker.url.as_str()),
        )
        .map(|rule| rule.id)
    }

    /// Take the tracker rules as the database holds them now, match every
    /// torrent's trackers against them again, and apply what that changes.
    pub async fn reload_tracker_rules(&self) -> Result<(), String> {
        self.load_rules()?;
        let rules = self.rules();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let job_id = torrent.record.job_id;
                let trackers = self
                    .inner
                    .db
                    .list_torrent_trackers(job_id)
                    .map_err(state_error)?;
                torrent.rule_id = self.rule_id(&trackers);
                // An SSL torrent takes its rule's certificate as it stands:
                // its new connections and announces present it.
                let ssl = certificate_of(torrent.rule(&rules));
                if let Some(attached) = torrent.attached.as_mut()
                    && attached.handle.is_ssl()
                    && attached.ssl != ssl
                {
                    if let Err(error) = attached
                        .session
                        .set_ssl_identity(&attached.handle, ssl_identity(ssl.as_ref()))
                    {
                        tracing::warn!(job_id = job_id.0, error = %error, "an SSL torrent kept its certificate");
                    }
                    attached.ssl = ssl;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    fn load_rules(&self) -> Result<(), String> {
        let rules = self.inner.db.list_tracker_rules().map_err(state_error)?;
        *self
            .inner
            .rules
            .write()
            .unwrap_or_else(PoisonError::into_inner) = Arc::from(rules);
        Ok(())
    }

    /// The torrents the schedule's pause leaves seeding, by the tracker
    /// rule that keeps them: none while no such pause is in force.
    pub async fn schedule_seeding(&self) -> Vec<ScheduleSeeding> {
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let now = self.inner.clock.now_ms();
        let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
        for torrent in self.inner.live.lock().await.values() {
            if torrent.resumed()
                && holds.spares(queued(&settings, &rules, torrent, now).target())
                && let Some(id) = torrent.rule_id
            {
                *counts.entry(id).or_default() += 1;
            }
        }
        rules
            .iter()
            .filter_map(|rule| {
                counts.get(&rule.id).map(|&torrents| ScheduleSeeding {
                    host_pattern: rule.host_pattern.clone(),
                    torrents,
                })
            })
            .collect()
    }

    /// Take the settings as they are now. Turning torrents off closes every
    /// session and leaves each torrent stopped until they are turned on.
    pub async fn set_settings(&self, settings: TorrentSettings) {
        let on = settings.torrent.enabled;
        self.inner.sessions.set_config(settings.torrent.clone());
        let was = std::mem::replace(
            &mut *self
                .inner
                .settings
                .write()
                .unwrap_or_else(PoisonError::into_inner),
            settings,
        )
        .torrent
        .enabled;
        if on != was {
            self.turn(on).await;
        }
        // The download limit and the seeding levels take hold now, not at
        // the next tick.
        self.apply_queue().await;
        self.publish().await;
        // The routes follow the settings now too: the tick runs at once.
        self.inner.settings_changed.notify_one();
    }

    // Every torrent is read before its session changes, as `stop` reads
    // before the sessions close.
    async fn turn(&self, on: bool) {
        self.read(self.inner.clock.now_ms(), true).await;
        self.inner.sessions.turn(on).await;
        self.read_sessions().await;
        self.run(on).await;
    }

    // While torrents are on the tick runs, putting each torrent in its
    // route's session; while they are off every torrent is out of its
    // session, held off.
    async fn run(&self, on: bool) {
        if on {
            self.spawn_tick();
        } else {
            self.stop_tick();
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                self.detach(torrent).await;
                torrent.held = self.inner.sessions.off().map(Held::RouteDown);
            }
        }
        self.publish().await;
    }

    /// Put the torrents in the queue that `queue` publishes, and send its
    /// actions on a torrent here.
    ///
    /// The torrents also take their share of the global download limit from
    /// it, and follow its global pause.
    pub fn publish_to(&self, queue: SchedulerHandle) {
        let seat = Arc::new(Seat(Arc::downgrade(&self.inner)));
        queue.state().seat_owner(seat.clone());
        queue.state().seat_torrent_rate(seat);
        let mut hold = queue.state().subscribe_holds();
        *self
            .inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(queue);
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            while hold.changed().await.is_ok() {
                service.apply_queue().await;
                service.publish().await;
            }
        }));
    }

    // The holds the queue has in force.
    fn holds(&self) -> HoldSet {
        self.queue()
            .map_or_else(HoldSet::default, |queue| queue.state().holds())
    }

    fn queue(&self) -> Option<SchedulerHandle> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // The torrents, as queue items, into the published queue, and with
    // their sessions as the tick last read them, as the exporter reads
    // them: one reading of each torrent.
    async fn publish(&self) {
        let now = self.inner.clock.now_ms();
        let listed = self.listed(None, now).await;
        let sessions = self
            .inner
            .sessions_read
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let (views, torrents): (Vec<TorrentView>, Vec<TorrentSeries>) = listed.into_iter().unzip();
        let sessions = with_routes_of(sessions, &torrents);
        self.inner
            .published
            .send_replace(Arc::new(Published { torrents, sessions }));
        if let Some(queue) = self.queue() {
            queue.publish_owned_jobs(job_infos(views, now));
        }
    }

    // Each route's session as it stands, for the exporter: read by the tick,
    // wherever sessions open or close outside it, and as a torrent is given
    // its session.
    async fn read_sessions(&self) {
        self.inner
            .sessions
            .read_into(&self.inner.sessions_read)
            .await;
    }

    // `plan`'s route's open session, opened for `opener` when it is not, and
    // read before a torrent runs in it: no publish shows a torrent running
    // on a route whose session it reads as held.
    async fn open_session(
        &self,
        plan: &RoutePlan,
        opener: &Identity,
    ) -> Result<Arc<Session>, SessionError> {
        let session = self.inner.sessions.session(plan, opener).await?;
        self.read_sessions().await;
        Ok(session)
    }

    /// The torrents and their sessions as last published, with the
    /// service's event counts as they stand.
    pub fn metrics(&self) -> TorrentMetrics {
        TorrentMetrics::of(
            Arc::clone(&self.inner.published.borrow()),
            &self.inner.counters,
        )
    }

    /// Stop the tick and the job tasks, and close every session.
    pub async fn stop(&self) {
        let tasks = std::mem::take(
            &mut *self
                .inner
                .tasks
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        for task in tasks {
            task.abort();
        }
        self.stop_tick();
        // The last reading of every torrent, and what of its ledger and
        // peers changed since the last write, before the sessions close
        // (qBittorrent saves only what changed as it exits, libtorrent
        // `torrent::write_resume_data` reads the peers off the torrent).
        let now = self.inner.clock.now_ms();
        self.read(now, true).await;
        self.inner.sessions.stop_all().await;
        self.save_checkpoints(self.inner.signal.take_unsaved())
            .await;
        // libtorrent `torrent::abort` waits out the disk jobs outstanding:
        // every save queued before here, a stopped task's among them, has
        // landed when stop returns.
        if let Err(error) = self.inner.db.flush_write_queue().await {
            tracing::warn!(error = %error, "could not wait for the torrents' saves");
        }
    }

    fn stop_tick(&self) {
        if let Some(tick) = self
            .inner
            .tick
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            tick.abort();
        }
    }

    // A task the service ends at stop. Only the running ones are kept.
    fn keep(&self, task: tokio::task::JoinHandle<()>) {
        let mut tasks = self
            .inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
    }

    /// Counts the ticks run so far.
    pub fn ticks(&self) -> watch::Receiver<u64> {
        self.inner.ticks.subscribe()
    }

    /// Counts the torrents whose jobs went to history.
    pub fn retired(&self) -> watch::Receiver<u64> {
        self.inner.retired.subscribe()
    }

    /// Counts the ledgers and checkpoints written, and the checkpoints
    /// read.
    pub fn written(&self) -> watch::Receiver<Written> {
        self.inner.written.subscribe()
    }

    /// Bring back every stored torrent, then keep them: the job mail, the
    /// settling of what jobs did, and, while torrents are on, the tick,
    /// which puts them in their routes' sessions as the routes answer.
    pub async fn start(&self) -> Result<(), String> {
        self.open().await?;
        self.run(self.inner.sessions.on()).await;
        Ok(())
    }

    fn spawn_tick(&self) {
        let service = self.clone();
        let tick = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    () = service.inner.settings_changed.notified() => {}
                    // A session whose leg was revoked opens again at once.
                    () = service.inner.sessions.revoked() => {}
                }
                service.tick().await;
            }
        });
        if let Some(before) = self
            .inner
            .tick
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(tick)
        {
            before.abort();
        }
    }

    // Everything but the tick.
    pub(crate) async fn open(&self) -> Result<(), String> {
        if let Some(task) = self.inner.env.hear_alone() {
            self.keep(task);
        }
        self.spawn_settle();
        self.spawn_follow_trackers();
        self.restore().await
    }

    // A tracker's answer, and a fetch moving on, are published as the
    // sessions ring them: the exporter reads them when the views do, not
    // at the next tick.
    fn spawn_follow_trackers(&self) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.sessions.trackers_changed().await;
                service.publish().await;
            }
        }));
    }

    fn spawn_settle(&self) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.signal.wake.notified().await;
                service.settle().await;
            }
        }));
    }

    // What the jobs that spoke did: a write the job could not take fails
    // its torrent, a refusal found while unpacking stops it, and a job that
    // is done completes it. A job whose checkpoint fell
    // [`CHECKPOINT_PIECES`] behind has it written.
    async fn settle(&self) {
        let dirty = self.inner.signal.take();
        for job_id in dirty {
            let job = {
                let live = self.inner.live.lock().await;
                live.get(&job_id).and_then(|torrent| torrent.job.clone())
            };
            let Some(job) = job else { continue };
            if let Err(error) = job.lend_handed() {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent job's repair lending failed");
            }
            if let Some(outcome) = job.with(GraphJob::relocated) {
                self.moved(job_id, outcome).await;
            }
            let (refusal, done) = job.with(|job| {
                (
                    job.writer()
                        .refusal()
                        .map(|refusal| refusal.job_failure_reason()),
                    job.phase() == RootPhase::Done,
                )
            });
            if let Some(error) = refusal {
                self.refused(job_id, error).await;
            } else if let Some(error) = self.inner.signal.take_refused(job_id) {
                self.fail(job_id, error).await;
            } else if done {
                self.completed(job_id).await;
            }
        }
        self.save_checkpoints(self.inner.signal.take_due()).await;
        self.apply_picking().await;
        self.publish().await;
    }

    // Write the checkpoints of `jobs`, each with its torrent's ledger.
    async fn save_checkpoints(&self, jobs: BTreeSet<JobId>) {
        let now = self.inner.clock.now_ms();
        let saves: Vec<CheckpointSave> = {
            let mut live = self.inner.live.lock().await;
            jobs.into_iter()
                .filter_map(|job_id| self.queue_checkpoint(live.get_mut(&job_id)?, now))
                .collect()
        };
        for save in saves {
            self.checkpoint_landed(save).await;
        }
    }

    // Queue a write of what changed in the torrent's ledger and peers
    // (libtorrent `torrent::write_resume_data`), in the step that takes it.
    // The one writer of both: every ban, every peer list and every ledger
    // goes through here under the torrent's lock.
    fn queue_resume(&self, torrent: &mut Live, now: i64, at_once: bool) -> Option<Landing> {
        let Resume {
            counters,
            peers,
            lost,
        } = torrent.take_resume(now, at_once)?;
        let job_id = torrent.record.job_id;
        let landed = self.queue_save(job_id, "torrent_resume", move |db| {
            db.save_torrent_resume(job_id, counters.as_ref(), peers.as_ref())
        });
        Some(Landing {
            job_id,
            landed,
            lost,
        })
    }

    // Wait for a write of a torrent's ledger and peers to land. One that
    // failed leaves what the database holds unknown, so the next writes
    // all of it.
    async fn landed(&self, landing: Option<Landing>) -> Result<(), String> {
        let Some(Landing {
            job_id,
            landed,
            lost,
        }) = landing
        else {
            return Ok(());
        };
        let result = match landed.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(state_error(error)),
            Err(error) => Err(error.to_string()),
        };
        match &result {
            Ok(()) => self
                .inner
                .written
                .send_modify(|written| written.ledgers += 1),
            Err(error) => {
                lost.store(true, Ordering::Release);
                tracing::warn!(job_id = job_id.0, %error, "could not keep a torrent's ledger and peers");
            }
        }
        result
    }

    // Queue a save of a torrent's state on its job's lane of the database's
    // ordered writer (libtorrent sends every resume-data write of a torrent
    // through its disk job queue). The caller takes the state it saves in
    // the same step, with no await between, so saves land in the order
    // their states were taken: one taken later lands later, and the save of
    // a task stopped while it waits still lands before the next one. The
    // receiver has what the save returned once it has landed.
    fn queue_save<T: Send + 'static>(
        &self,
        job_id: JobId,
        label: &'static str,
        save: impl FnOnce(&Database) -> Result<T, StateError> + Send + 'static,
    ) -> oneshot::Receiver<Result<T, StateError>> {
        let (landed, receiver) = oneshot::channel();
        if let Err(error) = self.inner.db.try_queue_job_write(job_id, label, move |db| {
            let _ = landed.send(save(db));
            Ok(())
        }) {
            tracing::warn!(job_id = job_id.0, label, error = %error, "could not queue a torrent's save");
        }
        receiver
    }

    // Queue a write of what the job's checkpoint changed since the database
    // last held it, the rows that differ as the pipeline writes its jobs',
    // with what changed in the torrent's ledger and peers as they stand now,
    // in one transaction: what a crash recovers is the pieces and the
    // counters of one moment. The ledger is taken in the step that queues
    // it, as every write of it is. None when the torrent has no job.
    fn queue_checkpoint(&self, torrent: &mut Live, now: i64) -> Option<CheckpointSave> {
        let job_id = torrent.record.job_id;
        let pending = torrent.job.as_ref()?.with(|job| {
            let pending = job.pending().clone();
            pending.put(job).then_some(pending)
        });
        torrent.settle(now);
        let resume = torrent.take_resume(now, false);
        if pending.is_none() && resume.is_none() {
            return Some(CheckpointSave {
                job_id,
                landed: None,
                pending: None,
                lost: None,
            });
        }
        let (counters, peers, lost) = resume.map_or((None, None, None), |resume| {
            (resume.counters, resume.peers, Some(resume.lost))
        });
        let writer = pending.clone();
        let landed = self.queue_save(job_id, "torrent_checkpoint", move |db| {
            let delta = writer.as_ref().and_then(checkpoint::Pending::take);
            let written = db.save_torrent_checkpoint(
                job_id,
                delta.map(Arc::new),
                counters.as_ref(),
                peers.as_ref(),
            );
            if written.is_err()
                && let Some(writer) = &writer
            {
                writer.forget();
            }
            written
        });
        Some(CheckpointSave {
            job_id,
            landed: Some(landed),
            pending,
            lost,
        })
    }

    // Wait for a checkpoint's write to land. One that failed leaves what
    // the database holds of the ledger unknown, so the next writes all of
    // it.
    async fn checkpoint_landed(&self, save: CheckpointSave) {
        let CheckpointSave {
            job_id,
            landed,
            pending,
            lost,
        } = save;
        if let Some(landed) = landed {
            let failed = match landed.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => {
                    if let Some(pending) = &pending {
                        pending.forget();
                    }
                    Some(error.to_string())
                }
            };
            match (failed, lost) {
                (Some(error), lost) => {
                    if let Some(lost) = lost {
                        lost.store(true, Ordering::Release);
                    }
                    tracing::warn!(job_id = job_id.0, %error, "could not checkpoint a torrent job");
                }
                (None, Some(_)) => self
                    .inner
                    .written
                    .send_modify(|written| written.ledgers += 1),
                (None, None) => {}
            }
        }
        self.inner
            .written
            .send_modify(|written| written.checkpoints += 1);
    }

    // The job is done: its completion is written on its own, so an intent
    // written beside it keeps it, and the ledger and peers as they stand.
    async fn completed(&self, job_id: JobId) {
        let now = self.inner.clock.now_ms();
        let landing = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            if torrent.complete() {
                return;
            }
            torrent.settle(now);
            torrent.record.completed_at_ms = Some(now);
            self.inner.counters.event(TorrentEvent::Completed);
            if let Err(error) = self.inner.db.set_torrent_completed(job_id, now) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's completion");
            }
            self.queue_resume(torrent, now, true)
        };
        let _ = self.landed(landing).await;
    }

    fn storage(&self, dir: PathBuf, layout: &PieceLayout, unpack: bool) -> Storage {
        let policy = self
            .inner
            .db
            .post_processing_settings()
            .map(Arc::new)
            .map_err(state_error);
        let declared = (0..layout.volume_count())
            .filter_map(|volume| layout.volume(VolumeIndex(volume as u32)))
            .map(|file| file.length)
            .collect();
        let env = &self.inner.env;
        Storage {
            working_dir: dir,
            accountant: Arc::clone(&env.accountant),
            free_disk: env.free_disk.clone(),
            slots: env.slots,
            mail: env.mail.clone(),
            passwords: Vec::new(),
            caps: Caps::new(Arc::clone(&env.limits), policy, declared),
            nesting: limits::NESTING,
            unpack,
        }
    }

    // What the torrent's checkpoint gives back. It reads the checkpoint and
    // the files it covers, so it runs off the tick's task.
    fn restored(
        &self,
        job_id: JobId,
        dir: PathBuf,
        layout: &Arc<PieceLayout>,
        unpack: bool,
    ) -> FromCheckpoint {
        let loaded = self.inner.db.load_graph_checkpoint(job_id);
        self.inner
            .written
            .send_modify(|written| written.checkpoints_read += 1);
        let rows = match loaded {
            Ok(Some(rows)) => rows,
            Ok(None) => return FromCheckpoint::Nothing,
            Err(error) => return FromCheckpoint::Unread(error.to_string()),
        };
        let restored = checkpoint::snapshot(&rows)
            .map_err(|error| error.to_string())
            .and_then(|snapshot| {
                GraphJob::restore_torrent(
                    &snapshot,
                    Arc::clone(layout),
                    job_id.0,
                    self.storage(dir, layout, unpack),
                    self.inner.env.slots,
                )
                .map_err(|error| error.to_string())
            });
        match restored {
            Ok(restored) => {
                // The database holds these rows: the next checkpoint writes
                // only what changes from them.
                restored.job.pending().written(rows);
                FromCheckpoint::Restored(restored)
            }
            Err(error) => FromCheckpoint::Failed(error),
        }
    }

    // The root the torrent's checkpoint keeps, from the database alone: what
    // its job holds once restored from it. None without a checkpoint, or
    // with one that does not restore: the job then starts fresh, and its
    // restore says why.
    fn kept_root(&self, job_id: JobId, layout: &Arc<PieceLayout>) -> Option<PieceRoot> {
        let root = self
            .inner
            .db
            .load_graph_root(job_id)
            .map_err(|error| error.to_string())
            .and_then(|rows| {
                let Some(rows) = rows else { return Ok(None) };
                let facts = checkpoint::root(&rows)
                    .map_err(|error| error.to_string())?
                    .root
                    .read::<PieceRoot>()
                    .map_err(|error| error.to_string())?;
                PieceRoot::restore(Arc::clone(layout), &facts)
                    .map(Some)
                    .map_err(|error| error.to_string())
            });
        root.unwrap_or_else(|error| {
            tracing::debug!(job_id = job_id.0, %error, "a torrent's stored root does not read");
            None
        })
    }

    // The job of a torrent with metainfo: the one its checkpoint gave back,
    // or a new one. A checkpoint that does not restore is said, with why,
    // and the torrent starts as a new one.
    fn graph_job(
        &self,
        job_id: JobId,
        dir: PathBuf,
        layout: &Arc<PieceLayout>,
        unpack: bool,
        checkpoint: FromCheckpoint,
    ) -> GraphJob<PieceRoot> {
        match checkpoint {
            // What the checkpoint held, and what of it was not on disk. A
            // piece that verified after the checkpoint was written is fetched
            // again too, but nothing short of a check of the files can say
            // which those are: every file has its full length from its first
            // piece on.
            FromCheckpoint::Restored(restored) => {
                tracing::debug!(
                    job_id = job_id.0,
                    read_bytes = restored.read,
                    in_hand = restored.job.have().count_ones(),
                    missing = restored.refetched.len(),
                    "a torrent's job is restored from its checkpoint"
                );
                return restored.job;
            }
            FromCheckpoint::Nothing => {}
            FromCheckpoint::Unread(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's checkpoint could not be read; it starts fresh");
            }
            FromCheckpoint::Failed(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's checkpoint did not restore; it starts fresh");
            }
        }
        GraphJob::for_torrent(
            job_id.0,
            Arc::clone(layout),
            self.storage(dir, layout, unpack),
        )
    }

    // The job joined to what feeds it: the runs' mail, and the intake the
    // session hands its pieces to.
    fn job(&self, job_id: JobId, job: GraphJob<PieceRoot>) -> (Arc<TorrentJob>, Arc<Intake>) {
        let slots = self.inner.env.slots;
        let env = &self.inner.env;
        let job = Arc::new(TorrentJob::new(
            job,
            Arc::clone(&env.repairs),
            Arc::clone(&self.inner.signal),
            slots,
        ));
        env.outside.join(
            job_id.0,
            Arc::new(Hearer {
                job: Arc::clone(&job),
                signal: Arc::clone(&self.inner.signal),
            }),
        );
        let signal = Arc::clone(&self.inner.signal);
        let intake = Arc::new(Intake::new(
            Arc::clone(&job) as Arc<dyn Pieces>,
            Room::new(Arc::clone(&env.limits), env.free_disk.clone()),
            Box::new(move |heard| signal.heard(job_id, heard)),
        ));
        (job, intake)
    }

    // Where a torrent's bytes in memory are counted: the accountant every
    // job shares, under the namespace of the job's root.
    fn resident(&self, job_id: JobId) -> Holds {
        Holds {
            accountant: Arc::clone(&self.inner.env.accountant),
            namespace: weaver_graph::holds::HoldsNamespace {
                scope: job_id.0,
                node: weaver_graph::NodeId::ROOT,
            },
        }
    }

    // The torrent's metainfo and its job over it, at once.
    fn give_metainfo(&self, torrent: &mut Live, parsed: &Parsed) {
        self.describe(torrent, parsed);
        let (job_id, dir, unpack) = (
            torrent.record.job_id,
            storage_dir(torrent),
            unpacks(torrent),
        );
        let checkpoint = self.restored(job_id, dir.clone(), &parsed.layout, unpack);
        let job = self.graph_job(job_id, dir, &parsed.layout, unpack, checkpoint);
        self.give_job(torrent, job);
    }

    // The job joined, and what the torrent shows of it.
    fn give_job(&self, torrent: &mut Live, job: GraphJob<PieceRoot>) {
        let (job, intake) = self.job(torrent.record.job_id, job);
        torrent.file_have = job.with(|job| job.file_have());
        torrent.job = Some(job);
        torrent.intake = Some(intake);
    }

    // What the metainfo says of the torrent, without its job. Whether it
    // unpacks is fixed here and kept, so a restart builds the same tree.
    fn describe(&self, torrent: &mut Live, parsed: &Parsed) {
        let job_id = torrent.record.job_id;
        if torrent.record.unpack.is_none() {
            let unpack = self
                .settings()
                .unpack(&torrent.record, torrent.rule(&self.rules()));
            if let Err(error) = self.inner.db.set_torrent_unpack(job_id, unpack) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record whether a torrent unpacks");
            }
            torrent.record.unpack = Some(unpack);
        }
        torrent.names = parsed.manifest.paths.clone();
        torrent.total = parsed.total;
        torrent.layout = Some(Arc::clone(&parsed.layout));
        let stored = self.inner.db.torrent_file_priorities(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
            Vec::new()
        });
        torrent.levels = parsed
            .layout
            .files()
            .enumerate()
            .map(|(index, file)| {
                let level = stored.get(index).copied().unwrap_or_default();
                level.held_by(file.padding)
            })
            .collect();
        torrent.about = parsed.about.clone();
        torrent.private = parsed.private;
    }

    // The jobs of the torrents brought back at start with their records
    // only, each restored from its checkpoint in turn. A torrent reads as it
    // was kept until its job is back.
    async fn restore_jobs(&self) {
        let waiting: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.job.is_none() && torrent.layout.is_some())
            .map(|torrent| torrent.record.job_id)
            .collect();
        for job_id in waiting {
            self.restore_job(job_id).await;
        }
    }

    // The job of a torrent that waits for one, restored on a blocking thread
    // under no lock but the one that keeps restores one at a time, and given
    // to the torrent if it still waits: one removed meanwhile drops it. A
    // restore that panics is a checkpoint that does not restore.
    async fn restore_job(&self, job_id: JobId) {
        let guard = Arc::clone(&self.inner.restoring).lock_owned().await;
        let waiting = {
            let live = self.inner.live.lock().await;
            live.get(&job_id)
                .filter(|torrent| torrent.job.is_none())
                .and_then(|torrent| {
                    Some((
                        storage_dir(torrent),
                        Arc::clone(torrent.layout.as_ref()?),
                        unpacks(torrent),
                    ))
                })
        };
        let Some((dir, layout, unpack)) = waiting else {
            return;
        };
        let restoring = {
            let (service, dir, layout) = (self.clone(), dir.clone(), Arc::clone(&layout));
            tokio::task::spawn_blocking(move || {
                let checkpoint = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.restored(job_id, dir, &layout, unpack)
                }))
                .unwrap_or_else(|_| FromCheckpoint::Failed("the restore panicked".to_string()));
                (guard, checkpoint)
            })
        };
        // The blocking task ends only with its result, or with the runtime.
        let Ok((_restoring, checkpoint)) = restoring.await else {
            return;
        };
        let job = self.graph_job(job_id, dir, &layout, unpack, checkpoint);
        if let Some(torrent) = self
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .filter(|torrent| torrent.job.is_none())
        {
            self.give_job(torrent, job);
        }
    }

    // Add a torrent. A metainfo becomes a job at once; a magnet needs its
    // route's session to fetch the metainfo, and becomes one when it has.
    /// Whether the route a torrent in `scope` would take carries DHT, so a
    /// magnet with no trackers can find its peers there.
    pub async fn carries_dht(&self, scope: TorrentScope) -> bool {
        let Ok(plan) = self.inner.sessions.routes().plan(scope) else {
            return false;
        };
        self.inner.sessions.carries_dht(&plan).await
    }

    /// The same loaded IP filter used when a route opens its session.
    pub async fn ip_filter(
        &self,
        torrent: &TorrentConfig,
    ) -> Result<weaver_torrent::IpFilter, String> {
        self.inner.sessions.ip_filter(torrent).await
    }

    pub async fn add(&self, request: AddRequest) -> Result<JobId, AddError> {
        if !self.inner.sessions.on() {
            return Err(AddError::Off);
        }
        let settings = self.settings();
        let category = match request
            .category
            .as_deref()
            .filter(|name| !name.trim().is_empty())
        {
            Some(name) => Some(
                resolve_category_config(&settings.categories, name)
                    .cloned()
                    .ok_or_else(|| AddError::Invalid(format!("no category {name}")))?,
            ),
            None => None,
        };
        match request.source.clone() {
            AddSource::Metainfo(bytes) => {
                let bytes = Bytes::from(bytes);
                let parsed = parse(&bytes)?;
                self.admit(&parsed.manifest)?;
                self.duplicate(parsed.hashes)?;
                let job_id = self.reserve()?;
                let dir = self
                    .claim_dir(&settings, category.as_ref(), &parsed.manifest, job_id)
                    .await?;
                let record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    parsed.hashes,
                    parsed.manifest.name.clone(),
                    TorrentSource::Metainfo,
                    Some(bytes),
                    Some(dir.clone()),
                );
                let mut torrent = match self.insert(record, &parsed.trackers) {
                    Ok(torrent) => torrent,
                    Err(error) => {
                        give_back(&dir).await;
                        return Err(error);
                    }
                };
                self.give_metainfo(&mut torrent, &parsed);
                self.inner.live.lock().await.insert(job_id, torrent);
                self.apply_queue().await;
                self.attach(job_id, None).await;
                self.inner.counters.event(TorrentEvent::Added);
                self.publish().await;
                Ok(job_id)
            }
            AddSource::Magnet(uri) => {
                let magnet = Magnet::parse(&uri)
                    .map_err(|error| AddError::Invalid(format!("not a magnet: {error}")))?;
                let hashes = magnet.info_hashes();
                self.duplicate(hashes)?;
                let trackers: Vec<TrackerEntry> = magnet
                    .trackers
                    .iter()
                    .enumerate()
                    .filter_map(|(tier, url)| tracker(tier as u32, url.as_bytes()))
                    .collect();
                let rules = self.rules();
                let rule =
                    tracker_rule_for(&rules, trackers.iter().map(|tracker| tracker.url.as_str()));
                let scope = settings.scope(category.as_ref().map(|category| category.id), rule);
                let plan = self
                    .inner
                    .sessions
                    .routes()
                    .plan(scope)
                    .map_err(AddError::NoNetwork)?;
                let session = self
                    .inner
                    .sessions
                    .session(&plan, &client_of(rule).identity())
                    .await
                    .map_err(|error| AddError::NoNetwork(error.to_string()))?;
                if magnet.trackers.is_empty() && session.get_dht().is_none() {
                    return Err(AddError::NoNetwork(
                        "a magnet with no trackers needs DHT, which this route does not carry"
                            .into(),
                    ));
                }
                let job_id = self.reserve()?;
                let name = magnet
                    .name
                    .clone()
                    .unwrap_or_else(|| hashes.best().as_string());
                let record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    hashes,
                    name,
                    TorrentSource::Magnet,
                    None,
                    None,
                );
                let mut torrent = self.insert(record, &trackers)?;
                let identity = identity_of(torrent.rule(&rules), &torrent.identity);
                let terms = PeerTerms::of(torrent.rule(&rules));
                torrent.route = Some(plan.key.clone());
                self.inner.live.lock().await.insert(job_id, torrent);
                self.spawn_resolve(job_id, plan, uri, None, identity, terms)
                    .await;
                self.inner.counters.event(TorrentEvent::Added);
                self.publish().await;
                Ok(job_id)
            }
        }
    }

    fn admit(&self, manifest: &Manifest) -> Result<(), AddError> {
        let policy = self
            .inner
            .db
            .post_processing_settings()
            .map_err(|error| AddError::State(state_error(error)))?;
        Ok(admission::admit(&policy, manifest)?)
    }

    fn duplicate(&self, hashes: InfoHashes) -> Result<(), AddError> {
        match self.inner.db.get_torrent_by_infohash(&infohashes(hashes)) {
            Ok(Some(existing)) => Err(AddError::Duplicate(existing.job_id)),
            Ok(None) => Ok(()),
            Err(error) => Err(AddError::State(state_error(error))),
        }
    }

    fn reserve(&self) -> Result<JobId, AddError> {
        self.inner
            .db
            .reserve_next_job_id()
            .map_err(|error| AddError::State(state_error(error)))
    }

    // The torrent's directory under its category's parent, taken on disk
    // now: its output name, suffixed with the job's id when that is taken.
    async fn claim_dir(
        &self,
        settings: &TorrentSettings,
        category: Option<&CategoryConfig>,
        manifest: &Manifest,
        job_id: JobId,
    ) -> Result<String, AddError> {
        let parent = completion_parent(
            &settings.complete_dir,
            &settings.categories,
            category.map(|category| category.name.as_str()),
        )
        .map_err(AddError::Invalid)?;
        claim_complete_destination_path(
            &parent,
            &admission::output_name(manifest),
            job_id,
            &HashSet::new(),
        )
        .await
        .map(|dir| dir.to_string_lossy().into_owned())
        .map_err(|error| {
            AddError::State(format!("could not make the torrent's directory: {error}"))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn record(
        &self,
        job_id: JobId,
        request: &AddRequest,
        category: Option<&CategoryConfig>,
        hashes: InfoHashes,
        name: String,
        source: TorrentSource,
        metainfo: Option<Bytes>,
        storage_dir: Option<String>,
    ) -> TorrentRecord {
        TorrentRecord {
            job_id,
            infohash_v1: hashes.v1.map(|hash| hash.0),
            infohash_v2: hashes.v2.map(|hash| hash.0),
            name,
            source,
            metainfo,
            category_id: category.map(|category| category.id),
            added_at_ms: self.inner.clock.now_ms(),
            intent: if request.paused {
                TorrentIntent::Paused
            } else {
                TorrentIntent::Resumed
            },
            forced: request.forced,
            completed_at_ms: None,
            storage_dir,
            policy_override: request.policy_override.clone(),
            unpack: request.unpack,
            metadata: request.metadata.clone(),
            upload_limit: request.upload_limit,
            download_limit: request.download_limit,
            sequential: false,
            first_last_piece_first: false,
        }
    }

    // Keep `record` with its trackers: a torrent new to the database, under
    // the rule its trackers match, and forced when that rule admits its
    // torrents so.
    fn insert(
        &self,
        mut record: TorrentRecord,
        trackers: &[TrackerEntry],
    ) -> Result<Live, AddError> {
        let rules = self.rules();
        let rule = tracker_rule_for(&rules, trackers.iter().map(|tracker| tracker.url.as_str()));
        record.forced |= rule.is_some_and(|rule| rule.forced);
        let rule_id = rule.map(|rule| rule.id);
        match self.inner.db.insert_torrent(&record, trackers) {
            Ok(TorrentInsert::Inserted) => {
                let now = self.inner.clock.now_ms();
                let mut torrent = Live::new(
                    record,
                    TorrentCounters::default(),
                    ResumePeers::default(),
                    now,
                );
                torrent.rule_id = rule_id;
                Ok(torrent)
            }
            Ok(TorrentInsert::Duplicate(existing)) => Err(AddError::Duplicate(existing)),
            Err(error) => Err(AddError::State(state_error(error))),
        }
    }

    // Fetch a magnet's metainfo on its route's session, once the route has
    // one, then admit it as any other. A route that gives no session holds
    // the magnet, which resolves again at a later tick. Until the fetch
    // ends, removing or pausing the torrent ends it (see `Resolving`). A
    // magnet already fetching goes on with that fetch; a paused one waits
    // for its resume to fetch.
    async fn spawn_resolve(
        &self,
        job_id: JobId,
        plan: RoutePlan,
        uri: String,
        tracker_tiers: Option<Vec<Vec<Tracker>>>,
        identity: Identity,
        terms: PeerTerms,
    ) {
        let service = self.clone();
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return;
        };
        match torrent.resolving {
            Some(Resolving::Fetching(_)) => return,
            _ if !torrent.resumed() => {
                torrent.resolving = Some(Resolving::Paused);
                return;
            }
            _ => {}
        }
        let task = tokio::spawn(async move {
            service
                .resolve_on(job_id, plan, uri, tracker_tiers, identity, terms)
                .await;
            // The fetch's announce table is the torrent's until it is
            // attached, which reads the same table, or the fetch's
            // trackers are let go.
            service.fetching(job_id, None).await;
        });
        torrent.resolving = Some(Resolving::Fetching(task.abort_handle()));
        drop(live);
        self.keep(task);
    }

    async fn resolve_on(
        &self,
        job_id: JobId,
        plan: RoutePlan,
        uri: String,
        tracker_tiers: Option<Vec<Vec<Tracker>>>,
        identity: Identity,
        terms: PeerTerms,
    ) {
        let session = match self.open_session(&plan, &identity).await {
            Ok(session) => session,
            Err(error) => {
                self.fetch_ended(job_id).await;
                self.inner.counters.fetch(FetchOutcome::NoSession);
                return self.held(job_id, Some(plan.key), held_by(error)).await;
            }
        };
        let mut options = AddTorrentOptions {
            list_only: true,
            tracker_tiers,
            identity: Some(identity.clone()),
            ..Default::default()
        };
        terms.apply(&mut options);
        let (fetch_tx, fetch_rx) = oneshot::channel();
        options.metadata_fetch = Some(fetch_tx);
        let fetch = session.add_torrent(AddTorrent::from_url(uri), Some(options));
        let fetching = async {
            if let Ok(fetch) = fetch_rx.await {
                self.fetching(job_id, Some(fetch)).await;
            }
        };
        let (listed, ()) = tokio::join!(fetch, fetching);
        // A fetch ended first was counted where it was ended.
        if !self.fetch_ended(job_id).await {
            return;
        }
        let listed = match listed {
            Ok(AddTorrentResponse::ListOnly(listed)) => listed,
            Ok(_) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, "the session did not return the metainfo".into())
                    .await;
                return;
            }
            Err(error) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, format!("{error:#}")).await;
                return;
            }
        };
        // The fetch's trackers go to the attach, which takes them over or
        // lets them go.
        let fetched = listed.trackers.map(|trackers| Fetched {
            session,
            identity,
            trackers,
        });
        match self.resolved(job_id, listed.torrent_bytes, fetched).await {
            Ok(()) => self.inner.counters.fetch(FetchOutcome::Resolved),
            Err(error) => {
                self.inner.counters.fetch(FetchOutcome::Failed);
                self.fail(job_id, error.to_string()).await;
            }
        }
    }

    // A magnet's metadata fetch has ended: false when the torrent is gone,
    // or a pause ended the fetch first.
    async fn fetch_ended(&self, job_id: JobId) -> bool {
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return false;
        };
        match torrent.resolving.take() {
            Some(Resolving::Fetching(_)) => true,
            paused => {
                torrent.resolving = paused;
                false
            }
        }
    }

    // A magnet's metadata fetch becomes `fetch`. A fetch is the torrent's
    // only while it runs: one the pause ended still gives is not shown.
    // Published either way: a fetch begun shows its phase, and one ended
    // what it ended in, the metainfo it gave among it.
    async fn fetching(&self, job_id: JobId, fetch: Option<weaver_torrent::MetadataFetch>) {
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id)
            && (fetch.is_none() || matches!(torrent.resolving, Some(Resolving::Fetching(_))))
        {
            torrent.fetching = fetch;
        }
        self.publish().await;
    }

    async fn resolved(
        &self,
        job_id: JobId,
        bytes: Bytes,
        fetched: Option<Fetched>,
    ) -> Result<(), AddError> {
        let parsed = parse(&bytes)?;
        self.admit(&parsed.manifest)?;
        let settings = self.settings();
        let Some((category_id, known)) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| (torrent.record.category_id, hashes_of(&torrent.record)))
        else {
            return Ok(());
        };
        // A hash only the metainfo names may be another torrent's: a hybrid
        // added once by each of its hashes.
        self.duplicate(InfoHashes {
            v1: parsed.hashes.v1.filter(|_| known.v1.is_none()),
            v2: parsed.hashes.v2.filter(|_| known.v2.is_none()),
        })?;
        let category = settings.category(category_id).cloned();
        let dir = self
            .claim_dir(&settings, category.as_ref(), &parsed.manifest, job_id)
            .await?;
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            drop(live);
            give_back(&dir).await;
            return Ok(());
        };
        torrent.record.infohash_v1 = parsed.hashes.v1.map(|hash| hash.0);
        torrent.record.infohash_v2 = parsed.hashes.v2.map(|hash| hash.0);
        torrent.record.name = parsed.manifest.name.clone();
        torrent.record.storage_dir = Some(dir.clone());
        torrent.record.metainfo = Some(bytes.clone());
        // The trackers the magnet listed stay, edits included, each as
        // whoever named it; the metainfo's own join after them as the
        // torrent's.
        let mut trackers = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .map_err(|error| AddError::State(state_error(error)))?;
        trackers.extend(parsed.trackers.iter().cloned());
        let trackers = tidy_trackers(trackers);
        torrent.rule_id = self.rule_id(&trackers);
        self.inner
            .db
            .set_torrent_metainfo(
                job_id,
                torrent.record.infohash_v1,
                torrent.record.infohash_v2,
                &parsed.manifest.name,
                &bytes,
                &dir,
                &trackers,
            )
            .map_err(|error| AddError::State(state_error(error)))?;
        self.give_metainfo(torrent, &parsed);
        drop(live);
        // An SSL torrent's trackers hear it only as it goes live, under its
        // own identity: the fetch's trackers are let go here.
        let fetched = fetched.filter(|_| !parsed.ssl);
        self.attach(job_id, fetched).await;
        Ok(())
    }

    // Fetch a stored magnet's metainfo again, on its route's session once
    // the route is up.
    async fn resolve(&self, job_id: JobId) {
        let rules = self.rules();
        let Some((record, rule, own)) = self.inner.live.lock().await.get(&job_id).map(|torrent| {
            (
                torrent.record.clone(),
                torrent.rule(&rules),
                torrent.identity.clone(),
            )
        }) else {
            return;
        };
        let scope = self.settings().scope(record.category_id, rule);
        let plan = match self.inner.sessions.routes().plan(scope) {
            Ok(plan) => plan,
            Err(reason) => {
                return self
                    .held(
                        job_id,
                        None,
                        Held::RouteDown(RouteFailure::with_detail(
                            TorrentRouteReason::RoutePlanFailed,
                            reason,
                        )),
                    )
                    .await;
            }
        };
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.route = Some(plan.key.clone());
            torrent.held = None;
        }
        let uri = Magnet::of(hashes_of(&record), None, Vec::new()).to_string();
        // The trackers it lists, in their tiers: a magnet's fetch announces
        // to tiers, not to a bare list.
        let tiers = tracker_tiers(
            &self
                .inner
                .db
                .list_torrent_trackers(job_id)
                .unwrap_or_default(),
        );
        self.spawn_resolve(
            job_id,
            plan,
            uri,
            Some(tiers),
            identity_of(rule, &own),
            PeerTerms::of(rule),
        )
        .await;
    }

    // Put a torrent in its route's session, or hold it while the route is
    // down. It runs under its own identity, and counts on from what it had
    // moved in the session it ran in before. A torrent whose metadata was
    // just `fetched` takes over the fetch's trackers when it starts running
    // now, on the fetch's session under the identity the fetch announced, so
    // they hear one torrent; otherwise the fetch's trackers are let go,
    // which sends them `stopped`, and the run starts its own.
    async fn attach(&self, job_id: JobId, fetched: Option<Fetched>) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let (
            scope,
            bytes,
            layout,
            intake,
            dir,
            paused,
            ratelimits,
            have,
            identity,
            totals,
            terms,
            ssl,
            picking,
            saved,
            listed,
        ) = {
            let live = self.inner.live.lock().await;
            let Some(torrent) = live.get(&job_id) else {
                return;
            };
            if torrent.attached.is_some() {
                return;
            }
            let (Some(bytes), Some(layout), Some(intake), Some(job)) = (
                torrent.record.metainfo.clone(),
                torrent.layout.clone(),
                torrent.intake.clone(),
                torrent.job.as_ref(),
            ) else {
                return;
            };
            let rule = torrent.rule(&rules);
            let ratelimits = ratelimits_of(
                &torrent.record,
                settings.policy(&torrent.record, rule),
                rule,
            );
            (
                settings.scope(torrent.record.category_id, rule),
                bytes,
                layout,
                intake,
                torrent.record.storage_dir.clone().unwrap_or_default(),
                !torrent.resumed()
                    || torrent.queued
                    || self
                        .holds()
                        .reason_for(queued(&settings, &rules, torrent, now).target())
                        .is_some(),
                ratelimits,
                job.with(|job| initial_have(job)),
                identity_of(rule, &torrent.identity),
                torrent.seen,
                PeerTerms::of(rule),
                certificate_of(rule),
                (
                    fetch_levels(torrent, job),
                    torrent.record.sequential,
                    torrent.record.first_last_piece_first,
                ),
                // libtorrent `torrent::read_resume_data`: the saved peers
                // are known from the start and the saved bans hold.
                torrent.resume.clone(),
                // The trackers it lists, read under the lock a tracker edit
                // writes under.
                self.inner.db.list_torrent_trackers(job_id),
            )
        };
        // The trackers the torrent lists now, each with who named it, stand
        // in for its metainfo's.
        let listed = match listed {
            Ok(listed) => listed,
            Err(error) => {
                self.fail(job_id, state_error(error)).await;
                return;
            }
        };
        let plan = match self.inner.sessions.routes().plan(scope) {
            Ok(plan) => plan,
            Err(reason) => {
                self.held(
                    job_id,
                    None,
                    Held::RouteDown(RouteFailure::with_detail(
                        TorrentRouteReason::RoutePlanFailed,
                        reason,
                    )),
                )
                .await;
                return;
            }
        };
        let session = match self.open_session(&plan, &identity).await {
            Ok(session) => session,
            Err(error) => {
                self.held(job_id, Some(plan.key), held_by(error)).await;
                return;
            }
        };
        let (identity, tracker_handover) = match fetched {
            Some(fetched)
                if !paused
                    && Arc::ptr_eq(&fetched.session, &session)
                    && fetched.identity == identity =>
            {
                (fetched.identity, Some(fetched.trackers))
            }
            _ => (identity, None),
        };
        let factory =
            PieceStorageFactory::new(layout, intake as Arc<dyn Pieces>, self.resident(job_id));
        let mut options = AddTorrentOptions {
            paused,
            overwrite: true,
            output_folder: Some(dir),
            storage_factory: Some(factory.boxed_factory()),
            ratelimits,
            identity: Some(identity),
            totals,
            tracker_tiers: Some(tracker_tiers(&listed)),
            initial_peers: (!saved.peers.is_empty()).then(|| saved.peers.clone()),
            banned_peers: saved.banned.clone(),
            file_priorities: Some(picking.0),
            sequential: picking.1,
            first_last_piece_first: picking.2,
            tracker_handover,
            ..Default::default()
        };
        terms.apply(&mut options);
        options.ssl_identity = ssl_identity(ssl.as_ref());
        apply_initial_have(&mut options, have);
        let added = session
            .add_torrent(AddTorrent::from_bytes(bytes), Some(options))
            .await;
        let handle = match added.map(AddTorrentResponse::into_handle) {
            Ok(Some(handle)) => handle,
            Ok(None) => {
                self.fail(job_id, "the session did not take the torrent".into())
                    .await;
                return;
            }
            Err(error) => {
                self.fail(job_id, format!("{error:#}")).await;
                return;
            }
        };
        let mut live = self.inner.live.lock().await;
        match live.get_mut(&job_id) {
            Some(torrent) => {
                // A ban made while the session took the torrent holds in
                // it too.
                let since: Vec<IpAddr> = torrent
                    .resume
                    .banned
                    .iter()
                    .filter(|ip| !saved.banned.contains(ip))
                    .copied()
                    .collect();
                if !since.is_empty() {
                    handle.ban_peers(&since);
                }
                // A tracker edit made while the session took the torrent,
                // which found it in no session, reaches it too.
                match self.inner.db.list_torrent_trackers(job_id) {
                    Ok(trackers) if trackers != listed => {
                        handle.replace_trackers(tracker_tiers(&trackers));
                    }
                    Ok(_) => {}
                    Err(error) => {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's trackers");
                    }
                }
                torrent.settle(self.inner.clock.now_ms());
                let leg = self
                    .queue()
                    .and_then(|queue| queue.proxy_runtime())
                    .map(|proxies| OnLeg {
                        network: proxies.network.clone(),
                        egress_id: plan.egress_id,
                        own_sockets: plan.kind.own_sockets(),
                        tenancy: proxies.network.tenant_torrent(
                            Consumer::Torrent(scope),
                            crate::torrents::sessions::TORRENT_LEG,
                            LegTorrent {
                                handle: handle.clone(),
                                own_sockets: plan.kind.own_sockets(),
                            },
                        ),
                    });
                torrent.attached = Some(Attached {
                    session,
                    handle,
                    terms,
                    ssl,
                    leg,
                });
                torrent.route = Some(plan.key);
                torrent.held = None;
                torrent.run_error = None;
            }
            None => {
                drop(live);
                if let Err(error) = session
                    .delete(TorrentIdOrHash::Id(handle.id()), false)
                    .await
                {
                    tracing::debug!(job_id = job_id.0, error = %error, "a removed torrent was already gone from its session");
                }
            }
        }
    }

    async fn held(&self, job_id: JobId, key: Option<RouteKey>, held: Held) {
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.settle(self.inner.clock.now_ms());
            torrent.route = key.or(torrent.route.take());
            torrent.held = Some(held);
        }
    }

    // Take a torrent out of its session, its checkpoint written first when
    // it is behind the job, and what changed in its ledger and peers taken
    // before the session lets it go.
    async fn detach(&self, torrent: &mut Live) {
        let job_id = torrent.record.job_id;
        let now = self.inner.clock.now_ms();
        if self.inner.signal.take_unsaved_one(job_id)
            && let Some(save) = self.queue_checkpoint(torrent, now)
        {
            self.checkpoint_landed(save).await;
        }
        torrent.settle(now);
        let landing = torrent
            .attached
            .is_some()
            .then(|| self.queue_resume(torrent, now, true))
            .flatten();
        if let Some(attached) = torrent.attached.take() {
            let id = TorrentIdOrHash::Id(attached.handle.id());
            if let Err(error) = attached.session.delete(id, false).await {
                tracing::debug!(error = %error, "a torrent was already gone from its session");
            }
        }
        let _ = self.landed(landing).await;
        torrent.engine = Engine::Absent;
        torrent.run_error = None;
    }

    // The torrent cannot go on: it leaves as a failed job and its data stays.
    async fn fail(&self, job_id: JobId, error: String) {
        let torrent = self.inner.live.lock().await.remove(&job_id);
        let Some(mut torrent) = torrent else { return };
        self.inner.counters.event(TorrentEvent::Failed);
        self.detach(&mut torrent).await;
        self.retire(torrent, "failed", Some(error)).await;
    }

    // Unpacking refused the torrent's output. Its job goes to history as
    // failed, once; the torrent stays, stopped with its data, until the
    // operator resumes it to seed.
    async fn refused(&self, job_id: JobId, error: String) {
        match self.inner.db.get_job_history(job_id.0) {
            Ok(None) => {}
            Ok(Some(_)) => return,
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's history");
                return;
            }
        }
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (report, hash, landing) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            let landing = self.stop_torrent(torrent, now);
            let report = report(
                self.view(&settings, torrent, now),
                JobStatus::Failed { error },
                now,
            );
            (report, hashes_of(&torrent.record).best(), landing)
        };
        let _ = self.landed(landing).await;
        self.inner.counters.event(TorrentEvent::Failed);
        self.apply_queue().await;
        self.finish(report, "failed", hash, now).await;
    }

    // A torrent that left the live ones goes to history: its row, then out
    // of the torrents and into the finished jobs.
    async fn retire(&self, torrent: Live, status: &str, error: Option<String>) {
        let job_id = torrent.record.job_id;
        self.inner.env.outside.leave(job_id.0);
        if let Some(job) = &torrent.job {
            job.forget();
        }
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let report = report(
            self.view(&settings, &torrent, now),
            job_status_from_persisted_str(status, error.as_deref()),
            now,
        );
        if let Err(error) = self.inner.db.delete_torrent(job_id) {
            tracing::warn!(job_id = job_id.0, error = %error, "could not remove a torrent");
        }
        self.finish(report, status, hashes_of(&torrent.record).best(), now)
            .await;
    }

    // A torrent's job, finished as `status`: into history and the queue's
    // finished jobs.
    async fn finish(&self, report: JobReport, status: &str, hash: Id20, now: i64) {
        let job_id = report.job_id;
        let row = report.history_row(status, Some(hash.0.to_vec()), now);
        if let Err(error) = self.inner.db.insert_job_history(&row) {
            tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's history");
        }
        self.publish().await;
        if let Some(queue) = self.queue()
            && let Err(error) = queue.retire_job(report.job_info()).await
        {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent did not reach history");
        }
        self.inner.retired.send_modify(|retired| *retired += 1);
    }

    /// Remove a torrent. A private torrent whose seeding goal is not met
    /// stays unless the removal says it knows.
    pub async fn remove(
        &self,
        job_id: JobId,
        delete_files: bool,
        confirm_unmet_goal: bool,
    ) -> Result<(), ActionError> {
        // Its outcome is its job's: a job still waiting for the tick is
        // restored first.
        self.restore_job(job_id).await;
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let mut live = self.inner.live.lock().await;
        let torrent = live.get(&job_id).ok_or(ActionError::NotFound(job_id))?;
        if torrent.private && !confirm_unmet_goal {
            let policy = settings.policy(&torrent.record, torrent.rule(&self.rules()));
            if let Some(left) =
                status::goal_left(policy.as_ref(), &torrent.counters, torrent.total, now)
            {
                return Err(ActionError::UnmetGoal(left));
            }
        }
        let mut torrent = live.remove(&job_id).ok_or(ActionError::NotFound(job_id))?;
        drop(live);
        if let Some(Resolving::Fetching(fetch)) = torrent.resolving.take() {
            fetch.abort();
            self.inner.counters.fetch(FetchOutcome::Cancelled);
        }
        let written = torrent
            .job
            .as_ref()
            .map(|job| job.with(|job| job.written()))
            .unwrap_or_default();
        self.detach(&mut torrent).await;
        if delete_files && let Some(dir) = torrent.record.storage_dir.clone() {
            let deleted =
                tokio::task::spawn_blocking(move || delete_written(Path::new(&dir), &written));
            if let Err(error) = deleted.await {
                tracing::warn!(job_id = job_id.0, error = %error, "could not delete a torrent's files");
            }
        }
        // The job's outcome: complete, refused by unpacking, or cut short.
        let (status, error) = match (torrent.complete(), torrent.refusal()) {
            (true, _) => ("complete", None),
            (false, Some(refusal)) => ("failed", Some(refusal)),
            (false, None) => ("cancelled", None),
        };
        self.inner.counters.event(TorrentEvent::Removed);
        self.retire(torrent, status, error).await;
        Ok(())
    }

    async fn with_torrent<T>(
        &self,
        job_id: JobId,
        change: impl FnOnce(&mut Live) -> Result<T, ActionError>,
    ) -> Result<T, ActionError> {
        let mut live = self.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
        change(torrent)
    }

    // What the operator asks of the torrent, written under its lock with
    // no other column, and the ledger and peers as they stand beside it.
    async fn set_intent(&self, job_id: JobId, intent: TorrentIntent) -> Result<(), ActionError> {
        let now = self.inner.clock.now_ms();
        let landing = self
            .with_torrent(job_id, |torrent| {
                self.inner
                    .db
                    .update_torrent_intent(job_id, intent)
                    .map_err(|error| ActionError::State(state_error(error)))?;
                torrent.settle(now);
                torrent.record.intent = intent;
                Ok(self.queue_resume(torrent, now, true))
            })
            .await?;
        let _ = self.landed(landing).await;
        Ok(())
    }

    // Stop the torrent for good, as its goal or a refusal does: its intent
    // is written, and the ledger and peers as they stand, queued.
    fn stop_torrent(&self, torrent: &mut Live, now: i64) -> Option<Landing> {
        let job_id = torrent.record.job_id;
        torrent.settle(now);
        torrent.record.intent = TorrentIntent::Stopped;
        if let Err(error) = self
            .inner
            .db
            .update_torrent_intent(job_id, torrent.record.intent)
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not keep a torrent's stop");
        }
        self.queue_resume(torrent, now, true)
    }

    // The torrent's place in its session, while it has one.
    async fn handle_of(&self, job_id: JobId) -> Result<Option<Arc<ManagedTorrent>>, ActionError> {
        self.with_torrent(job_id, |torrent| {
            Ok(torrent.attached.as_ref().map(|a| a.handle.clone()))
        })
        .await
    }

    /// The torrent's connections to peers, by address (libtorrent
    /// `torrent_handle::get_peer_info`); none while it is in no session.
    pub async fn peers(&self, job_id: JobId) -> Result<Vec<PeerInfo>, ActionError> {
        Ok(self
            .handle_of(job_id)
            .await?
            .map(|handle| handle.peers())
            .unwrap_or_default())
    }

    /// Ban each of `ips` from the torrent: its connections close now and
    /// none is made or taken from then on (libtorrent `torrent::ban_peer`).
    /// The bans are written with the peers and bans the torrent's next
    /// start begins with, so they outlive a restart.
    pub async fn ban_peers(&self, job_id: JobId, ips: &[IpAddr]) -> Result<(), ActionError> {
        let now = self.inner.clock.now_ms();
        let landing = self
            .with_torrent(job_id, |torrent| {
                match torrent
                    .attached
                    .as_ref()
                    .and_then(|attached| attached.handle.ban_peers(ips))
                {
                    Some(peers) => torrent.resume = peers,
                    // In no session, or stopped by an error: the bans join
                    // the list its next start begins with.
                    None => ips.iter().for_each(|ip| torrent.resume.ban(*ip)),
                }
                Ok(self.queue_resume(torrent, now, true))
            })
            .await?;
        self.landed(landing).await.map_err(ActionError::State)
    }

    /// The torrent's trackers, each with how announcing to it went.
    pub async fn trackers(&self, job_id: JobId) -> Result<Vec<TorrentTracker>, ActionError> {
        self.torrent(job_id)
            .await
            .map(|view| view.trackers)
            .ok_or(ActionError::NotFound(job_id))
    }

    /// Add `tiers`, which the operator names, after the torrent's last tier,
    /// leaving out the trackers it already lists.
    pub async fn add_trackers(
        &self,
        job_id: JobId,
        tiers: Vec<Vec<String>>,
    ) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |mut trackers| {
            let next = trackers
                .iter()
                .map(|tracker| tracker.tier + 1)
                .max()
                .unwrap_or(0);
            let added = tiers.into_iter().filter(|tier| !tier.is_empty());
            for (tier, urls) in (next..).zip(added) {
                trackers.extend(urls.into_iter().map(|url| TrackerEntry {
                    tier,
                    url,
                    named_by: NamedBy::Operator,
                }));
            }
            Ok(trackers)
        })
        .await
    }

    /// List `urls`, which the operator names, in their tiers, in place of
    /// the torrent's own: announces to a tracker it no longer lists stop.
    pub async fn replace_trackers(
        &self,
        job_id: JobId,
        urls: Vec<(u32, String)>,
    ) -> Result<(), ActionError> {
        let trackers = urls
            .into_iter()
            .map(|(tier, url)| TrackerEntry {
                tier,
                url,
                named_by: NamedBy::Operator,
            })
            .collect();
        self.edit_trackers(job_id, |_| Ok(trackers)).await
    }

    /// Stop listing each of `urls`.
    pub async fn remove_trackers(&self, job_id: JobId, urls: &[String]) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |mut trackers| {
            trackers.retain(|tracker| !urls.iter().any(|url| url.trim() == tracker.url));
            Ok(trackers)
        })
        .await
    }

    // Write the trackers `edit` makes of the torrent's list and hand them
    // to its session. A tracker the list already held keeps who named it;
    // the operator names any other. The rule they match is picked again; no
    // rule is stored, a restart picks it from the trackers written here.
    async fn edit_trackers(
        &self,
        job_id: JobId,
        edit: impl FnOnce(Vec<TrackerEntry>) -> Result<Vec<TrackerEntry>, ActionError>,
    ) -> Result<(), ActionError> {
        {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let listed = self
                .inner
                .db
                .list_torrent_trackers(job_id)
                .map_err(|error| ActionError::State(state_error(error)))?;
            let trackers = edit(listed.clone())?;
            tracker_urls(&trackers)?;
            let trackers: Vec<TrackerEntry> = tidy_trackers(trackers)
                .into_iter()
                .map(|tracker| TrackerEntry {
                    named_by: listed
                        .iter()
                        .find(|known| known.url == tracker.url)
                        .map_or(NamedBy::Operator, |known| known.named_by),
                    ..tracker
                })
                .collect();
            let stored = self
                .inner
                .db
                .set_torrent_trackers(job_id, &trackers)
                .map_err(|error| ActionError::State(state_error(error)))?;
            if !stored {
                return Err(ActionError::NotFound(job_id));
            }
            torrent.rule_id = self.rule_id(&trackers);
            if let Some(attached) = &torrent.attached {
                attached.handle.replace_trackers(tracker_tiers(&trackers));
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Announce to every tracker now, past the floor each set (libtorrent
    /// `torrent_handle::force_reannounce` with `ignore_min_interval`, as
    /// qBittorrent `TorrentImpl::forceReannounce` calls it). A stopped
    /// torrent announces nothing.
    pub async fn reannounce(&self, job_id: JobId) -> Result<(), ActionError> {
        if let Some(handle) = self.handle_of(job_id).await? {
            handle.force_reannounce(Duration::ZERO, None, true);
        }
        Ok(())
    }

    /// Announce to the tracker at `url` now, as [`Self::reannounce`] does to
    /// all of them.
    pub async fn reannounce_tracker(&self, job_id: JobId, url: &str) -> Result<(), ActionError> {
        let handle = self.handle_of(job_id).await?;
        let url = url.trim();
        let listed = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .map_err(|error| ActionError::State(state_error(error)))?;
        if listed.iter().all(|tracker| tracker.url != url) {
            return Err(ActionError::Invalid(format!("no tracker {url}")));
        }
        let url = weaver_torrent::Url::parse(url)
            .map_err(|error| ActionError::Invalid(format!("tracker {url}: {error}")))?;
        if let Some(handle) = handle {
            handle.force_reannounce(Duration::ZERO, Some(&url), true);
        }
        Ok(())
    }

    /// Look the torrent up on the DHT now (libtorrent
    /// `torrent_handle::force_dht_announce`).
    pub async fn dht_announce(&self, job_id: JobId) -> Result<(), ActionError> {
        if let Some(handle) = self.handle_of(job_id).await? {
            handle.force_dht_announce();
        }
        Ok(())
    }

    pub async fn pause(&self, job_id: JobId) -> Result<(), ActionError> {
        self.set_intent(job_id, TorrentIntent::Paused).await?;
        self.with_torrent(job_id, |torrent| {
            if let Some(Resolving::Fetching(fetch)) = &torrent.resolving {
                fetch.abort();
                self.inner.counters.fetch(FetchOutcome::Cancelled);
                torrent.resolving = Some(Resolving::Paused);
                // A paused magnet shows no fetch's table, as one paused
                // before its fetch began.
                torrent.fetching = None;
            }
            Ok(())
        })
        .await?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Resume a paused torrent, or one its seeding goal stopped.
    pub async fn resume(&self, job_id: JobId) -> Result<(), ActionError> {
        self.set_intent(job_id, TorrentIntent::Resumed).await?;
        // A fetch the pause ended starts again.
        let refetch = self
            .with_torrent(job_id, |torrent| {
                Ok(torrent
                    .resolving
                    .take_if(|fetch| matches!(fetch, Resolving::Paused))
                    .is_some())
            })
            .await?;
        if refetch {
            self.resolve(job_id).await;
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    pub async fn set_forced(&self, job_id: JobId, forced: bool) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_forced(job_id, forced)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.forced = forced;
            Ok(())
        })
        .await?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Move the torrent's directory under `parent`, where it goes on
    /// downloading or seeding (qBittorrent `TorrentImpl::setSavePath`,
    /// libtorrent `torrent::move_storage`). The move waits for the job's
    /// runs and happens whole; the torrent shows as moving until it did.
    /// A directory of the torrent's name is claimed under `parent`, so
    /// nothing there is replaced or kept.
    pub async fn set_location(&self, job_id: JobId, parent: String) -> Result<(), ActionError> {
        let parent = PathBuf::from(parent.trim());
        if !parent.is_absolute() {
            return Err(ActionError::Invalid(
                "the location must be an absolute path".to_owned(),
            ));
        }
        let (job, from) = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                match (torrent.job.clone(), torrent.record.storage_dir.clone()) {
                    (Some(job), Some(dir)) => Ok((job, PathBuf::from(dir))),
                    _ => Err(ActionError::Invalid(
                        "the torrent has no files to move yet".to_owned(),
                    )),
                }
            })
            .await?;
        if from.parent() == Some(parent.as_path()) {
            return Ok(());
        }
        let name = from
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| ActionError::State("the torrent's directory has no name".to_owned()))?;
        // A location the torrent's directory cannot be made under is the
        // caller's to fix (qBittorrent `TorrentsController::setLocationAction`
        // answers "Cannot make save path" as a conflict).
        let to = claim_complete_destination_path(&parent, name, job_id, &HashSet::new())
            .await
            .map_err(|error| {
                ActionError::Invalid(format!("could not make the torrent's directory: {error}"))
            })?;
        let started = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                torrent.settle(self.inner.clock.now_ms());
                torrent.moving = Some(to.clone());
                Ok(job.with(|job| {
                    job.relocate(to.clone());
                    job.relocated()
                }))
            })
            .await;
        match started {
            Ok(Some(outcome)) => self.moved(job_id, outcome).await,
            Ok(None) => {}
            Err(error) => {
                give_back(&to.to_string_lossy()).await;
                return Err(error);
            }
        }
        self.publish().await;
        Ok(())
    }

    /// Check every piece of a torrent again from what the job holds, trusting
    /// nothing saved (libtorrent `torrent::force_recheck`, qBittorrent's
    /// `torrents/recheck`). A running torrent runs again once checked, a
    /// paused one stays paused, and one being checked is left to its check.
    pub async fn recheck(&self, job_id: JobId) -> Result<(), ActionError> {
        {
            // Held across the recheck, as the queue holds it across a pause
            // or resume, so neither undoes the other halfway.
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let attached = torrent.attached.as_ref().ok_or_else(|| {
                ActionError::Invalid("the torrent is in no session to check it".into())
            })?;
            attached
                .session
                .force_recheck(&attached.handle)
                .await
                .map_err(|error| ActionError::State(format!("{error:#}")))?;
        }
        self.publish().await;
        Ok(())
    }

    // The job moved its directory, or could not: the record follows a
    // move, and a failed one gives its claim back and leaves the torrent
    // where it was.
    async fn moved(&self, job_id: JobId, outcome: Result<PathBuf, LeafError>) {
        let claimed = self
            .with_torrent(job_id, |torrent| {
                torrent.settle(self.inner.clock.now_ms());
                Ok(torrent.moving.take())
            })
            .await
            .ok()
            .flatten();
        match outcome {
            Ok(to) => {
                let to = to.to_string_lossy().into_owned();
                let kept = self
                    .with_torrent(job_id, |torrent| {
                        torrent.record.storage_dir = Some(to.clone());
                        Ok(())
                    })
                    .await;
                if kept.is_ok()
                    && let Err(error) = self.inner.db.set_torrent_storage_dir(job_id, &to)
                {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's new directory");
                }
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's move failed; it stays where it was");
                if let Some(claimed) = claimed {
                    give_back(&claimed.to_string_lossy()).await;
                }
            }
        }
    }

    /// The torrent's own upload and download limits in bytes per second
    /// (libtorrent `torrent::set_upload_limit`, `set_download_limit`):
    /// `None` keeps that limit as it is, `Some(None)` is unlimited and zero
    /// holds that direction closed. A limit is kept on the torrent's row
    /// before the torrent takes it, then reaches its peers at once; its
    /// seeding policy's upload cap holds it too where that is tighter.
    pub async fn set_rate_limits(
        &self,
        job_id: JobId,
        upload: Option<Option<u64>>,
        download: Option<Option<u64>>,
    ) -> Result<(), ActionError> {
        if let Some(limit) = [upload, download]
            .into_iter()
            .flatten()
            .flatten()
            .find(|limit| i64::try_from(*limit).is_err())
        {
            return Err(ActionError::Invalid(format!(
                "a limit of {limit} bytes a second is past the largest, {}",
                i64::MAX
            )));
        }
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_rate_limits(job_id, upload, download)
                .map_err(|error| ActionError::State(state_error(error)))?;
            let record = &mut torrent.record;
            if let Some(upload) = upload {
                record.upload_limit = upload;
            }
            if let Some(download) = download {
                record.download_limit = download;
            }
            Ok(())
        })
        .await?;
        self.apply_limits().await;
        self.publish().await;
        Ok(())
    }

    pub async fn set_policy_override(
        &self,
        job_id: JobId,
        policy: Option<SeedingPolicy>,
    ) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_policy_override(job_id, policy)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.policy_override = policy;
            Ok(())
        })
        .await?;
        // New goals move what the holds and the queue make of the torrent.
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Move a torrent to another category; when that category routes
    /// elsewhere the torrent moves to that route's session.
    pub async fn set_category(
        &self,
        job_id: JobId,
        category: Option<String>,
    ) -> Result<(), ActionError> {
        let settings = self.settings();
        let category_id = match category.as_deref().filter(|name| !name.trim().is_empty()) {
            Some(name) => Some(
                resolve_category_config(&settings.categories, name)
                    .map(|category| category.id)
                    .ok_or_else(|| ActionError::Invalid(format!("no category {name}")))?,
            ),
            None => None,
        };
        let rules = self.rules();
        let moved = {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let rule = torrent.rule(&rules);
            let before = settings.scope(torrent.record.category_id, rule);
            self.inner
                .db
                .set_torrent_category(job_id, category_id)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.category_id = category_id;
            let moved = before != settings.scope(category_id, rule);
            if moved {
                self.detach(torrent).await;
                torrent.route = None;
                torrent.held = None;
            }
            moved
        };
        if moved {
            self.attach(job_id, None).await;
        }
        self.publish().await;
        Ok(())
    }

    /// A queue action on a torrent. The top priority, or a move to the top,
    /// forces it past the download queue; any other priority, or a move to
    /// the bottom, leaves it to the queue.
    pub async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), ActionError> {
        match action {
            QueueAction::Pause => self.pause(job_id).await,
            QueueAction::Resume => self.resume(job_id).await,
            QueueAction::Cancel(origin) => {
                let delete_files = origin != CancellationOrigin::UserKeepingFiles;
                self.remove(job_id, delete_files, false).await
            }
            QueueAction::Update(update) => self.update(job_id, update).await,
            QueueAction::Move(QueueMoveTarget::Top) => self.set_forced(job_id, true).await,
            QueueAction::Move(QueueMoveTarget::Bottom) => self.set_forced(job_id, false).await,
            QueueAction::Move(QueueMoveTarget::Offset(_)) => Err(ActionError::Invalid(
                "a torrent moves to the top or the bottom of the queue".into(),
            )),
        }
    }

    async fn update(&self, job_id: JobId, update: JobUpdate) -> Result<(), ActionError> {
        if update.name.is_some() || !matches!(update.password, FieldUpdate::Unchanged) {
            return Err(ActionError::Invalid(
                "a torrent keeps its name and has no password".into(),
            ));
        }
        let forced = match &update.metadata {
            FieldUpdate::Unchanged => None,
            FieldUpdate::Clear => Some(false),
            FieldUpdate::Set(metadata) => Some(metadata.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY) && top_priority(value)
            })),
        };
        match update.category {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => self.set_category(job_id, None).await?,
            FieldUpdate::Set(name) => self.set_category(job_id, Some(name)).await?,
        }
        match forced {
            Some(forced) => self.set_forced(job_id, forced).await,
            None => Ok(()),
        }
    }

    // Bring back every stored torrent, its record only. Nothing here waits
    // on a route or reads a torrent's files: a torrent's job is restored
    // and it goes into its route's session at the tick, as one whose route
    // changed does, and a magnet waits for its session on the task that
    // fetches its metainfo.
    async fn restore(&self) -> Result<(), String> {
        self.load_rules()?;
        let records = self.inner.db.list_torrents().map_err(state_error)?;
        let now = self.inner.clock.now_ms();
        for record in records {
            let job_id = record.job_id;
            let counters = self
                .inner
                .db
                .get_torrent_counters(job_id)
                .unwrap_or_else(|error| {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's ledger");
                    None
                })
                .unwrap_or_default();
            let resume = self.inner.db.torrent_peers(job_id).unwrap_or_else(|error| {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's saved peers");
                ResumePeers::default()
            });
            // The rule is the one its trackers match now (libtorrent keeps
            // the trackers, not what was made of them).
            let trackers = self.inner.db.list_torrent_trackers(job_id).unwrap_or_else(|error| {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's trackers");
                Vec::new()
            });
            let mut torrent = Live::new(record.clone(), counters, resume, now);
            torrent.rule_id = self.rule_id(&trackers);
            match record.metainfo.as_deref().map(parse) {
                Some(Ok(parsed)) => {
                    self.describe(&mut torrent, &parsed);
                    let root = self.kept_root(job_id, &parsed.layout);
                    torrent.kept(self.stored_failure(job_id), root);
                    self.inner.live.lock().await.insert(job_id, torrent);
                }
                Some(Err(error)) => {
                    self.inner.live.lock().await.insert(job_id, torrent);
                    self.fail(job_id, error.to_string()).await;
                }
                None => {
                    self.inner.live.lock().await.insert(job_id, torrent);
                    self.resolve(job_id).await;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    // Which torrents the queue lets download, applied to their sessions.
    async fn apply_queue(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let holds = self.holds();
        let mut live = self.inner.live.lock().await;
        let mut order: Vec<&Live> = live.values().collect();
        order.sort_by_key(|torrent| (torrent.record.added_at_ms, torrent.record.job_id));
        let queue: Vec<Queued> = order
            .iter()
            .map(|torrent| queued(&settings, &rules, torrent, now))
            .collect();
        let active = status::active(&queue, settings.torrent.max_active_downloads, &holds);
        let targets: BTreeMap<JobId, _> = queue
            .iter()
            .map(|torrent| (torrent.job_id, torrent.target()))
            .collect();
        for torrent in live.values_mut() {
            let job_id = torrent.record.job_id;
            let active = active.contains(&job_id);
            torrent.settle(now);
            torrent.paused_by = torrent
                .resumed()
                .then(|| holds.reason_for(targets[&job_id]))
                .flatten();
            torrent.queued = !active && torrent.resumed() && torrent.paused_by.is_none();
            let run = active && torrent.resumed();
            let Some(attached) = &torrent.attached else {
                continue;
            };
            let result = if run && attached.handle.is_paused() {
                attached.session.unpause(&attached.handle).await
            } else if !run && !attached.handle.is_paused() {
                attached.session.pause(&attached.handle).await
            } else {
                continue;
            };
            torrent.ran(result);
        }
    }

    /// Read every torrent, keep their ledgers, follow their routes, and
    /// apply what that changes.
    pub async fn tick(&self) {
        self.restore_jobs().await;
        self.follow_routes().await;
        // Taken after the routes are followed, whose readings take their
        // own: no reading in the tick runs from before one already kept.
        let now = self.inner.clock.now_ms();
        self.read(now, false).await;
        self.save_checkpoints(self.inner.signal.take_unsaved())
            .await;
        self.apply_goals(now).await;
        self.apply_queue().await;
        self.apply_picking().await;
        self.apply_limits().await;
        self.drop_idle_sessions().await;
        self.read_sessions().await;
        self.publish().await;
        self.inner.ticks.send_modify(|ticks| *ticks += 1);
    }

    // A torrent whose route changed moves to the new route's session, and
    // one whose peer terms changed goes through its session again; one
    // whose route went down is held, and comes back when it is up. A
    // session the settings would now open otherwise, or whose leg was
    // revoked under it, opens again, its torrents with it; one left on a
    // session its route no longer runs moves to the one it does. A magnet
    // fetching on a session its route no longer runs, or on a route gone
    // down, ends that fetch and fetches again on its route as it stands.
    async fn follow_routes(&self) {
        let now = self.inner.clock.now_ms();
        let settings = self.settings();
        let rules = self.rules();
        let outdated = self.inner.sessions.outdated().await;
        let running = self.inner.sessions.running().await;
        let mut reattach = Vec::new();
        let mut down = BTreeMap::new();
        let mut unresolved = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let rule = torrent.rule(&rules);
                let scope = settings.scope(torrent.record.category_id, rule);
                let plan = self.inner.sessions.routes().plan(scope);
                if torrent.layout.is_none() {
                    let moved = matches!(torrent.resolving, Some(Resolving::Fetching(_)))
                        && plan.as_ref().ok().is_none_or(|plan| {
                            plan.down.is_some()
                                || torrent.route.as_ref() != Some(&plan.key)
                                || outdated.contains(&plan.key)
                        });
                    if moved && let Some(Resolving::Fetching(fetch)) = torrent.resolving.take() {
                        fetch.abort();
                        self.inner.counters.fetch(FetchOutcome::Cancelled);
                        torrent.fetching = None;
                    }
                    if moved || torrent.held.is_some() {
                        unresolved.push(torrent.record.job_id);
                    }
                    continue;
                }
                let terms = PeerTerms::of(rule);
                match plan {
                    Ok(plan) => {
                        if let Some(reason) = &plan.down {
                            down.insert(plan.key.clone(), reason.clone());
                            torrent.settle(now);
                            torrent.attached = None;
                            torrent.engine = Engine::Absent;
                            torrent.route = Some(plan.key);
                            torrent.held = Some(Held::RouteDown(reason.clone()));
                        } else if torrent.route.as_ref() != Some(&plan.key)
                            || outdated.contains(&plan.key)
                            || torrent.attached.as_ref().is_none_or(|attached| {
                                attached.terms != terms
                                    || !running
                                        .get(&plan.key)
                                        .is_some_and(|open| Arc::ptr_eq(open, &attached.session))
                            })
                        {
                            self.detach(torrent).await;
                            torrent.held = None;
                            reattach.push(torrent.record.job_id);
                        }
                    }
                    Err(reason) => {
                        self.detach(torrent).await;
                        torrent.held = Some(Held::RouteDown(RouteFailure::with_detail(
                            TorrentRouteReason::RoutePlanFailed,
                            reason,
                        )));
                    }
                }
            }
        }
        for key in &outdated {
            self.inner.sessions.drop_route(key).await;
        }
        for (key, reason) in down {
            self.inner.sessions.hold(&key, reason).await;
        }
        for job_id in reattach {
            self.attach(job_id, None).await;
        }
        for job_id in unresolved {
            self.resolve(job_id).await;
        }
    }

    // The session's figures for each torrent, and the ledger kept from
    // them: written whenever it changed, with its peers once a minute while
    // they change, or now when `at_once`.
    async fn read(&self, now: i64, at_once: bool) {
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let reading = torrent.reading(now);
                torrent.engine = reading.engine.clone();
                torrent.speeds = reading.speeds;
                torrent.peers = reading.peers;
                torrent.keep(reading);
                // Queued in the step that took it, so ledgers land in the
                // order they were read.
                landings.extend(self.queue_resume(torrent, now, at_once));
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
    }

    // A seeding torrent whose seeding goal is met does what its policy says.
    async fn apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let mut actions = Vec::new();
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                // A forced torrent seeds past its goal.
                if !torrent.seeding() || !torrent.resumed() || torrent.record.forced {
                    continue;
                }
                let Some(policy) = settings.policy(&torrent.record, torrent.rule(&rules)) else {
                    continue;
                };
                if status::goal_left(Some(&policy), &torrent.counters, torrent.total, now).is_some()
                {
                    continue;
                }
                match policy.action {
                    SeedingAction::Stop => landings.extend(self.stop_torrent(torrent, now)),
                    SeedingAction::Remove => actions.push((torrent.record.job_id, false)),
                    SeedingAction::RemoveWithData => actions.push((torrent.record.job_id, true)),
                }
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
        for (job_id, delete_files) in actions {
            if let Err(error) = self.remove(job_id, delete_files, true).await {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent that met its goal was not removed");
            }
        }
    }

    // What each session runs is what is stored: the levels the job wants
    // the files fetched at now (the operator's, with the job's raise),
    // sequential and first-and-last, each read from the torrent's handle and
    // pushed only where it differs. A change the session refuses (while it
    // checks its files) is pushed again on the next pass. A full disk
    // leaves the levels as they are: the session stops fetching in upload
    // mode, not by lower levels.
    async fn apply_picking(&self) {
        let live = self.inner.live.lock().await;
        for torrent in live.values() {
            let (Some(attached), Some(job)) = (&torrent.attached, &torrent.job) else {
                continue;
            };
            let handle = &attached.handle;
            let record = &torrent.record;
            if handle.sequential() != record.sequential {
                handle.set_sequential(record.sequential);
            }
            if handle.first_last_piece_first() != record.first_last_piece_first
                && let Err(error) = handle.set_first_last_piece_first(record.first_last_piece_first)
            {
                tracing::debug!(job_id = record.job_id.0, error = %error, "a torrent's first and last pieces did not move");
            }
            let wanted = fetch_levels(torrent, job);
            let running = attached.handle.file_priorities();
            let changes: Vec<(usize, FilePriority)> = wanted
                .iter()
                .enumerate()
                .filter(|(index, level)| running.get(*index) != Some(*level))
                .map(|(index, level)| (index, *level))
                .collect();
            if changes.is_empty() {
                continue;
            }
            if let Err(error) = attached
                .session
                .set_file_priorities(&attached.handle, &changes)
                .await
            {
                tracing::debug!(job_id = torrent.record.job_id.0, error = %error, "a torrent's file levels did not change");
            }
        }
    }

    // The limits each torrent's session holds it to, from its own and its
    // seeding policy's as they are now: a change of either at any level
    // reaches the torrent's peers without reconnecting them.
    async fn apply_limits(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let live = self.inner.live.lock().await;
        for torrent in live.values() {
            let Some(attached) = &torrent.attached else {
                continue;
            };
            let rule = torrent.rule(&rules);
            let limits = ratelimits_of(
                &torrent.record,
                settings.policy(&torrent.record, rule),
                rule,
            );
            if attached.handle.ratelimits() != limits {
                attached.handle.set_ratelimits(limits);
            }
        }
    }

    /// Set the operator's level for each given file, by its index in the
    /// torrent, and push the levels to the torrent's session.
    pub async fn set_file_priorities(
        &self,
        job_id: JobId,
        priorities: &[(u32, FilePriority)],
    ) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            let mut levels = torrent.levels.clone();
            let count = levels.len();
            for (index, priority) in priorities {
                let Some(level) = levels.get_mut(*index as usize) else {
                    return Err(ActionError::Invalid(format!(
                        "file {index} is out of range: the torrent has {count} files"
                    )));
                };
                *level = *priority;
            }
            let padding: Vec<bool> = torrent
                .layout
                .iter()
                .flat_map(|layout| layout.files().map(|file| file.padding))
                .collect();
            // Padding holds Ignored whatever is asked, and is stored as
            // nothing asked: the metainfo says which files are padding.
            let asked: Vec<FilePriority> = levels
                .iter()
                .zip(&padding)
                .map(|(level, padding)| {
                    if *padding {
                        FilePriority::Normal
                    } else {
                        *level
                    }
                })
                .collect();
            self.inner
                .db
                .set_torrent_file_priorities(job_id, &asked)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.levels = asked
                .iter()
                .zip(&padding)
                .map(|(level, padding)| level.held_by(*padding))
                .collect();
            Ok(())
        })
        .await?;
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    /// Whether the torrent's pieces are fetched in order.
    pub async fn set_sequential(&self, job_id: JobId, on: bool) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_sequential(job_id, on)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.sequential = on;
            Ok(())
        })
        .await?;
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    /// Whether each file's first and last pieces are fetched first.
    pub async fn set_first_last_piece_first(
        &self,
        job_id: JobId,
        on: bool,
    ) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_first_last_piece_first(job_id, on)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.first_last_piece_first = on;
            Ok(())
        })
        .await?;
        self.apply_picking().await;
        self.publish().await;
        Ok(())
    }

    // The scopes that run on a route of their own, with their names: the
    // default, then each category's and tracker rule's that asks for one.
    fn route_scopes(&self) -> Vec<(TorrentScope, Option<String>)> {
        let categories = self
            .settings()
            .categories
            .into_iter()
            .filter(|category| category.torrent_route)
            .map(|category| (TorrentScope::Category(category.id), Some(category.name)));
        let rules = self.rules();
        let rules = rules.iter().filter(|rule| rule.torrent_route).map(|rule| {
            (
                TorrentScope::TrackerRule(rule.id),
                Some(rule.host_pattern.clone()),
            )
        });
        std::iter::once((TorrentScope::Default, None))
            .chain(categories)
            .chain(rules)
            .collect()
    }

    /// Every torrent route, by the scopes that plan to it, in their order,
    /// then any session left on a route no scope plans to. A scope whose
    /// route cannot be planned is listed on its own, with why.
    pub async fn routes(&self) -> Vec<TorrentRoute> {
        let sessions = &self.inner.sessions;
        let mut listed: Vec<TorrentRoute> = Vec::new();
        for (scope, name) in self.route_scopes() {
            let (key, down, names) = match sessions.routes().plan(scope) {
                Ok(plan) => (plan.key, plan.down, plan.names),
                Err(error) => (
                    unplanned_key(scope),
                    Some(RouteFailure::with_detail(
                        TorrentRouteReason::RoutePlanFailed,
                        error,
                    )),
                    None,
                ),
            };
            match listed.iter_mut().find(|route| route.key == key) {
                Some(route) => route.scopes.push((scope, name)),
                None => listed.push(TorrentRoute {
                    key,
                    scopes: vec![(scope, name)],
                    session: None,
                    down: sessions.off().or(down),
                    names,
                }),
            }
        }
        for view in self.inner.sessions.views().await {
            match listed.iter_mut().find(|route| route.key == view.key) {
                Some(route) => route.session = Some(view),
                None => listed.push(TorrentRoute {
                    key: view.key.clone(),
                    scopes: Vec::new(),
                    session: Some(view),
                    down: None,
                    names: None,
                }),
            }
        }
        listed
    }

    async fn drop_idle_sessions(&self) {
        let used: BTreeSet<RouteKey> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter_map(|torrent| torrent.route.clone())
            .collect();
        for view in self.inner.sessions.views().await {
            if !used.contains(&view.key) {
                self.inner.sessions.drop_route(&view.key).await;
            }
        }
        // The routes that exist are the ones a scope plans to.
        let routes = self.inner.sessions.routes();
        let existing: BTreeSet<RouteKey> = self
            .route_scopes()
            .into_iter()
            .filter_map(|(scope, _)| routes.plan(scope).ok())
            .map(|plan| plan.key)
            .collect();
        self.inner.sessions.forget_routes(existing).await;
    }

    /// Warnings that stand: the disk filling up under waiting torrents.
    pub async fn warnings(&self) -> Vec<String> {
        let live = self.inner.live.lock().await;
        let waiting: Vec<&str> = live
            .values()
            .filter(|torrent| torrent.upload_mode())
            .map(|torrent| torrent.record.name.as_str())
            .collect();
        if waiting.is_empty() {
            return Vec::new();
        }
        let volume = self.settings().complete_dir;
        vec![disk_warning(&volume, &waiting)]
    }

    fn view(&self, settings: &TorrentSettings, torrent: &Live, now: i64) -> TorrentView {
        // The session as it is now, not as the last tick found it: the
        // state and every figure from one reading of it.
        self.view_of(settings, torrent, torrent.reading(now), now)
    }

    fn view_of(
        &self,
        settings: &TorrentSettings,
        torrent: &Live,
        reading: Reading,
        now: i64,
    ) -> TorrentView {
        let (state, why) = status::project(&torrent.observed(reading.engine));
        let (detail, route_reason) = status::Why::split(why);
        let counters = &reading.counters;
        let record = &torrent.record;
        let total = torrent.total;
        let have = on_disk(torrent.layout.as_deref(), &reading.file_have);
        let (wanted, wanted_have) = wanted(
            torrent.layout.as_deref(),
            &torrent.levels,
            &reading.file_have,
        );
        let rules = self.rules();
        let rule = torrent.rule(&rules);
        let policy = settings.policy(record, rule);
        let swarm = status::swarm(&torrent.announces(), now);
        // The time the torrent has run to this reading, the part of a
        // second included, and its averages over that same time.
        let active_seconds =
            status::active_seconds(counters.active_duration_s, reading.timed.active_ms);
        TorrentView {
            job_id: record.job_id,
            name: record.name.clone(),
            infohash: hashes_of(record).best().as_string(),
            infohash_v1: record.infohash_v1.map(hex::encode),
            infohash_v2: record.infohash_v2.map(hex::encode),
            version: torrent.about.version,
            magnet: Magnet::of(hashes_of(record), Some(record.name.clone()), Vec::new())
                .to_string(),
            source: record.source,
            state,
            detail,
            route_reason,
            paused_by: torrent.paused_by,
            category: settings
                .category(record.category_id)
                .map(|category| category.name.clone()),
            private: torrent.private,
            forced: record.forced,
            // Over the bytes it wants, all of them had when it wants none
            // (libtorrent `torrent_status::progress`).
            progress: if total == 0 {
                0.0
            } else if wanted == 0 {
                1.0
            } else {
                (wanted_have as f64 / wanted as f64).min(1.0)
            },
            total_bytes: total,
            have_bytes: have,
            uploaded_bytes: counters.uploaded_bytes,
            downloaded_bytes: counters.downloaded_bytes,
            ratio: status::ratio(counters, total),
            download_bps: reading.speeds.0,
            upload_bps: reading.speeds.1,
            upload_limit: record.upload_limit,
            download_limit: record.download_limit,
            eta_seconds: status::eta_seconds(total.saturating_sub(have), reading.speeds.0),
            peers: reading.peers.0,
            seeds: reading.peers.1,
            leechers: reading.peers.0.saturating_sub(reading.peers.1),
            max_peers: settings.torrent.max_peers_total,
            swarm_seeds: swarm.seeds,
            swarm_leechers: swarm.leechers,
            reannounce_seconds: swarm.reannounce_seconds,
            active_seconds,
            download_avg_bps: status::average_bps(counters.downloaded_bytes, active_seconds),
            upload_avg_bps: status::average_bps(counters.uploaded_bytes, active_seconds),
            wasted_bytes: wasted(&reading.seen),
            last_seen_complete_ms: counters.last_seen_complete_ms,
            pieces: torrent.pieces(),
            created_by: torrent.about.created_by.clone(),
            created_at_ms: torrent.about.created_at_ms,
            comment: torrent.about.comment.clone(),
            added_at_ms: record.added_at_ms,
            completed_at_ms: record.completed_at_ms,
            storage_dir: record.storage_dir.clone(),
            seeding_left: status::goal_left(policy.as_ref(), counters, total, now),
            seeding_override: record.policy_override,
            seeding_inherited: settings.inherited(record, rule),
            seeding_policy: policy.unwrap_or_default(),
            seeding_source: settings.seeding_source(record, rule),
            unpack: settings.unpack(record, rule),
            scope: settings.scope(record.category_id, rule),
            route: torrent.route.as_ref().map(|key| key.as_str().to_owned()),
            files: Vec::new(),
            sequential: record.sequential,
            first_last_piece_first: record.first_last_piece_first,
            file_have: reading.file_have,
            trackers: Vec::new(),
            metadata: record.metadata.clone(),
        }
    }

    /// One torrent, with its files and trackers.
    pub async fn torrent(&self, job_id: JobId) -> Option<TorrentView> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let listed = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .unwrap_or_default();
        let (mut view, announces) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let mut view = self.view(&settings, torrent, now);
            view.files = torrent.files();
            (view, torrent.announces())
        };
        self.track(&mut view, listed, &announces, &mut HashMap::new());
        Some(view)
    }

    /// What a support report reads of one torrent: its view, its metainfo,
    /// and the held figures the view folds together. One pass over the
    /// torrent under the lock its view takes; its peers are read after,
    /// from its session.
    pub async fn support(&self, job_id: JobId) -> Option<TorrentSupport> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let listed = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .unwrap_or_default();
        let (mut support, announces, handle) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let reading = torrent.reading(now);
            let (wanted_bytes, wanted_have_bytes) = wanted(
                torrent.layout.as_deref(),
                &torrent.levels,
                &reading.file_have,
            );
            let attached = torrent.attached.as_ref();
            let support = TorrentSupport {
                view: self.view(&settings, torrent, now),
                metainfo: torrent.record.metainfo.clone(),
                counters: reading.counters,
                failed_bytes: reading.seen.failed_bytes,
                redundant_bytes: reading.seen.redundant_bytes,
                peers: reading.peers,
                peer_kinds: PeerKinds::default(),
                fetch: torrent.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
                in_session: attached.is_some(),
                leg: attached
                    .and_then(|attached| attached.leg.as_ref())
                    .map(|leg| TorrentLeg {
                        position: leg.tenancy.leg().1,
                        egress_id: leg.egress_id,
                        own_sockets: leg.own_sockets,
                    }),
                has_job: torrent.job.is_some(),
                kept_pieces: torrent.kept_pieces,
                wanted_bytes,
                wanted_have_bytes,
                metrics: TorrentMetrics::default(),
            };
            (
                support,
                torrent.announces(),
                attached.map(|attached| Arc::clone(&attached.handle)),
            )
        };
        self.track(&mut support.view, listed, &announces, &mut HashMap::new());
        if let Some(handle) = handle {
            support.peer_kinds = PeerKinds::of(&handle.peers());
        }
        support.metrics = self.metrics();
        Some(support)
    }

    /// Every torrent, or a category's, in the order they were added, each
    /// with its trackers.
    pub async fn torrents(&self, category: Option<&str>) -> Vec<TorrentView> {
        let now = self.inner.clock.now_ms();
        self.listed(category, now)
            .await
            .into_iter()
            .map(|(view, _)| view)
            .collect()
    }

    // Every torrent, or a category's, in the order they were added, each
    // with its trackers, and its figures as the exporter reads them: both
    // from one reading of its session.
    async fn listed(&self, category: Option<&str>, now: i64) -> Vec<(TorrentView, TorrentSeries)> {
        let settings = self.settings();
        let mut listed = self.inner.db.torrent_trackers_by_job().unwrap_or_default();
        let mut views: Vec<(TorrentView, Vec<TrackerAnnounce>, Unviewed)> = {
            let live = self.inner.live.lock().await;
            live.values()
                .map(|torrent| {
                    let reading = torrent.reading(now);
                    let unviewed = self.unviewed(&settings, torrent, &reading, now);
                    let view = self.view_of(&settings, torrent, reading, now);
                    (view, torrent.announces(), unviewed)
                })
                .filter(|(view, ..)| {
                    category.is_none_or(|name| {
                        view.category
                            .as_deref()
                            .is_some_and(|category| category.eq_ignore_ascii_case(name))
                    })
                })
                .collect()
        };
        let mut names = HashMap::new();
        for (view, announces, _) in &mut views {
            let listed = listed.remove(&view.job_id).unwrap_or_default();
            self.track(view, listed, announces, &mut names);
        }
        let mut views: Vec<(TorrentView, TorrentSeries)> = views
            .into_iter()
            .map(|(view, announces, unviewed)| {
                let series = unviewed.series(&view, &announces);
                (view, series)
            })
            .collect();
        views.sort_by_key(|(view, _)| (view.added_at_ms, view.job_id));
        views
    }

    // What the exporter reads of `torrent` that its view does not carry,
    // from `reading`.
    fn unviewed(
        &self,
        settings: &TorrentSettings,
        torrent: &Live,
        reading: &Reading,
        now: i64,
    ) -> Unviewed {
        let rules = self.rules();
        let policy = settings.policy(&torrent.record, torrent.rule(&rules));
        let (wanted_bytes, _) = wanted(
            torrent.layout.as_deref(),
            &torrent.levels,
            &reading.file_have,
        );
        Unviewed {
            wanted_bytes,
            hash_failed_bytes: reading.seen.failed_bytes,
            redundant_bytes: reading.seen.redundant_bytes,
            seeding_seconds: reading.counters.finished_duration_s,
            peers_connecting: reading.peers.2,
            leg: torrent
                .attached
                .as_ref()
                .and_then(|attached| attached.leg.as_ref())
                .map(|leg| leg.tenancy.leg().clone()),
            fetch: torrent.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            seeding: SeedingSeries::of(policy.as_ref(), &reading.counters, torrent.total, now),
        }
    }

    // A view's trackers: those listed for it, as its run announces to
    // them, with who resolves their names on its route. `names` holds each
    // scope's planned once.
    fn track(
        &self,
        view: &mut TorrentView,
        listed: Vec<TrackerEntry>,
        announces: &[TrackerAnnounce],
        names: &mut HashMap<TorrentScope, Option<ResolvedBy>>,
    ) {
        let names = *names.entry(view.scope).or_insert_with(|| {
            self.inner
                .sessions
                .routes()
                .plan(view.scope)
                .ok()
                .and_then(|plan| plan.names)
        });
        view.trackers = status::trackers(listed, announces, names);
    }

    /// The torrents as queue items.
    pub async fn job_infos(&self) -> Vec<JobInfo> {
        let now = self.inner.clock.now_ms();
        job_infos(self.torrents(None).await, now)
    }

    /// The scope whose route a torrent added in `category` under `rule` takes.
    pub fn scope_of(&self, category: Option<&str>, rule: Option<&TrackerRule>) -> TorrentScope {
        let settings = self.settings();
        let id = category
            .and_then(|name| resolve_category_config(&settings.categories, name))
            .map(|category| category.id);
        settings.scope(id, rule)
    }

    /// The bytes at `url` for a torrent added in `category`, fetched over
    /// the route of its matched tracker rule, else its category's, else the
    /// default (see [`TorrentSettings::scope`]), as that rule's client. A leg
    /// that is down fetches nothing: the add fails for the route being down.
    pub async fn fetch_url(
        &self,
        proxies: &crate::proxies::ProxyRuntime,
        category: Option<&str>,
        url: &reqwest::Url,
        cookie: Option<&str>,
    ) -> Result<Vec<u8>, String> {
        let rules = self.rules();
        let rule = tracker_rule_for(&rules, [url.as_str()]);
        let scope = self.scope_of(category, rule);
        let plan = self.inner.sessions.routes().plan(scope)?;
        if let Some(reason) = plan.down {
            return Err(SessionError::RouteDown(reason).to_string());
        }
        let client = client_of(rule);
        crate::ingest::fetch_torrent_url(proxies, scope, url, cookie, client).await
    }
}

// The torrents' seat in the queue.
struct Seat(Weak<Inner>);

impl TorrentRate for Seat {
    fn download_bps(&self) -> u64 {
        self.0
            .upgrade()
            .map_or(0, |inner| inner.sessions.rates().download_bps())
    }

    fn set_rates(&self, rates: crate::bandwidth::TorrentRates) {
        if let Some(inner) = self.0.upgrade() {
            inner.sessions.rates().set(rates);
        }
    }
}

#[async_trait::async_trait]
impl QueueOwner for Seat {
    async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), SchedulerError> {
        let inner = self
            .0
            .upgrade()
            .ok_or(SchedulerError::JobNotFound(job_id))?;
        TorrentService { inner }
            .act(job_id, action)
            .await
            .map_err(|error| match error {
                ActionError::NotFound(job_id) => SchedulerError::JobNotFound(job_id),
                ActionError::UnmetGoal(_) => SchedulerError::Conflict(error.to_string()),
                ActionError::Invalid(reason) => SchedulerError::InvalidInput(reason),
                ActionError::State(reason) => SchedulerError::Internal(reason),
            })
    }
}

// The top priority a job can be given.
fn top_priority(value: &str) -> bool {
    use crate::post_processing::scan::numeric_priority;
    numeric_priority(value) >= numeric_priority("HIGH")
}

// The checkpoint the session starts from, so it does not check the files:
// the pieces the job has, and no block of any other. The storage keeps a
// piece's blocks in memory until the piece verifies, so no block of a piece
// not had is ever written to the job's files. None while the job vouches
// for nothing: the session checks every piece.
fn apply_initial_have(
    options: &mut AddTorrentOptions,
    have: Option<bitvec::boxed::BitBox<u8, bitvec::order::Msb0>>,
) {
    options.checkpoint = have.map(Checkpoint::pieces);
}

// Why a session refusal leaves a torrent without one.
fn held_by(error: SessionError) -> Held {
    match error {
        SessionError::Cap => Held::SessionLimit,
        SessionError::RouteDown(reason) => Held::RouteDown(reason),
        SessionError::Build(error) => Held::Session(error),
    }
}

// Give back a directory claimed for a torrent that was not added, when
// nothing went into it.
async fn give_back(dir: &str) {
    if let Err(error) = tokio::fs::remove_dir(dir).await {
        tracing::debug!(dir, error = %error, "a claimed torrent directory stays");
    }
}

/// The warning a full disk raises.
pub fn disk_warning(volume: &Path, waiting: &[&str]) -> String {
    format!(
        "Disk full on {}: waiting {}",
        volume.display(),
        waiting.join(", ")
    )
}

// The sessions as read, with every route a torrent names and no session was
// read on held: a route's series say what runs on it now, and with none
// open, nothing runs.
fn with_routes_of(
    mut sessions: Vec<SessionSeries>,
    torrents: &[TorrentSeries],
) -> Vec<SessionSeries> {
    for torrent in torrents {
        let route = torrent.route.as_str();
        if !route.is_empty() && !sessions.iter().any(|session| session.route == route) {
            sessions.push(SessionSeries::held(route));
        }
    }
    sessions.sort_by(|a, b| a.route.cmp(&b.route));
    sessions
}

// `views` as queue items at `now_ms`.
fn job_infos(views: Vec<TorrentView>, now_ms: i64) -> Vec<JobInfo> {
    views
        .into_iter()
        .map(|view| {
            let status = job_status(&view);
            report(view, status, now_ms).job_info()
        })
        .collect()
}

fn job_status(view: &TorrentView) -> JobStatus {
    match view.state {
        TorrentState::Queued => JobStatus::Queued,
        TorrentState::FetchingMetadata => JobStatus::FetchingMetadata,
        TorrentState::CheckingFiles => JobStatus::CheckingFiles,
        TorrentState::Downloading => JobStatus::Downloading,
        TorrentState::Stalled => JobStatus::Stalled,
        TorrentState::Seeding => JobStatus::Seeding,
        TorrentState::Paused => JobStatus::Paused,
        TorrentState::Moving => JobStatus::Moving,
        // It waits for a certificate, held where it is.
        TorrentState::NeedsCertificate => JobStatus::Stalled,
        // A stop with any reason but its met goal is a failure.
        TorrentState::Stopped => match view.detail.as_deref() {
            Some(status::GOAL_MET) => JobStatus::Complete,
            None if view.completed_at_ms.is_some() => JobStatus::Complete,
            detail => JobStatus::Failed {
                error: detail.unwrap_or_default().to_owned(),
            },
        },
    }
}

// A downloading torrent's progress, speed and ETA, as the queue reads a
// job's. One that has stalled short of complete is still downloading, at no
// speed, and keeps what it has.
fn downloading(view: &TorrentView, now_ms: i64) -> Option<JobPhaseProgress> {
    let fetching = match view.state {
        TorrentState::Downloading => true,
        TorrentState::Stalled => view.completed_at_ms.is_none(),
        _ => false,
    };
    fetching.then(|| JobPhaseProgress {
        phase: JobPhase::Downloading,
        completed_bytes: view.have_bytes,
        total_bytes: view.total_bytes,
        progress_percent: (view.progress * 100.0) as f32,
        rate_bps: Some(view.download_bps),
        estimated_remaining_ms: view.eta_seconds.map(|eta| eta.saturating_mul(1000)),
        started_at_epoch_ms: view.added_at_ms as f64,
        updated_at_epoch_ms: now_ms as f64,
    })
}

// The levels the session fetches a torrent's files at now.
fn fetch_levels(torrent: &Live, job: &TorrentJob) -> Vec<FilePriority> {
    raised_levels(&torrent.levels, job.with(|job| job.selection()))
}

// The operator's levels with the files the job raised at Maximum
// (qBittorrent's top level); every other file keeps its stored level. A file
// the operator ignored stays ignored.
fn raised_levels(levels: &[FilePriority], raised: Vec<usize>) -> Vec<FilePriority> {
    let mut levels = levels.to_vec();
    for index in raised {
        if let Some(level) = levels.get_mut(index)
            && level.wanted()
        {
            *level = FilePriority::Maximum;
        }
    }
    levels
}

// What a torrent's checkpoint gave back.
enum FromCheckpoint {
    // It has none.
    Nothing,
    Restored(Restored<PieceRoot>),
    // It could not be read, or did not restore, and why.
    Unread(String),
    Failed(String),
}

// Where the torrent's job keeps its files.
fn storage_dir(torrent: &Live) -> PathBuf {
    PathBuf::from(torrent.record.storage_dir.clone().unwrap_or_default())
}

// libtorrent `storage_utils::delete_files`: the torrent's files go, here
// with what its job made of them, and then every directory that leaves
// empty. Whatever else is in its directory stays.
pub(super) fn delete_written(dir: &Path, written: &[PathBuf]) {
    for path in written {
        let removed = match std::fs::symlink_metadata(path) {
            Ok(found) if found.is_dir() => std::fs::remove_dir_all(path),
            Ok(_) => std::fs::remove_file(path),
            Err(error) => Err(error),
        };
        match removed {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), error = %error, "could not delete a torrent's file");
            }
        }
    }
    remove_empty(dir);
}

// The directory, once every directory under it that is empty went first,
// if it is empty then.
fn remove_empty(dir: &Path) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
                remove_empty(&entry.path());
            }
        }
    }
    let _ = std::fs::remove_dir(dir);
}

// Whether the torrent's job unpacks, as its metainfo fixed it.
fn unpacks(torrent: &Live) -> bool {
    torrent.record.unpack.unwrap_or_default()
}

// The bytes in hand of the files that go to disk: the progress over the
// torrent's total, which leaves padding out as the session's own does
// (libtorrent `file_progress::total_on_disk`).
fn on_disk(layout: Option<&PieceLayout>, file_have: &[u64]) -> u64 {
    layout.map_or(0, |layout| {
        layout
            .files()
            .zip(file_have)
            .filter(|(file, _)| !file.padding)
            .map(|(_, have)| have)
            .sum()
    })
}

// The bytes of the files the torrent wants, and those of them in hand,
// padding left out.
fn wanted(layout: Option<&PieceLayout>, levels: &[FilePriority], file_have: &[u64]) -> (u64, u64) {
    layout.map_or((0, 0), |layout| {
        layout
            .files()
            .enumerate()
            .filter(|(index, file)| {
                !file.padding && levels.get(*index).copied().unwrap_or_default().wanted()
            })
            .fold((0, 0), |(length, have), (index, file)| {
                let in_hand = file_have.get(index).copied().unwrap_or(0);
                (length + file.length, have + in_hand)
            })
    })
}

// Every file's bytes in hand. The session's progress counts only the files
// above Ignored, so each file's own figure is what is kept. While the
// session checks the files, the checkpoint's figure stands.
fn take_file_have(kept: &mut Vec<u64>, engine: &Engine, reported: &[u64]) {
    if *engine != Engine::Checking && !reported.is_empty() {
        kept.clear();
        kept.extend_from_slice(reported);
    }
}

// A torrent as a job in `status`.
fn report(view: TorrentView, status: JobStatus, now_ms: i64) -> JobReport {
    JobReport {
        job_id: view.job_id,
        protocol: Protocol::Torrent,
        phase_progress: downloading(&view, now_ms).into_iter().collect(),
        name: view.name,
        status,
        progress: view.progress,
        total_bytes: view.total_bytes,
        downloaded_bytes: view.have_bytes,
        category: view.category,
        output_dir: view.storage_dir,
        wait_reason: view.detail,
        created_at_ms: view.added_at_ms,
        metadata: view.metadata,
    }
}
