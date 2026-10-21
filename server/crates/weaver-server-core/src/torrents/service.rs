// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
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
    ManagedTorrent, ResumePeers, Session, TorrentVersion, TrackerAnnounce, torrent_from_bytes,
};

use crate::SchedulerError;
use crate::StateError;
use crate::categories::CategoryConfig;
use crate::categories::model::{completion_parent, resolve_category_config};
use crate::jobs::JobId;
use crate::jobs::graph_checkpoint::Delta;
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
use crate::pipeline::graph::job::{GraphJob, Storage};
use crate::pipeline::graph::leaf::LeafError;
use crate::pipeline::graph::limits::{self, Caps, ExtractionLimits};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::PieceLayout;
use crate::pipeline::graph::status::RepairSlots;
use crate::proxies::TorrentScope;
use crate::settings::model::{TrackerRule, tracker_rule_for};
use crate::settings::{
    Client, Config, SeedingAction, SeedingPolicy, TorrentConfig, resolve_seeding_policy,
};
use crate::torrents::admission::{self, Manifest, Refusal};
use crate::torrents::intake::{Heard as IntakeHeard, Holds, Intake, Room};
use crate::torrents::rate::limit_kind;
use crate::torrents::sessions::{NetworkRoutes, RouteKey, SessionEnv, SessionError, Sessions};
use crate::torrents::status::{self, Engine, Held, Observed, Queued};
use crate::torrents::storage::{PieceStorageFactory, Pieces, TorrentJob, initial_have, layout_of};
use crate::torrents::{
    FilePriority, Infohash, PeerInfo, TorrentCounters, TorrentFile, TorrentInsert, TorrentIntent,
    TorrentRecord, TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
};

#[cfg(test)]
mod tests;
pub mod updates;

/// How often torrents are read and their routes checked.
pub const TICK: Duration = Duration::from_secs(10);

/// The longest a torrent's ledger goes unwritten.
pub const LEDGER_EVERY_MS: i64 = 60_000;

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

    fn policy(&self, record: &TorrentRecord, rules: &[TrackerRule]) -> Option<SeedingPolicy> {
        resolve_seeding_policy(
            record.policy_override.as_ref(),
            rule_of(record, rules).and_then(|rule| rule.seeding.as_ref()),
            self.category_seeding(record),
            self.seeding.as_ref(),
        )
    }

    // The policy the torrent would have without its own override.
    fn inherited(&self, record: &TorrentRecord, rules: &[TrackerRule]) -> Option<SeedingPolicy> {
        resolve_seeding_policy(
            None,
            rule_of(record, rules).and_then(|rule| rule.seeding.as_ref()),
            self.category_seeding(record),
            self.seeding.as_ref(),
        )
    }

    // Whether the torrent's archives are extracted: its own value, else its
    // tracker rule's, else its category's, else the global default.
    fn unpack(&self, record: &TorrentRecord, rules: &[TrackerRule]) -> bool {
        record
            .unpack
            .or_else(|| rule_of(record, rules).and_then(|rule| rule.unpack))
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
    fn seeding_source(&self, record: &TorrentRecord, rules: &[TrackerRule]) -> SeedingSource {
        let rule = rule_of(record, rules).filter(|rule| rule.seeding.is_some());
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

// The tracker rule `record` matched, while it stands.
fn rule_of<'r>(record: &TorrentRecord, rules: &'r [TrackerRule]) -> Option<&'r TrackerRule> {
    let id = record.tracker_rule_id?;
    rules.iter().find(|rule| rule.id == id)
}

// How `record` names itself to peers and trackers: as the client its tracker
// rule names, else Weaver, under a fresh peer id.
fn identity_of(record: &TorrentRecord, rules: &[TrackerRule]) -> Identity {
    rule_of(record, rules)
        .and_then(|rule| rule.client)
        .unwrap_or(Client::Weaver)
        .name()
        .identity()
}

// The limits `record`'s session holds it to: its own, and on upload its
// seeding policy's cap where that is tighter. Each is a channel the
// torrent's peers draw from beside the session's, as each peer class over a
// libtorrent peer is (`bandwidth_manager::request_bandwidth`).
fn ratelimits_of(record: &TorrentRecord, policy: Option<SeedingPolicy>) -> LimitsConfig {
    let cap = policy.and_then(|policy| policy.max_upload_bps);
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
        let routes = NetworkRoutes::new(
            db.clone(),
            Arc::clone(proxies),
            ROUTE_TIMEOUT,
            crate::torrents::identity::IDENTITY,
        );
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
    /// Who holds it stopped, while a hold does.
    pub paused_by: Option<HoldOrigin>,
    pub category: Option<String>,
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
    /// Fetched bytes whose pieces did not verify.
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

fn tracker(tier: u32, url: &[u8]) -> Option<TrackerEntry> {
    let url = std::str::from_utf8(url).ok()?.trim();
    (!url.is_empty()).then(|| TrackerEntry {
        tier,
        url: url.to_owned(),
    })
}

// `trackers` as a torrent lists them: by tier, in the order given within a
// tier, each URL once where it is first named (libtorrent
// `torrent::replace_trackers`). A tracker that is no URL is left out, as a
// session leaves it out.
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

// The tiers of `trackers`, listed by tier, as a session announces to them.
fn tracker_tiers(trackers: &[TrackerEntry]) -> Vec<Vec<weaver_torrent::Url>> {
    let mut tiers: Vec<(u32, Vec<weaver_torrent::Url>)> = Vec::new();
    for entry in trackers {
        let Ok(url) = weaver_torrent::Url::parse(&entry.url) else {
            continue;
        };
        match tiers.last_mut() {
            Some((tier, urls)) if *tier == entry.tier => urls.push(url),
            _ => tiers.push((entry.tier, vec![url])),
        }
    }
    tiers.into_iter().map(|(_, urls)| urls).collect()
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
}

// What the service keeps of each torrent while it lives.
struct Live {
    record: TorrentRecord,
    names: Vec<String>,
    total: u64,
    layout: Option<Arc<PieceLayout>>,
    job: Option<Arc<TorrentJob>>,
    intake: Option<Arc<Intake>>,
    attached: Option<Attached>,
    route: Option<RouteKey>,
    held: Option<Held>,
    about: About,
    counters: TorrentCounters,
    // What the session reported at the last tick, for the ledger's deltas.
    seen: Seen,
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
    peers: (u32, u32),
    // The directory claimed for a move, while the move waits or is under
    // way; the job does the moving.
    moving: Option<PathBuf>,
    // Each file's bytes in hand: the checkpoint's until the session has
    // checked the files, then the session's.
    file_have: Vec<u64>,
}

// `torrent` as the download queue and its holds see it. A complete torrent
// under a tracker rule that never pauses seeds through the schedule's holds
// until its goal is met.
fn queued(settings: &TorrentSettings, rules: &[TrackerRule], torrent: &Live, now: i64) -> Queued {
    let complete = torrent.complete();
    // A torrent under a never-pause rule still owes its tracker while it
    // downloads, and while it seeds with a goal left.
    let runs_through_schedule = rule_of(&torrent.record, rules)
        .is_some_and(|rule| rule.never_pause)
        && (!complete
            || status::goal_left(
                settings.policy(&torrent.record, rules).as_ref(),
                &torrent.counters,
                torrent.total,
                now,
            )
            .is_some());
    Queued {
        job_id: torrent.record.job_id,
        complete,
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

    fn new(record: TorrentRecord, counters: TorrentCounters) -> Self {
        Self {
            record,
            names: Vec::new(),
            total: 0,
            layout: None,
            job: None,
            intake: None,
            attached: None,
            route: None,
            held: None,
            about: About::default(),
            counters,
            seen: Seen::default(),
            engine: Engine::Absent,
            run_error: None,
            queued: false,
            paused_by: None,
            levels: Vec::new(),
            speeds: (0, 0),
            peers: (0, 0),
            moving: None,
            file_have: Vec::new(),
        }
    }

    fn complete(&self) -> bool {
        self.record.completed_at_ms.is_some()
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

    // Why unpacking refused the torrent's output, while that stands.
    fn refusal(&self) -> Option<String> {
        self.job.as_ref()?.with(|job| {
            job.writer()
                .refusal()
                .map(|refusal| refusal.job_failure_reason())
        })
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

    // The pieces, and how many of them the job holds verified.
    fn pieces(&self) -> Option<TorrentPieces> {
        let (layout, job) = (self.layout.as_ref()?, self.job.as_ref()?);
        Some(TorrentPieces {
            count: layout.piece_count(),
            size: layout.piece_length(),
            have: job.with(|job| job.have().count_ones()) as u32,
        })
    }

    // How announcing to each tracker of the session's latest run went.
    fn announces(&self) -> Vec<TrackerAnnounce> {
        self.attached
            .as_ref()
            .map(|attached| attached.handle.tracker_announces())
            .unwrap_or_default()
    }

    // What the session says of the torrent now, over what was kept of it:
    // the one reading the views show and the ledger keeps.
    fn reading(&self, now: i64) -> Reading {
        let Some(attached) = &self.attached else {
            return Reading {
                engine: Engine::Absent,
                file_have: self.file_have.clone(),
                counters: self.counters,
                seen: self.seen,
                speeds: (0, 0),
                peers: (0, 0),
            };
        };
        let stats = attached.handle.stats();
        let engine = Engine::of(&stats);
        let mut file_have = self.file_have.clone();
        take_file_have(&mut file_have, &engine, &stats.file_progress);
        let live = stats.live.as_ref();
        let seen = Seen {
            uploaded: stats.uploaded_bytes,
            fetched: live.map_or(0, |live| live.snapshot.fetched_bytes),
            checked: live.map_or(0, |live| live.snapshot.downloaded_and_checked_bytes),
        };
        // The session counts from zero each time it takes the torrent.
        let delta = |now: u64, then: u64| if now >= then { now - then } else { now };
        let (up, down) = (
            delta(seen.uploaded, self.seen.uploaded),
            delta(seen.fetched, self.seen.fetched),
        );
        let mut counters = self.counters;
        counters.uploaded_bytes += up;
        counters.downloaded_bytes += down;
        counters.checked_bytes += delta(seen.checked, self.seen.checked);
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
            speeds: live.map_or((0, 0), |live| {
                (live.download_speed.as_bytes(), live.upload_speed.as_bytes())
            }),
            peers: live.map_or((0, 0), |live| {
                (
                    live.snapshot.peer_stats.live,
                    live.snapshot.peer_stats.live_seeders,
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
    // The session's own totals, which the ledger's deltas are taken from.
    seen: Seen,
    speeds: (u64, u64),
    peers: (u32, u32),
}

// The session's own totals for a torrent, since it last took it.
#[derive(Debug, Clone, Copy, Default)]
struct Seen {
    uploaded: u64,
    fetched: u64,
    checked: u64,
}

// What the job side asks the service to look at: the jobs that changed,
// the ones whose checkpoint is behind them, and the writes a job's intake
// could not make.
#[derive(Default)]
pub(super) struct Signal {
    dirty: Mutex<BTreeSet<JobId>>,
    unsaved: Mutex<BTreeSet<JobId>>,
    refused: Mutex<BTreeMap<JobId, String>>,
    wake: Notify,
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Signal {
    pub(super) fn mark(&self, job_id: JobId) {
        locked(&self.dirty).insert(job_id);
        locked(&self.unsaved).insert(job_id);
        self.wake.notify_one();
    }

    fn heard(&self, job_id: JobId, heard: IntakeHeard) {
        if let IntakeHeard::Refused(error) = heard {
            locked(&self.refused).entry(job_id).or_insert(error);
        }
        self.mark(job_id);
    }

    fn take(&self) -> BTreeSet<JobId> {
        std::mem::take(&mut *locked(&self.dirty))
    }

    fn take_refused(&self, job_id: JobId) -> Option<String> {
        locked(&self.refused).remove(&job_id)
    }

    fn take_unsaved(&self) -> BTreeSet<JobId> {
        std::mem::take(&mut *locked(&self.unsaved))
    }

    fn take_unsaved_one(&self, job_id: JobId) -> bool {
        locked(&self.unsaved).remove(&job_id)
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

/// What the service has written so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    pub ledgers: u64,
    pub checkpoints: u64,
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
    signal: Arc<Signal>,
    ticks: watch::Sender<u64>,
    retired: watch::Sender<u64>,
    written: watch::Sender<Written>,
    last_tick_ms: Mutex<Option<i64>>,
    queue: Mutex<Option<SchedulerHandle>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    tick: Mutex<Option<tokio::task::JoinHandle<()>>>,
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
                signal: Arc::new(Signal::default()),
                ticks: watch::channel(0).0,
                retired: watch::channel(0).0,
                written: watch::channel(Written::default()).0,
                last_tick_ms: Mutex::new(None),
                queue: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
                tick: Mutex::new(None),
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
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let job_id = torrent.record.job_id;
                let trackers = self
                    .inner
                    .db
                    .list_torrent_trackers(job_id)
                    .map_err(state_error)?;
                let rule_id = self.rule_id(&trackers);
                if rule_id != torrent.record.tracker_rule_id {
                    self.inner
                        .db
                        .set_torrent_tracker_rule(job_id, rule_id)
                        .map_err(state_error)?;
                    torrent.record.tracker_rule_id = rule_id;
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
                && let Some(id) = torrent.record.tracker_rule_id
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
    }

    async fn turn(&self, on: bool) {
        self.inner.sessions.turn(on).await;
        if on {
            self.spawn_tick();
        } else {
            self.stop_tick();
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                self.detach(torrent).await;
                torrent.held = Some(Held::Off);
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

    // The torrents, as queue items, into the published queue.
    async fn publish(&self) {
        if let Some(queue) = self.queue() {
            queue.publish_owned_jobs(self.job_infos().await);
        }
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
        let attached: Vec<(JobId, Arc<ManagedTorrent>)> = self
            .inner
            .live
            .lock()
            .await
            .iter()
            .filter_map(|(job_id, torrent)| {
                Some((*job_id, torrent.attached.as_ref()?.handle.clone()))
            })
            .collect();
        for (job_id, handle) in attached {
            self.save_peers(job_id, &handle).await;
        }
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

    /// Counts the ledgers and checkpoints written.
    pub fn written(&self) -> watch::Receiver<Written> {
        self.inner.written.subscribe()
    }

    /// Bring back every stored torrent, then keep them: the job mail, the
    /// settling of what jobs did, and, while torrents are on, the tick.
    pub async fn start(&self) -> Result<(), String> {
        self.open().await?;
        if self.inner.sessions.on() {
            self.spawn_tick();
        }
        Ok(())
    }

    fn spawn_tick(&self) {
        let service = self.clone();
        let tick = tokio::spawn(async move {
            let mut interval = tokio::time::interval(TICK);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
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
        self.restore().await
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
    // is done completes it.
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
        self.apply_picking().await;
        self.publish().await;
    }

    // Write the checkpoints of `jobs`, and beside each the peers and bans of
    // its torrent while it is in a session.
    async fn save_checkpoints(&self, jobs: BTreeSet<JobId>) {
        type Saved = (JobId, Arc<TorrentJob>, Option<Arc<ManagedTorrent>>);
        let jobs: Vec<Saved> = {
            let live = self.inner.live.lock().await;
            jobs.into_iter()
                .filter_map(|job_id| {
                    let torrent = live.get(&job_id)?;
                    let handle = torrent.attached.as_ref().map(|a| a.handle.clone());
                    Some((job_id, torrent.job.clone()?, handle))
                })
                .collect()
        };
        for (job_id, job, handle) in jobs {
            self.save_checkpoint(job_id, &job).await;
            if let Some(handle) = handle {
                self.save_peers(job_id, &handle).await;
            }
        }
    }

    // Write the peers and bans the torrent begins its next start with
    // (libtorrent `torrent::write_resume_data`). A torrent stopped by an
    // error keeps those last written.
    async fn save_peers(&self, job_id: JobId, handle: &ManagedTorrent) {
        let Some(peers) = handle.resume_peers() else {
            return;
        };
        if let Err(error) = self.write_peers(job_id, peers).await {
            tracing::warn!(job_id = job_id.0, %error, "could not save a torrent's peers");
        }
    }

    // The one writer of a torrent's saved peers and bans.
    async fn write_peers(&self, job_id: JobId, peers: ResumePeers) -> Result<(), String> {
        let landed = self.queue_save(job_id, "torrent_peers", move |db| {
            db.save_torrent_peers(job_id, &peers)
        });
        match landed.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(error)) => Err(state_error(error)),
            Err(error) => Err(error.to_string()),
        }
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

    async fn save_checkpoint(&self, job_id: JobId, job: &TorrentJob) {
        let snapshot = job.with(|job| job.snapshot());
        // A torrent job's checkpoint is written whole: its rows change little
        // and seldom.
        let rows = Arc::new(Delta::between(None, &checkpoint::rows(&snapshot)));
        let landed = self.queue_save(job_id, "torrent_checkpoint", move |db| {
            db.save_graph_checkpoint(job_id, rows)
        });
        match landed.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not checkpoint a torrent job");
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not checkpoint a torrent job");
            }
        }
        self.inner
            .written
            .send_modify(|written| written.checkpoints += 1);
    }

    async fn completed(&self, job_id: JobId) {
        let now = self.inner.clock.now_ms();
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id) else {
            return;
        };
        if torrent.complete() {
            return;
        }
        torrent.record.completed_at_ms = Some(now);
        let (intent, at) = (torrent.record.intent, torrent.record.completed_at_ms);
        drop(live);
        if let Err(error) = self.inner.db.update_torrent_intent(job_id, intent, at) {
            tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's completion");
        }
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

    // The job of a torrent with metainfo, from its checkpoint when it has one.
    fn job(
        &self,
        job_id: JobId,
        dir: PathBuf,
        layout: &Arc<PieceLayout>,
        unpack: bool,
    ) -> (Arc<TorrentJob>, Arc<Intake>) {
        let slots = self.inner.env.slots;
        let restored = self
            .inner
            .db
            .load_graph_checkpoint(job_id)
            .ok()
            .flatten()
            .and_then(|rows| checkpoint::snapshot(&rows).ok())
            .and_then(|snapshot| {
                GraphJob::restore_torrent(
                    &snapshot,
                    Arc::clone(layout),
                    job_id.0,
                    self.storage(dir.clone(), layout, unpack),
                    slots,
                )
                .ok()
            });
        let job = match restored {
            Some(restored) => restored.job,
            None => GraphJob::for_torrent(
                job_id.0,
                Arc::clone(layout),
                self.storage(dir, layout, unpack),
            ),
        };
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

    // The torrent's job, over the metainfo. Whether it unpacks is fixed
    // here and kept, so a restart builds the same tree.
    fn give_metainfo(&self, torrent: &mut Live, parsed: &Parsed) {
        let job_id = torrent.record.job_id;
        let unpack = self.settings().unpack(&torrent.record, &self.rules());
        if torrent.record.unpack.is_none() {
            if let Err(error) = self.inner.db.set_torrent_unpack(job_id, unpack) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record whether a torrent unpacks");
            }
            torrent.record.unpack = Some(unpack);
        }
        let dir = PathBuf::from(torrent.record.storage_dir.clone().unwrap_or_default());
        let (job, intake) = self.job(job_id, dir, &parsed.layout, unpack);
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
        torrent.file_have = job.with(|job| job.file_have());
        torrent.job = Some(job);
        torrent.intake = Some(intake);
        torrent.about = parsed.about.clone();
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
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    parsed.hashes,
                    parsed.manifest.name.clone(),
                    TorrentSource::Metainfo,
                    Some(bytes),
                    parsed.private,
                    Some(dir.clone()),
                );
                if let Err(error) = self.insert(&mut record, &parsed.trackers) {
                    give_back(&dir).await;
                    return Err(error);
                }
                let mut torrent = Live::new(record, TorrentCounters::default());
                self.give_metainfo(&mut torrent, &parsed);
                self.inner.live.lock().await.insert(job_id, torrent);
                self.apply_queue().await;
                self.attach(job_id).await;
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
                    .filter_map(|url| tracker(0, url.as_bytes()))
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
                    .session(&plan)
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
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    hashes,
                    name,
                    TorrentSource::Magnet,
                    None,
                    false,
                    None,
                );
                self.insert(&mut record, &trackers)?;
                let identity = identity_of(&record, &rules);
                let mut torrent = Live::new(record, TorrentCounters::default());
                torrent.route = Some(plan.key.clone());
                self.inner.live.lock().await.insert(job_id, torrent);
                self.spawn_resolve(job_id, session, uri, None, identity);
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
        private: bool,
        storage_dir: Option<String>,
    ) -> TorrentRecord {
        TorrentRecord {
            job_id,
            infohash_v1: hashes.v1.map(|hash| hash.0),
            infohash_v2: hashes.v2.map(|hash| hash.0),
            name,
            source,
            metainfo,
            private,
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
            tracker_rule_id: None,
            unpack: request.unpack,
            metadata: request.metadata.clone(),
            upload_limit: None,
            download_limit: None,
            sequential: false,
            first_last_piece_first: false,
        }
    }

    // Keep `record` with the rule its trackers match.
    fn insert(
        &self,
        record: &mut TorrentRecord,
        trackers: &[TrackerEntry],
    ) -> Result<(), AddError> {
        record.tracker_rule_id = self.rule_id(trackers);
        match self.inner.db.insert_torrent(record, trackers) {
            Ok(TorrentInsert::Inserted) => Ok(()),
            Ok(TorrentInsert::Duplicate(existing)) => Err(AddError::Duplicate(existing)),
            Err(error) => Err(AddError::State(state_error(error))),
        }
    }

    // Fetch a magnet's metainfo on its session, then admit it as any other.
    fn spawn_resolve(
        &self,
        job_id: JobId,
        session: Arc<Session>,
        uri: String,
        trackers: Option<Vec<String>>,
        identity: Identity,
    ) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            let listed = session
                .add_torrent(
                    AddTorrent::from_url(uri),
                    Some(AddTorrentOptions {
                        list_only: true,
                        trackers,
                        identity: Some(identity),
                        ..Default::default()
                    }),
                )
                .await;
            let bytes = match listed {
                Ok(AddTorrentResponse::ListOnly(listed)) => listed.torrent_bytes,
                Ok(_) => {
                    service
                        .fail(job_id, "the session did not return the metainfo".into())
                        .await;
                    return;
                }
                Err(error) => {
                    service.fail(job_id, format!("{error:#}")).await;
                    return;
                }
            };
            if let Err(error) = service.resolved(job_id, bytes).await {
                service.fail(job_id, error.to_string()).await;
            }
        }));
    }

    async fn resolved(&self, job_id: JobId, bytes: Bytes) -> Result<(), AddError> {
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
        torrent.record.private = parsed.private;
        torrent.record.storage_dir = Some(dir.clone());
        torrent.record.metainfo = Some(bytes.clone());
        // libtorrent `torrent::set_metadata` keeps the trackers the magnet
        // listed, edits included; the metainfo's own join after them.
        let mut trackers = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .map_err(|error| AddError::State(state_error(error)))?;
        trackers.extend(parsed.trackers.iter().cloned());
        let trackers = tidy_trackers(trackers);
        let rule_id = self.rule_id(&trackers);
        self.inner
            .db
            .set_torrent_tracker_rule(job_id, rule_id)
            .map_err(|error| AddError::State(state_error(error)))?;
        torrent.record.tracker_rule_id = rule_id;
        self.inner
            .db
            .set_torrent_metainfo(
                job_id,
                torrent.record.infohash_v1,
                torrent.record.infohash_v2,
                &parsed.manifest.name,
                &bytes,
                parsed.private,
                &dir,
                &trackers,
            )
            .map_err(|error| AddError::State(state_error(error)))?;
        self.give_metainfo(torrent, &parsed);
        drop(live);
        self.attach(job_id).await;
        Ok(())
    }

    // Fetch a stored magnet's metainfo again, on its route's session once
    // the route is up.
    async fn resolve(&self, job_id: JobId) {
        let Some(record) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| torrent.record.clone())
        else {
            return;
        };
        let rules = self.rules();
        let scope = self
            .settings()
            .scope(record.category_id, rule_of(&record, &rules));
        let plan = match self.inner.sessions.routes().plan(scope) {
            Ok(plan) => plan,
            Err(reason) => return self.held(job_id, None, Held::RouteDown(reason)).await,
        };
        let session = match self.inner.sessions.session(&plan).await {
            Ok(session) => session,
            Err(error) => return self.held(job_id, Some(plan.key), held_by(error)).await,
        };
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.route = Some(plan.key);
            torrent.held = None;
        }
        let uri = Magnet::of(hashes_of(&record), None, Vec::new()).to_string();
        let trackers = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .unwrap_or_default()
            .into_iter()
            .map(|tracker| tracker.url)
            .collect();
        let identity = identity_of(&record, &rules);
        self.spawn_resolve(job_id, session, uri, Some(trackers), identity);
    }

    // Put a torrent in its route's session, or hold it while the route is
    // down.
    async fn attach(&self, job_id: JobId) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let (scope, bytes, layout, intake, dir, paused, ratelimits, have, identity, picking) = {
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
            let ratelimits =
                ratelimits_of(&torrent.record, settings.policy(&torrent.record, &rules));
            (
                settings.scope(torrent.record.category_id, rule_of(&torrent.record, &rules)),
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
                identity_of(&torrent.record, &rules),
                (
                    fetch_levels(torrent, job),
                    torrent.record.sequential,
                    torrent.record.first_last_piece_first,
                ),
            )
        };
        let plan = match self.inner.sessions.routes().plan(scope) {
            Ok(plan) => plan,
            Err(reason) => {
                self.held(job_id, None, Held::RouteDown(reason)).await;
                return;
            }
        };
        let session = match self.inner.sessions.session(&plan).await {
            Ok(session) => session,
            Err(error) => {
                self.held(job_id, Some(plan.key), held_by(error)).await;
                return;
            }
        };
        // libtorrent `torrent::read_resume_data`: the saved peers are known
        // from the start and the saved bans hold.
        let saved = self.inner.db.torrent_peers(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's saved peers");
            Default::default()
        });
        // libtorrent `torrent::read_resume_data`: the trackers the torrent
        // lists now stand in for its metainfo's.
        let trackers = match self.inner.db.list_torrent_trackers(job_id) {
            Ok(trackers) => tracker_tiers(&trackers)
                .into_iter()
                .map(|tier| tier.into_iter().map(String::from).collect())
                .collect(),
            Err(error) => {
                self.fail(job_id, state_error(error)).await;
                return;
            }
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
            tracker_tiers: Some(trackers),
            initial_peers: (!saved.peers.is_empty()).then_some(saved.peers),
            banned_peers: saved.banned,
            file_priorities: Some(picking.0),
            sequential: picking.1,
            first_last_piece_first: picking.2,
            ..Default::default()
        };
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
                torrent.attached = Some(Attached { session, handle });
                torrent.route = Some(plan.key);
                torrent.held = None;
                torrent.seen = Seen::default();
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
            torrent.route = key.or(torrent.route.take());
            torrent.held = Some(held);
        }
    }

    // Take a torrent out of its session, its checkpoint written first when
    // it is behind the job.
    async fn detach(&self, torrent: &mut Live) {
        let job_id = torrent.record.job_id;
        if let Some(job) = &torrent.job
            && self.inner.signal.take_unsaved_one(job_id)
        {
            self.save_checkpoint(job_id, job).await;
        }
        if let Some(attached) = torrent.attached.take() {
            self.save_peers(job_id, &attached.handle).await;
            let id = TorrentIdOrHash::Id(attached.handle.id());
            if let Err(error) = attached.session.delete(id, false).await {
                tracing::debug!(error = %error, "a torrent was already gone from its session");
            }
        }
        torrent.engine = Engine::Absent;
        torrent.run_error = None;
    }

    // The torrent cannot go on: it leaves as a failed job and its data stays.
    async fn fail(&self, job_id: JobId, error: String) {
        let torrent = self.inner.live.lock().await.remove(&job_id);
        let Some(mut torrent) = torrent else { return };
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
        let (report, hash) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            torrent.record.intent = TorrentIntent::Stopped;
            if let Err(error) = self.save_intent(&torrent.record) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not keep a torrent's stop");
            }
            let report = report(
                self.view(&settings, torrent, now),
                JobStatus::Failed { error },
                now,
            );
            (report, hashes_of(&torrent.record).best())
        };
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
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let mut live = self.inner.live.lock().await;
        let torrent = live.get(&job_id).ok_or(ActionError::NotFound(job_id))?;
        if torrent.record.private && !confirm_unmet_goal {
            let policy = settings.policy(&torrent.record, &self.rules());
            if let Some(left) =
                status::goal_left(policy.as_ref(), &torrent.counters, torrent.total, now)
            {
                return Err(ActionError::UnmetGoal(left));
            }
        }
        let mut torrent = live.remove(&job_id).ok_or(ActionError::NotFound(job_id))?;
        drop(live);
        self.detach(&mut torrent).await;
        if delete_files && let Some(dir) = torrent.record.storage_dir.clone() {
            match tokio::fs::remove_dir_all(&dir).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not delete a torrent's files");
                }
            }
        }
        // The job's outcome: complete, refused by unpacking, or cut short.
        let (status, error) = match (torrent.complete(), torrent.refusal()) {
            (true, _) => ("complete", None),
            (false, Some(refusal)) => ("failed", Some(refusal)),
            (false, None) => ("cancelled", None),
        };
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

    fn save_options(&self, record: &TorrentRecord) -> Result<(), ActionError> {
        self.inner
            .db
            .update_torrent_options(
                record.job_id,
                record.category_id,
                record.forced,
                record.policy_override.clone(),
            )
            .map(|_| ())
            .map_err(|error| ActionError::State(state_error(error)))
    }

    fn save_intent(&self, record: &TorrentRecord) -> Result<(), ActionError> {
        self.inner
            .db
            .update_torrent_intent(record.job_id, record.intent, record.completed_at_ms)
            .map(|_| ())
            .map_err(|error| ActionError::State(state_error(error)))
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
        let held = self
            .handle_of(job_id)
            .await?
            .and_then(|handle| handle.ban_peers(ips));
        let peers = match held {
            Some(peers) => peers,
            // In no session, or stopped by an error: the bans join the list
            // it saved, which its next start begins with.
            None => {
                let db = self.inner.db.clone();
                let mut saved = tokio::task::spawn_blocking(move || db.torrent_peers(job_id))
                    .await
                    .map_err(|error| ActionError::State(error.to_string()))?
                    .map_err(|error| ActionError::State(state_error(error)))?;
                ips.iter().for_each(|ip| saved.ban(*ip));
                saved
            }
        };
        self.write_peers(job_id, peers)
            .await
            .map_err(ActionError::State)
    }

    /// The torrent's trackers, each with how announcing to it went.
    pub async fn trackers(&self, job_id: JobId) -> Result<Vec<TorrentTracker>, ActionError> {
        self.torrent(job_id)
            .await
            .map(|view| view.trackers)
            .ok_or(ActionError::NotFound(job_id))
    }

    /// Add `tiers` after the torrent's last tier, leaving out the trackers
    /// it already lists (libtorrent `torrent::add_tracker`; qBittorrent
    /// `TorrentImpl::addTrackers`).
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
                trackers.extend(urls.into_iter().map(|url| TrackerEntry { tier, url }));
            }
            Ok(trackers)
        })
        .await
    }

    /// List `trackers` in place of the torrent's own (libtorrent
    /// `torrent::replace_trackers`): announces to a tracker it no longer
    /// lists stop.
    pub async fn replace_trackers(
        &self,
        job_id: JobId,
        trackers: Vec<TrackerEntry>,
    ) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |_| Ok(trackers)).await
    }

    /// Stop listing each of `urls` (qBittorrent `TorrentImpl::removeTrackers`).
    pub async fn remove_trackers(&self, job_id: JobId, urls: &[String]) -> Result<(), ActionError> {
        self.edit_trackers(job_id, |mut trackers| {
            trackers.retain(|tracker| !urls.iter().any(|url| url.trim() == tracker.url));
            Ok(trackers)
        })
        .await
    }

    // Write the trackers `edit` makes of the torrent's list and hand them
    // to its session. The rule they match is picked again in memory only:
    // the stored rule is not this path's to write.
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
            let trackers = edit(listed)?;
            tracker_urls(&trackers)?;
            let trackers = tidy_trackers(trackers);
            let stored = self
                .inner
                .db
                .set_torrent_trackers(job_id, &trackers)
                .map_err(|error| ActionError::State(state_error(error)))?;
            if !stored {
                return Err(ActionError::NotFound(job_id));
            }
            torrent.record.tracker_rule_id = self.rule_id(&trackers);
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
        let record = self
            .with_torrent(job_id, |torrent| {
                torrent.record.intent = TorrentIntent::Paused;
                Ok(torrent.record.clone())
            })
            .await?;
        self.save_intent(&record)?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Resume a paused torrent, or one its seeding goal stopped.
    pub async fn resume(&self, job_id: JobId) -> Result<(), ActionError> {
        let record = self
            .with_torrent(job_id, |torrent| {
                torrent.record.intent = TorrentIntent::Resumed;
                Ok(torrent.record.clone())
            })
            .await?;
        self.save_intent(&record)?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    pub async fn set_forced(&self, job_id: JobId, forced: bool) -> Result<(), ActionError> {
        let record = self
            .with_torrent(job_id, |torrent| {
                torrent.record.forced = forced;
                Ok(torrent.record.clone())
            })
            .await?;
        self.save_options(&record)?;
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
            .with_torrent(job_id, |torrent| Ok(torrent.moving.take()))
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
        let record = self
            .with_torrent(job_id, |torrent| {
                torrent.record.policy_override = policy;
                Ok(torrent.record.clone())
            })
            .await?;
        self.save_options(&record)?;
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
            let rule = rule_of(&torrent.record, &rules);
            let before = settings.scope(torrent.record.category_id, rule);
            torrent.record.category_id = category_id;
            self.save_options(&torrent.record)?;
            let moved = before != settings.scope(category_id, rule);
            if moved {
                self.detach(torrent).await;
                torrent.route = None;
                torrent.held = None;
            }
            moved
        };
        if moved {
            self.attach(job_id).await;
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

    // Bring back every stored torrent.
    async fn restore(&self) -> Result<(), String> {
        self.load_rules()?;
        let records = self.inner.db.list_torrents().map_err(state_error)?;
        for record in records {
            let job_id = record.job_id;
            let counters = self
                .inner
                .db
                .get_torrent_counters(job_id)
                .ok()
                .flatten()
                .unwrap_or_default();
            let mut torrent = Live::new(record.clone(), counters);
            match record.metainfo.as_deref().map(parse) {
                Some(Ok(parsed)) => {
                    self.give_metainfo(&mut torrent, &parsed);
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
        let attach: Vec<JobId> = self.inner.live.lock().await.keys().copied().collect();
        for job_id in attach {
            self.attach(job_id).await;
        }
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
        let now = self.inner.clock.now_ms();
        let elapsed_s = {
            let mut last = self
                .inner
                .last_tick_ms
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let elapsed = last.map_or(0, |last| ((now - last).max(0) / 1000) as u64);
            *last = Some(now);
            elapsed
        };
        self.follow_routes().await;
        self.read(now, elapsed_s).await;
        self.save_checkpoints(self.inner.signal.take_unsaved())
            .await;
        self.apply_goals(now).await;
        self.apply_queue().await;
        self.apply_picking().await;
        self.apply_limits().await;
        self.drop_idle_sessions().await;
        self.publish().await;
        self.inner.ticks.send_modify(|ticks| *ticks += 1);
    }

    // A torrent whose route changed moves to the new route's session; one
    // whose route went down is held, and comes back when it is up.
    async fn follow_routes(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let mut reattach = Vec::new();
        let mut down: BTreeMap<RouteKey, String> = BTreeMap::new();
        let mut unresolved = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                if torrent.layout.is_none() {
                    if torrent.held.is_some() {
                        unresolved.push(torrent.record.job_id);
                    }
                    continue;
                }
                let scope =
                    settings.scope(torrent.record.category_id, rule_of(&torrent.record, &rules));
                match self.inner.sessions.routes().plan(scope) {
                    Ok(plan) => {
                        if let Some(reason) = &plan.down {
                            down.insert(plan.key.clone(), reason.clone());
                            torrent.attached = None;
                            torrent.engine = Engine::Absent;
                            torrent.route = Some(plan.key);
                            torrent.held = Some(Held::RouteDown(reason.clone()));
                        } else if torrent.route.as_ref() != Some(&plan.key)
                            || torrent.attached.is_none()
                        {
                            self.detach(torrent).await;
                            torrent.held = None;
                            reattach.push(torrent.record.job_id);
                        }
                    }
                    Err(reason) => {
                        self.detach(torrent).await;
                        torrent.held = Some(Held::RouteDown(reason));
                    }
                }
            }
        }
        for (key, reason) in down {
            self.inner.sessions.hold(&key, reason).await;
        }
        for job_id in reattach {
            self.attach(job_id).await;
        }
        for job_id in unresolved {
            self.resolve(job_id).await;
        }
    }

    // The session's figures for each torrent, and the ledger kept from them.
    async fn read(&self, now: i64, elapsed_s: u64) {
        let mut changed = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let before = torrent.counters;
                let reading = torrent.reading(now);
                torrent.engine = reading.engine;
                torrent.file_have = reading.file_have;
                torrent.counters = reading.counters;
                torrent.seen = reading.seen;
                torrent.speeds = reading.speeds;
                torrent.peers = reading.peers;
                let (state, _) = status::project(&torrent.observed(torrent.engine.clone()));
                if state == TorrentState::Seeding {
                    torrent.counters.finished_duration_s += elapsed_s;
                }
                // Active is running in a session: not paused, held or queued.
                if matches!(torrent.engine, Engine::Live { .. }) {
                    torrent.counters.active_duration_s += elapsed_s;
                }
                if torrent.counters != before
                    || now - torrent.counters.updated_at_ms >= LEDGER_EVERY_MS
                {
                    torrent.counters.updated_at_ms = now;
                    changed.push((torrent.record.job_id, torrent.counters));
                }
            }
        }
        // Every ledger is queued before any is waited on, each in the step
        // that took it.
        let landed: Vec<_> = changed
            .into_iter()
            .map(|(job_id, counters)| {
                let landed = self.queue_save(job_id, "torrent_ledger", move |db| {
                    db.update_torrent_counters(job_id, &counters)
                });
                (job_id, landed)
            })
            .collect();
        for (job_id, landed) in landed {
            match landed.await {
                Ok(Ok(_)) => self
                    .inner
                    .written
                    .send_modify(|written| written.ledgers += 1),
                Ok(Err(error)) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not keep a torrent's ledger");
                }
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not keep a torrent's ledger");
                }
            }
        }
    }

    // A complete torrent whose seeding goal is met does what its policy says.
    async fn apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let mut actions = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                // A forced torrent seeds past its goal.
                if !torrent.complete() || !torrent.resumed() || torrent.record.forced {
                    continue;
                }
                let Some(policy) = settings.policy(&torrent.record, &rules) else {
                    continue;
                };
                if status::goal_left(Some(&policy), &torrent.counters, torrent.total, now).is_some()
                {
                    continue;
                }
                match policy.action {
                    SeedingAction::Stop => {
                        torrent.record.intent = TorrentIntent::Stopped;
                        if let Err(error) = self.save_intent(&torrent.record) {
                            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not keep a torrent's stop");
                        }
                    }
                    SeedingAction::Remove => actions.push((torrent.record.job_id, false)),
                    SeedingAction::RemoveWithData => actions.push((torrent.record.job_id, true)),
                }
            }
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
            let limits = ratelimits_of(&torrent.record, settings.policy(&torrent.record, &rules));
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
        let settings = self.settings();
        let routes = self.inner.sessions.routes();
        let existing: BTreeSet<RouteKey> = std::iter::once(TorrentScope::Default)
            .chain(
                settings
                    .categories
                    .iter()
                    .filter(|category| category.torrent_route)
                    .map(|category| TorrentScope::Category(category.id)),
            )
            .chain(
                self.rules()
                    .iter()
                    .filter(|rule| rule.torrent_route)
                    .map(|rule| TorrentScope::TrackerRule(rule.id)),
            )
            .filter_map(|scope| routes.plan(scope).ok())
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
        let reading = torrent.reading(now);
        let (state, detail) = status::project(&torrent.observed(reading.engine));
        let counters = &reading.counters;
        let record = &torrent.record;
        let total = torrent.total;
        let have = on_disk(torrent.layout.as_deref(), &reading.file_have);
        let rules = self.rules();
        let policy = settings.policy(record, &rules);
        let swarm = status::swarm(&torrent.announces(), now);
        let active = counters.active_duration_s;
        let average = |bytes: u64| bytes.checked_div(active).unwrap_or(0);
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
            paused_by: torrent.paused_by,
            category: settings
                .category(record.category_id)
                .map(|category| category.name.clone()),
            private: record.private,
            forced: record.forced,
            progress: if total == 0 {
                0.0
            } else {
                (have as f64 / total as f64).min(1.0)
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
            active_seconds: active,
            download_avg_bps: average(counters.downloaded_bytes),
            upload_avg_bps: average(counters.uploaded_bytes),
            wasted_bytes: counters.wasted_bytes(),
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
            seeding_inherited: settings.inherited(record, &rules),
            seeding_policy: policy.unwrap_or_default(),
            seeding_source: settings.seeding_source(record, &rules),
            unpack: settings.unpack(record, &rules),
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
        let (mut view, announces) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let mut view = self.view(&settings, torrent, now);
            view.files = torrent.files();
            (view, torrent.announces())
        };
        view.trackers = status::trackers(
            self.inner
                .db
                .list_torrent_trackers(job_id)
                .unwrap_or_default(),
            &announces,
        );
        Some(view)
    }

    /// Every torrent, or a category's, in the order they were added.
    pub async fn torrents(&self, category: Option<&str>) -> Vec<TorrentView> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let live = self.inner.live.lock().await;
        let mut views: Vec<TorrentView> = live
            .values()
            .map(|torrent| self.view(&settings, torrent, now))
            .filter(|view| {
                category.is_none_or(|name| {
                    view.category
                        .as_deref()
                        .is_some_and(|category| category.eq_ignore_ascii_case(name))
                })
            })
            .collect();
        views.sort_by_key(|view| (view.added_at_ms, view.job_id));
        views
    }

    /// The torrents as queue items.
    pub async fn job_infos(&self) -> Vec<JobInfo> {
        let now = self.inner.clock.now_ms();
        self.torrents(None)
            .await
            .into_iter()
            .map(|view| {
                let status = job_status(&view);
                report(view, status, now).job_info()
            })
            .collect()
    }

    /// The scope whose route a torrent added in `category` takes before its
    /// trackers are known.
    pub fn scope_of(&self, category: Option<&str>) -> TorrentScope {
        let settings = self.settings();
        let id = category
            .and_then(|name| resolve_category_config(&settings.categories, name))
            .map(|category| category.id);
        settings.scope(id, None)
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
        SessionError::Off => Held::Off,
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
        // A stop with any reason but its met goal is a failure.
        TorrentState::Stopped => match view.detail.as_deref() {
            None | Some(status::GOAL_MET) if view.completed_at_ms.is_some() => JobStatus::Complete,
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
