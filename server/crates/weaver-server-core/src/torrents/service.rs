// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, mpsc, oneshot, watch};
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_torrent::api::TorrentIdOrHash;
use weaver_torrent::core::{Id20, Id32, InfoHashes};
use weaver_torrent::limits::LimitsConfig;
use weaver_torrent::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, BF, Checkpoint, Identity, Magnet,
    ManagedTorrent, PeerConnectionOptions, ResumePeers, Session, SslIdentity, TorrentTotals,
    TorrentVersion, TrackerAnnounce, torrent_from_bytes, tracker::Tracker,
};

use crate::LoadError;
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
    ArchivePasswordCandidate, ArchivePasswordSource, FieldUpdate, JobStatus, JobUpdate,
    PRIORITY_ATTRIBUTE_KEY, Protocol, job_status_from_persisted_str,
};
use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress};
use crate::jobs::report::JobReport;
use crate::operations::disk::CapacityReader;
use crate::persistence::Database;
use crate::pipeline::ExtractionLimits;
use crate::pipeline::claim_complete_destination_path;
use crate::pipeline::graph::engine::{DecodePool, Outside, RunMail, Said};
use crate::pipeline::graph::job::Storage;
use crate::pipeline::graph::limits::{self, Caps};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::graph::window::ScratchLedger;
use crate::pipeline::post_health::names_a_piece;
use crate::post_processing::model::{QueueEvent, vars};
use crate::proxies::{
    Consumer, LegTenancy, LegTenant, NetworkRuntime, ResolvedBy, SessionTraffic, TorrentScope,
};
use crate::runtime::panic_trap::JobScope;
use crate::settings::model::{TrackerCertificate, TrackerRule, tracker_rule_for};
use crate::settings::{
    Client, Config, Leaf, Level, Levels, SeedTimeCounting, Seeding, SeedingAction, SeedingGoalMode,
    SeedingPolicy, TorrentConfig, TorrentEncryption,
};
use crate::torrents::admission::{self, Manifest, Refusal};
use crate::torrents::files::{self, Files};
use crate::torrents::identity::{ClientIdentity, TorrentIdentity};
use crate::torrents::intake::{Heard as IntakeHeard, Holds, Intake, Room};
use crate::torrents::layout::{PieceLayout, layout_of};
use crate::torrents::metrics::{
    Counters, FetchOutcome, Published, SeedingSeries, SessionSeries, TorrentEvent, TorrentMetrics,
    TorrentSeries,
};
use crate::torrents::model::Adopted;
use crate::torrents::rate::limit_kind;
use crate::torrents::relocate::{self, Existing, MoveError};
use crate::torrents::sessions::{
    NetworkRoutes, OPENING, RouteFailure, RouteKey, RoutePlan, SessionEnv, SessionError,
    SessionView, Sessions, TorrentRouteReason, encryption_policy,
};
use crate::torrents::status::{self, Engine, Held, Observed, Queued};
use crate::torrents::storage::{PieceStorageFactory, Pieces};
use crate::torrents::store::{Kept, Replayed, TorrentStore};
use crate::torrents::tee::{Parts, Plan, Tee, plan_outputs};
use crate::torrents::{
    ContentLayout, FilePriority, Infohash, NamedBy, PeerInfo, TorrentCounters, TorrentFile,
    TorrentInsert, TorrentIntent, TorrentRecord, TorrentSource, TorrentState, TorrentTracker,
    TrackerEntry, TrackerStatus,
};

pub mod fingerprint;
pub mod listing;
use listing::{CategoryPick, FileTotals, Pick, Shown};
mod scripts;
use scripts::Ending;
pub use scripts::ScriptDirs;
#[cfg(test)]
mod tests;
pub mod updates;

/// How often torrents are read and their routes checked.
pub const TICK: Duration = Duration::from_secs(10);

/// The longest a torrent's peers go unwritten while they change. Its ledger
/// is written at every tick its bytes or activity changed, as its
/// checkpoint is.
pub const PEERS_EVERY_MS: i64 = 60_000;

/// The longest a torrent's ledger goes unwritten while only its times run
/// on: an idle seed's durations count up at every tick, and writing them at
/// each would be one write per torrent per tick. A clean stop writes them
/// all; a crash loses at most this much of them (qBittorrent saves resume
/// data every `SaveResumeDataInterval`, 60 minutes, by default).
pub const TIMES_EVERY_MS: i64 = 30 * 60_000;

// How far into its TIMES_EVERY_MS period a torrent's times start. Job ids
// are sequential, so the id is mixed first: torrents added one after
// another land anywhere in the period, not in its first few seconds.
pub(crate) fn times_offset_ms(job_id: JobId) -> i64 {
    let mut z = job_id.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z % TIMES_EVERY_MS as u64) as i64
}

// How many torrents' jobs are restored from their checkpoints at once.
const RESTORES_AT_ONCE: usize = 8;

/// The most pieces a torrent's ledger goes without between ticks: once
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
    pub upload_cap: Option<u64>,
    pub torrent: TorrentConfig,
}

/// Where each level keeps one setting a torrent takes over its levels; a
/// level that does not carry it is `None`, and inherits. This is the one
/// place that says which level may set which setting:
///
/// | setting                  | torrent | rule | category | global          |
/// |--------------------------|---------|------|----------|-----------------|
/// | seeding goals, mode, act | yes     | yes  | yes      | yes             |
/// | upload cap               | no (1)  | yes  | yes      | yes             |
/// | unpack                   | yes     | yes  | yes      | yes             |
/// | DHT, PEX, LSD            | no      | yes  | no       | the session's   |
/// | encryption               | no      | yes  | no       | the session's   |
/// | route scope              | no      | yes  | yes      | the default     |
///
/// (1) A torrent's own upload limit is not a level of the cap: the tighter
/// of the two holds ([`ratelimits_of`]).
///
/// A tracker rule alone names a client, an SSL identity, whether its
/// torrents start forced, and whether the schedule may pause them; those
/// are read from the rule and have no chain.
pub struct Field<T> {
    torrent: Option<fn(&TorrentRecord) -> Leaf<T>>,
    rule: Option<fn(&TrackerRule) -> Leaf<T>>,
    category: Option<fn(&CategoryConfig) -> Leaf<T>>,
    global: fn(&TorrentSettings) -> Leaf<T>,
}

// A seeding leaf, where every level keeps a policy.
macro_rules! seeding_field {
    ($leaf:ident) => {
        Field {
            torrent: Some(|record| record.policy_override.unwrap_or_default().$leaf),
            rule: Some(|rule| rule.seeding.unwrap_or_default().$leaf),
            category: Some(|category| category.seeding.unwrap_or_default().$leaf),
            global: |settings| settings.seeding.unwrap_or_default().at_global().$leaf,
        }
    };
}

pub const RATIO_GOAL: Field<f32> = seeding_field!(ratio_goal);
pub const SEED_TIME_GOAL: Field<u32> = seeding_field!(seed_time_goal_minutes);
pub const INACTIVE_GOAL: Field<u32> = seeding_field!(inactive_goal_minutes);
pub const SEEDING_MODE: Field<SeedingGoalMode> = seeding_field!(mode);
pub const SEEDING_ACTION: Field<SeedingAction> = seeding_field!(action);
pub const SEED_TIME_COUNTING: Field<SeedTimeCounting> = seeding_field!(seed_time_counting);

pub const UPLOAD_CAP: Field<u64> = Field {
    torrent: None,
    rule: Some(|rule| rule.upload_cap),
    category: Some(|category| category.upload_cap),
    global: |settings| Leaf::limit(settings.upload_cap),
};

pub const UNPACK: Field<bool> = Field {
    torrent: Some(|record| Leaf::of(record.unpack)),
    rule: Some(|rule| Leaf::of(rule.unpack)),
    category: Some(|category| Leaf::of(category.unpack)),
    global: |settings| Leaf::Set(settings.torrent.unpack),
};

// The session runs the DHT, local discovery and peer exchange for every
// torrent it is not turned off for, under its own encryption.
pub const DHT: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.dht)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const PEX: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.pex)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const LSD: Field<bool> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.lsd)),
    category: None,
    global: |_| Leaf::Inherit,
};
pub const ENCRYPTION: Field<TorrentEncryption> = Field {
    torrent: None,
    rule: Some(|rule| Leaf::of(rule.encryption)),
    category: None,
    global: |_| Leaf::Inherit,
};

// A rule or category routes its torrents itself once it says so.
pub const SCOPE: Field<TorrentScope> = Field {
    torrent: None,
    rule: Some(|rule| {
        if rule.torrent_route {
            Leaf::Set(TorrentScope::TrackerRule(rule.id))
        } else {
            Leaf::Inherit
        }
    }),
    category: Some(|category| {
        if category.torrent_route {
            Leaf::Set(TorrentScope::Category(category.id))
        } else {
            Leaf::Inherit
        }
    }),
    global: |_| Leaf::Set(TorrentScope::Default),
};

impl TorrentSettings {
    pub fn of(config: &Config) -> Self {
        Self {
            complete_dir: PathBuf::from(config.complete_dir()),
            categories: config.categories.clone(),
            seeding: config.seeding,
            upload_cap: config.upload_cap,
            torrent: config.torrent.clone(),
        }
    }

    fn category(&self, id: Option<u32>) -> Option<&CategoryConfig> {
        id.and_then(|id| self.categories.iter().find(|category| category.id == id))
    }

    /// `field` at each level for a torrent: `torrent`, when there is one
    /// yet, in category `category` under tracker rule `rule`.
    pub fn levels<T>(
        &self,
        field: &Field<T>,
        torrent: Option<&TorrentRecord>,
        category: Option<u32>,
        rule: Option<&TrackerRule>,
    ) -> Levels<T> {
        fn at<L, T>(of: Option<fn(&L) -> Leaf<T>>, level: Option<&L>) -> Leaf<T> {
            of.zip(level).map_or(Leaf::Inherit, |(of, level)| of(level))
        }
        Levels {
            torrent: at(field.torrent, torrent),
            rule: at(field.rule, rule),
            category: at(field.category, self.category(category)),
            global: (field.global)(self),
        }
    }

    /// `field` at each level for `record` under `rule`.
    pub fn of_torrent<T>(
        &self,
        field: &Field<T>,
        record: &TorrentRecord,
        rule: Option<&TrackerRule>,
    ) -> Levels<T> {
        self.levels(field, Some(record), record.category_id, rule)
    }

    // The scope whose route a torrent takes: its tracker rule's when the
    // rule routes its torrents itself, else its category's when that does,
    // else the default.
    fn scope(&self, id: Option<u32>, rule: Option<&TrackerRule>) -> TorrentScope {
        self.levels(&SCOPE, None, id, rule)
            .resolve()
            .unwrap_or(TorrentScope::Default)
    }

    /// The seeding terms `record` is held to under `rule`, each resolved
    /// over its levels, and the level each comes from.
    pub fn seeding(
        &self,
        record: &TorrentRecord,
        rule: Option<&TrackerRule>,
    ) -> (Seeding, SeedingLevels) {
        let ratio = self.of_torrent(&RATIO_GOAL, record, rule);
        let seed_time = self.of_torrent(&SEED_TIME_GOAL, record, rule);
        let inactive = self.of_torrent(&INACTIVE_GOAL, record, rule);
        let mode = self.of_torrent(&SEEDING_MODE, record, rule);
        let action = self.of_torrent(&SEEDING_ACTION, record, rule);
        let counting = self.of_torrent(&SEED_TIME_COUNTING, record, rule);
        let cap = self.of_torrent(&UPLOAD_CAP, record, rule);
        (
            Seeding {
                ratio_goal: ratio.resolve(),
                seed_time_goal_minutes: seed_time.resolve(),
                inactive_goal_minutes: inactive.resolve(),
                mode: mode.resolve().unwrap_or_default(),
                action: action.resolve().unwrap_or_default(),
                seed_time_counting: counting.resolve().unwrap_or_default(),
            },
            SeedingLevels {
                ratio_goal: ratio.level(),
                seed_time_goal_minutes: seed_time.level(),
                inactive_goal_minutes: inactive.level(),
                mode: mode.level(),
                action: action.level(),
                seed_time_counting: counting.level(),
                upload_cap: cap.level(),
            },
        )
    }

    /// The seeding terms `record` is held to under `rule`.
    pub fn policy(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> Seeding {
        self.seeding(record, rule).0
    }

    /// The upload cap `record` takes from its levels under `rule`.
    pub fn upload_cap(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> Option<u64> {
        self.of_torrent(&UPLOAD_CAP, record, rule).resolve()
    }

    // Whether the torrent's archives would be extracted were its job built
    // now: its own value, else its tracker rule's, else its category's,
    // else the global one.
    fn unpack(&self, record: &TorrentRecord, rule: Option<&TrackerRule>) -> bool {
        self.of_torrent(&UNPACK, record, rule)
            .resolve()
            .unwrap_or(false)
    }
}

/// The level each of a torrent's seeding terms comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SeedingLevels {
    pub ratio_goal: Level,
    pub seed_time_goal_minutes: Level,
    pub inactive_goal_minutes: Level,
    pub mode: Level,
    pub action: Level,
    pub seed_time_counting: Level,
    pub upload_cap: Level,
}

impl Default for SeedingLevels {
    fn default() -> Self {
        Self {
            ratio_goal: Level::Global,
            seed_time_goal_minutes: Level::Global,
            inactive_goal_minutes: Level::Global,
            mode: Level::Global,
            action: Level::Global,
            seed_time_counting: Level::Global,
            upload_cap: Level::Global,
        }
    }
}

// What a torrent's session holds its peers to under its tracker rule:
// whether it takes them from its trackers alone, whether it looks for them
// on the DHT, on the local network and by peer exchange, and the encryption
// its connections are made under over the session's. A change of any takes
// the torrent through its session again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerTerms {
    trackers_only: bool,
    disable_dht: bool,
    disable_pex: bool,
    disable_lsd: bool,
    encryption: Option<TorrentEncryption>,
}

impl PeerTerms {
    // A switch no level sets is the session's, which runs each for every
    // torrent it is not turned off for.
    fn of(settings: &TorrentSettings, rule: Option<&TrackerRule>) -> Self {
        let off =
            |field: &Field<bool>| settings.levels(field, None, None, rule).resolve() == Some(false);
        Self {
            trackers_only: rule.is_some_and(TrackerRule::trackers_only),
            disable_dht: off(&DHT),
            disable_pex: off(&PEX),
            disable_lsd: off(&LSD),
            encryption: settings.levels(&ENCRYPTION, None, None, rule).resolve(),
        }
    }

    // `options` under these terms.
    fn apply(self, options: &mut AddTorrentOptions) {
        options.trackers_only = self.trackers_only;
        options.disable_dht = self.disable_dht;
        options.disable_pex = self.disable_pex;
        options.disable_lsd = self.disable_lsd;
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

// The one place a torrent's archive passwords are chosen: the passwords of
// the rule it is under, then the global list, a value already in the list
// skipped so its first copy keeps its place. Only this reads a rule's
// passwords, and it hands them to the graph of a torrent under that rule
// alone: an NZB job's harvest reads no rule, a torrent under another rule or
// none gets only the global list, and nothing writes a rule's passwords to
// the global list.
fn torrent_archive_passwords(
    rule: Option<&TrackerRule>,
    global: &[ArchivePasswordCandidate],
) -> Vec<ArchivePasswordCandidate> {
    let tracker = rule
        .into_iter()
        .flat_map(|rule| rule.archive_passwords.as_slice())
        .map(|value| ArchivePasswordCandidate::new(ArchivePasswordSource::Tracker, value.clone()));
    let mut candidates: Vec<ArchivePasswordCandidate> = Vec::new();
    for candidate in tracker.chain(global.iter().cloned()) {
        if candidates
            .iter()
            .all(|kept| kept.value() != candidate.value())
        {
            candidates.push(candidate);
        }
    }
    candidates
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

// The limits `record`'s session holds it to: its own, and on upload the cap
// its levels resolve to where that is tighter. Each is a channel the
// torrent's peers draw from beside the session's, as each peer class over a
// libtorrent peer is (`bandwidth_manager::request_bandwidth`).
fn ratelimits_of(record: &TorrentRecord, cap: Option<u64>) -> LimitsConfig {
    LimitsConfig {
        upload_bps: limit_kind(record.upload_limit).tighter(limit_kind(cap)),
        download_bps: limit_kind(record.download_limit),
    }
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
    // The decode pool the pipeline's jobs share, and where a decode window
    // goes: under `work`, else in weaver's data folder.
    pub(crate) decode: Arc<DecodePool>,
    pub(crate) window_ledger: ScratchLedger,
    pub(crate) data_dir: Option<(PathBuf, CapacityReader, ScratchLedger)>,
    // Where the jobs' runs speak, where the database's word on their
    // checkpoints comes, and where a job joins to hear both.
    pub(crate) mail: mpsc::Sender<Said>,
    pub(crate) written: mpsc::UnboundedSender<Said>,
    pub(crate) outside: Arc<Outside>,
    // Where a torrent's tee unpacks, each in a directory of its own.
    pub(crate) work: PathBuf,
    // The repair slots every torrent job shares, as the pipeline's jobs
    // share theirs.
    pub(crate) repairs: Arc<Mutex<RepairSlots>>,
    // With no pipeline to hear the mail, the mail itself, until the service
    // hears it in the pipeline's stead.
    alone: Arc<Mutex<Option<RunMail>>>,
    // Where the scripts a torrent's job runs are told weaver keeps things,
    // as the pipeline tells its jobs' scripts.
    pub(crate) script_dirs: ScriptDirs,
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
            decode: budget.decode,
            window_ledger: budget.window_ledger,
            data_dir: Some(budget.data_dir),
            mail: budget.mail,
            written: budget.written,
            outside: budget.outside,
            work: budget.work,
            repairs: Arc::default(),
            alone: Arc::default(),
            script_dirs: ScriptDirs {
                data: Some(pipeline.script_data_dir.clone()),
                intermediate: Some(pipeline.intermediate_dir.clone()),
                complete: Some(pipeline.complete_dir.clone()),
            },
        }
    }

    /// No budget at all: one slot, no limits, free space unknown, and
    /// unpacking under a directory of its own in the system's temporary one.
    pub fn unbounded() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let mail = RunMail::default();
        let work = std::env::temp_dir().join(format!(
            "weaver-torrent-work-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
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
            decode: Arc::new(DecodePool::new(u64::MAX)),
            window_ledger: ScratchLedger::default(),
            data_dir: None,
            mail: mail.sender(),
            written: mail.written(),
            outside: Arc::default(),
            work,
            repairs: Arc::default(),
            alone: Arc::new(Mutex::new(Some(mail))),
            script_dirs: ScriptDirs::default(),
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
        crashed: crate::runtime::panic_trap::CrashMarks,
    ) -> Result<Self, String> {
        let routes = NetworkRoutes::new(Arc::clone(proxies), ROUTE_TIMEOUT);
        let sessions = Sessions::new(Arc::new(routes), SessionEnv::of(config), db.clone());
        let service = Self::new(
            db,
            sessions,
            TorrentSettings::of(config),
            JobEnv::of_pipeline(pipeline),
            Arc::new(SystemClock),
        );
        service.publish_to(queue);
        service.park_crashed(crashed);
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
    // Where its files go.
    pub placement: Placement,
    // The tags its job is added with.
    pub tags: Vec<String>,
}

/// Where a torrent's files go, as a client adding it asks (qBittorrent
/// `AddTorrentParams`). The default is weaver's own: a folder of the
/// torrent's under its category's directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Placement {
    /// The directory its files go under; None for its category's.
    pub save_path: Option<String>,
    /// How they are laid out there; None for a folder of its own, or with
    /// a save path, as the metainfo lists them.
    pub layout: Option<ContentLayout>,
    /// The directory it downloads under, moving under the save path once
    /// complete; None to download where it stays.
    pub download_path: Option<String>,
    /// Its files are complete where they go already: it seeds them at once
    /// unchecked, and checks each piece as a peer first asks for it
    /// (libtorrent `torrent_flags::seed_mode`).
    pub seed_mode: bool,
}

// `placement` with its paths absolute, as qBittorrent
// `SessionImpl::initLoadTorrentParams` makes them as a torrent is added: a
// relative save path is under the default save path, here `default_save`,
// the torrent's category directory, and a download path with no save path
// moves to the default save path once complete. A relative download path is
// under the default download path, which weaver has none of, so under the
// save path.
fn absolute_placement(placement: &Placement, default_save: &Path) -> Placement {
    let under = |base: &Path, path: &str| {
        if Path::new(path).is_absolute() {
            path.to_owned()
        } else {
            base.join(path).to_string_lossy().into_owned()
        }
    };
    let save_path = match (&placement.save_path, &placement.download_path) {
        (Some(path), _) => Some(under(default_save, path)),
        (None, Some(_)) => Some(default_save.to_string_lossy().into_owned()),
        (None, None) => None,
    };
    let download_base = save_path.as_deref().map_or(default_save, Path::new);
    let download_path = placement
        .download_path
        .as_deref()
        .map(|path| under(download_base, path));
    Placement {
        save_path,
        download_path,
        ..placement.clone()
    }
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
    /// The wanted bytes in hand, over the wanted bytes: the pieces it
    /// counts, a check under way included.
    pub progress: f64,
    /// While its files are checked, the share of them the check has read.
    pub check_progress: Option<f64>,
    pub total_bytes: u64,
    pub have_bytes: u64,
    /// The bytes of the files it wants, and how many of them are on disk;
    /// a file turned off counts in neither.
    pub wanted_bytes: u64,
    pub wanted_have_bytes: u64,
    /// Payload sent and received over every run, kept through pauses and
    /// restarts (libtorrent `torrent_status::all_time_upload` and
    /// `all_time_download`).
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    /// Payload sent and received in the current run alone, from nothing
    /// at each start (libtorrent `torrent_status::total_payload_upload` and
    /// `total_payload_download`); zero while it does not run.
    pub run_uploaded_bytes: u64,
    pub run_downloaded_bytes: u64,
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
    /// Seconds spent complete and seeding, in all.
    pub seeding_seconds: u64,
    /// Seconds of the current unbroken seeding run.
    pub consecutive_seeding_seconds: u64,
    /// The bytes moved over the time active.
    pub download_avg_bps: u64,
    pub upload_avg_bps: u64,
    /// Bytes received and thrown away this run: pieces that failed their
    /// check, and blocks that arrived when already in hand. Counts from zero
    /// each time the torrent starts.
    pub wasted_bytes: u64,
    /// The last time a complete copy was in reach.
    pub last_seen_complete_ms: Option<i64>,
    /// The last time it sent or received payload, in milliseconds since the
    /// epoch; None while it never has.
    pub last_activity_ms: Option<i64>,
    /// The torrent's pieces, once its metainfo is in hand.
    pub pieces: Option<TorrentPieces>,
    pub created_by: Option<String>,
    pub created_at_ms: Option<i64>,
    pub comment: Option<String>,
    pub added_at_ms: i64,
    pub completed_at_ms: Option<i64>,
    pub storage_dir: Option<String>,
    /// Where its files sit when they were placed outside its storage
    /// directory, with no folder of their own.
    pub files_dir: Option<String>,
    /// Its place among the torrents still downloading, from 1; None while
    /// it seeds.
    pub queue_position: Option<u32>,
    pub seeding_left: Option<String>,
    /// The torrent's own seeding policy, when it sets a leaf.
    pub seeding_override: Option<SeedingPolicy>,
    /// The seeding terms in force, and the level each comes from.
    pub seeding_policy: Seeding,
    pub seeding_levels: SeedingLevels,
    /// The host pattern of the tracker rule its trackers match.
    pub tracker_rule: Option<String>,
    /// The upload cap its levels resolve to, held beside its own limit.
    pub upload_cap: Option<u64>,
    /// Whether its archives are extracted: as its job was built, or as the
    /// levels say until then.
    pub unpack: bool,
    /// Whether it was asked to extract its archives; `None` inherits.
    pub unpack_override: Option<bool>,
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
    /// Its job's tags, in tag order.
    pub tags: Vec<String>,
}

/// What a support report reads of a torrent beyond its view.
#[derive(Debug, Clone)]
pub struct TorrentSupport {
    pub view: TorrentView,
    /// The one copy of its metainfo, once it is in hand.
    pub metainfo: Option<Bytes>,
    /// The ledger as of this reading, with the bytes it wasted.
    pub counters: TorrentCounters,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
        self.paths_as(self.is_one_file())
    }

    /// Whether it is one file named as the torrent is.
    pub fn is_one_file(&self) -> bool {
        matches!(self.files.as_slice(), [file] if file.path == self.name)
    }

    /// Its `paths`, given whether it is one file named as the torrent is:
    /// for a view read without its files.
    pub fn paths_as(&self, one_file: bool) -> (String, String) {
        let Some(dir) = self.files_dir.as_ref().or(self.storage_dir.as_ref()) else {
            return (String::new(), String::new());
        };
        let dir = PathBuf::from(dir);
        let single = if one_file {
            Path::new(&self.name).file_name()
        } else {
            None
        };
        let content = match single {
            Some(name) => dir.join(name),
            None => dir.clone(),
        };
        // Files with no folder of their own sit in the directory they went
        // to, which is their save path (qBittorrent `TorrentImpl::contentPath`
        // is the save path for files with no root folder).
        let save = match &self.files_dir {
            Some(_) => dir,
            None => content
                .parent()
                .map_or_else(|| content.clone(), Path::to_path_buf),
        };
        (
            save.to_string_lossy().into_owned(),
            content.to_string_lossy().into_owned(),
        )
    }

    /// The tracker it goes by (qBittorrent `TorrentImpl::currentTracker`:
    /// libtorrent's `current_tracker`, the tracker that last answered, else
    /// the first one listed). Weaver keeps each tracker's latest outcome
    /// only, so the one that last answered is the working one announced to
    /// most recently.
    pub fn current_tracker(&self) -> Option<&TorrentTracker> {
        self.trackers
            .iter()
            .filter(|tracker| tracker.status == TrackerStatus::Working)
            .max_by_key(|tracker| tracker.last_announce_ms)
            .or_else(|| self.trackers.first())
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

// `trackers` in the order a torrent keeps them: by tier, then by the URL's
// bytes, whatever order a database read gave them in.
fn in_table_order(mut trackers: Vec<TrackerEntry>) -> Arc<[TrackerEntry]> {
    trackers.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    trackers.into()
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
            ..SessionTraffic::default()
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
    // The trackers it lists, as its rows in the database hold them, in
    // `in_table_order`: each change is written to the table first, then
    // here as a new list, under the lock that holds this torrent, so a
    // reader takes the list without copying it.
    trackers: Arc<[TrackerEntry]>,
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
    // What it has in its files, once it has metainfo.
    store: Option<Arc<TorrentStore>>,
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
    // The payload its ledger held as downloaded when this run of weaver took
    // it: more since means it downloaded in this run.
    downloaded_at_start: u64,
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
    // Why its restore is held, while it is: a checkpoint the database did
    // not give, tried again at each restore until it does.
    restore_held: Option<String>,
    // Not one of the files its pieces are in was there when its store was
    // made: it is in no session, its pieces kept, until a start, a recheck
    // or a move looks again. Found again at each load, never kept.
    files_missing: bool,
    queued: bool,
    // Its place among the torrents still downloading, from 1, as the queue
    // was last applied; None while it seeds (qBittorrent
    // `TorrentImpl::queuePosition`).
    queue_rank: Option<u32>,
    // Who holds it stopped, while a hold does.
    paused_by: Option<HoldOrigin>,
    // The operator's level for each file, as stored.
    levels: Vec<FilePriority>,
    speeds: (u64, u64),
    // Its established peers, the seeders among them, and the peers still
    // in their handshake.
    peers: (u32, u32, u32),
    // Where a move goes, while it is under way.
    moving: Option<Moving>,
    // How many pieces had passed their check in seed mode when they were
    // last kept.
    seed_verified: usize,
    // Each file's bytes in hand: the checkpoint's until the session has
    // checked the files, then the session's.
    file_have: Vec<u64>,
    // The pieces it kept, until its store is back.
    kept_pieces: u32,
    // Why unpacking refused it, as its history kept it when it was
    // restored: a restored job does not say.
    failed: Option<String>,
    // What the boot read of its rows for its first store and its first
    // attach, each taken by the one it is for; a later one reads its row.
    kept_at_boot: Option<Option<Kept>>,
    verified_at_boot: Option<Option<Vec<u8>>>,
    // Held while a session takes it and while it is removed, so the two
    // never cross: a torrent removed is never put in a session after, and
    // one put in a session is removed from it.
    gate: Arc<tokio::sync::Mutex<()>>,
}

// The torrents on each route's session, attached or held there: how loaded
// each pool member is when a torrent is placed.
fn load_of<'a>(torrents: impl Iterator<Item = &'a Live>) -> HashMap<RouteKey, usize> {
    let mut load = HashMap::new();
    for key in torrents.filter_map(|torrent| torrent.route.as_ref()) {
        *load.entry(key.clone()).or_default() += 1;
    }
    load
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
                &settings.policy(&torrent.record, rule),
                &torrent.counters,
                torrent.total,
                now,
            )
            .is_some());
    Queued {
        job_id: torrent.record.job_id,
        seeding,
        forced: torrent.record.forced,
        // One whose files are missing runs nowhere, so takes no slot.
        paused: !torrent.resumed() || torrent.files_missing,
        runs_through_schedule,
    }
}

// `torrent`'s turn in the queue as a pass gives it: its rank, the hold
// that pauses it, and whether it waits for a slot.
fn take_turn(torrent: &mut Live, queued: &Queued, turn: status::Turn, holds: &HoldSet, now: i64) {
    torrent.queue_rank = turn.rank;
    torrent.settle(now);
    torrent.paused_by = torrent
        .resumed()
        .then(|| holds.reason_for(queued.target()))
        .flatten();
    torrent.queued = !turn.active && torrent.resumed() && torrent.paused_by.is_none();
}

// A torrent's files under `layout`: the metainfo's names and lengths, at
// `levels`.
fn files_of(layout: Option<&PieceLayout>, levels: &[FilePriority]) -> Vec<TorrentFile> {
    layout
        .iter()
        .flat_map(|layout| layout.files())
        .enumerate()
        .map(|(index, file)| TorrentFile {
            index: index as u32,
            path: file.name.clone(),
            length: file.length,
            priority: levels.get(index).copied().unwrap_or_default(),
        })
        .collect()
}

impl Live {
    // Its files: the metainfo's names and lengths, at the levels it holds.
    fn files(&self) -> Vec<TorrentFile> {
        files_of(self.layout.as_deref(), &self.levels)
    }

    // The tracker rule it is under, while that rule stands.
    fn rule<'r>(&self, rules: &'r [TrackerRule]) -> Option<&'r TrackerRule> {
        rule_of(self.rule_id, rules)
    }

    // A torrent whose ledger and peers the database holds as given, as of
    // `now`. Its seeding run starts again from zero: the time Weaver was
    // down is no seeding.
    fn new(
        record: TorrentRecord,
        mut counters: TorrentCounters,
        resume: ResumePeers,
        now: i64,
    ) -> Self {
        let written = counters;
        counters.consecutive_seeding_s = 0;
        // Spread by job, so torrents restored together do not all write
        // their times at one tick.
        let times_at_ms = now - times_offset_ms(record.job_id);
        Self {
            record,
            identity: TorrentIdentity::fresh(),
            rule_id: None,
            trackers: Arc::from(Vec::new()),
            private: false,
            ledger: Ledger {
                written: Some((written, resume.clone())),
                peers_at_ms: now,
                times_at_ms,
                lost: Arc::default(),
            },
            resume,
            names: Vec::new(),
            total: 0,
            layout: None,
            store: None,
            intake: None,
            attached: None,
            fetching: None,
            resolving: None,
            route: None,
            held: None,
            about: About::default(),
            downloaded_at_start: counters.downloaded_bytes,
            counters,
            seen: TorrentTotals::default(),
            timed: Timed::at(now),
            engine: Engine::Absent,
            run_error: None,
            restore_held: None,
            files_missing: false,
            queued: false,
            queue_rank: None,
            paused_by: None,
            levels: Vec::new(),
            speeds: (0, 0),
            peers: (0, 0, 0),
            moving: None,
            seed_verified: 0,
            file_have: Vec::new(),
            kept_pieces: 0,
            failed: None,
            kept_at_boot: None,
            verified_at_boot: None,
            gate: Arc::default(),
        }
    }

    fn complete(&self) -> bool {
        self.record.completed_at_ms.is_some()
    }

    // Complete, or with every piece it wants had as the session last said.
    // A torrent in seed mode has every piece as added and seeds from the
    // start (libtorrent `torrent_flags::seed_mode`: the picker is created
    // having all; qBittorrent queues it as a seed), until its session says
    // a piece failed and it left seed mode.
    fn seeding(&self) -> bool {
        status::seeding(self.complete(), &self.engine) || self.record.seed_mode
    }

    // What it shows until its store is back, from what was stored: the
    // pieces it kept, as the store restored from them will have, and the
    // failure its history keeps.
    fn kept(&mut self, failed: Option<String>, kept: Option<&Kept>) {
        if let (Some(kept), Some(layout)) = (kept, &self.layout) {
            let (count, file_have) = kept.had(layout);
            self.file_have = file_have;
            self.kept_pieces = count;
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
        let times_due = at_once || now - self.ledger.times_at_ms >= TIMES_EVERY_MS;
        let (counters, peers) = match &self.ledger.written {
            Some((counters, peers)) => (
                (if times_due {
                    *counters != self.counters
                } else {
                    !same_but_times(counters, &self.counters)
                })
                .then_some(self.counters),
                (!same_peers(peers, &self.resume)).then(|| self.resume.clone()),
            ),
            None => (Some(self.counters), Some(self.resume.clone())),
        };
        if counters.is_some() {
            self.ledger.times_at_ms = now;
        }
        let kept = match (&counters, &self.ledger.written) {
            (None, Some((written, _))) => *written,
            _ => self.counters,
        };
        self.ledger.written = Some((kept, self.resume.clone()));
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
        self.store
            .as_ref()
            .and_then(|store| store.tee())
            .and_then(|tee| tee.refusal())
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

    // The pieces, and how many of them it has verified; until its store
    // is back, how many it kept.
    fn pieces(&self) -> Option<TorrentPieces> {
        let layout = self.layout.as_ref()?;
        let have = match &self.store {
            Some(store) => store.count(),
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
                checked: None,
                counters: self.counters,
                seen: self.seen,
                timed: self.timed,
                moved: (0, 0),
                run: (0, 0),
                speeds: (0, 0),
                peers: (0, 0, 0),
            };
        };
        let stats = attached.handle.stats();
        let engine = Engine::of(&stats);
        let mut file_have = self.file_have.clone();
        take_file_have(&mut file_have, &stats.file_progress, self.store.as_deref());
        // How far a check under way has read (libtorrent
        // `torrent_status::progress` while `checking_files`, torrent.cpp:2625
        // and :12278-12292, v2.0.15).
        let checked = matches!(engine, Engine::Starting | Engine::Checking).then(|| {
            if stats.total_bytes == 0 {
                0.0
            } else {
                (stats.progress_bytes as f64 / stats.total_bytes as f64).min(1.0)
            }
        });
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
        count_waste(&mut counters, &self.seen, &seen);
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
            checked,
            counters,
            seen,
            timed: self.timed,
            moved,
            run: live.map_or((0, 0), |live| {
                (live.snapshot.fetched_bytes, live.snapshot.uploaded_bytes)
            }),
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
            engine: self
                .run_error
                .clone()
                .or_else(|| self.restore_held.clone())
                .map_or(engine, Engine::Error),
            has_metadata: self.layout.is_some(),
            fetch: self.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            complete: self.complete(),
            intent: self.record.intent,
            queued: self.queued,
            paused_by: self.paused_by,
            failed: self.refusal(),
            held: self.held.clone(),
            moving: self.moving.is_some(),
            restore_held: self.restore_held.clone(),
            files_missing: self.files_missing,
        }
    }
}

// A torrent as the session reports it now.
struct Reading {
    engine: Engine,
    // Each file's bytes in hand.
    file_have: Vec<u64>,
    // The share of the torrent a check under way has read, while one is.
    checked: Option<f64>,
    // The ledger with what the session moved since it was last kept.
    counters: TorrentCounters,
    // The session's totals, which the ledger's deltas are taken from.
    seen: TorrentTotals,
    // The instant the counters' durations run to.
    timed: Timed,
    // The bytes received and sent on the wire since the last kept reading.
    moved: (u64, u64),
    // The payload received and sent in this run of the session, from
    // nothing at its start (libtorrent `torrent_status::total_payload_download`
    // and `total_payload_upload`, which a pause resets).
    run: (u64, u64),
    speeds: (u64, u64),
    peers: (u32, u32, u32),
}

// What the exporter reads of a torrent beside its view.
struct Unviewed {
    wanted_bytes: u64,
    hash_failed_bytes: u64,
    redundant_bytes: u64,
    seeding_seconds: u64,
    consecutive_seeding_seconds: u64,
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
            consecutive_seeding_seconds: self.consecutive_seeding_seconds,
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

// Fold into the ledger the bytes the session threw away between its totals
// `then` and `now`, so its wasted bytes are the torrent's across restarts.
fn count_waste(counters: &mut TorrentCounters, then: &TorrentTotals, now: &TorrentTotals) {
    counters.failed_bytes = counters
        .failed_bytes
        .saturating_add(now.failed_bytes.saturating_sub(then.failed_bytes));
    counters.redundant_bytes = counters
        .redundant_bytes
        .saturating_add(now.redundant_bytes.saturating_sub(then.redundant_bytes));
}

// The instant a torrent's durations run to, and the milliseconds each ran
// past the whole seconds its counter holds: a reading carries them over,
// so no part of a second is lost between readings.
#[derive(Debug, Clone, Copy)]
struct Timed {
    at_ms: i64,
    active_ms: u64,
    finished_ms: u64,
    consecutive_ms: u64,
}

impl Timed {
    fn at(now: i64) -> Self {
        Self {
            at_ms: now,
            active_ms: 0,
            finished_ms: 0,
            consecutive_ms: 0,
        }
    }

    // Run on to `now`: the time since runs into the active duration while
    // `active`, and into the finished one and the current seeding run while
    // `seeding`; a reading that is not `seeding` ends the run, and it starts
    // again from zero. A clock stepped back credits nothing for that
    // reading, and the next runs from `now`.
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
            consecutive_ms: if seeding {
                run(
                    self.consecutive_ms,
                    true,
                    &mut counters.consecutive_seeding_s,
                )
            } else {
                counters.consecutive_seeding_s = 0;
                0
            },
        }
    }
}

// Whether two ledgers differ at most in the times that run on by themselves
// while a torrent sits: its durations and when a complete copy was last
// seen. Bytes moved, bytes wasted and activity are written at the tick they
// change.
fn same_but_times(a: &TorrentCounters, b: &TorrentCounters) -> bool {
    a.uploaded_bytes == b.uploaded_bytes
        && a.downloaded_bytes == b.downloaded_bytes
        && a.failed_bytes == b.failed_bytes
        && a.redundant_bytes == b.redundant_bytes
        && a.last_activity_ms == b.last_activity_ms
}

// What the database holds of a torrent's ledger and peers, and when its
// peers were last taken.
struct Ledger {
    // `None` while what it holds is not known: the next write is all of it.
    written: Option<(TorrentCounters, ResumePeers)>,
    // When the peers were last taken from the session to be written.
    peers_at_ms: i64,
    // When the ledger was last written: its times alone go unwritten until
    // TIMES_EVERY_MS has passed since.
    times_at_ms: i64,
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
    lost: Option<Arc<AtomicBool>>,
    // The store whose pieces went with it, told when the write failed.
    pieces: Option<Arc<TorrentStore>>,
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
    // How many files pieces made whole since the last settle.
    whole: Mutex<BTreeMap<JobId, usize>>,
    // A slot or a rank moved where no pass runs: the next settle runs one.
    pass_wanted: AtomicBool,
    wake: Notify,
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Signal {
    pub(super) fn mark(&self, job_id: JobId) {
        self.marked(job_id, 0);
    }

    // Marked, with its checkpoint due at the settle: what changed is the
    // whole ledger, not a piece.
    fn due(&self, job_id: JobId) {
        self.marked(job_id, CHECKPOINT_PIECES);
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
            IntakeHeard::FileWhole => {
                *locked(&self.whole).entry(job_id).or_default() += 1;
                self.mark(job_id);
            }
        }
    }

    fn take(&self) -> BTreeSet<JobId> {
        std::mem::take(&mut *locked(&self.dirty))
    }

    // Ask for one pass over the queue at the next settle. Any number of
    // asks before it runs are one pass (libtorrent `trigger_auto_manage`,
    // posted once however many ask).
    fn want_pass(&self) {
        if !self.pass_wanted.swap(true, Ordering::AcqRel) {
            self.wake.notify_one();
        }
    }

    fn take_pass(&self) -> bool {
        self.pass_wanted.swap(false, Ordering::AcqRel)
    }

    fn take_refused(&self, job_id: JobId) -> Option<String> {
        locked(&self.refused).remove(&job_id)
    }

    fn take_whole(&self, job_id: JobId) -> usize {
        locked(&self.whole).remove(&job_id).unwrap_or_default()
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
    // for one to the job given: one restore of a torrent at a time, and at
    // most RESTORES_AT_ONCE torrents restoring. The blocking restore owns
    // both, so a restore whose awaiting task ends still holds them.
    restoring: std::sync::Mutex<HashMap<JobId, Arc<tokio::sync::Mutex<()>>>>,
    restore_permits: Arc<tokio::sync::Semaphore>,
    // The torrents whose work crashed the daemon on its last run: each is
    // kept paused, its job not restored, until a resume takes its marker.
    crash_marks: Mutex<crate::runtime::panic_trap::CrashMarks>,
    signal: Arc<Signal>,
    ticks: watch::Sender<u64>,
    retired: watch::Sender<u64>,
    written: watch::Sender<Written>,
    queue: Mutex<Option<SchedulerHandle>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    tick: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // The service hearing the mail no pipeline hears, which a stop ends
    // last: an object out for its work comes back through it.
    hearing: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Rung by a settings change: the tick runs then, not at its next turn.
    settings_changed: Notify,
    // The torrents and their sessions as last published, for the exporter.
    published: watch::Sender<Arc<Published>>,
    // Each route's session as last read. The tick reads them, and turning
    // torrents on or off, under the sessions' lock each takes anyway: a
    // publish from an action never waits on a session that is opening.
    sessions_read: Mutex<Vec<SessionSeries>>,
    counters: Counters,
    // The place in the download queue the next torrent added takes: past
    // every one kept (libtorrent puts a new torrent last in the queue).
    next_queue_position: AtomicI64,
    // What the last pass over the queue left at its end, written under
    // `live`'s lock: a torrent added goes there.
    queue_end: Mutex<QueueEnd>,
    // The readings of every torrent's views their followers share.
    readings: updates::Readings,
}

// A snapshot of the download queue's end from the last pass, extended by
// each torrent added since: the slots still free, the ranks given, and the
// place of the last torrent in it. A torrent added after them changes no
// earlier torrent's turn or rank, so it takes its own from here. What moved
// since the pass (a torrent gone or finished, one placed before the end) is
// corrected by the next, which those ask for.
#[derive(Debug, Clone, Copy)]
struct QueueEnd {
    slots: status::Slots,
    last: Option<(i64, JobId)>,
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
        let max_active = settings.torrent.max_active_downloads;
        Self {
            inner: Arc::new(Inner {
                db,
                sessions,
                settings: RwLock::new(settings),
                rules: RwLock::new(Arc::from([])),
                env,
                clock,
                live: tokio::sync::Mutex::new(BTreeMap::new()),
                restoring: std::sync::Mutex::new(HashMap::new()),
                restore_permits: Arc::new(tokio::sync::Semaphore::new(RESTORES_AT_ONCE)),
                crash_marks: Mutex::default(),
                signal: Arc::new(Signal::default()),
                ticks: watch::channel(0).0,
                retired: watch::channel(0).0,
                written: watch::channel(Written::default()).0,
                queue: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
                tick: Mutex::new(None),
                hearing: Mutex::new(None),
                settings_changed: Notify::new(),
                published: watch::channel(Arc::default()).0,
                sessions_read: Mutex::default(),
                counters: Counters::default(),
                next_queue_position: AtomicI64::new(0),
                // An empty queue: every slot free.
                queue_end: Mutex::new(QueueEnd {
                    slots: status::Slots::new(max_active),
                    last: None,
                }),
                readings: updates::Readings::default(),
            }),
        }
    }

    // The torrents that crashed the daemon on its last run, read at start
    // before any torrent's job is restored.
    pub(crate) fn park_crashed(&self, marks: crate::runtime::panic_trap::CrashMarks) {
        *self
            .inner
            .crash_marks
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = marks;
    }

    // Why the torrent is kept paused, while its work crashed the daemon.
    fn crashed(&self, job_id: JobId) -> Option<String> {
        self.inner
            .crash_marks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(job_id)
            .map(|mark| mark.reason())
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
        let rules = self.rules();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                let job_id = torrent.record.job_id;
                torrent.rule_id = self.rule_id(torrent.trackers.iter());
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
        let peer_limit = |torrent: &TorrentConfig| {
            crate::torrents::sessions::session_limits(torrent).connections_limit
        };
        let peers = peer_limit(&settings.torrent);
        self.inner.sessions.set_config(settings.torrent.clone());
        let was = std::mem::replace(
            &mut *self
                .inner
                .settings
                .write()
                .unwrap_or_else(PoisonError::into_inner),
            settings,
        )
        .torrent;
        if on != was.enabled {
            self.turn(on).await;
        }
        // Each torrent leg says how far its tunnel holds TCP peers below the
        // peer limit, so a new limit reads the routes again.
        if peers != peer_limit(&was)
            && let Some(proxies) = self.queue().and_then(|queue| queue.proxy_runtime())
            && let Err(error) = proxies.reload().await
        {
            tracing::warn!(error, "the routes did not take the new torrent peer limit");
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
        let mut starts = queue.state().subscribe_starts();
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
        // A start of every torrent looks again at those held for their
        // files, as a start of one does. Starts asked for while a look runs
        // are one more look, after it.
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            while starts.changed().await.is_ok() {
                service.look_again_held().await;
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
        let listed = self.listed(None, false, now).await;
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
        // Every tee's checkpoint goes at once, whether or not it was due,
        // once every object of its job out for its work is back.
        let tees: Vec<(JobId, Arc<Tee>)> = {
            let live = self.inner.live.lock().await;
            live.iter()
                .filter_map(|(&job_id, torrent)| {
                    Some((job_id, Arc::clone(torrent.store.as_ref()?.tee()?)))
                })
                .collect()
        };
        for (job_id, tee) in tees {
            self.checkpoint_tee(job_id, tee, true).await;
        }
        if let Some(hearing) = locked(&self.inner.hearing).take() {
            hearing.abort();
        }
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
            let mut reloads = service.inner.sessions.routes().reloads();
            loop {
                tokio::select! {
                    _ = interval.tick() => {}
                    () = service.woken(&mut reloads) => {}
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

    // Resolves when the tick runs before its interval: the settings changed,
    // a session's leg was revoked, so it opens again at once, or the routes
    // took a saved configuration, so a route held down or failing opens
    // again on it at once.
    pub(crate) async fn woken(&self, reloads: &mut Option<watch::Receiver<u64>>) {
        tokio::select! {
            () = self.inner.settings_changed.notified() => {}
            () = self.inner.sessions.revoked() => {}
            () = reloaded(reloads) => {}
        }
    }

    // Everything but the tick.
    pub(crate) async fn open(&self) -> Result<(), String> {
        if let Some(task) = self.inner.env.hear_alone() {
            *locked(&self.inner.hearing) = Some(task);
        }
        self.spawn_settle();
        self.spawn_follow_trackers();
        self.spawn_follow_egress_limits();
        self.spawn_follow_interfaces();
        self.restore().await
    }

    // A session's local service discovery joins its groups on the
    // interfaces up as it opens; each time those change, it joins again on
    // the ones up then.
    fn spawn_follow_interfaces(&self) {
        let Some(changes) = self.inner.sessions.routes().interface_changes() else {
            return;
        };
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            service.inner.sessions.follow_interfaces(changes).await;
        }));
    }

    // Each egress's caps and quota state reach the open sessions as they
    // change: a quota crossing its soft or hard cap, a reset, a new period
    // or a saved egress. A session is never closed for it; its uploads
    // trickle or move nothing, and it keeps announcing.
    fn spawn_follow_egress_limits(&self) {
        let Some(mut changes) = self.inner.sessions.routes().egress_limit_changes() else {
            return;
        };
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            loop {
                service.inner.sessions.apply_egress_limits();
                if !changes.changed().await {
                    break;
                }
            }
        }));
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

    // What the torrents that spoke did: a write the store could not make
    // fails its torrent, a refusal found while unpacking stops it, and one
    // with every file it wants whole, unpacked when it has a tee, completes.
    // A tee whose moves wait on a checkpoint has it written, and a torrent
    // whose pieces fell [`CHECKPOINT_PIECES`] behind has them written.
    async fn settle(&self) {
        let dirty = self.inner.signal.take();
        for &job_id in &dirty {
            let made_whole = self.inner.signal.take_whole(job_id);
            let found = {
                let live = self.inner.live.lock().await;
                live.get(&job_id).and_then(|torrent| {
                    let store = Arc::clone(torrent.store.as_ref()?);
                    let levels = torrent.levels.clone();
                    Some((store, levels, torrent.complete()))
                })
            };
            let Some((store, levels, complete)) = found else {
                continue;
            };
            if let Some(error) = self.inner.signal.take_refused(job_id) {
                self.fail(job_id, error).await;
                continue;
            }
            if complete {
                continue;
            }
            self.files_downloaded(job_id, made_whole).await;
            let wanted = |file: usize| levels.get(file).copied().unwrap_or_default().wanted();
            let whole = store.has_files(wanted);
            let Some(tee) = store.tee() else {
                if whole {
                    self.completed(job_id).await;
                }
                continue;
            };
            let stepped = tee.lend_handed().and_then(|()| {
                // Shares of a file the torrent does not want never come.
                if whole {
                    tee.lose_unwanted(wanted)
                } else {
                    Ok(())
                }
            });
            if let Err(error) = stepped {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking step failed");
            }
            if let Some(error) = tee.refusal() {
                // A checkpoint the tee let go for it ends, on the torrent's
                // lane, before this: its failure goes with it.
                drop(
                    self.queue_save(job_id, "torrent_checkpoint_let_go", move |db| {
                        db.forget_checkpoint_failure(job_id);
                        Ok(())
                    }),
                );
                // With every file it wants, the torrent is done downloading
                // whatever its unpacking came to.
                if whole {
                    self.short(job_id, &store, error, &[]).await;
                } else {
                    self.refused(job_id, error).await;
                }
                continue;
            }
            if tee.with(|job| job.awaits_commit() || job.moves_waiting()) {
                self.checkpoint_tee(job_id, Arc::clone(tee), false).await;
            }
            if whole && tee.done() {
                match self.unpacked(job_id, &store, tee).await {
                    Placing::Waits => {}
                    Placing::Placed => self.completed(job_id).await,
                    Placing::Short { why, placed } => {
                        self.short(job_id, &store, why, &placed).await;
                    }
                }
            }
        }
        self.save_checkpoints(self.inner.signal.take_due()).await;
        // A pass can move any torrent, so every one is picked and shown
        // again; otherwise only the torrents that spoke changed, and only
        // they are (libtorrent posts a state update for the torrents that
        // changed, not every one). The tick picks and publishes every one.
        if self.inner.signal.take_pass() {
            self.apply_queue().await;
            self.apply_picking().await;
            self.publish().await;
        } else {
            self.pick(Some(&dirty)).await;
            for &job_id in &dirty {
                self.publish_job(job_id).await;
            }
        }
    }

    // Unpacking is over: what it made goes beside the torrent's files,
    // never over one, and is kept on the torrent's row as its job's
    // checkpoint goes. Its directory goes last, with what it spent. The
    // torrent's files are claimed as a move claims them, so its directory
    // stays where it is with no lock its reads and writes wait on; while a
    // move has them, this waits for the move to end, which marks the
    // torrent. It waits while its job still uses its storage, so the next
    // settle asks again. Files it could not place are never removed for
    // it: they stay in its directory, which stays, and it ends short with
    // the ones it did place.
    async fn unpacked(&self, job_id: JobId, store: &Arc<TorrentStore>, tee: &Arc<Tee>) -> Placing {
        let claimed = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Ok(false);
                }
                torrent.moving = Some(Moving::Outputs);
                Ok(true)
            })
            .await;
        if !matches!(claimed, Ok(true)) {
            return Placing::Waits;
        }
        let releasing = Arc::clone(tee);
        let spent = match tokio::task::spawn_blocking(move || releasing.spent()).await {
            Ok(Ok(spent)) => spent,
            Ok(Err(why)) => {
                tracing::warn!(
                    job_id = job_id.0,
                    why,
                    "a torrent's unpacked files wait on its job"
                );
                self.unclaim_outputs(job_id).await;
                return Placing::Waits;
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files were not placed");
                self.unclaim_outputs(job_id).await;
                return Placing::Waits;
            }
        };
        tee.stop_checkpoints();
        tee.forget();
        let work = tee.work().to_path_buf();
        let (planned, tee) = (Arc::clone(store), Arc::clone(tee));
        let planned = tokio::task::spawn_blocking(move || {
            let _job = JobScope::enter(job_id);
            let files = planned.files();
            let listed: Vec<PathBuf> = files.names().flatten().map(Path::to_path_buf).collect();
            plan_outputs(tee.work(), &files.dir(), &listed, &spent)
        })
        .await;
        let outputs = match planned {
            Ok(outputs) => Arc::new(outputs),
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files were not placed");
                let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
                    db.finish_torrent_unpack(job_id, &[], &[])
                });
                self.recorded(job_id, recorded.await);
                self.unclaim_outputs(job_id).await;
                return Placing::Short {
                    why: unplaced(&work, &error),
                    placed: Vec::new(),
                };
            }
        };
        // Kept on the row before they move, so a crash between leaves no
        // file of the torrent's that its delete does not know of.
        let names = outputs.names.clone();
        let kept = self.queue_save(job_id, "keep_torrent_outputs", move |db| {
            db.keep_torrent_outputs(job_id, &names)
        });
        self.recorded(job_id, kept.await);
        let (from, moving) = (work.clone(), Arc::clone(&outputs));
        let placed = tokio::task::spawn_blocking(move || {
            let _job = JobScope::enter(job_id);
            moving
                .place(&from)
                .map_err(|error| (error.to_string(), Some(moving.there())))
        })
        .await
        .unwrap_or_else(|error| Err((error.to_string(), None)));
        // A move that failed takes off the row the files it put back; one
        // whose end is not known leaves every one on it, for its delete.
        let (names, unplaced_names) = match &placed {
            Ok(()) | Err((_, None)) => (outputs.names.clone(), Vec::new()),
            Err((_, Some(there))) => {
                let there: HashSet<&String> = there.iter().collect();
                let left = outputs
                    .names
                    .iter()
                    .filter(|name| !there.contains(name))
                    .cloned()
                    .collect();
                (Vec::new(), left)
            }
        };
        let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
            db.finish_torrent_unpack(job_id, &names, &unplaced_names)
        });
        self.recorded(job_id, recorded.await);
        self.unclaim_outputs(job_id).await;
        if let Err((error, there)) = placed {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files did not move beside it");
            return Placing::Short {
                why: unplaced(&work, &error),
                placed: there.unwrap_or_default(),
            };
        }
        let removed = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(work)).await;
        if let Ok(Err(error)) = removed
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not remove a torrent's unpacking directory");
        }
        Placing::Placed
    }

    fn recorded(
        &self,
        job_id: JobId,
        recorded: Result<Result<(), StateError>, oneshot::error::RecvError>,
    ) {
        match recorded {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record what a torrent unpacked");
            }
            Err(_) => {
                tracing::warn!(
                    job_id = job_id.0,
                    "what a torrent unpacked was not recorded"
                );
            }
        }
    }

    // The directory a torrent's unpacking works in.
    fn unpacking_dir(&self, job_id: JobId) -> PathBuf {
        self.inner.env.work.join(format!("torrent-{}", job_id.0))
    }

    // The claim `unpacked` took on the torrent's files, given back.
    async fn unclaim_outputs(&self, job_id: JobId) {
        let _ = self
            .with_torrent(job_id, |torrent| {
                if matches!(torrent.moving, Some(Moving::Outputs)) {
                    torrent.moving = None;
                }
                Ok(())
            })
            .await;
    }

    // Write the checkpoints of `jobs`: each torrent's pieces with its
    // ledger. A tee checkpoints its job on its own, as a job of the
    // pipeline's does, once the bytes it took or their age make a write due.
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

    // Queue a write of the pieces the torrent has, with what changed in its
    // ledger and peers as they stand now, in one transaction: what a crash
    // recovers is the pieces and the counters of one moment. The ledger is
    // taken in the step that queues it, as every write of it is. Its tee's
    // checkpoint is written apart from it, as the pipeline writes its
    // jobs': a share the tee's checkpoint lacks of a piece kept here goes to
    // it again from the files at the next start. None when the torrent has
    // no store.
    fn queue_checkpoint(&self, torrent: &mut Live, now: i64) -> Option<CheckpointSave> {
        let job_id = torrent.record.job_id;
        let store = Arc::clone(torrent.store.as_ref()?);
        let pieces = store.unsaved();
        torrent.settle(now);
        let resume = torrent.take_resume(now, false);
        // Nothing changed since the last write: none is queued.
        if pieces.is_none() && resume.is_none() {
            return Some(CheckpointSave {
                job_id,
                landed: None,
                lost: None,
                pieces: None,
            });
        }
        let (counters, peers, lost) = resume.map_or((None, None, None), |resume| {
            (resume.counters, resume.peers, Some(resume.lost))
        });
        let wrote = pieces.is_some().then(|| Arc::clone(&store));
        let landed = self.queue_save(job_id, "torrent_checkpoint", move |db| {
            db.save_torrent_checkpoint(job_id, pieces.as_ref(), counters.as_ref(), peers.as_ref())
        });
        Some(CheckpointSave {
            job_id,
            landed: Some(landed),
            lost,
            pieces: wrote,
        })
    }

    // Queue a checkpoint of the torrent's unpacking on a blocking thread:
    // it waits on the job, which an intake writing to disk may hold. Its
    // write goes at once when `force`, a stop's, once every object out for
    // its work is back; otherwise once it is due.
    async fn checkpoint_tee(&self, job_id: JobId, tee: Arc<Tee>, force: bool) {
        let queued = tokio::task::spawn_blocking(move || {
            if force {
                tee.checkpoint_stopped()
            } else {
                tee.checkpoint(false)
            }
        })
        .await;
        if let Ok(Err(error)) = queued {
            tracing::warn!(job_id = job_id.0, error = %error, "could not checkpoint a torrent's unpacking");
        }
    }

    // Wait for a checkpoint's write to land. One that failed leaves what
    // the database holds of the ledger unknown, so the next writes all of
    // it.
    async fn checkpoint_landed(&self, save: CheckpointSave) {
        let CheckpointSave {
            job_id,
            landed,
            lost,
            pieces,
        } = save;
        let failed = match landed {
            None => None,
            Some(landed) => match landed.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(error) => Some(error.to_string()),
            },
        };
        match (failed, lost) {
            (Some(error), lost) => {
                if let Some(lost) = lost {
                    lost.store(true, Ordering::Release);
                }
                if let Some(store) = pieces {
                    store.lost_saved();
                }
                tracing::warn!(job_id = job_id.0, %error, "could not checkpoint a torrent");
            }
            (None, Some(_)) => self
                .inner
                .written
                .send_modify(|written| written.ledgers += 1),
            (None, None) => {}
        }
        self.inner
            .written
            .send_modify(|written| written.checkpoints += 1);
    }

    // FILE_DOWNLOADED, once for each of `files` files a piece made whole,
    // while the torrent downloads: one heard after its download ended is
    // dropped, as an NZB job raises none once its download is over. The
    // event names no file, as an NZB job's does not.
    async fn files_downloaded(&self, job_id: JobId, files: usize) {
        if files == 0 {
            return;
        }
        let live = self.inner.live.lock().await;
        let Some(torrent) = live.get(&job_id) else {
            return;
        };
        if torrent.complete() {
            return;
        }
        let Some(context) = self.script_event(torrent, QueueEvent::FileDownloaded, &[]) else {
            return;
        };
        for _ in 1..files {
            self.queue_script_event(Some(context.clone()));
        }
        self.queue_script_event(Some(context));
    }

    // The job is done: its completion is written on its own, so an intent
    // written beside it keeps it, and the ledger and peers as they stand.
    async fn completed(&self, job_id: JobId) {
        let now = self.inner.clock.now_ms();
        let (landing, downloaded) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            if torrent.complete() {
                return;
            }
            torrent.settle(now);
            let was_seeding = torrent.seeding();
            // Its download finished in this run only if it downloaded in
            // this run: one added with its data in place, or restored with
            // nothing left to fetch, did not (qBittorrent marks a torrent
            // finished after its check, or added in seed mode, without
            // `torrentFinished`; libtorrent sends `completed` only for a run
            // that began short of a seed).
            let downloaded = torrent.counters.downloaded_bytes > torrent.downloaded_at_start;
            torrent.record.completed_at_ms = Some(now);
            // It seeds now: its slot and rank go to the torrents after it.
            // One that seeded already, as a seed-mode add does, held none.
            if !was_seeding {
                self.inner.signal.want_pass();
            }
            self.inner.counters.event(TorrentEvent::Completed);
            if let Err(error) = self.inner.db.set_torrent_completed(job_id, now) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's completion");
            }
            (
                (
                    self.queue_resume(torrent, now, true),
                    torrent.record.move_to.take(),
                ),
                downloaded,
            )
        };
        let (landing, move_to) = landing;
        let _ = self.landed(landing).await;
        // One that downloaded under its download path moves under its save
        // path now, over any file of the same name there (qBittorrent
        // `TorrentImpl::handleTorrentFinished` and `adjustStorageLocation`).
        if let Some(to) = move_to {
            if let Err(error) = self.inner.db.clear_torrent_move_to(job_id) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's move");
            }
            if let Err(error) = self.move_storage(job_id, to, Existing::Replace).await {
                tracing::warn!(job_id = job_id.0, error = %error, "a complete torrent did not move under its save path");
            }
        }
        // Raised where its files now are, once any move is over.
        if downloaded {
            let live = self.inner.live.lock().await;
            if let Some(torrent) = live.get(&job_id) {
                self.raise_script_event(torrent, QueueEvent::TorrentDownloaded, &[]);
            }
        }
    }

    // The archive passwords a graph built for `torrent` now takes. One
    // already extracting keeps the list it started with.
    fn archive_passwords(
        &self,
        torrent: &Live,
        global: &[ArchivePasswordCandidate],
    ) -> Vec<String> {
        torrent_archive_passwords(torrent.rule(&self.rules()), global)
            .into_iter()
            .map(|candidate| candidate.value().to_owned())
            .collect()
    }

    // The archive passwords every torrent may try, as the settings give
    // them now.
    fn global_passwords(&self) -> Arc<[ArchivePasswordCandidate]> {
        self.inner
            .db
            .global_archive_password_candidates()
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "could not load global archive passwords");
                Vec::new()
            })
            .into()
    }

    // The tee's job's storage, in its own directory `work`.
    fn storage(
        &self,
        work: PathBuf,
        layout: &PieceLayout,
        plan: &Plan,
        passwords: Vec<String>,
    ) -> Storage {
        let db = self.inner.db.clone();
        let policy: limits::PolicyRead = Arc::new(move || {
            db.post_processing_settings()
                .map(Arc::new)
                .map_err(state_error)
        });
        let env = &self.inner.env;
        Storage {
            working_dir: work,
            output: None,
            accountant: Arc::clone(&env.accountant),
            decode: Arc::clone(&env.decode),
            free_disk: env.free_disk.clone(),
            slots: env.slots,
            mail: env.mail.clone(),
            passwords,
            caps: Caps::new(Arc::clone(&env.limits), policy, plan.declared(layout)),
            nesting: limits::NESTING,
            hardware: Default::default(),
            metrics: None,
            data_dir: env.data_dir.clone(),
            window_ledger: env.window_ledger.clone(),
        }
    }

    // The torrent's store over its files, with the pieces it kept, and the
    // tee its archive files go to when it unpacks and is not complete: the
    // one its checkpoint gives back, or a new one. A checkpoint that does
    // not restore is said, with why, and the tee starts as a new one. The
    // pieces it kept that the tee's checkpoint lacks a share of go to the
    // tee from the files, once; a piece whose files do not read is had no
    // longer, and is checked or fetched again. Nothing here fails the
    // torrent: a tee that cannot start ends its unpacking, and the torrent
    // keeps its files. It reads the database and the files, so it runs off
    // the tick's task. The pieces it kept whose bytes its files no longer
    // hold, a file gone or cut short, are had no longer either: each file
    // with a piece had is looked at by its size, once, as the store is made.
    // When none of those files is there, the pieces are kept as they were,
    // none is read for the tee, and the notes say so with `Note::Absent`,
    // which holds the torrent for its files missing. A restore whose
    // checkpoint the database could not
    // read makes no store: the torrent waits, its own rows as they were,
    // and the next restore tries it again, as a usenet job's restore is
    // held on the same error (`RestoreSeat::restore`).
    fn store<W>(
        &self,
        job_id: JobId,
        made: Making,
        hold: Hold<W>,
    ) -> Result<(TorrentStore, Vec<Note>), Unmade<W>> {
        let _job = JobScope::enter(job_id);
        let Making {
            files,
            layout,
            tee,
            passwords,
            seeded,
            kept,
        } = made;
        // One policy for both of a restore's database reads, the pieces it
        // kept and the tee's checkpoint: one the database fails to give
        // holds the restore until it gives it, as a usenet job's restore is
        // held. What it gives and this build cannot use is no record: the
        // pieces are checked again, the tee starts fresh.
        let kept = match kept.map_or_else(
            || {
                self.inner
                    .db
                    .torrent_pieces(job_id)
                    .map_err(|error| hold(error.to_string()))
            },
            Ok,
        ) {
            Ok(kept) => kept,
            Err(Ok(wait)) => return Err(Unmade::Waits(wait)),
            Err(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's pieces; they are checked again");
                None
            }
        };
        let files = Files::new(&layout, files).map_err(|error| Unmade::Fails(error.to_string()))?;
        let mut notes = Vec::new();
        let tee = match tee.then(|| Plan::of(&layout)).flatten() {
            Some(plan) => {
                let (tee, fresh) = self.tee(job_id, &layout, plan, passwords, hold)?;
                notes.extend(fresh);
                Some(tee)
            }
            None => None,
        };
        let store = TorrentStore::new(layout, files, kept.as_ref(), tee);
        let unbacked = store.drop_unbacked();
        if let Some(tee) = store.tee() {
            // With none of its files there, nothing is read from them: the
            // pieces stay for when they are.
            let replayed = if seeded || unbacked.is_none() {
                Replayed::default()
            } else {
                store.replay_had()
            };
            if replayed.taken > 0 {
                tracing::debug!(
                    job_id = job_id.0,
                    replayed = replayed.taken,
                    "a torrent's tee took again the pieces its checkpoint lacked"
                );
            }
            if let Some(error) = replayed.unread {
                notes.push(Note::Dropped {
                    pieces: replayed.dropped,
                    error: error.to_string(),
                });
            }
            // A run that will not start is kept as the tee's refusal.
            let _ = tee.resume();
            if tee.refusal().is_some() {
                self.inner.signal.mark(job_id);
            }
        }
        match unbacked {
            Some(0) => {}
            Some(missing) => {
                notes.push(Note::Missing { pieces: missing });
                self.inner.signal.marked(job_id, missing);
            }
            None => notes.push(Note::Absent {
                pieces: store.count(),
            }),
        }
        Ok((store, notes))
    }

    // The tee of a torrent whose files `plan` names, restored from its
    // checkpoint or new, and why it is new when it had a checkpoint. One
    // whose directory cannot be made ready refuses at once, for why. Rows
    // this build cannot read are no checkpoint; a read the database failed
    // holds a restore, and starts a new torrent's tee fresh.
    fn tee<W>(
        &self,
        job_id: JobId,
        layout: &Arc<PieceLayout>,
        plan: Plan,
        passwords: Vec<String>,
        hold: Hold<W>,
    ) -> Result<(Arc<Tee>, Option<Note>), Unmade<W>> {
        let work = self.unpacking_dir(job_id);
        let (rows, mut fresh) = match self.inner.db.load_graph_checkpoint(job_id) {
            Ok(rows) => (rows, None),
            Err(LoadError::Read(error)) => match hold(error.to_string()) {
                Ok(wait) => return Err(Unmade::Waits(wait)),
                Err(error) => (None, Some(Note::Unread(error))),
            },
            Err(error) => (None, Some(Note::Unread(error.to_string()))),
        };
        let env = &self.inner.env;
        let parts = || Parts {
            job_id,
            layout: Arc::clone(layout),
            plan: plan.clone(),
            work: work.clone(),
            repairs: Arc::clone(&env.repairs),
            signal: Arc::clone(&self.inner.signal),
            outside: Arc::clone(&env.outside),
            limit: env.slots,
            db: self.inner.db.clone(),
            mail: env.written.clone(),
        };
        let storage = || self.storage(work.clone(), layout, &plan, passwords.clone());
        if let Some(rows) = rows {
            match Tee::restore(parts(), storage(), rows) {
                Ok(tee) => return Ok((tee, None)),
                // A rebuild that failed writing or stepping is held, as a
                // usenet job's is: only a checkpoint this build cannot read
                // starts fresh.
                Err(error) if !error.not_this_builds() => match hold(error.to_string()) {
                    Ok(wait) => return Err(Unmade::Waits(wait)),
                    Err(error) => fresh = Some(Note::Unrestored(error)),
                },
                Err(error) => fresh = Some(Note::Unrestored(error.to_string())),
            }
        }
        let tee = Tee::fresh(parts(), storage());
        // Nothing the job wrote before is its now.
        if let Err(error) = std::fs::remove_dir_all(&work)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tee.refuse(format!(
                "could not clear the torrent's unpacking directory: {error}"
            ));
        } else if let Err(error) = std::fs::create_dir_all(&work) {
            tee.refuse(format!(
                "could not make the torrent's unpacking directory: {error}"
            ));
        }
        Ok((tee, fresh))
    }

    // What a torrent's store is made from, read under the torrent's lock.
    fn making(&self, torrent: &Live, global: &[ArchivePasswordCandidate]) -> Option<Making> {
        Some(Making {
            files: files_dir(torrent),
            layout: Arc::clone(torrent.layout.as_ref()?),
            tee: unpacks(torrent) && !torrent.complete(),
            passwords: self.archive_passwords(torrent, global),
            seeded: torrent.record.seed_mode,
            kept: None,
        })
    }

    // The store joined to what feeds it: the intake the session hands its
    // pieces to.
    fn give_store(&self, torrent: &mut Live, store: TorrentStore) {
        let job_id = torrent.record.job_id;
        let env = &self.inner.env;
        let store = Arc::new(store);
        let signal = Arc::clone(&self.inner.signal);
        let intake = Arc::new(Intake::new(
            Arc::clone(&store) as Arc<dyn Pieces>,
            Room::new(Arc::clone(&env.limits), env.free_disk.clone()),
            Box::new(move |heard| signal.heard(job_id, heard)),
        ));
        torrent.file_have = store.file_have();
        if let Some(tee) = store.tee() {
            tee.hold_own_clock(torrent.record.intent == TorrentIntent::Paused);
        }
        torrent.store = Some(store);
        torrent.intake = Some(intake);
    }

    // Where a torrent's bytes in memory are counted: the accountant every
    // job shares, under the torrent's namespace.
    fn resident(&self, job_id: JobId) -> Holds {
        Holds {
            accountant: Arc::clone(&self.inner.env.accountant),
            namespace: weaver_graph::holds::HoldsNamespace {
                scope: job_id.0,
                node: weaver_graph::NodeId::ROOT,
            },
        }
    }

    // The torrent's metainfo and its store over it, at once. A store that
    // cannot be made fails the torrent. The files `left_out`, by their
    // index among the manifest's paths, are not downloaded: their level is
    // `Ignored`, kept as the operator's own would be.
    fn give_metainfo(
        &self,
        torrent: &mut Live,
        parsed: &Parsed,
        left_out: &[usize],
    ) -> Result<(), String> {
        let job_id = torrent.record.job_id;
        let mut stored = self.inner.db.torrent_file_priorities(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
            Vec::new()
        });
        if !left_out.is_empty() {
            let left_out: HashSet<usize> = left_out.iter().copied().collect();
            stored.resize(parsed.layout.file_count(), FilePriority::Normal);
            let listed = parsed
                .layout
                .files()
                .enumerate()
                .filter(|(_, file)| !file.padding)
                .map(|(index, _)| index);
            for (path, index) in listed.enumerate() {
                if left_out.contains(&path) {
                    stored[index] = FilePriority::Ignored;
                }
            }
            self.inner
                .db
                .set_torrent_file_priorities(job_id, &stored)
                .map_err(state_error)?;
        }
        self.describe(torrent, parsed, stored);
        let Some(made) = self.making(torrent, &self.global_passwords()) else {
            return Ok(());
        };
        let (store, notes) = self
            .store::<std::convert::Infallible>(job_id, made, Err)
            .map_err(|unmade| match unmade {
                Unmade::Fails(error) => error,
                Unmade::Waits(never) => match never {},
            })?;
        for note in &notes {
            note.say(job_id);
        }
        torrent.files_missing = Note::absent(&notes);
        self.give_store(torrent, store);
        Ok(())
    }

    // What the metainfo says of the torrent, without its job. Whether it
    // unpacks is fixed here and kept, so a restart builds the same tree.
    fn describe(&self, torrent: &mut Live, parsed: &Parsed, stored: Vec<FilePriority>) {
        let job_id = torrent.record.job_id;
        if torrent.record.unpack_in_force.is_none() {
            let unpack = self
                .settings()
                .unpack(&torrent.record, torrent.rule(&self.rules()));
            if let Err(error) = self.inner.db.set_torrent_unpack_in_force(job_id, unpack) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record whether a torrent unpacks");
            }
            torrent.record.unpack_in_force = Some(unpack);
        }
        torrent.names = parsed.manifest.paths.clone();
        torrent.total = parsed.total;
        torrent.layout = Some(Arc::clone(&parsed.layout));
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
    // only, each restored from its checkpoint, RESTORES_AT_ONCE at a time. A
    // torrent reads as it was kept until its job is back.
    async fn restore_jobs(&self) {
        let waiting: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.store.is_none() && torrent.layout.is_some())
            .map(|torrent| torrent.record.job_id)
            .collect();
        // RESTORES_AT_ONCE in flight, each torrent attached as soon as its
        // job is back rather than at the next tick, so the first seeds
        // without waiting for the last to restore (qBittorrent's
        // startUpTorrents keeps MAX_PROCESSING_RESUMEDATA_COUNT in flight
        // into libtorrent's async_add_torrent).
        // The global archive passwords, read once for all of them and
        // outside the live lock.
        let global = (!waiting.is_empty()).then(|| self.global_passwords());
        let mut restoring = tokio::task::JoinSet::new();
        for job_id in waiting {
            if restoring.len() >= RESTORES_AT_ONCE {
                restoring.join_next().await;
            }
            let service = self.clone();
            let global = global.clone();
            restoring.spawn(async move {
                if service.restore_job_with(job_id, global).await {
                    service.attach(job_id, None).await;
                }
            });
        }
        while restoring.join_next().await.is_some() {}
    }

    // The job of a torrent that waits for one, restored on a blocking thread
    // under no lock but its own job's and a restore permit, and given
    // to the torrent if it still waits: one removed meanwhile drops it. A
    // restore that panics is a checkpoint that does not restore. Whether it
    // gave the torrent its job, unheld, so it may attach now.
    async fn restore_job(&self, job_id: JobId) -> bool {
        self.restore_job_with(job_id, None).await
    }

    // [`Self::restore_job`] with the global archive passwords read for it,
    // or none to read them when its store is made.
    async fn restore_job_with(
        &self,
        job_id: JobId,
        global: Option<Arc<[ArchivePasswordCandidate]>>,
    ) -> bool {
        // However the restore ends, even cancelled or with the runtime, its
        // entry goes: a torrent with its job, or gone, waits for no restore.
        struct Done<'a>(&'a TorrentService, JobId);
        impl Drop for Done<'_> {
            fn drop(&mut self) {
                self.0
                    .inner
                    .restoring
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&self.1);
            }
        }
        let this_job = Arc::clone(
            self.inner
                .restoring
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(job_id)
                .or_default(),
        );
        let guard = (
            this_job.lock_owned().await,
            Arc::clone(&self.inner.restore_permits)
                .acquire_owned()
                .await
                .expect("the restore permits are never closed"),
        );
        let _done = Done(self, job_id);
        // A torrent whose work crashed the daemon would restore into the
        // same panic: it is kept paused, with why, and its job is not
        // rebuilt until a resume takes its marker.
        if self.park(job_id).await {
            return false;
        }
        let waiting = {
            let mut live = self.inner.live.lock().await;
            live.get_mut(&job_id)
                .filter(|torrent| torrent.store.is_none())
                .and_then(|torrent| {
                    let global = global.unwrap_or_else(|| self.global_passwords());
                    let made = self.making(torrent, &global)?;
                    Some(Making {
                        kept: torrent.kept_at_boot.take(),
                        ..made
                    })
                })
        };
        let Some(made) = waiting else {
            return false;
        };
        let restoring = {
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                service
                    .inner
                    .written
                    .send_modify(|written| written.checkpoints_read += 1);
                let store = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    service.store(job_id, made, Ok)
                }))
                .unwrap_or_else(|_| Err(Unmade::Fails("the restore panicked".to_string())));
                (guard, store)
            })
        };
        // The blocking task ends only with its result, or with the runtime.
        let Ok((_restoring, store)) = restoring.await else {
            return false;
        };
        let (store, missing) = match store {
            Ok((store, notes)) => {
                for note in &notes {
                    note.say(job_id);
                }
                (store, Note::absent(&notes))
            }
            Err(Unmade::Waits(error)) => {
                // Said once while it stands; the torrent shows it as its
                // error until a restore reads it.
                let mut live = self.inner.live.lock().await;
                if let Some(torrent) = live.get_mut(&job_id) {
                    let why = format!("its restore is held: {error}");
                    if torrent.restore_held.replace(why).is_none() {
                        tracing::warn!(job_id = job_id.0, error = %error, "a torrent's restore is held; it is tried again");
                    } else {
                        tracing::debug!(job_id = job_id.0, error = %error, "a torrent's restore is still held");
                    }
                }
                return false;
            }
            Err(Unmade::Fails(error)) => {
                self.fail(job_id, error).await;
                return false;
            }
        };
        match self
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .filter(|torrent| torrent.store.is_none())
        {
            Some(torrent) => {
                torrent.restore_held = None;
                torrent.files_missing = missing;
                self.give_store(torrent, store);
                torrent.held.is_none() && !missing
            }
            None => {
                store.forget();
                false
            }
        }
    }

    // Keep a torrent that crashed the daemon paused, its restore held for
    // why, until a resume takes its marker. Its marker is read under the
    // torrent's lock, which a resume takes after the marker, so a resume
    // is never undone. Whether the torrent waits for a resume.
    async fn park(&self, job_id: JobId) -> bool {
        let mut live = self.inner.live.lock().await;
        let Some(why) = self.crashed(job_id) else {
            return false;
        };
        let Some(torrent) = live.get_mut(&job_id) else {
            return true;
        };
        if torrent.record.intent == TorrentIntent::Resumed {
            if let Err(error) = self
                .inner
                .db
                .update_torrent_intent(job_id, TorrentIntent::Paused)
            {
                tracing::warn!(job_id = job_id.0, error = %error, "could not keep a crashed torrent's pause");
            }
            torrent.settle(self.inner.clock.now_ms());
            torrent.record.intent = TorrentIntent::Paused;
        }
        torrent.restore_held = Some(why);
        true
    }

    // Add a torrent. A metainfo becomes a job at once; a magnet needs its
    // route's session to fetch the metainfo, and becomes one when it has.
    /// Whether the route a torrent in `scope` would take carries DHT, so a
    /// magnet with no trackers can find its peers there.
    pub async fn carries_dht(&self, scope: TorrentScope) -> bool {
        let plan = self.inner.sessions.routes().plan(scope);
        let (plan, _) = self.placed(&plan, None).await;
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
        let mut request = request;
        let relative = |path: &Option<String>| {
            path.as_deref()
                .is_some_and(|path| !Path::new(path).is_absolute())
        };
        let placement = &request.placement;
        if relative(&placement.save_path)
            || relative(&placement.download_path)
            || (placement.download_path.is_some() && placement.save_path.is_none())
        {
            let parent = completion_parent(
                &settings.complete_dir,
                &settings.categories,
                category.as_ref().map(|category| category.name.as_str()),
            )
            .map_err(AddError::Invalid)?;
            request.placement = absolute_placement(&request.placement, &parent);
        }
        match request.source.clone() {
            AddSource::Metainfo(bytes) => {
                let bytes = Bytes::from(bytes);
                let parsed = parse(&bytes)?;
                let left_out = self.admit(&parsed.manifest)?;
                self.duplicate(parsed.hashes)?;
                let job_id = self.reserve()?;
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    parsed.hashes,
                    parsed.manifest.name.clone(),
                    TorrentSource::Metainfo,
                    Some(bytes),
                );
                let (dir, files_dir, adopted) = self
                    .place(
                        &settings,
                        category.as_ref(),
                        &record,
                        &parsed.manifest,
                        job_id,
                    )
                    .await?;
                record.storage_dir = Some(dir.clone());
                record.files_dir = files_dir;
                record.seed_mode = request.placement.seed_mode;
                let mut torrent = match self.insert(record, &parsed.trackers, &adopted) {
                    Ok(torrent) => torrent,
                    Err(error) => {
                        give_back(&dir).await;
                        return Err(error);
                    }
                };
                self.tag_added(job_id, &request.tags);
                let made = self.give_metainfo(&mut torrent, &parsed, &left_out);
                // Raised for an add, never for a restore (qBittorrent emits
                // `torrentAdded` from `addTorrent_impl` alone).
                self.raise_script_event(&torrent, QueueEvent::TorrentAdded, &[]);
                self.enqueue(torrent).await;
                match made {
                    Ok(()) => self.attach(job_id, None).await,
                    Err(error) => self.fail(job_id, error).await,
                }
                self.inner.counters.event(TorrentEvent::Added);
                self.publish_job(job_id).await;
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
                let plan = self.inner.sessions.routes().plan(scope);
                let (plan, pin) = self.placed(&plan, None).await;
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
                // A magnet has no files to seed until its metainfo is in
                // hand (libtorrent `torrent_flags::seed_mode` needs the
                // metainfo at add): it downloads.
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    hashes,
                    name,
                    TorrentSource::Magnet,
                    None,
                );
                record.pool_member = pin;
                let mut torrent = self.insert(record, &trackers, &Adopted::default())?;
                self.tag_added(job_id, &request.tags);
                let identity = identity_of(torrent.rule(&rules), &torrent.identity);
                let terms = PeerTerms::of(&self.settings(), torrent.rule(&rules));
                torrent.route = Some(plan.key.clone());
                self.raise_script_event(&torrent, QueueEvent::TorrentAdded, &[]);
                self.enqueue(torrent).await;
                self.spawn_resolve(job_id, plan, uri, None, identity, terms)
                    .await;
                self.inner.counters.event(TorrentEvent::Added);
                self.publish_job(job_id).await;
                Ok(job_id)
            }
        }
    }

    // A torrent just added takes the tags it was added with. It is in the
    // store already, so a tag that cannot be written leaves it untagged
    // rather than undoing the add.
    fn tag_added(&self, job_id: JobId, tags: &[String]) {
        if let Err(error) = self
            .inner
            .db
            .tag_new_job(job_id, tags.iter().map(String::as_str))
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not tag a torrent");
        }
    }

    // A torrent just added joins the live ones last in the queue. No
    // earlier torrent's turn or rank moves for it, so its own follow from
    // the queue's end as the last pass left it, which it then extends: an
    // add reads no other torrent. Whatever changed since that pass is
    // settled at the next. One that took its place before another added
    // since, and arrives after it, asks for that pass now: the turns given
    // since are out of order.
    async fn enqueue(&self, mut torrent: Live) {
        let mut live = self.inner.live.lock().await;
        let settings = self.settings();
        let rules = self.rules();
        let holds = self.holds();
        let now = self.inner.clock.now_ms();
        let queued = queued(&settings, &rules, &torrent, now);
        let place = (torrent.record.queue_position, torrent.record.job_id);
        {
            let mut end = self
                .inner
                .queue_end
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let turn = end.slots.take(&queued, &holds);
            if end.last.is_some_and(|last| place < last) {
                self.inner.signal.want_pass();
            } else {
                end.last = Some(place);
            }
            take_turn(&mut torrent, &queued, turn, &holds, now);
        }
        live.insert(torrent.record.job_id, torrent);
    }

    // The job of `job_id`, which changed, into the published queue beside
    // the others as they stand: a settle reads the torrents that spoke, not
    // every one (libtorrent posts a state update for the torrents that
    // changed). The exporter's reading takes it at the next publish of
    // every torrent.
    async fn publish_job(&self, job_id: JobId) {
        let Some(queue) = self.queue() else {
            return;
        };
        let now = self.inner.clock.now_ms();
        if let Some(view) = self.torrent(job_id).await {
            let status = job_status(&view);
            queue.publish_owned_job(report(view, status, now).job_info());
        }
    }

    // The files of `manifest.paths`, by index, that admission leaves out.
    fn admit(&self, manifest: &Manifest) -> Result<Vec<usize>, AddError> {
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

    // Where a torrent's job writes, and where its files go when that is not
    // there, as `record` asks. Without a save path or a layout that strips
    // the folder, its files go in a folder of its own claimed under its
    // category's directory, as weaver lays every torrent out. With a save
    // path they go under it as qBittorrent lays them out
    // (`SessionImpl::addTorrent_impl`), the metainfo's layout unless asked
    // otherwise, into whatever is there already. Files with no folder of
    // their own sit straight in the directory they go to, and the job
    // writes in a folder of its own claimed as weaver claims one. With it,
    // what was already in the directory the files go to before anything is
    // made there: nothing in a folder Weaver claims.
    async fn place(
        &self,
        settings: &TorrentSettings,
        category: Option<&CategoryConfig>,
        record: &TorrentRecord,
        manifest: &Manifest,
        job_id: JobId,
    ) -> Result<(String, Option<String>, Adopted), AddError> {
        let found = |dir: PathBuf| {
            let names = manifest.paths.clone();
            async move {
                tokio::task::spawn_blocking(move || files::adopted(&dir, &names))
                    .await
                    .map_err(|error| AddError::State(error.to_string()))
            }
        };
        let layout = record.content_layout;
        let category_parent = || {
            completion_parent(
                &settings.complete_dir,
                &settings.categories,
                category.map(|category| category.name.as_str()),
            )
            .map_err(AddError::Invalid)
        };
        // Absolute: `add` resolved a relative one.
        let parent = record.save_path.as_deref().map(PathBuf::from);
        let foldered = layout
            .unwrap_or(ContentLayout::Original)
            .foldered(manifest.single_file);
        let made = |dir: &Path| {
            std::fs::create_dir_all(dir).map_err(|error| {
                AddError::State(format!("could not make the torrent's directory: {error}"))
            })
        };
        match parent {
            Some(parent) if foldered => {
                let dir = parent.join(admission::output_name(manifest));
                let adopted = found(dir.clone()).await?;
                made(&dir)?;
                Ok((dir.to_string_lossy().into_owned(), None, adopted))
            }
            Some(parent) => {
                let adopted = found(parent.clone()).await?;
                made(&parent)?;
                let dir = self.claim_dir(settings, category, manifest, job_id).await?;
                Ok((dir, Some(parent.to_string_lossy().into_owned()), adopted))
            }
            None if layout.is_none_or(|layout| layout.foldered(manifest.single_file)) => Ok((
                self.claim_dir(settings, category, manifest, job_id).await?,
                None,
                Adopted::default(),
            )),
            None => {
                let parent = category_parent()?;
                let adopted = found(parent.clone()).await?;
                made(&parent)?;
                let dir = self.claim_dir(settings, category, manifest, job_id).await?;
                Ok((dir, Some(parent.to_string_lossy().into_owned()), adopted))
            }
        }
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
    ) -> TorrentRecord {
        // qBittorrent `TorrentImpl`: a torrent with a download path in use
        // downloads there and moves under its save path once complete.
        let placement = &request.placement;
        let (save_path, move_to) = match &placement.download_path {
            Some(download) if !placement.seed_mode => {
                (Some(download.clone()), placement.save_path.clone())
            }
            _ => (placement.save_path.clone(), None),
        };
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
            storage_dir: None,
            policy_override: SeedingPolicy::kept(request.policy_override),
            unpack: request.unpack,
            unpack_in_force: None,
            metadata: request.metadata.clone(),
            upload_limit: request.upload_limit,
            download_limit: request.download_limit,
            sequential: false,
            first_last_piece_first: false,
            save_path,
            content_layout: placement.layout,
            files_dir: None,
            seed_mode: false,
            move_to,
            queue_position: self
                .inner
                .next_queue_position
                .fetch_add(1, Ordering::Relaxed),
            pool_member: None,
        }
    }

    // Keep `record` with its trackers: a torrent new to the database, under
    // the rule its trackers match, and forced when that rule admits its
    // torrents so.
    fn insert(
        &self,
        mut record: TorrentRecord,
        trackers: &[TrackerEntry],
        adopted: &Adopted,
    ) -> Result<Live, AddError> {
        let rules = self.rules();
        let rule = tracker_rule_for(&rules, trackers.iter().map(|tracker| tracker.url.as_str()));
        record.forced |= rule.is_some_and(|rule| rule.forced);
        let rule_id = rule.map(|rule| rule.id);
        match self
            .inner
            .db
            .insert_placed_torrent(&record, trackers, adopted)
        {
            Ok(TorrentInsert::Inserted) => {
                let now = self.inner.clock.now_ms();
                let mut torrent = Live::new(
                    record,
                    TorrentCounters::default(),
                    ResumePeers::default(),
                    now,
                );
                torrent.rule_id = rule_id;
                torrent.trackers = in_table_order(trackers.to_vec());
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
        let left_out = self.admit(&parsed.manifest)?;
        let settings = self.settings();
        let Some(record) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| torrent.record.clone())
        else {
            return Ok(());
        };
        let known = hashes_of(&record);
        // A hash only the metainfo names may be another torrent's: a hybrid
        // added once by each of its hashes.
        self.duplicate(InfoHashes {
            v1: parsed.hashes.v1.filter(|_| known.v1.is_none()),
            v2: parsed.hashes.v2.filter(|_| known.v2.is_none()),
        })?;
        let category = settings.category(record.category_id).cloned();
        let (dir, files_dir, adopted) = self
            .place(
                &settings,
                category.as_ref(),
                &record,
                &parsed.manifest,
                job_id,
            )
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
        torrent.record.files_dir = files_dir.clone();
        torrent.record.metainfo = Some(bytes.clone());
        // The trackers the magnet listed stay, edits included, each as
        // whoever named it; the metainfo's own join after them as the
        // torrent's.
        let mut trackers = torrent.trackers.to_vec();
        trackers.extend(parsed.trackers.iter().cloned());
        let trackers = tidy_trackers(trackers);
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
        // The table holds the merged list now, whatever the next write does.
        torrent.rule_id = self.rule_id(&trackers);
        torrent.trackers = in_table_order(trackers);
        self.inner
            .db
            .set_torrent_files_dir(job_id, files_dir.as_deref())
            .map_err(|error| AddError::State(state_error(error)))?;
        self.inner
            .db
            .set_torrent_adopted(job_id, &adopted)
            .map_err(|error| AddError::State(state_error(error)))?;
        let made = self.give_metainfo(torrent, &parsed, &left_out);
        drop(live);
        if let Err(error) = made {
            self.fail(job_id, error).await;
            return Ok(());
        }
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
        let plan = self
            .pinned(job_id, self.inner.sessions.routes().plan(scope))
            .await;
        // The trackers it lists, in their tiers: a magnet's fetch announces
        // to tiers, not to a bare list.
        let mut tiers = Vec::new();
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id) {
            torrent.route = Some(plan.key.clone());
            torrent.held = None;
            tiers = tracker_tiers(&torrent.trackers);
        }
        let uri = Magnet::of(hashes_of(&record), None, Vec::new()).to_string();
        self.spawn_resolve(
            job_id,
            plan,
            uri,
            Some(tiers),
            identity_of(rule, &own),
            PeerTerms::of(&self.settings(), rule),
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
            verified,
        ) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            // One whose files are missing joins no session: it would claim
            // pieces it cannot read and write where its files are not.
            if torrent.attached.is_some() || torrent.files_missing {
                return;
            }
            let (Some(bytes), Some(layout), Some(intake), Some(store)) = (
                torrent.record.metainfo.clone(),
                torrent.layout.clone(),
                torrent.intake.clone(),
                torrent.store.clone(),
            ) else {
                return;
            };
            let rule = torrent.rule(&rules);
            let ratelimits =
                ratelimits_of(&torrent.record, settings.upload_cap(&torrent.record, rule));
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
                (
                    store.initial_have(placed(&torrent.record)),
                    torrent.record.seed_mode,
                    store,
                ),
                identity_of(rule, &torrent.identity),
                torrent.seen,
                PeerTerms::of(&settings, rule),
                certificate_of(rule),
                (
                    fetch_levels(torrent),
                    torrent.record.sequential,
                    torrent.record.first_last_piece_first,
                ),
                // libtorrent `torrent::read_resume_data`: the saved peers
                // are known from the start and the saved bans hold.
                torrent.resume.clone(),
                // The trackers it lists now, each with who named it, as a
                // tracker edit leaves them under this lock: they stand in
                // for its metainfo's.
                torrent.trackers.clone(),
                torrent.verified_at_boot.take(),
            )
        };
        let plan = self
            .pinned(job_id, self.inner.sessions.routes().plan(scope))
            .await;
        // It reads as its route does while it waits for the route's
        // session: opening, or why the route's last open failed.
        if let Some(torrent) = self.inner.live.lock().await.get_mut(&job_id)
            && torrent.attached.is_none()
            && torrent.held.is_none()
        {
            torrent.route = Some(plan.key.clone());
            torrent.held = Some(Held::Session(OPENING));
        }
        let session = match self.open_session(&plan, &identity).await {
            Ok(session) => session,
            Err(error) => {
                self.held(job_id, Some(plan.key), held_by(error)).await;
                return;
            }
        };
        // The session takes it only while it is still here: a remove made
        // while the route's session opened has deleted its files, and the
        // session would make them again. A remove made from here on waits
        // until the session has it, then takes it out.
        let Some(gate) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| Arc::clone(&torrent.gate))
        else {
            return;
        };
        let _taking = gate.lock().await;
        if self.inner.live.lock().await.get(&job_id).is_none() {
            return;
        }
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
        let pieces = intake as Arc<dyn Pieces>;
        let factory = PieceStorageFactory::new(
            Arc::clone(&layout),
            Arc::clone(&pieces),
            self.resident(job_id),
        );
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
        let (have, seed_mode, store) = have;
        apply_initial_have(&mut options, have);
        options.seed_mode = seed_mode;
        // The pieces that passed their check in seed mode before are passed
        // again (libtorrent restores `verified_pieces` on a seed-mode add).
        // Lost, they are checked again as a peer asks for them.
        if seed_mode {
            let verified = verified.map_or_else(|| self.inner.db.torrent_seed_verified(job_id), Ok);
            options.seed_verified = match verified {
                Ok(verified) => {
                    verified.map(|bytes| BF::from_boxed_slice(bytes.into_boxed_slice()))
                }
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read the pieces a torrent checked in seed mode");
                    None
                }
            };
        }
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
        // Another attach of this torrent may have finished while this one
        // waited: the session it put the torrent in keeps it, and a second
        // copy in another session is let go.
        let attached_meanwhile = live
            .get(&job_id)
            .and_then(|torrent| torrent.attached.as_ref())
            .map(|attached| {
                Arc::ptr_eq(&attached.session, &session) && attached.handle.id() == handle.id()
            });
        if let Some(same) = attached_meanwhile {
            drop(live);
            if !same
                && let Err(error) = session
                    .delete(TorrentIdOrHash::Id(handle.id()), false)
                    .await
            {
                tracing::debug!(job_id = job_id.0, error = %error, "a second copy of a torrent was already gone from its session");
            }
            return;
        }
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
                if torrent.trackers != listed {
                    handle.replace_trackers(tracker_tiers(&torrent.trackers));
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
                if seed_mode {
                    adopt_placed(
                        &handle,
                        &store,
                        job_id,
                        Arc::clone(&self.inner.signal),
                        session.cancellation_token().clone().cancelled_owned(),
                    );
                }
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

    // The plan a torrent not yet kept would run on, and the pool member it
    // would be pinned to: placed among the torrents kept now.
    async fn placed(&self, plan: &RoutePlan, pinned: Option<u32>) -> (RoutePlan, Option<u32>) {
        let load = load_of(self.inner.live.lock().await.values());
        plan.place(pinned, |key| load.get(key).copied().unwrap_or(0))
    }

    // The plan `job_id` runs on: on a pool route its member's, kept or
    // placed as `RoutePlan::place` says among the other torrents, its pin
    // written as it changes. It counts on that member at once, so the next
    // torrent placed sees it there.
    async fn pinned(&self, job_id: JobId, plan: RoutePlan) -> RoutePlan {
        let mut live = self.inner.live.lock().await;
        let load = load_of(
            live.values()
                .filter(|torrent| torrent.record.job_id != job_id),
        );
        let Some(torrent) = live.get_mut(&job_id) else {
            return plan;
        };
        let (plan, pin) = plan.place(torrent.record.pool_member, |key| {
            load.get(key).copied().unwrap_or(0)
        });
        self.pin(torrent, pin);
        if pin.is_some() {
            torrent.route = Some(plan.key.clone());
        }
        plan
    }

    // `torrent` is pinned to pool member `pin` from here, or to none.
    fn pin(&self, torrent: &mut Live, pin: Option<u32>) {
        if torrent.record.pool_member == pin {
            return;
        }
        torrent.record.pool_member = pin;
        if let Err(error) = self
            .inner
            .db
            .set_torrent_pool_member(torrent.record.job_id, pin)
        {
            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not record the pool member a torrent is pinned to");
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
    // before the session lets it go. Its tee stays with it, and writes its
    // own checkpoint when that is due, as it does in a session.
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
        let stopped = self.seeding_stopped(&torrent, "failed");
        // Its slot and rank go to the torrents after it.
        self.inner.signal.want_pass();
        self.inner.counters.event(TorrentEvent::Failed);
        self.detach(&mut torrent).await;
        self.retire(torrent, "failed", Some(error)).await;
        self.queue_script_event(stopped);
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
            self.queue_script_event(self.seeding_stopped(torrent, "failed"));
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

    // Unpacking ended short of what it would deliver, on a torrent with
    // every file it wants: the torrent completes and seeds on to its goal,
    // saying `why`, and its job goes to history as what it delivered says
    // (`short_status`): its files it has whole and the unpacked files
    // `placed` beside them.
    async fn short(&self, job_id: JobId, store: &TorrentStore, why: String, placed: &[String]) {
        self.completed(job_id).await;
        let mut delivered = store.whole_names();
        delivered.extend(placed.iter().cloned());
        let status = short_status(&delivered, &why);
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (report, hash) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            torrent.failed = Some(why.clone());
            let mut report = report(self.view(&settings, torrent, now), status.clone(), now);
            report.shortfall = Some(why);
            (report, hashes_of(&torrent.record).best())
        };
        if matches!(status, JobStatus::Failed { .. }) {
            self.inner.counters.event(TorrentEvent::Failed);
        }
        self.finish(report, status.persisted_status(), hash, now)
            .await;
    }

    // A torrent that left the live ones goes to history: its row, then out
    // of the torrents and into the finished jobs.
    async fn retire(&self, torrent: Live, status: &str, error: Option<String>) {
        let job_id = torrent.record.job_id;
        self.inner.env.outside.leave(job_id.0);
        if let Some(store) = &torrent.store {
            store.forget();
        }
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let mut report = report(
            self.view(&settings, &torrent, now),
            job_status_from_persisted_str(status, error.as_deref()),
            now,
        );
        report.shortfall = error;
        let row = report.history_row(
            status,
            Some(hashes_of(&torrent.record).best().0.to_vec()),
            now,
        );
        // Its history row and its removal are one write: the job is never
        // in neither, so an event raised while it was live, still being
        // queued, finds it.
        if let Err(error) = self.inner.db.retire_torrent(&row) {
            tracing::warn!(job_id = job_id.0, error = %error, "could not move a torrent to history");
        }
        self.finished(report).await;
    }

    // A torrent's job, finished as `status`: into history and the queue's
    // finished jobs.
    async fn finish(&self, report: JobReport, status: &str, hash: Id20, now: i64) {
        let job_id = report.job_id;
        let row = report.history_row(status, Some(hash.0.to_vec()), now);
        if let Err(error) = self.inner.db.insert_job_history(&row) {
            tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's history");
        }
        self.finished(report).await;
    }

    // A torrent's job, its history row written: out to the queue's
    // finished jobs.
    async fn finished(&self, report: JobReport) {
        let job_id = report.job_id;
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
        self.remove_as(job_id, delete_files, confirm_unmet_goal, Ending::Asked)
            .await
    }

    async fn remove_as(
        &self,
        job_id: JobId,
        delete_files: bool,
        confirm_unmet_goal: bool,
        ending: Ending,
    ) -> Result<(), ActionError> {
        // Its outcome is its job's: a job still waiting for the tick is
        // restored first.
        self.restore_job(job_id).await;
        // A session taking it finishes first; one not yet taking it finds
        // it gone and never does.
        let gate = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| Arc::clone(&torrent.gate))
            .ok_or(ActionError::NotFound(job_id))?;
        let _removing = gate.lock().await;
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let mut live = self.inner.live.lock().await;
        let torrent = live.get(&job_id).ok_or(ActionError::NotFound(job_id))?;
        if torrent.private && !confirm_unmet_goal {
            let policy = settings.policy(&torrent.record, torrent.rule(&self.rules()));
            if let Some(left) = status::goal_left(&policy, &torrent.counters, torrent.total, now) {
                return Err(ActionError::UnmetGoal(left));
            }
        }
        let mut torrent = live.remove(&job_id).ok_or(ActionError::NotFound(job_id))?;
        drop(live);
        // What its scripts hear of a removal asked for: a seed stops
        // seeding; a download is cancelled, as an NZB job's is.
        let ended = match ending {
            Ending::Goal => None,
            Ending::Asked if torrent.seeding() && torrent.resumed() => {
                self.seeding_stopped(&torrent, "removed")
            }
            Ending::Asked if !torrent.complete() => self.script_event(
                &torrent,
                QueueEvent::TorrentDeleted,
                &[("NZBNA_DELETESTATUS", "MANUAL")],
            ),
            Ending::Asked => None,
        };
        // Its slot and rank go to the torrents after it.
        self.inner.signal.want_pass();
        if let Some(Resolving::Fetching(fetch)) = torrent.resolving.take() {
            fetch.abort();
            self.inner.counters.fetch(FetchOutcome::Cancelled);
        }
        let written = self.written_paths(&torrent);
        self.detach(&mut torrent).await;
        // Nothing of its unpacking writes as its files go.
        if let Some(tee) = torrent.store.as_ref().and_then(|store| store.tee()) {
            tee.stop().await;
        }
        // Nothing holds its files open as they go.
        if let Some(store) = &torrent.store {
            store.forget();
        }
        if delete_files {
            let deleted = tokio::task::spawn_blocking(move || delete_written(&written));
            if let Err(error) = deleted.await {
                tracing::warn!(job_id = job_id.0, error = %error, "could not delete a torrent's files");
            }
        }
        // The job's outcome: complete, short of what unpacking would
        // deliver, refused by unpacking, or cut short.
        let (status, error) = match (torrent.complete(), torrent.refusal()) {
            (true, None) => ("complete", None),
            (true, Some(why)) => {
                let mut delivered = torrent
                    .store
                    .as_ref()
                    .map(|store| store.whole_names())
                    .unwrap_or_default();
                delivered.extend(self.inner.db.torrent_outputs(job_id).unwrap_or_else(|error| {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent unpacked");
                    Vec::new()
                }));
                let status = short_status(&delivered, &why);
                (status.persisted_status(), Some(why))
            }
            (false, Some(refusal)) => ("failed", Some(refusal)),
            (false, None) => ("cancelled", None),
        };
        self.inner.counters.event(TorrentEvent::Removed);
        self.retire(torrent, status, error).await;
        self.queue_script_event(ended);
        Ok(())
    }

    // What a deletion of the torrent's files removes (libtorrent
    // `storage_utils::delete_files`): its listed files Weaver made, what
    // unpacking placed beside them, and its tee's directory, nothing else.
    // A file that was there before the torrent stays; so does a folder
    // that was. On a failed read of what was there, every listed file is.
    fn written_paths(&self, torrent: &Live) -> Deletion {
        let job_id = torrent.record.job_id;
        let Some(store) = &torrent.store else {
            return Deletion::default();
        };
        let adopted = self.inner.db.torrent_adopted(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read what was on disk before a torrent");
            Adopted {
                all_files: true,
                dirs: vec![String::new()],
                ..Adopted::default()
            }
        });
        let outputs = self.inner.db.torrent_outputs(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent unpacked");
            Vec::new()
        });
        // A torrent placed in a shared folder has a folder of its own
        // claimed beside it, and the shared one is never its to remove.
        let claimed = torrent
            .record
            .files_dir
            .as_ref()
            .and(torrent.record.storage_dir.as_deref())
            .map(PathBuf::from);
        let mut deletion = deletion(store.files(), &adopted, &outputs, claimed);
        // With or without a tee: one whose unpacked files were not placed
        // keeps its directory past it.
        deletion.made.push(self.unpacking_dir(job_id));
        deletion
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
                if let Some(tee) = torrent.store.as_ref().and_then(|store| store.tee()) {
                    tee.hold_own_clock(intent == TorrentIntent::Paused);
                }
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
            let listed = &torrent.trackers;
            let trackers = edit(listed.to_vec())?;
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
            torrent.trackers = in_table_order(trackers);
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
            .live
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|torrent| torrent.trackers.iter().any(|tracker| tracker.url == url));
        if !listed {
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
        let stopped = self
            .with_torrent(
                job_id,
                |torrent| Ok(self.seeding_stopped(torrent, "paused")),
            )
            .await?;
        self.set_intent(job_id, TorrentIntent::Paused).await?;
        self.queue_script_event(stopped);
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
        // A resume is the go-ahead to restore a torrent that crashed the
        // daemon. Its marker goes first, so a panic in the restore leaves a
        // fresh one.
        let mark = self
            .inner
            .crash_marks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(job_id);
        if let Some(mark) = mark {
            mark.consume();
            self.with_torrent(job_id, |torrent| {
                torrent.restore_held = None;
                Ok(())
            })
            .await?;
        }
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
        // A start looks again for files that were missing (qBittorrent
        // `TorrentImpl::start`, torrentimpl.cpp:2048-2053).
        let found = self.look_again(job_id).await;
        self.apply_queue().await;
        if found {
            self.attach(job_id, None).await;
        }
        self.publish().await;
        Ok(())
    }

    // A torrent held for its files missing looks at them again, off the
    // runtime, as its store did when it was made. Still not one there, it
    // stays held; otherwise the pieces its files lost are checked or
    // fetched again and it is let go. Whether it was let go, and so may
    // join its session now.
    async fn look_again(&self, job_id: JobId) -> bool {
        let store = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .filter(|torrent| torrent.files_missing)
            .and_then(|torrent| torrent.store.clone());
        let Some(store) = store else {
            return false;
        };
        let looking = Arc::clone(&store);
        let Ok(unbacked) = tokio::task::spawn_blocking(move || looking.drop_unbacked()).await
        else {
            return false;
        };
        let mut live = self.inner.live.lock().await;
        let Some(torrent) = live.get_mut(&job_id).filter(|torrent| {
            torrent.files_missing
                && torrent
                    .store
                    .as_ref()
                    .is_some_and(|held| Arc::ptr_eq(held, &store))
        }) else {
            return false;
        };
        let Some(lost) = unbacked else {
            Note::Absent {
                pieces: store.count(),
            }
            .say(job_id);
            return false;
        };
        if lost > 0 {
            Note::Missing { pieces: lost }.say(job_id);
            self.inner.signal.marked(job_id, lost);
        }
        torrent.files_missing = false;
        torrent.file_have = store.file_have();
        true
    }

    // Every torrent held for its files that its own intent would run looks
    // at them again, RESTORES_AT_ONCE at a time, as a start of every torrent
    // asks (qBittorrent `torrents/start` with `all` calls `TorrentImpl::start`
    // on each). One its own pause stops stays as it is, as the start leaves
    // it. Those let go join their sessions once every look is done.
    async fn look_again_held(&self) {
        let held: Vec<JobId> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter(|torrent| torrent.files_missing && torrent.resumed())
            .map(|torrent| torrent.record.job_id)
            .collect();
        if held.is_empty() {
            return;
        }
        let mut looking = tokio::task::JoinSet::new();
        let mut found = Vec::new();
        for job_id in held {
            if looking.len() >= RESTORES_AT_ONCE
                && let Some(Ok(Some(job_id))) = looking.join_next().await
            {
                found.push(job_id);
            }
            let service = self.clone();
            looking.spawn(async move { service.look_again(job_id).await.then_some(job_id) });
        }
        while let Some(looked) = looking.join_next().await {
            if let Ok(Some(job_id)) = looked {
                found.push(job_id);
            }
        }
        if found.is_empty() {
            return;
        }
        self.apply_queue().await;
        for job_id in found {
            self.attach(job_id, None).await;
        }
        self.publish().await;
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
    /// libtorrent `torrent::move_storage`). The move holds off every read
    /// and write of its files and happens whole; the torrent shows as
    /// moving until it did.
    /// A directory of the torrent's name is claimed under `parent`, so
    /// nothing there is replaced or kept. Files placed outside the
    /// torrent's directory move straight into `parent` instead, where they
    /// sat with no folder of their own; a file of theirs already there
    /// stays, and the torrent is checked again from there. A move that
    /// fails leaves the torrent where it was, and says why when it failed
    /// at once.
    pub async fn set_location(&self, job_id: JobId, parent: String) -> Result<(), ActionError> {
        self.move_storage(job_id, parent, Existing::Keep).await
    }

    // The move of `set_location`. A placed file already at the new place
    // stays, its would-be replacement stays where it was, and the torrent
    // is checked again from the new place, as qBittorrent's move of a
    // user's asks for (`TorrentImpl::moveStorage`, `MoveStorageMode::
    // KeepExistingFiles`, which is libtorrent's `move_flags_t::dont_replace`;
    // libtorrent `torrent::on_storage_moved` rechecks on `need_full_check`).
    // `Existing::Replace` gives way instead, as its move of a finished
    // torrent under its save path does (`TorrentImpl::adjustStorageLocation`,
    // `MoveStorageMode::Overwrite`).
    async fn move_storage(
        &self,
        job_id: JobId,
        parent: String,
        existing: Existing,
    ) -> Result<(), ActionError> {
        let parent = PathBuf::from(parent.trim());
        if !parent.is_absolute() {
            return Err(ActionError::Invalid(
                "the location must be an absolute path".to_owned(),
            ));
        }
        let (store, from, placed) = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() {
                    return Err(ActionError::Invalid(
                        "the torrent is already moving".to_owned(),
                    ));
                }
                match (torrent.store.clone(), torrent.record.storage_dir.clone()) {
                    (Some(store), Some(dir)) => Ok((
                        store,
                        PathBuf::from(dir),
                        torrent.record.files_dir.clone().map(PathBuf::from),
                    )),
                    _ => Err(ActionError::Invalid(
                        "the torrent has no files to move yet".to_owned(),
                    )),
                }
            })
            .await?;
        if let Some(placed) = placed {
            return self
                .move_placed(job_id, store, &placed, parent, existing)
                .await;
        }
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
                torrent.moving = Some(Moving::Dir(to.clone()));
                Ok(())
            })
            .await;
        if let Err(error) = started {
            give_back(&to.to_string_lossy()).await;
            return Err(error);
        }
        let outcome = tokio::task::spawn_blocking(move || {
            store.files().relocate(|dir| {
                relocate::move_directory(dir, &to)?;
                Ok((to.clone(), (to, false)))
            })
        })
        .await
        .unwrap_or_else(|error| {
            Err(MoveError {
                path: from,
                source: std::io::Error::other(error),
            })
        });
        let moved = self.moved(job_id, outcome).await;
        self.publish().await;
        moved
    }

    // Move a torrent's files placed in `from`, outside its directory, to
    // `to` (libtorrent `torrent::move_storage` moves every file of a
    // torrent from its save path to the new one), with what unpacking
    // placed beside them. The directory claimed for it stays where it was.
    async fn move_placed(
        &self,
        job_id: JobId,
        store: Arc<TorrentStore>,
        from: &Path,
        to: PathBuf,
        existing: Existing,
    ) -> Result<(), ActionError> {
        if from == to {
            return Ok(());
        }
        // qBittorrent `TorrentsController::setLocationAction` makes the
        // location first, and answers a conflict when it cannot.
        tokio::fs::create_dir_all(&to).await.map_err(|error| {
            ActionError::Invalid(format!("could not make the torrent's directory: {error}"))
        })?;
        self.with_torrent(job_id, |torrent| {
            if torrent.moving.is_some() {
                return Err(ActionError::Invalid(
                    "the torrent is already moving".to_owned(),
                ));
            }
            torrent.settle(self.inner.clock.now_ms());
            torrent.moving = Some(Moving::Files);
            Ok(())
        })
        .await?;
        let outputs = self.inner.db.torrent_outputs(job_id).unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent unpacked");
            Vec::new()
        });
        let from = from.to_path_buf();
        let outcome =
            tokio::task::spawn_blocking(move || move_listed(store.files(), &outputs, to, existing))
                .await
                .unwrap_or_else(|error| {
                    Err(MoveError {
                        path: from,
                        source: std::io::Error::other(error),
                    })
                });
        let moved = self.moved(job_id, outcome).await;
        self.publish().await;
        moved
    }

    /// Check every piece of a torrent again from what the job holds, trusting
    /// nothing saved (libtorrent `torrent::force_recheck`, qBittorrent's
    /// `torrents/recheck`). A running torrent runs again once checked, a
    /// paused one stays paused, and one being checked is left to its check.
    pub async fn recheck(&self, job_id: JobId) -> Result<(), ActionError> {
        // One held for its files missing is in no session: it lets go of
        // the hold and of every piece, and the session it joins checks
        // them all (qBittorrent `TorrentImpl::forceRecheck` clears its
        // missing files, torrentimpl.cpp:1736-1744).
        let held = self
            .with_torrent(job_id, |torrent| {
                if !torrent.files_missing {
                    return Ok(false);
                }
                if let Some(store) = &torrent.store {
                    Pieces::checking(&**store)
                        .map_err(|error| ActionError::State(format!("{error:#}")))?;
                    torrent.file_have = store.file_have();
                }
                torrent.files_missing = false;
                Ok(true)
            })
            .await?;
        if held {
            self.apply_queue().await;
            self.attach(job_id, None).await;
            self.publish().await;
            return Ok(());
        }
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

    // The torrent's files moved, or could not: the record follows a move,
    // and a failed one gives its claim back and leaves the torrent where it
    // was. Files placed outside its directory take their save path and
    // their directory with them; a file already where one of them went was
    // kept, which the outcome says. A failed move says why.
    async fn moved(
        &self,
        job_id: JobId,
        outcome: Result<(PathBuf, bool), MoveError>,
    ) -> Result<(), ActionError> {
        let (moving, store) = self
            .with_torrent(job_id, |torrent| {
                torrent.settle(self.inner.clock.now_ms());
                Ok((torrent.moving.take(), torrent.store.clone()))
            })
            .await
            .unwrap_or((None, None));
        // An unpacking that waited on the move places its files now.
        self.inner.signal.mark(job_id);
        match outcome {
            Ok((to, kept_existing)) => {
                let to = to.to_string_lossy().into_owned();
                let files = matches!(moving, Some(Moving::Files));
                // Kept on its row before the torrent shows it, under the
                // same hold, so nothing that sees the new place finds the
                // row behind.
                let recorded = self
                    .with_torrent(job_id, |torrent| {
                        let recorded = if files {
                            self.inner
                                .db
                                .set_torrent_files_dir(job_id, Some(&to))
                                .and_then(|_| {
                                    self.inner.db.set_torrent_save_path(job_id, Some(&to))
                                })
                        } else {
                            self.inner.db.set_torrent_storage_dir(job_id, &to)
                        };
                        if files {
                            torrent.record.files_dir = Some(to.clone());
                            torrent.record.save_path = Some(to.clone());
                        } else {
                            torrent.record.storage_dir = Some(to.clone());
                        }
                        Ok(recorded)
                    })
                    .await;
                if let Ok(Err(error)) = recorded {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's new directory");
                }
                if files
                    && kept_existing
                    && let Some(store) = &store
                {
                    self.check_kept(job_id, store).await;
                }
                // A move looks again for files that were missing
                // (qBittorrent `TorrentImpl::handleMoveStorageJobFinished`,
                // torrentimpl.cpp:2140-2146).
                if self.look_again(job_id).await {
                    self.apply_queue().await;
                    self.attach(job_id, None).await;
                }
                Ok(())
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's move failed; it stays where it was");
                if let Some(Moving::Dir(claimed)) = moving {
                    give_back(&claimed.to_string_lossy()).await;
                }
                Err(ActionError::State(format!(
                    "the torrent's move failed, and it stays where it was: {error}"
                )))
            }
        }
    }

    // A move kept a file already at the torrent's new place, so its files
    // there are not all its own: every piece is checked again (libtorrent
    // `torrent::on_storage_moved` rechecks on `need_full_check`). A torrent
    // in no session forgets its pieces instead, and the session it next
    // joins checks them, as it checks any torrent whose store vouches for
    // nothing.
    async fn check_kept(&self, job_id: JobId, store: &Arc<TorrentStore>) {
        let attached = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|torrent| torrent.attached.is_some());
        let checked = if attached {
            self.recheck(job_id).await.map_err(anyhow::Error::from)
        } else {
            Pieces::checking(&**store).map(|_| ())
        };
        if let Err(error) = checked {
            tracing::warn!(job_id = job_id.0, error = %error, "could not check a torrent again after its move kept files there");
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
        if let Some(policy) = &policy {
            policy.validate().map_err(ActionError::Invalid)?;
        }
        let policy = SeedingPolicy::kept(policy);
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

    /// A queue action on a torrent. A move changes its place in the
    /// download queue; the top priority forces it past the queue, and any
    /// other leaves it to the queue.
    pub async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), ActionError> {
        match action {
            QueueAction::Pause => self.pause(job_id).await,
            QueueAction::Resume => self.resume(job_id).await,
            QueueAction::Cancel(origin) => {
                let delete_files = origin != CancellationOrigin::UserKeepingFiles;
                self.remove(job_id, delete_files, false).await
            }
            QueueAction::Update(update) => self.update(job_id, update).await,
            QueueAction::Move(target) => self.reorder(&[job_id], target).await,
        }
    }

    /// Moves `job_ids` in the download queue as qBittorrent's queue actions
    /// do (`SessionImpl::increaseTorrentsQueuePos` and the others): of the
    /// torrents named, those still downloading, taken in queue order, each
    /// move one place up for a negative offset, starting with the first, or
    /// one down for a positive one, starting with the last; to the top
    /// starting with the last, so they keep their order there; to the bottom
    /// starting with the first. A seed has no place to move (libtorrent
    /// `torrent::finished` takes it out of the queue). Nothing moves a
    /// torrent past the queue: forcing does that.
    pub async fn reorder(
        &self,
        job_ids: &[JobId],
        target: QueueMoveTarget,
    ) -> Result<(), ActionError> {
        {
            let mut live = self.inner.live.lock().await;
            if let Some(&missing) = job_ids.iter().find(|job_id| !live.contains_key(job_id)) {
                return Err(ActionError::NotFound(missing));
            }
            let mut order: Vec<&Live> =
                live.values().filter(|torrent| !torrent.seeding()).collect();
            order.sort_by_key(|torrent| (torrent.record.queue_position, torrent.record.job_id));
            let mut order: Vec<JobId> = order.iter().map(|torrent| torrent.record.job_id).collect();
            let named: Vec<JobId> = order
                .iter()
                .copied()
                .filter(|job_id| job_ids.contains(job_id))
                .collect();
            let first_first = match target {
                QueueMoveTarget::Top => false,
                QueueMoveTarget::Bottom => true,
                QueueMoveTarget::Offset(offset) => offset < 0,
            };
            let moving: Box<dyn Iterator<Item = &JobId>> = if first_first {
                Box::new(named.iter())
            } else {
                Box::new(named.iter().rev())
            };
            for &job_id in moving {
                // Named and still downloading, so in the order.
                let _ = crate::jobs::handle::splice_job_order(&mut order, job_id, target);
            }
            // The torrents still downloading take the places they held among
            // themselves, in their new order; a seed keeps its own.
            let mut places: Vec<i64> = order
                .iter()
                .map(|job_id| live[job_id].record.queue_position)
                .collect();
            places.sort_unstable();
            let moved: Vec<(JobId, i64)> = order
                .iter()
                .zip(places)
                .filter(|(job_id, place)| live[*job_id].record.queue_position != *place)
                .map(|(job_id, place)| (*job_id, place))
                .collect();
            if moved.is_empty() {
                return Ok(());
            }
            self.inner
                .db
                .set_torrent_queue_positions(&moved)
                .map_err(|error| ActionError::State(state_error(error)))?;
            for (job_id, place) in moved {
                if let Some(torrent) = live.get_mut(&job_id) {
                    torrent.record.queue_position = place;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
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
        // Each table a restore and a first attach read, read once for every
        // torrent. A torrent kept with no tracker list would write its next
        // edit over the rows it has, and one kept with no ledger would write
        // zeros over its counts at its next save, so those reads, like the
        // records', fail the restore when they fail. A history that does
        // not read is said, and no torrent shows its failure. One
        // torrent's part that does not read is said, and that torrent goes
        // on without it: its pieces and seed-mode pieces are then read
        // again, each by the one it is for.
        let records = self.inner.db.restored_torrents().map_err(state_error)?;
        let mut trackers = self
            .inner
            .db
            .torrent_trackers_by_job()
            .map_err(state_error)?;
        let mut counters = self
            .inner
            .db
            .torrent_counters_by_job()
            .map_err(state_error)?;
        let mut failures = self
            .inner
            .db
            .failed_torrent_histories()
            .unwrap_or_else(|error| {
                tracing::warn!(error = %error, "could not read the torrents' history");
                BTreeMap::new()
            });
        let now = self.inner.clock.now_ms();
        if let Some(last) = records.iter().map(|row| row.record.queue_position).max() {
            self.inner
                .next_queue_position
                .fetch_max(last + 1, Ordering::Relaxed);
        }
        for row in records {
            let record = row.record;
            let job_id = record.job_id;
            let (ledger, resume, kept) = match counters.remove(&job_id) {
                Some(stored) => (
                    stored.counters.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's ledger");
                        TorrentCounters::default()
                    }),
                    stored.peers.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's saved peers");
                        ResumePeers::default()
                    }),
                    stored.kept.map_err(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's pieces");
                    }).ok(),
                ),
                None => (
                    TorrentCounters::default(),
                    ResumePeers::default(),
                    Some(None),
                ),
            };
            // The rule is the one its trackers match now (libtorrent keeps
            // the trackers, not what was made of them).
            let mut torrent = Live::new(record.clone(), ledger, resume, now);
            torrent.trackers = in_table_order(trackers.remove(&job_id).unwrap_or_default());
            torrent.rule_id = self.rule_id(torrent.trackers.iter());
            match record.metainfo.as_deref().map(parse) {
                Some(Ok(parsed)) => {
                    let levels = row.levels.unwrap_or_else(|error| {
                        tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's file levels");
                        Vec::new()
                    });
                    self.describe(&mut torrent, &parsed, levels);
                    torrent.kept(
                        failures.remove(&job_id),
                        kept.as_ref().and_then(Option::as_ref),
                    );
                    torrent.kept_at_boot = kept;
                    if torrent.record.seed_mode {
                        torrent.verified_at_boot = row
                            .seed_verified
                            .map_err(|error| {
                                tracing::warn!(job_id = job_id.0, error = %error, "could not read the pieces a torrent checked in seed mode");
                            })
                            .ok();
                    }
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
        order.sort_by_key(|torrent| (torrent.record.queue_position, torrent.record.job_id));
        let queue: Vec<Queued> = order
            .iter()
            .map(|torrent| queued(&settings, &rules, torrent, now))
            .collect();
        let last = order
            .last()
            .map(|torrent| (torrent.record.queue_position, torrent.record.job_id));
        let (turns, slots) = status::turns(&queue, settings.torrent.max_active_downloads, &holds);
        let turns: BTreeMap<JobId, (&Queued, status::Turn)> = queue
            .iter()
            .zip(turns)
            .map(|(queued, turn)| (queued.job_id, (queued, turn)))
            .collect();
        *self
            .inner
            .queue_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = QueueEnd { slots, last };
        for torrent in live.values_mut() {
            let (queued, turn) = turns[&torrent.record.job_id];
            take_turn(torrent, queued, turn, &holds, now);
            let run = turn.active && torrent.resumed();
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
        // One plan per scope a pass: the torrents of a scope share it.
        let mut plans: HashMap<TorrentScope, RoutePlan> = HashMap::new();
        {
            let mut live = self.inner.live.lock().await;
            // A torrent placed on a pool member this pass counts there for
            // the next one placed.
            let mut load = load_of(live.values());
            for torrent in live.values_mut() {
                let rule = torrent.rule(&rules);
                let scope = settings.scope(torrent.record.category_id, rule);
                let (plan, pin) = plans
                    .entry(scope)
                    .or_insert_with(|| self.inner.sessions.routes().plan(scope))
                    .place(torrent.record.pool_member, |key| {
                        load.get(key).copied().unwrap_or(0)
                    });
                self.pin(torrent, pin);
                if torrent.route.as_ref() != Some(&plan.key) {
                    if let Some(count) = torrent.route.as_ref().and_then(|key| load.get_mut(key)) {
                        *count = count.saturating_sub(1);
                    }
                    *load.entry(plan.key.clone()).or_default() += 1;
                }
                if torrent.layout.is_none() {
                    let moved = matches!(torrent.resolving, Some(Resolving::Fetching(_)))
                        && (plan.down.is_some()
                            || torrent.route.as_ref() != Some(&plan.key)
                            || outdated.contains(&plan.key));
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
                let terms = PeerTerms::of(&settings, rule);
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
                    // A torrent placed on another route reads as that route
                    // from here, opening, while the torrents before it take
                    // their turn. One whose route's session could not be
                    // built reads as that cause while the route opens
                    // again; any other hold is over.
                    if torrent.route.as_ref() != Some(&plan.key) {
                        torrent.route = Some(plan.key.clone());
                        torrent.held = Some(Held::Session(OPENING));
                    } else if !matches!(torrent.held, Some(Held::Session(_))) {
                        torrent.held = None;
                    }
                    reattach.push(torrent.record.job_id);
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
                // Seed mode ended in the session: from here what the job has
                // is what the torrent has, after a restart too.
                if torrent.record.seed_mode
                    && let Some(attached) = &torrent.attached
                    && !attached.handle.seed_mode()
                {
                    torrent.record.seed_mode = false;
                    torrent.seed_verified = 0;
                    // Seeding no more, it may be queued again.
                    self.inner.signal.want_pass();
                    if let Err(error) = self.inner.db.clear_torrent_seed_mode(torrent.record.job_id)
                    {
                        tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not record that a torrent left seed mode");
                    }
                }
                // Still in it: the pieces that passed since they were last
                // kept are kept, so a restart serves them without a second
                // check (libtorrent saves `verified_pieces` in resume data).
                if torrent.record.seed_mode
                    && let Some(attached) = &torrent.attached
                    && let Some(verified) = attached.handle.seed_verified()
                    && verified.count_ones() > torrent.seed_verified
                {
                    let count = verified.count_ones();
                    match self
                        .inner
                        .db
                        .set_torrent_seed_verified(torrent.record.job_id, verified.as_raw_slice())
                    {
                        Ok(_) => torrent.seed_verified = count,
                        Err(error) => {
                            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not keep the pieces a torrent checked in seed mode");
                        }
                    }
                }
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
                let policy = settings.policy(&torrent.record, torrent.rule(&rules));
                if status::goal_left(&policy, &torrent.counters, torrent.total, now).is_some() {
                    continue;
                }
                let finished = self.script_event(
                    torrent,
                    QueueEvent::SeedingFinished,
                    &[(vars::SEEDING_ACTION, policy.action.as_str())],
                );
                match policy.action {
                    SeedingAction::Stop => {
                        landings.extend(self.stop_torrent(torrent, now));
                        self.queue_script_event(finished);
                    }
                    SeedingAction::Remove => {
                        actions.push((torrent.record.job_id, false, finished));
                    }
                    SeedingAction::RemoveWithData => {
                        actions.push((torrent.record.job_id, true, finished));
                    }
                }
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
        // A removed torrent's event is queued once its job is in history,
        // where the queue finds it.
        for (job_id, delete_files, finished) in actions {
            match self
                .remove_as(job_id, delete_files, true, Ending::Goal)
                .await
            {
                Ok(()) => self.queue_script_event(finished),
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "a torrent that met its goal was not removed");
                }
            }
        }
    }

    // What each session runs is what is stored: the levels the operator
    // set for the files, sequential and first-and-last, each read from the torrent's handle and
    // pushed only where it differs. A change the session refuses (while it
    // checks its files) is pushed again on the next pass. A full disk
    // leaves the levels as they are: the session stops fetching in upload
    // mode, not by lower levels.
    async fn apply_picking(&self) {
        self.pick(None).await;
    }

    // Picking for the torrents in `only`, or for every one.
    async fn pick(&self, only: Option<&BTreeSet<JobId>>) {
        let live = self.inner.live.lock().await;
        let torrents: Box<dyn Iterator<Item = &Live> + Send> = match only {
            Some(only) => Box::new(only.iter().filter_map(|job_id| live.get(job_id))),
            None => Box::new(live.values()),
        };
        for torrent in torrents {
            let (Some(attached), Some(_)) = (&torrent.attached, &torrent.store) else {
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
            let wanted = fetch_levels(torrent);
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
            let limits = ratelimits_of(&torrent.record, settings.upload_cap(&torrent.record, rule));
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
            // A pool route is listed as each member's session.
            let plan = sessions.routes().plan(scope);
            let planned: Vec<_> = if plan.members.is_empty() {
                vec![(plan.key, plan.down, plan.names)]
            } else {
                plan.members
                    .into_iter()
                    .map(|member| (member.key, member.down, member.names))
                    .collect()
            };
            for (key, down, names) in planned {
                match listed.iter_mut().find(|route| route.key == key) {
                    Some(route) => route.scopes.push((scope, name.clone())),
                    None => listed.push(TorrentRoute {
                        key,
                        scopes: vec![(scope, name.clone())],
                        session: None,
                        down: sessions.off().or(down),
                        names,
                    }),
                }
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
            .map(|(scope, _)| routes.plan(scope))
            .flat_map(|plan| plan.keys().cloned().collect::<Vec<_>>())
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
        let (policy, seeding_levels) = settings.seeding(record, rule);
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
            check_progress: reading.checked.filter(|_| {
                matches!(
                    state,
                    TorrentState::CheckingFiles | TorrentState::CheckingResumeData
                )
            }),
            total_bytes: total,
            have_bytes: have,
            wanted_bytes: wanted,
            wanted_have_bytes: wanted_have,
            uploaded_bytes: counters.uploaded_bytes,
            downloaded_bytes: counters.downloaded_bytes,
            run_uploaded_bytes: reading.run.1,
            run_downloaded_bytes: reading.run.0,
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
            seeding_seconds: counters.finished_duration_s,
            consecutive_seeding_seconds: counters.consecutive_seeding_s,
            download_avg_bps: status::average_bps(counters.downloaded_bytes, active_seconds),
            upload_avg_bps: status::average_bps(counters.uploaded_bytes, active_seconds),
            wasted_bytes: counters.wasted_bytes(),
            last_seen_complete_ms: counters.last_seen_complete_ms,
            last_activity_ms: counters.last_activity_ms,
            pieces: torrent.pieces(),
            created_by: torrent.about.created_by.clone(),
            created_at_ms: torrent.about.created_at_ms,
            comment: torrent.about.comment.clone(),
            added_at_ms: record.added_at_ms,
            completed_at_ms: record.completed_at_ms,
            storage_dir: record.storage_dir.clone(),
            files_dir: record.files_dir.clone(),
            queue_position: torrent.queue_rank,
            seeding_left: status::goal_left(&policy, counters, total, now),
            seeding_override: record.policy_override,
            seeding_policy: policy,
            seeding_levels,
            tracker_rule: rule.map(|rule| rule.host_pattern.clone()),
            upload_cap: settings.upload_cap(record, rule),
            unpack: record
                .unpack_in_force
                .unwrap_or_else(|| settings.unpack(record, rule)),
            unpack_override: record.unpack,
            scope: settings.scope(record.category_id, rule),
            route: torrent.route.as_ref().map(|key| key.as_str().to_owned()),
            files: Vec::new(),
            sequential: record.sequential,
            first_last_piece_first: record.first_last_piece_first,
            file_have: reading.file_have,
            trackers: Vec::new(),
            metadata: record.metadata.clone(),
            tags: Vec::new(),
        }
    }

    // Each view's tags, from the tags as last read. A view whose tags cannot
    // be read shows none.
    fn tag(&self, views: &mut [&mut TorrentView]) {
        match self.inner.db.tag_snapshot() {
            Ok(tags) => {
                for view in views {
                    view.tags = tags.of(view.job_id).to_vec();
                }
            }
            Err(error) => tracing::warn!(error = %error, "could not read the tags"),
        }
    }

    /// One torrent, with its files and trackers.
    pub async fn torrent(&self, job_id: JobId) -> Option<TorrentView> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (mut view, announces, pinned, listed) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            let mut view = self.view(&settings, torrent, now);
            view.files = torrent.files();
            (
                view,
                torrent.announces(),
                torrent.record.pool_member,
                torrent.trackers.clone(),
            )
        };
        self.track(&mut view, pinned, &listed, &announces, &mut HashMap::new());
        self.tag(&mut [&mut view]);
        Some(view)
    }

    /// What a support report reads of one torrent: its view, its metainfo,
    /// and the held figures the view folds together. One pass over the
    /// torrent under the lock its view takes; its peers are read after,
    /// from its session.
    pub async fn support(&self, job_id: JobId) -> Option<TorrentSupport> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (mut support, announces, handle, pinned, listed) = {
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
                has_job: torrent.store.is_some(),
                kept_pieces: torrent.kept_pieces,
                wanted_bytes,
                wanted_have_bytes,
                metrics: TorrentMetrics::default(),
            };
            (
                support,
                torrent.announces(),
                attached.map(|attached| Arc::clone(&attached.handle)),
                torrent.record.pool_member,
                torrent.trackers.clone(),
            )
        };
        self.track(
            &mut support.view,
            pinned,
            &listed,
            &announces,
            &mut HashMap::new(),
        );
        self.tag(&mut [&mut support.view]);
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
        self.listed(category, false, now)
            .await
            .into_iter()
            .map(|(view, _)| view)
            .collect()
    }

    /// Every torrent with its files, from one pass over them: what
    /// `torrent` gives one at a time.
    pub async fn torrents_with_files(&self) -> Vec<TorrentView> {
        let now = self.inner.clock.now_ms();
        self.listed(None, true, now)
            .await
            .into_iter()
            .map(|(view, _)| view)
            .collect()
    }

    // Every torrent, or a category's, in the order they were added, each
    // with its trackers, and its figures as the exporter reads them: both
    // from one reading of its session.
    async fn listed(
        &self,
        category: Option<&str>,
        files: bool,
        now: i64,
    ) -> Vec<(TorrentView, TorrentSeries)> {
        let shown = if files { Shown::WHOLE } else { Shown::LISTED };
        let pick = Pick {
            category: category.map(CategoryPick::Named),
            ..Pick::default()
        };
        self.listed_by(shown, true, false, now, |settings, live| {
            pick.ids(settings, live)
        })
        .await
        .into_iter()
        .map(|(view, series, _)| (view, series.expect("asked for")))
        .collect()
    }

    // The torrents `pick` names, in the order it names them, each with what
    // `shown` asks for, with its figures as the exporter reads them when
    // `series` asks for those, both from one reading of its session, and
    // with the totals of its files when `totals` asks for those. Only the
    // torrents picked are read.
    async fn listed_by(
        &self,
        shown: Shown,
        series: bool,
        totals: bool,
        now: i64,
        pick: impl FnOnce(&TorrentSettings, &BTreeMap<JobId, Live>) -> Vec<JobId>,
    ) -> Vec<(TorrentView, Option<TorrentSeries>, Option<FileTotals>)> {
        // A torrent's figures name its trackers.
        let shown = Shown {
            trackers: shown.trackers || series,
            ..shown
        };
        let settings = self.settings();
        // Under the lock, each torrent's files are only taken by reference
        // with its levels: the list is built after the lock is let go.
        type Files = Option<(Option<Arc<PieceLayout>>, Vec<FilePriority>)>;
        // With its pool member, which names its trackers' lookups, and the
        // trackers it lists.
        type Listed = (
            TorrentView,
            Vec<TrackerAnnounce>,
            Option<Unviewed>,
            Files,
            Option<u32>,
            Option<Arc<[TrackerEntry]>>,
        );
        let mut views: Vec<Listed> = {
            let live = self.inner.live.lock().await;
            pick(&settings, &live)
                .iter()
                .filter_map(|job_id| live.get(job_id))
                .map(|torrent| {
                    let reading = torrent.reading(now);
                    let unviewed = series.then(|| self.unviewed(&settings, torrent, &reading, now));
                    let view = self.view_of(&settings, torrent, reading, now);
                    let files = (shown.files || totals)
                        .then(|| (torrent.layout.clone(), torrent.levels.clone()));
                    (
                        view,
                        if shown.trackers {
                            torrent.announces()
                        } else {
                            Vec::new()
                        },
                        unviewed,
                        files,
                        torrent.record.pool_member,
                        shown.trackers.then(|| torrent.trackers.clone()),
                    )
                })
                .collect()
        };
        let mut names = HashMap::new();
        let mut file_totals = Vec::with_capacity(if totals { views.len() } else { 0 });
        for (view, announces, _, files, pinned, listed) in &mut views {
            if let Some((layout, levels)) = files.take() {
                if totals {
                    file_totals.push(FileTotals::of_layout(
                        &view.name,
                        layout.as_deref(),
                        &levels,
                        &view.file_have,
                    ));
                }
                if shown.files {
                    view.files = files_of(layout.as_deref(), &levels);
                }
            }
            if let Some(listed) = listed {
                self.track(view, *pinned, listed, announces, &mut names);
            }
        }
        if shown.tags {
            self.tag(&mut views.iter_mut().map(|(view, ..)| view).collect::<Vec<_>>());
        }
        let mut file_totals = file_totals.into_iter();
        views
            .into_iter()
            .map(|(view, announces, unviewed, ..)| {
                let series = unviewed.map(|unviewed| unviewed.series(&view, &announces));
                (view, series, file_totals.next())
            })
            .collect()
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
            hash_failed_bytes: reading.counters.failed_bytes,
            redundant_bytes: reading.counters.redundant_bytes,
            seeding_seconds: reading.counters.finished_duration_s,
            consecutive_seeding_seconds: reading.counters.consecutive_seeding_s,
            peers_connecting: reading.peers.2,
            leg: torrent
                .attached
                .as_ref()
                .and_then(|attached| attached.leg.as_ref())
                .map(|leg| leg.tenancy.leg().clone()),
            fetch: torrent.fetching.as_ref().map(|fetch| *fetch.phase.borrow()),
            seeding: SeedingSeries::of(&policy, &reading.counters, torrent.total, now),
        }
    }

    // A view's trackers: those listed for it, as its run announces to
    // them, with who resolves their names on its route, on a pool route its
    // member `pinned`. `names` holds each scope's and member's planned once.
    fn track(
        &self,
        view: &mut TorrentView,
        pinned: Option<u32>,
        listed: &[TrackerEntry],
        announces: &[TrackerAnnounce],
        names: &mut HashMap<(TorrentScope, Option<u32>), Option<ResolvedBy>>,
    ) {
        let names = *names.entry((view.scope, pinned)).or_insert_with(|| {
            self.inner
                .sessions
                .routes()
                .plan(view.scope)
                .names_for(pinned)
        });
        view.trackers = status::trackers(listed.to_vec(), announces, names);
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
        let plan = self.inner.sessions.routes().plan(scope);
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
    fn sample(&self) -> u64 {
        self.0
            .upgrade()
            .map_or(0, |inner| inner.sessions.rates().sample())
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

// Resolves once `reloads` rings after it was last seen; never when nothing
// reloads the routes, or once what did is gone.
async fn reloaded(reloads: &mut Option<watch::Receiver<u64>>) {
    if let Some(receiver) = reloads.as_mut()
        && receiver.changed().await.is_ok()
    {
        return;
    }
    *reloads = None;
    std::future::pending().await
}

// Whether the torrent's files went where its client asked, where they may
// be already.
fn placed(record: &TorrentRecord) -> bool {
    record.save_path.is_some() || record.files_dir.is_some()
}

// A seed-mode torrent's pieces are had from where its files were placed,
// piece by piece on a blocking thread, while its session seeds them, and
// what its tee waits on goes to it from there: the store comes to have what
// the session took as had. The session leaving seed mode
// stops it before any check of every piece begins, and so does the torrent
// leaving its session. A piece is read from the files outside seed mode's
// hold, so a peer's request never waits on that read.
fn adopt_placed(
    handle: &Arc<ManagedTorrent>,
    store: &Arc<TorrentStore>,
    job_id: JobId,
    signal: Arc<Signal>,
    stopping: impl Future<Output = ()> + Send + 'static,
) {
    let handle = Arc::downgrade(handle);
    let store = Arc::downgrade(store);
    tokio::spawn(async move {
        let adopting = (handle.clone(), store.clone());
        let adopted = tokio::task::spawn_blocking(move || {
            let (handle, store) = adopting;
            let count = store.upgrade()?.layout().piece_count();
            for piece in 0..count {
                let (Some(handle), Some(store)) = (handle.upgrade(), store.upgrade()) else {
                    return None;
                };
                if !handle.seed_mode() {
                    return None;
                }
                match store.adopt(piece, |take| handle.while_seeding(take)) {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        tracing::warn!(piece, error = %format!("{error:#}"), "a seed-mode torrent's placed files could not be read");
                        return None;
                    }
                    None => return None,
                }
            }
            Some(count)
        })
        .await;
        let Ok(Some(count)) = adopted else {
            return;
        };
        // The pieces the tee waits on are checked now, as a peer's request
        // would check them, and go to it as they pass: the graph takes no
        // byte a piece check has not passed. One that fails ends seed mode,
        // and the check of every piece that follows gives the tee the
        // pieces it finds and the session fetches the rest. The session
        // takes the torrent through its first check on its own task, and a
        // piece is checked only once that has ended, the torrent running or
        // paused; one that ends in an error has its pieces checked as peers
        // ask. The wait holds the torrent only while its session runs: a
        // session that stops leaves a torrent in its first check as it is,
        // and this ends with it, the torrent going with its session. One
        // removed from its session ends its first check, and the wait.
        let checks = match (handle.upgrade(), store.upgrade()) {
            (Some(torrent), Some(placed)) if placed.tee().is_some() => {
                drop(placed);
                tokio::select! {
                    started = torrent.wait_until_initialized() => started.is_ok(),
                    () = stopping => return,
                }
            }
            _ => false,
        };
        if checks {
            let checking = tokio::task::spawn_blocking(move || {
                let (Some(handle), Some(store)) = (handle.upgrade(), store.upgrade()) else {
                    return;
                };
                for piece in 0..count {
                    if !store.tee_wants(piece) {
                        continue;
                    }
                    match handle.check_seeded(piece) {
                        Some(Ok(true)) => {
                            if let Err(error) = store.replay(piece) {
                                tracing::warn!(piece, error = %error, "a seed-mode torrent's checked piece could not be read");
                                break;
                            }
                        }
                        Some(Ok(false)) | None => break,
                        Some(Err(error)) => {
                            tracing::warn!(piece, error = %format!("{error:#}"), "a seed-mode torrent's piece could not be checked");
                            break;
                        }
                    }
                }
            });
            if checking.await.is_err() {
                return;
            }
        }
        // The files were whole as placed: the next settle completes or
        // unpacks the torrent, shows it seeding, and saves the pieces it
        // adopted, as qBittorrent saves a torrent's resume data once it is
        // added, rather than leaving every add's to one tick. Adds that come
        // together share one settle.
        signal.due(job_id);
    });
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
        // Read back from what was saved: a check, as qBittorrent's filter
        // counts it (torrentfilter.cpp:190).
        TorrentState::CheckingResumeData => JobStatus::CheckingFiles,
        TorrentState::Downloading => JobStatus::Downloading,
        TorrentState::Stalled => JobStatus::Stalled,
        TorrentState::Seeding => JobStatus::Seeding,
        TorrentState::Paused => JobStatus::Paused,
        TorrentState::Moving => JobStatus::Moving,
        // It waits for a certificate, held where it is.
        TorrentState::NeedsCertificate => JobStatus::Stalled,
        // Held stopped until the operator starts it again.
        TorrentState::FilesMissing => JobStatus::Paused,
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

// The levels the session fetches a torrent's files at now: the operator's.
fn fetch_levels(torrent: &Live) -> Vec<FilePriority> {
    torrent.levels.clone()
}

// What a torrent's move takes.
#[derive(Debug, Clone)]
enum Moving {
    // Its directory, to the directory claimed for it.
    Dir(PathBuf),
    // Its files placed outside its directory.
    Files,
    // What its unpacking made, going beside its files.
    Outputs,
}

// Why a torrent has no store: one that cannot be made fails the torrent,
// and one whose checkpoint the database did not give waits to be tried
// again.
enum Unmade<W> {
    Fails(String),
    Waits(W),
}

// What a store makes of a database read that failed, or of a checkpoint
// whose rebuild failed writing or stepping: a restore waits on it (`Ok`),
// as a usenet job's restore is held; a torrent given its metainfo now has
// nothing saved to wait for (`Err`, with `W` uninhabited), and goes on.
type Hold<W> = fn(String) -> Result<W, String>;

// What a torrent's store made of what it found: why its tee starts fresh
// though it had a checkpoint, and the pieces its files no longer gave.
// Said where the restore is awaited, so the line goes where the caller's
// lines go.
enum Note {
    Unread(String),
    Unrestored(String),
    Dropped { pieces: u32, error: String },
    Missing { pieces: u32 },
    Absent { pieces: u32 },
}

impl Note {
    // Whether `notes` found none of the torrent's files with a piece had.
    fn absent(notes: &[Self]) -> bool {
        notes.iter().any(|note| matches!(note, Self::Absent { .. }))
    }

    fn say(&self, job_id: JobId) {
        match self {
            Self::Unread(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking checkpoint could not be read; it starts fresh");
            }
            Self::Unrestored(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacking checkpoint did not restore; it starts fresh");
            }
            Self::Dropped { pieces, error } => {
                tracing::warn!(job_id = job_id.0, pieces, error = %error, "a torrent's pieces did not read from its files; they are fetched again");
            }
            Self::Missing { pieces } => {
                tracing::warn!(
                    job_id = job_id.0,
                    pieces,
                    "a torrent's pieces are not in its files; they are checked or fetched again"
                );
            }
            Self::Absent { pieces } => {
                tracing::warn!(
                    job_id = job_id.0,
                    pieces,
                    "none of a torrent's files is there; its pieces are kept and it is held until a start, a recheck or a move"
                );
            }
        }
    }
}

// What a torrent's store is made from: the directory its files are in,
// its layout, whether it unpacks into a tee, and the archive passwords the
// tee takes.
struct Making {
    files: PathBuf,
    layout: Arc<PieceLayout>,
    tee: bool,
    passwords: Vec<String>,
    // In seed mode: its pieces had are unchecked, and go to the tee only
    // as their seed-mode check passes.
    seeded: bool,
    // The pieces it kept as the boot read them; none to read its row.
    kept: Option<Option<Kept>>,
}

// The directory the torrent's files are in: where they were placed, or else
// the directory claimed for it.
fn files_dir(torrent: &Live) -> PathBuf {
    PathBuf::from(
        torrent
            .record
            .files_dir
            .clone()
            .or_else(|| torrent.record.storage_dir.clone())
            .unwrap_or_default(),
    )
}

// The torrent's listed files and what unpacking put beside them, moved to
// `to` under the lock no read or write gets past, and read and written
// there from then on; where they went, and whether a file already there
// was kept in place of one of them.
pub(super) fn move_listed(
    files: &Files,
    outputs: &[String],
    to: PathBuf,
    existing: Existing,
) -> Result<(PathBuf, bool), MoveError> {
    let names: Vec<PathBuf> = files
        .names()
        .flatten()
        .map(Path::to_path_buf)
        .chain(outputs.iter().map(PathBuf::from))
        .collect();
    files.relocate(|dir| {
        let moves: Vec<(PathBuf, PathBuf)> = names
            .iter()
            .map(|name| (dir.join(name), to.join(name)))
            .collect();
        let kept = relocate::move_files(dir, &moves, existing)?;
        Ok((to.clone(), (to, kept)))
    })
}

// What a deletion of a torrent's files removes: see `written_paths`.
#[derive(Debug, Default)]
pub(super) struct Deletion {
    // The torrent's own files Weaver made, each removed only as a file.
    pub(super) paths: Vec<PathBuf>,
    // What its job made of them, files or folders, removed whole.
    pub(super) made: Vec<PathBuf>,
    // The directory the files are in.
    pub(super) dir: PathBuf,
    // Whether that directory is the torrent's own, to go once empty.
    pub(super) own: bool,
    // The folders that were there before the torrent, which stay.
    pub(super) kept_dirs: HashSet<PathBuf>,
    // The folder claimed for the torrent beside a shared one, to go once
    // empty.
    pub(super) claimed: Option<PathBuf>,
}

// What a deletion of the files takes: each listed file not among those
// adopted, and each of `outputs`, in the directory the files are in. With a
// folder `claimed` beside it, that directory is a shared one, and stays.
pub(super) fn deletion(
    files: &Files,
    adopted: &Adopted,
    outputs: &[String],
    claimed: Option<PathBuf>,
) -> Deletion {
    let dir = files.dir();
    let kept = files::adopted_paths(adopted);
    let listed = files
        .names()
        .flatten()
        .filter(|name| !adopted.all_files && !kept.contains(*name))
        .map(|name| dir.join(name));
    let paths = listed.collect();
    let made = outputs.iter().map(|output| dir.join(output)).collect();
    let kept_dirs = adopted.dirs.iter().map(|folder| dir.join(folder)).collect();
    Deletion {
        paths,
        made,
        own: claimed.is_none(),
        dir,
        kept_dirs,
        claimed,
    }
}

// libtorrent `storage_utils::delete_files`: the torrent's files go, here
// with what its job made of them, and then each folder above them, up to
// its directory, that this leaves empty and that was not there before the
// torrent. Whatever else is in its directory stays, and so does a folder
// found where one of the torrent's files was (`delete_one_file` removes a
// file, never a folder's contents).
pub(super) fn delete_written(deletion: &Deletion) {
    let mut folders = BTreeSet::new();
    let listed = deletion.paths.iter().map(|path| (path, false));
    let made = deletion.made.iter().map(|path| (path, true));
    for (path, whole) in listed.chain(made) {
        let removed = match std::fs::symlink_metadata(path) {
            Ok(found) if found.is_dir() && whole => std::fs::remove_dir_all(path),
            Ok(found) if found.is_dir() => {
                tracing::warn!(file = %path.display(), "a folder is where a torrent's file was, and stays");
                Ok(())
            }
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
        for folder in path.ancestors().skip(1) {
            if !folder.starts_with(&deletion.dir) {
                break;
            }
            if folder == deletion.dir && !deletion.own {
                break;
            }
            // The folders above one already taken were taken with it.
            if !folders.insert((folder.components().count(), folder.to_path_buf())) {
                break;
            }
        }
    }
    folders.extend(
        deletion
            .claimed
            .iter()
            .map(|folder| (folder.components().count(), folder.clone())),
    );
    // Deepest first, so a folder emptied by the one below it goes too; one
    // that is not empty stays, as `remove_dir` leaves it.
    for (_, folder) in folders.iter().rev() {
        if !deletion.kept_dirs.contains(folder) {
            let _ = std::fs::remove_dir(folder);
        }
    }
}

// How a torrent's job ends when unpacking left it short of what it would
// deliver, from the files it `delivered` whole (AGENTS.md, "Job status:
// Partial"): partial when one of them is a file people open, read as a
// graph job's delivery reads one (`Delivery::status`), and failed for `why`
// when none is. An archive volume, a split part and recovery data are never
// usable files, whole or not.
// What placing a torrent's unpacked files came to.
enum Placing {
    // Not yet: the next settle asks again.
    Waits,
    // Every one is beside the torrent's files.
    Placed,
    // Some or all stay where unpacking made them, for `why`; `placed` are
    // the ones beside the torrent's files.
    Short { why: String, placed: Vec<String> },
}

// Why a torrent's unpacked files are not all beside its own, and where
// they are.
fn unplaced(work: &Path, error: &impl std::fmt::Display) -> String {
    format!(
        "the unpacked files could not be placed beside the torrent's files ({error}); they are kept in {}",
        work.display()
    )
}

fn short_status(delivered: &[String], why: &str) -> JobStatus {
    let usable = delivered
        .iter()
        .any(|name| !names_a_piece(name) && weaver_graph::known_type(name));
    if usable {
        JobStatus::Partial
    } else {
        JobStatus::Failed {
            error: why.to_owned(),
        }
    }
}

// Whether the torrent's job unpacks, as its metainfo fixed it.
fn unpacks(torrent: &Live) -> bool {
    torrent.record.unpack_in_force.unwrap_or_default()
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
// session reports none, as while it checks the files, the store's figure
// stands: the pieces the check has found so far, or those the checkpoint
// it starts from vouches for, which are the pieces the torrent counts.
fn take_file_have(kept: &mut Vec<u64>, reported: &[u64], store: Option<&TorrentStore>) {
    if !reported.is_empty() {
        kept.clear();
        kept.extend_from_slice(reported);
    } else if let Some(store) = store {
        store.file_have_into(kept);
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
        wanted_bytes: view.wanted_bytes,
        remaining_bytes: view.wanted_bytes.saturating_sub(view.wanted_have_bytes),
        category: view.category,
        output_dir: view.storage_dir,
        wait_reason: view.detail,
        created_at_ms: view.added_at_ms,
        metadata: view.metadata,
        shortfall: None,
    }
}
