// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock, Weak};
use std::time::Duration;

use librqbit::api::TorrentIdOrHash;
use librqbit::limits::{LimitKind, LimitsConfig};
use librqbit::{
    AddTorrent, AddTorrentOptions, AddTorrentResponse, Magnet, ManagedTorrent, Session,
    TrackerAnnounce, torrent_from_bytes,
};
use tokio::sync::{Notify, mpsc, watch};
use weaver_graph::VolumeIndex;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};

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
use crate::pipeline::graph::limits::{self, Caps, ExtractionLimits};
use crate::pipeline::graph::root::phase::RootPhase;
use crate::pipeline::graph::root::pieces::PieceLayout;
use crate::proxies::TorrentScope;
use crate::settings::model::{TrackerRule, tracker_rule_for};
use crate::settings::{
    Config, SeedingAction, SeedingPolicy, TorrentConfig, resolve_seeding_policy,
};
use crate::torrents::admission::{self, Manifest, Refusal};
use crate::torrents::intake::{Heard as IntakeHeard, Holds, Intake, Room};
use crate::torrents::sessions::{NetworkRoutes, RouteKey, SessionEnv, SessionError, Sessions};
use crate::torrents::status::{self, Engine, Held, Observed, Queued};
use crate::torrents::storage::{PieceStorageFactory, Pieces, TorrentJob, initial_have, layout_of};
use crate::torrents::{
    Infohash, TorrentCounters, TorrentFile, TorrentInsert, TorrentIntent, TorrentRecord,
    TorrentSource, TorrentState, TorrentTracker, TrackerEntry,
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

    fn scope(&self, id: Option<u32>) -> TorrentScope {
        match self.category(id) {
            Some(category) if category.torrent_route => TorrentScope::Category(category.id),
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
        proxies: &crate::proxies::ProxyRuntime,
        pipeline: &crate::pipeline::Pipeline,
        queue: SchedulerHandle,
    ) -> Result<Self, String> {
        let routes = NetworkRoutes::new(
            db.clone(),
            Arc::clone(&proxies.network),
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
    pub infohash: String,
    pub source: TorrentSource,
    pub state: TorrentState,
    pub detail: Option<String>,
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
    pub eta_seconds: Option<u64>,
    pub peers: u32,
    pub seeds: u32,
    pub leechers: u32,
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
    pub route: Option<String>,
    pub files: Vec<TorrentFile>,
    /// Each file's bytes in hand, by its index in the torrent.
    pub file_have: Vec<u64>,
    pub trackers: Vec<TorrentTracker>,
}

impl TorrentView {
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
    infohash: [u8; 20],
    manifest: Manifest,
    layout: Arc<PieceLayout>,
    private: bool,
    files: Vec<TorrentFile>,
    trackers: Vec<TrackerEntry>,
    total: u64,
}

fn tracker(tier: u32, url: &[u8]) -> Option<TrackerEntry> {
    let url = std::str::from_utf8(url).ok()?.trim();
    (!url.is_empty()).then(|| TrackerEntry {
        tier,
        url: url.to_owned(),
    })
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
    let files: Vec<TorrentFile> = layout
        .files()
        .enumerate()
        .map(|(index, file)| TorrentFile {
            index: index as u32,
            path: file.name.clone(),
            length: file.length,
            selected: !file.padding,
        })
        .collect();
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
        infohash: metainfo.info_hash.0,
        manifest: Manifest {
            name,
            single_file: info.info().files.is_none(),
            paths: files
                .iter()
                .filter(|file| file.selected)
                .map(|file| file.path.clone())
                .collect(),
        },
        total: files
            .iter()
            .filter(|file| file.selected)
            .map(|file| file.length)
            .sum(),
        private: info.info().private,
        layout,
        files,
        trackers,
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
    metainfo: Option<Vec<u8>>,
    layout: Option<Arc<PieceLayout>>,
    job: Option<Arc<TorrentJob>>,
    intake: Option<Arc<Intake>>,
    attached: Option<Attached>,
    route: Option<RouteKey>,
    held: Option<Held>,
    counters: TorrentCounters,
    // What the session reported at the last tick, for the ledger's deltas.
    seen: (u64, u64),
    engine: Engine,
    // Why the session last refused to pause or resume the torrent, until it
    // takes the next change.
    run_error: Option<String>,
    queued: bool,
    // Who holds it stopped, while a hold does.
    paused_by: Option<HoldOrigin>,
    applied: Option<Vec<usize>>,
    speeds: (u64, u64),
    peers: (u32, u32),
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
    fn new(record: TorrentRecord, counters: TorrentCounters) -> Self {
        Self {
            record,
            names: Vec::new(),
            total: 0,
            metainfo: None,
            layout: None,
            job: None,
            intake: None,
            attached: None,
            route: None,
            held: None,
            counters,
            seen: (0, 0),
            engine: Engine::Absent,
            run_error: None,
            queued: false,
            paused_by: None,
            applied: None,
            speeds: (0, 0),
            peers: (0, 0),
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

    fn holding(&self) -> bool {
        self.intake.as_ref().is_some_and(|intake| intake.holding())
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
        let fetched = live.map_or(0, |live| live.snapshot.fetched_bytes);
        let uploaded = stats.uploaded_bytes;
        // The session counts from zero each time it takes the torrent.
        let delta = |now: u64, then: u64| if now >= then { now - then } else { now };
        let (up, down) = (delta(uploaded, self.seen.0), delta(fetched, self.seen.1));
        let mut counters = self.counters;
        counters.uploaded_bytes += up;
        counters.downloaded_bytes += down;
        if up + down > 0 {
            counters.last_activity_ms = Some(now);
        }
        Reading {
            engine,
            file_have,
            counters,
            seen: (uploaded, fetched),
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
            disk_full: self.holding(),
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
    seen: (u64, u64),
    speeds: (u64, u64),
    peers: (u32, u32),
}

// What the job side asks the service to look at: the jobs that changed,
// the ones whose checkpoint is behind them, and the writes a job's intake
// could not make.
#[derive(Default)]
struct Signal {
    dirty: Mutex<BTreeSet<JobId>>,
    unsaved: Mutex<BTreeSet<JobId>>,
    refused: Mutex<BTreeMap<JobId, String>>,
    wake: Notify,
}

fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Signal {
    fn mark(&self, job_id: JobId) {
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
        if let Err(error) = self.job.with(|job| job.run_said(said, slots)) {
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
        sessions.set_encryption(settings.torrent.encryption);
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
        self.inner
            .sessions
            .set_encryption(settings.torrent.encryption);
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
        self.inner.sessions.stop_all().await;
        self.save_checkpoints(self.inner.signal.take_unsaved())
            .await;
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

    fn keep(&self, task: tokio::task::JoinHandle<()>) {
        self.inner
            .tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(task);
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
    // is done completes it. A held piece stops the selection at once.
    async fn settle(&self) {
        let dirty = self.inner.signal.take();
        for job_id in dirty {
            let job = {
                let live = self.inner.live.lock().await;
                live.get(&job_id).and_then(|torrent| torrent.job.clone())
            };
            let Some(job) = job else { continue };
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
        self.apply_selection().await;
        self.publish().await;
    }

    // Write the checkpoints of `jobs`.
    async fn save_checkpoints(&self, jobs: BTreeSet<JobId>) {
        let jobs: Vec<(JobId, Arc<TorrentJob>)> = {
            let live = self.inner.live.lock().await;
            jobs.into_iter()
                .filter_map(|job_id| Some((job_id, live.get(&job_id)?.job.clone()?)))
                .collect()
        };
        for (job_id, job) in jobs {
            self.save_checkpoint(job_id, &job).await;
        }
    }

    async fn save_checkpoint(&self, job_id: JobId, job: &TorrentJob) {
        let snapshot = match job.with(|job| job.snapshot()) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not checkpoint a torrent job");
                return;
            }
        };
        // A torrent job's checkpoint is written whole: its rows change little
        // and seldom.
        let rows = Arc::new(Delta::between(None, &checkpoint::rows(&snapshot)));
        let db = self.inner.db.clone();
        match tokio::task::spawn_blocking(move || db.save_graph_checkpoint(job_id, rows)).await {
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

    fn storage(&self, dir: PathBuf, layout: &PieceLayout) -> Storage {
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
        }
    }

    // The job of a torrent with metainfo, from its checkpoint when it has one.
    fn job(
        &self,
        job_id: JobId,
        dir: PathBuf,
        layout: &Arc<PieceLayout>,
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
                    self.storage(dir.clone(), layout),
                    slots,
                )
                .ok()
            });
        let job = match restored {
            Some(restored) => restored.job,
            None => GraphJob::for_torrent(job_id.0, Arc::clone(layout), self.storage(dir, layout)),
        };
        let job = Arc::new(TorrentJob::new(job, slots));
        let env = &self.inner.env;
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
            Holds {
                accountant: Arc::clone(&env.accountant),
                namespace: weaver_graph::holds::HoldsNamespace {
                    scope: job_id.0,
                    node: weaver_graph::NodeId::ROOT,
                },
            },
            Box::new(move |heard| signal.heard(job_id, heard)),
        ));
        (job, intake)
    }

    fn give_metainfo(&self, torrent: &mut Live, bytes: Vec<u8>, parsed: &Parsed) {
        let dir = PathBuf::from(torrent.record.storage_dir.clone().unwrap_or_default());
        let (job, intake) = self.job(torrent.record.job_id, dir, &parsed.layout);
        torrent.names = parsed.manifest.paths.clone();
        torrent.total = parsed.total;
        torrent.layout = Some(Arc::clone(&parsed.layout));
        torrent.file_have = job.with(|job| job.file_have());
        torrent.job = Some(job);
        torrent.intake = Some(intake);
        torrent.metainfo = Some(bytes);
    }

    // Add a torrent. A metainfo becomes a job at once; a magnet needs its
    // route's session to fetch the metainfo, and becomes one when it has.
    /// Whether the route a torrent in `scope` would take carries DHT, so a
    /// magnet with no trackers can find its peers there.
    pub fn carries_dht(&self, scope: TorrentScope) -> bool {
        self.inner
            .sessions
            .routes()
            .plan(scope)
            .is_ok_and(|plan| plan.kind.shape().dht)
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
                let parsed = parse(&bytes)?;
                self.admit(&parsed.manifest)?;
                self.duplicate(Infohash::V1(parsed.infohash))?;
                let job_id = self.reserve()?;
                let dir = self
                    .claim_dir(&settings, category.as_ref(), &parsed.manifest, job_id)
                    .await?;
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    parsed.infohash,
                    parsed.manifest.name.clone(),
                    TorrentSource::Metainfo,
                    Some(bytes.clone()),
                    parsed.private,
                    Some(dir.clone()),
                );
                if let Err(error) = self.insert(&mut record, &parsed.files, &parsed.trackers) {
                    give_back(&dir).await;
                    return Err(error);
                }
                let mut torrent = Live::new(record, TorrentCounters::default());
                self.give_metainfo(&mut torrent, bytes, &parsed);
                self.inner.live.lock().await.insert(job_id, torrent);
                self.apply_queue().await;
                self.attach(job_id).await;
                self.publish().await;
                Ok(job_id)
            }
            AddSource::Magnet(uri) => {
                let magnet = Magnet::parse(&uri)
                    .map_err(|error| AddError::Invalid(format!("not a magnet: {error}")))?;
                let infohash = magnet
                    .as_id20()
                    .ok_or_else(|| AddError::Invalid("the magnet has no v1 infohash".into()))?
                    .0;
                self.duplicate(Infohash::V1(infohash))?;
                let scope = settings.scope(category.as_ref().map(|category| category.id));
                let plan = self
                    .inner
                    .sessions
                    .routes()
                    .plan(scope)
                    .map_err(AddError::NoNetwork)?;
                if magnet.trackers.is_empty() && !plan.kind.shape().dht {
                    return Err(AddError::NoNetwork(
                        "a magnet with no trackers needs DHT, which this route does not carry"
                            .into(),
                    ));
                }
                let session = self
                    .inner
                    .sessions
                    .session(&plan)
                    .await
                    .map_err(|error| AddError::NoNetwork(error.to_string()))?;
                let job_id = self.reserve()?;
                let name = magnet.name.clone().unwrap_or_else(|| hex::encode(infohash));
                let mut record = self.record(
                    job_id,
                    &request,
                    category.as_ref(),
                    infohash,
                    name,
                    TorrentSource::Magnet,
                    None,
                    false,
                    None,
                );
                let trackers: Vec<TrackerEntry> = magnet
                    .trackers
                    .iter()
                    .filter_map(|url| tracker(0, url.as_bytes()))
                    .collect();
                self.insert(&mut record, &[], &trackers)?;
                let mut torrent = Live::new(record, TorrentCounters::default());
                torrent.route = Some(plan.key.clone());
                self.inner.live.lock().await.insert(job_id, torrent);
                self.spawn_resolve(job_id, session, uri, None);
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

    fn duplicate(&self, infohash: Infohash) -> Result<(), AddError> {
        match self.inner.db.get_torrent_by_infohash(infohash) {
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
        infohash: [u8; 20],
        name: String,
        source: TorrentSource,
        metainfo: Option<Vec<u8>>,
        private: bool,
        storage_dir: Option<String>,
    ) -> TorrentRecord {
        TorrentRecord {
            job_id,
            infohash_v1: Some(infohash),
            infohash_v2: None,
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
        }
    }

    // Keep `record` with the rule its trackers match.
    fn insert(
        &self,
        record: &mut TorrentRecord,
        files: &[TorrentFile],
        trackers: &[TrackerEntry],
    ) -> Result<(), AddError> {
        record.tracker_rule_id = self.rule_id(trackers);
        match self.inner.db.insert_torrent(record, files, trackers) {
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
    ) {
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            let listed = session
                .add_torrent(
                    AddTorrent::from_url(uri),
                    Some(AddTorrentOptions {
                        list_only: true,
                        trackers,
                        ..Default::default()
                    }),
                )
                .await;
            let bytes = match listed {
                Ok(AddTorrentResponse::ListOnly(listed)) => listed.torrent_bytes.to_vec(),
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

    async fn resolved(&self, job_id: JobId, bytes: Vec<u8>) -> Result<(), AddError> {
        let parsed = parse(&bytes)?;
        self.admit(&parsed.manifest)?;
        let settings = self.settings();
        let Some(category_id) = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| torrent.record.category_id)
        else {
            return Ok(());
        };
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
        torrent.record.name = parsed.manifest.name.clone();
        torrent.record.private = parsed.private;
        torrent.record.storage_dir = Some(dir.clone());
        torrent.record.metainfo = Some(bytes.clone());
        let rule_id = self.rule_id(&parsed.trackers);
        self.inner
            .db
            .set_torrent_tracker_rule(job_id, rule_id)
            .map_err(|error| AddError::State(state_error(error)))?;
        torrent.record.tracker_rule_id = rule_id;
        self.inner
            .db
            .set_torrent_metainfo(
                job_id,
                &parsed.manifest.name,
                &bytes,
                parsed.private,
                &dir,
                &parsed.files,
                &parsed.trackers,
            )
            .map_err(|error| AddError::State(state_error(error)))?;
        self.give_metainfo(torrent, bytes, &parsed);
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
        let scope = self.settings().scope(record.category_id);
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
        let uri = format!(
            "magnet:?xt=urn:btih:{}",
            hex::encode(record.infohash_v1.unwrap_or_default())
        );
        let trackers = self
            .inner
            .db
            .list_torrent_trackers(job_id)
            .unwrap_or_default()
            .into_iter()
            .map(|tracker| tracker.url)
            .collect();
        self.spawn_resolve(job_id, session, uri, Some(trackers));
    }

    // Put a torrent in its route's session, or hold it while the route is
    // down.
    async fn attach(&self, job_id: JobId) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let (scope, bytes, layout, intake, dir, paused, limit, have) = {
            let live = self.inner.live.lock().await;
            let Some(torrent) = live.get(&job_id) else {
                return;
            };
            if torrent.attached.is_some() {
                return;
            }
            let (Some(bytes), Some(layout), Some(intake), Some(job)) = (
                torrent.metainfo.clone(),
                torrent.layout.clone(),
                torrent.intake.clone(),
                torrent.job.as_ref(),
            ) else {
                return;
            };
            let limit = settings
                .policy(&torrent.record, &rules)
                .and_then(|policy| policy.max_upload_bps)
                .map(|bps| u32::try_from(bps).unwrap_or(u32::MAX));
            (
                settings.scope(torrent.record.category_id),
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
                limit,
                job.with(|job| initial_have(job)),
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
        let factory = PieceStorageFactory::new(layout, intake as Arc<dyn Pieces>);
        let mut options = AddTorrentOptions {
            paused,
            overwrite: true,
            output_folder: Some(dir),
            storage_factory: Some(factory.boxed_factory()),
            ratelimits: LimitsConfig {
                upload_bps: LimitKind::from(limit),
                download_bps: LimitKind::Unlimited,
            },
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
                torrent.seen = (0, 0);
                torrent.applied = None;
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
            (report, torrent.record.infohash_v1)
        };
        self.apply_queue().await;
        self.finish(report, "failed", hash, now).await;
    }

    // A torrent that left the live ones goes to history: its row, then out
    // of the torrents and into the finished jobs.
    async fn retire(&self, torrent: Live, status: &str, error: Option<String>) {
        let job_id = torrent.record.job_id;
        self.inner.env.outside.leave(job_id.0);
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
        self.finish(report, status, torrent.record.infohash_v1, now)
            .await;
    }

    // A torrent's job, finished as `status`: into history and the queue's
    // finished jobs.
    async fn finish(&self, report: JobReport, status: &str, hash: Option<[u8; 20]>, now: i64) {
        let job_id = report.job_id;
        let row = report.history_row(status, hash.map(|hash| hash.to_vec()), now);
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
        let moved = {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            let before = settings.scope(torrent.record.category_id);
            torrent.record.category_id = category_id;
            self.save_options(&torrent.record)?;
            let moved = before != settings.scope(category_id);
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
                    let bytes = record.metainfo.clone().unwrap_or_default();
                    self.give_metainfo(&mut torrent, bytes, &parsed);
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
        self.retry_holds().await;
        self.read(now, elapsed_s).await;
        self.save_checkpoints(self.inner.signal.take_unsaved())
            .await;
        self.apply_goals(now).await;
        self.apply_queue().await;
        self.apply_selection().await;
        self.drop_idle_sessions().await;
        self.publish().await;
        self.inner.ticks.send_modify(|ticks| *ticks += 1);
    }

    // A torrent whose route changed moves to the new route's session; one
    // whose route went down is held, and comes back when it is up.
    async fn follow_routes(&self) {
        let settings = self.settings();
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
                let scope = settings.scope(torrent.record.category_id);
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

    async fn retry_holds(&self) {
        let intakes: Vec<(JobId, Arc<Intake>)> = self
            .inner
            .live
            .lock()
            .await
            .values()
            .filter_map(|torrent| Some((torrent.record.job_id, torrent.intake.clone()?)))
            .filter(|(_, intake)| intake.holding())
            .collect();
        for (job_id, intake) in intakes {
            if let Err(error) = tokio::task::spawn_blocking(move || intake.retry()).await {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's held pieces were not retried");
            }
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
                if torrent.counters != before
                    || now - torrent.counters.updated_at_ms >= LEDGER_EVERY_MS
                {
                    torrent.counters.updated_at_ms = now;
                    changed.push((torrent.record.job_id, torrent.counters));
                }
            }
        }
        for (job_id, counters) in changed {
            let db = self.inner.db.clone();
            let saved =
                tokio::task::spawn_blocking(move || db.update_torrent_counters(job_id, &counters))
                    .await;
            match saved {
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

    // The files the job wants now, or none while the disk is full.
    async fn apply_selection(&self) {
        let mut live = self.inner.live.lock().await;
        for torrent in live.values_mut() {
            let (Some(attached), Some(job)) = (&torrent.attached, &torrent.job) else {
                continue;
            };
            let holding = torrent
                .intake
                .as_ref()
                .is_some_and(|intake| intake.holding());
            let wanted = if holding {
                Vec::new()
            } else {
                let selection = job.with(|job| job.selection());
                if selection.is_empty() {
                    continue;
                }
                selection
            };
            if torrent.applied.as_ref() == Some(&wanted) {
                continue;
            }
            let set: HashSet<usize> = wanted.iter().copied().collect();
            match attached
                .session
                .update_only_files(&attached.handle, &set)
                .await
            {
                Ok(()) => torrent.applied = Some(wanted),
                Err(error) => {
                    tracing::debug!(job_id = torrent.record.job_id.0, error = %error, "a torrent's files did not change");
                }
            }
        }
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
            .filter(|torrent| torrent.holding())
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
        let have: u64 = reading.file_have.iter().sum();
        let rules = self.rules();
        let policy = settings.policy(record, &rules);
        TorrentView {
            job_id: record.job_id,
            name: record.name.clone(),
            infohash: record.infohash_v1.map(hex::encode).unwrap_or_default(),
            source: record.source,
            state,
            detail,
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
            eta_seconds: status::eta_seconds(total.saturating_sub(have), reading.speeds.0),
            peers: reading.peers.0,
            seeds: reading.peers.1,
            leechers: reading.peers.0.saturating_sub(reading.peers.1),
            added_at_ms: record.added_at_ms,
            completed_at_ms: record.completed_at_ms,
            storage_dir: record.storage_dir.clone(),
            seeding_left: status::goal_left(policy.as_ref(), counters, total, now),
            seeding_override: record.policy_override,
            seeding_inherited: settings.inherited(record, &rules),
            seeding_policy: policy.unwrap_or_default(),
            seeding_source: settings.seeding_source(record, &rules),
            route: torrent.route.as_ref().map(|key| key.as_str().to_owned()),
            files: Vec::new(),
            file_have: reading.file_have,
            trackers: Vec::new(),
        }
    }

    /// One torrent, with its files and trackers.
    pub async fn torrent(&self, job_id: JobId) -> Option<TorrentView> {
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (mut view, announces) = {
            let live = self.inner.live.lock().await;
            let torrent = live.get(&job_id)?;
            (self.view(&settings, torrent, now), torrent.announces())
        };
        view.files = self.inner.db.list_torrent_files(job_id).unwrap_or_default();
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

    /// The scope whose route a torrent added in `category` takes.
    pub fn scope_of(&self, category: Option<&str>) -> TorrentScope {
        let settings = self.settings();
        let id = category
            .and_then(|name| resolve_category_config(&settings.categories, name))
            .map(|category| category.id);
        settings.scope(id)
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

// The pieces the session starts from, so it does not check the files.
fn apply_initial_have(
    options: &mut AddTorrentOptions,
    have: bitvec::boxed::BitBox<u8, bitvec::order::Msb0>,
) {
    options.initial_have = Some(have);
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
        // A stop with any reason but its met goal is a failure.
        TorrentState::Stopped => match view.detail.as_deref() {
            None | Some(status::GOAL_MET) if view.completed_at_ms.is_some() => JobStatus::Complete,
            detail => JobStatus::Failed {
                error: detail.unwrap_or_default().to_owned(),
            },
        },
    }
}

// A downloading torrent's speed and ETA, as the queue reads a job's.
fn downloading(view: &TorrentView, now_ms: i64) -> Option<JobPhaseProgress> {
    (view.state == TorrentState::Downloading).then(|| JobPhaseProgress {
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

// Every file's bytes in hand. The session's progress counts only the files
// selected now, and the selection narrows as files finish or while the disk
// is full. While the session checks the files, the checkpoint's figure stands.
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
    }
}
