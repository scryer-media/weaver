// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, watch};
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_torrent::core::{Id20, Id32, InfoHashes};
use weaver_torrent::limits::LimitsConfig;
use weaver_torrent::{
    AddTorrentOptions, Identity, ManagedTorrent, PeerConnectionOptions, SslIdentity, TorrentTotals,
    tracker::Tracker,
};

use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::handle::{HoldSet, JobInfo, QueueMoveTarget, SchedulerHandle};
use crate::jobs::model::{JobStatus, Protocol};
use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress};
use crate::jobs::report::JobReport;
use crate::persistence::Database;
use crate::pipeline::ExtractionLimits;
use crate::pipeline::graph::engine::{DecodePool, Outside, RunMail, Said};
use crate::pipeline::graph::status::RepairSlots;
use crate::settings::model::{TrackerCertificate, TrackerRule};
use crate::settings::{Client, Config, TorrentEncryption, TransportRule};
use crate::torrents::identity::TorrentIdentity;
use crate::torrents::intake::Heard as IntakeHeard;
use crate::torrents::layout::PieceLayout;
use crate::torrents::metrics::{Counters, Published, SessionSeries, TorrentMetrics};
use crate::torrents::rate::limit_kind;
use crate::torrents::sessions::{
    NetworkRoutes, RouteKey, SessionEnv, SessionError, Sessions, encryption_policy,
};
use crate::torrents::status::{self, Engine, Held, Queued};
use crate::torrents::tee::Tee;
use crate::torrents::{
    FilePriority, KeptIntent, TorrentCounters, TorrentFile, TorrentIntent, TorrentRecord,
    TorrentState, TrackerEntry,
};
use weaver_server_base::clock::{Clock, SystemClock};
use weaver_server_disk::operations::disk::{CapacityReader, StorageCapacity};
use weaver_server_disk::scratch::ScratchLedger;

pub mod fingerprint;
pub mod listing;
mod scripts;
pub mod snapshot;
pub use scripts::ScriptDirs;
pub mod batch;
mod checkpoints;
mod completion;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use completion::placing_end;
pub mod disk_room;
pub mod intake;
pub mod live;
pub mod moves;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod old_tick;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod opened;
pub mod policy;
pub mod queue;
pub mod restore;
pub mod retire;
pub mod settings;
mod tick;
pub mod trackers;
pub mod updates;
pub mod views;
pub use batch::TorrentSetting;
use checkpoints::Landing;
pub use disk_room::disk_warning;
use disk_room::{DiskWait, NoRoom, Relook, RunMark};
use intake::client_of;
pub use intake::{AddError, AddRequest, AddSource, Placement};
use live::{Live, Timed};
#[cfg(test)]
pub(in crate::torrents) use moves::{Listed, Moved, NotMoved, move_listed};
use queue::QueueEnd;
#[cfg(test)]
pub(in crate::torrents) use retire::{delete_written, deletion, listed_files};
pub use settings::{
    Categories, DHT, ENCRYPTION, Field, INACTIVE_GOAL, LSD, PEX, Presence, RATIO_GOAL, SCOPE,
    SEED_TIME_COUNTING, SEED_TIME_GOAL, SEEDING_ACTION, ScheduleSeeding, SeedingLevels,
    TorrentSettings, UNPACK, UPLOAD_CAP, WindDown,
};
pub use tick::TorrentRoute;
pub use views::{
    ClockFields, FilesPage, PeerKinds, RowParts, TorrentLeg, TorrentPieces, TorrentSupport,
    TorrentView, ViewClock,
};

// Why a magnet with no trackers is turned away on a route with no DHT.
pub const NEEDS_DHT: &str = "a magnet with no trackers needs DHT, which this route does not carry";

pub use batch::HISTORY_CHUNK;

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
pub fn times_offset_ms(job_id: JobId) -> i64 {
    let mut z = job_id.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^= z >> 31;
    (z % TIMES_EVERY_MS as u64) as i64
}

// How many torrents' jobs are restored from their checkpoints at once.
pub const RESTORES_AT_ONCE: usize = 8;

/// The most pieces a torrent's ledger goes without between ticks: once
/// this many pieces reached its job, or left it, since the checkpoint was
/// last written, it is written at once, so a crash fetches again at most
/// these and those that verified while that write was on its way. A
/// download is never more than a few pieces a write behind, at any rate:
/// a faster one only waits on the write in flight, and its next write
/// takes everything verified meanwhile.
pub const CHECKPOINT_PIECES: u32 = 16;

// What a torrent's session holds its peers to under its tracker rule:
// whether it looks for them on the DHT, on the local network and by peer
// exchange, each switch on its own, and the encryption its connections are
// made under and the transport order they are dialed in, each over the
// session's. A change of any takes the torrent through its session again.
// The client it names itself as is one of them: its peers and trackers see
// its peer id, and a change of client stops the torrent and starts it
// again, so each tracker hears `stopped` from the old id and `started` from
// the new.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerTerms {
    client: Client,
    disable_dht: bool,
    disable_pex: bool,
    disable_lsd: bool,
    encryption: Option<TorrentEncryption>,
    dial_orders: Option<weaver_torrent::DialOrders>,
}

impl PeerTerms {
    // A switch no level sets is the session's, which runs each for every
    // torrent it is not turned off for.
    fn of(settings: &TorrentSettings, rule: Option<&TrackerRule>) -> Self {
        let off =
            |field: &Field<bool>| settings.levels(field, None, None, rule).resolve() == Some(false);
        Self {
            client: rule.and_then(|rule| rule.client).unwrap_or_default(),
            disable_dht: off(&DHT),
            disable_pex: off(&PEX),
            disable_lsd: off(&LSD),
            encryption: settings.levels(&ENCRYPTION, None, None, rule).resolve(),
            dial_orders: rule
                .map(|rule| rule.transports)
                .filter(TransportRule::is_set)
                .map(|transports| {
                    transports
                        .resolve(settings.torrent.transports)
                        .dial_orders()
                }),
        }
    }

    // `options` under these terms.
    fn apply(self, options: &mut AddTorrentOptions) {
        options.disable_dht = self.disable_dht;
        options.disable_pex = self.disable_pex;
        options.disable_lsd = self.disable_lsd;
        options.peer_opts = (self.encryption.is_some() || self.dial_orders.is_some()).then(|| {
            PeerConnectionOptions {
                encryption: self.encryption.map(encryption_policy),
                dial_orders: self.dial_orders,
                ..Default::default()
            }
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

// How a torrent that is `own` under `rule` names itself to peers and
// trackers: as its client, under its own peer id and tracker key.
fn identity_of(rule: Option<&TrackerRule>, own: &TorrentIdentity) -> Identity {
    client_of(rule).identity_of(own)
}

// The limits `record`'s session holds it to: its own, and on upload the cap
// its levels resolve to where that is tighter. Each is a channel the
// torrent's peers draw from beside the session's, as each peer class over a
// libtorrent peer is (`bandwidth_manager::request_bandwidth`).
pub fn ratelimits_of(record: &TorrentRecord, cap: Option<u64>) -> LimitsConfig {
    LimitsConfig {
        upload_bps: limit_kind(record.upload_limit).tighter(limit_kind(cap)),
        download_bps: limit_kind(record.download_limit),
    }
}

/// What torrent jobs are built with.
#[derive(Clone)]
pub struct JobEnv {
    pub accountant: Arc<HoldsAccountant>,
    pub free_disk: CapacityReader,
    // The disks every job's writes are judged on, the pipeline's own: a
    // torrent whose write found no room waits on them for room as a usenet
    // job does. None with no pipeline, where a torrent in upload mode
    // tries again by itself.
    pub capacity: Option<Arc<StorageCapacity>>,
    // Where every torrent's pieces are written: the volumes' writers the
    // pipeline writes through too.
    pub disk: Arc<crate::torrents::writes::TorrentDisk>,
    pub slots: usize,
    pub limits: Arc<ExtractionLimits>,
    // The decode pool the pipeline's jobs share, and where a decode window
    // goes: under `work`, else in weaver's data folder.
    pub decode: Arc<DecodePool>,
    pub window_ledger: ScratchLedger,
    pub data_dir: Option<(PathBuf, CapacityReader, ScratchLedger)>,
    // Where the jobs' runs speak, where the database's word on their
    // checkpoints comes, and where a job joins to hear both.
    pub mail: mpsc::Sender<Said>,
    pub written: mpsc::UnboundedSender<Said>,
    pub outside: Arc<Outside>,
    // Where a torrent's tee unpacks, each in a directory of its own.
    pub work: PathBuf,
    // The repair slots every torrent job shares, as the pipeline's jobs
    // share theirs.
    pub repairs: Arc<Mutex<RepairSlots>>,
    // With no pipeline to hear the mail, the mail itself, until the service
    // hears it in the pipeline's stead.
    pub alone: Arc<Mutex<Option<RunMail>>>,
    // Where the scripts a torrent's job runs are told weaver keeps things,
    // as the pipeline tells its jobs' scripts.
    pub script_dirs: ScriptDirs,
}

impl JobEnv {
    /// The pipeline's own budget: its holds, slots, extraction limits and
    /// the complete root's free space; written through the volumes' writers
    /// the process shares, `writers`, on the limits `buffers` sizes.
    pub fn of_pipeline(
        pipeline: &crate::pipeline::Pipeline,
        writers: Arc<crate::operations::VolumeWriters>,
        buffers: Arc<crate::operations::VolumeBuffers>,
    ) -> Self {
        let budget = pipeline.graph_budget();
        let capacity = pipeline.storage_capacity();
        Self {
            accountant: budget.accountant,
            free_disk: budget.free_disk,
            disk: crate::torrents::writes::TorrentDisk::new(
                writers,
                Some(Arc::clone(&capacity)),
                Some(buffers),
            ),
            capacity: Some(capacity),
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
            capacity: None,
            disk: crate::torrents::writes::TorrentDisk::alone(),
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
                if let Some(unheard) = outside.deliver(said, slots) {
                    // Off the runtime: the removal is a filesystem call.
                    tokio::task::spawn_blocking(move || {
                        crate::pipeline::graph::job::discard_unheard(unheard);
                    });
                }
            }
        }))
    }
}

/// How long a torrent leg waits to dial through its route.
const ROUTE_TIMEOUT: Duration = Duration::from_secs(30);

impl TorrentService {
    /// The server's torrent client, started: sessions on the proxy
    /// runtime's routes while the settings turn torrents on, jobs on the
    /// pipeline's budget, and its torrents in the pipeline's queue. Its
    /// pieces are written by the volumes' writers the process shares,
    /// `writers`, on the limits `buffers` sizes.
    #[allow(clippy::too_many_arguments)]
    pub async fn for_server(
        config: &Config,
        db: Database,
        proxies: &Arc<crate::proxies::ProxyRuntime>,
        pipeline: &crate::pipeline::Pipeline,
        writers: Arc<crate::operations::VolumeWriters>,
        buffers: Arc<crate::operations::VolumeBuffers>,
        queue: SchedulerHandle,
        crashed: weaver_server_base::runtime::panic_trap::CrashMarks,
    ) -> Result<Self, String> {
        let routes = NetworkRoutes::new(Arc::clone(proxies), ROUTE_TIMEOUT);
        let sessions = Sessions::new(Arc::new(routes), SessionEnv::of(config), db.clone());
        // An e2e run may move the wall clock on by an offset; otherwise the
        // system clock.
        let clock: Arc<dyn Clock> = match weaver_server_base::e2e_clock::torrent_clock() {
            Some(offset) => Arc::new(offset),
            None => Arc::new(SystemClock),
        };
        let service = Self::new(
            db,
            sessions,
            TorrentSettings::of(config),
            JobEnv::of_pipeline(pipeline, writers, buffers),
            clock,
        );
        service.wire_to(queue);
        service.park_crashed(crashed);
        service.turn_at_start(config.torrent.enabled).await;
        service.start_seated().await?;
        Ok(service)
    }
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
    // The caller may not use that location (`may_place`).
    #[error("{0}")]
    Forbidden(String),
}

fn state_error(error: StateError) -> String {
    error.to_string()
}

// `trackers` as a torrent lists them: by tier, in the order given within a
// tier, each URL once where, and as whoever, first named it. A tracker that
// is no URL is left out, as a session leaves it out.
pub fn tidy_trackers(trackers: Vec<TrackerEntry>) -> Vec<TrackerEntry> {
    let mut tidy: Vec<TrackerEntry> = Vec::with_capacity(trackers.len());
    let mut seen: std::collections::HashSet<String> =
        std::collections::HashSet::with_capacity(trackers.len());
    for entry in trackers {
        let url = entry.url.trim();
        if weaver_torrent::Url::parse(url).is_err() {
            continue;
        }
        if seen.insert(url.to_owned()) {
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

// `trackers` with each URL once in a tier, the first that names it kept, in
// the order given: what the table's key over a tier and a URL keeps.
pub fn once_a_tier(trackers: &[TrackerEntry]) -> Vec<TrackerEntry> {
    let mut seen: std::collections::HashSet<(u32, &str)> =
        std::collections::HashSet::with_capacity(trackers.len());
    trackers
        .iter()
        .filter(|entry| seen.insert((entry.tier, entry.url.as_str())))
        .cloned()
        .collect()
}

// `trackers` in the order a torrent keeps them: by tier, then by the URL's
// bytes, whatever order a database read gave them in.
fn in_table_order(mut trackers: Vec<TrackerEntry>) -> Arc<[TrackerEntry]> {
    trackers.sort_by(|a, b| (a.tier, &a.url).cmp(&(b.tier, &b.url)));
    trackers.into()
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

// The hashes `record` goes by.
pub fn hashes_of(record: &TorrentRecord) -> InfoHashes {
    InfoHashes {
        v1: record.infohash_v1.map(Id20::new),
        v2: record.infohash_v2.map(Id32::new),
    }
}

// The torrents on each route's session, attached or held there: how loaded
// each pool member is when a torrent is placed.
fn load_of<'a>(torrents: impl Iterator<Item = &'a Live>) -> HashMap<RouteKey, usize> {
    let mut load = HashMap::new();
    for key in torrents.filter_map(|torrent| torrent.route.key()) {
        *load.entry(key.clone()).or_default() += 1;
    }
    load
}

// `torrent` as the download queue and its holds see it. A seeding torrent
// under a tracker rule that never pauses seeds through the schedule's holds
// until its goal is met.
pub fn queued(settings: &TorrentSettings, rules: &live::Rules, torrent: &Live, now: i64) -> Queued {
    let seeding = torrent.seeding();
    // A torrent under a never-pause rule still owes its tracker while it
    // downloads, and while it seeds with a goal left.
    let rule = torrent.rule(rules);
    let runs_through_schedule = rule.is_some_and(|rule| rule.never_pause)
        && (!seeding
            || status::goal_left(
                &settings.policy(&torrent.record, rule),
                &torrent.counters,
                torrent.have_bytes,
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

// A torrent as the session reports it now.
pub struct Reading {
    engine: Engine,
    // Each file's bytes in hand.
    pub file_have: Vec<u64>,
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
    pub speeds: (u64, u64),
    peers: (u32, u32, u32),
}

// What the job side asks the service to look at: the jobs that changed,
// the ones whose checkpoint is behind them with how many pieces came or
// went since it was written, and the writes a job's intake could not make.
#[derive(Default)]
pub struct Signal {
    pub dirty: Mutex<BTreeSet<JobId>>,
    pub unsaved: Mutex<BTreeMap<JobId, u32>>,
    refused: Mutex<BTreeMap<JobId, String>>,
    // What the first write that found no room wanted, of the torrents
    // whose writes found none since the last settle.
    no_room: Mutex<BTreeMap<JobId, NoRoom>>,
    // The torrents a piece landed for since a wake promised them room
    // beside the other waiters'.
    landed: Mutex<BTreeSet<JobId>>,
    // How many files pieces made whole since the last settle.
    whole: Mutex<BTreeMap<JobId, usize>>,
    // A slot or a rank moved where no pass runs: the next settle runs one.
    pub pass_wanted: AtomicBool,
    wake: Notify,
}

pub fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Signal {
    pub fn mark(&self, job_id: JobId) {
        self.marked(job_id, 0);
    }

    // Looked at by the next settle, with nothing for its checkpoint: no
    // piece came or went.
    pub fn look(&self, job_id: JobId) {
        locked(&self.dirty).insert(job_id);
        self.wake.notify_one();
    }

    // Marked, with its checkpoint due at the settle: what changed is the
    // whole ledger, not a piece.
    fn due(&self, job_id: JobId) {
        self.marked(job_id, CHECKPOINT_PIECES);
    }

    // Its checkpoint due at the settle, with nothing for it to look at: a
    // torrent that completed keeps every piece it has at once, so a crash
    // while it seeds fetches none again (qBittorrent asks for its resume
    // data at finish, `TorrentImpl::handleTorrentFinished`,
    // torrentimpl.cpp:2205).
    pub(super) fn checkpoint_due(&self, job_id: JobId) {
        *locked(&self.unsaved).entry(job_id).or_default() += CHECKPOINT_PIECES;
        self.wake.notify_one();
    }

    fn marked(&self, job_id: JobId, pieces: u32) {
        locked(&self.dirty).insert(job_id);
        *locked(&self.unsaved).entry(job_id).or_default() += pieces;
        self.wake.notify_one();
    }

    pub fn heard(&self, job_id: JobId, heard: IntakeHeard) {
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
            IntakeHeard::NoRoom(wants) => {
                locked(&self.no_room)
                    .entry(job_id)
                    .or_insert(NoRoom::Disk(wants));
                self.mark(job_id);
            }
            IntakeHeard::NotBooked(bytes) => {
                locked(&self.no_room)
                    .entry(job_id)
                    .or_insert(NoRoom::Ledger(bytes));
                self.mark(job_id);
            }
            IntakeHeard::Landed => {
                locked(&self.landed).insert(job_id);
                self.mark(job_id);
            }
        }
    }

    // A refusal heard again: a run that entered upload mode for it.
    fn hold_again(&self, job_id: JobId, no_room: NoRoom) {
        locked(&self.no_room).entry(job_id).or_insert(no_room);
        self.mark(job_id);
    }

    fn take_no_room(&self, job_id: JobId) -> Option<NoRoom> {
        let mut no_room = locked(&self.no_room);
        if no_room.is_empty() {
            return None;
        }
        no_room.remove(&job_id)
    }

    fn take_landed(&self, job_id: JobId) -> bool {
        let mut landed = locked(&self.landed);
        !landed.is_empty() && landed.remove(&job_id)
    }

    // A torrent whose store is gone says nothing more of its writes: what
    // its intake said is dropped, so nothing piles up for it.
    fn forget_writes(&self, job_id: JobId) {
        self.take_no_room(job_id);
        self.take_landed(job_id);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn holds_writes_of(&self, job_id: JobId) -> bool {
        locked(&self.no_room).contains_key(&job_id) || locked(&self.landed).contains(&job_id)
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

    fn take_unsaved(&self) -> Owed<'_> {
        let jobs = std::mem::take(&mut *locked(&self.unsaved))
            .into_keys()
            .collect();
        Owed { signal: self, jobs }
    }

    fn take_unsaved_one(&self, job_id: JobId) -> bool {
        locked(&self.unsaved).remove(&job_id).is_some()
    }

    // The jobs whose checkpoint is [`CHECKPOINT_PIECES`] pieces behind.
    fn take_due(&self) -> Owed<'_> {
        let mut unsaved = locked(&self.unsaved);
        let due: BTreeSet<JobId> = unsaved
            .iter()
            .filter(|(_, pieces)| **pieces >= CHECKPOINT_PIECES)
            .map(|(job_id, _)| *job_id)
            .collect();
        unsaved.retain(|job_id, _| !due.contains(job_id));
        Owed {
            signal: self,
            jobs: due,
        }
    }
}

// Checkpoints taken off the unsaved, owed until [`Owed::queued`]: dropped
// before, as a task a stop aborts drops them part way, they are due
// again, so the next settle or the stop's own checkpoint writes them. One queued twice writes
// nothing the second time.
pub(super) struct Owed<'a> {
    signal: &'a Signal,
    jobs: BTreeSet<JobId>,
}

impl Owed<'_> {
    pub(super) fn jobs(&self) -> &BTreeSet<JobId> {
        &self.jobs
    }

    // Every checkpoint taken is queued.
    pub(super) fn queued(mut self) {
        self.jobs.clear();
    }
}

impl Drop for Owed<'_> {
    fn drop(&mut self) {
        if self.jobs.is_empty() {
            return;
        }
        let mut unsaved = locked(&self.signal.unsaved);
        for &job_id in &self.jobs {
            *unsaved.entry(job_id).or_default() += CHECKPOINT_PIECES;
        }
    }
}

// A test's hold on the load of the stored torrents.
#[cfg(any(test, feature = "test-support"))]
pub struct LoadSeam {
    // Held, the load reads no page.
    held: watch::Sender<bool>,
    // The rows of a page, in place of `LOAD_PAGE`; 0 for that.
    pub page_rows: std::sync::atomic::AtomicUsize,
    // The page read that fails, once, counted from the next as 1; 0 for
    // none.
    pub fail_in: std::sync::atomic::AtomicUsize,
}

#[cfg(any(test, feature = "test-support"))]
impl Default for LoadSeam {
    fn default() -> Self {
        Self {
            held: watch::channel(false).0,
            page_rows: std::sync::atomic::AtomicUsize::new(0),
            fail_in: std::sync::atomic::AtomicUsize::new(0),
        }
    }
}

/// What the service has written so far, and the checkpoints it has read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    pub ledgers: u64,
    pub checkpoints: u64,
    pub checkpoints_read: u64,
}

// Where the restore pass stands: none runs, one runs, or one runs and
// another is to follow it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restores {
    Idle,
    Running,
    Again,
}

// Where the read of the stored torrents at start stands: not begun, under
// way as far as the last job a torrent row had when it began, or ended. A
// page that does not read leaves it under way: the page is read again.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Load {
    Idle,
    Running(Option<JobId>),
    Done,
}

// What moved under a load of the stored torrents, kept only while it runs.
// The load ends under this lock, so what is asked here of whether it runs
// is true of everything else kept here.
#[derive(Default)]
pub struct LoadMarks {
    // The torrents taken out of the live ones: a page read before one went
    // does not bring it back. Kept while the load runs and while a read of
    // a torrent the load had not reached is in flight, which may outlast it.
    pub gone: BTreeSet<JobId>,
    // The reads of torrents the load had not reached that are in flight.
    readers: usize,
    // The torrents being read now, each by one reader, and what tells the
    // others that ask for one that its read has ended.
    reading: HashMap<JobId, watch::Receiver<bool>>,
    // What was asked of the queue's order, in the order asked: it is done
    // once the last stored torrent is read, when every place is known.
    asked: Vec<Asked>,
    // The last job the pages have read: the next page follows it.
    after: Option<JobId>,
    // Why the last page did not read, while it has not: the tick reads it
    // again, and the torrents read so far run meanwhile.
    fault: Option<String>,
    // A read of the page that failed runs.
    resuming: bool,
}

// What was asked of the queue's order while the stored torrents were read.
enum Asked {
    // A torrent added: it goes past every stored torrent.
    Added(JobId),
    // `TorrentService::reorder`.
    Reorder(Vec<JobId>, QueueMoveTarget),
    // `TorrentService::move_many`.
    Moves(Vec<(JobId, QueueMoveTarget)>),
}

/// Why an answer that needs every torrent was not given: the stored
/// torrents are still being read at start. With why, when a read of them
/// failed and is tried again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum NotReady {
    #[error("the stored torrents are still being read; ask again shortly")]
    Loading,
    #[error("the stored torrents did not load and are being read again: {0}")]
    Faulted(String),
}

/// Why a list of torrents was not given.
#[derive(Debug, thiserror::Error)]
pub enum ListError {
    #[error(transparent)]
    NotReady(#[from] NotReady),
    #[error(transparent)]
    State(#[from] StateError),
}

pub struct Inner {
    pub db: Database,
    pub sessions: Sessions,
    pub settings: RwLock<TorrentSettings>,
    // The tracker rules, as the database last held them.
    rules: RwLock<Arc<live::Rules>>,
    pub env: JobEnv,
    pub clock: Arc<dyn Clock>,
    pub live: tokio::sync::Mutex<BTreeMap<JobId, Live>>,
    // How many torrents read as each route, kept by their route slots.
    pub route_loads: live::RouteLoads,
    // Held while a torrent's job is restored, from the check that it waits
    // for one to the job given: one restore of a torrent at a time, and at
    // most RESTORES_AT_ONCE torrents restoring. The blocking restore owns
    // both, so a restore whose awaiting task ends still holds them.
    pub restoring: std::sync::Mutex<HashMap<JobId, Arc<tokio::sync::Mutex<()>>>>,
    pub restore_permits: Arc<tokio::sync::Semaphore>,
    // The pass that restores the jobs of the torrents brought back with
    // their records alone, run off the tick: none, one running, or one
    // running with another asked to follow it.
    pub restores: watch::Sender<Restores>,
    // Set where a torrent comes to wait for its job: a page of the load, a
    // restore held, a resume of one that crashed the daemon. The tick
    // begins a restore pass only when it is set, and takes it.
    restore_wanted: AtomicBool,
    // The load of the stored torrents at start, and what moved under it.
    load: watch::Sender<Load>,
    pub load_marks: Mutex<LoadMarks>,
    // A test's hold on the load.
    #[cfg(any(test, feature = "test-support"))]
    pub load_seam: LoadSeam,
    // The torrents whose work crashed the daemon on its last run: each is
    // kept paused, its job not restored, until a resume takes its marker.
    crash_marks: Mutex<weaver_server_base::runtime::panic_trap::CrashMarks>,
    pub signal: Arc<Signal>,
    ticks: watch::Sender<u64>,
    // The clock as the last tick read it: a tick that reads an earlier one
    // has found it stepped back.
    tick_at_ms: AtomicI64,
    // Counts the waits for a route's session to open, begun off an add or
    // an action, that have ended.
    attaches: watch::Sender<u64>,
    retired: watch::Sender<u64>,
    written: watch::Sender<Written>,
    // The torrents removed whose history row waits for room on the
    // database's disk, as the queue shows them: retiring, with the
    // disk-space wait an NZB job's retiring row shows.
    retiring: Mutex<BTreeMap<JobId, JobInfo>>,
    // The latest history row written for each job whose row is on its way,
    // by turn: a waiter whose row a newer one took the place of is not the
    // latest, and does nothing.
    history_turns: Mutex<HashMap<JobId, u64>>,
    next_history_turn: AtomicU64,
    pub queue: Mutex<Option<SchedulerHandle>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    tick: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // The service hearing the mail no pipeline hears, which a stop ends
    // last: an object out for its work comes back through it.
    hearing: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Rung by a settings change: the tick runs then, not at its next turn.
    settings_changed: Notify,
    // How the torrents still running end while torrents are off: none when
    // they are on, or off with nothing left to end.
    wind_down: watch::Sender<Option<WindDown>>,
    // Held while sessions are turned on or off for the settings or for a
    // wind-down's end, so the two never cross.
    pub turning: tokio::sync::Mutex<()>,
    // Whether there are torrents, as last published.
    presence: watch::Sender<Presence>,
    // Whether any torrent was downloading, as last published: memory goes
    // back as that turns false.
    downloading: AtomicBool,
    // How many memory releases each ending asked for, as a test reads them.
    #[cfg(any(test, feature = "test-support"))]
    pub releases: [std::sync::atomic::AtomicUsize; 2],
    // The torrents and their sessions as last published, for the exporter.
    pub published: watch::Sender<Arc<Published>>,
    // Where each torrent sits in the published torrents, written with
    // them: a publish of some torrents puts theirs in place. Held while the
    // published torrents are written.
    published_at: Mutex<HashMap<JobId, usize>>,
    // Each route's session as last read. The tick reads them, and turning
    // torrents on or off, under the sessions' lock each takes anyway: a
    // publish from an action never waits on a session that is opening.
    sessions_read: Mutex<Vec<SessionSeries>>,
    pub counters: Counters,
    // The place in the download queue the next torrent added takes: past
    // every one kept (libtorrent puts a new torrent last in the queue).
    pub next_queue_position: AtomicI64,
    // What the last pass over the queue left at its end, written under
    // `live`'s lock: a torrent added goes there.
    queue_end: Mutex<QueueEnd>,
    // The queue's order as the last pass worked it out, taken again while
    // no torrent's place moved. Kept under `live`'s lock.
    queue_order: Mutex<policy::QueueOrder>,
    // What the passes did: each take of `live`, walk and write.
    #[cfg(any(test, feature = "test-support"))]
    pub pass_counts: watch::Sender<policy::PassCounts>,
    // Every torrent's row as a list shows it, as last published.
    pub snapshot: snapshot::Snapshot,
    // How many views were built.
    #[cfg(any(test, feature = "test-support"))]
    views_built: std::sync::atomic::AtomicUsize,
    // Held by a test that counts what its own calls build: the tick's loop
    // takes it for each tick, so none runs while it is held.
    #[cfg(any(test, feature = "test-support"))]
    tick_hold: Arc<tokio::sync::Mutex<()>>,
    // The torrents waiting for room on their disks, each by the wait every
    // kind of job shares for its refusal, and how many times a wait began
    // or ended, or a torrent's disk became known.
    pub disk_waits: Mutex<BTreeMap<JobId, DiskWait>>,
    pub disk_waits_moved: watch::Sender<u64>,
    // Torrents whose start their root's disk had no room for wait, by the
    // start rule's wait every kind of job shares: one per root, in
    // `StorageRoot::ALL`'s order.
    pub start_held: Mutex<[weaver_server_disk::operations::start_rule::StartWait; 3]>,
    // Torrents whose disk could not be looked up: each is looked up again
    // at the next reading of a root's disk, which the complete root's
    // ledger is waited on for while any is listed. A torrent leaves at that
    // reading, or when it is gone; at most one entry per live torrent.
    pub relook: Mutex<Relook>,
    // The refusals heard while their run was not in upload mode, heard
    // again once it is: one task per torrent's run waits for that.
    pub entering: Mutex<BTreeMap<JobId, (RunMark, NoRoom)>>,
    // The service stops: a look its stop ended waits for no reading.
    stopping: AtomicBool,
    // The settings an add makes a category it names in, when it names one
    // Weaver does not have (`categories::make_category`). Unset, such an
    // add is refused.
    categories: std::sync::OnceLock<crate::settings::SharedConfig>,
    // How many settles ran: what a test waits on to know one consumed
    // what the intakes said before it.
    #[cfg(any(test, feature = "test-support"))]
    pub settled: watch::Sender<u64>,
    // A settle under way: set before it takes what it settles, cleared
    // before `settled` counts it.
    #[cfg(any(test, feature = "test-support"))]
    pub settling: AtomicBool,
    // How many magnets' fetches were shown ended: what a test waits on to
    // know every fetch a start began has said its end.
    #[cfg(any(test, feature = "test-support"))]
    fetches_ended: watch::Sender<u64>,
    // How many ends of a check were judged (`rejudge_after_check`): what a
    // test waits on to know the judgement a check's end makes was made.
    #[cfg(any(test, feature = "test-support"))]
    pub checks_judged: watch::Sender<u64>,
    // A test's hold on the sweep of idle sessions, once it has read the
    // routes the torrents use and before it reads the sessions, and how
    // many sweeps reached that hold.
    #[cfg(any(test, feature = "test-support"))]
    sweep_held: watch::Sender<bool>,
    #[cfg(any(test, feature = "test-support"))]
    sweeps_held: watch::Sender<u64>,
}

#[derive(Clone)]
pub struct TorrentService {
    pub inner: Arc<Inner>,
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
                rules: RwLock::new(Arc::default()),
                env,
                clock,
                live: tokio::sync::Mutex::new(BTreeMap::new()),
                route_loads: live::RouteLoads::default(),
                restoring: std::sync::Mutex::new(HashMap::new()),
                restore_permits: Arc::new(tokio::sync::Semaphore::new(RESTORES_AT_ONCE)),
                restores: watch::channel(Restores::Idle).0,
                restore_wanted: AtomicBool::new(false),
                load: watch::channel(Load::Idle).0,
                load_marks: Mutex::default(),
                #[cfg(any(test, feature = "test-support"))]
                load_seam: LoadSeam::default(),
                crash_marks: Mutex::default(),
                signal: Arc::new(Signal::default()),
                ticks: watch::channel(0).0,
                tick_at_ms: AtomicI64::new(i64::MIN),
                attaches: watch::channel(0).0,
                retired: watch::channel(0).0,
                written: watch::channel(Written::default()).0,
                retiring: Mutex::default(),
                history_turns: Mutex::default(),
                next_history_turn: AtomicU64::new(0),
                queue: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
                tick: Mutex::new(None),
                hearing: Mutex::new(None),
                settings_changed: Notify::new(),
                wind_down: watch::channel(None).0,
                turning: tokio::sync::Mutex::new(()),
                presence: watch::channel(Presence::default()).0,
                downloading: AtomicBool::new(false),
                #[cfg(any(test, feature = "test-support"))]
                releases: Default::default(),
                published: watch::channel(Arc::default()).0,
                published_at: Mutex::default(),
                sessions_read: Mutex::default(),
                counters: Counters::default(),
                next_queue_position: AtomicI64::new(0),
                // An empty queue: every slot free.
                queue_end: Mutex::new(QueueEnd {
                    slots: status::Slots::new(max_active),
                    last: None,
                }),
                queue_order: Mutex::default(),
                #[cfg(any(test, feature = "test-support"))]
                pass_counts: watch::channel(policy::PassCounts::default()).0,
                snapshot: snapshot::Snapshot::default(),
                #[cfg(any(test, feature = "test-support"))]
                views_built: std::sync::atomic::AtomicUsize::new(0),
                #[cfg(any(test, feature = "test-support"))]
                tick_hold: Arc::default(),
                disk_waits: Mutex::default(),
                disk_waits_moved: watch::channel(0).0,
                #[cfg(any(test, feature = "test-support"))]
                settled: watch::channel(0).0,
                #[cfg(any(test, feature = "test-support"))]
                settling: AtomicBool::new(false),
                #[cfg(any(test, feature = "test-support"))]
                fetches_ended: watch::channel(0).0,
                #[cfg(any(test, feature = "test-support"))]
                checks_judged: watch::channel(0).0,
                #[cfg(any(test, feature = "test-support"))]
                sweep_held: watch::channel(false).0,
                #[cfg(any(test, feature = "test-support"))]
                sweeps_held: watch::channel(0).0,
                start_held: Mutex::default(),
                relook: Mutex::default(),
                entering: Mutex::default(),
                stopping: AtomicBool::new(false),
                categories: std::sync::OnceLock::new(),
            }),
        }
    }

    /// The settings an add that names a category Weaver does not have makes
    /// that category in, as qBittorrent's add does. Set once; a later call
    /// changes nothing.
    pub fn make_categories_in(&self, config: crate::settings::SharedConfig) {
        let _ = self.inner.categories.set(config);
    }

    // The torrents that crashed the daemon on its last run, read at start
    // before any torrent's job is restored.
    pub fn park_crashed(&self, marks: weaver_server_base::runtime::panic_trap::CrashMarks) {
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

    pub fn settings(&self) -> TorrentSettings {
        self.inner
            .settings
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Whether a new torrent is taken: torrents are on and their sessions
    /// open. Off, a wind-down's included, an add is refused as `Off`.
    pub fn takes_new(&self) -> bool {
        self.inner.sessions.on()
            && self
                .inner
                .settings
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .torrent
                .enabled
    }

    fn rules(&self) -> Arc<live::Rules> {
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
        self.rules()
            .rule_for(trackers.into_iter().map(|tracker| tracker.url.as_str()))
            .map(|rule| rule.id)
    }

    // The holds the queue has in force.
    fn holds(&self) -> HoldSet {
        self.queue()
            .map_or_else(HoldSet::default, |queue| queue.state().holds())
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
        self.inner.stopping.store(true, Ordering::Release);
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
        for torrent in self.inner.live.lock().await.values_mut() {
            if let Some(attaching) = torrent.attaching.take() {
                attaching.abort();
            }
        }
        self.stop_tick();
        // The last reading of every torrent, and what of its ledger and
        // peers changed since the last write, before the sessions close
        // (qBittorrent saves only what changed as it exits, libtorrent
        // `torrent::write_resume_data` reads the peers off the torrent).
        let now = self.inner.clock.unix_ms();
        self.read(now, true).await;
        self.inner.sessions.stop_all().await;
        // Every tee's checkpoint goes at once, whether or not it was due,
        // once every object of its job out for its work is back.
        let tees: Vec<(JobId, Arc<Tee>)> = {
            let live = self.inner.live.lock().await;
            live.iter()
                .filter_map(|(&job_id, torrent)| Some((job_id, torrent.store.as_ref()?.tee()?)))
                .collect()
        };
        for (job_id, tee) in tees {
            self.checkpoint_tee(job_id, tee, true).await;
        }
        if let Some(hearing) = locked(&self.inner.hearing).take() {
            hearing.abort();
        }
        let unsaved = self.inner.signal.take_unsaved();
        self.save_checkpoints(unsaved.jobs().clone()).await;
        unsaved.queued();
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
        // The ended tasks go when the list is full, not at every keep: a
        // start keeps a task for each torrent it resumes, and a sweep per
        // keep would cost the square of them. After a sweep the list has
        // room for as many again as it holds, so a sweep comes only after
        // as many keeps as it reads.
        if tasks.len() == tasks.capacity() {
            tasks.retain(|task| !task.is_finished());
            let held = tasks.len().max(1);
            tasks.reserve(held);
        }
        tasks.push(task);
    }

    /// Every torrent's row as a list shows it, as last published.
    pub fn snapshot(&self) -> &snapshot::Snapshot {
        &self.inner.snapshot
    }

    /// The time on the service's clock, in milliseconds since the epoch:
    /// what its views' readings are stamped with.
    pub fn now_ms(&self) -> i64 {
        self.inner.clock.unix_ms()
    }

    /// Whether every torrent's row is in the snapshot: every stored torrent
    /// is here, and a publish since put them in.
    pub fn listing_ready(&self) -> Result<(), NotReady> {
        self.whole()?;
        if self.inner.snapshot.ready() {
            Ok(())
        } else {
            Err(NotReady::Loading)
        }
    }

    /// How many torrent views were built so far.
    #[cfg(any(test, feature = "test-support"))]
    pub fn views_built(&self) -> usize {
        self.inner.views_built.load(Ordering::Relaxed)
    }

    /// Holds the background tick, once one running ends, until the guard
    /// is dropped: what a test's own calls build is then all it counts.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn hold_tick(&self) -> tokio::sync::OwnedMutexGuard<()> {
        Arc::clone(&self.inner.tick_hold).lock_owned().await
    }

    /// Counts the ticks run so far.
    pub fn ticks(&self) -> watch::Receiver<u64> {
        self.inner.ticks.subscribe()
    }

    /// Counts the waits for a route's session to open, begun off an add or
    /// an action, that have ended: the torrent is in its session then, or
    /// held for why the session did not open.
    pub fn attaches(&self) -> watch::Receiver<u64> {
        self.inner.attaches.subscribe()
    }

    /// Whether `job_id` waits on its route's session to open, off the add
    /// or action that asked for it.
    pub async fn attaching(&self, job_id: JobId) -> bool {
        self.inner
            .live
            .lock()
            .await
            .get(&job_id)
            .is_some_and(|torrent| torrent.attaching.is_some())
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

    // Whether the stored torrents are still being read at start.
    pub fn loading(&self) -> bool {
        matches!(*self.inner.load.borrow(), Load::Running(_))
    }

    // Why the load's last page did not read, while it has not.
    fn load_fault(&self) -> Option<String> {
        locked(&self.inner.load_marks).fault.clone()
    }

    // Whether only some of the stored torrents are here and the rest are on
    // their way: no torrent takes a download turn among them. A load whose
    // page failed is not that: the torrents read so far run until it reads.
    fn partial(&self) -> bool {
        self.loading() && self.load_fault().is_none()
    }

    /// Whether every stored torrent is here: an answer or an action over
    /// every torrent is true only then, and says so otherwise.
    pub fn whole(&self) -> Result<(), NotReady> {
        if !self.loading() {
            return Ok(());
        }
        Err(self
            .load_fault()
            .map_or(NotReady::Loading, NotReady::Faulted))
    }

    // `job_id` taken out of the live torrents. Under a load, and while a
    // read begun under it is in flight, a page read before it went does
    // not bring it back.
    fn unlive(&self, live: &mut BTreeMap<JobId, Live>, job_id: JobId) -> Option<Live> {
        let torrent = live.remove(&job_id)?;
        let mut marks = locked(&self.inner.load_marks);
        if self.loading() || marks.readers > 0 {
            marks.gone.insert(job_id);
        }
        Some(torrent)
    }

    /// Hold the load of the stored torrents before its next page, or let
    /// it go.
    #[cfg(any(test, feature = "test-support"))]
    pub fn hold_load(&self, held: bool) {
        self.inner.load_seam.held.send_replace(held);
    }

    /// Start as the server starts: the stored torrents are read on a task
    /// of the service's, and this returns at once.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn start_loading(&self) -> Result<(), String> {
        self.start_seated().await
    }

    /// Once `count` fetches of magnets have ended and shown their end.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn fetches_ended(&self, count: u64) {
        let _ = self
            .inner
            .fetches_ended
            .subscribe()
            .wait_for(|ended| *ended >= count)
            .await;
    }

    /// Once every stored torrent is read, and its row is in the snapshot.
    #[cfg(any(test, feature = "test-support"))]
    pub async fn loaded(&self) {
        let mut revision = self.inner.snapshot.subscribe();
        let _ = self
            .inner
            .load
            .subscribe()
            .wait_for(|load| matches!(load, Load::Done))
            .await;
        while !self.inner.snapshot.ready() {
            if revision.changed().await.is_err() {
                break;
            }
        }
    }

    pub async fn with_torrent<T>(
        &self,
        job_id: JobId,
        change: impl FnOnce(&mut Live) -> Result<T, ActionError>,
    ) -> Result<T, ActionError> {
        self.ensure(&[job_id]).await;
        let mut live = self.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
        change(torrent)
    }

    // Run `op` against the database off the runtime.
    pub(super) async fn db<T: Send + 'static>(
        &self,
        op: impl FnOnce(&Database) -> Result<T, StateError> + Send + 'static,
    ) -> Result<T, StateError> {
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || op(&db))
            .await
            .map_err(|error| StateError::Database(error.to_string()))
            .and_then(|result| result)
    }

    // What the operator asks of the torrent, written under its lock with
    // no other column, and the ledger and peers as they stand beside it.
    pub async fn set_intent(
        &self,
        job_id: JobId,
        intent: TorrentIntent,
    ) -> Result<(), ActionError> {
        let now = self.inner.clock.unix_ms();
        let landing = {
            self.ensure(&[job_id]).await;
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            self.db(move |db| db.update_torrent_intent(job_id, intent))
                .await
                .map_err(|error| ActionError::State(state_error(error)))?;
            self.intend(torrent, intent, now)
        };
        let _ = self.landed(landing).await;
        Ok(())
    }

    // The intent written for the torrent, taken in memory: its ledger
    // settled and queued to be kept beside it.
    fn intend(&self, torrent: &mut Live, intent: TorrentIntent, now: i64) -> Option<Landing> {
        torrent.settle(now);
        // The operator's word ends a wind-down's hold on it.
        torrent.record.kept = KeptIntent::Intent(intent);
        if let Some(tee) = torrent.store.as_ref().and_then(|store| store.tee()) {
            tee.hold_own_clock(intent == TorrentIntent::Paused);
        }
        self.queue_resume(torrent, now, true)
    }

    // Stop the torrent for good, as its goal or a refusal does: its intent
    // is written, and the ledger and peers as they stand, queued.
    pub async fn stop_torrent(&self, torrent: &mut Live, now: i64) -> Option<Landing> {
        let job_id = torrent.record.job_id;
        torrent.settle(now);
        torrent.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
        if let Err(error) = self
            .db(move |db| db.update_torrent_intent(job_id, TorrentIntent::Stopped))
            .await
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

    // Give back the memory a download that ended left, by the release a
    // finished NZB job asks for, off the runtime: once per `why`, never per
    // piece or peer.
    pub(super) fn release_memory(&self, why: MemoryRelease) {
        #[cfg(any(test, feature = "test-support"))]
        self.inner.releases[why as usize].fetch_add(1, Ordering::Relaxed);
        let _ = why;
        weaver_server_base::runtime::thread_release::release_job_completion_memory_off_runtime();
    }

    // Whether the torrents went from downloading to none downloading at the
    // walk that found `downloading`: memory goes back then, as the pipeline
    // gives it back as it settles. While the pipeline still drives its own
    // downloads it does not, as the pipeline's settle does not: that settle
    // gives it all back once it comes.
    pub(super) fn note_downloading(&self, downloading: bool) {
        let was = self.inner.downloading.swap(downloading, Ordering::AcqRel);
        if was && !downloading && !self.queue().is_some_and(|queue| queue.pipeline_driving()) {
            self.release_memory(MemoryRelease::Idle);
        }
    }
}

// Why a torrent's memory goes back.
#[derive(Debug, Clone, Copy)]
pub enum MemoryRelease {
    // A torrent's download ended: it completed, or left short of complete.
    Ended = 0,
    // The last torrent downloading stopped downloading.
    Idle = 1,
}

// Whether the torrent's files went where its client asked, where they may
// be already.
fn placed(record: &TorrentRecord) -> bool {
    record.save_path.is_some() || record.files_dir.is_some()
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
async fn give_back(dir: &str, job_id: JobId) {
    let path = PathBuf::from(dir);
    let removed = tokio::task::spawn_blocking(move || {
        crate::jobs::working_dir::remove_emptied_output_dir(&path, job_id)
    })
    .await;
    match removed {
        Ok(Ok(true)) => {}
        Ok(Ok(false)) => tracing::debug!(dir, "a claimed torrent directory holds files, and stays"),
        Ok(Err(error)) => {
            tracing::debug!(dir, error = %error, "a claimed torrent directory stays");
        }
        Err(error) => tracing::debug!(dir, error = %error, "a claimed torrent directory stays"),
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
        progress_percent: view.progress * 100.0,
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

// Which of the files `levels` sets aside: those not downloaded.
fn ignored_of(levels: &[FilePriority]) -> Vec<bool> {
    levels
        .iter()
        .map(|level| *level == FilePriority::Ignored)
        .collect()
}

// The directory the torrent's files are in: where they were placed, or else
// the directory claimed for it.
pub fn files_dir(torrent: &Live) -> PathBuf {
    PathBuf::from(
        torrent
            .record
            .files_dir
            .clone()
            .or_else(|| torrent.record.storage_dir.clone())
            .unwrap_or_default(),
    )
}

pub use weaver_server_pipeline::torrents::service::unpacking_dir;

// Test seam: a removal's mark waits, once, until the test lets it go,
// saying first that it has reached that point.
#[cfg(any(test, feature = "test-support"))]
pub static MARK_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: a move, shown moving, waits once before it holds its
// destination, until the test lets it go, saying first that it has reached
// that point.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: the write that ends a failed move of placed files, of what the
// torrent made as the move left it, is refused once.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_UNDO_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: holds a move of placed files that forgot the torrent's pieces,
// before any file moves, until the test lets it go, saying first that it has
// reached that point.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_FORGOT_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: run with the name each of a torrent's unpacked files is about
// to move to, the look having found it free, right before the move.
#[cfg(any(test, feature = "test-support"))]
pub static OUTPUT_MEETS: Mutex<Option<OutputMeets>> = Mutex::new(None);
#[cfg(any(test, feature = "test-support"))]
pub(crate) type OutputMeets = Box<dyn FnMut(&Path) + Send>;

// Test seam: the next placing of a torrent's unpacked files panics once
// its files have moved, before its row is written.
#[cfg(any(test, feature = "test-support"))]
pub static PLACE_PANICS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of a torrent's unpacked names to its row is
// refused.
#[cfg(any(test, feature = "test-support"))]
pub static KEEP_FAILS: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of a torrent's completion is refused.
#[cfg(any(test, feature = "test-support"))]
pub static COMPLETION_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of the mark that a torrent's unpacking ended
// short is refused.
#[cfg(any(test, feature = "test-support"))]
pub static UNPACK_OVER_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: run once a move of a torrent's placed files has moved them,
// right before the write of the move to its row.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_RECORD_MEETS: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

// Test seam: run once a move of a torrent's placed files has written the
// move to its row, the files still held by the move.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_RECORDED_MEETS: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

// Test seam: run as a move of a torrent's placed files begins, after its
// look at the new place and before any file moves.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_BEGIN_MEETS: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

// Test seam: the next move of a torrent's placed files ends as a crash
// would end it, its files moved and its row not written.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_RECORD_DIES: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: holds a move of placed files that will check the torrent
// again, once the move is over and before any piece is forgotten, until the
// test lets it go, saying first that it has reached that point.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_CHECK_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: the next check a move owes the torrent once it is over is not
// begun, as when its pieces could not be forgotten.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_CHECK_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of a move of a torrent's placed files to its
// row is refused.
#[cfg(any(test, feature = "test-support"))]
pub static MOVE_RECORD_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: a write of what a torrent holds is refused, 1 once its files
// moved (`sync_held`), 2 before they move (`hold_destination`).
#[cfg(any(test, feature = "test-support"))]
pub static SYNC_HELD_FAILS: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

// Why a torrent's unpacked files are not all beside its own, and where
// they are.
fn unplaced(work: &Path, error: &impl std::fmt::Display) -> String {
    format!(
        "the unpacked files could not be placed beside the torrent's files ({error}); they are kept in {}",
        work.display()
    )
}

// The bytes in hand of the files that go to disk: the progress over the
// torrent's total, which leaves padding out as the session's own does
// (libtorrent `file_progress::total_on_disk`).
pub fn on_disk(layout: Option<&PieceLayout>, file_have: &[u64]) -> u64 {
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

// A torrent as a job in `status`.
pub fn report(view: TorrentView, status: JobStatus, now_ms: i64) -> JobReport {
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
        // A reason the service states goes by its key, a code the
        // downloads list puts in words; free words go as they are.
        wait_reason: view
            .stated
            .map(|stated| stated.key().to_owned())
            .or(view.detail),
        created_at_ms: view.added_at_ms,
        metadata: view.metadata,
        shortfall: None,
        disk_need_bytes: view.disk_need_bytes,
        disk_free_bytes: view.disk_free_bytes,
    }
}
