// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrent client: torrents become graph jobs fed by a session on their
//! route, are read back as one state with its reason, and leave as history.

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
use crate::clock::{Clock, SystemClock};
use crate::jobs::JobId;
use crate::jobs::handle::{HoldSet, JobInfo, SchedulerHandle};
use crate::jobs::model::{JobStatus, Protocol};
use crate::jobs::phase_progress::{JobPhase, JobPhaseProgress};
use crate::jobs::report::JobReport;
use crate::operations::disk::{CapacityReader, StorageCapacity};
use crate::persistence::Database;
use crate::pipeline::ExtractionLimits;
use crate::pipeline::graph::engine::{DecodePool, Outside, RunMail, Said};
use crate::pipeline::graph::status::RepairSlots;
use crate::pipeline::graph::status::{Delivery, Failure};
use crate::pipeline::graph::window::ScratchLedger;
use crate::settings::model::{TrackerCertificate, TrackerRule, tracker_rule_for};
use crate::settings::{Config, TorrentEncryption};
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
    FilePriority, KeptIntent, NamedBy, TorrentCounters, TorrentFile, TorrentIntent, TorrentRecord,
    TorrentState, TrackerEntry,
};

pub mod fingerprint;
pub mod listing;
mod scripts;
pub use scripts::ScriptDirs;
mod batch;
mod checkpoints;
mod completion;
mod disk_room;
mod intake;
mod live;
mod moves;
mod policy;
mod queue;
mod restore;
mod retire;
mod settings;
#[cfg(test)]
mod tests;
mod tick;
mod trackers;
pub mod updates;
mod views;
use checkpoints::Landing;
pub use disk_room::disk_warning;
use disk_room::{DiskWait, NoRoom, Relook, RunMark};
use intake::client_of;
pub use intake::{AddError, AddRequest, AddSource, Placement};
use live::{Live, Timed};
#[cfg(test)]
pub(in crate::torrents) use moves::{Listed, Moved, NotMoved, move_listed};
use queue::QueueEnd;
pub(crate) use retire::{ListedRemoval, LiveHeld, finish_retiring, remove_history_files};
#[cfg(test)]
pub(in crate::torrents) use retire::{delete_written, deletion, listed_files};
pub use settings::{
    DHT, ENCRYPTION, Field, INACTIVE_GOAL, LSD, PEX, Presence, RATIO_GOAL, SCOPE,
    SEED_TIME_COUNTING, SEED_TIME_GOAL, SEEDING_ACTION, SEEDING_MODE, ScheduleSeeding,
    SeedingLevels, TorrentSettings, UNPACK, UPLOAD_CAP, WindDown,
};
pub use tick::TorrentRoute;
pub use views::{PeerKinds, TorrentLeg, TorrentPieces, TorrentSupport, TorrentView};

// Why a magnet with no trackers is turned away on a route with no DHT.
const NEEDS_DHT: &str = "a magnet with no trackers needs DHT, which this route does not carry";

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

// What a torrent's session holds its peers to under its tracker rule:
// whether it looks for them on the DHT, on the local network and by peer
// exchange, each switch on its own, and the encryption
// its connections are made under over the session's. A change of any takes
// the torrent through its session again.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct PeerTerms {
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
            disable_dht: off(&DHT),
            disable_pex: off(&PEX),
            disable_lsd: off(&LSD),
            encryption: settings.levels(&ENCRYPTION, None, None, rule).resolve(),
        }
    }

    // `options` under these terms.
    fn apply(self, options: &mut AddTorrentOptions) {
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

/// What torrent jobs are built with.
#[derive(Clone)]
pub struct JobEnv {
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) free_disk: CapacityReader,
    // The disks every job's writes are judged on, the pipeline's own: a
    // torrent whose write found no room waits on them for room as a usenet
    // job does. None with no pipeline, where a torrent in upload mode
    // tries again by itself.
    pub(crate) capacity: Option<Arc<StorageCapacity>>,
    // Where every torrent's pieces are written: the volumes' writers the
    // pipeline writes through too.
    pub(crate) disk: Arc<crate::torrents::writes::TorrentDisk>,
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
        crashed: crate::runtime::panic_trap::CrashMarks,
    ) -> Result<Self, String> {
        let routes = NetworkRoutes::new(Arc::clone(proxies), ROUTE_TIMEOUT);
        let sessions = Sessions::new(Arc::new(routes), SessionEnv::of(config), db.clone());
        let service = Self::new(
            db,
            sessions,
            TorrentSettings::of(config),
            JobEnv::of_pipeline(pipeline, writers, buffers),
            Arc::new(SystemClock),
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
fn hashes_of(record: &TorrentRecord) -> InfoHashes {
    InfoHashes {
        v1: record.infohash_v1.map(Id20::new),
        v2: record.infohash_v2.map(Id32::new),
    }
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

// What the job side asks the service to look at: the jobs that changed,
// the ones whose checkpoint is behind them with how many pieces came or
// went since it was written, and the writes a job's intake could not make.
#[derive(Default)]
pub(super) struct Signal {
    dirty: Mutex<BTreeSet<JobId>>,
    unsaved: Mutex<BTreeMap<JobId, u32>>,
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

    #[cfg(test)]
    fn holds_writes_of(&self, job_id: JobId) -> bool {
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
    // Counts the waiters on a history row kept for room that have ended.
    #[cfg(test)]
    history_waits: watch::Sender<u64>,
    queue: Mutex<Option<SchedulerHandle>>,
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
    turning: tokio::sync::Mutex<()>,
    // Whether there are torrents, as last published.
    presence: watch::Sender<Presence>,
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
    // The queue's order as the last pass worked it out, taken again while
    // no torrent's place moved. Kept under `live`'s lock.
    queue_order: Mutex<policy::QueueOrder>,
    // What the passes did: each take of `live`, walk and write.
    #[cfg(test)]
    pass_counts: watch::Sender<policy::PassCounts>,
    // The readings of every torrent's views their followers share.
    readings: updates::Readings,
    // The torrents waiting for room on their disks, each by the wait every
    // kind of job shares for its refusal, and how many times a wait began
    // or ended, or a torrent's disk became known.
    disk_waits: Mutex<BTreeMap<JobId, DiskWait>>,
    disk_waits_moved: watch::Sender<u64>,
    // Torrents that never downloaded are kept from starting for their
    // root's disk, by the start check every kind of job shares: one latch
    // per root, in `StorageRoot::ALL`'s order.
    start_held: Mutex<[bool; 3]>,
    // Torrents whose disk could not be looked up: each is looked up again
    // at the next reading of a root's disk, which the complete root's
    // ledger is waited on for while any is listed. A torrent leaves at that
    // reading, or when it is gone; at most one entry per live torrent.
    relook: Mutex<Relook>,
    // The refusals heard while their run was not in upload mode, heard
    // again once it is: one task per torrent's run waits for that.
    entering: Mutex<BTreeMap<JobId, (RunMark, NoRoom)>>,
    // The service stops: a look its stop ended waits for no reading.
    stopping: AtomicBool,
    // How many settles ran: what a test waits on to know one consumed
    // what the intakes said before it.
    #[cfg(test)]
    settled: watch::Sender<u64>,
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
                attaches: watch::channel(0).0,
                retired: watch::channel(0).0,
                written: watch::channel(Written::default()).0,
                retiring: Mutex::default(),
                history_turns: Mutex::default(),
                next_history_turn: AtomicU64::new(0),
                #[cfg(test)]
                history_waits: watch::channel(0).0,
                queue: Mutex::new(None),
                tasks: Mutex::new(Vec::new()),
                tick: Mutex::new(None),
                hearing: Mutex::new(None),
                settings_changed: Notify::new(),
                wind_down: watch::channel(None).0,
                turning: tokio::sync::Mutex::new(()),
                presence: watch::channel(Presence::default()).0,
                published: watch::channel(Arc::default()).0,
                sessions_read: Mutex::default(),
                counters: Counters::default(),
                next_queue_position: AtomicI64::new(0),
                // An empty queue: every slot free.
                queue_end: Mutex::new(QueueEnd {
                    slots: status::Slots::new(max_active),
                    last: None,
                }),
                queue_order: Mutex::default(),
                #[cfg(test)]
                pass_counts: watch::channel(policy::PassCounts::default()).0,
                readings: updates::Readings::default(),
                disk_waits: Mutex::default(),
                disk_waits_moved: watch::channel(0).0,
                #[cfg(test)]
                settled: watch::channel(0).0,
                start_held: Mutex::new([false; 3]),
                relook: Mutex::default(),
                entering: Mutex::default(),
                stopping: AtomicBool::new(false),
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
        let now = self.inner.clock.unix_ms();
        let landing = self
            .with_torrent(job_id, |torrent| {
                self.inner
                    .db
                    .update_torrent_intent(job_id, intent)
                    .map_err(|error| ActionError::State(state_error(error)))?;
                Ok(self.intend(torrent, intent, now))
            })
            .await?;
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
    fn stop_torrent(&self, torrent: &mut Live, now: i64) -> Option<Landing> {
        let job_id = torrent.record.job_id;
        torrent.settle(now);
        torrent.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
        if let Err(error) = self
            .inner
            .db
            .update_torrent_intent(job_id, TorrentIntent::Stopped)
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

// Which of the files `levels` sets aside: those not downloaded.
fn ignored_of(levels: &[FilePriority]) -> Vec<bool> {
    levels
        .iter()
        .map(|level| *level == FilePriority::Ignored)
        .collect()
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

// The directory a torrent's unpacking works in, under `work`, marked the
// torrent's job's as an NZB job's working directory is: `torrent-<id>`, or
// the job's own suffixed name when another job's marker holds that one.
// Found by its marker first, so the name a job's folder took is the one it
// keeps (`own_named_dir`).
pub(super) fn unpacking_dir(work: &Path, job_id: JobId) -> PathBuf {
    crate::jobs::working_dir::own_named_dir(work, &format!("torrent-{}", job_id.0), job_id)
}

// Test seam: a removal's mark waits, once, until the test lets it go,
// saying first that it has reached that point.
#[cfg(test)]
pub(crate) static MARK_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: a move, shown moving, waits once before it holds its
// destination, until the test lets it go, saying first that it has reached
// that point.
#[cfg(test)]
pub(crate) static MOVE_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: the write that ends a failed move of placed files, of what the
// torrent made as the move left it, is refused once.
#[cfg(test)]
pub(crate) static MOVE_UNDO_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: holds a move of placed files that forgot the torrent's pieces,
// before any file moves, until the test lets it go, saying first that it has
// reached that point.
#[cfg(test)]
pub(crate) static MOVE_FORGOT_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: run with the name each of a torrent's unpacked files is about
// to move to, the look having found it free, right before the move.
#[cfg(test)]
pub(crate) static OUTPUT_MEETS: Mutex<Option<OutputMeets>> = Mutex::new(None);
#[cfg(test)]
pub(crate) type OutputMeets = Box<dyn FnMut(&Path) + Send>;

// Test seam: the next placing of a torrent's unpacked files panics once
// its files have moved, before its row is written.
#[cfg(test)]
pub(crate) static PLACE_PANICS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of a torrent's unpacked names to its row is
// refused.
#[cfg(test)]
pub(crate) static KEEP_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: run once a move of a torrent's placed files has moved them,
// right before the write of the move to its row.
#[cfg(test)]
pub(crate) static MOVE_RECORD_MEETS: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

// Test seam: run as a move of a torrent's placed files begins, after its
// look at the new place and before any file moves.
#[cfg(test)]
pub(crate) static MOVE_BEGIN_MEETS: Mutex<Option<Box<dyn FnOnce() + Send>>> = Mutex::new(None);

// Test seam: the next move of a torrent's placed files ends as a crash
// would end it, its files moved and its row not written.
#[cfg(test)]
pub(crate) static MOVE_RECORD_DIES: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: holds a move of placed files that will check the torrent
// again, once the move is over and before any piece is forgotten, until the
// test lets it go, saying first that it has reached that point.
#[cfg(test)]
pub(crate) static MOVE_CHECK_HOLD: Mutex<
    Option<(
        tokio::sync::oneshot::Sender<()>,
        tokio::sync::oneshot::Receiver<()>,
    )>,
> = Mutex::new(None);

// Test seam: the next check a move owes the torrent once it is over is not
// begun, as when its pieces could not be forgotten.
#[cfg(test)]
pub(crate) static MOVE_CHECK_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: the next write of a move of a torrent's placed files to its
// row is refused.
#[cfg(test)]
pub(crate) static MOVE_RECORD_FAILS: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

// Test seam: a write of what a torrent holds is refused, 1 once its files
// moved (`sync_held`), 2 before they move (`hold_destination`).
#[cfg(test)]
pub(crate) static SYNC_HELD_FAILS: std::sync::atomic::AtomicU8 =
    std::sync::atomic::AtomicU8::new(0);

// Why a torrent's unpacked files are not all beside its own, and where
// they are.
fn unplaced(work: &Path, error: &impl std::fmt::Display) -> String {
    format!(
        "the unpacked files could not be placed beside the torrent's files ({error}); they are kept in {}",
        work.display()
    )
}

// How a torrent's job ends when unpacking left it short of what it would
// deliver, from its own files it has whole, `own`, and the files unpacking
// placed beside them, `unpacked` (AGENTS.md, "Job status: Partial"): as a
// graph job's delivery ends (`Delivery::status`), the one judgement both
// kinds of job take. Partial when one of them is a usable end file, failed
// for `why` when none is. An archive volume, a split part and recovery data
// are never usable files, whole or not, and an own file that is furniture
// is no file of the delivery, as an NZB job's root furniture is not.
fn short_status(own: &[String], unpacked: &[String], why: &str) -> JobStatus {
    let failure = Failure {
        objects: Vec::new(),
        message: why.to_owned(),
        rejection: None,
        from_feed: false,
    };
    Delivery::of_whole_files(own.iter().cloned(), unpacked.iter().cloned()).status(Some(&failure))
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
        // A reason the service states goes by its key, a code the
        // downloads list puts in words; free words go as they are.
        wait_reason: view
            .stated
            .map(|stated| stated.key().to_owned())
            .or(view.detail),
        created_at_ms: view.added_at_ms,
        metadata: view.metadata,
        shortfall: None,
    }
}
