// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents as the Prometheus exporter reads them.
//!
//! The per-torrent and per-tracker figures are taken where the service
//! already reads every torrent, each time it publishes them to the queue;
//! the per-route figures are read by the tick, under the sessions' lock it
//! already takes. Both are kept whole behind one `Arc`: a scrape clones the
//! `Arc` and reads nothing else, so it takes no session lock and asks the
//! engine nothing. The few events counted here are relaxed atomic adds where
//! they happen, none of them on a peer, piece or socket path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub use weaver_torrent::session_stats::snapshot::UtpStatsSnapshot;
use weaver_torrent::{FetchPhase, Session};

use crate::jobs::JobId;
use crate::settings::Seeding;
use crate::settings::model::tracker_host;
use crate::torrents::service::TorrentPieces;
use crate::torrents::{TorrentCounters, TorrentState, TorrentTracker, TrackerStatus, status};

/// Every state a torrent can be in, in the order they are exported.
pub const TORRENT_STATES: [TorrentState; 11] = [
    TorrentState::Queued,
    TorrentState::CheckingResumeData,
    TorrentState::FetchingMetadata,
    TorrentState::CheckingFiles,
    TorrentState::Downloading,
    TorrentState::Stalled,
    TorrentState::Seeding,
    TorrentState::Paused,
    TorrentState::Stopped,
    TorrentState::Moving,
    TorrentState::NeedsCertificate,
];

/// Every phase of a magnet's metadata fetch.
pub const FETCH_PHASES: [FetchPhase; 3] = [
    FetchPhase::Trackers,
    FetchPhase::Discovery,
    FetchPhase::Waiting,
];

pub const fn fetch_phase_str(phase: FetchPhase) -> &'static str {
    match phase {
        FetchPhase::Trackers => "trackers",
        FetchPhase::Discovery => "discovery",
        FetchPhase::Waiting => "waiting",
    }
}

/// Every status announcing to a tracker can stand at.
pub const TRACKER_STATUSES: [TrackerStatus; 6] = [
    TrackerStatus::NotContacted,
    TrackerStatus::Working,
    TrackerStatus::NotWorking,
    TrackerStatus::TrackerError,
    TrackerStatus::Unreachable,
    TrackerStatus::Disabled,
];

pub const fn tracker_status_str(status: TrackerStatus) -> &'static str {
    match status {
        TrackerStatus::NotContacted => "not_contacted",
        TrackerStatus::Working => "working",
        TrackerStatus::NotWorking => "not_working",
        TrackerStatus::TrackerError => "tracker_error",
        TrackerStatus::Unreachable => "unreachable",
        TrackerStatus::Disabled => "disabled",
    }
}

/// What happened to a torrent as a whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TorrentEvent {
    Added,
    Completed,
    Removed,
    Failed,
}

impl TorrentEvent {
    pub const ALL: [Self; 4] = [Self::Added, Self::Completed, Self::Removed, Self::Failed];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Added => "added",
            Self::Completed => "completed",
            Self::Removed => "removed",
            Self::Failed => "failed",
        }
    }
}

/// How a magnet's metadata fetch ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchOutcome {
    /// The metainfo came and the torrent took it.
    Resolved,
    /// The fetch, or the metainfo it gave, failed the torrent.
    Failed,
    /// The route gave no session to fetch on; it fetches again later.
    NoSession,
    /// A pause or a removal ended it first.
    Cancelled,
}

impl FetchOutcome {
    pub const ALL: [Self; 4] = [
        Self::Resolved,
        Self::Failed,
        Self::NoSession,
        Self::Cancelled,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Resolved => "resolved",
            Self::Failed => "failed",
            Self::NoSession => "no_session",
            Self::Cancelled => "cancelled",
        }
    }
}

/// The service's event counters.
#[derive(Debug, Default)]
pub(crate) struct Counters {
    events: [AtomicU64; 4],
    fetches: [AtomicU64; 4],
}

impl Counters {
    pub(crate) fn event(&self, event: TorrentEvent) {
        self.events[event as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn fetch(&self, outcome: FetchOutcome) {
        self.fetches[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    fn events(&self) -> [(TorrentEvent, u64); 4] {
        TorrentEvent::ALL.map(|event| (event, self.events[event as usize].load(Ordering::Relaxed)))
    }

    fn fetches(&self) -> [(FetchOutcome, u64); 4] {
        FetchOutcome::ALL.map(|outcome| {
            (
                outcome,
                self.fetches[outcome as usize].load(Ordering::Relaxed),
            )
        })
    }
}

/// The torrents and their sessions as the exporter renders them.
#[derive(Debug, Clone, Default)]
pub struct TorrentMetrics {
    /// As the service last published them.
    pub published: Arc<Published>,
    pub events: Vec<(TorrentEvent, u64)>,
    pub fetches: Vec<(FetchOutcome, u64)>,
}

impl TorrentMetrics {
    pub(crate) fn of(published: Arc<Published>, counters: &Counters) -> Self {
        Self {
            published,
            events: counters.events().to_vec(),
            fetches: counters.fetches().to_vec(),
        }
    }
}

/// The torrents and their sessions as the service last read them.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Published {
    pub torrents: Vec<TorrentSeries>,
    pub sessions: Vec<SessionSeries>,
}

/// One torrent's figures with its trackers, and the session of the route
/// it runs on, while that route has one.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct JobSeries<'a> {
    pub torrent: &'a TorrentSeries,
    pub session: Option<&'a SessionSeries>,
}

impl Published {
    /// Each torrent with its route's session, in the order published.
    pub fn jobs(&self) -> impl Iterator<Item = JobSeries<'_>> {
        self.torrents.iter().map(|torrent| JobSeries {
            torrent,
            session: self
                .sessions
                .iter()
                .find(|session| !torrent.route.is_empty() && session.route == torrent.route),
        })
    }

    /// `job_id`'s torrent with its route's session, when it is a torrent.
    pub fn job(&self, job_id: JobId) -> Option<JobSeries<'_>> {
        self.jobs().find(|job| job.torrent.job_id == job_id)
    }
}

/// One torrent's figures.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentSeries {
    pub job_id: JobId,
    /// Its category's name; empty when it has none.
    pub category: String,
    /// The route it runs on, or would; empty before it has one.
    pub route: String,
    pub state: TorrentState,
    /// Bytes of its files in hand.
    pub have_bytes: u64,
    pub total_bytes: u64,
    /// Bytes of the files it fetches.
    pub wanted_bytes: u64,
    pub pieces: Option<TorrentPieces>,
    /// The ledger: every byte fetched from peers, and sent to them.
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    /// Received and thrown away since it was added in this process: pieces
    /// that failed their check, and blocks it already had.
    pub hash_failed_bytes: u64,
    pub redundant_bytes: u64,
    pub active_seconds: u64,
    pub seeding_seconds: u64,
    /// The seconds of its current unbroken seeding run.
    pub consecutive_seeding_seconds: u64,
    pub download_bps: u64,
    pub upload_bps: u64,
    pub ratio: f64,
    pub peers_established: u32,
    pub peers_connecting: u32,
    pub peer_seeders: u32,
    /// The route leg its session runs on: the leg's consumer and position.
    pub leg: Option<(String, usize)>,
    /// A magnet's metadata fetch, while it runs.
    pub fetch: Option<FetchPhase>,
    pub seeding: SeedingSeries,
    pub trackers: Vec<TrackerSeries>,
}

/// A seeding goal of the policy in force.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeedingGoal {
    Ratio,
    SeedTime,
    Inactive,
}

impl SeedingGoal {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ratio => "ratio",
            Self::SeedTime => "seed_seconds",
            Self::Inactive => "inactive_seconds",
        }
    }
}

/// A torrent's seeding goals: each goal the policy in force sets, as a
/// ratio or in seconds, with what is left of it; and whether the policy
/// counts them met.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SeedingSeries {
    pub goals: Vec<(SeedingGoal, f64, f64)>,
    pub met: bool,
}

impl SeedingSeries {
    /// The goals of `policy` against `counters` at `now_ms`; met as
    /// [`status::goal_left`] says.
    pub fn of(policy: &Seeding, counters: &TorrentCounters, total_bytes: u64, now_ms: i64) -> Self {
        let met = status::goal_left(policy, counters, total_bytes, now_ms).is_none();
        let mut goals = Vec::new();
        if let Some(goal) = policy.ratio_goal {
            let goal = f64::from(goal);
            let ratio = status::ratio(counters, total_bytes);
            goals.push((SeedingGoal::Ratio, goal, (goal - ratio).max(0.0)));
        }
        if let Some(minutes) = policy.seed_time_goal_minutes {
            let goal = u64::from(minutes) * 60;
            let left = goal.saturating_sub(policy.seeded_seconds(counters));
            goals.push((SeedingGoal::SeedTime, goal as f64, left as f64));
        }
        if let Some(minutes) = policy.inactive_goal_minutes {
            let goal = u64::from(minutes) * 60;
            // No activity yet: the whole goal is left, as `goal_left` counts
            // it unmet.
            let idle = counters
                .last_activity_ms
                .map_or(0, |at| (now_ms - at).max(0) as u64 / 1000);
            goals.push((
                SeedingGoal::Inactive,
                goal as f64,
                goal.saturating_sub(idle) as f64,
            ));
        }
        Self { goals, met }
    }
}

/// One tracker of a torrent: the first the torrent lists on its host, so a
/// host is one series per torrent.
#[derive(Debug, Clone, PartialEq)]
pub struct TrackerSeries {
    pub host: String,
    pub status: TrackerStatus,
    pub last_announce_ms: Option<i64>,
    pub next_announce_ms: Option<i64>,
    /// The interval its last accepted announce asked for.
    pub interval_seconds: Option<u64>,
    pub seeders: Option<u32>,
    pub leechers: Option<u32>,
    pub completed: Option<u32>,
    /// Failures in a row since it last answered.
    pub fails: u32,
}

/// A torrent's trackers, one per host, with the interval each run's table
/// holds for its URL.
pub fn trackers(
    trackers: &[TorrentTracker],
    announces: &[weaver_torrent::TrackerAnnounce],
) -> Vec<TrackerSeries> {
    let mut series: Vec<TrackerSeries> = Vec::with_capacity(trackers.len());
    for tracker in trackers {
        let Some(host) = tracker_host(&tracker.url) else {
            continue;
        };
        if series.iter().any(|kept| kept.host == host) {
            continue;
        }
        let interval_seconds = announces
            .iter()
            .find(|announce| announce.url == tracker.url)
            .and_then(|announce| announce.last.as_ref())
            .and_then(|last| last.result.as_ref().ok())
            .map(|accepted| accepted.interval.as_secs());
        series.push(TrackerSeries {
            host,
            status: tracker.status,
            last_announce_ms: tracker.last_announce_ms,
            next_announce_ms: tracker.next_announce_ms,
            interval_seconds,
            seeders: tracker.seeders,
            leechers: tracker.leechers,
            completed: tracker.downloaded,
            fails: tracker.fails,
        });
    }
    series
}

/// One route's session.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SessionSeries {
    pub route: String,
    /// Its session runs; the rest is zero, and goes unrendered, while it
    /// does not.
    pub open: bool,
    pub listen_port: Option<u16>,
    pub external_addresses: usize,
    pub uptime_seconds: u64,
    pub received_payload_bytes: u64,
    pub received_protocol_bytes: u64,
    pub sent_payload_bytes: u64,
    pub sent_protocol_bytes: u64,
    pub blocked_incoming: u64,
    pub blocked_outgoing: u64,
    pub utp: UtpStatsSnapshot,
    /// Its peers across every torrent, by state.
    pub peers: Vec<(&'static str, u32)>,
    /// Outgoing connections by transport and address family: attempted,
    /// succeeded, failed.
    pub connects: Vec<(&'static str, &'static str, [u64; 3])>,
    /// Its DHT, while it runs one: nodes in the v4 and v6 routing tables,
    /// and requests waiting for an answer.
    pub dht: Option<(usize, usize, usize)>,
}

/// The states a session's peers are counted in.
pub const SESSION_PEER_STATES: [&str; 5] =
    ["established", "connecting", "queued", "dead", "not_needed"];

impl SessionSeries {
    pub(crate) fn held(route: &str) -> Self {
        Self {
            route: route.to_owned(),
            ..Self::default()
        }
    }

    /// `session`'s figures, all read from its counters but the DHT's table
    /// sizes, which take the table's read lock for a length.
    pub(crate) fn open(route: &str, session: &Session, port: u16) -> Self {
        let stats = session.stats_snapshot();
        let peers = &stats.peers;
        let connections = &stats.connections;
        macro_rules! family {
            ($single:expr) => {
                [$single.attempts, $single.successes, $single.errors]
            };
        }
        Self {
            route: route.to_owned(),
            open: true,
            listen_port: session.listen_addr().map(|_| port),
            external_addresses: session.external_ips().len(),
            uptime_seconds: stats.uptime_seconds,
            received_payload_bytes: stats.counters.fetched_bytes,
            received_protocol_bytes: stats.counters.fetched_protocol_bytes,
            sent_payload_bytes: stats.counters.uploaded_bytes,
            sent_protocol_bytes: stats.counters.uploaded_protocol_bytes,
            blocked_incoming: stats.counters.blocked_incoming,
            blocked_outgoing: stats.counters.blocked_outgoing,
            utp: stats.utp,
            peers: SESSION_PEER_STATES
                .into_iter()
                .zip([
                    peers.live,
                    peers.connecting,
                    peers.queued,
                    peers.dead,
                    peers.not_needed,
                ])
                .collect(),
            connects: vec![
                ("tcp", "v4", family!(connections.tcp.v4)),
                ("tcp", "v6", family!(connections.tcp.v6)),
                ("utp", "v4", family!(connections.utp.v4)),
                ("utp", "v6", family!(connections.utp.v6)),
            ],
            dht: session.get_dht().map(|dht| {
                let dht = dht.stats();
                (
                    dht.routing_table_size,
                    dht.routing_table_size_v6,
                    dht.outstanding_requests,
                )
            }),
        }
    }
}
