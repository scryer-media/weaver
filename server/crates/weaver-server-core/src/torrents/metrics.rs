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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub use weaver_torrent::session_stats::Transport;
pub use weaver_torrent::session_stats::dht;
pub use weaver_torrent::session_stats::disk::{
    DiskOp, DiskOpSnapshot, DiskStatsSnapshot, SocketSnapshot,
};
pub use weaver_torrent::session_stats::incoming;
pub use weaver_torrent::session_stats::network;
pub use weaver_torrent::session_stats::snapshot::UtpStatsSnapshot;
pub use weaver_torrent::session_stats::swarm;
pub use weaver_torrent::session_stats::tracker;
pub use weaver_torrent::session_stats::web_seed;

pub use super::files::{FileCallsSnapshot, FileStatsSnapshot};

/// The file calls of every torrent and the handle pool since the process
/// started, read from their counters without a lock.
pub fn file_stats() -> FileStatsSnapshot {
    super::files::STATS.snapshot()
}
use weaver_torrent::session_stats::snapshot::SessionStatsSnapshot;
use weaver_torrent::{FetchPhase, Session};

use crate::jobs::JobId;
use crate::settings::Seeding;
use crate::settings::model::tracker_host;
use crate::torrents::service::TorrentPieces;
use crate::torrents::{TorrentCounters, TorrentState, TorrentTracker, TrackerStatus, status};

/// Every state a torrent can be in, in the order they are exported.
pub const TORRENT_STATES: [TorrentState; 12] = [
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
    TorrentState::FilesMissing,
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
    /// What the piece checks of every session asked of the disk and the
    /// processor since this process started.
    pub check: CheckSeries,
}

/// The process's piece-check counts, as read at one moment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CheckSeries {
    /// Read calls a check made to a storage, the bytes they read and the
    /// time spent inside them, in nanoseconds.
    pub read_calls: u64,
    pub read_bytes: u64,
    pub read_nanos: u64,
    /// Read calls that did not go on from the read before them.
    pub read_seeks: u64,
    /// Pieces hashed, and the time spent hashing them, in nanoseconds.
    pub pieces_hashed: u64,
    pub hash_nanos: u64,
    /// Seed-mode pieces whose check was started, and askers that waited
    /// on a check already running and took its verdict.
    pub seed_checks_started: u64,
    pub seed_checks_joined: u64,
}

impl CheckSeries {
    /// The counts as they stand: eight relaxed loads.
    pub fn read() -> Self {
        let read = weaver_torrent::session_stats::check::CHECK_STATS.snapshot();
        Self {
            read_calls: read.read_calls,
            read_bytes: read.read_bytes,
            read_nanos: read.read_nanos,
            read_seeks: read.read_seeks,
            pieces_hashed: read.pieces_hashed,
            hash_nanos: read.hash_nanos,
            seed_checks_started: read.seed_checks_started,
            seed_checks_joined: read.seed_checks_joined,
        }
    }
}

impl TorrentMetrics {
    pub(crate) fn of(published: Arc<Published>, counters: &Counters) -> Self {
        Self {
            published,
            events: counters.events().to_vec(),
            fetches: counters.fetches().to_vec(),
            check: CheckSeries::read(),
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
    pub fn of(policy: &Seeding, counters: &TorrentCounters, held_bytes: u64, now_ms: i64) -> Self {
        let met = status::goal_left(policy, counters, held_bytes, now_ms).is_none();
        let mut goals = Vec::new();
        if let Some(goal) = policy.ratio_goal {
            let goal = f64::from(goal);
            let ratio = status::ratio(counters, held_bytes);
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
    // Each URL's first announce, and the hosts kept so far, found without
    // a walk per tracker.
    let mut announce_of: HashMap<&str, &weaver_torrent::TrackerAnnounce> =
        HashMap::with_capacity(announces.len());
    for announce in announces {
        announce_of.entry(announce.url.as_str()).or_insert(announce);
    }
    let mut kept: HashSet<String> = HashSet::with_capacity(trackers.len());
    let mut series: Vec<TrackerSeries> = Vec::with_capacity(trackers.len());
    for tracker in trackers {
        let Some(host) = tracker_host(&tracker.url) else {
            continue;
        };
        if !kept.insert(host.clone()) {
            continue;
        }
        let interval_seconds = announce_of
            .get(tracker.url.as_str())
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
    /// Its address as the internet sees it, IPv4 first, of each family its
    /// vote settled on.
    pub external_ips: Vec<std::net::IpAddr>,
    pub uptime_seconds: u64,
    pub received_payload_bytes: u64,
    pub received_protocol_bytes: u64,
    pub sent_payload_bytes: u64,
    pub sent_protocol_bytes: u64,
    pub blocked_incoming: u64,
    pub blocked_outgoing: u64,
    pub utp: UtpStatsSnapshot,
    /// Its disk calls, blocking permits and peer sockets.
    pub disk: DiskStatsSnapshot,
    /// Its peers across every torrent, by state.
    pub peers: Vec<(&'static str, u32)>,
    /// Outgoing connections by transport and address family: attempted,
    /// then each attempt's end: connected, refused, timed out, failed
    /// otherwise.
    pub connects: Vec<(&'static str, &'static str, [u64; 5])>,
    /// Its DHT, while it runs one: nodes in the v4 and v6 routing tables,
    /// and requests waiting for an answer.
    pub dht: Option<(usize, usize, usize)>,
    /// What its peer connections did, counted since it opened.
    pub swarm: swarm::SwarmSnapshot,
    /// What its DHT node has counted since it started; all zero while it
    /// runs none.
    pub dht_counts: dht::DhtSnapshot,
    /// How the connections peers opened to it ended their way in, counted
    /// since it opened.
    pub incoming: incoming::IncomingSnapshot,
    /// Upload slots its unchoked peers hold now.
    pub unchoke_slots_held: u64,
    /// What it asked of its torrents' web seeds, counted since it opened.
    pub web_seeds: web_seed::WebSeedSnapshot,
    /// Its port mapping reports and holepunch steps, counted since it
    /// opened.
    pub network: network::NetworkSnapshot,
    /// What its local service discovery has counted, while it runs one.
    pub lsd: Option<network::LsdSnapshot>,
    /// What waited on a rate limit, the session's or a torrent's.
    pub limiter: network::LimiterSnapshot,
    /// What it asked of its trackers, counted since it opened.
    pub trackers: tracker::TrackerSnapshot,
    /// Its connected peers over TCP, then over uTP, and how many of them
    /// are seeds.
    pub connected: ConnectedPeers,
    /// Pieces its connections took from slower peers.
    pub piece_steals: u64,
    /// Its outgoing handshakes by how each ended: sent plain, or sent
    /// encrypted and settled on RC4, on header only, failed, or dropped
    /// before it ended.
    pub outgoing_encryption: [u64; 5],
}

/// A session's connected peers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ConnectedPeers {
    pub tcp: u32,
    pub utp: u32,
    pub seeders: u32,
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
        let connects = connects(&stats);
        let external_ips = session.external_ips();
        Self {
            route: route.to_owned(),
            open: true,
            listen_port: session.listen_addr().map(|_| port),
            external_addresses: external_ips.len(),
            external_ips,
            uptime_seconds: stats.uptime_seconds,
            received_payload_bytes: stats.counters.fetched_bytes,
            received_protocol_bytes: stats.counters.fetched_protocol_bytes,
            sent_payload_bytes: stats.counters.uploaded_bytes,
            sent_protocol_bytes: stats.counters.uploaded_protocol_bytes,
            blocked_incoming: stats.counters.blocked_incoming,
            blocked_outgoing: stats.counters.blocked_outgoing,
            utp: stats.utp,
            disk: stats.disk,
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
            connects,
            dht: session.get_dht().map(|dht| {
                let dht = dht.stats();
                (
                    dht.routing_table_size,
                    dht.routing_table_size_v6,
                    dht.outstanding_requests,
                )
            }),
            connected: ConnectedPeers {
                tcp: peers.live_tcp,
                utp: peers.live_utp,
                seeders: peers.live_seeders,
            },
            piece_steals: peers.steals.into(),
            outgoing_encryption: {
                let encryption = &connections.encryption;
                [
                    encryption.plaintext,
                    encryption.rc4,
                    encryption.header_only,
                    encryption.failures,
                    encryption.cancelled,
                ]
            },
            swarm: stats.swarm,
            trackers: stats.trackers,
            incoming: stats.incoming,
            web_seeds: stats.web_seeds,
            network: stats.network,
            lsd: stats.lsd,
            limiter: stats.limiter,
            unchoke_slots_held: stats.unchoke_slots_held as u64,
            dht_counts: session
                .get_dht()
                .map(|dht| dht.counted())
                .unwrap_or_default(),
        }
    }
}

/// A session's outgoing connects, one row per transport and family, as
/// `SessionSeries::connects` holds them.
fn connects(stats: &SessionStatsSnapshot) -> Vec<(&'static str, &'static str, [u64; 5])> {
    let connections = &stats.connections;
    macro_rules! family {
        ($single:expr) => {
            [
                $single.attempts,
                $single.successes,
                $single.refused,
                $single.timeouts,
                $single.errors,
            ]
        };
    }
    connections
        .kinds
        .iter()
        .flat_map(|(kind, by_family)| {
            [
                (kind.as_str(), "v4", family!(by_family.v4)),
                (kind.as_str(), "v6", family!(by_family.v6)),
            ]
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime};

    use weaver_torrent::{AnnounceOk, LastAnnounce, TrackerAnnounce};

    use super::{TrackerSeries, trackers};
    use crate::settings::model::tracker_host;
    use crate::torrents::{NamedBy, TorrentTracker, TrackerStatus};
    use weaver_torrent::ConnectionKind;

    /// The series as they were built before: a walk of the kept series and
    /// of the announces for every tracker.
    fn trackers_by_walking(
        trackers: &[TorrentTracker],
        announces: &[TrackerAnnounce],
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

    /// A step of a fixed sequence, so the data is the same every run.
    fn next(seed: &mut u64) -> u64 {
        *seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *seed >> 33
    }

    /// A torrent's `count` trackers and the announces of its run. Hosts and
    /// whole URLs repeat, some URLs have no host, some trackers have no
    /// announce, some announces were refused, and a URL may have two
    /// announces with different intervals.
    fn torrent(seed: &mut u64, count: usize) -> (Vec<TorrentTracker>, Vec<TrackerAnnounce>) {
        const STATUSES: [TrackerStatus; 4] = [
            TrackerStatus::NotContacted,
            TrackerStatus::Working,
            TrackerStatus::NotWorking,
            TrackerStatus::TrackerError,
        ];
        let hosts = (count / 2).max(1) as u64;
        let mut trackers = Vec::new();
        let mut announces = Vec::new();
        for at in 0..count {
            let url = match next(seed) % 10 {
                0 => format!("not a url {at}"),
                1..=3 => format!("http://tracker{}.invalid/announce", next(seed) % hosts),
                _ => format!(
                    "udp://tracker{}.invalid:{}/announce",
                    next(seed) % hosts,
                    6969 + next(seed) % 3
                ),
            };
            let some =
                |seed: &mut u64| (!next(seed).is_multiple_of(3)).then(|| (next(seed) % 500) as u32);
            trackers.push(TorrentTracker {
                tier: at as u32,
                url: url.clone(),
                named_by: NamedBy::Torrent,
                status: STATUSES[(next(seed) % 4) as usize],
                updating: next(seed).is_multiple_of(2),
                last_announce_ms: some(seed).map(i64::from),
                next_announce_ms: some(seed).map(i64::from),
                min_announce_ms: None,
                message: None,
                seeders: some(seed),
                leechers: some(seed),
                downloaded: some(seed),
                peers: some(seed),
                fails: (next(seed) % 4) as u32,
                resolved_by: None,
            });
            for _ in 0..next(seed) % 3 {
                let last = match next(seed) % 3 {
                    0 => None,
                    _ => Some(LastAnnounce {
                        at: SystemTime::UNIX_EPOCH,
                        result: Ok(AnnounceOk {
                            interval: Duration::from_secs(60 + next(seed) % 3600),
                            min_interval: Duration::from_secs(30),
                            seeders: None,
                            leechers: None,
                            downloaded: None,
                            peers: Vec::new(),
                            warning: None,
                        }),
                    }),
                };
                announces.push(TrackerAnnounce {
                    url: url.clone(),
                    last,
                    ..TrackerAnnounce::default()
                });
            }
        }
        (trackers, announces)
    }

    // The series built with one pass and two tables are the series the
    // walks built: the same trackers, in the same order, with the same
    // figures, for torrents of no, one, five and fifty trackers.
    #[test]
    fn the_tracker_series_are_those_the_walks_built() {
        let mut seed = 0x5eed;
        let mut kept_fewer = 0;
        let mut with_interval = 0;
        for count in [0, 1, 5, 50] {
            for _ in 0..40 {
                let (listed, announces) = torrent(&mut seed, count);
                let built = trackers(&listed, &announces);
                assert_eq!(built, trackers_by_walking(&listed, &announces));
                kept_fewer += usize::from(built.len() < listed.len());
                with_interval += built
                    .iter()
                    .filter(|series| series.interval_seconds.is_some())
                    .count();
            }
        }
        // The data did repeat hosts and did carry intervals.
        assert!(kept_fewer > 40, "{kept_fewer}");
        assert!(with_interval > 40, "{with_interval}");
    }

    // A session's connect counts, each a seed picks, give the rows they gave
    // when each kind had a field of its own: the same transports and
    // families, in the same order, with the same figures.
    #[test]
    fn the_connect_rows_are_those_each_kind_gave() {
        let mut stats = weaver_torrent::session_stats::snapshot::SessionStatsSnapshot {
            counters: Default::default(),
            download_speed: Default::default(),
            upload_speed: Default::default(),
            peers: Default::default(),
            uptime_seconds: 0,
            connections: Default::default(),
            utp: Default::default(),
            swarm: Default::default(),
            trackers: Default::default(),
            incoming: Default::default(),
            web_seeds: Default::default(),
            network: Default::default(),
            lsd: None,
            limiter: Default::default(),
            unchoke_slots_held: 0,
            disk: Default::default(),
        };
        let mut seed = 0x5eed;
        macro_rules! seeded {
            ($single:expr) => {
                $single.attempts = next(&mut seed) % 1000;
                $single.successes = next(&mut seed) % 1000;
                $single.refused = next(&mut seed) % 1000;
                $single.timeouts = next(&mut seed) % 1000;
                $single.errors = next(&mut seed) % 1000;
            };
        }
        seeded!(stats.connections.kinds[ConnectionKind::Tcp].v4);
        seeded!(stats.connections.kinds[ConnectionKind::Tcp].v6);
        seeded!(stats.connections.kinds[ConnectionKind::Utp].v4);
        seeded!(stats.connections.kinds[ConnectionKind::Utp].v6);
        assert_eq!(
            super::connects(&stats),
            [
                ("tcp", "v4", [174, 191, 580, 879, 648]),
                ("tcp", "v6", [787, 898, 350, 178, 127]),
                ("utp", "v4", [887, 234, 421, 179, 778]),
                ("utp", "v6", [819, 13, 993, 472, 331]),
            ]
        );
    }
}
