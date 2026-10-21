// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::IpAddr;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use tokio::time::Instant;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::Identity;
use crate::core::hash_id::{Id20, InfoHashes};
use crate::core::net::{HttpClients, Resolver};
use crate::tracker::tracker_comms_http;
use crate::tracker::tracker_comms_http::Ask;
use crate::tracker::tracker_comms_http::TrackerRequestEvent;
use crate::tracker::tracker_comms_udp;
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

pub struct TrackerComms {
    info_hash: Id20,
    identity: Identity,
    stats: Arc<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    /// The port peers reach this session on, read when each announce goes
    /// out (see [`announced_port`]).
    announce_port: ListenPort,
    /// Our own addresses an HTTP announce names (see [`AnnounceIps`]).
    announce_ips: AnnounceIps,
    http_clients: Arc<dyn HttpClients>,
    udp_client: UdpTrackerClient,
    key: u32,
    // The trackers under this hash and what announcing to them has done.
    list: Mutex<TrackerList>,
    // The `tracker id` each HTTP tracker last gave, sent back on every later
    // announce to it.
    tracker_ids: Mutex<HashMap<Url, Vec<u8>>>,
    status: TrackerStatus,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

/// The counters an announce reports (BEP 3), all for the current run: the
/// tracker task of a run sends `started` first, so "since `started`" and
/// "during this run" are the same span.
#[derive(Default, Clone, Copy, Debug)]
pub struct TrackerCommsStats {
    /// Payload bytes sent to peers during this run.
    pub uploaded_bytes: u64,
    /// Bytes of the pieces downloaded during this run that passed their hash
    /// check. Pieces the torrent already had when the run began are not
    /// counted, and neither is a piece that failed its check.
    pub downloaded_bytes: u64,
    /// Bytes of the pieces downloaded during this run that failed their
    /// hash check (libtorrent `torrent::m_total_failed_bytes`).
    pub corrupt_bytes: u64,
    /// Bytes of the blocks received during this run and not used: a copy
    /// of a block already had, or one no longer asked of the peer
    /// (libtorrent `torrent::m_total_redundant_bytes`).
    pub redundant_bytes: u64,
    /// Bytes of the whole torrent not yet had, whichever files are selected,
    /// so 0 only for a seed. None while the torrent's size is unknown (a
    /// magnet still resolving its metadata).
    pub left_bytes: Option<u64>,
    pub torrent_state: TrackerCommsStatsState,
    /// The torrent was incomplete when its current run began and is a seed
    /// now. Each tracker hears `completed` (BEP 3) once for such a run; a
    /// torrent that was already a seed when it started never sends it, and
    /// neither does one that has only its selected files.
    pub finished_during_run: bool,
}

impl TrackerCommsStats {
    /// `left` as announced. An unknown size is announced as one block still
    /// to come, so the announce never reads as a seed's.
    pub fn announced_left(&self) -> u64 {
        const UNKNOWN_LEFT: u64 = 16 * 1024;
        self.left_bytes.unwrap_or(UNKNOWN_LEFT)
    }

    pub fn is_completed(&self) -> bool {
        self.left_bytes == Some(0)
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;

    /// Resolves once the torrent is a seed, at once if it already is. Lets
    /// the tracker task send `completed` without waiting out the
    /// announce interval. The default never resolves, so `completed` goes out
    /// with the next regular announce.
    fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
        futures::future::pending().boxed()
    }
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

/// The port peers reach this session on, None when it accepts no
/// connections. It can change while a torrent runs (a gateway mapping that
/// lands after start), so each announce reads it as it goes out (libtorrent
/// `session_impl::queue_tracker_request` sets `req.listen_port` per request).
pub type ListenPort = tokio::sync::watch::Receiver<Option<u16>>;

/// Our own addresses an HTTP announce names as `ipv4=` and `ipv6=` (BEP 7),
/// read as each announce goes out (libtorrent `torrent::build_tracker_request`
/// reads the session's listen sockets per request).
pub type AnnounceIps = Arc<dyn Fn() -> Vec<IpAddr> + Send + Sync>;

#[derive(Clone, PartialEq, Eq)]
enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl SupportedTracker {
    fn url(&self) -> &Url {
        match self {
            SupportedTracker::Udp(u) | SupportedTracker::Http(u) => u,
        }
    }

    fn new(url: Url) -> Option<Self> {
        match url.scheme() {
            "http" | "https" => Some(SupportedTracker::Http(url)),
            "udp" => Some(SupportedTracker::Udp(url)),
            _ => {
                debug!("unsupported tracker URL: {}", url);
                None
            }
        }
    }
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedTracker::Udp(u) => std::fmt::Display::fmt(u, f),
            SupportedTracker::Http(u) => std::fmt::Display::fmt(u, f),
        }
    }
}

/// One tracker as one hash of the torrent announces to it, and how that has
/// gone (libtorrent `announce_infohash`, with the tier of its
/// `announce_entry`).
#[derive(Clone, Debug)]
struct Entry {
    tracker: SupportedTracker,
    tier: usize,
    /// Failures in a row since it last accepted an announce, at most 127.
    fails: u8,
    /// No regular announce goes to it before this.
    next_announce: Instant,
    /// No announce goes to it before this but one owed `completed` (the
    /// tracker's `min interval`).
    min_announce: Instant,
    /// An announce to it is waiting for its answer.
    updating: bool,
    /// The listen port our last announce to it carried; one that differs
    /// from the current port is due at once (libtorrent
    /// `session_impl::reopen_listen_sockets` forces every tracker's request
    /// when the listen port changes).
    port: Option<u16>,
    start_sent: bool,
    complete_sent: bool,
}

impl Entry {
    /// A tracker that has not failed since it last answered, or has never
    /// been asked (libtorrent `announce_infohash::is_working`).
    fn is_working(&self) -> bool {
        self.fails == 0
    }

    /// libtorrent `announce_infohash::can_announce`, with no fail limit: due,
    /// past its `min interval` unless it is owed `completed`, and not
    /// waiting for an answer. A second of slack takes trackers due together.
    fn can_announce(&self, now: Instant, seed: bool, port: Option<u16>) -> bool {
        let due = now + Duration::from_secs(1) >= self.next_announce
            && (now >= self.min_announce || (seed && !self.complete_sent));
        (due || self.port != port) && !self.updating
    }
}

/// The trackers one hash of a torrent announces to, tier by tier, and what
/// announcing to them has done (libtorrent `tracker_list`).
///
/// One tracker at a time is announced to, the first that works in list
/// order (libtorrent `settings_pack::announce_to_all_tiers` and
/// `announce_to_all_trackers`, both false by default). A tracker that fails
/// backs off and moves to the end of its tier, so the next one in the tier
/// is tried at once; a later tier is reached only once every tracker before
/// it is failing.
#[derive(Debug)]
struct TrackerList {
    /// In tier order.
    entries: Vec<Entry>,
    /// A tracker accepted `completed` (libtorrent `torrent::m_complete_sent`).
    complete_sent: bool,
}

impl TrackerList {
    /// A tracker named twice is kept where it is first named (libtorrent
    /// `tracker_list::add_tracker`).
    fn new(tiers: &[Vec<SupportedTracker>], now: Instant) -> Self {
        let mut entries: Vec<Entry> = Vec::new();
        for (tier, trackers) in tiers.iter().enumerate() {
            for tracker in trackers {
                if entries.iter().any(|e| &e.tracker == tracker) {
                    continue;
                }
                entries.push(Entry {
                    tracker: tracker.clone(),
                    tier,
                    fails: 0,
                    next_announce: now,
                    min_announce: now,
                    updating: false,
                    port: None,
                    start_sent: false,
                    complete_sent: false,
                });
            }
        }
        Self {
            entries,
            complete_sent: false,
        }
    }

    /// The walk of libtorrent `torrent::announce_with_tracker` with neither
    /// `announce_to_all_tiers` nor `announce_to_all_trackers`: over the
    /// `considered` trackers in list order, every failing one that
    /// `may_announce`, then the first working one if it may. The trackers
    /// after it wait.
    fn walk(
        &self,
        considered: impl Fn(&Entry) -> bool,
        may_announce: impl Fn(&Entry) -> bool,
    ) -> Vec<usize> {
        let mut picked = Vec::new();
        for (i, entry) in self.entries.iter().enumerate() {
            if !considered(entry) {
                continue;
            }
            if may_announce(entry) {
                picked.push(i);
            }
            if entry.is_working() {
                break;
            }
        }
        picked
    }

    /// The regular announces due at `now`, each with its event, marked as
    /// waiting for their answers; `port` is the listen port they carry.
    fn start_due(
        &mut self,
        now: Instant,
        port: Option<u16>,
        stats: &TrackerCommsStats,
    ) -> Vec<(SupportedTracker, Option<TrackerRequestEvent>)> {
        let seed = stats.finished_during_run;
        self.walk(|_| true, |e| e.can_announce(now, seed, port))
            .into_iter()
            .map(|i| {
                let event = self.event(&self.entries[i], stats);
                let entry = &mut self.entries[i];
                entry.updating = true;
                entry.port = port;
                (entry.tracker.clone(), event)
            })
            .collect()
    }

    /// The event a regular announce to `entry` carries (libtorrent
    /// `torrent::send_tracker_request`): `started` until it accepts one, then
    /// `completed` once the torrent became a seed during this run, unless a
    /// tracker has accepted `completed` already.
    fn event(&self, entry: &Entry, stats: &TrackerCommsStats) -> Option<TrackerRequestEvent> {
        if !entry.start_sent {
            Some(TrackerRequestEvent::Started)
        } else if stats.finished_during_run && !self.complete_sent && !entry.complete_sent {
            Some(TrackerRequestEvent::Completed)
        } else {
            None
        }
    }

    /// When the next regular announce may come due, None while every
    /// tracker it could go to waits for an answer (libtorrent
    /// `torrent::update_tracker_timer`).
    fn next_due(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        for entry in &self.entries {
            if entry.updating {
                break;
            }
            let due = entry.next_announce.max(entry.min_announce);
            next = Some(next.map_or(due, |next| next.min(due)));
            if entry.is_working() {
                break;
            }
        }
        next
    }

    fn position(&self, tracker: &SupportedTracker) -> Option<usize> {
        self.entries.iter().position(|e| &e.tracker == tracker)
    }

    /// `tracker` accepted an announce that carried `event` (libtorrent
    /// `torrent::tracker_response`).
    fn on_success(
        &mut self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        now: Instant,
        delay: &ReannounceDelay,
    ) {
        let Some(i) = self.position(tracker) else {
            return;
        };
        let entry = &mut self.entries[i];
        entry.updating = false;
        entry.fails = 0;
        entry.next_announce = now + delay.regular;
        entry.min_announce = now + delay.floor;
        match event {
            Some(TrackerRequestEvent::Started) => entry.start_sent = true,
            Some(TrackerRequestEvent::Completed) => {
                entry.complete_sent = true;
                self.complete_sent = true;
            }
            Some(TrackerRequestEvent::Stopped) | None => {}
        }
    }

    /// An announce to `tracker` failed: it backs off (libtorrent
    /// `announce_infohash::failed`) and goes to the end of its tier
    /// (`tracker_list::deprioritize_tracker`).
    fn on_failure(&mut self, tracker: &SupportedTracker, now: Instant, retry_interval: Duration) {
        let Some(i) = self.position(tracker) else {
            return;
        };
        let mut entry = self.entries.remove(i);
        entry.updating = false;
        entry.fails = entry.fails.saturating_add(1).min(127);
        entry.next_announce = now + retry_delay(entry.fails, retry_interval);
        let end_of_tier = self
            .entries
            .iter()
            .rposition(|e| e.tier == entry.tier)
            .map_or(i, |last| last + 1);
        self.entries.insert(end_of_tier, entry);
    }

    /// The torrent became a seed: every tracker not yet told is due at once
    /// (libtorrent `tracker_list::completed`).
    fn completed(&mut self, now: Instant) {
        for entry in self.entries.iter_mut().filter(|e| !e.complete_sent) {
            entry.next_announce = now;
            entry.min_announce = now;
        }
    }

    /// The trackers owed `stopped`: the walk over those that accepted
    /// `started`, each of them due (libtorrent `torrent::stop_announcing`).
    fn stopped(&self) -> Vec<SupportedTracker> {
        self.walk(|e| e.start_sent, |_| true)
            .into_iter()
            .map(|i| self.entries[i].tracker.clone())
            .collect()
    }
}

/// Whether the torrent may yet become a seed during this run, so its
/// trackers may yet be owed `completed`. A torrent live and complete
/// without finishing during this run was a seed from the start.
fn may_complete(stats: &TrackerCommsStats) -> bool {
    stats.finished_during_run
        || !(stats.torrent_state == TrackerCommsStatsState::Live && stats.is_completed())
}

/// The shortest and longest wait before a failing tracker is tried again
/// (libtorrent `announce_entry.cpp` `tracker_retry_delay_min`,
/// `tracker_retry_delay_max`), in seconds, and how steeply the wait grows
/// between them (`settings_pack::tracker_backoff`, a percentage).
const RETRY_DELAY_MIN: u64 = 5;
const RETRY_DELAY_MAX: u64 = 60 * 60;
const TRACKER_BACKOFF: u64 = 250;

/// How long a tracker waits after its `fails`-th failure in a row
/// (libtorrent `announce_infohash::failed`): 5 s plus 5 s × fails² × 250 %,
/// so 17, 55, 117, 205 … s up to an hour, and never sooner than the
/// `retry_interval` the failed answer gave.
fn retry_delay(fails: u8, retry_interval: Duration) -> Duration {
    let fails = u64::from(fails);
    let backoff = RETRY_DELAY_MIN + fails * fails * RETRY_DELAY_MIN * TRACKER_BACKOFF / 100;
    retry_interval.max(Duration::from_secs(backoff.min(RETRY_DELAY_MAX)))
}

/// An HTTP tracker answered the announce without accepting it: a `failure
/// reason`, or an answer that does not parse. libtorrent tries it again no
/// sooner than the answer's interval, raised to the minimum announce
/// interval (`http_tracker_connection::on_response`, passed on by
/// `tracker_connection::fail_impl`); a transport failure gives none.
#[derive(Debug)]
struct Refused {
    reason: String,
    retry_interval: Duration,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for Refused {}

impl Refused {
    fn new(reason: String, interval: Option<Duration>) -> anyhow::Error {
        let interval = interval.unwrap_or(tracker_comms_http::DEFAULT_INTERVAL);
        anyhow::Error::new(Self {
            reason,
            retry_interval: interval.max(MIN_ANNOUNCE_INTERVAL),
        })
    }

    /// The least wait before the tracker that gave `error` is tried again.
    fn retry_interval(error: &anyhow::Error) -> Duration {
        error
            .downcast_ref::<Self>()
            .map_or(Duration::ZERO, |r| r.retry_interval)
    }
}

/// An HTTP tracker's answer to an announce: its accepted response, or why it
/// refused.
fn read_answer(bytes: &[u8]) -> anyhow::Result<tracker_comms_http::TrackerResponse<'_>> {
    if let Ok((error, _)) =
        crate::bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(bytes)
    {
        return Err(Refused::new(
            format!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            ),
            error.interval,
        ));
    }
    crate::bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(bytes)
        .map(|(response, _)| response)
        .map_err(|e| {
            Refused::new(
                format!(
                    "error deserializing the tracker's answer: {}",
                    e.into_kind()
                ),
                None,
            )
        })
}

/// How long an announce or scrape may take from start to finish, whatever
/// the transport (libtorrent `settings_pack::tracker_completion_timeout`).
const COMPLETION_TIMEOUT: Duration = Duration::from_secs(30);

/// The `min interval` of a UDP tracker, which sends none (libtorrent
/// `udp_tracker_connection::on_announce_response`).
const UDP_MIN_INTERVAL: Duration = Duration::from_secs(60);

/// The shortest re-announce interval a tracker may ask for, a guard against
/// hammering a misconfigured one (libtorrent
/// `settings_pack::min_announce_interval`).
const MIN_ANNOUNCE_INTERVAL: Duration = Duration::from_secs(300);

/// Runs one request to a tracker, failing it after [`COMPLETION_TIMEOUT`].
async fn within_completion_timeout<T>(
    request: impl std::future::Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::time::timeout(COMPLETION_TIMEOUT, request)
        .await
        .map_err(|_| anyhow::anyhow!("the tracker did not answer within {COMPLETION_TIMEOUT:?}"))?
}

/// How long to wait before the next announce.
#[derive(Debug, PartialEq, Eq)]
struct ReannounceDelay {
    /// No announce other than `stopped` goes out before this (the tracker's
    /// `min interval`).
    floor: Duration,
    /// The regular re-announce delay, never below `floor`.
    regular: Duration,
}

impl ReannounceDelay {
    /// `interval` is the tracker's, raised to [`MIN_ANNOUNCE_INTERVAL`]
    /// (libtorrent `http_tracker_connection::on_response`,
    /// `udp_tracker_connection::on_announce_response`); a `forced` interval
    /// is the session's own and replaces it as given. `floor` is the
    /// tracker's min interval.
    fn new(interval: Duration, floor: Duration, forced: Option<Duration>) -> Self {
        Self {
            floor,
            regular: forced
                .unwrap_or(interval.max(MIN_ANNOUNCE_INTERVAL))
                .max(floor),
        }
    }
}

/// A successful announce.
struct Announced {
    peers: Vec<SocketAddr>,
    interval: Duration,
    min_interval: Duration,
    seeders: Option<u32>,
    leechers: Option<u32>,
}

/// What a tracker answered to an announce it accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnounceOk {
    pub interval: Duration,
    /// The least time between two announces: the tracker's, else the
    /// protocol's default.
    pub min_interval: Duration,
    /// Peers with the whole torrent, as the tracker counts them.
    pub seeders: Option<u32>,
    /// Peers still downloading, as the tracker counts them.
    pub leechers: Option<u32>,
}

/// The outcome of the most recent regular announce to a tracker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastAnnounce {
    /// When the announce finished.
    pub at: SystemTime,
    pub result: Result<AnnounceOk, String>,
}

/// One tracker of a torrent and how announcing to it is going.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackerAnnounce {
    /// The announce URL.
    pub url: String,
    /// A regular announce to this tracker is waiting for its answer.
    pub in_flight: bool,
    /// None until the tracker has been announced to.
    pub last: Option<LastAnnounce>,
}

/// A torrent's counts as a tracker's scrape reply gave them (BEP 48). A
/// count the tracker left out is None.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScrapeOk {
    /// Peers with the whole torrent.
    pub complete: Option<u32>,
    /// Peers still downloading.
    pub incomplete: Option<u32>,
    /// Downloads the tracker has seen finish.
    pub downloaded: Option<u32>,
}

impl ScrapeOk {
    /// The larger of each count: trackers and hashes count the same swarm
    /// (libtorrent `torrent::update_scrape_state`).
    fn max(self, other: Self) -> Self {
        Self {
            complete: self.complete.max(other.complete),
            incomplete: self.incomplete.max(other.incomplete),
            downloaded: self.downloaded.max(other.downloaded),
        }
    }

    /// The counts of two scrapes of one tracker, when either was answered.
    fn either(a: anyhow::Result<Self>, b: anyhow::Result<Self>) -> anyhow::Result<Self> {
        match (a, b) {
            (Ok(a), Ok(b)) => Ok(a.max(b)),
            (Ok(a), Err(_)) | (Err(_), Ok(a)) => Ok(a),
            (Err(e), Err(_)) => Err(e),
        }
    }
}

impl From<tracker_comms_udp::ScrapeResponse> for ScrapeOk {
    fn from(r: tracker_comms_udp::ScrapeResponse) -> Self {
        Self {
            complete: Some(r.complete),
            incomplete: Some(r.incomplete),
            downloaded: Some(r.downloaded),
        }
    }
}

/// The outcome of the most recent scrape of a tracker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastScrape {
    /// When the scrape finished.
    pub at: SystemTime,
    pub result: Result<ScrapeOk, String>,
}

/// One tracker of a torrent and how scraping it last went.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrackerScrape {
    /// The announce URL.
    pub url: String,
    /// None until the tracker has been scraped.
    pub last: Option<LastScrape>,
}

/// When paused torrents scrape their trackers on their own (libtorrent
/// `session_impl` auto-scrape of `torrent_want_scrape`): each torrent once
/// every `interval`, and the session no more than once every
/// `min_interval`, so with many torrents each waits its turn. Clones share
/// the session's turns: create one per session.
#[derive(Clone)]
pub struct AutoScrape {
    interval: Duration,
    min_interval: Duration,
    last: Arc<tokio::sync::Mutex<Option<tokio::time::Instant>>>,
}

impl Default for AutoScrape {
    fn default() -> Self {
        Self::new(Self::INTERVAL, Self::MIN_INTERVAL)
    }
}

impl AutoScrape {
    /// libtorrent `settings_pack::auto_scrape_interval`.
    pub const INTERVAL: Duration = Duration::from_secs(1800);
    /// libtorrent `settings_pack::auto_scrape_min_interval`.
    pub const MIN_INTERVAL: Duration = Duration::from_secs(300);

    pub fn new(interval: Duration, min_interval: Duration) -> Self {
        Self {
            interval,
            min_interval,
            last: Default::default(),
        }
    }

    /// Runs `scrape` once every interval, each run in the session's next
    /// turn, until dropped. libtorrent's round robin gives each of `n`
    /// torrents a scrape every `max(interval, n * min_interval)`; waiting
    /// out the interval, then queueing (fairly) for a turn spaced
    /// `min_interval` after the previous one, gives the same.
    async fn run<F: std::future::Future<Output = ()>>(&self, mut scrape: impl FnMut() -> F) {
        loop {
            tokio::time::sleep(self.interval).await;
            {
                let mut last = self.last.lock().await;
                if let Some(at) = *last {
                    tokio::time::sleep_until(at + self.min_interval).await;
                }
                *last = Some(tokio::time::Instant::now());
            }
            scrape().await;
        }
    }
}

/// Scrapes a torrent's trackers under every hash it announces.
struct Scraper {
    info_hashes: Vec<Id20>,
    user_agent: String,
    // In table order, one per URL.
    trackers: Vec<SupportedTracker>,
    http_clients: Arc<dyn HttpClients>,
    udp_client: UdpTrackerClient,
}

impl Scraper {
    /// The counts `tracker` gives under any of the torrent's hashes, the
    /// larger of each when both are answered.
    async fn scrape(&self, tracker: &SupportedTracker) -> anyhow::Result<ScrapeOk> {
        let results = futures::future::join_all(
            self.info_hashes
                .iter()
                .map(|info_hash| self.scrape_hash(tracker, *info_hash)),
        )
        .await;
        results
            .into_iter()
            .reduce(ScrapeOk::either)
            .context("the torrent has no hash to scrape")?
    }

    async fn scrape_hash(
        &self,
        tracker: &SupportedTracker,
        info_hash: Id20,
    ) -> anyhow::Result<ScrapeOk> {
        within_completion_timeout(async {
            match tracker {
                SupportedTracker::Http(url) => {
                    self.scrape_http(url, info_hash)
                        .instrument(trace_span!("http_scrape", tracker = %url))
                        .await
                }
                SupportedTracker::Udp(url) => {
                    self.scrape_udp(url, info_hash)
                        .instrument(trace_span!("udp_scrape", tracker = %url))
                        .await
                }
            }
        })
        .await
    }

    async fn scrape_http(&self, tracker_url: &Url, info_hash: Id20) -> anyhow::Result<ScrapeOk> {
        let url =
            tracker_comms_http::scrape_url(tracker_url).context("the tracker offers no scrape")?;
        let body = tracker_comms_http::get(
            &*self.http_clients,
            self.udp_client.resolver(),
            &url,
            Ask::Scrape,
            &tracker_comms_http::scrape_querystring(&info_hash),
            &self.user_agent,
        )
        .await?;
        tracker_comms_http::parse_scrape(&body, &info_hash)
    }

    async fn scrape_udp(&self, url: &Url, info_hash: Id20) -> anyhow::Result<ScrapeOk> {
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );
        let scrape = |addr: SocketAddr| async move {
            self.udp_client
                .scrape(addr, info_hash)
                .await
                .map(ScrapeOk::from)
        };
        match udp_tracker_to_socket_addrs(host, port, self.udp_client.resolver()).await? {
            UdpTrackerResolveResult::One(addr) => scrape(addr).await,
            UdpTrackerResolveResult::Two(v4, v6) => {
                let (r4, r6) = tokio::join!(scrape(v4.into()), scrape(v6.into()));
                ScrapeOk::either(r4, r6)
            }
        }
    }
}

struct StatusShared {
    announces: tokio::sync::watch::Sender<Vec<TrackerAnnounce>>,
    scrapes: tokio::sync::watch::Sender<Vec<TrackerScrape>>,
    scraper: Scraper,
}

/// The announce table of one [`TrackerComms`]: one entry per tracker it was
/// started with, in the order given, from the start, and beside it how
/// scraping each went. Clones share the tables, which stay readable, and
/// the trackers scrapable, after the tracker task ends.
#[derive(Clone)]
pub struct TrackerStatus(Arc<StatusShared>);

impl std::fmt::Debug for TrackerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TrackerStatus")
            .field(&*self.0.announces.borrow())
            .finish()
    }
}

impl TrackerStatus {
    fn new<'a>(
        trackers: impl IntoIterator<Item = &'a SupportedTracker>,
        info_hashes: Vec<Id20>,
        identity: &Identity,
        http_clients: Arc<dyn HttpClients>,
        udp_client: UdpTrackerClient,
    ) -> Self {
        let mut unique: Vec<SupportedTracker> = Vec::new();
        for tracker in trackers {
            if !unique.contains(tracker) {
                unique.push(tracker.clone());
            }
        }
        let announces = unique
            .iter()
            .map(|t| TrackerAnnounce {
                url: t.url().to_string(),
                in_flight: false,
                last: None,
            })
            .collect();
        let scrapes = unique
            .iter()
            .map(|t| TrackerScrape {
                url: t.url().to_string(),
                last: None,
            })
            .collect();
        Self(Arc::new(StatusShared {
            announces: tokio::sync::watch::channel(announces).0,
            scrapes: tokio::sync::watch::channel(scrapes).0,
            scraper: Scraper {
                info_hashes,
                user_agent: identity.user_agent.clone(),
                trackers: unique,
                http_clients,
                udp_client,
            },
        }))
    }

    /// A snapshot of the table.
    pub fn announces(&self) -> Vec<TrackerAnnounce> {
        self.0.announces.borrow().clone()
    }

    /// Sees every change to the table.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Vec<TrackerAnnounce>> {
        self.0.announces.subscribe()
    }

    /// How scraping each tracker last went, in the table's order.
    pub fn scrapes(&self) -> Vec<TrackerScrape> {
        self.0.scrapes.borrow().clone()
    }

    /// Asks the tracker that last accepted an announce, or the first
    /// tracker when none has, for the torrent's counts under every hash it
    /// announces (libtorrent `torrent::scrape_tracker` without an index,
    /// `torrent::scrape_tracker_impl`). The outcome goes to
    /// [`Self::scrapes`], and the counts given replace the seeds and
    /// leechers of the tracker's last accepted announce: libtorrent keeps
    /// one set of counts per tracker, which announce and scrape replies
    /// alike overwrite (`torrent::tracker_scrape_response`).
    pub async fn scrape(&self) -> anyhow::Result<ScrapeOk> {
        let tracker = self
            .scrape_target()
            .context("the torrent has no trackers")?;
        let result = self.0.scraper.scrape(&tracker).await;
        self.record_scrape(&tracker, &result);
        result
    }

    /// Scrapes on `schedule` until dropped.
    pub(crate) async fn auto_scrape(&self, schedule: &AutoScrape) {
        schedule
            .run(|| async {
                if let Err(e) = self.scrape().await {
                    debug!("auto-scrape failed: {e:#}");
                }
            })
            .await
    }

    /// libtorrent `tracker_list::last_working`, else `tracker_list::first`.
    fn scrape_target(&self) -> Option<SupportedTracker> {
        let last_working = self
            .0
            .announces
            .borrow()
            .iter()
            .filter_map(|a| {
                let last = a.last.as_ref()?;
                last.result.is_ok().then(|| (last.at, a.url.clone()))
            })
            .max_by_key(|(at, _)| *at)
            .map(|(_, url)| url);
        let trackers = &self.0.scraper.trackers;
        match last_working {
            Some(url) => trackers.iter().find(|t| t.url().as_str() == url),
            None => trackers.first(),
        }
        .cloned()
    }

    fn record_scrape(&self, tracker: &SupportedTracker, result: &anyhow::Result<ScrapeOk>) {
        let url = tracker.url().as_str();
        if let Ok(counts) = result {
            self.0.announces.send_if_modified(|announces| {
                let Some(accepted) = announces
                    .iter_mut()
                    .find(|a| a.url == url)
                    .and_then(|a| a.last.as_mut())
                    .and_then(|last| last.result.as_mut().ok())
                else {
                    return false;
                };
                let before = (accepted.seeders, accepted.leechers);
                accepted.seeders = counts.complete.or(accepted.seeders);
                accepted.leechers = counts.incomplete.or(accepted.leechers);
                before != (accepted.seeders, accepted.leechers)
            });
        }
        let last = LastScrape {
            at: SystemTime::now(),
            result: match result {
                Ok(counts) => Ok(*counts),
                Err(e) => Err(format!("{e:#}")),
            },
        };
        self.0.scrapes.send_modify(|scrapes| {
            if let Some(s) = scrapes.iter_mut().find(|s| s.url == url) {
                s.last = Some(last);
            }
        });
    }

    /// Marks an announce to `tracker` as sent; the returned guard records how
    /// it ended.
    fn begin(&self, tracker: &SupportedTracker) -> InFlight<'_> {
        let url = tracker.url().to_string();
        self.0.announces.send_modify(|announces| {
            if let Some(a) = announces.iter_mut().find(|a| a.url == url) {
                a.in_flight = true;
            }
        });
        InFlight {
            status: self,
            url,
            last: None,
        }
    }
}

/// An announce waiting for its answer. Dropping it ends the announce in the
/// table: with the outcome given to [`InFlight::finish`], or with none when
/// the announce was abandoned (the tracker task was dropped).
struct InFlight<'a> {
    status: &'a TrackerStatus,
    url: String,
    last: Option<LastAnnounce>,
}

impl InFlight<'_> {
    fn finish(mut self, result: &anyhow::Result<Announced>) {
        self.last = Some(LastAnnounce {
            at: SystemTime::now(),
            result: match result {
                Ok(a) => Ok(AnnounceOk {
                    interval: a.interval,
                    min_interval: a.min_interval,
                    seeders: a.seeders,
                    leechers: a.leechers,
                }),
                Err(e) => Err(format!("{e:#}")),
            },
        });
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        let last = self.last.take();
        self.status.0.announces.send_modify(|announces| {
            if let Some(a) = announces.iter_mut().find(|a| a.url == self.url) {
                a.in_flight = false;
                if last.is_some() {
                    a.last = last;
                }
            }
        });
    }
}

/// A started tracker task: the peers it finds, and its announce table.
pub struct TrackerTask {
    /// Dropping it ends the task and sends `stopped`.
    pub peers: BoxStream<'static, SocketAddr>,
    pub status: TrackerStatus,
}

/// Bounds the `stopped` announce (BEP 3) a tracker task sends when its peer
/// stream is dropped, and lets a shutdown wait for those announces.
///
/// Clones share their state: create one per session and hand a clone to every
/// [`TrackerComms::start_with_tiers`] call.
#[derive(Clone)]
pub struct StoppedAnnounces {
    timeout: Duration,
    running: Arc<tokio::sync::watch::Sender<usize>>,
}

impl Default for StoppedAnnounces {
    fn default() -> Self {
        Self::new(Self::DEFAULT_TIMEOUT)
    }
}

impl StoppedAnnounces {
    /// How long a `stopped` announce may take by default (libtorrent
    /// `settings_pack::stop_tracker_timeout`).
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// `timeout` bounds each `stopped` announce.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            running: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Resolves once every tracker task started with these settings has ended
    /// and its `stopped` announce has finished or timed out.
    pub async fn wait_idle(&self) {
        let mut rx = self.running.subscribe();
        // The sender lives in `self`, so this can't fail.
        let _ = rx.wait_for(|running| *running == 0).await;
    }

    fn register(&self) -> RunningGuard {
        self.running.send_modify(|r| *r += 1);
        RunningGuard(self.running.clone())
    }
}

struct RunningGuard(Arc<tokio::sync::watch::Sender<usize>>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.send_modify(|r| *r -= 1);
    }
}

/// Lives inside the peer stream. When the stream is dropped (the torrent was
/// paused, removed or the session is stopping), every tracker that accepted
/// `started` is sent `stopped` with the final counters.
struct StopOnDrop {
    comms: Arc<TrackerComms>,
    timeout: Duration,
    running: Option<RunningGuard>,
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let running = self.running.take();
        let trackers = self.comms.list.lock().stopped();
        if trackers.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            debug!("no runtime to send the stopped announce on");
            return;
        };
        let comms = self.comms.clone();
        let timeout = self.timeout;
        let span = debug_span!(parent: None, "tracker_stopped", info_hash = ?comms.info_hash);
        runtime.spawn(
            async move {
                let _running = running;
                let stats = comms.stats.get();
                let announces = trackers.iter().map(|tracker| {
                    comms
                        .announce(tracker, Some(TrackerRequestEvent::Stopped), &stats)
                        .map(move |r| {
                            if let Err(e) = r {
                                debug!(?tracker, "error sending stopped: {e:#}");
                            }
                        })
                });
                if tokio::time::timeout(timeout, futures::future::join_all(announces))
                    .await
                    .is_err()
                {
                    debug!(?timeout, "stopped announce timed out");
                }
            }
            .instrument(span),
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
    resolver: Option<&dyn Resolver>,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let res = match host {
        url::Host::Domain(name) => {
            // Use the first IPv4 and the first IPv6 addresses only.

            let mut v4: Option<SocketAddrV4> = None;
            let mut v6: Option<SocketAddrV6> = None;
            let addrs: Vec<SocketAddr> = match resolver {
                Some(resolver) => resolver.resolve(name, port).await,
                None => tokio::net::lookup_host((name, port))
                    .await
                    .map(Iterator::collect),
            }
            .with_context(|| format!("error looking up hostname {name}"))?;
            for addr in addrs {
                match (v4, v6, addr) {
                    (None, _, SocketAddr::V4(addr)) => v4 = Some(addr),
                    (_, None, SocketAddr::V6(addr)) => v6 = Some(addr),
                    _ => continue,
                }
            }
            let res = match (v4, v6) {
                (Some(v4), Some(v6)) => UdpTrackerResolveResult::Two(v4, v6),
                (Some(v4), None) => UdpTrackerResolveResult::One(v4.into()),
                (None, Some(v6)) => UdpTrackerResolveResult::One(v6.into()),
                _ => anyhow::bail!("zero addresses returned looking up {name}"),
            };
            trace!(?res, "resolved");
            res
        }
        url::Host::Ipv4(addr) => UdpTrackerResolveResult::One((addr, port).into()),
        url::Host::Ipv6(addr) => UdpTrackerResolveResult::One((addr, port).into()),
    };
    Ok(res)
}

/// The `port` of an announce: the port peers reach this session on, or 1
/// when it accepts no connections. An announce has no way to say "not
/// reachable", and some trackers refuse port 0, so a session without a
/// listener names port 1, which no peer is listening on.
fn announced_port(listen_port: Option<u16>) -> u16 {
    listen_port.unwrap_or(1)
}

/// How many peers an announce asks for: none with `stopped`, since the run
/// is over, otherwise libtorrent's default.
fn numwant(event: Option<TrackerRequestEvent>) -> u32 {
    match event {
        Some(TrackerRequestEvent::Stopped) => 0,
        _ => 200,
    }
}

fn saturating_u32(n: u64) -> u32 {
    u32::try_from(n).unwrap_or(u32::MAX)
}

impl TrackerComms {
    /// Announce to every tracker in `trackers`, each as a tier of its own.
    ///
    /// `stopped` announces are bounded by [`StoppedAnnounces::DEFAULT_TIMEOUT`].
    // TODO: fix too many args
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hashes: InfoHashes,
        identity: Identity,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        listen_port: ListenPort,
        announce_ips: AnnounceIps,
        http_clients: Arc<dyn HttpClients>,
        udp_client: UdpTrackerClient,
    ) -> Option<TrackerTask> {
        Self::start_with_tiers(
            info_hashes,
            identity,
            trackers.into_iter().map(|t| vec![t]).collect(),
            stats,
            force_interval,
            listen_port,
            announce_ips,
            http_clients,
            udp_client,
            StoppedAnnounces::default(),
        )
    }

    /// Announce to the trackers of a multitracker torrent (BEP 12): one
    /// tracker at a time, the first in tier order that works, the trackers
    /// of each tier in random order (see `TrackerList`). A failing tracker
    /// is tried again as libtorrent backs off (`retry_delay`).
    ///
    /// Each tracker is sent `started`, then `completed` if the torrent
    /// becomes a seed during this run (see
    /// [`TrackerCommsStats::finished_during_run`]) and no tracker has
    /// accepted it yet, and `stopped` when the returned stream is dropped. `stopped` is bounded
    /// and tracked by `stopped`. Each regular announce is recorded in the
    /// returned [`TrackerStatus`]. `listen_port` is the port peers reach this
    /// session on, None when it accepts no connections; when it changes, each
    /// tier announces it as soon as its tracker's `min interval` allows.
    /// `announce_ips` gives the addresses each HTTP announce names as ours.
    ///
    /// Each of `info_hashes.ids20()` is announced on its own, with its own
    /// events and failover: a hybrid under v1 and the truncated v2, a v2
    /// torrent under the truncated v2 (libtorrent
    /// `torrent::announce_with_tracker`, one announce per `protocol_version`).
    #[allow(clippy::too_many_arguments)]
    pub fn start_with_tiers(
        info_hashes: InfoHashes,
        identity: Identity,
        tiers: Vec<Vec<Url>>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        listen_port: ListenPort,
        announce_ips: AnnounceIps,
        http_clients: Arc<dyn HttpClients>,
        udp_client: UdpTrackerClient,
        stopped: StoppedAnnounces,
    ) -> Option<TrackerTask> {
        let tiers = tiers
            .into_iter()
            .filter_map(|urls| {
                let mut trackers = urls
                    .into_iter()
                    .filter_map(SupportedTracker::new)
                    .collect::<Vec<_>>();
                if trackers.is_empty() {
                    return None;
                }
                trackers.shuffle(&mut rand::rng());
                Some(trackers)
            })
            .collect::<Vec<_>>();
        if tiers.is_empty() {
            debug!(?info_hashes, "trackers list is empty");
            return None;
        }

        tracing::trace!(?tiers);

        let status = TrackerStatus::new(
            tiers.iter().flatten(),
            info_hashes.ids20().collect(),
            &identity,
            http_clients.clone(),
            udp_client.clone(),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let stats: Arc<dyn TorrentStatsProvider> = stats.into();
        // One key for the torrent, whatever hash it announces under
        // (libtorrent `torrent::tracker_key`).
        let key = rand::random();
        let comms = info_hashes
            .ids20()
            .map(|info_hash| {
                Arc::new(Self {
                    info_hash,
                    identity: identity.clone(),
                    stats: stats.clone(),
                    force_tracker_interval: force_interval,
                    tx: tx.clone(),
                    announce_port: listen_port.clone(),
                    announce_ips: announce_ips.clone(),
                    http_clients: http_clients.clone(),
                    udp_client: udp_client.clone(),
                    key,
                    list: Mutex::new(TrackerList::new(&tiers, Instant::now())),
                    tracker_ids: Default::default(),
                    status: status.clone(),
                })
            })
            .collect::<Vec<_>>();
        drop(tx);
        let stop_on_drop = comms
            .iter()
            .map(|comms| StopOnDrop {
                comms: comms.clone(),
                timeout: stopped.timeout(),
                running: Some(stopped.register()),
            })
            .collect::<Vec<_>>();

        let s = async_stream::stream! {
            use futures::StreamExt;
            let _stop_on_drop = stop_on_drop;
            let mut futures = FuturesUnordered::new();
            for comms in &comms {
                let span = debug_span!(parent: None, "tracker", info_hash = ?comms.info_hash);
                futures.push(comms.run().instrument(span));
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        if let Some(Err(e)) = e {
                            debug!("error: {e:#}");
                        }
                    }
                }
            }
        };

        Some(TrackerTask {
            peers: s.boxed(),
            status,
        })
    }

    /// Announces under this hash until dropped: whatever the tracker list
    /// says is due, then waits for an answer, for the next tracker to come
    /// due, or for the torrent to become a seed.
    async fn run(&self) -> anyhow::Result<()> {
        let mut in_flight = FuturesUnordered::new();
        // The torrent became a seed during this run and the list was told
        // (libtorrent `torrent::completed`, once per transition).
        let mut completion_seen = false;
        // The announces carry the port as they go out; a tracker that heard
        // another is announced to again (see `Entry::port`).
        let mut port = self.announce_port.clone();
        loop {
            let stats = self.stats.get();
            let current = *port.borrow_and_update();
            let due = self.list.lock().start_due(Instant::now(), current, &stats);
            for (tracker, event) in due {
                in_flight.push(self.announce_regular(tracker, event, stats));
            }
            let next_due = self.list.lock().next_due();
            let next_due = async {
                match next_due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some((tracker, event, result)) = in_flight.next() => {
                    self.on_answer(tracker, event, result).await?;
                }
                _ = next_due => {}
                Ok(()) = port.changed() => {
                    trace!("announce port changed, announcing early");
                }
                _ = self.stats.wait_until_completed(), if !completion_seen && may_complete(&stats) => {
                    completion_seen = true;
                    if self.stats.get().finished_during_run {
                        trace!("torrent completed, announcing early");
                        self.list.lock().completed(Instant::now());
                    }
                }
            }
        }
    }

    /// A regular announce, recorded in the status table.
    async fn announce_regular(
        &self,
        tracker: SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: TrackerCommsStats,
    ) -> (
        SupportedTracker,
        Option<TrackerRequestEvent>,
        anyhow::Result<Announced>,
    ) {
        let in_flight = self.status.begin(&tracker);
        let result = self.announce(&tracker, event, &stats).await;
        in_flight.finish(&result);
        (tracker, event, result)
    }

    async fn on_answer(
        &self,
        tracker: SupportedTracker,
        event: Option<TrackerRequestEvent>,
        result: anyhow::Result<Announced>,
    ) -> anyhow::Result<()> {
        let now = Instant::now();
        match result {
            Ok(announced) => {
                let delay = ReannounceDelay::new(
                    announced.interval,
                    announced.min_interval,
                    self.force_tracker_interval,
                );
                debug!(?tracker, ?delay, "tracker answered");
                self.list.lock().on_success(&tracker, event, now, &delay);
                for peer in announced.peers {
                    self.tx.send(peer).await.context("peer stream closed")?;
                }
            }
            Err(e) => {
                debug!(?tracker, "error calling tracker: {e:#}");
                let retry_interval = Refused::retry_interval(&e);
                self.list.lock().on_failure(&tracker, now, retry_interval);
            }
        }
        Ok(())
    }

    async fn announce(
        &self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        within_completion_timeout(async {
            match tracker {
                SupportedTracker::Http(url) => {
                    self.announce_http(url, event, stats)
                        .instrument(trace_span!("http_tracker", tracker = %url))
                        .await
                }
                SupportedTracker::Udp(url) => {
                    self.announce_udp(url, event, stats)
                        .instrument(trace_span!("udp_tracker", tracker = %url))
                        .await
                }
            }
        })
        .await
    }

    async fn announce_http(
        &self,
        tracker_url: &Url,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let trackerid = self.tracker_ids.lock().get(tracker_url).cloned();
        let ips = (self.announce_ips)();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.identity.peer_id,
            port: announced_port(*self.announce_port.borrow()),
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            corrupt: stats.corrupt_bytes,
            redundant: stats.redundant_bytes,
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: numwant(event),
            key: Some(self.key),
            trackerid: trackerid.as_deref(),
            ips: &ips,
        };

        let bytes = tracker_comms_http::get(
            &*self.http_clients,
            self.udp_client.resolver(),
            tracker_url,
            Ask::Announce,
            &request.as_querystring(),
            &self.identity.user_agent,
        )
        .await?;
        let response = read_answer(&bytes)?;
        if let Some(id) = &response.tracker_id {
            self.tracker_ids
                .lock()
                .insert(tracker_url.clone(), id.as_ref().to_vec());
        }

        Ok(Announced {
            peers: response.iter_peers().collect(),
            interval: response.interval,
            min_interval: response.min_interval,
            seeders: response.complete.map(saturating_u32),
            leechers: response.incomplete.map(saturating_u32),
        })
    }

    async fn announce_udp(
        &self,
        url: &Url,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );
        let addrs = udp_tracker_to_socket_addrs(host.clone(), port, self.udp_client.resolver())
            .instrument(trace_span!("resolve", ?host))
            .await?;

        match addrs {
            UdpTrackerResolveResult::One(addr) => {
                self.announce_udp_addr(addr, event, stats)
                    .instrument(trace_span!("udp request", ?addr))
                    .await
            }
            UdpTrackerResolveResult::Two(v4, v6) => {
                let (r4, r6) = tokio::join!(
                    self.announce_udp_addr(v4.into(), event, stats)
                        .instrument(trace_span!("udp request", addr=?v4)),
                    self.announce_udp_addr(v6.into(), event, stats)
                        .instrument(trace_span!("udp request", addr=?v6))
                );
                match (r4, r6) {
                    (Ok(mut a4), Ok(a6)) => {
                        a4.peers.extend(a6.peers);
                        Ok(a4)
                    }
                    (Ok(a), Err(_)) | (Err(_), Ok(a)) => Ok(a),
                    (Err(e), Err(_)) => Err(e),
                }
            }
        }
    }

    async fn announce_udp_addr(
        &self,
        addr: SocketAddr,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        use tracker_comms_udp::*;

        let request = AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.identity.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            uploaded: stats.uploaded_bytes,
            event: udp_event(event),
            key: self.key,
            num_want: numwant(event),
            port: announced_port(*self.announce_port.borrow()),
        };

        match self.udp_client.announce(addr, request).await {
            Ok(response) => {
                trace!(len = response.addrs.len(), "received announce response");
                Ok(Announced {
                    peers: response.addrs,
                    // UDP trackers (BEP 15) send only an interval; libtorrent
                    // takes 60 s as the min interval
                    // (`udp_tracker_connection::on_announce_response`).
                    interval: Duration::from_secs(response.interval.into()),
                    min_interval: UDP_MIN_INTERVAL,
                    seeders: Some(response.seeders),
                    leechers: Some(response.leechers),
                })
            }
            Err(e) => {
                debug!(?addr, "error reading announce response: {e:#}");
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::core::{
        hash_id::{Id20, Id32, InfoHashes},
        net::{DatagramSocket, HttpClients, HttpTarget, Resolver},
    };
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parking_lot::Mutex;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use tokio::time::Instant;

    use super::{
        AnnounceOk, AutoScrape, MIN_ANNOUNCE_INTERVAL, ReannounceDelay, Refused, ScrapeOk,
        StoppedAnnounces, SupportedTracker, TrackerAnnounce, TrackerComms, TrackerCommsStats,
        TrackerCommsStatsState, TrackerList, TrackerScrape, TrackerTask, UdpTrackerResolveResult,
        may_complete, read_answer, retry_delay, udp_tracker_to_socket_addrs,
    };
    use crate::Identity;
    use crate::tracker::tracker_comms_http::{MAX_INTERVAL, TrackerRequestEvent};
    use crate::tracker::tracker_comms_udp::{
        ACTION_CONNECT, CONNECTION_ID_MAGIC, UdpTrackerClient,
    };

    fn http(name: &str) -> SupportedTracker {
        SupportedTracker::Http(Url::parse(&format!("http://{name}/announce")).unwrap())
    }

    fn live(downloaded: u64, left: u64, finished_during_run: bool) -> TrackerCommsStats {
        TrackerCommsStats {
            uploaded_bytes: 0,
            downloaded_bytes: downloaded,
            corrupt_bytes: 0,
            redundant_bytes: 0,
            left_bytes: Some(left),
            torrent_state: TrackerCommsStatsState::Live,
            finished_during_run,
        }
    }

    fn list(tiers: &[&[&str]], now: Instant) -> TrackerList {
        let tiers = tiers
            .iter()
            .map(|tier| tier.iter().map(|name| http(name)).collect())
            .collect::<Vec<_>>();
        TrackerList::new(&tiers, now)
    }

    fn order(list: &TrackerList) -> Vec<SupportedTracker> {
        list.entries.iter().map(|e| e.tracker.clone()).collect()
    }

    /// A tracker's answer: re-announce in 30 minutes, no `min interval`.
    const ANSWERED: ReannounceDelay = ReannounceDelay {
        floor: Duration::ZERO,
        regular: Duration::from_secs(1800),
    };

    const STARTED: Option<TrackerRequestEvent> = Some(TrackerRequestEvent::Started);
    const COMPLETED: Option<TrackerRequestEvent> = Some(TrackerRequestEvent::Completed);

    #[test]
    fn a_failing_tracker_backs_off_as_libtorrent_does() {
        // libtorrent announce_infohash::failed with tracker_backoff 250.
        let s = Duration::from_secs;
        let delays = (1..=5)
            .map(|fails| retry_delay(fails, Duration::ZERO))
            .collect::<Vec<_>>();
        assert_eq!(delays, [s(17), s(55), s(117), s(205), s(317)]);
        assert_eq!(retry_delay(16, Duration::ZERO), s(3205));
        assert_eq!(retry_delay(17, Duration::ZERO), s(3600));
        assert_eq!(retry_delay(127, Duration::ZERO), s(3600));
        // The interval of the failed answer holds the retry off longer.
        assert_eq!(retry_delay(1, s(1800)), s(1800));
        assert_eq!(retry_delay(16, s(300)), s(3205));
    }

    #[test]
    fn a_refused_answer_is_retried_no_sooner_than_its_interval() {
        // libtorrent http_tracker_connection::on_response and
        // tracker_connection::fail_impl.
        let s = Duration::from_secs;
        let e = read_answer(b"d14:failure reason4:nopee").unwrap_err();
        assert!(format!("{e:#}").contains("nope"), "{e:#}");
        assert_eq!(Refused::retry_interval(&e), s(1800));
        let e = read_answer(b"d14:failure reason4:nope8:intervali3600ee").unwrap_err();
        assert_eq!(Refused::retry_interval(&e), s(3600));
        // Raised to the minimum announce interval.
        let e = read_answer(b"d14:failure reason4:nope8:intervali60ee").unwrap_err();
        assert_eq!(Refused::retry_interval(&e), MIN_ANNOUNCE_INTERVAL);
        // An answer that does not parse takes the default interval.
        let e = read_answer(b"not bencode").unwrap_err();
        assert_eq!(Refused::retry_interval(&e), s(1800));
        // A tracker that never answered gives no interval.
        let e = anyhow::anyhow!("connection refused");
        assert_eq!(Refused::retry_interval(&e), Duration::ZERO);
        assert!(read_answer(b"d8:intervali1800e5:peers0:e").is_ok());
    }

    #[test]
    fn an_interval_too_long_to_reckon_with_is_the_longest_libtorrent_holds() {
        // libtorrent keeps a tracker's intervals as seconds32
        // (parse_tracker_response); torrent::tracker_response adds them to
        // the time of the answer.
        let t0 = Instant::now();
        for secs in [u64::MAX, i64::MAX as u64] {
            let answer =
                format!("d8:intervali{secs}e12:min intervali{secs}e5:peers0:e").into_bytes();
            let response = read_answer(&answer).unwrap();
            assert_eq!(
                (response.interval, response.min_interval),
                (MAX_INTERVAL, MAX_INTERVAL)
            );
            let mut list = list(&[&["a"]], t0);
            list.start_due(t0, None, &live(0, 90, false));
            let delay = ReannounceDelay::new(response.interval, response.min_interval, None);
            list.on_success(&http("a"), STARTED, t0, &delay);
            assert_eq!(list.next_due(), Some(t0 + MAX_INTERVAL));

            let refusal = format!("d14:failure reason4:nope8:intervali{secs}ee").into_bytes();
            let retry = Refused::retry_interval(&read_answer(&refusal).unwrap_err());
            assert_eq!(retry, MAX_INTERVAL);
            list.start_due(t0 + MAX_INTERVAL, None, &live(0, 90, false));
            list.on_failure(&http("a"), t0 + MAX_INTERVAL, retry);
            assert_eq!(list.next_due(), Some(t0 + MAX_INTERVAL + MAX_INTERVAL));
        }
    }

    #[test]
    fn an_http_answer_naming_no_interval_takes_libtorrents_defaults() {
        // libtorrent parse_tracker_response: interval 1800 s, min interval
        // 30 s; an answer that names neither is still an answer.
        let s = Duration::from_secs;
        let response = read_answer(b"d5:peers0:e").unwrap();
        assert_eq!((response.interval, response.min_interval), (s(1800), s(30)));
        let response = read_answer(b"d8:intervali900e5:peers0:e").unwrap();
        assert_eq!((response.interval, response.min_interval), (s(900), s(30)));
        let response = read_answer(b"d12:min intervali0e5:peers0:e").unwrap();
        assert_eq!((response.interval, response.min_interval), (s(1800), s(0)));
    }

    #[test]
    fn one_tracker_is_announced_to_at_a_time_tier_by_tier() {
        // libtorrent torrent::announce_with_tracker with neither
        // announce_to_all_tiers nor announce_to_all_trackers.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"], &["c"]], t0);
        let stats = live(0, 90, false);

        // Only the first tracker of the first tier is announced to, and
        // nothing more while it waits for its answer.
        assert_eq!(list.start_due(t0, None, &stats), [(http("a"), STARTED)]);
        assert_eq!(list.start_due(t0, None, &stats), []);
        assert_eq!(list.next_due(), None);

        // a fails: it backs off to the end of its tier, b is tried at once.
        list.on_failure(&http("a"), t0, Duration::ZERO);
        assert_eq!(order(&list), [http("b"), http("a"), http("c")]);
        assert_eq!(list.start_due(t0, None, &stats), [(http("b"), STARTED)]);

        // b fails too: only now is the next tier tried.
        list.on_failure(&http("b"), t0, Duration::ZERO);
        assert_eq!(order(&list), [http("a"), http("b"), http("c")]);
        assert_eq!(list.start_due(t0, None, &stats), [(http("c"), STARTED)]);
        list.on_success(&http("c"), STARTED, t0, &ANSWERED);
        assert_eq!(list.start_due(t0, None, &stats), []);

        // The failing trackers come due after their back-off; c, which
        // works, is not due yet.
        assert_eq!(list.next_due(), Some(t0 + s(17)));
        let t1 = t0 + s(17);
        assert_eq!(
            list.start_due(t1, None, &stats),
            [(http("a"), STARTED), (http("b"), STARTED)]
        );

        // a answers and b fails again, backing off longer. a works and comes
        // first, so neither b nor c is announced to while it keeps working.
        list.on_success(&http("a"), STARTED, t1, &ANSWERED);
        list.on_failure(&http("b"), t1, Duration::ZERO);
        assert_eq!(list.entries[1].next_announce, t1 + s(55));
        assert_eq!(list.start_due(t0 + s(1800), None, &stats), []);
        assert_eq!(list.next_due(), Some(t1 + s(1800)));
        assert_eq!(
            list.start_due(t1 + s(1800), None, &stats),
            [(http("a"), None)]
        );
    }

    #[test]
    fn the_tracker_in_use_hears_completed_at_once_and_only_once() {
        // libtorrent tracker_list::completed, announce_infohash::can_announce
        // and torrent::send_tracker_request.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a"], &["b"]], t0);
        assert_eq!(
            list.start_due(t0, None, &live(0, 90, false)),
            [(http("a"), STARTED)]
        );
        let delay = ReannounceDelay {
            floor: s(900),
            regular: s(1800),
        };
        list.on_success(&http("a"), STARTED, t0, &delay);

        // The torrent becomes a seed: a is due at once, inside its min
        // interval, with `completed`.
        let seed = live(90, 0, true);
        let t1 = t0 + s(10);
        assert_eq!(list.start_due(t1, None, &seed), []);
        list.completed(t1);
        assert_eq!(list.start_due(t1, None, &seed), [(http("a"), COMPLETED)]);

        // Refused, a backs off, and b, the first tracker after it that
        // works, is asked at once.
        list.on_failure(&http("a"), t1, Duration::ZERO);
        assert_eq!(list.start_due(t1, None, &seed), [(http("b"), STARTED)]);
        list.on_success(&http("b"), STARTED, t1, &delay);

        // a is retried with `completed` after its back-off; b, which
        // works, waits for its interval.
        let t2 = t1 + s(17);
        assert_eq!(list.start_due(t2, None, &seed), [(http("a"), COMPLETED)]);
        list.on_success(&http("a"), COMPLETED, t2, &delay);

        // Once a tracker accepted it, no other is sent `completed`, and a,
        // working again, is the one announced to.
        assert_eq!(list.event(&list.entries[1], &seed), None);
        assert_eq!(
            list.start_due(t2 + s(1800), None, &seed),
            [(http("a"), None)]
        );
    }

    #[test]
    fn a_torrent_complete_from_the_start_never_sends_completed() {
        let t0 = Instant::now();
        let mut list = list(&[&["a"]], t0);
        let seeding = live(0, 0, false);
        assert!(!may_complete(&seeding));
        assert_eq!(list.start_due(t0, None, &seeding), [(http("a"), STARTED)]);
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);
        let t1 = t0 + ANSWERED.regular;
        assert_eq!(list.start_due(t1, None, &seeding), [(http("a"), None)]);
    }

    #[test]
    fn test_waits_for_completion_before_the_torrent_is_live() {
        // A magnet still resolving its metadata: nothing is known yet.
        assert!(may_complete(&TrackerCommsStats::default()));
        assert!(may_complete(&live(0, 90, false)));
        assert!(may_complete(&live(90, 0, true)));
    }

    #[test]
    fn test_unknown_size_never_announces_as_complete() {
        let unknown = TrackerCommsStats::default();
        assert!(!unknown.is_completed());
        assert_eq!(unknown.announced_left(), 16 * 1024);
        let done = live(0, 0, false);
        assert!(done.is_completed());
        assert_eq!(done.announced_left(), 0);
    }

    #[test]
    fn stopped_goes_to_the_trackers_announced_to_that_heard_started() {
        // libtorrent torrent::stop_announcing: the walk with event stopped
        // skips every tracker that never accepted `started`.
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"], &["c"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(list.stopped(), []);

        list.start_due(t0, None, &stats);
        // Not yet accepted: nothing is owed.
        assert_eq!(list.stopped(), []);
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);
        assert_eq!(list.stopped(), [http("a")]);

        // a fails and b takes over: until b accepts `started`, a, failing,
        // is still owed `stopped`.
        list.on_failure(&http("a"), t0, Duration::ZERO);
        assert_eq!(list.start_due(t0, None, &stats), [(http("b"), STARTED)]);
        assert_eq!(list.stopped(), [http("a")]);
        // Then b, the tracker in use, is.
        list.on_success(&http("b"), STARTED, t0, &ANSWERED);
        assert_eq!(list.stopped(), [http("b")]);
    }

    #[test]
    fn a_tracker_named_twice_is_kept_where_first_named() {
        let list = list(&[&["a", "b"], &["a", "c"]], Instant::now());
        assert_eq!(order(&list), [http("a"), http("b"), http("c")]);
        assert_eq!(
            list.entries.iter().map(|e| e.tier).collect::<Vec<_>>(),
            [0, 0, 1]
        );
    }

    #[test]
    fn test_min_interval_is_a_floor() {
        let s = Duration::from_secs;
        assert_eq!(
            ReannounceDelay::new(s(1800), s(900), None),
            ReannounceDelay {
                floor: s(900),
                regular: s(1800)
            }
        );
        // A forced interval below the tracker's min interval is raised to it.
        assert_eq!(
            ReannounceDelay::new(s(1800), s(900), Some(s(60))),
            ReannounceDelay {
                floor: s(900),
                regular: s(900)
            }
        );
        // An interval below min interval is raised too.
        assert_eq!(
            ReannounceDelay::new(s(600), s(900), None),
            ReannounceDelay {
                floor: s(900),
                regular: s(900)
            }
        );
        assert_eq!(
            ReannounceDelay::new(s(1800), Duration::ZERO, Some(s(60))),
            ReannounceDelay {
                floor: s(0),
                regular: s(60)
            }
        );
    }

    #[test]
    fn a_tracker_interval_is_never_below_the_minimum_announce_interval() {
        // libtorrent min_announce_interval, for the interval of either
        // transport: 0 would be a tight loop.
        let s = Duration::from_secs;
        for interval in [s(0), s(5), s(299)] {
            assert_eq!(
                ReannounceDelay::new(interval, Duration::ZERO, None).regular,
                MIN_ANNOUNCE_INTERVAL
            );
        }
        assert_eq!(
            ReannounceDelay::new(s(301), Duration::ZERO, None).regular,
            s(301)
        );
        // The session's forced interval is its own choice, not the tracker's.
        assert_eq!(
            ReannounceDelay::new(s(0), Duration::ZERO, Some(s(60))).regular,
            s(60)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracker_answer_that_outlasts_the_completion_timeout_fails() {
        // A request that never ends: a tracker trickling bytes, each within
        // the receive timeout, or a UDP exchange retried without end.
        let start = tokio::time::Instant::now();
        let e = super::within_completion_timeout(std::future::pending::<anyhow::Result<()>>())
            .await
            .unwrap_err();
        assert!(format!("{e:#}").contains("did not answer"), "{e:#}");
        assert!(start.elapsed() >= super::COMPLETION_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_http_tracker_fails_the_announce() {
        // Accepts the connection (the backlog does) and never answers.
        let (_listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some()) => r.unwrap(),
        };
        let error = status.announces()[0]
            .last
            .clone()
            .unwrap()
            .result
            .unwrap_err();
        assert!(error.contains("sent nothing"), "{error}");
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    /// A torrent that names none of its own addresses.
    fn no_ips() -> super::AnnounceIps {
        Arc::new(Vec::new)
    }

    #[tokio::test]
    async fn an_http_announce_names_our_addresses_as_they_are_when_it_goes_out() {
        // BEP 7; libtorrent torrent::build_tracker_request reads the listen
        // sockets per request.
        let (listener, url) = http_listener().await;
        let (port, listen_port) = tokio::sync::watch::channel(Some(6881));
        let ips = Arc::new(Mutex::new(vec![std::net::IpAddr::from([198, 51, 100, 7])]));
        let announce_ips: super::AnnounceIps = {
            let ips = ips.clone();
            Arc::new(move || ips.lock().clone())
        };
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(()),
            None,
            listen_port,
            announce_ips,
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            StoppedAnnounces::default(),
        )
        .unwrap()
        .peers;
        let first = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&first, "ipv4"), Some("198.51.100.7"));
        assert_eq!(query_param(&first, "ipv6"), None);

        *ips.lock() = vec!["2001:db8::1".parse().unwrap()];
        port.send_replace(Some(40001));
        let second = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&second, "ipv4"), None);
        assert_eq!(query_param(&second, "ipv6"), Some("2001%3Adb8%3A%3A1"));
        drop(peers);
        token.cancel();
    }

    fn v1_hash() -> InfoHashes {
        InfoHashes {
            v1: Some(Id20::new([1; 20])),
            v2: None,
        }
    }

    #[tokio::test]
    async fn a_hybrid_announces_under_v1_and_the_truncated_v2() {
        // libtorrent torrent::announce_with_tracker: one announce per
        // protocol_version the torrent has, each with its own event, and
        // torrent::tracker_key shared by both.
        let (listener, url) = http_listener().await;
        let hashes = InfoHashes {
            v1: Some(Id20::new([b'a'; 20])),
            v2: Some(Id32::new([b'b'; 32])),
        };
        let stopped = StoppedAnnounces::default();
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let mut peers = TrackerComms::start_with_tiers(
            hashes,
            identity(),
            vec![vec![url]],
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            stopped.clone(),
        )
        .unwrap()
        .peers;
        let mut announced = Vec::new();
        while announced.len() < 2 {
            tokio::select! {
                _ = peers.next() => panic!("the tracker sent no peers"),
                q = next_announce(&listener) => announced.push(q),
            }
        }
        let mut hashes_sent = announced
            .iter()
            .map(|q| query_param(q, "info_hash").unwrap().to_owned())
            .collect::<Vec<_>>();
        hashes_sent.sort();
        assert_eq!(hashes_sent, ["a".repeat(20), "b".repeat(20)]);
        for q in &announced {
            assert_eq!(query_param(q, "event"), Some("started"));
        }
        assert_eq!(
            query_param(&announced[0], "key"),
            query_param(&announced[1], "key")
        );
        drop(peers);
        token.cancel();
    }

    /// Stats the test drives by hand.
    struct ManualStats {
        stats: Mutex<TrackerCommsStats>,
        completed: tokio::sync::watch::Sender<bool>,
    }

    impl super::TorrentStatsProvider for Arc<ManualStats> {
        fn get(&self) -> TrackerCommsStats {
            *self.stats.lock()
        }

        fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
            let mut rx = self.completed.subscribe();
            async move {
                let _ = rx.wait_for(|c| *c).await;
            }
            .boxed()
        }
    }

    /// The query string of the next HTTP request, answered with an empty peer list.
    async fn next_announce(listener: &tokio::net::TcpListener) -> String {
        let (conn, query) = read_announce(listener).await;
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        query
    }

    /// The one peer [`answer_with_a_peer`] has the tracker name.
    fn the_peer() -> SocketAddr {
        (Ipv4Addr::LOCALHOST, 0x1a41).into()
    }

    /// Answers the next announce with `body`, which names [`the_peer`], and
    /// waits for that peer on the stream: the answer has been taken in.
    /// Returns the announce's query string.
    async fn answer_with_a_peer(
        listener: &tokio::net::TcpListener,
        peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
        body: &str,
    ) -> String {
        let (conn, query) = tokio::select! {
            _ = peers.next() => panic!("a peer came before the tracker was asked"),
            r = read_announce(listener) => r,
        };
        respond(conn, body).await;
        assert_eq!(peers.next().await, Some(the_peer()));
        query
    }

    /// The identity the tests' torrent names itself by.
    fn identity() -> Identity {
        Identity {
            peer_id: Id20::new([2; 20]),
            version: "Test 1.0".into(),
            user_agent: "Test/1.0".into(),
        }
    }

    /// The next HTTP request, unanswered, and its query string.
    async fn read_announce(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
        let (conn, request) = read_request(listener).await;
        let path = request.split(' ').nth(1).unwrap().to_owned();
        (conn, path.split_once('?').unwrap().1.to_owned())
    }

    /// The next HTTP request, unanswered: its request line and headers.
    async fn read_request(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
        let (conn, _) = listener.accept().await.unwrap();
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
            conn.readable().await.unwrap();
            match conn.try_read(&mut chunk) {
                Ok(0) => panic!("connection closed before the request was read"),
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("{e}"),
            }
        }
        (conn, String::from_utf8(buf).unwrap())
    }

    /// Answers an HTTP request with `body`.
    async fn respond(conn: tokio::net::TcpStream, body: &str) {
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let mut response = response.as_bytes();
        while !response.is_empty() {
            conn.writable().await.unwrap();
            match conn.try_write(response) {
                Ok(n) => response = &response[n..],
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("{e}"),
            }
        }
    }

    fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
        query
            .split('&')
            .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
    }

    #[tokio::test]
    async fn a_port_that_changes_is_announced_without_waiting_out_the_interval() {
        // libtorrent session_impl::queue_tracker_request reads the listen
        // port per request. The tracker's interval is 1800 s on the real
        // clock, so only the port change brings the second announce.
        let (listener, url) = http_listener().await;
        let (port, listen_port) = tokio::sync::watch::channel(Some(6881));
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(()),
            None,
            listen_port,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            StoppedAnnounces::default(),
        )
        .unwrap()
        .peers;
        let first = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&first, "port"), Some("6881"));
        assert_eq!(query_param(&first, "event"), Some("started"));
        port.send_replace(Some(40001));
        let second = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&second, "port"), Some("40001"));
        assert_eq!(query_param(&second, "event"), None);
        drop(peers);
        token.cancel();
    }

    #[tokio::test]
    async fn an_http_announce_reports_the_bytes_that_failed_their_hash_and_the_bytes_we_did_not_need()
     {
        // libtorrent torrent::build_tracker_request sends
        // m_total_failed_bytes as `corrupt` and m_total_redundant_bytes as
        // `redundant`, read per announce (http_tracker_connection::start).
        let (listener, url) = http_listener().await;
        let stats = Arc::new(ManualStats {
            stats: Mutex::new(TrackerCommsStats {
                corrupt_bytes: 16384,
                redundant_bytes: 5,
                ..live(0, 90, false)
            }),
            completed: tokio::sync::watch::channel(false).0,
        });
        let (port, listen_port) = tokio::sync::watch::channel(Some(6881));
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(stats.clone()),
            None,
            listen_port,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            StoppedAnnounces::default(),
        )
        .unwrap()
        .peers;
        let first = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&first, "corrupt"), Some("16384"));
        assert_eq!(query_param(&first, "redundant"), Some("5"));
        *stats.stats.lock() = TrackerCommsStats {
            corrupt_bytes: 32768,
            redundant_bytes: 21,
            ..live(0, 90, false)
        };
        port.send_replace(Some(40001));
        let second = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&second, "corrupt"), Some("32768"));
        assert_eq!(query_param(&second, "redundant"), Some("21"));
        drop(peers);
        token.cancel();
    }

    #[tokio::test]
    async fn test_http_tracker_hears_started_completed_and_stopped() {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tracker = Url::parse(&format!(
            "http://{}/announce",
            listener.local_addr().unwrap()
        ))
        .unwrap();

        let stats = Arc::new(ManualStats {
            stats: Mutex::new(live(0, 90, false)),
            completed: tokio::sync::watch::channel(false).0,
        });
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![tracker]],
            Box::new(stats.clone()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            stopped.clone(),
        )
        .unwrap()
        .peers;

        // Drive the peer stream while waiting for the tracker to be called.
        macro_rules! announce {
            () => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent no peers"),
                    q = next_announce(&listener) => q,
                }
            };
        }

        // The torrent completes only once `started` is answered: an answer
        // that comes in after it completed puts the next announce, and with
        // it `completed`, an interval away (libtorrent
        // torrent::tracker_response).
        let q = answer_with_a_peer(
            &listener,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        assert_eq!(query_param(&q, "downloaded"), Some("0"));
        assert_eq!(query_param(&q, "left"), Some("90"));

        *stats.stats.lock() = TrackerCommsStats {
            uploaded_bytes: 7,
            ..live(90, 0, true)
        };
        stats.completed.send_replace(true);
        let q = announce!();
        assert_eq!(query_param(&q, "event"), Some("completed"));
        assert_eq!(query_param(&q, "left"), Some("0"));

        *stats.stats.lock() = TrackerCommsStats {
            uploaded_bytes: 42,
            torrent_state: TrackerCommsStatsState::Paused,
            ..live(90, 0, false)
        };
        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        assert_eq!(query_param(&q, "uploaded"), Some("42"));
        assert_eq!(query_param(&q, "downloaded"), Some("90"));
        assert_eq!(query_param(&q, "left"), Some("0"));

        stopped.wait_idle().await;
        token.cancel();
    }

    // An announce names the torrent as its identity does, whatever the
    // client's own User-Agent.
    #[tokio::test]
    async fn test_http_announce_carries_the_torrents_peer_id_and_user_agent() {
        let (listener, url) = http_listener().await;
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| {
                reqwest::Client::builder()
                    .no_proxy()
                    .user_agent("Session/1.0")
            }),
            udp_client,
            stopped.clone(),
        )
        .unwrap()
        .peers;

        let request = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            request = async {
                let (conn, request) = read_request(&listener).await;
                respond(conn, "d8:intervali1800e5:peers0:e").await;
                request
            } => request,
        };
        let headers = request.to_ascii_lowercase();
        assert!(
            headers.contains("\r\nuser-agent: test/1.0\r\n"),
            "{request}"
        );
        assert!(!headers.contains("session/1.0"), "{request}");
        let query = request
            .split(' ')
            .nth(1)
            .unwrap()
            .split_once('?')
            .unwrap()
            .1;
        assert_eq!(
            query_param(query, "peer_id"),
            Some("%02".repeat(20).as_str())
        );

        drop(listener);
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test]
    async fn test_tracker_id_is_sent_back_and_stopped_wants_no_peers() {
        let (listener, url) = http_listener().await;
        let stats = Arc::new(ManualStats {
            stats: Mutex::new(live(0, 90, false)),
            completed: tokio::sync::watch::channel(false).0,
        });
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(stats.clone()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            stopped.clone(),
        )
        .unwrap()
        .peers;

        // Drive the peer stream while the tracker answers with `body`.
        macro_rules! announce {
            ($body:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent no peers"),
                    q = async {
                        let (conn, q) = read_announce(&listener).await;
                        respond(conn, $body).await;
                        q
                    } => q,
                }
            };
        }

        let q = answer_with_a_peer(
            &listener,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aA10:tracker id3:t-1e",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        assert_eq!(query_param(&q, "numwant"), Some("200"));
        assert_eq!(query_param(&q, "trackerid"), None);

        // The next announce carries the id back; an answer without one keeps it.
        *stats.stats.lock() = live(90, 0, true);
        stats.completed.send_replace(true);
        let q = announce!("d8:intervali1800e5:peers0:e");
        assert_eq!(query_param(&q, "event"), Some("completed"));
        assert_eq!(query_param(&q, "numwant"), Some("200"));
        assert_eq!(query_param(&q, "trackerid"), Some("t-1"));

        // `stopped` asks for no peers, and still names the id.
        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        assert_eq!(query_param(&q, "numwant"), Some("0"));
        assert_eq!(query_param(&q, "trackerid"), Some("t-1"));

        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test]
    async fn test_stream_dropped_before_any_announce_sends_nothing() {
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let stopped = StoppedAnnounces::default();
        let peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![Url::parse("http://tracker.invalid/announce").unwrap()]],
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(reqwest::Client::builder),
            udp_client,
            stopped.clone(),
        )
        .unwrap()
        .peers;
        drop(peers);
        // Never polled, so no tracker heard `started` and none is owed `stopped`.
        stopped.wait_idle().await;
        token.cancel();
    }

    async fn http_listener() -> (tokio::net::TcpListener, Url) {
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let url = Url::parse(&format!(
            "http://{}/announce",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        (listener, url)
    }

    async fn start(
        tiers: Vec<Vec<Url>>,
        stopped: &StoppedAnnounces,
    ) -> (TrackerTask, CancellationToken) {
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), None);
        let task = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            tiers,
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            stopped.clone(),
        )
        .unwrap();
        (task, token)
    }

    fn announce_of<'a>(announces: &'a [TrackerAnnounce], url: &Url) -> &'a TrackerAnnounce {
        announces
            .iter()
            .find(|a| a.url == url.as_str())
            .unwrap_or_else(|| panic!("{url} is not in the table"))
    }

    #[tokio::test]
    async fn test_announce_table_records_in_flight_success_and_error() {
        let (ok, ok_url) = http_listener().await;
        let (err, err_url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) =
            start(vec![vec![err_url.clone()], vec![ok_url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;

        // Every tracker is listed before any announce goes out.
        let never = |url: &Url| TrackerAnnounce {
            url: url.to_string(),
            in_flight: false,
            last: None,
        };
        assert_eq!(status.announces(), vec![never(&err_url), never(&ok_url)]);

        // Drive the peer stream while the trackers are called.
        macro_rules! driving {
            ($f:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent no peers"),
                    r = $f => r,
                }
            };
        }

        // The first tier's tracker alone is asked, and refuses.
        let (err_conn, _) = driving!(read_announce(&err));
        let announces = status.announces();
        assert!(announce_of(&announces, &err_url).in_flight);
        assert_eq!(announce_of(&announces, &ok_url), &never(&ok_url));
        respond(err_conn, "d14:failure reason4:nopee").await;

        // With the first tier failing, the next tier's is asked.
        let (ok_conn, _) = driving!(read_announce(&ok));
        let announces = status.announces();
        let a = announce_of(&announces, &ok_url);
        assert!(a.in_flight);
        assert_eq!(a.last, None);
        respond(
            ok_conn,
            "d8:completei3e10:incompletei5e8:intervali1800e12:min intervali900e5:peers0:e",
        )
        .await;
        let mut rx = status.subscribe();
        driving!(
            rx.wait_for(|announces| announces.iter().all(|a| !a.in_flight && a.last.is_some()))
        )
        .unwrap();

        let announces = status.announces();
        assert_eq!(
            announce_of(&announces, &ok_url)
                .last
                .as_ref()
                .unwrap()
                .result,
            Ok(AnnounceOk {
                interval: Duration::from_secs(1800),
                min_interval: Duration::from_secs(900),
                seeders: Some(3),
                leechers: Some(5),
            })
        );
        let error = announce_of(&announces, &err_url)
            .last
            .as_ref()
            .unwrap()
            .result
            .clone()
            .unwrap_err();
        assert!(error.contains("nope"), "{error}");

        // The table outlives the task: the outcomes stay readable.
        drop(peers);
        let q = next_announce(&ok).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        assert_eq!(status.announces(), announces);
        token.cancel();
    }

    #[tokio::test]
    async fn test_announce_abandoned_in_flight_leaves_no_outcome() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;

        let _conn = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = read_announce(&listener) => r,
        };
        assert!(status.announces()[0].in_flight);

        // Dropping the stream drops the task mid-announce.
        drop(peers);
        assert_eq!(
            status.announces(),
            vec![TrackerAnnounce {
                url: url.to_string(),
                in_flight: false,
                last: None,
            }]
        );
        // `started` was never accepted, so no `stopped` is owed.
        stopped.wait_idle().await;
        token.cancel();
    }

    /// A scrape reply giving `entry` as the counts of the tests' torrent.
    fn scrape_body(entry: &str) -> String {
        format!("d5:filesd20:{}{entry}ee", "\u{1}".repeat(20))
    }

    #[tokio::test]
    async fn scrape_asks_the_tracker_that_last_answered_and_keeps_its_counts() {
        let (ok, ok_url) = http_listener().await;
        let (err, err_url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) =
            start(vec![vec![err_url.clone()], vec![ok_url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        assert_eq!(
            status.scrapes(),
            vec![
                TrackerScrape {
                    url: err_url.to_string(),
                    last: None,
                },
                TrackerScrape {
                    url: ok_url.to_string(),
                    last: None,
                },
            ]
        );

        // `err` refuses the announce, then `ok` accepts it with counts.
        let ok_conn = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            conn = async {
                let (err_conn, _) = read_announce(&err).await;
                respond(err_conn, "d14:failure reason4:nopee").await;
                read_announce(&ok).await.0
            } => conn,
        };
        respond(
            ok_conn,
            "d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e",
        )
        .await;
        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a.iter().all(|a| !a.in_flight && a.last.is_some())) => r.unwrap(),
        };
        // The run ends; the table stays scrapable.
        drop(peers);
        let q = next_announce(&ok).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;

        // The scrape goes to `ok`, at its scrape URL, for the torrent's hash.
        let tracker = async {
            let (conn, request) = read_request(&ok).await;
            let target = request.split(' ').nth(1).unwrap().to_owned();
            respond(
                conn,
                &scrape_body("d8:completei7e10:downloadedi40e10:incompletei1ee"),
            )
            .await;
            target
        };
        let (counts, target) = tokio::join!(status.scrape(), tracker);
        assert_eq!(target, format!("/scrape?info_hash={}", "%01".repeat(20)));
        let counts = counts.unwrap();
        assert_eq!(
            counts,
            ScrapeOk {
                complete: Some(7),
                incomplete: Some(1),
                downloaded: Some(40),
            }
        );
        let scrapes = status.scrapes();
        assert_eq!(scrapes[1].last.as_ref().unwrap().result, Ok(counts));
        assert_eq!(scrapes[0].last, None);

        // The tracker's counts are the scrape's now.
        let announces = status.announces();
        let accepted = announce_of(&announces, &ok_url)
            .last
            .as_ref()
            .unwrap()
            .result
            .clone()
            .unwrap();
        assert_eq!((accepted.seeders, accepted.leechers), (Some(7), Some(1)));
        token.cancel();
    }

    #[tokio::test]
    async fn scrape_without_an_answered_announce_asks_the_first_tracker() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        // Never polled: nothing was announced.
        let TrackerTask { peers, status } = task;
        drop(peers);

        let tracker = async {
            let (conn, _) = read_request(&listener).await;
            respond(conn, "d14:failure reason4:nopee").await;
        };
        let (result, ()) = tokio::join!(status.scrape(), tracker);
        let e = result.unwrap_err();
        assert!(format!("{e:#}").contains("nope"), "{e:#}");
        let last = status.scrapes()[0].last.clone().unwrap();
        assert!(last.result.unwrap_err().contains("nope"));
        // A refused scrape leaves the announce table alone.
        assert_eq!(status.announces()[0].last, None);
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn auto_scrape_waits_the_interval_and_spaces_the_sessions_scrapes() {
        let schedule = AutoScrape::default();
        let start = tokio::time::Instant::now();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        // Two paused torrents on one session's schedule.
        for torrent in ["a", "b"] {
            let schedule = schedule.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                schedule
                    .run(|| {
                        let _ = tx.send((torrent, tokio::time::Instant::now()));
                        async {}
                    })
                    .await
            });
        }
        let mut scrapes = Vec::new();
        while scrapes.len() < 4 {
            scrapes.push(rx.recv().await.unwrap());
        }
        // Neither scrapes before the interval has passed.
        assert!(scrapes[0].1 - start >= AutoScrape::INTERVAL);
        // Every scrape is a minimum interval after the session's previous one.
        for pair in scrapes.windows(2) {
            assert!(
                pair[1].1 - pair[0].1 >= AutoScrape::MIN_INTERVAL,
                "{scrapes:?}"
            );
        }
        // Each torrent waits the interval again after its scrape.
        for torrent in ["a", "b"] {
            let at: Vec<_> = scrapes
                .iter()
                .filter(|(t, _)| *t == torrent)
                .map(|(_, at)| *at)
                .collect();
            assert_eq!(at.len(), 2, "{scrapes:?}");
            assert!(at[1] - at[0] >= AutoScrape::INTERVAL, "{scrapes:?}");
        }
    }

    /// Resolves every name to `at`'s address.
    struct FixedResolver {
        at: std::net::IpAddr,
    }

    impl Resolver for FixedResolver {
        fn resolve<'a>(
            &'a self,
            _host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<Vec<SocketAddr>>> {
            Box::pin(async move { Ok(vec![(self.at, port).into()]) })
        }
    }

    /// The error the announce table records for `url`, once it has one.
    async fn refusal_of(
        status: &super::TrackerStatus,
        peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
        url: &Url,
    ) -> String {
        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("a refused tracker gave peers"),
            r = rx.wait_for(|a| announce_of(a, url).last.is_some()) => r.unwrap(),
        };
        announce_of(&status.announces(), url)
            .last
            .clone()
            .unwrap()
            .result
            .unwrap_err()
    }

    #[tokio::test]
    async fn a_tracker_url_that_could_forge_a_request_is_never_asked() {
        // libtorrent ssrf_mitigation: http_tracker_connection::start refuses
        // a URL whose query names an announce argument, on_filter a loopback
        // tracker at a path other than /announce, whatever name it is
        // reached by. Each is refused before any request; the next tier's
        // tracker is asked.
        let (refused, at) = http_listener().await;
        let port = at.port().unwrap();
        let named = |s: &str| Url::parse(&s.replace("PORT", &port.to_string())).unwrap();
        let forged = [
            named("http://127.0.0.1:PORT/announce?passkey=k&INFO_HASH=x"),
            named("http://127.0.0.1:PORT/x?y=1"),
            named("http://[::1]:PORT/api"),
            named("http://tracker.invalid:PORT/announce/../admin"),
        ];
        let (ok, ok_url) = http_listener().await;
        let mut tiers: Vec<Vec<Url>> = forged.iter().map(|u| vec![u.clone()]).collect();
        tiers.push(vec![ok_url]);

        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let resolver = Arc::new(FixedResolver {
            at: Ipv4Addr::LOCALHOST.into(),
        });
        let udp_client =
            UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), Some(resolver));
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            tiers,
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            Arc::new(|| reqwest::Client::builder().no_proxy()),
            udp_client,
            stopped.clone(),
        )
        .unwrap();

        for url in &forged {
            let e = refusal_of(&status, &mut peers, url).await;
            assert!(e.contains("SSRF"), "{url}: {e}");
        }
        let q = answer_with_a_peer(
            &ok,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        // The forged trackers' host heard nothing.
        assert!(refused.accept().now_or_never().is_none());

        drop(peers);
        let q = next_announce(&ok).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
    }

    /// Answers an HTTP request with a redirect to `location`.
    async fn redirect(conn: tokio::net::TcpStream, location: &str) {
        let response = format!(
            "HTTP/1.1 302 Found\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
        );
        let mut response = response.as_bytes();
        while !response.is_empty() {
            conn.writable().await.unwrap();
            match conn.try_write(response) {
                Ok(n) => response = &response[n..],
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                Err(e) => panic!("{e}"),
            }
        }
    }

    #[tokio::test]
    async fn a_redirect_to_a_loopback_path_no_tracker_answers_is_never_fetched() {
        // libtorrent http_connection::on_read restarts the GET at the
        // redirect's URL, whose endpoints on_filter holds to the same rule
        // before connect: the moved request is refused before it is sent.
        let (tracker, url) = http_listener().await;
        let (moved, moved_url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let serve = async {
            let (conn, _) = read_announce(&tracker).await;
            redirect(conn, moved_url.join("/admin").unwrap().as_str()).await;
            std::future::pending::<()>().await
        };
        let e = tokio::select! {
            _ = serve => unreachable!(),
            e = refusal_of(&status, &mut peers, &url) => e,
        };
        assert!(e.contains("SSRF"), "{e}");
        // The moved host heard nothing.
        assert!(moved.accept().now_or_never().is_none());
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    // A client looks no name up: it reaches a host only at the addresses it
    // was built for.
    struct NoNames;

    impl reqwest::dns::Resolve for NoNames {
        fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
            let name = name.as_str().to_owned();
            Box::pin(async move { Err(format!("{name}: no lookups in tests").into()) })
        }
    }

    /// The session's clients, each looking no name up; records every target
    /// a client is built for.
    #[derive(Default)]
    struct Pinned {
        built: Mutex<Vec<(String, u16, Vec<IpAddr>)>>,
    }

    impl HttpClients for Pinned {
        fn client(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::Client> {
            if let Some(t) = &target {
                self.built
                    .lock()
                    .push((t.host.to_owned(), t.port, t.addrs.to_vec()));
            }
            let builder = reqwest::Client::builder()
                .no_proxy()
                .dns_resolver(Arc::new(NoNames));
            Ok(crate::core::net::for_target(builder, target).build()?)
        }
    }

    #[tokio::test]
    async fn every_hop_of_a_tracker_get_goes_only_to_the_addresses_checked_for_it() {
        // libtorrent http_connection::on_resolve hands the endpoints to
        // on_filter, then connect tries those left; a redirect starts over
        // at its own URL. The names are known to the engine's resolver
        // only: a client that looked one up itself would reach nothing.
        let (tracker, at) = http_listener().await;
        let (moved, moved_at) = http_listener().await;
        let (port, moved_port) = (at.port().unwrap(), moved_at.port().unwrap());
        let url = Url::parse(&format!("http://tracker.invalid:{port}/announce")).unwrap();
        let token = CancellationToken::new();
        let udp_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let resolver = Arc::new(FixedResolver {
            at: Ipv4Addr::LOCALHOST.into(),
        });
        let udp_client =
            UdpTrackerClient::new_with_socket(token.clone(), Arc::new(udp_sock), Some(resolver));
        let clients = Arc::new(Pinned::default());
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            vec![vec![url]],
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_ips(),
            clients.clone(),
            udp_client,
            StoppedAnnounces::default(),
        )
        .unwrap()
        .peers;

        let (conn, q) = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = read_announce(&tracker) => r,
        };
        redirect(
            conn,
            &format!("http://moved.invalid:{moved_port}/announce?{q}"),
        )
        .await;
        let q = answer_with_a_peer(
            &moved,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        let loopback = vec![IpAddr::from(Ipv4Addr::LOCALHOST)];
        assert_eq!(
            *clients.built.lock(),
            vec![
                ("tracker.invalid".to_owned(), port, loopback.clone()),
                ("moved.invalid".to_owned(), moved_port, loopback),
            ]
        );
        drop(peers);
        token.cancel();
    }

    struct CountingResolver {
        calls: AtomicUsize,
    }

    impl Resolver for CountingResolver {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<Vec<SocketAddr>>> {
            Box::pin(async move {
                assert_eq!(host, "tracker.invalid");
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![(Ipv4Addr::LOCALHOST, port).into()])
            })
        }
    }

    #[tokio::test]
    async fn test_udp_tracker_hostname_through_custom_resolver() {
        const CONNECTION_ID: u64 = 0x1122_3344_5566_7788;

        let tracker = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let tracker_addr = tracker.local_addr().unwrap();

        let client_sock = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let client_addr = client_sock.local_addr().unwrap();
        let client_sock: Arc<dyn DatagramSocket> = Arc::new(client_sock);

        let resolver = Arc::new(CountingResolver {
            calls: AtomicUsize::new(0),
        });
        let token = CancellationToken::new();
        let client =
            UdpTrackerClient::new_with_socket(token.clone(), client_sock, Some(resolver.clone()));

        let url = Url::parse(&format!(
            "udp://tracker.invalid:{}/announce",
            tracker_addr.port()
        ))
        .unwrap();
        let resolved = udp_tracker_to_socket_addrs(
            url.host().unwrap(),
            url.port().unwrap(),
            client.resolver(),
        )
        .await
        .unwrap();
        assert_eq!(resolver.calls.load(Ordering::SeqCst), 1);
        let addr = match resolved {
            UdpTrackerResolveResult::One(addr) => addr,
            other => panic!("expected one address, got {other:?}"),
        };
        assert_eq!(addr, tracker_addr);

        let fake_tracker = async {
            let mut buf = [0u8; 1024];
            let (len, from): (usize, SocketAddr) = tracker.recv_from(&mut buf).await.unwrap();
            assert_eq!(from, client_addr);
            assert_eq!(len, 16);
            assert_eq!(buf[..8], CONNECTION_ID_MAGIC.to_be_bytes());
            assert_eq!(buf[8..12], ACTION_CONNECT.to_be_bytes());
            let tid = &buf[12..16];

            let mut reply = Vec::new();
            reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
            reply.extend_from_slice(tid);
            reply.extend_from_slice(&CONNECTION_ID.to_be_bytes());
            tracker.send_to(&reply, from).await.unwrap();
        };

        let (connection_id, ()) = tokio::join!(client.get_connection_id(addr), fake_tracker);
        assert_eq!(connection_id.unwrap(), CONNECTION_ID);
        token.cancel();
    }

    #[tokio::test]
    async fn a_udp_answer_has_libtorrents_min_interval() {
        // libtorrent udp_tracker_connection::on_announce_response: BEP 15
        // carries no min interval, so it is 60 s.
        let tracker = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let url = Url::parse(&format!("udp://{}/announce", tracker.local_addr().unwrap())).unwrap();
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        // Answers every connect and announce, the stopped one included.
        let serve = tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            loop {
                let (len, from) = tracker.recv_from(&mut buf).await.unwrap();
                let tid = &buf[12..16];
                let mut reply = Vec::new();
                if len == 16 {
                    reply.extend_from_slice(&ACTION_CONNECT.to_be_bytes());
                    reply.extend_from_slice(tid);
                    reply.extend_from_slice(&1u64.to_be_bytes());
                } else {
                    reply.extend_from_slice(&1u32.to_be_bytes()); // announce
                    reply.extend_from_slice(tid);
                    reply.extend_from_slice(&1800u32.to_be_bytes()); // interval
                    reply.extend_from_slice(&5u32.to_be_bytes()); // leechers
                    reply.extend_from_slice(&3u32.to_be_bytes()); // seeders
                }
                tracker.send_to(&reply, from).await.unwrap();
            }
        });

        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|announces| announces.iter().all(|a| a.last.is_some())) => {
                r.unwrap();
            }
        }
        let announces = status.announces();
        assert_eq!(
            announce_of(&announces, &url).last.as_ref().unwrap().result,
            Ok(AnnounceOk {
                interval: Duration::from_secs(1800),
                min_interval: Duration::from_secs(60),
                seeders: Some(3),
                leechers: Some(5),
            })
        );

        drop(peers);
        stopped.wait_idle().await;
        serve.abort();
        token.cancel();
    }
}
