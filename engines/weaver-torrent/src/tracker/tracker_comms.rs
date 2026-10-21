// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::HashMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use anyhow::Context;
use backon::BackoffBuilder;
use backon::ExponentialBuilder;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::Identity;
use crate::core::hash_id::{Id20, InfoHashes};
use crate::core::net::Resolver;
use crate::tracker::tracker_comms_http;
use crate::tracker::tracker_comms_http::TrackerRequestEvent;
use crate::tracker::tracker_comms_udp;
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

pub struct TrackerComms {
    info_hash: Id20,
    identity: Identity,
    stats: Arc<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    /// The `port` every announce carries (see [`announced_port`]).
    announce_port: u16,
    reqwest_client: reqwest::Client,
    udp_client: UdpTrackerClient,
    key: u32,
    // Per tier, the tracker that accepted `started` and so is owed `stopped`.
    started: Mutex<Vec<Option<SupportedTracker>>>,
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
    /// Bytes of the selected files still to download, 0 once they are all
    /// here. None while the torrent's size is unknown (a magnet still
    /// resolving its metadata).
    pub left_bytes: Option<u64>,
    pub torrent_state: TrackerCommsStatsState,
    /// The torrent was incomplete when its current run began and has every
    /// wanted piece now. Each tracker hears `completed` (BEP 3) once for such a
    /// run; a torrent that was already complete when it started never sends it.
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

    /// Resolves once the torrent has every wanted piece, at once if it already
    /// has. Lets the tracker task send `completed` without waiting out the
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

/// One tier of a multitracker torrent (BEP 12).
///
/// Only one tracker of a tier is announced to at a time. When it fails, the
/// next one in the tier is tried; the one that answers moves to the front of
/// the tier and stays the tier's tracker while it keeps answering.
#[derive(Clone, Debug)]
struct Tier {
    trackers: Vec<SupportedTracker>,
    current: usize,
}

impl Tier {
    /// `trackers` must not be empty.
    fn new(trackers: Vec<SupportedTracker>) -> Self {
        assert!(!trackers.is_empty(), "a tier needs at least one tracker");
        Self {
            trackers,
            current: 0,
        }
    }

    fn current(&self) -> &SupportedTracker {
        &self.trackers[self.current]
    }

    /// The current tracker answered: move it to the front of the tier.
    fn on_success(&mut self) {
        let tracker = self.trackers.remove(self.current);
        self.trackers.insert(0, tracker);
        self.current = 0;
    }

    /// The current tracker failed: move on to the next one in the tier.
    /// Returns true when every tracker in the tier has failed in turn since
    /// the last one that answered.
    fn on_failure(&mut self) -> bool {
        self.current = (self.current + 1) % self.trackers.len();
        self.current == 0
    }
}

/// What the current tracker of a tier has been told (BEP 3 events).
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
struct AnnounceState {
    started: bool,
    completed: bool,
}

impl AnnounceState {
    /// The event the next regular announce carries.
    fn next_event(&self, stats: &TrackerCommsStats) -> Option<TrackerRequestEvent> {
        if !self.started {
            Some(TrackerRequestEvent::Started)
        } else if stats.finished_during_run && !self.completed {
            Some(TrackerRequestEvent::Completed)
        } else {
            None
        }
    }

    /// The tracker accepted an announce that carried `event`.
    fn on_accepted(&mut self, event: Option<TrackerRequestEvent>) {
        match event {
            Some(TrackerRequestEvent::Started) => self.started = true,
            Some(TrackerRequestEvent::Completed) => self.completed = true,
            Some(TrackerRequestEvent::Stopped) => *self = Self::default(),
            None => {}
        }
    }

    /// Whether this tracker may still be owed `completed`, so the wait for
    /// the next announce should end early when the torrent completes. A
    /// torrent that is live and complete without having finished during this
    /// run is seeding from the start and never owes it.
    fn awaits_completion(&self, stats: &TrackerCommsStats) -> bool {
        !self.completed
            && (stats.finished_during_run
                || !(stats.torrent_state == TrackerCommsStatsState::Live && stats.is_completed()))
    }
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
    fn new(interval: Duration, min_interval: Option<Duration>, forced: Option<Duration>) -> Self {
        let floor = min_interval.unwrap_or_default();
        Self {
            floor,
            regular: forced.unwrap_or(interval).max(floor),
        }
    }
}

/// A successful announce.
struct Announced {
    peers: Vec<SocketAddr>,
    interval: Duration,
    min_interval: Option<Duration>,
    seeders: Option<u32>,
    leechers: Option<u32>,
}

/// What a tracker answered to an announce it accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnounceOk {
    pub interval: Duration,
    pub min_interval: Option<Duration>,
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
    reqwest_client: reqwest::Client,
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
    }

    async fn scrape_http(&self, tracker_url: &Url, info_hash: Id20) -> anyhow::Result<ScrapeOk> {
        let mut url =
            tracker_comms_http::scrape_url(tracker_url).context("the tracker offers no scrape")?;
        let mut query = tracker_comms_http::scrape_querystring(&info_hash);
        if let Some(url_query) = url.query() {
            query.push_str(&format!("&{url_query}"));
        }
        url.set_query(Some(&query));
        let response = self
            .reqwest_client
            .get(url)
            .header(reqwest::header::USER_AGENT, &self.user_agent)
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        tracker_comms_http::parse_scrape(&response.bytes().await?, &info_hash)
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
        reqwest_client: reqwest::Client,
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
                reqwest_client,
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
    /// How long a `stopped` announce may take by default.
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

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
        let trackers: Vec<SupportedTracker> = self
            .comms
            .started
            .lock()
            .iter()
            .flatten()
            .cloned()
            .collect();
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

fn backoff() -> backon::ExponentialBackoff {
    ExponentialBuilder::new()
        .without_max_times()
        .with_jitter()
        .with_factor(2.)
        .with_min_delay(Duration::from_secs(10))
        .with_max_delay(Duration::from_secs(600))
        .build()
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
        listen_port: Option<u16>,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
    ) -> Option<TrackerTask> {
        Self::start_with_tiers(
            info_hashes,
            identity,
            trackers.into_iter().map(|t| vec![t]).collect(),
            stats,
            force_interval,
            listen_port,
            reqwest_client,
            udp_client,
            StoppedAnnounces::default(),
        )
    }

    /// Announce to the trackers of a multitracker torrent (BEP 12): one
    /// tracker per tier at a time, all tiers in parallel. Trackers within a
    /// tier are tried in random order, failing over to the next one.
    ///
    /// Each tracker is sent `started`, then `completed` once if the torrent
    /// finishes during this run (see [`TrackerCommsStats::finished_during_run`]),
    /// and `stopped` when the returned stream is dropped. `stopped` is bounded
    /// and tracked by `stopped`. Each regular announce is recorded in the
    /// returned [`TrackerStatus`]. `listen_port` is the port peers reach this
    /// session on, None when it accepts no connections.
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
        listen_port: Option<u16>,
        reqwest_client: reqwest::Client,
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
                Some(Tier::new(trackers))
            })
            .collect::<Vec<_>>();
        if tiers.is_empty() {
            debug!(?info_hashes, "trackers list is empty");
            return None;
        }

        tracing::trace!(?tiers);

        let status = TrackerStatus::new(
            tiers.iter().flat_map(|tier| tier.trackers.iter()),
            info_hashes.ids20().collect(),
            &identity,
            reqwest_client.clone(),
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
                    announce_port: announced_port(listen_port),
                    reqwest_client: reqwest_client.clone(),
                    udp_client: udp_client.clone(),
                    key,
                    started: Mutex::new(vec![None; tiers.len()]),
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
                for (idx, tier) in tiers.iter().cloned().enumerate() {
                    let span = debug_span!(parent: None, "tracker_tier", tier = idx, info_hash = ?comms.info_hash);
                    futures.push(comms.task_tier(idx, tier).instrument(span));
                }
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

    async fn task_tier(&self, idx: usize, mut tier: Tier) -> anyhow::Result<()> {
        let mut state = AnnounceState::default();
        let mut retry: Option<backon::ExponentialBackoff> = None;
        loop {
            let stats = self.stats.get();
            let event = state.next_event(&stats);
            let tracker = tier.current().clone();
            let in_flight = self.status.begin(&tracker);
            let result = self.announce(&tracker, event, &stats).await;
            in_flight.finish(&result);
            match result {
                Ok(announced) => {
                    tier.on_success();
                    retry = None;
                    state.on_accepted(event);
                    if state.started {
                        self.started.lock()[idx] = Some(tracker);
                    }
                    for peer in announced.peers {
                        self.tx.send(peer).await.context("peer stream closed")?;
                    }

                    let delay = ReannounceDelay::new(
                        announced.interval,
                        announced.min_interval,
                        self.force_tracker_interval,
                    );
                    debug!(?delay, "sleeping after calling tracker");
                    tokio::time::sleep(delay.floor).await;
                    let rest = delay.regular - delay.floor;
                    if state.awaits_completion(&self.stats.get()) {
                        tokio::select! {
                            _ = tokio::time::sleep(rest) => {}
                            _ = self.stats.wait_until_completed() => {
                                trace!("torrent completed, announcing early");
                            }
                        }
                    } else {
                        tokio::time::sleep(rest).await;
                    }
                }
                Err(e) => {
                    debug!(?tracker, "error calling tracker: {e:#}");
                    let tier_exhausted = tier.on_failure();
                    if tier.current() != &tracker {
                        // A different tracker has heard nothing from us yet.
                        state = AnnounceState::default();
                        self.started.lock()[idx] = None;
                    }
                    if tier_exhausted {
                        let delay = retry
                            .get_or_insert_with(backoff)
                            .next()
                            .unwrap_or(Duration::from_secs(600));
                        debug!(retry_in = ?delay, "every tracker in the tier failed");
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }
    }

    async fn announce(
        &self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
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
    }

    async fn announce_http(
        &self,
        tracker_url: &Url,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let trackerid = self.tracker_ids.lock().get(tracker_url).cloned();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.identity.peer_id,
            port: self.announce_port,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: numwant(event),
            key: Some(self.key),
            trackerid: trackerid.as_deref(),
        };

        let mut url = tracker_url.clone();

        let mut queries = request.as_querystring();
        if let Some(url_query) = url.query() {
            queries.push_str(&format!("&{}", url_query));
        }
        url.set_query(Some(&queries));

        let response: reqwest::Response = self
            .reqwest_client
            .get(url)
            .header(reqwest::header::USER_AGENT, &self.identity.user_agent)
            .send()
            .await?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        let bytes = response.bytes().await?;
        if let Ok((error, _)) =
            crate::bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(&bytes)
        {
            anyhow::bail!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            )
        };
        let response =
            crate::bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(&bytes)
                .map_err(|e| {
                    tracing::trace!("error deserializing TrackerResponse: {e:#}");
                    e.into_kind()
                })?
                .0;
        if let Some(id) = &response.tracker_id {
            self.tracker_ids
                .lock()
                .insert(tracker_url.clone(), id.as_ref().to_vec());
        }

        Ok(Announced {
            peers: response.iter_peers().collect(),
            interval: Duration::from_secs(response.interval),
            min_interval: response.min_interval.map(Duration::from_secs),
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
            port: self.announce_port,
        };

        match self.udp_client.announce(addr, request).await {
            Ok(response) => {
                trace!(len = response.addrs.len(), "received announce response");
                Ok(Announced {
                    peers: response.addrs,
                    // UDP trackers (BEP 15) send only an interval.
                    interval: Duration::from_secs(response.interval.max(5) as u64),
                    min_interval: None,
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
        net::{Ipv4Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::core::{
        hash_id::{Id20, Id32, InfoHashes},
        net::{DatagramSocket, Resolver},
    };
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parking_lot::Mutex;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::{
        AnnounceOk, AnnounceState, AutoScrape, ReannounceDelay, ScrapeOk, StoppedAnnounces,
        SupportedTracker, Tier, TrackerAnnounce, TrackerComms, TrackerCommsStats,
        TrackerCommsStatsState, TrackerScrape, TrackerTask, UdpTrackerResolveResult,
        udp_tracker_to_socket_addrs,
    };
    use crate::Identity;
    use crate::tracker::tracker_comms_http::TrackerRequestEvent;
    use crate::tracker::tracker_comms_udp::{
        ACTION_CONNECT, CONNECTION_ID_MAGIC, UdpTrackerClient,
    };

    fn http(name: &str) -> SupportedTracker {
        SupportedTracker::Http(Url::parse(&format!("http://{name}/announce")).unwrap())
    }

    #[test]
    fn test_tier_fails_over_and_promotes_the_tracker_that_answers() {
        let mut tier = Tier::new(vec![http("a"), http("b"), http("c")]);
        assert_eq!(tier.current(), &http("a"));

        // a fails, b fails, c answers: c moves to the front.
        assert!(!tier.on_failure());
        assert_eq!(tier.current(), &http("b"));
        assert!(!tier.on_failure());
        assert_eq!(tier.current(), &http("c"));
        tier.on_success();
        assert_eq!(tier.trackers, vec![http("c"), http("a"), http("b")]);
        assert_eq!(tier.current(), &http("c"));

        // While c keeps answering it stays the tier's tracker.
        tier.on_success();
        assert_eq!(tier.current(), &http("c"));

        // Every tracker failing in turn exhausts the tier and starts over.
        assert!(!tier.on_failure());
        assert!(!tier.on_failure());
        assert!(tier.on_failure());
        assert_eq!(tier.current(), &http("c"));
    }

    #[test]
    fn test_single_tracker_tier_is_exhausted_by_every_failure() {
        let mut tier = Tier::new(vec![http("a")]);
        assert!(tier.on_failure());
        assert!(tier.on_failure());
        assert_eq!(tier.current(), &http("a"));
    }

    fn live(downloaded: u64, left: u64, finished_during_run: bool) -> TrackerCommsStats {
        TrackerCommsStats {
            uploaded_bytes: 0,
            downloaded_bytes: downloaded,
            left_bytes: Some(left),
            torrent_state: TrackerCommsStatsState::Live,
            finished_during_run,
        }
    }

    #[test]
    fn test_completed_is_sent_once_after_finishing_during_the_run() {
        let mut state = AnnounceState::default();
        let downloading = live(10, 90, false);
        assert_eq!(
            state.next_event(&downloading),
            Some(TrackerRequestEvent::Started)
        );
        state.on_accepted(Some(TrackerRequestEvent::Started));
        assert_eq!(state.next_event(&downloading), None);
        assert!(state.awaits_completion(&downloading));

        let finished = live(100, 0, true);
        assert!(state.awaits_completion(&finished));
        assert_eq!(
            state.next_event(&finished),
            Some(TrackerRequestEvent::Completed)
        );
        state.on_accepted(Some(TrackerRequestEvent::Completed));
        assert_eq!(state.next_event(&finished), None);
        assert!(!state.awaits_completion(&finished));
    }

    #[test]
    fn test_completed_is_retried_until_accepted() {
        let mut state = AnnounceState::default();
        state.on_accepted(Some(TrackerRequestEvent::Started));
        let finished = live(100, 0, true);
        // The completed announce failed: nothing was accepted.
        assert_eq!(
            state.next_event(&finished),
            Some(TrackerRequestEvent::Completed)
        );
        assert_eq!(
            state.next_event(&finished),
            Some(TrackerRequestEvent::Completed)
        );
    }

    #[test]
    fn test_no_completed_for_a_torrent_complete_from_the_start() {
        let mut state = AnnounceState::default();
        let seeding = live(0, 0, false);
        assert_eq!(
            state.next_event(&seeding),
            Some(TrackerRequestEvent::Started)
        );
        state.on_accepted(Some(TrackerRequestEvent::Started));
        assert_eq!(state.next_event(&seeding), None);
        assert!(!state.awaits_completion(&seeding));
    }

    #[test]
    fn test_waits_for_completion_before_the_torrent_is_live() {
        let state = AnnounceState::default();
        // A magnet still resolving its metadata: nothing is known yet.
        assert!(state.awaits_completion(&TrackerCommsStats::default()));
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
    fn test_stopped_resets_what_the_tracker_was_told() {
        let mut state = AnnounceState::default();
        state.on_accepted(Some(TrackerRequestEvent::Started));
        state.on_accepted(Some(TrackerRequestEvent::Completed));
        state.on_accepted(Some(TrackerRequestEvent::Stopped));
        assert_eq!(state, AnnounceState::default());
    }

    #[test]
    fn test_min_interval_is_a_floor() {
        let s = Duration::from_secs;
        assert_eq!(
            ReannounceDelay::new(s(1800), Some(s(900)), None),
            ReannounceDelay {
                floor: s(900),
                regular: s(1800)
            }
        );
        // A forced interval below the tracker's min interval is raised to it.
        assert_eq!(
            ReannounceDelay::new(s(1800), Some(s(900)), Some(s(60))),
            ReannounceDelay {
                floor: s(900),
                regular: s(900)
            }
        );
        // An interval below min interval is raised too.
        assert_eq!(
            ReannounceDelay::new(s(60), Some(s(120)), None),
            ReannounceDelay {
                floor: s(120),
                regular: s(120)
            }
        );
        assert_eq!(
            ReannounceDelay::new(s(1800), None, Some(s(60))),
            ReannounceDelay {
                floor: s(0),
                regular: s(60)
            }
        );
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
            Some(6881),
            reqwest::Client::builder().no_proxy().build().unwrap(),
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
            Some(6881),
            reqwest::Client::builder().no_proxy().build().unwrap(),
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

        let q = announce!();
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
            Some(6881),
            reqwest::Client::builder()
                .no_proxy()
                .user_agent("Session/1.0")
                .build()
                .unwrap(),
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
            Some(6881),
            reqwest::Client::builder().no_proxy().build().unwrap(),
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

        let q = announce!("d8:intervali1800e5:peers0:10:tracker id3:t-1e");
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
            Some(6881),
            reqwest::Client::new(),
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
            Some(6881),
            reqwest::Client::builder().no_proxy().build().unwrap(),
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
            start(vec![vec![ok_url.clone()], vec![err_url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;

        // Every tracker is listed before any announce goes out.
        let never = |url: &Url| TrackerAnnounce {
            url: url.to_string(),
            in_flight: false,
            last: None,
        };
        assert_eq!(status.announces(), vec![never(&ok_url), never(&err_url)]);

        // Drive the peer stream while the trackers are called.
        macro_rules! driving {
            ($f:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent no peers"),
                    r = $f => r,
                }
            };
        }

        let ((ok_conn, _), (err_conn, _)) =
            driving!(async { tokio::join!(read_announce(&ok), read_announce(&err)) });
        let announces = status.announces();
        for url in [&ok_url, &err_url] {
            let a = announce_of(&announces, url);
            assert!(a.in_flight, "{url} should be in flight");
            assert_eq!(a.last, None);
        }

        respond(
            ok_conn,
            "d8:completei3e10:incompletei5e8:intervali1800e12:min intervali900e5:peers0:e",
        )
        .await;
        respond(err_conn, "d14:failure reason4:nopee").await;
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
                min_interval: Some(Duration::from_secs(900)),
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
            start(vec![vec![ok_url.clone()], vec![err_url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        assert_eq!(
            status.scrapes(),
            vec![
                TrackerScrape {
                    url: ok_url.to_string(),
                    last: None,
                },
                TrackerScrape {
                    url: err_url.to_string(),
                    last: None,
                },
            ]
        );

        // `ok` accepts the announce with counts, `err` refuses it.
        let ((ok_conn, _), (err_conn, _)) = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = async { tokio::join!(read_announce(&ok), read_announce(&err)) } => r,
        };
        respond(
            ok_conn,
            "d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e",
        )
        .await;
        respond(err_conn, "d14:failure reason4:nopee").await;
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
        assert_eq!(scrapes[0].last.as_ref().unwrap().result, Ok(counts));
        assert_eq!(scrapes[1].last, None);

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
}
