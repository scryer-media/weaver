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

use crate::core::hash_id::Id20;
use crate::core::net::Resolver;
use crate::tracker::tracker_comms_http;
use crate::tracker::tracker_comms_http::TrackerRequestEvent;
use crate::tracker::tracker_comms_udp;
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
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
#[derive(Debug)]
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

/// The announce table of one [`TrackerComms`]: one entry per tracker it was
/// started with, in the order given, from the start. Clones share the table,
/// which stays readable after the tracker task ends.
#[derive(Clone)]
pub struct TrackerStatus(Arc<tokio::sync::watch::Sender<Vec<TrackerAnnounce>>>);

impl std::fmt::Debug for TrackerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TrackerStatus")
            .field(&*self.0.borrow())
            .finish()
    }
}

impl TrackerStatus {
    fn new<'a>(urls: impl IntoIterator<Item = &'a Url>) -> Self {
        let mut announces: Vec<TrackerAnnounce> = Vec::new();
        for url in urls {
            let url = url.to_string();
            if announces.iter().all(|a| a.url != url) {
                announces.push(TrackerAnnounce {
                    url,
                    in_flight: false,
                    last: None,
                });
            }
        }
        Self(Arc::new(tokio::sync::watch::channel(announces).0))
    }

    /// A snapshot of the table.
    pub fn announces(&self) -> Vec<TrackerAnnounce> {
        self.0.borrow().clone()
    }

    /// Sees every change to the table.
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Vec<TrackerAnnounce>> {
        self.0.subscribe()
    }

    /// Marks an announce to `tracker` as sent; the returned guard records how
    /// it ended.
    fn begin(&self, tracker: &SupportedTracker) -> InFlight<'_> {
        let url = tracker.url().to_string();
        self.0.send_modify(|announces| {
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
        self.status.0.send_modify(|announces| {
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
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        listen_port: Option<u16>,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
    ) -> Option<TrackerTask> {
        Self::start_with_tiers(
            info_hash,
            peer_id,
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
    #[allow(clippy::too_many_arguments)]
    pub fn start_with_tiers(
        info_hash: Id20,
        peer_id: Id20,
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
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?tiers);

        let status = TrackerStatus::new(
            tiers
                .iter()
                .flat_map(|tier| tier.trackers.iter().map(SupportedTracker::url)),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let comms = Arc::new(Self {
            info_hash,
            peer_id,
            stats,
            force_tracker_interval: force_interval,
            tx,
            announce_port: announced_port(listen_port),
            reqwest_client,
            udp_client,
            key: rand::random(),
            started: Mutex::new(vec![None; tiers.len()]),
            tracker_ids: Default::default(),
            status: status.clone(),
        });
        let stop_on_drop = StopOnDrop {
            comms: comms.clone(),
            timeout: stopped.timeout(),
            running: Some(stopped.register()),
        };

        let s = async_stream::stream! {
            use futures::StreamExt;
            let _stop_on_drop = stop_on_drop;
            let mut futures = FuturesUnordered::new();
            for (idx, tier) in tiers.into_iter().enumerate() {
                let span = debug_span!(parent: None, "tracker_tier", tier = idx, info_hash = ?info_hash);
                futures.push(comms.task_tier(idx, tier).instrument(span));
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
            peer_id: &self.peer_id,
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

        let response: reqwest::Response = self.reqwest_client.get(url).send().await?;
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
            peer_id: self.peer_id,
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
        hash_id::Id20,
        net::{DatagramSocket, Resolver},
    };
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parking_lot::Mutex;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use super::{
        AnnounceOk, AnnounceState, ReannounceDelay, StoppedAnnounces, SupportedTracker, Tier,
        TrackerAnnounce, TrackerComms, TrackerCommsStats, TrackerCommsStatsState, TrackerTask,
        UdpTrackerResolveResult, udp_tracker_to_socket_addrs,
    };
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

    /// The next HTTP request, unanswered, and its query string.
    async fn read_announce(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
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
        let request = String::from_utf8(buf).unwrap();
        let path = request.split(' ').nth(1).unwrap().to_owned();
        (conn, path.split_once('?').unwrap().1.to_owned())
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
            Id20::new([1; 20]),
            Id20::new([2; 20]),
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
            Id20::new([1; 20]),
            Id20::new([2; 20]),
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
            Id20::new([1; 20]),
            Id20::new([2; 20]),
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
            Id20::new([1; 20]),
            Id20::new([2; 20]),
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
