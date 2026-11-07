// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::HashMap;
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
use crate::core::net::{Egress, HttpClients, NamedBy, Resolved, Resolver, is_public};
use crate::tracker::Tracker;
use crate::tracker::tracker_comms_http;
use crate::tracker::tracker_comms_http::Ask;
use crate::tracker::tracker_comms_http::Family;
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
    /// Where an accepted announce's `external ip` goes.
    vote: ExternalIpVote,
    http_clients: Arc<dyn HttpClients>,
    reachable: Reachable,
    udp_client: UdpTrackerClient,
    // The trackers under this hash and what announcing to them has done.
    list: Mutex<TrackerList>,
    // The corrupt and redundant bytes the torrent had counted when each
    // HTTP tracker was last sent `started`: its announces report what grew
    // since.
    started_at: Mutex<HashMap<Url, (u64, u64)>>,
    status: TrackerStatus,
    // Wakes the announce loop when the list changed under it: trackers
    // replaced or an announce forced.
    poke: tokio::sync::Notify,
    /// Bounds the `stopped` announces this hash sends.
    stopped: StoppedAnnounces,
    /// Hands the task an announce loop for another hash the torrent goes by
    /// (see [`TrackerTask::take_over`]).
    grow: tokio::sync::mpsc::UnboundedSender<Arc<TrackerComms>>,
}

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

/// What an announce reports of the torrent (BEP 3). Its corrupt and
/// redundant bytes are the torrent's own counts: an HTTP announce reports
/// what they grew by since that tracker last heard `started`.
#[derive(Default, Clone, Copy, Debug)]
pub struct TrackerCommsStats {
    /// Payload bytes sent to peers during this run.
    pub uploaded_bytes: u64,
    /// Bytes of the pieces downloaded during this run that passed their hash
    /// check. Pieces the torrent already had when the run began are not
    /// counted, and neither is a piece that failed its check.
    pub downloaded_bytes: u64,
    /// Bytes of the pieces downloaded that failed their hash check.
    pub corrupt_bytes: u64,
    /// Bytes of the blocks received and not used: a copy of a block already
    /// had, or one no longer asked of the peer.
    pub redundant_bytes: u64,
    /// Bytes of the whole torrent not yet had, whichever files are selected,
    /// so 0 only for a seed. None while the torrent's size is unknown (a
    /// magnet still resolving its metadata).
    pub left_bytes: Option<u64>,
    /// Every wanted piece is had (libtorrent `torrent::is_finished`): a
    /// seed, or a torrent whose selected files are done.
    pub finished: bool,
    /// The metainfo marks the torrent private.
    pub private: bool,
    pub torrent_state: TrackerCommsStatsState,
    /// The torrent lacked a piece when its current run began, whichever
    /// pieces were wanted then, and is a seed now. Each tracker hears
    /// `completed` (BEP 3) once for such a run; a torrent that was already a
    /// seed when it started never sends it, and neither does one that has
    /// only its selected files.
    pub completed_during_run: bool,
    /// The encryption the torrent's peer connections are made under, which
    /// an HTTP announce tells the tracker.
    pub encryption: crate::EncryptionPolicy,
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

    /// Resolves once every connection of the run that just ended has
    /// closed and counted what it dropped, so a `stopped` announce reports
    /// it (libtorrent `torrent::do_pause` disconnects every peer before
    /// `stop_announcing`). Asked as the run's announcing stops. The default
    /// resolves at once.
    fn run_ended(&self) -> BoxFuture<'static, ()> {
        futures::future::ready(()).boxed()
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

/// Takes the vote of a tracker that accepted an announce: the address it
/// says we are at (`external ip`), then the tracker's own address, the one
/// the answer came from (libtorrent `torrent::tracker_response` hands both
/// to `session_impl::set_external_address` as `source_tracker`).
pub type ExternalIpVote = Arc<dyn Fn(IpAddr, IpAddr) + Send + Sync>;

/// Why the session may not reach an address. The family is judged first, so
/// a refusal of every address names the last rule any of them met.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unreachable {
    /// Of a family the session does not connect over (libtorrent
    /// `errors::announce_skipped`).
    Family,
    /// Blocked by the IP filter (libtorrent `errors::banned_by_ip_filter`).
    Filtered,
}

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Unreachable::Family => "of a family the session does not connect over",
            Unreachable::Filtered => "banned by the IP filter",
        })
    }
}

impl std::error::Error for Unreachable {}

/// No address of a tracker is one it may be asked at, as it came with the
/// torrent (see [`NamedBy::may_ask`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NotPublic;

impl std::fmt::Display for NotPublic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("not public")
    }
}

impl std::error::Error for NotPublic {}

/// The addresses the session may reach (the session's `may_reach`): every
/// address a tracker is asked at, on every hop, passes it.
pub type Reachable = Arc<dyn Fn(IpAddr) -> Result<(), Unreachable> + Send + Sync>;

/// The addresses of `found` the session may reach that a tracker
/// `named_by` may be asked at, in the order found; the others are dropped
/// before any is connected to. With none left the tracker is refused for
/// the last rule they met, its naming judged last, and nothing is counted:
/// the failed announce is the record.
pub(crate) fn only_reachable<A>(
    found: Vec<A>,
    ip: impl Fn(&A) -> IpAddr,
    reachable: &Reachable,
    named_by: NamedBy,
) -> anyhow::Result<Vec<A>> {
    let (mut unreachable, mut not_public) = (None, false);
    let kept: Vec<A> = found
        .into_iter()
        .filter(|a| {
            let ip = ip(a);
            match reachable(ip) {
                Err(why) => unreachable = unreachable.max(Some(why)),
                Ok(()) if !named_by.may_announce_to(ip) => not_public = true,
                Ok(()) => return true,
            }
            false
        })
        .collect();
    let why = match (kept.is_empty(), not_public, unreachable) {
        (true, true, _) => anyhow::Error::new(NotPublic),
        (true, false, Some(why)) => anyhow::Error::new(why),
        _ => return Ok(kept),
    };
    Err(why.context("no address of the tracker may be reached"))
}

/// A tracker of a scheme the session announces over, and who named it. A
/// tracker is its URL: the same URL named again is the same tracker.
#[derive(Clone)]
struct SupportedTracker {
    url: Url,
    udp: bool,
    named_by: NamedBy,
}

impl PartialEq for SupportedTracker {
    fn eq(&self, other: &Self) -> bool {
        self.url == other.url
    }
}

impl Eq for SupportedTracker {}

impl std::hash::Hash for SupportedTracker {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.url.hash(state)
    }
}

impl SupportedTracker {
    fn url(&self) -> &Url {
        &self.url
    }

    fn new(Tracker { url, named_by }: Tracker) -> Option<Self> {
        let udp = match url.scheme() {
            "http" | "https" => false,
            "udp" => true,
            _ => {
                debug!(
                    "unsupported tracker URL: {}",
                    redacted_tracker_url(url.as_str())
                );
                return None;
            }
        };
        Some(Self { url, udp, named_by })
    }
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&redacted_tracker_url(self.url().as_str()))
    }
}

/// A tracker as logs and read-only views show it: its scheme, host and
/// port. Its user, path and query, where a private tracker puts the
/// passkey, are left out.
pub fn redacted_tracker_url(url: &str) -> String {
    let Ok(url) = Url::parse(url) else {
        return "…".to_owned();
    };
    let (scheme, host) = (url.scheme(), url.host_str().unwrap_or_default());
    match url.port_or_known_default() {
        Some(port) => format!("{scheme}://{host}:{port}/…"),
        None => format!("{scheme}://{host}/…"),
    }
}

/// `e` without the URL an HTTP client error names, which carries the
/// tracker's passkey. Every tracker request's error passes through it
/// before it is logged or kept as the tracker's message.
fn without_url(e: anyhow::Error) -> anyhow::Error {
    match e.downcast::<reqwest::Error>() {
        Ok(e) => e.without_url().into(),
        Err(e) => e,
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
    fn new(tracker: SupportedTracker, tier: usize, now: Instant) -> Self {
        Self {
            tracker,
            tier,
            fails: 0,
            next_announce: now,
            min_announce: now,
            updating: false,
            port: None,
            start_sent: false,
            complete_sent: false,
        }
    }

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
/// One tracker of each tier is announced to, the first in it that works
/// (libtorrent `settings_pack::announce_to_all_tiers` on, as qBittorrent
/// sets it by default, and `announce_to_all_trackers` off). A tracker that
/// fails backs off and moves to the end of its tier, so the next one in the
/// tier is tried at once.
#[derive(Debug)]
struct TrackerList {
    /// In tier order.
    entries: Vec<Entry>,
    /// A tracker accepted `completed` (libtorrent `torrent::m_complete_sent`).
    complete_sent: bool,
    /// Trackers forced one at a time, announced to on the next turn when
    /// they may be, whatever the walk would pick (libtorrent
    /// `torrent::announce_tracker_now`).
    forced: Vec<SupportedTracker>,
    /// The run announces until it stops (libtorrent
    /// `torrent::m_announcing`); after that no tracker is due again.
    announcing: bool,
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
                entries.push(Entry::new(tracker.clone(), tier, now));
            }
        }
        Self {
            entries,
            complete_sent: false,
            forced: Vec::new(),
            announcing: true,
        }
    }

    /// The trackers become `tiers`, in the order given, a tracker named
    /// twice kept where first named (libtorrent `tracker_list::replace`). A
    /// tracker kept keeps what announcing to it has done, so it is not sent
    /// `started` again nor let off its back-off; libtorrent starts every
    /// entry afresh. A tracker that is gone is forgotten; those of them that
    /// accepted `started` are returned.
    fn replace(&mut self, tiers: &[Vec<SupportedTracker>], now: Instant) -> Vec<SupportedTracker> {
        let mut old = std::mem::take(&mut self.entries);
        for (tier, trackers) in tiers.iter().enumerate() {
            for tracker in trackers {
                if self.position(tracker).is_some() {
                    continue;
                }
                let entry = match old.iter().position(|e| &e.tracker == tracker) {
                    Some(i) => Entry {
                        tracker: tracker.clone(),
                        tier,
                        ..old.remove(i)
                    },
                    None => Entry::new(tracker.clone(), tier, now),
                };
                self.entries.push(entry);
            }
        }
        let entries = &self.entries;
        self.forced
            .retain(|t| entries.iter().any(|e| &e.tracker == t));
        old.into_iter()
            .filter(|e| e.start_sent)
            .map(|e| e.tracker)
            .collect()
    }

    /// Brings the next announce forward to `at` (libtorrent
    /// `trigger_announce`): every tracker, or `tracker` alone, is due a
    /// second after `at`, or once its `min interval` is past unless
    /// `ignore_min_interval`, which lifts it. A tracker named alone is
    /// announced to on its own as soon as it may be (libtorrent
    /// `torrent::force_tracker_request` with an index); the others are
    /// reached by the walk. A tracker waiting for an answer is not asked
    /// twice, and the answer's intervals overwrite the force, which is then
    /// dropped, as libtorrent's `tracker_response` overwrites what
    /// `trigger_announce` wrote. False when there is no such tracker.
    fn force(
        &mut self,
        at: Instant,
        tracker: Option<&SupportedTracker>,
        ignore_min_interval: bool,
    ) -> bool {
        let mut found = false;
        for entry in self
            .entries
            .iter_mut()
            .filter(|e| tracker.is_none_or(|t| &e.tracker == t))
        {
            entry.next_announce = if ignore_min_interval {
                entry.min_announce = at;
                at
            } else {
                at.max(entry.min_announce)
            } + Duration::from_secs(1);
            found = true;
        }
        if let (true, Some(tracker)) = (found, tracker)
            && !self.forced.contains(tracker)
        {
            self.forced.push(tracker.clone());
        }
        found
    }

    /// The walk of libtorrent `torrent::announce_with_tracker` with
    /// `announce_to_all_tiers` and not `announce_to_all_trackers`, as
    /// qBittorrent sets them: over the `considered` trackers in list order,
    /// in each tier every failing one that `may_announce`, then the first
    /// working one if it may. The trackers after it in its tier wait; the
    /// next tier is walked the same way.
    fn walk(
        &self,
        considered: impl Fn(&Entry) -> bool,
        may_announce: impl Fn(&Entry) -> bool,
    ) -> Vec<usize> {
        let mut picked = Vec::new();
        // The tier whose working tracker the walk has reached.
        let mut settled = None;
        for (i, entry) in self.entries.iter().enumerate() {
            if !considered(entry) || settled == Some(entry.tier) {
                continue;
            }
            if may_announce(entry) {
                picked.push(i);
            }
            if entry.is_working() {
                settled = Some(entry.tier);
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
        let seed = stats.completed_during_run;
        // A tracker forced alone goes now if it may, else the walk and the
        // timer reach it (libtorrent `torrent::announce_tracker_now`); one
        // waiting for an answer is not asked twice.
        let forced = std::mem::take(&mut self.forced)
            .into_iter()
            .filter_map(|t| self.position(&t))
            .filter(|&i| self.entries[i].can_announce(now, seed, port));
        let mut picked: Vec<usize> = forced.collect();
        for i in self.walk(|_| true, |e| e.can_announce(now, seed, port)) {
            if !picked.contains(&i) {
                picked.push(i);
            }
        }
        picked
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
    /// tracker has accepted `completed` already, and `paused` while it has
    /// every piece it wants and is not a seed (`torrent::announce_with_tracker`).
    fn event(&self, entry: &Entry, stats: &TrackerCommsStats) -> Option<TrackerRequestEvent> {
        if !entry.start_sent {
            Some(TrackerRequestEvent::Started)
        } else if stats.completed_during_run && !self.complete_sent && !entry.complete_sent {
            Some(TrackerRequestEvent::Completed)
        } else if stats.finished && !stats.is_completed() {
            Some(TrackerRequestEvent::Paused)
        } else {
            None
        }
    }

    /// When the next regular announce may come due, None while every
    /// tracker it could go to waits for an answer (libtorrent
    /// `torrent::update_tracker_timer`). As in the walk, each tier counts up
    /// to its first working tracker, or the one waiting for an answer.
    fn next_due(&self) -> Option<Instant> {
        let mut next: Option<Instant> = None;
        let mut settled = None;
        for entry in &self.entries {
            if settled == Some(entry.tier) {
                continue;
            }
            if !entry.updating {
                let due = entry.next_announce.max(entry.min_announce);
                next = Some(next.map_or(due, |next| next.min(due)));
            }
            if entry.updating || entry.is_working() {
                settled = Some(entry.tier);
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
            Some(TrackerRequestEvent::Stopped | TrackerRequestEvent::Paused) | None => {}
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

    /// The run stops announcing (libtorrent `torrent::stop_announcing`): no
    /// tracker is due again, and the trackers owed `stopped` are the walk
    /// over those that accepted `started`, each of them due.
    fn stop(&mut self) -> Vec<SupportedTracker> {
        self.announcing = false;
        self.walk(|e| e.start_sent, |_| true)
            .into_iter()
            .map(|i| self.entries[i].tracker.clone())
            .collect()
    }
}

/// Whether the torrent may yet become a seed during this run, so its
/// trackers may yet be owed `completed`. A torrent live and complete
/// without completing during this run was a seed from the start.
fn may_complete(stats: &TrackerCommsStats) -> bool {
    stats.completed_during_run
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
    /// The tracker gave a `failure reason` (libtorrent
    /// `errors::tracker_failure`), rather than an answer that does not
    /// parse.
    failure: bool,
}

impl std::fmt::Display for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for Refused {}

impl Refused {
    fn error(reason: String, interval: Option<Duration>, failure: bool) -> anyhow::Error {
        let interval = interval.unwrap_or(tracker_comms_http::DEFAULT_INTERVAL);
        anyhow::Error::new(Self {
            reason,
            retry_interval: interval.max(MIN_ANNOUNCE_INTERVAL),
            failure,
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
        return Err(Refused::error(
            format!(
                "tracker returned failure. Failure reason: {}",
                error.failure_reason
            ),
            error.interval,
            true,
        ));
    }
    crate::bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(bytes)
        .map(|(response, _)| response)
        .map_err(|e| {
            Refused::error(
                format!(
                    "error deserializing the tracker's answer: {}",
                    e.into_kind()
                ),
                None,
                false,
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
    downloaded: Option<u32>,
    warning: Option<String>,
    /// Our address as the tracker saw it, and the tracker's address the
    /// answer came from: its vote (see [`ExternalIpVote`]).
    seen_as: Option<(IpAddr, IpAddr)>,
    /// The length of an HTTP answer's body, in bytes.
    body_len: Option<usize>,
}

/// What a tracker answered to an announce it accepted (libtorrent
/// `torrent::tracker_response` keeps these on the announce entry).
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
    /// Downloads the tracker has seen finish (`downloaded`, or a scrape's).
    pub downloaded: Option<u32>,
    /// Peers the answer named.
    pub peers: Vec<SocketAddr>,
    /// The tracker's `warning message` (libtorrent `announce_entry::message`).
    pub warning: Option<String>,
}

/// Why an announce was not accepted, as libtorrent tells them apart.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AnnounceErrorKind {
    /// The tracker gave a `failure reason` (libtorrent
    /// `errors::tracker_failure`).
    Refused,
    /// No address of the tracker is of a family the session connects over
    /// (libtorrent `errors::announce_skipped`).
    Skipped,
    /// Anything else: no answer, an answer that does not parse, a filtered
    /// address.
    Failed,
}

/// An announce that was not accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AnnounceError {
    pub kind: AnnounceErrorKind,
    pub message: String,
}

impl std::fmt::Display for AnnounceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<&anyhow::Error> for AnnounceError {
    fn from(e: &anyhow::Error) -> Self {
        let refused = e.chain().find_map(|c| c.downcast_ref::<Refused>());
        let skipped = e
            .chain()
            .any(|c| c.downcast_ref::<Unreachable>() == Some(&Unreachable::Family));
        Self {
            kind: match (refused, skipped) {
                (Some(r), _) if r.failure => AnnounceErrorKind::Refused,
                (_, true) => AnnounceErrorKind::Skipped,
                _ => AnnounceErrorKind::Failed,
            },
            message: format!("{e:#}"),
        }
    }
}

/// The outcome of the most recent announce to a tracker, `stopped`
/// included.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LastAnnounce {
    /// When the announce finished.
    pub at: SystemTime,
    pub result: Result<AnnounceOk, AnnounceError>,
}

/// One tracker of a torrent and how announcing to it is going.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TrackerAnnounce {
    /// The announce URL.
    pub url: String,
    /// An announce to this tracker is waiting for its answer.
    pub in_flight: bool,
    /// None until the tracker has been announced to.
    pub last: Option<LastAnnounce>,
    /// Failures in a row since it last accepted an announce (libtorrent
    /// `announce_infohash::fails`).
    pub fails: u8,
    /// When it is next due, once an announce to it has ended or one was
    /// forced (libtorrent `announce_infohash::next_announce`); None again
    /// once the run has stopped announcing.
    pub next_announce: Option<SystemTime>,
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

/// Scrapes a torrent's trackers under every hash it announces.
struct Scraper {
    info_hashes: Mutex<Vec<Id20>>,
    user_agent: String,
    /// The shape of the torrent's client's requests.
    shape: tracker_comms_http::Shape,
    // In table order, one per URL.
    trackers: Mutex<Vec<SupportedTracker>>,
    http_clients: Arc<dyn HttpClients>,
    reachable: Reachable,
    udp_client: UdpTrackerClient,
}

impl Scraper {
    /// The counts `tracker` gives under any of the torrent's hashes, the
    /// larger of each when both are answered.
    async fn scrape(&self, tracker: &SupportedTracker) -> anyhow::Result<ScrapeOk> {
        let info_hashes = self.info_hashes.lock().clone();
        let results = futures::future::join_all(
            info_hashes
                .into_iter()
                .map(|info_hash| self.scrape_hash(tracker, info_hash)),
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
            if tracker.udp {
                self.scrape_udp(tracker, info_hash)
                    .instrument(trace_span!("udp_scrape", ?tracker))
                    .await
            } else {
                self.scrape_http(tracker, info_hash)
                    .instrument(trace_span!("http_scrape", ?tracker))
                    .await
            }
        })
        .await
        .map_err(without_url)
    }

    async fn scrape_http(
        &self,
        tracker: &SupportedTracker,
        info_hash: Id20,
    ) -> anyhow::Result<ScrapeOk> {
        let url =
            tracker_comms_http::scrape_url(&tracker.url).context("the tracker offers no scrape")?;
        let answer = tracker_comms_http::get(
            &*self.http_clients,
            self.udp_client.resolver(),
            &self.reachable,
            &url,
            tracker.named_by,
            Ask::Scrape,
            None,
            &tracker_comms_http::scrape_querystring(&info_hash, self.shape),
            &self.user_agent,
            self.shape,
        )
        .await?;
        tracker_comms_http::parse_scrape(&answer.body, &info_hash)
    }

    async fn scrape_udp(
        &self,
        tracker: &SupportedTracker,
        info_hash: Id20,
    ) -> anyhow::Result<ScrapeOk> {
        let url = &tracker.url;
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
        let addrs = udp_tracker_to_socket_addrs(
            host,
            port,
            self.udp_client.resolver(),
            &self.reachable,
            tracker.named_by,
        )
        .await?;
        match addrs {
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
    /// The announce loops of the tracker task, one per hash, while it runs.
    runs: Mutex<Vec<std::sync::Weak<TrackerComms>>>,
}

/// One entry per tracker of `tiers` that the session can announce to, in
/// order, a tracker named twice kept where first named.
fn supported_tiers(tiers: Vec<Vec<Tracker>>) -> Vec<Vec<SupportedTracker>> {
    tiers
        .into_iter()
        .map(|urls| urls.into_iter().filter_map(SupportedTracker::new).collect())
        .filter(|tier: &Vec<SupportedTracker>| !tier.is_empty())
        .collect()
}

fn unique<'a>(trackers: impl IntoIterator<Item = &'a SupportedTracker>) -> Vec<SupportedTracker> {
    let mut unique: Vec<SupportedTracker> = Vec::new();
    for tracker in trackers {
        if !unique.contains(tracker) {
            unique.push(tracker.clone());
        }
    }
    unique
}

/// `instant` on the wall clock.
fn wall(instant: Instant, now: Instant, at: SystemTime) -> SystemTime {
    match instant.checked_duration_since(now) {
        Some(ahead) => at + ahead,
        None => at - now.duration_since(instant),
    }
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
        reachable: Reachable,
        udp_client: UdpTrackerClient,
    ) -> Self {
        let unique = unique(trackers);
        let announces = unique
            .iter()
            .map(|t| TrackerAnnounce {
                url: t.url().to_string(),
                ..Default::default()
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
                info_hashes: Mutex::new(info_hashes),
                user_agent: identity.user_agent.clone(),
                shape: tracker_comms_http::Shape::of(&identity.peer_id),
                trackers: Mutex::new(unique),
                http_clients,
                reachable,
                udp_client,
            },
            runs: Default::default(),
        }))
    }

    /// The trackers become `tiers`, each with who named it: the table lists
    /// them in order, a tracker kept keeping its row, and a
    /// running task announces to them from now on. An announce waiting on a
    /// tracker that is gone is abandoned.
    pub fn replace(&self, tiers: Vec<Vec<Tracker>>) {
        self.replace_tiers(&supported_tiers(tiers));
    }

    /// [`Self::replace`], with each running hash and the trackers gone from
    /// it that accepted `started`.
    fn replace_tiers(
        &self,
        tiers: &[Vec<SupportedTracker>],
    ) -> Vec<(Arc<TrackerComms>, Vec<SupportedTracker>)> {
        let trackers = unique(tiers.iter().flatten());
        self.0.announces.send_modify(|announces| {
            *announces = trackers
                .iter()
                .map(|t| {
                    let url = t.url().as_str();
                    announces
                        .iter()
                        .find(|a| a.url == url)
                        .cloned()
                        .unwrap_or_else(|| TrackerAnnounce {
                            url: url.to_owned(),
                            ..Default::default()
                        })
                })
                .collect();
        });
        self.0.scrapes.send_modify(|scrapes| {
            *scrapes = trackers
                .iter()
                .map(|t| {
                    let url = t.url().as_str();
                    scrapes
                        .iter()
                        .find(|s| s.url == url)
                        .cloned()
                        .unwrap_or_else(|| TrackerScrape {
                            url: url.to_owned(),
                            last: None,
                        })
                })
                .collect();
        });
        *self.0.scraper.trackers.lock() = trackers;
        self.runs()
            .into_iter()
            .map(|comms| {
                let gone = comms.list.lock().replace(tiers, Instant::now());
                comms.publish(Instant::now(), SystemTime::now());
                comms.poke.notify_one();
                (comms, gone)
            })
            .collect()
    }

    /// Brings the next announce forward to `after` from now, to every
    /// tracker or to `tracker` alone (libtorrent
    /// `torrent_handle::force_reannounce`, `torrent::force_tracker_request`
    /// and `force_tracker_request_url`). `ignore_min_interval` lifts the
    /// trackers' `min interval`. An announce already waiting for its answer
    /// is not sent again. Nothing happens unless the task runs (libtorrent
    /// returns while the torrent is paused). False when `tracker` is not
    /// one of the torrent's.
    pub fn reannounce(
        &self,
        after: Duration,
        tracker: Option<&Url>,
        ignore_min_interval: bool,
    ) -> bool {
        // A tracker is its URL, whoever named it.
        let tracker = match tracker.map(|url| SupportedTracker::new(Tracker::torrent(url.clone())))
        {
            Some(None) => return false,
            Some(Some(t)) => Some(t),
            None => None,
        };
        let known = tracker.as_ref().is_none_or(|t| {
            let url = t.url().as_str();
            self.0.announces.borrow().iter().any(|a| a.url == url)
        });
        if known {
            for comms in self.runs() {
                let at = Instant::now() + after;
                comms
                    .list
                    .lock()
                    .force(at, tracker.as_ref(), ignore_min_interval);
                comms.publish(Instant::now(), SystemTime::now());
                comms.poke.notify_one();
            }
        }
        known
    }

    /// The table of a run that follows `before`'s: each tracker both list
    /// keeps the last answer `before` had from it, announces and scrapes
    /// alike, until this run has one of its own. A resumed torrent's
    /// trackers read as they last answered while `started` is on its way.
    pub(crate) fn follow(&self, before: &TrackerStatus) {
        let announces = before.announces();
        self.0.announces.send_if_modified(|rows| {
            let mut modified = false;
            for row in rows.iter_mut().filter(|row| row.last.is_none()) {
                if let Some(last) = announces
                    .iter()
                    .find(|kept| kept.url == row.url)
                    .and_then(|kept| kept.last.clone())
                {
                    row.last = Some(last);
                    modified = true;
                }
            }
            modified
        });
        let scrapes = before.scrapes();
        self.0.scrapes.send_if_modified(|rows| {
            let mut modified = false;
            for row in rows.iter_mut().filter(|row| row.last.is_none()) {
                if let Some(last) = scrapes
                    .iter()
                    .find(|kept| kept.url == row.url)
                    .and_then(|kept| kept.last.clone())
                {
                    row.last = Some(last);
                    modified = true;
                }
            }
            modified
        });
    }

    fn runs(&self) -> Vec<Arc<TrackerComms>> {
        let mut runs = self.0.runs.lock();
        runs.retain(|run| run.strong_count() > 0);
        runs.iter().filter_map(std::sync::Weak::upgrade).collect()
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
        let trackers = self.0.scraper.trackers.lock();
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
                let before = (accepted.seeders, accepted.leechers, accepted.downloaded);
                accepted.seeders = counts.complete.or(accepted.seeders);
                accepted.leechers = counts.incomplete.or(accepted.leechers);
                accepted.downloaded = counts.downloaded.or(accepted.downloaded);
                before != (accepted.seeders, accepted.leechers, accepted.downloaded)
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
/// the announce was abandoned (the tracker task was dropped, or the tracker
/// removed).
struct InFlight<'a> {
    status: &'a TrackerStatus,
    url: String,
    last: Option<LastAnnounce>,
}

impl InFlight<'_> {
    fn finish(mut self, last: LastAnnounce) {
        self.last = Some(last);
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

impl TrackerTask {
    /// The task, taken over by the run that follows the one that started it
    /// (a magnet's metadata fetch, then its download): it announces to
    /// `tiers` under every one of `info_hashes` from now on, and keeps what the trackers heard from it,
    /// `started`, their intervals and their tracker ids, so they hear one
    /// torrent. A tracker the run leaves out that accepted `started` is sent
    /// `stopped`, as dropping the task would send it. A hash the task did
    /// not announce under is announced under from now on, as one more hash
    /// of the same torrent. A task that announced under none of
    /// `info_hashes` was another torrent's: it is dropped, which sends
    /// `stopped`, and `None` is returned.
    pub(crate) fn take_over(
        self,
        info_hashes: InfoHashes,
        tiers: Vec<Vec<Tracker>>,
    ) -> Option<Self> {
        let runs = self.status.runs();
        if !runs
            .iter()
            .any(|comms| info_hashes.answers_to(comms.info_hash))
        {
            return None;
        }
        let tiers = supported_tiers(tiers);
        for (comms, gone) in self.status.replace_tiers(&tiers) {
            comms.send_stopped(gone, comms.stopped.register());
        }
        if let Some(first) = runs.first() {
            for info_hash in info_hashes.ids20() {
                if runs.iter().all(|comms| comms.info_hash != info_hash) {
                    first.grow(info_hash, &tiers);
                }
            }
        }
        Some(self)
    }
}

/// Bounds the `stopped` announce (BEP 3) a tracker task sends when its peer
/// stream is dropped, and lets a shutdown wait for those announces.
///
/// Clones share their state: create one per session and hand a clone to every
/// [`TrackerComms::start_with_tiers`] call.
#[derive(Clone)]
pub struct StoppedAnnounces {
    timeout: Duration,
    // How long a `stopped` announce waits for its run's connections to
    // close and count what they cut, before its own `timeout` starts.
    run_end: Duration,
    // Set once the address these announces would leave from may no longer
    // carry them: from then on they take no time, so none is sent.
    withheld: Arc<std::sync::atomic::AtomicBool>,
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

    /// How long a `stopped` announce waits by default for its run's
    /// connections to close. They close as soon as the run is cancelled;
    /// this only bounds one that does not, and the announce then goes with
    /// the counters as they are and its own bound whole.
    pub const RUN_END_TIMEOUT: Duration = Duration::from_secs(2);

    /// `timeout` bounds each `stopped` announce; zero sends none.
    pub fn new(timeout: Duration) -> Self {
        Self {
            timeout,
            run_end: Self::RUN_END_TIMEOUT,
            withheld: Default::default(),
            running: Arc::new(tokio::sync::watch::channel(0).0),
        }
    }

    /// The bound on each `stopped` announce; zero once withheld.
    pub fn timeout(&self) -> Duration {
        if self.withheld.load(std::sync::atomic::Ordering::Acquire) {
            return Duration::ZERO;
        }
        self.timeout
    }

    /// `run_end` bounds the wait for a run's connections to close before
    /// its `stopped` announce, apart from the announce's own bound.
    pub fn run_end_within(mut self, run_end: Duration) -> Self {
        self.run_end = run_end;
        self
    }

    /// The most a `stopped` announce takes from its run's end: the wait for
    /// the run's connections, then the announce; zero once withheld.
    pub fn whole(&self) -> Duration {
        match self.timeout() {
            Duration::ZERO => Duration::ZERO,
            timeout => self.run_end + timeout,
        }
    }

    /// Sends no `stopped` announce from now on, through any clone.
    pub fn withhold(&self) {
        self.withheld
            .store(true, std::sync::atomic::Ordering::Release);
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

    // The tracker tasks and `stopped` announces that hold these settings.
    #[cfg(test)]
    fn running(&self) -> usize {
        *self.running.borrow()
    }
}

struct RunningGuard(Arc<tokio::sync::watch::Sender<usize>>);

impl Drop for RunningGuard {
    fn drop(&mut self) {
        self.0.send_modify(|r| *r -= 1);
    }
}

/// Lives inside the peer stream. When the stream is dropped (the torrent was
/// paused, removed or the session is stopping), the run stops announcing,
/// and every tracker that accepted `started` is sent `stopped` with the
/// final counters, each recorded in the table as any announce is, unless
/// the stopped announces may take no time at all: then none is sent
/// (libtorrent `torrent::announce_with_tracker` with
/// `stop_tracker_timeout <= 0`).
struct StopOnDrop {
    comms: Arc<TrackerComms>,
    running: Option<RunningGuard>,
}

impl StopOnDrop {
    fn new(comms: Arc<TrackerComms>) -> Self {
        let running = Some(comms.stopped.register());
        Self { comms, running }
    }
}

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        let trackers = self.comms.list.lock().stop();
        self.comms.publish(Instant::now(), SystemTime::now());
        if let Some(running) = self.running.take() {
            self.comms.send_stopped(trackers, running);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

/// Where a UDP tracker at `host`, named by `named_by`, is asked: of the
/// addresses `resolver` finds that it may be asked at and the session may
/// reach (see [`only_reachable`]), the first IPv4 and the first IPv6. A
/// `udp://` URL keeps its host as written, so a host that reads as an
/// address is that address and is not looked up. A datagram goes to an
/// address, so a name the session's network resolves where it exits cannot
/// be asked over UDP.
async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
    resolver: &dyn Resolver,
    reachable: &Reachable,
    named_by: NamedBy,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let found: Vec<SocketAddr> = match host {
        url::Host::Domain(name)
            if let Ok(ip) = name
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<IpAddr>() =>
        {
            vec![(ip, port).into()]
        }
        url::Host::Domain(name) => match resolver
            .resolve(name, port)
            .await
            .with_context(|| format!("error looking up hostname {name}"))?
        {
            Resolved::Addresses(found) => found,
            Resolved::AtExit => {
                anyhow::bail!("{name} is resolved where the leg exits, and UDP needs its address")
            }
        },
        url::Host::Ipv4(addr) => vec![(addr, port).into()],
        url::Host::Ipv6(addr) => vec![(addr, port).into()],
    };
    let mut v4: Option<SocketAddrV4> = None;
    let mut v6: Option<SocketAddrV6> = None;
    for addr in only_reachable(found, SocketAddr::ip, reachable, named_by)? {
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
        _ => anyhow::bail!("zero addresses returned looking up {host}"),
    };
    trace!(?res, "resolved");
    Ok(res)
}

/// The `port` of an announce: the port peers reach this session on, or 0
/// when it accepts no connections. Port 0 is the engine's one form for
/// "unreachable": a peer at port 0 is never dialled and a DHT node at port 0
/// is never used, so a session without a listener names no port anyone
/// could answer on.
fn announced_port(listen_port: Option<u16>) -> u16 {
    listen_port.unwrap_or(0)
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
    /// Announce to the trackers of a multitracker torrent (BEP 12): one
    /// tracker of each tier at a time, the first in it that works, the
    /// trackers of each tier in random order (see `TrackerList`). A failing tracker
    /// is tried again after a delay that grows with each failure
    /// (`retry_delay`).
    ///
    /// Each tracker is sent `started`, then `completed` if the torrent
    /// becomes a seed during this run (see
    /// [`TrackerCommsStats::completed_during_run`]) and no tracker has
    /// accepted it yet, and `stopped` when the returned stream is dropped. `stopped` is bounded
    /// and tracked by `stopped`. Each announce, `stopped` included, is
    /// recorded in the returned [`TrackerStatus`]. `listen_port` is the port peers reach this
    /// session on, None when it accepts no connections; when it changes, each
    /// tier announces it as soon as its tracker's `min interval` allows.
    /// An announce names none of our addresses: the tracker takes the one it
    /// was reached from. `vote` takes the address an accepted answer says is
    /// ours.
    /// A tracker, HTTP or UDP, announce or scrape, every hop, is asked only
    /// at the addresses `reachable` lets through, and only at public ones
    /// unless the operator named it (see [`NamedBy::may_ask`]).
    ///
    /// Each of `info_hashes.ids20()` is announced on its own, with its own
    /// events and failover: a hybrid under v1 and the truncated v2, a v2
    /// torrent under the truncated v2: one announce per protocol version.
    ///
    /// The task runs with no tracker at all, so trackers given later through
    /// [`TrackerStatus::replace`] are announced to.
    #[allow(clippy::too_many_arguments)]
    pub fn start_with_tiers(
        info_hashes: InfoHashes,
        identity: Identity,
        tiers: Vec<Vec<Tracker>>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        listen_port: ListenPort,
        vote: ExternalIpVote,
        http_clients: Arc<dyn HttpClients>,
        reachable: Reachable,
        udp_client: UdpTrackerClient,
        stopped: StoppedAnnounces,
    ) -> TrackerTask {
        let mut tiers = supported_tiers(tiers);
        for trackers in &mut tiers {
            trackers.shuffle(&mut rand::rng());
        }
        tracing::trace!(?tiers);

        let status = TrackerStatus::new(
            tiers.iter().flatten(),
            info_hashes.ids20().collect(),
            &identity,
            http_clients.clone(),
            reachable.clone(),
            udp_client.clone(),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);
        let (grow, mut grown) = tokio::sync::mpsc::unbounded_channel::<Arc<TrackerComms>>();

        let stats: Arc<dyn TorrentStatsProvider> = stats.into();
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
                    vote: vote.clone(),
                    http_clients: http_clients.clone(),
                    reachable: reachable.clone(),
                    udp_client: udp_client.clone(),
                    list: Mutex::new(TrackerList::new(&tiers, Instant::now())),
                    started_at: Default::default(),
                    status: status.clone(),
                    poke: tokio::sync::Notify::new(),
                    stopped: stopped.clone(),
                    grow: grow.clone(),
                })
            })
            .collect::<Vec<_>>();
        drop(tx);
        drop(grow);
        *status.0.runs.lock() = comms.iter().map(Arc::downgrade).collect();
        let stop_on_drop = comms
            .iter()
            .cloned()
            .map(StopOnDrop::new)
            .collect::<Vec<_>>();

        let s = async_stream::stream! {
            use futures::StreamExt;
            let mut stop_on_drop = stop_on_drop;
            let mut futures = FuturesUnordered::new();
            for comms in comms {
                futures.push(comms.running());
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    Some(comms) = grown.recv() => {
                        stop_on_drop.push(StopOnDrop::new(comms.clone()));
                        futures.push(comms.running());
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        if let Some(Err(e)) = e {
                            debug!("error: {e:#}");
                        }
                    }
                }
            }
            drop(stop_on_drop);
        };

        TrackerTask {
            peers: s.boxed(),
            status,
        }
    }

    /// [`Self::run`] in its own span, owning its hash.
    fn running(self: Arc<Self>) -> impl std::future::Future<Output = anyhow::Result<()>> {
        let span = debug_span!(parent: None, "tracker", info_hash = ?self.info_hash);
        async move { self.run().await }.instrument(span)
    }

    /// Announces under `info_hash` too, to `tiers`, as one more hash of this
    /// torrent: the same key, identity, counters and peer stream, in the
    /// same task, stopped with it.
    fn grow(&self, info_hash: Id20, tiers: &[Vec<SupportedTracker>]) {
        let comms = Arc::new(Self {
            info_hash,
            identity: self.identity.clone(),
            stats: self.stats.clone(),
            force_tracker_interval: self.force_tracker_interval,
            tx: self.tx.clone(),
            announce_port: self.announce_port.clone(),
            vote: self.vote.clone(),
            http_clients: self.http_clients.clone(),
            reachable: self.reachable.clone(),
            udp_client: self.udp_client.clone(),
            list: Mutex::new(TrackerList::new(tiers, Instant::now())),
            started_at: Default::default(),
            status: self.status.clone(),
            poke: tokio::sync::Notify::new(),
            stopped: self.stopped.clone(),
            grow: self.grow.clone(),
        });
        if self.grow.send(comms.clone()).is_err() {
            return;
        }
        self.status.0.runs.lock().push(Arc::downgrade(&comms));
        self.status.0.scraper.info_hashes.lock().push(info_hash);
    }

    /// Sends `stopped` to `trackers`, which accepted `started`, with the
    /// counters as they are once the run's connections closed, each
    /// recorded in the table as any announce is, within the stopped
    /// announces' bound; none when that bound is zero. The wait for the
    /// connections has its own bound, so a run slow to end still leaves the
    /// announces theirs whole. `running` is held until they end.
    fn send_stopped(self: &Arc<Self>, trackers: Vec<SupportedTracker>, running: RunningGuard) {
        let timeout = self.stopped.timeout();
        if timeout.is_zero() || trackers.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            debug!("no runtime to send the stopped announce on");
            return;
        };
        let comms = self.clone();
        let run_end = self.stopped.run_end;
        let ended = comms.stats.run_ended();
        let span = debug_span!(parent: None, "tracker_stopped", info_hash = ?comms.info_hash);
        runtime.spawn(
            async move {
                let _running = running;
                // The counters once the run's connections have counted what
                // they dropped; within their bound, as they are if not.
                if tokio::time::timeout(run_end, ended).await.is_err() {
                    debug!(?run_end, "the run's connections did not close in time");
                }
                let stats = comms.stats.get();
                let announces = trackers.into_iter().map(|tracker| {
                    comms.announce_recorded(tracker, Some(TrackerRequestEvent::Stopped), stats)
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

    /// Writes each tracker's failures and next announce to the table, `now`
    /// being `at` on the wall clock.
    fn publish(&self, now: Instant, at: SystemTime) {
        let list = self.list.lock();
        self.status.0.announces.send_if_modified(|announces| {
            let mut modified = false;
            for entry in &list.entries {
                let url = entry.tracker.url().as_str();
                let Some(row) = announces.iter_mut().find(|a| a.url == url) else {
                    continue;
                };
                let asked = entry.start_sent || entry.fails > 0;
                let next = (list.announcing && asked)
                    .then(|| wall(entry.next_announce.max(entry.min_announce), now, at));
                if (row.fails, row.next_announce) != (entry.fails, next) {
                    (row.fails, row.next_announce) = (entry.fails, next);
                    modified = true;
                }
            }
            modified
        });
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
        // Ends the announce to a tracker that has been removed.
        let mut aborts: HashMap<SupportedTracker, futures::future::AbortHandle> = HashMap::new();
        loop {
            aborts.retain(|tracker, abort| {
                let kept = self.list.lock().position(tracker).is_some();
                if !kept {
                    abort.abort();
                }
                kept && !abort.is_aborted()
            });
            let stats = self.stats.get();
            let current = *port.borrow_and_update();
            let due = self.list.lock().start_due(Instant::now(), current, &stats);
            for (tracker, event) in due {
                let (announce, abort) = futures::future::abortable(self.announce_recorded(
                    tracker.clone(),
                    event,
                    stats,
                ));
                aborts.insert(tracker, abort);
                in_flight.push(announce);
            }
            let next_due = self.list.lock().next_due();
            let next_due = async {
                match next_due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(answer) = in_flight.next() => {
                    if let Ok((tracker, peers)) = answer {
                        aborts.remove(&tracker);
                        for peer in peers {
                            self.tx.send(peer).await.context("peer stream closed")?;
                        }
                    }
                }
                _ = next_due => {}
                _ = self.poke.notified() => {}
                Ok(()) = port.changed() => {
                    trace!("announce port changed, announcing early");
                }
                _ = self.stats.wait_until_completed(), if !completion_seen && may_complete(&stats) => {
                    completion_seen = true;
                    if self.stats.get().completed_during_run {
                        trace!("torrent completed, announcing early");
                        self.list.lock().completed(Instant::now());
                    }
                }
            }
        }
    }

    /// An announce, its outcome told to the list and recorded in the status
    /// table, and the address an accepted answer says is ours handed to the
    /// vote; the peers it found.
    async fn announce_recorded(
        &self,
        tracker: SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: TrackerCommsStats,
    ) -> (SupportedTracker, Vec<SocketAddr>) {
        let in_flight = self.status.begin(&tracker);
        let result = self.announce(&tracker, event, &stats).await;
        let (now, at) = (Instant::now(), SystemTime::now());
        let (result, peers) = match result {
            Ok(announced) => {
                let delay = ReannounceDelay::new(
                    announced.interval,
                    announced.min_interval,
                    self.force_tracker_interval,
                );
                debug!(
                    ?tracker,
                    ?event,
                    peers = announced.peers.len(),
                    peers6 = announced.peers.iter().filter(|p| p.is_ipv6()).count(),
                    seeders = ?announced.seeders,
                    leechers = ?announced.leechers,
                    interval = ?announced.interval,
                    min_interval = ?announced.min_interval,
                    external_ip = announced.seen_as.is_some(),
                    body_len = ?announced.body_len,
                    ?delay,
                    "tracker answered"
                );
                self.list.lock().on_success(&tracker, event, now, &delay);
                if let Some((ours, by)) = announced.seen_as {
                    (self.vote)(ours, by);
                }
                let ok = AnnounceOk {
                    interval: announced.interval,
                    min_interval: announced.min_interval,
                    seeders: announced.seeders,
                    leechers: announced.leechers,
                    downloaded: announced.downloaded,
                    peers: announced.peers.clone(),
                    warning: announced.warning,
                };
                (Ok(ok), announced.peers)
            }
            Err(e) => {
                debug!(?tracker, "error calling tracker: {e:#}");
                let retry_interval = Refused::retry_interval(&e);
                self.list.lock().on_failure(&tracker, now, retry_interval);
                (Err(AnnounceError::from(&e)), Vec::new())
            }
        };
        self.publish(now, at);
        in_flight.finish(LastAnnounce { at, result });
        (tracker, peers)
    }

    async fn announce(
        &self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        within_completion_timeout(async {
            if tracker.udp {
                self.announce_udp(tracker, event, stats)
                    .instrument(trace_span!("udp_tracker", ?tracker))
                    .await
            } else {
                self.announce_http(tracker, event, stats)
                    .instrument(trace_span!("http_tracker", ?tracker))
                    .await
            }
        })
        .await
        .map_err(without_url)
    }

    /// The corrupt and redundant bytes counted since `tracker` last heard
    /// `started`; an announce that is `started` begins the count again. A
    /// tracker that heard no `started` from this run hears none counted.
    fn since_started(
        &self,
        tracker: &Url,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> (u64, u64) {
        let now = (stats.corrupt_bytes, stats.redundant_bytes);
        let mut started_at = self.started_at.lock();
        if event == Some(TrackerRequestEvent::Started) {
            started_at.insert(tracker.clone(), now);
        }
        let (corrupt, redundant) = started_at.get(tracker).copied().unwrap_or(now);
        (
            now.0.saturating_sub(corrupt),
            now.1.saturating_sub(redundant),
        )
    }

    async fn announce_http(
        &self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let tracker_url = &tracker.url;
        let trackerid = self.identity.tracker_ids.get(tracker_url);
        let (corrupt, redundant) = self.since_started(tracker_url, event, stats);
        let routes = self.http_routes(tracker_url).await;
        // libtorrent lists a private torrent's own public addresses, those
        // its announces leave from; through a proxy it knows none.
        let own_addrs: Vec<IpAddr> = routes
            .iter()
            .filter_map(|(_, from)| *from)
            .filter(|ip| stats.private && is_public(*ip))
            .collect();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.identity.peer_id,
            port: announced_port(*self.announce_port.borrow()),
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            corrupt,
            redundant,
            compact: true,
            no_peer_id: false,
            event,
            numwant: numwant(event),
            key: Some(self.identity.tracker_key),
            trackerid: trackerid.as_deref(),
            encryption: stats.encryption,
            own_addrs: &own_addrs,
        };
        let query = request.as_querystring();

        // One announce per route, at once; the peers of every answer are
        // kept, the rest of the first, as UDP's two families are.
        let answers = futures::future::join_all(routes.iter().map(|(family, _)| {
            tracker_comms_http::get(
                &*self.http_clients,
                self.udp_client.resolver(),
                &self.reachable,
                tracker_url,
                tracker.named_by,
                Ask::Announce,
                *family,
                &query,
                &self.identity.user_agent,
                tracker_comms_http::Shape::of(&self.identity.peer_id),
            )
        }))
        .await;
        let (mut announced, mut error) = (None::<Announced>, None);
        for answer in answers {
            match answer.and_then(|answer| self.read_http_announce(tracker_url, &answer)) {
                Ok(more) => match &mut announced {
                    Some(announced) => announced.peers.extend(more.peers),
                    None => announced = Some(more),
                },
                Err(e) => {
                    error.get_or_insert(e);
                }
            }
        }
        match (announced, error) {
            (Some(announced), _) => Ok(announced),
            (None, Some(error)) => Err(error),
            (None, None) => anyhow::bail!("the tracker was not announced to"),
        }
    }

    /// The announces an HTTP tracker at `url` is sent: one per family the
    /// session routes to the tracker by itself, held to that family, with
    /// the address it leaves from, as libtorrent announces once from each
    /// listen socket that can route to the tracker. One held to no family,
    /// from no known address, when a proxy dials the tracker (libtorrent's
    /// one proxy socket), the name is resolved where the leg exits, or no
    /// family routes, so the GET fails in its own words.
    async fn http_routes(&self, url: &Url) -> Vec<(Option<Family>, Option<IpAddr>)> {
        let one = vec![(None, None)];
        let Ok((_, found)) = tracker_comms_http::addresses(url, self.udp_client.resolver()).await
        else {
            return one;
        };
        let port = url.port_or_known_default().unwrap_or(0);
        let mut routes = Vec::new();
        for family in [Family::V4, Family::V6] {
            let Some(ip) = found.iter().find(|ip| Family::of(**ip) == family) else {
                continue;
            };
            match self.http_clients.egress(SocketAddr::new(*ip, port)) {
                Egress::Proxied => return one,
                Egress::NoRoute => {}
                Egress::From(from) => routes.push((Some(family), Some(from))),
            }
        }
        if routes.is_empty() { one } else { routes }
    }

    /// An HTTP tracker's answer to an announce, read.
    fn read_http_announce(
        &self,
        tracker_url: &Url,
        answer: &tracker_comms_http::Answer,
    ) -> anyhow::Result<Announced> {
        let response = read_answer(&answer.body)?;
        if let Some(id) = &response.tracker_id {
            self.identity.tracker_ids.set(tracker_url, id.as_ref());
        }
        Ok(Announced {
            peers: response.iter_peers().collect(),
            interval: response.interval,
            min_interval: response.min_interval,
            seeders: response.complete.map(saturating_u32),
            leechers: response.incomplete.map(saturating_u32),
            downloaded: response.downloaded.map(saturating_u32),
            warning: response
                .warning_message
                .as_ref()
                .map(|m| String::from_utf8_lossy(m.as_ref()).into_owned()),
            seen_as: response.external_ip().zip(answer.from),
            body_len: Some(answer.body.len()),
        })
    }

    async fn announce_udp(
        &self,
        tracker: &SupportedTracker,
        event: Option<TrackerRequestEvent>,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let url = &tracker.url;
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );
        let addrs = udp_tracker_to_socket_addrs(
            host.clone(),
            port,
            self.udp_client.resolver(),
            &self.reachable,
            tracker.named_by,
        )
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
            key: self.identity.tracker_key,
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
                    downloaded: None,
                    warning: None,
                    // A UDP answer (BEP 15) names no address of ours.
                    seen_as: None,
                    body_len: None,
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
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use crate::core::{
        hash_id::{Id20, Id32, InfoHashes},
        net::{ClientShape, Egress, HttpClients, HttpTarget, NamedBy, Resolver},
    };
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    use parking_lot::Mutex;
    use tokio_util::sync::CancellationToken;
    use url::Url;

    use tokio::time::Instant;

    use super::{
        AnnounceErrorKind, AnnounceOk, MIN_ANNOUNCE_INTERVAL, Reachable, ReannounceDelay, Refused,
        ScrapeOk, StoppedAnnounces, SupportedTracker, Tracker, TrackerAnnounce, TrackerComms,
        TrackerCommsStats, TrackerCommsStatsState, TrackerList, TrackerScrape, TrackerTask,
        UdpTrackerResolveResult, Unreachable, may_complete, read_answer, retry_delay,
        udp_tracker_to_socket_addrs,
    };
    use crate::Identity;
    use crate::tracker::tracker_comms_http::{MAX_INTERVAL, TrackerRequestEvent};
    use crate::tracker::tracker_comms_udp::{
        ACTION_CONNECT, CONNECTION_ID_MAGIC, UdpTrackerClient,
    };

    fn http(name: &str) -> SupportedTracker {
        let url = Url::parse(&format!("http://{name}/announce")).unwrap();
        SupportedTracker::new(Tracker::operator(url)).unwrap()
    }

    // Tiers of trackers the operator named: the test trackers listen on
    // loopback.
    fn op(tiers: Vec<Vec<Url>>) -> Vec<Vec<Tracker>> {
        tiers
            .into_iter()
            .map(|tier| tier.into_iter().map(Tracker::operator).collect())
            .collect()
    }

    /// A private tracker's URL: a user, a passkey in the path, one in the
    /// query.
    const PRIVATE: &str = "https://member:pw@tracker.example:8443/0123passkey/announce?pk=s3cret";

    /// Whether `text` names any part of [`PRIVATE`] past its host and port,
    /// or of `url`'s path and query.
    fn leaks(text: &str, url: &Url) -> bool {
        [
            "member",
            "pw@",
            "0123passkey",
            "s3cret",
            url.path(),
            url.query().unwrap_or("?"),
        ]
        .iter()
        .any(|secret| text.contains(secret))
    }

    #[test]
    fn a_tracker_shows_only_its_scheme_host_and_port() {
        let url = Url::parse(PRIVATE).unwrap();
        let shown = format!(
            "{:?}",
            SupportedTracker::new(Tracker::operator(url.clone())).unwrap()
        );
        assert_eq!(shown, "https://tracker.example:8443/…");
        assert!(!leaks(&shown, &url), "{shown}");
        let udp = Url::parse("udp://tracker.example:6969/0123passkey/announce").unwrap();
        assert_eq!(
            format!(
                "{:?}",
                SupportedTracker::new(Tracker::operator(udp)).unwrap()
            ),
            "udp://tracker.example:6969/…"
        );
        assert_eq!(
            super::redacted_tracker_url("http://[::1]/announce?pk=s3cret"),
            "http://[::1]:80/…"
        );
        assert_eq!(super::redacted_tracker_url("not a url"), "…");
    }

    /// Every log line written while it is the thread's subscriber.
    #[derive(Clone, Default)]
    struct Lines(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for Lines {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Lines {
        fn capture(&self) -> tracing::subscriber::DefaultGuard {
            let lines = self.clone();
            tracing::subscriber::set_default(
                tracing_subscriber::fmt()
                    .with_env_filter(tracing_subscriber::EnvFilter::new("weaver_torrent=trace"))
                    .with_ansi(false)
                    .with_writer(move || lines.clone())
                    .finish(),
            )
        }

        fn text(&self) -> String {
            String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
        }
    }

    /// The URL of a loopback tracker with a passkey in its path and query.
    async fn private_listener() -> (tokio::net::TcpListener, Url) {
        let (listener, url) = http_listener().await;
        let url = Url::parse(&format!("{url}/0123passkey?pk=s3cret")).unwrap();
        (listener, url)
    }

    #[tokio::test]
    async fn an_answered_announce_logs_no_passkey() {
        let lines = Lines::default();
        let _capture = lines.capture();
        let (listener, url) = private_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
        // The row is set after the answer is logged.
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some()) => { r.unwrap(); }
        }
        let text = lines.text();
        assert!(text.contains("tracker answered"), "{text}");
        assert!(text.contains(&format!("{:?}", http_of(&url))), "{text}");
        assert!(!leaks(&text, &url), "{text}");
        token.cancel();
    }

    #[tokio::test]
    async fn a_failed_announce_logs_and_keeps_no_passkey() {
        let lines = Lines::default();
        let _capture = lines.capture();
        let (listener, url) = private_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        // The tracker hangs up on the request: the client's error names
        // the URL it sent it to.
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = async {
                drop(read_announce(&listener).await);
                rx.wait_for(|a| a[0].last.is_some()).await.map(|_| ())
            } => r.unwrap(),
        }
        let error = status.announces()[0]
            .last
            .clone()
            .unwrap()
            .result
            .unwrap_err();
        assert!(!leaks(&error.message, &url), "{error}");
        let text = lines.text();
        assert!(text.contains("error calling tracker"), "{text}");
        assert!(!leaks(&text, &url), "{text}");
        token.cancel();
    }

    #[tokio::test]
    async fn a_client_error_is_mapped_without_its_url() {
        let (listener, url) = private_listener().await;
        let hang_up = async {
            drop(read_request(&listener).await);
        };
        let (sent, ()) = tokio::join!(
            crate::test_support::build(reqwest::Client::builder().no_proxy())
                .unwrap()
                .get(url.clone())
                .send(),
            hang_up
        );
        let e = anyhow::Error::from(sent.unwrap_err());
        assert!(
            leaks(&format!("{e:#}"), &url),
            "the client's error names the URL"
        );
        let mapped = format!("{:#}", super::without_url(e));
        assert!(!leaks(&mapped, &url), "{mapped}");
    }

    fn http_of(url: &Url) -> SupportedTracker {
        SupportedTracker::new(Tracker::operator(url.clone())).unwrap()
    }

    fn live(downloaded: u64, left: u64, completed_during_run: bool) -> TrackerCommsStats {
        TrackerCommsStats {
            uploaded_bytes: 0,
            downloaded_bytes: downloaded,
            corrupt_bytes: 0,
            redundant_bytes: 0,
            left_bytes: Some(left),
            finished: left == 0,
            private: false,
            torrent_state: TrackerCommsStatsState::Live,
            completed_during_run,
            encryption: crate::EncryptionPolicy::Disabled,
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
    fn one_tracker_of_each_tier_is_announced_to_at_a_time() {
        // libtorrent torrent::announce_with_tracker and
        // update_tracker_timer with announce_to_all_tiers and not
        // announce_to_all_trackers, as qBittorrent sets them.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"], &["c"]], t0);
        let stats = live(0, 90, false);

        // The first tracker of each tier is announced to, and nothing more
        // while they wait for their answers.
        assert_eq!(
            list.start_due(t0, None, &stats),
            [(http("a"), STARTED), (http("c"), STARTED)]
        );
        assert_eq!(list.start_due(t0, None, &stats), []);
        assert_eq!(list.next_due(), None);

        // a fails: it backs off to the end of its tier, b is tried at once.
        list.on_failure(&http("a"), t0, Duration::ZERO);
        assert_eq!(order(&list), [http("b"), http("a"), http("c")]);
        assert_eq!(list.start_due(t0, None, &stats), [(http("b"), STARTED)]);

        // b fails too: its tier has none left to try until the back-off is
        // up; c, answering, is not asked again.
        list.on_failure(&http("b"), t0, Duration::ZERO);
        assert_eq!(order(&list), [http("a"), http("b"), http("c")]);
        assert_eq!(list.start_due(t0, None, &stats), []);
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
        // first in its tier, so b is not announced to while it keeps
        // working; c, in its own tier, keeps its own interval.
        list.on_success(&http("a"), STARTED, t1, &ANSWERED);
        list.on_failure(&http("b"), t1, Duration::ZERO);
        assert_eq!(list.entries[1].next_announce, t1 + s(55));
        assert_eq!(list.next_due(), Some(t0 + s(1800)));
        assert_eq!(
            list.start_due(t0 + s(1800), None, &stats),
            [(http("c"), None)]
        );
        list.on_success(&http("c"), None, t0 + s(1800), &ANSWERED);
        assert_eq!(list.next_due(), Some(t1 + s(1800)));
        assert_eq!(
            list.start_due(t1 + s(1800), None, &stats),
            [(http("a"), None)]
        );
    }

    #[test]
    fn the_trackers_in_use_hear_completed_at_once_and_only_once() {
        // libtorrent tracker_list::completed, announce_infohash::can_announce
        // and torrent::send_tracker_request.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"], &["c"]], t0);
        assert_eq!(
            list.start_due(t0, None, &live(0, 90, false)),
            [(http("a"), STARTED), (http("c"), STARTED)]
        );
        let delay = ReannounceDelay {
            floor: s(900),
            regular: s(1800),
        };
        list.on_success(&http("a"), STARTED, t0, &delay);
        list.on_success(&http("c"), STARTED, t0, &delay);

        // The torrent becomes a seed: a and c are due at once, inside their
        // min interval, with `completed`.
        let seed = live(90, 0, true);
        let t1 = t0 + s(10);
        assert_eq!(list.start_due(t1, None, &seed), []);
        list.completed(t1);
        assert_eq!(
            list.start_due(t1, None, &seed),
            [(http("a"), COMPLETED), (http("c"), COMPLETED)]
        );

        // a refuses it and backs off, and b, the next in its tier, is asked
        // at once; c accepts it.
        list.on_failure(&http("a"), t1, Duration::ZERO);
        list.on_success(&http("c"), COMPLETED, t1, &delay);
        assert_eq!(list.start_due(t1, None, &seed), [(http("b"), STARTED)]);
        list.on_success(&http("b"), STARTED, t1, &delay);

        // Once a tracker accepted it, no other is sent `completed`: b,
        // working, is the one of its tier announced to.
        assert_eq!(list.event(&list.entries[0], &seed), None);
        assert_eq!(
            list.start_due(t1 + s(1800), None, &seed),
            [(http("b"), None), (http("c"), None)]
        );
    }

    #[test]
    fn a_torrent_with_its_selection_done_but_not_a_seed_announces_paused() {
        let t0 = Instant::now();
        let mut list = list(&[&["a"]], t0);
        let selection_done = TrackerCommsStats {
            finished: true,
            ..live(60, 30, false)
        };
        // `started` still goes first.
        assert_eq!(
            list.start_due(t0, None, &selection_done),
            [(http("a"), STARTED)]
        );
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);
        let t1 = t0 + ANSWERED.regular;
        assert_eq!(
            list.start_due(t1, None, &selection_done),
            [(http("a"), Some(TrackerRequestEvent::Paused))]
        );
        list.on_success(&http("a"), Some(TrackerRequestEvent::Paused), t1, &ANSWERED);
        // A seed announces without an event, and one still fetching too.
        let t2 = t1 + ANSWERED.regular;
        assert_eq!(
            list.start_due(t2, None, &live(0, 0, false)),
            [(http("a"), None)]
        );
        list.on_success(&http("a"), None, t2, &ANSWERED);
        let t3 = t2 + ANSWERED.regular;
        assert_eq!(
            list.start_due(t3, None, &live(0, 30, false)),
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
        assert_eq!(list.stop(), []);

        list.start_due(t0, None, &stats);
        // Not yet accepted: nothing is owed.
        assert_eq!(list.stop(), []);
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);
        assert_eq!(list.stop(), [http("a")]);
        list.on_success(&http("c"), STARTED, t0, &ANSWERED);
        assert_eq!(list.stop(), [http("a"), http("c")]);

        // a fails and b takes over: until b accepts `started`, a, failing,
        // is still owed `stopped`.
        list.on_failure(&http("a"), t0, Duration::ZERO);
        assert_eq!(list.start_due(t0, None, &stats), [(http("b"), STARTED)]);
        assert_eq!(list.stop(), [http("a"), http("c")]);
        // Then b, the tracker in use in its tier, is.
        list.on_success(&http("b"), STARTED, t0, &ANSWERED);
        assert_eq!(list.stop(), [http("b"), http("c")]);
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
        assert!(error.message.contains("sent nothing"), "{error}");
        assert_eq!(error.kind, AnnounceErrorKind::Failed);
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    /// Takes no tracker's vote.
    fn no_vote() -> super::ExternalIpVote {
        Arc::new(|_, _| {})
    }

    /// The session reaches every address.
    fn reach_all() -> Reachable {
        Arc::new(|_| Ok(()))
    }

    /// The session's rule for one with `blocked` on its blocklist, and no
    /// IPv6 when `ipv4_only`.
    fn reach_but(blocked: Vec<IpAddr>, ipv4_only: bool) -> Reachable {
        Arc::new(move |ip: IpAddr| {
            if ipv4_only && ip.is_ipv6() {
                Err(Unreachable::Family)
            } else if blocked.contains(&ip) {
                Err(Unreachable::Filtered)
            } else {
                Ok(())
            }
        })
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let mut peers = TrackerComms::start_with_tiers(
            hashes,
            identity(),
            op(vec![vec![url]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
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

    #[tokio::test]
    async fn a_task_taken_over_stops_the_trackers_left_out_and_announces_under_every_hash() {
        let (a, a_url) = http_listener().await;
        let (b, b_url) = http_listener().await;
        let v1 = InfoHashes {
            v1: Some(Id20::new([b'a'; 20])),
            v2: None,
        };
        let hybrid = InfoHashes {
            v2: Some(Id32::new([b'b'; 32])),
            ..v1
        };
        let stopped = StoppedAnnounces::default();
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            v1,
            identity(),
            op(vec![vec![a_url.clone()]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        );
        let (conn, started) = tokio::select! {
            _ = peers.next() => panic!("the tracker sent peers"),
            r = read_announce(&a) => r,
        };
        assert_eq!(query_param(&started, "event"), Some("started"));
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

        // Taken over by a run that announces to b alone, under both hashes.
        let TrackerTask { mut peers, status } = TrackerTask { peers, status }
            .take_over(hybrid, op(vec![vec![b_url.clone()]]))
            .expect("the task announced under one of the run's hashes");
        let left_out = async {
            let (conn, q) = read_announce(&a).await;
            respond(conn, "d8:intervali1800e5:peers0:e").await;
            q
        };
        let kept = async { (next_announce(&b).await, next_announce(&b).await) };
        let (stopped_a, (first, second)) = tokio::select! {
            _ = peers.next() => panic!("the tracker sent peers"),
            r = async { tokio::join!(left_out, kept) } => r,
        };
        // a, which took `started`, is sent `stopped`, under the same hash.
        assert_eq!(query_param(&stopped_a, "event"), Some("stopped"));
        assert_eq!(
            query_param(&stopped_a, "info_hash"),
            Some("a".repeat(20).as_str())
        );
        // b hears one torrent under both its hashes: one key, one peer id.
        let mut hashes = [&first, &second].map(|q| query_param(q, "info_hash").unwrap());
        hashes.sort();
        assert_eq!(hashes, ["a".repeat(20), "b".repeat(20)]);
        for q in [&stopped_a, &first, &second] {
            assert_eq!(query_param(q, "key"), query_param(&started, "key"));
            assert_eq!(query_param(q, "peer_id"), query_param(&started, "peer_id"));
        }
        for q in [&first, &second] {
            assert_eq!(query_param(q, "event"), Some("started"));
        }
        assert_eq!(
            status
                .announces()
                .iter()
                .map(|a| a.url.clone())
                .collect::<Vec<_>>(),
            [b_url.to_string()]
        );
        drop(peers);
        token.cancel();
    }

    #[tokio::test]
    async fn a_task_another_torrents_run_takes_over_is_dropped_with_stopped() {
        let (a, a_url) = http_listener().await;
        let mine = InfoHashes {
            v1: Some(Id20::new([b'a'; 20])),
            v2: None,
        };
        let other = InfoHashes {
            v1: Some(Id20::new([b'c'; 20])),
            v2: None,
        };
        let stopped = StoppedAnnounces::default();
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            mine,
            identity(),
            op(vec![vec![a_url.clone()]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        );
        let (conn, started) = tokio::select! {
            _ = peers.next() => panic!("the tracker sent peers"),
            r = read_announce(&a) => r,
        };
        assert_eq!(query_param(&started, "event"), Some("started"));
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

        // A run under hashes the task never announced under does not take
        // it: the task ends, and a hears it stop.
        let taken = TrackerTask { peers, status }.take_over(other, op(vec![vec![a_url.clone()]]));
        assert!(taken.is_none());
        let (conn, q) = read_announce(&a).await;
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        assert_eq!(query_param(&q, "info_hash"), Some("a".repeat(20).as_str()));
        assert_eq!(query_param(&q, "peer_id"), query_param(&started, "peer_id"));
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

    /// A bound on the stopped announces far past any exchange a test runs.
    const DAY: Duration = Duration::from_secs(86_400);

    /// [`ManualStats`] whose run ends once the test opens `ended`; `waiting`
    /// turns true once a stopped announce waits for it.
    struct EndingStats {
        stats: Arc<ManualStats>,
        ended: tokio::sync::watch::Sender<bool>,
        waiting: tokio::sync::watch::Sender<bool>,
    }

    impl super::TorrentStatsProvider for EndingStats {
        fn get(&self) -> TrackerCommsStats {
            super::TorrentStatsProvider::get(&self.stats)
        }

        fn run_ended(&self) -> BoxFuture<'static, ()> {
            let mut rx = self.ended.subscribe();
            let waiting = self.waiting.clone();
            async move {
                waiting.send_replace(true);
                let _ = rx.wait_for(|e| *e).await;
            }
            .boxed()
        }
    }

    // libtorrent `torrent::do_pause` disconnects every connection, each
    // counting the block it was cut in as redundant, before
    // `stop_announcing` builds `stopped`: the stopped announce reports what
    // the run's closing connections counted after its announcing stopped.
    #[tokio::test]
    async fn a_stopped_announce_reports_what_the_runs_closing_connections_counted() {
        let (listener, url) = http_listener().await;
        let stats = Arc::new(ManualStats {
            stats: Mutex::new(live(0, 90, false)),
            completed: tokio::sync::watch::channel(false).0,
        });
        let (ended, _) = tokio::sync::watch::channel(false);
        let (waiting, mut stopping) = tokio::sync::watch::channel(false);
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        // Bounds far past the exchange, so nothing here races them.
        let stopped = StoppedAnnounces::new(DAY).run_end_within(DAY);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(EndingStats {
                stats: stats.clone(),
                ended: ended.clone(),
                waiting,
            }),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
        .peers;
        let q = answer_with_a_peer(
            &listener,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));

        // The run ends: announcing stops first, then a closing connection
        // counts the 8192 bytes of the block it was cut in.
        drop(peers);
        stopping.wait_for(|w| *w).await.unwrap();
        *stats.stats.lock() = TrackerCommsStats {
            redundant_bytes: 8192,
            torrent_state: TrackerCommsStatsState::Paused,
            ..live(0, 90, false)
        };
        ended.send_replace(true);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        assert_eq!(query_param(&q, "redundant"), Some("8192"));

        stopped.wait_idle().await;
        token.cancel();
    }

    // The wait for a run's connections has its own bound apart from the
    // announce's: a run that never ends is waited for no longer than that,
    // and `stopped` still goes out, with the counters as they are, within
    // its own bound whole.
    #[tokio::test]
    async fn a_run_slow_to_end_still_leaves_the_stopped_announce_its_bound() {
        let (listener, url) = http_listener().await;
        let stats = Arc::new(ManualStats {
            stats: Mutex::new(live(0, 90, false)),
            completed: tokio::sync::watch::channel(false).0,
        });
        let (ended, _) = tokio::sync::watch::channel(false);
        let (waiting, _) = tokio::sync::watch::channel(false);
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::new(DAY).run_end_within(Duration::ZERO);
        assert_eq!(stopped.whole(), DAY);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(EndingStats {
                stats: stats.clone(),
                ended,
                waiting,
            }),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
        .peers;
        let q = answer_with_a_peer(
            &listener,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));

        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        assert_eq!(query_param(&q, "redundant"), Some("0"));

        stopped.wait_idle().await;
        token.cancel();
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
            tracker_key: 0x5eed,
            version: "Test 1.0".into(),
            user_agent: "Test/1.0".into(),
            tracker_ids: Default::default(),
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
    async fn respond(conn: tokio::net::TcpStream, body: impl AsRef<[u8]>) {
        let body = body.as_ref();
        let mut response = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        )
        .into_bytes();
        response.extend_from_slice(body);
        let mut response = response.as_slice();
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(()),
            None,
            listen_port,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            StoppedAnnounces::default(),
        )
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
    async fn an_http_announce_reports_the_bytes_that_failed_their_hash_and_the_bytes_we_did_not_need_since_started()
     {
        // The torrent had already counted some of each when the run began:
        // `started` reports none, and a later announce what grew since.
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(stats.clone()),
            None,
            listen_port,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            StoppedAnnounces::default(),
        )
        .peers;
        let first = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = next_announce(&listener) => q,
        };
        assert_eq!(query_param(&first, "event"), Some("started"));
        assert_eq!(query_param(&first, "corrupt"), Some("0"));
        assert_eq!(query_param(&first, "redundant"), Some("0"));
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
        assert_eq!(query_param(&second, "corrupt"), Some("16384"));
        assert_eq!(query_param(&second, "redundant"), Some("16"));
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![tracker]]),
            Box::new(stats.clone()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
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

    #[tokio::test]
    async fn a_zero_stopped_timeout_sends_no_stopped_announce() {
        // libtorrent torrent::announce_with_tracker: with
        // stop_tracker_timeout <= 0 the stopped event is not sent.
        let (listener, url) = http_listener().await;
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::new(Duration::ZERO);
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
        .peers;

        let q = answer_with_a_peer(
            &listener,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));

        // The stream's drop starts no `stopped` announce: the task it would
        // spawn holds the count, and none does.
        assert_eq!(stopped.running(), 1, "the tracker task is counted");
        drop(peers);
        assert_eq!(stopped.running(), 0, "a stopped announce was started");
        token.cancel();
    }

    // An announce names the torrent as its identity does, whatever the
    // client's own User-Agent.
    #[tokio::test]
    async fn test_http_announce_carries_the_torrents_peer_id_and_user_agent() {
        let (listener, url) = http_listener().await;
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            Arc::new(crate::test_support::Loopback::with(|| {
                reqwest::Client::builder()
                    .no_proxy()
                    .user_agent("Session/1.0")
            })),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(stats.clone()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
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

    /// An announce asks for 200 peers, compact; an answer naming many of
    /// both families hands every one on, in the order named, and the
    /// tracker's row names them all with the swarm it gave.
    #[tokio::test]
    async fn an_answer_hands_on_every_peer_it_names_of_both_families() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;

        let v4: Vec<SocketAddr> = (1..=120u16)
            .map(|i| {
                (
                    Ipv4Addr::new(10, 0, (i >> 8) as u8, u8::try_from(i & 0xff).unwrap()),
                    6000 + i,
                )
                    .into()
            })
            .collect();
        let v6: Vec<SocketAddr> = (1..=40u16)
            .map(|i| (Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, i), 7000 + i).into())
            .collect();
        let compact = |peers: &[SocketAddr]| -> Vec<u8> {
            peers
                .iter()
                .flat_map(|peer| {
                    let ip = match peer.ip() {
                        IpAddr::V4(ip) => ip.octets().to_vec(),
                        IpAddr::V6(ip) => ip.octets().to_vec(),
                    };
                    [ip, peer.port().to_be_bytes().to_vec()].concat()
                })
                .collect()
        };
        let (v4_bytes, v6_bytes) = (compact(&v4), compact(&v6));
        let mut body = b"d8:completei7e10:incompletei12e8:intervali1800e".to_vec();
        body.extend(format!("5:peers{}:", v4_bytes.len()).bytes());
        body.extend(&v4_bytes);
        body.extend(format!("6:peers6{}:", v6_bytes.len()).bytes());
        body.extend(&v6_bytes);
        body.push(b'e');

        let q = answer(&listener, &mut peers, body).await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        assert_eq!(query_param(&q, "compact"), Some("1"));
        assert_eq!(query_param(&q, "no_peer_id"), Some("0"));
        assert_eq!(query_param(&q, "numwant"), Some("200"));
        assert!(query_param(&q, "key").is_some(), "{q}");

        let expected: Vec<SocketAddr> = v4.iter().chain(&v6).copied().collect();
        let got: Vec<SocketAddr> = peers.by_ref().take(expected.len()).collect().await;
        assert_eq!(got, expected);

        let mut rx = status.subscribe();
        rx.wait_for(|announces| announces.iter().all(|a| !a.in_flight && a.last.is_some()))
            .await
            .unwrap();
        let announces = status.announces();
        assert_eq!(
            announce_of(&announces, &url).last.as_ref().unwrap().result,
            Ok(AnnounceOk {
                interval: Duration::from_secs(1800),
                min_interval: Duration::from_secs(30),
                seeders: Some(7),
                leechers: Some(12),
                downloaded: None,
                peers: expected,
                warning: None,
            })
        );

        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test]
    async fn test_stream_dropped_before_any_announce_sends_nothing() {
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let stopped = StoppedAnnounces::default();
        let peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![
                Url::parse("http://tracker.invalid/announce").unwrap(),
            ]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            Arc::new(crate::test_support::Loopback::with(
                reqwest::Client::builder,
            )),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let task = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(tiers),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        );
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
            ..Default::default()
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

        // Each tier's tracker is asked at once; the first refuses.
        let (err_conn, _) = driving!(read_announce(&err));
        let (ok_conn, _) = driving!(read_announce(&ok));
        let announces = status.announces();
        assert!(announce_of(&announces, &err_url).in_flight);
        let a = announce_of(&announces, &ok_url);
        assert!(a.in_flight);
        assert_eq!(a.last, None);
        respond(err_conn, "d14:failure reason4:nopee").await;
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
                downloaded: None,
                peers: Vec::new(),
                warning: None,
            })
        );
        let error = announce_of(&announces, &err_url)
            .last
            .as_ref()
            .unwrap()
            .result
            .clone()
            .unwrap_err();
        assert!(error.message.contains("nope"), "{error}");
        assert_eq!(error.kind, AnnounceErrorKind::Refused);

        // The table outlives the task: the `stopped` answer is the last
        // outcome of the tracker in use, the refusal stays the other's, and
        // with the run over no tracker is due again.
        drop(peers);
        let q = next_announce(&ok).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        let after = status.announces();
        assert!(
            after
                .iter()
                .all(|a| !a.in_flight && a.next_announce.is_none()),
            "{after:?}"
        );
        assert_eq!(
            announce_of(&after, &err_url).last,
            announce_of(&announces, &err_url).last
        );
        assert_eq!(announce_of(&after, &err_url).fails, 1);
        let stopped_answer = announce_of(&after, &ok_url).last.as_ref().unwrap();
        assert_eq!(
            stopped_answer.result.as_ref().map(|a| a.seeders),
            Ok(None),
            "the row holds the stopped answer, which gave no counts"
        );
        token.cancel();
    }

    /// Answers the next HTTP request with `body`, driving `peers` meanwhile;
    /// the request's query string.
    async fn answer(
        listener: &tokio::net::TcpListener,
        peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
        body: impl AsRef<[u8]>,
    ) -> String {
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            q = async {
                let (conn, q) = read_announce(listener).await;
                respond(conn, body).await;
                q
            } => q,
        }
    }

    #[tokio::test]
    async fn a_stopped_announce_is_the_rows_last_and_leaves_none_due_until_the_next_run() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();

        let q = answer(
            &listener,
            &mut peers,
            "d8:completei3e8:intervali1800e5:peers0:e",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
        }
        let started = status.announces()[0].clone();
        assert!(started.next_announce.is_some());
        let started_at = started.last.as_ref().unwrap().at;

        // Paused: `stopped` goes out and its answer is recorded as any is.
        drop(peers);
        let (conn, q) = read_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        // While it waits for its answer, the row says so, and no announce
        // is due any more.
        let waiting = status.announces()[0].clone();
        assert!(waiting.in_flight);
        assert_eq!(waiting.next_announce, None);
        assert_eq!(waiting.last, started.last);
        respond(conn, "d8:completei7e8:intervali1800e5:peers0:e").await;
        stopped.wait_idle().await;
        let row = status.announces()[0].clone();
        assert!(!row.in_flight);
        assert_eq!(row.next_announce, None);
        assert_eq!(row.fails, 0);
        let last = row.last.unwrap();
        assert!(last.at >= started_at);
        assert_eq!(last.result.map(|a| a.seeders), Ok(Some(7)));

        // Resumed: a new run, whose table records `started` again.
        let (task, token2) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        let q = answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
        }
        let row = status.announces()[0].clone();
        assert!(row.last.unwrap().result.is_ok());
        assert!(row.next_announce.is_some());
        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
        token2.cancel();
    }

    #[tokio::test]
    async fn a_refused_stopped_announce_counts_a_failure_and_leaves_none_due() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
        }

        drop(peers);
        let (conn, q) = read_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        respond(conn, "d14:failure reason4:nopee").await;
        stopped.wait_idle().await;
        let row = status.announces()[0].clone();
        assert_eq!(row.fails, 1);
        assert_eq!(row.next_announce, None);
        let error = row.last.unwrap().result.unwrap_err();
        assert_eq!(error.kind, AnnounceErrorKind::Refused);
        assert!(error.message.contains("nope"), "{error}");
        token.cancel();
    }

    #[tokio::test]
    async fn a_zero_stopped_timeout_still_leaves_no_announce_due() {
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::new(Duration::ZERO);
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        let mut rx = status.subscribe();
        answer(&listener, &mut peers, "d8:intervali1800e5:peers0:e").await;
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            r = rx.wait_for(|a| a[0].last.is_some() && !a[0].in_flight) => { r.unwrap(); }
        }
        let answered = status.announces()[0].clone();
        assert!(answered.next_announce.is_some());

        // No `stopped` goes out, and the run is over all the same.
        drop(peers);
        let row = status.announces()[0].clone();
        assert_eq!(row.next_announce, None);
        assert_eq!(row.last, answered.last);
        token.cancel();
    }

    #[tokio::test]
    async fn an_accepted_answers_external_ip_is_the_trackers_vote() {
        let (refusing, refusing_url) = http_listener().await;
        let (listener, url) = http_listener().await;
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, None);
        let (votes, mut voted) = tokio::sync::mpsc::unbounded_channel();
        let stopped = StoppedAnnounces::default();
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![refusing_url], vec![url]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            Arc::new(move |ours, by| votes.send((ours, by)).unwrap()),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        )
        .peers;

        // The first tier's tracker refuses, naming an address (65.65.65.65):
        // a refusal casts no vote.
        let q = answer(
            &refusing,
            &mut peers,
            "d11:external ip4:AAAA14:failure reason4:nopee",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        // The next tier's accepts, naming 88.89.90.91: its vote, at the
        // address its answer came from.
        let q = answer(
            &listener,
            &mut peers,
            "d11:external ip4:XYZ[8:intervali1800e5:peers0:e",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        let vote = tokio::select! {
            _ = peers.next() => panic!("the tracker sent no peers"),
            v = voted.recv() => v.unwrap(),
        };
        assert_eq!(
            vote,
            (
                IpAddr::from(Ipv4Addr::new(88, 89, 90, 91)),
                IpAddr::from(Ipv4Addr::LOCALHOST)
            )
        );
        drop(peers);
        let q = next_announce(&listener).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        assert!(voted.try_recv().is_err(), "an answer without one voted");
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
                ..Default::default()
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

    /// Resolves every name to `at`'s address.
    struct FixedResolver {
        at: std::net::IpAddr,
    }

    impl Resolver for FixedResolver {
        fn resolve<'a>(
            &'a self,
            _host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async move {
                Ok(crate::core::net::Resolved::Addresses(vec![
                    (self.at, port).into(),
                ]))
            })
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
            .message
    }

    // An e2e build asks a loopback tracker at any path.
    #[cfg(not(feature = "e2e-local-trackers"))]
    #[tokio::test]
    async fn a_tracker_url_that_could_forge_a_request_is_never_asked() {
        // A URL whose query names an announce argument is refused, and so is
        // a loopback tracker the operator named at a path with no segment
        // starting `announce`, whatever name it is reached by. Each is refused before
        // any request; the next tier's tracker is asked.
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
        let udp_sock = crate::test_support::loopback_udp().await;
        let resolver = Arc::new(FixedResolver {
            at: Ipv4Addr::LOCALHOST.into(),
        });
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(tiers),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            crate::test_support::loopback(),
            reach_all(),
            udp_client,
            stopped.clone(),
        );

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

    // An e2e build asks a loopback tracker at any path.
    #[cfg(not(feature = "e2e-local-trackers"))]
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
        fn client(
            &self,
            target: Option<HttpTarget<'_>>,
            shape: &ClientShape,
        ) -> anyhow::Result<reqwest::Client> {
            if let Some(t) = &target {
                self.built
                    .lock()
                    .push((t.host.to_owned(), t.port, t.addrs.to_vec()));
            }
            let builder = reqwest::Client::builder()
                .no_proxy()
                .dns_resolver(Arc::new(NoNames));
            crate::test_support::client(builder, target, shape)
        }
    }

    #[tokio::test]
    async fn every_hop_of_a_tracker_get_goes_only_to_the_addresses_checked_for_it() {
        // Each hop's addresses are checked before its client is built, and
        // the client tries only those left; a redirect starts over at its
        // own URL. The names are known to the engine's resolver only: a
        // client that looked one up itself would reach nothing.
        let (tracker, at) = http_listener().await;
        let (moved, moved_at) = http_listener().await;
        let (port, moved_port) = (at.port().unwrap(), moved_at.port().unwrap());
        let url = Url::parse(&format!("http://tracker.invalid:{port}/announce")).unwrap();
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let resolver = Arc::new(FixedResolver {
            at: Ipv4Addr::LOCALHOST.into(),
        });
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
        let clients = Arc::new(Pinned::default());
        let mut peers = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            clients.clone(),
            reach_all(),
            udp_client,
            StoppedAnnounces::default(),
        )
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

    /// The session's clients on a network where every address is the
    /// tracker at `at`: a client built for any address reaches it there.
    /// Records the addresses clients were built for. An address in `routes`
    /// is reached as it says; any other through a proxy.
    struct Wired {
        at: SocketAddr,
        built: Mutex<Vec<IpAddr>>,
        routes: Vec<(IpAddr, Egress)>,
    }

    impl Wired {
        fn new(at: &Url) -> Arc<Self> {
            Self::routing(at, Vec::new())
        }

        fn routing(at: &Url, routes: Vec<(IpAddr, Egress)>) -> Arc<Self> {
            Arc::new(Self {
                at: SocketAddr::new(at.host_str().unwrap().parse().unwrap(), at.port().unwrap()),
                built: Mutex::default(),
                routes,
            })
        }
    }

    impl HttpClients for Wired {
        fn client(
            &self,
            target: Option<HttpTarget<'_>>,
            shape: &ClientShape,
        ) -> anyhow::Result<reqwest::Client> {
            let at = [self.at.ip()];
            let target = target.map(|t| {
                self.built.lock().extend(t.addrs);
                HttpTarget { addrs: &at, ..t }
            });
            let builder = reqwest::Client::builder()
                .no_proxy()
                .dns_resolver(Arc::new(NoNames));
            crate::test_support::client(builder, target, shape)
        }

        fn egress(&self, to: SocketAddr) -> Egress {
            self.routes
                .iter()
                .find(|(ip, _)| *ip == to.ip())
                .map_or(Egress::Proxied, |(_, egress)| *egress)
        }
    }

    /// A resolver that knows `names` and no other.
    struct Names(Vec<(&'static str, IpAddr)>);

    impl Resolver for Names {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            let found: Vec<SocketAddr> = self
                .0
                .iter()
                .filter(|(name, _)| *name == host)
                .map(|(_, ip)| SocketAddr::new(*ip, port))
                .collect();
            Box::pin(async move {
                if found.is_empty() {
                    Err(std::io::ErrorKind::NotFound.into())
                } else {
                    Ok(crate::core::net::Resolved::Addresses(found))
                }
            })
        }
    }

    /// Starts announcing to `tiers` through `clients`, names looked up by
    /// `names`.
    fn start_through(
        tiers: Vec<Vec<Tracker>>,
        clients: Arc<Wired>,
        names: Names,
        token: &CancellationToken,
        stopped: &StoppedAnnounces,
    ) -> TrackerTask {
        let udp_sock = crate::test_support::loopback_udp_now();
        let udp_client =
            UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(Arc::new(names)));
        TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            tiers,
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            clients,
            reach_all(),
            udp_client,
            stopped.clone(),
        )
    }

    /// A tracker named by both families, and the session's own address in
    /// each.
    fn dual_stack() -> (Names, [IpAddr; 2], [IpAddr; 2]) {
        let tracker: [IpAddr; 2] = [
            Ipv4Addr::new(203, 0, 113, 7).into(),
            "2001:db8::7".parse().unwrap(),
        ];
        let own: [IpAddr; 2] = [
            Ipv4Addr::new(198, 51, 100, 1).into(),
            "2001:db8::1".parse().unwrap(),
        ];
        let names = Names(vec![
            ("dual.invalid", tracker[0]),
            ("dual.invalid", tracker[1]),
        ]);
        (names, tracker, own)
    }

    #[tokio::test]
    async fn a_tracker_both_families_route_to_is_announced_to_once_in_each() {
        // libtorrent announces from each listen socket that can route to
        // the tracker (torrent.cpp `on_filter`): with an IPv4 and an IPv6
        // route, two announces, each held to its family, and the peers of
        // both answers kept.
        let (tracker, at) = http_listener().await;
        let url = Url::parse(&format!(
            "http://dual.invalid:{}/announce",
            at.port().unwrap()
        ))
        .unwrap();
        let (names, addrs, own) = dual_stack();
        let clients = Wired::routing(
            &at,
            vec![
                (addrs[0], Egress::From(own[0])),
                (addrs[1], Egress::From(own[1])),
            ],
        );
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_through(
            vec![vec![Tracker::operator(url)]],
            clients.clone(),
            names,
            &token,
            &stopped,
        );

        // The peers of neither answer come before both are in.
        let mut queries = Vec::new();
        for body in [
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aBe",
        ] {
            let (conn, query) = tokio::select! {
                _ = peers.next() => panic!("a peer came before both announces were answered"),
                r = read_announce(&tracker) => r,
            };
            respond(conn, body).await;
            queries.push(query);
        }
        let [q1, q2] = <[String; 2]>::try_from(queries).unwrap();
        assert_eq!(query_param(&q1, "event"), Some("started"));
        assert_eq!(q1, q2);
        let other_peer: SocketAddr = (Ipv4Addr::LOCALHOST, 0x1a42).into();
        let mut got = vec![peers.next().await.unwrap(), peers.next().await.unwrap()];
        got.sort();
        assert_eq!(got, [the_peer(), other_peer]);
        let mut built = clients.built.lock().clone();
        built.sort();
        assert_eq!(built, addrs);
        drop(peers);
        token.cancel();
    }

    #[tokio::test]
    async fn a_tracker_reached_through_a_proxy_is_announced_to_once() {
        // Through a proxy libtorrent has one listen socket and announces
        // once; the proxy picks the family.
        let (tracker, at) = http_listener().await;
        let url = Url::parse(&format!(
            "http://dual.invalid:{}/announce",
            at.port().unwrap()
        ))
        .unwrap();
        let (names, addrs, _) = dual_stack();
        let clients = Wired::new(&at);
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_through(
            vec![vec![Tracker::operator(url)]],
            clients.clone(),
            names,
            &token,
            &stopped,
        );

        answer_with_a_peer(
            &tracker,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(*clients.built.lock(), addrs);
        drop(peers);
        token.cancel();
    }

    #[tokio::test]
    async fn a_family_with_no_route_to_the_tracker_is_not_announced_in() {
        // libtorrent skips a listen socket that cannot route to the
        // tracker: with no IPv6 route, one announce, held to IPv4.
        let (tracker, at) = http_listener().await;
        let url = Url::parse(&format!(
            "http://dual.invalid:{}/announce",
            at.port().unwrap()
        ))
        .unwrap();
        let (names, addrs, own) = dual_stack();
        let clients = Wired::routing(
            &at,
            vec![
                (addrs[0], Egress::From(own[0])),
                (addrs[1], Egress::NoRoute),
            ],
        );
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_through(
            vec![vec![Tracker::operator(url)]],
            clients.clone(),
            names,
            &token,
            &stopped,
        );

        answer_with_a_peer(
            &tracker,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(*clients.built.lock(), [addrs[0]]);
        drop(peers);
        token.cancel();
    }

    // The e2e carveout asks a torrent's tracker at these local addresses.
    #[cfg(not(feature = "e2e-local-trackers"))]
    #[tokio::test]
    async fn a_tracker_the_torrent_named_is_refused_at_a_private_address() {
        // A tracker that came with the torrent, at a loopback, private,
        // link-local or unique-local address, by IP or by name, at a
        // tracker's path or any other, over HTTP or UDP, is refused before
        // any client is built for it or any datagram sent; the operator's
        // own tracker on loopback, in the next tier, is asked.
        let (tracker, at) = http_listener().await;
        // Another tracker on loopback at /announce: one URL is one tracker,
        // so the torrent's may not share the operator's.
        let (_theirs, their_at) = http_listener().await;
        let port = at.port().unwrap();
        let named = |s: &str| Url::parse(&s.replace("PORT", &port.to_string())).unwrap();
        let refused = [
            their_at.clone(),
            named("http://ten.invalid:PORT/announce"),
            named("http://lan.invalid:PORT/admin"),
            named("http://link.invalid:PORT/announce"),
            named("http://ula.invalid:PORT/scrape"),
            named("udp://ten.invalid:PORT/announce"),
            named("udp://192.168.1.1:PORT/announce"),
        ];
        let names = Names(vec![
            ("ten.invalid", Ipv4Addr::new(10, 0, 0, 1).into()),
            ("lan.invalid", Ipv4Addr::new(192, 168, 1, 1).into()),
            ("link.invalid", Ipv4Addr::new(169, 254, 1, 1).into()),
            ("ula.invalid", "fd00::1".parse().unwrap()),
        ]);
        let mut tiers: Vec<Vec<Tracker>> = refused
            .iter()
            .map(|u| vec![Tracker::torrent(u.clone())])
            .collect();
        tiers.push(vec![Tracker::operator(at.clone())]);
        let clients = Wired::new(&at);
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } =
            start_through(tiers, clients.clone(), names, &token, &stopped);

        for url in &refused {
            let e = refusal_of(&status, &mut peers, url).await;
            assert!(e.contains("not public"), "{url}: {e}");
        }
        let q = answer_with_a_peer(
            &tracker,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        assert_eq!(*clients.built.lock(), [IpAddr::from(Ipv4Addr::LOCALHOST)]);

        drop(peers);
        let q = next_announce(&tracker).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
    }

    #[cfg(feature = "e2e-local-trackers")]
    #[tokio::test]
    async fn an_e2e_build_asks_a_tracker_the_torrent_named_at_a_local_address() {
        // In an e2e build a tracker that came with the torrent is asked at
        // any address, link-local too, at any path.
        let (tracker, at) = http_listener().await;
        let port = at.port().unwrap();
        let asked = Url::parse(&format!("http://link.invalid:{port}/0123abcd/announce")).unwrap();
        let names = Names(vec![("link.invalid", Ipv4Addr::new(169, 254, 1, 1).into())]);
        let tiers = vec![vec![Tracker::torrent(asked)]];
        let clients = Wired::new(&at);
        let token = CancellationToken::new();
        let stopped = StoppedAnnounces::default();
        let TrackerTask {
            mut peers,
            status: _status,
        } = start_through(tiers, clients.clone(), names, &token, &stopped);

        let q = answer_with_a_peer(
            &tracker,
            &mut peers,
            "d8:intervali1800e5:peers6:\x7f\0\0\x01\x1aAe",
        )
        .await;
        assert_eq!(query_param(&q, "event"), Some("started"));
        assert_eq!(
            *clients.built.lock(),
            [IpAddr::from(Ipv4Addr::new(169, 254, 1, 1))]
        );

        drop(peers);
        let q = next_announce(&tracker).await;
        assert_eq!(query_param(&q, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
    }

    // The e2e carveout asks a torrent's tracker at these local addresses.
    #[cfg(not(feature = "e2e-local-trackers"))]
    #[tokio::test]
    async fn a_udp_tracker_is_resolved_to_the_addresses_its_naming_allows() {
        // A UDP tracker that came with the torrent has no address to be
        // asked at among private ones; the operator's is asked there. A
        // `udp://` URL hands an address over as written, and it is not
        // looked up.
        let names = Names(vec![("ten.invalid", Ipv4Addr::new(10, 0, 0, 1).into())]);
        for (host, at) in [
            (url::Host::Domain("ten.invalid"), Ipv4Addr::new(10, 0, 0, 1)),
            (
                url::Host::Ipv4(Ipv4Addr::new(192, 168, 1, 1)),
                Ipv4Addr::new(192, 168, 1, 1),
            ),
            (
                url::Host::Domain("192.168.1.1"),
                Ipv4Addr::new(192, 168, 1, 1),
            ),
        ] {
            let refused = udp_tracker_to_socket_addrs(
                host.clone(),
                6969,
                &names,
                &reach_all(),
                NamedBy::Torrent,
            )
            .await
            .unwrap_err();
            assert!(
                refused.downcast_ref::<super::NotPublic>().is_some(),
                "{refused:#}"
            );
            let asked =
                udp_tracker_to_socket_addrs(host, 6969, &names, &reach_all(), NamedBy::Operator)
                    .await
                    .unwrap();
            assert!(
                matches!(asked, UdpTrackerResolveResult::One(a) if a == (at, 6969).into()),
                "{asked:?}"
            );
        }
    }

    // The e2e carveout asks a torrent's tracker at these local addresses.
    #[cfg(not(feature = "e2e-local-trackers"))]
    #[tokio::test]
    async fn a_public_tracker_may_not_move_an_announce_to_a_private_address() {
        // A tracker reached at public addresses only that redirects to a
        // private one is refused at the new hop, whoever named it: what it
        // moves is held to public addresses. No client is built for the
        // private hop.
        for named_by in [NamedBy::Torrent, NamedBy::Operator] {
            let (tracker, at) = http_listener().await;
            let port = at.port().unwrap();
            let url = Url::parse(&format!("http://public.invalid:{port}/announce")).unwrap();
            let names = Names(vec![
                ("public.invalid", Ipv4Addr::new(8, 8, 8, 8).into()),
                ("private.invalid", Ipv4Addr::new(10, 0, 0, 1).into()),
            ]);
            let clients = Wired::new(&at);
            let token = CancellationToken::new();
            let stopped = StoppedAnnounces::default();
            let TrackerTask { mut peers, status } = start_through(
                vec![vec![Tracker {
                    url: url.clone(),
                    named_by,
                }]],
                clients.clone(),
                names,
                &token,
                &stopped,
            );
            let serve = async {
                let (conn, q) = read_announce(&tracker).await;
                redirect(conn, &format!("http://private.invalid:{port}/announce?{q}")).await;
                std::future::pending::<()>().await
            };
            let e = tokio::select! {
                _ = serve => unreachable!(),
                e = refusal_of(&status, &mut peers, &url) => e,
            };
            assert!(e.contains("not public"), "{named_by:?}: {e}");
            assert_eq!(
                *clients.built.lock(),
                [IpAddr::from(Ipv4Addr::new(8, 8, 8, 8))],
                "{named_by:?}"
            );
            drop(peers);
            stopped.wait_idle().await;
            token.cancel();
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
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async move {
                assert_eq!(host, "tracker.invalid");
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(crate::core::net::Resolved::Addresses(vec![
                    (Ipv4Addr::LOCALHOST, port).into(),
                ]))
            })
        }
    }

    #[tokio::test]
    async fn test_udp_tracker_hostname_through_custom_resolver() {
        const CONNECTION_ID: u64 = 0x1122_3344_5566_7788;

        let tracker = crate::test_support::loopback_udp().await;
        let tracker_addr = tracker.bind_addr();

        let client_sock = crate::test_support::loopback_udp().await;
        let client_addr = client_sock.bind_addr();

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
            &reach_all(),
            NamedBy::Operator,
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
        let tracker = crate::test_support::loopback_udp().await;
        let url = Url::parse(&format!("udp://{}/announce", tracker.bind_addr())).unwrap();
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
                downloaded: None,
                peers: Vec::new(),
                warning: None,
            })
        );

        drop(peers);
        stopped.wait_idle().await;
        serve.abort();
        token.cancel();
    }

    /// Resolves each name to its own addresses, in the order given, and an
    /// address written out (a UDP URL's host is a name to `url`) to itself,
    /// as the host's resolver does.
    struct Named(Vec<(&'static str, Vec<IpAddr>)>);

    impl Resolver for Named {
        fn resolve<'a>(
            &'a self,
            host: &'a str,
            port: u16,
        ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
            Box::pin(async move {
                if let Ok(ip) = host.parse::<IpAddr>() {
                    return Ok(crate::core::net::Resolved::Addresses(vec![
                        (ip, port).into(),
                    ]));
                }
                let (_, addrs) =
                    self.0
                        .iter()
                        .find(|(name, _)| *name == host)
                        .ok_or_else(|| {
                            std::io::Error::new(std::io::ErrorKind::NotFound, host.to_owned())
                        })?;
                Ok(crate::core::net::Resolved::Addresses(
                    addrs.iter().map(|ip| (*ip, port).into()).collect(),
                ))
            })
        }
    }

    // A datagram goes to an address: a UDP tracker whose name the network
    // resolves at its exit is not asked.
    #[tokio::test]
    async fn a_udp_tracker_resolved_at_the_exit_is_not_asked() {
        struct AtExit(AtomicUsize);
        impl Resolver for AtExit {
            fn resolve<'a>(
                &'a self,
                host: &'a str,
                _: u16,
            ) -> crate::core::net::BoxFuture<'a, std::io::Result<crate::core::net::Resolved>>
            {
                assert_eq!(host, "tracker.invalid");
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(crate::core::net::Resolved::AtExit) })
            }
        }
        let resolver = AtExit(AtomicUsize::new(0));
        let found = udp_tracker_to_socket_addrs(
            url::Host::Domain("tracker.invalid"),
            6969,
            &resolver,
            &reach_all(),
            NamedBy::Operator,
        )
        .await;
        assert!(found.is_err(), "{found:?}");
        assert_eq!(resolver.0.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_tracker_with_no_address_the_session_may_reach_is_never_asked() {
        // libtorrent http_tracker_connection::on_filter and
        // udp_tracker_connection::name_lookup drop every endpoint the listen
        // socket cannot route (an IPv4-only session's IPv6 ones), then every
        // one the ip_filter blocks; with none left the announce fails,
        // announce_skipped or banned_by_ip_filter, before anything is sent.
        let (http, http_at) = http_listener().await;
        let udp = crate::test_support::loopback_kernel_udp();
        let (hp, up) = (
            http_at.port().unwrap(),
            crate::network::DatagramSocket::bind_addr(&*udp).port(),
        );
        let url = |s: &str| {
            Url::parse(
                &s.replace("HP", &hp.to_string())
                    .replace("UP", &up.to_string()),
            )
            .unwrap()
        };
        let banned = "banned by the IP filter";
        let family = "of a family the session does not connect over";
        let refused = [
            (url("http://127.0.0.1:HP/announce"), banned),
            (url("udp://127.0.0.1:UP/announce"), banned),
            (url("http://tracker.invalid:HP/announce"), banned),
            (url("udp://tracker.invalid:UP/announce"), banned),
            (url("http://[::1]:HP/announce"), family),
            (url("udp://[::1]:UP/announce"), family),
        ];
        let loopback = IpAddr::from(Ipv4Addr::LOCALHOST);
        let resolver = Arc::new(Named(vec![(
            "tracker.invalid",
            vec![Ipv6Addr::LOCALHOST.into(), loopback],
        )]));
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
        let clients = Arc::new(Pinned::default());
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(refused.iter().map(|(u, _)| vec![u.clone()]).collect()),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            clients.clone(),
            reach_but(vec![loopback], true),
            udp_client,
            stopped.clone(),
        );

        for (url, why) in &refused {
            let e = refusal_of(&status, &mut peers, url).await;
            assert!(e.contains(why), "{url}: {e}");
        }
        // No client was built, and neither tracker heard anything.
        assert!(clients.built.lock().is_empty());
        assert!(http.accept().now_or_never().is_none());
        let mut buf = [0u8; 64];
        assert_eq!(
            udp.try_recv_from(&mut buf).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test]
    async fn every_hop_of_a_tracker_get_goes_only_to_addresses_the_session_may_reach() {
        // libtorrent http_tracker_connection::on_filter, on the first
        // resolve and on each redirect's: the endpoints of a family the
        // listen socket cannot route and those the ip_filter blocks are
        // dropped, connect tries those left, and a hop with none left fails
        // the announce banned_by_ip_filter before it is sent.
        let (tracker, at) = http_listener().await;
        let port = at.port().unwrap();
        let url = Url::parse(&format!("http://tracker.invalid:{port}/announce")).unwrap();
        let blocked = IpAddr::from(Ipv4Addr::new(127, 0, 0, 3));
        let loopback = IpAddr::from(Ipv4Addr::LOCALHOST);
        let resolver = Arc::new(Named(vec![
            (
                "tracker.invalid",
                vec![Ipv6Addr::LOCALHOST.into(), blocked, loopback],
            ),
            ("moved.invalid", vec![blocked]),
        ]));
        let token = CancellationToken::new();
        let udp_sock = crate::test_support::loopback_udp().await;
        let udp_client = UdpTrackerClient::new_with_socket(token.clone(), udp_sock, Some(resolver));
        let clients = Arc::new(Pinned::default());
        let stopped = StoppedAnnounces::default();
        let TrackerTask { mut peers, status } = TrackerComms::start_with_tiers(
            v1_hash(),
            identity(),
            op(vec![vec![url.clone()]]),
            Box::new(()),
            None,
            tokio::sync::watch::channel(Some(6881)).1,
            no_vote(),
            clients.clone(),
            reach_but(vec![blocked], true),
            udp_client,
            stopped.clone(),
        );

        let serve = async {
            let (conn, q) = read_announce(&tracker).await;
            redirect(conn, &format!("http://moved.invalid:{port}/announce?{q}")).await;
            std::future::pending::<()>().await
        };
        let e = tokio::select! {
            _ = serve => unreachable!(),
            e = refusal_of(&status, &mut peers, &url) => e,
        };
        assert!(e.contains("banned by the IP filter"), "{e}");
        // The tracker was reached at the one address left; the moved host
        // never had a client built for it.
        assert_eq!(
            *clients.built.lock(),
            vec![("tracker.invalid".to_owned(), port, vec![loopback])]
        );
        drop(peers);
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test(start_paused = true)]
    async fn a_udp_tracker_is_asked_only_at_addresses_the_session_may_reach() {
        // libtorrent udp_tracker_connection::name_lookup keeps the endpoints
        // the listen socket can route and the ip_filter lets through, in the
        // order found, and asks from those.
        let blocked = IpAddr::from(Ipv4Addr::new(127, 0, 0, 3));
        let resolver = Named(vec![(
            "tracker.invalid",
            vec![
                Ipv6Addr::LOCALHOST.into(),
                blocked,
                Ipv4Addr::LOCALHOST.into(),
                Ipv4Addr::new(127, 0, 0, 4).into(),
            ],
        )]);
        let host = url::Host::Domain("tracker.invalid");
        let v4 = (Ipv4Addr::LOCALHOST, 6969).into();
        let v6 = (Ipv6Addr::LOCALHOST, 6969).into();

        let ipv4_only = reach_but(vec![blocked], true);
        let found = udp_tracker_to_socket_addrs(
            host.clone(),
            6969,
            &resolver,
            &ipv4_only,
            NamedBy::Operator,
        )
        .await
        .unwrap();
        assert!(
            matches!(found, UdpTrackerResolveResult::One(a) if a == v4),
            "{found:?}"
        );

        let both = reach_but(vec![blocked], false);
        let found = udp_tracker_to_socket_addrs(host, 6969, &resolver, &both, NamedBy::Operator)
            .await
            .unwrap();
        assert!(
            matches!(found, UdpTrackerResolveResult::Two(a, b) if SocketAddr::from(a) == v4 && SocketAddr::from(b) == v6),
            "{found:?}"
        );
    }

    #[test]
    fn replaced_trackers_keep_what_announcing_to_them_did() {
        // libtorrent tracker_list::replace orders by tier and drops a
        // repeated URL; a kept tracker keeps its state here.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(list.start_due(t0, None, &stats), [(http("a"), STARTED)]);
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);

        list.replace(
            &[vec![http("c"), http("a")], vec![http("a"), http("d")]],
            t0 + s(10),
        );
        assert_eq!(order(&list), [http("c"), http("a"), http("d")]);
        assert_eq!(
            list.entries.iter().map(|e| e.tier).collect::<Vec<_>>(),
            [0, 0, 1]
        );
        // a is not sent `started` again nor brought forward; c and d, new,
        // are due, each the first of its tier.
        let a = &list.entries[1];
        assert!(a.start_sent);
        assert_eq!(a.next_announce, t0 + ANSWERED.regular);
        assert_eq!(
            list.start_due(t0 + s(10), None, &stats),
            [(http("c"), STARTED), (http("d"), STARTED)]
        );
    }

    #[test]
    fn replaced_trackers_gone_that_accepted_started_are_returned() {
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"], &["c"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(
            list.start_due(t0, None, &stats),
            [(http("a"), STARTED), (http("c"), STARTED)]
        );
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);

        // a heard `started` and is owed `stopped`; b never heard from us.
        let gone = list.replace(&[vec![http("c")]], t0);
        assert_eq!(gone, [http("a")]);
        assert_eq!(order(&list), [http("c")]);
    }

    #[test]
    fn a_forced_reannounce_is_due_now_and_never_doubles_one_in_flight() {
        // libtorrent trigger_announce and torrent::announce_tracker_now.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a", "b"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(list.start_due(t0, None, &stats), [(http("a"), STARTED)]);
        let delay = ReannounceDelay {
            floor: s(900),
            regular: s(1800),
        };
        list.on_success(&http("a"), STARTED, t0, &delay);

        // Within a's min interval, a force that respects it waits for it.
        let t1 = t0 + s(60);
        assert!(list.force(t1, None, false));
        assert_eq!(list.start_due(t1, None, &stats), []);
        assert_eq!(list.next_due(), Some(t0 + s(901)));
        // Lifting it, a is due now, and once in flight a second force sends
        // nothing more.
        assert!(list.force(t1, None, true));
        assert_eq!(list.start_due(t1, None, &stats), [(http("a"), None)]);
        assert!(list.force(t1, None, true));
        assert!(list.force(t1, Some(&http("a")), true));
        assert_eq!(list.start_due(t1, None, &stats), []);

        // Forced alone, b goes though a works and the walk would stop there.
        list.on_success(&http("a"), None, t1, &delay);
        assert!(list.force(t1, Some(&http("b")), true));
        assert_eq!(list.start_due(t1, None, &stats), [(http("b"), STARTED)]);
        assert!(!list.force(t1, Some(&http("x")), true));
    }

    /// libtorrent `trigger_announce` writes the entry's next announce with
    /// no regard for an announce waiting for its answer, and
    /// `tracker_response` then writes the answer's intervals over it: a
    /// force made while an announce waits is dropped.
    #[test]
    fn a_force_while_an_announce_waits_is_overwritten_by_its_answer() {
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(list.start_due(t0, None, &stats), [(http("a"), STARTED)]);
        let delay = ReannounceDelay {
            floor: s(900),
            regular: s(1800),
        };

        // Forced while it waits, alone and with the rest: nothing goes.
        let t1 = t0 + s(5);
        assert!(list.force(t1, None, true));
        assert!(list.force(t1, Some(&http("a")), true));
        assert_eq!(list.start_due(t1, None, &stats), []);
        assert_eq!(list.next_due(), None);

        // Answered: due at the answer's interval, the force gone.
        list.on_success(&http("a"), STARTED, t1, &delay);
        assert_eq!(list.start_due(t1, None, &stats), []);
        assert_eq!(list.next_due(), Some(t1 + s(1800)));
    }

    /// Waits, driving `peers`, until the table says `done`.
    async fn table_once(
        status: &super::TrackerStatus,
        peers: &mut (impl futures::Stream<Item = SocketAddr> + Unpin),
        done: impl Fn(&[TrackerAnnounce]) -> bool,
    ) -> Vec<TrackerAnnounce> {
        let mut rx = status.subscribe();
        tokio::select! {
            _ = peers.next() => panic!("the tracker sent peers"),
            r = rx.wait_for(|a| done(a)) => { r.unwrap(); }
        };
        status.announces()
    }

    #[tokio::test]
    async fn a_forced_reannounce_goes_out_now_and_the_status_follows_each_answer() {
        // libtorrent torrent::force_tracker_request with ignore_min_interval,
        // and torrent::tracker_response, tracker_warning and
        // tracker_request_error for the entry's message, counts and fails.
        let (listener, url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        macro_rules! driving {
            ($f:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent peers"),
                    r = $f => r,
                }
            };
        }

        // Answered with a warning: working, the warning its message.
        let (conn, q) = driving!(read_announce(&listener));
        assert_eq!(query_param(&q, "event"), Some("started"));
        respond(
            conn,
            "d8:completei4e10:downloadedi7e10:incompletei2e8:intervali1800e12:min intervali900e15:warning message4:slow5:peers0:e",
        )
        .await;
        let table = table_once(&status, &mut peers, |a| a[0].last.is_some()).await;
        let last = table[0].last.clone().unwrap();
        let ok = last.result.clone().unwrap();
        assert_eq!(
            (
                ok.seeders,
                ok.leechers,
                ok.downloaded,
                ok.warning.as_deref()
            ),
            (Some(4), Some(2), Some(7), Some("slow"))
        );
        assert_eq!(table[0].fails, 0);
        assert_eq!(
            table[0].next_announce,
            Some(last.at + Duration::from_secs(1800))
        );

        // Forced, it goes out now, inside the min interval, with no event;
        // forced again while it waits, nothing more goes out.
        assert!(status.reannounce(Duration::ZERO, None, true));
        let (conn, q) = driving!(read_announce(&listener));
        assert_eq!(query_param(&q, "event"), None);
        assert!(status.announces()[0].in_flight);
        assert!(status.reannounce(Duration::ZERO, Some(&url), true));

        // Refused: not working, the reason its message, one failure.
        respond(conn, "d14:failure reason4:nopee").await;
        let table = table_once(&status, &mut peers, |a| a[0].fails == 1 && !a[0].in_flight).await;
        let error = table[0].last.clone().unwrap().result.unwrap_err();
        assert_eq!(error.kind, AnnounceErrorKind::Refused);
        assert!(error.message.contains("nope"), "{error}");
        assert!(table[0].next_announce.is_some());

        // Forced alone, answered again: working, failures cleared.
        assert!(status.reannounce(Duration::ZERO, Some(&url), true));
        let (conn, _) = driving!(read_announce(&listener));
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        let table = table_once(&status, &mut peers, |a| {
            a[0].fails == 0 && a[0].last.as_ref().is_some_and(|l| l.result.is_ok())
        })
        .await;
        assert_eq!(table[0].last.clone().unwrap().result.unwrap().warning, None);

        drop(peers);
        next_announce(&listener).await;
        stopped.wait_idle().await;
        token.cancel();
    }

    #[tokio::test]
    async fn trackers_added_replaced_and_removed_change_what_is_announced() {
        // libtorrent torrent::replace_trackers and add_tracker announce at
        // once to the trackers the walk now picks.
        let (a, a_url) = http_listener().await;
        let (b, b_url) = http_listener().await;
        let (c, c_url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(Vec::new(), &stopped).await;
        let TrackerTask { mut peers, status } = task;
        macro_rules! driving {
            ($f:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent peers"),
                    r = $f => r,
                }
            };
        }
        assert_eq!(status.announces(), []);

        // Added to a task with none, a is announced to.
        status.replace(op(vec![vec![a_url.clone()]]));
        let (conn, q) = driving!(read_announce(&a));
        assert_eq!(query_param(&q, "event"), Some("started"));
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

        // b put ahead of it is announced to at once; a, kept, keeps its row.
        status.replace(op(vec![vec![b_url.clone()], vec![a_url.clone()]]));
        let urls = |t: &[TrackerAnnounce]| t.iter().map(|a| a.url.clone()).collect::<Vec<_>>();
        assert_eq!(
            urls(&status.announces()),
            [b_url.to_string(), a_url.to_string()]
        );
        assert!(status.announces()[1].last.is_some());
        let (conn, q) = driving!(read_announce(&b));
        assert_eq!(query_param(&q, "event"), Some("started"));

        // c replaces b while b's announce waits: it is abandoned, and c,
        // new, is asked.
        status.replace(op(vec![vec![c_url.clone()], vec![a_url.clone()]]));
        let closed = async {
            let mut byte = [0u8; 1];
            loop {
                conn.readable().await.unwrap();
                match conn.try_read(&mut byte) {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
                    read => break read.unwrap_or(0),
                }
            }
        };
        assert_eq!(driving!(closed), 0, "b's announce ended");
        let (conn, _) = driving!(read_announce(&c));
        assert_eq!(
            urls(&status.announces()),
            [c_url.to_string(), a_url.to_string()]
        );
        respond(conn, "d8:intervali1800e5:peers0:e").await;
        table_once(&status, &mut peers, |t| t[0].last.is_some()).await;

        // c removed, a alone is left; forced, it hears no second `started`.
        status.replace(op(vec![vec![a_url.clone()]]));
        assert_eq!(urls(&status.announces()), [a_url.to_string()]);
        assert!(!status.reannounce(Duration::ZERO, Some(&c_url), true));
        assert!(status.reannounce(Duration::ZERO, None, true));
        let (conn, q) = driving!(read_announce(&a));
        assert_eq!(query_param(&q, "event"), None);
        respond(conn, "d8:intervali1800e5:peers0:e").await;

        drop(peers);
        next_announce(&a).await;
        stopped.wait_idle().await;
        drop((b, c));
        token.cancel();
    }

    #[tokio::test]
    async fn a_tier_added_while_the_torrent_runs_is_announced_to_at_once_and_on_schedule() {
        // libtorrent torrent::add_tracker announces at once while the
        // torrent announces, and with announce_to_all_tiers, as qBittorrent
        // sets it, torrent::announce_with_tracker and update_tracker_timer
        // take one tracker of every tier, not only the first that works.
        let (a, a_url) = http_listener().await;
        let (b, b_url) = http_listener().await;
        let stopped = StoppedAnnounces::default();
        let (task, token) = start(vec![vec![a_url.clone()]], &stopped).await;
        let TrackerTask { mut peers, status } = task;
        macro_rules! driving {
            ($f:expr) => {
                tokio::select! {
                    _ = peers.next() => panic!("the tracker sent peers"),
                    r = $f => r,
                }
            };
        }
        let answered = "d8:intervali1800e12:min intervali900e5:peers0:e";

        let (conn, q) = driving!(read_announce(&a));
        assert_eq!(query_param(&q, "event"), Some("started"));
        respond(conn, answered).await;
        let table = table_once(&status, &mut peers, |t| t[0].last.is_some()).await;
        // What a's row says of its announces; its next one is read off the
        // clock each time the table is.
        let heard = |row: &TrackerAnnounce| (row.in_flight, row.last.clone(), row.fails);
        let a_row = heard(&table[0]);

        // b, a tier of its own added after a heard `started`, is announced
        // to at once though a works; a is not asked again.
        status.replace(op(vec![vec![a_url.clone()], vec![b_url.clone()]]));
        let (conn, q) = driving!(read_announce(&b));
        assert_eq!(query_param(&q, "event"), Some("started"));
        let table = status.announces();
        assert_eq!(heard(&table[0]), a_row);
        assert!(table[1].in_flight);
        respond(conn, answered).await;

        // Answered, b is due again after its interval, as a is after its own.
        let table = table_once(&status, &mut peers, |t| {
            t[1].last.is_some() && !t[1].in_flight
        })
        .await;
        assert_eq!(heard(&table[0]), a_row);
        let b_last = table[1].last.clone().unwrap();
        assert!(b_last.result.is_ok());
        assert_eq!(
            table[1].next_announce,
            Some(b_last.at + Duration::from_secs(1800))
        );

        // Both heard `started`, so both hear `stopped`.
        drop(peers);
        let (stopped_a, stopped_b) = tokio::join!(next_announce(&a), next_announce(&b));
        assert_eq!(query_param(&stopped_a, "event"), Some("stopped"));
        assert_eq!(query_param(&stopped_b, "event"), Some("stopped"));
        stopped.wait_idle().await;
        token.cancel();
    }

    #[test]
    fn a_tier_added_while_announcing_comes_due_at_once_and_after_its_interval() {
        // libtorrent torrent::add_tracker, then update_tracker_timer with
        // announce_to_all_tiers: the new tier keeps its own schedule beside
        // the first's.
        let s = Duration::from_secs;
        let t0 = Instant::now();
        let mut list = list(&[&["a"]], t0);
        let stats = live(0, 90, false);
        assert_eq!(list.start_due(t0, None, &stats), [(http("a"), STARTED)]);
        list.on_success(&http("a"), STARTED, t0, &ANSWERED);
        assert_eq!(list.next_due(), Some(t0 + s(1800)));

        let t1 = t0 + s(60);
        list.replace(&[vec![http("a")], vec![http("b")]], t1);
        assert_eq!(list.next_due(), Some(t1));
        assert_eq!(list.start_due(t1, None, &stats), [(http("b"), STARTED)]);
        list.on_success(&http("b"), STARTED, t1, &ANSWERED);

        assert_eq!(list.next_due(), Some(t0 + s(1800)));
        assert_eq!(
            list.start_due(t0 + s(1800), None, &stats),
            [(http("a"), None)]
        );
        list.on_success(&http("a"), None, t0 + s(1800), &ANSWERED);
        assert_eq!(list.next_due(), Some(t1 + s(1800)));
        assert_eq!(
            list.start_due(t1 + s(1800), None, &stats),
            [(http("b"), None)]
        );
    }
}
