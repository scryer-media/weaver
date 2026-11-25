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
use crate::core::net::{Egress, HttpClients, NamedBy, Resolved, Resolver, is_idna, is_public};
use crate::session_stats::tracker::{AnnounceEvent, TrackerKind, TrackerResult, TrackerStats};
use crate::tracker::Tracker;
use crate::tracker::shape::AnnounceShape;
use crate::tracker::tracker_comms_http;
use crate::tracker::tracker_comms_http::Family;
use crate::tracker::tracker_comms_http::TrackerRequestEvent;
use crate::tracker::tracker_comms_udp;
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

mod protocol;
use protocol::{AnnounceAsk, TrackerProtocol, Turn};
pub use protocol::{TrackerNet, TrackerProtocols};

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
    // The trackers under this hash and what announcing to them has done.
    list: Mutex<TrackerList>,
    // The corrupt and redundant bytes the torrent had counted when each
    // HTTP tracker was last sent `started` over each leg: its announces
    // over that leg report what grew since.
    started_at: Mutex<protocol::StartedAt>,
    status: TrackerStatus,
    // Wakes the announce loop when the list changed under it: trackers
    // replaced or an announce forced.
    poke: tokio::sync::Notify,
    /// Bounds the `stopped` announces this hash sends.
    stopped: StoppedAnnounces,
    /// Hands the task an announce loop for another hash the torrent goes by
    /// (see [`TrackerTask::take_over`]).
    grow: tokio::sync::mpsc::UnboundedSender<Arc<TrackerComms>>,
    /// Where the session counts its announces.
    counts: Arc<TrackerStats>,
    /// The announces waiting for their answers. The run drives them; when
    /// it stops they are left to finish within the stop's bound (see
    /// [`StopOnDrop`]).
    in_flight: Mutex<FuturesUnordered<InFlightAnnounce>>,
    /// Names this hash's run as the one that last wrote every row of the
    /// table (see [`RowIndex::writer`]).
    run: u64,
}

/// A run id no other run of the process has had.
fn next_run() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// An announce of the run, abortable when its tracker is removed: the
/// tracker it went to and the peers its answer named.
type InFlightAnnounce =
    BoxFuture<'static, Result<(SupportedTracker, Vec<SocketAddr>), futures::future::Aborted>>;

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

/// What an announce reports of the torrent (BEP 3). Its byte counts are
/// those since the run began announcing (libtorrent
/// `torrent::start_announcing`); an HTTP announce reports the corrupt and
/// redundant bytes as they grew since that tracker last heard `started`.
#[derive(Default, Clone, Copy, Debug)]
pub struct TrackerCommsStats {
    /// Payload bytes sent to peers since the run began announcing.
    pub uploaded_bytes: u64,
    /// Payload bytes received from peers, less the corrupt and redundant
    /// bytes (see `TorrentTotals::announced_downloaded`).
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
    /// pieces were wanted then, and is a seed now: its trackers are due at
    /// once (libtorrent `torrent::completed`). Whether one is owed
    /// `completed` (BEP 3) is the torrent's [`crate::Completion`], which
    /// outlasts the run.
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

    /// What the run that just ended reports with `stopped`, once every
    /// connection of it has closed and counted what it dropped (libtorrent
    /// `torrent::do_pause` disconnects every peer before
    /// `stop_announcing`), counted from where that run began whatever run
    /// begins meanwhile. Asked as the run's announcing stops. The default
    /// reads [`Self::get`] at once.
    fn run_ended(&self) -> BoxFuture<'static, TrackerCommsStats> {
        futures::future::ready(self.get()).boxed()
    }
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

/// The ports peers reach this session on, one for each address family,
/// None when it accepts no connections. They can change while a torrent
/// runs (a gateway mapping that lands after start), so each announce reads
/// them as it goes out (libtorrent `session_impl::queue_tracker_request`
/// sets `req.listen_port` per request, from the listen socket it goes out
/// of).
pub type ListenPort = tokio::sync::watch::Receiver<ToldPorts>;

/// The port told over each address family.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ToldPorts {
    pub v4: Option<u16>,
    pub v6: Option<u16>,
}

impl ToldPorts {
    /// `port` over both families.
    pub const fn both(port: Option<u16>) -> Self {
        Self { v4: port, v6: port }
    }

    /// The port told over IPv6, or over IPv4.
    pub const fn of(self, ipv6: bool) -> Option<u16> {
        if ipv6 { self.v6 } else { self.v4 }
    }

    /// The port told to `ip`, an IPv4 address written as IPv6 included in
    /// IPv4.
    pub fn to(self, ip: IpAddr) -> Option<u16> {
        self.of(ip.to_canonical().is_ipv6())
    }
}

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

/// A tracker of a scheme the session announces over, the protocol it is
/// spoken to over, and who named it. A tracker is its URL: the same URL
/// named again is the same tracker.
#[derive(Clone)]
struct SupportedTracker {
    url: Url,
    protocol: Arc<dyn TrackerProtocol>,
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

    /// The tracker, spoken to over the protocol `protocols` has for its
    /// scheme; None when it has none.
    fn new(Tracker { url, named_by }: Tracker, protocols: &TrackerProtocols) -> Option<Self> {
        let Some(protocol) = protocols.of(url.scheme()) else {
            debug!(
                "unsupported tracker URL: {}",
                redacted_tracker_url(url.as_str())
            );
            return None;
        };
        Some(Self {
            protocol: protocol.clone(),
            url,
            named_by,
        })
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

/// `text` a tracker sent (a failure reason, a warning, a UDP error) with
/// every URL in it shown as [`redacted_tracker_url`] shows one: a tracker
/// that echoes its announce URL echoes the passkey in it. A URL runs from
/// its scheme to the next space, quote or angle bracket.
pub fn redacted_tracker_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("://") {
        let start = rest[..at]
            .char_indices()
            .rfind(|&(_, c)| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')))
            .map_or(0, |(before, c)| before + c.len_utf8());
        let end = rest[at..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>'))
            .map_or(rest.len(), |after| at + after);
        out.push_str(&rest[..start]);
        if start == at {
            // No scheme before it: not a URL, kept as it came.
            out.push_str(&rest[at..end]);
        } else {
            out.push_str(&redacted_tracker_url(&rest[start..end]));
        }
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
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

/// The way one announce to a tracker leaves: held to IPv4, held to IPv6, or
/// held to no family, as when a proxy dials the tracker. A tracker hears
/// each as a peer of its own, so each keeps whether it answered `started`
/// (libtorrent keeps one `announce_endpoint` per listen socket, each with
/// its own `start_sent`, torrent.cpp:2993-3035 `refresh_endpoint_list`,
/// 3605-3606, v2.0.15).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Leg {
    V4,
    V6,
    Unbound,
}

impl Leg {
    /// The leg of an HTTP announce held to `family`, or to none.
    fn of_route(family: Option<Family>) -> Self {
        match family {
            Some(Family::V4) => Self::V4,
            Some(Family::V6) => Self::V6,
            None => Self::Unbound,
        }
    }

    /// The leg of a datagram to `addr`.
    fn of_addr(addr: SocketAddr) -> Self {
        Self::of_route(Some(Family::of(addr.ip())))
    }

    const fn bit(self) -> u8 {
        match self {
            Self::V4 => 1,
            Self::V6 => 2,
            Self::Unbound => 4,
        }
    }

    const ALL: [Leg; 3] = [Leg::V4, Leg::V6, Leg::Unbound];
}

/// A set of [`Leg`]s.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Legs(u8);

impl Legs {
    fn has(self, leg: Leg) -> bool {
        self.0 & leg.bit() != 0
    }

    fn add(&mut self, leg: Leg) {
        self.0 |= leg.bit();
    }

    fn any(self) -> bool {
        self.0 != 0
    }

    fn iter(self) -> impl Iterator<Item = Leg> {
        Leg::ALL.into_iter().filter(move |leg| self.has(*leg))
    }
}

impl std::ops::BitOrAssign for Legs {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

impl FromIterator<Leg> for Legs {
    fn from_iter<I: IntoIterator<Item = Leg>>(legs: I) -> Self {
        let mut set = Self::default();
        for leg in legs {
            set.add(leg);
        }
        set
    }
}

/// Trackers that answered `started`, each with the legs that answered it.
type StartedTrackers = Vec<(SupportedTracker, Legs)>;

/// The events one announce to a tracker carries, leg by leg (libtorrent
/// picks the event of each endpoint from its own `start_sent`,
/// torrent.cpp:3323-3331, v2.0.15).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Events {
    /// The legs that answered `started`.
    started: Legs,
    /// The event over a leg that answered `started`.
    after_start: Option<TrackerRequestEvent>,
    /// The event over any other leg; None passes that leg over, as
    /// `stopped` passes over a leg that never answered `started`
    /// (torrent.cpp:3279-3282, v2.0.15).
    unstarted: Option<Option<TrackerRequestEvent>>,
}

impl Events {
    /// `stopped`, over the legs of `started` alone.
    fn stopped(started: Legs) -> Self {
        Self {
            started,
            after_start: Some(TrackerRequestEvent::Stopped),
            unstarted: None,
        }
    }

    /// The event over `leg`, or None when it is passed over.
    fn over(&self, leg: Leg) -> Option<Option<TrackerRequestEvent>> {
        match self.started.has(leg) {
            true => Some(self.after_start),
            false => self.unstarted,
        }
    }

    /// Some leg carries an event.
    fn any(&self) -> bool {
        self.after_start.is_some() || self.unstarted.flatten().is_some()
    }

    /// The event the announce is counted under: that of the legs that
    /// answered `started` when there are any, else that of the others.
    fn counted_as(&self) -> AnnounceEvent {
        let event = match self.started.any() {
            true => self.after_start,
            false => self.unstarted.flatten(),
        };
        match event {
            None => AnnounceEvent::Periodic,
            Some(TrackerRequestEvent::Started) => AnnounceEvent::Started,
            Some(TrackerRequestEvent::Completed) => AnnounceEvent::Completed,
            Some(TrackerRequestEvent::Stopped) => AnnounceEvent::Stopped,
            Some(TrackerRequestEvent::Paused) => AnnounceEvent::Paused,
        }
    }
}

/// What a tracker accepted of one announce: the legs it answered `started`
/// over, and whether it answered `completed` over any.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Accepted {
    started: Legs,
    completed: bool,
}

impl Accepted {
    /// `leg` carried `event`, and the tracker accepted it.
    fn take(&mut self, leg: Leg, event: Option<TrackerRequestEvent>) {
        match event {
            Some(TrackerRequestEvent::Started) => self.started.add(leg),
            Some(TrackerRequestEvent::Completed) => self.completed = true,
            Some(TrackerRequestEvent::Stopped | TrackerRequestEvent::Paused) | None => {}
        }
    }
}

/// One tracker as one hash of the torrent announces to it, and how that has
/// gone (libtorrent `announce_infohash`, with the tier of its
/// `announce_entry`). Its intervals and failures are the tracker's, shared
/// by its legs, which announce together.
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
    /// The listen ports our last announce to it carried; one that differs
    /// from the current ports is due at once (libtorrent
    /// `session_impl::reopen_listen_sockets` forces every tracker's request
    /// when the listen port changes).
    port: ToldPorts,
    /// The legs over which it answered `started`.
    start_sent: Legs,
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
            port: ToldPorts::default(),
            start_sent: Legs::default(),
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
    fn can_announce(&self, now: Instant, seed: bool, port: ToldPorts) -> bool {
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
    /// Whether a tracker accepted `completed`, the torrent's across its
    /// runs and hashes (libtorrent `torrent::m_complete_sent`).
    completion: crate::Completion,
    /// Trackers forced one at a time, announced to on the next turn when
    /// they may be, whatever the walk would pick (libtorrent
    /// `torrent::announce_tracker_now`).
    forced: Vec<SupportedTracker>,
    /// The run announces until it stops (libtorrent
    /// `torrent::m_announcing`); after that no tracker is due again.
    announcing: bool,
    /// A route move took the torrent to another session, where these
    /// announces go on: it stops with no `stopped`.
    handed_over: bool,
    /// How the torrent's announces are written.
    shape: &'static AnnounceShape,
    /// A tracker's failures or next announce changed since the list last
    /// wrote every row of the table, other than by an answer written
    /// since: the next write writes every row.
    unpublished: bool,
}

impl TrackerList {
    /// A tracker named twice is kept where it is first named (libtorrent
    /// `tracker_list::add_tracker`).
    fn new(
        tiers: &[Vec<SupportedTracker>],
        now: Instant,
        shape: &'static AnnounceShape,
        completion: crate::Completion,
    ) -> Self {
        let mut entries: Vec<Entry> = Vec::new();
        let mut named = HashSet::new();
        for (tier, trackers) in tiers.iter().enumerate() {
            for tracker in trackers {
                if !named.insert(tracker) {
                    continue;
                }
                entries.push(Entry::new(tracker.clone(), tier, now));
            }
        }
        Self {
            entries,
            completion,
            forced: Vec::new(),
            announcing: true,
            handed_over: false,
            shape,
            unpublished: false,
        }
    }

    /// The announces go on from a session a route move took the torrent
    /// from: each tracker of `started` answered `started` there over the
    /// legs named, so it is sent no other over them.
    fn going_on(mut self, started: &HashMap<(Url, Leg), (u64, u64)>) -> Self {
        let mut legs: HashMap<&Url, Legs> = HashMap::new();
        for (url, leg) in started.keys() {
            legs.entry(url).or_default().add(*leg);
        }
        for entry in &mut self.entries {
            if let Some(legs) = legs.get(&entry.tracker.url) {
                entry.start_sent |= *legs;
            }
        }
        self
    }

    /// The trackers become `tiers`, in the order given, a tracker named
    /// twice kept where first named (libtorrent `tracker_list::replace`). A
    /// tracker kept keeps what announcing to it has done, so it is not sent
    /// `started` again nor let off its back-off; libtorrent starts every
    /// entry afresh. A tracker that is gone is forgotten; those of them that
    /// accepted `started` are returned, with the legs that accepted it.
    // Each tracker is looked up once in sets built for the pass, so a pass
    // costs the trackers named and kept, not their product.
    fn replace(&mut self, tiers: &[Vec<SupportedTracker>], now: Instant) -> StartedTrackers {
        let old = std::mem::take(&mut self.entries);
        let at: HashMap<SupportedTracker, usize> = old
            .iter()
            .enumerate()
            .map(|(i, e)| (e.tracker.clone(), i))
            .collect();
        let mut old: Vec<Option<Entry>> = old.into_iter().map(Some).collect();
        let mut named = HashSet::new();
        for (tier, trackers) in tiers.iter().enumerate() {
            for tracker in trackers {
                if !named.insert(tracker) {
                    continue;
                }
                let entry = match at.get(tracker).and_then(|&i| old[i].take()) {
                    Some(kept) => Entry {
                        tracker: tracker.clone(),
                        tier,
                        ..kept
                    },
                    None => Entry::new(tracker.clone(), tier, now),
                };
                self.entries.push(entry);
            }
        }
        self.forced.retain(|t| named.contains(t));
        old.into_iter()
            .flatten()
            .filter(|e| e.start_sent.any())
            .map(|e| (e.tracker, e.start_sent))
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

    /// [`Self::force`] of each of `trackers` alone, in their order, in one
    /// pass over the list: what calling it for each in turn does.
    fn force_each(
        &mut self,
        at: Instant,
        trackers: &[SupportedTracker],
        ignore_min_interval: bool,
    ) {
        let named: HashSet<&SupportedTracker> = trackers.iter().collect();
        let mut found: HashSet<SupportedTracker> = HashSet::new();
        for entry in self
            .entries
            .iter_mut()
            .filter(|e| named.contains(&e.tracker))
        {
            entry.next_announce = if ignore_min_interval {
                entry.min_announce = at;
                at
            } else {
                at.max(entry.min_announce)
            } + Duration::from_secs(1);
            found.insert(entry.tracker.clone());
        }
        let mut forced: HashSet<SupportedTracker> = self.forced.iter().cloned().collect();
        for tracker in trackers {
            if found.contains(tracker) && forced.insert(tracker.clone()) {
                self.forced.push(tracker.clone());
            }
        }
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

    /// The regular announces due at `now`, each with its events, marked as
    /// waiting for their answers; `port` is the listen ports they carry.
    fn start_due(
        &mut self,
        now: Instant,
        port: ToldPorts,
        stats: &TrackerCommsStats,
    ) -> Vec<(SupportedTracker, Events)> {
        let seed = stats.completed_during_run || self.owes_completed(stats);
        // A tracker forced alone goes now if it may, else the walk and the
        // timer reach it (libtorrent `torrent::announce_tracker_now`); one
        // waiting for an answer is not asked twice.
        let forced = std::mem::take(&mut self.forced);
        let at: HashMap<&SupportedTracker, usize> = if forced.is_empty() {
            HashMap::new()
        } else {
            self.entries
                .iter()
                .enumerate()
                .rev()
                .map(|(i, e)| (&e.tracker, i))
                .collect()
        };
        let mut picked: Vec<usize> = forced
            .iter()
            .filter_map(|t| at.get(t).copied())
            .filter(|&i| self.entries[i].can_announce(now, seed, port))
            .collect();
        drop(at);
        let mut seen: HashSet<usize> = picked.iter().copied().collect();
        for i in self.walk(|_| true, |e| e.can_announce(now, seed, port)) {
            if seen.insert(i) {
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

    /// The torrent is a seed and no tracker has accepted `completed` from it
    /// (libtorrent `!m_complete_sent && is_seed()`, torrent.cpp:3327-3329,
    /// v2.0.15): a run that began as a seed after a pause or a move still
    /// owes the `completed` no tracker answered before it.
    fn owes_completed(&self, stats: &TrackerCommsStats) -> bool {
        stats.is_completed() && !self.completion.is_sent()
    }

    /// The events a regular announce to `entry` carries (libtorrent
    /// `torrent::announce_with_tracker`, torrent.cpp:3323-3333, v2.0.15),
    /// over each leg: `started` until that leg accepts one, then
    /// `completed` while the torrent owes one, and `paused` while it has
    /// every piece it wants and is not a seed. A shape that says `paused`
    /// first says it in place of `started` too.
    fn event(&self, entry: &Entry, stats: &TrackerCommsStats) -> Events {
        let paused = stats.finished && !stats.is_completed();
        let paused_first = paused && self.shape.paused_first;
        let after_start = if paused_first {
            Some(TrackerRequestEvent::Paused)
        } else if self.owes_completed(stats) && !entry.complete_sent {
            Some(TrackerRequestEvent::Completed)
        } else if paused {
            Some(TrackerRequestEvent::Paused)
        } else {
            None
        };
        let unstarted = match paused_first {
            true => TrackerRequestEvent::Paused,
            false => TrackerRequestEvent::Started,
        };
        Events {
            started: entry.start_sent,
            after_start,
            unstarted: Some(Some(unstarted)),
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

    /// `tracker` accepted an announce, `accepted` over the legs it says
    /// (libtorrent `torrent::tracker_response`, torrent.cpp:3605-3619,
    /// v2.0.15: `started` marks the endpoint that sent it, `completed` the
    /// whole torrent).
    fn on_success(
        &mut self,
        tracker: &SupportedTracker,
        accepted: Accepted,
        now: Instant,
        delay: &ReannounceDelay,
    ) -> Option<usize> {
        let i = self.position(tracker)?;
        let entry = &mut self.entries[i];
        entry.updating = false;
        entry.fails = 0;
        entry.next_announce = now + delay.regular;
        entry.min_announce = now + delay.floor;
        entry.start_sent |= accepted.started;
        if accepted.completed {
            entry.complete_sent = true;
            self.completion.sent();
        }
        Some(i)
    }

    /// An announce to `tracker` failed: it backs off (libtorrent
    /// `announce_infohash::failed`) and goes to the end of its tier
    /// (`tracker_list::deprioritize_tracker`). Where it now is in the list.
    fn on_failure(
        &mut self,
        tracker: &SupportedTracker,
        now: Instant,
        retry_interval: Duration,
    ) -> Option<usize> {
        let i = self.position(tracker)?;
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
        Some(end_of_tier)
    }

    /// The torrent became a seed: every tracker not yet told is due at once
    /// (libtorrent `tracker_list::completed`).
    fn completed(&mut self, now: Instant) {
        for entry in self.entries.iter_mut().filter(|e| !e.complete_sent) {
            entry.next_announce = now;
            entry.min_announce = now;
        }
        self.unpublished = true;
    }

    /// The failures and next announce the table shows for `entry`, `now`
    /// being `at` on the wall clock.
    fn row_value(&self, entry: &Entry, now: Instant, at: SystemTime) -> (u8, Option<SystemTime>) {
        let asked = entry.start_sent.any() || entry.fails > 0;
        let next = (self.announcing && asked)
            .then(|| wall(entry.next_announce.max(entry.min_announce), now, at));
        (entry.fails, next)
    }

    /// The run stops announcing (libtorrent `torrent::stop_announcing`): no
    /// tracker is due again, and the trackers owed `stopped` are the walk
    /// over those that accepted `started`, each of them due, less any whose
    /// last announce still waits for its answer
    /// (`announce_infohash::can_announce` with `updating`, as
    /// `torrent::announce_with_tracker` asks it for `stopped` too,
    /// torrent.cpp:3308, v2.0.15). A working tracker passed over still
    /// stands for its tier. A run whose announces a route move handed over
    /// owes none: the session it went to announces on. Each tracker owed
    /// one comes with the legs that answered `started`, the only ones it is
    /// sent over (torrent.cpp:3279-3282, v2.0.15).
    ///
    /// The trackers passed over because their announce waits are returned
    /// apart, as [`Stopping::waiting`]: each is owed `stopped` after all if
    /// that announce is cut, as at the session's shutdown, since the
    /// announce that would have told it more never comes.
    fn stop(&mut self) -> Stopping {
        self.announcing = false;
        let mut stopping = Stopping::default();
        if self.handed_over {
            return stopping;
        }
        for i in self.walk(|e| e.start_sent.any(), |_| true) {
            let entry = &self.entries[i];
            let owed = (entry.tracker.clone(), entry.start_sent);
            match entry.updating {
                true => stopping.waiting.push(owed),
                false => stopping.owed.push(owed),
            }
        }
        stopping
    }
}

/// The trackers a run's stop owes `stopped` (see [`TrackerList::stop`]).
#[derive(Debug, Default)]
struct Stopping {
    /// Sent `stopped` now.
    owed: StartedTrackers,
    /// Accepted `started`, and their last announce still waits for its
    /// answer: sent `stopped` only if that announce is cut.
    waiting: StartedTrackers,
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
                redacted_tracker_text(&error.failure_reason.to_string())
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

/// How long an announce may take from start to finish, whatever
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
        .map_err(|_| anyhow::Error::new(Unanswered))?
}

/// A request to a tracker that was given up after [`COMPLETION_TIMEOUT`].
#[derive(Debug)]
struct Unanswered;

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the tracker did not answer within {COMPLETION_TIMEOUT:?}"
        )
    }
}

impl std::error::Error for Unanswered {}

/// What a request to a tracker over `protocol` is counted as having ended
/// in.
fn counted_result<T>(result: &anyhow::Result<T>, protocol: &dyn TrackerProtocol) -> TrackerResult {
    let Err(e) = result else {
        return TrackerResult::Ok;
    };
    for cause in e.chain() {
        if cause.is::<Unanswered>() || cause.is::<tokio::time::error::Elapsed>() {
            return TrackerResult::Timeout;
        }
        if let Some(refused) = cause.downcast_ref::<Refused>() {
            return match refused.failure {
                true => TrackerResult::Refused,
                false => TrackerResult::BadReply,
            };
        }
        if let Some(counted) = protocol.classify(cause) {
            return counted;
        }
        if cause.downcast_ref::<Unreachable>() == Some(&Unreachable::Family) {
            return TrackerResult::Skipped;
        }
    }
    TrackerResult::Error
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
    /// The events of it the tracker accepted, leg by leg.
    accepted: Accepted,
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
    /// Downloads the tracker has seen finish (`downloaded`).
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

struct StatusShared {
    announces: tokio::sync::watch::Sender<Vec<TrackerAnnounce>>,
    /// The protocols trackers are spoken to over, by scheme.
    protocols: TrackerProtocols,
    /// What the session counts of its trackers, which the tests read here.
    #[cfg(test)]
    counts: Arc<TrackerStats>,
    /// The announce loops of the tracker task, one per hash, while it runs.
    runs: Mutex<Vec<std::sync::Weak<TrackerComms>>>,
    /// Where each tracker's row is. Locked only while the table is, after
    /// it.
    rows: Mutex<RowIndex>,
}

/// The row of each tracker in the table, rebuilt only when the table's
/// trackers are replaced, and the run whose list the rows' failures and
/// next announces show.
#[derive(Debug, Default)]
struct RowIndex {
    /// The first row of each URL.
    at: HashMap<String, usize>,
    /// The run that last wrote every row; none since the trackers were
    /// replaced. While it is the run writing, and its list has changed
    /// since only by answers it wrote, every row but the answered one
    /// already shows what its list holds.
    writer: u64,
}

impl RowIndex {
    fn of(rows: &[TrackerAnnounce]) -> Self {
        let mut at = HashMap::with_capacity(rows.len());
        for (i, row) in rows.iter().enumerate() {
            at.entry(row.url.clone()).or_insert(i);
        }
        Self { at, writer: 0 }
    }
}

/// One entry per tracker of `tiers` that the session can announce to over
/// one of `protocols`, in order, a tracker named twice kept where first
/// named.
fn supported_tiers(
    tiers: Vec<Vec<Tracker>>,
    protocols: &TrackerProtocols,
) -> Vec<Vec<SupportedTracker>> {
    tiers
        .into_iter()
        .map(|urls| {
            urls.into_iter()
                .filter_map(|url| SupportedTracker::new(url, protocols))
                .collect()
        })
        .filter(|tier: &Vec<SupportedTracker>| !tier.is_empty())
        .collect()
}

fn unique<'a>(trackers: impl IntoIterator<Item = &'a SupportedTracker>) -> Vec<SupportedTracker> {
    let mut seen = HashSet::new();
    trackers
        .into_iter()
        .filter(|tracker| seen.insert(*tracker))
        .cloned()
        .collect()
}

/// The table's rows for `trackers`, in their order: a tracker that has a
/// row in `rows` keeps the first of them, and one that has none gets a new
/// row. The rows are looked up in an index built once.
fn replaced_rows(rows: &[TrackerAnnounce], trackers: &[SupportedTracker]) -> Vec<TrackerAnnounce> {
    let index = RowIndex::of(rows);
    trackers
        .iter()
        .map(|t| {
            let url = t.url().as_str();
            match index.at.get(url) {
                Some(&i) => rows[i].clone(),
                None => TrackerAnnounce {
                    url: url.to_owned(),
                    ..Default::default()
                },
            }
        })
        .collect()
}

/// `instant` on the wall clock.
fn wall(instant: Instant, now: Instant, at: SystemTime) -> SystemTime {
    match instant.checked_duration_since(now) {
        Some(ahead) => at + ahead,
        None => at - now.duration_since(instant),
    }
}

/// The announce table of one [`TrackerComms`]: one entry per tracker it was
/// started with, in the order given, from the start. Clones share the
/// table, which stays readable after the tracker task ends.
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
        protocols: TrackerProtocols,
        counts: Arc<TrackerStats>,
    ) -> Self {
        let announces: Vec<TrackerAnnounce> = unique(trackers)
            .iter()
            .map(|t| TrackerAnnounce {
                url: t.url().to_string(),
                ..Default::default()
            })
            .collect();
        #[cfg(not(test))]
        let _ = counts;
        let rows = Mutex::new(RowIndex::of(&announces));
        Self(Arc::new(StatusShared {
            rows,
            announces: tokio::sync::watch::channel(announces).0,
            protocols,
            #[cfg(test)]
            counts,
            runs: Default::default(),
        }))
    }

    /// What the session has counted of its trackers.
    #[cfg(test)]
    fn counted(&self) -> crate::session_stats::tracker::TrackerSnapshot {
        self.0.counts.snapshot()
    }

    /// A route move takes the torrent to another session, where its
    /// announces go on: no hash that still announces sends `stopped` as it
    /// stops. The trackers that answered each hash's `started`, leg by leg,
    /// or None when no hash announces any more.
    pub(crate) fn hand_over(&self) -> Option<crate::identity::Started> {
        let mut started = crate::identity::Started::new();
        let mut announcing = false;
        for comms in self.0.runs.lock().iter().filter_map(|run| run.upgrade()) {
            let mut list = comms.list.lock();
            if !list.announcing {
                continue;
            }
            announcing = true;
            list.handed_over = true;
            let started_at = comms.started_at.lock();
            for entry in &list.entries {
                for leg in entry.start_sent.iter() {
                    let url = entry.tracker.url.clone();
                    let at = started_at
                        .get(&(url.clone(), leg))
                        .copied()
                        .unwrap_or_default();
                    started.insert((comms.info_hash, url, leg), at);
                }
            }
        }
        announcing.then_some(started)
    }

    /// The trackers become `tiers`, each with who named it: the table lists
    /// them in order, a tracker kept keeping its row, and a
    /// running task announces to them from now on. An announce waiting on a
    /// tracker that is gone is abandoned.
    pub fn replace(&self, tiers: Vec<Vec<Tracker>>) {
        self.replace_tiers(&supported_tiers(tiers, &self.0.protocols));
    }

    /// [`Self::replace`], made only when `tiers` name other trackers than
    /// the task holds, or one in another tier or by someone else: a list
    /// that holds them already keeps its shuffled order. For a run begun
    /// with the trackers as they stood when its task was made, which an edit
    /// since may have changed.
    pub(crate) fn catch_up(&self, tiers: Vec<Vec<Tracker>>) {
        let tiers = supported_tiers(tiers, &self.0.protocols);
        let mut wanted = HashMap::new();
        for (tier, trackers) in tiers.iter().enumerate() {
            for tracker in trackers {
                wanted
                    .entry(tracker.url())
                    .or_insert((tier, tracker.named_by));
            }
        }
        let held = self.runs().iter().all(|comms| {
            let list = comms.list.lock();
            list.entries.len() == wanted.len()
                && list
                    .entries
                    .iter()
                    .all(|e| wanted.get(e.tracker.url()) == Some(&(e.tier, e.tracker.named_by)))
        });
        if !held {
            self.replace_tiers(&tiers);
        }
    }

    /// [`Self::replace`], with each running hash and the trackers gone from
    /// it that accepted `started`, with the legs that accepted it.
    fn replace_tiers(
        &self,
        tiers: &[Vec<SupportedTracker>],
    ) -> Vec<(Arc<TrackerComms>, StartedTrackers)> {
        let trackers = unique(tiers.iter().flatten());
        self.0.announces.send_modify(|announces| {
            *announces = replaced_rows(announces, &trackers);
            *self.0.rows.lock() = RowIndex::of(announces);
        });
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

    /// [`Self::reannounce`] to each of `trackers` alone, in their order, with
    /// one pass over each table and one publish: what calling it for each
    /// in turn does. Those that are not the torrent's are passed over.
    pub fn reannounce_each(&self, after: Duration, trackers: &[Url], ignore_min_interval: bool) {
        let protocols = &self.0.protocols;
        let known: HashSet<String> = self
            .0
            .announces
            .borrow()
            .iter()
            .map(|a| a.url.clone())
            .collect();
        let trackers: Vec<SupportedTracker> = trackers
            .iter()
            .filter_map(|url| SupportedTracker::new(Tracker::torrent(url.clone()), protocols))
            .filter(|t| known.contains(t.url().as_str()))
            .collect();
        if trackers.is_empty() {
            return;
        }
        for comms in self.runs() {
            let at = Instant::now() + after;
            comms
                .list
                .lock()
                .force_each(at, &trackers, ignore_min_interval);
            comms.publish(Instant::now(), SystemTime::now());
            comms.poke.notify_one();
        }
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
        let protocols = &self.0.protocols;
        let tracker = match tracker
            .map(|url| SupportedTracker::new(Tracker::torrent(url.clone()), protocols))
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
    /// keeps the last answer `before` had from it, until this run has one
    /// of its own. A resumed torrent's
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

    /// Marks an announce to `tracker` as sent; the returned guard records how
    /// it ended.
    fn begin(&self, tracker: &SupportedTracker) -> InFlight<'_> {
        let url = tracker.url().to_string();
        self.0.announces.send_modify(|announces| {
            if let Some(&i) = self.0.rows.lock().at.get(&url) {
                announces[i].in_flight = true;
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
            let row = self.status.0.rows.lock().at.get(&self.url).copied();
            if let Some(a) = row.map(|i| &mut announces[i]) {
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
        let tiers = supported_tiers(tiers, &self.status.0.protocols);
        for (comms, gone) in self.status.replace_tiers(&tiers) {
            comms.send_stopped(
                Stopping {
                    owed: gone,
                    waiting: Vec::new(),
                },
                FuturesUnordered::new(),
                comms.stopped.register(),
            );
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

/// The HTTP announces that may be out at once, across every torrent
/// (libtorrent `settings_pack::max_concurrent_http_announces`,
/// settings_pack.cpp:388).
pub const MAX_CONCURRENT_HTTP_ANNOUNCES: usize = 50;

/// Holds the HTTP announces out at once to a limit, across every torrent
/// that shares it. An announce past the limit waits its turn: one that
/// carries an event (`started`, `completed`, `stopped`) ahead of every
/// plain re-announce, each kind in the order it came (libtorrent
/// `tracker_manager::queue_request` and `remove_request`,
/// tracker_manager.cpp:237-263 and 300-325, puts a high priority request
/// at the front of its one queue). The wait is not part of the announce's
/// own time limit, as libtorrent starts a queued request's timer only when
/// it is started.
///
/// Clones share their state: create one per session, or one for every
/// session that shares limits.
#[derive(Clone)]
pub struct HttpAnnounces(Arc<Mutex<HttpQueue>>);

type Waiter = tokio::sync::oneshot::Sender<HttpAnnounceSlot>;

struct HttpQueue {
    limit: usize,
    out: usize,
    events: std::collections::VecDeque<Waiter>,
    plain: std::collections::VecDeque<Waiter>,
}

impl Default for HttpAnnounces {
    fn default() -> Self {
        Self::new(MAX_CONCURRENT_HTTP_ANNOUNCES)
    }
}

impl HttpAnnounces {
    /// At most `limit` HTTP announces out at once, at least one.
    pub fn new(limit: usize) -> Self {
        Self(Arc::new(Mutex::new(HttpQueue {
            limit: limit.max(1),
            out: 0,
            events: Default::default(),
            plain: Default::default(),
        })))
    }

    /// A turn to announce, held until dropped; `event` puts the wait ahead
    /// of every plain re-announce's.
    async fn take(&self, event: bool) -> HttpAnnounceSlot {
        let rx = {
            let mut queue = self.0.lock();
            if queue.out < queue.limit {
                queue.out += 1;
                return HttpAnnounceSlot(Some(self.clone()));
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            match event {
                true => queue.events.push_back(tx),
                false => queue.plain.push_back(tx),
            }
            rx
        };
        // A waiter's sender stays queued until a turn is sent through it,
        // and the queue lives in `self`.
        rx.await.expect("the queue hands every waiter a turn")
    }

    /// A turn given back: handed to the first waiter still waiting, or
    /// counted free.
    fn give_back(&self) {
        let mut queue = self.0.lock();
        let queue = &mut *queue;
        while let Some(next) = queue.events.pop_front().or_else(|| queue.plain.pop_front()) {
            match next.send(HttpAnnounceSlot(Some(self.clone()))) {
                Ok(()) => return,
                // A waiter that gave up dropped its receiver: the turn comes
                // back unspent and goes to the next.
                Err(mut unspent) => unspent.0 = None,
            }
        }
        queue.out -= 1;
    }

    #[cfg(test)]
    fn out(&self) -> usize {
        self.0.lock().out
    }
}

/// One HTTP announce's turn, handed on once dropped. A turn sent to a
/// waiter that goes away before taking it is dropped with it, so handed on
/// too.
struct HttpAnnounceSlot(Option<HttpAnnounces>);

impl Drop for HttpAnnounceSlot {
    fn drop(&mut self) {
        if let Some(announces) = self.0.take() {
            announces.give_back();
        }
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
    // Cancelled once the session that shares these settings shuts down:
    // from then on an announce still waiting for its answer when its run
    // stops is dropped, not waited for.
    shutting_down: tokio_util::sync::CancellationToken,
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
            shutting_down: Default::default(),
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

    /// The session is shutting down, through any clone: every announce
    /// still waiting for its answer when its run stops, or already left to
    /// finish by a run that stopped before, is dropped at once. The
    /// `stopped` announces still go, within their bound. libtorrent's
    /// `tracker_manager::abort_all_requests` at `session_impl::abort` cuts
    /// every request but a `stopped` (tracker_manager.cpp:520-527).
    pub fn shut_down(&self) {
        self.shutting_down.cancel();
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.is_cancelled()
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
///
/// An announce still waiting for its answer is not cut off: it finishes,
/// and is recorded, within the same bound, as libtorrent's
/// `torrent::stop_announcing` leaves a request already sent to finish
/// (torrent.cpp:10815-10826; only `session_impl::abort` and `pause`,
/// through `tracker_manager::abort_all_requests`, abort one). So a
/// `completed` on its way when the run stops still reaches its tracker. A
/// run whose announces a route move handed over, or one with no time left
/// for its stop, abandons them, as does one stopped while its session
/// shuts down (see [`StoppedAnnounces::shut_down`]).
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
        let (mut stopping, handed_over) = {
            let mut list = self.comms.list.lock();
            (list.stop(), list.handed_over)
        };
        let in_flight = std::mem::take(&mut *self.comms.in_flight.lock());
        self.comms.publish(Instant::now(), SystemTime::now());
        if let Some(running) = self.running.take() {
            // Stopped while its session shuts down: the announces in flight
            // are cut now, so the trackers they leave are owed `stopped`.
            let in_flight = match handed_over || self.comms.stopped.is_shutting_down() {
                true => {
                    let waiting = std::mem::take(&mut stopping.waiting);
                    stopping.owed.extend(waiting);
                    FuturesUnordered::new()
                }
                false => in_flight,
            };
            self.comms.send_stopped(stopping, in_flight, running);
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

/// The `port` of an announce: the port peers reach this session on, or 1
/// for a session that accepts no connections, as libtorrent announces it
/// (`src/session_impl.cpp`, `make_announce_port`: "announce port=1 instead
/// of port=0, when there is no listen port", 1.2.5). A tracker may refuse
/// port 0, and an announce it refuses brings no peers.
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

/// One announce's answers, one per leg with the event it carried, as one:
/// the first accepted answer with the peers of every other accepted one,
/// and what each accepting leg accepted; the first refusal when no leg's
/// answer was accepted.
fn merged(
    answers: impl IntoIterator<Item = (Leg, Option<TrackerRequestEvent>, anyhow::Result<Announced>)>,
) -> anyhow::Result<Announced> {
    let (mut announced, mut error) = (None::<Announced>, None);
    let mut accepted = Accepted::default();
    for (leg, event, answer) in answers {
        match answer {
            Ok(more) => {
                accepted.take(leg, event);
                match &mut announced {
                    Some(announced) => announced.peers.extend(more.peers),
                    None => announced = Some(more),
                }
            }
            Err(e) => {
                error.get_or_insert(e);
            }
        }
    }
    match (announced, error) {
        (Some(mut announced), _) => {
            announced.accepted = accepted;
            Ok(announced)
        }
        (None, Some(error)) => Err(error),
        (None, None) => anyhow::bail!("the tracker was not announced to"),
    }
}

/// What a torrent's tracker task is started with (see
/// [`TrackerComms::start_with_tiers`]).
pub struct TrackerSetup {
    pub info_hashes: InfoHashes,
    pub identity: Identity,
    pub tiers: Vec<Vec<Tracker>>,
    pub stats: Box<dyn TorrentStatsProvider>,
    pub force_interval: Option<Duration>,
    /// The ports peers reach this session on, read as each announce goes
    /// out.
    pub listen_port: ListenPort,
    /// Takes the address an accepted answer says is ours.
    pub vote: ExternalIpVote,
    /// The protocols the trackers are spoken to over, by scheme.
    pub protocols: TrackerProtocols,
    pub stopped: StoppedAnnounces,
    /// Where the session counts its announces.
    pub counts: Arc<TrackerStats>,
}

impl TrackerComms {
    /// Announce to the trackers of a multitracker torrent (BEP 12): one
    /// tracker of each tier at a time, the first in it that works, the
    /// trackers of each tier in random order (see `TrackerList`). A failing tracker
    /// is tried again after a delay that grows with each failure
    /// (`retry_delay`).
    ///
    /// Each tracker is sent `started`, then `completed` once the torrent is
    /// a seed while no tracker has accepted one (see
    /// [`crate::Completion`]), and `stopped` when the returned stream is
    /// dropped. `stopped` is bounded
    /// and tracked by `stopped`. Each announce, `stopped` included, is
    /// recorded in the returned [`TrackerStatus`]. `listen_port` is the port peers reach this
    /// session on, None when it accepts no connections; when it changes, each
    /// tier announces it as soon as its tracker's `min interval` allows.
    /// An announce names none of our addresses, and the tracker takes the one
    /// it was reached from, except a private torrent's HTTP announce in a
    /// shape that lists them (qBittorrent's), which names the public
    /// addresses it leaves from as `ipv4` and `ipv6`.
    /// `vote` takes the address an accepted answer says is
    /// ours.
    /// A tracker, HTTP or UDP, every hop, is asked only
    /// at the addresses its protocols' `reachable` lets through (see
    /// [`TrackerNet`]), and only at public ones
    /// unless the operator named it (see [`NamedBy::may_ask`]).
    ///
    /// Each of `info_hashes.ids20()` is announced on its own, with its own
    /// events and failover: a hybrid under v1 and the truncated v2, a v2
    /// torrent under the truncated v2: one announce per protocol version.
    ///
    /// The task runs with no tracker at all, so trackers given later through
    /// [`TrackerStatus::replace`] are announced to.
    pub fn start_with_tiers(setup: TrackerSetup) -> TrackerTask {
        let TrackerSetup {
            info_hashes,
            identity,
            tiers,
            stats,
            force_interval,
            listen_port,
            vote,
            protocols,
            stopped,
            counts,
        } = setup;
        let mut tiers = supported_tiers(tiers, &protocols);
        for trackers in &mut tiers {
            trackers.shuffle(&mut rand::rng());
        }
        tracing::trace!(?tiers);

        let status = TrackerStatus::new(tiers.iter().flatten(), protocols, counts.clone());

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);
        let (grow, mut grown) = tokio::sync::mpsc::unbounded_channel::<Arc<TrackerComms>>();

        let stats: Arc<dyn TorrentStatsProvider> = stats.into();
        let comms = info_hashes
            .ids20()
            .map(|info_hash| {
                // What a route move handed over under this hash goes on here.
                let started = identity.announcing.take_started(info_hash);
                Arc::new(Self {
                    info_hash,
                    identity: identity.clone(),
                    stats: stats.clone(),
                    force_tracker_interval: force_interval,
                    tx: tx.clone(),
                    announce_port: listen_port.clone(),
                    vote: vote.clone(),
                    list: Mutex::new(
                        TrackerList::new(
                            &tiers,
                            Instant::now(),
                            identity.shape,
                            identity.completion.clone(),
                        )
                        .going_on(&started),
                    ),
                    started_at: Mutex::new(started),
                    status: status.clone(),
                    poke: tokio::sync::Notify::new(),
                    stopped: stopped.clone(),
                    grow: grow.clone(),
                    counts: counts.clone(),
                    in_flight: Default::default(),
                    run: next_run(),
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
        let started = self.identity.announcing.take_started(info_hash);
        let comms = Arc::new(Self {
            info_hash,
            identity: self.identity.clone(),
            stats: self.stats.clone(),
            force_tracker_interval: self.force_tracker_interval,
            tx: self.tx.clone(),
            announce_port: self.announce_port.clone(),
            vote: self.vote.clone(),
            list: Mutex::new(
                TrackerList::new(
                    tiers,
                    Instant::now(),
                    self.identity.shape,
                    self.identity.completion.clone(),
                )
                .going_on(&started),
            ),
            started_at: Mutex::new(started),
            status: self.status.clone(),
            poke: tokio::sync::Notify::new(),
            stopped: self.stopped.clone(),
            grow: self.grow.clone(),
            counts: self.counts.clone(),
            in_flight: Default::default(),
            run: next_run(),
        });
        if self.grow.send(comms.clone()).is_err() {
            return;
        }
        self.status.0.runs.lock().push(Arc::downgrade(&comms));
    }

    /// Sends `stopped` to the trackers `stopping` owes it now, over the legs
    /// of each that accepted `started`, with the counters as they are once
    /// the run's connections closed, each recorded in the table as any
    /// announce is, within the stopped announces' bound; none when that
    /// bound is zero. The wait for the connections has its own bound, so a
    /// run slow to end still leaves the announces theirs whole. The run's
    /// announces `in_flight` finish meanwhile, within the same bound whole,
    /// and are abandoned past it or when it is zero. Should the session shut
    /// down first, they are dropped then, and each tracker of
    /// [`Stopping::waiting`] whose announce had not finished is sent
    /// `stopped`, within the same bound, beside the others. Each send reads
    /// the bound as it starts, so none goes once the announces are withheld,
    /// however late. `running` is held until they all end.
    fn send_stopped(
        self: &Arc<Self>,
        stopping: Stopping,
        mut in_flight: FuturesUnordered<InFlightAnnounce>,
        running: RunningGuard,
    ) {
        let Stopping {
            owed: trackers,
            mut waiting,
        } = stopping;
        let timeout = self.stopped.timeout();
        if timeout.is_zero() || (trackers.is_empty() && in_flight.is_empty()) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            debug!("no runtime to send the stopped announce on");
            return;
        };
        let comms = self.clone();
        let run_end = self.stopped.run_end;
        let whole = run_end + timeout;
        let ended = (!trackers.is_empty()).then(|| comms.stats.run_ended());
        let shutting_down = self.stopped.shutting_down.clone();
        let span = debug_span!(parent: None, "tracker_stopped", info_hash = ?comms.info_hash);
        runtime.spawn(
            async move {
                let _running = running;
                // Owns the announces in flight, so they are dropped, their
                // connections with them, as soon as the session shuts down.
                // Gives back the trackers of `waiting` they leave owed a
                // `stopped`: none unless the shutdown cut them.
                let answers = async move {
                    let all = async {
                        while let Some(answered) = in_flight.next().await {
                            if let Ok((tracker, _)) = answered {
                                waiting.retain(|(owed, _)| *owed != tracker);
                            }
                        }
                    };
                    tokio::select! {
                        biased;
                        () = shutting_down.cancelled() => {
                            debug!("the session shut down: the announces in flight are dropped");
                            drop(in_flight);
                            return waiting;
                        }
                        finished = tokio::time::timeout(whole, all) => {
                            if finished.is_err() {
                                debug!(
                                    ?whole,
                                    "an announce in flight at the stop did not finish in time"
                                );
                            }
                        }
                    }
                    Vec::new()
                };
                let stopped = async {
                    let Some(ended) = ended else {
                        return;
                    };
                    // The counters once the run's connections have counted
                    // what they dropped; within their bound, as they are if
                    // not.
                    let stats = match tokio::time::timeout(run_end, ended).await {
                        Ok(stats) => stats,
                        Err(_) => {
                            debug!(?run_end, "the run's connections did not close in time");
                            comms.stats.get()
                        }
                    };
                    comms.stopped_to(trackers, stats).await;
                };
                // The trackers the shutdown cut hear `stopped` as soon as it
                // does, beside the ones owed it already, so neither waits on
                // the other and both end within the bound the shutdown waits.
                let cut = async {
                    let cut = answers.await;
                    if !cut.is_empty() {
                        // The session's connections close with it; the
                        // counters go as they stand.
                        comms.stopped_to(cut, comms.stats.get()).await;
                    }
                };
                tokio::join!(stopped, cut);
            }
            .instrument(span),
        );
    }

    /// Sends `stopped` to each of `trackers` with `stats`, recorded, within
    /// the stopped announces' bound as it is now; none once they are
    /// withheld, which may have happened since the run stopped.
    async fn stopped_to(self: &Arc<Self>, trackers: StartedTrackers, stats: TrackerCommsStats) {
        let timeout = self.stopped.timeout();
        if timeout.is_zero() {
            debug!("the stopped announces are withheld: none is sent");
            return;
        }
        let announces = trackers.into_iter().map(|(tracker, started)| {
            self.announce_recorded(tracker, Events::stopped(started), stats)
        });
        if tokio::time::timeout(timeout, futures::future::join_all(announces))
            .await
            .is_err()
        {
            debug!(?timeout, "stopped announce timed out");
        }
    }

    /// Writes each tracker's failures and next announce to the table, `now`
    /// being `at` on the wall clock.
    fn publish(&self, now: Instant, at: SystemTime) {
        self.publish_answered(now, at, None);
    }

    /// [`Self::publish`] after the announce to `answered` ended, which the
    /// list holds at the index given: only its row is written when the
    /// others already show what the list holds.
    fn publish_answered(
        &self,
        now: Instant,
        at: SystemTime,
        answered: Option<(&SupportedTracker, usize)>,
    ) {
        let mut list = self.list.lock();
        self.status.0.announces.send_if_modified(|announces| {
            let mut index = self.status.0.rows.lock();
            write_answered(
                announces, &mut index, self.run, &mut list, answered, now, at,
            )
        });
    }

    /// Announces under this hash until dropped: whatever the tracker list
    /// says is due, then waits for an answer, for the next tracker to come
    /// due, or for the torrent to become a seed.
    async fn run(self: &Arc<Self>) -> anyhow::Result<()> {
        // The torrent became a seed during this run and the list was told
        // (libtorrent `torrent::completed`, once per transition).
        let mut completion_seen = false;
        // The announces carry the port as they go out; a tracker that heard
        // another is announced to again (see `Entry::port`).
        let mut port = self.announce_port.clone();
        // Ends the announce to a tracker that has been removed.
        let mut aborts: HashMap<SupportedTracker, futures::future::AbortHandle> = HashMap::new();
        loop {
            let listed: HashSet<SupportedTracker> = if aborts.is_empty() {
                HashSet::new()
            } else {
                self.list
                    .lock()
                    .entries
                    .iter()
                    .map(|e| e.tracker.clone())
                    .collect()
            };
            aborts.retain(|tracker, abort| {
                let kept = listed.contains(tracker);
                if !kept {
                    abort.abort();
                }
                kept && !abort.is_aborted()
            });
            let stats = self.stats.get();
            let current = *port.borrow_and_update();
            let due = self.list.lock().start_due(Instant::now(), current, &stats);
            for (tracker, events) in due {
                // Owned, so that it may outlive the run (see `StopOnDrop`).
                let comms = self.clone();
                let announced = tracker.clone();
                let (announce, abort) = futures::future::abortable(
                    async move { comms.announce_recorded(announced, events, stats).await }
                        .in_current_span(),
                );
                aborts.insert(tracker, abort);
                self.in_flight.lock().push(announce.boxed());
            }
            let answered = futures::future::poll_fn(|cx| self.in_flight.lock().poll_next_unpin(cx));
            let next_due = self.list.lock().next_due();
            let next_due = async {
                match next_due {
                    Some(at) => tokio::time::sleep_until(at).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(answer) = answered => {
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
        events: Events,
        stats: TrackerCommsStats,
    ) -> (SupportedTracker, Vec<SocketAddr>) {
        let in_flight = self.status.begin(&tracker);
        let result = self.announce(&tracker, events, &stats).await;
        let (now, at) = (Instant::now(), SystemTime::now());
        let (result, peers, at_list) = match result {
            Ok(announced) => {
                let delay = ReannounceDelay::new(
                    announced.interval,
                    announced.min_interval,
                    self.force_tracker_interval,
                );
                debug!(
                    ?tracker,
                    ?events,
                    accepted = ?announced.accepted,
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
                let at_list =
                    self.list
                        .lock()
                        .on_success(&tracker, announced.accepted, now, &delay);
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
                (Ok(ok), announced.peers, at_list)
            }
            Err(e) => {
                debug!(?tracker, "error calling tracker: {e:#}");
                let retry_interval = Refused::retry_interval(&e);
                let at_list = self.list.lock().on_failure(&tracker, now, retry_interval);
                (Err(AnnounceError::from(&e)), Vec::new(), at_list)
            }
        };
        self.publish_answered(now, at, at_list.map(|i| (&tracker, i)));
        in_flight.finish(LastAnnounce { at, result });
        (tracker, peers)
    }

    async fn announce(
        &self,
        tracker: &SupportedTracker,
        events: Events,
        stats: &TrackerCommsStats,
    ) -> anyhow::Result<Announced> {
        let protocol = &*tracker.protocol;
        // An announce that waits for a turn waits first, outside its time
        // limit.
        let _turn = futures::future::OptionFuture::from(protocol.gate(events.any())).await;
        let asked = Instant::now();
        let result = within_completion_timeout(protocol.announce(AnnounceAsk {
            tracker,
            info_hash: self.info_hash,
            identity: &self.identity,
            events,
            stats,
            ports: &self.announce_port,
            started_at: &self.started_at,
        }))
        .await;
        self.counts.announced(
            protocol.kind(),
            events.counted_as(),
            counted_result(&result, protocol),
            asked.elapsed(),
            result.as_ref().map_or(0, |announced| announced.peers.len()),
        );
        result.map_err(without_url)
    }
}

/// HTTP(S) trackers (BEP 3).
struct HttpTracker {
    clients: Arc<dyn HttpClients>,
    /// The session's UDP tracker client, whose resolver looks up an HTTP
    /// tracker's name too: one path for every tracker name.
    names: UdpTrackerClient,
    reachable: Reachable,
    /// The turns its announces wait for, shared across the session.
    announces: HttpAnnounces,
    /// Whether a tracker whose host has an internationalized label may be
    /// asked.
    allow_idna: bool,
}

impl TrackerProtocol for HttpTracker {
    fn kind(&self) -> TrackerKind {
        TrackerKind::Http
    }

    fn gate(&self, event: bool) -> Option<BoxFuture<'_, Turn>> {
        Some(async move { Box::new(self.announces.take(event).await) as Turn }.boxed())
    }

    fn announce<'a>(&'a self, ask: AnnounceAsk<'a>) -> BoxFuture<'a, anyhow::Result<Announced>> {
        let span = trace_span!("http_tracker", tracker = ?ask.tracker);
        self.announce_http(ask).instrument(span).boxed()
    }
}

impl HttpTracker {
    fn new(net: &TrackerNet) -> Self {
        Self {
            clients: net.http_clients.clone(),
            names: net.udp_client.clone(),
            reachable: net.reachable.clone(),
            announces: net.http_announces.clone(),
            allow_idna: net.allow_idna,
        }
    }

    async fn announce_http(&self, ask: AnnounceAsk<'_>) -> anyhow::Result<Announced> {
        let AnnounceAsk {
            tracker,
            identity,
            events,
            stats,
            ..
        } = ask;
        let tracker_url = &tracker.url;
        let trackerid = identity.tracker_ids.get(tracker_url);
        let routes = self.http_routes(tracker_url).await;
        // libtorrent lists a private torrent's own public addresses, those
        // its announces leave from; through a proxy it knows none.
        let own_addrs: Vec<IpAddr> = routes
            .iter()
            .filter_map(|(_, from)| *from)
            .filter(|ip| stats.private && is_public(*ip))
            .collect();
        // Each route is a leg, with the event of its own; one the events
        // pass over is not announced over.
        let legs: Vec<(Option<Family>, Leg, Option<TrackerRequestEvent>)> = routes
            .iter()
            .filter_map(|(family, _)| {
                let leg = Leg::of_route(*family);
                Some((*family, leg, events.over(leg)?))
            })
            .collect();
        // The port, event and counts since `started` are set for each leg
        // below.
        let mut request = tracker_comms_http::TrackerRequest {
            info_hash: &ask.info_hash,
            peer_id: &identity.peer_id,
            port: 0,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            corrupt: 0,
            redundant: 0,
            compact: true,
            no_peer_id: false,
            event: None,
            numwant: 0,
            key: Some(identity.tracker_key),
            trackerid: trackerid.as_deref(),
            encryption: stats.encryption,
            own_addrs: &own_addrs,
        };
        // Each route's announce carries the port of its family, as each of
        // libtorrent's listen sockets announces its own; one held to no
        // family carries the IPv4 one, libtorrent's first listen socket's.
        let ports = *ask.ports.borrow();
        let mut queries = Vec::with_capacity(legs.len());
        for (family, leg, event) in &legs {
            request.port = announced_port(ports.of(*family == Some(Family::V6)));
            request.event = *event;
            request.numwant = numwant(*event);
            (request.corrupt, request.redundant) = ask.since_started(*leg, *event);
            queries.push(request.as_querystring(identity.shape));
        }

        // One announce per leg, at once; the peers of every answer are
        // kept, the rest of the first, as UDP's two families are.
        let answers =
            futures::future::join_all(legs.iter().zip(&queries).map(|((family, _, _), query)| {
                tracker_comms_http::get(
                    &*self.clients,
                    self.names.resolver(),
                    &self.reachable,
                    tracker_url,
                    tracker.named_by,
                    *family,
                    query,
                    &identity.user_agent,
                    identity.shape,
                    self.allow_idna,
                )
            }))
            .await;
        let answers = legs.iter().zip(answers).map(|((_, leg, event), answer)| {
            let answer =
                answer.and_then(|answer| read_http_announce(identity, tracker_url, &answer));
            (*leg, *event, answer)
        });
        merged(answers)
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
        // A name the GET refuses is not looked up here either.
        if !self.allow_idna && url.host_str().is_some_and(is_idna) {
            return one;
        }
        let Ok((_, found)) = tracker_comms_http::addresses(url, self.names.resolver()).await else {
            return one;
        };
        let port = url.port_or_known_default().unwrap_or(0);
        let mut routes = Vec::new();
        for family in [Family::V4, Family::V6] {
            let Some(ip) = found.iter().find(|ip| Family::of(**ip) == family) else {
                continue;
            };
            match self.clients.egress(SocketAddr::new(*ip, port)) {
                Egress::Proxied => return one,
                Egress::NoRoute => {}
                Egress::From(from) => routes.push((Some(family), Some(from))),
            }
        }
        if routes.is_empty() { one } else { routes }
    }
}

/// An HTTP tracker's answer to an announce, read; the tracker id it gives
/// is kept in `identity`.
fn read_http_announce(
    identity: &Identity,
    tracker_url: &Url,
    answer: &tracker_comms_http::Answer,
) -> anyhow::Result<Announced> {
    let response = read_answer(&answer.body)?;
    if let Some(id) = &response.tracker_id {
        identity.tracker_ids.set(tracker_url, id.as_ref());
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
            .map(|m| redacted_tracker_text(&String::from_utf8_lossy(m.as_ref()))),
        seen_as: response.external_ip().zip(answer.from),
        body_len: Some(answer.body.len()),
        accepted: Accepted::default(),
    })
}

/// UDP trackers (BEP 15).
struct UdpTracker {
    client: UdpTrackerClient,
    reachable: Reachable,
}

impl TrackerProtocol for UdpTracker {
    fn kind(&self) -> TrackerKind {
        TrackerKind::Udp
    }

    fn announce<'a>(&'a self, ask: AnnounceAsk<'a>) -> BoxFuture<'a, anyhow::Result<Announced>> {
        let span = trace_span!("udp_tracker", tracker = ?ask.tracker);
        self.announce_udp(ask).instrument(span).boxed()
    }

    fn classify(&self, cause: &(dyn std::error::Error + 'static)) -> Option<TrackerResult> {
        match cause.downcast_ref::<tracker_comms_udp::NoAnswer>()? {
            tracker_comms_udp::NoAnswer::Errored(_) => Some(TrackerResult::Refused),
            tracker_comms_udp::NoAnswer::Unparsed => Some(TrackerResult::BadReply),
        }
    }
}

impl UdpTracker {
    fn new(net: &TrackerNet) -> Self {
        Self {
            client: net.udp_client.clone(),
            reachable: net.reachable.clone(),
        }
    }

    async fn announce_udp(&self, ask: AnnounceAsk<'_>) -> anyhow::Result<Announced> {
        let (tracker, events) = (ask.tracker, ask.events);
        let url = &tracker.url;
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );
        let addrs = udp_tracker_to_socket_addrs(
            host.clone(),
            port,
            self.client.resolver(),
            &self.reachable,
            tracker.named_by,
        )
        .instrument(trace_span!("resolve", ?host))
        .await?;

        let addrs: Vec<SocketAddr> = match addrs {
            UdpTrackerResolveResult::One(addr) => vec![addr],
            UdpTrackerResolveResult::Two(v4, v6) => vec![v4.into(), v6.into()],
        };
        // Each address is a leg, with the event of its own, all at once;
        // one the events pass over is not announced over.
        let legs: Vec<(SocketAddr, Leg, Option<TrackerRequestEvent>)> = addrs
            .into_iter()
            .filter_map(|addr| {
                let leg = Leg::of_addr(addr);
                Some((addr, leg, events.over(leg)?))
            })
            .collect();
        let answers = futures::future::join_all(legs.iter().map(|(addr, _, event)| {
            self.announce_udp_addr(&ask, *addr, *event)
                .instrument(trace_span!("udp request", ?addr))
        }))
        .await;
        merged(
            legs.iter()
                .zip(answers)
                .map(|((_, leg, event), answer)| (*leg, *event, answer)),
        )
    }

    async fn announce_udp_addr(
        &self,
        ask: &AnnounceAsk<'_>,
        addr: SocketAddr,
        event: Option<TrackerRequestEvent>,
    ) -> anyhow::Result<Announced> {
        use tracker_comms_udp::*;

        let (identity, stats) = (ask.identity, ask.stats);
        let request = AnnounceFields {
            info_hash: ask.info_hash,
            peer_id: identity.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.announced_left(),
            uploaded: stats.uploaded_bytes,
            event: udp_event(event),
            key: identity.tracker_key,
            num_want: numwant(event),
            port: announced_port(ask.ports.borrow().to(addr.ip())),
            url_data: url_data(&ask.tracker.url),
        };

        match self.client.announce(addr, request).await {
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
                    accepted: Accepted::default(),
                })
            }
            Err(e) => {
                debug!(?addr, "error reading announce response: {e:#}");
                Err(e)
            }
        }
    }
}

/// Writes what `list` holds to the table after the announce to `answered`
/// ended, for the run `run`: that tracker's row alone when every other row
/// already shows what the list holds (the run wrote every row last, and the
/// list changed since only by answers written), else every row, as
/// [`write_rows`] does. `answered` is passed over unless the list still
/// holds it at the index given. Says whether a row changed.
fn write_answered(
    rows: &mut [TrackerAnnounce],
    index: &mut RowIndex,
    run: u64,
    list: &mut TrackerList,
    answered: Option<(&SupportedTracker, usize)>,
    now: Instant,
    at: SystemTime,
) -> bool {
    let one = answered
        .filter(|&(tracker, i)| list.entries.get(i).is_some_and(|e| &e.tracker == tracker))
        .filter(|_| index.writer == run && !list.unpublished);
    if let Some((tracker, i)) = one {
        let Some(&row) = index.at.get(tracker.url().as_str()) else {
            return false;
        };
        let value = list.row_value(&list.entries[i], now, at);
        let row = &mut rows[row];
        if (row.fails, row.next_announce) == value {
            return false;
        }
        (row.fails, row.next_announce) = value;
        return true;
    }
    index.writer = run;
    list.unpublished = false;
    let list = &*list;
    write_rows(
        rows,
        &list.entries,
        |entry| entry.tracker.url().as_str(),
        |entry| list.row_value(entry, now, at),
    )
}

/// Writes each entry's failures and next announce to the first row of its
/// URL, in the entries' order; an entry with no row is passed over. Says
/// whether a row changed.
fn write_rows<'a, E>(
    rows: &mut [TrackerAnnounce],
    entries: &'a [E],
    url: impl Fn(&'a E) -> &'a str,
    value: impl Fn(&'a E) -> (u8, Option<SystemTime>),
) -> bool {
    // The first row of each entry's URL, found in one pass over the rows.
    let mut first: HashMap<&str, Option<usize>> = entries.iter().map(|e| (url(e), None)).collect();
    for (i, row) in rows.iter().enumerate() {
        if let Some(at @ None) = first.get_mut(row.url.as_str()) {
            *at = Some(i);
        }
    }
    let mut modified = false;
    for entry in entries {
        let Some(&Some(i)) = first.get(url(entry)) else {
            continue;
        };
        let row = &mut rows[i];
        let value = value(entry);
        if (row.fails, row.next_announce) != value {
            (row.fails, row.next_announce) = value;
            modified = true;
        }
    }
    modified
}

#[cfg(test)]
mod reach_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod upkeep_tests;
