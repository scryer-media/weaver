// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::add::merge_two_optional_streams;
use super::options::AddTorrentOptions;
use std::{
    collections::HashSet,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use crate::Identity;
use crate::core::{hash_id::InfoHashes, torrent_metainfo::TorrentMetaV1Owned};
use crate::ssl::{SslHttpClients, SslTorrent};
use crate::tracker::{
    Reachable, ToldPorts, Tracker, TrackerAnnounce, TrackerComms, TrackerStatus, TrackerTask,
    Unreachable,
};
use crate::{
    ManagedTorrent,
    torrent_state::{ManagedTorrentHandle, TorrentTotals},
    type_aliases::{PeerSource, PeerSourceFlag, PeerStream},
};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use tokio::sync::Notify;
use tracing::{trace, warn};

use super::{PeerRequest, PeerRxTorrentInfo, Session, dht_peers};

fn parse_tracker_url(tracker: &[u8]) -> Option<url::Url> {
    match std::str::from_utf8(tracker) {
        Ok(url) => url::Url::parse(url).ok(),
        Err(_) => {
            warn!("cannot parse tracker url as utf-8, ignoring");
            None
        }
    }
}

/// A torrent's own tracker tiers (BEP 12), which came with it:
/// `announce-list` when it names any tracker, otherwise `announce` as the
/// only tier.
pub(super) fn metainfo_tracker_tiers(meta: &TorrentMetaV1Owned) -> Vec<Vec<Tracker>> {
    if meta.announce_list.iter().flatten().next().is_some() {
        meta.announce_list
            .iter()
            .map(|tier| {
                tier.iter()
                    .filter_map(|t| parse_tracker_url(t.as_ref()))
                    .map(Tracker::torrent)
                    .collect()
            })
            .collect()
    } else {
        meta.announce
            .iter()
            .filter_map(|t| parse_tracker_url(t.as_ref()))
            .map(|t| vec![Tracker::torrent(t)])
            .collect()
    }
}

/// The tiers the caller gave in place of the torrent's own.
pub(super) fn own_tracker_tiers(opts: &AddTorrentOptions) -> Option<Vec<Vec<Tracker>>> {
    opts.tracker_tiers.clone()
}

/// Whether a torrent takes peers from its trackers alone: its metainfo
/// marks it `private` (BEP 27), or it was added
/// [`trackers_only`](AddTorrentOptions::trackers_only). Such a torrent
/// neither looks for peers on the DHT, the local network or by peer
/// exchange, nor announces to any tracker but its own.
pub(crate) const fn trackers_only(private: bool, added_trackers_only: bool) -> bool {
    private || added_trackers_only
}

/// Where a torrent looks for peers beside its trackers, as each of
/// libtorrent's per-torrent flags leaves it: the DHT unless
/// `torrent_flags::disable_dht` (`torrent::should_announce_dht`), the local
/// network unless `disable_lsd` (`torrent::lsd_announce`). A torrent that
/// takes peers from its trackers alone looks at neither, whatever its
/// flags say: a private torrent is never announced on either
/// (`should_announce_dht` and `lsd_announce` both return on `priv()`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Discovery {
    pub dht: bool,
    pub lsd: bool,
}

impl Discovery {
    pub(crate) const fn of(trackers_only: bool, disable_dht: bool, disable_lsd: bool) -> Self {
        Self {
            dht: !trackers_only && !disable_dht,
            lsd: !trackers_only && !disable_lsd,
        }
    }

    /// Under `opts`, for a torrent `private` or not.
    pub(crate) const fn added(private: bool, opts: &AddTorrentOptions) -> Self {
        Self::of(
            trackers_only(private, opts.trackers_only),
            opts.disable_dht,
            opts.disable_lsd,
        )
    }
}

/// Whether a torrent speaks peer exchange: the session runs it, the torrent
/// was not added with `disable_pex`, and it takes peers from more than its
/// trackers. libtorrent's `create_ut_pex_plugin` adds no plugin to a
/// private torrent, and `ut_pex_plugin::tick` and
/// `ut_pex_peer_plugin::on_extended` return while `disable_pex` is set.
pub(crate) const fn speaks_pex(session_pex: bool, trackers_only: bool, disable_pex: bool) -> bool {
    session_pex && !trackers_only && !disable_pex
}

/// Where a magnet's metadata fetch asks for peers. Until its metadata says
/// whether it is private, it asks its own trackers alone, and goes on to
/// the DHT and local discovery only once they named no peer that may still
/// serve it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchPhase {
    /// Its own trackers, until one answers or every one fails, and then
    /// until every peer they named was found unable to serve the metadata.
    Trackers,
    /// Its trackers, the DHT and local discovery: no tracker named a peer
    /// that may still serve the metadata.
    Discovery,
    /// No tracker named a peer that may still serve the metadata, and there
    /// is no discovery to go on to.
    Waiting,
}

impl FetchPhase {
    /// The phase of a fetch whose trackers stand at `table`, `unable` being
    /// the peers the fetch found unable to serve the metadata, with
    /// discovery to go on to or not. The trackers are done once one
    /// answered or every one failed, and settled once every peer an answer
    /// named is unable; a fetch with no tracker is done at once.
    pub(super) fn of(
        table: &[TrackerAnnounce],
        unable: &HashSet<SocketAddr>,
        discovery: bool,
    ) -> Self {
        let results = || table.iter().map(|t| t.last.as_ref().map(|l| &l.result));
        let may_serve = results()
            .filter_map(|r| r?.as_ref().ok())
            .flat_map(|ok| &ok.peers)
            .any(|peer| !unable.contains(peer));
        let done = results().any(|r| matches!(r, Some(Ok(_))))
            || results().all(|r| matches!(r, Some(Err(_))));
        match (done && !may_serve, discovery) {
            (false, _) => Self::Trackers,
            (true, true) => Self::Discovery,
            (true, false) => Self::Waiting,
        }
    }
}

/// Whether a tracker of `table` stands answered: its last announce was
/// accepted, whatever peers it named.
pub(crate) fn a_tracker_answered(table: &[TrackerAnnounce]) -> bool {
    table
        .iter()
        .any(|t| matches!(t.last.as_ref().map(|l| &l.result), Some(Ok(_))))
}

impl std::fmt::Display for FetchPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trackers => "trackers",
            Self::Discovery => "trackers, DHT, LSD",
            Self::Waiting => "no peers from trackers",
        })
    }
}

/// A magnet's metadata fetch, while it runs.
#[derive(Clone, Debug)]
pub struct MetadataFetch {
    /// The announce table it announces from, unless trackers are off.
    pub trackers: Option<TrackerStatus>,
    /// Where it asks for peers now.
    pub phase: tokio::sync::watch::Receiver<FetchPhase>,
}

/// Follows a fetch's announce table, and the peers it found unable, into
/// `phase`: returns once the fetch goes on to discovery, and never while it
/// does not.
pub(super) async fn follow_fetch(
    mut table: Option<tokio::sync::watch::Receiver<Vec<TrackerAnnounce>>>,
    mut unable: tokio::sync::watch::Receiver<HashSet<SocketAddr>>,
    discovery: bool,
    phase: tokio::sync::watch::Sender<FetchPhase>,
    changed: Option<Arc<Notify>>,
) {
    loop {
        let now = {
            let unable = unable.borrow_and_update();
            match &mut table {
                Some(table) => FetchPhase::of(&table.borrow_and_update(), &unable, discovery),
                None => FetchPhase::of(&[], &unable, discovery),
            }
        };
        if phase.send_if_modified(|was| std::mem::replace(was, now) != now)
            && let Some(changed) = &changed
        {
            changed.notify_one();
        }
        if now == FetchPhase::Discovery {
            return;
        }
        let table_moved = async {
            match &mut table {
                Some(table) => table.changed().await.map_err(drop),
                None => Err(()),
            }
        };
        tokio::select! {
            Ok(()) = table_moved => {}
            Ok(()) = unable.changed() => {}
            else => std::future::pending::<()>().await,
        }
    }
}

/// The tiers a torrent announces to: its own tiers in order, then each of
/// `extra`, which the operator named, as a tier of its own, unless the
/// torrent takes peers from its own trackers only. A tracker listed more
/// than once keeps its first place, and empty tiers are dropped.
pub(super) fn merge_tracker_tiers(
    own: Vec<Vec<Tracker>>,
    extra: impl IntoIterator<Item = url::Url>,
    trackers_only: bool,
) -> Vec<Vec<Tracker>> {
    let extra = (!trackers_only).then(|| extra.into_iter().map(|t| vec![Tracker::operator(t)]));
    crate::tracker::tidy_tiers(own.into_iter().chain(extra.into_iter().flatten()))
}

impl Session {
    pub fn make_peer_rx_managed_torrent(
        self: &Arc<Self>,
        t: &Arc<ManagedTorrent>,
        announce: bool,
    ) -> Option<PeerStream> {
        self.peer_source_managed_torrent(t, announce)
            .map(|s| s.peers)
    }

    pub(crate) fn peer_source_managed_torrent(
        self: &Arc<Self>,
        t: &ManagedTorrentHandle,
        announce: bool,
    ) -> Option<PeerSource> {
        self.make_peer_rx(PeerRequest {
            info_hashes: t.info_hashes(),
            identity: &t.shared().identity(),
            tracker_tiers: t.shared().tracker_tiers(),
            announce,
            force_tracker_interval: t.shared().options.force_tracker_interval,
            initial_peers: t.shared().options.initial_peers.clone(),
            trackers_only: t.takes_peers_from_trackers_only(),
            discovery: t.discovery(),
            dht_announce: t.shared().dht_announce.subscribe(),
            ssl: t.shared().ssl.as_ref(),
            trackers: None,
        })
    }

    /// The tiers a torrent announces to: its own, then the session's trackers
    /// unless it takes peers from its own trackers only.
    pub(crate) fn announce_tiers(
        &self,
        tracker_tiers: Vec<Vec<Tracker>>,
        trackers_only: bool,
    ) -> Vec<Vec<Tracker>> {
        if self.disable_trackers {
            return Vec::new();
        }
        merge_tracker_tiers(tracker_tiers, self.trackers.iter().cloned(), trackers_only)
    }

    /// Whether the session may reach `ip`, whoever is there: a peer, a web
    /// seed, or a tracker at any hop. It must be of a family the session
    /// connects over, as an IPv4-only session reaches no IPv6 address
    /// (libtorrent `listen_socket_t::can_route`, which
    /// `http_tracker_connection::on_filter` and
    /// `udp_tracker_connection::name_lookup` apply first), and the IP filter,
    /// the blocklist and the allowlist, must let it through (libtorrent's one
    /// `ip_filter`: `torrent::add_peer` applies it to a peer,
    /// `torrent::connect_web_seed` to a web seed, and the two tracker
    /// functions to a tracker while [`IpFilter::trackers`] holds).
    pub(crate) fn may_reach(&self, ip: IpAddr) -> Result<(), Unreachable> {
        self.may_reach_under(ip, true)
    }

    // `may_reach`, the IP filter applied only when `filtered`.
    fn may_reach_under(&self, ip: IpAddr, filtered: bool) -> Result<(), Unreachable> {
        let ip = crate::network::canonical_ip_addr(ip);
        if self.ipv4_only && ip.is_ipv6() {
            return Err(Unreachable::Family);
        }
        if filtered
            && (self.ip_filter.read().blocked.has(ip)
                || self.allowlist.as_ref().is_some_and(|l| !l.has(ip)))
        {
            return Err(Unreachable::Filtered);
        }
        Ok(())
    }

    /// The addresses the tracker task may ask a tracker at: those of a
    /// family the session connects over, and while the filter applies to
    /// trackers those it lets through (libtorrent
    /// `torrent::build_tracker_request` and `scrape_tracker_impl` hand the
    /// request the filter only when `apply_ip_filter_to_trackers` is set),
    /// read at each lookup.
    // Weak, as the tracker task outlives no session: holding it strongly
    // would keep a session whose torrents announce nowhere alive.
    pub(super) fn reachable(self: &Arc<Self>) -> Reachable {
        let session = Arc::downgrade(self);
        Arc::new(move |ip| {
            session.upgrade().map_or(Ok(()), |s| {
                let filtered = s.ip_filter.read().trackers;
                s.may_reach_under(ip, filtered)
            })
        })
    }

    // A torrent's tracker task, under every hash it goes by. The task runs
    // with no tracker too, so trackers added later are announced to; a
    // session without trackers starts none.
    pub(super) fn start_trackers(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        identity: &Identity,
        tracker_tiers: Vec<Vec<Tracker>>,
        force_tracker_interval: Option<Duration>,
        trackers_only: bool,
        ssl: Option<&Arc<SslTorrent>>,
    ) -> Option<TrackerTask> {
        if self.disable_trackers {
            return None;
        }
        let stats = PeerRxTorrentInfo {
            info_hashes,
            session: Arc::downgrade(self),
            torrent: Default::default(),
        };
        let task = TrackerComms::start_with_tiers(
            info_hashes,
            identity.clone(),
            self.announce_tiers(tracker_tiers, trackers_only),
            Box::new(stats),
            force_tracker_interval,
            match ssl {
                Some(_) => self.ssl_port.subscribe(),
                None => self.announce_port.subscribe(),
            },
            self.external_ips.tracker_vote(),
            match ssl {
                Some(ssl) => Arc::new(SslHttpClients {
                    inner: self.http_clients.clone(),
                    ssl: ssl.clone(),
                }),
                None => self.http_clients.clone(),
            },
            self.reachable(),
            self.udp_tracker_client.clone(),
            self.stopped_announces.clone(),
            self.http_announces.clone(),
            self.allow_idna,
            self.stats.trackers.clone(),
        );
        // The table rings the session's watcher at each change, until the
        // table is gone.
        if let Some(changed) = &self.trackers_changed {
            let mut table = task.status.subscribe();
            let changed = Arc::clone(changed);
            tokio::spawn(async move {
                while table.changed().await.is_ok() {
                    changed.notify_one();
                }
            });
        }
        Some(task)
    }

    // Get a peer stream from both DHT and trackers, under every hash the
    // torrent goes by (libtorrent `torrent::dht_announce`, `lsd_announce`
    // and `announce_with_tracker` each loop over `info_hash_t::for_each`).
    pub(super) fn make_peer_rx(self: &Arc<Self>, request: PeerRequest<'_>) -> Option<PeerSource> {
        let PeerRequest {
            info_hashes,
            identity,
            tracker_tiers,
            announce,
            force_tracker_interval,
            initial_peers,
            trackers_only,
            discovery,
            dht_announce,
            ssl,
            trackers,
        } = request;
        // An SSL torrent with no identity asks no one for peers.
        if ssl.is_some_and(|ssl| !ssl.has_identity()) {
            return None;
        }
        let discovery_rx =
            self.discovery_peers(info_hashes, announce, dht_announce, ssl, discovery);

        let (tracker_rx, trackers) = trackers
            .or_else(|| {
                self.start_trackers(
                    info_hashes,
                    identity,
                    tracker_tiers,
                    force_tracker_interval,
                    trackers_only,
                    ssl,
                )
            })
            .map(|task| (task.peers, task.status))
            .unzip();

        let initial_peers_rx = if initial_peers.is_empty() {
            None
        } else {
            Some(futures::stream::iter(initial_peers))
        };
        // Each peer goes with who named it (libtorrent `torrent::add_peer`'s
        // `peer_source_flags_t`; a torrent's own peers are `resume_data`).
        let named = |source| move |addr| (addr, source);
        merge_two_optional_streams(
            merge_two_optional_streams(
                tracker_rx.map(|s| s.map(named(PeerSourceFlag::Tracker))),
                initial_peers_rx.map(|s| s.map(named(PeerSourceFlag::ResumeData))),
            ),
            discovery_rx,
        )
        .map(|peers| PeerSource { peers, trackers })
    }

    // The peers the DHT and local discovery name, under every hash the
    // torrent goes by, each only where `discovery` looks; None when it
    // looks at neither the session runs.
    pub(super) fn discovery_peers(
        self: &Arc<Self>,
        info_hashes: InfoHashes,
        announce: bool,
        dht_announce: tokio::sync::watch::Receiver<()>,
        ssl: Option<&Arc<SslTorrent>>,
        discovery: Discovery,
    ) -> Option<PeerStream> {
        // The port the torrent's peers are told to reach it at: an SSL
        // torrent's is the SSL listener's.
        let port = || match ssl {
            Some(_) => self.ssl_port.subscribe(),
            None => self.announce_port.subscribe(),
        };
        let dht_rx = self.dht.as_ref().filter(|_| discovery.dht).map(|dht| {
            futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                dht_peers(
                    dht.clone(),
                    info_hash,
                    if announce {
                        port()
                    } else {
                        tokio::sync::watch::channel(ToldPorts::default()).1
                    },
                    dht_announce.clone(),
                )
            }))
        });

        // Each source starts when the torrent first asks it for peers, which
        // is once it is checked and running: trackers and the DHT by their
        // streams, the LSD by registering here (libtorrent
        // `torrent::start_announcing` runs only after `files_checked`, never
        // while paused).
        let lsd_rx = self.lsd.as_ref().filter(|_| discovery.lsd).map(|lsd| {
            // Local peers are told the port the torrent's listener is
            // reached at: the one a mapper got on the gateway, else the
            // listen port, whatever announce port is set, read at each
            // announce (libtorrent `torrent::lsd_announce`,
            // torrent.cpp:2980-2983, takes `session_impl::listen_port` or
            // `ssl_listen_port`, which give the listen socket's
            // `tcp_external_port`, session_impl.cpp:5776-5805).
            let port = announce.then(|| {
                let port = match ssl {
                    Some(_) => self.ssl_port.clone(),
                    None => self.announce_port.clone(),
                };
                let port: crate::lsd::AnnouncedPort = Arc::new(move || port.external(false));
                port
            });
            futures::stream::select_all(info_hashes.ids20().map(|info_hash| {
                let lsd = lsd.clone();
                let port = port.clone();
                futures::stream::once(async move { lsd.announce(info_hash, port) })
                    .flatten()
                    .boxed()
            }))
        });

        let named = |source| move |addr| (addr, source);
        merge_two_optional_streams(
            dht_rx.map(|s| s.map(named(PeerSourceFlag::Dht))),
            lsd_rx.map(|s| s.map(named(PeerSourceFlag::Lsd))),
        )
    }
}

impl PeerRxTorrentInfo {
    fn torrent(&self) -> Option<&ManagedTorrentHandle> {
        if let Some(t) = self.torrent.get() {
            return Some(t);
        }
        let found = self.session.upgrade()?.with_torrents(|torrents| {
            torrents
                .map(|(_, t)| t)
                .find(|t| t.info_hashes().overlaps(&self.info_hashes))
                .cloned()
        })?;
        Some(self.torrent.get_or_init(|| found))
    }
}

impl crate::tracker::TorrentStatsProvider for PeerRxTorrentInfo {
    fn get(&self) -> crate::tracker::TrackerCommsStats {
        let mt = match self.torrent() {
            Some(mt) => mt,
            None => {
                trace!(info_hashes=?self.info_hashes, "can't find torrent in the session, using default stats");
                return Default::default();
            }
        };
        let from = *mt.shared().announced_from.lock();
        tracker_stats(&self.session, mt, &from)
    }

    fn run_ended(&self) -> BoxFuture<'static, crate::tracker::TrackerCommsStats> {
        let torrent = self.torrent().cloned();
        let session = self.session.clone();
        // The run that ended counts from where it began, whatever run
        // begins before its connections have closed.
        let from = torrent.as_ref().map(|t| *t.shared().announced_from.lock());
        let ending = torrent
            .as_ref()
            .and_then(|t| t.shared().ending_run.lock().clone());
        async move {
            let (Some(torrent), Some(from)) = (torrent, from) else {
                return Default::default();
            };
            if let Some(tasks) = ending {
                tasks.wait().await;
                // The run's tasks have all ended: the torrent forgets them,
                // unless a later run's end has named its own since.
                let mut ending = torrent.shared().ending_run.lock();
                if ending
                    .as_ref()
                    .is_some_and(|named| tokio_util::task::TaskTracker::ptr_eq(named, &tasks))
                {
                    *ending = None;
                }
            }
            tracker_stats(&session, &torrent, &from)
        }
        .boxed()
    }

    fn wait_until_completed(&self) -> BoxFuture<'_, ()> {
        async move {
            // A magnet still receiving its metadata isn't in the session
            // yet: it is looked for again as each torrent is added
            // (libtorrent waits on `metadata_received_alert`).
            let Some(added) = self.session.upgrade().map(|s| s.db.read().added.clone()) else {
                return futures::future::pending().await;
            };
            loop {
                let mut next_added = std::pin::pin!(added.notified());
                next_added.as_mut().enable();
                if let Some(mt) = self.torrent() {
                    // The torrent finishes its selection; it completes only if
                    // that makes it a seed. A selection that finishes short of
                    // a seed keeps the wait, so a later wider selection that
                    // makes it one still announces at once. An error leaves
                    // nothing to wait on in this run: a later regular
                    // announce reads the state.
                    if mt.wait_until_seed().await.is_err() {
                        futures::future::pending::<()>().await;
                    }
                    return;
                }
                next_added.await;
            }
        }
        .boxed()
    }
}

/// What `mt`'s announces report, counted from `from`: what it had moved when
/// its run began announcing.
fn tracker_stats(
    session: &std::sync::Weak<Session>,
    mt: &ManagedTorrentHandle,
    from: &TorrentTotals,
) -> crate::tracker::TrackerCommsStats {
    let live = mt.live();

    use crate::torrent_state::stats::TorrentStatsState as TS;
    use crate::tracker::TrackerCommsStatsState as S;

    let torrent_state = match mt.stats().state {
        TS::Initializing { .. } => S::Initializing,
        TS::Live => S::Live,
        TS::Paused | TS::NeedsCertificate => S::Paused,
        TS::Error => S::None,
    };
    // What the torrent has moved since its run began announcing, the
    // running run's bytes included.
    let last_run = *mt.shared().last_run.lock();
    let totals = mt.totals().since(from);
    // What the whole torrent still lacks, whatever is selected, so a
    // torrent that has its selected files but not every piece never reads
    // as a seed (libtorrent `torrent::bytes_left`). Once the pieces are
    // gone with the torrent, what its run recorded as it ended.
    let left_bytes = mt
        .with_chunk_tracker(|c| c.bytes_left())
        .ok()
        .or(last_run.map(|r| r.left_bytes));
    // No wanted piece is missing (libtorrent `torrent::is_finished`).
    let finished = mt
        .with_chunk_tracker(|c| c.get_hns().needed_bytes == 0)
        .unwrap_or(false);
    let private = mt.with_metadata(|m| m.info.info().private).unwrap_or(false);
    crate::tracker::TrackerCommsStats {
        private,
        uploaded_bytes: totals.uploaded_bytes,
        downloaded_bytes: totals.announced_downloaded(),
        corrupt_bytes: totals.failed_bytes,
        redundant_bytes: totals.redundant_bytes,
        left_bytes,
        finished,
        torrent_state,
        // Becoming a seed, not finishing a selection (libtorrent
        // `torrent::finished` calls `completed` only when `is_seed`), in a
        // run that began short of one, whatever was wanted when it began.
        completed_during_run: left_bytes == Some(0) && live.is_some_and(|l| !l.seed_at_start()),
        // The policy the torrent's connections are made under, as the
        // connector reads it for each one.
        encryption: session.upgrade().map_or(Default::default(), |session| {
            mt.shared()
                .options
                .peer_opts
                .encryption_under(session.connector.encryption())
        }),
    }
}
