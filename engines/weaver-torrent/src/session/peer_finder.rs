// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Where a torrent finds peers: one finder per source, each saying whether
//! a torrent may use it, and one list of them, which the session asks in
//! place of naming any source itself.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use futures::{StreamExt, stream::BoxStream};

use super::ports::AnnouncePort;
use super::{Discovery, Session, dht_peers};
use crate::Identity;
use crate::core::hash_id::InfoHashes;
use crate::ssl::SslTorrent;
use crate::tracker::{ToldPorts, Tracker, TrackerStatus, TrackerTask};
use crate::type_aliases::{PeerSourceFlag, PeerStream};

/// What a torrent asks its finders for peers with.
pub(super) struct PeerAsk<'a> {
    /// Every hash it goes by: each finder asks under all of them
    /// (libtorrent `torrent::dht_announce`, `lsd_announce` and
    /// `announce_with_tracker` each loop over `info_hash_t::for_each`).
    pub info_hashes: InfoHashes,
    /// Its SSL state, for an SSL torrent.
    pub ssl: Option<&'a Arc<SslTorrent>>,
    /// The port it is reached at, which its finders tell: an SSL torrent's
    /// is the SSL listener's. Chosen once, where the ask is built.
    pub port: Arc<AnnouncePort>,
    /// Whether it announces itself where it looks beside its trackers.
    pub announce: bool,
    /// Rings for a DHT announce now.
    pub dht_announce: tokio::sync::watch::Receiver<()>,
    /// Its own list: the peers it was added with or saved.
    pub initial_peers: Vec<SocketAddr>,
    /// What its trackers are asked with.
    pub trackers: TrackerAsk<'a>,
}

impl<'a> PeerAsk<'a> {
    // The ask of a torrent that names no tracker or peer of its own, and
    // does not announce itself: a magnet's fetch, once its trackers' phase
    // is over.
    fn unannounced(
        session: &Session,
        info_hashes: InfoHashes,
        identity: &'a Identity,
        dht_announce: tokio::sync::watch::Receiver<()>,
    ) -> Self {
        Self {
            info_hashes,
            ssl: None,
            port: session.told_port(None),
            announce: false,
            dht_announce,
            initial_peers: Vec::new(),
            trackers: TrackerAsk {
                identity,
                tiers: Vec::new(),
                force_interval: None,
                private: true,
                running: None,
            },
        }
    }
}

/// What a torrent's trackers are asked with.
pub(super) struct TrackerAsk<'a> {
    pub identity: &'a Identity,
    pub tiers: Vec<Vec<Tracker>>,
    pub force_interval: Option<Duration>,
    /// It is private, or taken to be (a magnet until its metadata says),
    /// and announces to no tracker of the session's.
    pub private: bool,
    /// A tracker task already running for the torrent, taken over in place
    /// of starting its own.
    pub running: Option<TrackerTask>,
}

/// What a finder found: the peers it names, and the announce table of the
/// trackers it asks, when it asks any.
pub(super) struct Found {
    pub peers: BoxStream<'static, SocketAddr>,
    pub trackers: Option<TrackerStatus>,
}

impl Found {
    fn peers(peers: BoxStream<'static, SocketAddr>) -> Self {
        Self {
            peers,
            trackers: None,
        }
    }
}

/// One place a torrent finds peers.
pub(super) trait PeerFinder: Sync {
    /// Who the peers it names are named by.
    fn source(&self) -> PeerSourceFlag;

    /// Whether a torrent that looks where `discovery` says may use it. The
    /// private-torrent and per-torrent rules for this source live here.
    fn allowed(&self, discovery: Discovery) -> bool;

    /// The peers it names for `ask`, or none when it has none to name: the
    /// session does not run it, or the torrent gave it nothing. A source
    /// starts when the torrent first polls its stream; the trackers start
    /// here.
    fn peers(&self, session: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found>;
}

/// Every finder, in the order their streams are merged.
pub(super) static FINDERS: [&dyn PeerFinder; 4] = [&Trackers, &OwnList, &DhtFinder, &LsdFinder];

// The finders a magnet's fetch goes on to once its trackers' phase is over:
// those a torrent taken to be private never asks. Neither its trackers nor
// its own list is on it, so that phase cannot reach them.
static AFTER_TRACKERS: [&dyn PeerFinder; 2] = [&DhtFinder, &LsdFinder];

/// The peers every finder `discovery` allows names, each with who named it,
/// and the announce table of the trackers asked; none when no finder named
/// any.
pub(super) fn run(
    session: &Arc<Session>,
    ask: &mut PeerAsk<'_>,
    discovery: Discovery,
) -> (Option<PeerStream>, Option<TrackerStatus>) {
    run_over(&FINDERS, session, ask, discovery)
}

fn run_over(
    finders: &[&dyn PeerFinder],
    session: &Arc<Session>,
    ask: &mut PeerAsk<'_>,
    discovery: Discovery,
) -> (Option<PeerStream>, Option<TrackerStatus>) {
    find(finders, session, ask, |finder| finder.allowed(discovery))
}

/// The peers a magnet's fetch hears once its trackers' phase is over: the
/// DHT's and local discovery's, each where `sources` (the torrent's
/// options) lets it look, announcing on neither. It asks no tracker and no
/// own list, whatever `sources` says.
pub(super) fn after_trackers(
    session: &Arc<Session>,
    info_hashes: InfoHashes,
    identity: &Identity,
    dht_announce: tokio::sync::watch::Receiver<()>,
    sources: Discovery,
) -> Option<PeerStream> {
    let mut ask = PeerAsk::unannounced(session, info_hashes, identity, dht_announce);
    run_over(&AFTER_TRACKERS, session, &mut ask, sources).0
}

// The peers the finders `which` picks name, each with who named it, and the
// announce table of the trackers asked; none when no finder named any.
fn find(
    finders: &[&dyn PeerFinder],
    session: &Arc<Session>,
    ask: &mut PeerAsk<'_>,
    which: impl Fn(&dyn PeerFinder) -> bool,
) -> (Option<PeerStream>, Option<TrackerStatus>) {
    let mut trackers = None;
    let streams = finders
        .iter()
        .map(|finder| {
            let found = which(*finder)
                .then(|| finder.peers(session, ask))
                .flatten()?;
            trackers = trackers.take().or(found.trackers);
            let source = finder.source();
            #[cfg(test)]
            session
                .handed_out
                .lock()
                .unwrap()
                .extend(ask.info_hashes.ids20().map(|info_hash| (info_hash, source)));
            Some(found.peers.map(move |addr| (addr, source)).boxed())
        })
        .collect::<Vec<_>>();
    (merged(streams), trackers)
}

// The streams merged pairwise, halves first, the order kept: four finders
// merge as ((1, 2), (3, 4)), and a finder with no stream drops out of its
// pair.
fn merged(mut streams: Vec<Option<PeerStream>>) -> Option<PeerStream> {
    if streams.len() <= 1 {
        return streams.pop().flatten();
    }
    let right = streams.split_off(streams.len() / 2);
    super::add::merge_two_optional_streams(merged(streams), merged(right))
}

/// The torrent's trackers (libtorrent `peer_info::tracker`). Every torrent
/// asks its own; one that takes peers from them alone asks none of the
/// session's.
struct Trackers;

impl PeerFinder for Trackers {
    fn source(&self) -> PeerSourceFlag {
        PeerSourceFlag::Tracker
    }

    fn allowed(&self, _: Discovery) -> bool {
        true
    }

    fn peers(&self, session: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found> {
        let trackers = &mut ask.trackers;
        let task = trackers
            .running
            .take()
            .or_else(|| session.start_trackers(ask.info_hashes, trackers, ask.ssl, &ask.port))?;
        Some(Found {
            peers: task.peers,
            trackers: Some(task.status),
        })
    }
}

/// The torrent's own list: the peers it was added with, the operator's or
/// those a restart saved (libtorrent `peer_info::resume_data`). Every
/// torrent uses it.
struct OwnList;

impl PeerFinder for OwnList {
    fn source(&self) -> PeerSourceFlag {
        PeerSourceFlag::ResumeData
    }

    fn allowed(&self, _: Discovery) -> bool {
        true
    }

    fn peers(&self, _: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found> {
        let peers = std::mem::take(&mut ask.initial_peers);
        (!peers.is_empty()).then(|| Found::peers(futures::stream::iter(peers).boxed()))
    }
}

/// The DHT, where [`Discovery::dht`] looks.
struct DhtFinder;

impl PeerFinder for DhtFinder {
    fn source(&self) -> PeerSourceFlag {
        PeerSourceFlag::Dht
    }

    fn allowed(&self, discovery: Discovery) -> bool {
        discovery.dht
    }

    fn peers(&self, session: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found> {
        let dht = session.dht.as_ref()?;
        Some(Found::peers(
            futures::stream::select_all(ask.info_hashes.ids20().map(|info_hash| {
                dht_peers(
                    dht.clone(),
                    info_hash,
                    if ask.announce {
                        ask.port.subscribe()
                    } else {
                        tokio::sync::watch::channel(ToldPorts::default()).1
                    },
                    ask.dht_announce.clone(),
                )
            }))
            .boxed(),
        ))
    }
}

/// Local service discovery, where [`Discovery::lsd`] looks.
struct LsdFinder;

impl PeerFinder for LsdFinder {
    fn source(&self) -> PeerSourceFlag {
        PeerSourceFlag::Lsd
    }

    fn allowed(&self, discovery: Discovery) -> bool {
        discovery.lsd
    }

    fn peers(&self, session: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found> {
        let lsd = session.lsd.as_ref()?;
        // Each source starts when the torrent first asks it for peers, which
        // is once it is checked and running: trackers and the DHT by their
        // streams, the LSD by registering here (libtorrent
        // `torrent::start_announcing` runs only after `files_checked`, never
        // while paused).
        //
        // Local peers are told the port the torrent's listener is reached
        // at: the one a mapper got on the gateway, else the listen port,
        // whatever announce port is set, read at each announce (libtorrent
        // `torrent::lsd_announce`, torrent.cpp:2980-2983, takes
        // `session_impl::listen_port` or `ssl_listen_port`, which give the
        // listen socket's `tcp_external_port`, session_impl.cpp:5776-5805).
        let port = ask.announce.then(|| {
            let port = ask.port.clone();
            let port: crate::lsd::AnnouncedPort = Arc::new(move || port.external(false));
            port
        });
        Some(Found::peers(
            futures::stream::select_all(ask.info_hashes.ids20().map(|info_hash| {
                let lsd = lsd.clone();
                let port = port.clone();
                futures::stream::once(async move { lsd.announce(info_hash, port) })
                    .flatten()
                    .boxed()
            }))
            .boxed(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Mutex};

    use futures::StreamExt;

    use super::{AFTER_TRACKERS, FINDERS, Found, PeerAsk, PeerFinder, run_over};
    use crate::session::{AddTorrentOptions, Discovery, Session};
    use crate::type_aliases::PeerSourceFlag;

    async fn session() -> (tempfile::TempDir, Arc<Session>) {
        let dir = tempfile::TempDir::new().unwrap();
        let session = Session::new_with_opts(
            dir.path().to_owned(),
            crate::test_support::session_options(),
        )
        .await
        .unwrap();
        (dir, session)
    }

    fn addr(n: u8) -> SocketAddr {
        (Ipv4Addr::new(192, 0, 2, n), 6881).into()
    }

    // A finder each test drives: it names one peer under `source`, and
    // records each ask it was started with, by whether it announces.
    struct Fake {
        source: PeerSourceFlag,
        allowed: Box<dyn Fn(Discovery) -> bool + Sync>,
        peer: SocketAddr,
        started: Mutex<Vec<bool>>,
    }

    impl Fake {
        // A fake standing in for `real`: it is allowed where `real` is.
        fn of(real: &'static dyn PeerFinder, n: u8) -> Self {
            Self {
                source: real.source(),
                allowed: Box::new(move |discovery| real.allowed(discovery)),
                peer: addr(n),
                started: Mutex::default(),
            }
        }

        fn started(&self) -> Vec<bool> {
            self.started.lock().unwrap().clone()
        }
    }

    impl PeerFinder for Fake {
        fn source(&self) -> PeerSourceFlag {
            self.source
        }

        fn allowed(&self, discovery: Discovery) -> bool {
            (self.allowed)(discovery)
        }

        fn peers(&self, _: &Arc<Session>, ask: &mut PeerAsk<'_>) -> Option<Found> {
            self.started.lock().unwrap().push(ask.announce);
            Some(Found::peers(futures::stream::iter([self.peer]).boxed()))
        }
    }

    // Stand-ins for every finder on `list`, in its order.
    fn stand_ins_of(list: &'static [&'static dyn PeerFinder]) -> Vec<Fake> {
        list.iter()
            .zip(1..)
            .map(|(real, n)| Fake::of(*real, n))
            .collect()
    }

    // Stand-ins for every finder the session runs, in its order.
    fn stand_ins() -> Vec<Fake> {
        stand_ins_of(&FINDERS)
    }

    // Where a test's finders are asked from.
    enum Asked {
        // A torrent's run, `make_peer_rx`'s path, under its discovery.
        Run(Discovery),
        // A magnet's fetch once its trackers' phase is over, `add`'s
        // path: where its options let it look.
        AfterTrackers(Discovery),
    }

    // The sources a finder list was started for, each with whether its
    // ask announces, and every peer it named, with who named it, sorted.
    async fn run(
        session: &Arc<Session>,
        finders: &[Fake],
        asked: Asked,
    ) -> (
        Vec<(PeerSourceFlag, bool)>,
        Vec<(SocketAddr, PeerSourceFlag)>,
    ) {
        let identity = session.identity().clone();
        let mut ask = PeerAsk::unannounced(
            session,
            crate::core::hash_id::InfoHashes {
                v1: Some(crate::dht::Id20::new([1; 20])),
                v2: None,
            },
            &identity,
            tokio::sync::watch::channel(()).1,
        );
        let as_dyn: Vec<&dyn PeerFinder> = finders.iter().map(|f| f as &dyn PeerFinder).collect();
        let peers = match asked {
            Asked::Run(discovery) => {
                ask.announce = true;
                run_over(&as_dyn, session, &mut ask, discovery).0
            }
            Asked::AfterTrackers(sources) => run_over(&as_dyn, session, &mut ask, sources).0,
        };
        let mut named = match peers {
            Some(peers) => peers.collect::<Vec<_>>().await,
            None => Vec::new(),
        };
        named.sort_by_key(|(addr, _)| *addr);
        let started = finders
            .iter()
            .flat_map(|f| f.started().into_iter().map(|a| (f.source, a)))
            .collect();
        (started, named)
    }

    // The merge takes whatever finder is registered, names its peers by
    // its source, and asks none that `which` leaves out.
    #[tokio::test]
    async fn the_merge_takes_any_registered_finder() {
        let (_dir, session) = session().await;
        let made_up = Fake {
            source: PeerSourceFlag::Pex,
            allowed: Box::new(|_| true),
            peer: addr(9),
            started: Mutex::default(),
        };
        let left_out = Fake {
            source: PeerSourceFlag::Incoming,
            allowed: Box::new(|_| false),
            peer: addr(10),
            started: Mutex::default(),
        };
        let finders = [made_up, left_out];
        let (started, named) = run(
            &session,
            &finders,
            Asked::Run(Discovery::of(false, false, false)),
        )
        .await;
        assert_eq!(started, vec![(PeerSourceFlag::Pex, true)]);
        assert_eq!(named, vec![(addr(9), PeerSourceFlag::Pex)]);
        session.stop().await;
    }

    // Every finder the session runs keeps the rules it had: a private
    // torrent starts its trackers and its own list and nothing else,
    // whatever else it was added with, and hears from nothing else; a
    // public one starts each finder its options leave on, all of them off
    // included. A magnet's fetch, taken to be private, starts the same two
    // first, and once its trackers' phase is over starts only the others
    // its options leave on, announcing on none.
    #[tokio::test]
    async fn a_private_torrent_starts_and_hears_only_its_trackers_and_own_list() {
        use PeerSourceFlag::{Dht, Lsd, ResumeData, Tracker};
        let (_dir, session) = session().await;
        let own = |started: &[(PeerSourceFlag, bool)]| {
            started.iter().map(|(s, _)| *s).collect::<Vec<_>>()
        };
        for (disable_dht, disable_lsd) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let opts = AddTorrentOptions {
                disable_dht,
                disable_lsd,
                ..Default::default()
            };
            // Private: its trackers and own list.
            let discovery = Discovery::added(true, &opts);
            let finders = stand_ins();
            let (started, named) = run(&session, &finders, Asked::Run(discovery)).await;
            assert_eq!(own(&started), vec![Tracker, ResumeData], "{discovery:?}");
            assert_eq!(named, vec![(addr(1), Tracker), (addr(2), ResumeData)]);
            // Public: every finder its options leave on.
            let finders = stand_ins();
            let discovery = Discovery::added(false, &opts);
            let (started, _) = run(&session, &finders, Asked::Run(discovery)).await;
            let mut want = vec![Tracker, ResumeData];
            want.extend((!disable_dht).then_some(Dht));
            want.extend((!disable_lsd).then_some(Lsd));
            assert_eq!(own(&started), want);
            // A magnet's fetch past its trackers' phase: the DHT and the
            // LSD as its options leave them, unannounced, and neither the
            // trackers nor the own list again.
            let finders = stand_ins_of(&AFTER_TRACKERS);
            let (started, _) = run(&session, &finders, Asked::AfterTrackers(discovery)).await;
            let mut want = Vec::new();
            want.extend((!disable_dht).then_some((Dht, false)));
            want.extend((!disable_lsd).then_some((Lsd, false)));
            assert_eq!(started, want);
        }
        session.stop().await;
    }

    // The phase after a magnet's trackers' can reach the DHT and local
    // discovery alone: no tracker and no own list is on its list, so no
    // `Discovery` makes it ask one.
    #[test]
    fn the_phase_after_the_trackers_reaches_only_the_dht_and_lsd() {
        use PeerSourceFlag::{Dht, Lsd};
        let sources = AFTER_TRACKERS
            .iter()
            .map(|f| f.source())
            .collect::<Vec<_>>();
        assert_eq!(sources, vec![Dht, Lsd]);
        // And each of them is one a torrent taken to be private never asks.
        let private = Discovery::of(true, false, false);
        assert!(AFTER_TRACKERS.iter().all(|f| !f.allowed(private)));
    }

    // Each real finder's own rule, read from the table: trackers and the
    // own list for every torrent, the DHT and the LSD only where
    // `Discovery` looks.
    #[test]
    fn each_finder_is_allowed_where_its_rule_says() {
        let rules = |d: Discovery| {
            FINDERS
                .iter()
                .filter(|f| f.allowed(d))
                .map(|f| f.source())
                .collect::<Vec<_>>()
        };
        use PeerSourceFlag::{Dht, Lsd, ResumeData, Tracker};
        assert_eq!(
            rules(Discovery::of(true, false, false)),
            vec![Tracker, ResumeData]
        );
        assert_eq!(
            rules(Discovery::of(false, false, false)),
            vec![Tracker, ResumeData, Dht, Lsd]
        );
        assert_eq!(
            rules(Discovery::of(false, true, false)),
            vec![Tracker, ResumeData, Lsd]
        );
        assert_eq!(
            rules(Discovery::of(false, false, true)),
            vec![Tracker, ResumeData, Dht]
        );
    }
}
