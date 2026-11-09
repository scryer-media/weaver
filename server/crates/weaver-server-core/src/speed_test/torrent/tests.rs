// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::speed_test::torrent_fixture::{Answer, CONTENT_LEN, LISTED, PIECE_LEN, TorrentFixture};

fn loopback() -> Arc<dyn Dialer> {
    Arc::new(weaver_tunnel::pipe::Egress::new(
        4,
        weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        // No connect deadline: the test waits on what it checks.
        Duration::MAX,
    ))
}

// A leg on the loopback source address: its sockets come from its dialer,
// it maps no port and multicasts nothing, and its loopback listener
// announces no port.
fn source_address() -> LegKind {
    LegKind::SourceAddress("127.0.0.1".parse().unwrap())
}

// Torrent settings whose sessions reach no host: no DHT bootstrap node and
// no port mapping.
fn quiet() -> TorrentConfig {
    TorrentConfig {
        upnp: false,
        dht_bootstrap_nodes: String::new(),
        ..TorrentConfig::default()
    }
}

fn leg(kind: LegKind) -> P2pLeg {
    P2pLeg {
        kind,
        stage: loopback(),
        down: None,
        names: Some(ResolvedBy::Host),
        torrent: quiet(),
        ip_filter: Ok(IpFilter::default()),
        allow_private_network: true,
        // A clock no one moves: no wait ends on time, only on what the
        // test checks.
        clock: crate::speed_test::HandClock::new().clock(),
    }
}

async fn run_probe(fixture: &TorrentFixture, kind: LegKind, budget: Option<u64>) -> P2pReport {
    let stop = std::pin::pin!(std::future::pending::<()>());
    probe(
        P2pPath::open(leg(kind)).await.unwrap(),
        &P2pPlan {
            source: fixture.source.clone(),
            upload_budget: budget,
        },
        stop,
    )
    .await
}

fn outcome(report: &P2pReport, step: P2pStep) -> &P2pOutcome {
    &report
        .rungs
        .iter()
        .find(|rung| rung.step == step)
        .unwrap_or_else(|| panic!("no {step:?} rung in {report:?}"))
        .outcome
}

fn failed(report: &P2pReport, step: P2pStep) -> String {
    match outcome(report, step) {
        P2pOutcome::Failed(error) => error.clone(),
        other => panic!("{step:?} did not fail: {other:?} in {report:?}"),
    }
}

#[test]
fn the_listing_names_the_netinst_torrent() {
    let listing = r#"<a href="debian-13.1.0-amd64-DVD-1.iso.torrent">x</a>
<a href="debian-edu-13.1.0-amd64-netinst.iso.torrent">x</a>
<a href="debian-13.1.0-amd64-netinst.iso.torrent">x</a>"#;
    assert_eq!(
        pick_torrent(listing).as_deref(),
        Some("debian-13.1.0-amd64-netinst.iso.torrent")
    );
    assert_eq!(pick_torrent("<a href=\"index.html\">"), None);
}

// Fetched, announced, dialled and one piece verified; a leg whose listener
// announces no port takes no peer in, and says what it showed. The tracker
// then hears `stopped`, and never heard a port the session cannot answer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_rung_passes_against_a_loopback_tracker_and_seeder() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        leechers: 0,
        failure: None,
        connect_in: true,
    });
    let report = run_probe(&fixture, source_address(), None).await;

    assert_eq!(
        outcome(&report, P2pStep::Torrent),
        &P2pOutcome::Passed(LISTED.into())
    );
    assert_eq!(
        outcome(&report, P2pStep::Announce),
        &P2pOutcome::Passed("1 seeders, 0 leechers".into())
    );
    assert!(report.passed(P2pStep::Handshake), "{report:?}");
    assert_eq!(
        outcome(&report, P2pStep::Piece),
        &P2pOutcome::Passed(format!("piece 0 ({PIECE_LEN} bytes) verified"))
    );
    assert_eq!(
        failed(&report, P2pStep::Inbound),
        "this path cannot take peer connections"
    );
    assert_eq!(report.mapped_port, None);
    assert_eq!(report.upload, None);
    assert_eq!(
        report.exposure,
        Some(Exposure {
            resolved_by: Some(ResolvedBy::Host),
            address: None,
            announced_port: 0,
            inbound: false,
            udp_trackers: true,
            dht: true,
            utp: false,
        })
    );

    let mut announces = fixture.announces();
    let announces = announces
        .wait_for(|announces| {
            announces
                .iter()
                .any(|announce| announce.event.as_deref() == Some("stopped"))
        })
        .await
        .unwrap()
        .clone();
    assert_eq!(announces[0].event.as_deref(), Some("started"));
    assert_eq!(announces[0].port, 0);
    fixture.stop().await;
}

// A peer that refuses the dial fails the handshake rung in the dial's own
// words; a path that cannot listen takes no peer in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_peer_fails_the_handshake_rung() {
    let fixture = TorrentFixture::start(true).await;
    let refused = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    fixture.answer(Answer {
        peers: vec![refused],
        seeders: 1,
        ..Answer::default()
    });
    let report = run_probe(&fixture, LegKind::Ladder, None).await;

    assert!(report.passed(P2pStep::Announce), "{report:?}");
    let handshake = failed(&report, P2pStep::Handshake);
    assert!(handshake.contains(&refused.to_string()), "{handshake}");
    assert_eq!(outcome(&report, P2pStep::Piece), &P2pOutcome::NotReached);
    assert_eq!(
        failed(&report, P2pStep::Inbound),
        "this path cannot take peer connections"
    );
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tracker_failure_fails_the_announce_rung() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        failure: Some("torrent not registered".into()),
        ..Answer::default()
    });
    let report = run_probe(&fixture, LegKind::Ladder, None).await;

    assert!(report.passed(P2pStep::Torrent), "{report:?}");
    let announce = failed(&report, P2pStep::Announce);
    assert!(announce.contains("torrent not registered"), "{announce}");
    assert_eq!(
        outcome(&report, P2pStep::Handshake),
        &P2pOutcome::NotReached
    );
    assert_eq!(outcome(&report, P2pStep::Piece), &P2pOutcome::NotReached);
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_listing_without_the_netinst_torrent_fails_the_first_rung() {
    let fixture = TorrentFixture::start(false).await;
    let report = run_probe(&fixture, LegKind::Ladder, None).await;

    let torrent = failed(&report, P2pStep::Torrent);
    assert!(torrent.contains("lists no netinst torrent"), "{torrent}");
    for step in [
        P2pStep::Announce,
        P2pStep::Handshake,
        P2pStep::Piece,
        P2pStep::Inbound,
    ] {
        assert_eq!(outcome(&report, step), &P2pOutcome::NotReached);
    }
    fixture.stop().await;
}

// The verified piece goes to the leecher the tracker sent, counted as the
// block bytes the leecher took.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_piece_is_served_to_a_leecher_the_tracker_sends() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder(), fixture.leecher()],
        seeders: 1,
        leechers: 1,
        ..Answer::default()
    });
    let report = run_probe(&fixture, LegKind::Ladder, Some(u64::from(PIECE_LEN))).await;

    match &report.upload {
        Some(PeerUpload::Measured { bytes, peers, .. }) => {
            assert_eq!(*bytes, u64::from(PIECE_LEN));
            assert_eq!(*peers, 1);
        }
        other => panic!("not measured: {other:?} in {report:?}"),
    }
    assert_eq!(*fixture.taken().borrow(), u64::from(PIECE_LEN));
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_leechers_is_no_demand() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        leechers: 0,
        ..Answer::default()
    });
    let report = run_probe(&fixture, LegKind::Ladder, Some(u64::from(PIECE_LEN))).await;

    assert!(report.passed(P2pStep::Piece), "{report:?}");
    assert_eq!(report.upload, Some(PeerUpload::NoDemand));
    fixture.stop().await;
}

// A pull counts every block it writes and holds no more verified bytes than
// it may keep; it tells the tracker `stopped` as it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pull_counts_every_block_and_holds_no_more_than_it_keeps() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        ..Answer::default()
    });
    let path = P2pPath::open(leg(LegKind::Ladder)).await.unwrap();
    let torrent = TestTorrent::fetch(&path, &fixture.source).await.unwrap();
    let received = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counted = received.clone();
    let pulled = pull(
        &path,
        &torrent,
        u64::from(PIECE_LEN),
        move |bytes| {
            counted.fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
        },
        std::pin::pin!(std::future::pending::<()>()),
    )
    .await;
    path.close().await;

    assert_eq!(pulled.failure, None);
    assert_eq!(
        received.load(std::sync::atomic::Ordering::Relaxed),
        CONTENT_LEN as u64
    );
    assert_eq!(pulled.store.held().len(), 1);
    let mut announces = fixture.announces();
    announces
        .wait_for(|announces| {
            announces
                .iter()
                .any(|announce| announce.event.as_deref() == Some("stopped"))
        })
        .await
        .unwrap();
    fixture.stop().await;
}

// The test torrent is fetched over the route's leg or not at all: while the
// leg is down nothing goes out, and the source never hears from the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_down_leg_fetches_nothing() {
    let source = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    source.set_nonblocking(true).unwrap();
    let plan = P2pPlan {
        source: TestTorrentSource {
            directory: format!("http://{}/", source.local_addr().unwrap())
                .parse()
                .unwrap(),
        },
        upload_budget: None,
    };
    let path = P2pPath::open(P2pLeg {
        down: Some(crate::torrents::sessions::RouteFailure::leg_down(
            "egress 3 is down".into(),
        )),
        ..leg(source_address())
    })
    .await
    .unwrap();
    let report = probe(path, &plan, std::pin::pin!(std::future::pending::<()>())).await;

    assert_eq!(
        failed(&report, P2pStep::Torrent),
        "route down: egress 3 is down"
    );
    assert_eq!(report.exposure, None);
    assert_eq!(
        source.accept().map(|_| ()).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

// What the probe's session opens with, as far as a route's session on the
// same leg says it: every field is the route's, save the probe's own port,
// the port it announces, its peer limit and where it keeps its state.
fn route_fields(options: &SessionOptions) -> String {
    let identity = options.identity.as_ref().unwrap();
    let listen = options.listen.as_ref().map(|listen| {
        format!(
            "{:?} {} {:?} {} {} {:?}",
            listen.mode,
            listen.listen_addr.ip(),
            listen.enable_upnp_port_forwarding,
            listen.ipv4_only,
            listen.max_pending_incoming_handshake_checks,
            listen.ssl_listen_port,
        )
    });
    let connect = options
        .connect
        .as_ref()
        .map(|connect| format!("{} {:?}", connect.enable_tcp, connect.peer_opts));
    format!(
        "dht {:?} device {:?} trackers off {} stopped {:?} fastresume {} \
         persistence {} peer id {:?} agent {} version {:?} listen {listen:?} \
         connect {connect:?} unchoke {} connections {} slack {} speed {} lsd off {} \
         pex off {} v4 {} resolver {} http {} encryption {:?} multiple {} \
         trackers {:?} allowlist {:?}",
        options.dht.as_ref().map(|dht| dht.bootstrap_addrs.clone()),
        options.bind_device_name,
        options.disable_trackers,
        options.tracker_stopped_timeout,
        options.fastresume,
        options.persistence.is_some(),
        &identity.peer_id.0[..8],
        identity.user_agent,
        identity.version,
        options.unchoke_slots_limit,
        options.connections_limit,
        options.connections_slack,
        options.connection_speed,
        options.disable_local_service_discovery,
        options.disable_pex,
        options.ipv4_only,
        options.resolver.is_some(),
        options.http_client.is_some(),
        options.encryption,
        options.allow_multiple_connections_per_ip,
        options.trackers,
        options.allowlist_url,
    )
}

// The probe's session takes a route's session's options on every kind of
// leg, through the one function a route's session takes them from: the
// leg's DHT, listener, port mapping, sockets, names and HTTP, and every
// setting. Only the probe's own port, the port it announces, its peer limit
// and its state directory are its own.
#[tokio::test]
async fn the_probe_opens_with_a_routes_options_on_every_kind_of_leg() {
    let dir = tempfile::tempdir().unwrap();
    let sessions = filter_loader(dir.path());
    let blocklist = dir.path().join("blocked.p2p");
    std::fs::write(&blocklist, "test:127.0.0.1-127.0.0.1\n").unwrap();
    let settings = TorrentConfig {
        upnp: true,
        announce_port: Some(51_413),
        allow_multiple_connections_per_ip: false,
        blocklist_enabled: true,
        blocklist: Some(blocklist.display().to_string()),
        ..quiet()
    };
    for kind in [
        LegKind::System,
        LegKind::Interface(crate::torrents::net::LOOPBACK_DEVICE.into()),
        source_address(),
        LegKind::Ladder,
    ] {
        let path = P2pPath::open(P2pLeg {
            torrent: settings.clone(),
            ip_filter: sessions.ip_filter(&settings).await,
            ..leg(kind.clone())
        })
        .await
        .unwrap();
        let bridge = match kind {
            LegKind::Ladder => Some(
                PathBridge::onto(StreamProvider::new(loopback(), None) as Arc<dyn FetchPath>)
                    .unwrap(),
            ),
            _ => None,
        };
        let route_leg = OpenLeg::on(kind.clone(), loopback(), bridge.clone(), None).unwrap();
        let route = session_options(
            &route_leg,
            6881,
            &dir.path().join("route"),
            100,
            &settings,
            sessions.ip_filter(&settings).await.unwrap(),
            &IDENTITY.identity(),
        );
        let probe = path.session_options(&dir.path().join("probe")).unwrap();

        assert_eq!(route_fields(&probe), route_fields(&route), "{kind:?}");
        assert_eq!(probe.ip_filter, route.ip_filter, "{kind:?}");
        assert!(probe.ip_filter.blocked.has("127.0.0.1".parse().unwrap()));
        // The probe's sockets are its leg's.
        assert!(
            Arc::ptr_eq(&probe.socket_factory, &path.leg.factory),
            "{kind:?}"
        );
        assert_eq!(probe.peer_limit, Some(PEER_LIMIT));
        assert_eq!(
            probe.dht.as_ref().map(|dht| dht.port),
            route.dht.as_ref().map(|_| Some(0)),
            "{kind:?}"
        );
        match (&probe.listen, &route.listen) {
            (Some(probe), Some(route)) => {
                assert_eq!(probe.listen_addr.port(), 0);
                assert_eq!(probe.announce_port, None);
                assert_eq!(route.announce_port, Some(51_413));
            }
            (None, None) => assert_eq!(kind, LegKind::Ladder),
            other => panic!("{kind:?}: {other:?}"),
        }
        path.close().await;
        if let Some(bridge) = bridge {
            bridge.revoke().await;
        }
    }
}

fn filter_loader(dir: &Path) -> crate::torrents::sessions::Sessions {
    crate::torrents::sessions::Sessions::new(
        Arc::new(crate::torrents::sessions::tests::FakeRoutes::default()),
        crate::torrents::sessions::tests::env(dir),
        crate::Database::open_in_memory().unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_probe_dials_no_peer_blocked_by_the_routes_filter() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        ..Answer::default()
    });
    let dir = tempfile::tempdir().unwrap();
    let blocklist = dir.path().join("blocked.p2p");
    let ip = fixture.seeder().ip();
    std::fs::write(&blocklist, format!("peer:{ip}-{ip}\n")).unwrap();
    let settings = TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some(blocklist.display().to_string()),
        filter_trackers: false,
        ..quiet()
    };
    let path = P2pPath::open(P2pLeg {
        ip_filter: filter_loader(dir.path()).ip_filter(&settings).await,
        torrent: settings,
        ..leg(source_address())
    })
    .await
    .unwrap();
    let plan = P2pPlan {
        source: fixture.source.clone(),
        upload_budget: None,
    };
    let torrent = TestTorrent::fetch(&path, &plan.source).await.unwrap();
    let watched = Arc::new(Watched::new());
    let store = PieceStore::new(vec![0]);
    let joined = Joined::open(&torrent, &path, &store, &[], watched.clone())
        .await
        .unwrap();
    let session = joined.session.clone();
    let handle = joined.handle.clone();
    // End this probe only once its own session has filtered the returned peer.
    let stop = std::pin::pin!(async {
        while session.stats_snapshot().counters.blocked_outgoing == 0
            || announced(&handle.tracker_announces()).is_none()
        {
            tokio::task::yield_now().await;
        }
    });
    let report = run_joined(&path, &plan, &torrent, store, watched.clone(), joined, stop).await;
    assert!(report.passed(P2pStep::Announce), "{report:?}");
    assert_eq!(watched.dials(), 0);
    assert_eq!(
        failed(&report, P2pStep::Handshake),
        "the tracker returned no peer to dial"
    );
    assert_eq!(outcome(&report, P2pStep::Piece), &P2pOutcome::NotReached);
    path.close().await;
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_probe_without_a_blocklist_dials_the_peer() {
    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        ..Answer::default()
    });
    let dir = tempfile::tempdir().unwrap();
    let settings = quiet();
    let path = P2pPath::open(P2pLeg {
        ip_filter: filter_loader(dir.path()).ip_filter(&settings).await,
        torrent: settings,
        ..leg(source_address())
    })
    .await
    .unwrap();
    let report = probe(
        path,
        &P2pPlan {
            source: fixture.source.clone(),
            upload_budget: None,
        },
        std::pin::pin!(std::future::pending::<()>()),
    )
    .await;
    assert!(report.passed(P2pStep::Handshake), "{report:?}");
    assert!(report.passed(P2pStep::Piece), "{report:?}");
    fixture.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_probe_reports_a_blocklist_that_does_not_load() {
    let fixture = TorrentFixture::start(true).await;
    let dir = tempfile::tempdir().unwrap();
    let settings = TorrentConfig {
        blocklist_enabled: true,
        blocklist: Some(dir.path().join("missing.p2p").display().to_string()),
        ..quiet()
    };
    let path = P2pPath::open(P2pLeg {
        ip_filter: filter_loader(dir.path()).ip_filter(&settings).await,
        torrent: settings,
        ..leg(source_address())
    })
    .await
    .unwrap();
    let report = probe(
        path,
        &P2pPlan {
            source: fixture.source.clone(),
            upload_budget: None,
        },
        std::pin::pin!(std::future::pending::<()>()),
    )
    .await;
    assert!(
        failed(&report, P2pStep::Announce).contains("the blocklist did not load"),
        "{report:?}"
    );
    assert_eq!(
        outcome(&report, P2pStep::Handshake),
        &P2pOutcome::NotReached
    );
    fixture.stop().await;
}
