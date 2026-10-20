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

// A path on the host's own sockets, which listens on every address. A
// loopback listener would announce no port.
fn listening() -> LegKind {
    LegKind::System
}

async fn run_probe(fixture: &TorrentFixture, kind: LegKind, budget: Option<u64>) -> P2pReport {
    let stop = std::pin::pin!(std::future::pending::<()>());
    probe(
        P2pPath {
            stage: loopback(),
            kind,
            gateway: None,
            internal_port: 0,
        },
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

// Fetched, announced, dialled, one piece verified, and a peer connected in;
// the tracker then hears `stopped`.
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
    let report = run_probe(&fixture, listening(), None).await;

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
        outcome(&report, P2pStep::Inbound),
        &P2pOutcome::Passed("1 connected in".into())
    );
    assert_eq!(report.mapped_port, None);
    assert_eq!(report.upload, None);

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
    assert_ne!(announces[0].port, 0);
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
    let path = P2pPath {
        stage: loopback(),
        kind: LegKind::Ladder,
        gateway: None,
        internal_port: 0,
    };
    let torrent = TestTorrent::fetch(path.stage.clone(), &fixture.source)
        .await
        .unwrap();
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
    torrent.close().await;

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
