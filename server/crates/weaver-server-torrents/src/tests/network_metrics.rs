// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline proxies/network_metrics.rs: these
// dial through the torrents crate's leg sockets.

use crate::proxies::NetworkRuntime;
use crate::proxies::network_metrics::*;

// A torrent that holds `0` on its leg.
struct Holds(crate::proxies::SessionTraffic);

impl crate::proxies::LegTenant for Holds {
    fn traffic(&self) -> crate::proxies::SessionTraffic {
        self.0
    }
}

// A torrent session that dials its peers through its leg counts the
// bytes it reads on that leg, as every connection the leg carries does.
// It runs on the current-thread runtime so the rate timer cannot close
// another window between the wait and the snapshot that reads it.
#[tokio::test(flavor = "current_thread")]
async fn a_torrent_session_through_its_leg_counts_its_reads_on_that_leg() {
    use crate::torrents::net::{LegKind, LegSockets};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use weaver_torrent::SocketFactory;

    let runtime = NetworkRuntime::new(
        crate::Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let scope = crate::proxies::TorrentScope::Default;
    let consumer = crate::proxies::Consumer::Torrent(scope);
    let timeout = std::time::Duration::from_secs(30);
    let leg = runtime.torrent_leg(scope, 0, 8, timeout).unwrap();
    let sockets = LegSockets::new(leg.dialer(), &LegKind::Ladder);
    let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut read, _write) = sockets
        .connect_tcp(peer.local_addr().unwrap())
        .await
        .unwrap();
    let (mut stream, _) = peer.accept().await.unwrap();
    stream.write_all(&[7; 64 * 1024]).await.unwrap();
    let mut piece = vec![0; 64 * 1024];
    read.read_exact(&mut piece).await.unwrap();

    // The leg's rate closes its first window a second after the reads.
    let route = runtime.route(consumer, 8, timeout).unwrap();
    route
        .weighted
        .subscribe()
        .wait_for(|legs| legs[0].bytes_per_second > 0)
        .await
        .unwrap();
    let metrics = snapshot(&runtime);
    let row = metrics
        .legs
        .iter()
        .find(|leg| leg.consumer == consumer.key() && leg.position == 0)
        .unwrap();
    assert_eq!((row.live, row.open), (true, 1));
    assert!(row.bytes_per_second > 0);
    runtime.shutdown().await;
}

// A connection a torrent session dials through its leg is open on the
// leg once its peer is established; until then, in its handshake, it is
// still being set up.
#[tokio::test(flavor = "current_thread")]
async fn a_torrent_leg_counts_open_only_its_established_peers() {
    use crate::torrents::net::{LegKind, LegSockets};
    use weaver_torrent::SocketFactory;

    let runtime = NetworkRuntime::new(
        crate::Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let scope = crate::proxies::TorrentScope::Default;
    let consumer = crate::proxies::Consumer::Torrent(scope);
    let timeout = std::time::Duration::from_secs(30);
    let leg = runtime.torrent_leg(scope, 0, 8, timeout).unwrap();
    let sockets = LegSockets::new(leg.dialer(), &LegKind::Ladder);
    let peers = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (mut dialed, mut accepted) = (Vec::new(), Vec::new());
    for _ in 0..3 {
        dialed.push(
            sockets
                .connect_tcp(peers.local_addr().unwrap())
                .await
                .unwrap(),
        );
        accepted.push(peers.accept().await.unwrap());
    }
    let row = |established: u64| {
        let _tenancy = runtime.tenant_torrent(
            consumer,
            0,
            Holds(crate::proxies::SessionTraffic {
                open: established,
                ..Default::default()
            }),
        );
        let metrics = snapshot(&runtime);
        let row = metrics
            .legs
            .iter()
            .find(|leg| leg.consumer == consumer.key() && leg.position == 0)
            .unwrap();
        (row.open, row.opening)
    };

    // Three connections, every one in its handshake.
    assert_eq!(row(0), (0, 3));
    // One peer is established.
    assert_eq!(row(1), (1, 2));
    assert_eq!(row(3), (3, 0));
    drop((dialed, accepted));
    runtime.shutdown().await;
}
