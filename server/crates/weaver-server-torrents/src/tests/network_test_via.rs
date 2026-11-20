// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline proxies/network_test_via_tests.rs: the
// route's test runs the torrents crate's test torrent.

use std::net::IpAddr;
use std::sync::Arc;

use crate::Database;
use crate::proxies::*;

fn runtime_with(
    profiles: Vec<ProxyProfile>,
    pools: Vec<ProxyPool>,
    policies: Vec<(Consumer, RoutingPolicy)>,
) -> Arc<NetworkRuntime> {
    let runtime = NetworkRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let mut config = runtime.configuration.read().unwrap().clone();
    config
        .profiles
        .extend(profiles.into_iter().map(|profile| (profile.id, profile)));
    config
        .pools
        .extend(pools.into_iter().map(|pool| (pool.id, pool)));
    for (consumer, policy) in policies {
        config.consumers.insert(consumer.key());
        config.policies.insert(consumer.key(), policy);
    }
    runtime.apply_configuration(config).unwrap();
    runtime
}

fn ip(text: &str) -> IpAddr {
    text.parse().unwrap()
}

// A torrent route's test runs the test torrent over each path that reached
// the torrent's host, on that path's leg as the route's session would ride
// it: a direct leg on a loopback source address takes peers through its
// own sockets, and its loopback listener announces no port.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_route_runs_the_test_torrent_over_each_path() {
    use crate::speed_test::torrent_fixture::{Answer, TorrentFixture};
    use crate::speed_test::{P2pStep, P2pTest};
    use crate::torrents::net::Exposure;

    let fixture = TorrentFixture::start(true).await;
    fixture.answer(Answer {
        peers: vec![fixture.seeder()],
        seeders: 1,
        leechers: 0,
        failure: None,
        connect_in: true,
    });
    let runtime = runtime_with(vec![], vec![], vec![]);
    let mut config = runtime.configuration.read().unwrap().clone();
    config.egresses.insert(
        5,
        EgressInterface {
            id: 5,
            name: "loopback".into(),
            binding: EgressBinding::SourceAddress {
                address: ip("127.0.0.1"),
            },
            ..EgressInterface::system()
        },
    );
    runtime.apply_configuration(config).unwrap();
    let route = Route {
        legs: vec![RouteLeg {
            egress_id: 5,
            weight: 100,
            path: LegPath::Direct,
        }],
        failover: Failover::default(),
    };
    let quiet = crate::settings::TorrentConfig {
        upnp: false,
        dht_bootstrap_nodes: String::new(),
        ..Default::default()
    };
    let tests = runtime
        .test_via_torrent(
            &route,
            SYSTEM_DNS_PROFILE_ID,
            TestSubject::Route,
            None,
            P2pTest {
                source: fixture.source.clone(),
                torrent: quiet,
                ip_filter: Ok(weaver_torrent::IpFilter::default()),
                allow_private_network: true,
                // A clock no one moves: the piece wait ends on the piece.
                clock: crate::speed_test::HandClock::new().clock(),
            },
        )
        .await
        .unwrap();

    assert_eq!(tests.len(), 1);
    assert_eq!(tests[0].outcome, Ok(ProxyTestPass::Connected));
    let report = tests[0].p2p.as_ref().expect("the torrent ran");
    for step in [
        P2pStep::Torrent,
        P2pStep::Announce,
        P2pStep::Handshake,
        P2pStep::Piece,
    ] {
        assert!(report.passed(step), "{step:?} in {report:?}");
    }
    assert!(!report.passed(P2pStep::Inbound), "{report:?}");
    assert_eq!(report.mapped_port, None);
    let exposure: Exposure = report.exposure.expect("the session opened");
    assert_eq!(exposure.resolved_by, Some(ResolvedBy::Host));
    assert_eq!(exposure.announced_port, 0);
    assert!(!exposure.inbound);
    assert!(exposure.udp_trackers && exposure.dht);
    fixture.stop().await;
    runtime.shutdown().await;
}
