// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved from weaver-server-pipeline proxies/runtime/tests.rs: this plans
// through the torrents crate's routes.

use std::sync::Arc;
use std::time::Duration;

use crate::Database;
use crate::proxies::ProxiesNetworkPersistenceQueries;
use crate::proxies::ProxiesPersistenceQueries;
use crate::proxies::*;

// A reload asked for while another runs is answered by a pass that read the
// rows after it was asked: a torrent route saved while a reload holds the
// rows it read before the save is planned from once the second reload
// returns.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reload_asked_for_during_another_reads_the_rows_again() {
    use crate::proxies::TorrentScope;
    use crate::torrents::sessions::{NetworkRoutes, Routes};

    let db = Database::open_in_memory().unwrap();
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = NetworkRoutes::new(Arc::clone(&runtime), Duration::from_secs(30));
    let absent = db
        .create_egress_interface(&crate::proxies::EgressInterface {
            id: 0,
            name: "absent".into(),
            binding: crate::proxies::EgressBinding::Interface {
                name: "weaverabsent0".into(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: Default::default(),
        })
        .unwrap();

    let (read_tx, read) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    *runtime.pause.lock().unwrap() = Some((read_tx, release_rx));
    let first = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.reload().await }
    });
    // The first reload has read the rows and holds them.
    read.await.unwrap();
    db.save_proxy_routing_policy(
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: absent.id,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let second = tokio::spawn({
        let runtime = Arc::clone(&runtime);
        async move { runtime.reload().await }
    });
    // The second is counted before the first is let go.
    while runtime.reloads.lock().unwrap().asked < 2 {
        tokio::task::yield_now().await;
    }
    release.send(()).unwrap();

    second.await.unwrap().unwrap();
    assert_eq!(routes.plan(TorrentScope::Default).egress_id, absent.id);
    first.await.unwrap().unwrap();
    runtime.network.shutdown().await;
}
