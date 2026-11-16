// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_that_goes_down_holds_its_torrents_and_a_category_move_changes_session() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let first = rig.service.torrent(job_id).await.unwrap().route.unwrap();

    rig.routes.set_down(Some("tunnel gone"));
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (Some("tunnel gone"), Some(TorrentRouteReason::LegDown))
    );
    let held = rig.service.inner.sessions.views().await;
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0]
            .held
            .as_ref()
            .and_then(|failure| failure.detail.as_deref()),
        Some("tunnel gone")
    );

    rig.routes.set_down(None);
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.route_reason, None);
    assert_eq!(view.route.as_deref(), Some(first.as_str()));
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .attached
            .is_some()
    );

    // The category routes through its own proxy: the torrent moves there,
    // and the session it left closes once nothing uses it.
    rig.routes.route(TorrentScope::Category(1), 9);
    rig.service
        .set_category(job_id, Some("tv".into()))
        .await
        .unwrap();
    rig.service.opened(job_id).await;
    let moved = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(moved.category.as_deref(), Some("tv"));
    assert_ne!(moved.route.as_deref(), Some(first.as_str()));
    rig.service.tick().await;
    let keys: Vec<String> = rig
        .service
        .inner
        .sessions
        .views()
        .await
        .into_iter()
        .map(|view| view.key.as_str().to_owned())
        .collect();
    assert_eq!(keys, [moved.route.unwrap()]);
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().category_id,
        Some(1)
    );
}

// `policy` stored as `consumer`'s route as it is, past the checks a save
// makes: a route kept from before they held.
async fn store_route_unchecked(
    db: &Database,
    proxies: &crate::proxies::ProxyRuntime,
    consumer: Consumer,
    policy: &RoutingPolicy,
) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let store = db.datastore();
    let row = (consumer.key(), serde_json::to_string(policy).unwrap());
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "route_fixture", |tx| {
            let (key, policy) = row.clone();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {})",
                    &[SqlArg::Text(key), SqlArg::Text(policy)],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
    proxies.reload().await.unwrap();
}

// A torrent added by URL is asked for as the client of the tracker rule its
// URL's host falls under, and as Weaver under no rule.
#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_is_asked_for_as_the_client_its_rule_names() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    service.open().await.unwrap();

    let site = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/a.torrent", site.local_addr().unwrap())).unwrap();
    let body = b"d4:infod4:name1:aee";
    let fetch_as = async || {
        let served = async {
            let (mut stream, _) = site.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended before it was read");
                request.extend_from_slice(&buf[..read]);
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            String::from_utf8(request)
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("user-agent")
                        .then(|| value.trim().to_owned())
                })
        };
        let (fetched, agent) = tokio::join!(service.fetch_url(&proxies, None, &url, None), served);
        assert_eq!(fetched.unwrap(), body);
        agent
    };

    assert_eq!(
        fetch_as().await.as_deref(),
        Some(crate::torrents::identity::IDENTITY.user_agent())
    );

    db.insert_tracker_rule(
        &TrackerRule {
            archive_passwords: Default::default(),
            bound: None,
            id: 0,
            host_pattern: "127.0.0.1".into(),
            seeding: None,
            never_pause: false,
            unpack: None,
            client: Some(Client::QBittorrent),
            torrent_route: false,
            dht: None,
            pex: None,
            lsd: None,
            encryption: None,
            upload_cap: Default::default(),
            forced: false,
            ssl: None,
        },
        EPOCH_MS,
    )
    .unwrap();
    service.reload_tracker_rules().await.unwrap();
    assert_eq!(
        fetch_as().await.as_deref(),
        Some(Client::QBittorrent.name().user_agent())
    );
    service.stop().await;
}

#[tokio::test]
async fn stored_route_faults_keep_their_typed_reason() {
    use crate::proxies::{Rung, TorrentRouteFault};
    use crate::torrents::sessions::{NetworkRoutes, TorrentRouteReason};

    let direct = RouteLeg {
        egress_id: SYSTEM_EGRESS_ID,
        weight: 100,
        path: LegPath::Direct,
    };
    let ladder = |rungs, direct_fallback| RouteLeg {
        path: LegPath::Ladder {
            rungs,
            direct_fallback,
        },
        ..direct.clone()
    };
    for (legs, fault) in [
        (
            vec![direct.clone(), direct.clone()],
            TorrentRouteFault::Legs,
        ),
        (
            vec![ladder(vec![Rung::Proxy { id: 1 }], true)],
            TorrentRouteFault::DirectFallback,
        ),
        (
            vec![ladder(
                vec![Rung::Proxy { id: 1 }, Rung::Proxy { id: 2 }],
                false,
            )],
            TorrentRouteFault::Rungs,
        ),
    ] {
        let db = Database::open_in_memory().unwrap();
        let proxies =
            crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current())
                .unwrap();
        store_route_unchecked(
            &db,
            &proxies,
            Consumer::Torrent(TorrentScope::Default),
            &RoutingPolicy {
                legs,
                ..Default::default()
            },
        )
        .await;
        let routes = NetworkRoutes::new(Arc::clone(&proxies), Duration::from_secs(30));
        let failure = routes.plan(TorrentScope::Default).down.unwrap();
        assert_eq!(failure.reason, TorrentRouteReason::RouteBroken(fault));
        assert_eq!(failure.message(), fault.message());
        proxies.network.shutdown().await;
    }
}

// A route whose one rung is a pool plans a session on each enabled member,
// each under its own key, and is up; placed, a torrent runs on the lowest
// member, whose leg is up.
#[tokio::test]
async fn a_pool_route_plans_a_session_on_each_member() {
    use crate::proxies::{ProxyKind, ProxyPool, Rung};
    use crate::torrents::sessions::{NetworkRoutes, member_key};

    let db = Database::open_in_memory().unwrap();
    for id in 1..=3 {
        db.save_proxy_profile(&crate::proxies::ProxyProfile {
            id,
            name: format!("exit-{id}"),
            enabled: id != 3,
            ..loopback_proxy(1080)
        })
        .unwrap();
    }
    let pool = db
        .create_proxy_pool(&ProxyPool {
            id: 0,
            name: "Region".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![2, 3, 1],
            enabled: true,
        })
        .unwrap();
    let path = LegPath::Ladder {
        rungs: vec![Rung::Pool { id: pool.id }],
        direct_fallback: false,
    };
    db.save_proxy_routing_policy(
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: path.clone(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = NetworkRoutes::new(Arc::clone(&proxies), Duration::from_secs(30));

    let plan = routes.plan(TorrentScope::Default);
    assert!(plan.down.is_none(), "{:?}", plan.down);
    let key = |member| member_key(&proxies.network, SYSTEM_EGRESS_ID, member);
    let members: Vec<_> = plan
        .members
        .iter()
        .map(|member| (member.id, member.key.clone(), member.down.is_none()))
        .collect();
    assert_eq!(members, [(2, key(2), true), (1, key(1), true)]);
    assert_ne!(key(1), key(2));
    assert!(!plan.keys().any(|member| *member == plan.key));

    let (placed, pin) = plan.place(None, |_| 0);
    assert_eq!((pin, &placed.key), (Some(1), &key(1)));
    assert!(routes.leg_down(&placed).is_none());
    proxies.network.shutdown().await;
}

#[tokio::test]
async fn a_missing_egress_has_a_typed_route_reason() {
    use crate::torrents::sessions::{NetworkRoutes, TorrentRouteReason};
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    store_route_unchecked(
        &db,
        &proxies,
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 999,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .await;
    let routes = NetworkRoutes::new(Arc::clone(&proxies), Duration::from_secs(30));
    assert_eq!(
        routes.plan(TorrentScope::Default).down.unwrap().reason,
        TorrentRouteReason::LegMissing
    );
    proxies.network.shutdown().await;
}

// A route whose proxy is disabled, alone or as a hop of its chain, plans
// down saying which, from the reload after the save; enabled again, it is
// up again.
#[tokio::test]
async fn a_route_on_a_disabled_proxy_plans_down() {
    use crate::proxies::Rung;
    use crate::torrents::sessions::{NetworkRoutes, RouteFailure};

    let db = Database::open_in_memory().unwrap();
    for id in 1..=2 {
        db.save_proxy_profile(&crate::proxies::ProxyProfile {
            id,
            name: format!("exit-{id}"),
            ..loopback_proxy(1080)
        })
        .unwrap();
    }
    let set_enabled = |enabled: bool| {
        let mut profile = db
            .list_proxy_profiles()
            .unwrap()
            .into_iter()
            .find(|profile| profile.id == 1)
            .unwrap();
        profile.enabled = enabled;
        profile.revision += 1;
        db.save_proxy_profile(&profile).unwrap();
    };
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = NetworkRoutes::new(Arc::clone(&proxies), Duration::from_secs(30));
    let down_on = async |rung: Rung| {
        db.save_proxy_routing_policy(
            Consumer::Torrent(TorrentScope::Default),
            &RoutingPolicy {
                legs: vec![RouteLeg {
                    egress_id: SYSTEM_EGRESS_ID,
                    weight: 100,
                    path: LegPath::Ladder {
                        rungs: vec![rung],
                        direct_fallback: false,
                    },
                }],
                ..Default::default()
            },
        )
        .unwrap();
        proxies.reload().await.unwrap();
        routes.plan(TorrentScope::Default).down
    };
    let one = || Rung::Proxy { id: 1 };
    let chain = || Rung::Chain { ids: vec![2, 1] };

    assert_eq!(down_on(one()).await, None);
    assert_eq!(down_on(chain()).await, None);
    set_enabled(false);
    proxies.reload().await.unwrap();
    let disabled = Some(RouteFailure::leg_down("proxy 1 is disabled".into()));
    assert_eq!(down_on(one()).await, disabled);
    assert_eq!(down_on(chain()).await, disabled);
    set_enabled(true);
    proxies.reload().await.unwrap();
    assert_eq!(down_on(one()).await, None);
    assert_eq!(down_on(chain()).await, None);
    proxies.network.shutdown().await;
}

// A route is planned from the policies the proxy runtime holds: a boot
// that restores and attaches its torrents reads no route row, and a policy
// saved is planned once the runtime reloads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routes_are_planned_from_the_runtime_not_the_route_rows() {
    use crate::torrents::sessions::NetworkRoutes;
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let counted = Arc::new(Counted {
        routes: NetworkRoutes::new(Arc::clone(&proxies), Duration::from_secs(30)),
        plans: AtomicUsize::new(0),
    });
    let routes: Arc<dyn Routes> = counted.clone();
    let service = service_over(&db, dir.path(), Arc::clone(&routes), job_env(), set_clock());
    service.open().await.unwrap();
    let mut jobs = Vec::new();
    for seed in 0..3 {
        let name = format!("planned-{seed}");
        jobs.push(
            service
                .add_opened(request(AddSource::Metainfo(plain(&name, seed))))
                .await
                .unwrap(),
        );
    }
    service.tick().await;
    service.stop().await;

    let reads = || db.proxy_route_reads.load(Ordering::Relaxed);
    let plans = || counted.plans.load(Ordering::Relaxed);
    let before = reads();
    let planned_before = plans();
    let again = service_over(&db, dir.path(), Arc::clone(&routes), job_env(), set_clock());
    reopen(&again).await;
    let live = again.inner.live.lock().await;
    assert!(jobs.iter().all(|job_id| live.contains_key(job_id)));
    drop(live);
    assert!(
        plans() - planned_before >= jobs.len(),
        "the boot planned no route"
    );
    assert_eq!(reads() - before, 0, "the boot read a route row");

    // Saved through an egress the host has no interface for, the route is
    // planned the old way until the runtime reloads, then down on that leg.
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
    let planned_before = plans();
    let planned = || routes.plan(TorrentScope::Default);
    assert_eq!(planned().egress_id, SYSTEM_EGRESS_ID);
    proxies.reload().await.unwrap();
    assert_eq!(planned().egress_id, absent.id);
    assert!(planned().down.is_some());
    assert_eq!(plans() - planned_before, 3);
    assert_eq!(reads() - before, 0, "a plan read a route row");
    again.stop().await;
    proxies.network.shutdown().await;
}

// A torrent added by URL is fetched over its route's one leg: up, the
// request reaches the site; down, the add fails for the route being down,
// and nothing is asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_is_fetched_over_the_routes_one_leg_or_not_at_all() {
    url_route(None, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_takes_its_matched_rules_route_before_its_categorys() {
    url_route(Some("tv"), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_without_a_matched_rule_takes_its_categorys_route() {
    url_route(Some("tv"), false).await;
}

async fn url_route(category: Option<&str>, matched: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    let rule = db
        .insert_tracker_rule(
            &TrackerRule {
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: if matched {
                    "127.0.0.1"
                } else {
                    "unmatched.test"
                }
                .into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Default::default(),
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    service.open().await.unwrap();
    service.reload_tracker_rules().await.unwrap();
    let scope = if matched {
        TorrentScope::TrackerRule(rule.id)
    } else {
        service.scope_of(category, None)
    };
    let category_config = &service.settings().categories[0];
    db.insert_category(category_config).unwrap();
    let category_scope = TorrentScope::Category(category_config.id);
    db.save_proxy_routing_policy(
        Consumer::Torrent(category_scope),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    proxies.reload().await.unwrap();

    let site = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/a.torrent", site.local_addr().unwrap())).unwrap();
    let body = b"d4:infod4:name1:aee";
    let served = async {
        let (mut stream, _) = site.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|end| end == b"\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            assert!(read > 0, "the request ended before it was read");
            request.extend_from_slice(&buf[..read]);
        }
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    };
    let (fetched, ()) =
        tokio::try_join!(service.fetch_url(&proxies, category, &url, None), async {
            served.await;
            Ok::<_, String>(())
        })
        .unwrap();
    assert_eq!(fetched, body);

    // The route's one leg on an interface the host does not have.
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
    db.save_proxy_routing_policy(
        Consumer::Torrent(scope),
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
    proxies.reload().await.unwrap();
    let down = service
        .inner
        .sessions
        .routes()
        .plan(scope)
        .down
        .expect("the leg is down");

    if matched {
        assert!(
            service
                .inner
                .sessions
                .routes()
                .plan(category_scope)
                .down
                .is_none()
        );
    } else {
        assert!(
            service
                .inner
                .sessions
                .routes()
                .plan(TorrentScope::TrackerRule(rule.id))
                .down
                .is_none()
        );
    }
    let refused = tokio::select! {
        refused = service.fetch_url(&proxies, category, &url, None) => refused,
        asked = site.accept() => panic!("the down route reached the site: {asked:?}"),
    };
    assert_eq!(
        down.reason,
        crate::torrents::sessions::TorrentRouteReason::LegDown
    );
    assert_eq!(refused, Err(format!("route down: {}", down.message())));
    let site = site.into_std().unwrap();
    site.set_nonblocking(true).unwrap();
    let asked = site.accept();
    assert!(
        matches!(&asked, Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "the site was asked: {asked:?}"
    );
    service.stop().await;
}

// A torrent route stored with two legs is no route a torrent may run on: it
// reads down for the refusal, the route view shows why, and its torrent is
// held. Saved again as its one leg, the route and the torrent are released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_route_with_two_exits_holds_until_it_is_edited_to_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&loopback_proxy(1)).unwrap();
    let through = |weight| RouteLeg {
        egress_id: SYSTEM_EGRESS_ID,
        weight,
        path: LegPath::Ladder {
            rungs: vec![crate::proxies::Rung::Proxy { id: 1 }],
            direct_fallback: false,
        },
    };
    let consumer = Consumer::Torrent(TorrentScope::Default);
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    store_route_unchecked(
        &db,
        &proxies,
        consumer,
        &RoutingPolicy {
            legs: vec![
                through(50),
                RouteLeg {
                    weight: 50,
                    path: LegPath::Direct,
                    ..through(50)
                },
            ],
            ..Default::default()
        },
    )
    .await;
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    service.open().await.unwrap();
    let job_id = service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    service.tick().await;

    let refusal = crate::torrents::sessions::TorrentRouteReason::RouteBroken(
        crate::proxies::TorrentRouteFault::Legs,
    );
    assert_eq!(
        service.inner.live.lock().await[&job_id].held,
        Some(Held::RouteDown(
            crate::torrents::sessions::TorrentRouteReason::RouteBroken(
                crate::proxies::TorrentRouteFault::Legs
            )
            .into()
        ))
    );
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(refusal))
    );
    let listed = service.routes().await;
    let held = listed
        .iter()
        .find(|route| {
            route
                .scopes
                .iter()
                .any(|(scope, _)| *scope == TorrentScope::Default)
        })
        .unwrap();
    assert_eq!(
        held.down.as_ref().map(|failure| failure.reason),
        Some(refusal)
    );
    assert_eq!(
        held.session
            .as_ref()
            .unwrap()
            .held
            .as_ref()
            .map(|failure| failure.reason),
        Some(refusal)
    );

    db.save_proxy_routing_policy(
        consumer,
        &RoutingPolicy {
            legs: vec![through(100)],
            ..Default::default()
        },
    )
    .unwrap();
    proxies.reload().await.unwrap();
    service.tick().await;

    let live = service.inner.live.lock().await;
    assert_eq!(live[&job_id].held, None);
    assert!(live[&job_id].attached.is_some());
    let key = live[&job_id].route.clone().unwrap();
    drop(live);
    let listed = service.routes().await;
    let route = listed.iter().find(|route| route.key == key).unwrap();
    assert_eq!(route.down, None);
    assert_eq!(route.session.as_ref().unwrap().held, None);
    service.stop().await;
}

// A listen port given while a torrent runs reopens its route there at the
// next tick, the torrent going through the new session; settings that change
// nothing a session opens with leave it be.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_listen_port_reopens_the_torrents_route_on_it() {
    let rig = rig(set_clock());
    unbound_leg(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let listening = || async {
        let views = rig.service.inner.sessions.views().await;
        assert_eq!(views.len(), 1);
        views[0].listen.map(|listen| listen.port())
    };
    let before = session_of(&rig.service, job_id).await;
    assert_eq!(listening().await, Some(6881));

    let mut settings = rig.service.settings();
    settings.torrent.max_peers_total = 12;
    rig.service.set_settings(settings.clone()).await;
    rig.service.tick().await;
    assert!(Arc::ptr_eq(
        &before,
        &session_of(&rig.service, job_id).await
    ));

    settings.torrent.base_port = 41_000;
    rig.service.set_settings(settings).await;
    rig.service.tick().await;
    let after = session_of(&rig.service, job_id).await;
    assert!(!Arc::ptr_eq(&before, &after));
    before.cancellation_token().cancelled().await;
    assert_eq!(listening().await, Some(41_000));
    assert_eq!(
        after.listen_addr().map(|listen| listen.port()),
        Some(41_000)
    );
    assert!(
        after
            .get(TorrentIdOrHash::Id(
                handle_of(&rig.service, job_id).await.id()
            ))
            .is_some()
    );
    rig.service.stop().await;
}

// A listen port given while the tick runs reopens the route there at once:
// the settings change wakes the tick, and the paused clock never reaches the
// tick's next turn.
#[tokio::test(start_paused = true)]
async fn a_new_listen_port_reaches_the_route_without_waiting_for_the_tick() {
    let clock = Arc::new(RuntimeClock::at_unix_ms(EPOCH_MS));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    unbound_leg(&rig.routes);
    let mut ticks = rig.service.ticks();
    rig.service.start().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    // The tick's first turn, at its start, is behind it.
    ticks.wait_for(|ticks| *ticks > 0).await.unwrap();
    let before = session_of(&rig.service, job_id).await;
    assert_eq!(before.listen_addr().map(|listen| listen.port()), Some(6881));

    let changed = tokio::time::Instant::now();
    let base = *ticks.borrow_and_update();
    let mut settings = rig.service.settings();
    settings.torrent.base_port = 41_000;
    rig.service.set_settings(settings).await;
    ticks.wait_for(|ticks| *ticks > base).await.unwrap();
    assert!(tokio::time::Instant::now() < changed + TICK);
    let after = session_of(&rig.service, job_id).await;
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(
        after.listen_addr().map(|listen| listen.port()),
        Some(41_000)
    );
    rig.service.stop().await;
}

// A leg revoked under a torrent's session, as saving its proxy's profile
// again revokes it, wakes the tick: the torrent moves to a session opened on
// the leg the network gives its route now, and the revoked one stops.
#[tokio::test(start_paused = true)]
async fn a_torrent_whose_leg_is_revoked_moves_to_a_session_on_the_new_leg() {
    let clock = Arc::new(RuntimeClock::at_unix_ms(EPOCH_MS));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    let (db, network) = on_network(&rig.routes);
    let mut ticks = rig.service.ticks();
    rig.service.start().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    ticks.wait_for(|ticks| *ticks > 0).await.unwrap();
    let before = session_of(&rig.service, job_id).await;
    let stage = || {
        network
            .route(
                crate::proxies::Consumer::Torrent(TorrentScope::Default),
                8,
                std::time::Duration::from_secs(30),
            )
            .unwrap()
            .legs
            .read()
            .unwrap()[0]
            .stage
            .clone()
    };
    let old_stage = stage();

    save_again(&db, &network).await;
    assert!(old_stage.is_revoked());
    // The torrent's attachment names another session once the tick that the
    // revocation woke has moved it.
    let attached = || async {
        let live = rig.service.inner.live.lock().await;
        live[&job_id]
            .attached
            .as_ref()
            .map(|attached| Arc::clone(&attached.session))
    };
    loop {
        ticks.borrow_and_update();
        if attached()
            .await
            .is_some_and(|session| !Arc::ptr_eq(&session, &before))
        {
            break;
        }
        ticks.changed().await.unwrap();
    }
    let after = session_of(&rig.service, job_id).await;
    before.cancellation_token().cancelled().await;
    assert!(!stage().is_revoked());
    let views = rig.service.inner.sessions.views().await;
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].held, None);
    assert!(
        after
            .get(TorrentIdOrHash::Id(
                handle_of(&rig.service, job_id).await.id()
            ))
            .is_some()
    );
    rig.service.stop().await;
    network.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_past_the_session_limit_says_so() {
    use crate::torrents::sessions::MAX_SESSIONS;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    for id in 0..MAX_SESSIONS as u32 {
        rig.routes.route(TorrentScope::Category(100 + id), 100 + id);
        let plan = rig.routes.plan(TorrentScope::Category(100 + id));
        rig.service
            .inner
            .sessions
            .session(&plan, &crate::torrents::identity::IDENTITY.identity())
            .await
            .unwrap();
    }
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::SessionLimit))
    );
    rig.service.stop().await;
}

// A torrent whose route's session is being opened is queued for it, and
// says its route is opening.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_waiting_on_its_routes_session_reads_as_opening() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let service = rig.service.clone();
    let added = tokio::spawn(async move {
        service
            .add_opened(request(AddSource::Metainfo(plain("set", 1))))
            .await
            .unwrap()
    });
    asked.wait_for(|asked| *asked > before).await.unwrap();
    let job_id = rig.service.torrents(None).await[0].job_id;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.route_reason),
        (TorrentState::Queued, Some(TorrentRouteReason::Opening))
    );
    rig.routes.unanswered.send_replace(false);
    assert_eq!(added.await.unwrap(), job_id);
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().route_reason,
        None
    );
    rig.service.stop().await;
}

// A leg's sockets that reach no one, each one asked for counted.
#[derive(Default)]
struct Watched {
    asked: AtomicUsize,
}

impl weaver_torrent::SocketFactory for Watched {
    fn connect_tcp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> weaver_torrent::network::BoxFuture<
        'a,
        std::io::Result<(
            weaver_torrent::network::BoxAsyncReadVectored,
            weaver_torrent::network::BoxAsyncWrite,
        )>,
    > {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::Unsupported)) })
    }

    fn listen_tcp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> weaver_torrent::network::BoxFuture<
        'a,
        std::io::Result<Box<dyn weaver_torrent::StreamListener>>,
    > {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::Unsupported)) })
    }

    fn bind_udp<'a>(
        &'a self,
        _: std::net::SocketAddr,
    ) -> weaver_torrent::network::BoxFuture<
        'a,
        std::io::Result<Arc<dyn weaver_torrent::network::DatagramSocket>>,
    > {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::Unsupported)) })
    }
}

// An add is answered while its route's session is still opening: the torrent
// reads as opening, its route not yet open, and once the route opens it joins the session by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_is_answered_before_its_routes_session_opens() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();

    // The route's open is held unanswered, and the add still answers.
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.route_reason),
        (TorrentState::Queued, Some(TorrentRouteReason::Opening))
    );
    assert!(rig.service.attaching(job_id).await);
    asked.wait_for(|asked| *asked > before).await.unwrap();
    assert_eq!(rig.routes.opened.load(Ordering::SeqCst), 0);

    rig.routes.unanswered.send_replace(false);
    rig.service.opened(job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.route_reason, None);
    handle_of(&rig.service, job_id).await;
    rig.service.stop().await;
}

// An add whose route then fails to open is answered as opening, and goes on
// to say why the route did not open: nothing of it went anywhere.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_whose_route_fails_to_open_says_why_and_sends_nothing() {
    let rig = rig(set_clock());
    let watched = Arc::new(Watched::default());
    rig.routes.set_leg(
        crate::torrents::net::LegKind::Ladder,
        Arc::clone(&watched) as Arc<dyn weaver_torrent::SocketFactory>,
    );
    rig.service.open().await.unwrap();
    rig.routes
        .set_refused(Some("No handshake within 30 seconds."));
    rig.routes.unanswered.send_replace(true);

    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().route_reason,
        Some(TorrentRouteReason::Opening)
    );

    rig.routes.unanswered.send_replace(false);
    rig.service.opened(job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (
            Some("No handshake within 30 seconds."),
            Some(TorrentRouteReason::LegOpenFailed)
        )
    );
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .attached
            .is_none()
    );
    assert_eq!(watched.asked.load(Ordering::SeqCst), 0);
    rig.service.stop().await;
}

// A category move to a route whose session is not open yet is answered at
// once: the torrent reads as its new route, opening, and joins that route's
// session by itself once it opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_category_move_is_answered_before_its_new_routes_session_opens() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.routes.route(TorrentScope::Category(1), 9);
    let new = route_key(&rig, TorrentScope::Category(1));
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();

    // The new route's open is held unanswered, and the move still answers.
    rig.service
        .set_category(job_id, Some("tv".into()))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.route.as_deref(), view.route_reason),
        (Some(new.as_str()), Some(TorrentRouteReason::Opening))
    );
    assert!(rig.service.attaching(job_id).await);
    asked.wait_for(|asked| *asked > before).await.unwrap();

    rig.routes.unanswered.send_replace(false);
    rig.service.opened(job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.route.as_deref(), view.route_reason),
        (Some(new.as_str()), None)
    );
    handle_of(&rig.service, job_id).await;
    rig.service.stop().await;
}

// A torrent paused while its route's session opens joins that session
// paused, as one paused after it joined is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_paused_while_its_routes_session_opens_joins_it_paused() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();
    rig.service.pause(job_id).await.unwrap();

    rig.routes.unanswered.send_replace(false);
    rig.service.opened(job_id).await;
    let handle = handle_of(&rig.service, job_id).await;
    assert!(handle.is_paused());
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Paused
    );
    rig.service.stop().await;
}

// A category moved while the torrent's route's session opens is not opened
// for again in the meantime: the one wait goes on, and once its open ends
// the torrent waits for its new route instead, and joins that route's
// session alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_category_moved_while_its_routes_session_opens_joins_the_new_route_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();

    rig.routes.route(TorrentScope::Category(1), 9);
    let new = route_key(&rig, TorrentScope::Category(1));
    rig.service
        .set_category(job_id, Some("tv".into()))
        .await
        .unwrap();
    rig.service
        .set_category(job_id, Some("tv".into()))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.route.as_deref(), view.route_reason),
        (Some(new.as_str()), Some(TorrentRouteReason::Opening))
    );
    assert_eq!(*asked.borrow(), before + 1, "a second open was asked for");

    rig.routes.unanswered.send_replace(false);
    rig.service.opened(job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.route.as_deref(), view.route_reason),
        (Some(new.as_str()), None)
    );
    handle_of(&rig.service, job_id).await;
    // The route it left opened a session, which never took it.
    let running = rig.service.inner.sessions.running().await;
    let held: Vec<(String, usize)> = running
        .iter()
        .map(|(key, session)| {
            (
                key.as_str().to_owned(),
                session.with_torrents(|torrents| torrents.count()),
            )
        })
        .filter(|(_, count)| *count > 0)
        .collect();
    assert_eq!(held, [(new, 1)]);
    rig.service.stop().await;
}

// A recheck of a torrent held for its files is answered before its route's
// session opens; once it opens the torrent joins it and every piece passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recheck_of_a_held_torrent_is_answered_before_its_routes_session_opens() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;
    std::fs::rename(&away, &dir).unwrap();
    rig.routes.unanswered.send_replace(true);

    again.recheck(job_id).await.unwrap();
    assert!(again.attaching(job_id).await);
    assert_eq!(
        again.torrent(job_id).await.unwrap().route_reason,
        Some(TorrentRouteReason::Opening)
    );

    rig.routes.unanswered.send_replace(false);
    again.opened(job_id).await;
    runs_with_every_piece(&again, job_id).await;
    again.stop().await;
}

// A resume that finds a held torrent's files is answered before its route's
// session opens; once it opens the torrent runs with every piece.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resume_that_finds_the_files_is_answered_before_its_routes_session_opens() {
    let rig = rig(set_clock());
    let (job_id, dir, away) = gone_at_restart(&rig).await;
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    held_for_its_files(&again, job_id, &dir).await;
    std::fs::rename(&away, &dir).unwrap();
    rig.routes.unanswered.send_replace(true);

    again.resume(job_id).await.unwrap();
    assert!(again.attaching(job_id).await);

    rig.routes.unanswered.send_replace(false);
    again.opened(job_id).await;
    runs_with_every_piece(&again, job_id).await;
    again.stop().await;
}

// The id the default route's session gives the next torrent it takes: 0
// when it has taken none before.
async fn next_taken(rig: &Rig) -> usize {
    let next = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("next", 2))))
        .await
        .unwrap();
    handle_of(&rig.service, next).await.id()
}

// A torrent removed with its files while its new route's session opens is
// never put in that session, and nothing of it is made on disk again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_removed_while_its_new_routes_session_opens_never_joins_it() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;

    rig.routes.route(TorrentScope::Default, 2);
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let service = rig.service.clone();
    let moving = tokio::spawn(async move { service.follow_routes().await });
    asked.wait_for(|asked| *asked > before).await.unwrap();
    rig.service.remove(job_id, true, false).await.unwrap();
    rig.routes.unanswered.send_replace(false);
    moving.await.unwrap();

    assert_eq!(files_under(&dir), Vec::<PathBuf>::new());
    assert!(rig.service.torrent(job_id).await.is_none());
    assert_eq!(
        next_taken(&rig).await,
        0,
        "the removed torrent joined the session"
    );
    rig.service.stop().await;
}

// A torrent removed with its files before its new route's session opens
// never joins it either, and nothing of it is made on disk again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_removed_before_its_new_routes_session_opens_never_joins_it() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;

    rig.routes.route(TorrentScope::Default, 2);
    rig.service.remove(job_id, true, false).await.unwrap();
    rig.service.follow_routes().await;

    assert_eq!(files_under(&dir), Vec::<PathBuf>::new());
    assert!(rig.service.torrent(job_id).await.is_none());
    assert_eq!(
        next_taken(&rig).await,
        0,
        "the removed torrent joined the session"
    );
    rig.service.stop().await;
}

// Every torrent moved to another route reads as that route, opening, as soon
// as the move begins: none waits on the torrents moved before it to read so.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_moved_to_a_new_route_read_as_it_at_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut jobs = Vec::new();
    for seed in 1..=3 {
        jobs.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("moved-{seed}"),
                    seed,
                ))))
                .await
                .unwrap(),
        );
    }
    let old = rig.service.torrent(jobs[0]).await.unwrap().route.unwrap();

    rig.routes.route(TorrentScope::Default, 2);
    let new = rig
        .service
        .inner
        .sessions
        .routes()
        .plan(TorrentScope::Default)
        .key;
    assert_ne!(old, new.as_str());
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let service = rig.service.clone();
    let moving = tokio::spawn(async move { service.follow_routes().await });
    // The first torrent's attach waits on the new route's session.
    asked.wait_for(|asked| *asked > before).await.unwrap();
    for job_id in &jobs {
        let view = rig.service.torrent(*job_id).await.unwrap();
        assert_eq!(
            (view.route.as_deref(), view.state, view.route_reason),
            (
                Some(new.as_str()),
                TorrentState::Queued,
                Some(TorrentRouteReason::Opening)
            ),
            "torrent {job_id:?}"
        );
    }
    rig.routes.unanswered.send_replace(false);
    moving.await.unwrap();
    for job_id in &jobs {
        let view = rig.service.torrent(*job_id).await.unwrap();
        assert_eq!(
            (view.route.as_deref(), view.route_reason),
            (Some(new.as_str()), None)
        );
    }
    rig.service.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_that_cannot_be_built_says_why() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.set_refused(Some("no listening port"));
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let said = |view: TorrentView| {
        assert_eq!(view.state, TorrentState::Stalled);
        assert_eq!(
            (view.detail.as_deref(), view.route_reason),
            (
                Some("no listening port"),
                Some(TorrentRouteReason::LegOpenFailed)
            )
        );
    };
    said(rig.service.torrent(job_id).await.unwrap());

    // While its route opens again it still says why, until the open says
    // otherwise.
    rig.routes.unanswered.send_replace(true);
    let mut asked = rig.routes.asked.subscribe();
    let before = *asked.borrow_and_update();
    let service = rig.service.clone();
    let tick = tokio::spawn(async move { service.tick().await });
    asked.wait_for(|asked| *asked > before).await.unwrap();
    said(rig.service.torrent(job_id).await.unwrap());
    rig.routes.unanswered.send_replace(false);
    tick.await.unwrap();
    said(rig.service.torrent(job_id).await.unwrap());
}

// A route whose session cannot be built is tried once a pass, however many
// torrents wait on it: each says why, and the pass does not open the route
// again for each of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failing_route_is_opened_once_a_pass_for_all_its_torrents() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes
        .set_refused(Some("No handshake within 30 seconds."));
    let mut jobs = Vec::new();
    for seed in 1..=3 {
        jobs.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("held-{seed}"),
                    seed,
                ))))
                .await
                .unwrap(),
        );
    }
    let before = *rig.routes.asked.borrow();
    rig.service.tick().await;
    assert_eq!(*rig.routes.asked.borrow() - before, 1);
    for job_id in jobs {
        let view = rig.service.torrent(job_id).await.unwrap();
        assert_eq!(
            (view.state, view.detail.as_deref(), view.route_reason),
            (
                TorrentState::Stalled,
                Some("No handshake within 30 seconds."),
                Some(TorrentRouteReason::LegOpenFailed)
            ),
            "torrent {job_id:?}"
        );
    }
    rig.service.stop().await;
}

// A queue whose network is `proxies`', published to `service`: the
// torrents it runs count on their legs of that network's routes.
fn counted_on(service: &TorrentService, proxies: &Arc<crate::proxies::ProxyRuntime>) {
    let queue = queue();
    queue.set_proxy_runtime(Arc::clone(proxies));
    service.publish_to(queue);
}

// The default torrent leg as a network snapshot reads it now: whether it
// is live, and its open and opening connections.
fn default_torrent_leg(network: &NetworkRuntime) -> (bool, u64, u64) {
    let snapshot = crate::proxies::network_metrics::snapshot(network);
    let leg = snapshot
        .legs
        .iter()
        .find(|leg| {
            leg.consumer == Consumer::Torrent(TorrentScope::Default).key()
                && leg.position == crate::torrents::sessions::TORRENT_LEG
        })
        .expect("the default torrent leg");
    (leg.live, leg.open, leg.opening)
}

// Two files of whole pieces: a sharer holding only the first and the rig's
// torrent connect, and neither has anything of the second, so neither only
// uploads and the connection stays up. Returns the sharer's session and the
// rig's torrent, attached on the default leg of `proxies`' network and told
// of the sharer by a loopback tracker.
async fn connected_to_a_sharer(
    rig: &Rig,
    proxies: &Arc<crate::proxies::ProxyRuntime>,
    seeds: &std::path::Path,
) -> (JobId, Arc<Session>, Arc<ManagedTorrent>) {
    let (first, second) = (vec![1u8; 2048], vec![2u8; 2048]);
    let files = [
        ("pair/a.bin", first.as_slice()),
        ("pair/b.bin", second.as_slice()),
    ];
    let bytes = metainfo("pair", &files, 1024, false);
    let (session, _) = sharing(seeds, &bytes, &files[..1]).await;
    let std::net::SocketAddr::V4(peer) = session.listen_addr().unwrap() else {
        panic!("the sharer listens on IPv4 loopback");
    };
    let url = tracker(peer).await;
    on_loopback(&rig.routes);
    counted_on(&rig.service, proxies);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let torrent = handle_of(&rig.service, job_id).await;
    (job_id, session, torrent)
}

// Until `torrent`'s run holds an established peer, read from its gauge
// after each turn the other workers run the sessions in; returns how many
// it holds. The engine publishes no wake for a peer becoming established,
// and adding one would put a lock on that path.
async fn established(torrent: &ManagedTorrent) -> u32 {
    loop {
        let held = torrent.gauges().map_or(0, |run| run.peers().live);
        if held > 0 {
            return held;
        }
        tokio::task::yield_now().await;
    }
}

// A torrent's leg shows the peers the torrent holds the instant a network
// snapshot is taken: no tick ran since the peer was established.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_leg_shows_an_established_peer_the_instant_it_is_read() {
    let rig = rig(set_clock());
    let proxies =
        crate::proxies::ProxyRuntime::new(rig.db.clone(), tokio::runtime::Handle::current())
            .unwrap();
    let seeds = tempfile::tempdir().unwrap();
    assert_eq!(default_torrent_leg(&proxies.network), (false, 0, 0));
    let (_, sharer, torrent) = connected_to_a_sharer(&rig, &proxies, seeds.path()).await;

    let held = established(&torrent).await;
    assert_eq!(
        default_torrent_leg(&proxies.network),
        (true, u64::from(held), 0)
    );
    rig.service.stop().await;
    sharer.stop().await;
    proxies.network.shutdown().await;
}

// A removed torrent leaves nothing on its leg: the next snapshot reads the
// leg empty, though the tick before the removal saw the torrent's peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_torrent_leaves_its_leg_empty_at_the_next_snapshot() {
    let rig = rig(set_clock());
    let proxies =
        crate::proxies::ProxyRuntime::new(rig.db.clone(), tokio::runtime::Handle::current())
            .unwrap();
    let seeds = tempfile::tempdir().unwrap();
    let (job_id, sharer, torrent) = connected_to_a_sharer(&rig, &proxies, seeds.path()).await;
    established(&torrent).await;
    rig.service.tick().await;

    rig.service.remove(job_id, false, false).await.unwrap();
    assert_eq!(default_torrent_leg(&proxies.network), (false, 0, 0));
    rig.service.stop().await;
    sharer.stop().await;
    proxies.network.shutdown().await;
}

// A route opened by a torrent under no rule names Weaver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_opened_by_a_torrent_under_no_rule_names_weaver() {
    use crate::torrents::identity::IDENTITY;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let plain = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("plain", 1))))
        .await
        .unwrap();
    let identity = session_of(&rig.service, plain).await.identity().clone();
    assert_eq!(&identity.peer_id.0[..8], IDENTITY.prefix());
    assert_eq!(identity.user_agent, IDENTITY.user_agent());
    assert_eq!(identity.version, IDENTITY.version());
}

// A torrent under a tracker rule that routes its torrents itself joins the
// rule's session ahead of its category's; one under no rule joins its
// category's. Once the rule's route is off, its torrent moves to its
// category's session at the next tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_route_carries_its_torrents_ahead_of_their_categorys() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "*.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Default::default(),
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.routes.route(TorrentScope::Category(1), 9);
    rig.routes.route(TorrentScope::TrackerRule(rule.id), 7);
    let key = |scope| rig.routes.plan(scope).key.as_str().to_owned();
    // Paused, so nothing announces.
    let in_tv = |metainfo| AddRequest {
        category: Some("tv".into()),
        paused: true,
        ..request(AddSource::Metainfo(metainfo))
    };
    let ruled = rig
        .service
        .add_opened(in_tv(announcing(
            &plain("ruled", 1),
            "http://tracker.test/announce",
        )))
        .await
        .unwrap();
    let unruled = rig
        .service
        .add_opened(in_tv(plain("unruled", 2)))
        .await
        .unwrap();
    let route = async |job_id| rig.service.torrent(job_id).await.unwrap().route;

    assert_eq!(
        route(ruled).await,
        Some(key(TorrentScope::TrackerRule(rule.id)))
    );
    assert_eq!(route(unruled).await, Some(key(TorrentScope::Category(1))));

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Default::default(),
                forced: false,
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    assert_eq!(route(ruled).await, Some(key(TorrentScope::Category(1))));
    rig.service.stop().await;
}

// The pool the default scope routes through in the pool tests.
const POOL: u32 = 7;

async fn add_plain(service: &TorrentService, name: &str, seed: u8) -> JobId {
    service
        .add_opened(request(AddSource::Metainfo(plain(name, seed))))
        .await
        .unwrap()
}

// Torrents on a pool route go to the member with the fewest, the lowest id
// of those, whatever the pool's order: the first to 1, the second to 2, the
// third to 1. Each member runs its own session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_on_a_pool_route_go_to_the_least_loaded_member() {
    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[2, 1]);
    rig.service.open().await.unwrap();

    let first = add_plain(&rig.service, "first", 1).await;
    let second = add_plain(&rig.service, "second", 2).await;
    let third = add_plain(&rig.service, "third", 3).await;

    assert_eq!(
        pins(&rig.service, &rig.db, &[first, second, third]).await,
        [Some(1), Some(2), Some(1)]
    );
    let running = rig.service.inner.sessions.running().await;
    let keys: BTreeSet<_> = running.keys().cloned().collect();
    assert_eq!(
        keys,
        BTreeSet::from([pool_member_key(1), pool_member_key(2)])
    );
    rig.service.stop().await;
}

// A torrent keeps its member while that member is up: another member come
// up with no torrents on it draws none away.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pinned_torrent_stays_on_its_member_when_another_comes_up_emptier() {
    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[1, 2]);
    rig.routes.set_member_down(2, true);
    rig.service.open().await.unwrap();
    let first = add_plain(&rig.service, "first", 1).await;
    let second = add_plain(&rig.service, "second", 2).await;
    assert_eq!(
        pins(&rig.service, &rig.db, &[first, second]).await,
        [Some(1), Some(1)]
    );

    rig.routes.set_member_down(2, false);
    rig.service.follow_routes().await;

    assert_eq!(
        pins(&rig.service, &rig.db, &[first, second]).await,
        [Some(1), Some(1)]
    );
    let live = rig.service.inner.live.lock().await;
    assert!(live[&first].attached.is_some() && live[&second].attached.is_some());
    drop(live);
    rig.service.stop().await;
}

// A member that fails moves its own torrents to the members still up, and
// no other torrent: the one on member 2 keeps its session. The failed member
// back up draws none of them back. With every member down, each torrent
// holds on its member for why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_member_moves_only_its_torrents_and_its_return_moves_none() {
    use crate::torrents::sessions::TorrentRouteReason;

    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[1, 2]);
    rig.service.open().await.unwrap();
    let jobs = [
        add_plain(&rig.service, "first", 1).await,
        add_plain(&rig.service, "second", 2).await,
        add_plain(&rig.service, "third", 3).await,
    ];
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(1), Some(2), Some(1)]
    );
    let session = |live: &BTreeMap<JobId, Live>, job_id: JobId| {
        Arc::clone(&live[&job_id].attached.as_ref().unwrap().session)
    };
    let kept = session(&*rig.service.inner.live.lock().await, jobs[1]);

    rig.routes.set_member_down(1, true);
    rig.service.follow_routes().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(2), Some(2), Some(2)]
    );
    let live = rig.service.inner.live.lock().await;
    assert!(Arc::ptr_eq(&session(&live, jobs[1]), &kept));
    assert!(Arc::ptr_eq(&session(&live, jobs[0]), &kept));
    assert!(Arc::ptr_eq(&session(&live, jobs[2]), &kept));
    drop(live);

    rig.routes.set_member_down(1, false);
    rig.service.follow_routes().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(2), Some(2), Some(2)]
    );
    let live = rig.service.inner.live.lock().await;
    assert!(
        jobs.iter()
            .all(|job_id| Arc::ptr_eq(&session(&live, *job_id), &kept))
    );
    drop(live);

    rig.routes.set_member_down(1, true);
    rig.routes.set_member_down(2, true);
    rig.service.follow_routes().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(2), Some(2), Some(2)]
    );
    for job_id in jobs {
        let view = rig.service.torrent(job_id).await.unwrap();
        assert_eq!(view.route_reason, Some(TorrentRouteReason::LegDown));
    }
    rig.service.stop().await;
}

// A member taken out of the pool moves its own torrents to the members left,
// and no other torrent: the one on member 2 keeps its session, which takes
// the moved ones in. The removed member's session, idle, closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_member_removed_from_the_pool_moves_only_its_torrents() {
    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[1, 2]);
    rig.service.open().await.unwrap();
    let jobs = [
        add_plain(&rig.service, "first", 1).await,
        add_plain(&rig.service, "second", 2).await,
        add_plain(&rig.service, "third", 3).await,
    ];
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(1), Some(2), Some(1)]
    );
    let kept = session_of(&rig.service, jobs[1]).await;
    let removed = session_of(&rig.service, jobs[0]).await;

    rig.routes.route_pool(TorrentScope::Default, POOL, &[2]);
    rig.service.tick().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &jobs).await,
        [Some(2), Some(2), Some(2)]
    );
    for job_id in jobs {
        assert!(Arc::ptr_eq(&session_of(&rig.service, job_id).await, &kept));
    }
    assert!(!kept.cancellation_token().is_cancelled());
    removed.cancellation_token().cancelled().await;
    rig.service.stop().await;
}

// Proxy 3 in the default scope's pool and in the category's: a torrent from
// each pins to it and both run in one session, listed once for both scopes.
// The default scope routed elsewhere, the category's torrent keeps that
// session; the category routed elsewhere too, no torrent uses it and it
// closes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_proxy_in_two_pools_runs_one_session_for_both_scopes() {
    let tv = TorrentScope::Category(1);
    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[3]);
    rig.routes.route_pool(tv, POOL + 1, &[3]);
    rig.service.open().await.unwrap();
    let default = add_plain(&rig.service, "default", 1).await;
    let category = rig
        .service
        .add_opened(AddRequest {
            category: Some("tv".into()),
            ..request(AddSource::Metainfo(plain("category", 2)))
        })
        .await
        .unwrap();

    assert_eq!(
        pins(&rig.service, &rig.db, &[default, category]).await,
        [Some(3), Some(3)]
    );
    let shared = session_of(&rig.service, default).await;
    assert!(Arc::ptr_eq(
        &session_of(&rig.service, category).await,
        &shared
    ));
    let listed = rig.service.routes().await;
    let route = listed
        .iter()
        .find(|route| route.key == pool_member_key(3))
        .unwrap();
    let scopes: Vec<_> = route.scopes.iter().map(|(scope, _)| *scope).collect();
    assert_eq!(scopes.len(), 2);
    assert!(scopes.contains(&TorrentScope::Default) && scopes.contains(&tv));
    assert!(route.session.is_some());

    rig.routes.route(TorrentScope::Default, 5);
    rig.service.tick().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &[default, category]).await,
        [None, Some(3)]
    );
    assert!(Arc::ptr_eq(
        &session_of(&rig.service, category).await,
        &shared
    ));
    assert!(!shared.cancellation_token().is_cancelled());

    rig.routes.route(tv, 6);
    rig.service.tick().await;
    assert_eq!(
        pins(&rig.service, &rig.db, &[default, category]).await,
        [None, None]
    );
    shared.cancellation_token().cancelled().await;
    assert!(
        !rig.service
            .inner
            .sessions
            .running()
            .await
            .contains_key(&pool_member_key(3))
    );
    rig.service.stop().await;
}

// A torrent's member is stored: restarted, it runs on that member though
// another is emptier and lower. Its route no longer through a pool, the pin
// is cleared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pin_survives_a_restart_and_clears_when_the_route_leaves_the_pool() {
    let rig = rig(set_clock());
    rig.routes.route_pool(TorrentScope::Default, POOL, &[1, 2]);
    rig.routes.set_member_down(1, true);
    rig.service.open().await.unwrap();
    let job_id = add_plain(&rig.service, "set", 1).await;
    assert_eq!(pins(&rig.service, &rig.db, &[job_id]).await, [Some(2)]);
    rig.service.stop().await;

    rig.routes.set_member_down(1, false);
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(pins(&again, &rig.db, &[job_id]).await, [Some(2)]);
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());

    rig.routes.route(TorrentScope::Default, 5);
    again.follow_routes().await;
    assert_eq!(pins(&again, &rig.db, &[job_id]).await, [None]);
    assert_eq!(
        again.inner.live.lock().await[&job_id].route,
        Some(rig.routes.plan(TorrentScope::Default).key)
    );
    again.stop().await;
}

// The routes taking a saved configuration again, as a profile saved or
// enabled has them do, wakes the tick at once, not at its next interval.
#[tokio::test]
async fn the_routes_reloading_wakes_the_tick() {
    let rig = rig(set_clock());
    let mut reloads = rig.service.inner.sessions.routes().reloads();
    let woken = rig.service.woken(&mut reloads);
    tokio::pin!(woken);
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(woken.as_mut().poll(&mut cx).is_pending());
    rig.routes.reloads.send_modify(|reloads| *reloads += 1);
    assert!(woken.as_mut().poll(&mut cx).is_ready());
}
