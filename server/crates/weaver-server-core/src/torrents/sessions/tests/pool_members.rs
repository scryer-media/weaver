// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Sessions on a pool route's members, on a network whose pools are edited
//! under them: an edit reaches only the sessions on the members it changed,
//! and one proxy on one egress runs one session, whichever pools hold it.

use super::*;
use crate::categories::CategoryConfig;
use crate::proxies::{EgressInterface, ProxyKind, ProxyPool, ProxyProfile};

// The category whose torrents route on their own.
const TV: TorrentScope = TorrentScope::Category(1);

// Proxies 1 to 3, each at a loopback port of its own, a category routed on
// its own, and the default scope's route on a network whose one leg is a pool
// of `members`: the legs `routes` opens are that route's, each held to its
// member.
fn on_pool_network(
    routes: &FakeRoutes,
    members: &[u32],
) -> (Database, Arc<NetworkRuntime>, ProxyPool) {
    let db = Database::open_in_memory().unwrap();
    for id in 1..=3 {
        db.save_proxy_profile(&ProxyProfile {
            id,
            name: format!("exit-{id}"),
            ..loopback_proxy(1000 + id as u16)
        })
        .unwrap();
    }
    db.insert_category(&CategoryConfig {
        upload_cap: Default::default(),
        id: 1,
        name: "tv".into(),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route: true,
        unpack: None,
    })
    .unwrap();
    let pool = add_pool(&db, members);
    route_through(
        &db,
        TorrentScope::Default,
        SYSTEM_EGRESS_ID,
        &pool_ladder(pool.id),
    );
    let network = NetworkRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    routes.route_pool(TorrentScope::Default, pool.id, members);
    *routes.network.lock().unwrap() = Some(Arc::clone(&network));
    (db, network, pool)
}

fn add_pool(db: &Database, members: &[u32]) -> ProxyPool {
    db.create_proxy_pool(&ProxyPool {
        id: 0,
        name: "Region".into(),
        kind: ProxyKind::Socks5,
        member_ids: members.to_vec(),
        enabled: true,
    })
    .unwrap()
}

// `scope` routes through `path` on egress `egress_id`.
fn route_through(db: &Database, scope: TorrentScope, egress_id: u32, path: &LegPath) {
    db.save_proxy_routing_policy(
        Consumer::Torrent(scope),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id,
                weight: 100,
                path: path.clone(),
            }],
            ..Default::default()
        },
    )
    .unwrap();
}

// Proxy `id` as it is saved now, changed by `edit` and saved again.
fn edit_proxy(db: &Database, id: u32, edit: impl FnOnce(&mut ProxyProfile)) {
    let mut profile = db
        .list_proxy_profiles()
        .unwrap()
        .into_iter()
        .find(|profile| profile.id == id)
        .unwrap();
    edit(&mut profile);
    db.save_proxy_profile(&profile).unwrap();
}

// Proxy `id` saved again at another port: the edit its revision counts.
fn move_proxy(db: &Database, id: u32) {
    edit_proxy(db, id, |profile| {
        profile.port += 100;
        profile.revision += 1;
    });
}

// The pool saved with `members`, enabled or not.
async fn set_pool(
    db: &Database,
    network: &NetworkRuntime,
    pool: &ProxyPool,
    members: &[u32],
    enabled: bool,
) {
    db.update_proxy_pool(&ProxyPool {
        member_ids: members.to_vec(),
        enabled,
        ..pool.clone()
    })
    .unwrap();
    network.reload().await.unwrap();
}

fn sessions_on(routes: &Arc<FakeRoutes>, dir: &Path) -> Sessions {
    let sessions = Sessions::new(
        routes.clone(),
        env(dir),
        Database::open_in_memory().unwrap(),
    );
    sessions.set_config(any_port());
    sessions
}

// The session `scope`'s pool route runs on `member`, opened when it is not.
async fn open_on(
    sessions: &Sessions,
    routes: &FakeRoutes,
    scope: TorrentScope,
    member: u32,
) -> Arc<Session> {
    let (plan, pin) = routes.plan(scope).place(Some(member), |_| 0);
    assert_eq!(pin, Some(member));
    sessions.session(&plan, &IDENTITY.identity()).await.unwrap()
}

// The stage the open session of `member` runs its leg on.
async fn stage_of(sessions: &Sessions, member: u32) -> Arc<weaver_tunnel::pipe::Revocable> {
    match sessions.slots.lock().await.get(&pool_member_key(member)) {
        Some(Slot::Open { leg: Some(leg), .. }) => Arc::clone(&leg.stage),
        _ => panic!("member {member} has no session open on a leg"),
    }
}

// Every session in `kept` still runs, the very one it was, on a stage not
// revoked, and is not outdated.
async fn untouched(sessions: &Sessions, kept: &[(u32, &Arc<Session>)]) {
    let running = sessions.running().await;
    let outdated = sessions.outdated().await;
    for (member, session) in kept {
        let key = pool_member_key(*member);
        assert!(
            running
                .get(&key)
                .is_some_and(|open| Arc::ptr_eq(open, session)),
            "member {member}'s session was replaced or closed"
        );
        assert!(!outdated.contains(&key), "member {member} is outdated");
        assert!(!stage_of(sessions, *member).await.is_revoked());
        assert!(!session.cancellation_token().is_cancelled());
    }
}

// The sessions outdated once the follower has been woken for every one of
// `keys`.
async fn outdated_after_wake(sessions: &Sessions, keys: &BTreeSet<RouteKey>) -> BTreeSet<RouteKey> {
    loop {
        let outdated = sessions.outdated().await;
        if keys.is_subset(&outdated) {
            return outdated;
        }
        sessions.revoked().await;
    }
}

// A member added to the pool moves nothing and revokes nothing: the sessions
// on the others run on as they were, and the new member takes a session of
// its own.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_added_to_a_pool_moves_and_revokes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    let two = open_on(&sessions, &routes, TorrentScope::Default, 2).await;

    set_pool(&db, &network, &pool, &[1, 2, 3], true).await;
    routes.route_pool(TorrentScope::Default, pool.id, &[1, 2, 3]);
    untouched(&sessions, &[(1, &one), (2, &two)]).await;

    let three = open_on(&sessions, &routes, TorrentScope::Default, 3).await;
    assert!(
        sessions
            .view(&pool_member_key(3))
            .await
            .unwrap()
            .held
            .is_none()
    );
    untouched(&sessions, &[(1, &one), (2, &two), (3, &three)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// A member no session runs on, edited and then removed, moves nothing and
// revokes nothing for the others.
#[tokio::test(flavor = "multi_thread")]
async fn a_dormant_member_edited_or_removed_moves_and_revokes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2, 3]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    let two = open_on(&sessions, &routes, TorrentScope::Default, 2).await;

    move_proxy(&db, 3);
    network.reload().await.unwrap();
    untouched(&sessions, &[(1, &one), (2, &two)]).await;

    set_pool(&db, &network, &pool, &[1, 2], true).await;
    untouched(&sessions, &[(1, &one), (2, &two)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// The pool's members put in another order, the same proxies as they were,
// changes nothing.
#[tokio::test(flavor = "multi_thread")]
async fn reordering_a_pools_members_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2, 3]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    let three = open_on(&sessions, &routes, TorrentScope::Default, 3).await;

    set_pool(&db, &network, &pool, &[3, 1, 2], true).await;
    untouched(&sessions, &[(1, &one), (3, &three)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// A member a session runs on, removed from the pool, fails that session
// alone: it is revoked for the member leaving, and the session on the other
// member runs on untouched.
#[tokio::test(flavor = "multi_thread")]
async fn an_active_member_removed_fails_only_its_session() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2, 3]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    let two = open_on(&sessions, &routes, TorrentScope::Default, 2).await;
    let left = stage_of(&sessions, 2).await;

    set_pool(&db, &network, &pool, &[1, 3], true).await;
    let gone = BTreeSet::from([pool_member_key(2)]);
    assert_eq!(outdated_after_wake(&sessions, &gone).await, gone);
    assert!(left.is_revoked());
    let held = sessions
        .view(&pool_member_key(2))
        .await
        .unwrap()
        .held
        .unwrap();
    assert_eq!(held.reason, TorrentRouteReason::LegRevoked);
    assert_eq!(
        held.detail.as_deref(),
        Some(format!("proxy 2 is no longer a member of pool {}", pool.id).as_str())
    );
    drop(two);
    untouched(&sessions, &[(1, &one)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// A member a session runs on, disabled, fails that session alone.
#[tokio::test(flavor = "multi_thread")]
async fn an_active_member_disabled_fails_only_its_session() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, _) = on_pool_network(&routes, &[1, 2]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    open_on(&sessions, &routes, TorrentScope::Default, 2).await;

    edit_proxy(&db, 2, |profile| {
        profile.enabled = false;
        profile.revision += 1;
    });
    network.reload().await.unwrap();
    let gone = BTreeSet::from([pool_member_key(2)]);
    assert_eq!(outdated_after_wake(&sessions, &gone).await, gone);
    untouched(&sessions, &[(1, &one)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// A member a session runs on, edited, fails that session alone: it is
// revoked, and its route opens again on the member as it is now, while the
// session on the other member runs on untouched.
#[tokio::test(flavor = "multi_thread")]
async fn an_active_member_edited_reopens_only_its_session() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2]);
    let sessions = sessions_on(&routes, dir.path());
    let one = open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    let old = open_on(&sessions, &routes, TorrentScope::Default, 2).await;
    let old_stage = stage_of(&sessions, 2).await;

    move_proxy(&db, 2);
    network.reload().await.unwrap();
    sessions.revoked().await;
    let gone = BTreeSet::from([pool_member_key(2)]);
    assert_eq!(sessions.outdated().await, gone);
    assert!(old_stage.is_revoked());
    let held = sessions
        .view(&pool_member_key(2))
        .await
        .unwrap()
        .held
        .unwrap();
    assert_eq!(
        held.detail.as_deref(),
        Some(format!("proxy 2 changed in pool {}", pool.id).as_str())
    );

    sessions.drop_route(&pool_member_key(2)).await;
    let new = open_on(&sessions, &routes, TorrentScope::Default, 2).await;
    assert!(!Arc::ptr_eq(&old, &new));
    old.cancellation_token().cancelled().await;
    untouched(&sessions, &[(1, &one), (2, &new)]).await;
    sessions.stop_all().await;
    network.shutdown().await;
}

// The whole pool disabled takes every member's session down.
#[tokio::test(flavor = "multi_thread")]
async fn a_disabled_pool_takes_every_member_session_down() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2]);
    let sessions = sessions_on(&routes, dir.path());
    open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    open_on(&sessions, &routes, TorrentScope::Default, 2).await;
    let route = network
        .route(
            Consumer::Torrent(TorrentScope::Default),
            8,
            Duration::from_secs(30),
        )
        .unwrap();
    let leg = Arc::clone(&route.legs.read().unwrap()[0].stage);

    set_pool(&db, &network, &pool, &[1, 2], false).await;
    let gone = BTreeSet::from([pool_member_key(1), pool_member_key(2)]);
    assert_eq!(outdated_after_wake(&sessions, &gone).await, gone);
    assert!(leg.is_revoked());
    sessions.stop_all().await;
    network.shutdown().await;
}

// The route's leg moved to another egress takes every member's session on
// the old one down.
#[tokio::test(flavor = "multi_thread")]
async fn a_leg_moved_to_another_egress_takes_every_member_session_down() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, pool) = on_pool_network(&routes, &[1, 2]);
    let sessions = sessions_on(&routes, dir.path());
    open_on(&sessions, &routes, TorrentScope::Default, 1).await;
    open_on(&sessions, &routes, TorrentScope::Default, 2).await;

    let egress = db
        .create_egress_interface(&EgressInterface {
            id: 0,
            name: "WAN B".into(),
            binding: EgressBinding::SourceAddress {
                address: "192.0.2.1".parse().unwrap(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: Default::default(),
        })
        .unwrap();
    route_through(&db, TorrentScope::Default, egress.id, &pool_ladder(pool.id));
    network.reload().await.unwrap();
    let gone = BTreeSet::from([pool_member_key(1), pool_member_key(2)]);
    assert_eq!(outdated_after_wake(&sessions, &gone).await, gone);
    sessions.stop_all().await;
    network.shutdown().await;
}

// Proxy 2 sits in the default scope's pool and in the category's: both
// scopes plan member 2 to one key, and a session opened for either is the
// other's. The default scope's route leaving its pool leaves that session
// running for the category; the category's leaving its own closes the last
// pool holding proxy 2, and the session goes down.
#[tokio::test(flavor = "multi_thread")]
async fn a_proxy_in_two_pools_runs_one_session_until_no_pool_holds_it() {
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let (db, network, _) = on_pool_network(&routes, &[1, 2]);
    let other = add_pool(&db, &[2, 3]);
    route_through(&db, TV, SYSTEM_EGRESS_ID, &pool_ladder(other.id));
    network.reload().await.unwrap();
    routes.route_pool(TV, other.id, &[2, 3]);
    let sessions = sessions_on(&routes, dir.path());

    let default = routes.plan(TorrentScope::Default);
    let tv = routes.plan(TV);
    let shared: Vec<_> = default
        .keys()
        .filter(|key| tv.keys().any(|other| other == *key))
        .collect();
    assert_eq!(shared, [&pool_member_key(2)]);
    let session = open_on(&sessions, &routes, TorrentScope::Default, 2).await;
    assert!(Arc::ptr_eq(
        &open_on(&sessions, &routes, TV, 2).await,
        &session
    ));
    assert_eq!(sessions.views().await.len(), 1);

    route_through(
        &db,
        TorrentScope::Default,
        SYSTEM_EGRESS_ID,
        &LegPath::Direct,
    );
    network.reload().await.unwrap();
    untouched(&sessions, &[(2, &session)]).await;
    assert!(Arc::ptr_eq(
        &open_on(&sessions, &routes, TV, 2).await,
        &session
    ));

    route_through(&db, TV, SYSTEM_EGRESS_ID, &LegPath::Direct);
    network.reload().await.unwrap();
    let gone = BTreeSet::from([pool_member_key(2)]);
    assert_eq!(outdated_after_wake(&sessions, &gone).await, gone);
    sessions.stop_all().await;
    network.shutdown().await;
}

// A pool member whose tunnel never completes its handshake opens no session.
// One failed open moves no torrent off it; once sessions failed to open on
// it as often in a row as the pool's plan takes to call a member suspect,
// the route passes it over for a member that is up, its torrents go there
// and stay, and the pool's plan shows it suspect.
#[tokio::test(flavor = "multi_thread")]
async fn a_member_whose_tunnel_cannot_open_is_passed_over_after_repeated_failures() {
    use weaver_tunnel::test_support::{WireGuardTestPeer, WireGuardTestPeerOptions, test_key};
    // The first peer accepts only another client's key, so it answers
    // nothing; the second answers.
    let silent = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
        client_public_key: test_key(77),
        ..WireGuardTestPeerOptions::default()
    })
    .await;
    let answering = WireGuardTestPeer::start_with(WireGuardTestPeerOptions::default()).await;
    let dir = tempfile::tempdir().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.through_legs.store(true, Ordering::SeqCst);
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&wireguard_proxy(&silent, 1)).unwrap();
    db.save_proxy_profile(&ProxyProfile {
        id: 2,
        name: "exit-2".into(),
        ..wireguard_proxy(&answering, 10)
    })
    .unwrap();
    let pool = db
        .create_proxy_pool(&ProxyPool {
            id: 0,
            name: "Region".into(),
            kind: ProxyKind::WireGuard,
            member_ids: vec![1, 2],
            enabled: true,
        })
        .unwrap();
    route_through(
        &db,
        TorrentScope::Default,
        SYSTEM_EGRESS_ID,
        &pool_ladder(pool.id),
    );
    let network = NetworkRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    routes.route_pool(TorrentScope::Default, pool.id, &[1, 2]);
    *routes.network.lock().unwrap() = Some(Arc::clone(&network));
    let sessions = sessions_on(&routes, dir.path());
    let place = |pinned| routes.plan(TorrentScope::Default).place(pinned, |_| 0);
    let fails_on_one = || async {
        let (plan, pin) = place(Some(1));
        assert_eq!(pin, Some(1));
        let Err(SessionError::Build(failure)) = sessions.session(&plan, &IDENTITY.identity()).await
        else {
            panic!("a member whose tunnel has no handshake opens no session");
        };
        assert_eq!(
            failure.reason,
            TorrentRouteReason::LegOpenFailed,
            "{failure:?}"
        );
    };

    fails_on_one().await;
    // Once is not enough to move: the torrent stays on its member.
    fails_on_one().await;

    let (plan, pin) = place(Some(1));
    assert_eq!(pin, Some(2));
    let down = routes
        .plan(TorrentScope::Default)
        .members
        .into_iter()
        .find(|member| member.id == 1)
        .and_then(|member| member.down)
        .unwrap()
        .message();
    assert!(
        down.contains("proxy 1 opened no session 2 times in a row: proxy 1: No handshake"),
        "{down}"
    );
    let (_, _, snapshot, _) = network
        .pool_status()
        .into_iter()
        .find(|(id, ..)| *id == pool.id)
        .unwrap();
    let one = snapshot
        .candidates
        .iter()
        .find(|candidate| candidate.candidate == 1)
        .unwrap();
    assert_eq!(one.state, "SUSPECT");
    assert_eq!(one.failures, 2);

    sessions.session(&plan, &IDENTITY.identity()).await.unwrap();
    // It stays on the member it moved to, and a torrent placed anew goes
    // there too.
    assert_eq!(place(Some(2)).1, Some(2));
    assert_eq!(place(None).1, Some(2));
    sessions.stop_all().await;
}
