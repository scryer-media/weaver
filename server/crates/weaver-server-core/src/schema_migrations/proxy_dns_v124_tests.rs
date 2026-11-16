// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use sqlx::SqlitePool;
use sqlx::sqlite::SqlitePoolOptions;

use super::super::{apply_version_range, embedded_catalog, embedded_payload_bytes};
use super::*;
use crate::migration_assets::MigrationInstallKind;

async fn below_schema_124() -> SqlitePool {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    super::super::replay_catalog_into_fresh_db(
        &pool,
        &embedded_catalog().unwrap(),
        &embedded_payload_bytes().unwrap(),
        Some(SCHEMA_VERSION - 1),
        true,
    )
    .await
    .unwrap();
    pool
}

async fn upgrade_to_124(pool: &SqlitePool) {
    apply_version_range(
        pool,
        &embedded_catalog().unwrap(),
        &embedded_payload_bytes().unwrap(),
        MigrationInstallKind::Upgrade,
        SCHEMA_VERSION,
        SCHEMA_VERSION,
    )
    .await
    .unwrap();
}

async fn save_proxy(pool: &SqlitePool, id: i64, config: &str) {
    sqlx::query("INSERT INTO proxy_profiles (id, config, password) VALUES (?1, ?2, NULL)")
        .bind(id)
        .bind(config)
        .execute(pool)
        .await
        .unwrap();
}

async fn save_pool(pool: &SqlitePool, id: i64, members: &str) {
    sqlx::query(
        "INSERT INTO proxy_pools (id, name, kind, member_ids, enabled, created_at, updated_at) VALUES (?1, 'pool', 'RoundRobin', ?2, 1, 0, 0)",
    )
    .bind(id)
    .bind(members)
    .execute(pool)
    .await
    .unwrap();
}

async fn save_route(pool: &SqlitePool, consumer: &str, policy: Value) {
    sqlx::query("INSERT INTO proxy_routes (consumer, policy) VALUES (?1, ?2)")
        .bind(consumer)
        .bind(policy.to_string())
        .execute(pool)
        .await
        .unwrap();
}

async fn saved_proxy(pool: &SqlitePool, id: i64) -> Value {
    let raw: String = sqlx::query_scalar("SELECT config FROM proxy_profiles WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

async fn route_profile(pool: &SqlitePool, consumer: &str) -> i64 {
    sqlx::query_scalar("SELECT dns_profile_id FROM proxy_routes WHERE consumer = ?1")
        .bind(consumer)
        .fetch_one(pool)
        .await
        .unwrap()
}

type ProfileRow = (
    i64,
    String,
    String,
    Option<String>,
    String,
    Option<i64>,
    bool,
);

async fn profiles(pool: &SqlitePool) -> Vec<ProfileRow> {
    sqlx::query_as(
        "SELECT id, name, who, transit, trust, server_list_id, system_fallback FROM dns_profiles ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

// The id of the profile named `name`, of `kind`.
async fn profile_named(pool: &SqlitePool, name: &str, kind: &str) -> i64 {
    sqlx::query_scalar("SELECT id FROM dns_profiles WHERE name = ?1 AND kind = ?2")
        .bind(name)
        .bind(kind)
        .fetch_one(pool)
        .await
        .unwrap()
}

// The profile that finds proxy `id`'s own address.
async fn proxy_name_profile(pool: &SqlitePool, id: i64) -> i64 {
    sqlx::query_scalar("SELECT dns_profile_id FROM proxy_profiles WHERE id = ?1")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn lists(pool: &SqlitePool) -> Vec<(i64, String, Value)> {
    let rows: Vec<(i64, String, String)> =
        sqlx::query_as("SELECT id, name, servers FROM dns_server_lists ORDER BY id")
            .fetch_all(pool)
            .await
            .unwrap();
    rows.into_iter()
        .map(|(id, name, servers)| (id, name, serde_json::from_str(&servers).unwrap()))
        .collect()
}

// A proxy as a build below 124 saved it.
fn before_124(name: &str, kind: &str, resolver: &str, dns_servers: &str) -> String {
    format!(
        r#"{{"id":0,"name":"{name}","kind":"{kind}","enabled":true,"host":"proxy.invalid","port":1080,"resolver":"{resolver}","dns_servers":{dns_servers},"tunnel_addresses":[],"peer_public_key":null,"mtu":1420,"keepalive_seconds":null,"timeout_seconds":30,"host_key_fingerprint":null,"revision":3,"natpmp":false,"forwarded_port":null}}"#
    )
}

fn through(rung: Value, direct_fallback: bool) -> Value {
    json!({
        "legs": [{
            "egressId": 0,
            "weight": 1,
            "path": { "kind": "ladder", "rungs": [rung], "directFallback": direct_fallback },
        }],
    })
}

fn plain_list(addresses: &[&str]) -> Value {
    json!({
        "v": 1,
        "servers": addresses
            .iter()
            .map(|address| json!({ "address": address, "wire": "Plain" }))
            .collect::<Vec<_>>(),
    })
}

#[tokio::test]
async fn each_distinct_proxy_setting_becomes_one_profile() {
    let pool = below_schema_124().await;
    save_proxy(&pool, 1, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_proxy(
        &pool,
        2,
        &before_124(
            "listed",
            "Socks5",
            "Weaver",
            r#"["192.0.2.53","2001:db8::53","192.0.2.53"]"#,
        ),
    )
    .await;
    // The same setting as proxy 2 makes no second profile.
    save_proxy(
        &pool,
        3,
        &before_124(
            "listed again",
            "HttpConnect",
            "Weaver",
            r#"["192.0.2.53","2001:db8::53"]"#,
        ),
    )
    .await;
    save_proxy(&pool, 4, &before_124("handed", "Ssh", "Proxy", "[]")).await;
    save_proxy(
        &pool,
        5,
        &before_124("carried", "Socks5", "Proxy", r#"["192.0.2.54"]"#),
    )
    .await;
    save_proxy(
        &pool,
        6,
        &before_124("tunnel", "WireGuard", "Proxy", r#"["10.64.0.1"]"#),
    )
    .await;

    upgrade_to_124(&pool).await;

    assert_eq!(
        profiles(&pool).await,
        vec![
            (
                1,
                "listed".into(),
                "weaver".into(),
                Some("egress".into()),
                "as_sent".into(),
                Some(1),
                false
            ),
            // Where the proxy answers, the host still answers a stage with
            // no proxy, as it did before.
            // The proxies that listed those servers find their own address
            // by them too.
            (
                2,
                "listed (proxy address)".into(),
                "weaver".into(),
                Some("egress".into()),
                "as_sent".into(),
                Some(1),
                false
            ),
            (
                3,
                "handed".into(),
                "proxy".into(),
                None,
                "as_sent".into(),
                None,
                true
            ),
            (
                4,
                "carried".into(),
                "weaver".into(),
                Some("proxy".into()),
                "as_sent".into(),
                Some(2),
                false
            ),
        ]
    );
    // The repeated address is listed once, and every server is plain on 53.
    assert_eq!(
        lists(&pool).await,
        vec![
            (
                1,
                "listed".into(),
                plain_list(&["192.0.2.53:53", "[2001:db8::53]:53"])
            ),
            (2, "carried".into(), plain_list(&["192.0.2.54:53"])),
        ]
    );
    for id in 1..=5 {
        let proxy = saved_proxy(&pool, id).await;
        assert!(proxy.get(RESOLVER).is_none() && proxy.get(DNS_SERVERS).is_none());
    }
    let kinds: Vec<String> = sqlx::query_scalar("SELECT kind FROM dns_profiles ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(kinds, ["route", "proxy_name", "route", "route"]);
    // Listed servers asked from the egress find the proxy's address, one
    // profile for the setting; the host found every other proxy before, and
    // still does.
    for (proxy, profile) in [(1, 0), (2, 2), (3, 2), (4, 0), (5, 0), (6, 0)] {
        assert_eq!(
            proxy_name_profile(&pool, proxy).await,
            profile,
            "proxy {proxy}"
        );
    }
    // A WireGuard tunnel keeps the servers it asks, as tunnel configuration.
    let tunnel = saved_proxy(&pool, 6).await;
    assert!(tunnel.get(RESOLVER).is_none());
    assert_eq!(tunnel[DNS_SERVERS], json!(["10.64.0.1"]));
    assert_eq!(tunnel["revision"], json!(3));
}

#[tokio::test]
async fn a_list_keeps_at_most_ten_servers() {
    let pool = below_schema_124().await;
    let servers: Vec<String> = (1..=12).map(|n| format!("\"192.0.2.{n}\"")).collect();
    save_proxy(
        &pool,
        1,
        &before_124(
            "many",
            "Socks5",
            "Weaver",
            &format!("[{}]", servers.join(",")),
        ),
    )
    .await;

    upgrade_to_124(&pool).await;

    let (_, _, stored) = lists(&pool).await.remove(0);
    assert_eq!(stored["servers"].as_array().unwrap().len(), 10);
    assert_eq!(stored["servers"][9]["address"], json!("192.0.2.10:53"));
}

#[tokio::test]
async fn a_route_takes_the_profile_of_its_first_legs_proxy() {
    let pool = below_schema_124().await;
    save_proxy(
        &pool,
        1,
        &before_124("listed", "Socks5", "Weaver", r#"["192.0.2.53"]"#),
    )
    .await;
    save_proxy(&pool, 2, &before_124("handed", "Socks5", "Proxy", "[]")).await;
    save_proxy(&pool, 3, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_pool(&pool, 1, "[2,1]").await;
    save_route(
        &pool,
        "server:1",
        through(json!({ "kind": "proxy", "id": 1 }), false),
    )
    .await;
    // A chain is named by the proxy it ends at.
    save_route(
        &pool,
        "server:2",
        through(json!({ "kind": "chain", "ids": [1, 2] }), false),
    )
    .await;
    // A pool is named by its first member.
    save_route(
        &pool,
        "server:3",
        through(json!({ "kind": "pool", "id": 1 }), false),
    )
    .await;
    // A legacy route is named by the first proxy it lists.
    save_route(
        &pool,
        "server:4",
        json!({ "proxyIds": [1, 3], "allowDirect": false }),
    )
    .await;
    // A proxy the host answered for keeps the System profile.
    save_route(
        &pool,
        "server:5",
        through(json!({ "kind": "proxy", "id": 3 }), false),
    )
    .await;
    // A direct route keeps the System profile.
    save_route(
        &pool,
        "server:6",
        json!({ "legs": [{ "egressId": 0, "weight": 1, "path": { "kind": "direct" } }] }),
    )
    .await;
    // A route with a stage that has no proxy takes the proxy's profile,
    // which lets the host answer that stage as it did before.
    save_route(
        &pool,
        "server:7",
        through(json!({ "kind": "proxy", "id": 2 }), true),
    )
    .await;
    save_route(
        &pool,
        "server:8",
        through(json!({ "kind": "proxy", "id": 2 }), false),
    )
    .await;

    upgrade_to_124(&pool).await;

    let listed = profile_named(&pool, "listed", "route").await;
    let handed = profile_named(&pool, "handed", "route").await;
    assert_eq!(route_profile(&pool, "server:1").await, listed);
    assert_eq!(route_profile(&pool, "server:2").await, handed);
    assert_eq!(route_profile(&pool, "server:3").await, handed);
    assert_eq!(route_profile(&pool, "server:4").await, listed);
    assert_eq!(route_profile(&pool, "server:5").await, 0);
    assert_eq!(route_profile(&pool, "server:6").await, 0);
    assert_eq!(route_profile(&pool, "server:7").await, handed);
    assert_eq!(route_profile(&pool, "server:8").await, handed);
}

// No route that a proxy or listed servers answered for lands on System, a
// direct leg included: a direct first leg gives way to the first leg that
// goes through a proxy.
#[tokio::test]
async fn a_route_with_a_direct_leg_keeps_its_proxys_dns() {
    let pool = below_schema_124().await;
    save_proxy(
        &pool,
        1,
        &before_124("listed", "Socks5", "Weaver", r#"["192.0.2.53"]"#),
    )
    .await;
    save_proxy(&pool, 2, &before_124("handed", "Socks5", "Proxy", "[]")).await;
    let direct_then = |id: u32| {
        json!({
            "legs": [
                { "egressId": 0, "weight": 1, "path": { "kind": "direct" } },
                {
                    "egressId": 0,
                    "weight": 1,
                    "path": {
                        "kind": "ladder",
                        "rungs": [{ "kind": "proxy", "id": id }],
                        "directFallback": false,
                    },
                },
            ],
        })
    };
    save_route(&pool, "server:1", direct_then(1)).await;
    save_route(&pool, "server:2", direct_then(2)).await;
    save_route(
        &pool,
        "server:3",
        json!({ "proxyIds": [2], "allowDirect": true }),
    )
    .await;

    upgrade_to_124(&pool).await;

    let listed = profile_named(&pool, "listed", "route").await;
    let handed = profile_named(&pool, "handed", "route").await;
    assert_eq!(route_profile(&pool, "server:1").await, listed);
    assert_eq!(route_profile(&pool, "server:2").await, handed);
    assert_eq!(route_profile(&pool, "server:3").await, handed);
    let fallback: Vec<(i64, bool)> = sqlx::query_as(
        "SELECT id, system_fallback FROM dns_profiles WHERE kind = 'route' ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(fallback, vec![(listed, false), (handed, true)]);
}

// A route any of whose proxies the proxy resolved for takes the profile
// where the proxy answers, whichever proxy comes first: a name that proxy
// was handed is never looked up elsewhere after the upgrade.
#[tokio::test]
async fn a_route_with_any_proxy_the_proxy_resolved_for_takes_the_proxy_profile() {
    let pool = below_schema_124().await;
    save_proxy(
        &pool,
        1,
        &before_124("listed", "Socks5", "Weaver", r#"["192.0.2.53"]"#),
    )
    .await;
    save_proxy(&pool, 2, &before_124("handed", "Socks5", "Proxy", "[]")).await;
    save_proxy(&pool, 3, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_pool(&pool, 1, "[1,2]").await;
    let ladder =
        |rungs: Value| json!({ "kind": "ladder", "rungs": rungs, "directFallback": false });
    // A later rung of the first leg.
    save_route(
        &pool,
        "server:1",
        json!({ "legs": [{
            "egressId": 0,
            "weight": 1,
            "path": ladder(json!([{ "kind": "proxy", "id": 1 }, { "kind": "proxy", "id": 2 }])),
        }] }),
    )
    .await;
    // A later leg.
    save_route(
        &pool,
        "server:2",
        json!({ "legs": [
            { "egressId": 0, "weight": 1, "path": ladder(json!([{ "kind": "proxy", "id": 3 }])) },
            { "egressId": 0, "weight": 1, "path": ladder(json!([{ "kind": "proxy", "id": 2 }])) },
        ] }),
    )
    .await;
    // A pool member after the first.
    save_route(
        &pool,
        "server:3",
        through(json!({ "kind": "pool", "id": 1 }), false),
    )
    .await;
    // A legacy route's later proxy.
    save_route(
        &pool,
        "server:4",
        json!({ "proxyIds": [1, 2], "allowDirect": false }),
    )
    .await;

    upgrade_to_124(&pool).await;

    let handed = profile_named(&pool, "handed", "route").await;
    for consumer in ["server:1", "server:2", "server:3", "server:4"] {
        assert_eq!(route_profile(&pool, consumer).await, handed, "{consumer}");
    }
    let fallback: Vec<(i64, bool)> =
        sqlx::query_as("SELECT id, system_fallback FROM dns_profiles WHERE id = ?1")
            .bind(handed)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(fallback, vec![(handed, true)]);
}

// Listed servers on any of a route's proxies beat a first proxy the host
// answered for, so the later proxy's names never move to the host.
#[tokio::test]
async fn a_route_whose_later_proxy_listed_servers_takes_that_profile_over_system() {
    let pool = below_schema_124().await;
    save_proxy(&pool, 1, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_proxy(
        &pool,
        2,
        &before_124("listed", "Socks5", "Weaver", r#"["192.0.2.53"]"#),
    )
    .await;
    save_route(
        &pool,
        "server:1",
        json!({ "legs": [{
            "egressId": 0,
            "weight": 1,
            "path": { "kind": "ladder", "rungs": [{ "kind": "proxy", "id": 1 }, { "kind": "proxy", "id": 2 }], "directFallback": false },
        }] }),
    )
    .await;
    save_route(
        &pool,
        "server:2",
        json!({ "proxyIds": [1, 2], "allowDirect": false }),
    )
    .await;

    upgrade_to_124(&pool).await;

    let listed: i64 =
        sqlx::query_scalar("SELECT id FROM dns_profiles WHERE who = 'weaver' AND kind = 'route'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_ne!(listed, 0);
    assert_eq!(route_profile(&pool, "server:1").await, listed);
    assert_eq!(route_profile(&pool, "server:2").await, listed);
}

// A proxy whose stored config cannot be read is left as it was, and counts
// as one the proxy resolved for: its routes take the profile where the
// proxy answers, never System.
#[tokio::test]
async fn an_unreadable_proxy_is_left_as_it_was_and_its_routes_hand_names_to_the_proxy() {
    let pool = below_schema_124().await;
    save_proxy(&pool, 1, r#"{"resolver":"Elsewhere"}"#).await;
    save_proxy(&pool, 2, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_route(
        &pool,
        "server:1",
        through(json!({ "kind": "proxy", "id": 1 }), false),
    )
    .await;
    // Behind a proxy the host answered for, it still decides.
    save_route(
        &pool,
        "server:2",
        json!({ "proxyIds": [2, 1], "allowDirect": false }),
    )
    .await;

    upgrade_to_124(&pool).await;

    assert_eq!(
        saved_proxy(&pool, 1).await,
        json!({ "resolver": "Elsewhere" })
    );
    assert_eq!(
        profiles(&pool).await,
        vec![(
            1,
            "Proxy 1".into(),
            "proxy".into(),
            None,
            "as_sent".into(),
            None,
            true
        )]
    );
    assert_eq!(route_profile(&pool, "server:1").await, 1);
    assert_eq!(route_profile(&pool, "server:2").await, 1);
}

// Routes are given their profiles past the bind limit of one statement.
#[tokio::test]
async fn routes_past_the_bind_limit_are_all_given_their_profile() {
    let pool = below_schema_124().await;
    save_proxy(&pool, 1, &before_124("handed", "Socks5", "Proxy", "[]")).await;
    let routes = 2 * crate::persistence::sql_runtime::SQLITE_BATCH_BIND_LIMIT + 3;
    let policy = through(json!({ "kind": "proxy", "id": 1 }), false).to_string();
    let mut tx = pool.begin().await.unwrap();
    for n in 0..routes {
        sqlx::query("INSERT INTO proxy_routes (consumer, policy) VALUES (?1, ?2)")
            .bind(format!("rss:{n}"))
            .bind(&policy)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();

    upgrade_to_124(&pool).await;

    let given: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM proxy_routes WHERE dns_profile_id = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(given, routes as i64);
}

#[tokio::test]
async fn an_install_with_no_proxy_dns_writes_nothing() {
    let pool = below_schema_124().await;
    save_proxy(&pool, 1, &before_124("host", "Socks5", "Weaver", "[]")).await;
    save_route(
        &pool,
        "server:1",
        through(json!({ "kind": "proxy", "id": 1 }), false),
    )
    .await;

    upgrade_to_124(&pool).await;

    assert!(profiles(&pool).await.is_empty());
    assert!(lists(&pool).await.is_empty());
    assert_eq!(route_profile(&pool, "server:1").await, 0);
}

// A SOCKS5, HTTP or SSH proxy whose proxy answered names, with servers
// listed, found its own address by the host: it keeps the host's profile,
// and no profile is made for its address. A route through it asks those
// servers through the proxy, as it did before.
#[tokio::test]
async fn a_stream_proxy_handed_names_with_servers_listed_keeps_the_hosts_name_profile() {
    for kind in ["Socks5", "HttpConnect", "Http3Connect", "Ssh"] {
        let pool = below_schema_124().await;
        save_proxy(
            &pool,
            1,
            &before_124("carried", kind, "Proxy", r#"["192.0.2.54"]"#),
        )
        .await;
        save_route(
            &pool,
            "server:1",
            through(json!({ "kind": "proxy", "id": 1 }), false),
        )
        .await;

        upgrade_to_124(&pool).await;

        assert_eq!(proxy_name_profile(&pool, 1).await, 0, "{kind}");
        let named: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM dns_profiles WHERE kind = 'proxy_name'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(named, 0, "{kind}");
        let carried = profile_named(&pool, "carried", "route").await;
        assert_eq!(route_profile(&pool, "server:1").await, carried, "{kind}");
        assert_eq!(
            profiles(&pool).await,
            vec![(
                carried,
                "carried".into(),
                "weaver".into(),
                Some("proxy".into()),
                "as_sent".into(),
                Some(1),
                false
            )],
            "{kind}"
        );
        assert_eq!(
            lists(&pool).await,
            vec![(1, "carried".into(), plain_list(&["192.0.2.54:53"]))],
            "{kind}"
        );
    }
}
