use super::*;
use crate::proxies::{Failover, ProxyKind, ProxySecrets, Resolver, RouteLeg};

// The code of the refusal a write was turned away with. A storage fault has
// none, so a refusal that degrades into one fails here.
fn refused<T: std::fmt::Debug>(result: Result<T, StateError>) -> &'static str {
    match result.unwrap_err() {
        StateError::Refused(refusal) => refusal.code,
        other => panic!("not a refusal: {other}"),
    }
}

fn egress() -> EgressInterface {
    EgressInterface {
        id: 0,
        name: "WAN A".into(),
        binding: EgressBinding::SourceAddress {
            address: "192.0.2.1".parse().unwrap(),
        },
        enabled: true,
        max_download_speed: 1_000_000,
        download_quota: Default::default(),
    }
}

fn profile(id: u32) -> ProxyProfile {
    ProxyProfile {
        id,
        name: format!("proxy-{id}"),
        kind: ProxyKind::Socks5,
        enabled: true,
        host: "proxy.example".into(),
        port: 1080,
        resolver: Resolver::Proxy,
        dns_servers: vec!["192.0.2.53".parse().unwrap()],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 30,
        host_key_fingerprint: None,
        revision: 1,
        secrets: ProxySecrets::default(),
    }
}

fn pool(db: &Database) -> ProxyPool {
    for id in 1..=3 {
        db.save_proxy_profile(&profile(id)).unwrap();
    }
    db.create_proxy_pool(&ProxyPool {
        id: 0,
        name: "Region".into(),
        kind: ProxyKind::Socks5,
        member_ids: vec![1, 2],
        enabled: true,
    })
    .unwrap()
}

fn insert_route(db: &Database, route: Route) {
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "route_fixture", |tx| {
            let route = route.clone();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {})",
                    &[
                        SqlArg::Text("server:1".into()),
                        SqlArg::Text(serde_json::to_string(&route).unwrap()),
                    ],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

#[test]
fn migration_seeds_exactly_one_system_egress_and_backup_accepts_it() {
    let db = Database::open_in_memory().unwrap();
    assert_eq!(
        db.list_egress_interfaces().unwrap(),
        [EgressInterface::system()]
    );
    assert!(db.list_proxy_pools().unwrap().is_empty());
    assert!(db.restore_target_is_pristine().unwrap());
    db.validate_backup_catalog().unwrap();
    assert_eq!(
        refused(db.delete_egress_interface(0)),
        "EGRESS_SYSTEM_UNDELETABLE"
    );
    assert!(
        db.create_egress_interface(&EgressInterface::system())
            .is_err()
    );
    let mut system = EgressInterface::system();
    system.max_download_speed = 5_000_000;
    assert_eq!(db.update_egress_interface(&system).unwrap(), system);
    system.name = "renamed".into();
    assert_eq!(
        refused(db.update_egress_interface(&system)),
        "EGRESS_SYSTEM_IMMUTABLE"
    );
}

#[test]
fn egress_and_pool_configuration_round_trips_through_backup() {
    let source = Database::open_in_memory().unwrap();
    let egress = source.create_egress_interface(&egress()).unwrap();
    let pool = pool(&source);
    let archive = source.export_logical_backup().unwrap();
    assert_eq!(archive.tables["egress_interfaces"].rows, 2);
    assert_eq!(archive.tables["proxy_pools"].rows, 1);
    let target = Database::open_in_memory().unwrap();
    target
        .import_logical_backup(
            &archive.staging.path().join("tables"),
            &archive.tables,
            archive.schema_version,
        )
        .unwrap();
    assert_eq!(
        target.list_egress_interfaces().unwrap(),
        [EgressInterface::system(), egress]
    );
    let restored = target.list_proxy_pools().unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].member_ids, pool.member_ids);
    assert!(!target.restore_target_is_pristine().unwrap());
}

#[test]
fn referenced_resources_cannot_be_deleted_and_pool_edits_validate_existing_ladders() {
    let db = Database::open_in_memory().unwrap();
    let egress = db.create_egress_interface(&egress()).unwrap();
    let mut pool = pool(&db);
    insert_route(
        &db,
        Route {
            legs: vec![RouteLeg {
                egress_id: egress.id,
                weight: 100,
                path: LegPath::Ladder {
                    rungs: vec![Rung::Pool { id: pool.id }, Rung::Proxy { id: 3 }],
                    direct_fallback: false,
                },
            }],
            failover: Failover::Redistribute,
        },
    );
    assert_eq!(
        refused(db.delete_egress_interface(egress.id)),
        "EGRESS_IN_USE"
    );
    assert_eq!(refused(db.delete_proxy_pool(pool.id)), "POOL_IN_USE");
    assert_eq!(refused(db.delete_proxy_profile(1)), "PROXY_IN_POOL");
    assert_eq!(refused(db.delete_proxy_profile(3)), "PROXY_IN_USE");
    pool.member_ids = vec![1, 3];
    assert_eq!(
        refused(db.update_proxy_pool(&pool)),
        "POOL_EDIT_REPEATS_PROXY"
    );
    assert_eq!(db.list_proxy_pools().unwrap()[0].member_ids, [1, 2]);
    let mut changed = profile(1);
    changed.kind = ProxyKind::HttpConnect;
    changed.revision += 1;
    assert_eq!(
        refused(db.save_proxy_profile(&changed)),
        "PROXY_KIND_LOCKED_BY_POOL"
    );
}

#[test]
fn a_pool_saves_with_any_member_count_but_a_route_needs_a_member() {
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&profile(1)).unwrap();
    let mut pool = db
        .create_proxy_pool(&ProxyPool {
            id: 0,
            name: "Region".into(),
            kind: ProxyKind::Socks5,
            member_ids: vec![],
            enabled: true,
        })
        .unwrap();
    assert!(pool.member_ids.is_empty());
    let consumer = crate::proxies::Consumer::Server(1);
    let pooled = RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![Rung::Pool { id: pool.id }],
                direct_fallback: false,
            },
        }],
        ..Default::default()
    };
    assert_eq!(
        refused(db.save_proxy_routing_policy(consumer, &pooled)),
        "ROUTE_POOL_EMPTY"
    );
    assert!(db.list_proxy_routes().unwrap().is_empty());

    pool.member_ids = vec![1];
    assert_eq!(db.update_proxy_pool(&pool).unwrap().member_ids, [1]);
    db.save_proxy_routing_policy(consumer, &pooled).unwrap();

    // The route keeps the pool's last member: not by emptying the pool, by
    // deleting the proxy, or by changing its kind.
    pool.member_ids.clear();
    assert_eq!(refused(db.update_proxy_pool(&pool)), "POOL_IN_USE_EMPTY");
    assert_eq!(refused(db.delete_proxy_profile(1)), "PROXY_IN_POOL");
    let mut retyped = profile(1);
    retyped.kind = ProxyKind::HttpConnect;
    retyped.revision += 1;
    assert_eq!(
        refused(db.save_proxy_profile(&retyped)),
        "PROXY_KIND_LOCKED_BY_POOL"
    );
    assert_eq!(db.list_proxy_pools().unwrap()[0].member_ids, [1]);

    // Once no route uses it, the pool empties again.
    db.save_proxy_routing_policy(
        consumer,
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 0,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    assert!(db.update_proxy_pool(&pool).unwrap().member_ids.is_empty());
}

#[test]
fn proxy_references_in_later_legs_and_chain_hops_are_protected() {
    let db = Database::open_in_memory().unwrap();
    for id in 1..=2 {
        db.save_proxy_profile(&profile(id)).unwrap();
    }
    insert_route(
        &db,
        Route {
            legs: vec![
                RouteLeg {
                    egress_id: 0,
                    weight: 50,
                    path: LegPath::Direct,
                },
                RouteLeg {
                    egress_id: 0,
                    weight: 50,
                    path: LegPath::Ladder {
                        rungs: vec![Rung::Chain { ids: vec![1, 2] }],
                        direct_fallback: false,
                    },
                },
            ],
            failover: Failover::Redistribute,
        },
    );
    for id in 1..=2 {
        assert_eq!(refused(db.delete_proxy_profile(id)), "PROXY_IN_USE");
    }
}

#[test]
fn unused_resources_can_be_updated_and_deleted_without_affecting_system() {
    let db = Database::open_in_memory().unwrap();
    let mut egress = db.create_egress_interface(&egress()).unwrap();
    egress.enabled = false;
    assert!(!db.update_egress_interface(&egress).unwrap().enabled);
    let pool = pool(&db);
    db.delete_proxy_pool(pool.id).unwrap();
    db.delete_proxy_profile(1).unwrap();
    db.delete_egress_interface(egress.id).unwrap();
    assert_eq!(
        db.list_egress_interfaces().unwrap(),
        [EgressInterface::system()]
    );
}

#[tokio::test]
async fn restored_missing_egress_stays_down_until_explicitly_repaired() {
    let source = Database::open_in_memory().unwrap();
    source
        .insert_server(&crate::servers::ServerConfig {
            id: 1,
            host: "news.fixture.invalid".into(),
            port: 119,
            tls: false,
            username: None,
            password: None,
            connections: 10,
            active: false,
            supports_pipelining: false,
            pipelining_depth: None,
            tls_name_mismatch_certificate_der: None,
            priority: 0,
            backfill: false,
            retention_days: 0,
            max_download_speed: 0,
            download_quota: Default::default(),
            tls_ca_cert: None,
        })
        .unwrap();
    insert_route(
        &source,
        Route {
            legs: vec![RouteLeg {
                egress_id: 91,
                weight: 100,
                path: LegPath::Direct,
            }],
            failover: Failover::Redistribute,
        },
    );
    let archive = source.export_logical_backup().unwrap();
    let target = Database::open_in_memory().unwrap();
    target
        .import_logical_backup(
            &archive.staging.path().join("tables"),
            &archive.tables,
            archive.schema_version,
        )
        .unwrap();
    let runtime =
        crate::proxies::ProxyRuntime::new(target.clone(), tokio::runtime::Handle::current())
            .unwrap();
    let route = runtime
        .network
        .route(
            crate::proxies::Consumer::Server(1),
            10,
            std::time::Duration::from_secs(1),
        )
        .unwrap();
    for _ in 0..2 {
        {
            let legs = route.legs.read().unwrap();
            assert_eq!(legs[0].definition.egress_id, 91);
            assert!(legs[0].warning.as_ref().unwrap().contains("91"));
            assert_eq!(route.weighted.allocations()[0].target, 0);
            assert!(matches!(
                route.weighted.allocations()[0].health,
                crate::proxies::LegHealthState::Down(_)
            ));
        }
        runtime.reload().await.unwrap();
    }
    // Unrelated resource edits remain possible while the restored reference awaits repair.
    target.save_proxy_profile(&profile(7)).unwrap();
    let policy = RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Direct,
        }],
        ..Default::default()
    };
    target
        .save_proxy_routing_policy(crate::proxies::Consumer::Server(1), &policy)
        .unwrap();
    runtime.reload().await.unwrap();
    assert!(route.legs.read().unwrap()[0].warning.is_none());
    runtime.stop_all().await;
}

#[test]
fn logical_backup_before_egress_catalog_restores_system_defaults() {
    let source = Database::open_in_memory().unwrap();
    source.save_proxy_profile(&profile(7)).unwrap();
    let mut archive = source.export_logical_backup().unwrap();
    for table in ["egress_interfaces", "proxy_pools"] {
        archive.tables.remove(table);
    }
    let mut target = Database::open_in_memory().unwrap();
    target.set_encryption_key(source.encryption_key().unwrap().clone());
    assert!(
        target
            .import_logical_backup(
                &archive.staging.path().join("tables"),
                &archive.tables,
                archive.schema_version
            )
            .is_err()
    );
    target
        .import_logical_backup(&archive.staging.path().join("tables"), &archive.tables, 50)
        .unwrap();
    assert_eq!(
        target.list_egress_interfaces().unwrap(),
        [EgressInterface::system()]
    );
    assert!(target.list_proxy_pools().unwrap().is_empty());
    assert_eq!(target.list_proxy_profiles().unwrap().len(), 1);
}

// Remove a row the way a restore of an older backup or a hand edit does:
// behind the store's reference checks.
fn delete_row_behind_the_store(db: &Database, sql: &'static str, id: u32) {
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "row_fixture", |tx| {
            Box::pin(async move {
                tx.execute(sql, &[SqlArg::I64(i64::from(id))]).await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

#[test]
fn an_egress_id_a_route_still_names_is_not_reused_after_its_row_vanishes() {
    let db = Database::open_in_memory().unwrap();
    let kept = db.create_egress_interface(&egress()).unwrap();
    let doomed = db.create_egress_interface(&egress()).unwrap();
    assert_eq!((kept.id, doomed.id), (1, 2));
    insert_route(
        &db,
        Route {
            legs: vec![
                RouteLeg {
                    egress_id: kept.id,
                    weight: 50,
                    path: LegPath::Direct,
                },
                RouteLeg {
                    egress_id: doomed.id,
                    weight: 50,
                    path: LegPath::Direct,
                },
            ],
            failover: Failover::Hold,
        },
    );
    delete_row_behind_the_store(
        &db,
        "DELETE FROM egress_interfaces WHERE id = {}",
        doomed.id,
    );

    // The vanished row held the highest id and the route still names it, so
    // the next egress must not take it over and rebind that leg.
    let next = db.create_egress_interface(&egress()).unwrap();
    assert_eq!(next.id, doomed.id + 1);
    assert_eq!(
        db.list_egress_interfaces()
            .unwrap()
            .iter()
            .map(|egress| egress.id)
            .collect::<Vec<_>>(),
        [0, kept.id, next.id]
    );
}

#[test]
fn a_route_may_stack_wireguard_only_on_wireguard() {
    use base64::Engine;
    let key = |seed: u8| base64::engine::general_purpose::STANDARD.encode([seed; 32]);
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&profile(1)).unwrap();
    for id in 2..=3 {
        db.save_proxy_profile(&ProxyProfile {
            kind: ProxyKind::WireGuard,
            host: "wg.example".into(),
            port: 51820,
            tunnel_addresses: vec!["10.63.0.2/32".into()],
            peer_public_key: Some(key(id as u8 + 10)),
            secrets: ProxySecrets {
                private_key: Some(key(id as u8)),
                ..Default::default()
            },
            ..profile(id)
        })
        .unwrap();
    }
    let chain = |ids: Vec<u32>| RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![Rung::Chain { ids }],
                direct_fallback: false,
            },
        }],
        ..Default::default()
    };
    let server = crate::proxies::Consumer::Server(1);
    db.save_proxy_routing_policy(server, &chain(vec![2, 3, 1]))
        .unwrap();
    for path in [vec![1, 2], vec![2, 1, 3]] {
        assert_eq!(
            refused(db.save_proxy_routing_policy(server, &chain(path.clone()))),
            "ROUTE_CHAIN_TRANSPORT",
            "{path:?}"
        );
    }
    // Turning the hop beneath into a stream proxy would strand the
    // WireGuard hop above it, so the edit is refused.
    let mut changed = db
        .list_proxy_profiles()
        .unwrap()
        .into_iter()
        .find(|p| p.id == 2)
        .unwrap();
    changed.kind = ProxyKind::Socks5;
    changed.revision += 1;
    assert_eq!(
        refused(db.save_proxy_profile(&changed)),
        "ROUTE_CHAIN_TRANSPORT"
    );
}

#[test]
fn a_wireguard_proxy_is_saved_on_only_one_path_per_egress() {
    use base64::Engine;
    let key = |seed: u8| base64::engine::general_purpose::STANDARD.encode([seed; 32]);
    let db = Database::open_in_memory().unwrap();
    for id in 2..=3 {
        db.save_proxy_profile(&ProxyProfile {
            kind: ProxyKind::WireGuard,
            host: "wg.example".into(),
            port: 51820,
            tunnel_addresses: vec!["10.63.0.2/32".into()],
            peer_public_key: Some(key(id as u8 + 10)),
            secrets: ProxySecrets {
                private_key: Some(key(id as u8)),
                ..Default::default()
            },
            ..profile(id)
        })
        .unwrap();
    }
    let ladder = |rung: Rung| RoutingPolicy {
        legs: vec![RouteLeg {
            egress_id: 0,
            weight: 100,
            path: LegPath::Ladder {
                rungs: vec![rung],
                direct_fallback: false,
            },
        }],
        ..Default::default()
    };
    let first = crate::proxies::Consumer::Server(1);
    let second = crate::proxies::Consumer::Server(2);
    db.save_proxy_routing_policy(first, &ladder(Rung::Chain { ids: vec![2, 3] }))
        .unwrap();
    // The chain's lower hop used directly is the same session.
    db.save_proxy_routing_policy(second, &ladder(Rung::Proxy { id: 2 }))
        .unwrap();
    // Its upper hop used directly as well would be a second session to the
    // same server under the same key.
    let error = db
        .save_proxy_routing_policy(second, &ladder(Rung::Proxy { id: 3 }))
        .unwrap_err();
    // The sentence stands alone: nothing in it reads as a storage fault.
    assert!(
        error
            .to_string()
            .starts_with("WireGuard proxy 3 is used on egress 0 by two different paths"),
        "{error}"
    );
    let refusal = error.refusal().expect("a refusal").clone();
    assert_eq!(refusal.code, "WIREGUARD_PROXY_PATH_CONFLICT");
    assert_eq!(
        serde_json::Value::Object(refusal.params),
        serde_json::json!({ "proxyId": 3, "egressId": 0, "paths": [[2, 3], [3]] })
    );
}
