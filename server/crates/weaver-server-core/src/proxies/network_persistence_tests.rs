use super::*;
use crate::proxies::{Failover, ProxyKind, ProxySecrets, RouteLeg};

fn egress() -> EgressInterface {
    EgressInterface {
        id: 0,
        name: "WAN A".into(),
        binding: EgressBinding::SourceAddress {
            address: "192.0.2.1".parse().unwrap(),
        },
        enabled: true,
        max_download_speed: 1_000_000,
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
    assert!(db.delete_egress_interface(0).is_err());
    assert!(
        db.create_egress_interface(&EgressInterface::system())
            .is_err()
    );
    let mut system = EgressInterface::system();
    system.max_download_speed = 5_000_000;
    assert_eq!(db.update_egress_interface(&system).unwrap(), system);
    system.name = "renamed".into();
    assert!(db.update_egress_interface(&system).is_err());
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
    assert!(
        db.delete_egress_interface(egress.id)
            .unwrap_err()
            .to_string()
            .contains("referenced")
    );
    assert!(
        db.delete_proxy_pool(pool.id)
            .unwrap_err()
            .to_string()
            .contains("referenced")
    );
    assert!(
        db.delete_proxy_profile(1)
            .unwrap_err()
            .to_string()
            .contains("pool")
    );
    assert!(
        db.delete_proxy_profile(3)
            .unwrap_err()
            .to_string()
            .contains("referenced")
    );
    pool.member_ids = vec![1, 3];
    assert!(
        db.update_proxy_pool(&pool)
            .unwrap_err()
            .to_string()
            .contains("repeat")
    );
    assert_eq!(db.list_proxy_pools().unwrap()[0].member_ids, [1, 2]);
    let mut changed = profile(1);
    changed.kind = ProxyKind::HttpConnect;
    changed.revision += 1;
    assert!(
        db.save_proxy_profile(&changed)
            .unwrap_err()
            .to_string()
            .contains("kind")
    );
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
        assert!(
            db.delete_proxy_profile(id)
                .unwrap_err()
                .to_string()
                .contains("referenced")
        );
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
