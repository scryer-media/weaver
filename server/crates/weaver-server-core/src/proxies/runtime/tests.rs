use super::*;
mod review_regressions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn profile(id: u32) -> ProxyProfile {
    ProxyProfile {
        id,
        name: format!("proxy {id}"),
        kind: ProxyKind::Socks5,
        enabled: true,
        host: "127.0.0.1".into(),
        port: 9,
        dns_servers: vec!["192.0.2.53".parse().unwrap()],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 1,
        host_key_fingerprint: None,
        revision: 1,
        secrets: ProxySecrets::default(),
    }
}
fn server(id: u32) -> crate::servers::ServerConfig {
    crate::servers::ServerConfig {
        id,
        host: "provider.invalid".into(),
        port: 119,
        tls: false,
        username: None,
        password: None,
        connections: 1,
        active: true,
        supports_pipelining: true,
        pipelining_depth: None,
        tls_name_mismatch_certificate_der: None,
        priority: 0,
        backfill: false,
        retention_days: 0,
        max_download_speed: 0,
        download_quota: Default::default(),
        tls_ca_cert: None,
    }
}
struct Controlled {
    id: u32,
    fail: AtomicBool,
    calls: Arc<Mutex<Vec<u32>>>,
    remote: Mutex<Vec<tokio::io::DuplexStream>>,
}
#[async_trait::async_trait]
impl TunnelProvider for Controlled {
    async fn dial(&self, _: &str, _: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        self.calls.lock().unwrap().push(self.id);
        if self.fail.load(Ordering::Acquire) {
            return Err(TunnelError::Engine("fixture unavailable".into()));
        }
        let (client, remote) = tokio::io::duplex(64);
        self.remote.lock().unwrap().push(remote);
        Ok(Box::new(client))
    }
    fn describe(&self) -> String {
        "controlled fixture".into()
    }
}
type ControlledRoute = (
    Arc<ConsumerRoute>,
    Vec<Arc<Controlled>>,
    Arc<Mutex<Vec<u32>>>,
);
fn controlled_route() -> ControlledRoute {
    let db = Database::open_in_memory().unwrap();
    let runtime = ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let providers: Vec<_> = (1..=3)
        .map(|id| {
            Arc::new(Controlled {
                id,
                fail: AtomicBool::new(id < 3),
                calls: calls.clone(),
                remote: Mutex::new(vec![]),
            })
        })
        .collect();
    for provider in &providers {
        runtime.profiles.write().unwrap().insert(
            provider.id,
            Arc::new(ProxyHop {
                profile: profile(provider.id),
                provider: provider.clone(),
                wireguard: None,
            }),
        );
    }
    let route = runtime
        .draft_route(
            RoutingPolicy {
                proxy_ids: vec![1, 2, 3],
                allow_direct: false,
            },
            Duration::from_secs(1),
        )
        .unwrap();
    (route, providers, calls)
}

#[tokio::test(start_paused = true)]
async fn ladder_order_cooldown_and_primary_recovery() {
    let (route, providers, calls) = controlled_route();
    let _first = Ladder(route.clone())
        .dial("provider.invalid", 119)
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3]);
    providers[0].fail.store(false, Ordering::Release);
    let _second = Ladder(route.clone())
        .dial("provider.invalid", 119)
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3, 3]);
    tokio::time::advance(Duration::from_secs(30)).await;
    let _third = Ladder(route.clone())
        .dial("provider.invalid", 119)
        .await
        .unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3, 3, 1]);
    assert_eq!(route.status().selected_proxy_id, Some(1));
    assert!(!route.status().failures.iter().any(|(id, _)| *id == 1));
}

#[tokio::test(start_paused = true)]
async fn one_recovery_probe_and_cancellation_releases_it() {
    let (route, _, _) = controlled_route();
    route.fail(1, "unreachable");
    assert!(route.begin(1).is_none());
    tokio::time::advance(Duration::from_secs(30)).await;
    let attempt = route.begin(1).unwrap();
    assert!(route.begin(1).is_none());
    drop(attempt);
    assert!(route.begin(1).is_some());
}

#[tokio::test]
async fn stream_loss_cools_route_without_splicing_transfer() {
    let (route, providers, calls) = controlled_route();
    let outcome = Arc::new(weaver_tunnel::bridge::ConnectionOutcome::default());
    let mut stream = Ladder(route.clone())
        .dial_observed("provider.invalid", 119, outcome.clone())
        .await
        .unwrap();
    providers[2].remote.lock().unwrap().clear();
    assert_eq!(stream.read(&mut [0; 1]).await.unwrap(), 0);
    // EOF alone is not a failure. The protocol reports it only when it still
    // expects response bytes, rather than after QUIT or a terminal response.
    assert!(route.begin(3).is_some());
    outcome.failed();
    assert!(route.begin(3).is_none());
    assert_eq!(*calls.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test]
async fn blocked_ladder_and_disabled_profile_never_dial_host_destination() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let db = Database::open_in_memory().unwrap();
    let mut disabled = profile(1);
    disabled.enabled = false;
    db.save_proxy_profile(&disabled).unwrap();
    let runtime = ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    for ids in [vec![], vec![1]] {
        let policy = RoutingPolicy {
            proxy_ids: ids,
            allow_direct: false,
        };
        let route = runtime
            .draft_route(policy.clone(), Duration::from_millis(30))
            .unwrap();
        assert!(
            Ladder(route.clone())
                .dial("127.0.0.1", addr.port())
                .await
                .is_err()
        );
        assert_eq!(route.status().state, RouteState::Blocked);
        assert_eq!(route.policy, policy);
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn direct_fallback_is_only_used_when_permitted() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let runtime = ProxyRuntime::new(
        Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap();
    let route = runtime
        .draft_route(
            RoutingPolicy {
                proxy_ids: vec![1],
                allow_direct: true,
            },
            Duration::from_millis(30),
        )
        .unwrap();
    let _stream = Ladder(route.clone())
        .dial("127.0.0.1", listener.local_addr().unwrap().port())
        .await
        .unwrap();
    assert_eq!(route.status().state, RouteState::Direct);
    let _accepted = listener.accept().await.unwrap();
}

#[tokio::test]
async fn policy_save_closes_active_direct_sockets_before_reload_returns() {
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server(1)).unwrap();
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let registry = runtime.nntp_sockets(1).unwrap();
    assert!(
        runtime.nntp_bridge(1).unwrap().is_none(),
        "direct socket path remains direct"
    );
    let fixture = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.write_all(b"200 fixture\r\n").await.unwrap();
        let mut data = [0; 1];
        tcp.read(&mut data).await
    });
    let config = weaver_nntp::ServerConfig {
        host: addr.ip().to_string(),
        port: addr.port(),
        tls: false,
        revocation: Some(registry.clone()),
        pipelining: weaver_nntp::PipeliningCapability::Known(false),
        ..Default::default()
    };
    let _connection = weaver_nntp::NntpConnection::connect(&config).await.unwrap();
    db.save_proxy_routing_policy(
        Consumer::Server(1),
        &RoutingPolicy {
            proxy_ids: vec![],
            allow_direct: false,
        },
    )
    .unwrap();
    runtime.reload().await.unwrap();
    assert!(registry.check().is_err());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), fixture)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    assert!(weaver_nntp::NntpConnection::connect(&config).await.is_err());
    runtime.stop_all().await;
}

#[tokio::test]
async fn reload_preserves_unaffected_sessions_and_revokes_changed_profile() {
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server(1)).unwrap();
    db.insert_server(&server(2)).unwrap();
    let mut p = profile(1);
    db.save_proxy_profile(&p).unwrap();
    db.save_proxy_routing_policy(
        Consumer::Server(1),
        &RoutingPolicy {
            proxy_ids: vec![1],
            allow_direct: false,
        },
    )
    .unwrap();
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routed = runtime
        .route(Consumer::Server(1), Duration::from_secs(1))
        .unwrap();
    let direct = runtime
        .route(Consumer::Server(2), Duration::from_secs(1))
        .unwrap();
    runtime.reload().await.unwrap();
    assert!(Arc::ptr_eq(
        &routed,
        &runtime
            .route(Consumer::Server(1), Duration::from_secs(1))
            .unwrap()
    ));
    p.enabled = false;
    p.revision += 1;
    db.save_proxy_profile(&p).unwrap();
    runtime.reload().await.unwrap();
    assert!(routed.is_revoked());
    assert!(!direct.is_revoked());
    let new = runtime
        .route(Consumer::Server(1), Duration::from_secs(1))
        .unwrap();
    assert!(!new.policy.allow_direct);
    runtime.stop_all().await;
    assert!(direct.is_revoked());
    assert!(
        runtime
            .route(Consumer::Server(2), Duration::from_secs(1))
            .is_err()
    );
}

#[tokio::test]
async fn deleting_consumer_revokes_routes_and_releases_profile_reference() {
    let db = Database::open_in_memory().unwrap();
    db.insert_server(&server(1)).unwrap();
    db.save_proxy_profile(&profile(1)).unwrap();
    db.save_proxy_routing_policy(
        Consumer::Server(1),
        &RoutingPolicy {
            proxy_ids: vec![1],
            allow_direct: false,
        },
    )
    .unwrap();
    assert!(db.delete_proxy_profile(1).is_err());
    let runtime = ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let route = runtime
        .route(Consumer::Server(1), Duration::from_secs(1))
        .unwrap();
    db.delete_server(1).unwrap();
    runtime.reload().await.unwrap();
    assert!(route.is_revoked());
    assert!(
        runtime
            .route(Consumer::Server(1), Duration::from_secs(1))
            .is_err()
    );
    db.delete_proxy_profile(1).unwrap();
}

#[test]
fn routing_validation_and_atomic_consumer_save_preserve_existing_policy() {
    let db = Database::open_in_memory().unwrap();
    let mut server = server(1);
    assert_eq!(
        db.proxy_routing_policy(Consumer::Server(1)).unwrap(),
        RoutingPolicy::default()
    );
    db.save_proxy_profile(&profile(1)).unwrap();
    let policy = RoutingPolicy {
        proxy_ids: vec![1],
        allow_direct: false,
    };
    db.insert_server_with_routing(&server, Some(&policy))
        .unwrap();
    server.host = "new.invalid".into();
    db.update_server(&server).unwrap();
    assert_eq!(
        db.proxy_routing_policy(Consumer::Server(1)).unwrap(),
        policy
    );
    for ids in [vec![1, 1], vec![999], (1..=9).collect()] {
        server.host = "must-not-save.invalid".into();
        assert!(
            db.update_server_with_routing(
                &server,
                Some(&RoutingPolicy {
                    proxy_ids: ids,
                    allow_direct: true
                })
            )
            .is_err()
        );
        assert_eq!(db.list_servers().unwrap()[0].host, "new.invalid");
        assert_eq!(
            db.proxy_routing_policy(Consumer::Server(1)).unwrap(),
            policy
        );
    }
}

#[test]
fn profile_secrets_are_encrypted_and_wrong_key_fails_closed() {
    use crate::persistence::{encryption::EncryptionKey, sql_runtime::SqlRuntime};
    let mut db = Database::open_in_memory().unwrap();
    let mut p = profile(1);
    p.secrets.username = Some("private-username".into());
    p.secrets.password = Some("private-password".into());
    db.save_proxy_profile(&p).unwrap();
    let store = db.datastore();
    let rows = db
        .run_sql_blocking_read(async move {
            SqlRuntime::fetch_all(
                store.read_exec(),
                "SELECT config, password FROM proxy_profiles",
                &[],
            )
            .await
        })
        .unwrap();
    let raw = format!(
        "{} {}",
        rows[0].text("config").unwrap(),
        rows[0].text("password").unwrap()
    );
    assert!(!raw.contains("private-username"));
    assert!(!raw.contains("private-password"));
    assert_eq!(
        db.list_proxy_profiles().unwrap()[0]
            .secrets
            .password
            .as_deref(),
        Some("private-password")
    );
    db.set_encryption_key(EncryptionKey::generate());
    assert!(db.list_proxy_profiles().is_err());
}

#[test]
fn backup_restores_encrypted_profiles_ordered_routes_and_host_trust() {
    use crate::persistence::encryption::EncryptionKey;
    let source = Database::open_in_memory().unwrap();
    let key = source.encryption_key().unwrap().clone();
    source.insert_server(&server(1)).unwrap();
    let mut p = profile(1);
    p.secrets.password = Some("backup-only-secret".into());
    source.save_proxy_profile(&p).unwrap();
    source.save_proxy_profile(&profile(2)).unwrap();
    source.pin_proxy_host_key(1, 1, "SHA256:fixture").unwrap();
    let policy = RoutingPolicy {
        proxy_ids: vec![2, 1],
        allow_direct: false,
    };
    source
        .save_proxy_routing_policy(Consumer::Server(1), &policy)
        .unwrap();
    assert!(source.has_encrypted_credentials().unwrap());
    source.validate_encrypted_credentials(&key).unwrap();
    assert!(
        source
            .validate_encrypted_credentials(&EncryptionKey::generate())
            .is_err()
    );
    let backup = tempfile::NamedTempFile::new().unwrap();
    source.export_stable_state(backup.path()).unwrap();
    let artifact = Database::open(backup.path()).unwrap();
    assert_eq!(
        artifact.proxy_routing_policy(Consumer::Server(1)).unwrap(),
        policy,
        "exported routing policy"
    );
    drop(artifact);
    assert!(
        !std::fs::read(backup.path())
            .unwrap()
            .windows(18)
            .any(|w| w == b"backup-only-secret")
    );
    let mut target = Database::open_in_memory().unwrap();
    target.set_encryption_key(key);
    target.import_stable_state(backup.path()).unwrap();
    assert_eq!(
        target.list_proxy_profiles().unwrap().len(),
        2,
        "imported profiles"
    );
    assert_eq!(
        target.proxy_routing_policy(Consumer::Server(1)).unwrap(),
        policy
    );
    let profiles = target.list_proxy_profiles().unwrap();
    assert_eq!(
        profiles[0].secrets.password.as_deref(),
        Some("backup-only-secret")
    );
    assert_eq!(
        profiles[0].host_key_fingerprint.as_deref(),
        Some("SHA256:fixture")
    );
    // Rewriting profiles with a replacement key preserves all secret fields and trust.
    let replacement = EncryptionKey::generate();
    target.set_encryption_key(replacement.clone());
    for profile in &profiles {
        target.save_proxy_profile(profile).unwrap();
    }
    target.validate_encrypted_credentials(&replacement).unwrap();
    assert_eq!(
        target.list_proxy_profiles().unwrap()[0].secrets.password,
        profiles[0].secrets.password
    );
    assert!(!target.restore_target_is_pristine().unwrap());
}

#[test]
fn ssh_trust_cannot_be_overwritten_by_conflicting_or_stale_handshakes() {
    let db = Database::open_in_memory().unwrap();
    let mut p = profile(1);
    p.kind = ProxyKind::Ssh;
    p.secrets.username = Some("fixture".into());
    p.secrets.password = Some("fixture".into());
    db.save_proxy_profile(&p).unwrap();
    db.pin_proxy_host_key(1, 1, "SHA256:first").unwrap();
    assert_eq!(
        db.proxy_host_key(1, 1).unwrap().as_deref(),
        Some("SHA256:first")
    );
    assert!(db.pin_proxy_host_key(1, 1, "SHA256:other").is_err());
    p.revision = 2;
    let saved = db.save_proxy_profile(&p).unwrap();
    assert_eq!(saved.host_key_fingerprint.as_deref(), Some("SHA256:first"));
    assert_eq!(
        db.proxy_host_key(1, 2).unwrap().as_deref(),
        Some("SHA256:first")
    );
    assert!(db.pin_proxy_host_key(1, 1, "SHA256:stale").is_err());
    assert!(db.proxy_host_key(1, 1).is_err());
    assert!(db.pin_proxy_host_key(1, 2, "SHA256:reset").is_err());
    p.revision = 3;
    db.reset_proxy_host_key(&p).unwrap();
    assert!(db.proxy_host_key(1, 3).unwrap().is_none());
    assert!(db.pin_proxy_host_key(1, 2, "SHA256:stale").is_err());
    db.pin_proxy_host_key(1, 3, "SHA256:reset").unwrap();
    assert_eq!(
        db.proxy_host_key(1, 3).unwrap().as_deref(),
        Some("SHA256:reset")
    );
}

#[tokio::test]
async fn cancelled_route_wakes_waiters_and_prevents_bridge_recreation() {
    let (route, _, _) = controlled_route();
    let waiting = route.clone();
    let waiter = tokio::spawn(async move {
        waiting.cancelled().await;
    });
    route.revoke().await;
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .unwrap()
        .unwrap();
    assert!(route.bridge().is_err());
    assert!(route.begin(1).is_none());
}
