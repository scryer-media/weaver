use super::*;

#[tokio::test]
async fn http3_profile_test_never_falls_back_to_tcp_when_dns_is_missing() {
    let db = Database::open_in_memory().unwrap();
    let runtime = ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut p = profile(1);
    p.kind = ProxyKind::Http3Connect;
    p.port = listener.local_addr().unwrap().port();
    p.dns_servers.clear();
    assert!(
        runtime
            .test_profile(&p)
            .await
            .unwrap_err()
            .contains("routed DNS")
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    runtime.stop_all().await;
}

#[tokio::test]
async fn http3_failure_blocks_direct_destination_and_preserves_policy() {
    let db = Database::open_in_memory().unwrap();
    let destination = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let udp = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut p = profile(1);
    p.kind = ProxyKind::Http3Connect;
    p.port = udp.local_addr().unwrap().port();
    db.save_proxy_profile(&p).unwrap();
    let runtime = ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    let policy = RoutingPolicy {
        proxy_ids: vec![1],
        allow_direct: false,
    };
    let route = runtime
        .draft_route(policy.clone(), Duration::from_millis(100))
        .unwrap();
    let result = Ladder(route.clone())
        .dial("127.0.0.1", destination.local_addr().unwrap().port())
        .await;
    assert!(result.is_err());
    assert_eq!(route.policy, policy);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), destination.accept())
            .await
            .is_err()
    );
    runtime.stop_all().await;
}

#[test]
fn http3_credentials_are_validated_and_persisted_through_existing_secret_storage() {
    let db = Database::open_in_memory().unwrap();
    let mut p = profile(1);
    p.kind = ProxyKind::Http3Connect;
    p.secrets.username = Some("fixture-user".into());
    p.secrets.password = Some("fixture-password".into());
    db.save_proxy_profile(&p).unwrap();
    let saved = db.list_proxy_profiles().unwrap().remove(0);
    assert_eq!(saved.kind, ProxyKind::Http3Connect);
    assert_eq!(saved.secrets.password.as_deref(), Some("fixture-password"));
    assert!(
        !serde_json::to_string(&saved)
            .unwrap()
            .contains("fixture-password")
    );
    for username in ["bad:name", "bad\r\nname", ""] {
        p.secrets.username = Some(username.into());
        assert!(p.validate().is_err());
    }
    p.secrets.username = None;
    assert!(
        p.validate().is_err(),
        "password alone must not silently disable authentication"
    );
}
