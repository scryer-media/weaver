// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[tokio::test]
async fn http3_profile_test_never_falls_back_to_tcp_when_dns_is_missing() {
    let db = Database::open_in_memory().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut p = profile(1);
    p.kind = ProxyKind::Http3Connect;
    p.port = listener.local_addr().unwrap().port();
    p.dns_servers.clear();
    db.save_proxy_profile(&p).unwrap();
    let runtime = ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap();
    let route = Route {
        legs: vec![RouteLeg {
            egress_id: SYSTEM_EGRESS_ID,
            weight: 100,
            path: LegPath::Direct,
        }],
        failover: Failover::default(),
    };
    let tests = runtime
        .network
        .test_via(&route, TestSubject::Proxy(1), None)
        .await
        .unwrap();
    assert_eq!(tests[0].outcome, Err(ProxyFault::TestNeedsDns));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
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
