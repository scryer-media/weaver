// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Moved to weaver-server-net-runtime; the tests open the composed database,
// so they stay here under the same module path.
pub use weaver_server_net_runtime::proxies::routed_http::*;

#[cfg(any(test, feature = "test-support"))]
use super::{Consumer, ProxyRuntime};
#[cfg(any(test, feature = "test-support"))]
use crate::proxies::{ProxiesDnsPersistenceQueries, ProxiesPersistenceQueries};
#[cfg(any(test, feature = "test-support"))]
use std::sync::Arc;

// A runtime with nothing saved: every consumer goes direct.
#[cfg(any(test, feature = "test-support"))]
pub fn direct_runtime() -> Arc<ProxyRuntime> {
    ProxyRuntime::new(
        crate::Database::open_in_memory().unwrap(),
        tokio::runtime::Handle::current(),
    )
    .unwrap()
}

// A runtime whose Default outbound goes only through one proxy at
// 127.0.0.1:`port`, which is handed every name.
#[cfg(any(test, feature = "test-support"))]
pub fn outbound_only_through(kind: super::ProxyKind, port: u16) -> Arc<ProxyRuntime> {
    let db = crate::Database::open_in_memory().unwrap();
    db.save_proxy_profile(&super::ProxyProfile {
        dns_profile_id: 0,
        id: 1,
        name: "outbound".into(),
        kind,
        enabled: true,
        host: "127.0.0.1".into(),
        port,
        dns_servers: vec![],
        tunnel_addresses: vec![],
        peer_public_key: None,
        mtu: 1280,
        keepalive_seconds: None,
        timeout_seconds: 300,
        host_key_fingerprint: None,
        natpmp: false,
        forwarded_port: None,
        revision: 1,
        secrets: super::ProxySecrets::default(),
    })
    .unwrap();
    let dns = db
        .save_dns_profile(
            super::DnsProfile {
                kind: crate::proxies::DnsProfileKind::Route,
                id: 0,
                name: "Proxy".into(),
                who: super::DnsWho::Proxy,
                transit: None,
                trust: super::DnsTrust::AsSent,
                server_list_id: None,
                system_fallback: false,
            },
            true,
        )
        .unwrap();
    db.save_proxy_routing_policy(
        Consumer::Outbound,
        &super::RoutingPolicy {
            proxy_ids: vec![1],
            allow_direct: false,
            dns_profile_id: Some(dns.id),
            ..Default::default()
        },
    )
    .unwrap();
    ProxyRuntime::new(db, tokio::runtime::Handle::current()).unwrap()
}

#[cfg(test)]
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};
#[cfg(test)]
use weaver_tunnel::{
    TunnelProvider,
    bridge::Bridge,
    pipe::{DialError, Resolution},
};

#[cfg(test)]
#[path = "routed_http_tests.rs"]
mod tests;
