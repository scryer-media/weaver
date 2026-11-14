// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::metrics::Resolver;
use crate::pipe::Egress;
use crate::resolution::record;
use crate::test_support::{TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT, WireGuardTestPeer};
use crate::{NoopTunnelObserver, TunnelProvider, WireGuardTunnelProvider};
use std::time::Duration;

fn egress() -> Arc<Egress> {
    Arc::new(crate::test_support::loopback::egress(
        4,
        Duration::from_secs(30),
    ))
}

fn host_lookups() -> u64 {
    let metrics = crate::metrics::snapshot();
    let resolved: u64 = metrics
        .resolutions
        .iter()
        .filter(|(by, ..)| *by == Resolver::Host)
        .map(|(_, ok, failed)| ok + failed)
        .sum();
    let held: u64 = metrics
        .cache
        .iter()
        .filter(|(by, ..)| *by == Resolver::Host)
        .map(|(.., count)| count)
        .sum();
    resolved + held
}

// A WireGuard hop whose endpoint is written as a name brings its tunnel up
// on the address the egress beneath resolved, and test via says the host's
// DNS answered.
#[tokio::test]
async fn a_wireguard_endpoint_name_is_asked_of_the_egress_beneath() {
    let peer = WireGuardTestPeer::start().await;
    let mut spec = peer.client_spec("endpoint-name");
    spec.endpoint_host = "localhost".to_string();
    let bottom = egress();
    let provider = WireGuardTunnelProvider::new(spec, Arc::new(NoopTunnelObserver))
        .with_udp_factory(bottom.clone())
        .with_endpoint_resolver(Arc::new(EndpointNames(bottom)));

    let before = host_lookups();
    let recording = Arc::new(record::Recording::default());
    recording
        .scope(async {
            provider
                .dial(&TEST_PEER_ADDRESS.to_string(), TEST_PEER_HTTP_PORT)
                .await
                .expect("dial through a tunnel whose endpoint is a name")
        })
        .await;

    let resolution = recording.finish("localhost", 0, None).unwrap();
    assert_eq!(resolution.by, record::ResolvedBy::Host);
    assert!(
        resolution.addresses.contains(&peer.endpoint().ip()),
        "{:?}",
        resolution.addresses
    );
    assert!(host_lookups() > before);
}

// A leg that hands names on cannot answer an endpoint name.
#[tokio::test]
async fn a_leg_that_resolves_no_names_refuses_an_endpoint_name() {
    struct HandsOn;
    #[async_trait::async_trait]
    impl Dialer for HandsOn {
        async fn dial(
            &self,
            _target: &crate::pipe::Target,
        ) -> Result<crate::pipe::Dialed, crate::pipe::DialError> {
            unreachable!("an endpoint name is only resolved")
        }
        async fn resolve(&self, _host: &str) -> Result<Resolution, crate::pipe::DialError> {
            Ok(Resolution::Unresolved)
        }
        fn budget(&self) -> Duration {
            Duration::from_secs(30)
        }
        fn describe(&self) -> String {
            "the test leg".into()
        }
    }
    let error = crate::endpoint::EndpointResolver::resolve(
        &EndpointNames(Arc::new(HandsOn)),
        "vpn.example.test",
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("resolves none"), "{error}");
}

// A shared WireGuard tunnel built without a resolver refuses an endpoint
// written as a name, and the host's own DNS is never asked for it.
#[tokio::test]
async fn a_wireguard_tunnel_without_a_resolver_refuses_an_endpoint_name() {
    let peer = WireGuardTestPeer::start().await;
    let mut spec = peer.client_spec("no-resolver");
    spec.endpoint_host = "vpn.example.test".to_string();
    let port = spec.endpoint_port;
    let provider = WireGuardTunnelProvider::new(spec, Arc::new(NoopTunnelObserver));

    let recording = Arc::new(record::Recording::default());
    let error = recording
        .scope(provider.dial(&TEST_PEER_ADDRESS.to_string(), TEST_PEER_HTTP_PORT))
        .await
        .err()
        .expect("an endpoint name with no resolver is refused");

    assert!(
        error.to_string().contains("no resolver was given"),
        "{error}"
    );
    assert!(
        recording.finish("vpn.example.test", port, None).is_none(),
        "the endpoint name reached a resolver"
    );
}

// A shared HTTP/3 tunnel built without a resolver refuses a proxy written as
// a name, and the host's own DNS is never asked for it.
#[tokio::test]
async fn an_http3_tunnel_without_a_resolver_refuses_a_proxy_name() {
    let provider = crate::Http3TunnelProvider::new(crate::Http3TunnelSpec {
        proxy_config_id: "no-resolver".into(),
        revision: "1".into(),
        host: "proxy.example.test".into(),
        port: 443,
        credentials: None,
        request_timeout: Duration::from_secs(300),
    })
    .unwrap();

    let recording = Arc::new(record::Recording::default());
    let error = recording
        .scope(provider.dial("origin.example.test", 80))
        .await
        .err()
        .expect("a proxy name with no resolver is refused");

    assert!(
        error.to_string().contains("no resolver was given"),
        "{error}"
    );
    assert!(
        recording.finish("proxy.example.test", 443, None).is_none(),
        "the proxy name reached a resolver"
    );
}
