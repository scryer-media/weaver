// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Resolver {
    failed_family: u16,
    disconnect: bool,
    stall: bool,
    queries: Arc<Mutex<Vec<u16>>>,
}

#[async_trait::async_trait]
impl Carrier for Resolver {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        assert_eq!(server.to_string(), "192.0.2.53");
        let (client, mut server) = tokio::io::duplex(1024);
        let failed_family = self.failed_family;
        let disconnect = self.disconnect;
        let stall = self.stall;
        let queries = self.queries.clone();
        tokio::spawn(async move {
            let size = server.read_u16().await.unwrap() as usize;
            let mut reply = vec![0; size];
            server.read_exact(&mut reply).await.unwrap();
            let family = u16_at(&reply, size - 4).unwrap();
            queries.lock().unwrap().push(family);
            let fail = failed_family == family || failed_family == 0;
            if fail && stall {
                // Keep the peer open without producing a DNS response.
                let _server = server;
                std::future::pending::<()>().await;
                return;
            }
            if fail && disconnect {
                return;
            }
            reply[2..4].copy_from_slice(&(if fail { 0x8182u16 } else { 0x8180 }).to_be_bytes());
            if !fail {
                reply[6..8].copy_from_slice(&1u16.to_be_bytes());
                reply.extend_from_slice(&[0xc0, 0x0c]);
                reply.extend_from_slice(&family.to_be_bytes());
                reply.extend_from_slice(&[0, 1, 0, 0, 0, 1]);
                let address = if family == 1 {
                    vec![192, 0, 2, 7]
                } else {
                    vec![0x20; 16]
                };
                reply.extend_from_slice(&(address.len() as u16).to_be_bytes());
                reply.extend_from_slice(&address);
            }
            server.write_u16(reply.len() as u16).await.unwrap();
            server.write_all(&reply).await.unwrap();
        });
        Ok(Box::new(client))
    }
}

#[tokio::test]
async fn one_family_survives_peer_protocol_or_transport_failure() {
    for disconnect in [false, true] {
        for failed_family in [1, 28] {
            let resolver = Resolver {
                failed_family,
                disconnect,
                stall: false,
                queries: Default::default(),
            };
            let addresses = resolve_name(
                &resolver,
                &[DnsServer::plain("192.0.2.53".parse().unwrap())],
                "fixture.invalid",
            )
            .await
            .unwrap()
            .addresses;
            assert_eq!(addresses.len(), 1);
            assert_eq!(addresses[0].is_ipv4(), failed_family == 28);
            assert_eq!(*resolver.queries.lock().unwrap(), [1, 28]);
        }
    }
}

#[tokio::test]
async fn both_family_failures_remain_an_error() {
    let resolver = Resolver {
        failed_family: 0,
        disconnect: false,
        stall: false,
        queries: Default::default(),
    };
    let unanswered = resolve_name(
        &resolver,
        &[DnsServer::plain("192.0.2.53".parse().unwrap())],
        "fixture.invalid",
    )
    .await
    .unwrap_err();
    assert!(unanswered.reached, "the server was reached and refused");
    assert_eq!(*resolver.queries.lock().unwrap(), [1, 28]);
}

#[tokio::test(start_paused = true)]
async fn a_silent_family_does_not_consume_the_outer_request_deadline() {
    for failed_family in [1, 28] {
        let resolver = Resolver {
            failed_family,
            disconnect: false,
            stall: true,
            queries: Default::default(),
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            resolve_name(
                &resolver,
                &[DnsServer::plain("192.0.2.53".parse().unwrap())],
                "fixture.invalid",
            ),
        )
        .await
        .expect("working family must survive before the request deadline")
        .unwrap()
        .addresses;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].is_ipv4(), failed_family == 28);
        assert_eq!(*resolver.queries.lock().unwrap(), [1, 28]);
    }
}

#[tokio::test]
async fn both_successful_families_are_retained() {
    let resolver = Resolver {
        failed_family: 255,
        disconnect: false,
        stall: false,
        queries: Default::default(),
    };
    let result = resolve_name(
        &resolver,
        &[DnsServer::plain("192.0.2.53".parse().unwrap())],
        "fixture.invalid",
    )
    .await
    .unwrap()
    .addresses;
    assert_eq!(result.len(), 2);
    assert!(result.iter().any(IpAddr::is_ipv4));
    assert!(result.iter().any(IpAddr::is_ipv6));
}

// A route that cannot carry anything: every dial is refused, or never
// completes.
struct DeadRoute {
    hang: bool,
    dials: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl Carrier for DeadRoute {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        self.dials.lock().unwrap().push(server.to_string());
        if self.hang {
            std::future::pending::<()>().await;
        }
        Err(TunnelError::Engine("route refused the connection".into()))
    }
}

#[tokio::test(start_paused = true)]
async fn a_dead_route_is_dialled_once_per_server_not_once_per_family() {
    for hang in [false, true] {
        let route = DeadRoute {
            hang,
            dials: Default::default(),
        };
        let started = tokio::time::Instant::now();
        let result = resolve_name(
            &route,
            &[
                DnsServer::plain("192.0.2.53".parse().unwrap()),
                DnsServer::plain("192.0.2.54".parse().unwrap()),
            ],
            "fixture.invalid",
        )
        .await;
        assert!(!result.unwrap_err().reached, "hang={hang}");
        assert_eq!(
            *route.dials.lock().unwrap(),
            ["192.0.2.53", "192.0.2.54"],
            "hang={hang}"
        );
        if hang {
            assert_eq!(started.elapsed(), SERVER_TIMEOUT * 2);
        }
    }
}

// Each listed server gets the whole time limit for both families, however
// long the servers before it took. Ten that take the dial and never answer
// cost ten limits, 50 seconds, one each and not one per family.
#[tokio::test(start_paused = true)]
async fn each_server_gets_one_full_time_limit_for_both_families() {
    let servers: Vec<DnsServer> = (1..=10)
        .map(|n| DnsServer::plain(format!("192.0.2.{n}").parse().unwrap()))
        .collect();
    let started = tokio::time::Instant::now();
    let unanswered = resolve_name(&super::ttl::Silent::default(), &servers, "fixture.invalid")
        .await
        .unwrap_err();
    assert!(unanswered.reached);
    assert_eq!(started.elapsed(), SERVER_TIMEOUT * 10);
    assert_eq!(started.elapsed(), std::time::Duration::from_secs(50));
}

// The first server takes the dial and never answers; the second answers at
// once, on a fresh limit of its own.
struct SilentFirst(super::ttl::Silent, super::ttl::Zone);

#[async_trait::async_trait]
impl Carrier for SilentFirst {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn crate::TunnelStream>, TunnelError> {
        if server.to_string() == "192.0.2.53" {
            self.0.open(server).await
        } else {
            self.1.open(server).await
        }
    }
}

#[tokio::test(start_paused = true)]
async fn a_silent_server_costs_one_time_limit_and_the_next_is_asked_afresh() {
    let carrier = SilentFirst(
        super::ttl::Silent::default(),
        super::ttl::Zone::new(|_, qtype, query| match qtype {
            1 => super::ttl::v4(query, 300),
            _ => super::ttl::respond(query, 0, &[], &[super::ttl::soa(300, 300)]),
        }),
    );
    let started = tokio::time::Instant::now();
    let lookup = resolve_name(
        &carrier,
        &[
            DnsServer::plain("192.0.2.53".parse().unwrap()),
            DnsServer::plain("192.0.2.54".parse().unwrap()),
        ],
        "fixture.invalid",
    )
    .await
    .unwrap();
    assert_eq!(lookup.addresses, ["192.0.2.7".parse::<IpAddr>().unwrap()]);
    assert_eq!(started.elapsed(), SERVER_TIMEOUT);
}
