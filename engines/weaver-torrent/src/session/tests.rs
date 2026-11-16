// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use tempfile::TempDir;

use super::add::torrent_file_from_info_bytes;
use super::peer_finder;
use super::peer_sources::{merge_tracker_tiers, metainfo_tracker_tiers};
use super::{
    AddTorrent, AddTorrentOptions, Discovery, ManagedTorrentHandle, Session, SessionOptions,
    is_bare_info_hash, torrent_from_bytes,
};
use crate::tracker::ToldPorts;
use crate::tracker::Tracker;

fn url(s: &str) -> url::Url {
    url::Url::parse(s).unwrap()
}

// Tiers of trackers that came with the torrent.
fn tiers(t: &[&[&str]]) -> Vec<Vec<Tracker>> {
    t.iter()
        .map(|tier| tier.iter().map(|u| Tracker::torrent(url(u))).collect())
        .collect()
}

fn operator(u: &str) -> Vec<Tracker> {
    vec![Tracker::operator(url(u))]
}

fn bstr(s: &str) -> String {
    format!("{}:{s}", s.len())
}

// The torrent index answers each lookup of an accepted connection as a
// scan over every torrent does, over a seeded set with a v1, a hybrid
// and a v2 torrent, a torrent added and removed, and an unknown hash.
#[test]
fn the_torrent_index_finds_what_a_scan_of_every_torrent_finds() {
    use super::{Id20, InfoHashes, TorrentIndex};
    use crate::core::hash_id::Id32;
    use crate::mse::{EncryptionPolicy, Incoming, obfuscated_hash};
    let policies = [
        EncryptionPolicy::Disabled,
        EncryptionPolicy::Prefer,
        EncryptionPolicy::Require,
    ];
    let v1 = |b| InfoHashes {
        v1: Some(Id20::new([b; 20])),
        v2: None,
    };
    let seeded: Vec<(usize, InfoHashes, Option<EncryptionPolicy>)> = vec![
        (0, v1(1), None),
        (
            1,
            InfoHashes {
                v1: Some(Id20::new([2; 20])),
                v2: Some(Id32::new([3; 32])),
            },
            Some(EncryptionPolicy::Require),
        ),
        (
            2,
            InfoHashes {
                v1: None,
                v2: Some(Id32::new([4; 32])),
            },
            Some(EncryptionPolicy::Disabled),
        ),
        (3, v1(5), Some(EncryptionPolicy::Prefer)),
        (4, v1(6), None),
    ];
    let removed = (5, v1(7), Some(EncryptionPolicy::Prefer));
    let unknown = Id20::new([8; 20]);

    // Every subset of the torrents, as a session may hold.
    for subset in 0..1u32 << seeded.len() {
        let held: Vec<_> = seeded
            .iter()
            .filter(|(id, _, _)| subset & (1 << id) != 0)
            .copied()
            .collect();
        let mut index = TorrentIndex::default();
        for &(id, hashes, encryption) in &held {
            index.insert(id, hashes, encryption);
        }
        index.insert(removed.0, removed.1, removed.2);
        index.remove(removed.0, removed.1, removed.2);

        let candidates: Vec<Id20> = seeded
            .iter()
            .chain([&removed])
            .flat_map(|(_, h, _)| h.ids20())
            .chain([unknown])
            .collect();
        for session in policies {
            let every = || {
                std::iter::once(session).chain(held.iter().map(|(_, _, p)| p.unwrap_or(session)))
            };
            assert_eq!(
                index.incoming(session),
                Incoming {
                    encrypted: !every().all(|p| p == EncryptionPolicy::Disabled),
                    plaintext: every().any(|p| p != EncryptionPolicy::Require),
                },
                "{subset:#b} {session:?}"
            );
            for &hash in &candidates {
                let obfuscated = obfuscated_hash(&hash);
                let scanned = held.iter().find_map(|(id, hashes, p)| {
                    hashes
                        .ids20()
                        .find(|ih| obfuscated_hash(ih) == obfuscated)
                        .map(|ih| (ih, *id, p.unwrap_or(session)))
                });
                assert_eq!(index.by_obfuscated(&obfuscated, session), scanned);
                let scanned = held
                    .iter()
                    .find(|(_, hashes, _)| hashes.answers_to(hash))
                    .map(|(id, _, _)| *id);
                assert_eq!(index.by_hash(hash), scanned);
            }
        }
    }
}

#[test]
fn the_session_announces_the_tcp_port_a_mapper_got_nat_pmp_before_upnp() {
    use super::AnnouncePort;
    use crate::upnp::{
        PortMapped,
        PortmapProtocol::{Tcp, Udp},
        PortmapTransport::{NatPmp, Upnp},
    };
    let event = |transport, protocol, external_port| PortMapped {
        transport,
        protocol,
        local_port: 6881,
        external_port,
        external_ip: None,
        error: None,
    };
    let port = AnnouncePort::new(Some(6881), None);
    let v4 = |port: &AnnouncePort| port.get().v4;
    assert_eq!(port.get(), ToldPorts::both(Some(6881)));
    port.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
    assert_eq!(v4(&port), Some(40001));
    // Peers connect over TCP; a UDP mapping does not move the port.
    port.on_port_mapping(&event(Upnp, Udp, Some(40002)));
    assert_eq!(v4(&port), Some(40001));
    port.on_port_mapping(&event(NatPmp, Tcp, Some(50000)));
    assert_eq!(v4(&port), Some(50000));
    // A failed or removed mapping hands the port back.
    port.on_port_mapping(&event(NatPmp, Tcp, None));
    assert_eq!(v4(&port), Some(40001));
    port.on_port_mapping(&event(Upnp, Tcp, None));
    assert_eq!(v4(&port), Some(6881));
    // An announce hears of a change of the port, and only of a change.
    let mut announce = port.subscribe();
    port.on_port_mapping(&event(Upnp, Udp, Some(40002)));
    port.on_port_mapping(&event(NatPmp, Tcp, None));
    assert!(!announce.has_changed().unwrap());
    port.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
    assert!(announce.has_changed().unwrap());
    assert_eq!(announce.borrow_and_update().v4, Some(40001));
    // Nothing to announce stays nothing.
    let none = AnnouncePort::new(None, None);
    none.on_port_mapping(&event(Upnp, Tcp, Some(40001)));
    assert_eq!(none.get(), ToldPorts::default());
    assert_eq!((none.external(false), none.external(true)), (None, None));
}

// libtorrent gives each listen socket the ports mapped for it
// (`listen_socket_t::tcp_external_port`, session_impl.hpp:208-215), and
// tells trackers, DHT nodes and handshake peers the port of the socket
// of their family (session_impl.cpp:1282-1290, 5859-5873;
// kademlia/node.cpp:449-453). An IPv4 mapping moves only the IPv4
// port, and PCP over IPv6 only the IPv6 one.
#[test]
fn each_family_is_told_the_port_mapped_for_it() {
    use super::AnnouncePort;
    use crate::upnp::{
        PortMapped,
        PortmapProtocol::Tcp,
        PortmapTransport::{NatPmp, PcpIpv6, Upnp},
    };
    let event = |transport, external_port| PortMapped {
        transport,
        protocol: Tcp,
        local_port: 6881,
        external_port,
        external_ip: None,
        error: None,
    };
    let port = AnnouncePort::new(Some(6881), None);
    let external = |port: &AnnouncePort| (port.external(false), port.external(true));

    // Only PCP mapped: IPv4 is still told the listen port.
    port.on_port_mapping(&event(PcpIpv6, Some(50002)));
    let told = ToldPorts {
        v4: Some(6881),
        v6: Some(50002),
    };
    assert_eq!(port.get(), told);
    assert_eq!(external(&port), (Some(6881), Some(50002)));

    // An IPv4 mapping leaves IPv6 on the PCP port.
    port.on_port_mapping(&event(Upnp, Some(40001)));
    port.on_port_mapping(&event(NatPmp, Some(50000)));
    let told = ToldPorts {
        v4: Some(50000),
        v6: Some(50002),
    };
    assert_eq!(port.get(), told);
    assert_eq!(external(&port), (Some(50000), Some(50002)));
    assert_eq!(told.to("192.0.2.1".parse().unwrap()), Some(50000));
    assert_eq!(told.to("::ffff:192.0.2.1".parse().unwrap()), Some(50000));
    assert_eq!(told.to("2001:db8::1".parse().unwrap()), Some(50002));

    // Only IPv4 mapped: IPv6 is told the listen port.
    port.on_port_mapping(&event(PcpIpv6, None));
    assert_eq!(external(&port), (Some(50000), Some(6881)));
    assert_eq!(port.get().v6, Some(6881));
}

// libtorrent `session_impl::queue_tracker_request` tells trackers the
// announce port set in the options whatever was mapped; the mapped
// port is still where peers reach the session.
#[test]
fn a_set_announce_port_is_told_whatever_a_mapper_gets() {
    use super::AnnouncePort;
    use crate::upnp::{PortMapped, PortmapProtocol::Tcp, PortmapTransport::NatPmp};
    let port = AnnouncePort::new(Some(6881), Some(40000));
    let set = ToldPorts::both(Some(40000));
    assert_eq!((port.get(), port.external(false)), (set, Some(6881)));
    let told = port.subscribe();
    port.on_port_mapping(&PortMapped {
        transport: NatPmp,
        protocol: Tcp,
        local_port: 6881,
        external_port: Some(50000),
        external_ip: None,
        error: None,
    });
    assert!(!told.has_changed().unwrap());
    assert_eq!((port.get(), port.external(false)), (set, Some(50000)));
}

// A listening session whose port a mapper got on the gateway announces
// the mapped port, not its listener's.
#[tokio::test(flavor = "multi_thread")]
async fn a_mapped_session_announces_the_mapped_port() -> anyhow::Result<()> {
    use crate::{
        ListenerMode, ListenerOptions,
        tests::e2e_tracker_announces::{started_port_after, tracker_session_opts},
        upnp::{PortMapped, PortmapProtocol, PortmapTransport},
    };
    let (port, listening) = started_port_after(
        SessionOptions {
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::UNSPECIFIED, 0).into(),
                ..Default::default()
            }),
            ..tracker_session_opts()?
        },
        |session| {
            let local_port = session.listen_addr().unwrap().port();
            session.announce_port.on_port_mapping(&PortMapped {
                transport: PortmapTransport::NatPmp,
                protocol: PortmapProtocol::Tcp,
                local_port,
                external_port: Some(50000),
                external_ip: None,
                error: None,
            });
        },
    )
    .await?;
    assert_ne!(listening, Some(50000));
    assert_eq!(port, "50000");
    Ok(())
}

// libtorrent announces to a tracker from the listen socket of the
// tracker's family, with that socket's external port
// (session_impl.cpp:1282-1290): a port PCP got over IPv6 goes to an
// IPv6 tracker, and an IPv4 tracker hears the IPv4 port, here the
// listen port, since nothing mapped one over IPv4.
#[tokio::test(flavor = "multi_thread")]
async fn a_pcp_port_is_announced_over_ipv6_only() -> anyhow::Result<()> {
    use crate::{
        ListenerMode, ListenerOptions,
        tests::e2e_tracker_announces::{started_port_at, tracker_session_opts},
        upnp::{PortMapped, PortmapProtocol, PortmapTransport},
    };
    use std::net::{Ipv4Addr, Ipv6Addr};
    for (tracker, mapped) in [
        (std::net::IpAddr::from(Ipv4Addr::LOCALHOST), false),
        (Ipv6Addr::LOCALHOST.into(), true),
    ] {
        let (port, listening) = started_port_at(
            SessionOptions {
                listen: Some(ListenerOptions {
                    mode: ListenerMode::TcpOnly,
                    listen_addr: (Ipv6Addr::UNSPECIFIED, 0).into(),
                    ..Default::default()
                }),
                ..tracker_session_opts()?
            },
            tracker,
            |session| {
                let local_port = session.listen_addr().unwrap().port();
                session.announce_port.on_port_mapping(&PortMapped {
                    transport: PortmapTransport::PcpIpv6,
                    protocol: PortmapProtocol::Tcp,
                    local_port,
                    external_port: Some(2002),
                    external_ip: None,
                    error: None,
                });
            },
        )
        .await?;
        let listening = listening.unwrap().to_string();
        let wanted = if mapped { "2002" } else { listening.as_str() };
        assert_eq!(port, wanted, "a tracker at {tracker}");
    }
    Ok(())
}

/// A NAT-PMP / PCP mapper for `requests` asking over `route` through
/// `session`'s clients.
fn natpmp_over(
    session: &Session,
    route: Result<crate::natpmp::GatewayRoute, String>,
    requests: Vec<crate::upnp::PortMapRequest>,
) -> crate::natpmp::NatPmpMapper {
    crate::natpmp::NatPmpMapper::new(route, requests, None, session.http_clients.clone())
}

/// `mapper`, its reports announced.
fn announced(mapper: crate::natpmp::NatPmpMapper) -> super::ports::Planned {
    super::ports::Planned {
        mapper: Box::new(mapper),
        announced: true,
    }
}

// A session on `[::]` maps its port over PCP from each global IPv6
// address, each from its own gateway (libtorrent starts a mapper on each
// expanded IPv6 listen socket, session_impl.cpp:5912-5919). It tells
// one IPv6 port: the one the first address got. Every report is passed
// on, the other address's too.
#[tokio::test(flavor = "multi_thread")]
async fn each_ipv6_address_is_mapped_and_the_first_one_announced() -> anyhow::Result<()> {
    use crate::natpmp::GatewayRoute;
    use crate::upnp::{PortMapRequest, PortmapProtocol};
    use anyhow::Context;
    use std::net::Ipv6Addr;
    let dir = TempDir::new()?;
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (Ipv6Addr::UNSPECIFIED, 0).into(),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await?;
    let local_port = session.listen_addr().context("no listener")?.port();
    let gateways = [
        crate::test_support::udp((Ipv6Addr::LOCALHOST, 0)).await,
        crate::test_support::udp((Ipv6Addr::LOCALHOST, 0)).await,
    ];
    let routes = gateways
        .iter()
        .map(|gateway| {
            Ok(GatewayRoute {
                local_ip: Ipv6Addr::LOCALHOST.into(),
                gateway: gateway.bind_addr(),
                interface_addresses: vec![Ipv6Addr::LOCALHOST.into()],
            })
        })
        .collect();
    let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
    let requests = vec![PortMapRequest {
        protocol: PortmapProtocol::Tcp,
        local_port,
        external_port: local_port,
    }];
    let routes: Vec<Result<GatewayRoute, String>> = routes;
    session.run_port_mappers(
        routes
            .into_iter()
            .enumerate()
            .map(|(at, route)| super::ports::Planned {
                mapper: Box::new(natpmp_over(&session, route, requests.clone()).ipv6()),
                announced: at == 0,
            }),
        Some(forward),
    );
    for (gateway, granted) in gateways.iter().zip([2004u16, 2006]) {
        let mut buf = [0u8; 1100];
        let (n, mapper) = gateway.recv_from(&mut buf).await?;
        let request = &buf[..n];
        let mut answer = vec![2, 0x80 | 1, 0, 0];
        answer.extend_from_slice(&7200u32.to_be_bytes());
        answer.extend_from_slice(&1u32.to_be_bytes());
        answer.extend_from_slice(&[0; 12]);
        answer.extend_from_slice(&request[24..42]);
        answer.extend_from_slice(&granted.to_be_bytes());
        answer.extend_from_slice(&Ipv6Addr::UNSPECIFIED.octets());
        gateway.send_to(&answer, mapper).await?;
    }
    let mut granted = Vec::new();
    while granted.len() < 2 {
        let report = forwarded.recv().await.context("the reports stopped")?;
        if let Some(port) = report.external_port {
            granted.push(port);
        }
    }
    granted.sort_unstable();
    assert_eq!(granted, [2004, 2006]);
    assert_eq!(
        session.announce_ports(),
        ToldPorts {
            v4: Some(local_port),
            v6: Some(2004),
        }
    );
    session.stop().await;
    Ok(())
}

// A session held to an address only the local network reaches starts
// no mapper: every holder of its report channel lets go of it with no
// report sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_session_on_a_local_only_address_maps_nothing() {
    let dir = TempDir::new().unwrap();
    let (events, mut reports) = tokio::sync::mpsc::unbounded_channel();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            port_map_events: Some(events),
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                enable_upnp_port_forwarding: true,
                ssl_listen_port: Some(0),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    assert_eq!(reports.recv().await, None);
    assert_eq!(session.announce_port_of(false), Some(6881));
    session.stop().await;
}

// An SSL torrent announces the port a mapper got for the SSL listener.
// Every other torrent announces the announce port set in the options,
// and peers reach it at the port mapped for the plain listener: each
// mapping moves only its own listener's port.
#[tokio::test(flavor = "multi_thread")]
async fn an_ssl_torrent_announces_the_port_mapped_for_the_ssl_listener() {
    use crate::{natpmp::GatewayRoute, upnp::PortMapRequest, upnp::PortmapProtocol};
    use weaver_tunnel::test_support::natpmp::NatPmpGateway;

    let dir = TempDir::new().unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                announce_port: Some(6881),
                ssl_listen_port: Some(0),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let plain = session.listen_addr().unwrap().port();
    let ssl = session.ssl_listen_addr().unwrap().port();
    assert_eq!(session.ssl_mapped_port(), Some(ssl));
    assert_eq!(session.announce_port_of(false), Some(6881));
    assert_eq!(session.announce_port_of(true), Some(ssl));

    let gateway =
        NatPmpGateway::granting(move |internal| if internal == ssl { 50001 } else { 50000 }).await;
    let requests = [plain, ssl]
        .into_iter()
        .map(|port| PortMapRequest {
            protocol: PortmapProtocol::Tcp,
            local_port: port,
            external_port: port,
        })
        .collect();
    let mut ssl_announced = session.ssl_port.subscribe();
    let (forward, mut forwarded) = tokio::sync::mpsc::unbounded_channel();
    let route = Ok(GatewayRoute {
        local_ip: std::net::Ipv4Addr::LOCALHOST.into(),
        gateway: gateway.address(),
        interface_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
    });
    session.run_port_mappers(
        [announced(natpmp_over(&session, route, requests))],
        Some(forward),
    );

    let mut granted = Vec::new();
    while granted.len() < 2 {
        let event = forwarded.recv().await.unwrap();
        granted.push((event.local_port, event.external_port));
    }
    granted.sort_unstable();
    let mut wanted = vec![(plain, Some(50000)), (ssl, Some(50001))];
    wanted.sort_unstable();
    assert_eq!(granted, wanted);
    ssl_announced
        .wait_for(|port| port.v4 == Some(50001))
        .await
        .unwrap();
    assert_eq!(session.announce_port_of(true), Some(50001));
    assert_eq!(session.announce_port_of(false), Some(6881));
    assert_eq!(session.external_port_of(false, false), Some(50000));
    session.stop().await;
}

// Two legs behind one gateway with one announce port set: each asks the
// gateway for its own listen port (libtorrent `session_impl::map_port`,
// session_impl.cpp:2375-2390) and gets its own mapping, and both still
// tell trackers and the DHT the set port (session_impl.cpp:1279-1281).
#[tokio::test(flavor = "multi_thread")]
async fn legs_behind_one_gateway_map_their_own_ports_and_announce_the_set_one() {
    use crate::{natpmp::GatewayRoute, upnp::PortMapRequest, upnp::PortmapProtocol};
    use weaver_tunnel::test_support::natpmp::NatPmpGateway;

    let dirs = [TempDir::new().unwrap(), TempDir::new().unwrap()];
    let mut sessions = Vec::new();
    for dir in &dirs {
        let session = Session::new_with_opts(
            dir.path().into(),
            SessionOptions {
                dht: None,
                persistence: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                listen: Some(crate::ListenerOptions {
                    mode: crate::ListenerMode::TcpOnly,
                    listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    announce_port: Some(40000),
                    ..Default::default()
                }),
                ..crate::test_support::session_options()
            },
        )
        .await
        .unwrap();
        sessions.push(session);
    }
    let ports: Vec<u16> = sessions
        .iter()
        .map(|s| s.listen_addr().unwrap().port())
        .collect();
    assert_ne!(ports[0], ports[1]);
    let first = ports[0];
    let gateway =
        NatPmpGateway::granting(move |internal| if internal == first { 50000 } else { 50001 })
            .await;

    let mut forwarded = Vec::new();
    for (session, &port) in sessions.iter().zip(&ports) {
        let (forward, reports) = tokio::sync::mpsc::unbounded_channel();
        let route = Ok(GatewayRoute {
            local_ip: std::net::Ipv4Addr::LOCALHOST.into(),
            gateway: gateway.address(),
            interface_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
        });
        let requests = vec![PortMapRequest {
            protocol: PortmapProtocol::Tcp,
            local_port: port,
            external_port: port,
        }];
        session.run_port_mappers(
            [announced(natpmp_over(session, route, requests))],
            Some(forward),
        );
        forwarded.push(reports);
    }

    for ((session, reports), (port, granted)) in sessions
        .iter()
        .zip(&mut forwarded)
        .zip(ports.iter().zip([50000, 50001]))
    {
        let report = reports.recv().await.unwrap();
        assert_eq!(
            (report.local_port, report.external_port),
            (*port, Some(granted))
        );
        assert_eq!(gateway.granted(2, *port).await.external, granted);
        assert_eq!(session.announce_port(), Some(40000));
        assert_eq!(session.external_port_of(false, false), Some(granted));
    }
    for session in sessions {
        session.stop().await;
    }
}

// A session mapping from an IPv6 address asks its gateway over PCP for
// its listen port and announces the port the gateway grants.
#[tokio::test(flavor = "multi_thread")]
async fn an_ipv6_mapping_over_pcp_moves_the_announced_port() {
    use crate::{natpmp::GatewayRoute, upnp::PortMapRequest, upnp::PortmapProtocol};
    use std::net::Ipv6Addr;

    let dir = TempDir::new().unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (Ipv6Addr::UNSPECIFIED, 0).into(),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let port = session.listen_addr().unwrap().port();
    assert_eq!(session.announce_port(), Some(port));

    // A PCP gateway on the IPv6 loopback that grants 50002 for the
    // first MAP it hears, and hands the request back.
    let gateway = crate::test_support::udp((Ipv6Addr::LOCALHOST, 0)).await;
    let gateway_addr = gateway.bind_addr();
    let heard = tokio::spawn(async move {
        let mut buf = [0u8; 1100];
        let (n, from) = gateway.recv_from(&mut buf).await.unwrap();
        let request = buf[..n].to_vec();
        let mut answer = vec![2, 0x80 | 1, 0, 0];
        answer.extend_from_slice(&7200u32.to_be_bytes());
        answer.extend_from_slice(&1000u32.to_be_bytes());
        answer.extend_from_slice(&[0; 12]);
        answer.extend_from_slice(&request[24..42]);
        answer.extend_from_slice(&50002u16.to_be_bytes());
        answer.extend_from_slice(&"2001:db8::5".parse::<Ipv6Addr>().unwrap().octets());
        gateway.send_to(&answer, from).await.unwrap();
        request
    });

    let mut announced = session.announce_port_changes();
    let route = Ok(GatewayRoute {
        local_ip: Ipv6Addr::LOCALHOST.into(),
        gateway: gateway_addr,
        interface_addresses: vec![Ipv6Addr::LOCALHOST.into()],
    });
    let requests = vec![PortMapRequest {
        protocol: PortmapProtocol::Tcp,
        local_port: port,
        external_port: port,
    }];
    session.run_port_mappers(
        [super::ports::Planned {
            mapper: Box::new(natpmp_over(&session, route, requests)),
            announced: true,
        }],
        None,
    );

    let request = heard.await.unwrap();
    assert_eq!(request[0], 2, "PCP");
    assert_eq!(&request[8..24], &Ipv6Addr::LOCALHOST.octets());
    assert_eq!(
        &request[40..44],
        &[port.to_be_bytes(), port.to_be_bytes()].concat()[..]
    );
    // The PCP port moves what IPv6 is told; IPv4 keeps the listen port.
    announced
        .wait_for(|announced| announced.v6 == Some(50002))
        .await
        .unwrap();
    // The report that moved the port was counted once, as a mapping made.
    use crate::session_stats::network::PortMapResult;
    let reports = session.stats.network.snapshot().port_maps;
    let mapped = |by: &[u64; PortMapResult::COUNT]| by[PortMapResult::Mapped as usize];
    assert_eq!(reports.iter().map(mapped).sum::<u64>(), 1);
    assert_eq!(reports.as_flattened().iter().sum::<u64>(), 1);
    assert_eq!(session.announce_ports().v4, Some(port));
    assert_eq!(session.external_port_of(false, true), Some(50002));
    assert_eq!(session.external_port_of(false, false), Some(port));
    session.stop().await;
}

#[tokio::test]
async fn the_dht_announces_a_port_that_changes_while_it_searches() {
    // libtorrent torrent::dht_announce reads the port at each announce.
    // A node run by hand on 127.0.0.1 answers ping, find_node and
    // get_peers, giving a token, and reports each announce_peer port.
    use crate::bencode::{BencodeValue, ByteBufOwned, dyn_from_bytes};
    use crate::core::net::DatagramSocket;
    use crate::dht::{DhtConfig, DhtState, Id20};
    use futures::StreamExt;
    use std::net::Ipv4Addr;

    const NODE_ID: [u8; 20] = [0x42; 20];

    fn get<'a>(v: &'a BencodeValue<ByteBufOwned>, key: &str) -> &'a BencodeValue<ByteBufOwned> {
        match v {
            BencodeValue::Dict(d) => &d[&ByteBufOwned::from(key.as_bytes())],
            _ => panic!("not a dict: {v:?}"),
        }
    }
    fn bytes(v: &BencodeValue<ByteBufOwned>) -> &[u8] {
        match v {
            BencodeValue::Bytes(b) => b.as_ref(),
            _ => panic!("not bytes: {v:?}"),
        }
    }

    // The port of the next announce_peer the node hears, answering
    // every query before it.
    async fn next_announced(node: &Arc<dyn DatagramSocket>) -> i64 {
        let mut buf = [0u8; 2048];
        loop {
            let (len, from) = node.recv_from(&mut buf).await.unwrap();
            let msg = dyn_from_bytes::<ByteBufOwned>(&buf[..len]).unwrap();
            if bytes(get(&msg, "y")) != b"q" {
                continue;
            }
            let tid = bytes(get(&msg, "t"));
            let mut reply = b"d1:rd2:id20:".to_vec();
            reply.extend_from_slice(&NODE_ID);
            reply.extend_from_slice(b"5:token2:tke1:t");
            reply.extend_from_slice(format!("{}:", tid.len()).as_bytes());
            reply.extend_from_slice(tid);
            reply.extend_from_slice(b"1:y1:re");
            node.send_to(&reply, from).await.unwrap();
            if bytes(get(&msg, "q")) == b"announce_peer" {
                match get(get(&msg, "a"), "port") {
                    BencodeValue::Integer(port) => return *port,
                    other => panic!("port is {other:?}"),
                }
            }
        }
    }

    let node = crate::test_support::loopback_udp().await;
    let node_addr = node.bind_addr();
    let dht = DhtState::with_config(DhtConfig {
        listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
        sockets: Some(crate::test_support::socket_factory()),
        bootstrap_addrs: Some(vec![node_addr.to_string()]),
        ..Default::default()
    })
    .await
    .unwrap();
    dht.add_node(node_addr);
    let (port, listen_port) = tokio::sync::watch::channel(ToldPorts::both(Some(6881)));
    // The node is the closest there is to the hash: its own id.
    let (announce, announce_rx) = tokio::sync::watch::channel(());
    let mut peers = super::dht_peers(dht, Id20::new(NODE_ID), listen_port, announce_rx);
    let first = tokio::select! {
        _ = peers.next() => panic!("the node gave no peers"),
        port = next_announced(&node) => port,
    };
    assert_eq!(first, 6881);
    port.send_replace(ToldPorts::both(Some(40001)));
    let second = tokio::select! {
        _ = peers.next() => panic!("the node gave no peers"),
        port = next_announced(&node) => port,
    };
    assert_eq!(second, 40001);

    // A forced DHT announce (libtorrent torrent_handle::force_dht_announce)
    // starts a new search, which announces again.
    announce.send_replace(());
    let third = tokio::select! {
        _ = peers.next() => panic!("the node gave no peers"),
        port = next_announced(&node) => port,
    };
    assert_eq!(third, 40001);
}

#[tokio::test]
async fn the_session_maps_tcp_and_udp_only_for_the_sockets_it_listens_on() {
    use super::ports::port_map_requests;
    use crate::{
        listen::{ListenerMode, ListenerOptions},
        upnp::PortmapProtocol::{Tcp, Udp},
    };
    for (mode, expected) in [
        (ListenerMode::TcpOnly, vec![Tcp]),
        (ListenerMode::UtpOnly, vec![Udp]),
        (ListenerMode::TcpAndUtp, vec![Tcp, Udp]),
    ] {
        for ssl in [false, true] {
            let token = tokio_util::sync::CancellationToken::new();
            let slots = crate::connection_slots::ConnectionSlots::new(
                super::DEFAULT_CONNECTIONS_LIMIT,
                crate::connection_slots::DEFAULT_CONNECTIONS_SLACK,
                crate::connection_slots::DEFAULT_CONNECTION_SPEED,
            );
            let listen = ListenerOptions {
                mode,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ssl_listen_port: ssl.then_some(0),
                ..Default::default()
            }
            .start(
                None,
                token.clone(),
                &crate::test_support::socket_factory(),
                &slots,
            )
            .await
            .unwrap();
            // Each listener asks for its own port, whatever announce
            // port is set.
            let plain = listen.addr.port();
            let mut wanted: Vec<_> = expected.iter().map(|&p| (p, plain, plain)).collect();
            if let Some(ssl) = &listen.ssl {
                let port = ssl.addr.port();
                assert_ne!(port, plain);
                wanted.extend(expected.iter().map(|&p| (p, port, port)));
            }
            let requests: Vec<_> = port_map_requests(&listen)
                .into_iter()
                .map(|r| (r.protocol, r.local_port, r.external_port))
                .collect();
            assert_eq!(requests, wanted, "{mode:?} ssl {ssl}");
            token.cancel();
        }
    }
}

/// Kernel sockets that note each receive buffer a uTP socket asks for,
/// the port of each TCP listener, each port uTP asks for, and each
/// address a datagram socket of its own is bound at. With
/// `first_utp_port_held`, another socket holds the first port uTP asks
/// for.
struct NotingAsks {
    kernel: Arc<dyn crate::network::SocketFactory>,
    first_utp_port_held: bool,
    asked: std::sync::Mutex<Vec<usize>>,
    tcp_ports: std::sync::Mutex<Vec<u16>>,
    utp_ports: std::sync::Mutex<Vec<u16>>,
    udp_binds: std::sync::Mutex<Vec<std::net::SocketAddr>>,
}

impl NotingAsks {
    fn new(first_utp_port_held: bool) -> Arc<Self> {
        Arc::new(Self {
            kernel: crate::test_support::socket_factory(),
            first_utp_port_held,
            asked: Default::default(),
            tcp_ports: Default::default(),
            utp_ports: Default::default(),
            udp_binds: Default::default(),
        })
    }
}

impl crate::network::SocketFactory for NotingAsks {
    fn connect_tcp<'a>(
        &'a self,
        addr: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<
        'a,
        std::io::Result<(
            crate::network::BoxAsyncReadVectored,
            crate::network::BoxAsyncWrite,
        )>,
    > {
        self.kernel.connect_tcp(addr)
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Box<dyn crate::network::StreamListener>>>
    {
        Box::pin(async move {
            let listener = self.kernel.listen_tcp(addr).await?;
            self.tcp_ports
                .lock()
                .unwrap()
                .push(listener.local_addr().port());
            Ok(listener)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: std::net::SocketAddr,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        self.udp_binds.lock().unwrap().push(addr);
        self.kernel.bind_udp(addr)
    }

    fn bind_utp<'a>(
        &'a self,
        addr: std::net::SocketAddr,
        receive_buffer: usize,
    ) -> crate::network::BoxFuture<'a, std::io::Result<Arc<dyn crate::network::DatagramSocket>>>
    {
        self.asked.lock().unwrap().push(receive_buffer);
        let first = {
            let mut ports = self.utp_ports.lock().unwrap();
            ports.push(addr.port());
            ports.len() == 1
        };
        if first && self.first_utp_port_held {
            return Box::pin(async { Err(std::io::Error::from(std::io::ErrorKind::AddrInUse)) });
        }
        self.kernel.bind_utp(addr, receive_buffer)
    }
}

#[tokio::test]
async fn a_sessions_utp_socket_asks_for_a_window_per_connection() {
    let dir = TempDir::new().unwrap();
    let sockets = NotingAsks::new(false);
    let _session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpAndUtp,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            connections_limit: 7,
            ..SessionOptions::new(sockets.clone())
        },
    )
    .await
    .unwrap();
    // A listener port whose UDP port another socket holds is given up,
    // so uTP may ask more than once: once for each port TCP took.
    let asked = sockets.asked.lock().unwrap().clone();
    assert_eq!(asked.len(), sockets.tcp_ports.lock().unwrap().len());
    assert!(
        asked
            .iter()
            .all(|&ask| ask == 7 * crate::udp_options::STREAM_WINDOW),
        "{asked:?}"
    );
}

// The DHT on the port uTP listens on runs over uTP's one socket: the
// session binds one datagram socket there, the DHT answers a ping sent
// to it, and a uTP peer connects to it.
#[tokio::test]
async fn the_dht_shares_the_utp_listeners_socket() {
    let dir = TempDir::new().unwrap();
    let sockets = NotingAsks::new(false);
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(crate::DhtSessionConfig {
                bootstrap_addrs: Some(Vec::new()),
                port: Some(0),
                persistence: None,
            }),
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ipv4_only: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpAndUtp,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..SessionOptions::new(sockets.clone())
        },
    )
    .await
    .unwrap();
    let listening = session.listen_addr().unwrap();
    assert_eq!(session.get_dht().unwrap().listen_addr(), listening);
    // One datagram socket is bound beside uTP's, the UDP tracker
    // client's: the DHT binds none of its own.
    assert_eq!(
        *sockets.udp_binds.lock().unwrap(),
        [std::net::SocketAddr::from((
            std::net::Ipv4Addr::UNSPECIFIED,
            0
        ))]
    );

    let client = crate::test_support::loopback_udp().await;
    client
        .send_to(
            b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
            listening,
        )
        .await
        .unwrap();
    let mut buf = [0u8; 1024];
    let len = loop {
        let (len, from) = client.recv_from(&mut buf).await.unwrap();
        if from == listening {
            break len;
        }
    };
    let reply =
        crate::dht::bprotocol::deserialize_message::<crate::bencode::ByteBufOwned>(&buf[..len])
            .unwrap();
    assert!(matches!(
        reply.kind,
        crate::dht::bprotocol::MessageKind::Response(_)
    ));
    assert_eq!(reply.transaction_id.as_ref(), b"aa");

    let peer = crate::listen::bind_utp(
        crate::test_support::socket_factory().as_ref(),
        (std::net::Ipv4Addr::LOCALHOST, 0).into(),
        Default::default(),
        crate::udp_options::STREAM_WINDOW,
        None,
    )
    .await
    .unwrap();
    peer.connect(listening).await.unwrap();
    session.stop().await;
}

// A port the TCP listener chose that a datagram socket holds is given up
// for another, so uTP and the DHT still listen on the listener's port
// over one socket, and the DHT binds none of its own.
#[tokio::test]
async fn a_listener_port_whose_udp_port_is_held_is_given_up_for_another() {
    let dir = TempDir::new().unwrap();
    let sockets = NotingAsks::new(true);
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: Some(crate::DhtSessionConfig {
                bootstrap_addrs: Some(Vec::new()),
                port: Some(0),
                persistence: None,
            }),
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ipv4_only: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpAndUtp,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            ..SessionOptions::new(sockets.clone())
        },
    )
    .await
    .unwrap();
    // Another socket on the host may hold a later port too, and that one
    // is given up as well.
    let tcp_ports = sockets.tcp_ports.lock().unwrap().clone();
    assert!(tcp_ports.len() >= 2, "the listener took {tcp_ports:?}");
    assert_eq!(
        tcp_ports
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        tcp_ports.len(),
        "the listener took {tcp_ports:?}"
    );
    assert_eq!(*sockets.utp_ports.lock().unwrap(), tcp_ports);
    let listening = session.listen_addr().unwrap();
    assert_eq!(Some(&listening.port()), tcp_ports.last());
    assert_eq!(session.get_dht().unwrap().listen_addr(), listening);
    // The UDP tracker client's socket alone: the DHT binds none.
    assert_eq!(
        *sockets.udp_binds.lock().unwrap(),
        [std::net::SocketAddr::from((
            std::net::Ipv4Addr::UNSPECIFIED,
            0
        ))]
    );
    session.stop().await;
}

#[tokio::test]
async fn a_session_reports_utp_stats_from_plain_and_ssl_listeners() {
    use std::sync::atomic::Ordering;

    let dir = TempDir::new().unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::UtpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ssl_listen_port: Some(0),
                ..Default::default()
            }),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    assert_eq!(session.utp_sockets.len(), 2);
    for (index, socket) in session.utp_sockets.iter().enumerate() {
        let value = index as u64 + 1;
        let stats = socket.0.stats();
        stats.packets_lost.fetch_add(value, Ordering::Relaxed);
        stats
            .max_congestion_window_bytes
            .store(1500 * value, Ordering::Relaxed);
    }
    let stats = session.stats_snapshot();
    assert_eq!(stats.utp.packets_lost, 3);
    assert_eq!(stats.utp.max_congestion_window_bytes, 3000);
    let serialized = serde_json::to_value(stats).unwrap();
    assert_eq!(serialized["utp"]["packets_lost"], 3);
    assert_eq!(serialized["utp"]["max_congestion_window_bytes"], 3000);
    session.stop().await;
}

#[tokio::test]
async fn shared_limit_changes_reach_every_plain_and_ssl_utp_listener() {
    let shared = super::SharedLimits::new(
        super::SessionLimits {
            connections_limit: 3,
            unchoke_slots_limit: usize::MAX,
            connection_speed: std::num::NonZeroU32::new(30).unwrap(),
        },
        0,
        1,
    );
    let dir = TempDir::new().unwrap();
    let open = |name: &str| {
        let shared = shared.clone();
        let path = dir.path().join(name);
        async move {
            Session::new_with_opts(
                path,
                SessionOptions {
                    dht: None,
                    persistence: None,
                    disable_trackers: true,
                    disable_local_service_discovery: true,
                    shared_limits: Some(shared),
                    listen: Some(crate::ListenerOptions {
                        mode: crate::ListenerMode::UtpOnly,
                        listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                        ssl_listen_port: Some(0),
                        utp_opts: Some(weaver_utp::SocketOpts {
                            connections_limit: std::num::NonZeroUsize::new(99),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..crate::test_support::session_options()
                },
            )
            .await
            .unwrap()
        }
    };
    let first = open("first").await;
    assert_eq!(first.utp_sockets.len(), 2);
    assert!(
        first
            .utp_sockets
            .iter()
            .all(|socket| socket.0.connections_limit() == Some(3))
    );

    let mut limits = first.limits();
    limits.connections_limit = 1;
    first.set_limits(limits);
    let second = open("second").await;
    assert_eq!(second.utp_sockets.len(), 2);
    for session in [&first, &second] {
        assert!(
            session
                .utp_sockets
                .iter()
                .all(|socket| socket.0.connections_limit() == Some(1))
        );
    }
    limits.connections_limit = 4;
    second.set_limits(limits);
    for session in [&first, &second] {
        assert!(
            session
                .utp_sockets
                .iter()
                .all(|socket| socket.0.connections_limit() == Some(4))
        );
    }
    limits.connections_limit = 0;
    first.set_limits(limits);
    for session in [&first, &second] {
        assert!(
            session
                .utp_sockets
                .iter()
                .all(|socket| socket.0.connections_limit() == Some(0))
        );
    }
    first.stop().await;
    second.stop().await;
}

/// A one-piece torrent whose announce-list is `[[a1, a2], [b1]]`.
fn torrent_bytes(name: &str, private: bool) -> Bytes {
    let a1 = "http://a1.test/announce";
    let a2 = "udp://a2.test:6969/announce";
    let b1 = "http://b1.test/announce";
    let private = if private { "7:privatei1e" } else { "" };
    format!(
        "d8:announce{}13:announce-listll{}{}el{}ee4:infod6:lengthi1e4:name{}12:piece lengthi16384e6:pieces{}{private}ee",
        bstr(a1),
        bstr(a1),
        bstr(a2),
        bstr(b1),
        bstr(name),
        bstr(&"x".repeat(20)),
    )
    .into_bytes()
    .into()
}

fn own_tiers() -> Vec<Vec<Tracker>> {
    tiers(&[
        &["http://a1.test/announce", "udp://a2.test:6969/announce"],
        &["http://b1.test/announce"],
    ])
}

#[test]
fn test_is_bare_info_hash() {
    assert!(is_bare_info_hash(
        "08ada5a7a6183aae1e09d831df6748d566095a10"
    ));
    assert!(is_bare_info_hash(
        "08ADA5A7A6183AAE1E09D831DF6748D566095A10"
    ));
    // 40 characters, but a URL.
    assert!(!is_bare_info_hash(
        "https://nyaa.si/download/2164312.torrent"
    ));
    assert!(!is_bare_info_hash(
        "08ada5a7a6183aae1e09d831df6748d566095a1"
    ));
}

#[test]
fn test_torrent_file_from_info_and_bytes() {
    let orig_full_torrent =
        include_bytes!("../../resources/ubuntu-21.04-desktop-amd64.iso.torrent");
    let parsed = torrent_from_bytes(Bytes::from_static(orig_full_torrent)).unwrap();
    let parsed_tiers = metainfo_tracker_tiers(&parsed.meta);

    let generated_torrent =
        torrent_file_from_info_bytes(parsed.meta.info.raw_bytes.as_ref(), &parsed_tiers).unwrap();
    let generated_parsed = torrent_from_bytes(generated_torrent).unwrap();
    assert_eq!(parsed.meta.info_hash, generated_parsed.meta.info_hash);
    assert_eq!(parsed.meta.info, generated_parsed.meta.info);
    assert_eq!(parsed_tiers, metainfo_tracker_tiers(&generated_parsed.meta));
}

// Reading a torrent logs its shape at trace level, never a value: the
// passkey in its trackers' and seed's URLs stays out of the log.
#[test]
fn a_read_torrent_logs_no_passkey() {
    let announce = "https://t.test/0123passkey/announce?pk=s3cret";
    let seed = "https://s.test/files/?token=s3cret";
    let torrent = format!(
        "d8:announce{}13:announce-listll{}ee4:infod6:lengthi1e4:name{}12:piece lengthi16384e6:pieces{}e8:url-list{}e",
        bstr(announce),
        bstr(announce),
        bstr("x"),
        bstr(&"x".repeat(20)),
        bstr(seed),
    );
    let lines = crate::tests::test_util::LogLines::default();
    let parsed = {
        let _capture = lines.capture();
        torrent_from_bytes(Bytes::from(torrent.into_bytes())).unwrap()
    };
    let text = lines.text();
    assert!(text.contains("torrent read"), "{text}");
    assert!(
        text.contains(r#"keys=["announce", "announce-list", "info", "url-list"]"#),
        "{text}"
    );
    assert!(
        text.contains(&format!("{:?}", parsed.meta.info_hash)),
        "{text}"
    );
    assert!(!text.contains("s3cret"), "{text}");
    assert!(!text.contains("0123passkey"), "{text}");
}

// Serves `head` and then `body` to the first request at a loopback URL.
async fn serve_once(head: String, body: Bytes) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|w| w == b"\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buf[..read]);
        }
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(&body).await.unwrap();
    });
    format!("http://{addr}/a.torrent")
}

#[tokio::test]
async fn a_torrent_url_past_the_byte_limit_is_refused() {
    let clients = crate::test_support::loopback();
    let limit = crate::core::torrent_metainfo::MAX_TORRENT_FILE_BYTES;
    // Said by its length.
    let url = serve_once(
        format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", limit + 1),
        Bytes::new(),
    )
    .await;
    let error = super::torrent_from_url(clients.as_ref(), &url)
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("larger than"), "{error:#}");
    // Found as it is read, with no length given.
    let url = serve_once(
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".into(),
        Bytes::from(vec![b'x'; limit + 1]),
    )
    .await;
    let error = super::torrent_from_url(clients.as_ref(), &url)
        .await
        .err()
        .unwrap();
    assert!(format!("{error:#}").contains("larger than"), "{error:#}");
    // A torrent within it loads.
    let torrent = torrent_bytes("x", false);
    let url = serve_once(
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n",
            torrent.len()
        ),
        torrent.clone(),
    )
    .await;
    let parsed = super::torrent_from_url(clients.as_ref(), &url)
        .await
        .unwrap();
    assert_eq!(parsed.torrent_bytes, torrent);
}

#[test]
fn test_metainfo_tracker_tiers() {
    let parsed = torrent_from_bytes(torrent_bytes("x", false)).unwrap();
    assert_eq!(metainfo_tracker_tiers(&parsed.meta), own_tiers());
}

#[test]
fn test_merge_tracker_tiers() {
    let extra = [
        url("http://extra.test/announce"),
        // Already one of the torrent's own trackers: keeps its own place.
        url("http://b1.test/announce"),
    ];

    // The extra trackers are the operator's.
    let mut merged = own_tiers();
    merged.push(operator("http://extra.test/announce"));
    assert_eq!(
        merge_tracker_tiers(own_tiers(), extra.clone(), false),
        merged
    );
    assert_eq!(merge_tracker_tiers(own_tiers(), extra, true), own_tiers());

    // A tracker repeated in a later tier keeps only its first place, and
    // a tier left empty is dropped.
    assert_eq!(
        merge_tracker_tiers(
            tiers(&[
                &["http://a.test/"],
                &["http://a.test/"],
                &["http://b.test/"]
            ]),
            std::iter::empty(),
            false
        ),
        tiers(&[&["http://a.test/"], &["http://b.test/"]])
    );
}

#[tokio::test]
async fn test_private_torrent_announces_only_to_its_own_tiers() {
    let dir = TempDir::new().unwrap();
    let session = Session::new_with_opts(
        dir.path().to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            trackers: HashSet::from([url("http://session.test/announce")]),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();

    let add = |name: &'static str, private: bool, trackers_only: bool| {
        let session = session.clone();
        let output_folder = dir.path().join(name).to_str().unwrap().to_owned();
        async move {
            session
                .add_torrent(
                    AddTorrent::from_bytes(torrent_bytes(name, private)),
                    Some(AddTorrentOptions {
                        paused: true,
                        output_folder: Some(output_folder),
                        trackers: Some(vec!["http://added.test/announce".to_owned()]),
                        trackers_only,
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap()
        }
    };

    let private = add("private", true, false).await;
    assert!(private.takes_peers_from_trackers_only());
    assert_eq!(private.shared().tracker_tiers(), own_tiers());
    assert_eq!(
        session.announce_tiers(private.shared().tracker_tiers(), true),
        own_tiers()
    );

    // A torrent its metainfo does not mark private, added to take peers
    // from its trackers alone, is held to the same.
    let trackers_only = add("trackers_only", false, true).await;
    assert!(trackers_only.takes_peers_from_trackers_only());
    assert_eq!(trackers_only.shared().tracker_tiers(), own_tiers());
    assert_eq!(
        session.announce_tiers(
            trackers_only.shared().tracker_tiers(),
            trackers_only.takes_peers_from_trackers_only()
        ),
        own_tiers()
    );

    let public = add("public", false, false).await;
    assert!(!public.takes_peers_from_trackers_only());
    let mut with_added = own_tiers();
    with_added.push(operator("http://added.test/announce"));
    assert_eq!(public.shared().tracker_tiers(), with_added);
    let mut with_session = with_added;
    with_session.push(operator("http://session.test/announce"));
    assert_eq!(
        session.announce_tiers(public.shared().tracker_tiers(), false),
        with_session
    );

    session.stop().await;
}

// libtorrent `torrent_flags::disable_dht` and `disable_lsd`: a torrent
// added without the DHT or the local network looks for peers on neither,
// while the session runs them for its other torrents. A private torrent
// looks on neither whatever it was added with
// (`torrent::should_announce_dht`, `torrent::lsd_announce`), and neither
// does one added to take peers from its trackers alone.
#[tokio::test]
async fn test_a_torrent_looks_for_peers_only_where_it_was_added_to() {
    use crate::lsd::RecordingSocket;

    let dir = TempDir::new().unwrap();
    let (sent_tx, _sent) = tokio::sync::mpsc::unbounded_channel();
    let session = Session::new_with_opts(
        dir.path().to_owned(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: false,
            lsd_socket: Some(Box::new(RecordingSocket(sent_tx))),
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    assert!(session.lsd.is_some());

    let add = |name: &'static str, private: bool, opts: AddTorrentOptions| {
        let session = session.clone();
        let output_folder = dir.path().join(name).to_str().unwrap().to_owned();
        async move {
            session
                .add_torrent(
                    AddTorrent::from_bytes(torrent_bytes(name, private)),
                    Some(AddTorrentOptions {
                        paused: true,
                        output_folder: Some(output_folder),
                        ..opts
                    }),
                )
                .await
                .unwrap()
                .into_handle()
                .unwrap()
        }
    };
    let everywhere = Discovery {
        dht: true,
        lsd: true,
    };
    let nowhere = Discovery {
        dht: false,
        lsd: false,
    };

    let public = add("public", false, AddTorrentOptions::default()).await;
    assert_eq!(public.discovery(), everywhere);
    let no_dht = add(
        "no_dht",
        false,
        AddTorrentOptions {
            disable_dht: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(
        no_dht.discovery(),
        Discovery {
            dht: false,
            lsd: true
        }
    );
    let neither = add(
        "neither",
        false,
        AddTorrentOptions {
            disable_dht: true,
            disable_lsd: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(neither.discovery(), nowhere);
    let private = add("private", true, AddTorrentOptions::default()).await;
    assert_eq!(private.discovery(), nowhere);
    let trackers_only = add(
        "trackers_only",
        false,
        AddTorrentOptions {
            trackers_only: true,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(trackers_only.discovery(), nowhere);

    // The session runs local discovery: a torrent that looks there is
    // handed its peers, one that looks nowhere is handed none.
    let peers = |t: &ManagedTorrentHandle| {
        let identity = session.identity().clone();
        peer_finder::after_trackers(
            &session,
            t.info_hashes(),
            &identity,
            tokio::sync::watch::channel(()).1,
            t.discovery(),
        )
    };
    assert!(peers(&public).is_some());
    assert!(peers(&no_dht).is_some());
    assert!(peers(&neither).is_none());
    assert!(peers(&private).is_none());

    session.stop().await;
}

#[test]
fn test_dht_client_version_is_weavers() {
    let (major, minor, _, _) = crate::crate_version!();
    assert_eq!(super::dht_client_version(), [b'W', b'V', major, minor]);
}

// A session listening on loopback that seeds a finished one-piece
// torrent whose own upload limit is `upload`.
pub(super) async fn seeding(
    connections_limit: usize,
    upload: crate::limits::LimitKind,
) -> (
    TempDir,
    std::sync::Arc<Session>,
    super::ManagedTorrentHandle,
) {
    seeding_with(connections_limit, super::DEFAULT_CONNECTIONS_SLACK, upload).await
}

// As [`seeding`], the session taking `connections_slack` incoming
// connections past its cap.
pub(super) async fn seeding_with(
    connections_limit: usize,
    connections_slack: usize,
    upload: crate::limits::LimitKind,
) -> (
    TempDir,
    std::sync::Arc<Session>,
    super::ManagedTorrentHandle,
) {
    let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
        1,
        16384,
        Some("connection_slots"),
    );
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            listen: Some(crate::ListenerOptions {
                mode: crate::ListenerMode::TcpOnly,
                listen_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                ..Default::default()
            }),
            connections_limit,
            connections_slack,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ratelimits: crate::limits::LimitsConfig {
                    upload_bps: upload,
                    ..Default::default()
                },
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_completed().await.unwrap();
    (dir, session, handle)
}

// What a tracker announce of `handle`'s torrent reports.
fn tracker_stats(
    session: &std::sync::Arc<Session>,
    handle: &super::ManagedTorrentHandle,
) -> (super::PeerRxTorrentInfo, crate::tracker::TrackerCommsStats) {
    use crate::tracker::TorrentStatsProvider;
    let info = super::PeerRxTorrentInfo {
        info_hashes: handle.info_hashes(),
        session: std::sync::Arc::downgrade(session),
        torrent: Default::default(),
    };
    let stats = info.get();
    (info, stats)
}

#[tokio::test]
async fn a_seed_announces_nothing_left() {
    use crate::tracker::TorrentStatsProvider;
    use futures::FutureExt;
    let (_dir, session, handle) = seeding(8, Default::default()).await;
    let (info, stats) = tracker_stats(&session, &handle);
    assert_eq!(stats.left_bytes, Some(0));
    // Seeding from the start: no `completed`, and nothing to wait for.
    assert!(!stats.completed_during_run);
    assert!(info.wait_until_completed().now_or_never().is_some());
    session.stop().await;
}

// A one-piece torrent of a file in a new directory, and a session
// reaching no network, the torrent not added to it.
async fn unadded(name: &str) -> (TempDir, Bytes, std::sync::Arc<Session>) {
    let dir =
        crate::tests::test_util::create_default_random_dir_with_torrents(1, 16384, Some(name));
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(16384),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    (dir, torrent, session)
}

// libtorrent waits for a magnet's metadata on `metadata_received_alert`:
// the announce's wait for completion finds the torrent as it enters the
// session, not on a later look.
#[tokio::test]
async fn a_magnets_completion_wait_finds_its_torrent_as_it_is_added() {
    use crate::tracker::TorrentStatsProvider;
    use futures::FutureExt;
    let (dir, torrent, session) = unadded("magnet_wait").await;
    let info_hashes = crate::core::torrent_metainfo::torrent_from_bytes(&torrent)
        .unwrap()
        .info_hashes;
    let info = super::PeerRxTorrentInfo {
        info_hashes,
        session: std::sync::Arc::downgrade(&session),
        torrent: Default::default(),
    };
    let mut wait = info.wait_until_completed();
    assert!((&mut wait).now_or_never().is_none());

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_completed().await.unwrap();
    assert!(wait.now_or_never().is_some());
    session.stop().await;
}

// A torrent's wait for completion follows its state: a torrent removed
// while it downloads ends the wait.
#[tokio::test]
async fn a_completion_wait_ends_when_its_torrent_is_deleted() {
    use futures::FutureExt;
    let (_dir, torrent, session) = unadded("deleted_wait").await;
    let empty = TempDir::with_prefix("deleted_wait_out").unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(empty.path().to_str().unwrap().to_owned()),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    assert!(handle.live().is_some());

    let mut wait = handle.wait_until_completed();
    assert!((&mut wait).now_or_never().is_none());
    session.delete(handle.id().into(), false).await.unwrap();
    assert!(wait.await.is_err());
    session.stop().await;
}

// The file levels, sequential and first-and-last given at add are the
// torrent's, and each change rewrites the pieces from the levels
// (libtorrent `torrent::prioritize_files`, qBittorrent
// `TorrentImpl::setFirstLastPiecePriority`).
#[tokio::test(start_paused = true)]
async fn file_levels_and_the_picking_toggles_are_set_at_add_and_changed_after() {
    use crate::{FilePriority::*, PiecePriority};
    const PIECE: usize = 16384;
    let dir =
        crate::tests::test_util::create_default_random_dir_with_torrents(2, PIECE, Some("levels"));
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(u32::try_from(PIECE).unwrap()),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let both = AddTorrentOptions {
        output_folder: Some(dir.path().to_str().unwrap().to_owned()),
        overwrite: true,
        only_files: Some(vec![0]),
        file_priorities: Some(vec![Normal, Normal]),
        ..Default::default()
    };
    assert!(
        session
            .add_torrent(AddTorrent::from_bytes(torrent.clone()), Some(both))
            .await
            .is_err()
    );

    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                file_priorities: Some(vec![Ignored, High]),
                sequential: true,
                first_last_piece_first: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let level = |file: usize| {
        let start = handle
            .with_metadata(|m| m.file_infos[file].piece_range.start)
            .unwrap();
        handle.piece_priority(start).unwrap()
    };
    assert_eq!(handle.file_priorities(), [Ignored, High]);
    assert_eq!(handle.only_files(), Some(vec![1]));
    assert!(handle.sequential());
    assert!(handle.first_last_piece_first());
    // A one-piece file is all head and tail.
    assert_eq!(level(0), PiecePriority::Skip);
    assert_eq!(level(1), PiecePriority::Critical);

    handle.set_first_last_piece_first(false).unwrap();
    assert_eq!(level(1), PiecePriority::High);
    handle
        .set_file_priorities(&[(0, Maximum), (1, Ignored)])
        .unwrap();
    assert_eq!(handle.file_priorities(), [Maximum, Ignored]);
    assert_eq!(handle.only_files(), Some(vec![0]));
    assert_eq!(level(0), PiecePriority::Critical);
    assert_eq!(level(1), PiecePriority::Skip);
    assert!(handle.set_file_priorities(&[(2, Normal)]).is_err());
    handle.set_sequential(false);
    assert!(!handle.sequential());
    session.stop().await;
}

#[tokio::test]
async fn a_finished_selection_short_of_a_seed_announces_what_the_torrent_lacks() {
    // libtorrent torrent::bytes_left counts the whole torrent, and
    // torrent::finished sends `completed` only when is_seed.
    use crate::tracker::TorrentStatsProvider;
    use futures::FutureExt;
    const PIECE: usize = 16384;
    let dir = crate::tests::test_util::create_default_random_dir_with_torrents(
        2,
        PIECE,
        Some("partial_selection"),
    );
    let torrent = crate::create_torrent(
        dir.path(),
        crate::CreateTorrentOptions {
            piece_length: Some(u32::try_from(PIECE).unwrap()),
            ..Default::default()
        },
        &crate::spawn_utils::BlockingSpawner::new(1),
    )
    .await
    .unwrap()
    .as_bytes()
    .unwrap();
    std::fs::remove_file(dir.path().join("1.data")).unwrap();
    let session = Session::new_with_opts(
        dir.path().into(),
        SessionOptions {
            dht: None,
            persistence: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            ..crate::test_support::session_options()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                output_folder: Some(dir.path().to_str().unwrap().to_owned()),
                overwrite: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let (_, stats) = tracker_stats(&session, &handle);
    assert_eq!(stats.left_bytes, Some(PIECE as u64));

    // Drop the missing file from the selection: the selection finishes
    // during this run, the torrent still lacks that file's piece.
    let had = handle
        .with_metadata(|m| {
            m.file_infos
                .iter()
                .position(|f| f.relative_filename.ends_with("0.data"))
                .unwrap()
        })
        .unwrap();
    let ignored: Vec<_> = (0..handle.file_priorities().len())
        .filter(|file| *file != had)
        .map(|file| (file, crate::FilePriority::Ignored))
        .collect();
    handle.set_file_priorities(&ignored).unwrap();
    assert!(handle.live().unwrap().is_finished());
    assert!(!handle.live().unwrap().is_seed());

    let (info, stats) = tracker_stats(&session, &handle);
    assert_eq!(stats.left_bytes, Some(PIECE as u64));
    assert!(!stats.completed_during_run);
    assert!(info.wait_until_completed().now_or_never().is_none());

    // Paused, the pieces still say what the torrent lacks, and so does
    // what the run recorded as it ended.
    session.pause(&handle).await.unwrap();
    let (_, stats) = tracker_stats(&session, &handle);
    assert_eq!(stats.left_bytes, Some(PIECE as u64));
    assert_eq!(
        handle.shared().last_run.lock().map(|run| run.left_bytes),
        Some(PIECE as u64)
    );
    session.stop().await;
}
