// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::{io, net::IpAddr, time::Duration};

#[tokio::test]
async fn async_tcp_reports_the_bound_source() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let binding = SocketEgress::SourceAddress("127.0.0.1".parse().unwrap());
    let (client, accepted) = tokio::join!(
        weaver_tunnel::test_support::loopback::connect_on(&binding, target),
        listener.accept()
    );
    let client = client.unwrap();
    let (server, peer) = accepted.unwrap();
    assert_eq!(client.local_addr().unwrap(), peer);
    assert_eq!(peer.ip(), "127.0.0.1".parse::<IpAddr>().unwrap());
    assert_eq!(server.local_addr().unwrap(), target);
    assert!(client.nodelay().unwrap());
}

#[test]
fn blocking_tcp_keeps_a_real_socket_and_the_source_binding() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let target = listener.local_addr().unwrap();
    let client = weaver_tunnel::test_support::loopback::connect_blocking_on(
        &SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        target,
        Duration::from_secs(30),
    )
    .unwrap();
    let (_, peer) = listener.accept().unwrap();
    assert_eq!(client.local_addr().unwrap(), peer);
    assert!(client.nodelay().unwrap());
}

#[test]
fn udp_binds_the_source_before_tunnel_construction() {
    let socket = weaver_tunnel::test_support::loopback::bind_udp_on(
        &SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        std::net::SocketAddr::from(([127, 0, 0, 1], 51820)),
    )
    .unwrap();
    assert_eq!(
        socket.local_addr().unwrap().ip(),
        "127.0.0.1".parse::<IpAddr>().unwrap()
    );
    assert_ne!(socket.local_addr().unwrap().port(), 0);
}

#[tokio::test]
async fn wrong_family_and_invalid_sources_are_refused_before_connect() {
    use weaver_tunnel::test_support::loopback::connect_on;
    let target: std::net::SocketAddr = "127.0.0.1:119".parse().unwrap();
    assert_eq!(
        connect_on(&SocketEgress::SourceAddress("::1".parse().unwrap()), target)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrNotAvailable
    );
    for source in ["0.0.0.0", "224.0.0.1"] {
        assert_eq!(
            connect_on(
                &SocketEgress::SourceAddress(source.parse().unwrap()),
                target
            )
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    for name in ["", "bad\0name"] {
        assert_eq!(
            connect_on(&SocketEgress::Interface(name.into()), target)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
}

#[cfg(any(target_os = "linux", target_os = "windows"))]
#[tokio::test]
async fn two_source_addresses_produce_two_distinct_socket_sources() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    for source in ["127.0.0.2", "127.0.0.3"] {
        let egress = SocketEgress::SourceAddress(source.parse().unwrap());
        let (client, accepted) = tokio::join!(
            weaver_tunnel::test_support::loopback::connect_on(&egress, target),
            listener.accept()
        );
        assert_eq!(
            client.unwrap().local_addr().unwrap().ip(),
            source.parse::<IpAddr>().unwrap()
        );
        assert_eq!(accepted.unwrap().1.ip(), source.parse::<IpAddr>().unwrap());
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn linux_device_binding_uses_loopback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let binding = SocketEgress::Interface("lo".into());
    let (client, accepted) = tokio::join!(
        weaver_tunnel::test_support::loopback::connect_on(&binding, listener.local_addr().unwrap()),
        listener.accept()
    );
    assert_eq!(client.unwrap().local_addr().unwrap(), accepted.unwrap().1);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_device_binding_uses_loopback() {
    for address in [
        std::net::SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, 0)),
        std::net::SocketAddr::from((std::net::Ipv6Addr::LOCALHOST, 0)),
    ] {
        let listener = match tokio::net::TcpListener::bind(address).await {
            Ok(listener) => listener,
            // A host with IPv6 disabled has no [::1] to listen on.
            Err(error) if address.is_ipv6() => {
                assert_eq!(error.kind(), io::ErrorKind::AddrNotAvailable);
                continue;
            }
            Err(error) => panic!("bind {address}: {error}"),
        };
        let binding = SocketEgress::Interface("lo0".into());
        let client = weaver_tunnel::test_support::loopback::connect_on(
            &binding,
            listener.local_addr().unwrap(),
        )
        .await
        .unwrap();
        let (_, peer) = listener.accept().await.unwrap();
        assert_eq!(client.local_addr().unwrap(), peer);
    }
}
