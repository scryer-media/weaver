use super::*;
use std::{io, net::IpAddr, time::Duration};

#[tokio::test]
async fn async_tcp_reports_the_bound_source() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let target = listener.local_addr().unwrap();
    let binding = SocketEgress::SourceAddress("127.0.0.1".parse().unwrap());
    let (client, accepted) = tokio::join!(binding.connect(target), listener.accept());
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
    let client = SocketEgress::SourceAddress("127.0.0.1".parse().unwrap())
        .connect_blocking(target, Duration::from_secs(30))
        .unwrap();
    let (_, peer) = listener.accept().unwrap();
    assert_eq!(client.local_addr().unwrap(), peer);
    assert!(client.nodelay().unwrap());
}

#[test]
fn udp_binds_the_source_before_tunnel_construction() {
    let socket = SocketEgress::SourceAddress("127.0.0.1".parse().unwrap())
        .bind_udp("127.0.0.1:51820".parse().unwrap())
        .unwrap();
    assert_eq!(
        socket.local_addr().unwrap().ip(),
        "127.0.0.1".parse::<IpAddr>().unwrap()
    );
    assert_ne!(socket.local_addr().unwrap().port(), 0);
}

#[tokio::test]
async fn wrong_family_and_invalid_sources_are_refused_before_connect() {
    let target = "127.0.0.1:119".parse().unwrap();
    assert_eq!(
        SocketEgress::SourceAddress("::1".parse().unwrap())
            .connect(target)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::AddrNotAvailable
    );
    for source in ["0.0.0.0", "224.0.0.1"] {
        assert_eq!(
            SocketEgress::SourceAddress(source.parse().unwrap())
                .connect(target)
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    for name in ["", "bad\0name"] {
        assert_eq!(
            SocketEgress::Interface(name.into())
                .connect(target)
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
        let (client, accepted) = tokio::join!(egress.connect(target), listener.accept());
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
        binding.connect(listener.local_addr().unwrap()),
        listener.accept()
    );
    assert_eq!(client.unwrap().local_addr().unwrap(), accepted.unwrap().1);
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn macos_device_binding_uses_loopback() {
    for address in ["127.0.0.1:0", "[::1]:0"] {
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let binding = SocketEgress::Interface("lo0".into());
        let client = binding
            .connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (_, peer) = listener.accept().await.unwrap();
        assert_eq!(client.local_addr().unwrap(), peer);
    }
}
