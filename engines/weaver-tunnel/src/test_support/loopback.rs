// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The one way tests and fixtures open sockets of their own: on this
// machine's loopback alone. Each refuses an address off this machine, so no
// test reaches the network outside a route. Other crates' tests take these
// through the test-support feature.
use std::{io, net::SocketAddr, time::Duration};

use crate::{egress::SocketEgress, pipe::Egress};

fn loopback(addr: impl Into<SocketAddr>) -> SocketAddr {
    let addr = addr.into();
    assert!(
        addr.ip().is_loopback(),
        "{addr} is off this machine: a test reaches only its own loopback listeners"
    );
    addr
}

// A stream to a loopback listener.
#[expect(
    clippy::disallowed_methods,
    reason = "tests and fixtures reach their own loopback listeners"
)]
pub async fn connect(addr: impl Into<SocketAddr>) -> io::Result<tokio::net::TcpStream> {
    tokio::net::TcpStream::connect(loopback(addr)).await
}

// A blocking stream to a loopback listener.
#[expect(
    clippy::disallowed_methods,
    reason = "tests and fixtures reach their own loopback listeners"
)]
pub fn connect_blocking(addr: impl Into<SocketAddr>) -> io::Result<std::net::TcpStream> {
    std::net::TcpStream::connect(loopback(addr))
}

// Connects a socket a test built itself to a loopback listener.
#[expect(
    clippy::disallowed_methods,
    reason = "tests and fixtures reach their own loopback listeners"
)]
pub fn connect_socket(socket: &socket2::Socket, addr: impl Into<SocketAddr>) -> io::Result<()> {
    socket.connect(&loopback(addr).into())
}

// A datagram socket on a loopback address.
#[expect(
    clippy::disallowed_methods,
    reason = "tests and fixtures bind their own loopback sockets"
)]
pub async fn bind_udp(addr: impl Into<SocketAddr>) -> io::Result<tokio::net::UdpSocket> {
    tokio::net::UdpSocket::bind(loopback(addr)).await
}

// `binding`'s own dial, of a loopback listener, for a test of the binding.
#[expect(
    clippy::disallowed_methods,
    reason = "a test of an egress binding dials its own loopback listener"
)]
pub async fn connect_on(
    binding: &SocketEgress,
    addr: impl Into<SocketAddr>,
) -> io::Result<tokio::net::TcpStream> {
    binding.connect(loopback(addr)).await
}

// `binding`'s own blocking dial, of a loopback listener.
#[expect(
    clippy::disallowed_methods,
    reason = "a test of an egress binding dials its own loopback listener"
)]
pub fn connect_blocking_on(
    binding: &SocketEgress,
    addr: impl Into<SocketAddr>,
    timeout: Duration,
) -> io::Result<std::net::TcpStream> {
    binding.connect_blocking(loopback(addr), timeout)
}

// `binding`'s own datagram socket, toward a loopback peer.
#[expect(
    clippy::disallowed_methods,
    reason = "a test of an egress binding binds toward a loopback peer"
)]
pub fn bind_udp_on(
    binding: &SocketEgress,
    peer: impl Into<SocketAddr>,
) -> io::Result<std::net::UdpSocket> {
    binding.bind_udp(loopback(peer))
}

// An egress whose sockets leave from 127.0.0.1, so everything it dials or
// binds stays on this machine.
#[expect(
    clippy::disallowed_methods,
    reason = "a test's egress, held to the loopback address"
)]
pub fn egress(id: u32, timeout: Duration) -> Egress {
    Egress::new(
        id,
        SocketEgress::SourceAddress(std::net::Ipv4Addr::LOCALHOST.into()),
        timeout,
    )
}
