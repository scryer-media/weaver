// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Pluggable network transport for a [`Session`](crate::Session).
//!
//! By default a session binds and connects kernel sockets itself. An embedder
//! that needs peer traffic to flow through something else (a userspace network
//! stack, a VPN tunnel, a network namespace, a test harness) can implement
//! [`SocketFactory`] and pass it in
//! [`SessionOptions::socket_factory`](crate::SessionOptions::socket_factory).
//!
//! When no factory is set, nothing in this module is on the hot path: the
//! session keeps using concrete kernel sockets exactly as before.

use std::{
    future::Future,
    io::{self, IoSlice},
    net::SocketAddr,
    sync::Arc,
    task::{Context, Poll},
};

use librqbit_dualstack_sockets::PollSendToVectored;
use librqbit_utp::{Transport, UtpSocketUdp, UtpStream};

pub use crate::core::net::{BoxFuture, DatagramSocket, Resolver, SystemResolver};
pub use crate::type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite};
pub use crate::vectored_traits::{
    AsyncReadToVectoredCompat, AsyncReadVectored, AsyncReadVectoredIntoCompat,
};

/// A listening stream socket, the shape of a TCP listener.
pub trait StreamListener: Send + Sync + 'static {
    /// The local address the listener is bound to. Its port is announced to
    /// trackers and DHT unless `ListenerOptions::announce_port` overrides it.
    fn local_addr(&self) -> SocketAddr;

    /// Wait for the next incoming connection and return the remote address
    /// with the read and write halves of the stream.
    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>>;
}

/// Creates the sockets a [`Session`](crate::Session) uses for peer traffic.
///
/// Set it in [`SessionOptions::socket_factory`](crate::SessionOptions::socket_factory)
/// to replace kernel sockets with another transport. Every `addr` passed to a
/// method may be an unspecified address (`0.0.0.0` or `[::]`) and may carry
/// port 0; the factory decides what it actually binds, and reports it back
/// through [`StreamListener::local_addr`] and [`DatagramSocket::bind_addr`].
///
/// With a factory set:
/// - outgoing peer TCP connections are dialed with [`SocketFactory::connect_tcp`];
/// - the incoming peer TCP listener is created with [`SocketFactory::listen_tcp`];
/// - the uTP socket (incoming and outgoing uTP) runs over [`SocketFactory::bind_udp`].
///
/// `SessionOptions::bind_device_name` does not apply to sockets created by the
/// factory.
///
/// HTTP traffic (HTTP trackers, torrent and blocklist downloads) is routed by
/// passing a prebuilt client in
/// [`SessionOptions::http_client`](crate::SessionOptions::http_client).
///
/// Local service discovery (multicast) and UPnP port forwarding are out of
/// scope and keep using kernel sockets; disable them if they must not reach
/// the host network.
pub trait SocketFactory: Send + Sync + 'static {
    /// Open a stream connection to `addr`.
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>>;

    /// Start listening for stream connections on `addr`.
    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>>;

    /// Bind a datagram socket on `addr`.
    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>>;

    /// The resolver for DHT bootstrap hostnames and UDP tracker hostnames,
    /// and for web seed hostnames, whose addresses are checked before the
    /// HTTP client asks them.
    ///
    /// The default, `None`, uses the host's resolver ([`SystemResolver`]).
    /// HTTP tracker hostnames are resolved by the HTTP client; see
    /// [`SessionOptions::http_client`](crate::SessionOptions::http_client).
    fn resolver(&self) -> Option<Arc<dyn Resolver>> {
        None
    }
}

impl std::fmt::Debug for dyn SocketFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SocketFactory")
    }
}

/// Adapts a [`DatagramSocket`] from a [`SocketFactory`] to the uTP transport.
pub(crate) struct DatagramTransport(pub(crate) Arc<dyn DatagramSocket>);

impl PollSendToVectored for DatagramTransport {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_to_vectored(cx, bufs, target)
    }
}

impl Transport for DatagramTransport {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = io::Result<(usize, SocketAddr)>> + Send + Sync + 'a {
        self.0.recv_from(buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> impl Future<Output = io::Result<usize>> + Send + Sync + 'a {
        self.0.send_to(buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_to(cx, buf, target)
    }

    fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }
}

// librqbit-utp does not export its default environment type by name, so it
// is recovered from the `UtpSocketUdp` alias.
pub(crate) trait UtpSocketEnv {
    type Env;
}

impl<T, E> UtpSocketEnv for librqbit_utp::UtpSocket<T, E> {
    type Env = E;
}

type DefaultUtpEnvironment = <UtpSocketUdp as UtpSocketEnv>::Env;

/// The session's uTP socket: over a kernel UDP socket by default, or over a
/// datagram socket from a [`SocketFactory`].
#[derive(Clone, Debug)]
pub(crate) enum UtpSocket {
    Udp(Arc<UtpSocketUdp>),
    Custom(Arc<librqbit_utp::UtpSocket<DatagramTransport, DefaultUtpEnvironment>>),
}

impl UtpSocket {
    pub(crate) fn bind_addr(&self) -> SocketAddr {
        match self {
            UtpSocket::Udp(s) => s.bind_addr(),
            UtpSocket::Custom(s) => s.bind_addr(),
        }
    }

    pub(crate) async fn accept(&self) -> librqbit_utp::Result<UtpStream> {
        match self {
            UtpSocket::Udp(s) => s.accept().await,
            UtpSocket::Custom(s) => s.accept().await,
        }
    }

    pub(crate) async fn connect(&self, remote: SocketAddr) -> librqbit_utp::Result<UtpStream> {
        match self {
            UtpSocket::Udp(s) => s.connect(remote).await,
            UtpSocket::Custom(s) => s.connect(remote).await,
        }
    }
}
