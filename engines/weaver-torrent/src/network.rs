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

use weaver_utp::{DefaultUtpEnvironment, PollSendToVectored, Transport, UtpStream};

pub use crate::core::net::{BoxFuture, DatagramSocket, NoNames, Resolved, Resolver};
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

    /// The reports of a port the transport keeps forwarded to this listener
    /// itself, for one whose transport does. Taken once, when the session
    /// starts the listener; the session announces the TCP port they give,
    /// as it does a port its own mappers got.
    fn port_mappings(&self) -> Option<crate::upnp::PortMapReceiver> {
        None
    }
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

    /// Start the session's peer listener on `addr`: the one a port the
    /// transport forwards reaches, which may listen on that port instead.
    /// Every other listener, SSL torrents' included, is a [`Self::listen_tcp`].
    fn listen_peers_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listen_tcp(addr)
    }

    /// Bind a datagram socket on `addr`.
    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>>;
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

/// The session's uTP socket, over a datagram socket from a
/// [`SocketFactory`], or from [`KernelSockets`] when none is set.
#[derive(Clone, Debug)]
pub(crate) struct UtpSocket(Arc<weaver_utp::UtpSocket<DatagramTransport, DefaultUtpEnvironment>>);

impl UtpSocket {
    /// A uTP socket over a datagram socket `factory` binds on `addr`.
    pub(crate) async fn bind(
        factory: &dyn SocketFactory,
        addr: SocketAddr,
        opts: weaver_utp::SocketOpts,
    ) -> anyhow::Result<Self> {
        use anyhow::Context;
        let transport = factory
            .bind_udp(addr)
            .await
            .context("error binding UDP socket")?;
        let sock = weaver_utp::UtpSocket::new_with_opts(
            DatagramTransport(transport),
            DefaultUtpEnvironment::default(),
            opts,
        )?;
        Ok(Self(sock))
    }

    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }

    pub(crate) async fn accept(&self) -> weaver_utp::Result<UtpStream> {
        self.0.accept().await
    }

    pub(crate) async fn connect(&self, remote: SocketAddr) -> weaver_utp::Result<UtpStream> {
        self.0.connect(remote).await
    }
}

/// The kernel sockets a session opens when it is given no factory, as a
/// factory: TCP dialled and listened on, and UDP bound, by the same calls
/// and on the same device. A caller that watches a session's sockets wraps
/// this and hands the session the wrapper.
pub struct KernelSockets {
    bind_device: Option<librqbit_dualstack_sockets::BindDevice>,
}

impl KernelSockets {
    /// The kernel sockets of a session bound to `bind_device_name`, or to
    /// none.
    pub fn new(bind_device_name: Option<&str>) -> anyhow::Result<Self> {
        let bind_device = bind_device_name
            .map(librqbit_dualstack_sockets::BindDevice::new_from_name)
            .transpose()?;
        Ok(Self { bind_device })
    }

    /// The kernel sockets of a session bound to `bind_device`, or to none.
    pub(crate) fn on_device(bind_device: Option<&librqbit_dualstack_sockets::BindDevice>) -> Self {
        Self {
            bind_device: bind_device.cloned(),
        }
    }

    fn bind_opts(&self) -> librqbit_dualstack_sockets::BindOpts<'_> {
        librqbit_dualstack_sockets::BindOpts {
            request_dualstack: true,
            reuseport: false,
            device: self.bind_device.as_ref(),
        }
    }
}

impl SocketFactory for KernelSockets {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let stream = librqbit_dualstack_sockets::tcp_connect(
                addr,
                librqbit_dualstack_sockets::ConnectOpts {
                    source_port: None,
                    bind_device: self.bind_device.as_ref(),
                },
            )
            .await
            .map_err(io::Error::other)?;
            let (read, write) = stream.into_split();
            Ok((
                Box::new(read) as BoxAsyncReadVectored,
                Box::new(write) as BoxAsyncWrite,
            ))
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        Box::pin(async move {
            let listener =
                librqbit_dualstack_sockets::TcpListener::bind_tcp(addr, self.bind_opts())
                    .map_err(io::Error::other)?;
            Ok(Box::new(KernelListener(listener)) as Box<dyn StreamListener>)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let socket = librqbit_dualstack_sockets::UdpSocket::bind_udp(addr, self.bind_opts())
                .map_err(io::Error::other)?;
            Ok(Arc::new(KernelUdp(socket)) as Arc<dyn DatagramSocket>)
        })
    }
}

struct KernelListener(librqbit_dualstack_sockets::TcpListener);

impl StreamListener for KernelListener {
    fn local_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, peer) = self.0.accept().await?;
            let (read, write) = stream.into_split();
            Ok((
                peer,
                Box::new(read) as BoxAsyncReadVectored,
                Box::new(write) as BoxAsyncWrite,
            ))
        })
    }
}

struct KernelUdp(librqbit_dualstack_sockets::UdpSocket);

impl DatagramSocket for KernelUdp {
    fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(self.0.recv_from(buf))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(self.0.send_to(buf, target))
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_to(cx, buf, target)
    }
}
