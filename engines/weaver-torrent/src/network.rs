// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Pluggable network transport for a [`Session`](crate::Session).
//!
//! A session opens no socket toward the internet itself: every one comes from
//! the [`SocketFactory`] in
//! [`SessionOptions::socket_factory`](crate::SessionOptions::socket_factory).
//! [`KernelSockets`] opens the kernel's; an embedder that needs peer traffic
//! to flow through something else (a userspace network stack, a VPN tunnel,
//! a network namespace, a test harness) implements its own.

use std::{
    future::Future,
    io::{self, IoSlice},
    net::{IpAddr, SocketAddr, SocketAddrV6},
    sync::Arc,
    task::{Context, Poll},
};

use librqbit_dualstack_sockets::PollSendToVectored;
use librqbit_utp::{Transport, UtpSocketUdp, UtpStream};

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
/// The session's is [`SessionOptions::socket_factory`](crate::SessionOptions::socket_factory).
/// Every `addr` passed to a
/// method may be an unspecified address (`0.0.0.0` or `[::]`) and may carry
/// port 0; the factory decides what it actually binds, and reports it back
/// through [`StreamListener::local_addr`] and [`DatagramSocket::bind_addr`].
///
/// - outgoing peer TCP connections are dialed with [`SocketFactory::connect_tcp`];
/// - the incoming peer TCP listener is created with [`SocketFactory::listen_tcp`];
/// - the uTP socket (incoming and outgoing uTP), the DHT socket and the UDP
///   trackers' socket run over [`SocketFactory::bind_udp`].
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

    /// Bind the datagram socket uTP runs over on `addr`, with a receive
    /// buffer of `receive_buffer` bytes where the factory sets one, and its
    /// datagrams never fragmented. A factory that sets neither binds it as
    /// any other.
    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        let _ = receive_buffer;
        self.bind_udp(addr)
    }

    /// The local address traffic toward `to` leaves from on this route, or
    /// why it has none. None when the route does not say, as one through a
    /// proxy does not.
    fn source_toward(&self, _to: SocketAddr) -> Option<io::Result<IpAddr>> {
        None
    }

    /// A datagram socket bound at `local` on the network the gateway is
    /// reached on: the one a port mapper asks the gateway from. A route with
    /// no such socket has no gateway to ask.
    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!("no socket to ask the gateway from {local}"),
            ))
        })
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

/// The session's uTP socket, over a datagram socket from its
/// [`SocketFactory`].
#[derive(Clone, Debug)]
pub(crate) struct UtpSocket(
    pub(crate) Arc<librqbit_utp::UtpSocket<DatagramTransport, DefaultUtpEnvironment>>,
);

impl UtpSocket {
    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }

    pub(crate) async fn accept(&self) -> librqbit_utp::Result<UtpStream> {
        self.0.accept().await
    }

    pub(crate) async fn connect(&self, remote: SocketAddr) -> librqbit_utp::Result<UtpStream> {
        self.0.connect(remote).await
    }
}

/// The kernel's sockets, as a factory: TCP dialled and listened on, and UDP
/// bound, on one device or none. A caller that watches a session's sockets
/// wraps this and hands the session the wrapper.
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

    fn bind_opts(&self) -> librqbit_dualstack_sockets::BindOpts<'_> {
        librqbit_dualstack_sockets::BindOpts {
            request_dualstack: true,
            reuseport: false,
            device: self.bind_device.as_ref(),
        }
    }

    /// A datagram socket at `addr`, at the kernel's defaults.
    fn kernel_udp(&self, addr: SocketAddr) -> io::Result<KernelUdp> {
        let socket = librqbit_dualstack_sockets::UdpSocket::bind_udp(addr, self.bind_opts())
            .map_err(io::Error::other)?;
        Ok(KernelUdp(socket))
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
        Box::pin(async move { Ok(Arc::new(self.kernel_udp(addr)?) as Arc<dyn DatagramSocket>) })
    }

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            let socket = self.kernel_udp(addr)?;
            crate::udp_options::set_for_utp(socket.0.socket(), addr, receive_buffer);
            Ok(Arc::new(socket) as Arc<dyn DatagramSocket>)
        })
    }

    // The port mapper asks the gateway from a socket of its own, bound as
    // every other socket here is.
    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.bind_udp(local)
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

pub(crate) struct KernelUdp(librqbit_dualstack_sockets::UdpSocket);

/// Set a kernel datagram socket bound at `requested` for uTP to run over it,
/// as [`SocketFactory::bind_utp`] asks: its datagrams never fragmented, and
/// its receive buffer `receive_buffer` bytes as far as the host allows.
pub fn set_for_utp(socket: &tokio::net::UdpSocket, requested: SocketAddr, receive_buffer: usize) {
    crate::udp_options::set_for_utp(socket, requested, receive_buffer);
}

/// Datagrams the kernel sockets sent from slices, each in one system call.
#[cfg(test)]
pub(crate) static VECTORED_SENDS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
impl KernelSockets {
    /// A datagram socket at `addr`, bound at once, as the kernel socket it is.
    pub(crate) fn bind_kernel_udp(&self, addr: SocketAddr) -> io::Result<Arc<KernelUdp>> {
        self.kernel_udp(addr).map(Arc::new)
    }

    /// [`Self::bind_kernel_udp`], set for uTP with `receive_buffer`.
    pub(crate) fn bind_kernel_utp(
        &self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> io::Result<Arc<KernelUdp>> {
        let socket = self.kernel_udp(addr)?;
        crate::udp_options::set_for_utp(socket.0.socket(), addr, receive_buffer);
        Ok(Arc::new(socket))
    }
}

#[cfg(test)]
impl KernelUdp {
    /// A datagram already waiting, without waiting for one.
    pub(crate) fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.0.socket().try_recv_from(buf)
    }

    /// The kernel socket itself.
    pub(crate) fn socket(&self) -> &tokio::net::UdpSocket {
        self.0.socket()
    }
}

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

    // One datagram from every slice, in one system call, without joining
    // them first. A send the kernel cannot take yet clears the socket's
    // readiness, so the task waits for room instead of trying again at once.
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let target = match target {
            SocketAddr::V4(v4) if self.0.is_dualstack() => {
                SocketAddr::V6(SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0))
            }
            target => target,
        };
        let socket = self.0.socket();
        loop {
            std::task::ready!(socket.poll_send_ready(cx))?;
            match socket.try_io(tokio::io::Interest::WRITABLE, || {
                socket2::SockRef::from(socket).send_to_vectored(bufs, &target.into())
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                sent => {
                    #[cfg(test)]
                    if sent.is_ok() {
                        VECTORED_SENDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    return Poll::Ready(sent);
                }
            }
        }
    }
}
