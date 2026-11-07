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
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV6},
    sync::Arc,
    task::{Context, Poll},
};

use weaver_tunnel::egress::SocketEgress;
use weaver_utp::{DefaultUtpEnvironment, PollSendToVectored, Transport, UtpStream};
pub use weaver_utp::{
    canonical_ip_addr, canonical_socket_addr, is_message_size_error, message_size_error,
};

pub use crate::core::net::{BoxFuture, DatagramSend, DatagramSocket, NoNames, Resolved, Resolver};
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

    /// Whether a TCP peer connection finds a socket free now. A route that
    /// holds its TCP peers to a share says no once the share is full, and
    /// peers then connect over uTP alone.
    fn tcp_room(&self) -> bool {
        true
    }

    /// Whether a datagram socket bind finds a socket free now. A route that
    /// holds its datagram sockets to a share says no once the share is full.
    fn datagram_room(&self) -> bool {
        true
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
    fn sender(&self) -> Option<Box<dyn DatagramSend>> {
        self.0.sender()
    }

    fn shutdown(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.0.shutdown()
    }

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

    fn link_mtu(&self) -> Option<u16> {
        self.0.link_mtu()
    }

    fn dont_fragment(&self) -> bool {
        self.0.dont_fragment()
    }
}

/// The session's uTP socket, over a datagram socket from its
/// [`SocketFactory`].
#[derive(Clone, Debug)]
pub(crate) struct UtpSocket(pub(crate) Arc<UtpSocketInner>);

pub(crate) type UtpSocketInner = weaver_utp::UtpSocket<DatagramTransport, DefaultUtpEnvironment>;

impl UtpSocket {
    pub(crate) async fn shutdown(&self) -> io::Result<()> {
        self.0.shutdown().await
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

/// The kernel's sockets, as a factory: TCP dialled and listened on, and UDP
/// bound, on one device or none. A caller that watches a session's sockets
/// wraps this and hands the session the wrapper.
pub struct KernelSockets {
    egress: SocketEgress,
}

impl KernelSockets {
    /// The kernel sockets of a session bound to `bind_device_name`, or to
    /// none.
    pub fn new(bind_device_name: Option<&str>) -> anyhow::Result<Self> {
        let egress = match bind_device_name {
            Some(name) => SocketEgress::existing_interface(name)?,
            None => SocketEgress::System,
        };
        Ok(Self { egress })
    }

    // Whether a bind at `addr` opens a socket for each family, IPv6 alone at
    // `addr` and IPv4 on its port, each held to the device: the unspecified
    // IPv6 address on a device, which one socket cannot hold to it for both
    // families on every host.
    fn paired(&self, addr: SocketAddr) -> bool {
        matches!(self.egress, SocketEgress::Interface(_))
            && addr.is_ipv6()
            && addr.ip().is_unspecified()
    }

    /// A datagram socket at `addr`, at the kernel's defaults. One at the
    /// unspecified IPv6 address takes IPv4 as well, unless it is one of a
    /// pair.
    fn kernel_udp(&self, addr: SocketAddr) -> io::Result<KernelUdp> {
        #[expect(
            clippy::disallowed_methods,
            reason = "the kernel's own route binds on its device, or the host's"
        )]
        let (socket, dual_stack) = self.egress.bind_udp_reusing(addr)?;
        let socket = tokio::net::UdpSocket::from_std(socket)?;
        let mut socket = KernelUdp {
            local: socket.local_addr()?,
            socket,
            dual_stack,
            dont_fragment: false,
        };
        socket.cache_dont_fragment();
        Ok(socket)
    }

    // The datagram sockets at `addr`, each set for uTP with a receive buffer
    // of `utp` bytes when it holds one.
    fn datagrams(
        &self,
        addr: SocketAddr,
        utp: Option<usize>,
    ) -> io::Result<Arc<dyn DatagramSocket>> {
        let set = |mut socket: KernelUdp, at: SocketAddr| {
            if let Some(receive_buffer) = utp {
                crate::udp_options::set_for_utp(&socket.socket, at, receive_buffer);
                socket.cache_dont_fragment();
            }
            socket
        };
        if !self.paired(addr) {
            return Ok(Arc::new(set(self.kernel_udp(addr)?, addr)));
        }
        let (v6, v4) = bind_pair(addr, |at| self.kernel_udp(at), |socket| socket.local.port())?;
        let (v6_addr, v4_addr) = (v6.local, v4.local);
        Ok(Arc::new(PairedUdp {
            v6: set(v6, v6_addr),
            v4: set(v4, v4_addr),
        }))
    }
}

// How many free IPv6 ports a pair tries before giving up on one IPv4 also
// has free.
const PAIR_ATTEMPTS: usize = 16;

// A socket for each family on one port: IPv6 alone at `addr`, then IPv4 on
// the port it took. Port 0 lets the kernel choose, and a port it chose for
// IPv6 that IPv4 holds is given up for another; the IPv6 sockets given up
// stay bound until the pair is made, so the kernel never chooses one again.
fn bind_pair<T>(
    addr: SocketAddr,
    bind: impl Fn(SocketAddr) -> io::Result<T>,
    port_of: impl Fn(&T) -> u16,
) -> io::Result<(T, T)> {
    let mut given_up = Vec::new();
    loop {
        let v6 = bind(addr)?;
        match bind(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port_of(&v6)))) {
            Ok(v4) => return Ok((v6, v4)),
            Err(error)
                if addr.port() == 0
                    && error.kind() == io::ErrorKind::AddrInUse
                    && given_up.len() + 1 < PAIR_ATTEMPTS =>
            {
                given_up.push(v6);
            }
            Err(error) => return Err(error),
        }
    }
}

impl SocketFactory for KernelSockets {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            #[expect(
                clippy::disallowed_methods,
                reason = "the kernel's own route dials from its device, or the host's"
            )]
            let stream = self.egress.connect(addr).await?;
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
            let listen = |at| -> io::Result<KernelListener> {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "the kernel's own route listens on its device, or the host's"
                )]
                let listener = tokio::net::TcpListener::from_std(self.egress.listen_tcp(at)?)?;
                Ok(KernelListener {
                    local: listener.local_addr()?,
                    listener,
                })
            };
            if !self.paired(addr) {
                return Ok(Box::new(listen(addr)?) as Box<dyn StreamListener>);
            }
            let (v6, v4) = bind_pair(addr, listen, |listener| listener.local.port())?;
            Ok(Box::new(PairedListener { v6, v4 }) as Box<dyn StreamListener>)
        })
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move { self.datagrams(addr, None) })
    }

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move { self.datagrams(addr, Some(receive_buffer)) })
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

struct KernelListener {
    listener: tokio::net::TcpListener,
    local: SocketAddr,
}

impl KernelListener {
    // The next peer, an IPv4 one on a socket that takes both families
    // named by its IPv4 address.
    async fn take(&self) -> io::Result<(tokio::net::TcpStream, SocketAddr)> {
        let (stream, peer) = self.listener.accept().await?;
        Ok((stream, canonical_socket_addr(peer)))
    }
}

impl StreamListener for KernelListener {
    fn local_addr(&self) -> SocketAddr {
        self.local
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, peer) = self.take().await?;
            let (read, write) = stream.into_split();
            Ok((
                peer,
                Box::new(read) as BoxAsyncReadVectored,
                Box::new(write) as BoxAsyncWrite,
            ))
        })
    }
}

// A listener for each family on one port, taking a peer from either.
struct PairedListener {
    v6: KernelListener,
    v4: KernelListener,
}

impl StreamListener for PairedListener {
    fn local_addr(&self) -> SocketAddr {
        self.v6.local
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, peer) = tokio::select! {
                accepted = self.v6.take() => accepted?,
                accepted = self.v4.take() => accepted?,
            };
            let (read, write) = stream.into_split();
            Ok((
                peer,
                Box::new(read) as BoxAsyncReadVectored,
                Box::new(write) as BoxAsyncWrite,
            ))
        })
    }
}

// A datagram socket for each family on one port: datagrams come in on
// either, and each goes out on the one of its target's family.
struct PairedUdp {
    v6: KernelUdp,
    v4: KernelUdp,
}

impl PairedUdp {
    // The socket for `target`, an IPv4-mapped IPv6 address taken as IPv4.
    fn toward(&self, target: SocketAddr) -> (&KernelUdp, SocketAddr) {
        match target {
            SocketAddr::V6(v6) => match v6.ip().to_ipv4_mapped() {
                Some(ip) => (&self.v4, SocketAddr::new(ip.into(), v6.port())),
                None => (&self.v6, target),
            },
            SocketAddr::V4(_) => (&self.v4, target),
        }
    }
}

impl DatagramSocket for PairedUdp {
    fn dont_fragment(&self) -> bool {
        self.v6.dont_fragment() && self.v4.dont_fragment()
    }

    fn bind_addr(&self) -> SocketAddr {
        self.v6.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(async move {
            let (v6, v4) = (&self.v6.socket, &self.v4.socket);
            loop {
                let socket = tokio::select! {
                    ready = v6.readable() => { ready?; v6 }
                    ready = v4.readable() => { ready?; v4 }
                };
                match socket.try_recv_from(buf) {
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                    received => return received,
                }
            }
        })
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        let (socket, target) = self.toward(target);
        socket.send_to(buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let (socket, target) = self.toward(target);
        socket.poll_send_to(cx, buf, target)
    }

    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let (socket, target) = self.toward(target);
        socket.poll_send_to_vectored(cx, bufs, target)
    }
}

/// A kernel datagram socket: the address it is bound at, whether it takes
/// IPv4 as well as IPv6, and whether its datagrams are never fragmented.
pub(crate) struct KernelUdp {
    socket: tokio::net::UdpSocket,
    local: SocketAddr,
    dual_stack: bool,
    dont_fragment: bool,
}

impl KernelUdp {
    fn cache_dont_fragment(&mut self) {
        let family = if self.local.is_ipv4() {
            crate::udp_options::Family::V4
        } else {
            crate::udp_options::Family::V6
        };
        self.dont_fragment = crate::udp_options::dont_fragment(&self.socket, family)
            .unwrap_or(false)
            && (!self.dual_stack
                || crate::udp_options::dont_fragment(&self.socket, crate::udp_options::Family::V4)
                    .unwrap_or(false));
    }

    // `target` as the socket names it: an IPv4 one by its IPv4-mapped IPv6
    // address on a socket that takes both families.
    fn toward(&self, target: SocketAddr) -> SocketAddr {
        match target {
            SocketAddr::V4(v4) if self.dual_stack => {
                SocketAddr::V6(SocketAddrV6::new(v4.ip().to_ipv6_mapped(), v4.port(), 0, 0))
            }
            target => target,
        }
    }
}

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
        let mut socket = self.kernel_udp(addr)?;
        crate::udp_options::set_for_utp(&socket.socket, addr, receive_buffer);
        socket.cache_dont_fragment();
        Ok(Arc::new(socket))
    }
}

#[cfg(test)]
impl KernelUdp {
    /// A datagram already waiting, without waiting for one.
    pub(crate) fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.socket.try_recv_from(buf)
    }

    /// The kernel socket itself.
    pub(crate) fn socket(&self) -> &tokio::net::UdpSocket {
        &self.socket
    }
}

impl DatagramSocket for KernelUdp {
    fn dont_fragment(&self) -> bool {
        self.dont_fragment
    }

    fn bind_addr(&self) -> SocketAddr {
        self.local
    }

    // A datagram from an IPv4 peer is from its IPv4 address, whichever
    // family's socket took it.
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(async move {
            let (len, from) = self.socket.recv_from(buf).await?;
            Ok((len, canonical_socket_addr(from)))
        })
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(self.socket.send_to(buf, self.toward(target)))
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_to(cx, buf, self.toward(target))
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
        let target = self.toward(target);
        let socket = &self.socket;
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

#[cfg(test)]
mod mapped_endpoint_tests;

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests {
    use std::{
        io,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    };

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::{KernelSockets, SocketFactory};

    // The loopback device, which an Interface leg can be held to.
    const LOOPBACK: &str = if cfg!(target_os = "linux") {
        "lo"
    } else {
        "lo0"
    };

    // Whether the host withholds holding a socket to a device.
    fn withheld(error: &io::Error) -> bool {
        error.kind() == io::ErrorKind::PermissionDenied
    }

    #[tokio::test]
    async fn an_interface_leg_at_the_unspecified_v6_address_reaches_both_families() {
        let leg = KernelSockets::new(Some(LOOPBACK)).unwrap();
        let unspecified = SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0));
        let socket = match leg
            .bind_utp(unspecified, crate::udp_options::STREAM_WINDOW)
            .await
        {
            Ok(socket) => socket,
            // Holding a socket to a device needs a privilege some Linux
            // hosts withhold.
            Err(error) if withheld(&error) => return,
            Err(error) => panic!("{error}"),
        };
        let port = socket.bind_addr().port();
        assert_eq!(socket.bind_addr(), (Ipv6Addr::UNSPECIFIED, port).into());
        let mut buf = [0u8; 16];
        for ip in [
            std::net::IpAddr::from(Ipv4Addr::LOCALHOST),
            Ipv6Addr::LOCALHOST.into(),
        ] {
            let peer = crate::test_support::udp((ip, 0)).await;
            let peer_addr = peer.bind_addr();
            // Out on the socket of the peer's family, from the leg's port.
            socket.send_to(b"out", peer_addr).await.unwrap();
            let (len, from) = peer.recv_from(&mut buf).await.unwrap();
            assert_eq!((&buf[..len], from), (&b"out"[..], (ip, port).into()));
            // In on it, from the peer's own address.
            peer.send_to(b"in", from).await.unwrap();
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            assert_eq!((&buf[..len], from), (&b"in"[..], peer_addr));
        }

        let listener = leg.listen_tcp(unspecified).await.unwrap();
        let port = listener.local_addr().port();
        for ip in [
            std::net::IpAddr::from(Ipv4Addr::LOCALHOST),
            Ipv6Addr::LOCALHOST.into(),
        ] {
            let (_, mut peer) = crate::test_support::tcp((ip, port)).await.unwrap();
            let (from, mut read, _write) = listener.accept().await.unwrap();
            assert_eq!(from.ip(), ip);
            peer.write_all(b"hi").await.unwrap();
            let mut got = [0u8; 2];
            read.read_exact(&mut got).await.unwrap();
            assert_eq!(&got, b"hi");
        }
    }
}

#[cfg(test)]
mod pair_tests {
    use std::{
        cell::{Cell, RefCell},
        collections::BTreeSet,
        io,
        net::{Ipv4Addr, Ipv6Addr, SocketAddr},
        rc::Rc,
    };

    use super::{DatagramSocket, KernelSockets, PAIR_ATTEMPTS, PairedUdp, bind_pair};

    // A bound port, held until dropped.
    struct Held {
        port: u16,
        bound: Rc<RefCell<BTreeSet<(bool, u16)>>>,
        v6: bool,
    }

    impl Drop for Held {
        fn drop(&mut self) {
            self.bound.borrow_mut().remove(&(self.v6, self.port));
        }
    }

    // A host whose IPv6 binds at port 0 take the lowest port no IPv6
    // socket holds, from 1, and whose IPv4 binds fail with `v4` while it
    // returns an error.
    struct Host {
        bound: Rc<RefCell<BTreeSet<(bool, u16)>>>,
        v4_binds: Cell<usize>,
        v4: Box<dyn Fn(usize) -> Option<io::ErrorKind>>,
    }

    impl Host {
        fn new(v4: impl Fn(usize) -> Option<io::ErrorKind> + 'static) -> Self {
            Self {
                bound: Rc::default(),
                v4_binds: Cell::new(0),
                v4: Box::new(v4),
            }
        }

        fn bind(&self, at: SocketAddr) -> io::Result<Held> {
            let v6 = at.is_ipv6();
            if !v6 {
                let n = self.v4_binds.get();
                self.v4_binds.set(n + 1);
                if let Some(kind) = (self.v4)(n) {
                    return Err(kind.into());
                }
            }
            let port = match at.port() {
                0 => (1..)
                    .find(|port| !self.bound.borrow().contains(&(v6, *port)))
                    .unwrap(),
                port => port,
            };
            self.bound.borrow_mut().insert((v6, port));
            Ok(Held {
                port,
                bound: Rc::clone(&self.bound),
                v6,
            })
        }

        fn pair(&self, port: u16) -> io::Result<(Held, Held)> {
            bind_pair(
                (Ipv6Addr::UNSPECIFIED, port).into(),
                |at| self.bind(at),
                |held| held.port,
            )
        }
    }

    #[test]
    fn a_port_ipv4_holds_is_given_up_for_a_new_one() {
        // IPv4 holds the first two ports IPv6 is given.
        let host = Host::new(|n| (n < 2).then_some(io::ErrorKind::AddrInUse));
        let (v6, v4) = host.pair(0).unwrap();
        assert_eq!((v6.port, v4.port), (3, 3));
        // The ports given up were held until the pair was made, then let go.
        assert_eq!(
            *host.bound.borrow(),
            BTreeSet::from([(true, 3), (false, 3)])
        );
    }

    #[test]
    fn a_pair_gives_up_after_its_last_attempt() {
        let host = Host::new(|_| Some(io::ErrorKind::AddrInUse));
        let error = host.pair(0).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(host.v4_binds.get(), PAIR_ATTEMPTS);
        // Nothing stays bound.
        assert!(host.bound.borrow().is_empty());
    }

    #[test]
    fn a_port_asked_for_is_never_given_up() {
        let host = Host::new(|_| Some(io::ErrorKind::AddrInUse));
        let error = host.pair(6881).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        assert_eq!(host.v4_binds.get(), 1);
        assert!(host.bound.borrow().is_empty());
    }

    #[test]
    fn any_other_ipv4_failure_is_returned_at_once() {
        let host = Host::new(|_| Some(io::ErrorKind::PermissionDenied));
        let error = host.pair(0).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(host.v4_binds.get(), 1);
        assert!(host.bound.borrow().is_empty());
    }

    #[tokio::test]
    async fn a_mapped_ipv4_target_goes_out_on_the_ipv4_socket_unmapped() {
        let sockets = KernelSockets::new(None).unwrap();
        let paired = PairedUdp {
            v6: sockets.kernel_udp((Ipv6Addr::LOCALHOST, 0).into()).unwrap(),
            v4: sockets.kernel_udp((Ipv4Addr::LOCALHOST, 0).into()).unwrap(),
        };
        let peer = crate::test_support::udp((Ipv4Addr::LOCALHOST, 0)).await;
        let port = peer.bind_addr().port();
        let mapped = SocketAddr::from((Ipv4Addr::LOCALHOST.to_ipv6_mapped(), port));
        let (socket, target) = paired.toward(mapped);
        assert!(std::ptr::eq(socket, &paired.v4));
        assert_eq!(target, SocketAddr::from((Ipv4Addr::LOCALHOST, port)));

        paired.send_to(b"out", mapped).await.unwrap();
        let mut buf = [0u8; 8];
        let (len, from) = peer.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&b"out"[..], paired.v4.bind_addr()));
    }
}
