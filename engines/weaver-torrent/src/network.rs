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

pub use crate::core::net::{
    BoxFuture, DatagramSend, DatagramSocket, Fragmenting, NoNames, Resolved, Resolver,
};
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
/// The socket of a session's peer listener may be shared with the DHT: its
/// DHT messages then go to the DHT, every other datagram to uTP.
pub(crate) struct DatagramTransport(
    pub(crate) Arc<dyn DatagramSocket>,
    pub(crate) Option<Arc<DhtTap>>,
);

impl DatagramTransport {
    /// A transport that keeps every datagram for uTP.
    #[cfg(test)]
    pub(crate) fn alone(socket: Arc<dyn DatagramSocket>) -> Self {
        Self(socket, None)
    }
}

/// How many DHT messages wait for the DHT on a shared socket before more are
/// dropped, as a full socket buffer drops them.
const DHT_INBOX: usize = 256;

/// Where uTP's receive loop hands the DHT messages it reads on a socket the
/// two share, once the DHT takes the socket.
#[derive(Default)]
pub(crate) struct DhtTap {
    inbox: std::sync::OnceLock<tokio::sync::mpsc::Sender<(Box<[u8]>, SocketAddr)>>,
}

impl DhtTap {
    /// Takes `datagram` for the DHT when the DHT shares the socket and it
    /// is a DHT message. libtorrent v2.0.15 hands a datagram uTP does not
    /// take to its DHT when it is longer than 20 bytes and starts with `d`
    /// and ends with `e` (session_impl.cpp:2648-2663); a uTP header's first
    /// byte carries version 1, never `d`, so testing for the DHT first takes
    /// the same datagrams.
    fn take(&self, datagram: &[u8], from: SocketAddr) -> bool {
        let Some(inbox) = self.inbox.get() else {
            return false;
        };
        if !(datagram.len() > 20
            && datagram.first() == Some(&b'd')
            && datagram.last() == Some(&b'e'))
        {
            return false;
        }
        // A full inbox drops the message; a DHT that has gone takes none.
        let _ = inbox.try_send((datagram.into(), from));
        true
    }

    /// [`Self::take`] for a `len`-byte datagram read into `head` and on
    /// into `spill`: one longer than `head` is joined only when the DHT
    /// takes it.
    fn take_split(&self, head: &[u8], spill: &[u8], len: usize, from: SocketAddr) -> bool {
        let len = len.min(spill.len());
        if len <= head.len() {
            return self.take(&head[..len], from);
        }
        let Some(inbox) = self.inbox.get() else {
            return false;
        };
        let tail = &spill[..len - head.len()];
        if !(head.first() == Some(&b'd') && tail.last() == Some(&b'e')) {
            return false;
        }
        let mut datagram = Vec::with_capacity(len);
        datagram.extend_from_slice(head);
        datagram.extend_from_slice(tail);
        let _ = inbox.try_send((datagram.into(), from));
        true
    }
}

/// A uTP socket's datagram socket the DHT may share, from the uTP socket of
/// a session's peer listener.
pub(crate) struct DhtShare {
    pub(crate) socket: Arc<dyn DatagramSocket>,
    pub(crate) tap: Arc<DhtTap>,
    // The uTP threads the socket's I/O is registered on, kept running while
    // the DHT sends on it, after the uTP socket has ended too.
    pub(crate) shards: Option<weaver_utp::Shards>,
}

impl DhtShare {
    /// The port the shared socket is bound to.
    pub(crate) fn port(&self) -> u16 {
        self.socket.bind_addr().port()
    }

    /// The DHT's socket: it sends on the shared socket and receives the DHT
    /// messages uTP's receive loop hands it, until that loop ends.
    pub(crate) fn dht_socket(self) -> Arc<dyn DatagramSocket> {
        let (inbox, messages) = tokio::sync::mpsc::channel(DHT_INBOX);
        // The tap is the DHT's alone: it is set once, here.
        let _ = self.tap.inbox.set(inbox);
        drop(self.tap);
        Arc::new(SharedDhtSocket {
            socket: self.socket,
            messages: tokio::sync::Mutex::new(messages),
            _shards: self.shards,
        })
    }
}

/// The DHT's side of a socket it shares with uTP.
struct SharedDhtSocket {
    socket: Arc<dyn DatagramSocket>,
    messages: tokio::sync::Mutex<tokio::sync::mpsc::Receiver<(Box<[u8]>, SocketAddr)>>,
    // Dropped after `socket`: the threads its I/O is registered on.
    _shards: Option<weaver_utp::Shards>,
}

impl DatagramSocket for SharedDhtSocket {
    fn bind_addr(&self) -> SocketAddr {
        self.socket.bind_addr()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(async move {
            let Some((message, from)) = self.messages.lock().await.recv().await else {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "the uTP socket the DHT shares has closed",
                ));
            };
            let len = message.len().min(buf.len());
            buf[..len].copy_from_slice(&message[..len]);
            Ok((len, from))
        })
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        self.socket.send_to(buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_to(cx, buf, target)
    }

    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_to_vectored(cx, bufs, target)
    }

    fn link_mtu(&self) -> Option<u16> {
        self.socket.link_mtu()
    }

    fn spreads_streams(&self) -> bool {
        self.socket.spreads_streams()
    }

    fn send_run_most(&self) -> usize {
        self.socket.send_run_most()
    }

    fn poll_send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.socket.poll_send_run(cx, bytes, segment, target)
    }
}

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

    // Every send and receive of a kernel socket happens on the uTP
    // socket's one I/O task, as libtorrent makes them all on its network
    // thread; a tunnel's streams keep the sender of their own.
    fn sends_from_one_task(&self) -> bool {
        true
    }

    fn register_io(&self) -> io::Result<()> {
        self.0.register_io()
    }

    fn shutdown(&self) -> impl Future<Output = io::Result<()>> + Send {
        self.0.shutdown()
    }

    async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        loop {
            let (len, from) = self.0.recv_from(buf).await?;
            match &self.1 {
                Some(tap) if tap.take(&buf[..len.min(buf.len())], from) => {}
                _ => return Ok((len, from)),
            }
        }
    }

    // A kernel socket reads each datagram straight into the two buffers;
    // a tunnel's socket reads into `spill`, and the datagram's start moves
    // to `head`.
    async fn recv_from_split(
        &self,
        head: &mut [u8],
        spill: &mut [u8],
    ) -> io::Result<(usize, SocketAddr)> {
        loop {
            let split =
                std::future::poll_fn(|cx| match self.0.poll_recv_from_split(cx, head, spill) {
                    None => Poll::Ready(None),
                    Some(Poll::Ready(received)) => Poll::Ready(Some(received)),
                    Some(Poll::Pending) => Poll::Pending,
                })
                .await;
            let (len, from) = match split {
                Some(received) => received?,
                None => {
                    let (len, from) = self.0.recv_from(spill).await?;
                    let first = len.min(head.len());
                    head[..first].copy_from_slice(&spill[..first]);
                    spill.copy_within(first..len, 0);
                    (len, from)
                }
            };
            match &self.1 {
                Some(tap) if tap.take_split(head, spill, len, from) => {}
                _ => return Ok((len, from)),
            }
        }
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

    fn poll_send_to_vectored_fragmenting(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<io::Result<usize>> {
        self.0
            .poll_send_to_vectored_fragmenting(cx, bufs, target, fragmenting)
    }

    fn bind_addr(&self) -> SocketAddr {
        self.0.bind_addr()
    }

    fn link_mtu(&self) -> Option<u16> {
        self.0.link_mtu()
    }

    fn spreads_streams(&self) -> bool {
        self.0.spreads_streams()
    }

    fn send_run_most(&self) -> usize {
        self.0.send_run_most()
    }

    fn poll_send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_run(cx, bytes, segment, target)
    }

    fn send_many_most(&self) -> usize {
        self.0.send_many_most()
    }

    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<io::Result<usize>> {
        self.0.poll_send_many(cx, bytes, parts)
    }

    fn recv_many_most(&self) -> usize {
        self.0.recv_many_most()
    }

    // The datagrams of one call the DHT takes are handed to it; the rest
    // close up in their order.
    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        into: &mut [weaver_utp::Received],
        spill: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        loop {
            let got = std::task::ready!(self.0.poll_recv_many(cx, into, spill))?;
            let Some(tap) = &self.1 else {
                return Poll::Ready(Ok(got));
            };
            let mut kept = 0;
            for i in 0..got {
                let entry = &into[i];
                // One that reads as long as `spill` is not whole, and no
                // message for the DHT.
                if entry.len < spill.len()
                    && tap.take_split(&entry.head, spill, entry.len, entry.from)
                {
                    continue;
                }
                into.swap(kept, i);
                kept += 1;
            }
            if kept > 0 {
                return Poll::Ready(Ok(kept));
            }
        }
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

    /// A datagram socket at `addr`, at the kernel's defaults, reusing the
    /// address when `reuse`. One at the unspecified IPv6 address takes IPv4
    /// as well, unless it is one of a pair.
    fn kernel_udp(&self, addr: SocketAddr, reuse: bool) -> io::Result<KernelUdp> {
        #[expect(
            clippy::disallowed_methods,
            reason = "the kernel's own route binds on its device, or the host's"
        )]
        let (socket, dual_stack) = self.egress.bind_udp_reusing(addr, reuse)?;
        KernelUdp::registered(socket, dual_stack)
    }

    // A datagram socket at `addr` as `kernel_udp` binds it, set for uTP
    // with a receive buffer of `receive_buffer` bytes, and left for uTP to
    // register with the runtime its I/O runs on.
    fn kernel_utp(
        &self,
        addr: SocketAddr,
        reuse: bool,
        receive_buffer: usize,
    ) -> io::Result<KernelUdp> {
        #[expect(
            clippy::disallowed_methods,
            reason = "the kernel's own route binds on its device, or the host's"
        )]
        let (socket, dual_stack) = self.egress.bind_udp_reusing(addr, reuse)?;
        crate::udp_options::set_for_utp(&socket, addr, receive_buffer);
        KernelUdp::unregistered(socket, dual_stack)
    }

    // The datagram sockets at `addr`, each set for uTP with a receive buffer
    // of `utp` bytes when it holds one.
    fn datagrams(
        &self,
        addr: SocketAddr,
        utp: Option<usize>,
    ) -> io::Result<Arc<dyn DatagramSocket>> {
        let bind = |at, reuse| match utp {
            Some(receive_buffer) => self.kernel_utp(at, reuse, receive_buffer),
            None => self.kernel_udp(at, reuse),
        };
        if !self.paired(addr) {
            return Ok(Arc::new(bind(addr, reuses(addr))?));
        }
        let (v6, v4) = bind_pair(addr, bind, |socket| socket.local.port())?;
        Ok(Arc::new(PairedUdp {
            v6,
            v4,
            v4_first: Default::default(),
        }))
    }
}

// How many free IPv6 ports a pair tries before giving up on one IPv4 also
// has free, and how many free TCP ports a peer listener tries before giving
// up on one UDP also has free.
pub(crate) const PAIR_ATTEMPTS: usize = 16;

// Whether a socket at `addr` reuses the address: only at a port the caller
// names, which a socket bound before it may have left in use. A port the
// kernel chooses is bound without reuse, so it is one no socket of the
// family holds at any address. With reuse a BSD kernel checks only the exact
// address: it chooses a port a socket at a host address holds, and the
// datagrams and connections to that address go to the other socket.
pub(crate) fn reuses(addr: SocketAddr) -> bool {
    addr.port() != 0
}

// A socket for each family on one port: IPv6 alone at `addr`, then IPv4 on
// the port it took, both reusing the address as one at `addr` would. Port 0
// lets the kernel choose, and a port it chose for IPv6 that IPv4 holds, at
// any address, is given up for another; the IPv6 sockets given up stay bound
// until the pair is made, so the kernel never chooses one again.
fn bind_pair<T>(
    addr: SocketAddr,
    bind: impl Fn(SocketAddr, bool) -> io::Result<T>,
    port_of: impl Fn(&T) -> u16,
) -> io::Result<(T, T)> {
    let reuse = reuses(addr);
    let mut given_up = Vec::new();
    loop {
        let v6 = bind(addr, reuse)?;
        match bind(
            SocketAddr::from((Ipv4Addr::UNSPECIFIED, port_of(&v6))),
            reuse,
        ) {
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
            let listen = |at, reuse| -> io::Result<KernelListener> {
                #[expect(
                    clippy::disallowed_methods,
                    reason = "the kernel's own route listens on its device, or the host's"
                )]
                let listener =
                    tokio::net::TcpListener::from_std(self.egress.listen_tcp(at, reuse)?)?;
                Ok(KernelListener {
                    local: listener.local_addr()?,
                    listener,
                })
            };
            if !self.paired(addr) {
                return Ok(Box::new(listen(addr, reuses(addr))?) as Box<dyn StreamListener>);
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
    // Which family's socket is read first, in turn, so a busy one does not
    // starve the other.
    v4_first: std::sync::atomic::AtomicBool,
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
    fn bind_addr(&self) -> SocketAddr {
        self.v6.bind_addr()
    }

    fn register_io(&self) -> io::Result<()> {
        self.v6.register()?;
        self.v4.register()
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(async move {
            let (v6, v4) = (self.v6.io()?, self.v4.io()?);
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

    fn poll_recv_from_split(
        &self,
        cx: &mut Context<'_>,
        head: &mut [u8],
        spill: &mut [u8],
    ) -> Option<Poll<io::Result<(usize, SocketAddr)>>> {
        let v4_first = self
            .v4_first
            .fetch_xor(true, std::sync::atomic::Ordering::Relaxed);
        let order = if v4_first {
            [&self.v4, &self.v6]
        } else {
            [&self.v6, &self.v4]
        };
        for socket in order {
            if let Poll::Ready(received) = socket.poll_recv_split(cx, head, spill) {
                return Some(Poll::Ready(received));
            }
        }
        Some(Poll::Pending)
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

    fn poll_send_to_vectored_fragmenting(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<io::Result<usize>> {
        let (socket, target) = self.toward(target);
        socket.poll_send_to_vectored_fragmenting(cx, bufs, target, fragmenting)
    }

    fn spreads_streams(&self) -> bool {
        true
    }

    // The larger of the two families' answers: a run toward the family
    // whose socket sends one datagram a call goes out one at a time below.
    fn send_run_most(&self) -> usize {
        self.v6.run_most().max(self.v4.run_most())
    }

    fn poll_send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let (socket, target) = self.toward(target);
        if socket.run_most() == 1 {
            let first = &bytes[..segment.min(bytes.len())];
            return socket.poll_send_to(cx, first, target).map_ok(|_| 1);
        }
        socket.send_run(cx, bytes, segment, target)
    }

    fn send_many_most(&self) -> usize {
        self.v6.many_most(crate::udp_options::SEND_MANY_DATAGRAMS)
    }

    // The datagrams from the first on that go out on one family's socket.
    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<io::Result<usize>> {
        let (socket, _) = self.toward(parts[0].1);
        let same = parts
            .iter()
            .take_while(|(_, target)| std::ptr::eq(self.toward(*target).0, socket))
            .count();
        socket.send_many(cx, bytes, &parts[..same])
    }

    fn recv_many_most(&self) -> usize {
        self.v6.many_most(crate::udp_options::RECV_MANY_DATAGRAMS)
    }

    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        into: &mut [weaver_utp::Received],
        spill: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let v4_first = self
            .v4_first
            .fetch_xor(true, std::sync::atomic::Ordering::Relaxed);
        let order = if v4_first {
            [&self.v4, &self.v6]
        } else {
            [&self.v6, &self.v4]
        };
        for socket in order {
            if let Poll::Ready(received) = socket.recv_many(cx, into, spill) {
                return Poll::Ready(received);
            }
        }
        Poll::Pending
    }
}

/// A kernel datagram socket: the address it is bound at, and whether it
/// takes IPv4 as well as IPv6.
pub(crate) struct KernelUdp {
    // Registered with a runtime's reactor when it is made, or, for one uTP
    // runs over, by uTP with the runtime its I/O task runs on, once.
    socket: std::sync::OnceLock<tokio::net::UdpSocket>,
    // Bound and set, waiting for that registration.
    unregistered: parking_lot::Mutex<Option<std::net::UdpSocket>>,
    local: SocketAddr,
    dual_stack: bool,
    // Held by the sends that test a uTP path's size alone: a send that
    // allows fragmenting, while don't-fragment is off, and a size probe, so
    // it never goes out while it is off. Ordinary sends never take it.
    size_sends: parking_lot::Mutex<()>,
    // The host can send several datagrams in one call on this socket.
    runs: bool,
    // Calls for several datagrams that failed, one after another with none
    // sent between them. At RUN_FAILURES_MOST the socket sends each
    // datagram alone from then on: a device that cannot take such a send
    // fails every one of them.
    run_failures: std::sync::atomic::AtomicU8,
}

// See `KernelUdp::run_failures`.
const RUN_FAILURES_MOST: u8 = 8;

impl KernelUdp {
    // `socket`, registered with the runtime entered now.
    fn registered(socket: std::net::UdpSocket, dual_stack: bool) -> io::Result<Self> {
        let udp = Self::unregistered(socket, dual_stack)?;
        udp.register()?;
        Ok(udp)
    }

    // `socket`, bound and set, for [`Self::register`] to register later.
    fn unregistered(socket: std::net::UdpSocket, dual_stack: bool) -> io::Result<Self> {
        socket.set_nonblocking(true)?;
        Ok(KernelUdp {
            local: socket.local_addr()?,
            runs: crate::udp_options::sends_runs(&socket),
            run_failures: std::sync::atomic::AtomicU8::new(0),
            socket: std::sync::OnceLock::new(),
            unregistered: parking_lot::Mutex::new(Some(socket)),
            dual_stack,
            size_sends: parking_lot::Mutex::new(()),
        })
    }

    // Registers the socket with the runtime entered: nothing when it is
    // registered already. A registration that failed fails again: the
    // socket is gone.
    fn register(&self) -> io::Result<()> {
        if self.socket.get().is_some() {
            return Ok(());
        }
        let Some(socket) = self.unregistered.lock().take() else {
            return match self.socket.get() {
                Some(_) => Ok(()),
                None => Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "the socket's registration failed",
                )),
            };
        };
        let _ = self.socket.set(tokio::net::UdpSocket::from_std(socket)?);
        Ok(())
    }

    // The registered socket.
    fn io(&self) -> io::Result<&tokio::net::UdpSocket> {
        self.socket.get().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "the socket is not registered with a runtime",
            )
        })
    }

    // One datagram read into `head` and on into `spill`, in one system call
    // (recvmsg with two buffers), at most `spill.len()` bytes in all; one
    // cut short reads as `spill.len()` bytes. Its sender is named as
    // `recv_from` names it.
    fn poll_recv_split(
        &self,
        cx: &mut Context<'_>,
        head: &mut [u8],
        spill: &mut [u8],
    ) -> Poll<io::Result<(usize, SocketAddr)>> {
        let socket = self.io()?;
        let most = spill.len();
        let tail = most.saturating_sub(head.len());
        loop {
            std::task::ready!(socket.poll_recv_ready(cx))?;
            let received = socket.try_io(tokio::io::Interest::READABLE, || {
                let mut bufs = [
                    socket2::MaybeUninitSlice::new(as_uninit(head)),
                    socket2::MaybeUninitSlice::new(as_uninit(&mut spill[..tail])),
                ];
                socket2::SockRef::from(socket).recv_from_vectored(&mut bufs)
            });
            match received {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Poll::Ready(Err(error)),
                Ok((len, flags, from)) => {
                    let from = from.as_socket().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "a datagram from no IP address")
                    })?;
                    let len = if flags.is_truncated() { most } else { len };
                    return Poll::Ready(Ok((len, canonical_socket_addr(from))));
                }
            }
        }
    }

    // How many datagrams one call sends on this socket.
    fn run_most(&self) -> usize {
        use std::sync::atomic::Ordering::Relaxed;
        if self.runs && self.run_failures.load(Relaxed) < RUN_FAILURES_MOST {
            crate::udp_options::SEND_RUN_DATAGRAMS
        } else {
            1
        }
    }

    // Datagrams that lie end to end in `bytes`, each `segment` bytes but
    // the last, sent to `target` in one system call: as many from the
    // first as one call carries. Ready with how many went.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        use std::sync::atomic::Ordering::Relaxed;

        use crate::udp_options::{SEND_RUN_BYTES, SEND_RUN_DATAGRAMS};

        let Ok(size) = u16::try_from(segment) else {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        };
        if size == 0 {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        }
        let count = bytes
            .len()
            .div_ceil(segment)
            .min(SEND_RUN_DATAGRAMS)
            .min((SEND_RUN_BYTES / segment).max(1));
        let run = &bytes[..bytes.len().min(count * segment)];
        let target: socket2::SockAddr = self.toward(target).into();
        let socket = self.io()?;
        loop {
            std::task::ready!(socket.poll_send_ready(cx))?;
            match socket.try_io(tokio::io::Interest::WRITABLE, || {
                crate::udp_options::send_run(socket, run, size, &target)
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => {
                    let failures = self.run_failures.load(Relaxed);
                    self.run_failures.store(failures.saturating_add(1), Relaxed);
                    return Poll::Ready(Err(error));
                }
                Ok(_) => {
                    if self.run_failures.load(Relaxed) != 0 {
                        self.run_failures.store(0, Relaxed);
                    }
                    return Poll::Ready(Ok(count));
                }
            }
        }
    }

    // A host with no call for several datagrams sends the first alone.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let first = &bytes[..segment.min(bytes.len())];
        self.io()?
            .poll_send_to(cx, first, self.toward(target))
            .map_ok(|_| 1)
    }

    // `most` where the host has calls for several datagrams, else 1.
    fn many_most(&self, most: usize) -> usize {
        if crate::udp_options::TAKES_MANY {
            most
        } else {
            1
        }
    }

    // Datagrams that lie end to end in `bytes`, with each one's length and
    // address in `parts`, sent in one system call: as many from the first
    // as one call carries. Ready with how many went.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<io::Result<usize>> {
        use crate::udp_options::SEND_MANY_DATAGRAMS;

        let count = parts.len().min(SEND_MANY_DATAGRAMS);
        if count == 0 {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        }
        let mut named = [(0, self.local); SEND_MANY_DATAGRAMS];
        for (to, (len, target)) in named.iter_mut().zip(&parts[..count]) {
            *to = (*len, self.toward(*target));
        }
        let socket = self.io()?;
        loop {
            std::task::ready!(socket.poll_send_ready(cx))?;
            match socket.try_io(tokio::io::Interest::WRITABLE, || {
                crate::udp_options::send_many(socket, bytes, &named[..count])
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                sent => return Poll::Ready(sent),
            }
        }
    }

    // A host with no call for several datagrams sends the first alone.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<io::Result<usize>> {
        let Some((len, target)) = parts.first() else {
            return Poll::Ready(Err(io::ErrorKind::InvalidInput.into()));
        };
        self.io()?
            .poll_send_to(cx, &bytes[..*len], self.toward(*target))
            .map_ok(|_| 1)
    }

    // The datagrams that wait, read in one system call into `into`'s
    // entries from the first on, with `spill` shared among them, each
    // sender named as `recv_from` names it. Ready with how many: never
    // none.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    fn recv_many(
        &self,
        cx: &mut Context<'_>,
        into: &mut [weaver_utp::Received],
        spill: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let socket = self.io()?;
        loop {
            std::task::ready!(socket.poll_recv_ready(cx))?;
            match socket.try_io(tokio::io::Interest::READABLE, || {
                crate::udp_options::recv_many(socket, into, spill)
            }) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Poll::Ready(Err(error)),
                Ok(got) => {
                    for entry in &mut into[..got] {
                        entry.from = canonical_socket_addr(entry.from);
                    }
                    return Poll::Ready(Ok(got));
                }
            }
        }
    }

    // A host with no call for several datagrams is never asked for them.
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    fn recv_many(
        &self,
        _cx: &mut Context<'_>,
        _into: &mut [weaver_utp::Received],
        _spill: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::ErrorKind::Unsupported.into()))
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

/// A kernel datagram socket bound at `requested`, made for uTP to run over,
/// as [`SocketFactory::bind_utp`] asks: its datagrams never fragmented, its
/// receive buffer `receive_buffer` bytes as far as the host allows, and its
/// I/O registered by uTP with the runtime uTP reads and sends it on.
pub fn utp_datagrams(
    socket: std::net::UdpSocket,
    requested: SocketAddr,
    receive_buffer: usize,
) -> io::Result<Arc<dyn DatagramSocket>> {
    crate::udp_options::set_for_utp(&socket, requested, receive_buffer);
    let dual_stack = crate::udp_options::takes_ipv4(&socket);
    Ok(Arc::new(KernelUdp::unregistered(socket, dual_stack)?))
}

/// Datagrams the kernel sockets sent from slices, each in one system call.
#[cfg(test)]
pub(crate) static VECTORED_SENDS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

#[cfg(test)]
impl KernelSockets {
    /// A datagram socket at `addr`, bound at once, as the kernel socket it is.
    pub(crate) fn bind_kernel_udp(&self, addr: SocketAddr) -> io::Result<Arc<KernelUdp>> {
        self.kernel_udp(addr, reuses(addr)).map(Arc::new)
    }

    /// [`Self::bind_kernel_udp`], set for uTP with `receive_buffer`.
    pub(crate) fn bind_kernel_utp(
        &self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> io::Result<Arc<KernelUdp>> {
        let socket = self.kernel_utp(addr, reuses(addr), receive_buffer)?;
        socket.register()?;
        Ok(Arc::new(socket))
    }
}

#[cfg(test)]
impl KernelUdp {
    /// A datagram already waiting, without waiting for one.
    pub(crate) fn try_recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        self.io()?.try_recv_from(buf)
    }

    /// The kernel socket itself.
    pub(crate) fn socket(&self) -> &tokio::net::UdpSocket {
        self.io().expect("a registered socket")
    }
}

// `buf` as bytes a read may fill.
fn as_uninit(buf: &mut [u8]) -> &mut [std::mem::MaybeUninit<u8>] {
    // SAFETY: `MaybeUninit<u8>` has the layout of `u8`, and every byte of
    // `buf` is initialized; the only writer through the result is a socket
    // read, which writes initialized bytes, so `buf` stays initialized.
    unsafe { &mut *(std::ptr::from_mut::<[u8]>(buf) as *mut [std::mem::MaybeUninit<u8>]) }
}

impl DatagramSocket for KernelUdp {
    fn bind_addr(&self) -> SocketAddr {
        self.local
    }

    fn register_io(&self) -> io::Result<()> {
        self.register()
    }

    fn poll_recv_from_split(
        &self,
        cx: &mut Context<'_>,
        head: &mut [u8],
        spill: &mut [u8],
    ) -> Option<Poll<io::Result<(usize, SocketAddr)>>> {
        Some(self.poll_recv_split(cx, head, spill))
    }

    // A datagram from an IPv4 peer is from its IPv4 address, whichever
    // family's socket took it.
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(async move {
            let (len, from) = self.io()?.recv_from(buf).await?;
            Ok((len, canonical_socket_addr(from)))
        })
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(async move { self.io()?.send_to(buf, self.toward(target)).await })
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.io()?.poll_send_to(cx, buf, self.toward(target))
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
        let socket = self.io()?;
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

    fn spreads_streams(&self) -> bool {
        true
    }

    fn send_run_most(&self) -> usize {
        self.run_most()
    }

    fn send_many_most(&self) -> usize {
        self.many_most(crate::udp_options::SEND_MANY_DATAGRAMS)
    }

    fn poll_send_many(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        parts: &[(usize, SocketAddr)],
    ) -> Poll<io::Result<usize>> {
        self.send_many(cx, bytes, parts)
    }

    fn recv_many_most(&self) -> usize {
        self.many_most(crate::udp_options::RECV_MANY_DATAGRAMS)
    }

    fn poll_recv_many(
        &self,
        cx: &mut Context<'_>,
        into: &mut [weaver_utp::Received],
        spill: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.recv_many(cx, into, spill)
    }

    fn poll_send_run(
        &self,
        cx: &mut Context<'_>,
        bytes: &[u8],
        segment: usize,
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.send_run(cx, bytes, segment, target)
    }

    // One datagram from every slice, as the vectored send above, under the
    // socket's lock for sends that test the path's size. One that may be
    // fragmented goes with don't-fragment off for its family, switched on
    // again before the lock is released. An ordinary datagram another task
    // sends inside that window may go out without don't-fragment too; that
    // is harmless, as it is no larger than the size the path has proven,
    // and the DHT's and trackers' datagrams keep no such setting.
    fn poll_send_to_vectored_fragmenting(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<io::Result<usize>> {
        let family = match target {
            SocketAddr::V4(_) => crate::udp_options::Family::V4,
            SocketAddr::V6(v6) if v6.ip().to_ipv4_mapped().is_some() => {
                crate::udp_options::Family::V4
            }
            SocketAddr::V6(_) => crate::udp_options::Family::V6,
        };
        let target = self.toward(target);
        let socket = self.io()?;
        loop {
            std::task::ready!(socket.poll_send_ready(cx))?;
            let sent = {
                let _held = self.size_sends.lock();
                socket.try_io(tokio::io::Interest::WRITABLE, || {
                    let send =
                        || socket2::SockRef::from(socket).send_to_vectored(bufs, &target.into());
                    match fragmenting {
                        Fragmenting::Never => send(),
                        Fragmenting::Allowed => {
                            crate::udp_options::send_fragmenting(socket, family, send)
                        }
                    }
                })
            };
            match sent {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                sent => return Poll::Ready(sent),
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

    // A pair whose port the kernel chooses never shares it with a socket at
    // an IPv4 host address: the IPv4 bind is refused there, the pair takes
    // another port, and datagrams to IPv4 loopback on it reach the pair.
    #[tokio::test]
    async fn a_chosen_pair_port_a_socket_at_ipv4_loopback_holds_is_given_up() {
        let sockets = KernelSockets::new(None).unwrap();
        // Once the kernel chooses the IPv6 port, IPv4 loopback holds it: by
        // a socket here, or by one elsewhere when that bind is refused.
        let held = std::cell::RefCell::new(None);
        let (v6, v4) = super::bind_pair(
            (Ipv6Addr::LOCALHOST, 0).into(),
            |at, reuse| {
                let socket = sockets.kernel_udp(at, reuse)?;
                if at.is_ipv6() && held.borrow().is_none() {
                    let port = socket.local.port();
                    let holder = sockets.kernel_udp((Ipv4Addr::LOCALHOST, port).into(), true);
                    *held.borrow_mut() = Some((port, holder.ok()));
                }
                Ok(socket)
            },
            |socket| socket.local.port(),
        )
        .unwrap();
        let (held_port, _holder) = held.into_inner().unwrap();
        let port = v6.local.port();
        assert_eq!(v4.local.port(), port);
        assert_ne!(port, held_port);

        let peer = crate::test_support::loopback_udp().await;
        peer.send_to(b"in", (Ipv4Addr::LOCALHOST, port).into())
            .await
            .unwrap();
        let mut buf = [0u8; 8];
        let (len, from) = super::DatagramSocket::recv_from(&v4, &mut buf)
            .await
            .unwrap();
        assert_eq!((&buf[..len], from), (&b"in"[..], peer.bind_addr()));
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
        // Registered, as uTP registers it with the runtime it runs on.
        socket.register_io().unwrap();
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
    // returns an error. It keeps whether each bind reused the address.
    struct Host {
        bound: Rc<RefCell<BTreeSet<(bool, u16)>>>,
        v4_binds: Cell<usize>,
        v4: Box<dyn Fn(usize) -> Option<io::ErrorKind>>,
        reused: RefCell<Vec<bool>>,
    }

    impl Host {
        fn new(v4: impl Fn(usize) -> Option<io::ErrorKind> + 'static) -> Self {
            Self {
                bound: Rc::default(),
                v4_binds: Cell::new(0),
                v4: Box::new(v4),
                reused: RefCell::default(),
            }
        }

        fn bind(&self, at: SocketAddr, reuse: bool) -> io::Result<Held> {
            self.reused.borrow_mut().push(reuse);
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
                |at, reuse| self.bind(at, reuse),
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
        // A port the kernel chooses is bound without reuse, the IPv4 socket
        // on it included, so a port IPv4 holds at any address refuses it.
        assert_eq!(*host.reused.borrow(), [false; 6]);
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
        // A port asked for is bound reusing the address, on both families.
        assert_eq!(*host.reused.borrow(), [true, true]);
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
            v6: sockets
                .kernel_udp((Ipv6Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4: sockets
                .kernel_udp((Ipv4Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4_first: Default::default(),
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

    // A socket that keeps the trait's own send-run methods takes one
    // datagram a call: asked for a run, it sends the first alone and says
    // one went.
    #[tokio::test]
    async fn a_socket_with_no_run_call_sends_the_first_datagram_alone() {
        use std::task::{Context, Poll};

        use std::sync::Arc;

        use super::{BoxFuture, KernelUdp};

        struct Plain(Arc<KernelUdp>);
        impl DatagramSocket for Plain {
            fn bind_addr(&self) -> SocketAddr {
                self.0.bind_addr()
            }
            fn recv_from<'a>(
                &'a self,
                buf: &'a mut [u8],
            ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
                self.0.recv_from(buf)
            }
            fn send_to<'a>(
                &'a self,
                buf: &'a [u8],
                target: SocketAddr,
            ) -> BoxFuture<'a, io::Result<usize>> {
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
        }

        let sockets = KernelSockets::new(None).unwrap();
        let plain = Plain(
            sockets
                .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
                .unwrap(),
        );
        let peer = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let socket: &dyn DatagramSocket = &plain;
        assert_eq!(socket.send_run_most(), 1);
        let mut bytes = vec![1u8; 100];
        bytes.extend_from_slice(&[2u8; 40]);
        let sent =
            std::future::poll_fn(|cx| socket.poll_send_run(cx, &bytes, 100, peer.bind_addr()))
                .await
                .unwrap();
        assert_eq!(sent, 1);
        // The second goes with a call of its own and arrives second: had
        // the first call carried it too, it would arrive twice.
        socket.send_to(&[3u8; 7], peer.bind_addr()).await.unwrap();
        let mut buf = [0u8; 2048];
        for (byte, len) in [(1u8, 100), (3, 7)] {
            let (got, _) = peer.recv_from(&mut buf).await.unwrap();
            assert_eq!(got, len);
            assert!(buf[..got].iter().all(|b| *b == byte));
        }
    }

    // A pair of sockets sends a run on the socket of the target's family.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_paired_socket_sends_a_run_on_the_targets_family() {
        let sockets = KernelSockets::new(None).unwrap();
        let paired = PairedUdp {
            v6: sockets
                .kernel_udp((Ipv6Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4: sockets
                .kernel_udp((Ipv4Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4_first: Default::default(),
        };
        assert_eq!(
            paired.send_run_most(),
            crate::udp_options::SEND_RUN_DATAGRAMS
        );
        let peer = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let mapped = SocketAddr::from((
            Ipv4Addr::LOCALHOST.to_ipv6_mapped(),
            peer.bind_addr().port(),
        ));
        let mut bytes = vec![1u8; 100];
        bytes.extend_from_slice(&[2u8; 100]);
        bytes.extend_from_slice(&[3u8; 40]);
        let sent = std::future::poll_fn(|cx| paired.poll_send_run(cx, &bytes, 100, mapped))
            .await
            .unwrap();
        assert_eq!(sent, 3);
        let mut buf = [0u8; 2048];
        for (byte, len) in [(1u8, 100), (2, 100), (3, 40)] {
            let (got, from) = peer.recv_from(&mut buf).await.unwrap();
            assert_eq!(from, paired.v4.bind_addr());
            assert_eq!(got, len);
            assert!(buf[..got].iter().all(|b| *b == byte));
        }
    }

    // One call sends datagrams that lie end to end: the peer receives each
    // as its own datagram, the short last one too, in order, on both
    // families of a socket that takes both.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_kernel_socket_sends_a_run_of_datagrams_in_one_call() {
        let sockets = KernelSockets::new(None).unwrap();
        for (bind, peer) in [
            (
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            ),
            (
                SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
                SocketAddr::from((Ipv6Addr::LOCALHOST, 0)),
            ),
        ] {
            let sender = sockets.kernel_udp(bind, false).unwrap();
            assert_eq!(
                sender.send_run_most(),
                crate::udp_options::SEND_RUN_DATAGRAMS
            );
            let peer = sockets.bind_kernel_udp(peer).unwrap();
            let mut bytes = Vec::new();
            for byte in 1..=4u8 {
                bytes.extend_from_slice(&[byte; 100]);
            }
            bytes.extend_from_slice(&[5u8; 40]);
            let sent =
                std::future::poll_fn(|cx| sender.poll_send_run(cx, &bytes, 100, peer.bind_addr()))
                    .await
                    .unwrap();
            assert_eq!(sent, 5);
            let mut buf = [0u8; 2048];
            for (byte, len) in [(1u8, 100), (2, 100), (3, 100), (4, 100), (5, 40)] {
                let (got, from) = peer.recv_from(&mut buf).await.unwrap();
                assert_eq!(from, sender.bind_addr());
                assert_eq!(got, len);
                assert!(buf[..got].iter().all(|b| *b == byte));
            }
        }
    }

    // One call carries no more than the kernel takes: a longer run is sent
    // from the first datagram and the count says how far it went.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_run_longer_than_one_call_carries_is_sent_in_part() {
        use crate::udp_options::{SEND_RUN_BYTES, SEND_RUN_DATAGRAMS};

        let sockets = KernelSockets::new(None).unwrap();
        let sender = sockets
            .kernel_udp((Ipv4Addr::LOCALHOST, 0).into(), false)
            .unwrap();
        let peer = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let send = |bytes: Vec<u8>, segment| {
            let (sender, to) = (&sender, peer.bind_addr());
            async move {
                std::future::poll_fn(|cx| sender.poll_send_run(cx, &bytes, segment, to))
                    .await
                    .unwrap()
            }
        };
        // More datagrams than a call takes.
        assert_eq!(send(vec![0u8; 20 * 70], 20).await, SEND_RUN_DATAGRAMS);
        // More bytes than a call takes.
        assert_eq!(
            send(vec![0u8; 1400 * 60], 1400).await,
            SEND_RUN_BYTES / 1400
        );
    }

    // A kernel socket reads a datagram straight into the two buffers: a
    // short one stays in the first, a long one runs on into the second, and
    // a long DHT message on a shared socket is joined for the DHT. Both
    // families of a pair are read.
    #[tokio::test]
    async fn a_kernel_socket_reads_a_datagram_into_two_buffers() {
        use weaver_utp::Transport;

        let sockets = KernelSockets::new(None).unwrap();
        let paired = std::sync::Arc::new(PairedUdp {
            v6: sockets
                .kernel_udp((Ipv6Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4: sockets
                .kernel_udp((Ipv4Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4_first: Default::default(),
        });
        let tap = std::sync::Arc::new(super::DhtTap::default());
        let share = super::DhtShare {
            socket: paired.clone(),
            tap: tap.clone(),
            shards: None,
        };
        let dht = share.dht_socket();
        let transport = super::DatagramTransport(paired.clone(), Some(tap));
        let peer4 = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let peer6 = sockets
            .bind_kernel_udp((Ipv6Addr::LOCALHOST, 0).into())
            .unwrap();
        let (mut head, mut spill) = (vec![0u8; 2048], vec![0u8; usize::from(u16::MAX)]);

        let short = vec![1u8; 100];
        peer4.send_to(&short, paired.v4.bind_addr()).await.unwrap();
        let (len, from) = transport
            .recv_from_split(&mut head, &mut spill)
            .await
            .unwrap();
        assert_eq!((len, from), (100, peer4.bind_addr()));
        assert_eq!(&head[..100], &short[..]);

        let long: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        peer6.send_to(&long, paired.v6.bind_addr()).await.unwrap();
        let (len, from) = transport
            .recv_from_split(&mut head, &mut spill)
            .await
            .unwrap();
        assert_eq!((len, from), (5000, peer6.bind_addr()));
        assert_eq!(&head[..], &long[..2048]);
        assert_eq!(&spill[..5000 - 2048], &long[2048..]);

        // A DHT message longer than the first buffer goes to the DHT whole,
        // and the next datagram to uTP.
        let mut message = vec![b'x'; 3000];
        message[0] = b'd';
        message[2999] = b'e';
        peer4
            .send_to(&message, paired.v4.bind_addr())
            .await
            .unwrap();
        peer4.send_to(&short, paired.v4.bind_addr()).await.unwrap();
        let (len, _) = transport
            .recv_from_split(&mut head, &mut spill)
            .await
            .unwrap();
        assert_eq!(len, 100);
        let mut got = vec![0u8; 4000];
        let (len, from) = dht.recv_from(&mut got).await.unwrap();
        assert_eq!((&got[..len], from), (&message[..], peer4.bind_addr()));
    }

    // One call reads the datagrams that wait into an entry each, with each
    // one's sender: one from an IPv4 peer is named by its IPv4 address, and
    // a DHT message among them goes to the DHT while the rest close up in
    // their order. The rest of a long datagram is read into the buffer the
    // entries share, and a long DHT message is joined for the DHT. Of two
    // long datagrams the last is whole, and the first is whole or reads as
    // long as the shared buffer.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_kernel_socket_reads_several_datagrams_a_call() {
        use weaver_utp::{Received, Transport};

        let sockets = KernelSockets::new(None).unwrap();
        let socket = std::sync::Arc::new(
            sockets
                .kernel_udp((Ipv6Addr::UNSPECIFIED, 0).into(), false)
                .unwrap(),
        );
        let port = socket.bind_addr().port();
        let tap = std::sync::Arc::new(super::DhtTap::default());
        let share = super::DhtShare {
            socket: socket.clone(),
            tap: tap.clone(),
            shards: None,
        };
        let dht = share.dht_socket();
        let transport = super::DatagramTransport(socket.clone(), Some(tap));
        assert_eq!(
            transport.recv_many_most(),
            crate::udp_options::RECV_MANY_DATAGRAMS
        );
        let peer4 = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let peer6 = sockets
            .bind_kernel_udp((Ipv6Addr::LOCALHOST, 0).into())
            .unwrap();
        let to4: SocketAddr = (Ipv4Addr::LOCALHOST, port).into();
        let to6: SocketAddr = (Ipv6Addr::LOCALHOST, port).into();

        let mut into: Vec<Received> = (0..8)
            .map(|_| Received {
                head: vec![0u8; 2048].into(),
                len: 0,
                from: socket.bind_addr(),
            })
            .collect();
        let mut spill = vec![0u8; usize::from(u16::MAX)];
        // The next `want` datagrams for uTP: each one's bytes, or none for
        // one that reads as long as the shared buffer, and its sender.
        // What is in the shared buffer is read before the next call.
        async fn read(
            transport: &impl Transport,
            into: &mut [Received],
            spill: &mut [u8],
            want: usize,
        ) -> Vec<(Option<Vec<u8>>, SocketAddr)> {
            let mut got = Vec::new();
            while got.len() < want {
                let count = std::future::poll_fn(|cx| transport.poll_recv_many(cx, into, spill))
                    .await
                    .unwrap();
                assert!(count > 0);
                for entry in &into[..count] {
                    let bytes = (entry.len < spill.len()).then(|| {
                        let mut bytes = entry.head[..entry.len.min(2048)].to_vec();
                        bytes.extend_from_slice(&spill[..entry.len.saturating_sub(2048)]);
                        bytes
                    });
                    got.push((bytes, entry.from));
                }
            }
            got
        }

        let short = vec![1u8; 100];
        let long: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let mut message = vec![b'x'; 3000];
        message[0] = b'd';
        message[2999] = b'e';
        let last = vec![7u8; 20];
        peer4.send_to(&short, to4).await.unwrap();
        peer6.send_to(&long, to6).await.unwrap();
        peer6.send_to(&last, to6).await.unwrap();
        assert_eq!(
            read(&transport, &mut into, &mut spill, 3).await,
            [
                (Some(short.clone()), peer4.bind_addr()),
                (Some(long.clone()), peer6.bind_addr()),
                (Some(last.clone()), peer6.bind_addr()),
            ]
        );

        // A long DHT message between two short datagrams.
        peer6.send_to(&last, to6).await.unwrap();
        peer4.send_to(&message, to4).await.unwrap();
        peer4.send_to(&short, to4).await.unwrap();
        assert_eq!(
            read(&transport, &mut into, &mut spill, 2).await,
            [
                (Some(last), peer6.bind_addr()),
                (Some(short), peer4.bind_addr()),
            ]
        );
        let mut taken = vec![0u8; 4000];
        let (len, from) = dht.recv_from(&mut taken).await.unwrap();
        assert_eq!((&taken[..len], from), (&message[..], peer4.bind_addr()));

        // Two long datagrams.
        let other: Vec<u8> = (0..4000u32).map(|i| (i % 241) as u8).collect();
        peer6.send_to(&long, to6).await.unwrap();
        peer4.send_to(&other, to4).await.unwrap();
        let got = read(&transport, &mut into, &mut spill, 2).await;
        assert!(got[0] == (Some(long), peer6.bind_addr()) || got[0] == (None, peer6.bind_addr()));
        assert_eq!(got[1], (Some(other), peer4.bind_addr()));
    }

    // One call sends datagrams of several lengths to several addresses, and
    // each arrives whole at its own. A pair sends the leading datagrams
    // that go out on one family's socket.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn a_kernel_socket_sends_datagrams_to_several_addresses_a_call() {
        let sockets = KernelSockets::new(None).unwrap();
        let paired = PairedUdp {
            v6: sockets
                .kernel_udp((Ipv6Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4: sockets
                .kernel_udp((Ipv4Addr::LOCALHOST, 0).into(), false)
                .unwrap(),
            v4_first: Default::default(),
        };
        assert_eq!(
            paired.send_many_most(),
            crate::udp_options::SEND_MANY_DATAGRAMS
        );
        let one = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let other = sockets
            .bind_kernel_udp((Ipv4Addr::LOCALHOST, 0).into())
            .unwrap();
        let sixth = sockets
            .bind_kernel_udp((Ipv6Addr::LOCALHOST, 0).into())
            .unwrap();

        let mut bytes = vec![1u8; 20];
        bytes.extend_from_slice(&[2u8; 1400]);
        bytes.extend_from_slice(&[3u8; 33]);
        bytes.extend_from_slice(&[4u8; 50]);
        let parts = [
            (20, one.bind_addr()),
            (1400, other.bind_addr()),
            (33, one.bind_addr()),
            (50, sixth.bind_addr()),
        ];
        let sent = std::future::poll_fn(|cx| paired.poll_send_many(cx, &bytes, &parts))
            .await
            .unwrap();
        assert_eq!(sent, 3);
        let sent =
            std::future::poll_fn(|cx| paired.poll_send_many(cx, &bytes[1453..], &parts[3..]))
                .await
                .unwrap();
        assert_eq!(sent, 1);

        let mut buf = vec![0u8; 2000];
        let (len, from) = one.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&[1u8; 20][..], paired.v4.bind_addr()));
        let (len, _) = one.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..len], &[3u8; 33][..]);
        let (len, from) = other.recv_from(&mut buf).await.unwrap();
        assert_eq!(
            (&buf[..len], from),
            (&[2u8; 1400][..], paired.v4.bind_addr())
        );
        let (len, from) = sixth.recv_from(&mut buf).await.unwrap();
        assert_eq!((&buf[..len], from), (&[4u8; 50][..], paired.v6.bind_addr()));
    }
}
