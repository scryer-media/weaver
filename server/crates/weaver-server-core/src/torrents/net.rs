// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The sockets a torrent session uses, for the one leg of its route.
//!
//! A leg's path decides whether the session dials with its own kernel
//! sockets or through the leg's dialer. Whether it may use UDP at all (DHT,
//! UDP trackers) and whether it can listen are the dialer's to say: a ladder
//! leg listens inside its tunnel when it ends in a WireGuard hop.

use std::future::Future;
use std::io::{self, IoSliceMut};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, ReadBuf};
use weaver_torrent::network::{
    AsyncReadVectored, AsyncReadVectoredIntoCompat, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture,
    DatagramSocket, Resolved, Resolver,
};
use weaver_torrent::upnp::{PortMapReceiver, PortMapped, PortmapProtocol, PortmapTransport};
use weaver_torrent::{
    Egress, HttpClients, HttpTarget, Session, SocketFactory, StreamListener, route_to,
};
use weaver_tunnel::bridge::{Bridge, ConnectionOutcome};
use weaver_tunnel::endpoint::{DatagramSocket as TunnelSocket, DatagramTransport};
use weaver_tunnel::pipe::{
    DialedStream, Dialer, InboundListener as Listening, Purpose, Resolution, Target,
};

use crate::proxies::{EgressBinding, LegPath, ResolvedBy};

/// How a leg reaches peers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LegKind {
    /// The host's own sockets.
    System,
    /// Kernel sockets bound to a device.
    Interface(String),
    /// Kernel sockets bound to a source address.
    SourceAddress(IpAddr),
    /// Through a proxy or tunnel rung: stream connections, and datagrams
    /// when the rung carries them.
    Ladder,
}

impl LegKind {
    pub fn of(binding: &EgressBinding, path: &LegPath) -> Self {
        match (path, binding) {
            (LegPath::Ladder { .. }, _) => Self::Ladder,
            (LegPath::Direct, EgressBinding::System) => Self::System,
            (LegPath::Direct, EgressBinding::Interface { name }) => Self::Interface(name.clone()),
            (LegPath::Direct, EgressBinding::SourceAddress { address }) => {
                Self::SourceAddress(*address)
            }
        }
    }

    /// Whether a session on this leg opens kernel sockets itself.
    pub fn own_sockets(&self) -> bool {
        matches!(self, Self::System | Self::Interface(_))
    }

    /// What a session on this leg may do over `dialer`, the leg's dialer
    /// when its sockets come from one. A ladder leg carries datagrams, and so
    /// DHT, exactly when its dialer does.
    pub fn shape(&self, dialer: Option<&Arc<dyn Dialer>>) -> Shape {
        match self {
            Self::System => Shape {
                own_sockets: true,
                dht: true,
                upnp: true,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::Interface(_) => Shape {
                own_sockets: true,
                dht: true,
                upnp: false,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::SourceAddress(_) => Shape {
                own_sockets: false,
                dht: true,
                upnp: false,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::Ladder => {
                let datagrams = dialer.is_some_and(|dialer| carrier(dialer).is_some());
                Shape {
                    own_sockets: false,
                    dht: datagrams,
                    upnp: false,
                    listen: dialer.is_some_and(|dialer| Arc::clone(dialer).inbound().is_some()),
                    utp: false,
                    datagrams,
                }
            }
        }
    }

    pub fn bind_device(&self) -> Option<&str> {
        match self {
            Self::Interface(name) => Some(name),
            _ => None,
        }
    }

    /// The address the leg's listener and datagram socket bind, when it
    /// pins one.
    pub fn listen_ip(&self) -> Option<IpAddr> {
        match self {
            Self::SourceAddress(address) => Some(*address),
            _ => None,
        }
    }
}

/// The capabilities of a session on one leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    /// The session opens kernel sockets itself; otherwise every socket comes
    /// from the leg's factory.
    pub own_sockets: bool,
    pub dht: bool,
    pub upnp: bool,
    /// Incoming peer connections.
    pub listen: bool,
    /// uTP beside TCP on the listener's port. A tunnel's datagram socket
    /// cannot be polled for a send, which uTP needs.
    pub utp: bool,
    /// UDP of any kind: UDP trackers, uTP and DHT.
    pub datagrams: bool,
}

// The datagrams `dialer` carries for the session's UDP.
fn carrier(dialer: &Arc<dyn Dialer>) -> Option<Arc<dyn DatagramTransport>> {
    Arc::clone(dialer).datagrams()
}

/// Peer sockets through a leg's dialer; a leg that pins a source address
/// also listens and carries UDP on it, and any other leg carries UDP through
/// its dialer when the dialer carries datagrams.
pub struct LegSockets {
    dialer: Arc<dyn Dialer>,
    source: Option<IpAddr>,
}

impl LegSockets {
    pub fn new(dialer: Arc<dyn Dialer>, kind: &LegKind) -> Self {
        Self {
            dialer,
            source: kind.listen_ip(),
        }
    }

    // A listener on `addr`'s port: on the pinned source address, else inside
    // the leg's tunnel, where the `forwarded` one takes the port the tunnel's
    // provider forwards.
    fn listen<'a>(
        &'a self,
        addr: SocketAddr,
        forwarded: bool,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        if let Some(ip) = self.source {
            return Box::pin(async move {
                let listener =
                    tokio::net::TcpListener::bind(SocketAddr::new(ip, addr.port())).await?;
                Ok(Box::new(Listener(listener)) as Box<dyn StreamListener>)
            });
        }
        let Some(inbound) = Arc::clone(&self.dialer).inbound() else {
            return Box::pin(async move {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "this leg cannot listen",
                ))
            });
        };
        shared(async move {
            let listener = inbound
                .listen(addr.port(), forwarded)
                .await
                .map_err(io::Error::other)?;
            Ok(Box::new(Arriving(listener)) as Box<dyn StreamListener>)
        })
    }

    // The datagrams the session's UDP rides, when not the source address.
    fn carrier(&self) -> Option<Arc<dyn DatagramTransport>> {
        self.source
            .is_none()
            .then(|| carrier(&self.dialer))
            .flatten()
    }
}

// A future the session can share between threads. The session's futures
// must be Sync and a tunnel socket's are not; nothing reaches this one but
// its poll, which holds it exclusively.
fn shared<'a, T>(future: impl Future<Output = T> + Send + 'a) -> BoxFuture<'a, T> {
    let mut future = Mutex::new(Box::pin(future));
    Box::pin(std::future::poll_fn(move |cx| {
        future
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .poll(cx)
    }))
}

// A peer connection's read half through a leg's dialer. The leg counts the
// bytes it reads, as it counts those of every connection it carries.
struct Counted {
    read: BoxAsyncReadVectored,
    outcome: Arc<ConnectionOutcome>,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.read).poll_read(cx, buf);
        self.outcome.read(buf.filled().len() - before);
        polled
    }
}

impl AsyncReadVectored for Counted {
    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        vec: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.read).poll_read_vectored(cx, vec);
        if let Poll::Ready(Ok(read)) = polled {
            self.outcome.read(read);
        }
        polled
    }
}

fn halves(stream: DialedStream) -> (BoxAsyncReadVectored, BoxAsyncWrite) {
    match stream {
        DialedStream::Socket(socket) => {
            let (read, write) = socket.into_split();
            (Box::new(read), Box::new(write))
        }
        DialedStream::Tunnel(stream) => {
            let (read, write) = tokio::io::split(stream);
            (Box::new(read.into_vectored_compat()), Box::new(write))
        }
    }
}

impl SocketFactory for LegSockets {
    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let target = Target {
                host: addr.ip().to_string(),
                port: addr.port(),
                purpose: Purpose::Torrent,
                addresses: vec![addr.ip()],
            };
            // The dial runs as a task of its own: its future is not Sync,
            // and the session's are.
            let dialer = Arc::clone(&self.dialer);
            let mut dial = Abort(tokio::spawn(async move { dialer.dial(&target).await }));
            let dialed = (&mut dial.0)
                .await
                .map_err(io::Error::other)?
                .map_err(|error| io::Error::other(error.to_string()))?;
            let (read, write) = halves(dialed.stream);
            let read = Counted {
                read,
                outcome: dialed.outcome,
            };
            Ok((Box::new(read) as BoxAsyncReadVectored, write))
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listen(addr, false)
    }

    fn listen_peers_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listen(addr, true)
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        if let Some(ip) = self.source {
            return Box::pin(async move {
                Ok(
                    Arc::new(tokio::net::UdpSocket::bind(SocketAddr::new(ip, addr.port())).await?)
                        as Arc<dyn DatagramSocket>,
                )
            });
        }
        let Some(carrier) = self.carrier() else {
            return Box::pin(async move {
                Ok(Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>)
            });
        };
        shared(async move {
            let socket = carrier.bind(addr).await.map_err(io::Error::other)?;
            Ok(Arc::new(Carried {
                bind_addr: socket.local_addr().unwrap_or(addr),
                socket,
            }) as Arc<dyn DatagramSocket>)
        })
    }
}

// A socket in the leg's tunnel, as the session's datagram socket.
struct Carried {
    socket: Arc<dyn TunnelSocket>,
    bind_addr: SocketAddr,
}

impl DatagramSocket for Carried {
    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        shared(self.socket.recv_from(buf))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        shared(async move {
            self.socket.send_to(buf, target).await?;
            Ok(buf.len())
        })
    }

    // The proxy's UDP header: reserved, fragment and address type, the
    // target's address and its port.
    fn datagram_overhead(&self, target: SocketAddr) -> usize {
        let address = if target.is_ipv4() { 4 } else { 16 };
        6 + address
    }

    // Only uTP polls a send, and a ladder leg runs no uTP.
    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
        _target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a tunnel's datagrams carry no uTP",
        )))
    }
}

/// The names a session resolves on its leg: who resolves each is the leg's
/// naming, as for every other dial on the leg. A name the leg hands to its
/// proxy resolves at the exit, and no lookup is made here.
pub struct LegNames(pub Arc<dyn Dialer>);

impl Resolver for LegNames {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        shared(async move {
            let resolved = self.0.resolve(host).await;
            Ok(
                match resolved.map_err(|error| io::Error::other(error.to_string()))? {
                    Resolution::Addresses(addresses) => Resolved::Addresses(
                        addresses
                            .into_iter()
                            .map(|ip| SocketAddr::new(ip, port))
                            .collect(),
                    ),
                    Resolution::Unresolved => Resolved::AtExit,
                },
            )
        })
    }
}

// A task that ends when its handle is dropped.
struct Abort<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Abort<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct Listener(pub(crate) tokio::net::TcpListener);

impl StreamListener for Listener {
    fn local_addr(&self) -> SocketAddr {
        self.0
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
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

// A listener inside the leg's tunnel. The leg counts the bytes each
// connection it accepts reads, as it counts those of the ones it dials.
struct Arriving(Box<dyn Listening>);

impl StreamListener for Arriving {
    fn local_addr(&self) -> SocketAddr {
        self.0.local_addr()
    }

    // Each word of the leg's gateway on the port it forwards, as a NAT-PMP
    // mapper's TCP report, until the listener goes.
    fn port_mappings(&self) -> Option<PortMapReceiver> {
        let mut mapping = self.0.mapping()?;
        let (reports, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while mapping.changed().await.is_ok() {
                let now = mapping.borrow_and_update().clone();
                let report = PortMapped {
                    transport: PortmapTransport::NatPmp,
                    protocol: PortmapProtocol::Tcp,
                    local_port: now.external_port.unwrap_or(0),
                    external_port: now.external_port,
                    external_ip: now.external_ip,
                    error: now.error,
                };
                if reports.send(report).is_err() {
                    return;
                }
            }
        });
        Some(received)
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        shared(async move {
            let accepted = self.0.accept().await.map_err(io::Error::other)?;
            let peer = accepted
                .peer
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
            let (read, write) = halves(accepted.stream);
            let read = Counted {
                read,
                outcome: accepted.outcome,
            };
            Ok((peer, Box::new(read) as BoxAsyncReadVectored, write))
        })
    }
}

/// The datagram socket of a leg that carries no UDP: nothing arrives and
/// every send is refused, so UDP trackers fail on their own.
pub struct NoDatagrams(SocketAddr);

impl NoDatagrams {
    pub fn new(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

fn no_udp() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "this leg carries no UDP")
}

impl DatagramSocket for NoDatagrams {
    fn bind_addr(&self) -> SocketAddr {
        self.0
    }

    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(std::future::pending())
    }

    fn send_to<'a>(
        &'a self,
        _buf: &'a [u8],
        _target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(async { Err(no_udp()) })
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
        _target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Err(no_udp()))
    }
}

/// The HTTP clients of a session on a leg: the one for no target downloads
/// metainfo, and a tracker or web seed is asked through one held to the
/// addresses its host was checked at. A ladder leg's clients ride `bridge`,
/// the leg's own. They name no client: each request names the client of
/// the torrent it is for.
pub fn tracker_client(
    kind: &LegKind,
    bridge: Option<Arc<Bridge>>,
) -> Result<Arc<dyn HttpClients>, String> {
    let bridge = match (kind, bridge) {
        (LegKind::Ladder, None) => return Err("a ladder leg needs its bridge".into()),
        (LegKind::Ladder, bridge) => bridge,
        _ => None,
    };
    let base = {
        let kind = kind.clone();
        move || {
            let builder = reqwest::Client::builder();
            Ok(match &kind {
                LegKind::Ladder | LegKind::System => builder,
                LegKind::SourceAddress(address) => builder.local_address(*address),
                LegKind::Interface(name) => bound_to(builder, name)?,
            })
        }
    };
    let clients = LegClients {
        base,
        bridge,
        kind: kind.clone(),
        trackers: Mutex::default(),
    };
    // A leg whose clients cannot be built does not open.
    let _ = clients.builder(None).map_err(|error| error.to_string())?;
    Ok(Arc::new(clients))
}

/// A session's HTTP clients: each from `base`, through `bridge` on a leg
/// that has one, leaving as the leg's `kind` does.
pub(crate) struct LegClients<F> {
    pub(crate) base: F,
    pub(crate) bridge: Option<Arc<Bridge>>,
    pub(crate) kind: LegKind,
    // The tracker clients built, by target and header case, so an announce
    // to a tracker the session announced to before builds no client (its
    // TLS roots, its connector) again.
    pub(crate) trackers: Mutex<std::collections::HashMap<TrackerTarget, reqwest::Client>>,
}

// A tracker client's target: its host, port and checked addresses, and
// whether it writes header names capitalised.
pub(crate) type TrackerTarget = (String, u16, Vec<IpAddr>, bool);

// The most tracker clients a session keeps; past it they are built again.
const TRACKER_CLIENTS_KEPT: usize = 1024;

impl<F> HttpClients for LegClients<F>
where
    F: Fn() -> Result<reqwest::ClientBuilder, String> + Send + Sync + 'static,
{
    // A client for a target dials the host at an address it was checked
    // at, and follows no redirect. Through the leg's bridge, the bridge is
    // handed that address, so the leg dials exactly it; with none, the
    // name, which the leg's exit resolves.
    fn builder(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::ClientBuilder> {
        let builder = (self.base)().map_err(anyhow::Error::msg)?;
        let (host, port, addrs) = target.map_or(("", 0, &[][..]), |target| {
            (target.host, target.port, target.addrs)
        });
        let builder = match &self.bridge {
            Some(bridge) => crate::proxies::through_bridge_at(builder, bridge, host, port, addrs)
                .map_err(anyhow::Error::msg)?,
            None => crate::proxies::pinned(builder, host, port, addrs),
        };
        Ok(match target {
            None => builder,
            Some(_) => builder.redirect(reqwest::redirect::Policy::none()),
        })
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a torrent session's client, built for its leg"
    )]
    fn client(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::Client> {
        Ok(self.builder(target)?.build()?)
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "a torrent session's client, built for its leg"
    )]
    fn title_case_client(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::Client> {
        Ok(self.builder(target)?.http1_title_case_headers().build()?)
    }

    // One client per target, kept: it keeps no connection idle, so each
    // announce dials and closes as a client built for it and dropped did.
    #[expect(
        clippy::disallowed_methods,
        reason = "a torrent session's client, built for its leg"
    )]
    fn tracker_client(
        &self,
        target: HttpTarget<'_>,
        title_case: bool,
    ) -> anyhow::Result<reqwest::Client> {
        let key = (
            target.host.to_owned(),
            target.port,
            target.addrs.to_vec(),
            title_case,
        );
        if let Some(client) = self
            .trackers
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
        {
            return Ok(client.clone());
        }
        let builder = self.builder(Some(target))?.pool_max_idle_per_host(0);
        let client = if title_case {
            builder.http1_title_case_headers()
        } else {
            builder
        }
        .build()?;
        let mut kept = self.trackers.lock().unwrap_or_else(PoisonError::into_inner);
        if kept.len() >= TRACKER_CLIENTS_KEPT {
            kept.clear();
        }
        kept.insert(key, client.clone());
        Ok(client)
    }

    // A request leaves as the leg's sockets do: through the bridge from
    // wherever the proxy exits, else from the host, bound as the leg is.
    fn egress(&self, to: SocketAddr) -> Egress {
        match (&self.bridge, &self.kind) {
            (Some(_), _) | (None, LegKind::Ladder) => Egress::Proxied,
            (None, LegKind::System) => route_to(to, None, None),
            (None, LegKind::SourceAddress(address)) => route_to(to, Some(*address), None),
            (None, LegKind::Interface(name)) => route_to(to, None, Some(name)),
        }
    }
}

#[cfg(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn bound_to(builder: reqwest::ClientBuilder, name: &str) -> Result<reqwest::ClientBuilder, String> {
    Ok(builder.interface(name))
}

#[cfg(not(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
)))]
fn bound_to(_: reqwest::ClientBuilder, _: &str) -> Result<reqwest::ClientBuilder, String> {
    Err("binding to an interface is not supported on this platform".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxies::Rung;

    fn ladder() -> LegPath {
        LegPath::Ladder {
            rungs: vec![Rung::Proxy { id: 1 }],
            direct_fallback: false,
        }
    }

    #[test]
    fn a_direct_leg_follows_its_egress_binding() {
        assert_eq!(
            LegKind::of(&EgressBinding::System, &LegPath::Direct),
            LegKind::System
        );
        assert_eq!(
            LegKind::of(
                &EgressBinding::Interface { name: "en1".into() },
                &LegPath::Direct
            ),
            LegKind::Interface("en1".into())
        );
        let address: IpAddr = "192.0.2.4".parse().unwrap();
        assert_eq!(
            LegKind::of(&EgressBinding::SourceAddress { address }, &LegPath::Direct),
            LegKind::SourceAddress(address)
        );
    }

    #[test]
    fn a_ladder_leg_is_a_ladder_on_any_egress() {
        for binding in [
            EgressBinding::System,
            EgressBinding::Interface { name: "en1".into() },
            EgressBinding::SourceAddress {
                address: "192.0.2.4".parse().unwrap(),
            },
        ] {
            assert_eq!(LegKind::of(&binding, &ladder()), LegKind::Ladder);
        }
    }

    #[test]
    fn dht_runs_wherever_the_leg_carries_udp() {
        for (kind, own_sockets, datagrams) in [
            (LegKind::System, true, true),
            (LegKind::Interface("en1".into()), true, true),
            (
                LegKind::SourceAddress("192.0.2.4".parse().unwrap()),
                false,
                true,
            ),
            (LegKind::Ladder, false, false),
        ] {
            let shape = kind.shape(None);
            assert_eq!(shape.own_sockets, own_sockets, "{kind:?}");
            assert_eq!(shape.datagrams, datagrams, "{kind:?}");
            assert_eq!(shape.dht, shape.datagrams, "{kind:?}");
        }
    }

    #[test]
    fn upnp_runs_only_on_system_egress() {
        assert!(LegKind::System.shape(None).upnp);
        assert!(!LegKind::Interface("en1".into()).shape(None).upnp);
        assert!(
            !LegKind::SourceAddress("192.0.2.4".parse().unwrap())
                .shape(None)
                .upnp
        );
        assert!(!LegKind::Ladder.shape(Some(&carrying())).upnp);
    }

    #[test]
    fn a_ladder_leg_carries_udp_only_when_its_dialer_does() {
        let shape = LegKind::Ladder.shape(Some(&loopback_egress()));
        assert!(!shape.listen && !shape.datagrams && !shape.dht && !shape.own_sockets);
        assert_eq!(LegKind::Ladder.listen_ip(), None);

        let shape = LegKind::Ladder.shape(Some(&carrying()));
        assert!(shape.datagrams && shape.dht);
        assert!(!shape.listen && !shape.upnp && !shape.own_sockets);
    }

    #[test]
    fn a_source_address_leg_listens_on_its_address() {
        let address: IpAddr = "192.0.2.4".parse().unwrap();
        let kind = LegKind::SourceAddress(address);
        assert!(kind.shape(None).listen);
        assert_eq!(kind.listen_ip(), Some(address));
        assert_eq!(kind.bind_device(), None);
        assert_eq!(LegKind::Interface("en1".into()).bind_device(), Some("en1"));
    }

    #[tokio::test]
    async fn no_datagrams_refuses_every_send() {
        let socket = NoDatagrams(SocketAddr::from(([127, 0, 0, 1], 0)));
        let error = socket
            .send_to(b"x", SocketAddr::from(([127, 0, 0, 1], 9)))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    fn loopback_egress() -> Arc<dyn Dialer> {
        Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
            weaver_tunnel::pipe::Egress::new(
                4,
                weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
                std::time::Duration::from_secs(30),
            ),
        )))
    }

    // A source-address leg's sockets: peers dialed through the leg, a
    // listener and a UDP socket bound to the address.
    #[tokio::test]
    async fn a_source_address_leg_dials_listens_and_carries_udp_on_its_address() {
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let sockets = LegSockets::new(loopback_egress(), &LegKind::SourceAddress(loopback));
        let listener = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(listener.local_addr().ip(), loopback);
        let (_read, mut write) = sockets.connect_tcp(listener.local_addr()).await.unwrap();
        let (_, mut read, _) = listener.accept().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut write, b"peer")
            .await
            .unwrap();
        let mut said = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut read, &mut said)
            .await
            .unwrap();
        assert_eq!(&said, b"peer");

        let udp = sockets
            .bind_udp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(udp.bind_addr().ip(), loopback);
        let other = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp.send_to(b"dht", other.local_addr().unwrap())
            .await
            .unwrap();
        let mut got = [0u8; 3];
        let (len, from) = other.recv_from(&mut got).await.unwrap();
        assert_eq!((&got[..len], from), (&b"dht"[..], udp.bind_addr()));
    }

    // A tunnel's datagrams, each socket a loopback socket that stands for
    // one inside the tunnel.
    #[derive(Default)]
    struct Tunnel {
        bound: Mutex<Vec<SocketAddr>>,
    }

    struct TunnelUdp(tokio::net::UdpSocket);

    #[async_trait::async_trait]
    impl TunnelSocket for TunnelUdp {
        async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
            self.0.send_to(data, target).await.map(drop)
        }
        async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            self.0.recv_from(buffer).await
        }
        fn local_addr(&self) -> Option<SocketAddr> {
            self.0.local_addr().ok()
        }
        fn link_mtu(&self) -> Option<u16> {
            None
        }
        fn is_closed(&self) -> bool {
            false
        }
        async fn closed(&self) {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl DatagramTransport for Tunnel {
        async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, weaver_tunnel::TunnelError> {
            Ok(match host {
                "router.example" => vec!["192.0.2.7".parse().unwrap()],
                _ => Vec::new(),
            })
        }
        async fn bind(
            &self,
            peer: SocketAddr,
        ) -> Result<Arc<dyn TunnelSocket>, weaver_tunnel::TunnelError> {
            self.bound.lock().unwrap().push(peer);
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            Ok(Arc::new(TunnelUdp(socket)))
        }
    }

    // A rung whose tunnel carries datagrams.
    struct Carrying(Arc<Tunnel>);

    #[async_trait::async_trait]
    impl Dialer for Carrying {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped(
                "datagrams only".into(),
            ))
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, weaver_tunnel::pipe::DialError> {
            let addresses = self.0.resolve(host).await.unwrap();
            Ok(Resolution::Addresses(addresses))
        }
        fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
            Some(self.0.clone())
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "carrying".into()
        }
    }

    fn carrying() -> Arc<dyn Dialer> {
        Arc::new(Carrying(Arc::default()))
    }

    // A ladder leg whose tunnel carries datagrams: the session's UDP binds
    // inside the tunnel, and its names resolve there.
    #[tokio::test]
    async fn a_ladder_leg_carries_udp_through_its_tunnel() {
        let tunnel = Arc::new(Tunnel::default());
        let sockets = LegSockets::new(Arc::new(Carrying(tunnel.clone())), &LegKind::Ladder);
        let asked = SocketAddr::from(([0, 0, 0, 0], 6881));
        let udp = sockets.bind_udp(asked).await.unwrap();
        assert_eq!(*tunnel.bound.lock().unwrap(), [asked]);
        assert_eq!(udp.bind_addr().ip(), IpAddr::from([127, 0, 0, 1]));

        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sent = udp
            .send_to(b"dht", peer.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(sent, 3);
        let mut got = [0u8; 3];
        let (len, from) = peer.recv_from(&mut got).await.unwrap();
        assert_eq!((&got[..len], from), (&b"dht"[..], udp.bind_addr()));

        peer.send_to(b"pong", udp.bind_addr()).await.unwrap();
        let mut back = [0u8; 4];
        let (len, from) = udp.recv_from(&mut back).await.unwrap();
        assert_eq!(
            (&back[..len], from),
            (&b"pong"[..], peer.local_addr().unwrap())
        );

        let resolver = LegNames(Arc::new(Carrying(tunnel.clone())));
        assert_eq!(
            resolver.resolve("router.example", 6881).await.unwrap(),
            Resolved::Addresses(vec![SocketAddr::from(([192, 0, 2, 7], 6881))])
        );
    }

    // A ladder leg's sockets: peers dialed through the leg, and neither a
    // listener nor UDP when its dialer carries no datagrams.
    #[tokio::test]
    async fn a_ladder_leg_dials_only() {
        let sockets = LegSockets::new(loopback_egress(), &LegKind::Ladder);
        let error = match sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
        {
            Ok(_) => panic!("a ladder leg listened"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        let udp = sockets
            .bind_udp(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let refused = udp
            .send_to(b"x", SocketAddr::from(([127, 0, 0, 1], 9)))
            .await
            .unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::Unsupported);
        let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_read, _write) = sockets
            .connect_tcp(peer.local_addr().unwrap())
            .await
            .unwrap();
        peer.accept().await.unwrap();
    }

    // A ladder whose last hop takes connections, here on a loopback
    // listener standing in for one inside a tunnel.
    #[derive(Default)]
    struct Accepting(Arc<std::sync::atomic::AtomicUsize>);
    struct LoopbackInbound(Arc<std::sync::atomic::AtomicUsize>);
    struct LoopbackListener(tokio::net::TcpListener, Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl weaver_tunnel::pipe::Inbound for LoopbackInbound {
        async fn listen(
            &self,
            port: u16,
            _: bool,
        ) -> Result<Box<dyn Listening>, weaver_tunnel::TunnelError> {
            let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            Ok(Box::new(LoopbackListener(listener, self.0.clone())))
        }
    }
    #[async_trait::async_trait]
    impl Listening for LoopbackListener {
        fn local_addr(&self) -> SocketAddr {
            self.0.local_addr().unwrap()
        }
        async fn accept(&self) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::TunnelError> {
            let (stream, peer) = self
                .0
                .accept()
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            let outcome = Arc::new(ConnectionOutcome::default());
            let reads = self.1.clone();
            outcome.on_read(move |bytes| {
                reads.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
            });
            Ok(weaver_tunnel::pipe::Dialed {
                stream: DialedStream::Socket(stream),
                outcome,
                path: weaver_tunnel::pipe::DialPath::default(),
                peer: Some(peer),
                source: Some(self.local_addr()),
                setup: None,
            })
        }
    }
    #[async_trait::async_trait]
    impl Dialer for Accepting {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped(
                "takes connections only".into(),
            ))
        }
        fn inbound(self: Arc<Self>) -> Option<Arc<dyn weaver_tunnel::pipe::Inbound>> {
            Some(Arc::new(LoopbackInbound(self.0.clone())))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "accepting".into()
        }
    }

    // A ladder leg whose last hop takes connections listens through it, and
    // runs no uTP; one whose last hop does not, never listens.
    #[test]
    fn a_ladder_leg_listens_exactly_when_its_last_hop_takes_connections() {
        let accepting: Arc<dyn Dialer> = Arc::new(Accepting::default());
        let shape = LegKind::Ladder.shape(Some(&accepting));
        assert!(shape.listen && !shape.utp && !shape.upnp && !shape.own_sockets);
        for dialer in [loopback_egress(), carrying()] {
            assert!(!LegKind::Ladder.shape(Some(&dialer)).listen);
        }
        assert!(!LegKind::Ladder.shape(None).listen);
        for kind in [
            LegKind::System,
            LegKind::Interface("en1".into()),
            LegKind::SourceAddress("192.0.2.4".parse().unwrap()),
        ] {
            assert!(kind.shape(None).utp, "{kind:?}");
        }
    }

    // A peer that connects in through the leg reaches the session, which
    // sees its address, the port it listens on, and counts what it reads.
    #[tokio::test]
    async fn a_ladder_leg_takes_peers_through_its_last_hop_and_counts_their_bytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let accepting = Arc::new(Accepting::default());
        let sockets = LegSockets::new(accepting.clone(), &LegKind::Ladder);
        let listener = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        let local = listener.local_addr();
        assert_eq!(local.ip(), IpAddr::from([127, 0, 0, 1]));
        let (accepted, peer) =
            tokio::join!(listener.accept(), tokio::net::TcpStream::connect(local));
        let (from, mut read, mut write) = accepted.unwrap();
        let mut peer = peer.unwrap();
        assert_eq!(from, peer.local_addr().unwrap());
        peer.write_all(b"interested").await.unwrap();
        let mut heard = [0u8; 10];
        read.read_exact(&mut heard).await.unwrap();
        assert_eq!(&heard, b"interested");
        assert_eq!(accepting.0.load(std::sync::atomic::Ordering::SeqCst), 10);
        write.write_all(b"unchoke").await.unwrap();
        let mut answer = [0u8; 7];
        peer.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"unchoke");
    }

    // One WireGuard hop of a chain, as the runtime builds it: over the
    // loopback egress for the first hop, else inside the hop beneath.
    fn wireguard_hop(
        id: u32,
        spec: weaver_tunnel::WireGuardSpec,
        beneath: Option<&Arc<weaver_tunnel::pipe::SessionHop>>,
        forwarding: weaver_tunnel::pipe::PortForwarding,
    ) -> Arc<weaver_tunnel::pipe::SessionHop> {
        let egress = Arc::new(weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            std::time::Duration::from_secs(30),
        ));
        let provider = weaver_tunnel::WireGuardTunnelProvider::new(
            spec,
            Arc::new(weaver_tunnel::NoopTunnelObserver),
        );
        let (provider, inner, proxies): (_, Arc<dyn Dialer>, Vec<u32>) = match beneath {
            None => (provider.with_udp_factory(egress.clone()), egress, vec![]),
            Some(beneath) => (
                provider.with_datagram_transport(
                    beneath
                        .clone()
                        .datagrams()
                        .expect("a WireGuard hop carries datagrams"),
                ),
                beneath.clone(),
                [beneath.path.proxies.clone(), vec![beneath.id]].concat(),
            ),
        };
        let provider = Arc::new(provider);
        Arc::new(weaver_tunnel::pipe::SessionHop {
            capacity: weaver_tunnel::pipe::SessionCapacity::new(Arc::new(
                tokio::sync::Semaphore::new(1),
            )),
            activity: Default::default(),
            id,
            provider: provider.clone(),
            inner,
            transport: None,
            path: weaver_tunnel::pipe::DialPath {
                egress: 4,
                proxies,
                ..Default::default()
            },
            endpoint: None,
            timeout: std::time::Duration::from_secs(30),
            naming: weaver_tunnel::pipe::Naming::proxy_servers(vec![
                weaver_tunnel::test_support::TEST_PEER_ADDRESS.into(),
            ]),
            resolver: Some(provider),
            forwarding,
            failure: Default::default(),
        })
    }

    // A ladder leg whose one rung is a chain of three WireGuard hops, each
    // carried inside the one before, listens on the chain's last hop and
    // announces the port that hop's gateway forwards over NAT-PMP; a peer
    // beyond the last hop connects in on that port.
    #[tokio::test]
    async fn a_ladder_leg_over_a_three_hop_wireguard_chain_listens_on_its_last_hop() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use weaver_tunnel::pipe::PortForwarding;
        use weaver_tunnel::test_support::{
            TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions,
            natpmp::{NatPmpGateway, PUBLIC_ADDRESS},
            test_key,
        };
        const CHAINED_PORT: u16 = 51820;
        const MAPPED: u16 = 47002;
        let gateway = NatPmpGateway::start(MAPPED).await;
        let exit = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(53),
            udp_forward: Some((5351, gateway.address())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let middle = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(52),
            udp_forward: Some((CHAINED_PORT, exit.endpoint())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let entry = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(51),
            udp_forward: Some((CHAINED_PORT, middle.endpoint())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let chained = |peer: &WireGuardTestPeer, id: &str| weaver_tunnel::WireGuardSpec {
            endpoint_host: TEST_PEER_ADDRESS.to_string(),
            endpoint_port: CHAINED_PORT,
            ..peer.client_spec(id)
        };
        let first = wireguard_hop(1, entry.client_spec("1"), None, PortForwarding::None);
        let second = wireguard_hop(2, chained(&middle, "2"), Some(&first), PortForwarding::None);
        let last = wireguard_hop(
            3,
            chained(&exit, "3"),
            Some(&second),
            PortForwarding::NatPmp {
                gateway: weaver_tunnel::pipe::gateway_of(TEST_CLIENT_ADDRESS),
            },
        );
        let rung: Arc<dyn Dialer> = last;
        let leg: Arc<dyn Dialer> = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
            weaver_tunnel::pipe::Fallback::new(vec![rung]),
        )));

        let shape = LegKind::Ladder.shape(Some(&leg));
        assert!(shape.listen && !shape.utp);

        let sockets = LegSockets::new(leg, &LegKind::Ladder);
        let listener = sockets
            .listen_peers_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(listener.local_addr().ip(), IpAddr::V4(TEST_CLIENT_ADDRESS));
        let mut reports = listener
            .port_mappings()
            .expect("the last hop's gateway forwards a port");
        let report = reports.recv().await.unwrap();
        assert_eq!(report.transport, PortmapTransport::NatPmp);
        assert_eq!(report.protocol, PortmapProtocol::Tcp);
        assert_eq!(report.external_port, Some(MAPPED));
        assert_eq!(report.external_ip, Some(IpAddr::V4(PUBLIC_ADDRESS)));
        assert_eq!(report.error, None);

        // The listener moves to the forwarded port on its next accept; the
        // peer dials once it says it listens there.
        let (accepted, peer) = tokio::join!(listener.accept(), async {
            while listener.local_addr().port() != MAPPED {
                tokio::task::yield_now().await;
            }
            exit.dial_client(MAPPED).await
        });
        let (from, mut read, mut write) = accepted.unwrap();
        let mut peer = peer.unwrap();
        assert_eq!(from.ip(), IpAddr::V4(TEST_PEER_ADDRESS));
        peer.write_all(b"interested").await.unwrap();
        let mut heard = [0u8; 10];
        read.read_exact(&mut heard).await.unwrap();
        assert_eq!(&heard, b"interested");
        write.write_all(b"unchoke").await.unwrap();
        let mut answer = [0u8; 7];
        peer.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"unchoke");

        // The outer hops' far sides find nothing on that port.
        assert!(entry.dial_client(MAPPED).await.is_err());
        assert!(middle.dial_client(MAPPED).await.is_err());

        // The SSL listener is not the one peers reach through the forwarded
        // port: it listens where it asked and reports no mapping.
        let other = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_ne!(other.local_addr().port(), MAPPED);
        assert!(other.port_mappings().is_none());
    }

    // A dialer whose naming rule answers one address, or hands names on.
    struct Naming(Option<IpAddr>);

    #[async_trait::async_trait]
    impl Dialer for Naming {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped("names only".into()))
        }
        async fn resolve(&self, _: &str) -> Result<Resolution, weaver_tunnel::pipe::DialError> {
            Ok(self
                .0
                .map_or(Resolution::Unresolved, |ip| Resolution::Addresses(vec![ip])))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "naming".into()
        }
    }

    // A leg resolves every name the session looks up by its dialer's
    // naming, and a name its dialer hands on resolves at the exit: no
    // lookup is made here, of the host's resolver or any other.
    #[tokio::test]
    async fn a_leg_resolves_names_through_its_dialer_and_hands_the_rest_on() {
        let answer: IpAddr = "192.0.2.9".parse().unwrap();
        let resolver = LegNames(Arc::new(Naming(Some(answer))));
        assert_eq!(
            resolver.resolve("router.test", 6881).await.unwrap(),
            Resolved::Addresses(vec![SocketAddr::new(answer, 6881)])
        );

        let host_lookups = || {
            let metrics = weaver_tunnel::metrics::snapshot();
            let resolved: u64 = metrics
                .resolutions
                .iter()
                .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
                .map(|(_, ok, failed)| ok + failed)
                .sum();
            let held: u64 = metrics
                .cache
                .iter()
                .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
                .map(|(.., count)| count)
                .sum();
            resolved + held
        };
        let before = host_lookups();
        let resolver = LegNames(Arc::new(Naming(None)));
        assert_eq!(
            resolver.resolve("localhost", 6881).await.unwrap(),
            Resolved::AtExit
        );
        assert_eq!(host_lookups(), before);
    }

    #[test]
    fn a_ladder_client_needs_its_bridge() {
        assert!(tracker_client(&LegKind::Ladder, None).is_err());
        assert!(tracker_client(&LegKind::System, None).is_ok());
    }

    // A leg's client for a web seed's target dials the host at the checked
    // address without looking the name up, and hands a redirect back
    // instead of following it.
    #[tokio::test(start_paused = true)]
    async fn a_target_client_dials_the_checked_address_and_follows_nothing() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let served = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.ends_with(b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended early");
                head.extend_from_slice(&buf[..read]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: http://elsewhere.test/\r\n\
                      Content-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            String::from_utf8(head).unwrap()
        });

        // A direct leg's builder, kept off any proxy the host names.
        let clients = LegClients {
            base: || Ok(reqwest::Client::builder().no_proxy()),
            bridge: None,
            kind: LegKind::System,
            trackers: Mutex::default(),
        };
        let client = clients
            .client(Some(HttpTarget {
                host: "seed.test",
                port,
                addrs: &[IpAddr::from([127, 0, 0, 1])],
            }))
            .unwrap();
        let response = client
            .get(format!("http://seed.test:{port}/f"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        let head = served.await.unwrap();
        assert!(head.starts_with("GET /f HTTP/1.1\r\n"), "{head}");
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("host: seed.test:{port}\r\n")),
            "{head}"
        );
    }
}

/// What a session on one leg shows the trackers and peers it reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exposure {
    /// Who resolves the names the session looks up.
    pub resolved_by: Option<ResolvedBy>,
    /// The address a tracker sees: the session's vote of its external
    /// address, else a gateway's; none until one is known.
    pub address: Option<IpAddr>,
    /// The port the session announces, 0 when it announces none.
    pub announced_port: u16,
    /// Peers can connect in.
    pub inbound: bool,
    pub udp_trackers: bool,
    pub dht: bool,
    pub utp: bool,
}

impl Exposure {
    /// The exposure of `session`, open on a leg of `shape` whose names
    /// `resolved_by` resolves, with the last report of each of its port
    /// mappings.
    pub fn of(
        session: &Session,
        shape: Shape,
        resolved_by: Option<ResolvedBy>,
        mappings: &[PortMapped],
    ) -> Self {
        let announced_port = session.announce_port().unwrap_or(0);
        Self {
            resolved_by,
            address: session
                .external_ips()
                .first()
                .copied()
                .or_else(|| mappings.iter().find_map(|mapping| mapping.external_ip)),
            announced_port,
            inbound: announced_port != 0,
            udp_trackers: shape.datagrams,
            dht: shape.dht,
            utp: session.listens_utp(),
        }
    }
}
