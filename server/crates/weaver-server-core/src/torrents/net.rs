// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The sockets a torrent session uses, for the one leg of its route.
//!
//! A leg's path decides whether the session dials with its own kernel
//! sockets or through the leg's dialer, and whether it can listen. Whether it
//! may use UDP at all (DHT, UDP trackers, uTP) is the dialer's to say.

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
use weaver_torrent::upnp::PortMapped;
use weaver_torrent::{
    Egress, HttpClients, HttpTarget, Session, SocketFactory, StreamListener, route_to,
};
use weaver_tunnel::bridge::{Bridge, ConnectionOutcome};
use weaver_tunnel::endpoint::{DatagramSocket as TunnelSocket, DatagramTransport};
use weaver_tunnel::pipe::{DialedStream, Dialer, Purpose, Resolution, Target};

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
                datagrams: true,
            },
            Self::Interface(_) => Shape {
                own_sockets: true,
                dht: true,
                upnp: false,
                listen: true,
                datagrams: true,
            },
            Self::SourceAddress(_) => Shape {
                own_sockets: false,
                dht: true,
                upnp: false,
                listen: true,
                datagrams: true,
            },
            Self::Ladder => {
                let datagrams = dialer.is_some_and(|dialer| carrier(dialer).is_some());
                Shape {
                    own_sockets: false,
                    dht: datagrams,
                    upnp: false,
                    listen: false,
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
        Box::pin(async move {
            let ip = self.source.ok_or_else(|| {
                io::Error::new(io::ErrorKind::Unsupported, "this leg cannot listen")
            })?;
            let listener = tokio::net::TcpListener::bind(SocketAddr::new(ip, addr.port())).await?;
            Ok(Box::new(Listener(listener)) as Box<dyn StreamListener>)
        })
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

    // Only uTP polls a send, and only on a leg that listens; a ladder leg
    // never does.
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
}

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
