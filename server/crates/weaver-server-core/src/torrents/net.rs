// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The sockets a torrent session uses, for the one leg of its route.
//!
//! A leg's path decides everything: whether the session dials with its own
//! kernel sockets or through the leg's dialer, whether it can listen, and
//! whether it may use UDP at all (DHT, UDP trackers, uTP).

use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use librqbit::network::{
    AsyncReadVectoredIntoCompat, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture, DatagramSocket,
};
use librqbit::{SocketFactory, StreamListener};
use weaver_tunnel::bridge::Bridge;
use weaver_tunnel::pipe::{DialedStream, Dialer, Purpose, Target};

use crate::proxies::{EgressBinding, LegPath};
use crate::torrents::identity::ClientIdentity;

/// How a leg reaches peers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LegKind {
    /// The host's own sockets.
    System,
    /// Kernel sockets bound to a device.
    Interface(String),
    /// Kernel sockets bound to a source address.
    SourceAddress(IpAddr),
    /// Through a proxy or tunnel rung: stream connections only.
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

    /// What a session on this leg may do.
    pub fn shape(&self) -> Shape {
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
            Self::Ladder => Shape {
                own_sockets: false,
                dht: false,
                upnp: false,
                listen: false,
                datagrams: false,
            },
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

/// Peer sockets through a leg's dialer; a leg that pins a source address
/// also listens and carries UDP on it.
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
            Ok(halves(dialed.stream))
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
        Box::pin(async move {
            Ok(match self.source {
                Some(ip) => {
                    Arc::new(tokio::net::UdpSocket::bind(SocketAddr::new(ip, addr.port())).await?)
                        as Arc<dyn DatagramSocket>
                }
                None => Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>,
            })
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

/// The client for HTTP trackers and metainfo downloads on a leg. A ladder
/// leg's client rides `bridge`, the leg's own.
pub fn tracker_client(
    kind: &LegKind,
    bridge: Option<&Bridge>,
    identity: &ClientIdentity,
) -> Result<reqwest::Client, String> {
    let builder = reqwest::Client::builder().user_agent(identity.user_agent());
    let builder = match (kind, bridge) {
        (LegKind::Ladder, Some(bridge)) => crate::proxies::through_bridge(builder, bridge)?,
        (LegKind::Ladder, None) => return Err("a ladder leg needs its bridge".into()),
        (LegKind::System, _) => builder,
        (LegKind::SourceAddress(address), _) => builder.local_address(*address),
        (LegKind::Interface(name), _) => bound_to(builder, name)?,
    };
    #[expect(
        clippy::disallowed_methods,
        reason = "a torrent session's tracker client, built for its leg"
    )]
    builder.build().map_err(|error| error.to_string())
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
            let shape = kind.shape();
            assert_eq!(shape.own_sockets, own_sockets, "{kind:?}");
            assert_eq!(shape.datagrams, datagrams, "{kind:?}");
            assert_eq!(shape.dht, shape.datagrams, "{kind:?}");
        }
    }

    #[test]
    fn upnp_runs_only_on_system_egress() {
        assert!(LegKind::System.shape().upnp);
        assert!(!LegKind::Interface("en1".into()).shape().upnp);
        assert!(
            !LegKind::SourceAddress("192.0.2.4".parse().unwrap())
                .shape()
                .upnp
        );
        assert!(!LegKind::Ladder.shape().upnp);
    }

    #[test]
    fn a_ladder_leg_neither_listens_nor_carries_udp() {
        let shape = LegKind::Ladder.shape();
        assert!(!shape.listen && !shape.datagrams && !shape.own_sockets);
        assert_eq!(LegKind::Ladder.listen_ip(), None);
    }

    #[test]
    fn a_source_address_leg_listens_on_its_address() {
        let address: IpAddr = "192.0.2.4".parse().unwrap();
        let kind = LegKind::SourceAddress(address);
        assert!(kind.shape().listen);
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

    // A ladder leg's sockets: peers dialed through the leg, and neither a
    // listener nor UDP.
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

    #[test]
    fn a_ladder_client_needs_its_bridge() {
        let identity = crate::torrents::identity::IDENTITY;
        assert!(tracker_client(&LegKind::Ladder, None, &identity).is_err());
        assert!(tracker_client(&LegKind::System, None, &identity).is_ok());
    }
}
