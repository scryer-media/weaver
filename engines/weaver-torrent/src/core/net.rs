// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Pluggable network primitives.
//!
//! By default every crate in this workspace binds and connects kernel sockets
//! itself. Embedders that need traffic to flow through something else (a
//! userspace VPN, a SOCKS/HTTP proxy, a per-tenant network namespace, a test
//! harness) can hand in their own implementations of these traits instead.
//!
//! The traits are object safe on purpose: an embedder passes an
//! `Arc<dyn DatagramSocket>` and never needs to name the concrete type.
//! Default code paths keep using concrete kernel sockets, so the indirection
//! costs nothing unless a custom socket is injected.

use std::{
    future::Future,
    io::{self, IoSlice},
    net::{IpAddr, SocketAddr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A boxed future that is both `Send` and `Sync`, as required by the uTP
/// transport abstraction this trait is adapted to.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

pub use weaver_utp::{DatagramSend, Fragmenting};

/// An unconnected, message-oriented socket: the shape of a UDP socket, without
/// requiring it to be one.
///
/// Used for DHT, UDP trackers and the uTP peer transport.
pub trait DatagramSocket: Send + Sync + 'static {
    /// An owned send readiness registration for one flow, when needed.
    /// `None` keeps polling this socket directly.
    fn sender(&self) -> Option<Box<dyn DatagramSend>> {
        None
    }

    /// Stop and join work owned by this dedicated socket before release.
    fn shutdown(&self) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    /// The local address the socket is bound to.
    fn bind_addr(&self) -> SocketAddr;

    fn recv_from<'a>(&'a self, buf: &'a mut [u8])
    -> BoxFuture<'a, io::Result<(usize, SocketAddr)>>;

    fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr)
    -> BoxFuture<'a, io::Result<usize>>;

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>>;

    /// Vectored send. The default implementation copies the slices into one
    /// buffer; implementations backed by a real socket should override it.
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        let mut joined = Vec::with_capacity(total);
        for b in bufs {
            joined.extend_from_slice(b);
        }
        self.poll_send_to(cx, &joined, target)
    }

    /// Sends a uTP size probe or a packet given up on as too large, as
    /// `fragmenting` says. A socket that cannot control fragmenting, such as
    /// a tunnel's, sends it as any other datagram.
    fn poll_send_to_vectored_fragmenting(
        &self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
        fragmenting: Fragmenting,
    ) -> Poll<io::Result<usize>> {
        let _ = fragmenting;
        self.poll_send_to_vectored(cx, bufs, target)
    }

    /// The largest IP packet the socket's route carries, where it knows
    /// it, such as a tunnel's MTU; uTP sends nothing larger.
    fn link_mtu(&self) -> Option<u16> {
        None
    }
}

impl DatagramSocket for tokio::net::UdpSocket {
    fn bind_addr(&self) -> SocketAddr {
        self.local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(tokio::net::UdpSocket::recv_from(self, buf))
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(tokio::net::UdpSocket::send_to(self, buf, target))
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        tokio::net::UdpSocket::poll_send_to(self, cx, buf, target)
    }
}

/// Who named a tracker or a web seed. What came with the torrent, from its
/// metainfo, its magnet link or a peer, is asked only at public addresses,
/// on every hop; what the operator named is asked wherever the session
/// reaches.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum NamedBy {
    #[default]
    Torrent,
    Operator,
}

impl NamedBy {
    /// Whether an endpoint so named may be asked at `ip`.
    pub fn may_ask(self, ip: IpAddr) -> bool {
        match self {
            NamedBy::Torrent => is_public(ip),
            NamedBy::Operator => true,
        }
    }

    /// Whether a tracker so named may be announced to at `ip`: where it may
    /// be asked, and, in an e2e build, anywhere, so the e2e rigs can serve
    /// real trackers on loopback and container networks. Web seeds stay
    /// under [`NamedBy::may_ask`].
    pub fn may_announce_to(self, ip: IpAddr) -> bool {
        cfg!(feature = "e2e-local-trackers") || self.may_ask(ip)
    }

    /// Whether an endpoint so named may be asked by the name `host` where
    /// the network resolves it at its exit ([`Resolved::AtExit`]), with no
    /// address here to check: what came with the torrent is refused under a
    /// plainly local name (see [`is_local_name`]). A public-looking name
    /// passes, wherever the exit resolves it.
    pub fn may_ask_by_name(self, host: &str) -> bool {
        match self {
            NamedBy::Torrent => !is_local_name(host),
            NamedBy::Operator => true,
        }
    }

    /// Whether a tracker so named may be announced to by the name `host`
    /// resolved at the network's exit: where it may be asked by it, and, in
    /// an e2e build, under any name, as [`NamedBy::may_announce_to`].
    pub fn may_announce_by_name(self, host: &str) -> bool {
        cfg!(feature = "e2e-local-trackers") || self.may_ask_by_name(host)
    }
}

/// Whether `host`, a domain name, is plainly one of the local network:
/// `localhost` or a name under it, a name of a single label (`nas`), or a
/// name under `lan`, `local`, `internal` or `home.arpa`. Case is ignored,
/// and so is one trailing dot.
pub(crate) fn is_local_name(host: &str) -> bool {
    let host = host.strip_suffix('.').unwrap_or(host).to_ascii_lowercase();
    !host.contains('.')
        || ["localhost", "lan", "local", "internal", "home.arpa"]
            .iter()
            .any(|zone| {
                host.strip_suffix(zone)
                    .is_some_and(|rest| rest.is_empty() || rest.ends_with('.'))
            })
}

/// Whether `ip` is a public internet address: global, so neither loopback,
/// private, link-local, unique local, multicast nor unspecified, and
/// outside the documentation ranges.
pub(crate) fn is_public(ip: IpAddr) -> bool {
    let documentation = match ip.to_canonical() {
        IpAddr::V4(a) => matches!(
            a.octets(),
            [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
        ),
        IpAddr::V6(a) => {
            let s = a.segments();
            (s[0], s[1]) == (0x2001, 0xdb8) || (s[0] & 0xfff0) == 0x3ff0
        }
    };
    crate::torrent_state::live::peers::holepunch::is_global(ip) && !documentation
}

/// Whether `host` has an internationalized (IDNA) label: one that starts
/// `xn--`, in any case. A URL writes a Unicode host in that form, so this
/// catches both spellings (libtorrent `is_idna`, src/parse_url.cpp:203-218
/// at v2.0.15).
pub(crate) fn is_idna(host: &str) -> bool {
    host.split('.').any(|label| {
        label
            .as_bytes()
            .get(..4)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(b"xn--"))
    })
}

/// What a name resolves to on the session's network.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// Every address found, each carrying the port asked for. The session
    /// checks them, and connects only to those it keeps.
    Addresses(Vec<SocketAddr>),
    /// The network resolves the name where it exits: no address is known
    /// here to check, and a connection hands the name on.
    AtExit,
}

/// Resolves a hostname on the session's network.
///
/// Every name the session looks up goes through it: DHT bootstrap nodes,
/// UDP and HTTP trackers, and web seeds. Embedders that route traffic
/// through their own network stack inject one so that lookups follow that
/// stack too.
pub trait Resolver: Send + Sync + 'static {
    /// Resolve `host` (a domain name, without port or brackets), every
    /// address found carrying `port`.
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, io::Result<Resolved>>;
}

/// The resolver of a session given none: it looks no name up, so the
/// session reaches only what is named by its address.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoNames;

impl Resolver for NoNames {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        Box::pin(async move {
            let address = host.trim_start_matches('[').trim_end_matches(']');
            match address.parse::<IpAddr>() {
                Ok(ip) => Ok(Resolved::Addresses(vec![SocketAddr::new(ip, port)])),
                Err(_) => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{host}: the session was given no resolver"),
                )),
            }
        })
    }
}

/// Where one HTTP request goes: the host its URL names, its port, and the
/// addresses the engine checked for that host, none when the network
/// resolves the name where it exits ([`Resolved::AtExit`]).
#[derive(Debug, Clone, Copy)]
pub struct HttpTarget<'a> {
    pub host: &'a str,
    pub port: u16,
    pub addrs: &'a [IpAddr],
}

/// What a session asks a client for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ClientUse {
    /// Metainfo, the block and allow lists, and web seeds. With no target
    /// the client resolves and follows redirects itself; held to a target
    /// it follows none. It keeps connections open between requests.
    Fetch,
    /// Tracker requests. The client keeps no connection open after a
    /// request, so each announce dials and closes as one from a client
    /// built for it alone does; `title_case` writes each word of a header
    /// name capitalised, as libtorrent writes them.
    Tracker { title_case: bool },
    /// A port mapper's requests to the gateway on the local network: never
    /// through a proxy.
    Gateway,
}

/// The TLS identity an SSL torrent's announces present, and the CA they
/// trust beside the system roots.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ClientTls {
    /// The identity's certificate chain and private key, PEM.
    pub identity: Arc<[u8]>,
    /// The torrent's CA, DER.
    pub ca: Arc<[u8]>,
}

impl std::fmt::Debug for ClientTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClientTls")
    }
}

/// A client's shape: what it is for, the TLS identity it presents, and the
/// torrent it carries requests for.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientShape {
    pub used_for: ClientUse,
    pub tls: Option<ClientTls>,
    /// The info-hash of the one torrent whose requests the client carries,
    /// when it carries one torrent's alone: a web seed's. Clients for two
    /// torrents never share a connection.
    pub torrent: Option<crate::core::hash_id::Id20>,
}

impl ClientShape {
    /// A client for metainfo, lists and web seeds.
    pub const FETCH: Self = Self::of(ClientUse::Fetch);
    /// A client for the gateway a port mapper asks.
    pub const GATEWAY: Self = Self::of(ClientUse::Gateway);

    pub const fn of(used_for: ClientUse) -> Self {
        Self {
            used_for,
            tls: None,
            torrent: None,
        }
    }

    /// This shape, for the requests of the torrent `info_hash` alone.
    pub fn for_torrent(self, info_hash: crate::core::hash_id::Id20) -> Self {
        Self {
            torrent: Some(info_hash),
            ..self
        }
    }

    /// `builder` given this shape, for the embedder to build the client
    /// it hands back from.
    pub fn apply(&self, builder: reqwest::ClientBuilder) -> anyhow::Result<reqwest::ClientBuilder> {
        let builder = match self.used_for {
            ClientUse::Fetch | ClientUse::Gateway => builder,
            ClientUse::Tracker { title_case } => {
                let builder = builder.pool_max_idle_per_host(0);
                if title_case {
                    builder.http1_title_case_headers()
                } else {
                    builder
                }
            }
        };
        Ok(match &self.tls {
            None => builder,
            Some(tls) => builder
                .identity(reqwest::Identity::from_pem(&tls.identity)?)
                .add_root_certificate(reqwest::Certificate::from_der(&tls.ca)?),
        })
    }
}

/// The session's HTTP clients, and the socket its port mapper asks the
/// gateway over. The embedder builds each: the session never does.
pub trait HttpClients: Send + Sync + 'static {
    /// The client of `shape` for `target`, finished. With no target, one
    /// that resolves names and follows redirects itself: metainfo, the
    /// block and allow lists. With a target, one that reaches
    /// `target.host` at `target.addrs` and nowhere else, and follows no
    /// redirect; through a proxy that is handed the name, the proxy
    /// reaches it. The embedder builds one for each target and shape, and
    /// hands that one back each time it is asked again.
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client>;

    /// Where a request from these clients to `to` leaves. Clients that do
    /// not say are a proxy's: the proxy picks how it reaches `to`.
    fn egress(&self, _to: SocketAddr) -> Egress {
        Egress::Proxied
    }

    /// A datagram socket bound at `local`, the address a port mapper asks
    /// the gateway from, on the network the gateway is reached on.
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

    /// The socket a UPnP search goes out on: bound at `from`, an address of
    /// the interface the gateway is on, its multicast sent out of that
    /// interface. By default a host socket; an embedder whose sockets keep
    /// to a route gives one bound as its own are.
    fn ssdp_socket<'a>(
        &'a self,
        from: std::net::Ipv4Addr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        Box::pin(async move {
            Ok(Arc::new(crate::upnp::host_ssdp_socket(from)?) as Arc<dyn DatagramSocket>)
        })
    }
}

/// The clients of a session given none: it makes no HTTP request, so it
/// fetches nothing by URL and announces to no HTTP tracker. Where its
/// traffic leaves, and the socket its port mapper asks the gateway from,
/// are its sockets' to say.
#[derive(Clone)]
pub struct NoClients {
    sockets: Arc<dyn crate::network::SocketFactory>,
}

impl NoClients {
    /// The clients of a session whose sockets come from `sockets`.
    pub fn over(sockets: Arc<dyn crate::network::SocketFactory>) -> Self {
        Self { sockets }
    }
}

impl std::fmt::Debug for NoClients {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoClients").finish_non_exhaustive()
    }
}

impl HttpClients for NoClients {
    fn client(
        &self,
        _target: Option<HttpTarget<'_>>,
        _shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        anyhow::bail!("the session was given no HTTP clients")
    }

    fn egress(&self, to: SocketAddr) -> Egress {
        match self.sockets.source_toward(to) {
            Some(Ok(ip)) => Egress::From(ip),
            Some(Err(error)) => {
                tracing::debug!(%to, %error, "the sockets have no route toward the address");
                Egress::NoRoute
            }
            None => Egress::Proxied,
        }
    }

    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.sockets.gateway_socket(local)
    }
}

/// Where a request to an address leaves, as libtorrent asks of each listen
/// socket before it announces from it (`listen_socket_t::can_route`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Egress {
    /// A proxy dials the address, from wherever it exits.
    Proxied,
    /// The host dials it, from this local address.
    From(IpAddr),
    /// The host has no route to it.
    NoRoute,
}

#[cfg(test)]
mod tests {
    use super::{NamedBy, is_local_name};

    #[test]
    fn plainly_local_names_are_local() {
        for local in [
            "localhost",
            "LOCALHOST.",
            "a.localhost",
            "nas",
            "NAS",
            "nas.",
            "nas.lan",
            "NAS.LAN.",
            "printer.local.",
            "router.home.arpa",
            "home.arpa",
            "db.corp.internal",
        ] {
            assert!(is_local_name(local), "{local}");
            assert!(!NamedBy::Torrent.may_ask_by_name(local), "{local}");
            assert!(NamedBy::Operator.may_ask_by_name(local), "{local}");
        }
    }

    #[test]
    fn public_looking_names_are_not_local() {
        for public in [
            "tracker.example.org",
            "tracker.example.org.",
            "seed.test",
            "planlan.org",
            "xlocal.net",
            "localhost.example.com",
            "internal.example.com",
            "myhome.arpa",
        ] {
            assert!(!is_local_name(public), "{public}");
            assert!(NamedBy::Torrent.may_ask_by_name(public), "{public}");
        }
    }
}

#[cfg(all(test, feature = "e2e-local-trackers"))]
mod e2e_local_tracker_tests {
    use super::NamedBy;
    use std::net::IpAddr;

    #[test]
    fn an_e2e_build_announces_to_a_torrents_tracker_at_any_address() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        for any in [
            "127.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "10.89.2.7",
            "172.16.0.1",
            "192.168.127.254",
            "fd00::1",
            "169.254.1.1",
            "fe80::1",
            "100.64.0.1",
            "8.8.8.8",
        ] {
            assert!(NamedBy::Torrent.may_announce_to(ip(any)), "{any}");
        }
        // Web seeds keep the public-only rule.
        for local in ["127.0.0.1", "10.89.2.7", "fd00::1", "169.254.1.1"] {
            assert!(!NamedBy::Torrent.may_ask(ip(local)), "{local}");
        }
    }
}
