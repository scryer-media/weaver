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
    task::{Context, Poll},
};

/// A boxed future that is both `Send` and `Sync`, as required by the uTP
/// transport abstraction this trait is adapted to.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + Sync + 'a>>;

/// An unconnected, message-oriented socket: the shape of a UDP socket, without
/// requiring it to be one.
///
/// Used for DHT, UDP trackers and the uTP peer transport.
pub trait DatagramSocket: Send + Sync + 'static {
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
    /// be asked, and, in an e2e build, a torrent's tracker at a local
    /// address too, so the e2e rigs can serve real trackers on loopback and
    /// container networks. Web seeds stay under [`NamedBy::may_ask`].
    pub fn may_announce_to(self, ip: IpAddr) -> bool {
        #[cfg(feature = "e2e-local-trackers")]
        if is_local(ip) {
            return true;
        }
        self.may_ask(ip)
    }
}

/// Whether `ip` is loopback, private IPv4 or unique-local IPv6.
#[cfg(feature = "e2e-local-trackers")]
fn is_local(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(a) => a.is_loopback() || a.is_private(),
        IpAddr::V6(a) => a.is_loopback() || a.is_unique_local(),
    }
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

/// Builds the session's HTTP clients.
pub trait HttpClients: Send + Sync + 'static {
    /// With no target, the builder of the client for requests it resolves
    /// and redirects itself: HTTP trackers, metainfo, the block and allow
    /// lists. With a target, of a client that reaches `target.host` at
    /// `target.addrs` and nowhere else, and follows no redirect; through a
    /// proxy that is handed the name, the proxy reaches it. An SSL torrent's
    /// announces add its TLS identity to it.
    fn builder(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::ClientBuilder>;

    /// The client [`Self::builder`] gives.
    fn client(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::Client> {
        Ok(self.builder(target)?.build()?)
    }
}

/// The clients a builder makes, each held to its target.
impl<F> HttpClients for F
where
    F: Fn() -> reqwest::ClientBuilder + Send + Sync + 'static,
{
    fn builder(&self, target: Option<HttpTarget<'_>>) -> anyhow::Result<reqwest::ClientBuilder> {
        Ok(for_target(self(), target))
    }
}

/// `builder` held to `target`: it follows no redirect, and dials the
/// target's host at the target's addresses only.
pub fn for_target(
    builder: reqwest::ClientBuilder,
    target: Option<HttpTarget<'_>>,
) -> reqwest::ClientBuilder {
    let Some(target) = target else {
        return builder;
    };
    let builder = builder.redirect(reqwest::redirect::Policy::none());
    // An address in the URL is dialed as it is.
    if target.host.parse::<IpAddr>().is_ok() {
        return builder;
    }
    let addrs: Vec<SocketAddr> = target
        .addrs
        .iter()
        .map(|ip| SocketAddr::new(*ip, target.port))
        .collect();
    builder.resolve_to_addrs(target.host, &addrs)
}

#[cfg(all(test, feature = "e2e-local-trackers"))]
mod e2e_local_tracker_tests {
    use super::NamedBy;
    use std::net::IpAddr;

    #[test]
    fn an_e2e_build_announces_to_a_torrents_tracker_at_local_addresses_only() {
        let ip = |s: &str| s.parse::<IpAddr>().unwrap();
        for local in [
            "127.0.0.1",
            "::1",
            "::ffff:127.0.0.1",
            "10.89.2.7",
            "172.16.0.1",
            "192.168.127.254",
            "fd00::1",
        ] {
            assert!(NamedBy::Torrent.may_announce_to(ip(local)), "{local}");
            // Web seeds keep the public-only rule.
            assert!(!NamedBy::Torrent.may_ask(ip(local)), "{local}");
        }
        for refused in [
            "169.254.1.1",
            "fe80::1",
            "0.0.0.0",
            "224.0.0.1",
            "192.0.2.1",
            "100.64.0.1",
        ] {
            assert!(!NamedBy::Torrent.may_announce_to(ip(refused)), "{refused}");
        }
        assert!(NamedBy::Torrent.may_announce_to(ip("8.8.8.8")));
    }
}
