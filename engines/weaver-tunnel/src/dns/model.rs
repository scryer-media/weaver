// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The DNS client's own words: the servers it asks and how, how their
// answers are judged, what it says when it fails, what one family's query
// found, and what carries a query.
use crate::{
    TunnelError, TunnelStream,
    pipe::{DialError, Dialer, KeeperRef, Purpose, Reservation, Target},
};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// One DNS server Weaver asks, and how it is asked.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DnsServer {
    pub address: SocketAddr,
    pub wire: Wire,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Wire {
    // DNS over TCP, in the clear. Port 53 by default.
    Plain,
    // DNS over TLS (RFC 7858). Port 853 by default. The certificate is
    // checked against the server's address, or against `name` when one is
    // given.
    Tls { name: Option<String> },
    // DNS over HTTPS (RFC 8484), POST to `path` (starts with '/') at the
    // server's address. Port 443 by default. With a `name`, the certificate
    // is checked against it and it is sent as the request's host, as the
    // URL named it; the name is never looked up, the address is dialled.
    // Without one, the address is both.
    Https { path: String, name: Option<String> },
}

// Which wire a server is asked over, without its settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum WireKind {
    Plain,
    Tls,
    Https,
}

impl Wire {
    pub fn kind(&self) -> WireKind {
        match self {
            Self::Plain => WireKind::Plain,
            Self::Tls { .. } => WireKind::Tls,
            Self::Https { .. } => WireKind::Https,
        }
    }
}

impl WireKind {
    pub const ALL: [Self; 3] = [Self::Plain, Self::Tls, Self::Https];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Plain => "plain",
            Self::Tls => "tls",
            Self::Https => "https",
        }
    }

    // The port a server on this wire listens on unless told otherwise.
    pub fn default_port(self) -> u16 {
        match self {
            Self::Plain => 53,
            Self::Tls => 853,
            Self::Https => 443,
        }
    }
}

impl DnsServer {
    // A plain server on port 53.
    pub fn plain(ip: IpAddr) -> Self {
        Self {
            address: SocketAddr::new(ip, WireKind::Plain.default_port()),
            wire: Wire::Plain,
        }
    }
}

// The server's address, without the port when it is the wire's default.
impl std::fmt::Display for DnsServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.address.port() == self.wire.kind().default_port() {
            write!(f, "{}", self.address.ip())
        } else {
            write!(f, "{}", self.address)
        }
    }
}

// How answers from listed servers are judged.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Trust {
    #[default]
    AsSent,
    // Check the DNSSEC signatures here: unsigned names resolve, names that
    // fail the check do not.
    Validate,
}

// What routed DNS says when it fails, and `fault` reads back.
pub(crate) const INVALID: &str = "routed DNS returned an invalid response";
pub(crate) const UNRESOLVED: &str = "routed DNS could not resolve the destination";
// A lookup whose answers failed the DNSSEC check, or came without the
// records to check, under `Validate`.
pub(crate) const NOT_VALIDATED: &str =
    "routed DNS could not resolve the destination: no answer passed DNSSEC validation";

// Ceiling on what one listed server is asked for one lookup: the dial, TLS,
// both address families with the aliases they follow, and under `Validate`
// every query of the DNSSEC check. Each server asked gets all of it, however
// long the servers before it took. When it runs out the next server is asked.
pub(super) const SERVER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// The address families a lookup asks for: A, then AAAA.
pub(super) const FAMILIES: [u16; 2] = [1, 28];

// What a query is asked over.
#[async_trait::async_trait]
pub(super) trait Carrier: Send + Sync {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn TunnelStream>, TunnelError>;

    // As `open`, with the bytes the stream's sockets reserved in each
    // session's share, which a connection kept for later is reclaimed at.
    // None by default: what keeps it is reclaimed only by a cut.
    async fn open_reserving(&self, server: &DnsServer) -> Result<Opened, TunnelError> {
        Ok(Opened {
            stream: self.open(server).await?,
            reserved: Vec::new(),
        })
    }

    // The keepers of the stages a connection opened here rides, the
    // dialling stage's first, which a connection kept for later is tied to.
    // None keeps nothing.
    fn keepers(&self) -> Vec<KeeperRef> {
        Vec::new()
    }
}

// A pipe stage carrying the queries. It keeps why it could not reach a
// server, so a lookup that reached none fails as the stage failed.
pub(super) struct Stage<'a> {
    pub(super) dialer: &'a dyn Dialer,
    pub(super) failure: Mutex<Option<DialError>>,
}

#[async_trait::async_trait]
impl Carrier for Stage<'_> {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn TunnelStream>, TunnelError> {
        Ok(self.open_reserving(server).await?.stream)
    }

    async fn open_reserving(&self, server: &DnsServer) -> Result<Opened, TunnelError> {
        let ip = server.address.ip();
        let target = Target {
            host: ip.to_string(),
            port: server.address.port(),
            purpose: Purpose::Dns,
            addresses: vec![ip],
        };
        match self.dialer.dial(&target).await {
            Ok(dialed) => {
                let reserved = dialed.outcome.reservations();
                Ok(Opened {
                    stream: dialed.into_observed_stream(),
                    reserved,
                })
            }
            Err(error) => {
                let said = TunnelError::Engine(error.to_string());
                *self.failure.lock().expect("dns stage failure") = Some(error);
                Err(said)
            }
        }
    }

    fn keepers(&self) -> Vec<KeeperRef> {
        self.dialer.keepers()
    }
}

// A stream a carrier opened, and the bytes its sockets reserved, by share.
pub(super) struct Opened {
    pub(super) stream: Box<dyn TunnelStream>,
    pub(super) reserved: Vec<Reservation>,
}

impl AsyncRead for Opened {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for Opened {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write_vectored(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        self.stream.is_write_vectored()
    }
}

// The failure of a lookup that found no address for the destination.
pub fn unresolved() -> DialError {
    DialError::Destination(std::io::Error::other(TunnelError::Engine(
        UNRESOLVED.into(),
    )))
}

// What one family's query found, and for how long that holds: the
// shortest TTL of the records the answer used, or for no address the
// zone's negative TTL. None for no address and no SOA to give one: such an
// answer is given but held for no time, as dnsmasq holds none by default.
pub(super) struct Found {
    pub(super) addresses: Vec<IpAddr>,
    pub(super) ttl: Option<u32>,
    // The server said the name does not exist.
    pub(super) nonexistent: bool,
    // Under `Validate`, why the answer could not be checked: the name is not
    // signed. None when it was checked, or not under `Validate`.
    pub(super) insecure: Option<weaver_dnssec::InsecureReason>,
}
