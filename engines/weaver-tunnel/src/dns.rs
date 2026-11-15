// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Bounded DNS-over-TCP to listed servers, asked through a pipe stage: the
// leg's egress or a proxy hop.
use crate::{
    TunnelError, TunnelStream,
    pipe::{DialError, Dialer, Purpose, Target},
    resolution::{
        Lookup, Ttl,
        record::{self, LookupFailure, TrustOutcome},
    },
};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, Ordering};
pub use wire::DnsSessions;
#[cfg(test)]
use wire::WIRES;
use wire::{Session, Wires};

mod https;
mod wire;

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
    // server's address. Port 443 by default.
    Https { path: String },
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
    // Refuse an answer the server did not mark authenticated (the AD bit).
    RequireAd,
}

// What routed DNS says when it fails, and `fault` reads back.
pub(crate) const INVALID: &str = "routed DNS returned an invalid response";
pub(crate) const UNRESOLVED: &str = "routed DNS could not resolve the destination";
// A lookup whose servers gave no answer marked authenticated. It reads as
// unresolved, and says why, so a caller that joined the lookup can tell.
pub(crate) const NOT_AUTHENTICATED: &str =
    "routed DNS could not resolve the destination: no answer was marked authenticated";

fn invalid() -> TunnelError {
    TunnelError::Engine(INVALID.into())
}
static NEXT_ID: AtomicU16 = AtomicU16::new(1);

// Ceiling on one address-family query, dial and CNAME walk included.
const FAMILY_QUERY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

// What a query is asked over.
#[async_trait::async_trait]
trait Carrier: Send + Sync {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn TunnelStream>, TunnelError>;
}

// Why no server gave an answer. `reached` says whether any server could be
// dialled at all.
#[derive(Debug)]
struct Unanswered {
    error: TunnelError,
    reached: bool,
}

// A pipe stage carrying the queries. It keeps why it could not reach a
// server, so a lookup that reached none fails as the stage failed.
struct Stage<'a> {
    dialer: &'a dyn Dialer,
    failure: Mutex<Option<DialError>>,
}

#[async_trait::async_trait]
impl Carrier for Stage<'_> {
    async fn open(&self, server: &DnsServer) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let ip = server.address.ip();
        let target = Target {
            host: ip.to_string(),
            port: server.address.port(),
            purpose: Purpose::Dns,
            addresses: vec![ip],
        };
        match self.dialer.dial(&target).await {
            Ok(dialed) => Ok(dialed.into_observed_stream()),
            Err(error) => {
                let said = TunnelError::Engine(error.to_string());
                *self.failure.lock().expect("dns stage failure") = Some(error);
                Err(said)
            }
        }
    }
}

// The failure of a lookup that found no address for the destination.
pub fn unresolved() -> DialError {
    DialError::Destination(std::io::Error::other(TunnelError::Engine(
        UNRESOLVED.into(),
    )))
}

// Resolves `host` by asking `servers` in order through `dialer`. When no
// server could be reached at all, the error is the stage's own, so a dead
// path counts against the path and not against the name. Every server
// saying the name has no address is a negative answer: no addresses, with
// how long that holds. `trust` is how the answers are to be judged. TLS
// sessions are not resumed from one call to the next.
pub async fn resolve_through(
    dialer: &dyn Dialer,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> Result<Lookup, DialError> {
    resolve_routed(dialer, &DnsSessions::default(), servers, trust, host).await
}

// `resolve_through`, resuming the TLS sessions of one naming.
pub(crate) async fn resolve_routed(
    dialer: &dyn Dialer,
    sessions: &DnsSessions,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> Result<Lookup, DialError> {
    if let Ok(ip) = host.parse() {
        return Ok(Lookup {
            addresses: vec![ip],
            ttl: Ttl::Unknown,
            nonexistent: false,
        });
    }
    let stage = Stage {
        dialer,
        failure: Mutex::new(None),
    };
    let resolved = resolve(&stage, sessions.wires(), servers, trust, host).await;
    crate::metrics::record_resolution(
        crate::metrics::Resolver::Routed,
        resolved
            .as_ref()
            .is_ok_and(|lookup| !lookup.addresses.is_empty()),
    );
    resolved.map_err(|unanswered| {
        let failure = stage.failure.into_inner().expect("dns stage failure");
        match failure {
            Some(failure) if !unanswered.reached => failure,
            _ => DialError::Destination(std::io::Error::other(unanswered.error)),
        }
    })
}

// What one family's query found, and for how long that holds: the
// shortest TTL of the records the answer used, or for no address the
// zone's negative TTL, when the answer gives one.
struct Found {
    addresses: Vec<IpAddr>,
    ttl: Option<u32>,
    // The server said the name does not exist.
    nonexistent: bool,
}

// Why one query got no answer.
enum Failed {
    // The server could not be reached.
    Unreached(TunnelError),
    // The connection closed before any byte of the reply came.
    Closed,
    // The connection closed part way through the reply.
    Cut,
    // The server answered with this error code instead of an answer.
    Rcode(u16),
    Invalid,
    // The naming requires the AD bit and the reply did not carry it.
    NotAuthenticated,
    // The TLS handshake or certificate check failed, for this fixed reason.
    Tls(&'static str),
    // The HTTPS server answered with this status instead of 200.
    Http(u16),
}

// Why a lookup found nothing, from what each server it asked said: the
// most telling answer any server gave.
#[derive(Default)]
struct Why(Option<(LookupFailure, String)>);

impl Why {
    fn rank(failure: LookupFailure) -> u8 {
        match failure {
            LookupFailure::NotAuthenticated => 7,
            LookupFailure::NoSuchName => 6,
            LookupFailure::NoAddress => 5,
            LookupFailure::TlsFailed => 4,
            LookupFailure::ServerFailure => 3,
            LookupFailure::TimedOut => 2,
            LookupFailure::Other => 1,
            LookupFailure::NoReply => 0,
        }
    }
    fn consider(&mut self, failure: LookupFailure, detail: impl FnOnce() -> String) {
        if self
            .0
            .as_ref()
            .is_none_or(|(held, _)| Self::rank(failure) > Self::rank(*held))
        {
            self.0 = Some((failure, detail()));
        }
    }
    // What one family's query came to at `server`.
    fn query(&mut self, server: &DnsServer, answered: &Result<Result<Found, Failed>, impl Sized>) {
        match answered {
            Ok(Ok(found)) if found.addresses.is_empty() && found.nonexistent => {
                self.consider(LookupFailure::NoSuchName, || format!("{server}: NXDOMAIN"))
            }
            Ok(Ok(found)) if found.addresses.is_empty() => {
                self.consider(LookupFailure::NoAddress, || format!("{server}: NODATA"))
            }
            Ok(Ok(_)) => {}
            // The stage's own words, as it failed to dial the server.
            Ok(Err(Failed::Unreached(error))) => {
                self.consider(LookupFailure::NoReply, || match error {
                    TunnelError::Engine(said) => said.clone(),
                    error => error.to_string(),
                })
            }
            Ok(Err(Failed::Closed)) => self.consider(LookupFailure::NoReply, || {
                format!("{server}: the connection closed without a reply")
            }),
            Ok(Err(Failed::Cut)) => self.consider(LookupFailure::NoReply, || {
                format!("{server}: the connection closed part way through a reply")
            }),
            Ok(Err(Failed::Rcode(rcode))) => self.consider(LookupFailure::ServerFailure, || {
                let said = match rcode {
                    2 => "SERVFAIL".to_owned(),
                    5 => "REFUSED".to_owned(),
                    rcode => format!("RCODE {rcode}"),
                };
                format!("{server}: {said}")
            }),
            Ok(Err(Failed::Invalid)) => {
                self.consider(LookupFailure::Other, || format!("{server}: {INVALID}"))
            }
            Ok(Err(Failed::NotAuthenticated)) => self
                .consider(LookupFailure::NotAuthenticated, || {
                    format!("{server}: the answer was not marked authenticated")
                }),
            Ok(Err(Failed::Tls(reason))) => self.consider(LookupFailure::TlsFailed, || {
                format!("{server}: TLS failed: {reason}")
            }),
            Ok(Err(Failed::Http(status))) => self.consider(LookupFailure::ServerFailure, || {
                format!("{server}: HTTP {status}")
            }),
            Err(_) => self.consider(LookupFailure::TimedOut, || {
                format!(
                    "{server}: no answer within {}s",
                    FAMILY_QUERY_TIMEOUT.as_secs()
                )
            }),
        }
    }
    // Notes why `host` found nothing in the test recording this lookup runs
    // in, if any.
    fn note(self, host: &str) {
        if let Some((failure, detail)) = self.0 {
            record::failed(host, failure, || detail);
        }
    }
}

// Every server plain and every answer taken as sent, as most tests ask.
#[cfg(test)]
async fn resolve_name(
    provider: &dyn Carrier,
    servers: &[DnsServer],
    host: &str,
) -> Result<Lookup, Unanswered> {
    resolve(provider, &WIRES, servers, Trust::AsSent, host).await
}

// How an answer the lookup used is judged.
fn judged(trust: Trust) -> TrustOutcome {
    match trust {
        Trust::AsSent => TrustOutcome::AsSent,
        Trust::RequireAd => TrustOutcome::Authenticated,
    }
}

// A lookup that came to an answer, positive or negative: the wire of the
// server it came from, and how it was judged.
fn settled(host: &str, wire: WireKind, trust: Trust) {
    record::answered(host, Some(wire), judged(trust));
    crate::metrics::record_dns_wire(wire);
    crate::metrics::record_dns_trust(judged(trust));
}

async fn resolve(
    provider: &dyn Carrier,
    wires: &Wires,
    servers: &[DnsServer],
    trust: Trust,
    host: &str,
) -> Result<Lookup, Unanswered> {
    if servers.is_empty() {
        record::failed(host, LookupFailure::Other, || {
            "no DNS servers to ask".into()
        });
        return Err(Unanswered {
            error: TunnelError::Configuration("no DNS servers to ask".into()),
            reached: false,
        });
    }
    let mut why = Why::default();
    let mut reached_any = false;
    // How long the name's having no address holds, while every server
    // asked has said so for both families.
    let mut negative = Some(u32::MAX);
    let mut encrypted_before = false;
    for server in servers {
        if never_in_plain(server, encrypted_before) {
            why.consider(LookupFailure::Other, || {
                format!("{server}: not asked in plain after an encrypted server")
            });
            continue;
        }
        encrypted_before |= server.wire != Wire::Plain;
        // An encrypted wire keeps one connection to the server for both
        // families and every alias followed.
        let mut session = Session::new(provider, wires, server);
        let mut addresses = Vec::new();
        // The shortest TTL either family's answer gave.
        let mut ttl: Option<u32> = None;
        let mut said_none = 0;
        for qtype in [1u16, 28] {
            // A resolver may answer for only one family. A failing or silent
            // peer query must neither erase the answers already collected nor
            // stop the other family from being asked, and its own wait is
            // bounded so it cannot spend the caller's whole deadline.
            //
            // A route that cannot reach the server at all is different: the
            // other family would only dial the same dead route again, and the
            // caller's ladder moves on sooner for being told once.
            // `query` marks `reached` as soon as the server answers the
            // dial, so a query cut off by the ceiling still says so.
            let mut reached = false;
            let answered = tokio::time::timeout(
                FAMILY_QUERY_TIMEOUT,
                query(&mut session, host, qtype, trust, &mut reached),
            )
            .await;
            reached_any |= reached;
            why.query(server, &answered);
            match answered {
                Ok(Ok(found)) => {
                    if let Some(held) = found.ttl {
                        ttl = Some(ttl.map_or(held, |ttl| ttl.min(held)));
                        said_none += usize::from(found.addresses.is_empty());
                    }
                    addresses.extend(found.addresses);
                }
                _ if !reached => break,
                // The other family would only fail the same handshake.
                Ok(Err(Failed::Tls(_))) => break,
                _ => {}
            }
        }
        if !addresses.is_empty() {
            addresses.sort();
            addresses.dedup();
            settled(host, server.wire.kind(), trust);
            return Ok(Lookup {
                addresses,
                ttl: ttl.map_or(Ttl::Unknown, Ttl::Seconds),
                nonexistent: false,
            });
        }
        negative = negative
            .zip(ttl)
            .filter(|_| said_none == 2)
            .map(|(negative, ttl)| negative.min(ttl));
    }
    let nonexistent = matches!(why.0, Some((LookupFailure::NoSuchName, _)));
    let refused = matches!(why.0, Some((LookupFailure::NotAuthenticated, _)));
    why.note(host);
    match negative {
        // Every server said so; the first is named as the one that answered.
        Some(ttl) => {
            settled(host, servers[0].wire.kind(), trust);
            Ok(Lookup {
                addresses: Vec::new(),
                ttl: Ttl::Seconds(ttl),
                nonexistent,
            })
        }
        None => {
            // A lookup with no authenticated answer fails. It is never a
            // negative answer, so nothing holds it.
            if refused {
                record::answered(host, None, TrustOutcome::Refused);
                crate::metrics::record_dns_trust(TrustOutcome::Refused);
            }
            Err(Unanswered {
                error: TunnelError::Engine(
                    if refused {
                        NOT_AUTHENTICATED
                    } else {
                        UNRESOLVED
                    }
                    .into(),
                ),
                reached: reached_any,
            })
        }
    }
}

// No downgrade (SECURITY.md): within one lookup, a plain server listed after
// an encrypted one is never asked, whatever came of the encrypted ones, so a
// name meant to go out encrypted never goes out in the clear. A plain server
// listed before them is asked in its turn.
fn never_in_plain(server: &DnsServer, encrypted_before: bool) -> bool {
    encrypted_before && server.wire == Wire::Plain
}

// Whether a reply carries the AD bit: the server says it validated the
// answer (RFC 4035 §3.2.3).
fn authenticated(bytes: &[u8]) -> bool {
    u16_at(bytes, 2).is_ok_and(|flags| flags & 0x0020 != 0)
}

async fn query(
    session: &mut Session<'_>,
    host: &str,
    qtype: u16,
    trust: Trust,
    reached: &mut bool,
) -> Result<Found, Failed> {
    let mut name = host.trim_end_matches('.').to_ascii_lowercase();
    // The shortest TTL of the aliases followed so far.
    let mut ttl = u32::MAX;
    for _ in 0..8 {
        // DNS over HTTPS sends id 0, so a cache between can serve it (RFC
        // 8484 §4.1).
        let id = match session.server().wire {
            Wire::Https { .. } => 0,
            Wire::Plain | Wire::Tls { .. } => NEXT_ID.fetch_add(1, Ordering::Relaxed),
        };
        let mut packet = Vec::from(id.to_be_bytes());
        // Recursion desired, and under `RequireAd` the AD bit, which asks
        // the server to say whether it validated the answer (RFC 6840 §5.7).
        let ad = match trust {
            Trust::AsSent => 0,
            Trust::RequireAd => 0x20,
        };
        packet.extend_from_slice(&[1, ad, 0, 1, 0, 0, 0, 0, 0, 0]);
        if name.is_empty() || name.len() > 253 {
            return Err(Failed::Invalid);
        }
        for label in name.split('.') {
            if label.is_empty() || label.len() > 63 || !label.is_ascii() {
                return Err(Failed::Invalid);
            }
            packet.push(label.len() as u8);
            packet.extend_from_slice(label.as_bytes());
        }
        packet.push(0);
        packet.extend_from_slice(&qtype.to_be_bytes());
        packet.extend_from_slice(&[0, 1]);
        let bytes = session.ask(&packet, reached).await?;
        let reply = parse_response(&bytes, id, &name, qtype)
            .map_err(|_| refusal(&bytes, id).map_or(Failed::Invalid, Failed::Rcode))?;
        // Under `RequireAd` every reply the answer rests on must carry the
        // AD bit: each alias step, and a reply saying there is no address.
        if trust == Trust::RequireAd && !authenticated(&bytes) {
            return Err(Failed::NotAuthenticated);
        }
        ttl = ttl.min(reply.ttl);
        if !reply.addresses.is_empty() {
            return Ok(Found {
                addresses: reply.addresses,
                ttl: Some(ttl),
                nonexistent: false,
            });
        }
        match reply.alias {
            Some(alias) => name = alias,
            None => {
                return Ok(Found {
                    addresses: Vec::new(),
                    ttl: reply.negative.map(|negative| negative.min(ttl)),
                    nonexistent: reply.nonexistent,
                });
            }
        }
    }
    Err(Failed::Invalid)
}

// The error code a reply to query `id` carries in place of an answer, when
// it is one: the server could not, or would not, resolve the name.
fn refusal(bytes: &[u8], id: u16) -> Option<u16> {
    let flags = u16_at(bytes, 2).ok()?;
    let rcode = flags & 0x000f;
    (u16_at(bytes, 0).ok()? == id && flags & 0x8000 != 0 && !matches!(rcode, 0 | 3))
        .then_some(rcode)
}

fn u16_at(bytes: &[u8], at: usize) -> Result<u16, TunnelError> {
    Ok(u16::from_be_bytes(
        bytes
            .get(at..at + 2)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}

fn u32_at(bytes: &[u8], at: usize) -> Result<u32, TunnelError> {
    Ok(u32::from_be_bytes(
        bytes
            .get(at..at + 4)
            .ok_or_else(invalid)?
            .try_into()
            .map_err(|_| invalid())?,
    ))
}

fn read_name(bytes: &[u8], cursor: &mut usize) -> Result<String, TunnelError> {
    let mut at = *cursor;
    let mut jumped = false;
    let mut labels = Vec::new();
    let mut length = 0;
    for _ in 0..128 {
        let head = *bytes.get(at).ok_or_else(invalid)?;
        if head & 0xc0 == 0xc0 {
            let target = (u16_at(bytes, at)? & 0x3fff) as usize;
            if target >= at {
                return Err(invalid());
            }
            if !jumped {
                *cursor = at + 2;
                jumped = true;
            }
            at = target;
            continue;
        }
        if head & 0xc0 != 0 {
            return Err(invalid());
        }
        at += 1;
        if head == 0 {
            if !jumped {
                *cursor = at;
            }
            return Ok(labels.join("."));
        }
        let label = bytes.get(at..at + head as usize).ok_or_else(invalid)?;
        if !label.is_ascii() {
            return Err(invalid());
        }
        length += label.len() + 1;
        if length > 254 {
            return Err(invalid());
        }
        labels.push(
            std::str::from_utf8(label)
                .map_err(|_| invalid())?
                .to_ascii_lowercase(),
        );
        at += head as usize;
    }
    Err(invalid())
}

// One resource record: its owner, type, class and TTL, and where its data
// lies in the message.
struct Record {
    owner: String,
    kind: u16,
    class: u16,
    ttl: u32,
    data: usize,
    size: usize,
}

fn record(bytes: &[u8], at: &mut usize) -> Result<Record, TunnelError> {
    let owner = read_name(bytes, at)?;
    let kind = u16_at(bytes, *at)?;
    let class = u16_at(bytes, *at + 2)?;
    // A TTL with the top bit set is read as zero (RFC 2181 §8).
    let ttl = match u32_at(bytes, *at + 4)? {
        ttl if ttl > i32::MAX as u32 => 0,
        ttl => ttl,
    };
    let size = u16_at(bytes, *at + 8)? as usize;
    let data = *at + 10;
    bytes.get(data..data + size).ok_or_else(invalid)?;
    *at = data + size;
    Ok(Record {
        owner,
        kind,
        class,
        ttl,
        data,
        size,
    })
}

// What one response said about the name asked.
struct Reply {
    addresses: Vec<IpAddr>,
    // The name the answer ended at, when it is an alias still to ask for.
    alias: Option<String>,
    // The shortest TTL of the records the answer used.
    ttl: u32,
    // For no address, how long that holds: the zone's negative TTL, from
    // the SOA in the authority section, when there is one.
    negative: Option<u32>,
    // The server said the name does not exist.
    nonexistent: bool,
}

fn parse_response(bytes: &[u8], id: u16, host: &str, qtype: u16) -> Result<Reply, TunnelError> {
    let flags = u16_at(bytes, 2)?;
    // A whole reply to a standard query that answers, or says the name does
    // not exist.
    let rcode = flags & 0x000f;
    if u16_at(bytes, 0)? != id
        || flags & 0xfa00 != 0x8000
        || !matches!(rcode, 0 | 3)
        || u16_at(bytes, 4)? != 1
    {
        return Err(invalid());
    }
    let count = u16_at(bytes, 6)? as usize;
    let authority = u16_at(bytes, 8)? as usize;
    if count > 128 || authority > 128 {
        return Err(invalid());
    }
    let mut at = 12;
    if read_name(bytes, &mut at)? != host
        || u16_at(bytes, at)? != qtype
        || u16_at(bytes, at + 2)? != 1
    {
        return Err(invalid());
    }
    at += 4;
    let mut records = Vec::new();
    let mut aliases = Vec::new();
    for _ in 0..count {
        let record = record(bytes, &mut at)?;
        if record.class != 1 {
            continue;
        }
        let data = &bytes[record.data..record.data + record.size];
        match (record.kind, record.size) {
            (1, 4) => records.push((
                record.owner,
                IpAddr::V4(Ipv4Addr::new(data[0], data[1], data[2], data[3])),
                record.ttl,
            )),
            (28, 16) => records.push((
                record.owner,
                IpAddr::V6(Ipv6Addr::from(
                    <[u8; 16]>::try_from(data).map_err(|_| invalid())?,
                )),
                record.ttl,
            )),
            (5, _) => {
                let mut cursor = record.data;
                let alias = read_name(bytes, &mut cursor)?;
                if cursor != record.data + record.size {
                    return Err(invalid());
                }
                aliases.push((record.owner, alias, record.ttl));
            }
            _ => {}
        }
    }
    let mut canonical = host.to_owned();
    let mut ttl = u32::MAX;
    for _ in 0..8 {
        match aliases.iter().find(|(owner, ..)| owner == &canonical) {
            Some((_, alias, held)) => {
                canonical = alias.clone();
                ttl = ttl.min(*held);
            }
            None => break,
        }
    }
    if aliases.iter().any(|(owner, ..)| owner == &canonical) {
        return Err(invalid());
    }
    let mut addresses = Vec::new();
    for (owner, ip, held) in records {
        if owner == canonical && ip.is_ipv4() == (qtype == 1) {
            addresses.push(ip);
            ttl = ttl.min(held);
        }
    }
    // A name that does not exist leaves no alias to ask for.
    let alias = (canonical != host && rcode == 0).then_some(canonical);
    let negative = if addresses.is_empty() && alias.is_none() {
        negative_ttl(bytes, at, authority)
    } else {
        None
    };
    Ok(Reply {
        addresses,
        alias,
        ttl,
        negative,
        nonexistent: rcode == 3,
    })
}

// The negative TTL from the SOA among the `count` authority records at
// `at`: the lower of the record's own TTL and the SOA's minimum (RFC 2308
// §5). A malformed authority section gives none rather than failing the
// answer.
fn negative_ttl(bytes: &[u8], mut at: usize, count: usize) -> Option<u32> {
    for _ in 0..count {
        let record = record(bytes, &mut at).ok()?;
        if record.kind != 6 || record.class != 1 {
            continue;
        }
        let mut cursor = record.data;
        read_name(bytes, &mut cursor).ok()?;
        read_name(bytes, &mut cursor).ok()?;
        // Serial, refresh, retry and expire come before the minimum.
        if cursor + 20 != record.data + record.size {
            return None;
        }
        let minimum = u32_at(bytes, cursor + 16).ok()?;
        return Some(record.ttl.min(minimum));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    mod families;
    mod trust;
    mod ttl;
    mod wires;
    #[test]
    fn compression_cycles_and_truncation_are_rejected() {
        assert!(read_name(&[0xc0, 0], &mut 0).is_err());
        assert!(read_name(&[3, b'a'], &mut 0).is_err());
        assert!(parse_response(&[0; 12], 1, "test", 1).is_err());
    }
}
