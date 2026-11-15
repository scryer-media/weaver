// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How the names a test dial named were resolved.
//
// A test runs its dial inside a recording, and every lookup on that dial's
// path notes what it did into it: who was asked, what came back, whether
// the answer was held, and which address was connected. Only a recording
// collects notes. Every other dial runs outside one, so its notes go
// nowhere and it looks up nothing it would not have looked up anyway.
//
// The recording is a task-local of the test's own task. Lookups reach it
// from code with no handle to pass one along: a hop's naming, the shared
// caches, the DNS client, and the tunnel engines' callbacks. A lookup made
// in another task does not reach it; the paths a test dials make theirs in
// the task that dials.
use crate::dns::{DnsServer, Trust, WireKind};
use std::{
    collections::HashMap,
    future::Future,
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
};

#[cfg(test)]
#[path = "resolution_record_tests.rs"]
mod tests;

tokio::task_local! {
    static RECORDING: Arc<Recording>;
}

// Who resolved a name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ResolvedBy {
    // The name is an address; nothing was asked.
    Literal,
    // This machine's own DNS.
    Host,
    // DNS servers Weaver asked from the route's egress.
    Servers,
    // The proxy was handed the name and resolved it.
    Proxy,
    // DNS servers asked through the proxy.
    ProxyServers,
}

// Why a lookup found no address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LookupFailure {
    NoSuchName,
    NoAddress,
    TimedOut,
    // The server answered that it could not resolve the name.
    ServerFailure,
    // No server answered at all.
    NoReply,
    // The server answered without marking the answer authenticated, and the
    // naming requires it.
    NotAuthenticated,
    // The answer failed DNSSEC validation.
    Bogus,
    // The server returns no DNSSEC records, so nothing can be validated.
    DnssecUnavailable,
    // The TLS handshake or certificate check with the server failed.
    TlsFailed,
    Other,
}

impl LookupFailure {
    pub fn code(self) -> &'static str {
        match self {
            Self::NoSuchName => "NO_SUCH_NAME",
            Self::NoAddress => "NO_ADDRESS",
            Self::TimedOut => "TIMED_OUT",
            Self::ServerFailure => "SERVER_FAILURE",
            Self::NoReply => "NO_REPLY",
            Self::NotAuthenticated => "NOT_AUTHENTICATED",
            Self::Bogus => "BOGUS",
            Self::DnssecUnavailable => "DNSSEC_UNAVAILABLE",
            Self::TlsFailed => "TLS_FAILED",
            Self::Other => "OTHER",
        }
    }
}

// A failed lookup, with what the resolver said.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LookupError {
    pub failure: LookupFailure,
    pub detail: String,
}

// How the answer from listed servers was judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TrustOutcome {
    // Taken as the server sent it.
    AsSent,
    // The server marked it authenticated.
    Authenticated,
    // Refused, as the server did not mark it authenticated.
    Refused,
    // Weaver checked its DNSSEC signatures and they held.
    Validated,
    // The zone is not signed, so there was nothing to check.
    Unsigned,
    // Refused, as its DNSSEC signatures failed Weaver's check.
    Bogus,
}

impl TrustOutcome {
    pub const ALL: [Self; 6] = [
        Self::AsSent,
        Self::Authenticated,
        Self::Refused,
        Self::Validated,
        Self::Unsigned,
        Self::Bogus,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AsSent => "as_sent",
            Self::Authenticated => "authenticated",
            Self::Refused => "refused",
            Self::Validated => "validated",
            Self::Unsigned => "unsigned",
            Self::Bogus => "bogus",
        }
    }
}

// How the name a dial ended at was resolved.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostResolution {
    pub host: String,
    pub by: ResolvedBy,
    // The proxy that resolved the name, or that the servers were asked
    // through.
    pub proxy: Option<u32>,
    // The DNS servers asked.
    pub servers: Vec<IpAddr>,
    // The wire of the server that answered, or when that is not known the
    // first listed server's. None when no servers were asked.
    pub wire: Option<WireKind>,
    // The listed server whose answer was used. None when no server
    // answered, or none was asked. When every server said the name has no
    // address, the first is named.
    pub answered_by: Option<IpAddr>,
    // How the servers' answer was judged. None when no servers were asked,
    // or when no answer came to judge, whatever the naming's trust.
    pub trust: Option<TrustOutcome>,
    // What the name resolved to; none when a proxy resolved it.
    pub addresses: Vec<IpAddr>,
    // Whether the answer was one Weaver held from an earlier lookup.
    pub cached: bool,
    // The address connected to, when Weaver knows it.
    pub connected: Option<IpAddr>,
    pub error: Option<LookupError>,
}

// The listed server an answer came from, and the wire it was asked over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Answerer {
    pub(crate) address: IpAddr,
    pub(crate) wire: WireKind,
}

impl Answerer {
    pub(crate) fn of(server: &DnsServer) -> Self {
        Self {
            address: server.address.ip(),
            wire: server.wire.kind(),
        }
    }
}

// Who a name is asked of.
#[derive(Clone, Debug)]
pub(crate) struct Asker {
    by: ResolvedBy,
    proxy: Option<u32>,
    servers: Vec<IpAddr>,
    wire: Option<WireKind>,
    // None when no servers are asked.
    trust: Option<Trust>,
}

impl Asker {
    fn asking(by: ResolvedBy, proxy: Option<u32>, servers: &[DnsServer], trust: Trust) -> Self {
        Self {
            by,
            proxy,
            servers: servers.iter().map(|server| server.address.ip()).collect(),
            wire: servers.first().map(|server| server.wire.kind()),
            trust: servers.first().map(|_| trust),
        }
    }
    pub(crate) fn host() -> Self {
        Self::asking(ResolvedBy::Host, None, &[], Trust::AsSent)
    }
    pub(crate) fn servers(servers: &[DnsServer], trust: Trust) -> Self {
        Self::asking(ResolvedBy::Servers, None, servers, trust)
    }
    pub(crate) fn proxy(proxy: u32) -> Self {
        Self::asking(ResolvedBy::Proxy, Some(proxy), &[], Trust::AsSent)
    }
    pub(crate) fn proxy_servers(proxy: u32, servers: &[DnsServer], trust: Trust) -> Self {
        Self::asking(ResolvedBy::ProxyServers, Some(proxy), servers, trust)
    }
}

// What was noted of one name. The first note of each kind stands: the
// innermost lookup notes its answer before the callers above it see one.
#[derive(Default)]
struct Entry {
    asker: Option<Asker>,
    cached: Option<bool>,
    answer: Option<Result<Vec<IpAddr>, LookupError>>,
    connected: Option<IpAddr>,
    // The server whose answer was used.
    from: Option<Answerer>,
    // How the servers' answer was judged, by the lookup that asked them.
    judged: Option<TrustOutcome>,
}

// A datagram socket a tunnel engine bound to reach its peer.
struct Bound {
    peer: SocketAddr,
    // Whether the engine asked the tunnel beneath for the peer's address,
    // rather than this machine's DNS.
    carried: bool,
}

#[derive(Default)]
pub struct Recording {
    names: Mutex<HashMap<String, Entry>>,
    bound: Mutex<Vec<Bound>>,
}

// What the WireGuard engine says when it resolved its endpoint with this
// machine's DNS and found nothing.
const ENDPOINT_UNRESOLVED: &str = "the endpoint name could not be resolved: ";
const ENDPOINT_LATE: &str = "the endpoint name did not resolve in time";
const ENDPOINT_EMPTY: &str = "the endpoint name resolved to no addresses";

impl Recording {
    // Runs `dial` with this recording collecting its notes.
    pub async fn scope<F: Future>(self: &Arc<Self>, dial: F) -> F::Output {
        RECORDING.scope(self.clone(), dial).await
    }

    // The recording this task's lookups note into, when there is one, for
    // a stage that dials in a task of its own to carry along.
    pub fn current() -> Option<Arc<Self>> {
        RECORDING.try_with(Arc::clone).ok()
    }

    // Takes `other`'s notes. A note this recording already holds stands.
    pub fn adopt(&self, other: &Self) {
        let taken = std::mem::take(&mut *other.names.lock().expect("resolution recording"));
        let mut names = self.names.lock().expect("resolution recording");
        for (host, note) in taken {
            let entry = names.entry(host).or_default();
            if entry.asker.is_none() {
                entry.asker = note.asker;
            }
            entry.cached = entry.cached.or(note.cached);
            if entry.answer.is_none() {
                entry.answer = note.answer;
            }
            entry.connected = entry.connected.or(note.connected);
            entry.from = entry.from.or(note.from);
            entry.judged = entry.judged.or(note.judged);
        }
        let taken = std::mem::take(&mut *other.bound.lock().expect("resolution recording"));
        self.bound
            .lock()
            .expect("resolution recording")
            .extend(taken);
    }

    // How `host` was resolved for a dial to it at `port`, as far as the
    // notes tell. `failure` is what the dial said when it failed, which
    // names a lookup no note recorded. `None` when nothing tells.
    pub fn finish(&self, host: &str, port: u16, failure: Option<&str>) -> Option<HostResolution> {
        let names = self.names.lock().expect("resolution recording");
        let bound = self.bound.lock().expect("resolution recording");
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = bare.parse::<IpAddr>() {
            let connected = names
                .get(&key(bare))
                .and_then(|entry| entry.connected)
                .or_else(|| {
                    bound
                        .iter()
                        .rev()
                        .find(|bound| bound.peer == SocketAddr::new(ip, port))
                        .map(|bound| bound.peer.ip())
                });
            return Some(HostResolution {
                host: host.to_owned(),
                by: ResolvedBy::Literal,
                proxy: None,
                servers: Vec::new(),
                wire: None,
                answered_by: None,
                trust: None,
                addresses: vec![ip],
                cached: false,
                connected,
                error: None,
            });
        }
        let entry = names.get(&key(host));
        if let Some((entry, asker)) = entry.and_then(|entry| Some((entry, entry.asker.as_ref()?))) {
            let (addresses, error) = match (&entry.answer, asker.by) {
                (_, ResolvedBy::Proxy) => (Vec::new(), None),
                (Some(Ok(addresses)), _) => (addresses.clone(), None),
                (Some(Err(error)), _) => (Vec::new(), Some(error.clone())),
                // The dial gave up while the lookup was still out.
                (None, _) => (
                    Vec::new(),
                    Some(LookupError {
                        failure: LookupFailure::TimedOut,
                        detail: format!("{}: {host}", crate::pipe::RESOLVE_TIMED_OUT),
                    }),
                ),
            };
            let connected = entry.connected.or_else(|| {
                bound
                    .iter()
                    .rev()
                    .find(|bound| bound.peer.port() == port && addresses.contains(&bound.peer.ip()))
                    .map(|bound| bound.peer.ip())
            });
            return Some(HostResolution {
                host: host.to_owned(),
                by: asker.by,
                proxy: asker.proxy,
                servers: asker.servers.clone(),
                wire: entry.from.map(|from| from.wire).or(asker.wire),
                answered_by: entry.from.map(|from| from.address),
                trust: asker.trust.and(entry.judged),
                addresses,
                cached: entry.cached.unwrap_or(false),
                connected,
                error,
            });
        }
        // A WireGuard engine that is the first hop resolves its endpoint
        // with this machine's DNS itself, and binds a socket to the first
        // address it finds.
        if let Some(bound) = bound
            .iter()
            .rev()
            .find(|bound| !bound.carried && bound.peer.port() == port)
        {
            return Some(HostResolution {
                host: host.to_owned(),
                by: ResolvedBy::Host,
                proxy: None,
                servers: Vec::new(),
                wire: None,
                answered_by: None,
                trust: None,
                addresses: vec![bound.peer.ip()],
                cached: false,
                connected: Some(bound.peer.ip()),
                error: None,
            });
        }
        let error = endpoint_failure(failure?)?;
        Some(HostResolution {
            host: host.to_owned(),
            by: ResolvedBy::Host,
            proxy: None,
            servers: Vec::new(),
            wire: None,
            answered_by: None,
            trust: None,
            addresses: Vec::new(),
            cached: false,
            connected: None,
            error: Some(error),
        })
    }
}

// What a WireGuard engine's own failed lookup of its endpoint came to.
fn endpoint_failure(text: &str) -> Option<LookupError> {
    let (failure, detail) = if let Some((_, said)) = text.split_once(ENDPOINT_UNRESOLVED) {
        (classify(said), said)
    } else if text.contains(ENDPOINT_LATE) {
        (LookupFailure::TimedOut, ENDPOINT_LATE)
    } else if text.contains(ENDPOINT_EMPTY) {
        (LookupFailure::NoAddress, ENDPOINT_EMPTY)
    } else {
        return None;
    };
    Some(LookupError {
        failure,
        detail: detail.to_owned(),
    })
}

// The phrases each failure is said with: this machine's resolver on each
// system, Weaver's own lookups, and the tunnel engines'. Matched without
// regard to case, in this order.
const NO_SUCH_NAME: [&str; 5] = [
    "not known",
    "no such host",
    "does not resolve",
    "nxdomain",
    "(os error 11001)",
];
const NO_ADDRESS: [&str; 8] = [
    "no address associated",
    "returned no address",
    "resolved to no address",
    "no usable address",
    "no address resolved",
    "could not resolve to any address",
    "nodata",
    "(os error 11004)",
];
const SERVER_FAILURE: [&str; 7] = [
    "temporary failure",
    "try again",
    "non-recoverable",
    "servfail",
    "refused",
    "(os error 11002)",
    "(os error 11003)",
];
const TIMED_OUT: [&str; 3] = ["timed out", "in time", "deadline exceeded"];

// Why a lookup failed, by what its resolver said.
pub(crate) fn classify(text: &str) -> LookupFailure {
    let text = text.to_ascii_lowercase();
    let said = |phrases: &[&str]| phrases.iter().any(|phrase| text.contains(phrase));
    // Never NotAuthenticated: only the DNS client's own failure says that.
    if said(&NO_SUCH_NAME) {
        LookupFailure::NoSuchName
    } else if said(&NO_ADDRESS) {
        LookupFailure::NoAddress
    } else if said(&SERVER_FAILURE) {
        LookupFailure::ServerFailure
    } else if said(&TIMED_OUT) {
        LookupFailure::TimedOut
    } else {
        LookupFailure::Other
    }
}

fn key(host: &str) -> String {
    host.trim_end_matches('.').to_ascii_lowercase()
}

// Notes into the recording, when there is one.
fn note(host: &str, write: impl FnOnce(&mut Entry)) {
    let _ = RECORDING.try_with(|recording| {
        let mut names = recording.names.lock().expect("resolution recording");
        write(names.entry(key(host)).or_default());
    });
}

// `host` is about to be asked of `asker`.
pub(crate) fn asking(host: &str, asker: impl FnOnce() -> Asker) {
    note(host, |entry| {
        if entry.asker.is_none() {
            entry.asker = Some(asker());
        }
    });
}

// Whether the answer for `host` was one held from before.
pub(crate) fn cached(host: &str, held: bool) {
    note(host, |entry| {
        entry.cached.get_or_insert(held);
    });
}

// The servers asked for `host` came to an answer judged `trust`, from
// `from` when a server answered.
pub(crate) fn answered(host: &str, from: Option<Answerer>, trust: TrustOutcome) {
    note(host, |entry| {
        entry.from = entry.from.or(from);
        entry.judged.get_or_insert(trust);
    });
}

pub(crate) fn found(host: &str, addresses: &[IpAddr]) {
    note(host, |entry| {
        entry.answer.get_or_insert_with(|| Ok(addresses.to_vec()));
    });
}

pub(crate) fn failed(host: &str, failure: LookupFailure, detail: impl FnOnce() -> String) {
    note(host, |entry| {
        entry.answer.get_or_insert_with(|| {
            Err(LookupError {
                failure,
                detail: detail(),
            })
        });
    });
}

// What a WireGuard tunnel's own resolver says for every answer without an
// address: the name does not exist, the server failed or refused, the name
// has no address of the families asked, or no server replied in time.
const TUNNEL_NO_ADDRESS: &str = "does not know this name";
pub(crate) const TUNNEL_NO_ADDRESS_SAID: &str = "the tunnel's DNS servers gave no address; \
     the tunnel does not say whether the name does not exist, a server failed, or none replied";

// `host`'s lookup through a WireGuard tunnel's own resolver failed, saying
// `detail`.
pub(crate) fn tunnel_failed(host: &str, detail: &str) {
    if detail.to_ascii_lowercase().contains(TUNNEL_NO_ADDRESS) {
        failed(host, LookupFailure::Other, || {
            TUNNEL_NO_ADDRESS_SAID.to_owned()
        });
    } else {
        failed_saying(host, &detail);
    }
}

// `host`'s lookup failed, saying `error`.
pub(crate) fn failed_saying(host: &str, error: &dyn std::fmt::Display) {
    note(host, |entry| {
        entry.answer.get_or_insert_with(|| {
            let detail = error.to_string();
            Err(LookupError {
                failure: classify(&detail),
                detail,
            })
        });
    });
}

// A dial to `host` connected to `ip`.
pub(crate) fn connected(host: &str, ip: IpAddr) {
    note(host, |entry| {
        entry.connected.get_or_insert(ip);
    });
}

// A tunnel engine bound a socket to reach `peer`.
pub(crate) fn bound(peer: SocketAddr, carried: bool) {
    let _ = RECORDING.try_with(|recording| {
        recording
            .bound
            .lock()
            .expect("resolution recording")
            .push(Bound { peer, carried });
    });
}
