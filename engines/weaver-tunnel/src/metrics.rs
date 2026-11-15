// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Process-local counters for the tunnel engine. Every label is a closed set
// held in a fixed array, so recording is a relaxed atomic add.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::dns::WireKind;
use crate::pipe::DialError;
use crate::resolution::record::TrustOutcome;

// The kind of proxy a tunnel hop speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TunnelKind {
    HttpConnect,
    Http3Connect,
    Socks5,
    Ssh,
    WireGuard,
}

impl TunnelKind {
    pub const ALL: [Self; 5] = [
        Self::HttpConnect,
        Self::Http3Connect,
        Self::Socks5,
        Self::Ssh,
        Self::WireGuard,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::HttpConnect => "http_connect",
            Self::Http3Connect => "http3_connect",
            Self::Socks5 => "socks5",
            Self::Ssh => "ssh",
            Self::WireGuard => "wireguard",
        }
    }

    // The tunnel kinds that hold a session, which a hop prepares and retires.
    pub const SESSIONS: [Self; 3] = [Self::Http3Connect, Self::Ssh, Self::WireGuard];
}

// How a dial ended: success, or the kind of error that ended it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DialResult {
    Success,
    Skipped,
    AtCapacity,
    Bind,
    Egress,
    Hop,
    Destination,
    Refused,
    Timeout,
    Fatal,
}

impl DialResult {
    pub const ALL: [Self; 10] = [
        Self::Success,
        Self::Skipped,
        Self::AtCapacity,
        Self::Bind,
        Self::Egress,
        Self::Hop,
        Self::Destination,
        Self::Refused,
        Self::Timeout,
        Self::Fatal,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Skipped => "skipped",
            Self::AtCapacity => "at_capacity",
            Self::Bind => "bind",
            Self::Egress => "egress",
            Self::Hop => "hop",
            Self::Destination => "destination",
            Self::Refused => "refused",
            Self::Timeout => "timeout",
            Self::Fatal => "fatal",
        }
    }

    pub fn of<T>(result: &Result<T, DialError>) -> Self {
        match result {
            Ok(_) => Self::Success,
            Err(DialError::Skipped(_)) => Self::Skipped,
            Err(DialError::AtCapacity(_)) => Self::AtCapacity,
            Err(DialError::Bind(_)) => Self::Bind,
            Err(DialError::Egress(_)) => Self::Egress,
            Err(DialError::Hop { .. }) => Self::Hop,
            Err(DialError::Destination(_)) => Self::Destination,
            Err(DialError::Refused(_)) => Self::Refused,
            Err(DialError::Timeout { .. }) => Self::Timeout,
            Err(DialError::Fatal(_)) => Self::Fatal,
        }
    }

    pub fn is_success(self) -> bool {
        self == Self::Success
    }
}

// Which resolver answered a name: the host's own DNS, DNS servers asked
// from the egress or over a proxy, or the resolver inside a WireGuard
// tunnel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolver {
    Host,
    Routed,
    WireGuard,
}

impl Resolver {
    pub const ALL: [Self; 3] = [Self::Host, Self::Routed, Self::WireGuard];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Routed => "routed",
            Self::WireGuard => "wireguard",
        }
    }
}

// How a resolution cache answered: from an answer it held, by waiting on a
// lookup already asking for the name, or by asking itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheOutcome {
    Hit,
    Joined,
    Miss,
}

impl CacheOutcome {
    pub const ALL: [Self; 3] = [Self::Hit, Self::Joined, Self::Miss];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Hit => "hit",
            Self::Joined => "joined",
            Self::Miss => "miss",
        }
    }
}

const KINDS: usize = TunnelKind::ALL.len();
const RESULTS: usize = DialResult::ALL.len();
// A ladder has at most eight rungs; anything past that shares the last slot.
pub const RUNGS: usize = 8;

static STREAMS: [[AtomicU64; RESULTS]; KINDS] =
    [const { [const { AtomicU64::new(0) }; RESULTS] }; KINDS];
static SESSION_PREPARES: [[AtomicU64; 2]; KINDS] =
    [const { [const { AtomicU64::new(0) }; 2] }; KINDS];
static SESSION_RETIREMENTS: [AtomicU64; KINDS] = [const { AtomicU64::new(0) }; KINDS];
static RESOLUTIONS: [[AtomicU64; 2]; Resolver::ALL.len()] =
    [const { [const { AtomicU64::new(0) }; 2] }; Resolver::ALL.len()];
static CACHE: [[AtomicU64; CacheOutcome::ALL.len()]; Resolver::ALL.len()] =
    [const { [const { AtomicU64::new(0) }; CacheOutcome::ALL.len()] }; Resolver::ALL.len()];
static RUNG_COOLDOWNS: [AtomicU64; RUNGS] = [const { AtomicU64::new(0) }; RUNGS];
static RUNG_FALLBACKS: [AtomicU64; RUNGS] = [const { AtomicU64::new(0) }; RUNGS];
static REVOCATIONS: AtomicU64 = AtomicU64::new(0);
static DNS_WIRES: [AtomicU64; WireKind::ALL.len()] =
    [const { AtomicU64::new(0) }; WireKind::ALL.len()];
static DNS_TRUST: [AtomicU64; TrustOutcome::ALL.len()] =
    [const { AtomicU64::new(0) }; TrustOutcome::ALL.len()];

fn rung(index: usize) -> usize {
    index.min(RUNGS - 1)
}

// A stream opened through a hop of `kind`.
pub fn record_stream<T>(kind: TunnelKind, result: &Result<T, DialError>) {
    STREAMS[kind as usize][DialResult::of(result) as usize].fetch_add(1, Ordering::Relaxed);
}

pub fn record_session_prepare(kind: TunnelKind, ok: bool) {
    SESSION_PREPARES[kind as usize][usize::from(ok)].fetch_add(1, Ordering::Relaxed);
}

pub fn record_session_retirement(kind: TunnelKind) {
    SESSION_RETIREMENTS[kind as usize].fetch_add(1, Ordering::Relaxed);
}

pub fn record_resolution(resolver: Resolver, ok: bool) {
    RESOLUTIONS[resolver as usize][usize::from(ok)].fetch_add(1, Ordering::Relaxed);
}

pub fn record_cache(resolver: Resolver, outcome: CacheOutcome) {
    CACHE[resolver as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
}

// A lookup of routed DNS came to an answer from a server on `wire`.
pub fn record_dns_wire(wire: WireKind) {
    DNS_WIRES[wire as usize].fetch_add(1, Ordering::Relaxed);
}

// How a lookup of routed DNS was judged: an answer taken as sent,
// authenticated, validated or unsigned, or one refused for want of the AD
// bit or for failing the DNSSEC check. Once per lookup; and bogus once more
// for each address family that failed the check in a lookup the other
// family answered.
pub fn record_dns_trust(outcome: TrustOutcome) {
    DNS_TRUST[outcome as usize].fetch_add(1, Ordering::Relaxed);
}

// Queries sent to listed servers to check DNSSEC signatures, the address
// queries among them: each counted as it goes out, whether or not a reply
// comes, and again when it is sent again.
static DNSSEC_QUERIES: AtomicU64 = AtomicU64::new(0);

pub fn record_dnssec_queries(sent: u64) {
    DNSSEC_QUERIES.fetch_add(sent, Ordering::Relaxed);
}

pub fn dnssec_queries() -> u64 {
    DNSSEC_QUERIES.load(Ordering::Relaxed)
}

// A rung of a ladder was set aside after its path failed.
pub fn record_rung_cooldown(index: usize) {
    RUNG_COOLDOWNS[rung(index)].fetch_add(1, Ordering::Relaxed);
}

// A ladder carried a connection on a rung below its first.
pub fn record_rung_fallback(index: usize) {
    RUNG_FALLBACKS[rung(index)].fetch_add(1, Ordering::Relaxed);
}

// A leg's sockets and streams were revoked.
pub fn record_revocation() {
    REVOCATIONS.fetch_add(1, Ordering::Relaxed);
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TunnelMetricsSnapshot {
    pub streams: Vec<(TunnelKind, DialResult, u64)>,
    // (kind, succeeded, failed), for session kinds only.
    pub session_prepares: Vec<(TunnelKind, u64, u64)>,
    pub session_retirements: Vec<(TunnelKind, u64)>,
    // (resolver, succeeded, failed).
    pub resolutions: Vec<(Resolver, u64, u64)>,
    pub cache: Vec<(Resolver, CacheOutcome, u64)>,
    // Indexed by rung.
    pub rung_cooldowns: Vec<u64>,
    pub rung_fallbacks: Vec<u64>,
    pub revocations: u64,
}

pub fn snapshot() -> TunnelMetricsSnapshot {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    TunnelMetricsSnapshot {
        streams: TunnelKind::ALL
            .into_iter()
            .flat_map(|kind| {
                DialResult::ALL.into_iter().map(move |result| {
                    (kind, result, load(&STREAMS[kind as usize][result as usize]))
                })
            })
            .collect(),
        session_prepares: TunnelKind::SESSIONS
            .into_iter()
            .map(|kind| {
                let counts = &SESSION_PREPARES[kind as usize];
                (kind, load(&counts[1]), load(&counts[0]))
            })
            .collect(),
        session_retirements: TunnelKind::SESSIONS
            .into_iter()
            .map(|kind| (kind, load(&SESSION_RETIREMENTS[kind as usize])))
            .collect(),
        resolutions: Resolver::ALL
            .into_iter()
            .map(|resolver| {
                let counts = &RESOLUTIONS[resolver as usize];
                (resolver, load(&counts[1]), load(&counts[0]))
            })
            .collect(),
        cache: Resolver::ALL
            .into_iter()
            .flat_map(|resolver| {
                CacheOutcome::ALL.into_iter().map(move |outcome| {
                    (
                        resolver,
                        outcome,
                        load(&CACHE[resolver as usize][outcome as usize]),
                    )
                })
            })
            .collect(),
        rung_cooldowns: RUNG_COOLDOWNS.iter().map(load).collect(),
        rung_fallbacks: RUNG_FALLBACKS.iter().map(load).collect(),
        revocations: load(&REVOCATIONS),
    }
}

// The routed DNS counters, apart from the tunnel snapshot so its shape
// stays as consumers build it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DnsMetricsSnapshot {
    // Answered lookups by the wire of the server that answered.
    pub wires: Vec<(WireKind, u64)>,
    // Lookups by how their answer was judged.
    pub trust: Vec<(TrustOutcome, u64)>,
    // Queries sent to check DNSSEC signatures.
    pub dnssec_queries: u64,
}

pub fn dns_snapshot() -> DnsMetricsSnapshot {
    let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    DnsMetricsSnapshot {
        wires: WireKind::ALL
            .into_iter()
            .map(|wire| (wire, load(&DNS_WIRES[wire as usize])))
            .collect(),
        trust: TrustOutcome::ALL
            .into_iter()
            .map(|outcome| (outcome, load(&DNS_TRUST[outcome as usize])))
            .collect(),
        dnssec_queries: dnssec_queries(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_dial_error_has_its_own_result_label() {
        let io = || std::io::Error::other("x");
        let errors = [
            DialError::Skipped("x".into()),
            DialError::AtCapacity(Vec::new()),
            DialError::Bind(io()),
            DialError::Egress(io()),
            DialError::Hop {
                proxy: 1,
                source: crate::TunnelError::Configuration("x".into()),
            },
            DialError::Destination(io()),
            DialError::Refused(io()),
            DialError::Timeout { stage: "x".into() },
            DialError::Fatal(crate::TunnelError::Configuration("x".into())),
        ];
        let mut results: Vec<DialResult> = errors
            .into_iter()
            .map(|error| DialResult::of::<()>(&Err(error)))
            .collect();
        results.insert(0, DialResult::of(&Ok::<(), DialError>(())));
        assert_eq!(results, DialResult::ALL);
        let labels: std::collections::HashSet<_> =
            DialResult::ALL.iter().map(|r| r.as_str()).collect();
        assert_eq!(labels.len(), DialResult::ALL.len());
    }

    #[test]
    fn a_rung_past_the_tracked_range_counts_on_the_last_one() {
        assert_eq!(rung(0), 0);
        assert_eq!(rung(RUNGS - 1), RUNGS - 1);
        assert_eq!(rung(RUNGS + 5), RUNGS - 1);
    }
}
