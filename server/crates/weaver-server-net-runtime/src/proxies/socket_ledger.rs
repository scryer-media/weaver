// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How each WireGuard tunnel's bytes are shared out.
//
// A tunnel holds WIREGUARD_INSTANCE_BYTES, and every byte it allocates is
// charged against that: its own overhead, then each socket at the size the
// tunnel allocates for it (see `weaver_tunnel::pipe::SessionSockets`). A
// reserve is kept off the split for what serves every kind of traffic: name
// lookups, probes, feeds and other outbound HTTP, and the carriers of
// proxies above. The rest is divided between usenet and torrents by the
// bandwidth split in force: one class alone may take all of it, and classes
// that both press get their shares.
//
// A save never refuses connections: the servers' connection counts and the
// torrent peer limit save whatever they ask. The tunnel holds them to its
// bytes when each socket opens: a usenet connection past what fits waits for
// room, its leg warns with what usenet is held to, and torrent TCP peers take
// what the split gives torrents, and connect over uTP past it.

use std::collections::{HashMap, HashSet};

use weaver_tunnel::pipe::{
    Purpose, SIDES, SocketBudget, SocketDivider, datagram_bytes, listener_bytes, overhead_bytes,
    socket_buffers, stream_bytes,
};

use super::{
    Consumer, Failover, LegPath, ProxyKind, ProxyPool, ProxyProfile, Route, Rung,
    WIREGUARD_INSTANCE_BYTES,
};

// The tunnel's own sockets kept for name lookups, probes and the carriers
// of proxies above it.
pub const SOCKET_CORE_BASE: usize = 8;

// The datagram sockets one torrent session holds on a tunnel for its whole
// life: uTP for its peer listener (or for outgoing peers when it does not
// listen), uTP for its SSL listener, DHT, UDP trackers, and the NAT-PMP
// mapping of a provider that forwards its port that way.
pub const DATAGRAMS_PER_TORRENT_SESSION: usize = 5;

// The TCP listeners one torrent session opens on a tunnel: its peer
// listener, and its SSL listener when SSL torrents are on.
pub const LISTENERS_PER_TORRENT_SESSION: usize = 2;

// The fewest HTTP sockets (feeds, other outbound) a tunnel keeps.
pub const SOCKET_HTTP_FLOOR: usize = 4;

// The MTU a tunnel is budgeted at when nothing names one.
const DEFAULT_MTU: u16 = 1420;

// What one torrent session holds on a tunnel of `mtu` for its whole life:
// its datagram sockets and its listeners.
pub fn torrent_session_bytes(mtu: u16) -> u64 {
    DATAGRAMS_PER_TORRENT_SESSION as u64 * datagram_bytes(mtu)
        + LISTENERS_PER_TORRENT_SESSION as u64 * listener_bytes()
}

// What one usenet connection holds of a tunnel, at the buffers the hardware
// profile in force gives it.
pub fn usenet_socket_bytes() -> u64 {
    stream_bytes(socket_buffers(&Purpose::Nntp {
        server: 0,
        leg: 0,
        probing: false,
    }))
}

// Put the hardware profile's usenet receive buffer in force. When that
// changes what a usenet connection holds, the routes are reloaded, so every
// tunnel's ledger, its over-budget warning and its sessions' budgets are
// computed again at the new size. Returns whether the size changed.
pub fn set_usenet_receive_bytes(
    bytes: usize,
    proxies: Option<std::sync::Arc<super::ProxyRuntime>>,
) -> bool {
    if !weaver_tunnel::pipe::set_usenet_receive_bytes(bytes) {
        return false;
    }
    if let (Some(proxies), Ok(runtime)) = (proxies, tokio::runtime::Handle::try_current()) {
        runtime.spawn(async move {
            if let Err(error) = proxies.reload().await {
                tracing::warn!(
                    error,
                    "budgeting the WireGuard tunnels for a new usenet buffer size failed"
                );
            }
        });
    }
    true
}

// One WireGuard session's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunnelSockets {
    pub egress: u32,
    pub proxy: u32,
    pub mtu: u16,
    // Every connection the usenet servers routed through it may hold there,
    // each with one more for replacing a connection.
    pub nntp: usize,
    // The torrent sessions on the tunnel, one for each leg torrent routes use.
    pub torrent_sessions: usize,
    // The SSH and HTTP/3 proxies above the tunnel, each holding one socket
    // in it that all its connections share.
    pub carriers: usize,
}

impl TunnelSockets {
    pub fn new(egress: u32, proxy: u32, mtu: u16) -> Self {
        Self {
            egress,
            proxy,
            mtu,
            nntp: 0,
            torrent_sessions: 0,
            carriers: 0,
        }
    }

    // The bytes kept off the split for every kind of traffic: the tunnel's
    // own sockets and HTTP, each at a stream's size, and the one datagram
    // socket each proxy above holds as its carrier.
    pub fn reserve(&self) -> u64 {
        (SOCKET_CORE_BASE + SOCKET_HTTP_FLOOR) as u64 * stream_bytes(None)
            + self.carriers as u64 * datagram_bytes(self.mtu)
    }

    // The bytes kept off the split for the torrent sessions' listeners and
    // datagram sockets, which they hold for their whole life and no
    // give-back can close.
    pub fn torrent_kept(&self) -> u64 {
        self.torrent_sessions as u64 * torrent_session_bytes(self.mtu)
    }

    // What the tunnel keeps apart from usenet: its overhead, the reserve,
    // and its torrent sessions' listeners and datagram sockets.
    pub fn kept(&self) -> u64 {
        overhead_bytes(self.mtu) + self.reserve() + self.torrent_kept()
    }

    // What usenet asks of the tunnel.
    pub fn nntp_bytes(&self) -> u64 {
        self.nntp as u64 * usenet_socket_bytes()
    }

    pub fn over_budget(&self) -> bool {
        self.nntp_bytes() + self.kept() > WIREGUARD_INSTANCE_BYTES
    }

    // The usenet connections the tunnel holds beside what it keeps, at most
    // what usenet asks.
    //
    // The usenet receive buffer each connection holds (1 MiB under the
    // Balanced hardware profile, 2 MiB under Performance) was set by the
    // operator on 2026-11-18, and so was the connection count per tunnel
    // that follows from it here. Both are intended, and the count is
    // enforced as each socket opens, not at save. Neither is a defect to
    // report or a value to retune without the operator.
    pub fn nntp_held(&self) -> usize {
        let room = WIREGUARD_INSTANCE_BYTES.saturating_sub(self.kept());
        self.nntp
            .min(usize::try_from(room / usenet_socket_bytes()).unwrap_or(usize::MAX))
    }

    // The bytes the session holds its sockets to.
    pub fn budget(&self) -> SocketBudget {
        SocketBudget {
            bytes: WIREGUARD_INSTANCE_BYTES,
            reserve: self.reserve(),
            torrent_kept: self.torrent_kept(),
            mtu: self.mtu,
        }
    }

    // What a leg over the tunnel says while usenet asks more of it than it
    // holds: the totals, and what usenet is held to as its sockets open.
    pub fn over_budget_warning(&self) -> String {
        const MIB: u64 = 1024 * 1024;
        format!(
            "Usenet servers ask {} connections of WireGuard proxy {} on egress {}, {} MiB, and the tunnel keeps {} MiB for itself, over the {} MiB it holds; usenet is held to {}.",
            self.nntp,
            self.proxy,
            self.egress,
            self.nntp_bytes().div_ceil(MIB),
            self.kept().div_ceil(MIB),
            WIREGUARD_INSTANCE_BYTES / MIB,
            self.nntp_held(),
        )
    }
}

// The bandwidth split's rule, dividing a tunnel's bytes between usenet and
// torrents by the split in force, as it divides the download limit. The
// tunnel's sides are the job classes, by protocol.
pub enum SplitDivider {
    // The split the bandwidth service put in force.
    InForce,
    // A split given, for tests of the rule.
    #[cfg(any(test, feature = "test-support"))]
    Fixed(Option<weaver_server_net::bandwidth::limits::BandwidthSplit>),
}

const _: () = assert!(SIDES == weaver_server_jobs::jobs::model::Protocol::COUNT);

impl SocketDivider for SplitDivider {
    fn shares(&self, cap: u64) -> [u64; SIDES] {
        let split = match self {
            Self::InForce => weaver_server_net::bandwidth::limits::split_in_force(),
            #[cfg(any(test, feature = "test-support"))]
            Self::Fixed(split) => *split,
        };
        split.unwrap_or_default().shares(cap)
    }

    fn share_out(&self, cap: u64, shares: &[u64], given: &[Option<u64>], used: &[u64]) -> Vec<u64> {
        weaver_server_net::bandwidth::limits::share_out(cap, shares, given, used)
    }

    fn presses(&self, used: u64, given: u64) -> bool {
        weaver_server_net::bandwidth::limits::presses(used, given)
    }
}

// A session, named as `wireguard_sessions` names it: the egress and the hops
// up to and including the WireGuard proxy.
pub type SessionPath = (u32, Vec<u32>);

// Every WireGuard session `routes` need, with its sockets. `connections`
// holds each active server's connection count; a server missing from it puts
// nothing on any tunnel.
pub fn socket_ledger(
    routes: &[(Consumer, Route)],
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
    connections: &HashMap<u32, u16>,
) -> HashMap<SessionPath, TunnelSockets> {
    let mut ledger: HashMap<SessionPath, TunnelSockets> = HashMap::new();
    // What each server may put on each session, before the replacement.
    let mut asks: HashMap<(SessionPath, u32), usize> = HashMap::new();
    // The torrent sessions reaching each tunnel, each counted once however
    // many routes share it.
    let mut torrents: HashSet<(SessionPath, TorrentSession)> = HashSet::new();
    // The chains whose connections ride a carrier above a session, each
    // counted once however many routes use it.
    let mut carried: HashSet<(SessionPath, Vec<u32>)> = HashSet::new();
    for (consumer, route) in routes {
        let targets = match consumer {
            Consumer::Server(id) => connections.get(id).map(|&count| {
                let healthy = vec![true; route.legs.len()];
                (
                    count,
                    route
                        .targets(count, &healthy)
                        .unwrap_or_else(|_| vec![count; route.legs.len()]),
                )
            }),
            _ => None,
        };
        for (position, leg) in route.legs.iter().enumerate() {
            let LegPath::Ladder { rungs, .. } = &leg.path else {
                continue;
            };
            for rung in rungs {
                // Every WireGuard hop in a chain is a session of its own.
                if let Rung::Chain { ids } = rung {
                    for (index, id) in ids.iter().enumerate() {
                        if wireguard(profiles, id) {
                            session(&mut ledger, profiles, leg.egress_id, &ids[..=index]);
                        }
                    }
                    if let Some(last) = carried_above(ids, profiles) {
                        carried.insert(((leg.egress_id, ids[..=last].to_vec()), ids.clone()));
                    }
                }
                let pooled = matches!(rung, Rung::Pool { .. });
                let carriers = rung_carriers(rung, profiles, pools);
                for path in carriers {
                    let key = session(&mut ledger, profiles, leg.egress_id, &path);
                    match consumer {
                        Consumer::Torrent(_) => {
                            torrents.insert((key, TorrentSession::of(&leg.path, pooled)));
                        }
                        Consumer::Server(id) => {
                            if let Some((count, targets)) = &targets {
                                let ask = if pooled || route.failover == Failover::Redistribute {
                                    *count
                                } else {
                                    targets[position]
                                };
                                *asks.entry((key, *id)).or_default() += usize::from(ask);
                            }
                        }
                        Consumer::Rss(_) | Consumer::Outbound => {}
                    }
                }
            }
        }
    }
    for (key, _) in torrents {
        if let Some(tunnel) = ledger.get_mut(&key) {
            tunnel.torrent_sessions += 1;
        }
    }
    for (key, _) in carried {
        if let Some(tunnel) = ledger.get_mut(&key) {
            tunnel.carriers += 1;
        }
    }
    for ((key, server), ask) in asks {
        let count = connections.get(&server).copied().unwrap_or(0);
        if let Some(tunnel) = ledger.get_mut(&key) {
            tunnel.nntp += ask.min(usize::from(count)) + 1;
        }
    }
    ledger
}

// A torrent session as the routes name it: one per leg, an egress and the
// path over it, which every route on that leg shares. A pool leg's session
// runs on whichever member it lands on, and a save cannot know which, so
// every member's tunnel is charged as if the pool's torrents all landed on
// it. A member runs one session for every pool on its egress, so that is
// one session on each member's tunnel.
#[derive(Debug, PartialEq, Eq, Hash)]
enum TorrentSession {
    Leg(String),
    Member,
}

impl TorrentSession {
    fn of(path: &LegPath, pooled: bool) -> Self {
        if pooled {
            Self::Member
        } else {
            Self::Leg(serde_json::to_string(path).expect("route legs always encode"))
        }
    }
}

fn wireguard(profiles: &HashMap<u32, ProxyProfile>, id: &u32) -> bool {
    profiles
        .get(id)
        .is_some_and(|profile| profile.enabled && profile.kind == ProxyKind::WireGuard)
}

// The last WireGuard hop of a chain whose connections all ride the one
// socket an SSH or HTTP/3 proxy above it holds there.
fn carried_above(ids: &[u32], profiles: &HashMap<u32, ProxyProfile>) -> Option<usize> {
    let last = ids.iter().rposition(|id| wireguard(profiles, id))?;
    ids[last + 1..]
        .iter()
        .any(|id| {
            profiles.get(id).is_some_and(|profile| {
                matches!(profile.kind, ProxyKind::Ssh | ProxyKind::Http3Connect)
            })
        })
        .then_some(last)
}

// The WireGuard sessions a rung's connections open their TCP sockets in,
// by the hops up to the session's proxy: a proxy, each member of a pool, or
// the last WireGuard hop of a chain, unless an SSH or HTTP/3 proxy above it
// carries them all in one socket.
pub fn rung_carriers(
    rung: &Rung,
    profiles: &HashMap<u32, ProxyProfile>,
    pools: &HashMap<u32, ProxyPool>,
) -> Vec<Vec<u32>> {
    let carriers = match rung {
        Rung::Proxy { id } => vec![vec![*id]],
        Rung::Pool { id } => pools
            .get(id)
            .filter(|pool| pool.enabled)
            .map_or_else(Vec::new, |pool| {
                pool.member_ids.iter().map(|id| vec![*id]).collect()
            }),
        Rung::Chain { ids } if carried_above(ids, profiles).is_some() => Vec::new(),
        Rung::Chain { ids } => ids
            .iter()
            .rposition(|id| wireguard(profiles, id))
            .map_or_else(Vec::new, |last| vec![ids[..=last].to_vec()]),
    };
    carriers
        .into_iter()
        .filter(|path| path.last().is_some_and(|id| wireguard(profiles, id)))
        .collect()
}

fn session(
    ledger: &mut HashMap<SessionPath, TunnelSockets>,
    profiles: &HashMap<u32, ProxyProfile>,
    egress: u32,
    path: &[u32],
) -> SessionPath {
    let key = (egress, path.to_vec());
    ledger.entry(key.clone()).or_insert_with(|| {
        let proxy = *path.last().expect("a session has a proxy");
        TunnelSockets::new(egress, proxy, tunnel_mtu(profiles, proxy))
    });
    key
}

// The MTU a tunnel of `proxy` is built with.
pub fn tunnel_mtu(profiles: &HashMap<u32, ProxyProfile>, proxy: u32) -> u16 {
    profiles
        .get(&proxy)
        .map_or(DEFAULT_MTU, |profile| profile.mtu)
}
