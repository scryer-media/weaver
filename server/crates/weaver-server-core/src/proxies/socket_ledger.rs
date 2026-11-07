// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How each WireGuard tunnel's sockets are shared out.
//
// A tunnel holds `cap` sockets. Usenet servers routed through it get every
// connection they may put on it, so a leg that takes over a failed leg's
// share still has the sockets. An eighth of the tunnel is reserved for the
// rest. Half of that is never handed out, so sockets closed but still
// lingering in the tunnel never leave a new one without room; the other half
// is split evenly between the tunnel's own sockets (name lookups, DHT and uTP
// datagram sockets, probes) and HTTP (trackers, web seeds, feeds), so a
// tracker storm never takes the sockets DHT and uTP bind. Torrent peers over
// TCP take what is left; once that is full they connect over uTP, which
// needs no socket of its own.
//
// Usenet connections are set per server, so a save that asks more than the
// tunnel can hold is refused. The torrent peer limit is one setting for
// every route, so it is never refused: its TCP peers are held to the share.

use std::collections::HashMap;

use weaver_tunnel::pipe::SocketShares;

use super::{
    Consumer, Failover, LegPath, NetworkRefusal, ProxyKind, ProxyPool, ProxyProfile, Route, Rung,
};

// TCP peers a tunnel keeps for a torrent route that uses it, however much
// usenet asks.
pub const SOCKET_PEER_FLOOR: usize = 16;

// The sockets a tunnel of `cap` reserves for everything but usenet and
// torrent peers over TCP.
pub fn socket_reserve(cap: usize) -> usize {
    cap / 8
}

// The part of the reserve kept for sockets closed but still lingering in the
// tunnel, which no share hands out.
pub fn socket_linger(cap: usize) -> usize {
    cap / 16
}

// One WireGuard session's sockets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TunnelSockets {
    pub egress: u32,
    pub proxy: u32,
    pub cap: usize,
    // Every connection the usenet servers routed through it may hold there,
    // each with one more for replacing a connection.
    pub nntp: usize,
    pub reserve: usize,
    // Whether a torrent route uses the tunnel.
    pub torrent: bool,
}

impl TunnelSockets {
    // The TCP peers kept for torrents.
    pub fn peer_floor(&self) -> usize {
        if self.torrent { SOCKET_PEER_FLOOR } else { 0 }
    }

    pub fn over_budget(&self) -> bool {
        self.nntp + self.reserve + self.peer_floor() > self.cap
    }

    // The shares the session holds its sockets to. A tunnel over budget,
    // as one becomes when the hardware profile lowers the cap under a saved
    // configuration, holds usenet to what the reserve and the peer floor
    // leave.
    pub fn shares(&self) -> SocketShares {
        let reserve = self.reserve.min(self.cap);
        let nntp = self
            .nntp
            .min(self.cap.saturating_sub(reserve + self.peer_floor()));
        let handed = reserve.saturating_sub(socket_linger(self.cap));
        SocketShares {
            nntp,
            torrent: self.cap - reserve - nntp,
            core: handed - handed / 2,
            http: handed / 2,
        }
    }

    pub fn refusal(&self) -> NetworkRefusal {
        NetworkRefusal::WireGuardSocketBudget {
            proxy: self.proxy,
            egress: self.egress,
            nntp: self.nntp,
            reserve: self.reserve,
            peers: self.peer_floor(),
            cap: self.cap,
        }
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
    cap: usize,
) -> HashMap<SessionPath, TunnelSockets> {
    let mut ledger: HashMap<SessionPath, TunnelSockets> = HashMap::new();
    // What each server may put on each session, before the replacement.
    let mut asks: HashMap<(SessionPath, u32), usize> = HashMap::new();
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
                            session(&mut ledger, leg.egress_id, &ids[..=index], cap);
                        }
                    }
                }
                let pooled = matches!(rung, Rung::Pool { .. });
                let carriers = rung_carriers(rung, profiles, pools);
                for path in carriers {
                    let key = session(&mut ledger, leg.egress_id, &path, cap);
                    match consumer {
                        Consumer::Torrent(_) => {
                            if let Some(tunnel) = ledger.get_mut(&key) {
                                tunnel.torrent = true;
                            }
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
    for ((key, server), ask) in asks {
        let count = connections.get(&server).copied().unwrap_or(0);
        if let Some(tunnel) = ledger.get_mut(&key) {
            tunnel.nntp += ask.min(usize::from(count)) + 1;
        }
    }
    ledger
}

fn wireguard(profiles: &HashMap<u32, ProxyProfile>, id: &u32) -> bool {
    profiles
        .get(id)
        .is_some_and(|profile| profile.enabled && profile.kind == ProxyKind::WireGuard)
}

// The WireGuard sessions a rung's connections open their TCP sockets in,
// by the hops up to the session's proxy: a proxy, each member of a pool, or
// the last WireGuard hop of a chain.
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
    egress: u32,
    path: &[u32],
    cap: usize,
) -> SessionPath {
    let key = (egress, path.to_vec());
    ledger.entry(key.clone()).or_insert_with(|| TunnelSockets {
        egress,
        proxy: *path.last().expect("a session has a proxy"),
        cap,
        nntp: 0,
        reserve: socket_reserve(cap),
        torrent: false,
    });
    key
}

// What the usenet servers ask of each tunnel in `ledger`.
pub fn socket_asks(ledger: &HashMap<SessionPath, TunnelSockets>) -> HashMap<SessionPath, usize> {
    ledger
        .iter()
        .map(|(path, tunnel)| (path.clone(), tunnel.nntp))
        .collect()
}

// Refuse a change that raises what usenet asks of a tunnel it leaves over
// budget, measured against `before`, what the servers asked before it. A
// change that leaves a tunnel over budget without asking it for more, as
// any does once the hardware profile lowers the cap, is no reason to refuse
// it: that tunnel's legs warn instead. The first refused tunnel, by egress
// and path, is named.
pub fn validate_socket_raise(
    before: &HashMap<SessionPath, usize>,
    after: HashMap<SessionPath, TunnelSockets>,
) -> Result<(), NetworkRefusal> {
    let mut over: Vec<_> = after
        .into_iter()
        .filter(|(path, tunnel)| {
            tunnel.over_budget() && tunnel.nntp > before.get(path).copied().unwrap_or(0)
        })
        .collect();
    over.sort_by(|a, b| a.0.cmp(&b.0));
    match over.into_iter().next() {
        Some((_, tunnel)) => Err(tunnel.refusal()),
        None => Ok(()),
    }
}

impl Consumer {
    // The consumer a stored route key names.
    pub fn from_key(key: &str) -> Option<Self> {
        use super::TorrentScope;
        let id = |rest: &str| rest.parse().ok();
        if key == "outbound" {
            return Some(Self::Outbound);
        }
        if key == "torrent:default" {
            return Some(Self::Torrent(TorrentScope::Default));
        }
        if let Some(rest) = key.strip_prefix("torrent:category:") {
            return id(rest).map(|id| Self::Torrent(TorrentScope::Category(id)));
        }
        if let Some(rest) = key.strip_prefix("torrent:tracker:") {
            return id(rest).map(|id| Self::Torrent(TorrentScope::TrackerRule(id)));
        }
        if let Some(rest) = key.strip_prefix("server:") {
            return id(rest).map(Self::Server);
        }
        key.strip_prefix("rss:").and_then(id).map(Self::Rss)
    }
}

#[cfg(test)]
#[path = "socket_ledger_tests.rs"]
mod tests;
