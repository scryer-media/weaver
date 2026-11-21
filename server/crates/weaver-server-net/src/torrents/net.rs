// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How a torrent session rides one leg of its route: the kind of leg, what
//! a session on it may do, and what it shows trackers and peers. The
//! sockets themselves live in weaver-server-torrents.

use std::net::IpAddr;
use std::sync::Arc;

use weaver_torrent::Session;
use weaver_torrent::upnp::PortMapped;
use weaver_tunnel::endpoint::DatagramTransport;
use weaver_tunnel::pipe::{Dialer, PortForwarding};

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

    /// Whether a session on this leg binds ports of the host: its own
    /// sockets, or the leg's at a pinned source address. A ladder leg's
    /// sockets are all the tunnel's or the proxy's, and take none.
    pub fn binds_host_ports(&self) -> bool {
        self.own_sockets() || self.listen_ip().is_some()
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
                mapped: false,
                utp: true,
                datagrams: true,
            },
            // A leg held to a device or a source address maps its ports on
            // that interface's own gateway, asking from the leg's sockets.
            Self::Interface(_) => Shape {
                own_sockets: true,
                dht: true,
                upnp: true,
                listen: true,
                mapped: false,
                utp: true,
                datagrams: true,
            },
            Self::SourceAddress(_) => Shape {
                own_sockets: false,
                dht: true,
                upnp: true,
                listen: true,
                mapped: false,
                utp: true,
                datagrams: true,
            },
            // A ladder leg listens only when its last hop's provider
            // forwards a port to it: peers outside the tunnel reach nothing
            // else. A port the gateway maps on request reaches it only once
            // the gateway has mapped one.
            Self::Ladder => {
                let datagrams = dialer.is_some_and(|dialer| carrier(dialer).is_some());
                let forwarding = dialer
                    .and_then(|dialer| Arc::clone(dialer).inbound())
                    .map(|inbound| inbound.forwarding());
                Shape {
                    own_sockets: false,
                    dht: datagrams,
                    upnp: false,
                    listen: forwarding.is_some_and(|forwarding| forwarding != PortForwarding::None),
                    mapped: matches!(forwarding, Some(PortForwarding::NatPmp { .. })),
                    utp: datagrams,
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
    /// Peers reach the listener only through a port the gateway maps on
    /// request, so only once it has mapped one.
    pub mapped: bool,
    /// uTP on the leg's datagram route.
    pub utp: bool,
    /// UDP of any kind: UDP trackers, uTP and DHT.
    pub datagrams: bool,
}

// The datagrams `dialer` carries for the session's UDP.
pub fn carrier(dialer: &Arc<dyn Dialer>) -> Option<Arc<dyn DatagramTransport>> {
    Arc::clone(dialer).datagrams()
}

// Whether peers can connect in to a session that announces `announced_port`
// on a leg of `shape`: it announces a port, and on a leg whose gateway maps
// one on request, the gateway has mapped one.
pub fn reachable(announced_port: u16, shape: Shape, mappings: &[PortMapped]) -> bool {
    announced_port != 0
        && (!shape.mapped
            || mappings
                .iter()
                .any(|mapping| mapping.external_port.is_some()))
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
    /// The session speaks uTP: it listens on uTP or connects out over it.
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
            inbound: reachable(announced_port, shape, mappings),
            udp_trackers: shape.datagrams,
            dht: shape.dht,
            utp: session.uses_utp(),
        }
    }
}
