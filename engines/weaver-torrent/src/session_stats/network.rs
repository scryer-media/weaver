// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session counted of the ways it is reached and held back: port
//! mappings, holepunches, the local network's announces and its rate
//! limits. Each count is one relaxed atomic add where the event happens.

use serde::Serialize;

use super::swarm::{Tally, add, counted, read};
use crate::port_mapper::Transport;
use crate::upnp::PortMapped;

counted! {
    /// Which mapper reported on a port mapping.
    PortMapper {
        NatPmp => "natpmp",
        Upnp => "upnp",
        /// PCP from a global IPv6 address.
        PcpIpv6 => "pcp_ipv6",
    }
}

counted! {
    /// What a mapper reported.
    PortMapResult {
        /// The gateway forwards a port.
        Mapped => "mapped",
        /// The gateway refused, or did not answer.
        Failed => "failed",
        /// The mapping was taken down.
        Removed => "removed",
    }
}

counted! {
    /// One step of a holepunch (BEP 55).
    HolepunchEvent {
        /// We asked a peer to introduce us to one we could not reach.
        Asked => "asked",
        /// A peer asked us for an introduction and both were told to
        /// connect.
        Introduced => "introduced",
        /// A peer asked us for an introduction we could not make.
        Refused => "refused",
        /// A peer told us to connect to another, and we will.
        Connect => "connect",
        /// A peer told us to connect to an address we do not take from it.
        ConnectIgnored => "connect_ignored",
        /// The peer we asked could not introduce us.
        Failed => "failed",
        /// A holepunch message from a peer that did not offer the
        /// extension.
        Unoffered => "unoffered",
    }
}

/// The port mapping and holepunch counts of one session.
#[derive(Debug, Default)]
pub struct NetworkStats {
    port_maps: [Tally<{ PortMapResult::COUNT }>; PortMapper::COUNT],
    holepunch: Tally<{ HolepunchEvent::COUNT }>,
}

impl NetworkStats {
    /// A mapper's report.
    pub(crate) fn port_mapped(&self, report: &PortMapped) {
        let mapper = Transport::of(report.transport).stat;
        let result = match (&report.error, report.external_port) {
            (Some(_), _) => PortMapResult::Failed,
            (None, Some(_)) => PortMapResult::Mapped,
            (None, None) => PortMapResult::Removed,
        };
        add(&self.port_maps[mapper as usize], result as usize, 1);
    }

    #[inline]
    pub fn holepunch(&self, event: HolepunchEvent) {
        add(&self.holepunch, event as usize, 1);
    }

    pub fn snapshot(&self) -> NetworkSnapshot {
        NetworkSnapshot {
            port_maps: self.port_maps.each_ref().map(read),
            holepunch: read(&self.holepunch),
        }
    }
}

/// [`NetworkStats`] as read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct NetworkSnapshot {
    /// By [`PortMapper`], then [`PortMapResult`].
    pub port_maps: [[u64; PortMapResult::COUNT]; PortMapper::COUNT],
    /// By [`HolepunchEvent`].
    pub holepunch: [u64; HolepunchEvent::COUNT],
}

/// What a session's local service discovery has counted since it started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LsdSnapshot {
    /// Announces sent to the local network, one per torrent each time.
    pub sent: u64,
    /// Announces of other hosts on the local network that were read.
    pub received: u64,
    /// Peers those announces named for a torrent the session announces.
    pub peers_found: u64,
}

/// What waited on a session's rate limits, download then upload.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct LimiterSnapshot {
    /// Requests for bytes that were granted only after a wait.
    pub waits: [u64; 2],
    /// How long those requests waited in all, in nanoseconds.
    pub wait_nanos: [u64; 2],
}
