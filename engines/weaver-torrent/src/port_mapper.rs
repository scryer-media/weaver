// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a port mapper is to the session, and the one table of the
//! transports mappers report as: the family each maps for, and which one's
//! port a family is told when several hold one.

use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use crate::session_stats::network::PortMapper as MapperStat;
use crate::upnp::{PortMapEvents, PortmapTransport};

/// One mapper a session runs: it keeps the session's ports mapped on a
/// gateway until cancelled, reporting each outcome, then removes them.
pub(crate) trait PortMapper: Send {
    /// The transport its reports carry, whose row of [`TRANSPORTS`] says
    /// the family it maps for.
    fn transport(&self) -> PortmapTransport;

    /// The name of the task it runs as.
    fn task_name(&self) -> &'static str;

    /// Keep every mapping until `cancel`, reporting to `events`, then
    /// remove them.
    fn run(
        self: Box<Self>,
        cancel: CancellationToken,
        events: PortMapEvents,
    ) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// One transport's row of [`TRANSPORTS`].
pub(crate) struct Transport {
    pub(crate) transport: PortmapTransport,
    /// It maps for IPv6; else for IPv4.
    pub(crate) ipv6: bool,
    /// Its rank in its family: of the ports a family's transports hold, the
    /// lowest-ranked one is the family's (libtorrent walks
    /// `tcp_port_mapping` in `portmap_transport` order, natpmp first).
    pub(crate) precedence: u8,
    /// What the session's stats count its reports under.
    pub(crate) stat: MapperStat,
}

/// How many transports there are.
pub(crate) const TRANSPORT_COUNT: usize = MapperStat::COUNT;

/// Every transport, each at the index of its discriminant: over IPv4
/// NAT-PMP before UPnP, over IPv6 PCP.
pub(crate) const TRANSPORTS: [Transport; TRANSPORT_COUNT] = [
    Transport {
        transport: PortmapTransport::NatPmp,
        ipv6: false,
        precedence: 0,
        stat: MapperStat::NatPmp,
    },
    Transport {
        transport: PortmapTransport::Upnp,
        ipv6: false,
        precedence: 1,
        stat: MapperStat::Upnp,
    },
    Transport {
        transport: PortmapTransport::PcpIpv6,
        ipv6: true,
        precedence: 0,
        stat: MapperStat::PcpIpv6,
    },
];

// A transport added without a row stops the build here.
const _: fn(PortmapTransport) = |transport| match transport {
    PortmapTransport::NatPmp | PortmapTransport::Upnp | PortmapTransport::PcpIpv6 => {}
};

// Each row sits at its transport's index and its stat's, and no two rows of
// one family share a rank.
const _: () = {
    let mut at = 0;
    while at < TRANSPORT_COUNT {
        let row = &TRANSPORTS[at];
        assert!(row.transport as usize == at);
        assert!(row.stat as usize == at);
        let mut other = at + 1;
        while other < TRANSPORT_COUNT {
            let next = &TRANSPORTS[other];
            assert!(!(next.ipv6 == row.ipv6 && next.precedence == row.precedence));
            other += 1;
        }
        at += 1;
    }
};

impl Transport {
    /// `transport`'s row.
    pub(crate) const fn of(transport: PortmapTransport) -> &'static Transport {
        &TRANSPORTS[transport as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::{TRANSPORT_COUNT, TRANSPORTS, Transport};
    use crate::upnp::PortmapTransport;

    // Over IPv4 NAT-PMP's port before UPnP's (libtorrent walks
    // `tcp_port_mapping` in `portmap_transport` order, natpmp first); over
    // IPv6 PCP's. Every transport has exactly one row.
    #[test]
    fn the_transport_table_ranks_each_family() {
        let every = [
            PortmapTransport::NatPmp,
            PortmapTransport::Upnp,
            PortmapTransport::PcpIpv6,
        ];
        assert_eq!(every.len(), TRANSPORT_COUNT);
        for transport in every {
            let rows = TRANSPORTS
                .iter()
                .filter(|row| row.transport == transport)
                .count();
            assert_eq!(rows, 1, "{transport:?}");
            assert_eq!(Transport::of(transport).transport, transport);
        }
        let family = |ipv6: bool| {
            let mut rows: Vec<_> = TRANSPORTS.iter().filter(|row| row.ipv6 == ipv6).collect();
            rows.sort_by_key(|row| row.precedence);
            rows.into_iter()
                .map(|row| row.transport)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            family(false),
            [PortmapTransport::NatPmp, PortmapTransport::Upnp]
        );
        assert_eq!(family(true), [PortmapTransport::PcpIpv6]);
    }
}
