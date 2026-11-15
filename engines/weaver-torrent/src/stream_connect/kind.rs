// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What carries a peer connection, and every name it goes by: one table,
//! which logs, serde, metrics labels, the swarm counts, GraphQL and the
//! qBittorrent facade read instead of a match of their own.

use std::net::SocketAddr;

use serde::{Serialize, Serializer};
use tracing::debug_span;

/// What carries a peer connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionKind {
    Tcp,
    Utp,
}

/// Every name a connection kind goes by.
#[derive(Debug, PartialEq, Eq)]
pub struct Names {
    /// Lower case, stable: serde, metrics labels and the swarm counts.
    pub key: &'static str,
    /// In logs and errors, as `Display` writes it.
    pub display: &'static str,
    /// The qBittorrent facade's `connection` (qBittorrent
    /// `PeerInfo::connectionType`).
    pub qbit_connection: &'static str,
    /// The qBittorrent facade's flag letter, when the kind has one; its
    /// meaning is `qbit_connection`.
    pub qbit_flag: Option<char>,
}

/// What the peer model reads off a connection's kind, in place of asking
/// which kind it is.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Capabilities {
    /// A peer connected to us from the port it listens on: uTP runs over the
    /// peer's listening UDP socket, while TCP connects from an ephemeral
    /// port.
    pub(crate) source_port_is_listen_port: bool,
    /// The PEX flag (BEP 11) our messages give a peer connected over it; 0
    /// when it has none.
    pub(crate) pex_flag: u8,
}

// The span a listener's accept task runs in: its parent, the listener's
// address, and whether it is the SSL listener.
type ListenSpan = fn(Option<tracing::Id>, SocketAddr, bool) -> tracing::Span;

// One row of the table.
struct Row {
    names: Names,
    capabilities: Capabilities,
    // The name of a listener's accept task, and its span, which takes the
    // same name.
    listen_task: &'static str,
    listen_span: ListenSpan,
}

const TCP: Row = Row {
    names: Names {
        key: "tcp",
        display: "tcp",
        qbit_connection: "BT",
        qbit_flag: None,
    },
    capabilities: Capabilities {
        source_port_is_listen_port: false,
        pex_flag: 0,
    },
    listen_task: "tcp_listen",
    listen_span: |parent, addr, tls| debug_span!(parent: parent, "tcp_listen", ?addr, tls),
};

const UTP: Row = Row {
    names: Names {
        key: "utp",
        display: "uTP",
        qbit_connection: "μTP",
        qbit_flag: Some('P'),
    },
    capabilities: Capabilities {
        source_port_is_listen_port: true,
        pex_flag: crate::peer_protocol::extended::ut_pex::PEX_UTP,
    },
    listen_task: "utp_listen",
    listen_span: |parent, addr, tls| debug_span!(parent: parent, "utp_listen", ?addr, tls),
};

impl ConnectionKind {
    /// Every one, in the order their counts are indexed.
    pub const ALL: &'static [Self] = &[Self::Tcp, Self::Utp];
    pub const COUNT: usize = Self::ALL.len();

    const fn row(self) -> &'static Row {
        match self {
            Self::Tcp => &TCP,
            Self::Utp => &UTP,
        }
    }

    pub const fn names(self) -> &'static Names {
        &self.row().names
    }

    pub(crate) const fn capabilities(self) -> &'static Capabilities {
        &self.row().capabilities
    }

    /// The name of a listener's accept task over this kind.
    pub(crate) const fn listen_task(self) -> &'static str {
        self.row().listen_task
    }

    /// The span of a listener's accept task over this kind, under `parent`,
    /// for the listener on `addr`, the SSL one when `tls`.
    pub(crate) fn listen_span(
        self,
        parent: Option<tracing::Id>,
        addr: SocketAddr,
        tls: bool,
    ) -> tracing::Span {
        (self.row().listen_span)(parent, addr, tls)
    }

    /// The name metrics and the swarm counts read it under.
    pub const fn as_str(self) -> &'static str {
        self.names().key
    }
}

impl std::fmt::Display for ConnectionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.names().display)
    }
}

impl Serialize for ConnectionKind {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit_variant("ConnectionKind", *self as u32, self.names().key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Every name of every kind is the string each site wrote before the
    // table, and the kinds index their counts in order.
    #[test]
    fn every_name_is_the_one_each_site_wrote() {
        let table: Vec<_> = ConnectionKind::ALL
            .iter()
            .map(|&kind| {
                let names = kind.names();
                (
                    kind as usize,
                    kind.to_string(),
                    serde_json::to_string(&kind).unwrap(),
                    kind.as_str(),
                    names.qbit_connection,
                    names.qbit_flag,
                )
            })
            .collect();
        assert_eq!(
            table,
            [
                (0, "tcp".to_owned(), "\"tcp\"".to_owned(), "tcp", "BT", None),
                (
                    1,
                    "uTP".to_owned(),
                    "\"utp\"".to_owned(),
                    "utp",
                    "μTP",
                    Some('P')
                ),
            ]
        );
        assert_eq!(ConnectionKind::COUNT, 2);
        assert_eq!(
            serde_json::to_string(&Some(ConnectionKind::Utp)).unwrap(),
            "\"utp\""
        );
    }
}
