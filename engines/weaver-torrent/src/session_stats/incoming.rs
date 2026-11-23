// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What became of the connections peers opened to a session, counted as
//! each is taken or turned away: one relaxed atomic add per connection.

use serde::Serialize;

use super::swarm::{Tally, Transport, add, counted, read};

counted! {
    /// How a connection a peer opened to us ended its way in.
    IncomingEnd {
        /// Handed to the torrent it greeted.
        Accepted => "accepted",
        /// The session holds as many connections as it may.
        SessionLimit => "session_limit",
        /// From an address the IP filter blocks.
        Filtered => "filtered",
        /// From an address of a family the session does not connect over.
        Family => "family",
        /// From an address banned for sending bad data.
        Banned => "banned",
        /// From a privileged port, which the session refuses.
        PrivilegedPort => "privileged_port",
        /// The TLS handshake of an SSL torrent failed or named no torrent.
        Tls => "tls",
        /// The encryption handshake failed.
        Encryption => "encryption",
        /// The BitTorrent handshake did not come whole in time.
        Handshake => "handshake",
        /// It greeted a torrent the session does not have.
        UnknownTorrent => "unknown_torrent",
        /// It came from this session.
        Ourselves => "ourselves",
        /// The torrent takes no connection of this kind: its encryption
        /// rule, or an SSL torrent greeted outside its own TLS.
        NotAllowed => "not_allowed",
        /// The torrent is not running.
        NotLive => "not_live",
        /// The torrent already has a connection with this peer.
        AlreadyConnected => "already_connected",
        /// The torrent holds as many peers as it may.
        TorrentLimit => "torrent_limit",
        /// The torrent is finished and uploads nothing.
        NotWanted => "not_wanted",
        /// The torrent could not take it for another reason.
        Error => "error",
    }
}

impl IncomingEnd {
    /// How the torrent's answer to a connection that passed the session's
    /// checks is counted.
    pub(crate) fn of(
        added: &anyhow::Result<crate::torrent_state::live::AddIncomingPeerResult>,
    ) -> Self {
        use crate::torrent_state::live::AddIncomingPeerResult as R;
        match added {
            Ok(R::Added) => Self::Accepted,
            Ok(R::AlreadyActive(_)) => Self::AlreadyConnected,
            Ok(R::ConcurrencyLimitReached | R::PeerListFull) => Self::TorrentLimit,
            Ok(R::NotWanted) => Self::NotWanted,
            Ok(R::Banned) => Self::Banned,
            Err(_) => Self::Error,
        }
    }
}

counted! {
    /// The connection we had with a peer when another came from it.
    Held {
        /// One we were still making.
        Dialling => "dialling",
        /// One we made, open.
        Ours => "ours",
        /// One the peer made, open.
        Theirs => "theirs",
    }
}

counted! {
    /// Our listen port against the port of the peer whose connection we had.
    PortOrder {
        OursLower => "ours_lower",
        OursHigher => "ours_higher",
        Equal => "equal",
    }
}

counted! {
    /// Whether the connection came from the very address, port included,
    /// of the peer whose connection we had.
    FromAddress {
        Same => "same",
        Other => "other",
    }
}

/// What stood when a connection was turned away as
/// [`IncomingEnd::AlreadyConnected`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Already {
    pub held: Held,
    pub ports: PortOrder,
    pub from: FromAddress,
}

impl Already {
    pub const COUNT: usize = Held::COUNT * PortOrder::COUNT * FromAddress::COUNT;

    pub(crate) fn new(held: Held, our_port: u16, their_port: u16, same_address: bool) -> Self {
        Self {
            held,
            ports: match our_port.cmp(&their_port) {
                std::cmp::Ordering::Less => PortOrder::OursLower,
                std::cmp::Ordering::Greater => PortOrder::OursHigher,
                std::cmp::Ordering::Equal => PortOrder::Equal,
            },
            from: if same_address {
                FromAddress::Same
            } else {
                FromAddress::Other
            },
        }
    }

    /// Where its count is in [`IncomingSnapshot::already`].
    pub const fn index(self) -> usize {
        (self.held as usize * PortOrder::COUNT + self.ports as usize) * FromAddress::COUNT
            + self.from as usize
    }
}

/// The incoming-connection counts of one session.
#[derive(Debug, Default)]
pub struct IncomingStats {
    ends: [Tally<{ IncomingEnd::COUNT }>; Transport::COUNT],
    already: [Tally<{ Already::COUNT }>; Transport::COUNT],
}

impl IncomingStats {
    #[inline]
    pub fn ended(&self, transport: Transport, end: IncomingEnd) {
        add(&self.ends[transport as usize], end as usize, 1);
    }

    /// A connection counted as [`IncomingEnd::AlreadyConnected`]: what
    /// stood when it was turned away.
    #[inline]
    pub fn already(&self, transport: Transport, already: Already) {
        add(&self.already[transport as usize], already.index(), 1);
    }

    pub fn snapshot(&self) -> IncomingSnapshot {
        IncomingSnapshot {
            ends: self.ends.each_ref().map(read),
            already: self.already.each_ref().map(read),
        }
    }
}

/// [`IncomingStats`] as read at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct IncomingSnapshot {
    /// By [`Transport`], then [`IncomingEnd`].
    pub ends: [[u64; IncomingEnd::COUNT]; Transport::COUNT],
    /// By [`Transport`], then [`Already::index`].
    pub already: [[u64; Already::COUNT]; Transport::COUNT],
}
