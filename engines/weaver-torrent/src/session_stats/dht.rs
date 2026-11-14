// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a DHT node sent, was sent and found, counted as it happens.
//!
//! The node keeps its own counts: one relaxed atomic add per query, reply
//! or datagram, in arrays indexed by small enums. Nothing names a node, an
//! address or a torrent.

use serde::Serialize;

use super::swarm::{Tally, add, counted, read};

counted! {
    /// A DHT query, by what it asks (BEP 5).
    DhtQuery {
        Ping => "ping",
        FindNode => "find_node",
        GetPeers => "get_peers",
        AnnouncePeer => "announce_peer",
    }
}

counted! {
    /// How a query we sent ended.
    DhtQueryEnd {
        /// The node answered.
        Reply => "reply",
        /// The node answered with an error.
        Error => "error",
        /// No answer in the time a query is given.
        Timeout => "timeout",
    }
}

counted! {
    /// Why a datagram that reached the node was not acted on.
    DhtIgnored {
        /// Not shaped like a message, or one that does not parse.
        Malformed => "malformed",
        /// Its source sends too many.
        SourceLimit => "source_limit",
        /// A reply to no query of ours that still waits.
        Unmatched => "unmatched",
        /// A query that came while the node's send budget was spent.
        NoBudget => "no_budget",
    }
}

/// The counts of one DHT node.
#[derive(Debug, Default)]
pub struct DhtCounts {
    sent: Tally<{ DhtQuery::COUNT }>,
    ended: [Tally<{ DhtQueryEnd::COUNT }>; DhtQuery::COUNT],
    received: Tally<{ DhtQuery::COUNT }>,
    ignored: Tally<{ DhtIgnored::COUNT }>,
    peers_found: Tally<1>,
}

impl DhtCounts {
    /// A query of ours went out.
    #[inline]
    pub fn sent(&self, query: DhtQuery) {
        add(&self.sent, query as usize, 1);
    }

    /// A query of ours that went out ended.
    #[inline]
    pub fn ended(&self, query: DhtQuery, end: DhtQueryEnd) {
        add(&self.ended[query as usize], end as usize, 1);
    }

    /// A node's query is being answered.
    #[inline]
    pub fn received(&self, query: DhtQuery) {
        add(&self.received, query as usize, 1);
    }

    /// A datagram was not acted on.
    #[inline]
    pub fn ignored(&self, why: DhtIgnored) {
        add(&self.ignored, why as usize, 1);
    }

    /// A reply named `peers` peers of a torrent searched for.
    #[inline]
    pub fn peers_found(&self, peers: u64) {
        add(&self.peers_found, 0, peers);
    }

    pub fn snapshot(&self) -> DhtSnapshot {
        DhtSnapshot {
            sent: read(&self.sent),
            ended: self.ended.each_ref().map(read),
            received: read(&self.received),
            ignored: read(&self.ignored),
            peers_found: read(&self.peers_found)[0],
        }
    }
}

/// [`DhtCounts`] as read at one moment. Each array is indexed by the enums
/// named, in the order given.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct DhtSnapshot {
    /// The queries sent, by [`DhtQuery`].
    pub sent: [u64; DhtQuery::COUNT],
    /// The sent queries that ended, by [`DhtQuery`], then [`DhtQueryEnd`].
    /// One withdrawn before its answer or its timeout is in neither.
    pub ended: [[u64; DhtQueryEnd::COUNT]; DhtQuery::COUNT],
    /// The queries answered, by [`DhtQuery`].
    pub received: [u64; DhtQuery::COUNT],
    /// The datagrams not acted on, by [`DhtIgnored`].
    pub ignored: [u64; DhtIgnored::COUNT],
    /// The peers the replies to our searches named, repeats included.
    pub peers_found: u64,
}
