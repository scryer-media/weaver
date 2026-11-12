// SPDX-License-Identifier: GPL-3.0-or-later

//! The limit on the datagrams the DHT reads from one source, answers to its
//! own queries among them (libtorrent `dos_blocker::incoming`, called for
//! every datagram in dht_tracker.cpp:538, v2.0.15).
//!
//! The thresholds are libtorrent's: its `dht_block_ratelimit` of 5 messages a
//! second is counted over 10 second windows, so the 50th message one source
//! sends within a window is ignored and starts a block; `dht_block_timeout`
//! ignores the source for 5 minutes from then; and its blocker tracks 20
//! sources at once.

use std::{net::IpAddr, time::Duration};

use tokio::time::Instant;

// The query that makes this many from one source within one window is not
// answered, and starts the source's block.
pub(crate) const QUERY_LIMIT: u32 = 50;
pub(crate) const WINDOW: Duration = Duration::from_secs(10);
// How long a source is ignored from the query that reached QUERY_LIMIT.
pub(crate) const BLOCK: Duration = Duration::from_secs(5 * 60);
// The sources tracked at once. A new one takes the place of the quietest.
const TRACKED_SOURCES: usize = 20;

struct Source {
    ip: IpAddr,
    // The queries counted in the current window; at QUERY_LIMIT the source is
    // blocked.
    queries: u32,
    // The end of the current window, or of the block.
    until: Instant,
}

pub(crate) struct QueryLimiter {
    sources: Vec<Source>,
}

impl QueryLimiter {
    pub(crate) fn new() -> Self {
        Self {
            sources: Vec::with_capacity(TRACKED_SOURCES),
        }
    }

    /// Whether a query from `ip` that arrived at `now` is to be answered.
    pub(crate) fn admit(&mut self, ip: IpAddr, now: Instant) -> bool {
        let fresh = Source {
            ip,
            queries: 1,
            until: now + WINDOW,
        };
        let Some(source) = self.sources.iter_mut().find(|s| s.ip == ip) else {
            if self.sources.len() < TRACKED_SOURCES {
                self.sources.push(fresh);
            } else if let Some(quietest) =
                self.sources.iter_mut().min_by_key(|s| (s.queries, s.until))
            {
                *quietest = fresh;
            }
            return true;
        };
        if now >= source.until {
            // Its window, or its block, is over: a new window starts.
            *source = fresh;
            return true;
        }
        if source.queries >= QUERY_LIMIT {
            return false;
        }
        source.queries += 1;
        if source.queries == QUERY_LIMIT {
            source.until = now + BLOCK;
            return false;
        }
        true
    }
}
