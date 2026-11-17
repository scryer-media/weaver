// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! What a session asked of its trackers, counted as each request ends.
//!
//! A count is one relaxed atomic add, in an array indexed by small enums.
//! Nothing here names a tracker: the counts are of the session as a whole.

use std::time::Duration;

use serde::Serialize;

use super::swarm::{Tally, add, counted, read};

counted! {
    /// How a tracker is spoken to.
    TrackerKind {
        Http => "http",
        Udp => "udp",
    }
}

counted! {
    /// The event an announce carried.
    AnnounceEvent {
        /// A regular announce, with no event.
        Periodic => "periodic",
        Started => "started",
        Completed => "completed",
        Stopped => "stopped",
        /// BEP 21: every wanted piece is here and the torrent is no seed.
        Paused => "paused",
    }
}

counted! {
    /// How a request to a tracker ended.
    TrackerResult {
        Ok => "ok",
        /// No answer in the time a request is given.
        Timeout => "timeout",
        /// The tracker answered with a failure of its own.
        Refused => "refused",
        /// The tracker answered with something that does not parse.
        BadReply => "bad_reply",
        /// No address of the tracker is of a family the session connects
        /// over, so nothing was sent.
        Skipped => "skipped",
        /// Anything else: no route, a refused connection, a failed lookup,
        /// a filtered address, an HTTP status that is no answer.
        Error => "error",
    }
}

/// The tracker counts of one session, over every torrent on it.
#[derive(Debug, Default)]
pub struct TrackerStats {
    announces: [[Tally<{ TrackerResult::COUNT }>; AnnounceEvent::COUNT]; TrackerKind::COUNT],
    announce_nanos: Tally<{ TrackerKind::COUNT }>,
    peers: Tally<{ TrackerKind::COUNT }>,
}

fn nanos(took: Duration) -> u64 {
    u64::try_from(took.as_nanos()).unwrap_or(u64::MAX)
}

impl TrackerStats {
    /// An announce to one tracker ended after `took`, naming `peers` peers.
    pub fn announced(
        &self,
        kind: TrackerKind,
        event: AnnounceEvent,
        result: TrackerResult,
        took: Duration,
        peers: usize,
    ) {
        add(
            &self.announces[kind as usize][event as usize],
            result as usize,
            1,
        );
        add(&self.announce_nanos, kind as usize, nanos(took));
        add(&self.peers, kind as usize, peers as u64);
    }

    pub fn snapshot(&self) -> TrackerSnapshot {
        TrackerSnapshot {
            announces: self.announces.each_ref().map(|by| by.each_ref().map(read)),
            announce_nanos: read(&self.announce_nanos),
            peers: read(&self.peers),
        }
    }
}

/// [`TrackerStats`] as read at one moment. Each array is indexed by the
/// enums named, in the order given.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TrackerSnapshot {
    /// By [`TrackerKind`], [`AnnounceEvent`], then [`TrackerResult`].
    pub announces: [[[u64; TrackerResult::COUNT]; AnnounceEvent::COUNT]; TrackerKind::COUNT],
    /// The time the announces that ended took, by [`TrackerKind`].
    pub announce_nanos: [u64; TrackerKind::COUNT],
    /// The peers accepted announces named, by [`TrackerKind`].
    pub peers: [u64; TrackerKind::COUNT],
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_announce_is_counted_under_its_kind_event_and_result() {
        let stats = TrackerStats::default();
        stats.announced(
            TrackerKind::Udp,
            AnnounceEvent::Started,
            TrackerResult::Ok,
            Duration::from_millis(250),
            40,
        );
        stats.announced(
            TrackerKind::Udp,
            AnnounceEvent::Periodic,
            TrackerResult::Timeout,
            Duration::from_secs(30),
            0,
        );

        let read = stats.snapshot();
        let udp = TrackerKind::Udp as usize;
        assert_eq!(
            read.announces[udp][AnnounceEvent::Started as usize][TrackerResult::Ok as usize],
            1
        );
        assert_eq!(
            read.announces[udp][AnnounceEvent::Periodic as usize][TrackerResult::Timeout as usize],
            1
        );
        assert_eq!(
            read.announces
                .as_flattened()
                .as_flattened()
                .iter()
                .sum::<u64>(),
            2
        );
        assert_eq!(read.announce_nanos[udp], 30_250_000_000);
        assert_eq!(read.peers, [0, 40]);
    }
}
