// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How a tracker is spoken to. Each protocol is one [`TrackerProtocol`], and
//! one table, [`TrackerProtocols`], maps a URL scheme to the protocol that
//! speaks it. The tracker task asks a tracker's protocol;
//! they never ask which protocol it is. A new protocol is one impl and one
//! row of [`TrackerProtocols::new`].

use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use url::Url;

use crate::Identity;
use crate::core::hash_id::Id20;
use crate::core::net::HttpClients;
use crate::session_stats::tracker::{TrackerKind, TrackerResult};
use crate::tracker::tracker_comms_http::TrackerRequestEvent;
use crate::tracker::tracker_comms_udp::UdpTrackerClient;

use super::{
    Announced, Events, HttpAnnounces, HttpTracker, Leg, ListenPort, Reachable, SupportedTracker,
    TrackerCommsStats, UdpTracker,
};

/// A turn an announce holds while it is out, handed on once dropped.
pub(super) type Turn = Box<dyn Send>;

/// One protocol trackers are spoken to over.
pub(super) trait TrackerProtocol: Send + Sync {
    /// What its requests are counted under.
    fn kind(&self) -> TrackerKind;

    /// The turn an announce waits for before it goes out, outside its time
    /// limit; None when it waits for none. `event` is whether the announce
    /// carries an event.
    fn gate(&self, _event: bool) -> Option<BoxFuture<'_, Turn>> {
        None
    }

    /// One announce, over every leg its events name, as one answer.
    fn announce<'a>(&'a self, ask: AnnounceAsk<'a>) -> BoxFuture<'a, anyhow::Result<Announced>>;

    /// What a cause of a failed request is counted as, when it is one of
    /// this protocol's own; None leaves it to the causes every protocol
    /// shares.
    fn classify(&self, _cause: &(dyn std::error::Error + 'static)) -> Option<TrackerResult> {
        None
    }
}

/// The corrupt and redundant bytes counted when each tracker last heard
/// `started` over each leg.
pub(super) type StartedAt = std::collections::HashMap<(Url, Leg), (u64, u64)>;

/// What an announce is sent with.
pub(super) struct AnnounceAsk<'a> {
    pub tracker: &'a SupportedTracker,
    pub info_hash: Id20,
    pub identity: &'a Identity,
    pub events: Events,
    pub stats: &'a TrackerCommsStats,
    /// The ports peers reach the session on, read as the announce goes out.
    pub ports: &'a ListenPort,
    /// The corrupt and redundant bytes counted when each tracker last heard
    /// `started` over each leg.
    pub started_at: &'a Mutex<StartedAt>,
}

impl AnnounceAsk<'_> {
    /// The corrupt and redundant bytes counted since the tracker last heard
    /// `started` over `leg`; an announce that is `started` begins the count
    /// again. A leg that heard no `started` from this run hears none
    /// counted.
    pub fn since_started(&self, leg: Leg, event: Option<TrackerRequestEvent>) -> (u64, u64) {
        let now = (self.stats.corrupt_bytes, self.stats.redundant_bytes);
        let mut started_at = self.started_at.lock();
        let key = (self.tracker.url.clone(), leg);
        if event == Some(TrackerRequestEvent::Started) {
            started_at.insert(key.clone(), now);
        }
        let (corrupt, redundant) = started_at.get(&key).copied().unwrap_or(now);
        (
            now.0.saturating_sub(corrupt),
            now.1.saturating_sub(redundant),
        )
    }
}

/// What a torrent's trackers are reached through.
pub struct TrackerNet {
    pub http_clients: Arc<dyn HttpClients>,
    /// Every address a tracker is asked at, on every hop, passes it.
    pub reachable: Reachable,
    /// Speaks to UDP trackers, and its resolver looks up the name of every
    /// tracker, HTTP or UDP.
    pub udp_client: UdpTrackerClient,
    /// The turns HTTP announces wait for, shared across the session.
    pub http_announces: HttpAnnounces,
    /// Whether an HTTP tracker whose host has an internationalized label may
    /// be asked.
    pub allow_idna: bool,
}

/// The protocols a torrent's trackers are spoken to over, by URL scheme.
/// Clones share the protocols.
#[derive(Clone)]
pub struct TrackerProtocols(Arc<[(&'static str, Arc<dyn TrackerProtocol>)]>);

impl TrackerProtocols {
    /// The table: one row per scheme the session announces over.
    pub fn new(net: TrackerNet) -> Self {
        let http: Arc<dyn TrackerProtocol> = Arc::new(HttpTracker::new(&net));
        let udp: Arc<dyn TrackerProtocol> = Arc::new(UdpTracker::new(&net));
        Self::from_rows(vec![("http", http.clone()), ("https", http), ("udp", udp)])
    }

    pub(super) fn from_rows(rows: Vec<(&'static str, Arc<dyn TrackerProtocol>)>) -> Self {
        Self(rows.into())
    }

    /// The protocol `scheme` names, None when the session speaks none.
    pub(super) fn of(&self, scheme: &str) -> Option<&Arc<dyn TrackerProtocol>> {
        self.0
            .iter()
            .find(|(named, _)| *named == scheme)
            .map(|(_, protocol)| protocol)
    }
}
