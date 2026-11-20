// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The key of the torrent session a route's leg runs: one session per leg.

use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};

use super::{EgressBinding, LegPath, Route};

/// A resolved route's identity: the leg it dials over.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RouteKey(String);

impl RouteKey {
    pub fn of(egress_id: u32, binding: &EgressBinding, path: &LegPath) -> Self {
        let encoded =
            serde_json::to_vec(&(egress_id, binding, path)).expect("route legs always encode");
        Self(hex::encode(&Sha1::digest(&encoded)[..8]))
    }

    /// The key of the session a pool route through `egress_id` runs on its
    /// member, proxy `member`: one proxy on one egress runs one session,
    /// whichever pools and routes hold it.
    pub fn member(egress_id: u32, binding: &EgressBinding, member: u32) -> Self {
        let encoded = serde_json::to_vec(&("member", egress_id, binding, member))
            .expect("route legs always encode");
        Self(hex::encode(&Sha1::digest(&encoded)[..8]))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The key of a route no torrent may run on, as stored: its own, so holding
/// it holds no session another route's leg runs.
pub fn refused_key(route: &Route) -> RouteKey {
    let encoded = serde_json::to_vec(&("refused", route)).expect("routes always encode");
    RouteKey(hex::encode(&Sha1::digest(&encoded)[..8]))
}
