// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::core::{hash_id::Id20, peer_id::generate_azereus_style};
use crate::crate_version;

/// How a torrent names itself to its peers and trackers. A torrent keeps one
/// for as long as it is kept: every run, session and tracker task of it goes
/// by the same peer id and tracker key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Its peer id in handshakes and announces.
    pub peer_id: Id20,
    /// The `key` of its announces: one per torrent, whatever hash it
    /// announces under.
    pub tracker_key: u32,
    /// The `v` of its extended handshake.
    pub version: String,
    /// The User-Agent of its HTTP announces.
    pub user_agent: String,
}

impl Default for Identity {
    // This build, under a fresh peer id and tracker key.
    fn default() -> Self {
        Self {
            peer_id: generate_azereus_style(*b"WV", crate_version!()),
            tracker_key: rand::random(),
            version: crate::client_name_and_version().to_owned(),
            user_agent: crate::client_name_and_version().to_owned(),
        }
    }
}
