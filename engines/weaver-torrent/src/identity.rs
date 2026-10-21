// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use crate::core::{hash_id::Id20, peer_id::generate_azereus_style};
use crate::crate_version;

/// How a torrent names itself to its peers and trackers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Identity {
    /// Its peer id in handshakes and announces.
    pub peer_id: Id20,
    /// The `v` of its extended handshake.
    pub version: String,
    /// The User-Agent of its HTTP announces.
    pub user_agent: String,
}

impl Default for Identity {
    // This build, under a fresh peer id.
    fn default() -> Self {
        Self {
            peer_id: generate_azereus_style(*b"WV", crate_version!()),
            version: crate::client_name_and_version().to_owned(),
            user_agent: crate::client_name_and_version().to_owned(),
        }
    }
}
