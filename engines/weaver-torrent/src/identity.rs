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
    /// The User-Agent of its HTTP announces and web-seed requests.
    pub user_agent: String,
}

impl Identity {
    /// The same client under a fresh peer id and tracker key: the eight
    /// bytes that name the client are kept, the rest drawn anew.
    pub fn same_client(&self) -> Self {
        let mut peer_id: [u8; 20] = rand::random();
        peer_id[..8].copy_from_slice(&self.peer_id.0[..8]);
        Self {
            peer_id: Id20::new(peer_id),
            tracker_key: rand::random(),
            version: self.version.clone(),
            user_agent: self.user_agent.clone(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_client_keeps_the_fingerprint_and_draws_the_rest() {
        let identity = Identity {
            peer_id: generate_azereus_style(*b"qB", (5, 2, 3, 0)),
            tracker_key: 1,
            version: "qBittorrent/5.2.3".into(),
            user_agent: "qBittorrent/5.2.3".into(),
        };
        let same = identity.same_client();
        assert_eq!(&same.peer_id.0[..8], b"-qB5230-");
        assert_ne!(same.peer_id, identity.peer_id);
        assert_eq!(
            (same.version.as_str(), same.user_agent.as_str()),
            ("qBittorrent/5.2.3", "qBittorrent/5.2.3")
        );
    }
}
