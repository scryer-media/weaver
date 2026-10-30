// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! How a client names itself to peers and trackers.

use weaver_torrent::core::Id20;
use weaver_torrent::core::peer_id::url_random;
use weaver_torrent::{Identity, TrackerIds};

/// A client's Azureus-style peer-id prefix, the User-Agent of its HTTP
/// announces and the `v` of its extended handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientIdentity {
    prefix: [u8; 8],
    user_agent: &'static str,
    version: &'static str,
}

/// The identity of this build: `-WV<major><minor:02><patch>-`, `Weaver/<version>`.
pub const IDENTITY: ClientIdentity = ClientIdentity::new(
    prefix_for(env!("CARGO_PKG_VERSION")),
    concat!("Weaver/", env!("CARGO_PKG_VERSION")),
    concat!("Weaver/", env!("CARGO_PKG_VERSION")),
);

const DIGITS: &[u8; 64] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz.-";

/// The eight prefix bytes for a `major.minor.patch` version. Major and patch
/// take one digit each from the Azureus alphabet, minor two decimal digits.
const fn prefix_for(version: &str) -> [u8; 8] {
    let bytes = version.as_bytes();
    let mut parts = [0usize; 3];
    let mut part = 0;
    let mut at = 0;
    while at < bytes.len() && part < 3 {
        let byte = bytes[at];
        if byte == b'.' {
            part += 1;
        } else if byte.is_ascii_digit() {
            parts[part] = parts[part] * 10 + (byte - b'0') as usize;
        } else {
            break;
        }
        at += 1;
    }
    assert!(parts[0] < 64 && parts[1] < 100 && parts[2] < 64);
    [
        b'-',
        b'W',
        b'V',
        DIGITS[parts[0]],
        b'0' + (parts[1] / 10) as u8,
        b'0' + (parts[1] % 10) as u8,
        DIGITS[parts[2]],
        b'-',
    ]
}

impl ClientIdentity {
    pub const fn new(prefix: [u8; 8], user_agent: &'static str, version: &'static str) -> Self {
        Self {
            prefix,
            user_agent,
            version,
        }
    }

    pub const fn prefix(&self) -> &[u8; 8] {
        &self.prefix
    }

    pub const fn user_agent(&self) -> &'static str {
        self.user_agent
    }

    pub const fn version(&self) -> &'static str {
        self.version
    }

    /// This client under a fresh peer id and tracker key.
    pub fn identity(&self) -> Identity {
        self.identity_of(&TorrentIdentity::fresh())
    }

    /// This client as the torrent `own` is: its peer id carries this
    /// client's prefix before the torrent's own bytes, and it sends each
    /// tracker back the id that tracker gave `own`.
    pub fn identity_of(&self, own: &TorrentIdentity) -> Identity {
        Identity {
            peer_id: self.peer_id_of(own),
            tracker_key: own.tracker_key,
            version: self.version.to_owned(),
            user_agent: self.user_agent.to_owned(),
            tracker_ids: own.tracker_ids.clone(),
        }
    }

    fn peer_id_of(&self, own: &TorrentIdentity) -> Id20 {
        let mut id = [0u8; 20];
        id[..8].copy_from_slice(&self.prefix);
        id[8..].copy_from_slice(&own.peer_id);
        Id20::new(id)
    }
}

/// Who a torrent is to its peers and trackers for as long as it is kept in
/// this process: the bytes of its peer id after the client's prefix, its
/// tracker key, and the `tracker id` each tracker gave it. A pause, a
/// session reopening or a route change keeps it; a restart or a remove and
/// add again makes another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TorrentIdentity {
    peer_id: [u8; 12],
    tracker_key: u32,
    tracker_ids: TrackerIds,
}

impl TorrentIdentity {
    pub fn fresh() -> Self {
        let mut peer_id = [0u8; 12];
        url_random(&mut peer_id);
        let mut key = [0u8; 4];
        getrandom::fill(&mut key).expect("system randomness");
        Self {
            peer_id,
            tracker_key: u32::from_le_bytes(key),
            tracker_ids: TrackerIds::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_encodes_major_minor_patch() {
        assert_eq!(&prefix_for("0.17.0"), b"-WV0170-");
        assert_eq!(&prefix_for("1.2.3"), b"-WV1023-");
        assert_eq!(&prefix_for("0.16.12"), b"-WV016C-");
        assert_eq!(&prefix_for("0.17.0-rc.1"), b"-WV0170-");
    }

    #[test]
    fn identity_follows_the_package_version() {
        assert_eq!(IDENTITY.prefix(), &prefix_for(env!("CARGO_PKG_VERSION")));
        assert_eq!(
            IDENTITY.user_agent(),
            format!("Weaver/{}", env!("CARGO_PKG_VERSION"))
        );
    }

    #[test]
    fn peer_ids_share_the_prefix_and_differ() {
        let first = IDENTITY.identity().peer_id;
        let second = IDENTITY.identity().peer_id;
        assert_eq!(&first.0[..8], IDENTITY.prefix());
        assert_eq!(&second.0[..8], IDENTITY.prefix());
        assert_ne!(first, second);
        assert!(weaver_torrent::core::peer_id::try_decode_peer_id(first).is_some());
    }
}
