// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use parking_lot::Mutex;
use url::Url;

use crate::core::{
    hash_id::Id20,
    peer_id::{generate_azereus_style, url_random},
};
use crate::crate_version;
use crate::torrent_state::stats::TorrentTotals;
use crate::tracker::{AnnounceShape, Leg};

/// How a torrent names itself to its peers and trackers. A torrent keeps one
/// for as long as it is kept: every run, session and tracker task of it goes
/// by the same peer id and tracker key, and sends each tracker back the
/// `tracker id` it last gave.
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
    /// What each HTTP tracker gave it to be known by.
    pub tracker_ids: TrackerIds,
    /// Whether a tracker has been told it completed.
    pub completion: Completion,
    /// Where its announces stood when a route move took it from its last
    /// session.
    pub announcing: Announcing,
    /// How its announces are written.
    pub shape: &'static AnnounceShape,
}

/// The `tracker id` each HTTP tracker last gave a torrent, sent back on every
/// later announce to it. Every copy of an identity holds the same one, so it
/// lasts as long as the identity is kept: across pauses, tracker tasks and
/// sessions, not past the process. It is what the torrent was told, not who
/// it is: two identities compare equal whatever theirs hold.
#[derive(Clone, Default)]
pub struct TrackerIds(Arc<Mutex<HashMap<Url, Vec<u8>>>>);

impl TrackerIds {
    /// The id `tracker` last gave, if any.
    pub(crate) fn get(&self, tracker: &Url) -> Option<Vec<u8>> {
        self.0.lock().get(tracker).cloned()
    }

    /// `tracker` gave `id`: it is sent back from now on.
    pub(crate) fn set(&self, tracker: &Url, id: &[u8]) {
        self.0.lock().insert(tracker.clone(), id.to_vec());
    }
}

impl PartialEq for TrackerIds {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for TrackerIds {}

/// Whether the torrent's trackers have been told it completed (libtorrent
/// `torrent::m_complete_sent`). Kept as [`TrackerIds`] is: every copy of an
/// identity holds the same one, so a `completed` no tracker accepted before a
/// pause or a move to another session is still owed after it. It does not
/// outlast the process, as libtorrent writes no `m_complete_sent` to resume
/// data. Two identities compare equal whatever theirs hold.
#[derive(Clone, Default)]
pub struct Completion(Arc<AtomicU8>);

/// No check of the torrent's pieces has ended under this identity.
const UNCHECKED: u8 = 0;
/// A check found the torrent short of a seed, and no tracker has accepted
/// `completed` since: once a seed, it owes one.
const UNSENT: u8 = 1;
/// A tracker accepted `completed`, or a check found a seed.
const SENT: u8 = 2;

impl Completion {
    /// A tracker accepted `completed`, or none need be told it
    /// (libtorrent `m_complete_sent`). Until a check of its pieces ends a
    /// torrent has sent none, as libtorrent's starts false.
    pub(crate) fn is_sent(&self) -> bool {
        self.0.load(Ordering::Relaxed) == SENT
    }

    /// A tracker accepted `completed` (libtorrent `torrent::tracker_response`,
    /// torrent.cpp:3608-3613, v2.0.15): no tracker is sent another.
    pub(crate) fn sent(&self) {
        self.0.store(SENT, Ordering::Relaxed);
    }

    /// A check of the torrent's pieces ended and found it a `seed` or not;
    /// `full` when it trusted nothing saved (a forced recheck, or the
    /// restart after an error). A seed found by the first check, or by a
    /// full one, is taken to have sent `completed` when it finished
    /// (libtorrent `torrent::files_checked`, torrent.cpp:8807-8820, v2.0.15).
    /// Any other check keeps what came before: a torrent added to another
    /// session under this identity still owes the `completed` it owed.
    pub(crate) fn checked(&self, seed: bool, full: bool) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |state| match state {
                _ if seed && (full || state == UNCHECKED) => Some(SENT),
                UNCHECKED => Some(UNSENT),
                _ => None,
            });
    }
}

impl PartialEq for Completion {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Completion {}

impl std::fmt::Debug for Completion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Completion")
            .field(&self.0.load(Ordering::Relaxed))
            .finish()
    }
}

/// Where a torrent's announces stood when a route move took it out of one
/// session to add it to another: a move is not a start, so the new session's
/// announces go on from there. Its counters go on from the totals its last
/// start counted from, and a tracker that answered `started` hears no other
/// one. Kept as [`TrackerIds`] is; the session the torrent is added to takes
/// it, and a run that is a start forgets it.
#[derive(Clone, Default)]
pub struct Announcing(Arc<Mutex<Option<Handed>>>);

/// The trackers that answered `started` under each hash, over each leg,
/// with the corrupt and redundant bytes the torrent had counted when it was
/// sent over it. A leg that never answered one is not listed, so it is
/// sent `started` after the move.
pub(crate) type Started = HashMap<(Id20, Url, Leg), (u64, u64)>;

/// What [`Announcing`] holds between two sessions.
#[derive(Default)]
struct Handed {
    /// What the torrent had moved when its announces began counting, in the
    /// totals the new session is added with.
    from: Option<TorrentTotals>,
    /// The trackers that answered `started`, under each hash.
    started: Started,
}

impl Announcing {
    /// A route move takes the torrent from its session: its announces
    /// counted from `from`, and `started` answered `started`.
    pub(crate) fn hand_over(&self, from: TorrentTotals, started: Started) {
        *self.0.lock() = Some(Handed {
            from: Some(from),
            started,
        });
    }

    /// The torrent left its session by any other way than a move, or joins
    /// the next one paused: its next announces are a start.
    pub(crate) fn forget(&self) {
        *self.0.lock() = None;
    }

    /// What the announces of the session taking the torrent count from,
    /// if a move handed it over.
    pub(crate) fn take_from(&self) -> Option<TorrentTotals> {
        self.0.lock().as_mut().and_then(|h| h.from.take())
    }

    /// The trackers that answered `started` under `info_hash` before the
    /// move, over each leg, taken by the announces that go on under it.
    pub(crate) fn take_started(&self, info_hash: Id20) -> HashMap<(Url, Leg), (u64, u64)> {
        let mut handed = self.0.lock();
        let Some(handed) = handed.as_mut() else {
            return HashMap::new();
        };
        let mut taken = HashMap::new();
        handed.started.retain(|(hash, url, leg), at| {
            let ours = *hash == info_hash;
            if ours {
                taken.insert((url.clone(), *leg), *at);
            }
            !ours
        });
        taken
    }
}

impl PartialEq for Announcing {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl Eq for Announcing {}

impl std::fmt::Debug for Announcing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Announcing")
            .field(&self.0.lock().as_ref().map(|h| h.started.len()))
            .finish()
    }
}

impl std::fmt::Debug for TrackerIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TrackerIds")
            .field(&self.0.lock().len())
            .finish()
    }
}

impl Identity {
    /// The same client under a fresh peer id and tracker key: the eight
    /// bytes that name the client and the shape of its announces are kept,
    /// the rest drawn anew.
    pub fn same_client(&self) -> Self {
        let mut peer_id = [0u8; 20];
        peer_id[..8].copy_from_slice(&self.peer_id.0[..8]);
        url_random(&mut peer_id[8..]);
        Self {
            peer_id: Id20::new(peer_id),
            tracker_key: rand::random(),
            version: self.version.clone(),
            user_agent: self.user_agent.clone(),
            tracker_ids: TrackerIds::default(),
            completion: Completion::default(),
            announcing: Announcing::default(),
            shape: self.shape,
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
            tracker_ids: TrackerIds::default(),
            completion: Completion::default(),
            announcing: Announcing::default(),
            shape: &AnnounceShape::WEAVER,
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
            tracker_ids: TrackerIds::default(),
            completion: Completion::default(),
            announcing: Announcing::default(),
            shape: &crate::tracker::qbittorrent::SHAPE,
        };
        let same = identity.same_client();
        assert_eq!(&same.peer_id.0[..8], b"-qB5230-");
        assert_ne!(same.peer_id, identity.peer_id);
        assert_eq!(same.shape, identity.shape);
        assert_eq!(
            (same.version.as_str(), same.user_agent.as_str()),
            ("qBittorrent/5.2.3", "qBittorrent/5.2.3")
        );
    }
}
