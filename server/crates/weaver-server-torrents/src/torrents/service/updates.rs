// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' views, followed as they move, read from the snapshot of
//! every torrent's row: a follower builds no view of its own, and wakes only
//! when the snapshot's revision moves.
//!
//! The first push is every followed torrent; each later one only the
//! torrents that changed or came, and the ids of those gone. A follower of a
//! few torrents (a list's page, at most `MAX_FOLLOWED`) is given their rows,
//! which leave out the fields that move with the clock alone when they tell
//! a change, and carry no files or trackers. One torrent's follower reads
//! that torrent whole, files and trackers with it, each time its row moves,
//! and tells a change in any field.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::watch;

use super::fingerprint::ViewParts;
use super::snapshot::TorrentRow;
use super::{NotReady, TorrentService, TorrentView};
use crate::jobs::JobId;

/// The most torrents one follower names.
pub const MAX_FOLLOWED: usize = 500;

/// The torrents' views as they move: one torrent's, or a few torrents'.
pub struct TorrentUpdates {
    service: TorrentService,
    follow: Follow,
    // The snapshot's revision.
    revision: watch::Receiver<u64>,
    // Each torrent last given, by its print.
    last: Option<HashMap<JobId, u64>>,
    // One torrent's follower: the revision its row had moved at when it was
    // last read, none while it has no row.
    seen: Option<u64>,
}

/// What moved since the last push.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentChanges {
    /// The first push: `torrents` is every followed torrent.
    pub full_update: bool,
    /// The torrents that changed or came, in the service's order.
    pub torrents: Vec<TorrentView>,
    /// The torrents gone since the last push.
    pub removed: Vec<JobId>,
}

/// Which torrents a follower follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Follow {
    /// One torrent, whole: its files, its trackers, and the fields that
    /// move with the clock alone.
    One(JobId),
    /// The torrents of a set, at most `MAX_FOLLOWED`, as a list shows them:
    /// a list shows a page of the torrents, and follows those alone.
    Some(BTreeSet<JobId>),
}

/// Why a follower is not made.
#[derive(Debug, thiserror::Error)]
pub enum FollowError {
    #[error(transparent)]
    NotReady(#[from] NotReady),
    #[error("a follower names at most {MAX_FOLLOWED} torrents, not {0}")]
    TooMany(usize),
}

impl TorrentService {
    /// Follow `job_id`'s view, whole.
    pub fn updates(&self, job_id: JobId) -> TorrentUpdates {
        self.updates_of(Follow::One(job_id))
    }

    /// Follow the views of the torrents `follow` names. A few torrents are
    /// followed only once every torrent's row is in the snapshot: asked
    /// before, it is an error.
    pub fn follow(&self, follow: Follow) -> Result<TorrentUpdates, FollowError> {
        if let Follow::Some(ids) = &follow {
            if ids.len() > MAX_FOLLOWED {
                return Err(FollowError::TooMany(ids.len()));
            }
            self.listing_ready()?;
        }
        Ok(self.updates_of(follow))
    }

    fn updates_of(&self, follow: Follow) -> TorrentUpdates {
        TorrentUpdates {
            service: self.clone(),
            follow,
            revision: self.inner.snapshot.subscribe(),
            last: None,
            seen: None,
        }
    }
}

impl TorrentUpdates {
    /// The views the next time they differ from the ones last given: every
    /// view on the first call, the changed, new and gone ones after.
    pub async fn next(&mut self) -> TorrentChanges {
        loop {
            // Taken before the rows are read, so a move after the read wakes
            // the wait.
            self.revision.borrow_and_update();
            if let Some(changes) = self.read().await {
                return changes;
            }
            if self.revision.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }

    async fn read(&mut self) -> Option<TorrentChanges> {
        let snapshot = self.service.snapshot();
        match &self.follow {
            Follow::One(job_id) => {
                let moved = snapshot.row(*job_id).map(|row| row.moved);
                if self.last.is_some() && moved == self.seen {
                    return None;
                }
                self.seen = moved;
                let views = self.service.torrent(*job_id).await.into_iter().collect();
                changes(&mut self.last, views, ViewParts::Whole)
            }
            Follow::Some(ids) => changes_of_rows(&mut self.last, &snapshot.rows_of(ids)),
        }
    }
}

// What `rows` moved from `last`, by their prints, kept as the new `last`;
// `None` when nothing did.
pub(super) fn changes_of_rows(
    last: &mut Option<HashMap<JobId, u64>>,
    rows: &[Arc<TorrentRow>],
) -> Option<TorrentChanges> {
    let now: HashMap<JobId, u64> = rows
        .iter()
        .map(|row| (row.view.job_id, row.print()))
        .collect();
    let before = last.replace(now);
    let now = last.as_ref().expect("just kept");
    told(before, now, rows.iter().map(|row| &row.view), Clone::clone)
}

// What `views` moved from `last` in the fields `parts` covers, kept as the
// new `last`; `None` when nothing did.
pub(super) fn changes(
    last: &mut Option<HashMap<JobId, u64>>,
    views: Vec<TorrentView>,
    parts: ViewParts,
) -> Option<TorrentChanges> {
    let now: HashMap<JobId, u64> = views
        .iter()
        .map(|view| (view.job_id, view.fingerprint(parts)))
        .collect();
    let before = last.replace(now);
    let now = last.as_ref().expect("just kept");
    told(before, now, views, std::convert::identity)
}

// The push from the prints `before` to `now` of `views`: all of them when
// there was none before, else the ones whose prints moved or are new and
// the ids of those gone.
fn told<V>(
    before: Option<HashMap<JobId, u64>>,
    now: &HashMap<JobId, u64>,
    views: impl IntoIterator<Item = V>,
    owned: impl Fn(V) -> TorrentView,
) -> Option<TorrentChanges>
where
    V: std::borrow::Borrow<TorrentView>,
{
    let Some(before) = before else {
        return Some(TorrentChanges {
            full_update: true,
            torrents: views.into_iter().map(owned).collect(),
            removed: Vec::new(),
        });
    };
    let mut removed: Vec<JobId> = before
        .keys()
        .filter(|job_id| !now.contains_key(job_id))
        .copied()
        .collect();
    removed.sort_unstable_by_key(|job_id| job_id.0);
    let torrents: Vec<TorrentView> = views
        .into_iter()
        .filter(|view| {
            let job_id = view.borrow().job_id;
            before.get(&job_id) != now.get(&job_id)
        })
        .map(owned)
        .collect();
    (!torrents.is_empty() || !removed.is_empty()).then_some(TorrentChanges {
        full_update: false,
        torrents,
        removed,
    })
}
