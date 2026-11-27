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
//! that torrent with what it shows beside its own fields (its files, its
//! trackers, its tags), each time its row moves, and tells a change in any
//! field it reads. One torrent's files follower reads one page of its files
//! each time its files move, at most twice a second.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use tokio::sync::watch;

use super::fingerprint::ViewParts;
use super::listing::Shown;
use super::snapshot::TorrentRow;
use super::{FilesPage, NotReady, TorrentService, TorrentView};
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
    // One torrent's follower: the revisions its row and its files had moved
    // at when it was last read, none while it has no row.
    seen: Option<(u64, u64)>,
}

/// What moved since the last push.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentChanges {
    /// The first push: `torrents` is every followed torrent.
    pub full_update: bool,
    /// The torrents that changed or came, in the service's order.
    pub torrents: Vec<TorrentView>,
    /// For each of `torrents`, whether it is one file named as the torrent
    /// is: a list's view comes without its files, which would say.
    pub one_file: Vec<bool>,
    /// The torrents gone since the last push.
    pub removed: Vec<JobId>,
}

/// Which torrents a follower follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Follow {
    /// One torrent, with what `Shown` names beside its own fields, the
    /// fields that move with the clock alone included. A file's move is
    /// told only when its files are shown.
    One(JobId, Shown),
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
        self.updates_of(Follow::One(job_id, Shown::WHOLE))
    }

    /// Follow one page of `job_id`'s files: at most `limit` after the first
    /// `offset`.
    pub fn file_updates(&self, job_id: JobId, offset: usize, limit: usize) -> FileUpdates {
        FileUpdates {
            service: self.clone(),
            job_id,
            offset,
            limit,
            revision: self.inner.snapshot.subscribe(),
            seen: None,
            last: None,
            next_at: None,
        }
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
            Follow::One(job_id, shown) => {
                // A move of its files alone is read only when they are shown.
                let moved = snapshot
                    .row(*job_id)
                    .map(|row| (row.moved, if shown.files { row.files_moved } else { 0 }));
                if self.last.is_some() && moved == self.seen {
                    return None;
                }
                self.seen = moved;
                let views = self
                    .service
                    .torrent_read(*job_id, *shown)
                    .await
                    .map(|(mut view, one_file)| {
                        // The bytes of each file move with its files: printed
                        // only when they are shown.
                        if !shown.files {
                            view.file_have = Vec::new();
                        }
                        (view, one_file)
                    })
                    .into_iter()
                    .collect();
                changes_read(&mut self.last, views, ViewParts::Whole)
            }
            Follow::Some(ids) => changes_of_rows(&mut self.last, &snapshot.rows_of(ids)),
        }
    }
}

// What `rows` moved from `last`, by their prints, kept as the new `last`;
// `None` when nothing did.
pub fn changes_of_rows(
    last: &mut Option<HashMap<JobId, u64>>,
    rows: &[Arc<TorrentRow>],
) -> Option<TorrentChanges> {
    let now: HashMap<JobId, u64> = rows
        .iter()
        .map(|row| (row.view.job_id, row.print()))
        .collect();
    let before = last.replace(now);
    let now = last.as_ref().expect("just kept");
    told(
        before,
        now,
        rows.iter().map(|row| (&row.view, row.totals.one_file)),
        Clone::clone,
    )
}

// What `views` moved from `last` in the fields `parts` covers, kept as the
// new `last`; `None` when nothing did.
pub fn changes(
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
    let views = views.into_iter().map(|view| {
        let one_file = view.is_one_file();
        (view, one_file)
    });
    told(before, now, views, std::convert::identity)
}

// `changes` of views each read with whether it is one file named as the
// torrent is, which a view read without its files cannot say itself.
fn changes_read(
    last: &mut Option<HashMap<JobId, u64>>,
    views: Vec<(TorrentView, bool)>,
    parts: ViewParts,
) -> Option<TorrentChanges> {
    let now: HashMap<JobId, u64> = views
        .iter()
        .map(|(view, _)| (view.job_id, view.fingerprint(parts)))
        .collect();
    let before = last.replace(now);
    let now = last.as_ref().expect("just kept");
    told(before, now, views, std::convert::identity)
}

/// One page of one torrent's files as they move.
pub struct FileUpdates {
    service: TorrentService,
    job_id: JobId,
    offset: usize,
    limit: usize,
    // The snapshot's revision.
    revision: watch::Receiver<u64>,
    // The revisions its row and its files had moved at when it was last
    // read, none while it has no row.
    seen: Option<(u64, u64)>,
    // The page last given.
    last: Option<FilesPage>,
    // The earliest the next page is read.
    next_at: Option<tokio::time::Instant>,
}

/// The least time between two pages a files follower gives: at most two a
/// second.
pub const FILE_UPDATES_SPACING: std::time::Duration = std::time::Duration::from_millis(500);

impl FileUpdates {
    /// The page as it is on the first call; after, the page the next time
    /// the torrent's files move and it differs from the one last given, at
    /// least `FILE_UPDATES_SPACING` after it. None once the torrent is gone.
    pub async fn next(&mut self) -> Option<FilesPage> {
        loop {
            if let Some(at) = self.next_at {
                tokio::time::sleep_until(at).await;
            }
            // Taken before the page is read, so a move after the read wakes
            // the wait.
            self.revision.borrow_and_update();
            let moved = self
                .service
                .snapshot()
                .row(self.job_id)
                .map(|row| (row.moved, row.files_moved));
            if self.last.is_none() || moved != self.seen {
                self.seen = moved;
                let page = self
                    .service
                    .torrent_files(self.job_id, self.offset, self.limit)
                    .await?;
                if self.last.as_ref() != Some(&page) {
                    self.last = Some(page.clone());
                    self.next_at = Some(tokio::time::Instant::now() + FILE_UPDATES_SPACING);
                    return Some(page);
                }
            }
            if self.revision.changed().await.is_err() {
                std::future::pending::<()>().await;
            }
        }
    }
}

// The push from the prints `before` to `now` of `views`: all of them when
// there was none before, else the ones whose prints moved or are new and
// the ids of those gone.
fn told<V>(
    before: Option<HashMap<JobId, u64>>,
    now: &HashMap<JobId, u64>,
    views: impl IntoIterator<Item = (V, bool)>,
    owned: impl Fn(V) -> TorrentView,
) -> Option<TorrentChanges>
where
    V: std::borrow::Borrow<TorrentView>,
{
    let Some(before) = before else {
        let (torrents, one_file) = views
            .into_iter()
            .map(|(view, one_file)| (owned(view), one_file))
            .unzip();
        return Some(TorrentChanges {
            full_update: true,
            torrents,
            one_file,
            removed: Vec::new(),
        });
    };
    let mut removed: Vec<JobId> = before
        .keys()
        .filter(|job_id| !now.contains_key(job_id))
        .copied()
        .collect();
    removed.sort_unstable_by_key(|job_id| job_id.0);
    let (torrents, one_file): (Vec<TorrentView>, Vec<bool>) = views
        .into_iter()
        .filter(|(view, _)| {
            let job_id = view.borrow().job_id;
            before.get(&job_id) != now.get(&job_id)
        })
        .map(|(view, one_file)| (owned(view), one_file))
        .unzip();
    (!torrents.is_empty() || !removed.is_empty()).then_some(TorrentChanges {
        full_update: false,
        torrents,
        one_file,
        removed,
    })
}
