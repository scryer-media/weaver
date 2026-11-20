// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! Every torrent's row as a list shows it, kept as the service publishes.
//!
//! The service builds every torrent's view when it publishes (each tick, a
//! tracker's answer, an action over many torrents) and one torrent's when
//! that torrent alone changed; the tags change the rows on their own. Each
//! of those hands its views here, and a row whose view moved takes the next
//! revision. A reader keeps the revision it last read and asks for the rows
//! moved past it: nothing is built for it, and a revision that did not move
//! costs it nothing (qBittorrent's `SyncController` reads the torrents
//! libtorrent's state update names, not every torrent).
//!
//! A row holds the fields a list shows, the totals of its files and what its
//! trackers come to, not its files, the bytes of each file or its trackers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use tokio::sync::watch;

use super::TorrentView;
use super::fingerprint::ViewParts;
use super::listing::FileTotals;
use crate::jobs::JobId;
use crate::torrents::{TorrentTracker, TrackerStatus};

// The removals kept for a reader to learn from by revision. A reader that
// last read before the oldest kept is answered as one that never read.
const REMOVALS_KEPT: usize = 4_096;

// The torrents gone kept against a reading begun before they went. Past it,
// the half gone longest are let go.
const GONE_KEPT: usize = 4_096;

/// One torrent as a list shows it.
#[derive(Debug, Clone, PartialEq)]
pub struct TorrentRow {
    /// Its view, with no files, no bytes of each file and no trackers: those
    /// are read on demand, one torrent at a time.
    pub view: TorrentView,
    /// The totals of its files.
    pub totals: FileTotals,
    /// What its trackers come to.
    pub trackers: TrackerSummary,
    /// The revision at which its fields, the clock's aside, last moved.
    pub listed: u64,
    /// The revision at which any of its fields last moved.
    pub moved: u64,
    /// The revision at which its files last moved where nothing a list
    /// shows did: what one torrent's follower reads beside `moved`.
    pub files_moved: u64,
    // The view's prints, its tags left out, with the totals: so the tags
    // alone can move the row.
    steady: u64,
    whole: u64,
}

// The stamps of the reading a row's view came from, and of the one its tags
// came from: kept beside the rows, so a reading that finds a row unchanged
// records that it read it without building the row again.
#[derive(Debug, Clone, Copy, Default)]
struct Stamps {
    read: u64,
    tagged: u64,
}

/// What a torrent's trackers come to, as a list shows them.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TrackerSummary {
    pub count: usize,
    /// The tracker it goes by (`TorrentView::current_tracker`).
    pub current: Option<TorrentTracker>,
    /// A working tracker answered with a message.
    pub warning: bool,
    /// A tracker answered with an error.
    pub error: bool,
    /// A tracker could not be announced to.
    pub other_error: bool,
}

impl TrackerSummary {
    pub fn of(view: &TorrentView) -> Self {
        let any = |pick: fn(&TorrentTracker) -> bool| view.trackers.iter().any(pick);
        Self {
            count: view.trackers.len(),
            current: view.current_tracker().cloned(),
            warning: any(|tracker| {
                tracker.status == TrackerStatus::Working
                    && tracker
                        .message
                        .as_deref()
                        .is_some_and(|message| !message.is_empty())
            }),
            error: any(|tracker| tracker.status == TrackerStatus::TrackerError),
            other_error: any(|tracker| {
                matches!(
                    tracker.status,
                    TrackerStatus::NotWorking | TrackerStatus::Unreachable
                )
            }),
        }
    }
}

impl TorrentRow {
    // The row of `view`, read with its trackers, its tags and the bytes of
    // each file, with the totals of its files and its prints.
    fn of(view: &TorrentView, totals: FileTotals, (steady, whole): (u64, u64)) -> Self {
        Self {
            view: TorrentView {
                files: Vec::new(),
                file_have: Vec::new(),
                trackers: Vec::new(),
                ..view.clone()
            },
            totals,
            trackers: TrackerSummary::of(view),
            listed: 0,
            moved: 0,
            files_moved: 0,
            steady,
            whole,
        }
    }

    /// A print of the fields a list shows: one that moves when one of them
    /// does.
    pub fn print(&self) -> u64 {
        mix(self.steady, &self.view.tags)
    }

    /// A print of every field.
    pub fn whole_print(&self) -> u64 {
        mix(self.whole, &self.view.tags)
    }

    // Where it sits in the order the torrents were added.
    fn place(&self) -> (i64, JobId) {
        (self.view.added_at_ms, self.view.job_id)
    }
}

// The view's prints, its tags and files left out, each with its files'
// totals: the same for a view read with its files and one read without.
fn prints(view: &TorrentView, totals: FileTotals) -> (u64, u64) {
    let print = |parts| {
        let mut hasher = DefaultHasher::new();
        (view.row_fingerprint(parts), totals).hash(&mut hasher);
        hasher.finish()
    };
    (print(ViewParts::Steady), print(ViewParts::Whole))
}

fn mix(print: u64, tags: &[String]) -> u64 {
    let mut hasher = DefaultHasher::new();
    (print, tags).hash(&mut hasher);
    hasher.finish()
}

/// The rows as they stand, shared by every reader.
#[derive(Default)]
pub struct Snapshot {
    rows: RwLock<Rows>,
    // The revision, rung each time it moves and only then.
    revision: watch::Sender<u64>,
    // Whether the rows are every torrent's: false from the start of a load
    // of the stored torrents until a publish after it ends.
    unready: AtomicBool,
    // The stamp of the last reading begun: a reading takes one before it
    // reads a torrent, and a row is never set from a reading older than the
    // one it holds, so a publish that read every torrent and lands after a
    // newer reading of one does not take that torrent back.
    stamps: AtomicU64,
}

#[derive(Default)]
struct Rows {
    revision: u64,
    rows: HashMap<JobId, Arc<TorrentRow>>,
    // The torrents in the order they were added.
    order: BTreeSet<(i64, JobId)>,
    // Each row by the revision it last moved at.
    moved: BTreeSet<(u64, JobId)>,
    // The rows taken out, by the revision they went at, with their hashes.
    removed: BTreeMap<(u64, JobId), String>,
    // The revision up to which removals were let go.
    forgotten: u64,
    // The torrents gone, each with the stamp of the reading that found it
    // gone: a reading older than that does not bring it back.
    gone: HashMap<JobId, u64>,
    // Each row's stamps.
    stamps: HashMap<JobId, Stamps>,
    sums: RowSums,
}

/// What every row adds up to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RowSums {
    pub download_bps: u64,
    pub upload_bps: u64,
    pub downloaded_bytes: u64,
    pub uploaded_bytes: u64,
    pub wasted_bytes: u64,
    pub peers: u64,
}

impl RowSums {
    fn add(&mut self, view: &TorrentView) {
        self.download_bps = self.download_bps.wrapping_add(view.download_bps);
        self.upload_bps = self.upload_bps.wrapping_add(view.upload_bps);
        self.downloaded_bytes = self.downloaded_bytes.wrapping_add(view.downloaded_bytes);
        self.uploaded_bytes = self.uploaded_bytes.wrapping_add(view.uploaded_bytes);
        self.wasted_bytes = self.wasted_bytes.wrapping_add(view.wasted_bytes);
        self.peers = self.peers.wrapping_add(u64::from(view.peers));
    }

    fn take(&mut self, view: &TorrentView) {
        self.download_bps = self.download_bps.wrapping_sub(view.download_bps);
        self.upload_bps = self.upload_bps.wrapping_sub(view.upload_bps);
        self.downloaded_bytes = self.downloaded_bytes.wrapping_sub(view.downloaded_bytes);
        self.uploaded_bytes = self.uploaded_bytes.wrapping_sub(view.uploaded_bytes);
        self.wasted_bytes = self.wasted_bytes.wrapping_sub(view.wasted_bytes);
        self.peers = self.peers.wrapping_sub(u64::from(view.peers));
    }
}

/// The rows moved past a revision.
#[derive(Debug, Clone, Default)]
pub struct RowsSince {
    /// The revision they bring the reader to.
    pub revision: u64,
    /// The rows that moved or came, in no set order.
    pub moved: Vec<Arc<TorrentRow>>,
    /// The torrents gone, each with its hash; `None` when the reader read
    /// before the removals still kept, and must look for itself.
    pub removed: Option<Vec<(JobId, String)>>,
}

impl Snapshot {
    /// The revision the rows stand at, rung each time it moves.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    /// The revision the rows stand at.
    pub fn revision(&self) -> u64 {
        self.read().revision
    }

    /// Whether the rows are every torrent's.
    pub fn ready(&self) -> bool {
        !self.unready.load(Ordering::Acquire)
    }

    // A load of the stored torrents begins: the rows are not every
    // torrent's until a publish after it ends.
    pub fn unready(&self) {
        self.unready.store(true, Ordering::Release);
    }

    // A stamp for a reading about to begin, newer than every one before.
    pub fn stamp(&self) -> u64 {
        self.stamps.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// One torrent's row.
    pub fn row(&self, job_id: JobId) -> Option<Arc<TorrentRow>> {
        self.read().rows.get(&job_id).cloned()
    }

    /// The rows of `ids` there are, in the order the torrents were added.
    pub fn rows_of<'a>(&self, ids: impl IntoIterator<Item = &'a JobId>) -> Vec<Arc<TorrentRow>> {
        let mut rows: Vec<Arc<TorrentRow>> = {
            let rows = self.read();
            ids.into_iter()
                .filter_map(|job_id| rows.rows.get(job_id).cloned())
                .collect()
        };
        rows.sort_unstable_by_key(|row| row.place());
        rows
    }

    /// Every row, in the order the torrents were added, at the revision
    /// they stand at.
    pub fn every(&self) -> (u64, Vec<Arc<TorrentRow>>) {
        let rows = self.read();
        let every = rows
            .order
            .iter()
            .filter_map(|(_, job_id)| rows.rows.get(job_id).cloned())
            .collect();
        (rows.revision, every)
    }

    /// The rows moved past `revision`, and the torrents gone since.
    pub fn since(&self, revision: u64) -> RowsSince {
        let rows = self.read();
        let moved = rows
            .moved
            .range((revision + 1, JobId(0))..)
            .filter_map(|(_, job_id)| rows.rows.get(job_id).cloned())
            .collect();
        let removed = (revision >= rows.forgotten).then(|| {
            rows.removed
                .range((revision + 1, JobId(0))..)
                .map(|((_, job_id), hash)| (*job_id, hash.clone()))
                .collect()
        });
        RowsSince {
            revision: rows.revision,
            moved,
            removed,
        }
    }

    /// What every row adds up to.
    pub fn sums(&self) -> RowSums {
        self.read().sums
    }

    /// How many rows there are.
    pub fn len(&self) -> usize {
        self.read().rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Every torrent's view, as a publish read them with their trackers,
    // their tags and the bytes of each file, with their files' totals: the
    // rows of those gone are taken out. `every` says whether these are every
    // stored torrent.
    /// Every row set to `views`, as a publish of every torrent sets them,
    /// for tests outside the service.
    #[cfg(any(test, feature = "test-support"))]
    pub fn replace_every(&self, views: &[(TorrentView, FileTotals)]) {
        self.replace(views, true, self.stamp());
    }

    // `stamp` is the one taken before the views were read.
    pub fn replace(&self, views: &[(TorrentView, FileTotals)], every: bool, stamp: u64) {
        let mut rows = self.write();
        let next = rows.revision + 1;
        let mut moved = false;
        let mut seen = HashMap::with_capacity(views.len());
        for (view, totals) in views {
            seen.insert(view.job_id, ());
            moved |= rows.put(view, *totals, next, stamp);
        }
        // A row a newer reading put is not one this reading missed.
        let gone: Vec<JobId> = rows
            .rows
            .keys()
            .filter(|job_id| !seen.contains_key(job_id) && rows.stamps_of(**job_id).read < stamp)
            .copied()
            .collect();
        for job_id in gone {
            moved |= rows.take(job_id, next, stamp);
        }
        if every {
            self.unready.store(false, Ordering::Release);
        }
        self.settle(rows, moved);
    }

    // The rows of some torrents, read as a publish of every torrent reads
    // them; the others stand. `stamp` is the one taken before they were read.
    pub fn put_some(&self, views: &[(TorrentView, FileTotals)], stamp: u64) {
        let mut rows = self.write();
        let next = rows.revision + 1;
        let mut moved = false;
        for (view, totals) in views {
            moved |= rows.put(view, *totals, next, stamp);
        }
        self.settle(rows, moved);
    }

    // One torrent's view, read whole, or none when it is gone; `stamp` is
    // the one taken before it was read.
    pub fn put_one(&self, job_id: JobId, view: Option<&TorrentView>, stamp: u64) {
        let mut rows = self.write();
        let next = rows.revision + 1;
        let moved = match view {
            Some(view) => rows.put(view, FileTotals::of(view), next, stamp),
            None => rows.take(job_id, next, stamp),
        };
        self.settle(rows, moved);
    }

    /// `job_id`'s files moved where nothing its row shows did (a file's
    /// level): its row is marked for one torrent's followers alone, and no
    /// list reads it as moved.
    pub fn files_moved(&self, job_id: JobId) {
        let mut rows = self.write();
        let next = rows.revision + 1;
        let Some(row) = rows.rows.get(&job_id) else {
            return;
        };
        let mut row = TorrentRow::clone(row);
        row.files_moved = next;
        rows.rows.insert(job_id, Arc::new(row));
        self.settle(rows, true);
    }

    // Each row's tags as `tags_of` gives them; `stamp` is the one taken
    // before the tags were read.
    pub fn retag(&self, tags_of: impl Fn(JobId) -> Vec<String>, stamp: u64) {
        let mut rows = self.write();
        let next = rows.revision + 1;
        // Each row's tags as read, unless a newer reading set them; the
        // stamp of this one is kept for every row it read, changed or not.
        let mut changed: Vec<(JobId, Vec<String>)> = Vec::new();
        {
            let rows = &mut *rows;
            for (job_id, row) in &rows.rows {
                let stamps = rows.stamps.entry(*job_id).or_default();
                if stamps.tagged > stamp {
                    continue;
                }
                stamps.tagged = stamp;
                let tags = tags_of(*job_id);
                if tags != row.view.tags {
                    changed.push((*job_id, tags));
                }
            }
        }
        let moved = !changed.is_empty();
        for (job_id, tags) in changed {
            let Some(row) = rows.rows.get(&job_id) else {
                continue;
            };
            let mut row = TorrentRow::clone(row);
            row.view.tags = tags;
            rows.moved.remove(&(row.moved, job_id));
            row.listed = next;
            row.moved = next;
            rows.moved.insert((next, job_id));
            rows.rows.insert(job_id, Arc::new(row));
        }
        self.settle(rows, moved);
    }

    // The revision moved on when a row did, and rung.
    fn settle(&self, mut rows: std::sync::RwLockWriteGuard<'_, Rows>, moved: bool) {
        if !moved {
            return;
        }
        rows.revision += 1;
        let revision = rows.revision;
        rows.forget_removals();
        drop(rows);
        self.revision.send_replace(revision);
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Rows> {
        self.rows.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Rows> {
        self.rows.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Rows {
    // `view`'s row put in at revision `next` when it moved or is new, unless
    // a reading newer than `stamp` set the row or found the torrent gone.
    // Tags a newer reading set stay. Whether it was put.
    fn put(&mut self, view: &TorrentView, totals: FileTotals, next: u64, stamp: u64) -> bool {
        let job_id = view.job_id;
        let stamps = self.stamps_of(job_id);
        if stamps.read > stamp || self.gone.get(&job_id).is_some_and(|gone| *gone > stamp) {
            return false;
        }
        let before = self.rows.get(&job_id).cloned();
        let tagged = before.as_ref().filter(|_| stamps.tagged > stamp);
        let retagged;
        let view = match tagged {
            Some(row) if row.view.tags != view.tags => {
                retagged = TorrentView {
                    tags: row.view.tags.clone(),
                    ..view.clone()
                };
                &retagged
            }
            _ => view,
        };
        // The reading is newer than any the row came from, whether or not
        // the row moved: an older reading that lands later does not set it.
        let kept = Stamps {
            read: stamp,
            tagged: if tagged.is_some() {
                stamps.tagged
            } else {
                stamp
            },
        };
        self.stamps.insert(job_id, kept);
        let prints = prints(view, totals);
        let listed = match &before {
            Some(row) if row.view.tags != view.tags => next,
            Some(row) if row.whole == prints.1 => return false,
            Some(row) if row.steady == prints.0 => row.listed,
            _ => next,
        };
        let mut row = TorrentRow::of(view, totals, prints);
        row.listed = listed;
        row.moved = next;
        row.files_moved = before.as_ref().map_or(0, |row| row.files_moved);
        if let Some(before) = before {
            self.moved.remove(&(before.moved, job_id));
            self.order.remove(&before.place());
            self.sums.take(&before.view);
        }
        self.moved.insert((next, job_id));
        self.order.insert(row.place());
        self.sums.add(&row.view);
        self.rows.insert(job_id, Arc::new(row));
        true
    }

    // The stamps `job_id`'s row was read at; none for a torrent with no row.
    fn stamps_of(&self, job_id: JobId) -> Stamps {
        self.stamps.get(&job_id).copied().unwrap_or_default()
    }

    // The row of `job_id` taken out at revision `next`, unless a reading
    // newer than `stamp` set it. Whether there was one.
    fn take(&mut self, job_id: JobId, next: u64, stamp: u64) -> bool {
        if self.stamps_of(job_id).read > stamp {
            return false;
        }
        self.went(job_id, stamp);
        self.stamps.remove(&job_id);
        let Some(row) = self.rows.remove(&job_id) else {
            return false;
        };
        self.moved.remove(&(row.moved, job_id));
        self.order.remove(&row.place());
        self.sums.take(&row.view);
        self.removed
            .insert((next, job_id), row.view.infohash.to_ascii_lowercase());
        true
    }

    // `job_id` found gone by the reading of `stamp`.
    fn went(&mut self, job_id: JobId, stamp: u64) {
        let gone = self.gone.entry(job_id).or_default();
        *gone = (*gone).max(stamp);
        // Past the number kept, the older half goes, ordered by stamp and
        // then by id: torrents found gone by one reading share a stamp, and
        // the id parts them, so each pruning halves the map.
        if self.gone.len() > GONE_KEPT {
            let mut stamps: Vec<(u64, JobId)> = self
                .gone
                .iter()
                .map(|(job_id, gone)| (*gone, *job_id))
                .collect();
            let (_, half, _) = stamps.select_nth_unstable(GONE_KEPT / 2);
            let half = *half;
            self.gone.retain(|job_id, gone| (*gone, *job_id) >= half);
        }
    }

    // The oldest removals let go past the number kept.
    fn forget_removals(&mut self) {
        while self.removed.len() > REMOVALS_KEPT {
            if let Some(((revision, _), _)) = self.removed.pop_first() {
                self.forgotten = self.forgotten.max(revision);
            }
        }
    }
}
