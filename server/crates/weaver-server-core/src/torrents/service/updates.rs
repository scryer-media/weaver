// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' views, followed as they move. The queue says when the
//! service publishes; announcing to a tracker moves a view with nothing in
//! the queue to say so, so a follower also waits on the announce table of
//! each torrent's current run. Rates move all the time, so the views are
//! read again on a beat as well.
//!
//! The first push is every followed torrent; each later one only the
//! torrents that changed or came, and the ids of those gone. Every torrent's
//! follower leaves out the fields that move with the clock alone when it
//! tells a change, or each beat would carry every running torrent; one
//! torrent's follower, which shows those seconds, counts them.
//!
//! Every torrent's followers share their reads: one reading of the views,
//! with their fingerprints and what moved since the reading before, serves
//! each of them that it is new enough for. A follower woken by the queue or
//! the tags takes a reading made after the revision it was woken by; one
//! woken by a tracker's answer, or following for the first time, a reading
//! begun after it woke; one woken by its beat, a reading younger than its
//! beat. Nothing is read while no follower asks, and a reading is let go
//! once no follower holds it.

use std::collections::{BTreeSet, HashMap};
use std::future::{Future, poll_fn};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Instant, Interval, MissedTickBehavior};
use weaver_torrent::TrackerAnnounce;

use super::fingerprint::ViewParts;
use super::{Live, TorrentService, TorrentView};
use crate::jobs::JobId;

/// The torrents' views as they move: one torrent's, a few torrents', or
/// every torrent's.
pub struct TorrentUpdates {
    service: TorrentService,
    follow: Follow,
    beat: Interval,
    queue: Option<watch::Receiver<u64>>,
    // The tags' revision: a view shows its job's tags.
    tags: Option<watch::Receiver<u64>>,
    // Each torrent last given, by its fingerprint.
    last: Option<HashMap<JobId, u64>>,
    // Every torrent's follower: the reading last given, and the announce
    // tables as that reading took them.
    reading: Option<Arc<Reading>>,
    tables: Vec<watch::Receiver<Vec<TrackerAnnounce>>>,
}

/// The readings every torrent's followers share: the last one made, kept
/// while a follower holds it, and how many have begun.
#[derive(Default)]
pub(super) struct Readings {
    last: tokio::sync::Mutex<Weak<Reading>>,
    begun: AtomicU64,
}

// One reading of every torrent's views, as their followers read them.
pub(super) struct Reading {
    // Counted from one, in the order the readings began.
    number: u64,
    // When it began.
    at: Instant,
    // The queue's and the tags' revisions, taken before the views.
    queue: Option<u64>,
    tags: Option<u64>,
    // The announce tables, taken before the views.
    tables: Vec<watch::Receiver<Vec<TrackerAnnounce>>>,
    views: Vec<TorrentView>,
    prints: HashMap<JobId, u64>,
    // What moved since the reading before it, when one was still held.
    since: Option<Moved>,
}

// What moved from the reading numbered `from`: the places in `views` of
// the torrents that changed or came, and the ids of those gone.
struct Moved {
    from: u64,
    torrents: Vec<usize>,
    removed: Vec<JobId>,
}

// What a follower takes a reading for, as it wakes.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Need {
    // The revisions it has seen: a reading taken at them or after.
    pub(super) queue: Option<u64>,
    pub(super) tags: Option<u64>,
    // A reading begun after this many had begun.
    pub(super) begun_after: Option<u64>,
    // A reading begun less than this long ago.
    pub(super) younger_than: Option<Duration>,
}

// Why a follower of every torrent reads again.
#[derive(Debug, Clone, Copy)]
enum Woke {
    // Its first call, or a tracker's answer.
    Asked,
    // The queue or the tags.
    Revision,
    // Its beat, or a call after a push.
    Beat,
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
    /// One torrent, with the fields that move with the clock alone.
    One(JobId),
    /// The torrents of a set, as every torrent's follower sees them: a list
    /// shows a page of the torrents, and follows those alone.
    Some(BTreeSet<JobId>),
    Every,
}

impl TorrentService {
    /// Follow the torrents' views: `job_id`'s alone when one is given, every
    /// torrent's otherwise. Between pushes the views are read again every
    /// `beat`.
    pub fn updates(&self, job_id: Option<JobId>, beat: Duration) -> TorrentUpdates {
        self.follow(job_id.map_or(Follow::Every, Follow::One), beat)
    }

    /// Follow the views of the torrents `follow` names. Between pushes the
    /// views are read again every `beat`.
    pub fn follow(&self, follow: Follow, beat: Duration) -> TorrentUpdates {
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + beat, beat);
        beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        TorrentUpdates {
            service: self.clone(),
            follow,
            beat,
            queue: self
                .queue()
                .map(|queue| queue.state().subscribe_job_changes()),
            tags: Some(self.inner.db.subscribe_tag_changes()),
            last: None,
            reading: None,
            tables: Vec::new(),
        }
    }

    /// How many readings of every torrent's views have begun.
    #[cfg(test)]
    pub(crate) fn readings_begun(&self) -> u64 {
        self.inner.readings.begun.load(Ordering::Acquire)
    }

    /// Whether a reading of every torrent's views is still kept.
    #[cfg(test)]
    pub(crate) async fn holds_reading(&self) -> bool {
        self.inner.readings.last.lock().await.strong_count() > 0
    }

    // The announce table of each followed torrent's current run, seen as it
    // is now. A run that starts later brings its own table, taken the next
    // time the views are read.
    async fn announce_tables(&self, follow: &Follow) -> Vec<watch::Receiver<Vec<TrackerAnnounce>>> {
        let live = self.inner.live.lock().await;
        let followed: Box<dyn Iterator<Item = &Live>> = match follow {
            Follow::One(job_id) => Box::new(live.get(job_id).into_iter()),
            Follow::Some(ids) => Box::new(ids.iter().filter_map(|job_id| live.get(job_id))),
            Follow::Every => Box::new(live.values()),
        };
        followed
            .filter_map(|torrent| match &torrent.attached {
                Some(attached) => attached.handle.tracker_status(),
                None => torrent
                    .fetching
                    .as_ref()
                    .and_then(|fetch| fetch.trackers.clone()),
            })
            .map(|table| table.subscribe())
            .collect()
    }
}

impl TorrentUpdates {
    /// The views the next time they differ from the ones last given: every
    /// view on the first call, the changed, new and gone ones after.
    pub async fn next(&mut self) -> TorrentChanges {
        if self.follow == Follow::Every {
            return self.next_of_every().await;
        }
        loop {
            // What is waited on is taken before the views are read, so a
            // move after the read wakes the wait.
            let mut tables = self.service.announce_tables(&self.follow).await;
            for changes in [&mut self.queue, &mut self.tags].into_iter().flatten() {
                changes.borrow_and_update();
            }
            let views = self.read().await;
            let parts = self.parts();
            if let Some(changes) = changes(&mut self.last, views, parts) {
                return changes;
            }
            tokio::select! {
                () = moved(&mut tables) => {}
                () = published(&mut self.queue) => {}
                () = published(&mut self.tags) => {}
                _ = self.beat.tick() => {}
            }
        }
    }

    // `next` for every torrent's follower, from the readings it shares.
    async fn next_of_every(&mut self) -> TorrentChanges {
        let service = self.service.clone();
        let readings = &service.inner.readings;
        let mut woke = if self.reading.is_some() {
            Woke::Beat
        } else {
            Woke::Asked
        };
        loop {
            // Counted as it wakes: a reading counted past it is begun after.
            let begun = readings.begun.fetch_add(0, Ordering::AcqRel);
            for changes in [&mut self.queue, &mut self.tags].into_iter().flatten() {
                changes.borrow_and_update();
            }
            let need = Need {
                queue: self.queue.as_ref().map(|changes| *changes.borrow()),
                tags: self.tags.as_ref().map(|changes| *changes.borrow()),
                begun_after: matches!(woke, Woke::Asked).then_some(begun),
                younger_than: matches!(woke, Woke::Beat).then(|| self.beat.period()),
            };
            let reading = readings
                .reading(&service, need, &self.queue, &self.tags)
                .await;
            if !self
                .reading
                .as_ref()
                .is_some_and(|held| Arc::ptr_eq(held, &reading))
            {
                self.tables = reading.tables.clone();
            }
            let changes = reading.changes_since(self.reading.as_deref());
            self.reading = Some(reading);
            if let Some(changes) = changes {
                return changes;
            }
            woke = tokio::select! {
                () = moved(&mut self.tables) => Woke::Asked,
                () = published(&mut self.queue) => Woke::Revision,
                () = published(&mut self.tags) => Woke::Revision,
                _ = self.beat.tick() => Woke::Beat,
            };
        }
    }

    // What a change is told by: one torrent's follower shows its seconds,
    // every torrent's follower reads the list's fields.
    fn parts(&self) -> ViewParts {
        if matches!(self.follow, Follow::One(_)) {
            ViewParts::Whole
        } else {
            ViewParts::Steady
        }
    }

    async fn read(&self) -> Vec<TorrentView> {
        match &self.follow {
            Follow::One(job_id) => self.service.torrent(*job_id).await.into_iter().collect(),
            Follow::Some(ids) => self.service.torrents_of(ids).await,
            Follow::Every => self.service.torrents(None).await,
        }
    }
}

impl Readings {
    // The last reading when it meets `need`, or a new one, read by the
    // follower whose revisions are `queue` and `tags`. One follower reads
    // at a time; those waiting take what it read when it meets their need.
    async fn reading(
        &self,
        service: &TorrentService,
        need: Need,
        queue: &Option<watch::Receiver<u64>>,
        tags: &Option<watch::Receiver<u64>>,
    ) -> Arc<Reading> {
        let mut last = self.last.lock().await;
        let held = last.upgrade();
        if let Some(reading) = held
            .as_ref()
            .filter(|reading| reading.serves(need, Instant::now()))
        {
            return Arc::clone(reading);
        }
        let number = self.begun.fetch_add(1, Ordering::AcqRel) + 1;
        let at = Instant::now();
        // What is waited on is taken before the views are read, so a move
        // after the read wakes the wait.
        let tables = service.announce_tables(&Follow::Every).await;
        let queue = queue.as_ref().map(|changes| *changes.borrow());
        let tags = tags.as_ref().map(|changes| *changes.borrow());
        let views = service.torrents(None).await;
        let reading = Arc::new(Reading::of(
            number,
            at,
            (queue, tags),
            tables,
            views,
            held.as_deref(),
        ));
        *last = Arc::downgrade(&reading);
        reading
    }
}

impl Reading {
    // The reading numbered `number`, begun `at`, of `views` read at the
    // revisions `(queue, tags)` after `tables` were taken; with what moved
    // since `before` when it is given.
    pub(super) fn of(
        number: u64,
        at: Instant,
        (queue, tags): (Option<u64>, Option<u64>),
        tables: Vec<watch::Receiver<Vec<TrackerAnnounce>>>,
        views: Vec<TorrentView>,
        before: Option<&Reading>,
    ) -> Self {
        let prints: HashMap<JobId, u64> = views
            .iter()
            .map(|view| (view.job_id, view.fingerprint(ViewParts::Steady)))
            .collect();
        let since = before.map(|before| {
            let (torrents, removed) = moved_from(&before.prints, &prints, &views);
            Moved {
                from: before.number,
                torrents,
                removed,
            }
        });
        Self {
            number,
            at,
            queue,
            tags,
            tables,
            views,
            prints,
            since,
        }
    }

    // Whether it meets `need` at `now`.
    pub(super) fn serves(&self, need: Need, now: Instant) -> bool {
        fn at_least(seen: Option<u64>, taken: Option<u64>) -> bool {
            seen.is_none_or(|seen| taken.is_some_and(|taken| taken >= seen))
        }
        at_least(need.queue, self.queue)
            && at_least(need.tags, self.tags)
            && need.begun_after.is_none_or(|begun| self.number > begun)
            && need
                .younger_than
                .is_none_or(|beat| now.saturating_duration_since(self.at) < beat)
    }

    // What it moved from `last`, the reading a follower was last given, as
    // `changes` tells it; every view when there was none, `None` when
    // nothing moved.
    pub(super) fn changes_since(&self, last: Option<&Reading>) -> Option<TorrentChanges> {
        let Some(last) = last else {
            return Some(TorrentChanges {
                full_update: true,
                torrents: self.views.clone(),
                removed: Vec::new(),
            });
        };
        if last.number == self.number {
            return None;
        }
        let (places, removed) = match &self.since {
            Some(since) if since.from == last.number => {
                (since.torrents.clone(), since.removed.clone())
            }
            _ => moved_from(&last.prints, &self.prints, &self.views),
        };
        (!places.is_empty() || !removed.is_empty()).then(|| TorrentChanges {
            full_update: false,
            torrents: places
                .into_iter()
                .map(|place| self.views[place].clone())
                .collect(),
            removed,
        })
    }
}

// The places in `views`, printed as `now`, of the torrents whose prints
// moved from `before` or are new, and the ids, in order, of those gone.
fn moved_from(
    before: &HashMap<JobId, u64>,
    now: &HashMap<JobId, u64>,
    views: &[TorrentView],
) -> (Vec<usize>, Vec<JobId>) {
    let mut removed: Vec<JobId> = before
        .keys()
        .filter(|job_id| !now.contains_key(job_id))
        .copied()
        .collect();
    removed.sort_unstable_by_key(|job_id| job_id.0);
    let torrents = views
        .iter()
        .enumerate()
        .filter(|(_, view)| before.get(&view.job_id) != now.get(&view.job_id))
        .map(|(place, _)| place)
        .collect();
    (torrents, removed)
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
    let Some(before) = last.replace(now) else {
        return Some(TorrentChanges {
            full_update: true,
            torrents: views,
            removed: Vec::new(),
        });
    };
    let now = last.as_ref().expect("just kept");
    let mut removed: Vec<JobId> = before
        .keys()
        .filter(|job_id| !now.contains_key(job_id))
        .copied()
        .collect();
    removed.sort_unstable_by_key(|job_id| job_id.0);
    let torrents: Vec<TorrentView> = views
        .into_iter()
        .filter(|view| before.get(&view.job_id) != now.get(&view.job_id))
        .collect();
    (!torrents.is_empty() || !removed.is_empty()).then_some(TorrentChanges {
        full_update: false,
        torrents,
        removed,
    })
}

// Once any of `tables` moves, or is gone with its run. Never, with none.
async fn moved(tables: &mut [watch::Receiver<Vec<TrackerAnnounce>>]) {
    let mut waits: Vec<_> = tables
        .iter_mut()
        .map(|table| Box::pin(table.changed()))
        .collect();
    poll_fn(|cx| {
        if waits
            .iter_mut()
            .any(|wait| wait.as_mut().poll(cx).is_ready())
        {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await
}

// Once `changes` moves: the service publishing its torrents to the queue, or
// the tags changing. Never, once its sender is gone or when there is none.
async fn published(queue: &mut Option<watch::Receiver<u64>>) {
    let gone = match queue {
        Some(changes) => changes.changed().await.is_err(),
        None => std::future::pending().await,
    };
    if gone {
        *queue = None;
    }
}
