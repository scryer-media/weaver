// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The torrents' views, followed as they move. The queue says when the
//! service publishes; announcing to a tracker moves a view with nothing in
//! the queue to say so, so a follower also waits on the announce table of
//! each torrent's current run. Rates move all the time, so the views are
//! read again on a beat as well.
//!
//! The first push is every followed torrent; each later one only the
//! torrents that changed or came, and the ids of those gone.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::task::Poll;
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::{Interval, MissedTickBehavior};
use weaver_torrent::TrackerAnnounce;

use super::{TorrentService, TorrentView};
use crate::jobs::JobId;

/// The torrents' views as they move: one torrent's, or every torrent's.
pub struct TorrentUpdates {
    service: TorrentService,
    job_id: Option<JobId>,
    beat: Interval,
    queue: Option<watch::Receiver<u64>>,
    last: Option<HashMap<JobId, TorrentView>>,
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

impl TorrentService {
    /// Follow the torrents' views: `job_id`'s alone when one is given, every
    /// torrent's otherwise. Between pushes the views are read again every
    /// `beat`.
    pub fn updates(&self, job_id: Option<JobId>, beat: Duration) -> TorrentUpdates {
        let mut beat = tokio::time::interval_at(tokio::time::Instant::now() + beat, beat);
        beat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        TorrentUpdates {
            service: self.clone(),
            job_id,
            beat,
            queue: self
                .queue()
                .map(|queue| queue.state().subscribe_job_changes()),
            last: None,
        }
    }

    // The announce table of each followed torrent's current run, seen as it
    // is now. A run that starts later brings its own table, taken the next
    // time the views are read.
    async fn announce_tables(
        &self,
        job_id: Option<JobId>,
    ) -> Vec<watch::Receiver<Vec<TrackerAnnounce>>> {
        let live = self.inner.live.lock().await;
        live.iter()
            .filter(|(id, _)| job_id.is_none_or(|job_id| **id == job_id))
            .filter_map(|(_, torrent)| match &torrent.attached {
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
        loop {
            // What is waited on is taken before the views are read, so a
            // move after the read wakes the wait.
            let mut tables = self.service.announce_tables(self.job_id).await;
            if let Some(queue) = &mut self.queue {
                queue.borrow_and_update();
            }
            let views = self.read().await;
            if let Some(changes) = changes(&mut self.last, views) {
                return changes;
            }
            tokio::select! {
                () = moved(&mut tables) => {}
                () = published(&mut self.queue) => {}
                _ = self.beat.tick() => {}
            }
        }
    }

    async fn read(&self) -> Vec<TorrentView> {
        match self.job_id {
            Some(job_id) => self.service.torrent(job_id).await.into_iter().collect(),
            None => self.service.torrents(None).await,
        }
    }
}

// What `views` moved from `last`, kept as the new `last`; `None` when
// nothing did.
pub(super) fn changes(
    last: &mut Option<HashMap<JobId, TorrentView>>,
    views: Vec<TorrentView>,
) -> Option<TorrentChanges> {
    let Some(before) = last.as_ref() else {
        *last = Some(
            views
                .iter()
                .map(|view| (view.job_id, view.clone()))
                .collect(),
        );
        return Some(TorrentChanges {
            full_update: true,
            torrents: views,
            removed: Vec::new(),
        });
    };
    let torrents: Vec<TorrentView> = views
        .iter()
        .filter(|view| before.get(&view.job_id) != Some(*view))
        .cloned()
        .collect();
    let now: HashMap<JobId, TorrentView> =
        views.into_iter().map(|view| (view.job_id, view)).collect();
    let mut removed: Vec<JobId> = before
        .keys()
        .filter(|job_id| !now.contains_key(job_id))
        .copied()
        .collect();
    removed.sort_unstable_by_key(|job_id| job_id.0);
    *last = Some(now);
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

// Once the service publishes its torrents to the queue. Never, once the
// queue is gone or when there is none.
async fn published(queue: &mut Option<watch::Receiver<u64>>) {
    let gone = match queue {
        Some(changes) => changes.changed().await.is_err(),
        None => std::future::pending().await,
    };
    if gone {
        *queue = None;
    }
}
