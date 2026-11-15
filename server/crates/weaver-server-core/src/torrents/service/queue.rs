// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_room::{DiskHeld, StartHold};
use super::intake::Resolving;
use super::live::Live;
use super::{ActionError, Inner, TorrentService, queued, state_error, take_turn};
use crate::SchedulerError;
use crate::jobs::JobId;
use crate::jobs::handle::{
    CancellationOrigin, QueueAction, QueueMoveTarget, QueueOwner, SchedulerHandle, TorrentRate,
};
use crate::jobs::model::{FieldUpdate, JobUpdate, PRIORITY_ATTRIBUTE_KEY};
use crate::torrents::metrics::FetchOutcome;
use crate::torrents::status::Queued;
use crate::torrents::{TorrentIntent, status};
use std::collections::BTreeMap;
use std::sync::{Arc, PoisonError, Weak};

// A snapshot of the download queue's end from the last pass, extended by
// each torrent added since: the slots still free, the ranks given, and the
// place of the last torrent in it. A torrent added after them changes no
// earlier torrent's turn or rank, so it takes its own from here. What moved
// since the pass (a torrent gone or finished, one placed before the end) is
// corrected by the next, which those ask for.
#[derive(Debug, Clone, Copy)]
pub(super) struct QueueEnd {
    pub(super) slots: status::Slots,
    pub(super) last: Option<(i64, JobId)>,
}

impl TorrentService {
    /// Put the torrents in the queue that `queue` publishes, and send its
    /// actions on a torrent here.
    ///
    /// The torrents also take their share of the global download limit from
    /// it, and follow its global pause.
    pub fn publish_to(&self, queue: SchedulerHandle) {
        let seat = Arc::new(Seat(Arc::downgrade(&self.inner)));
        queue.state().seat_owner(seat.clone());
        queue.state().seat_torrent_rate(seat);
        let mut hold = queue.state().subscribe_holds();
        let mut starts = queue.state().subscribe_starts();
        *self
            .inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(queue);
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            while hold.changed().await.is_ok() {
                service.apply_queue().await;
                service.publish().await;
            }
        }));
        // A start of every torrent looks again at those held for their
        // files, as a start of one does. Starts asked for while a look runs
        // are one more look, after it.
        let service = self.clone();
        self.keep(tokio::spawn(async move {
            while starts.changed().await.is_ok() {
                service.look_again_held().await;
            }
        }));
    }

    pub(super) fn queue(&self) -> Option<SchedulerHandle> {
        self.inner
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub async fn pause(&self, job_id: JobId) -> Result<(), ActionError> {
        let stopped = self
            .with_torrent(
                job_id,
                |torrent| Ok(self.seeding_stopped(torrent, "paused")),
            )
            .await?;
        self.set_intent(job_id, TorrentIntent::Paused).await?;
        self.queue_script_event(stopped);
        self.with_torrent(job_id, |torrent| {
            if let Some(Resolving::Fetching(fetch)) = &torrent.resolving {
                fetch.abort();
                self.inner.counters.fetch(FetchOutcome::Cancelled);
                torrent.resolving = Some(Resolving::Paused);
                // A paused magnet shows no fetch's table, as one paused
                // before its fetch began.
                torrent.fetching = None;
            }
            Ok(())
        })
        .await?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// Resume a paused torrent, or one its seeding goal stopped.
    pub async fn resume(&self, job_id: JobId) -> Result<(), ActionError> {
        // A resume is the go-ahead to restore a torrent that crashed the
        // daemon. Its marker goes first, so a panic in the restore leaves a
        // fresh one.
        let mark = self
            .inner
            .crash_marks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take(job_id);
        if let Some(mark) = mark {
            mark.consume();
            self.with_torrent(job_id, |torrent| {
                torrent.restore_held = None;
                Ok(())
            })
            .await?;
        }
        self.set_intent(job_id, TorrentIntent::Resumed).await?;
        self.with_torrent(job_id, |torrent| {
            torrent.copy_error = None;
            // A torrent waiting for room fetches again now, once, whatever
            // the readings say, as a usenet job held for a full disk does
            // when resumed. A write that finds no room again holds it again.
            if torrent.disk_held == Some(DiskHeld::Writing) {
                if let Some(run) = torrent
                    .attached
                    .as_ref()
                    .and_then(|attached| attached.handle.live())
                {
                    run.leave_upload_mode();
                }
                self.end_disk_wait(job_id, Some(torrent));
            }
            Ok(())
        })
        .await?;
        // A fetch the pause ended starts again.
        let refetch = self
            .with_torrent(job_id, |torrent| {
                Ok(torrent
                    .resolving
                    .take_if(|fetch| matches!(fetch, Resolving::Paused))
                    .is_some())
            })
            .await?;
        if refetch {
            self.resolve(job_id).await;
        }
        // A start looks again for files that were missing (qBittorrent
        // `TorrentImpl::start`, torrentimpl.cpp:2048-2053).
        let found = self.look_again(job_id).await;
        self.apply_queue().await;
        // Answered without waiting on its route, as an add is.
        if found {
            self.spawn_attach(job_id).await;
        }
        self.publish().await;
        Ok(())
    }

    pub async fn set_forced(&self, job_id: JobId, forced: bool) -> Result<(), ActionError> {
        self.with_torrent(job_id, |torrent| {
            self.inner
                .db
                .set_torrent_forced(job_id, forced)
                .map_err(|error| ActionError::State(state_error(error)))?;
            torrent.record.forced = forced;
            Ok(())
        })
        .await?;
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    /// A queue action on a torrent. A move changes its place in the
    /// download queue; the top priority forces it past the queue, and any
    /// other leaves it to the queue.
    pub async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), ActionError> {
        match action {
            QueueAction::Pause => self.pause(job_id).await,
            QueueAction::Resume => self.resume(job_id).await,
            QueueAction::Cancel(origin) => {
                let delete_files = origin != CancellationOrigin::UserKeepingFiles;
                self.remove(job_id, delete_files, false).await
            }
            QueueAction::Update(update) => self.update(job_id, update).await,
            QueueAction::Move(target) => self.reorder(&[job_id], target).await,
        }
    }

    /// Moves `job_ids` in the download queue as qBittorrent's queue actions
    /// do (`SessionImpl::increaseTorrentsQueuePos` and the others): of the
    /// torrents named, those still downloading, taken in queue order, each
    /// move one place up for a negative offset, starting with the first, or
    /// one down for a positive one, starting with the last; to the top
    /// starting with the last, so they keep their order there; to the bottom
    /// starting with the first. A seed has no place to move (libtorrent
    /// `torrent::finished` takes it out of the queue). Nothing moves a
    /// torrent past the queue: forcing does that.
    pub async fn reorder(
        &self,
        job_ids: &[JobId],
        target: QueueMoveTarget,
    ) -> Result<(), ActionError> {
        {
            let mut live = self.inner.live.lock().await;
            if let Some(&missing) = job_ids.iter().find(|job_id| !live.contains_key(job_id)) {
                return Err(ActionError::NotFound(missing));
            }
            let mut order: Vec<&Live> =
                live.values().filter(|torrent| !torrent.seeding()).collect();
            order.sort_by_key(|torrent| (torrent.record.queue_position, torrent.record.job_id));
            let mut order: Vec<JobId> = order.iter().map(|torrent| torrent.record.job_id).collect();
            let named: Vec<JobId> = order
                .iter()
                .copied()
                .filter(|job_id| job_ids.contains(job_id))
                .collect();
            let first_first = match target {
                QueueMoveTarget::Top => false,
                QueueMoveTarget::Bottom => true,
                QueueMoveTarget::Offset(offset) => offset < 0,
            };
            let moving: Box<dyn Iterator<Item = &JobId>> = if first_first {
                Box::new(named.iter())
            } else {
                Box::new(named.iter().rev())
            };
            for &job_id in moving {
                // Named and still downloading, so in the order.
                let _ = crate::jobs::handle::splice_job_order(&mut order, job_id, target);
            }
            // The torrents still downloading take the places they held among
            // themselves, in their new order; a seed keeps its own.
            let mut places: Vec<i64> = order
                .iter()
                .map(|job_id| live[job_id].record.queue_position)
                .collect();
            places.sort_unstable();
            let moved: Vec<(JobId, i64)> = order
                .iter()
                .zip(places)
                .filter(|(job_id, place)| live[*job_id].record.queue_position != *place)
                .map(|(job_id, place)| (*job_id, place))
                .collect();
            if moved.is_empty() {
                return Ok(());
            }
            self.inner
                .db
                .set_torrent_queue_positions(&moved)
                .map_err(|error| ActionError::State(state_error(error)))?;
            for (job_id, place) in moved {
                if let Some(torrent) = live.get_mut(&job_id) {
                    torrent.record.queue_position = place;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        Ok(())
    }

    async fn update(&self, job_id: JobId, update: JobUpdate) -> Result<(), ActionError> {
        if update.name.is_some() || !matches!(update.password, FieldUpdate::Unchanged) {
            return Err(ActionError::Invalid(
                "a torrent keeps its name and has no password".into(),
            ));
        }
        let forced = match &update.metadata {
            FieldUpdate::Unchanged => None,
            FieldUpdate::Clear => Some(false),
            FieldUpdate::Set(metadata) => Some(metadata.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY) && top_priority(value)
            })),
        };
        match update.category {
            FieldUpdate::Unchanged => {}
            FieldUpdate::Clear => self.set_category(job_id, None).await?,
            FieldUpdate::Set(name) => self.set_category(job_id, Some(name)).await?,
        }
        match forced {
            Some(forced) => self.set_forced(job_id, forced).await,
            None => Ok(()),
        }
    }

    // Which torrents the queue lets download, applied to their sessions.
    pub(super) async fn apply_queue(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.now_ms();
        let holds = self.holds();
        let mut live = self.inner.live.lock().await;
        let mut order: Vec<&Live> = live.values().collect();
        order.sort_by_key(|torrent| (torrent.record.queue_position, torrent.record.job_id));
        let queue: Vec<Queued> = order
            .iter()
            .map(|torrent| queued(&settings, &rules, torrent, now))
            .collect();
        let last = order
            .last()
            .map(|torrent| (torrent.record.queue_position, torrent.record.job_id));
        let (turns, slots) = status::turns(&queue, settings.torrent.max_active_downloads, &holds);
        let turns: BTreeMap<JobId, (&Queued, status::Turn)> = queue
            .iter()
            .zip(turns)
            .map(|(queued, turn)| (queued.job_id, (queued, turn)))
            .collect();
        *self
            .inner
            .queue_end
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = QueueEnd { slots, last };
        for torrent in live.values_mut() {
            let (queued, turn) = turns[&torrent.record.job_id];
            take_turn(torrent, queued, turn, &holds, now);
            let mut run = turn.active && torrent.resumed();
            // One not running yet whose turn lets it run is kept from
            // starting while the disk is short; one let go starts.
            let starting = torrent
                .attached
                .as_ref()
                .is_none_or(|attached| attached.handle.is_paused());
            if run && (starting || torrent.disk_held == Some(DiskHeld::Starting)) {
                match self.start_hold(torrent) {
                    StartHold::Short => {
                        torrent.disk_held = Some(DiskHeld::Starting);
                        run = false;
                    }
                    StartHold::Unplaced => run = false,
                    StartHold::Free if torrent.disk_held == Some(DiskHeld::Starting) => {
                        torrent.disk_held = None;
                    }
                    StartHold::Free => {}
                }
            } else if !run && torrent.disk_held == Some(DiskHeld::Starting) {
                torrent.disk_held = None;
            }
            let Some(attached) = &torrent.attached else {
                continue;
            };
            let result = if run && attached.handle.is_paused() {
                attached.session.unpause(&attached.handle).await
            } else if !run && !attached.handle.is_paused() {
                attached.session.pause(&attached.handle).await
            } else {
                continue;
            };
            // A run that ends or begins is not in upload mode: a wait for
            // room on it, and what a wake handed it, end.
            self.end_disk_room(torrent);
            torrent.ran(result);
        }
    }
}

// The torrents' seat in the queue.
struct Seat(Weak<Inner>);

impl TorrentRate for Seat {
    fn sample(&self) -> u64 {
        self.0
            .upgrade()
            .map_or(0, |inner| inner.sessions.rates().sample())
    }

    fn set_rates(&self, rates: crate::bandwidth::TorrentRates) {
        if let Some(inner) = self.0.upgrade() {
            inner.sessions.rates().set(rates);
        }
    }
}

#[async_trait::async_trait]
impl QueueOwner for Seat {
    async fn act(&self, job_id: JobId, action: QueueAction) -> Result<(), SchedulerError> {
        let inner = self
            .0
            .upgrade()
            .ok_or(SchedulerError::JobNotFound(job_id))?;
        TorrentService { inner }
            .act(job_id, action)
            .await
            .map_err(|error| match error {
                ActionError::NotFound(job_id) => SchedulerError::JobNotFound(job_id),
                ActionError::UnmetGoal(_) => SchedulerError::Conflict(error.to_string()),
                ActionError::Invalid(reason) => SchedulerError::InvalidInput(reason),
                ActionError::State(reason) => SchedulerError::Internal(reason),
            })
    }
}

// The top priority a job can be given.
fn top_priority(value: &str) -> bool {
    use crate::post_processing::scan::numeric_priority;
    numeric_priority(value) >= numeric_priority("HIGH")
}
