// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::disk_room::DiskHeld;
use super::intake::Resolving;
use super::intake::{AddError, AddRequest};
use super::live::Live;
use super::scripts::StoppedSeeding;
use super::{ActionError, Inner, TorrentService, state_error};
use crate::SchedulerError;
use crate::jobs::JobId;
use crate::jobs::class::{
    AddRefusal, ClassIntake, ClassRate, ClassRates, JobClass, RefusalReason, Submission,
};
use crate::jobs::handle::{CancellationOrigin, QueueAction, QueueMoveTarget, SchedulerHandle};
use crate::jobs::model::Protocol;
use crate::jobs::model::{FieldUpdate, JobUpdate, PRIORITY_ATTRIBUTE_KEY};
use crate::torrents::admission::Refusal;
use crate::torrents::metrics::FetchOutcome;
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
    ///
    /// It also seats the torrents as the queue's torrent class, so tests and
    /// callers that start the client themselves get the whole wiring; the
    /// server wires first and seats only once [`Self::start_seated`] has
    /// started the client.
    pub fn publish_to(&self, queue: SchedulerHandle) {
        self.wire_to(queue.clone());
        queue.state().classes().seat(self.class());
    }

    /// Start the client, and only once it runs seat it as the torrent class
    /// of the queue it was wired to: a client that failed to start is never
    /// the class the job layer hands torrents to.
    pub(super) async fn start_seated(&self) -> Result<(), String> {
        self.start().await?;
        if let Some(queue) = self.queue() {
            queue.state().classes().seat(self.class());
        }
        Ok(())
    }

    // The queue wiring without the seat: holds, starts and the queue the
    // torrents are published to.
    pub(super) fn wire_to(&self, queue: SchedulerHandle) {
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

    // The torrents as a job class: the job layer's way to them, which does
    // not keep the service alive.
    pub(crate) fn class(&self) -> Arc<dyn JobClass> {
        Arc::new(TorrentClass {
            inner: Arc::downgrade(&self.inner),
        })
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
            .with_torrent(job_id, |torrent| {
                Ok(self.seeding_stopped(torrent, StoppedSeeding::Paused))
            })
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
}

// The torrents as a class of job in the queue: the actions on them, their
// side of the global limits and their intake.
struct TorrentClass {
    inner: Weak<Inner>,
}

impl TorrentClass {
    fn service(&self) -> Option<TorrentService> {
        self.inner.upgrade().map(|inner| TorrentService { inner })
    }
}

impl ClassRate for TorrentClass {
    fn sample(&self) -> u64 {
        self.inner
            .upgrade()
            .map_or(0, |inner| inner.sessions.rates().sample())
    }

    fn set_rates(&self, rates: ClassRates) {
        if let Some(inner) = self.inner.upgrade() {
            inner.sessions.rates().set(rates);
        }
    }
}

#[async_trait::async_trait]
impl JobClass for TorrentClass {
    fn protocol(&self) -> Protocol {
        Protocol::Torrent
    }

    // The actions of one request as one unit (`act_many`).
    async fn act(
        &self,
        actions: Vec<(JobId, QueueAction)>,
    ) -> BTreeMap<JobId, Result<(), SchedulerError>> {
        let Some(service) = self.service() else {
            return actions
                .into_iter()
                .map(|(job_id, _)| (job_id, Err(SchedulerError::JobNotFound(job_id))))
                .collect();
        };
        service
            .act_many(actions)
            .await
            .into_iter()
            .map(|(job_id, result)| (job_id, result.map_err(scheduler_error)))
            .collect()
    }

    fn rate(&self) -> Option<&dyn ClassRate> {
        Some(self)
    }

    fn intake(&self) -> Option<&dyn ClassIntake> {
        Some(self)
    }

    // Close enough TCP peers on the tunnel's torrent sessions to cover the
    // bytes, each at a peer socket's size, those the sessions would keep
    // least first: the choice a connection past the cap already makes. A
    // peer closed this way is no failure and stays a connect candidate.
    // Listeners and datagram sockets are never closed for it.
    fn give_back(&self, ask: crate::jobs::TunnelGiveBack) {
        let Some(inner) = self.inner.upgrade() else {
            return;
        };
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let running = inner.sessions.running_on_tunnels().await;
            let dropped = answer_give_back(&running, &ask);
            weaver_tunnel::metrics::record_socket_peers_dropped(dropped as u64);
        });
    }
}

// Read what torrents owe the tunnel now, close that many TCP peers' worth,
// and say how many closed. An answer that owed some and closed none tells
// the tunnel, so it does not wake torrents again until their holdings there
// change.
fn answer_give_back(
    running: &std::collections::BTreeMap<
        crate::torrents::sessions::RouteKey,
        (
            Arc<weaver_torrent::Session>,
            Arc<crate::torrents::net::PeerTunnels>,
        ),
    >,
    ask: &crate::jobs::TunnelGiveBack,
) -> usize {
    let owed = ask.owed.bytes();
    if owed == 0 {
        return 0;
    }
    let peer = weaver_tunnel::pipe::stream_bytes(None);
    let peers = usize::try_from(owed.div_ceil(peer)).unwrap_or(usize::MAX);
    let dropped = give_back_peers(running, ask, peers);
    if dropped == 0 {
        ask.owed.dry();
    }
    dropped
}

// Close up to `peers` TCP peers of the asking tunnel's torrent sessions,
// those each session would keep least first, and say how many. Only a peer
// whose every live connection rides the asking tunnel is closed: one on
// another rung, another tunnel or the kernel's sockets never is.
fn give_back_peers(
    running: &std::collections::BTreeMap<
        crate::torrents::sessions::RouteKey,
        (
            Arc<weaver_torrent::Session>,
            Arc<crate::torrents::net::PeerTunnels>,
        ),
    >,
    ask: &crate::jobs::TunnelGiveBack,
    peers: usize,
) -> usize {
    give_back_through(running, ask, peers, |(session, tunnels), peers| {
        let on = |addr| tunnels.only_on(addr, |egress, proxies| ask.rides(egress, proxies));
        session.give_back_tcp_peers(peers, &on)
    })
}

// Hand up to `peers` to `close` of each running session the ask names, in
// its order, and say how many it closed. Once nothing is owed, before
// torrents act or while they do, nothing more closes: it is read just before
// each session's peers close.
pub(super) fn give_back_through<S>(
    running: &std::collections::BTreeMap<crate::torrents::sessions::RouteKey, S>,
    ask: &crate::jobs::TunnelGiveBack,
    peers: usize,
    mut close: impl FnMut(&S, usize) -> usize,
) -> usize {
    let mut dropped = 0;
    for key in &ask.torrent_routes {
        if dropped == peers || !ask.owed.stands() {
            break;
        }
        let Some(session) = running.get(key) else {
            continue;
        };
        dropped += close(session, peers - dropped);
    }
    dropped
}

#[async_trait::async_trait]
impl ClassIntake for TorrentClass {
    fn takes_new(&self) -> Result<(), AddRefusal> {
        match self.service() {
            Some(service) if service.takes_new() => Ok(()),
            Some(_) => Err(refusal(AddError::Off)),
            None => Err(AddRefusal::not_running(Protocol::Torrent)),
        }
    }

    async fn add(&self, submission: Submission) -> Result<JobId, AddRefusal> {
        let service = self
            .service()
            .ok_or_else(|| AddRefusal::not_running(Protocol::Torrent))?;
        let request = AddRequest::of(submission).map_err(refusal)?;
        service.add(request).await.map_err(refusal)
    }
}

// A refused add, as the intakes tell refusals apart, in the client's words.
impl From<AddError> for AddRefusal {
    fn from(error: AddError) -> Self {
        refusal(error)
    }
}

fn refusal(error: AddError) -> AddRefusal {
    let reason = match &error {
        AddError::Invalid(_) => RefusalReason::Invalid,
        AddError::Refused(Refusal::Removing(_)) => RefusalReason::Removing,
        AddError::Refused(_) => RefusalReason::Refused,
        AddError::Duplicate(job_id) => RefusalReason::Duplicate(*job_id),
        AddError::NoNetwork(_) => RefusalReason::NoNetwork,
        AddError::Off => RefusalReason::Off,
        AddError::State(_) => RefusalReason::State,
    };
    AddRefusal {
        reason,
        text: error.to_string(),
    }
}

pub(super) fn scheduler_error(error: ActionError) -> SchedulerError {
    match error {
        ActionError::NotFound(job_id) => SchedulerError::JobNotFound(job_id),
        ActionError::UnmetGoal(_) => SchedulerError::Conflict(error.to_string()),
        ActionError::Invalid(reason) => SchedulerError::InvalidInput(reason),
        ActionError::State(reason) => SchedulerError::Internal(reason),
    }
}

// The top priority a job can be given.
pub(super) fn top_priority(value: &str) -> bool {
    use crate::post_processing::scan::numeric_priority;
    numeric_priority(value) >= numeric_priority("HIGH")
}
