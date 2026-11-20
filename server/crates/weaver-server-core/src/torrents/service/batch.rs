// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A batch of queue actions on torrents as one unit: one write of what
//! the batch changes, one pass of the queue and one publish, ending where
//! the same actions taken one at a time end.

use super::disk_room::DiskHeld;
use super::intake::Resolving;
use super::retire::{Shown, mark_hold};
use super::scripts::{Ending, StoppedSeeding};
use super::{ActionError, Asked, TorrentService, locked, state_error};
use crate::StateError;
use crate::categories::model::resolve_category_config;
use crate::jobs::JobId;
use crate::jobs::handle::{CancellationOrigin, QueueAction, QueueMoveTarget};
use crate::jobs::model::{FieldUpdate, JobUpdate, PRIORITY_ATTRIBUTE_KEY};
use crate::settings::SeedingPolicy;
use crate::torrents::metrics::FetchOutcome;
use crate::torrents::persistence::TorrentSettingRows;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::persistence::TorrentsPersistenceMarkTorrentRetiringInternalQueries;
use crate::torrents::{KeptIntent, TorrentIntent};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Range;
use std::sync::PoisonError;
use std::sync::atomic::Ordering;

pub(super) type Outcome = BTreeMap<JobId, Result<(), ActionError>>;

/// How many removed torrents a batch takes out before it writes their
/// history rows, in one transaction, and lets them go. It bounds how long
/// that transaction holds the database's one writer, whatever the batch's
/// size, and how many removals a crash before it commits leaves to be
/// rebuilt from their retiring marks, with less than their history rows
/// would have said. At 32, a request naming thousands of torrents still
/// writes about thirty times fewer transactions than one at a time does,
/// and each holds the writer for a few dozen rows and their files.
pub const HISTORY_CHUNK: usize = 32;

/// One change to a torrent's own settings, as its own action makes it.
#[derive(Debug, Clone, Copy)]
pub enum TorrentSetting {
    /// As [`TorrentService::set_forced`].
    Forced(bool),
    /// As [`TorrentService::set_sequential`].
    Sequential(bool),
    /// As [`TorrentService::set_first_last_piece_first`].
    FirstLastPieceFirst(bool),
    /// As [`TorrentService::set_rate_limits`].
    RateLimits {
        upload: Option<Option<u64>>,
        download: Option<Option<u64>>,
    },
    /// As [`TorrentService::set_policy_override`].
    Policy(Option<SeedingPolicy>),
}

impl TorrentSetting {
    // The change as it is kept, or why it is refused, as its own action
    // refuses it before it changes anything.
    fn checked(self) -> Result<Self, ActionError> {
        match self {
            Self::RateLimits { upload, download } => {
                if let Some(limit) = [upload, download]
                    .into_iter()
                    .flatten()
                    .flatten()
                    .find(|limit| i64::try_from(*limit).is_err())
                {
                    return Err(ActionError::Invalid(format!(
                        "a limit of {limit} bytes a second is past the largest, {}",
                        i64::MAX
                    )));
                }
                Ok(self)
            }
            Self::Policy(policy) => {
                if let Some(policy) = &policy {
                    policy.validate().map_err(ActionError::Invalid)?;
                }
                Ok(Self::Policy(SeedingPolicy::kept(policy)))
            }
            Self::Forced(_) | Self::Sequential(_) | Self::FirstLastPieceFirst(_) => Ok(self),
        }
    }
}

impl TorrentService {
    /// The actions of one request, each run of the same kind as one unit.
    /// The runs keep the request's order, and the moves end at the first
    /// that fails, as they did when each came on its own.
    pub async fn act_many(&self, actions: Vec<(JobId, QueueAction)>) -> Outcome {
        let mut outcome = Outcome::new();
        let mut moves_failed = false;
        let mut actions = actions.into_iter().peekable();
        while let Some((job_id, action)) = actions.next() {
            match action {
                QueueAction::Pause => {
                    let mut ids = vec![job_id];
                    while let Some((job_id, _)) =
                        actions.next_if(|(_, action)| matches!(action, QueueAction::Pause))
                    {
                        ids.push(job_id);
                    }
                    outcome.extend(self.pause_many(&ids).await);
                }
                QueueAction::Resume => {
                    let mut ids = vec![job_id];
                    while let Some((job_id, _)) =
                        actions.next_if(|(_, action)| matches!(action, QueueAction::Resume))
                    {
                        ids.push(job_id);
                    }
                    outcome.extend(self.resume_many(&ids).await);
                }
                QueueAction::Cancel(origin) => {
                    let mut removals = vec![(job_id, deletes_files(origin))];
                    while let Some((job_id, QueueAction::Cancel(origin))) =
                        actions.next_if(|(_, action)| matches!(action, QueueAction::Cancel(_)))
                    {
                        removals.push((job_id, deletes_files(origin)));
                    }
                    outcome.extend(self.remove_many(&removals, false).await);
                }
                QueueAction::Update(update) => {
                    let mut updates = vec![(job_id, update)];
                    while let Some((job_id, QueueAction::Update(update))) =
                        actions.next_if(|(_, action)| matches!(action, QueueAction::Update(_)))
                    {
                        updates.push((job_id, update));
                    }
                    outcome.extend(self.update_many(updates).await);
                }
                QueueAction::Move(target) => {
                    let mut moves = vec![(job_id, target)];
                    while let Some((job_id, QueueAction::Move(target))) =
                        actions.next_if(|(_, action)| matches!(action, QueueAction::Move(_)))
                    {
                        moves.push((job_id, target));
                    }
                    if moves_failed {
                        continue;
                    }
                    let moved = self.move_many(&moves).await;
                    moves_failed = moved.len() < moves.len() || moved.values().any(Result::is_err);
                    outcome.extend(moved);
                }
            }
        }
        outcome
    }

    /// Each change made to its torrent as its own action makes it, the
    /// changes of one request as one unit: one write of what they change,
    /// then a pass of what that moves, and one publish. They are made in
    /// order, and one refused ends them, as it ended them one at a time:
    /// those before it are made and those after it are not.
    pub async fn set_many(&self, changes: Vec<(JobId, TorrentSetting)>) -> Outcome {
        let mut outcome = Outcome::new();
        let mut checked = Vec::with_capacity(changes.len());
        for (job_id, change) in changes {
            match change.checked() {
                Ok(change) => checked.push((job_id, change)),
                Err(error) => {
                    outcome.insert(job_id, Err(error));
                    break;
                }
            }
        }
        let ids: Vec<JobId> = checked.iter().map(|(job_id, _)| *job_id).collect();
        self.ensure(&ids).await;
        let (mut queue, mut limits, mut picking) = (false, false, false);
        {
            let mut live = self.inner.live.lock().await;
            checked.retain(|(job_id, _)| {
                let here = live.contains_key(job_id);
                if !here {
                    outcome.insert(*job_id, Err(ActionError::NotFound(*job_id)));
                }
                here
            });
            if checked.is_empty() {
                return outcome;
            }
            let mut rows = TorrentSettingRows::default();
            for &(job_id, change) in &checked {
                match change {
                    TorrentSetting::Forced(on) => rows.forced.push((job_id, on)),
                    TorrentSetting::Sequential(on) => rows.sequential.push((job_id, on)),
                    TorrentSetting::FirstLastPieceFirst(on) => {
                        rows.first_last_piece_first.push((job_id, on));
                    }
                    TorrentSetting::RateLimits { upload, download } => {
                        rows.upload_limit
                            .extend(upload.map(|limit| (job_id, limit)));
                        rows.download_limit
                            .extend(download.map(|limit| (job_id, limit)));
                    }
                    TorrentSetting::Policy(policy) => rows.policy_override.push((job_id, policy)),
                }
            }
            if let Err(error) = self.inner.db.set_torrent_settings(&rows) {
                return failed(outcome, &ids_of(&checked), &error);
            }
            for (job_id, change) in checked {
                let Some(torrent) = live.get_mut(&job_id) else {
                    continue;
                };
                let record = &mut torrent.record;
                match change {
                    TorrentSetting::Forced(on) => {
                        record.forced = on;
                        queue = true;
                    }
                    TorrentSetting::Sequential(on) => {
                        record.sequential = on;
                        picking = true;
                    }
                    TorrentSetting::FirstLastPieceFirst(on) => {
                        record.first_last_piece_first = on;
                        picking = true;
                    }
                    TorrentSetting::RateLimits { upload, download } => {
                        if let Some(upload) = upload {
                            record.upload_limit = upload;
                        }
                        if let Some(download) = download {
                            record.download_limit = download;
                        }
                        limits = true;
                    }
                    TorrentSetting::Policy(policy) => {
                        record.policy_override = policy;
                        queue = true;
                    }
                }
                outcome.insert(job_id, Ok(()));
            }
        }
        if limits {
            self.apply_limits().await;
        }
        // New goals and forced flags move what the holds and the queue make
        // of the torrents.
        if queue {
            self.apply_queue().await;
        }
        if picking {
            self.apply_picking().await;
        }
        self.publish().await;
        outcome
    }

    // `pause` for each torrent: one write of their intents, then one pass
    // of the queue and one publish.
    pub(super) async fn pause_many(&self, job_ids: &[JobId]) -> Outcome {
        self.ensure(job_ids).await;
        let now = self.inner.clock.unix_ms();
        let mut outcome = Outcome::new();
        let mut stopped = Vec::new();
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            let present = present_among(&live, job_ids, &mut outcome);
            if present.is_empty() {
                return outcome;
            }
            if let Err(error) = self.inner.db.update_torrent_intents(&[(
                KeptIntent::Intent(TorrentIntent::Paused),
                present.as_slice(),
            )]) {
                return failed(outcome, &present, &error);
            }
            for &job_id in &present {
                let Some(torrent) = live.get_mut(&job_id) else {
                    continue;
                };
                stopped.push(self.seeding_stopped(torrent, StoppedSeeding::Paused));
                landings.push(self.intend(torrent, TorrentIntent::Paused, now));
                if let Some(Resolving::Fetching(fetch)) = &torrent.resolving {
                    fetch.abort();
                    self.inner.counters.fetch(FetchOutcome::Cancelled);
                    torrent.resolving = Some(Resolving::Paused);
                    // A paused magnet shows no fetch's table, as one paused
                    // before its fetch began.
                    torrent.fetching = None;
                }
                outcome.insert(job_id, Ok(()));
            }
        }
        for landing in landings {
            let _ = self.landed(landing).await;
        }
        for stopped in stopped {
            self.queue_script_event(stopped);
        }
        self.apply_queue().await;
        self.publish().await;
        outcome
    }

    // `resume` for each torrent: one write of their intents, then one pass
    // of the queue and one publish.
    pub(super) async fn resume_many(&self, job_ids: &[JobId]) -> Outcome {
        self.ensure(job_ids).await;
        let now = self.inner.clock.unix_ms();
        let mut outcome = Outcome::new();
        // A resume is the go-ahead to restore a torrent that crashed the
        // daemon. Its marker goes first, so a panic in the restore leaves a
        // fresh one.
        let marked: BTreeSet<JobId> = {
            let mut marks = self
                .inner
                .crash_marks
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            job_ids
                .iter()
                .filter_map(|&job_id| marks.take(job_id).map(|mark| (job_id, mark)))
                .map(|(job_id, mark)| {
                    mark.consume();
                    job_id
                })
                .collect()
        };
        let mut landings = Vec::new();
        let mut refetch = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            let present = present_among(&live, job_ids, &mut outcome);
            if present.is_empty() {
                return outcome;
            }
            for job_id in present.iter().filter(|job_id| marked.contains(job_id)) {
                if let Some(torrent) = live.get_mut(job_id) {
                    torrent.restore_held = None;
                    // Its job is restored at the next tick.
                    self.inner.restore_wanted.store(true, Ordering::Release);
                }
            }
            if let Err(error) = self.inner.db.update_torrent_intents(&[(
                KeptIntent::Intent(TorrentIntent::Resumed),
                present.as_slice(),
            )]) {
                return failed(outcome, &present, &error);
            }
            for &job_id in &present {
                let Some(torrent) = live.get_mut(&job_id) else {
                    continue;
                };
                landings.push(self.intend(torrent, TorrentIntent::Resumed, now));
                torrent.copy_error = None;
                // A torrent waiting for room fetches again now, once,
                // whatever the readings say, as a usenet job held for a full
                // disk does when resumed.
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
                // A fetch the pause ended starts again.
                if torrent
                    .resolving
                    .take_if(|fetch| matches!(fetch, Resolving::Paused))
                    .is_some()
                {
                    refetch.push(job_id);
                }
                outcome.insert(job_id, Ok(()));
            }
        }
        for landing in landings {
            let _ = self.landed(landing).await;
        }
        for job_id in refetch {
            self.resolve(job_id).await;
        }
        // A start looks again for files that were missing.
        let mut found = Vec::new();
        for (&job_id, _) in outcome.iter().filter(|(_, result)| result.is_ok()) {
            if self.look_again(job_id).await {
                found.push(job_id);
            }
        }
        self.apply_queue().await;
        for job_id in found {
            self.spawn_attach(job_id).await;
        }
        self.publish().await;
        outcome
    }

    // `update` for each torrent: one write of the categories and forced
    // flags, then a pass of the queue when a flag changed, and one publish.
    pub(super) async fn update_many(&self, updates: Vec<(JobId, JobUpdate)>) -> Outcome {
        self.ensure(
            &updates
                .iter()
                .map(|(job_id, _)| *job_id)
                .collect::<Vec<_>>(),
        )
        .await;
        let settings = self.settings();
        let mut outcome = Outcome::new();
        // What each update asks, checked before anything is changed: a
        // category it names must exist.
        let mut asked: Vec<(JobId, Option<Option<u32>>, Option<bool>)> = Vec::new();
        for (job_id, update) in updates {
            if update.name.is_some() || !matches!(update.password, FieldUpdate::Unchanged) {
                outcome.insert(
                    job_id,
                    Err(ActionError::Invalid(
                        "a torrent keeps its name and has no password".into(),
                    )),
                );
                continue;
            }
            let forced = match &update.metadata {
                FieldUpdate::Unchanged => None,
                FieldUpdate::Clear => Some(false),
                FieldUpdate::Set(metadata) => Some(metadata.iter().any(|(key, value)| {
                    key.eq_ignore_ascii_case(PRIORITY_ATTRIBUTE_KEY)
                        && super::queue::top_priority(value)
                })),
            };
            let name = match update.category {
                FieldUpdate::Unchanged => None,
                FieldUpdate::Clear => Some(None),
                FieldUpdate::Set(name) => Some(Some(name)),
            };
            let category = match name {
                None => None,
                Some(name) => match name.as_deref().filter(|name| !name.trim().is_empty()) {
                    None => Some(None),
                    Some(name) => match resolve_category_config(&settings.categories, name) {
                        Some(category) => Some(Some(category.id)),
                        None => {
                            outcome.insert(
                                job_id,
                                Err(ActionError::Invalid(format!("no category {name}"))),
                            );
                            continue;
                        }
                    },
                },
            };
            if category.is_none() && forced.is_none() {
                // An update that asks nothing of a torrent changes nothing.
                outcome.insert(job_id, Ok(()));
                continue;
            }
            asked.push((job_id, category, forced));
        }
        if asked.is_empty() {
            return outcome;
        }
        let rules = self.rules();
        let mut moved = Vec::new();
        let any_forced;
        {
            let mut live = self.inner.live.lock().await;
            asked.retain(|(job_id, _, _)| {
                let here = live.contains_key(job_id);
                if !here {
                    outcome.insert(*job_id, Err(ActionError::NotFound(*job_id)));
                }
                here
            });
            if asked.is_empty() {
                return outcome;
            }
            let categories: Vec<(JobId, Option<u32>)> = asked
                .iter()
                .filter_map(|&(job_id, category, _)| category.map(|category| (job_id, category)))
                .collect();
            let forced: Vec<(JobId, bool)> = asked
                .iter()
                .filter_map(|&(job_id, _, forced)| forced.map(|forced| (job_id, forced)))
                .collect();
            if let Err(error) = self
                .inner
                .db
                .set_torrent_categories_and_forced(&categories, &forced)
            {
                let ids: Vec<JobId> = asked.iter().map(|(job_id, _, _)| *job_id).collect();
                return failed(outcome, &ids, &error);
            }
            any_forced = !forced.is_empty();
            for (job_id, category, forced) in asked {
                let Some(torrent) = live.get_mut(&job_id) else {
                    continue;
                };
                if let Some(category_id) = category {
                    let rule = torrent.rule(&rules);
                    let before = settings.scope(torrent.record.category_id, rule);
                    torrent.record.category_id = category_id;
                    if before != settings.scope(category_id, rule) {
                        self.move_out(torrent).await;
                        torrent.route = None;
                        torrent.held = None;
                        moved.push(job_id);
                    }
                }
                if let Some(forced) = forced {
                    torrent.record.forced = forced;
                }
                outcome.insert(job_id, Ok(()));
            }
        }
        // Answered without waiting on the new routes, as an add is.
        for job_id in moved {
            self.spawn_attach(job_id).await;
        }
        if any_forced {
            self.apply_queue().await;
        }
        self.publish().await;
        outcome
    }

    /// `remove` for each torrent, `confirm_unmet_goal` for each as `remove`
    /// takes it: their gates held, one write of their removal marks, then
    /// each removed as `remove_gated` removes one, and one publish once
    /// the last is out.
    pub async fn remove_many(
        &self,
        removals: &[(JobId, bool)],
        confirm_unmet_goal: bool,
    ) -> Outcome {
        self.ensure(
            &removals
                .iter()
                .map(|(job_id, _)| *job_id)
                .collect::<Vec<_>>(),
        )
        .await;
        let mut outcome = Outcome::new();
        // Their outcome is their job's: a job still waiting for the tick
        // is restored first.
        for &(job_id, _) in removals {
            self.restore_job(job_id).await;
        }
        // A session taking one finishes first; one not yet taking it finds
        // it gone and never does.
        let gates: BTreeMap<JobId, (bool, _)> = {
            let live = self.inner.live.lock().await;
            let mut gates = BTreeMap::new();
            for &(job_id, delete_files) in removals {
                match live.get(&job_id) {
                    Some(torrent) => {
                        gates
                            .entry(job_id)
                            .or_insert((delete_files, std::sync::Arc::clone(&torrent.gate)));
                    }
                    None => {
                        outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                    }
                }
            }
            gates
        };
        if gates.is_empty() {
            return outcome;
        }
        // Taken in job order, so two batches never hold each other's, and
        // then removed in the order asked, as one at a time removed them.
        let mut held = BTreeMap::new();
        for (job_id, (delete_files, gate)) in gates {
            held.insert(job_id, (delete_files, gate.lock_owned().await));
        }
        let gated: Vec<_> = removals
            .iter()
            .filter_map(|(job_id, _)| {
                let (delete_files, guard) = held.remove(job_id)?;
                Some((*job_id, delete_files, guard))
            })
            .collect();
        // The rest runs on its own task: a request dropped part way cannot
        // leave a torrent marked or flagged half removed.
        let service = self.clone();
        let removed =
            tokio::spawn(async move { service.remove_gated_many(gated, confirm_unmet_goal).await })
                .await;
        match removed {
            Ok(removed) => outcome.extend(removed),
            Err(error) => {
                for &(job_id, _) in removals {
                    outcome.entry(job_id).or_insert_with(|| {
                        Err(ActionError::State(format!(
                            "the removal did not finish: {error}"
                        )))
                    });
                }
            }
        }
        outcome
    }

    async fn remove_gated_many(
        &self,
        gated: Vec<(JobId, bool, tokio::sync::OwnedMutexGuard<()>)>,
        confirm_unmet_goal: bool,
    ) -> Outcome {
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let mut outcome = Outcome::new();
        let mut begun = Vec::new();
        // Those with files, whose rows say what they made.
        let mut stored = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for (job_id, delete_files, _) in &gated {
                let Some(torrent) = live.get_mut(job_id) else {
                    outcome.insert(*job_id, Err(ActionError::NotFound(*job_id)));
                    continue;
                };
                let has_store = torrent.store.is_some();
                match self.begin_removal(torrent, &settings, now, confirm_unmet_goal) {
                    Ok(()) => {
                        begun.push((*job_id, *delete_files));
                        if has_store {
                            stored.push(*job_id);
                        }
                    }
                    Err(error) => {
                        outcome.insert(*job_id, Err(error));
                    }
                }
            }
        }
        // Their rows are marked retiring, as one removal's is, in one write.
        mark_hold().await;
        let db = self.inner.db.clone();
        let marks = begun.clone();
        let marked = tokio::task::spawn_blocking(move || db.mark_torrents_retiring(&marks))
            .await
            .map_err(|error| StateError::Database(error.to_string()))
            .and_then(|result| result);
        match marked {
            Ok(()) => {}
            Err(StateError::OutOfSpace(error)) => {
                tracing::warn!(error = %error, torrents = begun.len(), "no room to mark the torrents' removal; it goes on unmarked");
            }
            Err(error) => {
                for (job_id, _) in begun {
                    let refused = self.mark_refused(job_id, &error).await;
                    outcome.insert(job_id, Err(refused));
                }
                return outcome;
            }
        }
        // What each made, read for all of them in a few statements, not two
        // reads a torrent.
        let db = self.inner.db.clone();
        let mut made = match stored.is_empty() {
            true => HashMap::new(),
            false => tokio::task::spawn_blocking(move || db.torrents_made(&stored))
                .await
                .unwrap_or_default(),
        };
        // Each shows its end, and gives up its slot and rank, by the one
        // publish and the one pass after the last. Their history rows are
        // one write for each chunk, once the chunk's last is out.
        let any = !begun.is_empty();
        for chunk in begun.chunks(HISTORY_CHUNK) {
            let mut retired = Vec::with_capacity(chunk.len());
            for &(job_id, delete_files) in chunk {
                let taken = self
                    .take_out(
                        job_id,
                        delete_files,
                        Ending::Asked,
                        Shown::ByBatch,
                        made.remove(&job_id),
                    )
                    .await;
                match taken {
                    Ok(one) => {
                        retired.push(one);
                        outcome.insert(job_id, Ok(()));
                    }
                    Err(error) => {
                        outcome.insert(job_id, Err(error));
                    }
                }
            }
            self.to_history(retired).await;
        }
        if any {
            self.inner.signal.want_pass();
            self.publish().await;
        }
        drop(gated);
        outcome
    }

    // `reorder` of each torrent in turn, one move after another as they
    // came, with one write of the places that changed, one pass of the
    // queue and one publish. A move of a torrent not found ends the moves.
    //
    // Asked for while the stored torrents are still being read, the moves
    // are kept and made once the last is in, among all of them.
    pub(super) async fn move_many(&self, moves: &[(JobId, QueueMoveTarget)]) -> Outcome {
        self.ensure(&moves.iter().map(|(job_id, _)| *job_id).collect::<Vec<_>>())
            .await;
        {
            let live = self.inner.live.lock().await;
            // Under the lock the load ends under: kept, or made now.
            let mut marks = locked(&self.inner.load_marks);
            if self.loading() {
                let mut outcome = Outcome::new();
                let mut kept = Vec::with_capacity(moves.len());
                for &(job_id, target) in moves {
                    if !live.contains_key(&job_id) {
                        outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                        break;
                    }
                    kept.push((job_id, target));
                    outcome.insert(job_id, Ok(()));
                }
                if !kept.is_empty() {
                    marks.asked.push(Asked::Moves(kept));
                }
                return outcome;
            }
        }
        let (outcome, moved) = self.move_now(moves).await;
        if moved {
            self.apply_queue().await;
            self.publish().await;
        }
        outcome
    }

    // `move_many` among the torrents here now, the queue not yet applied,
    // and whether any place changed.
    pub(super) async fn move_now(&self, moves: &[(JobId, QueueMoveTarget)]) -> (Outcome, bool) {
        let mut outcome = Outcome::new();
        {
            let mut live = self.inner.live.lock().await;
            // The torrents still downloading, in queue order, and the places
            // they hold among themselves; a move gives them out again in the
            // new order, and a seed keeps its own.
            let mut order: Vec<(i64, JobId)> = live
                .values()
                .filter(|torrent| !torrent.seeding())
                .map(|torrent| (torrent.record.queue_position, torrent.record.job_id))
                .collect();
            order.sort_unstable();
            let places: Vec<i64> = order.iter().map(|(place, _)| *place).collect();
            let mut order = Spliced::new(
                order.into_iter().map(|(_, job_id)| job_id).collect(),
                &places,
            );
            let mut first_change = None;
            for (index, &(job_id, target)) in moves.iter().enumerate() {
                if !live.contains_key(&job_id) {
                    outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                    break;
                }
                let spliced = order.splice(job_id, target);
                if first_change.is_none() && spliced {
                    first_change = Some(index);
                }
                outcome.insert(job_id, Ok(()));
            }
            let Some(first_change) = first_change else {
                return (outcome, false);
            };
            let moved: Vec<(JobId, i64)> = order
                .order
                .iter()
                .zip(places.iter().copied())
                .filter(|(job_id, place)| live[*job_id].record.queue_position != *place)
                .map(|(job_id, place)| (*job_id, place))
                .collect();
            if moved.is_empty() {
                return (outcome, false);
            }
            if let Err(error) = self.inner.db.set_torrent_queue_positions(&moved) {
                // Nothing of the moves was kept: the first that changed
                // anything fails, and those after it never ran.
                for (job_id, _) in &moves[first_change..] {
                    outcome.remove(job_id);
                }
                let (job_id, _) = moves[first_change];
                outcome.insert(job_id, Err(ActionError::State(state_error(error))));
                return (outcome, false);
            }
            for (job_id, place) in moved {
                if let Some(torrent) = live.get_mut(&job_id) {
                    torrent.record.queue_position = place;
                }
            }
        }
        (outcome, true)
    }
}

fn deletes_files(origin: CancellationOrigin) -> bool {
    origin != CancellationOrigin::UserKeepingFiles
}

// The ids of `job_ids` among the live torrents, once each in the order
// named; each one missing is not found.
fn present_among<V>(
    live: &BTreeMap<JobId, V>,
    job_ids: &[JobId],
    outcome: &mut Outcome,
) -> Vec<JobId> {
    let mut seen = BTreeSet::new();
    let mut present = Vec::with_capacity(job_ids.len());
    for &job_id in job_ids {
        if !seen.insert(job_id) {
            continue;
        }
        if live.contains_key(&job_id) {
            present.push(job_id);
        } else {
            outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
        }
    }
    present
}

fn ids_of<T>(changes: &[(JobId, T)]) -> Vec<JobId> {
    changes.iter().map(|(job_id, _)| *job_id).collect()
}

// A write that failed fails every torrent it was for.
fn failed(mut outcome: Outcome, job_ids: &[JobId], error: &StateError) -> Outcome {
    for &job_id in job_ids {
        outcome.insert(job_id, Err(ActionError::State(error.to_string())));
    }
    outcome
}

// The torrents still downloading, in queue order, as one move after
// another changes it, each move as `splice_job_order` makes it. Each
// torrent's index is kept, so a move costs the places it moves over, not
// the length of the queue.
pub(super) struct Spliced {
    pub(super) order: Vec<JobId>,
    at: HashMap<JobId, usize>,
    // The runs of slots that share a place, in order. Torrents in one run
    // keep the order a fresh read gives them: by job.
    ties: Vec<Range<usize>>,
}

impl Spliced {
    // `order` sorted by place then job, with `places` the place of each
    // slot.
    pub(super) fn new(order: Vec<JobId>, places: &[i64]) -> Self {
        let at = order
            .iter()
            .enumerate()
            .map(|(index, job_id)| (*job_id, index))
            .collect();
        let mut ties = Vec::new();
        let mut start = 0;
        while start < places.len() {
            let mut end = start + 1;
            while end < places.len() && places[end] == places[start] {
                end += 1;
            }
            if end - start > 1 {
                ties.push(start..end);
            }
            start = end;
        }
        Self { order, at, ties }
    }

    // Moves `job_id` to `target`; false when it did not move, or is not in
    // the order.
    pub(super) fn splice(&mut self, job_id: JobId, target: QueueMoveTarget) -> bool {
        let Some(&current) = self.at.get(&job_id) else {
            return false;
        };
        let last = self.order.len() - 1;
        let to = match target {
            QueueMoveTarget::Top => 0,
            QueueMoveTarget::Bottom => last,
            QueueMoveTarget::Offset(delta) => {
                (current as i64).saturating_add(delta).clamp(0, last as i64) as usize
            }
        };
        if to == current {
            return false;
        }
        let (low, high) = (current.min(to), current.max(to));
        if current < to {
            self.order[low..=high].rotate_left(1);
        } else {
            self.order[low..=high].rotate_right(1);
        }
        self.index(low..high + 1);
        // Only the runs of shared places the move shifted can be out of
        // job order.
        let first = self.ties.partition_point(|run| run.end <= low);
        for run in self.ties[first..]
            .iter()
            .take_while(|run| run.start <= high)
            .cloned()
            .collect::<Vec<_>>()
        {
            self.order[run.clone()].sort_unstable();
            self.index(run);
        }
        true
    }

    fn index(&mut self, slots: Range<usize>) {
        for slot in slots {
            self.at.insert(self.order[slot], slot);
        }
    }
}
