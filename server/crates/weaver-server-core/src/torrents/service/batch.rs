// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! A batch of queue actions on torrents as one unit: one write of what
//! the batch changes, one pass of the queue and one publish, ending where
//! the same actions taken one at a time end.

use super::disk_room::DiskHeld;
use super::intake::Resolving;
use super::retire::mark_hold;
use super::scripts::{Ending, StoppedSeeding};
use super::{ActionError, TorrentService, state_error};
use crate::StateError;
use crate::categories::model::resolve_category_config;
use crate::jobs::JobId;
use crate::jobs::handle::{CancellationOrigin, QueueAction, QueueMoveTarget};
use crate::jobs::model::{FieldUpdate, JobUpdate, PRIORITY_ATTRIBUTE_KEY};
use crate::torrents::metrics::FetchOutcome;
use crate::torrents::{KeptIntent, TorrentIntent};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::PoisonError;

pub(super) type Outcome = BTreeMap<JobId, Result<(), ActionError>>;

impl TorrentService {
    /// The actions of one request, each run of the same kind as one unit.
    /// The runs keep the request's order, and the moves end at the first
    /// that fails, as they did when each came on its own.
    pub(super) async fn act_many(&self, actions: Vec<(JobId, QueueAction)>) -> Outcome {
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
                    outcome.extend(self.remove_many(&removals).await);
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

    // `pause` for each torrent: one write of their intents, then one pass
    // of the queue and one publish.
    pub(super) async fn pause_many(&self, job_ids: &[JobId]) -> Outcome {
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

    // `remove` for each torrent: their gates held, one write of their
    // removal marks, then each removed as `remove_gated` removes one.
    pub(super) async fn remove_many(&self, removals: &[(JobId, bool)]) -> Outcome {
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
        // Taken in job order, so two batches never hold each other's.
        let mut gated = Vec::with_capacity(gates.len());
        for (job_id, (delete_files, gate)) in gates {
            gated.push((job_id, delete_files, gate.lock_owned().await));
        }
        // The rest runs on its own task: a request dropped part way cannot
        // leave a torrent marked or flagged half removed.
        let service = self.clone();
        let removed = tokio::spawn(async move { service.remove_gated_many(gated).await }).await;
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
    ) -> Outcome {
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let mut outcome = Outcome::new();
        let mut begun = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for (job_id, delete_files, _) in &gated {
                let Some(torrent) = live.get_mut(job_id) else {
                    outcome.insert(*job_id, Err(ActionError::NotFound(*job_id)));
                    continue;
                };
                match self.begin_removal(torrent, &settings, now, false) {
                    Ok(()) => begun.push((*job_id, *delete_files)),
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
        for (job_id, delete_files) in begun {
            let removed = self
                .remove_marked(job_id, delete_files, Ending::Asked)
                .await;
            outcome.insert(job_id, removed);
        }
        drop(gated);
        outcome
    }

    // `reorder` of each torrent in turn, one move after another as they
    // came, with one write of the places that changed, one pass of the
    // queue and one publish. A move of a torrent not found ends the moves.
    pub(super) async fn move_many(&self, moves: &[(JobId, QueueMoveTarget)]) -> Outcome {
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
            let mut order: Vec<JobId> = order.into_iter().map(|(_, job_id)| job_id).collect();
            let mut first_change = None;
            for (index, &(job_id, target)) in moves.iter().enumerate() {
                if !live.contains_key(&job_id) {
                    outcome.insert(job_id, Err(ActionError::NotFound(job_id)));
                    break;
                }
                let spliced = crate::jobs::handle::splice_job_order(&mut order, job_id, target)
                    .unwrap_or(false);
                // Torrents that share a place are taken by job, as a move
                // on its own reads them back.
                ties_by_job(&mut order, &places);
                if first_change.is_none() && spliced {
                    first_change = Some(index);
                }
                outcome.insert(job_id, Ok(()));
            }
            let Some(first_change) = first_change else {
                return outcome;
            };
            let moved: Vec<(JobId, i64)> = order
                .iter()
                .zip(places.iter().copied())
                .filter(|(job_id, place)| live[*job_id].record.queue_position != *place)
                .map(|(job_id, place)| (*job_id, place))
                .collect();
            if moved.is_empty() {
                return outcome;
            }
            if let Err(error) = self.inner.db.set_torrent_queue_positions(&moved) {
                // Nothing of the moves was kept: the first that changed
                // anything fails, and those after it never ran.
                for (job_id, _) in &moves[first_change..] {
                    outcome.remove(job_id);
                }
                let (job_id, _) = moves[first_change];
                outcome.insert(job_id, Err(ActionError::State(state_error(error))));
                return outcome;
            }
            for (job_id, place) in moved {
                if let Some(torrent) = live.get_mut(&job_id) {
                    torrent.record.queue_position = place;
                }
            }
        }
        self.apply_queue().await;
        self.publish().await;
        outcome
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

// A write that failed fails every torrent it was for.
fn failed(mut outcome: Outcome, job_ids: &[JobId], error: &StateError) -> Outcome {
    for &job_id in job_ids {
        outcome.insert(job_id, Err(ActionError::State(error.to_string())));
    }
    outcome
}

// Torrents given the same place keep the order a fresh read gives them:
// by job.
fn ties_by_job(order: &mut [JobId], places: &[i64]) {
    let mut start = 0;
    while start < places.len() {
        let mut end = start + 1;
        while end < places.len() && places[end] == places[start] {
            end += 1;
        }
        if end - start > 1 {
            order[start..end].sort_unstable();
        }
        start = end;
    }
}
