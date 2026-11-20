// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tick as it was before the policy pass (e53173421): reading, the
// checkpoints, the goals, the queue, the picking and the limits, each a pass
// of its own, one after another. The bodies are those of that commit, moved
// here unchanged but for their names, so that a test can run the old tick
// and the new one over the same torrents and compare what each leaves.

use super::*;
use crate::post_processing::model::{QueueEvent, vars};
use crate::settings::model::TrackerRule;
use crate::settings::{Seeding, SeedingAction};
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::service::disk_room::{DiskHeld, StartHold};
use crate::torrents::service::live::Live;
use crate::torrents::service::queue::QueueEnd;
use crate::torrents::service::scripts::Ending;
use crate::torrents::service::settings::{TorrentSettings, WindDown};
use crate::torrents::service::{queued, ratelimits_of, take_turn};
use crate::torrents::status::{self, Queued};
use crate::torrents::{FilePriority, KeptIntent, TorrentIntent};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::PoisonError;

fn old_goal_met(
    settings: &TorrentSettings,
    rules: &[TrackerRule],
    torrent: &Live,
    now: i64,
) -> Option<Seeding> {
    if torrent.record.forced {
        return None;
    }
    let policy = settings.policy(&torrent.record, torrent.rule(rules));
    status::goal_left(&policy, &torrent.counters, torrent.have_bytes, now)
        .is_none()
        .then_some(policy)
}

impl TorrentService {
    // `tick` at e53173421.
    pub async fn old_tick(&self) {
        self.restore_jobs().await;
        self.follow_routes().await;
        let now = self.inner.clock.unix_ms();
        self.old_read(now, false).await;
        let unsaved = self.inner.signal.take_unsaved();
        self.save_checkpoints(unsaved.jobs().clone()).await;
        unsaved.queued();
        self.old_apply_goals(now).await;
        self.old_apply_queue().await;
        self.old_pick(None).await;
        self.old_apply_limits().await;
        self.drop_idle_sessions().await;
        self.read_sessions().await;
        self.publish().await;
        if self.wind_down().is_some()
            && !self.settings().torrent.enabled
            && !self.presence().running
        {
            let service = self.clone();
            self.keep(tokio::spawn(async move { service.end_wind_down().await }));
        }
        self.inner.ticks.send_modify(|ticks| *ticks += 1);
    }

    // `read` at e53173421.
    pub async fn old_read(&self, now: i64, at_once: bool) {
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            for torrent in live.values_mut() {
                if torrent.record.seed_mode
                    && let Some(attached) = &torrent.attached
                    && !attached.handle.seed_mode()
                {
                    torrent.record.seed_mode = false;
                    torrent.seed_verified = 0;
                    self.inner.signal.want_pass();
                    if let Err(error) = self.inner.db.clear_torrent_seed_mode(torrent.record.job_id)
                    {
                        tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not record that a torrent left seed mode");
                    }
                }
                if torrent.record.seed_mode
                    && let Some(attached) = &torrent.attached
                    && let Some(verified) = attached.handle.seed_verified()
                    && verified.count_ones() > torrent.seed_verified
                {
                    let count = verified.count_ones();
                    match self
                        .inner
                        .db
                        .set_torrent_seed_verified(torrent.record.job_id, verified.as_raw_slice())
                    {
                        Ok(_) => torrent.seed_verified = count,
                        Err(error) => {
                            tracing::warn!(job_id = torrent.record.job_id.0, error = %error, "could not keep the pieces a torrent checked in seed mode");
                        }
                    }
                }
                let reading = torrent.reading(now);
                torrent.engine = reading.engine.clone();
                torrent.speeds = reading.speeds;
                torrent.peers = reading.peers;
                torrent.keep(reading);
                landings.extend(self.queue_resume(torrent, now, at_once));
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
    }

    // `apply_goals` at e53173421.
    pub async fn old_apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let mut actions = Vec::new();
        let mut landings = Vec::new();
        let downloads_only =
            !settings.torrent.enabled && self.wind_down() == Some(WindDown::FinishDownloads);
        {
            let mut live = self.inner.live.lock().await;
            let mut wound_down = Vec::new();
            let mut met = Vec::new();
            for torrent in live.values_mut() {
                let met_policy = (torrent.seeding() && torrent.resumed())
                    .then(|| old_goal_met(&settings, &rules, torrent, now))
                    .flatten();
                if met_policy.is_none()
                    && downloads_only
                    && torrent.resumed()
                    && (torrent.complete() || torrent.seeding())
                {
                    torrent.settle(now);
                    torrent.record.kept = KeptIntent::WoundDown;
                    wound_down.push(torrent.record.job_id);
                    landings.extend(self.queue_resume(torrent, now, true));
                    continue;
                }
                let Some(policy) = met_policy else {
                    continue;
                };
                let finished = self.script_event(
                    torrent,
                    QueueEvent::SeedingFinished,
                    &[(vars::SEEDING_ACTION, policy.action.as_str())],
                );
                match policy.action {
                    SeedingAction::Stop => {
                        torrent.settle(now);
                        torrent.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
                        met.push(torrent.record.job_id);
                        landings.extend(self.queue_resume(torrent, now, true));
                        self.queue_script_event(finished);
                    }
                    SeedingAction::Remove => {
                        actions.push((torrent.record.job_id, false, finished));
                    }
                    SeedingAction::RemoveWithData => {
                        actions.push((torrent.record.job_id, true, finished));
                    }
                }
            }
            if !wound_down.is_empty() || !met.is_empty() {
                let db = self.inner.db.clone();
                match tokio::task::spawn_blocking(move || {
                    db.update_torrent_intents(&[
                        (KeptIntent::WoundDown, &wound_down),
                        (KeptIntent::Intent(TorrentIntent::Stopped), &met),
                    ])
                })
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        tracing::warn!(error = %error, "could not keep the stops of torrents done");
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "could not keep the stops of torrents done");
                    }
                }
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
        for (job_id, delete_files, finished) in actions {
            match self
                .remove_as(job_id, delete_files, true, Ending::Goal)
                .await
            {
                Ok(()) => self.queue_script_event(finished),
                Err(error) => {
                    tracing::warn!(job_id = job_id.0, error = %error, "a torrent that met its goal was not removed");
                }
            }
        }
    }

    // `apply_queue` at e53173421.
    pub async fn old_apply_queue(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let now = self.inner.clock.unix_ms();
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
            self.end_disk_room(torrent);
            torrent.ran(result);
        }
    }

    // `pick` at e53173421 (`fetch_levels` is the torrent's levels, cloned).
    pub async fn old_pick(&self, only: Option<&BTreeSet<JobId>>) {
        let live = self.inner.live.lock().await;
        let torrents: Box<dyn Iterator<Item = &Live> + Send> = match only {
            Some(only) => Box::new(only.iter().filter_map(|job_id| live.get(job_id))),
            None => Box::new(live.values()),
        };
        for torrent in torrents {
            let (Some(attached), Some(_)) = (&torrent.attached, &torrent.store) else {
                continue;
            };
            let handle = &attached.handle;
            let record = &torrent.record;
            if handle.sequential() != record.sequential {
                handle.set_sequential(record.sequential);
            }
            if handle.first_last_piece_first() != record.first_last_piece_first
                && let Err(error) = handle.set_first_last_piece_first(record.first_last_piece_first)
            {
                tracing::debug!(job_id = record.job_id.0, error = %error, "a torrent's first and last pieces did not move");
            }
            let wanted = torrent.levels.clone();
            let running = attached.handle.file_priorities();
            let changes: Vec<(usize, FilePriority)> = wanted
                .iter()
                .enumerate()
                .filter(|(index, level)| running.get(*index) != Some(*level))
                .map(|(index, level)| (index, *level))
                .collect();
            if changes.is_empty() {
                continue;
            }
            if let Err(error) = attached
                .session
                .set_file_priorities(&attached.handle, &changes)
                .await
            {
                tracing::debug!(job_id = torrent.record.job_id.0, error = %error, "a torrent's file levels did not change");
            }
        }
    }

    // `apply_limits` at e53173421.
    pub async fn old_apply_limits(&self) {
        let settings = self.settings();
        let rules = self.rules();
        let live = self.inner.live.lock().await;
        for torrent in live.values() {
            let Some(attached) = &torrent.attached else {
                continue;
            };
            let rule = torrent.rule(&rules);
            let limits = ratelimits_of(&torrent.record, settings.upload_cap(&torrent.record, rule));
            if attached.handle.ratelimits() != limits {
                attached.handle.set_ratelimits(limits);
            }
        }
    }
}
