// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::live::Live;
use super::moves::Moving;
use super::retire::{held_tops, resolved};
use super::scripts::Ending;
use super::settings::{TorrentSettings, WindDown};
use super::{TorrentService, hashes_of, report, short_status, unpacking_dir, unplaced};
use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::model::JobStatus;
use crate::post_processing::model::{QueueEvent, vars};
use crate::runtime::panic_trap::JobScope;
use crate::settings::model::TrackerRule;
use crate::settings::{Seeding, SeedingAction};
use crate::torrents::metrics::TorrentEvent;
use crate::torrents::relocate::Existing;
use crate::torrents::store::TorrentStore;
use crate::torrents::tee::{Tee, plan_outputs};
use crate::torrents::{KeptIntent, TorrentIntent, status};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;

// The seeding terms of a torrent whose seeding goal is met, `None` while one
// is left or for a forced torrent, which seeds past its goal.
fn goal_met(
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
    // Torrents a wind-down to the downloads alone stopped resume, now that
    // torrents are on: their intents written together, off the runtime,
    // under `live` as every intent write is. Torrents the operator stopped,
    // or their goals did, stay stopped, and so does one whose goal is met
    // by now: its mark is cleared in the same write, as its goal's stop.
    pub(super) async fn resume_wound_down(&self) {
        let now = self.inner.clock.now_ms();
        let settings = self.settings();
        let rules = self.rules();
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            let mut resumed = Vec::new();
            let mut met = Vec::new();
            for torrent in live.values_mut() {
                if !torrent.record.wound_down {
                    continue;
                }
                torrent.record.wound_down = false;
                if goal_met(&settings, &rules, torrent, now).is_some() {
                    met.push(torrent.record.job_id);
                    continue;
                }
                torrent.settle(now);
                torrent.record.intent = TorrentIntent::Resumed;
                resumed.push(torrent.record.job_id);
                landings.extend(self.queue_resume(torrent, now, true));
            }
            if resumed.is_empty() && met.is_empty() {
                return;
            }
            let db = self.inner.db.clone();
            match tokio::task::spawn_blocking(move || {
                db.update_torrent_intents(&[
                    (KeptIntent::Intent(TorrentIntent::Resumed), &resumed),
                    (KeptIntent::Intent(TorrentIntent::Stopped), &met),
                ])
            })
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(error = %error, "could not keep the resumes of torrents a wind-down stopped");
                }
                Err(error) => {
                    tracing::warn!(error = %error, "could not keep the resumes of torrents a wind-down stopped");
                }
            }
        }
        for landing in landings {
            let _ = self.landed(Some(landing)).await;
        }
    }

    // Unpacking is over: what it made goes beside the torrent's files,
    // never over one, and is kept on the torrent's row as its job's
    // checkpoint goes. Its directory goes last, with what it spent. The
    // torrent's files are claimed as a move claims them, so its directory
    // stays where it is with no lock its reads and writes wait on; while a
    // move has them, this waits for the move to end, which marks the
    // torrent. It waits while its job still uses its storage, so the next
    // settle asks again. Files it could not place are never removed for
    // it: they stay in its directory, which stays, and it ends short with
    // the ones it did place.
    pub(super) async fn unpacked(
        &self,
        job_id: JobId,
        store: &Arc<TorrentStore>,
        tee: &Arc<Tee>,
    ) -> Placing {
        let claimed = self
            .with_torrent(job_id, |torrent| {
                if torrent.moving.is_some() || torrent.removing {
                    return Ok(false);
                }
                torrent.moving = Some(Moving::Outputs);
                Ok(true)
            })
            .await;
        if !matches!(claimed, Ok(true)) {
            return Placing::Waits;
        }
        // The queue shows the torrent moving while its outputs move, and
        // the settle that placed them shows where they ended.
        self.publish_job(job_id).await;
        let releasing = Arc::clone(tee);
        let spent = match tokio::task::spawn_blocking(move || releasing.spent()).await {
            Ok(Ok(spent)) => spent,
            Ok(Err(why)) => {
                tracing::warn!(
                    job_id = job_id.0,
                    why,
                    "a torrent's unpacked files wait on its job"
                );
                self.unclaim_outputs(job_id).await;
                return Placing::Waits;
            }
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files were not placed");
                self.unclaim_outputs(job_id).await;
                return Placing::Waits;
            }
        };
        tee.stop_checkpoints();
        tee.forget();
        let work = tee.work().to_path_buf();
        let (planned, tee) = (Arc::clone(store), Arc::clone(tee));
        let planned = tokio::task::spawn_blocking(move || {
            let _job = JobScope::enter(job_id);
            let files = planned.files();
            let listed: Vec<PathBuf> = files.names().flatten().map(Path::to_path_buf).collect();
            let dir = files.dir();
            let outputs = plan_outputs(tee.work(), &dir, &listed, &spent);
            // Held before they move, as the row keeps them.
            let held = held_tops(outputs.names.iter().map(String::as_str)).map_or_else(
                || vec![resolved(&dir).to_string_lossy().into_owned()],
                |tops| {
                    let base = resolved(&dir);
                    tops.iter()
                        .map(|top| base.join(top).to_string_lossy().into_owned())
                        .collect()
                },
            );
            (outputs, held)
        })
        .await;
        let (outputs, held) = match planned {
            Ok((outputs, held)) => (Arc::new(outputs), held),
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files were not placed");
                let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
                    db.finish_torrent_unpack(job_id, &[], &[])
                });
                self.recorded(job_id, recorded.await);
                self.unclaim_outputs(job_id).await;
                return Placing::Short {
                    why: unplaced(&work, &error),
                    placed: Vec::new(),
                };
            }
        };
        // Kept on the row before they move, so a crash between leaves no
        // file of the torrent's that its delete does not know of.
        let names = outputs.names.clone();
        let kept = self.queue_save(job_id, "keep_torrent_outputs", move |db| {
            db.keep_torrent_outputs(job_id, &names, &held)
        });
        self.recorded(job_id, kept.await);
        let (from, moving) = (work.clone(), Arc::clone(&outputs));
        let placed = tokio::task::spawn_blocking(move || {
            let _job = JobScope::enter(job_id);
            moving
                .place(&from)
                .map_err(|error| (error.to_string(), Some(moving.there())))
        })
        .await
        .unwrap_or_else(|error| Err((error.to_string(), None)));
        // A move that failed takes off the row the files it put back; one
        // whose end is not known leaves every one on it, for its delete.
        let (names, unplaced_names) = match &placed {
            Ok(()) | Err((_, None)) => (outputs.names.clone(), Vec::new()),
            Err((_, Some(there))) => {
                let there: HashSet<&String> = there.iter().collect();
                let left = outputs
                    .names
                    .iter()
                    .filter(|name| !there.contains(name))
                    .cloned()
                    .collect();
                (Vec::new(), left)
            }
        };
        let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
            db.finish_torrent_unpack(job_id, &names, &unplaced_names)
        });
        self.recorded(job_id, recorded.await);
        self.unclaim_outputs(job_id).await;
        if let Err((error, there)) = placed {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files did not move beside it");
            return Placing::Short {
                why: unplaced(&work, &error),
                placed: there.unwrap_or_default(),
            };
        }
        let root = self.inner.env.work.clone();
        let removed = tokio::task::spawn_blocking(move || {
            crate::jobs::working_dir::remove_job_working_dir(&root, &work, job_id)
        })
        .await;
        if let Ok(Err(error)) = removed
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(job_id = job_id.0, error = %error, "could not remove a torrent's unpacking directory");
        }
        Placing::Placed
    }

    fn recorded(
        &self,
        job_id: JobId,
        recorded: Result<Result<(), StateError>, oneshot::error::RecvError>,
    ) {
        match recorded {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record what a torrent unpacked");
            }
            Err(_) => {
                tracing::warn!(
                    job_id = job_id.0,
                    "what a torrent unpacked was not recorded"
                );
            }
        }
    }

    // The directory a torrent's unpacking works in, marked the torrent's
    // job's as an NZB job's working directory is: `torrent-<id>`, or the
    // job's own suffixed name when an NZB job's marker holds that one.
    pub(super) fn unpacking_dir(&self, job_id: JobId) -> PathBuf {
        unpacking_dir(&self.inner.env.work, job_id)
    }

    // Where a torrent's side store is: in Weaver's working directory, beside
    // its unpacking directory and never in it, so it stays where it is
    // whatever moves or clears the torrent's files or its unpacking, and
    // goes only with the torrent.
    pub(super) fn side_store(&self, job_id: JobId) -> PathBuf {
        self.inner
            .env
            .work
            .join(format!("torrent-{}.side", job_id.0))
    }

    // The claim `unpacked` took on the torrent's files, given back.
    async fn unclaim_outputs(&self, job_id: JobId) {
        let _ = self
            .with_torrent(job_id, |torrent| {
                if matches!(torrent.moving, Some(Moving::Outputs)) {
                    torrent.moving = None;
                }
                Ok(())
            })
            .await;
    }

    // FILE_DOWNLOADED, once for each of `files` files a piece made whole,
    // while the torrent downloads: one heard after its download ended is
    // dropped, as an NZB job raises none once its download is over. The
    // event names no file, as an NZB job's does not.
    pub(super) async fn files_downloaded(&self, job_id: JobId, files: usize) {
        if files == 0 {
            return;
        }
        let live = self.inner.live.lock().await;
        let Some(torrent) = live.get(&job_id) else {
            return;
        };
        if torrent.complete() {
            return;
        }
        let Some(context) = self.script_event(torrent, QueueEvent::FileDownloaded, &[]) else {
            return;
        };
        for _ in 1..files {
            self.queue_script_event(Some(context.clone()));
        }
        self.queue_script_event(Some(context));
    }

    // The job is done: its completion is written on its own, so an intent
    // written beside it keeps it, and the ledger and peers as they stand.
    pub(super) async fn completed(&self, job_id: JobId) {
        let now = self.inner.clock.now_ms();
        let (landing, downloaded) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            if torrent.complete() {
                return;
            }
            torrent.settle(now);
            let was_seeding = torrent.seeding();
            // Its download finished in this run only if it downloaded in
            // this run: one added with its data in place, or restored with
            // nothing left to fetch, did not (qBittorrent marks a torrent
            // finished after its check, or added in seed mode, without
            // `torrentFinished`; libtorrent sends `completed` only for a run
            // that began short of a seed).
            let downloaded = torrent.counters.downloaded_bytes > torrent.downloaded_at_start;
            torrent.record.completed_at_ms = Some(now);
            // It seeds now: its slot and rank go to the torrents after it.
            // One that seeded already, as a seed-mode add does, held none.
            if !was_seeding {
                self.inner.signal.want_pass();
            }
            self.inner.counters.event(TorrentEvent::Completed);
            if let Err(error) = self.inner.db.set_torrent_completed(job_id, now) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's completion");
            }
            (
                (
                    self.queue_resume(torrent, now, true),
                    torrent.record.move_to.take(),
                ),
                downloaded,
            )
        };
        let (landing, move_to) = landing;
        let _ = self.landed(landing).await;
        // Its files are all there now: what it holds is spelled as the
        // filesystem spells them.
        self.respell_held(Some(job_id)).await;
        // One that downloaded under its download path moves under its save
        // path now, over any file of the same name there (qBittorrent
        // `TorrentImpl::handleTorrentFinished` and `adjustStorageLocation`).
        if let Some(to) = move_to {
            if let Err(error) = self.inner.db.clear_torrent_move_to(job_id) {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's move");
            }
            if let Err(error) = self.move_storage(job_id, to, Existing::Replace).await {
                tracing::warn!(job_id = job_id.0, error = %error, "a complete torrent did not move under its save path");
            }
        }
        // Raised where its files now are, once any move is over.
        if downloaded {
            let live = self.inner.live.lock().await;
            if let Some(torrent) = live.get(&job_id) {
                self.raise_script_event(torrent, QueueEvent::TorrentDownloaded, &[]);
            }
        }
    }

    // Unpacking ended short of what it would deliver, on a torrent with
    // every file it wants: the torrent completes and seeds on to its goal,
    // saying `why`, and its job goes to history as what it delivered says
    // (`short_status`): its files it has whole and the unpacked files
    // `placed` beside them.
    pub(super) async fn short(
        &self,
        job_id: JobId,
        store: &TorrentStore,
        why: String,
        placed: &[String],
    ) {
        self.completed(job_id).await;
        let status = short_status(&store.whole_names(), placed, &why);
        let settings = self.settings();
        let now = self.inner.clock.now_ms();
        let (report, hash) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            torrent.failed = Some(why.clone());
            let mut report = report(self.view(&settings, torrent, now), status.clone(), now);
            report.shortfall = Some(why);
            (report, hashes_of(&torrent.record).best())
        };
        if matches!(status, JobStatus::Failed { .. }) {
            self.inner.counters.event(TorrentEvent::Failed);
        }
        self.finish(report, status.persisted_status(), hash, now)
            .await;
    }

    // A seeding torrent whose seeding goal is met does what its policy says.
    pub(super) async fn apply_goals(&self, now: i64) {
        let settings = self.settings();
        let rules = self.rules();
        let mut actions = Vec::new();
        let mut landings = Vec::new();
        // Winding down to the downloads alone, a torrent done downloading,
        // forced or not, seeding or waiting to, stops: its goal is not met.
        let downloads_only =
            !settings.torrent.enabled && self.wind_down() == Some(WindDown::FinishDownloads);
        {
            let mut live = self.inner.live.lock().await;
            // Stopped by the wind-down, and by their goals: their intents
            // are written together.
            let mut wound_down = Vec::new();
            let mut met = Vec::new();
            for torrent in live.values_mut() {
                // A seed whose goal is met in this pass ends by its goal,
                // wind-down or not.
                let met_policy = (torrent.seeding() && torrent.resumed())
                    .then(|| goal_met(&settings, &rules, torrent, now))
                    .flatten();
                if met_policy.is_none()
                    && downloads_only
                    && torrent.resumed()
                    && (torrent.complete() || torrent.seeding())
                {
                    torrent.settle(now);
                    torrent.record.intent = TorrentIntent::Stopped;
                    // Not the operator's stop: it resumes when torrents are
                    // turned on again.
                    torrent.record.wound_down = true;
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
                        torrent.record.intent = TorrentIntent::Stopped;
                        torrent.record.wound_down = false;
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
            // One transaction, off the runtime. `live` is held across it,
            // as for every other intent write: a resume taken between the
            // two would otherwise be written over.
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
        // A removed torrent's event is queued once its job is in history,
        // where the queue finds it.
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
}

// What placing a torrent's unpacked files came to.
pub(super) enum Placing {
    // Not yet: the next settle asks again.
    Waits,
    // Every one is beside the torrent's files.
    Placed,
    // Some or all stay where unpacking made them, for `why`; `placed` are
    // the ones beside the torrent's files.
    Short { why: String, placed: Vec<String> },
}
