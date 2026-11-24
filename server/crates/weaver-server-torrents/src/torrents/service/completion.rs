// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::moves::Moving;
use super::policy::goal_met;
use super::retire::{held_tops, resolved};
#[cfg(any(test, feature = "test-support"))]
use super::{COMPLETION_FAILS, KEEP_FAILS, OUTPUT_MEETS, PLACE_PANICS, UNPACK_OVER_FAILS, locked};
use super::{MemoryRelease, TorrentService, unpacking_dir, unplaced};
use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::status::{Delivery, Failure, shortfall_message};
use crate::post_processing::model::QueueEvent;
use crate::torrents::metrics::TorrentEvent;
use crate::torrents::model::UnpackEnd;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertTorrentQueries;
use crate::torrents::relocate::Existing;
use crate::torrents::store::TorrentStore;
use crate::torrents::tee::{Tee, place_outputs, remove_unwanted_outputs};
use crate::torrents::{KeptIntent, TorrentIntent};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::oneshot;
use weaver_server_base::runtime::panic_trap::JobScope;
use weaver_server_disk::runtime::fs::JobMoves;

impl TorrentService {
    // Torrents a wind-down to the downloads alone stopped resume, now that
    // torrents are on: their intents written together, off the runtime,
    // under `live` as every intent write is. Torrents the operator stopped,
    // or their goals did, stay stopped, and so does one whose goal is met
    // by now: its mark is cleared in the same write, as its goal's stop.
    pub(super) async fn resume_wound_down(&self) {
        let now = self.inner.clock.unix_ms();
        let settings = self.settings();
        let rules = self.rules();
        let mut landings = Vec::new();
        {
            let mut live = self.inner.live.lock().await;
            let mut resumed = Vec::new();
            let mut met = Vec::new();
            for torrent in live.values_mut() {
                if !torrent.record.wound_down() {
                    continue;
                }
                // Its wind-down's mark goes; it stays stopped until resumed.
                torrent.record.kept = KeptIntent::Intent(TorrentIntent::Stopped);
                if goal_met(&settings, &rules, torrent, now).is_some() {
                    met.push(torrent.record.job_id);
                    continue;
                }
                torrent.settle(now);
                torrent.record.kept = KeptIntent::Intent(TorrentIntent::Resumed);
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
    // never over one, each by a move that never replaces, and the names of
    // the files that moved go on the torrent's row after, in the one write
    // its job's checkpoint goes in. Its directory goes last, with what it
    // spent. The
    // torrent's files are claimed as a move claims them, so its directory
    // stays where it is with no lock its reads and writes wait on; while a
    // move has them, this waits for the move to end, which marks the
    // torrent. It waits while its job still uses its storage, so the next
    // settle asks again. Files it could not place are never removed for
    // it: they stay in its directory, which stays, and it ends short with
    // the ones it did place. How it came out is the unpacking's own, kept
    // on the row in the same write: the torrent never reads it.
    pub(super) async fn unpacked(
        &self,
        job_id: JobId,
        store: &Arc<TorrentStore>,
        tee: &Arc<Tee>,
        (delivery, failure): (Delivery, Option<Failure>),
    ) -> Placing {
        // What the job delivered rules the unpacking as it rules a post's:
        // short of whole, it ends short with the reason a post's would give,
        // and with nothing whole delivered it places nothing.
        let (nothing, part) = match delivery.status(failure.as_ref()) {
            JobStatus::Complete => (None, None),
            // Why, as a post's job words it: the failure, what was withheld
            // or removed. Only when none says why, the delivery's own words
            // that nothing it made was whole: the torrent's own files may
            // be (`short_status`).
            JobStatus::Failed { error } => (
                Some(shortfall_message(&delivery, failure.as_ref()).unwrap_or(error)),
                None,
            ),
            _ => (None, shortfall_message(&delivery, failure.as_ref())),
        };
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
        // The job's content policy, when it leaves unwanted files out.
        let removes = tee.with(|job| job.writer().removes().cloned());
        if let Some(why) = nothing {
            return self
                .delivered_nothing(job_id, store, tee, spent, removes, why)
                .await;
        }
        let work = tee.work().to_path_buf();
        let (placing, tee, db) = (Arc::clone(store), Arc::clone(tee), self.inner.db.clone());
        let placed = tokio::task::spawn_blocking(move || {
            let _job = JobScope::enter(job_id);
            let files = placing.files();
            let listed: HashSet<PathBuf> = files.names().flatten().map(Path::to_path_buf).collect();
            let spent: HashSet<PathBuf> = spent.into_iter().collect();
            let dir = files.dir();
            // The names an earlier placing of this torrent's kept on its row.
            let kept = db.torrent_outputs(job_id).unwrap_or_else(|error| {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read the files a torrent placed before");
                Vec::new()
            });
            // A file unpacking made with an extension the policy deletes is
            // deleted before the rest are placed, and the job ends short.
            let removed = match &removes {
                Some(policy) => remove_unwanted_outputs(tee.work(), &listed, &spent, policy),
                None => Ok(Vec::new()),
            };
            let moves = JobMoves::of(job_id.0);
            let placed = place_outputs(tee.work(), &dir, &listed, &spent, &kept, |from, to| {
                move_output(&moves, from, to)
            });
            #[cfg(any(test, feature = "test-support"))]
            assert!(
                !PLACE_PANICS.swap(false, std::sync::atomic::Ordering::AcqRel),
                "placing a torrent's unpacked files"
            );
            let held = match placed.names.is_empty() {
                true => Vec::new(),
                false => held_of(&dir, &placed.names),
            };
            (placed, held, removed)
        })
        .await;
        let (placed, held, removed) = match placed {
            Ok(placed) => placed,
            // Its end is not known, as a panic leaves it. The row was not
            // written for this placing and names none of its files: any it
            // moved stay beside the torrent's, on no row.
            Err(error) => {
                return self
                    .not_placed(job_id, &work, &error.to_string(), Vec::new(), Vec::new())
                    .await;
            }
        };
        // What the policy left out, worded as the job's reason; a file it
        // could not remove stays where unpacking made it, and says why.
        let removed = match removed {
            Ok(removed) => crate::post_processing::model::unwanted_files_removed_reason(&removed),
            Err(why) => Some(why),
        };
        // How it came out: short when a file did not move beside the
        // torrent's, or the delivery was in part, or the policy left a file
        // out; whole otherwise.
        let placed_any = !placed.names.is_empty();
        let end = match placed.failed.as_ref().map(ToString::to_string) {
            Some(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files did not all move beside it");
                let why = unplaced(&work, &error);
                UnpackEnd::short(
                    placed_any,
                    match removed {
                        Some(removed) => format!("{why}; {removed}"),
                        None => why,
                    },
                )
            }
            None => match part.or(removed) {
                Some(why) => UnpackEnd::short(placed_any, why),
                None => UnpackEnd::whole(),
            },
        };
        // The one write of this placing, after its moves: the row keeps
        // exactly the names of the files it moved and how it came out, what
        // the torrent holds gains them, and the job's checkpoint goes.
        let (names, gone) = (placed.names.clone(), placed.gone.clone());
        let ending = end.clone();
        let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
            #[cfg(any(test, feature = "test-support"))]
            if KEEP_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel) {
                return Err(StateError::Database("refused".to_owned()));
            }
            db.finish_torrent_unpack(job_id, &names, &gone, &held, &ending)
        });
        let recorded = match recorded.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(error.to_string()),
            Err(_) => Err("the torrent's row was not written".to_owned()),
        };
        // A row that does not name them keeps no output: each goes back
        // where unpacking made it, by the move that placed it.
        if let Err(error) = recorded {
            let gone = placed.gone.clone();
            let there = tokio::task::spawn_blocking(move || {
                let moves = JobMoves::of(job_id.0);
                placed
                    .put_back(|from, to| moves.move_file(from, to))
                    .into_iter()
                    .map(|(_, name)| name)
                    .collect()
            })
            .await
            .unwrap_or_default();
            return self.not_placed(job_id, &work, &error, gone, there).await;
        }
        self.unpack_over(job_id, end.clone()).await;
        self.unclaim_outputs(job_id).await;
        // Its folder stays while anything in it is short: a file that did
        // not move is there.
        if end.why.is_some() {
            return Placing::Ended(end);
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
        Placing::Ended(end)
    }

    // No output is on the row for this placing: the row lets go of
    // `dropped`, the names it kept that hold no file, and the torrent ends
    // short with its outputs in its unpacking folder, but for `there`, the
    // ones that are beside its files and would not go back.
    async fn not_placed(
        &self,
        job_id: JobId,
        work: &Path,
        error: &str,
        dropped: Vec<String>,
        there: Vec<String>,
    ) -> Placing {
        tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unpacked files were not placed");
        let end = UnpackEnd::short(!there.is_empty(), unplaced(work, &error));
        let ending = end.clone();
        let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
            db.finish_torrent_unpack(job_id, &[], &dropped, &[], &ending)
        });
        if self.recorded(job_id, recorded.await) {
            self.unpack_over(job_id, end.clone()).await;
        }
        self.unclaim_outputs(job_id).await;
        Placing::Ended(end)
    }

    // Unpacking delivered no usable file whole: nothing it made is placed,
    // as a failed post's job publishes nothing, and what it made stays in
    // its unpacking folder, but for a file with an extension the policy
    // deletes, which goes as it does from any unpacking. The row is written
    // as a placing of nothing, and the torrent ends short for `why`.
    async fn delivered_nothing(
        &self,
        job_id: JobId,
        store: &Arc<TorrentStore>,
        tee: &Arc<Tee>,
        spent: Vec<PathBuf>,
        removes: Option<crate::post_processing::model::PostProcessingSettings>,
        why: String,
    ) -> Placing {
        if let Some(policy) = removes {
            let (store, tee) = (Arc::clone(store), Arc::clone(tee));
            let removed = tokio::task::spawn_blocking(move || {
                let _job = JobScope::enter(job_id);
                let files = store.files();
                let listed: HashSet<PathBuf> =
                    files.names().flatten().map(Path::to_path_buf).collect();
                let spent: HashSet<PathBuf> = spent.into_iter().collect();
                remove_unwanted_outputs(tee.work(), &listed, &spent, &policy)
            })
            .await;
            if let Ok(Err(error)) = removed {
                tracing::warn!(job_id = job_id.0, error = %error, "a torrent's unwanted unpacked files were not all removed");
            }
        }
        let end = UnpackEnd::short(false, why);
        let ending = end.clone();
        let recorded = self.queue_save(job_id, "finish_torrent_unpack", move |db| {
            db.finish_torrent_unpack(job_id, &[], &[], &[], &ending)
        });
        if self.recorded(job_id, recorded.await) {
            self.unpack_over(job_id, end.clone()).await;
        }
        self.unclaim_outputs(job_id).await;
        Placing::Ended(end)
    }

    // Whether the placing was written.
    fn recorded(
        &self,
        job_id: JobId,
        recorded: Result<Result<(), StateError>, oneshot::error::RecvError>,
    ) -> bool {
        match recorded {
            Ok(Ok(())) => true,
            Ok(Err(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record what a torrent unpacked");
                false
            }
            Err(_) => {
                tracing::warn!(
                    job_id = job_id.0,
                    "what a torrent unpacked was not recorded"
                );
                false
            }
        }
    }

    // Its unpacking is over, as `end` says, as the placing written for it
    // keeps on its row (`finish_torrent_unpack`).
    async fn unpack_over(&self, job_id: JobId, end: UnpackEnd) {
        let _ = self
            .with_torrent(job_id, |torrent| {
                torrent.record.unpack_over = true;
                torrent.record.unpack_end = Some(end);
                Ok(())
            })
            .await;
        self.let_go_of_tee(job_id).await;
    }

    // Its unpacking is over, so its store lets go of its tee: a piece it
    // has after, as a reopen fetches, goes to no job, in this run as after
    // a restart, which makes its store with none. The tee's runs are
    // stopped and waited on, and it gives back what it holds.
    async fn let_go_of_tee(&self, job_id: JobId) {
        let tee = self
            .with_torrent(job_id, |torrent| {
                Ok(torrent
                    .store
                    .as_ref()
                    .and_then(|store| store.let_go_of_tee()))
            })
            .await;
        if let Ok(Some(tee)) = tee {
            tee.stop().await;
            tee.stop_checkpoints();
            tee.forget();
        }
    }

    // The directory a torrent's unpacking works in, marked the torrent's
    // job's as an NZB job's working directory is: `torrent-<id>`, or the
    // job's own suffixed name when an NZB job's marker holds that one.
    pub fn unpacking_dir(&self, job_id: JobId) -> PathBuf {
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
    pub async fn completed(&self, job_id: JobId) {
        let now = self.inner.clock.unix_ms();
        let (landing, downloaded, finished) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            if torrent.complete() {
                return;
            }
            torrent.settle(now);
            let was_seeding = torrent.seeding();
            // What it held to download in this run goes back only if it
            // downloaded in this run.
            let downloaded = torrent.counters.downloaded_bytes > torrent.downloaded_at_start;
            // Its finish is told as qBittorrent runs its program on finish:
            // for every torrent that becomes finished, one added with its
            // data in place and checked whole or wanting no file among them
            // (libtorrent `files_checked` calls `finished()`, which posts
            // `torrent_finished_alert`, before `torrent_checked_alert`,
            // torrent.cpp:8805-8824, 8839 and 11953-11957 at v2.0.15), but
            // not one added in seed mode, which is finished from its add
            // (qBittorrent `SessionImpl::initLoadTorrentParams`,
            // sessionimpl.cpp:2844, and `TorrentImpl::handleTorrentFinished`,
            // torrentimpl.cpp:2194-2198). One restored complete never gets
            // here. One reopened finishes again, as in qBittorrent
            // (`prioritizeFiles` clears its finished state,
            // torrentimpl.cpp:3045-3057), seed mode or not: each end of its
            // download is numbered, its count written with its completion,
            // and the script queue admits an end past the last it holds for
            // the job (`drain_admissions`), so one reopened, or reopened and
            // restarted, runs its program once more.
            let occurrence = torrent.record.download_ends;
            let finished = !torrent.record.seed_mode || occurrence > 0;
            torrent.record.download_ends = occurrence.saturating_add(1);
            torrent.record.completed_at_ms = Some(now);
            // What its start promised and did not land goes back: it
            // fetches nothing more.
            if let Some(intake) = &torrent.intake {
                intake.room().release_start();
            }
            // It seeds now: its slot and rank go to the torrents after it.
            // One that seeded already, as a seed-mode add does, held none.
            if !was_seeding {
                self.inner.signal.want_pass();
            }
            self.inner.counters.event(TorrentEvent::Completed);
            #[cfg(any(test, feature = "test-support"))]
            let refused = COMPLETION_FAILS
                .swap(false, std::sync::atomic::Ordering::AcqRel)
                .then(|| StateError::Database("refused".to_owned()));
            #[cfg(not(any(test, feature = "test-support")))]
            let refused = None;
            let written = match refused {
                Some(error) => Err(error),
                None => {
                    let ends = torrent.record.download_ends;
                    self.db(move |db| db.set_torrent_completed(job_id, now, ends))
                        .await
                }
            };
            if let Err(error) = written {
                // The count stays owed to its checkpoints, which write it
                // until one lands: a row left with the count before this
                // end would number a later end as this one, after a
                // restart, and the script queue would take it as had.
                torrent.owe_row();
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's completion");
            }
            // Due now: its checkpoint goes in this settle.
            self.inner.signal.checkpoint_due(job_id);
            (
                (
                    self.queue_resume(torrent, now, true),
                    torrent.record.move_to.take(),
                ),
                downloaded,
                finished.then_some(occurrence),
            )
        };
        let (landing, move_to) = landing;
        // Its download in this run is over: what it held for that goes
        // back, as a finished NZB job's does.
        if downloaded {
            self.release_memory(MemoryRelease::Ended);
        }
        let _ = self.landed(landing).await;
        // Its files are all there now: what it holds is spelled as the
        // filesystem spells them.
        self.respell_held(vec![job_id]).await;
        // One that downloaded under its download path moves under its save
        // path now, over any file of the same name there (qBittorrent
        // `TorrentImpl::handleTorrentFinished` and `adjustStorageLocation`).
        if let Some(to) = move_to {
            if let Err(error) = self.db(move |db| db.clear_torrent_move_to(job_id)).await {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's move");
            }
            if let Err(error) = self.move_storage(job_id, to, Existing::Replace).await {
                tracing::warn!(job_id = job_id.0, error = %error, "a complete torrent did not move under its save path");
            }
        }
        // Raised where its files now are, once any move is over.
        if let Some(occurrence) = finished {
            let live = self.inner.live.lock().await;
            if let Some(torrent) = live.get(&job_id) {
                self.raise_download_end(torrent, occurrence);
            }
        }
    }

    // Its unpacking is over, as `end` says: one whose placing was not
    // written, as when the tee refused, keeps the mark and how it came out
    // in a write of its own, so a reopen of it in this run or after a
    // restart does not unpack again. A mark whose write failed stays owed
    // to the torrent's checkpoints, which write it until one lands. Its
    // store lets go of its tee. The torrent never reads how its unpacking
    // came out: it downloads, completes and seeds as one with nothing to
    // unpack does.
    pub(super) async fn unpack_ended(&self, job_id: JobId, end: UnpackEnd) {
        let marks = self
            .with_torrent(job_id, |torrent| {
                torrent.record.unpack_end = Some(end.clone());
                Ok(!std::mem::replace(&mut torrent.record.unpack_over, true))
            })
            .await;
        if matches!(marks, Ok(true)) {
            let marked = self.queue_save(job_id, "torrent_unpack_over", move |db| {
                #[cfg(any(test, feature = "test-support"))]
                if UNPACK_OVER_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel) {
                    return Err(StateError::Database("refused".to_owned()));
                }
                db.set_torrent_unpack_end(job_id, &end).map(|_| ())
            });
            let failed = match marked.await {
                Ok(Ok(())) => None,
                Ok(Err(error)) => Some(error.to_string()),
                Err(_) => Some("its write was dropped".to_owned()),
            };
            if let Some(error) = failed {
                tracing::warn!(
                    job_id = job_id.0,
                    error,
                    "could not record that a torrent's unpacking is over"
                );
                let _ = self
                    .with_torrent(job_id, |torrent| {
                        torrent.owe_row();
                        Ok(())
                    })
                    .await;
                self.inner.signal.checkpoint_due(job_id);
            }
        }
        self.let_go_of_tee(job_id).await;
    }
}

// What placing a torrent's unpacked files came to.
pub(super) enum Placing {
    // Not yet: the next settle asks again.
    Waits,
    // Over, as the end says: whole with every one beside the torrent's
    // files, or short with why.
    Ended(UnpackEnd),
}

// The move every output is placed by: never over what is there.
pub(super) fn move_output(moves: &JobMoves, from: &Path, to: &Path) -> std::io::Result<()> {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(meets) = locked(&OUTPUT_MEETS).as_mut() {
        meets(to);
    }
    moves.move_file(from, to)
}

// What a torrent holds for the outputs `names` under `dir`: each one's top
// part there, or the folder itself when one of them has no part of its own.
// It reads the disk, so it runs off the runtime.
fn held_of(dir: &Path, names: &[String]) -> Vec<String> {
    let base = resolved(dir);
    held_tops(names.iter().map(String::as_str)).map_or_else(
        || vec![base.to_string_lossy().into_owned()],
        |tops| {
            tops.iter()
                .map(|top| base.join(top).to_string_lossy().into_owned())
                .collect()
        },
    )
}
