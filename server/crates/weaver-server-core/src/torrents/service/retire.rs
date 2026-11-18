// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::intake::{Resolving, parse};
use super::live::Live;
use super::scripts::{Ending, StoppedSeeding};
use super::{ActionError, TorrentService, hashes_of, report, short_status, unpacking_dir};
#[cfg(test)]
use super::{MARK_HOLD, MOVE_HOLD, SYNC_HELD_FAILS, locked};
use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::handle::JobInfo;
use crate::jobs::model::{JobStatus, job_status_from_persisted_str};
use crate::jobs::report::JobReport;
use crate::persistence::Database;
use crate::post_processing::model::QueueEvent;
use crate::torrents::files::Files;
use crate::torrents::metrics::{FetchOutcome, TorrentEvent};
use crate::torrents::model::Adopted;
use crate::torrents::model::HistoryEnd;
use crate::torrents::persistence::HistoryFile;
use crate::torrents::side::Side;
use crate::torrents::store::TorrentStore;
use crate::torrents::{TorrentRecord, files, status};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
#[cfg(test)]
use tokio::sync::watch;
use weaver_torrent::core::Id20;

impl TorrentService {
    // The torrents' jobs, and among them, by when each was added, the
    // retiring ones whose history row waits for room.
    pub(super) fn with_retiring(&self, mut jobs: Vec<JobInfo>) -> Vec<JobInfo> {
        let retiring = self
            .inner
            .retiring
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if retiring.is_empty() {
            return jobs;
        }
        jobs.extend(retiring.values().cloned());
        jobs.sort_by(|one, other| {
            one.created_at_epoch_ms
                .total_cmp(&other.created_at_epoch_ms)
                .then(one.job_id.cmp(&other.job_id))
        });
        jobs
    }

    // The torrent cannot go on: it leaves as a failed job and its data stays.
    pub(super) async fn fail(&self, job_id: JobId, error: String) {
        let torrent = {
            let mut live = self.inner.live.lock().await;
            // One whose removal has begun is the removal's to retire: a
            // failure written beside its mark would leave the mark for a
            // later start to act on after its row went to history.
            // Kept on it: a removal that then fails before its mark lands
            // fails it with this.
            if let Some(torrent) = live.get_mut(&job_id)
                && torrent.removing
            {
                tracing::debug!(job_id = job_id.0, error = %error, "a torrent being removed failed; its removal retires it");
                torrent.failed_removing.get_or_insert(error);
                return;
            }
            self.unlive(&mut live, job_id)
        };
        let Some(mut torrent) = torrent else { return };
        let stopped = self.seeding_stopped(&torrent, StoppedSeeding::Failed);
        // Its slot and rank go to the torrents after it.
        self.inner.signal.want_pass();
        self.inner.counters.event(TorrentEvent::Failed);
        self.detach(&mut torrent).await;
        let written = read_written(self.written_of(&torrent)).await;
        self.retire(
            torrent,
            HistoryEnd::Failed,
            Some(error),
            written,
            Shown::Now,
        )
        .await;
        self.queue_script_event(stopped);
    }

    // Unpacking refused the torrent's output. Its job goes to history as
    // failed, once; the torrent stays, stopped with its data, until the
    // operator resumes it to seed.
    pub(super) async fn refused(&self, job_id: JobId, error: String) {
        match self.inner.db.get_job_history(job_id.0) {
            Ok(None) => {}
            Ok(Some(_)) => return,
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not read a torrent's history");
                return;
            }
        }
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let (report, hash, landing) = {
            let mut live = self.inner.live.lock().await;
            let Some(torrent) = live.get_mut(&job_id) else {
                return;
            };
            self.queue_script_event(self.seeding_stopped(torrent, StoppedSeeding::Failed));
            let landing = self.stop_torrent(torrent, now);
            let report = report(
                self.view(&settings, torrent, now),
                JobStatus::Failed { error },
                now,
            );
            (report, hashes_of(&torrent.record).best(), landing)
        };
        let _ = self.landed(landing).await;
        self.inner.counters.event(TorrentEvent::Failed);
        self.apply_queue().await;
        self.finish(report, HistoryEnd::Failed, hash, now).await;
    }

    // A torrent that left the live ones goes to history: its row, then out
    // of the torrents and into the finished jobs, listing of what it
    // `written` what is still there once its removal is done with it.
    async fn retire(
        &self,
        mut torrent: Live,
        status: HistoryEnd,
        error: Option<String>,
        written: Wrote,
        shown: Shown,
    ) {
        let job_id = torrent.record.job_id;
        let files = history_files(written.deletion).await;
        self.inner.env.outside.leave(job_id.0);
        // A copy out of the side store under way ends before the store goes.
        if let Some(task) = torrent.leveling.take().and_then(|leveling| leveling.task) {
            let _ = task.await;
        }
        if let Some(store) = &torrent.store {
            store.forget().await;
            // Its side store goes with it: nothing else reads it.
            if let Some(side) = store.side_path() {
                let removed = tokio::task::spawn_blocking(move || Side::remove(&side)).await;
                if let Ok(Err(error)) = removed {
                    tracing::warn!(job_id = job_id.0, error = %error, "could not remove a torrent's side store");
                }
            }
        }
        let (report, row) = self.history_of(&torrent, status, error);
        // Its history row and its removal are one write: the job is never
        // in neither, so an event raised while it was live, still being
        // queued, finds it.
        self.write_history(row, true, report.job_info(), files, shown)
            .await;
    }

    // The torrent's job as it ends now, `status` for `error`, and its
    // history row.
    fn history_of(
        &self,
        torrent: &Live,
        status: HistoryEnd,
        error: Option<String>,
    ) -> (JobReport, crate::history::JobHistoryRow) {
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        let mut report = report(
            self.view(&settings, torrent, now),
            job_status_from_persisted_str(status.as_str(), error.as_deref()),
            now,
        );
        report.shortfall = error;
        let row = report.history_row(
            status.as_str(),
            Some(hashes_of(&torrent.record).best().0.to_vec()),
            now,
        );
        (report, row)
    }

    // A torrent's job, finished as `status`: into history and the queue's
    // finished jobs, listing the files it wrote so far.
    pub(super) async fn finish(&self, report: JobReport, status: HistoryEnd, hash: Id20, now: i64) {
        let of = {
            let live = self.inner.live.lock().await;
            live.get(&report.job_id)
                .map(|torrent| self.written_of(torrent))
        };
        let files = match of {
            Some(of) => history_files(read_written(of).await.deletion).await,
            None => Vec::new(),
        };
        let row = report.history_row(status.as_str(), Some(hash.0.to_vec()), now);
        self.write_history(row, false, report.job_info(), files, Shown::Now)
            .await;
    }

    // The job's history row goes through the database writer's archives,
    // as an NZB job's does, with the `files` it wrote listed beside it for an
    // archive from history, and the torrent removed when it `retires`; then
    // the job goes to the queue's finished jobs. A row the database's disk
    // has no room for is kept there and written once its folder shows room;
    // the job reaches the finished jobs then, and this does not wait for
    // it. A row lost any other way is said, and the job goes on. A job
    // whose row waited for room shows its end at once when it is written:
    // the batch it ended in has published by then.
    async fn write_history(
        &self,
        row: crate::history::JobHistoryRow,
        retires: bool,
        job: JobInfo,
        files: Vec<HistoryFile>,
        shown: Shown,
    ) {
        let job_id = job.job_id;
        let turn = self.history_turn(job_id);
        let queued = self
            .inner
            .db
            .queue_torrent_history(job_id, row, retires, files);
        let (mut written, kept) = match queued {
            Ok(both) => both,
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "could not record a torrent's history");
                if self.end_history_turn(job_id, turn) {
                    self.finished(job, shown).await;
                }
                return;
            }
        };
        let landed = tokio::select! {
            biased;
            landed = &mut written => Some(landed.is_ok()),
            held = kept => match held {
                Ok(()) => None,
                Err(_) => Some((&mut written).await.is_ok()),
            },
        };
        match landed {
            Some(landed) => {
                if !self.end_history_turn(job_id, turn) {
                    return;
                }
                if !landed {
                    tracing::warn!(job_id = job_id.0, "could not record a torrent's history");
                }
                self.finished(job, shown).await;
            }
            None => {
                tracing::info!(
                    job_id = job_id.0,
                    "a torrent's history waits for room on the database's disk"
                );
                if retires {
                    self.show_retiring(&job);
                }
                let service = self.clone();
                tokio::spawn(async move {
                    let landed = written.await.is_ok();
                    // One a newer row took the place of was let go for it:
                    // the newer row's waiter finishes the job.
                    if service.end_history_turn(job_id, turn) {
                        if !landed {
                            tracing::warn!(
                                job_id = job_id.0,
                                "could not record a torrent's history"
                            );
                        }
                        service.finished(job, Shown::Now).await;
                    }
                    #[cfg(test)]
                    service.inner.history_waits.send_modify(|ended| *ended += 1);
                });
            }
        }
    }

    // A new turn for the job's history row, the latest from now on.
    fn history_turn(&self, job_id: JobId) -> u64 {
        let turn = self.inner.next_history_turn.fetch_add(1, Ordering::Relaxed);
        self.inner
            .history_turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(job_id, turn);
        turn
    }

    // Whether `turn` is still the job's latest, ending it when it is.
    fn end_history_turn(&self, job_id: JobId, turn: u64) -> bool {
        let mut turns = self
            .inner
            .history_turns
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if turns.get(&job_id) != Some(&turn) {
            return false;
        }
        turns.remove(&job_id);
        true
    }

    // Counts the waiters on a history row kept for room that have ended.
    #[cfg(test)]
    pub(crate) fn history_waits(&self) -> watch::Receiver<u64> {
        self.inner.history_waits.subscribe()
    }

    // A torrent removed whose history row the database's disk has no room
    // for stays in the queue as an NZB job's retiring row does, saying it
    // waits for disk space, until the row is written.
    fn show_retiring(&self, job: &JobInfo) {
        let mut job = job.clone();
        job.download_wait_reason = Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON.to_owned());
        self.inner
            .retiring
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(job.job_id, job.clone());
        if let Some(queue) = self.queue() {
            queue.publish_owned_job(crate::jobs::Protocol::Torrent, job);
        }
    }

    // A torrent's job, its history row written: out to the queue's
    // finished jobs, and out of the torrents shown, at once or by the
    // publish its batch ends with.
    async fn finished(&self, job: JobInfo, shown: Shown) {
        let job_id = job.job_id;
        self.inner
            .retiring
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&job_id);
        if shown == Shown::Now {
            self.publish().await;
        }
        if let Some(queue) = self.queue()
            && let Err(error) = queue.retire_job(job).await
        {
            tracing::warn!(job_id = job_id.0, error = %error, "a torrent did not reach history");
        }
        self.inner.retired.send_modify(|retired| *retired += 1);
    }

    // The torrents a removal left retiring that the start could not write
    // to history: never live, each finishes its removal from its row as the
    // start does (`finish_retiring`) and is written through the kept
    // archives as a removal's row is, shown retiring while it waits for
    // room.
    pub(super) async fn retry_retiring(&self) {
        let (db, work) = (self.inner.db.clone(), self.inner.env.work.clone());
        let rows = match tokio::task::spawn_blocking(move || finish_retiring(&db, &work)).await {
            Ok(Ok(rows)) => rows,
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "could not read the torrents a removal left retiring");
                return;
            }
            Err(error) => {
                tracing::warn!(error = %error, "could not read the torrents a removal left retiring");
                return;
            }
        };
        for (_, history, files) in rows {
            let mut job = crate::operations::backup::job_info_from_history(history.clone());
            job.protocol = crate::jobs::Protocol::Torrent;
            self.write_history(history, true, job, files, Shown::Now)
                .await;
        }
    }

    /// Remove a torrent. A private torrent whose seeding goal is not met
    /// stays unless the removal says it knows.
    pub async fn remove(
        &self,
        job_id: JobId,
        delete_files: bool,
        confirm_unmet_goal: bool,
    ) -> Result<(), ActionError> {
        self.remove_as(job_id, delete_files, confirm_unmet_goal, Ending::Asked)
            .await
    }

    pub(super) async fn remove_as(
        &self,
        job_id: JobId,
        delete_files: bool,
        confirm_unmet_goal: bool,
        ending: Ending,
    ) -> Result<(), ActionError> {
        // Its outcome is its job's: a job still waiting for the tick is
        // restored first.
        self.restore_job(job_id).await;
        // A session taking it finishes first; one not yet taking it finds
        // it gone and never does.
        let gate = self
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .map(|torrent| Arc::clone(&torrent.gate))
            .ok_or(ActionError::NotFound(job_id))?;
        let gated = gate.lock_owned().await;
        // The rest runs on its own task: a request dropped part way, as a
        // client that goes away drops it, cannot leave the torrent marked
        // or flagged half removed. Once begun, the removal ends here.
        let service = self.clone();
        tokio::spawn(async move {
            let _gated = gated;
            service
                .remove_gated(job_id, delete_files, confirm_unmet_goal, ending)
                .await
        })
        .await
        .map_err(|error| ActionError::State(format!("the removal did not finish: {error}")))?
    }

    // `remove_as` once its gate is held, on a task of its own.
    async fn remove_gated(
        &self,
        job_id: JobId,
        delete_files: bool,
        confirm_unmet_goal: bool,
        ending: Ending,
    ) -> Result<(), ActionError> {
        let settings = self.settings();
        let now = self.inner.clock.unix_ms();
        {
            let mut live = self.inner.live.lock().await;
            let torrent = live.get_mut(&job_id).ok_or(ActionError::NotFound(job_id))?;
            self.begin_removal(torrent, &settings, now, confirm_unmet_goal)?;
        }
        // Its row is marked retiring, with whether its files go, while it is
        // still live and before anything of it goes: a stop from here on
        // brings it back retiring, never live, and the next start finishes
        // the removal. A mark the database's disk has no room for does not
        // hold the removal back, which may be what frees that room: it goes
        // on as it did before marks, its history row kept for room. A mark
        // refused any other way fails the removal, and nothing changed.
        match self.mark_retiring(job_id, delete_files).await {
            Ok(()) => {}
            Err(StateError::OutOfSpace(error)) => {
                tracing::warn!(job_id = job_id.0, error = %error, "no room to mark a torrent's removal; it goes on unmarked");
            }
            Err(error) => return Err(self.mark_refused(job_id, &error).await),
        }
        self.remove_marked(job_id, delete_files, ending, Shown::Now)
            .await
    }

    // Whether the torrent may be removed now, and if so its removal begun:
    // a private one whose goal is unmet stays unless the removal knows, and
    // one whose files are moving stays until the move ends.
    pub(super) fn begin_removal(
        &self,
        torrent: &mut Live,
        settings: &super::settings::TorrentSettings,
        now: i64,
        confirm_unmet_goal: bool,
    ) -> Result<(), ActionError> {
        if torrent.record.private && !confirm_unmet_goal {
            let policy = settings.policy(&torrent.record, torrent.rule(&self.rules()));
            if let Some(left) =
                status::goal_left(&policy, &torrent.counters, torrent.have_bytes, now)
            {
                return Err(ActionError::UnmetGoal(left));
            }
        }
        // A move of its files and its removal never overlap: one under way
        // refuses the other, each seen under the live lock.
        if torrent.moving.is_some() {
            return Err(ActionError::Invalid(
                "the torrent's files are moving; remove it once the move ends".to_owned(),
            ));
        }
        torrent.removing = true;
        Ok(())
    }

    // A removal whose mark was refused ends: the torrent is live again, and
    // a failure that came while it was being removed, the removal's to
    // retire, fails it now.
    pub(super) async fn mark_refused(&self, job_id: JobId, error: &StateError) -> ActionError {
        let failed = self
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .and_then(|torrent| {
                torrent.removing = false;
                torrent.failed_removing.take()
            });
        if let Some(failed) = failed {
            self.fail(job_id, failed).await;
        }
        ActionError::State(format!(
            "the torrent was not removed: its removal could not be recorded ({error})"
        ))
    }

    // The rest of a removal once its mark is written: out of the live
    // torrents, its files gone when they go, and into history, its end
    // shown as `shown` says.
    pub(super) async fn remove_marked(
        &self,
        job_id: JobId,
        delete_files: bool,
        ending: Ending,
        shown: Shown,
    ) -> Result<(), ActionError> {
        let mut torrent = self
            .unlive(&mut *self.inner.live.lock().await, job_id)
            .ok_or(ActionError::NotFound(job_id))?;
        let written = read_written(self.written_of(&torrent)).await;
        // The job's outcome: complete, short of what unpacking would
        // deliver, refused by unpacking, or cut short.
        let (status, error) = match (torrent.complete(), torrent.refusal()) {
            (true, None) => (HistoryEnd::Complete, None),
            (true, Some(why)) => {
                let own = torrent
                    .store
                    .as_ref()
                    .map(|store| store.whole_names())
                    .unwrap_or_default();
                let status = short_status(&own, &written.outputs, &why);
                (HistoryEnd::delivered(&status), Some(why))
            }
            (false, Some(refusal)) => (HistoryEnd::Failed, Some(refusal)),
            (false, None) => (HistoryEnd::Cancelled, None),
        };
        // What its scripts hear of a removal asked for: a seed stops
        // seeding; a download is cancelled, as an NZB job's is.
        let ended = match ending {
            Ending::Goal => None,
            Ending::Asked if torrent.seeding() && torrent.resumed() => {
                self.seeding_stopped(&torrent, StoppedSeeding::Removed)
            }
            Ending::Asked if !torrent.complete() => self.script_event(
                &torrent,
                QueueEvent::TorrentDeleted,
                &[("NZBNA_DELETESTATUS", "MANUAL")],
            ),
            Ending::Asked => None,
        };
        // Its slot and rank go to the torrents after it, by a pass asked for
        // now, or by the one its batch asks for once its last is out.
        if shown == Shown::Now {
            self.inner.signal.want_pass();
        }
        if let Some(Resolving::Fetching(fetch)) = torrent.resolving.take() {
            fetch.abort();
            self.inner.counters.fetch(FetchOutcome::Cancelled);
        }
        self.detach(&mut torrent).await;
        // Nothing of its unpacking writes as its files go.
        if let Some(tee) = torrent.store.as_ref().and_then(|store| store.tee()) {
            tee.stop().await;
        }
        // Nothing holds its files open as they go.
        if let Some(store) = &torrent.store {
            store.forget().await;
        }
        let written = if delete_files {
            let deleted = tokio::task::spawn_blocking(move || {
                delete_written(&written.deletion);
                written
            });
            deleted.await.unwrap_or_else(|error| {
                tracing::warn!(job_id = job_id.0, error = %error, "could not delete a torrent's files");
                Wrote::default()
            })
        } else {
            written
        };
        self.inner.counters.event(TorrentEvent::Removed);
        self.retire(torrent, status, error, written, shown).await;
        self.queue_script_event(ended);
        Ok(())
    }

    // A removal's mark on the torrent's row, written off the runtime.
    async fn mark_retiring(&self, job_id: JobId, delete_files: bool) -> Result<(), StateError> {
        mark_hold().await;
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || db.mark_torrent_retiring(job_id, delete_files))
            .await
            .map_err(|error| StateError::Database(error.to_string()))?
    }

    // What reading the torrent's written files takes, cloned under the
    // live lock so the read itself runs off it and off the runtime
    // (`read_written`).
    fn written_of(&self, torrent: &Live) -> WrittenOf {
        WrittenOf {
            db: self.inner.db.clone(),
            job_id: torrent.record.job_id,
            store: torrent.store.clone(),
            // A torrent placed in a shared folder has a folder of its own
            // claimed beside it, and the shared one is never its to remove.
            claimed: torrent
                .record
                .files_dir
                .as_ref()
                .and(torrent.record.storage_dir.as_deref())
                .map(PathBuf::from),
            work: self.inner.env.work.clone(),
        }
    }

    // What the torrent holds, written again from where its files are now
    // and what its row says unpacking placed beside them, read and
    // written off the runtime and outside the live lock. One whose files
    // are not known holds its whole folder.
    pub(super) async fn sync_held(&self, job_id: JobId) {
        if let Err(error) = self.write_held(job_id, None).await {
            tracing::warn!(job_id = job_id.0, error = %error, "could not write what a moved torrent holds; the next start writes it");
        }
    }

    // What the torrent holds once its files are at `to`, its storage and
    // files folders, added to what it holds before the first of them moves
    // there, so a file that reaches it is held from the moment it does
    // (`add_torrent_held`); what it held where they were stays until the
    // move ends (`sync_held`).
    pub(super) async fn hold_destination(
        &self,
        job_id: JobId,
        to: (Option<String>, Option<String>),
    ) -> Result<(), StateError> {
        #[cfg(test)]
        let held = locked(&MOVE_HOLD).take();
        #[cfg(test)]
        if let Some((reached, release)) = held {
            let _ = reached.send(());
            let _ = release.await;
        }
        self.write_held(job_id, Some(to)).await
    }

    // `sync_held`, or with `to`, `hold_destination`.
    async fn write_held(
        &self,
        job_id: JobId,
        to: Option<(Option<String>, Option<String>)>,
    ) -> Result<(), StateError> {
        let (storage, files, layout) = self
            .with_torrent(job_id, |torrent| {
                Ok((
                    torrent.record.storage_dir.clone(),
                    torrent.record.files_dir.clone(),
                    torrent.layout.clone(),
                ))
            })
            .await
            .map_err(|error| StateError::Database(error.to_string()))?;
        let adding = to.is_some();
        let (storage, files) = to.unwrap_or((storage, files));
        let db = self.inner.db.clone();
        tokio::task::spawn_blocking(move || {
            let outputs = db.torrent_outputs(job_id)?;
            let tops = layout.as_deref().and_then(|layout| {
                held_tops(
                    layout
                        .files()
                        .filter(|file| !file.padding)
                        .map(|file| file.name.as_str())
                        .chain(outputs.iter().map(String::as_str)),
                )
            });
            let held = held_of(storage.as_deref(), files.as_deref(), tops.as_ref());
            #[cfg(test)]
            if SYNC_HELD_FAILS.load(Ordering::Relaxed) == if adding { 2 } else { 1 } {
                return Err(StateError::Database("the held paths were refused".into()));
            }
            if adding {
                return db.add_torrent_held(job_id, &held);
            }
            db.set_torrent_held(job_id, held)
        })
        .await
        .map_err(|error| StateError::Database(error.to_string()))?
    }
}

// When a torrent that ended shows its end: at once, by a publish of the
// torrents, or by the one publish the batch it ended in makes once each of
// its torrents has ended. A publish of every torrent for each of a batch
// would cost the square of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Shown {
    Now,
    ByBatch,
}

// What reading a torrent's written files takes (`TorrentService::written_of`).
pub(in crate::torrents) struct WrittenOf {
    db: Database,
    job_id: JobId,
    store: Option<Arc<TorrentStore>>,
    claimed: Option<PathBuf>,
    work: PathBuf,
}

// What a torrent wrote: what a deletion of its files takes, and the files
// unpacking placed beside them, as its row keeps them.
#[derive(Debug, Default)]
pub(in crate::torrents) struct Wrote {
    pub(in crate::torrents) deletion: Deletion,
    pub(in crate::torrents) outputs: Vec<String>,
}

// `written_paths`, off the runtime: its two database reads and the marker
// read of its unpacking directory block.
pub(in crate::torrents) async fn read_written(of: WrittenOf) -> Wrote {
    let job_id = of.job_id;
    tokio::task::spawn_blocking(move || written_paths(&of))
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent wrote");
            Wrote::default()
        })
}

// What a deletion of the torrent's files removes (libtorrent
// `storage_utils::delete_files`): its listed files Weaver made, what
// unpacking placed beside them, and its tee's directory, nothing else.
// A file that was there before the torrent stays; so does a folder
// that was. On a failed read of what was there, every listed file is; when
// what was there was never written (a row from before it was kept), it is
// not known, and no file goes, as at a start (`retiring_deletion`).
fn written_paths(of: &WrittenOf) -> Wrote {
    let job_id = of.job_id;
    let Some(store) = &of.store else {
        return Wrote::default();
    };
    let adopted = of.db.torrent_adopted(job_id).unwrap_or_else(|error| {
        tracing::warn!(job_id = job_id.0, error = %error, "could not read what was on disk before a torrent");
        Some(Adopted {
            all_files: true,
            dirs: vec![String::new()],
            ..Adopted::default()
        })
    });
    let outputs = of.db.torrent_outputs(job_id).unwrap_or_else(|error| {
        tracing::warn!(job_id = job_id.0, error = %error, "could not read what a torrent unpacked");
        Vec::new()
    });
    let mut deletion = match &adopted {
        Some(adopted) => deletion(store.files(), adopted, &outputs, of.claimed.clone()),
        None => Deletion {
            dir: store.files().dir(),
            claimed: of.claimed.clone(),
            ..Deletion::default()
        },
    };
    deletion.owner = Some(job_id);
    // With or without a tee: one whose unpacked files were not placed
    // keeps its directory past it. It goes only as the torrent's job's.
    deletion.work = Some((of.work.clone(), unpacking_dir(&of.work, job_id), job_id));
    Wrote { deletion, outputs }
}

// What a deletion of a torrent's files removes: see `written_paths`.
#[derive(Debug, Default)]
pub(in crate::torrents) struct Deletion {
    // The job whose marker on `dir` or `claimed` lets that folder go once
    // the marker is all it holds.
    pub(in crate::torrents) owner: Option<JobId>,
    // The torrent's own files Weaver made, each removed only as a file.
    pub(in crate::torrents) paths: Vec<PathBuf>,
    // What its job made of them: unpacked files, each removed only while
    // its name holds a regular file. A folder or a link found there is not
    // what the job placed, and stays.
    pub(in crate::torrents) made: Vec<PathBuf>,
    // The directory the files are in.
    pub(in crate::torrents) dir: PathBuf,
    // Whether that directory is the torrent's own, to go once empty.
    pub(in crate::torrents) own: bool,
    // The folders that were there before the torrent, which stay.
    pub(in crate::torrents) kept_dirs: HashSet<PathBuf>,
    // The folder claimed for the torrent beside a shared one, to go once
    // empty.
    pub(in crate::torrents) claimed: Option<PathBuf>,
    // The torrent's unpacking directory, under its root, removed whole only
    // while its marker names the torrent's job.
    pub(in crate::torrents) work: Option<(PathBuf, PathBuf, JobId)>,
}

// What a deletion of the files takes: each listed file not among those
// adopted, and each of `outputs`, in the directory the files are in. With a
// folder `claimed` beside it, that directory is a shared one, and stays.
pub(in crate::torrents) fn deletion(
    files: &Files,
    adopted: &Adopted,
    outputs: &[String],
    claimed: Option<PathBuf>,
) -> Deletion {
    let dir = files.dir();
    let kept = files::adopted_paths(adopted);
    let listed = files
        .names()
        .flatten()
        .filter(|name| !adopted.all_files && !kept.contains(*name))
        .map(|name| dir.join(name));
    let paths = listed.collect();
    let made = outputs.iter().map(|output| dir.join(output)).collect();
    let kept_dirs = adopted.dirs.iter().map(|folder| dir.join(folder)).collect();
    Deletion {
        owner: None,
        paths,
        made,
        own: claimed.is_none(),
        dir,
        kept_dirs,
        claimed,
        work: None,
    }
}

// What a history row lists of a deletion: the torrent's own files and what
// its job made, each to go only as a file, and its unpacking directory, to
// go whole, while it carries the job's marker, as an NZB job's row records
// the working directory it kept. Only what is on disk as the row is written:
// never a path a removal of the torrent with its files just took. Read off
// the runtime.
async fn history_files(deletion: Deletion) -> Vec<HistoryFile> {
    tokio::task::spawn_blocking(move || listed_files(&deletion))
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(error = %error, "could not list what a torrent wrote");
            Vec::new()
        })
}

// [`history_files`], on the calling thread.
pub(in crate::torrents) fn listed_files(deletion: &Deletion) -> Vec<HistoryFile> {
    {
        let listed = deletion.paths.iter().map(|path| (path.clone(), false));
        // An unpacked file is listed only while its name holds a regular
        // file: a folder or a link there is someone else's.
        let made = deletion
            .made
            .iter()
            .filter(|path| is_file(path))
            .map(|path| (path.clone(), false));
        let work = deletion.work.as_ref().and_then(|(root, work, job_id)| {
            let owned = crate::jobs::working_dir::working_dir_owner(root, work)
                .is_ok_and(|owner| owner == Some(*job_id));
            owned.then(|| (work.clone(), true))
        });
        listed
            .chain(made)
            .chain(work)
            .filter(|(path, _)| std::fs::symlink_metadata(path).is_ok())
            .map(|(path, whole)| HistoryFile {
                path: path.to_string_lossy().into_owned(),
                whole,
            })
            .collect()
    }
}

// Start-up, and the retry of what it left: each torrent a removal left
// retiring finishes its removal from its row. One whose removal asked for
// its files has them deleted as a live removal deletes them, from its
// metainfo, what was there before it and what unpacking placed, under the
// same rules a removal from history keeps (`spare_held`); one that kept
// them lists them, its kept unpacking folder with them, so the sweep leaves
// it. Each comes back with its history row, built from its torrents row
// (`RetiringRow`), and the files that row lists. What live torrents hold
// is read once for all of them; when that read fails, those that would
// delete stay retiring for the next try. Reads and deletes on the calling
// thread: run off the runtime.
pub(crate) fn finish_retiring(
    db: &Database,
    work: &Path,
) -> Result<Vec<(JobId, crate::history::JobHistoryRow, Vec<HistoryFile>)>, StateError> {
    let rows = db.retiring_torrents()?;
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let planned: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let deletion = retiring_deletion(&row, work);
            (row, deletion)
        })
        .collect();
    let dirs: Vec<PathBuf> = planned
        .iter()
        .filter(|(row, _)| row.delete_files)
        .map(|(_, deletion)| deletion.dir.clone())
        .filter(|dir| !dir.as_os_str().is_empty())
        .collect();
    let held = LiveHeld::under(db, &dirs);
    if let Err(error) = &held {
        tracing::warn!(error = %error, "could not read what live torrents hold; removed torrents whose files go stay retiring");
    }
    let mut finished = Vec::with_capacity(planned.len());
    for (row, mut deletion) in planned {
        if row.delete_files {
            let Ok(held) = &held else {
                continue;
            };
            spare_held(&mut deletion, held, row.job_id);
            delete_written(&deletion);
        }
        finished.push((row.job_id, row.history, listed_files(&deletion)));
    }
    Ok(finished)
}

// What a retiring torrent's removal takes, from its row (`written_paths`
// for a live one): its files by its metainfo under the folder they are in,
// less what was there before it, and what unpacking placed; its unpacking
// folder by its marker. One whose metainfo or what was there before it does
// not read takes no file, and one with no folder takes only its unpacking
// folder.
fn retiring_deletion(row: &crate::torrents::persistence::RetiringRow, work: &Path) -> Deletion {
    let job_id = row.job_id;
    let claimed = row
        .files_dir
        .as_ref()
        .and(row.storage_dir.as_deref())
        .map(PathBuf::from);
    let base = row.files_dir.as_deref().or(row.storage_dir.as_deref());
    let files = match (row.metainfo.as_deref(), base) {
        (Some(bytes), Some(base)) => match parse(bytes) {
            Ok(parsed) => Files::new(&parsed.layout, PathBuf::from(base))
                .map_err(|error| tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's files do not read; they stay"))
                .ok(),
            Err(error) => {
                tracing::warn!(job_id = job_id.0, error = %error, "a removed torrent's metainfo does not read; its files stay");
                None
            }
        },
        _ => None,
    };
    let mut deletion = match (files, &row.adopted, &row.outputs) {
        (Some(files), Some(adopted), Some(outputs)) => deletion(&files, adopted, outputs, claimed),
        (files, ..) => Deletion {
            dir: files
                .map(|files| files.dir())
                .or_else(|| base.map(PathBuf::from))
                .unwrap_or_default(),
            claimed,
            ..Deletion::default()
        },
    };
    deletion.owner = Some(job_id);
    deletion.work = Some((work.to_path_buf(), unpacking_dir(work, job_id), job_id));
    deletion
}

// Leaves out of `deletion` what a removal from history would leave: what a
// live torrent holds (`held`), what lies under a link inside its folder or
// in a folder that is a link, and, in a folder of its own, everything while
// that folder's marker does not name its job.
fn spare_held(deletion: &mut Deletion, held: &LiveHeld, job_id: JobId) {
    use crate::jobs::working_dir::{OutputDirOwner, output_dir_owner};
    let dir = deletion.dir.clone();
    let linked = std::fs::symlink_metadata(&dir).is_ok_and(|found| found.file_type().is_symlink());
    let unowned = deletion.own && output_dir_owner(&dir) != Some(OutputDirOwner::Job(job_id));
    if linked || unowned {
        deletion.paths.clear();
        deletion.made.clear();
        deletion.own = false;
        return;
    }
    let mut links = Links::under(&dir);
    let mut spared = |path: &Path| held.holds(path, false) || links.above(path);
    deletion.paths.retain(|path| !spared(path));
    deletion.made.retain(|path| !spared(path));
}

// The one check that a removal under a torrent's folder does not leave it
// through a link (`SECURITY.md`, "Deletes are narrow"): whether a folder
// between `dir` and a path is a link. Each folder is read once, never
// followed. `dir` itself is not judged here.
pub(in crate::torrents) struct Links<'a> {
    dir: &'a Path,
    seen: HashMap<PathBuf, bool>,
}

impl<'a> Links<'a> {
    pub(in crate::torrents) fn under(dir: &'a Path) -> Self {
        Self {
            dir,
            seen: HashMap::new(),
        }
    }

    // Whether a folder above `path`, inside the folder, is a link.
    pub(in crate::torrents) fn above(&mut self, path: &Path) -> bool {
        let dir = self.dir;
        let seen = &mut self.seen;
        path.ancestors()
            .skip(1)
            .take_while(|folder| *folder != dir && folder.starts_with(dir))
            .any(|folder| {
                if let Some(linked) = seen.get(folder) {
                    return *linked;
                }
                let linked = std::fs::symlink_metadata(folder)
                    .is_ok_and(|found| found.file_type().is_symlink());
                seen.insert(folder.to_path_buf(), linked);
                linked
            })
    }
}

// Whether `path` holds a regular file, never following a link.
fn is_file(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|found| found.file_type().is_file())
}

// [`held_of`] for `record` as placed, off the runtime; its whole folder, as
// spelled, when that cannot run.
pub(super) async fn held_at(record: &TorrentRecord, tops: Option<BTreeSet<String>>) -> Vec<String> {
    let (storage, files) = (record.storage_dir.clone(), record.files_dir.clone());
    let whole: Vec<String> = files
        .clone()
        .or_else(|| storage.clone())
        .into_iter()
        .collect();
    tokio::task::spawn_blocking(move || held_of(storage.as_deref(), files.as_deref(), tops.as_ref()))
        .await
        .unwrap_or_else(|error| {
            tracing::warn!(job_id = record.job_id.0, error = %error, "could not read where a torrent's files are; it holds its whole folder");
            whole
        })
}

// `path` as the filesystem spells it, so two spellings of one place (through
// a link, or in another case where the filesystem ignores case) are one:
// the whole of it resolved when it is there and not a link; a link at its
// end kept, not followed, under its folder resolved; and a part that is not
// there yet as given, under the deepest folder of it that is.
pub(crate) fn held_key(path: &Path) -> PathBuf {
    if std::fs::symlink_metadata(path).is_ok_and(|found| !found.file_type().is_symlink())
        && let Ok(found) = std::fs::canonicalize(path)
    {
        return found;
    }
    under_resolved(path)
}

// `path` resolved, links and all, as far as it is there, and the rest as
// given: a folder's own spelling, a link to it included.
pub(super) fn resolved(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| under_resolved(path))
}

// `path`'s folder resolved, links and all, as far as it is there, and the
// rest as given.
fn under_resolved(path: &Path) -> PathBuf {
    match (path.parent(), path.file_name()) {
        (Some(parent), Some(name)) if !parent.as_os_str().is_empty() => resolved(parent).join(name),
        _ => path.to_path_buf(),
    }
}

// The first part of each of `names`, paths relative to a torrent's folder;
// None when one names the folder itself.
pub(crate) fn held_tops<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<BTreeSet<String>> {
    names
        .into_iter()
        .map(|name| {
            Path::new(name).components().find_map(|part| match part {
                std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
                _ => None,
            })
        })
        .collect()
}

// What a torrent holds where its files are (`torrent_held_paths`), through
// any link to its folders: under the folder its files are in, the first
// part of each of its files' and outputs' paths (`tops`), or that whole
// folder while they are not known, and the folder claimed for it when its
// files lie elsewhere. A part there now is held as the filesystem spells
// it ([`held_key`]: its case and form on disk, which an adopted folder may
// spell otherwise than the metainfo); one not there yet is held as the
// metainfo writes it, the name the torrent creates it by. Reads the
// filesystem: run off the runtime.
pub(crate) fn held_of(
    storage_dir: Option<&str>,
    files_dir: Option<&str>,
    tops: Option<&BTreeSet<String>>,
) -> Vec<String> {
    let Some(base) = files_dir.or(storage_dir) else {
        return Vec::new();
    };
    // The folders resolved whole: a save path given through a link holds
    // what lies where it leads.
    let base = resolved(Path::new(base));
    let mut held: Vec<PathBuf> = match tops {
        Some(tops) => tops.iter().map(|top| held_key(&base.join(top))).collect(),
        None => vec![base],
    };
    if let (Some(storage), Some(files)) = (storage_dir, files_dir)
        && storage != files
    {
        held.push(resolved(Path::new(storage)));
    }
    held.into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect()
}

// The paths live torrents hold under the folders history removes from, as
// [`held_key`] spells them, with every folder above each, so a listed
// folder that goes whole is known to hold one.
#[derive(Debug, Default)]
pub(crate) struct LiveHeld {
    pub(super) paths: HashSet<PathBuf>,
    pub(super) above: HashSet<PathBuf>,
}

impl LiveHeld {
    // What the live torrents hold under `dirs`, in one read for every
    // folder of the call (`live_held_paths`), never a torrent's metainfo.
    // Each folder is asked for as the filesystem spells it and as given:
    // the rows an upgrade seeds hold a torrent's folder as its row typed it,
    // until its first start writes them again as the filesystem spells
    // them. Reads the filesystem: run off the runtime.
    pub(crate) fn under(db: &Database, dirs: &[PathBuf]) -> Result<Self, StateError> {
        let mut keys: Vec<PathBuf> = dirs.iter().map(|dir| held_key(dir)).collect();
        keys.extend(dirs.iter().cloned());
        keys.sort();
        keys.dedup();
        let mut held = Self::default();
        for path in db.live_held_paths(&keys)? {
            held.above
                .extend(path.ancestors().skip(1).map(Path::to_path_buf));
            held.paths.insert(path);
        }
        Ok(held)
    }

    // Whether a live torrent holds `path`, is in it, or, for one that goes
    // `whole`, holds anything in it, by the path as the filesystem spells it
    // or as given (`under`).
    pub(super) fn holds(&self, path: &Path, whole: bool) -> bool {
        if self.paths.is_empty() {
            return false;
        }
        let key = held_key(path);
        [key.as_path(), path].into_iter().any(|path| {
            path.ancestors().any(|at| self.paths.contains(at))
                || (whole && self.above.contains(path))
        })
    }
}

// What an archive from history, or a delete with files, did with what a
// torrent's job listed: how many paths were there and went, and the folders
// it left because nothing proves Weaver may remove from them.
#[derive(Debug, Default)]
pub(crate) struct ListedRemoval {
    pub(crate) removed: usize,
    pub(crate) not_managed: Vec<PathBuf>,
}

// An archive from history, or a delete with files, of a torrent's job whose
// torrent is gone: the files its row lists go, under `dir`, the folder its
// row records, and only while that folder's marker names the job, read now
// (`output_dir_owner`), the proof an NZB job's archive takes of its output
// folder. A folder with no marker, or another job's, has nothing removed
// and is not managed. Under a proven folder a listed path stays when a live
// torrent holds it (`held`), when a folder between `dir` and it is a link,
// or when it climbs out; the folders each removal empties go, deepest
// first, never `dir`'s own parents, and `dir` goes once its marker is all
// it holds. A listed path outside `dir` is the torrent's in a shared
// folder, and stays, not managed; one under `intermediate`, its unpacking
// folder, goes by its marker as an NZB job's working folder does.
pub(crate) fn remove_history_files(
    job_id: JobId,
    intermediate: &Path,
    dir: &Path,
    files: &[HistoryFile],
    held: &LiveHeld,
) -> ListedRemoval {
    let mut removal = ListedRemoval::default();
    let mut outside = BTreeSet::new();
    let mut inside = Vec::new();
    for file in files {
        let path = PathBuf::from(&file.path);
        let plain = path.is_absolute()
            && path.components().all(|part| {
                !matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            });
        if plain && path.starts_with(intermediate) {
            continue;
        }
        if !plain || path == dir || !path.starts_with(dir) {
            // Said by the folder it is in, read without climbing: a
            // relative path names no folder, and is left unsaid.
            if path.is_absolute() && std::fs::symlink_metadata(&path).is_ok() {
                let mut at = PathBuf::new();
                for part in path.components() {
                    match part {
                        std::path::Component::ParentDir => {
                            at.pop();
                        }
                        std::path::Component::CurDir => {}
                        part => at.push(part),
                    }
                }
                outside.insert(at.parent().map_or_else(|| at.clone(), Path::to_path_buf));
            }
            continue;
        }
        inside.push((path, file.whole));
    }
    removal.not_managed.extend(outside);
    match std::fs::symlink_metadata(dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return removal,
        // A recorded folder that is a link is never followed: what it
        // points at was not proven the job's where the row recorded it.
        Ok(found) if found.file_type().is_symlink() => {
            tracing::warn!(job_id = job_id.0, dir = %dir.display(), "a torrent's recorded folder is a link; history leaves its files");
            removal.not_managed.push(dir.to_path_buf());
            return removal;
        }
        _ => {}
    }
    if crate::jobs::working_dir::output_dir_owner(dir)
        != Some(crate::jobs::working_dir::OutputDirOwner::Job(job_id))
    {
        tracing::warn!(job_id = job_id.0, dir = %dir.display(), "a torrent's folder does not carry its job's marker; history leaves its files");
        removal.not_managed.push(dir.to_path_buf());
        return removal;
    }
    // A link between the folder and a file would take the removal out of
    // it.
    let mut links = Links::under(dir);
    let mut folders = BTreeSet::new();
    // The folders its listed files are in, each swept once of the hidden
    // copies its job's moves left there (`JobMoves`), as a live removal
    // sweeps them (`delete_written`).
    let mut swept: HashSet<PathBuf> = HashSet::new();
    for (path, whole) in inside {
        if held.holds(&path, whole) {
            continue;
        }
        if links.above(&path) {
            tracing::warn!(file = %path.display(), "a torrent's listed file is under a link, and stays");
            continue;
        }
        if let Some(folder) = path.parent()
            && !swept.contains(folder)
        {
            swept.insert(folder.to_path_buf());
            crate::runtime::fs::sweep_move_copies(job_id.0, folder);
        }
        let found = match std::fs::symlink_metadata(&path) {
            Ok(found) => found,
            Err(_) => continue,
        };
        let removed = if found.is_dir() && whole {
            std::fs::remove_dir_all(&path)
        } else if found.is_dir() {
            tracing::warn!(file = %path.display(), "a folder is where a torrent's file was, and stays");
            continue;
        } else {
            std::fs::remove_file(&path)
        };
        match removed {
            Ok(()) => removal.removed += 1,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), error = %error, "could not remove a torrent's listed file");
            }
        }
        for folder in path.ancestors().skip(1).take_while(|at| *at != dir) {
            // The folders above one already taken were taken with it.
            if !folders.insert((folder.components().count(), folder.to_path_buf())) {
                break;
            }
        }
    }
    // Deepest first, so a folder emptied by the one below it goes too; one
    // that is not empty stays, as `remove_dir` leaves it.
    for (_, folder) in folders.iter().rev() {
        let _ = std::fs::remove_dir(folder);
    }
    match crate::jobs::working_dir::remove_emptied_output_dir(dir, job_id) {
        Ok(true) => removal.removed += 1,
        Ok(false) => {}
        Err(error) => {
            tracing::debug!(dir = %dir.display(), error = %error, "a torrent's folder stays");
        }
    }
    removal
}

// libtorrent `storage_utils::delete_files`: the torrent's files go, here
// with what its job made of them, and then each folder above them, up to
// its directory, that this leaves empty and that was not there before the
// torrent. Whatever else is in its directory stays, and so does a folder
// found where one of the torrent's files was (`delete_one_file` removes a
// file, never a folder's contents). What its job made is an unpacked file:
// it goes only while its name holds a regular file, so a folder or a link
// someone put there stays, whole. Nothing goes through a link inside the
// directory (`Links`), on a live removal as at a start.
pub(in crate::torrents) fn delete_written(deletion: &Deletion) {
    if let Some((root, work, job_id)) = &deletion.work {
        match crate::jobs::working_dir::remove_job_working_dir(root, work, *job_id) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(dir = %work.display(), error = %error, "a torrent's unpacking directory is not its own, or could not go; it stays");
            }
        }
    }
    let mut folders = BTreeSet::new();
    let mut links = Links::under(&deletion.dir);
    // The folders its unpacked files were moved into, each swept once of
    // the hidden copies its job's moves left there (`JobMoves`).
    let mut swept: HashSet<&Path> = HashSet::new();
    let listed = deletion.paths.iter().map(|path| (path, false));
    let made = deletion.made.iter().map(|path| (path, true));
    for (path, made) in listed.chain(made) {
        if links.above(path) {
            tracing::warn!(file = %path.display(), "a torrent's file is under a link, and stays");
            continue;
        }
        if let (true, Some(owner), Some(folder)) = (made, deletion.owner, path.parent())
            && swept.insert(folder)
        {
            crate::runtime::fs::sweep_move_copies(owner.0, folder);
        }
        let removed = match std::fs::symlink_metadata(path) {
            Ok(found) if found.is_dir() => {
                tracing::warn!(file = %path.display(), "a folder is where a torrent's file was, and stays");
                Ok(())
            }
            Ok(found) if made && !found.file_type().is_file() => {
                tracing::warn!(file = %path.display(), "an unpacked file's name does not hold a file, and is left");
                Ok(())
            }
            Ok(_) => std::fs::remove_file(path),
            Err(error) => Err(error),
        };
        match removed {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(file = %path.display(), error = %error, "could not delete a torrent's file");
            }
        }
        for folder in path.ancestors().skip(1) {
            if !folder.starts_with(&deletion.dir) {
                break;
            }
            if folder == deletion.dir && !deletion.own {
                break;
            }
            // The folders above one already taken were taken with it.
            if !folders.insert((folder.components().count(), folder.to_path_buf())) {
                break;
            }
        }
    }
    folders.extend(
        deletion
            .claimed
            .iter()
            .chain(deletion.own.then_some(&deletion.dir))
            .map(|folder| (folder.components().count(), folder.clone())),
    );
    // Deepest first, so a folder emptied by the one below it goes too; one
    // that is not empty stays, as `remove_dir` leaves it.
    // The torrent's own folder and the one claimed for it carry its job's
    // marker, which goes with them once it is all they hold.
    for (_, folder) in folders.iter().rev() {
        if deletion.kept_dirs.contains(folder) {
            continue;
        }
        let marked =
            (deletion.own && *folder == deletion.dir) || deletion.claimed.as_ref() == Some(folder);
        match deletion.owner.filter(|_| marked) {
            Some(owner) => {
                let _ = crate::jobs::working_dir::remove_emptied_output_dir(folder, owner);
            }
            None => {
                let _ = std::fs::remove_dir(folder);
            }
        }
    }
}

// A test's hold on a removal just before its mark is written.
pub(super) async fn mark_hold() {
    #[cfg(test)]
    let held = locked(&MARK_HOLD).take();
    #[cfg(test)]
    if let Some((reached, release)) = held {
        let _ = reached.send(());
        let _ = release.await;
    }
}
