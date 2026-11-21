// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::intake::Resolving;
use super::live::Live;
use super::scripts::{Ending, StoppedSeeding};
use super::{ActionError, MemoryRelease, TorrentService, hashes_of, report, unpacking_dir};
#[cfg(any(test, feature = "test-support"))]
use super::{MARK_HOLD, MOVE_HOLD, SYNC_HELD_FAILS, locked};
use crate::StateError;
use crate::jobs::JobId;
use crate::jobs::handle::JobInfo;
use crate::jobs::model::job_status_from_persisted_str;
use crate::jobs::report::JobReport;
use crate::persistence::Database;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::QueueEvent;
use crate::torrents::metrics::{FetchOutcome, TorrentEvent};
use crate::torrents::model::Adopted;
use crate::torrents::model::HistoryEnd;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceLiveHeldPathsInternalQueries;
use crate::torrents::persistence::TorrentsPersistenceMarkTorrentRetiringInternalQueries;
use crate::torrents::persistence::{HistoryFile, MadeRead};
use crate::torrents::side::Side;
use crate::torrents::store::TorrentStore;
use crate::torrents::{TorrentRecord, status};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
pub use weaver_server_pipeline::torrents::service::resolved;
pub(in crate::torrents) use weaver_server_pipeline::torrents::service::{
    Deletion, delete_written, deletion, listed_files,
};
pub use weaver_server_pipeline::torrents::service::{
    finish_retiring, held_key, held_of, held_tops,
};

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
    pub async fn fail(&self, job_id: JobId, error: String) {
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
        // What its start promised and did not land goes back.
        if let Some(intake) = &torrent.intake {
            intake.room().release_start();
        }
        let stopped = self.seeding_stopped(&torrent, StoppedSeeding::Failed);
        // Its slot and rank go to the torrents after it.
        self.inner.signal.want_pass();
        self.inner.counters.event(TorrentEvent::Failed);
        self.detach(&mut torrent).await;
        let written = read_written(self.written_of(&torrent)).await;
        let retired = self
            .ready_history(
                torrent,
                HistoryEnd::Failed,
                Some(error),
                written,
                Shown::Now,
                stopped,
            )
            .await;
        self.to_history(vec![retired]).await;
    }

    // A torrent that left the live ones, readied for history: its row, to
    // go out of the torrents and into the finished jobs (`to_history`), listing
    // of what it `written` what is still there once its removal is done
    // with it, and the script event its end raises, `ended`.
    async fn ready_history(
        &self,
        mut torrent: Live,
        status: HistoryEnd,
        error: Option<String>,
        written: Wrote,
        shown: Shown,
        ended: Option<EventContext>,
    ) -> Retired {
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
        Retired {
            row,
            job: report.job_info(),
            files,
            shown,
            short: !torrent.complete(),
            ended,
        }
    }

    // Torrents that left the live ones, to history, in order: their history
    // rows and their removals, one write for one torrent and one for a
    // batch's (`write_histories`); then for each, the memory a download cut
    // short held given back, and its script event queued. The job is never
    // in neither, so an event raised while it was live, still being queued,
    // finds it.
    pub(super) async fn to_history(&self, retired: Vec<Retired>) {
        let mut retired = retired;
        match retired.len() {
            0 => {}
            1 => {
                let Some(one) = retired.pop() else { return };
                let Retired {
                    row,
                    job,
                    files,
                    shown,
                    short,
                    ended,
                } = one;
                self.write_history(row, true, job, files, shown).await;
                self.after_history(short, ended);
            }
            _ => self.write_histories(retired).await,
        }
    }

    // What follows a retired torrent's history row: one that left short of
    // complete ended its download here, out of its session, and what it
    // held for that goes back, as a cancelled or failed NZB job's does (a
    // seed's download ended as it completed); then its script event.
    fn after_history(&self, short: bool, ended: Option<EventContext>) {
        if short {
            self.release_memory(MemoryRelease::Ended);
        }
        self.queue_script_event(ended);
    }

    // `write_history` of each of a batch's retired torrents, their rows
    // queued as one write the writer takes in one transaction, and each
    // then awaited in turn as one torrent's is.
    async fn write_histories(&self, retired: Vec<Retired>) {
        let mut rows = Vec::with_capacity(retired.len());
        let mut after = Vec::with_capacity(retired.len());
        for one in retired {
            let Retired {
                row,
                job,
                files,
                shown,
                short,
                ended,
            } = one;
            let turn = self.history_turn(job.job_id);
            rows.push((job.job_id, row, true, files));
            after.push((turn, job, shown, short, ended));
        }
        let mut queued = match self.inner.db.queue_torrent_histories(rows) {
            Ok(waits) => Ok(waits.into_iter()),
            Err(error) => Err(error.to_string()),
        };
        for (turn, job, shown, short, ended) in after {
            let waits = match &mut queued {
                Ok(waits) => waits.next().ok_or("the writer answered for fewer rows"),
                Err(error) => Err(error.as_str()),
            };
            self.land_history(waits, turn, true, job, shown).await;
            self.after_history(short, ended);
        }
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
        let turn = self.history_turn(job.job_id);
        let queued = self
            .inner
            .db
            .queue_torrent_history(job.job_id, row, retires, files);
        self.land_history(queued, turn, retires, job, shown).await;
    }

    // A history row queued for the job in its `turn` (`write_history`),
    // awaited: written, kept for room, or lost.
    async fn land_history(
        &self,
        queued: Result<
            (
                tokio::sync::oneshot::Receiver<()>,
                tokio::sync::oneshot::Receiver<()>,
            ),
            impl std::fmt::Display,
        >,
        turn: u64,
        retires: bool,
        job: JobInfo,
        shown: Shown,
    ) {
        let job_id = job.job_id;
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

    /// Remove a torrent, as the operator asks: never refused for a seeding
    /// goal still to meet. The web asks the operator first.
    pub async fn remove(&self, job_id: JobId, delete_files: bool) -> Result<(), ActionError> {
        self.remove_as(job_id, delete_files, true, Ending::Asked)
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
        self.remove_marked(job_id, delete_files, ending, Shown::Now, None)
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
        made: Option<MadeRead>,
    ) -> Result<(), ActionError> {
        let retired = self
            .take_out(job_id, delete_files, ending, shown, made)
            .await?;
        self.to_history(vec![retired]).await;
        Ok(())
    }

    // `remove_marked` up to its history row: out of the live torrents, its
    // files gone when they go, and readied for history (`to_history`).
    pub(super) async fn take_out(
        &self,
        job_id: JobId,
        delete_files: bool,
        ending: Ending,
        shown: Shown,
        made: Option<MadeRead>,
    ) -> Result<Retired, ActionError> {
        let mut torrent = self
            .unlive(&mut *self.inner.live.lock().await, job_id)
            .ok_or(ActionError::NotFound(job_id))?;
        let mut of = self.written_of(&torrent);
        of.made = made;
        let written = read_written(of).await;
        // The job's outcome: complete, or cut short. Its unpacking's never
        // changes it.
        let status = if torrent.complete() {
            HistoryEnd::Complete
        } else {
            HistoryEnd::Cancelled
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
        Ok(self
            .ready_history(torrent, status, None, written, shown, ended)
            .await)
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
            made: None,
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
        #[cfg(any(test, feature = "test-support"))]
        let held = locked(&MOVE_HOLD).take();
        #[cfg(any(test, feature = "test-support"))]
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
            #[cfg(any(test, feature = "test-support"))]
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

// A torrent that left the live ones, its history row not yet written
// (`TorrentService::ready_history`, `TorrentService::to_history`).
pub(super) struct Retired {
    row: crate::history::JobHistoryRow,
    job: JobInfo,
    files: Vec<HistoryFile>,
    shown: Shown,
    // Whether it left short of complete, its download cut short.
    short: bool,
    // The script event its end raises, queued once its row is.
    ended: Option<EventContext>,
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
    // What its row says it made, when its batch read that for all of them
    // at once (`Database::torrents_made`); read here otherwise.
    made: Option<MadeRead>,
}

// What a torrent wrote: what a deletion of its files takes, the files
// unpacking placed beside them among it.
#[derive(Debug, Default)]
pub(in crate::torrents) struct Wrote {
    pub(in crate::torrents) deletion: Deletion,
}

// `written_paths`, off the runtime: its two database reads and the marker
// read of its unpacking directory block.
pub(in crate::torrents) async fn read_written(of: WrittenOf) -> Wrote {
    let job_id = of.job_id;
    tokio::task::spawn_blocking(move || written_paths(of))
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
fn written_paths(mut of: WrittenOf) -> Wrote {
    let job_id = of.job_id;
    let Some(store) = &of.store else {
        return Wrote::default();
    };
    let (adopted, outputs) = match of.made.take() {
        Some(made) => made,
        None => (of.db.torrent_adopted(job_id), of.db.torrent_outputs(job_id)),
    };
    let adopted = adopted.unwrap_or_else(|error| {
        tracing::warn!(job_id = job_id.0, error = %error, "could not read what was on disk before a torrent");
        Some(Adopted {
            all_files: true,
            dirs: vec![String::new()],
            ..Adopted::default()
        })
    });
    let outputs = outputs.unwrap_or_else(|error| {
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
    Wrote { deletion }
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

// A test's hold on a removal just before its mark is written.
pub(super) async fn mark_hold() {
    #[cfg(any(test, feature = "test-support"))]
    let held = locked(&MARK_HOLD).take();
    #[cfg(any(test, feature = "test-support"))]
    if let Some((reached, release)) = held {
        let _ = reached.send(());
        let _ = release.await;
    }
}
