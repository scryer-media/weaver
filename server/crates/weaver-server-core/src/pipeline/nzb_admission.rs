// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::queue::Change;

pub(crate) const NZB_SLOT_WAIT_REASON: &str = "nzb_slot";

// How many candidates a walk looked at for a slot, or showed waiting for the
// disk, on this thread, so a test can pin that a walk visits no more of a
// long queue than its slots call for.
#[cfg(test)]
thread_local! {
    pub(crate) static ADMISSION_VISITS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

// One queue row as the admission pass sees it, copied out under the read lock.
struct AdmissionRow {
    id: JobId,
    // Where the row stands in dispatch order.
    key: crate::pipeline::queue::order::DispatchKey,
    paused: bool,
    // The row still carries an intake, a restore or a resume for the actor,
    // or its job is installed and waits for a slot to download.
    pending: bool,
    cancelled_or_retiring: bool,
    // The row carries a fresh intake the actor has not installed yet.
    intake: bool,
    // The job downloaded before: it held a slot, and let it go on a pause.
    started: bool,
}

// What a walk over every queue row answers to besides the actor's own state:
// the intents written since, the rows held, and the globals that gate a slot.
// A turn that finds the same mark, and nothing deferred by the last walk,
// skips the walks.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueueWalkMark {
    intent_epoch: u64,
    rows: usize,
    paused: bool,
    scheduled_pause: bool,
    speed_test_paused: bool,
    schedule_replay_paused: bool,
    shutting_down: bool,
    max_concurrent_nzbs: Option<std::num::NonZeroU32>,
    propagation_delay: std::time::Duration,
}

impl QueueWalkMark {
    pub(crate) fn of(inner: &crate::pipeline::queue::QueueInner) -> Self {
        let globals = &inner.globals;
        Self {
            intent_epoch: inner.intent_epoch(),
            rows: inner.jobs.len(),
            paused: globals.paused,
            scheduled_pause: globals.scheduled_pause,
            speed_test_paused: globals.speed_test_paused,
            schedule_replay_paused: globals.schedule_replay_paused,
            shutting_down: globals.shutting_down,
            max_concurrent_nzbs: globals.max_concurrent_nzbs,
            propagation_delay: globals.propagation_delay,
        }
    }
}

impl Pipeline {
    // Whether the row's job counts as paused for its slot.
    pub(in crate::pipeline) fn admission_paused(
        &self,
        row: &crate::pipeline::queue::JobRow,
    ) -> bool {
        row.intents.paused.unwrap_or_else(|| {
            row.admission
                .as_ref()
                .is_some_and(|job| job.options.initially_paused)
                || row
                    .intents
                    .restore
                    .as_ref()
                    .is_some_and(|job| job.status == JobStatus::Paused)
                || row.report.job.run_state == crate::RunState::Paused
                || self
                    .jobs
                    .get(&row.id)
                    .is_some_and(|state| state.run_state == crate::RunState::Paused)
        })
    }

    fn admission_row(
        &self,
        globals: &crate::pipeline::queue::Globals,
        position: usize,
        row: &crate::pipeline::queue::JobRow,
    ) -> AdmissionRow {
        // The job's state is looked up where whether it is pending asks for
        // it, and nowhere else: whether it started reads the same lookup.
        let mut installed = None;
        let pending = row.admission.is_some()
            || row.intents.restore.is_some()
            || row.intents.paused == Some(false)
            || {
                installed = self.jobs.get(&row.id);
                installed.is_some()
            }
            || self.window_parked(row.id);
        AdmissionRow {
            id: row.id,
            key: crate::pipeline::queue::order::DispatchKey::of_row(row, position, globals),
            paused: self.admission_paused(row),
            pending,
            cancelled_or_retiring: row.intents.cancelled.is_some() || row.report.retiring,
            intake: row.admission.is_some(),
            started: row.report.job.downloaded_bytes > 0
                || installed.is_some_and(|state| state.downloaded_bytes > 0),
        }
    }

    // Whether the working disk has no room over its reserve for a job that
    // never downloaded to start: once it has not, none starts until a good
    // reading shows room, or the disk has no reading. A disk with no
    // reading, or only the last good one held across a failed probe, or no
    // reserve, is not checked: only a good reading begins the hold. A hold
    // begun lasts across a stale reading, as a job's wait for room does.
    fn admission_disk_short(&mut self) -> bool {
        if self.admission_disk_held {
            return true;
        }
        let short = self.working_disk_short();
        if short {
            self.admission_disk_held = true;
            self.storage_capacity
                .ledger(crate::operations::StorageRoot::Intermediate)
                .wait();
        }
        short
    }

    // Whether the working disk's cached reading is a good one with no room
    // over the reserve. It reads the cache and begins nothing.
    fn working_disk_short(&self) -> bool {
        let reserve = self.extraction_limits.min_free_bytes;
        reserve > 0
            && self
                .storage_capacity
                .free(crate::operations::StorageRoot::Intermediate)
                .is_some_and(|free| !free.stale && free.bytes <= reserve)
    }

    // A reading came while jobs are kept from starting for the working
    // disk: a good one with room over the reserve, or none at all, lets
    // them start, and the next walk starts them.
    pub(in crate::pipeline) fn release_admission_disk_hold(&mut self) {
        if !self.admission_disk_held {
            return;
        }
        let reserve = self.extraction_limits.min_free_bytes;
        let root = crate::operations::StorageRoot::Intermediate;
        let room = self
            .storage_capacity
            .free(root)
            .is_none_or(|free| !free.stale && free.bytes > reserve);
        if room {
            self.admission_disk_held = false;
            self.storage_capacity.ledger(root).unwait();
            self.queue_walk_due = true;
        }
    }

    // Ownership survives empty queues, retries and all downstream processing.
    // Intake remains in the shared queue until its durable admission commits.
    //
    // The slots held are checked every turn by looking up their rows. The
    // walk over every row, which finds new candidates, retires forgotten ids
    // and restates each row's slot, runs when `walk` asks for it or a slot
    // was released this turn. Returns whether it walked.
    pub(crate) fn reconcile_nzb_admission(&mut self, walk: bool) -> bool {
        self.recheck_retired_busy();
        // A crash between accepting pause and draining work must not turn
        // the interrupted pause into an automatic resume.
        if !self.nzb_pause_releases.is_empty() {
            self.shared_state.queue.write(|inner| {
                let mut changed = false;
                for id in &self.nzb_pause_releases {
                    if let Some(row) = inner.job_mut(*id)
                        && row.intents.restore.is_some()
                    {
                        row.intents.paused = Some(true);
                        row.intents.seq = row.intents.seq.wrapping_add(1);
                        row.intents.pause_seq = row.intents.seq;
                        changed = true;
                    }
                }
                if changed {
                    Change::changed(())
                } else {
                    Change::unchanged(())
                }
            });
        }
        // The rows of the jobs holding a slot, as (paused, intake), or None
        // where the row is gone.
        let held: Vec<(JobId, Option<(bool, bool)>)> = self.shared_state.queue.read(|inner| {
            self.nzb_admitted
                .iter()
                .map(|id| {
                    (
                        *id,
                        inner
                            .job(*id)
                            .map(|row| (self.admission_paused(row), row.admission.is_some())),
                    )
                })
                .collect()
        });
        let releases: Vec<_> = held
            .iter()
            .filter(|(id, row)| {
                let gone = self.nzb_seen.contains(id) && row.is_none();
                // History retry/reprocess can reuse an ID before this actor
                // observes the old row's removal. The new run must compete
                // for a slot and commit its own ownership flag.
                let replaced = !self.jobs.contains_key(id) && row.is_some_and(|(_, intake)| intake);
                let paused =
                    self.nzb_pause_releases.contains(id) && row.is_some_and(|(paused, _)| paused);
                (gone || replaced || paused)
                    && !self.nzb_releasing.contains(id)
                    && !self.nzb_work_in_flight(*id)
            })
            .map(|(id, row)| {
                (
                    *id,
                    !self.jobs.contains_key(id) && row.is_some_and(|(_, intake)| intake),
                )
            })
            .collect();
        let mut released = false;
        for (id, replaced) in releases {
            // A replacement's old record has already been durably archived.
            // Its new record starts unowned and must acquire independently.
            // Any other slot is let go once its release commits: it counts as
            // held until then.
            if replaced {
                self.slot_written(id, false, true);
                released = true;
            } else {
                self.queue_slot_write(id, false);
            }
        }
        if !walk && !released {
            return false;
        }
        crate::pipeline::queue::count_walk();
        let (shutting_down, rows) = self.shared_state.queue.read(|inner| {
            (
                inner.globals.shutting_down,
                inner
                    .jobs
                    .iter()
                    .enumerate()
                    .map(|(position, row)| self.admission_row(&inner.globals, position, row))
                    .collect::<Vec<_>>(),
            )
        });
        let present: HashSet<JobId> = rows.iter().map(|row| row.id).collect();
        self.nzb_seen.extend(present.iter().copied());
        let retired: Vec<_> = self
            .nzb_seen
            .iter()
            .copied()
            .filter(|id| !self.nzb_admitted.contains(id) && !present.contains(id))
            .collect();
        for id in retired {
            if self.nzb_work_in_flight(id) {
                // Looked at again once its work drains.
                self.nzb_retired_busy.insert(id);
                continue;
            }
            self.forget_retired(id);
        }
        // A history retry can replace a canceled, unadmitted paused job
        // before this actor sees the old row disappear.
        for row in &rows {
            if row.intake && !self.jobs.contains_key(&row.id) && !self.nzb_work_in_flight(row.id) {
                self.nzb_reprocess_pending.remove(&row.id);
            }
        }
        // Only a shutdown stops admission; the pauses and the slot count are
        // read per candidate below.
        if shutting_down {
            return true;
        }
        // While the working disk may be short, the candidates that
        // downloaded before are kept apart as they are found, in the one
        // pass: only they start then. With room nothing is kept apart.
        let may_be_short = self.admission_disk_held || self.working_disk_short();
        let mut candidates = Vec::new();
        let mut started = Vec::new();
        for row in rows.iter().filter(|row| {
            row.pending
                && !row.paused
                && !row.cancelled_or_retiring
                && !self.nzb_admitted.contains(&row.id)
                && !self.nzb_admitting.contains(&row.id)
        }) {
            candidates.push(row.key);
            if may_be_short && row.started {
                started.push(row.key);
            }
        }
        drop(rows);
        if !self.disk_unadmitted.is_empty() {
            self.disk_unadmitted.clear();
        }
        candidates.sort_unstable();
        if may_be_short {
            started.sort_unstable();
        }
        let propagating: HashSet<_> = candidates
            .iter()
            .filter_map(|key| {
                self.propagation_hold_until(key.job_id())
                    .map(|_| key.job_id())
            })
            .collect();
        // A hold that ran out on a job that is not a candidate, such as a
        // paused one, holds nothing. It goes once seen, so it never calls for
        // a walk on every turn; a candidate's went just above.
        let now = Instant::now();
        let expired: Vec<_> = self
            .propagation_ready_at
            .iter()
            .filter(|(_, (ready_at, _))| *ready_at <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            self.propagation_hold_until(id);
        }
        // The disk is judged once per walk, and only when a job that never
        // downloaded would start. While it is short such a job is passed
        // over: only as many as there are free slots are shown waiting for
        // the disk, the next ones to start, and the rest wait for a slot as
        // they would behind them. Once those are shown, the walk goes on
        // over the candidates that started before, and no other. A hold
        // that begins or ends restates no more rows than there are slots.
        let mut short = None::<bool>;
        let mut slots = None::<usize>;
        let mut shown = 0usize;
        let mut shown_to = None;
        for key in &candidates {
            let id = key.job_id();
            // A job that never downloaded does not start while the working
            // disk has no room over its reserve: it waits, saying so, and a
            // reading with room starts it. One that started, then paused and
            // resumed, starts again whatever the disk holds: what it fetches
            // is never held for the reserve.
            if may_be_short
                && started.binary_search(key).is_err()
                && *short.get_or_insert_with(|| self.admission_disk_short())
            {
                #[cfg(test)]
                ADMISSION_VISITS.with(|visits| visits.set(visits.get() + 1));
                let slots = *slots.get_or_insert_with(|| self.admission_slots());
                if !propagating.contains(&id)
                    && self.nzb_admitted.len() + self.nzb_admitting.len() + shown < slots
                {
                    self.disk_unadmitted.insert(id);
                    shown += 1;
                }
                if self.nzb_admitted.len() + self.nzb_admitting.len() + shown >= slots {
                    shown_to = Some(*key);
                    break;
                }
                continue;
            }
            if !self.admit_candidate(id, &propagating) {
                break;
            }
        }
        if let Some(last) = shown_to {
            for key in started.iter().filter(|key| **key > last) {
                if !self.admit_candidate(key.job_id(), &propagating) {
                    break;
                }
            }
        }
        self.shared_state.queue.write(|inner| {
            let mut changed = false;
            for row in &mut inner.jobs {
                if propagating.contains(&row.id) {
                    row.report.job.download_state = crate::DownloadState::Queued;
                    row.report.propagation_until_epoch_ms = self
                        .propagation_ready_at
                        .get(&row.id)
                        .map(|(_, epoch)| *epoch as f64);
                    changed = true;
                }
                changed |= self.restate_slot(row);
            }
            if changed {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
        true
    }

    // How many jobs may hold a slot now: none while downloads are held.
    fn admission_slots(&self) -> usize {
        self.shared_state.queue.read(|inner| {
            let globals = &inner.globals;
            match globals.holds_downloads() {
                true => 0,
                false => globals
                    .max_concurrent_nzbs
                    .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                    .get() as usize,
            }
        })
    }

    // The candidate takes a slot if one is free. False once none is, which
    // ends the walk's admissions.
    fn admit_candidate(&mut self, id: JobId, propagating: &HashSet<JobId>) -> bool {
        #[cfg(test)]
        ADMISSION_VISITS.with(|visits| visits.set(visits.get() + 1));
        // A settings edit or global hold can arrive while the previous
        // acquisition awaits its durable commit. Re-read before admitting
        // the next job rather than using the pass's earlier snapshot.
        let allowed = self.shared_state.queue.read(|inner| {
            let globals = &inner.globals;
            self.nzb_admitted.len() + self.nzb_admitting.len()
                < globals
                    .max_concurrent_nzbs
                    .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                    .get() as usize
                && !globals.holds_downloads()
        });
        if !allowed {
            return false;
        }
        if !propagating.contains(&id) {
            self.queue_slot_write(id, true);
        }
        true
    }

    fn forget_retired(&mut self, id: JobId) {
        self.nzb_retired_busy.remove(&id);
        self.nzb_seen.remove(&id);
        self.nzb_pause_releases.remove(&id);
        self.nzb_reprocess_pending.remove(&id);
        self.resumes_stored.remove(&id);
    }

    // The retired ids whose work was in flight: one that came back to the
    // queue or took a slot is no longer retired, and one whose work drained
    // is forgotten, as the walk would have.
    fn recheck_retired_busy(&mut self) {
        if self.nzb_retired_busy.is_empty() {
            return;
        }
        let busy: Vec<JobId> = self.nzb_retired_busy.iter().copied().collect();
        let present: HashSet<JobId> = self.shared_state.queue.read(|inner| {
            busy.iter()
                .copied()
                .filter(|id| inner.contains_job(*id))
                .collect()
        });
        for id in busy {
            if present.contains(&id) || self.nzb_admitted.contains(&id) {
                self.nzb_retired_busy.remove(&id);
            } else if !self.nzb_work_in_flight(id) {
                self.forget_retired(id);
            }
        }
    }

    // The wait reason a queue row stores, the one answer every writer of the
    // row gives: a slot while the job would download but holds none (not
    // while its post is still propagating, which the row states by its
    // deadline), else the dispatcher's wait or hold.
    pub(crate) fn stored_wait_reason(
        &self,
        row: &crate::pipeline::queue::JobRow,
    ) -> Option<&'static str> {
        if self.waits_for_nzb_slot(row) {
            if !self.disk_unadmitted.is_empty() && self.disk_unadmitted.contains(&row.id) {
                return Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON);
            }
            return (!self.propagation_ready_at.contains_key(&row.id))
                .then_some(NZB_SLOT_WAIT_REASON);
        }
        self.dispatch_wait_reason(row.id)
    }

    // Whether the row's job would download now but holds no slot: an intake,
    // a restore or a resume that is not paused, or an installed job or one
    // out of the queue window that is not paused, none of them cancelled.
    pub(crate) fn waits_for_nzb_slot(&self, row: &crate::pipeline::queue::JobRow) -> bool {
        !self.nzb_admitted.contains(&row.id)
            && row.intents.cancelled.is_none()
            && row.intents.paused != Some(true)
            && (row
                .admission
                .as_ref()
                .is_some_and(|job| !job.options.initially_paused)
                || row
                    .intents
                    .restore
                    .as_ref()
                    .is_some_and(|job| job.status != JobStatus::Paused)
                || row.intents.paused == Some(false)
                || self
                    .jobs
                    .get(&row.id)
                    .is_some_and(|state| state.run_state != crate::RunState::Paused)
                || (self.window_parked(row.id)
                    && row.report.job.run_state != crate::RunState::Paused))
    }

    // A slot is taken or let go durably before the actor acts on it: the
    // write goes on the job's lane, behind every write the job queued
    // before, and the slot counts as held while it is out. Its answer comes
    // back as a [`DbReply::Slot`].
    fn queue_slot_write(&mut self, id: JobId, admitted: bool) {
        let replies = self.db_reply_tx.clone();
        let queued = self
            .db
            .try_queue_job_write(id, "set_nzb_admitted", move |db| {
                let written = db.set_nzb_admitted(id, admitted);
                let _ = replies.send(crate::pipeline::db_reply::DbReply::Slot {
                    job_id: id,
                    admitted,
                    committed: written.is_ok(),
                });
                written
            });
        match queued {
            Ok(()) if admitted => {
                self.nzb_admitting.insert(id);
            }
            Ok(()) => {
                self.nzb_releasing.insert(id);
            }
            Err(error) => {
                error!(job_id = id.0, admitted, %error, "failed to queue the NZB slot write");
                self.queue_walk_due = true;
            }
        }
    }

    // A slot's write came back. One that committed is acted on; one that
    // did not leaves the slot as it was, for the next walk to try again.
    pub(crate) fn slot_written(&mut self, id: JobId, admitted: bool, committed: bool) {
        if admitted {
            self.nzb_admitting.remove(&id);
        } else {
            self.nzb_releasing.remove(&id);
        }
        if !committed {
            error!(
                job_id = id.0,
                admitted, "failed to write the NZB slot; the next walk tries again"
            );
            self.queue_walk_due = true;
            return;
        }
        if admitted {
            self.nzb_admitted.insert(id);
            // A job restored without a slot queued its engine runs; release
            // them now that it holds one.
            self.resume_graph_work(id);
            self.download_dispatch_wake = true;
        } else {
            self.nzb_admitted.remove(&id);
            self.nzb_pause_releases.remove(&id);
        }
        // A slot that moved is walked to: the holder is installed wherever
        // it stands, and a slot let go is free for the next candidate.
        self.queue_walk_due = true;
        self.shared_state.queue.write(|inner| {
            if inner.job_mut(id).is_some_and(|row| self.restate_slot(row)) {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
    }

    // States on the row whether its job holds a slot, and the wait that
    // follows from it. Returns whether the row changed.
    fn restate_slot(&self, row: &mut crate::pipeline::queue::JobRow) -> bool {
        let mut changed = false;
        let admitted = self.nzb_admitted.contains(&row.id);
        if row.report.nzb_admitted != admitted {
            row.report.nzb_admitted = admitted;
            changed = true;
        }
        let reason = self.stored_wait_reason(row);
        if row.report.job.download_wait_reason.as_deref() != reason {
            row.report.job.download_wait_reason = reason.map(str::to_owned);
            if reason == Some(NZB_SLOT_WAIT_REASON)
                || (!self.disk_unadmitted.is_empty() && self.disk_unadmitted.contains(&row.id))
            {
                row.report.job.download_state = crate::DownloadState::Queued;
            }
            changed = true;
        }
        changed
    }

    pub(crate) fn nzb_work_in_flight(&self, id: JobId) -> bool {
        #[cfg(test)]
        if self.job_has_queued_decode(id) {
            return true;
        }
        self.download_lane_owners.values().any(|lane| {
            lane.outstanding
                .keys()
                .any(|segment| segment.file_id.job_id == id)
        }) || self
            .active_decodes_by_job
            .get(&id)
            .is_some_and(|count| *count > 0)
            || self
                .pending_released_download_results_by_job
                .get(&id)
                .is_some_and(|count| *count > 0)
            || self
                .graph_jobs
                .get(&id)
                .is_some_and(|job| job.awaits_mail())
            || self.inflight_terminal_post_processing.contains(&id)
            || self.added_script_holds.contains(&id)
            || self.inflight_moves.contains(&id)
    }
}
