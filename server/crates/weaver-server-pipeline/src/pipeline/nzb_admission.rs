// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::persistence::JobsPersistenceInternalQueries;
use crate::pipeline::download::LoginRefusalView;
use crate::pipeline::queue::Change;
use weaver_server_disk::operations::room_wait::StartBooking;
pub(crate) use weaver_server_disk::operations::start_rule::{
    StartMost, StartRefused as DiskUnadmitted, StartRule,
};

pub(crate) const NZB_SLOT_WAIT_REASON: &str = "nzb_slot";

// What a job holding a slot was promised on the disk when it started: the
// bytes it still had to fetch, on the working root's count, and, when the
// complete directory lies on another disk, its size on the complete root's
// count; still with the walk until its graph takes them, or handed to its
// graph, which settles them as its bytes land and gives back the rest when
// it stops. A start is promised on those two counts whatever root it was
// judged on: they are the ones its bytes land against, on its working and
// output devices, and the ones every give-back goes to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartPromise {
    Pending { working: u64, output: u64 },
    Handed,
}

// How many candidates a walk looked at for a slot, or showed waiting for the
// disk, on this thread, so a test can pin that a walk visits no more of a
// long queue than its slots call for.
#[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
    // The bytes the job still has to fetch, as its post declares them.
    need: u64,
    // What the job delivers: what its download is out to bring.
    size: u64,
}

// What a walk over every queue row answers to besides the actor's own state:
// the intents written since, the rows held, and the globals that gate a slot.
// A turn that finds the same mark, and nothing deferred by the last walk,
// skips the walks.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct QueueWalkMark {
    intent_epoch: u64,
    rows: usize,
    holds: crate::jobs::HoldSet,
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
            holds: globals.holds.clone(),
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
        // A restore whose outline is unread says nothing yet of what it
        // has to fetch, so it is not judged for a start until it does.
        let pending = (row.admission.is_some()
            || row.intents.restore.is_some()
            || row.intents.paused == Some(false)
            || self.jobs.contains_key(&row.id)
            || self.window_parked(row.id))
            && !row.outline_pending();
        AdmissionRow {
            id: row.id,
            key: crate::pipeline::queue::order::DispatchKey::of_row(row, position, globals),
            paused: self.admission_paused(row),
            pending,
            cancelled_or_retiring: row.intents.cancelled.is_some() || row.report.retiring,
            intake: row.admission.is_some(),
            need: row.report.job.remaining_bytes,
            size: Self::start_size(row),
        }
    }

    // The start check on the working disk: the owner every usenet job
    // starts by, a fresh one, a resumed one and one a full disk held alike.
    pub(in crate::pipeline) fn working_disk_check(
        &self,
    ) -> weaver_server_disk::operations::room_wait::StartCheck {
        self.start_rule().check()
    }

    // The start rule usenet jobs start by: fetched to the working root,
    // delivered to the complete one.
    pub(in crate::pipeline) fn start_rule(&self) -> StartRule {
        StartRule {
            working: crate::operations::StorageRoot::Intermediate,
            output: crate::operations::StorageRoot::Complete,
            reserve: self.extraction_limits.min_free_bytes,
        }
    }

    // Whether the complete directory lies on another disk than the working
    // one: a job's bytes may land on either, so a start is promised on both.
    pub(in crate::pipeline) fn complete_on_other_disk(&self) -> bool {
        self.start_rule().other_disk(&self.storage_capacity)
    }

    // What a start promises on the complete disk when that is another
    // disk: the job's size, all it delivers there, never less than what it
    // still has to fetch.
    pub(in crate::pipeline) fn start_size(row: &crate::pipeline::queue::JobRow) -> u64 {
        row.report
            .job
            .wanted_bytes
            .max(row.report.job.remaining_bytes)
    }

    // `need` bytes still to fetch are promised for a job that would start,
    // on the working disk, and its `size` on the complete disk when that is
    // another, when they fit there by a fresh reading.
    pub(in crate::pipeline) fn book_start_promise(
        &self,
        need: u64,
        size: u64,
    ) -> Result<StartPromise, StartBooking> {
        self.book_start_promise_on(crate::operations::StorageRoot::Intermediate, need, size)
    }

    // The same, judged on `read` when both folders share one disk: a job
    // held on the disk its write failed on is judged on that root's reading.
    // It is promised on the working root's count all the same.
    pub(in crate::pipeline) fn book_start_promise_on(
        &self,
        read: crate::operations::StorageRoot,
        need: u64,
        size: u64,
    ) -> Result<StartPromise, StartBooking> {
        self.start_rule()
            .book_on(&self.storage_capacity, read, need, size)
            .map(|(working, output)| StartPromise::Pending { working, output })
    }

    // The same for a job held for a full disk starting again: judged
    // without what the jobs held for a full disk wait for, being one of
    // them.
    pub(in crate::pipeline) fn book_held_start_promise_on(
        &self,
        read: crate::operations::StorageRoot,
        need: u64,
        size: u64,
    ) -> Result<StartPromise, StartBooking> {
        self.start_rule()
            .book_held_on(&self.storage_capacity, read, need, size)
            .map(|(working, output)| StartPromise::Pending { working, output })
    }

    // A job that held its slot across a restart is promised what it still
    // has to fetch, unjudged: it started before, and keeps its place. On a
    // complete disk of its own it is promised that much and no more: what
    // it fetched before is not promised again, and what it moves there
    // books its room as it is written.
    fn promise_held_start(&mut self, id: JobId, need: u64) {
        self.promise_start_unjudged(id, need, need);
        self.hand_start_promise(id);
    }

    // `need` is promised to the job's start on the working disk, and its
    // `size` on the complete disk when that is another, with nothing
    // judged: its room was judged otherwise. What it was promised before is
    // given back first. Its graph takes it once it is in place.
    pub(in crate::pipeline) fn promise_start_unjudged(&mut self, id: JobId, need: u64, size: u64) {
        self.release_start_promise(id);
        let (need, output) = self
            .start_rule()
            .promise_unjudged(&self.storage_capacity, need, size);
        self.start_promises.insert(
            id,
            StartPromise::Pending {
                working: need,
                output,
            },
        );
    }

    // A graph is put in place for the job. One that holds its slot still
    // from a graph that stopped, which gave back its promise as it went, as
    // a job paused and resumed before its slot was let go does, is promised
    // again what it has left to fetch, and its size on a complete disk of
    // its own, as a start is, unjudged: it keeps its place. With nothing
    // left to fetch, nothing is. The new graph then takes the promise.
    pub(in crate::pipeline) fn start_promise_to_graph(&mut self, id: JobId) {
        if matches!(self.start_promises.get(&id), Some(StartPromise::Handed))
            && let (Some(graph), Some(state)) = (self.graph_jobs.get(&id), self.jobs.get(&id))
        {
            let (remaining, wanted) = Self::graph_remaining(graph, &state.spec);
            if remaining > 0 {
                self.promise_start_unjudged(id, remaining, wanted.max(remaining));
            }
        }
        self.hand_start_promise(id);
    }

    // The job's graph takes the bytes promised at its start, once it is in
    // place: from here they settle as its bytes land.
    pub(in crate::pipeline) fn hand_start_promise(&mut self, id: JobId) {
        let Some(StartPromise::Pending { working, output }) = self.start_promises.get(&id).copied()
        else {
            return;
        };
        let Some(graph) = self.graph_jobs.get_mut(&id) else {
            return;
        };
        graph.take_posted(working, output);
        self.start_promises.insert(id, StartPromise::Handed);
    }

    // The job lets its slot go, or never took it: what its start was
    // promised and has not landed is given back, which rings the disk.
    pub(in crate::pipeline) fn release_start_promise(&mut self, id: JobId) {
        match self.start_promises.remove(&id) {
            Some(StartPromise::Pending { working, output }) => {
                self.give_back_start(working, output);
            }
            Some(StartPromise::Handed) => {
                if let Some(graph) = self.graph_jobs.get_mut(&id) {
                    graph.release_posted();
                }
            }
            None => {}
        }
    }

    // A disk rang while jobs are kept from starting for it: a walk is
    // brought only when one could start now, by the start rule's wait.
    pub(in crate::pipeline) fn release_admission_disk_hold(&mut self) {
        let rule = self.start_rule();
        if self.admission_disk.ring(&self.storage_capacity, rule) {
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
                    if let Some(mut row) = inner.job_mut(*id)
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
        // The rows of the jobs holding a slot, as (paused, intake, the bytes
        // still to fetch and the job's size, or None while the row waits for
        // its outline and says neither), or None where the row is gone.
        type HeldRow = Option<(bool, bool, Option<(u64, u64)>)>;
        let held: Vec<(JobId, HeldRow)> = self.shared_state.queue.read(|inner| {
            self.nzb_admitted
                .iter()
                .map(|id| {
                    (
                        *id,
                        inner.job(*id).map(|row| {
                            (
                                self.admission_paused(row),
                                row.admission.is_some(),
                                (!row.outline_pending()).then(|| {
                                    (row.report.job.remaining_bytes, Self::start_size(row))
                                }),
                            )
                        }),
                    )
                })
                .collect()
        });
        // A slot held across a restart is promised what its job still has
        // to fetch, unjudged, once its row is read: no other start takes
        // that room first. One paused, or held for a full disk, is promised
        // when it is let go on again. Until every held slot's row is read,
        // its outline among it, and every restore is in, no other job starts.
        let mut unread_holders = self.restores_waiting;
        for (id, row) in &held {
            match row {
                Some((paused, _, None)) => unread_holders |= !*paused,
                Some((paused, _, Some((need, _)))) => {
                    if !*paused
                        && !self.start_promises.contains_key(id)
                        && !self.full_disk.contains_key(id)
                    {
                        self.promise_held_start(*id, *need);
                    }
                }
                None => unread_holders |= !self.nzb_seen.contains(id),
            }
        }
        let releases: Vec<_> = held
            .iter()
            .filter(|(id, row)| {
                let gone = self.nzb_seen.contains(id) && row.is_none();
                // History retry/reprocess can reuse an ID before this actor
                // observes the old row's removal. The new run must compete
                // for a slot and commit its own ownership flag.
                let replaced =
                    !self.jobs.contains_key(id) && row.is_some_and(|(_, intake, _)| intake);
                let paused = self.nzb_pause_releases.contains(id)
                    && row.is_some_and(|(paused, _, _)| paused);
                (gone || replaced || paused)
                    && !self.nzb_releasing.contains(id)
                    && !self.nzb_work_in_flight(*id)
            })
            .map(|(id, row)| {
                (
                    *id,
                    !self.jobs.contains_key(id) && row.is_some_and(|(_, intake, _)| intake),
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
        let mut candidates = Vec::new();
        for row in rows.iter().filter(|row| {
            row.pending
                && !row.paused
                && !row.cancelled_or_retiring
                && !self.nzb_admitted.contains(&row.id)
                && !self.nzb_admitting.contains(&row.id)
        }) {
            candidates.push((row.key, row.need, row.size));
        }
        drop(rows);
        if !self.disk_unadmitted.is_empty() {
            self.disk_unadmitted.clear();
        }
        candidates.sort_unstable_by_key(|(key, ..)| *key);
        let propagating: HashSet<_> = candidates
            .iter()
            .filter_map(|(key, ..)| {
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
        // Each candidate, in dispatch order, starts when a slot is free and
        // the bytes it still has to fetch fit on its disk by a fresh
        // reading, less everything promised there: they are promised as it
        // takes the slot. One that does not fit is passed over, and the walk
        // goes on to the next, which may fit. Once one did not fit, a later
        // one that needs as much or more is passed over without asking the
        // disk again. As many as there are free slots are shown waiting for
        // the disk, the next ones to start; the rest wait for a slot as they
        // would behind them; so are as many turned away for their size,
        // counted apart, so neither hides why the other waits. A start
        // judged without a fresh reading waits for one, as does every later
        // one. A job too large for the disks ever to hold is turned away
        // before the slots are looked at.
        let mut slots = None::<usize>;
        let (mut shown_short, mut shown_small) = (0usize, 0usize);
        // By disk, the working one and the complete one when that is
        // another: the least room a refusal there showed in this walk, and
        // the least a job refused there needs on it. Whether a refusal
        // found no fresh reading.
        let mut short_room = [None::<u64>; 2];
        let mut need_min = [u64::MAX; 2];
        let mut unread = false;
        let mut kept = false;
        let other = self.complete_on_other_disk();
        // The most the disks could ever promise a start, read once: a job
        // larger is turned away by it, and the ledgers are not asked.
        let most = self.start_most();
        for (key, need, size) in &candidates {
            let (id, need, size) = (key.job_id(), *need, *size);
            if propagating.contains(&id) {
                if !self.slot_allowed() {
                    break;
                }
                continue;
            }
            if unread_holders {
                break;
            }
            // What the job is judged by on each disk.
            let needs = [need, if other { size.max(need) } else { 0 }];
            let known_short = (0..2).find_map(|disk| {
                short_room[disk]
                    .filter(|room| needs[disk] > *room)
                    .map(|room| (disk, room))
            });
            let refused = if let Some(too_small) = most.and_then(|most| most.refuses(need, size)) {
                Some(too_small)
            } else if unread {
                need_min[0] = need_min[0].min(need);
                Some(DiskUnadmitted::Short { need, room: None })
            } else if let Some((disk, room)) = known_short {
                need_min[disk] = need_min[disk].min(needs[disk]);
                Some(DiskUnadmitted::Short {
                    need: needs[disk],
                    room: Some(room),
                })
            } else {
                if !self.slot_allowed() {
                    break;
                }
                match self.book_start_promise(need, size) {
                    Ok(promise) => {
                        if self.queue_slot_write(id, true) {
                            self.start_promises.insert(id, promise);
                        } else if let StartPromise::Pending { working, output } = promise {
                            self.give_back_start(working, output);
                        }
                        None
                    }
                    Err(StartBooking::Short { room, need, part }) => {
                        let disk = part.min(1);
                        short_room[disk] =
                            Some(short_room[disk].map_or(room, |least| least.min(room)));
                        need_min[disk] = need_min[disk].min(need);
                        Some(DiskUnadmitted::Short {
                            need,
                            room: Some(room),
                        })
                    }
                    Err(StartBooking::TooLarge { most, need, .. }) => {
                        Some(DiskUnadmitted::TooSmall { need, most })
                    }
                    Err(StartBooking::Unread | StartBooking::Booked) => {
                        unread = true;
                        need_min[0] = need_min[0].min(need);
                        Some(DiskUnadmitted::Short { need, room: None })
                    }
                }
            };
            let Some(refused) = refused else {
                continue;
            };
            kept = true;
            let slots = *slots.get_or_insert_with(|| self.admission_slots());
            let free = slots.saturating_sub(self.nzb_admitted.len() + self.nzb_admitting.len());
            let shown = match refused {
                DiskUnadmitted::Short { .. } => &mut shown_short,
                DiskUnadmitted::TooSmall { .. } => &mut shown_small,
            };
            if *shown < free {
                self.disk_unadmitted.insert(id, refused);
                *shown += 1;
            } else if unread && matches!(refused, DiskUnadmitted::Short { .. }) {
                // Nothing more starts without a reading, and every slot
                // left has its job shown waiting for one.
                break;
            }
        }
        // The hold begins, or goes on, while a job was kept for the disk,
        // and ends once none was.
        let rule = self.start_rule();
        self.admission_disk
            .walked(&self.storage_capacity, rule, kept, (need_min, unread, most));
        let login_refusals = self.login_refusal_view();
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
                changed |= self.restate_slot(row, login_refusals);
            }
            if changed {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
        true
    }

    // The most a start could ever be promised on the working disk, and on
    // the complete disk when that is another, by fresh, good readings:
    // `None` while either has none, and the ledgers judge.
    fn start_most(&self) -> Option<StartMost> {
        self.start_rule().most(&self.storage_capacity)
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

    // Whether a slot is free for the next candidate. False once none is,
    // which ends the walk's admissions.
    fn slot_allowed(&self) -> bool {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
        ADMISSION_VISITS.with(|visits| visits.set(visits.get() + 1));
        // A settings edit or global hold can arrive while the previous
        // acquisition awaits its durable commit. Re-read before admitting
        // the next job rather than using the pass's earlier snapshot.
        self.shared_state.queue.read(|inner| {
            let globals = &inner.globals;
            self.nzb_admitted.len() + self.nzb_admitting.len()
                < globals
                    .max_concurrent_nzbs
                    .unwrap_or(crate::settings::model::DEFAULT_MAX_CONCURRENT_NZBS)
                    .get() as usize
                && !globals.holds_downloads()
        })
    }

    // Bytes promised for a start that did not take its slot go back.
    fn give_back_start(&self, working: u64, output: u64) {
        self.start_rule()
            .give_back(&self.storage_capacity, working, output);
    }

    fn forget_retired(&mut self, id: JobId) {
        self.release_start_promise(id);
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
    // deadline), else the dispatcher's wait or hold. `login_refusals` is the
    // pass's one read of the pool's login refusals, taken before the queue
    // lock ([`Self::login_refusal_view`]).
    pub(in crate::pipeline) fn stored_wait_reason(
        &self,
        row: &crate::pipeline::queue::JobRow,
        login_refusals: LoginRefusalView,
    ) -> Option<std::borrow::Cow<'static, str>> {
        use std::borrow::Cow;
        if self.waits_for_nzb_slot(row) {
            if !self.disk_unadmitted.is_empty() {
                match self.disk_unadmitted.get(&row.id) {
                    Some(DiskUnadmitted::Short { .. }) => {
                        return Some(Cow::Borrowed(crate::jobs::handle::DISK_SPACE_WAIT_REASON));
                    }
                    Some(DiskUnadmitted::TooSmall { .. }) => {
                        return Some(Cow::Borrowed(
                            crate::jobs::handle::DISK_TOO_SMALL_WAIT_REASON,
                        ));
                    }
                    None => {}
                }
            }
            return (!self.propagation_ready_at.contains_key(&row.id))
                .then_some(Cow::Borrowed(NZB_SLOT_WAIT_REASON));
        }
        self.dispatch_wait_reason(row.id, login_refusals)
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
    // back as a [`DbReply::Slot`]. Returns whether the write is out.
    fn queue_slot_write(&mut self, id: JobId, admitted: bool) -> bool {
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
                true
            }
            Ok(()) => {
                self.nzb_releasing.insert(id);
                true
            }
            Err(error) => {
                error!(job_id = id.0, admitted, %error, "failed to queue the NZB slot write");
                self.queue_walk_due = true;
                false
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
            // A start that did not take its slot gives back its promise;
            // the next walk judges it again.
            if admitted {
                self.release_start_promise(id);
            }
            self.queue_walk_due = true;
            return;
        }
        if admitted {
            self.nzb_admitted.insert(id);
            // A job already in place takes the bytes promised at its start.
            self.hand_start_promise(id);
            // A job restored without a slot queued its engine runs; release
            // them now that it holds one.
            self.resume_graph_work(id);
            self.download_dispatch_wake = true;
        } else {
            self.nzb_admitted.remove(&id);
            self.nzb_pause_releases.remove(&id);
            self.release_start_promise(id);
        }
        // A slot that moved is walked to: the holder is installed wherever
        // it stands, and a slot let go is free for the next candidate.
        self.queue_walk_due = true;
        let login_refusals = self.login_refusal_view();
        self.shared_state.queue.write(|inner| {
            if inner
                .job_mut(id)
                .is_some_and(|mut row| self.restate_slot(&mut row, login_refusals))
            {
                Change::changed(())
            } else {
                Change::unchanged(())
            }
        });
    }

    // States on the row whether its job holds a slot, and the wait that
    // follows from it. Returns whether the row changed.
    fn restate_slot(
        &self,
        row: &mut crate::pipeline::queue::JobRow,
        login_refusals: LoginRefusalView,
    ) -> bool {
        let mut changed = false;
        let admitted = self.nzb_admitted.contains(&row.id);
        if row.report.nzb_admitted != admitted {
            row.report.nzb_admitted = admitted;
            changed = true;
        }
        let reason = self.stored_wait_reason(row, login_refusals);
        if row.report.job.download_wait_reason.as_deref() != reason.as_deref() {
            let slot_wait = reason.as_deref() == Some(NZB_SLOT_WAIT_REASON);
            row.report.job.download_wait_reason = reason.map(std::borrow::Cow::into_owned);
            if slot_wait
                || (!self.disk_unadmitted.is_empty() && self.disk_unadmitted.contains_key(&row.id))
            {
                row.report.job.download_state = crate::DownloadState::Queued;
            }
            changed = true;
        }
        // Only a figure that changed restates the row.
        let (need, free) = self.disk_figures(row.id);
        let job = &mut row.report.job;
        if job.disk_need_bytes != need || job.disk_free_bytes != free {
            job.disk_need_bytes = need;
            job.disk_free_bytes = free;
            changed = true;
        }
        changed
    }

    // What a job kept for the disk says it needs and what the disk leaves,
    // or the most it ever holds: the one answer every writer of the row
    // gives. The figures go when the wait does.
    pub(in crate::pipeline) fn disk_figures(&self, id: JobId) -> (Option<u64>, Option<u64>) {
        match self.disk_unadmitted.get(&id) {
            Some(refused) => refused.figures(),
            None if !self.full_disk.is_empty() => self.full_disk_figures(id),
            None => (None, None),
        }
    }

    pub(crate) fn nzb_work_in_flight(&self, id: JobId) -> bool {
        #[cfg(any(test, feature = "pipeline-test-fixtures"))]
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
